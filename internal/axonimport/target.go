package axonimport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio-go/v7"
	"github.com/rs/xid"
)

type Target interface {
	// Exists reports which meta row ids are already imported, so re-runs skip
	// them without re-uploading artifacts.
	Exists(ctx context.Context, rows []Row) (map[string]bool, error)
	Write(ctx context.Context, rec *Record) error
	// PutArtifacts uploads artifacts without touching rows, for rewriting
	// the artifacts of rows an earlier run imported.
	PutArtifacts(ctx context.Context, arts []Artifact) error
}

type Picotera struct {
	DB     *pgxpool.Pool
	S3     *minio.Client
	Bucket string
}

func LoadProviders(ctx context.Context, db *pgxpool.Pool) (map[string]int64, error) {
	rows, err := db.Query(ctx, `SELECT id, name FROM provider`)
	if err != nil {
		return nil, fmt.Errorf("list providers: %w", err)
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		out[name] = id
	}
	return out, rows.Err()
}

func (p *Picotera) Exists(ctx context.Context, metas []Row) (map[string]bool, error) {
	if len(metas) == 0 {
		return nil, nil
	}
	ids := make([]string, len(metas))
	lo, hi := metas[0].CreatedAt, metas[0].CreatedAt
	for i, r := range metas {
		ids[i] = r.ID
		if r.CreatedAt.Before(lo) {
			lo = r.CreatedAt
		}
		if r.CreatedAt.After(hi) {
			hi = r.CreatedAt
		}
	}
	// The created_at bounds let TimescaleDB exclude unrelated chunks.
	rows, err := p.DB.Query(ctx, `SELECT id FROM request WHERE id = ANY($1) AND created_at BETWEEN $2 AND $3`, ids, lo, hi)
	if err != nil {
		return nil, fmt.Errorf("check existing: %w", err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = true
	}
	return out, rows.Err()
}

// Write uploads artifacts before inserting rows: a row whose artifacts are
// missing renders as broken, while orphaned artifacts from a crash are simply
// overwritten by the next run.
func (p *Picotera) PutArtifacts(ctx context.Context, arts []Artifact) error {
	for _, a := range arts {
		_, err := p.S3.PutObject(ctx, p.Bucket, a.Key, bytes.NewReader(a.Data), int64(len(a.Data)), minio.PutObjectOptions{
			ContentType:          "application/json",
			ContentEncoding:      "zstd",
			DisableContentSha256: true,
		})
		if err != nil {
			return fmt.Errorf("put %s: %w", a.Key, err)
		}
	}
	return nil
}

func (p *Picotera) Write(ctx context.Context, rec *Record) error {
	if err := p.PutArtifacts(ctx, rec.Artifacts); err != nil {
		return err
	}
	batch := &pgx.Batch{}
	for _, r := range rec.Rows {
		ann, err := json.Marshal(r.Annotations)
		if err != nil {
			return err
		}
		var usage any
		if r.UsageRaw != nil {
			usage = string(r.UsageRaw)
		}
		batch.Queue(`
			INSERT INTO request (id, created_at, span_id, parent_span_id, type, provider_id, endpoint_path, user_id, api_key_id, project_id,
			  model, upstream_model, input_tokens, cache_read_tokens, output_tokens, cache_write_tokens, cache_write_1h_tokens,
			  status_code, error_message, ttft_ms, time_spent_ms, finish_reason, user_message_preview, annotations, usage_raw)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24::jsonb,$25::jsonb)
			ON CONFLICT (id, created_at) DO NOTHING`,
			r.ID, r.CreatedAt, r.SpanID, r.ParentSpanID, r.Type, r.ProviderID, r.EndpointPath, r.UserID, r.APIKeyID, r.ProjectID,
			r.Model, r.UpstreamModel, int4(r.InputTokens), int4(r.CacheReadTokens), int4(r.OutputTokens), int4(r.CacheWriteTokens), int4(r.CacheWrite1hTokens),
			int4(r.StatusCode), r.ErrorMessage, int4(r.TTFTMs), int4(r.TimeSpentMs), r.FinishReason, r.UserMessagePreview, string(ann), usage)
	}
	return pgx.BeginFunc(ctx, p.DB, func(tx pgx.Tx) error {
		return tx.SendBatch(ctx, batch).Close()
	})
}

// Finalize does what PicoTera's gateway would have done live: one traces row
// per session, and continuous aggregates that only refresh the last 35 days
// on their own.
func Finalize(ctx context.Context, db *pgxpool.Pool, userID int64, since time.Time) error {
	rows, err := db.Query(ctx, `
		SELECT parent_span_id, min(created_at), max(created_at)
		FROM request
		WHERE annotations @> '{"source":"axonhub"}' AND user_id = $1
		  AND parent_span_id IS NOT NULL AND parent_span_id <> ''
		GROUP BY parent_span_id`, userID)
	if err != nil {
		return fmt.Errorf("collect traces: %w", err)
	}
	type trace struct {
		span        string
		first, last time.Time
	}
	var traces []trace
	for rows.Next() {
		var t trace
		if err := rows.Scan(&t.span, &t.first, &t.last); err != nil {
			rows.Close()
			return err
		}
		traces = append(traces, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	batch := &pgx.Batch{}
	for _, t := range traces {
		batch.Queue(`
			INSERT INTO traces (id, parent_span_id, user_id, first_request_at, last_request_at)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (parent_span_id, user_id) DO UPDATE
			SET first_request_at = LEAST(traces.first_request_at, EXCLUDED.first_request_at),
			    last_request_at = GREATEST(traces.last_request_at, EXCLUDED.last_request_at),
			    updated_at = CURRENT_TIMESTAMP`,
			xid.NewWithTime(t.first).String(), t.span, userID, t.first, t.last)
	}
	if err := db.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("upsert traces: %w", err)
	}
	fmt.Fprintf(os.Stderr, "upserted %d traces\n", len(traces))

	tag, err := db.Exec(ctx, `
		UPDATE project p
		SET first_seen_at = LEAST(COALESCE(p.first_seen_at, s.first), s.first),
		    last_seen_at  = GREATEST(COALESCE(p.last_seen_at, s.last), s.last),
		    updated_at    = now()
		FROM (
		  SELECT project_id, min(created_at) AS first, max(created_at) AS last
		  FROM request
		  WHERE annotations @> '{"source":"axonhub"}' AND user_id = $1 AND type = 0 AND project_id IS NOT NULL
		  GROUP BY project_id
		) s
		WHERE p.id = s.project_id`, userID)
	if err != nil {
		return fmt.Errorf("update project seen: %w", err)
	}
	fmt.Fprintf(os.Stderr, "updated first/last seen on %d projects\n", tag.RowsAffected())

	// refresh_continuous_aggregate refuses to run inside a transaction block.
	for _, view := range []string{"request_overview_bucketed", "request_outcome_bucketed"} {
		if _, err := db.Exec(ctx, `CALL refresh_continuous_aggregate($1::regclass, $2::timestamp, NULL)`, view, since); err != nil {
			return fmt.Errorf("refresh %s: %w", view, err)
		}
		fmt.Fprintf(os.Stderr, "refreshed %s since %s\n", view, since.Format(time.DateOnly))
	}
	return nil
}

// DumpTarget writes what would be imported to a directory, decompressing
// artifacts so they can be read directly.
type DumpTarget struct {
	Dir string
	dec *zstd.Decoder
	mu  sync.Mutex
}

func NewDumpTarget(dir string) (*DumpTarget, error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	return &DumpTarget{Dir: dir, dec: dec}, nil
}

func (d *DumpTarget) Exists(context.Context, []Row) (map[string]bool, error) { return nil, nil }

func (d *DumpTarget) PutArtifacts(context.Context, []Artifact) error { return nil }

func (d *DumpTarget) Write(_ context.Context, rec *Record) error {
	if d.Dir == "" {
		return nil
	}
	for _, a := range rec.Artifacts {
		raw, err := d.dec.DecodeAll(a.Data, nil)
		if err != nil {
			return err
		}
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, raw, "", "  "); err != nil {
			return err
		}
		path := filepath.Join(d.Dir, a.Key)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(path[:len(path)-len(".zst")], pretty.Bytes(), 0o644); err != nil {
			return err
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	f, err := os.OpenFile(filepath.Join(d.Dir, "rows.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, r := range rec.Rows {
		if err := enc.Encode(r); err != nil {
			return err
		}
	}
	return nil
}

// int4 drops values PicoTera's integer columns cannot hold. AxonHub wrote
// metrics_latency_ms = -9223372036854 (MinInt64 / 1e6) for requests whose
// end time it never recorded; none of these columns has a meaningful negative.
func int4(v *int64) *int32 {
	if v == nil || *v < 0 || *v > math.MaxInt32 {
		return nil
	}
	n := int32(*v)
	return &n
}

// ImportedHighWater is the largest AxonHub request id already in PicoTera,
// the starting point of an incremental run.
func ImportedHighWater(ctx context.Context, db *pgxpool.Pool, userID int64) (int64, error) {
	var id int64
	err := db.QueryRow(ctx, `
		SELECT COALESCE(max((annotations->>'axonhub_request_id')::bigint), 0)
		FROM request WHERE annotations @> '{"source":"axonhub"}' AND type = 0 AND user_id = $1`, userID).Scan(&id)
	return id, err
}
