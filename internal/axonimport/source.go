package axonimport

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Request struct {
	ID             int64
	CreatedAt      time.Time
	ModelID        string
	Format         string
	Stream         bool
	Status         string
	RequestHeaders []byte
	RequestBody    []byte
	ResponseBody   []byte
	LatencyMs      *int64
	TTFTMs         *int64
	ChannelID      *int64
	TraceID        *string
	APIKeyID       *int64

	Executions []Execution
	Usage      *Usage
}

type Execution struct {
	ID             int64
	CreatedAt      time.Time
	ModelID        string
	Format         string
	Stream         bool
	Status         string
	RequestHeaders []byte
	RequestBody    []byte
	ResponseBody   []byte
	ErrorMessage   *string
	StatusCode     *int64
	LatencyMs      *int64
	TTFTMs         *int64
	ChannelID      *int64
	RequestURL     *string
}

type Usage struct {
	ChannelID        *int64
	PromptTokens     int64
	CompletionTokens int64
	CachedTokens     int64
	WriteCached      int64
	WriteCached5m    int64
	WriteCached1h    int64
	TotalCost        *float64
}

type Source struct {
	pool *pgxpool.Pool
}

func NewSource(pool *pgxpool.Pool) *Source { return &Source{pool: pool} }

func (s *Source) Channels(ctx context.Context) (map[int64]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, name FROM channels`)
	if err != nil {
		return nil, fmt.Errorf("list channels: %w", err)
	}
	defer rows.Close()
	out := map[int64]string{}
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		out[id] = name
	}
	return out, rows.Err()
}

// Batch returns up to limit requests with id in (afterID, untilID], fully
// populated with executions, usage and trace id. Bodies are scanned as raw
// jsonb text so they are never decoded into Go values.
func (s *Source) Batch(ctx context.Context, afterID, untilID int64, limit int, sampleMod int64) ([]*Request, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT r.id, r.created_at, r.model_id, r.format, r.stream, r.status,
		       r.request_headers, r.request_body, r.response_body,
		       r.metrics_latency_ms, r.metrics_first_token_latency_ms, r.channel_id, t.trace_id, r.api_key_id
		FROM requests r LEFT JOIN traces t ON t.id = r.trace_id
		WHERE r.id > $1 AND r.id <= $2 AND ($4::bigint = 0 OR r.id % $4::bigint = 0)
		ORDER BY r.id
		LIMIT $3`, afterID, untilID, limit, sampleMod)
	if err != nil {
		return nil, fmt.Errorf("query requests: %w", err)
	}
	var reqs []*Request
	byID := map[int64]*Request{}
	for rows.Next() {
		r := &Request{}
		if err := rows.Scan(&r.ID, &r.CreatedAt, &r.ModelID, &r.Format, &r.Stream, &r.Status,
			&r.RequestHeaders, &r.RequestBody, &r.ResponseBody,
			&r.LatencyMs, &r.TTFTMs, &r.ChannelID, &r.TraceID, &r.APIKeyID); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan request: %w", err)
		}
		reqs = append(reqs, r)
		byID[r.ID] = r
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(reqs) == 0 {
		return nil, nil
	}
	ids := make([]int64, len(reqs))
	for i, r := range reqs {
		ids[i] = r.ID
	}

	rows, err = s.pool.Query(ctx, `
		SELECT request_id, id, created_at, model_id, format, stream, status,
		       request_headers, request_body, response_body, error_message, response_status_code,
		       metrics_latency_ms, metrics_first_token_latency_ms, channel_id, request_url
		FROM request_executions
		WHERE request_id = ANY($1)
		ORDER BY id`, ids)
	if err != nil {
		return nil, fmt.Errorf("query executions: %w", err)
	}
	for rows.Next() {
		var reqID int64
		e := Execution{}
		if err := rows.Scan(&reqID, &e.ID, &e.CreatedAt, &e.ModelID, &e.Format, &e.Stream, &e.Status,
			&e.RequestHeaders, &e.RequestBody, &e.ResponseBody, &e.ErrorMessage, &e.StatusCode,
			&e.LatencyMs, &e.TTFTMs, &e.ChannelID, &e.RequestURL); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan execution: %w", err)
		}
		if r := byID[reqID]; r != nil {
			r.Executions = append(r.Executions, e)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// One request has two usage_logs rows; ORDER BY id lets the later one win.
	rows, err = s.pool.Query(ctx, `
		SELECT request_id, channel_id, prompt_tokens, completion_tokens,
		       COALESCE(prompt_cached_tokens, 0), COALESCE(prompt_write_cached_tokens, 0),
		       COALESCE(prompt_write_cached_tokens_5m, 0), COALESCE(prompt_write_cached_tokens_1h, 0), total_cost
		FROM usage_logs
		WHERE request_id = ANY($1)
		ORDER BY id`, ids)
	if err != nil {
		return nil, fmt.Errorf("query usage: %w", err)
	}
	for rows.Next() {
		var reqID int64
		u := &Usage{}
		if err := rows.Scan(&reqID, &u.ChannelID, &u.PromptTokens, &u.CompletionTokens,
			&u.CachedTokens, &u.WriteCached, &u.WriteCached5m, &u.WriteCached1h, &u.TotalCost); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan usage: %w", err)
		}
		if r := byID[reqID]; r != nil {
			r.Usage = u
		}
	}
	rows.Close()
	return reqs, rows.Err()
}

func (s *Source) MaxID(ctx context.Context) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(max(id), 0) FROM requests`).Scan(&id)
	return id, err
}

type BodyRow struct {
	ID          int64
	CreatedAt   time.Time
	RequestBody []byte
}

// Bodies pages through client request bodies only, for the project report,
// which has no use for the far larger response columns.
func (s *Source) Bodies(ctx context.Context, afterID, untilID int64, limit int, sampleMod int64) ([]BodyRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, created_at, request_body FROM requests
		WHERE id > $1 AND id <= $2 AND ($4::bigint = 0 OR id % $4::bigint = 0)
		ORDER BY id LIMIT $3`, afterID, untilID, limit, sampleMod)
	if err != nil {
		return nil, fmt.Errorf("query bodies: %w", err)
	}
	defer rows.Close()
	var out []BodyRow
	for rows.Next() {
		var b BodyRow
		if err := rows.Scan(&b.ID, &b.CreatedAt, &b.RequestBody); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// SafeUntil is the newest request id an incremental run may import: older
// than settle, and before any request still in flight. Pending rows older
// than an hour are ones AxonHub never closed, imported as failed.
func (s *Source) SafeUntil(ctx context.Context, settle time.Duration) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `
		SELECT LEAST(
		  COALESCE((SELECT max(id) FROM requests WHERE created_at < now() - $1::interval), 0),
		  COALESCE((SELECT min(id) - 1 FROM requests
		            WHERE status IN ('pending', 'processing') AND created_at > now() - interval '1 hour'), 9223372036854775807))`,
		settle.String()).Scan(&id)
	return id, err
}
