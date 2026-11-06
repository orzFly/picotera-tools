package axonimport

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio-go/v7"
	"golang.org/x/sync/errgroup"

	"github.com/orzfly/picotera-tools/internal/picotera"
)

// Reproject re-resolves the project of every meta row currently in one
// project, reading the client body back from the request artifact, and moves
// the row (with its upstream attempts) when it resolves elsewhere.
type Reproject struct {
	DB        *pgxpool.Pool
	S3        *minio.Client
	Bucket    string
	Projects  *Projects
	ProjectID int32
	UserID    int64
	Batch     int
	Workers   int
	Write     bool

	Report  *ProjectReport
	Moved   atomic.Int64
	Stayed  atomic.Int64
	Missing atomic.Int64
}

func (rp *Reproject) Run(ctx context.Context) error {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return err
	}
	defer dec.Close()
	rp.Report = &ProjectReport{entries: map[string]*reportEntry{}}

	var afterTS time.Time
	afterID := ""
	start := time.Now()
	var total int64
	for {
		rows, err := rp.DB.Query(ctx, `
			SELECT id, created_at FROM request
			WHERE project_id = $1 AND user_id = $2 AND type = 0 AND (created_at, id) > ($3, $4)
			ORDER BY created_at, id LIMIT $5`, rp.ProjectID, rp.UserID, afterTS, afterID, rp.Batch)
		if err != nil {
			return fmt.Errorf("page meta rows: %w", err)
		}
		type meta struct {
			id string
			at time.Time
		}
		var metas []meta
		for rows.Next() {
			var m meta
			if err := rows.Scan(&m.id, &m.at); err != nil {
				rows.Close()
				return err
			}
			metas = append(metas, m)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(metas) == 0 {
			break
		}

		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(rp.Workers)
		for _, m := range metas {
			g.Go(func() error { return rp.one(gctx, dec, m.id, m.at) })
		}
		if err := g.Wait(); err != nil {
			return err
		}
		last := metas[len(metas)-1]
		afterTS, afterID = last.at, last.id
		total += int64(len(metas))
		log.Printf("reprojected through %s %s: %d rows, %d move, %d stay, %d artifact missing, %s",
			last.at.Format(time.DateTime), last.id, total, rp.Moved.Load(), rp.Stayed.Load(), rp.Missing.Load(), time.Since(start).Round(time.Second))
	}
	return nil
}

func (rp *Reproject) one(ctx context.Context, dec *zstd.Decoder, id string, at time.Time) error {
	body, err := rp.requestBody(ctx, dec, id, at)
	if err != nil {
		return err
	}
	if body == nil {
		rp.Missing.Add(1)
		rp.Stayed.Add(1)
		return nil
	}
	res, err := rp.Projects.Resolve(ctx, body)
	if err != nil {
		return err
	}
	if res == nil || (res.ID == rp.ProjectID) || (!res.Matched && !res.Created) {
		rp.Stayed.Add(1)
		rp.Report.add(&Resolution{ID: rp.ProjectID, Name: "(stays)", Matched: true}, at)
		return nil
	}
	rp.Report.add(res, at)
	rp.Moved.Add(1)
	if !rp.Write {
		return nil
	}
	// Upstream attempts share the meta row's span and start within its
	// lifetime; the created_at bound keeps the lookup to one or two chunks.
	_, err = rp.DB.Exec(ctx, `
		UPDATE request SET project_id = $1
		WHERE span_id = $2 AND user_id = $3 AND project_id = $4
		  AND created_at >= $5 AND created_at < $5 + interval '1 day'`,
		res.ID, id, rp.UserID, rp.ProjectID, at)
	if err != nil {
		return fmt.Errorf("move %s: %w", id, err)
	}
	return nil
}

func (rp *Reproject) requestBody(ctx context.Context, dec *zstd.Decoder, id string, at time.Time) ([]byte, error) {
	obj, err := rp.S3.GetObject(ctx, rp.Bucket, picotera.RequestKey(id, at), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	raw, err := io.ReadAll(obj)
	if err != nil {
		var resp minio.ErrorResponse
		if errors.As(err, &resp) && resp.Code == "NoSuchKey" {
			return nil, nil
		}
		return nil, fmt.Errorf("get artifact %s: %w", id, err)
	}
	js, err := dec.DecodeAll(raw, nil)
	if err != nil {
		return nil, fmt.Errorf("decode artifact %s: %w", id, err)
	}
	var p picotera.Payload
	if err := json.Unmarshal(js, &p); err != nil {
		return nil, fmt.Errorf("parse artifact %s: %w", id, err)
	}
	if p.BodyEncoding == "base64" {
		return base64.StdEncoding.DecodeString(p.Body)
	}
	return []byte(p.Body), nil
}

// RefreshProjects recomputes first/last seen for the given projects from
// their meta rows, then refreshes the continuous aggregates from since.
func RefreshProjects(ctx context.Context, db *pgxpool.Pool, ids []int32, since time.Time) error {
	_, err := db.Exec(ctx, `
		UPDATE project p SET first_seen_at = s.first, last_seen_at = s.last, updated_at = now()
		FROM (SELECT project_id, min(created_at) AS first, max(created_at) AS last
		      FROM request WHERE type = 0 AND project_id = ANY($1) GROUP BY project_id) s
		WHERE p.id = s.project_id`, ids)
	if err != nil {
		return fmt.Errorf("update project seen: %w", err)
	}
	for _, view := range []string{"request_overview_bucketed", "request_outcome_bucketed"} {
		if _, err := db.Exec(ctx, `CALL refresh_continuous_aggregate($1::regclass, $2::timestamp, NULL)`, view, since); err != nil {
			return fmt.Errorf("refresh %s: %w", view, err)
		}
	}
	return nil
}

// TouchedProjects lists the projects a reproject run moved rows into, plus
// the source project.
func (rp *Reproject) TouchedProjects() []int32 {
	ids := []int32{rp.ProjectID}
	for _, e := range rp.Report.entries {
		if e.id > 0 && e.id != rp.ProjectID {
			ids = append(ids, e.id)
		}
	}
	return ids
}
