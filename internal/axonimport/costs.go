package axonimport

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// BackfillCosts copies AxonHub's usage_logs.total_cost onto requests imported
// before the importer carried costs: the meta row and the winning attempt,
// which is the upstream row the importer gave tokens to. Rows already holding
// the same cost are left alone, so it can be re-run.
type BackfillCosts struct {
	Axon    *pgxpool.Pool
	Pico    *pgxpool.Pool
	Batch   int
	AfterID int64
	UntilID int64
	Write   bool

	Requests int64
	Rows     int64
	Total    float64
	Earliest time.Time
}

const costMatch = `
	FROM request
	WHERE span_id = $2 AND created_at >= $3 AND created_at < $3 + interval '1 day'
	  AND annotations @> '{"source":"axonhub"}'
	  AND (type = 0 OR input_tokens IS NOT NULL)
	  AND model_cost IS DISTINCT FROM $1`

func (b *BackfillCosts) Run(ctx context.Context) error {
	after := b.AfterID
	for {
		rows, err := b.Axon.Query(ctx, `
			SELECT r.id, r.created_at, u.total_cost
			FROM requests r
			JOIN LATERAL (SELECT total_cost FROM usage_logs WHERE request_id = r.id ORDER BY id DESC LIMIT 1) u ON true
			WHERE r.id > $1 AND r.id <= $2 AND u.total_cost IS NOT NULL
			ORDER BY r.id LIMIT $3`, after, b.UntilID, b.Batch)
		if err != nil {
			return fmt.Errorf("read costs: %w", err)
		}
		type cost struct {
			id    int64
			at    time.Time
			total float64
		}
		var costs []cost
		for rows.Next() {
			var c cost
			if err := rows.Scan(&c.id, &c.at, &c.total); err != nil {
				rows.Close()
				return err
			}
			costs = append(costs, c)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(costs) == 0 {
			return nil
		}

		batch := &pgx.Batch{}
		for _, c := range costs {
			metaID, err := deterministicID(c.at, kindMeta, c.id)
			if err != nil {
				return err
			}
			at := metaID.Time().UTC()
			if b.Write {
				batch.Queue(`UPDATE request SET model_cost = $1, model_cost_currency = '`+AxonHubCurrency+`'`+
					` WHERE (id, created_at) IN (SELECT id, created_at`+costMatch+`)`, c.total, metaID.String(), at)
			} else {
				batch.Queue(`SELECT count(*)`+costMatch, c.total, metaID.String(), at)
			}
			if b.Earliest.IsZero() || at.Before(b.Earliest) {
				b.Earliest = at
			}
		}
		send := func(q interface {
			SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
		}) error {
			br := q.SendBatch(ctx, batch)
			defer br.Close()
			for _, c := range costs {
				var n int64
				if b.Write {
					tag, err := br.Exec()
					if err != nil {
						return err
					}
					n = tag.RowsAffected()
				} else if err := br.QueryRow().Scan(&n); err != nil {
					return err
				}
				if n > 0 {
					b.Requests++
					b.Rows += n
					b.Total += c.total
				}
			}
			return nil
		}
		if b.Write {
			err = pgx.BeginFunc(ctx, b.Pico, func(tx pgx.Tx) error { return send(tx) })
		} else {
			err = send(b.Pico)
		}
		if err != nil {
			return fmt.Errorf("costs after axonhub id %d: %w", after, err)
		}
		after = costs[len(costs)-1].id
		log.Printf("costs through axonhub id %d: %d requests, %d rows, %.2f %s", after, b.Requests, b.Rows, b.Total, AxonHubCurrency)
	}
}

// RefreshOverview re-materializes the cost-bearing aggregate from since.
func RefreshOverview(ctx context.Context, db *pgxpool.Pool, since time.Time) error {
	_, err := db.Exec(ctx, `CALL refresh_continuous_aggregate('request_overview_bucketed', $1::timestamp, NULL)`, since)
	return err
}
