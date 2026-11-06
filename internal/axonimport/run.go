package axonimport

import (
	"context"
	"fmt"
	"io"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"golang.org/x/sync/errgroup"
)

type Stats struct {
	Requests      atomic.Int64
	Skipped       atomic.Int64
	Rows          atomic.Int64
	Artifacts     atomic.Int64
	ArtifactBytes atomic.Int64
	ProjectHit    atomic.Int64
	ProjectNew    atomic.Int64
	ProjectMiss   atomic.Int64
	ProjectNone   atomic.Int64
}

type Importer struct {
	Source  *Source
	Mapper  *Mapper
	Target  Target
	Batch   int
	Workers int
	AfterID int64
	UntilID int64
	Limit   int64
	// SampleMod > 0 imports only ids divisible by it, for dry-run spot checks.
	SampleMod int64
	Stats     Stats
	Report    *ProjectReport
}

func (im *Importer) Run(ctx context.Context) error {
	after := im.AfterID
	start := time.Now()
	for im.Limit <= 0 || im.Stats.Requests.Load() < im.Limit {
		n := im.Batch
		if im.Limit > 0 && int64(n) > im.Limit-im.Stats.Requests.Load() {
			n = int(im.Limit - im.Stats.Requests.Load())
		}
		reqs, err := im.Source.Batch(ctx, after, im.UntilID, n, im.SampleMod)
		if err != nil {
			return err
		}
		if len(reqs) == 0 {
			break
		}
		if err := im.runBatch(ctx, reqs); err != nil {
			return fmt.Errorf("batch after id %d: %w", after, err)
		}
		after = reqs[len(reqs)-1].ID
		// The last id is the resume point for --after-id.
		log.Printf("done through axonhub id %d: %d requests, %d skipped, %d rows, %d artifacts, %.1f MiB, %s",
			after, im.Stats.Requests.Load(), im.Stats.Skipped.Load(), im.Stats.Rows.Load(),
			im.Stats.Artifacts.Load(), float64(im.Stats.ArtifactBytes.Load())/(1<<20), time.Since(start).Round(time.Second))
	}
	return nil
}

func (im *Importer) runBatch(ctx context.Context, reqs []*Request) error {
	recs := make([]*Record, len(reqs))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(im.Workers)
	for i, r := range reqs {
		g.Go(func() error {
			rec, err := im.Mapper.Map(gctx, r)
			if err != nil {
				return fmt.Errorf("map axonhub request %d: %w", r.ID, err)
			}
			recs[i] = rec
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	metas := make([]Row, len(recs))
	for i, rec := range recs {
		metas[i] = rec.Rows[0]
	}
	existing, err := im.Target.Exists(ctx, metas)
	if err != nil {
		return err
	}

	g, gctx = errgroup.WithContext(ctx)
	g.SetLimit(im.Workers)
	for _, rec := range recs {
		im.Stats.Requests.Add(1)
		if existing[rec.Rows[0].ID] {
			im.Stats.Skipped.Add(1)
			continue
		}
		im.Report.add(rec.Project, rec.Rows[0].CreatedAt)
		switch p := rec.Project; {
		case p == nil:
			im.Stats.ProjectNone.Add(1)
		case p.Matched:
			im.Stats.ProjectHit.Add(1)
		case p.Created:
			im.Stats.ProjectNew.Add(1)
		default:
			im.Stats.ProjectMiss.Add(1)
		}
		g.Go(func() error {
			if err := im.Target.Write(gctx, rec); err != nil {
				return err
			}
			im.Stats.Rows.Add(int64(len(rec.Rows)))
			im.Stats.Artifacts.Add(int64(len(rec.Artifacts)))
			for _, a := range rec.Artifacts {
				im.Stats.ArtifactBytes.Add(int64(len(a.Data)))
			}
			return nil
		})
	}
	return g.Wait()
}

func (im *Importer) Summary(w io.Writer) {
	s := &im.Stats
	fmt.Fprintf(w, "requests %d (skipped %d), rows %d, artifacts %d, %.1f MiB compressed\n",
		s.Requests.Load(), s.Skipped.Load(), s.Rows.Load(), s.Artifacts.Load(), float64(s.ArtifactBytes.Load())/(1<<20))
	fmt.Fprintf(w, "projects: matched %d, created %d, unmatched %d, no workspace path %d\n",
		s.ProjectHit.Load(), s.ProjectNew.Load(), s.ProjectMiss.Load(), s.ProjectNone.Load())
}

// ProjectReport tallies, without writing anything, which existing project
// each request would land in and which projects auto-creation would add.
type ProjectReport struct {
	mu      sync.Mutex
	entries map[string]*reportEntry
	none    int64
}

type reportEntry struct {
	kind        string
	id          int32
	name        string
	source      string
	count       int64
	first, last time.Time
}

func RunProjectReport(ctx context.Context, src *Source, projects *Projects, afterID, untilID int64, batch, workers int, sampleMod int64) (*ProjectReport, error) {
	rep := &ProjectReport{entries: map[string]*reportEntry{}}
	after := afterID
	var total int64
	start := time.Now()
	for {
		rows, err := src.Bodies(ctx, after, untilID, batch, sampleMod)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			break
		}
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(workers)
		for _, b := range rows {
			g.Go(func() error {
				res, err := projects.Resolve(gctx, b.RequestBody)
				if err != nil {
					return err
				}
				rep.add(res, b.CreatedAt)
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return nil, err
		}
		after = rows[len(rows)-1].ID
		total += int64(len(rows))
		log.Printf("scanned through axonhub id %d: %d requests, %s", after, total, time.Since(start).Round(time.Second))
	}
	return rep, nil
}

func NewProjectReport() *ProjectReport {
	return &ProjectReport{entries: map[string]*reportEntry{}}
}

func (rep *ProjectReport) add(res *Resolution, at time.Time) {
	if rep == nil {
		return
	}
	rep.mu.Lock()
	defer rep.mu.Unlock()
	if res == nil {
		rep.none++
		return
	}
	kind, key := "existing", fmt.Sprintf("id:%d", res.ID)
	if !res.Matched {
		kind, key = "new", "src:"+res.Source
	}
	e := rep.entries[key]
	if e == nil {
		e = &reportEntry{kind: kind, id: res.ID, name: res.Name, source: res.Source, first: at, last: at}
		rep.entries[key] = e
	}
	e.count++
	if at.Before(e.first) {
		e.first = at
	}
	if at.After(e.last) {
		e.last = at
	}
}

func (rep *ProjectReport) Write(w io.Writer) {
	entries := make([]*reportEntry, 0, len(rep.entries))
	for _, e := range rep.entries {
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].kind != entries[j].kind {
			return entries[i].kind < entries[j].kind
		}
		return entries[i].count > entries[j].count
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "kind\tproject\trequests\tfirst\tlast\tseed path")
	for _, e := range entries {
		name := e.name
		if e.kind == "existing" {
			name = fmt.Sprintf("%s (#%d)", e.name, e.id)
		}
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\t%s\n", e.kind, name, e.count,
			e.first.UTC().Format(time.DateOnly), e.last.UTC().Format(time.DateOnly), e.source)
	}
	fmt.Fprintf(tw, "none\t(no workspace path)\t%d\t\t\t\n", rep.none)
	tw.Flush()
}
