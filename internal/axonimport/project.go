package axonimport

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/orzfly/picotera-tools/internal/picotera"
)

type ProjectMode string

const (
	ProjectsOff        ProjectMode = "off"
	ProjectsMatch      ProjectMode = "match"
	ProjectsAutoCreate ProjectMode = "autocreate"
)

// Projects resolves a client request body to a PicoTera project the way the
// gateway does: longest matching path among the user's projects, then
// optionally auto-create. Projects are held in memory because the importer
// is the only writer of new historical rows while it runs.
type Projects struct {
	Mode   ProjectMode
	UserID int64
	DB     *pgxpool.Pool // nil in dry-run; auto-creation is then only simulated
	// ExactOnly paths match only a workspace path equal to them, never as an
	// ancestor. PicoTera auto-created a project for /home/alice once, and
	// ancestor matching then swallowed every repo under it.
	ExactOnly map[string]bool

	mu     sync.Mutex
	byPath map[string]projectPath
	names  map[string]bool
	nextID int32 // simulated ids for dry-run auto-creation, counting down from -1
}

type projectPath struct {
	id     int32
	name   string
	source string // set for projects created during this run
}

type Resolution struct {
	ID      int32
	Name    string
	Matched bool   // a project that existed before this run matched
	Source  string // the path a new project was (or would be) seeded with
	Created bool   // the project was (or, with negative ID, would be) created by this run
}

func LoadProjects(ctx context.Context, db *pgxpool.Pool, userID int64, mode ProjectMode) (*Projects, error) {
	p := &Projects{Mode: mode, UserID: userID, byPath: map[string]projectPath{}, names: map[string]bool{}, nextID: -1}
	rows, err := db.Query(ctx, `SELECT id, name, paths FROM project WHERE user_id = $1`, userID)
	if err != nil {
		return nil, fmt.Errorf("list projects: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int32
		var name string
		var paths []string
		if err := rows.Scan(&id, &name, &paths); err != nil {
			return nil, err
		}
		p.names[name] = true
		for _, path := range paths {
			// MatchProjectByPaths breaks ties on the lower id.
			if cur, ok := p.byPath[path]; !ok || id < cur.id {
				p.byPath[path] = projectPath{id: id, name: name}
			}
		}
	}
	return p, rows.Err()
}

func (p *Projects) Resolve(ctx context.Context, body []byte) (*Resolution, error) {
	if p == nil || p.Mode == ProjectsOff {
		return nil, nil
	}
	paths := picotera.ProjectPaths(body)
	if len(paths) == 0 {
		return nil, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if r := p.match(paths); r != nil {
		return r, nil
	}
	source := picotera.AutoCreateSource(paths)
	base := picotera.LastPathComponent(source)
	if source == "" || base == "" {
		return nil, nil
	}
	if p.Mode != ProjectsAutoCreate {
		return &Resolution{Source: source, Name: base}, nil
	}
	return p.create(ctx, source, base)
}

func (p *Projects) match(paths []string) *Resolution {
	var best *Resolution
	bestLen := -1
	for _, path := range paths {
		for _, a := range picotera.AncestorPaths(path) {
			pp, ok := p.byPath[a]
			if !ok || (a != path && p.ExactOnly[a]) {
				continue
			}
			if len(a) > bestLen || (len(a) == bestLen && pp.id < best.ID) {
				best = &Resolution{ID: pp.id, Name: pp.name, Matched: pp.source == "", Created: pp.source != "", Source: pp.source}
				bestLen = len(a)
			}
		}
	}
	return best
}

func (p *Projects) create(ctx context.Context, source, base string) (*Resolution, error) {
	name := base
	for attempt := 0; attempt < 5; attempt++ {
		if p.DB == nil {
			if !p.names[name] {
				id := p.nextID
				p.nextID--
				p.remember(id, name, source)
				return &Resolution{ID: id, Name: name, Source: source, Created: true}, nil
			}
		} else {
			paths, _ := json.Marshal([]string{source})
			var id int32
			err := p.DB.QueryRow(ctx,
				`INSERT INTO project (name, paths, auto_created, user_id) VALUES ($1, $2, true, $3) RETURNING id`,
				name, paths, p.UserID).Scan(&id)
			if err == nil {
				p.remember(id, name, source)
				return &Resolution{ID: id, Name: name, Source: source, Created: true}, nil
			}
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
				return nil, fmt.Errorf("create project %q: %w", name, err)
			}
		}
		var b [4]byte
		_, _ = rand.Read(b[:])
		name = base + "-" + hex.EncodeToString(b[:])
	}
	return nil, nil
}

func (p *Projects) remember(id int32, name, source string) {
	p.names[name] = true
	p.byPath[source] = projectPath{id: id, name: name, source: source}
}
