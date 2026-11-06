package axonimport

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ProjectMode string

const (
	ProjectsOff        ProjectMode = "off"
	ProjectsMatch      ProjectMode = "match"
	ProjectsAutoCreate ProjectMode = "autocreate"
)

// Projects resolves a client request body to a PicoTera project under Rules:
// the client's current directory first, longest canonical path among the
// user's projects, then optionally auto-create. Projects are held in memory
// because the importer is the only writer of new historical rows while it
// runs.
type Projects struct {
	Mode   ProjectMode
	UserID int64
	Rules  *Rules
	DB     *pgxpool.Pool // nil in dry-run; auto-creation is then only simulated
	// Assign sends workspaces at or under a canonical path to a project
	// without adding the path to it: one-off decisions for this import.
	Assign map[string]int32

	mu     sync.Mutex
	byPath map[string]projectPath // canonical path -> project
	byID   map[int32]string
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

func LoadProjects(ctx context.Context, db *pgxpool.Pool, userID int64, mode ProjectMode, rules *Rules) (*Projects, error) {
	p := &Projects{
		Mode: mode, UserID: userID, Rules: rules,
		byPath: map[string]projectPath{}, byID: map[int32]string{}, names: map[string]bool{}, nextID: -1,
	}
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
		p.add(id, name, paths)
	}
	return p, rows.Err()
}

func (p *Projects) add(id int32, name string, paths []string) {
	p.names[name] = true
	p.byID[id] = name
	for _, path := range paths {
		key := p.Rules.Canonical(path)
		// MatchProjectByPaths breaks ties on the lower id.
		if cur, ok := p.byPath[key]; !ok || id < cur.id {
			p.byPath[key] = projectPath{id: id, name: name}
		}
	}
}

func (p *Projects) Resolve(ctx context.Context, body []byte) (*Resolution, error) {
	if p == nil || p.Mode == ProjectsOff {
		return nil, nil
	}
	dirs := Workdirs(body)
	if len(dirs) == 0 {
		return nil, nil
	}
	// The last mention is where the client was; earlier ones and the
	// "(was ...)" directories are history, consulted only if it misses.
	var order []string
	for i := len(dirs) - 1; i >= 0; i-- {
		order = append(order, dirs[i].Path)
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		if dirs[i].Was != "" {
			order = append(order, dirs[i].Was)
		}
	}
	current := dirs[len(dirs)-1].Path

	p.mu.Lock()
	defer p.mu.Unlock()
	for _, d := range order {
		if r := p.assigned(p.Rules.Canonical(d)); r != nil {
			return r, nil
		}
	}
	for _, d := range order {
		if r := p.match(p.Rules.Canonical(d)); r != nil {
			return r, nil
		}
	}
	seed := p.Rules.Canonical(current)
	base := Basename(seed)
	if len(seed) <= 5 || base == "" || p.Rules.Blacklist[seed] {
		return nil, nil
	}
	if p.Mode != ProjectsAutoCreate {
		return &Resolution{Source: seed, Name: base}, nil
	}
	// Until PicoTera learns the aliases, a project needs the original path
	// too, or requests from that machine keep missing it.
	paths := []string{seed}
	if raw := RepoRoot(current); raw != seed {
		paths = append(paths, raw)
	}
	return p.create(ctx, seed, base, paths)
}

func (p *Projects) assigned(key string) *Resolution {
	for prefix, id := range p.Assign {
		if key == prefix || strings.HasPrefix(key, prefix+"/") || strings.HasPrefix(key, prefix+`\`) {
			return &Resolution{ID: id, Name: p.byID[id], Matched: true}
		}
	}
	return nil
}

func (p *Projects) match(key string) *Resolution {
	for _, a := range Ancestors(key) {
		if a != key && p.Rules.Blacklist[a] {
			continue
		}
		pp, ok := p.byPath[a]
		if !ok {
			continue
		}
		return &Resolution{ID: pp.id, Name: pp.name, Matched: pp.source == "", Created: pp.source != "", Source: pp.source}
	}
	return nil
}

func (p *Projects) create(ctx context.Context, seed, base string, paths []string) (*Resolution, error) {
	name := base
	for attempt := 0; attempt < 5; attempt++ {
		if p.DB == nil {
			if !p.names[name] {
				id := p.nextID
				p.nextID--
				p.remember(id, name, seed)
				return &Resolution{ID: id, Name: name, Source: seed, Created: true}, nil
			}
		} else {
			raw, _ := json.Marshal(paths)
			var id int32
			err := p.DB.QueryRow(ctx,
				`INSERT INTO project (name, paths, auto_created, user_id) VALUES ($1, $2, true, $3) RETURNING id`,
				name, raw, p.UserID).Scan(&id)
			if err == nil {
				p.remember(id, name, seed)
				return &Resolution{ID: id, Name: name, Source: seed, Created: true}, nil
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

func (p *Projects) remember(id int32, name, seed string) {
	p.names[name] = true
	p.byID[id] = name
	p.byPath[seed] = projectPath{id: id, name: name, source: seed}
}

func jsonUnmarshalString(b []byte, s *string) error { return json.Unmarshal(b, s) }
