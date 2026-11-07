package axonimport

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Fold the fragments PicoTera's auto-creation leaves behind (one project per
// worktree, one per machine) back into the project of the repository.

type MergeProject struct {
	ID          int32
	Name        string
	Paths       []string
	AutoCreated bool
	FirstSeenAt *time.Time
}

type MergePlan struct {
	Target   MergeProject
	Sources  []MergeProject
	Name     string
	Paths    []string
	Warnings []string
}

func LoadMergeProjects(ctx context.Context, db *pgxpool.Pool, userID int64) ([]MergeProject, error) {
	rows, err := db.Query(ctx, `SELECT id, name, paths, auto_created, first_seen_at FROM project WHERE user_id = $1 ORDER BY id`, userID)
	if err != nil {
		return nil, fmt.Errorf("list projects: %w", err)
	}
	defer rows.Close()
	var out []MergeProject
	for rows.Next() {
		var p MergeProject
		if err := rows.Scan(&p.ID, &p.Name, &p.Paths, &p.AutoCreated, &p.FirstSeenAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

var suffixedName = regexp.MustCompile(`^(.+)-[0-9a-f]{8}$`)

// A name PicoTera derived from a worktree, possibly with its collision suffix.
func hasWorktreeName(p MergeProject) bool {
	if !p.AutoCreated {
		return false
	}
	names := map[string]bool{}
	for _, path := range p.Paths {
		if n := WorktreeName(path); n != "" {
			names[n] = true
		}
	}
	if names[p.Name] {
		return true
	}
	m := suffixedName.FindStringSubmatch(p.Name)
	return m != nil && names[m[1]]
}

func isWorktreeOnly(p MergeProject) bool {
	if len(p.Paths) == 0 {
		return false
	}
	for _, path := range p.Paths {
		if WorktreeName(path) == "" {
			return false
		}
	}
	return true
}

func pickTarget(group []MergeProject) MergeProject {
	b2i := func(b bool) int {
		if b {
			return 1
		}
		return 0
	}
	seen := func(p MergeProject) string {
		if p.FirstSeenAt == nil {
			return "9999"
		}
		return p.FirstSeenAt.UTC().Format(time.RFC3339Nano)
	}
	sorted := slices.Clone(group)
	sort.SliceStable(sorted, func(i, j int) bool {
		a, b := sorted[i], sorted[j]
		// Hand-made projects keep their human-chosen names.
		if x, y := b2i(a.AutoCreated), b2i(b.AutoCreated); x != y {
			return x < y
		}
		if x, y := b2i(isWorktreeOnly(a)), b2i(isWorktreeOnly(b)); x != y {
			return x < y
		}
		if x, y := seen(a), seen(b); x != y {
			return x < y
		}
		return a.ID < b.ID
	})
	return sorted[0]
}

// PlanMerges groups projects that share any canonical path and plans folding
// each group into one. Pure: it only reads the given projects.
func (r *Rules) PlanMerges(projects []MergeProject) []MergePlan {
	parent := map[int32]int32{}
	for _, p := range projects {
		parent[p.ID] = p.ID
	}
	var find func(int32) int32
	find = func(id int32) int32 {
		for parent[id] != id {
			id = parent[id]
		}
		return id
	}
	owner := map[string]int32{}
	for _, p := range projects {
		for _, path := range p.Paths {
			key := r.Canonical(path)
			if key == "" {
				continue
			}
			if o, ok := owner[key]; ok {
				parent[find(p.ID)] = find(o)
			} else {
				owner[key] = p.ID
			}
		}
	}
	groups := map[int32][]MergeProject{}
	var roots []int32
	for _, p := range projects {
		root := find(p.ID)
		if _, ok := groups[root]; !ok {
			roots = append(roots, root)
		}
		groups[root] = append(groups[root], p)
	}

	var plans []MergePlan
	for _, root := range roots {
		group := groups[root]
		target := pickTarget(group)
		inGroup := map[int32]bool{}
		var sources []MergeProject
		var all []string
		for _, p := range group {
			inGroup[p.ID] = true
			if p.ID != target.ID {
				sources = append(sources, p)
			}
			all = append(all, p.Paths...)
		}
		sort.Slice(sources, func(i, j int) bool { return sources[i].ID < sources[j].ID })

		// Canonical keys first so the main path leads in the UI. Worktree
		// paths are covered by the repository root; requests were rewritten
		// at merge time, so dropping them loses no history. The original
		// roots stay because PicoTera does not know the aliases yet.
		var paths []string
		seenPath := map[string]bool{}
		for _, f := range []func(string) string{r.Canonical, RepoRoot} {
			for _, p := range all {
				if v := f(p); v != "" && !seenPath[v] {
					seenPath[v] = true
					paths = append(paths, v)
				}
			}
		}

		var warnings []string
		name := target.Name
		if hasWorktreeName(target) {
			var keys []string
			for _, p := range all {
				if k := r.Canonical(p); k != "" {
					keys = append(keys, k)
				}
			}
			primary := ""
			for _, k := range keys {
				if r.MainRoot != "" && strings.HasPrefix(k, r.MainRoot+"/") {
					primary = k
					break
				}
			}
			if primary == "" && len(keys) > 0 {
				primary = keys[0]
			}
			if want := Basename(primary); want != "" {
				taken := false
				for _, p := range projects {
					if p.Name == want && !inGroup[p.ID] {
						taken = true
					}
				}
				if taken {
					warnings = append(warnings, fmt.Sprintf("wanted to rename to %q, but a project outside this group has that name; keeping the old name", want))
				} else {
					name = want
				}
			}
		}

		if len(sources) == 0 && name == target.Name && sameSet(paths, target.Paths) {
			continue
		}
		plans = append(plans, MergePlan{Target: target, Sources: sources, Name: name, Paths: paths, Warnings: warnings})
	}
	sort.Slice(plans, func(i, j int) bool { return plans[i].Target.Name < plans[j].Target.Name })
	return plans
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for _, x := range a {
		if !slices.Contains(b, x) {
			return false
		}
	}
	return true
}

func ShowMergePlans(w io.Writer, plans []MergePlan) {
	merged := 0
	for _, pl := range plans {
		rename := ""
		if pl.Name != pl.Target.Name {
			rename = " rename -> " + pl.Name
		}
		fmt.Fprintf(w, "#%d %s%s\n", pl.Target.ID, pl.Target.Name, rename)
		for _, s := range pl.Sources {
			fmt.Fprintf(w, "  <- #%d %s  %s\n", s.ID, s.Name, strings.Join(s.Paths, "  "))
		}
		for _, p := range pl.Paths {
			mark := " "
			if !slices.Contains(pl.Target.Paths, p) {
				mark = "+"
			}
			fmt.Fprintf(w, "  %s %s\n", mark, p)
		}
		var old []string
		old = append(old, pl.Target.Paths...)
		for _, s := range pl.Sources {
			old = append(old, s.Paths...)
		}
		dropped := map[string]bool{}
		for _, p := range old {
			if !slices.Contains(pl.Paths, p) && !dropped[p] {
				dropped[p] = true
				fmt.Fprintf(w, "  - %s\n", p)
			}
		}
		for _, warn := range pl.Warnings {
			fmt.Fprintf(w, "  ! %s\n", warn)
		}
		merged += len(pl.Sources)
	}
	if len(plans) == 0 {
		fmt.Fprintln(w, "nothing to merge")
		return
	}
	fmt.Fprintf(w, "\n%d projects change, %d merged away\n", len(plans), merged)
}

// ApplyMergePlans does what PicoTera's POST /projects/merge does (handle_project.go
// handleMergeProject), then sets the planned name and paths, one plan per
// transaction. The merge endpoint leaves continuous aggregates holding the
// source id outside their 35-day refresh window, so they are refreshed in
// full at the end.
func ApplyMergePlans(ctx context.Context, db *pgxpool.Pool, userID int64, plans []MergePlan, w io.Writer) error {
	for _, pl := range plans {
		err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
			for _, s := range pl.Sources {
				if _, err := tx.Exec(ctx, `
					UPDATE project AS p
					SET paths = COALESCE((
					      SELECT jsonb_agg(DISTINCT elem) FROM (
					        SELECT jsonb_array_elements_text(p.paths) AS elem
					        UNION
					        SELECT jsonb_array_elements_text(src.paths) FROM project AS src WHERE src.id = $1
					      ) all_paths), p.paths),
					    first_seen_at = LEAST(p.first_seen_at, (SELECT first_seen_at FROM project WHERE id = $1)),
					    last_seen_at  = GREATEST(p.last_seen_at, (SELECT last_seen_at FROM project WHERE id = $1)),
					    updated_at = now()
					WHERE p.id = $2 AND p.user_id = $3`, s.ID, pl.Target.ID, userID); err != nil {
					return fmt.Errorf("merge #%d into #%d: %w", s.ID, pl.Target.ID, err)
				}
				tag, err := tx.Exec(ctx, `UPDATE request SET project_id = $1 WHERE project_id = $2`, pl.Target.ID, s.ID)
				if err != nil {
					return fmt.Errorf("reassign requests of #%d: %w", s.ID, err)
				}
				if _, err := tx.Exec(ctx, `DELETE FROM project WHERE id = $1 AND user_id = $2`, s.ID, userID); err != nil {
					return fmt.Errorf("delete #%d: %w", s.ID, err)
				}
				fmt.Fprintf(w, "merged  #%d %s -> #%d (%d request rows)\n", s.ID, s.Name, pl.Target.ID, tag.RowsAffected())
			}
			paths, _ := json.Marshal(pl.Paths)
			if _, err := tx.Exec(ctx, `UPDATE project SET name = $1, paths = $2, updated_at = now() WHERE id = $3 AND user_id = $4`,
				pl.Name, paths, pl.Target.ID, userID); err != nil {
				return fmt.Errorf("update #%d: %w", pl.Target.ID, err)
			}
			fmt.Fprintf(w, "updated #%d %s\n", pl.Target.ID, pl.Name)
			return nil
		})
		if err != nil {
			return err
		}
	}
	if len(plans) == 0 {
		return nil
	}
	for _, view := range []string{"request_overview_bucketed", "request_outcome_bucketed"} {
		if _, err := db.Exec(ctx, `CALL refresh_continuous_aggregate($1::regclass, NULL, NULL)`, view); err != nil {
			return fmt.Errorf("refresh %s: %w", view, err)
		}
	}
	return nil
}
