package axonimport

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"
)

func cc(dirs ...string) []byte {
	text := "# Environment\n - Primary working directory: " + dirs[0] + "\n"
	for i := 1; i+1 < len(dirs); i += 2 {
		text += "# Environment update\n - Primary working directory: " + dirs[i] + " (was " + dirs[i+1] + ")\n"
	}
	b, _ := json.Marshal(map[string]any{"system": text})
	return b
}

func testProjects(projects map[int32][2]string) *Projects {
	p := &Projects{Mode: ProjectsAutoCreate, UserID: 1, Rules: DefaultRules(), nextID: -1,
		byPath: map[string]projectPath{}, byID: map[int32]string{}, names: map[string]bool{}}
	for id, np := range projects {
		p.add(id, np[0], []string{np[1]})
	}
	return p
}

func TestCanonical(t *testing.T) {
	r := DefaultRules()
	for in, want := range map[string]string{
		"/home/alice/src/alpha/.claude/worktrees/a-b-c":     "/home/alice/src/alpha",
		"/home/alice/src/alpha/.worktrees/x/pkg":            "/home/alice/src/alpha/pkg",
		`D:\Alice\src\bravo\.claude\worktrees\x`:             "/home/alice/src/bravo",
		"/Users/alice/src/charlie":                            "/home/alice/src/charlie",
		`d:\alice\src\bravo`:                                 "/home/alice/src/bravo",
		"/Users/alice/srcs/x":                              "/Users/alice/srcs/x",
		"/home/al/src/golf":                       "/home/alice/src/golf",
		"/home/alice/sandbox/homes/bob/code/lima3": "/home/alice/code/lima3",
		"\n/home/alice/src/delta/\n":                        "/home/alice/src/delta",
	} {
		if got := r.Canonical(in); got != want {
			t.Errorf("Canonical(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestWorkdirsSplitsWasAndKeepsOrder(t *testing.T) {
	dirs := Workdirs(cc("/home/alice/src/echo",
		"/home/alice/src/echo/.claude/worktrees/wt-1", "/home/alice/src/echo",
		"/home/alice/src/echo", "/home/alice/src/echo/.claude/worktrees/wt-1"))
	if len(dirs) != 3 || dirs[1].Path != "/home/alice/src/echo/.claude/worktrees/wt-1" || dirs[1].Was != "/home/alice/src/echo" {
		t.Fatalf("got %+v", dirs)
	}
}

func TestResolveWorktreeSeedsRepoRoot(t *testing.T) {
	p := testProjects(nil)
	r, _ := p.Resolve(context.Background(), cc("/home/alice/src/echo/.claude/worktrees/wt-2"))
	if r == nil || r.Name != "echo" || r.Source != "/home/alice/src/echo" {
		t.Fatalf("got %+v", r)
	}
}

func TestResolveBlacklistedHome(t *testing.T) {
	p := testProjects(map[int32][2]string{84: {"user", "/home/alice"}})
	if r, _ := p.Resolve(context.Background(), cc("/home/alice")); r == nil || r.ID != 84 {
		t.Fatalf("home itself: got %+v", r)
	}
	r, _ := p.Resolve(context.Background(), cc("/home/alice/src/foxtrot"))
	if r == nil || r.ID == 84 || r.Name != "foxtrot" {
		t.Fatalf("repo under home: got %+v", r)
	}
	q := testProjects(nil)
	if r, _ := q.Resolve(context.Background(), cc("/home/carol")); r != nil {
		t.Fatalf("blacklisted home must not seed, got %+v", r)
	}
}

func TestResolveAliasMatchesExisting(t *testing.T) {
	p := testProjects(map[int32][2]string{9: {"bravo", "/home/alice/src/bravo"}})
	r, _ := p.Resolve(context.Background(), cc(`D:\Alice\src\bravo\src`))
	if r == nil || r.ID != 9 {
		t.Fatalf("got %+v", r)
	}
}

func TestResolvePrefersCurrentDirectory(t *testing.T) {
	p := testProjects(map[int32][2]string{1: {"a", "/home/alice/src/a"}, 2: {"bb", "/home/alice/src/bb"}})
	// Started in bb, then moved to a: the request belongs to a.
	r, _ := p.Resolve(context.Background(), cc("/home/alice/src/bb", "/home/alice/src/a", "/home/alice/src/bb"))
	if r == nil || r.ID != 1 {
		t.Fatalf("got %+v", r)
	}
}

func TestResolveAssign(t *testing.T) {
	p := testProjects(map[int32][2]string{7: {"misc", "/tmp"}})
	p.Assign = map[string]int32{"/home/alice/games": 7}
	r, _ := p.Resolve(context.Background(), cc("/home/alice/games/one"))
	if r == nil || r.ID != 7 {
		t.Fatalf("got %+v", r)
	}
	if _, ok := p.byPath["/home/alice/games"]; ok {
		t.Fatal("assign must not add the path to the project")
	}
}

// Merge planning.
func mp(id int32, name string, paths []string, auto bool) MergeProject {
	at := time.Date(2026, 10, int(id), 0, 0, 0, 0, time.UTC)
	return MergeProject{ID: id, Name: name, Paths: paths, AutoCreated: auto, FirstSeenAt: &at}
}

func TestPlanMergesWorktreeOnly(t *testing.T) {
	plans := DefaultRules().PlanMerges([]MergeProject{
		mp(3, "b", []string{"/home/alice/src/alpha/.claude/worktrees/b"}, true),
		mp(2, "a", []string{"/home/alice/src/alpha/.claude/worktrees/a"}, true),
	})
	if len(plans) != 1 {
		t.Fatalf("got %d plans", len(plans))
	}
	pl := plans[0]
	if pl.Target.ID != 2 || len(pl.Sources) != 1 || pl.Sources[0].ID != 3 || pl.Name != "alpha" ||
		!slices.Equal(pl.Paths, []string{"/home/alice/src/alpha"}) {
		t.Fatalf("got %+v", pl)
	}
}

func TestPlanMergesKeepsOtherMachinePaths(t *testing.T) {
	pl := DefaultRules().PlanMerges([]MergeProject{
		mp(1, "bravo", []string{"/home/alice/src/bravo"}, true),
		mp(2, "bravo-1a2b3c4d", []string{`D:\Alice\src\bravo`}, true),
	})[0]
	if pl.Target.ID != 1 || pl.Name != "bravo" || !slices.Equal(pl.Paths, []string{"/home/alice/src/bravo", `D:\Alice\src\bravo`}) {
		t.Fatalf("got %+v", pl)
	}
}

func TestPlanMergesPrefersHandMade(t *testing.T) {
	pl := DefaultRules().PlanMerges([]MergeProject{
		mp(1, "x", []string{"/home/alice/src/x/.worktrees/x"}, true),
		mp(9, "mine", []string{"/home/alice/src/x"}, false),
	})[0]
	if pl.Target.ID != 9 || pl.Name != "mine" {
		t.Fatalf("got %+v", pl)
	}
}

func TestPlanMergesNameTaken(t *testing.T) {
	pl := DefaultRules().PlanMerges([]MergeProject{
		mp(1, "wt", []string{"/home/alice/src/alpha/.claude/worktrees/wt"}, true),
		mp(2, "alpha", []string{"/elsewhere/alpha"}, true),
	})[0]
	if pl.Name != "wt" || len(pl.Warnings) != 1 {
		t.Fatalf("got %+v", pl)
	}
}

func TestPlanMergesDirtyPaths(t *testing.T) {
	pl := DefaultRules().PlanMerges([]MergeProject{
		mp(1, "delta", []string{"\n/home/alice/src/delta/.claude/worktrees/p\n", "/home/alice/src/delta"}, true),
	})[0]
	if !slices.Equal(pl.Paths, []string{"/home/alice/src/delta"}) {
		t.Fatalf("got %+v", pl)
	}
}

func TestPlanMergesNoopAndNoParentFolding(t *testing.T) {
	plans := DefaultRules().PlanMerges([]MergeProject{
		mp(1, "a", []string{"/home/alice/src/a"}, true),
		mp(2, "sub", []string{"/home/alice/src/a/sub"}, true),
		mp(3, "misc", []string{"/tmp", "/private/tmp"}, true),
	})
	if len(plans) != 0 {
		t.Fatalf("got %+v", plans)
	}
}

func TestRootsFoldSubdirectories(t *testing.T) {
	p := testProjects(nil)
	p.Rules.Roots = []string{"/home/alice/src/hotel"}
	r, _ := p.Resolve(context.Background(), cc("/home/alice/src/hotel/backend"))
	if r == nil || r.Name != "hotel" || r.Source != "/home/alice/src/hotel" {
		t.Fatalf("got %+v", r)
	}
	again, _ := p.Resolve(context.Background(), cc("/home/alice/src/hotel"))
	if again == nil || again.ID != r.ID {
		t.Fatalf("got %+v, want the same project", again)
	}
}
