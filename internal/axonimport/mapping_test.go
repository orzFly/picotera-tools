package axonimport

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/orzfly/picotera-tools/internal/picotera"
)

func TestDeterministicID(t *testing.T) {
	at := time.Date(2026, 6, 26, 14, 21, 19, 987654321, time.UTC)
	a, err := deterministicID(at, kindMeta, 250008)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := deterministicID(at, kindMeta, 250008)
	if a != b {
		t.Fatalf("same input gave %s and %s", a, b)
	}
	if !a.Time().Equal(at.Truncate(time.Second)) {
		t.Fatalf("xid time %s, want %s", a.Time(), at.Truncate(time.Second))
	}
	up, _ := deterministicID(at, kindUpstream, 250008)
	if up == a {
		t.Fatal("meta and upstream ids collide")
	}
	if _, err := deterministicID(at, kindMeta, 1<<24); err == nil {
		t.Fatal("expected overflow error")
	}
}

func TestProjectsLongestMatchAndSimulatedCreate(t *testing.T) {
	p := &Projects{Mode: ProjectsAutoCreate, UserID: 1, nextID: -1, names: map[string]bool{"charlie": true, "homelab": true}, byPath: map[string]projectPath{
		"/home/u/Git/charlie":         {id: 1, name: "charlie"},
		"/home/u/Git/charlie/homelab": {id: 3, name: "homelab"},
	}}
	body := func(cwd string) []byte {
		b, _ := json.Marshal(map[string]any{"system": "env\nPrimary working directory: " + cwd + "\nmore"})
		return b
	}
	r, _ := p.Resolve(context.Background(), body("/home/u/Git/charlie/homelab/docs"))
	if r == nil || r.ID != 3 || !r.Matched {
		t.Fatalf("got %+v, want homelab", r)
	}
	r, _ = p.Resolve(context.Background(), body("/home/u/Git/old/homelab"))
	if r == nil || r.Matched || !r.Created || r.ID >= 0 || r.Name == "homelab" {
		t.Fatalf("got %+v, want a simulated project with a suffixed name", r)
	}
	again, _ := p.Resolve(context.Background(), body("/home/u/Git/old/homelab/sub"))
	if again == nil || again.ID != r.ID || again.Matched {
		t.Fatalf("got %+v, want the project created above", again)
	}
}

func TestApplyUsageExcludesCache(t *testing.T) {
	var row Row
	applyUsage(&row, &Usage{PromptTokens: 17359, CompletionTokens: 166, CachedTokens: 14324, WriteCached: 3032, WriteCached1h: 3032})
	if *row.InputTokens != 3 || *row.CacheReadTokens != 14324 || *row.CacheWriteTokens != 0 || *row.CacheWrite1hTokens != 3032 {
		t.Fatalf("got input=%d read=%d write=%d write1h=%d", *row.InputTokens, *row.CacheReadTokens, *row.CacheWriteTokens, *row.CacheWrite1hTokens)
	}
}

func TestStreamResponseUsesAggregated(t *testing.T) {
	body := []byte(`{"id": "msg_1", "type": "message", "content": []}`)
	data, err := buildResponse(ptr(int64(200)), true, picotera.FormatAnthropicMessages, body, nil)
	if err != nil {
		t.Fatal(err)
	}
	dec, _ := zstd.NewReader(nil)
	raw, err := dec.DecodeAll(data, nil)
	if err != nil {
		t.Fatal(err)
	}
	var p picotera.Payload
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	if p.Body != "" || p.Aggregated == nil || p.Aggregated.Format != "anthropicMessages" || p.Headers.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("unexpected payload %s", raw)
	}
}

func TestExactOnlyHomeDir(t *testing.T) {
	p := &Projects{Mode: ProjectsAutoCreate, UserID: 1, nextID: -1, names: map[string]bool{"user": true},
		byPath:    map[string]projectPath{"/home/alice": {id: 84, name: "user"}},
		ExactOnly: map[string]bool{"/home/alice": true}}
	body := func(cwd string) []byte {
		b, _ := json.Marshal(map[string]any{"system": "Primary working directory: " + cwd + "\n"})
		return b
	}
	if r, _ := p.Resolve(context.Background(), body("/home/alice")); r == nil || r.ID != 84 {
		t.Fatalf("home dir itself: got %+v, want user", r)
	}
	r, _ := p.Resolve(context.Background(), body("/home/alice/src/foxtrot"))
	if r == nil || r.ID == 84 || !r.Created || r.Name != "foxtrot" {
		t.Fatalf("repo under home: got %+v, want new foxtrot", r)
	}
}
