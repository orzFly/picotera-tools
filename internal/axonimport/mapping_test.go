package axonimport

import (
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

func TestApplyUsageExcludesCache(t *testing.T) {
	var row Row
	applyUsage(&row, &Usage{PromptTokens: 17359, CompletionTokens: 166, CachedTokens: 14324, WriteCached: 3032, WriteCached1h: 3032, Currency: "USD"})
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
	if p.Body != string(body) || p.Aggregated == nil || p.Aggregated.Format != "anthropicMessages" || p.Headers.Get("Content-Type") != "application/json" {
		t.Fatalf("unexpected payload %s", raw)
	}
}

func TestInt4DropsGarbage(t *testing.T) {
	if int4(ptr(int64(-9223372036854))) != nil || int4(ptr(int64(1<<31))) != nil || int4(nil) != nil {
		t.Fatal("out-of-range values must become NULL")
	}
	if v := int4(ptr(int64(5827))); v == nil || *v != 5827 {
		t.Fatalf("got %v", v)
	}
}
