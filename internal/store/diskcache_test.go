package store

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// countingStore counts reads that reach S3.
type countingStore struct {
	*memStore
	gets, ranges int
}

func (c *countingStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, objectInfo, error) {
	c.gets++
	return c.memStore.Get(ctx, bucket, key)
}

func (c *countingStore) GetRange(ctx context.Context, bucket, key string, off, n int64) (io.ReadCloser, error) {
	c.ranges++
	return c.memStore.GetRange(ctx, bucket, key, off, n)
}

func readRangeOf(t *testing.T, st store, key string, off, n int64) []byte {
	rc, err := st.GetRange(context.Background(), testArch, key, off, n)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rc)
	return b
}

func TestDiskCacheHitsEvictsAndRestarts(t *testing.T) {
	ctx := context.Background()
	mem := newMemStore()
	data := bytes.Repeat([]byte("0123456789"), 100) // 1000 bytes
	mem.Put(ctx, testArch, "d/g.data", bytes.NewReader(data), -1, "")
	mem.Put(ctx, testArch, "d/CURRENT", strings.NewReader("g\n"), -1, "")
	inner := &countingStore{memStore: mem}
	dir := t.TempDir()
	c, err := newDiskCache(inner, dir, testArch, 250)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if got := readRangeOf(t, c, "d/g.data", 0, 100); !bytes.Equal(got, data[:100]) {
			t.Fatal("wrong bytes")
		}
	}
	if inner.ranges != 1 {
		t.Fatalf("%d reads reached S3, want 1", inner.ranges)
	}
	// CURRENT is never cached.
	for i := 0; i < 2; i++ {
		rc, _, _ := c.Get(ctx, testArch, "d/CURRENT")
		rc.Close()
	}
	if inner.gets != 2 {
		t.Fatalf("CURRENT read %d times, want 2", inner.gets)
	}
	// 100 + 100 fit in 250; touching the first, then adding a third evicts the second.
	readRangeOf(t, c, "d/g.data", 100, 100)
	time.Sleep(10 * time.Millisecond)
	readRangeOf(t, c, "d/g.data", 0, 100)
	readRangeOf(t, c, "d/g.data", 200, 100)
	if c.size() > 250 {
		t.Fatalf("cache holds %d bytes over a 250 limit", c.size())
	}
	before := inner.ranges
	readRangeOf(t, c, "d/g.data", 0, 100)
	if inner.ranges != before {
		t.Fatal("the recently used range was evicted")
	}
	readRangeOf(t, c, "d/g.data", 100, 100)
	if inner.ranges != before+1 {
		t.Fatal("the least recently used range survived")
	}

	// A restart keeps what is on disk and drops half-written files.
	os.WriteFile(filepath.Join(dir, "leftover.123.tmp"), []byte("x"), 0o600)
	c2, err := newDiskCache(inner, dir, testArch, 250)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "leftover.123.tmp")); !os.IsNotExist(err) {
		t.Fatal("a temporary file survived the restart")
	}
	before = inner.ranges
	readRangeOf(t, c2, "d/g.data", 100, 100)
	if inner.ranges != before {
		t.Fatal("a range cached before the restart was read from S3 again")
	}

	// A file of the wrong length is a miss, not data.
	name := c2.name("d/g.data", 100, 100)
	os.WriteFile(filepath.Join(dir, name), []byte("short"), 0o600)
	if got := readRangeOf(t, c2, "d/g.data", 100, 100); !bytes.Equal(got, data[100:200]) {
		t.Fatal("a truncated cache file was served")
	}
}

func TestNoDiskCacheWithoutPath(t *testing.T) {
	var c commonFlags
	c.archBucket = testArch
	st, err := c.archiveStore(newMemStore())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := st.(*diskCache); ok {
		t.Fatal("a disk cache exists without --disk-cache")
	}
}

func TestV2ThroughDiskCache(t *testing.T) {
	ctx := context.Background()
	mem := newMemStore()
	want := v2Day(t, mem, testDay, 3, 8, 11)
	if err := newV2Archiver(mem, true).run(ctx); err != nil {
		t.Fatal(err)
	}
	inner := &countingStore{memStore: mem}
	dc, err := newDiskCache(inner, t.TempDir(), testArch, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 2; round++ {
		// A fresh fallback each round: its memory caches are empty, the disk is not.
		fb := newFallback(dc, testArch, 2, 1<<30, 1<<30, 0, 0, 0, quiet)
		for k, b := range want {
			e, err := fb.lookup(ctx, k)
			if err != nil || e == nil {
				t.Fatalf("%s: %v", k, err)
			}
			if raw, _ := decodeHTTPBody(e.body); !bytes.Equal(raw, b) {
				t.Fatalf("%s differs", k)
			}
		}
		if round == 0 {
			inner.ranges = 0
		}
	}
	if inner.ranges != 0 {
		t.Fatalf("%d frame reads reached S3 on the second round", inner.ranges)
	}
}

func TestParseSize(t *testing.T) {
	for in, want := range map[string]int64{"1024": 1024, "2KiB": 2048, "512MiB": 512 << 20, "1GiB": 1 << 30, "5B": 5} {
		if got, err := parseSize(in); err != nil || got != want {
			t.Errorf("%s: %d %v", in, got, err)
		}
	}
	for _, bad := range []string{"", "1G", "-1MiB", "x"} {
		if _, err := parseSize(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
