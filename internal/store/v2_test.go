package store

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// v2Day seeds conversations whose turns repeat earlier ones, embed shared and unique base64
// images, and include an entry with a raw NUL, as request/response pairs.
func v2Day(t *testing.T, st *memStore, day string, convs, turns int, salt int64) map[string][]byte {
	r := rand.New(rand.NewSource(salt))
	img := func(n int) string {
		b := make([]byte, n)
		r.Read(b)
		return base64.StdEncoding.EncodeToString(b)
	}
	shared := img(3000)
	want := map[string][]byte{}
	id := 0
	for c := 0; c < convs; c++ {
		history := fmt.Sprintf(`{"model":"m","system":"conv %d","messages":[`, c)
		for turn := 0; turn < turns; turn++ {
			msg := fmt.Sprintf(`{"turn":%d,"text":"%s"`, turn, img(200+r.Intn(400)))
			if turn%3 == 0 {
				msg += `,"image":"` + shared + `"`
			}
			if turn%4 == 1 {
				msg += `,"image":"` + img(2000+r.Intn(2000)) + `"`
			}
			history += msg + "},"
			req := []byte(fmt.Sprintf(`{"method":"POST","headers":{"X-Turn":["%d"]},"body":"%s]}","bodyEncoding":"utf8"}`, turn, history))
			if c == 1 && turn == 2 {
				req = append(req, 0) // a raw NUL: stored literally
			}
			resp := []byte(fmt.Sprintf(`{"statusCode":200,"body":"reply %d/%d %s","bodyEncoding":"utf8"}`, c, turn, img(100)))
			base := fmt.Sprintf("artifacts/%s/id%05d", day, id)
			id++
			for k, b := range map[string][]byte{base + ".request.json.zst": req, base + ".response.json.zst": resp} {
				st.Put(context.Background(), testBucket, k, bytes.NewReader(zstdObject(t, b)), -1, "application/json")
				want[k] = b
			}
		}
	}
	return want
}

func newV2Archiver(st store, del bool) *archiver {
	a := newTestArchiver(st, del)
	a.format, a.textFrame, a.blobFrame = "v2", 64<<10, 8<<10
	return a
}

func readAllV2(t *testing.T, st *memStore, day string) (*archiveIndex, func(key string) []byte) {
	ctx := context.Background()
	gen, _ := readCurrent(ctx, st, testArch, day)
	ix, err := readIndex(ctx, st, testArch, day, gen)
	if err != nil {
		t.Fatal(err)
	}
	frames := func(fi int) ([]byte, error) { return readRange(ctx, st, testArch, dataKey(day, gen), ix.Frames[fi]) }
	w, err := newV2Walker(ix, frames, newBlobReader(ix, frames, 2).blob)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	return ix, func(key string) []byte {
		i, ok := ix.lookup(key)
		if !ok {
			t.Fatalf("%s not in the index", key)
		}
		raw, err := w.raw(i)
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		return raw
	}
}

func TestV2RoundTrip(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	want := v2Day(t, st, testDay, 6, 20, 1)
	if err := newV2Archiver(st, true).run(ctx); err != nil {
		t.Fatal(err)
	}
	ix, get := readAllV2(t, st, testDay)
	if ix.Version != indexV2 {
		t.Fatalf("version %d", ix.Version)
	}
	var text, blob, keyframes, literal int
	for _, f := range ix.Frames {
		if f.Kind == frameText {
			text++
		} else {
			blob++
		}
	}
	for _, e := range ix.Entries {
		if e.Parent < 0 {
			keyframes++
		}
		if e.Literal {
			literal++
		}
	}
	if text < 3 || blob < 2 || literal != 1 || keyframes < 4 || keyframes == len(ix.Entries) {
		t.Fatalf("text %d, blob %d frames, %d literal, %d keyframes of %d", text, blob, literal, keyframes, len(ix.Entries))
	}
	// Random order, so the walker has to jump back and across frames.
	keys := make([]string, 0, len(want))
	for k := range want {
		keys = append(keys, k)
	}
	rand.New(rand.NewSource(2)).Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	for _, k := range keys {
		if !bytes.Equal(get(k), want[k]) {
			t.Fatalf("%s differs", k)
		}
	}
	if objs, _ := st.List(ctx, testBucket, "artifacts/"+testDay+"/"); len(objs) != 0 {
		t.Fatalf("%d loose objects left", len(objs))
	}
	// The shared image is stored once.
	n := 0
	for _, b := range ix.Blobs {
		if b.Length == int64(len(base64.StdEncoding.EncodeToString(make([]byte, 3000)))) {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("the shared image is stored %d times", n)
	}
}

func TestV2RepackKeepsOldAndTakesNew(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	want := v2Day(t, st, testDay, 4, 12, 3)
	a := newV2Archiver(st, true)
	if err := a.run(ctx); err != nil {
		t.Fatal(err)
	}
	gen1, _ := readCurrent(ctx, st, testArch, testDay)
	over := "artifacts/" + testDay + "/id00005.request.json.zst"
	want[over] = []byte(`{"rewritten":true}`)
	st.Put(ctx, testBucket, over, bytes.NewReader(zstdObject(t, want[over])), -1, "application/json")
	late := "artifacts/" + testDay + "/zz.response.json.zst"
	want[late] = []byte(`{"late":true}`)
	st.Put(ctx, testBucket, late, bytes.NewReader(zstdObject(t, want[late])), -1, "application/json")
	a.now = func() time.Time { return time.Date(2026, 1, 17, 4, 0, 0, 0, time.UTC) }
	if err := a.run(ctx); err != nil {
		t.Fatal(err)
	}
	gen2, _ := readCurrent(ctx, st, testArch, testDay)
	if gen2 == gen1 {
		t.Fatal("CURRENT did not move")
	}
	ix, get := readAllV2(t, st, testDay)
	if ix.Version != indexV2 || len(ix.Entries) != len(want) {
		t.Fatalf("version %d, %d entries, want %d", ix.Version, len(ix.Entries), len(want))
	}
	for k, b := range want {
		if !bytes.Equal(get(k), b) {
			t.Fatalf("%s differs after repack", k)
		}
	}
}

func TestV2LeavesV1DaysV1(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	seed(t, st, testDay, 10, 4)
	if err := newTestArchiver(st, false).run(ctx); err != nil { // v1
		t.Fatal(err)
	}
	extra := "artifacts/" + testDay + "/zz.request.json.zst"
	st.Put(ctx, testBucket, extra, bytes.NewReader(zstdObject(t, []byte(`{"x":1}`))), -1, "application/json")
	a := newV2Archiver(st, false)
	a.now = func() time.Time { return time.Date(2026, 1, 17, 4, 0, 0, 0, time.UTC) }
	if err := a.run(ctx); err != nil {
		t.Fatal(err)
	}
	gen, _ := readCurrent(ctx, st, testArch, testDay)
	ix, err := readIndex(ctx, st, testArch, testDay, gen)
	if err != nil || ix.Version != indexVersion || len(ix.Entries) != 11 {
		t.Fatalf("v1 day became version %d with %d entries (%v)", ix.Version, len(ix.Entries), err)
	}
}

func TestV2SkipsObjectChangedBetweenPasses(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	want := v2Day(t, st, testDay, 2, 6, 5)
	changed := "artifacts/" + testDay + "/id00003.request.json.zst"
	a := newV2Archiver(st, true)
	// Change the object after the first pass has read it: the prefetch of the second pass
	// starts once the first pass is done, so rewrite it from the first second-pass Get.
	var gets int
	a.st = &hookStore{memStore: st, onGet: func(key string) {
		if key == changed {
			gets++
			if gets == 2 {
				st.Put(ctx, testBucket, changed, bytes.NewReader(zstdObject(t, []byte(`{"new":1}`))), -1, "application/json")
			}
		}
	}}
	if err := a.run(ctx); err != nil {
		t.Fatal(err)
	}
	ix, get := readAllV2(t, st, testDay)
	if _, ok := ix.lookup(changed); ok {
		t.Fatal("an object changed between passes went into the archive")
	}
	if !st.has(testBucket, changed) {
		t.Fatal("an object changed between passes was deleted")
	}
	for k, b := range want {
		if k == changed {
			continue
		}
		if !bytes.Equal(get(k), b) {
			t.Fatalf("%s differs", k)
		}
	}
}

// hookStore runs onGet before every artifact-bucket Get.
type hookStore struct {
	*memStore
	onGet func(key string)
}

func (h *hookStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, objectInfo, error) {
	if bucket == testBucket {
		h.onGet(key)
	}
	return h.memStore.Get(ctx, bucket, key)
}

func TestV2ServedThroughProxy(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	want := v2Day(t, st, testDay, 5, 15, 7)
	if err := newV2Archiver(st, true).run(ctx); err != nil {
		t.Fatal(err)
	}
	srv, _, _ := newTestProxy(t, st)
	for k, b := range want {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			resp := do(t, method, srv.URL+"/"+testBucket+"/"+k, nil)
			if resp.StatusCode != 200 || resp.Header.Get("Content-Encoding") != "zstd" {
				t.Fatalf("%s %s: %d", method, k, resp.StatusCode)
			}
			if method == http.MethodGet {
				body, _ := io.ReadAll(resp.Body)
				if raw, _ := decodeHTTPBody(body); !bytes.Equal(raw, b) {
					t.Fatalf("%s differs through the proxy", k)
				}
			}
		}
	}
}

func TestV2PairShareOneDecode(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	want := v2Day(t, st, testDay, 3, 10, 9)
	a := newV2Archiver(st, true)
	a.textFrame = 1 << 30 // one text frame
	if err := a.run(ctx); err != nil {
		t.Fatal(err)
	}
	fb := newFallback(st, testArch, 2, 1<<30, 1<<30, 1<<30, 1<<30, 0, quiet)
	var decodes atomic.Int32
	fb.decodes = func() { decodes.Add(1) }
	var wg sync.WaitGroup
	for _, k := range []string{"id00012.request.json.zst", "id00012.response.json.zst"} {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			if e, err := fb.lookup(ctx, "artifacts/"+testDay+"/"+k); err != nil || e == nil {
				t.Errorf("%s: %v", k, err)
			}
		}(k)
	}
	wg.Wait()
	for k, b := range want {
		e, err := fb.lookup(ctx, k)
		if err != nil || e == nil {
			t.Fatalf("%s: %v", k, err)
		}
		if raw, _ := decodeHTTPBody(e.body); !bytes.Equal(raw, b) {
			t.Fatalf("%s differs", k)
		}
	}
	if n := decodes.Load(); n != 1 {
		t.Fatalf("%d decodes, want 1", n)
	}
}
