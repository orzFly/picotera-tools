package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	testBucket = "picotera-artifacts"
	testArch   = "picotera-archives"
	testDay    = "2026-01-10"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// conversation mimics what makes artifacts compress: each request repeats the one before it.
func conversation(r *rand.Rand, n int) [][]byte {
	var out [][]byte
	history := []byte(`{"model":"m","messages":[`)
	for i := 0; i < n; i++ {
		turn := make([]byte, 2000+r.Intn(3000))
		for j := range turn {
			turn[j] = "abcdefghij"[r.Intn(10)]
		}
		history = append(history, []byte(fmt.Sprintf(`{"turn":%d,"text":"%s"},`, i, turn))...)
		out = append(out, append(append([]byte{}, history...), ']', '}'))
	}
	return out
}

func zstdObject(t testing.TB, raw []byte) []byte {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	return enc.EncodeAll(raw, nil)
}

// seed puts n loose objects for day into st and returns their raw bodies by key.
func seed(t testing.TB, st *memStore, day string, n int, salt int64) map[string][]byte {
	r := rand.New(rand.NewSource(salt))
	want := map[string][]byte{}
	for i, body := range conversation(r, n) {
		key := fmt.Sprintf("artifacts/%s/id%04d.request.json.zst", day, i)
		st.Put(context.Background(), testBucket, key, bytes.NewReader(zstdObject(t, body)), -1, "application/json")
		want[key] = body
	}
	return want
}

func newTestArchiver(st store, del bool) *archiver {
	return &archiver{
		st: st, bucket: testBucket, archBucket: testArch, minAge: 2, del: del,
		frameSize: 64 << 10, windowLog: defaultWindowLog, level: zstd.SpeedDefault,
		fetchers: 4, now: func() time.Time { return time.Date(2026, 1, 16, 4, 0, 0, 0, time.UTC) }, log: quiet,
	}
}

func TestFormatRoundTrip(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	bodies := conversation(r, 60)
	ix := &archiveIndex{Version: indexVersion, Day: testDay, Generation: "g", WindowLog: defaultWindowLog}
	var data bytes.Buffer
	fw, err := newFrameWriter(&data, ix, 64<<10, zstd.SpeedDefault)
	if err != nil {
		t.Fatal(err)
	}
	for i, b := range bodies {
		if err := fw.add(fmt.Sprintf("k%03d", i), "application/json", "", 1, b); err != nil {
			t.Fatal(err)
		}
	}
	if err := fw.close(); err != nil {
		t.Fatal(err)
	}
	if len(ix.Frames) < 3 {
		t.Fatalf("want several frames, got %d", len(ix.Frames))
	}
	enc, _ := encodeIndex(ix)
	ix2, err := decodeIndex(enc)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := newFrameDecoder(ix2.WindowLog)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	got := 0
	for fi, fr := range ix2.Frames {
		err := walkFrame(dec, bytes.NewReader(data.Bytes()[fr.Offset:fr.Offset+fr.Length]), ix2, fi, func(i int, raw []byte) (bool, error) {
			got++
			if !bytes.Equal(raw, bodies[i]) {
				t.Errorf("entry %d differs", i)
			}
			return true, checkEntry(&ix2.Entries[i], raw)
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if got != len(bodies) {
		t.Fatalf("read %d entries, want %d", got, len(bodies))
	}
}

func TestDecoderRefusesWiderWindow(t *testing.T) {
	big := make([]byte, 1<<20)
	enc, _ := zstd.NewWriter(nil, zstd.WithWindowSize(1<<20), zstd.WithSingleSegment(false))
	frame := enc.EncodeAll(big, nil)
	enc.Close()
	dec, _ := newFrameDecoder(17)
	defer dec.Close()
	if err := dec.Reset(bytes.NewReader(frame)); err == nil {
		if _, err := io.ReadAll(dec); err == nil {
			t.Fatal("a 1 MiB window decoded under a 128 KiB limit")
		}
	}
}

func TestEncodeForHTTPWindow(t *testing.T) {
	raw := bytes.Repeat([]byte("x"), 20<<20)
	body, err := encodeForHTTP(raw)
	if err != nil {
		t.Fatal(err)
	}
	var h zstd.Header
	if err := h.Decode(body); err != nil {
		t.Fatal(err)
	}
	if h.SingleSegment || h.WindowSize > 8<<20 {
		t.Fatalf("browsers refuse this frame: single segment %v, window %d", h.SingleSegment, h.WindowSize)
	}
}

func TestArchiveAndRepack(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	want := seed(t, st, testDay, 40, 1)
	seed(t, st, "2026-01-15", 3, 9) // too recent, must stay untouched

	a := newTestArchiver(st, true)
	if err := a.run(ctx); err != nil {
		t.Fatal(err)
	}
	gen1, _ := readCurrent(ctx, st, testArch, testDay)
	if gen1 == "" {
		t.Fatal("no CURRENT after the first run")
	}
	if objs, _ := st.List(ctx, testBucket, "artifacts/"+testDay+"/"); len(objs) != 0 {
		t.Fatalf("%d loose objects left", len(objs))
	}
	if objs, _ := st.List(ctx, testBucket, "artifacts/2026-01-15/"); len(objs) != 3 {
		t.Fatal("a recent day was touched")
	}
	fb := newFallback(st, testArch, 2, 1<<30, 1<<30, 1<<30, 1<<30, 0, quiet)
	for k, body := range want {
		e, err := fb.lookup(ctx, k)
		if err != nil || e == nil {
			t.Fatalf("%s: %v %v", k, e, err)
		}
		if raw, _ := decodeHTTPBody(e.body); !bytes.Equal(raw, body) {
			t.Fatalf("%s differs", k)
		}
	}

	// A backdated write: one key overwritten, one new.
	over := "artifacts/" + testDay + "/id0003.request.json.zst"
	want[over] = []byte(`{"rewritten":true}`)
	st.Put(ctx, testBucket, over, bytes.NewReader(zstdObject(t, want[over])), -1, "application/json")
	added := "artifacts/" + testDay + "/zz-late.response.json.zst"
	want[added] = []byte(`{"late":true}`)
	st.Put(ctx, testBucket, added, bytes.NewReader(zstdObject(t, want[added])), -1, "application/json")

	a.now = func() time.Time { return time.Date(2026, 1, 17, 4, 0, 0, 0, time.UTC) }
	if err := a.run(ctx); err != nil {
		t.Fatal(err)
	}
	gen2, _ := readCurrent(ctx, st, testArch, testDay)
	if gen2 == gen1 {
		t.Fatal("CURRENT did not move")
	}
	if !st.has(testArch, dataKey(testDay, gen1)) {
		t.Fatal("the previous generation went in the same run that replaced it")
	}
	fb = newFallback(st, testArch, 2, 1<<30, 1<<30, 1<<30, 1<<30, 0, quiet)
	for k, body := range want {
		e, err := fb.lookup(ctx, k)
		if err != nil || e == nil {
			t.Fatalf("%s: %v %v", k, e, err)
		}
		if raw, _ := decodeHTTPBody(e.body); !bytes.Equal(raw, body) {
			t.Fatalf("%s: repack lost the newer object", k)
		}
	}

	// The next run, with nothing loose, retires the old generation.
	a.now = func() time.Time { return time.Date(2026, 1, 18, 4, 0, 0, 0, time.UTC) }
	if err := a.run(ctx); err != nil {
		t.Fatal(err)
	}
	if st.has(testArch, dataKey(testDay, gen1)) || st.has(testArch, indexKey(testDay, gen1)) {
		t.Fatal("old generation survived the next run")
	}
	if !st.has(testArch, dataKey(testDay, gen2)) {
		t.Fatal("current generation removed")
	}
}

func TestArchiveWithoutDeleteKeepsLoose(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	seed(t, st, testDay, 10, 2)
	if err := newTestArchiver(st, false).run(ctx); err != nil {
		t.Fatal(err)
	}
	if objs, _ := st.List(ctx, testBucket, "artifacts/"+testDay+"/"); len(objs) != 10 {
		t.Fatalf("without --delete %d loose objects remain, want 10", len(objs))
	}
	if gen, _ := readCurrent(ctx, st, testArch, testDay); gen == "" {
		t.Fatal("no CURRENT")
	}
}

func TestVerifyFailureLeavesEverything(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	seed(t, st, testDay, 10, 3)
	st.corrupt = func(key string) bool { return strings.HasSuffix(key, ".data") }
	if err := newTestArchiver(st, true).run(ctx); err == nil {
		t.Fatal("a corrupted archive passed")
	}
	if gen, _ := readCurrent(ctx, st, testArch, testDay); gen != "" {
		t.Fatal("CURRENT points at a corrupted generation")
	}
	if objs, _ := st.List(ctx, testBucket, "artifacts/"+testDay+"/"); len(objs) != 10 {
		t.Fatal("loose objects deleted after a failed verify")
	}
	if objs, _ := st.List(ctx, testArch, testDay+"/"); len(objs) != 0 {
		t.Fatalf("failed generation left behind: %v", objs)
	}
}

func TestUndecodableObjectStaysLoose(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	seed(t, st, testDay, 5, 4)
	bad := "artifacts/" + testDay + "/broken.request.json.zst"
	st.Put(ctx, testBucket, bad, strings.NewReader("not zstd"), -1, "application/json")
	if err := newTestArchiver(st, true).run(ctx); err != nil {
		t.Fatal(err)
	}
	if !st.has(testBucket, bad) {
		t.Fatal("an object that was never archived got deleted")
	}
	if objs, _ := st.List(ctx, testBucket, "artifacts/"+testDay+"/"); len(objs) != 1 {
		t.Fatalf("%d loose objects remain, want only the broken one", len(objs))
	}
}

// fakeMinIO answers like MinIO: 404 NoSuchKey for keys it lacks, 403 when told. As measured on
// MinIO RELEASE.2024-03-10: HEAD names the error only in X-Minio-Error-Code, GET only in the XML body.
type fakeMinIO struct {
	mu       sync.Mutex
	objects  map[string][]byte
	denied   map[string]string // path → error code
	requests []*http.Request
}

func (f *fakeMinIO) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests = append(f.requests, r.Clone(context.Background()))
	body, ok := f.objects[r.URL.Path]
	code := f.denied[r.URL.Path]
	f.mu.Unlock()
	fail := func(status int, code string) {
		if r.Method == http.MethodHead {
			w.Header().Set("X-Minio-Error-Code", code)
			w.WriteHeader(status)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>`+"\n"+`<Error><Code>%s</Code><Message>m</Message></Error>`, code)
	}
	if code != "" {
		fail(http.StatusForbidden, code)
		return
	}
	if !ok {
		fail(http.StatusNotFound, "NoSuchKey")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Encoding", "zstd")
	w.Write(body)
}

func (f *fakeMinIO) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

func newTestProxy(t *testing.T, st store) (*httptest.Server, *fakeMinIO, *fallback) {
	up := &fakeMinIO{objects: map[string][]byte{}, denied: map[string]string{}}
	upSrv := httptest.NewServer(up)
	t.Cleanup(upSrv.Close)
	u, _ := url.Parse(upSrv.URL)
	fb := newFallback(st, testArch, 2, 1<<30, 1<<30, 1<<30, 1<<30, 0, quiet)
	srv := httptest.NewServer(newServer(u, testBucket, fb, quiet))
	t.Cleanup(srv.Close)
	return srv, up, fb
}

func do(t *testing.T, method, rawURL string, hdr map[string]string) *http.Response {
	req, _ := http.NewRequest(method, rawURL, nil)
	req.Host = "dashboard.example.com"
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	// Keep the zstd body as sent; the browser is what decodes it.
	tr := &http.Transport{DisableCompression: true}
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestRefusedRequestsNeverReachUpstream(t *testing.T) {
	srv, up, _ := newTestProxy(t, newMemStore())
	ok := "/" + testBucket + "/artifacts/" + testDay + "/id.request.json.zst"
	for _, c := range []struct{ method, path string }{
		{http.MethodPut, ok},
		{http.MethodDelete, ok},
		{http.MethodPost, ok},
		{http.MethodGet, "/" + testBucket + "/"},
		{http.MethodGet, "/" + testBucket + "?list-type=2"},
		{http.MethodGet, "/"},
		{http.MethodGet, "/harbor-alpha/artifacts/" + testDay + "/id.request.json.zst"},
		{http.MethodGet, "/" + testBucket + "/artifacts/" + testDay + "/../x"},
		{http.MethodGet, "/" + testBucket + "/artifacts/" + testDay + "/%2e%2e"},
		{http.MethodGet, "/" + testBucket + "/other/" + testDay + "/id"},
		{http.MethodGet, "/" + testBucket + "/artifacts/2026-13-45/id"},
	} {
		resp := do(t, c.method, srv.URL+c.path, nil)
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s %s: %d, want 403", c.method, c.path, resp.StatusCode)
		}
	}
	if n := up.count(); n != 0 {
		t.Fatalf("upstream saw %d refused requests", n)
	}
}

func TestForwardKeepsHostDropsForwarded(t *testing.T) {
	srv, up, _ := newTestProxy(t, newMemStore())
	path := "/" + testBucket + "/artifacts/" + testDay + "/id.request.json.zst"
	up.objects[path] = []byte("payload")
	resp := do(t, http.MethodGet, srv.URL+path+"?X-Amz-Signature=abc&X-Amz-Date=1", map[string]string{
		"X-Forwarded-Host": "evil", "X-Forwarded-For": "1.2.3.4", "X-Forwarded-Proto": "https", "X-Real-Ip": "1.2.3.4",
	})
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	r := up.requests[0]
	if r.Host != "dashboard.example.com" {
		t.Fatalf("upstream Host %q", r.Host)
	}
	if r.URL.RawQuery != "X-Amz-Signature=abc&X-Amz-Date=1" {
		t.Fatalf("query changed: %q", r.URL.RawQuery)
	}
	for h := range r.Header {
		if strings.HasPrefix(h, "X-Forwarded-") || h == "X-Real-Ip" {
			t.Fatalf("%s reached upstream", h)
		}
	}
}

func TestFallbackOnlyOnNoSuchKey(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	want := seed(t, st, testDay, 30, 5)
	if err := newTestArchiver(st, true).run(ctx); err != nil {
		t.Fatal(err)
	}
	srv, up, _ := newTestProxy(t, st)
	key := "artifacts/" + testDay + "/id0007.request.json.zst"
	path := "/" + testBucket + "/" + key

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		resp := do(t, method, srv.URL+path, nil)
		if resp.StatusCode != 200 || resp.Header.Get("Content-Encoding") != "zstd" || resp.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("%s: %d %v", method, resp.StatusCode, resp.Header)
		}
		if method == http.MethodGet {
			body, _ := io.ReadAll(resp.Body)
			if raw, _ := decodeHTTPBody(body); !bytes.Equal(raw, want[key]) {
				t.Fatal("fallback body differs")
			}
		}
	}

	up.denied[path] = "SignatureDoesNotMatch"
	if resp := do(t, http.MethodGet, srv.URL+path, nil); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("a bad signature got %d from the archive", resp.StatusCode)
	}

	missing := "/" + testBucket + "/artifacts/" + testDay + "/nope.request.json.zst"
	resp := do(t, http.MethodGet, srv.URL+missing, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unarchived key: %d", resp.StatusCode)
	}
	if b, _ := io.ReadAll(resp.Body); !bytes.Contains(b, []byte("<Code>NoSuchKey</Code>")) {
		t.Fatalf("MinIO's error body did not reach the client: %q", b)
	}
}

func TestNeighboursComeFromOneDecode(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	want := seed(t, st, testDay, 40, 6)
	a := newTestArchiver(st, true)
	a.frameSize = 1 << 30 // one frame for the whole day
	if err := a.run(ctx); err != nil {
		t.Fatal(err)
	}
	fb := newFallback(st, testArch, 2, 1<<30, 1<<30, 1<<30, 1<<30, 0, quiet)
	var decodes atomic.Int32
	fb.decodes = func() { decodes.Add(1) }

	// The dashboard asks for both artifacts of a request at once.
	var wg sync.WaitGroup
	for _, k := range []string{"id0020", "id0021"} {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			key := "artifacts/" + testDay + "/" + k + ".request.json.zst"
			if e, err := fb.lookup(ctx, key); err != nil || e == nil {
				t.Errorf("%s: %v", k, err)
			}
		}(k)
	}
	wg.Wait()
	for k, body := range want {
		e, err := fb.lookup(ctx, k)
		if err != nil || e == nil {
			t.Fatalf("%s: %v", k, err)
		}
		if raw, _ := decodeHTTPBody(e.body); !bytes.Equal(raw, body) {
			t.Fatalf("%s differs", k)
		}
	}
	if n := decodes.Load(); n != 1 {
		t.Fatalf("%d frame decodes, want 1", n)
	}
}

func TestLRUEvictsByCost(t *testing.T) {
	c := newLRU[string](10, 0, func(b []byte) int64 { return int64(len(b)) })
	c.add("a", make([]byte, 4))
	c.add("b", make([]byte, 4))
	c.get("a")
	c.add("c", make([]byte, 4))
	if _, ok := c.get("b"); ok {
		t.Fatal("least recently used survived")
	}
	if _, ok := c.get("a"); !ok {
		t.Fatal("recently used evicted")
	}
	c.add("huge", make([]byte, 11))
	if _, ok := c.get("huge"); ok {
		t.Fatal("over-budget item cached")
	}
}

func TestLRUIdleExpiry(t *testing.T) {
	now := time.Unix(0, 0)
	c := newLRU[string](100, time.Minute, func(b []byte) int64 { return int64(len(b)) })
	c.now = func() time.Time { return now }
	c.add("old", make([]byte, 10))
	c.add("busy", make([]byte, 10))
	now = now.Add(50 * time.Second)
	c.get("busy")
	now = now.Add(20 * time.Second) // old idle 70s, busy idle 20s
	c.expire()
	if c.size() != 10 {
		t.Fatalf("size %d after expire, want 10", c.size())
	}
	if _, ok := c.get("old"); ok {
		t.Fatal("idle item survived expire")
	}
	if _, ok := c.get("busy"); !ok {
		t.Fatal("recently used item expired")
	}
	now = now.Add(61 * time.Second)
	if _, ok := c.get("busy"); ok {
		t.Fatal("get returned an item idle past the limit")
	}
	if c.size() != 0 {
		t.Fatalf("size %d, want 0", c.size())
	}
}

func TestUnchangedDayIsNotRepacked(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	seed(t, st, testDay, 10, 7)
	a := newTestArchiver(st, false)
	if err := a.run(ctx); err != nil {
		t.Fatal(err)
	}
	gen1, _ := readCurrent(ctx, st, testArch, testDay)

	a.now = func() time.Time { return time.Date(2026, 1, 17, 4, 0, 0, 0, time.UTC) }
	if err := a.run(ctx); err != nil {
		t.Fatal(err)
	}
	if gen, _ := readCurrent(ctx, st, testArch, testDay); gen != gen1 {
		t.Fatal("an unchanged day was repacked")
	}
	if objs, _ := st.List(ctx, testArch, testDay+"/"); len(objs) != 3 {
		t.Fatalf("%d archive objects, want data, index and CURRENT", len(objs))
	}

	// Turning --delete on later removes the loose objects without a new generation.
	a.del = true
	a.now = func() time.Time { return time.Date(2026, 1, 18, 4, 0, 0, 0, time.UTC) }
	if err := a.run(ctx); err != nil {
		t.Fatal(err)
	}
	if gen, _ := readCurrent(ctx, st, testArch, testDay); gen != gen1 {
		t.Fatal("turning on --delete repacked the day")
	}
	if objs, _ := st.List(ctx, testBucket, "artifacts/"+testDay+"/"); len(objs) != 0 {
		t.Fatalf("%d loose objects left", len(objs))
	}
}

// Between archiving a day and deleting its loose objects, a key may be written for the first
// time or overwritten with new content. Neither is in CURRENT byte for byte, so both survive.
func TestDeleteOnlyWhatTheIndexCovers(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	seed(t, st, testDay, 10, 8)
	a := newTestArchiver(st, false)
	if err := a.run(ctx); err != nil {
		t.Fatal(err)
	}
	over := "artifacts/" + testDay + "/id0004.request.json.zst"
	late := "artifacts/" + testDay + "/zz-late.request.json.zst"
	st.Put(ctx, testBucket, over, bytes.NewReader(zstdObject(t, []byte(`{"newer":true}`))), -1, "application/json")
	st.Put(ctx, testBucket, late, bytes.NewReader(zstdObject(t, []byte(`{"late":true}`))), -1, "application/json")
	if n, err := a.deleteCovered(ctx, testDay); err != nil || n != 9 {
		t.Fatalf("deleted %d (%v), want the 9 untouched originals", n, err)
	}
	if !st.has(testBucket, over) {
		t.Fatal("an object overwritten after archiving was deleted")
	}
	if !st.has(testBucket, late) {
		t.Fatal("an object written after archiving was deleted")
	}
}

// Each day's loose objects go as soon as its CURRENT is in place, while later days are still being
// packed, not all at the end of the run.
func TestDeletionFollowsTheRun(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	seed(t, st, "2026-01-08", 5, 21)
	seed(t, st, "2026-01-09", 5, 22)
	a := newTestArchiver(st, true)
	var mu sync.Mutex // onGet runs on the prefetch goroutines
	firstDayLeftWhenSecondStarted := -1
	a.st = &hookStore{memStore: st, onGet: func(key string) {
		mu.Lock()
		defer mu.Unlock()
		if strings.Contains(key, "/2026-01-09/") && firstDayLeftWhenSecondStarted < 0 {
			objs, _ := st.List(ctx, testBucket, "artifacts/2026-01-08/")
			firstDayLeftWhenSecondStarted = len(objs)
		}
	}}
	if err := a.run(ctx); err != nil {
		t.Fatal(err)
	}
	if firstDayLeftWhenSecondStarted != 0 {
		t.Fatalf("the first day still had %d loose objects when the second was packed", firstDayLeftWhenSecondStarted)
	}
	if objs, _ := st.List(ctx, testBucket, "artifacts/2026-01-09/"); len(objs) != 0 {
		t.Fatalf("the last day kept %d loose objects", len(objs))
	}
}

// Loose objects go the moment CURRENT moves, so a reader holding a CURRENT cached from before
// the move must re-read it rather than answer 404.
func TestStaleCurrentIsReread(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	want := seed(t, st, testDay, 6, 31)
	now := time.Date(2026, 1, 16, 4, 0, 0, 0, time.UTC)
	fb := newFallback(st, testArch, 2, 1<<30, 1<<30, 1<<30, 1<<30, 0, quiet)
	fb.now = func() time.Time { return now }
	first := "artifacts/" + testDay + "/id0000.request.json.zst"

	// Not archived yet: the miss caches "no CURRENT".
	if e, err := fb.lookup(ctx, first); err != nil || e != nil {
		t.Fatalf("before archiving: %v %v", e, err)
	}
	if err := newTestArchiver(st, true).run(ctx); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * currentRefresh) // well inside currentTTL
	e, err := fb.lookup(ctx, first)
	if err != nil || e == nil {
		t.Fatalf("a day archived after its CURRENT was cached as absent: %v %v", e, err)
	}
	if raw, _ := decodeHTTPBody(e.body); !bytes.Equal(raw, want[first]) {
		t.Fatal("wrong bytes")
	}

	// A key packed into a newer generation than the cached one.
	late := "artifacts/" + testDay + "/zz-late.request.json.zst"
	st.Put(ctx, testBucket, late, bytes.NewReader(zstdObject(t, []byte(`{"late":true}`))), -1, "application/json")
	a := newTestArchiver(st, true)
	a.now = func() time.Time { return time.Date(2026, 1, 17, 4, 0, 0, 0, time.UTC) }
	if err := a.run(ctx); err != nil {
		t.Fatal(err)
	}
	if st.has(testBucket, late) {
		t.Fatal("the late object was not deleted at once")
	}
	now = now.Add(2 * currentRefresh)
	if e, err := fb.lookup(ctx, late); err != nil || e == nil {
		t.Fatalf("a key in a newer generation than the cached CURRENT: %v %v", e, err)
	}

	// An absent key re-reads CURRENT at most once per currentRefresh.
	reads := 0
	counting := &hookStore{memStore: st, onGet: func(string) {}}
	fb.st = &currentCounter{store: counting, n: &reads}
	missing := "artifacts/" + testDay + "/nope.request.json.zst"
	for k := 0; k < 5; k++ {
		fb.lookup(ctx, missing)
	}
	if reads > 1 {
		t.Fatalf("CURRENT re-read %d times for one absent key within currentRefresh", reads)
	}
}

type currentCounter struct {
	store
	n *int
}

func (c *currentCounter) Get(ctx context.Context, bucket, key string) (io.ReadCloser, objectInfo, error) {
	if strings.HasSuffix(key, "/CURRENT") {
		*c.n++
	}
	return c.store.Get(ctx, bucket, key)
}
