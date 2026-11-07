package store

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

func TestConvertV1ToV2(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	want := v2Day(t, st, testDay, 4, 12, 13)
	if err := newTestArchiver(st, true).run(ctx); err != nil { // v1, loose objects deleted
		t.Fatal(err)
	}
	v1gen, _ := readCurrent(ctx, st, testArch, testDay)
	now := func() time.Time { return time.Date(2026, 1, 20, 0, 0, 0, 0, time.UTC) }

	// A trial into another bucket leaves the real archive alone.
	if _, err := convertDay(ctx, st, testBucket, testArch, "trial", testDay, now, quiet); err != nil {
		t.Fatal(err)
	}
	if gen, _ := readCurrent(ctx, st, testArch, testDay); gen != v1gen {
		t.Fatal("a trial conversion moved the real CURRENT")
	}
	trialGen, _ := readCurrent(ctx, st, "trial", testDay)
	if ix, err := readIndex(ctx, st, "trial", testDay, trialGen); err != nil || ix.Version != indexV2 {
		t.Fatalf("trial: %v", err)
	}

	// In place.
	now = func() time.Time { return time.Date(2026, 1, 21, 0, 0, 0, 0, time.UTC) }
	ds, err := convertDay(ctx, st, testBucket, testArch, testArch, testDay, now, quiet)
	if err != nil || ds.Entries != len(want) {
		t.Fatalf("%v, %d entries", err, ds.Entries)
	}
	ix, get := readAllV2(t, st, testDay)
	if ix.Version != indexV2 {
		t.Fatal("still v1")
	}
	if st.has(testArch, dataKey(testDay, v1gen)) || st.has(testArch, indexKey(testDay, v1gen)) {
		t.Fatal("the v1 generation outlived its conversion")
	}
	for k, b := range want {
		if !bytes.Equal(get(k), b) {
			t.Fatalf("%s differs after conversion", k)
		}
	}
	// Converting again is a no-op.
	if ds, err := convertDay(ctx, st, testBucket, testArch, testArch, testDay, now, quiet); err != nil || !ds.UpToDate {
		t.Fatalf("second conversion: %v %+v", err, ds)
	}
	// The proxy reads the converted day.
	srv, _, _ := newTestProxy(t, st)
	for k, b := range want {
		resp := do(t, "GET", srv.URL+"/"+testBucket+"/"+k, nil)
		body, _ := io.ReadAll(resp.Body)
		if raw, _ := decodeHTTPBody(body); resp.StatusCode != 200 || !bytes.Equal(raw, b) {
			t.Fatalf("%s: %d, or the body differs", k, resp.StatusCode)
		}
	}
}

func TestConvertRefusesDaysWithLooseObjects(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	seed(t, st, testDay, 5, 14)
	if err := newTestArchiver(st, false).run(ctx); err != nil {
		t.Fatal(err)
	}
	_, err := convertDay(ctx, st, testBucket, testArch, testArch, testDay, time.Now, quiet)
	if err == nil || !strings.Contains(err.Error(), "loose objects") {
		t.Fatalf("converted a day the archive run still owns: %v", err)
	}
}
