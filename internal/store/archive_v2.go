package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// v2Source supplies the entries of a day to the v2 writer: old ones from the previous generation,
// in that generation's own order, and loose ones in key order. The writer asks twice: once to
// collect blobs and sort keys, once in sorted order to write.
type v2Rec struct {
	key       string
	etag      string
	ct        string
	stored    int64
	loose     bool
	oldIdx    int
	rawLength int64
	sha       string
	literal   bool
	sk        sortKey
}

// archiveDayV2 writes a v2 generation of day: blob frames during the first pass, text frames
// during the second, then reads everything back before CURRENT moves. old is the current
// generation (v2) or nil; v1 days are repacked by archiveDay until they are converted.
func (a *archiver) archiveDayV2(ctx context.Context, day, gen string, loose []objectInfo, cur string, old *archiveIndex) (dayStatus, []string) {
	t0 := time.Now()
	ds := dayStatus{Day: day, Generation: gen, LooseObjects: len(loose)}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ix := &archiveIndex{Version: indexV2, Day: day, Generation: gen, WindowLog: a.windowLog}
	pr, pw := io.Pipe()
	putErr := make(chan error, 1)
	putDone := false
	go func() {
		err := a.st.Put(ctx, a.archBucket, dataKey(day, gen), pr, -1, "application/octet-stream")
		pr.CloseWithError(err)
		putErr <- err
	}()
	fail := func(err error) (dayStatus, []string) {
		pw.CloseWithError(err)
		if !putDone {
			<-putErr
		}
		cancel()
		cleanup, done := context.WithTimeout(context.Background(), time.Minute)
		defer done()
		a.st.Remove(cleanup, a.archBucket, []string{dataKey(day, gen), indexKey(day, gen)})
		ds.Error = err.Error()
		ds.Seconds = time.Since(t0).Seconds()
		return ds, nil
	}
	out := &countingWriter{w: pw}
	blobFrame, textFrame := int64(v2BlobFrame), int64(v2TextFrame)
	if a.blobFrame > 0 {
		blobFrame = a.blobFrame
	}
	if a.textFrame > 0 {
		textFrame = a.textFrame
	}
	blobs, err := newFramer(out, ix, frameBlob, blobFrame, v2BlobWindowLog, a.level)
	if err != nil {
		return fail(err)
	}
	blobIDs := map[[32]byte]int{}
	addBlob := func(run []byte) (int, error) {
		h := sha256.Sum256(run)
		if id, ok := blobIDs[h]; ok {
			return id, nil
		}
		fi, off, err := blobs.add(run)
		if err != nil {
			return 0, err
		}
		id := len(ix.Blobs)
		blobIDs[h] = id
		ix.Blobs = append(ix.Blobs, blobInfo{Frame: fi, Offset: off, Length: int64(len(run)), SHA256: fmt.Sprintf("%x", h)})
		return id, nil
	}
	findBlob := func(run []byte) (int, error) {
		if id, ok := blobIDs[sha256.Sum256(run)]; ok {
			return id, nil
		}
		return 0, errors.New("blob unseen in the first pass")
	}

	var oldFrames func(int) ([]byte, error)
	if old != nil {
		oldFrames = func(fi int) ([]byte, error) {
			return readRange(ctx, a.st, a.archBucket, dataKey(day, cur), old.Frames[fi])
		}
	}

	// First pass: blobs out, sort keys in.
	var recs []v2Rec
	inLoose := make(map[string]bool, len(loose))
	for _, o := range loose {
		inLoose[o.Key] = true
	}
	record := func(r v2Rec, raw []byte) error {
		stripped, literal, err := stripBlobs(raw, addBlob)
		if err != nil {
			return err
		}
		r.rawLength, r.sha, r.literal, r.sk = int64(len(raw)), sha256Hex(raw), literal, makeSortKey(stripped)
		recs = append(recs, r)
		return nil
	}
	if old != nil {
		w, err := newV2Walker(old, oldFrames, newBlobReader(old, oldFrames, 4).blob)
		if err != nil {
			return fail(err)
		}
		for {
			i, stripped, err := w.step()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				w.close()
				return fail(err)
			}
			e := old.Entries[i]
			if inLoose[e.Key] {
				continue // the loose object is newer
			}
			if stripped == nil {
				w.close()
				return fail(fmt.Errorf("%s: chain broken in the previous generation", e.Key))
			}
			raw, err := w.expand(i, stripped)
			if err != nil {
				w.close()
				return fail(err)
			}
			if err := record(v2Rec{key: e.Key, etag: e.ETag, ct: e.ContentType, stored: e.StoredSize, oldIdx: i}, raw); err != nil {
				w.close()
				return fail(err)
			}
		}
		w.close()
	}
	var looseItems []mergeItem
	for _, o := range loose {
		looseItems = append(looseItems, mergeItem{key: o.Key, etag: o.ETag, loose: true})
	}
	futures := a.prefetch(ctx, looseItems)
	for _, it := range looseItems {
		fut, ok := <-futures
		if !ok {
			return fail(fmt.Errorf("prefetch stopped: %w", context.Cause(ctx)))
		}
		f := <-fut
		if f.err != nil {
			return fail(f.err)
		}
		if f.bad != nil || f.etag != it.etag {
			ds.Skipped = append(ds.Skipped, it.key)
			continue
		}
		if err := record(v2Rec{key: it.key, etag: it.etag, ct: f.ct, stored: f.size, loose: true}, f.raw); err != nil {
			return fail(err)
		}
	}
	if err := blobs.close(); err != nil {
		return fail(err)
	}

	keys := make([]string, len(recs))
	sks := make([]sortKey, len(recs))
	for i, r := range recs {
		keys[i], sks[i] = r.key, r.sk
	}
	order := sortV2(keys, sks)

	// Second pass: in sorted order, each entry as a delta of the previous one of its kind.
	text, err := newFramer(out, ix, frameText, textFrame, a.windowLog, a.level)
	if err != nil {
		return fail(err)
	}
	var second []mergeItem
	for _, o := range order {
		if recs[o].loose {
			second = append(second, mergeItem{key: recs[o].key, etag: recs[o].etag, loose: true})
		}
	}
	futures = a.prefetch(ctx, second)
	var oldW *v2Walker
	if old != nil {
		if oldW, err = newV2Walker(old, oldFrames, newBlobReader(old, oldFrames, 4).blob); err != nil {
			return fail(err)
		}
		defer oldW.close()
	}
	last := map[bool][]byte{}
	lastAt := map[bool]int{true: -1, false: -1}
	sinceKey := map[bool]int{}
	var included []string
	for _, o := range order {
		r := recs[o]
		var raw []byte
		if r.loose {
			fut, ok := <-futures
			if !ok {
				return fail(fmt.Errorf("prefetch stopped: %w", context.Cause(ctx)))
			}
			f := <-fut
			if f.err != nil {
				return fail(f.err)
			}
			// Changed since the first pass: its blobs may be missing; leave it for the next run.
			if f.bad != nil || f.etag != r.etag || sha256Hex(f.raw) != r.sha {
				ds.Skipped = append(ds.Skipped, r.key)
				continue
			}
			raw = f.raw
		} else {
			if raw, err = oldW.raw(r.oldIdx); err != nil {
				return fail(err)
			}
		}
		stripped, literal, err := stripBlobs(raw, findBlob)
		if err != nil {
			return fail(fmt.Errorf("%s: %w", r.key, err))
		}
		_, kind := unitID(r.key)
		e := entry{Key: r.key, Parent: -1, Literal: literal, RawLength: r.rawLength, SHA256: r.sha,
			ContentType: r.ct, ETag: r.etag, StoredSize: r.stored}
		mid := stripped
		if lastAt[kind] >= 0 && sinceKey[kind] < keyEvery {
			mid, e.Prefix, e.Suffix = deltaOf(last[kind], stripped)
			e.Parent = lastAt[kind]
			sinceKey[kind]++
		} else {
			sinceKey[kind] = 1
		}
		fi, off, err := text.add(mid)
		if err != nil {
			return fail(err)
		}
		e.Frame, e.Offset, e.Length = fi, off, int64(len(mid))
		last[kind], lastAt[kind] = stripped, len(ix.Entries)
		ix.Entries = append(ix.Entries, e)
		ds.StoredBytes += r.stored
		if r.loose {
			included = append(included, r.key)
		}
	}
	if err := text.close(); err != nil {
		return fail(err)
	}
	pw.Close()
	putDone = true
	if err := <-putErr; err != nil {
		return fail(err)
	}
	idx, err := encodeIndex(ix)
	if err != nil {
		return fail(err)
	}
	if err := a.st.Put(ctx, a.archBucket, indexKey(day, gen), bytes.NewReader(idx), int64(len(idx)), "application/zstd"); err != nil {
		return fail(err)
	}
	if err := a.verifyV2(ctx, day, gen, ix); err != nil {
		return fail(fmt.Errorf("verify: %w", err))
	}
	if err := a.st.Put(ctx, a.archBucket, currentKey(day), strings.NewReader(gen+"\n"), int64(len(gen)+1), "text/plain"); err != nil {
		return fail(err)
	}
	if ds.Removed, err = dropOtherGenerations(ctx, a.st, a.archBucket, day, gen); err != nil {
		a.log.Warn("could not remove older generations", "day", day, "err", err)
	}
	ds.Entries = len(ix.Entries)
	for _, f := range ix.Frames {
		ds.ArchiveBytes += f.Length
	}
	if ds.ArchiveBytes > 0 {
		ds.Ratio = float64(ds.StoredBytes) / float64(ds.ArchiveBytes)
	}
	ds.Seconds = time.Since(t0).Seconds()
	return ds, included
}

// verifyV2 reads the generation back from the bucket front to back and checks every entry.
func (a *archiver) verifyV2(ctx context.Context, day, gen string, ix *archiveIndex) error {
	frames := func(fi int) ([]byte, error) {
		return readRange(ctx, a.st, a.archBucket, dataKey(day, gen), ix.Frames[fi])
	}
	w, err := newV2Walker(ix, frames, newBlobReader(ix, frames, 4).blob)
	if err != nil {
		return err
	}
	defer w.close()
	n := 0
	for {
		i, stripped, err := w.step()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if stripped == nil {
			return fmt.Errorf("%s: chain broken", ix.Entries[i].Key)
		}
		if _, err := w.expand(i, stripped); err != nil {
			return err
		}
		n++
	}
	if n != len(ix.Entries) {
		return fmt.Errorf("read back %d entries, index has %d", n, len(ix.Entries))
	}
	return nil
}

func readRange(ctx context.Context, st store, bucket, key string, f frameInfo) ([]byte, error) {
	rc, err := st.GetRange(ctx, bucket, key, f.Offset, f.Length)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != f.Length {
		return nil, fmt.Errorf("%s: got %d bytes at %d, want %d", key, len(b), f.Offset, f.Length)
	}
	return b, nil
}
