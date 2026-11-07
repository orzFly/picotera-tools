package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
)

type cachedEntry struct {
	body        []byte // already encoded for HTTP, see encodeForHTTP
	contentType string
}

type frameID struct {
	day, gen string
	frame    int
}

type currentGen struct {
	gen string
	at  time.Time
}

type decodeResult struct {
	entry *cachedEntry
	err   error
}

// fallback serves keys the bucket no longer holds out of picotera-archives.
type fallback struct {
	st         store
	bucket     string
	lookbehind int64
	lookahead  int64
	now        func() time.Time
	log        *slog.Logger

	entries *lru[string, *cachedEntry]
	frames  *lru[frameID, []byte]
	indexes *lru[string, *archiveIndex]
	sem     chan struct{}
	decodes func() // test hook, counts frame decodes

	mu      sync.Mutex
	current map[string]currentGen
	flights map[frameID]chan struct{}
}

func newFallback(st store, bucket string, maxDecodes int, entryCache, frameCache, lookbehind, lookahead int64, idle time.Duration, log *slog.Logger) *fallback {
	f := &fallback{
		st: st, bucket: bucket, lookbehind: lookbehind, lookahead: lookahead, now: time.Now, log: log,
		entries: newLRU[string](entryCache, idle, func(e *cachedEntry) int64 { return int64(len(e.body)) }),
		frames:  newLRU[frameID](frameCache, idle, func(b []byte) int64 { return int64(len(b)) }),
		indexes: newLRU[string](64, idle, func(*archiveIndex) int64 { return 1 }),
		sem:     make(chan struct{}, maxDecodes),
		decodes: func() {},
		current: map[string]currentGen{},
		flights: map[frameID]chan struct{}{},
	}
	if idle > 0 {
		go f.expireLoop(idle / 4)
	}
	return f
}

// expireLoop frees idle cache items even when no request comes along to notice them, and hands
// the memory back: the Go runtime would otherwise keep a burst's two-odd GiB mapped for minutes.
func (f *fallback) expireLoop(every time.Duration) {
	for range time.Tick(every) {
		if f.entries.expire()+f.frames.expire()+f.indexes.expire() > 0 {
			debug.FreeOSMemory()
		}
	}
}

// lookup returns nil, nil when the key is not archived either, so the caller can pass MinIO's 404 on.
func (f *fallback) lookup(ctx context.Context, key string) (*cachedEntry, error) {
	if e, ok := f.entries.get(key); ok {
		return e, nil
	}
	day, ok := dayOfKey(key)
	if !ok {
		return nil, nil
	}
	gen, err := f.currentGeneration(ctx, day)
	if err != nil || gen == "" {
		return nil, err
	}
	ix, err := f.index(ctx, day, gen)
	if err != nil {
		return nil, err
	}
	i, ok := ix.lookup(key)
	if !ok {
		return nil, nil
	}
	fid := frameID{day, gen, ix.Entries[i].Frame}
	if ix.Version == indexV2 {
		// A v2 read starts at the frame holding its chain's keyframe; requests sharing that
		// start share one decode.
		w := &v2Walker{ix: ix}
		fid.frame = ix.Entries[w.root(i)].Frame
	}
	for {
		f.mu.Lock()
		if done, busy := f.flights[fid]; busy {
			f.mu.Unlock()
			select {
			case <-done:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			// The pass that just finished cached its neighbours; a request for the response
			// artifact usually lands here while the request artifact is being decoded.
			if e, ok := f.entries.get(key); ok {
				return e, nil
			}
			continue
		}
		done := make(chan struct{})
		f.flights[fid] = done
		f.mu.Unlock()
		res := make(chan decodeResult, 1)
		if ix.Version == indexV2 {
			go f.decodeV2(fid, ix, i, done, res)
		} else {
			go f.decode(fid, ix, i, done, res)
		}
		select {
		case r := <-res:
			return r.entry, r.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (f *fallback) currentGeneration(ctx context.Context, day string) (string, error) {
	f.mu.Lock()
	c, ok := f.current[day]
	f.mu.Unlock()
	if ok && f.now().Sub(c.at) < currentTTL {
		return c.gen, nil
	}
	gen, err := readCurrent(ctx, f.st, f.bucket, day)
	if err != nil {
		return "", err
	}
	f.mu.Lock()
	f.current[day] = currentGen{gen: gen, at: f.now()}
	f.mu.Unlock()
	return gen, nil
}

func (f *fallback) index(ctx context.Context, day, gen string) (*archiveIndex, error) {
	k := day + "/" + gen
	if ix, ok := f.indexes.get(k); ok {
		return ix, nil
	}
	ix, err := readIndex(ctx, f.st, f.bucket, day, gen)
	if err != nil {
		return nil, err
	}
	f.indexes.add(k, ix)
	return ix, nil
}

func (f *fallback) frame(ctx context.Context, fid frameID, ix *archiveIndex) ([]byte, error) {
	if b, ok := f.frames.get(fid); ok {
		return b, nil
	}
	fr := ix.Frames[fid.frame]
	rc, err := f.st.GetRange(ctx, f.bucket, dataKey(fid.day, fid.gen), fr.Offset, fr.Length)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != fr.Length {
		return nil, fmt.Errorf("frame %v: got %d bytes, want %d", fid, len(b), fr.Length)
	}
	f.frames.add(fid, b)
	return b, nil
}

type rawEntry struct {
	i   int
	raw []byte
}

// decode runs detached from the request that started it: the neighbours it caches serve the
// next clicks even if that first request has gone away.
func (f *fallback) decode(fid frameID, ix *archiveIndex, target int, done chan struct{}, res chan<- decodeResult) {
	defer func() {
		f.mu.Lock()
		delete(f.flights, fid)
		f.mu.Unlock()
		close(done)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	sent := false
	fail := func(err error) {
		if !sent {
			res <- decodeResult{err: err}
			sent = true
		}
	}
	select {
	case f.sem <- struct{}{}:
		defer func() { <-f.sem }()
	case <-ctx.Done():
		fail(ctx.Err())
		return
	}
	f.decodes()
	b, err := f.frame(ctx, fid, ix)
	if err != nil {
		fail(err)
		return
	}
	dec, err := newFrameDecoder(ix.WindowLog)
	if err != nil {
		fail(err)
		return
	}
	defer dec.Close()

	var behind []rawEntry // entries before the target, trimmed to lookbehind bytes
	var behindBytes int64
	var ahead []rawEntry
	var aheadBytes int64
	start := time.Now()
	err = walkFrame(dec, bytes.NewReader(b), ix, fid.frame, func(i int, raw []byte) (bool, error) {
		switch {
		case i < target:
			behind = append(behind, rawEntry{i, raw})
			behindBytes += int64(len(raw))
			for len(behind) > 0 && behindBytes > f.lookbehind {
				behindBytes -= int64(len(behind[0].raw))
				behind[0] = rawEntry{} // let the dropped entry go before the slice reallocates
				behind = behind[1:]
			}
		case i == target:
			if err := checkEntry(&ix.Entries[i], raw); err != nil {
				return false, err
			}
			body, err := encodeForHTTP(raw)
			if err != nil {
				return false, err
			}
			e := &cachedEntry{body: body, contentType: ix.Entries[i].ContentType}
			f.entries.add(ix.Entries[i].Key, e)
			res <- decodeResult{entry: e}
			sent = true
			f.log.Info("fallback hit", "key", ix.Entries[i].Key, "frame", fid.frame, "decode", time.Since(start).Round(time.Millisecond))
		default:
			ahead = append(ahead, rawEntry{i, raw})
			aheadBytes += int64(len(raw))
			return aheadBytes < f.lookahead, nil
		}
		return true, nil
	})
	if err != nil {
		f.log.Error("fallback decode failed", "frame", fid, "err", err)
		fail(err)
		return
	}
	if !sent {
		fail(errors.New("target entry not reached"))
		return
	}
	f.cacheNeighbours(ix, append(behind, ahead...))
}

func (f *fallback) cacheNeighbours(ix *archiveIndex, neighbours []rawEntry) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, min(4, runtime.NumCPU()))
	for _, n := range neighbours {
		e := &ix.Entries[n.i]
		if _, ok := f.entries.get(e.Key); ok {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(e *entry, raw []byte) {
			defer func() { <-sem; wg.Done() }()
			if checkEntry(e, raw) != nil {
				return
			}
			if body, err := encodeForHTTP(raw); err == nil {
				f.entries.add(e.Key, &cachedEntry{body: body, contentType: e.ContentType})
			}
		}(e, n.raw)
	}
	wg.Wait()
}

func readCurrent(ctx context.Context, st store, bucket, day string) (string, error) {
	rc, _, err := st.Get(ctx, bucket, currentKey(day))
	if errors.Is(err, errNotFound) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	return strings.TrimSpace(string(b)), err
}

func readIndex(ctx context.Context, st store, bucket, day, gen string) (*archiveIndex, error) {
	rc, _, err := st.Get(ctx, bucket, indexKey(day, gen))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", indexKey(day, gen), err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return decodeIndex(b)
}

// decodeHTTPBody undoes encodeForHTTP, for the get subcommand and tests.
func decodeHTTPBody(body []byte) ([]byte, error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	return dec.DecodeAll(body, nil)
}

// decodeV2 is decode for format v2: walk text frames from the target's chain root, answer as soon
// as the target is rebuilt, and cache the rebuildable entries around it.
func (f *fallback) decodeV2(fid frameID, ix *archiveIndex, target int, done chan struct{}, res chan<- decodeResult) {
	defer func() {
		f.mu.Lock()
		delete(f.flights, fid)
		f.mu.Unlock()
		close(done)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	sent := false
	fail := func(err error) {
		if !sent {
			res <- decodeResult{err: err}
			sent = true
		}
	}
	select {
	case f.sem <- struct{}{}:
		defer func() { <-f.sem }()
	case <-ctx.Done():
		fail(ctx.Err())
		return
	}
	f.decodes()
	frames := func(fi int) ([]byte, error) { return f.frame(ctx, frameID{fid.day, fid.gen, fi}, ix) }
	blobs := newBlobReader(ix, frames, 4)
	w, err := newV2Walker(ix, frames, blobs.blob)
	if err != nil {
		fail(err)
		return
	}
	defer w.close()
	if err := w.open(fid.frame); err != nil {
		fail(err)
		return
	}
	type rebuilt struct {
		i        int
		stripped []byte
	}
	var behind, ahead []rebuilt
	var behindBytes, aheadBytes int64
	start := time.Now()
	for {
		i, stripped, err := w.step()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			f.log.Error("fallback decode failed", "frame", fid, "err", err)
			fail(err)
			return
		}
		switch {
		case i < target:
			if stripped == nil {
				continue
			}
			behind = append(behind, rebuilt{i, stripped})
			behindBytes += int64(len(stripped))
			for len(behind) > 0 && behindBytes > f.lookbehind {
				behindBytes -= int64(len(behind[0].stripped))
				behind[0] = rebuilt{}
				behind = behind[1:]
			}
			continue
		case i == target:
			if stripped == nil {
				fail(fmt.Errorf("%s: chain broken", ix.Entries[i].Key))
				return
			}
			raw, err := w.expand(i, stripped)
			if err != nil {
				fail(err)
				return
			}
			body, _ := encodeForHTTP(raw)
			e := &cachedEntry{body: body, contentType: ix.Entries[i].ContentType}
			f.entries.add(ix.Entries[i].Key, e)
			res <- decodeResult{entry: e}
			sent = true
			f.log.Info("fallback hit", "key", ix.Entries[i].Key, "frame", fid.frame, "decode", time.Since(start).Round(time.Millisecond))
			continue
		}
		if stripped != nil {
			ahead = append(ahead, rebuilt{i, stripped})
			aheadBytes += int64(len(stripped))
		}
		if aheadBytes >= f.lookahead {
			break
		}
	}
	if !sent {
		fail(errors.New("target entry not reached"))
		return
	}
	var neighbours []rawEntry
	for _, r := range append(behind, ahead...) {
		if _, ok := f.entries.get(ix.Entries[r.i].Key); ok {
			continue
		}
		// Expanding touches the blob reader, which is not safe for concurrent use.
		raw, err := w.expand(r.i, r.stripped)
		if err != nil {
			continue
		}
		neighbours = append(neighbours, rawEntry{r.i, raw})
	}
	f.cacheExpanded(ix, neighbours)
}

// cacheExpanded encodes already verified entries for HTTP and puts them in the entry cache.
func (f *fallback) cacheExpanded(ix *archiveIndex, rebuilt []rawEntry) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, min(4, runtime.NumCPU()))
	for _, n := range rebuilt {
		wg.Add(1)
		sem <- struct{}{}
		go func(e *entry, raw []byte) {
			defer func() { <-sem; wg.Done() }()
			if body, err := encodeForHTTP(raw); err == nil {
				f.entries.add(e.Key, &cachedEntry{body: body, contentType: e.ContentType})
			}
		}(&ix.Entries[n.i], n.raw)
	}
	wg.Wait()
}
