package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

type archiver struct {
	st         store
	bucket     string
	archBucket string
	minAge     int
	day        string
	del        bool
	dryRun     bool
	frameSize  int64
	windowLog  int
	level      zstd.EncoderLevel
	fetchers   int
	// picotera-s3proxy caches CURRENT for currentTTL; loose objects outlive the switch by twice
	// that, so a reader still holding the old generation finds the key one way or the other.
	grace time.Duration
	now   func() time.Time
	log   *slog.Logger
}

type dayStatus struct {
	Day          string   `json:"day"`
	Generation   string   `json:"generation,omitempty"`
	LooseObjects int      `json:"loose_objects"`
	Entries      int      `json:"entries"`
	StoredBytes  int64    `json:"stored_bytes"`
	ArchiveBytes int64    `json:"archive_bytes"`
	Ratio        float64  `json:"ratio"`
	Skipped      []string `json:"skipped,omitempty"`
	Deleted      int      `json:"deleted"`
	UpToDate     bool     `json:"up_to_date,omitempty"`
	Seconds      float64  `json:"seconds"`
	Error        string   `json:"error,omitempty"`
}

type runStatus struct {
	Started            time.Time   `json:"started"`
	Finished           time.Time   `json:"finished"`
	Delete             bool        `json:"delete"`
	Days               []dayStatus `json:"days"`
	RemovedGenerations []string    `json:"removed_generations,omitempty"`
	Error              string      `json:"error,omitempty"`
}

const statusKey = "_status.json"

func (a *archiver) run(ctx context.Context) error {
	start := a.now()
	gen := generationNow(start)
	rs := runStatus{Started: start.UTC(), Delete: a.del}
	days, err := a.days(ctx, start)
	if err != nil {
		return err
	}
	included := map[int][]string{} // index into rs.Days → loose keys now inside CURRENT
	var failed []string
	for _, d := range days {
		loose, err := a.st.List(ctx, a.bucket, "artifacts/"+d+"/")
		if err != nil {
			return fmt.Errorf("list %s: %w", d, err)
		}
		if len(loose) == 0 {
			continue
		}
		// Until --delete is on, every day keeps its loose objects; without this check each run
		// would repack every day ever archived.
		if ds, ok, err := a.upToDate(ctx, d, loose); err != nil {
			rs.Days = append(rs.Days, dayStatus{Day: d, LooseObjects: len(loose), Error: err.Error()})
			failed = append(failed, d)
			continue
		} else if ok {
			rs.Days = append(rs.Days, ds)
			keys := make([]string, len(loose))
			for i, o := range loose {
				keys[i] = o.Key
			}
			included[len(rs.Days)-1] = keys
			a.log.Info("already archived", "day", d, "generation", ds.Generation, "loose_objects", len(loose))
			continue
		}
		if a.dryRun {
			a.log.Info("would archive", "day", d, "loose_objects", len(loose))
			rs.Days = append(rs.Days, dayStatus{Day: d, LooseObjects: len(loose)})
			continue
		}
		ds, keys := a.archiveDay(ctx, d, gen, loose)
		rs.Days = append(rs.Days, ds)
		if ds.Error != "" {
			failed = append(failed, d)
			a.log.Error("day failed", "day", d, "err", ds.Error)
			continue
		}
		included[len(rs.Days)-1] = keys
		a.log.Info("archived", "day", d, "entries", ds.Entries, "ratio", fmt.Sprintf("%.1f", ds.Ratio), "seconds", int(ds.Seconds))
	}
	if a.dryRun {
		return nil
	}
	if a.del {
		if len(included) > 0 {
			a.log.Info("waiting before deleting loose objects", "grace", a.grace)
			select {
			case <-time.After(a.grace):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		for i := range included {
			n, err := a.deleteCovered(ctx, rs.Days[i].Day)
			rs.Days[i].Deleted = n
			if err != nil {
				rs.Days[i].Error = "delete: " + err.Error()
				failed = append(failed, rs.Days[i].Day)
			}
		}
		switched := map[string]bool{}
		for i := range included {
			if !rs.Days[i].UpToDate {
				switched[rs.Days[i].Day] = true
			}
		}
		removed, err := a.removeOldGenerations(ctx, gen, switched)
		rs.RemovedGenerations = removed
		if err != nil {
			rs.Error = err.Error()
		}
	}
	rs.Finished = a.now().UTC()
	b, _ := json.MarshalIndent(rs, "", "  ")
	if err := a.st.Put(ctx, a.archBucket, statusKey, bytes.NewReader(b), int64(len(b)), "application/json"); err != nil {
		return fmt.Errorf("write %s: %w", statusKey, err)
	}
	if len(failed) > 0 || rs.Error != "" {
		return fmt.Errorf("failed days %v %s", failed, rs.Error)
	}
	return nil
}

func (a *archiver) days(ctx context.Context, now time.Time) ([]string, error) {
	if a.day != "" {
		return []string{a.day}, nil
	}
	prefixes, err := a.st.ListPrefixes(ctx, a.bucket, "artifacts/")
	if err != nil {
		return nil, err
	}
	cutoff := now.UTC().AddDate(0, 0, -a.minAge).Format("2006-01-02")
	var days []string
	for _, p := range prefixes {
		d := strings.TrimSuffix(strings.TrimPrefix(p, "artifacts/"), "/")
		if _, err := time.Parse("2006-01-02", d); err == nil && d <= cutoff {
			days = append(days, d)
		}
	}
	sort.Strings(days)
	return days, nil
}

type mergeItem struct {
	key    string
	etag   string
	loose  bool
	oldIdx int
}

// upToDate reports whether CURRENT already holds every loose object of day, same key and ETag.
func (a *archiver) upToDate(ctx context.Context, day string, loose []objectInfo) (dayStatus, bool, error) {
	cur, err := readCurrent(ctx, a.st, a.archBucket, day)
	if err != nil || cur == "" {
		return dayStatus{}, false, err
	}
	ix, err := readIndex(ctx, a.st, a.archBucket, day, cur)
	if err != nil {
		return dayStatus{}, false, err
	}
	for _, o := range loose {
		i, ok := ix.lookup(o.Key)
		if !ok || o.ETag == "" || ix.Entries[i].ETag != o.ETag {
			return dayStatus{}, false, nil
		}
	}
	ds := dayStatus{Day: day, Generation: cur, LooseObjects: len(loose), Entries: len(ix.Entries), UpToDate: true}
	for _, e := range ix.Entries {
		ds.StoredBytes += e.StoredSize
	}
	for _, f := range ix.Frames {
		ds.ArchiveBytes += f.Length
	}
	if ds.ArchiveBytes > 0 {
		ds.Ratio = float64(ds.StoredBytes) / float64(ds.ArchiveBytes)
	}
	return ds, true, nil
}

type fetched struct {
	key  string
	raw  []byte
	ct   string
	size int64
	bad  error // the object is not what picotera writes; it stays loose
	err  error
}

// archiveDay writes a new generation holding the old one's entries plus every loose object,
// verifies it, and only then points CURRENT at it. It returns the loose keys now covered.
func (a *archiver) archiveDay(ctx context.Context, day, gen string, loose []objectInfo) (dayStatus, []string) {
	t0 := time.Now()
	ds := dayStatus{Day: day, Generation: gen, LooseObjects: len(loose)}
	fail := func(err error) (dayStatus, []string) {
		ds.Error = err.Error()
		ds.Seconds = time.Since(t0).Seconds()
		return ds, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cur, err := readCurrent(ctx, a.st, a.archBucket, day)
	if err != nil {
		return fail(err)
	}
	var old *archiveIndex
	if cur != "" {
		if old, err = readIndex(ctx, a.st, a.archBucket, day, cur); err != nil {
			return fail(err)
		}
	}
	items := make([]mergeItem, 0, len(loose))
	seen := make(map[string]bool, len(loose))
	for _, o := range loose {
		items = append(items, mergeItem{key: o.Key, etag: o.ETag, loose: true})
		seen[o.Key] = true
	}
	if old != nil {
		for i, e := range old.Entries {
			// A loose object is newer than whatever an earlier generation holds for its key.
			if !seen[e.Key] {
				items = append(items, mergeItem{key: e.Key, oldIdx: i})
			}
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].key < items[j].key })

	ix := &archiveIndex{Version: indexVersion, Day: day, Generation: gen, WindowLog: a.windowLog}
	pr, pw := io.Pipe()
	putErr := make(chan error, 1)
	putDone := false
	go func() {
		err := a.st.Put(ctx, a.archBucket, dataKey(day, gen), pr, -1, "application/octet-stream")
		pr.CloseWithError(err)
		putErr <- err
	}()
	abort := func(err error) (dayStatus, []string) {
		pw.CloseWithError(err)
		if !putDone {
			<-putErr
		}
		cancel()
		cleanupCtx, done := context.WithTimeout(context.Background(), time.Minute)
		defer done()
		a.st.Remove(cleanupCtx, a.archBucket, []string{dataKey(day, gen), indexKey(day, gen)})
		return fail(err)
	}
	fw, err := newFrameWriter(pw, ix, a.frameSize, a.level)
	if err != nil {
		return abort(err)
	}
	futures := a.prefetch(ctx, items)
	var oldR *oldReader
	if old != nil {
		oldR = &oldReader{st: a.st, bucket: a.archBucket, day: day, gen: cur, ix: old, frame: -1}
		defer oldR.close()
	}
	var keys []string
	for _, it := range items {
		if it.loose {
			fut, ok := <-futures
			if !ok {
				return abort(fmt.Errorf("prefetch stopped: %w", context.Cause(ctx)))
			}
			f := <-fut
			if f.key != it.key {
				return abort(fmt.Errorf("prefetch out of order: %s != %s", f.key, it.key))
			}
			if f.err != nil {
				return abort(f.err)
			}
			if f.bad != nil {
				ds.Skipped = append(ds.Skipped, it.key)
				a.log.Warn("left loose", "key", it.key, "err", f.bad)
				continue
			}
			if err := fw.add(it.key, f.ct, it.etag, f.size, f.raw); err != nil {
				return abort(err)
			}
			keys = append(keys, it.key)
			ds.StoredBytes += f.size
			continue
		}
		raw, err := oldR.read(ctx, it.oldIdx)
		if err != nil {
			return abort(err)
		}
		e := old.Entries[it.oldIdx]
		if err := fw.add(e.Key, e.ContentType, e.ETag, e.StoredSize, raw); err != nil {
			return abort(err)
		}
		ds.StoredBytes += e.StoredSize
	}
	if err := fw.close(); err != nil {
		return abort(err)
	}
	pw.Close()
	putDone = true
	if err := <-putErr; err != nil {
		return abort(err)
	}
	idx, err := encodeIndex(ix)
	if err != nil {
		return abort(err)
	}
	if err := a.st.Put(ctx, a.archBucket, indexKey(day, gen), bytes.NewReader(idx), int64(len(idx)), "application/zstd"); err != nil {
		return abort(err)
	}
	if err := a.verify(ctx, day, gen, ix); err != nil {
		return abort(fmt.Errorf("verify: %w", err))
	}
	if err := a.st.Put(ctx, a.archBucket, currentKey(day), strings.NewReader(gen+"\n"), int64(len(gen)+1), "text/plain"); err != nil {
		return abort(err)
	}
	ds.Entries = len(ix.Entries)
	for _, f := range ix.Frames {
		ds.ArchiveBytes += f.Length
	}
	if ds.ArchiveBytes > 0 {
		ds.Ratio = float64(ds.StoredBytes) / float64(ds.ArchiveBytes)
	}
	ds.Seconds = time.Since(t0).Seconds()
	return ds, keys
}

// deleteCovered removes the loose objects of day that CURRENT's index holds byte for byte: same
// key and same ETag, both read fresh at deletion time. A key written after the listing, or
// overwritten since it was archived, does not match and stays for the next run.
func (a *archiver) deleteCovered(ctx context.Context, day string) (int, error) {
	cur, err := readCurrent(ctx, a.st, a.archBucket, day)
	if err != nil || cur == "" {
		return 0, err
	}
	ix, err := readIndex(ctx, a.st, a.archBucket, day, cur)
	if err != nil {
		return 0, err
	}
	loose, err := a.st.List(ctx, a.bucket, "artifacts/"+day+"/")
	if err != nil {
		return 0, err
	}
	var keys []string
	for _, o := range loose {
		if i, ok := ix.lookup(o.Key); ok && o.ETag != "" && ix.Entries[i].ETag == o.ETag {
			keys = append(keys, o.Key)
		}
	}
	if len(keys) == 0 {
		return 0, nil
	}
	if err := a.st.Remove(ctx, a.bucket, keys); err != nil {
		return 0, err
	}
	return len(keys), nil
}

// prefetch downloads and unpacks loose objects ahead of the writer, a few at a time, in the
// order the writer will want them.
func (a *archiver) prefetch(ctx context.Context, items []mergeItem) <-chan chan fetched {
	out := make(chan chan fetched, a.fetchers*2)
	dec, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(a.fetchers))
	go func() {
		defer close(out)
		defer dec.Close()
		sem := make(chan struct{}, a.fetchers)
		for _, it := range items {
			if !it.loose {
				continue
			}
			fut := make(chan fetched, 1)
			select {
			case out <- fut:
			case <-ctx.Done():
				return
			}
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			go func(key string) {
				defer func() { <-sem }()
				fut <- a.fetchLoose(ctx, dec, key)
			}(it.key)
		}
		for i := 0; i < cap(sem); i++ {
			sem <- struct{}{}
		}
	}()
	return out
}

func (a *archiver) fetchLoose(ctx context.Context, dec *zstd.Decoder, key string) fetched {
	rc, info, err := a.st.Get(ctx, a.bucket, key)
	if errors.Is(err, errNotFound) {
		return fetched{key: key, bad: err}
	}
	if err != nil {
		return fetched{key: key, err: fmt.Errorf("%s: %w", key, err)}
	}
	defer rc.Close()
	stored, err := io.ReadAll(rc)
	if err != nil {
		return fetched{key: key, err: fmt.Errorf("%s: %w", key, err)}
	}
	raw, err := dec.DecodeAll(stored, nil)
	if err != nil {
		return fetched{key: key, bad: err}
	}
	return fetched{key: key, raw: raw, ct: info.ContentType, size: int64(len(stored))}
}

// verify reads the generation back from the bucket and checks every entry against the sha256
// taken from its source.
func (a *archiver) verify(ctx context.Context, day, gen string, ix *archiveIndex) error {
	dec, err := newFrameDecoder(ix.WindowLog)
	if err != nil {
		return err
	}
	defer dec.Close()
	n := 0
	for fi, fr := range ix.Frames {
		rc, err := a.st.GetRange(ctx, a.archBucket, dataKey(day, gen), fr.Offset, fr.Length)
		if err != nil {
			return err
		}
		var raw int64
		err = walkFrame(dec, rc, ix, fi, func(i int, b []byte) (bool, error) {
			n++
			raw += int64(len(b))
			return true, checkEntry(&ix.Entries[i], b)
		})
		rc.Close()
		if err != nil {
			return err
		}
		if raw != fr.RawLength {
			return fmt.Errorf("frame %d: %d raw bytes, index says %d", fi, raw, fr.RawLength)
		}
	}
	if n != len(ix.Entries) {
		return fmt.Errorf("read back %d entries, index has %d", n, len(ix.Entries))
	}
	return nil
}

// removeOldGenerations drops every generation that is neither CURRENT nor this run's own.
// A day switched by this run keeps its previous generation until the next run, for readers
// that still hold the old CURRENT.
func (a *archiver) removeOldGenerations(ctx context.Context, runGen string, switched map[string]bool) ([]string, error) {
	objs, err := a.st.List(ctx, a.archBucket, "")
	if err != nil {
		return nil, err
	}
	byDay := map[string][]string{}
	for _, o := range objs {
		day, name, ok := strings.Cut(o.Key, "/")
		if !ok || name == "CURRENT" || !(strings.HasSuffix(name, ".data") || strings.HasSuffix(name, ".index")) {
			continue
		}
		byDay[day] = append(byDay[day], o.Key)
	}
	var removed []string
	for day, keys := range byDay {
		if switched[day] {
			continue
		}
		cur, err := readCurrent(ctx, a.st, a.archBucket, day)
		if err != nil {
			return removed, err
		}
		var drop []string
		for _, k := range keys {
			g := generationOf(strings.TrimPrefix(k, day+"/"))
			if g != cur && g < runGen && cur != "" {
				drop = append(drop, k)
			}
		}
		if len(drop) == 0 {
			continue
		}
		if err := a.st.Remove(ctx, a.archBucket, drop); err != nil {
			return removed, err
		}
		removed = append(removed, drop...)
	}
	sort.Strings(removed)
	return removed, nil
}

// oldReader walks the previous generation front to back; a repack asks for its entries in key
// order, which is the order they were written in.
type oldReader struct {
	st     store
	bucket string
	day    string
	gen    string
	ix     *archiveIndex
	dec    *zstd.Decoder
	frame  int
	next   int
}

func (r *oldReader) read(ctx context.Context, i int) ([]byte, error) {
	e := &r.ix.Entries[i]
	if e.Frame != r.frame || i < r.next {
		if r.dec == nil {
			d, err := newFrameDecoder(r.ix.WindowLog)
			if err != nil {
				return nil, err
			}
			r.dec = d
		}
		fr := r.ix.Frames[e.Frame]
		rc, err := r.st.GetRange(ctx, r.bucket, dataKey(r.day, r.gen), fr.Offset, fr.Length)
		if err != nil {
			return nil, err
		}
		b, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return nil, err
		}
		if err := r.dec.Reset(bytes.NewReader(b)); err != nil {
			return nil, err
		}
		r.frame = e.Frame
		r.next = sort.Search(len(r.ix.Entries), func(j int) bool { return r.ix.Entries[j].Frame >= e.Frame })
	}
	for ; r.next < i; r.next++ {
		if _, err := io.CopyN(io.Discard, r.dec, r.ix.Entries[r.next].Length); err != nil {
			return nil, err
		}
	}
	buf := make([]byte, e.Length)
	if _, err := io.ReadFull(r.dec, buf); err != nil {
		return nil, err
	}
	r.next = i + 1
	return buf, checkEntry(e, buf)
}

func (r *oldReader) close() {
	if r.dec != nil {
		r.dec.Close()
	}
}
