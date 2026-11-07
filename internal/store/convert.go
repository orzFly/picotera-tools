package store

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

// convertDay rewrites the v1 generation of day as v2. The v1 frames are 1 GiB each and would
// have to be read in v2's sorted order, so the whole day is read into memory first, each entry
// compressed on its own: about the size of the day's original objects, 20-odd GiB for the
// largest days so far. Run it where there is room for that. In place, days that still have loose objects
// are left to the daily archive run, which owns their CURRENT; a trial into another bucket is not.
func convertDay(ctx context.Context, st store, bucket, archBucket, outBucket, day string, now func() time.Time, log *slog.Logger) (dayStatus, error) {
	cur, err := readCurrent(ctx, st, archBucket, day)
	if err != nil {
		return dayStatus{}, err
	}
	if cur == "" {
		return dayStatus{}, fmt.Errorf("%s is not archived", day)
	}
	ix, err := readIndex(ctx, st, archBucket, day, cur)
	if err != nil {
		return dayStatus{}, err
	}
	if ix.Version == indexV2 && outBucket == archBucket {
		return dayStatus{Day: day, Generation: cur, UpToDate: true}, nil
	}
	if ix.Version != indexVersion {
		return dayStatus{}, fmt.Errorf("%s: cannot convert from version %d", day, ix.Version)
	}
	if outBucket == archBucket {
		loose, err := st.List(ctx, bucket, "artifacts/"+day+"/")
		if err != nil {
			return dayStatus{}, err
		}
		if len(loose) > 0 {
			return dayStatus{}, fmt.Errorf("%s still has %d loose objects; let the archive run take it", day, len(loose))
		}
	}

	t0 := time.Now()
	mem, entries, err := loadV1(ctx, st, archBucket, day, cur, ix)
	if err != nil {
		return dayStatus{}, err
	}
	log.Info("loaded v1 generation", "day", day, "entries", len(entries), "seconds", int(time.Since(t0).Seconds()))

	a := &archiver{st: st, bucket: bucket, archBucket: outBucket, format: "v2", windowLog: defaultWindowLog,
		level: zstd.SpeedBestCompression, fetchers: 8, now: now, log: log, mem: mem}
	ds, _ := a.archiveDayV2(ctx, day, generationNow(now()), entries, "", nil)
	if ds.Error != "" {
		return ds, errors.New(ds.Error)
	}
	return ds, nil
}

// loadV1 reads a whole v1 generation into memory, each entry checked against its sha256 and
// kept zstd-compressed on its own, so a day costs about the size of its original objects.
func loadV1(ctx context.Context, st store, archBucket, day, cur string, ix *archiveIndex) (map[string]memEntry, []objectInfo, error) {
	mem := make(map[string]memEntry, len(ix.Entries))
	entries := make([]objectInfo, 0, len(ix.Entries))
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, nil, err
	}
	defer enc.Close()
	dec, err := newFrameDecoder(ix.WindowLog)
	if err != nil {
		return nil, nil, err
	}
	defer dec.Close()
	for fi, fr := range ix.Frames {
		rc, err := st.GetRange(ctx, archBucket, dataKey(day, cur), fr.Offset, fr.Length)
		if err != nil {
			return nil, nil, err
		}
		err = walkFrame(dec, rc, ix, fi, func(i int, raw []byte) (bool, error) {
			e := &ix.Entries[i]
			if err := checkEntry(e, raw); err != nil {
				return false, err
			}
			info := objectInfo{Key: e.Key, Size: e.StoredSize, ContentType: e.ContentType, ETag: e.ETag}
			mem[e.Key] = memEntry{comp: enc.EncodeAll(raw, nil), info: info}
			entries = append(entries, info)
			return true, nil
		})
		rc.Close()
		if err != nil {
			return nil, nil, fmt.Errorf("%s frame %d: %w", day, fi, err)
		}
	}
	return mem, entries, nil
}

type dayList []string

func (d *dayList) String() string { return strings.Join(*d, ",") }
func (d *dayList) Set(v string) error {
	if _, err := time.Parse("2006-01-02", v); err != nil {
		return fmt.Errorf("day %q: want YYYY-MM-DD", v)
	}
	*d = append(*d, v)
	return nil
}

func cmdConvert(ctx context.Context, args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("convert", flag.ExitOnError)
	var c commonFlags
	c.register(fs)
	var days dayList
	fs.Var(&days, "day", "day to convert (YYYY-MM-DD); repeat for several")
	out := fs.String("out-bucket", "", "write the v2 generation here instead of --archive-bucket (a trial run)")
	fs.Parse(args)
	if len(days) == 0 {
		return errors.New("--day is required")
	}
	st, err := c.store()
	if err != nil {
		return err
	}
	outBucket := c.archBucket
	if *out != "" {
		outBucket = *out
	}
	var failed []string
	for _, d := range days {
		ds, err := convertDay(ctx, st, c.s3.bucket, c.archBucket, outBucket, d, time.Now, log)
		if err != nil {
			log.Error("convert failed", "day", d, "err", err)
			failed = append(failed, d)
			continue
		}
		if ds.UpToDate {
			log.Info("already v2", "day", d)
			continue
		}
		log.Info("converted", "day", d, "entries", ds.Entries, "archive_bytes", ds.ArchiveBytes,
			"ratio", fmt.Sprintf("%.1f", ds.Ratio), "seconds", int(ds.Seconds))
	}
	if len(failed) > 0 {
		return fmt.Errorf("failed days %v", failed)
	}
	return nil
}
