package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	indexVersion = 1
	// Measured on a day of real traffic: 1 GiB frames give 44.7x with a 256 MiB window against
	// 48.5x with 1 GiB, at a quarter of the memory a reader must hold.
	defaultFrameSize = 1 << 30
	defaultWindowLog = 28
	currentTTL       = 60 * time.Second
	currentRefresh   = time.Second // least age of a cached CURRENT before a miss re-reads it
)

type frameInfo struct {
	Kind      string `json:"kind,omitempty"` // v2: frameText or frameBlob
	Offset    int64  `json:"offset"`
	Length    int64  `json:"length"`
	RawLength int64  `json:"raw_length"`
}

type entry struct {
	Key         string `json:"key"`
	Frame       int    `json:"frame"`
	Offset      int64  `json:"offset"`
	Length      int64  `json:"length"`
	SHA256      string `json:"sha256"`
	ContentType string `json:"content_type"`
	// Size of the original object, so a repacked day still reports a meaningful ratio.
	StoredSize int64 `json:"stored_size"`
	// ETag of the original object; a loose object with the same key and ETag is already in.
	ETag string `json:"etag,omitempty"`

	// v2 only. Length is then the stored middle part, not the entry.
	Parent    int   `json:"parent"` // entry this one is a delta of, -1 for a keyframe
	Prefix    int   `json:"prefix,omitempty"`
	Suffix    int   `json:"suffix,omitempty"`
	Literal   bool  `json:"literal,omitempty"` // holds a raw NUL, so no blob references
	RawLength int64 `json:"raw_length,omitempty"`
}

type archiveIndex struct {
	Version    int         `json:"version"`
	Day        string      `json:"day"`
	Generation string      `json:"generation"`
	WindowLog  int         `json:"window_log"`
	Frames     []frameInfo `json:"frames"`
	Blobs      []blobInfo  `json:"blobs,omitempty"`
	Entries    []entry     `json:"entries"`

	byKey map[string]int
}

func (ix *archiveIndex) lookup(key string) (int, bool) {
	if ix.byKey == nil {
		ix.byKey = make(map[string]int, len(ix.Entries))
		for i, e := range ix.Entries {
			ix.byKey[e.Key] = i
		}
	}
	i, ok := ix.byKey[key]
	return i, ok
}

func dataKey(day, gen string) string  { return day + "/" + gen + ".data" }
func indexKey(day, gen string) string { return day + "/" + gen + ".index" }
func currentKey(day string) string    { return day + "/CURRENT" }
func generationOf(name string) string {
	return strings.TrimSuffix(strings.TrimSuffix(name, ".data"), ".index")
}
func generationNow(t time.Time) string { return t.UTC().Format("20060102T150405Z") }
func dayOfKey(key string) (string, bool) {
	// artifacts/<day>/<file>, as picotera's RequestKey and ResponseKey write it.
	parts := strings.Split(key, "/")
	if len(parts) != 3 || parts[0] != "artifacts" {
		return "", false
	}
	if _, err := time.Parse("2006-01-02", parts[1]); err != nil {
		return "", false
	}
	return parts[1], true
}

func encodeIndex(ix *archiveIndex) ([]byte, error) {
	raw, err := json.Marshal(ix)
	if err != nil {
		return nil, err
	}
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	defer enc.Close()
	return enc.EncodeAll(raw, nil), nil
}

func decodeIndex(b []byte) (*archiveIndex, error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	raw, err := dec.DecodeAll(b, nil)
	if err != nil {
		return nil, fmt.Errorf("index: %w", err)
	}
	var ix archiveIndex
	if err := json.Unmarshal(raw, &ix); err != nil {
		return nil, fmt.Errorf("index: %w", err)
	}
	if ix.Version != indexVersion && ix.Version != indexV2 {
		return nil, fmt.Errorf("index: unknown version %d", ix.Version)
	}
	return &ix, nil
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// frameWriter packs entries into independent zstd frames, so a reader only ever decodes the one
// frame its entry sits in. Entries never straddle frames.
type frameWriter struct {
	out       *countingWriter
	enc       *zstd.Encoder
	frameSize int64
	ix        *archiveIndex
	open      bool
	start     int64
	raw       int64
}

func newFrameWriter(w io.Writer, ix *archiveIndex, frameSize int64, level zstd.EncoderLevel) (*frameWriter, error) {
	out := &countingWriter{w: w}
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(level),
		zstd.WithWindowSize(1<<ix.WindowLog),
		zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	return &frameWriter{out: out, enc: enc, frameSize: frameSize, ix: ix}, nil
}

func (fw *frameWriter) add(key, contentType, etag string, storedSize int64, raw []byte) error {
	if fw.open && fw.raw > 0 && fw.raw+int64(len(raw)) > fw.frameSize {
		if err := fw.closeFrame(); err != nil {
			return err
		}
	}
	if !fw.open {
		fw.enc.Reset(fw.out)
		fw.open, fw.start, fw.raw = true, fw.out.n, 0
	}
	if _, err := fw.enc.Write(raw); err != nil {
		return err
	}
	sum := sha256.Sum256(raw)
	fw.ix.Entries = append(fw.ix.Entries, entry{
		Key: key, Frame: len(fw.ix.Frames), Offset: fw.raw, Length: int64(len(raw)),
		SHA256: hex.EncodeToString(sum[:]), ContentType: contentType, StoredSize: storedSize, ETag: etag,
	})
	fw.raw += int64(len(raw))
	return nil
}

func (fw *frameWriter) closeFrame() error {
	if err := fw.enc.Close(); err != nil {
		return err
	}
	fw.ix.Frames = append(fw.ix.Frames, frameInfo{Offset: fw.start, Length: fw.out.n - fw.start, RawLength: fw.raw})
	fw.open = false
	return nil
}

func (fw *frameWriter) close() error {
	if fw.open {
		return fw.closeFrame()
	}
	return nil
}

func newFrameDecoder(windowLog int) (*zstd.Decoder, error) {
	return zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxWindow(1<<windowLog),
		zstd.WithDecoderLowmem(true))
}

// walkFrame decodes one frame from its start and hands every entry in it to visit, in order.
// visit returning false stops early; nothing before the entry being visited is retained here.
func walkFrame(dec *zstd.Decoder, compressed io.Reader, ix *archiveIndex, frame int, visit func(i int, raw []byte) (bool, error)) error {
	if err := dec.Reset(compressed); err != nil {
		return err
	}
	var pos int64
	// Entries are written frame by frame, so each frame's entries are one contiguous run.
	first := sort.Search(len(ix.Entries), func(i int) bool { return ix.Entries[i].Frame >= frame })
	for i := first; i < len(ix.Entries) && ix.Entries[i].Frame == frame; i++ {
		e := &ix.Entries[i]
		if e.Offset != pos {
			return fmt.Errorf("frame %d: entry %s at %d, expected %d", frame, e.Key, e.Offset, pos)
		}
		buf := make([]byte, e.Length)
		if _, err := io.ReadFull(dec, buf); err != nil {
			return fmt.Errorf("frame %d: entry %s: %w", frame, e.Key, err)
		}
		pos += e.Length
		more, err := visit(i, buf)
		if err != nil || !more {
			return err
		}
	}
	return nil
}

func checkEntry(e *entry, raw []byte) error {
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != e.SHA256 {
		return fmt.Errorf("%s: sha256 mismatch", e.Key)
	}
	return nil
}

// httpEncoder re-compresses entries the way picotera stored them. Browsers decode
// Content-Encoding: zstd themselves and refuse windows over 8 MiB, and EncodeAll would otherwise
// write a single-segment frame whose window is the whole entry. One shared encoder: EncodeAll is
// safe concurrently, and a fresh encoder per entry churned through hundreds of MiB of buffers.
var httpEncoder, _ = zstd.NewWriter(nil,
	zstd.WithEncoderLevel(zstd.SpeedFastest),
	zstd.WithWindowSize(1<<23),
	zstd.WithSingleSegment(false))

func encodeForHTTP(raw []byte) ([]byte, error) {
	return httpEncoder.EncodeAll(raw, make([]byte, 0, len(raw)/2)), nil
}
