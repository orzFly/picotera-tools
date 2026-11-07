package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/klauspost/compress/zstd"
)

// Format v2 stores a day as two kinds of independent zstd frames in one .data object: blob frames
// first, holding every long base64 run (screenshots, mostly) once, then text frames holding each
// entry with its base64 runs replaced by references and, unless it is a keyframe, cut down to the
// part that differs from the previous entry of its kind. Entries are sorted so the turns of one
// conversation sit next to each other. On real traffic this came out 2.8 times smaller than v1
// with a quarter of the decoding per read.
const (
	indexV2         = 2
	blobMinRun      = 1000 // base64 characters; shorter runs stay inline
	keyEvery        = 32   // a keyframe per this many entries of one kind bounds a read's chain
	v2TextFrame     = 256 << 20
	v2BlobFrame     = 16 << 20
	v2BlobWindowLog = 26
	blobRef         = 0x00 // JSON text never carries a raw NUL, so it can mark a reference
)

const (
	frameText = "text"
	frameBlob = "blob"
)

type blobInfo struct {
	Frame  int    `json:"frame"`
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
	SHA256 string `json:"sha256"`
}

func isB64(c byte) bool {
	return c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '+' || c == '/'
}

// eachB64Run calls f for every run of at least blobMinRun base64 characters in b, with up to two
// '=' of padding included.
func eachB64Run(b []byte, f func(start, end int) error) error {
	start := -1
	for i := 0; i <= len(b); i++ {
		if i < len(b) && isB64(b[i]) {
			if start < 0 {
				start = i
			}
			continue
		}
		if start >= 0 && i-start >= blobMinRun {
			end := i
			for end < len(b) && b[end] == '=' && end-i < 2 {
				end++
			}
			if err := f(start, end); err != nil {
				return err
			}
			i = end - 1
		}
		start = -1
	}
	return nil
}

// stripBlobs replaces long base64 runs with blobRef + uvarint(id). An entry holding a raw NUL is
// returned as is with literal set, since a reference could not be told apart from it.
func stripBlobs(raw []byte, id func(run []byte) (int, error)) (out []byte, literal bool, err error) {
	if bytes.IndexByte(raw, blobRef) >= 0 {
		return raw, true, nil
	}
	last := 0
	err = eachB64Run(raw, func(start, end int) error {
		n, err := id(raw[start:end])
		if err != nil {
			return err
		}
		out = append(out, raw[last:start]...)
		out = append(out, blobRef)
		out = binary.AppendUvarint(out, uint64(n))
		last = end
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if out == nil {
		return raw, false, nil
	}
	return append(out, raw[last:]...), false, nil
}

// expandBlobs undoes stripBlobs.
func expandBlobs(stripped []byte, literal bool, blob func(id int) ([]byte, error)) ([]byte, error) {
	if literal || bytes.IndexByte(stripped, blobRef) < 0 {
		return stripped, nil
	}
	out := make([]byte, 0, len(stripped)*2)
	for {
		i := bytes.IndexByte(stripped, blobRef)
		if i < 0 {
			return append(out, stripped...), nil
		}
		out = append(out, stripped[:i]...)
		id, n := binary.Uvarint(stripped[i+1:])
		if n <= 0 {
			return nil, errors.New("corrupt blob reference")
		}
		b, err := blob(int(id))
		if err != nil {
			return nil, err
		}
		out = append(out, b...)
		stripped = stripped[i+1+n:]
	}
}

func commonPrefix(a, b []byte) int {
	n := min(len(a), len(b))
	i := 0
	for i+8 <= n && binary.LittleEndian.Uint64(a[i:]) == binary.LittleEndian.Uint64(b[i:]) {
		i += 8
	}
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

func commonSuffix(a, b []byte, limit int) int {
	i := 0
	for i < limit && a[len(a)-1-i] == b[len(b)-1-i] {
		i++
	}
	return i
}

// deltaOf returns the middle of cur that differs from parent, with the shared prefix and suffix.
func deltaOf(parent, cur []byte) (mid []byte, prefix, suffix int) {
	p := commonPrefix(parent, cur)
	s := commonSuffix(parent, cur, min(len(parent), len(cur))-p)
	return cur[p : len(cur)-s], p, s
}

func applyDelta(parent, mid []byte, prefix, suffix int) ([]byte, error) {
	if prefix+suffix > len(parent) {
		return nil, errors.New("delta reaches past its parent")
	}
	out := make([]byte, 0, prefix+len(mid)+suffix)
	out = append(out, parent[:prefix]...)
	out = append(out, mid...)
	return append(out, parent[len(parent)-suffix:]...), nil
}

// sortKey approximates sorting entries by body: hashes of the body's first 1K, 4K, ... 4M bytes,
// compared level by level, then the length. Entries sharing a longer prefix land closer, which is
// what puts a conversation's turns together, and it needs no body in memory to sort.
type sortKey struct {
	levels [7]uint64
	n      int
}

var sortLevels = [7]int{1 << 10, 4 << 10, 16 << 10, 64 << 10, 256 << 10, 1 << 20, 4 << 20}

func makeSortKey(stripped []byte) sortKey {
	b := stripped
	if i := bytes.Index(b, []byte(`"body":"`)); i >= 0 {
		b = b[i:]
	}
	k := sortKey{n: len(b)}
	for l, size := range sortLevels {
		h := sha256.Sum256(b[:min(size, len(b))])
		k.levels[l] = binary.BigEndian.Uint64(h[:8])
	}
	return k
}

func (a sortKey) less(b sortKey) bool {
	for l := range a.levels {
		if a.levels[l] != b.levels[l] {
			return a.levels[l] < b.levels[l]
		}
	}
	return a.n < b.n
}

// unitID is the artifact id shared by a request and its response.
func unitID(key string) (id string, isReq bool) {
	k := key[bytes.LastIndexByte([]byte(key), '/')+1:]
	if len(k) > len(".request.json.zst") && k[len(k)-len(".request.json.zst"):] == ".request.json.zst" {
		return k[:len(k)-len(".request.json.zst")], true
	}
	if len(k) > len(".response.json.zst") && k[len(k)-len(".response.json.zst"):] == ".response.json.zst" {
		return k[:len(k)-len(".response.json.zst")], false
	}
	return k, true
}

// sortV2 orders entries as v2 stores them: each request with its response right after it, pairs
// ordered by the request's sort key (the response's when there is no request).
func sortV2(keys []string, sk []sortKey) []int {
	type unit struct {
		req, resp int
	}
	units := map[string]*unit{}
	var ids []string
	for i, k := range keys {
		id, isReq := unitID(k)
		u := units[id]
		if u == nil {
			u = &unit{-1, -1}
			units[id] = u
			ids = append(ids, id)
		}
		if isReq {
			u.req = i
		} else {
			u.resp = i
		}
	}
	lead := func(u *unit) int {
		if u.req >= 0 {
			return u.req
		}
		return u.resp
	}
	sort.SliceStable(ids, func(a, b int) bool {
		ka, kb := sk[lead(units[ids[a]])], sk[lead(units[ids[b]])]
		if ka.less(kb) || kb.less(ka) {
			return ka.less(kb)
		}
		return ids[a] < ids[b]
	})
	order := make([]int, 0, len(keys))
	for _, id := range ids {
		u := units[id]
		if u.req >= 0 {
			order = append(order, u.req)
		}
		if u.resp >= 0 {
			order = append(order, u.resp)
		}
	}
	return order
}

// framer packs byte strings into independent zstd frames of at most frameSize raw bytes each,
// appending them to out and their descriptions to ix.Frames.
type framer struct {
	out       *countingWriter
	enc       *zstd.Encoder
	ix        *archiveIndex
	kind      string
	frameSize int64
	open      bool
	start     int64
	raw       int64
}

func newFramer(out *countingWriter, ix *archiveIndex, kind string, frameSize int64, windowLog int, level zstd.EncoderLevel) (*framer, error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level), zstd.WithWindowSize(1<<windowLog), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	return &framer{out: out, enc: enc, ix: ix, kind: kind, frameSize: frameSize}, nil
}

// add writes b and returns the frame it went into and its offset there.
func (f *framer) add(b []byte) (int, int64, error) {
	if f.open && f.raw > 0 && f.raw+int64(len(b)) > f.frameSize {
		if err := f.closeFrame(); err != nil {
			return 0, 0, err
		}
	}
	if !f.open {
		f.enc.Reset(f.out)
		f.open, f.start, f.raw = true, f.out.n, 0
	}
	if _, err := f.enc.Write(b); err != nil {
		return 0, 0, err
	}
	off := f.raw
	f.raw += int64(len(b))
	return len(f.ix.Frames), off, nil
}

func (f *framer) closeFrame() error {
	if err := f.enc.Close(); err != nil {
		return err
	}
	f.ix.Frames = append(f.ix.Frames, frameInfo{Kind: f.kind, Offset: f.start, Length: f.out.n - f.start, RawLength: f.raw})
	f.open = false
	return nil
}

func (f *framer) close() error {
	if f.open {
		return f.closeFrame()
	}
	return nil
}

func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// v2Walker reconstructs v2 entries by decoding text frames front to back. It keeps the last
// stripped entry of each kind, which is exactly the parent the next entry of that kind refers
// to; jumping backwards, or to an entry whose chain starts in an earlier frame, restarts at the
// frame holding the chain's keyframe.
type v2Walker struct {
	ix           *archiveIndex
	frame        func(fi int) ([]byte, error) // compressed bytes of a frame
	blob         func(id int) ([]byte, error)
	dec          *zstd.Decoder
	cur          int // frame being decoded, -1 for none
	next         int // index of the next entry in cur
	last         map[bool][]byte
	lastAt       map[bool]int // entry index of last[kind]
	firstOfFrame []int
}

func newV2Walker(ix *archiveIndex, frame func(int) ([]byte, error), blob func(int) ([]byte, error)) (*v2Walker, error) {
	dec, err := newFrameDecoder(defaultWindowLog)
	if err != nil {
		return nil, err
	}
	w := &v2Walker{ix: ix, frame: frame, blob: blob, dec: dec, cur: -1, last: map[bool][]byte{}, lastAt: map[bool]int{true: -1, false: -1}}
	w.firstOfFrame = make([]int, len(ix.Frames)+1)
	for fi := range w.firstOfFrame {
		w.firstOfFrame[fi] = sort.Search(len(ix.Entries), func(i int) bool { return ix.Entries[i].Frame >= fi })
	}
	return w, nil
}

func (w *v2Walker) close() { w.dec.Close() }

func (w *v2Walker) isReq(i int) bool {
	_, r := unitID(w.ix.Entries[i].Key)
	return r
}

// root is the keyframe the chain of entry i starts from.
func (w *v2Walker) root(i int) int {
	for w.ix.Entries[i].Parent >= 0 {
		i = w.ix.Entries[i].Parent
	}
	return i
}

func (w *v2Walker) open(fi int) error {
	b, err := w.frame(fi)
	if err != nil {
		return err
	}
	if err := w.dec.Reset(bytes.NewReader(b)); err != nil {
		return err
	}
	w.cur, w.next = fi, w.firstOfFrame[fi]
	return nil
}

// step decodes the next entry of the current frame, moving to the following text frame when the
// current one is done, and returns its index and stripped text (nil when its parent is unknown).
func (w *v2Walker) step() (int, []byte, error) {
	for w.cur < 0 || w.next >= w.firstOfFrame[w.cur+1] {
		fi := w.cur + 1
		for fi < len(w.ix.Frames) && w.ix.Frames[fi].Kind != frameText {
			fi++
		}
		if fi >= len(w.ix.Frames) {
			return -1, nil, io.EOF
		}
		if err := w.open(fi); err != nil {
			return -1, nil, err
		}
	}
	i := w.next
	e := &w.ix.Entries[i]
	mid := make([]byte, e.Length)
	if _, err := io.ReadFull(w.dec, mid); err != nil {
		return -1, nil, fmt.Errorf("%s: %w", e.Key, err)
	}
	w.next++
	kind := w.isReq(i)
	var stripped []byte
	if e.Parent < 0 {
		stripped = mid
	} else if w.lastAt[kind] == e.Parent {
		s, err := applyDelta(w.last[kind], mid, e.Prefix, e.Suffix)
		if err != nil {
			return -1, nil, fmt.Errorf("%s: %w", e.Key, err)
		}
		stripped = s
	}
	if stripped != nil {
		w.last[kind], w.lastAt[kind] = stripped, i
	} else {
		w.lastAt[kind] = -1
	}
	return i, stripped, nil
}

// seek positions the walker so that the next steps reach entry i with its parent known.
func (w *v2Walker) seek(i int) error {
	r := w.root(i)
	kind := w.isReq(i)
	inPlace := w.cur >= 0 && w.next <= i && (w.next <= r || w.lastAt[kind] >= r)
	if inPlace {
		return nil
	}
	w.last, w.lastAt = map[bool][]byte{}, map[bool]int{true: -1, false: -1}
	return w.open(w.ix.Entries[r].Frame)
}

// raw reconstructs entry i, verifying it against its recorded sha256.
func (w *v2Walker) raw(i int) ([]byte, error) {
	if err := w.seek(i); err != nil {
		return nil, err
	}
	for {
		j, stripped, err := w.step()
		if err != nil {
			return nil, err
		}
		if j < i {
			continue
		}
		if j != i || stripped == nil {
			return nil, fmt.Errorf("%s: chain broken", w.ix.Entries[i].Key)
		}
		return w.expand(i, stripped)
	}
}

func (w *v2Walker) expand(i int, stripped []byte) ([]byte, error) {
	e := &w.ix.Entries[i]
	raw, err := expandBlobs(stripped, e.Literal, w.blob)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", e.Key, err)
	}
	if int64(len(raw)) != e.RawLength || sha256Hex(raw) != e.SHA256 {
		return nil, fmt.Errorf("%s: sha256 mismatch", e.Key)
	}
	return raw, nil
}

// blobReader serves blobs out of their frames, keeping a few decoded blob frames.
type blobReader struct {
	ix    *archiveIndex
	frame func(fi int) ([]byte, error)
	cache map[int][]byte
	order []int
	keep  int
}

func newBlobReader(ix *archiveIndex, frame func(int) ([]byte, error), keep int) *blobReader {
	return &blobReader{ix: ix, frame: frame, cache: map[int][]byte{}, keep: keep}
}

func (r *blobReader) blob(id int) ([]byte, error) {
	if id < 0 || id >= len(r.ix.Blobs) {
		return nil, fmt.Errorf("blob %d out of range", id)
	}
	b := r.ix.Blobs[id]
	raw, ok := r.cache[b.Frame]
	if !ok {
		comp, err := r.frame(b.Frame)
		if err != nil {
			return nil, err
		}
		dec, err := zstd.NewReader(nil, zstd.WithDecoderMaxWindow(1<<v2BlobWindowLog), zstd.WithDecoderConcurrency(1))
		if err != nil {
			return nil, err
		}
		raw, err = dec.DecodeAll(comp, nil)
		dec.Close()
		if err != nil {
			return nil, fmt.Errorf("blob frame %d: %w", b.Frame, err)
		}
		r.cache[b.Frame] = raw
		r.order = append(r.order, b.Frame)
		if len(r.order) > r.keep {
			delete(r.cache, r.order[0])
			r.order = r.order[1:]
		}
	}
	if b.Offset+b.Length > int64(len(raw)) {
		return nil, fmt.Errorf("blob %d past the end of its frame", id)
	}
	return raw[b.Offset : b.Offset+b.Length], nil
}
