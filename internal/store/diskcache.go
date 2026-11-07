package store

import (
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// diskCache keeps reads of a deployment's archive on local disk: worth it where picotera talks
// to a remote S3 that bills for traffic. Only immutable objects are cached, the .index and the
// byte ranges of .data, whose names carry a generation that is never reused, so nothing needs
// invalidating. CURRENT and the run status always go to S3.
type diskCache struct {
	store
	dir    string
	bucket string
	max    int64

	mu    sync.Mutex
	cur   int64
	ll    *list.List // front = most recently used
	items map[string]*list.Element
}

type diskItem struct {
	name string
	size int64
}

func newDiskCache(inner store, dir, bucket string, max int64) (*diskCache, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	c := &diskCache{store: inner, dir: dir, bucket: bucket, max: max, ll: list.New(), items: map[string]*list.Element{}}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	type found struct {
		name string
		size int64
		mod  time.Time
	}
	var fs []found
	for _, e := range ents {
		path := filepath.Join(dir, e.Name())
		if strings.HasSuffix(e.Name(), ".tmp") {
			os.Remove(path) // left by a process killed mid-write
			continue
		}
		info, err := e.Info()
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		fs = append(fs, found{e.Name(), info.Size(), info.ModTime()})
	}
	// Oldest first, so pushing each to the front leaves the newest there.
	sort.Slice(fs, func(a, b int) bool { return fs[a].mod.Before(fs[b].mod) })
	for _, f := range fs {
		c.items[f.name] = c.ll.PushFront(&diskItem{f.name, f.size})
		c.cur += f.size
	}
	c.mu.Lock()
	c.evict()
	c.mu.Unlock()
	return c, nil
}

func cacheable(key string) bool {
	return strings.HasSuffix(key, ".data") || strings.HasSuffix(key, ".index")
}

func (c *diskCache) name(key string, off, length int64) string {
	h := sha256.Sum256([]byte(c.bucket + "\x00" + key + "\x00" + strconv.FormatInt(off, 10) + "\x00" + strconv.FormatInt(length, 10)))
	return hex.EncodeToString(h[:16])
}

// lookup returns the cached bytes, or nil. want < 0 accepts any length.
func (c *diskCache) lookup(name string, want int64) []byte {
	c.mu.Lock()
	el, ok := c.items[name]
	if ok {
		c.ll.MoveToFront(el)
	}
	c.mu.Unlock()
	if !ok {
		return nil
	}
	path := filepath.Join(c.dir, name)
	b, err := os.ReadFile(path)
	if err != nil || (want >= 0 && int64(len(b)) != want) {
		c.drop(name)
		return nil
	}
	now := time.Now()
	os.Chtimes(path, now, now) // recency survives a restart
	return b
}

func (c *diskCache) put(name string, b []byte) {
	if int64(len(b)) > c.max {
		return
	}
	tmp, err := os.CreateTemp(c.dir, name+".*.tmp")
	if err != nil {
		return
	}
	_, werr := tmp.Write(b)
	cerr := tmp.Close()
	if werr != nil || cerr != nil || os.Rename(tmp.Name(), filepath.Join(c.dir, name)) != nil {
		os.Remove(tmp.Name())
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[name]; ok {
		c.cur -= el.Value.(*diskItem).size
		c.ll.Remove(el)
	}
	c.items[name] = c.ll.PushFront(&diskItem{name, int64(len(b))})
	c.cur += int64(len(b))
	c.evict()
}

func (c *diskCache) drop(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[name]; ok {
		c.remove(el)
	}
}

// evict needs c.mu held.
func (c *diskCache) evict() {
	for c.cur > c.max && c.ll.Len() > 0 {
		c.remove(c.ll.Back())
	}
}

func (c *diskCache) remove(el *list.Element) {
	it := el.Value.(*diskItem)
	c.ll.Remove(el)
	delete(c.items, it.name)
	c.cur -= it.size
	os.Remove(filepath.Join(c.dir, it.name))
}

func (c *diskCache) Get(ctx context.Context, bucket, key string) (io.ReadCloser, objectInfo, error) {
	if bucket != c.bucket || !cacheable(key) {
		return c.store.Get(ctx, bucket, key)
	}
	name := c.name(key, 0, -1)
	if b := c.lookup(name, -1); b != nil {
		return io.NopCloser(bytes.NewReader(b)), objectInfo{Key: key, Size: int64(len(b))}, nil
	}
	rc, info, err := c.store.Get(ctx, bucket, key)
	if err != nil {
		return nil, info, err
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		return nil, info, err
	}
	c.put(name, b)
	return io.NopCloser(bytes.NewReader(b)), info, nil
}

func (c *diskCache) GetRange(ctx context.Context, bucket, key string, off, length int64) (io.ReadCloser, error) {
	if bucket != c.bucket || !cacheable(key) {
		return c.store.GetRange(ctx, bucket, key, off, length)
	}
	name := c.name(key, off, length)
	if b := c.lookup(name, length); b != nil {
		return io.NopCloser(bytes.NewReader(b)), nil
	}
	rc, err := c.store.GetRange(ctx, bucket, key, off, length)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != length {
		return nil, fmt.Errorf("%s: got %d bytes at %d, want %d", key, len(b), off, length)
	}
	c.put(name, b)
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (c *diskCache) size() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cur
}

// parseSize reads sizes like 1073741824, 512MiB or 2GiB.
func parseSize(s string) (int64, error) {
	units := []struct {
		suffix string
		mult   int64
	}{{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"B", 1}, {"", 1}}
	for _, u := range units {
		if n, ok := strings.CutSuffix(s, u.suffix); ok {
			v, err := strconv.ParseInt(n, 10, 64)
			if err != nil || v < 0 {
				break
			}
			return v * u.mult, nil
		}
	}
	return 0, fmt.Errorf("size %q: want a number with an optional KiB, MiB or GiB", s)
}
