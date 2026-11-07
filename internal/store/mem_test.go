package store

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"sort"
	"strings"
	"sync"
)

type memObject struct {
	data []byte
	ct   string
}

type memStore struct {
	mu      sync.Mutex
	objects map[string]memObject // bucket + "/" + key
	// corrupt flips a byte in whatever is put under a matching key, to exercise verify.
	corrupt func(key string) bool
}

func newMemStore() *memStore { return &memStore{objects: map[string]memObject{}} }

func (m *memStore) Get(_ context.Context, bucket, key string) (io.ReadCloser, objectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objects[bucket+"/"+key]
	if !ok {
		return nil, objectInfo{}, errNotFound
	}
	return io.NopCloser(bytes.NewReader(o.data)), objectInfo{Key: key, Size: int64(len(o.data)), ContentType: o.ct}, nil
}

func (m *memStore) GetRange(_ context.Context, bucket, key string, off, length int64) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objects[bucket+"/"+key]
	if !ok {
		return nil, errNotFound
	}
	return io.NopCloser(bytes.NewReader(o.data[off : off+length])), nil
}

func (m *memStore) Put(_ context.Context, bucket, key string, r io.Reader, _ int64, ct string) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if m.corrupt != nil && m.corrupt(key) && len(b) > 0 {
		b[len(b)/2] ^= 0xff
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[bucket+"/"+key] = memObject{data: b, ct: ct}
	return nil
}

func (m *memStore) List(_ context.Context, bucket, prefix string) ([]objectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []objectInfo
	for k, o := range m.objects {
		b, key, _ := strings.Cut(k, "/")
		if b == bucket && strings.HasPrefix(key, prefix) {
			sum := md5.Sum(o.data)
			out = append(out, objectInfo{Key: key, Size: int64(len(o.data)), ETag: hex.EncodeToString(sum[:])})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (m *memStore) ListPrefixes(ctx context.Context, bucket, prefix string) ([]string, error) {
	objs, _ := m.List(ctx, bucket, prefix)
	seen := map[string]bool{}
	var out []string
	for _, o := range objs {
		rest := strings.TrimPrefix(o.Key, prefix)
		if i := strings.Index(rest, "/"); i >= 0 {
			p := prefix + rest[:i+1]
			if !seen[p] {
				seen[p] = true
				out = append(out, p)
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

func (m *memStore) Remove(_ context.Context, bucket string, keys []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range keys {
		delete(m.objects, bucket+"/"+k)
	}
	return nil
}

func (m *memStore) has(bucket, key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.objects[bucket+"/"+key]
	return ok
}
