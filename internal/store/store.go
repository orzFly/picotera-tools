package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var errNotFound = errors.New("not found")

type objectInfo struct {
	Key         string
	Size        int64
	ContentType string
	ETag        string // only List fills it
}

// store is the slice of S3 the archive and the fallback use; tests swap in memStore.
type store interface {
	Get(ctx context.Context, bucket, key string) (io.ReadCloser, objectInfo, error)
	GetRange(ctx context.Context, bucket, key string, off, length int64) (io.ReadCloser, error)
	Put(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string) error
	List(ctx context.Context, bucket, prefix string) ([]objectInfo, error)
	ListPrefixes(ctx context.Context, bucket, prefix string) ([]string, error)
	Remove(ctx context.Context, bucket string, keys []string) error
}

type minioStore struct{ c *minio.Client }

type s3Config struct {
	endpoint, accessKey, secretKey, bucket, region string
	useSSL, pathStyle                              bool
}

func newMinioStore(cfg s3Config) (*minioStore, error) {
	if cfg.accessKey == "" || cfg.secretKey == "" {
		return nil, errors.New("PICOTERA_S3_ACCESS_KEY and PICOTERA_S3_SECRET_KEY must be set")
	}
	lookup := minio.BucketLookupDNS
	if cfg.pathStyle {
		lookup = minio.BucketLookupPath
	}
	c, err := minio.New(cfg.endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(cfg.accessKey, cfg.secretKey, ""),
		Secure:       cfg.useSSL,
		Region:       cfg.region,
		BucketLookup: lookup,
	})
	if err != nil {
		return nil, err
	}
	return &minioStore{c: c}, nil
}

func isNoSuchKey(err error) bool {
	return minio.ToErrorResponse(err).Code == "NoSuchKey"
}

func (s *minioStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, objectInfo, error) {
	obj, err := s.c.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, objectInfo{}, err
	}
	// GetObject is lazy; Stat is what surfaces a missing key.
	st, err := obj.Stat()
	if err != nil {
		obj.Close()
		if isNoSuchKey(err) {
			return nil, objectInfo{}, errNotFound
		}
		return nil, objectInfo{}, err
	}
	return obj, objectInfo{Key: key, Size: st.Size, ContentType: st.ContentType}, nil
}

func (s *minioStore) GetRange(ctx context.Context, bucket, key string, off, length int64) (io.ReadCloser, error) {
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(off, off+length-1); err != nil {
		return nil, err
	}
	obj, err := s.c.GetObject(ctx, bucket, key, opts)
	if err != nil {
		return nil, err
	}
	if _, err := obj.Stat(); err != nil {
		obj.Close()
		if isNoSuchKey(err) {
			return nil, errNotFound
		}
		return nil, err
	}
	return obj, nil
}

func (s *minioStore) Put(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string) error {
	_, err := s.c.PutObject(ctx, bucket, key, r, size, minio.PutObjectOptions{
		ContentType: contentType,
		// Unknown sizes go multipart; 64 MiB parts keep a day's .data under the 10000-part cap.
		PartSize: 64 << 20,
	})
	return err
}

func (s *minioStore) List(ctx context.Context, bucket, prefix string) ([]objectInfo, error) {
	var out []objectInfo
	for o := range s.c.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if o.Err != nil {
			return nil, o.Err
		}
		out = append(out, objectInfo{Key: o.Key, Size: o.Size, ETag: strings.Trim(o.ETag, `"`)})
	}
	return out, nil
}

func (s *minioStore) ListPrefixes(ctx context.Context, bucket, prefix string) ([]string, error) {
	var out []string
	for o := range s.c.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix}) {
		if o.Err != nil {
			return nil, o.Err
		}
		out = append(out, o.Key)
	}
	return out, nil
}

func (s *minioStore) Remove(ctx context.Context, bucket string, keys []string) error {
	ch := make(chan minio.ObjectInfo)
	go func() {
		defer close(ch)
		for _, k := range keys {
			select {
			case ch <- minio.ObjectInfo{Key: k}:
			case <-ctx.Done():
				return
			}
		}
	}()
	var failed int
	var first error
	for e := range s.c.RemoveObjects(ctx, bucket, ch, minio.RemoveObjectsOptions{}) {
		failed++
		if first == nil {
			first = e.Err
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d removals failed, first: %w", failed, len(keys), first)
	}
	return ctx.Err()
}
