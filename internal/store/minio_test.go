package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

// fakeS3 serves one object path-style and honours Range, the way S3 and MinIO do. It exists to
// drive the real minio-go client: an older minio-go ignored the range set on GetObject and
// handed back the whole object, which no in-memory store could have shown.
func fakeS3(t *testing.T, path string, data []byte) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != path {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `<Error><Code>NoSuchKey</Code></Error>`)
			return
		}
		body, status := data, http.StatusOK
		if rg := r.Header.Get("Range"); rg != "" {
			var from, to int
			if _, err := fmt.Sscanf(strings.TrimPrefix(rg, "bytes="), "%d-%d", &from, &to); err != nil || to >= len(data) || from > to {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			body, status = data[from:to+1], http.StatusPartialContent
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", from, to, len(data)))
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("ETag", `"0123456789abcdef0123456789abcdef"`)
		w.Header().Set("Last-Modified", "Mon, 05 Jan 2026 00:00:00 GMT")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(status)
		if r.Method != http.MethodHead {
			w.Write(body)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestMinioStoreGetRangeReturnsTheRange(t *testing.T) {
	data := bytes.Repeat([]byte("abcdefghij"), 10000)
	srv := fakeS3(t, "/arch/day/gen.data", data)
	u, _ := url.Parse(srv.URL)
	st, err := newMinioStore(s3Config{endpoint: u.Host, accessKey: "k", secretKey: "s", region: "us-east-1", pathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []struct{ off, n int64 }{{0, 10}, {12345, 6789}, {int64(len(data)) - 7, 7}} {
		rc, err := st.GetRange(context.Background(), "arch", "day/gen.data", r.off, r.n)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if !bytes.Equal(got, data[r.off:r.off+r.n]) {
			t.Fatalf("range %d+%d: got %d bytes", r.off, r.n, len(got))
		}
	}
	rc, info, err := st.Get(context.Background(), "arch", "day/gen.data")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, data) || info.ETag != "0123456789abcdef0123456789abcdef" {
		t.Fatalf("whole object: %d bytes, etag %q", len(got), info.ETag)
	}
	if _, _, err := st.Get(context.Background(), "arch", "day/missing"); err != errNotFound {
		t.Fatalf("missing key: %v", err)
	}
}
