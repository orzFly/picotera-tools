package store

import (
	"bytes"
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// Only what picotera's dashboard asks for. Presigned reads carry their own credentials, so the
// route to this listener usually skips the dashboard's login; anything wider would expose the
// whole S3 API under the dashboard's name.
var allowedPath = regexp.MustCompile(`^/([a-z0-9-]+)/(artifacts/\d{4}-\d{2}-\d{2}/[A-Za-z0-9_-][A-Za-z0-9._-]*)$`)

type server struct {
	bucket string
	proxy  *httputil.ReverseProxy
	fb     *fallback
	log    *slog.Logger
}

func newServer(upstream *url.URL, bucket string, fb *fallback, log *slog.Logger) *server {
	s := &server{bucket: bucket, fb: fb, log: log}
	s.proxy = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(upstream)
			// The presigned signature covers Host, and S3 checks it against the Host it
			// receives, so the upstream has to accept the dashboard's own Host for the bucket.
			pr.Out.Host = pr.In.Host
			for h := range pr.Out.Header {
				if strings.HasPrefix(h, "X-Forwarded-") || h == "X-Real-Ip" {
					pr.Out.Header.Del(h)
				}
			}
		},
		ModifyResponse: s.modifyResponse,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			log.Error("upstream failed", "path", r.URL.Path, "err", err)
			writeS3Error(w, r, http.StatusBadGateway, "InternalError", "upstream unreachable")
		},
	}
	return s
}

func (s *server) key(r *http.Request) (string, bool) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return "", false
	}
	if r.URL.RawPath != "" && r.URL.RawPath != r.URL.Path {
		return "", false
	}
	m := allowedPath.FindStringSubmatch(r.URL.Path)
	if m == nil || m[1] != s.bucket || strings.Contains(m[2], "..") {
		return "", false
	}
	if _, ok := dayOfKey(m[2]); !ok {
		return "", false
	}
	return m[2], true
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.key(r); !ok {
		s.log.Warn("refused", "method", r.Method, "path", r.URL.Path)
		writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "not served by this proxy")
		return
	}
	s.proxy.ServeHTTP(w, r)
}

func (s *server) modifyResponse(resp *http.Response) error {
	if resp.StatusCode != http.StatusNotFound {
		return nil
	}
	// Only NoSuchKey proves the signature was good: MinIO authorises before it looks the key up.
	if code, err := errorCode(resp); err != nil || code != "NoSuchKey" {
		return err
	}
	r := resp.Request
	key, _ := s.key(r)
	e, err := s.fb.lookup(r.Context(), key)
	if err != nil {
		s.log.Error("fallback failed", "key", key, "err", err)
		replaceWithError(resp, http.StatusServiceUnavailable, "SlowDown", "archive read failed")
		return nil
	}
	if e == nil {
		return nil
	}
	resp.Body.Close()
	resp.StatusCode = http.StatusOK
	resp.Status = "200 OK"
	resp.Header = http.Header{}
	resp.Header.Set("Content-Type", e.contentType)
	resp.Header.Set("Content-Encoding", "zstd")
	resp.Header.Set("Content-Length", strconv.Itoa(len(e.body)))
	resp.ContentLength = int64(len(e.body))
	if r.Method == http.MethodHead {
		resp.Body = http.NoBody
	} else {
		resp.Body = io.NopCloser(bytes.NewReader(e.body))
	}
	return nil
}

// errorCode names a MinIO error response. HEAD carries it only in X-Minio-Error-Code, having no
// body; GET carries it only in the XML body, which is put back for the client when it is read.
func errorCode(resp *http.Response) (string, error) {
	if c := resp.Header.Get("X-Minio-Error-Code"); c != "" {
		return c, nil
	}
	if resp.Request.Method != http.MethodGet {
		return "", nil
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(b))
	if err != nil {
		return "", err
	}
	var e s3Error
	if xml.Unmarshal(b, &e) != nil {
		return "", nil
	}
	return e.Code, nil
}

type s3Error struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func s3ErrorBody(code, msg string) []byte {
	b, _ := xml.Marshal(s3Error{Code: code, Message: msg})
	return append([]byte(xml.Header), b...)
}

func writeS3Error(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	b := s3ErrorBody(code, msg)
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("X-Minio-Error-Code", code)
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		w.Write(b)
	}
}

func replaceWithError(resp *http.Response, status int, code, msg string) {
	b := s3ErrorBody(code, msg)
	resp.Body.Close()
	resp.StatusCode = status
	resp.Status = strconv.Itoa(status) + " " + http.StatusText(status)
	resp.Header = http.Header{}
	resp.Header.Set("Content-Type", "application/xml")
	resp.Header.Set("X-Minio-Error-Code", code)
	resp.Header.Set("Content-Length", strconv.Itoa(len(b)))
	resp.ContentLength = int64(len(b))
	if resp.Request.Method == http.MethodHead {
		resp.Body = http.NoBody
	} else {
		resp.Body = io.NopCloser(bytes.NewReader(b))
	}
}
