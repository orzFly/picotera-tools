package picotera

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/klauspost/compress/zstd"
)

// LogEntry mirrors jsx.LogEntry — duplicated here so artifacts doesn't depend
// on jsx. The gateway converts between the two with a one-line copy loop.
type LogEntry struct {
	Level   string    `json:"level"`
	Message string    `json:"message"`
	Ts      time.Time `json:"ts"`
}

type Payload struct {
	Method       string              `json:"method,omitempty"`
	URL          string              `json:"url,omitempty"`
	StatusCode   int                 `json:"statusCode,omitempty"`
	Headers      http.Header         `json:"headers"`
	Body         string              `json:"body"`
	BodyEncoding string              `json:"bodyEncoding"`
	Aggregated   *AggregatedResponse `json:"aggregated,omitempty"`
	Logs         []LogEntry          `json:"logs,omitempty"`
	Timings      []float64           `json:"timings,omitempty"`
}

type AggregatedResponse struct {
	Format       string          `json:"format"`
	Body         json.RawMessage `json:"body,omitempty"`
	BodyEncoding string          `json:"bodyEncoding"`
	Error        string          `json:"error,omitempty"`
}

func BuildRequest(method, url string, header http.Header, body []byte) ([]byte, error) {
	p := Payload{
		Method:  method,
		URL:     url,
		Headers: normalizeHeader(header),
	}
	encodeBody(&p, body)
	return marshalAndCompress(&p)
}

func BuildResponse(statusCode int, header http.Header, body []byte, timings []float64) ([]byte, error) {
	return buildResponse(statusCode, header, body, nil, nil, timings)
}

func BuildResponseWithAggregated(statusCode int, header http.Header, body []byte, aggregated *AggregatedResponse, timings []float64) ([]byte, error) {
	return buildResponse(statusCode, header, body, nil, aggregated, timings)
}

func buildResponse(statusCode int, header http.Header, body []byte, logs []LogEntry, aggregated *AggregatedResponse, timings []float64) ([]byte, error) {
	if err := validateAggregated(aggregated); err != nil {
		return nil, err
	}
	p := Payload{
		StatusCode: statusCode,
		Headers:    normalizeHeader(header),
		Logs:       logs,
		Aggregated: aggregated,
		Timings:    timings,
	}
	encodeBody(&p, body)
	return marshalAndCompress(&p)
}

// BuildResponseWithLogs is BuildResponse plus a logs array — used for meta
// response artifacts so JSX console output is visible in the dashboard.
func BuildResponseWithLogs(statusCode int, header http.Header, body []byte, logs []LogEntry, timings []float64) ([]byte, error) {
	return buildResponse(statusCode, header, body, logs, nil, timings)
}

func BuildResponseWithLogsAndAggregated(statusCode int, header http.Header, body []byte, logs []LogEntry, aggregated *AggregatedResponse, timings []float64) ([]byte, error) {
	return buildResponse(statusCode, header, body, logs, aggregated, timings)
}

func normalizeHeader(h http.Header) http.Header {
	if h == nil {
		return http.Header{}
	}
	return h
}

func encodeBody(p *Payload, body []byte) {
	if utf8.Valid(body) {
		p.Body = string(body)
		p.BodyEncoding = "utf8"
		return
	}
	p.Body = base64.StdEncoding.EncodeToString(body)
	p.BodyEncoding = "base64"
}

func validateAggregated(aggregated *AggregatedResponse) error {
	if aggregated == nil || len(aggregated.Body) == 0 {
		return nil
	}
	if !json.Valid(aggregated.Body) {
		return fmt.Errorf("invalid aggregated body json")
	}
	return nil
}

func marshalAndCompress(p *Payload) ([]byte, error) {
	raw, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	var buf bytes.Buffer
	w, err := zstd.NewWriter(&buf)
	if err != nil {
		return nil, fmt.Errorf("zstd writer: %w", err)
	}
	if _, err := w.Write(raw); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("zstd write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("zstd close: %w", err)
	}
	return buf.Bytes(), nil
}
