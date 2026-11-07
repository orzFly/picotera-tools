package axonimport

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/orzfly/picotera-tools/internal/picotera"
)

// PicoTera's request.type and request.finish_reason values
// (pkg/db/request_constants.go).
const (
	typeMeta     = 0
	typeUpstream = 1

	finishInternalError = 1
	finishCancelled     = 2
	finishEOF           = 3
)

// ImportedHeader marks every synthesized artifact so the raw view says where
// the record came from and why the stream body is missing.
const ImportedHeader = "X-Picotera-Imported-From"

type Row struct {
	ID                 string
	CreatedAt          time.Time
	SpanID             string
	ParentSpanID       *string
	Type               int
	ProviderID         *int64
	EndpointPath       *string
	UserID             int64
	APIKeyID           *int32
	ProjectID          *int32
	ProjectName        string `json:",omitempty"`
	Model              *string
	UpstreamModel      *string
	InputTokens        *int64
	CacheReadTokens    *int64
	OutputTokens       *int64
	CacheWriteTokens   *int64
	CacheWrite1hTokens *int64
	StatusCode         *int64
	ErrorMessage       *string
	TTFTMs             *int64
	TimeSpentMs        *int64
	FinishReason       int
	UserMessagePreview *string
	Annotations        map[string]string
	UsageRaw           json.RawMessage
}

type Artifact struct {
	Key      string
	Data     []byte
	Response bool
}

type Record struct {
	Rows      []Row
	Artifacts []Artifact
	Project   *Resolution
}

type Mapper struct {
	UserID    int64
	Channels  map[int64]string
	Providers map[string]int64
	Projects  *Projects
	Keys      *Keys
	Pico      *pgxpool.Pool // for key lookups
}

func wireFormat(axonFormat string) picotera.Format {
	switch axonFormat {
	case "anthropic/messages":
		return picotera.FormatAnthropicMessages
	case "openai/chat_completions":
		return picotera.FormatOpenAIChatCompletions
	case "openai/responses":
		return picotera.FormatOpenAIResponses
	case "gemini/contents":
		return picotera.FormatGeminiStreamGenerateContent
	}
	return picotera.FormatUnknown
}

func clientPath(r *Request) string {
	return formatPath(r.Format, r.ModelID, r.Stream)
}

// formatPath is the conventional path for an AxonHub format. Upstream rows
// fall back to it because early executions did not record request_url.
func formatPath(format, model string, stream bool) string {
	switch format {
	case "anthropic/messages":
		return "/v1/messages"
	case "openai/chat_completions":
		return "/v1/chat/completions"
	case "openai/responses":
		return "/v1/responses"
	case "gemini/contents":
		if stream {
			return "/v1beta/models/" + model + ":streamGenerateContent"
		}
		return "/v1beta/models/" + model + ":generateContent"
	}
	return "/axonhub/" + format
}

func finishReason(status string) int {
	switch status {
	case "completed":
		return finishEOF
	case "canceled":
		return finishCancelled
	}
	// failed, plus pending/processing rows AxonHub never closed.
	return finishInternalError
}

func (m *Mapper) Map(ctx context.Context, r *Request) (*Record, error) {
	metaID, err := deterministicID(r.CreatedAt, kindMeta, r.ID)
	if err != nil {
		return nil, err
	}
	var apiKey *int32
	if m.Keys != nil {
		if apiKey, err = m.Keys.Resolve(ctx, m.Pico, r.APIKeyID); err != nil {
			return nil, err
		}
	}
	meta := Row{
		ID:           metaID.String(),
		CreatedAt:    metaID.Time().UTC(),
		SpanID:       metaID.String(),
		ParentSpanID: r.TraceID,
		Type:         typeMeta,
		ProviderID:   m.provider(r.ChannelID),
		EndpointPath: ptr(clientPath(r)),
		UserID:       m.UserID,
		APIKeyID:     apiKey,
		Model:        nonEmpty(r.ModelID),
		TTFTMs:       r.TTFTMs,
		TimeSpentMs:  r.LatencyMs,
		FinishReason: finishReason(r.Status),
		Annotations:  m.annotations("axonhub_request_id", r.ID, r.ChannelID),
	}
	project, err := m.Projects.Resolve(ctx, r.RequestBody)
	if err != nil {
		return nil, err
	}
	if project != nil && (project.Matched || project.Created) {
		meta.ProjectID = &project.ID
		meta.ProjectName = project.Name
	}
	format := wireFormat(r.Format)
	if p, ok := picotera.UserMessagePreview(r.RequestBody, format); ok {
		meta.UserMessagePreview = &p
	}

	var last *Execution
	if n := len(r.Executions); n > 0 {
		last = &r.Executions[n-1]
		meta.UpstreamModel = nonEmpty(last.ModelID)
	}
	switch {
	case r.Status == "completed":
		meta.StatusCode = ptr(int64(http.StatusOK))
	case last != nil && last.StatusCode != nil:
		meta.StatusCode = last.StatusCode
	case r.Status == "failed":
		meta.StatusCode = ptr(int64(http.StatusBadGateway))
	}
	if r.Status != "completed" && last != nil && last.ErrorMessage != nil && *last.ErrorMessage != "" {
		meta.ErrorMessage = last.ErrorMessage
	}

	// Tokens belong to the attempt that produced the answer. PicoTera sums
	// type=1 rows for stats, and copies the winner's tokens onto the meta row.
	winner := -1
	for i := len(r.Executions) - 1; i >= 0; i-- {
		if r.Executions[i].Status == "completed" {
			winner = i
			break
		}
	}
	if r.Usage != nil {
		applyUsage(&meta, r.Usage)
		meta.UsageRaw = usageRaw(r.ResponseBody)
	}

	rec := &Record{Project: project}
	rec.Rows = append(rec.Rows, meta)
	reqArt, err := picotera.BuildRequest(http.MethodPost, clientPath(r), requestHeaders(r.RequestHeaders), r.RequestBody)
	if err != nil {
		return nil, err
	}
	respArt, err := buildResponse(meta.StatusCode, r.Stream, format, r.ResponseBody, meta.ErrorMessage)
	if err != nil {
		return nil, err
	}
	rec.Artifacts = append(rec.Artifacts,
		Artifact{Key: picotera.RequestKey(meta.ID, meta.CreatedAt), Data: reqArt},
		Artifact{Key: picotera.ResponseKey(meta.ID, meta.CreatedAt), Data: respArt, Response: true},
	)

	for i := range r.Executions {
		e := &r.Executions[i]
		upID, err := deterministicID(e.CreatedAt, kindUpstream, e.ID)
		if err != nil {
			return nil, err
		}
		up := Row{
			ID:            upID.String(),
			CreatedAt:     upID.Time().UTC(),
			SpanID:        meta.ID,
			ParentSpanID:  r.TraceID,
			Type:          typeUpstream,
			ProviderID:    m.provider(e.ChannelID),
			EndpointPath:  urlPath(e.RequestURL, formatPath(e.Format, e.ModelID, e.Stream)),
			UserID:        m.UserID,
			APIKeyID:      apiKey,
			ProjectID:     meta.ProjectID,
			ProjectName:   meta.ProjectName,
			Model:         meta.Model,
			UpstreamModel: nonEmpty(e.ModelID),
			TTFTMs:        e.TTFTMs,
			TimeSpentMs:   e.LatencyMs,
			FinishReason:  finishReason(e.Status),
			Annotations:   m.annotations("axonhub_execution_id", e.ID, e.ChannelID),
		}
		switch {
		case e.StatusCode != nil:
			up.StatusCode = e.StatusCode
		case e.Status == "completed":
			up.StatusCode = ptr(int64(http.StatusOK))
		}
		if e.ErrorMessage != nil && *e.ErrorMessage != "" {
			up.ErrorMessage = e.ErrorMessage
		}
		if i == winner && r.Usage != nil {
			applyUsage(&up, r.Usage)
			up.UsageRaw = usageRaw(e.ResponseBody)
		}
		rec.Rows = append(rec.Rows, up)

		reqURL := ""
		if e.RequestURL != nil {
			reqURL = *e.RequestURL
		}
		reqArt, err := picotera.BuildRequest(http.MethodPost, reqURL, requestHeaders(e.RequestHeaders), e.RequestBody)
		if err != nil {
			return nil, err
		}
		respArt, err := buildResponse(up.StatusCode, e.Stream, wireFormat(e.Format), e.ResponseBody, up.ErrorMessage)
		if err != nil {
			return nil, err
		}
		rec.Artifacts = append(rec.Artifacts,
			Artifact{Key: picotera.RequestKey(up.ID, up.CreatedAt), Data: reqArt},
			Artifact{Key: picotera.ResponseKey(up.ID, up.CreatedAt), Data: respArt, Response: true},
		)
	}
	return rec, nil
}

func (m *Mapper) provider(channelID *int64) *int64 {
	if channelID == nil {
		return nil
	}
	if id, ok := m.Providers[m.Channels[*channelID]]; ok {
		return &id
	}
	return nil
}

func (m *Mapper) annotations(idKey string, id int64, channelID *int64) map[string]string {
	a := map[string]string{
		"source": "axonhub",
		idKey:    strconv.FormatInt(id, 10),
	}
	if channelID != nil {
		if name, ok := m.Channels[*channelID]; ok {
			a["axonhub_channel"] = name
		}
	}
	return a
}

// AxonHub's prompt_tokens includes cache reads and writes; PicoTera's
// input_tokens excludes both (response_extractor.go).
func applyUsage(row *Row, u *Usage) {
	input := u.PromptTokens - u.CachedTokens - u.WriteCached
	if input < 0 {
		input = 0
	}
	row.InputTokens = ptr(input)
	row.OutputTokens = ptr(u.CompletionTokens)
	row.CacheReadTokens = ptr(u.CachedTokens)
	if u.WriteCached5m+u.WriteCached1h > 0 {
		row.CacheWriteTokens = ptr(u.WriteCached5m)
		row.CacheWrite1hTokens = ptr(u.WriteCached1h)
	} else {
		row.CacheWriteTokens = ptr(u.WriteCached)
	}
}

// usageRaw keeps the provider's own usage object; PicoTera drops anything
// that is not a JSON object.
func usageRaw(body []byte) json.RawMessage {
	var root map[string]json.RawMessage
	if json.Unmarshal(body, &root) != nil {
		return nil
	}
	for _, k := range []string{"usage", "usageMetadata"} {
		if v, ok := root[k]; ok && len(v) > 0 && v[0] == '{' {
			return v
		}
	}
	return nil
}

var secretHeaders = map[string]bool{
	"Authorization":       true,
	"Proxy-Authorization": true,
	"X-Api-Key":           true,
	"X-Goog-Api-Key":      true,
	"Api-Key":             true,
	"Cookie":              true,
}

// AxonHub already masks these, but rows written before it learned a header
// name would carry the real value into a bucket with presigned URLs.
func requestHeaders(raw []byte) http.Header {
	h := http.Header{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &h)
	}
	for k, vs := range h {
		if secretHeaders[http.CanonicalHeaderKey(k)] {
			for i, v := range vs {
				if strings.Trim(v, "*") != "" {
					vs[i] = "******"
				}
			}
		}
	}
	// The conversation tab skips request artifacts without a JSON content type.
	if h.Get("Content-Type") == "" {
		h.Set("Content-Type", "application/json")
	}
	h.Set(ImportedHeader, "axonhub")
	return h
}

func buildResponse(status *int64, stream bool, format picotera.Format, body []byte, errMsg *string) ([]byte, error) {
	code := 0
	if status != nil {
		code = int(*status)
	}
	h := http.Header{}
	h.Set(ImportedHeader, "axonhub")
	hasBody := len(body) > 0 && string(body) != "{}" && string(body) != "null"

	if !hasBody {
		h.Set("Content-Type", "text/plain; charset=utf-8")
		msg := ""
		if errMsg != nil {
			msg = *errMsg
		}
		return picotera.BuildResponse(code, h, []byte(msg), nil)
	}
	if !stream {
		h.Set("Content-Type", "application/json")
		return picotera.BuildResponse(code, h, body, nil)
	}
	// AxonHub kept only the response it assembled from the stream, never the
	// SSE events. It goes into the body as JSON as well as into aggregated:
	// the dashboard opens responses on the JSON view and, for an SSE content
	// type, falls back to Raw, which would show an empty body.
	h.Set("Content-Type", "application/json")
	h.Set(ImportedHeader, "axonhub; streamed, events were not recorded, body is the assembled response")
	if format == picotera.FormatUnknown {
		return picotera.BuildResponse(code, h, body, nil)
	}
	return picotera.BuildResponseWithAggregated(code, h, body, &picotera.AggregatedResponse{
		Format:       string(format),
		Body:         json.RawMessage(body),
		BodyEncoding: "json",
	}, nil)
}

func urlPath(raw *string, fallback string) *string {
	if raw == nil || *raw == "" {
		return &fallback
	}
	u, err := url.Parse(*raw)
	if err != nil || u.Path == "" {
		return &fallback
	}
	return &u.Path
}

func ptr[T any](v T) *T { return &v }

func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
