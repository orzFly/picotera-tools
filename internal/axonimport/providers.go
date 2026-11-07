package axonimport

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ChannelTarget is where an AxonHub channel's history goes: an existing
// PicoTera provider, or a provider to create under NewName.
type ChannelTarget struct {
	ProviderID int32
	NewName    string
}

// ReadChannelMap parses lines of "<channel id> id <provider id>" or
// "<channel id> new <provider name>"; blank lines and '#' comments are
// skipped. The mapping is deployment data and stays out of this repository.
func ReadChannelMap(path string) (map[int64]ChannelTarget, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := map[int64]ChannelTarget{}
	sc := bufio.NewScanner(f)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.SplitN(line, " ", 3)
		if len(fields) != 3 {
			return nil, fmt.Errorf("%s:%d: want '<channel id> id|new <target>'", path, n)
		}
		ch, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, n, err)
		}
		switch fields[1] {
		case "id":
			id, err := strconv.ParseInt(fields[2], 10, 32)
			if err != nil {
				return nil, fmt.Errorf("%s:%d: %w", path, n, err)
			}
			out[ch] = ChannelTarget{ProviderID: int32(id)}
		case "new":
			out[ch] = ChannelTarget{NewName: strings.TrimSpace(fields[2])}
		default:
			return nil, fmt.Errorf("%s:%d: unknown kind %q", path, n, fields[1])
		}
	}
	return out, sc.Err()
}

type AxonChannel struct {
	ID      int64
	Name    string
	Type    string
	BaseURL string
	Models  []string
	Uses    int64
}

func LoadAxonChannels(ctx context.Context, axon *pgxpool.Pool) (map[int64]*AxonChannel, error) {
	rows, err := axon.Query(ctx, `
		SELECT c.id, c.name, c.type, COALESCE(c.base_url, ''),
		       COALESCE((SELECT array_agg(m) FROM jsonb_array_elements_text(c.supported_models) m), '{}'),
		       (SELECT count(*) FROM request_executions e WHERE e.channel_id = c.id)
		FROM channels c`)
	if err != nil {
		return nil, fmt.Errorf("list channels: %w", err)
	}
	defer rows.Close()
	out := map[int64]*AxonChannel{}
	for rows.Next() {
		c := &AxonChannel{}
		if err := rows.Scan(&c.ID, &c.Name, &c.Type, &c.BaseURL, &c.Models, &c.Uses); err != nil {
			return nil, err
		}
		out[c.ID] = c
	}
	return out, rows.Err()
}

var versionSuffix = regexp.MustCompile(`/v\d+(beta)?$`)

// endpointsFor guesses PicoTera upstream URLs from an AxonHub channel type and
// base URL. AxonHub base URLs sometimes end in the API version and sometimes
// not; the providers are created disabled, so a wrong guess routes nothing.
func endpointsFor(c *AxonChannel) map[string]string {
	base := strings.TrimRight(c.BaseURL, "/")
	if base == "" {
		return nil
	}
	versioned := func(def, rest string) string {
		if versionSuffix.MatchString(base) {
			return base + rest
		}
		return base + "/" + def + rest
	}
	switch c.Type {
	case "anthropic", "claudecode":
		return map[string]string{
			"/v1/messages":              versioned("v1", "/messages"),
			"/v1/messages/count_tokens": versioned("v1", "/messages/count_tokens"),
		}
	case "openai", "deepseek", "volcengine", "github_copilot":
		return map[string]string{"/v1/chat/completions": versioned("v1", "/chat/completions")}
	case "openai_responses", "codex":
		return map[string]string{"/v1/responses": versioned("v1", "/responses")}
	case "gemini", "gemini_vertex":
		return map[string]string{
			"/v1beta/models/{model}:streamGenerateContent": versioned("v1beta", "/models/{model}:streamGenerateContent"),
			"/v1beta/models/{model}:generateContent":       versioned("v1beta", "/models/{model}:generateContent"),
		}
	}
	return nil
}

// LegacyProvider is a provider to create for one or more AxonHub channels.
type LegacyProvider struct {
	Name      string
	Channels  []*AxonChannel
	Models    []string
	Endpoints map[string]string
}

const (
	LegacyPriority    = -100
	legacyCredentials = "legacy-axonhub-no-credentials"
)

// PlanLegacyProviders groups channels by new provider name. Where merged
// channels disagree on an endpoint, the most used channel wins.
func PlanLegacyProviders(chmap map[int64]ChannelTarget, channels map[int64]*AxonChannel) []*LegacyProvider {
	byName := map[string]*LegacyProvider{}
	for id, t := range chmap {
		c := channels[id]
		if t.NewName == "" || c == nil {
			continue
		}
		lp := byName[t.NewName]
		if lp == nil {
			lp = &LegacyProvider{Name: t.NewName, Endpoints: map[string]string{}}
			byName[t.NewName] = lp
		}
		lp.Channels = append(lp.Channels, c)
	}
	var out []*LegacyProvider
	for _, lp := range byName {
		sort.Slice(lp.Channels, func(i, j int) bool { return lp.Channels[i].Uses > lp.Channels[j].Uses })
		seen := map[string]bool{}
		for _, c := range lp.Channels {
			for _, m := range c.Models {
				if !seen[m] {
					seen[m] = true
					lp.Models = append(lp.Models, m)
				}
			}
			for path, url := range endpointsFor(c) {
				if _, ok := lp.Endpoints[path]; !ok {
					lp.Endpoints[path] = url
				}
			}
		}
		sort.Strings(lp.Models)
		out = append(out, lp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func ShowLegacyProviders(w io.Writer, plans []*LegacyProvider, existing map[string]int32) {
	for _, lp := range plans {
		state := "create"
		if id, ok := existing[lp.Name]; ok {
			state = fmt.Sprintf("exists #%d", id)
		}
		fmt.Fprintf(w, "%s  [%s]  %d models\n", lp.Name, state, len(lp.Models))
		for _, c := range lp.Channels {
			fmt.Fprintf(w, "  <- #%d %s (%s, %d attempts)\n", c.ID, c.Name, c.Type, c.Uses)
		}
		paths := make([]string, 0, len(lp.Endpoints))
		for p := range lp.Endpoints {
			paths = append(paths, p)
		}
		sort.Strings(paths)
		for _, p := range paths {
			fmt.Fprintf(w, "     %s -> %s\n", p, lp.Endpoints[p])
		}
	}
}

// EnsureLegacyProviders creates the planned providers that do not exist yet:
// disabled, priority -100, placeholder credentials. Re-runs find them by name.
func EnsureLegacyProviders(ctx context.Context, db *pgxpool.Pool, plans []*LegacyProvider, existing map[string]int32) error {
	for _, lp := range plans {
		if _, ok := existing[lp.Name]; ok {
			continue
		}
		models := make([]map[string]string, len(lp.Models))
		for i, m := range lp.Models {
			models[i] = map[string]string{"model": m}
		}
		raw, _ := json.Marshal(models)
		err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
			var id int32
			if err := tx.QueryRow(ctx, `
				INSERT INTO provider (name, credentials, priority, provider_models, annotations, disabled)
				VALUES ($1, $2, $3, $4, '{}', true) RETURNING id`,
				lp.Name, legacyCredentials, LegacyPriority, raw).Scan(&id); err != nil {
				return fmt.Errorf("create provider %q: %w", lp.Name, err)
			}
			for path, url := range lp.Endpoints {
				if _, err := tx.Exec(ctx, `
					INSERT INTO provider_endpoint (provider_id, endpoint_path, upstream_url)
					VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, id, path, url); err != nil {
					return fmt.Errorf("endpoint %s of %q: %w", path, lp.Name, err)
				}
			}
			existing[lp.Name] = id
			log.Printf("created provider #%d %s", id, lp.Name)
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// ResolveChannels turns the map into channel id -> provider id, using
// existing (name -> id) for new names; channels whose provider does not exist
// yet are left out.
func ResolveChannels(chmap map[int64]ChannelTarget, existing map[string]int32) map[int64]int32 {
	out := map[int64]int32{}
	for ch, t := range chmap {
		if t.ProviderID > 0 {
			out[ch] = t.ProviderID
		} else if id, ok := existing[t.NewName]; ok {
			out[ch] = id
		}
	}
	return out
}

// BackfillProviders sets provider_id on imported rows from the channel of
// their AxonHub request (meta rows) or execution (upstream rows).
type BackfillProviders struct {
	Axon     *pgxpool.Pool
	Pico     *pgxpool.Pool
	Channels map[int64]int32
	Batch    int
	UntilID  int64
	Write    bool

	Rows int64
}

func (b *BackfillProviders) Run(ctx context.Context) error {
	for _, src := range []struct {
		table string
		kind  uint16
	}{{"requests", kindMeta}, {"request_executions", kindUpstream}} {
		limit := b.UntilID
		if src.table == "request_executions" {
			// Execution ids run ahead of request ids; rows not imported yet
			// simply match nothing.
			limit = 1<<62 - 1
		}
		var after int64
		for {
			rows, err := b.Axon.Query(ctx, `SELECT id, created_at, channel_id FROM `+src.table+`
				WHERE id > $1 AND id <= $2 AND channel_id IS NOT NULL ORDER BY id LIMIT $3`, after, limit, b.Batch)
			if err != nil {
				return err
			}
			batch := &pgx.Batch{}
			var last int64
			for rows.Next() {
				var id, ch int64
				var at time.Time
				if err := rows.Scan(&id, &at, &ch); err != nil {
					rows.Close()
					return err
				}
				last = id
				pid, ok := b.Channels[ch]
				if !ok {
					continue
				}
				x, err := deterministicID(at, src.kind, id)
				if err != nil {
					rows.Close()
					return err
				}
				if b.Write {
					batch.Queue(`UPDATE request SET provider_id = $1 WHERE id = $2 AND created_at = $3 AND provider_id IS DISTINCT FROM $1`,
						pid, x.String(), x.Time().UTC())
				} else {
					batch.Queue(`SELECT count(*) FROM request WHERE id = $2 AND created_at = $3 AND provider_id IS DISTINCT FROM $1`,
						pid, x.String(), x.Time().UTC())
				}
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return err
			}
			if last == 0 {
				break
			}
			send := func(q interface {
				SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
			}) error {
				br := q.SendBatch(ctx, batch)
				defer br.Close()
				for i := 0; i < batch.Len(); i++ {
					if b.Write {
						tag, err := br.Exec()
						if err != nil {
							return err
						}
						b.Rows += tag.RowsAffected()
					} else {
						var n int64
						if err := br.QueryRow().Scan(&n); err != nil {
							return err
						}
						b.Rows += n
					}
				}
				return nil
			}
			if b.Write {
				err = pgx.BeginFunc(ctx, b.Pico, func(tx pgx.Tx) error { return send(tx) })
			} else {
				err = send(b.Pico)
			}
			if err != nil {
				return fmt.Errorf("%s after %d: %w", src.table, after, err)
			}
			after = last
			log.Printf("providers: %s through %d, %d rows", src.table, after, b.Rows)
		}
	}
	return nil
}
