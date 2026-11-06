# picotera-tools

Tools around [PicoTera](https://git.atto.town/oott123/picotera). PicoTera's Go module path is the bare `picotera`, so formats it writes are mirrored in `internal/picotera` (see `doc.go` there for the source commit and license).

## axonhub-import

Copies AxonHub's request history (Postgres) into PicoTera's `request` table and artifact bucket.

| Command | Writes | What |
|---|---|---|
| `projects` | nothing | Which project each request lands in; which projects auto-creation would add |
| `import` | nothing, or `--dump DIR` locally | Maps requests and counts rows/artifact bytes; `--dump` writes decompressed artifacts and `rows.jsonl` |
| `import --write` | PicoTera DB + bucket | The real import; re-runnable, already imported requests are skipped |
| `finalize --write` | PicoTera DB | Upserts `traces`, project first/last seen, refreshes continuous aggregates |

Without `--write` the PicoTera connection runs with `default_transaction_read_only=on` and no S3 client exists. The AxonHub connection is always read-only.

### Mapping

| PicoTera | AxonHub |
|---|---|
| meta row (`type=0`) | `requests` |
| upstream row (`type=1`) | `request_executions`, one per attempt |
| `id` | xid from `created_at` + AxonHub id (deterministic, so re-runs hit the same key) |
| `parent_span_id` | `traces.trace_id` (Claude Code session id) |
| `provider_id` | channel name equal to a provider name; otherwise NULL |
| `project_id` | PicoTera's workspace-path regexes over the client body |
| tokens | `usage_logs`, on the meta row and the winning attempt |
| `annotations` | `{"source":"axonhub", "axonhub_request_id"/"axonhub_execution_id", "axonhub_channel"}` |
| response artifact | AxonHub never stored stream events: streamed responses carry an empty body and the assembled response in `aggregated` |

### Running

Both databases sit on the `internal` podman network, so run the static binary in a throwaway container with the services' own env files:

```sh
CGO_ENABLED=0 go build -trimpath -o bin/ ./cmd/...
scp bin/axonhub-import host:/srv/picotera-tools/
ssh host /srv/picotera-tools/run.sh projects
```

`run.sh` is `podman run --network internal --env-file …/axonhub-environment --env-file …/picotera-environment`. `--write` also needs the `PICOTERA_S3_*` variables that live inline in the picotera unit, not in its env file.
