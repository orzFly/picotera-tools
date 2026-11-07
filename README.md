# picotera-tools

Tools around [PicoTera](https://git.atto.town/oott123/picotera), an LLM gateway. PicoTera's Go module path is the bare `picotera`, so the formats it writes are mirrored in `internal/picotera` (see `doc.go` there for the source commit and license).

## axonhub-import

Moves request history from [AxonHub](https://github.com/looplj/axonhub) (Postgres) into PicoTera's `request` table and artifact bucket, so old requests can be browsed, filtered and costed next to new ones.

| Command | Writes | What |
|---|---|---|
| `projects` | nothing | Which project each request would land in; which projects auto-creation would add |
| `import` | nothing, or `--dump DIR` locally | Maps requests and counts rows and artifact bytes; `--dump` writes decompressed artifacts and `rows.jsonl`; `--report FILE` writes the project report |
| `import --write` | PicoTera DB and bucket | The import. Re-runnable; `--incremental` continues after the last imported request; ends with `finalize` |
| `providers` | with `--write`: providers, `provider_id` | Creates PicoTera providers for AxonHub channels per a channel map and labels imported rows |
| `costs` | with `--write`: `model_cost` | Copies AxonHub's recorded costs onto imported rows |
| `finalize --write` | PicoTera DB | Upserts `traces`, project first/last seen, refreshes continuous aggregates |
| `reproject` | with `--write`: projects, `project_id` | Re-resolves the project of every row in one project from its request artifacts |
| `merge-projects` | with `--write`: projects | Folds worktree and other-machine fragments into the repository's project |

Without `--write` the PicoTera connection runs with `default_transaction_read_only=on` and no S3 client exists. The AxonHub connection is always read-only.

### Environment

`AXONHUB_DB_DSN`, `PICOTERA_DATABASE_URL`, and for artifacts `PICOTERA_S3_ENDPOINT`, `PICOTERA_S3_ACCESS_KEY`, `PICOTERA_S3_SECRET_KEY`, `PICOTERA_S3_BUCKET`, `PICOTERA_S3_REGION`, `PICOTERA_S3_USE_SSL`, `PICOTERA_S3_PATH_STYLE`: the same names PicoTera itself reads.

### Mapping

| PicoTera | AxonHub |
|---|---|
| meta row (`type=0`) | `requests` |
| upstream row (`type=1`) | `request_executions`, one per attempt |
| `id` | xid built from `created_at` and the AxonHub id, so re-runs hit the same key |
| `parent_span_id` | `traces.trace_id` (the client's session id) |
| `provider_id` | `--channel-map`; without one, a channel whose name equals a provider's |
| `api_key_id` | `--key-map`; other keys get a disabled `axonhub/<name>` key on first use |
| `project_id` | workspace paths in the client body, resolved under the path rules below |
| tokens, `model_cost` | `usage_logs`, on the meta row and the winning attempt; currency from AxonHub's general settings |
| `annotations` | `{"source":"axonhub", "axonhub_request_id" / "axonhub_execution_id", "axonhub_channel"}` |
| response artifact | AxonHub keeps only the response it assembled from a stream: it goes into the body as JSON and into `aggregated` |

### Path rules

Projects are resolved the way `docs/picotera-project-matching.md` proposes for PicoTera: the client's current directory first, worktrees folded to the repository root, aliases and a blacklist from `--rules FILE`:

```json
{
  "mainRoot": "/home/alice/src",
  "aliases": [
    {"from": "/Users/alice/src", "to": "/home/alice/src"},
    {"from": "C:\\Users\\alice\\src", "to": "/home/alice/src"}
  ],
  "blacklist": ["/home/alice", "/Users/alice"],
  "roots": ["/home/alice/src/monorepo"]
}
```

| Key | Meaning |
|---|---|
| `mainRoot` | Where repositories canonically live; merges name projects after the repository found there |
| `aliases` | Other machines' directories for the same tree; a backslash `from` matches case-insensitively |
| `blacklist` | Paths that never seed a project and never match as an ancestor, e.g. home directories |
| `roots` | Directories whose subdirectories belong to them, for repositories first seen from a subdirectory |

One-off routing goes on the command line: `--assign PATH=PROJECT_ID` (repeatable) and `--root PATH`.

### Channel map

`providers` and `import --channel-map` read one line per AxonHub channel:

```
# <channel id> id <existing provider id>
12 id 4
# <channel id> new <name of a provider to create>
31 new legacy/example
```

New providers are created disabled, at priority -100, with placeholder credentials and endpoints guessed from the channel type and base URL.

### Running

AxonHub and PicoTera usually sit on a container network with their databases. Build a static binary and run it in a throwaway container on that network, with the services' own env files:

```sh
CGO_ENABLED=0 go build -trimpath -o bin/ ./cmd/...
podman run --rm --network <network> \
  --env-file <axonhub env file> --env-file <picotera env file> \
  -v "$PWD/bin:/w" --entrypoint /w/axonhub-import <any image> projects
```

Keep rules files, channel maps and `--assign` / `--key-map` choices next to the binary on the host: they describe a deployment, not this tool.
