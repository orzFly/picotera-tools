# picotera-tools

Tools around [PicoTera](https://git.atto.town/oott123/picotera), an LLM gateway. The command-line tool is `ptt`:

- **Migrate from [AxonHub](https://github.com/looplj/axonhub)**: copy its request history into PicoTera, so old requests can be browsed, filtered and costed next to new ones.
- **Tidy projects**: merge the fragments PicoTera's project auto-creation leaves behind, and re-sort requests a project swallowed by mistake.

A second binary, `pt-store`, keeps the request artifacts small: it packs old days of the artifact bucket into compressed daily archives and serves the dashboard's reads of them, see [Archiving artifacts](#archiving-artifacts).

Nothing is written unless you pass `--write`. Without it, the PicoTera connection runs with `default_transaction_read_only=on` and no S3 client exists; the AxonHub connection is always read-only.

## Setup

`ptt` reads the same environment variables as PicoTera: `PICOTERA_DATABASE_URL`, and for request artifacts `PICOTERA_S3_ENDPOINT`, `PICOTERA_S3_ACCESS_KEY`, `PICOTERA_S3_SECRET_KEY`, `PICOTERA_S3_BUCKET`, `PICOTERA_S3_REGION`, `PICOTERA_S3_USE_SSL`, `PICOTERA_S3_PATH_STYLE`. The AxonHub commands also need `AXONHUB_DB_DSN`.

The databases usually sit on a container network, so build a static binary and run it in a throwaway container on that network with the services' own env files. A small wrapper saves typing:

```sh
CGO_ENABLED=0 go build -trimpath -o bin/ ./cmd/...   # or: nix develop ./.nix, then ptt
mkdir -p ~/ptt && cp bin/ptt ~/ptt/ && cd ~/ptt
cat > run.sh <<'EOF'
#!/bin/sh
cd "$(dirname "$0")"
exec podman run --rm --network <network> \
  --env-file <axonhub env file> --env-file <picotera env file> \
  -v "$PWD:/w" -w /w --entrypoint /w/ptt <any image> "$@"
EOF
chmod +x run.sh
./run.sh            # prints the commands
```

Inside the container this directory is `/w`, so files you keep next to the binary (rules, maps, reports) are `/w/<name>`. Those files describe your deployment; keep them on the host, not in this repository.

## Migrating from AxonHub

The import is a loop of "look, adjust rules, look again" until the dry run says what you want, then a small real run you check in the dashboard, then the full run. Every step before the real run is read-only.

### 1. See where requests would land

PicoTera files each request under a project by the working directory the client (Claude Code, Codex, …) mentions in its prompt. Start with a sample of 1% of the AxonHub requests:

```sh
./run.sh axonhub projects --sample-mod 100
```

The report lists each existing project that would receive requests, each project auto-creation would add (with the path it would be seeded from), and how many requests mention no directory at all. Typical problems at this point:

| You see | Cause | Fix in step 2 |
|---|---|---|
| One project, seeded from a home directory, gets almost everything | ancestor matching: `/home/alice` matches every repository below it | `blacklist` the home directory |
| `foo` and `foo-1a2b3c4d`, one seeded from `/Users/…` or `D:\…` | the same repository on another machine | `aliases` |
| A project per worktree, named after the worktree | `.claude/worktrees/<name>` is handled already; other layouts are not | `aliases`, or `roots` |
| `backend` next to `bigrepo` | the repository was first seen from a subdirectory | `roots` |
| Odd directories (`~/bin`, dotfiles) as projects of their own | the client was started there | `--assign PATH=PROJECT_ID` into an existing project |

### 2. Write a rules file

Rules are a JSON file passed with `--rules`. All keys are optional:

```json
{
  "mainRoot": "/home/alice/src",
  "aliases": [
    {"from": "/Users/alice/src", "to": "/home/alice/src"},
    {"from": "C:\\Users\\alice\\src", "to": "/home/alice/src"}
  ],
  "blacklist": ["/home/alice", "/Users/alice"],
  "roots": ["/home/alice/src/bigrepo"]
}
```

| Key | Meaning |
|---|---|
| `mainRoot` | Where repositories canonically live; merged projects are named after the repository found here |
| `aliases` | Another machine's directory for the same tree. A `from` with backslashes matches case-insensitively, as Windows does |
| `blacklist` | Never seeds a project and never matches as an ancestor; a project whose path is exactly this still matches |
| `roots` | Every subdirectory belongs to this directory's project |

One-off decisions go on the command line. Collect them, with the rules, in an arguments file so every later command uses the same set:

```sh
cat > import.args <<'EOF'
--rules /w/rules.json
--assign /home/alice/bin=28
--root /home/alice/src/bigrepo
EOF
./run.sh axonhub projects $(cat import.args) --sample-mod 100
```

Repeat until the report looks right. `--sample-mod 33` (3%) shows more of the long tail; drop `--sample-mod` for the full list.

### 3. Map API keys, channels and model names

Imported rows carry the PicoTera API key and provider they would have had. Look up ids in both databases (`api_keys` and `channels` in AxonHub, `api_key` and `provider` in PicoTera).

**Keys.** For each AxonHub key that is the same client as a PicoTera key, add `--key-map AXONHUB_ID=PICOTERA_ID` to `import.args`. Any other key gets a disabled PicoTera key named `axonhub/<name>` the first time the import meets it.

**Channels.** Write a channel map, one line per AxonHub channel, mapping it to an existing provider or naming a provider to create:

```
# <channel id> id <existing provider id>
12 id 4
13 id 4
# <channel id> new <name>
31 new legacy/example
```

Then look at, and create, the providers:

```sh
./run.sh axonhub providers --channel-map /w/channels.map          # the plan, with guessed endpoints
./run.sh axonhub providers --channel-map /w/channels.map --write  # create them
```

New providers are disabled, at priority -100, with placeholder credentials and endpoints guessed from the channel type and base URL: they label history and never take traffic. Create them before importing, so the import fills `provider_id` directly.

**Model names.** If AxonHub saw a model under a different name than PicoTera uses, add `--model-rename FROM=TO` to `import.args`. Only the requested model is renamed; the upstream model stays what was sent.

### 4. Dry-run the whole import

```sh
./run.sh axonhub import $(cat import.args) --projects autocreate --channel-map /w/channels.map --report /w/report.txt
```

This maps every request and builds every artifact without writing, then prints rows, artifact bytes (what the bucket will grow by) and the project summary; `report.txt` has the full project list. To read what would be written, dump a sample: `--sample-mod 1000 --dump /w/dump` writes decompressed artifacts and `rows.jsonl`.

### 5. Import a few, check, import the rest

```sh
./run.sh axonhub import $(cat import.args) --projects autocreate --channel-map /w/channels.map --incremental --write --limit 50
```

Imported rows keep their original timestamps, so filter the dashboard to AxonHub's first days to find them. When they look right, run the same command without `--limit`. It is safe to stop and rerun: ids are derived from AxonHub's, so requests already imported are skipped, and `--incremental` starts just below the last one imported. The run ends by filling `traces`, project first/last seen and the dashboard's aggregates.

### 6. Keep up until AxonHub is retired

While clients still send through AxonHub, rerun the step 5 command now and then; with `--incremental` it only picks up what is new, and it leaves alone requests from the last ten minutes or still in flight. Save it as a script.

### If something changed after rows were imported

| Change | Command |
|---|---|
| Channel map, or rows imported before you had one | `axonhub providers --channel-map … --write` relabels them |
| Rows imported by a version that did not carry costs | `axonhub costs --write` |
| Response format changed | rerun the import with `--rewrite-responses` |
| Aggregates or traces look stale for some period | `axonhub finalize --write --since 2026-01-01` |

## Tidying projects

These commands only touch PicoTera and work whether or not you came from AxonHub. Pass the same `--rules` you would use for imports.

### Merge fragments

PicoTera auto-creates one project per worktree, and one per machine for the same repository. To fold them back:

```sh
./run.sh merge-projects --rules /w/rules.json           # the plan
./run.sh merge-projects --rules /w/rules.json --write   # merge; cannot be undone
```

The plan shows, per repository, which projects merge into which, the paths the result keeps, and renames (a project named after a worktree takes the repository's name).

### Re-sort a project that swallowed others

If a project ended up with a shallow path such as a home directory, every repository below it lands there. To recover:

1. Stop the bleeding: change that project's paths in the dashboard to something that never occurs, so new requests stop matching it.
2. See where its requests belong. `reproject` reads each request back from its artifact and resolves it again, treating the project's own paths as blacklisted:

   ```sh
   ./run.sh reproject --project-id 84 --rules /w/rules.json
   ```

3. Move them. Projects that do not exist yet are created, upstream rows move with their request, and first/last seen and aggregates are refreshed:

   ```sh
   ./run.sh reproject --project-id 84 --rules /w/rules.json --write
   ```

4. Delete the emptied project in the dashboard.

`docs/picotera-project-matching.md` describes how this happens and the changes PicoTera itself would need to prevent it.

## Archiving artifacts

PicoTera stores every request and response as its own zstd object, `artifacts/<UTC day>/<id>.{request,response}.json.zst`. Each request carries the whole conversation so far, screenshots included, so the bucket mostly holds the same bytes over and over, and per-object compression cannot see across objects. Packed by day, a bucket typically shrinks 40 to 100 times.

`pt-store` has four commands:

| Command | What it does |
|---|---|
| `pt-store archive` | Packs each day up to `--min-age-days` (2) old into `<archive bucket>/<day>/`, checks every entry back against its source, then moves the day's `CURRENT` to it and removes the day's earlier generation. With `--delete` it then, day by day as it goes, removes the loose objects that `CURRENT` holds with the same key and ETag; anything written or overwritten since is left for the next run. A day already packed is skipped; one that gained objects is packed again whole. With `--upgrade` it also rewrites every archived day stored in an older format than `--format`, merging any loose objects the day has; it reads such a day's old archive into memory, so run it where that fits: on the largest days seen so far the process peaked near twice the size of the day's original objects. |
| `pt-store serve` | Sits between the dashboard and S3 for presigned artifact reads. It forwards them unchanged and, where S3 answers `NoSuchKey`, serves the entry from the archive. |
| `pt-store get` | Prints one archived artifact, decompressed. |
| `pt-store convert` | Rewrites archived days from format v1 to v2, removing the v1 generation once the v2 one is checked and current. It holds a whole day in memory, so run it on a machine with room for one; `--out-bucket` writes a trial copy elsewhere. |

They read the same `PICOTERA_S3_*` variables as PicoTera; `--endpoint`, `--bucket` and the rest override them, and `--archive-bucket` is required. The key needs read and write on both buckets.

Every entry comes back byte for byte: it is checked by SHA-256 when it is written and when it is read. `archive` writes format v2; v1 is the earlier layout, still read, and still written with `--format v1`.

| | v2 | v1 (earlier) |
|---|---|---|
| Layout | base64 runs of 1000+ characters (screenshots, mostly) stored once per day in 16 MiB blob frames; the rest sorted so a conversation's turns sit together, each stored as the part that differs from the previous entry of its kind (a full keyframe every 32), in 256 MiB text frames | entries in key order, independent zstd frames of 1 GiB raw, 256 MiB window |
| Size, on real traffic | about 2.8 times smaller than v1 | 40–110 times smaller than the bucket |
| One read decodes | about a quarter of v1 | up to 1 GiB |
| Writing | two passes: the loose objects are fetched twice, nothing is kept on local disk | one pass |

A day archived as v1 stays v1 when it is repacked, until `convert` rewrites it.

### Serving reads

The dashboard opens an artifact through a presigned URL. Point PicoTera's `PICOTERA_S3_PUBLIC_URL` at a path in front of `pt-store serve`, for example `https://dashboard.example.com/_artifacts`, strip that prefix on the way in, and give `serve` an `--upstream` that reaches S3 **with the same Host**: the signature covers the Host header, and `serve` passes it through untouched. A reverse proxy that routes `dashboard.example.com/<artifact bucket>/` to S3 does this; PicoTera's own writes can use the same route.

`serve` lets through only `GET` and `HEAD` of `/<bucket>/artifacts/<day>/<file>` and refuses anything else itself, so the route to it can skip the dashboard's login without exposing the rest of S3. It falls back to the archive only on `NoSuchKey`, which S3 returns only after the signature checked out (MinIO names it in `X-Minio-Error-Code` for `HEAD` and in the XML body for `GET`); every other error passes through.

A `serve` that cached a day's `CURRENT` (for a minute) from before a run moved it re-reads it when the cached generation misses a key or its objects are gone, so loose objects and old generations can be deleted the moment `CURRENT` moves. No run ever lists or reads days it has nothing to do for, which matters on an S3 that bills per request. Reads from the archive are cached in memory, including the entries next to the one asked for, since the dashboard tends to open neighbouring requests; the caches drop what has been idle for a minute. Where PicoTera uses a remote S3 that bills for traffic, `--disk-cache DIR` (with `--disk-cache-size`, 1 GiB by default) also keeps what was read from the archive bucket on local disk; only immutable objects are cached, so nothing there goes stale.

```sh
pt-store serve --listen :9910 --upstream https://dashboard.example.com --archive-bucket picotera-archives
pt-store archive --archive-bucket picotera-archives --delete     # daily, e.g. from a systemd timer
pt-store get --archive-bucket picotera-archives artifacts/2026-01-10/<id>.request.json.zst
```

## Reference

| Command | Writes with `--write` |
|---|---|
| `ptt reproject` | projects, `project_id` |
| `ptt merge-projects` | projects, `project_id` |
| `ptt axonhub projects` | never |
| `ptt axonhub import` | request rows, artifacts, archived keys, auto-created projects; without `--write`, optionally `--dump` locally |
| `ptt axonhub providers` | providers, `provider_id` |
| `ptt axonhub costs` | `model_cost` |
| `ptt axonhub finalize` | traces, project first/last seen, aggregates (requires `--write`) |

Every command takes `-h` for its flags.

How AxonHub data lands in PicoTera:

| PicoTera | AxonHub |
|---|---|
| meta row (`type=0`) | `requests` |
| upstream row (`type=1`) | `request_executions`, one per attempt |
| `id` | xid built from `created_at` and the AxonHub id |
| `parent_span_id` | `traces.trace_id` (the client's session id) |
| `provider_id` | `--channel-map`; without one, a channel whose name equals a provider's |
| `api_key_id` | `--key-map`, else a disabled `axonhub/<name>` key |
| `project_id` | working directories in the client's request, under the rules |
| tokens, `model_cost` | `usage_logs`, on the meta row and the winning attempt, in AxonHub's configured currency |
| `annotations` | `{"source":"axonhub", "axonhub_request_id" / "axonhub_execution_id", "axonhub_channel"}` |
| response artifact | AxonHub keeps only the response assembled from a stream: it becomes the JSON body and `aggregated` |

PicoTera's Go module path is the bare `picotera`, so the formats it writes are mirrored in `internal/picotera` (see `doc.go` there for the source commit).

## License

BSD 3-Clause, see `LICENSE`. `internal/picotera` mirrors code from PicoTera under its own BSD 3-Clause license (`internal/picotera/LICENSE`).
