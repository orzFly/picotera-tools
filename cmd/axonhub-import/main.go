// Command axonhub-import copies AxonHub's request history into PicoTera's
// request table and artifact bucket so old requests can be browsed there.
//
// Without --write it only reads: the PicoTera connection is opened read-only
// and no S3 client is created.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/orzfly/picotera-tools/internal/axonimport"
)

const usage = `usage: axonhub-import <command> [flags]

commands:
  projects   report which PicoTera project each request would land in (read-only)
  import     map requests and dump them (default) or write them with --write
  finalize   upsert traces, project first/last seen, refresh aggregates (needs --write)
  reproject  re-resolve the project of rows in one PicoTera project from their
             request artifacts; dry-run unless --write
  providers  create legacy providers for AxonHub channels per a channel map and
             set provider_id on imported rows; dry-run unless --write
  costs      copy AxonHub's recorded costs onto requests imported without them;
             dry-run unless --write
  merge-projects
             fold worktree and other-machine fragments into the repository's
             project; prints the plan unless --write

environment:
  AXONHUB_DB_DSN, PICOTERA_DATABASE_URL,
  PICOTERA_S3_ENDPOINT, PICOTERA_S3_ACCESS_KEY, PICOTERA_S3_SECRET_KEY,
  PICOTERA_S3_BUCKET, PICOTERA_S3_REGION, PICOTERA_S3_USE_SSL, PICOTERA_S3_PATH_STYLE
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "projects":
		err = cmdProjects(ctx, os.Args[2:])
	case "import":
		err = cmdImport(ctx, os.Args[2:])
	case "finalize":
		err = cmdFinalize(ctx, os.Args[2:])
	case "reproject":
		err = cmdReproject(ctx, os.Args[2:])
	case "providers":
		err = cmdProviders(ctx, os.Args[2:])
	case "costs":
		err = cmdCosts(ctx, os.Args[2:])
	case "merge-projects":
		err = cmdMergeProjects(ctx, os.Args[2:])
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if err != nil {
		log.Fatal(err)
	}
}

type common struct {
	rulesPath string
	assign    assignFlag
	roots     rootsFlag
	userID    int64
	afterID   int64
	untilID   int64
	batch     int
	workers   int
}

func (c *common) register(fs *flag.FlagSet) {
	fs.Int64Var(&c.userID, "user-id", 1, "PicoTera user that owns the imported rows")
	fs.Int64Var(&c.afterID, "after-id", 0, "start after this AxonHub request id (resume point)")
	fs.Int64Var(&c.untilID, "until-id", 0, "stop at this AxonHub request id, inclusive (0 = current max)")
	fs.IntVar(&c.batch, "batch", 100, "AxonHub requests per query")
	fs.IntVar(&c.workers, "workers", 8, "concurrent mapping/upload workers")
	fs.StringVar(&c.rulesPath, "rules", "", "JSON file with mainRoot, aliases, blacklist and roots for workspace paths")
	fs.Var(&c.roots, "root", "PATH: workspaces under PATH belong to the project at PATH (repeatable)")
	fs.Var(&c.assign, "assign", "PATH=PROJECT_ID: put workspaces at or under PATH into that project without adding the path (repeatable)")
}

// assignFlag collects --assign PATH=PROJECT_ID; paths are canonicalized
// when applied.
type assignFlag map[string]int32

func (a *assignFlag) String() string { return fmt.Sprint(map[string]int32(*a)) }

func (a *assignFlag) Set(v string) error {
	path, id, ok := strings.Cut(v, "=")
	n, err := strconv.ParseInt(id, 10, 32)
	if !ok || path == "" || err != nil || n <= 0 {
		return fmt.Errorf("want PATH=PROJECT_ID, got %q", v)
	}
	if *a == nil {
		*a = assignFlag{}
	}
	(*a)[path] = int32(n)
	return nil
}

type keyMapFlag map[int64]int32

func (k *keyMapFlag) String() string { return fmt.Sprint(map[int64]int32(*k)) }

func (k *keyMapFlag) Set(v string) error {
	a, b, ok := strings.Cut(v, "=")
	from, err1 := strconv.ParseInt(a, 10, 64)
	to, err2 := strconv.ParseInt(b, 10, 32)
	if !ok || err1 != nil || err2 != nil || from <= 0 || to <= 0 {
		return fmt.Errorf("want AXONHUB_KEY_ID=PICOTERA_KEY_ID, got %q", v)
	}
	if *k == nil {
		*k = keyMapFlag{}
	}
	(*k)[from] = int32(to)
	return nil
}

type renameFlag map[string]string

func (r *renameFlag) String() string { return fmt.Sprint(map[string]string(*r)) }

func (r *renameFlag) Set(v string) error {
	from, to, ok := strings.Cut(v, "=")
	if !ok || from == "" || to == "" {
		return fmt.Errorf("want FROM=TO, got %q", v)
	}
	if *r == nil {
		*r = renameFlag{}
	}
	(*r)[from] = to
	return nil
}

type rootsFlag []string

func (r *rootsFlag) String() string     { return strings.Join(*r, ",") }
func (r *rootsFlag) Set(v string) error { *r = append(*r, v); return nil }

func (c *common) rules() (*axonimport.Rules, error) {
	rules, err := axonimport.LoadRules(c.rulesPath)
	if err != nil {
		return nil, err
	}
	for _, r := range c.roots {
		rules.Roots = append(rules.Roots, rules.Canonical(r))
	}
	return rules, nil
}

func (c *common) applyAssign(p *axonimport.Projects) {
	p.Assign = map[string]int32{}
	for path, id := range c.assign {
		p.Assign[p.Rules.Canonical(path)] = id
	}
}

func cmdProjects(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("projects", flag.ExitOnError)
	var c common
	c.register(fs)
	sampleMod := fs.Int64("sample-mod", 0, "only requests whose id is divisible by this")
	_ = fs.Parse(args)

	axon, pico, err := connect(ctx, false)
	if err != nil {
		return err
	}
	defer axon.Close()
	defer pico.Close()
	src := axonimport.NewSource(axon)
	if err := resolveUntil(ctx, src, &c); err != nil {
		return err
	}
	// Auto-creation is simulated in memory so the report shows the names
	// PicoTera would pick, including collision suffixes.
	rules, err := c.rules()
	if err != nil {
		return err
	}
	projects, err := axonimport.LoadProjects(ctx, pico, c.userID, axonimport.ProjectsAutoCreate, rules)
	if err != nil {
		return err
	}
	c.applyAssign(projects)
	rep, err := axonimport.RunProjectReport(ctx, src, projects, c.afterID, c.untilID, c.batch, c.workers, *sampleMod)
	if err != nil {
		return err
	}
	rep.Write(os.Stdout)
	return nil
}

func cmdImport(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("import", flag.ExitOnError)
	var c common
	c.register(fs)
	limit := fs.Int64("limit", 0, "stop after this many requests (0 = no limit)")
	dump := fs.String("dump", "", "dry-run: write decompressed artifacts and rows.jsonl here (empty = count only)")
	write := fs.Bool("write", false, "write to the live PicoTera database and bucket")
	projectMode := fs.String("projects", "match", "project resolution: off, match, autocreate")
	sampleMod := fs.Int64("sample-mod", 0, "only requests whose id is divisible by this (dry-run spot checks)")
	reportPath := fs.String("report", "", "also write the per-project report to this file")
	var modelRenames renameFlag
	fs.Var(&modelRenames, "model-rename", "FROM=TO: rename the requested model (repeatable); upstream_model is kept")
	var keyMap keyMapFlag
	fs.Var(&keyMap, "key-map", "AXONHUB_KEY_ID=PICOTERA_KEY_ID: label rows of that AxonHub key with an existing PicoTera key (repeatable); other keys get a disabled "+axonimport.ArchivedKeyPrefix+"<name> key")
	channelMap := fs.String("channel-map", "", "file mapping AxonHub channels to PicoTera providers (see providers); default matches by name")
	rewriteResponses := fs.Bool("rewrite-responses", false, "re-upload the response artifacts of requests already imported")
	incremental := fs.Bool("incremental", false, "start after the largest AxonHub request id already imported")
	settle := fs.Duration("settle", 10*time.Minute, "skip requests newer than this; they may still be in flight")
	_ = fs.Parse(args)

	mode := axonimport.ProjectMode(*projectMode)
	if mode != axonimport.ProjectsOff && mode != axonimport.ProjectsMatch && mode != axonimport.ProjectsAutoCreate {
		return fmt.Errorf("unknown --projects %q", *projectMode)
	}
	if *write && *sampleMod > 0 {
		return errors.New("--sample-mod is for dry runs only")
	}
	if *write && *dump != "" {
		return errors.New("--write and --dump are mutually exclusive")
	}

	axon, pico, err := connect(ctx, *write)
	if err != nil {
		return err
	}
	defer axon.Close()
	defer pico.Close()
	src := axonimport.NewSource(axon)
	if c.untilID == 0 {
		if c.untilID, err = src.SafeUntil(ctx, *settle); err != nil {
			return err
		}
	}
	if *incremental {
		hw, err := axonimport.ImportedHighWater(ctx, pico, c.userID)
		if err != nil {
			return err
		}
		// Requests within a batch are written concurrently, so a crash can
		// leave holes just below the high-water mark; re-scanning a margin is
		// cheap because existing rows are skipped.
		c.afterID = max(c.afterID, hw-1000)
	}
	log.Printf("importing axonhub requests in (%d, %d]", c.afterID, c.untilID)
	if c.afterID >= c.untilID {
		log.Print("nothing new to import")
		return nil
	}

	channels, err := src.Channels(ctx)
	if err != nil {
		return err
	}
	providers, err := axonimport.LoadProviders(ctx, pico)
	if err != nil {
		return err
	}
	rules, err := c.rules()
	if err != nil {
		return err
	}
	projects, err := axonimport.LoadProjects(ctx, pico, c.userID, mode, rules)
	if err != nil {
		return err
	}
	c.applyAssign(projects)

	var channelProviders map[int64]int32
	if *channelMap != "" {
		chmap, err := axonimport.ReadChannelMap(*channelMap)
		if err != nil {
			return err
		}
		channelProviders = axonimport.ResolveChannels(chmap, providerIDs(providers))
	}

	keyNames, err := axonimport.LoadAxonKeyNames(ctx, axon)
	if err != nil {
		return err
	}
	keys := &axonimport.Keys{UserID: c.userID, Map: keyMap, Names: keyNames}

	var target axonimport.Target
	if *write {
		keys.DB = pico
		s3, bucket, err := s3Client()
		if err != nil {
			return err
		}
		projects.DB = pico
		target = &axonimport.Picotera{DB: pico, S3: s3, Bucket: bucket}
	} else {
		target, err = axonimport.NewDumpTarget(*dump)
		if err != nil {
			return err
		}
	}

	im := &axonimport.Importer{
		Source:           src,
		Mapper:           &axonimport.Mapper{UserID: c.userID, Channels: channels, Providers: providers, Projects: projects, Keys: keys, Pico: pico, ChannelProviders: channelProviders, ModelRenames: modelRenames},
		Target:           target,
		Batch:            c.batch,
		Workers:          c.workers,
		AfterID:          c.afterID,
		UntilID:          c.untilID,
		Limit:            *limit,
		SampleMod:        *sampleMod,
		RewriteResponses: *rewriteResponses,
	}
	if *reportPath != "" {
		im.Report = axonimport.NewProjectReport()
	}
	err = im.Run(ctx)
	im.Summary(os.Stderr)
	if im.Report != nil {
		f, ferr := os.Create(*reportPath)
		if ferr != nil {
			return errors.Join(err, ferr)
		}
		im.Report.Write(f)
		if ferr := f.Close(); ferr != nil {
			return errors.Join(err, ferr)
		}
	}
	if err != nil || !*write || im.Stats.Rows.Load() == 0 {
		return err
	}
	return axonimport.Finalize(ctx, pico, c.userID, im.Earliest)
}

func cmdFinalize(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("finalize", flag.ExitOnError)
	userID := fs.Int64("user-id", 1, "PicoTera user that owns the imported rows")
	since := fs.String("since", "2026-02-01", "refresh continuous aggregates from this date")
	write := fs.Bool("write", false, "required: finalize writes to the live PicoTera database")
	_ = fs.Parse(args)
	if !*write {
		return errors.New("finalize writes to PicoTera; pass --write")
	}
	t, err := time.Parse(time.DateOnly, *since)
	if err != nil {
		return err
	}
	pico, err := pool(ctx, "PICOTERA_DATABASE_URL", false)
	if err != nil {
		return err
	}
	defer pico.Close()
	return axonimport.Finalize(ctx, pico, *userID, t)
}

func cmdReproject(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("reproject", flag.ExitOnError)
	userID := fs.Int64("user-id", 1, "PicoTera user that owns the rows")
	projectID := fs.Int("project-id", 0, "project whose rows are re-resolved (required)")
	batch := fs.Int("batch", 200, "meta rows per page")
	workers := fs.Int("workers", 16, "concurrent artifact fetches")
	write := fs.Bool("write", false, "create projects and move rows in the live PicoTera database")
	rulesPath := fs.String("rules", "", "JSON rules file (see import -rules)")
	_ = fs.Parse(args)
	if *projectID <= 0 {
		return errors.New("--project-id is required")
	}

	pico, err := pool(ctx, "PICOTERA_DATABASE_URL", !*write)
	if err != nil {
		return err
	}
	defer pico.Close()
	s3, bucket, err := s3Client()
	if err != nil {
		return err
	}
	rules, err := axonimport.LoadRules(*rulesPath)
	if err != nil {
		return err
	}
	// The source project's own paths are what swallowed everything below
	// them, so they may only match exactly from now on.
	var srcPaths []string
	if err := pico.QueryRow(ctx, `SELECT paths FROM project WHERE id = $1 AND user_id = $2`, *projectID, *userID).Scan(&srcPaths); err != nil {
		return fmt.Errorf("load project %d: %w", *projectID, err)
	}
	for _, p := range srcPaths {
		rules.Blacklist[rules.Canonical(p)] = true
	}
	projects, err := axonimport.LoadProjects(ctx, pico, *userID, axonimport.ProjectsAutoCreate, rules)
	if err != nil {
		return err
	}
	if *write {
		projects.DB = pico
	}

	rp := &axonimport.Reproject{
		DB: pico, S3: s3, Bucket: bucket, Projects: projects,
		ProjectID: int32(*projectID), UserID: *userID,
		Batch: *batch, Workers: *workers, Write: *write,
	}
	err = rp.Run(ctx)
	rp.Report.Write(os.Stdout)
	fmt.Fprintf(os.Stderr, "moved %d, stayed %d (artifact missing %d)\n", rp.Moved.Load(), rp.Stayed.Load(), rp.Missing.Load())
	if err != nil || !*write {
		return err
	}
	since := time.Now().AddDate(0, 0, -40)
	return axonimport.RefreshProjects(ctx, pico, rp.ProjectID, rp.TargetProjects(), since)
}

func providerIDs(byName map[string]int64) map[string]int32 {
	out := make(map[string]int32, len(byName))
	for name, id := range byName {
		out[name] = int32(id)
	}
	return out
}

func cmdProviders(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("providers", flag.ExitOnError)
	channelMap := fs.String("channel-map", "", "file of '<channel id> id <provider id>' / '<channel id> new <name>' lines (required)")
	batch := fs.Int("batch", 1000, "rows per round trip")
	write := fs.Bool("write", false, "create providers and update rows in the live PicoTera database")
	_ = fs.Parse(args)
	if *channelMap == "" {
		return errors.New("--channel-map is required")
	}
	chmap, err := axonimport.ReadChannelMap(*channelMap)
	if err != nil {
		return err
	}
	axon, pico, err := connect(ctx, *write)
	if err != nil {
		return err
	}
	defer axon.Close()
	defer pico.Close()

	channels, err := axonimport.LoadAxonChannels(ctx, axon)
	if err != nil {
		return err
	}
	for id := range channels {
		if _, ok := chmap[id]; !ok {
			fmt.Fprintf(os.Stderr, "warning: channel #%d %s is not in the map\n", id, channels[id].Name)
		}
	}
	byName, err := axonimport.LoadProviders(ctx, pico)
	if err != nil {
		return err
	}
	existing := providerIDs(byName)
	plans := axonimport.PlanLegacyProviders(chmap, channels)
	axonimport.ShowLegacyProviders(os.Stdout, plans, existing)
	if *write {
		if err := axonimport.EnsureLegacyProviders(ctx, pico, plans, existing); err != nil {
			return err
		}
	}
	src := axonimport.NewSource(axon)
	until, err := src.MaxID(ctx)
	if err != nil {
		return err
	}
	b := &axonimport.BackfillProviders{Axon: axon, Pico: pico, Channels: axonimport.ResolveChannels(chmap, existing), Batch: *batch, UntilID: until, Write: *write}
	err = b.Run(ctx)
	verb := "would update"
	if *write {
		verb = "updated"
	}
	fmt.Fprintf(os.Stderr, "%s provider_id on %d rows\n", verb, b.Rows)
	if err != nil || !*write || b.Rows == 0 {
		return err
	}
	return axonimport.RefreshOverview(ctx, pico, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
}

func cmdCosts(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("costs", flag.ExitOnError)
	afterID := fs.Int64("after-id", 0, "start after this AxonHub request id")
	untilID := fs.Int64("until-id", 0, "stop at this AxonHub request id, inclusive (0 = current max)")
	batch := fs.Int("batch", 500, "requests per round trip")
	write := fs.Bool("write", false, "update the live PicoTera database")
	_ = fs.Parse(args)

	axon, pico, err := connect(ctx, *write)
	if err != nil {
		return err
	}
	defer axon.Close()
	defer pico.Close()
	if *untilID == 0 {
		if *untilID, err = axonimport.NewSource(axon).MaxID(ctx); err != nil {
			return err
		}
	}
	currency, err := axonimport.NewSource(axon).Currency(ctx)
	if err != nil {
		return err
	}
	b := &axonimport.BackfillCosts{Axon: axon, Pico: pico, Batch: *batch, AfterID: *afterID, UntilID: *untilID, Write: *write, Currency: currency}
	err = b.Run(ctx)
	verb := "would update"
	if *write {
		verb = "updated"
	}
	fmt.Fprintf(os.Stderr, "%s %d rows on %d requests, %.2f %s\n", verb, b.Rows, b.Requests, b.Total, currency)
	if err != nil || !*write || b.Rows == 0 {
		return err
	}
	return axonimport.RefreshOverview(ctx, pico, b.Earliest)
}

func cmdMergeProjects(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("merge-projects", flag.ExitOnError)
	userID := fs.Int64("user-id", 1, "PicoTera user whose projects are merged")
	write := fs.Bool("write", false, "apply the plan; merges cannot be undone")
	rulesPath := fs.String("rules", "", "JSON rules file (see import -rules)")
	_ = fs.Parse(args)
	rules, err := axonimport.LoadRules(*rulesPath)
	if err != nil {
		return err
	}

	pico, err := pool(ctx, "PICOTERA_DATABASE_URL", !*write)
	if err != nil {
		return err
	}
	defer pico.Close()
	projects, err := axonimport.LoadMergeProjects(ctx, pico, *userID)
	if err != nil {
		return err
	}
	plans := rules.PlanMerges(projects)
	axonimport.ShowMergePlans(os.Stdout, plans)
	if !*write {
		if len(plans) > 0 {
			os.Exit(1)
		}
		return nil
	}
	fmt.Println()
	return axonimport.ApplyMergePlans(ctx, pico, *userID, plans, os.Stdout)
}

func resolveUntil(ctx context.Context, src *axonimport.Source, c *common) error {
	if c.untilID > 0 {
		return nil
	}
	max, err := src.MaxID(ctx)
	if err != nil {
		return err
	}
	c.untilID = max
	return nil
}

func connect(ctx context.Context, write bool) (axon, pico *pgxpool.Pool, err error) {
	axon, err = pool(ctx, "AXONHUB_DB_DSN", true)
	if err != nil {
		return nil, nil, err
	}
	pico, err = pool(ctx, "PICOTERA_DATABASE_URL", !write)
	if err != nil {
		axon.Close()
		return nil, nil, err
	}
	return axon, pico, nil
}

func pool(ctx context.Context, env string, readOnly bool) (*pgxpool.Pool, error) {
	dsn := os.Getenv(env)
	if dsn == "" {
		return nil, fmt.Errorf("%s is not set", env)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", env, err)
	}
	if readOnly {
		cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	}
	// Keep the load on AxonHub's shared Postgres and PicoTera's live
	// database modest; both serve other traffic.
	cfg.MaxConns = 8
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", env, err)
	}
	if err := p.Ping(ctx); err != nil {
		p.Close()
		return nil, fmt.Errorf("ping %s: %w", env, err)
	}
	return p, nil
}

func s3Client() (*minio.Client, string, error) {
	endpoint := os.Getenv("PICOTERA_S3_ENDPOINT")
	bucket := os.Getenv("PICOTERA_S3_BUCKET")
	if endpoint == "" || bucket == "" {
		return nil, "", errors.New("PICOTERA_S3_ENDPOINT and PICOTERA_S3_BUCKET must be set")
	}
	useSSL, _ := strconv.ParseBool(os.Getenv("PICOTERA_S3_USE_SSL"))
	lookup := minio.BucketLookupAuto
	if v, err := strconv.ParseBool(os.Getenv("PICOTERA_S3_PATH_STYLE")); err == nil {
		lookup = minio.BucketLookupDNS
		if v {
			lookup = minio.BucketLookupPath
		}
	}
	region := os.Getenv("PICOTERA_S3_REGION")
	if region == "" {
		region = "us-east-1"
	}
	c, err := minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(os.Getenv("PICOTERA_S3_ACCESS_KEY"), os.Getenv("PICOTERA_S3_SECRET_KEY"), ""),
		Secure:       useSSL,
		Region:       region,
		BucketLookup: lookup,
	})
	return c, bucket, err
}
