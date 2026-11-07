package store

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/klauspost/compress/zstd"
)

const usage = `usage: pt-store <command> [flags]

Keeps PicoTera's request artifacts readable after old days are packed into
daily archives.

commands:
  serve    forward the dashboard's presigned artifact reads to S3, answering
           NoSuchKey from the archive
  archive  pack days older than --min-age-days into the archive bucket
  get      print one archived artifact, decompressed, to stdout
  convert  rewrite archived days from format v1 as v2 (holds a day in memory)

environment (flags of the same name override them):
  PICOTERA_S3_ENDPOINT, PICOTERA_S3_ACCESS_KEY, PICOTERA_S3_SECRET_KEY,
  PICOTERA_S3_BUCKET, PICOTERA_S3_REGION, PICOTERA_S3_USE_SSL,
  PICOTERA_S3_PATH_STYLE
`

// Main runs pt-store with os.Args-style arguments and returns the exit code.
func Main(args []string) int {
	if len(args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	var err error
	switch args[1] {
	case "serve":
		err = cmdServe(ctx, args[2:], log)
	case "archive":
		err = cmdArchive(ctx, args[2:], log)
	case "get":
		err = cmdGet(ctx, args[2:])
	case "convert":
		err = cmdConvert(ctx, args[2:], log)
	case "-h", "--help", "help":
		fmt.Fprint(os.Stdout, usage)
		return 0
	default:
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
	if err != nil {
		log.Error(args[1]+" failed", "err", err)
		return 1
	}
	return 0
}

type commonFlags struct {
	s3         s3Config
	archBucket string
	diskCache  string
	diskSize   string
}

// readFlags are the flags of the commands that read the archive.
func (c *commonFlags) registerRead(fs *flag.FlagSet) {
	fs.StringVar(&c.diskCache, "disk-cache", "", "keep archive reads in this directory (off when empty)")
	fs.StringVar(&c.diskSize, "disk-cache-size", "1GiB", "upper bound of --disk-cache")
}

// archiveStore is the store archive reads go through, behind the disk cache when there is one.
func (c *commonFlags) archiveStore(st store) (store, error) {
	if c.diskCache == "" {
		return st, nil
	}
	max, err := parseSize(c.diskSize)
	if err != nil {
		return nil, err
	}
	return newDiskCache(st, c.diskCache, c.archBucket, max)
}

func envBool(name string, def bool) bool {
	if v, err := strconv.ParseBool(os.Getenv(name)); err == nil {
		return v
	}
	return def
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func (c *commonFlags) register(fs *flag.FlagSet) {
	// The endpoint picotera writes through need not reach the archive bucket (a proxy in front
	// of S3 may route only the artifact bucket), so each of these can be overridden here.
	fs.StringVar(&c.s3.endpoint, "endpoint", os.Getenv("PICOTERA_S3_ENDPOINT"), "S3 endpoint, host[:port]")
	fs.StringVar(&c.s3.bucket, "bucket", os.Getenv("PICOTERA_S3_BUCKET"), "picotera's artifact bucket")
	fs.StringVar(&c.s3.region, "region", envOr("PICOTERA_S3_REGION", "us-east-1"), "S3 region")
	fs.BoolVar(&c.s3.useSSL, "use-ssl", envBool("PICOTERA_S3_USE_SSL", false), "talk HTTPS to the endpoint")
	fs.BoolVar(&c.s3.pathStyle, "path-style", envBool("PICOTERA_S3_PATH_STYLE", true), "path-style bucket addressing")
	fs.StringVar(&c.archBucket, "archive-bucket", "", "bucket holding the daily archives (required)")
	c.s3.accessKey = os.Getenv("PICOTERA_S3_ACCESS_KEY")
	c.s3.secretKey = os.Getenv("PICOTERA_S3_SECRET_KEY")
}

func (c *commonFlags) check() error {
	switch {
	case c.s3.endpoint == "":
		return errors.New("no endpoint: set PICOTERA_S3_ENDPOINT or --endpoint")
	case c.s3.bucket == "":
		return errors.New("no bucket: set PICOTERA_S3_BUCKET or --bucket")
	case c.archBucket == "":
		return errors.New("--archive-bucket is required")
	}
	return nil
}

func (c *commonFlags) store() (*minioStore, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	return newMinioStore(c.s3)
}

func cmdServe(ctx context.Context, args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	var c commonFlags
	c.register(fs)
	c.registerRead(fs)
	listen := fs.String("listen", ":9910", "address to listen on")
	upstream := fs.String("upstream", "", "where presigned reads are forwarded with their Host kept (required)")
	maxDecodes := fs.Int("max-decodes", 2, "frames decoded at once")
	// Screenshot-heavy days barely compress per entry, so cached neighbours cost nearly their
	// raw size; these budgets keep a burst of dashboard clicks under about 2.5 GiB.
	entryCache := fs.Int64("entry-cache", 256<<20, "entry cache budget, bytes")
	frameCache := fs.Int64("frame-cache", 128<<20, "frame cache budget, bytes")
	lookbehind := fs.Int64("lookbehind", 128<<20, "bytes of entries before a hit to cache")
	lookahead := fs.Int64("lookahead", 32<<20, "bytes of entries after a hit to cache")
	idle := fs.Duration("cache-idle", time.Minute, "drop cached entries, frames and indexes unused this long")
	fs.Parse(args)
	if *upstream == "" {
		return errors.New("--upstream is required")
	}
	up, err := url.Parse(*upstream)
	if err != nil {
		return err
	}
	st, err := c.store()
	if err != nil {
		return err
	}
	ast, err := c.archiveStore(st)
	if err != nil {
		return err
	}
	fb := newFallback(ast, c.archBucket, *maxDecodes, *entryCache, *frameCache, *lookbehind, *lookahead, *idle, log)
	srv := &http.Server{Addr: *listen, Handler: newServer(up, c.s3.bucket, fb, log), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(shut)
	}()
	log.Info("serving", "listen", *listen, "upstream", up.String())
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

func cmdArchive(ctx context.Context, args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("archive", flag.ExitOnError)
	var c commonFlags
	c.register(fs)
	a := &archiver{frameSize: defaultFrameSize, windowLog: defaultWindowLog, level: zstd.SpeedBestCompression, now: time.Now, log: log}
	fs.IntVar(&a.minAge, "min-age-days", 2, "archive days up to today (UTC) minus this many")
	fs.StringVar(&a.day, "day", "", "archive this one day only (YYYY-MM-DD)")
	fs.BoolVar(&a.del, "delete", false, "delete loose objects the archive holds with the same key and ETag")
	fs.BoolVar(&a.dryRun, "dry-run", false, "list the days that would be archived")
	fs.IntVar(&a.fetchers, "fetchers", 8, "loose objects downloaded at once")
	fs.StringVar(&a.format, "format", "v2", "archive format for new days: v2, or v1 for the older layout")
	fs.Parse(args)
	if a.format != "v1" && a.format != "v2" {
		return fmt.Errorf("--format must be v1 or v2, not %q", a.format)
	}
	st, err := c.store()
	if err != nil {
		return err
	}
	a.st, a.bucket, a.archBucket = st, c.s3.bucket, c.archBucket
	return a.run(ctx)
}

func cmdGet(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	var c commonFlags
	c.register(fs)
	c.registerRead(fs)
	fs.Parse(args)
	if fs.NArg() != 1 {
		return errors.New("usage: pt-store get [flags] artifacts/<day>/<file>")
	}
	st, err := c.store()
	if err != nil {
		return err
	}
	ast, err := c.archiveStore(st)
	if err != nil {
		return err
	}
	fb := newFallback(ast, c.archBucket, 1, 1<<30, 0, 0, 0, 0, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	e, err := fb.lookup(ctx, fs.Arg(0))
	if err != nil {
		return err
	}
	if e == nil {
		return fmt.Errorf("%s is not archived", fs.Arg(0))
	}
	raw, err := decodeHTTPBody(e.body)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(raw)
	return err
}
