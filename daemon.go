package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// feedRunner bundles everything daemon needs to drive one feed end-to-end:
// the compiled schema, the connector, and the per-feed item processor.
type feedRunner struct {
	compiled *CompiledFeed
	rss      *RSSConnecter
	proc     *ItemsForProc
}

func main() {
	const step = "daemon.main"

	// Root context: cancelled by SIGINT/SIGTERM or by any fatal pipeline
	// error. Every goroutine in the pipeline derives from this context, so
	// one cancel tears the whole tree down.
	rootCtx, rootCancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer rootCancel()

	cfg, err := LoadConfig("./example.yml")
	if err != nil {
		logError(step, err, "reason", "load config")
		os.Exit(1)
	}
	if err := cfg.Validate(); err != nil {
		logError(step, err, "reason", "validate config")
		os.Exit(1)
	}
	compiled, err := CompileFeeds(cfg)
	if err != nil {
		logError(step, err, "reason", "compile feeds")
		os.Exit(1)
	}
	if len(compiled) == 0 {
		logWarn(step, "no feeds compiled (none have a schema); nothing to do")
		return
	}

	db, err := NewDB(rootCtx, "")
	if err != nil {
		logError(step, err, "reason", "open db")
		os.Exit(1)
	}
	defer db.Close()

	runners := buildRunners(rootCtx, db, compiled)
	if len(runners) == 0 {
		logError(step, errors.New("no feeds initialized"),
			"reason", "all feeds failed setup")
		return
	}

	// Buffered so a fast-failing worker never blocks on send when nothing
	// has read yet. Sized generously: 3 goroutines × N feeds.
	fatal := make(chan error, len(runners)*3)

	var wg sync.WaitGroup
	for name, r := range runners {
		startPoll(rootCtx, &wg, fatal, name, r)
		startProcessor(rootCtx, &wg, name, r)
		startInserter(rootCtx, &wg, fatal, db, name, r)
	}

	// Block until we either get a shutdown signal or a fatal error from a
	// worker. In either case, cancel the root context so every goroutine
	// downstream unwinds.
	select {
	case <-rootCtx.Done():
		logStep(step, "shutdown signal received", "err", rootCtx.Err().Error())
	case err := <-fatal:
		logError(step, err, "reason", "fatal pipeline error")
		rootCancel()
	}

	// Bounded grace period for in-flight transactions to commit and
	// goroutines to return. Beyond this we exit anyway — the deferred
	// db.Close() and signal handlers still run.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		logStep(step, "all workers exited cleanly")
	case <-time.After(10 * time.Second):
		logWarn(step, "shutdown grace period exceeded, exiting")
	}
}

// buildRunners ensures tables exist and constructs a runner per feed.
// Feeds whose connector setup fails are skipped (logged) rather than
// failing the whole daemon — one broken URL shouldn't stop healthy feeds.
// A table-creation failure, however, is fatal.
func buildRunners(ctx context.Context, db *DB, compiled []CompiledFeed) map[string]*feedRunner {
	const step = "daemon.buildRunners"

	runners := make(map[string]*feedRunner, len(compiled))
	for i := range compiled {
		c := &compiled[i]
		name := c.Config.Name

		if err := db.CreateTables(ctx, c); err != nil {
			logError(step, err, "feed", name, "reason", "create tables")
			continue
		}

		interval := time.Duration(c.Config.Interval) * time.Second
		rss := newRSSConnector(c.Config.Url, name, interval)
		if err := rss.SetConnection(); err != nil {
			logError(step, err, "feed", name, "reason", "set connection")
			continue
		}

		runners[name] = &feedRunner{
			compiled: c,
			rss:      rss,
			proc:     newItemsForProc(rss),
		}
	}
	return runners
}

// startPoll runs the RSS poll loop. Poll returns one error before exiting
// (either ctx.Err or a fetch failure); the goroutine reads that single
// value and forwards genuine failures to the fatal channel.
func startPoll(ctx context.Context, wg *sync.WaitGroup, fatal chan<- error, name string, r *feedRunner) {
	const step = "daemon.poll"
	wg.Add(1)
	go func() {
		defer wg.Done()
		interval := time.Duration(r.compiled.Config.Interval) * time.Second
		errc := r.rss.Poll(ctx, interval)

		err, ok := <-errc
		if !ok || err == nil {
			return
		}
		if isCtxErr(err) {
			logStep(step, "poll exited via context", "feed", name)
			return
		}
		select {
		case fatal <- fmt.Errorf("feed %s poll: %w", name, err):
		default:
		}
	}()
}

// startProcessor drives the Pull → Extract → RemoveItems loop. It owns
// proc.Output and closes it on exit so the inserter sees EOF and shuts
// down cleanly without needing a separate signal.
func startProcessor(ctx context.Context, wg *sync.WaitGroup, name string, r *feedRunner) {
	const step = "daemon.processor"
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(r.proc.Output)

		for {
			if err := r.proc.Pull(ctx); err != nil {
				logStep(step, "processor exiting", "feed", name, "err", err.Error())
				return
			}
			r.proc.Extract(r.compiled)
			r.proc.RemoveItems()

			if ctx.Err() != nil {
				logStep(step, "processor exiting via context", "feed", name)
				return
			}
		}
	}()
}

// startInserter runs the DB sink for one feed. Inserter.Run returns nil on
// channel close (clean shutdown) and ctx.Err on cancellation; anything else
// is treated as fatal.
func startInserter(ctx context.Context, wg *sync.WaitGroup, fatal chan<- error, db *DB, name string, r *feedRunner) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ins := NewInserter(db, r.compiled, r.proc.Output)
		if err := ins.Run(ctx); err != nil && !isCtxErr(err) {
			select {
			case fatal <- fmt.Errorf("feed %s inserter: %w", name, err):
			default:
			}
		}
	}()
}

func isCtxErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
