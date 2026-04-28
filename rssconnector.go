package main

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/mmcdole/gofeed"
)

// RSSConnecter is one feed's HTTP/parsing client plus its outbound
// channel of fetched feed snapshots. One instance per feed; not safe for
// concurrent Fetch calls (Poll serialises them via the ticker).
type RSSConnecter struct {
	ID                string
	Parser            *gofeed.Parser
	Url               string
	ResolvedUrl       string
	Etag              string
	LastModifiedSince *time.Time
	Timeout           *time.Duration
	LastUpdated       *time.Time
	FetchedResults    chan *gofeed.Feed
}

// Sentinel errors for HTTP-level fetch failures. Reserved for the
// status-code branching that auth/retry handling will need; not yet
// surfaced by Fetch (gofeed wraps errors as opaque strings today).
var (
	ErrNotFound     = errors.New("not found")
	ErrUnauthorized = errors.New("unauthorized")
	ErrRateLimited  = errors.New("rate limited")
	ErrServerError  = errors.New("server error")
)

// newRSSConnector returns a connector with the URL, ID, and timeout set.
// Call SetConnection before Fetch/Poll — the parser, HTTP client, and
// FetchedResults channel are initialised there, not in this constructor.
func newRSSConnector(url string, id string, timeout time.Duration) *RSSConnecter {
	return &RSSConnecter{
		Url:     url,
		ID:      id,
		Timeout: &timeout,
	}
}

// SetConnection builds the HTTP client and gofeed parser, then performs
// one priming fetch to resolve FeedLink (the canonical URL the feed
// advertises for itself). FetchedResults is allocated here, not in the
// constructor, so a half-built connector cannot be polled by accident.
func (rss *RSSConnecter) SetConnection() error {
	const step = "rssconnector.SetConnection"

	logStep(step, "initializing connection", "url", rss.Url)

	client := &http.Client{}
	client.Timeout = *rss.Timeout
	rss.Parser = gofeed.NewParser()
	rss.Parser.Client = client

	logDebug(step, "parsing feed URL for resolution", "id", rss.ID, "timeout", rss.Timeout.String())
	temp, err := rss.Parser.ParseURL(rss.Url)
	if err != nil {
		logError(step, err, "url", rss.Url)
		return err
	}
	rss.ResolvedUrl = temp.FeedLink
	rss.FetchedResults = make(chan *gofeed.Feed, 10)

	logStep(step, "connection established", "id", rss.ID, "resolved_url", rss.ResolvedUrl)
	return nil
}

// Fetch performs one parse of the resolved URL. When the feed's reported
// UpdatedParsed has not advanced since the last fetch, the call is a
// no-op (no dispatch, no error). On a fresh update, the parsed feed is
// pushed onto FetchedResults and LastUpdated is advanced.
//
// NOTE: rss.LastUpdated starts nil; the Equal() call below will nil-deref
// on the first fetch — see CLAUDE.md.
func (rss *RSSConnecter) Fetch(ctx context.Context) error {
	const step = "rssconnector.Fetch"

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	logDebug(step, "fetching feed", "id", rss.ID, "resolved_url", rss.ResolvedUrl)
	feed_data, err := rss.Parser.ParseURLWithContext(rss.ResolvedUrl, ctx)
	if err != nil {
		logError(step, err, "id", rss.ID, "resolved_url", rss.ResolvedUrl)
		return err
	}

	if rss.LastUpdated.Equal(*feed_data.UpdatedParsed) {
		logStep(step, "feed unchanged, skipping", "id", rss.ID, "last_updated", rss.LastUpdated)
		return nil
	}

	rss.FetchedResults <- feed_data
	rss.LastUpdated = feed_data.UpdatedParsed
	logStep(step, "feed updated, dispatched to channel",
		"id", rss.ID,
		"updated", feed_data.UpdatedParsed,
		"items", len(feed_data.Items),
	)
	return nil
}

// Poll launches a goroutine that fetches on every tick of interval until
// ctx is cancelled or a Fetch fails. The returned channel emits exactly
// one error before the goroutine exits — either ctx.Err() or the fetch
// failure that caused the loop to terminate. Callers must read the
// channel once to avoid leaking the goroutine on the unbuffered send.
func (rss *RSSConnecter) Poll(ctx context.Context, interval time.Duration) <-chan error {
	const step = "rssconnector.Poll"

	logStep(step, "starting poll loop", "id", rss.ID, "interval", interval.String())

	errchan := make(chan error)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
	caseLoop:
		for {
			select {
			case <-ticker.C:
				err := rss.Fetch(ctx)
				if err != nil {
					logError(step, err, "id", rss.ID, "reason", "fetch failed, stopping poll")
					errchan <- err
					break caseLoop
				}
			case <-ctx.Done():
				logWarn(step, "poll cancelled via context", "id", rss.ID, "err", ctx.Err().Error())
				errchan <- ctx.Err()
				break caseLoop
			}
		}
	}()

	return errchan
}
