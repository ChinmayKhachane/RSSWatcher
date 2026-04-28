package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/expr-lang/expr"
	"github.com/mmcdole/gofeed"
)

// SimpilifiedItem is the flattened, source-agnostic representation of a
// single feed item. Every feed produces these regardless of whether a
// schema is declared — they back the static items_<feed> table.
type SimpilifiedItem struct {
	ID          string
	Title       string
	Description string
	Content     string
	Link        string
	Updated     *time.Time
	Published   *time.Time
	Author      *gofeed.Person
	Categories  []string
}

// newItemsForProc constructs a processor bound to a single connector.
// Items starts empty with a small reserved capacity to avoid early grows;
// Output is buffered so Extract can hand off without blocking briefly.
func newItemsForProc(rss *RSSConnecter) *ItemsForProc {
	const step = "simplifieditems.newItemsForProc"

	logDebug(step, "constructing item processor", "feed", rss.ID)
	return &ItemsForProc{
		Items:  make([]*SimpilifiedItem, 0, 20),
		Rss:    rss,
		Output: make(chan ExtractedBatch, 4),
	}
}

// Add converts one gofeed.Item into a SimpilifiedItem and buffers it on
// the processor. Caller should hold sole ownership of the processor — Add
// is not safe for concurrent use.
func (i *ItemsForProc) Add(it gofeed.Item) {
	simpitem := SimpilifiedItem{
		ID:          it.GUID,
		Title:       it.Title,
		Categories:  it.Categories,
		Description: it.Description,
		Link:        it.Link,
		// NOTE: Published intentionally falls back to UpdatedParsed today;
		// gofeed.Item.PublishedParsed is a separate field worth using once
		// auth/feed coverage stabilizes.
		Updated:   it.UpdatedParsed,
		Published: it.UpdatedParsed,
		Author:    it.Author,
	}
	i.Items = append(i.Items, &simpitem)
}

// ExtractedBatch is the unit produced by ItemsForProc and consumed by the
// Inserter. Items and Extracted are positionally paired: Extracted[i]
// holds the user-defined field values for Items[i]. Extracted is nil when
// the feed declares no schema.
type ExtractedBatch struct {
	Items     []*SimpilifiedItem
	Extracted []map[string]any
}

// ItemsForProc is the per-feed work buffer between the connector and the
// inserter. It collects items pulled off the connector's channel, runs
// the feed's compiled expressions over them, and emits paired batches on
// Output.
type ItemsForProc struct {
	Items  []*SimpilifiedItem
	Rss    *RSSConnecter
	Output chan ExtractedBatch
}

// Pull blocks until either the connector dispatches a new feed or ctx is
// cancelled. On a successful dispatch, every item in the feed is appended
// to Items via Add. Returns ctx error wrapped on cancellation.
func (i *ItemsForProc) Pull(ctx context.Context) error {
	const step = "simplifieditems.Pull"

	select {
	case <-ctx.Done():
		logWarn(step, "pull cancelled via context", "feed", i.Rss.ID, "err", ctx.Err().Error())
		return fmt.Errorf("ending item transformer: %w", ctx.Err())
	case feed := <-i.Rss.FetchedResults:
		if feed == nil {
			logWarn(step, "received nil feed (channel likely closed)", "feed", i.Rss.ID)
			return fmt.Errorf("ending item transformer: nil feed")
		}
		for _, item := range feed.Items {
			i.Add(*item)
		}
		logStep(step, "pulled items", "feed", i.Rss.ID, "count", len(feed.Items))
	}

	return nil
}

// Extract runs every compiled field expression over every buffered item
// and pushes a paired ExtractedBatch onto Output. Items[] is copied so a
// later RemoveItems() can't race with the inserter still reading the
// batch (the slice header alone wouldn't isolate them).
//
// When the feed has no compiled fields, Extracted is left nil — the
// inserter recognises this and writes only the static items table.
func (i *ItemsForProc) Extract(compiledFeed *CompiledFeed) {
	const step = "simplifieditems.Extract"

	var extractedAll []map[string]any
	if compiledFeed != nil && len(compiledFeed.Fields) > 0 {
		extractedAll = make([]map[string]any, 0, len(i.Items))
		for _, simpitem := range i.Items {
			extracted := map[string]any{}
			for name, field := range compiledFeed.Fields {
				var source any
				switch field.From {
				case "title":
					source = simpitem.Title
				case "description", "summary":
					source = simpitem.Description
				case "content":
					source = simpitem.Content
				case "link":
					source = simpitem.Link
				case "categories":
					source = strings.Join(simpitem.Categories, " ")
				}
				result, err := expr.Run(field.Program, map[string]any{
					"$": source,
				})
				if err != nil {
					logWarn(step, "expr eval failed, skipping field",
						"feed", i.Rss.ID, "field", name, "err", err.Error())
					continue
				}
				extracted[name] = result
			}
			extractedAll = append(extractedAll, extracted)
		}
	}

	itemsCopy := make([]*SimpilifiedItem, len(i.Items))
	copy(itemsCopy, i.Items)

	logStep(step, "dispatching batch",
		"feed", i.Rss.ID,
		"items", len(itemsCopy),
		"with_custom", extractedAll != nil,
	)
	i.Output <- ExtractedBatch{Items: itemsCopy, Extracted: extractedAll}
}

// RemoveItems clears the in-memory buffer while keeping its capacity, so
// the next batch can fill in without reallocating. Safe to call after
// Extract has handed a copy of Items to the channel.
func (i *ItemsForProc) RemoveItems() {
	i.Items = i.Items[:0]
}
