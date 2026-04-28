package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Inserter consumes ExtractedBatch values produced by an ItemsForProc and
// persists each batch into the feed's items_<name> table and, when the
// feed declares a schema, the matching custom_<name> table. One Inserter
// per feed; Run is meant to be called on its own goroutine.
type Inserter struct {
	DB   *DB
	Feed *CompiledFeed
	In   <-chan ExtractedBatch
}

// NewInserter wires an Inserter around a feed's processor. The In channel
// is typically the ItemsForProc.Output for the same feed.
func NewInserter(db *DB, feed *CompiledFeed, in <-chan ExtractedBatch) *Inserter {
	return &Inserter{DB: db, Feed: feed, In: in}
}

// Run blocks on the input channel and inserts each received batch until ctx
// is cancelled or the channel is closed. Per-batch errors are logged but
// do not stop the loop — a bad batch shouldn't kill the pipeline.
func (ins *Inserter) Run(ctx context.Context) error {
	const step = "dbinserter.Run"
	name := ins.Feed.Config.Name

	logStep(step, "insert loop started", "feed", name)
	for {
		select {
		case <-ctx.Done():
			logWarn(step, "insert loop cancelled", "feed", name, "err", ctx.Err().Error())
			return ctx.Err()
		case batch, ok := <-ins.In:
			if !ok {
				logStep(step, "input channel closed, stopping", "feed", name)
				return nil
			}
			if err := ins.insertBatch(ctx, batch); err != nil {
				logError(step, err, "feed", name, "items", len(batch.Items))
			}
		}
	}
}

// insertBatch writes one batch atomically. The static items_<feed> row and
// its paired custom_<feed> row commit in a single transaction so partial
// state is never visible.
func (ins *Inserter) insertBatch(ctx context.Context, b ExtractedBatch) error {
	const step = "dbinserter.insertBatch"
	if len(b.Items) == 0 {
		return nil
	}
	name := ins.Feed.Config.Name

	tx, err := ins.DB.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) // no-op once Commit succeeds

	custom := hasCustomSchema(ins.Feed)
	insertedItems, insertedCustom := 0, 0

	for i, it := range b.Items {
		id, err := ins.insertItem(ctx, tx, it)
		if err != nil {
			return fmt.Errorf("insert item %s: %w", it.ID, err)
		}
		if id == 0 {
			// duplicate link; nothing to pair a custom row with
			continue
		}
		insertedItems++

		if !custom || i >= len(b.Extracted) {
			continue
		}
		if err := ins.insertCustom(ctx, tx, id, b.Extracted[i]); err != nil {
			return fmt.Errorf("insert custom for item %s: %w", it.ID, err)
		}
		insertedCustom++
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	logStep(step, "batch committed",
		"feed", name,
		"items", insertedItems,
		"custom", insertedCustom,
	)
	return nil
}

// insertItem writes one row into items_<feed>. Returns the new id, or 0
// when the row already existed (link UNIQUE collision via ON CONFLICT DO
// NOTHING) so the caller can skip the paired custom write.
func (ins *Inserter) insertItem(ctx context.Context, tx pgx.Tx, it *SimpilifiedItem) (int64, error) {
	table := pgx.Identifier{itemsTable(ins.Feed.Config.Name)}.Sanitize()
	stmt := fmt.Sprintf(`
		INSERT INTO %s (title, description, content, link, updated, published, author, categories)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (link) DO NOTHING
		RETURNING id`, table)

	var author string
	if it.Author != nil {
		author = it.Author.Name
	}

	var id int64
	err := tx.QueryRow(ctx, stmt,
		it.Title, it.Description, it.Content, it.Link,
		nullTime(it.Updated), nullTime(it.Published),
		author, it.Categories,
	).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return id, nil
}

// insertCustom writes one row into custom_<feed> referencing item_id.
// Iterates the compiled field set so the column order is stable and only
// known fields are written.
func (ins *Inserter) insertCustom(ctx context.Context, tx pgx.Tx, itemID int64, fields map[string]any) error {
	if len(fields) == 0 {
		return nil
	}
	cols := []string{"item_id"}
	placeholders := []string{"$1"}
	args := []any{itemID}

	idx := 2
	for fname := range ins.Feed.Fields {
		v, ok := fields[fname]
		if !ok {
			continue
		}
		cols = append(cols, pgx.Identifier{fname}.Sanitize())
		placeholders = append(placeholders, fmt.Sprintf("$%d", idx))
		args = append(args, v)
		idx++
	}

	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		pgx.Identifier{customTable(ins.Feed.Config.Name)}.Sanitize(),
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	)
	_, err := tx.Exec(ctx, stmt, args...)
	return err
}

// nullTime turns a *time.Time into a value pgx encodes as NULL when nil.
func nullTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return *t
}
