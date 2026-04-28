package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DB wraps a pgx connection pool. The zero value is not usable — construct
// one with NewDB and release it with Close.
type DB struct {
	Pool *pgxpool.Pool
}

// NewDB opens a pgx connection pool against dsn and verifies reachability
// with a Ping. If dsn is empty, the PG* / DATABASE_URL environment variables
// are consulted by pgx in the usual way.
func NewDB(ctx context.Context, dsn string) (*DB, error) {
	const step = "database.NewDB"

	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		logError(step, err, "reason", "parse dsn")
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	logStep(step, "opening pgx pool",
		"host", cfg.ConnConfig.Host,
		"database", cfg.ConnConfig.Database,
		"max_conns", cfg.MaxConns,
	)

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		logError(step, err, "reason", "pool open")
		return nil, fmt.Errorf("pool open: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		logError(step, err, "reason", "ping")
		return nil, fmt.Errorf("ping: %w", err)
	}

	logStep(step, "pgx pool ready")
	return &DB{Pool: pool}, nil
}

// Close releases all pooled connections. Safe to call on a nil *DB.
func (db *DB) Close() {
	const step = "database.Close"
	if db == nil || db.Pool == nil {
		return
	}
	logStep(step, "closing pgx pool")
	db.Pool.Close()
}

// sqlType maps a schema field type to its Postgres column type.
func sqlType(t string) (string, error) {
	switch t {
	case "string":
		return "TEXT", nil
	case "int":
		return "BIGINT", nil
	case "float":
		return "DOUBLE PRECISION", nil
	case "bool":
		return "BOOLEAN", nil
	case "datetime":
		return "TIMESTAMPTZ", nil
	default:
		return "", fmt.Errorf("unsupported field type %q", t)
	}
}

func itemsTable(feedName string) string  { return "items_" + feedName }
func customTable(feedName string) string { return "custom_" + feedName }

// hasCustomSchema reports whether a feed has user-defined extraction fields.
// When false, the custom-table branch of each Create/Update/Drop call is
// skipped and only the static SimpilfiedItems table is managed.
func hasCustomSchema(feed *CompiledFeed) bool {
	return feed != nil && len(feed.Fields) > 0
}

// CreateTables creates the static SimpilfiedItems table for a feed, and —
// only if the feed defines a schema — the user-defined extraction table.
func (db *DB) CreateTables(ctx context.Context, feed *CompiledFeed) error {
	const step = "database.CreateTables"

	name := feed.Config.Name

	itemsStmt := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id          BIGSERIAL PRIMARY KEY,
			title       TEXT,
			description TEXT,
			content     TEXT,
			link        TEXT UNIQUE,
			updated     TIMESTAMPTZ,
			published   TIMESTAMPTZ,
			author      TEXT,
			categories  TEXT[]
		)`, pgx.Identifier{itemsTable(name)}.Sanitize())

	logStep(step, "creating items table", "feed", name)
	if _, err := db.Pool.Exec(ctx, itemsStmt); err != nil {
		return err
	}

	if !hasCustomSchema(feed) {
		logStep(step, "no schema defined, skipping custom table", "feed", name)
		return nil
	}

	cols := []string{
		"id BIGSERIAL PRIMARY KEY",
		fmt.Sprintf("item_id BIGINT NOT NULL REFERENCES %s(id) ON DELETE CASCADE",
			pgx.Identifier{itemsTable(name)}.Sanitize()),
	}
	for fname, f := range feed.Fields {
		ct, err := sqlType(f.Type)
		if err != nil {
			return fmt.Errorf("feed %s: field %s: %w", name, fname, err)
		}
		col := fmt.Sprintf("%s %s", pgx.Identifier{fname}.Sanitize(), ct)
		if feed.Config.Schema[fname].Unique {
			col += " UNIQUE"
		}
		cols = append(cols, col)
	}

	customStmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)",
		pgx.Identifier{customTable(name)}.Sanitize(),
		strings.Join(cols, ", "))

	logStep(step, "creating custom table", "feed", name, "fields", len(feed.Fields))
	_, err := db.Pool.Exec(ctx, customStmt)
	return err
}

// UpdateTables reconciles column sets. The SimpilfiedItems table is static
// and needs no migration; the custom table is reconciled only when the feed
// defines a schema.
func (db *DB) UpdateTables(ctx context.Context, feed *CompiledFeed) error {
	const step = "database.UpdateTables"

	name := feed.Config.Name

	if !hasCustomSchema(feed) {
		logStep(step, "no schema defined, nothing to update", "feed", name)
		return nil
	}

	table := pgx.Identifier{customTable(name)}.Sanitize()
	for fname, f := range feed.Fields {
		ct, err := sqlType(f.Type)
		if err != nil {
			return fmt.Errorf("feed %s: field %s: %w", name, fname, err)
		}
		stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s",
			table, pgx.Identifier{fname}.Sanitize(), ct)
		if _, err := db.Pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("feed %s: field %s: %w", name, fname, err)
		}
	}
	logStep(step, "custom table reconciled", "feed", name)
	return nil
}

// DropTables drops the SimpilfiedItems table for a feed, and — only if the
// feed defines a schema — the user-defined extraction table.
func (db *DB) DropTables(ctx context.Context, feed *CompiledFeed) error {
	const step = "database.DropTables"

	name := feed.Config.Name

	if hasCustomSchema(feed) {
		stmt := fmt.Sprintf("DROP TABLE IF EXISTS %s",
			pgx.Identifier{customTable(name)}.Sanitize())
		logStep(step, "dropping custom table", "feed", name)
		if _, err := db.Pool.Exec(ctx, stmt); err != nil {
			return err
		}
	} else {
		logStep(step, "no schema defined, skipping custom table drop", "feed", name)
	}

	stmt := fmt.Sprintf("DROP TABLE IF EXISTS %s",
		pgx.Identifier{itemsTable(name)}.Sanitize())
	logStep(step, "dropping items table", "feed", name)
	_, err := db.Pool.Exec(ctx, stmt)
	return err
}
