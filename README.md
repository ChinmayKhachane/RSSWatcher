# RSSWatcher

A Go service that polls RSS/Atom feeds, flattens each item into a common shape, and (optionally) extracts user-defined fields with [expr-lang](https://github.com/expr-lang/expr) expressions. Everything is persisted to Postgres.

## Quick start

```sh
# 1. Postgres reachable via DATABASE_URL
export DATABASE_URL=postgres://user:pass@localhost:5432/rsswatcher

# 2. Drop a config file at ./example.yml (see below)

# 3. Run
go build -o rsswatcher
./rsswatcher
```

`Ctrl-C` triggers a graceful shutdown — in-flight transactions are given up to 10 s to commit.

## Configuration

```yaml
feeds:
  # Feed with custom extracted fields
  - name: hn-frontpage
    url: https://news.ycombinator.com/rss
    interval: 300            # seconds
    user_agent: RSSWatcher/0.1
    schema:
      title_upper:
        type: string         # int | bool | string | float | datetime
        from: title          # title | description | content | link | categories | published
        expr: upper($)       # expr-lang; $ is the value of `from`
      published_year:
        type: int
        from: published
        expr: $.Year()
      first_category:
        type: string
        from: categories
        expr: len($) > 0 ? $[0] : ""
        unique: true         # adds UNIQUE on the column

  # Feed with no schema — only the static SimpilfiedItem fields are stored
  - name: plain-feed
    url: https://example.com/feed.xml
    interval: 600
```

A full example lives in [`example.yml`](./example.yml).

### What gets stored

For every feed:

- **`items_<name>`** — always created. One row per item with `title`, `description`, `content`, `link` (UNIQUE), `updated`, `published`, `author`, `categories`.
- **`custom_<name>`** — created only when `schema:` is present. One row per item, FK to `items_<name>(id)` with `ON DELETE CASCADE`. Columns are derived from your schema entries; `unique: true` adds a UNIQUE constraint.

Duplicate `link` values are silently skipped (`ON CONFLICT DO NOTHING`), so re-polling is safe.

## Build / check

```sh
go build ./...
go vet ./...
```

Requires Go 1.25+ and Postgres.

## Status

Work in progress. Known issues are tracked in [`CLAUDE.md`](./CLAUDE.md).
