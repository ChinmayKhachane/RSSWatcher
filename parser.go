package main

import (
	"fmt"
	"os"
	"time"

	"github.com/expr-lang/expr"
	"gopkg.in/yaml.v3"
)

// ValidSource and DataType are reserved aliases for future strong-typing
// of the source/type maps; today the underlying maps still key on string.
type ValidSource string
type DataType string

// Field is one user-declared extraction in a feed's schema. The expression
// is evaluated with $ bound to the value of From on each item; the result
// is coerced to Type and persisted into the custom_<feed> table.
type Field struct {
	Type   string `yaml:"type"`
	Expr   string `yaml:"expr"`
	From   string `yaml:"from"`
	Index  bool   `yaml:"index"`
	Unique bool   `yaml:"unique"`
}

// Config is the top-level YAML document loaded by LoadConfig.
type Config struct {
	Feeds []*FeedConfig `yaml:"feeds"`
}

// FeedConfig is one feed definition. Schema may be nil/empty, in which
// case the feed flows through to the static items_<feed> table only and
// no custom table is created (see database.CreateTables, parser.CompileFeeds).
type FeedConfig struct {
	Name       string           `yaml:"name"`
	Url        string           `yaml:"url"`
	Interval   int              `yaml:"interval"`
	User_agent string           `yaml:"user_agent,omitempty"`
	authconfig AuthConfig       `yaml:"auth_config,omitempty"`
	Schema     map[string]Field `yaml:"schema,omitempty"`
}

// AuthConfig holds optional HTTP basic-auth credentials for a feed.
// Note: fields are unexported, so yaml.Unmarshal cannot populate them
// today — exporting Username/Password is required to wire auth in.
type AuthConfig struct {
	username string `yaml:"username,omitempty"`
	password string `yaml:"password,omitempty"`
}

// validSources is the closed set of gofeed.Item fields a schema may pull
// from. Extending this map requires a matching case in CompileFeeds and
// in ItemsForProc.Extract.
var validSources = map[string]bool{
	"title":       true,
	"link":        true,
	"summary":     true,
	"content":     true,
	"published":   true,
	"categories":  true,
	"description": true,
}

// validTypes is the closed set of declared field types. Extending this
// map requires a matching case in database.sqlType.
var validTypes = map[string]bool{
	"int":      true,
	"bool":     true,
	"string":   true,
	"datetime": true,
	"float":    true,
}

// LoadConfig reads and parses the YAML config at path. Does not validate
// the schema — call Validate afterwards.
func LoadConfig(path string) (*Config, error) {
	const step = "parser.LoadConfig"

	logStep(step, "reading config", "path", path)
	file, err := os.ReadFile(path)
	if err != nil {
		logError(step, err, "path", path)
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(file, &cfg); err != nil {
		logError(step, err, "path", path, "reason", "yaml unmarshal")
		return nil, err
	}

	logStep(step, "config loaded", "feeds", len(cfg.Feeds))
	return &cfg, nil
}

// Validate checks every schema field against the validSources / validTypes
// allowlists. The first violation aborts validation and returns a
// human-readable error.
func (cfg *Config) Validate() error {
	const step = "parser.Validate"

	logDebug(step, "validating config", "feeds", len(cfg.Feeds))
	for _, feed := range cfg.Feeds {
		for name, field := range feed.Schema {
			if !validSources[field.From] {
				err := fmt.Errorf("feed %s: field %s: invalid source %q",
					feed.Url, name, field.From)
				logError(step, err)
				return err
			}
			if !validTypes[field.Type] {
				err := fmt.Errorf("feed %s: field %s: invalid type %q",
					feed.Url, name, field.Type)
				logError(step, err)
				return err
			}
		}
	}
	logStep(step, "config valid")
	return nil
}

// CompileFeeds compiles every schema field's expr-lang expression once,
// up-front. Feeds with an empty schema are skipped — no CompiledFeed is
// returned for them, which is the signal downstream that no custom table
// or extraction work is needed.
//
// The expr environment binds $ to a typed zero value matching the field's
// From, so the compiler can type-check the expression body.
func CompileFeeds(cfg *Config) ([]CompiledFeed, error) {
	const step = "parser.CompileFeeds"

	var feeds []CompiledFeed
	skipped := 0

	for _, fc := range cfg.Feeds {
		if len(fc.Schema) == 0 {
			logDebug(step, "feed has no schema, skipping compile", "feed", fc.Name)
			skipped++
			continue
		}

		compiled := CompiledFeed{
			Config: fc,
			Fields: make(map[string]CompiledField, len(fc.Schema)),
		}

		for name, field := range fc.Schema {
			var typehint any
			switch field.From {
			case "title", "description", "content", "link":
				typehint = ""
			case "categories":
				typehint = []string{}
			case "published", "updated":
				typehint = &time.Time{}
			}
			program, err := expr.Compile(field.Expr, expr.Env(map[string]any{
				"$": typehint,
			}))
			if err != nil {
				logError(step, err, "feed", fc.Name, "field", name, "expr", field.Expr)
				return nil, fmt.Errorf("feed %s: field %s: %w", fc.Url, name, err)
			}

			compiled.Fields[name] = CompiledField{
				From:    field.From,
				Type:    field.Type,
				Program: program,
			}
		}

		feeds = append(feeds, compiled)
		logStep(step, "feed compiled", "feed", fc.Name, "fields", len(compiled.Fields))
	}

	logStep(step, "compile done", "compiled", len(feeds), "skipped_no_schema", skipped)
	return feeds, nil
}
