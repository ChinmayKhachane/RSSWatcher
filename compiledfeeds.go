package main

import (
	"github.com/expr-lang/expr/vm"
)

// CompiledField is one schema field after its expr-lang expression has
// been compiled. From and Type mirror the YAML config; Program is the
// compiled program ready to be evaluated against an item.
type CompiledField struct {
	From    string
	Type    string
	Program *vm.Program
}

// CompiledFeed is the runtime view of a feed: the original FeedConfig plus
// a map of field name → compiled program. A feed without a user-defined
// schema produces no CompiledFeed (see CompileFeeds), so a non-nil
// CompiledFeed always has at least one field.
type CompiledFeed struct {
	Config *FeedConfig
	Fields map[string]CompiledField
}
