package avro

import (
	"crypto/sha256"
	"maps"
	"sync"
)

// SchemaCache accumulates named types across multiple [SchemaCache.Parse]
// calls, allowing schemas to reference types defined in previously parsed
// schemas. This is useful for Schema Registry integrations where schemas
// have references to other schemas.
//
// Schemas must be parsed in dependency order: referenced types must be
// parsed before the schemas that reference them.
//
// Parsing the same schema string multiple times is allowed and returns the
// previously parsed result. This handles diamond dependencies in schema
// reference graphs (e.g. A→B→D, A→C→D) without requiring callers to
// track which schemas have already been parsed. Deduplication compares the
// raw schema string, not the canonical form: two schema strings that parse
// identically but differ textually (e.g. in whitespace or doc fields) are
// not deduplicated and the second parse will return a duplicate type error.
//
// The returned [*Schema] from each Parse call is fully resolved and
// independent of the cache — it can be used for [Schema.Encode] and
// [Schema.Decode] without the cache.
//
// A SchemaCache is safe for concurrent use.
type SchemaCache struct {
	mu    sync.Mutex
	named map[string]*namedType
	dedup map[[32]byte]*Schema
}

// NewSchemaCache returns a new empty SchemaCache.
func NewSchemaCache() *SchemaCache {
	return &SchemaCache{
		named: make(map[string]*namedType),
		dedup: make(map[[32]byte]*Schema),
	}
}

// Parse parses a schema string, registering any named types (records, enums,
// fixed) in the cache. Named types from previous Parse calls are available
// for reference resolution. On failure, the cache is not modified.
func (c *SchemaCache) Parse(schema string, opts ...ParseOpt) (*Schema, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	h := sha256.Sum256([]byte(schema))
	if s, ok := c.dedup[h]; ok {
		return s, nil
	}

	// Clone the cache's map so a failed parse doesn't corrupt the cache.
	b := &builder{
		named: maps.Clone(c.named),
	}
	applyParseOpts(b, opts)

	s, err := parse(schema, b)
	if err != nil {
		return nil, err
	}

	// Success: the builder's map is a superset of the cache's.
	c.named = b.named
	c.dedup[h] = s
	return s, nil
}
