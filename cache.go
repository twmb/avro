package avro

import (
	"crypto/sha256"
	"encoding/json"
	"maps"
	"strings"
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
// track which schemas have already been parsed. Deduplication normalizes
// the JSON (whitespace and key order) but not the Avro canonical form:
// schemas that differ only in formatting are deduplicated, but differences
// in non-canonical fields like doc or aliases are not and will return a
// duplicate type error.
//
// The returned [*Schema] from each Parse call is fully resolved and
// independent of the cache — it can be used for [Schema.Encode] and
// [Schema.Decode] without the cache.
//
// The zero value is ready to use. A SchemaCache is safe for concurrent use.
type SchemaCache struct {
	mu    sync.Mutex
	named map[string]*namedType
	dedup map[[32]byte]*Schema
}

// Parse parses a schema string, registering any named types (records, enums,
// fixed) in the cache. Named types from previous Parse calls are available
// for reference resolution. On failure, the cache is not modified.
func (c *SchemaCache) Parse(schema string, opts ...ParseOpt) (*Schema, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.named == nil {
		c.named = make(map[string]*namedType)
		c.dedup = make(map[[32]byte]*Schema)
	}

	dec := json.NewDecoder(strings.NewReader(schema))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err == nil {
		if normalized, err := json.Marshal(v); err == nil {
			schema = string(normalized)
		}
	}
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
