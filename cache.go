package avro

import (
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
// The returned [*Schema] from each Parse call is fully resolved and
// independent of the cache — it can be used for [Schema.Encode] and
// [Schema.Decode] without the cache.
//
// A SchemaCache is safe for concurrent use.
type SchemaCache struct {
	mu    sync.Mutex
	named map[string]*namedType
}

// NewSchemaCache returns a new empty SchemaCache.
func NewSchemaCache() *SchemaCache {
	return &SchemaCache{
		named: make(map[string]*namedType),
	}
}

// Parse parses a schema string, registering any named types (records, enums,
// fixed) in the cache. Named types from previous Parse calls are available
// for reference resolution. On failure, the cache is not modified.
func (c *SchemaCache) Parse(schema string) (*Schema, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Clone the cache's map so a failed parse doesn't corrupt the cache.
	b := &builder{
		named: maps.Clone(c.named),
	}

	s, err := parse(schema, b)
	if err != nil {
		return nil, err
	}

	// Success: the builder's map is a superset of the cache's.
	c.named = b.named
	return s, nil
}
