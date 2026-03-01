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
	mu      sync.Mutex
	types   map[string]serfn
	dtypes  map[string]deserfn
	stypes  map[string]*serRecord
	drtypes map[string]*deserRecord
	ntypes  map[string]*schemaNode
}

// NewSchemaCache returns a new empty SchemaCache.
func NewSchemaCache() *SchemaCache {
	return &SchemaCache{
		types:   make(map[string]serfn),
		dtypes:  make(map[string]deserfn),
		stypes:  make(map[string]*serRecord),
		drtypes: make(map[string]*deserRecord),
		ntypes:  make(map[string]*schemaNode),
	}
}

// Parse parses a schema string, registering any named types (records, enums,
// fixed) in the cache. Named types from previous Parse calls are available
// for reference resolution. On failure, the cache is not modified.
func (c *SchemaCache) Parse(schema string) (*Schema, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Clone the cache's maps so a failed parse doesn't corrupt the cache.
	b := &builder{
		types:   maps.Clone(c.types),
		dtypes:  maps.Clone(c.dtypes),
		stypes:  maps.Clone(c.stypes),
		drtypes: maps.Clone(c.drtypes),
		ntypes:  maps.Clone(c.ntypes),
	}

	s, err := parse(schema, b)
	if err != nil {
		return nil, err
	}

	// Success: the builder's maps are supersets of the cache's.
	c.types = b.types
	c.dtypes = b.dtypes
	c.stypes = b.stypes
	c.drtypes = b.drtypes
	c.ntypes = b.ntypes
	return s, nil
}
