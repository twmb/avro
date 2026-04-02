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
	mu           sync.Mutex
	named        map[string]*namedType
	dedup        map[[32]byte]*Schema
	customParsed map[[32]byte]bool // schemas previously parsed with custom types
}

// Parse parses a schema string, registering any named types (records, enums,
// fixed) in the cache. Named types from previous Parse calls are available
// for reference resolution. On failure, the cache is not modified.
func (c *SchemaCache) Parse(schema string, opts ...SchemaOpt) (*Schema, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.named == nil {
		c.named = make(map[string]*namedType)
		c.dedup = make(map[[32]byte]*Schema)
		c.customParsed = make(map[[32]byte]bool)
	}

	dec := json.NewDecoder(strings.NewReader(schema))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err == nil {
		if normalized, err := json.Marshal(v); err == nil {
			schema = string(normalized)
		}
	}
	// Clone the cache's map so a failed parse doesn't corrupt the cache.
	cloned := maps.Clone(c.named)
	b := &builder{
		named: cloned,
	}
	applySchemaOpts(b, opts)
	hasCustomTypes := len(b.customTypes) > 0

	// Skip dedup when custom types are registered: custom types produce
	// different compiled schemas for the same schema string.
	h := sha256.Sum256([]byte(schema))
	if !hasCustomTypes {
		if s, ok := c.dedup[h]; ok {
			return s, nil
		}
	}

	// Allow re-registration of inherited names when re-parsing a schema
	// that was previously parsed with custom types (which skipped dedup),
	// or when parsing with custom types now. This preserves the
	// "duplicate named type" error for genuinely conflicting definitions.
	needsCachedNames := hasCustomTypes || c.customParsed[h]
	if needsCachedNames && len(cloned) > 0 {
		b.cachedNames = make(map[string]bool, len(cloned))
		for name := range cloned {
			b.cachedNames[name] = true
		}
	}

	s, err := parse(schema, b)
	if err != nil {
		return nil, err
	}

	// Named types are safe to cache unconditionally: applyCustomTypes
	// wraps b.ser/b.deser without mutating the node's ser/deser, so
	// cached named type nodes keep their unwrapped functions.
	c.named = b.named
	if hasCustomTypes {
		c.customParsed[h] = true
	} else {
		c.dedup[h] = s
	}
	return s, nil
}
