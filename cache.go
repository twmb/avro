package avro

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
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
// track which schemas have already been parsed. Calls that pass options
// changing what the string compiles to — custom types or [WithLaxNames] —
// skip this deduplication and re-parse, since the schema string alone no
// longer identifies the result. Deduplication normalizes
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
		// json.Decoder.Decode stops after the first JSON value, silently
		// ignoring trailing bytes. Parse (json.Unmarshal) rejects trailing
		// non-whitespace, so only normalize when the input is a single
		// value: a second Decode returning io.EOF means the value was the
		// whole input (trailing whitespace is consumed). Anything else
		// (a syntax error on garbage, or a second value) means trailing
		// content — leave schema unchanged so parse() rejects it exactly
		// as bare Parse would, instead of silently truncating-and-accepting.
		var tail json.RawMessage
		if err2 := dec.Decode(&tail); errors.Is(err2, io.EOF) {
			if normalized, err := json.Marshal(v); err == nil {
				schema = string(normalized)
			}
		}
	}
	// Clone the cache's map so a failed parse doesn't corrupt the cache.
	cloned := maps.Clone(c.named)
	b := &builder{
		named: cloned,
	}
	applySchemaOpts(b, opts)
	hasCustomTypes := len(b.customTypes) > 0
	// WithLaxNames sets a non-default name validator (b.checkName), which
	// changes what the same schema string compiles to (a name strict Parse
	// rejects becomes accepted). The dedup key is the schema string only,
	// so lax parses must skip dedup the way custom types do — otherwise a
	// lax-then-strict call sequence returns the cached lax schema to the
	// strict caller (silently accepting an invalid name), and a
	// strict-then-lax sequence returns the strict schema ignoring the opt.
	hasLaxNames := b.checkName != nil
	skipDedup := hasCustomTypes || hasLaxNames

	// Skip dedup when custom types or lax names are in play: both produce
	// a compiled schema that the bare schema string alone doesn't identify.
	h := sha256.Sum256([]byte(schema))
	if !skipDedup {
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
	} else if !skipDedup {
		c.dedup[h] = s
	}
	return s, nil
}
