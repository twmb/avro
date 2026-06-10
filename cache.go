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
	laxParsed    map[[32]byte]bool // schemas previously parsed with WithLaxNames
	// defs holds each registered named type's self-contained JSON definition
	// (with all attributes — doc, order, props), keyed by fullname. A later
	// Parse that references a type defined in a prior Parse splices the
	// definition back in at the first dangling reference, so the returned
	// schema's JSON-derived forms (Canonical/Fingerprint/Root/String) are
	// self-contained and preserve every original attribute. See inlineTreeDefs.
	defs map[string]any
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
		c.laxParsed = make(map[[32]byte]bool)
		c.defs = make(map[string]any)
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

	// cachedNames marks every name INHERITED from the cache (cross-parse), so
	// rejectCachedRefIfCustomTypeWouldMatch can tell an inherited reference from
	// a name defined in THIS parse (a self-/forward reference), and the
	// duplicate-name check can do the same. Populated for EVERY parse with
	// inherited names — the cross-parse custom-boundary guard must fire even for
	// a plain (no-CustomType) parse that references a custom-built cached type.
	if len(cloned) > 0 {
		b.cachedNames = make(map[string]bool, len(cloned))
		for name := range cloned {
			b.cachedNames[name] = true
		}
	}
	// allowReRegister lets a parse re-DEFINE an inherited name (vs the
	// "duplicate named type" error), granted ONLY when the cache has
	// ALREADY seen this exact schema string (in dedup for strict,
	// customParsed for custom, laxParsed for lax). Such a re-parse
	// re-enters the builder — strict and custom because the dedup-return
	// is skipped, lax because it skips dedup entirely — and re-registers
	// the names IT ITSELF defined; that is the same string, not a
	// conflict. A parse of a NEW string re-defining an inherited name
	// still errors, preserving conflict detection — including under
	// custom mode (a custom parse that REFERENCES a cached name doesn't
	// re-define it, so it never needs this; only a redefinition does, and
	// a conflicting redefinition must error regardless of custom types).
	b.allowReRegister = c.dedup[h] != nil || c.customParsed[h] || c.laxParsed[h]

	s, err := parse(schema, b)
	if err != nil {
		return nil, err
	}

	// A parse that REFERENCES a type defined in a PRIOR cache Parse resolves
	// the reference in the node tree (so Encode/Decode work) but leaves a
	// dangling bare reference in the JSON-derived forms (s.c / s.full): the
	// inherited definition lives only in the resolved node. That makes
	// Canonical()/Fingerprint()/Root()/String() non-self-contained, violating
	// the documented "independent of the cache" contract and breaking
	// cross-impl fingerprint / single-object-encoding interop. Detect it by
	// re-parsing s.full cache-lessly; on failure (a dangling inherited ref),
	// splice the inherited definitions into the ORIGINAL JSON (preserving every
	// attribute — doc, order, field props — that the node tree never stored)
	// and rebuild the metadata forms from the now self-contained JSON, parsed
	// cache-LESSLY so the inlined definition is not also seen as inherited.
	// ser/deser/node (the cache-wired encode/decode path) are untouched.
	selfContained := s.full
	if len(cloned) > 0 {
		if _, perr := Parse(s.full, opts...); perr != nil {
			if tree, terr := unmarshalAnyPreservePrecision([]byte(s.full)); terr == nil {
				local := make(map[string]bool)
				collectTreeDefs(tree, "", func(fn string, _ any) { local[fn] = true })
				tree = inlineTreeDefs(tree, "", c.defs, local, make(map[string]bool))
				if marshaled, merr := json.Marshal(tree); merr == nil {
					if s2, rerr := Parse(string(marshaled), opts...); rerr == nil {
						selfContained = s2.full
						s.c = s2.c
						s.full = s2.full
						s.soe = s2.soe
					}
				}
			}
		}
	}

	// Record this parse's named-type definitions (from the self-contained form,
	// so each captured definition has its own transitive references spliced in)
	// for future cross-parse references.
	if tree, terr := unmarshalAnyPreservePrecision([]byte(selfContained)); terr == nil {
		collectTreeDefs(tree, "", func(fn string, def any) {
			if _, ok := c.defs[fn]; !ok {
				// Store with the namespace made explicit so the definition
				// resolves to the same fullname wherever it is later spliced,
				// regardless of the enclosing namespace at the reference site.
				c.defs[fn] = defWithExplicitNamespace(def, fn)
			}
		})
	}

	// Named types are safe to cache unconditionally: applyCustomTypes
	// wraps b.ser/b.deser without mutating the node's ser/deser, so
	// cached named type nodes keep their unwrapped functions.
	c.named = b.named
	switch {
	case hasCustomTypes:
		c.customParsed[h] = true
	case hasLaxNames:
		c.laxParsed[h] = true
	default:
		c.dedup[h] = s
	}
	return s, nil
}

// --- cross-parse self-containment (SchemaCache.defs) ---
//
// A SchemaCache schema may reference a named type defined in a prior Parse.
// The reference resolves in the node tree (Encode/Decode work), but the JSON
// forms keep a dangling bare reference. To make Canonical/Fingerprint/Root/
// String self-contained while preserving every original attribute, each
// inherited definition is spliced back into the schema JSON at its first
// reference before parsing. The helpers below operate on the generic JSON tree
// (a lossless round-trip via unmarshalAnyPreservePrecision + json.Marshal),
// tracking Avro namespace scope so references resolve to the right fullname.

// avroNamedRef reports whether a type string names a type (a reference), as
// opposed to a primitive or a container/definition keyword.
func avroNamedRef(typ string) bool {
	switch typ {
	case "null", "boolean", "int", "long", "float", "double", "bytes", "string",
		"record", "enum", "array", "map", "union", "fixed", "error":
		return false
	}
	return typ != ""
}

// nodeNamespace returns the namespace in scope inside a named-type object: the
// prefix of a dotted name, else its "namespace" attribute, else inherited.
func nodeNamespace(obj map[string]any, enclosingNS string) string {
	if name, ok := obj["name"].(string); ok && strings.Contains(name, ".") {
		return name[:strings.LastIndex(name, ".")]
	}
	if ns, ok := obj["namespace"].(string); ok {
		return ns
	}
	return enclosingNS
}

// nodeFullnameTree resolves a named-type object's fullname.
func nodeFullnameTree(obj map[string]any, enclosingNS string) string {
	name, _ := obj["name"].(string)
	short := name
	if i := strings.LastIndex(name, "."); i >= 0 {
		short = name[i+1:]
	}
	if ns := nodeNamespace(obj, enclosingNS); ns != "" {
		return ns + "." + short
	}
	return short
}

// resolveRef resolves a (possibly short) reference against an enclosing
// namespace into a fullname.
func resolveRef(ref, enclosingNS string) string {
	if strings.Contains(ref, ".") || enclosingNS == "" {
		return ref
	}
	return enclosingNS + "." + ref
}

// collectTreeDefs calls visit for every named-type definition in the tree, with
// its resolved fullname and sub-tree, tracking namespace scope.
func collectTreeDefs(node any, ns string, visit func(fullname string, def any)) {
	switch v := node.(type) {
	case []any:
		for _, b := range v {
			collectTreeDefs(b, ns, visit)
		}
	case map[string]any:
		typ, _ := v["type"].(string)
		name, _ := v["name"].(string)
		childNS := ns
		if name != "" && (typ == "record" || typ == "error" || typ == "enum" || typ == "fixed") {
			childNS = nodeNamespace(v, ns)
			visit(nodeFullnameTree(v, ns), v)
		}
		if t, ok := v["type"]; ok {
			collectTreeDefs(t, ns, visit)
		}
		if fs, ok := v["fields"].([]any); ok {
			for _, f := range fs {
				if fo, ok := f.(map[string]any); ok {
					collectTreeDefs(fo["type"], childNS, visit)
				}
			}
		}
		if it, ok := v["items"]; ok {
			collectTreeDefs(it, childNS, visit)
		}
		if vv, ok := v["values"]; ok {
			collectTreeDefs(vv, childNS, visit)
		}
	}
}

// inlineTreeDefs replaces the FIRST occurrence of each reference to a cache-
// inherited named type (in defs, not defined locally, not already inlined)
// with its definition, recursing into the inlined copy so transitive
// references resolve too. Subsequent occurrences stay bare. The def is deep-
// copied before any mutation so the cache is not corrupted.
func inlineTreeDefs(node any, ns string, defs map[string]any, local, inlined map[string]bool) any {
	switch v := node.(type) {
	case string:
		if !avroNamedRef(v) {
			return v
		}
		key := resolveRef(v, ns)
		def, ok := defs[key]
		if !ok {
			if def, ok = defs[v]; ok {
				key = v
			}
		}
		if !ok || local[key] || inlined[key] {
			return v
		}
		inlined[key] = true
		return inlineTreeDefs(deepCopyTree(def), ns, defs, local, inlined)
	case []any:
		for i := range v {
			v[i] = inlineTreeDefs(v[i], ns, defs, local, inlined)
		}
		return v
	case map[string]any:
		childNS := nodeNamespace(v, ns)
		if t, ok := v["type"]; ok {
			v["type"] = inlineTreeDefs(t, ns, defs, local, inlined)
		}
		if fs, ok := v["fields"].([]any); ok {
			for _, f := range fs {
				if fo, ok := f.(map[string]any); ok {
					if t, ok := fo["type"]; ok {
						fo["type"] = inlineTreeDefs(t, childNS, defs, local, inlined)
					}
				}
			}
		}
		if it, ok := v["items"]; ok {
			v["items"] = inlineTreeDefs(it, childNS, defs, local, inlined)
		}
		if vv, ok := v["values"]; ok {
			v["values"] = inlineTreeDefs(vv, childNS, defs, local, inlined)
		}
		return v
	}
	return node
}

// defWithExplicitNamespace deep-copies a named-type definition and makes its
// namespace explicit from the resolved fullname: "name" is set to the
// unqualified short name and "namespace" to the resolved namespace — "" for the
// null namespace, which forces null even inside a namespaced splice site (the
// documented "namespace":"" escape). Without this, a definition that relied on
// an inherited namespace (no explicit "namespace", short "name") would
// re-inherit the enclosing namespace of whatever schema it is later spliced
// into and resolve to the WRONG fullname (e.g. a.b.Inner becoming c.d.Inner).
// Only the top level is rewritten: nested named types resolve relative to this
// now-explicit namespace (inherited children) or keep their own explicit
// namespace, so both inheritance and explicit-override survive the splice.
func defWithExplicitNamespace(def any, fullname string) any {
	cp := deepCopyTree(def)
	if obj, ok := cp.(map[string]any); ok {
		obj["name"] = unqualified(fullname)
		obj["namespace"] = namespaceOf(fullname)
	}
	return cp
}

// deepCopyTree deep-copies a generic JSON tree.
func deepCopyTree(node any) any {
	switch v := node.(type) {
	case map[string]any:
		m := make(map[string]any, len(v))
		for k, val := range v {
			m[k] = deepCopyTree(val)
		}
		return m
	case []any:
		s := make([]any, len(v))
		for i, e := range v {
			s[i] = deepCopyTree(e)
		}
		return s
	default:
		return v
	}
}
