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
// [WithLaxNames] is sticky across the cache: if a type is defined with
// WithLaxNames, pass WithLaxNames to every later Parse that references it.
// A schema that contains a lax (non-standard) name is not parseable without
// WithLaxNames whether or not a cache produced it, so the referencing Parse's
// [Schema.String] and [Schema.Canonical] output likewise requires WithLaxNames
// to re-parse. [Schema.Encode] and [Schema.Decode] are unaffected either way.
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
		named:    cloned,
		building: make(map[*schemaNode]struct{}),
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
	// the reference in the node tree (so Encode/Decode work) but leaves a bare
	// reference in the JSON-derived forms (s.c / s.full): the inherited
	// definition lives only in the resolved node. That makes
	// Canonical()/Fingerprint()/Root()/String() non-self-contained, violating
	// the documented "independent of the cache" contract and breaking
	// cross-impl fingerprint / single-object-encoding interop. Splice the
	// inherited definitions into the ORIGINAL JSON (preserving every attribute —
	// doc, order, field props — that the node tree never stored) and rebuild the
	// metadata forms from the now self-contained JSON, parsed cache-LESSLY so the
	// inlined definition is not also seen as inherited. ser/deser/node (the
	// cache-wired encode/decode path) are untouched.
	//
	// The rebuild fires whenever inlineTreeDefs actually splices an inherited
	// definition (len(inlined) > 0). The earlier trigger was "s.full does not
	// re-parse cache-lessly" (a dangling inherited reference), but that misses a
	// reference that re-parses to the WRONG type: a bare reference to an
	// inherited type that, cache-lessly, FORWARD-binds to a same-named type
	// defined LATER in this same schema. Eager binding sent the wire reference to
	// the inherited type at its position (the later local definition cannot
	// retroactively rebind it — see NOT_BUGS.md #24), so the node tree is right;
	// but s.full re-parses fine with the reference bound to the local type, and
	// its metadata then silently describes a DIFFERENT schema than the wire codec
	// — the very resolver disagreement #24's "every resolver registers under the
	// wire builder's keys" forbids. Splicing the inherited definition in (which
	// inlineTreeDefs does by the same eager/positional rule) makes the metadata
	// faithful to the node tree. A schema that references no inherited type
	// splices nothing, so its original String() is preserved untouched.
	selfContained := s.full
	if len(cloned) > 0 {
		if tree, terr := unmarshalAnyPreservePrecision([]byte(s.full)); terr == nil {
			inlined := make(map[string]bool)
			tree = inlineTreeDefs(tree, "", c.defs, make(map[string]bool), inlined)
			if len(inlined) > 0 {
				if marshaled, merr := json.Marshal(tree); merr == nil {
					s2, rerr := Parse(string(marshaled), opts...)
					if rerr != nil {
						// The spliced form is self-contained but may carry a
						// name an inherited type was defined with under
						// WithLaxNames, which a strict re-parse rejects. Lax
						// names are sticky: a schema containing one is not
						// strict-parseable, cache or not (see the SchemaCache
						// doc). Retry permissively so the metadata forms still
						// describe the full self-contained schema instead of
						// falling back to a dangling reference; the result still
						// needs WithLaxNames to re-parse. WithLaxNames(nil) only
						// accepts the already-final names — it does not transform
						// them — so the canonical/fingerprint bytes match a
						// standalone lax parse of the same schema.
						laxOpts := make([]SchemaOpt, len(opts)+1)
						copy(laxOpts, opts)
						laxOpts[len(opts)] = WithLaxNames(nil)
						s2, rerr = Parse(string(marshaled), laxOpts...)
					}
					if rerr == nil {
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
// Keys are read case-insensitively, mirroring the parser's lookupCI.
func nodeNamespace(obj map[string]any, enclosingNS string) string {
	if v, ok := lookupCI(obj, "name"); ok {
		if name, ok := v.(string); ok && strings.Contains(name, ".") {
			return name[:strings.LastIndex(name, ".")]
		}
	}
	if v, ok := lookupCI(obj, "namespace"); ok {
		if ns, ok := v.(string); ok {
			return ns
		}
	}
	return enclosingNS
}

// nodeFullnameTree resolves a named-type object's fullname.
func nodeFullnameTree(obj map[string]any, enclosingNS string) string {
	name := ""
	if v, ok := lookupCI(obj, "name"); ok {
		name, _ = v.(string)
	}
	short := name
	if i := strings.LastIndex(name, "."); i >= 0 {
		short = name[i+1:]
	}
	if ns := nodeNamespace(obj, enclosingNS); ns != "" {
		return ns + "." + short
	}
	return short
}

// collectTreeDefs calls visit for every named-type definition in the tree, with
// its resolved fullname and sub-tree, tracking namespace scope. It mirrors the
// parser EXACTLY: object keys are read case-insensitively (lookupCI), and a
// flat-form ("linkedin/goavro") field whose own keys define a named type is
// itself a definition (the parser lifts it via liftFlatFieldType).
func collectTreeDefs(node any, ns string, visit func(fullname string, def any)) {
	switch v := node.(type) {
	case []any:
		for _, b := range v {
			collectTreeDefs(b, ns, visit)
		}
	case map[string]any:
		typVal, hasType := lookupCI(v, "type")
		typ, _ := typVal.(string)
		name := ""
		if nm, ok := lookupCI(v, "name"); ok {
			name, _ = nm.(string)
		}
		childNS := ns
		if name != "" && isNamedKind(typ) {
			childNS = nodeNamespace(v, ns)
			visit(nodeFullnameTree(v, ns), v)
		}
		if hasType {
			collectTreeDefs(typVal, ns, visit)
		}
		if fs, ok := lookupCI(v, "fields"); ok {
			if fsa, ok := fs.([]any); ok {
				for _, f := range fsa {
					fo, ok := f.(map[string]any)
					if !ok {
						continue
					}
					if lifted, ok := flatFieldNamedDef(fo); ok {
						collectTreeDefs(lifted, childNS, visit)
						continue
					}
					if t, ok := lookupCI(fo, "type"); ok {
						collectTreeDefs(t, childNS, visit)
					}
				}
			}
		}
		if it, ok := lookupCI(v, "items"); ok {
			collectTreeDefs(it, childNS, visit)
		}
		if vv, ok := lookupCI(v, "values"); ok {
			collectTreeDefs(vv, childNS, visit)
		}
	}
}

// flatFieldNamedDef reports whether fo is a flat-form ("linkedin/goavro")
// field that itself defines a NAMED type — its "type" names a named kind
// (record/error/enum/fixed) and that kind's defining key (fields/symbols/
// size) sits inline — and if so returns the lifted type object with field-
// only keys dropped, mirroring the parser's liftFlatFieldType. Only named
// kinds are referenceable across parses; flat array/map fields define no name.
func flatFieldNamedDef(fo map[string]any) (map[string]any, bool) {
	tv, ok := lookupCI(fo, "type")
	if !ok {
		return nil, false
	}
	ts, ok := tv.(string)
	if !ok || !isNamedKind(ts) {
		return nil, false
	}
	var defKey string
	switch ts {
	case "record", "error":
		defKey = "fields"
	case "enum":
		defKey = "symbols"
	case "fixed":
		defKey = "size"
	}
	if _, ok := lookupCI(fo, defKey); !ok {
		return nil, false
	}
	lifted := make(map[string]any, len(fo))
	for k, val := range fo {
		switch {
		case strings.EqualFold(k, "default"), strings.EqualFold(k, "order"), strings.EqualFold(k, "aliases"):
			// Field-only keys, not part of the type definition.
		default:
			lifted[k] = val
		}
	}
	return lifted, true
}

// ciKey returns the key actually present in m matching key case-insensitively
// (exact match preferred, else the lexicographically smallest case-insensitive
// match — same selection as lookupCI). A mutating walker uses it to write back
// to the present key instead of introducing a duplicate canonical-cased key.
func ciKey(m map[string]any, key string) (string, bool) {
	if _, ok := m[key]; ok {
		return key, true
	}
	pick, found := "", false
	for k := range m {
		if strings.EqualFold(k, key) && (!found || k < pick) {
			pick, found = k, true
		}
	}
	return pick, found
}

// inlineTreeDefs replaces the FIRST occurrence of each reference to a cache-
// inherited named type (in defs, not defined locally before the reference, not
// already inlined) with its definition, recursing into the inlined copy so
// transitive references resolve too. Subsequent occurrences stay bare. The def
// is deep-copied before any mutation so the cache is not corrupted.
//
// Reference binding mirrors the parser EXACTLY (eager, in-scope-first, and
// POSITIONAL): a bare reference resolves in scopedRefKeys precedence
// (enclosing-namespace-qualified first, then the null-namespace bare name),
// and at each candidate a LOCAL definition already registered AT THIS POINT IN
// THE WALK wins over a cache-inherited one. seen accumulates local fullnames in
// DFS pre-order as the parser registers them (a named type's name is in scope
// from the start of its own definition onward, for self-reference), so:
//   - a ref AFTER a local def of the same name keeps the ref bare (the parser
//     bound it to the local type, which is present inline); and
//   - a ref BEFORE the local def binds to the cached type (the local name was
//     not yet in scope at the reference), so the cached def is spliced —
//     matching the eager wire binding.
//
// Consulting a position-independent local set instead would wrongly keep a
// before-the-def reference bare and diverge String()/Canonical()/Fingerprint()/
// Root() from the wire codec the parser built.
//
// The walk also dedupes DEFINITIONS: spliced defs are stored self-contained,
// so two inherited refs whose definitions share a transitive type (or a
// nested type referenced before its container) would otherwise both carry a
// definition of the shared name — a duplicate the rebuild Parse rejects. A
// definition of a name already in seen is rewritten to a reference to the
// first definition (dupDefRef; flat-form fields via rewriteFlatFieldToRef),
// mirroring the parser's single resolved type per name.
func inlineTreeDefs(node any, ns string, defs map[string]any, seen, inlined map[string]bool) any {
	switch v := node.(type) {
	case string:
		if !avroNamedRef(v) {
			return v
		}
		var keys [2]string
		for _, key := range scopedRefKeys(&keys, v, ns) {
			if seen[key] {
				return v // bound to a local def already in scope; keep it bare
			}
			if def, ok := defs[key]; ok {
				if inlined[key] {
					return v
				}
				inlined[key] = true
				return inlineTreeDefs(deepCopyTree(def), ns, defs, seen, inlined)
			}
		}
		return v
	case []any:
		for i := range v {
			v[i] = inlineTreeDefs(v[i], ns, defs, seen, inlined)
		}
		return v
	case map[string]any:
		childNS := nodeNamespace(v, ns)
		// Register this node's own name BEFORE walking its children, mirroring
		// the parser's early self-registration (a type is in scope for its own
		// descendants). A later sibling/descendant reference then sees it; an
		// earlier one already resolved against the cache above. Keys are read
		// case-insensitively, mirroring collectTreeDefs and the parser.
		//
		// If the name is ALREADY defined at this point in the walk, this node
		// is a SECOND definition arriving inside another spliced subtree: two
		// inherited refs whose self-contained definitions share a transitive
		// type (the diamond A→{B,C}→D), or a nested type referenced before
		// the container whose definition carries it. The parser's node tree
		// shares ONE resolved type per name, so the self-contained JSON must
		// keep the first definition and reference it thereafter — the same
		// first-define-then-reference rule Java's Schema toString applies via
		// NamedSchema.writeNameRef. Without the rewrite the rebuilt JSON
		// defines the name twice, the rebuild Parse rejects it, and the
		// metadata forms silently fall back to the dangling original.
		if typVal, ok := lookupCI(v, "type"); ok {
			if typ, _ := typVal.(string); isNamedKind(typ) {
				if nameVal, ok := lookupCI(v, "name"); ok {
					if name, _ := nameVal.(string); name != "" {
						fullname := nodeFullnameTree(v, ns)
						if ref, ok := dupDefRef(fullname, ns, seen); ok {
							return ref
						}
						seen[fullname] = true
					}
				}
			}
		}
		// A node's own "type" value sits at the enclosing namespace; its
		// fields/items/values sit inside the node's own namespace scope.
		if tk, ok := ciKey(v, "type"); ok {
			v[tk] = inlineTreeDefs(v[tk], ns, defs, seen, inlined)
		}
		inlineNodeContainers(v, childNS, defs, seen, inlined)
		return v
	}
	return node
}

// inlineNodeContainers splices inherited refs in a node's record-fields,
// array-items, and map-values positions (all inside the node's own namespace
// scope). A flat-form ("linkedin/goavro") field defines its named type via the
// field's own structural keys, so its inline subtree is recursed directly
// rather than through a "type" value. Keys are read case-insensitively and
// written back to the present key, mirroring the parser and collectTreeDefs.
func inlineNodeContainers(v map[string]any, ns string, defs map[string]any, seen, inlined map[string]bool) {
	if fk, ok := ciKey(v, "fields"); ok {
		if fs, ok := v[fk].([]any); ok {
			for _, f := range fs {
				fo, ok := f.(map[string]any)
				if !ok {
					continue
				}
				if _, isFlat := flatFieldNamedDef(fo); isFlat {
					// The flat field is itself a named type opening its own
					// namespace; recurse its structural keys for transitive
					// refs (its "type" is a bare kind string, not a reference).
					// A flat definition of an already-defined name is the same
					// duplicate-definition case as inlineTreeDefs's map arm,
					// but a field object cannot be replaced by a bare string —
					// rewrite it to the equivalent normal-form reference field.
					if nameVal, ok := lookupCI(fo, "name"); ok {
						if name, _ := nameVal.(string); name != "" {
							fullname := nodeFullnameTree(fo, ns)
							if ref, ok := dupDefRef(fullname, ns, seen); ok {
								rewriteFlatFieldToRef(fo, ref)
								continue
							}
							seen[fullname] = true
						}
					}
					inlineNodeContainers(fo, nodeNamespace(fo, ns), defs, seen, inlined)
					continue
				}
				if ftk, ok := ciKey(fo, "type"); ok {
					fo[ftk] = inlineTreeDefs(fo[ftk], ns, defs, seen, inlined)
				}
			}
		}
	}
	if ik, ok := ciKey(v, "items"); ok {
		v[ik] = inlineTreeDefs(v[ik], ns, defs, seen, inlined)
	}
	if vk, ok := ciKey(v, "values"); ok {
		v[vk] = inlineTreeDefs(v[vk], ns, defs, seen, inlined)
	}
}

// dupDefRef decides how a SECOND definition of fullname, encountered at a
// position whose enclosing namespace is ns, is replaced by a reference to
// the first definition. Returns the reference spelling and true when one is
// expressible; ("", false) keeps the duplicate definition in place (the
// rebuild Parse then fails and the metadata forms fall back to the dangling
// original — degraded, but never describing a different schema than the wire
// codec).
//
// A dotted fullname is an exact lookup on re-parse (scopedRefKeys), so it is
// always a safe spelling. A null-namespace name has only its bare short name
// as a spelling, which the parser binds enclosing-namespace-first — safe
// unless a same-short-name type qualified by the enclosing namespace is
// already defined at this point in the walk. Binding is eager and positional,
// so only an EARLIER definition can capture the reference; one appearing
// later cannot. Avro has no absolute-reference syntax that could express the
// shadowed case (Java's toString shares the limitation), so it stays a
// definition.
func dupDefRef(fullname, ns string, seen map[string]bool) (string, bool) {
	if !seen[fullname] {
		return "", false
	}
	if strings.Contains(fullname, ".") {
		return fullname, true
	}
	if ns == "" || !seen[ns+"."+fullname] {
		return fullname, true
	}
	return "", false
}

// rewriteFlatFieldToRef converts a flat-form field that re-defines an
// already-defined named type into the equivalent normal-form field whose
// "type" is a name reference. The field keeps exactly the keys the parser
// treats as field-only when lifting a flat definition (liftFlatFieldType:
// name, default, order, aliases); every other key — the structural keys,
// namespace, doc, logicalType, custom props — belongs to the TYPE and is
// carried by its first definition.
func rewriteFlatFieldToRef(fo map[string]any, ref string) {
	for k := range fo {
		switch {
		case strings.EqualFold(k, "name"),
			strings.EqualFold(k, "default"),
			strings.EqualFold(k, "order"),
			strings.EqualFold(k, "aliases"),
			strings.EqualFold(k, "type"):
		default:
			delete(fo, k)
		}
	}
	tk, _ := ciKey(fo, "type")
	fo[tk] = ref
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
