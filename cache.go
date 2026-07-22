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
		named:      cloned,
		building:   make(map[*schemaNode]struct{}),
		definedSet: make(map[*namedType]bool),
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

	// cachedNames marks every name INHERITED from the cache (cross-parse).
	// cachedNames alone CANNOT tell an inherited REFERENCE from a name THIS
	// parse RE-DEFINES (a re-registered name is in both): so
	// rejectCachedRefIfCustomTypeWouldMatch keys its "defined this parse" skip on
	// definedSet membership of the resolved *namedType, and uses cachedNames only
	// to recognize a genuine cross-parse reference (in cachedNames, but its nt is
	// the cloned cached node, absent from definedSet). The duplicate-name check
	// pairs cachedNames with allowReRegister (string-keyed) to permit a
	// same-string re-parse. Populated for EVERY parse with inherited names — the
	// cross-parse custom-boundary guard must fire even for a plain (no-CustomType)
	// parse that references a custom-built cached type.
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
						// WithLaxNames, which a re-parse under this call's own
						// opts rejects. Lax names are sticky: a schema
						// containing one is not strict-parseable, cache or not
						// (see the SchemaCache doc). Retry with the internal
						// accept-everything validator (appended last, so it
						// wins over any user lax fn — the retry only ever
						// broadens) so the metadata forms still describe the
						// full self-contained schema instead of falling back
						// to a dangling reference; the result still needs
						// WithLaxNames to re-parse. See internalReparseNames
						// for why accept-all is sound here: every spliced name
						// was validated by the parse that defined it, and
						// names pass through verbatim, so the canonical/
						// fingerprint bytes match a standalone lax parse of
						// the same schema.
						laxOpts := make([]SchemaOpt, len(opts)+1)
						copy(laxOpts, opts)
						laxOpts[len(opts)] = internalReparseNames
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

// collectTreeDefs calls visit for every named-type definition in the tree,
// with its resolved fullname and sub-tree, tracking namespace scope. Child
// positions, key casing, and the flat-form ("linkedin/goavro") field lift
// come from walkNodeChildren, so a named type defined by a flat field
// (record/error/enum/fixed) or inside the field's items/values (array/map)
// is collected exactly where the parser registers it. The node's own
// "type" value is not walked: on a parser-accepted tree it is always a
// string (aobjectFromMap rejects any other JSON type there), and a string
// defines nothing to collect.
//
// The visit fires for every named KIND, "name" key or not, and the
// child scope is nodeChildScope: the parser resolves and registers a
// fullname even when the name key is absent entirely (an empty short
// name a WithLaxNames fn accepted — fullname "ns.", or "" with no
// namespace in scope), and scopes the children by its namespace
// attribute regardless, so a keyless definition is collected under the
// parser's fullname and its nested definitions under the parser's
// scope. Gating either on name-key presence misfiled nested defs under
// ENCLOSING-scoped fullnames: a cross-parse reference to the
// parser-scoped fullname found nothing to splice (the metadata forms
// degraded to the dangling reference), and a parse that
// references-then-locally-defines the misfiled short name spliced the
// STALE def over its own local definition — metadata describing a
// schema the wire codec rejects (AUDIT_PATTERNS.md B7 second instance).
// The "" fullname is collected but inert: no reference can spell it
// (avroNamedRef rejects the empty string, and no scoped key is empty).
func collectTreeDefs(node any, ns string, visit func(fullname string, def any)) {
	switch v := node.(type) {
	case []any:
		for _, b := range v {
			collectTreeDefs(b, ns, visit)
		}
	case map[string]any:
		typVal, _ := lookupCI(v, "type")
		if typ, _ := typVal.(string); isNamedKind(typ) {
			visit(nodeFullnameTree(v, ns), v)
		}
		walkNodeChildren(v, ns, nodeChildScope(v, ns), nodeChildVisitor{
			field: func(_ int, fo map[string]any, typeKey, scope string) {
				collectTreeDefs(fo[typeKey], scope, visit)
			},
			flatField: func(_ int, fo map[string]any, kind, scope string) {
				collectTreeDefs(flatLiftTypeMap(fo, kind), scope, visit)
			},
			items:  func(key, scope string) { collectTreeDefs(v[key], scope, visit) },
			values: func(key, scope string) { collectTreeDefs(v[key], scope, visit) },
		})
	}
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
		// A wrapped-form name reference — an object whose "type" value
		// names a type ({"type":"X"}, optionally with extra non-structural
		// keys) — is equivalent to the bare-string form "X" plus whatever
		// props it carries. The general path below would instead recurse
		// INTO the "type" value, producing the invalid {"type":{X-def}}
		// (a "type" value must be a string) when X splices: the rebuild
		// Parse then rejects it and the metadata silently falls back to a
		// dangling cross-parse reference. Both wrapper arities are
		// documented-accepted name-ref spellings (including forward
		// refs); a wrapper cannot carry schema-shaped structural keys
		// (the parse rejects those), so any extra keys are inert.
		//
		// When X splices to its inherited definition, the definition
		// replaces the wrapper at this position and the wrapper's PROPS
		// ride on it — keys the definition's own kind routes to props
		// (value-aware, so a malformed stray like "items":3 merges while
		// a key the def's kind binds never does), with the definition
		// winning collisions. Reserved usage-site attributes do not
		// survive the splice; Java drops usage-site extras at reference
		// sites entirely (Schema.java's textual-reference arms return
		// context.find(...) with no properties pass), so preserving the
		// props is already the more faithful treatment of accepted input,
		// and props are canonical-stripped so the schema's identity is
		// unchanged across the three reference spellings.
		//
		// When X stays a bare reference (a local/forward/unknown name, or
		// an already-inlined later occurrence): the SOLE-key wrapper
		// collapses to the bare spelling — it carries no information the
		// bare form lacks, and a later wrapped reference to a type whose
		// first occurrence was inlined would otherwise keep {"type":"X"}
		// where the canonical bare "X" belongs, diverging String() from
		// the identical bare-spelled / inline twin (Canonical/PCF already
		// emits bare fullnames, matching Java's NamedSchema.writeNameRef;
		// only String saw the wrapper). A props-carrying wrapper keeps
		// its shape — collapsing would drop the props.
		if typVal, ok := lookupCI(v, "type"); ok {
			if ref, ok := typVal.(string); ok && avroNamedRef(ref) {
				resolved := inlineTreeDefs(ref, ns, defs, seen, inlined)
				if def, isMap := resolved.(map[string]any); isMap {
					defTyp, defLogical := "", ""
					if tv, ok := lookupCI(def, "type"); ok {
						defTyp, _ = tv.(string)
					}
					if lv, ok := lookupCI(def, "logicalType"); ok {
						defLogical, _ = lv.(string)
					}
					// The wrapper's props are a flat key set (never a
					// nested-stray schema), so no recorded verdict is
					// needed and no re-decode compounds: a nil verdict
					// resolves each key with a fresh single shape check.
					// The reference's own "type" key is its pick and is
					// consumed by the reserved routing below; an unpicked
					// case-variant spelling of any reserved key is an
					// ordinary prop and merges like one.
					//
					// Definition-wins is checked against the definition's
					// OWN keys, snapshotted before any merge: a wrapper
					// prop colliding (case-insensitively) with a
					// definition attribute dies, but two distinct wrapper
					// props that collide only with EACH OTHER both merge —
					// checking the mutating map instead would keep
					// whichever the random map order merged first.
					defKeys := make([]string, 0, len(def))
					for dk := range def {
						defKeys = append(defKeys, dk)
					}
					defHasCI := func(k string) bool {
						for _, dk := range defKeys {
							if strings.EqualFold(dk, k) {
								return true
							}
						}
						return false
					}
					wrapPicks := reservedKeyVariantPicks(v, schemaReservedKeys)
					for k, wv := range v {
						if schemaReservedKeyForObject(v, k, wv, defTyp, defLogical, wrapPicks, nil) {
							continue
						}
						if defHasCI(k) {
							continue
						}
						def[k] = wv
					}
					return def
				}
				if len(v) == 1 {
					return resolved
				}
				if key, ok := ciKey(v, "type"); ok {
					v[key] = resolved
				}
				return v
			}
		}
		// Register this node's own name BEFORE walking its children, mirroring
		// the parser's early self-registration (a type is in scope for its own
		// descendants). A later sibling/descendant reference then sees it; an
		// earlier one already resolved against the cache above. Keys are read
		// case-insensitively, mirroring collectTreeDefs and the parser. The
		// registration fires for every named KIND, "name" key or not — the
		// parser registers a keyless definition's fullname ("ns.", lax-only)
		// the same way, and a spliced subtree can carry one as-written, so a
		// later reference to that fullname must see it in scope (or the walk
		// would splice a second copy and the rebuild would reject the
		// duplicate, degrading the metadata forms to the dangling original).
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
				fullname := nodeFullnameTree(v, ns)
				if ref, ok := dupDefRef(fullname, ns, seen); ok {
					return ref
				}
				seen[fullname] = true
			}
		}
		inlineNodeChildren(v, ns, defs, seen, inlined)
		return v
	}
	return node
}

// inlineNodeChildren splices inherited refs in v's child-schema positions
// via walkNodeChildren: the node's own "type" value (a bare reference there
// resolves in the ENCLOSING scope — see nodeChildVisitor.typeValue), each
// record field's type, and array items / map values (the node's own child
// scope). A flat-form ("linkedin/goavro") field is recursed IN PLACE: the
// field object itself carries the lifted type's structural keys, so the
// splice walks exactly the children the parser lifts while mutations land
// in the original tree (its "type" value is a bare kind string, never a
// reference, so the typeValue splice leaves it as-is). A flat NAMED field
// opens its own namespace scope
// (nodeChildScope on the field object, whose "type" is the lifted kind)
// and, when it re-defines an already-seen name, is rewritten to the
// equivalent normal-form reference field — the same duplicate-definition
// case as inlineTreeDefs's map arm, but a field object cannot be replaced
// by a bare string. A flat UNNAMED field (array/map) keeps the enclosing
// record's scope: the lift drops name/namespace keys for unnamed kinds
// (flatLiftTypeMap), so its items/values sit directly in the RECORD's
// namespace scope. A flat field carrying a structural key of a DIFFERENT
// kind never reaches here: the lift keeps the stray key and the per-kind
// build rejects it, failing the parse.
func inlineNodeChildren(v map[string]any, ns string, defs map[string]any, seen, inlined map[string]bool) {
	spliceAt := func(key, scope string) {
		v[key] = inlineTreeDefs(v[key], scope, defs, seen, inlined)
	}
	walkNodeChildren(v, ns, nodeChildScope(v, ns), nodeChildVisitor{
		typeValue: spliceAt,
		field: func(_ int, fo map[string]any, typeKey, scope string) {
			fo[typeKey] = inlineTreeDefs(fo[typeKey], scope, defs, seen, inlined)
		},
		flatField: func(_ int, fo map[string]any, kind, scope string) {
			if isNamedKind(kind) {
				fullname := nodeFullnameTree(fo, scope)
				if ref, ok := dupDefRef(fullname, scope, seen); ok {
					rewriteFlatFieldToRef(fo, ref)
					return
				}
				seen[fullname] = true
			}
			inlineNodeChildren(fo, scope, defs, seen, inlined)
		},
		items:  spliceAt,
		values: spliceAt,
	})
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
	// The "" fullname (a keyless definition with no namespace in scope)
	// has no reference spelling at all — avroNamedRef rejects the empty
	// string — so a second definition stays in place unconditionally.
	if fullname == "" || !seen[fullname] {
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
