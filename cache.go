package avro

import (
	"crypto/sha256"
	"encoding/json"
	"maps"
	"strings"
	"sync"
)

// SchemaCache accumulates named types across multiple [SchemaCache.Parse]
// calls, so that a schema can reference types defined in previously parsed
// schemas. This is how a schema registry's inter-schema references work.
//
// Parse schemas in dependency order: a referenced type must be parsed before
// the schemas that reference it.
//
// You can parse the same schema string more than once; we return the
// previously parsed result, so diamond dependencies (A->B->D, A->C->D) need
// no tracking on your side. Options that change what the string compiles to,
// custom types or [WithLaxNames], skip this deduplication and re-parse, since
// the string alone no longer identifies the result. We normalize JSON
// whitespace and key order when deduplicating, but not the Avro canonical
// form: schemas differing only in formatting dedupe, while differences in
// non-canonical fields like doc or aliases return a duplicate type error.
//
// Each returned [*Schema] is fully resolved and independent of the cache.
// That extends to sub-schemas: a node extracted from [Schema.Root] converts
// via [SchemaNode.Schema] with every cross-parse reference resolved, so you
// never need the cache again once Parse returns.
//
// Note that [WithLaxNames] is sticky: if a type is defined with it, pass it
// to every later Parse that references that type. A schema containing a lax
// name is not parseable without it, cache or no cache, so re-parsing the
// referencing schema's [Schema.String] or [Schema.Canonical] output also
// needs WithLaxNames. [Schema.Encode] and [Schema.Decode] are unaffected.
//
// The zero value is ready to use. A SchemaCache is safe for concurrent use.
type SchemaCache struct {
	mu    sync.Mutex
	named map[string]*namedType
	dedup map[[32]byte]*Schema
	// skipDedupParsed holds the schema strings this cache has parsed under
	// options that skip dedup: custom types or WithLaxNames. It answers one
	// question: may this string re-define a name the cache already holds? A
	// string the cache has compiled before under any option may; a new
	// string may not. We do not record which option, since nothing asks.
	skipDedupParsed map[[32]byte]bool
	// defs holds each registered named type's self-contained JSON
	// definition, all attributes included, keyed by fullname. A later Parse
	// referencing an earlier type splices the definition back in at the
	// first dangling reference; see inlineTreeDefs.
	defs map[string]any
}

// cacheNormalizeSchema re-marshals a schema string so two spellings of one
// schema, differing in whitespace or key order, share a dedup key. Only a
// schema that is exactly one JSON value normalizes: a decode that stopped at
// the first value would accept trailing content that Parse rejects, so
// anything with trailing content comes back untouched for parse to refuse.
func cacheNormalizeSchema(schema string) string {
	v, err := decodeSchemaAnyStrict(schema)
	if err != nil {
		return schema
	}
	normalized, err := json.Marshal(v)
	if err != nil {
		return schema
	}
	return string(normalized)
}

// Parse parses a schema string, registering any named types (records, enums,
// fixed) in the cache. Named types from previous Parse calls are available
// for reference resolution. On failure we do not modify the cache.
func (c *SchemaCache) Parse(schema string, opts ...SchemaOpt) (*Schema, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.named == nil {
		c.named = make(map[string]*namedType)
		c.dedup = make(map[[32]byte]*Schema)
		c.skipDedupParsed = make(map[[32]byte]bool)
		c.defs = make(map[string]any)
	}

	schema = cacheNormalizeSchema(schema)
	// Clone the cache's map so a failed parse doesn't corrupt the cache.
	cloned := maps.Clone(c.named)
	b := &builder{
		named:      cloned,
		building:   make(map[*schemaNode]struct{}),
		definedSet: make(map[*namedType]bool),
		minBytes:   newMinBytesWalk(),
	}
	applySchemaOpts(b, opts)
	hasCustomTypes := len(b.customTypes) > 0
	// WithLaxNames changes what a schema string compiles to, so a lax parse
	// skips dedup the way a custom-type parse does. Otherwise a lax-then-strict
	// sequence would hand the strict caller the cached lax schema, and a
	// strict-then-lax sequence would ignore the option.
	hasLaxNames := b.checkName != nil
	skipDedup := hasCustomTypes || hasLaxNames

	h := sha256.Sum256([]byte(schema))
	if !skipDedup {
		if s, ok := c.dedup[h]; ok {
			return s, nil
		}
	}

	// cachedNames marks every name inherited from the cache. It cannot by
	// itself tell an inherited reference from a name this parse re-defines,
	// since a re-registered name is in both, so
	// rejectCachedRefIfCustomTypeWouldMatch keys "defined this parse" on
	// definedSet and reads cachedNames only to recognize a cross-parse
	// reference. We populate it for every parse with inherited names: the
	// cross-parse custom-boundary guard must fire even for a plain parse that
	// references a custom-built cached type.
	if len(cloned) > 0 {
		b.cachedNames = make(map[string]bool, len(cloned))
		for name := range cloned {
			b.cachedNames[name] = true
		}
	}
	// allowReRegister lets a parse re-define an inherited name, granted only
	// when the cache has already seen this exact schema string: such a
	// re-parse re-registers the names it defined itself, which is not a
	// conflict. A new string re-defining an inherited name still errors,
	// custom mode included.
	b.allowReRegister = c.dedup[h] != nil || c.skipDedupParsed[h]

	s, err := parse(schema, b)
	if err != nil {
		return nil, err
	}

	// A parse referencing a type from a prior cache Parse resolves it in the
	// node tree but leaves a bare reference in the JSON forms, so
	// Canonical/Fingerprint/Root/String would not be self-contained. We
	// splice the inherited definitions into the original JSON, preserving
	// attributes the node tree never stored (doc, order, field props), and
	// rebuild the metadata forms from that cache-lessly. The encode/decode
	// path is untouched.
	//
	// The trigger is "inlineTreeDefs spliced something", not "s.full does not
	// re-parse cache-lessly": a bare reference to an inherited type can
	// re-parse cleanly by forward-binding to a same-named type defined later
	// in this schema. Binding is eager, so the wire codec holds the inherited
	// type, and the metadata would then describe a different schema.
	selfContained := s.full
	if len(cloned) > 0 {
		if tree, terr := unmarshalAnyPreservePrecision(s.full); terr == nil {
			inlined := make(map[string]bool)
			tree = inlineTreeDefs(tree, "", c.defs, make(map[string]bool), inlined)
			if len(inlined) > 0 {
				if marshaled, merr := json.Marshal(tree); merr == nil {
					s2, rerr := Parse(string(marshaled), opts...)
					if rerr != nil {
						// The spliced form may carry a name an inherited type
						// was defined with under WithLaxNames, which this
						// call's opts reject. Retry with the accept-everything
						// validator, appended last so it wins over any user
						// fn. Every spliced name was validated by the parse
						// that defined it, and names pass through verbatim.
						laxOpts := make([]SchemaOpt, len(opts)+1)
						copy(laxOpts, opts)
						laxOpts[len(opts)] = internalReparseNames
						s2, rerr = Parse(string(marshaled), laxOpts...)
					}
					if rerr == nil {
						selfContained = s2.full
						// Adopting s2's canonical form also adopts its SOE
						// header: it is hashed from c on first use, and
						// nothing has used this schema yet.
						s.c = s2.c
						s.full = s2.full
					}
				}
			}
		}
	}

	// Record this parse's named-type definitions (from the self-contained form,
	// so each captured definition has its own transitive references spliced in)
	// for future cross-parse references.
	if tree, terr := unmarshalAnyPreservePrecision(selfContained); terr == nil {
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
	// The condition that decided whether to consult dedup decides which side
	// to record on.
	if skipDedup {
		c.skipDedupParsed[h] = true
	} else {
		c.dedup[h] = s
	}
	return s, nil
}

// --- cross-parse self-containment (SchemaCache.defs) ---
//
// The helpers below splice each cache-inherited definition back into the
// schema JSON at its first reference, so Canonical/Fingerprint/Root/String
// stay self-contained with every original attribute. They work on the
// generic JSON tree, a lossless round-trip via
// unmarshalAnyPreservePrecision + json.Marshal, tracking namespace scope so
// references resolve to the right fullname.

func avroNamedRef(typ string) bool {
	switch typ {
	case "null", "boolean", "int", "long", "float", "double", "bytes", "string",
		"record", "enum", "array", "map", "union", "fixed", "error":
		return false
	}
	return typ != ""
}

// treeScope resolves a raw named-type object's fullname and the namespace
// in scope inside it, by resolveScope over the "name" and "namespace" keys
// read by exact name, as the parser reads them.
func treeScope(obj map[string]any, enclosingNS string) (fullname, ns string) {
	name, _ := obj["name"].(string)
	nsAttr, hasNS := obj["namespace"].(string)
	return resolveScope(name, nsAttr, hasNS, enclosingNS)
}

func nodeNamespace(obj map[string]any, enclosingNS string) string {
	_, ns := treeScope(obj, enclosingNS)
	return ns
}

func nodeFullnameTree(obj map[string]any, enclosingNS string) string {
	fullname, _ := treeScope(obj, enclosingNS)
	return fullname
}

// collectTreeDefs calls visit for every named-type definition in the tree
// with its resolved fullname, tracking namespace scope through
// walkNodeChildren so every definition is collected where the parser
// registers it. We fire for every named kind, "name" key or not: the parser
// registers a fullname even with no name key (fullname "ns.") and scopes
// children by the namespace attribute regardless. Gating on the name key
// would misfile nested defs, and a later reference would splice a stale def
// over its own. The "" fullname is collected but inert, since no reference
// can spell it.
func collectTreeDefs(node any, ns string, visit func(fullname string, def any)) {
	switch v := node.(type) {
	case []any:
		for _, b := range v {
			collectTreeDefs(b, ns, visit)
		}
	case map[string]any:
		if typ, _ := v["type"].(string); isNamedKind(typ) {
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

// inlineTreeDefs replaces the first occurrence of each reference to a
// cache-inherited named type (in defs, not defined locally before the
// reference, not already inlined) with a deep copy of its definition,
// recursing into the copy so transitive references resolve too. Later
// occurrences stay bare.
//
// Binding mirrors the parser: eager, in-scope-first, positional. A local
// definition registered at this point in the walk beats a cache-inherited
// one, and seen accumulates local fullnames in DFS pre-order as the parser
// registers them, so a reference after a local definition stays bare while
// one before it splices the cached type. A position-independent local set
// would diverge the JSON forms from the wire codec.
//
// The walk also dedupes definitions: two inherited references sharing a
// transitive type would each carry that type's definition, which the
// rebuild Parse rejects as a duplicate, so a second definition is rewritten
// to a reference to the first.
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
		// A wrapped name reference, {"type":"X"} plus optional non-structural
		// keys, is the bare "X" plus its props. Recursing into the "type"
		// value would emit {"type":{X-def}} when X splices, which the rebuild
		// Parse rejects. So when X splices, the definition replaces the
		// wrapper and the wrapper's props ride on it, the definition winning
		// collisions; reserved usage-site attributes do not survive, as in
		// Java. When X stays bare, a sole-key wrapper collapses to the bare
		// spelling so String() matches its bare-spelled twin, and a
		// props-carrying wrapper keeps its shape.
		if ref, ok := v["type"].(string); ok && avroNamedRef(ref) {
			resolved := inlineTreeDefs(ref, ns, defs, seen, inlined)
			if def, isMap := resolved.(map[string]any); isMap {
				defTyp, _ := def["type"].(string)
				defLogical, _ := def["logicalType"].(string)
				for k, wv := range v {
					if schemaReservedKeyForObject(k, wv, defTyp, defLogical, strayPresence(k, wv)) {
						continue
					}
					if _, has := def[k]; has {
						continue
					}
					def[k] = wv
				}
				return def
			}
			if len(v) == 1 {
				return resolved
			}
			v["type"] = resolved
			return v
		}
		// Register this node's own name before walking its children, as the
		// parser does, so a later sibling or descendant sees it. A name
		// already defined here is a second definition inside another spliced
		// subtree (the diamond A->{B,C}->D, or a nested type referenced
		// before its container); the JSON keeps the first definition and
		// references it thereafter, as Java's toString does.
		if typ, _ := v["type"].(string); isNamedKind(typ) {
			fullname := nodeFullnameTree(v, ns)
			if ref, ok := dupDefRef(fullname, ns, seen); ok {
				return ref
			}
			seen[fullname] = true
		}
		inlineNodeChildren(v, ns, defs, seen, inlined)
		return v
	}
	return node
}

// inlineNodeChildren splices inherited references in v's child-schema
// positions via walkNodeChildren: the node's own "type" value, each record
// field's type, and array items and map values. A flat-form field recurses
// in place, since the field object carries the lifted type's structural
// keys. A flat named field opens its own namespace scope and, when it
// re-defines an already-seen name, is rewritten to the normal-form reference
// field, since a field object cannot be replaced by a bare string.
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

// dupDefRef returns the reference spelling that replaces a second definition
// of fullname at a position whose enclosing namespace is ns, or ("", false)
// to leave the duplicate in place, in which case the rebuild Parse fails and
// the metadata falls back to the dangling original rather than describing a
// different schema than the wire codec.
//
// A dotted fullname is an exact lookup on re-parse. A null-namespace name
// has only its bare short name, which the parser binds
// enclosing-namespace-first, so it is safe unless an earlier definition of
// the same short name qualified by the enclosing namespace would capture it.
// Avro has no absolute-reference syntax for that case, and Java's toString
// shares the limitation.
func dupDefRef(fullname, ns string, seen map[string]bool) (string, bool) {
	// The "" fullname (a keyless definition with no namespace in scope) has
	// no reference spelling, since avroNamedRef rejects the empty string.
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
// already-defined named type into the normal-form field whose "type" is a
// name reference. We keep exactly the keys liftFlatFieldType treats as
// field-only: name, default, order, aliases. Everything else belongs to the
// type and rides on its first definition.
func rewriteFlatFieldToRef(fo map[string]any, ref string) {
	for k := range fo {
		switch k {
		case "name", "default", "order", "aliases", "type":
		default:
			delete(fo, k)
		}
	}
	fo["type"] = ref
}

// defWithExplicitNamespace deep-copies a named-type definition and makes its
// namespace explicit from the resolved fullname: "name" becomes the short
// name and "namespace" the resolved namespace, "" for the null namespace.
// Otherwise a definition relying on an inherited namespace would re-inherit
// whatever schema it is later spliced into (a.b.Inner becoming c.d.Inner).
// Only the top level is rewritten, so nested types resolve against this
// namespace or keep their own.
func defWithExplicitNamespace(def any, fullname string) any {
	cp := deepCopyTree(def)
	if obj, ok := cp.(map[string]any); ok {
		obj["name"] = unqualified(fullname)
		obj["namespace"] = namespaceOf(fullname)
	}
	return cp
}
