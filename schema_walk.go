package avro

// Shared child enumeration for the raw-JSON schema walkers.
//
// Outside the wire parser itself, parsed schema JSON (a map[string]any
// tree from a schema that already passed Parse) is re-walked in three
// places: the SchemaCache self-containment walkers (collectTreeDefs and
// inlineTreeDefs, cache.go) and the Root() metadata walker
// (nodeFromJSONObject, schema_node.go). All three consume one rule: WHICH
// keys of a node hold child schemas, WHAT a flat-form ("linkedin/goavro")
// field lifts to, and WHAT namespace scope each child resolves in — with
// the parser's exact-name key reads (a reserved attribute name matches
// only its exact lowercase spelling; a case-variant key is an ordinary
// custom property). walkNodeChildren is that rule's single
// implementation, built on the parser's own predicates
// (flatFieldNeedsLift / flatLiftTypeMap, isNamedKind); the walkers
// differ only in what they DO at each position (collect a definition,
// splice a reference, build a SchemaNode), supplied as callbacks.

// nodeChildScope returns the namespace scope a raw-JSON schema node's
// container children (record fields, array items, map values) resolve in:
// a named-kind node (record/error/enum/fixed) opens its own scope — the
// dotted-name prefix, else the explicit "namespace" attribute (including
// the explicit-empty null-namespace form), else the enclosing scope
// (nodeNamespace) — and every other node passes the enclosing scope
// through.
//
// This is the parser's rule, keyed on the KIND alone. Gating on name
// presence would be wrong: the build resolves and registers a fullname
// even when the "name" key is absent entirely (an empty short name that a
// WithLaxNames validator accepted — fullname "ns."), and the children
// build under that fullname's namespace. Non-named kinds never open a
// scope: a stray "namespace" key on them is inert metadata the parser
// never reads (the build rejects a non-empty "name" on unnamed CONTAINER
// kinds — "only record, enum, and fixed can have a name", schema.go — so
// the kind check here and the parser agree that container children keep
// the enclosing scope), and a wrapped reference
// {"type":"X","namespace":...} parses only with no container children for
// a scope to apply to (any of items/values/size/non-empty fields
// alongside the reference falls through to the "unknown complex type"
// reject).
func nodeChildScope(v map[string]any, ns string) string {
	if typ, _ := v["type"].(string); isNamedKind(typ) {
		return nodeNamespace(v, ns)
	}
	return ns
}

// nodeChildVisitor receives the child-schema positions of one raw-JSON
// schema node from walkNodeChildren. A nil callback skips its position
// (the lookups have no side effects, so skipping is free). Every key
// handed to a callback is the exact lowercase reserved spelling — the
// only spelling that binds — so mutating walkers write back to the key
// actually present in the map.
type nodeChildVisitor struct {
	// typeValue is the node's own "type" value (v[key]). It resolves in
	// the ENCLOSING namespace scope, not the node's own child scope: a
	// wrapped reference {"type":"X"} binds X where the node sits. On a
	// parser-accepted tree the value is always a string (aobjectFromMap
	// rejects any other JSON type for a node-level "type"), but a
	// reference string here can name a cache-inherited type, so it is a
	// child position for the splice walker.
	typeValue func(key, scope string)
	// fields fires once when a "fields" key holds an array, with the raw
	// array, before any field/flatField callback (for sizing).
	fields func(arr []any)
	// field is a normal-form record field: the child schema is
	// fo[typeKey], resolving in the node's child scope. Every fields
	// element that is a JSON object with a "type" key fires exactly one
	// of field/flatField; elements missing either never parse
	// (afieldFromAny rejects non-object fields, and build rejects a
	// field whose type is absent), so they are skipped.
	field func(i int, fo map[string]any, typeKey, scope string)
	// flatField is a flat-form ("linkedin/goavro") field: fo's own keys
	// carry the lifted type definition of kind (the field's bare-string
	// "type" value), and flatLiftTypeMap(fo, kind) is the lifted view.
	// scope is the node's child scope, where the lifted definition sits.
	// The lift decision is the parser's own flatFieldNeedsLift, so every
	// walker lifts exactly the fields the parser lifts.
	flatField func(i int, fo map[string]any, kind, scope string)
	// items / values are the array-items / map-values child (v[key]),
	// resolving in the node's child scope.
	items  func(key, scope string)
	values func(key, scope string)

	// fieldNoType fires for a field element that carries no "type" key —
	// reachable only inside a STRAY "fields" (a bound record build
	// rejects a nil field type, so bound walks never see one). Only the
	// strayKeys walker sets it: the element still surfaces as-written.
	fieldNoType func(i int, fo map[string]any)

	// strayKeys additionally fires the container callbacks on nodes whose
	// kind does not BIND the key, for bodies that parse as the key's
	// schema shape (strayBodyShapeOK — a non-schema-shaped stray body
	// stays a Props entry and is never walked). The parser's grammar is
	// kind-keyed — "fields" binds on record/error, "items" on array,
	// "values" on map — and on any other kind a present container key is
	// inert as-written metadata (primitive objects accept such keys;
	// container and named kinds reject foreign SCHEMA-SHAPED structural
	// keys at build). By default the walk enumerates bound keys only, so
	// a consumer that treats these positions as SCHEMA positions
	// (collecting definitions for cross-parse reference, splicing cached
	// definitions, registering names) can never consume structure the
	// parse never bound — a definition-shaped value inside a stray key
	// would otherwise occupy its fullname in a first-wins store and
	// shadow the real definition's metadata everywhere the store feeds
	// (Canonical, fingerprints, the single-object header, String, Root).
	//
	// The metadata walker alone sets strayKeys: SchemaNode surfaces stray
	// container keys as-written on the matching structural field, a
	// read-only surfacing duty with no registration or mutation. That
	// asymmetry is deliberate and pinned
	// (TestRegression_MetadataStrayKeySurfacedAsWritten); a uniformity
	// change that gates the metadata walker too breaks the surfacing
	// contract.
	strayKeys bool

	// strayShapeMemo, when set, memoizes the stray-body shape checks below
	// by subtree pointer across a whole walk, so a nested-stray schema is
	// validated once (linear) instead of once per enclosing level
	// (O(depth^2)). The metadata walker sets it (one memo per Root() call);
	// a nil memo takes the plain per-call check. Only consulted when
	// strayKeys is set, so non-stray walkers never touch it.
	strayShapeMemo strayShapeMemo
}

// walkNodeChildren enumerates the child-schema positions of the raw-JSON
// schema node v: its own "type" value, each record field's type (with
// flat-form fields lifted), array items, and map values. Keys are read
// by exact name, exactly as the parser reads them. ns is the scope v
// itself sits in (handed to typeValue); childNS is the scope v's
// containers resolve in — nodeChildScope(v, ns), or the metadata
// walker's equivalent derivation from its already-built node
// (nsForChildren, schema_node.go).
//
// The enumeration order is fixed: type, fields in declaration order,
// items, values. Each container key fires only on the kind that BINDS it
// (fields → record/error, items → array, values → map — the parser's
// kind-keyed grammar) unless vis.strayKeys opts into the stray positions
// too; see the strayKeys doc. Under the bound-only default a node fires
// at most one of fields/items/values: container and named kinds reject
// foreign structural keys at build ("invalid <kind> has schema for other
// types", schema.go), and the empty "fields":[] escape (a zero-length
// array passes the len(o.Fields) > 0 rejections) enumerates nothing. A
// strayKeys walk of a primitive object can fire several — a primitive
// carries any of them as inert metadata.
func walkNodeChildren(v map[string]any, ns, childNS string, vis nodeChildVisitor) {
	typ, _ := v["type"].(string)
	if vis.typeValue != nil {
		if _, ok := v["type"]; ok {
			vis.typeValue("type", ns)
		}
	}
	if vis.fields != nil || vis.field != nil || vis.flatField != nil {
		if fv, ok := v["fields"]; ok &&
			(isRecordKind(typ) || (vis.strayKeys && strayBodyShapeOKMemo(vis.strayShapeMemo, "fields", fv))) {
			if arr, ok := fv.([]any); ok {
				if vis.fields != nil {
					vis.fields(arr)
				}
				for i, f := range arr {
					fo, ok := f.(map[string]any)
					if !ok {
						continue
					}
					tv, ok := fo["type"]
					if !ok {
						// A field with no type key never parses at a BOUND
						// position (the record build rejects a nil field
						// type), but inside a STRAY "fields" the record
						// build never runs, so such elements are parseable
						// and must surface as-written — fieldNoType lets
						// the strayKeys walker fill the pre-sized slot with
						// the element's own attributes instead of leaving a
						// fabricated zero field behind.
						if vis.fieldNoType != nil {
							vis.fieldNoType(i, fo)
						}
						continue
					}
					if ts, isStr := tv.(string); isStr && flatFieldNeedsLift(fo, ts) {
						if vis.flatField != nil {
							vis.flatField(i, fo, ts, childNS)
						}
						continue
					}
					if vis.field != nil {
						vis.field(i, fo, "type", childNS)
					}
				}
			}
		}
	}
	if vis.items != nil {
		if iv, ok := v["items"]; ok &&
			(typ == "array" || (vis.strayKeys && strayBodyShapeOKMemo(vis.strayShapeMemo, "items", iv))) {
			vis.items("items", childNS)
		}
	}
	if vis.values != nil {
		if vv, ok := v["values"]; ok &&
			(typ == "map" || (vis.strayKeys && strayBodyShapeOKMemo(vis.strayShapeMemo, "values", vv))) {
			vis.values("values", childNS)
		}
	}
}
