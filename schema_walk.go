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
// the parser's case-insensitive key reads. walkNodeChildren is that rule's
// single implementation, built on the parser's own predicates (lookupCI /
// ciKey, flatFieldNeedsLift / flatLiftTypeMap, isNamedKind); the walkers
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
// build under that fullname's namespace. Non-named kinds never reach
// nodeNamespace's attribute reads on a parser-accepted tree: the build
// rejects a non-empty "name" or any "namespace" key on them ("only
// record, enum, and fixed can have a name", schema.go), and the one
// non-named object shape that may carry a "namespace" — a wrapped
// reference {"type":"X","namespace":...}, which returns from the build
// before that check — parses only with no container children for the
// scope to apply to (any of items/values/size/non-empty fields alongside
// the reference falls through to the "unknown complex type" reject).
func nodeChildScope(v map[string]any, ns string) string {
	if typVal, ok := lookupCI(v, "type"); ok {
		if typ, _ := typVal.(string); isNamedKind(typ) {
			return nodeNamespace(v, ns)
		}
	}
	return ns
}

// nodeChildVisitor receives the child-schema positions of one raw-JSON
// schema node from walkNodeChildren. A nil callback skips its position
// (the lookups have no side effects, so skipping is free). Every key
// handed to a callback is the key actually present in the map, resolved
// case-insensitively with an exact match preferred (ciKey — the same
// selection lookupCI makes), so mutating walkers write back to the
// present key instead of introducing a duplicate canonical-cased key.
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
}

// walkNodeChildren enumerates the child-schema positions of the raw-JSON
// schema node v: its own "type" value, each record field's type (with
// flat-form fields lifted), array items, and map values. Keys are read
// case-insensitively exactly as the parser reads them. ns is the scope v
// itself sits in (handed to typeValue); childNS is the scope v's
// containers resolve in — nodeChildScope(v, ns), or the metadata
// walker's equivalent derivation from its already-built node
// (nsForChildren, schema_node.go).
//
// The enumeration order is fixed: type, fields in declaration order,
// items, values. On a parser-accepted tree no per-walker order could be
// observed to differ: each kind's build rejects the other kinds'
// structural keys ("invalid <kind> has schema for other types",
// schema.go), so a node carries at most one of fields/items/values — the
// only coexistence is the empty "fields":[] escape (a zero-length array
// passes the len(o.Fields) > 0 rejections), which enumerates nothing.
func walkNodeChildren(v map[string]any, ns, childNS string, vis nodeChildVisitor) {
	if vis.typeValue != nil {
		if key, ok := ciKey(v, "type"); ok {
			vis.typeValue(key, ns)
		}
	}
	if vis.fields != nil || vis.field != nil || vis.flatField != nil {
		if fk, ok := ciKey(v, "fields"); ok {
			if arr, ok := v[fk].([]any); ok {
				if vis.fields != nil {
					vis.fields(arr)
				}
				for i, f := range arr {
					fo, ok := f.(map[string]any)
					if !ok {
						continue
					}
					tk, ok := ciKey(fo, "type")
					if !ok {
						continue
					}
					if ts, isStr := fo[tk].(string); isStr && flatFieldNeedsLift(fo, ts) {
						if vis.flatField != nil {
							vis.flatField(i, fo, ts, childNS)
						}
						continue
					}
					if vis.field != nil {
						vis.field(i, fo, tk, childNS)
					}
				}
			}
		}
	}
	if vis.items != nil {
		if key, ok := ciKey(v, "items"); ok {
			vis.items(key, childNS)
		}
	}
	if vis.values != nil {
		if key, ok := ciKey(v, "values"); ok {
			vis.values(key, childNS)
		}
	}
}
