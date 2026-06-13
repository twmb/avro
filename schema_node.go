package avro

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

// SchemaNode is a read-write representation of an Avro schema. It can be
// obtained from a parsed schema via [Schema.Root], or constructed directly
// and converted to a [*Schema] via the [SchemaNode.Schema] method.
//
// The Type field determines which other fields are relevant:
//
//   - Primitives (null, boolean, int, long, float, double, string, bytes):
//     LogicalType, Precision, Scale, and Props are optional. Other fields
//     are ignored.
//   - record/error: Name and Fields are required. Namespace, Doc, and Props
//     are optional.
//   - enum: Name and Symbols are required. Namespace, Doc, and Props are
//     optional.
//   - array: Items is required.
//   - map: Values is required.
//   - fixed: Name and Size are required. LogicalType, Precision, Scale,
//     Namespace, and Props are optional.
//   - union: Branches lists the member schemas.
//
// A named type (record, enum, fixed) that has already been defined
// elsewhere in the schema can be referenced by setting Type to its full
// name (e.g. com.example.Address) with no other fields.
type SchemaNode struct {
	Type        string // Avro type or named type reference
	LogicalType string // e.g. date, timestamp-millis, decimal, uuid; empty if none

	Name string // name for record, enum, fixed

	// Namespace is the named type's RESOLVED namespace. [Schema.Root]
	// populates it for every named type: a child that inherits its
	// enclosing namespace surfaces that namespace here, and "" always
	// means the null namespace, never "inherit from the parent". When
	// constructing a SchemaNode by hand, set Namespace explicitly or
	// leave "" for the null namespace — [SchemaNode.Schema] emits a
	// "namespace":"" escape when a null-namespace type sits inside a
	// namespaced scope, so the distinction survives the round trip.
	// A dotted Name carries its own namespace and takes precedence
	// over this field.
	Namespace string

	Aliases []string // alternate names for named types (record, enum, fixed)
	Doc     string   // documentation string

	Fields   []SchemaField // record fields
	Items    *SchemaNode   // array element schema
	Values   *SchemaNode   // map value schema
	Branches []SchemaNode  // union member schemas
	Symbols  []string      // enum symbols
	Size     int           // fixed byte size

	EnumDefault    string // default symbol for enum schema evolution
	HasEnumDefault bool   // true if an enum default is defined

	Precision int // decimal precision
	Scale     int // decimal scale

	// Props holds custom (non-reserved) schema attributes — anything
	// in the schema JSON that is not a standard Avro field (e.g.
	// namespace-prefixed metadata like "com.example.tag").
	//
	// Values use the natural Go types from JSON: string, bool, nil,
	// []any, map[string]any, plus int64 for whole numbers, float64
	// for fractional, and json.Number for whole numbers too large for
	// int64. Whole-valued exponents collapse to int64 (1e3 reads as
	// int64(1000)), and exponents that overflow float64 give ±Inf.
	//
	// math.NaN() stored in Props re-reads as string "NaN" after
	// round-tripping through Schema() and Root(), because JSON has
	// no NaN literal. ±Inf round-trips correctly as float64(±Inf).
	Props map[string]any
}

// SchemaField represents a field in an Avro record schema.
type SchemaField struct {
	Name string     // field name
	Type SchemaNode // field schema

	// Default is the field's default value, present when HasDefault is
	// true. The Go type matches the schema:
	//
	//   - int schemas give int32, long schemas give int64. Out-of-range
	//     defaults are rejected at parse.
	//   - float schemas give float32, double schemas give float64.
	//     Overflows narrow to ±Inf; NaN, ±Inf, and a float-syntax negative
	//     zero ("-0.0") round-trip correctly. An integer-syntax "-0" is the
	//     sign-less integer 0 and surfaces as +0.0 (matching Java/fastavro),
	//     even though the wire encoder writes -0.0 for that literal.
	//   - string and enum schemas give string.
	//   - bytes and fixed schemas give []byte, already decoded from
	//     the JSON spec's codepoint-per-byte form so you can pass
	//     Default straight back to AppendEncode without conversion.
	//   - record, array, and map schemas give map[string]any or []any
	//     respectively, with each leaf following these same rules.
	//
	// Union defaults pick the first branch that accepts the value; the
	// Go type tells you which branch was chosen. For ["float","int"]
	// with default 42 you get float32(42) because float matches first.
	//
	// Unlike Props, Default for numeric fields is never json.Number:
	// schema parse rejects defaults that do not fit the declared type.
	Default any

	HasDefault bool     // true if a default value is defined in the schema
	Aliases    []string // field aliases for schema evolution
	Order      string   // sort order: "ascending" (default), "descending", or "ignore"
	Doc        string   // documentation string

	// Props holds custom (non-reserved) field properties. Numbers decode as
	// in [SchemaNode.Props].
	Props map[string]any
}

// Schema parses the SchemaNode into a [*Schema] that can be used for
// encoding and decoding. Returns an error if the node is invalid.
//
// Named types (record, enum, fixed) that appear multiple times in the
// tree are automatically deduplicated, matched by FULLNAME: the first
// occurrence emits the full definition and subsequent occurrences emit
// the fullname as a reference. Two types sharing a short name across
// namespaces are distinct and both emit full definitions.
//
// opts are passed through to the internal [Parse]: a schema originally
// parsed with [SchemaOpt]s that change what Parse accepts or wires —
// [WithLaxNames] for non-standard names, [CustomType] registrations —
// needs the same opts here, or the rebuilt schema fails to parse (lax
// names) or silently lacks the custom wiring.
func (n *SchemaNode) Schema(opts ...SchemaOpt) (*Schema, error) {
	d := &deduper{
		defined: make(map[string]*SchemaNode),
		visited: make(map[*SchemaNode]struct{}),
	}
	tree := n.toJSONDedup(d)
	if d.err != nil {
		return nil, d.err
	}
	b, err := json.Marshal(tree)
	if err != nil {
		return nil, fmt.Errorf("avro: marshaling schema node: %w", err)
	}
	return Parse(string(b), opts...)
}

// deduper tracks named type definitions during toJSONDedup and records
// conflicting redefinitions. It also detects cycles introduced via
// *SchemaNode Items/Values pointers (which are the only way a SchemaNode
// tree can have true cycles — Fields and Branches are value slices).
type deduper struct {
	defined map[string]*SchemaNode   // fullname → first definition's node
	visited map[*SchemaNode]struct{} // seen *SchemaNode pointers (cycle detection)
	err     error                    // first conflict or cycle encountered
}

// Root returns a SchemaNode tree describing the parsed schema. All
// metadata is preserved (doc strings, namespaces, custom properties,
// numeric defaults). See [SchemaNode.Props] and [SchemaField.Default]
// for how values decode.
//
// Root re-parses the JSON on each call. Cache the result if you need
// to access it repeatedly (e.g. in a per-message processing loop).
func (s *Schema) Root() SchemaNode {
	raw, err := unmarshalAnyPreservePrecision([]byte(s.full))
	if err != nil {
		panic("avro: Schema.Root: invalid stored JSON: " + err.Error())
	}
	n := nodeFromJSON(raw, "")
	fixupNameRefDefaults(&n)
	return n
}

// toJSONDedup is like toJSON but deduplicates named types. The first
// occurrence of a named type (record, enum, fixed) emits the full
// definition; subsequent occurrences emit the name as a reference.
func (n *SchemaNode) toJSONDedup(d *deduper) any {
	return n.toJSONWalk(d.visited, d, "", 0)
}

// jsonSerializableValue returns v with three Avro-JSON-specific shape
// fixups applied (directly or under map[string]any / []any container
// layers):
//
//  1. ±Inf float → [json.Number]("±1e1000") literal that re-parses to
//     the same value via [parseFloatAcceptOverflow] (schema.go). The
//     inverse of normalizeJSONNumber's ErrRange-with-Inf accept.
//     Required because Go's standard JSON encoder unconditionally
//     rejects ±Inf and NaN, so a SchemaNode obtained from [Schema.Root]
//     for a schema whose Default / Props normalized an exponent-form
//     overflow to ±Inf cannot otherwise round-trip through
//     [SchemaNode.Schema].
//
//  2. NaN float → JSON string "NaN". RFC 8259 has no NaN literal, so
//     no JSON number can encode NaN (compare item 1's ±Inf overflow
//     trick — no analogue exists for NaN). Re-parse recovers
//     float64(NaN) only for SchemaField.Default of a float / double
//     field (via coerceMetadataDefault → defaultAsFloat's string
//     arm; the schema type drives the coercion). For SchemaNode.Props
//     and SchemaField.Props the string survives unchanged —
//     auto-coercing literal "NaN" in normalizeJSONValue would silently
//     reinterpret user-intentional string Props (Parse can never
//     produce NaN in Props; a hand-written {"x":"NaN"} is the user
//     storing a string). User-facing note lives on SchemaNode.Props.
//
//  3. []byte → codepoint-per-byte string (each byte 0x00-0xFF becomes
//     a rune at the same code point). The inverse of
//     [avroJSONBytesToBytes] / [coerceMetadataDefault]'s bytes/fixed
//     arm: that arm materializes Default as []byte (the wire form);
//     re-emitting requires putting it back in the Avro JSON
//     codepoint-string form (the spec form for bytes/fixed defaults).
//     A plain []byte marshal would base64-encode the slice
//     ("AQID" for {0x01,0x02,0x03}) which the Avro parser would then
//     re-read as raw bytes [0x41,0x51,0x49,0x44] — a silent value
//     corruption breaking [SchemaNode.Schema] round-trips for any
//     bytes/fixed default. Programmatically-constructed Props with
//     []byte values also get the codepoint encoding (Avro's
//     convention), not base64; users who need base64 in Props should
//     pre-encode to a string.
//
// Container values (map[string]any, []any) are deep-copied only when a
// descendant requires conversion, so the common no-fixup case is
// allocation-free and the user's SchemaNode storage is never mutated.
func jsonSerializableValue(v any) any {
	if !needsJSONFixup(v) {
		return v
	}
	return applyJSONFixup(v)
}

// isNegativeZero reports whether f is IEEE-754 negative zero (−0.0). Distinct
// from +0.0 only by the sign bit; both compare == 0.
func isNegativeZero(f float64) bool {
	return f == 0 && math.Signbit(f)
}

func needsJSONFixup(v any) bool {
	switch tv := v.(type) {
	case float64:
		return math.IsInf(tv, 0) || math.IsNaN(tv) || isNegativeZero(tv)
	case float32:
		return math.IsInf(float64(tv), 0) || math.IsNaN(float64(tv)) || isNegativeZero(float64(tv))
	case []byte:
		return true
	case map[string]any:
		for _, val := range tv {
			if needsJSONFixup(val) {
				return true
			}
		}
	case []any:
		return slices.ContainsFunc(tv, needsJSONFixup)
	}
	return false
}

func applyJSONFixup(v any) any {
	switch tv := v.(type) {
	case float64:
		if math.IsInf(tv, 1) {
			return json.Number("1e1000")
		}
		if math.IsInf(tv, -1) {
			return json.Number("-1e1000")
		}
		if math.IsNaN(tv) {
			return "NaN"
		}
		if isNegativeZero(tv) {
			// encoding/json.Marshal renders -0.0 as integer-syntax "-0",
			// which re-parses to a sign-less integer 0 (+0.0 on the wire) —
			// silently flipping the rebuilt schema's default away from the
			// original -0.0. Emit float syntax so Root().Schema() round-trips
			// the sign (matching the wire and Java/fastavro).
			return json.Number("-0.0")
		}
		return tv
	case float32:
		if math.IsInf(float64(tv), 1) {
			return json.Number("1e1000")
		}
		if math.IsInf(float64(tv), -1) {
			return json.Number("-1e1000")
		}
		if math.IsNaN(float64(tv)) {
			return "NaN"
		}
		if isNegativeZero(float64(tv)) {
			return json.Number("-0.0")
		}
		return tv
	case []byte:
		return bytesToAvroJSONString(tv)
	case map[string]any:
		out := make(map[string]any, len(tv))
		for k, val := range tv {
			out[k] = applyJSONFixup(val)
		}
		return out
	case []any:
		out := make([]any, len(tv))
		for i, val := range tv {
			out[i] = applyJSONFixup(val)
		}
		return out
	}
	return v
}

// valueNestsTooDeep reports whether v — a Props value or a
// SchemaField.Default, an arbitrary user-supplied JSON tree — nests its
// container layers deeper than remaining. It is the value-channel analogue of
// toJSONWalk's structural depth bound: a Props/Default value is handed to
// jsonSerializableValue (needsJSONFixup/applyJSONFixup) and then to
// json.Marshal at [SchemaNode.Schema], none of which bounds recursion, so a
// hand-built value nested far enough overflows the goroutine stack uncatchably
// before Schema's eventual Parse (whose maxSchemaJSONDepth pre-scan would have
// rejected the JSON) can run.
//
// The walk mirrors what json.Marshal recurses into — maps, slices, arrays,
// structs, and pointer/interface indirection — not just the map[string]any /
// []any shapes [Schema.Root] produces. A hand-built node (or a SchemaFor
// CustomType.Schema) can store ANY Go value the map[string]any field accepts;
// a typed container ([]map[string]any, a struct, a []*T chain) marshals just as
// deeply but was invisible to a map[string]any/[]any-only type switch, so it
// reached json.Marshal unbounded. remaining is decremented on EVERY descent
// (indirection included), so the check terminates after at most `remaining`
// recursive calls — it can neither overflow its own stack nor hang on a cyclic
// Go type (type P *P), and []byte/[N]byte (a base64/number scalar to
// json.Marshal, never a nested array) short-circuit.
func valueNestsTooDeep(v any, remaining int) bool {
	return valueNestsTooDeepValue(reflect.ValueOf(v), remaining)
}

func valueNestsTooDeepValue(rv reflect.Value, remaining int) bool {
	if remaining < 0 {
		return true
	}
	switch rv.Kind() {
	case reflect.Interface, reflect.Pointer:
		if rv.IsNil() {
			return false
		}
		return valueNestsTooDeepValue(rv.Elem(), remaining-1)
	case reflect.Map:
		for iter := rv.MapRange(); iter.Next(); {
			if valueNestsTooDeepValue(iter.Value(), remaining-1) {
				return true
			}
		}
	case reflect.Slice, reflect.Array:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return false // []byte/[N]byte → base64/number scalar, not a nested array
		}
		for i := 0; i < rv.Len(); i++ {
			if valueNestsTooDeepValue(rv.Index(i), remaining-1) {
				return true
			}
		}
	case reflect.Struct:
		t := rv.Type()
		for i := 0; i < rv.NumField(); i++ {
			if !t.Field(i).IsExported() {
				continue // json.Marshal skips unexported fields
			}
			if valueNestsTooDeepValue(rv.Field(i), remaining-1) {
				return true
			}
		}
	}
	return false
}

// boundedSerializableValue applies jsonSerializableValue to a Props value or
// SchemaField.Default, but first bounds its nesting depth so neither the
// fixup walk nor the downstream json.Marshal overflows the stack. depth is the
// structural nesting already accrued by toJSONWalk, so the value may add at
// most maxSchemaJSONDepth-depth further levels — keeping the total marshaled
// nesting within the same ceiling the structural walk enforces. A value that
// exceeds it records the error on the dedup path (so [SchemaNode.Schema]
// returns it) and truncates to nil on the bare path (so the marshal cannot
// crash), mirroring toJSONWalk's own over-depth handling.
func boundedSerializableValue(d *deduper, depth int, v any) any {
	if valueNestsTooDeep(v, maxSchemaJSONDepth-depth) {
		if d != nil && d.err == nil {
			d.err = fmt.Errorf("avro: SchemaNode default/property value nests deeper than the supported limit (%d)", maxSchemaJSONDepth)
		}
		return nil
	}
	return jsonSerializableValue(v)
}

// toJSON converts a SchemaNode to a JSON-serializable representation.
// Cycles in n's Items/Values pointers (programmatically constructed)
// are detected and emitted as the cyclic node's name (for named types)
// or nil (for unnamed).
func (n *SchemaNode) toJSON() any {
	return n.toJSONWalk(make(map[*SchemaNode]struct{}), nil, "", 0)
}

// toJSONWalk is the cycle-aware walker shared by toJSON and toJSONDedup.
// visited is threaded through every recursive call so cycles introduced
// via Items / Values pointers terminate — see
// TestRegression_SchemaNodeToJSONCycleSafe for the invariant. When d is
// non-nil it tracks named-type definitions and reports conflicting
// redefinitions; when nil it just emits the JSON tree. enclosingNS is
// the namespace scope at this node's position: named types emit their
// namespace relative to it (omitted when inherited; "namespace":"" when
// a null-namespace type sits inside a namespaced scope — Java's
// Name.writeName escape), and name references emit the fullname so they
// re-bind position-independently.
//
// depth is the structural nesting level (items/values/branches/fields
// descents). The visited map only terminates true *pointer cycles; a
// distinct-node-per-level acyclic chain (a hand-built array<array<…>> a
// million deep) has no repeated pointer, so without this bound the walk
// would recurse until the goroutine stack overflows and the process
// dies uncatchably — before Schema's eventual Parse (which bounds JSON
// bracket nesting at maxSchemaJSONDepth) ever runs. Cap the walk at the
// same maxSchemaJSONDepth ceiling: any tree shallow enough to encode or
// decode sits far below it (the wire codec's own maxDepth is 4× smaller),
// so a usable tree is never rejected, and a deeper one stops with a clean
// error (dedup path) or a truncated subtree Parse then rejects (bare
// path) instead of crashing.
func (n *SchemaNode) toJSONWalk(visited map[*SchemaNode]struct{}, d *deduper, enclosingNS string, depth int) any {
	if depth > maxSchemaJSONDepth {
		if d != nil && d.err == nil {
			d.err = fmt.Errorf("avro: SchemaNode tree nests deeper than the supported limit (%d)", maxSchemaJSONDepth)
		}
		return nil
	}
	if _, cycle := visited[n]; cycle {
		// Cycle through Items/Values back to n. Named types emit the
		// fullname as a reference (the canonical Avro recursive-schema
		// shape). Unnamed cycles are an error in the dedup walker and
		// return nil-stable JSON in the bare walker (snapshot/equality
		// comparison stays meaningful: two equal cyclic subtrees
		// produce the same partial JSON).
		if isNamedKind(n.Type) && n.Name != "" {
			return nodeFullname(n)
		}
		if d != nil && d.err == nil {
			d.err = fmt.Errorf("avro: cyclic SchemaNode detected")
		}
		return nil
	}
	visited[n] = struct{}{}
	defer delete(visited, n)

	// Dedup: named types that have already been emitted become name refs;
	// a redefinition with a different body is reported as a conflict.
	// Keyed by the FULLNAME — equality of names is defined on the
	// fullname (spec, "Names"), so two distinct types sharing a short
	// name across namespaces are not redefinitions of each other. The
	// reference is the fullname too: a dotted reference re-binds exactly
	// anywhere, and a null-namespace type's bare fullname re-binds via
	// the parser's null-namespace fallback. (A bare reference from
	// inside a namespaced scope that ALSO collides with an in-scope
	// short name re-binds in-scope — the same inherent reference
	// ambiguity Java's getQualified/Names.get pair has; references have
	// no "namespace":"" escape syntax.)
	if d != nil && isNamedKind(n.Type) && n.Name != "" {
		if prev, exists := d.defined[nodeFullname(n)]; exists {
			// A repeated fullname becomes a bare name reference. Marshal-
			// compare the bodies only when the two are DISTINCT nodes (a
			// possible conflicting redefinition); a named type referenced
			// multiple times resolves to the same *SchemaNode and is
			// definitionally equal, so it needs no marshal. Deferring the
			// comparison to an actual collision keeps the common all-
			// distinct-names case O(n) instead of marshaling every named
			// type's full subtree eagerly (O(depth*subtree) on nesting).
			if prev != n && d.err == nil {
				cur, _ := json.Marshal(n.toJSON())
				prevB, _ := json.Marshal(prev.toJSON())
				if string(cur) != string(prevB) {
					d.err = fmt.Errorf("avro: conflicting definitions for named type %q", truncForError(nodeFullname(n)))
				}
			}
			return nodeFullname(n)
		}
	}

	// The namespace scope inside this node: a named type opens its own.
	childNS := nsForChildren(n, enclosingNS)

	switch n.Type {
	case "null", "boolean", "int", "long", "float", "double", "string", "bytes":
		if n.LogicalType == "" && len(n.Props) == 0 {
			return n.Type
		}
	case "union":
		branches := make([]any, len(n.Branches))
		for i := range n.Branches {
			branches[i] = n.Branches[i].toJSONWalk(visited, d, childNS, depth+1)
		}
		return branches
	}

	if n.Name == "" && n.Type != "array" && n.Type != "map" &&
		!isNamedKind(n.Type) &&
		n.Type != "union" && n.LogicalType == "" && len(n.Props) == 0 {
		return n.Type
	}

	// Dedup: remember this named type's node for the next occurrence's
	// conflict check. Store the node, not its marshaled body — marshaling
	// every named type eagerly is O(depth*subtree) on nested schemas, and
	// the body is only needed if a duplicate fullname actually appears.
	if d != nil {
		if isNamedKind(n.Type) && n.Name != "" {
			d.defined[nodeFullname(n)] = n
		}
	}

	m := map[string]any{"type": n.Type}
	if n.Name != "" {
		m["name"] = n.Name
	}
	if isNamedKind(n.Type) && n.Name != "" && !strings.Contains(n.Name, ".") {
		// Emit the namespace relative to the enclosing scope, mirroring
		// Java's Name.writeName: omit when equal (re-parse inherits it),
		// "namespace":"" to escape inheritance for a null-namespace type
		// inside a namespaced scope, the value otherwise. A dotted Name
		// carries its own namespace, so no attribute is emitted for it
		// (the spec ignores the attribute when the name is dotted).
		switch eff := n.Namespace; {
		case eff == enclosingNS:
			// inherited (or both null): omit
		case eff == "":
			m["namespace"] = ""
		default:
			m["namespace"] = eff
		}
	} else if n.Namespace != "" && !isNamedKind(n.Type) {
		// Unnamed node with a namespace attribute set by hand: preserve
		// as-written (the parser ignores it; fidelity only).
		m["namespace"] = n.Namespace
	}
	if len(n.Aliases) > 0 {
		m["aliases"] = n.Aliases
	}
	if n.Doc != "" {
		m["doc"] = n.Doc
	}
	if n.HasEnumDefault {
		m["default"] = n.EnumDefault
	}
	if n.LogicalType != "" {
		m["logicalType"] = n.LogicalType
	}
	if n.Precision != 0 {
		m["precision"] = n.Precision
	}
	if n.Scale != 0 {
		m["scale"] = n.Scale
	}
	// fixed.size is a required attribute and 0 is a legal size, so for
	// fixed types it is always emitted — omitting a zero value would make
	// the re-emitted schema unparseable ("fixed is missing size").
	if n.Type == "fixed" {
		m["size"] = n.Size
	} else if n.Size != 0 {
		m["size"] = n.Size
	}
	// enum.symbols is a required attribute per the Avro spec (Complex
	// Types > Enums: "symbols: a JSON array, listing symbols, as JSON
	// strings (required)"), always emit for enum types even when empty.
	if n.Type == "enum" {
		if n.Symbols == nil {
			m["symbols"] = []string{}
		} else {
			m["symbols"] = n.Symbols
		}
	} else if len(n.Symbols) > 0 {
		m["symbols"] = n.Symbols
	}
	if n.Items != nil {
		m["items"] = n.Items.toJSONWalk(visited, d, childNS, depth+1)
	}
	if n.Values != nil {
		m["values"] = n.Values.toJSONWalk(visited, d, childNS, depth+1)
	}
	// record.fields is a required attribute per the Avro spec (Complex
	// Types > Records: "fields: a JSON array, listing fields (required)"),
	// always emit for record/error types even when empty.
	if isRecordKind(n.Type) || len(n.Fields) > 0 {
		fields := make([]map[string]any, len(n.Fields))
		for i, f := range n.Fields {
			fd := map[string]any{
				"name": f.Name,
				"type": f.Type.toJSONWalk(visited, d, childNS, depth+1),
			}
			if f.HasDefault || f.Default != nil {
				// jsonSerializableValue converts ±Inf — which a Root()
				// of "default":1e1000 normalizes to via normalizeJSONNumber
				// → parseFloatAcceptOverflow — back to a json.Number
				// literal so encoding/json.Marshal at SchemaNode.Schema()
				// doesn't fail. Inverse of the metadata-API normalization.
				fd["default"] = boundedSerializableValue(d, depth, f.Default)
			}
			if len(f.Aliases) > 0 {
				fd["aliases"] = f.Aliases
			}
			if f.Order != "" {
				fd["order"] = f.Order
			}
			if f.Doc != "" {
				fd["doc"] = f.Doc
			}
			for k, v := range f.Props {
				fd[k] = boundedSerializableValue(d, depth, v)
			}
			fields[i] = fd
		}
		m["fields"] = fields
	}
	for k, v := range n.Props {
		m[k] = boundedSerializableValue(d, depth, v)
	}
	return m
}

// nodeFromJSON converts a parsed JSON value into a SchemaNode. parentNS
// is the enclosing namespace scope; named types without an explicit
// "namespace" attribute resolve into it (see [SchemaNode].Namespace).
func nodeFromJSON(v any, parentNS string) SchemaNode {
	switch s := v.(type) {
	case string:
		return SchemaNode{Type: s}
	case []any:
		branches := make([]SchemaNode, len(s))
		for i, b := range s {
			branches[i] = nodeFromJSON(b, parentNS)
		}
		return SchemaNode{Type: "union", Branches: branches}
	case map[string]any:
		return nodeFromJSONObject(s, parentNS)
	default:
		return SchemaNode{}
	}
}

// Known schema keys that are NOT custom properties.
var schemaReservedKeys = map[string]bool{
	"type": true, "name": true, "namespace": true, "doc": true,
	"fields": true, "symbols": true, "items": true, "values": true,
	"size": true, "logicalType": true, "precision": true, "scale": true,
	"aliases": true, "default": true, "order": true,
}

// Known field keys that are NOT custom properties.
var fieldReservedKeys = map[string]bool{
	"name": true, "type": true, "default": true, "doc": true,
	"aliases": true, "order": true,
}

// jsonNumericInt accepts a value parsed via unmarshalAnyPreservePrecision
// (int64 for integer literals) and falls through to float64 / json.Number
// for compatibility with values originating from a bare encoding/json
// Unmarshal — primarily SchemaNode trees constructed programmatically
// and round-tripped through Schema().Root().
func jsonNumericInt(v any) (int, bool) {
	switch t := v.(type) {
	case int64:
		return int(t), true
	case float64:
		return int(t), true
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return int(i), true
		}
	case string:
		// The Avro [INTEGERS] rule allows a quoted-string size (e.g.
		// "size":"16"), accepted at parse via laxInt; the metadata tree
		// must read it too, or Root().Size returns 0 and Root().Schema()
		// round-trips to "missing size".
		if len(t) <= maxLaxIntDataLen {
			if i, err := strconv.Atoi(t); err == nil {
				return i, true
			}
		}
	}
	return 0, false
}

// getCIString assigns *dst to m[key] when present and string-typed.
// Mirrors the lookupCI + type-assert pattern repeated ~6 times in
// nodeFromJSONObject and its inner field loop.
func getCIString(m map[string]any, key string, dst *string) {
	if v, ok := lookupCI(m, key); ok {
		if s, ok := v.(string); ok {
			*dst = s
		}
	}
}

// getCIInt assigns *dst to m[key] when present and parseable via
// jsonNumericInt (precision/scale/size).
func getCIInt(m map[string]any, key string, dst *int) {
	if v, ok := lookupCI(m, key); ok {
		if p, ok := jsonNumericInt(v); ok {
			*dst = p
		}
	}
}

// getCIStringSlice assigns *dst to m[key] when it is a []any of strings
// (aliases, symbols).
func getCIStringSlice(m map[string]any, key string, dst *[]string) {
	if v, ok := lookupCI(m, key); ok {
		if vs, ok := v.([]any); ok {
			out := make([]string, len(vs))
			for i, x := range vs {
				out[i], _ = x.(string)
			}
			*dst = out
		}
	}
}

// lookupCI looks up key k in m, matching case-insensitively, so schemas
// with non-canonical casing ("tYpe" instead of "type") round-trip through
// Root/Schema. An EXACT-case match wins first (the common path). When no
// exact match exists but multiple keys collide case-insensitively (e.g.
// both "tYpe" and "TYpe", with no plain "type"), the smallest by Unicode
// code-point wins — a deterministic tie-break for that malformed input
// (Avro keys are case-sensitive per spec, so two case-variants of one key
// are already non-conformant). This differs from a struct-decode's
// document-order resolution on that degenerate case, but is deterministic
// where a bare `for k := range m` was not — Go's randomized map iteration
// otherwise made Root() return different branches on different calls.
func lookupCI(m map[string]any, key string) (any, bool) {
	if v, ok := m[key]; ok {
		return v, true
	}
	var pickKey string
	var found bool
	for k := range m {
		if !strings.EqualFold(k, key) {
			continue
		}
		if !found || k < pickKey {
			pickKey = k
			found = true
		}
	}
	if found {
		return m[pickKey], true
	}
	return nil, false
}

// isRecordKind reports whether typ names the Avro record kind in
// [SchemaNode.Type]. Both "record" and "error" are valid JSON literals
// for the same on-wire kind (the Avro RPC convention names error-record
// types with "error"); the schema builder normalizes both to
// node.kind=="record" at schema.go's `case "record", "error":` arm.
// SchemaNode.Type preserves the JSON-as-written name, so any
// metadata-API dispatcher on SchemaNode.Type that branches on the
// record kind must accept either alias — this helper centralizes the
// predicate so the alias set can't drift across call sites.
func isRecordKind(typ string) bool {
	return typ == "record" || typ == "error"
}

// isNamedKind reports whether typ is one of the four Avro named-type kinds
// (record / error / enum / fixed) — the set that carries a Name and can be
// referenced, deduped, and aliased. "error" is the record alias and must
// always travel with "record" here. Centralizes the four-element set so the
// many dedup / name-validation / alias call sites can't drift (the named-type
// analogue of [isRecordKind]).
func isNamedKind(typ string) bool {
	return typ == "record" || typ == "error" || typ == "enum" || typ == "fixed"
}

// coerceMetadataDefault is the metadata-API parallel of [coerceDefault]
// (schema.go). It transforms a parsed-JSON default value into the
// canonical Go form the wire-encode pipeline materializes for that
// field type — so SchemaField.Default surfaces a value type matching
// the wire bytes rather than the raw JSON form
// unmarshalAnyPreservePrecision returns.
//
// Currently:
//   - int / long / float / double fields → schema-width-faithful Go
//     type (int32 / int64 / float32 / float64), matching Java's
//     JacksonUtils.toObject(jsonNode, schema) at JacksonUtils.java:150-155.
//     Schema parse rejects out-of-range integer defaults so int32/int64
//     narrowing is lossless; float32 narrowing of finite-overflow
//     inputs surfaces ±Inf (matching the wire bytes).
//   - string defaults for bytes/fixed fields → []byte via Avro's
//     codepoint-per-byte mapping (mirrors [convertDefaultBytes] in
//     schema.go, which produces the same []byte for the wire-encode
//     pipeline's internal defaultVal). Without this conversion, a
//     metadata-API consumer doing
//     `defs[f.Name] = f.Default; s.Encode(defs)` succeeds for every
//     bytes/fixed default EXCEPT fixed+uuid (the encoder's UUID arm
//     hard-fails parseUUID on the 16-codepoint wire-form string), and
//     the round-trip contract breaks asymmetrically. Converting to
//     []byte here brings every bytes/fixed default into a form the
//     encoder accepts uniformly (raw bytes via serSize/serBytes /
//     the JSON fixed/string-slice/array arm).
//
// Walks unions (Avro 1.12: union default may match any branch) and
// nested record/array/map types. For single-field float/double fields
// with a string-form numeric default, this coerces the string to
// float32/float64 — Java parity with parseField's text→DoubleNode
// coercion at Schema.java:1899-1902, scoped to outer FLOAT/DOUBLE
// field types only. For union branches the coercion does NOT fire
// (matching Java's isValidDefault for the union arm, avro-rs's
// resolve_internal, and goavro's strict type assertions): union+
// numeric-string defaults are rejected at schema parse, so they never
// reach this function.
//
// Non-numeric / non-string defaults and non-handled types pass through
// unchanged.
func coerceMetadataDefault(val any, t *SchemaNode, table map[string]*SchemaNode, ns string) any {
	if t == nil {
		return val
	}
	// Name-ref resolution: when the caller passes a non-nil name-table
	// and t.Type is a bare name-reference (e.g. "Inner"), resolve to
	// the actual named SchemaNode and recurse — inside the TARGET's own
	// namespace scope. table == nil means the caller is doing
	// best-effort inline coercion only — used by the synchronous call
	// during nodeFromJSON construction where the full tree (and
	// therefore the name-table) isn't available yet.
	if resolved := lookupNameRef(t, table, ns); resolved != nil {
		return coerceMetadataDefault(val, resolved, table, nodeEffNS(resolved))
	}
	if t.Type == "union" {
		// Best-effort first pass (table == nil): name-referenced branches
		// can't be resolved yet, so a greedy earlier branch (e.g. a bytes
		// branch accepting a string) would destructively coerce the value
		// (string to []byte) and lock out the correct name-ref branch (e.g.
		// an enum) that the table-populated pass would have picked — the
		// enum arm only accepts a string, never a []byte, so the value can
		// never be reclaimed. Defer ALL union branch selection to
		// coerceTreeDefaults, which runs with the name table populated
		// (Schema.Root); leave the raw value untouched here.
		if table == nil {
			return val
		}
		// Pick the FIRST branch that accepts val's Go type — matches
		// the wire-encode pipeline's coerceDefault (which uses
		// validateDefault for branch selection) and Java's Schema.
		// parseField (which Jackson-coerces against the first
		// accepting branch). Picking "first transformation" instead
		// would diverge for ["string","float"] with default "1.5":
		// wire picks string (first accept), but a transform-based
		// helper would pick float because string→string is a no-op.
		if branch := firstMetadataBranchAcceptingDefault(t, val, table, ns); branch != nil {
			return coerceMetadataDefault(val, branch, table, nsForChildren(branch, ns))
		}
		return val
	}
	if val == nil {
		return val
	}
	if t.Type == "int" {
		// Schema-width-faithful narrowing: int defaults surface as
		// int32 so SchemaField.Default's Go type matches the wire
		// width AND the user's natural Go field type (`Foo int32
		// `avro:"default=42"`` → Default.(int32) works directly).
		// Schema parse rejects out-of-int32 defaults via defaultAsInt32,
		// so the narrowing is always lossless here.
		switch val := val.(type) {
		case int32:
			return val
		case int64:
			return int32(val)
		case json.Number, string, float64:
			if n, err := defaultAsInt32(val); err == nil {
				return n
			}
			return val
		}
		return val
	}
	if t.Type == "long" {
		// Coerce non-int64 numeric inputs (json.Number, string,
		// float64-whole-number) to int64. int64 inputs pass through.
		// Schema parse rejects out-of-int64 via defaultAsInt64.
		switch val := val.(type) {
		case int64:
			return val
		case int32:
			return int64(val)
		case json.Number, string, float64:
			if n, err := defaultAsInt64(val); err == nil {
				return n
			}
			return val
		}
		return val
	}
	if t.Type == "float" || t.Type == "double" {
		// Schema-width-faithful narrowing per Java's JacksonUtils.toObject
		// (lang/java/avro/src/main/java/org/apache/avro/util/internal/
		// JacksonUtils.java:150-155): "float" schema → float32,
		// "double" schema → float64. The wire-faithful narrowing means
		// Default == the value the wire encoder will emit, including
		// for finite-overflow inputs (`{"default":1e100,"type":"float"}`
		// → metadata float32(+Inf), wire +Inf bits) and integer-form
		// inputs whose magnitude exceeds the mantissa (silently IEEE-
		// rounded). Users get Default.(float32) for float fields and
		// Default.(float64) for double fields, matching their Go field
		// types directly.
		//
		// String inputs are handled inline via parseFloatAcceptOverflow
		// rather than through [defaultAsFloat], which is now strict
		// (no string arm) so it can be reused at union-branch
		// matching and encode-time arms without accepting strings
		// where the spec says it shouldn't. This single-field arm
		// mirrors [coerceDefault]'s parseField-style text→float
		// coercion (schema.go) for outer FLOAT/DOUBLE schemas.
		var f float64
		switch val := val.(type) {
		case float64:
			f = val
		case float32:
			f = float64(val)
		case string:
			var err error
			if f, err = parseFloatAcceptOverflow(val, 64); err != nil {
				return val
			}
		case int64, int32, json.Number:
			var err error
			if f, err = defaultAsFloat(val); err != nil {
				return val
			}
		default:
			return val
		}
		if t.Type == "float" {
			return float32(f)
		}
		return f
	}
	if t.Type == "bytes" || t.Type == "fixed" {
		if s, ok := val.(string); ok {
			if b, err := avroJSONBytesToBytes(s); err == nil {
				return b
			}
		}
		return val
	}
	if isRecordKind(t.Type) {
		if m, ok := val.(map[string]any); ok {
			childNS := nsForChildren(t, ns)
			out := make(map[string]any, len(m))
			for k, v := range m {
				inner := v
				for i := range t.Fields {
					if t.Fields[i].Name == k {
						inner = coerceMetadataDefault(v, &t.Fields[i].Type, table, childNS)
						break
					}
				}
				out[k] = inner
			}
			return out
		}
		return val
	}
	if t.Type == "array" && t.Items != nil {
		if a, ok := val.([]any); ok {
			out := make([]any, len(a))
			for i, v := range a {
				out[i] = coerceMetadataDefault(v, t.Items, table, ns)
			}
			return out
		}
		return val
	}
	if t.Type == "map" && t.Values != nil {
		if m, ok := val.(map[string]any); ok {
			out := make(map[string]any, len(m))
			for k, v := range m {
				out[k] = coerceMetadataDefault(v, t.Values, table, ns)
			}
			return out
		}
		return val
	}
	return val
}

// nodeEffNS returns n's effective namespace: a dotted Name carries its
// own namespace (taking precedence per the spec); otherwise Namespace,
// which is already resolved ("" means the null namespace).
func nodeEffNS(n *SchemaNode) string {
	if i := strings.LastIndexByte(n.Name, '.'); i >= 0 {
		return n.Name[:i]
	}
	return n.Namespace
}

// nodeFullname returns n's fullname: the dotted Name verbatim, or the
// resolved namespace joined with the name.
func nodeFullname(n *SchemaNode) string {
	if strings.Contains(n.Name, ".") {
		return n.Name
	}
	if n.Namespace != "" {
		return n.Namespace + "." + n.Name
	}
	return n.Name
}

// nsForChildren returns the namespace scope in effect inside n: a named
// type opens its own scope; unnamed nodes pass the enclosing scope
// through.
func nsForChildren(n *SchemaNode, enclosing string) string {
	if n != nil && n.Name != "" {
		return nodeEffNS(n)
	}
	return enclosing
}

// lookupNameRef returns the named target of t if t.Type is a name
// reference (not a structural or primitive kind) AND table has it, else
// nil. A nil table always returns nil (synchronous-build callers disable
// name-ref resolution because the tree isn't fully walked yet). ns is
// the enclosing namespace scope at the reference site; the key order
// comes from scopedRefKeys (schema.go) so the metadata binding cannot
// drift from the wire's.
func lookupNameRef(t *SchemaNode, table map[string]*SchemaNode, ns string) *SchemaNode {
	if t == nil || table == nil {
		return nil
	}
	// Structural kinds (primitives, "record"/"error", "enum", "fixed",
	// "array", "map", "union") are schema definitions, not name-ref
	// targets. "error" is in this list per [isRecordKind] — without it,
	// a SchemaNode with t.Type=="error" would fall through to
	// table["error"] and wrongly resolve to any record literally named
	// "error" in the schema.
	switch t.Type {
	case "null", "boolean", "int", "long", "float", "double",
		"bytes", "string", "record", "error", "enum", "fixed", "array", "map", "union":
		return nil
	}
	var keys [2]string
	for _, k := range scopedRefKeys(&keys, t.Type, ns) {
		if r, ok := table[k]; ok {
			return r
		}
	}
	return nil
}

// fixupNameRefDefaults walks the SchemaNode tree once to populate a
// name-table of every reachable record/enum/fixed, then re-coerces
// HasDefault fields with the table so name-referenced defaults (and
// defaults whose union contains a name-ref branch) materialize the
// way inline-typed siblings already do via the synchronous coerce.
func fixupNameRefDefaults(root *SchemaNode) {
	table := map[string]*SchemaNode{}
	collectNamedTypes(root, table)
	if len(table) == 0 {
		return
	}
	coerceTreeDefaults(root, table, "")
}

func collectNamedTypes(n *SchemaNode, table map[string]*SchemaNode) {
	if n == nil {
		return
	}
	if n.Name != "" { // record / enum / fixed
		// Namespace is resolved at construction (see [SchemaNode]), so
		// the fullname is direct — no inheritance walk needed here.
		// Register exactly what the wire builder registers: every type
		// under its fullname ONLY. A null-namespace type's fullname IS
		// its bare name, so it owns the bare key; also registering
		// namespaced types under their short name would make the bare
		// key last-walked-wins, binding a bare reference at
		// null-namespace scope to a different type than the wire bound
		// whenever short names collide.
		table[nodeFullname(n)] = n
	}
	if n.Items != nil {
		collectNamedTypes(n.Items, table)
	}
	if n.Values != nil {
		collectNamedTypes(n.Values, table)
	}
	for i := range n.Fields {
		collectNamedTypes(&n.Fields[i].Type, table)
	}
	for i := range n.Branches {
		collectNamedTypes(&n.Branches[i], table)
	}
}

func coerceTreeDefaults(n *SchemaNode, table map[string]*SchemaNode, ns string) {
	if n == nil {
		return
	}
	childNS := nsForChildren(n, ns)
	for i := range n.Fields {
		f := &n.Fields[i]
		if f.HasDefault {
			f.Default = coerceMetadataDefault(f.Default, &f.Type, table, childNS)
		}
		coerceTreeDefaults(&f.Type, table, childNS)
	}
	if n.Items != nil {
		coerceTreeDefaults(n.Items, table, childNS)
	}
	if n.Values != nil {
		coerceTreeDefaults(n.Values, table, childNS)
	}
	for i := range n.Branches {
		coerceTreeDefaults(&n.Branches[i], table, childNS)
	}
}

// defaultMatchesBytesOrFixedKind mirrors the wire-encode pipeline's
// validateLeaf for "bytes" / "fixed" branches: codepoint range
// (validateAvroByteString) and, for "fixed", the exact-rune-count
// size match. The metadata-side branch selection must enforce the
// same codepoint range and fixed-size constraint as the wire path
// or [fixed:8,"string"] with a 4-char default would metadata-match
// fixed (string-kind only) while wire matches string (size check
// rejects fixed) — pattern 14a sibling of the convertDefaultBytes/
// validateDefault delegation at
// TestRegression_UnionBytesFixedDefaultMisroutedToWrongBranch.
func defaultMatchesBytesOrFixedKind(t *SchemaNode, val any) bool {
	switch v := val.(type) {
	case string:
		if err := validateAvroByteString(v, t.Type); err != nil {
			return false
		}
		if t.Type == "fixed" && len([]rune(v)) != t.Size {
			return false
		}
		return true
	case []byte:
		if t.Type == "fixed" && len(v) != t.Size {
			return false
		}
		return true
	}
	return false
}

// branchAcceptsDefault reports whether the Avro type t natively accepts
// val as a default value, using the same Go-type → Avro-type
// compatibility the wire-encode pipeline's validateDefault enforces.
// Used by coerceMetadataDefault's union-branch selection: iterate
// branches in order; the first accepting branch is the chosen one
// (matches Java's Schema.parseField first-matching-branch behavior).
//
// The numeric arms (int/long/float/double) delegate to the wire-encode
// pipeline's defaultAsInt32 / defaultAsInt64 / defaultAsFloat so the
// metadata branch selector and the wire branch selector apply the same
// per-value predicates (int32-bounds, whole-number, JSON-grammar,
// ParseFloat-ErrRange-with-Inf). float/double accept any numeric input
// per the lossy-destination policy (matching Java/fastavro), so
// ["float","int"] default 42 picks the float branch (first match) on
// both surfaces.
//
// The float/double arm rejects string defaults at union-branch matching
// (Java parity: parseField text→DoubleNode coercion at
// Schema.java:1899-1902 fires only for OUTER FLOAT/DOUBLE field types,
// not union branches). bytes/fixed branch accepts string (codepoint-
// mapped form per Avro JSON spec) or []byte.
//
// The structural arms (record/error, array, map) recurse into children
// so per-element validity mirrors the wire-side walkDefault. The record
// arm enforces required-field-no-default presence; the array/map arms
// require every item/value to itself accept against items/values.
// Without these per-element checks, a union like [{record needing X},
// {record needing nothing}] with default {} metadata-matches the first
// branch (type-only) while the wire path picks the second (Java parity);
// users type-switching on Default see a Go type that contradicts the
// wire-decoded auto-fill.
func branchAcceptsDefault(t *SchemaNode, val any, table map[string]*SchemaNode, ns string) bool {
	// Resolve a bare name-reference if the caller supplied a name-table.
	if resolved := lookupNameRef(t, table, ns); resolved != nil {
		return branchAcceptsDefault(resolved, val, table, nodeEffNS(resolved))
	}
	switch t.Type {
	case "null":
		return val == nil
	case "boolean":
		_, ok := val.(bool)
		return ok
	case "int":
		_, err := defaultAsInt32(val)
		return err == nil
	case "long":
		_, err := defaultAsInt64(val)
		return err == nil
	case "float", "double":
		_, err := defaultAsFloat(val)
		return err == nil
	case "string":
		_, ok := val.(string)
		return ok
	case "enum":
		sym, ok := val.(string)
		if !ok {
			return false
		}
		// Wire-side validateLeaf for enum rejects non-member symbols
		// (schema.go's enum arm at the `slices.Contains(node.symbols,
		// sym)` check). Enum needs its own arm — falling into the
		// string arm accepts any string regardless of symbol membership,
		// so a union [enum:{A,B}, bytes] with default "Z" would
		// metadata-match enum (type-only) while wire rejects enum and
		// picks bytes. Membership is unconditional, mirroring the wire
		// side: an empty enum accepts no default, so the union walk must
		// fall through to a later branch exactly as the wire side does
		// (a name-referenced enum branch reaches here only after
		// lookupNameRef resolution, with its symbols final; the
		// table-nil construction pass defers union selection entirely).
		return slices.Contains(t.Symbols, sym)
	case "bytes", "fixed":
		return defaultMatchesBytesOrFixedKind(t, val)
	case "record", "error":
		m, ok := val.(map[string]any)
		if !ok {
			return false
		}
		childNS := nsForChildren(t, ns)
		for i := range t.Fields {
			f := &t.Fields[i]
			fv, present := m[f.Name]
			if !present {
				if !f.HasDefault {
					return false
				}
				continue
			}
			if !branchAcceptsDefault(&f.Type, fv, table, childNS) {
				return false
			}
		}
		return true
	case "array":
		arr, ok := val.([]any)
		if !ok {
			return false
		}
		if t.Items == nil {
			return true
		}
		for _, item := range arr {
			if !branchAcceptsDefault(t.Items, item, table, ns) {
				return false
			}
		}
		return true
	case "map":
		m, ok := val.(map[string]any)
		if !ok {
			return false
		}
		if t.Values == nil {
			return true
		}
		for _, v := range m {
			if !branchAcceptsDefault(t.Values, v, table, ns) {
				return false
			}
		}
		return true
	case "union":
		return firstMetadataBranchAcceptingDefault(t, val, table, ns) != nil
	}
	return false
}

// firstMetadataBranchAcceptingDefault returns the first branch of the
// union t whose [branchAcceptsDefault] accepts val (with name-ref
// resolution applied to each branch), or nil if no branch accepts.
// Mirrors the wire-side [firstUnionBranchAcceptingDefault] (schema.go)
// on the *SchemaNode public type — the two implement Avro's "first
// matching branch wins" default-resolution rule (1.12 relaxed from
// "first branch" to "any branch," with deterministic first-match
// tie-break) on opposite sides of the dual-namespace boundary.
//
// Shared by [coerceMetadataDefault] (returns the resolved branch so
// the coerce step recurses against it) and [branchAcceptsDefault]'s
// union arm (returns nil/non-nil for the accept predicate).
func firstMetadataBranchAcceptingDefault(t *SchemaNode, val any, table map[string]*SchemaNode, ns string) *SchemaNode {
	for i := range t.Branches {
		branch := &t.Branches[i]
		if resolved := lookupNameRef(branch, table, ns); resolved != nil {
			branch = resolved
		}
		if branchAcceptsDefault(branch, val, table, nsForChildren(branch, ns)) {
			return branch
		}
	}
	return nil
}

func nodeFromJSONObject(m map[string]any, parentNS string) SchemaNode {
	n := SchemaNode{}

	getCIString(m, "type", &n.Type)
	getCIString(m, "name", &n.Name)
	// Namespace resolves at build: an explicit attribute wins (including
	// the explicit-empty "namespace":"" null-namespace form — a DIFFERENT
	// type than one inheriting the enclosing namespace, per the spec's
	// fullname rules); an undotted named type without the attribute
	// inherits the enclosing scope. A dotted name carries its own
	// namespace; any attribute alongside it is preserved as-written for
	// fidelity but ignored, exactly as the parser ignores it.
	explicitNS, hasExplicitNS := "", false
	if v, ok := lookupCI(m, "namespace"); ok {
		if s, ok := v.(string); ok {
			explicitNS, hasExplicitNS = s, true
		}
	}
	switch {
	case n.Name != "" && !strings.Contains(n.Name, "."):
		if hasExplicitNS {
			n.Namespace = explicitNS
		} else {
			n.Namespace = parentNS
		}
	case hasExplicitNS:
		n.Namespace = explicitNS
	}
	childNS := nsForChildren(&n, parentNS)
	getCIString(m, "doc", &n.Doc)
	getCIString(m, "logicalType", &n.LogicalType)
	// precision/scale/size are int per spec. After
	// unmarshalAnyPreservePrecision, integer JSON literals come back as
	// int64 (not float64); jsonNumericInt accepts both.
	getCIInt(m, "precision", &n.Precision)
	getCIInt(m, "scale", &n.Scale)
	getCIInt(m, "size", &n.Size)
	getCIStringSlice(m, "aliases", &n.Aliases)
	getCIStringSlice(m, "symbols", &n.Symbols)

	if v, ok := lookupCI(m, "default"); ok && n.Type == "enum" {
		if d, ok := v.(string); ok {
			n.EnumDefault = d
			n.HasEnumDefault = true
		}
	}

	if items, ok := lookupCI(m, "items"); ok {
		node := nodeFromJSON(items, childNS)
		n.Items = &node
	}
	if values, ok := lookupCI(m, "values"); ok {
		node := nodeFromJSON(values, childNS)
		n.Values = &node
	}

	if v, ok := lookupCI(m, "fields"); ok {
		if fields, ok := v.([]any); ok {
			n.Fields = make([]SchemaField, len(fields))
			for i, f := range fields {
				fm, _ := f.(map[string]any)
				sf := SchemaField{}
				getCIString(fm, "name", &sf.Name)
				if t, ok := lookupCI(fm, "type"); ok {
					sf.Type = nodeFromJSON(t, childNS)
				}
				if d, ok := lookupCI(fm, "default"); ok {
					// Coerce string defaults to typed float64 for
					// float/double fields (and recurse through nested
					// record/array/map/union types), matching Java's
					// Schema.parseField text→DoubleNode coercion and
					// the wire-encode pipeline's coerceDefault — so
					// SchemaField.Default reflects the materialized
					// wire form instead of the raw JSON string.
					// nil name-table: best-effort inline coercion only;
					// fixupNameRefDefaults (called at the end of Root)
					// re-coerces with a populated table to resolve
					// name-references that aren't visible during this
					// per-field construction.
					sf.Default = coerceMetadataDefault(d, &sf.Type, nil, "")
					sf.HasDefault = true
				}
				getCIString(fm, "doc", &sf.Doc)
				getCIStringSlice(fm, "aliases", &sf.Aliases)
				getCIString(fm, "order", &sf.Order)
				for k, v := range fm {
					if fieldReservedKeyCI(k) {
						continue
					}
					if sf.Props == nil {
						sf.Props = make(map[string]any)
					}
					sf.Props[k] = v
				}
				n.Fields[i] = sf
			}
		}
	}

	// Collect custom properties (anything not in the reserved set).
	for k, v := range m {
		if schemaReservedKeyCI(k) {
			continue
		}
		if n.Props == nil {
			n.Props = make(map[string]any)
		}
		n.Props[k] = v
	}

	return n
}

// reservedKeyCI is a case-insensitive wrapper for membership in a
// reserved-key map. Shared by fieldReservedKeyCI / schemaReservedKeyCI
// so the case-insensitive fall-through scan lives in one place.
func reservedKeyCI(k string, reserved map[string]bool) bool {
	if reserved[k] {
		return true
	}
	for rk := range reserved {
		if strings.EqualFold(k, rk) {
			return true
		}
	}
	return false
}

func fieldReservedKeyCI(k string) bool  { return reservedKeyCI(k, fieldReservedKeys) }
func schemaReservedKeyCI(k string) bool { return reservedKeyCI(k, schemaReservedKeys) }
