package avro

import (
	"encoding/json"
	"fmt"
	"math"
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

	Name      string   // name for record, enum, fixed
	Namespace string   // namespace for named types
	Aliases   []string // alternate names for named types (record, enum, fixed)
	Doc       string   // documentation string

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

	// Props holds custom (non-reserved) schema properties. Integer JSON
	// literals decode to int64 (json.Number when the magnitude exceeds
	// int64); fractional and exponent-form literals decode to float64,
	// with ±Inf for magnitudes overflowing the float64 exponent (e.g.
	// "1e1000" → float64(+Inf), matching Java's Jackson DoubleNode and
	// fastavro's Python float()).
	//
	// DoS-defense cap: numeric literals exceeding maxParseFloatLen
	// (1024) chars fall back to json.Number rather than float64 to
	// bound strconv.ParseFloat cost on hostile multi-MB inputs. Java
	// and fastavro have no analogous cap; twmb's defense-in-depth
	// posture (see "DoS-resistance defense-in-depth" intentional
	// divergence) makes this trade explicitly: legitimate numbers fit
	// comfortably under 1024 chars; truly large literals are preserved
	// losslessly as json.Number for callers that need them.
	Props map[string]any
}

// SchemaField represents a field in an Avro record schema.
type SchemaField struct {
	Name string     // field name
	Type SchemaNode // field schema

	// Default is the field's default value, meaningful only when HasDefault
	// is true. Numbers decode as in [SchemaNode.Props]. Bytes and fixed
	// defaults decode as []byte (the wire form): the JSON schema spec
	// writes them as code-point-per-byte strings, and Default exposes the
	// already-mapped raw bytes so the value can be re-encoded directly
	// (s.AppendEncode(defaultsMap, ...) succeeds for any default,
	// including fixed+uuid whose encoder arm rejects the codepoint-string
	// form via strict parseUUID gating). Enum defaults stay as the symbol
	// string; record/array/map defaults stay as their parsed map/slice
	// shape with each leaf coerced by these same rules.
	//
	// Union defaults: the chosen branch's natural Go type wins over the
	// JSON-literal-form normalization rule. The metadata path uses the
	// same first-matching-branch selector as the wire-encode path
	// (defaultAsInt32 / defaultAsInt64 / defaultAsFloat applied to each
	// branch in declaration order). For ["float","int"] default 42 (an
	// integer literal that would otherwise normalize to int64), the
	// float branch matches first and Default surfaces as float64(42) —
	// type-switching on Default identifies the chosen branch the same
	// way it identifies the underlying Avro type for a non-union field.
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
// tree are automatically deduplicated: the first occurrence emits the
// full definition and subsequent occurrences emit a name reference.
func (n *SchemaNode) Schema() (*Schema, error) {
	d := &deduper{
		defined: make(map[string]string),
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
	return Parse(string(b))
}

// deduper tracks named type definitions during toJSONDedup and records
// conflicting redefinitions. It also detects cycles introduced via
// *SchemaNode Items/Values pointers (which are the only way a SchemaNode
// tree can have true cycles — Fields and Branches are value slices).
type deduper struct {
	defined map[string]string        // name → marshaled JSON of first definition
	visited map[*SchemaNode]struct{} // seen *SchemaNode pointers (cycle detection)
	err     error                    // first conflict or cycle encountered
}

// Root returns the SchemaNode representation of a parsed schema by
// re-parsing the original schema JSON. This preserves all metadata
// including doc strings, namespaces, custom properties, and numeric
// defaults — JSON integer literals come back as int64 (or json.Number
// for the rare ones that overflow int64), not float64; see
// unmarshalAnyPreservePrecision for the precision-preservation rationale.
//
// Root re-parses the JSON on each call. Cache the result if you need
// to access it repeatedly (e.g. in a per-message processing loop).
func (s *Schema) Root() SchemaNode {
	raw, err := unmarshalAnyPreservePrecision([]byte(s.full))
	if err != nil {
		panic("avro: Schema.Root: invalid stored JSON: " + err.Error())
	}
	n := nodeFromJSON(raw)
	fixupNameRefDefaults(&n)
	return n
}

// toJSONDedup is like toJSON but deduplicates named types. The first
// occurrence of a named type (record, enum, fixed) emits the full
// definition; subsequent occurrences emit the name as a reference.
func (n *SchemaNode) toJSONDedup(d *deduper) any {
	return n.toJSONWalk(d.visited, d)
}

// jsonSerializableValue returns v with two Avro-JSON-specific shape
// fixups applied (directly or under map[string]any / []any container
// layers):
//
//  1. ±Inf float → [json.Number]("±1e1000") literal that re-parses to
//     the same value via [parseFloatAcceptOverflow] (schema.go). The
//     inverse of normalizeJSONNumber's ErrRange-with-Inf accept.
//     Required because [encoding/json.Marshal] unconditionally rejects
//     ±Inf and NaN, so a SchemaNode obtained from [Schema.Root] for a
//     schema whose Default / Props normalized an exponent-form overflow
//     to ±Inf cannot otherwise round-trip through [SchemaNode.Schema].
//
//  2. []byte → codepoint-per-byte string (each byte 0x00-0xFF becomes
//     a rune at the same code point). The inverse of
//     [avroJSONBytesToBytes] / [coerceMetadataDefault]'s bytes/fixed
//     arm: that arm materializes Default as []byte (the wire form);
//     re-emitting requires putting it back in the Avro JSON
//     codepoint-string form (the spec form for bytes/fixed defaults).
//     Plain [encoding/json.Marshal] would base64-encode the slice
//     ("AQID" for {0x01,0x02,0x03}) which the Avro parser would then
//     re-read as raw bytes [0x41,0x51,0x49,0x44] — a silent value
//     corruption breaking [SchemaNode.Schema] round-trips for any
//     bytes/fixed default. Programmatically-constructed Props with
//     []byte values also get the codepoint encoding (Avro's
//     convention), not Go's base64 default; users who need base64 in
//     Props should pre-encode to a string.
//
// Container values (map[string]any, []any) are deep-copied only when a
// descendant requires conversion, so the common no-fixup case is
// allocation-free and the user's SchemaNode storage is never mutated.
//
// NaN is intentionally left untouched: no JSON literal re-parses to NaN,
// so json.Marshal's UnsupportedValueError is the honest answer. Users
// who put NaN in Default/Props must convert to the string-form "NaN"
// themselves (defaultAsFloat accepts the string-form for float/double
// fields).
func jsonSerializableValue(v any) any {
	if !needsJSONFixup(v) {
		return v
	}
	return applyJSONFixup(v)
}

func needsJSONFixup(v any) bool {
	switch tv := v.(type) {
	case float64:
		return math.IsInf(tv, 0)
	case float32:
		return math.IsInf(float64(tv), 0)
	case []byte:
		return true
	case map[string]any:
		for _, val := range tv {
			if needsJSONFixup(val) {
				return true
			}
		}
	case []any:
		for _, val := range tv {
			if needsJSONFixup(val) {
				return true
			}
		}
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
		return tv
	case float32:
		if math.IsInf(float64(tv), 1) {
			return json.Number("1e1000")
		}
		if math.IsInf(float64(tv), -1) {
			return json.Number("-1e1000")
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

// toJSON converts a SchemaNode to a JSON-serializable representation.
// Cycles in n's Items/Values pointers (programmatically constructed)
// are detected and emitted as the cyclic node's name (for named types)
// or nil (for unnamed).
func (n *SchemaNode) toJSON() any {
	return n.toJSONWalk(make(map[*SchemaNode]struct{}), nil)
}

// toJSONWalk is the cycle-aware walker shared by toJSON and toJSONDedup.
// visited is threaded through every recursive call so cycles introduced
// via Items / Values pointers terminate — see
// TestRegression_SchemaNodeToJSONCycleSafe for the invariant. When d is
// non-nil it tracks named-type definitions and reports conflicting
// redefinitions; when nil it just emits the JSON tree.
func (n *SchemaNode) toJSONWalk(visited map[*SchemaNode]struct{}, d *deduper) any {
	if _, cycle := visited[n]; cycle {
		// Cycle through Items/Values back to n. Named types emit the
		// name as a reference (the canonical Avro recursive-schema
		// shape). Unnamed cycles are an error in the dedup walker and
		// return nil-stable JSON in the bare walker (snapshot/equality
		// comparison stays meaningful: two equal cyclic subtrees
		// produce the same partial JSON).
		switch n.Type {
		case "record", "error", "enum", "fixed":
			if n.Name != "" {
				return n.Name
			}
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
	if d != nil {
		switch n.Type {
		case "record", "error", "enum", "fixed":
			if n.Name != "" {
				if prev, exists := d.defined[n.Name]; exists {
					cur, _ := json.Marshal(n.toJSON())
					if string(cur) != prev && d.err == nil {
						d.err = fmt.Errorf("avro: conflicting definitions for named type %q", n.Name)
					}
					return n.Name
				}
			}
		}
	}

	switch n.Type {
	case "null", "boolean", "int", "long", "float", "double", "string", "bytes":
		if n.LogicalType == "" && len(n.Props) == 0 {
			return n.Type
		}
	case "union":
		branches := make([]any, len(n.Branches))
		for i := range n.Branches {
			branches[i] = n.Branches[i].toJSONWalk(visited, d)
		}
		return branches
	}

	if n.Name == "" && n.Type != "array" && n.Type != "map" &&
		n.Type != "record" && n.Type != "error" && n.Type != "enum" && n.Type != "fixed" &&
		n.Type != "union" && n.LogicalType == "" && len(n.Props) == 0 {
		return n.Type
	}

	// Dedup: remember this named type's canonical body for the next
	// occurrence's conflict check.
	if d != nil {
		switch n.Type {
		case "record", "error", "enum", "fixed":
			if n.Name != "" {
				b, _ := json.Marshal(n.toJSON())
				d.defined[n.Name] = string(b)
			}
		}
	}

	m := map[string]any{"type": n.Type}
	if n.Name != "" {
		m["name"] = n.Name
	}
	if n.Namespace != "" {
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
	if n.Size != 0 {
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
		m["items"] = n.Items.toJSONWalk(visited, d)
	}
	if n.Values != nil {
		m["values"] = n.Values.toJSONWalk(visited, d)
	}
	// record.fields is a required attribute per the Avro spec (Complex
	// Types > Records: "fields: a JSON array, listing fields (required)"),
	// always emit for record/error types even when empty.
	if isRecordKind(n.Type) || len(n.Fields) > 0 {
		fields := make([]map[string]any, len(n.Fields))
		for i, f := range n.Fields {
			fd := map[string]any{
				"name": f.Name,
				"type": f.Type.toJSONWalk(visited, d),
			}
			if f.HasDefault || f.Default != nil {
				// jsonSerializableValue converts ±Inf — which a Root()
				// of "default":1e1000 normalizes to via normalizeJSONNumber
				// → parseFloatAcceptOverflow — back to a json.Number
				// literal so encoding/json.Marshal at SchemaNode.Schema()
				// doesn't fail. Inverse of the metadata-API normalization.
				fd["default"] = jsonSerializableValue(f.Default)
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
				fd[k] = jsonSerializableValue(v)
			}
			fields[i] = fd
		}
		m["fields"] = fields
	}
	for k, v := range n.Props {
		m[k] = jsonSerializableValue(v)
	}
	return m
}

// nodeFromJSON converts a parsed JSON value into a SchemaNode.
func nodeFromJSON(v any) SchemaNode {
	switch s := v.(type) {
	case string:
		return SchemaNode{Type: s}
	case []any:
		branches := make([]SchemaNode, len(s))
		for i, b := range s {
			branches[i] = nodeFromJSON(b)
		}
		return SchemaNode{Type: "union", Branches: branches}
	case map[string]any:
		return nodeFromJSONObject(s)
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

// lookupCI looks up key k in m, matching case-insensitively. Mirrors
// encoding/json's struct field matching so schemas with non-canonical
// casing ("tYpe" instead of "type") round-trip through Root/Schema.
// When multiple keys collide case-insensitively (e.g. both "tYpe" and
// "TYpe"), the smallest by Unicode code-point wins so the result is
// deterministic — bare `for k := range m` was non-deterministic per
// Go's randomized map iteration, which made Root() return different
// branches on different calls for the same parsed Schema.
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

// coerceMetadataDefault is the metadata-API parallel of [coerceDefault]
// (schema.go). It transforms a parsed-JSON default value into the
// canonical Go form the wire-encode pipeline materializes for that
// field type — so SchemaField.Default surfaces a value type matching
// the wire bytes rather than the raw JSON form
// unmarshalAnyPreservePrecision returns.
//
// Currently:
//   - string defaults for float/double fields → float64 (Java's
//     Schema.parseField text→DoubleNode coercion at Schema.java:1899-1902).
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
// nested record/array/map types. fastavro keeps raw Python strings
// (footgun); twmb sides with Java's typed-materialization per the
// "String-form float defaults" intentional-divergence entry's promise.
//
// Non-string defaults and non-float/double/bytes/fixed/union/container
// types pass through unchanged.
func coerceMetadataDefault(val any, t *SchemaNode, table map[string]*SchemaNode) any {
	if t == nil {
		return val
	}
	// Name-ref resolution: when the caller passes a non-nil name-table
	// and t.Type is a bare name-reference (e.g. "Inner"), resolve to
	// the actual named SchemaNode and recurse. table == nil means the
	// caller is doing best-effort inline coercion only — used by the
	// synchronous call during nodeFromJSON construction where the full
	// tree (and therefore the name-table) isn't available yet.
	if resolved := lookupNameRef(t, table); resolved != nil {
		return coerceMetadataDefault(val, resolved, table)
	}
	if t.Type == "union" {
		// Pick the FIRST branch that accepts val's Go type — matches
		// the wire-encode pipeline's coerceDefault (which uses
		// validateDefault for branch selection) and Java's Schema.
		// parseField (which Jackson-coerces against the first
		// accepting branch). Picking "first transformation" instead
		// would diverge for ["string","float"] with default "1.5":
		// wire picks string (first accept), but a transform-based
		// helper would pick float because string→string is a no-op.
		for i := range t.Branches {
			branch := &t.Branches[i]
			if resolved := lookupNameRef(branch, table); resolved != nil {
				branch = resolved
			}
			if branchAcceptsDefault(branch, val, table) {
				return coerceMetadataDefault(val, branch, table)
			}
		}
		return val
	}
	if val == nil {
		return val
	}
	if t.Type == "float" || t.Type == "double" {
		bitSize := 32
		if t.Type == "double" {
			bitSize = 64
		}
		// Convert string / int64 / int32 inputs to float64 so
		// SchemaField.Default's Go type matches the chosen branch
		// (users type-switching on float64 vs int64 to identify the
		// branch get the right answer). float64 inputs pass through
		// unchanged via the trailing return; json.Number passes through
		// unchanged for the >int64-magnitude case that the metadata
		// API contract preserves losslessly. defaultAsFloat applies
		// the precision predicates (integerFormFitsFloat for string;
		// intFitsFloat for int64/int32) so an out-of-mantissa value
		// passes through unchanged rather than silently rounding.
		switch val.(type) {
		case string, int64, int32:
			if f, err := defaultAsFloat(val, bitSize); err == nil {
				return f
			}
		}
		return val
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
			out := make(map[string]any, len(m))
			for k, v := range m {
				inner := v
				for i := range t.Fields {
					if t.Fields[i].Name == k {
						inner = coerceMetadataDefault(v, &t.Fields[i].Type, table)
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
				out[i] = coerceMetadataDefault(v, t.Items, table)
			}
			return out
		}
		return val
	}
	if t.Type == "map" && t.Values != nil {
		if m, ok := val.(map[string]any); ok {
			out := make(map[string]any, len(m))
			for k, v := range m {
				out[k] = coerceMetadataDefault(v, t.Values, table)
			}
			return out
		}
		return val
	}
	return val
}

// lookupNameRef returns the named target of t if t.Type is a bare
// name-reference (not a structural or primitive kind) AND table has it,
// else nil. A nil table always returns nil (synchronous-build callers
// disable name-ref resolution because the tree isn't fully walked yet).
func lookupNameRef(t *SchemaNode, table map[string]*SchemaNode) *SchemaNode {
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
	return table[t.Type]
}

// fixupNameRefDefaults walks the SchemaNode tree once to populate a
// name-table of every reachable record/enum/fixed, then re-coerces
// HasDefault fields with the table so name-referenced defaults (and
// defaults whose union contains a name-ref branch) materialize the
// way inline-typed siblings already do via the synchronous coerce.
func fixupNameRefDefaults(root *SchemaNode) {
	table := map[string]*SchemaNode{}
	collectNamedTypes(root, "", table)
	if len(table) == 0 {
		return
	}
	coerceTreeDefaults(root, table)
}

func collectNamedTypes(n *SchemaNode, parentNS string, table map[string]*SchemaNode) {
	if n == nil {
		return
	}
	if n.Name != "" { // record / enum / fixed
		ns := n.Namespace
		if ns == "" {
			ns = parentNS
		}
		full := n.Name
		if ns != "" {
			full = ns + "." + n.Name
		}
		table[full] = n
		table[n.Name] = n // unqualified fallback, matches schema-build lookup
		parentNS = ns
	}
	if n.Items != nil {
		collectNamedTypes(n.Items, parentNS, table)
	}
	if n.Values != nil {
		collectNamedTypes(n.Values, parentNS, table)
	}
	for i := range n.Fields {
		collectNamedTypes(&n.Fields[i].Type, parentNS, table)
	}
	for i := range n.Branches {
		collectNamedTypes(&n.Branches[i], parentNS, table)
	}
}

func coerceTreeDefaults(n *SchemaNode, table map[string]*SchemaNode) {
	if n == nil {
		return
	}
	for i := range n.Fields {
		f := &n.Fields[i]
		if f.HasDefault {
			f.Default = coerceMetadataDefault(f.Default, &f.Type, table)
		}
		coerceTreeDefaults(&f.Type, table)
	}
	if n.Items != nil {
		coerceTreeDefaults(n.Items, table)
	}
	if n.Values != nil {
		coerceTreeDefaults(n.Values, table)
	}
	for i := range n.Branches {
		coerceTreeDefaults(&n.Branches[i], table)
	}
}

// defaultMatchesBytesOrFixedKind mirrors the wire-encode pipeline's
// validateLeaf for "bytes" / "fixed" branches: codepoint range
// (validateAvroByteString) and, for "fixed", the exact-rune-count
// size match. The pre-fix structural matcher (`case string, []byte:
// return true`) accepted ANY string regardless of codepoint range or
// fixed-size constraint, so [fixed:8,"string"] with a 4-char default
// would metadata-match the fixed branch while the wire path matched
// the string branch via validateDefault's size check — pattern 14a
// sibling of the convertDefaultBytes/validateDefault fix at
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
// per-value predicates (mantissa, whole-number, int32-bounds, JSON-
// grammar, big.Rat-precision, ParseFloat-ErrRange-with-Inf). A pure
// type-only predicate would silently mis-route across branches when
// the type matched but the value didn't — e.g. ["float","int"] default
// 42 (integer-form json.Number → normalized to int64 by the metadata
// path) would metadata-match the int branch via type-only acceptance
// while the wire path matches the float branch via integerFormFitsFloat
// on the same magnitude.
//
// The float/double arm lenient-accepts string (twmb's documented
// "String-form float defaults" intentional divergence). bytes/fixed
// branch accepts string (codepoint-mapped form per Avro JSON spec) or
// []byte.
func branchAcceptsDefault(t *SchemaNode, val any, table map[string]*SchemaNode) bool {
	// Resolve a bare name-reference if the caller supplied a name-table.
	if resolved := lookupNameRef(t, table); resolved != nil {
		return branchAcceptsDefault(resolved, val, table)
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
		bitSize := 64
		if t.Type == "float" {
			bitSize = 32
		}
		_, err := defaultAsFloat(val, bitSize)
		return err == nil
	case "string", "enum":
		_, ok := val.(string)
		return ok
	case "bytes", "fixed":
		return defaultMatchesBytesOrFixedKind(t, val)
	case "record", "error":
		_, ok := val.(map[string]any)
		return ok
	case "array":
		_, ok := val.([]any)
		return ok
	case "map":
		_, ok := val.(map[string]any)
		return ok
	case "union":
		for i := range t.Branches {
			if branchAcceptsDefault(&t.Branches[i], val, table) {
				return true
			}
		}
		return false
	}
	return false
}

func nodeFromJSONObject(m map[string]any) SchemaNode {
	n := SchemaNode{}

	getCIString(m, "type", &n.Type)
	getCIString(m, "name", &n.Name)
	getCIString(m, "namespace", &n.Namespace)
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
		node := nodeFromJSON(items)
		n.Items = &node
	}
	if values, ok := lookupCI(m, "values"); ok {
		node := nodeFromJSON(values)
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
					sf.Type = nodeFromJSON(t)
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
					sf.Default = coerceMetadataDefault(d, &sf.Type, nil)
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
