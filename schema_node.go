package avro

import (
	"encoding/json"
	"fmt"
	"maps"
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
	// int64); fractional and exponent-form literals decode to float64.
	Props map[string]any
}

// SchemaField represents a field in an Avro record schema.
type SchemaField struct {
	Name string     // field name
	Type SchemaNode // field schema

	// Default is the field's default value, meaningful only when HasDefault
	// is true. Numbers decode as in [SchemaNode.Props].
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
	return nodeFromJSON(raw)
}

// toJSONDedup is like toJSON but deduplicates named types. The first
// occurrence of a named type (record, enum, fixed) emits the full
// definition; subsequent occurrences emit the name as a reference.
func (n *SchemaNode) toJSONDedup(d *deduper) any {
	return n.toJSONWalk(d.visited, d)
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
	if n.Type == "record" || n.Type == "error" || len(n.Fields) > 0 {
		fields := make([]map[string]any, len(n.Fields))
		for i, f := range n.Fields {
			fd := map[string]any{
				"name": f.Name,
				"type": f.Type.toJSONWalk(visited, d),
			}
			if f.HasDefault || f.Default != nil {
				fd["default"] = f.Default
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
			maps.Copy(fd, f.Props)
			fields[i] = fd
		}
		m["fields"] = fields
	}
	maps.Copy(m, n.Props)
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
					sf.Default = d
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
