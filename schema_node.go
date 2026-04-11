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

	Precision int            // decimal precision
	Scale     int            // decimal scale
	Props     map[string]any // custom properties (any JSON value)
}

// SchemaField represents a field in an Avro record schema.
type SchemaField struct {
	Name       string         // field name
	Type       SchemaNode     // field schema
	Default    any            // default value (only meaningful when HasDefault is true)
	HasDefault bool           // true if a default value is defined in the schema
	Aliases    []string       // field aliases for schema evolution
	Order      string         // sort order: "ascending" (default), "descending", or "ignore"
	Doc        string         // documentation string
	Props      map[string]any // custom properties (any JSON value)
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
// including doc strings, namespaces, and custom properties.
//
// Root re-parses the JSON on each call. Cache the result if you need
// to access it repeatedly (e.g. in a per-message processing loop).
func (s *Schema) Root() SchemaNode {
	var raw any
	if err := json.Unmarshal([]byte(s.full), &raw); err != nil {
		panic("avro: Schema.Root: invalid stored JSON: " + err.Error())
	}
	return nodeFromJSON(raw)
}

// toJSONDedup is like toJSON but deduplicates named types. The first
// occurrence of a named type (record, enum, fixed) emits the full
// definition; subsequent occurrences emit the name as a reference.
func (n *SchemaNode) toJSONDedup(d *deduper) any {
	// Cycle detection for *SchemaNode pointers (Items/Values). Fields
	// and Branches are value slices and cannot form true cycles.
	if _, cycle := d.visited[n]; cycle {
		if d.err == nil {
			d.err = fmt.Errorf("avro: cyclic SchemaNode detected")
		}
		return nil
	}
	d.visited[n] = struct{}{}
	defer delete(d.visited, n)

	// For named types with a Name, check if already defined.
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

	switch n.Type {
	case "null", "boolean", "int", "long", "float", "double", "string", "bytes":
		if n.LogicalType == "" && len(n.Props) == 0 {
			return n.Type
		}
	case "union":
		branches := make([]any, len(n.Branches))
		for i := range n.Branches {
			branches[i] = n.Branches[i].toJSONDedup(d)
		}
		return branches
	}

	if n.Name == "" && n.Type != "array" && n.Type != "map" &&
		n.Type != "record" && n.Type != "error" && n.Type != "enum" && n.Type != "fixed" &&
		n.Type != "union" && n.LogicalType == "" && len(n.Props) == 0 {
		return n.Type
	}

	// Mark named types as defined, storing the canonical JSON for comparison.
	switch n.Type {
	case "record", "error", "enum", "fixed":
		if n.Name != "" {
			b, _ := json.Marshal(n.toJSON())
			d.defined[n.Name] = string(b)
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
		m["items"] = n.Items.toJSONDedup(d)
	}
	if n.Values != nil {
		m["values"] = n.Values.toJSONDedup(d)
	}
	// record.fields is a required attribute per the Avro spec (Complex
	// Types > Records: "fields: a JSON array, listing fields (required)"),
	// always emit for record/error types even when empty.
	if n.Type == "record" || n.Type == "error" || len(n.Fields) > 0 {
		fields := make([]map[string]any, len(n.Fields))
		for i, f := range n.Fields {
			fd := map[string]any{
				"name": f.Name,
				"type": f.Type.toJSONDedup(d),
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

// toJSON converts a SchemaNode to a JSON-serializable representation.
func (n *SchemaNode) toJSON() any {
	// Named type reference: just the name string.
	switch n.Type {
	case "null", "boolean", "int", "long", "float", "double", "string", "bytes":
		if n.LogicalType == "" && len(n.Props) == 0 {
			return n.Type
		}
	case "union":
		branches := make([]any, len(n.Branches))
		for i := range n.Branches {
			branches[i] = n.Branches[i].toJSON()
		}
		return branches
	}

	// Check if this is a named type reference (no fields, no items, etc.)
	if n.Name == "" && n.Type != "array" && n.Type != "map" &&
		n.Type != "record" && n.Type != "error" && n.Type != "enum" && n.Type != "fixed" &&
		n.Type != "union" && n.LogicalType == "" && len(n.Props) == 0 {
		// Named type reference.
		return n.Type
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
		m["items"] = n.Items.toJSON()
	}
	if n.Values != nil {
		m["values"] = n.Values.toJSON()
	}
	// record.fields is a required attribute per the Avro spec (Complex
	// Types > Records: "fields: a JSON array, listing fields (required)"),
	// always emit for record/error types even when empty.
	if n.Type == "record" || n.Type == "error" || len(n.Fields) > 0 {
		fields := make([]map[string]any, len(n.Fields))
		for i, f := range n.Fields {
			fd := map[string]any{
				"name": f.Name,
				"type": f.Type.toJSON(),
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

// lookupCI looks up key k in m, matching case-insensitively. Mirrors
// encoding/json's struct field matching so schemas with non-canonical
// casing ("tYpe" instead of "type") round-trip through Root/Schema.
func lookupCI(m map[string]any, key string) (any, bool) {
	if v, ok := m[key]; ok {
		return v, true
	}
	for k, v := range m {
		if strings.EqualFold(k, key) {
			return v, true
		}
	}
	return nil, false
}

func nodeFromJSONObject(m map[string]any) SchemaNode {
	n := SchemaNode{}

	if v, ok := lookupCI(m, "type"); ok {
		if t, ok := v.(string); ok {
			n.Type = t
		}
	}
	if v, ok := lookupCI(m, "name"); ok {
		if name, ok := v.(string); ok {
			n.Name = name
		}
	}
	if v, ok := lookupCI(m, "namespace"); ok {
		if ns, ok := v.(string); ok {
			n.Namespace = ns
		}
	}
	if v, ok := lookupCI(m, "doc"); ok {
		if doc, ok := v.(string); ok {
			n.Doc = doc
		}
	}
	if v, ok := lookupCI(m, "logicalType"); ok {
		if lt, ok := v.(string); ok {
			n.LogicalType = lt
		}
	}
	if v, ok := lookupCI(m, "precision"); ok {
		if p, ok := v.(float64); ok {
			n.Precision = int(p)
		}
	}
	if v, ok := lookupCI(m, "scale"); ok {
		if s, ok := v.(float64); ok {
			n.Scale = int(s)
		}
	}
	if v, ok := lookupCI(m, "size"); ok {
		if s, ok := v.(float64); ok {
			n.Size = int(s)
		}
	}

	if v, ok := lookupCI(m, "aliases"); ok {
		if aliases, ok := v.([]any); ok {
			n.Aliases = make([]string, len(aliases))
			for i, a := range aliases {
				n.Aliases[i], _ = a.(string)
			}
		}
	}

	if v, ok := lookupCI(m, "symbols"); ok {
		if syms, ok := v.([]any); ok {
			n.Symbols = make([]string, len(syms))
			for i, s := range syms {
				n.Symbols[i], _ = s.(string)
			}
		}
	}
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
				if v, ok := lookupCI(fm, "name"); ok {
					if name, ok := v.(string); ok {
						sf.Name = name
					}
				}
				if t, ok := lookupCI(fm, "type"); ok {
					sf.Type = nodeFromJSON(t)
				}
				if d, ok := lookupCI(fm, "default"); ok {
					sf.Default = d
					sf.HasDefault = true
				}
				if v, ok := lookupCI(fm, "doc"); ok {
					if doc, ok := v.(string); ok {
						sf.Doc = doc
					}
				}
				if v, ok := lookupCI(fm, "aliases"); ok {
					if aliases, ok := v.([]any); ok {
						sf.Aliases = make([]string, len(aliases))
						for j, a := range aliases {
							sf.Aliases[j], _ = a.(string)
						}
					}
				}
				if v, ok := lookupCI(fm, "order"); ok {
					if order, ok := v.(string); ok {
						sf.Order = order
					}
				}
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

// fieldReservedKeyCI is a case-insensitive wrapper over fieldReservedKeys.
func fieldReservedKeyCI(k string) bool {
	if fieldReservedKeys[k] {
		return true
	}
	for rk := range fieldReservedKeys {
		if strings.EqualFold(k, rk) {
			return true
		}
	}
	return false
}

// schemaReservedKeyCI is a case-insensitive wrapper over schemaReservedKeys.
func schemaReservedKeyCI(k string) bool {
	if schemaReservedKeys[k] {
		return true
	}
	for rk := range schemaReservedKeys {
		if strings.EqualFold(k, rk) {
			return true
		}
	}
	return false
}
