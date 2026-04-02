package avro

import (
	"encoding/json"
	"fmt"
	"maps"
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
func (n *SchemaNode) Schema() (*Schema, error) {
	b, err := json.Marshal(n.toJSON())
	if err != nil {
		return nil, fmt.Errorf("avro: marshaling schema node: %w", err)
	}
	return Parse(string(b))
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
	if len(n.Symbols) > 0 {
		m["symbols"] = n.Symbols
	}
	if n.Items != nil {
		m["items"] = n.Items.toJSON()
	}
	if n.Values != nil {
		m["values"] = n.Values.toJSON()
	}
	if len(n.Fields) > 0 {
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

func nodeFromJSONObject(m map[string]any) SchemaNode {
	n := SchemaNode{}

	if t, ok := m["type"].(string); ok {
		n.Type = t
	}
	if name, ok := m["name"].(string); ok {
		n.Name = name
	}
	if ns, ok := m["namespace"].(string); ok {
		n.Namespace = ns
	}
	if doc, ok := m["doc"].(string); ok {
		n.Doc = doc
	}
	if lt, ok := m["logicalType"].(string); ok {
		n.LogicalType = lt
	}
	if p, ok := m["precision"].(float64); ok {
		n.Precision = int(p)
	}
	if s, ok := m["scale"].(float64); ok {
		n.Scale = int(s)
	}
	if s, ok := m["size"].(float64); ok {
		n.Size = int(s)
	}

	if aliases, ok := m["aliases"].([]any); ok {
		n.Aliases = make([]string, len(aliases))
		for i, a := range aliases {
			n.Aliases[i], _ = a.(string)
		}
	}

	if syms, ok := m["symbols"].([]any); ok {
		n.Symbols = make([]string, len(syms))
		for i, s := range syms {
			n.Symbols[i], _ = s.(string)
		}
	}
	if d, ok := m["default"].(string); ok && n.Type == "enum" {
		n.EnumDefault = d
		n.HasEnumDefault = true
	}

	if items, ok := m["items"]; ok {
		node := nodeFromJSON(items)
		n.Items = &node
	}
	if values, ok := m["values"]; ok {
		node := nodeFromJSON(values)
		n.Values = &node
	}

	if fields, ok := m["fields"].([]any); ok {
		n.Fields = make([]SchemaField, len(fields))
		for i, f := range fields {
			fm, _ := f.(map[string]any)
			sf := SchemaField{}
			if name, ok := fm["name"].(string); ok {
				sf.Name = name
			}
			if t, ok := fm["type"]; ok {
				sf.Type = nodeFromJSON(t)
			}
			if d, ok := fm["default"]; ok {
				sf.Default = d
				sf.HasDefault = true
			}
			if doc, ok := fm["doc"].(string); ok {
				sf.Doc = doc
			}
			if aliases, ok := fm["aliases"].([]any); ok {
				sf.Aliases = make([]string, len(aliases))
				for j, a := range aliases {
					sf.Aliases[j], _ = a.(string)
				}
			}
			if order, ok := fm["order"].(string); ok {
				sf.Order = order
			}
			for k, v := range fm {
				if fieldReservedKeys[k] {
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

	// Collect custom properties (anything not in the reserved set).
	for k, v := range m {
		if schemaReservedKeys[k] {
			continue
		}
		if n.Props == nil {
			n.Props = make(map[string]any)
		}
		n.Props[k] = v
	}

	return n
}
