package avro

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSchemaNodeRoundTrip(t *testing.T) {
	// Build a SchemaNode, convert to Schema, get Root back, verify.
	node := &SchemaNode{
		Type:      "record",
		Name:      "User",
		Namespace: "com.example",
		Doc:       "A user record",
		Fields: []SchemaField{
			{Name: "name", Type: SchemaNode{Type: "string"}},
			{Name: "age", Type: SchemaNode{Type: "int"}, Default: float64(18)},
			{Name: "email", Type: SchemaNode{
				Type:     "union",
				Branches: []SchemaNode{{Type: "null"}, {Type: "string"}},
			}},
		},
	}

	s, err := node.Schema()
	if err != nil {
		t.Fatal(err)
	}

	got := s.Root()

	if got.Type != "record" {
		t.Errorf("type: got %q, want record", got.Type)
	}
	if got.Name != "User" {
		t.Errorf("name: got %q, want User", got.Name)
	}
	if got.Namespace != "com.example" {
		t.Errorf("namespace: got %q, want com.example", got.Namespace)
	}
	if got.Doc != "A user record" {
		t.Errorf("doc: got %q, want 'A user record'", got.Doc)
	}
	if len(got.Fields) != 3 {
		t.Fatalf("fields: got %d, want 3", len(got.Fields))
	}
	if got.Fields[0].Name != "name" || got.Fields[0].Type.Type != "string" {
		t.Errorf("field 0: got %+v", got.Fields[0])
	}
	// Root() narrows defaults to the schema's wire width: int → int32,
	// long → int64, float → float32, double → float64. See
	// coerceMetadataDefault in schema_node.go and SchemaField.Default
	// docstring for the type table.
	if got.Fields[1].Name != "age" || got.Fields[1].Default != int32(18) {
		t.Errorf("field 1: got %+v", got.Fields[1])
	}
	if got.Fields[2].Type.Type != "union" || len(got.Fields[2].Type.Branches) != 2 {
		t.Errorf("field 2: got %+v", got.Fields[2])
	}
}

func TestSchemaNodePrimitives(t *testing.T) {
	for _, prim := range []string{"null", "boolean", "int", "long", "float", "double", "string", "bytes"} {
		t.Run(prim, func(t *testing.T) {
			node := &SchemaNode{Type: prim}
			s, err := node.Schema()
			if err != nil {
				t.Fatal(err)
			}
			got := s.Root()
			if got.Type != prim {
				t.Errorf("got %q, want %q", got.Type, prim)
			}
		})
	}
}

func TestSchemaNodeLogicalTypes(t *testing.T) {
	tests := []struct {
		base    string
		logical string
	}{
		{"long", "timestamp-millis"},
		{"long", "timestamp-micros"},
		{"long", "timestamp-nanos"},
		{"int", "date"},
		{"int", "time-millis"},
		{"long", "time-micros"},
		{"string", "uuid"},
	}
	for _, tt := range tests {
		t.Run(tt.logical, func(t *testing.T) {
			node := &SchemaNode{Type: tt.base, LogicalType: tt.logical}
			s, err := node.Schema()
			if err != nil {
				t.Fatal(err)
			}
			got := s.Root()
			if got.LogicalType != tt.logical {
				t.Errorf("logicalType: got %q, want %q", got.LogicalType, tt.logical)
			}
		})
	}
}

func TestSchemaNodeDecimal(t *testing.T) {
	node := &SchemaNode{
		Type:        "bytes",
		LogicalType: "decimal",
		Precision:   10,
		Scale:       2,
	}
	s, err := node.Schema()
	if err != nil {
		t.Fatal(err)
	}
	got := s.Root()
	if got.Precision != 10 {
		t.Errorf("precision: got %d, want 10", got.Precision)
	}
	if got.Scale != 2 {
		t.Errorf("scale: got %d, want 2", got.Scale)
	}
}

func TestSchemaNodeEnum(t *testing.T) {
	node := &SchemaNode{
		Type:    "enum",
		Name:    "Color",
		Symbols: []string{"RED", "GREEN", "BLUE"},
	}
	s, err := node.Schema()
	if err != nil {
		t.Fatal(err)
	}
	got := s.Root()
	if !reflect.DeepEqual(got.Symbols, []string{"RED", "GREEN", "BLUE"}) {
		t.Errorf("symbols: got %v", got.Symbols)
	}
}

// TestSchemaNodeEmptyRecord exercises the Avro spec requirement (Complex
// Types > Records) that "fields: a JSON array, listing fields (required)"
// — a record with zero user-declared fields must still emit "fields": [].
// Strict readers like Java Avro reject {"type":"record","name":"x"} with
// "Record has no fields".
func TestSchemaNodeEmptyRecord(t *testing.T) {
	node := &SchemaNode{
		Type: "record",
		Name: "Empty",
	}
	s, err := node.Schema()
	if err != nil {
		t.Fatal(err)
	}
	got := s.Canonical()
	want := `{"name":"Empty","type":"record","fields":[]}`
	if string(got) != want {
		t.Errorf("canonical: got %s, want %s", got, want)
	}
}

// TestSchemaNodeEmptyRecordNested exercises the required-fields fix in
// every position an empty record can appear: as a named-type reference,
// as array items, as map values, as a union branch, and as a field
// type inside another record. Each must emit "fields":[] so Java Avro
// and other strict readers can parse it.
func TestSchemaNodeEmptyRecordNested(t *testing.T) {
	empty := SchemaNode{Type: "record", Name: "Inner"}

	cases := []struct {
		name string
		node SchemaNode
	}{
		{"as field type", SchemaNode{
			Type: "record", Name: "Outer",
			Fields: []SchemaField{{Name: "inner", Type: empty}},
		}},
		{"as array items", SchemaNode{
			Type: "array", Items: &empty,
		}},
		{"as map values", SchemaNode{
			Type: "map", Values: &empty,
		}},
		{"as union branch", SchemaNode{
			Type:     "union",
			Branches: []SchemaNode{{Type: "null"}, empty},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := tc.node.Schema()
			if err != nil {
				t.Fatal(err)
			}
			canon := string(s.Canonical())
			if !strings.Contains(canon, `"fields":[]`) {
				t.Errorf("canonical missing 'fields':[]: %s", canon)
			}
			// Re-parseability: Canonical() output must itself parse.
			if _, err := Parse(canon); err != nil {
				t.Errorf("Canonical() output failed to re-parse: %v\noutput: %s", err, canon)
			}
		})
	}
}

// TestSchemaNodeCanonicalIdempotent verifies that Canonical() is a
// fixed point: parsing the canonical form and re-canonicalizing must
// produce byte-identical output.
//
// A record MISSING the fields attribute is not in this table: fields is
// required (Java: "Record has no fields") and Parse rejects its absence —
// the empty record is spelled "fields":[]. The rejection is pinned by the
// record-missing-fields mutants in TestMatrix_AcceptanceMutantsRejectLocally.
func TestSchemaNodeCanonicalIdempotent(t *testing.T) {
	inputs := []string{
		`{"type":"record","name":"Empty","fields":[]}`,
		`{"type":"record","name":"Outer","fields":[{"name":"inner","type":{"type":"record","name":"I","fields":[]}}]}`,
		`{"type":"array","items":{"type":"record","name":"E","fields":[]}}`,
		`{"type":"map","values":{"type":"record","name":"E","fields":[]}}`,
		`["null",{"type":"record","name":"E","fields":[]}]`,
	}
	for _, in := range inputs {
		t.Run(in, func(t *testing.T) {
			s1, err := Parse(in)
			if err != nil {
				t.Fatal(err)
			}
			c1 := s1.Canonical()
			s2, err := Parse(string(c1))
			if err != nil {
				t.Fatalf("re-parse canonical failed: %v\ncanonical: %s", err, c1)
			}
			c2 := s2.Canonical()
			if string(c1) != string(c2) {
				t.Errorf("canonical not idempotent:\n  first:  %s\n  second: %s", c1, c2)
			}
		})
	}
}

// TestSchemaNodeCanonicalOrder verifies Parsing Canonical Form's [ORDER]
// rule: "name, type, fields, symbols, items, values, size". Since PCF
// strips all other attributes, a record's canonical form always has the
// key order: name, type, fields.
func TestSchemaNodeCanonicalOrder(t *testing.T) {
	node := &SchemaNode{
		Type:      "record",
		Name:      "Ordered",
		Namespace: "ns",
		Doc:       "doc",
		Aliases:   []string{"Old"},
		Fields: []SchemaField{
			{Name: "a", Type: SchemaNode{Type: "int"}},
		},
	}
	s, err := node.Schema()
	if err != nil {
		t.Fatal(err)
	}
	got := string(s.Canonical())
	want := `{"name":"ns.Ordered","type":"record","fields":[{"name":"a","type":"int"}]}`
	if got != want {
		t.Errorf("canonical:\n got %s\nwant %s", got, want)
	}
}

func TestSchemaNodeFixed(t *testing.T) {
	node := &SchemaNode{
		Type: "fixed",
		Name: "Hash",
		Size: 32,
	}
	s, err := node.Schema()
	if err != nil {
		t.Fatal(err)
	}
	got := s.Root()
	if got.Size != 32 {
		t.Errorf("size: got %d, want 32", got.Size)
	}
}

func TestSchemaNodeArray(t *testing.T) {
	node := &SchemaNode{
		Type:  "array",
		Items: &SchemaNode{Type: "string"},
	}
	s, err := node.Schema()
	if err != nil {
		t.Fatal(err)
	}
	got := s.Root()
	if got.Items == nil || got.Items.Type != "string" {
		t.Errorf("items: got %+v", got.Items)
	}
}

func TestSchemaNodeMap(t *testing.T) {
	node := &SchemaNode{
		Type:   "map",
		Values: &SchemaNode{Type: "int"},
	}
	s, err := node.Schema()
	if err != nil {
		t.Fatal(err)
	}
	got := s.Root()
	if got.Values == nil || got.Values.Type != "int" {
		t.Errorf("values: got %+v", got.Values)
	}
}

func TestSchemaNodeNestedRecords(t *testing.T) {
	node := &SchemaNode{
		Type: "record",
		Name: "Outer",
		Fields: []SchemaField{
			{Name: "inner", Type: SchemaNode{
				Type: "record",
				Name: "Inner",
				Fields: []SchemaField{
					{Name: "x", Type: SchemaNode{Type: "int"}},
					{Name: "y", Type: SchemaNode{Type: "string"}},
				},
			}},
			{Name: "z", Type: SchemaNode{Type: "long"}},
		},
	}
	s, err := node.Schema()
	if err != nil {
		t.Fatal(err)
	}

	// Verify encode/decode works.
	type Inner struct {
		X int32  `avro:"x"`
		Y string `avro:"y"`
	}
	type Outer struct {
		Inner Inner `avro:"inner"`
		Z     int64 `avro:"z"`
	}
	v := Outer{Inner: Inner{X: 1, Y: "hello"}, Z: 42}
	data, err := s.Encode(&v)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got Outer
	if _, err := s.Decode(data, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != v {
		t.Errorf("got %+v, want %+v", got, v)
	}
}

func TestSchemaNodeFourLevelWithReuse(t *testing.T) {
	inner := SchemaNode{
		Type: "record",
		Name: "Inner",
		Fields: []SchemaField{
			{Name: "v", Type: SchemaNode{Type: "int"}},
		},
	}
	node := &SchemaNode{
		Type: "record",
		Name: "Root",
		Fields: []SchemaField{
			{Name: "a", Type: SchemaNode{
				Type: "record",
				Name: "Mid",
				Fields: []SchemaField{
					{Name: "deep", Type: inner},
				},
			}},
			{Name: "b", Type: SchemaNode{Type: "Inner"}}, // reference
		},
	}
	s, err := node.Schema()
	if err != nil {
		t.Fatal(err)
	}
	got := s.Root()
	// Field "b" should be a reference to "Inner".
	if got.Fields[1].Type.Type != "Inner" {
		t.Errorf("field b type: got %q, want Inner reference", got.Fields[1].Type.Type)
	}
}

func TestSchemaNodeCustomProps(t *testing.T) {
	schemaJSON := `{
		"type": "record",
		"name": "Event",
		"fields": [{
			"name": "ts",
			"type": "long",
			"connect.name": "io.debezium.time.Timestamp"
		}],
		"connect.name": "com.example.Event"
	}`
	s, err := Parse(schemaJSON)
	if err != nil {
		t.Fatal(err)
	}
	got := s.Root()
	if got.Props["connect.name"] != "com.example.Event" {
		t.Errorf("record prop: got %q", got.Props["connect.name"])
	}
	// Field-level props are in the field's Type node... actually they're
	// on the field definition in JSON, not the type. Let me check.
}

func TestSchemaNodeFieldProps(t *testing.T) {
	schemaJSON := `{
		"type": "record",
		"name": "Event",
		"fields": [{
			"name": "ts",
			"type": "long",
			"connect.name": "io.debezium.time.Timestamp"
		}]
	}`
	s, err := Parse(schemaJSON)
	if err != nil {
		t.Fatal(err)
	}
	got := s.Root()
	if got.Fields[0].Props["connect.name"] != "io.debezium.time.Timestamp" {
		t.Errorf("field prop: got %q", got.Fields[0].Props["connect.name"])
	}
}

func TestSchemaNodeFieldAliases(t *testing.T) {
	node := &SchemaNode{
		Type: "record",
		Name: "R",
		Fields: []SchemaField{
			{
				Name:    "new_name",
				Type:    SchemaNode{Type: "string"},
				Aliases: []string{"old_name"},
			},
		},
	}
	s, err := node.Schema()
	if err != nil {
		t.Fatal(err)
	}
	got := s.Root()
	if len(got.Fields[0].Aliases) != 1 || got.Fields[0].Aliases[0] != "old_name" {
		t.Errorf("aliases: got %v", got.Fields[0].Aliases)
	}
}

func TestSchemaNodeFieldDoc(t *testing.T) {
	node := &SchemaNode{
		Type: "record",
		Name: "R",
		Fields: []SchemaField{
			{
				Name: "x",
				Type: SchemaNode{Type: "int"},
				Doc:  "the x coordinate",
			},
		},
	}
	s, err := node.Schema()
	if err != nil {
		t.Fatal(err)
	}
	got := s.Root()
	if got.Fields[0].Doc != "the x coordinate" {
		t.Errorf("doc: got %q", got.Fields[0].Doc)
	}
}

func TestSchemaNodeInvalid(t *testing.T) {
	node := &SchemaNode{
		Type: "record",
		// Missing name.
		Fields: []SchemaField{
			{Name: "x", Type: SchemaNode{Type: "int"}},
		},
	}
	_, err := node.Schema()
	if err == nil {
		t.Fatal("expected error for record without name")
	}
}

func TestSchemaNodeEncodeDecodeRoundTrip(t *testing.T) {
	// Build schema from node, encode data, decode it back.
	node := &SchemaNode{
		Type:      "record",
		Name:      "Product",
		Namespace: "com.shop",
		Fields: []SchemaField{
			{Name: "name", Type: SchemaNode{Type: "string"}},
			{Name: "price", Type: SchemaNode{Type: "double"}},
			{Name: "tags", Type: SchemaNode{
				Type:  "array",
				Items: &SchemaNode{Type: "string"},
			}},
			{Name: "metadata", Type: SchemaNode{
				Type:   "map",
				Values: &SchemaNode{Type: "int"},
			}},
		},
	}
	s, err := node.Schema()
	if err != nil {
		t.Fatal(err)
	}

	type Product struct {
		Name     string           `avro:"name"`
		Price    float64          `avro:"price"`
		Tags     []string         `avro:"tags"`
		Metadata map[string]int32 `avro:"metadata"`
	}
	p := Product{
		Name:     "Widget",
		Price:    9.99,
		Tags:     []string{"sale", "new"},
		Metadata: map[string]int32{"stock": 42},
	}
	data, err := s.Encode(&p)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got Product
	if _, err := s.Decode(data, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Name != p.Name || got.Price != p.Price {
		t.Errorf("got %+v, want %+v", got, p)
	}
}

func TestRootFromParsedSchema(t *testing.T) {
	// Parse a complex schema and verify Root() preserves everything.
	schemaJSON := `{
		"type": "record",
		"name": "Event",
		"namespace": "com.example",
		"doc": "An event",
		"fields": [
			{"name": "id", "type": "string", "doc": "unique id"},
			{"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
			{"name": "data", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
			{"name": "tags", "type": {"type": "array", "items": "string"}},
			{"name": "meta", "type": {"type": "map", "values": "int"}},
			{"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["ACTIVE", "DELETED"]}},
			{"name": "hash", "type": {"type": "fixed", "name": "Hash", "size": 16}},
			{"name": "extra", "type": ["null", "string"], "default": null, "aliases": ["old_extra"]}
		]
	}`
	s, err := Parse(schemaJSON)
	if err != nil {
		t.Fatal(err)
	}
	got := s.Root()

	if got.Type != "record" {
		t.Errorf("type: %q", got.Type)
	}
	if got.Name != "Event" {
		t.Errorf("name: %q", got.Name)
	}
	if got.Namespace != "com.example" {
		t.Errorf("namespace: %q", got.Namespace)
	}
	if got.Doc != "An event" {
		t.Errorf("doc: %q", got.Doc)
	}
	if len(got.Fields) != 8 {
		t.Fatalf("fields: %d", len(got.Fields))
	}

	// id
	if got.Fields[0].Doc != "unique id" {
		t.Errorf("field 0 doc: %q", got.Fields[0].Doc)
	}
	// ts — logical type
	if got.Fields[1].Type.LogicalType != "timestamp-millis" {
		t.Errorf("field 1 logical: %q", got.Fields[1].Type.LogicalType)
	}
	// data — decimal
	if got.Fields[2].Type.Precision != 10 || got.Fields[2].Type.Scale != 2 {
		t.Errorf("field 2 decimal: p=%d s=%d", got.Fields[2].Type.Precision, got.Fields[2].Type.Scale)
	}
	// tags — array
	if got.Fields[3].Type.Items == nil || got.Fields[3].Type.Items.Type != "string" {
		t.Errorf("field 3 items: %+v", got.Fields[3].Type.Items)
	}
	// meta — map
	if got.Fields[4].Type.Values == nil || got.Fields[4].Type.Values.Type != "int" {
		t.Errorf("field 4 values: %+v", got.Fields[4].Type.Values)
	}
	// status — enum
	if len(got.Fields[5].Type.Symbols) != 2 {
		t.Errorf("field 5 symbols: %v", got.Fields[5].Type.Symbols)
	}
	// hash — fixed
	if got.Fields[6].Type.Size != 16 {
		t.Errorf("field 6 size: %d", got.Fields[6].Type.Size)
	}
	// extra — union with default and aliases
	if got.Fields[7].Type.Type != "union" || len(got.Fields[7].Type.Branches) != 2 {
		t.Errorf("field 7 union: %+v", got.Fields[7].Type)
	}
	if got.Fields[7].Default != nil {
		t.Errorf("field 7 default: got %v, want nil", got.Fields[7].Default)
	}
	if len(got.Fields[7].Aliases) != 1 || got.Fields[7].Aliases[0] != "old_extra" {
		t.Errorf("field 7 aliases: %v", got.Fields[7].Aliases)
	}
}

func TestSchemaNodeAliasesEnumDefaultOrderRoundTrip(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "R",
		"aliases": ["OldR", "AncientR"],
		"fields": [
			{"name": "status", "type": {
				"type": "enum",
				"name": "Status",
				"symbols": ["ACTIVE", "DELETED"],
				"default": "ACTIVE",
				"aliases": ["OldStatus"]
			}},
			{"name": "score", "type": "int", "order": "descending"},
			{"name": "tags", "type": {"type": "array", "items": "string"}, "order": "ignore"}
		]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	root := s.Root()

	// Record aliases
	if len(root.Aliases) != 2 || root.Aliases[0] != "OldR" {
		t.Errorf("record aliases: %v", root.Aliases)
	}

	// Enum aliases, default
	enumField := root.Fields[0]
	if len(enumField.Type.Aliases) != 1 || enumField.Type.Aliases[0] != "OldStatus" {
		t.Errorf("enum aliases: %v", enumField.Type.Aliases)
	}
	if !enumField.Type.HasEnumDefault || enumField.Type.EnumDefault != "ACTIVE" {
		t.Errorf("enum default: has=%v val=%q", enumField.Type.HasEnumDefault, enumField.Type.EnumDefault)
	}

	// Field order
	if root.Fields[1].Order != "descending" {
		t.Errorf("score order: %q", root.Fields[1].Order)
	}
	if root.Fields[2].Order != "ignore" {
		t.Errorf("tags order: %q", root.Fields[2].Order)
	}

	// Round-trip: SchemaNode → Schema → Root
	node := s.Root()
	s2, err := node.Schema()
	if err != nil {
		t.Fatal(err)
	}
	root2 := s2.Root()
	if len(root2.Aliases) != 2 {
		t.Errorf("round-trip aliases lost: %v", root2.Aliases)
	}
	if !root2.Fields[0].Type.HasEnumDefault {
		t.Error("round-trip enum default lost")
	}
	if root2.Fields[1].Order != "descending" {
		t.Error("round-trip order lost")
	}
}

func TestSchemaNodeCustomPropsExtended(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "R",
		"custom.tag": "hello",
		"custom.num": 42,
		"fields": [
			{"name": "x", "type": "int", "custom.field": true}
		]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	root := s.Root()
	if root.Props["custom.tag"] != "hello" {
		t.Errorf("record props: %v", root.Props)
	}
	// Integer JSON literals preserve precision and come back as int64,
	// not float64. See TestRegression_SchemaExtraNumberPrecisionLoss.
	if root.Props["custom.num"] != int64(42) {
		t.Errorf("record num prop: %v", root.Props["custom.num"])
	}
	if root.Fields[0].Props["custom.field"] != true {
		t.Errorf("field props: %v", root.Fields[0].Props)
	}
}

func TestSchemaNodeDedupNamedTypes(t *testing.T) {
	uuid := SchemaNode{Type: "fixed", Name: "uuid_f", Size: 16, LogicalType: "uuid"}
	node := SchemaNode{
		Type: "record",
		Name: "r",
		Fields: []SchemaField{
			{Name: "a", Type: SchemaNode{Type: "union", Branches: []SchemaNode{{Type: "null"}, uuid}}},
			{Name: "b", Type: SchemaNode{Type: "union", Branches: []SchemaNode{{Type: "null"}, uuid}}},
		},
	}
	s, err := node.Schema()
	if err != nil {
		t.Fatalf("Schema() with duplicate named type should succeed: %v", err)
	}
	// Round-trip: encode and decode to verify both fields work.
	input := map[string]any{"a": [16]byte{1}, "b": [16]byte{2}}
	enc, err := s.Encode(input)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if _, err := s.Decode(enc, &out); err != nil {
		t.Fatal(err)
	}
	a, _ := out["a"].([16]byte)
	b, _ := out["b"].([16]byte)
	if a[0] != 1 || b[0] != 2 {
		t.Fatalf("round-trip failed: a=%v b=%v", a, b)
	}
}

func TestSchemaNodeDedupConflictingNameErrors(t *testing.T) {
	node := SchemaNode{
		Type: "record",
		Name: "r",
		Fields: []SchemaField{
			{Name: "a", Type: SchemaNode{Type: "fixed", Name: "f", Size: 16}},
			{Name: "b", Type: SchemaNode{Type: "fixed", Name: "f", Size: 8}}, // same name, different size
		},
	}
	_, err := node.Schema()
	if err == nil {
		t.Fatal("expected error for conflicting named type definitions")
	}
}

func TestSchemaNodeCyclicItems(t *testing.T) {
	outer := &SchemaNode{Type: "array"}
	outer.Items = outer
	_, err := outer.Schema()
	if err == nil {
		t.Fatal("expected error for cyclic SchemaNode via Items")
	}
}

func TestSchemaNodeCyclicValues(t *testing.T) {
	outer := &SchemaNode{Type: "map"}
	outer.Values = outer
	_, err := outer.Schema()
	if err == nil {
		t.Fatal("expected error for cyclic SchemaNode via Values")
	}
}

func TestSchemaNodeCyclicIndirect(t *testing.T) {
	// A.Items → B.Values → A
	a := &SchemaNode{Type: "array"}
	b := &SchemaNode{Type: "map", Values: a}
	a.Items = b
	if _, err := a.Schema(); err == nil {
		t.Fatal("expected error for indirect 2-node cycle")
	}
}

func TestSchemaNodeCyclic3Node(t *testing.T) {
	// A.Items → B.Items → C.Items → A
	a := &SchemaNode{Type: "array"}
	b := &SchemaNode{Type: "array"}
	c := &SchemaNode{Type: "array"}
	a.Items = b
	b.Items = c
	c.Items = a
	if _, err := a.Schema(); err == nil {
		t.Fatal("expected error for 3-node cycle")
	}
}

// A deep ACYCLIC SchemaNode chain (distinct node per level, so the
// cycle-detection visited map never fires) must bound its own walk: the
// toJSONWalk recursion has no pointer cycle to terminate it, so without a
// depth bound a hand-built array<array<…>> deep enough overflows the
// goroutine stack and kills the process uncatchably — before Schema's
// eventual Parse can reject it. The walk caps at maxSchemaJSONDepth, so a
// too-deep tree stops with this bounded error rather than crashing. This
// pins that the SchemaNode walk bounds depth ITSELF (message names the
// node tree) rather than relying on Parse's downstream JSON-bracket cap.
func TestRegression_SchemaNodeSchemaDeepAcyclicBounded(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked: %v", r)
		}
	}()
	const depth = maxSchemaJSONDepth + 50
	cur := &SchemaNode{Type: "long"}
	for range depth {
		cur = &SchemaNode{Type: "array", Items: cur}
	}
	_, err := cur.Schema()
	if err == nil {
		t.Fatal("expected a bounded error for an over-deep SchemaNode chain, got nil")
	}
	if !strings.Contains(err.Error(), "SchemaNode tree nests deeper") {
		t.Fatalf("expected the node-walk depth bound to fire, got %v", err)
	}

	// Boundary-1: a tree well below the cap (and below the wire codec's own
	// maxDepth, so genuinely usable) must still build — the bound must not
	// false-reject a legitimately deep hand-built tree.
	ok := &SchemaNode{Type: "long"}
	for range 500 {
		ok = &SchemaNode{Type: "array", Items: ok}
	}
	if _, err := ok.Schema(); err != nil {
		t.Fatalf("a 500-deep array tree should build, got %v", err)
	}
}

// The value channel — a Props value or a SchemaField.Default — is a SEPARATE
// recursion from the structural node walk: each value is handed to
// jsonSerializableValue (needsJSONFixup/applyJSONFixup) and then to
// json.Marshal at Schema(), none of which bounds depth. So a value nested
// deeply enough overflows the goroutine stack uncatchably (recover cannot
// catch a stack overflow) even when the node tree itself is one level deep —
// the structural depth bound never sees it. The walk bounds the value at the
// same maxSchemaJSONDepth ceiling, so an over-deep value stops with a bounded
// error rather than crashing.
func TestRegression_SchemaNodeSchemaDeepValueBounded(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked: %v", r)
		}
	}()
	deepValue := func() any {
		var v any = "leaf"
		for range maxSchemaJSONDepth + 50 {
			v = map[string]any{"k": v}
		}
		return v
	}

	t.Run("props value", func(t *testing.T) {
		node := &SchemaNode{Type: "int", Props: map[string]any{"x": deepValue()}}
		_, err := node.Schema()
		if err == nil {
			t.Fatal("expected a bounded error for an over-deep Props value, got nil")
		}
		if !strings.Contains(err.Error(), "value nests deeper") {
			t.Fatalf("expected the value-channel depth bound to fire, got %v", err)
		}
	})

	t.Run("field default value", func(t *testing.T) {
		node := &SchemaNode{
			Type: "record",
			Name: "R",
			Fields: []SchemaField{
				{Name: "f", HasDefault: true, Default: deepValue(), Type: SchemaNode{Type: "int"}},
			},
		}
		_, err := node.Schema()
		if err == nil {
			t.Fatal("expected a bounded error for an over-deep Default value, got nil")
		}
		if !strings.Contains(err.Error(), "value nests deeper") {
			t.Fatalf("expected the value-channel depth bound to fire, got %v", err)
		}
	})

	// Boundary-1: a value nested well below the cap must still build AND
	// survive the round-trip — the bound must not false-reject or silently
	// drop a legitimately structured property.
	t.Run("usable depth builds and round-trips", func(t *testing.T) {
		var v any = "leaf"
		for range 500 {
			v = map[string]any{"k": v}
		}
		node := &SchemaNode{Type: "int", Props: map[string]any{"x": v}}
		s, err := node.Schema()
		if err != nil {
			t.Fatalf("a 500-deep Props value should build, got %v", err)
		}
		if got := s.Root().Props["x"]; got == nil {
			t.Fatal("the usable-depth Props value was dropped")
		}
	})

	// Sibling channel: SchemaFor embeds a hand-built CustomType.Schema via the
	// same value walk (the bare, d==nil path). A deep value there must also
	// terminate instead of crashing the process; the bare path truncates the
	// over-deep value rather than erroring (mirroring the structural walk's
	// documented bare-path behavior), so the assertion is simply that it
	// returns without a stack-overflow death. The CustomType.Schema is embedded
	// (and its value walk runs) ONLY when a struct FIELD matches the custom
	// GoType — SchemaFor over a non-struct errors before any walk — so this uses
	// a struct field, and a typed-container value to cover the broadened bound.
	t.Run("SchemaFor custom-schema deep value terminates", func(t *testing.T) {
		var v any = "leaf"
		for range maxSchemaJSONDepth + 50 {
			v = []map[string]any{{"k": v}}
		}
		type recForCustom struct{ F int32 }
		ct := CustomType{
			GoType: reflect.TypeOf(int32(0)),
			Schema: &SchemaNode{Type: "int", Props: map[string]any{"deep": v}},
		}
		_, _ = SchemaFor[recForCustom](WithCustomType(ct))
	})

	// json.Marshal recurses into EVERY container kind, not just the
	// map[string]any / []any shapes Root() produces. A hand-built node can store
	// any value the map[string]any field accepts; a TYPED container nests just
	// as deeply yet was invisible to a map[string]any/[]any-only depth check, so
	// it reached json.Marshal unbounded and overflowed the goroutine stack. The
	// bound must cover the typed shapes too.
	t.Run("typed-container props value rejected", func(t *testing.T) {
		var v any = "leaf"
		for range maxSchemaJSONDepth + 50 {
			v = []map[string]any{{"k": v}} // []map[string]any: NOT []any, NOT map[string]any
		}
		node := &SchemaNode{Type: "int", Props: map[string]any{"x": v}}
		_, err := node.Schema()
		if err == nil {
			t.Fatal("expected a bounded error for an over-deep typed-container Props value, got nil")
		}
		if !strings.Contains(err.Error(), "value nests deeper") {
			t.Fatalf("expected the value-channel depth bound to fire (not a post-marshal Parse error), got %v", err)
		}
	})

	// Boundary-1: a typed container nested well below the cap must still build —
	// the broadened walk must not false-reject legitimate typed values.
	t.Run("usable typed-container builds", func(t *testing.T) {
		var v any = "leaf"
		for range 200 {
			v = []map[string]any{{"k": v}}
		}
		node := &SchemaNode{Type: "int", Props: map[string]any{"x": v}}
		s, err := node.Schema()
		if err != nil {
			t.Fatalf("a 200-deep typed-container Props value should build, got %v", err)
		}
		if got := s.Root().Props["x"]; got == nil {
			t.Fatal("the usable-depth typed-container Props value was dropped")
		}
	})

	// A cyclic Go type (type P *P) has an infinite reflect-value chain. The walk
	// decrements its budget on every indirection, so it must TERMINATE with the
	// bound rather than recursing forever (the trap a reflect indirection walk
	// without a bound would fall into) — Schema returns, no hang, no crash.
	t.Run("cyclic pointer value terminates", func(t *testing.T) {
		type selfPtr *selfPtr
		var p selfPtr
		p = &p // p points to itself: p.Elem() == p, an unbounded pointer chain
		node := &SchemaNode{Type: "int", Props: map[string]any{"x": p}}
		_, _ = node.Schema() // must return (the bound stops the walk), not hang
	})
}

// The SchemaNode→JSON walk (toJSONWalk + the value channel) must bound DEPTH on
// EVERY channel a hand-built node can carry nesting through, or a refactor that
// drops one channel's depth charge ships an uncatchable stack overflow with a
// green battery. The structural recursions are four distinct sites
// (Items/Values/Branches/Fields), the value channel is three distinct
// boundedSerializableValue call sites (node Props, field Props, field Default),
// and the value walk must recurse into every container kind json.Marshal does
// (map, slice, array, struct, pointer/interface) — this pins each cell so its
// bound cannot be silently removed.
func TestRegression_SchemaNodeWalkDepthAllChannels(t *testing.T) {
	const deep = maxSchemaJSONDepth + 50

	// Each structural recursion must charge depth. A too-deep chain through any
	// one of them stops with the structural depth error, never a crash.
	structural := map[string]func() *SchemaNode{
		"items": func() *SchemaNode {
			cur := &SchemaNode{Type: "long"}
			for range deep {
				cur = &SchemaNode{Type: "array", Items: cur}
			}
			return cur
		},
		"values": func() *SchemaNode {
			cur := &SchemaNode{Type: "long"}
			for range deep {
				cur = &SchemaNode{Type: "map", Values: cur}
			}
			return cur
		},
		"branches": func() *SchemaNode {
			cur := SchemaNode{Type: "long"}
			for range deep {
				cur = SchemaNode{Type: "union", Branches: []SchemaNode{cur}}
			}
			return &cur
		},
		"fields": func() *SchemaNode {
			cur := SchemaNode{Type: "long"}
			for i := range deep {
				// Distinct names per level: same-named records would dedup to a
				// reference and collapse the chain instead of nesting it.
				cur = SchemaNode{Type: "record", Name: fmt.Sprintf("R%d", i), Fields: []SchemaField{{Name: "f", Type: cur}}}
			}
			return &cur
		},
	}
	for name, build := range structural {
		t.Run("structural/"+name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panicked: %v", r)
				}
			}()
			_, err := build().Schema()
			if err == nil || !strings.Contains(err.Error(), "tree nests deeper") {
				t.Fatalf("want the structural depth bound to fire, got %v", err)
			}
		})
	}

	deepMap := func() any {
		var v any = "leaf"
		for range deep {
			v = map[string]any{"k": v}
		}
		return v
	}
	// Each of the three value sites routes its value through
	// boundedSerializableValue; all three must bound a too-deep value. node Props
	// and field Default were already pinned; field Props (the third call site)
	// was not — a dropped bound there crashes only on a field-level property.
	valueSites := map[string]func(any) *SchemaNode{
		"node-props": func(v any) *SchemaNode {
			return &SchemaNode{Type: "int", Props: map[string]any{"x": v}}
		},
		"field-props": func(v any) *SchemaNode {
			return &SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{
				{Name: "f", Type: SchemaNode{Type: "int"}, Props: map[string]any{"x": v}},
			}}
		},
		"field-default": func(v any) *SchemaNode {
			return &SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{
				{Name: "f", HasDefault: true, Default: v, Type: SchemaNode{Type: "int"}},
			}}
		},
	}
	for name, build := range valueSites {
		t.Run("value-site/"+name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panicked: %v", r)
				}
			}()
			_, err := build(deepMap()).Schema()
			if err == nil || !strings.Contains(err.Error(), "value nests deeper") {
				t.Fatalf("want the value depth bound to fire, got %v", err)
			}
		})
	}

	// The value walk mirrors json.Marshal's recursion into EVERY container kind,
	// not just the map[string]any/[]any shapes Root() emits — a hand-built node
	// can store any Go value the map[string]any field accepts. Each kind deep
	// enough must hit the value depth bound; removing its arm from valueWalkLimit
	// would let json.Marshal stack-overflow on that shape instead.
	type box struct{ X any }
	kinds := map[string]func() any{
		"typed-slice": func() any {
			var v any = "leaf"
			for range deep {
				v = []map[string]any{{"k": v}}
			}
			return v
		},
		"array": func() any {
			var v any = "leaf"
			for range deep {
				v = [1]any{v}
			}
			return v
		},
		"struct": func() any {
			var v any = "leaf"
			for range deep {
				v = box{X: v}
			}
			return v
		},
		"pointer": func() any {
			var v any = "leaf"
			for range deep {
				x := v
				v = &x
			}
			return v
		},
	}
	for name, build := range kinds {
		t.Run("value-kind/"+name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panicked: %v", r)
				}
			}()
			node := &SchemaNode{Type: "int", Props: map[string]any{"x": build()}}
			_, err := node.Schema()
			if err == nil || !strings.Contains(err.Error(), "value nests deeper") {
				t.Fatalf("want the value depth bound to fire for %s, got %v", name, err)
			}
		})
	}
}

// Depth is not the only unbounded axis: a shared-reference DAG nests SHALLOW yet
// fans out into a 2^depth tree when serialized, because neither toJSONWalk nor
// json.Marshal memoizes shared references. The depth bound (which caps the
// longest PATH) is blind to it — depth 60 here is ~120 allocated nodes but 2^60
// emitted nodes, which would hang/OOM the process before Schema's eventual Parse
// runs. The node-count budget bounds the total emitted nodes across the whole
// walk (structural plus every value), shared so the combined json.Marshal cost
// stays bounded. This pins the expansion axis on every channel — the cell the
// three prior depth bounds all missed.
func TestRegression_SchemaNodeSharedDAGExpansionBounded(t *testing.T) {
	const fanout = 60 // 2^60 emitted nodes if unbounded; ~120 nodes in memory

	// Run the build in a goroutine so a REGRESSION (a removed budget → an
	// unbounded fan-out) fails this test via the timeout instead of hanging the
	// whole suite. Post-fix the budget rejects in well under a second.
	noHangReject := func(t *testing.T, build func() (*Schema, error), wantMsg string) {
		t.Helper()
		ch := make(chan error, 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					ch <- fmt.Errorf("panicked: %v", r)
				}
			}()
			_, err := build()
			ch <- err
		}()
		select {
		case err := <-ch:
			if err == nil || !strings.Contains(err.Error(), wantMsg) {
				t.Fatalf("want %q, got %v", wantMsg, err)
			}
		case <-time.After(hangDeadline):
			t.Fatalf("did not reject within 30s — the node budget is not bounding the fan-out")
		}
	}

	// Structural fan-out: each level's Items AND Values point at the SAME child,
	// so toJSONWalk re-walks it twice per level (the visited map is path-scoped,
	// so off-path sharing is not a cycle).
	t.Run("structural", func(t *testing.T) {
		noHangReject(t, func() (*Schema, error) {
			cur := &SchemaNode{Type: "long"}
			for range fanout {
				cur = &SchemaNode{Type: "array", Items: cur, Values: cur}
			}
			return cur.Schema()
		}, "tree expands to more")
	})

	// Value fan-out at each of the three value sites: a map whose two keys share
	// the same child value, compounded per level.
	valDAG := func() any {
		var v any = "leaf"
		for range fanout {
			inner := v
			v = map[string]any{"a": inner, "b": inner}
		}
		return v
	}
	valueSites := map[string]func(any) *SchemaNode{
		"node-props": func(v any) *SchemaNode {
			return &SchemaNode{Type: "int", Props: map[string]any{"x": v}}
		},
		"field-props": func(v any) *SchemaNode {
			return &SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{
				{Name: "f", Type: SchemaNode{Type: "int"}, Props: map[string]any{"x": v}},
			}}
		},
		"field-default": func(v any) *SchemaNode {
			return &SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{
				{Name: "f", HasDefault: true, Default: v, Type: SchemaNode{Type: "int"}},
			}}
		},
	}
	for name, build := range valueSites {
		t.Run("value-site/"+name, func(t *testing.T) {
			noHangReject(t, func() (*Schema, error) { return build(valDAG()).Schema() }, "value expands to more")
		})
	}

	// The bare path (SchemaFor's hand-built CustomType.Schema, walked via toJSON
	// with d==nil) truncates over-budget subtrees rather than erroring, so the
	// invariant is simply that it TERMINATES instead of fanning out forever.
	t.Run("schemafor-bare-path", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panicked: %v", r)
			}
		}()
		type recForCustom struct{ F int32 }
		ct := CustomType{
			GoType: reflect.TypeOf(int32(0)),
			Schema: &SchemaNode{Type: "int", Props: map[string]any{"x": valDAG()}},
		}
		done := make(chan struct{})
		go func() {
			_, _ = SchemaFor[recForCustom](WithCustomType(ct))
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(hangDeadline):
			t.Fatal("SchemaFor on a shared-DAG CustomType.Schema did not terminate")
		}
	})

	// Boundary: the bound rejects compounding FAN-OUT, not all sharing. A benign
	// shallow double-reference (json.Marshal expands it to two copies, cheaply)
	// must still build — and a structural reuse of a named sub-node must dedup to
	// a reference, not be rejected as fan-out.
	t.Run("benign-sharing-builds", func(t *testing.T) {
		shared := map[string]any{"k": "v"}
		node := &SchemaNode{Type: "int", Props: map[string]any{"a": shared, "b": shared}}
		if _, err := node.Schema(); err != nil {
			t.Fatalf("a benign shallow double-reference should build, got %v", err)
		}
		inner := &SchemaNode{Type: "record", Name: "Inner", Fields: []SchemaField{{Name: "n", Type: SchemaNode{Type: "long"}}}}
		reuse := &SchemaNode{Type: "record", Name: "Outer", Fields: []SchemaField{
			{Name: "a", Type: SchemaNode{Type: "array", Items: inner}},
			{Name: "b", Type: SchemaNode{Type: "array", Items: inner}},
		}}
		if _, err := reuse.Schema(); err != nil {
			t.Fatalf("reusing a named sub-node (deduped to a ref) should build, got %v", err)
		}
	})
}

// TestRegression_SchemaNodeDuplicateNamedDefinitionBounded pins that
// SchemaNode.Schema()'s dedup conflict check bounds its cost by the SHARED
// per-walk node budget. When a named type re-occurs as a DISTINCT pointer with
// an identical body, the body is marshal-compared against the first definition
// to detect a conflicting redefinition; that comparison must charge the same
// maxSchemaJSONNodes budget the rest of the walk uses (toJSONShared), so k
// identical-bodied copies of a w-node definition cost O(maxSchemaJSONNodes),
// not O(k*w) -- even though the emitted schema is tiny (one definition plus
// k-1 name references). If the comparison re-marshals each copy on a fresh
// budget, k*w can reach the budget SQUARED while k+w stays within it, an
// amplification reachable from a hand-built node via the public Schema() API.
func TestRegression_SchemaNodeDuplicateNamedDefinitionBounded(t *testing.T) {
	// "Dup": a record with w long fields. The outer record references it via k
	// distinct-pointer copies (value copies => &Fields[i].Type are distinct,
	// bodies byte-identical). Valid Avro: a record may have many fields of one
	// named type, so the dedup yields one Dup definition + k-1 references.
	buildDup := func(w int) SchemaNode {
		fields := make([]SchemaField, w)
		for i := range fields {
			fields[i] = SchemaField{Name: fmt.Sprintf("g%d", i), Type: SchemaNode{Type: "long"}}
		}
		return SchemaNode{Type: "record", Name: "Dup", Fields: fields}
	}
	buildOuter := func(dup SchemaNode, k int) *SchemaNode {
		fields := make([]SchemaField, k)
		for i := range fields {
			fields[i] = SchemaField{Name: fmt.Sprintf("f%d", i), Type: dup}
		}
		return &SchemaNode{Type: "record", Name: "Outer", Fields: fields}
	}

	t.Run("many-large-duplicates-bounded", func(t *testing.T) {
		// w*k = 2.25M; an unbounded conflict marshal would re-emit ~2*w*k nodes
		// (far past maxSchemaJSONNodes) while the output is one Dup def + refs.
		// The shared budget caps total work and rejects over-budget. Run in a
		// goroutine so a regression (fresh-budget re-marshal restored) surfaces
		// as the timeout instead of hanging the suite; the bounded reject
		// finishes well under it.
		node := buildOuter(buildDup(1500), 1500)
		ch := make(chan error, 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					ch <- fmt.Errorf("panicked: %v", r)
				}
			}()
			_, err := node.Schema()
			ch <- err
		}()
		select {
		case err := <-ch:
			if err == nil || !strings.Contains(err.Error(), "expands to more") {
				t.Fatalf("want an over-budget rejection (the conflict marshal must share the node budget), got %v", err)
			}
		case <-time.After(hangDeadline):
			t.Fatal("did not complete within 30s — the conflict-comparison marshal is not bounded by the shared node budget")
		}
	})

	t.Run("legit-duplication-builds", func(t *testing.T) {
		// A handful of distinct-pointer copies of a small named type build fine
		// and dedup to one definition + references — the boundary the bound
		// must NOT false-reject. (If dedup failed, Parse would reject the
		// repeated "Dup" name, so a successful build proves the dedup ran.)
		s, err := buildOuter(buildDup(3), 5).Schema()
		if err != nil {
			t.Fatalf("legitimate small duplication should build, got %v", err)
		}
		if _, err := Parse(s.String()); err != nil {
			t.Fatalf("deduped schema must re-parse, got %v", err)
		}
	})

	t.Run("conflicting-bodies-errors", func(t *testing.T) {
		// Distinct-pointer, same fullname, DIFFERENT bodies => a genuine
		// redefinition conflict the bound must still surface.
		a := SchemaNode{Type: "record", Name: "C", Fields: []SchemaField{{Name: "x", Type: SchemaNode{Type: "long"}}}}
		b := SchemaNode{Type: "record", Name: "C", Fields: []SchemaField{{Name: "y", Type: SchemaNode{Type: "string"}}}}
		node := &SchemaNode{Type: "record", Name: "Outer", Fields: []SchemaField{
			{Name: "a", Type: a}, {Name: "b", Type: b},
		}}
		if _, err := node.Schema(); err == nil || !strings.Contains(err.Error(), "conflicting definitions") {
			t.Fatalf("want a conflicting-definitions error, got %v", err)
		}
	})
}

func TestSchemaNodeUnmarshalablePropsErrors(t *testing.T) {
	// json.Marshal rejects channels, funcs, complex numbers.
	node := SchemaNode{
		Type:  "int",
		Props: map[string]any{"bad": make(chan int)},
	}
	if _, err := node.Schema(); err == nil {
		t.Fatal("expected error for unmarshalable Props value")
	}
}

// A fixed with a quoted-string size ("16", the spec [INTEGERS] form) must
// surface Size in the metadata tree and round-trip through Root().Schema().
// jsonNumericInt previously handled only numeric forms, so Root().Size was
// 0 and Root().Schema() failed "fixed is missing size" (a Pattern-13b
// metadata bug: Parse accepted the quoted size but no test read it back).
func TestRegression_RootQuotedFixedSize(t *testing.T) {
	for _, schema := range []string{
		`{"type":"fixed","name":"F","size":"16"}`,
		`{"type":"fixed","name":"F","size":"016"}`,
		`{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"fixed","name":"G","size":"4"}}]}`,
	} {
		s := MustParse(schema)
		node := s.Root()
		rt, err := node.Schema()
		if err != nil {
			t.Errorf("%s: Root().Schema(): %v", schema, err)
			continue
		}
		if string(s.Canonical()) != string(rt.Canonical()) {
			t.Errorf("%s: canonical changed across Root().Schema():\n %s\n %s", schema, s.Canonical(), rt.Canonical())
		}
	}
}

// TestRegression_SchemaNodeWalkBudgetBattery is THE consolidated DoS battery for
// the SchemaNode→JSON metadata walk reached via the public SchemaNode.Schema()
// (dedup path, d != nil, errors) and the bare toJSON() SchemaFor reaches via a
// hand-built CustomType.Schema (d == nil, truncates). The walk has THREE
// independently-unbounded axes, and a hand-built node drives cost through every
// recursion point, fan-out point, AND per-node payload on each:
//
//   - DEPTH (maxSchemaJSONDepth): the longest container PATH. Unbounded → the
//     fixup walk or json.Marshal overflows the goroutine stack uncatchably.
//   - NODES (maxSchemaJSONNodes): the COUNT of emitted JSON nodes (objects,
//     array elements, every enum symbol and alias). Unbounded → a shared-
//     reference DAG, tiny in memory, fans out into a 2^depth tree.
//   - BYTES (maxSchemaJSONBytes): the SIZE of every emitted scalar payload
//     (type/name/namespace/doc/.../symbols/aliases strings, Props keys, string/
//     []byte values). Unbounded → a huge or widely-shared string/slice — stored
//     by reference, invisible to the node count, re-expanded by json.Marshal —
//     blows the output past memory while the node count stays tiny.
//
// Five prior rounds each dribbled ONE bound here (structural depth 46d4dde,
// value depth 7f13cf9, typed-value depth 01b0b32, node fan-out 885e132, dedup
// conflict-marshal e76cd84) — a process failure. This battery drives the whole
// surface at once: a later schema_node-walk DoS find is expected to EXTEND it
// (proving the enumeration here was incomplete), not to be bounded from scratch
// in a fresh one-off test. Every cell isolates ONE hostile payload to ONE charge
// site (everything else tiny) and asserts the bound-specific message — which no
// other code emits (see grep in schema_node.go) — so a cell cannot pass on an
// unrelated Parse error and a removed charge turns exactly its cell red. Boundary
// cells pin that a usable schema is never false-rejected.
func TestRegression_SchemaNodeWalkBudgetBattery(t *testing.T) {
	wantBytes := fmt.Sprintf("supported %d bytes", maxSchemaJSONBytes)
	wantNodes := fmt.Sprintf("supported %d nodes", maxSchemaJSONNodes)

	// huge is one byte past the output-size budget: any single emission of it
	// trips the byte bound alone. Allocated ONCE and shared by reference across
	// cells, so the battery's footprint is one budget-sized string, not one per
	// cell. hugeBytes is its []byte sibling for the bytes/fixed value channel.
	huge := strings.Repeat("x", maxSchemaJSONBytes+1)
	hugeBytes := make([]byte, maxSchemaJSONBytes+1)

	// reject runs build() in a goroutine so a REGRESSION (a removed bound →
	// unbounded output/hang) surfaces as the timeout rather than wedging the
	// suite; the bounded reject returns in well under it. want is the bound-
	// specific fragment, so the cell fails unless THAT bound fired.
	reject := func(t *testing.T, want string, build func() (*Schema, error)) {
		t.Helper()
		ch := make(chan error, 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					ch <- fmt.Errorf("panicked: %v", r)
				}
			}()
			_, err := build()
			ch <- err
		}()
		select {
		case err := <-ch:
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("want a bounded error containing %q, got %v", want, err)
			}
		case <-time.After(hangDeadline):
			t.Fatalf("did not reject within 30s — the %q bound is not firing", want)
		}
	}

	// Axis BYTES — per-node scalar payload. Each cell carries `huge` (or its
	// []byte form) at exactly one emission/charge site; nothing else is large,
	// so only that site's byte charge can fire.
	t.Run("bytes/scalar-payload", func(t *testing.T) {
		intField := []SchemaField{{Name: "f", Type: SchemaNode{Type: "int"}}}
		cells := map[string]func() (*Schema, error){
			// toJSONWalk top charge (Type / Name / Namespace), charged before the
			// fullname is hashed into the dedup map or emitted as a reference.
			"node-type": func() (*Schema, error) { return (&SchemaNode{Type: huge}).Schema() },
			"node-name": func() (*Schema, error) { return (&SchemaNode{Type: "record", Name: huge, Fields: intField}).Schema() },
			"node-namespace": func() (*Schema, error) {
				return (&SchemaNode{Type: "record", Name: "R", Namespace: huge, Fields: intField}).Schema()
			},
			// node-level scalar strings.
			"node-doc":         func() (*Schema, error) { return (&SchemaNode{Type: "fixed", Name: "F", Size: 1, Doc: huge}).Schema() },
			"node-logicalType": func() (*Schema, error) { return (&SchemaNode{Type: "int", LogicalType: huge}).Schema() },
			"enum-default": func() (*Schema, error) {
				return (&SchemaNode{Type: "enum", Name: "E", Symbols: []string{"A"}, HasEnumDefault: true, EnumDefault: huge}).Schema()
			},
			// field-level scalar strings.
			"field-name": func() (*Schema, error) {
				return (&SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{{Name: huge, Type: SchemaNode{Type: "int"}}}}).Schema()
			},
			"field-order": func() (*Schema, error) {
				return (&SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{{Name: "f", Order: huge, Type: SchemaNode{Type: "int"}}}}).Schema()
			},
			"field-doc": func() (*Schema, error) {
				return (&SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{{Name: "f", Doc: huge, Type: SchemaNode{Type: "int"}}}}).Schema()
			},
			// Props object keys (the VALUE channel charges values; the KEY string
			// is charged separately as an emitted object key).
			"node-props-key": func() (*Schema, error) { return (&SchemaNode{Type: "int", Props: map[string]any{huge: "v"}}).Schema() },
			"field-props-key": func() (*Schema, error) {
				return (&SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{{Name: "f", Type: SchemaNode{Type: "int"}, Props: map[string]any{huge: "v"}}}}).Schema()
			},
			// Value leaves walked by valueWalkLimit (Props / Default channel).
			"value-string": func() (*Schema, error) { return (&SchemaNode{Type: "int", Props: map[string]any{"x": huge}}).Schema() },
			"value-jsonNumber": func() (*Schema, error) {
				return (&SchemaNode{Type: "int", Props: map[string]any{"x": json.Number(huge)}}).Schema()
			},
			"value-bytes": func() (*Schema, error) {
				return (&SchemaNode{Type: "int", Props: map[string]any{"x": hugeBytes}}).Schema()
			},
			"value-map-key": func() (*Schema, error) {
				return (&SchemaNode{Type: "int", Props: map[string]any{"x": map[string]any{huge: "v"}}}).Schema()
			},
			"value-struct-field-name": func() (*Schema, error) {
				// json.Marshal emits a struct field name as an object key; a
				// pathological StructOf type carries a huge one. Fan a 1 MiB
				// field name across enough instances to clear the budget.
				ft := reflect.StructOf([]reflect.StructField{{Name: "F" + strings.Repeat("a", 1<<20), Type: reflect.TypeOf(0)}})
				n := maxSchemaJSONBytes/(1<<20) + 8
				return (&SchemaNode{Type: "int", Props: map[string]any{"x": reflect.MakeSlice(reflect.SliceOf(ft), n, n).Interface()}}).Schema()
			},
		}
		for name, build := range cells {
			t.Run(name, func(t *testing.T) { reject(t, wantBytes, build) })
		}
	})

	// Axis NODES — string SLICES emit one array node per element, so their
	// element COUNT (not just the structural node) is charged. A slice one past
	// the node budget trips it on its own.
	t.Run("nodes/slice-payload", func(t *testing.T) {
		bigSlice := make([]string, maxSchemaJSONNodes+1) // empty strings: nodes axis, not bytes
		cells := map[string]func() (*Schema, error){
			"symbols": func() (*Schema, error) { return (&SchemaNode{Type: "enum", Name: "E", Symbols: bigSlice}).Schema() },
			"node-aliases": func() (*Schema, error) {
				return (&SchemaNode{Type: "fixed", Name: "F", Size: 1, Aliases: bigSlice}).Schema()
			},
			"field-aliases": func() (*Schema, error) {
				return (&SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{{Name: "f", Type: SchemaNode{Type: "int"}, Aliases: bigSlice}}}).Schema()
			},
		}
		for name, build := range cells {
			t.Run(name, func(t *testing.T) { reject(t, wantNodes, build) })
		}
	})

	// The shared-reference DAG — the exact shape that is O(K+L) in memory but
	// K*L in json.Marshal's output. The 885e132 node bound closed it for
	// STRUCTURE; these close it for leaf PAYLOAD (and confirm the dedup-
	// reference and dedup-map-hashing paths are bounded too).
	t.Run("shared-dag-amplification", func(t *testing.T) {
		shared := strings.Repeat("x", 1<<20) // 1 MiB, shared by reference
		refs := maxSchemaJSONBytes/len(shared) + 8

		// One shared 1 MiB doc fanned across many distinct named branches.
		t.Run("doc-across-branches", func(t *testing.T) {
			reject(t, wantBytes, func() (*Schema, error) {
				branches := make([]SchemaNode, refs)
				for i := range branches {
					branches[i] = SchemaNode{Type: "fixed", Name: fmt.Sprintf("F%d", i), Size: 1, Doc: shared}
				}
				return (&SchemaNode{Type: "union", Branches: branches}).Schema()
			})
		})

		// A 1 MiB-named record referenced via many distinct-pointer copies → one
		// definition + refs-1 bare references, each re-emitting (and re-hashing)
		// the 1 MiB fullname. Bounded by the top charge, which runs before the
		// dedup map touches the name.
		t.Run("name-via-dedup-references", func(t *testing.T) {
			reject(t, wantBytes, func() (*Schema, error) {
				dup := SchemaNode{Type: "record", Name: shared, Fields: []SchemaField{{Name: "g", Type: SchemaNode{Type: "long"}}}}
				fields := make([]SchemaField, refs)
				for i := range fields {
					fields[i] = SchemaField{Name: fmt.Sprintf("f%d", i), Type: dup}
				}
				return (&SchemaNode{Type: "record", Name: "Outer", Fields: fields}).Schema()
			})
		})

		// One shared symbols slice fanned across distinct-named enums → the node
		// budget (slice elements) prunes the fan-out.
		t.Run("symbols-slice-across-enums", func(t *testing.T) {
			reject(t, wantNodes, func() (*Schema, error) {
				sym := make([]string, maxSchemaJSONNodes/4)
				branches := make([]SchemaNode, 6) // 6 * (budget/4) > budget
				for i := range branches {
					branches[i] = SchemaNode{Type: "enum", Name: fmt.Sprintf("E%d", i), Symbols: sym}
				}
				return (&SchemaNode{Type: "union", Branches: branches}).Schema()
			})
		})
	})

	// Axis DEPTH — structural and value channels (the 46d4dde / 7f13cf9 cells,
	// driven here too so this battery covers the whole surface).
	t.Run("depth", func(t *testing.T) {
		const deep = maxSchemaJSONDepth + 50
		t.Run("structural", func(t *testing.T) {
			reject(t, "SchemaNode tree nests deeper", func() (*Schema, error) {
				cur := &SchemaNode{Type: "long"}
				for range deep {
					cur = &SchemaNode{Type: "array", Items: cur}
				}
				return cur.Schema()
			})
		})
		t.Run("value", func(t *testing.T) {
			reject(t, "value nests deeper", func() (*Schema, error) {
				var v any = "leaf"
				for range deep {
					v = map[string]any{"k": v}
				}
				return (&SchemaNode{Type: "int", Props: map[string]any{"x": v}}).Schema()
			})
		})
	})

	// Axis NODES — structural and value shared-DAG fan-out (the 885e132 cells).
	t.Run("fanout", func(t *testing.T) {
		t.Run("structural", func(t *testing.T) {
			reject(t, wantNodes, func() (*Schema, error) {
				cur := &SchemaNode{Type: "long"}
				for range 60 {
					cur = &SchemaNode{Type: "array", Items: cur, Values: cur}
				}
				return cur.Schema()
			})
		})
		t.Run("value", func(t *testing.T) {
			reject(t, "value expands", func() (*Schema, error) {
				var v any = "leaf"
				for range 60 {
					inner := v
					v = map[string]any{"a": inner, "b": inner}
				}
				return (&SchemaNode{Type: "int", Props: map[string]any{"x": v}}).Schema()
			})
		})
	})

	// The dedup conflict-comparison marshal (the e76cd84 axis): many large-bodied
	// distinct-pointer duplicates of one named type must be bounded by the SHARED
	// budget, not re-marshalled on a fresh one.
	t.Run("dedup-conflict-marshal", func(t *testing.T) {
		reject(t, "expands to more", func() (*Schema, error) {
			fields := make([]SchemaField, 1500)
			for i := range fields {
				fields[i] = SchemaField{Name: fmt.Sprintf("g%d", i), Type: SchemaNode{Type: "long"}}
			}
			dup := SchemaNode{Type: "record", Name: "Dup", Fields: fields}
			outer := make([]SchemaField, 1500)
			for i := range outer {
				outer[i] = SchemaField{Name: fmt.Sprintf("f%d", i), Type: dup}
			}
			return (&SchemaNode{Type: "record", Name: "Outer", Fields: outer}).Schema()
		})
	})

	// The bare path (SchemaFor's hand-built CustomType.Schema, toJSON with
	// d == nil) truncates over-budget output rather than erroring, so the
	// invariant is that it TERMINATES — no uncatchable stack overflow, no
	// unbounded fan-out, no multi-GB marshal — on every axis.
	t.Run("bare-path-terminates", func(t *testing.T) {
		type recForCustom struct{ F int32 }
		run := func(t *testing.T, sn *SchemaNode) {
			t.Helper()
			done := make(chan struct{})
			go func() {
				defer func() {
					_ = recover()
					close(done)
				}()
				_, _ = SchemaFor[recForCustom](WithCustomType(CustomType{GoType: reflect.TypeOf(int32(0)), Schema: sn}))
			}()
			select {
			case <-done:
			case <-time.After(hangDeadline):
				t.Fatal("bare-path walk did not terminate")
			}
		}
		t.Run("payload-bytes-doc", func(t *testing.T) { run(t, &SchemaNode{Type: "fixed", Name: "F", Size: 1, Doc: huge}) })
		t.Run("payload-bytes-value", func(t *testing.T) { run(t, &SchemaNode{Type: "int", Props: map[string]any{"x": huge}}) })
		t.Run("slice-symbols", func(t *testing.T) {
			run(t, &SchemaNode{Type: "enum", Name: "E", Symbols: make([]string, maxSchemaJSONNodes+1)})
		})
		t.Run("shared-dag-value", func(t *testing.T) {
			var v any = "leaf"
			for range 60 {
				inner := v
				v = map[string]any{"a": inner, "b": inner}
			}
			run(t, &SchemaNode{Type: "int", Props: map[string]any{"x": v}})
		})
	})

	// Boundary — a usable schema well under each bound must still build and
	// round-trip. The bounds reject the pathological, never the merely large.
	t.Run("boundary-usable-builds", func(t *testing.T) {
		t.Run("large-doc", func(t *testing.T) {
			doc := strings.Repeat("d", 1<<20) // 1 MiB, far under the 64 MiB byte budget
			s, err := (&SchemaNode{Type: "record", Name: "R", Doc: doc, Fields: []SchemaField{{Name: "f", Type: SchemaNode{Type: "int"}}}}).Schema()
			if err != nil {
				t.Fatalf("a 1 MiB doc should build, got %v", err)
			}
			if s.Root().Doc != doc {
				t.Fatal("the usable-size doc was dropped or truncated")
			}
		})
		t.Run("many-symbols", func(t *testing.T) {
			syms := make([]string, 10_000) // 10k symbols, far under the 1 MiB node budget
			for i := range syms {
				syms[i] = fmt.Sprintf("S%d", i)
			}
			if _, err := (&SchemaNode{Type: "enum", Name: "E", Symbols: syms}).Schema(); err != nil {
				t.Fatalf("a 10k-symbol enum should build, got %v", err)
			}
		})
		t.Run("benign-shared-payload", func(t *testing.T) {
			// A handful of copies of a moderate string: json.Marshal expands it to
			// a few copies, cheaply. The bound rejects compounding amplification,
			// not all sharing.
			shared := strings.Repeat("x", 1<<10)
			branches := make([]SchemaNode, 8)
			for i := range branches {
				branches[i] = SchemaNode{Type: "fixed", Name: fmt.Sprintf("F%d", i), Size: 1, Doc: shared}
			}
			if _, err := (&SchemaNode{Type: "union", Branches: branches}).Schema(); err != nil {
				t.Fatalf("a benign shared 1 KiB doc across 8 branches should build, got %v", err)
			}
		})
	})
}
