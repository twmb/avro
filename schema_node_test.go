package avro

import (
	"reflect"
	"strings"
	"testing"
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
	if got.Fields[1].Name != "age" || got.Fields[1].Default != float64(18) {
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
// produce byte-identical output. This is especially important for
// empty records where the parser accepts the lenient form but the
// emitter must produce the strict form.
func TestSchemaNodeCanonicalIdempotent(t *testing.T) {
	inputs := []string{
		`{"type":"record","name":"Empty","fields":[]}`,
		`{"type":"record","name":"Empty"}`, // lenient, missing fields
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
	if root.Props["custom.num"] != float64(42) {
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
