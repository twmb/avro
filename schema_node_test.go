package avro

import (
	"reflect"
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

	got, err := s.Root()
	if err != nil {
		t.Fatal(err)
	}

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
			got, err := s.Root()
			if err != nil {
				t.Fatal(err)
			}
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
			got, err := s.Root()
			if err != nil {
				t.Fatal(err)
			}
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
	got, err := s.Root()
	if err != nil {
		t.Fatal(err)
	}
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
	got, err := s.Root()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Symbols, []string{"RED", "GREEN", "BLUE"}) {
		t.Errorf("symbols: got %v", got.Symbols)
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
	got, err := s.Root()
	if err != nil {
		t.Fatal(err)
	}
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
	got, err := s.Root()
	if err != nil {
		t.Fatal(err)
	}
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
	got, err := s.Root()
	if err != nil {
		t.Fatal(err)
	}
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
	got, err := s.Root()
	if err != nil {
		t.Fatal(err)
	}
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
	got, err := s.Root()
	if err != nil {
		t.Fatal(err)
	}
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
	got, err := s.Root()
	if err != nil {
		t.Fatal(err)
	}
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
	got, err := s.Root()
	if err != nil {
		t.Fatal(err)
	}
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
	got, err := s.Root()
	if err != nil {
		t.Fatal(err)
	}
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
		Name     string            `avro:"name"`
		Price    float64           `avro:"price"`
		Tags     []string          `avro:"tags"`
		Metadata map[string]int32  `avro:"metadata"`
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
	got, err := s.Root()
	if err != nil {
		t.Fatal(err)
	}

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
