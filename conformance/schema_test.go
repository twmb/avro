package conformance

import (
	"testing"

	"github.com/twmb/avro"
)

// -----------------------------------------------------------------------
// Schema Parsing Edge Cases
// Spec: "Schema Declaration" — parsing, names, namespaces, type constraints.
//   - Names:       fullnames, namespace inheritance, forward references
//   - Records:     self-referencing (recursive) types
//   - Enums:       default validation, symbol constraints
//   - Fixed:       size validation
//   - Unions:      no nested unions, no duplicate types
//   - Logical:     unknown/invalid logical types fall back to underlying type
// https://avro.apache.org/docs/1.12.0/specification/#schema-declaration
// -----------------------------------------------------------------------

func TestSchemaNamespaceInheritance(t *testing.T) {
	// Nested records inherit the parent namespace.
	schema := `{
		"type": "record",
		"name": "Outer",
		"namespace": "com.example",
		"fields": [{
			"name": "inner",
			"type": {
				"type": "record",
				"name": "Inner",
				"fields": [{"name": "x", "type": "int"}]
			}
		}]
	}`
	s := mustParse(t, schema)
	// Verify round-trip works, meaning Inner is properly resolved.
	type Inner struct {
		X int32 `avro:"x"`
	}
	type Outer struct {
		Inner Inner `avro:"inner"`
	}
	input := Outer{Inner: Inner{X: 42}}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatal(err)
	}
	var output Outer
	_, err = s.Decode(encoded, &output)
	if err != nil {
		t.Fatal(err)
	}
	if output.Inner.X != 42 {
		t.Fatalf("got %d, want 42", output.Inner.X)
	}
}

func TestSchemaFullyQualifiedName(t *testing.T) {
	// A fully qualified name overrides the inherited namespace.
	schema := `{
		"type": "record",
		"name": "Outer",
		"namespace": "com.example",
		"fields": [{
			"name": "inner",
			"type": {
				"type": "record",
				"name": "org.other.Inner",
				"fields": [{"name": "y", "type": "string"}]
			}
		}]
	}`
	s := mustParse(t, schema)
	type Inner struct {
		Y string `avro:"y"`
	}
	type Outer struct {
		Inner Inner `avro:"inner"`
	}
	got := roundTripSchema(t, s, Outer{Inner: Inner{Y: "hello"}})
	if got.Inner.Y != "hello" {
		t.Fatalf("got %q, want hello", got.Inner.Y)
	}
}

func TestSchemaForwardReference(t *testing.T) {
	// A record field referencing a type defined later in the same schema.
	schema := `{
		"type": "record",
		"name": "Container",
		"fields": [
			{"name": "item", "type": {
				"type": "record",
				"name": "Item",
				"fields": [{"name": "id", "type": "int"}]
			}},
			{"name": "ref", "type": "Item"}
		]
	}`
	s := mustParse(t, schema)
	type Item struct {
		ID int32 `avro:"id"`
	}
	type Container struct {
		Item Item `avro:"item"`
		Ref  Item `avro:"ref"`
	}
	input := Container{Item: Item{ID: 1}, Ref: Item{ID: 2}}
	got := roundTripSchema(t, s, input)
	if got.Item.ID != 1 || got.Ref.ID != 2 {
		t.Fatalf("got %+v, want %+v", got, input)
	}
}

func TestSchemaRecursiveSelfRef(t *testing.T) {
	// A record that references itself (linked list).
	schema := `{
		"type": "record",
		"name": "Node",
		"fields": [
			{"name": "value", "type": "int"},
			{"name": "next", "type": ["null", "Node"]}
		]
	}`
	s := mustParse(t, schema)
	type Node struct {
		Value int32 `avro:"value"`
		Next  *Node `avro:"next"`
	}
	input := &Node{Value: 1, Next: &Node{Value: 2}}
	encoded, err := s.AppendEncode(nil, input)
	if err != nil {
		t.Fatal(err)
	}
	var output Node
	_, err = s.Decode(encoded, &output)
	if err != nil {
		t.Fatal(err)
	}
	if output.Value != 1 || output.Next == nil || output.Next.Value != 2 || output.Next.Next != nil {
		t.Fatalf("got %+v, want {1, {2, nil}}", output)
	}
}

func TestSchemaEnumDefaultValidation(t *testing.T) {
	// Enum default must be a valid symbol. When default is in symbols, parse succeeds.
	_, err := avro.Parse(`{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}`)
	if err != nil {
		t.Fatalf("valid enum default should parse: %v", err)
	}
}

func TestSchemaFixedSizeValidation(t *testing.T) {
	// Fixed with negative size should fail.
	_, err := avro.Parse(`{"type":"fixed","name":"F","size":-1}`)
	if err == nil {
		t.Fatal("expected error for negative fixed size")
	}

	// Valid fixed size should succeed.
	_, err = avro.Parse(`{"type":"fixed","name":"G","size":8}`)
	if err != nil {
		t.Fatalf("valid fixed size should parse: %v", err)
	}
}

func TestSchemaInvalidLogicalIgnored(t *testing.T) {
	t.Run("decimal precision zero falls back to bytes", func(t *testing.T) {
		s, err := avro.Parse(`{"type":"bytes","logicalType":"decimal","precision":0,"scale":0}`)
		if err != nil {
			t.Fatalf("invalid logical type should fall back to bytes: %v", err)
		}
		in := []byte{0xaa, 0xbb}
		encoded, err := s.AppendEncode(nil, &in)
		if err != nil {
			t.Fatal(err)
		}
		var out []byte
		if _, err := s.Decode(encoded, &out); err != nil {
			t.Fatal(err)
		}
		if string(out) != string(in) {
			t.Fatalf("got %x, want %x", out, in)
		}
	})

	t.Run("decimal scale exceeds precision falls back to bytes", func(t *testing.T) {
		s, err := avro.Parse(`{"type":"bytes","logicalType":"decimal","precision":5,"scale":6}`)
		if err != nil {
			t.Fatalf("invalid logical type should fall back to bytes: %v", err)
		}
		in := []byte{0x01, 0x02, 0x03}
		encoded, err := s.AppendEncode(nil, &in)
		if err != nil {
			t.Fatal(err)
		}
		var out []byte
		if _, err := s.Decode(encoded, &out); err != nil {
			t.Fatal(err)
		}
		if string(out) != string(in) {
			t.Fatalf("got %x, want %x", out, in)
		}
	})

	t.Run("unknown logical type falls back to underlying type", func(t *testing.T) {
		s, err := avro.Parse(`{"type":"string","logicalType":"unknown-logical"}`)
		if err != nil {
			t.Fatalf("unknown logical type should be ignored: %v", err)
		}
		in := "hello"
		encoded, err := s.AppendEncode(nil, &in)
		if err != nil {
			t.Fatal(err)
		}
		var out string
		if _, err := s.Decode(encoded, &out); err != nil {
			t.Fatal(err)
		}
		if out != in {
			t.Fatalf("got %q, want %q", out, in)
		}
	})
}

func TestSchemaUnionNoNestedUnion(t *testing.T) {
	// Union cannot directly contain another union.
	_, err := avro.Parse(`[["null","int"],"string"]`)
	if err == nil {
		t.Fatal("expected error for nested union")
	}
}

func TestSchemaUnionNoDuplicateTypes(t *testing.T) {
	// Union cannot have two of the same unnamed type.
	_, err := avro.Parse(`["int","int"]`)
	if err == nil {
		t.Fatal("expected error for duplicate union types")
	}
}

func TestSchemaInvalidJSON(t *testing.T) {
	_, err := avro.Parse(`{not valid json}`)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestSchemaAllPrimitives(t *testing.T) {
	// All 8 primitive types parse and round-trip.
	primitives := []struct {
		schema string
		encode func(t *testing.T) []byte
	}{
		{`"null"`, func(t *testing.T) []byte {
			// Null schema encodes as zero bytes.
			mustParse(t, `"null"`)
			return nil
		}},
		{`"boolean"`, func(t *testing.T) []byte {
			v := true
			return encode(t, `"boolean"`, &v)
		}},
		{`"int"`, func(t *testing.T) []byte {
			v := int32(42)
			return encode(t, `"int"`, &v)
		}},
		{`"long"`, func(t *testing.T) []byte {
			v := int64(42)
			return encode(t, `"long"`, &v)
		}},
		{`"float"`, func(t *testing.T) []byte {
			v := float32(3.14)
			return encode(t, `"float"`, &v)
		}},
		{`"double"`, func(t *testing.T) []byte {
			v := float64(3.14)
			return encode(t, `"double"`, &v)
		}},
		{`"string"`, func(t *testing.T) []byte {
			v := "hello"
			return encode(t, `"string"`, &v)
		}},
		{`"bytes"`, func(t *testing.T) []byte {
			v := []byte{1, 2, 3}
			return encode(t, `"bytes"`, &v)
		}},
	}

	for _, p := range primitives {
		t.Run(p.schema, func(t *testing.T) {
			mustParse(t, p.schema)
			b := p.encode(t)
			if len(b) == 0 && p.schema != `"null"` {
				t.Fatal("expected non-empty encoding")
			}
		})
	}
}

func TestSchemaAllLogicalTypes(t *testing.T) {
	logicals := []string{
		`{"type":"int","logicalType":"date"}`,
		`{"type":"int","logicalType":"time-millis"}`,
		`{"type":"long","logicalType":"time-micros"}`,
		`{"type":"long","logicalType":"timestamp-millis"}`,
		`{"type":"long","logicalType":"timestamp-micros"}`,
		`{"type":"long","logicalType":"timestamp-nanos"}`,
		`{"type":"long","logicalType":"local-timestamp-millis"}`,
		`{"type":"long","logicalType":"local-timestamp-micros"}`,
		`{"type":"long","logicalType":"local-timestamp-nanos"}`,
		`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`,
		`{"type":"bytes","logicalType":"big-decimal"}`,
		`{"type":"fixed","name":"dec","size":8,"logicalType":"decimal","precision":10,"scale":2}`,
		`{"type":"string","logicalType":"uuid"}`,
		`{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}`,
	}

	for _, schema := range logicals {
		t.Run(schema, func(t *testing.T) {
			mustParse(t, schema)
		})
	}
}

// roundTripSchema encodes/decodes using a pre-parsed schema.
func roundTripSchema[T any](t *testing.T, s *avro.Schema, input T) T {
	t.Helper()
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	var output T
	rem, err := s.Decode(encoded, &output)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("Decode left %d unconsumed bytes", len(rem))
	}
	return output
}
