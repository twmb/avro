package conformance

import (
	"reflect"
	"testing"
)

// -----------------------------------------------------------------------
// Complex Nesting Scenarios
// Spec: "Complex Types" — composition of records, arrays, maps, unions,
// enums, and fixed types in arbitrarily nested structures.
// https://avro.apache.org/docs/1.12.0/specification/#complex-types
// -----------------------------------------------------------------------

func TestNestingArrayOfRecords(t *testing.T) {
	schema := `{
		"type": "array",
		"items": {
			"type": "record",
			"name": "Item",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "name", "type": "string"}
			]
		}
	}`
	type Item struct {
		ID   int32  `avro:"id"`
		Name string `avro:"name"`
	}
	input := []Item{{1, "a"}, {2, "b"}, {3, "c"}}
	got := roundTrip(t, schema, input)
	if !reflect.DeepEqual(got, input) {
		t.Fatalf("got %+v, want %+v", got, input)
	}
}

func TestNestingMapOfArrays(t *testing.T) {
	schema := `{"type": "map", "values": {"type": "array", "items": "int"}}`
	input := map[string][]int32{
		"evens": {2, 4, 6},
		"odds":  {1, 3, 5},
	}
	got := roundTrip(t, schema, input)
	if !reflect.DeepEqual(got, input) {
		t.Fatalf("got %+v, want %+v", got, input)
	}
}

func TestNestingUnionOfRecords(t *testing.T) {
	schema := `[
		"null",
		{"type":"record","name":"Cat","fields":[{"name":"lives","type":"int"}]},
		{"type":"record","name":"Dog","fields":[{"name":"breed","type":"string"}]}
	]`

	// Encode a Cat.
	v := any(map[string]any{"lives": int32(9)})
	dst := encode(t, schema, &v)
	if dst[0] != 0x02 {
		t.Fatalf("Cat branch: got %x, want 02", dst[0])
	}
	var result any
	decode(t, schema, dst, &result)
	m := result.(map[string]any)
	if m["lives"] != int32(9) {
		t.Fatalf("got lives=%v, want 9", m["lives"])
	}

	// Encode a Dog.
	v = any(map[string]any{"breed": "lab"})
	dst = encode(t, schema, &v)
	if dst[0] != 0x04 {
		t.Fatalf("Dog branch: got %x, want 04", dst[0])
	}
	decode(t, schema, dst, &result)
	m = result.(map[string]any)
	if m["breed"] != "lab" {
		t.Fatalf("got breed=%v, want lab", m["breed"])
	}
}

func TestNestingRecordInRecordInRecord(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "L1",
		"fields": [{
			"name": "l2",
			"type": {
				"type": "record",
				"name": "L2",
				"fields": [{
					"name": "l3",
					"type": {
						"type": "record",
						"name": "L3",
						"fields": [{"name": "val", "type": "string"}]
					}
				}]
			}
		}]
	}`
	type L3 struct {
		Val string `avro:"val"`
	}
	type L2 struct {
		L3 L3 `avro:"l3"`
	}
	type L1 struct {
		L2 L2 `avro:"l2"`
	}
	input := L1{L2: L2{L3: L3{Val: "deep"}}}
	got := roundTrip(t, schema, input)
	if got.L2.L3.Val != "deep" {
		t.Fatalf("got %q, want deep", got.L2.L3.Val)
	}
}

func TestNestingArrayOfUnions(t *testing.T) {
	// Array of union items: decode from known bytes.
	schema := `{"type": "array", "items": ["null", "int", "string"]}`

	// count=3 (zigzag 6=0x06),
	// item0: null branch (0x00),
	// item1: int branch (0x02), value 42 (zigzag 84=0x54),
	// item2: string branch (0x04), len 2 (zigzag 4=0x04), "hi",
	// terminator: 0x00
	data := []byte{0x06, 0x00, 0x02, 0x54, 0x04, 0x04, 0x68, 0x69, 0x00}
	var output []any
	decode(t, schema, data, &output)
	if len(output) != 3 {
		t.Fatalf("length: got %d, want 3", len(output))
	}
	if output[0] != nil {
		t.Fatalf("output[0]: got %v, want nil", output[0])
	}
	if output[1] != int32(42) {
		t.Fatalf("output[1]: got %v, want 42", output[1])
	}
	if output[2] != "hi" {
		t.Fatalf("output[2]: got %v, want hi", output[2])
	}
}

func TestNestingMapOfUnions(t *testing.T) {
	schema := `{"type": "map", "values": ["null", "int"]}`
	s := mustParse(t, schema)
	input := map[string]any{"a": int32(1), "b": nil}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatal(err)
	}
	var output map[string]any
	_, err = s.Decode(encoded, &output)
	if err != nil {
		t.Fatal(err)
	}
	if output["a"] != int32(1) {
		t.Fatalf("a: got %v, want 1", output["a"])
	}
	if output["b"] != nil {
		t.Fatalf("b: got %v, want nil", output["b"])
	}
}

func TestNestingRecordWithAllTypes(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "AllTypes",
		"fields": [
			{"name": "b", "type": "boolean"},
			{"name": "i", "type": "int"},
			{"name": "l", "type": "long"},
			{"name": "f", "type": "float"},
			{"name": "d", "type": "double"},
			{"name": "s", "type": "string"},
			{"name": "byt", "type": "bytes"},
			{"name": "arr", "type": {"type": "array", "items": "int"}},
			{"name": "m", "type": {"type": "map", "values": "string"}},
			{"name": "e", "type": {"type": "enum", "name": "Color", "symbols": ["RED", "GREEN", "BLUE"]}},
			{"name": "fix", "type": {"type": "fixed", "name": "F4", "size": 4}},
			{"name": "u", "type": ["null", "int"]}
		]
	}`
	s := mustParse(t, schema)
	type AllTypes struct {
		B   bool              `avro:"b"`
		I   int32             `avro:"i"`
		L   int64             `avro:"l"`
		F   float32           `avro:"f"`
		D   float64           `avro:"d"`
		S   string            `avro:"s"`
		Byt []byte            `avro:"byt"`
		Arr []int32           `avro:"arr"`
		M   map[string]string `avro:"m"`
		E   string            `avro:"e"`
		Fix [4]byte           `avro:"fix"`
		U   *int32            `avro:"u"`
	}
	uval := int32(99)
	input := AllTypes{
		B:   true,
		I:   42,
		L:   1000000,
		F:   2.5,
		D:   3.14159,
		S:   "test",
		Byt: []byte{0xFF},
		Arr: []int32{1, 2, 3},
		M:   map[string]string{"k": "v"},
		E:   "GREEN",
		Fix: [4]byte{0xDE, 0xAD, 0xBE, 0xEF},
		U:   &uval,
	}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatal(err)
	}
	var output AllTypes
	_, err = s.Decode(encoded, &output)
	if err != nil {
		t.Fatal(err)
	}
	if output.B != input.B || output.I != input.I || output.L != input.L ||
		output.S != input.S || output.E != input.E || output.Fix != input.Fix ||
		*output.U != *input.U {
		t.Fatalf("mismatch: got %+v, want %+v", output, input)
	}
}
