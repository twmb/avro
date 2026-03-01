package avro

import (
	"bytes"
	"encoding"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"testing"
	"time"
	"unsafe"
)

func roundTrip[T any](t *testing.T, schema string, input T) T {
	t.Helper()
	s, err := Parse(schema)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
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

// encode is a test helper that encodes v with schema and returns the raw bytes.
func encode(t *testing.T, schema string, v any) []byte {
	t.Helper()
	s, err := Parse(schema)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	dst, err := s.AppendEncode(nil, v)
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	return dst
}

// decode is a test helper that decodes src into v with schema.
func decode(t *testing.T, schema string, src []byte, v any) {
	t.Helper()
	s, err := Parse(schema)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	rem, err := s.Decode(src, v)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("Decode left %d unconsumed bytes", len(rem))
	}
}

// decodeErr is a test helper that expects Decode to return an error.
func decodeErr(t *testing.T, schema string, src []byte, v any) {
	t.Helper()
	s, err := Parse(schema)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	_, err = s.Decode(src, v)
	if err == nil {
		t.Fatal("expected error from Decode, got nil")
	}
}

// -----------------------------------------------------------------------
// Wire-format tests: verify encoded bytes match Avro spec, and that
// decoding those bytes produces the correct value. These ensure
// interoperability with other Avro implementations.
// -----------------------------------------------------------------------

func TestWireFormatBoolean(t *testing.T) {
	dst := encode(t, `"boolean"`, new(true))
	if !bytes.Equal(dst, []byte{0x01}) {
		t.Fatalf("encode true: got %x, want 01", dst)
	}
	dst = encode(t, `"boolean"`, new(false))
	if !bytes.Equal(dst, []byte{0x00}) {
		t.Fatalf("encode false: got %x, want 00", dst)
	}

	var v bool
	decode(t, `"boolean"`, []byte{0x01}, &v)
	if !v {
		t.Fatal("decode 0x01: got false, want true")
	}
}

func TestWireFormatInt(t *testing.T) {
	// int 27 → zigzag 54 → 0x36
	dst := encode(t, `"int"`, new(int32(27)))
	if !bytes.Equal(dst, []byte{0x36}) {
		t.Fatalf("encode 27: got %x, want 36", dst)
	}

	var v int32
	decode(t, `"int"`, []byte{0x36}, &v)
	if v != 27 {
		t.Fatalf("decode 0x36: got %d, want 27", v)
	}
}

func TestWireFormatLong(t *testing.T) {
	// long 2147483648 → zigzag 4294967296 → multi-byte varint
	dst := encode(t, `"long"`, new(int64(2147483648)))
	if !bytes.Equal(dst, []byte{0x80, 0x80, 0x80, 0x80, 0x10}) {
		t.Fatalf("encode 2147483648: got %x, want 8080808010", dst)
	}

	var v int64
	decode(t, `"long"`, []byte{0x80, 0x80, 0x80, 0x80, 0x10}, &v)
	if v != 2147483648 {
		t.Fatalf("decode: got %d, want 2147483648", v)
	}
}

func TestWireFormatFloat(t *testing.T) {
	// float32(1.15) → bits 0x3F933333 → LE bytes 33 33 93 3F
	dst := encode(t, `"float"`, new(float32(1.15)))
	if !bytes.Equal(dst, []byte{0x33, 0x33, 0x93, 0x3F}) {
		t.Fatalf("encode 1.15f: got %x, want 3333933f", dst)
	}

	var v float32
	decode(t, `"float"`, []byte{0x33, 0x33, 0x93, 0x3F}, &v)
	if v != float32(1.15) {
		t.Fatalf("decode: got %v, want 1.15", v)
	}
}

func TestWireFormatDouble(t *testing.T) {
	// float64(1.15) → bits 0x3FF2666666666666 → LE bytes
	dst := encode(t, `"double"`, new(float64(1.15)))
	if !bytes.Equal(dst, []byte{0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0xF2, 0x3F}) {
		t.Fatalf("encode 1.15: got %x, want 66666666666666f23f", dst)
	}

	var v float64
	decode(t, `"double"`, []byte{0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0xF2, 0x3F}, &v)
	if v != 1.15 {
		t.Fatalf("decode: got %v, want 1.15", v)
	}
}

func TestWireFormatString(t *testing.T) {
	// "foo" → length 3, zigzag 6, then 0x66 0x6F 0x6F
	dst := encode(t, `"string"`, new("foo"))
	if !bytes.Equal(dst, []byte{0x06, 0x66, 0x6F, 0x6F}) {
		t.Fatalf("encode foo: got %x, want 06666f6f", dst)
	}

	var v string
	decode(t, `"string"`, []byte{0x06, 0x66, 0x6F, 0x6F}, &v)
	if v != "foo" {
		t.Fatalf("decode: got %q, want foo", v)
	}
}

func TestWireFormatBytes(t *testing.T) {
	// 4 bytes → length 4, zigzag 8 → 0x08 then raw
	b := []byte{0xEC, 0xAB, 0x44, 0x00}
	dst := encode(t, `"bytes"`, &b)
	if !bytes.Equal(dst, []byte{0x08, 0xEC, 0xAB, 0x44, 0x00}) {
		t.Fatalf("encode bytes: got %x, want 08ecab4400", dst)
	}

	var v []byte
	decode(t, `"bytes"`, []byte{0x08, 0xEC, 0xAB, 0x44, 0x00}, &v)
	if !bytes.Equal(v, b) {
		t.Fatalf("decode: got %x, want %x", v, b)
	}
}

func TestWireFormatRecord(t *testing.T) {
	type R struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}
	schema := `{"type":"record","name":"test","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}`

	// Record: long 27 (0x36), string "foo" (0x06 0x66 0x6f 0x6f)
	dst := encode(t, schema, &R{A: 27, B: "foo"})
	if !bytes.Equal(dst, []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}) {
		t.Fatalf("encode record: got %x, want 3606666f6f", dst)
	}

	var v R
	decode(t, schema, []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}, &v)
	if v.A != 27 || v.B != "foo" {
		t.Fatalf("decode record: got %+v, want {A:27 B:foo}", v)
	}
}

func TestWireFormatArray(t *testing.T) {
	// Array [27, 28]: count=2 (zigzag 4), items 0x36 0x38, end 0x00
	schema := `{"type":"array","items":"int"}`
	data := []byte{0x04, 0x36, 0x38, 0x00}

	var v []int32
	decode(t, schema, data, &v)
	if !reflect.DeepEqual(v, []int32{27, 28}) {
		t.Fatalf("decode array: got %v, want [27 28]", v)
	}
}

func TestWireFormatArrayNegativeBlockCount(t *testing.T) {
	// Negative block count: count=-2 (zigzag 3), block_size=2 (zigzag 4),
	// two items: int 27 (0x36), int 27 (0x36), end 0x00.
	schema := `{"type":"array","items":"int"}`
	data := []byte{0x03, 0x04, 0x36, 0x36, 0x00}

	var v []int32
	decode(t, schema, data, &v)
	if !reflect.DeepEqual(v, []int32{27, 27}) {
		t.Fatalf("decode array neg block: got %v, want [27 27]", v)
	}
}

func TestWireFormatMap(t *testing.T) {
	// Map {"foo": "foo"}: count=1 (zigzag 2), key "foo" (06 66 6f 6f),
	// value "foo" (06 66 6f 6f), end 0x00.
	schema := `{"type":"map","values":"string"}`
	data := []byte{0x02, 0x06, 0x66, 0x6F, 0x6F, 0x06, 0x66, 0x6F, 0x6F, 0x00}

	var v map[string]string
	decode(t, schema, data, &v)
	if !reflect.DeepEqual(v, map[string]string{"foo": "foo"}) {
		t.Fatalf("decode map: got %v, want {foo: foo}", v)
	}
}

func TestWireFormatEnum(t *testing.T) {
	// Enum index 1 → zigzag 2 → 0x02
	schema := `{"type":"enum","name":"test","symbols":["foo","bar"]}`
	data := []byte{0x02}

	var v string
	decode(t, schema, data, &v)
	if v != "bar" {
		t.Fatalf("decode enum: got %q, want bar", v)
	}
}

func TestWireFormatFixed(t *testing.T) {
	schema := `{"type":"fixed","name":"test","size":6}`
	data := []byte{'f', 'o', 'o', 'f', 'o', 'o'}

	var v [6]byte
	decode(t, schema, data, &v)
	if v != [6]byte{'f', 'o', 'o', 'f', 'o', 'o'} {
		t.Fatalf("decode fixed: got %v", v)
	}
}

func TestWireFormatUnionNull(t *testing.T) {
	schema := `["null","string"]`

	// Null branch: index 0 → 0x00
	var v *string
	decode(t, schema, []byte{0x00}, &v)
	if v != nil {
		t.Fatalf("decode union null: got %v, want nil", v)
	}

	// String branch: index 1 → 0x02, then "foo"
	decode(t, schema, []byte{0x02, 0x06, 0x66, 0x6F, 0x6F}, &v)
	if v == nil || *v != "foo" {
		t.Fatalf("decode union string: got %v, want foo", v)
	}
}

// -----------------------------------------------------------------------
// Round-trip tests: encode then decode, verify equality.
// -----------------------------------------------------------------------

func TestRoundTripBoolean(t *testing.T) {
	for _, v := range []bool{true, false} {
		got := roundTrip(t, `"boolean"`, v)
		if got != v {
			t.Errorf("boolean round-trip: got %v, want %v", got, v)
		}
	}
}

func TestRoundTripInt(t *testing.T) {
	for _, v := range []int32{0, 1, -1, 127, -128, math.MaxInt32, math.MinInt32} {
		got := roundTrip(t, `"int"`, v)
		if got != v {
			t.Errorf("int round-trip: got %v, want %v", got, v)
		}
	}
}

func TestRoundTripLong(t *testing.T) {
	for _, v := range []int64{0, 1, -1, math.MaxInt64, math.MinInt64} {
		got := roundTrip(t, `"long"`, v)
		if got != v {
			t.Errorf("long round-trip: got %v, want %v", got, v)
		}
	}
}

func TestRoundTripFloat(t *testing.T) {
	for _, v := range []float32{0, 1.5, -1.5, math.MaxFloat32, math.SmallestNonzeroFloat32} {
		got := roundTrip(t, `"float"`, v)
		if got != v {
			t.Errorf("float round-trip: got %v, want %v", got, v)
		}
	}
}

func TestRoundTripDouble(t *testing.T) {
	for _, v := range []float64{0, 1.5, -1.5, math.MaxFloat64, math.SmallestNonzeroFloat64} {
		got := roundTrip(t, `"double"`, v)
		if got != v {
			t.Errorf("double round-trip: got %v, want %v", got, v)
		}
	}
}

func TestRoundTripBytes(t *testing.T) {
	for _, v := range [][]byte{{}, {0}, {1, 2, 3}, make([]byte, 256)} {
		got := roundTrip(t, `"bytes"`, v)
		if !reflect.DeepEqual(got, v) {
			t.Errorf("bytes round-trip: got %v, want %v", got, v)
		}
	}
}

func TestRoundTripString(t *testing.T) {
	for _, v := range []string{"", "hello", "hello world", "日本語"} {
		got := roundTrip(t, `"string"`, v)
		if got != v {
			t.Errorf("string round-trip: got %q, want %q", got, v)
		}
	}
}

func TestRoundTripNull(t *testing.T) {
	s, err := Parse(`"null"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	var p *int
	encoded, err := s.AppendEncode(nil, p)
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	if len(encoded) != 0 {
		t.Fatalf("null should encode to empty bytes, got %v", encoded)
	}
	var p2 *int
	rem, err := s.Decode(encoded, &p2)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("Decode left %d unconsumed bytes", len(rem))
	}
	if p2 != nil {
		t.Fatalf("expected nil pointer after null decode, got %v", p2)
	}
}

func TestRoundTripRecord(t *testing.T) {
	type Simple struct {
		Name string `avro:"name"`
		Age  int32  `avro:"age"`
	}
	schema := `{
		"type": "record",
		"name": "Simple",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "age", "type": "int"}
		]
	}`
	input := Simple{Name: "Alice", Age: 30}
	got := roundTrip(t, schema, input)
	if got != input {
		t.Errorf("record round-trip: got %+v, want %+v", got, input)
	}
}

func TestRoundTripNestedRecord(t *testing.T) {
	type Inner struct {
		X int32 `avro:"x"`
	}
	type Outer struct {
		Inner Inner  `avro:"inner"`
		Label string `avro:"label"`
	}
	schema := `{
		"type": "record",
		"name": "Outer",
		"fields": [
			{"name": "inner", "type": {
				"type": "record",
				"name": "Inner",
				"fields": [{"name": "x", "type": "int"}]
			}},
			{"name": "label", "type": "string"}
		]
	}`
	input := Outer{Inner: Inner{X: 42}, Label: "test"}
	got := roundTrip(t, schema, input)
	if got != input {
		t.Errorf("nested record round-trip: got %+v, want %+v", got, input)
	}
}

func TestRoundTripEmbedded(t *testing.T) {
	type Base struct {
		ID int32 `avro:"id"`
	}
	type Extended struct {
		Base
		Name string `avro:"name"`
	}
	schema := `{
		"type": "record",
		"name": "Extended",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}`
	input := Extended{Base: Base{ID: 7}, Name: "test"}
	got := roundTrip(t, schema, input)
	if got != input {
		t.Errorf("embedded record round-trip: got %+v, want %+v", got, input)
	}
}

func TestRoundTripArray(t *testing.T) {
	type Wrapper struct {
		Values []int32 `avro:"values"`
	}
	schema := `{
		"type": "record",
		"name": "Wrapper",
		"fields": [
			{"name": "values", "type": {"type": "array", "items": "int"}}
		]
	}`

	t.Run("non-empty", func(t *testing.T) {
		input := Wrapper{Values: []int32{1, 2, 3}}
		got := roundTrip(t, schema, input)
		if !reflect.DeepEqual(got, input) {
			t.Errorf("array round-trip: got %+v, want %+v", got, input)
		}
	})

	t.Run("empty", func(t *testing.T) {
		input := Wrapper{Values: []int32{}}
		got := roundTrip(t, schema, input)
		if len(got.Values) != 0 {
			t.Errorf("empty array round-trip: got %+v, want empty", got)
		}
	})
}

func TestRoundTripArrayRecords(t *testing.T) {
	got := roundTrip(t, `
["null",
{
"name": "Superhero",
"type": "record",
"fields": [
	{"name": "id", "type": "int"},
	{"name": "affiliation_id", "type": "int"},
	{"name": "name", "type": "string"},
	{"name": "life", "type": "float"},
	{"name": "energy", "type": "float"},
	{"name": "powers", "type": {
		"type": "array",
		"items": {
			"name": "Superpower",
			"type": "record",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "name", "type": "string"},
				{"name": "damage", "type": "float"},
				{"name": "energy", "type": "float"},
				{"name": "passive", "type": "boolean"}
			]
		}
	}}
]
}]`, Superhero{
		ID:            234765,
		AffiliationID: 9867,
		Name:          "Wolverine",
		Life:          85.25,
		Energy:        32.75,
		Powers: []*Superpower{
			{ID: 2345, Name: "Bone Claws", Damage: 5, Energy: 1.15, Passive: false},
			{ID: 2346, Name: "Regeneration", Damage: -2, Energy: 0.55, Passive: true},
			{ID: 2347, Name: "Adamant skeleton", Damage: -10, Energy: 0, Passive: true},
		},
	})

	if got.ID != 234765 || got.Name != "Wolverine" {
		t.Errorf("superhero mismatch: %+v", got)
	}
	if len(got.Powers) != 3 {
		t.Fatalf("expected 3 powers, got %d", len(got.Powers))
	}
	if got.Powers[0].Name != "Bone Claws" || got.Powers[0].Damage != 5 {
		t.Errorf("power[0] mismatch: %+v", got.Powers[0])
	}
	if got.Powers[1].Name != "Regeneration" || !got.Powers[1].Passive {
		t.Errorf("power[1] mismatch: %+v", got.Powers[1])
	}
}

func TestRoundTripMap(t *testing.T) {
	type Wrapper struct {
		M map[string]int32 `avro:"m"`
	}
	schema := `{
		"type": "record",
		"name": "Wrapper",
		"fields": [
			{"name": "m", "type": {"type": "map", "values": "int"}}
		]
	}`

	t.Run("non-empty", func(t *testing.T) {
		input := Wrapper{M: map[string]int32{"a": 1, "b": 2}}
		got := roundTrip(t, schema, input)
		if !reflect.DeepEqual(got, input) {
			t.Errorf("map round-trip: got %+v, want %+v", got, input)
		}
	})

	t.Run("empty", func(t *testing.T) {
		input := Wrapper{M: map[string]int32{}}
		got := roundTrip(t, schema, input)
		if len(got.M) != 0 {
			t.Errorf("empty map round-trip: got %+v, want empty", got)
		}
	})
}

func TestRoundTripEnum(t *testing.T) {
	type Wrapper struct {
		Color string `avro:"color"`
	}
	schema := `{
		"type": "record",
		"name": "Wrapper",
		"fields": [
			{"name": "color", "type": {
				"type": "enum",
				"name": "Color",
				"symbols": ["RED", "GREEN", "BLUE"]
			}}
		]
	}`
	for _, color := range []string{"RED", "GREEN", "BLUE"} {
		input := Wrapper{Color: color}
		got := roundTrip(t, schema, input)
		if got != input {
			t.Errorf("enum round-trip: got %+v, want %+v", got, input)
		}
	}
}

func TestRoundTripEnumInt(t *testing.T) {
	type Wrapper struct {
		Color int32 `avro:"color"`
	}
	schema := `{
		"type": "record",
		"name": "Wrapper",
		"fields": [
			{"name": "color", "type": {
				"type": "enum",
				"name": "Color",
				"symbols": ["RED", "GREEN", "BLUE"]
			}}
		]
	}`
	input := Wrapper{Color: 1}
	got := roundTrip(t, schema, input)
	if got != input {
		t.Errorf("enum int round-trip: got %+v, want %+v", got, input)
	}
}

func TestRoundTripFixed(t *testing.T) {
	type Wrapper struct {
		Hash [4]byte `avro:"hash"`
	}
	schema := `{
		"type": "record",
		"name": "Wrapper",
		"fields": [
			{"name": "hash", "type": {"type": "fixed", "name": "hash", "size": 4}}
		]
	}`
	input := Wrapper{Hash: [4]byte{0xDE, 0xAD, 0xBE, 0xEF}}
	got := roundTrip(t, schema, input)
	if got != input {
		t.Errorf("fixed round-trip: got %+v, want %+v", got, input)
	}
}

func TestRoundTripUnionNull(t *testing.T) {
	type Wrapper struct {
		Value *int32 `avro:"value"`
	}
	schema := `{
		"type": "record",
		"name": "Wrapper",
		"fields": [
			{"name": "value", "type": ["null", "int"]}
		]
	}`

	t.Run("null", func(t *testing.T) {
		input := Wrapper{Value: nil}
		got := roundTrip(t, schema, input)
		if got.Value != nil {
			t.Errorf("union null round-trip: got %v, want nil", got.Value)
		}
	})

	t.Run("non-null", func(t *testing.T) {
		v := int32(42)
		input := Wrapper{Value: &v}
		got := roundTrip(t, schema, input)
		if got.Value == nil || *got.Value != 42 {
			t.Errorf("union non-null round-trip: got %v, want 42", got.Value)
		}
	})
}

func TestRoundTripRecursive(t *testing.T) {
	type LongList struct {
		Value int64     `avro:"value"`
		Next  *LongList `avro:"next"`
	}
	schema := `{
		"type": "record",
		"name": "LongList",
		"aliases": ["LinkedLongs"],
		"fields" : [
			{"name": "value", "type": "long"},
			{"name": "next", "type": ["null", "LongList"]}
		]
	}`
	input := LongList{
		Value: 1,
		Next: &LongList{
			Value: 2,
			Next: &LongList{
				Value: 3,
				Next:  nil,
			},
		},
	}
	got := roundTrip(t, schema, input)
	if got.Value != 1 {
		t.Errorf("recursive[0]: got %d, want 1", got.Value)
	}
	if got.Next == nil || got.Next.Value != 2 {
		t.Fatalf("recursive[1]: got %v, want 2", got.Next)
	}
	if got.Next.Next == nil || got.Next.Next.Value != 3 {
		t.Fatalf("recursive[2]: got %v, want 3", got.Next.Next)
	}
	if got.Next.Next.Next != nil {
		t.Errorf("recursive[3]: got %v, want nil", got.Next.Next.Next)
	}
}

func TestRoundTripInterface(t *testing.T) {
	type Iface struct {
		S stringer `avro:"s"`
	}
	schema := `{
		"type": "record",
		"name": "iface",
		"fields" : [
			{
				"name": "s", "type": {
					"type": "record",
					"name": "Foobar",
					"fields": [
						{"name": "f", "type": "int"}
					]
				}
			}
		]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	input := Iface{S: &IfaceF{F: 3}}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}

	// Deserialize into a struct where the interface field is pre-set
	// with the concrete pointer type.
	output := Iface{S: &IfaceF{}}
	rem, err := s.Decode(encoded, &output)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("Decode left %d unconsumed bytes", len(rem))
	}
	f := output.S.(*IfaceF)
	if f.F != 3 {
		t.Errorf("interface round-trip: got F=%d, want 3", f.F)
	}
}

func TestRoundTripZeroValues(t *testing.T) {
	type AllTypes struct {
		B  bool    `avro:"b"`
		I  int32   `avro:"i"`
		L  int64   `avro:"l"`
		F  float32 `avro:"f"`
		D  float64 `avro:"d"`
		S  string  `avro:"s"`
		Bs []byte  `avro:"bs"`
	}
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
			{"name": "bs", "type": "bytes"}
		]
	}`
	input := AllTypes{}
	got := roundTrip(t, schema, input)
	if got.B != false || got.I != 0 || got.L != 0 || got.F != 0 || got.D != 0 || got.S != "" {
		t.Errorf("zero values round-trip: got %+v", got)
	}
}

func TestRoundTripUnsignedInt(t *testing.T) {
	type U struct {
		V uint32 `avro:"v"`
	}
	schema := `{
		"type": "record",
		"name": "U",
		"fields": [{"name": "v", "type": "int"}]
	}`
	input := U{V: 12345}
	got := roundTrip(t, schema, input)
	if got != input {
		t.Errorf("unsigned int round-trip: got %+v, want %+v", got, input)
	}
}

// -----------------------------------------------------------------------
// Decode error tests: truncated data, type mismatches, invalid indices.
// -----------------------------------------------------------------------

func TestDecodeShortBuffer(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		data   []byte
	}{
		{"boolean", `"boolean"`, nil},
		{"int truncated", `"int"`, []byte{0xe6}},                              // high bit set, needs more
		{"long truncated", `"long"`, []byte{0xe6}},                            // high bit set, needs more
		{"float", `"float"`, []byte{0x33, 0x33}},                              // need 4 bytes, have 2
		{"double", `"double"`, []byte{0x66, 0x66, 0x66}},                      // need 8 bytes, have 3
		{"string truncated", `"string"`, []byte{0x08}},                        // says 4 bytes, has 0
		{"bytes truncated", `"bytes"`, []byte{0x08, 0xEC}},                    // says 4 bytes, has 1
		{"fixed", `{"type":"fixed","name":"f","size":4}`, []byte{0x01, 0x02}}, // need 4, have 2
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			// Use a generic target; the error should come from reading, not type mismatch.
			var v any
			switch tt.schema {
			case `"boolean"`:
				var b bool
				_, err = s.Decode(tt.data, &b)
			case `"int"`:
				var i int32
				_, err = s.Decode(tt.data, &i)
			case `"long"`:
				var i int64
				_, err = s.Decode(tt.data, &i)
			case `"float"`:
				var f float32
				_, err = s.Decode(tt.data, &f)
			case `"double"`:
				var f float64
				_, err = s.Decode(tt.data, &f)
			case `"string"`:
				var str string
				_, err = s.Decode(tt.data, &str)
			case `"bytes"`:
				var b []byte
				_, err = s.Decode(tt.data, &b)
			default:
				var arr [4]byte
				_, err = s.Decode(tt.data, &arr)
			}
			_ = v
			if err == nil {
				t.Fatal("expected error for short buffer, got nil")
			}
		})
	}
}

func TestDecodeTypeMismatch(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		data   []byte
		target any
	}{
		{"bool into string", `"boolean"`, []byte{0x01}, new("")},
		{"int into bool", `"int"`, []byte{0x36}, new(false)},
		{"string into int", `"string"`, []byte{0x06, 0x66, 0x6f, 0x6f}, new(int32(0))},
		{"int into float", `"int"`, []byte{0x36}, new(float32(0))},
		{"bytes into string", `"bytes"`, []byte{0x04, 0x01, 0x02}, new("")},
		{"fixed into int array", `{"type":"fixed","name":"f","size":6}`, []byte{1, 2, 3, 4, 5, 6}, new([6]int{})},
		{"array into string", `{"type":"array","items":"int"}`, []byte{0x00}, new("")},
		{"map into string", `{"type":"map","values":"int"}`, []byte{0x00}, new("")},
		{"map with int key", `{"type":"map","values":"int"}`, []byte{0x00}, new(map[int]int32{})},
		{"record into int", `{"type":"record","name":"r","fields":[{"name":"a","type":"int"}]}`, []byte{0x02}, new(int32(0))},
		{"float into string", `"float"`, []byte{0, 0, 0, 0}, new("")},
		{"double into string", `"double"`, []byte{0, 0, 0, 0, 0, 0, 0, 0}, new("")},
		{"boolean into int", `"boolean"`, []byte{0x01}, new(int32(0))},
		{"long into bool", `"long"`, []byte{0x02}, new(false)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			_, err = s.Decode(tt.data, tt.target)
			if err == nil {
				t.Fatal("expected type mismatch error, got nil")
			}
		})
	}
}

func TestDecodeInvalidUnionIndex(t *testing.T) {
	// Union ["null", "string"] only has indices 0 and 1; index 2 is out of range.
	decodeErr(t, `["null","string"]`, []byte{0x04}, new((*string)(nil))) // zigzag 4 → 2
}

func TestDecodeInvalidEnumIndex(t *testing.T) {
	// Enum with 2 symbols; index 2 is out of range.
	schema := `{"type":"enum","name":"e","symbols":["a","b"]}`
	decodeErr(t, schema, []byte{0x04}, new("")) // zigzag 4 → 2
}

func TestDecodeNonPointer(t *testing.T) {
	s, err := Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Decode([]byte{0x02}, 42) // not a pointer
	if err == nil {
		t.Fatal("expected error for non-pointer, got nil")
	}
}

// -----------------------------------------------------------------------
// Edge case tests ported from hamba/avro patterns.
// -----------------------------------------------------------------------

func TestDecodeRecordNilPointer(t *testing.T) {
	// Decode into **Record where the inner pointer is nil → allocate through it.
	type R struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}
	schema := `{"type":"record","name":"r","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}`
	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f} // {a:27, b:"foo"}

	var got *R
	decode(t, schema, data, &got)
	if got == nil {
		t.Fatal("expected non-nil pointer")
	}
	if got.A != 27 || got.B != "foo" {
		t.Errorf("got %+v, want {A:27 B:foo}", got)
	}
}

func TestDecodeEmbeddedPointerStruct(t *testing.T) {
	type Embed struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}
	type Outer struct {
		*Embed
		C string `avro:"c"`
	}
	schema := `{"type":"record","name":"r","fields":[
		{"name":"a","type":"long"},
		{"name":"b","type":"string"},
		{"name":"c","type":"string"}
	]}`
	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f, 0x06, 0x62, 0x61, 0x72} // a=27, b="foo", c="bar"

	// Embed pointer is nil — decoder should allocate it.
	var got Outer
	decode(t, schema, data, &got)
	if got.Embed == nil {
		t.Fatal("expected Embed to be allocated")
	}
	if got.A != 27 || got.B != "foo" || got.C != "bar" {
		t.Errorf("got %+v, want {A:27 B:foo C:bar}", got)
	}
}

func TestDecodeEmbeddedPointerStructPreset(t *testing.T) {
	// Same as above but C is pre-set to a non-zero value.
	// Ensures the embedded pointer allocation doesn't interfere.
	type Embed struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}
	type Outer struct {
		*Embed
		C string `avro:"c"`
	}
	schema := `{"type":"record","name":"r","fields":[
		{"name":"a","type":"long"},
		{"name":"b","type":"string"},
		{"name":"c","type":"string"}
	]}`
	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f, 0x06, 0x62, 0x61, 0x72}

	got := Outer{C: "nonzero"}
	decode(t, schema, data, &got)
	if got.Embed == nil {
		t.Fatal("expected Embed to be allocated")
	}
	if got.A != 27 || got.B != "foo" || got.C != "bar" {
		t.Errorf("got %+v, want {A:27 B:foo C:bar}", got)
	}
}

func TestDecodeTypeReference(t *testing.T) {
	// Schema defines a named record, then references it by name.
	type R struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}
	type Parent struct {
		X R `avro:"x"`
		Y R `avro:"y"`
	}
	schema := `{
		"type":"record","name":"parent","fields":[
			{"name":"x","type":{"type":"record","name":"child","fields":[
				{"name":"a","type":"long"},{"name":"b","type":"string"}
			]}},
			{"name":"y","type":"child"}
		]
	}`
	input := Parent{X: R{A: 1, B: "one"}, Y: R{A: 2, B: "two"}}
	got := roundTrip(t, schema, input)
	if got != input {
		t.Errorf("type reference round-trip: got %+v, want %+v", got, input)
	}
}

func TestDecodeRecursiveArray(t *testing.T) {
	type Rec struct {
		A int32 `avro:"a"`
		B []Rec `avro:"b"`
	}
	schema := `{
		"type":"record","name":"test","fields":[
			{"name":"a","type":"int"},
			{"name":"b","type":{"type":"array","items":"test"}}
		]
	}`
	input := Rec{
		A: 1,
		B: []Rec{
			{A: 2, B: []Rec{}},
			{A: 3, B: []Rec{}},
		},
	}
	got := roundTrip(t, schema, input)
	if got.A != 1 || len(got.B) != 2 || got.B[0].A != 2 || got.B[1].A != 3 {
		t.Errorf("recursive array: got %+v, want %+v", got, input)
	}
}

func TestDecodeRecursiveMap(t *testing.T) {
	type Rec struct {
		A int32          `avro:"a"`
		B map[string]Rec `avro:"b"`
	}
	schema := `{
		"type":"record","name":"test","fields":[
			{"name":"a","type":"int"},
			{"name":"b","type":{"type":"map","values":"test"}}
		]
	}`
	input := Rec{
		A: 1,
		B: map[string]Rec{
			"x": {A: 2, B: map[string]Rec{}},
		},
	}
	got := roundTrip(t, schema, input)
	if got.A != 1 {
		t.Fatalf("recursive map root: got %d, want 1", got.A)
	}
	child, ok := got.B["x"]
	if !ok || child.A != 2 {
		t.Errorf("recursive map child: got %+v", got.B)
	}
}

func TestDecodeUnionRecursive(t *testing.T) {
	type Rec struct {
		A int32 `avro:"a"`
		B *Rec  `avro:"b"`
	}
	schema := `{
		"type":"record","name":"test","fields":[
			{"name":"a","type":"int"},
			{"name":"b","type":["null","test"]}
		]
	}`
	input := Rec{A: 1, B: &Rec{A: 2}}
	got := roundTrip(t, schema, input)
	if got.A != 1 || got.B == nil || got.B.A != 2 || got.B.B != nil {
		t.Errorf("union recursive: got %+v", got)
	}
}

func TestDecodeMapOfStruct(t *testing.T) {
	type R struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}
	schema := `{"type":"map","values":{"type":"record","name":"r","fields":[
		{"name":"a","type":"long"},{"name":"b","type":"string"}
	]}}`
	input := map[string]R{"k": {A: 27, B: "foo"}}
	got := roundTrip(t, schema, input)
	if !reflect.DeepEqual(got, input) {
		t.Errorf("map of struct: got %+v, want %+v", got, input)
	}
}

func TestDecodeArrayOfStruct(t *testing.T) {
	type R struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}
	schema := `{"type":"array","items":{"type":"record","name":"r","fields":[
		{"name":"a","type":"long"},{"name":"b","type":"string"}
	]}}`
	input := []R{{A: 27, B: "foo"}, {A: 28, B: "bar"}}
	got := roundTrip(t, schema, input)
	if !reflect.DeepEqual(got, input) {
		t.Errorf("array of struct: got %+v, want %+v", got, input)
	}
}

func TestDecodeUnionNullableBytes(t *testing.T) {
	schema := `["null","bytes"]`

	t.Run("non-null", func(t *testing.T) {
		// index=1 (0x02), then bytes "foo" (0x06, 0x66, 0x6f, 0x6f)
		data := []byte{0x02, 0x06, 0x66, 0x6F, 0x6F}
		var got []byte
		decode(t, schema, data, &got)
		if !bytes.Equal(got, []byte("foo")) {
			t.Errorf("got %v, want foo", got)
		}
	})

	t.Run("null", func(t *testing.T) {
		data := []byte{0x00}
		got := []byte("preallocated")
		decode(t, schema, data, &got)
		if got != nil {
			t.Errorf("got %v, want nil", got)
		}
	})

	t.Run("non-null-empty", func(t *testing.T) {
		// index=1 (0x02), then bytes length 0 (0x00)
		data := []byte{0x02, 0x00}
		var got []byte
		decode(t, schema, data, &got)
		if got == nil {
			t.Fatal("expected non-nil empty slice")
		}
		if len(got) != 0 {
			t.Errorf("expected empty slice, got %v", got)
		}
	})
}

func TestDecodeUnionPtrReuse(t *testing.T) {
	// When decoding a union into a pre-existing pointer, reuse the allocation.
	type R struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}
	schema := `["null",{"type":"record","name":"r","fields":[
		{"name":"a","type":"long"},{"name":"b","type":"string"}
	]}]`
	data := []byte{0x02, 0x36, 0x06, 0x66, 0x6F, 0x6F} // index=1, a=27, b="foo"

	original := new(R{})
	got := original
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Decode(data, &got)
	if err != nil {
		t.Fatal(err)
	}
	if got != original {
		t.Error("expected pointer reuse, got new allocation")
	}
	if got.A != 27 || got.B != "foo" {
		t.Errorf("got %+v, want {A:27 B:foo}", got)
	}
}

func TestDecodeRecordIntoMap(t *testing.T) {
	// Decode a record schema into map[string]any.
	schema := `{"type":"record","name":"r","fields":[
		{"name":"a","type":"long"},
		{"name":"b","type":"string"}
	]}`
	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f} // a=27, b="foo"

	var got map[string]any
	decode(t, schema, data, &got)
	if got["a"] != int64(27) {
		t.Errorf("a: got %v (%T), want 27", got["a"], got["a"])
	}
	if got["b"] != "foo" {
		t.Errorf("b: got %v, want foo", got["b"])
	}
}

func TestDecodeRecordMapInvalidKey(t *testing.T) {
	schema := `{"type":"record","name":"r","fields":[
		{"name":"a","type":"long"},
		{"name":"b","type":"string"}
	]}`
	decodeErr(t, schema, []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}, new(map[int]any{}))
}

func TestDecodeRecordMapInvalidElem(t *testing.T) {
	// map[string]string cannot hold a long value.
	schema := `{"type":"record","name":"r","fields":[
		{"name":"a","type":"long"},
		{"name":"b","type":"string"}
	]}`
	decodeErr(t, schema, []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}, new(map[string]string{}))
}

func TestDecodeRecordMapInvalidData(t *testing.T) {
	schema := `{"type":"record","name":"r","fields":[
		{"name":"a","type":"long"},
		{"name":"b","type":"string"}
	]}`
	// Corrupt varint for field "a".
	decodeErr(t, schema, []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}, new(map[string]any{}))
}

func TestDecodeRecordIntoMapWithUnion(t *testing.T) {
	// Record with union and null fields decoded into map[string]any.
	schema := `{"type":"record","name":"r","fields":[
		{"name":"a","type":"long"},
		{"name":"b","type":"string"},
		{"name":"c","type":["null","string"]},
		{"name":"d","type":"null"}
	]}`
	// a=27 (0x36), b="foo" (06 66 6f 6f), c=union index 1 string "foo" (02 06 66 6f 6f), d=null
	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f, 0x02, 0x06, 0x66, 0x6f, 0x6f}

	var got map[string]any
	decode(t, schema, data, &got)
	if got["a"] != int64(27) {
		t.Errorf("a: got %v (%T), want 27", got["a"], got["a"])
	}
	if got["b"] != "foo" {
		t.Errorf("b: got %v, want foo", got["b"])
	}
	if got["c"] != "foo" {
		t.Errorf("c: got %v, want foo", got["c"])
	}
	if got["d"] != nil {
		t.Errorf("d: got %v, want nil", got["d"])
	}
}

func TestDecodeArrayItemError(t *testing.T) {
	// Array block says 2 items, but data for second item is corrupt.
	schema := `{"type":"array","items":"string"}`
	// count=2 (zigzag 4), item1="foo" (06 66 6f 6f), item2=corrupt length
	data := []byte{0x04, 0x06, 0x66, 0x6f, 0x6f, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}
	decodeErr(t, schema, data, new([]string{}))
}

func TestDecodeArrayBlockError(t *testing.T) {
	// Array with corrupt block count.
	schema := `{"type":"array","items":"int"}`
	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}
	decodeErr(t, schema, data, new([]int32{}))
}

func TestDecodeMapValueShortRead(t *testing.T) {
	// Map with truncated value data.
	schema := `{"type":"map","values":"string"}`
	// count=1 (0x02), key="foo" (06 66 6f 6f), value=corrupt (06 06)
	data := []byte{0x02, 0x06, 0x66, 0x6F, 0x6F, 0x06, 0x06}
	decodeErr(t, schema, data, new(map[string]string{}))
}

func TestDecodeMapBlockError(t *testing.T) {
	// Map with corrupt block count.
	schema := `{"type":"map","values":"string"}`
	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}
	decodeErr(t, schema, data, new(map[string]string{}))
}

func TestDecodeEnumCorruptVarint(t *testing.T) {
	schema := `{"type":"enum","name":"e","symbols":["a","b","c"]}`
	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD}
	decodeErr(t, schema, data, new(""))
}

func TestDecodeRecordStructInvalidData(t *testing.T) {
	type R struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}
	schema := `{"type":"record","name":"r","fields":[
		{"name":"a","type":"long"},
		{"name":"b","type":"string"}
	]}`
	// Corrupt varint.
	decodeErr(t, schema, []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}, new(R{}))
}

func TestDecodeUnionIntoInterface(t *testing.T) {
	// Decode union ["null","int"] into any.
	schema := `["null","int"]`

	// index=1, int 27 (0x36)
	var got any
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Decode([]byte{0x02, 0x36}, &got)
	if err != nil {
		t.Fatal(err)
	}
	if got != int32(27) {
		t.Errorf("got %v (%T), want int32(27)", got, got)
	}

	// index=0 (null)
	got = "preallocated"
	_, err = s.Decode([]byte{0x00}, &got)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

func TestDecodeNegativeStringLength(t *testing.T) {
	// Negative length for string should error.
	decodeErr(t, `"string"`, []byte{0x01}, new("")) // zigzag 1 → -1
}

func TestDecodeNegativeBytesLength(t *testing.T) {
	// Negative length for bytes should error.
	decodeErr(t, `"bytes"`, []byte{0x01}, new([]byte{})) // zigzag 1 → -1
}

func TestDecodeFloatIntoInterface(t *testing.T) {
	s, err := Parse(`"float"`)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := s.AppendEncode(nil, new(float32(3.14)))
	if err != nil {
		t.Fatal(err)
	}
	var got any
	_, err = s.Decode(encoded, &got)
	if err != nil {
		t.Fatal(err)
	}
	f, ok := got.(float32)
	if !ok {
		t.Fatalf("expected float32, got %T", got)
	}
	if f < 3.13 || f > 3.15 {
		t.Errorf("got %v, want ~3.14", f)
	}
}

func TestDecodeDoubleIntoInterface(t *testing.T) {
	s, err := Parse(`"double"`)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := s.AppendEncode(nil, new(float64(2.718)))
	if err != nil {
		t.Fatal(err)
	}
	var got any
	_, err = s.Decode(encoded, &got)
	if err != nil {
		t.Fatal(err)
	}
	f, ok := got.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", got)
	}
	if f < 2.717 || f > 2.719 {
		t.Errorf("got %v, want ~2.718", f)
	}
}

func TestDecodeBytesIntoInterface(t *testing.T) {
	s, err := Parse(`"bytes"`)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := s.AppendEncode(nil, new([]byte{1, 2, 3}))
	if err != nil {
		t.Fatal(err)
	}
	var got any
	_, err = s.Decode(encoded, &got)
	if err != nil {
		t.Fatal(err)
	}
	b, ok := got.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", got)
	}
	if !bytes.Equal(b, []byte{1, 2, 3}) {
		t.Errorf("got %v", b)
	}
}

func TestDecodeBytesIntoArray(t *testing.T) {
	s, err := Parse(`"bytes"`)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := s.AppendEncode(nil, new([]byte{1, 2, 3}))
	if err != nil {
		t.Fatal(err)
	}
	var got [3]byte
	_, err = s.Decode(encoded, &got)
	if err != nil {
		t.Fatal(err)
	}
	if got != [3]byte{1, 2, 3} {
		t.Errorf("got %v", got)
	}
}

func TestDecodeBytesArrayWrongLength(t *testing.T) {
	s, err := Parse(`"bytes"`)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := s.AppendEncode(nil, new([]byte{1, 2, 3}))
	if err != nil {
		t.Fatal(err)
	}
	var got [5]byte
	_, err = s.Decode(encoded, &got)
	if err == nil {
		t.Fatal("expected error for wrong array length")
	}
}

func TestDecodeBytesNonUint8Slice(t *testing.T) {
	decodeErr(t, `"bytes"`, []byte{0x04, 0x01, 0x02}, new([]int32{}))
}

func TestDecodeBytesNonUint8Array(t *testing.T) {
	decodeErr(t, `"bytes"`, []byte{0x04, 0x01, 0x02}, new([2]int32{}))
}

func TestDecodeEnumIntoUint(t *testing.T) {
	schema := `{"type":"enum","name":"e","symbols":["a","b","c"]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	// Encode "b" (index 1).
	encoded, err := s.AppendEncode(nil, new("b"))
	if err != nil {
		t.Fatal(err)
	}
	var got uint32
	_, err = s.Decode(encoded, &got)
	if err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Errorf("got %d, want 1", got)
	}
}

func TestDecodeEnumTypeMismatch(t *testing.T) {
	schema := `{"type":"enum","name":"e","symbols":["a","b"]}`
	decodeErr(t, schema, []byte{0x00}, new(false))
}

func TestDecodeFixedIntoInterface(t *testing.T) {
	s, err := Parse(`{"type":"fixed","name":"f","size":4}`)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte{1, 2, 3, 4}
	var got any
	_, err = s.Decode(data, &got)
	if err != nil {
		t.Fatal(err)
	}
	b, ok := got.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", got)
	}
	if !bytes.Equal(b, data) {
		t.Errorf("got %v", b)
	}
}

func TestDecodeFixedTypeMismatch(t *testing.T) {
	// Fixed into a non-array.
	decodeErr(t, `{"type":"fixed","name":"f","size":4}`, []byte{1, 2, 3, 4}, new(int32(0)))
}

func TestDecodeFixedWrongSize(t *testing.T) {
	// Fixed of size 4 into a [3]byte array.
	decodeErr(t, `{"type":"fixed","name":"f","size":4}`, []byte{1, 2, 3, 4}, new([3]byte{}))
}

func TestDecodeMapIntoInterface(t *testing.T) {
	schema := `{"type":"map","values":"int"}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	input := map[string]int32{"a": 1, "b": 2}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatal(err)
	}
	var got any
	_, err = s.Decode(encoded, &got)
	if err != nil {
		t.Fatal(err)
	}
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", got)
	}
	if m["a"] != int32(1) || m["b"] != int32(2) {
		t.Errorf("got %v", m)
	}
}

func TestDecodeArrayIntoInterface(t *testing.T) {
	schema := `{"type":"array","items":"string"}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	input := []string{"hello", "world"}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatal(err)
	}
	var got any
	_, err = s.Decode(encoded, &got)
	if err != nil {
		t.Fatal(err)
	}
	arr, ok := got.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", got)
	}
	if len(arr) != 2 || arr[0] != "hello" || arr[1] != "world" {
		t.Errorf("got %v", arr)
	}
}

func TestDecodeEnumIntoInterface(t *testing.T) {
	schema := `{"type":"enum","name":"e","symbols":["a","b","c"]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := s.AppendEncode(nil, new("b"))
	if err != nil {
		t.Fatal(err)
	}
	var got any
	_, err = s.Decode(encoded, &got)
	if err != nil {
		t.Fatal(err)
	}
	if got != "b" {
		t.Errorf("got %v, want b", got)
	}
}

func TestDecodeRecordMissingField(t *testing.T) {
	schema := `{"type":"record","name":"r","fields":[
		{"name":"a","type":"int"},
		{"name":"missing_field","type":"string"}
	]}`
	type R struct {
		A int32 `avro:"a"`
	}
	decodeErr(t, schema, []byte{0x02, 0x02, 0x78}, new(R{}))
}

func TestDecodeRecordIntoTypedMap(t *testing.T) {
	schema := `{"type":"record","name":"r","fields":[
		{"name":"a","type":"string"},
		{"name":"b","type":"string"}
	]}`
	data := []byte{0x06, 0x66, 0x6f, 0x6f, 0x06, 0x62, 0x61, 0x72}
	var got map[string]string
	decode(t, schema, data, &got)
	if got["a"] != "foo" || got["b"] != "bar" {
		t.Errorf("got %v", got)
	}
}

func TestDecodeUnionShortBuffer(t *testing.T) {
	decodeErr(t, `["null","int"]`, []byte{0xE6, 0xA2}, new((*int32)(nil)))
}

func TestDecodeMapNegativeBlockCount(t *testing.T) {
	// Negative block count means the absolute value is the count,
	// followed by a block size long, then entries.
	schema := `{"type":"map","values":"int"}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	// Manually build: count=-1 (zigzag 1), block_size=3 (zigzag 6), key="a" (02 61), value=1 (02), terminator 0
	data := []byte{0x01, 0x06, 0x02, 0x61, 0x02, 0x00}
	var got map[string]int32
	_, err = s.Decode(data, &got)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got["a"] != 1 {
		t.Errorf("got %v", got)
	}
}

func TestDecodeArrayNegativeBlockCount(t *testing.T) {
	schema := `{"type":"array","items":"int"}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	// count=-2 (zigzag 3), block_size=2 (zigzag 4), items: 1 (02), 2 (04), terminator 0
	data := []byte{0x03, 0x04, 0x02, 0x04, 0x00}
	var got []int32
	_, err = s.Decode(data, &got)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Errorf("got %v", got)
	}
}

func TestDecodeMapKeyInvalid(t *testing.T) {
	schema := `{"type":"map","values":"int"}`
	// count=1 (0x02), key length = -1 (zigzag 0x01)
	decodeErr(t, schema, []byte{0x02, 0x01}, new(map[string]int32{}))
}

func TestTypeFieldMappingAvroSkip(t *testing.T) {
	// Fields tagged avro:"-" are skipped, both for regular and embedded fields.
	type Embed struct {
		A int32 `avro:"a"`
	}
	type R struct {
		Embed
		B       string `avro:"b"`
		Ignored int    `avro:"-"`
	}
	schema := `{"type":"record","name":"r","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}
	]}`
	input := R{Embed: Embed{A: 42}, B: "hello"}
	got := roundTrip(t, schema, input)
	if got.A != 42 || got.B != "hello" {
		t.Errorf("got %+v", got)
	}
}

func TestTypeFieldMappingEmbedWithTag(t *testing.T) {
	// Embedded struct with explicit avro tag is treated as a named field.
	type Inner struct {
		X int32 `avro:"x"`
	}
	type Outer struct {
		Inner `avro:"inner"`
	}
	schema := `{"type":"record","name":"outer","fields":[
		{"name":"inner","type":{"type":"record","name":"inner","fields":[
			{"name":"x","type":"int"}
		]}}
	]}`
	input := Outer{Inner: Inner{X: 99}}
	got := roundTrip(t, schema, input)
	if got.Inner.X != 99 {
		t.Errorf("got %+v", got)
	}
}

func TestTypeFieldMappingSkipEmbeddedDash(t *testing.T) {
	// Embedded struct tagged avro:"-" should not be inlined.
	type Skip struct {
		Hidden int32 `avro:"hidden"`
	}
	type R struct {
		Skip `avro:"-"`
		A    int32 `avro:"a"`
	}
	schema := `{"type":"record","name":"r","fields":[{"name":"a","type":"int"}]}`
	input := R{A: 7}
	got := roundTrip(t, schema, input)
	if got.A != 7 {
		t.Errorf("got %+v", got)
	}
}

func TestTypeFieldMappingUnexportedNonStruct(t *testing.T) {
	// Unexported non-struct field should be skipped.
	type R struct {
		a int32 //nolint:unused
		B int32 `avro:"b"`
	}
	schema := `{"type":"record","name":"r","fields":[{"name":"b","type":"int"}]}`
	input := R{B: 5}
	got := roundTrip(t, schema, input)
	if got.B != 5 {
		t.Errorf("got %+v", got)
	}
}

func TestTypeFieldMappingTaggedBeatsUntagged(t *testing.T) {
	// A deeper tagged field should beat a shallower untagged field with same name.
	type Embed struct {
		Name string `avro:"Name"` // tagged, resolves to "Name"
	}
	type R struct {
		Name string // untagged, Go field name "Name"
		Embed
	}
	schema := `{"type":"record","name":"r","fields":[{"name":"Name","type":"string"}]}`
	input := R{Name: "outer", Embed: Embed{Name: "inner"}}
	got := roundTrip(t, schema, input)
	// The tagged "Name" from Embed should win over untagged Name.
	if got.Embed.Name != "inner" {
		t.Errorf("expected inner, got %+v", got)
	}
}

func TestTypeFieldMappingDuplicateFirstWins(t *testing.T) {
	// Two untagged fields at different depths with same name: shallower wins.
	type Deep struct {
		Foo string
	}
	type R struct {
		Foo string
		Deep
	}
	schema := `{"type":"record","name":"r","fields":[{"name":"Foo","type":"string"}]}`
	input := R{Foo: "shallow", Deep: Deep{Foo: "deep"}}
	got := roundTrip(t, schema, input)
	if got.Foo != "shallow" {
		t.Errorf("expected shallow, got %+v", got)
	}
}

func TestTypeFieldMappingRecursiveEmbed(t *testing.T) {
	// Recursive embedded struct should not cause infinite recursion.
	type Recursive struct {
		*Recursive
		X int32 `avro:"x"`
	}
	schema := `{"type":"record","name":"r","fields":[{"name":"x","type":"int"}]}`
	input := Recursive{X: 42}
	got := roundTrip(t, schema, input)
	if got.X != 42 {
		t.Errorf("got %d, want 42", got.X)
	}
}

func TestTypeFieldMappingUnexportedAnonymousNonStruct(t *testing.T) {
	// Unexported anonymous non-struct field should be skipped.
	type myString string
	type R struct {
		myString       //nolint:unused
		B        int32 `avro:"b"`
	}
	schema := `{"type":"record","name":"r","fields":[{"name":"b","type":"int"}]}`
	input := R{B: 7}
	got := roundTrip(t, schema, input)
	if got.B != 7 {
		t.Errorf("got %+v", got)
	}
}

func TestTypeFieldMappingFieldNameFallback(t *testing.T) {
	// Field without avro tag uses the Go field name.
	type R struct {
		SomeField int32
	}
	schema := `{"type":"record","name":"r","fields":[{"name":"SomeField","type":"int"}]}`
	input := R{SomeField: 42}
	got := roundTrip(t, schema, input)
	if got.SomeField != 42 {
		t.Errorf("got %+v", got)
	}
}

func TestDecodeBooleanIntoInterface(t *testing.T) {
	s, err := Parse(`"boolean"`)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := s.AppendEncode(nil, new(true))
	if err != nil {
		t.Fatal(err)
	}
	var got any
	_, err = s.Decode(encoded, &got)
	if err != nil {
		t.Fatal(err)
	}
	if got != true {
		t.Errorf("got %v, want true", got)
	}
}

func TestDecodeLongIntoUint(t *testing.T) {
	s, err := Parse(`"long"`)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := s.AppendEncode(nil, new(int64(42)))
	if err != nil {
		t.Fatal(err)
	}
	var got uint64
	_, err = s.Decode(encoded, &got)
	if err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Errorf("got %d, want 42", got)
	}
}

func TestDecodeBytesShortRead(t *testing.T) {
	// Corrupt varlong for bytes length.
	decodeErr(t, `"bytes"`, []byte{0xE6, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}, new([]byte{}))
}

func TestDecodeRecordIntoInterfaceWithError(t *testing.T) {
	// Record decoded into any, but with corrupt field data.
	schema := `{"type":"record","name":"r","fields":[
		{"name":"a","type":"long"},
		{"name":"b","type":"string"}
	]}`
	// Corrupt varlong for field "a".
	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}
	var got any
	decodeErr(t, schema, data, &got)
}

func TestDecodeRecordIntoInterfaceSuccess(t *testing.T) {
	// Record decoded into any should produce map[string]any.
	schema := `{"type":"record","name":"r","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}
	]}`
	data := []byte{0x04, 0x06, 0x66, 0x6f, 0x6f} // a=2, b="foo"
	var got any
	decode(t, schema, data, &got)
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", got)
	}
	if m["a"] != int32(2) {
		t.Errorf("a: got %v (%T)", m["a"], m["a"])
	}
	if m["b"] != "foo" {
		t.Errorf("b: got %v", m["b"])
	}
}

func TestDecodeArrayNegativeBlockShortRead(t *testing.T) {
	// Negative block count followed by truncated block size.
	schema := `{"type":"array","items":"int"}`
	data := []byte{0x01, 0xE6} // count=-1 (zigzag 0x01), then truncated block size
	decodeErr(t, schema, data, new([]int32{}))
}

func TestDecodeArrayCapGrowth(t *testing.T) {
	// Test that the array can grow from pre-allocated to larger.
	schema := `{"type":"array","items":"int"}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	input := []int32{1, 2, 3, 4, 5}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatal(err)
	}
	// Decode into pre-allocated slice with smaller cap.
	got := make([]int32, 0, 2)
	_, err = s.Decode(encoded, &got)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Errorf("got len %d, want 5", len(got))
	}
}

func TestDecodeArrayExistingCap(t *testing.T) {
	// Decode into slice with sufficient capacity (SetLen path).
	schema := `{"type":"array","items":"int"}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	input := []int32{1, 2, 3}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]int32, 0, 10) // plenty of cap
	_, err = s.Decode(encoded, &got)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Errorf("got %v", got)
	}
}

func TestDecodeMapNegativeBlockShortRead(t *testing.T) {
	// Negative block count with truncated block size.
	schema := `{"type":"map","values":"int"}`
	data := []byte{0x01, 0xE6} // count=-1 (zigzag 0x01), then truncated
	decodeErr(t, schema, data, new(map[string]int32{}))
}

func TestDecodeMapKeyLengthShortRead(t *testing.T) {
	// Map key length read fails.
	schema := `{"type":"map","values":"int"}`
	// count=1 (0x02), then truncated key length
	data := []byte{0x02, 0xE6, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}
	decodeErr(t, schema, data, new(map[string]int32{}))
}

func TestVarint4Byte(t *testing.T) {
	// Values in the range [1<<21, 1<<28) encode as 4-byte uvarints.
	// int32(1<<21) = 2097152, zigzag = 4194304 = 0x400000
	s, err := Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	v := int32(1 << 21)
	encoded, err := s.AppendEncode(nil, &v)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(encoded) != 4 {
		t.Fatalf("expected 4-byte varint, got %d bytes: %x", len(encoded), encoded)
	}
	var got int32
	_, err = s.Decode(encoded, &got)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != v {
		t.Errorf("got %d, want %d", got, v)
	}
}

func TestRoundTripIntWidths(t *testing.T) {
	// Verify all signed/unsigned integer widths that fit in avro int.
	schema := `"int"`

	for _, tc := range []struct {
		name string
		v    any
		want int64
	}{
		{"int8", new(int8(27)), 27},
		{"int16", new(int16(27)), 27},
		{"int32", new(int32(27)), 27},
		{"uint8", new(uint8(27)), 27},
		{"uint16", new(uint16(27)), 27},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := Parse(schema)
			if err != nil {
				t.Fatal(err)
			}
			dst, err := s.AppendEncode(nil, tc.v)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			// All should produce the same wire bytes as int32(27).
			if !bytes.Equal(dst, []byte{0x36}) {
				t.Fatalf("encode: got %x, want 36", dst)
			}
		})
	}
}

func TestRoundTripLongWidths(t *testing.T) {
	schema := `"long"`
	for _, tc := range []struct {
		name string
		v    any
	}{
		{"int32", new(int32(27))},
		{"int64", new(int64(27))},
		{"uint32", new(uint32(27))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := Parse(schema)
			if err != nil {
				t.Fatal(err)
			}
			dst, err := s.AppendEncode(nil, tc.v)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if !bytes.Equal(dst, []byte{0x36}) {
				t.Fatalf("encode: got %x, want 36", dst)
			}
		})
	}
}

// -----------------------------------------------------------------------
// Benchmarks
// -----------------------------------------------------------------------

func BenchmarkDeserialize(b *testing.B) {
	superhero := Superhero{
		ID:            234765,
		AffiliationID: 9867,
		Name:          "Wolverine",
		Life:          85.25,
		Energy:        32.75,
		Powers: []*Superpower{
			{ID: 2345, Name: "Bone Claws", Damage: 5, Energy: 1.15, Passive: false},
			{ID: 2346, Name: "Regeneration", Damage: -2, Energy: 0.55, Passive: true},
			{ID: 2347, Name: "Adamant skeleton", Damage: -10, Energy: 0, Passive: true},
		},
	}

	s, err := Parse(`["null", {
"name": "Superhero",
"type": "record",
"fields": [
	{"name": "id", "type": "int"},
	{"name": "affiliation_id", "type": "int"},
	{"name": "name", "type": "string"},
	{"name": "life", "type": "float"},
	{"name": "energy", "type": "float"},
	{"name": "powers", "type": {
		"type": "array",
		"items": {
			"name": "Superpower",
			"type": "record",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "name", "type": "string"},
				{"name": "damage", "type": "float"},
				{"name": "energy", "type": "float"},
				{"name": "passive", "type": "boolean"}
			]
		}
	}}
]
}]`)
	if err != nil {
		b.Fatalf("unable to prime: %v", err)
	}

	encoded, err := s.AppendEncode(nil, &superhero)
	if err != nil {
		b.Fatalf("unable to encode: %v", err)
	}

	b.Run("cold", func(b *testing.B) {
		var out Superhero
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			out = Superhero{}
			if _, err = s.Decode(encoded, &out); err != nil {
				b.Fatalf("unable to decode: %v", err)
			}
		}
	})

	b.Run("reuse", func(b *testing.B) {
		var out Superhero
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err = s.Decode(encoded, &out); err != nil {
				b.Fatalf("unable to decode: %v", err)
			}
		}
	})
}

func BenchmarkDeserializeRecursive(b *testing.B) {
	type LongList struct {
		Value int64     `avro:"value"`
		Next  *LongList `avro:"next"`
	}
	llist := LongList{
		Value: 1,
		Next: &LongList{
			Value: 2,
			Next: &LongList{
				Value: 3,
				Next:  nil,
			},
		},
	}

	s, err := Parse(`{
		"type": "record",
		"name": "LongList",
		"aliases": ["LinkedLongs"],
		"fields" : [
			{"name": "value", "type": "long"},
			{"name": "next", "type": ["null", "LongList"]}
		]
	}`)
	if err != nil {
		b.Fatalf("unable to prime: %v", err)
	}

	encoded, err := s.AppendEncode(nil, &llist)
	if err != nil {
		b.Fatalf("unable to encode: %v", err)
	}

	var out LongList
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = LongList{}
		if _, err = s.Decode(encoded, &out); err != nil {
			b.Fatalf("unable to decode: %v", err)
		}
	}
}

func BenchmarkDeserializePrimitives(b *testing.B) {
	type Prims struct {
		B  bool    `avro:"b"`
		I  int32   `avro:"i"`
		L  int64   `avro:"l"`
		F  float32 `avro:"f"`
		D  float64 `avro:"d"`
		S  string  `avro:"s"`
		Bs []byte  `avro:"bs"`
	}
	s, err := Parse(`{
		"type":"record","name":"prims","fields":[
			{"name":"b","type":"boolean"},
			{"name":"i","type":"int"},
			{"name":"l","type":"long"},
			{"name":"f","type":"float"},
			{"name":"d","type":"double"},
			{"name":"s","type":"string"},
			{"name":"bs","type":"bytes"}
		]
	}`)
	if err != nil {
		b.Fatal(err)
	}

	input := Prims{B: true, I: 42, L: 123456789, F: 3.14, D: 2.718281828, S: "hello world", Bs: []byte{1, 2, 3, 4, 5}}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		b.Fatal(err)
	}

	var out Prims
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = Prims{}
		if _, err = s.Decode(encoded, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSerializePrimitives(b *testing.B) {
	type Prims struct {
		B  bool    `avro:"b"`
		I  int32   `avro:"i"`
		L  int64   `avro:"l"`
		F  float32 `avro:"f"`
		D  float64 `avro:"d"`
		S  string  `avro:"s"`
		Bs []byte  `avro:"bs"`
	}
	s, err := Parse(`{
		"type":"record","name":"prims","fields":[
			{"name":"b","type":"boolean"},
			{"name":"i","type":"int"},
			{"name":"l","type":"long"},
			{"name":"f","type":"float"},
			{"name":"d","type":"double"},
			{"name":"s","type":"string"},
			{"name":"bs","type":"bytes"}
		]
	}`)
	if err != nil {
		b.Fatal(err)
	}

	input := Prims{B: true, I: 42, L: 123456789, F: 3.14, D: 2.718281828, S: "hello world", Bs: []byte{1, 2, 3, 4, 5}}
	dst, _ := s.AppendEncode(nil, &input)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst, err = s.AppendEncode(dst[:0], &input)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeserializeGeneric(b *testing.B) {
	superhero := Superhero{
		ID:            234765,
		AffiliationID: 9867,
		Name:          "Wolverine",
		Life:          85.25,
		Energy:        32.75,
		Powers: []*Superpower{
			{ID: 2345, Name: "Bone Claws", Damage: 5, Energy: 1.15, Passive: false},
			{ID: 2346, Name: "Regeneration", Damage: -2, Energy: 0.55, Passive: true},
			{ID: 2347, Name: "Adamant skeleton", Damage: -10, Energy: 0, Passive: true},
		},
	}

	s, err := Parse(`["null", {
"name": "Superhero",
"type": "record",
"fields": [
	{"name": "id", "type": "int"},
	{"name": "affiliation_id", "type": "int"},
	{"name": "name", "type": "string"},
	{"name": "life", "type": "float"},
	{"name": "energy", "type": "float"},
	{"name": "powers", "type": {
		"type": "array",
		"items": {
			"name": "Superpower",
			"type": "record",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "name", "type": "string"},
				{"name": "damage", "type": "float"},
				{"name": "energy", "type": "float"},
				{"name": "passive", "type": "boolean"}
			]
		}
	}}
]
}]`)
	if err != nil {
		b.Fatalf("unable to prime: %v", err)
	}

	encoded, err := s.AppendEncode(nil, &superhero)
	if err != nil {
		b.Fatalf("unable to encode: %v", err)
	}

	var out map[string]any
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = nil
		if _, err = s.Decode(encoded, &out); err != nil {
			b.Fatalf("unable to decode: %v", err)
		}
	}
	_ = out
}

// -----------------------------------------------------------------------
// Unsafe fast-path coverage: all Go type widths through struct fields
// -----------------------------------------------------------------------

// These tests exercise every branch in usInt/udInt, usLong/udLong,
// usFloat/udFloat, usDouble/udDouble by creating record schemas where
// the Go struct field types vary. The unsafe fast path compiles per-Kind
// closures, so we need struct fields of each Kind.

func testUnsafeIntLongAllKinds(t *testing.T, avroType string) {
	t.Helper()

	type I struct {
		V int `avro:"v"`
	}
	type I8 struct {
		V int8 `avro:"v"`
	}
	type I16 struct {
		V int16 `avro:"v"`
	}
	type I32 struct {
		V int32 `avro:"v"`
	}
	type I64 struct {
		V int64 `avro:"v"`
	}
	type U struct {
		V uint `avro:"v"`
	}
	type U8 struct {
		V uint8 `avro:"v"`
	}
	type U16 struct {
		V uint16 `avro:"v"`
	}
	type U32 struct {
		V uint32 `avro:"v"`
	}
	type U64 struct {
		V uint64 `avro:"v"`
	}

	schema := `{"type":"record","name":"R","fields":[{"name":"v","type":"` + avroType + `"}]}`

	check := func(t *testing.T, name string, input, output any, want int64) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			s, err := Parse(schema)
			if err != nil {
				t.Fatal(err)
			}
			dst, err := s.AppendEncode(nil, input)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if _, err = s.Decode(dst, output); err != nil {
				t.Fatalf("decode: %v", err)
			}
			got := reflect.ValueOf(output).Elem().Field(0).Int()
			if got != want {
				t.Fatalf("got %d, want %d", got, want)
			}
		})
	}

	checkU := func(t *testing.T, name string, input, output any, want uint64) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			s, err := Parse(schema)
			if err != nil {
				t.Fatal(err)
			}
			dst, err := s.AppendEncode(nil, input)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if _, err = s.Decode(dst, output); err != nil {
				t.Fatalf("decode: %v", err)
			}
			got := reflect.ValueOf(output).Elem().Field(0).Uint()
			if got != want {
				t.Fatalf("got %d, want %d", got, want)
			}
		})
	}

	check(t, "int", &I{27}, &I{}, 27)
	check(t, "int8", &I8{27}, &I8{}, 27)
	check(t, "int16", &I16{27}, &I16{}, 27)
	check(t, "int32", &I32{27}, &I32{}, 27)
	check(t, "int64", &I64{27}, &I64{}, 27)
	checkU(t, "uint", &U{27}, &U{}, 27)
	checkU(t, "uint8", &U8{27}, &U8{}, 27)
	checkU(t, "uint16", &U16{27}, &U16{}, 27)
	checkU(t, "uint32", &U32{27}, &U32{}, 27)
	checkU(t, "uint64", &U64{27}, &U64{}, 27)
}

func TestUnsafeIntAllKinds(t *testing.T)  { testUnsafeIntLongAllKinds(t, "int") }
func TestUnsafeLongAllKinds(t *testing.T) { testUnsafeIntLongAllKinds(t, "long") }

func TestUnsafeFloatDoubleKinds(t *testing.T) {
	// Test avro "float" mapped to Go float64, and avro "double" mapped to Go float32.
	type FF32 struct {
		V float32 `avro:"v"`
	}
	type FF64 struct {
		V float64 `avro:"v"`
	}

	t.Run("float_to_float32", func(t *testing.T) {
		out := roundTrip(t,
			`{"type":"record","name":"R","fields":[{"name":"v","type":"float"}]}`,
			FF32{3.14})
		if out.V < 3.13 || out.V > 3.15 {
			t.Fatalf("got %v", out.V)
		}
	})

	t.Run("float_to_float64", func(t *testing.T) {
		out := roundTrip(t,
			`{"type":"record","name":"R","fields":[{"name":"v","type":"float"}]}`,
			FF64{3.14})
		// Precision is limited to float32
		if out.V < 3.13 || out.V > 3.15 {
			t.Fatalf("got %v", out.V)
		}
	})

	t.Run("double_to_float64", func(t *testing.T) {
		out := roundTrip(t,
			`{"type":"record","name":"R","fields":[{"name":"v","type":"double"}]}`,
			FF64{2.718281828})
		if out.V != 2.718281828 {
			t.Fatalf("got %v", out.V)
		}
	})

	t.Run("double_to_float32", func(t *testing.T) {
		out := roundTrip(t,
			`{"type":"record","name":"R","fields":[{"name":"v","type":"double"}]}`,
			FF32{2.718})
		if out.V < 2.71 || out.V > 2.72 {
			t.Fatalf("got %v", out.V)
		}
	})
}

func TestUnsafePointerToPrimitive(t *testing.T) {
	// Test the tryCompileFieldSer recursive pointer-through-primitive path.
	type R struct {
		V *int32 `avro:"v"`
	}
	schema := `{"type":"record","name":"R","fields":[{"name":"v","type":"int"}]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	v := int32(42)
	in := R{V: &v}
	dst, err := s.AppendEncode(nil, &in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// Decode back — pointer fields go through slow path for deser.
	var out R
	if _, err = s.Decode(dst, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.V == nil || *out.V != 42 {
		t.Fatalf("got %v", out.V)
	}
}

func TestUnsafePointerToComplexFallback(t *testing.T) {
	// A field with a pointer to a complex type (e.g., *[]int32) should
	// use the slow path, not crash tryCompileFieldSer.
	type R struct {
		V *[]int32 `avro:"v"`
	}
	schema := `{"type":"record","name":"R","fields":[{"name":"v","type":{"type":"array","items":"int"}}]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	inner := []int32{1, 2, 3}
	in := R{V: &inner}
	dst, err := s.AppendEncode(nil, &in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out R
	if _, err = s.Decode(dst, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.V == nil || len(*out.V) != 3 || (*out.V)[0] != 1 {
		t.Fatalf("got %v", out.V)
	}
}

func TestUnsafeDecodeTruncatedBuffer(t *testing.T) {
	// Exercise error branches in unsafe deserializers (udBool, udString,
	// udBytesSlice) by feeding truncated data into struct-field decode.
	type BoolRec struct {
		V bool `avro:"v"`
	}
	type StrRec struct {
		V string `avro:"v"`
	}
	type BytesRec struct {
		V []byte `avro:"v"`
	}

	t.Run("bool_short", func(t *testing.T) {
		decodeErr(t,
			`{"type":"record","name":"R","fields":[{"name":"v","type":"boolean"}]}`,
			[]byte{}, &BoolRec{})
	})
	t.Run("string_short_length", func(t *testing.T) {
		// varint for length=100, but only 2 bytes follow.
		decodeErr(t,
			`{"type":"record","name":"R","fields":[{"name":"v","type":"string"}]}`,
			[]byte{0xc8, 0x01, 'a', 'b'}, &StrRec{})
	})
	t.Run("string_negative_length", func(t *testing.T) {
		decodeErr(t,
			`{"type":"record","name":"R","fields":[{"name":"v","type":"string"}]}`,
			[]byte{0x01}, &StrRec{}) // zigzag -1
	})
	t.Run("bytes_short_length", func(t *testing.T) {
		decodeErr(t,
			`{"type":"record","name":"R","fields":[{"name":"v","type":"bytes"}]}`,
			[]byte{0xc8, 0x01, 0x01}, &BytesRec{})
	})
	t.Run("bytes_negative_length", func(t *testing.T) {
		decodeErr(t,
			`{"type":"record","name":"R","fields":[{"name":"v","type":"bytes"}]}`,
			[]byte{0x01}, &BytesRec{}) // zigzag -1
	})
	t.Run("string_no_data", func(t *testing.T) {
		// Valid varint for length but no string data at all.
		decodeErr(t,
			`{"type":"record","name":"R","fields":[{"name":"v","type":"string"}]}`,
			[]byte{}, &StrRec{})
	})
}

func TestDecodeIntUint(t *testing.T) {
	// Exercise the CanUint branch in the slow-path deserInt.
	schema := `"int"`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	dst, err := s.AppendEncode(nil, new(int32(42)))
	if err != nil {
		t.Fatal(err)
	}
	var got uint32
	if _, err = s.Decode(dst, &got); err != nil {
		t.Fatalf("decode uint32: %v", err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
}

func TestDecodeLongUint(t *testing.T) {
	schema := `"long"`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	dst, err := s.AppendEncode(nil, new(int64(42)))
	if err != nil {
		t.Fatal(err)
	}
	var got uint64
	if _, err = s.Decode(dst, &got); err != nil {
		t.Fatalf("decode uint64: %v", err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
}

// TestUnsafeSerializeDefaults covers usInt/usLong/usFloat/usDouble returning
// nil for unsupported Go kinds (the default: branches).
func TestUnsafeSerializeDefaults(t *testing.T) {
	if usInt(reflect.Bool) != nil {
		t.Fatal("usInt(Bool) should be nil")
	}
	if usLong(reflect.Bool) != nil {
		t.Fatal("usLong(Bool) should be nil")
	}
	if usFloat(reflect.Bool) != nil {
		t.Fatal("usFloat(Bool) should be nil")
	}
	if usDouble(reflect.Bool) != nil {
		t.Fatal("usDouble(Bool) should be nil")
	}
}

// TestUnsafeDeserializeDefaults covers udInt/udLong/udFloat/udDouble returning
// nil for unsupported Go kinds (the default: branches).
func TestUnsafeDeserializeDefaults(t *testing.T) {
	if udInt(reflect.Bool) != nil {
		t.Fatal("udInt(Bool) should be nil")
	}
	if udLong(reflect.Bool) != nil {
		t.Fatal("udLong(Bool) should be nil")
	}
	if udFloat(reflect.Bool) != nil {
		t.Fatal("udFloat(Bool) should be nil")
	}
	if udDouble(reflect.Bool) != nil {
		t.Fatal("udDouble(Bool) should be nil")
	}
}

// TestUnsafeDeserializeIntErrors covers the error branches inside each
// per-Kind closure returned by udInt when given truncated input.
func TestUnsafeDeserializeIntErrors(t *testing.T) {
	kinds := []reflect.Kind{
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
	}
	for _, k := range kinds {
		fn := udInt(k)
		if fn == nil {
			t.Fatalf("udInt(%v) returned nil", k)
		}
		var buf [8]byte
		_, err := fn([]byte{}, unsafe.Pointer(&buf[0]), &slab{})
		if err == nil {
			t.Fatalf("udInt(%v) with empty input should error", k)
		}
	}
}

// TestUnsafeDeserializeLongErrors covers the error branches inside each
// per-Kind closure returned by udLong when given truncated input.
func TestUnsafeDeserializeLongErrors(t *testing.T) {
	kinds := []reflect.Kind{
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
	}
	for _, k := range kinds {
		fn := udLong(k)
		if fn == nil {
			t.Fatalf("udLong(%v) returned nil", k)
		}
		var buf [8]byte
		_, err := fn([]byte{}, unsafe.Pointer(&buf[0]), &slab{})
		if err == nil {
			t.Fatalf("udLong(%v) with empty input should error", k)
		}
	}
}

// TestUnsafeDeserializeFloatErrors covers the error branches in udFloat/udDouble
// closures when given truncated input.
func TestUnsafeDeserializeFloatErrors(t *testing.T) {
	for _, k := range []reflect.Kind{reflect.Float32, reflect.Float64} {
		fn := udFloat(k)
		if fn == nil {
			t.Fatalf("udFloat(%v) returned nil", k)
		}
		var buf [8]byte
		_, err := fn([]byte{}, unsafe.Pointer(&buf[0]), &slab{})
		if err == nil {
			t.Fatalf("udFloat(%v) with empty input should error", k)
		}
	}
	for _, k := range []reflect.Kind{reflect.Float32, reflect.Float64} {
		fn := udDouble(k)
		if fn == nil {
			t.Fatalf("udDouble(%v) returned nil", k)
		}
		var buf [8]byte
		_, err := fn([]byte{}, unsafe.Pointer(&buf[0]), &slab{})
		if err == nil {
			t.Fatalf("udDouble(%v) with empty input should error", k)
		}
	}
}

// TestUnsafePtrNilSerialize covers the nil-pointer check in the ptr-wrapping
// closure generated by tryCompileFieldSer (unsafe.go lines 178-180).
func TestUnsafePtrNilSerialize(t *testing.T) {
	type S struct {
		X *int32 `avro:"x"`
	}
	s, err := Parse(`{"type":"record","name":"S","fields":[{"name":"x","type":"int"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	// Encode with nil pointer — should trigger errUnsafeNilPtr via the fast path.
	_, err = s.AppendEncode(nil, &S{X: nil})
	if err == nil {
		t.Fatal("expected error for nil pointer field")
	}
}

// TestUnsafeBytesDeserErrors covers error paths in udBytesDeser:
// truncated input (readVarlong error) and negative length.
func TestUnsafeBytesDeserErrors(t *testing.T) {
	fn := udBytesDeser
	var buf [24]byte

	// Truncated input: readVarlong fails.
	_, err := fn([]byte{}, unsafe.Pointer(&buf[0]), &slab{})
	if err == nil {
		t.Fatal("expected error for truncated input")
	}

	// Negative length: unsigned varint 1 → zigzag-decoded = -1.
	_, err = fn([]byte{0x01}, unsafe.Pointer(&buf[0]), &slab{})
	if err == nil {
		t.Fatal("expected error for negative bytes length")
	}
}

// TestSerRecordSlowPath covers the reflect-based slow path in serRecord.ser
// by encoding a non-addressable struct value (not a pointer).
func TestSerRecordSlowPath(t *testing.T) {
	type S struct {
		X int32 `avro:"x"`
	}
	s, err := Parse(`{"type":"record","name":"S","fields":[{"name":"x","type":"int"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	// Encode struct value (not pointer) → v.CanAddr() is false → slow path.
	got, err := s.AppendEncode(nil, S{X: 42})
	if err != nil {
		t.Fatalf("slow path encode: %v", err)
	}
	// Verify by decoding.
	var out S
	if _, err := s.Decode(got, &out); err != nil {
		t.Fatal(err)
	}
	if out.X != 42 {
		t.Fatalf("got %d, want 42", out.X)
	}
}

// TestSerRecordSlowPathError covers the error branch in the reflect-based
// slow path of serRecord.ser (ser.go lines 242-244).
func TestSerRecordSlowPathError(t *testing.T) {
	type S struct {
		X int32  `avro:"x"`
		Y *int32 `avro:"y"`
	}
	s, err := Parse(`{"type":"record","name":"S","fields":[{"name":"x","type":"int"},{"name":"y","type":"int"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	// Non-addressable struct value with nil pointer field → slow path + error.
	_, err = s.AppendEncode(nil, S{X: 42, Y: nil})
	if err == nil {
		t.Fatal("expected error for nil pointer in slow path")
	}
}

// -----------------------------------------------------------------------
// Coverage tests for optimization fast paths
// -----------------------------------------------------------------------

func TestGenericUnionRoundTrip(t *testing.T) {
	// Multi-branch union ["int","string"] exercises the generic serUnion.ser
	// and deserUnion.deser paths (not the null-union fast path).
	schema := `["int","string"]`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("int branch", func(t *testing.T) {
		dst, err := s.AppendEncode(nil, new(int32(42)))
		if err != nil {
			t.Fatal(err)
		}
		var got any
		_, err = s.Decode(dst, &got)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("string branch", func(t *testing.T) {
		dst, err := s.AppendEncode(nil, new("hello"))
		if err != nil {
			t.Fatal(err)
		}
		var got any
		_, err = s.Decode(dst, &got)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("all fail encode", func(t *testing.T) {
		// Neither int nor string can encode a bool.
		encodeErr(t, schema, new(true))
	})

	t.Run("decode short buffer", func(t *testing.T) {
		var got any
		_, err := s.Decode(nil, &got)
		if err == nil {
			t.Fatal("expected error for empty buffer on generic union decode")
		}
	})

	t.Run("decode out of range", func(t *testing.T) {
		var got any
		// zigzag(2) = 0x04, out of range for 2-element union
		_, err := s.Decode([]byte{0x04}, &got)
		if err == nil {
			t.Fatal("expected error for out-of-range union index")
		}
	})
}

func TestSerNullNonNil(t *testing.T) {
	// Standalone "null" schema with non-nil value should fail.
	encodeErr(t, `"null"`, new(int32(42)))
}

func TestDeserNullUnionErrors(t *testing.T) {
	schema := `["null","int"]`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("short buffer", func(t *testing.T) {
		var v *int32
		_, err := s.Decode(nil, &v)
		if err == nil {
			t.Fatal("expected error for empty buffer")
		}
	})

	t.Run("invalid index byte", func(t *testing.T) {
		var v *int32
		_, err := s.Decode([]byte{0x04}, &v) // 0x04 is neither 0 nor 2
		if err == nil {
			t.Fatal("expected error for invalid index byte")
		}
	})
}

func TestReadUvarintOverflow(t *testing.T) {
	// 5 bytes all with continuation bit set → overflow error.
	_, _, err := readUvarint([]byte{0x80, 0x80, 0x80, 0x80, 0x80})
	if err == nil {
		t.Fatal("expected overflow error")
	}
}

func TestReadUvarlongOverflow(t *testing.T) {
	// 10 bytes all with continuation bit set → overflow error.
	_, _, err := readUvarlong([]byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80})
	if err == nil {
		t.Fatal("expected overflow error")
	}
}

// TestRoundTripArrayNullUnionRecord covers usArrayNullUnionRecord for ser
// and the reflect fallback for deser of []*T where items are ["null", record].
func TestRoundTripArrayNullUnionRecord(t *testing.T) {
	type Inner struct {
		X int32  `avro:"x"`
		Y string `avro:"y"`
	}
	type Outer struct {
		Items []*Inner `avro:"items"`
	}
	schema := `{
		"type": "record",
		"name": "Outer",
		"fields": [{
			"name": "items",
			"type": {"type": "array", "items": ["null", {
				"type": "record",
				"name": "Inner",
				"fields": [
					{"name": "x", "type": "int"},
					{"name": "y", "type": "string"}
				]
			}]}
		}]
	}`

	t.Run("mixed_nil_and_values", func(t *testing.T) {
		input := Outer{Items: []*Inner{
			{X: 1, Y: "one"},
			nil,
			{X: 3, Y: "three"},
		}}
		got := roundTrip(t, schema, input)
		if len(got.Items) != 3 {
			t.Fatalf("expected 3 items, got %d", len(got.Items))
		}
		if got.Items[0] == nil || got.Items[0].X != 1 || got.Items[0].Y != "one" {
			t.Errorf("items[0]: got %+v, want {1, one}", got.Items[0])
		}
		if got.Items[1] != nil {
			t.Errorf("items[1]: got %+v, want nil", got.Items[1])
		}
		if got.Items[2] == nil || got.Items[2].X != 3 || got.Items[2].Y != "three" {
			t.Errorf("items[2]: got %+v, want {3, three}", got.Items[2])
		}
	})

	t.Run("empty", func(t *testing.T) {
		input := Outer{Items: []*Inner{}}
		got := roundTrip(t, schema, input)
		if len(got.Items) != 0 {
			t.Errorf("expected empty, got %+v", got.Items)
		}
	})

	t.Run("all_nil", func(t *testing.T) {
		input := Outer{Items: []*Inner{nil, nil}}
		got := roundTrip(t, schema, input)
		if len(got.Items) != 2 {
			t.Fatalf("expected 2 items, got %d", len(got.Items))
		}
		if got.Items[0] != nil || got.Items[1] != nil {
			t.Errorf("expected all nil, got %+v", got.Items)
		}
	})
}

// TestRoundTripArrayNullUnionPrimitive covers usArrayNullUnionPtr for ser
// of []*int32 where items are ["null", "int"].
func TestRoundTripArrayNullUnionPrimitive(t *testing.T) {
	type Outer struct {
		Vals []*int32 `avro:"vals"`
	}
	schema := `{
		"type": "record",
		"name": "Outer",
		"fields": [{
			"name": "vals",
			"type": {"type": "array", "items": ["null", "int"]}
		}]
	}`

	t.Run("mixed", func(t *testing.T) {
		input := Outer{Vals: []*int32{new(int32(10)), nil, new(int32(30))}}
		got := roundTrip(t, schema, input)
		if len(got.Vals) != 3 {
			t.Fatalf("expected 3 vals, got %d", len(got.Vals))
		}
		if got.Vals[0] == nil || *got.Vals[0] != 10 {
			t.Errorf("vals[0]: got %v, want 10", got.Vals[0])
		}
		if got.Vals[1] != nil {
			t.Errorf("vals[1]: got %v, want nil", got.Vals[1])
		}
		if got.Vals[2] == nil || *got.Vals[2] != 30 {
			t.Errorf("vals[2]: got %v, want 30", got.Vals[2])
		}
	})

	t.Run("empty", func(t *testing.T) {
		input := Outer{Vals: []*int32{}}
		got := roundTrip(t, schema, input)
		if len(got.Vals) != 0 {
			t.Errorf("expected empty, got %+v", got.Vals)
		}
	})
}

// TestTopLevelArrayPtrDecode covers the reflect slow path for deserArray
// when decoding a top-level array into []*T (batch pointer alloc path).
func TestTopLevelArrayPtrDecode(t *testing.T) {
	type Rec struct {
		V int32 `avro:"v"`
	}
	schema := `{"type": "array", "items": {
		"type": "record", "name": "Rec",
		"fields": [{"name": "v", "type": "int"}]
	}}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	input := []*Rec{{V: 1}, {V: 2}, {V: 3}}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var output []*Rec
	rem, err := s.Decode(encoded, &output)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("leftover bytes: %d", len(rem))
	}
	if len(output) != 3 {
		t.Fatalf("expected 3 records, got %d", len(output))
	}
	for i, r := range output {
		if r == nil || r.V != int32(i+1) {
			t.Errorf("output[%d]: got %+v, want {V: %d}", i, r, i+1)
		}
	}
}

// TestSerNullUnionNilReflectPath covers the reflect serNullUnion nil encoding
// by encoding a nil value through a top-level ["null", record] schema.
func TestSerNullUnionNilReflectPath(t *testing.T) {
	schema := `["null", {"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}]`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	type R struct {
		X int32 `avro:"x"`
	}
	var nilPtr *R
	encoded, err := s.AppendEncode(nil, nilPtr)
	if err != nil {
		t.Fatalf("encode nil: %v", err)
	}
	// Null union encodes as a single 0 byte.
	if !bytes.Equal(encoded, []byte{0}) {
		t.Fatalf("expected [0], got %v", encoded)
	}
}

// TestSerArrayEmptyReflectPath covers serArray.ser empty-length path
// via the reflect slow path (non-addressable value).
func TestSerArrayEmptyReflectPath(t *testing.T) {
	schema := `{"type": "array", "items": "int"}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	// Encode non-addressable empty slice.
	empty := []int32{}
	encoded, err := s.AppendEncode(nil, empty)
	if err != nil {
		t.Fatalf("encode empty array: %v", err)
	}
	// Empty array: varlong(0) = 0x00.
	if !bytes.Equal(encoded, []byte{0}) {
		t.Fatalf("expected [0], got %v", encoded)
	}
}

// TestUnsafeNullUnionErrors covers error paths in unsafe null-union deser:
// short buffer and invalid index bytes.
func TestUnsafeNullUnionErrors(t *testing.T) {
	type Wrapper struct {
		Value *int32 `avro:"value"`
	}
	schema := `{
		"type": "record",
		"name": "Wrapper",
		"fields": [{"name": "value", "type": ["null", "int"]}]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("short_buffer", func(t *testing.T) {
		var out Wrapper
		_, err := s.Decode([]byte{}, &out)
		if err == nil {
			t.Fatal("expected error for short buffer")
		}
	})

	t.Run("invalid_index", func(t *testing.T) {
		var out Wrapper
		_, err := s.Decode([]byte{0x04}, &out)
		if err == nil {
			t.Fatal("expected error for invalid index byte")
		}
	})
}

// TestUnsafeRecordFastPtrError covers the error path in serRecordFastPtr
// and deserRecordFastPtr when an inner field returns an error.
func TestUnsafeRecordFastPtrError(t *testing.T) {
	type Inner struct {
		X int32  `avro:"x"`
		Y string `avro:"y"`
	}
	type Outer struct {
		Item *Inner `avro:"item"`
	}
	schema := `{
		"type": "record",
		"name": "Outer",
		"fields": [{"name": "item", "type": ["null", {
			"type": "record", "name": "Inner",
			"fields": [
				{"name": "x", "type": "int"},
				{"name": "y", "type": "string"}
			]
		}]}]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	// Warm up: encode a valid value to compile fast paths.
	input := Outer{Item: &Inner{X: 1, Y: "hello"}}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// Decode truncated data to trigger error inside deserRecordFastPtr.
	var out Outer
	// Take just the non-null union byte + partial record.
	if len(encoded) > 2 {
		_, err = s.Decode(encoded[:2], &out)
		if err == nil {
			t.Fatal("expected error for truncated record")
		}
	}
}

// TestArrayDirectIntDeser covers udArrayDirect for []int32 inside a record.
func TestArrayDirectIntDeser(t *testing.T) {
	type Wrapper struct {
		Vals []int32 `avro:"vals"`
	}
	schema := `{
		"type": "record",
		"name": "Wrapper",
		"fields": [{"name": "vals", "type": {"type": "array", "items": "int"}}]
	}`
	input := Wrapper{Vals: []int32{10, 20, 30}}
	got := roundTrip(t, schema, input)
	if !reflect.DeepEqual(got.Vals, input.Vals) {
		t.Errorf("got %v, want %v", got.Vals, input.Vals)
	}
}

// TestArrayDirectStringDeser covers udArrayDirect/usArrayDirect for []string.
func TestArrayDirectStringDeser(t *testing.T) {
	type Wrapper struct {
		Vals []string `avro:"vals"`
	}
	schema := `{
		"type": "record",
		"name": "Wrapper",
		"fields": [{"name": "vals", "type": {"type": "array", "items": "string"}}]
	}`
	input := Wrapper{Vals: []string{"a", "bb", "ccc"}}
	got := roundTrip(t, schema, input)
	if !reflect.DeepEqual(got.Vals, input.Vals) {
		t.Errorf("got %v, want %v", got.Vals, input.Vals)
	}
}

// TestArrayRecordValueDeser covers usArrayRecord for []T (value slice).
func TestArrayRecordValueDeser(t *testing.T) {
	type Item struct {
		N int32 `avro:"n"`
	}
	type Wrapper struct {
		Items []Item `avro:"items"`
	}
	schema := `{
		"type": "record",
		"name": "Wrapper",
		"fields": [{"name": "items", "type": {"type": "array", "items": {
			"type": "record", "name": "Item",
			"fields": [{"name": "n", "type": "int"}]
		}}}]
	}`
	input := Wrapper{Items: []Item{{N: 1}, {N: 2}}}
	got := roundTrip(t, schema, input)
	if !reflect.DeepEqual(got.Items, input.Items) {
		t.Errorf("got %v, want %v", got.Items, input.Items)
	}
}

// TestWarmFastPathArrayPtrRecord does two round-trips so the second pass
// goes through the serRecordFastPtr / deserRecordFastPtr branches inside
// array and null-union callers after the inner record's fast path is compiled.
func TestWarmFastPathArrayPtrRecord(t *testing.T) {
	schema := `
["null",
{
"name": "Superhero",
"type": "record",
"fields": [
	{"name": "id", "type": "int"},
	{"name": "affiliation_id", "type": "int"},
	{"name": "name", "type": "string"},
	{"name": "life", "type": "float"},
	{"name": "energy", "type": "float"},
	{"name": "powers", "type": {
		"type": "array",
		"items": {
			"name": "Superpower",
			"type": "record",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "name", "type": "string"},
				{"name": "damage", "type": "float"},
				{"name": "energy", "type": "float"},
				{"name": "passive", "type": "boolean"}
			]
		}
	}}
]
}]`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	hero := Superhero{
		ID: 1, AffiliationID: 2, Name: "X", Life: 1, Energy: 2,
		Powers: []*Superpower{{ID: 1, Name: "P", Damage: 1, Energy: 1, Passive: true}},
	}
	// First pass: compiles fast paths for inner records.
	enc1, err := s.AppendEncode(nil, &hero)
	if err != nil {
		t.Fatal(err)
	}
	var out1 Superhero
	if _, err := s.Decode(enc1, &out1); err != nil {
		t.Fatal(err)
	}
	// Second pass: now inner fast paths are compiled, so
	// serRecordFastPtr / deserRecordFastPtr branches are taken.
	enc2, err := s.AppendEncode(nil, &hero)
	if err != nil {
		t.Fatal(err)
	}
	var out2 Superhero
	if _, err := s.Decode(enc2, &out2); err != nil {
		t.Fatal(err)
	}
	if out2.Powers[0].Name != "P" {
		t.Errorf("second pass mismatch: %+v", out2.Powers[0])
	}
}

// TestWarmFastPathNullUnionRecord does two round-trips for a null-union
// record field to exercise the allFast serRecordFastPtr / deserRecordFastPtr
// branches inside usNullUnionRecord / udNullUnionRecord.
func TestWarmFastPathNullUnionRecord(t *testing.T) {
	type LongList struct {
		Value int64     `avro:"value"`
		Next  *LongList `avro:"next"`
	}
	schema := `{
		"type": "record",
		"name": "LongList",
		"fields": [
			{"name": "value", "type": "long"},
			{"name": "next", "type": ["null", "LongList"]}
		]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	input := LongList{Value: 1, Next: &LongList{Value: 2}}
	// First pass: compiles fast paths.
	enc1, _ := s.AppendEncode(nil, &input)
	var out1 LongList
	s.Decode(enc1, &out1)
	// Second pass: exercises serRecordFastPtr / deserRecordFastPtr.
	enc2, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatal(err)
	}
	var out2 LongList
	if _, err := s.Decode(enc2, &out2); err != nil {
		t.Fatal(err)
	}
	if out2.Next == nil || out2.Next.Value != 2 {
		t.Errorf("second pass: got %+v", out2)
	}
}

// TestWarmFastPathArrayNullUnionRecord does two round-trips for an array
// of null-union records to exercise the allFast branch inside
// usArrayNullUnionRecord.
func TestWarmFastPathArrayNullUnionRecord(t *testing.T) {
	type Inner struct {
		X int32 `avro:"x"`
	}
	type Outer struct {
		Items []*Inner `avro:"items"`
	}
	schema := `{
		"type": "record",
		"name": "Outer",
		"fields": [{"name": "items", "type": {"type": "array", "items": ["null", {
			"type": "record", "name": "Inner",
			"fields": [{"name": "x", "type": "int"}]
		}]}}]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	input := Outer{Items: []*Inner{{X: 1}, nil, {X: 3}}}
	// First pass: warms up.
	enc1, _ := s.AppendEncode(nil, &input)
	var out1 Outer
	s.Decode(enc1, &out1)
	// Second pass: hits fast branches.
	enc2, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatal(err)
	}
	var out2 Outer
	if _, err := s.Decode(enc2, &out2); err != nil {
		t.Fatal(err)
	}
	if out2.Items[0].X != 1 || out2.Items[1] != nil || out2.Items[2].X != 3 {
		t.Errorf("second pass: %+v", out2)
	}
}

// TestRegularUnionField covers the "union" avroType path in tryCompileFieldSer
// and tryCompileFieldDeser, which return nil and fall back to the reflect path.
func TestRegularUnionField(t *testing.T) {
	type Wrapper struct {
		Val any `avro:"val"`
	}
	schema := `{
		"type": "record",
		"name": "Wrapper",
		"fields": [{"name": "val", "type": ["int", "string"]}]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	// Encode int value.
	input := Wrapper{Val: int32(42)}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatalf("encode int: %v", err)
	}
	var out Wrapper
	if _, err := s.Decode(encoded, &out); err != nil {
		t.Fatalf("decode int: %v", err)
	}
	if out.Val != int32(42) {
		t.Errorf("got %v, want 42", out.Val)
	}
	// Encode string value.
	input2 := Wrapper{Val: "hello"}
	encoded2, err := s.AppendEncode(nil, &input2)
	if err != nil {
		t.Fatalf("encode string: %v", err)
	}
	var out2 Wrapper
	if _, err := s.Decode(encoded2, &out2); err != nil {
		t.Fatalf("decode string: %v", err)
	}
	if out2.Val != "hello" {
		t.Errorf("got %v, want hello", out2.Val)
	}
}

// TestSerRecordFastPtrError triggers the error path in serRecordFastPtr
// by encoding a nil *int32 field mapped to non-null "int" inside a
// null-union record whose fast path is pre-warmed.
func TestSerRecordFastPtrError(t *testing.T) {
	type Inner struct {
		P *int32 `avro:"p"`
	}
	type Outer struct {
		Item *Inner `avro:"item"`
	}
	schema := `{
		"type": "record",
		"name": "Outer",
		"fields": [{"name": "item", "type": ["null", {
			"type": "record", "name": "Inner",
			"fields": [{"name": "p", "type": "int"}]
		}]}]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	// Warm up: encode valid value to compile fast paths.
	valid := Outer{Item: &Inner{P: new(int32(42))}}
	if _, err := s.AppendEncode(nil, &valid); err != nil {
		t.Fatal(err)
	}
	var dummy Outer
	enc, _ := s.AppendEncode(nil, &valid)
	s.Decode(enc, &dummy)
	// Now encode with nil P → error in serRecordFastPtr.
	bad := Outer{Item: &Inner{P: nil}}
	_, err = s.AppendEncode(nil, &bad)
	if err == nil {
		t.Fatal("expected error for nil non-union pointer")
	}
}

// TestDeserRecordFastPtrError triggers the error path in deserRecordFastPtr
// by feeding truncated data after warming up the fast path.
func TestDeserRecordFastPtrError(t *testing.T) {
	type Inner struct {
		X int32  `avro:"x"`
		Y string `avro:"y"`
	}
	type Outer struct {
		Item *Inner `avro:"item"`
	}
	schema := `{
		"type": "record",
		"name": "Outer",
		"fields": [{"name": "item", "type": ["null", {
			"type": "record", "name": "Inner",
			"fields": [
				{"name": "x", "type": "int"},
				{"name": "y", "type": "string"}
			]
		}]}]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	valid := Outer{Item: &Inner{X: 1, Y: "hello"}}
	enc, _ := s.AppendEncode(nil, &valid)
	// First decode: warms up inner record fast path.
	var out1 Outer
	s.Decode(enc, &out1)
	// Second decode: now inner fast path is compiled.
	var out2 Outer
	s.Decode(enc, &out2)
	// Third decode with truncated data: triggers error in deserRecordFastPtr.
	var out3 Outer
	_, err = s.Decode(enc[:2], &out3)
	if err == nil {
		t.Fatal("expected error for truncated data")
	}
}

// TestArrayPtrRecordNilError covers the nil pointer error in usArrayPtrRecord
// when an array element is nil but the schema items are a record (not null-union).
func TestArrayPtrRecordNilError(t *testing.T) {
	type Inner struct {
		X int32 `avro:"x"`
	}
	type Outer struct {
		Items []*Inner `avro:"items"`
	}
	schema := `{
		"type": "record",
		"name": "Outer",
		"fields": [{"name": "items", "type": {"type": "array", "items": {
			"type": "record", "name": "Inner",
			"fields": [{"name": "x", "type": "int"}]
		}}}]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	bad := Outer{Items: []*Inner{nil}}
	_, err = s.AppendEncode(nil, &bad)
	if err == nil {
		t.Fatal("expected error for nil array element")
	}
}

// TestArrayNegativeCountBlock covers the negative-count block path in array
// deser (both udArrayPtrRecord and udArrayDirect). A negative count
// indicates the block's byte size follows.
func TestArrayNegativeCountBlock(t *testing.T) {
	// Test with a value array ([]int32) to exercise udArrayDirect.
	t.Run("direct", func(t *testing.T) {
		type Wrapper struct {
			Vals []int32 `avro:"vals"`
		}
		schema := `{
			"type": "record",
			"name": "Wrapper",
			"fields": [{"name": "vals", "type": {"type": "array", "items": "int"}}]
		}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		// Warm up fast path with normal encode/decode.
		normal := Wrapper{Vals: []int32{1, 2, 3}}
		enc, _ := s.AppendEncode(nil, &normal)
		var out Wrapper
		s.Decode(enc, &out)

		// Manually craft data with negative count block:
		// negative count, then byte_size as varlong,
		// then 3 varint-encoded elements, then terminating 0.
		var elems []byte
		elems = appendVarint(elems, 1)
		elems = appendVarint(elems, 2)
		elems = appendVarint(elems, 3)
		var data []byte
		data = appendVarlong(data, -3)                // negative count: -3
		data = appendVarlong(data, int64(len(elems))) // byte size
		data = append(data, elems...)
		data = append(data, 0) // terminator

		var out2 Wrapper
		_, err = s.Decode(data, &out2)
		if err != nil {
			t.Fatalf("decode negative count block: %v", err)
		}
		if !reflect.DeepEqual(out2.Vals, []int32{1, 2, 3}) {
			t.Errorf("got %v, want [1 2 3]", out2.Vals)
		}
	})

	// Test with ptr record array to exercise udArrayPtrRecord.
	t.Run("ptr_record", func(t *testing.T) {
		type Rec struct {
			V int32 `avro:"v"`
		}
		type Wrapper struct {
			Items []*Rec `avro:"items"`
		}
		schema := `{
			"type": "record",
			"name": "Wrapper",
			"fields": [{"name": "items", "type": {"type": "array", "items": {
				"type": "record", "name": "Rec",
				"fields": [{"name": "v", "type": "int"}]
			}}}]
		}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		// Warm up.
		normal := Wrapper{Items: []*Rec{{V: 1}}}
		enc, _ := s.AppendEncode(nil, &normal)
		var out Wrapper
		s.Decode(enc, &out)

		// Craft negative count block: -2 elements, each varint(10)=0x14, varint(20)=0x28.
		var elems []byte
		elems = appendVarint(elems, 10)
		elems = appendVarint(elems, 20)
		var data []byte
		data = appendVarlong(data, -2)
		data = appendVarlong(data, int64(len(elems)))
		data = append(data, elems...)
		data = append(data, 0) // terminator

		var out2 Wrapper
		_, err = s.Decode(data, &out2)
		if err != nil {
			t.Fatalf("decode negative count block: %v", err)
		}
		if len(out2.Items) != 2 || out2.Items[0].V != 10 || out2.Items[1].V != 20 {
			t.Errorf("got %+v, want [{10} {20}]", out2.Items)
		}
	})
}

// TestArrayMultiBlockDeser covers the else branch in array deser (cap >= newLen)
// by feeding a two-block array through the fast deser path.
func TestArrayMultiBlockDeser(t *testing.T) {
	t.Run("direct", func(t *testing.T) {
		type Wrapper struct {
			Vals []int32 `avro:"vals"`
		}
		schema := `{
			"type": "record",
			"name": "Wrapper",
			"fields": [{"name": "vals", "type": {"type": "array", "items": "int"}}]
		}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		// Warm up.
		s.AppendEncode(nil, &Wrapper{Vals: []int32{1}})

		// Craft two blocks: block1=[1,2], block2=[3], terminator.
		var data []byte
		data = appendVarlong(data, 2) // count=2
		data = appendVarint(data, 1)  // elem 1
		data = appendVarint(data, 2)  // elem 2
		data = appendVarlong(data, 1) // count=1
		data = appendVarint(data, 3)  // elem 3
		data = append(data, 0)        // terminator

		var out Wrapper
		_, err = s.Decode(data, &out)
		if err != nil {
			t.Fatalf("decode multi-block: %v", err)
		}
		if !reflect.DeepEqual(out.Vals, []int32{1, 2, 3}) {
			t.Errorf("got %v, want [1 2 3]", out.Vals)
		}
	})

	t.Run("ptr_record", func(t *testing.T) {
		type Rec struct {
			V int32 `avro:"v"`
		}
		type Wrapper struct {
			Items []*Rec `avro:"items"`
		}
		schema := `{
			"type": "record",
			"name": "Wrapper",
			"fields": [{"name": "items", "type": {"type": "array", "items": {
				"type": "record", "name": "Rec",
				"fields": [{"name": "v", "type": "int"}]
			}}}]
		}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		// Warm up.
		normal := Wrapper{Items: []*Rec{{V: 1}}}
		enc, _ := s.AppendEncode(nil, &normal)
		var dummy Wrapper
		s.Decode(enc, &dummy)

		// Two blocks: block1=[Rec{10}], block2=[Rec{20}], terminator.
		var data []byte
		data = appendVarlong(data, 1)
		data = appendVarint(data, 10)
		data = appendVarlong(data, 1)
		data = appendVarint(data, 20)
		data = append(data, 0)

		var out Wrapper
		_, err = s.Decode(data, &out)
		if err != nil {
			t.Fatalf("decode multi-block: %v", err)
		}
		if len(out.Items) != 2 || out.Items[0].V != 10 || out.Items[1].V != 20 {
			t.Errorf("got %+v, want [{10} {20}]", out.Items)
		}
	})
}

// TestArrayDeserTruncatedData covers error paths in udArrayDirect and
// udArrayPtrRecord when the data is truncated mid-element.
func TestArrayDeserTruncatedData(t *testing.T) {
	t.Run("direct_truncated_count", func(t *testing.T) {
		type Wrapper struct {
			Vals []int32 `avro:"vals"`
		}
		schema := `{
			"type": "record",
			"name": "Wrapper",
			"fields": [{"name": "vals", "type": {"type": "array", "items": "int"}}]
		}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		// Warm up so fast path is compiled.
		s.AppendEncode(nil, &Wrapper{Vals: []int32{1}})

		// Truncated: count says 2 elements but only 1 provided.
		var data []byte
		data = appendVarlong(data, 2)
		data = appendVarint(data, 1) // only 1 element
		var out Wrapper
		_, err = s.Decode(data, &out)
		if err == nil {
			t.Fatal("expected error for truncated array data")
		}
	})

	t.Run("ptr_record_truncated", func(t *testing.T) {
		type Rec struct {
			V int32 `avro:"v"`
		}
		type Wrapper struct {
			Items []*Rec `avro:"items"`
		}
		schema := `{
			"type": "record",
			"name": "Wrapper",
			"fields": [{"name": "items", "type": {"type": "array", "items": {
				"type": "record", "name": "Rec",
				"fields": [{"name": "v", "type": "int"}]
			}}}]
		}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		// Warm up.
		normal := Wrapper{Items: []*Rec{{V: 1}}}
		enc, _ := s.AppendEncode(nil, &normal)
		var dummy Wrapper
		s.Decode(enc, &dummy)

		// Count says 2 but only 1 element of data.
		var data []byte
		data = appendVarlong(data, 2)
		data = appendVarint(data, 10) // only 1 element
		var out Wrapper
		_, err = s.Decode(data, &out)
		if err == nil {
			t.Fatal("expected error for truncated array data")
		}
	})

	t.Run("direct_truncated_readvarlong", func(t *testing.T) {
		type Wrapper struct {
			Vals []int32 `avro:"vals"`
		}
		schema := `{
			"type": "record",
			"name": "Wrapper",
			"fields": [{"name": "vals", "type": {"type": "array", "items": "int"}}]
		}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		s.AppendEncode(nil, &Wrapper{Vals: []int32{1}})
		// Empty data: readVarlong fails.
		var out Wrapper
		_, err = s.Decode([]byte{}, &out)
		if err == nil {
			t.Fatal("expected error for empty data")
		}
	})

	t.Run("ptr_record_truncated_readvarlong", func(t *testing.T) {
		type Rec struct {
			V int32 `avro:"v"`
		}
		type Wrapper struct {
			Items []*Rec `avro:"items"`
		}
		schema := `{
			"type": "record",
			"name": "Wrapper",
			"fields": [{"name": "items", "type": {"type": "array", "items": {
				"type": "record", "name": "Rec",
				"fields": [{"name": "v", "type": "int"}]
			}}}]
		}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		normal := Wrapper{Items: []*Rec{{V: 1}}}
		enc, _ := s.AppendEncode(nil, &normal)
		var dummy Wrapper
		s.Decode(enc, &dummy)
		// Empty data: readVarlong fails.
		var out Wrapper
		_, err = s.Decode([]byte{}, &out)
		if err == nil {
			t.Fatal("expected error for empty data")
		}
	})
}

// TestArraySerError covers error paths in usArrayDirect and usArrayRecord.
func TestArraySerError(t *testing.T) {
	type Inner struct {
		P *int32 `avro:"p"` // non-null-union pointer; nil → error
	}
	type Outer struct {
		Items []Inner `avro:"items"` // value slice, not ptr slice
	}
	schema := `{
		"type": "record",
		"name": "Outer",
		"fields": [{"name": "items", "type": {"type": "array", "items": {
			"type": "record", "name": "Inner",
			"fields": [{"name": "p", "type": "int"}]
		}}}]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	bad := Outer{Items: []Inner{{P: nil}}}
	_, err = s.AppendEncode(nil, &bad)
	if err == nil {
		t.Fatal("expected error for nil pointer in array element")
	}
}

// TestNonPtrNullUnionField covers tryCompileFieldSer/tryCompileFieldDeser
// returning nil for null-union fields where the Go type is not a pointer.
func TestNonPtrNullUnionField(t *testing.T) {
	type Wrapper struct {
		Value int32 `avro:"value"` // non-pointer for ["null","int"]
	}
	schema := `{
		"type": "record",
		"name": "Wrapper",
		"fields": [{"name": "value", "type": ["null", "int"]}]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	input := Wrapper{Value: 42}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out Wrapper
	if _, err := s.Decode(encoded, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Value != 42 {
		t.Errorf("got %d, want 42", out.Value)
	}
}

// TestNullUnionMapField covers tryCompileFieldSer/tryCompileFieldDeser
// returning nil for null-union of map (inner compile fails).
func TestNullUnionMapField(t *testing.T) {
	type Wrapper struct {
		M *map[string]int32 `avro:"m"`
	}
	schema := `{
		"type": "record",
		"name": "Wrapper",
		"fields": [{"name": "m", "type": ["null", {"type": "map", "values": "int"}]}]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]int32{"a": 1, "b": 2}
	input := Wrapper{M: &m}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out Wrapper
	if _, err := s.Decode(encoded, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.M == nil || (*out.M)["a"] != 1 {
		t.Errorf("got %v, want map with a=1", out.M)
	}
	// Also test nil case.
	input2 := Wrapper{M: nil}
	encoded2, err := s.AppendEncode(nil, &input2)
	if err != nil {
		t.Fatalf("encode nil: %v", err)
	}
	var out2 Wrapper
	if _, err := s.Decode(encoded2, &out2); err != nil {
		t.Fatalf("decode nil: %v", err)
	}
	if out2.M != nil {
		t.Errorf("got %v, want nil", out2.M)
	}
}

// TestArrayEmptyPtrRecord covers the n==0 early return in usArrayPtrRecord.
func TestArrayEmptyPtrRecord(t *testing.T) {
	type Rec struct {
		V int32 `avro:"v"`
	}
	type Wrapper struct {
		Items []*Rec `avro:"items"`
	}
	schema := `{
		"type": "record",
		"name": "Wrapper",
		"fields": [{"name": "items", "type": {"type": "array", "items": {
			"type": "record", "name": "Rec",
			"fields": [{"name": "v", "type": "int"}]
		}}}]
	}`
	input := Wrapper{Items: []*Rec{}}
	got := roundTrip(t, schema, input)
	if len(got.Items) != 0 {
		t.Errorf("expected empty, got %v", got.Items)
	}
}

// TestUdNullUnionRecordErrors covers error paths in udNullUnionRecord
// after the fast path has been warmed up.
func TestUdNullUnionRecordErrors(t *testing.T) {
	type LongList struct {
		Value int64     `avro:"value"`
		Next  *LongList `avro:"next"`
	}
	schema := `{
		"type": "record",
		"name": "LongList",
		"fields": [
			{"name": "value", "type": "long"},
			{"name": "next", "type": ["null", "LongList"]}
		]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	// Warm up: encode/decode twice to compile inner fast path.
	input := LongList{Value: 1, Next: &LongList{Value: 2}}
	enc, _ := s.AppendEncode(nil, &input)
	var out LongList
	s.Decode(enc, &out)
	s.Decode(enc, &out) // second decode to ensure inner fast path is compiled

	t.Run("short_buffer", func(t *testing.T) {
		// Craft data: value=1 (varint 0x02), then empty for next field.
		var data []byte
		data = appendVarlong(data, 1)
		var out LongList
		_, err := s.Decode(data, &out)
		if err == nil {
			t.Fatal("expected error for short buffer in null-union")
		}
	})

	t.Run("invalid_index", func(t *testing.T) {
		// Craft data: value=1, then invalid null-union byte 0x04.
		var data []byte
		data = appendVarlong(data, 1)
		data = append(data, 0x04) // invalid index byte
		var out LongList
		_, err := s.Decode(data, &out)
		if err == nil {
			t.Fatal("expected error for invalid index byte")
		}
	})
}

// TestArraySerErrorWarm covers error paths in usArrayPtrRecord, usArrayRecord,
// usArrayNullUnionRecord, usArrayNullUnionPtr, and usArrayDirect after
// the fast path has been warmed up.
func TestArraySerErrorWarm(t *testing.T) {
	t.Run("ptr_record_error", func(t *testing.T) {
		type Inner struct {
			P *int32 `avro:"p"` // non-null-union; nil will error
		}
		type Outer struct {
			Items []*Inner `avro:"items"`
		}
		schema := `{
			"type": "record",
			"name": "Outer",
			"fields": [{"name": "items", "type": {"type": "array", "items": {
				"type": "record", "name": "Inner",
				"fields": [{"name": "p", "type": "int"}]
			}}}]
		}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		// Warm up with valid data.
		valid := Outer{Items: []*Inner{{P: new(int32(1))}}}
		enc, _ := s.AppendEncode(nil, &valid)
		var dummy Outer
		s.Decode(enc, &dummy)
		s.AppendEncode(nil, &valid) // second call warms inner fast path

		// Now encode with nil P inside a record → error.
		bad := Outer{Items: []*Inner{{P: nil}}}
		_, err = s.AppendEncode(nil, &bad)
		if err == nil {
			t.Fatal("expected error for nil pointer in array record")
		}
	})

	t.Run("null_union_record_error", func(t *testing.T) {
		type Inner struct {
			P *int32 `avro:"p"` // non-null-union
		}
		type Outer struct {
			Items []*Inner `avro:"items"`
		}
		schema := `{
			"type": "record",
			"name": "Outer",
			"fields": [{"name": "items", "type": {"type": "array", "items": ["null", {
				"type": "record", "name": "Inner",
				"fields": [{"name": "p", "type": "int"}]
			}]}}]
		}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		// Warm up.
		valid := Outer{Items: []*Inner{{P: new(int32(1))}}}
		enc, _ := s.AppendEncode(nil, &valid)
		var dummy Outer
		s.Decode(enc, &dummy)
		s.AppendEncode(nil, &valid) // warm inner fast path

		// Encode with nil P → error in inner record ser.
		bad := Outer{Items: []*Inner{{P: nil}}}
		_, err = s.AppendEncode(nil, &bad)
		if err == nil {
			t.Fatal("expected error for nil pointer in array null-union record")
		}
	})

	t.Run("value_array_record_error", func(t *testing.T) {
		type Inner struct {
			P *int32 `avro:"p"` // non-null-union
		}
		type Outer struct {
			Items []Inner `avro:"items"` // value slice
		}
		schema := `{
			"type": "record",
			"name": "Outer",
			"fields": [{"name": "items", "type": {"type": "array", "items": {
				"type": "record", "name": "Inner",
				"fields": [{"name": "p", "type": "int"}]
			}}}]
		}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		valid := Outer{Items: []Inner{{P: new(int32(1))}}}
		enc, _ := s.AppendEncode(nil, &valid)
		var dummy Outer
		s.Decode(enc, &dummy)
		s.AppendEncode(nil, &valid)

		bad := Outer{Items: []Inner{{P: nil}}}
		_, err = s.AppendEncode(nil, &bad)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("direct_error", func(t *testing.T) {
		type Inner struct {
			P *int32 `avro:"p"` // non-null-union
		}
		type Outer struct {
			Items []Inner `avro:"items"` // value slice
		}
		schema := `{
			"type": "record",
			"name": "Outer",
			"fields": [{"name": "items", "type": {"type": "array", "items": {
				"type": "record", "name": "Inner",
				"fields": [{"name": "p", "type": "int"}]
			}}}]
		}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		valid := Outer{Items: []Inner{{P: new(int32(1))}}}
		s.AppendEncode(nil, &valid) // warm up

		bad := Outer{Items: []Inner{{P: nil}}}
		_, err = s.AppendEncode(nil, &bad)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("null_union_ptr_error", func(t *testing.T) {
		type Inner struct {
			P *int32 `avro:"p"` // non-null-union
		}
		type Outer struct {
			Items []*Inner `avro:"items"`
		}
		schema := `{
			"type": "record",
			"name": "Outer",
			"fields": [{"name": "items", "type": {"type": "array", "items": ["null", {
				"type": "record", "name": "Inner",
				"fields": [{"name": "p", "type": "int"}]
			}]}}]
		}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		valid := Outer{Items: []*Inner{{P: new(int32(1))}}}
		s.AppendEncode(nil, &valid) // warm up

		bad := Outer{Items: []*Inner{{P: nil}}}
		_, err = s.AppendEncode(nil, &bad)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

// TestArrayReusedSliceCap covers the else branch (cap >= newLen) in
// udArrayPtrRecord and udArrayDirect by decoding twice into the same
// variable, so the second decode reuses the existing slice capacity.
func TestArrayReusedSliceCap(t *testing.T) {
	t.Run("direct", func(t *testing.T) {
		type Wrapper struct {
			Vals []int32 `avro:"vals"`
		}
		schema := `{
			"type": "record",
			"name": "Wrapper",
			"fields": [{"name": "vals", "type": {"type": "array", "items": "int"}}]
		}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		big := Wrapper{Vals: []int32{1, 2, 3, 4, 5}}
		encBig, _ := s.AppendEncode(nil, &big)
		small := Wrapper{Vals: []int32{1, 2}}
		encSmall, _ := s.AppendEncode(nil, &small)
		// First decode: allocates slice cap=5.
		var out Wrapper
		s.Decode(encBig, &out)
		// Second decode: reuses cap=5 for 2 elements → else branch.
		_, err = s.Decode(encSmall, &out)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(out.Vals, []int32{1, 2}) {
			t.Errorf("got %v, want [1 2]", out.Vals)
		}
	})

	t.Run("ptr_record", func(t *testing.T) {
		type Rec struct {
			V int32 `avro:"v"`
		}
		type Wrapper struct {
			Items []*Rec `avro:"items"`
		}
		schema := `{
			"type": "record",
			"name": "Wrapper",
			"fields": [{"name": "items", "type": {"type": "array", "items": {
				"type": "record", "name": "Rec",
				"fields": [{"name": "v", "type": "int"}]
			}}}]
		}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		big := Wrapper{Items: []*Rec{{V: 1}, {V: 2}, {V: 3}}}
		encBig, _ := s.AppendEncode(nil, &big)
		small := Wrapper{Items: []*Rec{{V: 10}}}
		encSmall, _ := s.AppendEncode(nil, &small)
		// First decode: allocates.
		var out Wrapper
		s.Decode(encBig, &out)
		// Second decode: reuses capacity → else branch.
		_, err = s.Decode(encSmall, &out)
		if err != nil {
			t.Fatal(err)
		}
		if len(out.Items) != 1 || out.Items[0].V != 10 {
			t.Errorf("got %+v, want [{10}]", out.Items)
		}
	})
}

// TestArrayNegativeCountReadVarlongError covers the error path when a
// negative count block has a truncated byte-size field.
func TestArrayNegativeCountReadVarlongError(t *testing.T) {
	t.Run("direct", func(t *testing.T) {
		type Wrapper struct {
			Vals []int32 `avro:"vals"`
		}
		schema := `{
			"type": "record",
			"name": "Wrapper",
			"fields": [{"name": "vals", "type": {"type": "array", "items": "int"}}]
		}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		s.AppendEncode(nil, &Wrapper{Vals: []int32{1}}) // warm up

		// Negative count with truncated byte-size.
		var data []byte
		data = appendVarlong(data, -3) // negative count
		// No byte-size follows → readVarlong error.
		var out Wrapper
		_, err = s.Decode(data, &out)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("ptr_record", func(t *testing.T) {
		type Rec struct {
			V int32 `avro:"v"`
		}
		type Wrapper struct {
			Items []*Rec `avro:"items"`
		}
		schema := `{
			"type": "record",
			"name": "Wrapper",
			"fields": [{"name": "items", "type": {"type": "array", "items": {
				"type": "record", "name": "Rec",
				"fields": [{"name": "v", "type": "int"}]
			}}}]
		}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		normal := Wrapper{Items: []*Rec{{V: 1}}}
		enc, _ := s.AppendEncode(nil, &normal)
		var dummy Wrapper
		s.Decode(enc, &dummy) // warm up

		// Negative count with truncated byte-size.
		var data []byte
		data = appendVarlong(data, -3)
		var out Wrapper
		_, err = s.Decode(data, &out)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

// TestPtrToMapField covers tryCompileFieldSer returning nil for *map
// (ptr inner compile fails) and the reflect fallback handling it.
func TestPtrToMapField(t *testing.T) {
	type Wrapper struct {
		M *map[string]int32 `avro:"m"`
	}
	schema := `{
		"type": "record",
		"name": "Wrapper",
		"fields": [{"name": "m", "type": {"type": "map", "values": "int"}}]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]int32{"x": 10}
	input := Wrapper{M: &m}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out Wrapper
	if _, err := s.Decode(encoded, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.M == nil || (*out.M)["x"] != 10 {
		t.Errorf("got %v, want map with x=10", out.M)
	}
}

// TestRecordMappedToMap covers tryCompileFieldSer/tryCompileFieldDeser
// returning nil for a record avroType with a map Go type.
func TestRecordMappedToMap(t *testing.T) {
	type Outer struct {
		Inner map[string]any `avro:"inner"`
	}
	schema := `{
		"type": "record",
		"name": "Outer",
		"fields": [{"name": "inner", "type": {
			"type": "record", "name": "Inner",
			"fields": [{"name": "x", "type": "int"}]
		}}]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	input := Outer{Inner: map[string]any{"x": int32(42)}}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out Outer
	if _, err := s.Decode(encoded, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Inner["x"] != int32(42) {
		t.Errorf("got %v, want 42", out.Inner["x"])
	}
}

// TestTryCompileFieldSerDefensive covers defensive nil-meta guards
// in tryCompileFieldSer that protect against internal schema builder bugs.
func TestTryCompileFieldSerDefensive(t *testing.T) {
	// Null-union with nil meta → return nil.
	fn := tryCompileFieldSer(&serRecordField{avroType: "nullunion"}, reflect.TypeFor[*int]())
	if fn != nil {
		t.Error("expected nil for nullunion with nil meta")
	}
	// Null-union with nil inner meta → return nil.
	fn = tryCompileFieldSer(&serRecordField{avroType: "nullunion", meta: &fieldMeta{avroType: "nullunion"}}, reflect.TypeFor[*int]())
	if fn != nil {
		t.Error("expected nil for nullunion with nil inner meta")
	}
	// Array with nil meta → return nil.
	fn = tryCompileFieldSer(&serRecordField{avroType: "array"}, reflect.SliceOf(reflect.TypeFor[int32]()))
	if fn != nil {
		t.Error("expected nil for array with nil meta")
	}
	// Array nullunion with non-ptr element → return nil.
	fn = tryCompileFieldSer(&serRecordField{
		avroType: "array",
		meta:     &fieldMeta{avroType: "array", inner: &fieldMeta{avroType: "nullunion", inner: &fieldMeta{avroType: "int"}}},
	}, reflect.SliceOf(reflect.TypeFor[int32]()))
	if fn != nil {
		t.Error("expected nil for array nullunion with non-ptr element")
	}
	// Array nullunion with ptr to non-compilable inner (covers catch-all return nil).
	fn = tryCompileFieldSer(&serRecordField{
		avroType: "array",
		meta:     &fieldMeta{avroType: "array", inner: &fieldMeta{avroType: "nullunion", inner: &fieldMeta{avroType: "map"}}},
	}, reflect.SliceOf(reflect.TypeFor[*map[string]int]()))
	if fn != nil {
		t.Error("expected nil for array nullunion with non-compilable inner")
	}
	// Record with nil meta → return nil.
	fn = tryCompileFieldSer(&serRecordField{avroType: "record"}, reflect.TypeFor[struct{}]())
	if fn != nil {
		t.Error("expected nil for record with nil meta")
	}
}

// TestUsArraySerErrorPaths covers the error handling in usArrayNullUnionPtr
// and usArrayDirect by passing a synthetic error-returning userfn.
func TestUsArraySerErrorPaths(t *testing.T) {
	errFake := fmt.Errorf("fake")
	failFn := func(dst []byte, p unsafe.Pointer) ([]byte, error) {
		return nil, errFake
	}

	t.Run("null_union_ptr", func(t *testing.T) {
		fn := usArrayNullUnionPtr(failFn, 0, 2)
		s := []*int32{new(int32(1))}
		_, err := fn(nil, unsafe.Pointer(&s))
		if err != errFake {
			t.Errorf("expected fake error, got %v", err)
		}
	})

	t.Run("direct", func(t *testing.T) {
		fn := usArrayDirect(failFn, unsafe.Sizeof(int32(0)))
		s := []int32{1}
		_, err := fn(nil, unsafe.Pointer(&s))
		if err != errFake {
			t.Errorf("expected fake error, got %v", err)
		}
	})
}

// TestTryCompileFieldDeserDefensive covers defensive nil-meta guards
// in tryCompileFieldDeser.
func TestTryCompileFieldDeserDefensive(t *testing.T) {
	// Null-union with nil meta → return nil.
	fn := tryCompileFieldDeser(&deserRecordField{avroType: "nullunion"}, reflect.TypeFor[*int]())
	if fn != nil {
		t.Error("expected nil for nullunion with nil meta")
	}
	fn = tryCompileFieldDeser(&deserRecordField{avroType: "nullunion", meta: &fieldMeta{avroType: "nullunion"}}, reflect.TypeFor[*int]())
	if fn != nil {
		t.Error("expected nil for nullunion with nil inner meta")
	}
	// Array with nil meta → return nil.
	fn = tryCompileFieldDeser(&deserRecordField{avroType: "array"}, reflect.SliceOf(reflect.TypeFor[int32]()))
	if fn != nil {
		t.Error("expected nil for array with nil meta")
	}
	// Record with nil meta → return nil.
	fn = tryCompileFieldDeser(&deserRecordField{avroType: "record"}, reflect.TypeFor[struct{}]())
	if fn != nil {
		t.Error("expected nil for record with nil meta")
	}
}

// -----------------------------------------------------------------------
// Adversarial / Pathological Edge Cases
// -----------------------------------------------------------------------
//
// These tests feed crafted malicious inputs to the unsafe fast paths to
// verify that length lies, count overflows, truncated data, and other
// adversarial patterns are caught cleanly without memory corruption.

// TestAdversarialStringLengthLie exercises the unsafe string deserializer
// (udStringDeser) with wire data where the encoded length exceeds the
// available bytes in src.
func TestAdversarialStringLengthLie(t *testing.T) {
	type R struct {
		A string `avro:"a"`
		B int32  `avro:"b"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"a","type":"string"},
		{"name":"b","type":"int"}
	]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		data []byte
	}{
		// String length claims more bytes than available.
		{"exceeds_src", func() []byte {
			var d []byte
			d = appendVarlong(d, 1000)
			d = append(d, 'h', 'i')
			return d
		}()},
		// String length exactly consumes remaining src, leaving no
		// bytes for field b.
		{"consumes_all", func() []byte {
			var d []byte
			d = appendVarlong(d, 3)
			d = append(d, 'a', 'b', 'c')
			return d
		}()},
		// Negative length.
		{"negative", []byte{0x01}}, // zigzag 1 → -1
		// Truncated varint (continuation bit set, no terminator).
		{"truncated_varint", []byte{0x80}},
		// Empty src.
		{"empty", []byte{}},
		// Off-by-one: length claims 4 but only 3 bytes follow.
		{"off_by_one", func() []byte {
			var d []byte
			d = appendVarlong(d, 4)
			d = append(d, 'a', 'b', 'c')
			return d
		}()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out R
			_, err := s.Decode(tc.data, &out)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

// TestAdversarialBytesLengthLie exercises the unsafe bytes deserializer
// (udBytesDeser) with lying lengths.
func TestAdversarialBytesLengthLie(t *testing.T) {
	type R struct {
		A []byte `avro:"a"`
		B int32  `avro:"b"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"a","type":"bytes"},
		{"name":"b","type":"int"}
	]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		data []byte
	}{
		{"exceeds_src", func() []byte {
			var d []byte
			d = appendVarlong(d, 999)
			d = append(d, 0x01, 0x02)
			return d
		}()},
		{"negative", []byte{0x01}}, // zigzag → -1
		{"off_by_one", func() []byte {
			var d []byte
			d = appendVarlong(d, 3)
			d = append(d, 0x01, 0x02) // only 2
			return d
		}()},
		{"consumes_all", func() []byte {
			var d []byte
			d = appendVarlong(d, 2)
			d = append(d, 0x01, 0x02)
			return d
		}()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out R
			_, err := s.Decode(tc.data, &out)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

// TestAdversarialArrayCountLie exercises the unsafe array deserializers
// (udArrayDirect and udArrayPtrRecord) with block counts that lie about
// the number of items available in the data.
func TestAdversarialArrayCountLie(t *testing.T) {
	t.Run("direct_int", func(t *testing.T) {
		type R struct {
			Vals []int32 `avro:"vals"`
		}
		schema := `{"type":"record","name":"R","fields":[
			{"name":"vals","type":{"type":"array","items":"int"}}
		]}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		tests := []struct {
			name string
			data []byte
		}{
			// Count says 10, only data for 1 item.
			{"count_exceeds_data", func() []byte {
				var d []byte
				d = appendVarlong(d, 10)
				d = appendVarint(d, 42)
				return d
			}()},
			// Count says 1, but the item's varint is truncated.
			{"item_varint_truncated", func() []byte {
				var d []byte
				d = appendVarlong(d, 1)
				d = append(d, 0x80) // truncated varint
				return d
			}()},
			// Two blocks: first valid, second lies.
			{"second_block_lies", func() []byte {
				var d []byte
				d = appendVarlong(d, 1)  // block 1: 1 item
				d = appendVarint(d, 42)  // the item
				d = appendVarlong(d, 50) // block 2: 50 items
				d = appendVarint(d, 1)   // only 1 item
				return d
			}()},
			// Negative block count with missing byte-size.
			{"negative_count_truncated", func() []byte {
				var d []byte
				d = appendVarlong(d, -5)
				return d
			}()},
			// Missing terminating zero block.
			{"no_terminator", func() []byte {
				var d []byte
				d = appendVarlong(d, 1)
				d = appendVarint(d, 42)
				return d
			}()},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				var out R
				_, err := s.Decode(tc.data, &out)
				if err == nil {
					t.Fatal("expected error")
				}
			})
		}
	})

	t.Run("ptr_record", func(t *testing.T) {
		type Inner struct {
			X int32 `avro:"x"`
		}
		type R struct {
			Items []*Inner `avro:"items"`
		}
		schema := `{"type":"record","name":"R","fields":[
			{"name":"items","type":{"type":"array","items":{
				"type":"record","name":"Inner",
				"fields":[{"name":"x","type":"int"}]
			}}}
		]}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		tests := []struct {
			name string
			data []byte
		}{
			// Count says 5, only data for 1 record.
			{"count_exceeds_data", func() []byte {
				var d []byte
				d = appendVarlong(d, 5)
				d = appendVarint(d, 42) // only 1 record's x field
				return d
			}()},
			// Count says 1, record field data truncated.
			{"record_truncated", func() []byte {
				var d []byte
				d = appendVarlong(d, 1)
				d = append(d, 0x80) // truncated varint for x
				return d
			}()},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				var out R
				_, err := s.Decode(tc.data, &out)
				if err == nil {
					t.Fatal("expected error")
				}
			})
		}
	})
}

// TestAdversarialMinInt64BlockCount tests that a crafted block count of
// math.MinInt64 is rejected. -MinInt64 overflows to MinInt64 in two's
// complement, so without explicit checking the count stays negative and
// causes a panic in SetLen.
func TestAdversarialMinInt64BlockCount(t *testing.T) {
	t.Run("array_reflect", func(t *testing.T) {
		schema := `{"type":"array","items":"int"}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		data := appendVarlong(nil, math.MinInt64)
		var out []int32
		_, err = s.Decode(data, &out)
		if err == nil {
			t.Fatal("expected error for MinInt64 block count")
		}
	})

	t.Run("array_unsafe_direct", func(t *testing.T) {
		type R struct {
			Vals []int32 `avro:"vals"`
		}
		schema := `{"type":"record","name":"R","fields":[
			{"name":"vals","type":{"type":"array","items":"int"}}
		]}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		data := appendVarlong(nil, math.MinInt64)
		var out R
		_, err = s.Decode(data, &out)
		if err == nil {
			t.Fatal("expected error for MinInt64 block count")
		}
	})

	t.Run("array_unsafe_ptr_record", func(t *testing.T) {
		type Inner struct {
			X int32 `avro:"x"`
		}
		type R struct {
			Items []*Inner `avro:"items"`
		}
		schema := `{"type":"record","name":"R","fields":[
			{"name":"items","type":{"type":"array","items":{
				"type":"record","name":"Inner",
				"fields":[{"name":"x","type":"int"}]
			}}}
		]}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		data := appendVarlong(nil, math.MinInt64)
		var out R
		_, err = s.Decode(data, &out)
		if err == nil {
			t.Fatal("expected error for MinInt64 block count")
		}
	})

	t.Run("map_reflect", func(t *testing.T) {
		schema := `{"type":"map","values":"int"}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		data := appendVarlong(nil, math.MinInt64)
		var out map[string]int32
		_, err = s.Decode(data, &out)
		if err == nil {
			t.Fatal("expected error for MinInt64 block count")
		}
	})
}

// TestAdversarialNullUnionBadIndex tests null-union deserialization
// through the unsafe path with invalid index bytes.
func TestAdversarialNullUnionBadIndex(t *testing.T) {
	t.Run("primitive", func(t *testing.T) {
		type R struct {
			V *int32 `avro:"v"`
		}
		schema := `{"type":"record","name":"R","fields":[
			{"name":"v","type":["null","int"]}
		]}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		for _, idx := range []byte{1, 3, 4, 0x80, 0xFE, 0xFF} {
			t.Run(fmt.Sprintf("0x%02x", idx), func(t *testing.T) {
				var out R
				_, err := s.Decode([]byte{idx}, &out)
				if err == nil {
					t.Fatalf("expected error for null-union index 0x%02x", idx)
				}
			})
		}
		t.Run("empty_src", func(t *testing.T) {
			var out R
			_, err := s.Decode([]byte{}, &out)
			if err == nil {
				t.Fatal("expected error for empty null-union input")
			}
		})
		t.Run("branch1_truncated", func(t *testing.T) {
			var out R
			_, err := s.Decode([]byte{0x02}, &out) // branch=1, no int data
			if err == nil {
				t.Fatal("expected error for truncated null-union value")
			}
		})
	})

	t.Run("record", func(t *testing.T) {
		type Inner struct {
			X int32 `avro:"x"`
		}
		type R struct {
			V *Inner `avro:"v"`
		}
		schema := `{"type":"record","name":"R","fields":[
			{"name":"v","type":["null",{
				"type":"record","name":"Inner",
				"fields":[{"name":"x","type":"int"}]
			}]}
		]}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		t.Run("bad_index", func(t *testing.T) {
			var out R
			_, err := s.Decode([]byte{0x04}, &out) // zigzag 4→2, invalid
			if err == nil {
				t.Fatal("expected error")
			}
		})
		t.Run("branch1_truncated", func(t *testing.T) {
			var out R
			_, err := s.Decode([]byte{0x02}, &out) // branch=1, no record data
			if err == nil {
				t.Fatal("expected error")
			}
		})
	})
}

// TestAdversarialTruncationSweep encodes a valid multi-field record and then
// decodes every possible truncation of the encoded bytes. Every prefix
// shorter than the full encoding must produce an error, not a panic.
func TestAdversarialTruncationSweep(t *testing.T) {
	type R struct {
		A int32   `avro:"a"`
		B string  `avro:"b"`
		C float32 `avro:"c"`
		D int64   `avro:"d"`
		E []byte  `avro:"e"`
		F bool    `avro:"f"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"},
		{"name":"c","type":"float"},
		{"name":"d","type":"long"},
		{"name":"e","type":"bytes"},
		{"name":"f","type":"boolean"}
	]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	valid := R{A: 42, B: "hello", C: 3.14, D: 999, E: []byte{0xDE, 0xAD}, F: true}
	full, _ := s.AppendEncode(nil, &valid)

	for i := range full {
		var out R
		_, err := s.Decode(full[:i], &out)
		if err == nil {
			t.Fatalf("expected error at truncation point %d/%d", i, len(full))
		}
	}
}

// TestAdversarialNestedRecordTruncation tests truncation within nested
// records through the unsafe fast path.
func TestAdversarialNestedRecordTruncation(t *testing.T) {
	type Inner struct {
		X int32  `avro:"x"`
		Y string `avro:"y"`
	}
	type Outer struct {
		A int32 `avro:"a"`
		B Inner `avro:"b"`
		C int32 `avro:"c"`
	}
	schema := `{"type":"record","name":"Outer","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":{"type":"record","name":"Inner","fields":[
			{"name":"x","type":"int"},
			{"name":"y","type":"string"}
		]}},
		{"name":"c","type":"int"}
	]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	valid := Outer{A: 1, B: Inner{X: 2, Y: "nested"}, C: 3}
	full, _ := s.AppendEncode(nil, &valid)

	for i := range full {
		var out Outer
		_, err := s.Decode(full[:i], &out)
		if err == nil {
			t.Fatalf("expected error at truncation point %d/%d", i, len(full))
		}
	}
}

// TestAdversarialNoAliasing verifies that decoded strings and byte slices
// do not alias the input buffer. Mutating the input after decode must not
// affect the decoded values.
func TestAdversarialNoAliasing(t *testing.T) {
	type R struct {
		S string `avro:"s"`
		B []byte `avro:"b"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"s","type":"string"},
		{"name":"b","type":"bytes"}
	]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	input := R{S: "hello", B: []byte{1, 2, 3, 4, 5}}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatal(err)
	}

	var out R
	_, err = s.Decode(encoded, &out)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt the encoded buffer.
	for i := range encoded {
		encoded[i] = 0xFF
	}
	// Decoded values must be unaffected.
	if out.S != "hello" {
		t.Errorf("string aliased input buffer: got %q", out.S)
	}
	if !bytes.Equal(out.B, []byte{1, 2, 3, 4, 5}) {
		t.Errorf("bytes aliased input buffer: got %x", out.B)
	}
}

// TestAdversarialRedecodeOverwrite verifies that decoding into a struct
// that already contains values correctly overwrites all fields through
// the unsafe fast path.
func TestAdversarialRedecodeOverwrite(t *testing.T) {
	type R struct {
		A int32  `avro:"a"`
		B string `avro:"b"`
		C bool   `avro:"c"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"},
		{"name":"c","type":"boolean"}
	]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	first := R{A: 100, B: "first", C: true}
	enc1, _ := s.AppendEncode(nil, &first)
	second := R{A: 200, B: "second", C: false}
	enc2, _ := s.AppendEncode(nil, &second)

	var out R
	if _, err = s.Decode(enc1, &out); err != nil {
		t.Fatal(err)
	}
	if out.A != 100 || out.B != "first" || out.C != true {
		t.Fatalf("first decode wrong: %+v", out)
	}
	// Re-decode second value over the same struct.
	if _, err = s.Decode(enc2, &out); err != nil {
		t.Fatal(err)
	}
	if out.A != 200 || out.B != "second" || out.C != false {
		t.Fatalf("redecode wrong: %+v", out)
	}
}

// TestAdversarialNullUnionRedecode verifies that null-union fields are
// correctly zeroed when decoding null over a previously non-null value.
func TestAdversarialNullUnionRedecode(t *testing.T) {
	type R struct {
		V *int32 `avro:"v"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"v","type":["null","int"]}
	]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}

	val := int32(42)
	enc1, _ := s.AppendEncode(nil, &R{V: &val})
	enc2, _ := s.AppendEncode(nil, &R{V: nil})

	var out R
	if _, err = s.Decode(enc1, &out); err != nil {
		t.Fatal(err)
	}
	if out.V == nil || *out.V != 42 {
		t.Fatalf("first decode: got %v", out.V)
	}
	// Decode null over the non-null value.
	if _, err = s.Decode(enc2, &out); err != nil {
		t.Fatal(err)
	}
	if out.V != nil {
		t.Fatalf("expected nil after null decode, got %d", *out.V)
	}
}

// TestAdversarialVarintBoundary tests varint/varlong extreme values and
// overflow through the unsafe fast path.
func TestAdversarialVarintBoundary(t *testing.T) {
	t.Run("int32_extremes", func(t *testing.T) {
		for _, v := range []int32{math.MaxInt32, math.MinInt32, 0, 1, -1} {
			got := roundTrip(t, `"int"`, v)
			if got != v {
				t.Errorf("roundTrip(%d) = %d", v, got)
			}
		}
	})
	t.Run("int64_extremes", func(t *testing.T) {
		for _, v := range []int64{math.MaxInt64, math.MinInt64, 0, 1, -1} {
			got := roundTrip(t, `"long"`, v)
			if got != v {
				t.Errorf("roundTrip(%d) = %d", v, got)
			}
		}
	})
	t.Run("varint_overflow", func(t *testing.T) {
		// 5-byte varint with all continuation bits set: overflows 32 bits.
		data := []byte{0x80, 0x80, 0x80, 0x80, 0x80}
		decodeErr(t, `"int"`, data, new(int32(0)))
	})
	t.Run("varlong_overflow", func(t *testing.T) {
		// 10-byte varlong with all continuation bits set: overflows 64 bits.
		data := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}
		decodeErr(t, `"long"`, data, new(int64(0)))
	})
	t.Run("non_terminating_varint", func(t *testing.T) {
		// 4 bytes all with continuation bit, then EOF.
		data := []byte{0xFF, 0xFF, 0xFF, 0xFF}
		decodeErr(t, `"int"`, data, new(int32(0)))
	})
}

// TestAdversarialMapKeyLengthLie exercises map deserialization with
// adversarial key lengths.
func TestAdversarialMapKeyLengthLie(t *testing.T) {
	schema := `{"type":"map","values":"int"}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		data []byte
	}{
		// Key length exceeds remaining src.
		{"key_exceeds_src", func() []byte {
			var d []byte
			d = appendVarlong(d, 1)    // 1 entry
			d = appendVarlong(d, 1000) // key length 1000
			d = append(d, 'k')         // only 1 byte
			return d
		}()},
		// Negative key length.
		{"negative_key", func() []byte {
			var d []byte
			d = appendVarlong(d, 1)  // 1 entry
			d = appendVarlong(d, -1) // negative key length
			return d
		}()},
		// Key consumes all remaining, no value data.
		{"key_consumes_all", func() []byte {
			var d []byte
			d = appendVarlong(d, 1)      // 1 entry
			d = appendVarlong(d, 3)      // key length 3
			d = append(d, 'k', 'e', 'y') // 3 bytes, nothing for value
			return d
		}()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out map[string]int32
			_, err := s.Decode(tc.data, &out)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

// TestAdversarialAlignmentStress verifies the unsafe fast path handles
// struct fields at various alignment boundaries correctly by round-tripping
// a struct with fields of every primitive type at different offsets.
func TestAdversarialAlignmentStress(t *testing.T) {
	type Packed struct {
		A bool    `avro:"a"` // offset 0, align 1
		B int8    `avro:"b"` // offset 1, align 1
		C int16   `avro:"c"` // offset 2, align 2
		D int32   `avro:"d"` // offset 4, align 4
		E int64   `avro:"e"` // offset 8, align 8
		F float32 `avro:"f"` // offset 16, align 4
		G float64 `avro:"g"` // offset 24, align 8 (padding after F)
		H string  `avro:"h"` // offset 32, align 8
		I []byte  `avro:"i"` // offset 48, align 8
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"a","type":"boolean"},
		{"name":"b","type":"int"},
		{"name":"c","type":"int"},
		{"name":"d","type":"int"},
		{"name":"e","type":"long"},
		{"name":"f","type":"float"},
		{"name":"g","type":"double"},
		{"name":"h","type":"string"},
		{"name":"i","type":"bytes"}
	]}`
	input := Packed{
		A: true,
		B: 42,
		C: 1000,
		D: 100000,
		E: 1 << 50,
		F: 3.14,
		G: 2.718281828,
		H: "alignment test",
		I: []byte{0xDE, 0xAD, 0xBE, 0xEF},
	}
	got := roundTrip(t, schema, input)
	if got.A != input.A {
		t.Errorf("A: got %v, want %v", got.A, input.A)
	}
	if got.B != input.B {
		t.Errorf("B: got %v, want %v", got.B, input.B)
	}
	if got.C != input.C {
		t.Errorf("C: got %v, want %v", got.C, input.C)
	}
	if got.D != input.D {
		t.Errorf("D: got %v, want %v", got.D, input.D)
	}
	if got.E != input.E {
		t.Errorf("E: got %v, want %v", got.E, input.E)
	}
	if got.F != input.F {
		t.Errorf("F: got %v, want %v", got.F, input.F)
	}
	if got.G != input.G {
		t.Errorf("G: got %v, want %v", got.G, input.G)
	}
	if got.H != input.H {
		t.Errorf("H: got %q, want %q", got.H, input.H)
	}
	if !bytes.Equal(got.I, input.I) {
		t.Errorf("I: got %x, want %x", got.I, input.I)
	}
}

// TestAdversarialFieldOrderMismatch tests that the unsafe fast path handles
// structs where Go field order differs from schema field order.
func TestAdversarialFieldOrderMismatch(t *testing.T) {
	type R struct {
		Z int64   `avro:"z"`
		A bool    `avro:"a"`
		B int32   `avro:"b"`
		C float64 `avro:"c"`
	}
	// Schema field order: a, b, c, z (different from Go struct order).
	schema := `{"type":"record","name":"R","fields":[
		{"name":"a","type":"boolean"},
		{"name":"b","type":"int"},
		{"name":"c","type":"double"},
		{"name":"z","type":"long"}
	]}`
	input := R{Z: 9999, A: true, B: 42, C: 2.718}
	got := roundTrip(t, schema, input)
	if got != input {
		t.Errorf("got %+v, want %+v", got, input)
	}
}

// TestAdversarialEmbeddedStructOffset tests that computeFieldOffset
// correctly handles embedded (anonymous) struct fields by summing offsets
// along the index path.
func TestAdversarialEmbeddedStructOffset(t *testing.T) {
	type Base struct {
		A int32  `avro:"a"`
		B string `avro:"b"`
	}
	type Outer struct {
		Base
		C int64 `avro:"c"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"},
		{"name":"c","type":"long"}
	]}`
	input := Outer{Base: Base{A: 42, B: "embedded"}, C: 999}
	got := roundTrip(t, schema, input)
	if got.A != input.A || got.B != input.B || got.C != input.C {
		t.Errorf("got %+v, want %+v", got, input)
	}
}

// TestAdversarialZeroLengthValues tests zero-length strings, bytes, and
// empty arrays/maps through the unsafe fast path.
func TestAdversarialZeroLengthValues(t *testing.T) {
	type R struct {
		S string `avro:"s"`
		B []byte `avro:"b"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"s","type":"string"},
		{"name":"b","type":"bytes"}
	]}`
	input := R{S: "", B: []byte{}}
	got := roundTrip(t, schema, input)
	if got.S != "" {
		t.Errorf("string: got %q, want empty", got.S)
	}
	if len(got.B) != 0 {
		t.Errorf("bytes: got %v, want empty", got.B)
	}
}

// TestAdversarialArrayNullUnionLie tests arrays of nullable records with
// adversarial inputs through the unsafe fast path.
func TestAdversarialArrayNullUnionLie(t *testing.T) {
	type Inner struct {
		V int32 `avro:"v"`
	}
	type R struct {
		Items []*Inner `avro:"items"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"items","type":{"type":"array","items":["null",{
			"type":"record","name":"Inner",
			"fields":[{"name":"v","type":"int"}]
		}]}}
	]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		data []byte
	}{
		// Array with 1 item, null-union index is invalid.
		{"bad_union_index", func() []byte {
			var d []byte
			d = appendVarlong(d, 1)
			d = append(d, 0x04) // invalid index byte
			return d
		}()},
		// Array with 1 item, null-union says non-null but record truncated.
		{"truncated_record", func() []byte {
			var d []byte
			d = appendVarlong(d, 1)
			d = append(d, 0x02) // non-null
			d = append(d, 0x80) // truncated varint for v
			return d
		}()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out R
			_, err := s.Decode(tc.data, &out)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

// ---- TextUnmarshaler tests ----

type testTextUnmarshaler struct{ val string }

func (tu *testTextUnmarshaler) UnmarshalText(text []byte) error {
	tu.val = "unmarshaled:" + string(text)
	return nil
}

var _ encoding.TextUnmarshaler = (*testTextUnmarshaler)(nil)

func TestDeserStringTextUnmarshaler(t *testing.T) {
	s, err := Parse(`"string"`)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := s.AppendEncode(nil, new("hello"))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var v testTextUnmarshaler
	_, err = s.Decode(encoded, &v)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if v.val != "unmarshaled:hello" {
		t.Fatalf("got %q, want %q", v.val, "unmarshaled:hello")
	}
}

func TestTextUnmarshalerRoundTrip(t *testing.T) {
	type R struct {
		Name textMarshalerType `avro:"name"`
	}
	type RD struct {
		Name testTextUnmarshaler `avro:"name"`
	}

	schema := `{"type":"record","name":"r","fields":[{"name":"name","type":"string"}]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	input := R{Name: textMarshalerType{val: "world"}}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var output RD
	_, err = s.Decode(encoded, &output)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if output.Name.val != "unmarshaled:world" {
		t.Fatalf("got %q, want %q", output.Name.val, "unmarshaled:world")
	}
}

// ---- time.Time logical type tests ----

func TestTimestampMillisRoundTrip(t *testing.T) {
	schema := `{"type":"long","logicalType":"timestamp-millis"}`
	now := time.UnixMilli(time.Now().UnixMilli()) // truncate to millis
	got := roundTrip(t, schema, now)
	if !got.Equal(now) {
		t.Fatalf("timestamp-millis round trip: got %v, want %v", got, now)
	}
}

func TestTimestampMicrosRoundTrip(t *testing.T) {
	schema := `{"type":"long","logicalType":"timestamp-micros"}`
	now := time.UnixMicro(time.Now().UnixMicro()) // truncate to micros
	got := roundTrip(t, schema, now)
	if !got.Equal(now) {
		t.Fatalf("timestamp-micros round trip: got %v, want %v", got, now)
	}
}

func TestDateRoundTrip(t *testing.T) {
	schema := `{"type":"int","logicalType":"date"}`
	// Use a date at midnight UTC.
	input := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	got := roundTrip(t, schema, input)
	if !got.Equal(input) {
		t.Fatalf("date round trip: got %v, want %v", got, input)
	}
}

func TestTimeMillisRoundTrip(t *testing.T) {
	schema := `{"type":"int","logicalType":"time-millis"}`
	input := 45*time.Second + 123*time.Millisecond
	got := roundTrip(t, schema, input)
	if got != input {
		t.Fatalf("time-millis round trip: got %v, want %v", got, input)
	}
}

func TestTimeMicrosRoundTrip(t *testing.T) {
	schema := `{"type":"long","logicalType":"time-micros"}`
	input := 2*time.Minute + 500*time.Microsecond
	got := roundTrip(t, schema, input)
	if got != input {
		t.Fatalf("time-micros round trip: got %v, want %v", got, input)
	}
}

func TestLocalTimestampMillisRoundTrip(t *testing.T) {
	schema := `{"type":"long","logicalType":"local-timestamp-millis"}`
	now := time.UnixMilli(time.Now().UnixMilli())
	got := roundTrip(t, schema, now)
	if !got.Equal(now) {
		t.Fatalf("local-timestamp-millis round trip: got %v, want %v", got, now)
	}
}

func TestLocalTimestampMicrosRoundTrip(t *testing.T) {
	schema := `{"type":"long","logicalType":"local-timestamp-micros"}`
	now := time.UnixMicro(time.Now().UnixMicro())
	got := roundTrip(t, schema, now)
	if !got.Equal(now) {
		t.Fatalf("local-timestamp-micros round trip: got %v, want %v", got, now)
	}
}

func TestTimestampMillisFallbackToInt64(t *testing.T) {
	// When the Go type is int64, the logical type should fall back to plain long.
	schema := `{"type":"long","logicalType":"timestamp-millis"}`
	input := int64(1718400000000)
	got := roundTrip(t, schema, input)
	if got != input {
		t.Fatalf("timestamp-millis int64 fallback: got %d, want %d", got, input)
	}
}

func TestTimestampMillisInRecord(t *testing.T) {
	type R struct {
		Created time.Time `avro:"created"`
	}
	schema := `{"type":"record","name":"r","fields":[
		{"name":"created","type":{"type":"long","logicalType":"timestamp-millis"}}
	]}`
	now := time.UnixMilli(time.Now().UnixMilli())
	got := roundTrip(t, schema, R{Created: now})
	if !got.Created.Equal(now) {
		t.Fatalf("timestamp in record: got %v, want %v", got.Created, now)
	}
}

func TestDateInRecord(t *testing.T) {
	type R struct {
		Birthday time.Time `avro:"birthday"`
	}
	schema := `{"type":"record","name":"r","fields":[
		{"name":"birthday","type":{"type":"int","logicalType":"date"}}
	]}`
	input := time.Date(1990, 3, 25, 0, 0, 0, 0, time.UTC)
	got := roundTrip(t, schema, R{Birthday: input})
	if !got.Birthday.Equal(input) {
		t.Fatalf("date in record: got %v, want %v", got.Birthday, input)
	}
}

func TestTimeMillisInRecord(t *testing.T) {
	type R struct {
		Elapsed time.Duration `avro:"elapsed"`
	}
	schema := `{"type":"record","name":"r","fields":[
		{"name":"elapsed","type":{"type":"int","logicalType":"time-millis"}}
	]}`
	input := 5 * time.Second
	got := roundTrip(t, schema, R{Elapsed: input})
	if got.Elapsed != input {
		t.Fatalf("time-millis in record: got %v, want %v", got.Elapsed, input)
	}
}

// ---- Encode convenience method tests ----

func TestEncode(t *testing.T) {
	s, err := Parse(`"string"`)
	if err != nil {
		t.Fatal(err)
	}
	data, err := s.Encode(new("hello"))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var got string
	_, err = s.Decode(data, &got)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != "hello" {
		t.Fatalf("got %q, want %q", got, "hello")
	}
}

// ---- SemanticError / ShortBufferError tests ----

func TestSemanticErrorFormat(t *testing.T) {
	tests := []struct {
		name string
		err  *SemanticError
		want string
	}{
		{
			"full",
			&SemanticError{GoType: reflect.TypeFor[int](), AvroType: "string", Field: "name", Err: fmt.Errorf("oops")},
			"avro: field name: cannot use Go type int with Avro type string: oops",
		},
		{
			"no field",
			&SemanticError{GoType: reflect.TypeFor[string](), AvroType: "int"},
			"avro: cannot use Go type string with Avro type int",
		},
		{
			"go type only",
			&SemanticError{GoType: reflect.TypeFor[bool]()},
			"avro: unsupported Go type bool",
		},
		{
			"avro type only",
			&SemanticError{AvroType: "map"},
			"avro: unsupported Avro type map",
		},
		{
			"bare",
			&SemanticError{},
			"avro: semantic error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSemanticErrorUnwrap(t *testing.T) {
	inner := fmt.Errorf("inner")
	err := &SemanticError{Err: inner}
	if !errors.Is(err, inner) {
		t.Fatal("Unwrap failed")
	}
}

func TestSemanticErrorAs(t *testing.T) {
	inner := fmt.Errorf("boom")
	err := &SemanticError{GoType: reflect.TypeFor[int](), AvroType: "string", Err: inner}
	var se *SemanticError
	if !errors.As(err, &se) {
		t.Fatal("errors.As failed")
	}
	if se.GoType != reflect.TypeFor[int]() {
		t.Fatalf("GoType mismatch: %v", se.GoType)
	}
}

func TestShortBufferErrorFormat(t *testing.T) {
	err := &ShortBufferError{Type: "string", Need: 10, Have: 3}
	if err.Error() != "avro: short buffer for string: need 10, have 3" {
		t.Fatalf("got %q", err.Error())
	}
	err2 := &ShortBufferError{Type: "boolean"}
	if err2.Error() != "avro: short buffer for boolean" {
		t.Fatalf("got %q", err2.Error())
	}
}

func TestShortBufferErrorAs(t *testing.T) {
	var err error = &ShortBufferError{Type: "int", Need: 4, Have: 1}
	var sbe *ShortBufferError
	if !errors.As(err, &sbe) {
		t.Fatal("errors.As failed for ShortBufferError")
	}
	if sbe.Type != "int" || sbe.Need != 4 || sbe.Have != 1 {
		t.Fatalf("wrong values: %+v", sbe)
	}
}

func TestSemanticErrorIntegration(t *testing.T) {
	s, err := Parse(`"boolean"`)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := s.Encode(true)
	if err != nil {
		t.Fatal(err)
	}
	var n int
	_, err = s.Decode(encoded, &n)
	if err == nil {
		t.Fatal("expected error")
	}
	var se *SemanticError
	if !errors.As(err, &se) {
		t.Fatalf("expected *SemanticError, got %T: %v", err, err)
	}
	if se.AvroType != "boolean" {
		t.Fatalf("expected AvroType boolean, got %s", se.AvroType)
	}
}

func TestShortBufferErrorIntegration(t *testing.T) {
	s, err := Parse(`"boolean"`)
	if err != nil {
		t.Fatal(err)
	}
	var b bool
	_, err = s.Decode(nil, &b)
	if err == nil {
		t.Fatal("expected error")
	}
	var sbe *ShortBufferError
	if !errors.As(err, &sbe) {
		t.Fatalf("expected *ShortBufferError, got %T: %v", err, err)
	}
	if sbe.Type != "boolean" {
		t.Fatalf("expected Type boolean, got %s", sbe.Type)
	}
}

// ---- omitzero tests ----

func TestOmitzero(t *testing.T) {
	type R struct {
		Name *string `avro:"name,omitzero"`
	}
	schema := `{"type":"record","name":"r","fields":[
		{"name":"name","type":["null","string"]}
	]}`

	t.Run("nil pointer", func(t *testing.T) {
		got := roundTrip(t, schema, R{Name: nil})
		if got.Name != nil {
			t.Fatalf("expected nil, got %v", *got.Name)
		}
	})

	t.Run("non-nil pointer", func(t *testing.T) {
		s := "hello"
		got := roundTrip(t, schema, R{Name: &s})
		if got.Name == nil || *got.Name != "hello" {
			t.Fatalf("expected hello, got %v", got.Name)
		}
	})
}

func TestOmitzeroStringValue(t *testing.T) {
	// Test omitzero with a non-pointer string field in a null union.
	type R struct {
		Name string `avro:"name,omitzero"`
	}
	schema := `{"type":"record","name":"r","fields":[
		{"name":"name","type":["null","string"]}
	]}`

	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}

	// Empty string should be serialized as null.
	encoded, err := s.AppendEncode(nil, &R{Name: ""})
	if err != nil {
		t.Fatalf("encode empty: %v", err)
	}
	if len(encoded) != 1 || encoded[0] != 0 {
		t.Fatalf("expected null encoding [0x00], got %x", encoded)
	}

	// Non-empty string should be serialized normally.
	encoded, err = s.AppendEncode(nil, &R{Name: "hi"})
	if err != nil {
		t.Fatalf("encode non-empty: %v", err)
	}
	if encoded[0] != 2 {
		t.Fatalf("expected non-null union index, got %x", encoded[0])
	}
}

func TestOmitzeroWithIsZero(t *testing.T) {
	type R struct {
		When time.Time `avro:"when,omitzero"`
	}
	schema := `{"type":"record","name":"r","fields":[
		{"name":"when","type":["null",{"type":"long","logicalType":"timestamp-millis"}]}
	]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}

	// Zero time should be serialized as null.
	encoded, err := s.AppendEncode(nil, &R{When: time.Time{}})
	if err != nil {
		t.Fatalf("encode zero: %v", err)
	}
	if len(encoded) != 1 || encoded[0] != 0 {
		t.Fatalf("expected null encoding, got %x", encoded)
	}
}

// ---- inline tag tests ----

func TestInlineTag(t *testing.T) {
	type Inner struct {
		A int32  `avro:"a"`
		B string `avro:"b"`
	}
	type Outer struct {
		Inner Inner `avro:",inline"`
	}
	schema := `{"type":"record","name":"r","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}
	]}`

	got := roundTrip(t, schema, Outer{Inner: Inner{A: 42, B: "hello"}})
	if got.Inner.A != 42 || got.Inner.B != "hello" {
		t.Fatalf("inline round trip: got %+v", got)
	}
}

func TestInlineTagPointer(t *testing.T) {
	type Inner struct {
		X int64 `avro:"x"`
	}
	type Outer struct {
		Inner *Inner `avro:",inline"`
	}
	schema := `{"type":"record","name":"r","fields":[
		{"name":"x","type":"long"}
	]}`

	got := roundTrip(t, schema, Outer{Inner: &Inner{X: 99}})
	if got.Inner == nil || got.Inner.X != 99 {
		t.Fatalf("inline pointer round trip: got %+v", got)
	}
}

// ---- Schema defaults tests ----

func TestSchemaDefaultsValid(t *testing.T) {
	tests := []struct {
		name   string
		schema string
	}{
		{"string default", `{"type":"record","name":"r","fields":[
			{"name":"a","type":"string","default":"hello"}
		]}`},
		{"int default", `{"type":"record","name":"r","fields":[
			{"name":"a","type":"int","default":42}
		]}`},
		{"null union default", `{"type":"record","name":"r","fields":[
			{"name":"a","type":["null","string"],"default":null}
		]}`},
		{"boolean default", `{"type":"record","name":"r","fields":[
			{"name":"a","type":"boolean","default":true}
		]}`},
		{"array default", `{"type":"record","name":"r","fields":[
			{"name":"a","type":{"type":"array","items":"int"},"default":[]}
		]}`},
		{"map default", `{"type":"record","name":"r","fields":[
			{"name":"a","type":{"type":"map","values":"string"},"default":{}}
		]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.schema)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
		})
	}
}

func TestSchemaDefaultsInvalid(t *testing.T) {
	tests := []struct {
		name   string
		schema string
	}{
		{"string field with int default", `{"type":"record","name":"r","fields":[
			{"name":"a","type":"string","default":42}
		]}`},
		{"int field with string default", `{"type":"record","name":"r","fields":[
			{"name":"a","type":"int","default":"hello"}
		]}`},
		{"boolean field with string default", `{"type":"record","name":"r","fields":[
			{"name":"a","type":"boolean","default":"true"}
		]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.schema)
			if err == nil {
				t.Fatal("expected error for invalid default")
			}
		})
	}
}

// ---- TextUnmarshaler error path ----

type testTextUnmarshalerErr struct{}

func (*testTextUnmarshalerErr) UnmarshalText([]byte) error { return fmt.Errorf("unmarshal error") }

func TestDeserStringTextUnmarshalerError(t *testing.T) {
	s, err := Parse(`"string"`)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := s.AppendEncode(nil, new("hello"))
	if err != nil {
		t.Fatal(err)
	}
	var v testTextUnmarshalerErr
	_, err = s.Decode(encoded, &v)
	if err == nil {
		t.Fatal("expected error from UnmarshalText")
	}
}

// ---- Logical type deser into interface{} ----

func TestLogicalTypeDeserIntoInterface(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		encode any
	}{
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, new(int64(1000))},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`, new(int64(1000))},
		{"date", `{"type":"int","logicalType":"date"}`, new(int32(100))},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`, new(int32(5000))},
		{"time-micros", `{"type":"long","logicalType":"time-micros"}`, new(int64(5000))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := s.AppendEncode(nil, tt.encode)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			var v any
			_, err = s.Decode(encoded, &v)
			if err != nil {
				t.Fatalf("decode into interface: %v", err)
			}
		})
	}
}

// ---- Logical type deser fallback to int/uint ----

func TestLogicalTypeDeserFallbackInt(t *testing.T) {
	// timestamp-millis into int64
	{
		schema := `{"type":"long","logicalType":"timestamp-millis"}`
		input := int64(1718400000000)
		got := roundTrip(t, schema, input)
		if got != input {
			t.Fatalf("timestamp-millis int64: got %d, want %d", got, input)
		}
	}
	// timestamp-micros into int64
	{
		schema := `{"type":"long","logicalType":"timestamp-micros"}`
		input := int64(1718400000000000)
		got := roundTrip(t, schema, input)
		if got != input {
			t.Fatalf("timestamp-micros int64: got %d, want %d", got, input)
		}
	}
	// date into int32
	{
		schema := `{"type":"int","logicalType":"date"}`
		input := int32(19888)
		got := roundTrip(t, schema, input)
		if got != input {
			t.Fatalf("date int32: got %d, want %d", got, input)
		}
	}
	// time-millis into int32
	{
		schema := `{"type":"int","logicalType":"time-millis"}`
		input := int32(45123)
		got := roundTrip(t, schema, input)
		if got != input {
			t.Fatalf("time-millis int32: got %d, want %d", got, input)
		}
	}
	// time-micros into int64
	{
		schema := `{"type":"long","logicalType":"time-micros"}`
		input := int64(120000500)
		got := roundTrip(t, schema, input)
		if got != input {
			t.Fatalf("time-micros int64: got %d, want %d", got, input)
		}
	}
}

func TestLogicalTypeDeserFallbackUint(t *testing.T) {
	// timestamp-millis into uint64
	{
		schema := `{"type":"long","logicalType":"timestamp-millis"}`
		input := uint64(1718400000000)
		got := roundTrip(t, schema, input)
		if got != input {
			t.Fatalf("timestamp-millis uint64: got %d, want %d", got, input)
		}
	}
	// date into uint32
	{
		schema := `{"type":"int","logicalType":"date"}`
		input := uint32(19888)
		got := roundTrip(t, schema, input)
		if got != input {
			t.Fatalf("date uint32: got %d, want %d", got, input)
		}
	}
}

func TestLogicalTypeDeserTypeMismatch(t *testing.T) {
	tests := []struct {
		name   string
		schema string
	}{
		{"timestamp-millis into bool", `{"type":"long","logicalType":"timestamp-millis"}`},
		{"timestamp-micros into bool", `{"type":"long","logicalType":"timestamp-micros"}`},
		{"date into bool", `{"type":"int","logicalType":"date"}`},
		{"time-millis into bool", `{"type":"int","logicalType":"time-millis"}`},
		{"time-micros into bool", `{"type":"long","logicalType":"time-micros"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatal(err)
			}
			// Encode a valid value.
			encoded, err := s.AppendEncode(nil, new(int64(42)))
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			// Decode into incompatible type.
			var v bool
			_, err = s.Decode(encoded, &v)
			if err == nil {
				t.Fatal("expected error decoding logical type into bool")
			}
		})
	}
}

// ---- Logical type ser fallback (non time.Time/Duration) ----

func TestLogicalTypeSerFallback(t *testing.T) {
	// timestamp-micros with raw int64
	{
		schema := `{"type":"long","logicalType":"timestamp-micros"}`
		input := int64(999)
		got := roundTrip(t, schema, input)
		if got != input {
			t.Fatalf("timestamp-micros int64: got %d, want %d", got, input)
		}
	}
	// date with raw int32
	{
		schema := `{"type":"int","logicalType":"date"}`
		input := int32(100)
		got := roundTrip(t, schema, input)
		if got != input {
			t.Fatalf("date int32: got %d, want %d", got, input)
		}
	}
	// time-millis with raw int32
	{
		schema := `{"type":"int","logicalType":"time-millis"}`
		input := int32(5000)
		got := roundTrip(t, schema, input)
		if got != input {
			t.Fatalf("time-millis int32: got %d, want %d", got, input)
		}
	}
	// time-micros with raw int64
	{
		schema := `{"type":"long","logicalType":"time-micros"}`
		input := int64(5000)
		got := roundTrip(t, schema, input)
		if got != input {
			t.Fatalf("time-micros int64: got %d, want %d", got, input)
		}
	}
}

// ---- In-record logical type round-trips for unsafe fast path ----

func TestTimestampMicrosInRecord(t *testing.T) {
	type R struct {
		Created time.Time `avro:"created"`
	}
	schema := `{"type":"record","name":"r","fields":[
		{"name":"created","type":{"type":"long","logicalType":"timestamp-micros"}}
	]}`
	now := time.UnixMicro(time.Now().UnixMicro())
	got := roundTrip(t, schema, R{Created: now})
	if !got.Created.Equal(now) {
		t.Fatalf("timestamp-micros in record: got %v, want %v", got.Created, now)
	}
}

func TestTimeMicrosInRecord(t *testing.T) {
	type R struct {
		Elapsed time.Duration `avro:"elapsed"`
	}
	schema := `{"type":"record","name":"r","fields":[
		{"name":"elapsed","type":{"type":"long","logicalType":"time-micros"}}
	]}`
	input := 3*time.Minute + 250*time.Microsecond
	got := roundTrip(t, schema, R{Elapsed: input})
	if got.Elapsed != input {
		t.Fatalf("time-micros in record: got %v, want %v", got.Elapsed, input)
	}
}

// In-record with int64 fields for logical types (exercises tryCompileLogicalSer/Deser fallback).
func TestLogicalTypeInRecordWithIntFields(t *testing.T) {
	type RMillis struct {
		TS int64 `avro:"ts"`
	}
	type RMicros struct {
		TS int64 `avro:"ts"`
	}
	type RDate struct {
		D int32 `avro:"d"`
	}
	type RTimeMillis struct {
		T int32 `avro:"t"`
	}
	type RTimeMicros struct {
		T int64 `avro:"t"`
	}

	t.Run("timestamp-millis int64", func(t *testing.T) {
		schema := `{"type":"record","name":"r","fields":[
			{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}
		]}`
		got := roundTrip(t, schema, RMillis{TS: 1718400000000})
		if got.TS != 1718400000000 {
			t.Fatalf("got %d", got.TS)
		}
	})
	t.Run("timestamp-micros int64", func(t *testing.T) {
		schema := `{"type":"record","name":"r","fields":[
			{"name":"ts","type":{"type":"long","logicalType":"timestamp-micros"}}
		]}`
		got := roundTrip(t, schema, RMicros{TS: 1718400000000000})
		if got.TS != 1718400000000000 {
			t.Fatalf("got %d", got.TS)
		}
	})
	t.Run("date int32", func(t *testing.T) {
		schema := `{"type":"record","name":"r","fields":[
			{"name":"d","type":{"type":"int","logicalType":"date"}}
		]}`
		got := roundTrip(t, schema, RDate{D: 19888})
		if got.D != 19888 {
			t.Fatalf("got %d", got.D)
		}
	})
	t.Run("time-millis int32", func(t *testing.T) {
		schema := `{"type":"record","name":"r","fields":[
			{"name":"t","type":{"type":"int","logicalType":"time-millis"}}
		]}`
		got := roundTrip(t, schema, RTimeMillis{T: 45123})
		if got.T != 45123 {
			t.Fatalf("got %d", got.T)
		}
	})
	t.Run("time-micros int64", func(t *testing.T) {
		schema := `{"type":"record","name":"r","fields":[
			{"name":"t","type":{"type":"long","logicalType":"time-micros"}}
		]}`
		got := roundTrip(t, schema, RTimeMicros{T: 120000500})
		if got.T != 120000500 {
			t.Fatalf("got %d", got.T)
		}
	})
	t.Run("local-timestamp-millis int64", func(t *testing.T) {
		schema := `{"type":"record","name":"r","fields":[
			{"name":"ts","type":{"type":"long","logicalType":"local-timestamp-millis"}}
		]}`
		got := roundTrip(t, schema, RMillis{TS: 1718400000000})
		if got.TS != 1718400000000 {
			t.Fatalf("got %d", got.TS)
		}
	})
	t.Run("local-timestamp-micros int64", func(t *testing.T) {
		schema := `{"type":"record","name":"r","fields":[
			{"name":"ts","type":{"type":"long","logicalType":"local-timestamp-micros"}}
		]}`
		got := roundTrip(t, schema, RMicros{TS: 1718400000000000})
		if got.TS != 1718400000000000 {
			t.Fatalf("got %d", got.TS)
		}
	})
}

// ---- omitzero slow path (non-addressable struct) ----

func TestOmitzeroSlowPath(t *testing.T) {
	type R struct {
		Name string `avro:"name,omitzero"`
	}
	schema := `{"type":"record","name":"r","fields":[
		{"name":"name","type":["null","string"]}
	]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	// Pass struct by value (not pointer) to force non-addressable slow path.
	var v any = R{Name: ""}
	encoded, err := s.AppendEncode(nil, v)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(encoded) != 1 || encoded[0] != 0 {
		t.Fatalf("expected null encoding, got %x", encoded)
	}

	// Non-zero value via slow path.
	v = R{Name: "hi"}
	encoded, err = s.AppendEncode(nil, v)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if encoded[0] != 2 {
		t.Fatalf("expected non-null, got %x", encoded[0])
	}
}

// ---- validateDefault extra coverage ----

func TestSchemaDefaultsValidExtra(t *testing.T) {
	tests := []struct {
		name   string
		schema string
	}{
		{"float default", `{"type":"record","name":"r","fields":[
			{"name":"a","type":"float","default":3.14}
		]}`},
		{"double default", `{"type":"record","name":"r","fields":[
			{"name":"a","type":"double","default":2.718}
		]}`},
		{"long default", `{"type":"record","name":"r","fields":[
			{"name":"a","type":"long","default":9999}
		]}`},
		{"bytes default", `{"type":"record","name":"r","fields":[
			{"name":"a","type":"bytes","default":"\\u0000\\u0001"}
		]}`},
		{"null default", `{"type":"record","name":"r","fields":[
			{"name":"a","type":"null","default":null}
		]}`},
		{"enum default", `{"type":"record","name":"r","fields":[
			{"name":"a","type":{"type":"enum","name":"e","symbols":["X","Y"]},"default":"X"}
		]}`},
		{"record default", `{"type":"record","name":"r","fields":[
			{"name":"a","type":{"type":"record","name":"inner","fields":[{"name":"x","type":"int"}]},"default":{"x":1}}
		]}`},
		{"fixed default", `{"type":"record","name":"r","fields":[
			{"name":"a","type":{"type":"fixed","name":"f","size":4},"default":"\\u0000\\u0000\\u0000\\u0000"}
		]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.schema)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
		})
	}
}

func TestSchemaDefaultsInvalidExtra(t *testing.T) {
	tests := []struct {
		name   string
		schema string
	}{
		{"null with non-null", `{"type":"record","name":"r","fields":[
			{"name":"a","type":"null","default":42}
		]}`},
		{"float with string", `{"type":"record","name":"r","fields":[
			{"name":"a","type":"float","default":"not a number"}
		]}`},
		{"double with bool", `{"type":"record","name":"r","fields":[
			{"name":"a","type":"double","default":true}
		]}`},
		{"bytes with number", `{"type":"record","name":"r","fields":[
			{"name":"a","type":"bytes","default":42}
		]}`},
		{"enum with number", `{"type":"record","name":"r","fields":[
			{"name":"a","type":{"type":"enum","name":"e","symbols":["X"]},"default":42}
		]}`},
		{"array with string", `{"type":"record","name":"r","fields":[
			{"name":"a","type":{"type":"array","items":"int"},"default":"notarray"}
		]}`},
		{"map with string", `{"type":"record","name":"r","fields":[
			{"name":"a","type":{"type":"map","values":"string"},"default":"notmap"}
		]}`},
		{"fixed with number", `{"type":"record","name":"r","fields":[
			{"name":"a","type":{"type":"fixed","name":"f","size":4},"default":42}
		]}`},
		{"record with string", `{"type":"record","name":"r","fields":[
			{"name":"a","type":{"type":"record","name":"inner","fields":[{"name":"x","type":"int"}]},"default":"notrecord"}
		]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.schema)
			if err == nil {
				t.Fatal("expected error for invalid default")
			}
		})
	}
}

// ---- Coverage: logical deser short buffer (readVarlong/readVarint error) ----

func TestLogicalDeserShortBuffer(t *testing.T) {
	tests := []struct {
		name   string
		schema string
	}{
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`},
		{"date", `{"type":"int","logicalType":"date"}`},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`},
		{"time-micros", `{"type":"long","logicalType":"time-micros"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Truncated varint/varlong.
			decodeErr(t, tt.schema, []byte{0xE6, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}, new(int64(0)))
		})
	}
}

// ---- Coverage: logical deser uint fallback for timestamp-micros, time-millis, time-micros ----

func TestLogicalTypeDeserFallbackUintExtra(t *testing.T) {
	// timestamp-micros into uint64
	{
		schema := `{"type":"long","logicalType":"timestamp-micros"}`
		input := uint64(1718400000000000)
		got := roundTrip(t, schema, input)
		if got != input {
			t.Fatalf("timestamp-micros uint64: got %d, want %d", got, input)
		}
	}
	// time-millis into uint32
	{
		schema := `{"type":"int","logicalType":"time-millis"}`
		input := uint32(45123)
		got := roundTrip(t, schema, input)
		if got != input {
			t.Fatalf("time-millis uint32: got %d, want %d", got, input)
		}
	}
	// time-micros into uint64
	{
		schema := `{"type":"long","logicalType":"time-micros"}`
		input := uint64(120000500)
		got := roundTrip(t, schema, input)
		if got != input {
			t.Fatalf("time-micros uint64: got %d, want %d", got, input)
		}
	}
}

// ---- Coverage: logical ser nil pointer (indirect error) ----

func TestLogicalSerNilPointer(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		v      any
	}{
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, (*time.Time)(nil)},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`, (*time.Time)(nil)},
		{"date", `{"type":"int","logicalType":"date"}`, (*time.Time)(nil)},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`, (*time.Duration)(nil)},
		{"time-micros", `{"type":"long","logicalType":"time-micros"}`, (*time.Duration)(nil)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.AppendEncode(nil, tt.v)
			if err == nil {
				t.Fatal("expected error encoding nil pointer")
			}
		})
	}
}

// ---- Coverage: tryCompileLogicalSer/Deser default (uuid logical type in record) ----

func TestUUIDLogicalTypeInRecord(t *testing.T) {
	type R struct {
		ID string `avro:"id"`
	}
	schema := `{"type":"record","name":"r","fields":[
		{"name":"id","type":{"type":"string","logicalType":"uuid"}}
	]}`
	input := R{ID: "550e8400-e29b-41d4-a716-446655440000"}
	got := roundTrip(t, schema, input)
	if got.ID != input.ID {
		t.Fatalf("uuid in record: got %s, want %s", got.ID, input.ID)
	}
}

func TestUUIDByteArrayRoundTrip(t *testing.T) {
	uuidSchema := `{"type":"string","logicalType":"uuid"}`
	uuidBytes := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	wantStr := "550e8400-e29b-41d4-a716-446655440000"

	t.Run("bare [16]byte", func(t *testing.T) {
		got := roundTrip(t, uuidSchema, uuidBytes)
		if got != uuidBytes {
			t.Fatalf("got %x, want %x", got, uuidBytes)
		}
	})

	t.Run("custom type MyUUID", func(t *testing.T) {
		type MyUUID [16]byte
		input := MyUUID(uuidBytes)
		got := roundTrip(t, uuidSchema, input)
		if got != input {
			t.Fatalf("got %x, want %x", got, input)
		}
	})

	t.Run("[16]byte in record", func(t *testing.T) {
		type R struct {
			ID [16]byte `avro:"id"`
		}
		schema := `{"type":"record","name":"r","fields":[
			{"name":"id","type":{"type":"string","logicalType":"uuid"}}
		]}`
		input := R{ID: uuidBytes}
		got := roundTrip(t, schema, input)
		if got.ID != input.ID {
			t.Fatalf("got %x, want %x", got.ID, input.ID)
		}
	})

	t.Run("wire format is 36-char hex-dash string", func(t *testing.T) {
		encoded := encode(t, uuidSchema, &uuidBytes)
		// Avro string: varint length prefix + string bytes.
		// 36 encodes as varint 72 (zigzag), which is a single byte.
		if len(encoded) < 1 {
			t.Fatal("encoded too short")
		}
		// Read varint length.
		length, rest, err := readVarlong(encoded)
		if err != nil {
			t.Fatalf("readVarlong: %v", err)
		}
		if length != 36 {
			t.Fatalf("wire string length: got %d, want 36", length)
		}
		if string(rest) != wantStr {
			t.Fatalf("wire string: got %q, want %q", string(rest), wantStr)
		}
	})

	t.Run("invalid UUID decode error", func(t *testing.T) {
		// Encode a non-UUID string and try to decode into [16]byte.
		s, err := Parse(uuidSchema)
		if err != nil {
			t.Fatal(err)
		}
		badStr := "not-a-uuid"
		encoded, err := s.Encode(&badStr)
		if err != nil {
			t.Fatal(err)
		}
		var out [16]byte
		_, err = s.Decode(encoded, &out)
		if err == nil {
			t.Fatal("expected error decoding invalid UUID into [16]byte")
		}
	})

	t.Run("string field still works", func(t *testing.T) {
		input := wantStr
		got := roundTrip(t, uuidSchema, input)
		if got != input {
			t.Fatalf("got %q, want %q", got, input)
		}
	})
}

// ---- Coverage: unsafe deser short buffer for time logical types in records ----

func TestLogicalTypeUnsafeDeserShortBuffer(t *testing.T) {
	corrupt := []byte{0xE6, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}

	t.Run("timestamp-millis", func(t *testing.T) {
		type R struct {
			T time.Time `avro:"t"`
		}
		decodeErr(t, `{"type":"record","name":"r","fields":[
			{"name":"t","type":{"type":"long","logicalType":"timestamp-millis"}}
		]}`, corrupt, new(R))
	})
	t.Run("timestamp-micros", func(t *testing.T) {
		type R struct {
			T time.Time `avro:"t"`
		}
		decodeErr(t, `{"type":"record","name":"r","fields":[
			{"name":"t","type":{"type":"long","logicalType":"timestamp-micros"}}
		]}`, corrupt, new(R))
	})
	t.Run("date", func(t *testing.T) {
		type R struct {
			T time.Time `avro:"t"`
		}
		decodeErr(t, `{"type":"record","name":"r","fields":[
			{"name":"t","type":{"type":"int","logicalType":"date"}}
		]}`, corrupt, new(R))
	})
	t.Run("time-millis", func(t *testing.T) {
		type R struct {
			D time.Duration `avro:"d"`
		}
		decodeErr(t, `{"type":"record","name":"r","fields":[
			{"name":"d","type":{"type":"int","logicalType":"time-millis"}}
		]}`, corrupt, new(R))
	})
	t.Run("time-micros", func(t *testing.T) {
		type R struct {
			D time.Duration `avro:"d"`
		}
		decodeErr(t, `{"type":"record","name":"r","fields":[
			{"name":"d","type":{"type":"long","logicalType":"time-micros"}}
		]}`, corrupt, new(R))
	})
}

// ---- Coverage: validateLogical error in buildComplex ----

func TestSchemaValidateLogicalError(t *testing.T) {
	// date requires int, not string.
	_, err := Parse(`{"type":"string","logicalType":"date"}`)
	if err == nil {
		t.Fatal("expected error for date on string type")
	}
}

// ---- Duration logical type ----

func TestDurationRoundTrip(t *testing.T) {
	schema := `{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}`
	d := Duration{Months: 3, Days: 15, Milliseconds: 86400000}
	got := roundTrip(t, schema, d)
	if got != d {
		t.Fatalf("got %+v, want %+v", got, d)
	}
}

func TestDurationZero(t *testing.T) {
	schema := `{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}`
	d := Duration{}
	got := roundTrip(t, schema, d)
	if got != d {
		t.Fatalf("got %+v, want %+v", got, d)
	}
}

func TestDurationMaxValues(t *testing.T) {
	schema := `{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}`
	d := Duration{Months: math.MaxUint32, Days: math.MaxUint32, Milliseconds: math.MaxUint32}
	got := roundTrip(t, schema, d)
	if got != d {
		t.Fatalf("got %+v, want %+v", got, d)
	}
}

func TestDurationInRecord(t *testing.T) {
	type R struct {
		D Duration `avro:"d"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"d","type":{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}}
	]}`
	in := R{D: Duration{Months: 1, Days: 2, Milliseconds: 3}}
	got := roundTrip(t, schema, in)
	if got != in {
		t.Fatalf("got %+v, want %+v", got, in)
	}
}

func TestDurationAsFixedBytes(t *testing.T) {
	// Deserialize into [12]byte instead of Duration.
	schema := `{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}`
	d := Duration{Months: 1, Days: 2, Milliseconds: 3}
	s, err := NewSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := s.AppendEncode(nil, &d)
	if err != nil {
		t.Fatal(err)
	}
	var raw [12]byte
	rem, err := s.Decode(encoded, &raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("leftover bytes: %d", len(rem))
	}
	// Verify the raw bytes encode LE uint32s.
	if raw[0] != 1 || raw[4] != 2 || raw[8] != 3 {
		t.Fatalf("unexpected raw bytes: %x", raw)
	}
}

func TestDurationPointer(t *testing.T) {
	schema := `{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}`
	d := &Duration{Months: 10, Days: 20, Milliseconds: 30}
	got := roundTrip(t, schema, d)
	if *got != *d {
		t.Fatalf("got %+v, want %+v", *got, *d)
	}
}

func TestDurationShortBuffer(t *testing.T) {
	schema := `{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}`
	s, err := NewSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	// Only 11 bytes — needs 12.
	short := make([]byte, 11)
	var out Duration
	_, err = s.Decode(short, &out)
	if err == nil {
		t.Fatal("expected error for short buffer")
	}
}

// ---- Coverage: timestamp-nanos / local-timestamp-nanos ----

func TestTimestampNanosRoundTrip(t *testing.T) {
	schema := `{"type":"long","logicalType":"timestamp-nanos"}`
	now := time.Now()
	got := roundTrip(t, schema, now)
	if !got.Equal(now) {
		t.Fatalf("timestamp-nanos round trip: got %v, want %v", got, now)
	}
}

func TestLocalTimestampNanosRoundTrip(t *testing.T) {
	schema := `{"type":"long","logicalType":"local-timestamp-nanos"}`
	now := time.Now()
	got := roundTrip(t, schema, now)
	if !got.Equal(now) {
		t.Fatalf("local-timestamp-nanos round trip: got %v, want %v", got, now)
	}
}

func TestTimestampNanosFallbackToInt64(t *testing.T) {
	schema := `{"type":"long","logicalType":"timestamp-nanos"}`
	input := int64(1718400000000000000)
	got := roundTrip(t, schema, input)
	if got != input {
		t.Fatalf("timestamp-nanos int64 fallback: got %d, want %d", got, input)
	}
}

func TestTimestampNanosInRecord(t *testing.T) {
	type R struct {
		Created time.Time `avro:"created"`
	}
	schema := `{"type":"record","name":"r","fields":[
		{"name":"created","type":{"type":"long","logicalType":"timestamp-nanos"}}
	]}`
	input := R{Created: time.Now()}
	got := roundTrip(t, schema, input)
	if !got.Created.Equal(input.Created) {
		t.Fatalf("got %v, want %v", got.Created, input.Created)
	}
}

func TestTimestampNanosDecodeIntoInterface(t *testing.T) {
	schema := `{"type":"long","logicalType":"timestamp-nanos"}`
	now := time.Now()
	encoded := encode(t, schema, &now)
	var v any
	decode(t, schema, encoded, &v)
	got, ok := v.(int64)
	if !ok {
		t.Fatalf("expected int64, got %T", v)
	}
	if got != now.UnixNano() {
		t.Fatalf("got %d, want %d", got, now.UnixNano())
	}
}

func TestTimestampNanosDecodeUint(t *testing.T) {
	schema := `{"type":"long","logicalType":"timestamp-nanos"}`
	now := time.Now()
	encoded := encode(t, schema, &now)
	var v uint64
	decode(t, schema, encoded, &v)
	if v != uint64(now.UnixNano()) {
		t.Fatalf("got %d, want %d", v, uint64(now.UnixNano()))
	}
}

func TestTimestampNanosDecodeError(t *testing.T) {
	schema := `{"type":"long","logicalType":"timestamp-nanos"}`
	now := time.Now()
	encoded := encode(t, schema, &now)
	var v string
	s, _ := Parse(schema)
	_, err := s.Decode(encoded, &v)
	if err == nil {
		t.Fatal("expected error decoding nanos into string")
	}
}

// ---- Coverage: UUID edge cases ----

func TestUUIDDecodeIntoInterface(t *testing.T) {
	uuidSchema := `{"type":"string","logicalType":"uuid"}`
	input := "550e8400-e29b-41d4-a716-446655440000"
	encoded := encode(t, uuidSchema, &input)
	var v any
	decode(t, uuidSchema, encoded, &v)
	got, ok := v.(string)
	if !ok {
		t.Fatalf("expected string, got %T", v)
	}
	if got != input {
		t.Fatalf("got %s, want %s", got, input)
	}
}

func TestUUIDDecodeIntoTextUnmarshaler(t *testing.T) {
	uuidSchema := `{"type":"string","logicalType":"uuid"}`
	input := "550e8400-e29b-41d4-a716-446655440000"
	encoded := encode(t, uuidSchema, &input)
	var v testTextUnmarshaler
	decode(t, uuidSchema, encoded, &v)
	want := "unmarshaled:" + input
	if v.val != want {
		t.Fatalf("got %s, want %s", v.val, want)
	}
}

func TestUUIDDecodeTypeError(t *testing.T) {
	uuidSchema := `{"type":"string","logicalType":"uuid"}`
	input := "550e8400-e29b-41d4-a716-446655440000"
	encoded := encode(t, uuidSchema, &input)
	var v int
	s, _ := Parse(uuidSchema)
	_, err := s.Decode(encoded, &v)
	if err == nil {
		t.Fatal("expected error decoding UUID into int")
	}
}

func TestUUIDDecodeNegativeLength(t *testing.T) {
	uuidSchema := `{"type":"string","logicalType":"uuid"}`
	s, _ := Parse(uuidSchema)
	// Encode a negative length varint: -1 zigzag = 0x01
	var v string
	_, err := s.Decode([]byte{0x01}, &v)
	if err == nil {
		t.Fatal("expected error for negative UUID string length")
	}
}

func TestUUIDDecodeShortBuffer(t *testing.T) {
	uuidSchema := `{"type":"string","logicalType":"uuid"}`
	s, _ := Parse(uuidSchema)
	// Length 36, but only 2 bytes of data.
	var v string
	_, err := s.Decode([]byte{72, 'a', 'b'}, &v) // 72 = zigzag(36)
	if err == nil {
		t.Fatal("expected error for short buffer")
	}
}

func TestDurationInRecordUnsafeShortBuffer(t *testing.T) {
	type R struct {
		D Duration `avro:"d"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"d","type":{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}}
	]}`
	decodeErr(t, schema, make([]byte, 11), new(R))
}

func TestDurationSchemaValidation(t *testing.T) {
	// duration must be on fixed.
	_, err := NewSchema(`{"type":"int","logicalType":"duration"}`)
	if err == nil {
		t.Fatal("expected error: duration on int")
	}
	// duration fixed must be size 12.
	_, err = NewSchema(`{"type":"fixed","name":"d","size":8,"logicalType":"duration"}`)
	if err == nil {
		t.Fatal("expected error: duration size != 12")
	}
}

// ---- Decimal logical type (bytes) ----

func TestBytesDecimalRoundTrip(t *testing.T) {
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	// 123.45 = 12345/100
	r := new(big.Rat).SetFrac64(12345, 100)
	got := roundTrip(t, schema, r)
	if got.Cmp(r) != 0 {
		t.Fatalf("got %s, want %s", got.RatString(), r.RatString())
	}
}

func TestBytesDecimalZero(t *testing.T) {
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	r := new(big.Rat)
	got := roundTrip(t, schema, r)
	if got.Cmp(r) != 0 {
		t.Fatalf("got %s, want %s", got.RatString(), r.RatString())
	}
}

func TestBytesDecimalNegative(t *testing.T) {
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	// -99.99 = -9999/100
	r := new(big.Rat).SetFrac64(-9999, 100)
	got := roundTrip(t, schema, r)
	if got.Cmp(r) != 0 {
		t.Fatalf("got %s, want %s", got.RatString(), r.RatString())
	}
}

func TestBytesDecimalScale0(t *testing.T) {
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":0}`
	r := new(big.Rat).SetInt64(42)
	got := roundTrip(t, schema, r)
	if got.Cmp(r) != 0 {
		t.Fatalf("got %s, want %s", got.RatString(), r.RatString())
	}
}

func TestBytesDecimalLargeValue(t *testing.T) {
	schema := `{"type":"bytes","logicalType":"decimal","precision":38,"scale":10}`
	// Large value: 123456789012345678.1234567890
	num, _ := new(big.Int).SetString("1234567890123456781234567890", 10)
	r := new(big.Rat).SetFrac(num, new(big.Int).Exp(big.NewInt(10), big.NewInt(10), nil))
	got := roundTrip(t, schema, r)
	if got.Cmp(r) != 0 {
		t.Fatalf("got %s, want %s", got.RatString(), r.RatString())
	}
}

func TestBytesDecimalInRecord(t *testing.T) {
	type R struct {
		V *big.Rat `avro:"v"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"v","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}}
	]}`
	in := R{V: new(big.Rat).SetFrac64(12345, 100)}
	got := roundTrip(t, schema, in)
	if got.V.Cmp(in.V) != 0 {
		t.Fatalf("got %s, want %s", got.V.RatString(), in.V.RatString())
	}
}

// ---- Decimal logical type (fixed) ----

func TestFixedDecimalRoundTrip(t *testing.T) {
	schema := `{"type":"fixed","name":"dec","size":8,"logicalType":"decimal","precision":18,"scale":2}`
	r := new(big.Rat).SetFrac64(12345, 100)
	got := roundTrip(t, schema, r)
	if got.Cmp(r) != 0 {
		t.Fatalf("got %s, want %s", got.RatString(), r.RatString())
	}
}

func TestFixedDecimalZero(t *testing.T) {
	schema := `{"type":"fixed","name":"dec","size":8,"logicalType":"decimal","precision":18,"scale":2}`
	r := new(big.Rat)
	got := roundTrip(t, schema, r)
	if got.Cmp(r) != 0 {
		t.Fatalf("got %s, want %s", got.RatString(), r.RatString())
	}
}

func TestFixedDecimalNegative(t *testing.T) {
	schema := `{"type":"fixed","name":"dec","size":8,"logicalType":"decimal","precision":18,"scale":2}`
	r := new(big.Rat).SetFrac64(-9999, 100)
	got := roundTrip(t, schema, r)
	if got.Cmp(r) != 0 {
		t.Fatalf("got %s, want %s", got.RatString(), r.RatString())
	}
}

func TestFixedDecimalOverflow(t *testing.T) {
	// Fixed size 2 can hold at most ±32767 unscaled. Try a value that overflows.
	schema := `{"type":"fixed","name":"dec","size":2,"logicalType":"decimal","precision":5,"scale":0}`
	s, err := NewSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	// 100000 requires 3 bytes, won't fit in 2.
	r := new(big.Rat).SetInt64(100000)
	_, err = s.AppendEncode(nil, &r)
	if err == nil {
		t.Fatal("expected overflow error")
	}
}

func TestFixedDecimalFallbackToArray(t *testing.T) {
	// Deserialize fixed decimal into [8]byte instead of *big.Rat.
	schema := `{"type":"fixed","name":"dec","size":8,"logicalType":"decimal","precision":18,"scale":2}`
	s, err := NewSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	r := new(big.Rat).SetFrac64(12345, 100)
	encoded, err := s.AppendEncode(nil, &r)
	if err != nil {
		t.Fatal(err)
	}
	var raw [8]byte
	rem, err := s.Decode(encoded, &raw)
	if err != nil {
		t.Fatalf("Decode into [8]byte: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("leftover: %d", len(rem))
	}
}

func TestFixedDecimalInRecord(t *testing.T) {
	type R struct {
		V *big.Rat `avro:"v"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"v","type":{"type":"fixed","name":"dec","size":8,"logicalType":"decimal","precision":18,"scale":2}}
	]}`
	in := R{V: new(big.Rat).SetFrac64(-12345, 100)}
	got := roundTrip(t, schema, in)
	if got.V.Cmp(in.V) != 0 {
		t.Fatalf("got %s, want %s", got.V.RatString(), in.V.RatString())
	}
}

func TestDecimalSchemaValidation(t *testing.T) {
	// decimal requires precision.
	_, err := NewSchema(`{"type":"bytes","logicalType":"decimal"}`)
	if err == nil {
		t.Fatal("expected error: decimal without precision")
	}
	// decimal must be bytes or fixed.
	_, err = NewSchema(`{"type":"int","logicalType":"decimal","precision":10}`)
	if err == nil {
		t.Fatal("expected error: decimal on int")
	}
}

func TestBytesDecimalShortBuffer(t *testing.T) {
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	s, err := NewSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	// Varint says 10 bytes but only 2 available.
	data := []byte{20, 0x30, 0x39} // length=10 (zigzag), only 2 data bytes
	var out *big.Rat
	_, err = s.Decode(data, &out)
	if err == nil {
		t.Fatal("expected short buffer error")
	}
}

func TestFixedDecimalShortBuffer(t *testing.T) {
	schema := `{"type":"fixed","name":"dec","size":8,"logicalType":"decimal","precision":18,"scale":2}`
	s, err := NewSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	short := make([]byte, 7) // needs 8
	var out *big.Rat
	_, err = s.Decode(short, &out)
	if err == nil {
		t.Fatal("expected short buffer error")
	}
}

func TestBytesDecimalNegativeLength(t *testing.T) {
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	s, err := NewSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	// Zigzag encode -1 as length → 0x01.
	data := []byte{0x01}
	var out *big.Rat
	_, err = s.Decode(data, &out)
	if err == nil {
		t.Fatal("expected negative length error")
	}
}

// ---- Duration/Decimal: fallback to raw byte types ----

func TestDurationSerAsFixedArray(t *testing.T) {
	// Encode a [12]byte through a duration schema (fallback path).
	schema := `{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}`
	raw := [12]byte{1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0}
	got := roundTrip(t, schema, raw)
	if got != raw {
		t.Fatalf("got %x, want %x", got, raw)
	}
}

func TestBytesDecimalSerAsBytes(t *testing.T) {
	// Encode a []byte through a bytes+decimal schema (fallback to serBytes).
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	raw := []byte{0x30, 0x39} // 12345 in big-endian
	s, err := NewSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := s.AppendEncode(nil, &raw)
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	if len(encoded) == 0 {
		t.Fatal("expected non-empty output")
	}
}

func TestFixedDecimalSerAsFixedArray(t *testing.T) {
	// Encode a [8]byte through a fixed+decimal schema (fallback path).
	schema := `{"type":"fixed","name":"dec","size":8,"logicalType":"decimal","precision":18,"scale":2}`
	raw := [8]byte{0, 0, 0, 0, 0, 0, 0x30, 0x39}
	got := roundTrip(t, schema, raw)
	if got != raw {
		t.Fatalf("got %x, want %x", got, raw)
	}
}

// ---- Duration in record as [12]byte triggers unsafe fallback ----

func TestDurationInRecordAsFixedArray(t *testing.T) {
	type R struct {
		D [12]byte `avro:"d"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"d","type":{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}}
	]}`
	in := R{D: [12]byte{1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0}}
	got := roundTrip(t, schema, in)
	if got != in {
		t.Fatalf("got %+v, want %+v", got, in)
	}
}

// ---- Decimal: bigIntToBytes edge cases ----

func TestBytesDecimalHighBitPositive(t *testing.T) {
	// Value 1.28 (unscaled 128) — 128=0x80, needs 0x00 prefix.
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	r := new(big.Rat).SetFrac64(128, 100) // 1.28, unscaled = 128
	got := roundTrip(t, schema, r)
	if got.Cmp(r) != 0 {
		t.Fatalf("got %s, want %s", got.RatString(), r.RatString())
	}
}

func TestBytesDecimalMinusOne(t *testing.T) {
	// Value -0.01 (unscaled -1) — special case in bigIntToBytes.
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	r := new(big.Rat).SetFrac64(-1, 100)
	got := roundTrip(t, schema, r)
	if got.Cmp(r) != 0 {
		t.Fatalf("got %s, want %s", got.RatString(), r.RatString())
	}
}

func TestBytesDecimalNegativeNeedsPadding(t *testing.T) {
	// Value -1.29 (unscaled -129) — abs=128, bytes=[0x80], flip=[0x7f], needs 0xff prefix.
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	r := new(big.Rat).SetFrac64(-129, 100)
	got := roundTrip(t, schema, r)
	if got.Cmp(r) != 0 {
		t.Fatalf("got %s, want %s", got.RatString(), r.RatString())
	}
}

// ---- Decimal: interface deserialization ----

func TestBytesDecimalDeserInterface(t *testing.T) {
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	r := new(big.Rat).SetFrac64(12345, 100)
	s, err := NewSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := s.AppendEncode(nil, &r)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	rem, err := s.Decode(encoded, &out)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("leftover: %d", len(rem))
	}
	got, ok := out.(*big.Rat)
	if !ok {
		t.Fatalf("expected *big.Rat, got %T", out)
	}
	if got.Cmp(r) != 0 {
		t.Fatalf("got %s, want %s", got.RatString(), r.RatString())
	}
}

func TestFixedDecimalDeserInterface(t *testing.T) {
	schema := `{"type":"fixed","name":"dec","size":8,"logicalType":"decimal","precision":18,"scale":2}`
	r := new(big.Rat).SetFrac64(12345, 100)
	s, err := NewSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := s.AppendEncode(nil, &r)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	rem, err := s.Decode(encoded, &out)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("leftover: %d", len(rem))
	}
	got, ok := out.(*big.Rat)
	if !ok {
		t.Fatalf("expected *big.Rat, got %T", out)
	}
	if got.Cmp(r) != 0 {
		t.Fatalf("got %s, want %s", got.RatString(), r.RatString())
	}
}

// ---- Decimal: type mismatch error ----

func TestBytesDecimalDeserWrongType(t *testing.T) {
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	r := new(big.Rat).SetFrac64(12345, 100)
	s, err := NewSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := s.AppendEncode(nil, &r)
	if err != nil {
		t.Fatal(err)
	}
	var out string
	_, err = s.Decode(encoded, &out)
	if err == nil {
		t.Fatal("expected error decoding decimal into string")
	}
}

// ---- Bytes decimal: truncated varint ----

func TestBytesDecimalTruncatedVarint(t *testing.T) {
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	s, err := NewSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	// Byte with continuation bit but no following byte.
	data := []byte{0x80}
	var out *big.Rat
	_, err = s.Decode(data, &out)
	if err == nil {
		t.Fatal("expected error for truncated varint")
	}
}

// ---- Decimal: empty bytes (zero length) → bytesToBigInt empty ----

func TestBytesDecimalEmptyBytes(t *testing.T) {
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	s, err := NewSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	// Length 0 → empty bytes → bytesToBigInt([]) → 0.
	data := []byte{0x00} // varint 0
	var out *big.Rat
	rem, err := s.Decode(data, &out)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("leftover: %d", len(rem))
	}
	if out.Sign() != 0 {
		t.Fatalf("expected zero rat, got %s", out.RatString())
	}
}

// ---- Fixed decimal: negative sign extension padding (ser) ----

func TestFixedDecimalNegativePadding(t *testing.T) {
	// Small negative value in large fixed: needs 0xff padding.
	schema := `{"type":"fixed","name":"dec","size":8,"logicalType":"decimal","precision":18,"scale":0}`
	r := new(big.Rat).SetInt64(-1)
	got := roundTrip(t, schema, r)
	if got.Cmp(r) != 0 {
		t.Fatalf("got %s, want %s", got.RatString(), r.RatString())
	}
}

// ---- Nil pointer errors for duration/decimal ser ----

func TestDurationSerNilPointer(t *testing.T) {
	schema := `{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}`
	s, err := NewSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	var d *Duration
	_, err = s.AppendEncode(nil, &d)
	if err == nil {
		t.Fatal("expected error for nil Duration pointer")
	}
}

func TestBytesDecimalSerNilPointer(t *testing.T) {
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	s, err := NewSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	var r *big.Rat
	_, err = s.AppendEncode(nil, &r)
	if err == nil {
		t.Fatal("expected error for nil *big.Rat pointer")
	}
}

func TestFixedDecimalSerNilPointer(t *testing.T) {
	schema := `{"type":"fixed","name":"dec","size":8,"logicalType":"decimal","precision":18,"scale":2}`
	s, err := NewSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	var r *big.Rat
	_, err = s.AppendEncode(nil, &r)
	if err == nil {
		t.Fatal("expected error for nil *big.Rat pointer")
	}
}

func TestParseUUIDInvalidHex(t *testing.T) {
	// Test each hex segment separately to hit all parseUUID error branches.
	uuidSchema := `{"type":"string","logicalType":"uuid"}`
	s, _ := Parse(uuidSchema)
	invalids := []string{
		"ZZZZZZZZ-e29b-41d4-a716-446655440000", // bad group 1
		"550e8400-ZZZZ-41d4-a716-446655440000", // bad group 2
		"550e8400-e29b-ZZZZ-a716-446655440000", // bad group 3
		"550e8400-e29b-41d4-ZZZZ-446655440000", // bad group 4
		"550e8400-e29b-41d4-a716-ZZZZZZZZZZZZ", // bad group 5
	}
	for _, bad := range invalids {
		encoded := encode(t, `"string"`, &bad)
		var u [16]byte
		_, err := s.Decode(encoded, &u)
		if err == nil {
			t.Fatalf("expected error for invalid UUID %q", bad)
		}
	}
}

// ---- Coverage: serNullUnion with invalid value ----

func TestSerNullUnionInvalidValue(t *testing.T) {
	schema := `["null","int"]`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	// Encode nil (zero reflect.Value via nil interface)
	encoded, err := s.Encode(nil)
	if err != nil {
		t.Fatalf("encode nil: %v", err)
	}
	if len(encoded) != 1 || encoded[0] != 0 {
		t.Fatalf("expected [0], got %v", encoded)
	}
}

// ---- Coverage: floorDiv positive exact division ----

func TestFloorDivPositive(t *testing.T) {
	// Positive exact division (no remainder, no adjustment needed).
	if got := floorDiv(10, 5); got != 2 {
		t.Fatalf("floorDiv(10,5) = %d, want 2", got)
	}
	// Negative with remainder (adjustment needed).
	if got := floorDiv(-1, 86400); got != -1 {
		t.Fatalf("floorDiv(-1,86400) = %d, want -1", got)
	}
	// Negative exact (no adjustment).
	if got := floorDiv(-86400, 86400); got != -1 {
		t.Fatalf("floorDiv(-86400,86400) = %d, want -1", got)
	}
}

// ---- Coverage: MustParse panic ----

func TestMustParsePanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic from MustParse")
		}
	}()
	MustParse(`invalid`)
}

// ---- Coverage: Schema.String() ----

func TestSchemaString(t *testing.T) {
	input := `{"type":"record","name":"R","fields":[{"name":"f","type":"int"}]}`
	s, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	if s.String() != input {
		t.Fatalf("String() = %q, want %q", s.String(), input)
	}
}

// ---- Coverage: error record type ----

func TestErrorRecordType(t *testing.T) {
	schema := `{"type":"error","name":"MyError","fields":[{"name":"msg","type":"string"},{"name":"code","type":"int"}]}`
	type MyError struct {
		Msg  string `avro:"msg"`
		Code int32  `avro:"code"`
	}
	input := MyError{Msg: "not found", Code: 404}
	got := roundTrip(t, schema, input)
	if got != input {
		t.Fatalf("got %+v, want %+v", got, input)
	}
}

// ---- Coverage: field order validation ----

func TestFieldOrderValidation(t *testing.T) {
	for _, order := range []string{"ascending", "descending", "ignore"} {
		schema := fmt.Sprintf(`{"type":"record","name":"R","fields":[{"name":"f","type":"int","order":"%s"}]}`, order)
		_, err := Parse(schema)
		if err != nil {
			t.Fatalf("unexpected error for order=%q: %v", order, err)
		}
	}
	_, err := Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":"int","order":"backwards"}]}`)
	if err == nil {
		t.Fatal("expected error for invalid field order")
	}
}

// ---- Coverage: big-decimal logical type ----

func TestBigDecimalLogicalType(t *testing.T) {
	schema := `{"type":"bytes","logicalType":"big-decimal"}`
	input := []byte{0, 0, 0, 2, 0x01, 0x39} // scale=2, unscaled=313
	got := roundTrip(t, schema, input)
	if !bytes.Equal(got, input) {
		t.Fatalf("got %x, want %x", got, input)
	}
}

func TestBigDecimalOnFixedRejected(t *testing.T) {
	_, err := Parse(`{"type":"fixed","name":"F","size":16,"logicalType":"big-decimal"}`)
	if err == nil {
		t.Fatal("expected error for big-decimal on fixed")
	}
}

// ---- Coverage: duplicate union named type ----

func TestDuplicateUnionNamedType(t *testing.T) {
	_, err := Parse(`[{"type":"record","name":"A","fields":[]},{"type":"record","name":"A","fields":[]}]`)
	if err == nil {
		t.Fatal("expected error for duplicate named type in union")
	}
}

// ---- Coverage: decimal on fixed with precision/scale in schemaNode ----

func TestDecimalFixedPrecisionScale(t *testing.T) {
	schema := `{"type":"fixed","name":"D","size":4,"logicalType":"decimal","precision":8,"scale":2}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	// Round-trip a fixed(4) value.
	input := [4]byte{0x00, 0x01, 0x86, 0xa0} // 100000 unscaled = 1000.00
	encoded, err := s.Encode(input)
	if err != nil {
		t.Fatal(err)
	}
	var got [4]byte
	_, err = s.Decode(encoded, &got)
	if err != nil {
		t.Fatal(err)
	}
	if got != input {
		t.Fatalf("got %x, want %x", got, input)
	}
}

func TestDecimalFixedInvalidPrecision(t *testing.T) {
	// size=1 can hold at most floor(log10(2^7-1)) = 2 digits.
	_, err := Parse(`{"type":"fixed","name":"D","size":1,"logicalType":"decimal","precision":3}`)
	if err == nil {
		t.Fatal("expected error for precision too large for fixed size")
	}
}

func TestMaxDecimalDigitsZeroSize(t *testing.T) {
	if got := maxDecimalDigits(0); got != 0 {
		t.Fatalf("maxDecimalDigits(0) = %d, want 0", got)
	}
}

// ---- Coverage: timestamp-nanos logicalSer/logicalDeser paths ----

func TestTimestampNanosLogicalTypeInComplexSchema(t *testing.T) {
	// This exercises the logicalSer/logicalDeser paths for nanos when
	// the schema is given as a complex object (not already a primitive).
	schema := `{"type":"long","logicalType":"timestamp-nanos"}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	encoded, err := s.Encode(&now)
	if err != nil {
		t.Fatal(err)
	}
	var got time.Time
	_, err = s.Decode(encoded, &got)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(now) {
		t.Fatalf("got %v, want %v", got, now)
	}
}

// ---- Coverage: UUID ser with [16]byte through non-record path ----

func TestSerUUIDArrayType(t *testing.T) {
	schema := `{"type":"string","logicalType":"uuid"}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	u := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	encoded, err := s.Encode(u)
	if err != nil {
		t.Fatal(err)
	}
	var got string
	_, err = s.Decode(encoded, &got)
	if err != nil {
		t.Fatal(err)
	}
	if got != "550e8400-e29b-41d4-a716-446655440000" {
		t.Fatalf("got %s, want 550e8400-e29b-41d4-a716-446655440000", got)
	}
}

// ---- Coverage: serUUID error path (non-uuid, non-string type) ----

func TestSerUUIDTypeError(t *testing.T) {
	schema := `{"type":"string","logicalType":"uuid"}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Encode(42)
	if err == nil {
		t.Fatal("expected error encoding int as UUID")
	}
}

// ---- Coverage: timestamp-nanos int64 in record (unsafe fast path fallback) ----

func TestTimestampNanosInt64InRecord(t *testing.T) {
	type R struct {
		V int64 `avro:"v"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"v","type":{"type":"long","logicalType":"timestamp-nanos"}}
	]}`
	input := R{V: 1718400000000000000}
	got := roundTrip(t, schema, input)
	if got.V != input.V {
		t.Fatalf("got %d, want %d", got.V, input.V)
	}
}

// ---- Coverage: UUID string in record (unsafe fast path for string) ----

func TestUUIDStringInRecord(t *testing.T) {
	type R struct {
		ID string `avro:"id"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"id","type":{"type":"string","logicalType":"uuid"}}
	]}`
	input := R{ID: "550e8400-e29b-41d4-a716-446655440000"}
	got := roundTrip(t, schema, input)
	if got.ID != input.ID {
		t.Fatalf("got %s, want %s", got.ID, input.ID)
	}
}

// ---- Coverage: UUID on fixed(16) in record (unsafe returns nil, default fixed path) ----

func TestUUIDFixed16InRecord(t *testing.T) {
	type R struct {
		ID [16]byte `avro:"id"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"id","type":{"type":"fixed","name":"uuid","size":16,"logicalType":"uuid"}}
	]}`
	input := R{ID: [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}}
	got := roundTrip(t, schema, input)
	if got.ID != input.ID {
		t.Fatalf("got %x, want %x", got.ID, input.ID)
	}
}

// ---- Coverage: array/map block count exceeding buffer ----

func TestArrayBlockCountExceedsBuffer(t *testing.T) {
	s, _ := Parse(`{"type":"array","items":"int"}`)
	// Manually craft: block count = 1000 (varint), no items.
	buf := appendVarlong(nil, 1000) // count=1000 but no data
	var v []int32
	_, err := s.Decode(buf, &v)
	if err == nil {
		t.Fatal("expected error for array block count exceeding buffer")
	}
}

func TestMapBlockCountExceedsBuffer(t *testing.T) {
	s, _ := Parse(`{"type":"map","values":"int"}`)
	buf := appendVarlong(nil, 1000)
	var v map[string]int32
	_, err := s.Decode(buf, &v)
	if err == nil {
		t.Fatal("expected error for map block count exceeding buffer")
	}
}

// ---- Coverage: deserTimestampNanos short buffer ----

func TestDeserTimestampNanosShortBuffer(t *testing.T) {
	s, _ := Parse(`{"type":"long","logicalType":"timestamp-nanos"}`)
	var v time.Time
	_, err := s.Decode(nil, &v) // empty buffer
	if err == nil {
		t.Fatal("expected error for short buffer")
	}
}

// ---- Coverage: deserUUID readVarlong error ----

func TestDeserUUIDShortBuffer(t *testing.T) {
	s, _ := Parse(`{"type":"string","logicalType":"uuid"}`)
	var v string
	_, err := s.Decode(nil, &v) // empty buffer
	if err == nil {
		t.Fatal("expected error for short buffer")
	}
}

// ---- Coverage: deserUUID TextUnmarshaler error ----

func TestDeserUUIDTextUnmarshalerError(t *testing.T) {
	s, _ := Parse(`{"type":"string","logicalType":"uuid"}`)
	input := "550e8400-e29b-41d4-a716-446655440000"
	encoded := encode(t, `{"type":"string","logicalType":"uuid"}`, &input)
	var v testTextUnmarshalerErr
	_, err := s.Decode(encoded, &v)
	if err == nil {
		t.Fatal("expected error from TextUnmarshaler")
	}
}

// ---- Coverage: decimal precision <= 0, scale > precision ----

func TestDecimalPrecisionZero(t *testing.T) {
	_, err := Parse(`{"type":"bytes","logicalType":"decimal","precision":0}`)
	if err == nil {
		t.Fatal("expected error for precision=0")
	}
}

func TestDecimalScaleExceedsPrecision(t *testing.T) {
	_, err := Parse(`{"type":"bytes","logicalType":"decimal","precision":5,"scale":6}`)
	if err == nil {
		t.Fatal("expected error for scale > precision")
	}
}

// ---- Coverage: serTimestampNanos nil pointer, serUUID nil pointer ----

func TestSerTimestampNanosNilPointer(t *testing.T) {
	s, _ := Parse(`{"type":"long","logicalType":"timestamp-nanos"}`)
	var p *time.Time
	_, err := s.Encode(p)
	if err == nil {
		t.Fatal("expected error for nil pointer")
	}
}

func TestSerUUIDNilPointer(t *testing.T) {
	s, _ := Parse(`{"type":"string","logicalType":"uuid"}`)
	var p *string
	_, err := s.Encode(p)
	if err == nil {
		t.Fatal("expected error for nil pointer")
	}
}

// ---- Coverage: duplicate union named type same name ----

func TestDuplicateUnionNamedTypeSameName(t *testing.T) {
	// Two different named records with the same name in a union.
	_, err := Parse(`[
		{"type":"record","name":"X","fields":[{"name":"a","type":"int"}]},
		{"type":"record","name":"X","fields":[{"name":"b","type":"string"}]}
	]`)
	if err == nil {
		t.Fatal("expected error for duplicate named union type")
	}
}

// ---- Coverage: unsafe udTimestampNanos error path ----

func TestUnsafeUdTimestampNanosError(t *testing.T) {
	type R struct {
		V time.Time `avro:"v"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"v","type":{"type":"long","logicalType":"timestamp-nanos"}}
	]}`
	s, _ := Parse(schema)
	var r R
	_, err := s.Decode(nil, &r) // empty buffer
	if err == nil {
		t.Fatal("expected error for short buffer on unsafe path")
	}
}

// ---- Coverage: unsafe udUUID error paths ----

func TestUnsafeUdUUIDShortBuffer(t *testing.T) {
	type R struct {
		ID [16]byte `avro:"id"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"id","type":{"type":"string","logicalType":"uuid"}}
	]}`
	s, _ := Parse(schema)
	var r R
	_, err := s.Decode(nil, &r)
	if err == nil {
		t.Fatal("expected error for short buffer")
	}
}

func TestUnsafeUdUUIDNegativeLength(t *testing.T) {
	type R struct {
		ID [16]byte `avro:"id"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"id","type":{"type":"string","logicalType":"uuid"}}
	]}`
	s, _ := Parse(schema)
	var r R
	_, err := s.Decode([]byte{0x01}, &r) // -1 zigzag
	if err == nil {
		t.Fatal("expected error for negative length")
	}
}

func TestUnsafeUdUUIDTooShort(t *testing.T) {
	type R struct {
		ID [16]byte `avro:"id"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"id","type":{"type":"string","logicalType":"uuid"}}
	]}`
	s, _ := Parse(schema)
	var r R
	_, err := s.Decode([]byte{72, 'a', 'b'}, &r) // length 36, only 2 bytes
	if err == nil {
		t.Fatal("expected error for short buffer")
	}
}

// ---- Coverage: uuid unsafe fallback nil (non-string, non-[16]byte struct field) ----

func TestUUIDUnsafeFallbackNil(t *testing.T) {
	type R struct {
		ID any `avro:"id"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"id","type":{"type":"string","logicalType":"uuid"}}
	]}`
	input := R{ID: "550e8400-e29b-41d4-a716-446655440000"}
	got := roundTrip(t, schema, input)
	if got.ID != input.ID {
		t.Fatalf("got %v, want %v", got.ID, input.ID)
	}
}

func TestUnsafeUdUUIDInvalidHex(t *testing.T) {
	type R struct {
		ID [16]byte `avro:"id"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"id","type":{"type":"string","logicalType":"uuid"}}
	]}`
	s, _ := Parse(schema)
	// Encode a valid-length but invalid-hex UUID string.
	bad := "ZZZZZZZZ-e29b-41d4-a716-446655440000"
	data := appendVarlong(nil, int64(len(bad)))
	data = append(data, bad...)
	var r R
	_, err := s.Decode(data, &r)
	if err == nil {
		t.Fatal("expected error for invalid UUID hex")
	}
}

func TestNullSecondUnion(t *testing.T) {
	// Test ["string", "null"] union (null-second).
	schema := `{"type":"record","name":"r","fields":[
		{"name":"val","type":["string","null"]}
	]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}

	type R struct {
		Val *string `avro:"val"`
	}

	// Non-nil value.
	str := "hello"
	input := R{Val: &str}
	dst, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatalf("encode non-nil: %v", err)
	}
	// Index 0 (string) encoded as varint 0x00.
	if dst[0] != 0 {
		t.Fatalf("expected index byte 0x00 for string, got 0x%02x", dst[0])
	}
	var out R
	rem, err := s.Decode(dst, &out)
	if err != nil {
		t.Fatalf("decode non-nil: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("leftover bytes: %d", len(rem))
	}
	if out.Val == nil || *out.Val != "hello" {
		t.Fatalf("expected 'hello', got %v", out.Val)
	}

	// Nil value.
	input = R{Val: nil}
	dst, err = s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatalf("encode nil: %v", err)
	}
	// Index 1 (null) encoded as varint 0x02.
	if dst[0] != 2 {
		t.Fatalf("expected index byte 0x02 for null, got 0x%02x", dst[0])
	}
	out = R{}
	rem, err = s.Decode(dst, &out)
	if err != nil {
		t.Fatalf("decode nil: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("leftover bytes: %d", len(rem))
	}
	if out.Val != nil {
		t.Fatalf("expected nil, got %v", *out.Val)
	}
}

func TestNullSecondUnionRoundTrip(t *testing.T) {
	// Test round-trip with various types in ["T", "null"] unions.
	schema := `{"type":"record","name":"r","fields":[
		{"name":"num","type":["int","null"]},
		{"name":"text","type":["string","null"]}
	]}`
	type R struct {
		Num  *int32  `avro:"num"`
		Text *string `avro:"text"`
	}

	n := int32(42)
	s := "hello"
	got := roundTrip(t, schema, R{Num: &n, Text: &s})
	if got.Num == nil || *got.Num != 42 {
		t.Fatalf("expected Num=42, got %v", got.Num)
	}
	if got.Text == nil || *got.Text != "hello" {
		t.Fatalf("expected Text='hello', got %v", got.Text)
	}

	// Both nil.
	got = roundTrip(t, schema, R{Num: nil, Text: nil})
	if got.Num != nil {
		t.Fatalf("expected Num=nil, got %v", *got.Num)
	}
	if got.Text != nil {
		t.Fatalf("expected Text=nil, got %v", *got.Text)
	}
}

func TestNullSecondUnionReflectPath(t *testing.T) {
	// Test ["int", "null"] union through the reflect slow path by passing
	// values directly (not through an addressable struct field).
	schema := `["int","null"]`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}

	// Non-nil value: pass int32 directly (no &).
	v := int32(42)
	dst, err := s.AppendEncode(nil, v)
	if err != nil {
		t.Fatalf("encode non-nil: %v", err)
	}
	if dst[0] != 0 {
		t.Fatalf("expected index byte 0x00 for int, got 0x%02x", dst[0])
	}
	var out any
	rem, err := s.Decode(dst, &out)
	if err != nil {
		t.Fatalf("decode non-nil: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("leftover bytes: %d", len(rem))
	}

	// Nil value: pass nil directly → reflect.ValueOf(nil) is invalid.
	dst, err = s.AppendEncode(nil, nil)
	if err != nil {
		t.Fatalf("encode nil: %v", err)
	}
	if dst[0] != 2 {
		t.Fatalf("expected index byte 0x02 for null, got 0x%02x", dst[0])
	}
	out = "not nil"
	rem, err = s.Decode(dst, &out)
	if err != nil {
		t.Fatalf("decode nil: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("leftover bytes: %d", len(rem))
	}
	if out != nil {
		t.Fatalf("expected nil, got %v", out)
	}

	// Nil slice (nilable but not pointer).
	var sl []int
	dst, err = s.AppendEncode(nil, sl)
	if err != nil {
		t.Fatalf("encode nil slice: %v", err)
	}
	if dst[0] != 2 {
		t.Fatalf("expected null index for nil slice, got 0x%02x", dst[0])
	}

	// Invalid index byte in deser.
	_, err = s.Decode([]byte{4}, &out)
	if err == nil {
		t.Fatal("expected error for invalid index byte")
	}

	// Short buffer in deser.
	_, err = s.Decode(nil, &out)
	if err == nil {
		t.Fatal("expected error for empty buffer")
	}
}

func TestNullSecondUnionPtrReflect(t *testing.T) {
	// Test the reflect Ptr path of deserNullSecondUnion by decoding
	// a ["int", "null"] union into a top-level *int32.
	schema := `["int","null"]`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}

	// Encode non-nil: index 0 (int), value 42.
	var v *int32
	dst, err := s.AppendEncode(nil, int32(42))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rem, err := s.Decode(dst, &v)
	if err != nil {
		t.Fatalf("decode non-nil: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("leftover bytes: %d", len(rem))
	}
	if v == nil || *v != 42 {
		t.Fatalf("expected 42, got %v", v)
	}

	// Encode nil: index 1 (null).
	dst, err = s.AppendEncode(nil, nil)
	if err != nil {
		t.Fatalf("encode nil: %v", err)
	}
	v = new(int32) // pre-allocate to test zeroing
	rem, err = s.Decode(dst, &v)
	if err != nil {
		t.Fatalf("decode nil: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("leftover bytes: %d", len(rem))
	}
	if v != nil {
		t.Fatalf("expected nil, got %v", *v)
	}
}

func TestFixedSliceRoundTrip(t *testing.T) {
	// Verify that fixed-type values survive encode as []byte → decode as []byte.
	schema := `{"type":"record","name":"r","fields":[
		{"name":"data","type":{"type":"fixed","name":"f","size":4}}
	]}`
	type R struct {
		Data []byte `avro:"data"`
	}
	got := roundTrip(t, schema, R{Data: []byte{0xDE, 0xAD, 0xBE, 0xEF}})
	if len(got.Data) != 4 || got.Data[0] != 0xDE || got.Data[3] != 0xEF {
		t.Fatalf("unexpected data: %x", got.Data)
	}
}
