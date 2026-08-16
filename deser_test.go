package avro

import (
	"bytes"
	"context"
	"encoding"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"
)

// ---------- deser_test.go ----------

// ptr returns a pointer to v. Used in tests instead of Go 1.26's new(v).
func ptr[T any](v T) *T { return &v }

func roundTrip[T any](t *testing.T, schema string, input T) T {
	t.Helper()
	s := mustParse(t, schema)
	encoded := mustAppendEncode(t, s, nil, &input)
	var output T
	rem := mustDecode(t, s, encoded, &output)
	if len(rem) != 0 {
		t.Fatalf("Decode left %d unconsumed bytes", len(rem))
	}
	return output
}

// encode is a test helper that encodes v with schema and returns the raw bytes.
func encode(t *testing.T, schema string, v any) []byte {
	t.Helper()
	s := mustParse(t, schema)
	dst := mustAppendEncode(t, s, nil, v)
	return dst
}

// decode is a test helper that decodes src into v with schema.
func decode(t *testing.T, schema string, src []byte, v any) {
	t.Helper()
	s := mustParse(t, schema)
	rem := mustDecode(t, s, src, v)
	if len(rem) != 0 {
		t.Fatalf("Decode left %d unconsumed bytes", len(rem))
	}
}

// decodeErr is a test helper that expects Decode to return an error.
func decodeErr(t *testing.T, schema string, src []byte, v any) {
	t.Helper()
	s := mustParse(t, schema)
	_, err := s.Decode(src, v)
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
	dst := encode(t, `"boolean"`, ptr(true))
	if !bytes.Equal(dst, []byte{0x01}) {
		t.Fatalf("encode true: got %x, want 01", dst)
	}
	dst = encode(t, `"boolean"`, ptr(false))
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
	dst := encode(t, `"int"`, ptr(int32(27)))
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
	dst := encode(t, `"long"`, ptr(int64(2147483648)))
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
	dst := encode(t, `"float"`, ptr(float32(1.15)))
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
	dst := encode(t, `"double"`, ptr(float64(1.15)))
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
	dst := encode(t, `"string"`, ptr("foo"))
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

func TestRoundTripPrimitives(t *testing.T) {
	t.Run("boolean", func(t *testing.T) {
		for _, v := range []bool{true, false} {
			got := roundTrip(t, `"boolean"`, v)
			if got != v {
				t.Errorf("got %v, want %v", got, v)
			}
		}
	})
	t.Run("int", func(t *testing.T) {
		for _, v := range []int32{0, 1, -1, 127, -128, math.MaxInt32, math.MinInt32} {
			got := roundTrip(t, `"int"`, v)
			if got != v {
				t.Errorf("got %v, want %v", got, v)
			}
		}
	})
	t.Run("long", func(t *testing.T) {
		for _, v := range []int64{0, 1, -1, math.MaxInt64, math.MinInt64} {
			got := roundTrip(t, `"long"`, v)
			if got != v {
				t.Errorf("got %v, want %v", got, v)
			}
		}
	})
	t.Run("float", func(t *testing.T) {
		for _, v := range []float32{0, 1.5, -1.5, math.MaxFloat32, math.SmallestNonzeroFloat32, float32(math.Inf(1)), float32(math.Inf(-1))} {
			got := roundTrip(t, `"float"`, v)
			if got != v {
				t.Errorf("got %v, want %v", got, v)
			}
		}
		// NaN != NaN, so test separately.
		got := roundTrip(t, `"float"`, float32(math.NaN()))
		if !math.IsNaN(float64(got)) {
			t.Errorf("NaN round-trip: got %v", got)
		}
	})
	t.Run("double", func(t *testing.T) {
		for _, v := range []float64{0, 1.5, -1.5, math.MaxFloat64, math.SmallestNonzeroFloat64, math.Inf(1), math.Inf(-1)} {
			got := roundTrip(t, `"double"`, v)
			if got != v {
				t.Errorf("got %v, want %v", got, v)
			}
		}
		got := roundTrip(t, `"double"`, math.NaN())
		if !math.IsNaN(got) {
			t.Errorf("NaN round-trip: got %v", got)
		}
	})
	t.Run("bytes", func(t *testing.T) {
		for _, v := range [][]byte{{}, {0}, {1, 2, 3}, make([]byte, 256)} {
			got := roundTrip(t, `"bytes"`, v)
			if !reflect.DeepEqual(got, v) {
				t.Errorf("got %v, want %v", got, v)
			}
		}
	})
	t.Run("string", func(t *testing.T) {
		for _, v := range []string{"", "hello", "hello world", "日本語"} {
			got := roundTrip(t, `"string"`, v)
			if got != v {
				t.Errorf("got %q, want %q", got, v)
			}
		}
	})
}

func TestRoundTripNull(t *testing.T) {
	s := mustParse(t, `"null"`)
	var p *int
	encoded := mustAppendEncode(t, s, nil, p)
	if len(encoded) != 0 {
		t.Fatalf("null should encode to empty bytes, got %v", encoded)
	}
	var p2 *int
	rem := mustDecode(t, s, encoded, &p2)
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
	got := roundTrip(t, superheroUnionSchema, wolverine())

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
	schema := enumColorSchema
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
	schema := enumColorSchema
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
	schema := nullableIntSchema

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
	schema := longListAliasSchema
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
		S fmt.Stringer `avro:"s"`
	}
	schema := ifaceFoobarSchema
	s := mustParse(t, schema)
	input := Iface{S: &IfaceF{F: 3}}
	encoded := mustAppendEncode(t, s, nil, &input)

	// Deserialize into a struct where the interface field is pre-set
	// with the concrete pointer type.
	output := Iface{S: &IfaceF{}}
	rem := mustDecode(t, s, encoded, &output)
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
		{"fixed uuid", `{"type":"fixed","name":"u","size":16,"logicalType":"uuid"}`, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06}}, // need 16, have 6
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
				// Decode into any — hits reflect path.
				_, err = s.Decode(tt.data, &v)
			}
			_ = v
			if err == nil {
				t.Fatal("expected error for short buffer, got nil")
			}
		})
	}

	// Also test fixed UUID short buffer with typed targets.
	t.Run("fixed uuid [16]byte", func(t *testing.T) {
		s := MustParse(`{"type":"fixed","name":"u","size":16,"logicalType":"uuid"}`)
		var u [16]byte
		_, err := s.Decode([]byte{0x01, 0x02, 0x03}, &u)
		if err == nil {
			t.Fatal("expected error for short buffer")
		}
	})
	t.Run("fixed uuid string", func(t *testing.T) {
		s := MustParse(`{"type":"fixed","name":"u","size":16,"logicalType":"uuid"}`)
		var str string
		_, err := s.Decode([]byte{0x01, 0x02, 0x03}, &str)
		if err == nil {
			t.Fatal("expected error for short buffer")
		}
	})
	// Test short buffer through struct decode (hits unsafe fast path).
	t.Run("fixed uuid in struct short", func(t *testing.T) {
		type R struct {
			ID [16]byte `avro:"id"`
		}
		s := MustParse(recUUIDFixedSchema)
		var r R
		_, err := s.Decode([]byte{0x01, 0x02, 0x03}, &r)
		if err == nil {
			t.Fatal("expected error for short buffer")
		}
	})
	t.Run("fixed uuid in struct string short", func(t *testing.T) {
		type R struct {
			ID string `avro:"id"`
		}
		s := MustParse(recUUIDFixedSchema)
		var r R
		_, err := s.Decode([]byte{0x01, 0x02, 0x03}, &r)
		if err == nil {
			t.Fatal("expected error for short buffer")
		}
	})
	t.Run("fixed uuid in struct string bad encode", func(t *testing.T) {
		type R struct {
			ID string `avro:"id"`
		}
		s := MustParse(recUUIDFixedSchema)
		_, err := s.Encode(&R{ID: "not-a-uuid"})
		if err == nil {
			t.Fatal("expected error for bad UUID string")
		}
	})
}

func TestDecodeTypeMismatch(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		data   []byte
		target any
	}{
		{"bool into string", `"boolean"`, []byte{0x01}, ptr("")},
		{"int into bool", `"int"`, []byte{0x36}, ptr(false)},
		{"string into int", `"string"`, []byte{0x06, 0x66, 0x6f, 0x6f}, ptr(int32(0))},
		// "int into float" used to be pinned as rejection; now
		// supported (round-trip parity with the documented encode-side
		// whole-number-float-as-int divergence). See
		// TestMatrix_IntLongDecodeIntoFloatJSONNumber.
		{"fixed into int array", `{"type":"fixed","name":"f","size":6}`, []byte{1, 2, 3, 4, 5, 6}, ptr([6]int{})},
		{"array into string", `{"type":"array","items":"int"}`, []byte{0x00}, ptr("")},
		{"map into string", `{"type":"map","values":"int"}`, []byte{0x00}, ptr("")},
		{"map with int key", `{"type":"map","values":"int"}`, []byte{0x00}, ptr(map[int]int32{})},
		{"record into int", `{"type":"record","name":"r","fields":[{"name":"a","type":"int"}]}`, []byte{0x02}, ptr(int32(0))},
		{"float into string", `"float"`, []byte{0, 0, 0, 0}, ptr("")},
		{"double into string", `"double"`, []byte{0, 0, 0, 0, 0, 0, 0, 0}, ptr("")},
		{"boolean into int", `"boolean"`, []byte{0x01}, ptr(int32(0))},
		{"long into bool", `"long"`, []byte{0x02}, ptr(false)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustParse(t, tt.schema)
			_, err := s.Decode(tt.data, tt.target)
			if err == nil {
				t.Fatal("expected type mismatch error, got nil")
			}
		})
	}
}

func TestDecodeInvalidUnionIndex(t *testing.T) {
	// Union ["null", "string"] only has indices 0 and 1; index 2 is out of range.
	decodeErr(t, `["null","string"]`, []byte{0x04}, ptr((*string)(nil))) // zigzag 4 → 2
}

func TestDecodeInvalidEnumIndex(t *testing.T) {
	// Enum with 2 symbols; index 2 is out of range.
	schema := `{"type":"enum","name":"e","symbols":["a","b"]}`
	decodeErr(t, schema, []byte{0x04}, ptr("")) // zigzag 4 → 2
}

func TestDecodeNonPointer(t *testing.T) {
	s := mustParse(t, `"int"`)
	_, err := s.Decode([]byte{0x02}, 42) // not a pointer
	if err == nil {
		t.Fatal("expected error for non-pointer, got nil")
	}
}

// -----------------------------------------------------------------------
// Edge case tests ported from hamba/avro patterns.
// -----------------------------------------------------------------------

func TestDecodeRecordNilPointer(t *testing.T) {
	// Decode into **Record where the inner pointer is nil → allocate through it.
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
	schema := `["null",{"type":"record","name":"r","fields":[
		{"name":"a","type":"long"},{"name":"b","type":"string"}
	]}]`
	data := []byte{0x02, 0x36, 0x06, 0x66, 0x6F, 0x6F} // index=1, a=27, b="foo"

	original := ptr(R{})
	got := original
	s := mustParse(t, schema)
	mustDecode(t, s, data, &got)
	if got != original {
		t.Error("expected pointer reuse, got new allocation")
	}
	if got.A != 27 || got.B != "foo" {
		t.Errorf("got %+v, want {A:27 B:foo}", got)
	}
}

func TestDecodeRecordIntoMap(t *testing.T) {
	// Decode a record schema into map[string]any.
	schema := recLongBSchema
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
	schema := recLongBSchema
	decodeErr(t, schema, []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}, ptr(map[int]any{}))
}

func TestDecodeRecordMapInvalidElem(t *testing.T) {
	// map[string]string cannot hold a long value.
	schema := recLongBSchema
	decodeErr(t, schema, []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}, ptr(map[string]string{}))
}

func TestDecodeRecordMapInvalidData(t *testing.T) {
	schema := recLongBSchema
	// Corrupt varint for field "a".
	decodeErr(t, schema, []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}, ptr(map[string]any{}))
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
	decodeErr(t, schema, data, ptr([]string{}))
}

func TestDecodeArrayBlockError(t *testing.T) {
	// Array with corrupt block count.
	schema := `{"type":"array","items":"int"}`
	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}
	decodeErr(t, schema, data, ptr([]int32{}))
}

func TestDecodeArrayOversizedCountUnsafe(t *testing.T) {
	// A crafted array block count that exceeds the buffer length.
	// This tests the unsafe fast path (udArrayDirect and udArrayPtrRecord)
	// to ensure they don't allocate based on untrusted counts.
	schema := `{"type":"array","items":"int"}`
	s := mustParse(t, schema)
	// count=999999 (zigzag-encoded), but only a few bytes of data.
	var data []byte
	data = appendVarlong(data, 999999)
	data = append(data, 0x00, 0x00)

	// Decode into []int32 — triggers the unsafe primitive array path.
	var sl []int32
	_, err := s.Decode(data, &sl)
	if err == nil {
		t.Fatal("expected error for oversized array count")
	}

	// Also test with struct containing a slice (triggers unsafe struct fast path).
	type R struct {
		A []int32 `avro:"a"`
	}
	rs, _ := Parse(`{"type":"record","name":"r","fields":[{"name":"a","type":{"type":"array","items":"int"}}]}`)
	var rdata []byte
	rdata = appendVarlong(rdata, 999999)
	rdata = append(rdata, 0x00, 0x00)
	var r R
	_, err = rs.Decode(rdata, &r)
	if err == nil {
		t.Fatal("expected error for oversized array count in struct")
	}
}

func TestDecodeMapValueShortRead(t *testing.T) {
	// Map with truncated value data.
	schema := `{"type":"map","values":"string"}`
	// count=1 (0x02), key="foo" (06 66 6f 6f), value=corrupt (06 06)
	data := []byte{0x02, 0x06, 0x66, 0x6F, 0x6F, 0x06, 0x06}
	decodeErr(t, schema, data, ptr(map[string]string{}))
}

func TestDecodeMapBlockError(t *testing.T) {
	// Map with corrupt block count.
	schema := `{"type":"map","values":"string"}`
	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}
	decodeErr(t, schema, data, ptr(map[string]string{}))
}

func TestDecodeEnumCorruptVarint(t *testing.T) {
	schema := `{"type":"enum","name":"e","symbols":["a","b","c"]}`
	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD}
	decodeErr(t, schema, data, ptr(""))
}

func TestDecodeRecordStructInvalidData(t *testing.T) {
	schema := recLongBSchema
	// Corrupt varint.
	decodeErr(t, schema, []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}, ptr(R{}))
}

func TestDecodeUnionIntoInterface(t *testing.T) {
	// Decode union ["null","int"] into any.
	schema := `["null","int"]`

	// index=1, int 27 (0x36)
	var got any
	s := mustParse(t, schema)
	mustDecode(t, s, []byte{0x02, 0x36}, &got)
	if got != int32(27) {
		t.Errorf("got %v (%T), want int32(27)", got, got)
	}

	// index=0 (null)
	got = "preallocated"
	mustDecode(t, s, []byte{0x00}, &got)
	if got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

func TestDecodeNegativeStringLength(t *testing.T) {
	// Negative length for string should error.
	decodeErr(t, `"string"`, []byte{0x01}, ptr("")) // zigzag 1 → -1
}

func TestDecodeNegativeBytesLength(t *testing.T) {
	// Negative length for bytes should error.
	decodeErr(t, `"bytes"`, []byte{0x01}, ptr([]byte{})) // zigzag 1 → -1
}

func TestDecodeFloatIntoInterface(t *testing.T) {
	s := mustParse(t, `"float"`)
	encoded := mustAppendEncode(t, s, nil, ptr(float32(3.14)))
	var got any
	mustDecode(t, s, encoded, &got)
	f, ok := got.(float32)
	if !ok {
		t.Fatalf("expected float32, got %T", got)
	}
	if f < 3.13 || f > 3.15 {
		t.Errorf("got %v, want ~3.14", f)
	}
}

func TestDecodeDoubleIntoInterface(t *testing.T) {
	s := mustParse(t, `"double"`)
	encoded := mustAppendEncode(t, s, nil, ptr(float64(2.718)))
	var got any
	mustDecode(t, s, encoded, &got)
	f, ok := got.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", got)
	}
	if f < 2.717 || f > 2.719 {
		t.Errorf("got %v, want ~2.718", f)
	}
}

func TestDecodeBytesIntoInterface(t *testing.T) {
	s := mustParse(t, `"bytes"`)
	encoded := mustAppendEncode(t, s, nil, ptr([]byte{1, 2, 3}))
	var got any
	mustDecode(t, s, encoded, &got)
	b, ok := got.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", got)
	}
	if !bytes.Equal(b, []byte{1, 2, 3}) {
		t.Errorf("got %v", b)
	}
}

func TestDecodeBytesIntoArray(t *testing.T) {
	s := mustParse(t, `"bytes"`)
	encoded := mustAppendEncode(t, s, nil, ptr([]byte{1, 2, 3}))
	var got [3]byte
	mustDecode(t, s, encoded, &got)
	if got != [3]byte{1, 2, 3} {
		t.Errorf("got %v", got)
	}
}

func TestDecodeBytesArrayWrongLength(t *testing.T) {
	s := mustParse(t, `"bytes"`)
	encoded := mustAppendEncode(t, s, nil, ptr([]byte{1, 2, 3}))
	var got [5]byte
	_, err := s.Decode(encoded, &got)
	if err == nil {
		t.Fatal("expected error for wrong array length")
	}
}

func TestDecodeBytesNonUint8Slice(t *testing.T) {
	decodeErr(t, `"bytes"`, []byte{0x04, 0x01, 0x02}, ptr([]int32{}))
}

func TestDecodeBytesNonUint8Array(t *testing.T) {
	decodeErr(t, `"bytes"`, []byte{0x04, 0x01, 0x02}, ptr([2]int32{}))
}

func TestDecodeEnumIntoUint(t *testing.T) {
	schema := `{"type":"enum","name":"e","symbols":["a","b","c"]}`
	s := mustParse(t, schema)
	// Encode "b" (index 1).
	encoded := mustAppendEncode(t, s, nil, ptr("b"))
	var got uint32
	mustDecode(t, s, encoded, &got)
	if got != 1 {
		t.Errorf("got %d, want 1", got)
	}
}

func TestDecodeEnumTypeMismatch(t *testing.T) {
	schema := `{"type":"enum","name":"e","symbols":["a","b"]}`
	decodeErr(t, schema, []byte{0x00}, ptr(false))
}

func TestDecodeFixedIntoInterface(t *testing.T) {
	s := mustParse(t, `{"type":"fixed","name":"f","size":4}`)
	data := []byte{1, 2, 3, 4}
	var got any
	mustDecode(t, s, data, &got)
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
	decodeErr(t, `{"type":"fixed","name":"f","size":4}`, []byte{1, 2, 3, 4}, ptr(int32(0)))
}

func TestDecodeFixedWrongSize(t *testing.T) {
	// Fixed of size 4 into a [3]byte array.
	decodeErr(t, `{"type":"fixed","name":"f","size":4}`, []byte{1, 2, 3, 4}, ptr([3]byte{}))
}

func TestDecodeMapIntoInterface(t *testing.T) {
	schema := `{"type":"map","values":"int"}`
	s := mustParse(t, schema)
	input := map[string]int32{"a": 1, "b": 2}
	encoded := mustAppendEncode(t, s, nil, &input)
	var got any
	mustDecode(t, s, encoded, &got)
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
	s := mustParse(t, schema)
	input := []string{"hello", "world"}
	encoded := mustAppendEncode(t, s, nil, &input)
	var got any
	mustDecode(t, s, encoded, &got)
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
	s := mustParse(t, schema)
	encoded := mustAppendEncode(t, s, nil, ptr("b"))
	var got any
	mustDecode(t, s, encoded, &got)
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
	decodeErr(t, schema, []byte{0x02, 0x02, 0x78}, ptr(R{}))
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
	decodeErr(t, `["null","int"]`, []byte{0xE6, 0xA2}, ptr((*int32)(nil)))
}

func TestDecodeMapNegativeBlockCount(t *testing.T) {
	// Negative block count means the absolute value is the count,
	// followed by a block size long, then entries.
	schema := `{"type":"map","values":"int"}`
	s := mustParse(t, schema)
	// Manually build: count=-1 (zigzag 1), block_size=3 (zigzag 6), key="a" (02 61), value=1 (02), terminator 0
	data := []byte{0x01, 0x06, 0x02, 0x61, 0x02, 0x00}
	var got map[string]int32
	mustDecode(t, s, data, &got)
	if got["a"] != 1 {
		t.Errorf("got %v", got)
	}
}

func TestDecodeArrayNegativeBlockCount(t *testing.T) {
	schema := `{"type":"array","items":"int"}`
	s := mustParse(t, schema)
	// count=-2 (zigzag 3), block_size=2 (zigzag 4), items: 1 (02), 2 (04), terminator 0
	data := []byte{0x03, 0x04, 0x02, 0x04, 0x00}
	var got []int32
	mustDecode(t, s, data, &got)
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Errorf("got %v", got)
	}
}

func TestDecodeMapKeyInvalid(t *testing.T) {
	schema := `{"type":"map","values":"int"}`
	// count=1 (0x02), key length = -1 (zigzag 0x01)
	decodeErr(t, schema, []byte{0x02, 0x01}, ptr(map[string]int32{}))
}

func TestTypeFieldMappingAvroSkip(t *testing.T) {
	// Fields tagged avro:"-" are skipped, both for regular and embedded fields.
	type R struct {
		Embed
		B       string `avro:"b"`
		Ignored int    `avro:"-"`
	}
	schema := recIntBSchema
	input := R{Embed: Embed{A: 42}, B: "hello"}
	got := roundTrip(t, schema, input)
	if got.A != 42 || got.B != "hello" {
		t.Errorf("got %+v", got)
	}
}

func TestTypeFieldMappingEmbedWithTag(t *testing.T) {
	// Embedded struct with explicit avro tag is treated as a named field.
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
		a int32
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

func TestTypeFieldMappingSameDepthTaggedBeatsUntagged(t *testing.T) {
	// Two same-depth fields resolving to one name with DIFFERENT tagged
	// status are a tag tiebreak, not an ambiguous collision: the tagged
	// field wins. Ambiguity is reserved for same-depth fields with the
	// SAME tagged status (the lazy-error pins in embed_selection_test.go).
	type EmbA struct {
		Val string `avro:"Name"` // tagged, depth 2
	}
	type EmbB struct {
		Name string // untagged, depth 2, Go field name "Name"
	}
	type R struct {
		EmbA
		EmbB
	}
	schema := `{"type":"record","name":"r","fields":[{"name":"Name","type":"string"}]}`
	input := R{EmbA: EmbA{Val: "tagged"}, EmbB: EmbB{Name: "untagged"}}
	got := roundTrip(t, schema, input)
	if got.EmbA.Val != "tagged" {
		t.Errorf("expected the tagged field to win the same-depth tiebreak, got %+v", got)
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
		myString
		B int32 `avro:"b"`
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
	s := mustParse(t, `"boolean"`)
	encoded := mustAppendEncode(t, s, nil, ptr(true))
	var got any
	mustDecode(t, s, encoded, &got)
	if got != true {
		t.Errorf("got %v, want true", got)
	}
}

func TestDecodeLongIntoUint(t *testing.T) {
	s := mustParse(t, `"long"`)
	encoded := mustAppendEncode(t, s, nil, ptr(int64(42)))
	var got uint64
	mustDecode(t, s, encoded, &got)
	if got != 42 {
		t.Errorf("got %d, want 42", got)
	}
}

func TestDecodeBytesShortRead(t *testing.T) {
	// Corrupt varlong for bytes length.
	decodeErr(t, `"bytes"`, []byte{0xE6, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}, ptr([]byte{}))
}

func TestDecodeRecordIntoInterfaceWithError(t *testing.T) {
	// Record decoded into any, but with corrupt field data.
	schema := recLongBSchema
	// Corrupt varlong for field "a".
	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}
	var got any
	decodeErr(t, schema, data, &got)
}

func TestDecodeRecordIntoInterfaceSuccess(t *testing.T) {
	// Record decoded into any should produce map[string]any.
	schema := recIntBSchema
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
	decodeErr(t, schema, data, ptr([]int32{}))
}

func TestDecodeArrayCapGrowth(t *testing.T) {
	// Test that the array can grow from pre-allocated to larger.
	schema := `{"type":"array","items":"int"}`
	s := mustParse(t, schema)
	input := []int32{1, 2, 3, 4, 5}
	encoded := mustAppendEncode(t, s, nil, &input)
	// Decode into pre-allocated slice with smaller cap.
	got := make([]int32, 0, 2)
	mustDecode(t, s, encoded, &got)
	if len(got) != 5 {
		t.Errorf("got len %d, want 5", len(got))
	}
}

func TestDecodeArrayExistingCap(t *testing.T) {
	// Decode into slice with sufficient capacity (SetLen path).
	schema := `{"type":"array","items":"int"}`
	s := mustParse(t, schema)
	input := []int32{1, 2, 3}
	encoded := mustAppendEncode(t, s, nil, &input)
	got := make([]int32, 0, 10) // plenty of cap
	mustDecode(t, s, encoded, &got)
	if len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Errorf("got %v", got)
	}
}

func TestDecodeArrayIntoFixedArray(t *testing.T) {
	schema := `{"type":"array","items":"int"}`
	s := mustParse(t, schema)
	input := []int32{10, 20, 30}
	encoded := mustAppendEncode(t, s, nil, &input)
	var got [3]int32
	mustDecode(t, s, encoded, &got)
	if got != [3]int32{10, 20, 30} {
		t.Errorf("got %v, want [10 20 30]", got)
	}
}

func TestDecodeArrayIntoFixedArrayTooFew(t *testing.T) {
	schema := `{"type":"array","items":"int"}`
	s := mustParse(t, schema)
	input := []int32{10, 20} // 2 elements
	encoded := mustAppendEncode(t, s, nil, &input)
	var got [3]int32 // expects 3
	_, err := s.Decode(encoded, &got)
	if err == nil {
		t.Fatal("expected error for too few elements")
	}
}

func TestDecodeArrayIntoFixedArrayTooMany(t *testing.T) {
	schema := `{"type":"array","items":"int"}`
	s := mustParse(t, schema)
	input := []int32{10, 20, 30, 40} // 4 elements
	encoded := mustAppendEncode(t, s, nil, &input)
	var got [3]int32 // expects 3
	_, err := s.Decode(encoded, &got)
	if err == nil {
		t.Fatal("expected error for too many elements")
	}
}

func TestDecodeMapNegativeBlockShortRead(t *testing.T) {
	// Negative block count with truncated block size.
	schema := `{"type":"map","values":"int"}`
	data := []byte{0x01, 0xE6} // count=-1 (zigzag 0x01), then truncated
	decodeErr(t, schema, data, ptr(map[string]int32{}))
}

func TestDecodeMapKeyLengthShortRead(t *testing.T) {
	// Map key length read fails.
	schema := `{"type":"map","values":"int"}`
	// count=1 (0x02), then truncated key length
	data := []byte{0x02, 0xE6, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}
	decodeErr(t, schema, data, ptr(map[string]int32{}))
}

func TestVarint4Byte(t *testing.T) {
	// Values in the range [1<<21, 1<<28) encode as 4-byte uvarints.
	// int32(1<<21) = 2097152, zigzag = 4194304 = 0x400000
	s := mustParse(t, `"int"`)
	v := int32(1 << 21)
	encoded := mustAppendEncode(t, s, nil, &v)
	if len(encoded) != 4 {
		t.Fatalf("expected 4-byte varint, got %d bytes: %x", len(encoded), encoded)
	}
	var got int32
	mustDecode(t, s, encoded, &got)
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
		{"int8", ptr(int8(27)), 27},
		{"int16", ptr(int16(27)), 27},
		{"int32", ptr(int32(27)), 27},
		{"uint8", ptr(uint8(27)), 27},
		{"uint16", ptr(uint16(27)), 27},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := mustParse(t, schema)
			dst := mustAppendEncode(t, s, nil, tc.v)
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
		{"int32", ptr(int32(27))},
		{"int64", ptr(int64(27))},
		{"uint32", ptr(uint32(27))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := mustParse(t, schema)
			dst := mustAppendEncode(t, s, nil, tc.v)
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
	superhero := wolverine()

	s, err := Parse(superheroUnionSchema)
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

	s, err := Parse(longListAliasSchema)
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
	s := mustParse(b, primsSchema)

	input := Prims{B: true, I: 42, L: 123456789, F: 3.14, D: 2.718281828, S: "hello world", Bs: []byte{1, 2, 3, 4, 5}}
	encoded := mustAppendEncode(b, s, nil, &input)

	var out Prims
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = Prims{}
		mustDecode(b, s, encoded, &out)
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
	s := mustParse(b, primsSchema)

	input := Prims{B: true, I: 42, L: 123456789, F: 3.14, D: 2.718281828, S: "hello world", Bs: []byte{1, 2, 3, 4, 5}}
	dst, _ := s.AppendEncode(nil, &input)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = mustAppendEncode(b, s, dst[:0], &input)
	}
}

func BenchmarkDeserializeGeneric(b *testing.B) {
	superhero := wolverine()

	s, err := Parse(superheroUnionSchema)
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
			s := mustParse(t, schema)
			dst := mustAppendEncode(t, s, nil, input)
			mustDecode(t, s, dst, output)
			got := reflect.ValueOf(output).Elem().Field(0).Int()
			if got != want {
				t.Fatalf("got %d, want %d", got, want)
			}
		})
	}

	checkU := func(t *testing.T, name string, input, output any, want uint64) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			s := mustParse(t, schema)
			dst := mustAppendEncode(t, s, nil, input)
			mustDecode(t, s, dst, output)
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
	s := mustParse(t, schema)
	v := int32(42)
	in := R{V: &v}
	dst := mustAppendEncode(t, s, nil, &in)
	// Decode back — pointer fields go through slow path for deser.
	var out R
	mustDecode(t, s, dst, &out)
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
	s := mustParse(t, schema)
	inner := []int32{1, 2, 3}
	in := R{V: &inner}
	dst := mustAppendEncode(t, s, nil, &in)
	var out R
	mustDecode(t, s, dst, &out)
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
	s := mustParse(t, schema)
	dst := mustAppendEncode(t, s, nil, ptr(int32(42)))
	var got uint32
	if _, err := s.Decode(dst, &got); err != nil {
		t.Fatalf("decode uint32: %v", err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
}

func TestDecodeLongUint(t *testing.T) {
	schema := `"long"`
	s := mustParse(t, schema)
	dst := mustAppendEncode(t, s, nil, ptr(int64(42)))
	var got uint64
	if _, err := s.Decode(dst, &got); err != nil {
		t.Fatalf("decode uint64: %v", err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
}

// numericKindTestType returns a reflect.Type whose Kind is k, for the unsafe
// per-kind ser/deser constructors (usInt/usLong/udInt/udLong/udDouble) which
// take the field's reflect.Type so SemanticError.GoType matches the reflect
// path. Test-only helper.
func numericKindTestType(k reflect.Kind) reflect.Type {
	switch k {
	case reflect.Bool:
		return reflect.TypeOf(false)
	case reflect.Int:
		return reflect.TypeOf(int(0))
	case reflect.Int8:
		return reflect.TypeOf(int8(0))
	case reflect.Int16:
		return reflect.TypeOf(int16(0))
	case reflect.Int32:
		return reflect.TypeOf(int32(0))
	case reflect.Int64:
		return reflect.TypeOf(int64(0))
	case reflect.Uint:
		return reflect.TypeOf(uint(0))
	case reflect.Uint8:
		return reflect.TypeOf(uint8(0))
	case reflect.Uint16:
		return reflect.TypeOf(uint16(0))
	case reflect.Uint32:
		return reflect.TypeOf(uint32(0))
	case reflect.Uint64:
		return reflect.TypeOf(uint64(0))
	case reflect.Float32:
		return reflect.TypeOf(float32(0))
	case reflect.Float64:
		return reflect.TypeOf(float64(0))
	}
	return nil
}

// TestUnsafeSerializeDefaults covers usInt/usLong/usFloat/usDouble returning
// nil for unsupported Go kinds (the default: branches).
func TestUnsafeSerializeDefaults(t *testing.T) {
	if usInt(numericKindTestType(reflect.Bool)) != nil {
		t.Fatal("usInt(Bool) should be nil")
	}
	if usLong(numericKindTestType(reflect.Bool)) != nil {
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
	if udInt(numericKindTestType(reflect.Bool)) != nil {
		t.Fatal("udInt(Bool) should be nil")
	}
	if udLong(numericKindTestType(reflect.Bool)) != nil {
		t.Fatal("udLong(Bool) should be nil")
	}
	if udFloat(reflect.Bool) != nil {
		t.Fatal("udFloat(Bool) should be nil")
	}
	if udDouble(numericKindTestType(reflect.Bool)) != nil {
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
		fn := udInt(numericKindTestType(k))
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
		fn := udLong(numericKindTestType(k))
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
		fn := udDouble(numericKindTestType(k))
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

// TestMatrix_UnionEmitTagsAreFullLengthAtEveryBranch pins that a union's two
// emit-tag tables carry an entry for every branch, by decoding through every
// branch index of every union shape with the tagging options on.
//
// maybeWrap picks branchNames or logicalNames by the wire's branch index and
// indexes it without a length check, so a table that skipped entries — for
// branches with no logical type, or whose qualified spelling another branch
// already owns — would panic on exactly the branches it skipped. The axes are
// the union's shape, the branch actually on the wire, and how the union was
// compiled: directly by a parse, or by a resolution, which builds these tables
// at three separate sites of its own.
//
// The assertion is answerable from the input: the options say a decoded union
// value is a one-key envelope, so any branch that comes back bare or panics is
// wrong whatever the other branches did.
func TestMatrix_UnionEmitTagsAreFullLengthAtEveryBranch(t *testing.T) {
	shapes := []struct {
		name   string
		union  string
		values []any    // one per branch, in branch order
		tags   []string // the tag each branch must emit; "" for the null branch
	}{
		{"null-and-int", `["null","int"]`,
			[]any{nil, int32(1)}, []string{"", "int"}},
		{"no-logical-anywhere", `["null","int","string","boolean"]`,
			[]any{nil, int32(1), "s", true}, []string{"", "int", "string", "boolean"}},
		{"logical-on-some", `["null","int",{"type":"long","logicalType":"timestamp-millis"},"string"]`,
			[]any{nil, int32(1), int64(1000), "s"},
			[]string{"", "int", "long.timestamp-millis", "string"}},
		{"logical-first", `[{"type":"long","logicalType":"timestamp-millis"},"null","int"]`,
			[]any{int64(1000), nil, int32(1)},
			[]string{"long.timestamp-millis", "", "int"}},
		// Two qualified branches: each must keep its OWN qualification, so a
		// table that shifted by one would swap them rather than go short.
		{"two-logicals", `["null",{"type":"long","logicalType":"timestamp-millis"},{"type":"int","logicalType":"date"},"string"]`,
			[]any{nil, int64(1000), int32(5), "s"},
			[]string{"", "long.timestamp-millis", "int.date", "string"}},
		{"named-branch", `["null",{"type":"record","name":"N","fields":[{"name":"v","type":"int"}]}]`,
			[]any{nil, map[string]any{"v": int32(2)}}, []string{"", "N"}},
	}

	realized := 0
	for _, sh := range shapes {
		schema := `{"type":"record","name":"R","fields":[{"name":"u","type":` + sh.union + `}]}`
		direct := mustParse(t, schema)
		// A resolution compiles the union again, through its own sites.
		resolved := mustResolve(t, mustParse(t, schema),
			mustParse(t, `{"type":"record","name":"R","fields":[{"name":"u","type":`+sh.union+`},{"name":"extra","type":"int","default":9}]}`))

		for branch, v := range sh.values {
			for _, compiled := range []struct {
				how string
				s   *Schema
			}{{"direct", direct}, {"resolved", resolved}} {
				t.Run(sh.name+"/branch"+strconv.Itoa(branch)+"/"+compiled.how, func(t *testing.T) {
					wire, err := direct.AppendEncode(nil, map[string]any{"u": v})
					if err != nil {
						t.Fatalf("encode: %v", err)
					}
					var got map[string]any
					if _, err := compiled.s.Decode(wire, &got, TaggedUnions(), TagLogicalTypes()); err != nil {
						t.Fatalf("decode: %v", err)
					}
					if v == nil {
						// A null decodes to a nil interface, and maybeWrap
						// returns before the tables on an invalid element —
						// so the null branch is the one index that does NOT
						// exercise them, and it must stay bare.
						if got["u"] != nil {
							t.Fatalf("null branch decoded %#v, want a bare nil", got["u"])
						}
						return
					}
					m, ok := got["u"].(map[string]any)
					if !ok || len(m) != 1 {
						t.Fatalf("branch %d decoded %#v, want a one-key envelope", branch, got["u"])
					}
					// The exact tag, not merely some tag: a table that
					// shifted or dropped a qualification still produces an
					// envelope, and the wrong one routes a re-encode to a
					// different branch.
					for k := range m {
						if k != sh.tags[branch] {
							t.Fatalf("branch %d emitted tag %q, want %q", branch, k, sh.tags[branch])
						}
					}
				})
				realized++
			}
		}
	}
	want := 0
	for _, sh := range shapes {
		want += len(sh.values) * 2
	}
	if realized != want {
		t.Fatalf("realized %d cells, want %d", realized, want)
	}
}

// TestMatrix_FastPathDeclinesOnIncompleteFieldMeta pins that the unsafe field
// compilers decline — rather than fault — whenever the metadata a shape needs
// is missing, on both wires.
//
// A record field names its Avro type by asking its fieldMeta, so a field
// carrying NO meta names no type and reaches no shape arm at all; the other
// cells name a shape but withhold the inner metadata that shape needs. The
// axes are the wire (encode and decode compile through separate switches over
// one vocabulary) and the shape whose metadata is withheld.
//
// Every cell carries a COMPLETE twin: the same shape and the same Go type with
// the metadata supplied, which must compile to a non-nil fast path. Without
// it a cell proves only that something declined, which the kind checks would
// also do — the twin is what attributes the refusal to the missing metadata.
func TestMatrix_FastPathDeclinesOnIncompleteFieldMeta(t *testing.T) {
	// A real record's compiled tables, so the record cells' complete twins
	// carry something the compiler will actually accept.
	rec := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"v","type":"int"}]}`)
	type R struct {
		V int32 `avro:"v"`
	}
	recGo := reflect.TypeFor[R]()

	intMeta := func() *fieldMeta { return &fieldMeta{avroType: "int"} }

	cells := []struct {
		name       string
		goType     reflect.Type
		incomplete *fieldMeta // nil means the field carries no meta at all
		complete   *fieldMeta
	}{
		{"no-meta-at-all/ptr", reflect.TypeFor[*int32](), nil,
			&fieldMeta{avroType: "nullunion", inner: intMeta()}},
		{"no-meta-at-all/slice", reflect.SliceOf(reflect.TypeFor[int32]()), nil,
			&fieldMeta{avroType: "array", inner: intMeta()}},
		{"no-meta-at-all/scalar", reflect.TypeFor[int32](), nil, intMeta()},
		{"nullunion/no-inner", reflect.TypeFor[*int32](),
			&fieldMeta{avroType: "nullunion"},
			&fieldMeta{avroType: "nullunion", inner: intMeta()}},
		{"array/no-inner", reflect.SliceOf(reflect.TypeFor[int32]()),
			&fieldMeta{avroType: "array"},
			&fieldMeta{avroType: "array", inner: intMeta()}},
	}

	realized := 0
	for _, c := range cells {
		for _, wire := range []string{"encode", "decode"} {
			t.Run(wire+"/"+c.name, func(t *testing.T) {
				complete := c.complete
				switch wire {
				case "encode":
					if got := tryCompileFieldSer(&serRecordField{meta: c.incomplete}, c.goType); got != nil {
						t.Fatalf("compiled a fast path from incomplete metadata")
					}
					if got := tryCompileFieldSer(&serRecordField{meta: complete}, c.goType); got == nil {
						t.Fatalf("declined the COMPLETE twin, so the refusal above is not attributable to the missing metadata")
					}
				case "decode":
					if got := tryCompileFieldDeser(&deserRecordField{meta: c.incomplete}, c.goType); got != nil {
						t.Fatalf("compiled a fast path from incomplete metadata")
					}
					if got := tryCompileFieldDeser(&deserRecordField{meta: complete}, c.goType); got == nil {
						t.Fatalf("declined the COMPLETE twin, so the refusal above is not attributable to the missing metadata")
					}
				}
			})
			realized++
		}
	}

	// The record arm needs a real compiled table on both wires, so it is
	// crossed separately rather than being forced into the table above.
	t.Run("encode/record/no-table", func(t *testing.T) {
		if got := tryCompileFieldSer(&serRecordField{meta: &fieldMeta{avroType: "record"}}, recGo); got != nil {
			t.Fatalf("compiled a record fast path with no serRecord")
		}
		if got := tryCompileFieldSer(&serRecordField{meta: &fieldMeta{avroType: "record", serRecord: rec.node.serRecord}}, recGo); got == nil {
			t.Fatalf("declined the COMPLETE twin, so the refusal above is not attributable to the missing table")
		}
	})
	realized++
	t.Run("decode/record/no-table", func(t *testing.T) {
		if got := tryCompileFieldDeser(&deserRecordField{meta: &fieldMeta{avroType: "record"}}, recGo); got != nil {
			t.Fatalf("compiled a record fast path with no deserRecord")
		}
		if got := tryCompileFieldDeser(&deserRecordField{meta: &fieldMeta{avroType: "record", deserRecord: rec.node.deserRecord}}, recGo); got == nil {
			t.Fatalf("declined the COMPLETE twin, so the refusal above is not attributable to the missing table")
		}
	})
	realized++

	// Element shapes an array declines for reasons of its own, kept because
	// they are the arms that walk the inner metadata one level deeper.
	if fn := tryCompileFieldSer(&serRecordField{
		meta: &fieldMeta{avroType: "array", inner: &fieldMeta{avroType: "nullunion", inner: intMeta()}},
	}, reflect.SliceOf(reflect.TypeFor[int32]())); fn != nil {
		t.Error("compiled an array-of-nullunion fast path for a non-pointer element")
	}
	if fn := tryCompileFieldSer(&serRecordField{
		meta: &fieldMeta{avroType: "array", inner: &fieldMeta{avroType: "nullunion", inner: &fieldMeta{avroType: "map"}}},
	}, reflect.SliceOf(reflect.TypeFor[*map[string]int]())); fn != nil {
		t.Error("compiled an array-of-nullunion fast path for a non-compilable inner")
	}

	if want := len(cells)*2 + 2; realized != want {
		t.Fatalf("realized %d cells, want %d", realized, want)
	}
}

// TestUsArraySerErrorPaths covers the error handling in usArrayNullUnionPtr
// and usArrayDirect by passing a synthetic error-returning userfn.
func TestUsArraySerErrorPaths(t *testing.T) {
	errFake := fmt.Errorf("fake")
	failFn := func(dst []byte, p unsafe.Pointer, depth int) ([]byte, error) {
		return nil, errFake
	}

	t.Run("null_union_ptr", func(t *testing.T) {
		fn := usArrayNullUnionPtr(failFn, 0, 2)
		s := []*int32{ptr(int32(1))}
		_, err := fn(nil, unsafe.Pointer(&s), 0)
		if err != errFake {
			t.Errorf("expected fake error, got %v", err)
		}
	})

	t.Run("direct", func(t *testing.T) {
		fn := usArrayDirect(failFn, unsafe.Sizeof(int32(0)))
		s := []int32{1}
		_, err := fn(nil, unsafe.Pointer(&s), 0)
		if err != errFake {
			t.Errorf("expected fake error, got %v", err)
		}
	})
}

// TestUnsafePtrNilSerialize covers the nil-pointer check in the ptr-wrapping
// closure generated by tryCompileFieldSer (unsafe.go lines 178-180).
func TestUnsafePtrNilSerialize(t *testing.T) {
	type S struct {
		X *int32 `avro:"x"`
	}
	s := mustParse(t, `{"type":"record","name":"S","fields":[{"name":"x","type":"int"}]}`)
	// Encode with nil pointer — should trigger errUnsafeNilPtr via the fast path.
	_, err := s.AppendEncode(nil, &S{X: nil})
	if err == nil {
		t.Fatal("expected error for nil pointer field")
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
	mustDecode(t, s, got, &out)
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
	s := mustParse(t, `{"type":"record","name":"S","fields":[{"name":"x","type":"int"},{"name":"y","type":"int"}]}`)
	// Non-addressable struct value with nil pointer field → slow path + error.
	_, err := s.AppendEncode(nil, S{X: 42, Y: nil})
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
	s := mustParse(t, schema)

	t.Run("int branch", func(t *testing.T) {
		dst := mustAppendEncode(t, s, nil, ptr(int32(42)))
		var got any
		mustDecode(t, s, dst, &got)
	})

	t.Run("string branch", func(t *testing.T) {
		dst := mustAppendEncode(t, s, nil, ptr("hello"))
		var got any
		mustDecode(t, s, dst, &got)
	})

	t.Run("all fail encode", func(t *testing.T) {
		// Neither int nor string can encode a bool.
		encodeErr(t, schema, ptr(true))
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
	encodeErr(t, `"null"`, ptr(int32(42)))
}

// TestDeserIntegerOverflow verifies that decoding an Avro int or long into a
// too-narrow Go integer target returns an error rather than silently
// truncating or wrapping. This mirrors the range checks already performed on
// the encode side in [Schema.Encode].
func TestDeserIntegerOverflow(t *testing.T) {
	t.Run("long into int32 positive overflow", func(t *testing.T) {
		s := MustParse(`"long"`)
		data := mustEncode(t, s, int64(2147483648)) // MaxInt32+1
		var out int32
		if _, err := s.Decode(data, &out); err == nil {
			t.Fatalf("expected overflow error, got out=%d", out)
		}
	})

	t.Run("long into int32 negative overflow", func(t *testing.T) {
		s := MustParse(`"long"`)
		data := mustEncode(t, s, int64(-2147483649)) // MinInt32-1
		var out int32
		if _, err := s.Decode(data, &out); err == nil {
			t.Fatalf("expected overflow error, got out=%d", out)
		}
	})

	t.Run("int into int8 overflow", func(t *testing.T) {
		s := MustParse(`"int"`)
		data := mustEncode(t, s, int32(200))
		var out int8
		if _, err := s.Decode(data, &out); err == nil {
			t.Fatalf("expected overflow error, got out=%d", out)
		}
	})

	t.Run("int negative into uint32", func(t *testing.T) {
		s := MustParse(`"int"`)
		data := mustEncode(t, s, int32(-1))
		var out uint32
		if _, err := s.Decode(data, &out); err == nil {
			t.Fatalf("expected overflow error, got out=%d", out)
		}
	})

	t.Run("long negative into uint64", func(t *testing.T) {
		s := MustParse(`"long"`)
		data := mustEncode(t, s, int64(-1))
		var out uint64
		if _, err := s.Decode(data, &out); err == nil {
			t.Fatalf("expected overflow error, got out=%d", out)
		}
	})

	t.Run("in-range values decode cleanly", func(t *testing.T) {
		// Sanity check: values that fit must still decode without error.
		s := MustParse(`"long"`)
		data := mustEncode(t, s, int64(42))
		var out32 int32
		if _, err := s.Decode(data, &out32); err != nil {
			t.Fatalf("unexpected error for in-range value: %v", err)
		}
		if out32 != 42 {
			t.Fatalf("got %d, want 42", out32)
		}
	})
}

// TestDeserNullIntoNonPointerZeroes verifies that decoding a null-branch union
// into a non-pointer Go field always replaces the prior value with the Go zero,
// matching encoding/json/v2: "A JSON null may be decoded into every supported Go
// value where it is equivalent to storing the zero value", and "the decoded
// value replaces any pre-existing value". Prior to v1.x.0 twmb/avro matched
// encoding/json v1 and left non-pointer targets untouched on null, which
// preserved prior values across reused struct decodes — a silent
// data-corruption footgun.
func TestDeserNullIntoNonPointerZeroes(t *testing.T) {
	// Covers: deserNullUnion (["null", T]), deserNullSecondUnion (["T", "null"]),
	// and deserNull (general null branch in a 3+ way union).
	schema := `{"type":"record","name":"R","fields":[
		{"name":"a","type":["null","int"],"default":null},
		{"name":"b","type":["int","null"]},
		{"name":"c","type":["null","int","string"]},
		{"name":"d","type":["null","string"],"default":null}
	]}`
	s := mustParse(t, schema)

	// Encode an all-null record using writer-side maps.
	encoded := mustEncode(t, s, map[string]any{"a": nil, "b": nil, "c": nil, "d": nil})

	type Row struct {
		A int32  `avro:"a"`
		B int32  `avro:"b"`
		C int32  `avro:"c"`
		D string `avro:"d"`
	}

	// Pre-populate the struct to verify null always zeroes, regardless of
	// prior state.
	got := Row{A: 99, B: 88, C: 77, D: "prior"}
	mustDecode(t, s, encoded, &got)
	want := Row{}
	if got != want {
		t.Fatalf("null decoded into pre-populated struct: got %+v, want %+v", got, want)
	}
}

func TestDeserNullUnionErrors(t *testing.T) {
	schema := `["null","int"]`
	s := mustParse(t, schema)

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

func TestReadUvarlongShort10(t *testing.T) {
	// 9 continuation bytes with no 10th byte → short buffer.
	_, _, err := readUvarlong([]byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80})
	if err == nil {
		t.Fatal("expected short buffer error")
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
		input := Outer{Vals: []*int32{ptr(int32(10)), nil, ptr(int32(30))}}
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
	schema := `{"type": "array", "items": {
		"type": "record", "name": "Rec",
		"fields": [{"name": "v", "type": "int"}]
	}}`
	s := mustParse(t, schema)
	input := []*Rec{{V: 1}, {V: 2}, {V: 3}}
	encoded := mustAppendEncode(t, s, nil, &input)
	var output []*Rec
	rem := mustDecode(t, s, encoded, &output)
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
	schema := nullableIntSchema
	s := mustParse(t, schema)

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
	schema := nullableInnerSchema
	s := mustParse(t, schema)
	// Warm up: encode a valid value to compile fast paths.
	input := Outer{Item: &Inner{X: 1, Y: "hello"}}
	encoded := mustAppendEncode(t, s, nil, &input)
	// Decode truncated data to trigger error inside deserRecordFastPtr.
	var out Outer
	// Take just the non-null union byte + partial record.
	if len(encoded) > 2 {
		_, err := s.Decode(encoded[:2], &out)
		if err == nil {
			t.Fatal("expected error for truncated record")
		}
	}
}

// TestArrayPrimitiveRoundTrips covers the specialized ser/deser paths for
// all primitive array types through the reflect-based code path (bare slices)
// and struct-field round-trips (int, string).
func TestArrayPrimitiveRoundTrips(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		schema := arrayOfIntSchema
		input := Wrapper{Vals: []int32{10, 20, 30}}
		got := roundTrip(t, schema, input)
		if !reflect.DeepEqual(got.Vals, input.Vals) {
			t.Errorf("got %v, want %v", got.Vals, input.Vals)
		}
	})
	t.Run("string", func(t *testing.T) {
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
	})
	t.Run("boolean", func(t *testing.T) {
		input := []bool{true, false, true, false}
		got := roundTrip(t, `{"type":"array","items":"boolean"}`, input)
		if !reflect.DeepEqual(got, input) {
			t.Errorf("got %v, want %v", got, input)
		}
	})
	t.Run("long", func(t *testing.T) {
		input := []int64{100, -200, 300, 0, math.MaxInt64}
		got := roundTrip(t, `{"type":"array","items":"long"}`, input)
		if !reflect.DeepEqual(got, input) {
			t.Errorf("got %v, want %v", got, input)
		}
	})
	t.Run("float", func(t *testing.T) {
		input := []float32{1.1, -2.2, 3.3}
		got := roundTrip(t, `{"type":"array","items":"float"}`, input)
		if !reflect.DeepEqual(got, input) {
			t.Errorf("got %v, want %v", got, input)
		}
	})
	t.Run("double", func(t *testing.T) {
		input := []float64{1.11, -2.22, 3.33, math.Pi}
		got := roundTrip(t, `{"type":"array","items":"double"}`, input)
		if !reflect.DeepEqual(got, input) {
			t.Errorf("got %v, want %v", got, input)
		}
	})
}

// TestMapPrimitiveRoundTrips covers the specialized ser/deser paths for
// all primitive map value types through the reflect-based code path.
func TestMapPrimitiveRoundTrips(t *testing.T) {
	t.Run("boolean", func(t *testing.T) {
		input := map[string]bool{"a": true, "b": false}
		got := roundTrip(t, `{"type":"map","values":"boolean"}`, input)
		if !reflect.DeepEqual(got, input) {
			t.Errorf("got %v, want %v", got, input)
		}
	})
	t.Run("long", func(t *testing.T) {
		input := map[string]int64{"x": 100, "y": -200}
		got := roundTrip(t, `{"type":"map","values":"long"}`, input)
		if !reflect.DeepEqual(got, input) {
			t.Errorf("got %v, want %v", got, input)
		}
	})
	t.Run("float", func(t *testing.T) {
		input := map[string]float32{"pi": 3.14, "e": 2.71}
		got := roundTrip(t, `{"type":"map","values":"float"}`, input)
		if !reflect.DeepEqual(got, input) {
			t.Errorf("got %v, want %v", got, input)
		}
	})
	t.Run("double", func(t *testing.T) {
		input := map[string]float64{"pi": math.Pi, "e": math.E}
		got := roundTrip(t, `{"type":"map","values":"double"}`, input)
		if !reflect.DeepEqual(got, input) {
			t.Errorf("got %v, want %v", got, input)
		}
	})
}

// TestArrayInterfaceRoundTrips covers the reflect.Interface unwrap branches
// in serArray specialized methods by encoding []any slices through typed schemas.
func TestArrayInterfaceRoundTrips(t *testing.T) {
	t.Run("boolean", func(t *testing.T) {
		s := mustParse(t, `{"type":"array","items":"boolean"}`)
		input := []any{true, false, true}
		dst := mustAppendEncode(t, s, nil, &input)
		var got []bool
		mustDecode(t, s, dst, &got)
		want := []bool{true, false, true}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("int", func(t *testing.T) {
		s := mustParse(t, `{"type":"array","items":"int"}`)
		input := []any{int32(10), int32(20)}
		dst := mustAppendEncode(t, s, nil, &input)
		var got []int32
		mustDecode(t, s, dst, &got)
		want := []int32{10, 20}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("long", func(t *testing.T) {
		s := mustParse(t, `{"type":"array","items":"long"}`)
		input := []any{int64(100), int64(200)}
		dst := mustAppendEncode(t, s, nil, &input)
		var got []int64
		mustDecode(t, s, dst, &got)
		want := []int64{100, 200}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("float", func(t *testing.T) {
		s := mustParse(t, `{"type":"array","items":"float"}`)
		input := []any{float32(1.5), float32(2.5)}
		dst := mustAppendEncode(t, s, nil, &input)
		var got []float32
		mustDecode(t, s, dst, &got)
		want := []float32{1.5, 2.5}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("double", func(t *testing.T) {
		s := mustParse(t, `{"type":"array","items":"double"}`)
		input := []any{float64(1.11), float64(2.22)}
		dst := mustAppendEncode(t, s, nil, &input)
		var got []float64
		mustDecode(t, s, dst, &got)
		want := []float64{1.11, 2.22}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("string", func(t *testing.T) {
		s := mustParse(t, `{"type":"array","items":"string"}`)
		input := []any{"hello", "world"}
		dst := mustAppendEncode(t, s, nil, &input)
		var got []string
		mustDecode(t, s, dst, &got)
		want := []string{"hello", "world"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}

// TestMapInterfaceRoundTrips covers the reflect.Interface unwrap branches
// in serMap specialized methods by encoding map[string]any through typed schemas.
func TestMapInterfaceRoundTrips(t *testing.T) {
	t.Run("boolean", func(t *testing.T) {
		s := mustParse(t, `{"type":"map","values":"boolean"}`)
		input := map[string]any{"a": true, "b": false}
		dst := mustAppendEncode(t, s, nil, &input)
		var got map[string]bool
		mustDecode(t, s, dst, &got)
		want := map[string]bool{"a": true, "b": false}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("long", func(t *testing.T) {
		s := mustParse(t, `{"type":"map","values":"long"}`)
		input := map[string]any{"x": int64(100)}
		dst := mustAppendEncode(t, s, nil, &input)
		var got map[string]int64
		mustDecode(t, s, dst, &got)
		want := map[string]int64{"x": 100}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("float", func(t *testing.T) {
		s := mustParse(t, `{"type":"map","values":"float"}`)
		input := map[string]any{"pi": float32(3.14)}
		dst := mustAppendEncode(t, s, nil, &input)
		var got map[string]float32
		mustDecode(t, s, dst, &got)
		want := map[string]float32{"pi": 3.14}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("double", func(t *testing.T) {
		s := mustParse(t, `{"type":"map","values":"double"}`)
		input := map[string]any{"e": math.E}
		dst := mustAppendEncode(t, s, nil, &input)
		var got map[string]float64
		mustDecode(t, s, dst, &got)
		want := map[string]float64{"e": math.E}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("string", func(t *testing.T) {
		s := mustParse(t, `{"type":"map","values":"string"}`)
		input := map[string]any{"k": "v"}
		dst := mustAppendEncode(t, s, nil, &input)
		var got map[string]string
		mustDecode(t, s, dst, &got)
		want := map[string]string{"k": "v"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}

// TestArrayUintRoundTrips covers CanUint() branches in serArray serInt/serLong.
func TestArrayUintRoundTrips(t *testing.T) {
	t.Run("uint_as_int", func(t *testing.T) {
		s := mustParse(t, `{"type":"array","items":"int"}`)
		input := []uint16{10, 20, 30}
		dst := mustAppendEncode(t, s, nil, &input)
		var got []int32
		mustDecode(t, s, dst, &got)
		want := []int32{10, 20, 30}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("uint_as_long", func(t *testing.T) {
		s := mustParse(t, `{"type":"array","items":"long"}`)
		input := []uint32{100, 200}
		dst := mustAppendEncode(t, s, nil, &input)
		var got []int64
		mustDecode(t, s, dst, &got)
		want := []int64{100, 200}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("float64_as_int", func(t *testing.T) {
		s := mustParse(t, `{"type":"array","items":"int"}`)
		input := []float64{10.0, 20.0}
		dst := mustAppendEncode(t, s, nil, &input)
		var got []int32
		mustDecode(t, s, dst, &got)
		want := []int32{10, 20}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("float64_as_long", func(t *testing.T) {
		s := mustParse(t, `{"type":"array","items":"long"}`)
		input := []float64{100.0, 200.0}
		dst := mustAppendEncode(t, s, nil, &input)
		var got []int64
		mustDecode(t, s, dst, &got)
		want := []int64{100, 200}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}

// TestMapUintRoundTrips covers CanUint() and CanFloat() branches in serMap serInt/serLong.
func TestMapUintRoundTrips(t *testing.T) {
	t.Run("uint_as_int", func(t *testing.T) {
		s := mustParse(t, `{"type":"map","values":"int"}`)
		input := map[string]uint16{"a": 10, "b": 20}
		dst := mustAppendEncode(t, s, nil, &input)
		var got map[string]int32
		mustDecode(t, s, dst, &got)
		want := map[string]int32{"a": 10, "b": 20}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("uint_as_long", func(t *testing.T) {
		s := mustParse(t, `{"type":"map","values":"long"}`)
		input := map[string]uint32{"x": 100}
		dst := mustAppendEncode(t, s, nil, &input)
		var got map[string]int64
		mustDecode(t, s, dst, &got)
		want := map[string]int64{"x": 100}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("float64_as_int", func(t *testing.T) {
		s := mustParse(t, `{"type":"map","values":"int"}`)
		input := map[string]float64{"a": 10.0}
		dst := mustAppendEncode(t, s, nil, &input)
		var got map[string]int32
		mustDecode(t, s, dst, &got)
		want := map[string]int32{"a": 10}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("float64_as_long", func(t *testing.T) {
		s := mustParse(t, `{"type":"map","values":"long"}`)
		input := map[string]float64{"x": 100.0}
		dst := mustAppendEncode(t, s, nil, &input)
		var got map[string]int64
		mustDecode(t, s, dst, &got)
		want := map[string]int64{"x": 100}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}

// TestArraySerTypeMismatch covers error paths in serArray specialized methods
// when elements have the wrong type.
func TestArraySerTypeMismatch(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		input  any
	}{
		{"string_bad", `{"type":"array","items":"string"}`, &[]int{1}},
		{"boolean_bad", `{"type":"array","items":"boolean"}`, &[]int{1}},
		{"int_bad", `{"type":"array","items":"int"}`, &[]string{"x"}},
		{"long_bad", `{"type":"array","items":"long"}`, &[]string{"x"}},
		{"float_bad", `{"type":"array","items":"float"}`, &[]string{"x"}},
		{"double_bad", `{"type":"array","items":"double"}`, &[]string{"x"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encodeErr(t, tt.schema, tt.input)
		})
	}
}

// TestMapSerTypeMismatch covers error paths in serMap specialized methods
// when values have the wrong type.
func TestMapSerTypeMismatch(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		input  any
	}{
		{"string_bad", `{"type":"map","values":"string"}`, &map[string]int{"a": 1}},
		{"boolean_bad", `{"type":"map","values":"boolean"}`, &map[string]int{"a": 1}},
		{"int_bad", `{"type":"map","values":"int"}`, &map[string]string{"a": "x"}},
		{"long_bad", `{"type":"map","values":"long"}`, &map[string]string{"a": "x"}},
		{"float_bad", `{"type":"map","values":"float"}`, &map[string]string{"a": "x"}},
		{"double_bad", `{"type":"map","values":"double"}`, &map[string]string{"a": "x"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encodeErr(t, tt.schema, tt.input)
		})
	}
}

// TestArraySerNotSlice covers the "not a slice/array" error path in
// serArray specialized methods.
func TestArraySerNotSlice(t *testing.T) {
	schemas := []string{
		`{"type":"array","items":"string"}`,
		`{"type":"array","items":"boolean"}`,
		`{"type":"array","items":"int"}`,
		`{"type":"array","items":"long"}`,
		`{"type":"array","items":"float"}`,
		`{"type":"array","items":"double"}`,
	}
	for _, schema := range schemas {
		t.Run(schema, func(t *testing.T) {
			bad := "not a slice"
			encodeErr(t, schema, &bad)
		})
	}
}

// TestMapSerNotMap covers the "not a map" error path in serMap specialized methods.
func TestMapSerNotMap(t *testing.T) {
	schemas := []string{
		`{"type":"map","values":"string"}`,
		`{"type":"map","values":"boolean"}`,
		`{"type":"map","values":"int"}`,
		`{"type":"map","values":"long"}`,
		`{"type":"map","values":"float"}`,
		`{"type":"map","values":"double"}`,
	}
	for _, schema := range schemas {
		t.Run(schema, func(t *testing.T) {
			bad := "not a map"
			encodeErr(t, schema, &bad)
		})
	}
}

// TestArrayDeserTruncatedPrimitives covers error paths in fast array deser
// loops when the source buffer is truncated.
func TestArrayDeserTruncatedPrimitives(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		input  any
	}{
		{"boolean", `{"type":"array","items":"boolean"}`, &[]bool{true, false}},
		{"long", `{"type":"array","items":"long"}`, &[]int64{100, 200}},
		{"float", `{"type":"array","items":"float"}`, &[]float32{1.1, 2.2}},
		{"double", `{"type":"array","items":"double"}`, &[]float64{1.11, 2.22}},
		{"string", `{"type":"array","items":"string"}`, &[]string{"hello", "world"}},
		{"int", `{"type":"array","items":"int"}`, &[]int32{10, 20}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustParse(t, tt.schema)
			dst := mustAppendEncode(t, s, nil, tt.input)
			// Truncate: keep the block count but remove some item data.
			truncated := dst[:len(dst)/2]
			out := reflect.New(reflect.TypeOf(tt.input).Elem()).Interface()
			_, err := s.Decode(truncated, out)
			if err == nil {
				t.Fatal("expected error for truncated data")
			}
		})
	}
}

// TestMapDeserTruncatedPrimitives covers error paths in fast map deser
// block functions when the source buffer is truncated.
func TestMapDeserTruncatedPrimitives(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		input  any
	}{
		{"boolean", `{"type":"map","values":"boolean"}`, &map[string]bool{"aa": true, "bb": false}},
		{"long", `{"type":"map","values":"long"}`, &map[string]int64{"aa": 100, "bb": 200}},
		{"float", `{"type":"map","values":"float"}`, &map[string]float32{"aa": 1.1, "bb": 2.2}},
		{"double", `{"type":"map","values":"double"}`, &map[string]float64{"aa": 1.11, "bb": 2.22}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustParse(t, tt.schema)
			dst := mustAppendEncode(t, s, nil, tt.input)
			// Truncate: keep the block count but remove some data.
			truncated := dst[:len(dst)/2]
			out := reflect.New(reflect.TypeOf(tt.input).Elem()).Interface()
			_, err := s.Decode(truncated, out)
			if err == nil {
				t.Fatal("expected error for truncated data")
			}
		})
	}
}

// TestArrayFastLoopErrors covers error paths inside the specialized fast
// loop functions (deserArrayStringLoop, etc.) by crafting payloads where
// the block count passes the outer sanity check but element data is malformed.
func TestArrayFastLoopErrors(t *testing.T) {
	t.Run("string_negative_length", func(t *testing.T) {
		s := mustParse(t, `{"type":"array","items":"string"}`)
		// Block count=1, then a negative varlong for string length (-1 = zigzag 0x01).
		data := []byte{0x02, 0x01} // count=1, string_len=-1
		var got []string
		if _, err := s.Decode(data, &got); err == nil {
			t.Fatal("expected error for negative string length")
		}
	})
	t.Run("string_short_buffer", func(t *testing.T) {
		s := mustParse(t, `{"type":"array","items":"string"}`)
		// Block count=1, string length=10 (zigzag=20=0x28), but only 2 bytes of data.
		data := []byte{0x02, 0x28, 'a', 'b'}
		var got []string
		if _, err := s.Decode(data, &got); err == nil {
			t.Fatal("expected error for short string buffer")
		}
	})
	t.Run("boolean_short", func(t *testing.T) {
		s := mustParse(t, `{"type":"array","items":"boolean"}`)
		// Block count=2, but only 1 boolean byte. count(2) <= len(src)(1) passes
		// outer check, but inner loop fails on second element.
		data := []byte{0x04, 0x01}
		var got []bool
		if _, err := s.Decode(data, &got); err == nil {
			t.Fatal("expected error for short boolean buffer")
		}
	})
	t.Run("int_truncated", func(t *testing.T) {
		s := mustParse(t, `{"type":"array","items":"int"}`)
		// Block count=2, first int=0 (1 byte), then 0x80 (continuation bit, truncated).
		// count(2) <= len(src)(2) passes outer check, but readVarint fails on second element.
		data := []byte{0x04, 0x00, 0x80}
		var got []int32
		if _, err := s.Decode(data, &got); err == nil {
			t.Fatal("expected error for truncated int data")
		}
	})
	t.Run("long_truncated", func(t *testing.T) {
		s := mustParse(t, `{"type":"array","items":"long"}`)
		// Same pattern: count=2, first long=0, second truncated varlong.
		data := []byte{0x04, 0x00, 0x80}
		var got []int64
		if _, err := s.Decode(data, &got); err == nil {
			t.Fatal("expected error for truncated long data")
		}
	})
	t.Run("float_truncated", func(t *testing.T) {
		s := mustParse(t, `{"type":"array","items":"float"}`)
		// Block count=2, first float=4 valid bytes, second only 2 bytes.
		// count(2) <= len(src)(6) passes, but readUint32 fails on second.
		data := []byte{0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
		var got []float32
		if _, err := s.Decode(data, &got); err == nil {
			t.Fatal("expected error for truncated float data")
		}
	})
	t.Run("double_truncated", func(t *testing.T) {
		s := mustParse(t, `{"type":"array","items":"double"}`)
		// Block count=2, first double=8 valid bytes, second only 4 bytes.
		// count(2) <= len(src)(12) passes, but readUint64 fails on second.
		data := []byte{0x04, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		var got []float64
		if _, err := s.Decode(data, &got); err == nil {
			t.Fatal("expected error for truncated double data")
		}
	})
}

// TestMapFastBlockErrors covers error paths inside the specialized fast
// block functions by crafting payloads where block count passes the outer
// sanity check but key or value data is malformed.
func TestMapFastBlockErrors(t *testing.T) {
	t.Run("string_key_readvarlong_error", func(t *testing.T) {
		s := mustParse(t, `{"type":"map","values":"string"}`)
		// Block count=1, then 0x80 (continuation bit, truncated varlong for key length).
		data := []byte{0x02, 0x80}
		var got map[string]string
		if _, err := s.Decode(data, &got); err == nil {
			t.Fatal("expected error for truncated key varlong")
		}
	})
	t.Run("string_key_bad_length", func(t *testing.T) {
		s := mustParse(t, `{"type":"map","values":"string"}`)
		// Block count=1, key_len=10 but only 2 bytes available.
		data := []byte{0x02, 0x28, 'a', 'b'}
		var got map[string]string
		if _, err := s.Decode(data, &got); err == nil {
			t.Fatal("expected error for bad map key length")
		}
	})
	t.Run("string_value_readvarlong_error", func(t *testing.T) {
		s := mustParse(t, `{"type":"map","values":"string"}`)
		// Block count=1, key_len=1, key="a", then 0x80 (truncated value length varlong).
		data := []byte{0x02, 0x02, 'a', 0x80}
		var got map[string]string
		if _, err := s.Decode(data, &got); err == nil {
			t.Fatal("expected error for truncated value varlong")
		}
	})
	t.Run("string_value_truncated", func(t *testing.T) {
		s := mustParse(t, `{"type":"map","values":"string"}`)
		// Block count=1, key_len=1, key="a", value_len=10 but truncated.
		data := []byte{0x02, 0x02, 'a', 0x28, 'b'}
		var got map[string]string
		if _, err := s.Decode(data, &got); err == nil {
			t.Fatal("expected error for truncated map string value")
		}
	})
	t.Run("boolean_key_bad_length", func(t *testing.T) {
		s := mustParse(t, `{"type":"map","values":"boolean"}`)
		// Block count=1, key_len=10 but truncated.
		data := []byte{0x02, 0x28, 'a'}
		var got map[string]bool
		if _, err := s.Decode(data, &got); err == nil {
			t.Fatal("expected error for bad map key length")
		}
	})
	t.Run("boolean_value_short", func(t *testing.T) {
		s := mustParse(t, `{"type":"map","values":"boolean"}`)
		// Block count=1, key_len=1, key="a", but no boolean byte.
		data := []byte{0x02, 0x02, 'a'}
		var got map[string]bool
		if _, err := s.Decode(data, &got); err == nil {
			t.Fatal("expected error for missing boolean value")
		}
	})
	t.Run("long_key_bad_length", func(t *testing.T) {
		s := mustParse(t, `{"type":"map","values":"long"}`)
		// Block count=1, negative key_len (-1 = zigzag 0x01).
		data := []byte{0x02, 0x01}
		var got map[string]int64
		if _, err := s.Decode(data, &got); err == nil {
			t.Fatal("expected error for negative map key length")
		}
	})
	t.Run("long_value_truncated", func(t *testing.T) {
		s := mustParse(t, `{"type":"map","values":"long"}`)
		// Block count=1, key_len=1, key="a", then truncated varlong value.
		data := []byte{0x02, 0x02, 'a', 0x80}
		var got map[string]int64
		if _, err := s.Decode(data, &got); err == nil {
			t.Fatal("expected error for truncated long value")
		}
	})
	t.Run("float_key_bad_length", func(t *testing.T) {
		s := mustParse(t, `{"type":"map","values":"float"}`)
		// Block count=1, negative key_len.
		data := []byte{0x02, 0x01}
		var got map[string]float32
		if _, err := s.Decode(data, &got); err == nil {
			t.Fatal("expected error for negative map key length")
		}
	})
	t.Run("float_value_truncated", func(t *testing.T) {
		s := mustParse(t, `{"type":"map","values":"float"}`)
		// Block count=1, key_len=1, key="a", only 2 of 4 float bytes.
		data := []byte{0x02, 0x02, 'a', 0x00, 0x00}
		var got map[string]float32
		if _, err := s.Decode(data, &got); err == nil {
			t.Fatal("expected error for truncated float value")
		}
	})
	t.Run("double_key_bad_length", func(t *testing.T) {
		s := mustParse(t, `{"type":"map","values":"double"}`)
		// Block count=1, negative key_len.
		data := []byte{0x02, 0x01}
		var got map[string]float64
		if _, err := s.Decode(data, &got); err == nil {
			t.Fatal("expected error for negative map key length")
		}
	})
	t.Run("double_value_truncated", func(t *testing.T) {
		s := mustParse(t, `{"type":"map","values":"double"}`)
		// Block count=1, key_len=1, key="a", only 4 of 8 double bytes.
		data := []byte{0x02, 0x02, 'a', 0x00, 0x00, 0x00, 0x00}
		var got map[string]float64
		if _, err := s.Decode(data, &got); err == nil {
			t.Fatal("expected error for truncated double value")
		}
	})
}

// TestMapSlowPathErrors covers error paths in the deserMap slow path
// (non-fast-block) by decoding truncated data into any (interface target).
func TestMapSlowPathErrors(t *testing.T) {
	t.Run("key_readvarlong_error", func(t *testing.T) {
		s := mustParse(t, `{"type":"map","values":"string"}`)
		// Block count=1, then a truncated varlong for the key length.
		data := []byte{0x02, 0x80}
		var got any
		if _, err := s.Decode(data, &got); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("key_invalid_length", func(t *testing.T) {
		s := mustParse(t, `{"type":"map","values":"string"}`)
		// Block count=1, key_len=-1 (zigzag 0x01).
		data := []byte{0x02, 0x01}
		var got any
		if _, err := s.Decode(data, &got); err == nil {
			t.Fatal("expected error for negative key length")
		}
	})
	t.Run("value_deser_error", func(t *testing.T) {
		// Use a map with record values to exercise the slow path (non-fast-block).
		// Encode valid first entry, but truncate second entry's value data.
		schema := `{"type":"map","values":{"type":"record","name":"R","fields":[{"name":"n","type":"int"}]}}`
		s := mustParse(t, schema)
		type R struct {
			N int32 `avro:"n"`
		}
		input := map[string]R{"a": {N: 1}, "b": {N: 2}}
		dst := mustAppendEncode(t, s, nil, &input)
		// Truncate last 2 bytes to corrupt second entry's value.
		truncated := dst[:len(dst)-2]
		var got map[string]R
		if _, err := s.Decode(truncated, &got); err == nil {
			t.Fatal("expected error for truncated map value")
		}
	})
}

// TestGenericArrayDeserError covers the error path in the generic
// (non-fast-loop) array deser when deserItem fails.
func TestGenericArrayDeserError(t *testing.T) {
	// Use array of records (non-primitive) to exercise generic deser path.
	// Decode into any (interface) to ensure we go through the reflect path,
	// not the unsafe struct path.
	schema := `{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"n","type":"int"}]}}`
	s := mustParse(t, schema)
	// Craft binary: count=2 passes sanity check, record1 decodes OK,
	// record2 has malformed varint causing deserItem error at L508.
	// count=2 (varlong 0x04), record1 n=1 (varint 0x02),
	// record2 n=malformed (0x80 continuation bit, no following byte).
	data := []byte{0x04, 0x02, 0x80}
	var got any
	if _, err := s.Decode(data, &got); err == nil {
		t.Fatal("expected error for malformed array item")
	}
}

// TestSerOverflowErrors covers overflow error paths in specialized
// ser functions for int/long arrays and maps.
func TestSerOverflowErrors(t *testing.T) {
	// Array int overflow errors.
	t.Run("array_int_overflow_int64", func(t *testing.T) {
		encodeErr(t, `{"type":"array","items":"int"}`, &[]int64{math.MaxInt32 + 1})
	})
	t.Run("array_int_overflow_uint64", func(t *testing.T) {
		encodeErr(t, `{"type":"array","items":"int"}`, &[]uint64{math.MaxInt32 + 1})
	})
	t.Run("array_int_fractional_float", func(t *testing.T) {
		encodeErr(t, `{"type":"array","items":"int"}`, &[]float64{1.5})
	})
	t.Run("array_int_overflow_float", func(t *testing.T) {
		encodeErr(t, `{"type":"array","items":"int"}`, &[]float64{float64(math.MaxInt32) + 100})
	})
	// Array long overflow errors.
	t.Run("array_long_fractional_float", func(t *testing.T) {
		encodeErr(t, `{"type":"array","items":"long"}`, &[]float64{1.5})
	})
	t.Run("array_long_overflow_float", func(t *testing.T) {
		encodeErr(t, `{"type":"array","items":"long"}`, &[]float64{1e19})
	})
	// Map int overflow errors.
	t.Run("map_int_overflow_int64", func(t *testing.T) {
		encodeErr(t, `{"type":"map","values":"int"}`, &map[string]int64{"a": math.MaxInt32 + 1})
	})
	t.Run("map_int_overflow_uint64", func(t *testing.T) {
		encodeErr(t, `{"type":"map","values":"int"}`, &map[string]uint64{"a": math.MaxInt32 + 1})
	})
	t.Run("map_int_fractional_float", func(t *testing.T) {
		encodeErr(t, `{"type":"map","values":"int"}`, &map[string]float64{"a": 1.5})
	})
	t.Run("map_int_overflow_float", func(t *testing.T) {
		encodeErr(t, `{"type":"map","values":"int"}`, &map[string]float64{"a": float64(math.MaxInt32) + 100})
	})
	// Map long overflow errors.
	t.Run("map_long_fractional_float", func(t *testing.T) {
		encodeErr(t, `{"type":"map","values":"long"}`, &map[string]float64{"a": 1.5})
	})
	t.Run("map_long_overflow_float", func(t *testing.T) {
		encodeErr(t, `{"type":"map","values":"long"}`, &map[string]float64{"a": 1e19})
	})
}

// TestSerGenericArrayMapErrors covers error and edge paths in the generic
// (non-specialized) serArray.ser and serMap.ser methods.
func TestSerGenericArrayMapErrors(t *testing.T) {
	// Generic serArray.ser is used for non-primitive array items (e.g., records).
	recordArraySchema := `{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"n","type":"int"}]}}`
	t.Run("array_generic_nil_pointer", func(t *testing.T) {
		type R struct{ N int32 }
		encodeErr(t, recordArraySchema, (*[]R)(nil))
	})
	t.Run("array_generic_not_slice", func(t *testing.T) {
		bad := "not a slice"
		encodeErr(t, recordArraySchema, &bad)
	})
	t.Run("array_generic_empty", func(t *testing.T) {
		type R struct {
			N int32 `avro:"n"`
		}
		input := []R{}
		got := roundTrip(t, recordArraySchema, input)
		if len(got) != 0 {
			t.Errorf("expected empty slice, got %v", got)
		}
	})
	t.Run("array_generic_item_error", func(t *testing.T) {
		// Pass wrong struct type to trigger serItem error.
		type Wrong struct {
			X string `avro:"x"` // field name mismatch
		}
		bad := []Wrong{{X: "oops"}}
		encodeErr(t, recordArraySchema, &bad)
	})
	// Generic serMap.ser is used for non-primitive map values.
	recordMapSchema := `{"type":"map","values":{"type":"record","name":"R2","fields":[{"name":"n","type":"int"}]}}`
	t.Run("map_generic_nil_pointer", func(t *testing.T) {
		type R2 struct{ N int32 }
		encodeErr(t, recordMapSchema, (*map[string]R2)(nil))
	})
	t.Run("map_generic_not_map", func(t *testing.T) {
		bad := "not a map"
		encodeErr(t, recordMapSchema, &bad)
	})
	t.Run("map_generic_empty", func(t *testing.T) {
		type R2 struct {
			N int32 `avro:"n"`
		}
		input := map[string]R2{}
		got := roundTrip(t, recordMapSchema, input)
		if len(got) != 0 {
			t.Errorf("expected empty map, got %v", got)
		}
	})
	t.Run("map_generic_value_error", func(t *testing.T) {
		type Wrong struct {
			X string `avro:"x"`
		}
		bad := map[string]Wrong{"a": {X: "oops"}}
		encodeErr(t, recordMapSchema, &bad)
	})
}

// TestSerSpecializedEmpty covers the l==0 early return in specialized
// ser methods for both arrays and maps.
func TestSerSpecializedEmpty(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		input  any
	}{
		{"array_string", `{"type":"array","items":"string"}`, &[]string{}},
		{"array_boolean", `{"type":"array","items":"boolean"}`, &[]bool{}},
		{"array_int", `{"type":"array","items":"int"}`, &[]int32{}},
		{"array_long", `{"type":"array","items":"long"}`, &[]int64{}},
		{"array_float", `{"type":"array","items":"float"}`, &[]float32{}},
		{"array_double", `{"type":"array","items":"double"}`, &[]float64{}},
		{"map_string", `{"type":"map","values":"string"}`, &map[string]string{}},
		{"map_boolean", `{"type":"map","values":"boolean"}`, &map[string]bool{}},
		{"map_int", `{"type":"map","values":"int"}`, &map[string]int32{}},
		{"map_long", `{"type":"map","values":"long"}`, &map[string]int64{}},
		{"map_float", `{"type":"map","values":"float"}`, &map[string]float32{}},
		{"map_double", `{"type":"map","values":"double"}`, &map[string]float64{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustParse(t, tt.schema)
			dst := mustAppendEncode(t, s, nil, tt.input)
			// Empty array/map encodes as just a zero block count.
			if len(dst) != 1 || dst[0] != 0 {
				t.Errorf("expected [0x00], got %v", dst)
			}
		})
	}
}

// TestSerSpecializedNilPointer covers the indirect error path in
// specialized ser methods when given a nil pointer.
func TestSerSpecializedNilPointer(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		input  any
	}{
		{"array_string", `{"type":"array","items":"string"}`, (*[]string)(nil)},
		{"array_boolean", `{"type":"array","items":"boolean"}`, (*[]bool)(nil)},
		{"array_int", `{"type":"array","items":"int"}`, (*[]int32)(nil)},
		{"array_long", `{"type":"array","items":"long"}`, (*[]int64)(nil)},
		{"array_float", `{"type":"array","items":"float"}`, (*[]float32)(nil)},
		{"array_double", `{"type":"array","items":"double"}`, (*[]float64)(nil)},
		{"map_string", `{"type":"map","values":"string"}`, (*map[string]string)(nil)},
		{"map_boolean", `{"type":"map","values":"boolean"}`, (*map[string]bool)(nil)},
		{"map_int", `{"type":"map","values":"int"}`, (*map[string]int32)(nil)},
		{"map_long", `{"type":"map","values":"long"}`, (*map[string]int64)(nil)},
		{"map_float", `{"type":"map","values":"float"}`, (*map[string]float32)(nil)},
		{"map_double", `{"type":"map","values":"double"}`, (*map[string]float64)(nil)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encodeErr(t, tt.schema, tt.input)
		})
	}
}

// TestTimeOverflow covers overflow error paths in time-millis/time-micros
// serialization and deserialization.
func TestTimeOverflow(t *testing.T) {
	t.Run("ser_time_millis_overflow", func(t *testing.T) {
		// A duration too large for int32 milliseconds.
		schema := `{"type":"int","logicalType":"time-millis"}`
		d := time.Duration(math.MaxInt32+1) * time.Millisecond
		encodeErr(t, schema, &d)
	})
	t.Run("ser_time_millis_overflow_unsafe", func(t *testing.T) {
		// Through the unsafe path (struct field).
		type W struct {
			T time.Duration `avro:"t"`
		}
		schema := `{"type":"record","name":"W","fields":[{"name":"t","type":{"type":"int","logicalType":"time-millis"}}]}`
		w := W{T: time.Duration(math.MaxInt32+1) * time.Millisecond}
		encodeErr(t, schema, &w)
	})
	t.Run("deser_time_micros_overflow", func(t *testing.T) {
		// Encode a raw long value that overflows when converted to time.Duration.
		schema := `{"type":"long","logicalType":"time-micros"}`
		s := mustParse(t, schema)
		// Encode MaxInt64 as raw long (it will overflow when * Microsecond).
		var big int64 = math.MaxInt64
		dst := mustAppendEncode(t, s, nil, &big)
		var d time.Duration
		_, err := s.Decode(dst, &d)
		if err == nil {
			t.Fatal("expected overflow error for time-micros")
		}
	})
	t.Run("deser_time_micros_overflow_unsafe", func(t *testing.T) {
		type W struct {
			T time.Duration `avro:"t"`
		}
		schema := `{"type":"record","name":"W","fields":[{"name":"t","type":{"type":"long","logicalType":"time-micros"}}]}`
		s := mustParse(t, schema)
		// Encode a record with a huge long value for time-micros.
		rawSchema := `{"type":"record","name":"W","fields":[{"name":"t","type":"long"}]}`
		rawS := mustParse(t, rawSchema)
		type RawW struct {
			T int64 `avro:"t"`
		}
		raw := RawW{T: math.MaxInt64}
		dst := mustAppendEncode(t, rawS, nil, &raw)
		var got W
		_, err := s.Decode(dst, &got)
		if err == nil {
			t.Fatal("expected overflow error for time-micros")
		}
	})
}

// TestUsIntOverflow covers overflow paths in unsafe.go usInt for
// int, int64, uint, uint32, uint64 types that overflow int32.
func TestUsIntOverflow(t *testing.T) {
	schema := `{"type":"record","name":"W","fields":[{"name":"v","type":"int"}]}`
	t.Run("int_overflow", func(t *testing.T) {
		if strconv.IntSize == 32 {
			t.Skip("a 32-bit int cannot hold a value that overflows int32")
		}
		type W struct {
			V int `avro:"v"`
		}
		// Runtime conversion so the file compiles on 32-bit platforms,
		// where the constant form would not.
		over := int64(math.MaxInt32) + 1
		encodeErr(t, schema, &W{V: int(over)})
	})
	t.Run("int64_overflow", func(t *testing.T) {
		type W struct {
			V int64 `avro:"v"`
		}
		encodeErr(t, schema, &W{V: math.MaxInt32 + 1})
	})
	t.Run("uint_overflow", func(t *testing.T) {
		type W struct {
			V uint `avro:"v"`
		}
		encodeErr(t, schema, &W{V: math.MaxInt32 + 1})
	})
	t.Run("uint32_overflow", func(t *testing.T) {
		type W struct {
			V uint32 `avro:"v"`
		}
		encodeErr(t, schema, &W{V: math.MaxInt32 + 1})
	})
	t.Run("uint64_overflow", func(t *testing.T) {
		type W struct {
			V uint64 `avro:"v"`
		}
		encodeErr(t, schema, &W{V: math.MaxInt32 + 1})
	})
}

// TestSerFixedDefault covers the default case in serSize.ser (fixed)
// when given a type that is neither array, slice, nor uint8-based.
func TestSerFixedDefault(t *testing.T) {
	encodeErr(t, `{"type":"fixed","name":"f","size":4}`, &struct{}{})
}

// TestNullSecondUnionForwardRef covers the buildUnion branch for
// ["type","null"] unions where the type is a forward reference to a
// record defined later in the same parent record's fields.
func TestNullSecondUnionForwardRef(t *testing.T) {
	// Field "b" references "B" which is defined later in field "c".
	// This exercises the isMissing check at schema.go L461.
	schema := `{
		"type": "record",
		"name": "A",
		"fields": [
			{"name": "b", "type": ["B", "null"]},
			{"name": "c", "type": {"type": "record", "name": "B", "fields": [{"name": "x", "type": "int"}]}}
		]
	}`
	type B struct {
		X int32 `avro:"x"`
	}
	type A struct {
		B *B `avro:"b"`
		C B  `avro:"c"`
	}
	input := A{B: &B{X: 42}, C: B{X: 7}}
	got := roundTrip(t, schema, input)
	if got.B == nil || got.B.X != 42 || got.C.X != 7 {
		t.Errorf("got %+v", got)
	}
	// Also test nil (null second).
	input2 := A{B: nil, C: B{X: 7}}
	got2 := roundTrip(t, schema, input2)
	if got2.B != nil {
		t.Errorf("got %+v, want B=nil", got2)
	}
}

// TestDecimalScale covers the decimal logical type with explicit scale
// for both bytes and fixed types.
func TestDecimalScale(t *testing.T) {
	t.Run("bytes", func(t *testing.T) {
		schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
		s := mustParse(t, schema)
		val := big.NewRat(314, 100)
		dst := mustAppendEncode(t, s, nil, val)
		var got big.Rat
		mustDecode(t, s, dst, &got)
	})
	t.Run("fixed", func(t *testing.T) {
		schema := `{"type":"fixed","name":"dec","size":8,"logicalType":"decimal","precision":10,"scale":2}`
		s := mustParse(t, schema)
		val := big.NewRat(314, 100)
		dst := mustAppendEncode(t, s, nil, val)
		var got big.Rat
		mustDecode(t, s, dst, &got)
	})
	t.Run("fixed_no_scale", func(t *testing.T) {
		// Decimal with precision but no explicit scale (scale defaults to 0).
		schema := `{"type":"fixed","name":"dec2","size":8,"logicalType":"decimal","precision":10}`
		s := mustParse(t, schema)
		val := big.NewRat(314, 1) // integer value, scale=0
		dst := mustAppendEncode(t, s, nil, val)
		var got big.Rat
		mustDecode(t, s, dst, &got)
	})
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
	schema := superheroUnionSchema
	s := mustParse(t, schema)
	hero := Superhero{
		ID: 1, AffiliationID: 2, Name: "X", Life: 1, Energy: 2,
		Powers: []*Superpower{{ID: 1, Name: "P", Damage: 1, Energy: 1, Passive: true}},
	}
	// First pass: compiles fast paths for inner records.
	enc1 := mustAppendEncode(t, s, nil, &hero)
	var out1 Superhero
	mustDecode(t, s, enc1, &out1)
	// Second pass: now inner fast paths are compiled, so
	// serRecordFastPtr / deserRecordFastPtr branches are taken.
	enc2 := mustAppendEncode(t, s, nil, &hero)
	var out2 Superhero
	mustDecode(t, s, enc2, &out2)
	if out2.Powers[0].Name != "P" {
		t.Errorf("second pass mismatch: %+v", out2.Powers[0])
	}
}

// TestWarmFastPathNullUnionRecord does two round-trips for a null-union
// record field to exercise the allFast serRecordFastPtr / deserRecordFastPtr
// branches inside usNullUnionRecord / udNullUnionRecord.
func TestWarmFastPathNullUnionRecord(t *testing.T) {
	schema := longListSchema
	s := mustParse(t, schema)
	input := LongList{Value: 1, Next: &LongList{Value: 2}}
	// First pass: compiles fast paths.
	enc1, _ := s.AppendEncode(nil, &input)
	var out1 LongList
	s.Decode(enc1, &out1)
	// Second pass: exercises serRecordFastPtr / deserRecordFastPtr.
	enc2 := mustAppendEncode(t, s, nil, &input)
	var out2 LongList
	mustDecode(t, s, enc2, &out2)
	if out2.Next == nil || out2.Next.Value != 2 {
		t.Errorf("second pass: got %+v", out2)
	}
}

// TestWarmFastPathArrayNullUnionRecord does two round-trips for an array
// of null-union records to exercise the allFast branch inside
// usArrayNullUnionRecord.
func TestWarmFastPathArrayNullUnionRecord(t *testing.T) {
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
	s := mustParse(t, schema)
	input := Outer{Items: []*Inner{{X: 1}, nil, {X: 3}}}
	// First pass: warms up.
	enc1, _ := s.AppendEncode(nil, &input)
	var out1 Outer
	s.Decode(enc1, &out1)
	// Second pass: hits fast branches.
	enc2 := mustAppendEncode(t, s, nil, &input)
	var out2 Outer
	mustDecode(t, s, enc2, &out2)
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
	s := mustParse(t, schema)
	// Warm up: encode valid value to compile fast paths.
	valid := Outer{Item: &Inner{P: ptr(int32(42))}}
	mustAppendEncode(t, s, nil, &valid)
	var dummy Outer
	enc, _ := s.AppendEncode(nil, &valid)
	s.Decode(enc, &dummy)
	// Now encode with nil P → error in serRecordFastPtr.
	bad := Outer{Item: &Inner{P: nil}}
	_, err := s.AppendEncode(nil, &bad)
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
	schema := nullableInnerSchema
	s := mustParse(t, schema)
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
	_, err := s.Decode(enc[:2], &out3)
	if err == nil {
		t.Fatal("expected error for truncated data")
	}
}

// TestArrayPtrRecordNilError covers the nil pointer error in usArrayPtrRecord
// when an array element is nil but the schema items are a record (not null-union).
func TestArrayPtrRecordNilError(t *testing.T) {
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
	s := mustParse(t, schema)
	bad := Outer{Items: []*Inner{nil}}
	_, err := s.AppendEncode(nil, &bad)
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
		schema := arrayOfIntSchema
		s := mustParse(t, schema)
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
		_, err := s.Decode(data, &out2)
		if err != nil {
			t.Fatalf("decode negative count block: %v", err)
		}
		if !reflect.DeepEqual(out2.Vals, []int32{1, 2, 3}) {
			t.Errorf("got %v, want [1 2 3]", out2.Vals)
		}
	})

	// Test with ptr record array to exercise udArrayPtrRecord.
	t.Run("ptr_record", func(t *testing.T) {
		type Wrapper struct {
			Items []*Rec `avro:"items"`
		}
		schema := arrayOfPtrRecSchema
		s := mustParse(t, schema)
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
		_, err := s.Decode(data, &out2)
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
		schema := arrayOfIntSchema
		s := mustParse(t, schema)
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
		_, err := s.Decode(data, &out)
		if err != nil {
			t.Fatalf("decode multi-block: %v", err)
		}
		if !reflect.DeepEqual(out.Vals, []int32{1, 2, 3}) {
			t.Errorf("got %v, want [1 2 3]", out.Vals)
		}
	})

	t.Run("ptr_record", func(t *testing.T) {
		type Wrapper struct {
			Items []*Rec `avro:"items"`
		}
		schema := arrayOfPtrRecSchema
		s := mustParse(t, schema)
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
		_, err := s.Decode(data, &out)
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
		schema := arrayOfIntSchema
		s := mustParse(t, schema)
		// Warm up so fast path is compiled.
		s.AppendEncode(nil, &Wrapper{Vals: []int32{1}})

		// Truncated: count says 2 elements but only 1 provided.
		var data []byte
		data = appendVarlong(data, 2)
		data = appendVarint(data, 1) // only 1 element
		var out Wrapper
		_, err := s.Decode(data, &out)
		if err == nil {
			t.Fatal("expected error for truncated array data")
		}
	})

	t.Run("ptr_record_truncated", func(t *testing.T) {
		type Wrapper struct {
			Items []*Rec `avro:"items"`
		}
		schema := arrayOfPtrRecSchema
		s := mustParse(t, schema)
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
		_, err := s.Decode(data, &out)
		if err == nil {
			t.Fatal("expected error for truncated array data")
		}
	})

	t.Run("direct_truncated_readvarlong", func(t *testing.T) {
		schema := arrayOfIntSchema
		s := mustParse(t, schema)
		s.AppendEncode(nil, &Wrapper{Vals: []int32{1}})
		// Empty data: readVarlong fails.
		var out Wrapper
		_, err := s.Decode([]byte{}, &out)
		if err == nil {
			t.Fatal("expected error for empty data")
		}
	})

	t.Run("ptr_record_truncated_readvarlong", func(t *testing.T) {
		type Wrapper struct {
			Items []*Rec `avro:"items"`
		}
		schema := arrayOfPtrRecSchema
		s := mustParse(t, schema)
		normal := Wrapper{Items: []*Rec{{V: 1}}}
		enc, _ := s.AppendEncode(nil, &normal)
		var dummy Wrapper
		s.Decode(enc, &dummy)
		// Empty data: readVarlong fails.
		var out Wrapper
		_, err := s.Decode([]byte{}, &out)
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
	schema := arrayOfPtrInnerSchema
	s := mustParse(t, schema)
	bad := Outer{Items: []Inner{{P: nil}}}
	_, err := s.AppendEncode(nil, &bad)
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
	schema := nullableIntSchema
	s := mustParse(t, schema)
	input := Wrapper{Value: 42}
	encoded := mustAppendEncode(t, s, nil, &input)
	var out Wrapper
	mustDecode(t, s, encoded, &out)
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
	mustDecode(t, s, encoded, &out)
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
	type Wrapper struct {
		Items []*Rec `avro:"items"`
	}
	schema := arrayOfPtrRecSchema
	input := Wrapper{Items: []*Rec{}}
	got := roundTrip(t, schema, input)
	if len(got.Items) != 0 {
		t.Errorf("expected empty, got %v", got.Items)
	}
}

// TestUdNullUnionRecordErrors covers error paths in udNullUnionRecord
// after the fast path has been warmed up.
func TestUdNullUnionRecordErrors(t *testing.T) {
	schema := longListSchema
	s := mustParse(t, schema)
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
		schema := arrayOfPtrInnerSchema
		s := mustParse(t, schema)
		// Warm up with valid data.
		valid := Outer{Items: []*Inner{{P: ptr(int32(1))}}}
		enc, _ := s.AppendEncode(nil, &valid)
		var dummy Outer
		s.Decode(enc, &dummy)
		s.AppendEncode(nil, &valid) // second call warms inner fast path

		// Now encode with nil P inside a record → error.
		bad := Outer{Items: []*Inner{{P: nil}}}
		_, err := s.AppendEncode(nil, &bad)
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
		schema := arrayOfNullableInnerSchema
		s := mustParse(t, schema)
		// Warm up.
		valid := Outer{Items: []*Inner{{P: ptr(int32(1))}}}
		enc, _ := s.AppendEncode(nil, &valid)
		var dummy Outer
		s.Decode(enc, &dummy)
		s.AppendEncode(nil, &valid) // warm inner fast path

		// Encode with nil P → error in inner record ser.
		bad := Outer{Items: []*Inner{{P: nil}}}
		_, err := s.AppendEncode(nil, &bad)
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
		schema := arrayOfPtrInnerSchema
		s := mustParse(t, schema)
		valid := Outer{Items: []Inner{{P: ptr(int32(1))}}}
		enc, _ := s.AppendEncode(nil, &valid)
		var dummy Outer
		s.Decode(enc, &dummy)
		s.AppendEncode(nil, &valid)

		bad := Outer{Items: []Inner{{P: nil}}}
		_, err := s.AppendEncode(nil, &bad)
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
		schema := arrayOfPtrInnerSchema
		s := mustParse(t, schema)
		valid := Outer{Items: []Inner{{P: ptr(int32(1))}}}
		s.AppendEncode(nil, &valid) // warm up

		bad := Outer{Items: []Inner{{P: nil}}}
		_, err := s.AppendEncode(nil, &bad)
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
		schema := arrayOfNullableInnerSchema
		s := mustParse(t, schema)
		valid := Outer{Items: []*Inner{{P: ptr(int32(1))}}}
		s.AppendEncode(nil, &valid) // warm up

		bad := Outer{Items: []*Inner{{P: nil}}}
		_, err := s.AppendEncode(nil, &bad)
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
		schema := arrayOfIntSchema
		s := mustParse(t, schema)
		big := Wrapper{Vals: []int32{1, 2, 3, 4, 5}}
		encBig, _ := s.AppendEncode(nil, &big)
		small := Wrapper{Vals: []int32{1, 2}}
		encSmall, _ := s.AppendEncode(nil, &small)
		// First decode: allocates slice cap=5.
		var out Wrapper
		s.Decode(encBig, &out)
		// Second decode: reuses cap=5 for 2 elements → else branch.
		mustDecode(t, s, encSmall, &out)
		if !reflect.DeepEqual(out.Vals, []int32{1, 2}) {
			t.Errorf("got %v, want [1 2]", out.Vals)
		}
	})

	t.Run("ptr_record", func(t *testing.T) {
		type Wrapper struct {
			Items []*Rec `avro:"items"`
		}
		schema := arrayOfPtrRecSchema
		s := mustParse(t, schema)
		big := Wrapper{Items: []*Rec{{V: 1}, {V: 2}, {V: 3}}}
		encBig, _ := s.AppendEncode(nil, &big)
		small := Wrapper{Items: []*Rec{{V: 10}}}
		encSmall, _ := s.AppendEncode(nil, &small)
		// First decode: allocates.
		var out Wrapper
		s.Decode(encBig, &out)
		// Second decode: reuses capacity → else branch.
		mustDecode(t, s, encSmall, &out)
		if len(out.Items) != 1 || out.Items[0].V != 10 {
			t.Errorf("got %+v, want [{10}]", out.Items)
		}
	})
}

// TestArrayNegativeCountReadVarlongError covers the error path when a
// negative count block has a truncated byte-size field.
func TestArrayNegativeCountReadVarlongError(t *testing.T) {
	t.Run("direct", func(t *testing.T) {
		schema := arrayOfIntSchema
		s := mustParse(t, schema)
		s.AppendEncode(nil, &Wrapper{Vals: []int32{1}}) // warm up

		// Negative count with truncated byte-size.
		var data []byte
		data = appendVarlong(data, -3) // negative count
		// No byte-size follows → readVarlong error.
		var out Wrapper
		_, err := s.Decode(data, &out)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("ptr_record", func(t *testing.T) {
		type Wrapper struct {
			Items []*Rec `avro:"items"`
		}
		schema := arrayOfPtrRecSchema
		s := mustParse(t, schema)
		normal := Wrapper{Items: []*Rec{{V: 1}}}
		enc, _ := s.AppendEncode(nil, &normal)
		var dummy Wrapper
		s.Decode(enc, &dummy) // warm up

		// Negative count with truncated byte-size.
		var data []byte
		data = appendVarlong(data, -3)
		var out Wrapper
		_, err := s.Decode(data, &out)
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
	s := mustParse(t, schema)
	m := map[string]int32{"x": 10}
	input := Wrapper{M: &m}
	encoded := mustAppendEncode(t, s, nil, &input)
	var out Wrapper
	mustDecode(t, s, encoded, &out)
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
	schema := nestedInnerSchema
	s := mustParse(t, schema)
	input := Outer{Inner: map[string]any{"x": int32(42)}}
	encoded := mustAppendEncode(t, s, nil, &input)
	var out Outer
	mustDecode(t, s, encoded, &out)
	if out.Inner["x"] != int32(42) {
		t.Errorf("got %v, want 42", out.Inner["x"])
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
	s := mustParse(t, schema)
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
	s := mustParse(t, schema)
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
		s := mustParse(t, schema)
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
		type R struct {
			Items []*Inner `avro:"items"`
		}
		schema := `{"type":"record","name":"R","fields":[
			{"name":"items","type":{"type":"array","items":{
				"type":"record","name":"Inner",
				"fields":[{"name":"x","type":"int"}]
			}}}
		]}`
		s := mustParse(t, schema)
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
		s := mustParse(t, schema)
		data := appendVarlong(nil, math.MinInt64)
		var out []int32
		_, err := s.Decode(data, &out)
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
		s := mustParse(t, schema)
		data := appendVarlong(nil, math.MinInt64)
		var out R
		_, err := s.Decode(data, &out)
		if err == nil {
			t.Fatal("expected error for MinInt64 block count")
		}
	})

	t.Run("array_unsafe_ptr_record", func(t *testing.T) {
		type R struct {
			Items []*Inner `avro:"items"`
		}
		schema := `{"type":"record","name":"R","fields":[
			{"name":"items","type":{"type":"array","items":{
				"type":"record","name":"Inner",
				"fields":[{"name":"x","type":"int"}]
			}}}
		]}`
		s := mustParse(t, schema)
		data := appendVarlong(nil, math.MinInt64)
		var out R
		_, err := s.Decode(data, &out)
		if err == nil {
			t.Fatal("expected error for MinInt64 block count")
		}
	})

	t.Run("map_reflect", func(t *testing.T) {
		schema := `{"type":"map","values":"int"}`
		s := mustParse(t, schema)
		data := appendVarlong(nil, math.MinInt64)
		var out map[string]int32
		_, err := s.Decode(data, &out)
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
		s := mustParse(t, schema)
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
		type R struct {
			V *Inner `avro:"v"`
		}
		schema := `{"type":"record","name":"R","fields":[
			{"name":"v","type":["null",{
				"type":"record","name":"Inner",
				"fields":[{"name":"x","type":"int"}]
			}]}
		]}`
		s := mustParse(t, schema)
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
	s := mustParse(t, schema)
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
	s := mustParse(t, schema)
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
	s := mustParse(t, schema)
	input := R{S: "hello", B: []byte{1, 2, 3, 4, 5}}
	encoded := mustAppendEncode(t, s, nil, &input)

	var out R
	mustDecode(t, s, encoded, &out)

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
	s := mustParse(t, schema)
	first := R{A: 100, B: "first", C: true}
	enc1, _ := s.AppendEncode(nil, &first)
	second := R{A: 200, B: "second", C: false}
	enc2, _ := s.AppendEncode(nil, &second)

	var out R
	mustDecode(t, s, enc1, &out)
	if out.A != 100 || out.B != "first" || out.C != true {
		t.Fatalf("first decode wrong: %+v", out)
	}
	// Re-decode second value over the same struct.
	mustDecode(t, s, enc2, &out)
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
	s := mustParse(t, schema)

	val := int32(42)
	enc1, _ := s.AppendEncode(nil, &R{V: &val})
	enc2, _ := s.AppendEncode(nil, &R{V: nil})

	var out R
	mustDecode(t, s, enc1, &out)
	if out.V == nil || *out.V != 42 {
		t.Fatalf("first decode: got %v", out.V)
	}
	// Decode null over the non-null value.
	mustDecode(t, s, enc2, &out)
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
		decodeErr(t, `"int"`, data, ptr(int32(0)))
	})
	t.Run("varlong_overflow", func(t *testing.T) {
		// 10-byte varlong with all continuation bits set: overflows 64 bits.
		data := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}
		decodeErr(t, `"long"`, data, ptr(int64(0)))
	})
	t.Run("non_terminating_varint", func(t *testing.T) {
		// 4 bytes all with continuation bit, then EOF.
		data := []byte{0xFF, 0xFF, 0xFF, 0xFF}
		decodeErr(t, `"int"`, data, ptr(int32(0)))
	})
}

// TestAdversarialMapKeyLengthLie exercises map deserialization with
// adversarial key lengths.
func TestAdversarialMapKeyLengthLie(t *testing.T) {
	schema := `{"type":"map","values":"int"}`
	s := mustParse(t, schema)
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
	s := mustParse(t, schema)
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
	s := mustParse(t, `"string"`)
	encoded := mustAppendEncode(t, s, nil, ptr("hello"))
	var v testTextUnmarshaler
	mustDecode(t, s, encoded, &v)
	if v.val != "unmarshaled:hello" {
		t.Fatalf("got %q, want %q", v.val, "unmarshaled:hello")
	}
}

func TestTextMarshalerRoundTrip(t *testing.T) {
	type R struct {
		Name testTextMarshaler `avro:"name"`
	}
	type RD struct {
		Name testTextUnmarshaler `avro:"name"`
	}

	schema := `{"type":"record","name":"r","fields":[{"name":"name","type":"string"}]}`
	s := mustParse(t, schema)
	input := R{Name: testTextMarshaler{val: "world"}}
	encoded := mustAppendEncode(t, s, nil, &input)
	var output RD
	mustDecode(t, s, encoded, &output)
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
	// Per Avro 1.12 spec / Java reference, local-timestamp encodes the
	// wall-clock fields as-if-UTC; decode returns a UTC time.Time with
	// matching wall-clock components. Use a UTC input so the host
	// timezone doesn't affect the round-trip.
	now := time.UnixMilli(time.Now().UnixMilli()).UTC()
	got := roundTrip(t, schema, now)
	if !got.Equal(now) {
		t.Fatalf("local-timestamp-millis round trip: got %v, want %v", got, now)
	}
}

func TestLocalTimestampMicrosRoundTrip(t *testing.T) {
	schema := `{"type":"long","logicalType":"local-timestamp-micros"}`
	now := time.UnixMicro(time.Now().UnixMicro()).UTC()
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
	s := mustParse(t, `"string"`)
	data := mustEncode(t, s, ptr("hello"))
	var got string
	mustDecode(t, s, data, &got)
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
			"avro: field name: cannot use int with Avro type string: oops",
		},
		{
			"no field",
			&SemanticError{GoType: reflect.TypeFor[string](), AvroType: "int"},
			"avro: cannot use string with Avro type int",
		},
		{
			"go type only",
			&SemanticError{GoType: reflect.TypeFor[bool]()},
			"avro: unsupported type bool",
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
	s := mustParse(t, `"boolean"`)
	encoded := mustEncode(t, s, true)
	var n int
	_, err := s.Decode(encoded, &n)
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
	s := mustParse(t, `"boolean"`)
	var b bool
	_, err := s.Decode(nil, &b)
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

// TestRegression_OmitzeroNullSecondUnion locks in that omitzero on a null-SECOND
// union (["T","null"]) emits the correct null-branch index (0x02 = zigzag 1), not
// 0x00. Both the slow path (serRecord.ser at ser.go:704) and the fast-path
// slow-fn fallback (serRecordFast at unsafe.go:152) must look up the null
// branch's actual index; unconditionally emitting 0x00 would corrupt the wire for
// null-second unions — twmb couldn't decode its own output. The slow-path case is
// a bare struct value, hitting the ser.go shortcut through the reflect fallback.
func TestRegression_OmitzeroNullSecondUnion(t *testing.T) {
	type R struct {
		Name string `avro:"name,omitzero"`
		Tail int32  `avro:"tail"`
	}
	s := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"name","type":["string","null"]},
		{"name":"tail","type":"int"}
	]}`)
	enc := mustAppendEncode(t, s, nil, &R{Name: "", Tail: 42})
	// name → null branch (index 1, byte 0x02), tail → 42 (zigzag 0x54).
	want := []byte{0x02, 0x54}
	if !bytes.Equal(enc, want) {
		t.Fatalf("wire mismatch: got %x, want %x", enc, want)
	}
	var out R
	if _, err := s.Decode(enc, &out); err != nil {
		t.Fatalf("decode of self-produced wire bytes failed: %v (wire = %x)", err, enc)
	}
	if out.Tail != 42 {
		t.Fatalf("Tail: got %d, want 42 (decoder misaligned)", out.Tail)
	}
}

// TestRegression_OmitzeroNullSecondUnionPtr is the *T variant; this
// exercises the fast-path's slowFn fallback at unsafe.go:152.
func TestRegression_OmitzeroNullSecondUnionPtr(t *testing.T) {
	type R struct {
		Name *string `avro:"name,omitzero"`
		Tail int32   `avro:"tail"`
	}
	s := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"name","type":["string","null"]},
		{"name":"tail","type":"int"}
	]}`)
	enc := mustAppendEncode(t, s, nil, &R{Name: nil, Tail: 42})
	want := []byte{0x02, 0x54}
	if !bytes.Equal(enc, want) {
		t.Fatalf("wire mismatch: got %x, want %x", enc, want)
	}
	var out R
	if _, err := s.Decode(enc, &out); err != nil {
		t.Fatalf("decode of self-produced wire bytes failed: %v (wire = %x)", err, enc)
	}
	if out.Tail != 42 {
		t.Fatalf("Tail: got %d, want 42", out.Tail)
	}
}

// TestRegression_OmitzeroMapFillEffectiveDefaultParity pins the relationship
// between map default-fill and omitzero across the three no-written-default
// field shapes (the doc.go "Struct tags" contract):
//
//   - ["null", T]: an implicit null default is inferred, so BOTH routes encode
//     null and map fill does not error.
//   - [T, "null"]: a union default must match the first branch, so none is
//     inferred. The one divergence: omitzero encodes the null branch, map fill
//     errors on the missing key.
//   - plain T: nothing to fill with — omitzero keeps the zero, map fill errors.
func TestRegression_OmitzeroMapFillEffectiveDefaultParity(t *testing.T) {
	type R struct {
		F int64 `avro:"f,omitzero"`
	}
	for _, tc := range []struct {
		name      string
		fieldType string
		mapWire   string // hex of map-fill encoding; "" means map fill errors
		omitWire  string // hex of omitzero encoding
	}{
		{"null_first_union", `["null","long"]`, "00", "00"},
		{"null_second_union", `["long","null"]`, "", "02"},
		{"non_nullable", `"long"`, "", "00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"f","type":`+tc.fieldType+`}]}`)
			mw, merr := s.Encode(map[string]any{})
			if tc.mapWire == "" {
				if merr == nil || !strings.Contains(merr.Error(), "missing key") {
					t.Errorf("map fill: got (%x, %v), want the missing-key error", mw, merr)
				}
			} else if merr != nil || fmt.Sprintf("%x", mw) != tc.mapWire {
				t.Errorf("map fill: got (%x, %v), want wire %s", mw, merr, tc.mapWire)
			}
			ow, oerr := s.Encode(&R{})
			if oerr != nil || fmt.Sprintf("%x", ow) != tc.omitWire {
				t.Errorf("omitzero: got (%x, %v), want wire %s", ow, oerr, tc.omitWire)
			}
		})
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
	schema := recIntBSchema

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
			mustParse(t, tt.schema)
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
	s := mustParse(t, `"string"`)
	encoded := mustAppendEncode(t, s, nil, ptr("hello"))
	var v testTextUnmarshalerErr
	_, err := s.Decode(encoded, &v)
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
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, ptr(int64(1000))},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`, ptr(int64(1000))},
		{"date", `{"type":"int","logicalType":"date"}`, ptr(int32(100))},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`, ptr(int32(5000))},
		{"time-micros", `{"type":"long","logicalType":"time-micros"}`, ptr(int64(5000))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustParse(t, tt.schema)
			encoded := mustAppendEncode(t, s, nil, tt.encode)
			var v any
			_, err := s.Decode(encoded, &v)
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
			s := mustParse(t, tt.schema)
			// Encode a valid value.
			encoded := mustAppendEncode(t, s, nil, ptr(int64(42)))
			// Decode into incompatible type.
			var v bool
			_, err := s.Decode(encoded, &v)
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
	s := mustParse(t, schema)
	// Pass struct by value (not pointer) to force non-addressable slow path.
	var v any = R{Name: ""}
	encoded := mustAppendEncode(t, s, nil, v)
	if len(encoded) != 1 || encoded[0] != 0 {
		t.Fatalf("expected null encoding, got %x", encoded)
	}

	// Non-zero value via slow path.
	v = R{Name: "hi"}
	encoded = mustAppendEncode(t, s, nil, v)
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
			{"name":"a","type":{"type":"fixed","name":"f","size":4},"default":"\u0000\u0000\u0000\u0000"}
		]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mustParse(t, tt.schema)
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
			decodeErr(t, tt.schema, []byte{0xE6, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}, ptr(int64(0)))
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
			s := mustParse(t, tt.schema)
			_, err := s.AppendEncode(nil, tt.v)
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
		s := mustParse(t, uuidSchema)
		badStr := "not-a-uuid"
		encoded := mustEncode(t, s, &badStr)
		var out [16]byte
		_, err := s.Decode(encoded, &out)
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

// ---- Coverage: validateLogical soft-drop in buildComplex ----

func TestSchemaValidateLogicalSoftDrop(t *testing.T) {
	// date-on-string soft-drops the logical and treats as bare string,
	// matching Java's fromSchemaIgnoreInvalid (Schema.java:1979 ->
	// LogicalTypes.java:120-194) and fastavro/hamba behavior. Spec text:
	// "If a logical type is invalid, ... implementations should ignore
	// the logical type and use the underlying Avro type."
	s, err := Parse(`{"type":"string","logicalType":"date"}`)
	if err != nil {
		t.Fatalf("expected soft-drop accept for date-on-string, got: %v", err)
	}
	// Round-trip a plain string through the schema: the logical is
	// dropped so encode/decode is bare string.
	enc, err := s.AppendEncode(nil, "hello")
	if err != nil {
		t.Fatalf("encode bare string: %v", err)
	}
	var out string
	mustDecode(t, s, enc, &out)
	if out != "hello" {
		t.Fatalf("got %q want %q", out, "hello")
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
	s := mustParse(t, schema)
	encoded := mustAppendEncode(t, s, nil, &d)
	var raw [12]byte
	rem := mustDecode(t, s, encoded, &raw)
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
	s := mustParse(t, schema)
	// Only 11 bytes — needs 12.
	short := make([]byte, 11)
	var out Duration
	_, err := s.Decode(short, &out)
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
	// Use UTC input — see TestLocalTimestampMillisRoundTrip rationale.
	now := time.Now().UTC()
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
	got, ok := v.(time.Time)
	if !ok {
		t.Fatalf("expected time.Time, got %T", v)
	}
	if got.UnixNano() != now.UnixNano() {
		t.Fatalf("got %v, want %v", got, now)
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

// TestTimestampNanosDecodeIntoString pins decoder symmetry with the
// encoder for timestamp-nanos into a *string. deserTimeAsLong has a
// String arm that emits the RFC 3339 Nano form so the round-trip
// succeeds. See TestMatrix_TimeLogicalStringRoundTrip for the
// full matrix across all seven string-accepting time logicals; this
// test keeps a single-cell schema/input shape as a sanity check.
func TestTimestampNanosDecodeIntoString(t *testing.T) {
	schema := `{"type":"long","logicalType":"timestamp-nanos"}`
	now := time.Now()
	encoded := encode(t, schema, &now)
	var v string
	s, _ := Parse(schema)
	if _, err := s.Decode(encoded, &v); err != nil {
		t.Fatalf("decode nanos into string: %v", err)
	}
	if v == "" {
		t.Fatal("decoded string is empty")
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
	// duration on non-fixed soft-drops the logical (matches Java
	// fromSchemaIgnoreInvalid + hamba's parseFixedLogicalType returning
	// nil for non-(duration && size==12) combos). The schema parses as
	// bare int.
	if _, err := Parse(`{"type":"int","logicalType":"duration"}`); err != nil {
		t.Fatalf("expected soft-drop accept for duration on int, got: %v", err)
	}
	// duration on fixed with size != 12: same soft-drop (matches Java's
	// Duration.validate throw caught by fromSchemaIgnoreInvalid + hamba
	// dropping the logical via the (ltyp == Duration && size == 12)
	// match miss).
	if _, err := Parse(`{"type":"fixed","name":"d","size":8,"logicalType":"duration"}`); err != nil {
		t.Fatalf("expected soft-drop accept for duration size != 12, got: %v", err)
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
	schema := `{"type":"fixed","name":"dec","size":2,"logicalType":"decimal","precision":4,"scale":0}`
	s := mustParse(t, schema)
	// 100000 requires 3 bytes, won't fit in 2.
	r := new(big.Rat).SetInt64(100000)
	_, err := s.AppendEncode(nil, &r)
	if err == nil {
		t.Fatal("expected overflow error")
	}
}

func TestFixedDecimalFallbackToArray(t *testing.T) {
	// Deserialize fixed decimal into [8]byte instead of *big.Rat.
	schema := `{"type":"fixed","name":"dec","size":8,"logicalType":"decimal","precision":18,"scale":2}`
	s, err := Parse(schema)
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

func TestBytesDecimalDecodeIntoFloat(t *testing.T) {
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	s := mustParse(t, schema)
	encoded := mustAppendEncode(t, s, nil, 123.45)
	t.Run("float64", func(t *testing.T) {
		var f64 float64
		mustDecode(t, s, encoded, &f64)
		if f64 != 123.45 {
			t.Errorf("got %v, want 123.45", f64)
		}
	})
	t.Run("float32", func(t *testing.T) {
		var f32 float32
		mustDecode(t, s, encoded, &f32)
		if f32 != 123.45 {
			t.Errorf("got %v, want 123.45", f32)
		}
	})
}

func TestBytesDecimalDecodeIntoString(t *testing.T) {
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	s := mustParse(t, schema)
	encoded := mustAppendEncode(t, s, nil, "123.45")
	var str string
	mustDecode(t, s, encoded, &str)
	if str != "123.45" {
		t.Errorf("got %q, want %q", str, "123.45")
	}
}

func TestFixedDecimalDecodeIntoFloat(t *testing.T) {
	schema := `{"type":"fixed","name":"dec","size":8,"logicalType":"decimal","precision":18,"scale":2}`
	s := mustParse(t, schema)
	encoded := mustAppendEncode(t, s, nil, 42.5)
	var f64 float64
	mustDecode(t, s, encoded, &f64)
	if f64 != 42.5 {
		t.Errorf("got %v, want 42.5", f64)
	}
}

func TestFixedDecimalDecodeIntoString(t *testing.T) {
	schema := `{"type":"fixed","name":"dec","size":8,"logicalType":"decimal","precision":18,"scale":2}`
	s := mustParse(t, schema)
	encoded := mustAppendEncode(t, s, nil, "-99.99")
	var str string
	mustDecode(t, s, encoded, &str)
	if str != "-99.99" {
		t.Errorf("got %q, want %q", str, "-99.99")
	}
}

func TestBytesDecimalDecodeFloat32Overflow(t *testing.T) {
	// A decimal value that fits in float64 but overflows float32 (> ~3.4e38).
	schema := `{"type":"bytes","logicalType":"decimal","precision":50,"scale":0}`
	s := mustParse(t, schema)
	big, _ := new(big.Rat).SetString("1e39")
	encoded := mustAppendEncode(t, s, nil, big)
	var f32 float32
	_, err := s.Decode(encoded, &f32)
	if err == nil {
		t.Fatal("expected float32 overflow error")
	}
}

func TestBytesDecimalDecodeFloat64Overflow(t *testing.T) {
	// A decimal value that overflows float64 itself (> ~1.8e308). Without
	// the Inf guard this would silently set the target to +Inf.
	schema := `{"type":"bytes","logicalType":"decimal","precision":500,"scale":0}`
	s := mustParse(t, schema)
	huge, _ := new(big.Rat).SetString("1e400")
	encoded := mustAppendEncode(t, s, nil, huge)
	var f64 float64
	if _, err := s.Decode(encoded, &f64); err == nil {
		t.Fatal("expected float64 overflow error")
	}
	var f32 float32
	if _, err := s.Decode(encoded, &f32); err == nil {
		t.Fatal("expected float32 overflow error")
	}
}

func TestDecimalSchemaValidation(t *testing.T) {
	// decimal without precision is invalid per spec; twmb rejects at parse.
	// The references are laxer here: fastavro 1.12.2's parse validation
	// skips a missing precision entirely (parses; its writer then KeyErrors
	// at use — observed), and Java's Decimal.validate throw is caught by
	// fromSchemaIgnoreInvalid, soft-dropping the logical to bare bytes.
	// Rejecting beats both: a spec-required attribute is missing, and
	// silently dropping the decimal is a silent interop divergence.
	if _, err := Parse(`{"type":"bytes","logicalType":"decimal"}`); err == nil {
		t.Fatal("expected error for decimal missing precision")
	}
	// decimal on int is wrong-underlying-type → falls back to int (forward
	// compat for unknown logical-on-primitive combinations).
	if _, err := Parse(`{"type":"int","logicalType":"decimal","precision":10}`); err != nil {
		t.Fatalf("expected fallback to int, got error: %v", err)
	}
}

func TestBytesDecimalShortBuffer(t *testing.T) {
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	s := mustParse(t, schema)
	// Varint says 10 bytes but only 2 available.
	data := []byte{20, 0x30, 0x39} // length=10 (zigzag), only 2 data bytes
	var out *big.Rat
	_, err := s.Decode(data, &out)
	if err == nil {
		t.Fatal("expected short buffer error")
	}
}

func TestFixedDecimalShortBuffer(t *testing.T) {
	schema := `{"type":"fixed","name":"dec","size":8,"logicalType":"decimal","precision":18,"scale":2}`
	s := mustParse(t, schema)
	short := make([]byte, 7) // needs 8
	var out *big.Rat
	_, err := s.Decode(short, &out)
	if err == nil {
		t.Fatal("expected short buffer error")
	}
}

func TestBytesDecimalNegativeLength(t *testing.T) {
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	s := mustParse(t, schema)
	// Zigzag encode -1 as length → 0x01.
	data := []byte{0x01}
	var out *big.Rat
	_, err := s.Decode(data, &out)
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
	s := mustParse(t, schema)
	encoded := mustAppendEncode(t, s, nil, &raw)
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
	s := mustParse(t, schema)
	encoded := mustAppendEncode(t, s, nil, &r)
	var out any
	rem := mustDecode(t, s, encoded, &out)
	if len(rem) != 0 {
		t.Fatalf("leftover: %d", len(rem))
	}
	got, ok := out.(*big.Rat)
	if !ok {
		t.Fatalf("expected *big.Rat, got %T", out)
	}
	want := new(big.Rat).SetFrac64(12345, 100)
	if got.Cmp(want) != 0 {
		t.Fatalf("got %s, want %s", got.FloatString(2), want.FloatString(2))
	}
}

func TestFixedDecimalDeserInterface(t *testing.T) {
	schema := `{"type":"fixed","name":"dec","size":8,"logicalType":"decimal","precision":18,"scale":2}`
	r := new(big.Rat).SetFrac64(12345, 100)
	s := mustParse(t, schema)
	encoded := mustAppendEncode(t, s, nil, &r)
	var out any
	rem := mustDecode(t, s, encoded, &out)
	if len(rem) != 0 {
		t.Fatalf("leftover: %d", len(rem))
	}
	got, ok := out.(*big.Rat)
	if !ok {
		t.Fatalf("expected *big.Rat, got %T", out)
	}
	want := new(big.Rat).SetFrac64(12345, 100)
	if got.Cmp(want) != 0 {
		t.Fatalf("got %s, want %s", got.FloatString(2), want.FloatString(2))
	}
}

// ---- Decimal: type mismatch error ----

func TestBytesDecimalDeserWrongType(t *testing.T) {
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	r := new(big.Rat).SetFrac64(12345, 100)
	s := mustParse(t, schema)
	encoded := mustAppendEncode(t, s, nil, &r)
	// bool is not a supported decimal target.
	var out bool
	_, err := s.Decode(encoded, &out)
	if err == nil {
		t.Fatal("expected error decoding decimal into bool")
	}
}

// ---- Bytes decimal: truncated varint ----

func TestBytesDecimalTruncatedVarint(t *testing.T) {
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	s := mustParse(t, schema)
	// Byte with continuation bit but no following byte.
	data := []byte{0x80}
	var out *big.Rat
	_, err := s.Decode(data, &out)
	if err == nil {
		t.Fatal("expected error for truncated varint")
	}
}

// ---- Decimal: empty bytes (zero length) → bytesToBigInt empty ----

func TestBytesDecimalEmptyBytes(t *testing.T) {
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	s := mustParse(t, schema)
	// Length 0 → empty bytes → bytesToBigInt([]) → 0.
	data := []byte{0x00} // varint 0
	var out *big.Rat
	rem := mustDecode(t, s, data, &out)
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
	s := mustParse(t, schema)
	var d *Duration
	_, err := s.AppendEncode(nil, &d)
	if err == nil {
		t.Fatal("expected error for nil Duration pointer")
	}
}

func TestBytesDecimalSerNilPointer(t *testing.T) {
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	s := mustParse(t, schema)
	var r *big.Rat
	_, err := s.AppendEncode(nil, &r)
	if err == nil {
		t.Fatal("expected error for nil *big.Rat pointer")
	}
}

func TestFixedDecimalSerNilPointer(t *testing.T) {
	schema := `{"type":"fixed","name":"dec","size":8,"logicalType":"decimal","precision":18,"scale":2}`
	s := mustParse(t, schema)
	var r *big.Rat
	_, err := s.AppendEncode(nil, &r)
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
	s := mustParse(t, input)
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

// TestBigDecimalLogicalType verifies a basic big.Rat round-trip through
// the wired big-decimal encoder/decoder. Detailed wire-format coverage
// (Java ground truth, negative scale on decode, non-terminating rejection,
// carrier canonicalization) is in conformance_test.go's
// TestSpecBigDecimalWireFormat.
func TestBigDecimalLogicalType(t *testing.T) {
	s := mustParse(t, `{"type":"bytes","logicalType":"big-decimal"}`)
	in := big.NewRat(313, 100)
	enc := mustAppendEncode(t, s, nil, in)
	var got big.Rat
	mustDecode(t, s, enc, &got)
	if got.Cmp(in) != 0 {
		t.Fatalf("got %s, want %s", got.RatString(), in.RatString())
	}
}

func TestBigDecimalOnFixedSoftDrop(t *testing.T) {
	// big-decimal requires bytes; on fixed (any size), the logical
	// soft-drops. Matches Java's BigDecimal.validate at
	// LogicalTypes.java:466-470 (throws for non-bytes; caught by
	// fromSchemaIgnoreInvalid → silent drop). Schema parses as bare
	// fixed(16); the user's wire bytes are treated opaquely.
	if _, err := Parse(`{"type":"fixed","name":"F","size":16,"logicalType":"big-decimal"}`); err != nil {
		t.Fatalf("expected soft-drop accept for big-decimal on fixed, got: %v", err)
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
	s := mustParse(t, schema)
	// Round-trip a fixed(4) value.
	input := [4]byte{0x00, 0x01, 0x86, 0xa0} // 100000 unscaled = 1000.00
	encoded := mustEncode(t, s, input)
	var got [4]byte
	mustDecode(t, s, encoded, &got)
	if got != input {
		t.Fatalf("got %x, want %x", got, input)
	}
}

func TestDecimalFixedInvalidPrecision(t *testing.T) {
	// size=1 can hold at most floor(log10(2^7-1)) = 2 digits;
	// precision=3 exceeds capacity. Java rejects, we do too.
	if _, err := Parse(`{"type":"fixed","name":"D","size":1,"logicalType":"decimal","precision":3}`); err == nil {
		t.Fatal("expected error for precision > fixed capacity")
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
	s := mustParse(t, schema)
	now := time.Now()
	encoded := mustEncode(t, s, &now)
	var got time.Time
	mustDecode(t, s, encoded, &got)
	if !got.Equal(now) {
		t.Fatalf("got %v, want %v", got, now)
	}
}

// ---- Coverage: UUID ser with [16]byte through non-record path ----

func TestSerUUIDArrayType(t *testing.T) {
	schema := `{"type":"string","logicalType":"uuid"}`
	s := mustParse(t, schema)
	u := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	encoded := mustEncode(t, s, u)
	var got string
	mustDecode(t, s, encoded, &got)
	if got != "550e8400-e29b-41d4-a716-446655440000" {
		t.Fatalf("got %s, want 550e8400-e29b-41d4-a716-446655440000", got)
	}
}

// ---- Coverage: serUUID error path (non-uuid, non-string type) ----

func TestSerUUIDTypeError(t *testing.T) {
	schema := `{"type":"string","logicalType":"uuid"}`
	s := mustParse(t, schema)
	_, err := s.Encode(42)
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
	schema := recUUIDStringSchema
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

// ---- Coverage: fixed(16) UUID into various target types ----

func TestUUIDFixed16IntoAny(t *testing.T) {
	schema := `{"type":"fixed","name":"uuid_f","size":16,"logicalType":"uuid"}`
	s := MustParse(schema)
	input := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	enc := mustEncode(t, s, input)
	var got any
	mustDecode(t, s, enc, &got)
	arr, ok := got.([16]byte)
	if !ok {
		t.Fatalf("expected [16]byte, got %T", got)
	}
	if arr != input {
		t.Fatalf("got %x, want %x", arr, input)
	}
}

func TestUUIDFixed16IntoString(t *testing.T) {
	schema := `{"type":"fixed","name":"uuid_f","size":16,"logicalType":"uuid"}`
	s := MustParse(schema)
	input := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	enc := mustEncode(t, s, input)
	var got string
	mustDecode(t, s, enc, &got)
	want := "550e8400-e29b-41d4-a716-446655440000"
	if got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestUUIDFixed16IntoBytes(t *testing.T) {
	schema := `{"type":"fixed","name":"uuid_f","size":16,"logicalType":"uuid"}`
	s := MustParse(schema)
	input := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	enc := mustEncode(t, s, input)
	var got []byte
	mustDecode(t, s, enc, &got)
	if len(got) != 16 {
		t.Fatalf("expected 16 bytes, got %d", len(got))
	}
	if [16]byte(got) != input {
		t.Fatalf("got %x, want %x", got, input)
	}
}

func TestUUIDFixed16EncodeFromString(t *testing.T) {
	schema := `{"type":"fixed","name":"uuid_f","size":16,"logicalType":"uuid"}`
	s := MustParse(schema)
	enc := mustEncode(t, s, "550e8400-e29b-41d4-a716-446655440000")
	want := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	if [16]byte(enc) != want {
		t.Fatalf("got %x, want %x", enc, want)
	}
}

func TestUUIDFixed16StructString(t *testing.T) {
	type R struct {
		ID string `avro:"id"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"id","type":{"type":"fixed","name":"uuid_f","size":16,"logicalType":"uuid"}}
	]}`
	s := MustParse(schema)

	// Encode string, decode string
	enc := mustEncode(t, s, &R{ID: "550e8400-e29b-41d4-a716-446655440000"})
	var got R
	mustDecode(t, s, enc, &got)
	if got.ID != "550e8400-e29b-41d4-a716-446655440000" {
		t.Fatalf("got %s", got.ID)
	}
}

func TestUUIDFixed16IntoUnsupportedType(t *testing.T) {
	s := MustParse(`{"type":"fixed","name":"u","size":16,"logicalType":"uuid"}`)
	data := make([]byte, 16)
	var v int
	_, err := s.Decode(data, &v)
	if err == nil {
		t.Fatal("expected error decoding fixed uuid into int")
	}
}

func TestUUIDFixed16EncodeFromBytes(t *testing.T) {
	s := MustParse(`{"type":"fixed","name":"u","size":16,"logicalType":"uuid"}`)
	input := make([]byte, 16)
	input[0] = 0xAB
	enc := mustEncode(t, s, input)
	if len(enc) != 16 || enc[0] != 0xAB {
		t.Fatalf("unexpected encoding: %x", enc)
	}
}

// ---- Coverage: struct field shadowing (shallower wins) ----

func TestStructFieldShadowing(t *testing.T) {
	type Outer struct {
		Inner
		X *int32 `avro:"x"`
	}
	schema := `{"type":"record","name":"Outer","fields":[{"name":"x","type":["null","int"]}]}`
	s := MustParse(schema)
	val := int32(42)
	enc := mustEncode(t, s, &Outer{X: &val})
	var out Outer
	mustDecode(t, s, enc, &out)
	if out.X == nil {
		t.Fatal("shallower *int32 field was not populated")
	}
	if *out.X != 42 {
		t.Fatalf("got %d, want 42", *out.X)
	}
}

// ---- Coverage: implicit null default for nullable unions ----

func TestImplicitNullDefaultMap(t *testing.T) {
	schema := `{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":["null","int"]}
	]}`
	s := MustParse(schema)
	enc, err := s.Encode(map[string]any{"a": int32(1)})
	if err != nil {
		t.Fatalf("encoding map with missing nullable key should succeed: %v", err)
	}
	var out map[string]any
	mustDecode(t, s, enc, &out)
	if out["b"] != nil {
		t.Fatalf("expected nil for implicit null default, got %v", out["b"])
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
	// Java's LogicalTypes.Decimal.validate throws for precision <= 0
	// (LogicalTypes.java:383-385) — though its schema parse catches the
	// throw and soft-drops the logical rather than failing. fastavro's
	// parse_schema hard-rejects NEGATIVE precision but its `if precision:`
	// truthiness guard skips the check for 0 (parses, observed 1.12.2).
	// twmb hard-rejects the whole <= 0 range: a decimal that can hold
	// zero digits is malformed per the spec's positive-precision rule.
	if _, err := Parse(`{"type":"bytes","logicalType":"decimal","precision":0}`); err == nil {
		t.Fatal("expected error for decimal precision=0")
	}
}

func TestDecimalScaleExceedsPrecision(t *testing.T) {
	if _, err := Parse(`{"type":"bytes","logicalType":"decimal","precision":5,"scale":6}`); err == nil {
		t.Fatal("expected error for decimal scale > precision")
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
	schema := recUUIDStringSchema
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
	schema := recUUIDStringSchema
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
	schema := recUUIDStringSchema
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
	schema := recUUIDStringSchema
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
	schema := recUUIDStringSchema
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

// TestDecodeReuseAnyTarget covers indirectAlloc's interface-target
// handling. indirectAlloc keeps the interface as the destination
// unless its inner is a non-nil pointer (where the pointee IS
// addressable). Without that guard, decoding twice into the same
// *any panics with "SetInt using unaddressable value": the first
// decode populates *any with a concrete (e.g. int32) value, and the
// second decode unwraps the non-nil interface to that unaddressable
// inner value, so SetInt panics.
func TestDecodeReuseAnyTarget(t *testing.T) {
	cases := []struct {
		name    string
		schema  string
		srcs    [][]byte
		jsonSrc [][]byte
	}{
		{
			name:    "int",
			schema:  `"int"`,
			srcs:    [][]byte{{84}, {86}, {88}},
			jsonSrc: [][]byte{[]byte(`42`), []byte(`43`)},
		},
		{
			name:    "string",
			schema:  `"string"`,
			srcs:    [][]byte{{2, 'a'}, {4, 'b', 'c'}, {0}},
			jsonSrc: [][]byte{[]byte(`"a"`), []byte(`"bc"`)},
		},
		{
			name:    "boolean",
			schema:  `"boolean"`,
			srcs:    [][]byte{{0}, {1}, {0}},
			jsonSrc: [][]byte{[]byte(`true`), []byte(`false`)},
		},
		{
			name:    "nullable union",
			schema:  `["null","int"]`,
			srcs:    [][]byte{{0}, {2, 84}, {0}},
			jsonSrc: [][]byte{[]byte(`null`), []byte(`{"int":42}`)},
		},
		{
			name:    "record",
			schema:  `{"type":"record","name":"r","fields":[{"name":"x","type":"int"}]}`,
			srcs:    [][]byte{{84}, {86}},
			jsonSrc: [][]byte{[]byte(`{"x":1}`), []byte(`{"x":2}`)},
		},
		{
			name:    "array",
			schema:  `{"type":"array","items":"int"}`,
			srcs:    [][]byte{{2, 84, 0}, {0}},
			jsonSrc: [][]byte{[]byte(`[1,2]`), []byte(`[]`)},
		},
		{
			name:    "map",
			schema:  `{"type":"map","values":"int"}`,
			srcs:    [][]byte{{2, 2, 'k', 84, 0}, {0}},
			jsonSrc: [][]byte{[]byte(`{"k":1}`), []byte(`{}`)},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name+"_binary", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic: %v", r)
				}
			}()
			s := mustParse(t, tc.schema)
			var v any
			for i, src := range tc.srcs {
				if _, err := s.Decode(src, &v); err != nil {
					t.Fatalf("decode #%d: %v", i, err)
				}
			}
		})
		t.Run(tc.name+"_json", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic: %v", r)
				}
			}()
			s := mustParse(t, tc.schema)
			var v any
			for i, src := range tc.jsonSrc {
				if err := s.DecodeJSON(src, &v); err != nil {
					t.Fatalf("decode #%d: %v", i, err)
				}
			}
		})
	}
}

// TestDecodeNonEmptyInterfaceTarget covers the setIface fix. Before the
// fix, decoding any schema into *interface{Foo()} panicked with
// "reflect.Set: value of type X is not assignable to type
// interface{Foo()}" — reflect.Value.Set has no built-in assignability
// guard for interface targets. The decoder now returns a SemanticError
// from setIface instead.
func TestDecodeNonEmptyInterfaceTarget(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		bin    []byte
		json   string
	}{
		{"int", `"int"`, []byte{84}, `42`},
		{"long", `"long"`, []byte{84}, `42`},
		{"float", `"float"`, []byte{0, 0, 0, 0}, `0`},
		{"double", `"double"`, []byte{0, 0, 0, 0, 0, 0, 0, 0}, `0`},
		{"string", `"string"`, []byte{2, 'x'}, `"x"`},
		{"bytes", `"bytes"`, []byte{0}, `""`},
		{"boolean", `"boolean"`, []byte{1}, `true`},
		{"enum", `{"type":"enum","name":"e","symbols":["A"]}`, []byte{0}, `"A"`},
		{"fixed", `{"type":"fixed","name":"f","size":4}`, []byte{1, 2, 3, 4}, `"abcd"`},
		{"array", `{"type":"array","items":"int"}`, []byte{2, 84, 0}, `[42]`},
		{"map", `{"type":"map","values":"int"}`, []byte{0}, `{}`},
		{"record", `{"type":"record","name":"r","fields":[{"name":"x","type":"int"}]}`, []byte{84}, `{"x":1}`},
		{"date", `{"type":"int","logicalType":"date"}`, []byte{84}, `42`},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, []byte{84}, `42`},
		// null branches produce nil, which IS assignable to any
		// interface — those legitimately decode without error.
		// Only non-null produced values need the assignability error.
		{"nullable union value", `["null","int"]`, []byte{2, 84}, `{"int":42}`},
		{"3-branch bare", `["null","int","string"]`, nil, `"hello"`},
	}
	for _, tc := range cases {
		t.Run(tc.name+"_binary", func(t *testing.T) {
			if tc.bin == nil {
				t.Skip("no binary input")
			}
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic: %v", r)
				}
			}()
			s := mustParse(t, tc.schema)
			var v interface{ Foo() }
			_, err := s.Decode(tc.bin, &v)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
		t.Run(tc.name+"_json", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic: %v", r)
				}
			}()
			s := mustParse(t, tc.schema)
			var v interface{ Foo() }
			err := s.DecodeJSON([]byte(tc.json), &v)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

// TestDecodeReuseAnyTargetStaleKeys pins the documented stale-key
// behavior of map reuse in deserRecord.deser and decodeRecordAny:
// when *any already wraps a map[string]any, the decoder overwrites
// keys present in the schema and leaves any other keys untouched.
// This matches encoding/json's behavior when unmarshaling into a
// non-empty map. Callers that want a fresh decode should clear or
// replace the map.
func TestDecodeReuseAnyTargetStaleKeys(t *testing.T) {
	schemaA := `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`
	schemaB := `{"type":"record","name":"S","fields":[{"name":"y","type":"int"}]}`
	t.Run("binary_pre_seeded", func(t *testing.T) {
		sa := mustParse(t, schemaA)
		encoded := mustAppendEncode(t, sa, nil, map[string]any{"x": int32(7)})
		var v any = map[string]any{"stale": "keep me"}
		mustDecode(t, sa, encoded, &v)
		m, ok := v.(map[string]any)
		if !ok {
			t.Fatalf("got %T, want map[string]any", v)
		}
		if m["x"] != int32(7) {
			t.Fatalf("schema field not overwritten: x=%v", m["x"])
		}
		if got, ok := m["stale"]; !ok || got != "keep me" {
			t.Fatalf("stale key dropped: m=%v", m)
		}
	})
	t.Run("binary_different_schema", func(t *testing.T) {
		sa := mustParse(t, schemaA)
		sb := mustParse(t, schemaB)
		encA, _ := sa.AppendEncode(nil, map[string]any{"x": int32(1)})
		encB, _ := sb.AppendEncode(nil, map[string]any{"y": int32(2)})
		var v any
		mustDecode(t, sa, encA, &v)
		mustDecode(t, sb, encB, &v)
		m := v.(map[string]any)
		if m["y"] != int32(2) {
			t.Fatalf("y not set: %v", m)
		}
		if _, ok := m["x"]; !ok {
			t.Fatalf("x cleared after second decode: %v", m)
		}
	})
	t.Run("json_pre_seeded", func(t *testing.T) {
		sa := mustParse(t, schemaA)
		var v any = map[string]any{"stale": "keep me"}
		mustDecodeJSON(t, sa, []byte(`{"x":7}`), &v)
		m := v.(map[string]any)
		if m["x"] != int32(7) {
			t.Fatalf("x not overwritten: %v", m)
		}
		if got, ok := m["stale"]; !ok || got != "keep me" {
			t.Fatalf("stale key dropped: m=%v", m)
		}
	})
}

func TestSetLongValueInterface(t *testing.T) {
	var v any
	rv := reflect.ValueOf(&v).Elem()
	if err := setLongValue(rv, 42); err != nil {
		t.Fatal(err)
	}
	if v.(int64) != 42 {
		t.Errorf("got %v", v)
	}
}

func TestSetIntValueInterface(t *testing.T) {
	var v any
	rv := reflect.ValueOf(&v).Elem()
	if err := setIntValue(rv, 7); err != nil {
		t.Fatal(err)
	}
	if v.(int32) != 7 {
		t.Errorf("got %v", v)
	}
}

// ---- Regression tests: unsafe-fast-path parity with safe path ----
//
// All tests below lock in safe/unsafe behavioral parity for primitive
// numeric and float types decoded through the struct-field unsafe fast
// path. Without parity, the unsafe path silently truncates/wraps
// values that the safe path explicitly rejects.

// Finding 6: deserFixedUUIDReflect aliased input on []byte target.
func TestRegression_DeserFixedUUIDBytesAliasesInput(t *testing.T) {
	sch := MustParse(`{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`)
	src := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	encoded := mustAppendEncode(t, sch, nil, &src)
	var got []byte
	mustDecode(t, sch, encoded, &got)
	want := append([]byte(nil), got...)
	for i := range encoded {
		encoded[i] = 0xFF
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("decoded []byte aliases input buffer: got=%x want=%x", got, want)
	}
}

// Finding 7: union default may match any branch — Avro 1.12+. POSITIVE
// regression test: locks in the deliberate spec-1.12 behavior. Earlier
// 1.11 strict-first-branch readers (and goavro) reject this; Java 1.12.0+
// and fastavro v1.7+ accept. Reference: Apache Avro AVRO-3649 / PR #2503.
func TestRegression_UnionDefaultAcceptsAnyBranch_Avro112(t *testing.T) {
	if _, err := Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":["null","int"],"default":42}]}`); err != nil {
		t.Fatalf("expected Avro 1.12+ to accept non-first-branch union default, got: %v", err)
	}
}

// TestRegression_EncodeJSONBareUnionsByDefault locks in the deliberate design
// choice that EncodeJSON emits bare (non-tagged) unions by default, diverging
// from the Avro 1.12 JSON-encoding spec's {"type_name": value}; spec-compliant
// tagged output is opt-in via TaggedUnions(), whose doc carries the rationale.
// This exists so the choice can't drift silently — flipping it would be a
// behavior change for existing users.
func TestRegression_EncodeJSONBareUnionsByDefault(t *testing.T) {
	schema := MustParse(`{
		"type":"record","name":"R",
		"fields":[{"name":"u","type":["null","string"]}]
	}`)
	got := mustAppendEncodeJSON(t, schema, nil, map[string]any{"u": "hi"})
	const bare = `{"u":"hi"}`
	if string(got) != bare {
		t.Fatalf("default EncodeJSON union shape changed: got %s, want %s "+
			"(spec-compliant tagged form is %s, available via TaggedUnions())",
			got, bare, `{"u":{"string":"hi"}}`)
	}
	// And TaggedUnions produces the spec-compliant form.
	tagged := mustAppendEncodeJSON(t, schema, nil, map[string]any{"u": "hi"}, TaggedUnions())
	const wantTagged = `{"u":{"string":"hi"}}`
	if string(tagged) != wantTagged {
		t.Fatalf("TaggedUnions: got %s, want %s", tagged, wantTagged)
	}
}

// TestRegression_DecodeJSONUnionTagShortName locks in that the JSON
// union decoder accepts the unqualified short-name tag form (e.g.
// {"User": {...}} for a record named com.example.User) as a leniency
// for hand-written JSON. No reference implementation emits or reads
// this form: Java emits and requires the fullname envelope, and
// fastavro 1.12.2 does too (its json_writer keys by fullname and its
// AvroJSONDecoder.read_index exact-matches branch labels — a
// short-name tag raises, observed). The uniqueness guard in
// findUnionBranch keeps the leniency unambiguous.
func TestRegression_DecodeJSONUnionTagShortName(t *testing.T) {
	sch := MustParse(`{"type":"record","name":"Wrapper","fields":[
		{"name":"u","type":["null",{"type":"record","name":"User","namespace":"com.example","fields":[
			{"name":"name","type":"string"}
		]}]}
	]}`)
	// Short-name tag (hand-written-JSON leniency).
	in := []byte(`{"u":{"User":{"name":"alice"}}}`)
	var out map[string]any
	if err := sch.DecodeJSON(in, &out); err != nil {
		t.Fatalf("short-name tag rejected: %v", err)
	}
	user, ok := out["u"].(map[string]any)
	if !ok {
		t.Fatalf("expected user map, got %T", out["u"])
	}
	if user["name"] != "alice" {
		t.Fatalf("got name=%v, want alice", user["name"])
	}
	// Java-style fullname tag still works.
	in2 := []byte(`{"u":{"com.example.User":{"name":"bob"}}}`)
	var out2 map[string]any
	if err := sch.DecodeJSON(in2, &out2); err != nil {
		t.Fatalf("fullname tag rejected: %v", err)
	}
}

// TestRegression_DecodeJSONUnionTagAmbiguousShortName locks in that
// when two named-type branches share a short name (across different
// namespaces), the short-name fallback bails rather than silently
// pick a branch. The fullname form must still work for both.
func TestRegression_DecodeJSONUnionTagAmbiguousShortName(t *testing.T) {
	sch := MustParse(`{"type":"record","name":"Wrapper","fields":[
		{"name":"u","type":[
			"null",
			{"type":"record","name":"User","namespace":"a","fields":[{"name":"x","type":"int"}]},
			{"type":"record","name":"User","namespace":"b","fields":[{"name":"y","type":"int"}]}
		]}
	]}`)
	// Ambiguous short name → must error rather than silently pick.
	in := []byte(`{"u":{"User":{"x":1}}}`)
	var out map[string]any
	if err := sch.DecodeJSON(in, &out); err == nil {
		t.Fatalf("expected error on ambiguous short-name tag, got %v", out)
	}
	// Fullname disambiguates.
	in2 := []byte(`{"u":{"a.User":{"x":1}}}`)
	if err := sch.DecodeJSON(in2, &out); err != nil {
		t.Fatalf("fullname a.User rejected: %v", err)
	}
	in3 := []byte(`{"u":{"b.User":{"y":2}}}`)
	if err := sch.DecodeJSON(in3, &out); err != nil {
		t.Fatalf("fullname b.User rejected: %v", err)
	}
}

// TestRegression_WriterUnionBranchMismatchFailsFast locks in this library's
// fail-fast posture for writer-union resolution: every writer branch must be
// compatible with the reader at Resolve time, and the first incompatibility is
// returned eagerly. This deliberately diverges from Java's Resolver.WriterUnion
// (per-branch ErrorAction deferred to decode time) and fastavro's read_union —
// see checkWriterUnion's doc for the rationale. A producer that narrowed during
// evolution but never emits the dropped branch must update its schema first.
func TestRegression_WriterUnionBranchMismatchFailsFast(t *testing.T) {
	writer := MustParse(`["null","string"]`)
	reader := MustParse(`"string"`)
	if _, err := Resolve(writer, reader); err == nil {
		t.Fatal("expected Resolve to fail eagerly when a writer branch (null) is incompatible with the reader (string)")
	}
}

// TestRegression_NullUnionNonCanonicalVarint verifies that the
// null-union fast path accepts non-canonical multi-byte varint
// encodings of the branch index. Spec ("Binary encoding > Unions")
// says the index is a generic int (varint); Java's BinaryDecoder.readIndex
// calls readInt() which is a full multi-byte loop. A fast path that
// peeks src[0] only and rejects anything other than 0x00 / 0x02
// would break interop with producers that emit non-canonical
// varints.
func TestRegression_NullUnionNonCanonicalVarint(t *testing.T) {
	s := MustParse(`{"type":"record","name":"R","fields":[{"name":"x","type":["null","int"]}]}`)
	type rec struct {
		X *int32 `avro:"x"`
	}
	var out rec
	// 0x80 0x00 = non-canonical encoding of varint 0 (the null branch).
	if _, err := s.Decode([]byte{0x80, 0x00}, &out); err != nil {
		t.Fatalf("decode non-canonical varint: %v", err)
	}
	if out.X != nil {
		t.Fatalf("expected nil x, got %v", *out.X)
	}
}

// TestRegression_LongDefaultPrecisionLoss verifies that long-typed
// schema defaults > 2^53 round-trip exact (no float64 truncation).
// json.Unmarshal-into-any would decode numeric defaults as float64,
// silently rounding 9007199254740993 → 9007199254740992;
// unmarshalDefault uses UseNumber to preserve the integer.
func TestRegression_LongDefaultPrecisionLoss(t *testing.T) {
	const want = int64(9007199254740993)
	src := `{"type":"record","name":"R","fields":[{"name":"x","type":"long","default":9007199254740993}]}`
	s := MustParse(src)
	enc := mustAppendEncode(t, s, nil, map[string]any{}) // missing field → use default
	type recOut struct {
		X int64 `avro:"x"`
	}
	var got recOut
	mustDecode(t, s, enc, &got)
	if got.X != want {
		t.Fatalf("default: got %d want %d", got.X, want)
	}
}

// TestMatrix_InvalidDecimalRejected verifies that malformed decimal
// logical types (precision <= 0, scale > precision, missing precision,
// precision exceeding fixed capacity) are rejected at parse time,
// aligning with fastavro's parse_schema hard-rejects (negative values
// and scale > precision; its truthiness guards skip 0/missing, observed
// 1.12.2). Java's Decimal.validate throws for each, but schema parse
// catches the throw (fromSchemaIgnoreInvalid) and soft-drops the
// logical — silently stripping the logical type and treating the schema
// as plain bytes/fixed is exactly the interop hazard rejecting avoids.
func TestMatrix_InvalidDecimalRejected(t *testing.T) {
	cases := []struct {
		name, schema string
	}{
		{"scale > precision", `{"type":"bytes","logicalType":"decimal","precision":2,"scale":3}`},
		{"precision = 0", `{"type":"bytes","logicalType":"decimal","precision":0}`},
		{"missing precision", `{"type":"bytes","logicalType":"decimal"}`},
		{"precision > fixed capacity", `{"type":"fixed","name":"D","size":1,"logicalType":"decimal","precision":3}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := Parse(c.schema); err == nil {
				t.Fatalf("expected parse error for %s", c.name)
			}
		})
	}
}

// TestMatrix_DecimalNaNInfNoPanic verifies that encoding non-finite
// floats (NaN, ±Inf) into decimal-typed schemas does not panic.
// tryCoerceToRat must guard against (*big.Rat).SetFloat64 returning
// nil for non-finite values; without the guard, downstream
// serRat / FloatString dereferences the nil pointer. Java/fastavro/
// goavro all reject these inputs with a typed error rather than
// crash.
func TestMatrix_DecimalNaNInfNoPanic(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		json   bool
		input  any
	}{
		{"bytes-decimal NaN binary", `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`, false, math.NaN()},
		{"bytes-decimal +Inf binary", `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`, false, math.Inf(1)},
		{"bytes-decimal -Inf binary", `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`, false, math.Inf(-1)},
		{"fixed-decimal NaN binary", `{"type":"fixed","name":"D","size":4,"logicalType":"decimal","precision":4,"scale":2}`, false, math.NaN()},
		{"bytes-decimal NaN json", `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`, true, math.NaN()},
		{"fixed-decimal NaN json", `{"type":"fixed","name":"D","size":4,"logicalType":"decimal","precision":4,"scale":2}`, true, math.NaN()},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panicked: %v", r)
				}
			}()
			s := MustParse(c.schema)
			var err error
			if c.json {
				_, err = s.AppendEncodeJSON(nil, c.input)
			} else {
				_, err = s.AppendEncode(nil, c.input)
			}
			if err == nil {
				t.Fatal("expected error for non-finite decimal input, got success")
			}
		})
	}
}

// TestRegression_CanonicalU2028Escaping verifies Canonical() emits U+2028
// and U+2029 as raw UTF-8 (per PCF [STRINGS]) rather than as the
// six-byte \uXXXX JSON escape Go's encoding/json forces. Java's
// SchemaNormalization.toParsingForm emits raw UTF-8; without this
// post-process our fingerprints would diverge from Java for any name
// containing those code points.
func TestRegression_CanonicalU2028Escaping(t *testing.T) {
	src := "{\"type\":\"record\",\"name\":\"R \",\"fields\":[]}"
	s, err := Parse(src, WithLaxNames(func(string) error { return nil }))
	if err != nil {
		t.Skipf("schema rejected: %v", err)
	}
	canon := s.Canonical()
	// Should contain the literal UTF-8 bytes for U+2028 (e2 80 a8).
	if !bytes.Contains(canon, []byte{0xe2, 0x80, 0xa8}) {
		t.Errorf("canonical missing raw U+2028 UTF-8 bytes; got %q", canon)
	}
	// And NOT the JSON escape sequence.
	if bytes.Contains(canon, []byte{'\\', 'u', '2', '0', '2', '8'}) {
		t.Errorf("canonical contains \\u2028 escape — breaks Java fingerprint parity; got %q", canon)
	}
}

// TestRegression_UnionResolutionPrefersExactKindOverPromotion verifies
// that union resolution does a two-pass scan: exact-kind branches win
// over promotion branches even when the promotion branch comes first
// in declaration order. Matches Java's Resolver.firstMatchingBranch
// (Resolver.java:634 "first scan for exact match", :666 "then scan
// match via numeric promotion") and fastavro. A single-pass scan
// would silently pick the first promotion match, producing float64
// for an int writer when the reader is ["double","int"].
func TestRegression_UnionResolutionPrefersExactKindOverPromotion(t *testing.T) {
	t.Run("writer non-union, reader union with promotion before exact", func(t *testing.T) {
		writer := MustParse(`"int"`)
		reader := MustParse(`["double","int"]`)
		encoded := mustAppendEncode(t, writer, nil, ptr(int32(42)))
		rs := mustResolve(t, writer, reader)
		var got any
		mustDecode(t, rs, encoded, &got)
		if v, ok := got.(int32); !ok || v != 42 {
			t.Fatalf("reader-union: expected int32(42), got %T(%v)", got, got)
		}
	})
	t.Run("writer and reader both unions", func(t *testing.T) {
		writer := MustParse(`["int","long"]`)
		reader := MustParse(`["double","int"]`)
		encoded := mustAppendEncode(t, writer, nil, ptr(int32(5)))
		rs := mustResolve(t, writer, reader)
		var got any
		mustDecode(t, rs, encoded, &got)
		if v, ok := got.(int32); !ok || v != 5 {
			t.Fatalf("union-union: expected int32(5), got %T(%v)", got, got)
		}
	})
}

// TestRegression_DecodeJSON_FixedLengthMismatch locks in JSON-decoder length
// validation for fixed types. Per Avro 1.12 JSON spec a fixed value is a JSON
// string whose code points are the bytes of the value, so its length must equal
// the schema's size; Java's JsonDecoder.readFixed throws when they differ and
// fastavro raises ValueError. Without the check the decoder would silently
// truncate, zero-pad, or return the wrong length depending on the target type.
func TestRegression_DecodeJSON_FixedLengthMismatch(t *testing.T) {
	schema := `{"type":"fixed","name":"F","size":4}`
	s := MustParse(schema)

	// 3-char JSON for a fixed(4): should error.
	var got [4]byte
	if err := s.DecodeJSON([]byte(`"abc"`), &got); err == nil {
		t.Errorf("[4]byte too few: expected error, got %x", got)
	}
	// 5-char JSON for a fixed(4): should error.
	var gotMore [4]byte
	if err := s.DecodeJSON([]byte(`"abcde"`), &gotMore); err == nil {
		t.Errorf("[4]byte too many: expected error, got %x", gotMore)
	}
	// []byte target also unvalidated.
	var gotS []byte
	if err := s.DecodeJSON([]byte(`"abc"`), &gotS); err == nil {
		t.Errorf("[]byte: expected error, got len=%d", len(gotS))
	}
	// *any target also unvalidated.
	var gotAny any
	if err := s.DecodeJSON([]byte(`"abc"`), &gotAny); err == nil {
		t.Errorf("*any: expected error, got %v", gotAny)
	}
	// Same for fixed-decimal (the JSON-string path).
	sd := MustParse(`{"type":"fixed","name":"D","size":4,"logicalType":"decimal","precision":9,"scale":2}`)
	var rat big.Rat
	if err := sd.DecodeJSON([]byte(`"abc"`), &rat); err == nil {
		t.Errorf("fixed-decimal: expected error, got %s", rat.RatString())
	}
}

// TestRegression_DecodeArrayOfNullLargeCount locks in that decoding an
// Avro array<null> with a count larger than the remaining buffer
// succeeds, up to the absolute cap. Each null element takes zero bytes
// on the wire, so the usual `count > len(src)` DoS guard is wrong for
// null items.
func TestRegression_DecodeArrayOfNullLargeCount(t *testing.T) {
	sch := MustParse(`{"type":"array","items":"null"}`)
	// Wire: block count = 100 (varlong = 0xC8 0x01), then 0 element
	// bytes (null is empty), then terminator count = 0 (0x00).
	wire := []byte{0xC8, 0x01, 0x00}
	var got []any
	if _, err := sch.Decode(wire, &got); err != nil {
		t.Fatalf("decode array of 100 nulls: %v", err)
	}
	if len(got) != 100 {
		t.Fatalf("got %d elements, want 100", len(got))
	}
	for i, e := range got {
		if e != nil {
			t.Fatalf("element %d is non-nil: %v", i, e)
		}
	}
}

// TestRegression_DecodeArrayOfNullCappedAtLimit verifies the
// maxZeroByteItems absolute cap rejects DoS-sized counts.
func TestRegression_DecodeArrayOfNullCappedAtLimit(t *testing.T) {
	sch := MustParse(`{"type":"array","items":"null"}`)
	// 1<<30 nulls: 5 wire bytes total (varlong + terminator), would
	// allocate ~16 GiB without the cap.
	var data []byte
	data = binary.AppendVarint(data, 1<<30)
	data = binary.AppendVarint(data, 0)
	var got []any
	if _, err := sch.Decode(data, &got); err == nil {
		t.Fatalf("expected zero-byte-array cap error, got success with len=%d", len(got))
	}
}

// TestRegression_DecodeArrayOfNullCumulativeAcrossBlocks verifies the
// cap is cumulative — chunking the count across multiple sub-cap blocks
// must still fail.
func TestRegression_DecodeArrayOfNullCumulativeAcrossBlocks(t *testing.T) {
	sch := MustParse(`{"type":"array","items":"null"}`)
	// Two blocks of 3000 each (sum 6000 > cap 4096).
	var data []byte
	data = binary.AppendVarint(data, 3000)
	data = binary.AppendVarint(data, 3000)
	data = binary.AppendVarint(data, 0)
	var got []any
	if _, err := sch.Decode(data, &got); err == nil {
		t.Fatalf("expected cumulative cap error, got success with len=%d", len(got))
	}
}

// TestRegression_DecodeArrayOfEmptyRecord verifies that arrays whose
// items take 0 wire bytes (empty record) decode correctly up to the cap.
// The zero-byte-item detector must include records-that-encode-to-
// zero-bytes, not just primitive null.
func TestRegression_DecodeArrayOfEmptyRecord(t *testing.T) {
	sch := MustParse(`{"type":"array","items":{"type":"record","name":"E","fields":[]}}`)
	var data []byte
	data = binary.AppendVarint(data, 100)
	data = binary.AppendVarint(data, 0)
	var got []map[string]any
	if _, err := sch.Decode(data, &got); err != nil {
		t.Fatalf("decode 100 empty records: %v", err)
	}
	if len(got) != 100 {
		t.Fatalf("got %d, want 100", len(got))
	}
}

// TestRegression_DecodeArrayOfRecordWithAllNullFields verifies that a
// record whose fields are all null encodes to 0 bytes per record and
// the array of such still decodes (up to the cap).
func TestRegression_DecodeArrayOfRecordWithAllNullFields(t *testing.T) {
	sch := MustParse(`{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"x","type":"null"}]}}`)
	var data []byte
	data = binary.AppendVarint(data, 50)
	data = binary.AppendVarint(data, 0)
	var got []map[string]any
	mustDecode(t, sch, data, &got)
	if len(got) != 50 {
		t.Fatalf("got %d, want 50", len(got))
	}
}

// TestRegression_TimeMicrosAnyVsDurationParity locks in that *any and
// *time.Duration paths agree on overflow handling. Both arms must
// error on val > MaxInt64/Microsecond; without parity, the *any arm
// silently wraps via time.Duration(val) * time.Microsecond,
// delivering a wrong typed value to user code while the
// *time.Duration arm errors.
func TestRegression_TimeMicrosAnyVsDurationParity(t *testing.T) {
	sch := MustParse(`{"type":"long","logicalType":"time-micros"}`)
	encSch := MustParse(`"long"`)
	v := int64(math.MaxInt64)
	encoded, err := encSch.AppendEncode(nil, &v)
	if err != nil {
		t.Fatalf("encode helper: %v", err)
	}
	// *time.Duration target: errors (correct).
	var d time.Duration
	if _, err := sch.Decode(encoded, &d); err == nil {
		t.Fatalf("*time.Duration: expected overflow error, got d=%v", d)
	}
	// *any target: same schema, same wire bytes — should also error.
	var a any
	if _, err := sch.Decode(encoded, &a); err == nil {
		t.Fatalf("*any: expected same overflow error as *time.Duration, got a=%v (paths diverged)", a)
	}
}

// TestRegression_JSONBytesUnicodeCharCodepointSemantics locks in spec /
// Java parity: per the Avro 1.12 JSON-encoding section, "each character
// represents one byte" and "Unicode code points 0-255 are mapped to
// unsigned 8-bit byte values 0-255". Java decodes JSON strings into
// Java Strings (UTF-8 → UTF-16) then maps each char to a byte via
// ISO-8859-1; fastavro does the same via str.encode("iso-8859-1").
// twmb must apply codepoint mapping; walking raw input bytes one-by-
// one would decode JSON literal "é" (UTF-8 c3 a9) to [0xC3, 0xA9]
// instead of the spec-correct [0xE9].
func TestRegression_JSONBytesUnicodeCharCodepointSemantics(t *testing.T) {
	sch := MustParse(`"bytes"`)
	// Path A: \u-escape form.
	var fromEscape []byte
	if err := sch.DecodeJSON([]byte(`"é"`), &fromEscape); err != nil {
		t.Fatalf("escape form: %v", err)
	}
	// Path B: literal Unicode character (UTF-8 bytes c3 a9 in source).
	var fromLiteral []byte
	if err := sch.DecodeJSON([]byte("\"é\""), &fromLiteral); err != nil {
		t.Fatalf("literal form: %v", err)
	}
	if !bytes.Equal(fromEscape, fromLiteral) {
		t.Fatalf("JSON bytes parity: escape form=%x literal form=%x — Avro spec / Java treat one Unicode character as one byte regardless of UTF-8 source encoding",
			fromEscape, fromLiteral)
	}
	if len(fromLiteral) != 1 || fromLiteral[0] != 0xE9 {
		t.Fatalf("expected [0xE9], got %x", fromLiteral)
	}
}

// TestRegression_LocalTimestampMillisNonUTCWallClock locks in spec/Java
// parity for local-timestamp encoding of non-UTC time.Time inputs.
// Per Avro 1.12 + Java reference (TimeConversions.LocalTimestampMillisConversion
// uses LocalDateTime.toInstant(ZoneOffset.UTC)) and fastavro's
// data.replace(tzinfo=datetime.timezone.utc), local-timestamp encodes
// the wall-clock fields as-if-UTC. Using t.UnixMilli() would encode
// the absolute UTC moment instead, breaking interop for non-UTC
// inputs.
func TestRegression_LocalTimestampMillisNonUTCWallClock(t *testing.T) {
	schema := MustParse(`{"type":"long","logicalType":"local-timestamp-millis"}`)
	nyc := time.FixedZone("NYC", -5*3600)
	in := time.Date(2024, 1, 1, 12, 0, 0, 0, nyc)
	encoded := mustAppendEncode(t, schema, nil, in)
	rawSchema := MustParse(`"long"`)
	var v int64
	mustDecode(t, rawSchema, encoded, &v)
	const want = int64(1704110400000) // 2024-01-01 12:00 UTC ms (wall-clock-as-UTC)
	if v != want {
		t.Errorf("local-timestamp-millis wire value: got %d, want %d "+
			"(Java/fastavro encode wall-clock as-if-UTC; library used to encode the absolute moment)", v, want)
	}
}

// TestMatrix_TimestampDeserParity locks in that
// deserTimestamp{Millis,Micros,Nanos} and deserDate handle the same
// five target shapes (any, time.Time, typed-interface-implemented-by-
// time.Time, typed-interface-not-implemented, plain integer) in lockstep.
// All four sites route through deserTimeAsLong + setIface so the
// interface guard is shared and can't drift across changes.
func TestMatrix_TimestampDeserParity(t *testing.T) {
	type stringerIface interface{ String() string } // time.Time implements
	type unsupported interface{ Mock() }            // time.Time does not

	for _, tc := range []struct {
		name, schema string
		in           time.Time
	}{
		{"millis", `{"type":"long","logicalType":"timestamp-millis"}`,
			time.Date(2026, 5, 8, 12, 30, 45, 0, time.UTC)},
		{"micros", `{"type":"long","logicalType":"timestamp-micros"}`,
			time.Date(2026, 5, 8, 12, 30, 45, 123_000, time.UTC)},
		{"nanos", `{"type":"long","logicalType":"timestamp-nanos"}`,
			time.Date(2026, 5, 8, 12, 30, 45, 123_456, time.UTC)},
		{"date", `{"type":"int","logicalType":"date"}`,
			time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := MustParse(tc.schema)
			enc := mustAppendEncode(t, s, nil, tc.in)

			// 1. any-target → empty-interface arm of setIface.
			var anyT any
			if _, err := s.Decode(enc, &anyT); err != nil {
				t.Fatalf("any: %v", err)
			}
			if _, ok := anyT.(time.Time); !ok {
				t.Errorf("expected time.Time, got %T", anyT)
			}

			// 2. time.Time target → timeType arm.
			var typed time.Time
			if _, err := s.Decode(enc, &typed); err != nil {
				t.Fatalf("time.Time: %v", err)
			}
			if !typed.Equal(tc.in) {
				t.Errorf("got %v want %v", typed, tc.in)
			}

			// 3. typed-interface implemented by time.Time → setIface
			//    AssignableTo(true) accepts.
			var st stringerIface
			if _, err := s.Decode(enc, &st); err != nil {
				t.Errorf("Stringer: time.Time should be assignable: %v", err)
			}

			// 4. typed-interface NOT implemented → setIface rejects.
			var bad unsupported
			if _, err := s.Decode(enc, &bad); err == nil {
				t.Errorf("unsupported iface: expected error")
			}

			// 5. Plain integer fallback (setLongValue/setIntValue).
			if tc.name == "date" {
				var n int32
				if _, err := s.Decode(enc, &n); err != nil {
					t.Errorf("int32 fallback: %v", err)
				}
			} else {
				var n int64
				if _, err := s.Decode(enc, &n); err != nil {
					t.Errorf("int64 fallback: %v", err)
				}
			}
		})
	}
}

// TestRegression_DecimalBytesUnionJSONRoundTrip locks in that
// EncodeJSON / DecodeJSON round-trip a decimal-bytes value when it is
// a non-null branch of a union. Two failure modes: EncodeJSON
// emitting 0.33 (non-spec) and the union dispatch table refusing to
// route digit tokens to bytes/fixed branches; either alone breaks
// EncodeJSON ↔ DecodeJSON round-trip.
func TestRegression_DecimalBytesUnionJSONRoundTrip(t *testing.T) {
	s := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"v","type":["null",{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}]}
	]}`)
	r := new(big.Rat).SetFrac64(33, 100)
	enc, err := s.AppendEncodeJSON(nil, map[string]any{"v": r})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out any
	if err := s.DecodeJSON(enc, &out); err != nil {
		t.Fatalf("DecodeJSON failed to round-trip its own EncodeJSON output: %v\nencoded=%s", err, enc)
	}
}

// TestRegression_TimeMicrosJSONOverflowParity locks in that the JSON
// decoder rejects out-of-range time-micros values when decoding to
// *time.Duration, matching the binary path's overflow guard. The
// guard lives inside timeMicrosToDuration so all callers (binary
// safe, binary unsafe, JSON any, JSON typed) reject uniformly;
// without it, the JSON path silently wraps via val * time.Microsecond
// while the binary path errors.
func TestRegression_TimeMicrosJSONOverflowParity(t *testing.T) {
	schema := MustParse(`{"type":"long","logicalType":"time-micros"}`)
	bigVal := int64(math.MaxInt64/1000) + 1
	jsonInput := []byte(fmt.Sprintf("%d", bigVal))

	var d time.Duration
	if err := schema.DecodeJSON(jsonInput, &d); err == nil {
		t.Fatalf("DecodeJSON of out-of-range time-micros %d into *time.Duration succeeded with d=%d (silent wrap from val*1000); binary path errors on the same value",
			bigVal, int64(d))
	}
}

// TestRegression_TimeMicrosJSONOverflowAnyPath is the *any companion:
// the same overflow must also fail when DecodeJSON targets a *any
// (which routes through decodeLogicalLong). Without the shared
// guard, the *any path silently produces time.Duration(-2562047h47m16s).
func TestRegression_TimeMicrosJSONOverflowAnyPath(t *testing.T) {
	schema := MustParse(`{"type":"long","logicalType":"time-micros"}`)
	bigVal := int64(math.MaxInt64/1000) + 1
	jsonInput := []byte(fmt.Sprintf("%d", bigVal))

	var v any
	if err := schema.DecodeJSON(jsonInput, &v); err == nil {
		t.Fatalf("DecodeJSON to *any of out-of-range time-micros %d returned %T(%v) instead of erroring (binary path errors)",
			bigVal, v, v)
	}
}

type tmDurField struct {
	F time.Duration `avro:"f"`
}

type tmTimeField struct {
	F time.Time `avro:"f"`
}

type tmIntField struct {
	F int64 `avro:"f"`
}

// TestMatrix_TimeMicrosOverflowGuardIsUniform crosses the overflow guard's three
// axes at once. The guard lives in one conversion helper so every caller rejects
// the same values, and "every caller" is the claim — but the suite reached it
// one caller and one target at a time, so no cell asked whether the four callers
// AGREE, and the time.Time target's overflow arm was reached by nothing.
//
//	caller  binary safe, binary unsafe (a struct field), JSON typed, JSON any
//	target  time.Duration, time.Time, any, and int64 as the control
//	value   in range, both boundaries, and one past each boundary
//
// The rule is stated independently of the code: a value overflows exactly when
// it cannot be scaled to nanoseconds inside an int64, and the guard must fire
// for every target that MATERIALIZES a duration and none that does not. int64 is
// in the matrix precisely because it must keep accepting the overflowing values,
// and the boundary cells are the two immediately inside the limit, so an
// off-by-one shows up as a rejected legal value.
func TestMatrix_TimeMicrosOverflowGuardIsUniform(t *testing.T) {
	const microsPerNano = int64(time.Microsecond) // 1000
	values := []struct {
		name     string
		val      int64
		overflow bool
	}{
		{"zero", 0, false},
		{"typical", 12_345_678, false},
		{"boundary-hi", math.MaxInt64 / microsPerNano, false},
		{"boundary-lo", math.MinInt64 / microsPerNano, false},
		{"overflow-hi", math.MaxInt64/microsPerNano + 1, true},
		{"overflow-lo", math.MinInt64/microsPerNano - 1, true},
	}
	targets := []struct {
		name string
		// materializes reports whether this target converts the value into
		// a duration, which is what the guard protects. A target that
		// keeps the raw number is not protected and must accept.
		materializes bool
		scalar       func() any // fresh pointer for the bare-target callers
		record       func() any // fresh pointer for the struct-field caller
		field        func(any) any
	}{
		{"duration", true, func() any { return new(time.Duration) }, func() any { return new(tmDurField) }, nil},
		{"time", true, func() any { return new(time.Time) }, func() any { return new(tmTimeField) }, nil},
		{"any", true, func() any { return new(any) }, nil, nil},
		{"int64", false, func() any { return new(int64) }, func() any { return new(tmIntField) }, nil},
	}

	scalarS := MustParse(`{"type":"long","logicalType":"time-micros"}`)
	recS := MustParse(`{"type":"record","name":"TMR","fields":[{"name":"f","type":{"type":"long","logicalType":"time-micros"}}]}`)
	// The wire is written through the PLAIN long schema, so the payload is
	// whatever int64 the cell names — the encoder's own range checks cannot
	// pre-filter the values this test is about.
	plainScalarS := MustParse(`"long"`)
	plainRecS := MustParse(`{"type":"record","name":"TMR","fields":[{"name":"f","type":"long"}]}`)

	// Liveness floor, counted inside the cell after the verdict is checked.
	rejected, accepted := 0, 0
	callersRun := map[string]int{}

	for _, v := range values {
		binScalar, err := plainScalarS.Encode(v.val)
		if err != nil {
			t.Fatalf("%s: encode scalar: %v", v.name, err)
		}
		binRec, err := plainRecS.Encode(map[string]any{"f": v.val})
		if err != nil {
			t.Fatalf("%s: encode record: %v", v.name, err)
		}
		jsonScalar := []byte(strconv.FormatInt(v.val, 10))

		for _, tg := range targets {
			wantErr := v.overflow && tg.materializes

			callers := []struct {
				name string
				run  func() error
			}{
				{"binary-safe", func() error {
					_, err := scalarS.Decode(binScalar, tg.scalar())
					return err
				}},
				{"json-typed", func() error { return scalarS.DecodeJSON(jsonScalar, tg.scalar()) }},
			}
			if tg.record != nil {
				callers = append(callers, struct {
					name string
					run  func() error
				}{"binary-unsafe", func() error {
					_, err := recS.Decode(binRec, tg.record())
					return err
				}})
			}

			for _, c := range callers {
				t.Run(v.name+"/"+tg.name+"/"+c.name, func(t *testing.T) {
					err := c.run()
					if wantErr && err == nil {
						t.Fatalf("%d (%s) into a %s target via %s was accepted; %d microseconds cannot be scaled to nanoseconds inside an int64, and every other caller rejects it",
							v.val, v.name, tg.name, c.name, v.val)
					}
					if !wantErr && err != nil {
						t.Fatalf("%d (%s) into a %s target via %s was rejected: %v", v.val, v.name, tg.name, c.name, err)
					}
					if wantErr {
						rejected++
					} else {
						accepted++
					}
					callersRun[c.name]++
				})
			}
		}
	}

	// Both verdicts must occur, or the matrix proves only that the guard
	// is consistent about one answer.
	if rejected == 0 || accepted == 0 {
		t.Fatalf("the verdict axis collapsed: %d rejected, %d accepted", rejected, accepted)
	}
	// Every caller must have run. The claim under test is that they agree,
	// which a matrix missing one of them cannot make.
	for _, name := range []string{"binary-safe", "binary-unsafe", "json-typed"} {
		if callersRun[name] == 0 {
			t.Errorf("caller %q never ran; the guard's uniformity across callers is unasserted", name)
		}
	}
	t.Logf("time-micros overflow guard: %d rejected, %d accepted across %d callers", rejected, accepted, len(callersRun))
}

// TestDurationSubResolutionTruncatesTowardZero locks that time.Duration values
// whose nanosecond component is not a whole multiple of the schema's resolution
// unit are silently truncated toward zero at encode, matching
// time.Duration.Milliseconds() and .Microseconds(). The wire cannot represent
// sub-resolution precision, so encode must truncate, round, or reject; this
// implementation truncates, as README §Logical Types and the serTimeMillis
// doc-string record. A whole-millisecond Duration round-trips exactly.
func TestDurationSubResolutionTruncatesTowardZero(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		in     time.Duration
		want   time.Duration
	}{
		{
			name:   "time-millis exact ms",
			schema: `{"type":"int","logicalType":"time-millis"}`,
			in:     2 * time.Millisecond,
			want:   2 * time.Millisecond,
		},
		{
			name:   "time-millis sub-ms truncates",
			schema: `{"type":"int","logicalType":"time-millis"}`,
			in:     time.Duration(1_500_500), // 1.5005ms
			want:   1 * time.Millisecond,
		},
		{
			name:   "time-micros exact us",
			schema: `{"type":"long","logicalType":"time-micros"}`,
			in:     3 * time.Microsecond,
			want:   3 * time.Microsecond,
		},
		{
			name:   "time-micros sub-us truncates",
			schema: `{"type":"long","logicalType":"time-micros"}`,
			in:     time.Duration(1_500_999), // 1.500999ms → 1500us
			want:   1500 * time.Microsecond,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := MustParse(tc.schema)
			buf := mustAppendEncode(t, s, nil, tc.in)
			var out time.Duration
			mustDecode(t, s, buf, &out)
			if out != tc.want {
				t.Fatalf("in=%v: got out=%v, want %v", tc.in, out, tc.want)
			}
		})
	}
}

// TestRegression_UnionWithoutNullBranchAcceptsJsonNull locks in that
// DecodeJSON rejects a null token when the union schema has no null
// branch. Java's JsonDecoder.readIndex and fastavro's read_index both
// reject (synthesize "null" label, look it up in the branch list,
// fail if absent). decodeUnion must check the branch list before
// short-circuiting on the 'n' peek byte; otherwise it silently
// consumes null and either zeroes the target or leaves it untouched.
func TestRegression_UnionWithoutNullBranchAcceptsJsonNull(t *testing.T) {
	schema := MustParse(`["int","string"]`)
	var v any
	if err := schema.DecodeJSON([]byte(`null`), &v); err == nil {
		t.Fatalf("DecodeJSON of null into union without null branch returned no error; got v=%v (%T)", v, v)
	}
}

type rTagLong struct {
	N int64 `avro:"n,default=9223372036854775807"`
}

// TestRegression_SchemaForLongDefaultPrecisionLoss locks in that
// SchemaFor preserves long defaults > 2^53 when reading the default=
// struct tag. The default-value pipeline through SchemaFor uses
// arbitrary-precision parsing so values like MaxInt64 round-trip
// exactly. Mirrors unmarshalDefault on the Parse path; the two sites
// must stay in lockstep on number-precision handling.
func TestRegression_SchemaForLongDefaultPrecisionLoss(t *testing.T) {
	reader, err := SchemaFor[rTagLong]()
	if err != nil {
		t.Fatalf("SchemaFor: %v", err)
	}
	// Writer schema matches SchemaFor's record name (rTagLong) with no
	// fields, so Resolve uses the reader's default for N.
	writer := MustParse(`{"type":"record","name":"rTagLong","fields":[]}`)
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatalf("Resolve: %v (default value lost precision in SchemaFor)", err)
	}
	enc, err := writer.AppendEncode(nil, struct{}{})
	if err != nil {
		t.Fatalf("encode empty: %v", err)
	}
	var got rTagLong
	if _, err := resolved.Decode(enc, &got); err != nil {
		t.Fatalf("decode with default: %v", err)
	}
	if got.N != 9223372036854775807 {
		t.Fatalf("default N=%d, want %d", got.N, int64(9223372036854775807))
	}
}

// TestRegression_UUIDStringJSONEncodeFromArrayParity locks in that
// JSON encode of a string-backed UUID accepts a [16]byte input and
// canonicalizes it to the RFC 4122 hex-dash form, matching the
// binary serUUID. avroStringValue runs a UUID pre-pass on [16]byte
// inputs before the generic string-target dispatch.
func TestRegression_UUIDStringJSONEncodeFromArrayParity(t *testing.T) {
	schema := MustParse(`{"type":"string","logicalType":"uuid"}`)
	u := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}

	if _, err := schema.AppendEncode(nil, u); err != nil {
		t.Fatalf("binary encode reference: %v", err)
	}
	jb, err := schema.AppendEncodeJSON(nil, u)
	if err != nil {
		t.Fatalf("JSON encode of [16]byte UUID failed: %v", err)
	}
	want := `"550e8400-e29b-41d4-a716-446655440000"`
	if string(jb) != want {
		t.Fatalf("JSON encode = %s, want %s", jb, want)
	}
}

// TestRegression_UUIDStringJSONDecodeIntoArrayParity locks in that
// JSON decode of a string-backed UUID into a [16]byte target parses
// the hex-dash string into raw bytes, matching deserUUID.
func TestRegression_UUIDStringJSONDecodeIntoArrayParity(t *testing.T) {
	schema := MustParse(`{"type":"string","logicalType":"uuid"}`)
	want := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	var got [16]byte
	if err := schema.DecodeJSON([]byte(`"550e8400-e29b-41d4-a716-446655440000"`), &got); err != nil {
		t.Fatalf("JSON decode of UUID into [16]byte failed: %v", err)
	}
	if got != want {
		t.Fatalf("JSON decoded %x, want %x", got, want)
	}
}

// TestRegression_UUIDFixedJSONEncodeFromString locks in that JSON
// encode of a fixed(16) UUID accepts a hex-dash string input and
// parses it to 16 bytes, matching serFixedUUIDReflect. The
// JSON "fixed" case routes through the UUID pre-pass when the
// schema carries `logicalType:"uuid"`, so the 36-char hex-dash
// string is parsed before the generic fixed-size check applies.
func TestRegression_UUIDFixedJSONEncodeFromString(t *testing.T) {
	schema := MustParse(`{"type":"fixed","name":"u","size":16,"logicalType":"uuid"}`)
	const s = "550e8400-e29b-41d4-a716-446655440000"
	if _, err := schema.AppendEncode(nil, s); err != nil {
		t.Fatalf("binary encode reference: %v", err)
	}
	jb, err := schema.AppendEncodeJSON(nil, s)
	if err != nil {
		t.Fatalf("JSON encode of hex-dash string UUID into fixed(16) failed: %v", err)
	}
	if len(jb) < 2 || jb[0] != '"' || jb[len(jb)-1] != '"' {
		t.Fatalf("JSON encoded UUID is not a JSON string: %s", jb)
	}
	var rt [16]byte
	if err := schema.DecodeJSON(jb, &rt); err != nil {
		t.Fatalf("decode round-trip: %v", err)
	}
	want := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	if rt != want {
		t.Fatalf("round-trip got %x, want %x", rt, want)
	}
}

// TestRegression_UUIDFixedJSONDecodeIntoString locks in that JSON
// decode of a fixed(16) UUID into a string target produces the RFC
// 4122 hex-dash form (canonical UUID text), matching
// deserFixedUUIDReflect's binary-path output.
func TestRegression_UUIDFixedJSONDecodeIntoString(t *testing.T) {
	schema := MustParse(`{"type":"fixed","name":"u","size":16,"logicalType":"uuid"}`)
	u := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	jb, err := schema.AppendEncodeJSON(nil, u)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got string
	if err := schema.DecodeJSON(jb, &got); err != nil {
		t.Fatalf("JSON decode of fixed(16) UUID into string failed: %v", err)
	}
	const want = "550e8400-e29b-41d4-a716-446655440000"
	if got != want {
		t.Fatalf("got %q, want %q (JSON returned raw bytes; binary path returns hex-dash)", got, want)
	}
}

// TestRegression_UUIDFixedJSONDecodeIntoAny locks in that JSON
// decode of a fixed(16) UUID into *any returns [16]byte (not []byte),
// matching deserFixedUUIDReflect's any path.
func TestRegression_UUIDFixedJSONDecodeIntoAny(t *testing.T) {
	schema := MustParse(`{"type":"fixed","name":"u","size":16,"logicalType":"uuid"}`)
	u := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	jb, err := schema.AppendEncodeJSON(nil, u)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got any
	if err := schema.DecodeJSON(jb, &got); err != nil {
		t.Fatalf("JSON decode of fixed(16) UUID into *any failed: %v", err)
	}
	gotArr, ok := got.([16]byte)
	if !ok {
		t.Fatalf("JSON decoded %T(%v), want [16]byte (binary path returns [16]byte)", got, got)
	}
	if gotArr != u {
		t.Fatalf("got %x, want %x", gotArr, u)
	}
}

// TestRegression_ArrayLongTimeMicrosBypassesLogicalSer locks in that
// the specialized array<long> ser path does NOT silently skip the
// time-micros logical conversion when the user provides
// []time.Duration. The specialization selector must inspect the
// inner's logical type, not just af.canon.primitive — otherwise the
// encoder appendVarlong's raw nanoseconds (1500*time.Microsecond =
// 1500000 ns) instead of the spec-required microseconds (1500).
func TestRegression_ArrayLongTimeMicrosBypassesLogicalSer(t *testing.T) {
	s := MustParse(`{"type":"array","items":{"type":"long","logicalType":"time-micros"}}`)
	in := []time.Duration{1500 * time.Microsecond}
	bin := mustAppendEncode(t, s, nil, in)
	itemS := MustParse(`{"type":"long","logicalType":"time-micros"}`)
	itemBin, _ := itemS.AppendEncode(nil, time.Duration(1500)*time.Microsecond)
	want := append([]byte{0x02}, itemBin...)
	want = append(want, 0x00)
	if string(bin) != string(want) {
		t.Fatalf("array encode wire mismatch: got %x want %x", bin, want)
	}
}

// TestRegression_DeserArrayLongTimeMicrosBypassesLogicalDeser is the
// decode-side parity test: the specialized deserArrayLongLoop fast
// path was selected on sliceType.Elem().Kind() == reflect.Int64
// (time.Duration's underlying kind matches), which bypassed
// deserTimeMicros's val * time.Microsecond conversion. 1500us on the
// wire silently became 1500ns in the slice.
func TestRegression_DeserArrayLongTimeMicrosBypassesLogicalDeser(t *testing.T) {
	itemS := MustParse(`{"type":"long","logicalType":"time-micros"}`)
	itemBin, _ := itemS.AppendEncode(nil, time.Duration(1500)*time.Microsecond)
	arr := append([]byte{0x02}, itemBin...)
	arr = append(arr, 0x00)
	s := MustParse(`{"type":"array","items":{"type":"long","logicalType":"time-micros"}}`)
	var got []time.Duration
	mustDecode(t, s, arr, &got)
	want := time.Duration(1500) * time.Microsecond
	if len(got) != 1 || got[0] != want {
		t.Fatalf("array decode mismatch: got %v, want [%v]", got, want)
	}
}

// TestRegression_ArrayIntTimeMillisBypassesLogicalSer is the int /
// time-millis variant of the bypass: encoded raw nanoseconds (which
// for a 1500ms input is 1500000000, fitting int32 by accident; for
// values >= 2s would have errored on int32 overflow).
func TestRegression_ArrayIntTimeMillisBypassesLogicalSer(t *testing.T) {
	s := MustParse(`{"type":"array","items":{"type":"int","logicalType":"time-millis"}}`)
	in := []time.Duration{1500 * time.Millisecond}
	bin := mustAppendEncode(t, s, nil, in)
	itemS := MustParse(`{"type":"int","logicalType":"time-millis"}`)
	itemBin, _ := itemS.AppendEncode(nil, time.Duration(1500)*time.Millisecond)
	want := append([]byte{0x02}, itemBin...)
	want = append(want, 0x00)
	if string(bin) != string(want) {
		t.Fatalf("array encode wire mismatch: got %x want %x", bin, want)
	}
}

// TestRegression_ArrayLongTimestampMillisAcceptsTime locks in that
// array<long, timestamp-millis> accepts []time.Time, matching the
// scalar serTimestampMillis. The specialized serArray.serLong must
// inspect the inner's logical type so the time-millis arm fires for
// time.Time inputs rather than the bare-long rejection.
func TestRegression_ArrayLongTimestampMillisAcceptsTime(t *testing.T) {
	s := MustParse(`{"type":"array","items":{"type":"long","logicalType":"timestamp-millis"}}`)
	in := []time.Time{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}
	mustAppendEncode(t, s, nil, in)
}

// TestRegression_ArrayStringUUIDAccepts16Byte locks in that
// array<string, uuid> accepts [][16]byte, matching the scalar serUUID
// (which canonicalizes [16]byte to hex-dash). The specialized
// serArray.serString consults the inner's logical type so the UUID
// pre-pass applies before the generic string-target dispatch rejects
// [16]byte.
func TestRegression_ArrayStringUUIDAccepts16Byte(t *testing.T) {
	s := MustParse(`{"type":"array","items":{"type":"string","logicalType":"uuid"}}`)
	u := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	mustAppendEncode(t, s, nil, [][16]byte{u})
}

// TestRegression_RecordArrayTimeMillisAddrVsByValParity locks in that
// the same record + same data produces the same wire bytes regardless
// of input addressability — the unsafe fast path (addressable) and
// the safe specialized serArray.serInt (by-value) must agree on the
// time-millis conversion, not silently encode raw nanoseconds.
func TestRegression_RecordArrayTimeMillisAddrVsByValParity(t *testing.T) {
	type R struct {
		Vs []time.Duration `avro:"vs"`
	}
	s := MustParse(`{"type":"record","name":"R","fields":[{"name":"vs","type":{"type":"array","items":{"type":"int","logicalType":"time-millis"}}}]}`)
	in := R{Vs: []time.Duration{1500 * time.Millisecond}}
	gotAddr, errAddr := s.AppendEncode(nil, &in)
	gotVal, errVal := s.AppendEncode(nil, in)
	if errAddr != nil || errVal != nil {
		t.Fatalf("encode err: addr=%v byval=%v", errAddr, errVal)
	}
	if string(gotAddr) != string(gotVal) {
		t.Fatalf("path divergence: addr=%x byval=%x", gotAddr, gotVal)
	}
}

// TestRegression_RecordArrayIntPtrAddrVsByValParity locks in that
// addressable and by-value record encoding produce the same result for
// a []*int32 array field. The safe specialized serArray.serInt must
// unwrap both reflect.Interface AND reflect.Pointer (matching the
// unsafe fast path's pp := *(*unsafe.Pointer)(p) indirection).
func TestRegression_RecordArrayIntPtrAddrVsByValParity(t *testing.T) {
	type R struct {
		Vs []*int32 `avro:"vs"`
	}
	s := MustParse(`{"type":"record","name":"R","fields":[{"name":"vs","type":{"type":"array","items":"int"}}]}`)
	v := int32(1)
	in := R{Vs: []*int32{&v, &v, &v}}
	gotAddr, errAddr := s.AppendEncode(nil, &in)
	gotVal, errVal := s.AppendEncode(nil, in)
	if (errAddr == nil) != (errVal == nil) {
		t.Fatalf("path divergence: addr err=%v, byval err=%v", errAddr, errVal)
	}
	if errAddr == nil && string(gotAddr) != string(gotVal) {
		t.Fatalf("output mismatch: addr=%x byval=%x", gotAddr, gotVal)
	}
}

// TestRegression_LocalTimestampMillisJSONEncodeNonUTC locks in that
// EncodeJSON for local-timestamp-millis encodes wall-clock fields
// as-if-UTC, matching the binary path (serLocalTimestampMillis), Java
// (TimeConversions.LocalTimestampMillisConversion: toInstant(ZoneOffset.UTC))
// and fastavro (data.replace(tzinfo=datetime.timezone.utc)). Using
// the absolute-moment conversion (timeToTimestampMillis = UnixMilli)
// would produce different binary vs JSON wire values for non-UTC
// inputs.
func TestRegression_LocalTimestampMillisJSONEncodeNonUTC(t *testing.T) {
	schema := MustParse(`{"type":"long","logicalType":"local-timestamp-millis"}`)
	nyc := time.FixedZone("NYC", -5*3600)
	in := time.Date(2024, 1, 1, 12, 0, 0, 0, nyc)
	jb := mustEncodeJSON(t, schema, in)
	const want = "1704110400000" // 2024-01-01 12:00 UTC ms (wall-clock-as-UTC)
	if string(jb) != want {
		t.Fatalf("json wire: got %s, want %s (binary path returns the same)", jb, want)
	}
}

// TestRegression_LocalTimestampMicrosJSONEncodeNonUTC is the micros
// parity test for the same wall-clock-as-UTC rule.
func TestRegression_LocalTimestampMicrosJSONEncodeNonUTC(t *testing.T) {
	schema := MustParse(`{"type":"long","logicalType":"local-timestamp-micros"}`)
	nyc := time.FixedZone("NYC", -5*3600)
	in := time.Date(2024, 1, 1, 12, 0, 0, 0, nyc)
	jb := mustEncodeJSON(t, schema, in)
	const want = "1704110400000000"
	if string(jb) != want {
		t.Fatalf("json wire: got %s, want %s", jb, want)
	}
}

// TestRegression_LocalTimestampNanosJSONEncodeNonUTC is the nanos
// parity test.
func TestRegression_LocalTimestampNanosJSONEncodeNonUTC(t *testing.T) {
	schema := MustParse(`{"type":"long","logicalType":"local-timestamp-nanos"}`)
	nyc := time.FixedZone("NYC", -5*3600)
	in := time.Date(2024, 1, 1, 12, 0, 0, 0, nyc)
	jb := mustEncodeJSON(t, schema, in)
	const want = "1704110400000000000"
	if string(jb) != want {
		t.Fatalf("json wire: got %s, want %s", jb, want)
	}
}

// TestRegression_LocalTimestampMillisJSONEncodeFromString locks in
// that the RFC 3339 string input path (tryParseTimeString) also uses
// the wall-clock-as-UTC conversion for local-timestamp logical types,
// matching the time.Time input path.
func TestRegression_LocalTimestampMillisJSONEncodeFromString(t *testing.T) {
	schema := MustParse(`{"type":"long","logicalType":"local-timestamp-millis"}`)
	const in = "2024-01-01T12:00:00-05:00"
	jb := mustEncodeJSON(t, schema, in)
	const want = "1704110400000"
	if string(jb) != want {
		t.Fatalf("json wire: got %s, want %s", jb, want)
	}
}

// TestRegression_DefaultJSONIgnoresTaggedUnions locks in that
// missing-field record defaults respect encoder options. Missing-field
// defaults route through the full appendAvroJSON dispatch — so under
// TaggedUnions a "hello" union default emits {"string":"hello"} (the
// form Java/fastavro JsonDecoder require) rather than the bare
// pre-marshalled value.
func TestRegression_DefaultJSONIgnoresTaggedUnions(t *testing.T) {
	s := MustParse(`{
		"type": "record", "name": "R",
		"fields": [
			{"name": "f", "type": ["null", "string"], "default": "hello"}
		]
	}`)
	want := `{"f":{"string":"hello"}}`

	// Control: present value already wraps under TaggedUnions.
	got, err := s.EncodeJSON(map[string]any{"f": "hello"}, TaggedUnions())
	if err != nil {
		t.Fatalf("present: %v", err)
	}
	if string(got) != want {
		t.Fatalf("present: got %s want %s", got, want)
	}

	// Bug: missing-field default must wrap too.
	got, err = s.EncodeJSON(map[string]any{}, TaggedUnions())
	if err != nil {
		t.Fatalf("missing: %v", err)
	}
	if string(got) != want {
		t.Fatalf("missing-field default: got %s want %s", got, want)
	}
}

// TestRegression_NestedDefaultJSONIgnoresTaggedUnions locks in the
// recursive variant: a record default whose inner field is a non-null
// union must wrap the inner value under TaggedUnions. The encoder
// recursively re-dispatches into appendAvroJSON for nested defaults
// rather than splicing them verbatim.
func TestRegression_NestedDefaultJSONIgnoresTaggedUnions(t *testing.T) {
	s := MustParse(`{
		"type": "record", "name": "Outer",
		"fields": [
			{"name": "inner",
			 "type": {
				"type": "record", "name": "Inner",
				"fields": [
					{"name": "x", "type": ["null", "string"], "default": "hi"}
				]
			 },
			 "default": {"x": "hi"}}
		]
	}`)
	want := `{"inner":{"x":{"string":"hi"}}}`
	got, err := s.EncodeJSON(map[string]any{}, TaggedUnions())
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if string(got) != want {
		t.Fatalf("nested: got %s want %s", got, want)
	}
}

// TestMatrix_TimestampMillisMicrosSilentOverflow locks that the
// timestamp-millis/micros and local-timestamp-millis/micros encoders return an
// error for time.Time values whose UnixMilli/UnixMicro would overflow int64
// instead of silently wrapping. Java's Instant.toEpochMilli and
// TimestampMicrosConversion.toLong throw on the same input, and Go's UnixMilli /
// UnixMicro are documented as undefined for out-of-range times.
// timeToTimestampNanos has an analogous guard. Year 300_000 overflows micros
// (MaxInt64us is about year 294246); year 300_000_000 overflows millis.
func TestMatrix_TimestampMillisMicrosSilentOverflow(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		val    time.Time
	}{
		{"timestamp-micros y300k", `{"type":"long","logicalType":"timestamp-micros"}`, time.Date(300_000, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"local-timestamp-micros y300k", `{"type":"long","logicalType":"local-timestamp-micros"}`, time.Date(300_000, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"timestamp-millis y300M", `{"type":"long","logicalType":"timestamp-millis"}`, time.Date(300_000_000, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"local-timestamp-millis y300M", `{"type":"long","logicalType":"local-timestamp-millis"}`, time.Date(300_000_000, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := MustParse(tc.schema)
			if out, err := s.AppendEncode(nil, tc.val); err == nil {
				t.Errorf("binary encode of out-of-range %v silently produced %d bytes", tc.val, len(out))
			}
			if out, err := s.AppendEncodeJSON(nil, tc.val); err == nil {
				t.Errorf("JSON encode of out-of-range %v silently produced %s", tc.val, out)
			}
		})
	}
}

// TestRegression_TimestampMillisMicrosUnsafeOverflow is the unsafe-path
// parity test: the struct-field fast path (usTimestampMillis etc.) must
// also reject overflow, matching the safe path.
func TestRegression_TimestampMillisMicrosUnsafeOverflow(t *testing.T) {
	type RMillis struct {
		T time.Time `avro:"t"`
	}
	type RMicros struct {
		T time.Time `avro:"t"`
	}
	type RLocalMillis struct {
		T time.Time `avro:"t"`
	}
	type RLocalMicros struct {
		T time.Time `avro:"t"`
	}

	bigMicros := time.Date(300_000, 1, 1, 0, 0, 0, 0, time.UTC)
	bigMillis := time.Date(300_000_000, 1, 1, 0, 0, 0, 0, time.UTC)

	sMicros := MustParse(`{"type":"record","name":"R","fields":[{"name":"t","type":{"type":"long","logicalType":"timestamp-micros"}}]}`)
	sLocalMicros := MustParse(`{"type":"record","name":"R","fields":[{"name":"t","type":{"type":"long","logicalType":"local-timestamp-micros"}}]}`)
	sMillis := MustParse(`{"type":"record","name":"R","fields":[{"name":"t","type":{"type":"long","logicalType":"timestamp-millis"}}]}`)
	sLocalMillis := MustParse(`{"type":"record","name":"R","fields":[{"name":"t","type":{"type":"long","logicalType":"local-timestamp-millis"}}]}`)

	if _, err := sMicros.AppendEncode(nil, &RMicros{T: bigMicros}); err == nil {
		t.Errorf("unsafe timestamp-micros: silent overflow")
	}
	if _, err := sLocalMicros.AppendEncode(nil, &RLocalMicros{T: bigMicros}); err == nil {
		t.Errorf("unsafe local-timestamp-micros: silent overflow")
	}
	if _, err := sMillis.AppendEncode(nil, &RMillis{T: bigMillis}); err == nil {
		t.Errorf("unsafe timestamp-millis: silent overflow")
	}
	if _, err := sLocalMillis.AppendEncode(nil, &RLocalMillis{T: bigMillis}); err == nil {
		t.Errorf("unsafe local-timestamp-millis: silent overflow")
	}
}

// TestMatrix_DecodeJSONLongOverflowGap locks in that JSON-encoded long values
// exceeding int64 are rejected, not silently wrapped. parseJSONInt64 uses a
// per-digit pre-multiply bound that is safe near 2^64/9 — the boundary where the
// naive "n*10+d wrapped if it went down" post-multiply check has a gap, since
// n*10+d can wrap mod 2^64 to a value still ≥ prev. The 20-digit family
// 2049638230412172402d (d ∈ 0..9) probes that boundary. Java's
// JsonParser.getLongValue throws InputCoercionException; goavro's ParseInt rejects.
func TestMatrix_DecodeJSONLongOverflowGap(t *testing.T) {
	t.Run("bare long exceeding MaxInt64", func(t *testing.T) {
		s := MustParse(`"long"`)
		var got int64
		if err := s.DecodeJSON([]byte(`20496382304121724020`), &got); err == nil {
			t.Errorf("expected overflow error; got value=%d", got)
		}
	})
	t.Run("long inside record", func(t *testing.T) {
		s := MustParse(`{"type":"record","name":"R","fields":[{"name":"x","type":"long"}]}`)
		type rec struct {
			X int64 `avro:"x"`
		}
		var r rec
		if err := s.DecodeJSON([]byte(`{"x":20496382304121724020}`), &r); err == nil {
			t.Errorf("expected overflow error; got X=%d", r.X)
		}
	})
	t.Run("negative below MinInt64", func(t *testing.T) {
		s := MustParse(`"long"`)
		var got int64
		if err := s.DecodeJSON([]byte(`-20496382304121724020`), &got); err == nil {
			t.Errorf("expected overflow error; got value=%d", got)
		}
	})
	// MaxInt64 / MinInt64 themselves must still parse exactly.
	t.Run("MaxInt64 boundary", func(t *testing.T) {
		s := MustParse(`"long"`)
		var got int64
		if err := s.DecodeJSON([]byte(`9223372036854775807`), &got); err != nil {
			t.Fatalf("MaxInt64 should parse: %v", err)
		}
		if got != 9223372036854775807 {
			t.Errorf("got %d, want MaxInt64", got)
		}
	})
	t.Run("MinInt64 boundary", func(t *testing.T) {
		s := MustParse(`"long"`)
		var got int64
		if err := s.DecodeJSON([]byte(`-9223372036854775808`), &got); err != nil {
			t.Fatalf("MinInt64 should parse: %v", err)
		}
		if got != -9223372036854775808 {
			t.Errorf("got %d, want MinInt64", got)
		}
	})
	t.Run("MaxInt64 + 1 rejected", func(t *testing.T) {
		s := MustParse(`"long"`)
		var got int64
		if err := s.DecodeJSON([]byte(`9223372036854775808`), &got); err == nil {
			t.Errorf("MaxInt64+1 should be rejected; got %d", got)
		}
	})
	t.Run("MinInt64 - 1 rejected", func(t *testing.T) {
		s := MustParse(`"long"`)
		var got int64
		if err := s.DecodeJSON([]byte(`-9223372036854775809`), &got); err == nil {
			t.Errorf("MinInt64-1 should be rejected; got %d", got)
		}
	})
}

// TestRegression_DeserFixedArrayBlockCountOverflow locks in that
// decoding into a fixed-size Go array [N]T errors instead of panicking
// when the wire's block count would overflow the int64 idx+count
// arithmetic. The bound check uses `count > int64(arrLen-idx)` —
// comparing two non-negative int64 values without wrap risk — rather
// than the `idx+int(count) > arrLen` form which wraps for count near
// MaxInt64 and would otherwise let an attacker-controlled count panic
// v.Index(idx) when idx exceeds arrLen.
func TestRegression_DeserFixedArrayBlockCountOverflow(t *testing.T) {
	s := MustParse(`{"type":"array","items":"null"}`)
	enc := func(n int64) []byte {
		zz := uint64(n<<1) ^ uint64(n>>63)
		var out []byte
		for zz >= 0x80 {
			out = append(out, byte(zz)|0x80)
			zz >>= 7
		}
		return append(out, byte(zz))
	}
	src := append(append([]byte{}, enc(1)...), enc(math.MaxInt64)...)

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("decode panicked: %v", r)
		}
	}()
	var v [3]any
	if _, err := s.Decode(src, &v); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

// TestRegression_SkipArrayBlockCountOverflow locks in that schema
// resolution dropping an array<null> field doesn't hang on a hostile
// wire. The pre-add check `count > maxZeroByteItems-totalItems`
// catches accumulator wraparound: a totalItems += count form would
// wrap to MinInt64+999 for block 1 count=4000 + block 2 count=
// MaxInt64-3000, bypass the maxZeroByteItems cap, and run ~MaxInt64
// iterations of skipNull (a no-op), hanging the process.
func TestRegression_SkipArrayBlockCountOverflow(t *testing.T) {
	writer := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"drop","type":{"type":"array","items":"null"}},
		{"name":"keep","type":"int"}]}`)
	reader := MustParse(`{"type":"record","name":"R","fields":[{"name":"keep","type":"int"}]}`)

	enc := func(n int64) []byte {
		zz := uint64(n<<1) ^ uint64(n>>63)
		var out []byte
		for zz >= 0x80 {
			out = append(out, byte(zz)|0x80)
			zz >>= 7
		}
		return append(out, byte(zz))
	}
	src := []byte{}
	src = append(src, enc(4000)...)
	src = append(src, enc(math.MaxInt64-3000)...)
	src = append(src, enc(0)...)
	src = append(src, enc(42)...)

	resolved := mustResolve(t, writer, reader)
	type recOut struct {
		Keep int32 `avro:"keep"`
	}

	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		var got recOut
		_, err := resolved.Decode(src, &got)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
	case <-ctx.Done():
		t.Fatalf("decode timed out: infinite loop in skipArray")
	}
}

// TestRegression_DeserArraySliceBlockCountOverflow locks in that the
// slice-target array decode path rejects a hostile block stream where
// totalItems would wrap past int64. Caught primarily by the pre-add
// overflow-safe cap in checkArrayBlockBounds; the downstream
// `start > math.MaxInt-n` slice-grow check is a secondary defense.
func TestRegression_DeserArraySliceBlockCountOverflow(t *testing.T) {
	s := MustParse(`{"type":"array","items":"null"}`)
	enc := func(n int64) []byte {
		zz := uint64(n<<1) ^ uint64(n>>63)
		var out []byte
		for zz >= 0x80 {
			out = append(out, byte(zz)|0x80)
			zz >>= 7
		}
		return append(out, byte(zz))
	}
	src := append(append([]byte{}, enc(4000)...), enc(math.MaxInt64-3000)...)
	src = append(src, enc(0)...)

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("decode panicked: %v", r)
		}
	}()
	var v []any
	if _, err := s.Decode(src, &v); err == nil {
		t.Fatalf("expected error, got nil; len(v)=%d", len(v))
	}
}

// TestRegression_TimestampMillisMinInt64 locks in that timeToTimestampMillis
// accepts time.Time values constructed from MinInt64 milliseconds since epoch.
// Go's time normalization makes the seconds component of
// time.UnixMilli(MinInt64) = -maxSec - 1 (the remainder being negative,
// normalization decrements sec and adds 1e9 to nsec), so we mirror Java's
// Instant.toEpochMilli adjustment branch for sec < 0 && nsec > 0 —
// `(sec+1)*1000 + (nsec/1e6 - 1000)` — which accepts the full int64 range. A
// naive symmetric guard `sec > maxSec || sec < -maxSec` would reject
// sec = -maxSec - 1.
func TestRegression_TimestampMillisMinInt64(t *testing.T) {
	in := time.UnixMilli(math.MinInt64).UTC()
	type R struct {
		T time.Time `avro:"t"`
	}
	s := MustParse(`{"type":"record","name":"R","fields":[{"name":"t","type":{"type":"long","logicalType":"timestamp-millis"}}]}`)
	if _, err := s.AppendEncode(nil, R{T: in}); err != nil {
		t.Fatalf("encode failed for MinInt64 millis: %v", err)
	}
}

// TestRegression_TimestampMicrosMinInt64 — same parity rule for
// microseconds. Java's TimestampMicrosConversion.toLong (line 185-198)
// has the explicit adjustment branch.
func TestRegression_TimestampMicrosMinInt64(t *testing.T) {
	in := time.UnixMicro(math.MinInt64).UTC()
	type R struct {
		T time.Time `avro:"t"`
	}
	s := MustParse(`{"type":"record","name":"R","fields":[{"name":"t","type":{"type":"long","logicalType":"timestamp-micros"}}]}`)
	if _, err := s.AppendEncode(nil, R{T: in}); err != nil {
		t.Fatalf("encode failed for MinInt64 micros: %v", err)
	}
}

// TestRegression_TimestampMillisMinInt64UnsafePath — the unsafe
// struct-fast-path (usTimestampMillis) calls the same helper so the
// MinInt64 acceptance propagates automatically. This test pins that.
func TestRegression_TimestampMillisMinInt64UnsafePath(t *testing.T) {
	type R struct {
		T time.Time `avro:"t"`
	}
	s := MustParse(`{"type":"record","name":"R","fields":[{"name":"t","type":{"type":"long","logicalType":"timestamp-millis"}}]}`)
	if _, err := s.AppendEncode(nil, R{T: time.UnixMilli(0).UTC()}); err != nil {
		t.Fatalf("warm encode failed: %v", err)
	}
	in := time.UnixMilli(math.MinInt64).UTC()
	if _, err := s.AppendEncode(nil, R{T: in}); err != nil {
		t.Fatalf("encode failed for MinInt64 millis (unsafe path): %v", err)
	}
}

// TestRegression_LocalTimestampMillisMinInt64 — local-timestamp
// variants delegate to the timestamp-* helpers, inheriting the same
// MinInt64 acceptance. Java's LocalTimestampMillisConversion.toLong
// (line 274-277) similarly delegates to TimestampMillisConversion.toLong.
func TestRegression_LocalTimestampMillisMinInt64(t *testing.T) {
	in := time.UnixMilli(math.MinInt64).UTC()
	type R struct {
		T time.Time `avro:"t"`
	}
	s := MustParse(`{"type":"record","name":"R","fields":[{"name":"t","type":{"type":"long","logicalType":"local-timestamp-millis"}}]}`)
	if _, err := s.AppendEncode(nil, R{T: in}); err != nil {
		t.Fatalf("encode failed for MinInt64 local-timestamp-millis: %v", err)
	}
}

// TestRegression_DecodeJSONCustomDecoderConcurrentRace locks in that
// concurrent Schema.DecodeJSON calls on a schema with custom-typed
// fields don't race on shared state. JSON dispatch is closure-
// captured at schema build (parallel to the binary path's
// wrapDeserWithCustomDecoders), so the schema graph is read-only at
// decode time.
func TestRegression_DecodeJSONCustomDecoderConcurrentRace(t *testing.T) {
	type Money struct{ Cents int64 }
	moneyType := NewCustomType[Money, int64](
		"money",
		func(m Money, _ *SchemaNode) (int64, error) { return m.Cents, nil },
		func(c int64, _ *SchemaNode) (Money, error) { return Money{Cents: c}, nil },
	)
	s := mustParse(t, `{"type":"long","logicalType":"money"}`, moneyType)

	const N = 200
	var wg sync.WaitGroup
	errs := make([]error, N)
	results := make([]Money, N)
	for i := range N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var got Money
			err := s.DecodeJSON([]byte(`42`), &got)
			errs[idx] = err
			results[idx] = got
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: error %v", i, err)
		}
		if results[i].Cents != 42 {
			t.Errorf("goroutine %d: got %+v, want Money{Cents:42}", i, results[i])
		}
	}
}

// TestRegression_EncodeJSONNilPtrIntoNonNullableUnion locks in that
// EncodeJSON of a nil *T into a union that doesn't contain "null"
// errors instead of silently emitting "null". The binary path
// (serUnion.ser → branch tryAll) errors with "no matching branch",
// Java/fastavro reject the same input (UnresolvedUnionException /
// "do not match"), and the library's own DecodeJSON rejects null
// against a no-null union (see
// TestRegression_UnionWithoutNullBranchAcceptsJsonNull). Encoding
// "null" here would produce output the library can't read back.
func TestRegression_EncodeJSONNilPtrIntoNonNullableUnion(t *testing.T) {
	s := MustParse(`["int","string"]`)
	var p *int
	out, err := s.EncodeJSON(p)
	if err == nil {
		t.Errorf("EncodeJSON(*int(nil)) into [int,string]: got %q, want error", out)
	}
}

// TestRegression_EncodeJSONNilInterfaceIntoNonNullableUnion is the
// nil-any sibling of the nil-pointer reject above.
func TestRegression_EncodeJSONNilInterfaceIntoNonNullableUnion(t *testing.T) {
	s := MustParse(`["int","string"]`)
	var x any
	out, err := s.EncodeJSON(x)
	if err == nil {
		t.Errorf("EncodeJSON(any(nil)) into [int,string]: got %q, want error", out)
	}
}

// TestRegression_EncodeJSONNilPtrIntoNullableUnion is the positive
// counterpart: nil into a union containing "null" must still emit
// "null".
func TestRegression_EncodeJSONNilPtrIntoNullableUnion(t *testing.T) {
	s := MustParse(`["null","string"]`)
	var p *string
	out, err := s.EncodeJSON(p)
	if err != nil {
		t.Fatalf("nullable union should accept nil: %v", err)
	}
	if string(out) != "null" {
		t.Errorf("got %q, want \"null\"", out)
	}
}

// TestMatrix_EncodeJSONNullParity locks binary/JSON encode parity for the plain
// "null" type across every site reaching serNull or appendAvroJSON's case
// "null". Both must (1) reject non-nil non-nilable values with errNonNil — the
// JSON arm cannot just emit literal `null` regardless of v — and (2) accept
// typed-nil values arriving via an Interface wrapper, since generic serUnion /
// serArray / serMap dispatch calls serNull with Kind=Interface and both sides
// must peel before the kind switch.
//
// The 2-branch [null,T] optimization is unaffected (serNullUnionAt → isNilValue
// peels interfaces); the concern is 3+ branch dispatch and the
// array<null>/map<null>/null-typed-field cases.
//
// The matrix covers both directions at the four sites routing through serNull's
// kind-switch: a top-level "null" schema, a null-typed record field, a
// tagged-union null branch in a 3+ branch union, and array<null> items /
// map<null> values.
//
// Cross-impl: Java and fastavro are silently lenient on both wires
// (GenericDatumWriter.NULL writes the marker without checking datum; same in
// write_null). twmb's binary path is deliberately strict per
// TestSerNullNonNilableType, so this brings JSON to the strict choice rather
// than weakening binary.
func TestMatrix_EncodeJSONNullParity(t *testing.T) {
	// ---- Reject arm: non-nil non-nilable values must error on both paths ----

	t.Run("plain null schema, non-nil int", func(t *testing.T) {
		s := MustParse(`"null"`)
		if _, err := s.AppendEncode(nil, 42); err == nil {
			t.Fatal("binary: expected error encoding non-nil into null schema")
		}
		if out, err := s.AppendEncodeJSON(nil, 42); err == nil {
			t.Errorf("JSON: expected error encoding non-nil into null schema, got %s", out)
		}
	})
	t.Run("null-typed record field, non-nil value", func(t *testing.T) {
		s := MustParse(`{"type":"record","name":"R","fields":[{"name":"x","type":"null"}]}`)
		in := map[string]any{"x": 42}
		if _, err := s.AppendEncode(nil, in); err == nil {
			t.Fatal("binary: expected error encoding non-nil into null field")
		}
		if out, err := s.AppendEncodeJSON(nil, in); err == nil {
			t.Errorf("JSON: expected error encoding non-nil into null field, got %s", out)
		}
	})
	t.Run("tagged-union null tag with non-nil value", func(t *testing.T) {
		s := MustParse(`["null","int"]`)
		in := map[string]any{"null": 42}
		if _, err := s.AppendEncode(nil, in); err == nil {
			t.Fatal("binary: expected error for {\"null\":42} against [null,int]")
		}
		if out, err := s.AppendEncodeJSON(nil, in); err == nil {
			t.Errorf("JSON: expected error for {\"null\":42} against [null,int], got %s", out)
		}
	})
	t.Run("typed-nil map into null schema accepted", func(t *testing.T) {
		// Symmetric positive case: a typed-nil map IS a valid no-value
		// representation (matches serNull's IsNil arm for Map kind).
		s := MustParse(`"null"`)
		var m map[string]any
		out, err := s.AppendEncodeJSON(nil, m)
		if err != nil {
			t.Fatalf("typed-nil map into null schema: %v", err)
		}
		if string(out) != "null" {
			t.Errorf("got %q, want \"null\"", out)
		}
	})

	// ---- Accept arm: typed-nil wrapped in any() must be recognized as
	// null at every dispatch site that calls serNull on iter.Value()
	// (the wrapped-Interface form). The 2-branch [null,T] case at the
	// top-level uses serNullUnionAt → isNilValue and is indirect-aware
	// by construction; the cases below all route through the generic
	// serNull which must peel the Interface wrapper before its kind
	// switch (a bare Kind=Interface IsNil=false check would miss
	// typed-nil maps/pointers wrapped via any()). Each subtest exercises
	// one of the four sites and asserts binary == JSON outcome.

	parity := func(t *testing.T, s *Schema, in any) {
		t.Helper()
		_, binErr := s.AppendEncode(nil, in)
		_, jsonErr := s.AppendEncodeJSON(nil, in)
		if (binErr == nil) != (jsonErr == nil) {
			t.Errorf("parity violation: binary err=%v, JSON err=%v", binErr, jsonErr)
		}
		if binErr != nil {
			t.Errorf("binary rejected typed-nil-via-interface: %v", binErr)
		}
	}

	t.Run("3-branch union tagged null with typed-nil pointer", func(t *testing.T) {
		s := MustParse(`["null","int","string"]`)
		parity(t, s, map[string]any{"null": (*int)(nil)})
	})
	t.Run("3-branch union tagged null with typed-nil map", func(t *testing.T) {
		s := MustParse(`["null","int","string"]`)
		parity(t, s, map[string]any{"null": map[string]any(nil)})
	})
	t.Run("3-branch union tagged null with typed-nil slice", func(t *testing.T) {
		s := MustParse(`["null","int","string"]`)
		parity(t, s, map[string]any{"null": []byte(nil)})
	})
	t.Run("array<null> with typed-nil pointer items", func(t *testing.T) {
		s := MustParse(`{"type":"array","items":"null"}`)
		parity(t, s, []any{(*int)(nil), (*int)(nil)})
	})
	t.Run("array<null> with typed-nil map items", func(t *testing.T) {
		s := MustParse(`{"type":"array","items":"null"}`)
		parity(t, s, []any{map[string]any(nil), map[string]any(nil)})
	})
	t.Run("map<null> with typed-nil pointer values", func(t *testing.T) {
		s := MustParse(`{"type":"map","values":"null"}`)
		parity(t, s, map[string]any{"a": (*int)(nil), "b": (*int)(nil)})
	})
	t.Run("map<null> with typed-nil map values", func(t *testing.T) {
		s := MustParse(`{"type":"map","values":"null"}`)
		parity(t, s, map[string]any{"a": map[string]any(nil)})
	})
	t.Run("record field 3-branch union, tagged null typed-nil pointer", func(t *testing.T) {
		s := MustParse(`{"type":"record","name":"R","fields":[
			{"name":"x","type":["null","int","string"]}
		]}`)
		parity(t, s, map[string]any{"x": map[string]any{"null": (*int)(nil)}})
	})
	t.Run("record field 3-branch union, tagged null typed-nil map", func(t *testing.T) {
		s := MustParse(`{"type":"record","name":"R","fields":[
			{"name":"x","type":["null","int","string"]}
		]}`)
		parity(t, s, map[string]any{"x": map[string]any{"null": map[string]any(nil)}})
	})
	t.Run("null-typed record field, typed-nil pointer value", func(t *testing.T) {
		// Sibling: the map-fast-path of serRecord.ser implicitly
		// unwraps via Go's interface unboxing (m["x"] returns the
		// underlying nil pointer), so the value reaches serNull as
		// Kind=Pointer not Kind=Interface and was always accepted.
		// Pin it so a future change to that path can't regress.
		s := MustParse(`{"type":"record","name":"R","fields":[{"name":"x","type":"null"}]}`)
		parity(t, s, map[string]any{"x": (*int)(nil)})
	})
}

// TestMatrix_EncodeJSONNullParityPointerToNilPointer extends the null-parity net
// to the **T-with-nil-inner shape: a non-nil outer pointer whose Elem() is a nil
// pointer, with or without an enclosing any{} wrapper. The 2-branch [null,T]
// optimization works via isNilValue, which peels both Pointer and Interface, but
// serNull peeled only Interface, so the outer Pointer's IsNil()==false reached
// the kind switch and errNonNil came back — while JSON's appendAvroJSON indirect
// loop already peeled both and succeeded, a binary/JSON asymmetry.
func TestMatrix_EncodeJSONNullParityPointerToNilPointer(t *testing.T) {
	nilIntPtrPtr := func() any { var p *int; return &p }
	nilMapPtrPtr := func() any { var p *map[string]any; return &p }

	parity := func(t *testing.T, s *Schema, in any) {
		t.Helper()
		_, binErr := s.AppendEncode(nil, in)
		_, jsonErr := s.AppendEncodeJSON(nil, in)
		if (binErr == nil) != (jsonErr == nil) {
			t.Errorf("parity violation: binary err=%v, JSON err=%v", binErr, jsonErr)
		}
		if binErr != nil {
			t.Errorf("binary rejected **T with nil inner: %v", binErr)
		}
	}

	t.Run("top-level null + &nilIntPtr", func(t *testing.T) {
		parity(t, MustParse(`"null"`), nilIntPtrPtr())
	})
	t.Run("top-level null + any(&nilIntPtr)", func(t *testing.T) {
		parity(t, MustParse(`"null"`), any(nilIntPtrPtr()))
	})
	t.Run("top-level null + &nilMapPtr", func(t *testing.T) {
		parity(t, MustParse(`"null"`), nilMapPtrPtr())
	})
	t.Run("3-branch union + &nilIntPtr", func(t *testing.T) {
		parity(t, MustParse(`["null","int","string"]`), nilIntPtrPtr())
	})
	t.Run("3-branch union + any(&nilIntPtr)", func(t *testing.T) {
		parity(t, MustParse(`["null","int","string"]`), any(nilIntPtrPtr()))
	})
	t.Run("array<null> + any(&nilIntPtr) elements", func(t *testing.T) {
		parity(t, MustParse(`{"type":"array","items":"null"}`),
			[]any{any(nilIntPtrPtr())})
	})
	t.Run("map<null> + any(&nilIntPtr) values", func(t *testing.T) {
		parity(t, MustParse(`{"type":"map","values":"null"}`),
			map[string]any{"a": any(nilIntPtrPtr())})
	})
	t.Run("record field null + &nilIntPtr", func(t *testing.T) {
		s := MustParse(`{"type":"record","name":"R","fields":[{"name":"x","type":"null"}]}`)
		parity(t, s, map[string]any{"x": nilIntPtrPtr()})
	})
	t.Run("record field 3-branch + any(&nilIntPtr)", func(t *testing.T) {
		s := MustParse(`{"type":"record","name":"R","fields":[{"name":"x","type":["null","int","string"]}]}`)
		parity(t, s, map[string]any{"x": any(nilIntPtrPtr())})
	})
}

// TestMatrix_EncodeJSONNullParityBareNilContainer locks parity between the
// binary and JSON union encoders for a bare nil Map / Slice / []byte against a
// multi-branch union containing "null". appendAvroJSONUnion's try-each loop
// unconditionally skipped the null branch, and the upstream peel only handles
// Pointer/Interface, so a bare nil Map never landed on case "null": binary
// picked null while JSON returned "no union branch matched", or silently emitted
// "" for nil []byte. The fix drops the null-skip; case "null" rejects non-nil
// with errNonNil so non-nil inputs fall cleanly through.
//
// Distinct from TestMatrix_EncodeJSONNullParity, which covers TAGGED dispatch.
// The bare form goes through unionTypeNameForValue into try-each, exactly where
// the null-skip blocked it — every existing parity test passes the typed-nil
// through the tagged form, so widening serNull's peel alone hides the bug.
func TestMatrix_EncodeJSONNullParityBareNilContainer(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		value  any
		// wantBin / wantJSON are the expected wire / JSON outputs.
		wantBin  []byte
		wantJSON string
	}{
		{
			name:     "bare nil map against [null,int,string]",
			schema:   `["null","int","string"]`,
			value:    map[string]any(nil),
			wantBin:  []byte{0},
			wantJSON: "null",
		},
		{
			name:     "bare nil slice against [null,int,string]",
			schema:   `["null","int","string"]`,
			value:    []int(nil),
			wantBin:  []byte{0},
			wantJSON: "null",
		},
		{
			name:     "bare nil []byte against [null,int,string]",
			schema:   `["null","int","string"]`,
			value:    []byte(nil),
			wantBin:  []byte{0},
			wantJSON: "null",
		},
		{
			name:     "any-wrapped nil map",
			schema:   `["null","int","string"]`,
			value:    any(map[string]any(nil)),
			wantBin:  []byte{0},
			wantJSON: "null",
		},
		{
			name:     "any-wrapped nil slice",
			schema:   `["null","int","string"]`,
			value:    any([]int(nil)),
			wantBin:  []byte{0},
			wantJSON: "null",
		},
		{
			name:     "any-wrapped nil []byte",
			schema:   `["null","int","string"]`,
			value:    any([]byte(nil)),
			wantBin:  []byte{0},
			wantJSON: "null",
		},
		{
			name:     "2-branch [null,int] + bare nil map",
			schema:   `["null","int"]`,
			value:    map[string]any(nil),
			wantBin:  []byte{0},
			wantJSON: "null",
		},
		{
			name:     "2-branch [null,int] + bare nil slice",
			schema:   `["null","int"]`,
			value:    []int(nil),
			wantBin:  []byte{0},
			wantJSON: "null",
		},
		{
			name:     "2-branch [int,null] + bare nil map (null second)",
			schema:   `["int","null"]`,
			value:    map[string]any(nil),
			wantBin:  []byte{2},
			wantJSON: "null",
		},
		{
			name:     "array<[null,int,string]> with nil-map items",
			schema:   `{"type":"array","items":["null","int","string"]}`,
			value:    []any{map[string]any(nil), map[string]any(nil)},
			wantBin:  []byte{4, 0, 0, 0},
			wantJSON: "[null,null]",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := MustParse(tc.schema)
			bin, err := s.AppendEncode(nil, tc.value)
			if err != nil {
				t.Fatalf("binary encode: %v", err)
			}
			if !bytes.Equal(bin, tc.wantBin) {
				t.Errorf("binary: got %v, want %v", bin, tc.wantBin)
			}
			jbin, err := s.AppendEncodeJSON(nil, tc.value)
			if err != nil {
				t.Fatalf("JSON encode (binary↔JSON parity gap): %v", err)
			}
			if string(jbin) != tc.wantJSON {
				t.Errorf("JSON: got %q, want %q", jbin, tc.wantJSON)
			}
		})
	}
}

// TestMatrix_EncodeJSONNullBytesUnionParity locks the "Go nil = absent → null
// branch" semantic uniformly across union arities and encoders: all four
// dispatch sites must agree — binary 2-branch (serNullUnionAt → isNilValue),
// binary N-branch, JSON N-branch, and JSON tagged. A nil-first short-circuit at
// the entry of serUnion.ser and appendAvroJSONUnion applies the general rule;
// without it, three near-identical schemas give three results for []byte(nil),
// N-branch type-name dispatch naming Slice<uint8> "bytes" regardless of IsNil.
// Dropping the null-skip from try-each alone does not suffice — type-name
// dispatch fires first.
func TestMatrix_EncodeJSONNullBytesUnionParity(t *testing.T) {
	tests := []struct {
		name     string
		schema   string
		value    any
		wantBin  []byte
		wantJSON string
	}{
		{
			name:     "2-branch [null,bytes] + []byte(nil)",
			schema:   `["null","bytes"]`,
			value:    []byte(nil),
			wantBin:  []byte{0}, // null branch
			wantJSON: "null",
		},
		{
			name:     "2-branch [bytes,null] + []byte(nil) (null second)",
			schema:   `["bytes","null"]`,
			value:    []byte(nil),
			wantBin:  []byte{2}, // null branch (idx 1)
			wantJSON: "null",
		},
		{
			name:     "2-branch [null,bytes] + any([]byte(nil))",
			schema:   `["null","bytes"]`,
			value:    any([]byte(nil)),
			wantBin:  []byte{0},
			wantJSON: "null",
		},
		{
			name: "record with [null,bytes] field + []byte(nil)",
			schema: `{
				"type":"record",
				"name":"R",
				"fields":[
					{"name":"data","type":["null","bytes"],"default":null}
				]
			}`,
			value:    map[string]any{"data": []byte(nil)},
			wantBin:  []byte{0},
			wantJSON: `{"data":null}`,
		},
		{
			name: "array<[null,bytes]> with nil-byte items",
			schema: `{
				"type":"array",
				"items":["null","bytes"]
			}`,
			value: []any{[]byte(nil), []byte("hi"), []byte(nil)},
			// block-count=3, null-byte, bytes-branch len=2 "hi", null-byte, block-end
			wantBin:  []byte{6, 0, 2, 4, 'h', 'i', 0, 0},
			wantJSON: `[null,"hi",null]`,
		},
		// Sanity: non-nil empty []byte{} stays in bytes branch on both sides.
		{
			name:     "2-branch [null,bytes] + []byte{} (non-nil empty)",
			schema:   `["null","bytes"]`,
			value:    []byte{},
			wantBin:  []byte{2, 0},
			wantJSON: `""`,
		},
		// 3-branch: nil-first dispatch wins on both sides — picks
		// null uniformly with the 2-branch case. Without the nil-first
		// short-circuit, binary would pick bytes (idx 2 → wire [4, 0])
		// and JSON would pick bytes (`""`), a binary 2-branch ↔
		// 3-branch inconsistency.
		{
			name:     "3-branch [null,int,bytes] + []byte(nil)",
			schema:   `["null","int","bytes"]`,
			value:    []byte(nil),
			wantBin:  []byte{0}, // null branch
			wantJSON: "null",
		},
		// 3-branch with null NOT first: still picks null because the
		// nil-first rule is order-independent (type-name dispatch
		// would otherwise pick bytes regardless of position).
		{
			name:     "3-branch [bytes,int,null] + []byte(nil)",
			schema:   `["bytes","int","null"]`,
			value:    []byte(nil),
			wantBin:  []byte{4}, // null branch (idx 2 → zigzag 4)
			wantJSON: "null",
		},
		// 3-branch without a null branch: type-name dispatch picks
		// bytes (the only sensible choice).
		{
			name:     "3-branch [int,bytes,string] (no null) + []byte(nil)",
			schema:   `["int","bytes","string"]`,
			value:    []byte(nil),
			wantBin:  []byte{2, 0}, // bytes branch (idx 1), length 0
			wantJSON: `""`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := MustParse(tc.schema)
			bin := mustAppendEncode(t, s, nil, tc.value)
			if !bytes.Equal(bin, tc.wantBin) {
				t.Errorf("binary: got %v want %v", bin, tc.wantBin)
			}
			js := mustAppendEncodeJSON(t, s, nil, tc.value)
			if string(js) != tc.wantJSON {
				t.Errorf("JSON: got %q want %q", js, tc.wantJSON)
			}
		})
	}
}

// TestMatrix_EncodeNullParity2BranchNilChanFunc locks parity between the binary
// 2-branch [null,T] optimization and the binary 3-branch / JSON paths for nil
// Chan and nil Func. serNull and appendAvroJSON's case "null" both accept
// v.IsNil() for {Pointer, Interface, Map, Slice, Chan, Func}, but isNilValue —
// used only by the 2-branch optimization — peeled Pointer/Interface while its
// terminal kind switch covered only {Map, Slice}, so a nil Chan/Func fell to
// fns[valIdx] and errored where both try-each paths accepted. isNilValue's
// terminal switch must match serNull's accept set exactly.
func TestMatrix_EncodeNullParity2BranchNilChanFunc(t *testing.T) {
	cases := []struct {
		name    string
		schema  string
		value   any
		wantBin []byte
	}{
		{
			name:    "2-branch [null,int] + nil chan",
			schema:  `["null","int"]`,
			value:   chan int(nil),
			wantBin: []byte{0}, // null branch
		},
		{
			name:    "2-branch [int,null] + nil chan",
			schema:  `["int","null"]`,
			value:   chan int(nil),
			wantBin: []byte{2}, // null branch at idx 1
		},
		{
			name:    "2-branch [null,int] + nil func",
			schema:  `["null","int"]`,
			value:   (func())(nil),
			wantBin: []byte{0},
		},
		{
			name:    "2-branch [null,string] + nil chan",
			schema:  `["null","string"]`,
			value:   chan int(nil),
			wantBin: []byte{0},
		},
		// Sanity: 3-branch path already worked because serUnion.ser
		// try-each reaches serNull which has the broader kind set.
		{
			name:    "3-branch [null,int,string] + nil chan (unchanged)",
			schema:  `["null","int","string"]`,
			value:   chan int(nil),
			wantBin: []byte{0},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := MustParse(tc.schema)
			bin, err := s.AppendEncode(nil, tc.value)
			if err != nil {
				t.Fatalf("binary: %v (want encode to %v)", err, tc.wantBin)
			}
			if !bytes.Equal(bin, tc.wantBin) {
				t.Errorf("binary: got %v want %v", bin, tc.wantBin)
			}
			// JSON side: should also produce null. Both 2-branch and
			// 3-branch JSON paths route to "null" via the patched
			// appendAvroJSONUnion (case "null" / 2-branch short-circuit).
			js, err := s.AppendEncodeJSON(nil, tc.value)
			if err != nil {
				t.Fatalf("json: %v", err)
			}
			if string(js) != "null" {
				t.Errorf("json: got %q want %q", js, "null")
			}
		})
	}
}

// TestRegression_TimestampNanosMinInt64 locks in that
// timeToTimestampNanos accepts time.Time values constructed from
// MinInt64 nanoseconds since epoch, matching avro-rs and fastavro.
// Java's TimestampNanosConversion.toLong has an off-by-1000 typo
// (TimeConversions.java:238 uses `nanos - 1_000_000` instead of
// `nanos - 1_000_000_000`) that would propagate ~999ms of error per
// negative-second instant, so we deliberately diverge from Java for
// this single conversion and align with avro-rs / fastavro instead.
func TestRegression_TimestampNanosMinInt64(t *testing.T) {
	in := time.Unix(0, math.MinInt64).UTC()
	type R struct {
		T time.Time `avro:"t"`
	}
	s := MustParse(`{"type":"record","name":"R","fields":[{"name":"t","type":{"type":"long","logicalType":"timestamp-nanos"}}]}`)
	enc, err := s.AppendEncode(nil, R{T: in})
	if err != nil {
		t.Fatalf("encode failed for MinInt64 nanos: %v", err)
	}
	type R2 struct {
		T int64 `avro:"t"`
	}
	rawSchema := MustParse(`{"type":"record","name":"R","fields":[{"name":"t","type":"long"}]}`)
	var r2 R2
	mustDecode(t, rawSchema, enc, &r2)
	if r2.T != math.MinInt64 {
		t.Fatalf("wire long: got %d, want MinInt64", r2.T)
	}
}

// TestRegression_LocalTimestampNanosMinInt64 — local-timestamp-nanos
// delegates to timeToTimestampNanos, inheriting the same fix.
func TestRegression_LocalTimestampNanosMinInt64(t *testing.T) {
	in := time.Unix(0, math.MinInt64).UTC()
	type R struct {
		T time.Time `avro:"t"`
	}
	s := MustParse(`{"type":"record","name":"R","fields":[{"name":"t","type":{"type":"long","logicalType":"local-timestamp-nanos"}}]}`)
	if _, err := s.AppendEncode(nil, R{T: in}); err != nil {
		t.Fatalf("encode failed for MinInt64 local-timestamp-nanos: %v", err)
	}
}

type myKey string

// TestRegression_DecodeJSONFixedSizeMismatchTypedTarget locks in that
// JSON decode of fixed/bytes into [N]byte rejects a length mismatch
// instead of silently zero-padding (when wire shorter than N) or
// truncating (when wire longer than N). reflect.Copy at the previous
// site copied min(len(b), v.Len()) bytes without erroring, producing
// silently-corrupt values. The binary path's deserBytes / deserFixed
// already reject this; the JSON encode side already rejects this; the
// JSON decode side was the outlier.
func TestRegression_DecodeJSONFixedSizeMismatchTypedTarget(t *testing.T) {
	s := MustParse(`{"type":"fixed","name":"F","size":4}`)
	enc := mustEncodeJSON(t, s, [4]byte{0xde, 0xad, 0xbe, 0xef})
	var got [16]byte
	if err := s.DecodeJSON(enc, &got); err == nil {
		t.Errorf("expected size-mismatch error, got %x with no error", got)
	}
}

func TestRegression_DecodeJSONBytesSizeMismatchTypedTarget(t *testing.T) {
	s := MustParse(`"bytes"`)
	enc := mustEncodeJSON(t, s, []byte{0xde, 0xad, 0xbe, 0xef})
	var got [16]byte
	if err := s.DecodeJSON(enc, &got); err == nil {
		t.Errorf("expected size-mismatch error, got %x with no error", got)
	}
}

func TestRegression_DecodeJSONBytesOverflowTypedTarget(t *testing.T) {
	s := MustParse(`"bytes"`)
	enc := mustEncodeJSON(t, s, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	var got [4]byte
	if err := s.DecodeJSON(enc, &got); err == nil {
		t.Errorf("expected size-mismatch error, got %x with no error", got)
	}
}

// TestRegression_SerRecordMapNamedStringKey locks in that binary
// encoding a record from map[NamedKey]any (where NamedKey has
// underlying type string) doesn't panic. v.MapIndex must use a key
// value of the map's exact key type — a bare string panics when the
// map's key type is a named subtype, even though Go's type system
// allows the conversion at the source level.
func TestRegression_SerRecordMapNamedStringKey(t *testing.T) {
	s := MustParse(`{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`)
	m := map[myKey]any{myKey("x"): int32(7)}
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("encode panicked: %v", r)
		}
	}()
	if _, err := s.AppendEncode(nil, m); err != nil {
		t.Errorf("encode err: %v", err)
	}
}

func TestRegression_AppendAvroJSONRecordMapNamedStringKey(t *testing.T) {
	s := MustParse(`{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`)
	m := map[myKey]any{myKey("x"): int32(7)}
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("EncodeJSON panicked: %v", r)
		}
	}()
	if _, err := s.EncodeJSON(m); err != nil {
		t.Errorf("encode err: %v", err)
	}
}

func TestRegression_DecodeBinaryMapNamedStringKey(t *testing.T) {
	s := MustParse(`{"type":"map","values":"int"}`)
	enc := mustAppendEncode(t, s, nil, map[string]int32{"x": 7})
	var got map[myKey]int32
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Decode panicked: %v", r)
		}
	}()
	if _, err := s.Decode(enc, &got); err != nil {
		t.Errorf("decode err: %v", err)
	}
	if got[myKey("x")] != 7 {
		t.Errorf("got %v, want map[x:7]", got)
	}
}

func TestRegression_DeserMapStringBlockNamedKey(t *testing.T) {
	s := MustParse(`{"type":"map","values":"string"}`)
	enc := mustAppendEncode(t, s, nil, map[string]string{"x": "hi"})
	var got map[myKey]string
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("string-fast-block decode panicked: %v", r)
		}
	}()
	if _, err := s.Decode(enc, &got); err != nil {
		t.Errorf("decode err: %v", err)
	}
	if got[myKey("x")] != "hi" {
		t.Errorf("got %v, want map[x:hi]", got)
	}
}

func TestRegression_DecodeJSONMapNamedStringKey(t *testing.T) {
	s := MustParse(`{"type":"map","values":"int"}`)
	var got map[myKey]int32
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("DecodeJSON panicked: %v", r)
		}
	}()
	if err := s.DecodeJSON([]byte(`{"x":7}`), &got); err != nil {
		t.Errorf("decode err: %v", err)
	}
	if got[myKey("x")] != 7 {
		t.Errorf("got %v, want map[x:7]", got)
	}
}

func TestRegression_DecodeJSONRecordMapNamedStringKey(t *testing.T) {
	s := MustParse(`{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`)
	var got map[myKey]any
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("DecodeJSON panicked: %v", r)
		}
	}()
	if err := s.DecodeJSON([]byte(`{"x":7}`), &got); err != nil {
		t.Errorf("decode err: %v", err)
	}
}

func TestRegression_ResolveDeserRecordMapNamedKey(t *testing.T) {
	wr := MustParse(`{"type":"record","name":"R","fields":[{"name":"x","type":"int"},{"name":"y","type":"int"}]}`)
	rr := MustParse(`{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`)
	resolved := mustResolve(t, wr, rr)
	enc := mustAppendEncode(t, wr, nil, map[string]int32{"x": 1, "y": 2})
	var got map[myKey]int32
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("resolve+decode panicked: %v", r)
		}
	}()
	if _, err := resolved.Decode(enc, &got); err != nil {
		t.Errorf("decode err: %v", err)
	}
}

// TestRegression_DecodeJSONDecimalRecordFloatField — the typical user
// shape: a record with a float-typed field backed by a decimal
// logical type. The struct-decode path delegates to the same decimal
// arm, so binary↔JSON parity for the leaf flows up to records.
func TestRegression_DecodeJSONDecimalRecordFloatField(t *testing.T) {
	type R struct {
		V float64 `avro:"v"`
	}
	s := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"v","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}}
	]}`)
	in := R{V: 0.5}
	binEnc := mustAppendEncode(t, s, nil, in)
	var binOut R
	if _, err := s.Decode(binEnc, &binOut); err != nil || binOut.V != 0.5 {
		t.Fatalf("binary: V=%v err=%v", binOut.V, err)
	}
	jsonEnc := mustAppendEncodeJSON(t, s, nil, in)
	var jsonOut R
	if err := s.DecodeJSON(jsonEnc, &jsonOut); err != nil {
		t.Fatalf("JSON: %v", err)
	}
	if jsonOut.V != 0.5 {
		t.Fatalf("json: got %v want 0.5", jsonOut.V)
	}
}

// TestRegression_UnionDefaultStringMatchesStringBranchNotFloat locks that a
// textual union default matches the string branch first when the union contains
// both string and float in that order, matching Java/fastavro/hamba:
// coerceDefault must consult branch order, so a string default against
// ["string","float"] picks branch 0, not the float branch via string-to-float
// coercion. Per the spec, "default values for union fields correspond to the
// first schema that matches in the union"; Java's isCompatible(STRING, TextNode)
// is true, and fastavro's and hamba's validators both pick branch 0 by
// type-equality first.
func TestRegression_UnionDefaultStringMatchesStringBranchNotFloat(t *testing.T) {
	w := MustParse(`{"type":"record","name":"R","fields":[{"name":"keep","type":"int"}]}`)
	r := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"keep","type":"int"},
		{"name":"f","type":["string","float"],"default":"3.14"}
	]}`)
	src := mustAppendEncode(t, w, nil, map[string]any{"keep": int32(7)})
	res := mustResolve(t, w, r)
	var out map[string]any
	mustDecode(t, res, src, &out)
	got, ok := out["f"].(string)
	if !ok || got != "3.14" {
		t.Fatalf("expected reader default to decode as string %q, got %T(%v)", "3.14", out["f"], out["f"])
	}
}

// TestRegression_MapDecodeBucketAmplificationDoS pins that an Avro map<V> decode
// does not pre-allocate bucket storage proportional to an attacker-declared
// count. Two layered defenses: the block-count bound uses minMapEntryBytes like
// the array path's minItemBytes, since a bare `count > len(src)` admits
// zero-byte-entry inflation, and the MakeMapWithSize hint is capped at
// maxMapPreAllocSize. Without them, a 4 MB input declaring 2M empty-key entries
// collapses to one decoded entry while heap-allocating ~160 MB of bucket
// overhead. fastavro avoids it by not pre-sizing; Java has a ~4x version.
func TestRegression_MapDecodeBucketAmplificationDoS(t *testing.T) {
	s := MustParse(`{"type":"map","values":"long"}`)

	const declaredCount = int64(2_000_000)
	zigzag := uint64(declaredCount) << 1
	var blob []byte
	for zigzag >= 0x80 {
		blob = append(blob, byte(zigzag)|0x80)
		zigzag >>= 7
	}
	blob = append(blob, byte(zigzag))
	for range int(declaredCount) {
		blob = append(blob, 0, 0) // 1-byte empty key + 1-byte value 0
	}
	blob = append(blob, 0)

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	var got any
	mustDecode(t, s, blob, &got)

	runtime.GC()
	runtime.ReadMemStats(&after)
	delta := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	gotMap := got.(map[string]any)
	t.Logf("input=%dMB declared_count=%d actual_entries=%d heap_delta=%dMB amplification=%.1fx",
		len(blob)>>20, declaredCount, len(gotMap), delta>>20, float64(delta)/float64(len(blob)))

	if delta > int64(len(blob))*10 {
		t.Errorf("DoS: %d-byte input → %d-byte heap delta (%.1fx amplification)",
			len(blob), delta, float64(delta)/float64(len(blob)))
	}
}

// TestRegression_ResolveArrayPromotion_MinItemBytesBoundTooStrict locks that
// resolveArray bounds the writer's wire block-count against the WRITER's
// per-item minimum, not the reader's resolved item size — the wire was produced
// by the writer, so its minimum is what the bound must use. Using the reader's
// resolved node would reject array<int> writer → array<double> reader on a valid
// 18-byte stream of 16 small ints, because the reader's min is 8 bytes;
// resolveMap uses the same writer-min rule. Java, fastavro and avro-rs all
// decode promoted arrays with no per-block count-times-item-size check at all.
func TestRegression_ResolveArrayPromotion_MinItemBytesBoundTooStrict(t *testing.T) {
	w := MustParse(`{"type":"array","items":"int"}`)
	r := MustParse(`{"type":"array","items":"double"}`)
	src := mustAppendEncode(t, w, nil, []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	res := mustResolve(t, w, r)
	var got []float64
	mustDecode(t, res, src, &got)
	if len(got) != 16 {
		t.Fatalf("got %d elements, want 16", len(got))
	}
}

func TestRegression_ResolveArrayPromotion_LongToFloat_TooStrict(t *testing.T) {
	w := MustParse(`{"type":"array","items":"long"}`)
	r := MustParse(`{"type":"array","items":"float"}`)
	src := mustAppendEncode(t, w, nil, []int64{1, 2, 3, 4, 5, 6, 7, 8})
	res := mustResolve(t, w, r)
	var got []float32
	mustDecode(t, res, src, &got)
}

func TestRegression_ResolveArrayPromotion_LongToDouble_TooStrict(t *testing.T) {
	w := MustParse(`{"type":"array","items":"long"}`)
	r := MustParse(`{"type":"array","items":"double"}`)
	src := mustAppendEncode(t, w, nil, []int64{1, 2, 3, 4, 5, 6, 7, 8})
	res := mustResolve(t, w, r)
	var got []float64
	mustDecode(t, res, src, &got)
}

func TestRegression_ResolveArrayRecordEvolution_DefaultedField_TooStrict(t *testing.T) {
	w := MustParse(`{"type":"array","items":{
		"type":"record","name":"R",
		"fields":[{"name":"a","type":"int"}]
	}}`)
	r := MustParse(`{"type":"array","items":{
		"type":"record","name":"R",
		"fields":[
			{"name":"a","type":"int"},
			{"name":"b","type":"long","default":0},
			{"name":"c","type":"long","default":0},
			{"name":"d","type":"long","default":0}
		]
	}}`)
	type Wr struct {
		A int32 `avro:"a"`
	}
	wrecs := make([]Wr, 16)
	for i := range wrecs {
		wrecs[i].A = int32(i)
	}
	src := mustAppendEncode(t, w, nil, wrecs)
	res := mustResolve(t, w, r)
	var got []map[string]any
	mustDecode(t, res, src, &got)
}

// TestRegression_JSONDurationIgnoresNonDurationFixedSize locks that the JSON
// encoder's case "fixed" arm invokes the avro.Duration 12-byte path only when
// node.logical == "duration". Without the gate the Duration branch fires for any
// fixed node regardless of logical or size, producing a 12-byte duration
// encoding in a fixed schema declaring a different size — invalid Avro JSON that
// this library's own decoder rejects and Java/fastavro consumers reject. The
// binary path assigns serDuration only when node.logical == "duration"
// (schema.go:1665), so the gate must mirror that.
func TestRegression_JSONDurationIgnoresNonDurationFixedSize(t *testing.T) {
	s := MustParse(`{"type":"fixed","name":"F","size":8}`)
	d := Duration{Months: 1, Days: 2, Milliseconds: 3}
	if _, err := s.AppendEncodeJSON(nil, d); err == nil {
		t.Errorf("expected error encoding avro.Duration into non-duration fixed(8); binary path errors")
	}
}

// TestRegression_EncodeJSONBytesDefaultCodepointMapping locks in
// that JSON encode of a missing bytes-typed field with a string
// default emits the spec codepoint-mapped byte string. appendAvroJSON's
// case "bytes" String branch uses codepoint mapping (1 byte per
// codepoint up to U+00FF), matching the binary path's
// defaultStringToBytes. A naive []byte(v.String()) UTF-8 conversion
// would produce 2 bytes for U+00FF — wrong per spec.
func TestRegression_EncodeJSONBytesDefaultCodepointMapping(t *testing.T) {
	s := MustParse(`{"type":"record","name":"r","fields":[{"name":"a","type":"bytes","default":"ÿ"}]}`)
	out := mustEncodeJSON(t, s, map[string]any{})
	const want = `{"a":"\u00ff"}`
	if string(out) != want {
		t.Fatalf("EncodeJSON got %s, want %s", out, want)
	}
}

// TestRegression_EncodeJSONFixedDefaultCodepointMapping is the fixed
// counterpart. Same codepoint-mapping rule applies — for a fixed(size=1)
// field with a U+00FF default, the encoded byte is 0xff (1 byte). A
// UTF-8 conversion would produce 2 bytes and fail the fixed size check.
func TestRegression_EncodeJSONFixedDefaultCodepointMapping(t *testing.T) {
	s := MustParse(`{"type":"record","name":"r","fields":[{"name":"a","type":{"type":"fixed","name":"f","size":1},"default":"ÿ"}]}`)
	out := mustEncodeJSON(t, s, map[string]any{})
	const want = `{"a":"\u00ff"}`
	if string(out) != want {
		t.Fatalf("EncodeJSON got %s, want %s", out, want)
	}
}

// TestRegression_ResolveReaderUnionAmbiguousUnqualifiedNames locks that Resolve
// picks the correct full-name match when a reader union contains two named types
// with the same unqualified name in different namespaces — a configuration the
// spec explicitly permits. findMatchingBranch / namesMatch must match by FULL
// name when namespaces differ; unqualified-name match alone would let writer
// b.Foo pick reader a.Foo, typically erroring with "field has no default and is
// missing from writer" or silently yielding wrong output. Java and fastavro both
// match by full name here.
func TestRegression_ResolveReaderUnionAmbiguousUnqualifiedNames(t *testing.T) {
	w := MustParse(`{"type":"record","name":"Foo","namespace":"b","fields":[{"name":"bf","type":"int"}]}`)
	r := MustParse(`[
		{"type":"record","name":"Foo","namespace":"a","fields":[{"name":"af","type":"int"}]},
		{"type":"record","name":"Foo","namespace":"b","fields":[{"name":"bf","type":"int"}]}
	]`)
	bin := mustAppendEncode(t, w, nil, map[string]any{"bf": int32(42)})
	resolved := mustResolve(t, w, r)
	var got any
	mustDecode(t, resolved, bin, &got)
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T(%v)", got, got)
	}
	if _, hasBf := m["bf"]; !hasBf {
		t.Fatalf("decoded record lacks bf — wrong branch selected: got %v", m)
	}
}

// TestRegression_JSONEncodeNonStringKeyMap locks in that the JSON
// encoder rejects a map whose Go key kind isn't reflect.String,
// matching the binary path's serMapPreamble (ser.go) which errors
// with SemanticError{AvroType: "map"}. The JSON path checks both
// v.Kind() == reflect.Map AND the key kind — without the key check,
// a map[int]V would silently emit JSON object keys like
// "<int Value>" (invalid Avro JSON that round-trips to a wrong
// map). Avro spec: "Map keys are assumed to be strings."
func TestRegression_JSONEncodeNonStringKeyMap(t *testing.T) {
	s := MustParse(`{"type":"map","values":"long"}`)
	in := map[int]int64{1: 100}

	if _, err := s.AppendEncode(nil, in); err == nil {
		t.Fatal("binary path: expected error for map[int]int64, got nil")
	}
	out, errJSON := s.EncodeJSON(in)
	if errJSON == nil {
		t.Fatalf("JSON path: expected error for map[int]int64, got nil; output=%q", out)
	}
}

// TestMatrix_ArrayMapMultiLevelPointerElements locks in that the
// per-primitive serArray/serMap specializations accept multi-level
// pointer element types (**T, ***T), matching the unsafe fast path
// which chases arbitrarily-deep pointers via
// `pp := *(*unsafe.Pointer)(p)`. The safe specializations must
// recurse through Elem() until reaching the concrete primitive type
// — a single-level unwrap would reject **T as "cannot use *int32
// with Avro type int".
func TestMatrix_ArrayMapMultiLevelPointerElements(t *testing.T) {
	intArr := MustParse(`{"type":"array","items":"int"}`)
	longArr := MustParse(`{"type":"array","items":"long"}`)
	stringArr := MustParse(`{"type":"array","items":"string"}`)
	floatArr := MustParse(`{"type":"array","items":"float"}`)
	doubleArr := MustParse(`{"type":"array","items":"double"}`)
	boolArr := MustParse(`{"type":"array","items":"boolean"}`)
	intMap := MustParse(`{"type":"map","values":"int"}`)
	stringMap := MustParse(`{"type":"map","values":"string"}`)

	i32 := int32(7)
	i32p := &i32
	i32pp := &i32p
	i64 := int64(8)
	i64p := &i64
	i64pp := &i64p
	str := "hi"
	strp := &str
	strpp := &strp
	f32 := float32(1.5)
	f32p := &f32
	f32pp := &f32p
	f64 := float64(2.5)
	f64p := &f64
	f64pp := &f64p
	bv := true
	bvp := &bv
	bvpp := &bvp

	// **T: every primitive specialization accepts a [single-element]
	// slice and the wire matches the canonical T-element encoding.
	cases := []struct {
		name string
		s    *Schema
		vpp  any
		vd   any
	}{
		{"int/**T", intArr, []**int32{i32pp}, []int32{i32}},
		{"long/**T", longArr, []**int64{i64pp}, []int64{i64}},
		{"string/**T", stringArr, []**string{strpp}, []string{str}},
		{"float/**T", floatArr, []**float32{f32pp}, []float32{f32}},
		{"double/**T", doubleArr, []**float64{f64pp}, []float64{f64}},
		{"bool/**T", boolArr, []**bool{bvpp}, []bool{bv}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotPP, err := c.s.AppendEncode(nil, c.vpp)
			if err != nil {
				t.Fatalf("**T encode: %v", err)
			}
			wantDirect, err := c.s.AppendEncode(nil, c.vd)
			if err != nil {
				t.Fatalf("direct encode: %v", err)
			}
			if string(gotPP) != string(wantDirect) {
				t.Fatalf("wire mismatch: got %x, want %x", gotPP, wantDirect)
			}
		})
	}

	// Map: **T values must encode like direct.
	gotMapPP, err := intMap.AppendEncode(nil, map[string]**int32{"a": i32pp})
	if err != nil {
		t.Fatalf("map **int32 encode: %v", err)
	}
	wantMapDirect, err := intMap.AppendEncode(nil, map[string]int32{"a": i32})
	if err != nil {
		t.Fatalf("map int32 encode: %v", err)
	}
	if string(gotMapPP) != string(wantMapDirect) {
		t.Fatalf("map int wire mismatch: got %x, want %x", gotMapPP, wantMapDirect)
	}
	gotMapStrPP, err := stringMap.AppendEncode(nil, map[string]**string{"a": strpp})
	if err != nil {
		t.Fatalf("map **string encode: %v", err)
	}
	wantMapStrDirect, err := stringMap.AppendEncode(nil, map[string]string{"a": str})
	if err != nil {
		t.Fatalf("map string encode: %v", err)
	}
	if string(gotMapStrPP) != string(wantMapStrDirect) {
		t.Fatalf("map string wire mismatch: got %x, want %x", gotMapStrPP, wantMapStrDirect)
	}

	// Sanity: nil at any pointer level is rejected with errIndirectNil,
	// matching the existing single-level *T behavior.
	if _, err := intArr.AppendEncode(nil, []**int32{nil}); err == nil {
		t.Fatalf("expected error on nil outer pointer in []**int32")
	}
	var nilInner *int32
	if _, err := intArr.AppendEncode(nil, []**int32{&nilInner}); err == nil {
		t.Fatalf("expected error on nil inner pointer in []**int32")
	}
}

// TestRegression_ArrayLongTimeMicrosAcceptsTime locks in that
// array<long, time-micros> accepts []time.Time, matching the scalar
// serTimeMicros. The array path falls through to per-element scalar
// encoding when element type is time.Time, so this is parity with the
// scalar fix.
func TestRegression_ArrayLongTimeMicrosAcceptsTime(t *testing.T) {
	s := MustParse(`{"type":"array","items":{"type":"long","logicalType":"time-micros"}}`)
	tm := time.Date(2020, 6, 15, 14, 30, 45, 123_456_000, time.UTC)
	mustAppendEncode(t, s, nil, []time.Time{tm})
}

// TestRegression_SerTimeMicrosAcceptsTime locks in that the time-micros
// logical type accepts time.Time on encode, mirroring time-millis (and
// the timestamp-* variants). The time-of-day component is extracted
// via timeOfDay so a time.Time input produces the same wire as the
// equivalent time.Duration. Binary and JSON paths both checked.
func TestRegression_SerTimeMicrosAcceptsTime(t *testing.T) {
	s := MustParse(`{"type":"long","logicalType":"time-micros"}`)
	// 14:30:45.123456 — non-zero in every field.
	tm := time.Date(2020, 6, 15, 14, 30, 45, 123_456_000, time.UTC)
	d := time.Duration(tm.Hour())*time.Hour + time.Duration(tm.Minute())*time.Minute + time.Duration(tm.Second())*time.Second + time.Duration(tm.Nanosecond())
	// Binary parity with time.Duration input.
	gotBin, err := s.AppendEncode(nil, tm)
	if err != nil {
		t.Fatalf("AppendEncode time.Time: %v", err)
	}
	wantBin, err := s.AppendEncode(nil, d)
	if err != nil {
		t.Fatalf("AppendEncode time.Duration: %v", err)
	}
	if string(gotBin) != string(wantBin) {
		t.Fatalf("binary wire: got %x, want %x", gotBin, wantBin)
	}
	// JSON parity.
	gotJSON, err := s.EncodeJSON(tm)
	if err != nil {
		t.Fatalf("EncodeJSON time.Time: %v", err)
	}
	wantJSON, err := s.EncodeJSON(d)
	if err != nil {
		t.Fatalf("EncodeJSON time.Duration: %v", err)
	}
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("json wire: got %s, want %s", gotJSON, wantJSON)
	}
}

// TestMatrix_FloatDecodeIntegerOverflowBoundary pins that decoding a
// float/double wire value into an integer Go target performs its out-of-range
// check in float space, like the encode-side floatFitsInt64, not via the
// platform-dependent int64(f) round trip. Go's float-to-int conversion is
// implementation-defined on overflow, so `f != float64(int64(f))` gives
// different answers per platform for |f| >= 2^63: on arm64 it silently accepts
// the double 2^63 and stores int64(2^63-1), an off-by-one corruption of a value
// that must be rejected, exactly as the encode side rejects float64(2^63)
// against a long schema.
func TestMatrix_FloatDecodeIntegerOverflowBoundary(t *testing.T) {
	doubleWire := func(f float64) []byte {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, math.Float64bits(f))
		return b
	}
	floatWire := func(f float32) []byte {
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, math.Float32bits(f))
		return b
	}
	twoTo63 := float64(uint64(1) << 63) // 2^63 = MaxInt64+1, exact in float32 and float64

	dbl := MustParse(`"double"`)
	flt := MustParse(`"float"`)

	t.Run("binary double 2^63 into int64 rejects", func(t *testing.T) {
		var n int64
		if _, err := dbl.Decode(doubleWire(twoTo63), &n); err == nil {
			t.Fatalf("want reject (out of int64 range), got n=%d", n)
		}
	})
	t.Run("binary float 2^63 into int64 rejects", func(t *testing.T) {
		var n int64
		if _, err := flt.Decode(floatWire(float32(twoTo63)), &n); err == nil {
			t.Fatalf("want reject (out of int64 range), got n=%d", n)
		}
	})
	t.Run("json double 2^63 into int64 rejects", func(t *testing.T) {
		var n int64
		if err := dbl.DecodeJSON([]byte("9223372036854775808"), &n); err == nil {
			t.Fatalf("want reject (out of int64 range), got n=%d", n)
		}
	})
	t.Run("binary double 2^63 into uint64 yields 2^63", func(t *testing.T) {
		// 2^63 is a valid uint64; the encode side accepts uint64(2^63)->double,
		// so decode must round-trip it (never the silent off-by-one 2^63-1).
		var u uint64
		if _, err := dbl.Decode(doubleWire(twoTo63), &u); err != nil {
			t.Fatalf("want accept, got err=%v", err)
		}
		if u != uint64(1)<<63 {
			t.Fatalf("got %d, want %d", u, uint64(1)<<63)
		}
	})
	t.Run("encode-decode symmetry at 2^63", func(t *testing.T) {
		// The encode side already rejects float64(2^63) against an exact int
		// schema (floatFitsInt64, float-space bound) and accepts it against a
		// lossy double schema. Decode must mirror that boundary: reject into
		// int64, accept into double. Locking both directions documents the
		// encode<->decode parity at the exact MaxInt64+1 boundary.
		long := MustParse(`"long"`)
		if _, err := long.Encode(twoTo63); err == nil {
			t.Fatalf("encode float64(2^63) into long: want reject")
		}
		if _, err := dbl.Encode(twoTo63); err != nil {
			t.Fatalf("encode float64(2^63) into double: want accept, got %v", err)
		}
	})
}

// TestMatrix_FloatDecodeIntegerTargetMatrix exercises the float/double ->
// integer-target decode arm (setFloatValue) across every integer width and at
// each type's overflow boundary, including the int64/uint64 boundary the
// platform-dependent conversion hid. Boundary-minus-one values must still
// decode; boundary-plus-one must reject; non-whole and non-finite values must
// reject for every integer target. The full uint64 range [0, 2^64) is decodable
// (the int64 intermediate previously could not represent [2^63, 2^64)).
func TestMatrix_FloatDecodeIntegerTargetMatrix(t *testing.T) {
	doubleWire := func(f float64) []byte {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, math.Float64bits(f))
		return b
	}
	dbl := MustParse(`"double"`)

	checkS := func(name string, target any, f float64, wantOK bool, want int64) {
		t.Run(name, func(t *testing.T) {
			_, err := dbl.Decode(doubleWire(f), target)
			ev := reflect.ValueOf(target).Elem()
			if !wantOK {
				if err == nil {
					t.Fatalf("f=%g into %s: want reject, got %v", f, ev.Type(), ev)
				}
				return
			}
			if err != nil {
				t.Fatalf("f=%g into %s: want accept, got err=%v", f, ev.Type(), err)
			}
			if ev.Int() != want {
				t.Fatalf("f=%g into %s: got %d, want %d", f, ev.Type(), ev.Int(), want)
			}
		})
	}
	checkU := func(name string, target any, f float64, wantOK bool, want uint64) {
		t.Run(name, func(t *testing.T) {
			_, err := dbl.Decode(doubleWire(f), target)
			ev := reflect.ValueOf(target).Elem()
			if !wantOK {
				if err == nil {
					t.Fatalf("f=%g into %s: want reject, got %v", f, ev.Type(), ev)
				}
				return
			}
			if err != nil {
				t.Fatalf("f=%g into %s: want accept, got err=%v", f, ev.Type(), err)
			}
			if ev.Uint() != want {
				t.Fatalf("f=%g into %s: got %d, want %d", f, ev.Type(), ev.Uint(), want)
			}
		})
	}

	// Signed widths: boundary, boundary+1 (reject), -boundary, -boundary-1 (reject).
	checkS("int8 max", new(int8), 127, true, 127)
	checkS("int8 max+1", new(int8), 128, false, 0)
	checkS("int8 min", new(int8), -128, true, -128)
	checkS("int8 min-1", new(int8), -129, false, 0)
	checkS("int16 max", new(int16), 32767, true, 32767)
	checkS("int16 max+1", new(int16), 32768, false, 0)
	checkS("int16 min-1", new(int16), -32769, false, 0)
	checkS("int32 max", new(int32), 2147483647, true, 2147483647)
	checkS("int32 max+1", new(int32), 2147483648, false, 0)
	checkS("int32 min-1", new(int32), -2147483649, false, 0)
	checkS("int64 2^53", new(int64), 1<<53, true, 1<<53)
	checkS("int64 2^62", new(int64), 1<<62, true, 1<<62)
	checkS("int64 MinInt64", new(int64), float64(math.MinInt64), true, math.MinInt64)
	checkS("int64 MaxInt64+1 (2^63)", new(int64), float64(uint64(1)<<63), false, 0)
	checkS("int (platform) 2^63 rejects", new(int), float64(uint64(1)<<63), false, 0)

	// Unsigned widths.
	checkU("uint8 max", new(uint8), 255, true, 255)
	checkU("uint8 max+1", new(uint8), 256, false, 0)
	checkU("uint8 neg", new(uint8), -1, false, 0)
	checkU("uint8 non-whole", new(uint8), 2.5, false, 0)
	checkU("uint16 max", new(uint16), 65535, true, 65535)
	checkU("uint16 max+1", new(uint16), 65536, false, 0)
	checkU("uint32 max", new(uint32), 4294967295, true, 4294967295)
	checkU("uint32 max+1", new(uint32), 4294967296, false, 0)
	checkU("uint64 2^63", new(uint64), float64(uint64(1)<<63), true, uint64(1)<<63)
	checkU("uint64 2^63+2^53", new(uint64), float64(uint64(1)<<63+uint64(1)<<53), true, uint64(1)<<63+uint64(1)<<53)
	checkU("uint64 2^64 overflow", new(uint64), float64(uint64(1)<<63)*2, false, 0)
	checkU("uint64 neg", new(uint64), -1, false, 0)

	// Non-whole / non-finite into integer targets reject regardless of width.
	checkS("int64 non-whole", new(int64), 1.5, false, 0)
	checkS("int64 NaN", new(int64), math.NaN(), false, 0)
	checkS("int64 +Inf", new(int64), math.Inf(1), false, 0)
	checkS("int64 -Inf", new(int64), math.Inf(-1), false, 0)
	checkU("uint64 NaN", new(uint64), math.NaN(), false, 0)
	checkU("uint64 +Inf", new(uint64), math.Inf(1), false, 0)
}

// ---------- resolve_test.go ----------

func TestResolveIdenticalSchemas(t *testing.T) {
	s := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`)
	resolved := mustResolve(t, s, s)
	if resolved != s {
		t.Fatal("expected identical schemas to return reader directly")
	}
}

func TestResolveFieldAddedWithDefault(t *testing.T) {
	writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	reader := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string","default":"hello"}]}`)
	resolved := mustResolve(t, writer, reader)

	// Encode with writer schema.
	encoded := mustEncode(t, writer, map[string]any{"a": 42})

	// Decode with resolved schema into interface.
	var result any
	mustDecode(t, resolved, encoded, &result)
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", result)
	}
	if m["a"] != int32(42) {
		t.Fatalf("expected a=42, got %v (%T)", m["a"], m["a"])
	}
	if m["b"] != "hello" {
		t.Fatalf("expected b=hello, got %v", m["b"])
	}
}

func TestResolveFieldRemoved(t *testing.T) {
	writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`)
	reader := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	resolved := mustResolve(t, writer, reader)

	encoded := mustEncode(t, writer, map[string]any{"a": 42, "b": "drop me"})

	var result any
	mustDecode(t, resolved, encoded, &result)
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", result)
	}
	if m["a"] != int32(42) {
		t.Fatalf("expected a=42, got %v", m["a"])
	}
	if _, exists := m["b"]; exists {
		t.Fatal("expected field b to be absent")
	}
}

func TestResolveFieldRenamedViaAlias(t *testing.T) {
	writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"old_name","type":"int"}]}`)
	reader := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"new_name","type":"int","aliases":["old_name"]}]}`)
	resolved := mustResolve(t, writer, reader)

	encoded := mustEncode(t, writer, map[string]any{"old_name": 99})

	var result any
	mustDecode(t, resolved, encoded, &result)
	m := result.(map[string]any)
	if m["new_name"] != int32(99) {
		t.Fatalf("expected new_name=99, got %v", m["new_name"])
	}
}

func TestResolveTypePromotion(t *testing.T) {
	tests := []struct {
		name       string
		writerType string
		readerType string
		writerVal  any
		expectVal  any
	}{
		{"int to long", `"int"`, `"long"`, int32(42), int64(42)},
		{"int to float", `"int"`, `"float"`, int32(7), float32(7)},
		{"int to double", `"int"`, `"double"`, int32(42), float64(42)},
		{"long to float", `"long"`, `"float"`, int64(9), float32(9)},
		{"long to double", `"long"`, `"double"`, int64(100), float64(100)},
		{"float to double", `"float"`, `"double"`, float32(1.5), float64(float32(1.5))},
		{"string to bytes", `"string"`, `"bytes"`, "abc", []byte("abc")},
		{"bytes to string", `"bytes"`, `"string"`, []byte("xyz"), "xyz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer := mustParse(t, tt.writerType)
			reader := mustParse(t, tt.readerType)
			resolved := mustResolve(t, writer, reader)
			encoded := mustEncode(t, writer, tt.writerVal)
			var result any
			mustDecode(t, resolved, encoded, &result)
			if !reflect.DeepEqual(result, tt.expectVal) {
				t.Fatalf("expected %v (%T), got %v (%T)", tt.expectVal, tt.expectVal, result, result)
			}
		})
	}
}

func TestResolveEnumEvolution(t *testing.T) {
	writer := mustParse(t, `{"type":"enum","name":"E","symbols":["A","B","C"]}`)
	reader := mustParse(t, `{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}`)
	resolved := mustResolve(t, writer, reader)

	// Encode "C" with writer.
	encoded := mustEncode(t, writer, "C")

	var result string
	mustDecode(t, resolved, encoded, &result)
	if result != "A" {
		t.Fatalf("expected default A, got %s", result)
	}

	// Known symbol should pass through.
	encoded = mustEncode(t, writer, "B")
	mustDecode(t, resolved, encoded, &result)
	if result != "B" {
		t.Fatalf("expected B, got %s", result)
	}
}

func TestResolveNestedRecords(t *testing.T) {
	writer := mustParse(t, nestedInnerSchema)
	reader := mustParse(t, `{
		"type":"record","name":"Outer","fields":[
			{"name":"inner","type":{
				"type":"record","name":"Inner","fields":[
					{"name":"x","type":"long"},
					{"name":"y","type":"string","default":"default_y"}
				]
			}}
		]
	}`)
	resolved := mustResolve(t, writer, reader)

	encoded := mustEncode(t, writer, map[string]any{
		"inner": map[string]any{"x": 10},
	})

	var result any
	mustDecode(t, resolved, encoded, &result)
	outer := result.(map[string]any)
	inner := outer["inner"].(map[string]any)
	if inner["x"] != int64(10) {
		t.Fatalf("expected x=10 (int64), got %v (%T)", inner["x"], inner["x"])
	}
	if inner["y"] != "default_y" {
		t.Fatalf("expected y=default_y, got %v", inner["y"])
	}
}

func TestResolveUnionEvolution(t *testing.T) {
	writer := mustParse(t, `["null","int"]`)
	reader := mustParse(t, `["null","long"]`)
	resolved := mustResolve(t, writer, reader)

	// Encode int value 42 (index 1 in writer union) using a pointer.
	val := int32(42)
	encoded := mustEncode(t, writer, &val)

	var result *int64
	mustDecode(t, resolved, encoded, &result)
	if result == nil || *result != 42 {
		t.Fatalf("expected *int64(42), got %v", result)
	}

	// Encode null.
	encoded = mustEncode(t, writer, (*int32)(nil))
	mustDecode(t, resolved, encoded, &result)
	if result != nil {
		t.Fatalf("expected nil, got %v", *result)
	}
}

func TestResolveSelfReferencingRecord(t *testing.T) {
	schema := nodeRecursiveSchema
	writer := mustParse(t, schema)
	reader := mustParse(t, schema)
	resolved := mustResolve(t, writer, reader)

	encoded := mustEncode(t, writer, map[string]any{
		"value": 1,
		"next": map[string]any{
			"value": 2,
			"next":  nil,
		},
	})

	var result any
	mustDecode(t, resolved, encoded, &result)
	root := result.(map[string]any)
	if root["value"] != int32(1) {
		t.Fatalf("expected root value=1, got %v", root["value"])
	}
	next := root["next"].(map[string]any)
	if next["value"] != int32(2) {
		t.Fatalf("expected next value=2, got %v", next["value"])
	}
}

func TestResolveDecodeIntoStruct(t *testing.T) {
	writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`)
	reader := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"},{"name":"c","type":"double","default":3.14}]}`)
	resolved := mustResolve(t, writer, reader)

	encoded := mustEncode(t, writer, map[string]any{"a": 42, "b": "hello"})

	type Result struct {
		A int64   `avro:"a"`
		B string  `avro:"b"`
		C float64 `avro:"c"`
	}
	var result Result
	mustDecode(t, resolved, encoded, &result)
	if result.A != 42 {
		t.Fatalf("expected A=42, got %d", result.A)
	}
	if result.B != "hello" {
		t.Fatalf("expected B=hello, got %s", result.B)
	}
	if result.C != 3.14 {
		t.Fatalf("expected C=3.14, got %f", result.C)
	}
}

func TestResolveDecodeIntoMap(t *testing.T) {
	writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	reader := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"int","default":99}]}`)
	resolved := mustResolve(t, writer, reader)

	encoded := mustEncode(t, writer, map[string]any{"a": 10})

	result := make(map[string]int32)
	mustDecode(t, resolved, encoded, &result)
	if result["a"] != 10 {
		t.Fatalf("expected a=10, got %d", result["a"])
	}
	if result["b"] != 99 {
		t.Fatalf("expected b=99, got %d", result["b"])
	}
}

func TestResolveArrayEvolution(t *testing.T) {
	writer := mustParse(t, `{"type":"array","items":"int"}`)
	reader := mustParse(t, `{"type":"array","items":"long"}`)
	resolved := mustResolve(t, writer, reader)

	encoded := mustEncode(t, writer, []int32{1, 2, 3})

	var result []int64
	mustDecode(t, resolved, encoded, &result)
	if len(result) != 3 || result[0] != 1 || result[1] != 2 || result[2] != 3 {
		t.Fatalf("unexpected result: %v", result)
	}
}

func TestResolveMapEvolution(t *testing.T) {
	writer := mustParse(t, `{"type":"map","values":"float"}`)
	reader := mustParse(t, `{"type":"map","values":"double"}`)
	resolved := mustResolve(t, writer, reader)

	encoded := mustEncode(t, writer, map[string]float32{"x": 1.5})

	var result map[string]float64
	mustDecode(t, resolved, encoded, &result)
	expected := float64(float32(1.5))
	if math.Abs(result["x"]-expected) > 1e-10 {
		t.Fatalf("expected x=%v, got %v", expected, result["x"])
	}
}

func TestEncodeDefault(t *testing.T) {
	tests := []struct {
		name    string
		val     any
		schema  string
		checkFn func(t *testing.T, encoded []byte)
	}{
		{
			name:   "null",
			val:    nil,
			schema: `"null"`,
			checkFn: func(t *testing.T, encoded []byte) {
				if len(encoded) != 0 {
					t.Fatalf("expected empty, got %v", encoded)
				}
			},
		},
		{
			name:   "boolean true",
			val:    true,
			schema: `"boolean"`,
			checkFn: func(t *testing.T, encoded []byte) {
				if len(encoded) != 1 || encoded[0] != 1 {
					t.Fatalf("expected [1], got %v", encoded)
				}
			},
		},
		{
			name:   "int",
			val:    float64(42),
			schema: `"int"`,
			checkFn: func(t *testing.T, encoded []byte) {
				s := mustParse(t, `"int"`)
				var v int32
				mustDecode(t, s, encoded, &v)
				if v != 42 {
					t.Fatalf("expected 42, got %d", v)
				}
			},
		},
		{
			name:   "string",
			val:    "hello",
			schema: `"string"`,
			checkFn: func(t *testing.T, encoded []byte) {
				s := mustParse(t, `"string"`)
				var v string
				mustDecode(t, s, encoded, &v)
				if v != "hello" {
					t.Fatalf("expected hello, got %s", v)
				}
			},
		},
		{
			name: "record",
			val:  map[string]any{"x": float64(10), "y": "test"},
			schema: `{"type":"record","name":"R","fields":[
				{"name":"x","type":"int"},
				{"name":"y","type":"string"}
			]}`,
			checkFn: func(t *testing.T, encoded []byte) {
				s := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"x","type":"int"},{"name":"y","type":"string"}]}`)
				var v any
				mustDecode(t, s, encoded, &v)
				m := v.(map[string]any)
				if m["x"] != int32(10) {
					t.Fatalf("expected x=10, got %v", m["x"])
				}
				if m["y"] != "test" {
					t.Fatalf("expected y=test, got %v", m["y"])
				}
			},
		},
		{
			name:   "enum",
			val:    "B",
			schema: `{"type":"enum","name":"E","symbols":["A","B","C"]}`,
			checkFn: func(t *testing.T, encoded []byte) {
				s := mustParse(t, `{"type":"enum","name":"E","symbols":["A","B","C"]}`)
				var v string
				mustDecode(t, s, encoded, &v)
				if v != "B" {
					t.Fatalf("expected B, got %s", v)
				}
			},
		},
		{
			name:   "array",
			val:    []any{float64(1), float64(2)},
			schema: `{"type":"array","items":"int"}`,
			checkFn: func(t *testing.T, encoded []byte) {
				s := mustParse(t, `{"type":"array","items":"int"}`)
				var v []int32
				mustDecode(t, s, encoded, &v)
				if len(v) != 2 || v[0] != 1 || v[1] != 2 {
					t.Fatalf("expected [1,2], got %v", v)
				}
			},
		},
		{
			name:   "union (null first branch)",
			val:    nil,
			schema: `["null","int"]`,
			checkFn: func(t *testing.T, encoded []byte) {
				s := mustParse(t, `["null","int"]`)
				var v any
				mustDecode(t, s, encoded, &v)
				if v != nil {
					t.Fatalf("expected nil, got %v", v)
				}
			},
		},
		{
			name:   "long",
			val:    float64(100000),
			schema: `"long"`,
			checkFn: func(t *testing.T, encoded []byte) {
				s := mustParse(t, `"long"`)
				var v int64
				mustDecode(t, s, encoded, &v)
				if v != 100000 {
					t.Fatalf("expected 100000, got %d", v)
				}
			},
		},
		{
			name:   "float",
			val:    float64(1.5),
			schema: `"float"`,
			checkFn: func(t *testing.T, encoded []byte) {
				s := mustParse(t, `"float"`)
				var v float32
				mustDecode(t, s, encoded, &v)
				if v != 1.5 {
					t.Fatalf("expected 1.5, got %f", v)
				}
			},
		},
		{
			name:   "double",
			val:    float64(2.718),
			schema: `"double"`,
			checkFn: func(t *testing.T, encoded []byte) {
				s := mustParse(t, `"double"`)
				var v float64
				mustDecode(t, s, encoded, &v)
				if v != 2.718 {
					t.Fatalf("expected 2.718, got %f", v)
				}
			},
		},
		{
			name:   "bytes",
			val:    "raw",
			schema: `"bytes"`,
			checkFn: func(t *testing.T, encoded []byte) {
				s := mustParse(t, `"bytes"`)
				var v []byte
				mustDecode(t, s, encoded, &v)
				if string(v) != "raw" {
					t.Fatalf("expected raw, got %s", v)
				}
			},
		},
		{
			name:   "fixed",
			val:    "abcd",
			schema: `{"type":"fixed","name":"F","size":4}`,
			checkFn: func(t *testing.T, encoded []byte) {
				s := mustParse(t, `{"type":"fixed","name":"F","size":4}`)
				var v [4]byte
				mustDecode(t, s, encoded, &v)
				if string(v[:]) != "abcd" {
					t.Fatalf("expected abcd, got %s", v[:])
				}
			},
		},
		{
			name:   "map with entries",
			val:    map[string]any{"x": float64(1), "y": float64(2)},
			schema: `{"type":"map","values":"int"}`,
			checkFn: func(t *testing.T, encoded []byte) {
				s := mustParse(t, `{"type":"map","values":"int"}`)
				var v map[string]int32
				mustDecode(t, s, encoded, &v)
				if v["x"] != 1 || v["y"] != 2 {
					t.Fatalf("expected {x:1,y:2}, got %v", v)
				}
			},
		},
		{
			name:   "boolean false",
			val:    false,
			schema: `"boolean"`,
			checkFn: func(t *testing.T, encoded []byte) {
				if len(encoded) != 1 || encoded[0] != 0 {
					t.Fatalf("expected [0], got %v", encoded)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := encodeDefault(nil, tt.val, s.node)
			if err != nil {
				t.Fatal(err)
			}
			tt.checkFn(t, encoded)
		})
	}
}

func TestSkipUnion(t *testing.T) {
	s, err := Parse(`["null","int","string"]`)
	if err != nil {
		t.Fatal(err)
	}

	// Encode an int value (index 1) via a generic union.
	// The union ser tries each branch; int should succeed.
	encoded, err := s.Encode(int32(42))
	if err != nil {
		t.Fatal(err)
	}
	sentinel := byte(0xFE)
	data := append(encoded, sentinel)

	skip := buildSkip(s.node, newMinBytesWalk())
	rem, err := skip(data, &slab{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rem) != 1 || rem[0] != sentinel {
		t.Fatalf("expected sentinel, got %v", rem)
	}
}

func TestSkipFunctions(t *testing.T) {
	// Encode various types and verify skip advances past them correctly.
	tests := []struct {
		name   string
		schema string
		val    any
	}{
		{"boolean", `"boolean"`, true},
		{"int", `"int"`, 42},
		{"long", `"long"`, int64(12345)},
		{"float", `"float"`, float32(1.5)},
		{"double", `"double"`, float64(2.5)},
		{"string", `"string"`, "hello"},
		{"bytes", `"bytes"`, []byte("world")},
		{"array", `{"type":"array","items":"int"}`, []int32{1, 2, 3}},
		{"map", `{"type":"map","values":"string"}`, map[string]string{"k": "v"}},
		{"enum", `{"type":"enum","name":"E","symbols":["A","B"]}`, "B"},
		{"fixed", `{"type":"fixed","name":"F","size":4}`, [4]byte{1, 2, 3, 4}},
		{
			"record", `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`,
			map[string]any{"a": 1, "b": "x"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := s.Encode(tt.val)
			if err != nil {
				t.Fatal(err)
			}
			sentinel := byte(0xFE)
			data := append(encoded, sentinel)

			skip := buildSkip(s.node, newMinBytesWalk())
			rem, err := skip(data, &slab{})
			if err != nil {
				t.Fatal(err)
			}
			if len(rem) != 1 || rem[0] != sentinel {
				t.Fatalf("expected sentinel byte remaining, got %v", rem)
			}
		})
	}

	// Null is zero bytes, test directly.
	t.Run("null", func(t *testing.T) {
		sentinel := byte(0xFE)
		data := []byte{sentinel}
		rem, err := skipNull(data, &slab{})
		if err != nil {
			t.Fatal(err)
		}
		if len(rem) != 1 || rem[0] != sentinel {
			t.Fatalf("expected sentinel byte remaining, got %v", rem)
		}
	})
}

func TestResolveRecordFieldReorder(t *testing.T) {
	// Writer has fields in different order than reader.
	writer := mustParse(t, recBASchema)
	reader := mustParse(t, recABSchema)
	resolved := mustResolve(t, writer, reader)

	encoded := mustEncode(t, writer, map[string]any{"b": "hello", "a": 42})

	var result any
	mustDecode(t, resolved, encoded, &result)
	m := result.(map[string]any)
	if m["a"] != int32(42) {
		t.Fatalf("expected a=42, got %v", m["a"])
	}
	if m["b"] != "hello" {
		t.Fatalf("expected b=hello, got %v", m["b"])
	}
}

func TestResolveComplexDefault(t *testing.T) {
	writer := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"x","type":"int"}
	]}`)
	reader := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"x","type":"int"},
		{"name":"tags","type":{"type":"array","items":"string"},"default":[]},
		{"name":"meta","type":{"type":"map","values":"int"},"default":{}}
	]}`)
	resolved := mustResolve(t, writer, reader)

	encoded := mustEncode(t, writer, map[string]any{"x": 1})

	var result any
	mustDecode(t, resolved, encoded, &result)
	m := result.(map[string]any)
	if m["x"] != int32(1) {
		t.Fatalf("expected x=1, got %v", m["x"])
	}
	tags := m["tags"].([]any)
	if len(tags) != 0 {
		t.Fatalf("expected empty tags, got %v", tags)
	}
	meta := m["meta"].(map[string]any)
	if len(meta) != 0 {
		t.Fatalf("expected empty meta, got %v", meta)
	}
}

func TestResolvePromotionInRecord(t *testing.T) {
	writer := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"float"}
	]}`)
	reader := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"a","type":"long"},
		{"name":"b","type":"double"}
	]}`)
	resolved := mustResolve(t, writer, reader)

	encoded := mustEncode(t, writer, map[string]any{"a": 42, "b": float32(1.5)})

	type R struct {
		A int64   `avro:"a"`
		B float64 `avro:"b"`
	}
	var result R
	mustDecode(t, resolved, encoded, &result)
	if result.A != 42 {
		t.Fatalf("expected A=42, got %d", result.A)
	}
	expected := float64(float32(1.5))
	if math.Abs(result.B-expected) > 1e-10 {
		t.Fatalf("expected B=%v, got %v", expected, result.B)
	}
}

func TestResolveRecordDefault(t *testing.T) {
	// Test default for a record field whose type is also a record.
	writer := mustParse(t, `{"type":"record","name":"Outer","fields":[
		{"name":"x","type":"int"}
	]}`)
	reader := mustParse(t, `{"type":"record","name":"Outer","fields":[
		{"name":"x","type":"int"},
		{"name":"inner","type":{"type":"record","name":"Inner","fields":[
			{"name":"a","type":"int","default":0},
			{"name":"b","type":"string","default":""}
		]},"default":{"a":10,"b":"def"}}
	]}`)
	resolved := mustResolve(t, writer, reader)

	encoded := mustEncode(t, writer, map[string]any{"x": 1})

	var result any
	mustDecode(t, resolved, encoded, &result)
	outer := result.(map[string]any)
	inner := outer["inner"].(map[string]any)
	if inner["a"] != int32(10) {
		t.Fatalf("expected inner.a=10, got %v", inner["a"])
	}
	if inner["b"] != "def" {
		t.Fatalf("expected inner.b=def, got %v", inner["b"])
	}
}

func TestResolveWriterUnionReaderNonUnion(t *testing.T) {
	// Writer is ["null","int"], reader is just "int". The null branch
	// is incompatible with the int reader, so Resolve must fail eagerly
	// (fail-fast posture; see checkWriterUnion's doc comment).
	writer := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"a","type":["null","int"]}
	]}`)
	reader := mustParse(t, recASchema)
	if _, err := Resolve(writer, reader); err == nil {
		t.Fatal("expected Resolve to fail eagerly: null branch is incompatible with int reader")
	}
}

func TestResolveWriterUnionReaderNonUnionSuccess(t *testing.T) {
	// Writer is ["int","long"], reader is "double". Both promote to double.
	writer := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"a","type":["int","long"]}
	]}`)
	reader := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"a","type":"double"}
	]}`)
	resolved := mustResolve(t, writer, reader)

	// Encode int32(7) through the writer union (will pick index 0 = int).
	encoded := mustEncode(t, writer, map[string]any{"a": int32(7)})

	var result any
	mustDecode(t, resolved, encoded, &result)
	m := result.(map[string]any)
	if m["a"] != float64(7) {
		t.Fatalf("expected a=7.0, got %v (%T)", m["a"], m["a"])
	}
}

func TestResolveReaderUnionWriterNonUnion(t *testing.T) {
	// Writer is "int", reader is ["null","long"].
	writer := mustParse(t, `"int"`)
	reader := mustParse(t, `["null","long"]`)
	resolved := mustResolve(t, writer, reader)

	encoded := mustEncode(t, writer, int32(42))

	var result any
	mustDecode(t, resolved, encoded, &result)
	if result != int64(42) {
		t.Fatalf("expected int64(42), got %v (%T)", result, result)
	}
}

func TestResolveReaderUnionTaggedUnions(t *testing.T) {
	// Writer is "string", reader is ["null","string"].
	// Schema evolution: writer non-union → reader union.
	// TaggedUnions should wrap the result.
	writer := mustParse(t, `"string"`)
	reader := mustParse(t, `["null","string"]`)
	resolved := mustResolve(t, writer, reader)

	encoded := mustEncode(t, writer, "hello")

	// Without TaggedUnions: bare value.
	var bare any
	mustDecode(t, resolved, encoded, &bare)
	if bare != "hello" {
		t.Fatalf("bare: expected \"hello\", got %v (%T)", bare, bare)
	}

	// With TaggedUnions: wrapped value.
	var tagged any
	mustDecode(t, resolved, encoded, &tagged, TaggedUnions())
	wrapper, ok := tagged.(map[string]any)
	if !ok {
		t.Fatalf("tagged: expected map wrapper, got %T: %v", tagged, tagged)
	}
	if wrapper["string"] != "hello" {
		t.Fatalf("tagged: expected {\"string\":\"hello\"}, got %v", wrapper)
	}
}

// TestResolveReaderUnionTaggedWrapTargetParity verifies that decoding
// through a resolved reader-union (writer non-union, reader union) treats
// every decode-target shape exactly like the natural union path. The
// TaggedUnions {branch: value} envelope applies only to targets that
// map[string]any is assignable to; for every other target (concrete
// types, non-empty interfaces) the wrap is skipped silently — the
// contract documented on deserUnion.maybeWrap — never turned into an
// error. The natural decode of the reader-shaped wire is the oracle for
// each cell; resolved binary and resolved JSON (which funnels through the
// same resolving deser) must agree with it.
func TestResolveReaderUnionTaggedWrapTargetParity(t *testing.T) {
	writer := mustParse(t, `{"type":"long","logicalType":"timestamp-millis"}`)
	reader := mustParse(t, `["null",{"type":"long","logicalType":"timestamp-millis"}]`)
	resolved := mustResolve(t, writer, reader)
	want := time.UnixMilli(5).UTC()
	writerWire := mustEncode(t, writer, want)
	readerWire := mustEncode(t, reader, want)

	type nanoer interface{ UnixNano() int64 } // satisfied by time.Time

	t.Run("any_untagged", func(t *testing.T) {
		var nat, res any
		mustDecode(t, reader, readerWire, &nat)
		mustDecode(t, resolved, writerWire, &res)
		if !reflect.DeepEqual(nat, res) {
			t.Fatalf("natural %#v != resolved %#v", nat, res)
		}
	})

	t.Run("any_tagged", func(t *testing.T) {
		var nat, res any
		mustDecode(t, reader, readerWire, &nat, TaggedUnions())
		mustDecode(t, resolved, writerWire, &res, TaggedUnions())
		if !reflect.DeepEqual(nat, res) {
			t.Fatalf("natural %#v != resolved %#v", nat, res)
		}
		m, ok := res.(map[string]any)
		if !ok || !m["long"].(time.Time).Equal(want) {
			t.Fatalf("expected {\"long\": %v} envelope, got %#v", want, res)
		}
	})

	t.Run("any_tagged_logical", func(t *testing.T) {
		var nat, res any
		mustDecode(t, reader, readerWire, &nat, TaggedUnions(), TagLogicalTypes())
		mustDecode(t, resolved, writerWire, &res, TaggedUnions(), TagLogicalTypes())
		if !reflect.DeepEqual(nat, res) {
			t.Fatalf("natural %#v != resolved %#v", nat, res)
		}
		m, ok := res.(map[string]any)
		if !ok || !m["long.timestamp-millis"].(time.Time).Equal(want) {
			t.Fatalf("expected {\"long.timestamp-millis\": %v} envelope, got %#v", want, res)
		}
	})

	t.Run("typed_interface_tagged", func(t *testing.T) {
		var nat, res nanoer
		mustDecode(t, reader, readerWire, &nat, TaggedUnions())
		if _, err := resolved.Decode(writerWire, &res, TaggedUnions()); err != nil {
			t.Fatalf("non-empty interface target must skip the tagged wrap silently like the natural path: %v", err)
		}
		if !nat.(time.Time).Equal(want) || !res.(time.Time).Equal(want) {
			t.Fatalf("natural %#v / resolved %#v, want bare %v", nat, res, want)
		}
	})

	t.Run("typed_interface_tagged_logical", func(t *testing.T) {
		var res nanoer
		if _, err := resolved.Decode(writerWire, &res, TaggedUnions(), TagLogicalTypes()); err != nil {
			t.Fatalf("non-empty interface target must skip the tagged wrap silently like the natural path: %v", err)
		}
		if !res.(time.Time).Equal(want) {
			t.Fatalf("resolved %#v, want bare %v", res, want)
		}
	})

	t.Run("pointer_target_tagged", func(t *testing.T) {
		var nat, res *time.Time
		mustDecode(t, reader, readerWire, &nat, TaggedUnions())
		mustDecode(t, resolved, writerWire, &res, TaggedUnions())
		if nat == nil || res == nil || !nat.Equal(want) || !res.Equal(want) {
			t.Fatalf("natural %v / resolved %v, want %v", nat, res, want)
		}
	})

	t.Run("json_typed_interface_tagged", func(t *testing.T) {
		var res nanoer
		if err := resolved.DecodeJSON([]byte(`5`), &res, TaggedUnions()); err != nil {
			t.Fatalf("resolved DecodeJSON into non-empty interface must skip the tagged wrap silently: %v", err)
		}
		if !res.(time.Time).Equal(want) {
			t.Fatalf("resolved JSON %#v, want bare %v", res, want)
		}
	})
}

// TestResolvedRecordIntoAnyMapReuseParity verifies the resolved record
// decoder honors the documented map-reuse contract for *any targets: when
// the target interface already wraps a map[string]any, schema fields are
// written into the existing map and unrelated keys are retained (see
// reuseOrMakeStringAnyMap and TestDecodeReuseAnyTargetStaleKeys, which pin
// the natural decoder's behavior). The natural decode is the oracle;
// resolved binary and resolved JSON decodes must match it.
func TestResolvedRecordIntoAnyMapReuseParity(t *testing.T) {
	writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}`)
	reader := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"long","default":7}]}`)
	resolved := mustResolve(t, writer, reader)
	writerWire := mustEncode(t, writer, map[string]any{"a": int64(1)})
	readerWire := mustEncode(t, reader, map[string]any{"a": int64(1), "b": int64(7)})
	want := map[string]any{"stale": int64(99), "a": int64(1), "b": int64(7)}

	t.Run("binary_preseeded", func(t *testing.T) {
		// "a" pre-seeded with a different value proves schema keys are
		// overwritten while unrelated keys survive.
		var nat any = map[string]any{"stale": int64(99), "a": int64(42)}
		mustDecode(t, reader, readerWire, &nat)
		var res any = map[string]any{"stale": int64(99), "a": int64(42)}
		mustDecode(t, resolved, writerWire, &res)
		if !reflect.DeepEqual(nat, res) {
			t.Fatalf("natural %#v != resolved %#v", nat, res)
		}
		if !reflect.DeepEqual(res, want) {
			t.Fatalf("resolved %#v, want %#v", res, want)
		}
	})

	t.Run("binary_fresh", func(t *testing.T) {
		var res any
		mustDecode(t, resolved, writerWire, &res)
		if !reflect.DeepEqual(res, map[string]any{"a": int64(1), "b": int64(7)}) {
			t.Fatalf("fresh decode got %#v", res)
		}
	})

	t.Run("typed_map_preseeded", func(t *testing.T) {
		nat := map[string]int64{"stale": 99}
		mustDecode(t, reader, readerWire, &nat)
		res := map[string]int64{"stale": 99}
		mustDecode(t, resolved, writerWire, &res)
		exp := map[string]int64{"stale": 99, "a": 1, "b": 7}
		if !reflect.DeepEqual(nat, exp) || !reflect.DeepEqual(res, exp) {
			t.Fatalf("natural %v / resolved %v, want %v", nat, res, exp)
		}
	})

	t.Run("json_preseeded", func(t *testing.T) {
		var res any = map[string]any{"stale": int64(99)}
		mustDecodeJSON(t, resolved, []byte(`{"a":1}`), &res)
		if !reflect.DeepEqual(res, want) {
			t.Fatalf("resolved JSON %#v, want %#v", res, want)
		}
	})
}

// --- Direct skip function error path tests ---

func TestSkipBooleanShortBuffer(t *testing.T) {
	_, err := skipBoolean(nil, &slab{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSkipFloatShortBuffer(t *testing.T) {
	_, err := skipFloat([]byte{1, 2}, &slab{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSkipDoubleShortBuffer(t *testing.T) {
	_, err := skipDouble([]byte{1, 2, 3, 4}, &slab{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSkipBytesErrors(t *testing.T) {
	// Empty buffer: readVarlong fails.
	_, err := skipBytes(nil, &slab{})
	if err == nil {
		t.Fatal("expected error for empty input")
	}

	// Short buffer after reading length.
	data := appendVarlong(nil, 100) // length=100 but no data
	_, err = skipBytes(data, &slab{})
	if err == nil {
		t.Fatal("expected error for short data")
	}
}

func TestSkipFixedShortBuffer(t *testing.T) {
	skip := skipFixed(8)
	_, err := skip([]byte{1, 2, 3}, &slab{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSkipArrayErrors(t *testing.T) {
	intNode := &schemaNode{kind: "int"}
	arrNode := &schemaNode{kind: "array", items: intNode}
	skip := buildSkip(arrNode, newMinBytesWalk())

	// Empty buffer: readVarlong fails.
	_, err := skip(nil, &slab{})
	if err == nil {
		t.Fatal("expected error for empty input")
	}

	// Negative block count with byte-size skip.
	// Encode: count=-3 (zigzag), byteSize=2, then 2 bytes of data, then 0 terminator.
	var data []byte
	data = appendVarlong(data, -3)  // negative count => abs(3) items, but skip by byte size
	data = appendVarlong(data, 2)   // byte size = 2
	data = append(data, 0x01, 0x02) // 2 bytes
	data = appendVarlong(data, 0)   // terminator
	rem, err := skip(data, &slab{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("expected empty remainder, got %d bytes", len(rem))
	}

	// Negative count byte-size exceeds available data.
	data = data[:0]
	data = appendVarlong(data, -2)
	data = appendVarlong(data, 100) // byte size > available
	_, err = skip(data, &slab{})
	if err == nil {
		t.Fatal("expected error for byte-size exceeding data")
	}

	// Negative count: error reading byte size.
	data = data[:0]
	data = appendVarlong(data, -2)
	// No byte size follows.
	_, err = skip(data, &slab{})
	if err == nil {
		t.Fatal("expected error for missing byte size")
	}

	// Positive count with item skip error (truncated int).
	data = data[:0]
	data = appendVarlong(data, 1) // 1 item
	// No int data for the item.
	_, err = skip(data, &slab{})
	if err == nil {
		t.Fatal("expected error for truncated item")
	}
}

func TestSkipMapErrors(t *testing.T) {
	intNode := &schemaNode{kind: "int"}
	mapNode := &schemaNode{kind: "map", values: intNode}
	skip := buildSkip(mapNode, newMinBytesWalk())

	// Empty buffer.
	_, err := skip(nil, &slab{})
	if err == nil {
		t.Fatal("expected error for empty input")
	}

	// Negative count with byte-size skip.
	var data []byte
	data = appendVarlong(data, -2)
	data = appendVarlong(data, 3)
	data = append(data, 0x01, 0x02, 0x03)
	data = appendVarlong(data, 0)
	rem, err := skip(data, &slab{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("expected empty remainder, got %d bytes", len(rem))
	}

	// Negative count byte-size exceeds data.
	data = data[:0]
	data = appendVarlong(data, -1)
	data = appendVarlong(data, 100)
	_, err = skip(data, &slab{})
	if err == nil {
		t.Fatal("expected error for oversized byte-size")
	}

	// Negative count: error reading byte size.
	data = data[:0]
	data = appendVarlong(data, -1)
	_, err = skip(data, &slab{})
	if err == nil {
		t.Fatal("expected error for missing byte size")
	}

	// Positive count: key skip error.
	data = data[:0]
	data = appendVarlong(data, 1)
	// No key string data.
	_, err = skip(data, &slab{})
	if err == nil {
		t.Fatal("expected error for truncated key")
	}

	// Positive count: value skip error (key ok, value truncated).
	data = data[:0]
	data = appendVarlong(data, 1)
	data = appendVarlong(data, 1) // key length=1
	data = append(data, 'k')      // key data
	// No value int data.
	_, err = skip(data, &slab{})
	if err == nil {
		t.Fatal("expected error for truncated value")
	}
}

func TestSkipUnionErrors(t *testing.T) {
	node := &schemaNode{
		kind: "union",
		branches: []*schemaNode{
			{kind: "null"},
			{kind: "int"},
		},
	}
	skip := buildSkip(node, newMinBytesWalk())

	// Empty buffer: readVarint error.
	_, err := skip(nil, &slab{})
	if err == nil {
		t.Fatal("expected error for empty input")
	}

	// Out of range index.
	data := appendVarint(nil, 5)
	_, err = skip(data, &slab{})
	if err == nil {
		t.Fatal("expected error for out-of-range index")
	}

	// Negative index.
	data = appendVarint(nil, -1)
	_, err = skip(data, &slab{})
	if err == nil {
		t.Fatal("expected error for negative index")
	}
}

func TestBuildSkipUnknownType(t *testing.T) {
	node := &schemaNode{kind: "unknown_type"}
	skip := buildSkip(node, newMinBytesWalk())
	_, err := skip([]byte{1, 2, 3}, &slab{})
	if err == nil {
		t.Fatal("expected error for unknown type")
	}
}

func TestSkipToDeser(t *testing.T) {
	deser := skipToDeser(skipBoolean)
	rem, err := deser([]byte{1, 2, 3}, reflect.Value{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rem) != 2 {
		t.Fatalf("expected 2 remaining bytes, got %d", len(rem))
	}
}

func TestSkipRecordFieldError(t *testing.T) {
	// Record with an int field, but pass truncated data.
	node := &schemaNode{
		kind: "record",
		fields: []fieldNode{
			{name: "a", node: &schemaNode{kind: "int"}},
		},
	}
	skip := buildSkip(node, newMinBytesWalk())
	_, err := skip(nil, &slab{})
	if err == nil {
		t.Fatal("expected error for truncated record field")
	}
}

// --- Direct promotion function typed-target and error path tests ---

func TestPromoteIntToLongTyped(t *testing.T) {
	data := appendVarint(nil, 42)

	// CanInt path.
	var i64 int64
	v := reflect.ValueOf(&i64).Elem()
	_, err := promoteIntToLong(data, v, nil)
	if err != nil {
		t.Fatal(err)
	}
	if i64 != 42 {
		t.Fatalf("expected 42, got %d", i64)
	}

	// CanUint path.
	var u64 uint64
	v = reflect.ValueOf(&u64).Elem()
	_, err = promoteIntToLong(data, v, nil)
	if err != nil {
		t.Fatal(err)
	}
	if u64 != 42 {
		t.Fatalf("expected 42, got %d", u64)
	}

	// SemanticError: wrong type.
	var s string
	v = reflect.ValueOf(&s).Elem()
	_, err = promoteIntToLong(data, v, nil)
	if err == nil {
		t.Fatal("expected error for string target")
	}

	// readVarint error.
	_, err = promoteIntToLong(nil, v, nil)
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestPromoteIntToFloatTyped(t *testing.T) {
	data := appendVarint(nil, 7)

	// SetFloat path.
	var f32 float32
	v := reflect.ValueOf(&f32).Elem()
	_, err := promoteIntToFloat(data, v, nil)
	if err != nil {
		t.Fatal(err)
	}
	if f32 != 7 {
		t.Fatalf("expected 7, got %f", f32)
	}

	// SemanticError.
	var s string
	v = reflect.ValueOf(&s).Elem()
	_, err = promoteIntToFloat(data, v, nil)
	if err == nil {
		t.Fatal("expected error for string target")
	}

	// readVarint error.
	_, err = promoteIntToFloat(nil, v, nil)
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestPromoteIntToDoubleTyped(t *testing.T) {
	data := appendVarint(nil, 42)

	var f64 float64
	v := reflect.ValueOf(&f64).Elem()
	_, err := promoteIntToDouble(data, v, nil)
	if err != nil {
		t.Fatal(err)
	}
	if f64 != 42 {
		t.Fatalf("expected 42, got %f", f64)
	}

	var s string
	v = reflect.ValueOf(&s).Elem()
	_, err = promoteIntToDouble(data, v, nil)
	if err == nil {
		t.Fatal("expected error for string target")
	}

	_, err = promoteIntToDouble(nil, v, nil)
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestPromoteLongToFloatTyped(t *testing.T) {
	data := appendVarlong(nil, 9)

	var f32 float32
	v := reflect.ValueOf(&f32).Elem()
	_, err := promoteLongToFloat(data, v, nil)
	if err != nil {
		t.Fatal(err)
	}
	if f32 != 9 {
		t.Fatalf("expected 9, got %f", f32)
	}

	var s string
	v = reflect.ValueOf(&s).Elem()
	_, err = promoteLongToFloat(data, v, nil)
	if err == nil {
		t.Fatal("expected error for string target")
	}

	_, err = promoteLongToFloat(nil, v, nil)
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestPromoteLongToDoubleTyped(t *testing.T) {
	data := appendVarlong(nil, 100)

	var f64 float64
	v := reflect.ValueOf(&f64).Elem()
	_, err := promoteLongToDouble(data, v, nil)
	if err != nil {
		t.Fatal(err)
	}
	if f64 != 100 {
		t.Fatalf("expected 100, got %f", f64)
	}

	var s string
	v = reflect.ValueOf(&s).Elem()
	_, err = promoteLongToDouble(data, v, nil)
	if err == nil {
		t.Fatal("expected error for string target")
	}

	_, err = promoteLongToDouble(nil, v, nil)
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestPromoteFloatToDoubleErrors(t *testing.T) {
	// readUint32 error.
	var f64 float64
	v := reflect.ValueOf(&f64).Elem()
	_, err := promoteFloatToDouble([]byte{1}, v, nil)
	if err == nil {
		t.Fatal("expected error for short buffer")
	}

	// SetFloat typed path.
	data := appendUint32(nil, math.Float32bits(2.5))
	_, err = promoteFloatToDouble(data, v, nil)
	if err != nil {
		t.Fatal(err)
	}
	if f64 != float64(float32(2.5)) {
		t.Fatalf("expected %f, got %f", float64(float32(2.5)), f64)
	}

	// SemanticError.
	var s string
	v = reflect.ValueOf(&s).Elem()
	_, err = promoteFloatToDouble(data, v, nil)
	if err == nil {
		t.Fatal("expected error for string target")
	}
}

func TestPromoteStringToBytesTyped(t *testing.T) {
	var data []byte
	data = appendVarlong(data, 3)
	data = append(data, "abc"...)

	// SetBytes slice path.
	var b []byte
	v := reflect.ValueOf(&b).Elem()
	_, err := promoteStringToBytes(data, v, &slab{})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "abc" {
		t.Fatalf("expected abc, got %s", b)
	}

	// SemanticError (wrong type).
	var i int
	v = reflect.ValueOf(&i).Elem()
	_, err = promoteStringToBytes(data, v, &slab{})
	if err == nil {
		t.Fatal("expected error for int target")
	}

	// readVarlong error.
	_, err = promoteStringToBytes(nil, v, &slab{})
	if err == nil {
		t.Fatal("expected error for empty input")
	}

	// Short buffer after length.
	short := appendVarlong(nil, 100) // length=100 but no data
	_, err = promoteStringToBytes(short, v, &slab{})
	if err == nil {
		t.Fatal("expected error for short buffer")
	}
}

func TestPromoteBytesToStringTyped(t *testing.T) {
	var data []byte
	data = appendVarlong(data, 3)
	data = append(data, "xyz"...)

	// SetString path.
	var s string
	v := reflect.ValueOf(&s).Elem()
	_, err := promoteBytesToString(data, v, &slab{})
	if err != nil {
		t.Fatal(err)
	}
	if s != "xyz" {
		t.Fatalf("expected xyz, got %s", s)
	}

	// SemanticError (wrong type).
	var i int
	v = reflect.ValueOf(&i).Elem()
	_, err = promoteBytesToString(data, v, &slab{})
	if err == nil {
		t.Fatal("expected error for int target")
	}

	// readVarlong error.
	_, err = promoteBytesToString(nil, v, &slab{})
	if err == nil {
		t.Fatal("expected error for empty input")
	}

	// Short buffer after length.
	short := appendVarlong(nil, 100)
	_, err = promoteBytesToString(short, v, &slab{})
	if err == nil {
		t.Fatal("expected error for short buffer")
	}
}

// --- Resolve edge case tests ---

func TestResolveFixedIdentity(t *testing.T) {
	writer := mustParse(t, `{"type":"fixed","name":"F","size":4}`)
	reader := mustParse(t, `{"type":"fixed","name":"F","size":4}`)
	resolved := mustResolve(t, writer, reader)
	encoded := mustEncode(t, writer, [4]byte{1, 2, 3, 4})
	var result [4]byte
	mustDecode(t, resolved, encoded, &result)
	if result != [4]byte{1, 2, 3, 4} {
		t.Fatalf("expected [1,2,3,4], got %v", result)
	}
}

func TestResolveEnumIdentity(t *testing.T) {
	// Identical enums should return reader directly (identity path).
	writer, err := Parse(`{"type":"enum","name":"E","symbols":["A","B","C"]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"enum","name":"E","symbols":["A","B","C"]}`)
	if err != nil {
		t.Fatal(err)
	}
	// Make canonical forms different so Resolve doesn't short-circuit.
	// Actually they'll be the same... so we need to use the resolveEnum directly.
	// Let's test via resolveEnum.
	resolved, err := resolveEnum(reader.node, writer.node, &resolveCtx{seen: make(map[nodePair]*schemaNode)})
	if err != nil {
		t.Fatal(err)
	}
	// Identity: should return reader.node directly.
	if resolved != reader.node {
		t.Fatal("expected identity path to return reader node")
	}
}

func TestResolveEnumDeserTyped(t *testing.T) {
	// Non-identity enum to exercise the closure branches.
	writer := mustParse(t, `{"type":"enum","name":"E","symbols":["A","B","C"]}`)
	reader := mustParse(t, `{"type":"enum","name":"E","symbols":["B","A","C"],"default":"A"}`)
	resolved := mustResolve(t, writer, reader)

	// Encode "A" (index 0 in writer).
	encoded := mustEncode(t, writer, "A")

	// Decode into string (SetString path).
	var s string
	mustDecode(t, resolved, encoded, &s)
	if s != "A" {
		t.Fatalf("expected A, got %s", s)
	}

	// Decode into int (CanInt path).
	var i int
	mustDecode(t, resolved, encoded, &i)
	// "A" maps to reader index 1 (B=0, A=1, C=2).
	if i != 1 {
		t.Fatalf("expected reader index 1, got %d", i)
	}

	// Decode into uint (CanUint path).
	var u uint
	mustDecode(t, resolved, encoded, &u)
	if u != 1 {
		t.Fatalf("expected reader index 1, got %d", u)
	}

	// Decode into incompatible type (SemanticError path).
	var f float64
	_, err := resolved.Decode(encoded, &f)
	if err == nil {
		t.Fatal("expected error for float64 target")
	}
}

func TestResolveEnumDeserErrors(t *testing.T) {
	writer := mustParse(t, `{"type":"enum","name":"E","symbols":["A","B","C"]}`)
	reader := mustParse(t, `{"type":"enum","name":"E","symbols":["B","A","C"],"default":"A"}`)
	resolved := mustResolve(t, writer, reader)

	// readVarint error: empty input.
	var s string
	_, err := resolved.Decode(nil, &s)
	if err == nil {
		t.Fatal("expected error for empty input")
	}

	// Out-of-range index.
	data := appendVarint(nil, 10)
	_, err = resolved.Decode(data, &s)
	if err == nil {
		t.Fatal("expected error for out-of-range enum index")
	}
}

func TestResolveArrayIdentity(t *testing.T) {
	// Array with same items type should return reader node (identity path).
	r := &schemaNode{kind: "int", deser: nil}
	rArr := &schemaNode{kind: "array", items: r}
	// resolveArray with same items node.
	resolved, err := resolveArray(rArr, rArr, "", &resolveCtx{seen: make(map[nodePair]*schemaNode)})
	if err != nil {
		t.Fatal(err)
	}
	if resolved != rArr {
		t.Fatal("expected identity path")
	}
}

func TestResolveMapIdentity(t *testing.T) {
	r := &schemaNode{kind: "string", deser: nil}
	rMap := &schemaNode{kind: "map", values: r}
	resolved, err := resolveMap(rMap, rMap, "", &resolveCtx{seen: make(map[nodePair]*schemaNode)})
	if err != nil {
		t.Fatal(err)
	}
	if resolved != rMap {
		t.Fatal("expected identity path")
	}
}

func TestResolveBuildDeserSemanticError(t *testing.T) {
	// buildDeser with unsupported target type (e.g. slice).
	writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	reader := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}`)
	resolved := mustResolve(t, writer, reader)

	encoded := mustEncode(t, writer, map[string]any{"a": 42})

	// Decode into a slice (unsupported target for record).
	var result []int
	_, err := resolved.Decode(encoded, &result)
	if err == nil {
		t.Fatal("expected error for slice target")
	}
}

func TestResolveSelfReferencingRecordDivergent(t *testing.T) {
	// Self-referencing record where reader and writer differ to exercise cycle detection placeholder.
	writerSchema := nodeRecursiveSchema
	readerSchema := `{
		"type":"record","name":"Node","fields":[
			{"name":"value","type":"long"},
			{"name":"next","type":["null","Node"]}
		]
	}`
	writer := mustParse(t, writerSchema)
	reader := mustParse(t, readerSchema)
	resolved := mustResolve(t, writer, reader)

	encoded := mustEncode(t, writer, map[string]any{
		"value": 1,
		"next": map[string]any{
			"value": 2,
			"next":  nil,
		},
	})

	var result any
	mustDecode(t, resolved, encoded, &result)
	root := result.(map[string]any)
	if root["value"] != int64(1) {
		t.Fatalf("expected root value=1 (int64), got %v (%T)", root["value"], root["value"])
	}
}

func TestResolveWriterUnionNullUnionOptimization(t *testing.T) {
	// Writer ["null","int"], reader "long" — exercises null-union optimization path in resolveWriterUnion.
	writer := mustParse(t, `["null","int"]`)
	reader := mustParse(t, `["null","long"]`)
	resolved := mustResolve(t, writer, reader)

	// Encode non-null int.
	val := int32(7)
	encoded := mustEncode(t, writer, &val)

	var result *int64
	mustDecode(t, resolved, encoded, &result)
	if result == nil || *result != 7 {
		t.Fatalf("expected *int64(7), got %v", result)
	}
}

func TestEncodeDefaultErrors(t *testing.T) {
	tests := []struct {
		name   string
		val    any
		schema string
	}{
		{
			name:   "boolean wrong type",
			val:    "notbool",
			schema: `"boolean"`,
		},
		{
			name:   "int wrong type",
			val:    "notnum",
			schema: `"int"`,
		},
		{
			name:   "long wrong type",
			val:    true,
			schema: `"long"`,
		},
		{
			name:   "float wrong type",
			val:    "x",
			schema: `"float"`,
		},
		{
			name:   "double wrong type",
			val:    []int{1},
			schema: `"double"`,
		},
		{
			name:   "string wrong type",
			val:    42,
			schema: `"string"`,
		},
		{
			name:   "bytes wrong type",
			val:    42,
			schema: `"bytes"`,
		},
		{
			name:   "enum wrong type",
			val:    42,
			schema: `{"type":"enum","name":"E","symbols":["A","B"]}`,
		},
		{
			name:   "enum unknown symbol",
			val:    "UNKNOWN",
			schema: `{"type":"enum","name":"E","symbols":["A","B"]}`,
		},
		{
			name:   "fixed wrong type",
			val:    42,
			schema: `{"type":"fixed","name":"F","size":4}`,
		},
		{
			name:   "fixed wrong length",
			val:    "ab",
			schema: `{"type":"fixed","name":"F","size":4}`,
		},
		{
			name:   "array wrong type",
			val:    "notarray",
			schema: `{"type":"array","items":"int"}`,
		},
		{
			name:   "map wrong type",
			val:    "notmap",
			schema: `{"type":"map","values":"int"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustParse(t, tt.schema)
			_, err := encodeDefault(nil, tt.val, s.node)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestEncodeDefaultUnsupportedType(t *testing.T) {
	// Unknown node kind.
	node := &schemaNode{kind: "unknown_kind"}
	_, err := encodeDefault(nil, nil, node)
	if err == nil {
		t.Fatal("expected error for unsupported type")
	}
}

func TestEncodeDefaultEmptyUnion(t *testing.T) {
	node := &schemaNode{kind: "union", branches: nil}
	_, err := encodeDefault(nil, nil, node)
	if err == nil {
		t.Fatal("expected error for empty union")
	}
}

func TestSkipBytesNegativeLength(t *testing.T) {
	// Encode a negative varint as the length.
	data := appendVarlong(nil, -5)
	_, err := skipBytes(data, &slab{})
	if err == nil {
		t.Fatal("expected error for negative bytes length")
	}
}

func TestPromoteStringToBytesNegativeLength(t *testing.T) {
	data := appendVarlong(nil, -1)
	var b []byte
	v := reflect.ValueOf(&b).Elem()
	_, err := promoteStringToBytes(data, v, &slab{})
	if err == nil {
		t.Fatal("expected error for negative length")
	}
}

func TestPromoteBytesToStringNegativeLength(t *testing.T) {
	data := appendVarlong(nil, -1)
	var s string
	v := reflect.ValueOf(&s).Elem()
	_, err := promoteBytesToString(data, v, &slab{})
	if err == nil {
		t.Fatal("expected error for negative length")
	}
}

func TestResolveDeserTruncatedData(t *testing.T) {
	// Set up a resolved schema where writer has fields A (kept) and B (skipped),
	// reader has fields A (promoted) and C (default).
	writer := mustParse(t, recABSchema)
	reader := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"a","type":"long"},
		{"name":"c","type":"int","default":0}
	]}`)
	resolved := mustResolve(t, writer, reader)

	// Test with truncated data (read error on first wire field).
	var result any
	_, err := resolved.Decode(nil, &result)
	if err == nil {
		t.Fatal("expected error for empty data")
	}

	// Test with data that has first field but skip of second field fails.
	data := appendVarint(nil, 42) // field a ok
	// field b (string) is missing -> skip error
	_, err = resolved.Decode(data, &result)
	if err == nil {
		t.Fatal("expected error for truncated skip data")
	}

	// Same tests decoding into map.
	resultMap := make(map[string]int64)
	_, err = resolved.Decode(nil, &resultMap)
	if err == nil {
		t.Fatal("expected error for empty data into map")
	}
	_, err = resolved.Decode(data, &resultMap)
	if err == nil {
		t.Fatal("expected error for truncated skip data into map")
	}

	// Same tests decoding into struct.
	type R struct {
		A int64 `avro:"a"`
		C int32 `avro:"c"`
	}
	var resultStruct R
	_, err = resolved.Decode(nil, &resultStruct)
	if err == nil {
		t.Fatal("expected error for empty data into struct")
	}
	_, err = resolved.Decode(data, &resultStruct)
	if err == nil {
		t.Fatal("expected error for truncated skip data into struct")
	}
}

func TestResolveDeserReadError(t *testing.T) {
	// Writer and reader both have field A but promoted (int→long).
	// Test with truncated data to trigger read error.
	writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	reader := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}`)
	resolved := mustResolve(t, writer, reader)

	// Empty data: read error.
	var result any
	_, err := resolved.Decode(nil, &result)
	if err == nil {
		t.Fatal("expected read error for empty data into interface")
	}

	m := make(map[string]int64)
	_, err = resolved.Decode(nil, &m)
	if err == nil {
		t.Fatal("expected read error for empty data into map")
	}

	type R struct {
		A int64 `avro:"a"`
	}
	var s R
	_, err = resolved.Decode(nil, &s)
	if err == nil {
		t.Fatal("expected read error for empty data into struct")
	}
}

func TestResolveDeserDefaultError(t *testing.T) {
	// Set up a resolved schema where reader has a field with a default
	// whose deser will fail due to bad encoded data. This is hard to trigger
	// normally, so we test the deserMap/deserStruct error paths for defaults
	// by using valid data that exercises the default code path.
	writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	reader := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string","default":"hello"}
	]}`)
	resolved := mustResolve(t, writer, reader)

	encoded := mustEncode(t, writer, map[string]any{"a": 1})

	// Default into map[string]int32: type mismatch (string default into int32 value).
	badMap := make(map[string]int32)
	_, err := resolved.Decode(encoded, &badMap)
	if err == nil {
		t.Fatal("expected error for string default into int32 map")
	}

	// Default into struct with wrong field type.
	type BadR struct {
		A int32 `avro:"a"`
		B int32 `avro:"b"` // wrong type for string default
	}
	var badStruct BadR
	_, err = resolved.Decode(encoded, &badStruct)
	if err == nil {
		t.Fatal("expected error for string default into int32 struct field")
	}
}

func TestResolveDeserMapNilInit(t *testing.T) {
	// Test that a nil map gets initialized during deserialization.
	writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	reader := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}`)
	resolved := mustResolve(t, writer, reader)

	encoded := mustEncode(t, writer, map[string]any{"a": 5})

	// Pass nil map pointer (v.IsNil() path).
	var m map[string]int64
	mustDecode(t, resolved, encoded, &m)
	if m["a"] != 5 {
		t.Fatalf("expected a=5, got %v", m["a"])
	}
}

func TestEncodeDefaultArrayNil(t *testing.T) {
	// Java, fastavro, and hamba all reject null as an array default
	// (Schema.java ARRAY case: `if (!defaultValue.isArray()) return
	// false;`). Accepting it lenient would make a union [Array,null]
	// with default null match the Array branch (empty-array bytes)
	// instead of falling through to the null branch.
	node := &schemaNode{kind: "array", items: &schemaNode{kind: "int"}}
	if _, err := encodeDefault(nil, nil, node); err == nil {
		t.Fatal("expected error for nil array default")
	}
}

func TestEncodeDefaultMapNil(t *testing.T) {
	// Same rationale as TestEncodeDefaultArrayNil: nil is not a map.
	node := &schemaNode{kind: "map", values: &schemaNode{kind: "int"}}
	if _, err := encodeDefault(nil, nil, node); err == nil {
		t.Fatal("expected error for nil map default")
	}
}

func TestEncodeDefaultArrayItemError(t *testing.T) {
	node := &schemaNode{kind: "array", items: &schemaNode{kind: "int"}}
	// Array with wrong-type item.
	_, err := encodeDefault(nil, []any{"not_a_number"}, node)
	if err == nil {
		t.Fatal("expected error for wrong item type in array")
	}
}

func TestEncodeDefaultMapValueError(t *testing.T) {
	node := &schemaNode{kind: "map", values: &schemaNode{kind: "int"}}
	// Map with wrong-type value.
	_, err := encodeDefault(nil, map[string]any{"k": "not_a_number"}, node)
	if err == nil {
		t.Fatal("expected error for wrong value type in map")
	}
}

func TestEncodeDefaultRecordNilVal(t *testing.T) {
	// nil is not a record. Java's isValidDefault rejects (RECORD case:
	// `if (!defaultValue.isObject()) return false;`); fastavro requires
	// isinstance(datum, Mapping); hamba returns false on type-assertion
	// failure. The previous lenient path made unions like [Record,null]
	// with default null incorrectly encode the Record branch instead of
	// the null branch.
	node := &schemaNode{
		kind: "record",
		name: "R",
		fields: []fieldNode{
			{name: "a", node: &schemaNode{kind: "int"}, defaultVal: float64(0), hasDefault: true},
		},
	}
	if _, err := encodeDefault(nil, nil, node); err == nil {
		t.Fatal("expected error for nil record default")
	}
}

func TestEncodeDefaultRecordMissingFieldNoDefault(t *testing.T) {
	node := &schemaNode{
		kind: "record",
		name: "R",
		fields: []fieldNode{
			{name: "a", node: &schemaNode{kind: "int"}, hasDefault: false},
		},
	}
	_, err := encodeDefault(nil, map[string]any{}, node)
	if err == nil {
		t.Fatal("expected error for missing field with no default")
	}
}

func TestEncodeDefaultRecordFieldSubError(t *testing.T) {
	node := &schemaNode{
		kind: "record",
		name: "R",
		fields: []fieldNode{
			{name: "a", node: &schemaNode{kind: "int"}},
		},
	}
	// Provide wrong type for field.
	_, err := encodeDefault(nil, map[string]any{"a": "not_a_number"}, node)
	if err == nil {
		t.Fatal("expected error for wrong field type")
	}
}

func TestEncodeDefaultRecordFieldDefault(t *testing.T) {
	node := &schemaNode{
		kind: "record",
		name: "R",
		fields: []fieldNode{
			{name: "a", node: &schemaNode{kind: "int"}, defaultVal: float64(42), hasDefault: true},
			{name: "b", node: &schemaNode{kind: "string"}},
		},
	}
	// Provide "b" but not "a" — "a" has a default.
	encoded, err := encodeDefault(nil, map[string]any{"b": "hello"}, node)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) == 0 {
		t.Fatal("expected non-empty encoded output")
	}
}

func TestResolveFieldRemovedIntoMap(t *testing.T) {
	// Writer has fields [a, b], reader has field [a]. Field b gets skipped.
	// Decode into map to exercise deserMap's skip-continue path.
	writer := mustParse(t, recABSchema)
	reader := mustParse(t, recASchema)
	resolved := mustResolve(t, writer, reader)

	encoded := mustEncode(t, writer, map[string]any{"a": 42, "b": "drop me"})

	m := make(map[string]int32)
	mustDecode(t, resolved, encoded, &m)
	if m["a"] != 42 {
		t.Fatalf("expected a=42, got %v", m["a"])
	}
}

func TestResolveFieldRemovedIntoStruct(t *testing.T) {
	// Same setup but decode into struct to exercise deserStruct skip-continue path.
	writer := mustParse(t, recABSchema)
	reader := mustParse(t, recASchema)
	resolved := mustResolve(t, writer, reader)

	encoded := mustEncode(t, writer, map[string]any{"a": 42, "b": "drop me"})

	type R struct {
		A int32 `avro:"a"`
	}
	var result R
	mustDecode(t, resolved, encoded, &result)
	if result.A != 42 {
		t.Fatalf("expected A=42, got %d", result.A)
	}
}

func TestResolveEnumDeserInterface(t *testing.T) {
	// Non-identity enum decoded into interface.
	writer := mustParse(t, `{"type":"enum","name":"E","symbols":["A","B","C"]}`)
	reader := mustParse(t, `{"type":"enum","name":"E","symbols":["B","A","C"],"default":"A"}`)
	resolved := mustResolve(t, writer, reader)

	encoded := mustEncode(t, writer, "B")

	var result any
	mustDecode(t, resolved, encoded, &result)
	if result != "B" {
		t.Fatalf("expected B, got %v", result)
	}
}

func TestSkipArrayMinInt64(t *testing.T) {
	// Craft a varint encoding of math.MinInt64 as a zigzag-encoded block count.
	// zigzag(math.MinInt64) = math.MaxUint64 (a 10-byte varint).
	// After reading as negative and negating, -math.MinInt64 overflows to math.MinInt64 (still negative).
	intNode := &schemaNode{kind: "int"}
	arrNode := &schemaNode{kind: "array", items: intNode}
	skip := buildSkip(arrNode, newMinBytesWalk())

	// math.MinInt64 zigzag-encoded as varint.
	data := appendVarlong(nil, math.MinInt64)
	_, err := skip(data, &slab{})
	if err == nil {
		t.Fatal("expected error for math.MinInt64 block count")
	}
}

func TestSkipMapMinInt64(t *testing.T) {
	intNode := &schemaNode{kind: "int"}
	mapNode := &schemaNode{kind: "map", values: intNode}
	skip := buildSkip(mapNode, newMinBytesWalk())

	data := appendVarlong(nil, math.MinInt64)
	_, err := skip(data, &slab{})
	if err == nil {
		t.Fatal("expected error for math.MinInt64 block count")
	}
}

func TestDoResolveFixed(t *testing.T) {
	// Call doResolve directly for fixed schemas (unreachable through Resolve
	// because fixed schemas with same name/size have identical canonical forms).
	r := &schemaNode{kind: "fixed", name: "F", size: 4}
	w := &schemaNode{kind: "fixed", name: "F", size: 4}
	resolved, err := doResolve(r, w, "", &resolveCtx{seen: make(map[nodePair]*schemaNode)})
	if err != nil {
		t.Fatal(err)
	}
	if resolved != r {
		t.Fatal("expected reader node returned directly for fixed")
	}
}

func TestDoResolveIncompatible(t *testing.T) {
	// Call doResolve directly for incompatible types.
	r := &schemaNode{kind: "int"}
	w := &schemaNode{kind: "string"}
	_, err := doResolve(r, w, "", &resolveCtx{seen: make(map[nodePair]*schemaNode)})
	if err == nil {
		t.Fatal("expected error for incompatible types")
	}
}

func TestResolveNodeError(t *testing.T) {
	// Call resolveNode directly to trigger the doResolve error path.
	r := &schemaNode{kind: "int"}
	w := &schemaNode{kind: "boolean"}
	_, err := resolveNode(r, w, "", &resolveCtx{seen: make(map[nodePair]*schemaNode)})
	if err == nil {
		t.Fatal("expected error for incompatible types")
	}
}

func TestResolveRecordFieldError(t *testing.T) {
	// Call resolveRecord directly with incompatible field types.
	r := &schemaNode{
		kind: "record", name: "R",
		fields: []fieldNode{
			{name: "a", node: &schemaNode{kind: "int"}},
		},
	}
	w := &schemaNode{
		kind: "record", name: "R",
		fields: []fieldNode{
			{name: "a", node: &schemaNode{kind: "boolean"}},
		},
	}
	_, err := resolveRecord(r, w, "", &resolveCtx{seen: make(map[nodePair]*schemaNode)})
	if err == nil {
		t.Fatal("expected error for incompatible field types")
	}
}

func TestResolveRecordDefaultError(t *testing.T) {
	// Call resolveRecord directly with a field that has a bad default.
	r := &schemaNode{
		kind: "record", name: "R",
		fields: []fieldNode{
			{name: "a", node: &schemaNode{kind: "int"}, defaultVal: "not_a_number", hasDefault: true},
		},
	}
	w := &schemaNode{
		kind: "record", name: "R",
		fields: nil,
	}
	_, err := resolveRecord(r, w, "", &resolveCtx{seen: make(map[nodePair]*schemaNode)})
	if err == nil {
		t.Fatal("expected error for bad default value")
	}
}

func TestResolveEnumNoDefault(t *testing.T) {
	// Call resolveEnum directly: writer has symbol not in reader, no default.
	r := &schemaNode{kind: "enum", name: "E", symbols: []string{"A", "B"}}
	w := &schemaNode{kind: "enum", name: "E", symbols: []string{"A", "B", "C"}}
	_, err := resolveEnum(r, w, &resolveCtx{seen: make(map[nodePair]*schemaNode)})
	if err == nil {
		t.Fatal("expected error for writer symbol not in reader")
	}
}

func TestResolveEnumBadDefault(t *testing.T) {
	// Call resolveEnum directly: reader has default not in its own symbols.
	r := &schemaNode{kind: "enum", name: "E", symbols: []string{"A", "B"}, enumDef: "MISSING", hasEnumDef: true}
	w := &schemaNode{kind: "enum", name: "E", symbols: []string{"A", "B", "C"}}
	_, err := resolveEnum(r, w, &resolveCtx{seen: make(map[nodePair]*schemaNode)})
	if err == nil {
		t.Fatal("expected error for invalid enum default")
	}
}

func TestResolveArrayError(t *testing.T) {
	// Call resolveArray directly with incompatible items.
	r := &schemaNode{kind: "array", items: &schemaNode{kind: "int"}}
	w := &schemaNode{kind: "array", items: &schemaNode{kind: "boolean"}}
	_, err := resolveArray(r, w, "", &resolveCtx{seen: make(map[nodePair]*schemaNode)})
	if err == nil {
		t.Fatal("expected error for incompatible array items")
	}
}

func TestResolveMapError(t *testing.T) {
	// Call resolveMap directly with incompatible values.
	r := &schemaNode{kind: "map", values: &schemaNode{kind: "int"}}
	w := &schemaNode{kind: "map", values: &schemaNode{kind: "boolean"}}
	_, err := resolveMap(r, w, "", &resolveCtx{seen: make(map[nodePair]*schemaNode)})
	if err == nil {
		t.Fatal("expected error for incompatible map values")
	}
}

func TestResolveWriterUnionError(t *testing.T) {
	// Call resolveWriterUnion directly: any incompatible branch causes
	// Resolve to fail eagerly (fail-fast posture; see resolveWriterUnion
	// doc comment).
	r := &schemaNode{kind: "int"}
	w := &schemaNode{kind: "union", branches: []*schemaNode{
		{kind: "boolean"},
	}}
	_, err := resolveWriterUnion(r, w, "", &resolveCtx{seen: make(map[nodePair]*schemaNode)})
	if err == nil {
		t.Fatal("expected error when a writer branch is incompatible")
	}
}

func TestResolveReaderUnionError(t *testing.T) {
	// Call resolveReaderUnion directly: no matching branch.
	r := &schemaNode{kind: "union", branches: []*schemaNode{
		{kind: "null"},
		{kind: "int"},
	}}
	w := &schemaNode{kind: "boolean"}
	_, err := resolveReaderUnion(r, w, "", &resolveCtx{seen: make(map[nodePair]*schemaNode)})
	if err == nil {
		t.Fatal("expected error for no matching reader union branch")
	}
}

func TestResolveReaderUnionBranchError(t *testing.T) {
	// Call resolveReaderUnion directly: matching branch found but resolveNode fails.
	r := &schemaNode{kind: "union", branches: []*schemaNode{
		{kind: "record", name: "R", fields: []fieldNode{
			{name: "a", node: &schemaNode{kind: "int"}},
		}},
	}}
	w := &schemaNode{kind: "record", name: "R", fields: []fieldNode{
		{name: "a", node: &schemaNode{kind: "boolean"}},
	}}
	_, err := resolveReaderUnion(r, w, "", &resolveCtx{seen: make(map[nodePair]*schemaNode)})
	if err == nil {
		t.Fatal("expected error for incompatible record in reader union")
	}
}

func TestResolveUnionUnionNoMatch(t *testing.T) {
	// Call resolveUnionUnion directly: writer branch has no match in reader.
	r := &schemaNode{kind: "union", branches: []*schemaNode{
		{kind: "null"},
		{kind: "int"},
	}}
	w := &schemaNode{kind: "union", branches: []*schemaNode{
		{kind: "boolean"},
	}}
	_, err := resolveUnionUnion(r, w, "", &resolveCtx{seen: make(map[nodePair]*schemaNode)})
	if err == nil {
		t.Fatal("expected error for unmatched writer union branch")
	}
}

func TestResolveUnionUnionBranchError(t *testing.T) {
	// Call resolveUnionUnion directly: branch matches by kind but resolveNode fails.
	r := &schemaNode{kind: "union", branches: []*schemaNode{
		{kind: "record", name: "R", fields: []fieldNode{
			{name: "a", node: &schemaNode{kind: "int"}},
		}},
	}}
	w := &schemaNode{kind: "union", branches: []*schemaNode{
		{kind: "record", name: "R", fields: []fieldNode{
			{name: "a", node: &schemaNode{kind: "boolean"}},
		}},
	}}
	_, err := resolveUnionUnion(r, w, "", &resolveCtx{seen: make(map[nodePair]*schemaNode)})
	if err == nil {
		t.Fatal("expected error for incompatible records in union-union")
	}
}

func TestResolveDeserStructMissingField(t *testing.T) {
	// Struct is missing a field the reader schema expects → typeFieldMapping error.
	// Schemas must differ so Resolve doesn't short-circuit.
	writer := mustParse(t, recABSchema)
	reader := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"a","type":"long"},
		{"name":"b","type":"string"}
	]}`)
	resolved := mustResolve(t, writer, reader)

	encoded := mustEncode(t, writer, map[string]any{"a": 1, "b": "x"})

	// Struct only has field A, missing field B.
	type Partial struct {
		A int64 `avro:"a"`
	}
	var result Partial
	_, err := resolved.Decode(encoded, &result)
	if err == nil {
		t.Fatal("expected error for struct missing field b")
	}
}

func TestResolveNamespacedAlias(t *testing.T) {
	// Reader uses a namespaced name with aliases, writer uses one of the aliases.
	// This exercises qualifyAliases with a dot in fullname and unqualified aliases.
	reader := mustParse(t, `{
		"type":"record",
		"name":"com.example.NewName",
		"aliases":["OldName"],
		"fields":[{"name":"a","type":"int"}]
	}`)
	writer := mustParse(t, `{
		"type":"record",
		"name":"com.example.OldName",
		"fields":[{"name":"a","type":"int"}]
	}`)
	err := CheckCompatibility(writer, reader)
	if err != nil {
		t.Fatalf("expected compatible via alias: %v", err)
	}
}

func TestResolveFullyQualifiedAlias(t *testing.T) {
	// Alias already contains a dot (fully qualified).
	reader := mustParse(t, `{
		"type":"record",
		"name":"com.example.NewName",
		"aliases":["com.other.OldName"],
		"fields":[{"name":"a","type":"int"}]
	}`)
	writer := mustParse(t, `{
		"type":"record",
		"name":"com.other.OldName",
		"fields":[{"name":"a","type":"int"}]
	}`)
	err := CheckCompatibility(writer, reader)
	if err != nil {
		t.Fatalf("expected compatible via fully-qualified alias: %v", err)
	}
}

// A namespace-qualified alias names exactly that fullname — spec "Aliases": if a
// type named "a.b" has aliases "c" and "x.y", their fully qualified names are
// "a.c" and "x.y". It must not match a same-short-name type in a DIFFERENT
// namespace: Java rewrites writer names through a fullname-keyed alias map
// (Schema.applyAliases) and fastavro matches the writer's fullname or bare short
// name against the alias strings as written (match_schemas), and both reject this
// pair. Only an alias declared WITHOUT a dot short-matches across namespaces
// (fastavro's raw-string tier, executed; Java is fullname-only).
func TestResolveQualifiedAliasIsNamespaceScoped(t *testing.T) {
	writer := MustParse(`{"type":"record","name":"n2.Old","fields":[{"name":"a","type":"int"}]}`)
	reader := MustParse(`{"type":"record","name":"n1.New","aliases":["n1.Old"],"fields":[{"name":"a","type":"int"}]}`)
	if err := CheckCompatibility(writer, reader); err == nil {
		t.Errorf("CheckCompatibility: qualified alias n1.Old matched writer n2.Old")
	}
	if _, err := Resolve(writer, reader); err == nil {
		t.Errorf("Resolve: qualified alias n1.Old matched writer n2.Old")
	}

	// The union-branch matcher applies the same rule.
	readerUnion := MustParse(`["int",{"type":"record","name":"n1.New","aliases":["n1.Old"],"fields":[{"name":"a","type":"int"}]}]`)
	if err := CheckCompatibility(writer, readerUnion); err == nil {
		t.Errorf("CheckCompatibility union branch: qualified alias n1.Old matched writer n2.Old")
	}
	if _, err := Resolve(writer, readerUnion); err == nil {
		t.Errorf("Resolve union branch: qualified alias n1.Old matched writer n2.Old")
	}

	// Kept behaviors: an alias declared without a dot short-matches a
	// foreign-namespace writer (fastavro's raw-string tier)...
	readerBare := MustParse(`{"type":"record","name":"n1.New","aliases":["Old"],"fields":[{"name":"a","type":"int"}]}`)
	if err := CheckCompatibility(writer, readerBare); err != nil {
		t.Errorf("bare alias must keep short-matching a foreign-namespace writer: %v", err)
	}
	// ...and a qualified alias matches its exact fullname.
	writerN1 := MustParse(`{"type":"record","name":"n1.Old","fields":[{"name":"a","type":"int"}]}`)
	if err := CheckCompatibility(writerN1, reader); err != nil {
		t.Errorf("qualified alias must keep matching its exact fullname: %v", err)
	}
}

// Aliases follow the names' dot rule (leadingDotName): a single leading dot
// with a DOTLESS remainder is the null-namespace escape (".x" is the
// fullname "x"), and any other dotted spelling is a fullname VERBATIM —
// Java's Name constructor nulls the space only when it is empty (lastDot
// split, then `if ("".equals(space)) space = null`), so ".a.b" keeps its
// non-empty space ".a"; fastavro compares alias strings as written, so a
// raw ".a.b" matches only a writer literally named ".a.b". Stripping the
// dot from ".a.b" would match writer "a.b" — a match neither reference
// makes.
func TestResolveLeadingDotAliasDotRule(t *testing.T) {
	lax := WithLaxNames(func(string) error { return nil })

	// The escape spelling keeps working: ".x" aliases the null-namespace x.
	writerX := MustParse(`{"type":"record","name":"x","fields":[{"name":"a","type":"int"}]}`)
	readerEsc := MustParse(`{"type":"record","name":"n1.New","aliases":[".x"],"fields":[{"name":"a","type":"int"}]}`)
	if err := CheckCompatibility(writerX, readerEsc); err != nil {
		t.Errorf(`alias ".x" must keep matching the null-namespace writer x: %v`, err)
	}

	// A multi-dot leading-dot alias is verbatim: it must NOT match the
	// dotless-namespace writer a.b ...
	writerAB := MustParse(`{"type":"record","name":"a.b","fields":[{"name":"a","type":"int"}]}`)
	readerDot := MustParse(`{"type":"record","name":"n1.New","aliases":[".a.b"],"fields":[{"name":"a","type":"int"}]}`)
	if err := CheckCompatibility(writerAB, readerDot); err == nil {
		t.Errorf(`alias ".a.b" matched writer "a.b"; the verbatim spelling denotes only a writer literally named ".a.b"`)
	}
	if _, err := Resolve(writerAB, readerDot); err == nil {
		t.Errorf(`Resolve: alias ".a.b" matched writer "a.b"`)
	}

	// ... and it DOES match a (lax-named) writer literally called ".a.b".
	writerDot, err := Parse(`{"type":"record","name":".a.b","fields":[{"name":"a","type":"int"}]}`, lax)
	if err != nil {
		t.Fatalf("lax writer .a.b: %v", err)
	}
	if err := CheckCompatibility(writerDot, readerDot); err != nil {
		t.Errorf(`alias ".a.b" must match the writer literally named ".a.b": %v`, err)
	}
}

func TestResolveNullUnionDefault(t *testing.T) {
	writer := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"x","type":"int"}
	]}`)
	reader := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"x","type":"int"},
		{"name":"opt","type":["null","string"],"default":null}
	]}`)
	resolved := mustResolve(t, writer, reader)

	encoded := mustEncode(t, writer, map[string]any{"x": 5})

	var result any
	mustDecode(t, resolved, encoded, &result)
	m := result.(map[string]any)
	if m["x"] != int32(5) {
		t.Fatalf("expected x=5, got %v", m["x"])
	}
	// opt should be nil (null default).
	if m["opt"] != nil {
		t.Fatalf("expected opt=nil, got %v", m["opt"])
	}
}

// ---------- map_setiter_test.go ----------

type ptrTextString string

func (p *ptrTextString) MarshalText() ([]byte, error) { return []byte("MT:" + string(*p)), nil }

// long→int JSON native decode must never silently truncate: a wire value
// outside int32 range into []int / map[string]int is preserved on 64-bit
// (native) and rejected on 32-bit (reflect fallback) — but never garbage. The
// int32 value type always rejects (parseJSONInt32 range-checks). Locks the
// 32-bit narrowing fix.
func TestRegression_JSONNativeLongIntNoTruncate(t *testing.T) {
	const big = `5000000000` // > math.MaxInt32

	// int32 value type: must error regardless of platform.
	if err := MustParse(`{"type":"array","items":"int"}`).DecodeJSON([]byte("["+big+"]"), &[]int32{}); err == nil {
		t.Fatal("[]int32: expected overflow error for 5000000000")
	}

	// []int "long":
	var sl []int
	errSl := MustParse(`{"type":"array","items":"long"}`).DecodeJSON([]byte("["+big+"]"), &sl)
	// map[string]int "long":
	var mp map[string]int
	errMp := MustParse(`{"type":"map","values":"long"}`).DecodeJSON([]byte(`{"k":`+big+`}`), &mp)

	if strconv.IntSize == 64 {
		if errSl != nil || len(sl) != 1 || int64(sl[0]) != 5000000000 {
			t.Fatalf("64-bit []int: err=%v val=%v (want [5000000000])", errSl, sl)
		}
		if errMp != nil || int64(mp["k"]) != 5000000000 {
			t.Fatalf("64-bit map[string]int: err=%v val=%v", errMp, mp)
		}
	} else {
		if errSl == nil {
			t.Fatalf("32-bit []int: expected overflow error, got %v", sl)
		}
		if errMp == nil {
			t.Fatalf("32-bit map[string]int: expected overflow error, got %v", mp)
		}
	}
}

// []fpString (named string, no text method) decodes via the array fast loop
// (deserArrayStringLoop / JSON reflect) — the native loop's exact-string
// assertion misses it. map[string]fpString covers the map fast block; this
// covers the array path, binary and JSON.
func TestRegression_ArrayNamedStringFastLoopDecode(t *testing.T) {
	s := MustParse(`{"type":"array","items":"string"}`)
	want := []fpString{"a", "", "b\x00c", "héllo"}
	wire := mustEncode(t, s, want)
	var out []fpString
	mustDecode(t, s, wire, &out)
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("binary: %v != %v", out, want)
	}
	j := mustEncodeJSON(t, s, want)
	var jout []fpString
	mustDecodeJSON(t, s, j, &jout)
	if !reflect.DeepEqual(jout, want) {
		t.Fatalf("json: %v != %v", jout, want)
	}
}

// A pointer-receiver MarshalText does not fire on a non-addressable by-value
// scalar (the value's method set lacks the pointer method), so it encodes as
// the raw string — matching encoding/json. By pointer (addressable) it fires.
func TestRegression_PointerMarshalTextNonAddressableScalar(t *testing.T) {
	s := MustParse(`"string"`)
	v := ptrTextString("hi")

	byVal := mustEncode(t, s, v)
	rawHi, _ := s.Encode("hi")
	if !bytes.Equal(byVal, rawHi) {
		t.Fatalf("by-value pointer-MarshalText fired: got % x, want raw % x", byVal, rawHi)
	}
	jv := mustMarshal(t, v) // parity baseline: encoding/json also doesn't fire
	if string(jv) != `"hi"` {
		t.Fatalf("encoding/json parity broken: got %s want \"hi\"", jv)
	}

	byPtr := mustEncode(t, s, &v)
	rawMT, _ := s.Encode("MT:hi")
	if !bytes.Equal(byPtr, rawMT) {
		t.Fatalf("by-pointer MarshalText did not fire: got % x, want % x", byPtr, rawMT)
	}
}

// appendMapPrimitive, serMap.ser, and the JSON map encoder reuse two addressable
// Values via SetIterKey/SetIterValue instead of allocating a fresh Value per
// entry. Because the reused value Value is addressable (iter.Value() is not), a
// struct-valued map now reaches serRecord's unsafe fast path. These pin that the
// change is behavior-neutral: every map shape round-trips on both wires to a
// deep-equal value, and the struct-valued map's record bytes match a standalone
// encode. Maps iterate in randomized order, so multi-entry wire is not
// byte-stable — decoded values are compared, except for the deterministic
// single-entry struct case.

type setIterRec struct {
	A int32  `avro:"a"`
	B string `avro:"b"`
}

const setIterRecSchema = `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`

func TestMatrix_MapSetIterRoundTrip(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		val    any
	}{
		{"builtinString", `{"type":"map","values":"string"}`,
			map[string]string{"a": "1", "b": "two", "c": "", "d": "x\x00y"}},
		{"namedString", `{"type":"map","values":"string"}`,
			map[string]fpString{"a": "1", "b": "two", "c": ""}},
		{"int", `{"type":"map","values":"int"}`,
			map[string]int32{"a": 0, "b": -1, "c": math.MaxInt32, "d": math.MinInt32}},
		{"long", `{"type":"map","values":"long"}`,
			map[string]int64{"a": 0, "b": -1, "c": math.MaxInt64, "d": math.MinInt64}},
		{"double", `{"type":"map","values":"double"}`,
			map[string]float64{"a": 0, "b": -1.5, "c": math.MaxFloat64}},
		{"float", `{"type":"map","values":"float"}`,
			map[string]float32{"a": 0, "b": -1.5, "c": math.MaxFloat32}},
		{"bool", `{"type":"map","values":"boolean"}`,
			map[string]bool{"a": true, "b": false}},
		{"structVal", `{"type":"map","values":` + setIterRecSchema + `}`,
			map[string]setIterRec{"x": {1, "one"}, "y": {2, "two"}, "z": {-3, ""}}},
		{"ptrStructVal", `{"type":"map","values":` + setIterRecSchema + `}`,
			map[string]*setIterRec{"x": {1, "one"}, "y": {2, "two"}}},
		{"nestedMap", `{"type":"map","values":{"type":"map","values":"int"}}`,
			map[string]map[string]int32{"o": {"i": 1, "j": 2}, "p": {"k": 3}}},
		{"jsonNumberKey", `{"type":"map","values":"string"}`,
			map[json.Number]string{"1": "a", "22": "b", "-3": "c"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := MustParse(c.schema)

			// Binary round-trip.
			b := mustEncode(t, s, c.val)
			bOut := reflect.New(reflect.TypeOf(c.val)).Interface()
			mustDecode(t, s, b, bOut)
			if got := reflect.ValueOf(bOut).Elem().Interface(); !reflect.DeepEqual(got, c.val) {
				t.Fatalf("binary round-trip mismatch:\n got=%#v\n want=%#v", got, c.val)
			}

			// JSON round-trip.
			j := mustEncodeJSON(t, s, c.val)
			jOut := reflect.New(reflect.TypeOf(c.val)).Interface()
			if err := s.DecodeJSON(j, jOut); err != nil {
				t.Fatalf("json decode: %v (json=%s)", err, j)
			}
			if got := reflect.ValueOf(jOut).Elem().Interface(); !reflect.DeepEqual(got, c.val) {
				t.Fatalf("json round-trip mismatch:\n got=%#v\n want=%#v", got, c.val)
			}
		})
	}
}

// The numeric/bool value switch in appendMapPrimitive must be byte-identical
// to the general (named-type) path it replaced. Single-entry maps have
// deterministic wire order, so bytes are directly comparable: a builtin
// value type takes the switch, a same-underlying named type takes the
// general appendFn path.
func TestMatrix_MapValueSwitchMatchesGeneral(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		fast   any
		gen    any
	}{
		{"int", `{"type":"map","values":"int"}`,
			map[string]int32{"k": -123456}, map[string]fpInt32{"k": -123456}},
		{"long", `{"type":"map","values":"long"}`,
			map[string]int64{"k": math.MinInt64}, map[string]fpInt64{"k": math.MinInt64}},
		{"longFromInt", `{"type":"map","values":"long"}`,
			map[string]int{"k": -1}, map[string]fpInt{"k": -1}},
		{"float", `{"type":"map","values":"float"}`,
			map[string]float32{"k": 3.14}, map[string]fpFloat32{"k": 3.14}},
		{"floatSignalingNaN", `{"type":"map","values":"float"}`,
			map[string]float32{"k": math.Float32frombits(0x7f800001)},
			map[string]fpFloat32{"k": fpFloat32(math.Float32frombits(0x7f800001))}},
		{"double", `{"type":"map","values":"double"}`,
			map[string]float64{"k": 2.718281828}, map[string]fpFloat64{"k": 2.718281828}},
		{"bool", `{"type":"map","values":"boolean"}`,
			map[string]bool{"k": true}, map[string]fpBool{"k": true}},
		{"string", `{"type":"map","values":"string"}`,
			map[string]string{"k": "v"}, map[string]fpString{"k": "v"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := MustParse(c.schema)
			fast, err := s.Encode(c.fast)
			if err != nil {
				t.Fatalf("fast: %v", err)
			}
			gen, err := s.Encode(c.gen)
			if err != nil {
				t.Fatalf("gen: %v", err)
			}
			if !bytes.Equal(fast, gen) {
				t.Fatalf("map value switch diverges from general path:\n fast=% x\n gen =% x", fast, gen)
			}
		})
	}
}

// Named slice and element types for the array destination-shape axis. A
// DEFINED slice type and a builtin slice of a DEFINED element type both
// leave the native loop, but by different tests, so they are separate axis
// values rather than two spellings of one.
type (
	nsInt32   []int32
	nsInt64   []int64
	nsFloat32 []float32
	nsFloat64 []float64
	nsBool    []bool
	nsString  []string
)

// TestMatrix_ArrayElementSwitchMatchesGeneral is the ARRAY sibling of
// TestMatrix_MapValueSwitchMatchesGeneral. The map net crosses the Go type's
// DEFINEDNESS — builtin value type against a defined one — over every primitive;
// the array nets never did, so every array cell handed the decoder a builtin
// slice of a builtin element and took the native loop, leaving the reflect-typed
// fallback unrun for every primitive.
//
//	builtin-slice   []int32       native loop
//	defined-slice   nsInt32       reflect loop (the slice type is not []int32)
//	defined-elem    []fpInt32     reflect loop (the element type is not int32)
//
// The oracle is cross-shape agreement on both wires. Float carries a
// signaling-NaN payload, so a shape routing through a float64 round trip is
// caught by the bits rather than by ==, which cannot see a quieted NaN.
func TestMatrix_ArrayElementSwitchMatchesGeneral(t *testing.T) {
	const snan = uint32(0x7f800001)
	f := math.Float32frombits(snan)
	cases := []struct {
		name    string
		schema  string
		builtin any
		defSlic any
		defElem any
	}{
		{"int", `{"type":"array","items":"int"}`,
			[]int32{0, 1, -1, math.MaxInt32, math.MinInt32},
			nsInt32{0, 1, -1, math.MaxInt32, math.MinInt32},
			[]fpInt32{0, 1, -1, math.MaxInt32, math.MinInt32}},
		{"long", `{"type":"array","items":"long"}`,
			[]int64{0, 1, -1, math.MaxInt64, math.MinInt64},
			nsInt64{0, 1, -1, math.MaxInt64, math.MinInt64},
			[]fpInt64{0, 1, -1, math.MaxInt64, math.MinInt64}},
		{"float", `{"type":"array","items":"float"}`,
			[]float32{3.14, 0, -0},
			nsFloat32{3.14, 0, -0},
			[]fpFloat32{3.14, 0, -0}},
		{"floatSignalingNaN", `{"type":"array","items":"float"}`,
			[]float32{f, 1},
			nsFloat32{fpFloat32Conv(f), 1},
			[]fpFloat32{fpFloat32(f), 1}},
		{"double", `{"type":"array","items":"double"}`,
			[]float64{2.718281828, 0},
			nsFloat64{2.718281828, 0},
			[]fpFloat64{2.718281828, 0}},
		{"boolean", `{"type":"array","items":"boolean"}`,
			[]bool{true, false, true},
			nsBool{true, false, true},
			[]fpBool{true, false, true}},
		{"string", `{"type":"array","items":"string"}`,
			[]string{"a", "", "cde"},
			nsString{"a", "", "cde"},
			[]fpString{"a", "", "cde"}},
	}
	// Liveness floor: each shape must actually have been round-tripped, not
	// merely listed. Counted inside the cell, after the assertion.
	shapeRuns := map[string]int{}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := MustParse(c.schema)
			want, err := s.Encode(c.builtin)
			if err != nil {
				t.Fatalf("builtin encode: %v", err)
			}
			for _, shape := range []struct {
				name string
				v    any
			}{{"defined-slice", c.defSlic}, {"defined-elem", c.defElem}} {
				got, err := s.Encode(shape.v)
				if err != nil {
					t.Fatalf("%s encode: %v", shape.name, err)
				}
				if !bytes.Equal(got, want) {
					t.Errorf("%s encode diverges from the builtin slice:\n got  % x\n want % x", shape.name, got, want)
				}
				// Decode back into the same shape and re-encode: a loop
				// that dropped or reordered elements survives an encode
				// comparison but not this.
				out := reflect.New(reflect.TypeOf(shape.v))
				if _, err := s.Decode(want, out.Interface()); err != nil {
					t.Fatalf("%s decode: %v", shape.name, err)
				}
				again, err := s.Encode(out.Elem().Interface())
				if err != nil {
					t.Fatalf("%s re-encode: %v", shape.name, err)
				}
				if !bytes.Equal(again, want) {
					t.Errorf("%s decode lost information:\n got  % x\n want % x", shape.name, again, want)
				}
				// The JSON wire has its own native-vs-fallback dispatch,
				// with its own test for the destination shape, so a shape
				// proved on the binary wire is not proved here.
				jsonWant, err := s.EncodeJSON(c.builtin)
				if err != nil {
					t.Fatalf("builtin encodeJSON: %v", err)
				}
				jsonGot, err := s.EncodeJSON(shape.v)
				if err != nil {
					t.Fatalf("%s encodeJSON: %v", shape.name, err)
				}
				if !bytes.Equal(jsonGot, jsonWant) {
					t.Errorf("%s JSON encode diverges from the builtin slice:\n got  %s\n want %s", shape.name, jsonGot, jsonWant)
				}
				// The JSON round trip is compared against the BUILTIN
				// shape's JSON round trip, not against the binary wire:
				// the JSON representation of any NaN is the bare token
				// NaN, which carries no payload, so a signaling NaN is
				// quieted on that wire for every shape alike. That is a
				// property of the representation, not a divergence
				// between shapes, and the question here is whether the
				// shapes agree.
				jbuiltin := reflect.New(reflect.TypeOf(c.builtin))
				if err := s.DecodeJSON(jsonWant, jbuiltin.Interface()); err != nil {
					t.Fatalf("builtin decodeJSON: %v", err)
				}
				jbuiltinWire, err := s.Encode(jbuiltin.Elem().Interface())
				if err != nil {
					t.Fatalf("builtin re-encode after JSON: %v", err)
				}
				jout := reflect.New(reflect.TypeOf(shape.v))
				if err := s.DecodeJSON(jsonWant, jout.Interface()); err != nil {
					t.Fatalf("%s decodeJSON: %v", shape.name, err)
				}
				jagain, err := s.Encode(jout.Elem().Interface())
				if err != nil {
					t.Fatalf("%s re-encode after JSON: %v", shape.name, err)
				}
				if !bytes.Equal(jagain, jbuiltinWire) {
					t.Errorf("%s JSON decode diverges from the builtin shape's:\n got  % x\n want % x", shape.name, jagain, jbuiltinWire)
				}
				shapeRuns[shape.name]++
			}
		})
	}
	for _, shape := range []string{"defined-slice", "defined-elem"} {
		if shapeRuns[shape] != len(cases) {
			t.Errorf("destination shape %q ran %d of %d cells; the axis is not spanning the primitives", shape, shapeRuns[shape], len(cases))
		}
	}
}

// fpFloat32Conv converts without going through a float64, so a signaling
// NaN's payload survives into a nsFloat32 literal.
func fpFloat32Conv(f float32) float32 { return f }

type f32Field struct {
	F float32 `avro:"f"`
}

// float32 must preserve exact bits (signaling-NaN payload included) on every
// path — matching Java (floatToRawIntBits/intBitsToFloat), fastavro, and IEEE
// "float is 4 opaque bytes." reflect.Value.Float()/SetFloat would quiet sNaN
// via a float64 round-trip; the encode/decode paths avoid that. Pins that the
// unsafe (addressable) and reflect (by-value) paths agree, and that maps and
// arrays agree with both.
func TestRegression_Float32SignalingNaNPreserved(t *testing.T) {
	const bits = uint32(0x7f800001) // signaling NaN (quiet bit clear)
	f := math.Float32frombits(bits)
	wire := []byte{0x01, 0x00, 0x80, 0x7f} // little-endian 0x7f800001

	// ENCODE: record field, both by-value (reflect) and by-pointer (unsafe).
	rec := MustParse(`{"type":"record","name":"R","fields":[{"name":"f","type":"float"}]}`)
	for _, enc := range []any{f32Field{f}, &f32Field{f}} {
		b, err := rec.Encode(enc)
		if err != nil {
			t.Fatalf("encode %T: %v", enc, err)
		}
		if !bytes.Equal(b, wire) {
			t.Fatalf("encode %T quieted sNaN: got % x want % x", enc, b, wire)
		}
	}
	mapS := MustParse(`{"type":"map","values":"float"}`)
	arrS := MustParse(`{"type":"array","items":"float"}`)
	mb, _ := mapS.Encode(map[string]float32{"k": f})
	if !bytes.Contains(mb, wire) {
		t.Fatalf("map encode quieted sNaN: % x", mb)
	}
	ab, _ := arrS.Encode([]float32{f})
	if !bytes.Contains(ab, wire) {
		t.Fatalf("array encode quieted sNaN: % x", ab)
	}

	// DECODE: record field, map value, array element, interface — all preserve.
	var sf f32Field
	mustDecode(t, rec, wire, &sf)
	if got := math.Float32bits(sf.F); got != bits {
		t.Fatalf("record decode quieted: got %08x want %08x", got, bits)
	}
	var m map[string]float32
	mustDecode(t, mapS, mb, &m)
	if got := math.Float32bits(m["k"]); got != bits {
		t.Fatalf("map decode quieted: got %08x want %08x", got, bits)
	}
	var a []float32
	mustDecode(t, arrS, ab, &a)
	if got := math.Float32bits(a[0]); got != bits {
		t.Fatalf("array decode quieted: got %08x want %08x", got, bits)
	}
	var anyV any
	mustDecode(t, MustParse(`"float"`), wire, &anyV)
	if got := math.Float32bits(anyV.(float32)); got != bits {
		t.Fatalf("interface decode quieted: got %08x want %08x", got, bits)
	}

	// Named float32 (fpFloat32): a non-addressable scalar encode hits
	// float32WireBits's typedmemmove-into-temp branch — distinct from the
	// builtin-float32 Interface() branch and the unsafe-addressable branch
	// exercised above, and the only float32 path the rest of this test (and
	// TestMatrix_MapValueSwitchMatchesGeneral, which uses an addressable
	// map elem) does not reach with a payload. Scalar decode and the
	// []fpFloat32 reflect loop must preserve the raw bits too.
	fltS := MustParse(`"float"`)
	nb := mustEncode(t, fltS, fpFloat32(f))
	if !bytes.Equal(nb, wire) {
		t.Fatalf("named float32 scalar encode quieted sNaN: got % x want % x", nb, wire)
	}
	var nf fpFloat32
	mustDecode(t, fltS, wire, &nf)
	if got := math.Float32bits(float32(nf)); got != bits {
		t.Fatalf("named float32 scalar decode quieted: got %08x want %08x", got, bits)
	}
	nab, _ := arrS.Encode([]fpFloat32{fpFloat32(f)})
	if !bytes.Contains(nab, wire) {
		t.Fatalf("named []fpFloat32 encode quieted sNaN: % x", nab)
	}
	var na []fpFloat32
	mustDecode(t, arrS, nab, &na)
	if got := math.Float32bits(float32(na[0])); got != bits {
		t.Fatalf("named []fpFloat32 decode quieted: got %08x want %08x", got, bits)
	}
}

// JSON array encode native must be byte-identical to the reflect path.
// Arrays are ordered, so the whole encoding is byte-comparable.
func TestMatrix_ArrayJSONNativeMatchesGeneral(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		fast   any
		gen    any
	}{
		{"string", `{"type":"array","items":"string"}`,
			[]string{"a", "b\"c\n", ""}, []fpString{"a", "b\"c\n", ""}},
		{"int", `{"type":"array","items":"int"}`,
			[]int32{0, -1, math.MaxInt32, math.MinInt32}, []fpInt32{0, -1, math.MaxInt32, math.MinInt32}},
		{"long", `{"type":"array","items":"long"}`,
			[]int64{0, math.MinInt64, math.MaxInt64}, []fpInt64{0, math.MinInt64, math.MaxInt64}},
		{"float", `{"type":"array","items":"float"}`,
			[]float32{3.5, -1, 0}, []fpFloat32{3.5, -1, 0}},
		{"double", `{"type":"array","items":"double"}`,
			[]float64{2.5, -1}, []fpFloat64{2.5, -1}},
		{"bool", `{"type":"array","items":"boolean"}`,
			[]bool{true, false, true}, []fpBool{true, false, true}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := MustParse(c.schema)
			fast, err := s.EncodeJSON(c.fast)
			if err != nil {
				t.Fatalf("fast: %v", err)
			}
			gen, err := s.EncodeJSON(c.gen)
			if err != nil {
				t.Fatalf("gen: %v", err)
			}
			if !bytes.Equal(fast, gen) {
				t.Fatalf("JSON array native diverges from reflect:\n fast=%s\n gen =%s", fast, gen)
			}
		})
	}
}

// JSON map encode native must be byte-identical to the reflect path. Single
// entry → deterministic order. A builtin value type takes the native path; a
// same-underlying named type takes the reflect (appendAvroJSON) path.
func TestMatrix_MapJSONNativeMatchesGeneral(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		fast   any
		gen    any
	}{
		{"string", `{"type":"map","values":"string"}`,
			map[string]string{"k": "a\"b\n"}, map[string]fpString{"k": "a\"b\n"}},
		{"int", `{"type":"map","values":"int"}`,
			map[string]int32{"k": -123456}, map[string]fpInt32{"k": -123456}},
		{"long", `{"type":"map","values":"long"}`,
			map[string]int64{"k": math.MinInt64}, map[string]fpInt64{"k": math.MinInt64}},
		{"float", `{"type":"map","values":"float"}`,
			map[string]float32{"k": 3.5}, map[string]fpFloat32{"k": 3.5}},
		{"double", `{"type":"map","values":"double"}`,
			map[string]float64{"k": 2.5}, map[string]fpFloat64{"k": 2.5}},
		{"bool", `{"type":"map","values":"boolean"}`,
			map[string]bool{"k": true}, map[string]fpBool{"k": true}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := MustParse(c.schema)
			fast, err := s.EncodeJSON(c.fast)
			if err != nil {
				t.Fatalf("fast: %v", err)
			}
			gen, err := s.EncodeJSON(c.gen)
			if err != nil {
				t.Fatalf("gen: %v", err)
			}
			if !bytes.Equal(fast, gen) {
				t.Fatalf("JSON native diverges from reflect path:\n fast=%s\n gen =%s", fast, gen)
			}
		})
	}
}

// JSON decode native (map[string]V via parse leaves, []V via append) must
// round-trip, and named slice/elem/map types must fall back to reflect.
func TestRegression_JSONDecodeNative(t *testing.T) {
	type nsInt []int32
	type nElem int32
	arrS := MustParse(`{"type":"array","items":"int"}`)
	in := []int32{0, 1, -1, math.MaxInt32, math.MinInt32}
	aj := mustEncodeJSON(t, arrS, in)
	var aOut []int32 // native slice
	mustDecodeJSON(t, arrS, aj, &aOut)
	if !reflect.DeepEqual(aOut, in) {
		t.Fatalf("json array native: %v != %v", aOut, in)
	}
	var nsOut nsInt // named slice → fallback
	mustDecodeJSON(t, arrS, aj, &nsOut)
	if !reflect.DeepEqual([]int32(nsOut), in) {
		t.Fatalf("json named-slice fallback: %v", nsOut)
	}
	var neOut []nElem // named elem → fallback
	mustDecodeJSON(t, arrS, aj, &neOut)
	if !reflect.DeepEqual(neOut, []nElem{0, 1, -1, math.MaxInt32, math.MinInt32}) {
		t.Fatalf("json named-elem fallback: %v", neOut)
	}

	mapS := MustParse(`{"type":"map","values":"long"}`)
	m := map[string]int64{"a": 1, "b": math.MinInt64, "c": math.MaxInt64}
	mj := mustEncodeJSON(t, mapS, m)
	var mOut map[string]int64 // native map
	mustDecodeJSON(t, mapS, mj, &mOut)
	if !reflect.DeepEqual(mOut, m) {
		t.Fatalf("json map native: %v != %v", mOut, m)
	}
}

// A named map type (type M map[string]int32) has Key()==string and
// Elem()==int32, so it enters appendMapPrimitive's native switch — but
// v.Interface() yields the named type, so the comma-ok assertion to the
// unnamed map[string]int32 fails and it must fall through to the reflect
// path (not panic, not mis-encode). Single-entry wire must match the
// unnamed map.
type namedIntMap map[string]int32

func TestRegression_MapNamedTypeFallsThroughToReflect(t *testing.T) {
	s := MustParse(`{"type":"map","values":"int"}`)
	m := namedIntMap{"a": 1, "b": -2, "c": math.MaxInt32}
	b, err := s.Encode(m)
	if err != nil {
		t.Fatalf("encode named map: %v", err)
	}
	var out namedIntMap
	mustDecode(t, s, b, &out)
	if !reflect.DeepEqual(out, m) {
		t.Fatalf("round-trip mismatch:\n got=%#v\n want=%#v", out, m)
	}
	got, _ := s.Encode(namedIntMap{"k": 7})
	want, _ := s.Encode(map[string]int32{"k": 7})
	if !bytes.Equal(got, want) {
		t.Fatalf("named vs unnamed map wire differ:\n named  =% x\n unnamed=% x", got, want)
	}
}

// The struct-valued-map flip: a single-entry map[string]Struct encodes its
// value through serRecord's unsafe fast path (now that valV is
// addressable). Its record bytes must be byte-identical to encoding that
// struct standalone. Single entry → deterministic wire layout:
// [count=1: 0x02][key "k": 0x02 'k'][record bytes...][terminator 0x00].
func TestRegression_MapStructValueMatchesStandaloneRecord(t *testing.T) {
	mapS := MustParse(`{"type":"map","values":` + setIterRecSchema + `}`)
	recS := MustParse(setIterRecSchema)

	val := setIterRec{A: 7, B: "hi"}
	mapWire := mustEncode(t, mapS, map[string]setIterRec{"k": val})
	recWire := mustEncode(t, recS, val)
	// Strip 3-byte prefix (count 0x02, keylen 0x02, 'k') and 1-byte
	// terminator.
	if len(mapWire) < 4 {
		t.Fatalf("map wire too short: % x", mapWire)
	}
	inner := mapWire[3 : len(mapWire)-1]
	if !bytes.Equal(inner, recWire) {
		t.Fatalf("struct-valued map record bytes differ from standalone:\n map-inner=% x\n standalone=% x", inner, recWire)
	}
	if term := mapWire[len(mapWire)-1]; term != 0 {
		t.Fatalf("expected 0x00 block terminator, got 0x%02x", term)
	}
}

// ---------- union_branch_match_test.go ----------

// Union-branch selection: the index must give the SCAN's verdict.
//
// Which reader branch a writer node selects is a rule with four ranks — full
// name, alias, unqualified short name, bare-alias short name — plus numeric and
// string/bytes promotion, and a fixed's SIZE folded into the match rather than
// checked after it. Answering it by ranking every reader branch is a scan inside
// the loop both Resolve and CheckCompatibility run, so the answer is now indexed
// ahead of the questions.
//
// Indexing a rule is where a rule quietly changes. Java's
// Resolver.firstMatchingBranch scans per writer branch too, so there is no
// reference to re-derive the verdict from — the only thing that can catch a
// drift is the scan itself, stated independently and asked the same questions.

// matchTierOracle ranks how strongly a reader branch matches a writer node.
// This is the rule written out longhand, from the spec clauses and NOT_BUGS
// #44's ruling, rather than read off branchMatchTiers — so it is an
// independent statement of what the index is supposed to encode, and a
// disagreement means the index changed a verdict rather than only its cost.
type matchTierOracle int

const (
	oracleNone matchTierOracle = iota
	oraclePromotion
	oracleUnqualified
	oracleExact
)

func (t matchTierOracle) String() string {
	switch t {
	case oracleExact:
		return "exact"
	case oracleUnqualified:
		return "unqualified"
	case oraclePromotion:
		return "promotion"
	}
	return "none"
}

func oracleTier(r, w *schemaNode) matchTierOracle {
	if r.kind == w.kind {
		switch r.kind {
		case "record", "enum", "fixed":
			// Size is part of the MATCH predicate for fixed, not a
			// post-selection check: a wrong-size same-name fixed must not
			// match, so selection keeps looking and a later size-matching
			// branch wins.
			if r.kind == "fixed" && r.size != w.size {
				return oracleNone
			}
			if r.name == w.name {
				return oracleExact
			}
			for _, a := range r.aliases {
				if a == w.name {
					return oracleExact
				}
			}
			if unqualified(r.name) == unqualified(w.name) {
				return oracleUnqualified
			}
			for _, a := range r.bareAliases {
				if a == unqualified(w.name) {
					return oracleUnqualified
				}
			}
			return oracleNone
		default:
			return oracleExact
		}
	}
	if promotionDeser(w.kind, r.kind) != nil {
		return oraclePromotion
	}
	return oracleNone
}

// oracleMatch is the best-tier scan: rank every reader branch, keep the best,
// ties resolve by declaration order.
func oracleMatch(r, w *schemaNode) (*schemaNode, matchTierOracle) {
	best, bestTier := (*schemaNode)(nil), oracleNone
	for _, rb := range r.branches {
		if rb == nil {
			continue
		}
		if t := oracleTier(rb, w); t > bestTier {
			bestTier, best = t, rb
		}
	}
	return best, bestTier
}

// branchMatchCorpus returns reader unions spanning every rank the rule has,
// including the shapes where two ranks compete and where declaration order is
// the only thing separating two candidates.
func branchMatchCorpus() []string {
	return []string{
		`["null","int","string"]`,
		`["long","double"]`,
		`["double","long"]`,
		`["string","bytes"]`,
		`["float","double","long"]`,
		// Same short name in two namespaces: exact must beat unqualified, and
		// the reversed pair proves the winner is the NAME and not the order.
		`[{"type":"record","name":"a.R","fields":[]},{"type":"record","name":"b.R","fields":[]}]`,
		`[{"type":"record","name":"b.R","fields":[]},{"type":"record","name":"a.R","fields":[]}]`,
		// A qualified alias matches a writer fullname exactly; a bare alias
		// short-matches any namespace. Both live beside a plain branch so the
		// tier that answers is observable.
		`[{"type":"record","name":"a.Q","aliases":["a.R"],"fields":[]}]`,
		`[{"type":"record","name":"a.Q","aliases":["R"],"fields":[]}]`,
		`[{"type":"record","name":"z.Z","fields":[]},{"type":"record","name":"a.Q","aliases":["a.R"],"fields":[]}]`,
		// Same short name, different sizes: the 4-size writer must skip PAST
		// the 8-size branch rather than match and fail later.
		`[{"type":"fixed","name":"b.F","size":8},{"type":"fixed","name":"a.F","size":4}]`,
		`[{"type":"fixed","name":"a.F","size":4},{"type":"fixed","name":"b.F","size":8}]`,
		`[{"type":"enum","name":"a.E","symbols":["A"]},"string"]`,
		`[{"type":"enum","name":"b.E","symbols":["A"]}]`,
		`["null",{"type":"map","values":"int"},{"type":"array","items":"int"}]`,
		`[{"type":"record","name":"R","fields":[]}]`,
		// A named branch that fails both name tiers must NOT fall through to
		// promotion, so a union of only records answers nothing for a record
		// writer of an unrelated name.
		`[{"type":"record","name":"q.Q","fields":[]}]`,
		`["int"]`,
		`[]`,
	}
}

func branchMatchWriters() []string {
	return []string{
		`"null"`, `"boolean"`, `"int"`, `"long"`, `"float"`, `"double"`, `"string"`, `"bytes"`,
		`{"type":"record","name":"a.R","fields":[]}`,
		`{"type":"record","name":"b.R","fields":[]}`,
		`{"type":"record","name":"c.R","fields":[]}`,
		`{"type":"record","name":"R","fields":[]}`,
		`{"type":"fixed","name":"a.F","size":4}`,
		`{"type":"fixed","name":"c.F","size":4}`,
		`{"type":"fixed","name":"c.F","size":8}`,
		`{"type":"enum","name":"a.E","symbols":["A"]}`,
		`{"type":"enum","name":"c.E","symbols":["A"]}`,
		`{"type":"map","values":"int"}`,
		`{"type":"array","items":"int"}`,
	}
}

// TestInvariant_ReaderBranchLookupMatchesTheScan is the verdict half. The
// lookup exists to make selection constant-time per writer branch; the one
// thing it may not do is select differently.
func TestInvariant_ReaderBranchLookupMatchesTheScan(t *testing.T) {
	tierHits := map[matchTierOracle]int{}
	cells := 0
	for _, readerText := range branchMatchCorpus() {
		r := MustParse(readerText).node
		lk := newReaderBranchLookup(r)
		for _, writerText := range branchMatchWriters() {
			w := MustParse(writerText).node
			wantNode, wantTier := oracleMatch(r, w)
			gotNode := lk.match(w)
			cells++
			tierHits[wantTier]++
			if gotNode != wantNode {
				t.Errorf("reader %s\nwriter %s\n  scan  → %s\n  index → %s\n  (tier %s)",
					readerText, writerText, nodeDesc(wantNode), nodeDesc(gotNode), wantTier)
			}
		}
	}
	// A net that never reaches a rank cannot notice that rank changing. Every
	// rank the rule has, plus the no-match verdict, has to appear.
	for _, tier := range []matchTierOracle{oracleExact, oracleUnqualified, oraclePromotion, oracleNone} {
		if tierHits[tier] == 0 {
			t.Errorf("no corpus cell resolves at the %s rank — that rank ships undriven", tier)
		}
	}
	t.Logf("cells=%d exact=%d unqualified=%d promotion=%d none=%d",
		cells, tierHits[oracleExact], tierHits[oracleUnqualified], tierHits[oraclePromotion], tierHits[oracleNone])
}

func nodeDesc(n *schemaNode) string {
	if n == nil {
		return "<no branch>"
	}
	if n.name != "" {
		return fmt.Sprintf("%s(%s)", n.kind, n.name)
	}
	return n.kind
}

// TestInvariant_EveryBranchMatchTierIsDriven derives the rank set from
// branchMatchTiers rather than listing it, so a rank added there without a
// corpus shape fails here instead of shipping unexercised. It also asserts each
// rank actually ANSWERS for some cell: a rank whose writerName never returns a
// registered key is present in source and absent from behavior.
func TestInvariant_EveryBranchMatchTierIsDriven(t *testing.T) {
	answered := make([]int, len(branchMatchTiers))
	for _, readerText := range branchMatchCorpus() {
		r := MustParse(readerText).node
		lk := newReaderBranchLookup(r)
		for _, writerText := range branchMatchWriters() {
			w := MustParse(writerText).node
			for ti, tier := range branchMatchTiers {
				name, ok := tier.writerName(w)
				if !ok {
					continue
				}
				if _, hit := lk.byTier[ti][branchMatchKey{kind: w.kind, name: name, size: branchSizeKey(w)}]; hit {
					answered[ti]++
				}
			}
		}
	}
	for ti, tier := range branchMatchTiers {
		if answered[ti] == 0 {
			t.Errorf("rank %q (branchMatchTiers[%d]) answers no corpus cell — add a reader/writer pair that it selects, or the rank is unexercised", tier.name, ti)
		}
	}
	// The promotion rank has no tier entry (it is keyed by kind alone), so its
	// vocabulary is checked against the table it derives from.
	if len(promotionTargetKinds) == 0 {
		t.Fatal("promotionTargetKinds derived nothing from the promotions table")
	}
	for writerKind, readerKinds := range promotionTargetKinds {
		for _, readerKind := range readerKinds {
			if promotionDeser(writerKind, readerKind) == nil {
				t.Errorf("promotionTargetKinds says %s→%s but the promotions table has no such entry", writerKind, readerKind)
			}
		}
	}
	for key := range promotions {
		writerKind, readerKind, _ := strings.Cut(key, ">")
		found := false
		for _, k := range promotionTargetKinds[writerKind] {
			if k == readerKind {
				found = true
			}
		}
		if !found {
			t.Errorf("the promotions table has %s but promotionTargetKinds dropped it — the promotion rank would never select that reader kind", key)
		}
	}
}

// TestMatrix_UnionBranchSelectionSurvivesIndexing pins the individual
// verdicts the ranks exist to produce, so a future change that collapses two
// ranks fails with the shape it broke rather than only as a corpus diff.
func TestMatrix_UnionBranchSelectionSurvivesIndexing(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reader string
		writer string
		want   string // the selected branch, or "" for no match
	}{
		{"exact fullname beats same-short-name sibling",
			`[{"type":"record","name":"b.R","fields":[]},{"type":"record","name":"a.R","fields":[]}]`,
			`{"type":"record","name":"a.R","fields":[]}`, "record(a.R)"},
		{"unqualified short name when no fullname matches",
			`[{"type":"record","name":"b.R","fields":[]}]`,
			`{"type":"record","name":"c.R","fields":[]}`, "record(b.R)"},
		{"qualified alias matches the writer fullname exactly",
			`[{"type":"record","name":"a.Q","aliases":["a.R"],"fields":[]}]`,
			`{"type":"record","name":"a.R","fields":[]}`, "record(a.Q)"},
		{"bare alias short-matches across namespaces",
			`[{"type":"record","name":"a.Q","aliases":["R"],"fields":[]}]`,
			`{"type":"record","name":"z.R","fields":[]}`, "record(a.Q)"},
		{"fixed size is part of the match, so selection skips past a wrong-size sibling",
			`[{"type":"fixed","name":"b.F","size":8},{"type":"fixed","name":"a.F","size":4}]`,
			`{"type":"fixed","name":"c.F","size":4}`, "fixed(a.F)"},
		{"no fixed of the writer's size matches at all",
			`[{"type":"fixed","name":"b.F","size":8}]`,
			`{"type":"fixed","name":"c.F","size":2}`, ""},
		{"promotion is the last rank",
			`["long","double"]`, `"int"`, "long"},
		{"promotion takes the earliest promotable branch",
			`["double","long"]`, `"int"`, "double"},
		{"same kind outranks promotion",
			`["long","int"]`, `"int"`, "int"},
		{"a named branch that fails both name ranks does not fall through to promotion",
			`[{"type":"record","name":"q.Q","fields":[]}]`,
			`{"type":"record","name":"a.R","fields":[]}`, ""},
		{"string and bytes promote to each other",
			`["bytes"]`, `"string"`, "bytes"},
		{"an empty union answers nothing",
			`[]`, `"int"`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := MustParse(tc.reader).node
			w := MustParse(tc.writer).node
			got := ""
			if n := newReaderBranchLookup(r).match(w); n != nil {
				got = nodeDesc(n)
			}
			if got != tc.want {
				t.Errorf("selected %q, want %q", got, tc.want)
			}
		})
	}
}

// ---------- overflow_audit_test.go ----------

// TestOverflowAuditAllPaths runs integer-overflow probes through every decode
// entry point that accepts a Go integer target, ensuring silent wrap/truncation
// cannot occur.
func TestOverflowAuditAllPaths(t *testing.T) {
	t.Run("binary deserInt: int->int8 overflow", func(t *testing.T) {
		s := MustParse(`"int"`)
		data := mustEncode(t, s, int32(200))
		var out int8
		if _, err := s.Decode(data, &out); err == nil {
			t.Fatalf("expected overflow error, got %d", out)
		}
	})

	t.Run("binary deserLong: long->int32 overflow", func(t *testing.T) {
		s := MustParse(`"long"`)
		data := mustEncode(t, s, int64(1)<<33)
		var out int32
		if _, err := s.Decode(data, &out); err == nil {
			t.Fatalf("expected overflow error, got %d", out)
		}
	})

	t.Run("promote int->long with int16 target", func(t *testing.T) {
		// Writer says int, reader says long. Reader Go target is int16.
		// Value 100000 fits in int32 but overflows int16.
		writer := MustParse(`"int"`)
		reader := MustParse(`"long"`)
		data := mustEncode(t, writer, int32(100000))
		resolved := mustResolve(t, writer, reader)
		var out int16
		if _, err := resolved.Decode(data, &out); err == nil {
			t.Fatalf("expected overflow error, got %d", out)
		}
	})

	t.Run("enum ordinal overflow", func(t *testing.T) {
		// Build an enum with 200 symbols, target int8.
		symbols := `["`
		for i := range 200 {
			if i > 0 {
				symbols += `","`
			}
			symbols += `s` + itoa(i)
		}
		symbols += `"]`
		s := MustParse(`{"type":"enum","name":"E","symbols":` + symbols + `}`)
		// Encode symbol at ordinal 150.
		data := mustEncode(t, s, "s150")
		var out int8
		if _, err := s.Decode(data, &out); err == nil {
			t.Fatalf("expected enum overflow, got %d", out)
		}
	})

	t.Run("logical-type decoder: timestamp-millis into int32 overflow", func(t *testing.T) {
		// timestamp-millis values routinely exceed int32. Encode a modern
		// timestamp (millis since epoch > 2^31).
		s := MustParse(`{"type":"long","logicalType":"timestamp-millis"}`)
		// ~2024 in millis since epoch is ~1.7e12, well beyond int32.
		data := mustEncode(t, s, int64(1700000000000))
		var out int32
		if _, err := s.Decode(data, &out); err == nil {
			t.Fatalf("expected overflow, got %d", out)
		}
	})

	t.Run("JSON decodeInt: int->int8 overflow", func(t *testing.T) {
		s := MustParse(`"int"`)
		var out int8
		err := s.DecodeJSON([]byte(`200`), &out)
		if err == nil {
			t.Fatalf("expected overflow, got %d", out)
		}
	})

	t.Run("JSON decodeLong: long->int32 overflow", func(t *testing.T) {
		s := MustParse(`"long"`)
		var out int32
		err := s.DecodeJSON([]byte(`2147483648`), &out)
		if err == nil {
			t.Fatalf("expected overflow, got %d", out)
		}
	})

	t.Run("negative into unsigned target", func(t *testing.T) {
		s := MustParse(`"int"`)
		data := mustEncode(t, s, int32(-1))
		var out uint32
		if _, err := s.Decode(data, &out); err == nil {
			t.Fatalf("expected overflow, got %d", out)
		}
	})

	t.Run("sanity: in-range values still decode", func(t *testing.T) {
		s := MustParse(`"long"`)
		data := mustEncode(t, s, int64(42))
		var out int32
		mustDecode(t, s, data, &out)
		if out != 42 {
			t.Fatalf("got %d, want 42", out)
		}
	})
}

// TestFloatOverflowAllPaths exercises every path where narrowing float64 to
// float32 could silently produce ±Inf. Overflow must error; NaN/±Inf input
// must pass through; normal precision-loss rounding is allowed.
func TestFloatOverflowAllPaths(t *testing.T) {
	overflow := math.MaxFloat32 * 2 // finite float64 that becomes +Inf in float32

	t.Run("binary deserDouble: overflow into float32", func(t *testing.T) {
		s := MustParse(`"double"`)
		data := mustEncode(t, s, overflow)
		var out float32
		if _, err := s.Decode(data, &out); err == nil {
			t.Fatalf("expected overflow error, got %v", out)
		}
	})

	t.Run("binary deserDouble: ±Inf passes through", func(t *testing.T) {
		s := MustParse(`"double"`)
		for _, v := range []float64{math.Inf(+1), math.Inf(-1)} {
			data := mustEncode(t, s, v)
			var out float32
			if _, err := s.Decode(data, &out); err != nil {
				t.Fatalf("unexpected error on %v: %v", v, err)
			}
			if !math.IsInf(float64(out), 0) {
				t.Fatalf("%v did not round-trip: got %v", v, out)
			}
		}
	})

	t.Run("binary deserDouble: NaN passes through", func(t *testing.T) {
		s := MustParse(`"double"`)
		data := mustEncode(t, s, math.NaN())
		var out float32
		if _, err := s.Decode(data, &out); err != nil {
			t.Fatalf("unexpected error on NaN: %v", err)
		}
		if !math.IsNaN(float64(out)) {
			t.Fatalf("NaN did not round-trip: got %v", out)
		}
	})

	t.Run("binary deserDouble: in-range rounding is silent", func(t *testing.T) {
		s := MustParse(`"double"`)
		precise := 1.1234567890123456
		data := mustEncode(t, s, precise)
		var out float32
		if _, err := s.Decode(data, &out); err != nil {
			t.Fatalf("in-range value errored: %v", err)
		}
	})

	t.Run("serFloat: float64 overflow silently narrows to ±Inf", func(t *testing.T) {
		// Lossy-destination policy: matches Java's
		// GenericDatumWriter.writeFloat(Number.floatValue()) and fastavro's
		// struct.pack("<f", v) — finite float64 → float32 silently narrows
		// to ±Inf when out of range.
		s := MustParse(`"float"`)
		data, err := s.Encode(overflow)
		if err != nil {
			t.Fatalf("encode rejected overflow: %v", err)
		}
		var out float32
		mustDecode(t, s, data, &out)
		if !math.IsInf(float64(out), +1) {
			t.Fatalf("expected +Inf wire value, got %v", out)
		}
	})

	t.Run("serFloat: ±Inf passes through", func(t *testing.T) {
		s := MustParse(`"float"`)
		for _, v := range []float64{math.Inf(+1), math.Inf(-1)} {
			if _, err := s.Encode(v); err != nil {
				t.Fatalf("unexpected error encoding %v: %v", v, err)
			}
		}
	})

	t.Run("JSON decodeDouble: overflow into float32", func(t *testing.T) {
		s := MustParse(`"double"`)
		var out float32
		// Use a large exact-format number that parses but overflows float32.
		if err := s.DecodeJSON([]byte(`6.805646932770577e+38`), &out); err == nil {
			t.Fatalf("expected overflow, got %v", out)
		}
	})

	t.Run("EncodeJSON float: float64 overflow silently narrows", func(t *testing.T) {
		// Lossy-destination policy: float64 → float32 narrowing produces
		// ±Inf, emitted via the dedicated "Infinity" JSON literal.
		s := MustParse(`"float"`)
		out, err := s.EncodeJSON(overflow)
		if err != nil {
			t.Fatalf("encode rejected overflow: %v", err)
		}
		if !strings.Contains(string(out), "Infinity") {
			t.Fatalf("expected Infinity literal in output, got %s", out)
		}
	})

	t.Run("EncodeJSON float: ±Inf and NaN pass through", func(t *testing.T) {
		s := MustParse(`"float"`)
		for _, v := range []float64{math.Inf(+1), math.Inf(-1), math.NaN()} {
			if _, err := s.EncodeJSON(v); err != nil {
				t.Fatalf("unexpected error encoding %v: %v", v, err)
			}
		}
	})

	t.Run("EncodeJSON float: json.Number overflow silently narrows", func(t *testing.T) {
		// Lossy-destination policy: parseFloatAcceptOverflow returns +Inf
		// for overflowing exponent-form input; encoded as "Infinity" literal.
		s := MustParse(`"float"`)
		out, err := s.EncodeJSON(json.Number("1e100"))
		if err != nil {
			t.Fatalf("encode rejected overflow: %v", err)
		}
		if !strings.Contains(string(out), "Infinity") {
			t.Fatalf("expected Infinity literal in output, got %s", out)
		}
	})
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf []byte
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	return string(buf)
}

// ---------- enum_ordinal_overflow_test.go ----------

// An integer-kind enum carrier is validated as an ordinal in [0, len(symbols))
// in the carrier's own width BEFORE narrowing to int. Narrowing first
// (int(v.Uint())) truncates a value ≥ 2^32 to its low bits on a 32-bit build,
// so an out-of-range ordinal like uint64(1<<32+5) would wrap to 5 and encode
// the wrong symbol there while erroring on 64-bit — a platform-dependent
// silent-wrong-output divergence. The wide comparison rejects it on every
// platform; this also pins that the error reports the TRUE value, not a
// truncated/sign-wrapped one (the observable proxy on a 64-bit host).
func TestMatrix_EnumOrdinalOverflowRejected(t *testing.T) {
	const schema = `{"type":"enum","name":"e","symbols":["a","b","c"]}` // len 3

	// A uint64 ordinal whose low bits (mod 2^32) land inside [0,3): on a 32-bit
	// build int(v.Uint()) would truncate to 1 and wrongly accept. Must reject,
	// and the error must name the real value, not the truncated 1.
	reject := func(t *testing.T, enc func(*Schema) ([]byte, error), wantInMsg string) {
		t.Helper()
		s := MustParse(schema)
		_, err := enc(s)
		if err == nil {
			t.Fatal("expected out-of-range error, got nil (ordinal silently truncated and accepted)")
		}
		if !strings.Contains(err.Error(), wantInMsg) {
			t.Errorf("error %q does not mention the true ordinal %q", err, wantInMsg)
		}
	}

	t.Run("binary uint64 ordinal past 2^32", func(t *testing.T) {
		v := uint64(1<<32 + 1) // 4294967297; low 32 bits = 1, which is a valid index
		reject(t, func(s *Schema) ([]byte, error) { return s.AppendEncode(nil, &v) }, "4294967297")
	})
	t.Run("json uint64 ordinal past 2^32", func(t *testing.T) {
		v := uint64(1<<32 + 1)
		reject(t, func(s *Schema) ([]byte, error) { return s.AppendEncodeJSON(nil, &v) }, "4294967297")
	})
	t.Run("binary int64 ordinal past 2^32", func(t *testing.T) {
		v := int64(1<<32 + 2) // low 32 bits = 2, a valid index on 32-bit
		reject(t, func(s *Schema) ([]byte, error) { return s.AppendEncode(nil, &v) }, "4294967298")
	})

	// Boundaries that MUST still encode: valid ordinals across int/uint carriers.
	for _, tc := range []struct {
		name string
		v    any
	}{
		{"uint 0", ptrAny(uint64(0))},
		{"uint last", ptrAny(uint64(2))},
		{"int 0", ptrAny(int64(0))},
		{"int last", ptrAny(int64(2))},
	} {
		t.Run("accept "+tc.name, func(t *testing.T) {
			s := MustParse(schema)
			if _, err := s.AppendEncode(nil, tc.v); err != nil {
				t.Errorf("binary encode of valid ordinal %v: %v", tc.v, err)
			}
			if _, err := s.AppendEncodeJSON(nil, tc.v); err != nil {
				t.Errorf("json encode of valid ordinal %v: %v", tc.v, err)
			}
		})
	}

	// A negative int ordinal still rejects (unchanged behavior).
	t.Run("negative int rejects", func(t *testing.T) {
		v := int64(-1)
		s := MustParse(schema)
		if _, err := s.AppendEncode(nil, &v); err == nil {
			t.Error("expected error for negative ordinal")
		}
	})
}

func ptrAny[T any](v T) *T { return &v }

// ---------- unsafe_depth_test.go ----------

// TestRegression_UnsafeDecodeDepthBounded gives end-to-end coverage of the
// recursion-depth bound on the UNSAFE decode path: a self-referential record
// nested past maxDepth, decoded into an addressable struct, must error. Triage
// note: a scoped mutation run flagged the slab-depth bookkeeping as surviving,
// and this test does NOT kill those mutants — verified by neutering each, the
// decode still errors, because the limit is enforced REDUNDANTLY. The wire is
// hand-built because encode cannot produce an over-deep value, its own depth
// guard stopping it first.
func TestRegression_UnsafeDecodeDepthBounded(t *testing.T) {
	// Node = record{ child: ["null", Node], v: int } — a self-referential
	// type whose decode recurses once per nesting level.
	s := MustParse(`{"type":"record","name":"Node","fields":[` +
		`{"name":"child","type":["null","Node"]},{"name":"v","type":"int"}]}`)

	// Build a wire nested deeper than maxDepth:
	//   level: child-union-index=1 (Node) ... then v=0
	//   leaf:  child-union-index=0 (null), v=0
	// zigzag(1)=0x02, zigzag(0)=0x00.
	const depth = maxDepth + 5
	var wire []byte
	for range depth {
		wire = append(wire, 0x02) // child = the Node branch
	}
	wire = append(wire, 0x00, 0x00) // innermost: child = null, v = 0
	for range depth {
		wire = append(wire, 0x00) // v = 0 unwinding each level
	}

	// Node has a *Node field, so decoding into an addressable &Node routes
	// through the unsafe null-union-record/record path that bumps sl.depth.
	type Node struct {
		Child *Node `avro:"child"`
		V     int32 `avro:"v"`
	}
	var n Node
	if _, err := s.Decode(wire, &n); err == nil {
		t.Fatal("decode of a structure nested past maxDepth through the unsafe path must error (recursion-depth DoS guard); got nil — the depth guard is defeated")
	}

	// A shallow value must still decode (the guard must not false-trigger on
	// the unwound path — catches the inverse sl.depth-- on enter / ++ on exit).
	shallow := []byte{0x02, 0x00, 0x00, 0x00} // one level: child=Node{child=null,v=0}, v=0
	var sn Node
	if _, err := s.Decode(shallow, &sn); err != nil {
		t.Fatalf("shallow nested decode falsely rejected (depth guard mis-restored): %v", err)
	}
}

// ---------- logical_gate_test.go ----------

// jsonDecodeAppliesLogical derives its answer by probing decodeLogical*, so it
// can't drift from what decode actually does. This test independently pins the
// probe's output against the HUMAN-KNOWN transform set for every logical — if
// the probe's type-assertion logic is ever wrong (or a decodeLogical* change
// flips a logical's transform behavior), one of these explicit expectations
// fails, forcing a conscious review. Expected values are spelled out (not
// re-probed) so this is a genuine check, not a tautology.
func TestMatrix_JSONDecodeAppliesLogicalMatchesDecode(t *testing.T) {
	cases := []struct {
		kind, logical string
		size          int
		want          bool
	}{
		// Transforming logicals (decode → enriched Go type).
		{"int", "date", 0, true},                    // → time.Time
		{"int", "time-millis", 0, true},             // → time.Duration
		{"long", "time-micros", 0, true},            // → time.Duration
		{"long", "timestamp-millis", 0, true},       // → time.Time
		{"long", "timestamp-micros", 0, true},       // → time.Time
		{"long", "timestamp-nanos", 0, true},        // → time.Time
		{"long", "local-timestamp-millis", 0, true}, // → time.Time
		{"long", "local-timestamp-micros", 0, true}, // → time.Time
		{"long", "local-timestamp-nanos", 0, true},  // → time.Time
		{"bytes", "decimal", 0, true},               // → *big.Rat
		{"fixed", "decimal", 8, true},               // → *big.Rat
		{"bytes", "big-decimal", 0, true},           // → *big.Rat
		{"fixed", "uuid", 16, true},                 // → [16]byte
		{"fixed", "duration", 12, true},             // → avro.Duration

		// uuid-on-string transforms for a TYPED target — decodeString parses the
		// hex-dash string into a [16]byte / UUID-typed target (into *any/string
		// it is identity, but the gate must report the transform so a no-Decode
		// CustomType installs the suppression wrapper and the raw decode matches
		// binary's deserString, which has no [16]byte arm).
		{"string", "uuid", 0, true},

		// Non-transforming: no logical; and an unknown future logical
		// (decodeLogical* returns raw).
		{"int", "", 0, false},
		{"long", "", 0, false},
		{"bytes", "", 0, false},
		{"string", "", 0, false},
		{"fixed", "", 16, false},
		{"long", "some-future-logical", 0, false},
		{"bytes", "some-future-logical", 0, false},

		// Logical types on a kind they are NOT spec-valid for — reachable only
		// when a CustomType resurrects a soft-dropped non-standard placement.
		// uuid/duration are fixed-only, big-decimal is bytes-only; on the wrong
		// kind neither the *any decodeLogical{Bytes,Fixed} NOR the typed-target
		// assignBytes transforms (assignBytes is kind-gated), so the decode is
		// raw on both wire formats and the probe must report false — otherwise a
		// no-Decode CustomType would over-install the suppression wrapper for a
		// transform that no longer exists.
		{"bytes", "uuid", 0, false},
		{"bytes", "duration", 0, false},
		{"fixed", "big-decimal", 8, false},

		// Hostile fixed size: the probe must NOT allocate proportional to size.
		// jsonDecodeAppliesLogical caps its probe buffer at maxFixedLogicalLen+1,
		// so a size > maxFixedLogicalLen is neither the uuid(16) nor duration(12)
		// length and yields the same answer the small non-match case does, while
		// decimal still transforms at any length. fixed size is schema-controlled
		// and only validated non-negative, so without the cap make([]byte, size)
		// here is a parse-time DoS; at 1<<62 a regressed cap panics immediately
		// with "makeslice: len out of range" (it exceeds the runtime max alloc).
		{"fixed", "uuid", 1 << 62, false},
		{"fixed", "duration", 1 << 62, false},
		{"fixed", "decimal", 1 << 62, true},
	}
	for _, c := range cases {
		node := &schemaNode{kind: c.kind, logical: c.logical, size: c.size}
		if got := jsonDecodeAppliesLogical(node); got != c.want {
			t.Errorf("jsonDecodeAppliesLogical(kind=%s logical=%q)=%v, want %v — probe disagrees with the known transform set (decodeLogical* changed?)", c.kind, c.logical, got, c.want)
		}
	}
}

// ---------- integration_test.go ----------

// TestEncodeFromJSONUnmarshal tests that data from json.Unmarshal (which
// produces float64 for all numbers) can be encoded for every schema type.
// This catches coercion gaps in the json.Unmarshal → Encode pipeline.
func TestEncodeFromJSONUnmarshal(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		json   string
	}{
		// Primitives.
		{"null", `"null"`, `null`},
		{"boolean", `"boolean"`, `true`},
		{"int", `"int"`, `42`},
		{"long", `"long"`, `100000`},
		{"float", `"float"`, `1.5`},
		{"double", `"double"`, `3.14`},
		{"string", `"string"`, `"hello"`},
		{"bytes from string", `"bytes"`, `"hello"`},

		// Arrays of primitives (exercises specialized array serializers).
		{"array of int", `{"type":"array","items":"int"}`, `[1,2,3]`},
		{"array of long", `{"type":"array","items":"long"}`, `[100,200]`},
		{"array of float", `{"type":"array","items":"float"}`, `[1.5,2.5]`},
		{"array of double", `{"type":"array","items":"double"}`, `[3.14]`},
		{"array of string", `{"type":"array","items":"string"}`, `["a","b"]`},
		{"array of boolean", `{"type":"array","items":"boolean"}`, `[true,false]`},

		// Maps of primitives (exercises specialized map serializers).
		{"map of int", `{"type":"map","values":"int"}`, `{"k":42}`},
		{"map of long", `{"type":"map","values":"long"}`, `{"k":100}`},
		{"map of float", `{"type":"map","values":"float"}`, `{"k":1.5}`},
		{"map of double", `{"type":"map","values":"double"}`, `{"k":3.14}`},
		{"map of string", `{"type":"map","values":"string"}`, `{"k":"v"}`},
		{"map of boolean", `{"type":"map","values":"boolean"}`, `{"k":true}`},

		// Unions.
		{"nullable string null", `["null","string"]`, `null`},
		{"nullable string value", `["null","string"]`, `"hello"`},
		{"nullable int null", `["null","int"]`, `null`},
		{"nullable int value", `["null","int"]`, `42`},

		// Enum.
		{"enum", `{"type":"enum","name":"Color","symbols":["RED","GREEN"]}`, `"RED"`},

		// Records.
		{"simple record", `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`, `{"a":1,"b":"hello"}`},
		{"record with nullable", `{"type":"record","name":"R","fields":[{"name":"a","type":"string"},{"name":"b","type":["null","int"]}]}`, `{"a":"x","b":42}`},
		{"record with nullable null", `{"type":"record","name":"R","fields":[{"name":"a","type":"string"},{"name":"b","type":["null","int"]}]}`, `{"a":"x","b":null}`},
		{"record with default", `{"type":"record","name":"R","fields":[{"name":"a","type":"int","default":0},{"name":"b","type":"string"}]}`, `{"b":"hello"}`},
		{"record with bytes field", `{"type":"record","name":"R","fields":[{"name":"data","type":"bytes"}]}`, `{"data":"hello"}`},

		// Nested records.
		{"nested record", `{"type":"record","name":"O","fields":[{"name":"inner","type":{"type":"record","name":"I","fields":[{"name":"x","type":"int"}]}}]}`, `{"inner":{"x":1}}`},

		// Logical types.
		{"timestamp-millis from number", `{"type":"long","logicalType":"timestamp-millis"}`, `1742385600000`},
		{"timestamp-millis from string", `{"type":"long","logicalType":"timestamp-millis"}`, `"2026-03-19T10:00:00Z"`},
		{"timestamp-micros from string", `{"type":"long","logicalType":"timestamp-micros"}`, `"2026-03-19T10:00:00Z"`},
		{"timestamp-nanos from string", `{"type":"long","logicalType":"timestamp-nanos"}`, `"2026-03-19T10:00:00Z"`},
		{"date from number", `{"type":"int","logicalType":"date"}`, `19435`},
		{"date from RFC3339", `{"type":"int","logicalType":"date"}`, `"2026-03-19T00:00:00Z"`},
		{"date from YYYY-MM-DD", `{"type":"int","logicalType":"date"}`, `"2026-03-19"`},

		// Arrays/maps inside records.
		{"record with array", `{"type":"record","name":"R","fields":[{"name":"tags","type":{"type":"array","items":"string"}}]}`, `{"tags":["a","b"]}`},
		{"record with map", `{"type":"record","name":"R","fields":[{"name":"meta","type":{"type":"map","values":"int"}}]}`, `{"meta":{"k":1}}`},
		{"record with array of int", `{"type":"record","name":"R","fields":[{"name":"nums","type":{"type":"array","items":"int"}}]}`, `{"nums":[1,2,3]}`},
		{"record with map of long", `{"type":"record","name":"R","fields":[{"name":"counts","type":{"type":"map","values":"long"}}]}`, `{"counts":{"a":100}}`},

		// Union inside array.
		{"array of nullable string", `{"type":"array","items":["null","string"]}`, `["hello",null,"world"]`},

		// Deeply nested.
		{"deep nesting", `{
			"type":"record","name":"L1","fields":[
				{"name":"l2","type":{"type":"record","name":"L2","fields":[
					{"name":"l3","type":{"type":"record","name":"L3","fields":[
						{"name":"val","type":"int"}
					]}}
				]}}
			]}`, `{"l2":{"l3":{"val":42}}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustParse(t, tt.schema)
			var native any
			if err := json.Unmarshal([]byte(tt.json), &native); err != nil {
				t.Fatalf("json.Unmarshal: %v", err)
			}
			binary := mustEncode(t, s, native)
			// Verify it decodes back.
			var decoded any
			mustDecode(t, s, binary, &decoded)
		})
	}
}

// TestEncodeStringBytesCoercionInCollections tests []byte → string and
// string → bytes coercion in arrays and maps.
func TestEncodeStringBytesCoercionInCollections(t *testing.T) {
	// []byte in array of strings.
	t.Run("array of string from bytes", func(t *testing.T) {
		s, _ := Parse(`{"type":"array","items":"string"}`)
		data := []any{[]byte("hello"), []byte("world")}
		binary := mustEncode(t, s, data)
		var decoded any
		s.Decode(binary, &decoded)
		arr := decoded.([]any)
		if arr[0] != "hello" || arr[1] != "world" {
			t.Errorf("got %v", arr)
		}
	})

	// []byte in map of strings.
	t.Run("map of string from bytes", func(t *testing.T) {
		s, _ := Parse(`{"type":"map","values":"string"}`)
		data := map[string]any{"k": []byte("hello")}
		binary := mustEncode(t, s, data)
		var decoded any
		s.Decode(binary, &decoded)
		m := decoded.(map[string]any)
		if m["k"] != "hello" {
			t.Errorf("got %v", m["k"])
		}
	})

	// string in array of bytes.
	t.Run("array of bytes from string", func(t *testing.T) {
		s, _ := Parse(`{"type":"array","items":"bytes"}`)
		data := []any{"hello", "world"}
		binary := mustEncode(t, s, data)
		var decoded any
		s.Decode(binary, &decoded)
		arr := decoded.([]any)
		if string(arr[0].([]byte)) != "hello" {
			t.Errorf("got %v", arr)
		}
	})
}

// TestEncodeJSONEdgeCases covers paths in appendAvroJSON and DecodeJSON
// that normal integration tests don't reach.
func TestEncodeJSONEdgeCases(t *testing.T) {
	// Null standalone.
	t.Run("encode null", func(t *testing.T) {
		s, _ := Parse(`"null"`)
		b := mustEncodeJSON(t, s, nil)
		if string(b) != "null" {
			t.Errorf("got %s", b)
		}
	})

	// Nil pointer in union.
	t.Run("nil pointer union", func(t *testing.T) {
		s, _ := Parse(`["null","string"]`)
		var p *string
		b := mustEncodeJSON(t, s, p)
		if string(b) != "null" {
			t.Errorf("got %s", b)
		}
	})

	// DecodeJSON with null union.
	t.Run("decode null union", func(t *testing.T) {
		s, _ := Parse(`["null","string"]`)
		var got any
		mustDecodeJSON(t, s, []byte(`null`), &got)
		if got != nil {
			t.Errorf("got %v", got)
		}
	})

	// DecodeJSON float/double passthrough.
	t.Run("decode float", func(t *testing.T) {
		s, _ := Parse(`"float"`)
		var got any
		mustDecodeJSON(t, s, []byte(`1.5`), &got)
		if got != float32(1.5) {
			t.Errorf("got %v (%T)", got, got)
		}
	})

	t.Run("decode double", func(t *testing.T) {
		s, _ := Parse(`"double"`)
		var got any
		mustDecodeJSON(t, s, []byte(`3.14`), &got)
		if got != 3.14 {
			t.Errorf("got %v (%T)", got, got)
		}
	})

	// DecodeJSON long overflow.
	t.Run("decode long overflow", func(t *testing.T) {
		s, _ := Parse(`"long"`)
		var got any
		err := s.DecodeJSON([]byte(`1e25`), &got)
		if err == nil {
			t.Fatal("expected overflow error")
		}
	})

	// Uint values for int field via EncodeJSON.
	t.Run("encode uint16 as int", func(t *testing.T) {
		s, _ := Parse(`"int"`)
		b := mustEncodeJSON(t, s, uint16(42))
		if string(b) != "42" {
			t.Errorf("got %s", b)
		}
	})

	t.Run("encode uint32 as long", func(t *testing.T) {
		s, _ := Parse(`"long"`)
		b := mustEncodeJSON(t, s, uint32(100))
		if string(b) != "100" {
			t.Errorf("got %s", b)
		}
	})
}

// TestEncodeJSONCoercionPaths exercises type coercion paths in EncodeJSON
// that aren't covered by the json.Unmarshal tests (which always produce
// float64, not float32/uint/etc).
func TestEncodeJSONCoercionPaths(t *testing.T) {
	// float32 for int field.
	t.Run("float32 to int", func(t *testing.T) {
		s, _ := Parse(`"int"`)
		b := mustEncodeJSON(t, s, float32(42))
		if string(b) != "42" {
			t.Errorf("got %s", b)
		}
	})

	// float32 for long field.
	t.Run("float32 to long", func(t *testing.T) {
		s, _ := Parse(`"long"`)
		b := mustEncodeJSON(t, s, float32(100))
		if string(b) != "100" {
			t.Errorf("got %s", b)
		}
	})

	// local-timestamp-millis (hits the default case in timestamp switch).
	t.Run("local-timestamp-millis", func(t *testing.T) {
		s, _ := Parse(`{"type":"long","logicalType":"local-timestamp-millis"}`)
		ts := time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)
		b := mustEncodeJSON(t, s, ts)
		want := strconv.FormatInt(ts.UnixMilli(), 10)
		if string(b) != want {
			t.Errorf("got %s, want %s", b, want)
		}
	})

	// nil for non-nullable type.
	t.Run("nil for int", func(t *testing.T) {
		s, _ := Parse(`"int"`)
		_, err := s.EncodeJSON(nil)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	// null schema.
	t.Run("null schema", func(t *testing.T) {
		s, _ := Parse(`"null"`)
		b := mustEncodeJSON(t, s, nil)
		if string(b) != "null" {
			t.Errorf("got %s", b)
		}
	})

	// json.Number for bytes-backed decimal (from Decode → EncodeJSON round-trip).
	t.Run("decimal round-trip", func(t *testing.T) {
		s, _ := Parse(`{"type":"record","name":"R","fields":[
			{"name":"v","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}}
		]}`)
		r := new(big.Rat).SetFrac64(314, 100)
		binary, _ := s.Encode(map[string]any{"v": r})
		var decoded any
		s.Decode(binary, &decoded)
		jb := mustEncodeJSON(t, s, decoded)
		if !json.Valid(jb) {
			t.Fatalf("invalid JSON: %s", jb)
		}
	})

	// []byte for string field — encode and decode round-trips.
	t.Run("bytes to string to bytes", func(t *testing.T) {
		s, _ := Parse(`{"type":"record","name":"R","fields":[{"name":"s","type":"string"}]}`)
		type R struct {
			S []byte `avro:"s"`
		}
		binary := mustEncode(t, s, &R{S: []byte("hello")})
		var decoded R
		s.Decode(binary, &decoded)
		if string(decoded.S) != "hello" {
			t.Errorf("got %s", decoded.S)
		}
	})

	// string for bytes field — encode and decode round-trips.
	t.Run("string to bytes to string", func(t *testing.T) {
		s, _ := Parse(`{"type":"record","name":"R","fields":[{"name":"b","type":"bytes"}]}`)
		type R struct {
			B string `avro:"b"`
		}
		binary := mustEncode(t, s, &R{B: "hello"})
		var decoded R
		s.Decode(binary, &decoded)
		if decoded.B != "hello" {
			t.Errorf("got %s", decoded.B)
		}
	})
}

// TestDecodeStringIntoBytes tests deserString → []byte directly.
func TestDecodeStringIntoBytes(t *testing.T) {
	s, _ := Parse(`"string"`)
	binary, _ := s.Encode("hello")
	var got []byte
	mustDecode(t, s, binary, &got)
	if string(got) != "hello" {
		t.Errorf("got %s", got)
	}
}

// TestDecodeBytesIntoString tests deserBytes → string directly.
func TestDecodeBytesIntoString(t *testing.T) {
	s, _ := Parse(`"bytes"`)
	binary, _ := s.Encode([]byte("hello"))
	var got string
	mustDecode(t, s, binary, &got)
	if got != "hello" {
		t.Errorf("got %s", got)
	}
}

func TestDecodeBytesIntoWrongType(t *testing.T) {
	s, _ := Parse(`"bytes"`)
	binary, _ := s.Encode([]byte{1, 2})
	var got int
	_, err := s.Decode(binary, &got)
	if err == nil {
		t.Fatal("expected error decoding bytes into int")
	}
}

func TestEncodeJSONNullSchema(t *testing.T) {
	s, _ := Parse(`"null"`)
	// Nil values produce "null" wire output.
	t.Run("nil value", func(t *testing.T) {
		b := mustEncodeJSON(t, s, nil)
		if string(b) != "null" {
			t.Errorf("got %s", b)
		}
	})
	// Non-nil values must error, matching binary serNull's errNonNil
	// rejection — see TestMatrix_EncodeJSONNullParity.
	t.Run("non-nil value rejected", func(t *testing.T) {
		if out, err := s.EncodeJSON("ignored"); err == nil {
			t.Errorf("expected error encoding non-nil value into null schema, got %s", out)
		}
	})
}

func TestEncodeJSONDecimalFixedRoundTrip(t *testing.T) {
	s, _ := Parse(`{"type":"record","name":"R","fields":[
		{"name":"v","type":{"type":"fixed","name":"d","size":8,"logicalType":"decimal","precision":10,"scale":2}}
	]}`)
	r := new(big.Rat).SetFrac64(314, 100)
	binary, _ := s.Encode(map[string]any{"v": r})
	var decoded any
	s.Decode(binary, &decoded)
	// decoded has json.Number for the decimal — EncodeJSON should handle it.
	jb := mustEncodeJSON(t, s, decoded)
	if !json.Valid(jb) {
		t.Fatalf("invalid JSON: %s", jb)
	}
}

func TestEncodeJSONNilPointerUnion(t *testing.T) {
	type R struct {
		V *string `avro:"v"`
	}
	s, _ := Parse(`{"type":"record","name":"R","fields":[{"name":"v","type":["null","string"]}]}`)
	b := mustEncodeJSON(t, s, &R{V: nil})
	if !json.Valid(b) {
		t.Fatalf("invalid JSON: %s", b)
	}
}

func TestEncodeJSONStructFieldError(t *testing.T) {
	type R struct {
		A bool `avro:"a"` // schema says int — will fail
	}
	s, _ := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	_, err := s.EncodeJSON(&R{A: true})
	if err == nil {
		t.Fatal("expected error encoding bool as int in struct")
	}
}

func TestEncodeJSONNilPointerTopLevel(t *testing.T) {
	// Nil *string directly for a union — hits appendAvroJSONUnion with nil pointer.
	s, _ := Parse(`["null","string"]`)
	var p *string
	b := mustEncodeJSON(t, s, p)
	if string(b) != "null" {
		t.Errorf("got %s", b)
	}
}

func TestEncodeJSONNilInterfaceInUnion(t *testing.T) {
	// Map with nil interface value for a union field — hits the IsNil check
	// in appendAvroJSONUnion.
	s, _ := Parse(`{"type":"record","name":"R","fields":[{"name":"v","type":["null","string"]}]}`)
	data := map[string]any{"v": nil}
	b := mustEncodeJSON(t, s, data)
	var parsed map[string]any
	json.Unmarshal(b, &parsed)
	if parsed["v"] != nil {
		t.Errorf("expected null, got %v", parsed["v"])
	}
}

func TestEncodeJSONStructMappingError(t *testing.T) {
	// Struct missing a required field — hits typeFieldMapping error in record encoder.
	type R struct {
		A int `avro:"a"`
	}
	s, _ := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`)
	_, err := s.EncodeJSON(&R{A: 1})
	if err == nil {
		t.Fatal("expected error for struct missing field b")
	}
}

func TestEncodeJSONNestedError(t *testing.T) {
	// Array with wrong element type triggers error propagation.
	s, _ := Parse(`{"type":"array","items":"int"}`)
	_, err := s.EncodeJSON([]any{true}) // bool can't encode as int
	if err == nil {
		t.Fatal("expected error")
	}

	// Map with wrong value type.
	s2, _ := Parse(`{"type":"map","values":"int"}`)
	_, err = s2.EncodeJSON(map[string]any{"k": true})
	if err == nil {
		t.Fatal("expected error")
	}

	// Record with wrong field type.
	s3, _ := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	_, err = s3.EncodeJSON(map[string]any{"a": true})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDecodeJSONNullStandalone(t *testing.T) {
	s, _ := Parse(`"null"`)
	var got any
	mustDecodeJSON(t, s, []byte(`null`), &got)
}

func TestDecodeJSONNullUnionTyped(t *testing.T) {
	s, _ := Parse(`["null","string"]`)
	input := `null`
	var got *string
	mustDecodeJSON(t, s, []byte(input), &got)
	if got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestDecodeJSONArrayError(t *testing.T) {
	s, _ := Parse(`{"type":"array","items":"int"}`)
	// "hello" can't be an int.
	var got any
	err := s.DecodeJSON([]byte(`[1, "hello"]`), &got)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDeserFixedArrayNegativeBlock(t *testing.T) {
	s := mustParse(t, `{"type":"array","items":"int"}`)
	// Craft binary with negative block count (indicates byte-size follows).
	var elems []byte
	elems = appendVarint(elems, 10)
	elems = appendVarint(elems, 20)
	elems = appendVarint(elems, 30)
	var data []byte
	data = appendVarlong(data, -3)                // negative count
	data = appendVarlong(data, int64(len(elems))) // byte size
	data = append(data, elems...)
	data = append(data, 0) // terminator

	var got [3]int32
	mustDecode(t, s, data, &got)
	if got != [3]int32{10, 20, 30} {
		t.Errorf("got %v", got)
	}
}

func TestDeserFixedArrayTruncated(t *testing.T) {
	s := mustParse(t, `{"type":"array","items":"int"}`)
	// Truncated data — readVarlong fails.
	var got [3]int32
	_, err := s.Decode([]byte{}, &got)
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestDeserFixedArrayNegBlockOverflow(t *testing.T) {
	s, _ := Parse(`{"type":"array","items":"int"}`)
	// MinInt64 zigzag-encoded: negating it still gives negative.
	data := []byte{0x01} // zigzag for -1... actually need MinInt64.
	// zigzag(MinInt64) = MaxUint64 which is 0xFF 0xFF ... 0xFF 0x01 (10 bytes)
	data = appendVarlong(nil, math.MinInt64)
	var got [1]int32
	_, err := s.Decode(data, &got)
	if err == nil {
		t.Fatal("expected error for MinInt64 block count")
	}
}

func TestDeserFixedArrayNegBlockTruncatedSize(t *testing.T) {
	s, _ := Parse(`{"type":"array","items":"int"}`)
	// Negative count but truncated byte-size varlong.
	data := appendVarlong(nil, -3) // count=-3
	// No byte-size follows — truncated.
	var got [3]int32
	_, err := s.Decode(data, &got)
	if err == nil {
		t.Fatal("expected error for truncated byte size")
	}
}

func TestDeserFixedArrayItemError(t *testing.T) {
	s := mustParse(t, `{"type":"array","items":"string"}`)
	// Craft a block with count=1 but truncated string data.
	var data []byte
	data = appendVarlong(data, 1)       // 1 element
	data = appendVarlong(data, 1000000) // string length: huge, will fail
	var got [1]string
	_, err := s.Decode(data, &got)
	if err == nil {
		t.Fatal("expected error for truncated string in fixed array")
	}
}

func TestDecodeJSONNullWithNonNilInput(t *testing.T) {
	// Passing a non-null JSON value with a null schema.
	// DecodeJSON sees 42 with a null schema.
	s, _ := Parse(`"null"`)
	var got any
	err := s.DecodeJSON([]byte(`42`), &got)
	// This may error during Encode(nil) or succeed with nil.
	_ = err
	// Just verify no panic.
}

func TestDecodeJSONArrayItemError(t *testing.T) {
	// Array of union where an item has an unknown branch name.
	s, _ := Parse(`{"type":"array","items":["null","int"]}`)
	var got any
	err := s.DecodeJSON([]byte(`[{"bogus":42}]`), &got)
	if err == nil {
		t.Fatal("expected error for unknown union branch in array item")
	}
}

// TestDecodeJSONCoercionPaths exercises DecodeJSON coercion paths that normal
// tests don't reach.
func TestDecodeJSONCoercionPaths(t *testing.T) {
	// Null for null schema.
	t.Run("null schema", func(t *testing.T) {
		s, _ := Parse(`"null"`)
		var got any
		mustDecodeJSON(t, s, []byte(`null`), &got)
	})

	// Null for union.
	t.Run("null union", func(t *testing.T) {
		s, _ := Parse(`["null","string"]`)
		var got any
		mustDecodeJSON(t, s, []byte(`null`), &got)
		if got != nil {
			t.Errorf("got %v", got)
		}
	})

	// Float passthrough.
	t.Run("float", func(t *testing.T) {
		s, _ := Parse(`"float"`)
		var got any
		mustDecodeJSON(t, s, []byte(`1.5`), &got)
	})

	// Double passthrough.
	t.Run("double", func(t *testing.T) {
		s, _ := Parse(`"double"`)
		var got any
		mustDecodeJSON(t, s, []byte(`3.14`), &got)
	})
}

// TestEncodeFromJSONUseNumber tests that data from json.Decoder.UseNumber()
// (which produces json.Number instead of float64) can be encoded for every
// numeric schema type. This catches json.Number gaps in specialized serializers.
func TestEncodeFromJSONUseNumber(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		json   string
	}{
		// Primitives.
		{"int", `"int"`, `42`},
		{"long", `"long"`, `100000`},
		{"float", `"float"`, `1.5`},
		{"double", `"double"`, `3.14`},

		// Arrays.
		{"array of int", `{"type":"array","items":"int"}`, `[1,2,3]`},
		{"array of long", `{"type":"array","items":"long"}`, `[100,200]`},
		{"array of float", `{"type":"array","items":"float"}`, `[1.5]`},
		{"array of double", `{"type":"array","items":"double"}`, `[3.14]`},

		// Maps.
		{"map of int", `{"type":"map","values":"int"}`, `{"k":42}`},
		{"map of long", `{"type":"map","values":"long"}`, `{"k":100}`},
		{"map of float", `{"type":"map","values":"float"}`, `{"k":1.5}`},
		{"map of double", `{"type":"map","values":"double"}`, `{"k":3.14}`},

		// Record with numeric fields.
		{"record with int", `{"type":"record","name":"R","fields":[{"name":"n","type":"int"}]}`, `{"n":42}`},
		{"record with long", `{"type":"record","name":"R","fields":[{"name":"n","type":"long"}]}`, `{"n":100}`},
		{"record with float", `{"type":"record","name":"R","fields":[{"name":"n","type":"float"}]}`, `{"n":1.5}`},
		{"record with double", `{"type":"record","name":"R","fields":[{"name":"n","type":"double"}]}`, `{"n":3.14}`},

		// Logical types.
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, `1742385600000`},

		// Nullable numeric.
		{"nullable int", `["null","int"]`, `42`},
		{"nullable long", `["null","long"]`, `100`},

		// Record with map of long (the exact case that broke connect).
		{"record with map of long", `{"type":"record","name":"R","fields":[{"name":"m","type":{"type":"map","values":"long"}}]}`, `{"m":{"i":3}}`},

		// Nested record with numeric fields.
		{"nested numeric", `{"type":"record","name":"O","fields":[{"name":"inner","type":{"type":"record","name":"I","fields":[{"name":"x","type":"int"},{"name":"y","type":"long"}]}}]}`, `{"inner":{"x":1,"y":2}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustParse(t, tt.schema)
			// Use json.Decoder.UseNumber() to produce json.Number values.
			dec := json.NewDecoder(bytes.NewReader([]byte(tt.json)))
			dec.UseNumber()
			var native any
			if err := dec.Decode(&native); err != nil {
				t.Fatalf("json.Decode: %v", err)
			}
			binary := mustEncode(t, s, native)
			var decoded any
			mustDecode(t, s, binary, &decoded)
		})
	}
}

// TestEncodeJSONFromDecoded tests the full pipeline: binary decode → EncodeJSON.
// This catches gaps where decoded types (json.Number for decimals, int64 for
// timestamps) aren't handled by the JSON encoder.
func TestEncodeJSONFromDecoded(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		value  any // Go value to encode to binary first
	}{
		{"simple record", `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`,
			map[string]any{"a": int32(1), "b": "hello"}},
		{"record with nullable", `{"type":"record","name":"R","fields":[{"name":"v","type":["null","string"]}]}`,
			map[string]any{"v": "hello"}},
		{"record with nullable null", `{"type":"record","name":"R","fields":[{"name":"v","type":["null","string"]}]}`,
			map[string]any{"v": nil}},
		{"record with array of unions", `{"type":"record","name":"R","fields":[{"name":"items","type":{"type":"array","items":["null","string"]}}]}`,
			map[string]any{"items": []any{nil, "a", nil, "b"}}},
		{"record with map of int", `{"type":"record","name":"R","fields":[{"name":"m","type":{"type":"map","values":"int"}}]}`,
			map[string]any{"m": map[string]any{"k": int32(1)}}},
		{"nested union record", `{"type":"record","name":"R","fields":[{"name":"inner","type":["null",{"type":"record","name":"I","fields":[{"name":"x","type":"int"}]}]}]}`,
			map[string]any{"inner": map[string]any{"x": int32(42)}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			// Encode to binary.
			binary, err := s.Encode(tt.value)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			// Decode to any.
			var decoded any
			mustDecode(t, s, binary, &decoded)
			// EncodeJSON from the decoded value.
			jb, err := s.EncodeJSON(decoded)
			if err != nil {
				t.Fatalf("EncodeJSON: %v", err)
			}
			// Verify valid JSON.
			if !json.Valid(jb) {
				t.Fatalf("invalid JSON: %s", jb)
			}
			// DecodeJSON back and re-encode to binary — full round trip.
			var rt any
			mustDecodeJSON(t, s, jb, &rt)
			binary2, err := s.Encode(rt)
			if err != nil {
				t.Fatalf("re-Encode: %v", err)
			}
			// Binary should match.
			if !bytes.Equal(binary, binary2) {
				t.Errorf("binary mismatch:\n  original: %v\n  roundtrip: %v", binary, binary2)
			}
		})
	}
}

// ---------- custom_type_test.go ----------

type testMoney struct {
	Cents    int64
	Currency string
}

type testGeoPoint struct {
	Lat, Lng float64
}

type testStatus int32

var moneyCT = NewCustomType[testMoney, int64]("money",
	func(m testMoney, _ *SchemaNode) (int64, error) { return m.Cents, nil },
	func(c int64, _ *SchemaNode) (testMoney, error) { return testMoney{Cents: c, Currency: "USD"}, nil },
)

func parseMoney(t *testing.T, schema string) *Schema {
	t.Helper()
	s := mustParse(t, schema, moneyCT)
	return s
}

func TestCustomTypeRoundTrip(t *testing.T) {
	t.Run("struct", func(t *testing.T) {
		type Order struct {
			ID    int64     `avro:"id"`
			Price testMoney `avro:"price"`
		}
		s := parseMoney(t, orderIDPriceSchema)
		input := Order{ID: 1, Price: testMoney{Cents: 500, Currency: "USD"}}
		data := mustEncode(t, s, &input)
		var out Order
		mustDecode(t, s, data, &out)
		if out.ID != 1 || out.Price.Cents != 500 || out.Price.Currency != "USD" {
			t.Errorf("got %+v", out)
		}
	})

	t.Run("any", func(t *testing.T) {
		s := parseMoney(t, `{"type":"long","logicalType":"money"}`)
		data := mustEncode(t, s, testMoney{Cents: 999})
		var v any
		mustDecode(t, s, data, &v)
		if m := v.(testMoney); m.Cents != 999 {
			t.Errorf("got %d", m.Cents)
		}
	})

	t.Run("double_pointer", func(t *testing.T) {
		s := parseMoney(t, `{"type":"long","logicalType":"money"}`)
		data, _ := s.Encode(int64(55))
		var out *testMoney
		mustDecode(t, s, data, &out)
		if out == nil || out.Cents != 55 {
			t.Fatalf("got %+v", out)
		}
	})
}

func TestCustomTypeJSON(t *testing.T) {
	s := parseMoney(t, `{"type":"long","logicalType":"money"}`)

	t.Run("encode", func(t *testing.T) {
		j := mustEncodeJSON(t, s, testMoney{Cents: 1234})
		if string(j) != "1234" {
			t.Errorf("got %s", j)
		}
	})

	t.Run("decode", func(t *testing.T) {
		var v any
		mustDecodeJSON(t, s, []byte("5678"), &v)
		if m := v.(testMoney); m.Cents != 5678 {
			t.Errorf("got %d", m.Cents)
		}
	})
}

func TestCustomTypeSchemaFor(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		type Order struct {
			Price testMoney `avro:"price"`
		}
		s := mustSchemaFor[Order](t, moneyCT)
		f := s.Root().Fields[0]
		if f.Type.Type != "long" || f.Type.LogicalType != "money" {
			t.Errorf("got type=%q logical=%q", f.Type.Type, f.Type.LogicalType)
		}
	})

	t.Run("pointer", func(t *testing.T) {
		type R struct {
			Price *testMoney `avro:"price"`
		}
		s := mustSchemaFor[R](t, moneyCT)
		var m map[string]any
		json.Unmarshal([]byte(s.String()), &m)
		typ := m["fields"].([]any)[0].(map[string]any)["type"].([]any)
		if len(typ) != 2 || typ[0] != "null" {
			t.Fatalf("expected nullable union, got %v", typ)
		}
		inner := typ[1].(map[string]any)
		if inner["type"] != "long" || inner["logicalType"] != "money" {
			t.Errorf("got %v", inner)
		}
	})

	t.Run("with_schema", func(t *testing.T) {
		type Data struct {
			Addr testGeoPoint `avro:"addr"`
		}
		s := mustSchemaFor[Data](t, CustomType{
			GoType: reflect.TypeFor[testGeoPoint](),
			Schema: &SchemaNode{Type: "fixed", Name: "geo", Size: 16},
		})
		f := s.Root().Fields[0]
		if f.Type.Type != "fixed" || f.Type.Size != 16 {
			t.Errorf("got type=%q size=%d", f.Type.Type, f.Type.Size)
		}
	})
}

func TestCustomTypeOverrideBuiltIn(t *testing.T) {
	s, err := Parse(`{"type":"long","logicalType":"timestamp-millis"}`, CustomType{
		LogicalType: "timestamp-millis",
		GoType:      reflect.TypeFor[time.Time](),
		Encode: func(v any, _ *SchemaNode) (any, error) {
			return v.(time.Time).UnixMilli(), nil
		},
		Decode: func(v any, _ *SchemaNode) (any, error) {
			return v, nil // pass through raw int64
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Millisecond).UTC()
	data, _ := s.Encode(now)
	var v any
	s.Decode(data, &v)
	if v.(int64) != now.UnixMilli() {
		t.Errorf("got %d, want %d", v, now.UnixMilli())
	}
}

func TestCustomTypeNullableUnion(t *testing.T) {
	type R struct {
		Price *testMoney `avro:"price"`
		Name  string     `avro:"name"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"price","type":["null",{"type":"long","logicalType":"money"}]},
		{"name":"name","type":"string"}
	]}`
	s := parseMoney(t, schema)

	t.Run("non_null", func(t *testing.T) {
		m := testMoney{Cents: 999}
		data := mustEncode(t, s, &R{Price: &m, Name: "test"})
		var out R
		mustDecode(t, s, data, &out)
		if out.Price == nil || out.Price.Cents != 999 || out.Name != "test" {
			t.Errorf("got %+v", out)
		}
		// Encode again to exercise fast-path compilation.
		data2, _ := s.Encode(&R{Price: &m, Name: "t2"})
		var out2 R
		s.Decode(data2, &out2)
		if out2.Name != "t2" {
			t.Errorf("got %q", out2.Name)
		}
	})

	t.Run("null", func(t *testing.T) {
		data, _ := s.Encode(&R{Price: nil, Name: "x"})
		var out R
		s.Decode(data, &out)
		if out.Price != nil {
			t.Errorf("got %+v, want nil", out.Price)
		}
	})

	t.Run("any_target", func(t *testing.T) {
		s2 := parseMoney(t, `{"type":"record","name":"R2","fields":[
			{"name":"v","type":["null",{"type":"long","logicalType":"money"}]}
		]}`)
		data, _ := s2.Encode(map[string]any{"v": int64(100)})
		var v any
		s2.Decode(data, &v)
		if m := v.(map[string]any); m["v"].(testMoney).Cents != 100 {
			t.Errorf("got %v", m["v"])
		}
	})
}

func TestCustomTypeErrors(t *testing.T) {
	t.Run("decode_fatal", func(t *testing.T) {
		myErr := errors.New("boom")
		s, _ := Parse(`{"type":"long","logicalType":"money"}`, CustomType{
			LogicalType: "money",
			Decode:      func(any, *SchemaNode) (any, error) { return nil, myErr },
		})
		data, _ := s.Encode(int64(1))
		var v any
		_, err := s.Decode(data, &v)
		if !errors.Is(err, myErr) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("encode_fatal", func(t *testing.T) {
		myErr := errors.New("encode boom")
		s, _ := Parse(`{"type":"long","logicalType":"money"}`, CustomType{
			LogicalType: "money",
			GoType:      reflect.TypeFor[testMoney](),
			Encode:      func(any, *SchemaNode) (any, error) { return nil, myErr },
		})
		_, err := s.Encode(testMoney{Cents: 1})
		if !errors.Is(err, myErr) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("encode_json_fatal", func(t *testing.T) {
		myErr := errors.New("json boom")
		s, _ := Parse(`{"type":"long","logicalType":"money"}`, CustomType{
			LogicalType: "money",
			GoType:      reflect.TypeFor[testMoney](),
			Encode:      func(any, *SchemaNode) (any, error) { return nil, myErr },
		})
		_, err := s.EncodeJSON(testMoney{Cents: 1})
		if !errors.Is(err, myErr) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("encode_skip", func(t *testing.T) {
		s, _ := Parse(`{"type":"long","logicalType":"money"}`, CustomType{
			LogicalType: "money",
			GoType:      reflect.TypeFor[testMoney](),
			Encode:      func(any, *SchemaNode) (any, error) { return nil, ErrSkipCustomType },
		})
		_, err := s.Encode(testMoney{Cents: 1})
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("decode_nil_result", func(t *testing.T) {
		s, _ := Parse(`{"type":"long","logicalType":"money"}`, CustomType{
			LogicalType: "money",
			Decode:      func(any, *SchemaNode) (any, error) { return nil, nil },
		})
		data, _ := s.Encode(int64(42))
		var v any
		s.Decode(data, &v)
		if v != nil {
			t.Errorf("got %v", v)
		}
	})

	t.Run("decode_short_buffer", func(t *testing.T) {
		s := parseMoney(t, `{"type":"long","logicalType":"money"}`)
		var v any
		_, err := s.Decode(nil, &v)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("invalid_avro_type", func(t *testing.T) {
		type Bad struct{}
		ct := NewCustomType[Bad, complex128]("bad",
			func(Bad, *SchemaNode) (complex128, error) { return 0, nil },
			func(complex128, *SchemaNode) (Bad, error) { return Bad{}, nil },
		)
		_, err := Parse(`{"type":"long","logicalType":"bad"}`, ct)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestCustomTypeMatching(t *testing.T) {
	t.Run("first_match_wins", func(t *testing.T) {
		s, _ := Parse(`{"type":"long","logicalType":"money"}`,
			NewCustomType[testMoney, int64]("money", nil,
				func(c int64, _ *SchemaNode) (testMoney, error) { return testMoney{Cents: c, Currency: "FIRST"}, nil },
			),
			NewCustomType[testMoney, int64]("money", nil,
				func(c int64, _ *SchemaNode) (testMoney, error) { return testMoney{Cents: c, Currency: "SECOND"}, nil },
			),
		)
		data, _ := s.Encode(int64(100))
		var v any
		s.Decode(data, &v)
		if v.(testMoney).Currency != "FIRST" {
			t.Errorf("got %q", v.(testMoney).Currency)
		}
	})

	t.Run("avro_type_mismatch", func(t *testing.T) {
		s, _ := Parse(`{"type":"long","logicalType":"money"}`, CustomType{
			LogicalType: "money",
			AvroType:    "string",
			Decode:      func(any, *SchemaNode) (any, error) { return "bad", nil },
		})
		data, _ := s.Encode(int64(42))
		var v any
		s.Decode(data, &v)
		if _, ok := v.(int64); !ok {
			t.Fatalf("expected int64, got %T", v)
		}
	})

	t.Run("encode_gotype_skip", func(t *testing.T) {
		s := parseMoney(t, `{"type":"long","logicalType":"money"}`)
		// Raw int64 → GoType doesn't match → passes through.
		data := mustEncode(t, s, int64(42))
		if len(data) == 0 {
			t.Fatal("empty")
		}
	})

	t.Run("nil_encode_func", func(t *testing.T) {
		s, _ := Parse(`{"type":"long","logicalType":"money"}`,
			NewCustomType[testMoney, int64]("money", nil,
				func(c int64, _ *SchemaNode) (testMoney, error) { return testMoney{Cents: c}, nil },
			),
		)
		data, _ := s.Encode(int64(42))
		var v any
		s.Decode(data, &v)
		if v.(testMoney).Cents != 42 {
			t.Errorf("got %d", v.(testMoney).Cents)
		}
	})

	t.Run("nil_decode_func", func(t *testing.T) {
		s, _ := Parse(`{"type":"long","logicalType":"money"}`,
			NewCustomType[testMoney, int64]("money",
				func(m testMoney, _ *SchemaNode) (int64, error) { return m.Cents, nil }, nil,
			),
		)
		data, _ := s.Encode(testMoney{Cents: 77})
		var v any
		s.Decode(data, &v)
		if _, ok := v.(int64); !ok {
			t.Fatalf("expected int64, got %T", v)
		}
	})

	t.Run("skip_fallthrough", func(t *testing.T) {
		s, _ := Parse(`{"type":"long"}`, CustomType{
			AvroType: "long",
			Decode:   func(any, *SchemaNode) (any, error) { return nil, ErrSkipCustomType },
		})
		data, _ := s.Encode(int64(42))
		var v any
		s.Decode(data, &v)
		if v.(int64) != 42 {
			t.Errorf("got %v", v)
		}
	})

	t.Run("empty_criteria", func(t *testing.T) {
		calls := 0
		s, _ := Parse(recABSchema, CustomType{
			Decode: func(any, *SchemaNode) (any, error) { calls++; return nil, ErrSkipCustomType },
		})
		data, _ := s.Encode(map[string]any{"a": int32(1), "b": "hello"})
		var v any
		s.Decode(data, &v)
		if calls == 0 {
			t.Error("expected calls")
		}
	})

	t.Run("wildcard_preserves_builtins", func(t *testing.T) {
		s, _ := Parse(`{"type":"long","logicalType":"timestamp-millis"}`, CustomType{
			Decode: func(any, *SchemaNode) (any, error) { return nil, ErrSkipCustomType },
		})
		data, _ := s.Encode(int64(1687221496000))
		var v any
		s.Decode(data, &v)
		if _, ok := v.(time.Time); !ok {
			t.Fatalf("expected time.Time, got %T", v)
		}
	})
}

func TestCustomTypeBackedByRecord(t *testing.T) {
	s, _ := Parse(`{"type":"record","name":"R","fields":[
		{"name":"loc","type":{"type":"record","name":"Loc","logicalType":"geo",
			"fields":[{"name":"lat","type":"double"},{"name":"lng","type":"double"}]
		}}
	]}`, CustomType{
		LogicalType: "geo",
		AvroType:    "record",
		GoType:      reflect.TypeFor[testGeoPoint](),
		Encode: func(v any, _ *SchemaNode) (any, error) {
			g := v.(testGeoPoint)
			return map[string]any{"lat": g.Lat, "lng": g.Lng}, nil
		},
		Decode: func(v any, _ *SchemaNode) (any, error) {
			m := v.(map[string]any)
			return testGeoPoint{Lat: m["lat"].(float64), Lng: m["lng"].(float64)}, nil
		},
	})
	data, _ := s.Encode(map[string]any{"loc": testGeoPoint{Lat: 37.7749, Lng: -122.4194}})
	var v any
	s.Decode(data, &v)
	g := v.(map[string]any)["loc"].(testGeoPoint)
	if math.Abs(g.Lat-37.7749) > 0.0001 || math.Abs(g.Lng+122.4194) > 0.0001 {
		t.Errorf("got %+v", g)
	}
}

func TestCustomTypeSchemaProps(t *testing.T) {
	s, _ := Parse(`{"type":"record","name":"R","fields":[
		{"name":"ts","type":{"type":"long","connect.name":"io.debezium.time.Timestamp"}}
	]}`, CustomType{
		Decode: func(v any, sn *SchemaNode) (any, error) {
			if sn.Props["connect.name"] == "io.debezium.time.Timestamp" {
				return time.UnixMilli(v.(int64)).UTC(), nil
			}
			return nil, ErrSkipCustomType
		},
	})
	data, _ := s.Encode(map[string]any{"ts": int64(1687221496000)})
	var v any
	s.Decode(data, &v)
	if ts := v.(map[string]any)["ts"].(time.Time); ts.UnixMilli() != 1687221496000 {
		t.Errorf("got %v", ts)
	}
}

func TestCustomTypeSchemaCache(t *testing.T) {
	t.Run("no_leak", func(t *testing.T) {
		var cache SchemaCache
		s1, _ := cache.Parse(`{"type":"long","logicalType":"money"}`)
		s2, _ := cache.Parse(`{"type":"long","logicalType":"money"}`, moneyCT)
		if s1 == s2 {
			t.Error("should not return cached schema for custom parse")
		}
		data, _ := s1.Encode(int64(42))
		var v1, v2 any
		s1.Decode(data, &v1)
		s2.Decode(data, &v2)
		if _, ok := v1.(int64); !ok {
			t.Errorf("s1: expected int64, got %T", v1)
		}
		if _, ok := v2.(testMoney); !ok {
			t.Errorf("s2: expected testMoney, got %T", v2)
		}
		s3, _ := cache.Parse(`{"type":"long","logicalType":"money"}`)
		if s1 != s3 {
			t.Error("expected cached schema")
		}
	})

	t.Run("reparse", func(t *testing.T) {
		var cache SchemaCache
		s1 := mustCacheParse(t, &cache, orderPriceSchema, moneyCT)
		s2 := mustCacheParse(t, &cache, orderPriceSchema, moneyCT)
		data, _ := s1.Encode(map[string]any{"price": testMoney{Cents: 1}})
		var v1, v2 any
		s1.Decode(data, &v1)
		s2.Decode(data, &v2)
		if v1.(map[string]any)["price"].(testMoney).Cents != 1 {
			t.Error("s1 failed")
		}
		if v2.(map[string]any)["price"].(testMoney).Cents != 1 {
			t.Error("s2 failed")
		}
	})
}

func TestCustomTypeResolve(t *testing.T) {
	t.Run("promotion", func(t *testing.T) {
		writer, _ := Parse(`"int"`)
		reader := parseMoney(t, `{"type":"long","logicalType":"money"}`)
		resolved, _ := Resolve(writer, reader)
		data, _ := writer.Encode(int32(500))
		var v any
		resolved.Decode(data, &v)
		if v.(testMoney).Cents != 500 {
			t.Errorf("got %d", v.(testMoney).Cents)
		}
	})

	t.Run("same_kind", func(t *testing.T) {
		writer := parseMoney(t, `{"type":"long","logicalType":"money"}`)
		reader := parseMoney(t, `{"type":"long","logicalType":"money"}`)
		resolved, _ := Resolve(writer, reader)
		data, _ := writer.Encode(testMoney{Cents: 42})
		var v any
		resolved.Decode(data, &v)
		if v.(testMoney).Cents != 42 {
			t.Errorf("got %d", v.(testMoney).Cents)
		}
	})
}

func TestNewCustomTypeAllAvroTypes(t *testing.T) {
	tests := []struct {
		name, want string
		ct         CustomType
	}{
		{"bool", "boolean", NewCustomType[testStatus, bool]("b", nil, nil)},
		{"int32", "int", NewCustomType[testStatus, int32]("i", nil, nil)},
		{"int64", "long", NewCustomType[testStatus, int64]("l", nil, nil)},
		{"float32", "float", NewCustomType[testStatus, float32]("f", nil, nil)},
		{"float64", "double", NewCustomType[testStatus, float64]("d", nil, nil)},
		{"string", "string", NewCustomType[testStatus, string]("s", nil, nil)},
		{"bytes", "bytes", NewCustomType[testStatus, []byte]("b2", nil, nil)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.ct.AvroType != tt.want {
				t.Errorf("got %q", tt.ct.AvroType)
			}
		})
	}
}

func TestCustomTypeSchemaCacheNonCustomAfterCustom(t *testing.T) {
	var cache SchemaCache
	_, err := cache.Parse(orderPriceSchema, moneyCT)
	if err != nil {
		t.Fatalf("custom parse: %v", err)
	}
	s, err := cache.Parse(orderPriceSchema)
	if err != nil {
		t.Fatalf("non-custom parse after custom: %v", err)
	}
	data, _ := s.Encode(map[string]any{"price": int64(42)})
	var v any
	s.Decode(data, &v)
	if v.(map[string]any)["price"].(int64) != 42 {
		t.Errorf("got %v", v)
	}
}

func TestCustomTypePointerGoType(t *testing.T) {
	type Wrapper struct{ V string }
	ct := CustomType{
		LogicalType: "wrapped",
		AvroType:    "string",
		GoType:      reflect.TypeFor[*Wrapper](),
		Encode: func(v any, _ *SchemaNode) (any, error) {
			return v.(*Wrapper).V, nil
		},
		Decode: func(v any, _ *SchemaNode) (any, error) {
			return &Wrapper{V: v.(string)}, nil
		},
	}
	s := mustParse(t, `{"type":"string","logicalType":"wrapped"}`, ct)
	w := &Wrapper{V: "hello"}
	data := mustEncode(t, s, w)

	// Decode into *any — exercises the customEncode pointer-level GoType match.
	var out any
	mustDecode(t, s, data, &out)
	if got := out.(*Wrapper).V; got != "hello" {
		t.Errorf("any: got %q", got)
	}

	// Decode into typed *Wrapper — exercises setCustomResult AssignableTo
	// for pointer-valued results into pointer targets.
	var typed *Wrapper
	if _, err := s.Decode(data, &typed); err != nil {
		t.Fatalf("typed decode: %v", err)
	}
	if typed == nil || typed.V != "hello" {
		t.Errorf("typed: got %+v", typed)
	}
}

func TestCustomTypePointerGoTypeEncodeError(t *testing.T) {
	type Wrapper struct{ V string }
	myErr := errors.New("ptr encode fail")
	ct := CustomType{
		LogicalType: "wrapped",
		AvroType:    "string",
		GoType:      reflect.TypeFor[*Wrapper](),
		Encode: func(v any, _ *SchemaNode) (any, error) {
			return nil, myErr
		},
	}
	s, _ := Parse(`{"type":"string","logicalType":"wrapped"}`, ct)
	_, err := s.Encode(&Wrapper{V: "x"})
	if !errors.Is(err, myErr) {
		t.Fatalf("expected myErr, got %v", err)
	}
}

func TestCustomTypePointerGoTypeEncodeSkip(t *testing.T) {
	type Wrapper struct{ V string }
	ct := CustomType{
		LogicalType: "wrapped",
		AvroType:    "string",
		GoType:      reflect.TypeFor[*Wrapper](),
		Encode: func(v any, _ *SchemaNode) (any, error) {
			return nil, ErrSkipCustomType
		},
	}
	s, _ := Parse(`{"type":"string","logicalType":"wrapped"}`, ct)
	// Pointer GoType match, but encoder skips → falls through to raw
	// string ser which fails for *Wrapper.
	_, err := s.Encode(&Wrapper{V: "x"})
	if err == nil {
		t.Fatal("expected error after skip")
	}
}

func TestCustomTypeNilPointerEncode(t *testing.T) {
	// Nil pointer value should pass through without panic.
	s, _ := Parse(`{"type":"string","logicalType":"wrapped"}`, CustomType{
		LogicalType: "wrapped",
		GoType:      reflect.TypeFor[*testMoney](),
		Encode: func(v any, _ *SchemaNode) (any, error) {
			return "converted", nil
		},
	})
	// Encode nil *testMoney via a map with nil value.
	_, err := s.Encode((*testMoney)(nil))
	// Should not panic. May error (nil for non-null string) but not panic.
	_ = err
}

func TestWithCustomTypeWrapper(t *testing.T) {
	// Exercises the WithCustomType discoverability wrapper.
	ct := NewCustomType[testMoney, int64]("money",
		func(m testMoney, _ *SchemaNode) (int64, error) { return m.Cents, nil },
		func(c int64, _ *SchemaNode) (testMoney, error) { return testMoney{Cents: c}, nil },
	)
	s := mustParse(t, `{"type":"long","logicalType":"money"}`, WithCustomType(ct))
	data, _ := s.Encode(testMoney{Cents: 1})
	var v any
	s.Decode(data, &v)
	if v.(testMoney).Cents != 1 {
		t.Errorf("got %v", v)
	}
}

func TestCustomTypeArrayFastPathDisabled(t *testing.T) {
	// Exercises schema.go array hasCustomType path.
	s := mustParse(t, `{"type":"array","items":{"type":"long","logicalType":"money"}}`, moneyCT)
	data := mustEncode(t, s, []testMoney{{Cents: 1}, {Cents: 2}})
	var out any
	mustDecode(t, s, data, &out)
	arr := out.([]any)
	if len(arr) != 2 || arr[0].(testMoney).Cents != 1 || arr[1].(testMoney).Cents != 2 {
		t.Errorf("got %v", arr)
	}
}

func TestCustomTypeMapFastPathDisabled(t *testing.T) {
	s := mustParse(t, `{"type":"map","values":{"type":"long","logicalType":"money"}}`, moneyCT)
	data := mustEncode(t, s, map[string]testMoney{"a": {Cents: 10}})
	var out any
	mustDecode(t, s, data, &out)
	m := out.(map[string]any)
	if m["a"].(testMoney).Cents != 10 {
		t.Errorf("got %v", m)
	}
}

// An AvroType-only CustomType (no logicalType) must fire on the JSON
// array/map element paths exactly as it does on binary. The binary
// fast-path gate disables specialization when the element carries a custom
// type (meta.hasCustomType); the JSON fast-path gate previously checked only
// logical=="" and emitted/parsed the raw element, silently skipping the
// custom codec — a binary↔JSON wire divergence. (The existing
// TestCustomType{Array,Map}FastPathDisabled use a logicalType-bearing custom
// type, so logical!="" also tripped the JSON gate and masked this gap.)
func TestCustomTypeJSONArrayAvroTypeOnly(t *testing.T) {
	ct := CustomType{
		AvroType: "long",
		Encode:   func(v any, _ *SchemaNode) (any, error) { return v.(int64) + 1000, nil },
		Decode:   func(v any, _ *SchemaNode) (any, error) { return v.(int64) - 1000, nil },
	}
	s := mustParse(t, `{"type":"array","items":"long"}`, ct)
	in := []int64{5, 6}
	bin := mustEncode(t, s, in)
	js := mustEncodeJSON(t, s, in)
	// Read the raw wire values each encoder wrote, via a no-custom schema.
	plain := MustParse(`{"type":"array","items":"long"}`)
	var rawBin, rawJSON []int64
	mustDecode(t, plain, bin, &rawBin)
	mustDecodeJSON(t, plain, js, &rawJSON)
	want := []int64{1005, 1006} // custom Encode added 1000
	if !reflect.DeepEqual(rawBin, want) {
		t.Fatalf("binary raw wire = %v, want %v", rawBin, want)
	}
	if !reflect.DeepEqual(rawJSON, want) {
		t.Fatalf("json raw wire = %v, want %v (custom Encode skipped on JSON array fast path)", rawJSON, want)
	}
	// JSON decode must apply the custom Decode (subtract 1000).
	var out []int64
	mustDecodeJSON(t, s, js, &out)
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("json decode = %v, want %v (custom Decode skipped on JSON array fast path)", out, in)
	}
}

func TestCustomTypeJSONMapAvroTypeOnly(t *testing.T) {
	ct := CustomType{
		AvroType: "long",
		Encode:   func(v any, _ *SchemaNode) (any, error) { return v.(int64) + 1000, nil },
		Decode:   func(v any, _ *SchemaNode) (any, error) { return v.(int64) - 1000, nil },
	}
	s := mustParse(t, `{"type":"map","values":"long"}`, ct)
	in := map[string]int64{"a": 5}
	bin := mustEncode(t, s, in)
	js := mustEncodeJSON(t, s, in)
	plain := MustParse(`{"type":"map","values":"long"}`)
	var rawBin, rawJSON map[string]int64
	mustDecode(t, plain, bin, &rawBin)
	mustDecodeJSON(t, plain, js, &rawJSON)
	want := map[string]int64{"a": 1005}
	if !reflect.DeepEqual(rawBin, want) {
		t.Fatalf("binary raw wire = %v, want %v", rawBin, want)
	}
	if !reflect.DeepEqual(rawJSON, want) {
		t.Fatalf("json raw wire = %v, want %v (custom Encode skipped on JSON map fast path)", rawJSON, want)
	}
	var out map[string]int64
	mustDecodeJSON(t, s, js, &out)
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("json decode = %v, want %v (custom Decode skipped on JSON map fast path)", out, in)
	}
}

func TestCustomTypeFixedLogicalType(t *testing.T) {
	// Exercises hasMatchingCustomType("fixed", logical) path.
	type PackedID [8]byte
	ct := CustomType{
		LogicalType: "packed-id",
		AvroType:    "fixed",
		GoType:      reflect.TypeFor[string](),
		Encode: func(v any, _ *SchemaNode) (any, error) {
			s := v.(string)
			var b [8]byte
			copy(b[:], s)
			return b[:], nil
		},
		Decode: func(v any, _ *SchemaNode) (any, error) {
			b := v.([]byte)
			return string(b), nil
		},
	}
	s := mustParse(t, `{"type":"fixed","name":"pid","size":8,"logicalType":"packed-id"}`, ct)
	data := mustEncode(t, s, "hello!!!")
	var v any
	mustDecode(t, s, data, &v)
	if v.(string) != "hello!!!" {
		t.Errorf("got %q", v)
	}
}

func TestCustomTypeJsonNumberInt64Validation(t *testing.T) {
	// Exercises jsonNumberToInt64 non-whole-number error.
	s := mustParse(t, `{"type":"array","items":"long"}`)
	_, err := s.Encode([]any{json.Number("1.5")})
	if err == nil {
		t.Fatal("expected error for non-whole json.Number in long array")
	}
}

func TestCustomTypeDecodeIntIntoAny(t *testing.T) {
	// Exercises setIntValue interface path through custom decode wrapper.
	ct := CustomType{
		AvroType: "int",
		Decode: func(v any, _ *SchemaNode) (any, error) {
			return v, nil // pass through raw int32
		},
	}
	s := mustParse(t, `"int"`, ct)
	data, _ := s.Encode(int32(42))
	var v any
	mustDecode(t, s, data, &v)
	if v.(int32) != 42 {
		t.Errorf("got %v", v)
	}
}

// TestMatrix_DecodeJSONFillsDefaultThroughCustomDecoder locks that DecodeJSON
// applies a registered CustomType.Decode to a record field's default when the
// field is absent — matching binary, where the pre-encoded defaultBytes
// round-trip through the same wrapped fn as a present field's wire bytes.
// Without it, applyFieldDefault dispatched through the UNWRAPPED deser, built
// before applyCustomTypes installed the chain, so the raw Avro-native value
// reached a target expecting the user's domain type. Subtests cover the three
// iterateRecordFields entry points and each pairs the JSON decode with its
// binary round-trip equivalent.
func TestMatrix_DecodeJSONFillsDefaultThroughCustomDecoder(t *testing.T) {
	s := parseMoney(t, `{"type":"record","name":"R","fields":[
		{"name":"price","type":{"type":"long","logicalType":"money"},"default":42}
	]}`)

	t.Run("into_struct", func(t *testing.T) {
		type R struct {
			Price testMoney `avro:"price"`
		}
		// Binary parity (default is encoded into wire then decoded through
		// the wrapped deser): produces the user's domain type.
		wire := mustAppendEncode(t, s, nil, map[string]any{})
		var rBin R
		mustDecode(t, s, wire, &rBin)
		if rBin.Price.Cents != 42 || rBin.Price.Currency != "USD" {
			t.Fatalf("binary: Price=%+v, want {Cents:42, Currency:USD}", rBin.Price)
		}

		// JSON decode of empty object must materialize the same value.
		var rJSON R
		mustDecodeJSON(t, s, []byte(`{}`), &rJSON)
		if rJSON.Price != rBin.Price {
			t.Fatalf("JSON Price=%+v, want %+v (binary parity)", rJSON.Price, rBin.Price)
		}
	})

	t.Run("into_any", func(t *testing.T) {
		var v any
		mustDecodeJSON(t, s, []byte(`{}`), &v)
		got, ok := v.(map[string]any)
		if !ok {
			t.Fatalf("decoded into %T, want map[string]any", v)
		}
		price, ok := got["price"].(testMoney)
		if !ok {
			t.Fatalf("price: got %T %#v, want testMoney", got["price"], got["price"])
		}
		if price.Cents != 42 || price.Currency != "USD" {
			t.Fatalf("price=%+v, want {Cents:42, Currency:USD}", price)
		}
	})

	t.Run("into_map_string_any", func(t *testing.T) {
		var got map[string]any
		mustDecodeJSON(t, s, []byte(`{}`), &got)
		price, ok := got["price"].(testMoney)
		if !ok {
			t.Fatalf("price: got %T %#v, want testMoney", got["price"], got["price"])
		}
		if price.Cents != 42 {
			t.Fatalf("price.Cents=%d, want 42", price.Cents)
		}
	})

	t.Run("partial_fill_present_and_default", func(t *testing.T) {
		// One field present, one filled from default — both must produce
		// the user's domain type through the custom decoder.
		s := parseMoney(t, `{"type":"record","name":"R","fields":[
			{"name":"price","type":{"type":"long","logicalType":"money"},"default":42},
			{"name":"shipping","type":{"type":"long","logicalType":"money"},"default":7}
		]}`)
		type R struct {
			Price    testMoney `avro:"price"`
			Shipping testMoney `avro:"shipping"`
		}
		var r R
		mustDecodeJSON(t, s, []byte(`{"price":100}`), &r)
		if r.Price.Cents != 100 || r.Shipping.Cents != 7 {
			t.Fatalf("got %+v, want Price.Cents=100 (present) Shipping.Cents=7 (default)", r)
		}
	})
}

// TestRegression_EncodeJSONBypassesCustomEncoderForDefaultFill locks that
// AppendEncodeJSON does NOT invoke a registered CustomType.Encode for
// default-filled record fields, matching binary's encodeDefault. CustomType.Encode
// converts user-Go-type → Avro-native, and the parsed default is already
// Avro-native, never having had a Go-domain representation, so the directional
// contract has nothing to apply. Pre-fix, appendJSONFieldDefault routed defaults
// through appendAvroJSON with a non-nil custom map, firing the user's Encode
// once per default-filled field where binary fired it zero times — benign for
// GoType-typed encoders that fall through on a type-assertion miss, but a
// surprise for the GoType=nil encoders used for logging or dispatch.
func TestRegression_EncodeJSONBypassesCustomEncoderForDefaultFill(t *testing.T) {
	// GoType=nil so the encoder fires on every value reaching the long+
	// money node — instrumentation pattern that surfaces the asymmetry.
	calls := 0
	ct := CustomType{
		LogicalType: "money",
		AvroType:    "long",
		Encode: func(v any, _ *SchemaNode) (any, error) {
			calls++
			return v, nil
		},
	}
	s := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"f","type":{"type":"long","logicalType":"money"},"default":42}
	]}`, ct)

	calls = 0
	mustAppendEncode(t, s, nil, map[string]any{})
	binaryCalls := calls
	if binaryCalls != 0 {
		t.Fatalf("binary AppendEncode fired the user encoder %d times on default-fill; defaults bypass encodeDefault", binaryCalls)
	}

	calls = 0
	mustAppendEncodeJSON(t, s, nil, map[string]any{})
	if calls != 0 {
		t.Fatalf("AppendEncodeJSON fired the user encoder %d times on default-fill; must match binary (0)", calls)
	}

	// User-supplied values still fire the encoder on both paths. Lock
	// that the bypass only applies to defaults.
	calls = 0
	if _, err := s.AppendEncodeJSON(nil, map[string]any{"f": int64(99)}); err != nil {
		t.Fatalf("AppendEncodeJSON with present field: %v", err)
	}
	if calls != 1 {
		t.Fatalf("AppendEncodeJSON with present field fired the user encoder %d times, want 1", calls)
	}
}

// The decode-side companion to the encoder default-fill bypass: when a reader
// field is ABSENT from the writer and filled from its default through
// resolution, the field's custom Decode must fire EXACTLY ONCE on the SAME raw
// (logical-suppressed) Avro-native value a natural decode would feed it, on both
// resolved wires. This pins resolveRecord's default-fill deser construction: the
// reader field node's deser is the raw, logical-suppressed one, with the custom
// chain wrapped onto it once. A double-wrap, or feeding the callback the
// enriched logical value, are the two regressions guarded; the x10 transform
// makes "the callback fired" distinguishable from a coincidental raw coercion.
func TestRegression_ResolvedDefaultFillFiresCustomDecodeOnceRaw(t *testing.T) {
	var decodeCalls int
	var lastIn any
	ct := CustomType{
		LogicalType: "money",
		AvroType:    "long",
		Decode: func(v any, _ *SchemaNode) (any, error) {
			decodeCalls++
			lastIn = v
			return v.(int64) * 10, nil
		},
	}
	reader := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"f","type":{"type":"long","logicalType":"money"},"default":42}
	]}`, ct)
	writer := MustParse(`{"type":"record","name":"R","fields":[]}`)
	res, err := Resolve(writer, reader)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	// Reference: a NATURAL decode of an explicit f=42 fires the custom once on
	// the raw int64(42) (the money logical is suppressed because a CustomType
	// matched) and yields the ×10 transform.
	wireExplicit, err := reader.AppendEncode(nil, map[string]any{"f": int64(42)})
	if err != nil {
		t.Fatalf("encode explicit: %v", err)
	}
	decodeCalls, lastIn = 0, nil
	var natOut map[string]any
	if _, err := reader.Decode(wireExplicit, &natOut); err != nil {
		t.Fatalf("natural decode: %v", err)
	}
	if decodeCalls != 1 {
		t.Fatalf("natural decode fired custom %d times, want 1", decodeCalls)
	}
	if _, ok := lastIn.(int64); !ok {
		t.Fatalf("natural custom got %T, want raw int64 (logical suppressed)", lastIn)
	}
	if natOut["f"] != int64(420) {
		t.Fatalf("natural out[f]=%v, want int64(420) (×10 of raw 42)", natOut["f"])
	}

	// Default-fill through Resolve must match the natural reference on both
	// resolved wire formats: writer omits f, reader fills default 42.
	wBin, err := writer.AppendEncode(nil, map[string]any{})
	if err != nil {
		t.Fatalf("encode empty bin: %v", err)
	}
	wJSON, err := writer.AppendEncodeJSON(nil, map[string]any{})
	if err != nil {
		t.Fatalf("encode empty json: %v", err)
	}
	for _, tc := range []struct {
		name   string
		decode func() (map[string]any, error)
	}{
		{"binary", func() (map[string]any, error) { var m map[string]any; _, e := res.Decode(wBin, &m); return m, e }},
		{"json", func() (map[string]any, error) { m := map[string]any{}; e := res.DecodeJSON(wJSON, &m); return m, e }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decodeCalls, lastIn = 0, nil
			out, err := tc.decode()
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if decodeCalls != 1 {
				t.Fatalf("default-fill fired custom Decode %d times, want exactly 1 (double-wrap or skip)", decodeCalls)
			}
			if _, ok := lastIn.(int64); !ok {
				t.Fatalf("default-fill custom got %T, want raw int64 matching natural decode (logical suppressed)", lastIn)
			}
			if out["f"] != int64(420) {
				t.Fatalf("default-fill out[f]=%T(%v), want int64(420) — the ×10 transform of raw default 42, identical to natural decode", out["f"], out["f"])
			}
		})
	}
}

// A custom-decoded value whose decode TARGET is a recursive pointer type
// (cyclic type graph: ctRecursivePtr's element is itself) must terminate with
// an error, not loop forever allocating a pointer level per iteration.
// setCustomResult's pointer walk is bounded by maxIndirectDepth — the same
// ceiling the non-custom indirect/indirectAlloc decode path uses, which
// already errors for this target (so registering a CustomType must not turn a
// clean error into an unbounded loop). Watchdog so a regression fails by
// timeout rather than hanging the suite.
type ctRecursivePtr *ctRecursivePtr

func TestRegression_CustomDecodeBoundsRecursivePointerTarget(t *testing.T) {
	s := mustParse(t, `"long"`, CustomType{
		AvroType: "long",
		Decode:   func(v any, _ *SchemaNode) (any, error) { return v, nil },
	})
	wire := mustEncode(t, s, int64(5))
	done := make(chan error, 1)
	go func() {
		var p ctRecursivePtr
		_, derr := s.Decode(wire, &p)
		done <- derr
	}()
	select {
	case derr := <-done:
		if derr == nil {
			t.Fatal("decode into recursive pointer target must error, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("decode into recursive pointer target did not terminate (setCustomResult pointer walk unbounded)")
	}
}

// ---------- callback_contract_matrix_test.go ----------

// User-supplied callback contract matrix: every point where the codecs do
// arithmetic, slicing, or a state transition on a value returned by USER code —
// text-out methods beyond the plain-string positions
// text_appender_contract_test.go pins, TextUnmarshaler error returns, and
// CustomType Encode/Decode returns. The invariant per cell: a contract-violating
// return NEVER panics through a public API and NEVER silently corrupts sibling
// data. The lax-name validator, IsZero() bool, and the wire-side use of map keys
// are structurally immune — the first two return no value the library computes
// with, and map keys are read and written as raw strings on every path.

// symbolTexter's MarshalText names an enum symbol (or violates the
// contract, per mode). The enum encoders look the returned text up in
// the symbol table, so every wrong-content shape is detectable there.
type symbolTexter struct{ mode string }

func (e symbolTexter) MarshalText() ([]byte, error) {
	switch e.mode {
	case "valid":
		return []byte("B"), nil
	case "unknown":
		return []byte("NOPE"), nil
	case "nil-nil":
		return nil, nil
	case "error":
		return nil, errors.New("texter boom")
	}
	panic("bad mode " + e.mode)
}

// uuidTexter's MarshalText yields UUID text (or violations). On a
// fixed(16)+uuid schema the 16 wire bytes are DERIVED from the returned
// text (parseUUID), so wrong content is detectable and must reject; on a
// string+uuid schema the encoder is string-lenient (serUUID delegates
// non-[16]byte sources to the string encoder), so arbitrary text encodes
// verbatim — those cells assert byte-parity with the plain-string twin
// rather than rejection.
type uuidTexter struct{ mode string }

func (u uuidTexter) MarshalText() ([]byte, error) {
	switch u.mode {
	case "valid":
		return []byte("12345678-1234-1234-1234-123456789abc"), nil
	case "garbage":
		return []byte("not-a-uuid"), nil
	case "nil-nil":
		return nil, nil
	case "error":
		return nil, errors.New("uuid boom")
	}
	panic("bad mode " + u.mode)
}

func TestMatrix_TextOutCallbackReturnShapes(t *testing.T) {
	encode := func(s *Schema, wire string, v any) ([]byte, error) {
		if wire == "binary" {
			return s.Encode(v)
		}
		return s.EncodeJSON(v)
	}

	t.Run("enum", func(t *testing.T) {
		s := MustParse(`{"type":"enum","name":"E","symbols":["A","B"]}`)
		for _, wire := range []string{"binary", "json"} {
			for _, mode := range []string{"valid", "unknown", "nil-nil", "error"} {
				t.Run(mode+"/"+wire, func(t *testing.T) {
					out, err := encode(s, wire, symbolTexter{mode})
					if mode == "valid" {
						if err != nil {
							t.Fatalf("valid symbol via MarshalText rejected: %v", err)
						}
						twin, _ := encode(s, wire, "B")
						if !bytes.Equal(out, twin) {
							t.Errorf("text-out enum diverged from plain-string twin: % x vs % x", out, twin)
						}
						return
					}
					// unknown and nil-nil (empty text) miss the symbol table;
					// an error return is surfaced — all as *SemanticError.
					if err == nil {
						t.Fatalf("%s silently encoded: % x", mode, out)
					}
					var se *SemanticError
					if !errors.As(err, &se) {
						t.Errorf("not a SemanticError: %v", err)
					}
					if mode == "error" && !strings.Contains(err.Error(), "texter boom") {
						t.Errorf("user error identity lost: %v", err)
					}
				})
			}
		}
	})

	// Every INPUT ARM of the enum encoders — plain string, named string
	// without text methods, text-out (covered above), int ordinal — must
	// produce the same *SemanticError{AvroType: "enum"} identity on both
	// wires for a value naming no symbol / an out-of-range ordinal. The
	// cells run at TOP LEVEL deliberately: record positions wrap any
	// error in a SemanticError via the field-path wrapper, which would
	// mask a plain-error arm; top level has no wrapper to hide behind.
	t.Run("enum-arm-identity", func(t *testing.T) {
		s := MustParse(`{"type":"enum","name":"E","symbols":["A","B"]}`)
		type bare string // named string kind, no text methods
		arms := []struct {
			name string
			val  any
		}{
			{"plain-string", "NOPE"},
			{"named-string", bare("NOPE")},
			{"int-ordinal", int64(99)},
		}
		for _, arm := range arms {
			for _, wire := range []string{"binary", "json"} {
				t.Run(arm.name+"/"+wire, func(t *testing.T) {
					var err error
					if wire == "binary" {
						_, err = s.Encode(arm.val)
					} else {
						_, err = s.EncodeJSON(arm.val)
					}
					if err == nil {
						t.Fatal("non-symbol value accepted")
					}
					var se *SemanticError
					if !errors.As(err, &se) {
						t.Fatalf("no SemanticError identity: %v", err)
					}
					if se.AvroType != "enum" {
						t.Errorf("AvroType = %q, want enum: %v", se.AvroType, err)
					}
				})
			}
		}
	})

	// Sibling user-value failures with the same two-wire contract: a
	// wrong-length source for fixed, and a source map missing a
	// defaultless record field. Binary rejects both as *SemanticError
	// (serSize's shape check; the record loop's "missing key"
	// construction); JSON encode must carry the identical identity.
	t.Run("fixed-size-mismatch-identity", func(t *testing.T) {
		s := MustParse(`{"type":"fixed","name":"F","size":4}`)
		for _, wire := range []string{"binary", "json"} {
			t.Run(wire, func(t *testing.T) {
				var err error
				if wire == "binary" {
					_, err = s.Encode([]byte("toolongvalue"))
				} else {
					_, err = s.EncodeJSON([]byte("toolongvalue"))
				}
				if err == nil {
					t.Fatal("wrong-length fixed source accepted")
				}
				var se *SemanticError
				if !errors.As(err, &se) {
					t.Fatalf("no SemanticError identity: %v", err)
				}
				if se.AvroType != "fixed" {
					t.Errorf("AvroType = %q, want fixed: %v", se.AvroType, err)
				}
			})
		}
	})
	t.Run("missing-required-field-identity", func(t *testing.T) {
		s := MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}`)
		for _, wire := range []string{"binary", "json"} {
			t.Run(wire, func(t *testing.T) {
				var err error
				if wire == "binary" {
					_, err = s.Encode(map[string]any{})
				} else {
					_, err = s.EncodeJSON(map[string]any{})
				}
				if err == nil {
					t.Fatal("missing defaultless field accepted")
				}
				var se *SemanticError
				if !errors.As(err, &se) {
					t.Fatalf("no SemanticError identity: %v", err)
				}
				if se.Field != "a" {
					t.Errorf("Field = %q, want a: %v", se.Field, err)
				}
			})
		}
	})

	// The remaining encode user-value failures already AGREE across the
	// two wires — nil-for-non-nullable is plain on both (its own family),
	// union no-match and numeric-content rejects are SemanticError on
	// both. Pin the agreement (identity equality, not any specific shape)
	// so neither wire drifts alone.
	t.Run("cross-wire-identity-agreement", func(t *testing.T) {
		identityOf := func(err error) string {
			if err == nil {
				return "nil"
			}
			var se *SemanticError
			if errors.As(err, &se) {
				return "semantic:" + se.AvroType
			}
			return "plain"
		}
		rows := []struct {
			name   string
			schema string
			val    any
		}{
			{"nil-non-nullable", `"long"`, (*int64)(nil)},
			{"union-no-match", `["null","long"]`, "zz"},
			{"float-bad-string", `"float"`, "abc"},
		}
		for _, row := range rows {
			t.Run(row.name, func(t *testing.T) {
				s := MustParse(row.schema)
				_, berr := s.Encode(row.val)
				_, jerr := s.EncodeJSON(row.val)
				if berr == nil || jerr == nil {
					t.Fatalf("both wires must reject: bin=%v json=%v", berr, jerr)
				}
				if identityOf(berr) != identityOf(jerr) {
					t.Errorf("error identity diverged: binary=%s (%v) vs json=%s (%v)",
						identityOf(berr), berr, identityOf(jerr), jerr)
				}
			})
		}
	})

	// The encode-side unknown-symbol reject is a USER-VALUE failure and
	// carries *SemanticError identity on both wires (asserted above). The
	// decode-side counterparts — a binary ordinal outside the symbol
	// table, a JSON string naming no symbol — are WIRE-CONTENT failures,
	// plain errors on both wires like the union-index and map-key-length
	// rejects. Pin the two families' boundary so neither side drifts.
	t.Run("enum-decode-content-errors-stay-plain", func(t *testing.T) {
		s := MustParse(`{"type":"enum","name":"E","symbols":["A","B"]}`)
		var got string
		_, err := s.Decode([]byte{0x08}, &got) // ordinal 4: out of range
		if err == nil {
			t.Fatal("out-of-range ordinal accepted")
		}
		var se *SemanticError
		if errors.As(err, &se) {
			t.Errorf("binary wire-content error gained SemanticError identity: %v", err)
		}
		if err := s.DecodeJSON([]byte(`"NOPE"`), &got); err == nil {
			t.Fatal("unknown wire symbol accepted")
		} else if errors.As(err, &se) {
			t.Errorf("JSON wire-content error gained SemanticError identity: %v", err)
		}
	})

	t.Run("fixed-uuid", func(t *testing.T) {
		s := MustParse(`{"type":"fixed","name":"F","size":16,"logicalType":"uuid"}`)
		for _, wire := range []string{"binary", "json"} {
			for _, mode := range []string{"valid", "garbage", "nil-nil", "error"} {
				t.Run(mode+"/"+wire, func(t *testing.T) {
					out, err := encode(s, wire, uuidTexter{mode})
					if mode == "valid" {
						if err != nil {
							t.Fatalf("valid uuid text rejected: %v", err)
						}
						twin, _ := encode(s, wire, "12345678-1234-1234-1234-123456789abc")
						if !bytes.Equal(out, twin) {
							t.Errorf("diverged from plain-string twin: % x vs % x", out, twin)
						}
						return
					}
					if err == nil {
						t.Fatalf("%s silently encoded: % x", mode, out)
					}
					if mode == "error" && !strings.Contains(err.Error(), "uuid boom") {
						t.Errorf("user error identity lost: %v", err)
					}
				})
			}
		}
	})

	t.Run("string-uuid-lenient", func(t *testing.T) {
		s := MustParse(`{"type":"string","logicalType":"uuid"}`)
		twinFor := map[string]string{"garbage": "not-a-uuid", "nil-nil": ""}
		for _, wire := range []string{"binary", "json"} {
			for mode, twinStr := range twinFor {
				t.Run(mode+"/"+wire, func(t *testing.T) {
					out, err := encode(s, wire, uuidTexter{mode})
					if err != nil {
						t.Fatalf("string+uuid is string-lenient for non-[16]byte sources: %v", err)
					}
					twin, _ := encode(s, wire, twinStr)
					if !bytes.Equal(out, twin) {
						t.Errorf("text-out diverged from plain-string twin: % x vs % x", out, twin)
					}
				})
			}
			t.Run("error/"+wire, func(t *testing.T) {
				if _, err := encode(s, wire, uuidTexter{"error"}); err == nil ||
					!strings.Contains(err.Error(), "uuid boom") {
					t.Fatalf("user error not surfaced with identity: %v", err)
				}
			})
		}
	})

	// A MarshalText-only type (no AppendText) on a plain string schema:
	// the returned bytes are materialized and copied, so nil text with a
	// nil error is simply the empty string; an error is surfaced.
	t.Run("string-marshaler-only", func(t *testing.T) {
		s := MustParse(`"string"`)
		for _, wire := range []string{"binary", "json"} {
			out, err := encode(s, wire, symbolTexter{"nil-nil"})
			if err != nil {
				t.Fatalf("nil text + nil error must encode the empty string: %v", err)
			}
			twin, _ := encode(s, wire, "")
			if !bytes.Equal(out, twin) {
				t.Errorf("nil-text encode diverged from empty-string twin: % x vs % x", out, twin)
			}
			if _, err := encode(s, wire, symbolTexter{"error"}); err == nil ||
				!strings.Contains(err.Error(), "texter boom") {
				t.Fatalf("user error not surfaced with identity: %v", err)
			}
		}
	})
}

// failingUnmarshaler errors on any text not prefixed "ok". Its error
// must surface — wrapped so the user's identity is preserved — from
// every text-shaped decode position on both wire formats.
type failingUnmarshaler struct{ S string }

func (f *failingUnmarshaler) UnmarshalText(b []byte) error {
	if strings.HasPrefix(string(b), "ok") {
		f.S = string(b)
		return nil
	}
	return errors.New("unmarshal boom")
}

func TestMatrix_TextUnmarshalerReturnShapes(t *testing.T) {
	strS := MustParse(`"string"`)
	recS := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"string"},{"name":"b","type":"string"},{"name":"c","type":"string"}]}`)
	arrS := MustParse(`{"type":"array","items":"string"}`)
	mapS := MustParse(`{"type":"map","values":"string"}`)
	enumS := MustParse(`{"type":"enum","name":"E","symbols":["A","B"]}`)
	uuidS := MustParse(`{"type":"string","logicalType":"uuid"}`)

	type recTarget struct {
		A string             `avro:"a"`
		B failingUnmarshaler `avro:"b"`
		C string             `avro:"c"`
	}

	t.Run("error-surfaces", func(t *testing.T) {
		cases := []struct {
			name   string
			decode func() error
		}{
			{"top/binary", func() error {
				wire, _ := strS.Encode("bad")
				var f failingUnmarshaler
				_, err := strS.Decode(wire, &f)
				return err
			}},
			{"top/json", func() error {
				var f failingUnmarshaler
				return strS.DecodeJSON([]byte(`"bad"`), &f)
			}},
			{"record-mid/binary", func() error {
				wire, _ := recS.Encode(map[string]any{"a": "okA", "b": "bad", "c": "okC"})
				var tg recTarget
				_, err := recS.Decode(wire, &tg)
				return err
			}},
			{"record-mid/json", func() error {
				var tg recTarget
				return recS.DecodeJSON([]byte(`{"a":"okA","b":"bad","c":"okC"}`), &tg)
			}},
			{"array-item/binary", func() error {
				wire, _ := arrS.Encode([]string{"bad"})
				var tg []failingUnmarshaler
				_, err := arrS.Decode(wire, &tg)
				return err
			}},
			{"map-value/binary", func() error {
				wire, _ := mapS.Encode(map[string]string{"k": "bad"})
				var tg map[string]failingUnmarshaler
				_, err := mapS.Decode(wire, &tg)
				return err
			}},
			{"enum/binary", func() error {
				wire, _ := enumS.Encode("B")
				var f failingUnmarshaler
				_, err := enumS.Decode(wire, &f)
				return err
			}},
			{"uuid-string/binary", func() error {
				wire, _ := uuidS.Encode("12345678-1234-1234-1234-123456789abc")
				var f failingUnmarshaler
				_, err := uuidS.Decode(wire, &f)
				return err
			}},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				err := c.decode()
				if err == nil {
					t.Fatal("UnmarshalText error swallowed")
				}
				if !strings.Contains(err.Error(), "unmarshal boom") {
					t.Errorf("user error identity lost: %v", err)
				}
			})
		}
	})

	// Success control: the method fires and the value lands.
	t.Run("control", func(t *testing.T) {
		wire, _ := strS.Encode("okYes")
		var f failingUnmarshaler
		if _, err := strS.Decode(wire, &f); err != nil || f.S != "okYes" {
			t.Fatalf("control: %v %q", err, f.S)
		}
	})
}

// contractLong is the GoType for the CustomType return-shape matrices.
type contractLong int64

func customEncodeReturning(shape string) CustomType {
	return CustomType{
		AvroType: "long",
		GoType:   reflect.TypeFor[contractLong](),
		Encode: func(v any, sn *SchemaNode) (any, error) {
			switch shape {
			case "ok":
				return int64(v.(contractLong)) * 10, nil
			case "untyped-nil":
				return nil, nil
			case "typed-nil":
				return (*int64)(nil), nil
			case "wrong-type":
				return "zz", nil
			case "err-with-value":
				return int64(42), errors.New("enc boom")
			case "skip-wrapped":
				return nil, fmt.Errorf("wrapped: %w", ErrSkipCustomType)
			}
			panic("bad shape " + shape)
		},
	}
}

// TestMatrix_CustomTypeEncodeReturnShapes crosses CustomType.Encode return
// shapes with encode positions on both wires. The contract: an untyped nil
// return is the named "returned nil" reject; a typed-nil or wrong-typed return
// re-enters the underlying serializer, whose type validation names it; a non-nil
// error is fatal with the value discarded and the user's identity preserved; an
// ErrSkipCustomType return (wrapped counts, since the chain matches with
// errors.Is) falls through to the built-in encode of the original value. No
// shape panics, and sibling values already encoded are unaffected.
func TestMatrix_CustomTypeEncodeReturnShapes(t *testing.T) {
	positions := []struct {
		name   string
		schema string
		val    func() any
		twin   func() any // plain value with contractLong(5)'s wire stand-in int64(5)
	}{
		{"top", `"long"`,
			func() any { return contractLong(5) },
			func() any { return int64(5) }},
		{"record-mid", `{"type":"record","name":"R","fields":[
			{"name":"a","type":"string"},{"name":"b","type":"long"},{"name":"c","type":"string"}]}`,
			func() any { return map[string]any{"a": "AA", "b": contractLong(5), "c": "CC"} },
			func() any { return map[string]any{"a": "AA", "b": int64(5), "c": "CC"} }},
		{"array-item", `{"type":"array","items":"long"}`,
			func() any { return []any{contractLong(5)} },
			func() any { return []any{int64(5)} }},
		{"map-value", `{"type":"map","values":"long"}`,
			func() any { return map[string]any{"k": contractLong(5)} },
			func() any { return map[string]any{"k": int64(5)} }},
		{"union-branch", `["null","long"]`,
			func() any { return contractLong(5) },
			func() any { return int64(5) }},
	}
	shapes := []string{"ok", "untyped-nil", "typed-nil", "wrong-type", "err-with-value", "skip-wrapped"}

	for _, pos := range positions {
		plain := MustParse(pos.schema)
		for _, shape := range shapes {
			s := mustParse(t, pos.schema, customEncodeReturning(shape))
			for _, wire := range []string{"binary", "json"} {
				t.Run(pos.name+"/"+shape+"/"+wire, func(t *testing.T) {
					encode := func(sc *Schema, v any) ([]byte, error) {
						if wire == "binary" {
							return sc.Encode(v)
						}
						return sc.EncodeJSON(v)
					}
					out, err := encode(s, pos.val())
					switch shape {
					case "ok":
						if err != nil {
							t.Fatalf("transforming encode rejected: %v", err)
						}
						twin, _ := encode(plain, func() any {
							switch pos.name {
							case "record-mid":
								return map[string]any{"a": "AA", "b": int64(50), "c": "CC"}
							case "array-item":
								return []any{int64(50)}
							case "map-value":
								return map[string]any{"k": int64(50)}
							default:
								return int64(50)
							}
						}())
						if !bytes.Equal(out, twin) {
							t.Errorf("transformed encode != plain x10 twin: % x vs % x", out, twin)
						}
					case "skip-wrapped":
						if err != nil {
							t.Fatalf("wrapped ErrSkipCustomType not honored: %v", err)
						}
						twin, _ := encode(plain, pos.twin())
						if !bytes.Equal(out, twin) {
							t.Errorf("fall-through encode != plain twin: % x vs % x", out, twin)
						}
					case "untyped-nil":
						if err == nil {
							t.Fatalf("untyped-nil return silently encoded: % x", out)
						}
						if !strings.Contains(err.Error(), "custom type encoder returned nil") {
							t.Errorf("want the named returned-nil reject, got: %v", err)
						}
					case "err-with-value":
						if err == nil {
							t.Fatalf("error swallowed, value encoded: % x", out)
						}
						if !strings.Contains(err.Error(), "enc boom") {
							t.Errorf("user error identity lost: %v", err)
						}
					case "typed-nil", "wrong-type":
						// The returned value re-enters the underlying
						// serializer; its type validation names the shape
						// (nil for a non-nullable long / string vs long).
						if err == nil {
							t.Fatalf("%s return silently encoded: % x", shape, out)
						}
					}
				})
			}
		}
	}
}

func customDecodeReturning(shape string) CustomType {
	return CustomType{
		AvroType: "long",
		Decode: func(v any, sn *SchemaNode) (any, error) {
			switch shape {
			case "ok":
				return contractLong(v.(int64) * 10), nil
			case "nil-nil":
				return nil, nil
			case "wrong-type":
				return "zz", nil
			case "err-with-value":
				return contractLong(9), errors.New("dec boom")
			case "skip-wrapped":
				return nil, fmt.Errorf("wrapped: %w", ErrSkipCustomType)
			}
			panic("bad shape " + shape)
		},
	}
}

// TestMatrix_CustomTypeDecodeReturnShapes crosses CustomType.Decode
// return shapes with a typed record target on both wire formats. The
// contract: a nil result zeroes the target field; a result whose type
// is not assignable to the target is the named *SemanticError (never a
// reflect.Set panic); a non-nil error is fatal with the value discarded
// and the user's identity preserved; a wrapped ErrSkipCustomType falls
// through to the value a no-custom decode produces. Whenever Decode
// returns nil error, sibling fields hold their decoded values — a
// violating callback can never corrupt data beside its own node.
func TestMatrix_CustomTypeDecodeReturnShapes(t *testing.T) {
	const recSchema = `{"type":"record","name":"R","fields":[
		{"name":"a","type":"string"},{"name":"b","type":"long"},{"name":"c","type":"string"}]}`
	plain := MustParse(recSchema)
	wire := mustEncode(t, plain, map[string]any{"a": "AA", "b": int64(5), "c": "CC"})
	jsonWire := []byte(`{"a":"AA","b":5,"c":"CC"}`)

	type target struct {
		A string       `avro:"a"`
		B contractLong `avro:"b"`
		C string       `avro:"c"`
	}

	for _, shape := range []string{"ok", "nil-nil", "wrong-type", "err-with-value", "skip-wrapped"} {
		s := mustParse(t, recSchema, customDecodeReturning(shape))
		for _, wk := range []string{"binary", "json"} {
			t.Run(shape+"/"+wk, func(t *testing.T) {
				var tg target
				var derr error
				if wk == "binary" {
					_, derr = s.Decode(wire, &tg)
				} else {
					derr = s.DecodeJSON(jsonWire, &tg)
				}
				siblingsIntact := func() {
					if tg.A != "AA" || tg.C != "CC" {
						t.Errorf("sibling fields corrupted: %+v", tg)
					}
				}
				switch shape {
				case "ok":
					if derr != nil || tg.B != 50 {
						t.Fatalf("transforming decode: err=%v B=%d", derr, tg.B)
					}
					siblingsIntact()
				case "nil-nil":
					if derr != nil {
						t.Fatalf("nil result must zero, not error: %v", derr)
					}
					if tg.B != 0 {
						t.Errorf("nil result must zero the field: %d", tg.B)
					}
					siblingsIntact()
				case "wrong-type":
					if derr == nil {
						t.Fatalf("unassignable result silently placed: %+v", tg)
					}
					var se *SemanticError
					if !errors.As(derr, &se) {
						t.Errorf("not a SemanticError: %v", derr)
					}
				case "err-with-value":
					if derr == nil {
						t.Fatalf("error swallowed: %+v", tg)
					}
					if !strings.Contains(derr.Error(), "dec boom") {
						t.Errorf("user error identity lost: %v", derr)
					}
				case "skip-wrapped":
					if derr != nil {
						t.Fatalf("wrapped ErrSkipCustomType not honored: %v", derr)
					}
					if tg.B != 5 {
						t.Errorf("fall-through must match no-custom decode: %d", tg.B)
					}
					siblingsIntact()
				}
			})
		}
	}

	// An interface target accepts any result type — the callback's value
	// is the user's own choice there, placed verbatim.
	t.Run("wrong-type/any-target", func(t *testing.T) {
		s, _ := Parse(`"long"`, customDecodeReturning("wrong-type"))
		w2, _ := MustParse(`"long"`).Encode(int64(5))
		var v any
		if _, err := s.Decode(w2, &v); err != nil {
			t.Fatalf("interface target must accept any result: %v", err)
		}
		if v != "zz" {
			t.Errorf("callback result not placed verbatim: %v", v)
		}
	})
}

// rawKey carries transforming text methods that the map-key paths must
// NOT consult: Avro map keys are already string-kind, and all four
// paths (binary/JSON x encode/decode) read and write them as raw
// strings. If any single path started consulting text methods, the
// transform here would break the raw-key agreement across paths.
type rawKey string

func (k rawKey) MarshalText() ([]byte, error)  { return []byte(strings.ToUpper(string(k))), nil }
func (k *rawKey) UnmarshalText(b []byte) error { *k = rawKey(strings.ToLower(string(b))); return nil }

func TestRegression_MapKeysBypassTextMethods(t *testing.T) {
	s := MustParse(`{"type":"map","values":"long"}`)
	in := map[rawKey]int64{"Key": 7}

	bin := mustEncode(t, s, in)
	if !bytes.Contains(bin, []byte("Key")) {
		t.Errorf("binary map key not the raw string: % x", bin)
	}
	var back map[rawKey]int64
	mustDecode(t, s, bin, &back)
	if _, ok := back["Key"]; !ok {
		t.Errorf("binary decode transformed the key: %v", back)
	}

	jout := mustEncodeJSON(t, s, in)
	if !bytes.Contains(jout, []byte(`"Key"`)) {
		t.Errorf("JSON map key not the raw string: %s", jout)
	}
	var jback map[rawKey]int64
	mustDecodeJSON(t, s, jout, &jback)
	if _, ok := jback["Key"]; !ok {
		t.Errorf("JSON decode transformed the key: %v", jback)
	}
}

// TestRegression_SchemaNodePropsUnmarshalableValueNamedError pins that a
// hand-built SchemaNode whose Props holds a value the schema rebuild
// cannot marshal (a channel) surfaces as a named error from
// SchemaNode.Schema, not a panic.
func TestRegression_SchemaNodePropsUnmarshalableValueNamedError(t *testing.T) {
	sn := &SchemaNode{Type: "record", Name: "R",
		Fields: []SchemaField{{Name: "f", Type: SchemaNode{Type: "int"}}},
		Props:  map[string]any{"x": make(chan int)},
	}
	if _, err := sn.Schema(); err == nil {
		t.Fatal("unmarshalable Props value silently accepted")
	}
}

// ---------- hidden_state_census_test.go ----------

// Hidden state on a user-composable public struct is a correctness hazard: a
// caller who sets an exported field expects that field to decide the outcome, so
// unexported state must never silently win over it. A struct is
// "user-composable" only if it has exported fields a caller sets; the rest carry
// unexported state no caller can contradict. This census freezes the
// enumeration, so a new exported struct gaining both fails here and forces the
// same analysis rather than letting the hazard land unexamined.

type myMillis int64

func fieldSplit(t reflect.Type) (exported, hidden []string) {
	for i := range t.NumField() {
		f := t.Field(i)
		if f.IsExported() {
			exported = append(exported, f.Name)
		} else {
			hidden = append(hidden, f.Name)
		}
	}
	return
}

func TestInvariant_HiddenStateOnPublicStructs(t *testing.T) {
	// Every exported struct type in the package (and the two in ocf are
	// covered by the same reasoning: zero exported fields).
	types := []reflect.Type{
		reflect.TypeFor[Schema](),
		reflect.TypeFor[SchemaNode](),
		reflect.TypeFor[SchemaField](),
		reflect.TypeFor[SchemaCache](),
		reflect.TypeFor[CustomType](),
		reflect.TypeFor[SemanticError](),
		reflect.TypeFor[CompatibilityError](),
		reflect.TypeFor[ShortBufferError](),
		reflect.TypeFor[Duration](),
	}
	// The ONLY types where a caller-set exported field coexists with
	// unexported state. Each is justified below and pinned by a behavior
	// test; adding a name here requires doing the same.
	composableWithHiddenState := map[string]string{
		"CustomType": "needsAvroType is fail-loud only: it can make Parse REJECT (when AvroType is empty), never silently substitute a value — pinned by TestInvariant_CustomTypeHiddenStateFailsLoud",
		"SchemaNode": "refTarget (with refNS, the scope it was resolved in — the two are one stamp and are only meaningful together) is consulted only while the name resolver still binds the node's exported Type to it (nodeRefTargetAgrees), so an edited Type always wins — pinned by TestNodeRefSchema_EditedTypeIgnoresStaleStamp. " +
			"present is PRESENCE-ONLY and value-transparent: one bit per attribute whose body can be its own destination's zero, deciding whether such an attribute gets written at all, never what any attribute says — so the value a caller sets is the value that comes back for every input, pinned by TestInvariant_PresenceStateIsValueTransparent",
		"SchemaField": "docSet is the field-level twin of SchemaNode's, and carries the same proof: presence-only and value-transparent — pinned by TestInvariant_PresenceStateIsValueTransparent",
	}
	for _, ty := range types {
		exported, hidden := fieldSplit(ty)
		if len(hidden) == 0 || len(exported) == 0 {
			continue // not user-composable, or no hidden state: no hazard
		}
		why, ok := composableWithHiddenState[ty.Name()]
		if !ok {
			t.Errorf("%s has BOTH exported fields (%v) and unexported state (%v), so hidden state could silently override a caller's edit. Prove it cannot, add it to composableWithHiddenState with the reason, and pin the behavior.",
				ty.Name(), exported, hidden)
			continue
		}
		t.Logf("%-12s exported=%d hidden=%v — %s", ty.Name(), len(exported), hidden, why)
	}
	// The census must not silently go vacuous if the types list rots.
	if len(types) < 9 {
		t.Fatal("types list shrank; the census only covers what it lists")
	}
}

// TestInvariant_CustomTypeHiddenStateFailsLoud executes the claim that
// CustomType's one unexported field cannot silently win: NewCustomType sets
// needsAvroType, and a caller who copies that value and clears the exported
// AvroType gets a loud parse error rather than a stale conversion.
func TestInvariant_CustomTypeHiddenStateFailsLoud(t *testing.T) {
	ct := NewCustomType("",
		func(v myMillis, _ *SchemaNode) (int64, error) { return int64(v), nil },
		func(v int64, _ *SchemaNode) (myMillis, error) { return myMillis(v), nil },
	)
	if ct.AvroType == "" {
		// NewCustomType infers the Avro type from the Go types; if it did
		// not set one, the "cleared" case below is not distinguishable.
		t.Skip("NewCustomType did not infer an AvroType for this pair")
	}
	cleared := ct // struct copy: carries needsAvroType
	cleared.AvroType = ""
	_, err := Parse(`"string"`, cleared)
	if err == nil {
		t.Fatal("clearing AvroType on a NewCustomType value parsed silently; the hidden needsAvroType must make this fail loudly")
	}
	if !strings.Contains(err.Error(), "unsupported Avro native type") {
		t.Fatalf("unexpected error %v; want the loud unsupported-Avro-native-type reject", err)
	}
	t.Logf("fails loud as required: %v", err)
}

// TestInvariant_PresenceStateIsValueTransparent executes the claim that the
// presence flags cannot win over a caller. They differ in kind from refTarget,
// which selects a DEFINITION and so could substitute one schema for another: a
// presence flag decides only whether an attribute whose value is the field's own
// zero is written at all, never which value. So for every value a caller can set,
// the value that comes back is the value they set, flag set and flag clear —
// proved over a node extracted from a parse and the same node hand-composed,
// including the case a caller cannot otherwise reach, clearing the field to "".
// Wire, canonical form and fingerprint must be identical across the pair too.
func TestInvariant_PresenceStateIsValueTransparent(t *testing.T) {
	// extracted carries every presence flag set; composed carries none.
	extracted := MustParse(`{"type":"record","name":"R","doc":"","fields":[` +
		`{"name":"f","type":{"type":"int","logicalType":""},"doc":""}]}`).Root()
	if !extracted.present.has(presDoc) {
		t.Fatal("the extracted node did not record a written doc; the control is broken")
	}
	if !extracted.Fields[0].docSet {
		t.Fatal("the extracted field did not record a written doc; the control is broken")
	}
	if !extracted.Fields[0].Type.present.has(presLogicalType) {
		t.Fatal("the extracted type did not record a written logicalType; the control is broken")
	}

	for _, docValue := range []string{"", "x", "a longer doc string"} {
		for _, ltValue := range []string{"", "date", "not-a-logical"} {
			withState := extracted
			withState.Fields = append([]SchemaField(nil), extracted.Fields...)
			withState.Fields[0].Type = extracted.Fields[0].Type
			withState.Doc = docValue
			withState.Fields[0].Doc = docValue
			withState.Fields[0].Type.LogicalType = ltValue

			clean := SchemaNode{Type: "record", Name: "R", Doc: docValue,
				Fields: []SchemaField{{Name: "f", Doc: docValue,
					Type: SchemaNode{Type: "int", LogicalType: ltValue}}}}

			for label, n := range map[string]SchemaNode{"extracted": *withState, "composed": clean} {
				s, err := n.Schema()
				if err != nil {
					t.Fatalf("%s doc=%q lt=%q: Schema(): %v", label, docValue, ltValue, err)
				}
				back := s.Root()
				if back.Doc != docValue {
					t.Errorf("%s: node Doc set to %q came back %q — hidden state changed a caller's value",
						label, docValue, back.Doc)
				}
				if back.Fields[0].Doc != docValue {
					t.Errorf("%s: field Doc set to %q came back %q", label, docValue, back.Fields[0].Doc)
				}
				if back.Fields[0].Type.LogicalType != ltValue {
					t.Errorf("%s: LogicalType set to %q came back %q",
						label, ltValue, back.Fields[0].Type.LogicalType)
				}
			}

			// The surfaces presence must not reach.
			sa, err := withState.Schema()
			if err != nil {
				t.Fatalf("extracted Schema(): %v", err)
			}
			sb, err := clean.Schema()
			if err != nil {
				t.Fatalf("composed Schema(): %v", err)
			}
			if string(sa.Canonical()) != string(sb.Canonical()) {
				t.Errorf("presence state changed the canonical form:\n %s\n %s", sa.Canonical(), sb.Canonical())
			}
			if len(sa.Canonical()) == 0 {
				t.Fatal("canonical form came back empty, so the comparison proved nothing")
			}
			fa, fb := sa.Fingerprint(NewRabin()), sb.Fingerprint(NewRabin())
			if string(fa) != string(fb) {
				t.Errorf("presence state changed the fingerprint: %x vs %x", fa, fb)
			}
			val := map[string]any{"f": 0}
			ea, err := sa.AppendEncode(nil, val)
			if err != nil {
				t.Fatalf("extracted encode: %v", err)
			}
			eb, err := sb.AppendEncode(nil, val)
			if err != nil {
				t.Fatalf("composed encode: %v", err)
			}
			if string(ea) != string(eb) {
				t.Errorf("presence state changed the wire: %x vs %x", ea, eb)
			}
		}
	}
}

// wolverine returns a fresh Superhero exercising every field of
// superheroUnionSchema, including a populated union-typed Powers slice. Fresh
// per call: the slice is caller-mutable.
func wolverine() Superhero {
	return Superhero{
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
}

// TestInvariant_SlabNeverRewindsHandedOutMemory pins the property that lets
// slab.string return an unsafe.String over pooled memory: buf only ever
// shrinks from the front, or is replaced wholesale by a fresh make. It is
// never rewound. Every string the slab has handed out aliases a region of some
// buf, so reclaiming that memory does not free it — it hands the same bytes
// out twice and silently mutates strings the caller already holds, long after
// Decode returned. A decoded string used as a map key changes value in place.
//
// The slab struct itself IS pooled and reused (put deliberately retains buf),
// so "the memory is fresh each call" is not what makes this safe. Only the
// never-rewound rule does.
func TestInvariant_SlabNeverRewindsHandedOutMemory(t *testing.T) {
	sl := &slab{}

	type region struct{ start, end uintptr }
	var handed []region
	var held, want []string

	for i := range 300 {
		// Sizes stride across slabSize so the buffer is exhausted and
		// replaced several times; a rewind that only shows up at the
		// replacement boundary is the one worth catching.
		w := fmt.Sprintf("v%03d-%s", i, strings.Repeat("x", i%53))
		got := sl.string([]byte(w), len(w))
		if got != w {
			t.Fatalf("slab.string returned %q, want %q", got, w)
		}
		p := uintptr(unsafe.Pointer(unsafe.StringData(got)))
		// held keeps every handed-out region reachable for the whole test, so
		// the allocator cannot recycle one into a later buf and stage an
		// "overlap" that is not a rewind.
		held = append(held, got)
		want = append(want, w)
		handed = append(handed, region{p, p + uintptr(len(got))})

		if len(sl.buf) == 0 {
			continue
		}
		bs := uintptr(unsafe.Pointer(unsafe.SliceData(sl.buf)))
		be := bs + uintptr(len(sl.buf))
		for j, r := range handed {
			if bs < r.end && r.start < be {
				t.Fatalf("after %d strings, the slab's remaining buf [%#x,%#x) overlaps the region handed out for string %d [%#x,%#x): buf was rewound",
					i+1, bs, be, j, r.start, r.end)
			}
		}
	}

	for i, s := range held {
		if s != want[i] {
			t.Errorf("string %d mutated by %d later slab allocations: got %q, want %q", i, len(held)-i-1, s, want[i])
		}
	}
}

// TestInvariant_SlabPutDoesNotReclaimBuf guards the same rule at the one place
// it would plausibly be broken. put hands the slab back for reuse, so
// reclaiming the tail there — sl.buf = sl.buf[:0], or sl.buf = nil to "free"
// it — reads like an obvious win and costs nothing visible in any test that
// does not hold a decoded string across two decodes.
//
// Read from source rather than executed: calling put publishes the slab to the
// pool, and reading its fields afterward races with whoever Gets it next.
func TestInvariant_SlabPutDoesNotReclaimBuf(t *testing.T) {
	const decl = "func (sl *slab) put()"
	src := readFile(t, "deser.go")
	i := strings.Index(src, decl)
	if i < 0 {
		t.Fatalf("%s not found in deser.go — this guard reads it from source, so a rename must update the guard, not silence it", decl)
	}
	body := src[i:]
	if e := strings.Index(body, "\n}\n"); e >= 0 {
		body = body[:e+2]
	}
	if strings.Contains(body, "sl.buf") {
		t.Errorf("slab.put touches sl.buf:\n\n%s\nbuf must carry over untouched. Every string slab.string returned aliases it; resetting or re-slicing here hands the same bytes out twice.", body)
	}
}
