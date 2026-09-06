package avro

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"math"
	"math/big"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/twmb/avro/internal/optmark"
)

// ---------- deser_test.go ----------

// ptr returns a pointer to v. We use it instead of Go 1.26's new(v).
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

// encode encodes v with schema and returns the raw bytes.
func encode(t *testing.T, schema string, v any) []byte {
	t.Helper()
	s := mustParse(t, schema)
	dst := mustAppendEncode(t, s, nil, v)
	return dst
}

// decode decodes src into v with schema.
func decode(t *testing.T, schema string, src []byte, v any) {
	t.Helper()
	s := mustParse(t, schema)
	rem := mustDecode(t, s, src, v)
	if len(rem) != 0 {
		t.Fatalf("Decode left %d unconsumed bytes", len(rem))
	}
}

// decodeErr expects Decode to return an error.
func decodeErr(t *testing.T, schema string, src []byte, v any) {
	t.Helper()
	s := mustParse(t, schema)
	_, err := s.Decode(src, v)
	if err == nil {
		t.Fatal("expected error from Decode, got nil")
	}
}

// -----------------------------------------------------------------------
// Wire-format tests. We assert the encoded bytes match the Avro spec, and
// that decoding those bytes gives the value back. This is what keeps us
// interoperable with other Avro implementations.
// -----------------------------------------------------------------------

func TestWireFormatInt(t *testing.T) {
	// int 27 -> zigzag 54 -> 0x36
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

func TestWireFormatFloat(t *testing.T) {
	// float32(1.15) -> bits 0x3F933333 -> LE bytes 33 33 93 3F
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

func TestWireFormatString(t *testing.T) {
	// "foo" -> length 3, zigzag 6, then 0x66 0x6F 0x6F
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

// -----------------------------------------------------------------------
// Round-trip tests: we encode, then decode, then check equality.
// -----------------------------------------------------------------------

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

func TestRoundTripInterface(t *testing.T) {
	type Iface struct {
		S fmt.Stringer `avro:"s"`
	}
	schema := ifaceFoobarSchema
	s := mustParse(t, schema)
	input := Iface{S: &IfaceF{F: 3}}
	encoded := mustAppendEncode(t, s, nil, &input)

	// Here we deserialize into a struct whose interface field is pre-set
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
			// A generic target keeps the error coming from reading, not a type mismatch.
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
				// Decode into any: hits the reflect path.
				_, err = s.Decode(tt.data, &v)
			}
			_ = v
			if err == nil {
				t.Fatal("expected error for short buffer, got nil")
			}
		})
	}

	// Also the fixed UUID short buffer, through typed targets.
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
	// Short buffer through struct decode, which hits the unsafe fast path.
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
		// "int into float" used to be pinned as a rejection. We now
		// support it, for round-trip parity with the documented
		// encode-side whole-number-float-as-int divergence. See
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
	decodeErr(t, `["null","string"]`, []byte{0x04}, ptr((*string)(nil))) // zigzag 4 -> 2
}

func TestDecodeInvalidEnumIndex(t *testing.T) {
	// Enum with 2 symbols; index 2 is out of range.
	schema := `{"type":"enum","name":"e","symbols":["a","b"]}`
	decodeErr(t, schema, []byte{0x04}, ptr("")) // zigzag 4 -> 2
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
	// A crafted array block count that exceeds the buffer length. We drive
	// the unsafe fast path (udArrayDirect and udArrayPtrRecord) to prove
	// neither allocates on an untrusted count.
	schema := `{"type":"array","items":"int"}`
	s := mustParse(t, schema)
	// count=999999 (zigzag-encoded), but only a few bytes of data.
	var data []byte
	data = appendVarlong(data, 999999)
	data = append(data, 0x00, 0x00)

	// Decode into []int32: triggers the unsafe primitive array path.
	var sl []int32
	_, err := s.Decode(data, &sl)
	if err == nil {
		t.Fatal("expected error for oversized array count")
	}

	// Also a struct holding a slice, which triggers the unsafe struct fast path.
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

func TestDecodeNegativeStringLength(t *testing.T) {
	decodeErr(t, `"string"`, []byte{0x01}, ptr("")) // zigzag 1 -> -1
}

func TestDecodeNegativeBytesLength(t *testing.T) {
	decodeErr(t, `"bytes"`, []byte{0x01}, ptr([]byte{})) // zigzag 1 -> -1
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
	decodeErr(t, `{"type":"fixed","name":"f","size":4}`, []byte{1, 2, 3, 4}, ptr(int32(0)))
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

func TestDecodeMapKeyInvalid(t *testing.T) {
	schema := `{"type":"map","values":"int"}`
	// count=1 (0x02), key length = -1 (zigzag 0x01)
	decodeErr(t, schema, []byte{0x02, 0x01}, ptr(map[string]int32{}))
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

func TestDecodeArrayNegativeBlockShortRead(t *testing.T) {
	// Negative block count followed by truncated block size.
	schema := `{"type":"array","items":"int"}`
	data := []byte{0x01, 0xE6} // count=-1 (zigzag 0x01), then truncated block size
	decodeErr(t, schema, data, ptr([]int32{}))
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

func TestRoundTripIntWidths(t *testing.T) {
	// Every signed and unsigned integer width that fits in avro int.
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

// Here we drive every branch in usInt/udInt, usLong/udLong, usFloat/udFloat,
// and usDouble/udDouble. The Go struct field types vary across record schemas.
// The unsafe fast path compiles per-Kind closures, so we need a struct field
// of each Kind.

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

func TestUnsafeIntAllKinds(t *testing.T) { testUnsafeIntLongAllKinds(t, "int") }

func TestUnsafeFloatDoubleKinds(t *testing.T) {
	// Avro "float" mapped to Go float64, and avro "double" mapped to Go float32.
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
	// Here we drive tryCompileFieldSer's recursive pointer-through-primitive path.
	type R struct {
		V *int32 `avro:"v"`
	}
	schema := `{"type":"record","name":"R","fields":[{"name":"v","type":"int"}]}`
	s := mustParse(t, schema)
	v := int32(42)
	in := R{V: &v}
	dst := mustAppendEncode(t, s, nil, &in)
	// Decode back: pointer fields go through the slow path.
	var out R
	mustDecode(t, s, dst, &out)
	if out.V == nil || *out.V != 42 {
		t.Fatalf("got %v", out.V)
	}
}

func TestUnsafeDecodeTruncatedBuffer(t *testing.T) {
	// Here we drive the error branches in the unsafe deserializers (udBool,
	// udString, udBytesSlice) by feeding truncated data to a struct decode.
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

// numericKindTestType returns a reflect.Type whose Kind is k. We feed it to
// the unsafe per-kind ser/deser constructors (usInt/usLong/udInt/udLong/
// udDouble), which take the field's reflect.Type so SemanticError.GoType
// matches the reflect path. Test-only helper.
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

// TestUnsafeBytesDeserErrors covers the error paths in udBytesDeser: truncated
// input (a readVarlong error) and a negative length.
func TestUnsafeBytesDeserErrors(t *testing.T) {
	fn := udBytesDeser
	var buf [24]byte

	// Truncated input: readVarlong fails.
	_, err := fn([]byte{}, unsafe.Pointer(&buf[0]), &slab{})
	if err == nil {
		t.Fatal("expected error for truncated input")
	}

	// Negative length: unsigned varint 1 zigzag-decodes to -1.
	_, err = fn([]byte{0x01}, unsafe.Pointer(&buf[0]), &slab{})
	if err == nil {
		t.Fatal("expected error for negative bytes length")
	}
}

// TestMatrix_UnionEmitTagsAreFullLengthAtEveryBranch pins that a union's two
// emit-tag tables carry an entry for every branch. We decode through every
// branch index of every union shape with the tagging options on.
//
// maybeWrap picks branchNames or logicalNames by the wire's branch index. It
// indexes without a length check, so a table that skipped entries would panic
// on exactly the branches it skipped. Entries get skipped for branches with no
// logical type, or whose qualified spelling another branch already owns. The
// axes are the union's shape, the branch actually on the wire, and how we
// compiled the union: directly by a parse, or by a resolution, which builds
// these tables at three separate sites of its own.
//
// The assertion is answerable from the input. The options say a decoded union
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
		// Two qualified branches. Each must keep its own qualification, so a
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
						// returns before the tables on an invalid element.
						// So the null branch is the one index that does
						// *not* reach them, and it must stay bare.
						if got["u"] != nil {
							t.Fatalf("null branch decoded %#v, want a bare nil", got["u"])
						}
						return
					}
					m, ok := got["u"].(map[string]any)
					if !ok || len(m) != 1 {
						t.Fatalf("branch %d decoded %#v, want a one-key envelope", branch, got["u"])
					}
					// We want the exact tag, not merely some tag. A
					// table that shifted or dropped a qualification
					// still produces an envelope, and the wrong one
					// routes a re-encode to a different branch.
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
// compilers decline, rather than fault, whenever the metadata a shape needs
// is missing, on both wires.
//
// A record field names its Avro type by asking its fieldMeta. A field carrying
// no meta names no type and reaches no shape arm at all. The other cells name
// a shape but withhold the inner metadata that shape needs. The axes are the
// wire (encode and decode compile through separate switches over one
// vocabulary) and the shape whose metadata we withhold.
//
// Every cell carries a *complete* twin: the same shape and the same Go type
// with the metadata supplied, which must compile to a non-nil fast path.
// Without it a cell proves only that something declined, which the kind checks
// would also do. The twin is what attributes the refusal to the missing
// metadata.
func TestMatrix_FastPathDeclinesOnIncompleteFieldMeta(t *testing.T) {
	// A real record's compiled tables, so the record cells' complete twins
	// carry something the compiler actually accepts.
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

	// The record arm needs a real compiled table on both wires, so we cross
	// it separately rather than force it into the table above.
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

	// Element shapes an array declines for reasons of its own. We keep them
	// because they are the arms that walk the inner metadata one level deeper.
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
// and usArrayDirect, via a synthetic error-returning userfn.
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
	// A nil pointer must trigger errUnsafeNilPtr via the fast path.
	_, err := s.AppendEncode(nil, &S{X: nil})
	if err == nil {
		t.Fatal("expected error for nil pointer field")
	}
}

// TestSerRecordSlowPath covers the reflect-based slow path in serRecord.ser,
// through a non-addressable struct value rather than a pointer.
func TestSerRecordSlowPath(t *testing.T) {
	type S struct {
		X int32 `avro:"x"`
	}
	s, err := Parse(`{"type":"record","name":"S","fields":[{"name":"x","type":"int"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	// Encode struct value (not pointer): v.CanAddr() is false, so slow path.
	got, err := s.AppendEncode(nil, S{X: 42})
	if err != nil {
		t.Fatalf("slow path encode: %v", err)
	}
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
	// Non-addressable struct value with nil pointer field: slow path + error.
	_, err := s.AppendEncode(nil, S{X: 42, Y: nil})
	if err == nil {
		t.Fatal("expected error for nil pointer in slow path")
	}
}

// -----------------------------------------------------------------------
// Coverage tests for optimization fast paths
// -----------------------------------------------------------------------

func TestGenericUnionRoundTrip(t *testing.T) {
	// A multi-branch union ["int","string"] takes us through the generic
	// serUnion.ser and deserUnion.deser paths, not the null-union fast path.
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
	// 5 bytes all with continuation bit set: overflow error.
	_, _, err := readUvarint([]byte{0x80, 0x80, 0x80, 0x80, 0x80})
	if err == nil {
		t.Fatal("expected overflow error")
	}
}

func TestReadUvarlongOverflow(t *testing.T) {
	// 10 bytes all with continuation bit set: overflow error.
	_, _, err := readUvarlong([]byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80})
	if err == nil {
		t.Fatal("expected overflow error")
	}
}

func TestReadUvarlongShort10(t *testing.T) {
	// 9 continuation bytes with no 10th byte: short buffer.
	_, _, err := readUvarlong([]byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80})
	if err == nil {
		t.Fatal("expected short buffer error")
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

// TestArrayFastLoopErrors covers the error paths inside the specialized fast
// loop functions (deserArrayStringLoop, etc.). We craft payloads whose block
// count passes the outer sanity check but whose element data is malformed.
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

// TestMapFastBlockErrors covers the error paths inside the specialized fast
// block functions. We craft payloads whose block count passes the outer sanity
// check but whose key or value data is malformed.
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
		// We use a map with record values to reach the slow path (non-fast-block).
		// The first entry is valid; we truncate the second entry's value data.
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
	// We use an array of records (non-primitive) to reach the generic deser
	// path. We decode into any so we go through the reflect path, not the
	// unsafe struct path.
	schema := `{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"n","type":"int"}]}}`
	s := mustParse(t, schema)
	// We craft the binary so count=2 passes the sanity check and record1
	// decodes OK. record2 has a malformed varint, hitting the deserItem
	// error at L508. count=2 (varlong 0x04), record1 n=1 (varint 0x02),
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
		P *int32 `avro:"p"` // non-null-union pointer; nil errors
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
		// No byte-size follows: readVarlong error.
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
// Here we feed crafted malicious inputs to the unsafe fast paths. Length lies,
// count overflows, truncated data, and other adversarial patterns must all be
// caught cleanly, without memory corruption.

// TestAdversarialStringLengthLie drives the unsafe string deserializer
// (udStringDeser) with wire data whose encoded length exceeds the bytes
// available in src.
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
		{"negative", []byte{0x01}}, // zigzag 1 -> -1
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

// TestAdversarialBytesLengthLie drives the unsafe bytes deserializer
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
		{"negative", []byte{0x01}}, // zigzag -> -1
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

// TestAdversarialArrayCountLie drives the unsafe array deserializers
// (udArrayDirect and udArrayPtrRecord) with block counts that lie about the
// number of items available in the data.
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

// TestAdversarialMinInt64BlockCount pins that we reject a crafted block count
// of math.MinInt64. -MinInt64 overflows to MinInt64 in two's complement.
// Without an explicit check the count stays negative and panics in SetLen.
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

// TestAdversarialTruncationSweep encodes a valid multi-field record, then we
// decode every possible truncation of the encoded bytes. Every prefix shorter
// than the full encoding must error, not panic.
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

// TestAdversarialNestedRecordTruncation drives truncation within nested
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

// TestAdversarialRedecodeOverwrite pins that decoding into a struct that
// already holds values overwrites every field through the unsafe fast path.
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

// TestAdversarialMapKeyLengthLie drives map deserialization with adversarial
// key lengths.
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

// TestAdversarialZeroLengthValues drives zero-length strings, bytes, and
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

// ---- TextUnmarshaler tests ----

type testTextUnmarshaler struct{ val string }

func (tu *testTextUnmarshaler) UnmarshalText(text []byte) error {
	tu.val = "unmarshaled:" + string(text)
	return nil
}

var _ encoding.TextUnmarshaler = (*testTextUnmarshaler)(nil)

// ---- time.Time logical type tests ----

func TestDateRoundTrip(t *testing.T) {
	schema := `{"type":"int","logicalType":"date"}`
	// A date at midnight UTC.
	input := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	got := roundTrip(t, schema, input)
	if !got.Equal(input) {
		t.Fatalf("date round trip: got %v, want %v", got, input)
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

// ---- Logical type deser fallback to int/uint ----

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

// ---- omitzero slow path (non-addressable struct) ----

// ---- validateDefault extra coverage ----

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
	// We round-trip a plain string: the logical is dropped, so encode and
	// decode are bare string.
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

func TestDurationShortBuffer(t *testing.T) {
	schema := `{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}`
	s := mustParse(t, schema)
	// Only 11 bytes, needs 12.
	short := make([]byte, 11)
	var out Duration
	_, err := s.Decode(short, &out)
	if err == nil {
		t.Fatal("expected error for short buffer")
	}
}

// ---- Coverage: timestamp-nanos / local-timestamp-nanos ----

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

// ---- Decimal logical type (fixed) ----

// ---- Duration/Decimal: fallback to raw byte types ----

// ---- Duration in record as [12]byte triggers unsafe fallback ----

// ---- Decimal: bigIntToBytes edge cases ----

// ---- Decimal: interface deserialization ----

// ---- Decimal: type mismatch error ----

// ---- Bytes decimal: truncated varint ----

// ---- Decimal: empty bytes (zero length), bytesToBigInt empty ----

// ---- Fixed decimal: negative sign extension padding (ser) ----

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

func TestParseUUIDInvalidHex(t *testing.T) {
	// One corrupt hex segment per case, to hit every parseUUID error branch.
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
	// fromSchemaIgnoreInvalid, a silent drop). The schema parses as bare
	// fixed(16), and we treat the wire bytes opaquely.
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

// ---- Coverage: UUID ser with [16]byte through non-record path ----

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

// ---- Coverage: array/map block count exceeding buffer ----

func TestArrayBlockCountExceedsBuffer(t *testing.T) {
	s, _ := Parse(`{"type":"array","items":"int"}`)
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

func TestNullSecondUnionRoundTrip(t *testing.T) {
	// Various types in ["T", "null"] unions.
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

func TestFixedSliceRoundTrip(t *testing.T) {
	// Fixed-type values must survive encode and decode as []byte.
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

// TestRegression_WriterUnionBranchMismatchFailsFast locks in the fail-fast
// posture we take on writer-union resolution: every writer branch must be
// compatible with the reader at Resolve time, and we return the first
// incompatibility eagerly. This deliberately diverges from Java's
// Resolver.WriterUnion (per-branch ErrorAction deferred to decode time) and
// fastavro's read_union; see checkWriterUnion's doc for the rationale. A
// producer that narrowed during evolution but never emits the dropped branch
// must update its schema first.
func TestRegression_WriterUnionBranchMismatchFailsFast(t *testing.T) {
	writer := MustParse(`["null","string"]`)
	reader := MustParse(`"string"`)
	if _, err := Resolve(writer, reader); err == nil {
		t.Fatal("expected Resolve to fail eagerly when a writer branch (null) is incompatible with the reader (string)")
	}
}

// TestMatrix_InvalidDecimalRejected verifies that malformed decimal
// logical types (precision <= 0, scale > precision, missing precision,
// precision exceeding fixed capacity) are rejected at parse time,
// aligning with fastavro's parse_schema hard-rejects (negative values
// and scale > precision; its truthiness guards skip 0/missing, observed
// 1.12.2). Java's Decimal.validate throws for each, but schema parse
// catches the throw (fromSchemaIgnoreInvalid) and soft-drops the
// logical. Silently stripping the logical type and treating the schema as
// plain bytes/fixed is exactly the interop hazard rejecting avoids.
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
// cap is cumulative: chunking the count across multiple sub-cap blocks
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

// TestRegression_JSONBytesUnicodeCharCodepointSemantics locks in spec /
// Java parity: per the Avro 1.12 JSON-encoding section, "each character
// represents one byte" and "Unicode code points 0-255 are mapped to
// unsigned 8-bit byte values 0-255". Java decodes JSON strings into
// Java Strings (UTF-8 to UTF-16) then maps each char to a byte via
// ISO-8859-1; fastavro does the same via str.encode("iso-8859-1").
// We must apply codepoint mapping. Walking raw input bytes one by one
// would decode the JSON literal "é" (UTF-8 c3 a9) to [0xC3, 0xA9]
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

			// 1. any-target: empty-interface arm of setIface.
			var anyT any
			if _, err := s.Decode(enc, &anyT); err != nil {
				t.Fatalf("any: %v", err)
			}
			if _, ok := anyT.(time.Time); !ok {
				t.Errorf("expected time.Time, got %T", anyT)
			}

			// 2. time.Time target: timeType arm.
			var typed time.Time
			if _, err := s.Decode(enc, &typed); err != nil {
				t.Fatalf("time.Time: %v", err)
			}
			if !typed.Equal(tc.in) {
				t.Errorf("got %v want %v", typed, tc.in)
			}

			// 3. typed-interface implemented by time.Time: setIface
			//    AssignableTo(true) accepts.
			var st stringerIface
			if _, err := s.Decode(enc, &st); err != nil {
				t.Errorf("Stringer: time.Time should be assignable: %v", err)
			}

			// 4. typed-interface NOT implemented: setIface rejects.
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

type tmDurField struct {
	F time.Duration `avro:"f"`
}

type tmTimeField struct {
	F time.Time `avro:"f"`
}

type tmIntField struct {
	F int64 `avro:"f"`
}

// TestMatrix_TimeMicrosOverflowGuardIsUniform crosses the overflow guard's
// three axes at once. The guard lives in one conversion helper so every caller
// rejects the same values. "Every caller" is the claim, but we reached it one
// caller and one target at a time. No cell asked whether the four callers
// agree, and nothing reached the time.Time target's overflow arm.
//
//	caller  binary safe, binary unsafe (a struct field), JSON typed, JSON any
//	target  time.Duration, time.Time, any, and int64 as the control
//	value   in range, both boundaries, and one past each boundary
//
// The rule is stated independently of the code. A value overflows exactly when
// it cannot be scaled to nanoseconds inside an int64. The guard must fire for
// every target that materializes a duration and none that does not. int64 is
// in the matrix precisely because it must keep accepting the overflowing
// values. The boundary cells are the two immediately inside the limit, so an
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
	// The wire is written through the plain long schema, so the payload is
	// whatever int64 the cell names: the encoder's own range checks cannot
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

// TestRegression_RecordArrayIntPtrAddrVsByValParity locks in that
// addressable and by-value record encoding produce the same result for
// a []*int32 array field. The safe specialized serArray.serInt must
// unwrap both reflect.Interface and reflect.Pointer (matching the
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
// per-digit pre-multiply bound that is safe near 2^64/9, the boundary where the
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
// (serUnion.ser -> branch tryAll) errors with "no matching branch",
// Java/fastavro reject the same input (UnresolvedUnionException /
// "do not match"), and our own DecodeJSON rejects null against a
// no-null union (see
// TestRegression_UnionWithoutNullBranchAcceptsJsonNull). Encoding
// "null" here would produce output we cannot read back.
func TestRegression_EncodeJSONNilPtrIntoNonNullableUnion(t *testing.T) {
	s := MustParse(`["int","string"]`)
	var p *int
	out, err := s.EncodeJSON(p)
	if err == nil {
		t.Errorf("EncodeJSON(*int(nil)) into [int,string]: got %q, want error", out)
	}
}

// TestMatrix_EncodeJSONNullParity locks binary/JSON encode parity for the plain
// "null" type across every site reaching serNull or appendAvroJSON's case
// "null". Both must (1) reject non-nil non-nilable values with errNonNil (the
// JSON arm cannot just emit literal `null` regardless of v) and (2) accept
// typed-nil values arriving via an Interface wrapper, since generic serUnion /
// serArray / serMap dispatch calls serNull with Kind=Interface and both sides
// must peel before the kind switch.
//
// The 2-branch [null,T] optimization is unaffected (serNullUnionAt calls
// isNilValue, which peels interfaces); the concern is 3+ branch dispatch and
// the array<null>/map<null>/null-typed-field cases.
//
// The matrix covers both directions at the four sites routing through serNull's
// kind-switch: a top-level "null" schema, a null-typed record field, a
// tagged-union null branch in a 3+ branch union, and array<null> items /
// map<null> values.
//
// Cross-impl: Java and fastavro are silently lenient on both wires
// (GenericDatumWriter.NULL writes the marker without checking datum; same in
// write_null). Our binary path is deliberately strict per
// TestSerNullNonNilableType, so we bring JSON to the strict choice rather
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
		// Symmetric positive case: a typed-nil map *is* a valid no-value
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
	// top-level uses serNullUnionAt -> isNilValue and is indirect-aware
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
		// We pin it so a future change to that path cannot regress.
		s := MustParse(`{"type":"record","name":"R","fields":[{"name":"x","type":"null"}]}`)
		parity(t, s, map[string]any{"x": (*int)(nil)})
	})
}

// TestMatrix_EncodeJSONNullParityPointerToNilPointer extends the null-parity net
// to the **T-with-nil-inner shape: a non-nil outer pointer whose Elem() is a nil
// pointer, with or without an enclosing any{} wrapper. The 2-branch [null,T]
// optimization works via isNilValue, which peels both Pointer and Interface, but
// serNull peeled only Interface, so the outer Pointer's IsNil()==false reached
// the kind switch and errNonNil came back, while JSON's appendAvroJSON indirect
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
// Pointer/Interface, so a bare nil Map never landed on case "null". Binary
// picked null while JSON returned "no union branch matched", or silently
// emitted "" for nil []byte. The fix drops the null-skip; case "null" rejects
// non-nil with errNonNil so non-nil inputs fall cleanly through.
//
// Distinct from TestMatrix_EncodeJSONNullParity, which covers *tagged*
// dispatch. The bare form goes through unionTypeNameForValue into try-each,
// exactly where the null-skip blocked it. Every existing parity test passes
// the typed-nil through the tagged form, so widening serNull's peel alone
// hides the bug.
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

// TestMatrix_EncodeJSONNullBytesUnionParity locks the "Go nil = absent -> null
// branch" semantic uniformly across union arities and encoders: all four
// dispatch sites must agree: binary 2-branch (serNullUnionAt -> isNilValue),
// binary N-branch, JSON N-branch, and JSON tagged. A nil-first short-circuit at
// the entry of serUnion.ser and appendAvroJSONUnion applies the general rule;
// without it, three near-identical schemas give three results for []byte(nil),
// N-branch type-name dispatch naming Slice<uint8> "bytes" regardless of IsNil.
// Dropping the null-skip from try-each alone does not suffice: type-name
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
		// 3-branch: nil-first dispatch wins on both sides, picking
		// null uniformly with the 2-branch case. Without the nil-first
		// short-circuit, binary would pick bytes (idx 2 -> wire [4, 0])
		// and JSON would pick bytes (`""`), a binary 2-branch versus
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
			wantBin:  []byte{4}, // null branch (idx 2 -> zigzag 4)
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
// v.IsNil() for {Pointer, Interface, Map, Slice, Chan, Func}, but isNilValue,
// used only by the 2-branch optimization, peeled Pointer/Interface while its
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

type myKey string

// TestMatrix_MapNamedStringKeyEveryPath drives a map whose key type is a
// named string through every codec path that walks map keys by reflection.
// reflect.Value.MapIndex wants a key of the map's exact key type, so a bare
// string key panics on such a map even though the conversion compiles, and
// each path has its own MapIndex or SetMapIndex call.
func TestMatrix_MapNamedStringKeyEveryPath(t *testing.T) {
	record := MustParse(`{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`)
	for _, c := range []struct {
		name string
		run  func(t *testing.T) error
	}{
		{"binary encode record", func(t *testing.T) error {
			_, err := record.AppendEncode(nil, map[myKey]any{myKey("x"): int32(7)})
			return err
		}},
		{"JSON encode record", func(t *testing.T) error {
			_, err := record.EncodeJSON(map[myKey]any{myKey("x"): int32(7)})
			return err
		}},
		{"binary decode map", func(t *testing.T) error {
			s := MustParse(`{"type":"map","values":"int"}`)
			var got map[myKey]int32
			if _, err := s.Decode(mustAppendEncode(t, s, nil, map[string]int32{"x": 7}), &got); err != nil {
				return err
			}
			if got[myKey("x")] != 7 {
				return fmt.Errorf("got %v, want map[x:7]", got)
			}
			return nil
		}},
		{"binary decode map string fast block", func(t *testing.T) error {
			s := MustParse(`{"type":"map","values":"string"}`)
			var got map[myKey]string
			if _, err := s.Decode(mustAppendEncode(t, s, nil, map[string]string{"x": "hi"}), &got); err != nil {
				return err
			}
			if got[myKey("x")] != "hi" {
				return fmt.Errorf("got %v, want map[x:hi]", got)
			}
			return nil
		}},
		{"JSON decode map", func(t *testing.T) error {
			var got map[myKey]int32
			if err := MustParse(`{"type":"map","values":"int"}`).DecodeJSON([]byte(`{"x":7}`), &got); err != nil {
				return err
			}
			if got[myKey("x")] != 7 {
				return fmt.Errorf("got %v, want map[x:7]", got)
			}
			return nil
		}},
		{"JSON decode record", func(t *testing.T) error {
			var got map[myKey]any
			return record.DecodeJSON([]byte(`{"x":7}`), &got)
		}},
		{"resolved decode record", func(t *testing.T) error {
			writer := MustParse(`{"type":"record","name":"R","fields":[{"name":"x","type":"int"},{"name":"y","type":"int"}]}`)
			var got map[myKey]int32
			_, err := mustResolve(t, writer, record).Decode(mustAppendEncode(t, writer, nil, map[string]int32{"x": 1, "y": 2}), &got)
			return err
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panicked on a named string key: %v", r)
				}
			}()
			if err := c.run(t); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestRegression_DecodeJSONDecimalRecordFloatField covers the typical
// user shape: a record with a float-typed field backed by a decimal
// logical type. The struct-decode path delegates to the same decimal
// arm, so binary and JSON parity for the leaf flows up to records.
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

// TestRegression_ResolveReaderUnionAmbiguousUnqualifiedNames locks that Resolve
// picks the correct full-name match when a reader union contains two named types
// with the same unqualified name in different namespaces, a configuration the
// spec explicitly permits. findMatchingBranch / namesMatch must match by *full*
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
// v.Kind() == reflect.Map *and* the key kind: without the key check,
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
// recurse through Elem() until reaching the concrete primitive type;
// a single-level unwrap would reject **T as "cannot use *int32
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
	// A record with an int field, but truncated data.
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
	// The canonical forms come out the same here, so Resolve would
	// short-circuit. We call resolveEnum directly instead.
	resolved, err := resolveEnum(reader.node, writer.node, &resolveCtx{seen: make(map[nodePair]*schemaNode)})
	if err != nil {
		t.Fatal(err)
	}
	// Identity: should return reader.node directly.
	if resolved != reader.node {
		t.Fatal("expected identity path to return reader node")
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

func TestResolveDeserReadError(t *testing.T) {
	// Writer and reader both have field A but promoted (int to long).
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
	// Struct is missing a field the reader schema expects: typeFieldMapping error.
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

// ---------- map_setiter_test.go ----------

// long-to-int JSON native decode must never silently truncate: a wire value
// outside int32 range into []int / map[string]int is preserved on 64-bit
// (native) and rejected on 32-bit (reflect fallback), but never garbage. The
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
// (deserArrayStringLoop / JSON reflect): the native loop's exact-string
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

// appendMapPrimitive, serMap.ser, and the JSON map encoder reuse two addressable
// Values via SetIterKey/SetIterValue instead of allocating a fresh Value per
// entry. Because the reused value Value is addressable (iter.Value() is not), a
// struct-valued map now reaches serRecord's unsafe fast path. Here we pin that
// the change is behavior-neutral: every map shape round-trips on both wires to
// a deep-equal value, and the struct-valued map's record bytes match a
// standalone encode. Maps iterate in randomized order, so multi-entry wire is
// not byte-stable. We compare decoded values instead, except for the
// deterministic single-entry struct case.

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
// defined slice type and a builtin slice of a defined element type both
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

// TestMatrix_ArrayElementSwitchMatchesGeneral is the array sibling of
// TestMatrix_MapValueSwitchMatchesGeneral. The map net crosses the Go type's
// definedness (builtin value type against a defined one) over every primitive.
// The array nets never did. Every array cell handed the decoder a builtin slice
// of a builtin element and took the native loop, so the reflect-typed fallback
// went unrun for every primitive.
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
				// We compare the JSON round trip against the builtin
				// shape's JSON round trip, not against the binary
				// wire. The JSON representation of any NaN is the
				// bare token NaN, which carries no payload, so a
				// signaling NaN is quieted on that wire for every
				// shape alike. That is a property of the
				// representation, not a divergence between shapes,
				// and the question here is whether the shapes agree.
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
// entry, so the order is deterministic. A builtin value type takes the native
// path; a same-underlying named type takes the reflect (appendAvroJSON) path.
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
	var nsOut nsInt // named slice, fallback
	mustDecodeJSON(t, arrS, aj, &nsOut)
	if !reflect.DeepEqual([]int32(nsOut), in) {
		t.Fatalf("json named-slice fallback: %v", nsOut)
	}
	var neOut []nElem // named elem, fallback
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
// Elem()==int32, so it enters appendMapPrimitive's native switch, but
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
// struct standalone. Single entry, so the wire layout is deterministic:
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

// Union-branch selection: the index must give the scan's verdict.
//
// Which reader branch a writer node selects is a rule with four ranks (full
// name, alias, unqualified short name, bare-alias short name) plus numeric and
// string/bytes promotion, and a fixed's size folded into the match rather than
// checked after it. Answering it by ranking every reader branch is a scan inside
// the loop both Resolve and CheckCompatibility run, so the answer is now indexed
// ahead of the questions.
//
// Indexing a rule is where a rule quietly changes. Java's
// Resolver.firstMatchingBranch scans per writer branch too, so there is no
// reference to re-derive the verdict from: the only thing that can catch a
// drift is the scan itself, stated independently and asked the same questions.

// matchTierOracle ranks how strongly a reader branch matches a writer node.
// This is the rule written out longhand from the spec clauses, rather than
// read off branchMatchTiers. It is an independent
// statement of what the index is supposed to encode. A disagreement means the
// index changed a verdict rather than only its cost.
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
			// Size is part of the *match* predicate for fixed, not a
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
		// the reversed pair proves the winner is the name and not the order.
		`[{"type":"record","name":"a.R","fields":[]},{"type":"record","name":"b.R","fields":[]}]`,
		`[{"type":"record","name":"b.R","fields":[]},{"type":"record","name":"a.R","fields":[]}]`,
		// A qualified alias matches a writer fullname exactly; a bare alias
		// short-matches any namespace. Both live beside a plain branch so the
		// tier that answers is observable.
		`[{"type":"record","name":"a.Q","aliases":["a.R"],"fields":[]}]`,
		`[{"type":"record","name":"a.Q","aliases":["R"],"fields":[]}]`,
		`[{"type":"record","name":"z.Z","fields":[]},{"type":"record","name":"a.Q","aliases":["a.R"],"fields":[]}]`,
		// Same short name, different sizes: the 4-size writer must skip past
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
// rank actually answers for some cell: a rank whose writerName never returns a
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

// ---------- enum_ordinal_overflow_test.go ----------

// An integer-kind enum carrier is validated as an ordinal in [0, len(symbols))
// in the carrier's own width BEFORE narrowing to int. Narrowing first
// (int(v.Uint())) truncates a value ≥ 2^32 to its low bits on a 32-bit build,
// so an out-of-range ordinal like uint64(1<<32+5) would wrap to 5 and encode
// the wrong symbol there while erroring on 64-bit, a platform-dependent
// silent-wrong-output divergence. The wide comparison rejects it on every
// platform; this also pins that the error reports the true value, not a
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

	// Boundaries that must still encode: valid ordinals across int/uint carriers.
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
// recursion-depth bound on the unsafe decode path: a self-referential record
// nested past maxDepth, decoded into an addressable struct, must error. Triage
// note: a scoped mutation run flagged the slab-depth bookkeeping as surviving,
// and this test does *not* kill those mutants. Verified by neutering each, the
// decode still errors, because the limit is enforced redundantly. The wire is
// hand-built because encode cannot produce an over-deep value; its own depth
// guard stops it first.
func TestRegression_UnsafeDecodeDepthBounded(t *testing.T) {
	// Node = record{ child: ["null", Node], v: int }, a self-referential
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
	// the unwound path; catches the inverse sl.depth-- on enter / ++ on exit).
	shallow := []byte{0x02, 0x00, 0x00, 0x00} // one level: child=Node{child=null,v=0}, v=0
	var sn Node
	if _, err := s.Decode(shallow, &sn); err != nil {
		t.Fatalf("shallow nested decode falsely rejected (depth guard mis-restored): %v", err)
	}
}

// ---------- logical_gate_test.go ----------

// jsonDecodeAppliesLogical derives its answer by probing decodeLogical*, so it
// can't drift from what decode actually does. This test independently pins the
// probe's output against the human-known transform set for every logical. If
// the probe's type-assertion logic is ever wrong, or a decodeLogical* change
// flips a logical's transform behavior, one of these explicit expectations
// fails, forcing a conscious review. Expected values are spelled out (not
// re-probed) so this is a genuine check, not a tautology.
func TestMatrix_JSONDecodeAppliesLogicalMatchesDecode(t *testing.T) {
	cases := []struct {
		kind, logical string
		size          int
		want          bool
	}{
		// Transforming logicals (decode into an enriched Go type).
		{"int", "date", 0, true},                    // -> time.Time
		{"int", "time-millis", 0, true},             // -> time.Duration
		{"long", "time-micros", 0, true},            // -> time.Duration
		{"long", "timestamp-millis", 0, true},       // -> time.Time
		{"long", "timestamp-micros", 0, true},       // -> time.Time
		{"long", "timestamp-nanos", 0, true},        // -> time.Time
		{"long", "local-timestamp-millis", 0, true}, // -> time.Time
		{"long", "local-timestamp-micros", 0, true}, // -> time.Time
		{"long", "local-timestamp-nanos", 0, true},  // -> time.Time
		{"bytes", "decimal", 0, true},               // -> *big.Rat
		{"fixed", "decimal", 8, true},               // -> *big.Rat
		{"bytes", "big-decimal", 0, true},           // -> *big.Rat
		{"fixed", "uuid", 16, true},                 // -> [16]byte
		{"fixed", "duration", 12, true},             // -> avro.Duration

		// uuid-on-string transforms for a *typed* target: decodeString parses the
		// hex-dash string into a [16]byte / UUID-typed target. Into *any/string
		// it is identity, but the gate must report the transform, so a no-Decode
		// CustomType installs the suppression wrapper and the raw decode matches
		// binary's deserString, which has no [16]byte arm.
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

		// Logical types on a kind they are NOT spec-valid for, reachable only
		// when a CustomType resurrects a soft-dropped non-standard placement.
		// uuid/duration are fixed-only, big-decimal is bytes-only. On the wrong
		// kind neither the *any decodeLogical{Bytes,Fixed} nor the typed-target
		// assignBytes transforms (assignBytes is kind-gated), so the decode is
		// raw on both wire formats and the probe must report false. Otherwise a
		// no-Decode CustomType would over-install the suppression wrapper for a
		// transform that no longer exists.
		{"bytes", "uuid", 0, false},
		{"bytes", "duration", 0, false},
		{"fixed", "big-decimal", 8, false},

		// Hostile fixed size: the probe must NOT allocate proportional to size.
		// jsonDecodeAppliesLogical caps its probe buffer at maxFixedLogicalLen+1.
		// A size > maxFixedLogicalLen is neither the uuid(16) nor duration(12)
		// length, so it yields the same answer the small non-match case does,
		// while decimal still transforms at any length. fixed size is
		// schema-controlled and only validated non-negative, so without the cap
		// make([]byte, size) here is a parse-time DoS. At 1<<62 a regressed cap
		// panics immediately with "makeslice: len out of range" (it exceeds the
		// runtime max alloc).
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

// TestDecodeStringIntoBytes tests deserString into []byte directly.
func TestDecodeStringIntoBytes(t *testing.T) {
	s, _ := Parse(`"string"`)
	binary, _ := s.Encode("hello")
	var got []byte
	mustDecode(t, s, binary, &got)
	if string(got) != "hello" {
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
	// rejection; see TestMatrix_EncodeJSONNullParity.
	t.Run("non-nil value rejected", func(t *testing.T) {
		if out, err := s.EncodeJSON("ignored"); err == nil {
			t.Errorf("expected error encoding non-nil value into null schema, got %s", out)
		}
	})
}

func TestEncodeJSONStructFieldError(t *testing.T) {
	type R struct {
		A bool `avro:"a"` // schema says int, will fail
	}
	s, _ := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	_, err := s.EncodeJSON(&R{A: true})
	if err == nil {
		t.Fatal("expected error encoding bool as int in struct")
	}
}

func TestEncodeJSONNilInterfaceInUnion(t *testing.T) {
	// Map with nil interface value for a union field: hits the IsNil check
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
	// Struct missing a required field: hits the typeFieldMapping error
	// in the record encoder.
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
	// Truncated data: readVarlong fails.
	var got [3]int32
	_, err := s.Decode([]byte{}, &got)
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestDeserFixedArrayNegBlockOverflow(t *testing.T) {
	s, _ := Parse(`{"type":"array","items":"int"}`)
	// MinInt64 zigzag-encoded: negating it still gives negative.
	data := []byte{0x01} // placeholder, overwritten with MinInt64 below
	// zigzag(MinInt64) = MaxUint64, which is 0xFF 0xFF ... 0xFF 0x01 (10 bytes)
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
	// No byte-size follows: truncated.
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
	// Pointer GoType match, but the encoder skips, so it falls through to raw
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

func TestCustomTypeFixedLogicalType(t *testing.T) {
	// Exercises hasMatchingCustomType("fixed", logical) path.
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
// field is absent, matching binary, where the pre-encoded defaultBytes
// round-trip through the same wrapped fn as a present field's wire bytes.
// Without it, applyFieldDefault dispatched through the *unwrapped* deser, built
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
		// One field present, one filled from default: both must produce
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
// converts user-Go-type to Avro-native, and the parsed default is already
// Avro-native, never having had a Go-domain representation, so the directional
// contract has nothing to apply. Pre-fix, appendJSONFieldDefault routed defaults
// through appendAvroJSON with a non-nil custom map, firing the user's Encode
// once per default-filled field where binary fired it zero times: benign for
// GoType-typed encoders that fall through on a type-assertion miss, but a
// surprise for the GoType=nil encoders used for logging or dispatch.
func TestRegression_EncodeJSONBypassesCustomEncoderForDefaultFill(t *testing.T) {
	// GoType=nil so the encoder fires on every value reaching the long+
	// money node, the instrumentation pattern that surfaces the asymmetry.
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

// A custom-decoded value whose decode target is a recursive pointer type
// (cyclic type graph: ctRecursivePtr's element is itself) must terminate with
// an error, not loop forever allocating a pointer level per iteration.
// setCustomResult's pointer walk is bounded by maxIndirectDepth, the same
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
// arithmetic, slicing, or a state transition on a value returned by user code:
// text-out methods beyond the plain-string positions
// text_appender_contract_test.go pins, TextUnmarshaler error returns, and
// CustomType Encode/Decode returns. The invariant per cell: a contract-violating
// return never panics through a public API and never silently corrupts sibling
// data. The lax-name validator, IsZero() bool, and the wire-side use of map keys
// are structurally immune. The first two return no value the library computes
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
// fixed(16)+uuid schema the 16 wire bytes are *derived* from the returned
// text (parseUUID), so wrong content is detectable and must reject. On a
// string+uuid schema the encoder is string-lenient (serUUID delegates
// non-[16]byte sources to the string encoder), so arbitrary text encodes
// verbatim. Those cells assert byte-parity with the plain-string twin
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
					// an error return is surfaced, all as *SemanticError.
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

	// Every input arm of the enum encoders -- plain string, named string
	// without text methods, text-out (covered above), int ordinal -- must
	// produce the same *SemanticError{AvroType: "enum"} identity on both
	// wires for a value naming no symbol / an out-of-range ordinal. The
	// cells run at top level deliberately: record positions wrap any
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

	// The remaining encode user-value failures already agree across the
	// two wires: nil-for-non-nullable is plain on both (its own family),
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

	// The encode-side unknown-symbol reject is a user-value failure and
	// carries *SemanticError identity on both wires (asserted above). The
	// decode-side counterparts -- a binary ordinal outside the symbol
	// table, a JSON string naming no symbol -- are wire-content failures,
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
// must surface, wrapped so the user's identity is preserved, from
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
// returns nil error, sibling fields hold their decoded values: a
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

	// An interface target accepts any result type: the callback's value
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
	// The *only* types where a caller-set exported field coexists with
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
// which selects a definition and so could substitute one schema for another: a
// presence flag decides only whether an attribute whose value is the field's own
// zero is written at all, never which value. So for every value a caller can set,
// the value that comes back is the value they set, flag set and flag clear,
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
// buf, so reclaiming that memory does not free it: it hands the same bytes
// out twice and silently mutates strings the caller already holds, long after
// Decode returned. A decoded string used as a map key changes value in place.
//
// The slab struct itself *is* pooled and reused (put deliberately retains buf),
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
// reclaiming the tail there (sl.buf = sl.buf[:0], or sl.buf = nil to "free"
// it) reads like an obvious win and costs nothing visible in any test that
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

//////////////////
// SKIP UNKNOWN //
//////////////////

// skipUnknownSchema exercises one field of every kind that skips differently:
// a varint, a length-prefixed run, both block-framed containers, a union, a
// fixed run, and a nested record whose own skip is compiled lazily.
const skipUnknownSchema = `{"type":"record","name":"R","fields":[
	{"name":"a","type":"int"},
	{"name":"b","type":"string"},
	{"name":"c","type":{"type":"array","items":"long"}},
	{"name":"d","type":["null","string"]},
	{"name":"e","type":{"type":"map","values":"bytes"}},
	{"name":"f","type":{"type":"fixed","name":"F","size":3}},
	{"name":"g","type":{"type":"record","name":"Inner","fields":[
		{"name":"x","type":"int"},
		{"name":"y","type":"string"},
		{"name":"z","type":{"type":"array","items":"string"}}]}},
	{"name":"h","type":"double"}
]}`

// skipUnknownPartial maps the first and last fields only, so a skip that
// over- or under-advances anywhere in between corrupts H rather than
// silently landing on the right byte.
type skipUnknownPartial struct {
	A int32   `avro:"a"`
	H float64 `avro:"h"`
}

// TestSkipUnknownDoesNotRelaxEncode is the hazard the option is deliberately
// one-sided about: a struct that does not cover the record would encode zeros
// for the fields it lacks.
func TestSkipUnknownDoesNotRelaxEncode(t *testing.T) {
	s := MustParse(skipUnknownSchema)
	v := skipUnknownPartial{A: 1, H: 2}
	for _, tc := range []struct {
		name string
		fn   func(opts ...Opt) error
	}{
		{"Encode", func(opts ...Opt) error { _, err := s.Encode(v, opts...); return err }},
		{"AppendEncode", func(opts ...Opt) error { _, err := s.AppendEncode(nil, v, opts...); return err }},
		{"EncodeJSON", func(opts ...Opt) error { _, err := s.EncodeJSON(v, opts...); return err }},
		{"AppendSingleObject", func(opts ...Opt) error { _, err := s.AppendSingleObject(nil, v, opts...); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The missing-field error, specifically: an encode that mapped
			// the record's fields loosely and then failed further along, on a
			// struct handed to a string encoder, would satisfy a bare "errored"
			// while having already relaxed the mapping.
			for _, opts := range [][]Opt{nil, {SkipUnknown()}} {
				err := tc.fn(opts...)
				if err == nil {
					t.Fatalf("opts=%v: encode accepted a partial struct", opts)
				}
				if !strings.Contains(err.Error(), "missing field") {
					t.Errorf("opts=%v: got %v, want the missing-field error", opts, err)
				}
			}
		})
	}
}

/////////////////
// ALIAS INPUT //
/////////////////

// aliasOffset reports the offset of p within src, or -1 when p points
// elsewhere. Go's collector does not move heap objects, so comparing the two as
// addresses holds for the life of the call.
func aliasOffset(p unsafe.Pointer, src []byte) int {
	if p == nil || len(src) == 0 {
		return -1
	}
	base := uintptr(unsafe.Pointer(unsafe.SliceData(src)))
	q := uintptr(p)
	if q < base || q >= base+uintptr(len(src)) {
		return -1
	}
	return int(q - base)
}
func bytePtr(b []byte) unsafe.Pointer { return unsafe.Pointer(unsafe.SliceData(b)) }

func mustAliasDecode(t *testing.T, s *Schema, wire []byte, v any, opts ...Opt) {
	t.Helper()
	if _, err := s.Decode(wire, v, opts...); err != nil {
		t.Fatalf("decode: %v", err)
	}
}

// TestAliasInputMutationIsVisible pins the contract rather than treating it as
// a bug: the decoded values *are* src, so writing to src rewrites them,
// including the strings, which Go otherwise guarantees are immutable.
func TestAliasInputMutationIsVisible(t *testing.T) {
	s := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"s","type":"string"},
		{"name":"b","type":"bytes"}]}`)
	var got struct {
		S string `avro:"s"`
		B []byte `avro:"b"`
	}
	wire := []byte{6, 'a', 'b', 'c', 6, 'x', 'y', 'z'}
	mustAliasDecode(t, s, wire, &got, AliasInput())
	if got.S != "abc" || string(got.B) != "xyz" {
		t.Fatalf("got %q / %q, want abc / xyz", got.S, got.B)
	}
	wire[1] = 'Z'
	wire[5] = 'Q'
	if got.S != "Zbc" {
		t.Errorf("string did not follow src: %q, want Zbc", got.S)
	}
	if string(got.B) != "Qyz" {
		t.Errorf("bytes did not follow src: %q, want Qyz", got.B)
	}

	// And the inverse, which is what every existing caller relies on.
	var plain struct {
		S string `avro:"s"`
		B []byte `avro:"b"`
	}
	wire2 := []byte{6, 'a', 'b', 'c', 6, 'x', 'y', 'z'}
	mustAliasDecode(t, s, wire2, &plain)
	wire2[1] = 'Z'
	wire2[5] = 'Q'
	if plain.S != "abc" || string(plain.B) != "xyz" {
		t.Errorf("no option: src mutation reached %q / %q", plain.S, plain.B)
	}
}

// TestAliasInputIsNotASchemaOpt is the structural half of "OCF never aliases
// its block buffer": ocf.WithSchemaOpts is the one path that forwards caller
// options into an OCF reader, and it takes SchemaOpts. AliasInput must stay out
// of that set: the block buffer is replaced every block.
func TestAliasInputIsNotASchemaOpt(t *testing.T) {
	if _, ok := any(AliasInput()).(SchemaOpt); ok {
		t.Error("AliasInput satisfies SchemaOpt; ocf.WithSchemaOpts would forward it into a reader whose block buffer is overwritten every block")
	}
	if _, ok := any(AliasInput()).(optmark.AliasesInput); !ok {
		t.Error("AliasInput does not implement optmark.AliasesInput; a host that reuses its decode buffer cannot drop it")
	}
}

// TestInvariant_ResolvedJSONDropsAliasingOpts reads decodeJSONResolved from
// source. The composed resolved-JSON path decodes from a re-encoded
// intermediate it allocates itself, so forwarding an aliasing option there
// points the caller's values at a buffer they never handed over and pins it for
// as long as one field is held. Nothing observable distinguishes it (the
// intermediate is per-call and unreachable), so a source guard is the only
// thing that can hold the forward honest.
func TestInvariant_ResolvedJSONDropsAliasingOpts(t *testing.T) {
	const decl = "func (s *Schema) decodeJSONResolved("
	src := readFile(t, "json_codec.go")
	i := strings.Index(src, decl)
	if i < 0 {
		t.Fatalf("%s not found in json_codec.go — this guard reads it from source, so a rename must update the guard, not silence it", decl)
	}
	body := src[i:]
	if e := strings.Index(body, "\n}\n"); e >= 0 {
		body = body[:e+2]
	}
	call := strings.Index(body, "s.Decode(")
	if call < 0 {
		t.Fatalf("decodeJSONResolved no longer calls s.Decode:\n\n%s", body)
	}
	line := body[call:]
	if e := strings.IndexByte(line, '\n'); e >= 0 {
		line = line[:e]
	}
	if !strings.Contains(line, "dropAliasingOpts(opts)") {
		t.Errorf("decodeJSONResolved forwards the caller's options raw:\n    %s\nThe buffer it decodes is an intermediate this function allocated, not the caller's src.", strings.TrimSpace(line))
	}
}

// foreignBufferDecodeSites rows every call in the package that hands a decode
// function a src buffer other than the one its caller is decoding. Such a
// buffer outlives the call or is shared, so under [AliasInput] a decoded value
// can point into memory the caller never supplied, which is sound only where
// the contract covers it. Each row states what makes its buffer safe to hand
// out.
//
// Keyed "enclosing func: callee: arg0", the spelling
// TestInvariant_ForeignBufferDecodeSitesAreRowed derives.
var foreignBufferDecodeSites = map[string]string{
	"deser: (&deserFixed{…}).deser: append(b[:0:0], b...)": "a fresh copy of just this value's payload, made so the delegate's remainder is the copy's tail rather than the caller's; nothing else can reach it, so aliasing it is aliasing per-call garbage the value itself keeps alive",

	"applyFieldDefault: node.deserRecord.fields[idx].fn: enc": "the schema's pre-encoded default; DecodeJSON never sets the alias flag, so this path copies out of it regardless",

	"deserInterface: d.deser: d.encodedDefault": "the schema's pre-encoded default, shared by every decode of this resolution; AliasInput's contract forbids writing to anything a decode returns, which is what makes handing it out sound",
	"deserMap: d.deser: d.encodedDefault":       "same buffer and same rule as deserInterface",
	"deserStruct: d.deser: d.encodedDefault":    "same buffer and same rule as deserInterface; a struct target is not a different contract",
}

// fallbackImporter tries compiled export data first and re-asks a source
// importer for anything it cannot resolve.
type fallbackImporter struct{ fast, slow types.Importer }

func (i fallbackImporter) Import(path string) (*types.Package, error) {
	if p, err := i.fast.Import(path); err == nil {
		return p, nil
	}
	return i.slow.Import(path)
}

// typedPackageFiles type-checks the package's non-test sources so a derivation
// can ask what a call's callee *is* rather than what it is spelled.
func typedPackageFiles(t *testing.T) (*token.FileSet, []*ast.File, *types.Package, *types.Info) {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading package dir: %v", err)
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, n, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", n, err)
		}
		files = append(files, f)
	}
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}}
	// Compiled export data for everything it covers, source only for what it
	// does not, which is this module's own internal package. Type-checking the
	// stdlib from source instead costs seconds, and we run inside the -race
	// gate, whose headroom is what we are conserving.
	conf := types.Config{Importer: fallbackImporter{
		fast: importer.Default(),
		slow: importer.ForCompiler(fset, "source", nil),
	}}
	pkg, err := conf.Check("github.com/twmb/avro", fset, files, info)
	if err != nil {
		t.Fatalf("type-checking the package: %v", err)
	}
	return fset, files, pkg, info
}

// TestInvariant_ForeignBufferDecodeSitesAreRowed derives, from the type
// checker rather than from a spelling, every call whose callee has a deserfn's
// or skipfn's signature and whose first argument is not the enclosing src, and
// requires each to be rowed. Signature rather than named type on purpose: a
// method value like (&deserFixed{n}).deser has the shape without the name, and
// keying on the name would drop it.
//
// It reds in both directions: a new site that feeds a decode some other buffer
// fails here, and a row whose site has vanished fails too.
func TestInvariant_ForeignBufferDecodeSitesAreRowed(t *testing.T) {
	fset, files, pkg, info := typedPackageFiles(t)
	sigOf := func(name string) *types.Signature {
		o := pkg.Scope().Lookup(name)
		if o == nil {
			t.Fatalf("%s is gone from the package; this derivation names it, so a rename must update the guard rather than silence it", name)
		}
		return o.Type().Underlying().(*types.Signature)
	}
	deser, skip := sigOf("deserfn"), sigOf("skipfn")

	found := map[string]bool{}
	for _, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				ce, ok := n.(*ast.CallExpr)
				if !ok || len(ce.Args) == 0 {
					return true
				}
				if tv, ok := info.Types[ce.Fun]; ok && tv.IsType() {
					return true // a deserfn(x) conversion, not a call
				}
				ft := info.TypeOf(ce.Fun)
				if ft == nil {
					return true
				}
				sig, ok := ft.Underlying().(*types.Signature)
				if !ok || (!types.Identical(sig, deser) && !types.Identical(sig, skip)) {
					return true
				}
				if a0 := types.ExprString(ce.Args[0]); a0 != "src" {
					found[fd.Name.Name+": "+types.ExprString(ce.Fun)+": "+a0] = true
				}
				return true
			})
		}
	}
	if len(found) == 0 {
		t.Fatalf("the derivation found no decode call at all across %d files; the walk is broken, not the package", len(files))
	}
	for site := range found {
		if _, ok := foreignBufferDecodeSites[site]; !ok {
			t.Errorf("%s\n  hands a decode a buffer that is not its caller's src. Row it with what makes that buffer safe to hand out, or pass the caller's src.", site)
		}
	}
	for site := range foreignBufferDecodeSites {
		if !found[site] {
			t.Errorf("foreignBufferDecodeSites rows %q, which the source no longer contains", site)
		}
	}
	_ = fset
}

// byteSourceCase drives one byte source, meaning the buffer a decoded value may
// point into, against a target shape. The axis exists because the alias
// contract is a rule about the values a decode returns, not about src: the
// memory behind one is src for a field read off the wire and the parsed Schema
// for a field filled from its default, and a net that only ever decodes
// wire-read fields cannot tell those apart.
type byteSourceCase struct {
	name string
	// decoder is called *once* per cell and returns the closure the cell drives.
	// Once, because the schema has to be built outside the two runs: the memory
	// a default-filled value points into belongs to one parsed schema, so a
	// cell that re-parses per run compares two schemas and can never observe
	// sharing, whatever the code does.
	decoder func(t *testing.T) func(src []byte) unsafe.Pointer
	// wire builds a fresh src for each run.
	wire func(t *testing.T) []byte
	// inSrc is whether the returned value must point *into* the src it was
	// handed. shared is whether two independent decodes must hand back the
	// same address, which is what "points at something the schema owns" looks
	// like from outside the package.
	inSrc  bool
	shared bool
}

// aliasResolvedDefaultSchemas builds writer and resolved reader for a record
// whose second field exists only on the reader, filled from a bytes default.
func aliasResolvedDefaultSchemas(t *testing.T) (writer, resolved *Schema) {
	t.Helper()
	w := MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	r := MustParse("{\"type\":\"record\",\"name\":\"R\",\"fields\":[" +
		"{\"name\":\"a\",\"type\":\"int\"}," +
		"{\"name\":\"d\",\"type\":\"bytes\",\"default\":\"\\u0001\\u0002\\u0003\"}]}")
	res, err := Resolve(w, r)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	return w, res
}

func aliasResolvedWire(t *testing.T) []byte {
	t.Helper()
	w, _ := aliasResolvedDefaultSchemas(t)
	b, err := w.Encode(map[string]any{"a": int32(7)})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return b
}

// aliasByteSourceCases crosses byte source with the target shapes that reach
// it. Expectations come from the contract, not from current behavior: a value
// read off the wire under AliasInput points into src and so cannot be shared
// between two decodes; a value filled from the schema's default points at
// memory the schema owns and so *is* shared; a path that does not honor the
// option points at neither.
var aliasByteSourceCases = []byteSourceCase{
	{
		name: "caller-src/struct",
		wire: func(t *testing.T) []byte { return []byte{6, 'a', 'b', 'c'} },
		decoder: func(t *testing.T) func([]byte) unsafe.Pointer {
			s := MustParse(`"bytes"`)
			return func(src []byte) unsafe.Pointer {
				var got []byte
				mustAliasDecode(t, s, src, &got, AliasInput())
				return bytePtr(got)
			}
		},
		inSrc: true, shared: false,
	},
	{
		name: "schema-default/struct",
		wire: aliasResolvedWire,
		decoder: func(t *testing.T) func([]byte) unsafe.Pointer {
			_, res := aliasResolvedDefaultSchemas(t)
			return func(src []byte) unsafe.Pointer {
				var got struct {
					A int32  `avro:"a"`
					D []byte `avro:"d"`
				}
				mustAliasDecode(t, res, src, &got, AliasInput())
				return bytePtr(got.D)
			}
		},
		inSrc: false, shared: true,
	},
	{
		name: "schema-default/map",
		wire: aliasResolvedWire,
		decoder: func(t *testing.T) func([]byte) unsafe.Pointer {
			_, res := aliasResolvedDefaultSchemas(t)
			return func(src []byte) unsafe.Pointer {
				got := map[string]any{}
				mustAliasDecode(t, res, src, &got, AliasInput())
				return bytePtr(got["d"].([]byte))
			}
		},
		inSrc: false, shared: true,
	},
	{
		name: "schema-default/interface",
		wire: aliasResolvedWire,
		decoder: func(t *testing.T) func([]byte) unsafe.Pointer {
			_, res := aliasResolvedDefaultSchemas(t)
			return func(src []byte) unsafe.Pointer {
				var got any
				mustAliasDecode(t, res, src, &got, AliasInput())
				return bytePtr(got.(map[string]any)["d"].([]byte))
			}
		},
		inSrc: false, shared: true,
	},
	{
		name: "schema-default/no-option",
		wire: aliasResolvedWire,
		decoder: func(t *testing.T) func([]byte) unsafe.Pointer {
			_, res := aliasResolvedDefaultSchemas(t)
			return func(src []byte) unsafe.Pointer {
				got := map[string]any{}
				mustAliasDecode(t, res, src, &got)
				return bytePtr(got["d"].([]byte))
			}
		},
		inSrc: false, shared: false,
	},
	{
		name: "json-default/ignores-option",
		wire: func(t *testing.T) []byte { return []byte(`{"a":7}`) },
		decoder: func(t *testing.T) func([]byte) unsafe.Pointer {
			s := MustParse("{\"type\":\"record\",\"name\":\"R\",\"fields\":[" +
				"{\"name\":\"a\",\"type\":\"int\"}," +
				"{\"name\":\"d\",\"type\":\"bytes\",\"default\":\"\\u0001\\u0002\\u0003\"}]}")
			return func(src []byte) unsafe.Pointer {
				got := map[string]any{}
				if err := s.DecodeJSON(src, &got, AliasInput()); err != nil {
					t.Fatalf("DecodeJSON: %v", err)
				}
				return bytePtr(got["d"].([]byte))
			}
		},
		inSrc: false, shared: false,
	},
	{
		name: "synthesized-copy/fixed-decimal-escape",
		wire: func(t *testing.T) []byte { return []byte{1, 2, 3, 4} },
		decoder: func(t *testing.T) func([]byte) unsafe.Pointer {
			s := MustParse(`{"type":"fixed","name":"D","size":4,"logicalType":"decimal","precision":9,"scale":2}`)
			return func(src []byte) unsafe.Pointer {
				var got []byte
				mustAliasDecode(t, s, src, &got, AliasInput())
				return bytePtr(got)
			}
		},
		inSrc: false, shared: false,
	},
}

// TestMatrix_AliasInputByteSource crosses the buffer a decoded value may point
// into with the target shape that reaches it.
//
// The oracle is memory, not a sibling decode path: where an address lies
// relative to a buffer the test allocated is answerable from the input alone,
// so a defect the aliasing and copying paths would share cannot hide behind
// their agreement.
func TestMatrix_AliasInputByteSource(t *testing.T) {
	sources := map[string]int{}
	for _, c := range aliasByteSourceCases {
		t.Run(c.name, func(t *testing.T) {
			sources[strings.SplitN(c.name, "/", 2)[0]]++

			decode := c.decoder(t)
			src1 := c.wire(t)
			// The rowed reason every foreign buffer is safe to hand over
			// starts with "no deserfn writes through its src". That is a claim
			// about behavior, so every cell checks it rather than trusting the
			// comment: a decode that scribbled on its input would rewrite the
			// schema's own default bytes on the cells that pass one.
			untouched := append([]byte(nil), src1...)
			p1 := decode(src1)
			if !bytes.Equal(src1, untouched) {
				t.Errorf("the decode wrote through its src: %x became %x", untouched, src1)
			}
			if p1 == nil {
				t.Fatal("decode returned no payload address; the cell measured nothing")
			}
			if in := aliasOffset(p1, src1) >= 0; in != c.inSrc {
				t.Errorf("value points into src = %v, want %v", in, c.inSrc)
			}

			// A second decode over a separate buffer: an address that repeats
			// cannot be a copy handed back twice, since the two runs allocate
			// independently.
			src2 := c.wire(t)
			p2 := decode(src2)
			if got := p1 == p2; got != c.shared {
				t.Errorf("two decodes share a backing address = %v, want %v", got, c.shared)
			}
			if c.inSrc && aliasOffset(p2, src2) < 0 {
				t.Error("second run stopped pointing into its own src")
			}
		})
	}
	// Liveness floor: every source arm must have actually run, or an axis this
	// matrix claims to cross is carrying no value.
	for _, want := range []string{"caller-src", "schema-default", "json-default", "synthesized-copy"} {
		if sources[want] == 0 {
			t.Errorf("byte source %q was never realized; the axis is dead", want)
		}
	}
	if sources["schema-default"] < 4 {
		t.Errorf("schema-default ran %d target shapes, want the struct/map/interface/no-option set", sources["schema-default"])
	}
}
