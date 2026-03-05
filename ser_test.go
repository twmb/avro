package avro

import (
	"encoding"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

type Superhero struct {
	ID            int32         `avro:"id"`
	AffiliationID int32         `avro:"affiliation_id"`
	Name          string        `avro:"name"`
	Life          float32       `avro:"life"`
	Energy        float32       `avro:"energy"`
	Powers        []*Superpower `avro:"powers"`
}

type Superpower struct {
	ID      int32   `avro:"id"`
	Name    string  `avro:"name"`
	Damage  float32 `avro:"damage"`
	Energy  float32 `avro:"energy"`
	Passive bool    `avro:"passive"`
}

func BenchmarkSerialize(b *testing.B) {
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

	s, err := Parse(`

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

}

]
`)
	if err != nil {
		b.Fatalf("unable to prime serializer: %v", err)
	}

	dst, _ := s.AppendEncode(nil, &superhero)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst, err = s.AppendEncode(dst[:0], &superhero)
		if err != nil {
			b.Fatalf("unable to encode: %v", err)
		}
	}
}

func BenchmarkRecursive(b *testing.B) {
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

	s, err := Parse(`
{
  "type": "record",
  "name": "LongList",
  "aliases": ["LinkedLongs"],
  "fields" : [
    {"name": "value", "type": "long"},
    {"name": "next", "type": ["null", "LongList"]}
  ]
}
`)
	if err != nil {
		b.Fatalf("unable to prime serializer: %v", err)
	}

	dst, _ := s.AppendEncode(nil, &llist)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst, err = s.AppendEncode(dst[:0], &llist)
		if err != nil {
			b.Fatalf("unable to encode: %v", err)
		}
	}
}

func TestEmbed(t *testing.T) {
	type BaseDataModel struct {
		ID int `json:"id" avro:"id"`
	}

	type UserDataModel struct {
		BaseDataModel
		Name string `json:"name" avro:"name"`
	}

	u := UserDataModel{
		BaseDataModel: BaseDataModel{
			ID: 1,
		},
		Name: "test",
	}

	s, err := Parse(`
{
  "type": "record",
  "name": "UDM",
  "fields" : [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"}
  ]
}
`)
	if err != nil {
		t.Fatalf("unable to prime serializer: %v", err)
	}

	dst, err := s.AppendEncode(nil, &u)
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	if len(dst) == 0 {
		t.Fatal("expected non-empty output for TestInterface")
	}
}

func encodeErr(t *testing.T, schema string, v any) {
	t.Helper()
	s, err := Parse(schema)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	_, err = s.AppendEncode(nil, v)
	if err == nil {
		t.Fatal("expected encode error, got nil")
	}
}

func TestSerTypeMismatch(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		val    any
	}{
		// primitives
		{"boolean from string", `"boolean"`, new("true")},
		{"int from bool", `"int"`, new(true)},
		{"int from string", `"int"`, new("42")},
		{"long from bool", `"long"`, new(true)},
		{"long from string", `"long"`, new("42")},
		{"float from string", `"float"`, new("3.14")},
		{"double from string", `"double"`, new("3.14")},
		{"bytes from string", `"bytes"`, new("hello")},
		{"bytes from int slice", `"bytes"`, new([]int{1, 2})},
		{"string from int", `"string"`, new(42)},

		// complex
		{"array from string", `{"type":"array","items":"int"}`, new("hello")},
		{"map from string", `{"type":"map","values":"int"}`, new("hello")},
		{"map from int-key map", `{"type":"map","values":"int"}`, new(map[int]int32{1: 2})},
		{"fixed from int array", `{"type":"fixed","name":"f","size":4}`, new([4]int{1, 2, 3, 4})},
		{"fixed wrong size array", `{"type":"fixed","name":"f","size":4}`, new([3]byte{1, 2, 3})},
		{"fixed wrong size slice", `{"type":"fixed","name":"f","size":4}`, new([]byte{1, 2, 3})},
		{"record from int", `{"type":"record","name":"r","fields":[{"name":"a","type":"int"}]}`, new(42)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encodeErr(t, tt.schema, tt.val)
		})
	}
}

func TestSerNilPointer(t *testing.T) {
	encodeErr(t, `"int"`, new((*int32)(nil)))
}

func TestSerNilInterface(t *testing.T) {
	var v stringer
	encodeErr(t, `"string"`, &v)
}

func TestSerEnumErrors(t *testing.T) {
	schema := `{"type":"enum","name":"e","symbols":["a","b","c"]}`

	t.Run("unknown symbol", func(t *testing.T) {
		encodeErr(t, schema, new("unknown"))
	})

	t.Run("out of range int", func(t *testing.T) {
		encodeErr(t, schema, new(int32(-1)))
	})

	t.Run("type mismatch", func(t *testing.T) {
		encodeErr(t, schema, new(3.14))
	})

	t.Run("uint encode", func(t *testing.T) {
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		dst, err := s.AppendEncode(nil, new(uint(1)))
		if err != nil {
			t.Fatalf("encode uint enum: %v", err)
		}
		if len(dst) == 0 {
			t.Fatal("expected non-empty output")
		}
	})

	t.Run("int encode", func(t *testing.T) {
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		dst, err := s.AppendEncode(nil, new(int(1)))
		if err != nil {
			t.Fatalf("encode int enum: %v", err)
		}
		if len(dst) == 0 {
			t.Fatal("expected non-empty output")
		}
	})
}

func TestSerRecordAsMap(t *testing.T) {
	schema := `{"type":"record","name":"r","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}
	]}`

	t.Run("success", func(t *testing.T) {
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		m := map[string]any{"a": int32(42), "b": "hello"}
		dst, err := s.AppendEncode(nil, &m)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		if len(dst) == 0 {
			t.Fatal("expected non-empty output")
		}
	})

	t.Run("missing key", func(t *testing.T) {
		m := map[string]any{"a": int32(42)}
		encodeErr(t, schema, &m)
	})
}

func TestSerUnionAllFail(t *testing.T) {
	encodeErr(t, `["null","int"]`, new("hello"))
}

func TestSerNullNonNilableType(t *testing.T) {
	// serNull should not panic when given a non-nilable type (int, string, etc.).
	// It should return errNonNil, not crash.
	v := reflect.ValueOf(42)
	_, err := serNull(nil, v)
	if err != errNonNil {
		t.Fatalf("expected errNonNil, got %v", err)
	}
	v = reflect.ValueOf("hello")
	_, err = serNull(nil, v)
	if err != errNonNil {
		t.Fatalf("expected errNonNil, got %v", err)
	}
}

func TestSerNullGenericUnionNonNilable(t *testing.T) {
	// 3-branch union takes the generic serUnion.ser path, which tries
	// serNull first. This would panic on non-nilable types before the fix.
	s, err := Parse(`["null","int","string"]`)
	if err != nil {
		t.Fatal(err)
	}
	// int32 is non-nilable; serNull must not panic, and the int branch should match.
	dst, err := s.AppendEncode(nil, new(int32(42)))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(dst) == 0 {
		t.Fatal("expected non-empty output")
	}
}

type textMarshalerType struct{ val string }

func (tm textMarshalerType) MarshalText() ([]byte, error) { return []byte(tm.val), nil }

var _ encoding.TextMarshaler = textMarshalerType{}

type textMarshalerErr struct{}

func (textMarshalerErr) MarshalText() ([]byte, error) { return nil, fmt.Errorf("marshal error") }

var _ encoding.TextMarshaler = textMarshalerErr{}

type textAppenderType struct{ val string }

func (ta textAppenderType) AppendText(b []byte) ([]byte, error) { return append(b, ta.val...), nil }

var _ encoding.TextAppender = textAppenderType{}

type textAppenderErr struct{}

func (textAppenderErr) AppendText([]byte) ([]byte, error) { return nil, fmt.Errorf("append error") }

var _ encoding.TextAppender = textAppenderErr{}

type valStringer struct{ v string }

func (vs valStringer) String() string { return vs.v }

func TestSerStringStringer(t *testing.T) {
	s, err := Parse(`"string"`)
	if err != nil {
		t.Fatal(err)
	}
	// Use a value-receiver stringer so indirect doesn't lose the method.
	v := valStringer{v: "hello"}
	dst, err := s.AppendEncode(nil, &v)
	if err != nil {
		t.Fatalf("encode stringer: %v", err)
	}
	if len(dst) == 0 {
		t.Fatal("expected non-empty output")
	}
}

func TestSerStringTextMarshaler(t *testing.T) {
	s, err := Parse(`"string"`)
	if err != nil {
		t.Fatal(err)
	}
	v := textMarshalerType{val: "hello"}
	dst, err := s.AppendEncode(nil, &v)
	if err != nil {
		t.Fatalf("encode TextMarshaler: %v", err)
	}
	if len(dst) == 0 {
		t.Fatal("expected non-empty output")
	}
}

func TestSerStringTextMarshalerError(t *testing.T) {
	s, err := Parse(`"string"`)
	if err != nil {
		t.Fatal(err)
	}
	v := textMarshalerErr{}
	_, err = s.AppendEncode(nil, &v)
	if err == nil {
		t.Fatal("expected error from MarshalText")
	}
}

func TestSerStringTextAppender(t *testing.T) {
	s, err := Parse(`"string"`)
	if err != nil {
		t.Fatal(err)
	}
	for _, val := range []string{
		"hello",
		"",
		strings.Repeat("a", 200), // multi-byte varlong length
	} {
		v := textAppenderType{val: val}
		dst, err := s.AppendEncode(nil, &v)
		if err != nil {
			t.Fatalf("encode TextAppender %q: %v", val, err)
		}
		var got string
		if _, err := s.Decode(dst, &got); err != nil {
			t.Fatalf("decode %q: %v", val, err)
		}
		if got != val {
			t.Fatalf("got %q, want %q", got, val)
		}
	}
}

func TestSerStringTextAppenderError(t *testing.T) {
	s, err := Parse(`"string"`)
	if err != nil {
		t.Fatal(err)
	}
	v := textAppenderErr{}
	_, err = s.AppendEncode(nil, &v)
	if err == nil {
		t.Fatal("expected error from AppendText")
	}
}

func TestSerFixedNonAddressable(t *testing.T) {
	s, err := Parse(`{"type":"fixed","name":"f","size":4}`)
	if err != nil {
		t.Fatal(err)
	}
	v := [4]byte{1, 2, 3, 4}
	dst, err := s.AppendEncode(nil, &v)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(dst) != 4 || dst[0] != 1 || dst[3] != 4 {
		t.Errorf("got %v", dst)
	}
}

func TestSerBytesNonAddressable(t *testing.T) {
	s, err := Parse(`"bytes"`)
	if err != nil {
		t.Fatal(err)
	}
	v := [3]byte{0xAA, 0xBB, 0xCC}
	dst, err := s.AppendEncode(nil, &v)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(dst) == 0 {
		t.Fatal("expected non-empty output")
	}
}

func TestSerRecordFieldError(t *testing.T) {
	schema := `{"type":"record","name":"r","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}
	]}`
	type R struct {
		A int32 `avro:"a"`
		B int32 `avro:"b"`
	}
	encodeErr(t, schema, &R{A: 1, B: 2})
}

func TestSerIndirectNilPointer(t *testing.T) {
	v := reflect.ValueOf((*int)(nil))
	_, err := indirect(v)
	if err == nil {
		t.Fatal("expected error for nil pointer")
	}
}

func TestSerIndirectNilInterface(t *testing.T) {
	var iface stringer
	v := reflect.ValueOf(&iface).Elem()
	_, err := indirect(v)
	if err == nil {
		t.Fatal("expected error for nil interface")
	}
}

func TestSerNilPointerPrimitives(t *testing.T) {
	// Exercise indirect nil error in each primitive serializer.
	tests := []struct {
		name   string
		schema string
		val    any
	}{
		{"boolean", `"boolean"`, new((*bool)(nil))},
		{"int", `"int"`, new((*int32)(nil))},
		{"long", `"long"`, new((*int64)(nil))},
		{"float", `"float"`, new((*float32)(nil))},
		{"double", `"double"`, new((*float64)(nil))},
		{"bytes", `"bytes"`, new((*[]byte)(nil))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encodeErr(t, tt.schema, tt.val)
		})
	}
}

func TestSerNilPointerComplex(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		val    any
	}{
		{"array", `{"type":"array","items":"int"}`, new((*[]int32)(nil))},
		{"map", `{"type":"map","values":"int"}`, new((*map[string]int32)(nil))},
		{"enum", `{"type":"enum","name":"e","symbols":["a"]}`, new((*string)(nil))},
		{"fixed", `{"type":"fixed","name":"f","size":4}`, new((*[4]byte)(nil))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encodeErr(t, tt.schema, tt.val)
		})
	}
}

func TestSerRecordIndirectError(t *testing.T) {
	schema := `{"type":"record","name":"r","fields":[{"name":"a","type":"int"}]}`
	type R struct {
		A int32 `avro:"a"`
	}
	encodeErr(t, schema, new((*R)(nil)))
}

func TestSerRecordMapFieldError(t *testing.T) {
	// Record-as-map where the value is wrong type, triggering fn error.
	schema := `{"type":"record","name":"r","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}
	]}`
	m := map[string]any{"a": int32(1), "b": 42} // b should be string
	encodeErr(t, schema, &m)
}

func TestSerRecordMissingFieldInStruct(t *testing.T) {
	schema := `{"type":"record","name":"r","fields":[
		{"name":"a","type":"int"},
		{"name":"missing","type":"string"}
	]}`
	type R struct {
		A int32 `avro:"a"`
	}
	encodeErr(t, schema, &R{A: 1})
}

func TestSerArrayItemError(t *testing.T) {
	schema := `{"type":"array","items":"string"}`
	// Items are int, not string.
	v := []int{1, 2, 3}
	encodeErr(t, schema, &v)
}

func TestSerMapValueError(t *testing.T) {
	schema := `{"type":"map","values":"string"}`
	// Values are int, not string.
	v := map[string]int{"a": 1}
	encodeErr(t, schema, &v)
}

func TestSerFixedNonAddressableValue(t *testing.T) {
	// Pass array by value (not pointer) to exercise non-addressable path.
	s, err := Parse(`{"type":"fixed","name":"f","size":4}`)
	if err != nil {
		t.Fatal(err)
	}
	// Pass directly as interface{}, not as &v. The value inside the
	// interface is not addressable.
	var v any = [4]byte{1, 2, 3, 4}
	dst, err := s.AppendEncode(nil, v)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(dst) != 4 || dst[0] != 1 || dst[3] != 4 {
		t.Errorf("got %v", dst)
	}
}

func TestSerBytesNonAddressableValue(t *testing.T) {
	// Pass byte array by value to exercise non-addressable doSerBytes path.
	s, err := Parse(`"bytes"`)
	if err != nil {
		t.Fatal(err)
	}
	var v any = [3]byte{0xAA, 0xBB, 0xCC}
	dst, err := s.AppendEncode(nil, v)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(dst) == 0 {
		t.Fatal("expected non-empty output")
	}
}

type IfaceF struct {
	F int `avro:"f"`
}

func (*IfaceF) String() string { return "f" }

func TestInterface(t *testing.T) {
	type Iface struct {
		S stringer `avro:"s"`
	}

	s, err := Parse(`
{
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
}
`)
	if err != nil {
		t.Fatalf("unable to prime serializer: %v", err)
	}

	u := Iface{
		S: &IfaceF{
			3,
		},
	}

	dst, err := s.AppendEncode(nil, &u)
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	if len(dst) == 0 {
		t.Fatal("expected non-empty output")
	}
}

func TestSerIntOverflow(t *testing.T) {
	schema := `"int"`
	// int64 that overflows int32.
	var big int64 = 1 << 33
	encodeErr(t, schema, &big)

	// Negative overflow.
	var neg int64 = -(1 << 33)
	encodeErr(t, schema, &neg)

	// uint64 that overflows int32.
	var ubig uint64 = 1 << 33
	encodeErr(t, schema, &ubig)

	// Values within range should succeed.
	var ok int64 = 42
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendEncode(nil, &ok); err != nil {
		t.Fatalf("expected success for in-range int, got %v", err)
	}
}

func TestSerFixedFromSlice(t *testing.T) {
	schema := `{"type":"fixed","name":"f","size":4}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	// []byte of correct length should work now.
	input := []byte{1, 2, 3, 4}
	dst, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatalf("expected success for []byte fixed, got %v", err)
	}
	if len(dst) != 4 || dst[0] != 1 || dst[3] != 4 {
		t.Fatalf("unexpected encoding: %v", dst)
	}

	// Wrong size should still error.
	bad := []byte{1, 2, 3}
	if _, err := s.AppendEncode(nil, &bad); err == nil {
		t.Fatal("expected error for wrong-size slice")
	}
}

func TestSerTimestampNanosOverflow(t *testing.T) {
	schema := `{"type":"long","logicalType":"timestamp-nanos"}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	// Dates far in the past/future that would overflow time.UnixNano().
	farPast := time.Date(1800, 1, 1, 0, 0, 0, 0, time.UTC)
	dst, err := s.AppendEncode(nil, &farPast)
	if err != nil {
		t.Fatalf("expected success for far past time, got %v", err)
	}
	if len(dst) == 0 {
		t.Fatal("expected non-empty encoding")
	}

	farFuture := time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC)
	dst, err = s.AppendEncode(nil, &farFuture)
	if err != nil {
		t.Fatalf("expected success for far future time, got %v", err)
	}
	if len(dst) == 0 {
		t.Fatal("expected non-empty encoding")
	}
}

func TestSerMapMissingFieldDefault(t *testing.T) {
	schema := `{"type":"record","name":"r","fields":[
		{"name":"a","type":"int","default":42},
		{"name":"b","type":"string"}
	]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	// Defaults are read-time only. Missing writer fields should always error,
	// even if a default is specified.
	m := map[string]any{"b": "hello"}
	if _, err := s.AppendEncode(nil, &m); err == nil {
		t.Fatal("expected error for missing field during encoding")
	}

	// Map missing field with no default should also error.
	m2 := map[string]any{"a": int32(1)}
	if _, err := s.AppendEncode(nil, &m2); err == nil {
		t.Fatal("expected error for missing field with no default")
	}
}

func TestSerFloat64CoercionInt(t *testing.T) {
	s, err := Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	// Whole float64 should encode as int.
	v := float64(42)
	dst, err := s.AppendEncode(nil, &v)
	if err != nil {
		t.Fatalf("encode float64(42) as int: %v", err)
	}

	var got int32
	if _, err := s.Decode(dst, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}

	// Non-whole float64 should error.
	bad := float64(42.5)
	if _, err := s.AppendEncode(nil, &bad); err == nil {
		t.Fatal("expected error for non-whole float64")
	}

	// Overflow should error.
	big := float64(1 << 33)
	if _, err := s.AppendEncode(nil, &big); err == nil {
		t.Fatal("expected error for float64 overflow of int32")
	}

	// Negative overflow should error.
	negbig := float64(-(1 << 33))
	if _, err := s.AppendEncode(nil, &negbig); err == nil {
		t.Fatal("expected error for negative float64 overflow of int32")
	}

	// Boundary values should work.
	maxv := float64(math.MaxInt32)
	dst, err = s.AppendEncode(nil, &maxv)
	if err != nil {
		t.Fatalf("encode MaxInt32 as float64: %v", err)
	}
	if _, err := s.Decode(dst, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != math.MaxInt32 {
		t.Fatalf("expected %d, got %d", int32(math.MaxInt32), got)
	}

	minv := float64(math.MinInt32)
	dst, err = s.AppendEncode(nil, &minv)
	if err != nil {
		t.Fatalf("encode MinInt32 as float64: %v", err)
	}
	if _, err := s.Decode(dst, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != math.MinInt32 {
		t.Fatalf("expected %d, got %d", int32(math.MinInt32), got)
	}
}

func TestSerFloat64CoercionLong(t *testing.T) {
	s, err := Parse(`"long"`)
	if err != nil {
		t.Fatal(err)
	}

	// Whole float64 should encode as long.
	v := float64(123456789)
	dst, err := s.AppendEncode(nil, &v)
	if err != nil {
		t.Fatalf("encode float64 as long: %v", err)
	}

	var got int64
	if _, err := s.Decode(dst, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != 123456789 {
		t.Fatalf("expected 123456789, got %d", got)
	}

	// Non-whole float64 should error.
	bad := float64(1.5)
	if _, err := s.AppendEncode(nil, &bad); err == nil {
		t.Fatal("expected error for non-whole float64")
	}

	// NaN should error.
	nan := math.NaN()
	if _, err := s.AppendEncode(nil, &nan); err == nil {
		t.Fatal("expected error for NaN")
	}

	// Inf should error.
	inf := math.Inf(1)
	if _, err := s.AppendEncode(nil, &inf); err == nil {
		t.Fatal("expected error for Inf")
	}
}

func TestSerJSONRoundtrip(t *testing.T) {
	// This tests the rpk use case: json.Unmarshal → Encode → Decode → json.Marshal.
	schema := `{
		"type": "record",
		"name": "test",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "age", "type": "int"},
			{"name": "score", "type": "long"},
			{"name": "rating", "type": "float"},
			{"name": "precise", "type": "double"},
			{"name": "active", "type": "boolean"},
			{"name": "tags", "type": {"type": "array", "items": "string"}},
			{"name": "metadata", "type": {"type": "map", "values": "int"}}
		]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}

	input := `{"name":"alice","age":30,"score":100000,"rating":4.5,"precise":3.14159,"active":true,"tags":["go","avro"],"metadata":{"x":1,"y":2}}`

	// Step 1: json.Unmarshal (produces float64 for all numbers).
	var native any
	if err := json.Unmarshal([]byte(input), &native); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}

	// Step 2: Encode to Avro binary.
	binary, err := s.Encode(native)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// Step 3: Decode back to Go types.
	var decoded any
	rest, err := s.Decode(binary, &decoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("unexpected remaining bytes: %v", rest)
	}

	// Step 4: Verify specific field values and types.
	m := decoded.(map[string]any)
	if m["name"] != "alice" {
		t.Errorf("name: got %v", m["name"])
	}
	if m["age"] != int32(30) {
		t.Errorf("age: got %v (%T)", m["age"], m["age"])
	}
	if m["score"] != int64(100000) {
		t.Errorf("score: got %v (%T)", m["score"], m["score"])
	}
	if m["active"] != true {
		t.Errorf("active: got %v", m["active"])
	}
}

func TestSerLongUint64Overflow(t *testing.T) {
	// Top-level long: uint64 > MaxInt64.
	var big uint64 = math.MaxInt64 + 1
	encodeErr(t, `"long"`, &big)

	// Array of longs: uint64 > MaxInt64 in element.
	schema := `{"type":"array","items":"long"}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	arr := []uint64{big}
	if _, err := s.AppendEncode(nil, &arr); err == nil {
		t.Fatal("expected overflow error for uint64 in array long")
	}

	// Map of longs: uint64 > MaxInt64 in value.
	schema = `{"type":"map","values":"long"}`
	s, err = Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]uint64{"k": big}
	if _, err := s.AppendEncode(nil, &m); err == nil {
		t.Fatal("expected overflow error for uint64 in map long")
	}
}
