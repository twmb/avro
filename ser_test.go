package avro

import (
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"

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
		{"boolean from string", `"boolean"`, ptr("true")},
		{"int from bool", `"int"`, ptr(true)},
		{"int from string", `"int"`, ptr("42")},
		{"long from bool", `"long"`, ptr(true)},
		{"long from string", `"long"`, ptr("42")},
		{"float from string", `"float"`, ptr("3.14")},
		{"double from string", `"double"`, ptr("3.14")},
		{"bytes from int slice", `"bytes"`, ptr([]int{1, 2})},
		{"string from int", `"string"`, ptr(42)},

		// complex
		{"array from string", `{"type":"array","items":"int"}`, ptr("hello")},
		{"map from string", `{"type":"map","values":"int"}`, ptr("hello")},
		{"map from int-key map", `{"type":"map","values":"int"}`, ptr(map[int]int32{1: 2})},
		{"fixed from int array", `{"type":"fixed","name":"f","size":4}`, ptr([4]int{1, 2, 3, 4})},
		{"fixed wrong size array", `{"type":"fixed","name":"f","size":4}`, ptr([3]byte{1, 2, 3})},
		{"fixed wrong size slice", `{"type":"fixed","name":"f","size":4}`, ptr([]byte{1, 2, 3})},
		{"record from int", `{"type":"record","name":"r","fields":[{"name":"a","type":"int"}]}`, ptr(42)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encodeErr(t, tt.schema, tt.val)
		})
	}
}

func TestSerNilPointer(t *testing.T) {
	encodeErr(t, `"int"`, ptr((*int32)(nil)))
}

func TestSerNilInterface(t *testing.T) {
	var v fmt.Stringer
	encodeErr(t, `"string"`, &v)
}

func TestSerEnumErrors(t *testing.T) {
	schema := `{"type":"enum","name":"e","symbols":["a","b","c"]}`

	t.Run("unknown symbol", func(t *testing.T) {
		encodeErr(t, schema, ptr("unknown"))
	})

	t.Run("out of range int", func(t *testing.T) {
		encodeErr(t, schema, ptr(int32(-1)))
	})

	t.Run("type mismatch", func(t *testing.T) {
		encodeErr(t, schema, ptr(3.14))
	})

	t.Run("uint encode", func(t *testing.T) {
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		dst, err := s.AppendEncode(nil, ptr(uint(1)))
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
		dst, err := s.AppendEncode(nil, ptr(int(1)))
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

func TestSerRecordMapNullField(t *testing.T) {
	t.Run("nil value for non-nullable field returns error not panic", func(t *testing.T) {
		schema := `{"type":"record","name":"r","fields":[
			{"name":"id","type":"int"},
			{"name":"name","type":"string"}
		]}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		m := map[string]any{"id": int32(1), "name": nil}
		_, err = s.AppendEncode(nil, &m)
		if err == nil {
			t.Fatal("expected error encoding nil into non-nullable string field, got nil")
		}
	})

	t.Run("nil value for nullable union field encodes as null branch", func(t *testing.T) {
		schema := `{"type":"record","name":"r","fields":[
			{"name":"id","type":"int"},
			{"name":"name","type":["null","string"]}
		]}`
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		m := map[string]any{"id": int32(1), "name": nil}
		dst, err := s.AppendEncode(nil, &m)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got map[string]any
		if _, err := s.Decode(dst, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got["name"] != nil {
			t.Fatalf("expected nil, got %v", got["name"])
		}
	})
}

func TestSerUnionAllFail(t *testing.T) {
	encodeErr(t, `["null","int"]`, ptr("hello"))
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
	dst, err := s.AppendEncode(nil, ptr(int32(42)))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(dst) == 0 {
		t.Fatal("expected non-empty output")
	}
}

func TestSerTaggedUnionMap(t *testing.T) {
	// Encode should accept the tagged union format {"typeName": value}
	// that Decode with TaggedUnions produces.
	s, err := Parse(`["null","string","int"]`)
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct {
		name  string
		input any
		want  any
	}{
		{"tagged string", map[string]any{"string": "hello"}, "hello"},
		{"tagged int", map[string]any{"int": int32(42)}, int32(42)},
		{"tagged null", map[string]any{"null": nil}, nil},
		{"bare string", "hello", "hello"},
		{"bare int", int32(42), int32(42)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bin, err := s.Encode(tt.input)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			var out any
			if _, err := s.Decode(bin, &out); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if out != tt.want {
				t.Fatalf("got %v (%T), want %v (%T)", out, out, tt.want, tt.want)
			}
		})
	}
}

func TestSerTaggedUnionNullUnion(t *testing.T) {
	// The common ["null", T] fast path should also handle tagged maps.
	s, err := Parse(`["null","string"]`)
	if err != nil {
		t.Fatal(err)
	}

	tagged := map[string]any{"string": "hello"}
	bin, err := s.Encode(tagged)
	if err != nil {
		t.Fatalf("encode tagged: %v", err)
	}
	var out any
	if _, err := s.Decode(bin, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != "hello" {
		t.Fatalf("got %v, want hello", out)
	}

	tagged = map[string]any{"null": nil}
	bin, err = s.Encode(tagged)
	if err != nil {
		t.Fatalf("encode tagged null: %v", err)
	}
	if _, err := s.Decode(bin, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != nil {
		t.Fatalf("got %v, want nil", out)
	}
}

func TestSerTaggedUnionRoundTrip(t *testing.T) {
	// Decode with TaggedUnions → Encode should round-trip.
	schema := `{"type":"record","name":"R","fields":[
		{"name":"id","type":"long"},
		{"name":"payload","type":["null","string","int"]}
	]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	data := map[string]any{"id": int64(1), "payload": "hello"}
	bin1, err := s.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	// Decode with tagged unions.
	var native any
	if _, err := s.Decode(bin1, &native, TaggedUnions()); err != nil {
		t.Fatal(err)
	}
	// native.payload is now map[string]any{"string": "hello"}.
	// Re-encode should work.
	bin2, err := s.Encode(native)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if string(bin1) != string(bin2) {
		t.Fatalf("round-trip mismatch: %x vs %x", bin1, bin2)
	}
}

func TestSerTaggedUnionNullSecondUnion(t *testing.T) {
	// The ["T", "null"] fast path should also handle tagged maps.
	s, err := Parse(`["string","null"]`)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name  string
		input any
		want  any
	}{
		{"tagged string", map[string]any{"string": "hello"}, "hello"},
		{"tagged null", map[string]any{"null": nil}, nil},
		{"bare string", "hello", "hello"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bin, err := s.Encode(tt.input)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			var out any
			if _, err := s.Decode(bin, &out); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if out != tt.want {
				t.Fatalf("got %v (%T), want %v (%T)", out, out, tt.want, tt.want)
			}
		})
	}
}

func TestSerTaggedUnionNested(t *testing.T) {
	t.Run("array of unions", func(t *testing.T) {
		s, err := Parse(`{"type":"array","items":["null","string","int"]}`)
		if err != nil {
			t.Fatal(err)
		}
		input := []any{"hello", int32(42), nil}
		bin, err := s.Encode(input)
		if err != nil {
			t.Fatal(err)
		}
		// Decode with tagged unions, re-encode.
		var native any
		if _, err := s.Decode(bin, &native, TaggedUnions()); err != nil {
			t.Fatal(err)
		}
		// native is []any with tagged maps.
		arr := native.([]any)
		if _, ok := arr[0].(map[string]any); !ok {
			t.Fatalf("expected tagged map, got %T", arr[0])
		}
		bin2, err := s.Encode(native)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		if string(bin) != string(bin2) {
			t.Fatalf("round-trip mismatch: %x vs %x", bin, bin2)
		}
	})

	t.Run("map of unions", func(t *testing.T) {
		s, err := Parse(`{"type":"map","values":["null","string","int"]}`)
		if err != nil {
			t.Fatal(err)
		}
		input := map[string]any{"a": "hello", "b": int32(42)}
		bin, err := s.Encode(input)
		if err != nil {
			t.Fatal(err)
		}
		var native any
		if _, err := s.Decode(bin, &native, TaggedUnions()); err != nil {
			t.Fatal(err)
		}
		bin2, err := s.Encode(native)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		// Map iteration order is non-deterministic, so compare decoded values.
		var decoded any
		if _, err := s.Decode(bin2, &decoded); err != nil {
			t.Fatalf("decode re-encoded: %v", err)
		}
		m := decoded.(map[string]any)
		if m["a"] != "hello" || m["b"] != int32(42) {
			t.Fatalf("got %v, want {a:hello, b:42}", m)
		}
	})

	t.Run("nested record", func(t *testing.T) {
		s, err := Parse(`{"type":"record","name":"Outer","fields":[
			{"name":"inner","type":{"type":"record","name":"Inner","fields":[
				{"name":"v","type":["null","string"]}
			]}}
		]}`)
		if err != nil {
			t.Fatal(err)
		}
		input := map[string]any{"inner": map[string]any{"v": "hello"}}
		bin, err := s.Encode(input)
		if err != nil {
			t.Fatal(err)
		}
		var native any
		if _, err := s.Decode(bin, &native, TaggedUnions()); err != nil {
			t.Fatal(err)
		}
		bin2, err := s.Encode(native)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		if string(bin) != string(bin2) {
			t.Fatalf("round-trip mismatch: %x vs %x", bin, bin2)
		}
	})

	t.Run("record union with logical names", func(t *testing.T) {
		s, err := Parse(`{"type":"record","name":"R","fields":[
			{"name":"ts","type":["null",{"type":"long","logicalType":"timestamp-millis"}]}
		]}`)
		if err != nil {
			t.Fatal(err)
		}
		input := map[string]any{"ts": time.UnixMilli(1700000000000).UTC()}
		bin, err := s.Encode(input)
		if err != nil {
			t.Fatal(err)
		}
		// TagLogicalTypes produces "long.timestamp-millis" as branch name.
		var native any
		if _, err := s.Decode(bin, &native, TaggedUnions(), TagLogicalTypes()); err != nil {
			t.Fatal(err)
		}
		m := native.(map[string]any)
		tsMap := m["ts"].(map[string]any)
		if _, ok := tsMap["long.timestamp-millis"]; !ok {
			t.Fatalf("expected logical branch name, got %v", tsMap)
		}
		// Re-encode with the logical branch name.
		bin2, err := s.Encode(native)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		if string(bin) != string(bin2) {
			t.Fatalf("round-trip mismatch: %x vs %x", bin, bin2)
		}
	})
}

func TestSerTaggedUnionMapBranchFallback(t *testing.T) {
	// A map with a key that matches a branch name but whose value fails
	// to encode on that branch should fall back to trying the map as a
	// raw value on other branches.
	s, err := Parse(`["null",{"type":"map","values":"string"},"int"]`)
	if err != nil {
		t.Fatal(err)
	}
	// Key "int" matches the int branch, but the value "not-an-int"
	// fails on the int branch. The map should then be tried on the
	// map branch as a one-entry map.
	data := map[string]any{"int": "not-an-int"}
	bin, err := s.Encode(data)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out any
	if _, err := s.Decode(bin, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	m, ok := out.(map[string]any)
	if !ok || m["int"] != "not-an-int" {
		t.Fatalf("got %v, want map with int→not-an-int", out)
	}
}

// TestJsonNumberExponentInInt locks in consistent handling of exponent-
// notation json.Number values across scalar, array, and map int/long
// encoders. Prior to the fix, scalar serInt rejected "1.5e3" with a
// misleading "overflows int64" error while serArray.serInt accepted it.
func TestJsonNumberExponentInInt(t *testing.T) {
	cases := []struct {
		name  string
		sch   string
		value any
	}{
		{"scalar int", `"int"`, json.Number("1.5e3")},
		{"scalar long", `"long"`, json.Number("1.5e3")},
		{"array int", `{"type":"array","items":"int"}`, []any{json.Number("1.5e3")}},
		{"array long", `{"type":"array","items":"long"}`, []any{json.Number("1.5e3")}},
		{"map int", `{"type":"map","values":"int"}`, map[string]any{"k": json.Number("1.5e3")}},
		{"map long", `{"type":"map","values":"long"}`, map[string]any{"k": json.Number("1.5e3")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := Parse(tc.sch)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.AppendEncode(nil, tc.value); err != nil {
				t.Fatalf("encode 1.5e3 (=1500) should succeed, got: %v", err)
			}
		})
	}
}

// TestJsonNumberFractionalRejected locks in that fractional json.Number
// values are rejected consistently across scalar, array, and map encoders.
func TestJsonNumberFractionalRejected(t *testing.T) {
	cases := []struct {
		name  string
		sch   string
		value any
	}{
		{"scalar int", `"int"`, json.Number("1.5")},
		{"scalar long", `"long"`, json.Number("1.5")},
		{"array int", `{"type":"array","items":"int"}`, []any{json.Number("1.5")}},
		{"array long", `{"type":"array","items":"long"}`, []any{json.Number("1.5")}},
		{"map int", `{"type":"map","values":"int"}`, map[string]any{"k": json.Number("1.5")}},
		{"map long", `{"type":"map","values":"long"}`, map[string]any{"k": json.Number("1.5")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := Parse(tc.sch)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.AppendEncode(nil, tc.value); err == nil {
				t.Fatal("expected error for fractional json.Number")
			}
		})
	}
}

type testTextMarshaler struct{ val string }

func (tm testTextMarshaler) MarshalText() ([]byte, error) { return []byte(tm.val), nil }

var _ encoding.TextMarshaler = testTextMarshaler{}

type textMarshalerErr struct{}

func (textMarshalerErr) MarshalText() ([]byte, error) { return nil, fmt.Errorf("marshal error") }

var _ encoding.TextMarshaler = textMarshalerErr{}

type testTextAppender struct{ val string }

func (ta testTextAppender) AppendText(b []byte) ([]byte, error) { return append(b, ta.val...), nil }

var _ encoding.TextAppender = testTextAppender{}

type textAppenderErr struct{}

func (textAppenderErr) AppendText([]byte) ([]byte, error) { return nil, fmt.Errorf("append error") }

var _ encoding.TextAppender = textAppenderErr{}

type valStringer struct{ v string }

func (vs valStringer) String() string { return vs.v }

func TestSerStringRejectsStringer(t *testing.T) {
	s, err := Parse(`"string"`)
	if err != nil {
		t.Fatal(err)
	}
	v := valStringer{v: "hello"}
	_, err = s.AppendEncode(nil, &v)
	if err == nil {
		t.Fatal("expected error: Stringer should not be accepted for string fields")
	}
}

func TestSerStringAcceptsTextMarshaler(t *testing.T) {
	s, err := Parse(`"string"`)
	if err != nil {
		t.Fatal(err)
	}
	v := testTextMarshaler{val: "hello"}
	encoded, err := s.AppendEncode(nil, &v)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got string
	if _, err := s.Decode(encoded, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != "hello" {
		t.Fatalf("got %q, want %q", got, "hello")
	}
}

func TestSerStringRejectsJsonNumber(t *testing.T) {
	s, err := Parse(`"string"`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.AppendEncode(nil, json.Number("42"))
	if err == nil {
		t.Fatal("expected error: json.Number should not be accepted for string fields")
	}
}

func TestSerStringRejectsJsonNumberInArray(t *testing.T) {
	s, err := Parse(`{"type":"array","items":"string"}`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Encode([]any{json.Number("42")})
	if err == nil {
		t.Fatal("expected error: json.Number should not be accepted for string array items")
	}
}

func TestSerStringRejectsJsonNumberInMap(t *testing.T) {
	s, err := Parse(`{"type":"map","values":"string"}`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Encode(map[string]any{"k": json.Number("42")})
	if err == nil {
		t.Fatal("expected error: json.Number should not be accepted for string map values")
	}
}

func TestSerStringAcceptsTextAppender(t *testing.T) {
	s, err := Parse(`"string"`)
	if err != nil {
		t.Fatal(err)
	}
	v := testTextAppender{val: "hello"}
	encoded, err := s.AppendEncode(nil, &v)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got string
	if _, err := s.Decode(encoded, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != "hello" {
		t.Fatalf("got %q, want %q", got, "hello")
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
	var iface fmt.Stringer
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
		{"boolean", `"boolean"`, ptr((*bool)(nil))},
		{"int", `"int"`, ptr((*int32)(nil))},
		{"long", `"long"`, ptr((*int64)(nil))},
		{"float", `"float"`, ptr((*float32)(nil))},
		{"double", `"double"`, ptr((*float64)(nil))},
		{"bytes", `"bytes"`, ptr((*[]byte)(nil))},
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
		{"array", `{"type":"array","items":"int"}`, ptr((*[]int32)(nil))},
		{"map", `{"type":"map","values":"int"}`, ptr((*map[string]int32)(nil))},
		{"enum", `{"type":"enum","name":"e","symbols":["a"]}`, ptr((*string)(nil))},
		{"fixed", `{"type":"fixed","name":"f","size":4}`, ptr((*[4]byte)(nil))},
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
	encodeErr(t, schema, ptr((*R)(nil)))
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
		S fmt.Stringer `avro:"s"`
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

func TestSerNestedCDCPipeline(t *testing.T) {
	schema := `{
		"type":"record","name":"user_event",
		"fields":[
			{"name":"user","type":"string"},
			{"name":"address","type":{
				"type":"record","name":"address",
				"fields":[
					{"name":"city","type":"string"},
					{"name":"zip","type":"int"},
					{"name":"since","type":{"type":"long","logicalType":"timestamp-millis"}}
				]
			}},
			{"name":"tags","type":{"type":"array","items":"string"},"default":[]}
		]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate CDC: nested record with timestamp string, outer field uses default.
	input := `{
		"user":"alice",
		"address":{
			"city":"Seattle",
			"zip":98101,
			"since":"2026-03-19T10:00:00Z"
		}
	}`
	var native any
	if err := json.Unmarshal([]byte(input), &native); err != nil {
		t.Fatal(err)
	}
	binary, err := s.Encode(native)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded any
	if _, err := s.Decode(binary, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	m := decoded.(map[string]any)

	if m["user"] != "alice" {
		t.Errorf("user: got %v", m["user"])
	}

	// Nested record: timestamp string should have been parsed.
	addr := m["address"].(map[string]any)
	if addr["city"] != "Seattle" {
		t.Errorf("city: got %v", addr["city"])
	}
	if addr["zip"] != int32(98101) {
		t.Errorf("zip: got %v (%T)", addr["zip"], addr["zip"])
	}
	want := time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)
	got, ok := addr["since"].(time.Time)
	if !ok {
		t.Errorf("since: expected time.Time, got %T: %v", addr["since"], addr["since"])
	} else if !got.Equal(want) {
		t.Errorf("since: got %v, want %v", got, want)
	}

	// "tags" was missing from input — should use default [].
	tags := m["tags"].([]any)
	if len(tags) != 0 {
		t.Errorf("tags: got %v, want []", tags)
	}
}

func TestSerNullableRecordUnion(t *testing.T) {
	schema := `{
		"type":"record","name":"event",
		"fields":[
			{"name":"id","type":"string"},
			{"name":"metadata","type":["null",{
				"type":"record","name":"meta",
				"fields":[
					{"name":"source","type":"string"},
					{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}
				]
			}]}
		]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}

	// Non-null branch: plain map (not pre-wrapped).
	input := `{"id":"abc","metadata":{"source":"cdc","ts":"2026-03-19T10:00:00Z"}}`
	var native any
	if err := json.Unmarshal([]byte(input), &native); err != nil {
		t.Fatal(err)
	}
	binary, err := s.Encode(native)
	if err != nil {
		t.Fatalf("encode non-null: %v", err)
	}
	var decoded any
	if _, err := s.Decode(binary, &decoded); err != nil {
		t.Fatalf("decode non-null: %v", err)
	}
	m := decoded.(map[string]any)
	meta := m["metadata"].(map[string]any)
	if meta["source"] != "cdc" {
		t.Errorf("source: got %v", meta["source"])
	}
	want := time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)
	got, ok := meta["ts"].(time.Time)
	if !ok {
		t.Errorf("ts: expected time.Time, got %T: %v", meta["ts"], meta["ts"])
	} else if !got.Equal(want) {
		t.Errorf("ts: got %v, want %v", got, want)
	}

	// Null branch.
	inputNull := `{"id":"abc","metadata":null}`
	var nativeNull any
	if err := json.Unmarshal([]byte(inputNull), &nativeNull); err != nil {
		t.Fatal(err)
	}
	binaryNull, err := s.Encode(nativeNull)
	if err != nil {
		t.Fatalf("encode null: %v", err)
	}
	var decodedNull any
	if _, err := s.Decode(binaryNull, &decodedNull); err != nil {
		t.Fatalf("decode null: %v", err)
	}
	mNull := decodedNull.(map[string]any)
	if mNull["metadata"] != nil {
		t.Errorf("metadata: got %v, want nil", mNull["metadata"])
	}
}

func TestSerErrorDottedPath(t *testing.T) {
	schema := `{
		"type":"record","name":"outer",
		"fields":[
			{"name":"id","type":"string"},
			{"name":"address","type":{
				"type":"record","name":"addr",
				"fields":[
					{"name":"city","type":"string"},
					{"name":"zip","type":"int"}
				]
			}}
		]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	// zip is a string, should be int — error should show "address.zip".
	input := map[string]any{
		"id": "abc",
		"address": map[string]any{
			"city": "Seattle",
			"zip":  "not-a-number",
		},
	}
	_, err = s.Encode(input)
	if err == nil {
		t.Fatal("expected error")
	}
	var se *SemanticError
	if !errors.As(err, &se) {
		t.Fatalf("expected SemanticError, got %T: %v", err, err)
	}
	if se.Field != "address.zip" {
		t.Errorf("field path: got %q, want %q", se.Field, "address.zip")
	}
}

func TestSerJSONNumberOverflowInCollections(t *testing.T) {
	// json.Number that overflows int32 in array of int.
	s, _ := Parse(`{"type":"array","items":"int"}`)
	_, err := s.AppendEncode(nil, []any{json.Number("3000000000")})
	if err == nil {
		t.Fatal("expected overflow error for array of int")
	}

	// json.Number that overflows int32 in map of int.
	s2, _ := Parse(`{"type":"map","values":"int"}`)
	_, err = s2.AppendEncode(nil, map[string]any{"k": json.Number("3000000000")})
	if err == nil {
		t.Fatal("expected overflow error for map of int")
	}
}

func TestSerDecimalCoercion(t *testing.T) {
	bytesSchema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	fixedSchema := `{"type":"fixed","name":"dec","size":8,"logicalType":"decimal","precision":10,"scale":2}`

	for _, schema := range []string{bytesSchema, fixedSchema} {
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}

		// Reference: encode *big.Rat directly.
		want := new(big.Rat).SetFrac64(314, 100) // 3.14
		refDst, err := s.AppendEncode(nil, want)
		if err != nil {
			t.Fatalf("encode *big.Rat: %v", err)
		}

		for _, tt := range []struct {
			name  string
			input any
		}{
			{"float64", float64(3.14)},
			{"json.Number", json.Number("3.14")},
			{"string", "3.14"},
		} {
			t.Run(schema[:5]+"/"+tt.name, func(t *testing.T) {
				dst, err := s.AppendEncode(nil, &tt.input)
				if err != nil {
					t.Fatalf("encode: %v", err)
				}
				// Decode both and compare as *big.Rat.
				var got, ref big.Rat
				if _, err := s.Decode(dst, &got); err != nil {
					t.Fatalf("decode: %v", err)
				}
				if _, err := s.Decode(refDst, &ref); err != nil {
					t.Fatalf("decode ref: %v", err)
				}
				// float64(3.14) has precision loss, so compare the
				// decoded values rather than exact byte equality.
				if tt.name == "float64" {
					// Just verify it decodes without error and is close.
					f, _ := got.Float64()
					if f < 3.13 || f > 3.15 {
						t.Errorf("got %v, want ~3.14", f)
					}
				} else {
					if got.Cmp(&ref) != 0 {
						t.Errorf("got %s, want %s", got.RatString(), ref.RatString())
					}
				}
			})
		}
	}

	// Invalid json.Number should error, not panic.
	t.Run("invalid", func(t *testing.T) {
		s, _ := Parse(`"int"`)
		bad := json.Number("not_a_number")
		if _, err := s.AppendEncode(nil, &bad); err == nil {
			t.Fatal("expected error for invalid json.Number")
		}
	})
}

func TestSerDateBadString(t *testing.T) {
	s, _ := Parse(`{"type":"int","logicalType":"date"}`)
	bad := "not-a-date"
	if _, err := s.AppendEncode(nil, &bad); err == nil {
		t.Fatal("expected error for non-date string")
	}
}

func TestSerTimestampNanosOverflow(t *testing.T) {
	schema := `{"type":"long","logicalType":"timestamp-nanos"}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	// int64 nanoseconds since epoch span roughly 1677-09-21 to 2262-04-11.
	// Times within that window encode cleanly; times outside it must
	// return an error rather than silently wrap.
	inRangePast := time.Date(1800, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := s.AppendEncode(nil, &inRangePast); err != nil {
		t.Fatalf("1800 is within the nanosecond range, got %v", err)
	}
	inRangeFuture := time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := s.AppendEncode(nil, &inRangeFuture); err != nil {
		t.Fatalf("2200 is within the nanosecond range, got %v", err)
	}

	// Year 2300 is past 2262-04-11, must error.
	farFuture := time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := s.AppendEncode(nil, &farFuture); err == nil {
		t.Fatal("expected overflow error for year 2300")
	}
	// Year 1600 is before 1677-09-21, must error.
	farPast := time.Date(1600, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := s.AppendEncode(nil, &farPast); err == nil {
		t.Fatal("expected overflow error for year 1600")
	}
}

func TestSerMapMissingFieldUsesDefault(t *testing.T) {
	tests := []struct {
		name      string
		schema    string
		input     map[string]any
		expErr    bool
		expDecode map[string]any // expected values after decode round-trip
	}{
		{
			name: "null default for union field",
			schema: `{"type":"record","name":"r","fields":[
				{"name":"a","type":["null"],"default":null}
			]}`,
			input:     map[string]any{},
			expDecode: map[string]any{"a": nil},
		},
		{
			name: "int default",
			schema: `{"type":"record","name":"r","fields":[
				{"name":"a","type":"int","default":42}
			]}`,
			input:     map[string]any{},
			expDecode: map[string]any{"a": int32(42)},
		},
		{
			name: "string default",
			schema: `{"type":"record","name":"r","fields":[
				{"name":"a","type":"string","default":"hello"}
			]}`,
			input:     map[string]any{},
			expDecode: map[string]any{"a": "hello"},
		},
		{
			name: "mixed fields with some defaults",
			schema: `{"type":"record","name":"r","fields":[
				{"name":"a","type":"int","default":42},
				{"name":"b","type":"string"}
			]}`,
			input:     map[string]any{"b": "world"},
			expDecode: map[string]any{"a": int32(42), "b": "world"},
		},
		{
			name: "forward-reference record field with default",
			schema: `{"type":"record","name":"outer","fields":[
				{"name":"name","type":"string"},
				{"name":"inner","type":"inner","default":{"x":99}},
				{"name":"dummy","type":{"type":"record","name":"inner","fields":[
					{"name":"x","type":"int"}
				]}}
			]}`,
			input:     map[string]any{"name": "hi", "dummy": map[string]any{"x": float64(1)}},
			expDecode: map[string]any{"name": "hi", "inner": map[string]any{"x": int32(99)}, "dummy": map[string]any{"x": int32(1)}},
		},
		{
			name: "boolean default",
			schema: `{"type":"record","name":"r","fields":[
				{"name":"a","type":"boolean","default":false}
			]}`,
			input:     map[string]any{},
			expDecode: map[string]any{"a": false},
		},
		{
			name: "long default",
			schema: `{"type":"record","name":"r","fields":[
				{"name":"a","type":"long","default":100000}
			]}`,
			input:     map[string]any{},
			expDecode: map[string]any{"a": int64(100000)},
		},
		{
			name: "float default",
			schema: `{"type":"record","name":"r","fields":[
				{"name":"a","type":"float","default":1.5}
			]}`,
			input:     map[string]any{},
			expDecode: map[string]any{"a": float32(1.5)},
		},
		{
			name: "double default",
			schema: `{"type":"record","name":"r","fields":[
				{"name":"a","type":"double","default":3.14}
			]}`,
			input:     map[string]any{},
			expDecode: map[string]any{"a": 3.14},
		},
		{
			name: "enum default",
			schema: `{"type":"record","name":"r","fields":[
				{"name":"a","type":{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]},"default":"GREEN"}
			]}`,
			input:     map[string]any{},
			expDecode: map[string]any{"a": "GREEN"},
		},
		{
			name: "empty array default",
			schema: `{"type":"record","name":"r","fields":[
				{"name":"a","type":{"type":"array","items":"string"},"default":[]}
			]}`,
			input:     map[string]any{},
			expDecode: map[string]any{"a": []any{}},
		},
		{
			name: "non-empty array default",
			schema: `{"type":"record","name":"r","fields":[
				{"name":"a","type":{"type":"array","items":"int"},"default":[1,2,3]}
			]}`,
			input:     map[string]any{},
			expDecode: map[string]any{"a": []any{int32(1), int32(2), int32(3)}},
		},
		{
			name: "empty map default",
			schema: `{"type":"record","name":"r","fields":[
				{"name":"a","type":{"type":"map","values":"string"},"default":{}}
			]}`,
			input:     map[string]any{},
			expDecode: map[string]any{"a": map[string]any{}},
		},
		{
			name: "nullable union with null default",
			schema: `{"type":"record","name":"r","fields":[
				{"name":"a","type":["null","string"],"default":null}
			]}`,
			input:     map[string]any{},
			expDecode: map[string]any{"a": nil},
		},
		{
			name: "nested record default",
			schema: `{"type":"record","name":"r","fields":[
				{"name":"inner","type":{"type":"record","name":"inner","fields":[
					{"name":"x","type":"int"},
					{"name":"y","type":"string","default":"hi"}
				]},"default":{"x":7}}
			]}`,
			input:     map[string]any{},
			expDecode: map[string]any{"inner": map[string]any{"x": int32(7), "y": "hi"}},
		},
		{
			name: "bytes default with high code points",
			schema: `{"type":"record","name":"r","fields":[
				{"name":"a","type":"bytes","default":"\u00FF\u0001\u0000"}
			]}`,
			input:     map[string]any{},
			expDecode: map[string]any{"a": []byte{0xFF, 0x01, 0x00}},
		},
		{
			name: "fixed default with unicode escapes",
			schema: `{"type":"record","name":"r","fields":[
				{"name":"a","type":{"type":"fixed","name":"f","size":4},"default":"\u0001\u0002\u0003\u0004"}
			]}`,
			input:     map[string]any{},
			expDecode: map[string]any{"a": []byte{1, 2, 3, 4}},
		},
		{
			name: "missing field without default still errors",
			schema: `{"type":"record","name":"r","fields":[
				{"name":"a","type":"int","default":42},
				{"name":"b","type":"string"}
			]}`,
			input:  map[string]any{"a": int32(1)},
			expErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatal(err)
			}
			dst, err := s.AppendEncode(nil, &tt.input)
			if tt.expErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected encode error: %v", err)
			}
			// Round-trip: decode and verify defaults appear.
			var decoded any
			if _, err := s.Decode(dst, &decoded); err != nil {
				t.Fatalf("decode error: %v", err)
			}
			m, ok := decoded.(map[string]any)
			if !ok {
				t.Fatalf("expected map, got %T", decoded)
			}
			for k, want := range tt.expDecode {
				got := m[k]
				if !reflect.DeepEqual(got, want) {
					t.Errorf("field %q: got %v (%T), want %v (%T)", k, got, got, want, want)
				}
			}
		})
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

func TestSerIntCoercionToFloat(t *testing.T) {
	// float and double fields should accept Go integer types (goavro compat).
	sf, err := Parse(`"float"`)
	if err != nil {
		t.Fatal(err)
	}
	sd, err := Parse(`"double"`)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		val  any
	}{
		{"int", int(42)},
		{"int8", int8(42)},
		{"int16", int16(42)},
		{"int32", int32(42)},
		{"int64", int64(42)},
		{"uint", uint(42)},
		{"uint8", uint8(42)},
		{"uint16", uint16(42)},
		{"uint32", uint32(42)},
		{"uint64", uint64(42)},
	}
	for _, tt := range tests {
		t.Run("float/"+tt.name, func(t *testing.T) {
			dst, err := sf.AppendEncode(nil, tt.val)
			if err != nil {
				t.Fatalf("encode %T(%v) as float: %v", tt.val, tt.val, err)
			}
			var got float32
			if _, err := sf.Decode(dst, &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got != 42 {
				t.Fatalf("expected 42, got %v", got)
			}
		})
		t.Run("double/"+tt.name, func(t *testing.T) {
			dst, err := sd.AppendEncode(nil, tt.val)
			if err != nil {
				t.Fatalf("encode %T(%v) as double: %v", tt.val, tt.val, err)
			}
			var got float64
			if _, err := sd.Decode(dst, &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got != 42 {
				t.Fatalf("expected 42, got %v", got)
			}
		})
	}
}

func TestSerIntCoercionToFloatPrecisionOverflow(t *testing.T) {
	sf, err := Parse(`"float"`)
	if err != nil {
		t.Fatal(err)
	}
	sd, err := Parse(`"double"`)
	if err != nil {
		t.Fatal(err)
	}

	// float32: exact range is [-2^24, 2^24].
	// Values at the boundary should work.
	atFloat32Limit := int64(1 << 24)
	if _, err := sf.AppendEncode(nil, atFloat32Limit); err != nil {
		t.Fatalf("float32 at limit: %v", err)
	}
	if _, err := sf.AppendEncode(nil, -atFloat32Limit); err != nil {
		t.Fatalf("float32 at negative limit: %v", err)
	}
	// One past the boundary should fail.
	if _, err := sf.AppendEncode(nil, atFloat32Limit+1); err == nil {
		t.Fatal("expected error for int64 overflowing float32 precision")
	}
	if _, err := sf.AppendEncode(nil, -atFloat32Limit-1); err == nil {
		t.Fatal("expected error for negative int64 overflowing float32 precision")
	}
	// Large uint should fail.
	if _, err := sf.AppendEncode(nil, uint64(1<<24+1)); err == nil {
		t.Fatal("expected error for uint64 overflowing float32 precision")
	}

	// float64: exact range is [-2^53, 2^53].
	atFloat64Limit := int64(1 << 53)
	if _, err := sd.AppendEncode(nil, atFloat64Limit); err != nil {
		t.Fatalf("float64 at limit: %v", err)
	}
	if _, err := sd.AppendEncode(nil, -atFloat64Limit); err != nil {
		t.Fatalf("float64 at negative limit: %v", err)
	}
	if _, err := sd.AppendEncode(nil, atFloat64Limit+1); err == nil {
		t.Fatal("expected error for int64 overflowing float64 precision")
	}
	if _, err := sd.AppendEncode(nil, uint64(1<<53+1)); err == nil {
		t.Fatal("expected error for uint64 overflowing float64 precision")
	}
}

func TestSerFixedAcceptsString(t *testing.T) {
	s, err := Parse(`{"type":"fixed","name":"F","size":4}`)
	if err != nil {
		t.Fatal(err)
	}

	// String of correct length should work.
	str := "abcd"
	dst, err := s.AppendEncode(nil, str)
	if err != nil {
		t.Fatalf("encode string as fixed: %v", err)
	}
	var got [4]byte
	if _, err := s.Decode(dst, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(got[:]) != "abcd" {
		t.Fatalf("expected %q, got %q", "abcd", got)
	}

	// Wrong length should fail.
	short := "abc"
	if _, err := s.AppendEncode(nil, short); err == nil {
		t.Fatal("expected error for wrong-length string")
	}
	long := "abcde"
	if _, err := s.AppendEncode(nil, long); err == nil {
		t.Fatal("expected error for wrong-length string")
	}
}

func TestSerJSONRoundtrip(t *testing.T) {
	// This tests the rpk use case: json.Unmarshal → Encode → Decode → json.Marshal.
	tests := []struct {
		name      string
		schema    string
		record    string
		expRecord string
		expEncErr bool
	}{
		{
			name: "all primitive types plus array and map",
			schema: `{
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
			}`,
			record:    `{"name":"alice","age":30,"score":100000,"rating":4.5,"precise":3.14159,"active":true,"tags":["go","avro"],"metadata":{"x":1,"y":2}}`,
			expRecord: `{"name":"alice","age":30,"score":100000,"rating":4.5,"precise":3.14159,"active":true,"tags":["go","avro"],"metadata":{"x":1,"y":2}}`,
		},
		{
			name: "simple string field",
			schema: `{
				"type":"record",
				"name":"test",
				"fields":[{"name":"name","type":"string"}]
			}`,
			record:    `{"name":"redpanda"}`,
			expRecord: `{"name":"redpanda"}`,
		},
		{
			name: "nested record with array",
			schema: `{
				"type":"record",
				"name":"test",
				"fields":[
					{"name":"name","type":"string"},
					{"name":"complex","type":{
						"type":"record",
						"name":"nestedSchemaName",
						"fields":[
							{"name":"list","type":{"type":"array","items":"int"}}
						]
					}}
				]
			}`,
			record:    `{"name":"redpanda","complex":{"list":[1,2,3,4]}}`,
			expRecord: `{"name":"redpanda","complex":{"list":[1,2,3,4]}}`,
		},
		{
			name: "empty record with default null",
			schema: `{
				"type":"record",
				"name":"test",
				"fields":[{
					"name":"name",
					"type":["null"],
					"default":null
				}]
			}`,
			record:    "{}",
			expRecord: `{"name":null}`,
		},
		{
			name: "invalid record for valid schema",
			schema: `{
				"type":"record",
				"name":"test",
				"fields":[{"name":"name","type":"string"}]
			}`,
			record:    `{"notValid":123}`,
			expEncErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatal(err)
			}
			var native any
			if err := json.Unmarshal([]byte(tt.record), &native); err != nil {
				t.Fatal(err)
			}
			binary, err := s.Encode(native)
			if tt.expEncErr {
				if err == nil {
					t.Fatal("expected encode error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			var decoded any
			rest, err := s.Decode(binary, &decoded)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if len(rest) != 0 {
				t.Fatalf("unexpected remaining bytes: %v", rest)
			}
			got, err := json.Marshal(decoded)
			if err != nil {
				t.Fatal(err)
			}
			// Compare unmarshaled to avoid map ordering issues.
			var gotU, expU any
			if err := json.Unmarshal(got, &gotU); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(tt.expRecord), &expU); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(gotU, expU) {
				t.Errorf("got %s, expected %s", got, tt.expRecord)
			}
		})
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

func TestDurationString(t *testing.T) {
	tests := []struct {
		d    Duration
		want string
	}{
		{Duration{}, "P0D"},
		{Duration{Days: 30}, "P30D"},
		{Duration{Months: 15, Days: 10}, "P1Y3M10D"},
		{Duration{Milliseconds: 3600000}, "PT1H"},
		{Duration{Milliseconds: 5400500}, "PT1H30M0.500S"},
		{Duration{Milliseconds: 1000}, "PT1S"},
		{Duration{Milliseconds: 61000}, "PT1M1S"},
		{Duration{Months: 1, Days: 2, Milliseconds: 3723500}, "P1M2DT1H2M3.500S"},
		{Duration{Milliseconds: 500}, "PT0.500S"},
	}
	for _, tt := range tests {
		got := tt.d.String()
		if got != tt.want {
			t.Errorf("Duration%+v.String() = %q, want %q", tt.d, got, tt.want)
		}
	}
}

func TestDurationFromBytesShort(t *testing.T) {
	d := DurationFromBytes([]byte{1, 2, 3})
	if d != (Duration{}) {
		t.Errorf("expected zero Duration for short input, got %+v", d)
	}
}

func TestDurationBytesRoundTrip(t *testing.T) {
	d := Duration{Months: 3, Days: 15, Milliseconds: 86400000}
	b := d.Bytes()
	got := DurationFromBytes(b[:])
	if got != d {
		t.Errorf("round-trip: got %+v, want %+v", got, d)
	}
}
