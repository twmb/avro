package avro

import (
	"bytes"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
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

// TestEncodeCyclicInput covers the depth-bound fix. A cyclic
// map[string]any (m["next"] = m) against a recursive schema would
// otherwise stack-overflow the goroutine — fatal in Go (not
// recoverable via recover). The encoder now bails with a clean error.
// Both the binary and JSON encoders are covered.
func TestEncodeCyclicInput(t *testing.T) {
	s, err := Parse(`{"type":"record","name":"Node","fields":[
		{"name":"value","type":"int"},
		{"name":"next","type":["null","Node"]}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	node := map[string]any{"value": int32(1)}
	node["next"] = node
	t.Run("binary", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panicked: %v", r)
			}
		}()
		_, err := s.AppendEncode(nil, node)
		if err == nil {
			t.Fatal("expected error on cyclic input, got nil")
		}
		if !errors.Is(err, errTooDeep) {
			t.Fatalf("expected errTooDeep, got %v", err)
		}
	})
	t.Run("json", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panicked: %v", r)
			}
		}()
		_, err := s.AppendEncodeJSON(nil, node)
		if err == nil {
			t.Fatal("expected error on cyclic input, got nil")
		}
		if !errors.Is(err, errTooDeep) {
			t.Fatalf("expected errTooDeep, got %v", err)
		}
	})
	// Struct fast path: map[string]any goes through serRecord.ser (slow
	// path which checks depth), but a struct with a *Self field is encoded
	// via the unsafe fast-field chain (usNullUnionRecord -> serRecordFastPtr
	// -> field fn -> usNullUnionRecord -> ...). That chain bypasses
	// serRecord.ser entirely, so the depth check must live on
	// serRecordFastPtr itself.
	type cyclicStructNode struct {
		Value int32             `avro:"value"`
		Next  *cyclicStructNode `avro:"next"`
	}
	t.Run("binary_struct", func(t *testing.T) {
		n := &cyclicStructNode{Value: 1}
		n.Next = n
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panicked: %v", r)
			}
		}()
		_, err := s.AppendEncode(nil, n)
		if err == nil {
			t.Fatal("expected error on cyclic struct, got nil")
		}
		if !errors.Is(err, errTooDeep) {
			t.Fatalf("expected errTooDeep, got %v", err)
		}
	})
	t.Run("json_struct", func(t *testing.T) {
		n := &cyclicStructNode{Value: 1}
		n.Next = n
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panicked: %v", r)
			}
		}()
		_, err := s.AppendEncodeJSON(nil, n)
		if err == nil {
			t.Fatal("expected error on cyclic struct json, got nil")
		}
		if !errors.Is(err, errTooDeep) {
			t.Fatalf("expected errTooDeep, got %v", err)
		}
	})
}

// TestDecodeDeepInputDoesntPanic ensures the decoder bails with errTooDeep
// rather than stack-overflowing on deeply nested encoded data. Mirrors
// TestEncodeCyclicInput on the decode side. Uses the binary fast-field
// chain (udNullUnionRecord -> deserRecordFastPtr -> field fn -> ...).
func TestDecodeDeepInputDoesntPanic(t *testing.T) {
	s, err := Parse(`{"type":"record","name":"Node","fields":[
		{"name":"value","type":"int"},
		{"name":"next","type":["null","Node"]}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	type node struct {
		Value int32 `avro:"value"`
		Next  *node `avro:"next"`
	}
	// Build deeply-nested binary: 5000 levels of "value=0, next=valueByte".
	var src []byte
	for range 5000 {
		src = append(src, 0)    // value (zigzag 0)
		src = append(src, 0x02) // ["null","Node"] union: idx 1 = "Node"
	}
	src = append(src, 0) // terminate inner-most: union idx 0 = null
	t.Run("binary", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panicked: %v", r)
			}
		}()
		var n node
		_, err := s.Decode(src, &n)
		if err == nil {
			t.Fatal("expected error on deep nesting, got nil")
		}
		if !errors.Is(err, errTooDeep) {
			t.Fatalf("expected errTooDeep, got %v", err)
		}
	})
	// Resolved-decode path: same recursive schema for writer and reader,
	// goes through resolvedRecord.buildDeser which has its own depth bump.
	t.Run("resolved", func(t *testing.T) {
		r, err := Parse(`{"type":"record","name":"Node","fields":[
			{"name":"value","type":"int"},
			{"name":"next","type":["null","Node"]}
		]}`)
		if err != nil {
			t.Fatal(err)
		}
		resolved, err := Resolve(s, r)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panicked: %v", r)
			}
		}()
		var n node
		_, err = resolved.Decode(src, &n)
		if err == nil {
			t.Fatal("expected error on deep nesting, got nil")
		}
		if !errors.Is(err, errTooDeep) {
			t.Fatalf("expected errTooDeep, got %v", err)
		}
	})
	// Skip path: reader drops the recursive "next" field, so the
	// resolved decoder must skip the writer's deeply nested subtree
	// via skipRecord/skipUnion.
	t.Run("skip", func(t *testing.T) {
		r, err := Parse(`{"type":"record","name":"Node","fields":[
			{"name":"value","type":"int"}
		]}`)
		if err != nil {
			t.Fatal(err)
		}
		resolved, err := Resolve(s, r)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panicked: %v", r)
			}
		}()
		type readerR struct {
			Value int32 `avro:"value"`
		}
		var rr readerR
		_, err = resolved.Decode(src, &rr)
		if err == nil {
			t.Fatal("expected error on deep skip, got nil")
		}
		if !errors.Is(err, errTooDeep) {
			t.Fatalf("expected errTooDeep, got %v", err)
		}
	})
	// JSON union trial loop: decodeUnionObject and decodeUnionBare iterate
	// each branch on failure. The per-branch error must propagate
	// errTooDeep rather than being swallowed and reported as
	// "no union branch matched". Builds deeply-nested JSON for a
	// recursive ["null","Node"] schema; at maxDepth the decode must
	// surface errTooDeep, not the trial-loop's generic error.
	t.Run("json_union_trial_propagates", func(t *testing.T) {
		var src []byte
		for range 5000 {
			src = append(src, []byte(`{"value":0,"next":{"Node":`)...)
		}
		src = append(src, []byte(`{"value":0,"next":null}`)...)
		for range 5000 {
			src = append(src, []byte(`}}`)...)
		}
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panicked: %v", r)
			}
		}()
		// Decode into *any so decodeUnionObject's toAny=true branch
		// runs (the path the bug was in).
		var out any
		err := s.DecodeJSON(src, &out)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errors.Is(err, errTooDeep) {
			t.Fatalf("expected errTooDeep, got %v", err)
		}
	})
}

// TestParseDeeplyNestedSchema exercises the schema-parse depth bound. A
// schema nested past maxDepth must error rather than recurse the parser
// to stack overflow.
func TestParseDeeplyNestedSchema(t *testing.T) {
	var b strings.Builder
	const n = maxDepth + 50
	for range n {
		b.WriteString(`{"type":"array","items":`)
	}
	b.WriteString(`"int"`)
	for range n {
		b.WriteString(`}`)
	}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked: %v", r)
		}
	}()
	_, err := Parse(b.String())
	if err == nil {
		t.Fatal("expected parse error on deeply nested schema, got nil")
	}
	if !strings.Contains(err.Error(), "deeper than the supported limit") {
		t.Fatalf("expected depth-limit error, got %v", err)
	}
}

// TestIndirectCyclicInterfaceDoesntLoop ensures every pointer/interface
// unwrap loop in the library (indirect, indirectAlloc, isNilValue,
// appendAvroJSON's deref, customEncode's deref) caps at
// maxIndirectDepth so `var p any; p = &p` (a real cycle through the
// empty interface) terminates instead of spinning forever.
func TestIndirectCyclicInterfaceDoesntLoop(t *testing.T) {
	s, err := Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	var p any
	p = &p
	t.Run("binary", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panicked: %v", r)
			}
		}()
		if _, err := s.AppendEncode(nil, p); err == nil {
			t.Fatal("expected error, got nil")
		}
	})
	t.Run("json", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panicked: %v", r)
			}
		}()
		if _, err := s.AppendEncodeJSON(nil, p); err == nil {
			t.Fatal("expected error, got nil")
		}
	})
	// Nullable union: encoder consults isNilValue first, which has its
	// own unwrap loop. Without the cap this hangs.
	t.Run("nullable_union", func(t *testing.T) {
		s2, err := Parse(`["null","int"]`)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panicked: %v", r)
			}
		}()
		if _, err := s2.AppendEncode(nil, p); err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

// TestSerArrayNilAnyElement covers the specialized serArray fast paths
// (string/boolean/int/long/float/double item types). A nil interface element
// in []any unwraps to an invalid reflect.Value; calling .Type() / .Bool() /
// .Int() on it panics. Each path must return an error, not crash.
func TestSerArrayNilAnyElement(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		v      any
	}{
		{"string", `{"type":"array","items":"string"}`, []any{"a", nil}},
		{"boolean", `{"type":"array","items":"boolean"}`, []any{true, nil}},
		{"int", `{"type":"array","items":"int"}`, []any{int32(1), nil}},
		{"long", `{"type":"array","items":"long"}`, []any{int64(1), nil}},
		{"float", `{"type":"array","items":"float"}`, []any{float32(1), nil}},
		{"double", `{"type":"array","items":"double"}`, []any{float64(1), nil}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic: %v", r)
				}
			}()
			encodeErr(t, tc.schema, tc.v)
		})
	}
}

// TestSerMapNilAnyValue covers the specialized serMap fast paths (same set
// of value types). Nil interface values in map[string]any have the same
// panic shape as TestSerArrayNilAnyElement.
func TestSerMapNilAnyValue(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		v      any
	}{
		{"string", `{"type":"map","values":"string"}`, map[string]any{"k": nil}},
		{"boolean", `{"type":"map","values":"boolean"}`, map[string]any{"k": nil}},
		{"int", `{"type":"map","values":"int"}`, map[string]any{"k": nil}},
		{"long", `{"type":"map","values":"long"}`, map[string]any{"k": nil}},
		{"float", `{"type":"map","values":"float"}`, map[string]any{"k": nil}},
		{"double", `{"type":"map","values":"double"}`, map[string]any{"k": nil}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic: %v", r)
				}
			}()
			encodeErr(t, tc.schema, tc.v)
		})
	}
}

func TestSerUnionAllFail(t *testing.T) {
	encodeErr(t, `["null","int"]`, ptr("hello"))
}

func TestSerNullNonNilableType(t *testing.T) {
	// serNull should not panic when given a non-nilable type (int, string, etc.).
	// It should return errNonNil, not crash.
	v := reflect.ValueOf(42)
	_, err := serNull(nil, v, 0)
	if err != errNonNil {
		t.Fatalf("expected errNonNil, got %v", err)
	}
	v = reflect.ValueOf("hello")
	_, err = serNull(nil, v, 0)
	if err != errNonNil {
		t.Fatalf("expected errNonNil, got %v", err)
	}
}

func TestSerNullGenericUnionNonNilable(t *testing.T) {
	// 3-branch union takes the generic serUnion.ser path, which tries
	// serNull first. Pins that serNull tolerates non-nilable types
	// (e.g. int32) without panicking, falling through to the int branch.
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

// TestRegression_TaggedUnionEncodeIndirection locks in that the binary
// union encoder peels Pointer/Interface chains before recognizing a
// tagged-union map, matching the JSON encoder's entry-peel
// (appendAvroJSON at json_codec.go) and isNilValue's loop (ser.go).
// serUnion.tryUnwrapTagged must peel every Pointer and Interface layer
// — &m and any(&m) wrapping a tagged-form map must encode identically
// to m and any(m). Pins binary↔JSON parity at top-level, inside
// arrays of unions, and inside record fields of union type.
func TestRegression_TaggedUnionEncodeIndirection(t *testing.T) {
	m := map[string]any{"int": int32(42)}
	wantInt32 := int32(42)

	t.Run("2-branch top-level", func(t *testing.T) {
		s := MustParse(`["null","int"]`)
		for _, tc := range []struct {
			name string
			in   any
		}{
			{"map (baseline)", m},
			{"any(map) (baseline)", any(m)},
			{"*map (was rejected)", &m},
			{"any(*map) (was rejected)", any(&m)},
		} {
			t.Run(tc.name, func(t *testing.T) {
				bin, err := s.AppendEncode(nil, tc.in)
				if err != nil {
					t.Fatalf("AppendEncode: %v", err)
				}
				jsonOut, err := s.AppendEncodeJSON(nil, tc.in)
				if err != nil {
					t.Fatalf("AppendEncodeJSON: %v", err)
				}
				// Binary↔binary round-trip: decode should produce the same int.
				var out any
				if _, err := s.Decode(bin, &out); err != nil {
					t.Fatalf("Decode: %v", err)
				}
				if out != wantInt32 {
					t.Fatalf("binary round-trip: got %v (%T), want %v", out, out, wantInt32)
				}
				// JSON parity: same byte output regardless of indirection.
				if string(jsonOut) != "42" {
					t.Fatalf("JSON: got %s, want 42", jsonOut)
				}
			})
		}
	})

	t.Run("3-branch top-level", func(t *testing.T) {
		s := MustParse(`["null","int","string"]`)
		// 3-branch goes through the generic serUnion.ser path; 2-branch
		// goes through serNullUnionAt. Both share tryUnwrapTagged so a
		// single fix closes both, but lock both explicitly.
		bin, err := s.AppendEncode(nil, &m)
		if err != nil {
			t.Fatalf("3-branch AppendEncode(&m): %v", err)
		}
		var out any
		if _, err := s.Decode(bin, &out); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if out != wantInt32 {
			t.Fatalf("got %v, want %v", out, wantInt32)
		}
	})

	t.Run("array of unions", func(t *testing.T) {
		s := MustParse(`{"type":"array","items":["null","int"]}`)
		arr := []any{&m, m, any(&m)}
		bin, err := s.AppendEncode(nil, arr)
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var out any
		if _, err := s.Decode(bin, &out); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		got, ok := out.([]any)
		if !ok || len(got) != 3 {
			t.Fatalf("got %v, want []any{42, 42, 42}", out)
		}
		for i, v := range got {
			if v != wantInt32 {
				t.Errorf("item %d: got %v (%T)", i, v, v)
			}
		}
	})

	t.Run("record field of union type", func(t *testing.T) {
		s := MustParse(`{"type":"record","name":"R","fields":[{"name":"u","type":["null","int"]}]}`)
		rec := map[string]any{"u": &m}
		bin, err := s.AppendEncode(nil, rec)
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var out any
		if _, err := s.Decode(bin, &out); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		got, ok := out.(map[string]any)
		if !ok || got["u"] != wantInt32 {
			t.Fatalf("got %v, want map{u:42}", out)
		}
	})

	t.Run("nil cases still picked up by nil-first dispatch", func(t *testing.T) {
		// The peel-before-tagged-check must NOT route a nil pointer/
		// interface into the tagged-map branch — it must fall through
		// to the nil-first dispatch and pick the null branch. Pins
		// that the peel never hijacks nil into try-each.
		s := MustParse(`["null","int"]`)
		var nilMap *map[string]any
		bin, err := s.AppendEncode(nil, nilMap)
		if err != nil {
			t.Fatalf("AppendEncode(nilMap): %v", err)
		}
		// varint(0) = byte 0x00 → null branch.
		if len(bin) != 1 || bin[0] != 0x00 {
			t.Fatalf("nil *map: got %x, want [00] (null branch)", bin)
		}
		var ifaceNil any
		bin, err = s.AppendEncode(nil, ifaceNil)
		if err != nil {
			t.Fatalf("AppendEncode(any(nil)): %v", err)
		}
		if len(bin) != 1 || bin[0] != 0x00 {
			t.Fatalf("any(nil): got %x, want [00]", bin)
		}
	})

	t.Run("non-tagged map shapes still rejected", func(t *testing.T) {
		// A pointer to a map whose key matches NO branch must still
		// fail (not silently match the wrong branch).
		s := MustParse(`["null","int"]`)
		unknown := map[string]any{"notABranch": int32(42)}
		if _, err := s.AppendEncode(nil, &unknown); err == nil {
			t.Fatalf("expected rejection for unknown branch key, got nil")
		}
		// A pointer to a multi-key map (Len != 1) must still fail.
		multi := map[string]any{"int": int32(1), "x": int32(2)}
		if _, err := s.AppendEncode(nil, &multi); err == nil {
			t.Fatalf("expected rejection for multi-key map, got nil")
		}
	})
}

// TestJsonNumberExponentInInt locks in consistent handling of exponent-
// notation json.Number values across scalar, array, and map int/long
// encoders. All three paths must accept "1.5e3" identically; any
// asymmetry between scalar serInt and serArray.serInt would produce
// the "overflows int64" message on one path while another path
// silently rounds.
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

// TestSerIntCoercionToFloatPrecision verifies the lossy-destination
// policy for int → float encode: values within the target's mantissa
// preserve precision exactly, values beyond silently IEEE-round. Matches
// Java's Number.floatValue()/doubleValue() and fastavro's float()
// coercion — see [appendAvroFloat32] / [appendAvroFloat64].
func TestSerIntCoercionToFloatPrecision(t *testing.T) {
	sf, err := Parse(`"float"`)
	if err != nil {
		t.Fatal(err)
	}
	sd, err := Parse(`"double"`)
	if err != nil {
		t.Fatal(err)
	}

	// float32: exact range is [-2^24, 2^24].
	atFloat32Limit := int64(1 << 24)
	if _, err := sf.AppendEncode(nil, atFloat32Limit); err != nil {
		t.Fatalf("float32 at limit: %v", err)
	}
	if _, err := sf.AppendEncode(nil, -atFloat32Limit); err != nil {
		t.Fatalf("float32 at negative limit: %v", err)
	}
	// One past the boundary silently IEEE-rounds.
	if _, err := sf.AppendEncode(nil, atFloat32Limit+1); err != nil {
		t.Fatalf("expected lossy round for int64 beyond float32 precision, got: %v", err)
	}
	if _, err := sf.AppendEncode(nil, -atFloat32Limit-1); err != nil {
		t.Fatalf("expected lossy round for negative int64 beyond float32 precision, got: %v", err)
	}
	if _, err := sf.AppendEncode(nil, uint64(1<<24+1)); err != nil {
		t.Fatalf("expected lossy round for uint64 beyond float32 precision, got: %v", err)
	}

	// float64: exact range is [-2^53, 2^53].
	atFloat64Limit := int64(1 << 53)
	if _, err := sd.AppendEncode(nil, atFloat64Limit); err != nil {
		t.Fatalf("float64 at limit: %v", err)
	}
	if _, err := sd.AppendEncode(nil, -atFloat64Limit); err != nil {
		t.Fatalf("float64 at negative limit: %v", err)
	}
	if _, err := sd.AppendEncode(nil, atFloat64Limit+1); err != nil {
		t.Fatalf("expected lossy round for int64 beyond float64 precision, got: %v", err)
	}
	if _, err := sd.AppendEncode(nil, uint64(1<<53+1)); err != nil {
		t.Fatalf("expected lossy round for uint64 beyond float64 precision, got: %v", err)
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

// TestRegression_SerArrayFloatSilentInf pins the lossy-destination
// policy: array<float> with []float64{1e40} silently narrows to +Inf
// on the wire (matches Java/fastavro). Parity check across serFloat
// (top-level), serArray.serFloat (specialized array), serMap.serFloat
// (specialized map), and usFloat (unsafe) — every float-encode path
// must apply the same policy uniformly.
func TestRegression_SerArrayFloatSilentInf(t *testing.T) {
	s := MustParse(`{"type":"array","items":"float"}`)
	huge := 1e40
	if !math.IsInf(float64(float32(huge)), 1) {
		t.Fatalf("test assumption failed: float32(1e40) should be +Inf")
	}
	if _, err := s.AppendEncode(nil, []float64{huge}); err != nil {
		t.Fatalf("expected lossy narrow to +Inf, got error: %v", err)
	}
}

// TestRegression_SerMapFloatSilentInf is the map<float> parity test for
// the lossy-destination policy.
func TestRegression_SerMapFloatSilentInf(t *testing.T) {
	s := MustParse(`{"type":"map","values":"float"}`)
	if _, err := s.AppendEncode(nil, map[string]float64{"k": 1e40}); err != nil {
		t.Fatalf("expected lossy narrow to +Inf, got error: %v", err)
	}
}

// TestRegression_SerArrayFloatAcceptsInt verifies that the specialized
// array<float> path accepts integer elements (silently IEEE-rounding
// beyond float32's 24-bit mantissa), matching the single-value serFloat
// path. Pins int → float lossy-destination acceptance for the
// specialized array path.
func TestRegression_SerArrayFloatAcceptsInt(t *testing.T) {
	s := MustParse(`{"type":"array","items":"float"}`)
	if _, err := s.AppendEncode(nil, []int64{1, 2, 3}); err != nil {
		t.Fatalf("expected []int64{1,2,3} to encode as array<float>, got %v", err)
	}
	// Lossy-destination: values exceeding float32's 24-bit mantissa
	// silently IEEE-round (matches Java/fastavro).
	if _, err := s.AppendEncode(nil, []int64{1 << 25}); err != nil {
		t.Fatalf("expected []int64{1<<25} to silently round, got %v", err)
	}
}

// TestRegression_SerArrayDoubleAcceptsInt verifies array<double> accepts
// integer elements (silently IEEE-rounding beyond float64's 53-bit
// mantissa per the lossy-destination policy).
func TestRegression_SerArrayDoubleAcceptsInt(t *testing.T) {
	s := MustParse(`{"type":"array","items":"double"}`)
	if _, err := s.AppendEncode(nil, []int64{1, 2, 3}); err != nil {
		t.Fatalf("expected []int64{1,2,3} to encode as array<double>, got %v", err)
	}
	if _, err := s.AppendEncode(nil, []int64{1 << 54}); err != nil {
		t.Fatalf("expected []int64{1<<54} to silently round, got %v", err)
	}
}

// TestSafeUnsafeFloat32OverflowParity locks in that the safe and unsafe
// encode paths agree on the lossy-destination policy: float64 → float32
// narrowing produces ±Inf on the wire without error, matching Java's
// (float)doubleValue() silent narrowing.
func TestSafeUnsafeFloat32OverflowParity(t *testing.T) {
	s, err := Parse(`{"type":"record","name":"R","fields":[{"name":"v","type":"float"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	const huge = math.MaxFloat64

	// Safe path via map[string]any.
	if _, err := s.AppendEncode(nil, map[string]any{"v": huge}); err != nil {
		t.Fatalf("safe path: expected lossy narrow to +Inf, got error: %v", err)
	}

	// Unsafe fast path via struct.
	type R struct {
		V float64 `avro:"v"`
	}
	if _, err := s.AppendEncode(nil, &R{V: huge}); err != nil {
		t.Fatalf("unsafe path: expected lossy narrow to +Inf, got error: %v", err)
	}
}

// TestSafeUnsafeUint64LongOverflowParity locks in that the unsafe fast path
// rejects uint64 values that exceed math.MaxInt64 when encoding to Avro
// long, matching serLong. Without the parity, the unsafe path would
// silently wrap to a negative int64 while the safe path rejected.
func TestSafeUnsafeUint64LongOverflowParity(t *testing.T) {
	s, err := Parse(`{"type":"record","name":"R","fields":[{"name":"v","type":"long"}]}`)
	if err != nil {
		t.Fatal(err)
	}

	// Safe path: map[string]any with uint64 value.
	if _, err := s.AppendEncode(nil, map[string]any{"v": uint64(math.MaxUint64)}); err == nil {
		t.Fatalf("safe path: expected overflow error for MaxUint64, got nil")
	}

	// Unsafe fast path: struct field uint64.
	type R struct {
		V uint64 `avro:"v"`
	}
	if _, err := s.AppendEncode(nil, &R{V: math.MaxUint64}); err == nil {
		t.Fatalf("unsafe path: expected overflow error for MaxUint64 (parity with safe path), got nil")
	}
}

// textBytesMarshaler is a []byte-kind type that ALSO implements
// TextMarshaler / TextUnmarshaler. The two precedence orders disagree
// on it: a []byte-first order encodes the raw bytes, a TextMarshaler-
// first order encodes the text. Used by the array/map/JSON parity
// regression tests below to lock in the correct precedence.
type textBytesMarshaler []byte

func (b textBytesMarshaler) MarshalText() ([]byte, error) {
	return []byte("TEXT:" + string(b)), nil
}

func (b *textBytesMarshaler) UnmarshalText(text []byte) error {
	*b = textBytesMarshaler("UNTEXT:" + string(text))
	return nil
}

// TestRegression_SerArrayStringTextMarshaler locks in that the
// specialized array<string> ser path resolves a []byte-kind value that
// also implements TextMarshaler via its text representation, not its
// raw bytes — matching scalar serString. The shared appendAvroString
// helper enforces the precedence across all three sites (scalar,
// array, map).
func TestRegression_SerArrayStringTextMarshaler(t *testing.T) {
	s := MustParse(`{"type":"array","items":"string"}`)
	encoded, err := s.AppendEncode(nil, []textBytesMarshaler{textBytesMarshaler("hello")})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out := MustParse(`{"type":"array","items":"string"}`)
	var got []string
	if _, err := out.Decode(encoded, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 1 || got[0] != "TEXT:hello" {
		t.Fatalf("got %v, want [TEXT:hello]; the array path encoded raw bytes instead of MarshalText output", got)
	}
}

// TestRegression_SerMapStringTextMarshaler is the map<string> parity
// test for the same precedence rule.
func TestRegression_SerMapStringTextMarshaler(t *testing.T) {
	s := MustParse(`{"type":"map","values":"string"}`)
	encoded, err := s.AppendEncode(nil, map[string]textBytesMarshaler{"k": textBytesMarshaler("hello")})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out := MustParse(`{"type":"map","values":"string"}`)
	var got map[string]string
	if _, err := out.Decode(encoded, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["k"] != "TEXT:hello" {
		t.Fatalf("got %q, want %q; the map path encoded raw bytes instead of MarshalText output", got["k"], "TEXT:hello")
	}
}

// TestRegression_JSONEncodeStringTextMarshaler locks in that the JSON
// encoder for "string" picks TextMarshaler over the []byte fallback for
// types that implement both. net.IP-style values must JSON-encode as
// their text form, not their raw bytes (interpreted as UTF-8).
func TestRegression_JSONEncodeStringTextMarshaler(t *testing.T) {
	s := MustParse(`"string"`)
	v := textBytesMarshaler("hello")
	got, err := s.EncodeJSON(v)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	const want = `"TEXT:hello"`
	if string(got) != want {
		t.Fatalf("got %s, want %s; JSON encoder used []byte fallback instead of MarshalText", got, want)
	}
}

// TestRegression_JSONDecodeStringTextUnmarshaler locks in that the JSON
// decoder for "string" routes into TextUnmarshaler when the target
// implements it, mirroring deserString. Without this routing, the
// target would receive raw bytes ([]byte-kind targets) or an error.
func TestRegression_JSONDecodeStringTextUnmarshaler(t *testing.T) {
	s := MustParse(`"string"`)
	var v textBytesMarshaler
	if err := s.DecodeJSON([]byte(`"hello"`), &v); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(v) != "UNTEXT:hello" {
		t.Fatalf("got %q, want %q; JSON decoder skipped TextUnmarshaler", string(v), "UNTEXT:hello")
	}
}

// A nil pointer field with ,omitzero whose element type implements
// IsZero() (e.g. *time.Time) must encode as the null branch, not panic.
// valueIsZero must not call the promoted IsZero() on a nil pointer (the
// value-receiver method dereferences nil). Covers the slow, unsafe, and
// JSON encode paths, which all route zero-checks through valueIsZero.
func TestRegression_OmitzeroNilPointerIsZero(t *testing.T) {
	type R struct {
		T *time.Time `avro:"t,omitzero"`
	}
	s := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"t","type":["null",{"type":"long","logicalType":"timestamp-millis"}],"default":null}]}`)
	v := R{T: nil}

	// unsafe (addressable) + slow (non-addressable) + JSON encode paths.
	wireAddr, err := s.AppendEncode(nil, &v)
	if err != nil {
		t.Fatalf("encode &v (unsafe path): %v", err)
	}
	wireVal, err := s.AppendEncode(nil, v)
	if err != nil {
		t.Fatalf("encode v (reflect path): %v", err)
	}
	if !bytes.Equal(wireAddr, wireVal) {
		t.Errorf("unsafe vs reflect wire differ: % x vs % x", wireAddr, wireVal)
	}
	if _, err := s.EncodeJSON(&v); err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	// The null branch is index 0 → single 0x00 byte.
	if len(wireAddr) != 1 || wireAddr[0] != 0x00 {
		t.Errorf("nil omitzero field: got wire % x, want 00 (null branch)", wireAddr)
	}
	// A non-nil pointer to a zero time still takes the null branch (the
	// pre-existing correct behavior).
	var zero time.Time
	w2, err := s.AppendEncode(nil, R{T: &zero})
	if err != nil {
		t.Fatalf("encode non-nil zero time: %v", err)
	}
	if len(w2) != 1 || w2[0] != 0x00 {
		t.Errorf("non-nil zero time omitzero: got % x, want 00", w2)
	}
}

// ozPtrCounter has a POINTER-receiver IsZero that treats the sentinel 7 as
// "zero" — a value that DISAGREES with the structural zero (0). Honoring it is
// therefore observable in both directions: 7 (structurally non-zero) must be
// omitted, and 0 (structurally zero) must NOT be omitted.
type ozPtrCounter int64

func (c *ozPtrCounter) IsZero() bool { return *c == 7 }

// ozValCounter is the value-receiver twin, pinning that the pre-existing
// value-receiver path still works after the pointer-receiver path was added.
type ozValCounter int64

func (c ozValCounter) IsZero() bool { return c == 7 }

// omitzero must honor an IsZero() method regardless of whether its receiver is
// a value or a pointer (doc.go: "fields whose IsZero() method returns true").
// A value-typed field whose type has a POINTER-receiver IsZero is addressable
// when encoding &struct, so (&field).IsZero() is callable; valueIsZero reached
// only the value method set, silently encoding the value instead of the
// default/null. The sentinel (7) disagrees with structural zero (0) so both
// directions are pinned: IsZero()==true omits a structurally-non-zero value,
// IsZero()==false keeps a structurally-zero value. Covers the reflect, unsafe,
// and JSON encode paths (all route through valueIsZero).
func TestRegression_OmitzeroPointerReceiverIsZero(t *testing.T) {
	s := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"f","type":["null","long"],"default":null}]}`)
	null := []byte{0x00}    // union index 0 (null branch) — omitzero acted
	longZero := []byte{2, 0} // union index 1 (long), value 0 — not omitted
	longThree := []byte{2, 6} // union index 1 (long), value 3 (zigzag 6)

	// encodeAll returns the reflect (value) and unsafe (&value, addressable)
	// wire, asserting they agree, plus the JSON. The addressable path is the
	// one where a pointer-receiver IsZero is legitimately reachable.
	encodeAll := func(t *testing.T, v any, pv any) []byte {
		t.Helper()
		wireVal, err := s.AppendEncode(nil, v)
		if err != nil {
			t.Fatalf("reflect encode: %v", err)
		}
		wireAddr, err := s.AppendEncode(nil, pv)
		if err != nil {
			t.Fatalf("unsafe encode: %v", err)
		}
		if !bytes.Equal(wireVal, wireAddr) {
			t.Errorf("reflect vs unsafe wire differ: % x vs % x", wireVal, wireAddr)
		}
		if _, err := s.EncodeJSON(pv); err != nil {
			t.Fatalf("EncodeJSON: %v", err)
		}
		return wireAddr
	}

	t.Run("ptr-receiver IsZero true omits non-structural-zero", func(t *testing.T) {
		type R struct {
			F ozPtrCounter `avro:"f,omitzero"`
		}
		v := R{F: 7} // IsZero()==true, structurally non-zero
		if got := encodeAll(t, v, &v); !bytes.Equal(got, null) {
			t.Errorf("got % x, want % x (omitzero must honor pointer-receiver IsZero)", got, null)
		}
	})
	t.Run("ptr-receiver IsZero false keeps structural-zero", func(t *testing.T) {
		type R struct {
			F ozPtrCounter `avro:"f,omitzero"`
		}
		v := R{F: 0} // IsZero()==false, structurally zero
		if got := encodeAll(t, v, &v); !bytes.Equal(got, longZero) {
			t.Errorf("got % x, want % x (IsZero()==false must override structural zero)", got, longZero)
		}
	})
	t.Run("ptr-receiver IsZero false keeps nonzero", func(t *testing.T) {
		type R struct {
			F ozPtrCounter `avro:"f,omitzero"`
		}
		v := R{F: 3}
		if got := encodeAll(t, v, &v); !bytes.Equal(got, longThree) {
			t.Errorf("got % x, want % x", got, longThree)
		}
	})
	t.Run("value-receiver IsZero still honored both ways", func(t *testing.T) {
		type R struct {
			F ozValCounter `avro:"f,omitzero"`
		}
		v7 := R{F: 7}
		if got := encodeAll(t, v7, &v7); !bytes.Equal(got, null) {
			t.Errorf("value-receiver IsZero()==true: got % x, want % x", got, null)
		}
		v0 := R{F: 0}
		if got := encodeAll(t, v0, &v0); !bytes.Equal(got, longZero) {
			t.Errorf("value-receiver IsZero()==false: got % x, want % x", got, longZero)
		}
	})
}

type EmbeddedInner struct {
	A int32 `avro:"a"`
}

type withNilEmbedPtr struct {
	*EmbeddedInner
	C int32 `avro:"c"`
}

type unexportedInner struct {
	A int32 `avro:"a"`
}

type withUnexportedEmbedPtr struct {
	*unexportedInner
	C int32 `avro:"c"`
}

// A nil anonymous embedded *struct must not panic on encode: its promoted
// fields encode as zero (symmetric with decode allocating the embedded
// pointer). Decode into a struct whose embedded pointer is named via an
// UNEXPORTED type must error cleanly, not panic (Go reflection cannot
// allocate/set through an unexported embedded pointer).
func TestRegression_EmbeddedPointerStructNoPanic(t *testing.T) {
	s := MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"c","type":"int"}]}`)

	t.Run("nil embedded encode zero-fills", func(t *testing.T) {
		v := withNilEmbedPtr{C: 3}
		// unsafe (addressable), reflect (value), and JSON encode paths.
		wAddr, err := s.AppendEncode(nil, &v)
		if err != nil {
			t.Fatalf("encode &v: %v", err)
		}
		wVal, err := s.AppendEncode(nil, v)
		if err != nil {
			t.Fatalf("encode v: %v", err)
		}
		if !bytes.Equal(wAddr, wVal) {
			t.Errorf("unsafe vs reflect: % x vs % x", wAddr, wVal)
		}
		if _, err := s.EncodeJSON(&v); err != nil {
			t.Fatalf("EncodeJSON: %v", err)
		}
		// a=0 (zero-filled), c=3 → zig-zag 0x00, 0x06.
		var got withNilEmbedPtr
		if _, err := s.Decode(wAddr, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.EmbeddedInner == nil || got.A != 0 || got.C != 3 {
			t.Errorf("round-trip: got %+v (A via embed=%v)", got, got.EmbeddedInner)
		}
	})

	t.Run("unexported embedded decode errors cleanly", func(t *testing.T) {
		wire, err := s.AppendEncode(nil, &withNilEmbedPtr{EmbeddedInner: &EmbeddedInner{A: 7}, C: 3})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got withUnexportedEmbedPtr
		_, err = s.Decode(wire, &got) // must error, not panic
		if err == nil {
			t.Fatal("expected clean error decoding into unexported embedded pointer; got nil")
		}
	})

	t.Run("unexported embedded decode works when pre-allocated", func(t *testing.T) {
		// The refusal above is specific to a NIL unexported embedded
		// pointer (reflection cannot allocate it — writing the pointer
		// field itself is what's off-limits). Writing a promoted EXPORTED
		// field through a non-nil unexported embed is permitted, so a
		// caller who allocates the embed before decoding must succeed.
		wire, err := s.AppendEncode(nil, &withNilEmbedPtr{EmbeddedInner: &EmbeddedInner{A: 7}, C: 3})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		got := withUnexportedEmbedPtr{unexportedInner: &unexportedInner{}}
		if _, err := s.Decode(wire, &got); err != nil {
			t.Fatalf("decode into pre-allocated unexported embed: %v", err)
		}
		if got.A != 7 || got.C != 3 {
			t.Errorf("promoted field not filled through pre-allocated embed: got %+v", got)
		}
	})
}

// A multi-pointer field (**T / **Record) mapped to ["null", T] holding
// &(*T)(nil) — a non-nil outer pointer wrapping a nil inner — is
// nil-equivalent per isNilValue, so it must encode as the null branch.
// The unsafe struct fast-path enter peeled only the outer pointer and
// committed to the value branch (then faulted on the nil inner), diverging
// from the reflect path (which emits null) and from JSON. Such fields now
// decline to the reflect path.
func TestRegression_UnsafeMultiPtrNullUnionNil(t *testing.T) {
	type Inner struct {
		X int32 `avro:"x"`
	}
	s := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"p","type":["null","int"]},
		{"name":"r","type":["null",{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}]}]}`)
	type R struct {
		P **int32 `avro:"p"`
		Rr **Inner `avro:"r"`
	}
	nilInt := (*int32)(nil)
	nilRec := (*Inner)(nil)
	v := R{P: &nilInt, Rr: &nilRec} // &(*T)(nil) for both

	wAddr, err := s.AppendEncode(nil, &v) // unsafe (addressable)
	if err != nil {
		t.Fatalf("encode &v (unsafe): %v", err)
	}
	wVal, err := s.AppendEncode(nil, v) // reflect (non-addressable)
	if err != nil {
		t.Fatalf("encode v (reflect): %v", err)
	}
	if !bytes.Equal(wAddr, wVal) {
		t.Errorf("unsafe vs reflect diverge: % x vs % x", wAddr, wVal)
	}
	// Both null → two 0x00 bytes.
	if len(wAddr) != 2 || wAddr[0] != 0x00 || wAddr[1] != 0x00 {
		t.Errorf("got wire % x, want 00 00 (both null branches)", wAddr)
	}
	if _, err := s.EncodeJSON(&v); err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
}

// TestRegression_TextAppenderHeaderGrowth pins appendAvroString's
// AppendText inline-write slow path: it reserves a 1-byte length
// placeholder, lets AppendText write directly into dst, then — when the
// real text length needs MORE varint header bytes than the 1-byte
// placeholder — grows dst by exactly (len(realHdr) - placeholderLen) and
// shifts the text right. Every other AppendText test uses short values
// (<64 bytes) that stay on the 1-byte-header fast path and never enter the
// grow branch, so the grow arithmetic (ser.go: make([]byte, len(hdr)-
// hdrLen)) had no wire-level coverage — an over- or under-grow there
// leaves trailing garbage or truncates, corrupting the wire for the NEXT
// field. This drives text lengths across both varint-width boundaries
// (>=64 → 2-byte header, >=8192 → 3-byte header) and asserts an exact
// round-trip both standalone and as the first field of a record (so a
// length error shows up as a misread of the following field).
func TestRegression_TextAppenderHeaderGrowth(t *testing.T) {
	s := MustParse(`"string"`)
	rec := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"s","type":"string"},{"name":"n","type":"int"}]}`)

	for _, n := range []int{0, 1, 63, 64, 65, 127, 128, 8191, 8192, 8193, 70000} {
		val := strings.Repeat("x", n)

		// Standalone: encode the TextAppender, decode as a plain string,
		// and confirm exact value + zero trailing bytes.
		enc, err := s.AppendEncode(nil, testTextAppender{val: val})
		if err != nil {
			t.Fatalf("n=%d: encode: %v", n, err)
		}
		// The wire must be exactly varint(n) + n text bytes, nothing more.
		plain, _ := s.AppendEncode(nil, val)
		if !bytes.Equal(enc, plain) {
			t.Fatalf("n=%d: AppendText wire differs from plain string wire:\n ta=%x\n  s=%x", n, enc, plain)
		}
		var out string
		rest, err := s.Decode(enc, &out)
		if err != nil || out != val || len(rest) != 0 {
			t.Fatalf("n=%d: decode: err=%v len(out)=%d trailing=%d", n, err, len(out), len(rest))
		}

		// As the first field of a record: a header-length error here
		// would desynchronize the following int field.
		renc, err := rec.AppendEncode(nil, map[string]any{"s": testTextAppender{val: val}, "n": int32(0x2A)})
		if err != nil {
			t.Fatalf("n=%d: record encode: %v", n, err)
		}
		var rout struct {
			S string `avro:"s"`
			N int32  `avro:"n"`
		}
		if _, err := rec.Decode(renc, &rout); err != nil {
			t.Fatalf("n=%d: record decode: %v", n, err)
		}
		if rout.S != val || rout.N != 0x2A {
			t.Fatalf("n=%d: record round-trip corrupted: len(S)=%d N=%#x (want len %d, 0x2a)", n, len(rout.S), rout.N, n)
		}
	}
}
