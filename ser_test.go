package avro

import (
	"bytes"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// ---------- ser_test.go ----------

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

	s, err := Parse(superheroUnionSchema)
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
	schema := recIntBSchema

	t.Run("success", func(t *testing.T) {
		s := mustParse(t, schema)
		m := map[string]any{"a": int32(42), "b": "hello"}
		dst := mustAppendEncode(t, s, nil, &m)
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
		s := mustParse(t, schema)
		m := map[string]any{"id": int32(1), "name": nil}
		dst := mustAppendEncode(t, s, nil, &m)
		var got map[string]any
		mustDecode(t, s, dst, &got)
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
	s := mustParse(t, nodeRecursiveSchema)
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
	s := mustParse(t, nodeRecursiveSchema)
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
		r, err := Parse(nodeRecursiveSchema)
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
	s := mustParse(t, `"int"`)
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
		s2 := mustParse(t, `["null","int"]`)
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
	s := mustParse(t, `["null","int","string"]`)
	// int32 is non-nilable; serNull must not panic, and the int branch should match.
	dst := mustAppendEncode(t, s, nil, ptr(int32(42)))
	if len(dst) == 0 {
		t.Fatal("expected non-empty output")
	}
}

func TestSerTaggedUnionMap(t *testing.T) {
	// Encode should accept the tagged union format {"typeName": value}
	// that Decode with TaggedUnions produces.
	s := mustParse(t, `["null","string","int"]`)

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
			bin := mustEncode(t, s, tt.input)
			var out any
			mustDecode(t, s, bin, &out)
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
	mustDecode(t, s, bin, &out)
	if out != "hello" {
		t.Fatalf("got %v, want hello", out)
	}

	tagged = map[string]any{"null": nil}
	bin, err = s.Encode(tagged)
	if err != nil {
		t.Fatalf("encode tagged null: %v", err)
	}
	mustDecode(t, s, bin, &out)
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
	mustDecode(t, s, bin1, &native, TaggedUnions())
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
	s := mustParse(t, `["string","null"]`)
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
			bin := mustEncode(t, s, tt.input)
			var out any
			mustDecode(t, s, bin, &out)
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
		mustDecode(t, s, bin, &native, TaggedUnions())
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
		mustDecode(t, s, bin, &native, TaggedUnions())
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
		mustDecode(t, s, bin, &native, TaggedUnions())
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
		mustDecode(t, s, bin, &native, TaggedUnions(), TagLogicalTypes())
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
	s := mustParse(t, `["null",{"type":"map","values":"string"},"int"]`)
	// Key "int" matches the int branch, but the value "not-an-int"
	// fails on the int branch. The map should then be tried on the
	// map branch as a one-entry map.
	data := map[string]any{"int": "not-an-int"}
	bin := mustEncode(t, s, data)
	var out any
	mustDecode(t, s, bin, &out)
	m, ok := out.(map[string]any)
	if !ok || m["int"] != "not-an-int" {
		t.Fatalf("got %v, want map with int→not-an-int", out)
	}
}

// TestMatrix_TaggedUnionEncodeIndirection locks in that the binary
// union encoder peels Pointer/Interface chains before recognizing a
// tagged-union map, matching the JSON encoder's entry-peel
// (appendAvroJSON at json_codec.go) and isNilValue's loop (ser.go).
// serUnion.tryUnwrapTagged must peel every Pointer and Interface layer
// — &m and any(&m) wrapping a tagged-form map must encode identically
// to m and any(m). Pins binary↔JSON parity at top-level, inside
// arrays of unions, and inside record fields of union type.
func TestMatrix_TaggedUnionEncodeIndirection(t *testing.T) {
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
				bin := mustAppendEncode(t, s, nil, tc.in)
				jsonOut := mustAppendEncodeJSON(t, s, nil, tc.in)
				// Binary↔binary round-trip: decode should produce the same int.
				var out any
				mustDecode(t, s, bin, &out)
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
		mustDecode(t, s, bin, &out)
		if out != wantInt32 {
			t.Fatalf("got %v, want %v", out, wantInt32)
		}
	})

	t.Run("array of unions", func(t *testing.T) {
		s := MustParse(`{"type":"array","items":["null","int"]}`)
		arr := []any{&m, m, any(&m)}
		bin := mustAppendEncode(t, s, nil, arr)
		var out any
		mustDecode(t, s, bin, &out)
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
		bin := mustAppendEncode(t, s, nil, rec)
		var out any
		mustDecode(t, s, bin, &out)
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
			s := mustParse(t, tc.sch)
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
			s := mustParse(t, tc.sch)
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
	s := mustParse(t, `"string"`)
	v := testTextMarshaler{val: "hello"}
	encoded := mustAppendEncode(t, s, nil, &v)
	var got string
	mustDecode(t, s, encoded, &got)
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
	s := mustParse(t, `"string"`)
	v := testTextAppender{val: "hello"}
	encoded := mustAppendEncode(t, s, nil, &v)
	var got string
	mustDecode(t, s, encoded, &got)
	if got != "hello" {
		t.Fatalf("got %q, want %q", got, "hello")
	}
}

func TestSerFixedNonAddressable(t *testing.T) {
	s := mustParse(t, `{"type":"fixed","name":"f","size":4}`)
	v := [4]byte{1, 2, 3, 4}
	dst := mustAppendEncode(t, s, nil, &v)
	if len(dst) != 4 || dst[0] != 1 || dst[3] != 4 {
		t.Errorf("got %v", dst)
	}
}

func TestSerBytesNonAddressable(t *testing.T) {
	s := mustParse(t, `"bytes"`)
	v := [3]byte{0xAA, 0xBB, 0xCC}
	dst := mustAppendEncode(t, s, nil, &v)
	if len(dst) == 0 {
		t.Fatal("expected non-empty output")
	}
}

func TestSerRecordFieldError(t *testing.T) {
	schema := recIntBSchema
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
	schema := recIntBSchema
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
	s := mustParse(t, `{"type":"fixed","name":"f","size":4}`)
	// Pass directly as interface{}, not as &v. The value inside the
	// interface is not addressable.
	var v any = [4]byte{1, 2, 3, 4}
	dst := mustAppendEncode(t, s, nil, v)
	if len(dst) != 4 || dst[0] != 1 || dst[3] != 4 {
		t.Errorf("got %v", dst)
	}
}

func TestSerBytesNonAddressableValue(t *testing.T) {
	// Pass byte array by value to exercise non-addressable doSerBytes path.
	s := mustParse(t, `"bytes"`)
	var v any = [3]byte{0xAA, 0xBB, 0xCC}
	dst := mustAppendEncode(t, s, nil, v)
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

	s, err := Parse(ifaceFoobarSchema)
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
	s := mustParse(t, schema)
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
	s := mustParse(t, schema)

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
	mustUnmarshal(t, []byte(input), &native)
	binary := mustEncode(t, s, native)
	var decoded any
	mustDecode(t, s, binary, &decoded)
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
	mustUnmarshal(t, []byte(input), &native)
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
	mustUnmarshal(t, []byte(inputNull), &nativeNull)
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
				dst := mustAppendEncode(t, s, nil, &tt.input)
				// Decode both and compare as *big.Rat.
				var got, ref big.Rat
				mustDecode(t, s, dst, &got)
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
	s := mustParse(t, schema)
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
			mustDecode(t, s, dst, &decoded)
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
	mustDecode(t, s, dst, &got)
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
	mustDecode(t, s, dst, &got)
	if got != math.MaxInt32 {
		t.Fatalf("expected %d, got %d", int32(math.MaxInt32), got)
	}

	minv := float64(math.MinInt32)
	dst, err = s.AppendEncode(nil, &minv)
	if err != nil {
		t.Fatalf("encode MinInt32 as float64: %v", err)
	}
	mustDecode(t, s, dst, &got)
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
	mustDecode(t, s, dst, &got)
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
	sf := mustParse(t, `"float"`)
	sd := mustParse(t, `"double"`)

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
			mustDecode(t, sf, dst, &got)
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
			mustDecode(t, sd, dst, &got)
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
	sf := mustParse(t, `"float"`)
	sd := mustParse(t, `"double"`)

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
	mustDecode(t, s, dst, &got)
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
			mustUnmarshal(t, []byte(tt.record), &native)
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
			mustUnmarshal(t, got, &gotU)
			mustUnmarshal(t, []byte(tt.expRecord), &expU)
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
	s := mustParse(t, schema)
	arr := []uint64{big}
	if _, err := s.AppendEncode(nil, &arr); err == nil {
		t.Fatal("expected overflow error for uint64 in array long")
	}

	// Map of longs: uint64 > MaxInt64 in value.
	schema = `{"type":"map","values":"long"}`
	s = mustParse(t, schema)
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
	s := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"v","type":"float"}]}`)
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
	s := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"v","type":"long"}]}`)

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
	encoded := mustAppendEncode(t, s, nil, []textBytesMarshaler{textBytesMarshaler("hello")})
	out := MustParse(`{"type":"array","items":"string"}`)
	var got []string
	mustDecode(t, out, encoded, &got)
	if len(got) != 1 || got[0] != "TEXT:hello" {
		t.Fatalf("got %v, want [TEXT:hello]; the array path encoded raw bytes instead of MarshalText output", got)
	}
}

// TestRegression_SerMapStringTextMarshaler is the map<string> parity
// test for the same precedence rule.
func TestRegression_SerMapStringTextMarshaler(t *testing.T) {
	s := MustParse(`{"type":"map","values":"string"}`)
	encoded := mustAppendEncode(t, s, nil, map[string]textBytesMarshaler{"k": textBytesMarshaler("hello")})
	out := MustParse(`{"type":"map","values":"string"}`)
	var got map[string]string
	mustDecode(t, out, encoded, &got)
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
	mustEncodeJSON(t, s, &v)
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
func TestMatrix_OmitzeroPointerReceiverIsZero(t *testing.T) {
	s := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"f","type":["null","long"],"default":null}]}`)
	null := []byte{0x00}      // union index 0 (null branch) — omitzero acted
	longZero := []byte{2, 0}  // union index 1 (long), value 0 — not omitted
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
		mustEncodeJSON(t, s, pv)
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
		mustEncodeJSON(t, s, &v)
		// a=0 (zero-filled), c=3 → zig-zag 0x00, 0x06.
		var got withNilEmbedPtr
		mustDecode(t, s, wAddr, &got)
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
		wire := mustAppendEncode(t, s, nil, &withNilEmbedPtr{EmbeddedInner: &EmbeddedInner{A: 7}, C: 3})
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
	s := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"p","type":["null","int"]},
		{"name":"r","type":["null",{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}]}]}`)
	type R struct {
		P  **int32 `avro:"p"`
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
	mustEncodeJSON(t, s, &v)
}

// TestMatrix_TextAppenderHeaderGrowth pins appendAvroString's
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
func TestMatrix_TextAppenderHeaderGrowth(t *testing.T) {
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

// ---------- array_element_fastpath_test.go ----------

// The array/map primitive encoders fast-path the exact natural Go element
// type (int32 for "int", int64/int for "long", float32/float64, bool,
// string) with a direct read+emit loop, bypassing the per-element
// appendAvro* dispatch. A named element type of the same underlying kind
// routes through the GENERAL appendAvro* path instead. This pins the
// invariant that the two produce byte-identical wire — the fast loop must
// never diverge from the general path, including at boundary values.

type fpInt32 int32
type fpInt64 int64
type fpInt int
type fpFloat32 float32
type fpFloat64 float64
type fpBool bool
type fpString string

func TestMatrix_ArrayElementFastPathMatchesGeneral(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		fast   any // builtin element type → fast loop
		gen    any // named element type → general appendAvro* path
	}{
		{"int", `{"type":"array","items":"int"}`,
			[]int32{0, 1, -1, math.MaxInt32, math.MinInt32},
			[]fpInt32{0, 1, -1, math.MaxInt32, math.MinInt32}},
		{"long_int64", `{"type":"array","items":"long"}`,
			[]int64{0, 1, -1, math.MaxInt64, math.MinInt64},
			[]fpInt64{0, 1, -1, math.MaxInt64, math.MinInt64}},
		{"long_int", `{"type":"array","items":"long"}`,
			// Platform int extremes: identical to MaxInt64/MinInt64 on
			// 64-bit, and the representable extremes (so the file still
			// compiles) on 32-bit platforms.
			[]int{0, 1, -1, math.MaxInt, math.MinInt},
			[]fpInt{0, 1, -1, math.MaxInt, math.MinInt}},
		{"float", `{"type":"array","items":"float"}`,
			[]float32{0, 1, -1, math.MaxFloat32, float32(math.Inf(1)), float32(math.NaN())},
			[]fpFloat32{0, 1, -1, math.MaxFloat32, fpFloat32(math.Inf(1)), fpFloat32(math.NaN())}},
		{"double", `{"type":"array","items":"double"}`,
			[]float64{0, 1, -1, math.MaxFloat64, math.Inf(-1), math.NaN()},
			[]fpFloat64{0, 1, -1, math.MaxFloat64, fpFloat64(math.Inf(-1)), fpFloat64(math.NaN())}},
		{"boolean", `{"type":"array","items":"boolean"}`,
			[]bool{true, false, true},
			[]fpBool{true, false, true}},
		{"string", `{"type":"array","items":"string"}`,
			[]string{"", "a", "héllo", "x\x00y"},
			[]fpString{"", "a", "héllo", "x\x00y"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := MustParse(c.schema)
			fast, err := s.Encode(c.fast)
			if err != nil {
				t.Fatalf("fast encode: %v", err)
			}
			gen, err := s.Encode(c.gen)
			if err != nil {
				t.Fatalf("general encode: %v", err)
			}
			if !bytes.Equal(fast, gen) {
				t.Fatalf("fast-loop wire diverges from general path:\n fast=% x\n gen =% x", fast, gen)
			}
			// Round-trip the fast wire back into the builtin slice type.
			out := reflect.New(reflect.TypeOf(c.fast)).Interface()
			mustDecode(t, s, fast, out)
			re, err := s.Encode(reflect.ValueOf(out).Elem().Interface())
			if err != nil {
				t.Fatalf("re-encode: %v", err)
			}
			if !bytes.Equal(re, fast) {
				t.Fatalf("round-trip wire mismatch:\n in =% x\n out=% x", fast, re)
			}
		})
	}
}

// Decode-side native path: a builtin []V decodes via the concrete-slice loop
// (s[i]=v); a named slice type and a named-element type must fall back to the
// reflect loop (the unnamed-[]V assertion returns handled=false) and still
// decode correctly with src untouched on the fallthrough.
func TestRegression_ArrayDecodeNamedFallback(t *testing.T) {
	type namedSlice []int32
	type namedElem int32
	s := MustParse(`{"type":"array","items":"int"}`)
	want := []int32{0, 1, -1, math.MaxInt32, math.MinInt32}
	wire := mustEncode(t, s, want)
	var builtin []int32 // native loop
	mustDecode(t, s, wire, &builtin)
	if !reflect.DeepEqual(builtin, want) {
		t.Fatalf("native []int32: %v != %v", builtin, want)
	}
	var ns namedSlice // named slice type → fallback
	mustDecode(t, s, wire, &ns)
	if !reflect.DeepEqual([]int32(ns), want) {
		t.Fatalf("named slice fallback: %v", ns)
	}
	var ne []namedElem // named elem type → fallback
	mustDecode(t, s, wire, &ne)
	if !reflect.DeepEqual(ne, []namedElem{0, 1, -1, math.MaxInt32, math.MinInt32}) {
		t.Fatalf("named elem fallback: %v", ne)
	}
}

// Map builtin-string value fast path must match the general (named-value) path.
func TestRegression_MapValueFastPathMatchesGeneral(t *testing.T) {
	s := MustParse(`{"type":"map","values":"string"}`)
	fast, _ := s.Encode(map[string]string{"k": "v"})
	gen, _ := s.Encode(map[string]fpString{"k": "v"})
	if !bytes.Equal(fast, gen) {
		t.Fatalf("map value fast-loop diverges: fast=% x gen=% x", fast, gen)
	}
}

// ---------- array_zerobyte_compat_test.go ----------

// TestRegression_ArrayZeroByteProducerCompliance pins producer-side compliance
// with the decoder's zero-byte-item cap (checkArrayBlockBounds /
// maxZeroByteItems). The decoder rejects an array of more than maxZeroByteItems
// zero-byte items (array<null>, array<EmptyRecord>, array<size-0-fixed>) as a
// deliberate DoS defense (BUG_AUDIT "DOS-resistance defense-in-depth"). The
// core array ENCODER had no matching check, so s.Encode produced a tiny wire
// (a count with no body) that s.Decode then rejected — a silent self-
// incompatible round-trip. This is the same class as the OCF zero-byte writer
// bound (TestWriterZeroByteDatumsSelfReadable): every reader-side cap needs a
// producer-side compliance check. The encoder now rejects at encode time with
// a clear error, and everything at or below the cap still round-trips.
func TestRegression_ArrayZeroByteProducerCompliance(t *testing.T) {
	zeroByteItemSchemas := []struct {
		label  string
		schema string
		item   any // a single zero-byte item value for this schema
	}{
		{"null", `{"type":"array","items":"null"}`, nil},
		{"empty-record", `{"type":"array","items":{"type":"record","name":"E","fields":[]}}`, map[string]any{}},
		{"size-0-fixed", `{"type":"array","items":{"type":"fixed","name":"Z","size":0}}`, []byte{}},
	}

	fill := func(item any, n int) []any {
		a := make([]any, n)
		for i := range a {
			a[i] = item
		}
		return a
	}

	for _, zb := range zeroByteItemSchemas {
		t.Run(zb.label, func(t *testing.T) {
			s := MustParse(zb.schema)

			// At the cap: must encode AND round-trip (self-readable).
			atCap := fill(zb.item, maxZeroByteItems)
			wire, err := s.AppendEncode(nil, atCap)
			if err != nil {
				t.Fatalf("encode at the cap (%d) rejected: %v", maxZeroByteItems, err)
			}
			var back []any
			if _, err := s.Decode(wire, &back); err != nil {
				t.Fatalf("SELF-INCOMPATIBILITY: encoded %d zero-byte items it cannot decode: %v", maxZeroByteItems, err)
			}
			if len(back) != maxZeroByteItems {
				t.Fatalf("round-trip length: got %d want %d", len(back), maxZeroByteItems)
			}

			// One past the cap: the encoder must REJECT (producer compliance),
			// not emit a wire the decoder rejects.
			over := fill(zb.item, maxZeroByteItems+1)
			if _, err := s.AppendEncode(nil, over); err == nil {
				t.Fatalf("encoder produced a %d zero-byte-item array the decoder rejects (self-incompatible); want an encode-time error", maxZeroByteItems+1)
			} else if !strings.Contains(err.Error(), "zero-byte") {
				t.Fatalf("over-cap encode rejected, but not with the zero-byte-cap reason: %v", err)
			}
		})
	}
}

// TestRegression_ArrayZeroByteSkipPathCompliance covers the resolution skip
// path: a writer record with an array<null> field the reader drops. Because
// the encoder now refuses to PRODUCE an over-cap zero-byte array, no such wire
// reaches the skip path from a twmb writer — the self-incompatibility is
// resolved at its source. The under-cap case still resolves+skips cleanly.
func TestRegression_ArrayZeroByteSkipPathCompliance(t *testing.T) {
	wSchema := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"drop","type":{"type":"array","items":"null"}},
		{"name":"keep","type":"int"}]}`)
	rSchema := MustParse(`{"type":"record","name":"R","fields":[{"name":"keep","type":"int"}]}`)
	res, err := Resolve(wSchema, rSchema)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	// Under cap: encode, resolve-skip the dropped array, keep survives.
	under := map[string]any{"drop": make([]any, maxZeroByteItems), "keep": int32(7)}
	wire, err := wSchema.AppendEncode(nil, under)
	if err != nil {
		t.Fatalf("under-cap encode: %v", err)
	}
	var got map[string]any
	if _, err := res.Decode(wire, &got); err != nil {
		t.Fatalf("SELF-INCOMPATIBILITY (skip path): cannot skip a dropped array<null> field it produced: %v", err)
	}
	if got["keep"] != int32(7) {
		t.Fatalf("keep field after skip: %v", got["keep"])
	}

	// Over cap: the encoder refuses to produce the unreadable wire.
	over := map[string]any{"drop": make([]any, maxZeroByteItems+1), "keep": int32(7)}
	if _, err := wSchema.AppendEncode(nil, over); err == nil {
		t.Fatal("encoder produced a record whose dropped array<null> field exceeds the decoder cap; want an encode-time error")
	}
}

// ---------- array_zerobyte_unsafe_test.go ----------

// TestRegression_ArrayZeroByteUnsafePathCompliance covers the UNSAFE array
// encoders (usArrayRecord / usArrayPtrRecord / usArrayDirect), reached when an
// array of zero-byte items is an addressable struct field. The first fix added
// the producer-side maxZeroByteItems check only to the reflect serArray.ser;
// its unsafe twins write the count + body with no guard, so a struct field
// []EmptyRecord / []*EmptyRecord / [][0]byte of more than maxZeroByteItems
// elements still encoded to a tiny wire the decoder rejects. (Sibling sweep:
// the reflect and unsafe array encoders must share one compliance helper so
// they cannot drift — that is what this pins.)
func TestRegression_ArrayZeroByteUnsafePathCompliance(t *testing.T) {
	type emptyRec struct{}

	cases := []struct {
		label  string
		schema string
		atCap  any
		over   any
	}{
		{
			"slice-of-empty-record",
			`{"type":"record","name":"H","fields":[{"name":"a","type":{"type":"array","items":{"type":"record","name":"E","fields":[]}}}]}`,
			&struct {
				A []emptyRec `avro:"a"`
			}{A: make([]emptyRec, maxZeroByteItems)},
			&struct {
				A []emptyRec `avro:"a"`
			}{A: make([]emptyRec, maxZeroByteItems+1)},
		},
		{
			"slice-of-ptr-empty-record",
			`{"type":"record","name":"H","fields":[{"name":"a","type":{"type":"array","items":{"type":"record","name":"E","fields":[]}}}]}`,
			func() any {
				a := make([]*emptyRec, maxZeroByteItems)
				for i := range a {
					a[i] = &emptyRec{}
				}
				return &struct {
					A []*emptyRec `avro:"a"`
				}{A: a}
			}(),
			func() any {
				a := make([]*emptyRec, maxZeroByteItems+1)
				for i := range a {
					a[i] = &emptyRec{}
				}
				return &struct {
					A []*emptyRec `avro:"a"`
				}{A: a}
			}(),
		},
		{
			"slice-of-size0-fixed",
			`{"type":"record","name":"H","fields":[{"name":"a","type":{"type":"array","items":{"type":"fixed","name":"Z","size":0}}}]}`,
			&struct {
				A [][0]byte `avro:"a"`
			}{A: make([][0]byte, maxZeroByteItems)},
			&struct {
				A [][0]byte `avro:"a"`
			}{A: make([][0]byte, maxZeroByteItems+1)},
		},
	}

	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			s := MustParse(c.schema)

			// At the cap: encodes and round-trips (self-readable).
			wire, err := s.AppendEncode(nil, c.atCap)
			if err != nil {
				t.Fatalf("encode at cap: %v", err)
			}
			var sink any
			if _, err := s.Decode(wire, &sink); err != nil {
				t.Fatalf("SELF-INCOMPATIBILITY (unsafe path): encoded at the cap but cannot decode: %v", err)
			}

			// Over the cap: the unsafe encoder must REJECT, not emit a wire
			// the decoder refuses.
			if _, err := s.AppendEncode(nil, c.over); err == nil {
				t.Fatal("unsafe array encoder produced an over-cap zero-byte array the decoder rejects; want an encode-time error")
			} else if !strings.Contains(err.Error(), "zero-byte") {
				t.Fatalf("over-cap unsafe encode rejected, but not with the zero-byte reason: %v", err)
			}
		})
	}
}

// ---------- empty_bytes_identity_test.go ----------

// Decoding empty Avro bytes into `any` must produce a NON-nil []byte.
// A nil []byte is nil-equivalent on re-encode (the documented nil-first
// union dispatch sends Go nil to the null branch), so a nil result flips
// {"bytes": ""} to null through any decode→re-encode pipeline:
//
//	["null","bytes"] wire 02 00 (bytes branch, length 0)
//	  → decode → []byte(nil) → re-encode → 00 (null branch)   ← corruption
//
// Java decodes empty bytes to an empty (non-null) ByteBuffer and fastavro
// to b”, both re-encoding onto the bytes branch; twmb's JSON decoder,
// deserFixed, and the unsafe udBytesDeser all already produce non-nil
// empties via make+copy. setBytesValue's interface arm was the one
// sibling manufacturing nil (append onto a nil base).
func TestRegression_EmptyBytesDecodeNonNil(t *testing.T) {
	s := MustParse(`"bytes"`)
	wire, err := s.AppendEncode(nil, []byte{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var a any
	mustDecode(t, s, wire, &a)
	b, ok := a.([]byte)
	if !ok {
		t.Fatalf("decoded %T", a)
	}
	if b == nil {
		t.Fatal("empty bytes decoded into any as nil []byte; must be non-nil empty")
	}

	// The union round-trip consequence: the bytes branch survives a
	// decode→re-encode cycle.
	u := MustParse(`["null","bytes"]`)
	w1 := []byte{0x02, 0x00} // bytes branch, length 0
	var ua any
	if _, err := u.Decode(w1, &ua); err != nil {
		t.Fatalf("union decode: %v", err)
	}
	w2, err := u.AppendEncode(nil, ua)
	if err != nil {
		t.Fatalf("union re-encode: %v", err)
	}
	if !bytes.Equal(w2, w1) {
		t.Fatalf("union re-encode flipped branch: w1=%x w2=%x (empty bytes became null)", w1, w2)
	}

	// JSON decoder parity: also non-nil.
	var ja any
	mustDecodeJSON(t, s, []byte(`""`), &ja)
	if jb, ok := ja.([]byte); !ok || jb == nil {
		t.Fatalf("JSON decoded %T nil=%v; want non-nil []byte", ja, ja == nil)
	}

	// The string→bytes promotion path shares setBytesValue, so a
	// promoted empty string also surfaces as non-nil bytes.
	ws := MustParse(`"string"`)
	rb := MustParse(`"bytes"`)
	res, err := Resolve(ws, rb)
	if err != nil {
		t.Fatalf("Resolve(string→bytes): %v", err)
	}
	emptyStr, _ := ws.AppendEncode(nil, "")
	var pa any
	if _, err := res.Decode(emptyStr, &pa); err != nil {
		t.Fatalf("promoted decode: %v", err)
	}
	if pb, ok := pa.([]byte); !ok || pb == nil {
		t.Fatalf("promoted empty string decoded as %T nil=%v; want non-nil []byte", pa, pa == nil)
	}

	// Encoding a Go-nil []byte still picks the null branch (the
	// documented nil-first dispatch is about ENCODE inputs, untouched).
	wNil, err := u.AppendEncode(nil, []byte(nil))
	if err != nil {
		t.Fatalf("encode nil []byte: %v", err)
	}
	if !bytes.Equal(wNil, []byte{0x00}) {
		t.Fatalf("nil []byte should encode to null branch: %x", wNil)
	}
}

// ---------- min_bytes_standin_test.go ----------

// A per-element minimum selects which block-count RULE applies, and the rules
// are not ordered: zero takes the zero-byte cap, positive takes the
// buffer-relative bound, and neither is uniformly looser. So the walk may never
// round a minimum UP when it cannot compute one — reporting 1 for a type whose
// true minimum is 0 does not loosen the bound, it moves a legitimately
// zero-byte container onto a rule it cannot satisfy.
//
// The walk has two places it cannot compute: an unwired forward reference
// (nil child) and an exhausted allowance. Both used to report 1.

// standInSCC is a cyclic SCC deep enough that one walk over it exhausts the
// min-bytes allowance. Defined first and fully wired, so a later container over
// "L0" resolves to a built node on the BUILD path.
func standInSCC(levels int) string {
	inner := `["null","L0"]`
	for i := levels - 1; i >= 0; i-- {
		if i == levels-1 {
			inner = fmt.Sprintf(`{"type":"record","name":"L%d","fields":[{"name":"f0","type":["null","L0"]},{"name":"f1","type":["null","L0"]}]}`, i)
			continue
		}
		inner = fmt.Sprintf(`{"type":"record","name":"L%d","fields":[{"name":"f0","type":["null",%s]},{"name":"f1","type":["null","L%d"]}]}`, i, inner, i+1)
	}
	return inner
}

const standInSCCLevels = 26

// TestRegression_ZeroMinimumContainerAfterDrainedAllowance pins the exhaustion
// stand-in. Two schemas differing ONLY in field order: the shared walk is
// drained by the SCC container before the zero-byte container is reached in one
// and after it in the other. Both must accept the array their own encoder
// produced — an uncomputed minimum may change how loose a bound is, never
// whether a valid wire passes.
func TestRegression_ZeroMinimumContainerAfterDrainedAllowance(t *testing.T) {
	mk := func(drainFirst bool) string {
		var b strings.Builder
		b.WriteString(`{"type":"record","name":"Root","fields":[{"name":"def","type":` + standInSCC(standInSCCLevels) + `}`)
		zero := `,{"name":"z","type":{"type":"array","items":"null"}}`
		drain := `,{"name":"a","type":{"type":"array","items":"L0"}}`
		if drainFirst {
			b.WriteString(drain + zero)
		} else {
			b.WriteString(zero + drain)
		}
		b.WriteString(`]}`)
		return b.String()
	}
	for _, drainFirst := range []bool{false, true} {
		name := map[bool]string{false: "zero-min container built first", true: "zero-min container built after the drain"}[drainFirst]
		t.Run(name, func(t *testing.T) {
			s := MustParse(mk(drainFirst))
			val := map[string]any{
				"def": map[string]any{"f0": nil, "f1": nil},
				"a":   []any{},
				"z":   make([]any, maxZeroByteItems),
			}
			wire := mustEncode(t, s, val)
			var out map[string]any
			if _, err := s.Decode(wire, &out); err != nil {
				t.Fatalf("own encoder produced %d bytes its own decoder rejects: %v", len(wire), err)
			}
			if got := len(out["z"].([]any)); got != maxZeroByteItems {
				t.Fatalf("decoded %d zero-byte items, want %d", got, maxZeroByteItems)
			}
		})
	}
}

// TestMatrix_ZeroMinimumContainerBehindForwardRef pins the nil-child
// stand-in. The forward reference must sit BELOW the container's direct child,
// and that is the whole shape — do not "simplify" this to an
// array-of-forward-reference.
//
// A container whose DIRECT child is the forward reference is registered in
// containerFixups, and finalize re-derives its minimum from the resolved node
// once the reference is wired; the build-time answer is overwritten and the nil
// stand-in never reaches the wire path. That case is the "direct" control
// below, and it is correct on both declaration orders even without this fix.
//
// One level down, nothing re-patches: the array's items is an inline record,
// immediately resolvable, so the array is NOT a fixup — while that record's own
// FIELD is the forward reference, so the walk sees a nil child at build and the
// value it computes there is the value the decoder uses forever.
func TestMatrix_ZeroMinimumContainerBehindForwardRef(t *testing.T) {
	const later = `{"type":"record","name":"Later","fields":[]}` // true minimum: 0 wire bytes
	// nested: items is an inline record whose FIELD is the forward reference.
	const nested = `{"type":"array","items":{"type":"record","name":"Inner","fields":[{"name":"g","type":"Later"}]}}`
	// direct: items IS the forward reference — the fixup re-derives this one.
	const direct = `{"type":"array","items":"Later"}`

	for _, c := range []struct {
		name    string
		arr     string
		elem    func() any
		fixedUp bool // finalize re-derives the minimum for this shape
	}{
		{"nested (forward ref below the container's child)", nested,
			func() any { return map[string]any{"g": map[string]any{}} }, false},
		{"direct (forward ref IS the container's child — control)", direct,
			func() any { return map[string]any{} }, true},
	} {
		for _, order := range []struct {
			name    string
			forward bool
		}{{"Later declared after (forward ref)", true}, {"Later declared before (backward ref)", false}} {
			t.Run(c.name+"/"+order.name, func(t *testing.T) {
				z := `{"name":"z","type":` + c.arr + `}`
				d := `{"name":"d","type":` + later + `}`
				fields := z + "," + d
				if !order.forward {
					fields = d + "," + z
				}
				s := MustParse(`{"type":"record","name":"Root","fields":[` + fields + `]}`)
				items := make([]any, maxZeroByteItems)
				for i := range items {
					items[i] = c.elem()
				}
				wire := mustEncode(t, s, map[string]any{"z": items, "d": map[string]any{}})
				var out map[string]any
				if _, err := s.Decode(wire, &out); err != nil {
					t.Fatalf("own encoder produced %d bytes its own decoder rejects: %v", len(wire), err)
				}
				if got := len(out["z"].([]any)); got != maxZeroByteItems {
					t.Fatalf("decoded %d zero-byte items, want %d", got, maxZeroByteItems)
				}
			})
		}
	}
}

// ---- the class matrix ----------------------------------------------------

// standInCase is one cell: a schema whose container's per-element minimum is
// reached through a named stand-in source, with a known true minimum.
type standInCase struct {
	name     string
	src      string
	value    func(n int) map[string]any // the record value carrying n elements
	zeroMin  bool                       // the element's TRUE minimum is 0
	countKey string                     // field holding the container under test
}

// standInCases crosses STAND-IN SOURCE x CONTAINER x ELEMENT-TRUE-MINIMUM.
// The stand-in source is the axis the bug turned on; the element's true minimum
// is the axis that decides which rule is correct, and holding it at "positive"
// is why an over-reporting stand-in looked harmless.
func standInCases() []standInCase {
	scc := standInSCC(standInSCCLevels)
	drainPrefix := `{"name":"def","type":` + scc + `},{"name":"a","type":{"type":"array","items":"L0"}},`
	drainVal := func(m map[string]any) map[string]any {
		m["def"] = map[string]any{"f0": nil, "f1": nil}
		m["a"] = []any{}
		return m
	}
	emptyRec := `{"type":"record","name":"E","fields":[]}`

	arrVal := func(elem func() any) func(int) map[string]any {
		return func(n int) map[string]any {
			items := make([]any, n)
			for i := range items {
				items[i] = elem()
			}
			return map[string]any{"z": items}
		}
	}
	mapVal := func(elem func() any) func(int) map[string]any {
		return func(n int) map[string]any {
			m := make(map[string]any, n)
			for i := range n {
				m[fmt.Sprintf("k%d", i)] = elem()
			}
			return map[string]any{"z": m}
		}
	}
	nilElem := func() any { return nil }
	recElem := func() any { return map[string]any{} }
	intElem := func() any { return int32(1) }

	rec := func(fields string) string {
		return `{"type":"record","name":"Root","fields":[` + fields + `]}`
	}

	var cs []standInCase
	for _, container := range []struct {
		kind string
		wrap func(elem string) string
		val  func(func() any) func(int) map[string]any
	}{
		{"array", func(e string) string { return `{"type":"array","items":` + e + `}` }, arrVal},
		{"map", func(e string) string { return `{"type":"map","values":` + e + `}` }, mapVal},
	} {
		for _, elem := range []struct {
			kind string
			src  string
			mk   func() any
			zero bool
		}{
			{"zero-min/null", `"null"`, nilElem, true},
			{"zero-min/empty-record", emptyRec, recElem, true},
			{"positive-min/int", `"int"`, intElem, false},
		} {
			z := `{"name":"z","type":` + container.wrap(elem.src) + `}`
			// none: the control — nothing prevents the walk from computing.
			cs = append(cs, standInCase{
				name: "none/" + container.kind + "/" + elem.kind, src: rec(z),
				value: container.val(elem.mk), zeroMin: elem.zero, countKey: "z",
			})
			// drained: the container is built after a walk-exhausting sibling.
			cs = append(cs, standInCase{
				name: "drained/" + container.kind + "/" + elem.kind, src: rec(drainPrefix + z),
				value:   func(n int) map[string]any { return drainVal(container.val(elem.mk)(n)) },
				zeroMin: elem.zero, countKey: "z",
			})
		}
		// nil-child: the element's own subtree holds an unwired forward
		// reference at build. Crossed against BOTH true minima, because the
		// stand-in is only wrong for one of them.
		for _, elem := range []struct {
			kind   string
			fields string
			mk     func() any
			zero   bool
		}{
			{"zero-min/fwd-ref-to-empty", `{"name":"g","type":"Later"}`,
				func() any { return map[string]any{"g": map[string]any{}} }, true},
			{"positive-min/fwd-ref-plus-int", `{"name":"p","type":"int"},{"name":"g","type":"Later"}`,
				func() any { return map[string]any{"p": int32(1), "g": map[string]any{}} }, false},
		} {
			inner := `{"type":"record","name":"Inner","fields":[` + elem.fields + `]}`
			z := `{"name":"z","type":` + container.wrap(inner) + `}`
			later := `{"name":"d","type":{"type":"record","name":"Later","fields":[]}}`
			cs = append(cs, standInCase{
				name: "nil-child/" + container.kind + "/" + elem.kind, src: rec(z + "," + later),
				value: func(n int) map[string]any {
					m := container.val(elem.mk)(n)
					m["d"] = map[string]any{}
					return m
				},
				zeroMin: elem.zero, countKey: "z",
			})
		}
	}
	return cs
}

// TestMatrix_MinBytesStandInNeverOverReports is the class net. Its oracle is
// ENCODE-IMPLIES-DECODE — this package's own encoder produces the wire, so its
// own decoder must accept it — which is calibration-free and reads nothing off
// the walk's current behavior.
//
// The DoS half runs on every cell too: a bound that stopped false-rejecting by
// disappearing would pass the accept half alone.
func TestMatrix_MinBytesStandInNeverOverReports(t *testing.T) {
	for _, c := range standInCases() {
		t.Run(c.name, func(t *testing.T) {
			s, err := Parse(c.src)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			// A count that exercises the rule: for a zero-minimum element the
			// documented zero-byte cap is the only thing bounding it, so sit
			// exactly ON the cap.
			n := maxZeroByteItems
			if !c.zeroMin {
				n = 512
			}
			wire, err := s.Encode(c.value(n))
			if err != nil {
				t.Fatalf("encode %d elements: %v", n, err)
			}
			var out map[string]any
			if _, err := s.Decode(wire, &out); err != nil {
				t.Fatalf("ACCEPT half: own encoder produced %d bytes its own decoder rejects: %v", len(wire), err)
			}

			// DoS half: a declared count far past anything the buffer could
			// hold must still be refused, whatever rule the cell landed on.
			hostile := append([]byte(nil), wire...)
			if i := indexOfBlockCount(wire, n); i >= 0 {
				hostile = append(append(append([]byte(nil), wire[:i]...), dosVarlong(1<<40)...), wire[i+len(appendVarlong(nil, int64(n))):]...)
				var sink map[string]any
				if _, err := s.Decode(hostile, &sink); err == nil {
					t.Fatalf("DoS half: a block count of 2^40 in a %d-byte buffer was accepted", len(hostile))
				}
			}
		})
	}
}

// indexOfBlockCount locates the container's block-count varint in the encoded
// record so the DoS half can overwrite it. The count is the first occurrence of
// n's own varint encoding, which is unambiguous here because every cell's
// element count is far larger than any other number in the wire.
func indexOfBlockCount(wire []byte, n int) int {
	want := appendVarlong(nil, int64(n))
	for i := 0; i+len(want) <= len(wire); i++ {
		if string(wire[i:i+len(want)]) == string(want) {
			return i
		}
	}
	return -1
}

// TestRegression_ZeroByteItemCapStillHolds is the boundary-1 half of the pins
// above: the fix must not have bought acceptance by dropping the cap. AT the cap
// accepts, one past it rejects — on a schema with no stand-in and on one where
// the walk was drained, so the unknown RULE is held to the same limit as the
// computed one.
//
// The over-cap wire is hand-built because the encoder enforces the same cap
// (encoding 4097 zero-byte items is refused), which is itself the property that
// makes the accept half's encode-implies-decode oracle meaningful.
func TestRegression_ZeroByteItemCapStillHolds(t *testing.T) {
	plain := `{"type":"record","name":"Root","fields":[{"name":"z","type":{"type":"array","items":"null"}}]}`
	drained := `{"type":"record","name":"Root","fields":[{"name":"def","type":` + standInSCC(standInSCCLevels) +
		`},{"name":"a","type":{"type":"array","items":"L0"}},{"name":"z","type":{"type":"array","items":"null"}}]}`
	for _, c := range []struct {
		name, src string
		prefix    []byte // def (two null union indexes) + a (empty array), when present
	}{
		{"no stand-in", plain, nil},
		{"drained allowance", drained, []byte{0, 0, 0}},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := MustParse(c.src)
			wireFor := func(n int) []byte {
				w := append([]byte(nil), c.prefix...)
				w = appendVarlong(w, int64(n)) // one block of n zero-byte items
				return append(w, 0)            // terminating zero block count
			}
			var out map[string]any
			if _, err := s.Decode(wireFor(maxZeroByteItems), &out); err != nil {
				t.Fatalf("AT the %d-element cap must accept: %v", maxZeroByteItems, err)
			}
			if got := len(out["z"].([]any)); got != maxZeroByteItems {
				t.Fatalf("decoded %d items at the cap, want %d", got, maxZeroByteItems)
			}
			var sink map[string]any
			if _, err := s.Decode(wireFor(maxZeroByteItems+1), &sink); err == nil {
				t.Fatalf("one past the %d-element cap must reject, but decode succeeded", maxZeroByteItems)
			}
		})
	}
}

// ---------- magnitude_arithmetic_test.go ----------

// ---------------------------------------------------------------------------
// The producer half of "integer arithmetic over a schema-declared magnitude",
// and the source-derived registry that keeps the class closed.
//
// The behavioral cells live next door (magnitude_arithmetic_matrix_test.go);
// they can only see arithmetic that reaches an observable outcome. Two things
// they cannot see are here: the invariant the PRODUCER of a derived bound
// owes its callers, and the enumeration of every site in the package where a
// magnitude meets an arithmetic operator at all.
// ---------------------------------------------------------------------------

// TestInvariant_SchemaMinBytesSaturates is what makes a ceiling at the
// producer sufficient. Two properties, both derived from what a per-element
// minimum MEANS rather than from what the code returns:
//
//   - It is a byte count, so it is never negative, and `1 + it` (which three
//     callers compute) is never zero — a zero divides, and a negative one
//     turns a buffer-relative bound into its own opposite.
//   - A value that provably occupies at least one wire byte must report at
//     least one. Reporting zero or less for such an element is not a loose
//     bound but a MISCLASSIFICATION: the zero-byte element cap and the
//     buffer-relative bound are different rules with different limits, and a
//     non-positive minimum silently routes an element through the wrong one.
//     That half is invisible to a decode test, because both rules end in an
//     error for a truncated wire.
func TestInvariant_SchemaMinBytesSaturates(t *testing.T) {
	const huge = `{"type":"fixed","name":"HF","size":9223372036854775807}`
	// Sums that wrap: a small lead, then magnitudes large enough to carry the
	// running total past the top of the range and back around.
	const sumToZero = `{"type":"record","name":"WZ","fields":[
		{"name":"lead","type":"long"},
		{"name":"a","type":{"type":"fixed","name":"WZA","size":9223372036854775807}},
		{"name":"b","type":{"type":"fixed","name":"WZB","size":9223372036854775807}}]}`
	const sumToNeg = `{"type":"record","name":"WN","fields":[
		{"name":"u","type":[{"type":"fixed","name":"WNU","size":9223372036854775807}]},
		{"name":"a","type":{"type":"fixed","name":"WNA","size":9223372036854775807}}]}`

	type probe struct {
		name    string
		schema  string
		nonZero bool // every value of this schema occupies >= 1 wire byte
	}
	probes := []probe{
		{"huge-fixed", huge, true},
		{"sum-wraps-to-zero", sumToZero, true},
		{"sum-wraps-negative", sumToNeg, true},
		// A union reports one byte for its branch index plus its SMALLEST
		// branch, so a union carrying a null branch reports 1 no matter what
		// else is in it. Only a union whose every branch is huge drives the
		// union arm's own arithmetic — the ["null", huge] shape below is the
		// one that looks like it covers this and does not.
		{"union-of-huge-only", `[` + huge + `]`, true},
		{"union-of-two-huge", `[` + huge + `,{"type":"fixed","name":"HF2","size":9223372036854775806}]`, true},
		{"huge-fixed-in-union", `["null",` + huge + `]`, true},
		{"huge-fixed-in-array", `{"type":"array","items":` + huge + `}`, true},
		{"huge-fixed-in-map", `{"type":"map","values":` + huge + `}`, true},
		{"wrap-nested-in-record", `{"type":"record","name":"NW","fields":[{"name":"in","type":` + sumToZero + `}]}`, true},
		{"wrap-behind-array", `{"type":"array","items":` + sumToZero + `}`, true},
		{"wrap-behind-map", `{"type":"map","values":` + sumToZero + `}`, true},
		{"many-huge-fields", `{"type":"record","name":"MH","fields":[
			{"name":"a","type":"long"},
			{"name":"b","type":{"type":"fixed","name":"MHB","size":9223372036854775807}},
			{"name":"c","type":{"type":"fixed","name":"MHC","size":9223372036854775807}},
			{"name":"d","type":{"type":"fixed","name":"MHD","size":9223372036854775807}},
			{"name":"e","type":{"type":"fixed","name":"MHE","size":9223372036854775807}}]}`, true},
		// Controls: genuinely zero-byte shapes must keep reporting zero, or
		// the zero-byte element cap stops applying to the elements it exists
		// for.
		{"plain-int", `"int"`, true},
		{"plain-null", `"null"`, false},
		{"empty-record", `{"type":"record","name":"ER","fields":[]}`, false},
		{"record-of-null", `{"type":"record","name":"RN","fields":[{"name":"n","type":"null"}]}`, false},
	}
	for _, p := range probes {
		t.Run(p.name, func(t *testing.T) {
			s := mustParse(t, p.schema)
			got := schemaMinBytes(s.node)
			if got < 0 {
				t.Errorf("minimum wire bytes is %d; a byte count cannot be negative, and a negative one inverts every bound derived from it", got)
			}
			if 1+got <= 0 {
				t.Errorf("minimum wire bytes is %d, so a caller's `1 + minimum` is %d; three callers compute that and one divides by it", got, 1+got)
			}
			if got > maxSchemaMagnitude {
				t.Errorf("minimum wire bytes is %d, above the stated ceiling %d; callers add to this value and must not have to re-derive the headroom", got, maxSchemaMagnitude)
			}
			if p.nonZero && got < 1 {
				t.Errorf("every value of this schema occupies at least one wire byte, but the minimum reports %d — "+
					"a non-positive minimum routes the block bound through the zero-byte element cap instead of the buffer-relative one", got)
			}
			if !p.nonZero && got != 0 {
				t.Errorf("this schema's values occupy no wire bytes, but the minimum reports %d — "+
					"the zero-byte element cap exists for exactly these and stops applying if they report more", got)
			}
		})
	}
}

// TestInvariant_UnionMinimumCoversItsBranches catches the one wrong answer a
// range check cannot: a union arm that scans for the smallest branch starting
// from a SENTINEL reports the no-branches answer whenever a branch's own
// minimum happens to equal that sentinel — and the no-branches answer is 1,
// which is small, plausible, and inside every bound anyone would assert.
//
// The property is calibration-free: a union writes a branch index and then a
// branch, so it cannot cost less than its cheapest branch costs alone.
func TestInvariant_UnionMinimumCoversItsBranches(t *testing.T) {
	const huge = `{"type":"fixed","name":"UHF","size":9223372036854775807}`
	for _, schema := range []string{
		`[` + huge + `]`,
		`[` + huge + `,{"type":"fixed","name":"UHF2","size":9223372036854775806}]`,
		`["null",` + huge + `]`,
		`["int","string"]`,
		`["null","int"]`,
		`[{"type":"fixed","name":"UF3","size":4},{"type":"fixed","name":"UF4","size":9}]`,
	} {
		s, err := Parse(schema)
		if err != nil {
			t.Fatalf("parse %s: %v", schema, err)
		}
		got := schemaMinBytes(s.node)
		cheapest := -1
		for _, b := range s.node.branches {
			m := schemaMinBytes(b)
			if cheapest < 0 || m < cheapest {
				cheapest = m
			}
		}
		if got < cheapest {
			t.Errorf("union %s reports a minimum of %d, below its cheapest branch's own %d — "+
				"a union writes a branch index AND a branch, so it cannot cost less than the branch does alone",
				schema, got, cheapest)
		}
	}
}

// TestInvariant_SaturateSchemaMagnitudeIsTotal pins the accessor's own
// contract, which every consumer leans on: the result is in range for ANY
// input, including the ones no parse can produce today. A consumer that has
// to ask whether its input was already validated is a consumer that will
// eventually guess wrong.
func TestInvariant_SaturateSchemaMagnitudeIsTotal(t *testing.T) {
	const maxInt = int(^uint(0) >> 1)
	const minInt = -maxInt - 1
	for _, n := range []int{
		minInt, minInt + 1, -maxSchemaMagnitude, -1, 0, 1,
		maxSchemaMagnitude - 1, maxSchemaMagnitude, maxSchemaMagnitude + 1,
		1 << 40, maxInt - 1, maxInt,
	} {
		got := saturateSchemaMagnitude(n)
		if got < 0 || got > maxSchemaMagnitude {
			t.Errorf("saturateSchemaMagnitude(%d) = %d, outside [0, %d]", n, got, maxSchemaMagnitude)
		}
		if n >= 0 && n <= maxSchemaMagnitude && got != n {
			t.Errorf("saturateSchemaMagnitude(%d) = %d; a value already in range must pass through unchanged", n, got)
		}
	}
}

// TestInvariant_MagnitudeCeilingSurvivesItsLargestMultiplier is the reason the
// ceiling has the value it does. The package's consumers do not just add
// magnitudes, they scale them — the widest is bits-per-byte in the decimal
// capacity calculation — and the ceiling has to leave that product inside a
// 32-bit int so the arithmetic is safe on every build, not just the ones
// where int happens to be 64 bits.
func TestInvariant_MagnitudeCeilingSurvivesItsLargestMultiplier(t *testing.T) {
	const int32Max = 1<<31 - 1
	if got := int64(maxSchemaMagnitude) * magnitudeWidestMultiplier; got > int32Max {
		t.Errorf("ceiling %d times the widest multiplier %d is %d, past a 32-bit int's %d — "+
			"the ceiling no longer covers its own consumers", maxSchemaMagnitude, magnitudeWidestMultiplier, got, int32Max)
	}
	// And it must still be generous enough that no real schema is clipped:
	// the largest fixed anyone writes is orders of magnitude below it.
	if maxSchemaMagnitude < 1<<24 {
		t.Errorf("ceiling %d is below 16 MiB; a legitimate large fixed would be clipped, loosening every bound derived from it", maxSchemaMagnitude)
	}
}

// ---------------------------------------------------------------------------
// The enumeration: every site in the package where a schema-declared magnitude
// meets an arithmetic operator.
//
// WHAT DISTINGUISHES A HAZARDOUS SITE FROM A SAFE ONE. Three conditions, all
// required:
//
//  1. An operand carries a magnitude the schema TEXT chose. The only primitive
//     one is a `fixed` size (the parser leaves its upper bound open); the
//     others — precision, scale — are parse-capped, and counts are bounded by
//     the input length. But magnitude PROPAGATES: through arithmetic, through
//     a function that returns it, and through a function it is passed into.
//     A per-record sum over field minimums holds no `.size` anywhere in its
//     expression and is the wrap this file exists for, which is why a set
//     built by grepping `.size` is the wrong set.
//  2. The operator can leave the integer range: + - * / % <<, or a make()
//     length. Comparisons cannot — `a < b` has the same answer at every
//     magnitude — and neither can formatting or assignment. This is why
//     grepping `.size` OVER-reports: most of its reads are comparisons.
//  3. The value is an INTEGER. A magnitude handed on as []byte, string or
//     *big.Int has left the integer domain, and nothing downstream of it can
//     wrap on the magnitude's account.
//
// So the rule is reachability, not a pattern, and it is derived below rather
// than listed: seeds are the magnitude-bearing fields, and taint flows to
// integer-typed returns and integer-typed parameters until it stops growing.
// The derivation deliberately OVER-approximates — it has no types, so a
// reflect.Type's Size() reads as a magnitude — and every over-report is a row
// saying so. An enumeration with a reason per entry is auditable; the count
// alone is what keeps costing rounds.
// ---------------------------------------------------------------------------

type magVerdict string

const (
	// The magnitude reaching this expression is saturated, so the operator
	// cannot leave the range.
	magSaturated magVerdict = "saturated"
	// Bounded at this site for a reason of its own, stated in the row.
	magBoundedHere magVerdict = "bounded-here"
	// An over-report: the operand is not a schema-declared magnitude.
	magNotAMagnitude magVerdict = "not-a-magnitude"
	// Wrapping IS the operation's definition.
	magWrapIsTheContract magVerdict = "wrap-is-the-contract"
)

type magnitudeSite struct {
	where   string // "file.go::funcName"
	count   int    // hazardous expressions the derivation finds there
	verdict magVerdict
	reason  string
}

// magnitudeSites classifies every site the derivation reports. A site that
// appears with no row FAILS: that is a new consumer of a magnitude, and the
// point of the table is that someone has to say what happens to it at the top
// of the range. A row naming no site fails too, so the table cannot go stale
// while reading as coverage.
var magnitudeSites = []magnitudeSite{
	{
		where: "deser.go::minBytesWalk.minBytesFromChildren", count: 2, verdict: magSaturated,
		reason: "the producer. `1 + m` over the smallest branch and the running sum over a record's fields; " +
			"both are clamped by saturateSchemaMagnitude, and the sum is clamped per FIELD so the next addition " +
			"starts in range. This is the wrap that reached a divisor. The recursion's own `depth+1` is not " +
			"reported here and must not be: depth counts path length, which the fixpoint does not taint",
	},
	{
		where: "deser.go::mapEntryMinBytes", count: 1, verdict: magSaturated,
		reason: "`1 + valueMin` — the SINGLE constructor of the per-entry minimum for all four map sites. " +
			"valueMin is either a saturated minimum or minBytesUnknown, and the unknown is peeled off before " +
			"the addition, so the operand is in [0, maxSchemaMagnitude] and the sum cannot wrap. Its whole " +
			"job is that checkMapBlockBounds' divisor is >= 1: routing every site through here is what keeps " +
			"an unknown from arriving as 1 + (-1) = 0",
	},
	{
		where: "deser.go::checkMapBlockBounds", count: 1, verdict: magSaturated,
		reason: "divides by minEntryBytes, which is `1 + a saturated minimum` at all four call sites, so it is >= 1. " +
			"This is the division the wrap turned into a panic",
	},
	{
		where: "deser.go::checkArrayBlockBounds", count: 1, verdict: magSaturated,
		reason: "divides by minItemBytes only inside `if minItemBytes > 0`; the saturated producer also makes the " +
			"non-positive branch mean what it says, since only a genuinely zero-byte element can reach it now",
	},
	{
		where: "schema.go::builder.buildComplex", count: 1, verdict: magNotAMagnitude,
		reason: "`make(_, len(nd.fields))`, whose length is a field COUNT — bounded by the input, since every " +
			"field costs bytes to write. The per-entry `1 + <minimum>` that used to live here (and separately " +
			"in resolveMap and skipMap) is now the single mapEntryMinBytes rowed above: three sites each " +
			"reasoning out the same ceiling is the shape that leaves the question with no owner",
	},
	{
		where: "schema.go::maxDecimalDigits", count: 3, verdict: magSaturated,
		reason: "`8*size - 1` and the float scale that follows. Asks the shared accessor rather than clamping to a " +
			"ceiling of its own; magnitudeWidestMultiplier is this site's factor and is what the ceiling is chosen against",
	},
	{
		where: "json_decode.go::jsonDecodeAppliesLogical", count: 1, verdict: magBoundedHere,
		reason: "`make(_, probeLen)` is an ALLOCATION, not arithmetic: it needs a far tighter bound than the " +
			"arithmetic ceiling, and caps at the largest length any fixed logical inspects. See the accessor's note " +
			"on why allocation is a different question",
	},
	{
		where: "unsafe.go::udArrayDirect", count: 1, verdict: magNotAMagnitude,
		reason: "elemSize is reflect.Type.Size() — a Go type's in-memory width, fixed by the compiler. The " +
			"derivation has no type information and reads the selector name as a schema size",
	},
	{
		where: "unsafe.go::usArrayDirect", count: 1, verdict: magNotAMagnitude,
		reason: "elemSize is reflect.Type.Size(); see udArrayDirect",
	},
	{
		where: "unsafe.go::udArrayPtrRecord", count: 1, verdict: magNotAMagnitude,
		reason: "innerSize is reflect.Type.Size(); see udArrayDirect",
	},
	{
		where: "unsafe.go::usArrayRecord", count: 1, verdict: magNotAMagnitude,
		reason: "elemSize is reflect.Type.Size(); see udArrayDirect",
	},
	{
		where: "varint.go::appendVarlong", count: 1, verdict: magWrapIsTheContract,
		reason: "`uint64(i) << 1` is the zigzag transform. It operates on a wire value, not a schema magnitude, and " +
			"the shift discarding the top bit is what zigzag IS",
	},
}

// magSeedFields are the schema-object fields that carry a caller-chosen
// magnitude. `size` is the one with no parse-time ceiling; precision and scale
// are capped during validation and are here so a site that starts doing
// arithmetic on them has to say so rather than inherit the cap silently.
var magSeedFields = map[string]bool{
	"size": true, "Size": true,
	"precision": true, "Precision": true,
	"scale": true, "Scale": true,
}

var magArithOps = map[token.Token]bool{
	token.ADD: true, token.SUB: true, token.MUL: true,
	token.QUO: true, token.REM: true, token.SHL: true,
}

var magArithAssign = map[token.Token]bool{
	token.ADD_ASSIGN: true, token.SUB_ASSIGN: true, token.MUL_ASSIGN: true,
	token.QUO_ASSIGN: true, token.REM_ASSIGN: true, token.SHL_ASSIGN: true,
}

var magIntTypes = map[string]bool{
	"int": true, "int8": true, "int16": true, "int32": true, "int64": true,
	"uint": true, "uint8": true, "uint16": true, "uint32": true, "uint64": true,
	"uintptr": true, "byte": true, "rune": true, "laxInt": true,
}

type magSrcFile struct {
	name string
	f    *ast.File
}

type magTaint struct {
	fns    map[string]bool            // integer-returning fns that return a magnitude
	params map[string]map[string]bool // "Recv.Fn" -> integer param names carrying one
}

// magReturnsInteger reports whether fd has an integer-typed result. Integer
// overflow is the class, so a function whose results are all slices, strings,
// errors or structs cannot carry a magnitude onward as an integer.
func magReturnsInteger(fd *ast.FuncDecl) bool {
	if fd.Type.Results == nil {
		return false
	}
	for _, r := range fd.Type.Results.List {
		if id, ok := r.Type.(*ast.Ident); ok && magIntTypes[id.Name] {
			return true
		}
	}
	return false
}

func magFuncName(fd *ast.FuncDecl) string {
	if fd.Recv != nil && len(fd.Recv.List) > 0 {
		t := fd.Recv.List[0].Type
		if star, ok := t.(*ast.StarExpr); ok {
			t = star.X
		}
		if id, ok := t.(*ast.Ident); ok {
			return id.Name + "." + fd.Name.Name
		}
	}
	return fd.Name.Name
}

func magHasSeed(n ast.Node) bool {
	found := false
	ast.Inspect(n, func(x ast.Node) bool {
		if sel, ok := x.(*ast.SelectorExpr); ok && magSeedFields[sel.Sel.Name] {
			found = true
			return false
		}
		return true
	})
	return found
}

func magCalleeName(call *ast.CallExpr) string {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		return fn.Name
	case *ast.SelectorExpr:
		return fn.Sel.Name
	}
	return ""
}

func magCallsMagnitude(n ast.Node, st *magTaint) bool {
	found := false
	ast.Inspect(n, func(x ast.Node) bool {
		if call, ok := x.(*ast.CallExpr); ok && st.fns[magCalleeName(call)] {
			found = true
		}
		return true
	})
	return found
}

func magUsesIdent(n ast.Node, names map[string]bool) bool {
	found := false
	ast.Inspect(n, func(x ast.Node) bool {
		if id, ok := x.(*ast.Ident); ok && names[id.Name] {
			found = true
		}
		return true
	})
	return found
}

// magTainted returns every identifier in fd carrying a magnitude: its tainted
// PARAMETERS plus locals assigned from a seed, a magnitude-returning call, or
// another tainted identifier. The local loop runs to a fixpoint so order of
// assignment inside the function does not matter.
func magTainted(fd *ast.FuncDecl, st *magTaint) map[string]bool {
	out := map[string]bool{}
	for p := range st.params[magFuncName(fd)] {
		out[p] = true
	}
	for {
		grew := false
		ast.Inspect(fd, func(n ast.Node) bool {
			var lhs, rhs []ast.Expr
			switch s := n.(type) {
			case *ast.AssignStmt:
				lhs, rhs = s.Lhs, s.Rhs
			case *ast.ValueSpec:
				for _, id := range s.Names {
					lhs = append(lhs, id)
				}
				rhs = s.Values
			default:
				return true
			}
			hot := false
			for _, r := range rhs {
				if magHasSeed(r) || magCallsMagnitude(r, st) || magUsesIdent(r, out) {
					hot = true
				}
			}
			if !hot {
				return true
			}
			for _, l := range lhs {
				if id, ok := l.(*ast.Ident); ok && !out[id.Name] && id.Name != "_" {
					out[id.Name] = true
					grew = true
				}
			}
			return true
		})
		if !grew {
			return out
		}
	}
}

// magParamNames returns fd's parameter names positionally, blanking any whose
// declared type is not an integer: a magnitude handed over as []byte, string
// or *big.Int has left the integer domain and cannot wrap downstream.
func magParamNames(fd *ast.FuncDecl) []string {
	if fd.Type.Params == nil {
		return nil
	}
	var out []string
	for _, f := range fd.Type.Params.List {
		id, isIdent := f.Type.(*ast.Ident)
		isInt := isIdent && magIntTypes[id.Name]
		if len(f.Names) == 0 {
			out = append(out, "")
			continue
		}
		for _, n := range f.Names {
			if !isInt {
				out = append(out, "")
				continue
			}
			out = append(out, n.Name)
		}
	}
	return out
}

// magScan derives the taint fixpoint and returns hazardous-expression counts
// keyed "file.go::funcName", plus the magnitude-returning function set.
func magScan(t *testing.T) (map[string]int, map[string]bool) {
	t.Helper()
	var files []magSrcFile
	for _, dir := range []string{".", "ocf"} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read %s: %v", dir, err)
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
				continue
			}
			p := filepath.Join(dir, e.Name())
			f, err := parser.ParseFile(token.NewFileSet(), p, nil, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", p, err)
			}
			name := e.Name()
			if dir != "." {
				name = dir + "/" + name
			}
			files = append(files, magSrcFile{name, f})
		}
	}

	type decl struct {
		file string
		fd   *ast.FuncDecl
	}
	var decls []decl
	byName := map[string]*ast.FuncDecl{}
	for _, fl := range files {
		for _, d := range fl.f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Body != nil {
				decls = append(decls, decl{fl.name, fd})
				byName[fd.Name.Name] = fd
			}
		}
	}

	st := &magTaint{fns: map[string]bool{}, params: map[string]map[string]bool{}}
	for {
		grew := false
		for _, d := range decls {
			local := magTainted(d.fd, st)
			if magReturnsInteger(d.fd) && !st.fns[d.fd.Name.Name] {
				ast.Inspect(d.fd.Body, func(n ast.Node) bool {
					rs, ok := n.(*ast.ReturnStmt)
					if !ok {
						return true
					}
					for _, r := range rs.Results {
						if magHasSeed(r) || magCallsMagnitude(r, st) || magUsesIdent(r, local) {
							st.fns[d.fd.Name.Name] = true
							grew = true
						}
					}
					return true
				})
			}
			ast.Inspect(d.fd.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				target, known := byName[magCalleeName(call)]
				if !known {
					return true
				}
				names := magParamNames(target)
				for i, a := range call.Args {
					if i >= len(names) || names[i] == "" || names[i] == "_" {
						continue
					}
					if !magHasSeed(a) && !magCallsMagnitude(a, st) && !magUsesIdent(a, local) {
						continue
					}
					key := magFuncName(target)
					if st.params[key] == nil {
						st.params[key] = map[string]bool{}
					}
					if !st.params[key][names[i]] {
						st.params[key][names[i]] = true
						grew = true
					}
				}
				return true
			})
		}
		if !grew {
			break
		}
	}

	counts := map[string]int{}
	for _, d := range decls {
		local := magTainted(d.fd, st)
		ast.Inspect(d.fd.Body, func(n ast.Node) bool {
			hit := false
			switch s := n.(type) {
			case *ast.BinaryExpr:
				if !magArithOps[s.Op] {
					return true
				}
				hit = magHasSeed(s.X) || magHasSeed(s.Y) ||
					magCallsMagnitude(s.X, st) || magCallsMagnitude(s.Y, st) ||
					magUsesIdent(s.X, local) || magUsesIdent(s.Y, local)
			case *ast.AssignStmt:
				if !magArithAssign[s.Tok] {
					return true
				}
				for _, r := range s.Rhs {
					hit = hit || magHasSeed(r) || magCallsMagnitude(r, st) || magUsesIdent(r, local)
				}
				for _, l := range s.Lhs {
					hit = hit || magUsesIdent(l, local)
				}
			case *ast.CallExpr:
				id, ok := s.Fun.(*ast.Ident)
				if !ok || id.Name != "make" || len(s.Args) < 2 {
					return true
				}
				for _, a := range s.Args[1:] {
					hit = hit || magHasSeed(a) || magCallsMagnitude(a, st) || magUsesIdent(a, local)
				}
			}
			if hit {
				counts[d.file+"::"+magFuncName(d.fd)]++
			}
			return true
		})
	}
	return counts, st.fns
}

// TestInvariant_EveryMagnitudeArithmeticSiteIsClassified is the completeness
// half. A new expression that puts a schema-declared magnitude under an
// arithmetic operator lands with no row and fails here until someone says what
// it does at the top of the range; a row that names no site fails too, so the
// table cannot quietly describe code that no longer exists.
func TestInvariant_EveryMagnitudeArithmeticSiteIsClassified(t *testing.T) {
	counts, magFns := magScan(t)

	// Anti-rot: the derivation is name-based, so a rename of the seed fields
	// or of the producer would leave it scanning for nothing and reporting a
	// clean table. These two functions ARE the class; if the fixpoint stops
	// finding them, the guard is watching an empty set.
	for _, want := range []string{"minBytesFromChildren", "maxDecimalDigits"} {
		if !magFns[want] {
			t.Fatalf("the taint fixpoint no longer reaches %s — the seed fields or the producer were renamed, "+
				"and this guard is now watching nothing", want)
		}
	}
	if len(counts) == 0 {
		t.Fatal("the derivation found no arithmetic on any magnitude at all — it has rotted")
	}

	rows := map[string]magnitudeSite{}
	for _, r := range magnitudeSites {
		if _, dup := rows[r.where]; dup {
			t.Errorf("duplicate row for %s", r.where)
		}
		rows[r.where] = r
	}
	for where, n := range counts {
		row, ok := rows[where]
		if !ok {
			t.Errorf("%s puts a schema-declared magnitude under an arithmetic operator (%d expression(s)) and has no row.\n"+
				"  Say what happens at the top of the range: saturated (asks saturateSchemaMagnitude), bounded-here\n"+
				"  (with the reason its bound differs), not-a-magnitude (with what the operand really is), or\n"+
				"  wrap-is-the-contract. A count alone is what keeps this class open.", where, n)
			continue
		}
		if row.count != n {
			t.Errorf("%s now has %d magnitude-arithmetic expression(s), the table says %d.\n"+
				"  A changed count means an expression was added or removed here; re-read the row's reason (%q)\n"+
				"  and confirm it still covers every expression at this site.", where, n, row.count, row.reason)
		}
	}
	for where := range rows {
		if _, ok := counts[where]; !ok {
			t.Errorf("row %s names no magnitude arithmetic in the sources — the site moved or was deleted, "+
				"and this row now reads as coverage it does not have", where)
		}
	}
	t.Logf("classified %d sites: %d saturated, %d bounded-here, %d not-a-magnitude, %d wrap-is-the-contract",
		len(magnitudeSites),
		magCountVerdict(magSaturated), magCountVerdict(magBoundedHere),
		magCountVerdict(magNotAMagnitude), magCountVerdict(magWrapIsTheContract))
}

func magCountVerdict(v magVerdict) int {
	n := 0
	for _, r := range magnitudeSites {
		if r.verdict == v {
			n++
		}
	}
	return n
}

// TestInvariant_ClippedMagnitudeStillRejects is the control for the one thing
// saturation gives up: a magnitude above the ceiling yields a LOOSER
// buffer-relative bound than the true magnitude would. The bound still has to
// refuse a block that cannot fit, which for any buffer below the ceiling it
// does on the bound itself — so the concession costs an error message on
// absurd schemas and nothing else.
func TestInvariant_ClippedMagnitudeStillRejects(t *testing.T) {
	// Each element needs 2^40 bytes; the ceiling clips that to 2^27.
	s := mustParse(t, `{"type":"array","items":{"type":"fixed","name":"CBF","size":1099511627776}}`)
	for _, count := range []int{1, 4, 1000} {
		wire := append([]byte{byte(count << 1)}, make([]byte, 1<<16)...)
		var v any
		if _, err := s.Decode(wire, &v); err == nil {
			t.Errorf("a block claiming %d elements of 2^40 bytes was accepted from a 64 KiB buffer", count)
		}
	}
}

// ---------- union_tag_tiers_test.go ----------

// ---------------------------------------------------------------------------
// The tag namespace's TIER SET is derived, not listed.
//
// Two consumers read it: findUnionBranch resolves a caller-written name, and
// fillUnionTagTables builds the binary tagged-map lookup. Both walk
// unionTagTiers, so neither can grow a tier the other lacks. What remains
// possible is someone adding a tier by HAND inside one of them, or adding one
// to the slice that no test ever reaches — and those are what these guards
// refuse.
// ---------------------------------------------------------------------------

// unionTagTierCount is the number of tiers the suite knows how to reach. It is
// stated here so that adding a tier is a DECISION: the count fails, and the
// person adding it has to extend the corpus in
// TestInvariant_EveryUnionTagTierIsReachable rather than let a tier ship
// unexercised.
const unionTagTierCount = 3

func TestInvariant_UnionTagTierCountIsStated(t *testing.T) {
	if len(unionTagTiers) != unionTagTierCount {
		t.Fatalf("unionTagTiers has %d tiers, the suite is written for %d.\n"+
			"A tier is a new rule by which a caller's tag names a branch: extend the corpus in "+
			"TestInvariant_EveryUnionTagTierIsReachable so the new tier is actually exercised on every "+
			"wire, then raise this count.", len(unionTagTiers), unionTagTierCount)
	}
	seen := map[string]bool{}
	for i, tier := range unionTagTiers {
		if tier.name == "" {
			t.Errorf("tier %d has no name; the guards report by name", i)
		}
		if seen[tier.name] {
			t.Errorf("two tiers are both named %q", tier.name)
		}
		seen[tier.name] = true
		if tier.claim == nil {
			t.Errorf("tier %q has no claim function", tier.name)
		}
	}
}

// funcBody returns the source of the named top-level function in file.
func funcBody(t *testing.T, file, fn string) string {
	t.Helper()
	src, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	start := strings.Index(string(src), "\nfunc "+fn+"(")
	if start < 0 {
		t.Fatalf("%s: no top-level func %s", file, fn)
	}
	rest := string(src)[start+1:]
	end := strings.Index(rest, "\n}\n")
	if end < 0 {
		t.Fatalf("%s: func %s has no terminator", file, fn)
	}
	return rest[:end]
}

// TestInvariant_UnionTagTiersAreDerived is the source-level half: both
// consumers must reach the tier set by WALKING it. A tier open-coded inside
// either one is invisible to the other, which is the drift this whole
// structure exists to remove — and it is exactly how the legacy
// "<kind>.<logicalType>" spelling came to be honored by the resolver and not
// by the lookup table.
func TestInvariant_UnionTagTiersAreDerived(t *testing.T) {
	resolver := funcBody(t, "json_codec.go", "scanUnionBranch")
	if n := strings.Count(resolver, "range unionTagTiers"); n != 1 {
		t.Errorf("scanUnionBranch walks the tier slice %d times, want exactly 1", n)
	}
	// One scan over the branches, and it must be the one INSIDE the tier walk.
	// A second scan is a hand-written tier: it answers names the lookup table
	// will never register.
	if n := strings.Count(resolver, "range union.branches"); n != 1 {
		t.Errorf("scanUnionBranch scans union.branches %d times, want exactly 1 (inside the tier walk).\n"+
			"A scan outside the walk is a tier only the resolver knows about; move it into unionTagTiers "+
			"so fillUnionTagTables honors it too.", n)
	}
	// The name a caller writes is resolved through the parse-time table, not
	// by re-walking the tiers per value. A tier walk reappearing here is both
	// a per-value cost linear in the branch count and a second place the tier
	// rule could be stated.
	lookup := funcBody(t, "json_codec.go", "findUnionBranch")
	if n := strings.Count(lookup, "range unionTagTiers"); n != 0 {
		t.Errorf("findUnionBranch walks the tier slice %d times, want 0 — it must ask the table the walk already built", n)
	}
	if !strings.Contains(lookup, "union.tags.byName[name]") {
		t.Error("findUnionBranch no longer reads unionTags.byName; the per-value question has to be answered by the table")
	}
	builder := funcBody(t, "schema.go", "fillUnionTagTables")
	if n := strings.Count(builder, "range unionTagTiers"); n != 1 {
		t.Errorf("fillUnionTagTables walks the tier slice %d times, want exactly 1", n)
	}
	// The kind vocabulary the logical-qualifier tier is defined over must have
	// exactly one copy, and it must live in the tier. A second copy is the
	// same set written twice, which is how the two sides drifted before.
	const kindList = `case "null", "boolean", "int", "long", "float", "double", "string", "bytes", "fixed":`
	src, err := os.ReadFile("json_codec.go")
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(src), kindList); n != 1 {
		t.Errorf("the logical-qualifier kind vocabulary appears %d times in json_codec.go, want 1", n)
	}
	if !strings.Contains(tierSource(t, "logical qualifier"), kindList) {
		t.Errorf("the logical-qualifier kind vocabulary is not inside its own tier")
	}
}

// tierSource returns the source text of the named tier's literal.
func tierSource(t *testing.T, name string) string {
	t.Helper()
	src, err := os.ReadFile("json_codec.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	i := strings.Index(s, "unionTagTiers = []unionTagTier{")
	if i < 0 {
		t.Fatal("no unionTagTiers literal")
	}
	j := strings.Index(s[i:], "\n}\n")
	block := s[i : i+j]
	k := strings.Index(block, "name:    "+`"`+name+`"`)
	if k < 0 {
		k = strings.Index(block, "name: "+`"`+name+`"`)
	}
	if k < 0 {
		t.Fatalf("tier %q not found in the literal", name)
	}
	return block[k:]
}

// tierAnswering reports which tier resolves name against union, or -1.
// It re-walks the tiers the way findUnionBranch does, so it can attribute a
// resolution rather than guess at it.
func tierAnswering(union *schemaNode, name string) int {
	for ti, tier := range unionTagTiers {
		var match bool
		var found int
		for _, b := range union.branches {
			if b == nil {
				continue
			}
			if !tierMatches(tier, b, name) {
				continue
			}
			if !tier.guarded {
				return ti
			}
			if match {
				found = -1
				break
			}
			match, found = true, ti
		}
		if match && found >= 0 {
			return ti
		}
		if found == -1 {
			return -1 // refused as ambiguous by this tier
		}
	}
	return -1
}

// TestInvariant_EveryUnionTagTierIsReachable is the behavioral half: every
// tier in the slice must actually answer for some (union, tag) in the corpus.
// A tier nothing reaches is a rule nothing tests, and its guard would neuter
// green. This is the assertion that has to be extended when a tier is added.
func TestInvariant_EveryUnionTagTierIsReachable(t *testing.T) {
	type cell struct{ schema, tag string }
	corpus := []cell{
		// exact name
		{`["null","int"]`, "int"},
		{`["null",{"type":"record","name":"R","namespace":"ns","fields":[{"name":"x","type":"int"}]}]`, "ns.R"},
		// logical qualifier, primitive-backed and named-fixed-backed
		{`["null",{"type":"long","logicalType":"timestamp-millis"}]`, "long.timestamp-millis"},
		{`["null",{"type":"fixed","name":"F","namespace":"n","size":16,"logicalType":"uuid"}]`, "fixed.uuid"},
		// unqualified short name
		{`["null",{"type":"record","name":"R","namespace":"ns","fields":[{"name":"x","type":"int"}]}]`, "R"},
	}
	reached := make([]bool, len(unionTagTiers))
	for _, c := range corpus {
		s, err := Parse(c.schema)
		if err != nil {
			t.Fatalf("%s: %v", c.schema, err)
		}
		ti := tierAnswering(s.node, c.tag)
		if ti < 0 {
			t.Errorf("no tier answers %q for %s", c.tag, c.schema)
			continue
		}
		reached[ti] = true
		// The attribution must agree with the real resolver.
		if got := findUnionBranch(s.node, c.tag); got == nil {
			t.Errorf("tier %d attributed %q for %s, but findUnionBranch refuses it",
				ti, c.tag, c.schema)
		}
	}
	for i, ok := range reached {
		if !ok {
			t.Errorf("tier %q (index %d) is never reached by the corpus — it ships unexercised",
				unionTagTiers[i].name, i)
		}
	}
}

// TestInvariant_GuardedTiersRefuseAmbiguity states the rule the guarded flag
// encodes, over every guarded tier: a name two branches claim within one tier
// resolves NOWHERE. Silently taking the first is a coin flip between two
// branches the caller may have meant either of, and the two wires would have
// to make the same coin flip to stay in agreement.
func TestInvariant_GuardedTiersRefuseAmbiguity(t *testing.T) {
	cells := []struct {
		tier   string
		schema string
		tag    string
	}{
		{"logical qualifier",
			`["null",{"type":"fixed","name":"A","size":16,"logicalType":"uuid"},{"type":"fixed","name":"B","size":16,"logicalType":"uuid"}]`,
			"fixed.uuid"},
		{"unqualified short name",
			`["null",{"type":"record","name":"R","namespace":"n1","fields":[{"name":"x","type":"int"}]},{"type":"record","name":"R","namespace":"n2","fields":[{"name":"y","type":"int"}]}]`,
			"R"},
	}
	guarded := map[string]bool{}
	for _, tier := range unionTagTiers {
		if tier.guarded {
			guarded[tier.name] = true
		}
	}
	covered := map[string]bool{}
	for _, c := range cells {
		if !guarded[c.tier] {
			t.Errorf("cell names tier %q, which is not guarded", c.tier)
			continue
		}
		covered[c.tier] = true
		s, err := Parse(c.schema)
		if err != nil {
			t.Fatalf("%s: %v", c.schema, err)
		}
		if b := findUnionBranch(s.node, c.tag); b != nil {
			t.Errorf("tier %q: %q is claimed by two branches yet resolved to %q — a guarded tier must refuse it",
				c.tier, c.tag, b.name)
		}
		// The other wire has to refuse it too, or a caller gets a value on one
		// and an error on the other. Probed through the public encoder rather
		// than the table so the assertion is about what a caller sees.
		if _, err := s.Encode(map[string]any{c.tag: []byte("0123456789abcdef")}); err == nil {
			t.Errorf("tier %q: %q is claimed by two branches yet the binary tagged-map encode accepted it — "+
				"the two wires disagree", c.tier, c.tag)
		}
	}
	for name := range guarded {
		if !covered[name] {
			t.Errorf("guarded tier %q has no ambiguity cell; its guard is unexercised", name)
		}
	}
}

// TestInvariant_UnionTagResolveDoesNotAllocate locks the property that made it
// safe to route the resolver through a shared tier slice rather than leaving
// the rules open-coded: a tier appends its claim into a stack buffer and the
// comparison `string(claimed) == name` is a compare, not a conversion, so
// resolving a tag allocates nothing however many tiers exist. A tier that
// builds its claim some other way — fmt, a map lookup, strings.Join — would
// put an allocation on a per-value JSON path, and this is where that shows up.
func TestInvariant_UnionTagResolveDoesNotAllocate(t *testing.T) {
	s := MustParse(`["null",{"type":"long","logicalType":"timestamp-millis"},` +
		`{"type":"fixed","name":"F","namespace":"n","size":16,"logicalType":"uuid"},` +
		`{"type":"record","name":"R","namespace":"ns","fields":[{"name":"x","type":"int"}]}]`)
	for _, tag := range []string{
		"long.timestamp-millis", // logical qualifier, primitive
		"fixed.uuid",            // logical qualifier, named fixed
		"n.F",                   // exact
		"R",                     // unqualified short name
		"absent",                // no tier claims it: every tier runs
	} {
		got := testing.AllocsPerRun(50, func() {
			findUnionBranch(s.node, tag)
		})
		if got != 0 {
			t.Errorf("resolving %q allocates %.0f times per call; the tier walk must stay allocation-free", tag, got)
		}
	}
}

// ---------- text_appender_contract_test.go ----------

// freshShortAppender violates the encoding.TextAppender contract by
// returning a fresh slice instead of appending to its input — the
// realistic implementation bug `return []byte(s), nil`. Its returned
// slice is SHORTER than the accumulated output the encoder handed it.
type freshShortAppender struct{}

func (freshShortAppender) AppendText(b []byte) ([]byte, error) { return []byte("x"), nil }

// TestRegression_AppendTextShortReturnNamedError pins that a
// contract-violating TextAppender whose returned slice is SHORTER than
// its input surfaces as a named *SemanticError from binary encode —
// never a slice-bounds panic. appendAvroString's inline-write backfill
// derives the text length from the returned slice; without the length
// guard the arithmetic indexes dst[mark:...] past the end of the fresh
// short slice and panics the calling goroutine. The record's first
// field makes the accumulated output longer than the fresh return, the
// shape that drives the arithmetic out of bounds.
func TestRegression_AppendTextShortReturnNamedError(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Encode panicked, want named error: %v", r)
		}
	}()
	s := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"string"},{"name":"b","type":"string"}]}`)
	_, err := s.Encode(map[string]any{"a": "0123456789", "b": freshShortAppender{}})
	if err == nil {
		t.Fatal("want error for AppendText returning a shorter slice, got nil")
	}
	var se *SemanticError
	if !errors.As(err, &se) {
		t.Fatalf("want *SemanticError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "AppendText returned a slice shorter than its input") {
		t.Fatalf("error does not name the AppendText contract violation: %v", err)
	}
}

// contractAppender's mode selects the return shape of AppendText,
// covering the legal contract forms and the violating ones.
type contractAppender struct{ mode string }

func (c contractAppender) AppendText(b []byte) ([]byte, error) {
	switch c.mode {
	case "legal-append":
		return append(b, "hello"...), nil
	case "legal-zero-append":
		return b, nil
	case "fresh-short":
		return []byte("x"), nil
	case "fresh-long":
		return []byte(strings.Repeat("z", 64)), nil
	case "fresh-equal-len":
		return bytes.Repeat([]byte{'q'}, len(b)), nil
	case "error-return":
		return nil, errors.New("appender boom")
	}
	panic("unknown mode " + c.mode)
}

type appenderStruct struct {
	A string           `avro:"a"`
	B contractAppender `avro:"b"`
}

// TestMatrix_AppendTextReturnShapes crosses every AppendText return
// shape with every encode position that reaches appendAvroString's
// inline-write backfill. The contract:
//
//   - No return shape may panic, anywhere.
//   - A legal append (including a zero-length append) produces wire
//     bytes byte-identical to encoding the equivalent plain string —
//     the happy path is untouched by the shrunk-return guard.
//   - A fresh return SHORTER than the input is the detectable
//     violation (the backfill length arithmetic would go negative):
//     it yields the named *SemanticError at every position.
//   - A fresh return >= the input length passes the length guard and
//     is NOT detectable without comparing prefix bytes on every encode
//     (a per-string memcmp of everything encoded so far — a cost the
//     encoder deliberately does not pay for the caller's own contract
//     violation; encoding/json/v2's jsontext.AppendRaw trusts the
//     append contract the same way). Documenting: those shapes return
//     err == nil with the accumulated output replaced by the fresh
//     slice's content and the length header backfilled at the
//     placeholder offset; the exact observed bytes are pinned below so
//     any future change to this posture is a deliberate one.
//   - An error return surfaces the appender's error.
func TestMatrix_AppendTextReturnShapes(t *testing.T) {
	recordSchema := `{"type":"record","name":"R","fields":[
		{"name":"a","type":"string"},{"name":"b","type":"string"}]}`
	positions := []struct {
		name   string
		schema string
		val    func(v contractAppender) any
		twin   func(tw string) any // same shape with a plain string standing in for the appender
	}{
		{"top-level", `"string"`,
			func(v contractAppender) any { return v },
			func(tw string) any { return tw }},
		{"record-second-field", recordSchema,
			func(v contractAppender) any { return map[string]any{"a": "0123456789", "b": v} },
			func(tw string) any { return map[string]any{"a": "0123456789", "b": tw} }},
		// A struct field routes through the reflect record path (the
		// unsafe string fast paths exclude text-method types), reaching
		// the same backfill; must behave identically to the map form.
		{"record-struct-field", recordSchema,
			func(v contractAppender) any { return appenderStruct{A: "0123456789", B: v} },
			func(tw string) any { return map[string]any{"a": "0123456789", "b": tw} }},
		{"array-element", `{"type":"array","items":"string"}`,
			func(v contractAppender) any { return []any{"0123456789", v} },
			func(tw string) any { return []any{"0123456789", tw} }},
		{"map-value", `{"type":"map","values":"string"}`,
			func(v contractAppender) any { return map[string]any{"k": v} },
			func(tw string) any { return map[string]any{"k": tw} }},
		{"union-branch", `["null","string"]`,
			func(v contractAppender) any { return v },
			func(tw string) any { return tw }},
	}

	// Observed outputs for the undetectable fresh-return shapes
	// (Documenting, see the test doc): the fresh slice's bytes with the
	// real length header backfilled at the placeholder offset, plus any
	// container terminator appended after the corrupted element.
	rep := strings.Repeat
	goldens := map[string]string{
		// Position-dependent detectability (Documenting): at top level
		// the 1-byte fresh-short return has the same length as its input
		// (the placeholder is the only accumulated byte), so the
		// shorter-than-input violation is not length-detectable there —
		// the cell lands in the documented undetectable class and
		// encodes the empty string (the backfilled header is the entire
		// output). At every other position the accumulated output
		// exceeds the fresh return and the guard names the violation.
		"top-level/fresh-short":               "00",
		"top-level/fresh-long":                "7e" + rep("7a", 63),
		"top-level/fresh-equal-len":           "00",
		"record-second-field/fresh-long":      rep("7a", 11) + "68" + rep("7a", 52),
		"record-second-field/fresh-equal-len": rep("71", 11) + "00",
		"record-struct-field/fresh-long":      rep("7a", 11) + "68" + rep("7a", 52),
		"record-struct-field/fresh-equal-len": rep("71", 11) + "00",
		"array-element/fresh-long":            rep("7a", 12) + "66" + rep("7a", 51) + "00",
		"array-element/fresh-equal-len":       rep("71", 12) + "0000",
		"map-value/fresh-long":                rep("7a", 3) + "78" + rep("7a", 60) + "00",
		"map-value/fresh-equal-len":           rep("71", 3) + "0000",
		"union-branch/fresh-long":             "7a7c" + rep("7a", 62),
		"union-branch/fresh-equal-len":        "7100",
	}

	shapes := []struct {
		mode  string
		class string // legal | guard | silent | error
		twin  string // legal only: the plain-string equivalent
	}{
		{"legal-append", "legal", "hello"},
		{"legal-zero-append", "legal", ""},
		{"fresh-short", "guard", ""},
		{"fresh-long", "silent", ""},
		{"fresh-equal-len", "silent", ""},
		{"error-return", "error", ""},
	}

	for _, pos := range positions {
		s := MustParse(pos.schema)
		for _, sh := range shapes {
			t.Run(pos.name+"/"+sh.mode, func(t *testing.T) {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("Encode panicked: %v", r)
					}
				}()
				out, err := s.Encode(pos.val(contractAppender{mode: sh.mode}))
				class := sh.class
				if class == "guard" {
					// A guard cell whose observed output is pinned in
					// goldens is a position where the length information
					// does not exist (see the goldens doc) — silent there.
					if _, undetectable := goldens[pos.name+"/"+sh.mode]; undetectable {
						class = "silent"
					}
				}
				switch class {
				case "legal":
					if err != nil {
						t.Fatalf("legal shape errored: %v", err)
					}
					want, werr := s.Encode(pos.twin(sh.twin))
					if werr != nil {
						t.Fatalf("plain-string twin errored: %v", werr)
					}
					if !bytes.Equal(out, want) {
						t.Fatalf("legal shape diverged from plain-string twin:\n got %x\nwant %x", out, want)
					}
				case "guard":
					if err == nil {
						t.Fatalf("want named error, got nil (out=%x)", out)
					}
					var se *SemanticError
					if !errors.As(err, &se) {
						t.Fatalf("want *SemanticError in chain, got %T: %v", err, err)
					}
					if !strings.Contains(err.Error(), "AppendText returned a slice shorter than its input") {
						t.Fatalf("error does not name the violation: %v", err)
					}
				case "silent":
					if err != nil {
						t.Fatalf("documented-silent shape errored: %v", err)
					}
					if got := hex.EncodeToString(out); got != goldens[pos.name+"/"+sh.mode] {
						t.Fatalf("observed silent output changed:\n got %s\nwant %s", got, goldens[pos.name+"/"+sh.mode])
					}
				case "error":
					if err == nil || !strings.Contains(err.Error(), "appender boom") {
						t.Fatalf("want appender's own error, got %v", err)
					}
				}
			})
		}
	}
}

// TestMatrix_AppendTextReturnShapesJSONImmunity pins that the JSON
// encoder cannot be affected by any AppendText return shape: it
// materializes text via AppendText(nil) (textValue), so the returned
// bytes simply ARE the text — there is no backfill arithmetic to
// corrupt and nothing to guard. Documenting: for contract-violating
// appenders the two wire formats legitimately differ — binary rejects
// the shorter-than-input return via the backfill guard while JSON
// emits the fresh slice's content verbatim.
func TestMatrix_AppendTextReturnShapesJSONImmunity(t *testing.T) {
	recordSchema := `{"type":"record","name":"R","fields":[
		{"name":"a","type":"string"},{"name":"b","type":"string"}]}`
	positions := []struct {
		name   string
		schema string
		val    func(v contractAppender) any
		wrap   func(cell string) string // cell JSON → full document
	}{
		{"top-level", `"string"`,
			func(v contractAppender) any { return v },
			func(cell string) string { return cell }},
		{"record-second-field", recordSchema,
			func(v contractAppender) any { return map[string]any{"a": "0123456789", "b": v} },
			func(cell string) string { return `{"a":"0123456789","b":` + cell + `}` }},
	}
	shapes := []struct {
		mode string
		cell string // expected JSON for the appender's field; "" means expect the appender's error
	}{
		{"legal-append", `"hello"`},
		{"legal-zero-append", `""`},
		// AppendText(nil): a fresh return is the text, verbatim.
		{"fresh-short", `"x"`},
		{"fresh-long", `"` + strings.Repeat("z", 64) + `"`},
		// len(nil) == 0, so the equal-len fresh return is empty.
		{"fresh-equal-len", `""`},
		{"error-return", ""},
	}
	for _, pos := range positions {
		s := MustParse(pos.schema)
		for _, sh := range shapes {
			t.Run(pos.name+"/"+sh.mode, func(t *testing.T) {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("EncodeJSON panicked: %v", r)
					}
				}()
				out, err := s.EncodeJSON(pos.val(contractAppender{mode: sh.mode}))
				if sh.cell == "" {
					if err == nil || !strings.Contains(err.Error(), "appender boom") {
						t.Fatalf("want appender's own error, got %v", err)
					}
					return
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if want := pos.wrap(sh.cell); string(out) != want {
					t.Fatalf("json output:\n got %s\nwant %s", out, want)
				}
			})
		}
	}
}

// ---------- text_interface_precedence_test.go ----------

// The text-interface (TextMarshaler / AppendText / TextUnmarshaler)
// precedence contract, pinned here for the string / enum / uuid sites on
// both the binary and JSON paths:
//
//   - For a bytes-shaped UUID wire (fixed+uuid), a [16]byte-shaped Go type
//     is authoritative by its raw bytes; the text interface is NOT
//     consulted. The 16 bytes ARE the UUID, so a MarshalText->parseUUID
//     round trip would be redundant and would let a non-canonical text
//     method diverge the binary and JSON wire.
//   - Everywhere the text interface IS consulted, it is tried BEFORE the
//     reflect.String fast path (and, for enum, before the int-ordinal
//     arm), matching encoding/json's preference for TextMarshaler and
//     Java's name-based enum matching.

// nonCanonicalArrUUID is a [16]byte that also implements the text
// interfaces, deliberately NON-canonically: MarshalText ignores the bytes
// and returns the all-zero UUID; UnmarshalText ignores its input and
// writes all-0xFF. For fixed+uuid the wire is the raw 16 bytes, so if
// either text method fired the encoded/decoded value would reflect
// zeros/0xFF and the binary and JSON paths would disagree.
type nonCanonicalArrUUID [16]byte

func (nonCanonicalArrUUID) MarshalText() ([]byte, error) {
	return []byte("00000000-0000-0000-0000-000000000000"), nil
}

func (u *nonCanonicalArrUUID) UnmarshalText([]byte) error {
	for i := range u {
		u[i] = 0xFF
	}
	return nil
}

func TestRegression_FixedUUIDByteArrayTrustsRawBytes(t *testing.T) {
	s := MustParse(`{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`)
	in := nonCanonicalArrUUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	bin := mustEncode(t, s, in)
	// fixed has no length prefix; the wire is exactly the raw 16 bytes.
	// If MarshalText fired the wire would be the all-zero UUID's bytes.
	if len(bin) != 16 {
		t.Fatalf("binary wire len = %d, want 16", len(bin))
	}
	for i := range bin {
		if bin[i] != byte(i+1) {
			t.Fatalf("binary wire = % x, want raw 01..10 (MarshalText must not fire)", bin)
		}
	}

	jsonW := mustEncodeJSON(t, s, in)

	// Both wires decode back to the raw bytes; the [16]byte target trusts
	// them (UnmarshalText, which would write 0xFF, must not fire). Binary
	// and JSON must agree.
	var binBack, jsonBack nonCanonicalArrUUID
	mustDecode(t, s, bin, &binBack)
	mustDecodeJSON(t, s, jsonW, &jsonBack)
	if binBack != in {
		t.Fatalf("binary round-trip = % x, want % x (UnmarshalText must not fire)", binBack[:], in[:])
	}
	if jsonBack != in {
		t.Fatalf("json round-trip = % x, want % x (UnmarshalText must not fire)", jsonBack[:], in[:])
	}
	if binBack != jsonBack {
		t.Fatalf("binary vs JSON decode diverge: % x vs % x", binBack[:], jsonBack[:])
	}
}

// upperString is a string-kind type whose text methods transform the
// value (MarshalText uppercases, UnmarshalText lowercases). A string-kind
// type implementing TextMarshaler must use the marshaled form rather than
// its raw underlying string, matching encoding/json and keeping binary and
// JSON in lockstep.
type upperString string

func (u upperString) MarshalText() ([]byte, error) {
	return []byte(strings.ToUpper(string(u))), nil
}

func (u *upperString) UnmarshalText(b []byte) error {
	*u = upperString(strings.ToLower(string(b)))
	return nil
}

func TestRegression_StringKindPrefersTextMarshaler(t *testing.T) {
	s := MustParse(`"string"`)
	in := upperString("hello")

	bin := mustEncode(t, s, in)
	// 1-byte length prefix (zigzag(5)=0x0a), then the MARSHALED form
	// "HELLO" — not the raw underlying string "hello".
	if got := string(bin[1:]); got != "HELLO" {
		t.Fatalf("binary wire body = %q, want HELLO (TextMarshaler, not the raw string)", got)
	}
	jsonW := mustEncodeJSON(t, s, in)
	if string(jsonW) != `"HELLO"` {
		t.Fatalf("json wire = %s, want \"HELLO\"", jsonW)
	}

	// Decode applies UnmarshalText (lowercases): wire "HELLO" -> "hello"
	// on both paths.
	var binBack, jsonBack upperString
	mustDecode(t, s, bin, &binBack)
	mustDecodeJSON(t, s, jsonW, &jsonBack)
	if binBack != "hello" {
		t.Fatalf("binary decode = %q, want hello (UnmarshalText lowercases)", binBack)
	}
	if jsonBack != "hello" {
		t.Fatalf("json decode = %q, want hello (UnmarshalText lowercases)", jsonBack)
	}
}

// TestRegression_StringKindTextMarshalerConsistentAcrossContexts pins that
// a string-kind type with text methods is handled identically whether it
// appears as a scalar, a struct field, or a container element. The unsafe
// struct fast paths (usString / udStringDeser) and the array/map fast loops
// (which capture reflect.Value.SetString) bypass appendAvroString /
// setStringValue, so they must be gated off for text-method types — else a
// struct field or container element would encode/decode its raw string
// while the same value as a scalar uses MarshalText/UnmarshalText.
func TestRegression_StringKindTextMarshalerConsistentAcrossContexts(t *testing.T) {
	// struct field — exercises usString / udStringDeser compile gates.
	t.Run("struct field", func(t *testing.T) {
		type rec struct {
			F upperString `avro:"f"`
		}
		s := MustParse(`{"type":"record","name":"R","fields":[{"name":"f","type":"string"}]}`)
		bin := mustEncode(t, s, rec{F: "hello"})
		js := mustEncodeJSON(t, s, rec{F: "hello"})
		if !strings.Contains(string(bin), "HELLO") {
			t.Fatalf("struct binary = %q, want the MarshalText form HELLO", bin)
		}
		if !strings.Contains(string(js), "HELLO") {
			t.Fatalf("struct json = %s, want the MarshalText form HELLO", js)
		}
		var binBack, jsonBack rec
		mustDecode(t, s, bin, &binBack)
		mustDecodeJSON(t, s, js, &jsonBack)
		if binBack.F != "hello" || jsonBack.F != "hello" {
			t.Fatalf("struct decode bin=%q json=%q, want hello (UnmarshalText)", binBack.F, jsonBack.F)
		}
	})

	// array element — exercises the deserArrayStringLoop fast-loop gate.
	t.Run("array element", func(t *testing.T) {
		s := MustParse(`{"type":"array","items":"string"}`)
		bin := mustEncode(t, s, []upperString{"hello", "world"})
		js := mustEncodeJSON(t, s, []upperString{"hello", "world"})
		if string(js) != `["HELLO","WORLD"]` {
			t.Fatalf("array json = %s, want [\"HELLO\",\"WORLD\"]", js)
		}
		var binBack, jsonBack []upperString
		mustDecode(t, s, bin, &binBack)
		mustDecodeJSON(t, s, js, &jsonBack)
		want := []upperString{"hello", "world"}
		for i := range want {
			if binBack[i] != want[i] || jsonBack[i] != want[i] {
				t.Fatalf("array decode bin=%v json=%v, want %v (UnmarshalText)", binBack, jsonBack, want)
			}
		}
	})

	// map value — exercises the deserMapStringBlock fast-loop gate.
	t.Run("map value", func(t *testing.T) {
		s := MustParse(`{"type":"map","values":"string"}`)
		bin := mustEncode(t, s, map[string]upperString{"k": "hello"})
		js := mustEncodeJSON(t, s, map[string]upperString{"k": "hello"})
		if !strings.Contains(string(bin), "HELLO") || !strings.Contains(string(js), "HELLO") {
			t.Fatalf("map encode bin=%q json=%s, want MarshalText form HELLO", bin, js)
		}
		var binBack, jsonBack map[string]upperString
		mustDecode(t, s, bin, &binBack)
		mustDecodeJSON(t, s, js, &jsonBack)
		if binBack["k"] != "hello" || jsonBack["k"] != "hello" {
			t.Fatalf("map decode bin=%q json=%q, want hello (UnmarshalText)", binBack["k"], jsonBack["k"])
		}
	})
}

var errUnknownOrdinalColor = errors.New("unknown ordinalColor symbol")

var enumColorNames = [...]string{"RED", "GREEN", "BLUE"}

// ordinalColor is an int-kind enum carrier with name-based text methods.
// Its Go integer values (ordRed=0, ordGreen=1, ordBlue=2) deliberately do
// NOT line up with the Avro symbol order used in the test
// (["BLUE","GREEN","RED"], where RED is ordinal 2). Name-based matching
// via the text interface must win over trusting the Go int as the ordinal.
type ordinalColor int

const (
	ordRed ordinalColor = iota
	ordGreen
	ordBlue
)

func (c ordinalColor) MarshalText() ([]byte, error) {
	if int(c) < 0 || int(c) >= len(enumColorNames) {
		return nil, errUnknownOrdinalColor
	}
	return []byte(enumColorNames[c]), nil
}

func (c *ordinalColor) UnmarshalText(b []byte) error {
	for i, n := range enumColorNames {
		if n == string(b) {
			*c = ordinalColor(i)
			return nil
		}
	}
	return errUnknownOrdinalColor
}

func TestRegression_EnumTextMarshalerNameMatchOverOrdinal(t *testing.T) {
	// Avro symbol order differs from the Go int order: "RED" is Avro
	// ordinal 2 here, while ordRed is Go int 0.
	s := MustParse(`{"type":"enum","name":"C","symbols":["BLUE","GREEN","RED"]}`)
	in := ordRed // Go 0; symbol "RED"; Avro ordinal 2

	bin := mustEncode(t, s, in)
	// zigzag(2) = 0x04. If the encoder trusted the Go int (0) as the
	// ordinal, the wire would be zigzag(0) = 0x00 = "BLUE".
	if len(bin) != 1 || bin[0] != 0x04 {
		t.Fatalf("binary wire = % x, want 04 (RED = Avro ordinal 2, name-matched)", bin)
	}
	jsonW := mustEncodeJSON(t, s, in)
	if string(jsonW) != `"RED"` {
		t.Fatalf("json wire = %s, want \"RED\"", jsonW)
	}

	// Round-trip: wire ordinal 2 = "RED" -> UnmarshalText -> ordRed (Go 0).
	var binBack, jsonBack ordinalColor
	mustDecode(t, s, bin, &binBack)
	mustDecodeJSON(t, s, jsonW, &jsonBack)
	if binBack != ordRed {
		t.Fatalf("binary decode = %d, want ordRed=0 (name-matched UnmarshalText)", binBack)
	}
	if jsonBack != ordRed {
		t.Fatalf("json decode = %d, want ordRed=0", jsonBack)
	}
}

// SchemaFor must infer fixed(16) uuid for a ,uuid-tagged [16]byte EVEN WHEN
// the type implements a text interface. The codec trusts the raw bytes of a
// uuid-on-fixed [16]byte and never consults its text method (see
// TestRegression_FixedUUIDByteArrayTrustsRawBytes), so inferring a plain Avro
// "string" silently drops both the fixed(16) shape and the uuid logical type,
// and produces a different wire format / fingerprint than an identical
// text-less [16]byte field. The text-interface arm in inferType must not
// intercept a uuid [16]byte headed for the fixed(16) Array case.
func TestRegression_SchemaForUUIDByteArrayWithTextMethod(t *testing.T) {
	// Bug case: nonCanonicalArrUUID is a [16]byte with MarshalText +
	// UnmarshalText (mirrors github.com/google/uuid.UUID).
	type recTexty struct {
		ID nonCanonicalArrUUID `avro:"id,uuid"`
	}
	s, err := SchemaFor[recTexty]()
	if err != nil {
		t.Fatalf("SchemaFor[recTexty]: %v", err)
	}
	if ft := s.Root().Fields[0].Type; ft.Type != "fixed" || ft.Size != 16 || ft.LogicalType != "uuid" {
		t.Fatalf("uuid [16]byte with a text method: want fixed(16) uuid, got Type=%q Size=%d LogicalType=%q",
			ft.Type, ft.Size, ft.LogicalType)
	}

	// Boundary-1 control (must still hold): a plain [16]byte with no text
	// method already infers fixed(16) uuid.
	type recPlain struct {
		ID [16]byte `avro:"id,uuid"`
	}
	sp, err := SchemaFor[recPlain]()
	if err != nil {
		t.Fatalf("SchemaFor[recPlain]: %v", err)
	}
	if fp := sp.Root().Fields[0].Type; fp.Type != "fixed" || fp.Size != 16 || fp.LogicalType != "uuid" {
		t.Fatalf("plain uuid [16]byte: want fixed(16) uuid, got Type=%q Size=%d LogicalType=%q",
			fp.Type, fp.Size, fp.LogicalType)
	}
}

// ---------- decode_reencode_audit_test.go ----------

// TestDecodeReencodeSymmetry verifies that values the decoder produces
// can be re-encoded by the encoder. A pair where decode succeeds but
// encode of the decoded value fails would be a round-trip asymmetry.
//
// Exercises every Avro type with *any and one or more representative
// typed targets, then re-encodes each decoded value. Wire-byte
// canonicalization on the encoder side is fine; the Go-value
// round-trip is what we lock in.
//
// Documented intentional asymmetries are excluded here and pinned by their own
// tests — [TestTextUnmarshalerOnlyDecodeOnly] below for the TextUnmarshaler-only
// case, which is the one this sweep would otherwise report as a round-trip
// failure. Both of this sentence's previous pointers had rotted: a
// SKIPPED_FOLLOWUPS.md that no longer exists and a test name that never did, so
// an exclusion the reader could not check read as one they could.
func TestDecodeReencodeSymmetry(t *testing.T) {
	type tc struct {
		name   string
		schema string
		// build a fresh encoded payload for the test
		encoded func() []byte
		// list of decode target types to probe
		targets []func() any
	}

	// shorthand encoder
	enc := func(t *testing.T, schemaStr string, v any) []byte {
		s := MustParse(schemaStr)
		b, err := s.AppendEncode(nil, v)
		if err != nil {
			t.Fatalf("setup encode failed: %v", err)
		}
		return b
	}

	cases := []tc{
		{
			"boolean",
			`"boolean"`,
			func() []byte { return enc(t, `"boolean"`, true) },
			[]func() any{
				func() any { v := false; return &v },
				func() any { var v any; return &v },
			},
		},
		{
			"int",
			`"int"`,
			func() []byte { return enc(t, `"int"`, int32(42)) },
			[]func() any{
				func() any { var v int32; return &v },
				func() any { var v int64; return &v },
				func() any { var v float32; return &v },
				func() any { var v json.Number; return &v },
				func() any { var v any; return &v },
			},
		},
		{
			"long",
			`"long"`,
			func() []byte { return enc(t, `"long"`, int64(9007199254740993)) },
			[]func() any{
				func() any { var v int64; return &v },
				func() any { var v json.Number; return &v },
				func() any { var v any; return &v },
			},
		},
		{
			"double",
			`"double"`,
			func() []byte { return enc(t, `"double"`, 3.14) },
			[]func() any{
				func() any { var v float64; return &v },
				func() any { var v int64; return &v }, // decode rejects (3.14 is not whole-number); test verifies the skip-on-rejection path
				func() any { var v json.Number; return &v },
				func() any { var v any; return &v },
			},
		},
		{
			"string",
			`"string"`,
			func() []byte { return enc(t, `"string"`, "hello") },
			[]func() any{
				func() any { var v string; return &v },
				func() any { var v []byte; return &v },
				func() any { var v any; return &v },
			},
		},
		{
			"bytes",
			`"bytes"`,
			func() []byte { return enc(t, `"bytes"`, []byte{0xC3, 0xA9}) },
			[]func() any{
				func() any { var v []byte; return &v },
				func() any { var v string; return &v },
				func() any { var v any; return &v },
			},
		},
		{
			"date",
			`{"type":"int","logicalType":"date"}`,
			func() []byte {
				s := MustParse(`{"type":"int","logicalType":"date"}`)
				b, err := s.AppendEncode(nil, time.Date(2025, 5, 21, 0, 0, 0, 0, time.UTC))
				if err != nil {
					t.Fatalf("date encode: %v", err)
				}
				return b
			},
			[]func() any{
				func() any { var v time.Time; return &v },
				func() any { var v int32; return &v },
				func() any { var v string; return &v },
				func() any { var v any; return &v },
			},
		},
		{
			"timestamp-millis",
			`{"type":"long","logicalType":"timestamp-millis"}`,
			func() []byte {
				s := MustParse(`{"type":"long","logicalType":"timestamp-millis"}`)
				b, err := s.AppendEncode(nil, time.Date(2025, 5, 21, 12, 30, 0, 0, time.UTC))
				if err != nil {
					t.Fatalf("ts encode: %v", err)
				}
				return b
			},
			[]func() any{
				func() any { var v time.Time; return &v },
				func() any { var v int64; return &v },
				func() any { var v string; return &v },
				func() any { var v any; return &v },
			},
		},
		{
			"fixed(16)+uuid",
			`{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`,
			func() []byte {
				s := MustParse(`{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`)
				b, err := s.AppendEncode(nil, [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
				if err != nil {
					t.Fatalf("uuid encode: %v", err)
				}
				return b
			},
			[]func() any{
				func() any { var v [16]byte; return &v },
				func() any { var v string; return &v },
				func() any { var v any; return &v },
			},
		},
	}

	for _, c := range cases {
		for _, mk := range c.targets {
			target := mk()
			name := c.name + "/" + reflect.TypeOf(target).Elem().String()
			t.Run(name, func(t *testing.T) {
				wire := c.encoded()
				s := MustParse(c.schema)
				if _, err := s.Decode(wire, target); err != nil {
					// Decode rejection is fine; only
					// decode-succeeds-but-encode-fails is an
					// asymmetry.
					t.Logf("decode skipped (target rejects): %v", err)
					return
				}
				v := reflect.ValueOf(target).Elem().Interface()
				if _, err := s.AppendEncode(nil, v); err != nil {
					t.Fatalf("ASYMMETRY: decode produced %T(%v) but encode of that value rejects: %v", v, v, err)
				}
			})
		}
	}
}

// textUnmarshalerOnly implements UnmarshalText but not MarshalText or
// AppendText — the standard Go pattern for parse-only types (config
// values, enums, lookup keys, one-way ingest pipelines). Decode
// accepts; encode of the produced value rejects because the type
// provides no text-out method. This asymmetry matches Go's stdlib
// idiom — TextUnmarshaler is explicitly a one-way interface — and
// the library doesn't force users to write no-op MarshalText shims
// on types they only ever decode into.
type textUnmarshalerOnly struct{ Got string }

func (t *textUnmarshalerOnly) UnmarshalText(b []byte) error {
	t.Got = string(b)
	return nil
}

// TestTextUnmarshalerOnlyDecodeOnly pins the documented one-way
// pattern: Decode into a TextUnmarshaler-only target succeeds; the
// caller doesn't need a sibling MarshalText. Re-encoding the produced
// value would fail (no text-out method), but that's by user choice —
// the type is decode-only.
func TestTextUnmarshalerOnlyDecodeOnly(t *testing.T) {
	s := MustParse(`"string"`)
	wire := mustAppendEncode(t, s, nil, "hello")

	var got textUnmarshalerOnly
	if _, err := s.Decode(wire, &got); err != nil {
		t.Fatalf("decode into TextUnmarshaler-only: %v", err)
	}
	if got.Got != "hello" {
		t.Fatalf("UnmarshalText not called: got %q", got.Got)
	}

	// Re-encoding the value rejects because the type has no text-out
	// method. Standard Go one-way pattern.
	if _, err := s.AppendEncode(nil, &got); err == nil {
		t.Fatalf("expected encode rejection of TextUnmarshaler-only value; got success")
	}
}

// ptrMarshalerSymmetry has MarshalText and UnmarshalText both on the
// pointer receiver. Used to verify symmetric encode/decode discovery
// of pointer-receiver text methods.
type ptrMarshalerSymmetry struct{ val string }

func (m *ptrMarshalerSymmetry) MarshalText() ([]byte, error) { return []byte(m.val), nil }
func (m *ptrMarshalerSymmetry) UnmarshalText(b []byte) error { m.val = string(b); return nil }

// colorEnum demonstrates a type with both MarshalText and UnmarshalText
// used as an Avro enum carrier (the text matches a symbol).
type colorEnum struct{ symbol string }

func (c colorEnum) MarshalText() ([]byte, error) { return []byte(c.symbol), nil }
func (c *colorEnum) UnmarshalText(b []byte) error {
	c.symbol = string(b)
	return nil
}

// uuidViaText demonstrates a type that carries a UUID via the Text*
// interfaces. The encode side parses the text as a UUID; the decode
// side receives the canonical hex-dash form.
type uuidViaText struct{ s string }

func (u uuidViaText) MarshalText() ([]byte, error) { return []byte(u.s), nil }
func (u *uuidViaText) UnmarshalText(b []byte) error {
	u.s = string(b)
	return nil
}

// TestTextInterfaceCoverageForEnumAndFixedUUID pins that the text-shaped
// Avro sites — enum and fixed+uuid — accept Text* on both binary and
// JSON paths. Parity with string and string+uuid which already accept.
func TestTextInterfaceCoverageForEnumAndFixedUUID(t *testing.T) {
	t.Run("enum binary round-trip via Text*", func(t *testing.T) {
		s := MustParse(`{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]}`)
		wire := mustAppendEncode(t, s, nil, colorEnum{symbol: "GREEN"})
		var got colorEnum
		mustDecode(t, s, wire, &got)
		if got.symbol != "GREEN" {
			t.Fatalf("round-trip: got %q, want GREEN", got.symbol)
		}
	})
	t.Run("enum binary unknown symbol rejects", func(t *testing.T) {
		s := MustParse(`{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]}`)
		if _, err := s.AppendEncode(nil, colorEnum{symbol: "PURPLE"}); err == nil {
			t.Fatalf("expected encode rejection of unknown symbol")
		}
	})
	t.Run("enum JSON round-trip via Text*", func(t *testing.T) {
		s := MustParse(`{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]}`)
		out := mustEncodeJSON(t, s, colorEnum{symbol: "BLUE"})
		if string(out) != `"BLUE"` {
			t.Fatalf("EncodeJSON got %s, want \"BLUE\"", out)
		}
		var got colorEnum
		mustDecodeJSON(t, s, []byte(`"RED"`), &got)
		if got.symbol != "RED" {
			t.Fatalf("DecodeJSON got %q, want RED", got.symbol)
		}
	})

	const uuidSchema = `{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`
	const uuidStr = "12345678-1234-5678-1234-567812345678"

	t.Run("fixed+uuid binary round-trip via Text*", func(t *testing.T) {
		s := MustParse(uuidSchema)
		wire := mustAppendEncode(t, s, nil, uuidViaText{s: uuidStr})
		if len(wire) != 16 {
			t.Fatalf("expected 16-byte wire, got %d", len(wire))
		}
		var got uuidViaText
		mustDecode(t, s, wire, &got)
		if got.s != uuidStr {
			t.Fatalf("round-trip: got %q, want %q", got.s, uuidStr)
		}
	})
	t.Run("fixed+uuid binary malformed text rejects", func(t *testing.T) {
		s := MustParse(uuidSchema)
		if _, err := s.AppendEncode(nil, uuidViaText{s: "not-a-uuid"}); err == nil {
			t.Fatalf("expected encode rejection of non-UUID text")
		}
	})
	t.Run("fixed+uuid JSON round-trip via Text*", func(t *testing.T) {
		s := MustParse(uuidSchema)
		out := mustEncodeJSON(t, s, uuidViaText{s: uuidStr})
		var got uuidViaText
		mustDecodeJSON(t, s, out, &got)
		if got.s != uuidStr {
			t.Fatalf("JSON round-trip: got %q, want %q", got.s, uuidStr)
		}
	})
}

// TestMapRecordEncodeIgnoresAliases pins that map-record encoding
// looks up only by canonical field name across every map encode path
// (binary map[string]any, binary typed-map with plain string key,
// binary typed-map with a named string-key subtype, plus the JSON
// equivalents). Aliases are a reader-side / decode concept (Avro 1.12
// spec; Apache Avro Java GenericDatumWriter; fastavro write_record
// — none of the three reference impls consult aliases on encode);
// our encode matches that. Input keyed by an alias hits the missing-
// field path just like any other unrecognized key. Canonical present
// + extra alias key is silently accepted (the alias key is simply not
// consulted — it's an unrecognized stray, not a collision).
func TestMapRecordEncodeIgnoresAliases(t *testing.T) {
	schema := `{"type":"record","name":"R","fields":[
		{"name":"new_name","type":"long","aliases":["old_name"]}
	]}`
	s := MustParse(schema)

	// --- binary encode ---

	t.Run("binary map[string]any via alias key: missing-field error", func(t *testing.T) {
		if _, err := s.AppendEncode(nil, map[string]any{"old_name": int64(42)}); err == nil {
			t.Fatalf("expected missing-key error; encode silently accepted the alias")
		}
	})

	t.Run("binary map[string]any via canonical key: succeeds", func(t *testing.T) {
		if _, err := s.AppendEncode(nil, map[string]any{"new_name": int64(42)}); err != nil {
			t.Fatalf("binary encode with canonical key: %v", err)
		}
	})

	t.Run("binary map[string]any canonical+stray alias: alias silently ignored", func(t *testing.T) {
		// Canonical present, alias also present; encoder iterates schema
		// fields by canonical name, so the alias key is a stray and is
		// simply not consulted. Same contract as any other unrecognized
		// key in the input map.
		out, err := s.AppendEncode(nil, map[string]any{"new_name": int64(42), "old_name": int64(99)})
		if err != nil {
			t.Fatalf("canonical+stray-alias should succeed: %v", err)
		}
		var got map[string]any
		mustDecode(t, s, out, &got)
		if got["new_name"] != int64(42) {
			t.Fatalf("expected new_name=42 (stray old_name ignored); got %v", got)
		}
	})

	t.Run("binary map[string]int64 via alias key: missing-field error", func(t *testing.T) {
		// Exercises the typed-map encode path (map[string]T with plain
		// string key — different from map[string]any).
		if _, err := s.AppendEncode(nil, map[string]int64{"old_name": 42}); err == nil {
			t.Fatalf("expected missing-key error on typed-map encode; encode accepted alias")
		}
	})

	t.Run("binary map[NK]int64 via alias key: missing-field error", func(t *testing.T) {
		// Exercises the typed-map encode path with a named-string-key
		// subtype (forces mapKeyAs.Convert).
		type NK string
		if _, err := s.AppendEncode(nil, map[NK]int64{"old_name": 42}); err == nil {
			t.Fatalf("expected missing-key error on named-key typed-map encode; encode accepted alias")
		}
	})

	t.Run("binary map[NK]int64 via canonical key: succeeds", func(t *testing.T) {
		type NK string
		if _, err := s.AppendEncode(nil, map[NK]int64{"new_name": 42}); err != nil {
			t.Fatalf("named-key typed-map encode with canonical key: %v", err)
		}
	})

	// --- JSON encode ---

	t.Run("JSON map[string]any via alias key: missing-field error", func(t *testing.T) {
		if _, err := s.EncodeJSON(map[string]any{"old_name": int64(42)}); err == nil {
			t.Fatalf("expected missing-field error; JSON encode silently accepted the alias")
		}
	})

	t.Run("JSON map[string]int64 via alias key: missing-field error", func(t *testing.T) {
		// JSON typed-map (non-map[string]any) generic path.
		if _, err := s.EncodeJSON(map[string]int64{"old_name": 42}); err == nil {
			t.Fatalf("expected missing-field error on JSON typed-map encode; alias accepted")
		}
	})
}

// TestPointerReceiverTextMarshalerSymmetry verifies that the encoder
// reaches a pointer-receiver MarshalText via v.Addr(), matching the
// decoder's TextUnmarshaler lookup via v.Addr(). Without the Addr()
// hop, only value-method-set MarshalText would resolve, silently
// missing a pointer-receiver MarshalText on an addressable struct
// field.
func TestPointerReceiverTextMarshalerSymmetry(t *testing.T) {
	t.Run("via pointer", func(t *testing.T) {
		s := MustParse(`"string"`)
		v := &ptrMarshalerSymmetry{val: "hello"}
		wire := mustAppendEncode(t, s, nil, v)
		var got ptrMarshalerSymmetry
		mustDecode(t, s, wire, &got)
		if got.val != "hello" {
			t.Fatalf("round-trip got %q, want %q", got.val, "hello")
		}
	})
	t.Run("via struct field", func(t *testing.T) {
		type wrapper struct {
			Name ptrMarshalerSymmetry `avro:"name"`
		}
		s := MustParse(`{"type":"record","name":"R","fields":[{"name":"name","type":"string"}]}`)
		in := wrapper{Name: ptrMarshalerSymmetry{val: "world"}}
		wire := mustAppendEncode(t, s, nil, &in)
		var got wrapper
		mustDecode(t, s, wire, &got)
		if got.Name.val != "world" {
			t.Fatalf("round-trip got %q, want %q", got.Name.val, "world")
		}
	})
}
