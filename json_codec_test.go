package avro

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// ---------- json_codec_test.go ----------

// TestMatrix_JSONDecodeOptionsAgreeAcrossFieldProvenance pins that one
// DecodeJSON call answers TaggedUnions and TagLogicalTypes the same way for
// every field, however that field got its value.
//
// A field present in the JSON is decoded by the JSON decoder. A field absent
// from it is filled from its schema default. That routes through the binary
// deser fn, which reads the options off the per-call slab. So we have two
// readers of one option, and a caller sees both in a single returned map. We
// cross the option combination with the field's provenance, then again with
// whether the union branch carries a logical type, since that is what the
// second option renames.
//
// We assert twice per cell. The two provenances must agree; that is the
// cross-path question, and the one a second copy of an option can get wrong.
// Each must also match what the option itself says. We want a {branch: value}
// envelope exactly when TaggedUnions is on, and the branch named with its
// logical type exactly when TagLogicalTypes is on. Agreement alone would stay
// green if both readers were wrong together, and that is what a parity oracle
// cannot see.
func TestMatrix_JSONDecodeOptionsAgreeAcrossFieldProvenance(t *testing.T) {
	const schema = `{"type":"record","name":"R","fields":[
		{"name":"p","type":["int","string"]},
		{"name":"f","type":["int","string"],"default":7},
		{"name":"pl","type":[{"type":"long","logicalType":"timestamp-millis"},"string"]},
		{"name":"fl","type":[{"type":"long","logicalType":"timestamp-millis"},"string"],"default":0}
	]}`
	// p and pl are present; f and fl are absent and fill from their defaults.
	const doc = `{"p":{"int":5},"pl":{"long":1000}}`

	s := mustParse(t, schema)

	combos := []struct {
		name    string
		opts    []Opt
		tagged  bool
		logical bool
	}{
		{"neither", nil, false, false},
		{"tagged", []Opt{TaggedUnions()}, true, false},
		{"taglogical", []Opt{TagLogicalTypes()}, false, true},
		{"both", []Opt{TaggedUnions(), TagLogicalTypes()}, true, true},
	}

	// tagOf reports the envelope key a value is wrapped in, or "" when it is
	// not wrapped at all. A union value is never legitimately a one-key map
	// in this schema, so we can tell the two apart.
	tagOf := func(v any) string {
		m, ok := v.(map[string]any)
		if !ok || len(m) != 1 {
			return ""
		}
		for k := range m {
			return k
		}
		return ""
	}

	realized := 0
	for _, c := range combos {
		for _, pair := range []struct {
			kind            string
			present, filled string
			wantTag         string
		}{
			{"plain", "p", "f", "int"},
			{"logical", "pl", "fl", "long"},
		} {
			t.Run(c.name+"/"+pair.kind, func(t *testing.T) {
				var got map[string]any
				if err := s.DecodeJSON([]byte(doc), &got, c.opts...); err != nil {
					t.Fatalf("decode: %v", err)
				}
				presentTag := tagOf(got[pair.present])
				filledTag := tagOf(got[pair.filled])
				if presentTag != filledTag {
					t.Fatalf("present field %q tagged %q but default-filled field %q tagged %q; one option, two answers",
						pair.present, presentTag, pair.filled, filledTag)
				}

				// We answer from the options alone, so both readers
				// being wrong together cannot pass.
				want := ""
				if c.tagged {
					want = pair.wantTag
					if c.logical && pair.kind == "logical" {
						want += ".timestamp-millis"
					}
				}
				if presentTag != want {
					t.Fatalf("tagged=%v taglogical=%v produced tag %q, want %q", c.tagged, c.logical, presentTag, want)
				}
			})
			realized++
		}
	}
	if want := len(combos) * 2; realized != want {
		t.Fatalf("realized %d cells, want %d", realized, want)
	}
}

func TestEncodeJSON(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		value  any
		want   string
	}{
		{"null", `"null"`, nil, `null`},
		{"boolean", `"boolean"`, true, `true`},
		{"int", `"int"`, int32(42), `42`},
		{"long", `"long"`, int64(123456789), `123456789`},
		{"float", `"float"`, float32(1.5), `1.5`},
		{"double", `"double"`, float64(3.14), `3.14`},
		{"string", `"string"`, "hello", `"hello"`},
		{"bytes", `"bytes"`, []byte{0x00, 0xFF, 0x41}, `"\u0000\u00ffA"`},
		{"enum", `{"type":"enum","name":"Color","symbols":["RED","GREEN"]}`, "RED", `"RED"`},
		{
			"fixed",
			`{"type":"fixed","name":"F","size":3}`,
			[3]byte{0x01, 0x02, 0x03},
			`"\u0001\u0002\u0003"`,
		},
		{
			"array",
			`{"type":"array","items":"int"}`,
			[]any{int32(1), int32(2), int32(3)},
			`[1,2,3]`,
		},
		{
			"map",
			`{"type":"map","values":"int"}`,
			map[string]any{"a": int32(1)},
			`{"a":1}`,
		},
		{
			"union null",
			`["null","string"]`,
			nil,
			`null`,
		},
		{
			"union string",
			`["null","string"]`,
			"hello",
			`"hello"`,
		},
		{
			"union int",
			`["null","int","string"]`,
			int32(42),
			`42`,
		},
		{
			"record",
			`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`,
			map[string]any{"a": int32(1), "b": "hello"},
			`{"a":1,"b":"hello"}`,
		},
		{
			"nested record with union",
			recNameEmailSchema,
			map[string]any{"name": "Alice", "email": "a@b.com"},
			`{"name":"Alice","email":"a@b.com"}`,
		},
		{
			"nested record with null union",
			recNameEmailSchema,
			map[string]any{"name": "Bob", "email": nil},
			`{"name":"Bob","email":null}`,
		},
		{
			"float NaN",
			`"float"`,
			float32(math.Float32frombits(0x7fc00000)),
			`"NaN"`,
		},
		{
			"double Infinity",
			`"double"`,
			math.Inf(1),
			`"Infinity"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustParse(t, tt.schema)
			got := mustEncodeJSON(t, s, tt.value)
			if string(got) != tt.want {
				t.Errorf("got %s, want %s", got, tt.want)
			}
		})
	}
}

func TestDecodeJSON(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		input  string
		want   any
	}{
		{"null", `"null"`, `null`, nil},
		{"boolean", `"boolean"`, `true`, true},
		{"int", `"int"`, `42`, int32(42)},
		{"long", `"long"`, `123456789`, int64(123456789)},
		{"float", `"float"`, `1.5`, float32(1.5)},
		{"double", `"double"`, `3.14`, 3.14},
		{"string", `"string"`, `"hello"`, "hello"},
		{"bytes", `"bytes"`, `"\u0000\u00FFA"`, []byte{0x00, 0xFF, 0x41}},
		{"enum", `{"type":"enum","name":"Color","symbols":["RED","GREEN"]}`, `"RED"`, "RED"},
		{
			"array",
			`{"type":"array","items":"int"}`,
			`[1,2,3]`,
			[]any{int32(1), int32(2), int32(3)},
		},
		{
			"union null",
			`["null","string"]`,
			`null`,
			nil,
		},
		{
			"union string",
			`["null","string"]`,
			`{"string":"hello"}`,
			"hello",
		},
		{
			"record with union",
			recNameEmailSchema,
			`{"name":"Alice","email":{"string":"a@b.com"}}`,
			map[string]any{"name": "Alice", "email": "a@b.com"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustParse(t, tt.schema)
			var got any
			mustDecodeJSON(t, s, []byte(tt.input), &got)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestAvroJSONRoundTrip(t *testing.T) {
	schema := `{
		"type":"record","name":"Event",
		"fields":[
			{"name":"id","type":"string"},
			{"name":"ts","type":"long"},
			{"name":"data","type":"bytes"},
			{"name":"tags","type":{"type":"array","items":"string"}},
			{"name":"meta","type":{"type":"map","values":"int"}},
			{"name":"status","type":{"type":"enum","name":"Status","symbols":["ACTIVE","DELETED"]}},
			{"name":"extra","type":["null","string","int"]}
		]
	}`
	s := mustParse(t, schema)

	original := map[string]any{
		"id":     "abc",
		"ts":     int64(1000),
		"data":   []byte{0x01, 0x02},
		"tags":   []any{"go", "avro"},
		"meta":   map[string]any{"x": int32(1)},
		"status": "ACTIVE",
		"extra":  "hello",
	}

	encoded := mustEncodeJSON(t, s, original)

	// The output must be valid JSON.
	var parsed any
	if err := json.Unmarshal(encoded, &parsed); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, encoded)
	}

	var decoded any
	mustDecodeJSON(t, s, encoded, &decoded)

	m := decoded.(map[string]any)
	if m["id"] != "abc" {
		t.Errorf("id: got %v", m["id"])
	}
	if m["ts"] != int64(1000) {
		t.Errorf("ts: got %v", m["ts"])
	}
	if m["status"] != "ACTIVE" {
		t.Errorf("status: got %v", m["status"])
	}
	if m["extra"] != "hello" {
		t.Errorf("extra: got %v", m["extra"])
	}
}

func TestAvroJSONNamedUnionBranch(t *testing.T) {
	schema := `{
		"type":"record","name":"Wrapper",
		"fields":[{
			"name":"value",
			"type":["null",{"type":"record","name":"Inner","fields":[
				{"name":"x","type":"int"}
			]}]
		}]
	}`
	s := mustParse(t, schema)

	// With TaggedUnions we expect the non-null branch to key off the record
	// name, so "Inner" is the union type name.
	data := map[string]any{
		"value": map[string]any{"x": int32(42)},
	}
	encoded := mustEncodeJSON(t, s, data, TaggedUnions())
	var parsed map[string]any
	json.Unmarshal(encoded, &parsed)
	valueObj, ok := parsed["value"].(map[string]any)
	if !ok {
		t.Fatalf("value: expected object, got %T: %s", parsed["value"], encoded)
	}
	if _, ok := valueObj["Inner"]; !ok {
		t.Errorf("expected Inner key in union, got: %s", encoded)
	}

	var decoded any
	mustDecodeJSON(t, s, encoded, &decoded)
	m := decoded.(map[string]any)
	inner := m["value"].(map[string]any)
	if inner["x"] != int32(42) {
		t.Errorf("x: got %v", inner["x"])
	}
}

func TestDecodeJSONIntoStruct(t *testing.T) {
	schema := recordNameEmailSchema
	s := mustParse(t, schema)

	input := `{"name":"Alice","email":{"string":"a@b.com"}}`
	var got Record
	mustDecodeJSON(t, s, []byte(input), &got)
	if got.Name != "Alice" {
		t.Errorf("name: got %q", got.Name)
	}
	if got.Email == nil || *got.Email != "a@b.com" {
		t.Errorf("email: got %v", got.Email)
	}
}

// TestDecodeJSONUnionTaggedNullIntoAny exercises the union object decode
// path at json_decode.go:809 / 844. When the JSON is a tagged union object
// whose branch is "null" (e.g. {"null":null}), the inner decode produces nil
// and wrapUnion returns nil. The previous code then did
// v.Set(reflect.ValueOf(nil)), which panics with "reflect.Value.Set on zero
// Value". We must decode to a nil any, not panic.
func TestDecodeJSONUnionTaggedNullIntoAny(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		src    string
		opts   []Opt
	}{
		{"two-branch null bare", `["null","int"]`, `null`, nil},
		{"two-branch null tagged", `["null","int"]`, `{"null":null}`, nil},
		{"two-branch null tagged with TaggedUnions", `["null","int"]`, `{"null":null}`, []Opt{TaggedUnions()}},
		{"three-branch null tagged", `["null","int","string"]`, `{"null":null}`, nil},
		{"three-branch null tagged with TaggedUnions", `["null","int","string"]`, `{"null":null}`, []Opt{TaggedUnions()}},
		{"record with nullable any field, tagged null inner", `{"type":"record","name":"r","fields":[{"name":"x","type":["null","int"]}]}`, `{"x":{"null":null}}`, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic: %v", r)
				}
			}()
			s := mustParse(t, tc.schema)
			var v any
			if err := s.DecodeJSON([]byte(tc.src), &v, tc.opts...); err != nil {
				t.Fatalf("decode: %v", err)
			}
		})
	}
}

// TestMatrix_TaggedUnionsBareNullForNullBranch locks that EncodeJSON emits
// bare `null` for the null branch under TaggedUnions. That matches the doc
// commitment, Java's JsonEncoder.writeIndex, and the Avro JSON spec's
// bare-null union form. Without it, appendAvroJSONUnion's four cfg.tagged
// sites wrap any branch including null, producing {"null":null}. Meanwhile
// the entry early-null, reached when the entry peel converts a nil
// Pointer/Interface to invalid, emits bare "null" regardless. Two paths, same
// conceptual input, different output. The structural fix is
// appendUnionBranch, which centralizes `wrap iff cfg.tagged && branch.kind !=
// "null"` so a future dispatcher inherits the special case.
func TestMatrix_TaggedUnionsBareNullForNullBranch(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		value  any
	}{
		// Nil Pointer / Interface: we reach the entry early-null path
		// via the peel loop at appendAvroJSON:189-197. This leg pins
		// that path's bare-null emission.
		{"nil ptr against [null,bytes]", `["null","bytes"]`, (*[]byte)(nil)},
		{"nil ptr against [null,int]", `["null","int"]`, (*int)(nil)},
		{"any holding nil ptr against [null,int]", `["null","int"]`, any((*int)(nil))},

		// Nil Slice / Map / Chan / Func: we reach appendAvroJSONUnion's
		// nil-first dispatch. Without bare-null emission this site
		// wraps null in {"null":null} under TaggedUnions.
		{"nil slice against [null,bytes]", `["null","bytes"]`, []byte(nil)},
		{"nil slice against [null,int,bytes]", `["null","int","bytes"]`, []byte(nil)},
		{"nil map against [null,{type:map,values:int}]", `["null",{"type":"map","values":"int"}]`, map[string]int(nil)},

		// Try-each null branch, reached from a non-nil shape that fails
		// every other branch: we must emit bare null even though the
		// non-null branches would have been wrapped.
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := mustParse(t, tc.schema)
			got := mustEncodeJSON(t, s, tc.value, TaggedUnions())
			if string(got) != "null" {
				t.Errorf("got %s, want null (TaggedUnions doc: \"wraps non-null union values\")", got)
			}
			// Round-trip: the decoder must accept bare null whatever
			// the TaggedUnions setting, so what we emit stays valid.
			var back any
			if err := s.DecodeJSON(got, &back, TaggedUnions()); err != nil {
				t.Fatalf("DecodeJSON round-trip: %v", err)
			}
			if back != nil {
				t.Errorf("DecodeJSON of %s with TaggedUnions: got %T %v, want nil", got, back, back)
			}
		})
	}
}

func TestDecodeJSONInvalidUnion(t *testing.T) {
	s := mustParse(t, `["null","string"]`)
	// Wrong branch name.
	var v any
	if err := s.DecodeJSON([]byte(`{"int":42}`), &v); err == nil {
		t.Fatal("expected error for unknown union branch")
	}
}

func TestAvroJSONNamespacedUnionBranch(t *testing.T) {
	schema := `["null",{"type":"enum","name":"Status","namespace":"com.example","symbols":["ACTIVE","DELETED"]}]`
	s := mustParse(t, schema)

	// With TaggedUnions, the fully qualified name.
	encoded := mustEncodeJSON(t, s, "ACTIVE", TaggedUnions())
	want := `{"com.example.Status":"ACTIVE"}`
	if string(encoded) != want {
		t.Errorf("got %s, want %s", encoded, want)
	}

	var got any
	mustDecodeJSON(t, s, encoded, &got)
	if got != "ACTIVE" {
		t.Errorf("got %v, want ACTIVE", got)
	}
}

func TestAvroJSONNestedUnionRecord(t *testing.T) {
	// Three-level nested record with union fields (like goavro's LongList test).
	schema := nodeRecursiveSchema
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}

	data := map[string]any{
		"value": int32(1),
		"next": map[string]any{
			"value": int32(2),
			"next": map[string]any{
				"value": int32(3),
				"next":  nil,
			},
		},
	}
	// Tagged: Node wrapping at each level.
	encoded, err := s.EncodeJSON(data, TaggedUnions())
	if err != nil {
		t.Fatalf("EncodeJSON(TaggedUnions()): %v", err)
	}
	var parsed any
	json.Unmarshal(encoded, &parsed)
	m := parsed.(map[string]any)
	next := m["next"].(map[string]any)
	if _, ok := next["Node"]; !ok {
		t.Errorf("expected Node key in tagged union, got: %s", encoded)
	}
	var got any
	if err := s.DecodeJSON(encoded, &got); err != nil {
		t.Fatalf("DecodeJSON tagged: %v", err)
	}
	gm := got.(map[string]any)
	if gm["value"] != int32(1) {
		t.Errorf("value: got %v", gm["value"])
	}

	// Bare: nested records, no wrapping.
	bare, err := s.EncodeJSON(data)
	if err != nil {
		t.Fatalf("EncodeJSON(bare): %v", err)
	}
	var got2 any
	if err := s.DecodeJSON(bare, &got2); err != nil {
		t.Fatalf("DecodeJSON bare: %v", err)
	}
	gm2 := got2.(map[string]any)
	if gm2["value"] != int32(1) {
		t.Errorf("value: got %v", gm2["value"])
	}
}

func TestAvroJSONBytesEdgeCases(t *testing.T) {
	s := mustParse(t, `"bytes"`)

	tests := []struct {
		name  string
		input []byte
	}{
		{"empty", []byte{}},
		{"ascii", []byte("hello")},
		{"quote", []byte(`a"b`)},
		{"backslash", []byte(`a\b`)},
		{"control", []byte{0x00, 0x01, 0x0A}},
		{"high bytes", []byte{0x80, 0xFF, 0xFE}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := mustEncodeJSON(t, s, tt.input)
			var got any
			mustDecodeJSON(t, s, encoded, &got)
			if !reflect.DeepEqual(got.([]byte), tt.input) {
				t.Errorf("got %v, want %v", got, tt.input)
			}
		})
	}
}

func TestAvroJSONArrayOfUnions(t *testing.T) {
	schema := `{"type":"array","items":["null","string","int"]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}

	// We first check that binary encoding works.
	data := []any{"hello", int32(42)}
	binary, err := s.Encode(data)
	if err != nil {
		t.Fatalf("Encode binary: %v", err)
	}
	t.Logf("binary: %v", binary)

	// Bare (default): unwrapped values.
	encoded, err := s.EncodeJSON(data)
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	want := `["hello",42]`
	if string(encoded) != want {
		t.Errorf("got %s, want %s", encoded, want)
	}

	// Decode back from bare.
	var got any
	if err := s.DecodeJSON(encoded, &got); err != nil {
		t.Fatalf("DecodeJSON bare: %v", err)
	}
	arr := got.([]any)
	if len(arr) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(arr))
	}

	// Tagged: wrapped values.
	tagged, err := s.EncodeJSON(data, TaggedUnions())
	if err != nil {
		t.Fatalf("EncodeJSON(TaggedUnions()): %v", err)
	}
	wantTagged := `[{"string":"hello"},{"int":42}]`
	if string(tagged) != wantTagged {
		t.Errorf("got %s, want %s", tagged, wantTagged)
	}
}

func TestAvroJSONArrayOfUnionsWithNull(t *testing.T) {
	schema := `{"type":"array","items":["null","string"]}`
	s := mustParse(t, schema)
	data := []any{nil, "hello", nil}
	encoded := mustEncodeJSON(t, s, data)
	want := `[null,"hello",null]`
	if string(encoded) != want {
		t.Errorf("got %s, want %s", encoded, want)
	}
}

func TestDecodeJSONArrayOfUnionsWithNull(t *testing.T) {
	schema := `{"type":"array","items":["null","string"]}`
	s := mustParse(t, schema)
	input := `[null,{"string":"hello"},null]`
	var got any
	mustDecodeJSON(t, s, []byte(input), &got)
	arr := got.([]any)
	if len(arr) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(arr))
	}
	if arr[0] != nil {
		t.Errorf("arr[0]: got %v, want nil", arr[0])
	}
	if arr[1] != "hello" {
		t.Errorf("arr[1]: got %v, want hello", arr[1])
	}
	if arr[2] != nil {
		t.Errorf("arr[2]: got %v, want nil", arr[2])
	}
}

func TestDecodeJSONFixed(t *testing.T) {
	s := mustParse(t, `{"type":"fixed","name":"F","size":3}`)
	var got any
	mustDecodeJSON(t, s, []byte(`"\u0001\u0002\u0003"`), &got)
	b, ok := got.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", got)
	}
	if len(b) != 3 || b[0] != 1 || b[1] != 2 || b[2] != 3 {
		t.Errorf("got %v, want [1 2 3]", b)
	}
}

func TestDecodeJSONNull(t *testing.T) {
	s := mustParse(t, `"null"`)
	var got any
	mustDecodeJSON(t, s, []byte(`null`), &got)
	if got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

func TestDecodeJSONMapMultipleKeys(t *testing.T) {
	s := mustParse(t, `{"type":"map","values":"int"}`)
	var got any
	mustDecodeJSON(t, s, []byte(`{"a":1,"b":2,"c":3}`), &got)
	m := got.(map[string]any)
	if len(m) != 3 {
		t.Errorf("expected 3 keys, got %d", len(m))
	}
}

func TestDecodeJSONRecordMissingField(t *testing.T) {
	s := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"a","type":"int","default":0},
		{"name":"b","type":"string"}
	]}`)
	// "a" is missing from the JSON; it has a default so Encode fills it.
	var got any
	mustDecodeJSON(t, s, []byte(`{"b":"hello"}`), &got)
	m := got.(map[string]any)
	if m["b"] != "hello" {
		t.Errorf("b: got %v", m["b"])
	}
}

func TestDecodeJSONUnionNull(t *testing.T) {
	s := mustParse(t, `["null","string"]`)
	var got any
	mustDecodeJSON(t, s, []byte(`null`), &got)
	if got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

func TestEncodeJSONMapMultipleEntries(t *testing.T) {
	s := mustParse(t, `{"type":"map","values":"int"}`)
	data := map[string]any{"a": int32(1), "b": int32(2)}
	encoded := mustEncodeJSON(t, s, data)
	// Verify it's valid JSON with 2 entries.
	var parsed map[string]any
	if err := json.Unmarshal(encoded, &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if len(parsed) != 2 {
		t.Errorf("expected 2 entries, got %d", len(parsed))
	}
}

func TestEncodeJSONNegativeInfinity(t *testing.T) {
	s := mustParse(t, `"double"`)
	encoded := mustEncodeJSON(t, s, math.Inf(-1))
	if string(encoded) != `"-Infinity"` {
		t.Errorf("got %s, want \"-Infinity\"", encoded)
	}
}

func TestDecodeJSONTypeErrors(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		input  string
	}{
		{"bool expects bool", `"boolean"`, `42`},
		{"bytes expects string", `"bytes"`, `42`},
		{"fixed expects string", `{"type":"fixed","name":"F","size":2}`, `42`},
		{"array expects array", `{"type":"array","items":"int"}`, `42`},
		{"map expects object", `{"type":"map","values":"int"}`, `42`},
		{"record expects object", `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`, `42`},
		{"union expects object", `["null","string"]`, `42`},
		{"union wrong key count", `["null","string"]`, `{"a":1,"b":2}`},
		{"union unknown branch", `["null","string"]`, `{"int":42}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustParse(t, tt.schema)
			var got any
			if err := s.DecodeJSON([]byte(tt.input), &got); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestAppendAvroJSONTypeErrors(t *testing.T) {
	// We drive appendAvroJSON's error paths directly.
	tests := []struct {
		name string
		kind string
		val  any
	}{
		{"bool wrong type", "boolean", 42},
		{"int wrong type", "int", "not int"},
		{"long wrong type", "long", "not long"},
		{"float wrong type", "float", "not float"},
		{"double wrong type", "double", "not double"},
		{"string wrong type", "string", 42},
		{"bytes wrong type", "bytes", 42},
		{"enum wrong type", "enum", 42},
		{"array wrong type", "array", 42},
		{"map wrong type", "map", 42},
		{"record wrong type", "record", 42},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &schemaNode{kind: tt.kind}
			if tt.kind == "array" {
				node.items = &schemaNode{kind: "int"}
			}
			if tt.kind == "map" {
				node.values = &schemaNode{kind: "int"}
			}
			_, err := appendAvroJSON(nil, reflect.ValueOf(tt.val), node, &optConfig{}, nil, 0)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestAppendAvroJSONFixedReflect(t *testing.T) {
	node := &schemaNode{kind: "fixed", size: 3}
	buf, err := appendAvroJSON(nil, reflect.ValueOf([3]byte{1, 2, 3}), node, &optConfig{}, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf) != `"\u0001\u0002\u0003"` {
		t.Errorf("got %s", buf)
	}

	// Non-byte array should error.
	_, err = appendAvroJSON(nil, reflect.ValueOf([3]int{1, 2, 3}), node, &optConfig{}, nil, 0)
	if err == nil {
		t.Fatal("expected error for non-byte array")
	}
}

func TestAppendAvroJSONUnionNoMatch(t *testing.T) {
	node := &schemaNode{
		kind:     "union",
		branches: []*schemaNode{{kind: "null"}, {kind: "string"}},
	}
	_, err := appendAvroJSON(nil, reflect.ValueOf(int32(42)), node, &optConfig{}, nil, 0)
	if err == nil {
		t.Fatal("expected error for unmatched union")
	}
}

func TestAppendAvroJSONUnknownKind(t *testing.T) {
	node := &schemaNode{kind: "bogus"}
	_, err := appendAvroJSON(nil, reflect.ValueOf(42), node, &optConfig{}, nil, 0)
	if err == nil {
		t.Fatal("expected error for unknown kind")
	}
}

func TestEncodeJSONRejectsNonWholeFloat(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		s, _ := Parse(`"int"`)
		_, err := s.EncodeJSON(float64(42.5))
		if err == nil {
			t.Fatal("expected error for non-whole float in int field")
		}
		b, err := s.EncodeJSON(float64(42.0))
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != "42" {
			t.Fatalf("got %s", b)
		}
	})
	t.Run("long", func(t *testing.T) {
		s, _ := Parse(`"long"`)
		_, err := s.EncodeJSON(float64(42.5))
		if err == nil {
			t.Fatal("expected error for non-whole float in long field")
		}
		b, err := s.EncodeJSON(float64(42.0))
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != "42" {
			t.Fatalf("got %s", b)
		}
	})
}

func TestEncodeJSONTaggedUnionMaps(t *testing.T) {
	s, err := Parse(`{"type":"record","name":"T","fields":[
		{"name":"u","type":["null","int","string"]}
	]}`)
	if err != nil {
		t.Fatal(err)
	}

	// Tagged union map should be accepted.
	out, err := s.EncodeJSON(map[string]any{"u": map[string]any{"int": float64(42)}}, TaggedUnions())
	if err != nil {
		t.Fatalf("tagged int: %v", err)
	}
	if string(out) != `{"u":{"int":42}}` {
		t.Fatalf("tagged int: got %s", out)
	}

	out, err = s.EncodeJSON(map[string]any{"u": map[string]any{"string": "hello"}}, TaggedUnions())
	if err != nil {
		t.Fatalf("tagged string: %v", err)
	}
	if string(out) != `{"u":{"string":"hello"}}` {
		t.Fatalf("tagged string: got %s", out)
	}

	// Without TaggedUnions, tagged maps should still be unwrapped.
	out, err = s.EncodeJSON(map[string]any{"u": map[string]any{"int": float64(7)}})
	if err != nil {
		t.Fatalf("tagged bare: %v", err)
	}
	if string(out) != `{"u":7}` {
		t.Fatalf("tagged bare: got %s", out)
	}

	// Wrong branch name should fail.
	_, err = s.EncodeJSON(map[string]any{"u": map[string]any{"long": float64(42)}})
	if err == nil {
		t.Fatal("expected error for wrong branch name")
	}

	// Tagged map where the key matches a branch name but the value does
	// not match that branch's type. We fall through and try the whole map
	// against other branches (e.g. a map branch), matching Encode.
	s3, err := Parse(`{"type":"record","name":"T","fields":[
		{"name":"u","type":["null","int",{"type":"map","values":"string"}]}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	// {"int": "not_a_number"}: key "int" matches the int branch, but
	// "not_a_number" fails for int, so the whole map then matches the
	// map<string> branch.
	out, err = s3.EncodeJSON(map[string]any{"u": map[string]any{"int": "not_a_number"}}, TaggedUnions())
	if err != nil {
		t.Fatalf("fallthrough to map branch: %v", err)
	}
	if string(out) != `{"u":{"map":{"int":"not_a_number"}}}` {
		t.Fatalf("fallthrough to map branch: got %s", out)
	}

	// Logical type branch names (goavro convention).
	s2, err := Parse(`{"type":"record","name":"T","fields":[
		{"name":"t","type":["null",{"type":"int","logicalType":"time-millis"}]}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	out, err = s2.EncodeJSON(map[string]any{"t": map[string]any{"int.time-millis": float64(35245000)}}, TaggedUnions(), TagLogicalTypes())
	if err != nil {
		t.Fatalf("logical tag: %v", err)
	}
	if string(out) != `{"t":{"int.time-millis":35245000}}` {
		t.Fatalf("logical tag: got %s", out)
	}
}

func TestSchemaNodeErrors(t *testing.T) {
	// Schema() with invalid node.
	n := &SchemaNode{Type: "record"} // missing name
	_, err := n.Schema()
	if err == nil {
		t.Fatal("expected error for record without name")
	}

	// Root() on a schema: the JSON re-parse cannot actually fail, since
	// Parse always sets Schema.full to valid JSON. We check Root on every
	// schema type.
	for _, schema := range []string{
		`"null"`,
		`"int"`,
		`["null","string"]`,
		`{"type":"array","items":"int"}`,
		`{"type":"map","values":"string"}`,
	} {
		s := mustParse(t, schema)
		_ = s.Root() // should not panic
	}
}

func TestEncodeJSONStruct(t *testing.T) {
	type Record struct {
		Name   string   `avro:"name"`
		Age    int32    `avro:"age"`
		Score  float64  `avro:"score"`
		Active bool     `avro:"active"`
		Tags   []string `avro:"tags"`
		Inner  Inner    `avro:"inner"`
		Email  *string  `avro:"email"`
	}
	s := mustParse(t, `{
		"type":"record","name":"Record",
		"fields":[
			{"name":"name","type":"string"},
			{"name":"age","type":"int"},
			{"name":"score","type":"double"},
			{"name":"active","type":"boolean"},
			{"name":"tags","type":{"type":"array","items":"string"}},
			{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}},
			{"name":"email","type":["null","string"]}
		]
	}`)
	email := "a@b.com"
	r := Record{
		Name:   "Alice",
		Age:    30,
		Score:  98.6,
		Active: true,
		Tags:   []string{"go"},
		Inner:  Inner{X: 42},
		Email:  &email,
	}
	encoded := mustEncodeJSON(t, s, &r)
	// Verify it's valid JSON and round-trips.
	var decoded any
	mustDecodeJSON(t, s, encoded, &decoded)
	m := decoded.(map[string]any)
	if m["name"] != "Alice" {
		t.Errorf("name: got %v", m["name"])
	}
	if m["age"] != int32(30) {
		t.Errorf("age: got %v", m["age"])
	}
	if m["email"] != "a@b.com" {
		t.Errorf("email: got %v", m["email"])
	}
}

func TestEncodeJSONStructNilPointer(t *testing.T) {
	s := mustParse(t, recordNameEmailSchema)
	r := Record{Name: "Bob", Email: nil}
	encoded := mustEncodeJSON(t, s, &r)
	var parsed map[string]any
	json.Unmarshal(encoded, &parsed)
	if parsed["email"] != nil {
		t.Errorf("email: got %v, want null", parsed["email"])
	}
}

func TestEncodeJSONTimestamp(t *testing.T) {
	s := mustParse(t, `{"type":"long","logicalType":"timestamp-millis"}`)
	ts := time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)
	encoded := mustEncodeJSON(t, s, ts)
	want := strconv.FormatInt(ts.UnixMilli(), 10)
	if string(encoded) != want {
		t.Errorf("got %s, want %s", encoded, want)
	}
}

func TestEncodeJSONReflectErrors(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		value  any
	}{
		{"bool wrong", `"boolean"`, 42},
		{"int wrong", `"int"`, "nope"},
		{"long wrong", `"long"`, "nope"},
		{"float wrong", `"float"`, "nope"},
		{"double wrong", `"double"`, "nope"},
		{"string wrong", `"string"`, 42},
		{"bytes wrong", `"bytes"`, 42},
		{"fixed wrong", `{"type":"fixed","name":"F","size":2}`, 42},
		{"enum wrong", `{"type":"enum","name":"E","symbols":["A"]}`, 42},
		{"array wrong", `{"type":"array","items":"int"}`, 42},
		{"map wrong", `{"type":"map","values":"int"}`, 42},
		{"record wrong", `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`, 42},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustParse(t, tt.schema)
			if _, err := s.EncodeJSON(tt.value); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestEncodeJSONTimestampVariants(t *testing.T) {
	ts := time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		schema string
		want   int64
	}{
		{"micros", `{"type":"long","logicalType":"timestamp-micros"}`, ts.UnixMicro()},
		{"nanos", `{"type":"long","logicalType":"timestamp-nanos"}`, ts.Unix()*1e9 + int64(ts.Nanosecond())},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustParse(t, tt.schema)
			encoded := mustEncodeJSON(t, s, ts)
			if string(encoded) != strconv.FormatInt(tt.want, 10) {
				t.Errorf("got %s, want %d", encoded, tt.want)
			}
		})
	}
}

func TestEncodeJSONUintValues(t *testing.T) {
	s, _ := Parse(`"int"`)
	encoded := mustEncodeJSON(t, s, uint16(42))
	if string(encoded) != "42" {
		t.Errorf("got %s, want 42", encoded)
	}

	s2, _ := Parse(`"long"`)
	encoded = mustEncodeJSON(t, s2, uint32(100))
	if string(encoded) != "100" {
		t.Errorf("got %s, want 100", encoded)
	}
}

func TestEncodeJSONFixedAsSlice(t *testing.T) {
	s := mustParse(t, `{"type":"fixed","name":"F","size":3}`)
	encoded := mustEncodeJSON(t, s, []byte{1, 2, 3})
	if string(encoded) != `"\u0001\u0002\u0003"` {
		t.Errorf("got %s", encoded)
	}
}

func TestEncodeJSONMissingMapKey(t *testing.T) {
	s := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"a","type":"int","default":0},
		{"name":"b","type":"string"}
	]}`)
	// "a" is missing from the map, so it encodes as its default.
	encoded := mustEncodeJSON(t, s, map[string]any{"b": "hello"})
	var parsed map[string]any
	json.Unmarshal(encoded, &parsed)
	if parsed["b"] != "hello" {
		t.Errorf("b: got %v", parsed["b"])
	}
}

func TestEncodeJSONNilInUnion(t *testing.T) {
	type R struct {
		V *string `avro:"v"`
	}
	s := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"v","type":["null","string"]}]}`)
	encoded := mustEncodeJSON(t, s, &R{V: nil})
	var parsed map[string]any
	json.Unmarshal(encoded, &parsed)
	if parsed["v"] != nil {
		t.Errorf("v: got %v, want null", parsed["v"])
	}
}

func TestAvroJSONBinaryRoundTrip(t *testing.T) {
	// We encode to Avro JSON, decode to any, encode to binary, decode to
	// any, then check the values match.
	schema := `{
		"type":"record","name":"Event",
		"fields":[
			{"name":"id","type":"string"},
			{"name":"ts","type":"long"},
			{"name":"data","type":"bytes"},
			{"name":"status","type":{"type":"enum","name":"Status","symbols":["A","B"]}},
			{"name":"extra","type":["null","string","int"]}
		]
	}`
	s := mustParse(t, schema)

	original := map[string]any{
		"id":     "abc",
		"ts":     int64(1000),
		"data":   []byte{0x01, 0x02},
		"status": "A",
		"extra":  "hello",
	}

	// Path 1: binary encode, then binary decode.
	binary := mustEncode(t, s, original)
	var fromBinary any
	mustDecode(t, s, binary, &fromBinary)

	// Path 2: Avro JSON encode, then Avro JSON decode.
	jsonBytes := mustEncodeJSON(t, s, original)
	var fromJSON any
	mustDecodeJSON(t, s, jsonBytes, &fromJSON)

	// Both paths must agree.
	mb := fromBinary.(map[string]any)
	mj := fromJSON.(map[string]any)
	if mb["id"] != mj["id"] {
		t.Errorf("id mismatch: binary=%v json=%v", mb["id"], mj["id"])
	}
	if mb["ts"] != mj["ts"] {
		t.Errorf("ts mismatch: binary=%v json=%v", mb["ts"], mj["ts"])
	}
	if mb["status"] != mj["status"] {
		t.Errorf("status mismatch: binary=%v json=%v", mb["status"], mj["status"])
	}
	if mb["extra"] != mj["extra"] {
		t.Errorf("extra mismatch: binary=%v json=%v", mb["extra"], mj["extra"])
	}
}

func TestAvroJSONStructRoundTrip(t *testing.T) {
	type Record struct {
		Name  string  `avro:"name"`
		Age   int32   `avro:"age"`
		Email *string `avro:"email"`
	}
	s := mustParse(t, `{"type":"record","name":"Record","fields":[
		{"name":"name","type":"string"},
		{"name":"age","type":"int"},
		{"name":"email","type":["null","string"]}
	]}`)
	email := "a@b.com"
	original := Record{Name: "Alice", Age: 30, Email: &email}

	// Struct to Avro JSON and back to struct.
	jsonBytes := mustEncodeJSON(t, s, &original)
	var got Record
	mustDecodeJSON(t, s, jsonBytes, &got)
	if got.Name != original.Name || got.Age != original.Age {
		t.Errorf("got %+v, want %+v", got, original)
	}
	if got.Email == nil || *got.Email != *original.Email {
		t.Errorf("email: got %v, want %v", got.Email, original.Email)
	}
}

func TestAvroJSONUnionArrayNilRoundTrip(t *testing.T) {
	// Array of nullable unions with nil elements.
	s := mustParse(t, `{"type":"array","items":["null","string"]}`)
	original := []any{nil, "hello", nil, "world"}
	jsonBytes := mustEncodeJSON(t, s, original)
	var got any
	mustDecodeJSON(t, s, jsonBytes, &got)
	arr := got.([]any)
	if len(arr) != 4 {
		t.Fatalf("expected 4 elements, got %d", len(arr))
	}
	if arr[0] != nil || arr[1] != "hello" || arr[2] != nil || arr[3] != "world" {
		t.Errorf("got %v", arr)
	}
}

func TestDecodeJSONIntOverflow(t *testing.T) {
	s, _ := Parse(`"int"`)
	// 3 billion exceeds int32 max.
	var got any
	err := s.DecodeJSON([]byte(`3000000000`), &got)
	if err == nil {
		t.Fatal("expected error for int32 overflow")
	}
}

func TestDecodeJSONBytesHighUnicode(t *testing.T) {
	s, _ := Parse(`"bytes"`)
	// \u0100 exceeds byte range.
	var got any
	err := s.DecodeJSON([]byte(`"\u0100"`), &got)
	if err == nil {
		t.Fatal("expected error for code point > 255")
	}
}

func TestSchemaForAnonymousStruct(t *testing.T) {
	type Outer struct {
		Inner struct{ X int } `avro:"inner"`
	}
	_, err := SchemaFor[Outer]()
	if err == nil {
		t.Fatal("expected error for anonymous struct field")
	}
}

func TestEncodeJSONNil(t *testing.T) {
	s, _ := Parse(`"null"`)
	encoded := mustEncodeJSON(t, s, nil)
	if string(encoded) != "null" {
		t.Errorf("got %s, want null", encoded)
	}
}

func TestDecodeJSONInvalidJSON(t *testing.T) {
	s := mustParse(t, `"int"`)
	var v any
	if err := s.DecodeJSON([]byte(`{not json`), &v); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestEncodeJSONLinkedinFloats(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		value  any
		want   string
	}{
		{"float NaN", `"float"`, float32(math.Float32frombits(0x7fc00000)), `null`},
		{"float +Inf", `"float"`, float32(math.Inf(1)), `1e999`},
		{"float -Inf", `"float"`, float32(math.Inf(-1)), `-1e999`},
		{"double NaN", `"double"`, math.NaN(), `null`},
		{"double +Inf", `"double"`, math.Inf(1), `1e999`},
		{"double -Inf", `"double"`, math.Inf(-1), `-1e999`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustParse(t, tt.schema)
			got := mustEncodeJSON(t, s, tt.value, LinkedinFloats())
			if string(got) != tt.want {
				t.Errorf("got %s, want %s", got, tt.want)
			}
		})
	}
}

// LinkedinFloats encodes NaN as a bare JSON null. Inside a bare (untagged)
// union, the union's null branch claims that bare null, or the union rejects
// it when there is no null branch. Either way that happens before the float
// branch's null-means-NaN rule runs, so a union NaN does not round-trip. That
// is the inherent ambiguity of the null-for-NaN convention when null is also
// a structural union value, and TaggedUnions disambiguates it. ±Inf encodes
// as the number token ±1e999 and round-trips in a bare union regardless.
// This pins the contract documented on LinkedinFloats.
func TestMatrix_LinkedinFloatsNaNUnionAmbiguity(t *testing.T) {
	nan := float32(math.Float32frombits(0x7fc00000))

	// Bare union *with* a null branch: NaN encodes as null and decodes to the
	// null branch (nil), not back to NaN.
	t.Run("bare union with null branch loses NaN to null branch", func(t *testing.T) {
		s := MustParse(`["null","float"]`)
		js := mustAppendEncodeJSON(t, s, nil, nan, LinkedinFloats())
		if string(js) != `null` {
			t.Fatalf("EncodeJSON NaN: got %s, want null", js)
		}
		var out any
		mustDecodeJSON(t, s, js, &out, LinkedinFloats())
		if out != nil {
			t.Fatalf("bare-union NaN: got %#v, want nil (null branch)", out)
		}
	})

	// Bare union *without* a null branch: NaN still encodes as null, which
	// the decoder rejects: there is no null branch to receive it.
	t.Run("bare union without null branch rejects null on decode", func(t *testing.T) {
		s := MustParse(`["float","string"]`)
		js := mustAppendEncodeJSON(t, s, nil, nan, LinkedinFloats())
		if string(js) != `null` {
			t.Fatalf("EncodeJSON NaN: got %s, want null", js)
		}
		var out any
		if err := s.DecodeJSON(js, &out, LinkedinFloats()); err == nil {
			t.Fatal("DecodeJSON of null into null-less union: want error, got nil")
		}
	})

	// TaggedUnions disambiguates: {"float":null} routes the null to the
	// float branch, which reapplies the null-means-NaN rule, so NaN survives.
	t.Run("tagged union round-trips NaN", func(t *testing.T) {
		s := MustParse(`["null","float"]`)
		js := mustAppendEncodeJSON(t, s, nil, nan, LinkedinFloats(), TaggedUnions())
		if string(js) != `{"float":null}` {
			t.Fatalf("EncodeJSON NaN tagged: got %s, want {\"float\":null}", js)
		}
		var out any
		mustDecodeJSON(t, s, js, &out, LinkedinFloats(), TaggedUnions())
		m, ok := out.(map[string]any)
		if !ok {
			t.Fatalf("tagged decode: got %T, want map[string]any", out)
		}
		if f, ok := m["float"].(float32); !ok || !math.IsNaN(float64(f)) {
			t.Fatalf("tagged decode: got %#v, want float branch NaN", m)
		}
	})

	// ±Inf are number tokens (±1e999), not null, so they round-trip in a
	// bare union under LinkedinFloats.
	t.Run("bare union round-trips +Inf", func(t *testing.T) {
		s := MustParse(`["null","float"]`)
		js := mustAppendEncodeJSON(t, s, nil, float32(math.Inf(1)), LinkedinFloats())
		if string(js) != `1e999` {
			t.Fatalf("EncodeJSON +Inf: got %s, want 1e999", js)
		}
		var out any
		mustDecodeJSON(t, s, js, &out, LinkedinFloats())
		if f, ok := out.(float32); !ok || !math.IsInf(float64(f), 1) {
			t.Fatalf("bare-union +Inf: got %#v, want float32(+Inf)", out)
		}
	})
}

func TestEncodeJSONTaggedUnions(t *testing.T) {
	s := mustParse(t, `["null","string","int"]`)
	// Tagged: wrapped.
	got := mustEncodeJSON(t, s, "hello", TaggedUnions())
	if string(got) != `{"string":"hello"}` {
		t.Errorf("tagged: got %s", got)
	}
	// Bare (default): unwrapped.
	got = mustEncodeJSON(t, s, "hello")
	if string(got) != `"hello"` {
		t.Errorf("bare: got %s", got)
	}
}

func TestDecodeTaggedUnions(t *testing.T) {
	schema := `{"type":"record","name":"R","fields":[
		{"name":"v","type":["null","string","int"]}
	]}`
	s := mustParse(t, schema)
	bin := mustEncode(t, s, map[string]any{"v": "hello"})

	// Without TaggedUnions: bare.
	var bare any
	mustDecode(t, s, bin, &bare)
	m := bare.(map[string]any)
	if m["v"] != "hello" {
		t.Errorf("bare: got %v", m["v"])
	}

	// With TaggedUnions: wrapped.
	var tagged any
	mustDecode(t, s, bin, &tagged, TaggedUnions())
	m = tagged.(map[string]any)
	wrapper, ok := m["v"].(map[string]any)
	if !ok {
		t.Fatalf("tagged: expected map wrapper, got %T: %v", m["v"], m["v"])
	}
	if wrapper["string"] != "hello" {
		t.Errorf("tagged: got %v", wrapper)
	}
}

func TestDecodeTaggedUnionsComplex(t *testing.T) {
	schema := `{"type":"record","name":"R","fields":[
		{"name":"u_bool","type":["null","boolean"]},
		{"name":"u_int","type":["null","int"]},
		{"name":"u_long","type":["null","long"]},
		{"name":"u_float","type":["null","float"]},
		{"name":"u_double","type":["null","double"]},
		{"name":"u_string","type":["null","string"]},
		{"name":"u_bytes","type":["null","bytes"]},
		{"name":"u_null","type":["null","string"]},
		{"name":"arr","type":{"type":"array","items":["null","string"]}},
		{"name":"m","type":{"type":"map","values":["null","int"]}}
	]}`
	s := mustParse(t, schema)
	input := map[string]any{
		"u_bool":   true,
		"u_int":    int32(42),
		"u_long":   int64(100),
		"u_float":  float32(1.5),
		"u_double": float64(3.14),
		"u_string": "hello",
		"u_bytes":  []byte{0x01, 0x02},
		"u_null":   nil,
		"arr":      []any{nil, "a"},
		"m":        map[string]any{"k": int32(1)},
	}
	bin := mustEncode(t, s, input)
	var got any
	mustDecode(t, s, bin, &got, TaggedUnions())
	m := got.(map[string]any)

	// Check each union is wrapped.
	check := func(field, branch string) {
		t.Helper()
		wrapper, ok := m[field].(map[string]any)
		if !ok {
			if m[field] == nil {
				return // null union values stay nil
			}
			t.Errorf("%s: expected map wrapper, got %T: %v", field, m[field], m[field])
			return
		}
		if _, ok := wrapper[branch]; !ok {
			t.Errorf("%s: expected branch %q, got keys %v", field, branch, wrapper)
		}
	}
	check("u_bool", "boolean")
	check("u_int", "int")
	check("u_long", "long")
	check("u_float", "float")
	check("u_double", "double")
	check("u_string", "string")
	check("u_bytes", "bytes")
	if m["u_null"] != nil {
		t.Errorf("u_null: expected nil, got %v", m["u_null"])
	}

	// Array items should be wrapped.
	arr := m["arr"].([]any)
	if arr[0] != nil {
		t.Errorf("arr[0]: expected nil, got %v", arr[0])
	}
	arrItem, ok := arr[1].(map[string]any)
	if !ok {
		t.Fatalf("arr[1]: expected map, got %T", arr[1])
	}
	if arrItem["string"] != "hello" && arrItem["string"] != "a" {
		t.Errorf("arr[1]: got %v", arrItem)
	}

	// Map values should be wrapped.
	mv := m["m"].(map[string]any)
	kv, ok := mv["k"].(map[string]any)
	if !ok {
		t.Fatalf("m[k]: expected map, got %T", mv["k"])
	}
	if _, ok := kv["int"]; !ok {
		t.Errorf("m[k]: expected int branch, got %v", kv)
	}
}

func TestDecodeTaggedUnionsNullAtRoot(t *testing.T) {
	s := mustParse(t, `["null","string"]`)
	bin := mustEncode(t, s, nil)
	var got any
	mustDecode(t, s, bin, &got, TaggedUnions())
	if got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestDecodeTaggedUnionsWithLogicalNames(t *testing.T) {
	schema := `{"type":"record","name":"R","fields":[
		{"name":"ts","type":["null",{"type":"long","logicalType":"timestamp-millis"}]}
	]}`
	s := mustParse(t, schema)
	now := time.UnixMilli(1687221496000).UTC()
	bin := mustEncode(t, s, map[string]any{"ts": now})

	// TaggedUnions only: branch name is "long".
	var std any
	mustDecode(t, s, bin, &std, TaggedUnions())
	m := std.(map[string]any)
	wrapper := m["ts"].(map[string]any)
	if _, ok := wrapper["long"]; !ok {
		t.Errorf("expected 'long' key, got %v", wrapper)
	}

	// TaggedUnions + TagLogicalTypes: branch name is "long.timestamp-millis".
	var logical any
	mustDecode(t, s, bin, &logical, TaggedUnions(), TagLogicalTypes())
	m = logical.(map[string]any)
	wrapper = m["ts"].(map[string]any)
	if _, ok := wrapper["long.timestamp-millis"]; !ok {
		t.Errorf("expected 'long.timestamp-millis' key, got %v", wrapper)
	}
}

func TestEncodeJSONTaggedUnionsWithLogicalNames(t *testing.T) {
	s := mustParse(t, `["null",{"type":"long","logicalType":"timestamp-millis"}]`)
	now := time.UnixMilli(1687221496000).UTC()

	// Without TagLogicalTypes: "long".
	got := mustEncodeJSON(t, s, now, TaggedUnions())
	if string(got) != `{"long":1687221496000}` {
		t.Errorf("got %s", got)
	}

	// With TagLogicalTypes: "long.timestamp-millis".
	got = mustEncodeJSON(t, s, now, TaggedUnions(), TagLogicalTypes())
	if string(got) != `{"long.timestamp-millis":1687221496000}` {
		t.Errorf("got %s", got)
	}
}

func TestDecodeJSONTaggedUnions(t *testing.T) {
	s := mustParse(t, `["null","string"]`)
	var bare any
	mustDecodeJSON(t, s, []byte(`"hello"`), &bare)
	if bare != "hello" {
		t.Errorf("bare: got %v", bare)
	}

	var tagged any
	mustDecodeJSON(t, s, []byte(`"hello"`), &tagged, TaggedUnions())
	wrapper, ok := tagged.(map[string]any)
	if !ok {
		t.Fatalf("tagged: expected map, got %T: %v", tagged, tagged)
	}
	if wrapper["string"] != "hello" {
		t.Errorf("tagged: got %v", wrapper)
	}
}

func TestDecodeJSONNaNInfRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		input  string
	}{
		{"float NaN string", `"float"`, `"NaN"`},
		{"float Inf string", `"float"`, `"Infinity"`},
		{"float -Inf string", `"float"`, `"-Infinity"`},
		{"float INF string", `"float"`, `"INF"`},
		{"float -INF string", `"float"`, `"-INF"`},
		{"double NaN string", `"double"`, `"NaN"`},
		{"double Inf string", `"double"`, `"Infinity"`},
		{"double -Inf string", `"double"`, `"-Infinity"`},
		{"float null → NaN", `"float"`, `null`},
		{"double null → NaN", `"double"`, `null`},
		// We reject lowercase quoted "nan" to match Java/fastavro/
		// goavro, which all exact-match "NaN"; see
		// TestMatrix_JSONDecodeBareNaNInfinityCasingParity.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustParse(t, tt.schema)
			var got any
			mustDecodeJSON(t, s, []byte(tt.input), &got)
			switch v := got.(type) {
			case float32:
				if tt.input == `null` || tt.input == `"NaN"` {
					if !math.IsNaN(float64(v)) {
						t.Errorf("expected NaN, got %v", v)
					}
				} else if !math.IsInf(float64(v), 0) {
					t.Errorf("expected Inf, got %v", v)
				}
			case float64:
				if tt.input == `null` || tt.input == `"NaN"` {
					if !math.IsNaN(v) {
						t.Errorf("expected NaN, got %v", v)
					}
				} else if !math.IsInf(v, 0) {
					t.Errorf("expected Inf, got %v", v)
				}
			default:
				t.Fatalf("unexpected type %T", got)
			}
		})
	}
}

func TestDecodeJSONBadFloatString(t *testing.T) {
	s := mustParse(t, `"float"`)
	var v any
	if err := s.DecodeJSON([]byte(`"bogus"`), &v); err == nil {
		t.Fatal("expected error for unknown float string")
	}
}

func TestDecodeJSONBadDoubleString(t *testing.T) {
	s := mustParse(t, `"double"`)
	var v any
	if err := s.DecodeJSON([]byte(`"bogus"`), &v); err == nil {
		t.Fatal("expected error for unknown double string")
	}
}

func TestEncodeJSONBareUnionRecord(t *testing.T) {
	schema := `{
		"type":"record","name":"R",
		"fields":[{"name":"v","type":["null",{"type":"record","name":"Inner","fields":[
			{"name":"x","type":"int"}
		]}]}]
	}`
	s := mustParse(t, schema)
	data := map[string]any{"v": map[string]any{"x": int32(42)}}
	// Bare: record without type wrapper.
	bare := mustEncodeJSON(t, s, data)
	// Decode back from bare.
	var got any
	if err := s.DecodeJSON(bare, &got); err != nil {
		t.Fatalf("DecodeJSON bare: %v", err)
	}
	m := got.(map[string]any)
	inner := m["v"].(map[string]any)
	if inner["x"] != int32(42) {
		t.Errorf("x: got %v", inner["x"])
	}
}

func TestEncodeJSONRecordMissingRequiredField(t *testing.T) {
	schema := recABSchema
	s := mustParse(t, schema)
	// Missing required field "b".
	_, err := s.EncodeJSON(map[string]any{"a": int32(1)})
	if err == nil {
		t.Fatal("expected error for missing required field")
	}
}

func TestEncodeJSONRecordOptionalField(t *testing.T) {
	schema := `{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string","default":"hi"}
	]}`
	s := mustParse(t, schema)
	// Missing field "b" has a default, so encode succeeds with it.
	got := mustEncodeJSON(t, s, map[string]any{"a": int32(1)})
	if string(got) != `{"a":1,"b":"hi"}` {
		t.Errorf("got %s", got)
	}
}

func TestEncodeJSONBytesFromString(t *testing.T) {
	s := mustParse(t, `"bytes"`)
	got := mustEncodeJSON(t, s, "hello")
	if string(got) != `"hello"` {
		t.Errorf("got %s", got)
	}
}

func TestBareUnionMultiRecordRoundTrip(t *testing.T) {
	schema := `["null",
		{"type":"record","name":"Foo","fields":[{"name":"x","type":"int"}]},
		{"type":"record","name":"Bar","fields":[{"name":"y","type":"string"}]}
	]`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}

	// The tagged form recovers any branch deterministically. It is the Avro
	// JSON spec form for a non-null union. It is also the only JSON form that
	// round-trips a multi-record union; Java/fastavro/goavro require it.
	var v1 any
	if err := s.DecodeJSON([]byte(`{"Bar":{"y":"hello"}}`), &v1); err != nil {
		t.Fatalf("tagged DecodeJSON: %v", err)
	}

	// Binary carries an explicit branch index, so a Bar value round-trips on the
	// binary wire regardless of declaration order.
	bar := map[string]any{"y": "hello"}
	bin, err := s.Encode(bar)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var native any
	mustDecode(t, s, bin, &native)
	// Tagged JSON round-trips the recovered branch.
	jb, err := s.EncodeJSON(native, TaggedUnions())
	if err != nil {
		t.Fatalf("EncodeJSON tagged: %v", err)
	}
	var rt any
	if err := s.DecodeJSON(jb, &rt); err != nil {
		t.Fatalf("DecodeJSON tagged: %v", err)
	}

	// Bare JSON of a multi-record union commits to the first
	// declaration-order branch whose structure the object matches. It does
	// NOT backtrack to a later record branch. That backtracking is the
	// branch-guessing the Avro JSON spec and Java/fastavro/goavro avoid;
	// they require the tagged form. It is also 2^depth for recursive unions,
	// see TestRegression_BareUnionJSONNoExponentialBacktrack. So a bare
	// object matching only the second branch (Bar's {"y":...}) fails against
	// the first branch, since Foo needs "x". The tagged form recovers Bar.
	var barBare any
	if err := s.DecodeJSON([]byte(`{"y":"hello"}`), &barBare); err == nil {
		t.Fatal(`bare {"y":"hello"} must commit to the first record branch Foo and fail (missing "x"); tagged form required for Bar`)
	}
	// A bare object matching the first branch (Foo) decodes fine.
	var foo any
	if err := s.DecodeJSON([]byte(`{"x":5}`), &foo); err != nil {
		t.Fatalf("bare first-branch decode: %v", err)
	}

	// Direct Encode of a bare map still matches branches by structure on the
	// binary encode side, unaffected by the JSON-decode commit-to-first.
	if _, err := s.Encode(map[string]any{"y": "hello"}); err != nil {
		t.Fatalf("direct Encode of Bar map: %v", err)
	}
}

func TestDecodeJSONBareUnionFallthrough(t *testing.T) {
	// Union ["null","string"] with input {"int":42}, not a valid branch. This
	// must still error even with bare matching.
	s := mustParse(t, `["null","string"]`)
	var v any
	if err := s.DecodeJSON([]byte(`{"int":42}`), &v); err == nil {
		t.Fatal("expected error for unmatched bare union value")
	}
}

func TestDecodeJSONTaggedFloatNull(t *testing.T) {
	// {"float": null} in a ["null","float"] union: the null is inside the
	// float branch, which decodes as NaN (goavro convention).
	s := mustParse(t, `["null","float"]`)
	var v any
	mustDecodeJSON(t, s, []byte(`{"float":null}`), &v)
	f, ok := v.(float32)
	if !ok {
		t.Fatalf("expected float32, got %T: %v", v, v)
	}
	if !math.IsNaN(float64(f)) {
		t.Fatalf("expected NaN, got %v", f)
	}
}

func TestSerStringJsonNumberInUnion(t *testing.T) {
	// json.Number in a ["null","string"] union has no numeric branch, so it
	// errors: string rejects json.Number.
	s, err := Parse(`["null","string"]`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Encode(json.Number("42"))
	if err == nil {
		t.Fatal("expected error: json.Number should not match string branch")
	}

	// json.Number in a ["null","int","string"] union matches int.
	s2, err := Parse(`["null","int","string"]`)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s2.Encode(json.Number("42"))
	if err != nil {
		t.Fatalf("json.Number should match int branch: %v", err)
	}
	if len(b) == 0 {
		t.Fatal("expected non-empty output")
	}
}

func TestDecodeJSONBareUnionStringVsRecord(t *testing.T) {
	// Union ["null","string",record]: a map matches the record, not string.
	schema := `["null","string",{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}]`
	s := mustParse(t, schema)
	var v any
	mustDecodeJSON(t, s, []byte(`{"x":42}`), &v)
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T: %v", v, v)
	}
	if m["x"] != int32(42) {
		t.Errorf("x: got %v", m["x"])
	}
}

func TestEncodeJSONFixedFromString(t *testing.T) {
	s := mustParse(t, `{"type":"fixed","name":"F","size":5}`)
	got := mustEncodeJSON(t, s, "hello")
	if string(got) != `"hello"` {
		t.Errorf("got %s", got)
	}
}

func TestEncodeJSONLogicalTypeRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		value  any
		want   string
	}{
		{"date", `{"type":"int","logicalType":"date"}`, time.Date(1977, 5, 12, 0, 0, 0, 0, time.UTC), "2688"},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`, time.Duration(35245000) * time.Millisecond, "35245000"},
		{"time-micros", `{"type":"long","logicalType":"time-micros"}`, time.Duration(20192000) * time.Microsecond, "20192000"},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, time.UnixMilli(1687221496000).UTC(), "1687221496000"},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`, time.UnixMicro(1687221496000000).UTC(), "1687221496000000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustParse(t, tt.schema)
			bin := mustEncode(t, s, tt.value)
			var native any
			mustDecode(t, s, bin, &native)
			got := mustEncodeJSON(t, s, native)
			if string(got) != tt.want {
				t.Errorf("got %s, want %s", got, tt.want)
			}
		})
	}
}

func TestEncodeJSONDurationRoundTrip(t *testing.T) {
	schema := `{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}`
	s := mustParse(t, schema)
	d := Duration{Months: 3, Days: 15, Milliseconds: 86400000}
	bin := mustEncode(t, s, d)
	var native any
	mustDecode(t, s, bin, &native)
	if got := native.(Duration); got != d {
		t.Fatalf("Decode: got %+v, want %+v", got, d)
	}
	j := mustEncodeJSON(t, s, native)
	var rt any
	mustDecodeJSON(t, s, j, &rt)
	if got := rt.(Duration); got != d {
		t.Fatalf("round-trip: got %+v, want %+v", got, d)
	}
}

func TestEncodeJSONStructCacheSharing(t *testing.T) {
	// Binary Encode and EncodeJSON share the typeFieldMapping cache on
	// serRecord: calling one warms the cache for the other.
	type R struct {
		Name string `avro:"name"`
		Age  int32  `avro:"age"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"name","type":"string"},
		{"name":"age","type":"int"}
	]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	v := R{Name: "Alice", Age: 30}

	// Binary encode first, which warms the cache.
	bin, err := s.Encode(&v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(bin) == 0 {
		t.Fatal("empty binary output")
	}

	// JSON encode second, which must reuse the cached mapping.
	j, err := s.EncodeJSON(&v)
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	if string(j) != `{"name":"Alice","age":30}` {
		t.Errorf("got %s", j)
	}

	// Reverse order: JSON first, binary second, fresh schema.
	s2, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	j2, err := s2.EncodeJSON(&v)
	if err != nil {
		t.Fatalf("EncodeJSON first: %v", err)
	}
	if string(j2) != `{"name":"Alice","age":30}` {
		t.Errorf("got %s", j2)
	}
	bin2, err := s2.Encode(&v)
	if err != nil {
		t.Fatalf("Encode second: %v", err)
	}
	if len(bin2) == 0 {
		t.Fatal("empty binary output")
	}

	// Concurrent: both paths simultaneously.
	s3, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	const goroutines = 8
	errs := make(chan error, goroutines*2)
	for range goroutines {
		go func() {
			_, err := s3.Encode(&v)
			errs <- err
		}()
		go func() {
			_, err := s3.EncodeJSON(&v)
			errs <- err
		}()
	}
	for range goroutines * 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

func TestEncodeJSONTimeAsDate(t *testing.T) {
	s, _ := Parse(`{"type":"int","logicalType":"date"}`)
	d := time.Date(2026, 3, 19, 0, 0, 0, 0, time.UTC)
	got := mustEncodeJSON(t, s, d)
	// days since epoch
	want := strconv.FormatInt(d.Unix()/86400, 10)
	if string(got) != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestEncodeJSONTimeAsTimeMillis(t *testing.T) {
	s, _ := Parse(`{"type":"int","logicalType":"time-millis"}`)
	// Duration input (from Decode).
	d := time.Duration(35245000) * time.Millisecond
	got := mustEncodeJSON(t, s, d)
	if string(got) != "35245000" {
		t.Errorf("duration: got %s", got)
	}
	// time.Time input (manually constructed time-of-day).
	tod := time.Date(0, 1, 1, 9, 47, 25, 0, time.UTC)
	got = mustEncodeJSON(t, s, tod)
	if string(got) != "35245000" {
		t.Errorf("time.Time: got %s", got)
	}
}

func TestEncodeJSONTimestampNanos(t *testing.T) {
	s, _ := Parse(`{"type":"long","logicalType":"timestamp-nanos"}`)
	now := time.Date(2026, 3, 19, 10, 0, 0, 123456789, time.UTC)
	got := mustEncodeJSON(t, s, now)
	want := strconv.FormatInt(now.UnixNano(), 10)
	if string(got) != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestFromAvroJSONIntTruncation(t *testing.T) {
	s, _ := Parse(`"int"`)
	var v any
	// Non-whole number for int should error.
	if err := s.DecodeJSON([]byte(`3.14`), &v); err == nil {
		t.Fatal("expected error for non-whole int")
	}
	// Overflow should error.
	if err := s.DecodeJSON([]byte(`3000000000`), &v); err == nil {
		t.Fatal("expected error for int32 overflow")
	}
}

func TestFromAvroJSONLongTruncation(t *testing.T) {
	s, _ := Parse(`"long"`)
	var v any
	if err := s.DecodeJSON([]byte(`1.5`), &v); err == nil {
		t.Fatal("expected error for non-whole long")
	}
}

func TestFromAvroJSONFloatTypeCheck(t *testing.T) {
	s, _ := Parse(`"float"`)
	var v any
	if err := s.DecodeJSON([]byte(`"not a number"`), &v); err == nil {
		t.Fatal("expected error for string as float")
	}
}

func TestFromAvroJSONDoubleTypeCheck(t *testing.T) {
	s, _ := Parse(`"double"`)
	var v any
	if err := s.DecodeJSON([]byte(`true`), &v); err == nil {
		t.Fatal("expected error for bool as double")
	}
}

func TestAppendJSONStringEscaping(t *testing.T) {
	s, _ := Parse(`"string"`)
	// Control characters and special escapes.
	tests := []struct {
		in   string
		want string
	}{
		{`hello`, `"hello"`},
		{"a\"b", `"a\"b"`},
		{"a\\b", `"a\\b"`},
		{"a\nb", `"a\nb"`},
		{"a\x00b", `"a\u0000b"`},
		{"日本語", `"日本語"`},           // multi-byte UTF-8 passed through
		{"a\u2028b", `"a\u2028b"`}, // U+2028 escaped
		{"a\u2029b", `"a\u2029b"`}, // U+2029 escaped
		// Invalid UTF-8 bytes are replaced with U+FFFD encoded as raw
		// UTF-8 (efbfbd), not as the literal `\ufffd` escape. Using raw
		// UTF-8 makes encode idempotent: a re-decode of an actual U+FFFD
		// codepoint round-trips to the same bytes.
		{string([]byte{0xff, 0xfe}), "\"\xef\xbf\xbd\xef\xbf\xbd\""},
	}
	for _, tt := range tests {
		got := mustEncodeJSON(t, s, tt.in)
		if string(got) != tt.want {
			t.Errorf("EncodeJSON(%q) = %s, want %s", tt.in, got, tt.want)
		}
	}
}

// TestEncodeJSONUFFFDIdempotence pins the canonical-idempotence guarantee of
// the JSON encoder for U+FFFD. An invalid UTF-8 byte and a valid U+FFFD
// codepoint must encode to the same bytes. Repeated decode and encode of any
// input is then a no-op after the first pass.
func TestEncodeJSONUFFFDIdempotence(t *testing.T) {
	s, _ := Parse(`"string"`)
	// String containing one invalid UTF-8 byte.
	invalid, err := s.EncodeJSON(string([]byte{0xff}))
	if err != nil {
		t.Fatal(err)
	}
	// String containing the valid U+FFFD codepoint encoded as UTF-8.
	valid, err := s.EncodeJSON("�")
	if err != nil {
		t.Fatal(err)
	}
	if string(invalid) != string(valid) {
		t.Errorf("encoder is not idempotent on U+FFFD:\n  invalid byte %x: %s\n  U+FFFD value:    %s",
			0xff, invalid, valid)
	}
	// And the round-trip-twice property: decode then re-encode the
	// canonical output must be a no-op.
	var v any
	if err := s.DecodeJSON(invalid, &v); err != nil {
		t.Fatalf("decode after encode failed: %v", err)
	}
	again, err := s.EncodeJSON(v)
	if err != nil {
		t.Fatalf("re-encode failed: %v", err)
	}
	if string(again) != string(invalid) {
		t.Errorf("canonical encode is not stable:\n  first:  %s\n  second: %s", invalid, again)
	}
}

func TestDecodeJSONGoavroLogicalBranchName(t *testing.T) {
	// goavro uses "long.timestamp-millis" as union branch names. We accept
	// those through the findUnionBranch fallback.
	schema := `["null",{"type":"long","logicalType":"timestamp-millis"}]`
	s := mustParse(t, schema)
	var v any
	mustDecodeJSON(t, s, []byte(`{"long.timestamp-millis":1687221496000}`), &v)
	got, ok := v.(time.Time)
	if !ok {
		t.Fatalf("expected time.Time, got %T: %v", v, v)
	}
	if got.UnixMilli() != 1687221496000 {
		t.Errorf("got %v", got)
	}
}

func TestEncodeJSONTimeLongDefault(t *testing.T) {
	// time.Time for a bare long (no logical type) should be rejected,
	// matching Encode's behavior.
	s, _ := Parse(`"long"`)
	now := time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)
	_, err := s.EncodeJSON(now)
	if err == nil {
		t.Fatal("expected error for time.Time on bare long")
	}
}

func TestSerStringTextMarshalerError(t *testing.T) {
	s, _ := Parse(`"string"`)
	v := textMarshalerErr{}
	_, err := s.AppendEncode(nil, &v)
	if err == nil {
		t.Fatal("expected error from MarshalText")
	}
}

func TestSerStringTextAppenderLong(t *testing.T) {
	// TextAppender with text > 63 bytes forces multi-byte varlong header.
	s, _ := Parse(`"string"`)
	long := testTextAppender{val: string(make([]byte, 200))}
	encoded := mustAppendEncode(t, s, nil, &long)
	var got string
	mustDecode(t, s, encoded, &got)
	if len(got) != 200 {
		t.Errorf("got len %d, want 200", len(got))
	}
}

// TestLogicalTypeRoundTrips verifies that every logical type round-trips
// through all four encode/decode functions:
//
//	value -> Encode -> Decode -> same value
//	value -> EncodeJSON -> DecodeJSON -> same value
//	value -> Encode -> Decode -> EncodeJSON -> DecodeJSON -> same value
func TestLogicalTypeRoundTrips(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		value  any    // input to Encode / EncodeJSON
		want   any    // expected from Decode / DecodeJSON to *any
		json   string // expected EncodeJSON output
	}{
		{
			name:   "date",
			schema: `{"type":"int","logicalType":"date"}`,
			value:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			want:   time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			json:   "20089",
		},
		{
			name:   "time-millis",
			schema: `{"type":"int","logicalType":"time-millis"}`,
			value:  time.Duration(35245000) * time.Millisecond,
			want:   time.Duration(35245000) * time.Millisecond,
			json:   "35245000",
		},
		{
			name:   "time-micros",
			schema: `{"type":"long","logicalType":"time-micros"}`,
			value:  time.Duration(35245000) * time.Microsecond,
			want:   time.Duration(35245000) * time.Microsecond,
			json:   "35245000",
		},
		{
			name:   "timestamp-millis",
			schema: `{"type":"long","logicalType":"timestamp-millis"}`,
			value:  time.Date(2025, 3, 19, 10, 0, 0, 0, time.UTC),
			want:   time.Date(2025, 3, 19, 10, 0, 0, 0, time.UTC),
			json:   "1742378400000",
		},
		{
			name:   "timestamp-micros",
			schema: `{"type":"long","logicalType":"timestamp-micros"}`,
			value:  time.Date(2025, 3, 19, 10, 0, 0, 0, time.UTC),
			want:   time.Date(2025, 3, 19, 10, 0, 0, 0, time.UTC),
			json:   "1742378400000000",
		},
		{
			name:   "timestamp-nanos",
			schema: `{"type":"long","logicalType":"timestamp-nanos"}`,
			value:  time.Date(2025, 3, 19, 10, 0, 0, 0, time.UTC),
			want:   time.Date(2025, 3, 19, 10, 0, 0, 0, time.UTC),
			json:   "1742378400000000000",
		},
		{
			// Spec / Java / fastavro form: unscaled-int (33 as 0x21)
			// emitted as a codepoint-mapped Avro JSON byte string.
			name:   "decimal-bytes",
			schema: `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`,
			value:  json.Number("0.33"),
			want:   new(big.Rat).SetFrac64(33, 100),
			json:   `"!"`,
		},
		{
			// Fixed-size variant: 8 bytes, sign-extended for unscaled=33,
			// so seven leading 0x00 codepoints + literal '!' for 0x21.
			name:   "decimal-fixed",
			schema: `{"type":"fixed","name":"dec","size":8,"logicalType":"decimal","precision":10,"scale":2}`,
			value:  json.Number("0.33"),
			want:   new(big.Rat).SetFrac64(33, 100),
			json:   `"\u0000\u0000\u0000\u0000\u0000\u0000\u0000!"`,
		},
		{
			name:   "uuid",
			schema: `{"type":"string","logicalType":"uuid"}`,
			value:  "550e8400-e29b-41d4-a716-446655440000",
			want:   "550e8400-e29b-41d4-a716-446655440000",
			json:   `"550e8400-e29b-41d4-a716-446655440000"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}

			// Encode, then Decode.
			binary, err := s.Encode(tt.value)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			var decoded any
			mustDecode(t, s, binary, &decoded)
			if !reflect.DeepEqual(tt.want, decoded) {
				t.Errorf("Encode→Decode: got %T(%v), want %T(%v)", decoded, decoded, tt.want, tt.want)
			}

			// EncodeJSON, then DecodeJSON.
			jb, err := s.EncodeJSON(tt.value)
			if err != nil {
				t.Fatalf("EncodeJSON: %v", err)
			}
			if string(jb) != tt.json {
				t.Errorf("EncodeJSON: got %s, want %s", jb, tt.json)
			}
			var djDecoded any
			if err := s.DecodeJSON(jb, &djDecoded); err != nil {
				t.Fatalf("DecodeJSON(%s): %v", jb, err)
			}
			if !reflect.DeepEqual(tt.want, djDecoded) {
				t.Errorf("EncodeJSON→DecodeJSON: got %T(%v), want %T(%v)", djDecoded, djDecoded, tt.want, tt.want)
			}

			// Full round-trip: Encode, Decode, EncodeJSON, DecodeJSON.
			jb2, err := s.EncodeJSON(decoded)
			if err != nil {
				t.Fatalf("EncodeJSON(decoded): %v", err)
			}
			var final any
			if err := s.DecodeJSON(jb2, &final); err != nil {
				t.Fatalf("DecodeJSON(jb2): %v", err)
			}
			if !reflect.DeepEqual(tt.want, final) {
				t.Errorf("full round-trip: got %T(%v), want %T(%v)", final, final, tt.want, tt.want)
			}
		})
	}
}

// TestPrimitiveRoundTrips verifies that all primitive types round-trip
// through Encode/Decode and EncodeJSON/DecodeJSON.
func TestPrimitiveRoundTrips(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		value  any
		want   any
		json   string
	}{
		{"null", `"null"`, nil, nil, "null"},
		{"boolean true", `"boolean"`, true, true, "true"},
		{"boolean false", `"boolean"`, false, false, "false"},
		{"int", `"int"`, int32(42), int32(42), "42"},
		{"int negative", `"int"`, int32(-7), int32(-7), "-7"},
		{"long", `"long"`, int64(9876543210), int64(9876543210), "9876543210"},
		{"float", `"float"`, float32(1.5), float32(1.5), "1.5"},
		{"double", `"double"`, float64(3.14159), float64(3.14159), "3.14159"},
		{"string", `"string"`, "hello", "hello", `"hello"`},
		{"bytes", `"bytes"`, []byte{0x01, 0x02}, []byte{0x01, 0x02}, `"\u0001\u0002"`},
		{"enum", `{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]}`, "GREEN", "GREEN", `"GREEN"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}

			// Encode, then Decode.
			binary, err := s.Encode(tt.value)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			var decoded any
			mustDecode(t, s, binary, &decoded)
			if !reflect.DeepEqual(tt.want, decoded) {
				t.Errorf("Encode→Decode: got %T(%v), want %T(%v)", decoded, decoded, tt.want, tt.want)
			}

			// EncodeJSON, then DecodeJSON.
			jb, err := s.EncodeJSON(tt.value)
			if err != nil {
				t.Fatalf("EncodeJSON: %v", err)
			}
			if string(jb) != tt.json {
				t.Errorf("EncodeJSON: got %s, want %s", jb, tt.json)
			}
			var djDecoded any
			if err := s.DecodeJSON(jb, &djDecoded); err != nil {
				t.Fatalf("DecodeJSON(%s): %v", jb, err)
			}
			if !reflect.DeepEqual(tt.want, djDecoded) {
				t.Errorf("EncodeJSON→DecodeJSON: got %T(%v), want %T(%v)", djDecoded, djDecoded, tt.want, tt.want)
			}

			// Cross: Encode, Decode, EncodeJSON, DecodeJSON.
			jb2, err := s.EncodeJSON(decoded)
			if err != nil {
				t.Fatalf("EncodeJSON(decoded): %v", err)
			}
			var final any
			if err := s.DecodeJSON(jb2, &final); err != nil {
				t.Fatalf("DecodeJSON(jb2): %v", err)
			}
			if !reflect.DeepEqual(tt.want, final) {
				t.Errorf("full round-trip: got %T(%v), want %T(%v)", final, final, tt.want, tt.want)
			}
		})
	}
}

// TestEncodeJSONCoercion exercises the numeric coercion paths in
// jsonCoerceToInt32, jsonCoerceToInt64, and jsonCoerceToFloat64.
func TestEncodeJSONCoercion(t *testing.T) {
	intSchema := MustParse(`"int"`)
	longSchema := MustParse(`"long"`)
	floatSchema := MustParse(`"float"`)
	doubleSchema := MustParse(`"double"`)

	type enc struct {
		name    string
		schema  *Schema
		v       any
		wantErr bool
	}
	cases := []enc{
		// int32
		{"int to int32", intSchema, int(42), false},
		{"uint to int32", intSchema, uint(42), false},
		{"float to int32", intSchema, float64(42), false},
		{"json.Number int to int32", intSchema, json.Number("42"), false},
		{"json.Number whole float int32", intSchema, json.Number("42.0"), false},
		{"json.Number sci int32", intSchema, json.Number("1e2"), false},
		{"json.Number float int32", intSchema, json.Number("3.14"), true},
		{"int64 overflow int32", intSchema, int64(1 << 40), true},
		{"uint overflow int32", intSchema, uint64(1 << 40), true},
		{"float overflow int32", intSchema, float64(1 << 40), true},
		{"float non-whole int32", intSchema, float64(3.14), true},
		{"json.Number overflow int32", intSchema, json.Number("99999999999"), true},
		{"json.Number invalid int32", intSchema, json.Number("not a number"), true},
		{"string to int32", intSchema, "hello", true},
		// int64
		{"int to int64", longSchema, int(42), false},
		{"uint to int64", longSchema, uint(42), false},
		{"float to int64", longSchema, float64(42), false},
		{"json.Number int to int64", longSchema, json.Number("42"), false},
		{"float non-whole int64", longSchema, float64(3.14), true},
		{"uint64 max overflow int64", longSchema, uint64(1<<63 + 1), true},
		{"float overflow int64", longSchema, float64(1e20), true},
		{"json.Number whole float int64", longSchema, json.Number("42.0"), false},
		{"json.Number sci int64", longSchema, json.Number("1e2"), false},
		{"json.Number float non-whole int64", longSchema, json.Number("3.14"), true},
		{"json.Number float overflow int64", longSchema, json.Number("1e20"), true},
		{"json.Number invalid int64", longSchema, json.Number("nope"), true},
		{"string to int64", longSchema, "hello", true},
		// float
		{"int to float", floatSchema, int(42), false},
		{"uint to float", floatSchema, uint(42), false},
		{"json.Number to float", floatSchema, json.Number("3.14"), false},
		// Lossy-destination policy: int/uint beyond float mantissa silently
		// IEEE-rounds, matching Java/fastavro.
		{"int overflow float lossy round", floatSchema, int64(1 << 30), false},
		{"uint overflow float lossy round", floatSchema, uint64(1 << 30), false},
		{"int to double", doubleSchema, int(42), false},
		{"invalid json.Number float", floatSchema, json.Number("nope"), true},
		{"string to float", floatSchema, "hello", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.schema.EncodeJSON(tc.v)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

// TestEncodeJSONLogical exercises logical type encoding paths in
// appendAvroJSON for date, time, timestamp, decimal, and duration.
func TestEncodeJSONLogical(t *testing.T) {
	tm := time.Date(2020, 1, 1, 12, 30, 45, 123456789, time.UTC)
	dur := 3*time.Hour + 45*time.Minute + 30*time.Second

	cases := []struct {
		name   string
		schema string
		v      any
	}{
		// date: time.Time to int
		{"date from time.Time", `{"type":"int","logicalType":"date"}`, tm},
		{"date from string", `{"type":"int","logicalType":"date"}`, "2020-01-01"},
		// time-millis: int path from time.Time (hour/min/sec derived)
		{"time-millis from time.Time", `{"type":"int","logicalType":"time-millis"}`, tm},
		{"time-millis from time.Duration", `{"type":"int","logicalType":"time-millis"}`, dur},
		// time-micros: int64 path from time.Duration / time.Time
		{"time-micros from time.Duration", `{"type":"long","logicalType":"time-micros"}`, dur},
		{"time-micros from time.Time", `{"type":"long","logicalType":"time-micros"}`, tm},
		// timestamp variants from time.Time
		{"timestamp-millis from time.Time", `{"type":"long","logicalType":"timestamp-millis"}`, tm},
		{"timestamp-micros from time.Time", `{"type":"long","logicalType":"timestamp-micros"}`, tm},
		{"timestamp-nanos from time.Time", `{"type":"long","logicalType":"timestamp-nanos"}`, tm},
		{"local-timestamp-millis from time.Time", `{"type":"long","logicalType":"local-timestamp-millis"}`, tm},
		{"local-timestamp-micros from time.Time", `{"type":"long","logicalType":"local-timestamp-micros"}`, tm},
		{"local-timestamp-nanos from time.Time", `{"type":"long","logicalType":"local-timestamp-nanos"}`, tm},
		// timestamp from RFC 3339 string
		{"timestamp-millis from string", `{"type":"long","logicalType":"timestamp-millis"}`, "2020-01-01T12:30:45Z"},
		{"timestamp-micros from string", `{"type":"long","logicalType":"timestamp-micros"}`, "2020-01-01T12:30:45Z"},
		{"timestamp-nanos from string", `{"type":"long","logicalType":"timestamp-nanos"}`, "2020-01-01T12:30:45Z"},
		// decimal bytes from various numeric sources
		{"decimal bytes from json.Number", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, json.Number("12.34")},
		{"decimal bytes from big.Rat", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, *big.NewRat(1234, 100)},
		{"decimal bytes from *big.Rat", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, big.NewRat(1234, 100)},
		{"decimal bytes from float", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, float64(12.34)},
		// decimal fixed from various numeric sources
		{"decimal fixed from json.Number", `{"type":"fixed","name":"d","size":8,"logicalType":"decimal","precision":10,"scale":2}`, json.Number("12.34")},
		{"decimal fixed from big.Rat", `{"type":"fixed","name":"d","size":8,"logicalType":"decimal","precision":10,"scale":2}`, *big.NewRat(1234, 100)},
		{"decimal fixed from *big.Rat", `{"type":"fixed","name":"d","size":8,"logicalType":"decimal","precision":10,"scale":2}`, big.NewRat(1234, 100)},
		{"decimal fixed from float", `{"type":"fixed","name":"d","size":8,"logicalType":"decimal","precision":10,"scale":2}`, float64(12.34)},
		// bytes from string
		{"bytes from string", `"bytes"`, "hello"},
		// duration
		{"duration from Duration", `{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}`, Duration{Months: 1, Days: 2, Milliseconds: 3}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := MustParse(tc.schema)
			mustEncodeJSON(t, s, tc.v)
		})
	}
}

// TestBinaryEncodeCoercion covers the binary encode path's json.Number
// and numeric coercion branches in serInt, serLong, serFloat, serDouble.
func TestBinaryEncodeCoercion(t *testing.T) {
	tm := time.Date(2020, 6, 15, 14, 30, 45, 0, time.UTC)
	// serInt json.Number paths
	intS := MustParse(`"int"`)
	if _, err := intS.Encode(json.Number("42")); err != nil {
		t.Error(err)
	}
	if _, err := intS.Encode(json.Number("3.14")); err == nil {
		t.Error("expected non-whole error")
	}
	if _, err := intS.Encode(json.Number("99999999999")); err == nil {
		t.Error("expected overflow")
	}
	if _, err := intS.Encode(json.Number("nope")); err == nil {
		t.Error("expected invalid")
	}
	// serLong json.Number paths
	longS := MustParse(`"long"`)
	if _, err := longS.Encode(json.Number("42")); err != nil {
		t.Error(err)
	}
	if _, err := longS.Encode(json.Number("3.14")); err == nil {
		t.Error("expected non-whole error")
	}
	if _, err := longS.Encode(json.Number("1e20")); err == nil {
		t.Error("expected overflow")
	}
	if _, err := longS.Encode(json.Number("nope")); err == nil {
		t.Error("expected invalid")
	}
	// serLong with float non-whole and overflow
	if _, err := longS.Encode(3.14); err == nil {
		t.Error("expected non-whole error")
	}
	if _, err := longS.Encode(1e20); err == nil {
		t.Error("expected overflow error")
	}
	// serTimeMillis with time.Time
	tmsS := MustParse(`{"type":"int","logicalType":"time-millis"}`)
	if _, err := tmsS.Encode(tm); err != nil {
		t.Error(err)
	}
	// serInt with encode nil for non-union errors
	if _, err := intS.Encode(nil); err == nil {
		t.Error("expected error encoding nil as int")
	}
}

// TestEncodeJSONStringEscapes covers all JSON escape sequences in
// appendJSONString and appendAvroJSONBytes.
func TestEncodeJSONStringEscapes(t *testing.T) {
	strS := MustParse(`"string"`)
	bytesS := MustParse(`"bytes"`)

	// Every escape byte: \b \f \t \n \r \" \\
	escapes := "\b\f\t\n\r\"\\"
	mustEncodeJSON(t, strS, escapes)
	mustEncodeJSON(t, bytesS, []byte(escapes))

	// Control chars (non-printable, < 0x20 but not one of the named escapes)
	mustEncodeJSON(t, strS, "\x01\x02\x03")
	mustEncodeJSON(t, bytesS, []byte{0x01, 0x02, 0xFF})

	// U+2028 and U+2029 (line/paragraph separator)
	mustEncodeJSON(t, strS, "\u2028\u2029")

	// Invalid UTF-8
	mustEncodeJSON(t, strS, "\xff\xfe")

	// Multi-byte valid UTF-8
	mustEncodeJSON(t, strS, "héllo")
}

// TestEncodeJSONStringBytesEnumCoverage covers remaining gaps in
// appendAvroJSON for string, bytes, fixed, and enum types.
func TestEncodeJSONStringBytesEnumCoverage(t *testing.T) {
	strS := MustParse(`"string"`)
	bytesS := MustParse(`"bytes"`)
	fixedS := MustParse(`{"type":"fixed","name":"f","size":4}`)
	enumS := MustParse(`{"type":"enum","name":"E","symbols":["A","B","C"]}`)

	// string: json.Number rejected
	if _, err := strS.EncodeJSON(json.Number("42")); err == nil {
		t.Error("expected error for json.Number as string")
	}
	// string: []byte accepted
	if _, err := strS.EncodeJSON([]byte("hi")); err != nil {
		t.Error(err)
	}
	// string: TextMarshaler (time.Time implements it)
	if _, err := strS.EncodeJSON(time.Now()); err != nil {
		t.Error(err)
	}
	// string: unsupported type
	if _, err := strS.EncodeJSON(42); err == nil {
		t.Error("expected unsupported error")
	}

	// bytes: unsupported type
	if _, err := bytesS.EncodeJSON(42); err == nil {
		t.Error("expected unsupported error")
	}

	// fixed: string source
	if _, err := fixedS.EncodeJSON("abcd"); err != nil {
		t.Error(err)
	}
	// fixed: [4]byte array
	if _, err := fixedS.EncodeJSON([4]byte{1, 2, 3, 4}); err != nil {
		t.Error(err)
	}
	// fixed: size mismatch
	if _, err := fixedS.EncodeJSON("xyz"); err == nil {
		t.Error("expected size mismatch")
	}
	// fixed: wrong type
	if _, err := fixedS.EncodeJSON(42); err == nil {
		t.Error("expected error")
	}

	// enum: unknown symbol
	if _, err := enumS.EncodeJSON("D"); err == nil {
		t.Error("expected unknown symbol error")
	}
	// enum: integer index
	if _, err := enumS.EncodeJSON(0); err != nil {
		t.Error(err)
	}
	if _, err := enumS.EncodeJSON(uint(1)); err != nil {
		t.Error(err)
	}
	// enum: integer out of range
	if _, err := enumS.EncodeJSON(99); err == nil {
		t.Error("expected range error")
	}
	// enum: wrong type
	if _, err := enumS.EncodeJSON(3.14); err == nil {
		t.Error("expected error")
	}
}

// TestMatrix_BytesToAvroJSONStringCodepointPerByte pins that
// [bytesToAvroJSONString] emits each byte 0x00-0xFF as a separate Unicode
// codepoint. `string(b)` is NOT equivalent. It reinterprets the slice as
// UTF-8, collapsing adjacent bytes that form a valid sequence (c3 a9 becomes
// one codepoint instead of two). It also maps invalid bytes to U+FFFD, which
// avroJSONBytesToBytes then rejects as out of range. The spec mandates one
// byte per codepoint.
//
// Round-trip invariant: avroJSONBytesToBytes(bytesToAvroJSONString(b)) == b
// for every []byte. That
// inverse pair is what makes SchemaField.Default round-trip through
// SchemaNode.Schema. The naive string(b) path, or json.Marshal's base64,
// breaks it for any default containing a byte >= 0x80.
func TestMatrix_BytesToAvroJSONStringCodepointPerByte(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []byte
	}{
		{"ascii", []byte{0x41, 0x42}},
		{"single high-bit byte", []byte{0xFF}},
		{"two-byte UTF-8 looking pair", []byte{0xC3, 0xA9}}, // string([]byte) collapses to "é"
		{"isolated invalid UTF-8", []byte{0x00, 0xE9}},      // string([]byte) maps E9 to U+FFFD
		{"all 256 byte values", func() []byte {
			b := make([]byte, 256)
			for i := range b {
				b[i] = byte(i)
			}
			return b
		}()},
		{"empty", []byte{}},
		{"nil", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoded := bytesToAvroJSONString(tc.in)
			decoded, err := avroJSONBytesToBytes(encoded)
			if err != nil {
				t.Fatalf("avroJSONBytesToBytes(bytesToAvroJSONString(%x)): %v", tc.in, err)
			}
			if !bytes.Equal(decoded, tc.in) {
				t.Errorf("round-trip mismatch: bytesToAvroJSONString(%x) → %q → avroJSONBytesToBytes → %x",
					tc.in, encoded, decoded)
			}
			// One codepoint per input byte, the property string([]byte)
			// violates whenever the slice contains bytes ≥ 0x80.
			runeCount := 0
			for range encoded {
				runeCount++
			}
			if runeCount != len(tc.in) {
				t.Errorf("rune count: got %d, want %d (each input byte must become one codepoint)",
					runeCount, len(tc.in))
			}
		})
	}

	// Here we show directly that string(b) does NOT satisfy the contract.
	// Bytes c3 a9 (which happen to spell U+00E9 in UTF-8) collapse to the
	// single rune 'é' under string([]byte). avroJSONBytesToBytes then maps
	// that one rune back to a single byte 0xE9, losing the original 2-byte
	// input. This locks in "do not use string(b) as a shortcut" against a
	// future simplification of the helper.
	t.Run("string([]byte) breaks the round-trip on high-bit bytes", func(t *testing.T) {
		in := []byte{0xC3, 0xA9}
		naive := string(in)
		naiveDecoded, _ := avroJSONBytesToBytes(naive)
		if bytes.Equal(naiveDecoded, in) {
			t.Errorf("string([]byte) unexpectedly preserved round-trip — this test was meant to prove it doesn't")
		}
		if len(naiveDecoded) != 1 || naiveDecoded[0] != 0xE9 {
			t.Errorf("naive shortcut produced %x; documented behavior is E9 (the single codepoint U+00E9 = bytes c3 a9 interpreted as UTF-8)", naiveDecoded)
		}
	})
}

// The errTooDeep recursion bound must be uniform across binary encode, JSON
// encode, binary decode, and JSON decode: one increment per schema nesting
// level. Record/union JSON encode formerly incremented twice per level, since
// a same-level dispatch hop also bumped depth. That halved the budget, so a
// value DecodeJSON and binary Encode both accepted failed EncodeJSON with
// errTooDeep at half the depth. That is a round-trip break.
func TestRegression_JSONEncodeDepthMatchesDecode(t *testing.T) {
	var b strings.Builder
	const n = 900 // well under maxDepth (1000), well over the former /2 break
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, `{"type":"record","name":"R%d","fields":[{"name":"f","type":`, i)
	}
	b.WriteString(`"int"`)
	b.WriteString(strings.Repeat(`}]}`, n))
	s := MustParse(b.String())

	js := []byte(strings.Repeat(`{"f":`, n) + `0` + strings.Repeat(`}`, n))
	var v any
	if err := s.DecodeJSON(js, &v); err != nil {
		t.Fatalf("DecodeJSON at depth %d: %v", n, err)
	}
	if _, err := s.Encode(v); err != nil {
		t.Fatalf("binary Encode at depth %d: %v", n, err)
	}
	if _, err := s.EncodeJSON(v); err != nil {
		t.Fatalf("EncodeJSON at depth %d must match Decode/binary, got: %v", n, err)
	}

	// The bound still protects against a cyclic Go value (must error, not
	// loop forever).
	type Node struct {
		Next *Node `avro:"next"`
		V    int32 `avro:"v"`
	}
	cyc := MustParse(`{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]},{"name":"v","type":"int"}]}`)
	n0 := &Node{V: 1}
	n0.Next = n0 // cycle
	if _, err := cyc.EncodeJSON(n0); err == nil {
		t.Error("EncodeJSON of a cyclic value must error (errTooDeep), not loop")
	}
	if _, err := cyc.Encode(n0); err == nil {
		t.Error("binary Encode of a cyclic value must error, not loop")
	}
}

// ---------- json_decode_test.go ----------

// TestDecodeJSONTypedInt exercises typed target paths for int fields.
func TestDecodeJSONTypedInt(t *testing.T) {
	s, _ := Parse(`{"type":"record","name":"R","fields":[
		{"name":"d","type":{"type":"int","logicalType":"date"}},
		{"name":"tm","type":{"type":"int","logicalType":"time-millis"}},
		{"name":"n","type":"int"}
	]}`)
	type R struct {
		D  time.Time     `avro:"d"`
		TM time.Duration `avro:"tm"`
		N  int32         `avro:"n"`
	}
	var r R
	mustDecodeJSON(t, s, []byte(`{"d":19700,"tm":43200000,"n":42}`), &r)
	if r.N != 42 {
		t.Fatalf("N: got %d", r.N)
	}
	if r.D.IsZero() {
		t.Fatal("D is zero")
	}
	if r.TM == 0 {
		t.Fatal("TM is zero")
	}
}

// TestDecodeJSONTypedLong exercises typed target paths for long fields.
func TestDecodeJSONTypedLong(t *testing.T) {
	s, _ := Parse(`{"type":"record","name":"R","fields":[
		{"name":"ts_ms","type":{"type":"long","logicalType":"timestamp-millis"}},
		{"name":"ts_us","type":{"type":"long","logicalType":"timestamp-micros"}},
		{"name":"ts_ns","type":{"type":"long","logicalType":"timestamp-nanos"}},
		{"name":"tm","type":{"type":"long","logicalType":"time-micros"}},
		{"name":"n","type":"long"}
	]}`)
	type R struct {
		TsMs time.Time     `avro:"ts_ms"`
		TsUs time.Time     `avro:"ts_us"`
		TsNs time.Time     `avro:"ts_ns"`
		TM   time.Duration `avro:"tm"`
		N    int64         `avro:"n"`
	}
	var r R
	mustDecodeJSON(t, s, []byte(`{"ts_ms":1700000000000,"ts_us":1700000000000000,"ts_ns":1700000000000000000,"tm":1500000,"n":99}`), &r)
	if r.TsMs.IsZero() || r.TsUs.IsZero() || r.TsNs.IsZero() {
		t.Fatal("timestamps are zero")
	}
	if r.TM == 0 {
		t.Fatal("TM is zero")
	}
	if r.N != 99 {
		t.Fatalf("N: got %d", r.N)
	}
}

// TestDecodeJSONTypedBytes exercises assignBytes for typed targets.
func TestDecodeJSONTypedBytes(t *testing.T) {
	t.Run("bytes to []byte", func(t *testing.T) {
		s, _ := Parse(`"bytes"`)
		var b []byte
		mustDecodeJSON(t, s, []byte(`"hello"`), &b)
		if string(b) != "hello" {
			t.Fatalf("got %q", b)
		}
	})
	t.Run("bytes to string", func(t *testing.T) {
		s, _ := Parse(`"bytes"`)
		var str string
		mustDecodeJSON(t, s, []byte(`"hello"`), &str)
		if str != "hello" {
			t.Fatalf("got %q", str)
		}
	})
	t.Run("fixed to [N]byte", func(t *testing.T) {
		s, _ := Parse(`{"type":"fixed","name":"f","size":3}`)
		var arr [3]byte
		mustDecodeJSON(t, s, []byte(`"abc"`), &arr)
		if arr != [3]byte{'a', 'b', 'c'} {
			t.Fatalf("got %v", arr)
		}
	})
	t.Run("decimal to json.Number", func(t *testing.T) {
		s, _ := Parse(`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)
		var n json.Number
		mustDecodeJSON(t, s, []byte("\"!\""), &n)
	})
	t.Run("decimal to big.Rat", func(t *testing.T) {
		s, _ := Parse(`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)
		var r big.Rat
		mustDecodeJSON(t, s, []byte("\"!\""), &r)
	})
	t.Run("decimal bytes from JSON number to big.Rat", func(t *testing.T) {
		s, _ := Parse(`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)
		var r big.Rat
		mustDecodeJSON(t, s, []byte("12.34"), &r)
	})
	t.Run("decimal bytes from JSON number to json.Number", func(t *testing.T) {
		s, _ := Parse(`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)
		var n json.Number
		mustDecodeJSON(t, s, []byte("12.34"), &n)
	})
	t.Run("decimal fixed from JSON number to big.Rat", func(t *testing.T) {
		s, _ := Parse(`{"type":"fixed","name":"d","size":8,"logicalType":"decimal","precision":10,"scale":2}`)
		var r big.Rat
		mustDecodeJSON(t, s, []byte("12.34"), &r)
	})
	t.Run("decimal fixed from JSON number to json.Number", func(t *testing.T) {
		s, _ := Parse(`{"type":"fixed","name":"d","size":8,"logicalType":"decimal","precision":10,"scale":2}`)
		var n json.Number
		mustDecodeJSON(t, s, []byte("12.34"), &n)
	})
	t.Run("decimal bytes from JSON number to unsupported type errors", func(t *testing.T) {
		s, _ := Parse(`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)
		var i int
		if err := s.DecodeJSON([]byte("12.34"), &i); err == nil {
			t.Fatal("expected error for unsupported target")
		}
	})
	t.Run("fixed duration to Duration", func(t *testing.T) {
		s, _ := Parse(`{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}`)
		var d Duration
		mustDecodeJSON(t, s, []byte(`"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000"`), &d)
	})
}

// TestDecodeJSONTypedBool exercises typed bool target.
func TestDecodeJSONTypedBool(t *testing.T) {
	s, _ := Parse(`"boolean"`)
	var b bool
	mustDecodeJSON(t, s, []byte(`true`), &b)
	if !b {
		t.Fatal("expected true")
	}
}

// TestDecodeJSONTypedString exercises typed string targets.
func TestDecodeJSONTypedString(t *testing.T) {
	s, _ := Parse(`"string"`)
	var str string
	mustDecodeJSON(t, s, []byte(`"hello"`), &str)
	if str != "hello" {
		t.Fatalf("got %q", str)
	}
	// String with escapes.
	mustDecodeJSON(t, s, []byte(`"hello\nworld"`), &str)
	if str != "hello\nworld" {
		t.Fatalf("got %q", str)
	}
}

// TestDecodeJSONTypedFloat exercises typed float targets.
func TestDecodeJSONTypedFloat(t *testing.T) {
	s, _ := Parse(`"float"`)
	var f float32
	mustDecodeJSON(t, s, []byte(`3.14`), &f)
	if f < 3.13 || f > 3.15 {
		t.Fatalf("got %v", f)
	}
}

// TestDecodeJSONTypedDouble exercises typed double targets.
func TestDecodeJSONTypedDouble(t *testing.T) {
	s, _ := Parse(`"double"`)
	var f float64
	mustDecodeJSON(t, s, []byte(`3.14159`), &f)
	if f != 3.14159 {
		t.Fatalf("got %v", f)
	}
}

// TestDecodeJSONRecordMap exercises DecodeJSON into map[string]T.
func TestDecodeJSONRecordMap(t *testing.T) {
	s, _ := Parse(recABSchema)
	var m map[string]any
	mustDecodeJSON(t, s, []byte(`{"a":1,"b":"hello"}`), &m)
	if m["b"] != "hello" {
		t.Fatalf("got %v", m)
	}
}

// TestDecodeJSONMapTyped exercises DecodeJSON into map[string]T typed values.
func TestDecodeJSONMapTyped(t *testing.T) {
	s, _ := Parse(`{"type":"map","values":"int"}`)
	var m map[string]int32
	mustDecodeJSON(t, s, []byte(`{"x":1,"y":2}`), &m)
	if m["x"] != 1 || m["y"] != 2 {
		t.Fatalf("got %v", m)
	}
}

// TestDecodeJSONArrayTyped exercises DecodeJSON into typed slices.
func TestDecodeJSONArrayTyped(t *testing.T) {
	s, _ := Parse(`{"type":"array","items":"string"}`)
	var arr []string
	mustDecodeJSON(t, s, []byte(`["a","b","c"]`), &arr)
	if len(arr) != 3 || arr[0] != "a" {
		t.Fatalf("got %v", arr)
	}
}

// TestDecodeJSONUnionBranchTyped exercises typed union targets.
func TestDecodeJSONUnionBranchTyped(t *testing.T) {
	s, _ := Parse(`["null","string"]`)
	var str *string
	mustDecodeJSON(t, s, []byte(`{"string":"hello"}`), &str)
	if str == nil || *str != "hello" {
		t.Fatalf("got %v", str)
	}
}

// TestDecodeJSONSkipCompound exercises skipping unknown object/array fields.
func TestDecodeJSONSkipCompound(t *testing.T) {
	s, _ := Parse(recASchema)
	// Extra fields with nested objects and arrays should be skipped.
	input := `{"a":1,"unknown_obj":{"nested":true},"unknown_arr":[1,2,3]}`
	var out any
	mustDecodeJSON(t, s, []byte(input), &out)
	m := out.(map[string]any)
	if m["a"] != int32(1) {
		t.Fatalf("got %v", m)
	}
	if _, ok := m["unknown_obj"]; ok {
		t.Fatal("unknown_obj should have been skipped")
	}
}

// TestDecodeJSONEscapedStrings exercises resolveJSONEscapes via DecodeJSON.
func TestDecodeJSONEscapedStrings(t *testing.T) {
	s, _ := Parse(`"string"`)
	tests := []struct {
		input string
		want  string
	}{
		{`"hello"`, "hello"},
		{`"line1\nline2"`, "line1\nline2"},
		{`"tab\there"`, "tab\there"},
		{`"quote\"inside"`, `quote"inside`},
		{`"back\\slash"`, `back\slash`},
		{`"unicode\u0041"`, "unicodeA"},
		{`"slash\/"`, "slash/"},
	}
	for _, tt := range tests {
		var got any
		if err := s.DecodeJSON([]byte(tt.input), &got); err != nil {
			t.Fatalf("input %s: %v", tt.input, err)
		}
		if got != tt.want {
			t.Fatalf("input %s: got %q, want %q", tt.input, got, tt.want)
		}
	}
}

// TestDecodeJSONInvalidEscapeRejected pins that an unrecognized escape
// sequence (\x, \z, \q, none of the eight escapes JSON defines) is
// rejected rather than silently decoded with the backslash dropped.
// Dropping the backslash corrupts string content (e.g. "C:\dir" would
// decode to "C:dir"), and the authoritative implementations reject:
// Java's JsonDecoder uses Jackson with no backslash-escaping feature
// enabled ("Unrecognized character escape"), and fastavro's
// AvroJSONDecoder parses through Python's json (raises "Invalid \escape").
// The rejection is uniform across every string-shaped target (string,
// enum, map key, bytes/fixed) because all route through walkJSONEscapes.
func TestDecodeJSONInvalidEscapeRejected(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		input  string
	}{
		{"string", `"string"`, `"\x41"`},
		{"string mid", `"string"`, `"a\qb"`},
		{"enum", `{"type":"enum","name":"E","symbols":["AZ"]}`, `"\AZ"`},
		{"bytes", `"bytes"`, `"\z"`},
		{"map key", `{"type":"map","values":"int"}`, `{"\qkey":1}`},
		{"map value", `{"type":"map","values":"string"}`, `{"k":"\z"}`},
		{"array item", `{"type":"array","items":"string"}`, `["\z"]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := MustParse(c.schema)
			var out any
			if err := s.DecodeJSON([]byte(c.input), &out); err == nil {
				t.Fatalf("invalid escape in %s accepted (got %#v); want reject", c.input, out)
			}
		})
	}

	// The eight valid JSON escapes must still decode; rejecting bad
	// escape *sequences* must not touch them.
	s := MustParse(`"string"`)
	for _, ok := range []struct{ in, want string }{
		{`"a\nb"`, "a\nb"},
		{`"a\tb"`, "a\tb"},
		{`"a\\b"`, `a\b`},
		{`"a\"b"`, `a"b`},
		{`"a\/b"`, "a/b"},
		{`"aAb"`, "aAb"},
	} {
		var out any
		if err := s.DecodeJSON([]byte(ok.in), &out); err != nil {
			t.Fatalf("valid input %q rejected: %v", ok.in, err)
		} else if out != ok.want {
			t.Fatalf("input %q: got %q, want %q", ok.in, out, ok.want)
		}
	}
}

// TestDecodeJSONRawControlCharRejected pins that an unescaped control
// character (U+0000-U+001F) inside a JSON string is rejected, matching
// encoding/json, Java (Jackson), fastavro, and RFC 8259 §7 (which
// requires control chars to be escaped). The escaped forms, plus
// 0x7F (DEL, not a JSON control char) stay accepted. The rejection is
// uniform across string/enum/map-key/bytes (all route through
// consumeStringRaw).
func TestDecodeJSONRawControlCharRejected(t *testing.T) {
	reject := []struct{ schema, input string }{
		{`"string"`, "\"a\nb\""},
		{`"string"`, "\"a\tb\""},
		{`"string"`, "\"\x00\""},
		{`"string"`, "\"\x1f\""},
		{`{"type":"enum","name":"E","symbols":["AB"]}`, "\"A\nB\""},
		{`{"type":"map","values":"int"}`, "{\"k\ny\":1}"},
		{`"bytes"`, "\"a\nb\""},
	}
	for _, c := range reject {
		var out any
		if err := MustParse(c.schema).DecodeJSON([]byte(c.input), &out); err == nil {
			t.Errorf("%s DecodeJSON(%q) accepted; want reject (unescaped control char)", c.schema, c.input)
		}
	}
	accept := []struct{ in, want string }{
		{`"a\nb"`, "a\nb"},
		{`"a\tb"`, "a\tb"},
		{`"\u0000"`, "\x00"},
		{`"\u001f"`, "\x1f"},
		{"\"a\x7fb\"", "a\x7fb"}, // 0x7F is allowed raw per RFC 8259
	}
	s := MustParse(`"string"`)
	for _, c := range accept {
		var out any
		if err := s.DecodeJSON([]byte(c.in), &out); err != nil {
			t.Errorf("DecodeJSON(%q) rejected: %v", c.in, err)
		} else if out != c.want {
			t.Errorf("DecodeJSON(%q): got %q want %q", c.in, out, c.want)
		}
	}
}

// TestDecodeJSONInvalidUTF8Rejected pins that invalid UTF-8 byte
// sequences inside a JSON string are rejected. Valid multi-byte UTF-8
// stays accepted, as do the \u00XX escapes the encoder produces for
// non-ASCII bytes, so the encode and decode round trip that the bytes
// display path relies on is preserved.
func TestDecodeJSONInvalidUTF8Rejected(t *testing.T) {
	s := MustParse(`"string"`)
	for _, in := range []string{
		"\"\x80\"",       // lone continuation byte
		"\"\xff\xfe\"",   // two invalid bytes
		"\"\xc3\"",       // truncated 2-byte sequence
		"\"a\xe2\x82b\"", // truncated 3-byte sequence
	} {
		var out any
		if err := s.DecodeJSON([]byte(in), &out); err == nil {
			t.Errorf("DecodeJSON(% x) accepted; want reject (invalid UTF-8)", []byte(in))
		}
	}
	for _, c := range []struct{ in, want string }{
		{"\"é\"", "é"},
		{"\"中文\"", "中文"},
		{"\"😀\"", "😀"},
		{`"Û"`, "Û"},
		{`""`, ""},
	} {
		var out any
		if err := s.DecodeJSON([]byte(c.in), &out); err != nil {
			t.Errorf("DecodeJSON(%q) rejected: %v", c.in, err)
		} else if out != c.want {
			t.Errorf("DecodeJSON(%q): got %q want %q", c.in, out, c.want)
		}
	}
	// Bytes round-trip: EncodeJSON escapes every non-ASCII byte; DecodeJSON
	// reads it back exactly (the path console #2425 / rpk produce rely on).
	bs := MustParse(`"bytes"`)
	orig := string([]byte{0x00, 0x0a, 0xdb, 0x80, 0xff, 0x41})
	enc := mustAppendEncodeJSON(t, bs, nil, []byte(orig))
	var back []byte
	if err := bs.DecodeJSON(enc, &back); err != nil {
		t.Fatalf("bytes round-trip rejected: %v (enc=%s)", err, enc)
	}
	if string(back) != orig {
		t.Fatalf("bytes round-trip mismatch: got % x want % x", back, []byte(orig))
	}
}

// TestDecodeJSONTrailingContentRejected pins that trailing non-whitespace
// after a single decoded value is rejected (matching encoding/json.Unmarshal
// and fastavro). DecodeJSON decodes exactly one value and returns no
// offset, so concatenated values cannot be streamed. Surrounding/trailing
// whitespace stays accepted.
func TestDecodeJSONTrailingContentRejected(t *testing.T) {
	for _, c := range []struct{ schema, input string }{
		{`"int"`, "5 6"},
		{`"int"`, "5true"},
		{`"int"`, "5null"},
		{`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`, `{"a":1}{"a":2}`},
		{`{"type":"array","items":"int"}`, "[1,2] [3,4]"},
	} {
		var out any
		if err := MustParse(c.schema).DecodeJSON([]byte(c.input), &out); err == nil {
			t.Errorf("%s DecodeJSON(%q) accepted; want reject (trailing content)", c.schema, c.input)
		}
	}
	for _, c := range []struct{ schema, input string }{
		{`"int"`, "5"},
		{`"int"`, "  5  "},
		{`"int"`, "5\n"},
		{`{"type":"array","items":"int"}`, "[1,2]"},
	} {
		var out any
		if err := MustParse(c.schema).DecodeJSON([]byte(c.input), &out); err != nil {
			t.Errorf("%s DecodeJSON(%q) rejected: %v", c.schema, c.input, err)
		}
	}
}

// TestDecodeJSONFloatGrammarRejected pins that a JSON number whose grammar
// is invalid per RFC 8259, a trailing dot with no fractional digit ("5.",
// "5.e3", "1.e5"), is rejected when decoding into a float/double target,
// the same as the int/long arms already reject it and the same as Java
// (Jackson), fastavro (Python json), and goavro (whose numberLength state
// machine requires a digit after the dot) reject. Without the shared
// isJSONNumber gate, strconv.ParseFloat would silently accept these. The
// IEEE special forms (±Inf from overflow, NaN/Infinity tokens) remain
// accepted: those are semantic leniencies, not grammar violations.
func TestDecodeJSONFloatGrammarRejected(t *testing.T) {
	bad := []string{"5.", "5.e3", "1.e5", "-5.", "0.", "5.E3"}
	for _, schema := range []string{`"float"`, `"double"`, `"long"`, `"int"`} {
		s := MustParse(schema)
		for _, in := range bad {
			var out any
			if err := s.DecodeJSON([]byte(in), &out); err == nil {
				t.Errorf("%s DecodeJSON(%q) accepted (=%v); want reject", schema, in, out)
			}
		}
	}

	// Valid grammar must still decode on float/double, including the
	// overflow-to-±Inf and special-float forms that are deliberately lenient.
	type fc struct {
		in   string
		want float64
	}
	for _, schema := range []string{`"float"`, `"double"`} {
		s := MustParse(schema)
		for _, c := range []fc{
			{"5.5", 5.5}, {"5e3", 5000}, {"5.5e3", 5500}, {"0.0", 0}, {"-5.5", -5.5},
			{"42", 42}, {"1e999", math.Inf(1)}, {"-1e999", math.Inf(-1)},
			{`"NaN"`, math.NaN()}, {`"Infinity"`, math.Inf(1)}, {"NaN", math.NaN()},
		} {
			var out any
			if err := s.DecodeJSON([]byte(c.in), &out); err != nil {
				t.Errorf("%s DecodeJSON(%q) rejected: %v", schema, c.in, err)
				continue
			}
			got := toF64(out)
			if math.IsNaN(c.want) {
				if !math.IsNaN(got) {
					t.Errorf("%s DecodeJSON(%q): got %v, want NaN", schema, c.in, got)
				}
			} else if got != c.want {
				t.Errorf("%s DecodeJSON(%q): got %v, want %v", schema, c.in, got, c.want)
			}
		}
	}

	// Encode/decode symmetry: EncodeJSON rejects json.Number("5.") and so
	// must DecodeJSON; both run the same isJSONNumber gate.
	d := MustParse(`"double"`)
	for _, in := range []string{"5.", "5.e3"} {
		_, encErr := d.EncodeJSON(json.Number(in))
		decErr := d.DecodeJSON([]byte(in), new(any))
		if (encErr == nil) != (decErr == nil) {
			t.Errorf("encode/decode disagree on %q: enc=%v dec=%v", in, encErr, decErr)
		}
	}
}

func toF64(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	}
	return math.NaN()
}

// TestDecodeJSONUnionBareMatch exercises bare union matching by token type.
func TestDecodeJSONUnionBareMatch(t *testing.T) {
	s, _ := Parse(`["null","boolean","int","long","float","string",{"type":"array","items":"int"},{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}]`)

	tests := []struct {
		input string
		check func(any) bool
	}{
		{`null`, func(v any) bool { return v == nil }},
		{`true`, func(v any) bool { return v == true }},
		{`42`, func(v any) bool { return v == int32(42) }},
		{`"hello"`, func(v any) bool { return v == "hello" }},
		{`[1,2]`, func(v any) bool { return len(v.([]any)) == 2 }},
		{`{"x":1}`, func(v any) bool { return v.(map[string]any)["x"] == int32(1) }},
	}
	for _, tt := range tests {
		var out any
		if err := s.DecodeJSON([]byte(tt.input), &out); err != nil {
			t.Fatalf("input %s: %v", tt.input, err)
		}
		if !tt.check(out) {
			t.Fatalf("input %s: got %v (%T)", tt.input, out, out)
		}
	}
}

// TestDecodeJSONErrors exercises error paths.
func TestDecodeJSONErrors(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		input  string
	}{
		{"bool type mismatch", `"boolean"`, `42`},
		{"int type mismatch", `"int"`, `"hello"`},
		{"long type mismatch", `"long"`, `true`},
		{"float type mismatch", `"float"`, `[1]`},
		{"double type mismatch", `"double"`, `[1]`},
		{"string type mismatch", `"string"`, `42`},
		{"bytes type mismatch", `"bytes"`, `42`},
		{"array type mismatch", `{"type":"array","items":"int"}`, `"hello"`},
		{"record type mismatch", `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`, `[1]`},
		{"union no match", `["null","int"]`, `"hello"`},
		{"missing required field", `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`, `{}`},
		{"int overflow", `"int"`, `3000000000`},
		{"long overflow", `"long"`, `1e25`},
		{"not whole number int", `"int"`, `1.5`},
		{"not whole number long", `"long"`, `1.5`},
		{"null expected", `"null"`, `42`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustParse(t, tt.schema)
			var out any
			if err := s.DecodeJSON([]byte(tt.input), &out); err == nil {
				t.Fatalf("expected error, got %v", out)
			}
		})
	}
}

// TestDecodeJSONTypedErrors exercises error paths with typed targets.
func TestDecodeJSONTypedErrors(t *testing.T) {
	t.Run("bool into int", func(t *testing.T) {
		s, _ := Parse(`"boolean"`)
		var n int
		if err := s.DecodeJSON([]byte(`true`), &n); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("int into string", func(t *testing.T) {
		s, _ := Parse(`"int"`)
		var str string
		if err := s.DecodeJSON([]byte(`42`), &str); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("string into int", func(t *testing.T) {
		s, _ := Parse(`"string"`)
		var n int
		if err := s.DecodeJSON([]byte(`"hello"`), &n); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("float into bool", func(t *testing.T) {
		s, _ := Parse(`"float"`)
		var b bool
		if err := s.DecodeJSON([]byte(`3.14`), &b); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("double into bool", func(t *testing.T) {
		s, _ := Parse(`"double"`)
		var b bool
		if err := s.DecodeJSON([]byte(`3.14`), &b); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("array into map", func(t *testing.T) {
		s, _ := Parse(`{"type":"array","items":"int"}`)
		var m map[string]int
		if err := s.DecodeJSON([]byte(`[1,2]`), &m); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("map into slice", func(t *testing.T) {
		s, _ := Parse(`{"type":"map","values":"int"}`)
		var sl []int
		if err := s.DecodeJSON([]byte(`{"a":1}`), &sl); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("record into slice", func(t *testing.T) {
		s, _ := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
		var sl []int
		if err := s.DecodeJSON([]byte(`{"a":1}`), &sl); err == nil {
			t.Fatal("expected error")
		}
	})
}

// TestDecodeJSONNullPointer exercises nil pointer for non-nil target.
func TestDecodeJSONNullPointer(t *testing.T) {
	var out any
	s, _ := Parse(`"int"`)
	err := s.DecodeJSON([]byte(`42`), out) // non-pointer
	if err == nil {
		t.Fatal("expected error for non-pointer target")
	}
}

// TestDecodeJSONCustomWithTypedTarget exercises custom decoder with struct target.
func TestDecodeJSONCustomWithTypedTarget(t *testing.T) {
	type Money struct {
		Cents int64
	}
	s := mustParse(t, `{"type":"long","logicalType":"money"}`, NewCustomType[Money, int64]("money",
		func(m Money, _ *SchemaNode) (int64, error) { return m.Cents, nil },
		func(c int64, _ *SchemaNode) (Money, error) { return Money{Cents: c}, nil },
	))
	var m Money
	mustDecodeJSON(t, s, []byte(`42`), &m)
	if m.Cents != 42 {
		t.Fatalf("got %d", m.Cents)
	}
}

// TestDecodeJSONFloatSpecials exercises NaN/Infinity in DecodeJSON.
func TestDecodeJSONFloatSpecials(t *testing.T) {
	sf, _ := Parse(`"float"`)
	sd, _ := Parse(`"double"`)

	t.Run("float NaN string", func(t *testing.T) {
		var v any
		mustDecodeJSON(t, sf, []byte(`"NaN"`), &v)
		if !math.IsNaN(float64(v.(float32))) {
			t.Fatalf("expected NaN, got %v", v)
		}
	})
	t.Run("float null NaN", func(t *testing.T) {
		var v any
		mustDecodeJSON(t, sf, []byte(`null`), &v)
		if !math.IsNaN(float64(v.(float32))) {
			t.Fatalf("expected NaN, got %v", v)
		}
	})
	t.Run("double NaN string", func(t *testing.T) {
		var v any
		mustDecodeJSON(t, sd, []byte(`"NaN"`), &v)
		if !math.IsNaN(v.(float64)) {
			t.Fatalf("expected NaN, got %v", v)
		}
	})
	t.Run("double null NaN", func(t *testing.T) {
		var v any
		mustDecodeJSON(t, sd, []byte(`null`), &v)
		if !math.IsNaN(v.(float64)) {
			t.Fatalf("expected NaN, got %v", v)
		}
	})
}

// TestTimestampNanosConversion covers the shared conversion function.
func TestTimestampNanosConversion(t *testing.T) {
	now := time.Now().UTC()
	ns, err := timeToTimestampNanos(now)
	if err != nil {
		t.Fatal(err)
	}
	got := timestampNanosToTime(ns)
	if !now.Equal(got) {
		t.Fatalf("round-trip: %v != %v", now, got)
	}
}

// TestDecodeJSONEnumTyped exercises enum to typed string.
func TestDecodeJSONEnumTyped(t *testing.T) {
	s, _ := Parse(`{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]}`)
	var c string
	mustDecodeJSON(t, s, []byte(`"GREEN"`), &c)
	if c != "GREEN" {
		t.Fatalf("got %q", c)
	}
}

// TestDecodeJSONStringToBytes exercises string to []byte target.
func TestDecodeJSONStringToBytes(t *testing.T) {
	s, _ := Parse(`"string"`)
	var b []byte
	mustDecodeJSON(t, s, []byte(`"hello"`), &b)
	if string(b) != "hello" {
		t.Fatalf("got %q", b)
	}
}

// TestScannerErrors exercises scanner error paths.
func TestScannerErrors(t *testing.T) {
	s, _ := Parse(`"int"`)
	var out any
	// Truncated input.
	if err := s.DecodeJSON([]byte(``), &out); err == nil {
		t.Fatal("expected error for empty input")
	}
	// Invalid JSON.
	if err := s.DecodeJSON([]byte(`{`), &out); err == nil {
		t.Fatal("expected error for truncated object")
	}
}

// TestDecodeJSONNilTarget exercises the nil pointer check.
func TestDecodeJSONNilTarget(t *testing.T) {
	s, _ := Parse(`"int"`)
	err := s.DecodeJSON([]byte(`42`), (*int)(nil))
	if err == nil {
		t.Fatal("expected error for nil pointer")
	}
}

// TestWalkJSONEscapesSurrogatePair exercises UTF-16 surrogate pair handling.
func TestWalkJSONEscapesSurrogatePair(t *testing.T) {
	// 𐐷 is U+10437 = surrogate pair D801 DC37
	raw := []byte(`\uD801\uDC37`)
	got, err := resolveJSONEscapes(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got != "𐐷" {
		t.Fatalf("got %q, want 𐐷", got)
	}
}

// TestDecodeJSONIntUint exercises uint target for int fields.
func TestDecodeJSONIntUint(t *testing.T) {
	s, _ := Parse(`"int"`)
	var u uint32
	mustDecodeJSON(t, s, []byte(`42`), &u)
	if u != 42 {
		t.Fatalf("got %d", u)
	}
}

// TestDecodeJSONLongUint exercises uint target for long fields.
func TestDecodeJSONLongUint(t *testing.T) {
	s, _ := Parse(`"long"`)
	var u uint64
	mustDecodeJSON(t, s, []byte(`42`), &u)
	if u != 42 {
		t.Fatalf("got %d", u)
	}
}

// TestDecodeJSONDecodeValueUnknownKind exercises the default case.
func TestDecodeJSONDecodeValueUnknownKind(t *testing.T) {
	// Create a node with bogus kind to exercise the default error path.
	// This can't happen through Parse, so test directly.
	ctx := &jsonDecoder{
		scanner: &jsonScanner{data: []byte(`42`)},
		slab:    &slab{},
	}
	var out any
	err := ctx.decodeValue(reflect.ValueOf(&out).Elem(), &schemaNode{kind: "bogus"})
	if err == nil {
		t.Fatal("expected error for unknown kind")
	}
}

// TestDecodeJSONLogicalTypesAny exercises all decodeLogical* branches for *any.
func TestDecodeJSONLogicalTypesAny(t *testing.T) {
	tests := []struct {
		schema string
		input  string
		check  func(any) bool
	}{
		// decodeLogicalInt: date
		{`{"type":"int","logicalType":"date"}`, `19700`, func(v any) bool { _, ok := v.(time.Time); return ok }},
		// decodeLogicalInt: time-millis
		{`{"type":"int","logicalType":"time-millis"}`, `43200000`, func(v any) bool { _, ok := v.(time.Duration); return ok }},
		// decodeLogicalInt: plain (no logical)
		{`"int"`, `42`, func(v any) bool { return v == int32(42) }},
		// decodeLogicalLong: timestamp-millis
		{`{"type":"long","logicalType":"timestamp-millis"}`, `1700000000000`, func(v any) bool { _, ok := v.(time.Time); return ok }},
		// decodeLogicalLong: timestamp-micros
		{`{"type":"long","logicalType":"timestamp-micros"}`, `1700000000000000`, func(v any) bool { _, ok := v.(time.Time); return ok }},
		// decodeLogicalLong: timestamp-nanos
		{`{"type":"long","logicalType":"timestamp-nanos"}`, `1700000000000000000`, func(v any) bool { _, ok := v.(time.Time); return ok }},
		// decodeLogicalLong: local-timestamp-millis
		{`{"type":"long","logicalType":"local-timestamp-millis"}`, `1700000000000`, func(v any) bool { _, ok := v.(time.Time); return ok }},
		// decodeLogicalLong: local-timestamp-micros
		{`{"type":"long","logicalType":"local-timestamp-micros"}`, `1700000000000000`, func(v any) bool { _, ok := v.(time.Time); return ok }},
		// decodeLogicalLong: local-timestamp-nanos
		{`{"type":"long","logicalType":"local-timestamp-nanos"}`, `1700000000000000000`, func(v any) bool { _, ok := v.(time.Time); return ok }},
		// decodeLogicalLong: time-micros
		{`{"type":"long","logicalType":"time-micros"}`, `1500000`, func(v any) bool { _, ok := v.(time.Duration); return ok }},
		// decodeLogicalLong: plain (no logical)
		{`"long"`, `99`, func(v any) bool { return v == int64(99) }},
		// decodeLogicalFixed: decimal (4 bytes: value 33, scale 2, so 0.33;
		// precision must fit in 4 bytes: max 9)
		{`{"type":"fixed","name":"d","size":4,"logicalType":"decimal","precision":9,"scale":2}`, `"\u0000\u0000\u0000!"`, func(v any) bool { _, ok := v.(*big.Rat); return ok }},
		// decodeLogicalFixed: duration (12 bytes, all printable ASCII for simplicity)
		{`{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}`, "\"abcdefghijkl\"", func(v any) bool { _, ok := v.(Duration); return ok }},
		// decodeLogicalFixed: plain (no logical)
		{`{"type":"fixed","name":"f","size":3}`, `"abc"`, func(v any) bool { b, ok := v.([]byte); return ok && len(b) == 3 }},
		// decodeLogicalBytes: decimal
		{`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, `"!"`, func(v any) bool { _, ok := v.(*big.Rat); return ok }},
		// decodeLogicalBytes: plain
		{`"bytes"`, `"hello"`, func(v any) bool { _, ok := v.([]byte); return ok }},
	}
	for _, tt := range tests {
		s, err := Parse(tt.schema)
		if err != nil {
			t.Fatalf("parse %s: %v", tt.schema, err)
		}
		var out any
		if err := s.DecodeJSON([]byte(tt.input), &out); err != nil {
			t.Fatalf("schema %s input %s: %v", tt.schema, tt.input, err)
		}
		if !tt.check(out) {
			t.Fatalf("schema %s: got %v (%T)", tt.schema, out, out)
		}
	}
}

// TestDecodeJSONSkipValueTypes exercises all skipValue branches.
func TestDecodeJSONSkipValueTypes(t *testing.T) {
	s, _ := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	tests := []struct {
		name  string
		input string
	}{
		{"skip string", `{"unknown":"hello","a":1}`},
		{"skip number", `{"unknown":3.14,"a":1}`},
		{"skip bool true", `{"unknown":true,"a":1}`},
		{"skip bool false", `{"unknown":false,"a":1}`},
		{"skip null", `{"unknown":null,"a":1}`},
		{"skip array", `{"unknown":[1,[2],{"x":3}],"a":1}`},
		{"skip object", `{"unknown":{"nested":{"deep":true}},"a":1}`},
		{"skip string in array", `{"unknown":["a\"b"],"a":1}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out any
			if err := s.DecodeJSON([]byte(tt.input), &out); err != nil {
				t.Fatalf("input %s: %v", tt.input, err)
			}
			m := out.(map[string]any)
			if m["a"] != int32(1) {
				t.Fatalf("got %v", m)
			}
		})
	}
}

// TestDecodeJSONCustomDecoderSkipAll exercises the "no decoder matched" fallback.
func TestDecodeJSONCustomDecoderSkipAll(t *testing.T) {
	s := mustParse(t, `{"type":"long","logicalType":"custom"}`, CustomType{
		LogicalType: "custom",
		AvroType:    "long",
		Decode: func(v any, _ *SchemaNode) (any, error) {
			return nil, ErrSkipCustomType
		},
	})
	var out any
	mustDecodeJSON(t, s, []byte(`42`), &out)
	// All decoders skipped, so we get the raw int64 value.
	if out != int64(42) {
		t.Fatalf("got %v (%T)", out, out)
	}
}

// TestDecodeJSONCustomDecoderError exercises fatal custom decoder error.
func TestDecodeJSONCustomDecoderError(t *testing.T) {
	s := mustParse(t, `{"type":"long","logicalType":"boom"}`, CustomType{
		LogicalType: "boom",
		AvroType:    "long",
		Decode: func(v any, _ *SchemaNode) (any, error) {
			return nil, fmt.Errorf("kaboom")
		},
	})
	var out any
	if err := s.DecodeJSON([]byte(`42`), &out); err == nil {
		t.Fatal("expected error")
	}
}

// TestDecodeJSONCustomDecoderNilResult exercises custom decoder returning nil.
func TestDecodeJSONCustomDecoderNilResult(t *testing.T) {
	s := mustParse(t, `{"type":"long","logicalType":"nilout"}`, CustomType{
		LogicalType: "nilout",
		AvroType:    "long",
		Decode: func(v any, _ *SchemaNode) (any, error) {
			return nil, nil
		},
	})
	var out any
	mustDecodeJSON(t, s, []byte(`42`), &out)
	if out != nil {
		t.Fatalf("expected nil, got %v", out)
	}
}

// TestDecodeJSONNullTypedTargets exercises null into various typed targets.
func TestDecodeJSONNullTypedTargets(t *testing.T) {
	s, _ := Parse(`"null"`)
	t.Run("any", func(t *testing.T) {
		var v any
		mustDecodeJSON(t, s, []byte(`null`), &v)
		if v != nil {
			t.Fatal("expected nil")
		}
	})
	t.Run("map", func(t *testing.T) {
		var m map[string]any
		mustDecodeJSON(t, s, []byte(`null`), &m)
		if m != nil {
			t.Fatal("expected nil")
		}
	})
	t.Run("slice", func(t *testing.T) {
		var sl []int
		mustDecodeJSON(t, s, []byte(`null`), &sl)
		if sl != nil {
			t.Fatal("expected nil")
		}
	})
}

// TestDecodeJSONNullIntoNonPointerZeroes is the JSON sibling of
// TestDeserNullIntoNonPointerZeroes. doc.go states that a null union branch
// decodes to the target's Go zero value, always replacing any prior value: the
// binary path honors this unconditionally, while the JSON path historically
// zeroed only nilable kinds, leaving non-nilable concrete targets at whatever
// they held, a silent value-bleed footgun across reused decode targets. Covers
// decodeNull, the decodeUnion null branch, and assignAny's nil value.
func TestDecodeJSONNullIntoNonPointerZeroes(t *testing.T) {
	t.Run("top-level null", func(t *testing.T) {
		s, _ := Parse(`"null"`)
		out := 42
		mustDecodeJSON(t, s, []byte(`null`), &out)
		if out != 0 {
			t.Fatalf("top-level null into int target did not zero: got %d", out)
		}
	})

	t.Run("null in union, non-pointer struct fields", func(t *testing.T) {
		s := mustParse(t, `{"type":"record","name":"R","fields":[
			{"name":"a","type":["null","int"],"default":null},
			{"name":"b","type":["int","null"]},
			{"name":"c","type":["null","int","string"]},
			{"name":"d","type":["null","string"],"default":null}
		]}`)
		buf := mustAppendEncodeJSON(t, s, nil, map[string]any{"a": nil, "b": nil, "c": nil, "d": nil})
		type Row struct {
			A int32  `avro:"a"`
			B int32  `avro:"b"`
			C int32  `avro:"c"`
			D string `avro:"d"`
		}
		got := Row{A: 99, B: 88, C: 77, D: "prior"}
		mustDecodeJSON(t, s, buf, &got)
		want := Row{}
		if got != want {
			t.Fatalf("null decoded into pre-populated struct: got %+v, want %+v", got, want)
		}
	})

	t.Run("null in 2-branch union, bare int target", func(t *testing.T) {
		s, _ := Parse(`["null","int"]`)
		out := int32(99)
		mustDecodeJSON(t, s, []byte(`null`), &out)
		if out != 0 {
			t.Fatalf("2-branch null union did not zero int target: got %d", out)
		}
	})

	t.Run("null in 3-branch union, bare bool target", func(t *testing.T) {
		s, _ := Parse(`["null","boolean","string"]`)
		out := true
		mustDecodeJSON(t, s, []byte(`null`), &out)
		if out {
			t.Fatalf("3-branch null union did not zero bool target")
		}
	})

	t.Run("null in union, bare string target", func(t *testing.T) {
		s, _ := Parse(`["null","string"]`)
		out := "prior"
		mustDecodeJSON(t, s, []byte(`null`), &out)
		if out != "" {
			t.Fatalf("null did not zero string target: got %q", out)
		}
	})
}

// TestDecodeJSONTaggedUnionTypedTarget exercises tagged union decode to struct.
func TestDecodeJSONTaggedUnionTypedTarget(t *testing.T) {
	s, _ := Parse(`{"type":"record","name":"R","fields":[
		{"name":"v","type":["null","string"]}
	]}`)
	type R struct {
		V *string `avro:"v"`
	}
	var r R
	mustDecodeJSON(t, s, []byte(`{"v":{"string":"hello"}}`), &r)
	if r.V == nil || *r.V != "hello" {
		t.Fatalf("got %v", r.V)
	}
}

// TestDecodeJSONUnionBareTypedTarget exercises bare union into non-pointer typed target.
func TestDecodeJSONUnionBareTypedTarget(t *testing.T) {
	s, _ := Parse(`["null","string","int"]`)
	// Decode bare string into *any, already covered.
	// Decode bare int into a typed int32 target (non-pointer, multi-branch).
	var out int32
	mustDecodeJSON(t, s, []byte(`42`), &out)
	if out != 42 {
		t.Fatalf("got %d", out)
	}
}

// TestDecodeJSONParseJSONInt64EdgeCases exercises integer parsing edge cases.
func TestDecodeJSONParseJSONInt64EdgeCases(t *testing.T) {
	s, _ := Parse(`"long"`)
	tests := []struct {
		name  string
		input string
		want  int64
	}{
		{"zero", `0`, 0},
		{"negative", `-42`, -42},
		{"max int64 approx", `9223372036854775807`, math.MaxInt64},
		{"scientific whole", `1e3`, 1000},
		{"negative scientific", `-1e3`, -1000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out any
			if err := s.DecodeJSON([]byte(tt.input), &out); err != nil {
				t.Fatalf("input %s: %v", tt.input, err)
			}
			if out != tt.want {
				t.Fatalf("got %v, want %d", out, tt.want)
			}
		})
	}
	// Error cases.
	errors := []struct {
		name  string
		input string
	}{
		{"empty", ``},
		{"just minus", `-`},
		{"overflow", `99999999999999999999`},
		{"negative overflow", `-99999999999999999999`},
	}
	for _, tt := range errors {
		t.Run("error_"+tt.name, func(t *testing.T) {
			var out any
			if err := s.DecodeJSON([]byte(tt.input), &out); err == nil {
				t.Fatalf("expected error for %s", tt.input)
			}
		})
	}
}

// TestDecodeJSONWalkEscapesEdgeCases exercises remaining escape branches.
func TestDecodeJSONWalkEscapesEdgeCases(t *testing.T) {
	s, _ := Parse(`"string"`)
	tests := []struct {
		input string
		want  string
	}{
		{`"\b"`, "\b"},
		{`"\f"`, "\f"},
		{`"\r"`, "\r"},
		{`"\/"`, "/"},
		{`"\\n"`, `\n`}, // literal backslash-n
	}
	for _, tt := range tests {
		var out any
		if err := s.DecodeJSON([]byte(tt.input), &out); err != nil {
			t.Fatalf("input %s: %v", tt.input, err)
		}
		if out != tt.want {
			t.Fatalf("input %s: got %q, want %q", tt.input, out, tt.want)
		}
	}
}

// TestDecodeJSONScannerEdgeCases exercises scanner error paths.
func TestDecodeJSONScannerEdgeCases(t *testing.T) {
	t.Run("peek EOF", func(t *testing.T) {
		sc := &jsonScanner{data: []byte{}}
		if sc.peek() != 0 {
			t.Fatal("expected 0 for EOF")
		}
	})
	t.Run("expect EOF", func(t *testing.T) {
		sc := &jsonScanner{data: []byte{}}
		if err := sc.expect('{'); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("expect wrong byte", func(t *testing.T) {
		sc := &jsonScanner{data: []byte(`[`)}
		if err := sc.expect('{'); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("consumeBool not bool", func(t *testing.T) {
		sc := &jsonScanner{data: []byte(`42`)}
		if _, err := sc.consumeBool(); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("consumeStringRaw unterminated", func(t *testing.T) {
		sc := &jsonScanner{data: []byte(`"hello`)}
		if _, _, _, err := sc.consumeStringRaw(); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("consumeStringRaw not string", func(t *testing.T) {
		sc := &jsonScanner{data: []byte(`42`)}
		if _, _, _, err := sc.consumeStringRaw(); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("consumeStringRaw unterminated escape", func(t *testing.T) {
		sc := &jsonScanner{data: []byte(`"hello\`)}
		if _, _, _, err := sc.consumeStringRaw(); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("consumeNumberBytes not number", func(t *testing.T) {
		sc := &jsonScanner{data: []byte(`"hello"`)}
		if _, err := sc.consumeNumberBytes(); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("skipValue EOF", func(t *testing.T) {
		sc := &jsonScanner{data: []byte{}}
		if err := sc.skipValue(); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("consumeNull not null", func(t *testing.T) {
		sc := &jsonScanner{data: []byte(`42`)}
		if err := sc.consumeNull(); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("parseHex4 short", func(t *testing.T) {
		if _, err := parseHex4([]byte(`00`)); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("parseHex4 invalid", func(t *testing.T) {
		if _, err := parseHex4([]byte(`00GG`)); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("walkJSONEscapes unterminated", func(t *testing.T) {
		err := walkJSONEscapes([]byte(`\`), func(r rune) error { return nil })
		if err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("walkJSONEscapes short unicode", func(t *testing.T) {
		err := walkJSONEscapes([]byte(`\u00`), func(r rune) error { return nil })
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

// TestDecodeJSONFloatTypedErrors exercises float/double into wrong typed targets.
func TestDecodeJSONFloatTypedErrors(t *testing.T) {
	t.Run("float NaN string typed", func(t *testing.T) {
		s, _ := Parse(`"float"`)
		var f float32
		mustDecodeJSON(t, s, []byte(`"NaN"`), &f)
		if !math.IsNaN(float64(f)) {
			t.Fatalf("expected NaN, got %v", f)
		}
	})
	t.Run("double NaN string typed", func(t *testing.T) {
		s, _ := Parse(`"double"`)
		var f float64
		mustDecodeJSON(t, s, []byte(`"NaN"`), &f)
		if !math.IsNaN(f) {
			t.Fatalf("expected NaN, got %v", f)
		}
	})
	t.Run("float invalid string", func(t *testing.T) {
		s, _ := Parse(`"float"`)
		var out any
		if err := s.DecodeJSON([]byte(`"notanumber"`), &out); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("double invalid string", func(t *testing.T) {
		s, _ := Parse(`"double"`)
		var out any
		if err := s.DecodeJSON([]byte(`"notanumber"`), &out); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("float null typed", func(t *testing.T) {
		s, _ := Parse(`"float"`)
		var f float32
		mustDecodeJSON(t, s, []byte(`null`), &f)
		if !math.IsNaN(float64(f)) {
			t.Fatalf("expected NaN, got %v", f)
		}
	})
	t.Run("double null typed", func(t *testing.T) {
		s, _ := Parse(`"double"`)
		var f float64
		mustDecodeJSON(t, s, []byte(`null`), &f)
		if !math.IsNaN(f) {
			t.Fatalf("expected NaN, got %v", f)
		}
	})
}

// TestDecodeJSONRecordWithDefault exercises record field default handling.
func TestDecodeJSONRecordWithDefault(t *testing.T) {
	s, _ := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"int","default":99}
	]}`)
	var out any
	mustDecodeJSON(t, s, []byte(`{"a":1}`), &out)
	m := out.(map[string]any)
	if m["a"] != int32(1) {
		t.Fatalf("a: got %v", m["a"])
	}
	// b should be absent (default not populated in JSON decode to any).
}

// TestDecodeJSONMapTypedErrors exercises map decode to wrong types.
func TestDecodeJSONMapTypedErrors(t *testing.T) {
	s, _ := Parse(`{"type":"map","values":"int"}`)
	var n int
	if err := s.DecodeJSON([]byte(`{"a":1}`), &n); err == nil {
		t.Fatal("expected error decoding map into int")
	}
}

// TestDecodeJSONIterateRecordFieldsErrors exercises field decode errors.
func TestDecodeJSONIterateRecordFieldsErrors(t *testing.T) {
	s, _ := Parse(recASchema)
	var out any
	if err := s.DecodeJSON([]byte(`{"a":"notanint"}`), &out); err == nil {
		t.Fatal("expected error")
	}
}

// TestDecodeJSONWrapUnionQualifyLogical exercises the qualifyLogical branch.
func TestDecodeJSONWrapUnionQualifyLogical(t *testing.T) {
	s, _ := Parse(`["null",{"type":"long","logicalType":"timestamp-millis"}]`)
	var out any
	mustDecodeJSON(t, s, []byte(`1700000000000`), &out, TaggedUnions(), TagLogicalTypes())
	m, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("expected tagged map, got %T", out)
	}
	if _, ok := m["long.timestamp-millis"]; !ok {
		t.Fatalf("expected long.timestamp-millis key, got %v", m)
	}
}

// TestDecodeJSONLongTypedSemanticError exercises long into wrong type.
func TestDecodeJSONLongTypedSemanticError(t *testing.T) {
	s, _ := Parse(`"long"`)
	var b bool
	if err := s.DecodeJSON([]byte(`42`), &b); err == nil {
		t.Fatal("expected error")
	}
}

// TestDecodeJSONCustomDecoderInnerError exercises custom decoder with bad JSON.
func TestDecodeJSONCustomDecoderInnerError(t *testing.T) {
	s := mustParse(t, `{"type":"long","logicalType":"custom"}`, CustomType{LogicalType: "custom", AvroType: "long",
		Decode: func(v any, _ *SchemaNode) (any, error) { return v, nil },
	})
	var out any
	if err := s.DecodeJSON([]byte(`"notanumber"`), &out); err == nil {
		t.Fatal("expected error from inner decode")
	}
}

// TestDecodeJSONFixedScanError exercises malformed fixed/bytes string.
func TestDecodeJSONFixedScanError(t *testing.T) {
	s, _ := Parse(`{"type":"fixed","name":"f","size":3}`)
	var out any
	// Not a string, so this errors.
	if err := s.DecodeJSON([]byte(`42`), &out); err == nil {
		t.Fatal("expected error")
	}
}

// TestDecodeJSONAssignBytesError exercises bytes into wrong typed target.
func TestDecodeJSONAssignBytesError(t *testing.T) {
	s, _ := Parse(`"bytes"`)
	var n int
	if err := s.DecodeJSON([]byte(`"hello"`), &n); err == nil {
		t.Fatal("expected error")
	}
}

// TestDecodeJSONEmptyArrayAny exercises empty array to *any.
func TestDecodeJSONEmptyArrayAny(t *testing.T) {
	s, _ := Parse(`{"type":"array","items":"int"}`)
	var out any
	mustDecodeJSON(t, s, []byte(`[]`), &out)
	arr := out.([]any)
	if len(arr) != 0 {
		t.Fatalf("expected empty array, got %v", arr)
	}
}

// TestDecodeJSONArrayTypedError exercises typed array item error.
func TestDecodeJSONArrayTypedError(t *testing.T) {
	s, _ := Parse(`{"type":"array","items":"int"}`)
	var out []int32
	if err := s.DecodeJSON([]byte(`["notanint"]`), &out); err == nil {
		t.Fatal("expected error")
	}
}

// TestDecodeJSONMapAnyErrors exercises map-to-any error paths.
func TestDecodeJSONMapAnyErrors(t *testing.T) {
	s, _ := Parse(`{"type":"map","values":"int"}`)
	var out any
	// Bad key (truncated).
	if err := s.DecodeJSON([]byte(`{"a`), &out); err == nil {
		t.Fatal("expected error for truncated key")
	}
	// Bad colon.
	if err := s.DecodeJSON([]byte(`{"a" 1}`), &out); err == nil {
		t.Fatal("expected error for missing colon")
	}
	// Bad value.
	if err := s.DecodeJSON([]byte(`{"a":"notanint"}`), &out); err == nil {
		t.Fatal("expected error for wrong value type")
	}
	// Unclosed.
	if err := s.DecodeJSON([]byte(`{"a":1`), &out); err == nil {
		t.Fatal("expected error for unclosed map")
	}
}

// TestDecodeJSONMapTypedErrors2 exercises typed map error paths.
func TestDecodeJSONMapTypedErrors2(t *testing.T) {
	s, _ := Parse(`{"type":"map","values":"int"}`)
	var m map[string]int32
	// Bad key.
	if err := s.DecodeJSON([]byte(`{42:1}`), &m); err == nil {
		t.Fatal("expected error")
	}
	// Bad colon.
	if err := s.DecodeJSON([]byte(`{"a" 1}`), &m); err == nil {
		t.Fatal("expected error")
	}
	// Bad value type.
	if err := s.DecodeJSON([]byte(`{"a":"notanint"}`), &m); err == nil {
		t.Fatal("expected error")
	}
}

// TestDecodeJSONRecordFieldErrors exercises record field key/colon/skip errors.
func TestDecodeJSONRecordFieldErrors(t *testing.T) {
	s, _ := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	var out any
	// Truncated key.
	if err := s.DecodeJSON([]byte(`{"a`), &out); err == nil {
		t.Fatal("expected error")
	}
	// Missing colon.
	if err := s.DecodeJSON([]byte(`{"a" 1}`), &out); err == nil {
		t.Fatal("expected error")
	}
	// Skip error (truncated unknown value).
	if err := s.DecodeJSON([]byte(`{"unknown":`), &out); err == nil {
		t.Fatal("expected error")
	}
	// Unclosed record.
	if err := s.DecodeJSON([]byte(`{"a":1`), &out); err == nil {
		t.Fatal("expected error")
	}
}

// TestDecodeJSONRecordMapFieldError exercises decodeRecordMap field error.
func TestDecodeJSONRecordMapFieldError(t *testing.T) {
	s, _ := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	var m map[string]int32
	if err := s.DecodeJSON([]byte(`{"a":"notanint"}`), &m); err == nil {
		t.Fatal("expected error")
	}
}

// TestDecodeJSONRecordStructMissingDeserRecord exercises the nil deserRecord path.
func TestDecodeJSONRecordStructMissingDeserRecord(t *testing.T) {
	// This can't happen through Parse, so test the decoder directly.
	node := &schemaNode{kind: "record", fields: []fieldNode{{name: "a"}}}
	// No deserRecord set.
	ctx := &jsonDecoder{scanner: &jsonScanner{data: []byte(`{"a":1}`)}}
	ctx.scanner.pos = 1 // past the '{'
	type R struct {
		A int `avro:"a"`
	}
	var r R
	err := ctx.decodeRecordStruct(reflect.ValueOf(&r).Elem(), node)
	if err == nil {
		t.Fatal("expected error for missing deserRecord")
	}
}

// TestDecodeJSONUnionNullConsumeError exercises null branch with bad JSON.
func TestDecodeJSONUnionNullConsumeError(t *testing.T) {
	s, _ := Parse(`["null","string"]`)
	var out any
	// "nul" is truncated null.
	if err := s.DecodeJSON([]byte(`nul`), &out); err == nil {
		t.Fatal("expected error")
	}
}

// TestDecodeJSONScanParseInt64EdgeCases covers remaining parseJSONInt64 branches.
func TestDecodeJSONScanParseInt64EdgeCases(t *testing.T) {
	// These go through the scanner directly.
	tests := []struct {
		name    string
		input   []byte
		wantErr bool
	}{
		{"empty", []byte{}, true},
		{"just minus", []byte("-"), true},
		{"invalid char", []byte("12x4"), true},
		{"negative overflow", []byte("-9999999999999999999999"), true},
		{"positive overflow", []byte("99999999999999999999"), true},
		{"invalid float", []byte("1e1e1"), true},
		{"valid", []byte("42"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseJSONInt64(tt.input)
			if tt.wantErr && err == nil {
				t.Fatal("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestDecodeJSONWalkEscapesEmitError exercises the emit callback error path.
func TestDecodeJSONWalkEscapesEmitError(t *testing.T) {
	err := walkJSONEscapes([]byte("abc"), func(r rune) error {
		return fmt.Errorf("stop")
	})
	if err == nil {
		t.Fatal("expected error from emit callback")
	}
}

// TestDecodeJSONWalkEscapesDefaultChar exercises the default arm: an
// unrecognized escape (\x is not one of JSON's eight escapes) is rejected,
// not silently decoded with the backslash dropped.
func TestDecodeJSONWalkEscapesDefaultChar(t *testing.T) {
	err := walkJSONEscapes([]byte{'\\', 'x'}, func(r rune) error {
		t.Fatalf("emit should not be called for an invalid escape; got %q", r)
		return nil
	})
	if err == nil {
		t.Fatal("expected error for invalid escape sequence \\x")
	}
}

// TestResolveJSONEscapesError exercises resolveJSONEscapes error path.
func TestResolveJSONEscapesError(t *testing.T) {
	// Short \u escape.
	_, err := resolveJSONEscapes([]byte(`\u00`))
	if err == nil {
		t.Fatal("expected error")
	}
}

// TestParseHex4Lowercase exercises lowercase hex digits.
func TestParseHex4Lowercase(t *testing.T) {
	r, err := parseHex4([]byte("00ff"))
	if err != nil {
		t.Fatal(err)
	}
	if r != 0xFF {
		t.Fatalf("got %d", r)
	}
}

// TestScanAvroJSONBytesError exercises scanAvroJSONBytes emit error.
func TestScanAvroJSONBytesError(t *testing.T) {
	// Code point > 255 via \u escape.
	s, _ := Parse(`"bytes"`)
	var out any
	if err := s.DecodeJSON([]byte(`"\u0100"`), &out); err == nil {
		t.Fatal("expected error for code point > 255")
	}
}

// TestSkipCompoundUnterminated exercises unterminated object/array skip.
func TestSkipCompoundUnterminated(t *testing.T) {
	s, _ := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	var out any
	if err := s.DecodeJSON([]byte(`{"unknown":{"nested":true`), &out); err == nil {
		t.Fatal("expected error for unterminated nested object")
	}
}

// TestDecodeJSONFloatOverflowIsInfinity verifies that numeric overflow
// (e.g. 1e999, goavro's convention for Infinity) decodes as ±Inf.
func TestDecodeJSONFloatOverflowIsInfinity(t *testing.T) {
	s, _ := Parse(`"float"`)
	var out any
	if err := s.DecodeJSON([]byte(`1e999`), &out); err != nil {
		t.Fatalf("expected +Inf, got error: %v", err)
	}
	if !math.IsInf(float64(out.(float32)), 1) {
		t.Fatalf("expected +Inf, got %v", out)
	}
	out = nil
	if err := s.DecodeJSON([]byte(`-1e999`), &out); err != nil {
		t.Fatalf("expected -Inf, got error: %v", err)
	}
	if !math.IsInf(float64(out.(float32)), -1) {
		t.Fatalf("expected -Inf, got %v", out)
	}
}

// TestDecodeJSONNestedStructures exercises deeply nested decode.
func TestDecodeJSONNestedStructures(t *testing.T) {
	t.Run("array of records", func(t *testing.T) {
		s, _ := Parse(`{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}}`)
		var out any
		mustDecodeJSON(t, s, []byte(`[{"x":1},{"x":2}]`), &out)
		arr := out.([]any)
		if len(arr) != 2 {
			t.Fatalf("got %d items", len(arr))
		}
	})
	t.Run("map of arrays", func(t *testing.T) {
		s, _ := Parse(`{"type":"map","values":{"type":"array","items":"string"}}`)
		var out any
		mustDecodeJSON(t, s, []byte(`{"a":["x","y"],"b":["z"]}`), &out)
	})
	t.Run("record with union of record", func(t *testing.T) {
		s, _ := Parse(`{"type":"record","name":"Outer","fields":[
			{"name":"inner","type":["null",{"type":"record","name":"Inner","fields":[
				{"name":"v","type":"int"}
			]}]}
		]}`)
		var out any
		mustDecodeJSON(t, s, []byte(`{"inner":{"Inner":{"v":42}}}`), &out)
		mustDecodeJSON(t, s, []byte(`{"inner":null}`), &out)
	})
	t.Run("nested struct decode", func(t *testing.T) {
		type Inner struct {
			X int32  `avro:"x"`
			Y string `avro:"y"`
		}
		type Outer struct {
			Inner Inner `avro:"inner"`
			Z     int64 `avro:"z"`
		}
		s, _ := Parse(`{"type":"record","name":"Outer","fields":[
			{"name":"inner","type":{"type":"record","name":"Inner","fields":[
				{"name":"x","type":"int"},{"name":"y","type":"string"}
			]}},
			{"name":"z","type":"long"}
		]}`)
		var out Outer
		mustDecodeJSON(t, s, []byte(`{"inner":{"x":1,"y":"hello"},"z":99}`), &out)
		if out.Inner.X != 1 || out.Inner.Y != "hello" || out.Z != 99 {
			t.Fatalf("got %+v", out)
		}
	})
}

// TestEncodeJSONTimeDurationError exercises time-millis EncodeJSON overflow.
func TestEncodeJSONTimeDurationError(t *testing.T) {
	s, _ := Parse(`{"type":"int","logicalType":"time-millis"}`)
	huge := time.Duration(math.MaxInt32+1) * time.Millisecond
	if _, err := s.EncodeJSON(huge); err == nil {
		t.Fatal("expected overflow error")
	}
}

// TestJsonNumberToInt64Overflow exercises the jsonNumberToInt64 overflow path.
func TestJsonNumberToInt64Overflow(t *testing.T) {
	s, _ := Parse(`"long"`)
	// json.Number with a value that parses as float but overflows int64.
	_, err := s.Encode(json.Number("1e25"))
	if err == nil {
		t.Fatal("expected overflow error from json.Number")
	}
}

// TestEncodeJSONUnionAllBranchesFail exercises the EncodeJSON union fallthrough.
func TestEncodeJSONUnionAllBranchesFail(t *testing.T) {
	s, _ := Parse(`["null","int"]`)
	// A value that matches no union branch in EncodeJSON.
	_, err := s.EncodeJSON([]string{"not", "a", "union", "match"})
	if err == nil {
		t.Fatal("expected error")
	}
}

// TestSchemaNodePropsInToJSON exercises schema node Props propagation.
func TestSchemaNodePropsInToJSON(t *testing.T) {
	s, _ := Parse(`{"type":"int","logicalType":"date","connect.name":"io.debezium.time.Date"}`)
	root := s.Root()
	if root.Props == nil || root.Props["connect.name"] != "io.debezium.time.Date" {
		t.Fatalf("expected props, got %v", root.Props)
	}
	// Exercise Schema() which goes through toJSON.
	mustNodeSchema(t, root)
}

// TestSchemaNodeFieldPropsInToJSON exercises field-level props in toJSON.
func TestSchemaNodeFieldPropsInToJSON(t *testing.T) {
	s, _ := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int","connect.name":"custom"}
	]}`)
	root := s.Root()
	if len(root.Fields) == 0 {
		t.Fatal("no fields")
	}
	f := root.Fields[0]
	if f.Props == nil || f.Props["connect.name"] != "custom" {
		t.Fatalf("expected field props, got %v", f.Props)
	}
}

// TestDecodeJSONLogicalTypesNonAddressable exercises the reflect.ValueOf
// fallback paths for non-addressable time.Time/time.Duration targets.
// Map values are non-addressable, so decoding into map[string]time.Time
// hits the fallback.
func TestDecodeJSONLogicalTypesNonAddressable(t *testing.T) {
	t.Run("date into map value", func(t *testing.T) {
		s, _ := Parse(`{"type":"map","values":{"type":"int","logicalType":"date"}}`)
		var m map[string]time.Time
		mustDecodeJSON(t, s, []byte(`{"d":19700}`), &m)
		if m["d"].IsZero() {
			t.Fatal("expected non-zero time")
		}
	})
	t.Run("time-millis into map value", func(t *testing.T) {
		s, _ := Parse(`{"type":"map","values":{"type":"int","logicalType":"time-millis"}}`)
		var m map[string]time.Duration
		mustDecodeJSON(t, s, []byte(`{"t":43200000}`), &m)
		if m["t"] == 0 {
			t.Fatal("expected non-zero duration")
		}
	})
	t.Run("timestamp-millis into map value", func(t *testing.T) {
		s, _ := Parse(`{"type":"map","values":{"type":"long","logicalType":"timestamp-millis"}}`)
		var m map[string]time.Time
		mustDecodeJSON(t, s, []byte(`{"ts":1700000000000}`), &m)
		if m["ts"].IsZero() {
			t.Fatal("expected non-zero time")
		}
	})
	t.Run("timestamp-micros into map value", func(t *testing.T) {
		s, _ := Parse(`{"type":"map","values":{"type":"long","logicalType":"timestamp-micros"}}`)
		var m map[string]time.Time
		mustDecodeJSON(t, s, []byte(`{"ts":1700000000000000}`), &m)
	})
	t.Run("timestamp-nanos into map value", func(t *testing.T) {
		s, _ := Parse(`{"type":"map","values":{"type":"long","logicalType":"timestamp-nanos"}}`)
		var m map[string]time.Time
		mustDecodeJSON(t, s, []byte(`{"ts":1700000000000000000}`), &m)
	})
	t.Run("time-micros into map value", func(t *testing.T) {
		s, _ := Parse(`{"type":"map","values":{"type":"long","logicalType":"time-micros"}}`)
		var m map[string]time.Duration
		mustDecodeJSON(t, s, []byte(`{"t":1500000}`), &m)
		if m["t"] == 0 {
			t.Fatal("expected non-zero duration")
		}
	})
}

// TestDecodeJSONStringWithEscapes exercises the resolveJSONEscapes path
// in consumeString and consumeStringZeroCopy.
func TestDecodeJSONStringWithEscapes(t *testing.T) {
	// String value with escapes (consumeSlabString into resolveJSONEscapes).
	s, _ := Parse(`"string"`)
	var out any
	mustDecodeJSON(t, s, []byte(`"hello\tworld"`), &out)
	if out != "hello\tworld" {
		t.Fatalf("got %q", out)
	}

	// Map key with escapes (consumeSlabString into resolveJSONEscapes).
	sm, _ := Parse(`{"type":"map","values":"int"}`)
	var mout any
	mustDecodeJSON(t, sm, []byte(`{"key\twith\ttabs":42}`), &mout)
	m := mout.(map[string]any)
	if _, ok := m["key\twith\ttabs"]; !ok {
		t.Fatalf("expected key with tabs, got %v", m)
	}
}

// TestDecodeJSONDoubleOverflowIsInfinity verifies that numeric overflow
// (e.g. 1e999, goavro's convention for Infinity) decodes as ±Inf.
func TestDecodeJSONDoubleOverflowIsInfinity(t *testing.T) {
	s, _ := Parse(`"double"`)
	var out any
	if err := s.DecodeJSON([]byte(`1e999`), &out); err != nil {
		t.Fatalf("expected +Inf, got error: %v", err)
	}
	if !math.IsInf(out.(float64), 1) {
		t.Fatalf("expected +Inf, got %v", out)
	}
	out = nil
	if err := s.DecodeJSON([]byte(`-1e999`), &out); err != nil {
		t.Fatalf("expected -Inf, got error: %v", err)
	}
	if !math.IsInf(out.(float64), -1) {
		t.Fatalf("expected -Inf, got %v", out)
	}
}

// TestDecodeJSONFloatInvalidNaNString exercises bad NaN/Infinity string.
func TestDecodeJSONFloatInvalidNaNString(t *testing.T) {
	s, _ := Parse(`"float"`)
	var out any
	if err := s.DecodeJSON([]byte(`"NotASpecialFloat"`), &out); err == nil {
		t.Fatal("expected error for invalid float string")
	}
}

// TestDecodeJSONDoubleInvalidNaNString exercises bad NaN/Infinity string.
func TestDecodeJSONDoubleInvalidNaNString(t *testing.T) {
	s, _ := Parse(`"double"`)
	var out any
	if err := s.DecodeJSON([]byte(`"NotASpecialDouble"`), &out); err == nil {
		t.Fatal("expected error for invalid double string")
	}
}

// TestDecodeJSONFloatTruncatedNull exercises truncated null in float context.
func TestDecodeJSONFloatTruncatedNull(t *testing.T) {
	s, _ := Parse(`"float"`)
	var out any
	if err := s.DecodeJSON([]byte(`nul`), &out); err == nil {
		t.Fatal("expected error for truncated null")
	}
}

// TestDecodeJSONDoubleTruncatedNull exercises truncated null in double context.
func TestDecodeJSONDoubleTruncatedNull(t *testing.T) {
	s, _ := Parse(`"double"`)
	var out any
	if err := s.DecodeJSON([]byte(`nul`), &out); err == nil {
		t.Fatal("expected error for truncated null")
	}
}

// TestDecodeJSONFixedBadEscape exercises invalid escape in fixed field.
func TestDecodeJSONFixedBadEscape(t *testing.T) {
	s, _ := Parse(`{"type":"fixed","name":"f","size":3}`)
	var out any
	// Truncated \u escape inside fixed string.
	if err := s.DecodeJSON([]byte(`"\u00"`), &out); err == nil {
		t.Fatal("expected error for bad escape in fixed")
	}
}

// TestDecodeJSONArrayAnyTruncated exercises truncated array for *any.
func TestDecodeJSONArrayAnyTruncated(t *testing.T) {
	s, _ := Parse(`{"type":"array","items":"int"}`)
	var out any
	if err := s.DecodeJSON([]byte(`[1,2`), &out); err == nil {
		t.Fatal("expected error for truncated array")
	}
}

// TestDecodeJSONRecordStructBadFieldMapping exercises struct with wrong field types.
func TestDecodeJSONRecordStructBadFieldMapping(t *testing.T) {
	s, _ := Parse(recABSchema)
	// Struct where field "a" is tagged but wrong avro field name won't match.
	// Use a struct with no matching fields to trigger mapping error.
	type Bad struct {
		X int `avro:"-"`
	}
	var b Bad
	if err := s.DecodeJSON([]byte(`{"a":1,"b":"hello"}`), &b); err == nil {
		t.Fatal("expected error for unmappable struct")
	}
}

// TestDecodeJSONParseInt64NegativeOverflow exercises negative overflow.
func TestDecodeJSONParseInt64NegativeOverflow(t *testing.T) {
	// A number that overflows int64 in the negative direction.
	_, err := parseJSONInt64([]byte("-9999999999999999999999"))
	if err == nil {
		t.Fatal("expected overflow error")
	}
	// Also test the n > MaxInt64 path for positive numbers.
	_, err = parseJSONInt64([]byte("9999999999999999999"))
	if err == nil {
		t.Fatal("expected overflow error")
	}
}

// TestDecodeJSONFloatUnterminatedString exercises unterminated string in float.
func TestDecodeJSONFloatUnterminatedString(t *testing.T) {
	s, _ := Parse(`"float"`)
	var out any
	if err := s.DecodeJSON([]byte(`"NaN`), &out); err == nil {
		t.Fatal("expected error for unterminated string")
	}
}

// TestDecodeJSONDoubleUnterminatedString exercises unterminated string in double.
func TestDecodeJSONDoubleUnterminatedString(t *testing.T) {
	s, _ := Parse(`"double"`)
	var out any
	if err := s.DecodeJSON([]byte(`"NaN`), &out); err == nil {
		t.Fatal("expected error for unterminated string")
	}
}

// TestParseJSONInt64NegOverflowExact exercises the exact -2^63-1 case.
func TestParseJSONInt64NegOverflowExact(t *testing.T) {
	// -9223372036854775809 is -(2^63 + 1), overflows int64.
	_, err := parseJSONInt64([]byte("-9223372036854775809"))
	if err == nil {
		t.Fatal("expected overflow")
	}
}

// TestDecodeJSONWalkEscapesBadHex exercises bad hex in \u inside walkJSONEscapes.
func TestDecodeJSONWalkEscapesBadHex(t *testing.T) {
	s, _ := Parse(`"string"`)
	var out any
	// \uXXGG, invalid hex digits.
	if err := s.DecodeJSON([]byte(`"\u00GG"`), &out); err == nil {
		t.Fatal("expected error for bad hex in \\u")
	}
}

// TestDecodeJSONLogicalTyped exercises typed target paths for logical
// types in decodeInt, decodeLong, decodeFixed, and decodeBytes.
func TestDecodeJSONLogicalTyped(t *testing.T) {
	// date decodes to time.Time
	t.Run("date to time.Time", func(t *testing.T) {
		s := MustParse(`{"type":"int","logicalType":"date"}`)
		var tm time.Time
		mustDecodeJSON(t, s, []byte("18262"), &tm)
	})
	// time-millis decodes to time.Duration
	t.Run("time-millis to time.Duration", func(t *testing.T) {
		s := MustParse(`{"type":"int","logicalType":"time-millis"}`)
		var d time.Duration
		mustDecodeJSON(t, s, []byte("12345"), &d)
	})
	// timestamp-millis decodes to time.Time (each variant)
	for _, lt := range []string{"timestamp-millis", "local-timestamp-millis"} {
		t.Run(lt+" to time.Time", func(t *testing.T) {
			s := MustParse(`{"type":"long","logicalType":"` + lt + `"}`)
			var tm time.Time
			mustDecodeJSON(t, s, []byte("1577880645000"), &tm)
		})
	}
	for _, lt := range []string{"timestamp-micros", "local-timestamp-micros"} {
		t.Run(lt+" to time.Time", func(t *testing.T) {
			s := MustParse(`{"type":"long","logicalType":"` + lt + `"}`)
			var tm time.Time
			mustDecodeJSON(t, s, []byte("1577880645000000"), &tm)
		})
	}
	for _, lt := range []string{"timestamp-nanos", "local-timestamp-nanos"} {
		t.Run(lt+" to time.Time", func(t *testing.T) {
			s := MustParse(`{"type":"long","logicalType":"` + lt + `"}`)
			var tm time.Time
			mustDecodeJSON(t, s, []byte("1577880645000000000"), &tm)
		})
	}
	t.Run("time-micros to time.Duration", func(t *testing.T) {
		s := MustParse(`{"type":"long","logicalType":"time-micros"}`)
		var d time.Duration
		mustDecodeJSON(t, s, []byte("12345"), &d)
	})
	// long to int/uint targets
	t.Run("long to int", func(t *testing.T) {
		s := MustParse(`"long"`)
		var n int
		mustDecodeJSON(t, s, []byte("42"), &n)
	})
	t.Run("long to uint", func(t *testing.T) {
		s := MustParse(`"long"`)
		var n uint
		mustDecodeJSON(t, s, []byte("42"), &n)
	})
	t.Run("long to unsupported errors", func(t *testing.T) {
		s := MustParse(`"long"`)
		var f string
		if err := s.DecodeJSON([]byte("42"), &f); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("int to uint", func(t *testing.T) {
		s := MustParse(`"int"`)
		var n uint
		mustDecodeJSON(t, s, []byte("42"), &n)
	})
	t.Run("int to unsupported errors", func(t *testing.T) {
		s := MustParse(`"int"`)
		var f string
		if err := s.DecodeJSON([]byte("42"), &f); err == nil {
			t.Fatal("expected error")
		}
	})
}

// TestDecodeJSONErrorPaths exercises error branches in decodeEnum,
// decodeBytes, decodeFixed, decodeFloat, decodeDouble.
func TestDecodeJSONErrorPaths(t *testing.T) {
	// enum errors
	enumS := MustParse(`{"type":"enum","name":"E","symbols":["A","B"]}`)
	if err := enumS.DecodeJSON([]byte(`"C"`), new(string)); err == nil {
		t.Error("expected unknown symbol error")
	}
	// int / uint targets are accepted as the ordinal, in binary parity; see
	// TestMatrix_JSONEnumDecodeIntoIntTargetParity. Only genuinely
	// unsupported targets (channel, slice, etc.) error.
	if err := enumS.DecodeJSON([]byte(`"A"`), new([]int)); err == nil {
		t.Error("expected unsupported target error for slice")
	}
	// decodeBytes decimal invalid number
	bytesDecS := MustParse(`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)
	if err := bytesDecS.DecodeJSON([]byte("not_a_number"), new(json.Number)); err == nil {
		t.Error("expected invalid decimal error")
	}
	// decodeFixed decimal invalid number
	fixedDecS := MustParse(`{"type":"fixed","name":"d","size":8,"logicalType":"decimal","precision":10,"scale":2}`)
	if err := fixedDecS.DecodeJSON([]byte("not_a_number"), new(json.Number)); err == nil {
		t.Error("expected invalid decimal error")
	}
	// decodeFloat invalid
	floatS := MustParse(`"float"`)
	if err := floatS.DecodeJSON([]byte(`"not a float"`), new(float32)); err == nil {
		t.Error("expected invalid float error")
	}
	// decodeDouble invalid
	doubleS := MustParse(`"double"`)
	if err := doubleS.DecodeJSON([]byte(`"not a double"`), new(float64)); err == nil {
		t.Error("expected invalid double error")
	}
	// An int into a float target is supported, in round-trip parity with the
	// documented encode-side whole-number divergence; see
	// TestMatrix_IntLongDecodeIntoFloatJSONNumber. Genuinely unsupported
	// targets (slice, struct without method, etc.) still error.
	intS := MustParse(`"int"`)
	if err := intS.DecodeJSON([]byte(`42`), new([]int)); err == nil {
		t.Error("expected unsupported target for int (slice)")
	}
	// fixed decimal with typed unsupported target
	if err := fixedDecS.DecodeJSON([]byte("12.34"), new(int)); err == nil {
		t.Error("expected error for unsupported target")
	}
}

// TestDecodeJSONMapTimeValues exercises the non-addressable time.Time/
// time.Duration paths in decodeInt and decodeLong (map values are not
// addressable).
func TestDecodeJSONMapTimeValues(t *testing.T) {
	// map with time.Time values (timestamp-micros)
	t.Run("map timestamp-millis time.Time", func(t *testing.T) {
		s := MustParse(`{"type":"map","values":{"type":"long","logicalType":"timestamp-millis"}}`)
		var m map[string]time.Time
		mustDecodeJSON(t, s, []byte(`{"a":1577880645000}`), &m)
	})
	t.Run("map timestamp-micros time.Time", func(t *testing.T) {
		s := MustParse(`{"type":"map","values":{"type":"long","logicalType":"timestamp-micros"}}`)
		var m map[string]time.Time
		mustDecodeJSON(t, s, []byte(`{"a":1577880645000000}`), &m)
	})
	t.Run("map timestamp-nanos time.Time", func(t *testing.T) {
		s := MustParse(`{"type":"map","values":{"type":"long","logicalType":"timestamp-nanos"}}`)
		var m map[string]time.Time
		mustDecodeJSON(t, s, []byte(`{"a":1577880645000000000}`), &m)
	})
	t.Run("map time-micros time.Duration", func(t *testing.T) {
		s := MustParse(`{"type":"map","values":{"type":"long","logicalType":"time-micros"}}`)
		var m map[string]time.Duration
		mustDecodeJSON(t, s, []byte(`{"a":12345}`), &m)
	})
	t.Run("map date time.Time", func(t *testing.T) {
		s := MustParse(`{"type":"map","values":{"type":"int","logicalType":"date"}}`)
		var m map[string]time.Time
		mustDecodeJSON(t, s, []byte(`{"a":18262}`), &m)
	})
	t.Run("map time-millis time.Duration", func(t *testing.T) {
		s := MustParse(`{"type":"map","values":{"type":"int","logicalType":"time-millis"}}`)
		var m map[string]time.Duration
		mustDecodeJSON(t, s, []byte(`{"a":12345}`), &m)
	})
}

// DecodeJSON must honor TaggedUnions / TagLogicalTypes for a union field
// filled from its default (absent in the input) exactly as it does for a
// present union field, and exactly as Schema.Decode (binary), resolved
// DecodeJSON, and EncodeJSON already do. The default-fill path routes through
// the binary deser fn, which reads the slab's taggedUnions flag; DecodeJSON
// populated only the jsonDecoder's wrapUnions field and left the slab flag at
// the pool default, so the envelope was dropped on default-filled fields only.
func TestRegression_DecodeJSONTaggedUnionDefaultFill(t *testing.T) {
	s := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"f","type":["null","string"],"default":"hello"},
		{"name":"g","type":"int"}]}`)

	t.Run("present field wraps", func(t *testing.T) {
		var out map[string]any
		if err := s.DecodeJSON([]byte(`{"f":{"string":"world"},"g":1}`), &out, TaggedUnions()); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got, ok := out["f"].(map[string]any); !ok || got["string"] != "world" {
			t.Fatalf("present union field: got %#v, want {\"string\":\"world\"}", out["f"])
		}
	})
	t.Run("default-filled field wraps", func(t *testing.T) {
		var out map[string]any
		if err := s.DecodeJSON([]byte(`{"g":1}`), &out, TaggedUnions()); err != nil {
			t.Fatalf("decode: %v", err)
		}
		got, ok := out["f"].(map[string]any)
		if !ok || got["string"] != "hello" {
			t.Fatalf("default-filled union field: got %#v (%T), want {\"string\":\"hello\"}", out["f"], out["f"])
		}
	})

	// TagLogicalTypes: a default-filled logical union field tags with the
	// qualified name, not the bare logical string.
	sl := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"t","type":["null",{"type":"long","logicalType":"timestamp-millis"}],"default":null},
		{"name":"g","type":"int"}]}`)
	t.Run("logical present field tags", func(t *testing.T) {
		var out map[string]any
		if err := sl.DecodeJSON([]byte(`{"t":{"long.timestamp-millis":0},"g":1}`), &out, TaggedUnions(), TagLogicalTypes()); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if _, ok := out["t"].(map[string]any)["long.timestamp-millis"]; !ok {
			t.Fatalf("present logical union: got %#v", out["t"])
		}
	})
}

// TestDecodeJSONTaggedUnionTypedInterfaceTarget verifies that DecodeJSON
// under TaggedUnions handles a non-empty interface target exactly like
// binary Decode: the {branch: value} envelope applies only to targets
// that map[string]any is assignable to, and is skipped silently for
// every other interface target (deserUnion.maybeWrap's rule), so the
// decoded branch value lands bare. Without the skip, a union value that
// satisfies the caller's interface decodes through binary but errors
// through JSON on the same option.
func TestDecodeJSONTaggedUnionTypedInterfaceTarget(t *testing.T) {
	type nanoer interface{ UnixNano() int64 } // satisfied by time.Time

	s := mustParse(t, `["null",{"type":"long","logicalType":"timestamp-millis"}]`)
	want := time.UnixMilli(5).UTC()

	// Binary reference: index 1 + long 5; the wrap is skipped silently.
	wire := mustEncode(t, s, want)
	var bin nanoer
	mustDecode(t, s, wire, &bin, TaggedUnions())
	if !bin.(time.Time).Equal(want) {
		t.Fatalf("binary decode got %#v, want %v", bin, want)
	}

	t.Run("tagged_input", func(t *testing.T) {
		var got nanoer
		if err := s.DecodeJSON([]byte(`{"long":5}`), &got, TaggedUnions()); err != nil {
			t.Fatalf("non-empty interface target must skip the tagged wrap silently like binary Decode: %v", err)
		}
		if !got.(time.Time).Equal(want) {
			t.Fatalf("got %#v, want bare %v", got, want)
		}
	})

	t.Run("bare_input", func(t *testing.T) {
		// The documented bare-union leniency composes with the same skip.
		var got nanoer
		if err := s.DecodeJSON([]byte(`5`), &got, TaggedUnions()); err != nil {
			t.Fatalf("non-empty interface target must skip the tagged wrap silently like binary Decode: %v", err)
		}
		if !got.(time.Time).Equal(want) {
			t.Fatalf("got %#v, want bare %v", got, want)
		}
	})

	t.Run("any_still_wrapped", func(t *testing.T) {
		var got any
		mustDecodeJSON(t, s, []byte(`{"long":5}`), &got, TaggedUnions())
		m, ok := got.(map[string]any)
		if !ok || !m["long"].(time.Time).Equal(want) {
			t.Fatalf("expected {\"long\": %v} envelope for *any, got %#v", want, got)
		}
	})

	t.Run("untagged_off", func(t *testing.T) {
		var got nanoer
		mustDecodeJSON(t, s, []byte(`5`), &got)
		if !got.(time.Time).Equal(want) {
			t.Fatalf("got %#v, want bare %v", got, want)
		}
	})
}
