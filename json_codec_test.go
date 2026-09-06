package avro

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
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

func TestDecodeTaggedUnionsNullAtRoot(t *testing.T) {
	s := mustParse(t, `["null","string"]`)
	bin := mustEncode(t, s, nil)
	var got any
	mustDecode(t, s, bin, &got, TaggedUnions())
	if got != nil {
		t.Errorf("expected nil, got %v", got)
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

func TestEncodeJSONRecordMissingRequiredField(t *testing.T) {
	schema := recABSchema
	s := mustParse(t, schema)
	// Missing required field "b".
	_, err := s.EncodeJSON(map[string]any{"a": int32(1)})
	if err == nil {
		t.Fatal("expected error for missing required field")
	}
}

func TestEncodeJSONBytesFromString(t *testing.T) {
	s := mustParse(t, `"bytes"`)
	got := mustEncodeJSON(t, s, "hello")
	if string(got) != `"hello"` {
		t.Errorf("got %s", got)
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

func TestEncodeJSONFixedFromString(t *testing.T) {
	s := mustParse(t, `{"type":"fixed","name":"F","size":5}`)
	got := mustEncodeJSON(t, s, "hello")
	if string(got) != `"hello"` {
		t.Errorf("got %s", got)
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

// ---------- json_decode_test.go ----------

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
