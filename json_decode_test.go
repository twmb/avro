package avro

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"testing"
	"time"
)

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
	if err := s.DecodeJSON([]byte(`{"d":19700,"tm":43200000,"n":42}`), &r); err != nil {
		t.Fatal(err)
	}
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
	if err := s.DecodeJSON([]byte(`{"ts_ms":1700000000000,"ts_us":1700000000000000,"ts_ns":1700000000000000000,"tm":1500000,"n":99}`), &r); err != nil {
		t.Fatal(err)
	}
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
		if err := s.DecodeJSON([]byte(`"hello"`), &b); err != nil {
			t.Fatal(err)
		}
		if string(b) != "hello" {
			t.Fatalf("got %q", b)
		}
	})
	t.Run("bytes to string", func(t *testing.T) {
		s, _ := Parse(`"bytes"`)
		var str string
		if err := s.DecodeJSON([]byte(`"hello"`), &str); err != nil {
			t.Fatal(err)
		}
		if str != "hello" {
			t.Fatalf("got %q", str)
		}
	})
	t.Run("fixed to [N]byte", func(t *testing.T) {
		s, _ := Parse(`{"type":"fixed","name":"f","size":3}`)
		var arr [3]byte
		if err := s.DecodeJSON([]byte(`"abc"`), &arr); err != nil {
			t.Fatal(err)
		}
		if arr != [3]byte{'a', 'b', 'c'} {
			t.Fatalf("got %v", arr)
		}
	})
	t.Run("decimal to json.Number", func(t *testing.T) {
		s, _ := Parse(`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)
		var n json.Number
		if err := s.DecodeJSON([]byte("\"!\""), &n); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("decimal to big.Rat", func(t *testing.T) {
		s, _ := Parse(`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)
		var r big.Rat
		if err := s.DecodeJSON([]byte("\"!\""), &r); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("decimal bytes from JSON number to big.Rat", func(t *testing.T) {
		s, _ := Parse(`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)
		var r big.Rat
		if err := s.DecodeJSON([]byte("12.34"), &r); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("decimal bytes from JSON number to json.Number", func(t *testing.T) {
		s, _ := Parse(`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)
		var n json.Number
		if err := s.DecodeJSON([]byte("12.34"), &n); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("decimal fixed from JSON number to big.Rat", func(t *testing.T) {
		s, _ := Parse(`{"type":"fixed","name":"d","size":8,"logicalType":"decimal","precision":10,"scale":2}`)
		var r big.Rat
		if err := s.DecodeJSON([]byte("12.34"), &r); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("decimal fixed from JSON number to json.Number", func(t *testing.T) {
		s, _ := Parse(`{"type":"fixed","name":"d","size":8,"logicalType":"decimal","precision":10,"scale":2}`)
		var n json.Number
		if err := s.DecodeJSON([]byte("12.34"), &n); err != nil {
			t.Fatal(err)
		}
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
		if err := s.DecodeJSON([]byte("\"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\""), &d); err != nil {
			t.Fatal(err)
		}
	})
}

// TestDecodeJSONTypedBool exercises typed bool target.
func TestDecodeJSONTypedBool(t *testing.T) {
	s, _ := Parse(`"boolean"`)
	var b bool
	if err := s.DecodeJSON([]byte(`true`), &b); err != nil {
		t.Fatal(err)
	}
	if !b {
		t.Fatal("expected true")
	}
}

// TestDecodeJSONTypedString exercises typed string targets.
func TestDecodeJSONTypedString(t *testing.T) {
	s, _ := Parse(`"string"`)
	var str string
	if err := s.DecodeJSON([]byte(`"hello"`), &str); err != nil {
		t.Fatal(err)
	}
	if str != "hello" {
		t.Fatalf("got %q", str)
	}
	// String with escapes.
	if err := s.DecodeJSON([]byte(`"hello\nworld"`), &str); err != nil {
		t.Fatal(err)
	}
	if str != "hello\nworld" {
		t.Fatalf("got %q", str)
	}
}

// TestDecodeJSONTypedFloat exercises typed float targets.
func TestDecodeJSONTypedFloat(t *testing.T) {
	s, _ := Parse(`"float"`)
	var f float32
	if err := s.DecodeJSON([]byte(`3.14`), &f); err != nil {
		t.Fatal(err)
	}
	if f < 3.13 || f > 3.15 {
		t.Fatalf("got %v", f)
	}
}

// TestDecodeJSONTypedDouble exercises typed double targets.
func TestDecodeJSONTypedDouble(t *testing.T) {
	s, _ := Parse(`"double"`)
	var f float64
	if err := s.DecodeJSON([]byte(`3.14159`), &f); err != nil {
		t.Fatal(err)
	}
	if f != 3.14159 {
		t.Fatalf("got %v", f)
	}
}

// TestDecodeJSONRecordMap exercises DecodeJSON into map[string]T.
func TestDecodeJSONRecordMap(t *testing.T) {
	s, _ := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}
	]}`)
	var m map[string]any
	if err := s.DecodeJSON([]byte(`{"a":1,"b":"hello"}`), &m); err != nil {
		t.Fatal(err)
	}
	if m["b"] != "hello" {
		t.Fatalf("got %v", m)
	}
}

// TestDecodeJSONMapTyped exercises DecodeJSON into map[string]T typed values.
func TestDecodeJSONMapTyped(t *testing.T) {
	s, _ := Parse(`{"type":"map","values":"int"}`)
	var m map[string]int32
	if err := s.DecodeJSON([]byte(`{"x":1,"y":2}`), &m); err != nil {
		t.Fatal(err)
	}
	if m["x"] != 1 || m["y"] != 2 {
		t.Fatalf("got %v", m)
	}
}

// TestDecodeJSONArrayTyped exercises DecodeJSON into typed slices.
func TestDecodeJSONArrayTyped(t *testing.T) {
	s, _ := Parse(`{"type":"array","items":"string"}`)
	var arr []string
	if err := s.DecodeJSON([]byte(`["a","b","c"]`), &arr); err != nil {
		t.Fatal(err)
	}
	if len(arr) != 3 || arr[0] != "a" {
		t.Fatalf("got %v", arr)
	}
}

// TestDecodeJSONUnionBranchTyped exercises typed union targets.
func TestDecodeJSONUnionBranchTyped(t *testing.T) {
	s, _ := Parse(`["null","string"]`)
	var str *string
	if err := s.DecodeJSON([]byte(`{"string":"hello"}`), &str); err != nil {
		t.Fatal(err)
	}
	if str == nil || *str != "hello" {
		t.Fatalf("got %v", str)
	}
}

// TestDecodeJSONSkipCompound exercises skipping unknown object/array fields.
func TestDecodeJSONSkipCompound(t *testing.T) {
	s, _ := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"}
	]}`)
	// Extra fields with nested objects and arrays should be skipped.
	input := `{"a":1,"unknown_obj":{"nested":true},"unknown_arr":[1,2,3]}`
	var out any
	if err := s.DecodeJSON([]byte(input), &out); err != nil {
		t.Fatal(err)
	}
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
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatal(err)
			}
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
	s, err := Parse(`{"type":"long","logicalType":"money"}`,
		NewCustomType[Money, int64]("money",
			func(m Money, _ *SchemaNode) (int64, error) { return m.Cents, nil },
			func(c int64, _ *SchemaNode) (Money, error) { return Money{Cents: c}, nil },
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	var m Money
	if err := s.DecodeJSON([]byte(`42`), &m); err != nil {
		t.Fatal(err)
	}
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
		if err := sf.DecodeJSON([]byte(`"NaN"`), &v); err != nil {
			t.Fatal(err)
		}
		if !math.IsNaN(float64(v.(float32))) {
			t.Fatalf("expected NaN, got %v", v)
		}
	})
	t.Run("float null NaN", func(t *testing.T) {
		var v any
		if err := sf.DecodeJSON([]byte(`null`), &v); err != nil {
			t.Fatal(err)
		}
		if !math.IsNaN(float64(v.(float32))) {
			t.Fatalf("expected NaN, got %v", v)
		}
	})
	t.Run("double NaN string", func(t *testing.T) {
		var v any
		if err := sd.DecodeJSON([]byte(`"NaN"`), &v); err != nil {
			t.Fatal(err)
		}
		if !math.IsNaN(v.(float64)) {
			t.Fatalf("expected NaN, got %v", v)
		}
	})
	t.Run("double null NaN", func(t *testing.T) {
		var v any
		if err := sd.DecodeJSON([]byte(`null`), &v); err != nil {
			t.Fatal(err)
		}
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
	if err := s.DecodeJSON([]byte(`"GREEN"`), &c); err != nil {
		t.Fatal(err)
	}
	if c != "GREEN" {
		t.Fatalf("got %q", c)
	}
}

// TestDecodeJSONStringToBytes exercises string to []byte target.
func TestDecodeJSONStringToBytes(t *testing.T) {
	s, _ := Parse(`"string"`)
	var b []byte
	if err := s.DecodeJSON([]byte(`"hello"`), &b); err != nil {
		t.Fatal(err)
	}
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
	if err := s.DecodeJSON([]byte(`42`), &u); err != nil {
		t.Fatal(err)
	}
	if u != 42 {
		t.Fatalf("got %d", u)
	}
}

// TestDecodeJSONLongUint exercises uint target for long fields.
func TestDecodeJSONLongUint(t *testing.T) {
	s, _ := Parse(`"long"`)
	var u uint64
	if err := s.DecodeJSON([]byte(`42`), &u); err != nil {
		t.Fatal(err)
	}
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
		// decodeLogicalFixed: decimal (4 bytes: value 33, scale 2 → 0.33; precision must fit in 4 bytes: max 9)
		{`{"type":"fixed","name":"d","size":4,"logicalType":"decimal","precision":9,"scale":2}`, "\"\u0000\u0000\u0000!\"", func(v any) bool { _, ok := v.(*big.Rat); return ok }},
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
	s, err := Parse(`{"type":"long","logicalType":"custom"}`,
		CustomType{
			LogicalType: "custom",
			AvroType:    "long",
			Decode: func(v any, _ *SchemaNode) (any, error) {
				return nil, ErrSkipCustomType
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := s.DecodeJSON([]byte(`42`), &out); err != nil {
		t.Fatal(err)
	}
	// All decoders skipped → raw int64 value.
	if out != int64(42) {
		t.Fatalf("got %v (%T)", out, out)
	}
}

// TestDecodeJSONCustomDecoderError exercises fatal custom decoder error.
func TestDecodeJSONCustomDecoderError(t *testing.T) {
	s, err := Parse(`{"type":"long","logicalType":"boom"}`,
		CustomType{
			LogicalType: "boom",
			AvroType:    "long",
			Decode: func(v any, _ *SchemaNode) (any, error) {
				return nil, fmt.Errorf("kaboom")
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := s.DecodeJSON([]byte(`42`), &out); err == nil {
		t.Fatal("expected error")
	}
}

// TestDecodeJSONCustomDecoderNilResult exercises custom decoder returning nil.
func TestDecodeJSONCustomDecoderNilResult(t *testing.T) {
	s, err := Parse(`{"type":"long","logicalType":"nilout"}`,
		CustomType{
			LogicalType: "nilout",
			AvroType:    "long",
			Decode: func(v any, _ *SchemaNode) (any, error) {
				return nil, nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := s.DecodeJSON([]byte(`42`), &out); err != nil {
		t.Fatal(err)
	}
	if out != nil {
		t.Fatalf("expected nil, got %v", out)
	}
}

// TestDecodeJSONNullTypedTargets exercises null into various typed targets.
func TestDecodeJSONNullTypedTargets(t *testing.T) {
	s, _ := Parse(`"null"`)
	t.Run("any", func(t *testing.T) {
		var v any
		if err := s.DecodeJSON([]byte(`null`), &v); err != nil {
			t.Fatal(err)
		}
		if v != nil {
			t.Fatal("expected nil")
		}
	})
	t.Run("map", func(t *testing.T) {
		var m map[string]any
		if err := s.DecodeJSON([]byte(`null`), &m); err != nil {
			t.Fatal(err)
		}
		if m != nil {
			t.Fatal("expected nil")
		}
	})
	t.Run("slice", func(t *testing.T) {
		var sl []int
		if err := s.DecodeJSON([]byte(`null`), &sl); err != nil {
			t.Fatal(err)
		}
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
	if err := s.DecodeJSON([]byte(`{"v":{"string":"hello"}}`), &r); err != nil {
		t.Fatal(err)
	}
	if r.V == nil || *r.V != "hello" {
		t.Fatalf("got %v", r.V)
	}
}

// TestDecodeJSONUnionBareTypedTarget exercises bare union into non-pointer typed target.
func TestDecodeJSONUnionBareTypedTarget(t *testing.T) {
	s, _ := Parse(`["null","string","int"]`)
	// Decode bare string into *any — already covered.
	// Decode bare int into a typed int32 target (non-pointer, multi-branch).
	var out int32
	if err := s.DecodeJSON([]byte(`42`), &out); err != nil {
		t.Fatal(err)
	}
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
		if err := s.DecodeJSON([]byte(`"NaN"`), &f); err != nil {
			t.Fatal(err)
		}
		if !math.IsNaN(float64(f)) {
			t.Fatalf("expected NaN, got %v", f)
		}
	})
	t.Run("double NaN string typed", func(t *testing.T) {
		s, _ := Parse(`"double"`)
		var f float64
		if err := s.DecodeJSON([]byte(`"NaN"`), &f); err != nil {
			t.Fatal(err)
		}
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
		if err := s.DecodeJSON([]byte(`null`), &f); err != nil {
			t.Fatal(err)
		}
		if !math.IsNaN(float64(f)) {
			t.Fatalf("expected NaN, got %v", f)
		}
	})
	t.Run("double null typed", func(t *testing.T) {
		s, _ := Parse(`"double"`)
		var f float64
		if err := s.DecodeJSON([]byte(`null`), &f); err != nil {
			t.Fatal(err)
		}
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
	if err := s.DecodeJSON([]byte(`{"a":1}`), &out); err != nil {
		t.Fatal(err)
	}
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
	s, _ := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"}
	]}`)
	var out any
	if err := s.DecodeJSON([]byte(`{"a":"notanint"}`), &out); err == nil {
		t.Fatal("expected error")
	}
}

// TestDecodeJSONWrapUnionQualifyLogical exercises the qualifyLogical branch.
func TestDecodeJSONWrapUnionQualifyLogical(t *testing.T) {
	s, _ := Parse(`["null",{"type":"long","logicalType":"timestamp-millis"}]`)
	var out any
	if err := s.DecodeJSON([]byte(`1700000000000`), &out, TaggedUnions(), TagLogicalTypes()); err != nil {
		t.Fatal(err)
	}
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
	s, err := Parse(`{"type":"long","logicalType":"custom"}`,
		CustomType{LogicalType: "custom", AvroType: "long",
			Decode: func(v any, _ *SchemaNode) (any, error) { return v, nil },
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := s.DecodeJSON([]byte(`"notanumber"`), &out); err == nil {
		t.Fatal("expected error from inner decode")
	}
}

// TestDecodeJSONFixedScanError exercises malformed fixed/bytes string.
func TestDecodeJSONFixedScanError(t *testing.T) {
	s, _ := Parse(`{"type":"fixed","name":"f","size":3}`)
	var out any
	// Not a string → error.
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
	if err := s.DecodeJSON([]byte(`[]`), &out); err != nil {
		t.Fatal(err)
	}
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

// TestDecodeJSONWalkEscapesDefaultChar exercises the default escape char.
func TestDecodeJSONWalkEscapesDefaultChar(t *testing.T) {
	// Construct raw bytes with \x (not valid JSON but walkJSONEscapes handles it).
	var got []byte
	err := walkJSONEscapes([]byte{'\\', 'x'}, func(r rune) error {
		got = append(got, byte(r))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "x" {
		t.Fatalf("got %q, want x", got)
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
		if err := s.DecodeJSON([]byte(`[{"x":1},{"x":2}]`), &out); err != nil {
			t.Fatal(err)
		}
		arr := out.([]any)
		if len(arr) != 2 {
			t.Fatalf("got %d items", len(arr))
		}
	})
	t.Run("map of arrays", func(t *testing.T) {
		s, _ := Parse(`{"type":"map","values":{"type":"array","items":"string"}}`)
		var out any
		if err := s.DecodeJSON([]byte(`{"a":["x","y"],"b":["z"]}`), &out); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("record with union of record", func(t *testing.T) {
		s, _ := Parse(`{"type":"record","name":"Outer","fields":[
			{"name":"inner","type":["null",{"type":"record","name":"Inner","fields":[
				{"name":"v","type":"int"}
			]}]}
		]}`)
		var out any
		if err := s.DecodeJSON([]byte(`{"inner":{"Inner":{"v":42}}}`), &out); err != nil {
			t.Fatal(err)
		}
		if err := s.DecodeJSON([]byte(`{"inner":null}`), &out); err != nil {
			t.Fatal(err)
		}
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
		if err := s.DecodeJSON([]byte(`{"inner":{"x":1,"y":"hello"},"z":99}`), &out); err != nil {
			t.Fatal(err)
		}
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
	_, err := root.Schema()
	if err != nil {
		t.Fatal(err)
	}
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
		if err := s.DecodeJSON([]byte(`{"d":19700}`), &m); err != nil {
			t.Fatal(err)
		}
		if m["d"].IsZero() {
			t.Fatal("expected non-zero time")
		}
	})
	t.Run("time-millis into map value", func(t *testing.T) {
		s, _ := Parse(`{"type":"map","values":{"type":"int","logicalType":"time-millis"}}`)
		var m map[string]time.Duration
		if err := s.DecodeJSON([]byte(`{"t":43200000}`), &m); err != nil {
			t.Fatal(err)
		}
		if m["t"] == 0 {
			t.Fatal("expected non-zero duration")
		}
	})
	t.Run("timestamp-millis into map value", func(t *testing.T) {
		s, _ := Parse(`{"type":"map","values":{"type":"long","logicalType":"timestamp-millis"}}`)
		var m map[string]time.Time
		if err := s.DecodeJSON([]byte(`{"ts":1700000000000}`), &m); err != nil {
			t.Fatal(err)
		}
		if m["ts"].IsZero() {
			t.Fatal("expected non-zero time")
		}
	})
	t.Run("timestamp-micros into map value", func(t *testing.T) {
		s, _ := Parse(`{"type":"map","values":{"type":"long","logicalType":"timestamp-micros"}}`)
		var m map[string]time.Time
		if err := s.DecodeJSON([]byte(`{"ts":1700000000000000}`), &m); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("timestamp-nanos into map value", func(t *testing.T) {
		s, _ := Parse(`{"type":"map","values":{"type":"long","logicalType":"timestamp-nanos"}}`)
		var m map[string]time.Time
		if err := s.DecodeJSON([]byte(`{"ts":1700000000000000000}`), &m); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("time-micros into map value", func(t *testing.T) {
		s, _ := Parse(`{"type":"map","values":{"type":"long","logicalType":"time-micros"}}`)
		var m map[string]time.Duration
		if err := s.DecodeJSON([]byte(`{"t":1500000}`), &m); err != nil {
			t.Fatal(err)
		}
		if m["t"] == 0 {
			t.Fatal("expected non-zero duration")
		}
	})
}

// TestDecodeJSONStringWithEscapes exercises the resolveJSONEscapes path
// in consumeString and consumeStringZeroCopy.
func TestDecodeJSONStringWithEscapes(t *testing.T) {
	// String value with escapes (goes through consumeSlabString → resolveJSONEscapes).
	s, _ := Parse(`"string"`)
	var out any
	if err := s.DecodeJSON([]byte(`"hello\tworld"`), &out); err != nil {
		t.Fatal(err)
	}
	if out != "hello\tworld" {
		t.Fatalf("got %q", out)
	}

	// Map key with escapes (goes through consumeSlabString → resolveJSONEscapes).
	sm, _ := Parse(`{"type":"map","values":"int"}`)
	var mout any
	if err := sm.DecodeJSON([]byte(`{"key\twith\ttabs":42}`), &mout); err != nil {
		t.Fatal(err)
	}
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
	s, _ := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}
	]}`)
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
	// \uXXGG — invalid hex digits.
	if err := s.DecodeJSON([]byte(`"\u00GG"`), &out); err == nil {
		t.Fatal("expected error for bad hex in \\u")
	}
}

// TestDecodeJSONLogicalTyped exercises typed target paths for logical
// types in decodeInt, decodeLong, decodeFixed, and decodeBytes.
func TestDecodeJSONLogicalTyped(t *testing.T) {
	// date → time.Time
	t.Run("date to time.Time", func(t *testing.T) {
		s := MustParse(`{"type":"int","logicalType":"date"}`)
		var tm time.Time
		if err := s.DecodeJSON([]byte("18262"), &tm); err != nil {
			t.Fatal(err)
		}
	})
	// time-millis → time.Duration
	t.Run("time-millis to time.Duration", func(t *testing.T) {
		s := MustParse(`{"type":"int","logicalType":"time-millis"}`)
		var d time.Duration
		if err := s.DecodeJSON([]byte("12345"), &d); err != nil {
			t.Fatal(err)
		}
	})
	// timestamp-millis → time.Time (each variant)
	for _, lt := range []string{"timestamp-millis", "local-timestamp-millis"} {
		t.Run(lt+" to time.Time", func(t *testing.T) {
			s := MustParse(`{"type":"long","logicalType":"` + lt + `"}`)
			var tm time.Time
			if err := s.DecodeJSON([]byte("1577880645000"), &tm); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, lt := range []string{"timestamp-micros", "local-timestamp-micros"} {
		t.Run(lt+" to time.Time", func(t *testing.T) {
			s := MustParse(`{"type":"long","logicalType":"` + lt + `"}`)
			var tm time.Time
			if err := s.DecodeJSON([]byte("1577880645000000"), &tm); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, lt := range []string{"timestamp-nanos", "local-timestamp-nanos"} {
		t.Run(lt+" to time.Time", func(t *testing.T) {
			s := MustParse(`{"type":"long","logicalType":"` + lt + `"}`)
			var tm time.Time
			if err := s.DecodeJSON([]byte("1577880645000000000"), &tm); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("time-micros to time.Duration", func(t *testing.T) {
		s := MustParse(`{"type":"long","logicalType":"time-micros"}`)
		var d time.Duration
		if err := s.DecodeJSON([]byte("12345"), &d); err != nil {
			t.Fatal(err)
		}
	})
	// long to int/uint targets
	t.Run("long to int", func(t *testing.T) {
		s := MustParse(`"long"`)
		var n int
		if err := s.DecodeJSON([]byte("42"), &n); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("long to uint", func(t *testing.T) {
		s := MustParse(`"long"`)
		var n uint
		if err := s.DecodeJSON([]byte("42"), &n); err != nil {
			t.Fatal(err)
		}
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
		if err := s.DecodeJSON([]byte("42"), &n); err != nil {
			t.Fatal(err)
		}
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
	if err := enumS.DecodeJSON([]byte(`"A"`), new(int)); err == nil {
		t.Error("expected unsupported target error")
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
	// int unsupported target
	intS := MustParse(`"int"`)
	var f float32
	if err := intS.DecodeJSON([]byte(`42`), &f); err == nil {
		t.Error("expected unsupported target for int")
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
		if err := s.DecodeJSON([]byte(`{"a":1577880645000}`), &m); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("map timestamp-micros time.Time", func(t *testing.T) {
		s := MustParse(`{"type":"map","values":{"type":"long","logicalType":"timestamp-micros"}}`)
		var m map[string]time.Time
		if err := s.DecodeJSON([]byte(`{"a":1577880645000000}`), &m); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("map timestamp-nanos time.Time", func(t *testing.T) {
		s := MustParse(`{"type":"map","values":{"type":"long","logicalType":"timestamp-nanos"}}`)
		var m map[string]time.Time
		if err := s.DecodeJSON([]byte(`{"a":1577880645000000000}`), &m); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("map time-micros time.Duration", func(t *testing.T) {
		s := MustParse(`{"type":"map","values":{"type":"long","logicalType":"time-micros"}}`)
		var m map[string]time.Duration
		if err := s.DecodeJSON([]byte(`{"a":12345}`), &m); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("map date time.Time", func(t *testing.T) {
		s := MustParse(`{"type":"map","values":{"type":"int","logicalType":"date"}}`)
		var m map[string]time.Time
		if err := s.DecodeJSON([]byte(`{"a":18262}`), &m); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("map time-millis time.Duration", func(t *testing.T) {
		s := MustParse(`{"type":"map","values":{"type":"int","logicalType":"time-millis"}}`)
		var m map[string]time.Duration
		if err := s.DecodeJSON([]byte(`{"a":12345}`), &m); err != nil {
			t.Fatal(err)
		}
	})
}
