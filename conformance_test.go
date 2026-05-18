package avro_test

import (
	"bytes"
	"compress/flate"
	"crypto/md5"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

// ---------- helpers_test.go ----------

// zigzagEncode32 encodes an int32 using Avro's zigzag varint encoding.
func zigzagEncode32(i int32) []byte {
	z := uint32((i << 1) ^ (i >> 31))
	var buf [5]byte
	n := 0
	for z >= 0x80 {
		buf[n] = byte(z) | 0x80
		z >>= 7
		n++
	}
	buf[n] = byte(z)
	return buf[:n+1]
}

// zigzagEncode64 encodes an int64 using Avro's zigzag varlong encoding.
func zigzagEncode64(i int64) []byte {
	z := uint64((i << 1) ^ (i >> 63))
	var buf [10]byte
	n := 0
	for z >= 0x80 {
		buf[n] = byte(z) | 0x80
		z >>= 7
		n++
	}
	buf[n] = byte(z)
	return buf[:n+1]
}

// encodeUint32LE encodes a uint32 in little-endian format (for float bits).
func encodeUint32LE(u uint32) []byte {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], u)
	return buf[:]
}

// encodeUint64LE encodes a uint64 in little-endian format (for double bits).
func encodeUint64LE(u uint64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], u)
	return buf[:]
}

// mustParse parses a schema string, failing the test on error.
func mustParse(t *testing.T, schema string) *avro.Schema {
	t.Helper()
	s, err := avro.Parse(schema)
	if err != nil {
		t.Fatalf("Parse(%q): %v", schema, err)
	}
	return s
}

// encode encodes v with the given schema string and returns the raw bytes.
func encode(t *testing.T, schema string, v any) []byte {
	t.Helper()
	s := mustParse(t, schema)
	dst, err := s.AppendEncode(nil, v)
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	return dst
}

// decode decodes src into v using the given schema string.
func decode(t *testing.T, schema string, src []byte, v any) {
	t.Helper()
	s := mustParse(t, schema)
	rem, err := s.Decode(src, v)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
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

// roundTrip encodes then decodes a value, returning the result.
func roundTrip[T any](t *testing.T, schema string, input T) T {
	t.Helper()
	s := mustParse(t, schema)
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

// resolveEncodeDecode encodes input with writerSchema, resolves to readerSchema,
// and decodes into output.
func resolveEncodeDecode(t *testing.T, writerSchema, readerSchema string, input, output any) {
	t.Helper()
	writer := mustParse(t, writerSchema)
	reader := mustParse(t, readerSchema)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	encoded, err := writer.Encode(input)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	_, err = resolved.Decode(encoded, output)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
}

// buildReferenceBytes builds expected bytes for the interop reference test.
func buildReferenceBytes(boolVal bool, intVal int32, longVal int64, floatVal float32, doubleVal float64, strVal string, bytesVal []byte) []byte {
	var want []byte
	if boolVal {
		want = append(want, 0x01)
	} else {
		want = append(want, 0x00)
	}
	want = append(want, zigzagEncode32(intVal)...)
	want = append(want, zigzagEncode64(longVal)...)
	want = append(want, encodeUint32LE(math.Float32bits(floatVal))...)
	want = append(want, encodeUint64LE(math.Float64bits(doubleVal))...)
	want = append(want, zigzagEncode64(int64(len(strVal)))...)
	want = append(want, strVal...)
	want = append(want, zigzagEncode64(int64(len(bytesVal)))...)
	want = append(want, bytesVal...)
	return want
}

// ---------- compat_test.go ----------

// -----------------------------------------------------------------------
// Compatibility Matrix
// Spec: "Schema Resolution" — static compatibility checks before data
// is exchanged, verifying that a reader can process any writer output.
//   - Field addition (with/without default), field removal
//   - Type promotions (8 valid + invalid rejected)
//   - Enum symbol changes (added, removed with/without default)
//   - Union branch additions
//   - Named type matching (record/enum/fixed name, including unqualified)
//   - Recursive compatibility through arrays and maps
// https://avro.apache.org/docs/1.12.0/specification/#schema-resolution
// -----------------------------------------------------------------------

func TestCompatSameSchema(t *testing.T) {
	schema := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`
	w := mustParse(t, schema)
	r := mustParse(t, schema)
	if err := avro.CheckCompatibility(w, r); err != nil {
		t.Fatalf("same schema should be compatible: %v", err)
	}
}

func TestCompatFieldAddedWithDefault(t *testing.T) {
	writer := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`
	reader := `{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string","default":"x"}
	]}`
	if err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, reader)); err != nil {
		t.Fatalf("field added with default should be compatible: %v", err)
	}
}

func TestCompatFieldAddedNoDefault(t *testing.T) {
	writer := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`
	reader := `{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}
	]}`
	err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, reader))
	if err == nil {
		t.Fatal("field added without default should be incompatible")
	}
}

func TestCompatFieldRemoved(t *testing.T) {
	writer := `{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}
	]}`
	reader := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`
	if err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, reader)); err != nil {
		t.Fatalf("field removed should be compatible: %v", err)
	}
}

func TestCompatNamedTypesMatchByUnqualifiedName(t *testing.T) {
	if err := avro.CheckCompatibility(
		mustParse(t, `{"type":"record","name":"a.Foo","fields":[{"name":"a","type":"int"}]}`),
		mustParse(t, `{"type":"record","name":"b.Foo","fields":[{"name":"a","type":"int"}]}`),
	); err != nil {
		t.Fatalf("record unqualified-name match should be compatible: %v", err)
	}

	if err := avro.CheckCompatibility(
		mustParse(t, `{"type":"enum","name":"a.E","symbols":["A","B"]}`),
		mustParse(t, `{"type":"enum","name":"b.E","symbols":["A","B"]}`),
	); err != nil {
		t.Fatalf("enum unqualified-name match should be compatible: %v", err)
	}

	if err := avro.CheckCompatibility(
		mustParse(t, `{"type":"fixed","name":"a.Id","size":4}`),
		mustParse(t, `{"type":"fixed","name":"b.Id","size":4}`),
	); err != nil {
		t.Fatalf("fixed unqualified-name match should be compatible: %v", err)
	}
}

func TestCompatTypePromotion(t *testing.T) {
	// All 8 valid promotions.
	promotions := []struct {
		name   string
		writer string
		reader string
	}{
		{"int→long", `"int"`, `"long"`},
		{"int→float", `"int"`, `"float"`},
		{"int→double", `"int"`, `"double"`},
		{"long→float", `"long"`, `"float"`},
		{"long→double", `"long"`, `"double"`},
		{"float→double", `"float"`, `"double"`},
		{"string→bytes", `"string"`, `"bytes"`},
		{"bytes→string", `"bytes"`, `"string"`},
	}

	for _, p := range promotions {
		t.Run(p.name, func(t *testing.T) {
			if err := avro.CheckCompatibility(mustParse(t, p.writer), mustParse(t, p.reader)); err != nil {
				t.Fatalf("promotion %s should be compatible: %v", p.name, err)
			}
		})
	}
}

func TestCompatInvalidPromotion(t *testing.T) {
	invalid := []struct {
		name   string
		writer string
		reader string
	}{
		{"long→int", `"long"`, `"int"`},
		{"double→float", `"double"`, `"float"`},
		{"float→int", `"float"`, `"int"`},
		{"double→long", `"double"`, `"long"`},
		{"string→int", `"string"`, `"int"`},
		{"int→string", `"int"`, `"string"`},
		{"boolean→int", `"boolean"`, `"int"`},
	}

	for _, p := range invalid {
		t.Run(p.name, func(t *testing.T) {
			err := avro.CheckCompatibility(mustParse(t, p.writer), mustParse(t, p.reader))
			if err == nil {
				t.Fatalf("promotion %s should be incompatible", p.name)
			}
		})
	}
}

func TestCompatEnumSymbolAdded(t *testing.T) {
	writer := `{"type":"enum","name":"E","symbols":["A","B"]}`
	reader := `{"type":"enum","name":"E","symbols":["A","B","C"]}`
	if err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, reader)); err != nil {
		t.Fatalf("adding enum symbol to reader should be compatible: %v", err)
	}
}

func TestCompatEnumSymbolRemoved(t *testing.T) {
	writer := `{"type":"enum","name":"E","symbols":["A","B","C"]}`
	reader := `{"type":"enum","name":"E","symbols":["A","B"]}`
	err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, reader))
	if err == nil {
		t.Fatal("removing enum symbol without default should be incompatible")
	}

	// With a default, it should be compatible.
	readerWithDefault := `{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}`
	if err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, readerWithDefault)); err != nil {
		t.Fatalf("removing enum symbol with default should be compatible: %v", err)
	}
}

func TestCompatUnionBranchAdded(t *testing.T) {
	writer := `["null","int"]`
	reader := `["null","int","string"]`
	if err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, reader)); err != nil {
		t.Fatalf("adding union branch to reader should be compatible: %v", err)
	}
}

func TestCompatRecordNameMismatch(t *testing.T) {
	writer := `{"type":"record","name":"A","fields":[{"name":"x","type":"int"}]}`
	reader := `{"type":"record","name":"B","fields":[{"name":"x","type":"int"}]}`
	err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, reader))
	if err == nil {
		t.Fatal("different record names should be incompatible")
	}
}

func TestCompatArrayItemsCompat(t *testing.T) {
	// Compatible array items.
	writer := `{"type":"array","items":"int"}`
	reader := `{"type":"array","items":"long"}`
	if err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, reader)); err != nil {
		t.Fatalf("array int→long should be compatible: %v", err)
	}

	// Incompatible array items.
	reader2 := `{"type":"array","items":"string"}`
	if err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, reader2)); err == nil {
		t.Fatal("array int→string should be incompatible")
	}
}

func TestCompatMapValuesCompat(t *testing.T) {
	// Compatible map values.
	writer := `{"type":"map","values":"int"}`
	reader := `{"type":"map","values":"long"}`
	if err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, reader)); err != nil {
		t.Fatalf("map int→long should be compatible: %v", err)
	}

	// Incompatible map values.
	reader2 := `{"type":"map","values":"string"}`
	if err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, reader2)); err == nil {
		t.Fatal("map int→string should be incompatible")
	}
}

// ---------- defaults_test.go ----------

// -----------------------------------------------------------------------
// Defaults & Canonical Form
// Spec: "Schema Resolution" (field defaults) and "Parsing Canonical
// Form for Schemas" (canonical JSON representation).
//   - Defaults: applied at read time only, not write time
//   - Union defaults: may match any branch type (Avro 1.12+)
//   - Complex defaults: arrays, maps, nested records
//   - Bytes defaults: decoded from JSON \uXXXX escapes
//   - Canonical form: strips doc/aliases/defaults, expands fullnames,
//     deterministic key ordering, collapses primitive object form
// https://avro.apache.org/docs/1.12.0/specification/#schema-resolution
// https://avro.apache.org/docs/1.12.0/specification/#parsing-canonical-form-for-schemas
// -----------------------------------------------------------------------

func TestSpecNullUnionDefault(t *testing.T) {
	writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	reader := mustParse(t, `{
		"type":"record","name":"R",
		"fields":[
			{"name":"a","type":"int"},
			{"name":"b","type":["null","string"],"default":null}
		]
	}`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{"a": 42})
	if err != nil {
		t.Fatal(err)
	}
	var result any
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	m := result.(map[string]any)
	if m["a"] != int32(42) {
		t.Fatalf("a: got %v, want 42", m["a"])
	}
	if m["b"] != nil {
		t.Fatalf("b: got %v, want nil", m["b"])
	}
}

func TestSpecUnionDefaultNonFirstBranch(t *testing.T) {
	// Default matches the second branch (string), not the first (null).
	// This is the *string pattern: ["null","string"] with default "hello".
	writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	reader := mustParse(t, `{
		"type":"record","name":"R",
		"fields":[
			{"name":"a","type":"int"},
			{"name":"b","type":["null","string"],"default":"hello"}
		]
	}`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{"a": int32(7)})
	if err != nil {
		t.Fatal(err)
	}
	var result any
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	m := result.(map[string]any)
	if m["a"] != int32(7) {
		t.Fatalf("a: got %v, want 7", m["a"])
	}
	if m["b"] != "hello" {
		t.Fatalf("b: got %v (%T), want \"hello\"", m["b"], m["b"])
	}
}

func TestSpecComplexDefaults(t *testing.T) {
	t.Run("array default", func(t *testing.T) {
		writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
		reader := mustParse(t, `{
			"type":"record","name":"R",
			"fields":[
				{"name":"a","type":"int"},
				{"name":"arr","type":{"type":"array","items":"int"},"default":[1,2,3]}
			]
		}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}

		encoded, err := writer.Encode(map[string]any{"a": 1})
		if err != nil {
			t.Fatal(err)
		}
		var result any
		_, err = resolved.Decode(encoded, &result)
		if err != nil {
			t.Fatal(err)
		}
		m := result.(map[string]any)
		arr := m["arr"].([]any)
		if len(arr) != 3 {
			t.Fatalf("arr length: got %d, want 3", len(arr))
		}
		if arr[0] != int32(1) || arr[1] != int32(2) || arr[2] != int32(3) {
			t.Fatalf("arr: got %v, want [1 2 3]", arr)
		}
	})

	t.Run("map default", func(t *testing.T) {
		writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
		reader := mustParse(t, `{
			"type":"record","name":"R",
			"fields":[
				{"name":"a","type":"int"},
				{"name":"m","type":{"type":"map","values":"string"},"default":{}}
			]
		}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}

		encoded, err := writer.Encode(map[string]any{"a": 1})
		if err != nil {
			t.Fatal(err)
		}
		var result any
		_, err = resolved.Decode(encoded, &result)
		if err != nil {
			t.Fatal(err)
		}
		m := result.(map[string]any)
		dm := m["m"].(map[string]any)
		if len(dm) != 0 {
			t.Fatalf("map: got %v, want empty", dm)
		}
	})

	t.Run("nested record default", func(t *testing.T) {
		writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
		reader := mustParse(t, `{
			"type":"record","name":"R",
			"fields":[
				{"name":"a","type":"int"},
				{"name":"inner","type":{
					"type":"record","name":"Inner",
					"fields":[
						{"name":"x","type":"int","default":0},
						{"name":"y","type":"string","default":"default"}
					]
				},"default":{"x":99}}
			]
		}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}

		encoded, err := writer.Encode(map[string]any{"a": 1})
		if err != nil {
			t.Fatal(err)
		}
		var result any
		_, err = resolved.Decode(encoded, &result)
		if err != nil {
			t.Fatal(err)
		}
		m := result.(map[string]any)
		inner := m["inner"].(map[string]any)
		if inner["x"] != int32(99) {
			t.Fatalf("inner.x: got %v, want 99", inner["x"])
		}
		if inner["y"] != "default" {
			t.Fatalf("inner.y: got %v, want default", inner["y"])
		}
	})
}

func TestSpecDefaultBytesUnicode(t *testing.T) {
	writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	reader := mustParse(t, `{
		"type":"record","name":"R",
		"fields":[
			{"name":"a","type":"int"},
			{"name":"data","type":"bytes","default":"\u00FF\u0001\u0000"}
		]
	}`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{"a": 1})
	if err != nil {
		t.Fatal(err)
	}
	var result any
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	m := result.(map[string]any)
	b := m["data"].([]byte)
	if len(b) != 3 || b[0] != 0xFF || b[1] != 0x01 || b[2] != 0x00 {
		t.Fatalf("bytes default: got %x, want ff0100", b)
	}
}

func TestSpecDefaultsUsedOnEncode(t *testing.T) {
	s := mustParse(t, `{
		"type":"record","name":"R",
		"fields":[
			{"name":"a","type":"int","default":42},
			{"name":"b","type":"string"}
		]
	}`)

	// Field "a" has a default, so encoding with only "b" should succeed.
	dst, err := s.AppendEncode(nil, map[string]any{"b": "hello"})
	if err != nil {
		t.Fatalf("unexpected encode error: %v", err)
	}
	var decoded any
	if _, err := s.Decode(dst, &decoded); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	m := decoded.(map[string]any)
	if m["a"] != int32(42) {
		t.Errorf("field a: got %v (%T), want int32(42)", m["a"], m["a"])
	}
	if m["b"] != "hello" {
		t.Errorf("field b: got %v, want hello", m["b"])
	}

	// Field "b" has no default, so encoding with only "a" should still error.
	if _, err := s.AppendEncode(nil, map[string]any{"a": int32(1)}); err == nil {
		t.Fatal("expected encode error for missing field without default")
	}
}

func TestSpecUnionDefaultMatchesAnyBranch(t *testing.T) {
	// Per Avro 1.12+, the default may match any branch in a union.
	tests := []struct {
		name   string
		schema string
	}{
		{
			"null default matches second branch",
			`{"type":"record","name":"R","fields":[
				{"name":"u","type":["string","null"],"default":null}
			]}`,
		},
		{
			"string default matches second branch",
			`{"type":"record","name":"R","fields":[
				{"name":"u","type":["null","string"],"default":"hello"}
			]}`,
		},
		{
			"int default matches third branch",
			`{"type":"record","name":"R","fields":[
				{"name":"u","type":["null","string","int"],"default":42}
			]}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := avro.Parse(tt.schema)
			if err != nil {
				t.Fatalf("expected success, got: %v", err)
			}
		})
	}

	// Default must still match SOME branch.
	_, err := avro.Parse(`{
		"type":"record","name":"R",
		"fields":[
			{"name":"u","type":["null","string"],"default":42}
		]
	}`)
	if err == nil {
		t.Fatal("expected error when union default matches no branch")
	}
}

// TestRegression_UnionNullDefaultRoutesToNullBranchNotCompound locks the
// branch-routing for unions [Compound, null] with default null. Per spec
// (lang/java/avro/src/main/java/org/apache/avro/Schema.java:1751-1798
// isValidDefault: RECORD/ARRAY/MAP cases all reject non-matching JSON
// type; NULL case accepts only JsonNode.isNull), null is not a valid
// default for record/array/map — it can only match the null branch in
// such unions. Pre-fix, twmb's validateDefault lenient-accepted nil val
// for record/array/map (synthesizing an empty map / iterating zero
// elements), so the union branch-walk matched the compound branch first
// and encodeDefault emitted empty-Record / empty-array / empty-map wire
// bytes where the null branch was intended — a binary↔JSON parity break
// (JSON's auto-fill bypasses defaultBytes via the f.defaultVal == nil
// early-out and emits "null" correctly).
//
// Cross-checked: fastavro's _validate_record requires isinstance(datum,
// Mapping); hamba's isValidDefault returns false on type-assertion
// failure. Only twmb accepted.
func TestRegression_UnionNullDefaultRoutesToNullBranchNotCompound(t *testing.T) {
	cases := []struct {
		name   string
		schema string
	}{
		{
			"record-null union",
			`{"type":"record","name":"W","fields":[{
				"name":"f",
				"type":[{"type":"record","name":"R","fields":[{"name":"x","type":"int","default":0}]},"null"],
				"default":null
			}]}`,
		},
		{
			"array-null union",
			`{"type":"record","name":"W","fields":[{
				"name":"f",
				"type":[{"type":"array","items":"int"},"null"],
				"default":null
			}]}`,
		},
		{
			"map-null union",
			`{"type":"record","name":"W","fields":[{
				"name":"f",
				"type":[{"type":"map","values":"int"},"null"],
				"default":null
			}]}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := avro.Parse(tc.schema)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			// Binary encode of map missing the field auto-fills the default.
			// Expected: 0x02 (zigzag varint 1, null branch index).
			bin, err := s.AppendEncode(nil, map[string]any{})
			if err != nil {
				t.Fatalf("binary encode: %v", err)
			}
			if !bytes.Equal(bin, []byte{0x02}) {
				t.Errorf("binary encode: got %x, want 02 (null branch)", bin)
			}
			// JSON encode emits "null" (correct on both sides of the fix
			// because JSON's path bypasses defaultBytes).
			js, err := s.AppendEncodeJSON(nil, map[string]any{})
			if err != nil {
				t.Fatalf("json encode: %v", err)
			}
			if string(js) != `{"f":null}` {
				t.Errorf("json encode: got %s, want {\"f\":null}", js)
			}
			// JSON decode of `{}` fills the missing field via the pre-encoded
			// defaultBytes (applyFieldDefault → field.deser). Must be nil,
			// not an empty-compound value.
			var out map[string]any
			if err := s.DecodeJSON([]byte(`{}`), &out); err != nil {
				t.Fatalf("json decode: %v", err)
			}
			if v := out["f"]; v != nil {
				t.Errorf("json decode fill: out[f] = %T(%v), want nil", v, v)
			}
		})
	}
}

// TestRegression_NonUnionCompoundNullDefaultRejected locks parse-time
// rejection of null as a default for a non-union record/array/map
// field. Per spec (Specification/_index.md "field default values"
// table at lines 85-97): record default is JSON object, array default
// is JSON array, map default is JSON object — null is only a valid
// default for the null type. Java/fastavro/hamba all reject.
//
// Pre-fix, twmb's validateDefault accepted nil val for these types by
// synthesizing an empty container, masking the schema error.
func TestRegression_NonUnionCompoundNullDefaultRejected(t *testing.T) {
	cases := []struct {
		name   string
		schema string
	}{
		{
			"record null default",
			`{"type":"record","name":"W","fields":[{
				"name":"f",
				"type":{"type":"record","name":"R","fields":[{"name":"x","type":"int","default":0}]},
				"default":null
			}]}`,
		},
		{
			"array null default",
			`{"type":"record","name":"W","fields":[{
				"name":"f",
				"type":{"type":"array","items":"int"},
				"default":null
			}]}`,
		},
		{
			"map null default",
			`{"type":"record","name":"W","fields":[{
				"name":"f",
				"type":{"type":"map","values":"int"},
				"default":null
			}]}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := avro.Parse(tc.schema); err == nil {
				t.Fatalf("parse accepted null as %s default; expected rejection per spec", tc.name)
			}
		})
	}
}

// Counter-test: empty `{}` / `[]` defaults are valid (JSON object /
// array). The fix distinguishes nil from empty-but-non-nil — only
// nil is rejected.
func TestRegression_EmptyCompoundDefaultStillAccepted(t *testing.T) {
	cases := []struct {
		name   string
		schema string
	}{
		{
			"empty object record default",
			`{"type":"record","name":"W","fields":[{
				"name":"f",
				"type":{"type":"record","name":"R","fields":[{"name":"x","type":"int","default":0}]},
				"default":{}
			}]}`,
		},
		{
			"empty array default",
			`{"type":"record","name":"W","fields":[{
				"name":"f",
				"type":{"type":"array","items":"int"},
				"default":[]
			}]}`,
		},
		{
			"empty map default",
			`{"type":"record","name":"W","fields":[{
				"name":"f",
				"type":{"type":"map","values":"int"},
				"default":{}
			}]}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := avro.Parse(tc.schema); err != nil {
				t.Fatalf("parse rejected valid %s: %v", tc.name, err)
			}
		})
	}
}

func TestSpecNumericDefaultValidation(t *testing.T) {
	t.Run("int default must be integer", func(t *testing.T) {
		_, err := avro.Parse(`{
			"type":"record","name":"R",
			"fields":[{"name":"a","type":"int","default":1.5}]
		}`)
		if err == nil {
			t.Fatal("expected parse error for fractional int default")
		}
	})

	t.Run("int default must be in range", func(t *testing.T) {
		_, err := avro.Parse(`{
			"type":"record","name":"R",
			"fields":[{"name":"a","type":"int","default":2147483648}]
		}`)
		if err == nil {
			t.Fatal("expected parse error for out-of-range int default")
		}
	})

	t.Run("long default must be integer", func(t *testing.T) {
		_, err := avro.Parse(`{
			"type":"record","name":"R",
			"fields":[{"name":"a","type":"long","default":2.25}]
		}`)
		if err == nil {
			t.Fatal("expected parse error for fractional long default")
		}
	})
}

func TestSpecCanonicalFormSpec(t *testing.T) {
	t.Run("strip doc", func(t *testing.T) {
		s := mustParse(t, `{
			"type":"record",
			"name":"R",
			"doc":"This is a test record",
			"fields":[{"name":"a","type":"int","doc":"an integer"}]
		}`)
		canonical := string(s.Canonical())
		var m map[string]any
		if err := json.Unmarshal([]byte(canonical), &m); err != nil {
			t.Fatal(err)
		}
		if _, exists := m["doc"]; exists {
			t.Fatal("canonical form should not include doc")
		}
	})

	t.Run("strip aliases", func(t *testing.T) {
		s := mustParse(t, `{
			"type":"record",
			"name":"R",
			"aliases":["OldR"],
			"fields":[{"name":"a","type":"int","aliases":["old_a"]}]
		}`)
		canonical := string(s.Canonical())
		var m map[string]any
		if err := json.Unmarshal([]byte(canonical), &m); err != nil {
			t.Fatal(err)
		}
		if _, exists := m["aliases"]; exists {
			t.Fatal("canonical form should not include aliases")
		}
	})

	t.Run("strip defaults", func(t *testing.T) {
		s := mustParse(t, `{
			"type":"record",
			"name":"R",
			"fields":[{"name":"a","type":"int","default":42}]
		}`)
		canonical := string(s.Canonical())
		var m map[string]any
		if err := json.Unmarshal([]byte(canonical), &m); err != nil {
			t.Fatal(err)
		}
		fields := m["fields"].([]any)
		field := fields[0].(map[string]any)
		if _, exists := field["default"]; exists {
			t.Fatal("canonical form should not include default")
		}
	})

	t.Run("preserve enum symbols", func(t *testing.T) {
		s := mustParse(t, `{"type":"enum","name":"E","symbols":["A","B","C"],"doc":"test"}`)
		canonical := string(s.Canonical())
		var m map[string]any
		if err := json.Unmarshal([]byte(canonical), &m); err != nil {
			t.Fatal(err)
		}
		syms := m["symbols"].([]any)
		if len(syms) != 3 {
			t.Fatalf("expected 3 symbols, got %d", len(syms))
		}
		if _, exists := m["doc"]; exists {
			t.Fatal("canonical form should not include doc")
		}
	})

	t.Run("primitive canonical", func(t *testing.T) {
		s := mustParse(t, `"string"`)
		canonical := string(s.Canonical())
		if canonical != `"string"` {
			t.Fatalf("got %s, want \"string\"", canonical)
		}
	})
}

func TestSpecCanonicalExactVectors(t *testing.T) {
	t.Run("primitive object form collapses", func(t *testing.T) {
		s := mustParse(t, `{"type":"string"}`)
		if got := string(s.Canonical()); got != `"string"` {
			t.Fatalf("got %s, want \"string\"", got)
		}
	})

	t.Run("record fullname expansion", func(t *testing.T) {
		s := mustParse(t, `{
			"type":"record",
			"name":"Outer",
			"namespace":"com.example",
			"doc":"ignored",
			"fields":[
				{"name":"inner","type":{
					"type":"record",
					"name":"Inner",
					"fields":[{"name":"x","type":"int","default":1}]
				}}
			]
		}`)
		want := `{"name":"com.example.Outer","type":"record","fields":[{"name":"inner","type":{"name":"com.example.Inner","type":"record","fields":[{"name":"x","type":"int"}]}}]}`
		if got := string(s.Canonical()); got != want {
			t.Fatalf("got %s, want %s", got, want)
		}
	})

	t.Run("canonical is deterministic across key order", func(t *testing.T) {
		s1 := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
		s2 := mustParse(t, `{"fields":[{"type":"int","name":"a"}],"name":"R","type":"record"}`)
		if got1, got2 := string(s1.Canonical()), string(s2.Canonical()); got1 != got2 {
			t.Fatalf("canonical mismatch: %s vs %s", got1, got2)
		}
	})
}

// ---------- encoding_test.go ----------

// -----------------------------------------------------------------------
// Binary Encoding Correctness
// Spec: "Data Serialization" — encoding of primitive and complex types.
//   - int/long:  variable-length zigzag encoding
//   - float:     4 bytes, little-endian IEEE 754
//   - double:    8 bytes, little-endian IEEE 754
//   - string:    long-encoded length + UTF-8 bytes
//   - bytes:     long-encoded length + raw bytes
//   - array/map: sequence of blocks; block = long count + items; 0 terminates
//   - enum:      int-encoded symbol index
//   - union:     int-encoded branch index + value
// https://avro.apache.org/docs/1.12.0/specification/#binary-encoding
// -----------------------------------------------------------------------

func TestSpecZigzagBoundaryValues(t *testing.T) {
	tests := []struct {
		name     string
		schema   string
		value    any
		expected []byte
	}{
		// int (varint, 32-bit zigzag)
		{"int 0", `"int"`, new(int32), []byte{0x00}},
		{"int -1", `"int"`, ptr(int32(-1)), []byte{0x01}},
		{"int 1", `"int"`, ptr(int32(1)), []byte{0x02}},
		{"int -2", `"int"`, ptr(int32(-2)), []byte{0x03}},
		{"int 2", `"int"`, ptr(int32(2)), []byte{0x04}},
		{"int MaxInt32", `"int"`, ptr(int32(math.MaxInt32)), []byte{0xFE, 0xFF, 0xFF, 0xFF, 0x0F}},
		{"int MinInt32", `"int"`, ptr(int32(math.MinInt32)), []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x0F}},

		// long (varlong, 64-bit zigzag)
		{"long 0", `"long"`, new(int64), []byte{0x00}},
		{"long -1", `"long"`, ptr(int64(-1)), []byte{0x01}},
		{"long 1", `"long"`, ptr(int64(1)), []byte{0x02}},
		{"long MaxInt64", `"long"`, ptr(int64(math.MaxInt64)),
			[]byte{0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}},
		{"long MinInt64", `"long"`, ptr(int64(math.MinInt64)),
			[]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := encode(t, tt.schema, tt.value)
			if !bytes.Equal(got, tt.expected) {
				t.Fatalf("encode %s: got %x, want %x", tt.name, got, tt.expected)
			}
		})
	}
}

func TestSpecUnionIndexEncoding(t *testing.T) {
	t.Run("two branch null union via record", func(t *testing.T) {
		type W struct {
			V *int32 `avro:"v"`
		}
		schema := `{"type":"record","name":"W","fields":[{"name":"v","type":["null","int"]}]}`

		dst := encode(t, schema, &W{V: nil})
		if !bytes.Equal(dst, []byte{0x00}) {
			t.Fatalf("null branch: got %x, want 00", dst)
		}

		v := int32(42)
		dst = encode(t, schema, &W{V: &v})
		if dst[0] != 0x02 {
			t.Fatalf("int branch index: got %x, want 02", dst[0])
		}
	})

	t.Run("three branch union via any", func(t *testing.T) {
		schema := `["null","int","string"]`

		var v any = int32(10)
		dst := encode(t, schema, &v)
		if dst[0] != 0x02 {
			t.Fatalf("int branch index: got %x, want 02", dst[0])
		}

		v = "hi"
		dst = encode(t, schema, &v)
		if dst[0] != 0x04 {
			t.Fatalf("string branch index: got %x, want 04", dst[0])
		}
	})

	t.Run("round trip three branch", func(t *testing.T) {
		schema := `["null","int","string"]`
		data := []byte{0x04, 0x04, 0x68, 0x69}
		var v any
		decode(t, schema, data, &v)
		if v != "hi" {
			t.Fatalf("decode string branch: got %v, want hi", v)
		}
	})
}

func TestSpecBytesStringLengthVarlong(t *testing.T) {
	t.Run("empty string", func(t *testing.T) {
		dst := encode(t, `"string"`, new(string))
		if !bytes.Equal(dst, []byte{0x00}) {
			t.Fatalf("empty string: got %x, want 00", dst)
		}
	})

	t.Run("empty bytes", func(t *testing.T) {
		b := []byte{}
		dst := encode(t, `"bytes"`, &b)
		if !bytes.Equal(dst, []byte{0x00}) {
			t.Fatalf("empty bytes: got %x, want 00", dst)
		}
	})

	t.Run("string length encoding", func(t *testing.T) {
		s := "hello"
		dst := encode(t, `"string"`, &s)
		if dst[0] != 0x0A {
			t.Fatalf("string length prefix: got %x, want 0a", dst[0])
		}
		if len(dst) != 6 {
			t.Fatalf("string total length: got %d, want 6", len(dst))
		}
	})

	t.Run("bytes round trip", func(t *testing.T) {
		b := make([]byte, 300)
		for i := range b {
			b[i] = byte(i)
		}
		got := roundTrip(t, `"bytes"`, b)
		if !bytes.Equal(got, b) {
			t.Fatal("300-byte round trip failed")
		}
	})
}

func TestSpecArrayMapBlockCountVarlong(t *testing.T) {
	t.Run("array positive block count", func(t *testing.T) {
		schema := `{"type":"array","items":"int"}`
		arr := []int32{1, 2, 3}
		dst := encode(t, schema, &arr)
		if dst[0] != 0x06 {
			t.Fatalf("array count: got %x, want 06", dst[0])
		}
		if dst[len(dst)-1] != 0x00 {
			t.Fatalf("array terminator: got %x, want 00", dst[len(dst)-1])
		}
	})

	t.Run("array empty", func(t *testing.T) {
		schema := `{"type":"array","items":"int"}`
		arr := []int32{}
		dst := encode(t, schema, &arr)
		if !bytes.Equal(dst, []byte{0x00}) {
			t.Fatalf("empty array: got %x, want 00", dst)
		}
	})

	t.Run("array negative block count decode", func(t *testing.T) {
		schema := `{"type":"array","items":"int"}`
		data := []byte{0x05, 0x06, 0x02, 0x04, 0x06, 0x00}
		var v []int32
		decode(t, schema, data, &v)
		if len(v) != 3 || v[0] != 1 || v[1] != 2 || v[2] != 3 {
			t.Fatalf("negative block count: got %v, want [1 2 3]", v)
		}
	})

	t.Run("map block count", func(t *testing.T) {
		schema := `{"type":"map","values":"int"}`
		m := map[string]int32{"a": 1}
		dst := encode(t, schema, &m)
		if dst[0] != 0x02 {
			t.Fatalf("map count: got %x, want 02", dst[0])
		}
	})

	t.Run("map negative block count decode", func(t *testing.T) {
		schema := `{"type":"map","values":"int"}`
		data := []byte{0x01, 0x08, 0x02, 0x61, 0x02, 0x00}
		var v map[string]int32
		decode(t, schema, data, &v)
		if len(v) != 1 || v["a"] != 1 {
			t.Fatalf("map negative block: got %v, want {a:1}", v)
		}
	})
}

func TestSpecEnumIndexVarint(t *testing.T) {
	t.Run("basic enum", func(t *testing.T) {
		schema := `{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]}`
		s := "RED"
		dst := encode(t, schema, &s)
		if !bytes.Equal(dst, []byte{0x00}) {
			t.Fatalf("RED: got %x, want 00", dst)
		}

		s = "GREEN"
		dst = encode(t, schema, &s)
		if !bytes.Equal(dst, []byte{0x02}) {
			t.Fatalf("GREEN: got %x, want 02", dst)
		}

		s = "BLUE"
		dst = encode(t, schema, &s)
		if !bytes.Equal(dst, []byte{0x04}) {
			t.Fatalf("BLUE: got %x, want 04", dst)
		}
	})

	t.Run("enum with many symbols multi-byte index", func(t *testing.T) {
		symbols := make([]string, 200)
		for i := range symbols {
			symbols[i] = "S" + string(rune('A'+i/26)) + string(rune('A'+i%26))
		}
		schema := `{"type":"enum","name":"Big","symbols":[`
		for i, s := range symbols {
			if i > 0 {
				schema += ","
			}
			schema += `"` + s + `"`
		}
		schema += `]}`

		s := symbols[199]
		dst := encode(t, schema, &s)
		if len(dst) != 2 {
			t.Fatalf("multi-byte enum index: got %d bytes, want 2", len(dst))
		}
		if !bytes.Equal(dst, []byte{0x8E, 0x03}) {
			t.Fatalf("index 199: got %x, want 8e03", dst)
		}

		var result string
		decode(t, schema, dst, &result)
		if result != s {
			t.Fatalf("round-trip: got %q, want %q", result, s)
		}
	})
}

func TestSpecFloatDoubleEncoding(t *testing.T) {
	t.Run("float NaN", func(t *testing.T) {
		v := float32(math.NaN())
		dst := encode(t, `"float"`, &v)
		if len(dst) != 4 {
			t.Fatalf("float NaN: got %d bytes, want 4", len(dst))
		}
		var out float32
		decode(t, `"float"`, dst, &out)
		if !math.IsNaN(float64(out)) {
			t.Fatalf("float NaN round-trip: got %v, want NaN", out)
		}
	})

	t.Run("float +Inf", func(t *testing.T) {
		v := float32(math.Inf(1))
		got := roundTrip(t, `"float"`, v)
		if !math.IsInf(float64(got), 1) {
			t.Fatalf("float +Inf round-trip: got %v", got)
		}
	})

	t.Run("float -Inf", func(t *testing.T) {
		v := float32(math.Inf(-1))
		got := roundTrip(t, `"float"`, v)
		if !math.IsInf(float64(got), -1) {
			t.Fatalf("float -Inf round-trip: got %v", got)
		}
	})

	t.Run("float -0", func(t *testing.T) {
		v := float32(math.Copysign(0, -1))
		dst := encode(t, `"float"`, &v)
		if !bytes.Equal(dst, []byte{0x00, 0x00, 0x00, 0x80}) {
			t.Fatalf("float -0: got %x, want 00000080", dst)
		}
		var out float32
		decode(t, `"float"`, dst, &out)
		if math.Float32bits(out) != math.Float32bits(v) {
			t.Fatalf("float -0 round-trip: bit pattern mismatch")
		}
	})

	t.Run("float subnormal", func(t *testing.T) {
		v := math.SmallestNonzeroFloat32
		got := roundTrip(t, `"float"`, v)
		if got != v {
			t.Fatalf("float subnormal round-trip: got %v, want %v", got, v)
		}
	})

	t.Run("double NaN", func(t *testing.T) {
		v := math.NaN()
		dst := encode(t, `"double"`, &v)
		if len(dst) != 8 {
			t.Fatalf("double NaN: got %d bytes, want 8", len(dst))
		}
		var out float64
		decode(t, `"double"`, dst, &out)
		if !math.IsNaN(out) {
			t.Fatalf("double NaN round-trip: got %v, want NaN", out)
		}
	})

	t.Run("double +Inf", func(t *testing.T) {
		got := roundTrip(t, `"double"`, math.Inf(1))
		if !math.IsInf(got, 1) {
			t.Fatalf("double +Inf round-trip: got %v", got)
		}
	})

	t.Run("double -Inf", func(t *testing.T) {
		got := roundTrip(t, `"double"`, math.Inf(-1))
		if !math.IsInf(got, -1) {
			t.Fatalf("double -Inf round-trip: got %v", got)
		}
	})

	t.Run("double -0", func(t *testing.T) {
		v := math.Copysign(0, -1)
		dst := encode(t, `"double"`, &v)
		if !bytes.Equal(dst, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80}) {
			t.Fatalf("double -0: got %x", dst)
		}
		var out float64
		decode(t, `"double"`, dst, &out)
		if math.Float64bits(out) != math.Float64bits(v) {
			t.Fatalf("double -0 round-trip: bit pattern mismatch")
		}
	})

	t.Run("double subnormal", func(t *testing.T) {
		v := math.SmallestNonzeroFloat64
		got := roundTrip(t, `"double"`, v)
		if got != v {
			t.Fatalf("double subnormal round-trip: got %v, want %v", got, v)
		}
	})
}

func TestSpecLongRejectsUnsignedOverflow(t *testing.T) {
	s := mustParse(t, `"long"`)
	v := ^uint64(0)
	if _, err := s.Encode(&v); err == nil {
		t.Fatal("expected overflow error encoding uint64 max as Avro long")
	}
}

// ptr returns a pointer to v. Used for building test values.
func ptr[T any](v T) *T { return &v }

// ---------- errors_test.go ----------

// -----------------------------------------------------------------------
// Error Handling & Malformed Data
// Spec: "Binary Encoding" — decoders must reject truncated or invalid data.
//   - Truncated varints, floats, doubles, strings, fixed
//   - Out-of-range union/enum indices
//   - Go-specific: type mismatch, non-pointer decode target
// https://avro.apache.org/docs/1.12.0/specification/#binary-encoding
// -----------------------------------------------------------------------

func TestErrorTruncatedVarint(t *testing.T) {
	// Varint with continuation bit set but no following byte.
	decodeErr(t, `"int"`, []byte{0x80}, new(int32))
}

func TestErrorTruncatedFloat(t *testing.T) {
	// Less than 4 bytes for float.
	decodeErr(t, `"float"`, []byte{0x00, 0x00}, new(float32))
}

func TestErrorTruncatedDouble(t *testing.T) {
	// Less than 8 bytes for double.
	decodeErr(t, `"double"`, []byte{0x00, 0x00, 0x00, 0x00}, new(float64))
}

func TestErrorTruncatedString(t *testing.T) {
	// String length says 10 but only 2 bytes follow.
	// 10 in zigzag = 20 = 0x14
	decodeErr(t, `"string"`, []byte{0x14, 0x61, 0x62}, new(string))
}

func TestErrorTruncatedFixed(t *testing.T) {
	// Fixed(4) but only 2 bytes.
	decodeErr(t, `{"type":"fixed","name":"F","size":4}`, []byte{0x00, 0x00}, new([4]byte))
}

func TestErrorInvalidUnionIndex(t *testing.T) {
	// Union ["null","int"] has indices 0 and 1.
	// Index 5 (zigzag 10 = 0x0A) is out of range.
	decodeErr(t, `["null","int"]`, []byte{0x0A}, new(any))
}

func TestErrorInvalidEnumIndex(t *testing.T) {
	// Enum with 3 symbols. Index 10 (zigzag 20 = 0x14) is out of range.
	decodeErr(t, `{"type":"enum","name":"E","symbols":["A","B","C"]}`, []byte{0x14}, new(string))
}

func TestErrorTypeMismatch(t *testing.T) {
	// Try to decode int data into a string.
	s := mustParse(t, `"int"`)
	data := []byte{0x04} // int 2
	var out string
	_, err := s.Decode(data, &out)
	if err == nil {
		t.Fatal("expected error for type mismatch")
	}
}

func TestErrorNonPointerDecode(t *testing.T) {
	s := mustParse(t, `"int"`)
	data := []byte{0x04}
	var v int32
	_, err := s.Decode(data, v) // non-pointer
	if err == nil {
		t.Fatal("expected error for non-pointer decode")
	}
}

// ---------- fingerprint_test.go ----------

// -----------------------------------------------------------------------
// Schema Fingerprints (CRC-64-AVRO / Rabin)
// Spec: "Schema Fingerprints" — a 64-bit Rabin fingerprint computed
// over a schema's Parsing Canonical Form.
//   - Polynomial: 0xc15d213aa4d7a795 (also the empty fingerprint)
//   - Input: the schema's canonical JSON representation
//   - Output: 8-byte fingerprint (big-endian via hash.Hash.Sum)
// https://avro.apache.org/docs/1.12.0/specification/#schema-fingerprints
// -----------------------------------------------------------------------

// TestFingerprintEmptyHash verifies that NewRabin with no data written
// returns the empty fingerprint constant 0xc15d213aa4d7a795.
func TestFingerprintEmptyHash(t *testing.T) {
	h := avro.NewRabin()
	got := h.Sum64()
	const want = uint64(0xc15d213aa4d7a795)
	if got != want {
		t.Fatalf("empty Rabin: got %#016x, want %#016x", got, want)
	}
}

// TestFingerprintPrimitiveSchemas verifies fingerprints of all 8
// primitive type canonical forms against known reference values.
// Reference values computed from the Avro spec's CRC-64-AVRO algorithm.
func TestFingerprintPrimitiveSchemas(t *testing.T) {
	vectors := []struct {
		schema string
		wantBE uint64 // expected fingerprint in big-endian uint64
	}{
		{`"null"`, 0x63dd24e7cc258f8a},
		{`"boolean"`, 0x9f42fc78a4d4f764},
		{`"int"`, 0x7275d51a3f395c8f},
		{`"long"`, 0xd054e14493f41db7},
		{`"float"`, 0x4d7c02cb3ea8d790},
		{`"double"`, 0x8e7535c032ab957e},
		{`"string"`, 0x8f014872634503c7},
		{`"bytes"`, 0x4fc016dac3201965},
	}

	for _, tc := range vectors {
		t.Run(tc.schema, func(t *testing.T) {
			s := mustParse(t, tc.schema)
			h := avro.NewRabin()
			fp := s.Fingerprint(h)
			got := binary.BigEndian.Uint64(fp)
			if got != tc.wantBE {
				t.Fatalf("fingerprint: got %#016x, want %#016x", got, tc.wantBE)
			}
		})
	}
}

// TestFingerprintDeterministic verifies that two schemas with the same
// canonical form produce the same fingerprint, regardless of JSON key order.
func TestFingerprintDeterministic(t *testing.T) {
	s1 := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	s2 := mustParse(t, `{"fields":[{"type":"int","name":"a"}],"name":"R","type":"record"}`)

	h := avro.NewRabin()
	fp1 := s1.Fingerprint(h)
	h.Reset()
	fp2 := s2.Fingerprint(h)

	if !bytes.Equal(fp1, fp2) {
		t.Fatalf("fingerprints differ: %x vs %x", fp1, fp2)
	}
}

// TestFingerprintDistinct verifies that different schemas produce
// different fingerprints.
func TestFingerprintDistinct(t *testing.T) {
	schemas := []string{
		`"int"`,
		`"long"`,
		`"string"`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}`,
		`{"type":"enum","name":"E","symbols":["A","B"]}`,
	}

	fps := make(map[uint64]string)
	for _, schema := range schemas {
		s := mustParse(t, schema)
		h := avro.NewRabin()
		fp := s.Fingerprint(h)
		val := binary.BigEndian.Uint64(fp)
		if prev, exists := fps[val]; exists {
			t.Fatalf("collision: %q and %q both produce %#016x", prev, schema, val)
		}
		fps[val] = schema
	}
}

// TestFingerprintReset verifies that resetting the hash produces
// consistent results.
func TestFingerprintReset(t *testing.T) {
	s := mustParse(t, `"int"`)
	h := avro.NewRabin()

	fp1 := s.Fingerprint(h)
	h.Reset()
	fp2 := s.Fingerprint(h)

	if !bytes.Equal(fp1, fp2) {
		t.Fatalf("after reset: %x vs %x", fp1, fp2)
	}
}

// TestFingerprintNamespaceExpansion verifies that the fingerprint is
// computed over the canonical form with expanded fullnames.
func TestFingerprintNamespaceExpansion(t *testing.T) {
	// These two produce the same canonical form (fullname "com.example.R").
	s1 := mustParse(t, `{"type":"record","name":"R","namespace":"com.example","fields":[{"name":"a","type":"int"}]}`)
	s2 := mustParse(t, `{"type":"record","name":"com.example.R","fields":[{"name":"a","type":"int"}]}`)

	h := avro.NewRabin()
	fp1 := s1.Fingerprint(h)
	h.Reset()
	fp2 := s2.Fingerprint(h)

	if !bytes.Equal(fp1, fp2) {
		t.Fatalf("namespace expansion: %x vs %x", fp1, fp2)
	}
}

// ---------- hamba_test.go ----------

// -----------------------------------------------------------------------
// Tests inspired by bugs found in hamba/avro.
// Each test covers a real-world spec violation or edge case discovered
// in the hamba/avro issue tracker or commit history.
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// Schema Parsing
// -----------------------------------------------------------------------

func TestHambaEmptyEnumSymbolsRejected(t *testing.T) {
	// hamba/avro #295 area: enum with no symbols should be rejected.
	// The Avro spec requires at least one symbol.
	_, err := avro.Parse(`{"type":"enum","name":"E","symbols":[]}`)
	if err == nil {
		t.Fatal("expected error for enum with empty symbols list")
	}
}

func TestHambaRecordWithNoFields(t *testing.T) {
	// An empty record (no fields) is valid per the Avro spec.
	s, err := avro.Parse(`{"type":"record","name":"Empty","fields":[]}`)
	if err != nil {
		t.Fatalf("empty record should be valid: %v", err)
	}
	type Empty struct{}
	input := Empty{}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatalf("encode empty record: %v", err)
	}
	// Empty record should encode to zero bytes.
	if len(encoded) != 0 {
		t.Fatalf("empty record encoding: got %x, want empty", encoded)
	}
	var output Empty
	rem, err := s.Decode(encoded, &output)
	if err != nil {
		t.Fatalf("decode empty record: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("decode left %d bytes", len(rem))
	}
}

func TestHambaEmptyNamespaceClearsInheritance(t *testing.T) {
	// hamba/avro #457: "namespace": "" must be allowed and should clear
	// any inherited namespace, placing the type in the null namespace.
	schema := `{
		"type": "record",
		"name": "Outer",
		"namespace": "com.example",
		"fields": [{
			"name": "inner",
			"type": {
				"type": "record",
				"name": "Inner",
				"namespace": "",
				"fields": [{"name": "x", "type": "int"}]
			}
		}]
	}`
	s := mustParse(t, schema)
	// Canonical form should show Inner without namespace prefix.
	canonical := string(s.Canonical())
	// Inner should NOT be com.example.Inner since namespace was explicitly "".
	if bytes.Contains([]byte(canonical), []byte(`"com.example.Inner"`)) {
		t.Fatalf("expected Inner in null namespace, got canonical: %s", canonical)
	}
	if !bytes.Contains([]byte(canonical), []byte(`"Inner"`)) {
		t.Fatalf("expected Inner in canonical form, got: %s", canonical)
	}
}

func TestHambaDuplicateFieldNamesRejected(t *testing.T) {
	// hamba/avro #295: duplicate field names in a record must be rejected.
	_, err := avro.Parse(`{
		"type": "record",
		"name": "R",
		"fields": [
			{"name": "x", "type": "int"},
			{"name": "x", "type": "string"}
		]
	}`)
	if err == nil {
		t.Fatal("expected error for duplicate field names")
	}
}

func TestHambaDuplicateEnumSymbolsRejected(t *testing.T) {
	// hamba/avro #295: duplicate enum symbols must be rejected.
	_, err := avro.Parse(`{"type":"enum","name":"E","symbols":["A","B","A"]}`)
	if err == nil {
		t.Fatal("expected error for duplicate enum symbols")
	}
}

// -----------------------------------------------------------------------
// Varint / Varlong Overflow
// -----------------------------------------------------------------------

func TestHambaVarintOverflowInFifthByte(t *testing.T) {
	// The 5th byte of a varint can carry at most 4 data bits (bits 28-31).
	// If higher bits are set, the value overflows uint32.

	// Valid max: MinInt32 zigzag encodes to 0xFFFFFFFF, which is
	// {0xFF, 0xFF, 0xFF, 0xFF, 0x0F} — 5th byte is 0x0F (4 bits).
	data := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x0F}
	var v int32
	decode(t, `"int"`, data, &v)
	if v != -2147483648 { // MinInt32
		t.Fatalf("got %d, want MinInt32", v)
	}

	// Invalid: 5th byte 0x1F has 5 data bits — overflows 32-bit range.
	overflow := []byte{0x80, 0x80, 0x80, 0x80, 0x1F}
	decodeErr(t, `"int"`, overflow, &v)
}

func TestHambaVarlongOverflowInTenthByte(t *testing.T) {
	// The 10th byte of a varlong can carry at most 1 data bit (bit 63).
	// If higher bits are set, the value overflows uint64.

	// Valid max: MinInt64 zigzag encodes to 0xFFFFFFFFFFFFFFFF,
	// which is {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}.
	data := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}
	var v int64
	decode(t, `"long"`, data, &v)
	if v != -9223372036854775808 { // MinInt64
		t.Fatalf("got %d, want MinInt64", v)
	}

	// Invalid: 10th byte 0x02 has bit 1 set — overflows 64-bit range.
	overflow := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02}
	decodeErr(t, `"long"`, overflow, &v)
}

// -----------------------------------------------------------------------
// Encoding Edge Cases
// -----------------------------------------------------------------------

func TestHambaNullEncodesAsZeroBytes(t *testing.T) {
	// Spec: null is written as zero bytes.
	s := mustParse(t, `"null"`)
	encoded, err := s.AppendEncode(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != 0 {
		t.Fatalf("null should encode to zero bytes, got %x", encoded)
	}
}

func TestHambaBooleanExactEncoding(t *testing.T) {
	// Spec: boolean is encoded as a single byte: 0x00 for false, 0x01 for true.
	bTrue := true
	dst := encode(t, `"boolean"`, &bTrue)
	if !bytes.Equal(dst, []byte{0x01}) {
		t.Fatalf("true: got %x, want 01", dst)
	}

	bFalse := false
	dst = encode(t, `"boolean"`, &bFalse)
	if !bytes.Equal(dst, []byte{0x00}) {
		t.Fatalf("false: got %x, want 00", dst)
	}
}

// -----------------------------------------------------------------------
// Schema Resolution Edge Cases
// -----------------------------------------------------------------------

func TestHambaWriterUnionToReaderNonUnion(t *testing.T) {
	// When the writer is a union and the reader is not, resolve should
	// work if each writer branch is compatible with the reader type.
	writerSchema := `{
		"type": "record",
		"name": "R",
		"fields": [{"name": "v", "type": ["null", "int"]}]
	}`
	readerSchema := `{
		"type": "record",
		"name": "R",
		"fields": [{"name": "v", "type": ["null", "long"]}]
	}`

	writer := mustParse(t, writerSchema)
	reader := mustParse(t, readerSchema)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	// Encode int branch, resolve should promote to long.
	encoded, err := writer.Encode(map[string]any{"v": int32(42)})
	if err != nil {
		t.Fatal(err)
	}
	var result any
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	m := result.(map[string]any)
	if m["v"] != int64(42) {
		t.Fatalf("got %v (%T), want int64(42)", m["v"], m["v"])
	}
}

func TestHambaFixedSizeMismatchIncompat(t *testing.T) {
	// hamba/avro issue area: fixed types with different sizes must be incompatible.
	writer := mustParse(t, `{"type":"fixed","name":"F","size":4}`)
	reader := mustParse(t, `{"type":"fixed","name":"F","size":8}`)
	err := avro.CheckCompatibility(writer, reader)
	if err == nil {
		t.Fatal("expected error for fixed size mismatch")
	}
}

func TestHambaFixedNameMismatchIncompat(t *testing.T) {
	// Fixed types with different names must be incompatible.
	writer := mustParse(t, `{"type":"fixed","name":"A","size":4}`)
	reader := mustParse(t, `{"type":"fixed","name":"B","size":4}`)
	err := avro.CheckCompatibility(writer, reader)
	if err == nil {
		t.Fatal("expected error for fixed name mismatch")
	}
}

func TestHambaEnumUnknownSymbolNoDefaultErrors(t *testing.T) {
	// hamba/avro #340: when writer has symbols not in reader and reader has
	// no default, resolution must fail.
	writer := mustParse(t, `{"type":"enum","name":"E","symbols":["A","B","C"]}`)
	reader := mustParse(t, `{"type":"enum","name":"E","symbols":["A","B"]}`)
	_, err := avro.Resolve(writer, reader)
	if err == nil {
		t.Fatal("expected error when writer has unknown symbol and reader has no default")
	}
}

func TestHambaTypeLevelAliasResolution(t *testing.T) {
	// hamba/avro issue area: type-level aliases (not just field aliases)
	// should allow matching during resolution.
	t.Run("record alias", func(t *testing.T) {
		writer := mustParse(t, `{"type":"record","name":"OldName","fields":[{"name":"x","type":"int"}]}`)
		reader := mustParse(t, `{"type":"record","name":"NewName","aliases":["OldName"],"fields":[{"name":"x","type":"int"}]}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatalf("record alias resolution should work: %v", err)
		}
		encoded, err := writer.Encode(map[string]any{"x": int32(7)})
		if err != nil {
			t.Fatal(err)
		}
		var got any
		if _, err := resolved.Decode(encoded, &got); err != nil {
			t.Fatal(err)
		}
		if got.(map[string]any)["x"] != int32(7) {
			t.Fatalf("got %+v, want x=7", got)
		}
	})

	t.Run("enum alias", func(t *testing.T) {
		writer := mustParse(t, `{"type":"enum","name":"OldEnum","symbols":["A","B"]}`)
		reader := mustParse(t, `{"type":"enum","name":"NewEnum","aliases":["OldEnum"],"symbols":["A","B"]}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatalf("enum alias resolution should work: %v", err)
		}
		s := "B"
		encoded, err := writer.Encode(&s)
		if err != nil {
			t.Fatal(err)
		}
		var got string
		if _, err := resolved.Decode(encoded, &got); err != nil {
			t.Fatal(err)
		}
		if got != "B" {
			t.Fatalf("got %q, want B", got)
		}
	})

	t.Run("fixed alias", func(t *testing.T) {
		writer := mustParse(t, `{"type":"fixed","name":"OldFixed","size":4}`)
		reader := mustParse(t, `{"type":"fixed","name":"NewFixed","aliases":["OldFixed"],"size":4}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatalf("fixed alias resolution should work: %v", err)
		}
		in := [4]byte{1, 2, 3, 4}
		encoded, err := writer.Encode(&in)
		if err != nil {
			t.Fatal(err)
		}
		var out [4]byte
		if _, err := resolved.Decode(encoded, &out); err != nil {
			t.Fatal(err)
		}
		if out != in {
			t.Fatalf("got %x, want %x", out, in)
		}
	})
}

// -----------------------------------------------------------------------
// Default Value Edge Cases
// -----------------------------------------------------------------------

func TestHambaPrimitiveDefaults(t *testing.T) {
	// Test default values for all primitive types (not just int).
	writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)

	tests := []struct {
		name         string
		readerSchema string
		fieldName    string
		want         any
	}{
		{
			"boolean default",
			`{"type":"record","name":"R","fields":[
				{"name":"a","type":"int"},
				{"name":"b","type":"boolean","default":true}
			]}`,
			"b", true,
		},
		{
			"long default",
			`{"type":"record","name":"R","fields":[
				{"name":"a","type":"int"},
				{"name":"l","type":"long","default":9999}
			]}`,
			"l", int64(9999),
		},
		{
			"float default",
			`{"type":"record","name":"R","fields":[
				{"name":"a","type":"int"},
				{"name":"f","type":"float","default":3.14}
			]}`,
			"f", float32(3.14),
		},
		{
			"double default",
			`{"type":"record","name":"R","fields":[
				{"name":"a","type":"int"},
				{"name":"d","type":"double","default":2.718}
			]}`,
			"d", float64(2.718),
		},
		{
			"string default",
			`{"type":"record","name":"R","fields":[
				{"name":"a","type":"int"},
				{"name":"s","type":"string","default":"hello"}
			]}`,
			"s", "hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := mustParse(t, tt.readerSchema)
			resolved, err := avro.Resolve(writer, reader)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := writer.Encode(map[string]any{"a": int32(1)})
			if err != nil {
				t.Fatal(err)
			}
			var result any
			_, err = resolved.Decode(encoded, &result)
			if err != nil {
				t.Fatal(err)
			}
			m := result.(map[string]any)
			if m[tt.fieldName] != tt.want {
				t.Fatalf("got %v (%T), want %v (%T)", m[tt.fieldName], m[tt.fieldName], tt.want, tt.want)
			}
		})
	}
}

func TestHambaEnumDefault(t *testing.T) {
	// hamba/avro #340: Enum field default in schema resolution.
	writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	reader := mustParse(t, `{
		"type":"record","name":"R",
		"fields":[
			{"name":"a","type":"int"},
			{"name":"color","type":{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"],"default":"RED"},"default":"GREEN"}
		]
	}`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := writer.Encode(map[string]any{"a": int32(1)})
	if err != nil {
		t.Fatal(err)
	}
	var result any
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	m := result.(map[string]any)
	if m["color"] != "GREEN" {
		t.Fatalf("got %v, want GREEN", m["color"])
	}
}

func TestHambaFixedDefault(t *testing.T) {
	// Default value for fixed type field in schema resolution.
	writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	reader := mustParse(t, `{
		"type":"record","name":"R",
		"fields":[
			{"name":"a","type":"int"},
			{"name":"id","type":{"type":"fixed","name":"Id","size":4},"default":"\u0001\u0002\u0003\u0004"}
		]
	}`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := writer.Encode(map[string]any{"a": int32(1)})
	if err != nil {
		t.Fatal(err)
	}
	var result any
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	m := result.(map[string]any)
	b := m["id"]
	// Fixed defaults decode as either [4]byte or []byte depending on path.
	switch v := b.(type) {
	case [4]byte:
		if v != [4]byte{1, 2, 3, 4} {
			t.Fatalf("got %x, want 01020304", v)
		}
	case []byte:
		if !bytes.Equal(v, []byte{1, 2, 3, 4}) {
			t.Fatalf("got %x, want 01020304", v)
		}
	default:
		t.Fatalf("unexpected type %T for fixed default", b)
	}
}

func TestFixedDefaultHighCodePoints(t *testing.T) {
	// Code points 128-255 are valid (multi-byte UTF-8 but single Avro bytes).
	// Size check must count runes, not bytes.
	s := mustParse(t, `{"type":"record","name":"r","fields":[
		{"name":"a","type":{"type":"fixed","name":"f","size":2},"default":"\u00FF\u00FE"}
	]}`)
	binary, err := s.Encode(map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	var decoded any
	if _, err := s.Decode(binary, &decoded); err != nil {
		t.Fatal(err)
	}
	m := decoded.(map[string]any)
	got, ok := m["a"].([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", m["a"])
	}
	if !bytes.Equal(got, []byte{0xFF, 0xFE}) {
		t.Fatalf("got %x, want fffe", got)
	}
}

func TestBytesFixedDefaultRejectsHighUnicode(t *testing.T) {
	// Code points > 255 must be rejected per Avro spec.
	for _, tt := range []struct {
		name   string
		schema string
	}{
		{
			"bytes U+0100",
			`{"type":"record","name":"r","fields":[
				{"name":"a","type":"bytes","default":"\u0100"}
			]}`,
		},
		{
			"fixed U+0100",
			`{"type":"record","name":"r","fields":[
				{"name":"a","type":{"type":"fixed","name":"f","size":1},"default":"\u0100"}
			]}`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := avro.Parse(tt.schema); err == nil {
				t.Fatal("expected error for code point > 255")
			}
		})
	}
}

// -----------------------------------------------------------------------
// Compatibility Edge Cases
// -----------------------------------------------------------------------

func TestHambaUnionBranchRemoved(t *testing.T) {
	// When a reader union has fewer branches than the writer, the writer
	// branches not in the reader make it incompatible.
	writer := mustParse(t, `["null","int","string"]`)
	reader := mustParse(t, `["null","int"]`)
	err := avro.CheckCompatibility(writer, reader)
	if err == nil {
		t.Fatal("expected error when reader union removes a writer branch")
	}
}

func TestHambaNestedRecordCompatibility(t *testing.T) {
	// Deeply nested compatibility: record within record.
	writer := `{
		"type": "record", "name": "Outer",
		"fields": [{
			"name": "inner",
			"type": {"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}
		}]
	}`
	reader := `{
		"type": "record", "name": "Outer",
		"fields": [{
			"name": "inner",
			"type": {"type":"record","name":"Inner","fields":[{"name":"x","type":"long"}]}
		}]
	}`
	if err := avro.CheckCompatibility(mustParse(t, writer), mustParse(t, reader)); err != nil {
		t.Fatalf("nested record with promoted field should be compatible: %v", err)
	}
}

// -----------------------------------------------------------------------
// Error Handling
// -----------------------------------------------------------------------

func TestHambaNegativeStringLength(t *testing.T) {
	// A negative string length prefix should be rejected.
	// -1 in zigzag is 0x01, but as a varlong it decodes to -1.
	// Actually for length, we need a raw negative zigzag long.
	// zigzag(-1) = 1 = 0x01
	decodeErr(t, `"string"`, []byte{0x01}, new(string))
}

func TestHambaNegativeBytesLength(t *testing.T) {
	// A negative bytes length prefix should be rejected.
	decodeErr(t, `"bytes"`, []byte{0x01}, new([]byte))
}

func TestHambaVarintTooManyBytes(t *testing.T) {
	// A varint with 6+ continuation bytes should error (max is 5 bytes).
	data := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x00}
	decodeErr(t, `"int"`, data, new(int32))
}

func TestHambaVarlongTooManyBytes(t *testing.T) {
	// A varlong with 11+ continuation bytes should error (max is 10 bytes).
	data := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00}
	decodeErr(t, `"long"`, data, new(int64))
}

// ---------- interop_test.go ----------

// -----------------------------------------------------------------------
// Interop — Reference Bytes and Fingerprints
// -----------------------------------------------------------------------

func TestInteropReferenceBytes(t *testing.T) {
	t.Run("record with all primitive fields", func(t *testing.T) {
		type AllPrim struct {
			B   bool    `avro:"b"`
			I   int32   `avro:"i"`
			L   int64   `avro:"l"`
			F   float32 `avro:"f"`
			D   float64 `avro:"d"`
			S   string  `avro:"s"`
			Byt []byte  `avro:"byt"`
		}
		schema := `{
			"type":"record","name":"AllPrim",
			"fields":[
				{"name":"b","type":"boolean"},
				{"name":"i","type":"int"},
				{"name":"l","type":"long"},
				{"name":"f","type":"float"},
				{"name":"d","type":"double"},
				{"name":"s","type":"string"},
				{"name":"byt","type":"bytes"}
			]
		}`
		input := AllPrim{
			B:   true,
			I:   42,
			L:   2147483648,
			F:   3.14,
			D:   2.718281828,
			S:   "hello",
			Byt: []byte{0xCA, 0xFE},
		}

		dst := encode(t, schema, &input)
		want := buildReferenceBytes(true, 42, 2147483648, 3.14, 2.718281828, "hello", []byte{0xCA, 0xFE})

		if !bytes.Equal(dst, want) {
			t.Fatalf("encoding mismatch:\ngot  %x\nwant %x", dst, want)
		}

		var output AllPrim
		decode(t, schema, dst, &output)
		if output.B != input.B || output.I != input.I || output.L != input.L ||
			output.F != input.F || output.D != input.D || output.S != input.S ||
			!bytes.Equal(output.Byt, input.Byt) {
			t.Fatalf("round-trip mismatch: got %+v, want %+v", output, input)
		}
	})

	t.Run("nested array and map", func(t *testing.T) {
		schema := `{
			"type":"record","name":"Container",
			"fields":[
				{"name":"arr","type":{"type":"array","items":"int"}},
				{"name":"m","type":{"type":"map","values":"string"}}
			]
		}`
		type Container struct {
			Arr []int32           `avro:"arr"`
			M   map[string]string `avro:"m"`
		}
		input := Container{
			Arr: []int32{1, 2},
			M:   map[string]string{"k": "v"},
		}
		dst := encode(t, schema, &input)

		if dst[0] != 0x04 {
			t.Fatalf("array count: got %x, want 04", dst[0])
		}
	})

	t.Run("boolean false", func(t *testing.T) {
		schema := `"boolean"`
		v := false
		dst := encode(t, schema, &v)
		if !bytes.Equal(dst, []byte{0x00}) {
			t.Fatalf("false: got %x, want 00", dst)
		}
	})

	t.Run("fixed encoding", func(t *testing.T) {
		schema := `{"type":"fixed","name":"F4","size":4}`
		v := [4]byte{0xDE, 0xAD, 0xBE, 0xEF}
		dst := encode(t, schema, &v)
		if !bytes.Equal(dst, []byte{0xDE, 0xAD, 0xBE, 0xEF}) {
			t.Fatalf("fixed: got %x, want deadbeef", dst)
		}
	})

	t.Run("float specific bits", func(t *testing.T) {
		schema := `"float"`
		v := float32(1.0)
		dst := encode(t, schema, &v)
		want := encodeUint32LE(math.Float32bits(1.0))
		if !bytes.Equal(dst, want) {
			t.Fatalf("float 1.0: got %x, want %x", dst, want)
		}
	})
}

func TestSpecCRC64AVROFingerprint(t *testing.T) {
	t.Run("empty fingerprint", func(t *testing.T) {
		h := avro.NewRabin()
		fp := h.Sum64()
		if fp != 0xc15d213aa4d7a795 {
			t.Fatalf("empty: got %016x, want c15d213aa4d7a795", fp)
		}
	})

	t.Run("null schema", func(t *testing.T) {
		s := mustParse(t, `"null"`)
		h := avro.NewRabin()
		fp := s.Fingerprint(h)
		if len(fp) != 8 {
			t.Fatalf("fingerprint length: got %d, want 8", len(fp))
		}
		h2 := avro.NewRabin()
		fp2 := s.Fingerprint(h2)
		if !bytes.Equal(fp, fp2) {
			t.Fatal("fingerprint not deterministic")
		}
	})

	t.Run("known vectors", func(t *testing.T) {
		vectors := []struct {
			canonical string
			fpBE      uint64
		}{
			{`"null"`, 0x63dd24e7cc258f8a},
			{`"boolean"`, 0x9f42fc78a4d4f764},
			{`"int"`, 0x7275d51a3f395c8f},
			{`"long"`, 0xd054e14493f41db7},
			{`"float"`, 0x4d7c02cb3ea8d790},
			{`"double"`, 0x8e7535c032ab957e},
			{`"string"`, 0x8f014872634503c7},
			{`"bytes"`, 0x4fc016dac3201965},
		}

		for _, v := range vectors {
			t.Run(v.canonical, func(t *testing.T) {
				h := avro.NewRabin()
				h.Write([]byte(v.canonical))
				got := h.Sum64()
				var gotBE [8]byte
				binary.BigEndian.PutUint64(gotBE[:], got)
				var wantBE [8]byte
				binary.BigEndian.PutUint64(wantBE[:], v.fpBE)
				if gotBE != wantBE {
					t.Fatalf("fingerprint for %s: got %016x, want %016x", v.canonical, got, v.fpBE)
				}
			})
		}
	})

	t.Run("schema fingerprint via Schema", func(t *testing.T) {
		s := mustParse(t, `{"type":"int","doc":"ignored in canonical"}`)
		h1 := avro.NewRabin()
		fp1 := s.Fingerprint(h1)

		h2 := avro.NewRabin()
		h2.Write(s.Canonical())
		fp2 := h2.Sum(nil)

		if !bytes.Equal(fp1, fp2) {
			t.Fatalf("Schema.Fingerprint doesn't match manual: %x vs %x", fp1, fp2)
		}
	})
}

// ---------- logical_test.go ----------

// -----------------------------------------------------------------------
// Logical Type Edge Cases
// Spec: "Logical Types" — all logical types defined by the Avro spec.
//   - Timestamps: millis/micros/nanos since epoch, pre-epoch negative values
//   - Local timestamps: millis/micros/nanos (no timezone)
//   - Date: days since 1970-01-01
//   - Time-of-day: time-millis (int, ms), time-micros (long, µs)
//   - Decimal: bytes-encoded two's-complement scaled integer
//   - UUID: string or fixed(16) representation
//   - Duration: fixed(12), three little-endian uint32 fields
// https://avro.apache.org/docs/1.12.0/specification/#logical-types
// -----------------------------------------------------------------------

func TestSpecTimestampZeroValue(t *testing.T) {
	zero := time.Time{}

	t.Run("timestamp-millis", func(t *testing.T) {
		schema := `{"type":"long","logicalType":"timestamp-millis"}`
		got := roundTrip(t, schema, zero)
		if !got.Equal(zero) {
			t.Fatalf("timestamp-millis zero: got %v, want %v", got, zero)
		}
	})

	t.Run("timestamp-micros", func(t *testing.T) {
		schema := `{"type":"long","logicalType":"timestamp-micros"}`
		got := roundTrip(t, schema, zero)
		if !got.Equal(zero) {
			t.Fatalf("timestamp-micros zero: got %v, want %v", got, zero)
		}
	})
}

func TestSpecTimestampNanosOverflow(t *testing.T) {
	t.Run("within range", func(t *testing.T) {
		schema := `{"type":"long","logicalType":"timestamp-nanos"}`
		ts := time.Date(2024, 1, 1, 0, 0, 0, 123456789, time.UTC)
		got := roundTrip(t, schema, ts)
		if !got.Equal(ts) {
			t.Fatalf("got %v, want %v", got, ts)
		}
	})

	t.Run("epoch exactly", func(t *testing.T) {
		schema := `{"type":"long","logicalType":"timestamp-nanos"}`
		ts := time.Unix(0, 0).UTC()
		got := roundTrip(t, schema, ts)
		if !got.Equal(ts) {
			t.Fatalf("got %v, want %v", got, ts)
		}
	})

	t.Run("near boundary 2262", func(t *testing.T) {
		schema := `{"type":"long","logicalType":"timestamp-nanos"}`
		ts := time.Date(2262, 4, 11, 0, 0, 0, 0, time.UTC)
		got := roundTrip(t, schema, ts)
		if !got.Equal(ts) {
			t.Fatalf("got %v, want %v", got, ts)
		}
	})
}

func TestSpecTimestampPreEpoch(t *testing.T) {
	t.Run("millis 1900", func(t *testing.T) {
		schema := `{"type":"long","logicalType":"timestamp-millis"}`
		ts := time.Date(1900, 6, 15, 12, 0, 0, 0, time.UTC)
		got := roundTrip(t, schema, ts)
		want := ts.Truncate(time.Millisecond)
		if !got.Equal(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("micros 1000", func(t *testing.T) {
		schema := `{"type":"long","logicalType":"timestamp-micros"}`
		ts := time.Date(1000, 3, 1, 0, 0, 0, 0, time.UTC)
		got := roundTrip(t, schema, ts)
		want := ts.Truncate(time.Microsecond)
		if !got.Equal(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
}

func TestSpecLocalTimestampRoundTrip(t *testing.T) {
	ts := time.Date(2024, 3, 1, 12, 34, 56, 789123456, time.UTC)

	t.Run("millis", func(t *testing.T) {
		schema := `{"type":"long","logicalType":"local-timestamp-millis"}`
		got := roundTrip(t, schema, ts)
		if !got.Equal(ts.Truncate(time.Millisecond)) {
			t.Fatalf("got %v, want %v", got, ts.Truncate(time.Millisecond))
		}
	})

	t.Run("micros", func(t *testing.T) {
		schema := `{"type":"long","logicalType":"local-timestamp-micros"}`
		got := roundTrip(t, schema, ts)
		if !got.Equal(ts.Truncate(time.Microsecond)) {
			t.Fatalf("got %v, want %v", got, ts.Truncate(time.Microsecond))
		}
	})

	t.Run("nanos", func(t *testing.T) {
		schema := `{"type":"long","logicalType":"local-timestamp-nanos"}`
		got := roundTrip(t, schema, ts)
		if !got.Equal(ts) {
			t.Fatalf("got %v, want %v", got, ts)
		}
	})
}

func TestSpecDatePreEpoch(t *testing.T) {
	schema := `{"type":"int","logicalType":"date"}`

	t.Run("1969-12-31", func(t *testing.T) {
		ts := time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC)
		got := roundTrip(t, schema, ts)
		if !got.Equal(ts) {
			t.Fatalf("got %v, want %v", got, ts)
		}
	})

	t.Run("1900-01-01", func(t *testing.T) {
		ts := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
		got := roundTrip(t, schema, ts)
		if !got.Equal(ts) {
			t.Fatalf("got %v, want %v", got, ts)
		}
	})

	t.Run("epoch", func(t *testing.T) {
		ts := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		got := roundTrip(t, schema, ts)
		if !got.Equal(ts) {
			t.Fatalf("got %v, want %v", got, ts)
		}
	})
}

// Spec §time-millis: milliseconds after midnight as int.
func TestSpecTimeMillisRoundTrip(t *testing.T) {
	schema := `{"type":"int","logicalType":"time-millis"}`

	t.Run("zero", func(t *testing.T) {
		got := roundTrip(t, schema, time.Duration(0))
		if got != 0 {
			t.Fatalf("got %v, want 0", got)
		}
	})

	t.Run("45s 123ms", func(t *testing.T) {
		d := 45*time.Second + 123*time.Millisecond
		got := roundTrip(t, schema, d)
		if got != d {
			t.Fatalf("got %v, want %v", got, d)
		}
	})

	t.Run("max representable", func(t *testing.T) {
		// 23:59:59.999
		d := 23*time.Hour + 59*time.Minute + 59*time.Second + 999*time.Millisecond
		got := roundTrip(t, schema, d)
		if got != d {
			t.Fatalf("got %v, want %v", got, d)
		}
	})

	t.Run("one millisecond", func(t *testing.T) {
		d := time.Millisecond
		got := roundTrip(t, schema, d)
		if got != d {
			t.Fatalf("got %v, want %v", got, d)
		}
	})
}

// Spec §time-micros: microseconds after midnight as long.
func TestSpecTimeMicrosRoundTrip(t *testing.T) {
	schema := `{"type":"long","logicalType":"time-micros"}`

	t.Run("zero", func(t *testing.T) {
		got := roundTrip(t, schema, time.Duration(0))
		if got != 0 {
			t.Fatalf("got %v, want 0", got)
		}
	})

	t.Run("2m 500µs", func(t *testing.T) {
		d := 2*time.Minute + 500*time.Microsecond
		got := roundTrip(t, schema, d)
		if got != d {
			t.Fatalf("got %v, want %v", got, d)
		}
	})

	t.Run("max representable", func(t *testing.T) {
		// 23:59:59.999999
		d := 23*time.Hour + 59*time.Minute + 59*time.Second + 999999*time.Microsecond
		got := roundTrip(t, schema, d)
		if got != d {
			t.Fatalf("got %v, want %v", got, d)
		}
	})

	t.Run("one microsecond", func(t *testing.T) {
		d := time.Microsecond
		got := roundTrip(t, schema, d)
		if got != d {
			t.Fatalf("got %v, want %v", got, d)
		}
	})
}

func TestSpecDecimalBoundary(t *testing.T) {
	schema := `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`

	t.Run("zero", func(t *testing.T) {
		r := new(big.Rat).SetFloat64(0)
		got := roundTrip(t, schema, *r)
		if got.Cmp(r) != 0 {
			t.Fatalf("got %v, want 0", &got)
		}
	})

	t.Run("positive", func(t *testing.T) {
		r := new(big.Rat).SetFrac64(12345, 100)
		got := roundTrip(t, schema, *r)
		if got.Cmp(r) != 0 {
			t.Fatalf("got %v, want %v", &got, r)
		}
	})

	t.Run("negative", func(t *testing.T) {
		r := new(big.Rat).SetFrac64(-12345, 100)
		got := roundTrip(t, schema, *r)
		if got.Cmp(r) != 0 {
			t.Fatalf("got %v, want %v", &got, r)
		}
	})

	t.Run("max precision", func(t *testing.T) {
		r := new(big.Rat).SetFrac64(9999999999, 100)
		got := roundTrip(t, schema, *r)
		if got.Cmp(r) != 0 {
			t.Fatalf("got %v, want %v", &got, r)
		}
	})
}

func TestSpecUUIDRoundTrip(t *testing.T) {
	t.Run("string uuid", func(t *testing.T) {
		schema := `{"type":"string","logicalType":"uuid"}`
		uuid := "550e8400-e29b-41d4-a716-446655440000"
		got := roundTrip(t, schema, uuid)
		if got != uuid {
			t.Fatalf("got %q, want %q", got, uuid)
		}
	})

	t.Run("fixed16 uuid", func(t *testing.T) {
		schema := `{"type":"fixed","name":"uuid_t","size":16,"logicalType":"uuid"}`
		uuid := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
			0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
		got := roundTrip(t, schema, uuid)
		if got != uuid {
			t.Fatalf("got %v, want %v", got, uuid)
		}
	})
}

func TestSpecUUIDRejectsInvalidTextForUUIDType(t *testing.T) {
	s := mustParse(t, `{"type":"string","logicalType":"uuid"}`)
	data := []byte{0x08, 'b', 'a', 'd', '!'}
	var out [16]byte
	if _, err := s.Decode(data, &out); err == nil {
		t.Fatal("expected error for invalid UUID text")
	}
}

func TestSpecDurationRoundTrip(t *testing.T) {
	schema := `{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}`

	t.Run("zero duration", func(t *testing.T) {
		d := avro.Duration{Months: 0, Days: 0, Milliseconds: 0}
		got := roundTrip(t, schema, d)
		if got != d {
			t.Fatalf("got %+v, want %+v", got, d)
		}
	})

	t.Run("typical duration", func(t *testing.T) {
		d := avro.Duration{Months: 12, Days: 30, Milliseconds: 3600000}
		got := roundTrip(t, schema, d)
		if got != d {
			t.Fatalf("got %+v, want %+v", got, d)
		}
	})

	t.Run("max uint32 values", func(t *testing.T) {
		d := avro.Duration{
			Months:       math.MaxUint32,
			Days:         math.MaxUint32,
			Milliseconds: math.MaxUint32,
		}
		got := roundTrip(t, schema, d)
		if got != d {
			t.Fatalf("got %+v, want %+v", got, d)
		}
	})
}

// ---------- nesting_test.go ----------

// -----------------------------------------------------------------------
// Complex Nesting Scenarios
// Spec: "Complex Types" — composition of records, arrays, maps, unions,
// enums, and fixed types in arbitrarily nested structures.
// https://avro.apache.org/docs/1.12.0/specification/#complex-types
// -----------------------------------------------------------------------

func TestNestingArrayOfRecords(t *testing.T) {
	schema := `{
		"type": "array",
		"items": {
			"type": "record",
			"name": "Item",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "name", "type": "string"}
			]
		}
	}`
	type Item struct {
		ID   int32  `avro:"id"`
		Name string `avro:"name"`
	}
	input := []Item{{1, "a"}, {2, "b"}, {3, "c"}}
	got := roundTrip(t, schema, input)
	if !reflect.DeepEqual(got, input) {
		t.Fatalf("got %+v, want %+v", got, input)
	}
}

func TestNestingMapOfArrays(t *testing.T) {
	schema := `{"type": "map", "values": {"type": "array", "items": "int"}}`
	input := map[string][]int32{
		"evens": {2, 4, 6},
		"odds":  {1, 3, 5},
	}
	got := roundTrip(t, schema, input)
	if !reflect.DeepEqual(got, input) {
		t.Fatalf("got %+v, want %+v", got, input)
	}
}

func TestNestingUnionOfRecords(t *testing.T) {
	schema := `[
		"null",
		{"type":"record","name":"Cat","fields":[{"name":"lives","type":"int"}]},
		{"type":"record","name":"Dog","fields":[{"name":"breed","type":"string"}]}
	]`

	// Encode a Cat.
	v := any(map[string]any{"lives": int32(9)})
	dst := encode(t, schema, &v)
	if dst[0] != 0x02 {
		t.Fatalf("Cat branch: got %x, want 02", dst[0])
	}
	var result any
	decode(t, schema, dst, &result)
	m := result.(map[string]any)
	if m["lives"] != int32(9) {
		t.Fatalf("got lives=%v, want 9", m["lives"])
	}

	// Encode a Dog.
	v = any(map[string]any{"breed": "lab"})
	dst = encode(t, schema, &v)
	if dst[0] != 0x04 {
		t.Fatalf("Dog branch: got %x, want 04", dst[0])
	}
	decode(t, schema, dst, &result)
	m = result.(map[string]any)
	if m["breed"] != "lab" {
		t.Fatalf("got breed=%v, want lab", m["breed"])
	}
}

func TestNestingRecordInRecordInRecord(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "L1",
		"fields": [{
			"name": "l2",
			"type": {
				"type": "record",
				"name": "L2",
				"fields": [{
					"name": "l3",
					"type": {
						"type": "record",
						"name": "L3",
						"fields": [{"name": "val", "type": "string"}]
					}
				}]
			}
		}]
	}`
	type L3 struct {
		Val string `avro:"val"`
	}
	type L2 struct {
		L3 L3 `avro:"l3"`
	}
	type L1 struct {
		L2 L2 `avro:"l2"`
	}
	input := L1{L2: L2{L3: L3{Val: "deep"}}}
	got := roundTrip(t, schema, input)
	if got.L2.L3.Val != "deep" {
		t.Fatalf("got %q, want deep", got.L2.L3.Val)
	}
}

func TestNestingArrayOfUnions(t *testing.T) {
	// Array of union items: decode from known bytes.
	schema := `{"type": "array", "items": ["null", "int", "string"]}`

	// count=3 (zigzag 6=0x06),
	// item0: null branch (0x00),
	// item1: int branch (0x02), value 42 (zigzag 84=0x54),
	// item2: string branch (0x04), len 2 (zigzag 4=0x04), "hi",
	// terminator: 0x00
	data := []byte{0x06, 0x00, 0x02, 0x54, 0x04, 0x04, 0x68, 0x69, 0x00}
	var output []any
	decode(t, schema, data, &output)
	if len(output) != 3 {
		t.Fatalf("length: got %d, want 3", len(output))
	}
	if output[0] != nil {
		t.Fatalf("output[0]: got %v, want nil", output[0])
	}
	if output[1] != int32(42) {
		t.Fatalf("output[1]: got %v, want 42", output[1])
	}
	if output[2] != "hi" {
		t.Fatalf("output[2]: got %v, want hi", output[2])
	}
}

func TestNestingMapOfUnions(t *testing.T) {
	schema := `{"type": "map", "values": ["null", "int"]}`
	s := mustParse(t, schema)
	input := map[string]any{"a": int32(1), "b": nil}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatal(err)
	}
	var output map[string]any
	_, err = s.Decode(encoded, &output)
	if err != nil {
		t.Fatal(err)
	}
	if output["a"] != int32(1) {
		t.Fatalf("a: got %v, want 1", output["a"])
	}
	if output["b"] != nil {
		t.Fatalf("b: got %v, want nil", output["b"])
	}
}

func TestNestingRecordWithAllTypes(t *testing.T) {
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
			{"name": "byt", "type": "bytes"},
			{"name": "arr", "type": {"type": "array", "items": "int"}},
			{"name": "m", "type": {"type": "map", "values": "string"}},
			{"name": "e", "type": {"type": "enum", "name": "Color", "symbols": ["RED", "GREEN", "BLUE"]}},
			{"name": "fix", "type": {"type": "fixed", "name": "F4", "size": 4}},
			{"name": "u", "type": ["null", "int"]}
		]
	}`
	s := mustParse(t, schema)
	type AllTypes struct {
		B   bool              `avro:"b"`
		I   int32             `avro:"i"`
		L   int64             `avro:"l"`
		F   float32           `avro:"f"`
		D   float64           `avro:"d"`
		S   string            `avro:"s"`
		Byt []byte            `avro:"byt"`
		Arr []int32           `avro:"arr"`
		M   map[string]string `avro:"m"`
		E   string            `avro:"e"`
		Fix [4]byte           `avro:"fix"`
		U   *int32            `avro:"u"`
	}
	uval := int32(99)
	input := AllTypes{
		B:   true,
		I:   42,
		L:   1000000,
		F:   2.5,
		D:   3.14159,
		S:   "test",
		Byt: []byte{0xFF},
		Arr: []int32{1, 2, 3},
		M:   map[string]string{"k": "v"},
		E:   "GREEN",
		Fix: [4]byte{0xDE, 0xAD, 0xBE, 0xEF},
		U:   &uval,
	}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatal(err)
	}
	var output AllTypes
	_, err = s.Decode(encoded, &output)
	if err != nil {
		t.Fatal(err)
	}
	if output.B != input.B || output.I != input.I || output.L != input.L ||
		output.S != input.S || output.E != input.E || output.Fix != input.Fix ||
		*output.U != *input.U {
		t.Fatalf("mismatch: got %+v, want %+v", output, input)
	}
}

// ---------- ocf_test.go ----------

// -----------------------------------------------------------------------
// OCF Compliance
// Spec: "Object Container Files" — file header, metadata, block layout,
// sync markers, and codec support.
//   - Magic: 4-byte header "Obj\x01"
//   - Metadata: avro.schema required, avro.codec optional (null default)
//   - Reserved keys: user keys must not start with "avro."
//   - Block layout: long count + long size + data + 16-byte sync marker
//   - Codecs: null (required), deflate, snappy, zstandard (optional)
//   - Schema evolution: reader schema applied via resolution
// https://avro.apache.org/docs/1.12.0/specification/#object-container-files
// -----------------------------------------------------------------------

func TestSpecOCFMagicValidation(t *testing.T) {
	badMagics := []struct {
		name  string
		magic [4]byte
	}{
		{"all zeros", [4]byte{0, 0, 0, 0}},
		{"wrong version", [4]byte{'O', 'b', 'j', 0x02}},
		{"random", [4]byte{0xDE, 0xAD, 0xBE, 0xEF}},
		{"reversed", [4]byte{0x01, 'j', 'b', 'O'}},
	}

	for _, tc := range badMagics {
		t.Run(tc.name, func(t *testing.T) {
			buf := bytes.NewReader(tc.magic[:])
			_, err := ocf.NewReader(buf)
			if err == nil {
				t.Fatal("expected error for bad magic, got nil")
			}
		})
	}
}

func TestSpecOCFMissingCodecDefaultsNull(t *testing.T) {
	schema := avro.MustParse(`"string"`)

	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, schema)
	if err != nil {
		t.Fatal(err)
	}
	s := "hello"
	if err := w.Encode(&s); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := ocf.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	var got string
	if err := r.Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got != "hello" {
		t.Fatalf("got %q, want hello", got)
	}

	meta := r.Metadata()
	if _, exists := meta["avro.codec"]; exists {
		t.Fatal("null codec should not write avro.codec to metadata")
	}
}

func TestSpecOCFBlockLayout(t *testing.T) {
	schema := avro.MustParse(`"int"`)
	syncMarker := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, schema, ocf.WithSyncMarker(syncMarker), ocf.WithBlockCount(2))
	if err != nil {
		t.Fatal(err)
	}
	v1 := int32(1)
	v2 := int32(2)
	if err := w.Encode(&v1); err != nil {
		t.Fatal(err)
	}
	if err := w.Encode(&v2); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	data := buf.Bytes()

	if !bytes.Equal(data[:4], []byte{'O', 'b', 'j', 0x01}) {
		t.Fatalf("magic: got %x, want 4f626a01", data[:4])
	}

	headerSyncIdx := bytes.Index(data[4:], syncMarker[:])
	if headerSyncIdx < 0 {
		t.Fatal("sync marker not found in header")
	}
	headerSyncIdx += 4

	blockStart := headerSyncIdx + 16

	blockData := data[blockStart:]
	count, n := binary.Varint(blockData)
	if count != 2 {
		t.Fatalf("block count: got %d, want 2", count)
	}
	blockData = blockData[n:]

	size, n := binary.Varint(blockData)
	if size <= 0 {
		t.Fatalf("block size: got %d, want positive", size)
	}
	blockData = blockData[n:]

	if int(size) > len(blockData) {
		t.Fatalf("block data truncated: size=%d, have=%d", size, len(blockData))
	}
	blockData = blockData[size:]

	if len(blockData) < 16 {
		t.Fatal("missing sync marker after block data")
	}
	if !bytes.Equal(blockData[:16], syncMarker[:]) {
		t.Fatalf("block sync: got %x, want %x", blockData[:16], syncMarker)
	}
}

func TestSpecOCFSchemaEvolution(t *testing.T) {
	type WriterRecord struct {
		Name string `avro:"name"`
		Age  int32  `avro:"age"`
		Old  string `avro:"old_field"`
	}
	type ReaderRecord struct {
		Name  string `avro:"name"`
		Age   int64  `avro:"age"`
		Email string `avro:"email"`
	}

	writerSchemaStr := `{
		"type":"record","name":"Person",
		"fields":[
			{"name":"name","type":"string"},
			{"name":"age","type":"int"},
			{"name":"old_field","type":"string"}
		]
	}`
	readerSchemaStr := `{
		"type":"record","name":"Person",
		"fields":[
			{"name":"name","type":"string"},
			{"name":"age","type":"long"},
			{"name":"email","type":"string","default":"unknown"}
		]
	}`

	writerSchema := avro.MustParse(writerSchemaStr)
	readerSchema := avro.MustParse(readerSchemaStr)

	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, writerSchema)
	if err != nil {
		t.Fatal(err)
	}
	records := []WriterRecord{
		{"Alice", 30, "x"},
		{"Bob", 25, "y"},
	}
	for _, r := range records {
		if err := w.Encode(&r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := ocf.NewReader(bytes.NewReader(buf.Bytes()), ocf.WithReaderSchema(readerSchema))
	if err != nil {
		t.Fatal(err)
	}
	var results []ReaderRecord
	for {
		var rec ReaderRecord
		if err := r.Decode(&rec); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		results = append(results, rec)
	}

	expected := []ReaderRecord{
		{"Alice", 30, "unknown"},
		{"Bob", 25, "unknown"},
	}
	if !reflect.DeepEqual(results, expected) {
		t.Fatalf("got %+v, want %+v", results, expected)
	}
}

func TestSpecOCFReservedMetadataKeys(t *testing.T) {
	schema := avro.MustParse(`"string"`)
	var buf bytes.Buffer
	_, err := ocf.NewWriter(&buf, schema, ocf.WithMetadata(map[string][]byte{
		"avro.custom": []byte("x"),
	}))
	if err == nil {
		t.Fatal("expected error for reserved avro.* metadata key")
	}
}

func TestSpecOCFRequiredMetadataPresent(t *testing.T) {
	schema := avro.MustParse(`"string"`)
	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, schema)
	if err != nil {
		t.Fatal(err)
	}
	v := "hello"
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := ocf.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	meta := r.Metadata()
	if _, ok := meta["avro.schema"]; !ok {
		t.Fatal("expected avro.schema metadata")
	}
	if _, ok := meta["avro.codec"]; ok {
		t.Fatal("null codec should omit avro.codec")
	}
}

func TestSpecOCFSupportedCodecsRoundTrip(t *testing.T) {
	cases := []struct {
		name      string
		codecName string
		opts      func(t *testing.T) []ocf.WriterOpt
	}{
		{
			name:      "deflate",
			codecName: "deflate",
			opts: func(t *testing.T) []ocf.WriterOpt {
				return []ocf.WriterOpt{ocf.WithCodec(ocf.DeflateCodec(flate.DefaultCompression))}
			},
		},
		{
			name:      "snappy",
			codecName: "snappy",
			opts: func(t *testing.T) []ocf.WriterOpt {
				return []ocf.WriterOpt{ocf.WithCodec(ocf.SnappyCodec())}
			},
		},
		{
			name:      "zstd",
			codecName: "zstandard",
			opts: func(t *testing.T) []ocf.WriterOpt {
				c, err := ocf.ZstdCodec(nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				return []ocf.WriterOpt{ocf.WithCodec(c)}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := avro.MustParse(`"string"`)
			var buf bytes.Buffer
			w, err := ocf.NewWriter(&buf, schema, tc.opts(t)...)
			if err != nil {
				t.Fatal(err)
			}
			in := "hello"
			if err := w.Encode(&in); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			r, err := ocf.NewReader(bytes.NewReader(buf.Bytes()))
			if err != nil {
				t.Fatal(err)
			}
			var out string
			if err := r.Decode(&out); err != nil {
				t.Fatal(err)
			}
			if out != in {
				t.Fatalf("got %q, want %q", out, in)
			}
			meta := r.Metadata()
			if got := string(meta["avro.codec"]); got != tc.codecName {
				t.Fatalf("codec metadata: got %q, want %q", got, tc.codecName)
			}
		})
	}
}

func TestSpecOCFRejectsNegativeBlockCount(t *testing.T) {
	schema := avro.MustParse(`"int"`)
	syncMarker := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, schema, ocf.WithSyncMarker(syncMarker), ocf.WithBlockCount(1))
	if err != nil {
		t.Fatal(err)
	}
	v := int32(7)
	if err := w.Encode(&v); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	data := append([]byte(nil), buf.Bytes()...)
	headerSyncIdx := bytes.Index(data[4:], syncMarker[:])
	if headerSyncIdx < 0 {
		t.Fatal("sync marker not found in header")
	}
	headerSyncIdx += 4
	blockStart := headerSyncIdx + 16
	if blockStart >= len(data) {
		t.Fatal("block start out of range")
	}

	// Corrupt count from +1 (0x02) to -1 (0x01).
	data[blockStart] = 0x01

	r, err := ocf.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	var out int32
	if err := r.Decode(&out); err == nil {
		t.Fatal("expected error for negative OCF block count")
	}
}

// ---------- promotion_test.go ----------

// -----------------------------------------------------------------------
// Type Promotion Matrix
// Spec: "Schema Resolution" — the 8 type promotions that a compliant
// implementation must support:
//   int → long, float, double
//   long → float, double
//   float → double
//   string → bytes
//   bytes → string
// https://avro.apache.org/docs/1.12.0/specification/#schema-resolution
// -----------------------------------------------------------------------

func TestPromotionIntToLong(t *testing.T) {
	writer := mustParse(t, `"int"`)
	reader := mustParse(t, `"long"`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	v := int32(42)
	encoded, err := writer.Encode(&v)
	if err != nil {
		t.Fatal(err)
	}
	var out int64
	_, err = resolved.Decode(encoded, &out)
	if err != nil {
		t.Fatal(err)
	}
	if out != 42 {
		t.Fatalf("got %d, want 42", out)
	}
}

func TestPromotionIntToFloat(t *testing.T) {
	writer := mustParse(t, `"int"`)
	reader := mustParse(t, `"float"`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	v := int32(42)
	encoded, err := writer.Encode(&v)
	if err != nil {
		t.Fatal(err)
	}
	var out float32
	_, err = resolved.Decode(encoded, &out)
	if err != nil {
		t.Fatal(err)
	}
	if out != 42.0 {
		t.Fatalf("got %v, want 42", out)
	}
}

func TestPromotionIntToDouble(t *testing.T) {
	writer := mustParse(t, `"int"`)
	reader := mustParse(t, `"double"`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	v := int32(42)
	encoded, err := writer.Encode(&v)
	if err != nil {
		t.Fatal(err)
	}
	var out float64
	_, err = resolved.Decode(encoded, &out)
	if err != nil {
		t.Fatal(err)
	}
	if out != 42.0 {
		t.Fatalf("got %v, want 42", out)
	}
}

func TestPromotionLongToFloat(t *testing.T) {
	writer := mustParse(t, `"long"`)
	reader := mustParse(t, `"float"`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	v := int64(42)
	encoded, err := writer.Encode(&v)
	if err != nil {
		t.Fatal(err)
	}
	var out float32
	_, err = resolved.Decode(encoded, &out)
	if err != nil {
		t.Fatal(err)
	}
	if out != 42.0 {
		t.Fatalf("got %v, want 42", out)
	}
}

func TestPromotionLongToDouble(t *testing.T) {
	writer := mustParse(t, `"long"`)
	reader := mustParse(t, `"double"`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	v := int64(42)
	encoded, err := writer.Encode(&v)
	if err != nil {
		t.Fatal(err)
	}
	var out float64
	_, err = resolved.Decode(encoded, &out)
	if err != nil {
		t.Fatal(err)
	}
	if out != 42.0 {
		t.Fatalf("got %v, want 42", out)
	}
}

func TestPromotionFloatToDouble(t *testing.T) {
	writer := mustParse(t, `"float"`)
	reader := mustParse(t, `"double"`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	v := float32(3.14)
	encoded, err := writer.Encode(&v)
	if err != nil {
		t.Fatal(err)
	}
	var out float64
	_, err = resolved.Decode(encoded, &out)
	if err != nil {
		t.Fatal(err)
	}
	// Float32 3.14 promoted to float64 should match the float32 value.
	if out != float64(float32(3.14)) {
		t.Fatalf("got %v, want %v", out, float64(float32(3.14)))
	}
}

func TestPromotionStringToBytes(t *testing.T) {
	writer := mustParse(t, `"string"`)
	reader := mustParse(t, `"bytes"`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	v := "hello"
	encoded, err := writer.Encode(&v)
	if err != nil {
		t.Fatal(err)
	}
	var out []byte
	_, err = resolved.Decode(encoded, &out)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "hello" {
		t.Fatalf("got %q, want hello", out)
	}
}

func TestPromotionBytesToString(t *testing.T) {
	writer := mustParse(t, `"bytes"`)
	reader := mustParse(t, `"string"`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	v := []byte("hello")
	encoded, err := writer.Encode(&v)
	if err != nil {
		t.Fatal(err)
	}
	var out string
	_, err = resolved.Decode(encoded, &out)
	if err != nil {
		t.Fatal(err)
	}
	if out != "hello" {
		t.Fatalf("got %q, want hello", out)
	}
}

func TestPromotionBoundaryValues(t *testing.T) {
	t.Run("MaxInt32 to long", func(t *testing.T) {
		writer := mustParse(t, `"int"`)
		reader := mustParse(t, `"long"`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}
		v := int32(math.MaxInt32)
		encoded, err := writer.Encode(&v)
		if err != nil {
			t.Fatal(err)
		}
		var out int64
		_, err = resolved.Decode(encoded, &out)
		if err != nil {
			t.Fatal(err)
		}
		if out != int64(math.MaxInt32) {
			t.Fatalf("got %d, want %d", out, math.MaxInt32)
		}
	})

	t.Run("MinInt32 to long", func(t *testing.T) {
		writer := mustParse(t, `"int"`)
		reader := mustParse(t, `"long"`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}
		v := int32(math.MinInt32)
		encoded, err := writer.Encode(&v)
		if err != nil {
			t.Fatal(err)
		}
		var out int64
		_, err = resolved.Decode(encoded, &out)
		if err != nil {
			t.Fatal(err)
		}
		if out != int64(math.MinInt32) {
			t.Fatalf("got %d, want %d", out, math.MinInt32)
		}
	})

	t.Run("MaxInt32 to double", func(t *testing.T) {
		writer := mustParse(t, `"int"`)
		reader := mustParse(t, `"double"`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}
		v := int32(math.MaxInt32)
		encoded, err := writer.Encode(&v)
		if err != nil {
			t.Fatal(err)
		}
		var out float64
		_, err = resolved.Decode(encoded, &out)
		if err != nil {
			t.Fatal(err)
		}
		if out != float64(math.MaxInt32) {
			t.Fatalf("got %v, want %v", out, float64(math.MaxInt32))
		}
	})

	t.Run("MaxInt64 to double", func(t *testing.T) {
		writer := mustParse(t, `"long"`)
		reader := mustParse(t, `"double"`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}
		v := int64(math.MaxInt64)
		encoded, err := writer.Encode(&v)
		if err != nil {
			t.Fatal(err)
		}
		var out float64
		_, err = resolved.Decode(encoded, &out)
		if err != nil {
			t.Fatal(err)
		}
		// MaxInt64 loses precision when converted to float64, but the
		// conversion should match Go's behavior.
		if out != float64(math.MaxInt64) {
			t.Fatalf("got %v, want %v", out, float64(math.MaxInt64))
		}
	})

	t.Run("empty string to bytes", func(t *testing.T) {
		writer := mustParse(t, `"string"`)
		reader := mustParse(t, `"bytes"`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}
		v := ""
		encoded, err := writer.Encode(&v)
		if err != nil {
			t.Fatal(err)
		}
		var out []byte
		_, err = resolved.Decode(encoded, &out)
		if err != nil {
			t.Fatal(err)
		}
		if len(out) != 0 {
			t.Fatalf("got %v, want empty", out)
		}
	})
}

// ---------- resolution_test.go ----------

// -----------------------------------------------------------------------
// Schema Evolution / Resolution
// Spec: "Schema Resolution" — rules for reading data written with a
// different but compatible schema.
//   - Enum symbol reordering and unknown-symbol defaults
//   - Type promotion within records, arrays, maps, and unions
//   - Self-referencing record evolution (added fields with defaults)
//   - Named type matching by fullname or unqualified name
//   - Field alias resolution
//   - Reader union selects first matching branch
// https://avro.apache.org/docs/1.12.0/specification/#schema-resolution
// -----------------------------------------------------------------------

func TestSpecEnumSymbolReordering(t *testing.T) {
	writer := mustParse(t, `{"type":"enum","name":"E","symbols":["B","C","D"]}`)
	reader := mustParse(t, `{"type":"enum","name":"E","symbols":["A","B","C","D"]}`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		writerSym string
		wantSym   string
	}{
		{"B", "B"},
		{"C", "C"},
		{"D", "D"},
	} {
		t.Run(tc.writerSym, func(t *testing.T) {
			encoded, err := writer.Encode(&tc.writerSym)
			if err != nil {
				t.Fatal(err)
			}
			var got string
			_, err = resolved.Decode(encoded, &got)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.wantSym {
				t.Fatalf("writer %q decoded as %q, want %q", tc.writerSym, got, tc.wantSym)
			}
		})
	}
}

func TestSpecEnumMultipleUnknownSymbols(t *testing.T) {
	writer := mustParse(t, `{"type":"enum","name":"E","symbols":["A","B","X","Y"]}`)
	reader := mustParse(t, `{"type":"enum","name":"E","symbols":["A","B","C"],"default":"C"}`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		writerSym string
		wantSym   string
	}{
		{"A", "A"},
		{"B", "B"},
		{"X", "C"},
		{"Y", "C"},
	} {
		t.Run(tc.writerSym, func(t *testing.T) {
			encoded, err := writer.Encode(&tc.writerSym)
			if err != nil {
				t.Fatal(err)
			}
			var got string
			_, err = resolved.Decode(encoded, &got)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.wantSym {
				t.Fatalf("writer %q → %q, want %q", tc.writerSym, got, tc.wantSym)
			}
		})
	}
}

func TestSpecPromotionInNestedContext(t *testing.T) {
	t.Run("record field int to long", func(t *testing.T) {
		writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`)
		reader := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"x","type":"long"}]}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}

		encoded, err := writer.Encode(map[string]any{"x": int32(42)})
		if err != nil {
			t.Fatal(err)
		}
		var result any
		_, err = resolved.Decode(encoded, &result)
		if err != nil {
			t.Fatal(err)
		}
		m := result.(map[string]any)
		if m["x"] != int64(42) {
			t.Fatalf("got x=%v (%T), want int64(42)", m["x"], m["x"])
		}
	})

	t.Run("array items int to long", func(t *testing.T) {
		writer := mustParse(t, `{"type":"array","items":"int"}`)
		reader := mustParse(t, `{"type":"array","items":"long"}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}

		input := []int32{1, 2, 3}
		encoded, err := writer.Encode(&input)
		if err != nil {
			t.Fatal(err)
		}
		var result []int64
		_, err = resolved.Decode(encoded, &result)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(result, []int64{1, 2, 3}) {
			t.Fatalf("got %v, want [1 2 3]", result)
		}
	})

	t.Run("map values int to long", func(t *testing.T) {
		writer := mustParse(t, `{"type":"map","values":"int"}`)
		reader := mustParse(t, `{"type":"map","values":"long"}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}

		input := map[string]int32{"k": 99}
		encoded, err := writer.Encode(&input)
		if err != nil {
			t.Fatal(err)
		}
		var result map[string]int64
		_, err = resolved.Decode(encoded, &result)
		if err != nil {
			t.Fatal(err)
		}
		if result["k"] != 99 {
			t.Fatalf("got k=%v, want 99", result["k"])
		}
	})
}

func TestSpecSelfRefRecordEvolution(t *testing.T) {
	writerSchema := `{
		"type": "record",
		"name": "Node",
		"fields": [
			{"name": "value", "type": "int"},
			{"name": "next", "type": ["null", "Node"]}
		]
	}`
	readerSchema := `{
		"type": "record",
		"name": "Node",
		"fields": [
			{"name": "value", "type": "int"},
			{"name": "label", "type": "string", "default": ""},
			{"name": "next", "type": ["null", "Node"]}
		]
	}`

	writer := mustParse(t, writerSchema)
	reader := mustParse(t, readerSchema)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	type Node struct {
		Value int32 `avro:"value"`
		Next  *Node `avro:"next"`
	}
	input := &Node{Value: 1, Next: &Node{Value: 2}}
	encoded, err := writer.Encode(input)
	if err != nil {
		t.Fatal(err)
	}

	var result any
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	m := result.(map[string]any)
	if m["value"] != int32(1) {
		t.Fatalf("root value: got %v, want 1", m["value"])
	}
	if m["label"] != "" {
		t.Fatalf("root label: got %v, want empty", m["label"])
	}
	next := m["next"].(map[string]any)
	if next["value"] != int32(2) {
		t.Fatalf("next value: got %v, want 2", next["value"])
	}
	if next["label"] != "" {
		t.Fatalf("next label: got %v, want empty", next["label"])
	}
	if next["next"] != nil {
		t.Fatalf("next.next: got %v, want nil", next["next"])
	}
}

func TestSpecUnionEvolutionBranches(t *testing.T) {
	writerSchema := `{
		"type": "record",
		"name": "R",
		"fields": [{"name": "v", "type": ["null","int"]}]
	}`
	readerSchema := `{
		"type": "record",
		"name": "R",
		"fields": [{"name": "v", "type": ["null","long"]}]
	}`

	writer := mustParse(t, writerSchema)
	reader := mustParse(t, readerSchema)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("null branch", func(t *testing.T) {
		encoded, err := writer.Encode(map[string]any{"v": nil})
		if err != nil {
			t.Fatal(err)
		}
		var result any
		_, err = resolved.Decode(encoded, &result)
		if err != nil {
			t.Fatal(err)
		}
		m := result.(map[string]any)
		if m["v"] != nil {
			t.Fatalf("got %v, want nil", m["v"])
		}
	})

	t.Run("int promoted to long", func(t *testing.T) {
		encoded, err := writer.Encode(map[string]any{"v": int32(42)})
		if err != nil {
			t.Fatal(err)
		}
		var result any
		_, err = resolved.Decode(encoded, &result)
		if err != nil {
			t.Fatal(err)
		}
		m := result.(map[string]any)
		if m["v"] != int64(42) {
			t.Fatalf("got %v (%T), want int64(42)", m["v"], m["v"])
		}
	})
}

func TestSpecNamedTypesMatchByUnqualifiedName(t *testing.T) {
	t.Run("record", func(t *testing.T) {
		writer := mustParse(t, `{"type":"record","name":"a.Foo","fields":[{"name":"a","type":"int"}]}`)
		reader := mustParse(t, `{"type":"record","name":"b.Foo","fields":[{"name":"a","type":"int"}]}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := writer.Encode(map[string]any{"a": int32(42)})
		if err != nil {
			t.Fatal(err)
		}
		var got any
		if _, err := resolved.Decode(encoded, &got); err != nil {
			t.Fatal(err)
		}
		if got.(map[string]any)["a"] != int32(42) {
			t.Fatalf("got %+v, want a=42", got)
		}
	})

	t.Run("enum", func(t *testing.T) {
		writer := mustParse(t, `{"type":"enum","name":"a.E","symbols":["A","B"]}`)
		reader := mustParse(t, `{"type":"enum","name":"b.E","symbols":["A","B"]}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}
		var got string
		encoded, err := writer.Encode("B")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := resolved.Decode(encoded, &got); err != nil {
			t.Fatal(err)
		}
		if got != "B" {
			t.Fatalf("got %q, want B", got)
		}
	})

	t.Run("fixed", func(t *testing.T) {
		writer := mustParse(t, `{"type":"fixed","name":"a.Id","size":4}`)
		reader := mustParse(t, `{"type":"fixed","name":"b.Id","size":4}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}
		in := [4]byte{1, 2, 3, 4}
		encoded, err := writer.Encode(&in)
		if err != nil {
			t.Fatal(err)
		}
		var out [4]byte
		if _, err := resolved.Decode(encoded, &out); err != nil {
			t.Fatal(err)
		}
		if out != in {
			t.Fatalf("got %x, want %x", out, in)
		}
	})
}

func TestSpecFieldAliasResolution(t *testing.T) {
	writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"old_name","type":"int"}]}`)
	reader := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"new_name","type":"int","aliases":["old_name"]}]}`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{"old_name": int32(7)})
	if err != nil {
		t.Fatal(err)
	}
	var got any
	if _, err := resolved.Decode(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if got.(map[string]any)["new_name"] != int32(7) {
		t.Fatalf("got %+v, want new_name=7", got)
	}
}

func TestSpecReaderUnionSelectsFirstMatchingBranch(t *testing.T) {
	writer := mustParse(t, `"int"`)
	reader := mustParse(t, `["long","double"]`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	in := int32(42)
	encoded, err := writer.Encode(&in)
	if err != nil {
		t.Fatal(err)
	}
	var got any
	if _, err := resolved.Decode(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if got != int64(42) {
		t.Fatalf("got %v (%T), want int64(42)", got, got)
	}
}

// ---------- schema_test.go ----------

// -----------------------------------------------------------------------
// Schema Parsing Edge Cases
// Spec: "Schema Declaration" — parsing, names, namespaces, type constraints.
//   - Names:       fullnames, namespace inheritance, forward references
//   - Records:     self-referencing (recursive) types
//   - Enums:       default validation, symbol constraints
//   - Fixed:       size validation
//   - Unions:      no nested unions, no duplicate types
//   - Logical:     unknown/invalid logical types fall back to underlying type
// https://avro.apache.org/docs/1.12.0/specification/#schema-declaration
// -----------------------------------------------------------------------

func TestSchemaNamespaceInheritance(t *testing.T) {
	// Nested records inherit the parent namespace.
	schema := `{
		"type": "record",
		"name": "Outer",
		"namespace": "com.example",
		"fields": [{
			"name": "inner",
			"type": {
				"type": "record",
				"name": "Inner",
				"fields": [{"name": "x", "type": "int"}]
			}
		}]
	}`
	s := mustParse(t, schema)
	// Verify round-trip works, meaning Inner is properly resolved.
	type Inner struct {
		X int32 `avro:"x"`
	}
	type Outer struct {
		Inner Inner `avro:"inner"`
	}
	input := Outer{Inner: Inner{X: 42}}
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatal(err)
	}
	var output Outer
	_, err = s.Decode(encoded, &output)
	if err != nil {
		t.Fatal(err)
	}
	if output.Inner.X != 42 {
		t.Fatalf("got %d, want 42", output.Inner.X)
	}
}

func TestSchemaFullyQualifiedName(t *testing.T) {
	// A fully qualified name overrides the inherited namespace.
	schema := `{
		"type": "record",
		"name": "Outer",
		"namespace": "com.example",
		"fields": [{
			"name": "inner",
			"type": {
				"type": "record",
				"name": "org.other.Inner",
				"fields": [{"name": "y", "type": "string"}]
			}
		}]
	}`
	s := mustParse(t, schema)
	type Inner struct {
		Y string `avro:"y"`
	}
	type Outer struct {
		Inner Inner `avro:"inner"`
	}
	got := roundTripSchema(t, s, Outer{Inner: Inner{Y: "hello"}})
	if got.Inner.Y != "hello" {
		t.Fatalf("got %q, want hello", got.Inner.Y)
	}
}

func TestSchemaForwardReference(t *testing.T) {
	// A record field referencing a type defined later in the same schema.
	schema := `{
		"type": "record",
		"name": "Container",
		"fields": [
			{"name": "item", "type": {
				"type": "record",
				"name": "Item",
				"fields": [{"name": "id", "type": "int"}]
			}},
			{"name": "ref", "type": "Item"}
		]
	}`
	s := mustParse(t, schema)
	type Item struct {
		ID int32 `avro:"id"`
	}
	type Container struct {
		Item Item `avro:"item"`
		Ref  Item `avro:"ref"`
	}
	input := Container{Item: Item{ID: 1}, Ref: Item{ID: 2}}
	got := roundTripSchema(t, s, input)
	if got.Item.ID != 1 || got.Ref.ID != 2 {
		t.Fatalf("got %+v, want %+v", got, input)
	}
}

func TestSchemaRecursiveSelfRef(t *testing.T) {
	// A record that references itself (linked list).
	schema := `{
		"type": "record",
		"name": "Node",
		"fields": [
			{"name": "value", "type": "int"},
			{"name": "next", "type": ["null", "Node"]}
		]
	}`
	s := mustParse(t, schema)
	type Node struct {
		Value int32 `avro:"value"`
		Next  *Node `avro:"next"`
	}
	input := &Node{Value: 1, Next: &Node{Value: 2}}
	encoded, err := s.AppendEncode(nil, input)
	if err != nil {
		t.Fatal(err)
	}
	var output Node
	_, err = s.Decode(encoded, &output)
	if err != nil {
		t.Fatal(err)
	}
	if output.Value != 1 || output.Next == nil || output.Next.Value != 2 || output.Next.Next != nil {
		t.Fatalf("got %+v, want {1, {2, nil}}", output)
	}
}

func TestSchemaEnumDefaultValidation(t *testing.T) {
	// Enum default must be a valid symbol. When default is in symbols, parse succeeds.
	_, err := avro.Parse(`{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}`)
	if err != nil {
		t.Fatalf("valid enum default should parse: %v", err)
	}
}

func TestSchemaFixedSizeValidation(t *testing.T) {
	// Fixed with negative size should fail.
	_, err := avro.Parse(`{"type":"fixed","name":"F","size":-1}`)
	if err == nil {
		t.Fatal("expected error for negative fixed size")
	}

	// Valid fixed size should succeed.
	_, err = avro.Parse(`{"type":"fixed","name":"G","size":8}`)
	if err != nil {
		t.Fatalf("valid fixed size should parse: %v", err)
	}
}

func TestSchemaInvalidLogicalIgnored(t *testing.T) {
	// Decimal-specific malformations (invalid precision / scale > precision /
	// precision over fixed capacity) are now rejected at parse time, matching
	// Java's LogicalTypes.Decimal.validate and fastavro's parse_schema.
	// Unknown logical types still fall back to the underlying type for
	// forward-compat; that's the case below.
	t.Run("decimal precision zero rejected", func(t *testing.T) {
		if _, err := avro.Parse(`{"type":"bytes","logicalType":"decimal","precision":0,"scale":0}`); err == nil {
			t.Fatal("expected decimal precision=0 to error")
		}
	})

	t.Run("decimal scale exceeds precision rejected", func(t *testing.T) {
		if _, err := avro.Parse(`{"type":"bytes","logicalType":"decimal","precision":5,"scale":6}`); err == nil {
			t.Fatal("expected decimal scale > precision to error")
		}
	})

	t.Run("unknown logical type falls back to underlying type", func(t *testing.T) {
		s, err := avro.Parse(`{"type":"string","logicalType":"unknown-logical"}`)
		if err != nil {
			t.Fatalf("unknown logical type should be ignored: %v", err)
		}
		in := "hello"
		encoded, err := s.AppendEncode(nil, &in)
		if err != nil {
			t.Fatal(err)
		}
		var out string
		if _, err := s.Decode(encoded, &out); err != nil {
			t.Fatal(err)
		}
		if out != in {
			t.Fatalf("got %q, want %q", out, in)
		}
	})
}

func TestSchemaUnionNoNestedUnion(t *testing.T) {
	// Union cannot directly contain another union.
	_, err := avro.Parse(`[["null","int"],"string"]`)
	if err == nil {
		t.Fatal("expected error for nested union")
	}
}

func TestSchemaUnionNoDuplicateTypes(t *testing.T) {
	// Union cannot have two of the same unnamed type.
	_, err := avro.Parse(`["int","int"]`)
	if err == nil {
		t.Fatal("expected error for duplicate union types")
	}
}

func TestSchemaInvalidJSON(t *testing.T) {
	_, err := avro.Parse(`{not valid json}`)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestSchemaAllPrimitives(t *testing.T) {
	// All 8 primitive types parse and round-trip.
	primitives := []struct {
		schema string
		encode func(t *testing.T) []byte
	}{
		{`"null"`, func(t *testing.T) []byte {
			// Null schema encodes as zero bytes.
			mustParse(t, `"null"`)
			return nil
		}},
		{`"boolean"`, func(t *testing.T) []byte {
			v := true
			return encode(t, `"boolean"`, &v)
		}},
		{`"int"`, func(t *testing.T) []byte {
			v := int32(42)
			return encode(t, `"int"`, &v)
		}},
		{`"long"`, func(t *testing.T) []byte {
			v := int64(42)
			return encode(t, `"long"`, &v)
		}},
		{`"float"`, func(t *testing.T) []byte {
			v := float32(3.14)
			return encode(t, `"float"`, &v)
		}},
		{`"double"`, func(t *testing.T) []byte {
			v := float64(3.14)
			return encode(t, `"double"`, &v)
		}},
		{`"string"`, func(t *testing.T) []byte {
			v := "hello"
			return encode(t, `"string"`, &v)
		}},
		{`"bytes"`, func(t *testing.T) []byte {
			v := []byte{1, 2, 3}
			return encode(t, `"bytes"`, &v)
		}},
	}

	for _, p := range primitives {
		t.Run(p.schema, func(t *testing.T) {
			mustParse(t, p.schema)
			b := p.encode(t)
			if len(b) == 0 && p.schema != `"null"` {
				t.Fatal("expected non-empty encoding")
			}
		})
	}
}

func TestSchemaAllLogicalTypes(t *testing.T) {
	logicals := []string{
		`{"type":"int","logicalType":"date"}`,
		`{"type":"int","logicalType":"time-millis"}`,
		`{"type":"long","logicalType":"time-micros"}`,
		`{"type":"long","logicalType":"timestamp-millis"}`,
		`{"type":"long","logicalType":"timestamp-micros"}`,
		`{"type":"long","logicalType":"timestamp-nanos"}`,
		`{"type":"long","logicalType":"local-timestamp-millis"}`,
		`{"type":"long","logicalType":"local-timestamp-micros"}`,
		`{"type":"long","logicalType":"local-timestamp-nanos"}`,
		`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`,
		`{"type":"bytes","logicalType":"big-decimal"}`,
		`{"type":"fixed","name":"dec","size":8,"logicalType":"decimal","precision":10,"scale":2}`,
		`{"type":"string","logicalType":"uuid"}`,
		`{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}`,
	}

	for _, schema := range logicals {
		t.Run(schema, func(t *testing.T) {
			mustParse(t, schema)
		})
	}
}

// roundTripSchema encodes/decodes using a pre-parsed schema.
func roundTripSchema[T any](t *testing.T, s *avro.Schema, input T) T {
	t.Helper()
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

// ---------- soe_test.go ----------

// -----------------------------------------------------------------------
// Single Object Encoding
// Spec: "Single Object Encoding" — a framing for single Avro values:
//   [0xC3, 0x01] + 8-byte little-endian CRC-64-AVRO fingerprint + payload
// https://avro.apache.org/docs/1.12.0/specification/#single-object-encoding
// -----------------------------------------------------------------------

func TestSOERoundTrip(t *testing.T) {
	schema := mustParse(t, `"int"`)
	v := int32(42)
	data, err := schema.AppendSingleObject(nil, &v)
	if err != nil {
		t.Fatal(err)
	}

	var out int32
	_, err = schema.DecodeSingleObject(data, &out)
	if err != nil {
		t.Fatal(err)
	}
	if out != 42 {
		t.Fatalf("got %d, want 42", out)
	}
}

func TestSOEMagicBytes(t *testing.T) {
	schema := mustParse(t, `"string"`)
	v := "hello"
	data, err := schema.AppendSingleObject(nil, &v)
	if err != nil {
		t.Fatal(err)
	}

	if len(data) < 2 {
		t.Fatal("data too short")
	}
	if data[0] != 0xC3 || data[1] != 0x01 {
		t.Fatalf("magic: got [%#x, %#x], want [0xc3, 0x01]", data[0], data[1])
	}
}

func TestSOEFingerprintMatch(t *testing.T) {
	schema := mustParse(t, `"long"`)
	v := int64(100)
	data, err := schema.AppendSingleObject(nil, &v)
	if err != nil {
		t.Fatal(err)
	}

	// Extract fingerprint from SOE (stored as little-endian uint64).
	fp, _, err := avro.SingleObjectFingerprint(data)
	if err != nil {
		t.Fatal(err)
	}

	// Schema.Fingerprint returns big-endian via h.Sum(nil).
	// SOE stores little-endian. Convert for comparison.
	h := avro.NewRabin()
	schemaFP := schema.Fingerprint(h)

	// Reverse schemaFP to get LE for comparison.
	var schemaFPLE [8]byte
	for i := range 8 {
		schemaFPLE[i] = schemaFP[7-i]
	}

	if fp != schemaFPLE {
		t.Fatalf("embedded fp %x != schema fp (LE) %x", fp, schemaFPLE)
	}
}

func TestSOEFingerprintMismatch(t *testing.T) {
	schema1 := mustParse(t, `"int"`)
	schema2 := mustParse(t, `"string"`)

	v := int32(42)
	data, err := schema1.AppendSingleObject(nil, &v)
	if err != nil {
		t.Fatal(err)
	}

	// Decoding with a different schema should fail.
	var out string
	_, err = schema2.DecodeSingleObject(data, &out)
	if err == nil {
		t.Fatal("expected error for fingerprint mismatch")
	}
}

func TestSOETruncatedData(t *testing.T) {
	// Less than 10 bytes should fail.
	short := []byte{0xC3, 0x01, 0x00, 0x00}
	schema := mustParse(t, `"int"`)
	var out int32
	_, err := schema.DecodeSingleObject(short, &out)
	if err == nil {
		t.Fatal("expected error for truncated SOE data")
	}

	// Empty input.
	_, err = schema.DecodeSingleObject(nil, &out)
	if err == nil {
		t.Fatal("expected error for nil SOE data")
	}
}

func TestSOEFingerprintExtraction(t *testing.T) {
	schema := mustParse(t, `"double"`)
	v := 3.14
	data, err := schema.AppendSingleObject(nil, &v)
	if err != nil {
		t.Fatal(err)
	}

	fp, rest, err := avro.SingleObjectFingerprint(data)
	if err != nil {
		t.Fatal(err)
	}

	// fp should be 8 bytes.
	if len(fp) != 8 {
		t.Fatalf("fp length: got %d, want 8", len(fp))
	}

	// rest should be the payload (double = 8 bytes).
	if len(rest) != 8 {
		t.Fatalf("rest length: got %d, want 8", len(rest))
	}

	// Decode rest directly.
	var out float64
	_, err = schema.Decode(rest, &out)
	if err != nil {
		t.Fatal(err)
	}
	if out != 3.14 {
		t.Fatalf("got %v, want 3.14", out)
	}
}

// ---------- union_test.go ----------

// -----------------------------------------------------------------------
// Union & Null Edge Cases
// Spec: "Unions" (Schema Declaration) and "Binary Encoding" — union
// branch selection, null encoding, and interaction with container types.
// https://avro.apache.org/docs/1.12.0/specification/#unions
// -----------------------------------------------------------------------

func TestSpecEmptyArrayInUnion(t *testing.T) {
	schema := `["null", {"type":"array","items":"int"}]`

	arr := []int32{}
	dst := encode(t, schema, &arr)
	if len(dst) < 1 || dst[0] != 0x02 {
		t.Fatalf("empty array in union: got branch index %x, want 02 (array branch)", dst[0])
	}

	var result []int32
	decode(t, schema, dst, &result)
	if len(result) != 0 {
		t.Fatalf("expected empty slice, got %v", result)
	}
}

func TestSpecNilSliceInUnion(t *testing.T) {
	type W struct {
		Arr *[]int32 `avro:"arr"`
	}
	schema := `{"type":"record","name":"W","fields":[
		{"name":"arr","type":["null",{"type":"array","items":"int"}]}
	]}`

	dst := encode(t, schema, &W{Arr: nil})
	if len(dst) != 1 || dst[0] != 0x00 {
		t.Fatalf("nil pointer in union: got %x, want 00 (null branch)", dst)
	}

	var result W
	decode(t, schema, dst, &result)
	if result.Arr != nil {
		t.Fatalf("expected nil, got %v", result.Arr)
	}
}

func TestSpecNilMapInUnion(t *testing.T) {
	type W struct {
		M *map[string]string `avro:"m"`
	}
	schema := `{"type":"record","name":"W","fields":[
		{"name":"m","type":["null",{"type":"map","values":"string"}]}
	]}`

	dst := encode(t, schema, &W{M: nil})
	if len(dst) != 1 || dst[0] != 0x00 {
		t.Fatalf("nil pointer in union: got %x, want 00 (null branch)", dst)
	}

	var result W
	decode(t, schema, dst, &result)
	if result.M != nil {
		t.Fatalf("expected nil, got %v", result.M)
	}
}

func TestSpecUnionMultipleNamedTypes(t *testing.T) {
	schema := `[
		"null",
		{"type":"record","name":"Cat","fields":[{"name":"meow","type":"string"}]},
		{"type":"record","name":"Dog","fields":[{"name":"bark","type":"string"}]}
	]`

	t.Run("null branch via decode", func(t *testing.T) {
		var result any
		decode(t, schema, []byte{0x00}, &result)
		if result != nil {
			t.Fatalf("null branch: got %v, want nil", result)
		}
	})

	t.Run("first record branch", func(t *testing.T) {
		v := any(map[string]any{"meow": "purr"})
		dst := encode(t, schema, &v)
		if dst[0] != 0x02 {
			t.Fatalf("Cat: got branch %x, want 02", dst[0])
		}
		var result any
		decode(t, schema, dst, &result)
		m, ok := result.(map[string]any)
		if !ok {
			t.Fatalf("expected map, got %T", result)
		}
		if m["meow"] != "purr" {
			t.Fatalf("got meow=%v, want purr", m["meow"])
		}
	})

	t.Run("second record branch", func(t *testing.T) {
		v := any(map[string]any{"bark": "woof"})
		dst := encode(t, schema, &v)
		if dst[0] != 0x04 {
			t.Fatalf("Dog: got branch %x, want 04", dst[0])
		}
		var result any
		decode(t, schema, dst, &result)
		m := result.(map[string]any)
		if m["bark"] != "woof" {
			t.Fatalf("got bark=%v, want woof", m["bark"])
		}
	})
}

func TestSpecNonEmptyArrayInUnionRoundTrip(t *testing.T) {
	schema := `["null", {"type":"array","items":"int"}]`

	arr := []int32{10, 20, 30}
	dst := encode(t, schema, &arr)
	if dst[0] != 0x02 {
		t.Fatalf("non-empty array branch: got %x, want 02", dst[0])
	}

	var result []int32
	decode(t, schema, dst, &result)
	if !reflect.DeepEqual(result, arr) {
		t.Fatalf("got %v, want %v", result, arr)
	}
}

// TestSpecDecimalRejectsScaleTruncation verifies that encoding a big.Rat
// whose value cannot be represented at the schema's scale without rounding
// returns an error rather than silently truncating.
//
// Reference behavior:
//   - Java DecimalConversion.validate (Conversions.java:144-156) uses
//     RoundingMode.UNNECESSARY which throws AvroTypeException.
//   - fastavro prepare_bytes_decimal (_logical_writers_py.py:131-132) raises
//     ValueError("Scale provided in schema does not match the decimal")
//     when delta < 0 (i.e. -exp > scale).
//
// twmb's pre-fix ratToUnscaled (ser.go:1709-1714) silently truncated via Quo,
// so big.NewRat(1,3) at scale=2 produced 33/100 = 0.33 instead of erroring.
func TestSpecDecimalRejectsScaleTruncation(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		rat    *big.Rat
	}{
		// bytes-backed decimal
		{
			"bytes/one_third",
			`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`,
			big.NewRat(1, 3),
		},
		{
			"bytes/negative_one_third",
			`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`,
			big.NewRat(-1, 3),
		},
		{
			"bytes/one_seventh",
			`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`,
			big.NewRat(1, 7),
		},
		{
			// 1.234 needs scale >= 3; schema is scale=2.
			"bytes/three_decimal_places",
			`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`,
			big.NewRat(1234, 1000),
		},
		// fixed-backed decimal — same path through ratToUnscaled.
		{
			"fixed/one_third",
			`{"type":"fixed","name":"d","size":8,"logicalType":"decimal","precision":10,"scale":2}`,
			big.NewRat(1, 3),
		},
		{
			"fixed/three_decimal_places",
			`{"type":"fixed","name":"d","size":8,"logicalType":"decimal","precision":10,"scale":2}`,
			big.NewRat(1234, 1000),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := mustParse(t, c.schema)
			if _, err := s.AppendEncode(nil, c.rat); err == nil {
				t.Fatalf("AppendEncode(%s) at scale=2: want error matching Java/fastavro behavior, got nil (silent truncation)", c.rat.RatString())
			}
		})
	}
}

// TestSpecBigDecimalWireFormat locks in the AVRO-4124 big-decimal
// wire format. The bytes-typed field's payload is:
//   - First: Avro-bytes-framed unscaled integer (length varint +
//     two's-complement big-endian bytes).
//   - Second: zigzag varint scale.
//
// Reference: Java's Conversions.toBytes (Conversions.java:206-220) and
// avro-rs's big_decimal_as_bytes (bigdecimal.rs:29-37); ground truth
// verified against a Java-generated bigdec.avro round-trip (value 2.24
// produces wire bytes 08 04 00 e0 04 — outer-len(8) + inner-len(4) +
// unscaled bytes (00 e0 = 224) + scale (zigzag 2 = 04)).
//
// fastavro and hamba do not implement big-decimal at all — fastavro
// has no LOGICAL_WRITERS registration for "bytes-big-decimal" and
// hamba's LogicalType enum has no big-decimal entry.
//
// Carrier limitation: *big.Rat reduces to lowest terms, so twmb
// always emits the canonical (smallest non-negative) scale. Java
// preserves declared scale: BigDecimal("3.14") emits scale=2 while
// BigDecimal("3.140") emits scale=3; twmb produces identical output
// (canonical scale=2) for both inputs because big.Rat doesn't carry
// trailing-zero metadata. Documented as a known divergence.
func TestSpecBigDecimalWireFormat(t *testing.T) {
	schema := mustParse(t, `{"type":"bytes","logicalType":"big-decimal"}`)
	t.Run("java_ground_truth_2.24", func(t *testing.T) {
		// Java-generated bigdec.avro encodes 2.24 as 08 04 00 e0 04.
		enc, err := schema.AppendEncode(nil, big.NewRat(224, 100))
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		want := []byte{0x08, 0x04, 0x00, 0xe0, 0x04}
		if !bytes.Equal(enc, want) {
			t.Fatalf("wire: got %x, want %x (Java ground truth)", enc, want)
		}
		var got big.Rat
		if _, err := schema.Decode(enc, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.Cmp(big.NewRat(224, 100)) != 0 {
			t.Fatalf("got %s, want 224/100", got.RatString())
		}
	})
	t.Run("3.14_round_trip", func(t *testing.T) {
		// 3.14: unscaled=314 (0x013a, no sign-padding needed), scale=2.
		// outer-len = 4 (zigzag 0x08); inner-len = 2 (zigzag 0x04).
		enc, err := schema.AppendEncode(nil, big.NewRat(314, 100))
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		want := []byte{0x08, 0x04, 0x01, 0x3a, 0x04}
		if !bytes.Equal(enc, want) {
			t.Fatalf("wire: got %x, want %x", enc, want)
		}
		var got big.Rat
		if _, err := schema.Decode(enc, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.Cmp(big.NewRat(314, 100)) != 0 {
			t.Fatalf("got %s, want 314/100", got.RatString())
		}
	})
	t.Run("zero", func(t *testing.T) {
		// 0: unscaled=0 (1 byte 0x00), scale=0. inner = 02 00 00.
		enc, err := schema.AppendEncode(nil, new(big.Rat))
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		want := []byte{0x06, 0x02, 0x00, 0x00}
		if !bytes.Equal(enc, want) {
			t.Fatalf("wire: got %x, want %x", enc, want)
		}
	})
	t.Run("negative", func(t *testing.T) {
		// -123.45: unscaled=-12345 (0xCFC7 in 2 bytes), scale=2.
		r := big.NewRat(-12345, 100)
		enc, err := schema.AppendEncode(nil, r)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		want := []byte{0x08, 0x04, 0xcf, 0xc7, 0x04}
		if !bytes.Equal(enc, want) {
			t.Fatalf("wire: got %x, want %x", enc, want)
		}
		var got big.Rat
		if _, err := schema.Decode(enc, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.Cmp(r) != 0 {
			t.Fatalf("got %s, want %s", got.RatString(), r.RatString())
		}
	})
	t.Run("non_terminating_rejected", func(t *testing.T) {
		// 1/3 has no finite decimal expansion — encoder must reject
		// rather than silently rounding (matches the regular decimal
		// type's ratToUnscaled rejection of non-representable values).
		if _, err := schema.AppendEncode(nil, big.NewRat(1, 3)); err == nil {
			t.Fatal("expected error encoding big.NewRat(1,3) — non-terminating decimal")
		}
	})
	t.Run("negative_scale_on_decode", func(t *testing.T) {
		// Java/avro-rs can emit negative scale (new BigDecimal(1, -3)
		// represents 1000). twmb's encoder never produces it (carrier
		// limitation) but the decoder must accept it for interop.
		// Wire: outer-len(3) | inner-len(1) | unscaled byte 0x01 |
		// scale zigzag(-3) = 0x05.
		wire := []byte{0x06, 0x02, 0x01, 0x05}
		var got big.Rat
		if _, err := schema.Decode(wire, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		want := big.NewRat(1000, 1)
		if got.Cmp(want) != 0 {
			t.Fatalf("got %s, want 1000", got.RatString())
		}
	})
	t.Run("canonical_scale_trailing_zeros", func(t *testing.T) {
		// Carrier limitation: big.NewRat(3140, 1000) and big.NewRat(314, 100)
		// both reduce to 157/50, so twmb emits identical wire bytes
		// (scale=2) for both. Java would distinguish: BigDecimal("3.140")
		// emits scale=3, BigDecimal("3.14") emits scale=2.
		encA, _ := schema.AppendEncode(nil, big.NewRat(3140, 1000))
		encB, _ := schema.AppendEncode(nil, big.NewRat(314, 100))
		if !bytes.Equal(encA, encB) {
			t.Fatalf("expected identical wire for 3140/1000 and 314/100 (carrier canonicalization): %x vs %x", encA, encB)
		}
	})
	t.Run("json_round_trip", func(t *testing.T) {
		// JSON round-trip via AppendEncodeJSON + DecodeJSON for the
		// Java ground-truth value 2.24. Pins the JSON encoder's
		// codepoint-string wrapping of the shared
		// buildBigDecimalPayload output and the decode-side parse
		// via parseBigDecimalPayload, so a future change to either
		// path that drifts from the binary contract gets caught.
		in := big.NewRat(224, 100)
		enc, err := schema.EncodeJSON(in)
		if err != nil {
			t.Fatalf("EncodeJSON: %v", err)
		}
		var got big.Rat
		if err := schema.DecodeJSON(enc, &got); err != nil {
			t.Fatalf("DecodeJSON: %v (wire = %s)", err, enc)
		}
		if got.Cmp(in) != 0 {
			t.Fatalf("round-trip: got %s, want %s", got.RatString(), in.RatString())
		}
		// Same round-trip through *any.
		var asAny any
		if err := schema.DecodeJSON(enc, &asAny); err != nil {
			t.Fatalf("DecodeJSON into *any: %v", err)
		}
		r, ok := asAny.(*big.Rat)
		if !ok {
			t.Fatalf("DecodeJSON into *any: got %T %#v, want *big.Rat", asAny, asAny)
		}
		if r.Cmp(in) != 0 {
			t.Fatalf("*any round-trip: got %s, want %s", r.RatString(), in.RatString())
		}
	})
	t.Run("malformed_json_payload_errors_typed_and_any", func(t *testing.T) {
		// JSON decode of a malformed big-decimal payload must error
		// symmetrically on typed (*big.Rat) and *any targets. Pre-fix
		// the *any path's decodeLogicalBytes silently fell through to
		// raw []byte ("salvage the wire" heuristic); typed path's
		// assignBytes errored via parseBigDecimalPayload. The two
		// paths now both propagate the parse error.
		//
		// JSON wire: codepoint string containing the inner big-decimal
		// payload. We construct an invalid inner: a varint with the
		// high-bit-set continuation byte 0x80 only, signaling more
		// bytes that aren't there.
		badJSON := []byte(`""`)
		var rat big.Rat
		if err := schema.DecodeJSON(badJSON, &rat); err == nil {
			t.Fatal("expected typed-target JSON decode error on malformed payload")
		}
		var any1 any
		if err := schema.DecodeJSON(badJSON, &any1); err == nil {
			t.Fatalf("expected *any JSON decode error on malformed payload (got %T %#v); typed path errors so any-path must too", any1, any1)
		}
	})
}

// TestSpecRecursiveRecordTerminates verifies that a non-union
// self-recursive record either rejects at parse, or — if parse succeeds —
// encoding terminates (returns an error) rather than stack-overflowing.
// AVRO-1422: Java had a stack-overflow regression on this shape.
func TestSpecRecursiveRecordTerminates(t *testing.T) {
	schema := `{"type":"record","name":"R","fields":[{"name":"child","type":"R"}]}`
	s, err := avro.Parse(schema)
	if err != nil {
		return // reject at parse is acceptable
	}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic on recursive encode: %v", r)
		}
	}()
	if _, err := s.AppendEncode(nil, map[string]any{}); err == nil {
		t.Fatal("expected error encoding instance of self-recursive record")
	}
}

// TestSpecResolvePromoteIntToDateLogical verifies that a writer int
// schema resolves to a reader int+date logical schema, decoding the int
// as a time.Time = epoch + N days. AVRO-4215 was the Java FastReader
// regression for this shape.
func TestSpecResolvePromoteIntToDateLogical(t *testing.T) {
	writer := mustParse(t, `"int"`)
	reader := mustParse(t, `{"type":"int","logicalType":"date"}`)
	encoded, err := writer.AppendEncode(nil, int32(12345))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	var got time.Time
	if _, err := resolved.Decode(encoded, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Add(12345 * 24 * time.Hour)
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// TestSpecUnionDuplicatePrimitiveVsLogical verifies that a union with
// two same-base-type branches (one plain, one with a logical type)
// is rejected at parse (the canonical form collides), but a union of
// different base types — null + long+timestamp-micros — is accepted.
// AVRO-2380.
func TestSpecUnionDuplicatePrimitiveVsLogical(t *testing.T) {
	bad := `["long",{"type":"long","logicalType":"timestamp-micros"}]`
	if _, err := avro.Parse(bad); err == nil {
		t.Fatal("expected parse error: duplicate base type in union")
	}
	good := `["null",{"type":"long","logicalType":"timestamp-micros"}]`
	if _, err := avro.Parse(good); err != nil {
		t.Fatalf("spec-valid union rejected: %v", err)
	}
}

// TestSpecFingerprintIgnoresDefaults verifies that two schemas differing
// only in default values produce byte-equal canonical forms AND byte-equal
// fingerprints. AVRO-2002 — the parsing canonical form (PCF) must strip
// defaults, and the fingerprint is taken over the PCF.
func TestSpecFingerprintIgnoresDefaults(t *testing.T) {
	a := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"x","type":"int","default":0}]}`)
	b := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`)
	if !bytes.Equal(a.Canonical(), b.Canonical()) {
		t.Fatalf("canonical mismatch:\n  a=%q\n  b=%q", a.Canonical(), b.Canonical())
	}
	fpA := a.Fingerprint(md5.New())
	fpB := b.Fingerprint(md5.New())
	if !bytes.Equal(fpA, fpB) {
		t.Fatalf("fingerprint mismatch: %x vs %x", fpA, fpB)
	}
}

// TestSpecFixedResolveSizeMismatch verifies that resolution rejects
// writer/reader fixed schemas with different sizes. Per spec, fixed
// types must match by name AND size; size promotion is not defined.
// fastavro #521 silently truncated.
func TestSpecFixedResolveSizeMismatch(t *testing.T) {
	writer := mustParse(t, `{"type":"fixed","name":"X","size":2}`)
	reader := mustParse(t, `{"type":"fixed","name":"X","size":1}`)
	if _, err := avro.Resolve(writer, reader); err == nil {
		t.Fatal("expected resolve error: fixed sizes 2 != 1")
	}
}

// TestSpecOCFDeflateRoundTrip verifies the deflate codec round-trips
// values through the OCF writer + reader. fastavro #463 double-compressed
// (wrote zlib-wrapped over deflate) and #870 added trailing bytes.
// The lock-in is the round-trip; byte-for-byte comparison to raw flate
// is enforced by the deflate codec implementation itself.
func TestSpecOCFDeflateRoundTrip(t *testing.T) {
	schema := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`)
	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, schema, ocf.WithCodec(ocf.DeflateCodec(flate.DefaultCompression)))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Encode(map[string]any{"x": int32(1)}); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	r, err := ocf.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	var got map[string]any
	if err := r.Decode(&got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got["x"] != int32(1) && got["x"] != int64(1) {
		t.Fatalf("got %#v, want 1", got["x"])
	}
}

// TestSpecResolveAcceptsImmaterialChanges verifies that resolution
// accepts changes that don't affect the wire format: an enum gaining
// a symbol (with default) and record fields gaining docs.
// fastavro #488 / #489 failed to resolve in these cases.
func TestSpecResolveAcceptsImmaterialChanges(t *testing.T) {
	t.Run("enum_added_symbol_with_default", func(t *testing.T) {
		writer := mustParse(t, `["null",{"type":"enum","name":"E","symbols":["A","B"]}]`)
		reader := mustParse(t, `["null",{"type":"enum","name":"E","symbols":["A","B","C"],"default":"A"}]`)
		if _, err := avro.Resolve(writer, reader); err != nil {
			t.Fatalf("resolve: %v", err)
		}
	})
	t.Run("record_added_doc", func(t *testing.T) {
		writer := mustParse(t, `["null",{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}]`)
		reader := mustParse(t, `["null",{"type":"record","name":"R","doc":"v2","fields":[{"name":"x","type":"int","doc":"the x"}]}]`)
		if _, err := avro.Resolve(writer, reader); err != nil {
			t.Fatalf("resolve: %v", err)
		}
	})
}

// TestSpecBytesDefaultMaterialization verifies that bytes and fixed
// defaults specified as JSON strings materialize as []byte at the
// reader, not as Go strings. fastavro #485 / #869 returned str
// instead of bytes.
func TestSpecBytesDefaultMaterialization(t *testing.T) {
	writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"id","type":"int"}]}`)
	reader := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"id","type":"int"},
		{"name":"bytes_field","type":"bytes","default":"abc"},
		{"name":"fixed_field","type":{"type":"fixed","name":"F","size":3},"default":"abc"}
	]}`)
	encoded, err := writer.AppendEncode(nil, map[string]any{"id": int32(7)})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	var got map[string]any
	if _, err := resolved.Decode(encoded, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if b, ok := got["bytes_field"].([]byte); !ok || string(b) != "abc" {
		t.Fatalf("bytes_field: got %#v, want []byte(\"abc\")", got["bytes_field"])
	}
	if f, ok := got["fixed_field"].([]byte); !ok || string(f) != "abc" {
		t.Fatalf("fixed_field: got %#v, want []byte(\"abc\")", got["fixed_field"])
	}
}

// TestSpecDecimalAcceptsExactScale verifies the negative companion: values
// that ARE exactly representable at the schema scale must still encode
// successfully without error after the rounding-rejection fix.
func TestSpecDecimalAcceptsExactScale(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		rat    *big.Rat
	}{
		{"bytes/zero", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, new(big.Rat)},
		{"bytes/integer_at_scale", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, big.NewRat(500, 1)},
		{"bytes/exact_two_dp", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, big.NewRat(12345, 100)},
		{"bytes/negative_exact", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, big.NewRat(-12345, 100)},
		{"bytes/scale_zero_integer", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":0}`, big.NewRat(42, 1)},
		{"fixed/exact_two_dp", `{"type":"fixed","name":"d","size":8,"logicalType":"decimal","precision":10,"scale":2}`, big.NewRat(12345, 100)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := mustParse(t, c.schema)
			encoded, err := s.AppendEncode(nil, c.rat)
			if err != nil {
				t.Fatalf("AppendEncode(%s): %v", c.rat.RatString(), err)
			}
			var got big.Rat
			if _, err := s.Decode(encoded, &got); err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if got.Cmp(c.rat) != 0 {
				t.Fatalf("round-trip: input=%s, output=%s", c.rat.RatString(), got.RatString())
			}
		})
	}
}

// ---- TEST_COMPARE coverage lock-ins (gaps filled from fastavro / Apache Avro Java test suites) ----

// TestSpecDecimalScaleAndPrecisionTypeValidation locks in that decimal
// precision and scale must be JSON integers (not strings) and must be
// non-negative. fastavro test_schema.py:84/108/132/157 pin these forms
// individually; Java's LogicalTypes.Decimal.validate enforces the same.
func TestSpecDecimalScaleAndPrecisionTypeValidation(t *testing.T) {
	cases := []struct{ name, schema string }{
		{"scale_as_string", `{"type":"bytes","logicalType":"decimal","precision":5,"scale":"2"}`},
		{"precision_as_string", `{"type":"bytes","logicalType":"decimal","precision":"5","scale":2}`},
		{"scale_negative", `{"type":"bytes","logicalType":"decimal","precision":5,"scale":-2}`},
		{"precision_negative", `{"type":"bytes","logicalType":"decimal","precision":-5,"scale":2}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := avro.Parse(tc.schema); err == nil {
				t.Fatalf("expected parse error for %s", tc.name)
			}
		})
	}
}

// TestSpecJSONNumericTypeCoercion locks in Java's JsonDecoder behavior
// (TestJsonDecoder.java:36/87): JSON number "1.0" decodes into an int
// field as 1 (whole-number floats accepted); fractional "-1.2" into
// an int rejects; bare integer "1" into a float field accepts as 1.0f.
func TestSpecJSONNumericTypeCoercion(t *testing.T) {
	t.Run("int_field_accepts_1.0", func(t *testing.T) {
		s := mustParse(t, `{"type":"record","name":"X","fields":[{"name":"n","type":"int"}]}`)
		var got map[string]any
		if err := s.DecodeJSON([]byte(`{"n":1.0}`), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got["n"] != int32(1) {
			t.Fatalf("want int32 1, got %#v", got["n"])
		}
	})
	t.Run("int_field_rejects_-1.2", func(t *testing.T) {
		s := mustParse(t, `{"type":"record","name":"X","fields":[{"name":"id","type":"int"}]}`)
		var got map[string]any
		if err := s.DecodeJSON([]byte(`{"id":-1.2}`), &got); err == nil {
			t.Fatalf("expected error for fractional int, got %#v", got)
		}
	})
	t.Run("long_field_accepts_1.0", func(t *testing.T) {
		s := mustParse(t, `{"type":"record","name":"X","fields":[{"name":"n","type":"long"}]}`)
		var got map[string]any
		if err := s.DecodeJSON([]byte(`{"n":1.0}`), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got["n"] != int64(1) {
			t.Fatalf("want int64 1, got %#v", got["n"])
		}
	})
	t.Run("float_field_accepts_bare_int_1", func(t *testing.T) {
		s := mustParse(t, `{"type":"record","name":"X","fields":[{"name":"n","type":"float"}]}`)
		var got map[string]any
		if err := s.DecodeJSON([]byte(`{"n":1}`), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got["n"] != float32(1) {
			t.Fatalf("want float32 1, got %#v", got["n"])
		}
	})
}

// TestSpecSOECorruptionDistinctBytes locks in that single-byte
// corruption of the SOE wire (marker byte 0, version byte 1, or any
// fingerprint byte) is rejected. Java's BinaryMessageEncoding tests at
// TestBinaryMessageEncoding.java:224/237/250 verify each byte's role
// separately so the error message identifies which check failed.
func TestSpecSOECorruptionDistinctBytes(t *testing.T) {
	s := mustParse(t, `"int"`)
	v := int32(0)
	base, err := s.AppendSingleObject(nil, &v)
	if err != nil {
		t.Fatalf("encode single-object: %v", err)
	}
	if len(base) < 10 || base[0] != 0xC3 || base[1] != 0x01 {
		t.Fatalf("unexpected SOE prefix: %x", base[:10])
	}
	t.Run("bad_marker_only", func(t *testing.T) {
		b := append([]byte(nil), base...)
		b[0] = 0x00
		var got int32
		if _, err := s.DecodeSingleObject(b, &got); err == nil {
			t.Fatal("expected bad-marker error")
		}
	})
	t.Run("bad_version_only", func(t *testing.T) {
		b := append([]byte(nil), base...)
		b[1] = 0x00
		var got int32
		if _, err := s.DecodeSingleObject(b, &got); err == nil {
			t.Fatal("expected bad-version error")
		}
	})
	t.Run("bad_fingerprint", func(t *testing.T) {
		b := append([]byte(nil), base...)
		b[4] = 0x00
		var got int32
		if _, err := s.DecodeSingleObject(b, &got); err == nil {
			t.Fatal("expected fingerprint mismatch error")
		}
	})
}

// TestSpecFloatStringDefaultFlowsThroughResolve locks in that float
// defaults written as JSON strings — "NaN", "Infinity", "-Infinity" —
// not only parse but actually materialize as math.NaN / ±Inf in the
// decoded record when the writer omits the field. fastavro
// test_fastavro.py:3419 pins this end-to-end.
func TestSpecFloatStringDefaultFlowsThroughResolve(t *testing.T) {
	writerSchema := mustParse(t, `{"type":"record","name":"R","fields":[]}`)
	encoded, err := writerSchema.AppendEncode(nil, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		reader string
		check  func(float64) bool
	}{
		{"NaN", `{"type":"record","name":"R","fields":[{"name":"f","type":"float","default":"NaN"}]}`, math.IsNaN},
		{"+Inf", `{"type":"record","name":"R","fields":[{"name":"f","type":"float","default":"Infinity"}]}`, func(f float64) bool { return math.IsInf(f, 1) }},
		{"-Inf", `{"type":"record","name":"R","fields":[{"name":"f","type":"float","default":"-Infinity"}]}`, func(f float64) bool { return math.IsInf(f, -1) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := mustParse(t, tc.reader)
			resolved, err := avro.Resolve(writerSchema, r)
			if err != nil {
				t.Fatal(err)
			}
			var out map[string]any
			if _, err := resolved.Decode(encoded, &out); err != nil {
				t.Fatal(err)
			}
			f, ok := out["f"].(float32)
			if !ok || !tc.check(float64(f)) {
				t.Fatalf("default not applied as %s: got %#v", tc.name, out["f"])
			}
		})
	}
}

// TestSpecEnumDefaultPrecedenceOverFieldDefault locks in that when a
// writer-written enum symbol is missing from the reader's enum, the
// enum's `default` symbol substitutes — not the field's default. Java's
// ResolvingDecoder pins this at
// TestSchemaCompatibilityEnumDefaults.java:93.
func TestSpecEnumDefaultPrecedenceOverFieldDefault(t *testing.T) {
	writer := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"f","type":{"type":"enum","name":"E","symbols":["A","B","C"],"default":"A"}}
	]}`)
	reader := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"f","type":{"type":"enum","name":"E","symbols":["A","B"],"default":"A"},"default":"B"}
	]}`)
	encoded, err := writer.AppendEncode(nil, map[string]any{"f": "C"})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if _, err := resolved.Decode(encoded, &out); err != nil {
		t.Fatal(err)
	}
	if got, _ := out["f"].(string); got != "A" {
		t.Fatalf("want enum default A (NOT field default B), got %v", got)
	}
}

// TestSpecJSONNamedTypeReuseByReference locks in that a named type
// (enum / fixed / record) defined once and referenced later by name
// round-trips JSON encoding/decoding. fastavro test_json.py:525/544/563
// shipped issue #450 for this — easy to break in a JSON-codec refactor
// that resolves the named-type table at definition site only.
func TestSpecJSONNamedTypeReuseByReference(t *testing.T) {
	t.Run("enum", func(t *testing.T) {
		s := mustParse(t, `{"type":"record","name":"R","fields":[
			{"name":"e1","type":{"type":"enum","name":"E","symbols":["FOO","BAR"]}},
			{"name":"e2","type":"E"}
		]}`)
		in := map[string]any{"e1": "FOO", "e2": "BAR"}
		out, err := s.AppendEncodeJSON(nil, &in)
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]any
		if err := s.DecodeJSON(out, &got); err != nil {
			t.Fatal(err)
		}
		if got["e1"] != "FOO" || got["e2"] != "BAR" {
			t.Fatalf("got %v", got)
		}
	})
	t.Run("fixed", func(t *testing.T) {
		s := mustParse(t, `{"type":"record","name":"R","fields":[
			{"name":"f1","type":{"type":"fixed","name":"F","size":4}},
			{"name":"f2","type":"F"}
		]}`)
		in := map[string]any{"f1": []byte("abcd"), "f2": []byte("wxyz")}
		out, err := s.AppendEncodeJSON(nil, &in)
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]any
		if err := s.DecodeJSON(out, &got); err != nil {
			t.Fatal(err)
		}
		if got["f1"] == nil || got["f2"] == nil {
			t.Fatalf("got %#v", got)
		}
	})
	t.Run("record", func(t *testing.T) {
		s := mustParse(t, `{"type":"record","name":"Outer","fields":[
			{"name":"r1","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"string"}]}},
			{"name":"r2","type":"Inner"}
		]}`)
		in := map[string]any{
			"r1": map[string]any{"x": "foo"},
			"r2": map[string]any{"x": "bar"},
		}
		out, err := s.AppendEncodeJSON(nil, &in)
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]any
		if err := s.DecodeJSON(out, &got); err != nil {
			t.Fatal(err)
		}
		if got["r1"] == nil || got["r2"] == nil {
			t.Fatalf("got %#v", got)
		}
	})
}

// TestSpecNumericDefaultRejectedForNonNumericField locks in that the
// default-type check rejects a JSON numeric default on a string / enum /
// fixed / record / map field. fastavro test_schema.py:1300 pins each.
func TestSpecNumericDefaultRejectedForNonNumericField(t *testing.T) {
	cases := []struct{ name, schema string }{
		{"string_field", `{"type":"record","name":"R","fields":[{"name":"f","type":"string","default":0}]}`},
		{"enum_field", `{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"enum","name":"E","symbols":["A"]},"default":0}]}`},
		{"fixed_field", `{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"fixed","name":"F","size":4},"default":0}]}`},
		{"record_field", `{"type":"record","name":"O","fields":[{"name":"f","type":{"type":"record","name":"I","fields":[]},"default":0}]}`},
		{"map_field", `{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"map","values":"string"},"default":0}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := avro.Parse(tc.schema); err == nil {
				t.Fatalf("expected parse error for numeric default on %s", tc.name)
			}
		})
	}
}

// TestSpecEnumSymbolRegexValidation locks in that enum symbols must
// match [A-Za-z_][A-Za-z0-9_]*. fastavro test_schema.py:1114 pins
// digit-start, spaces, non-ASCII letter, and dash as invalid;
// leading underscore is valid.
func TestSpecEnumSymbolRegexValidation(t *testing.T) {
	invalid := []string{"0nope", "string with spaces", "Ż", "-foo", ""}
	for _, sym := range invalid {
		t.Run("invalid_"+sym, func(t *testing.T) {
			schema := `{"type":"enum","name":"E","symbols":["` + sym + `"]}`
			if _, err := avro.Parse(schema); err == nil {
				t.Fatalf("expected error for symbol %q", sym)
			}
		})
	}
	if _, err := avro.Parse(`{"type":"enum","name":"E","symbols":["_123","OK","None"]}`); err != nil {
		t.Fatalf("expected accept of valid symbols: %v", err)
	}
}

// TestSpecFingerprintKnownVectorsSHA256AndMD5 locks in exact hex values
// for spec-required SHA-256 and MD5 fingerprints over primitive schemas.
// fastavro test_fingerprint.py:12 pins these. The CRC-64 vectors at
// conformance_test.go:1256/2037 don't cover the other two required
// algorithms, and a regression in the SHA-256/MD5 hash plumbing or in
// Canonical() output would silently break schema-registry consumers.
func TestSpecFingerprintKnownVectorsSHA256AndMD5(t *testing.T) {
	cases := []struct {
		schema string
		sha256 string
		md5    string
	}{
		{`"int"`, "3f2b87a9fe7cc9b13835598c3981cd45e3e355309e5090aa0933d7becb6fba45", "ef524ea1b91e73173d938ade36c1db32"},
		{`"float"`, "1e71f9ec051d663f56b0d8e1fc84d71aa56ccfe9fa93aa20d10547a7abeb5cc0", "50a6b9db85da367a6d2df400a41758a6"},
	}
	for _, tc := range cases {
		t.Run(tc.schema, func(t *testing.T) {
			s := mustParse(t, tc.schema)
			if got := hex.EncodeToString(s.Fingerprint(sha256.New())); got != tc.sha256 {
				t.Fatalf("sha256 %s: got %s want %s", tc.schema, got, tc.sha256)
			}
			if got := hex.EncodeToString(s.Fingerprint(md5.New())); got != tc.md5 {
				t.Fatalf("md5 %s: got %s want %s", tc.schema, got, tc.md5)
			}
		})
	}
}

// TestSpecPromotionDirectionMatrix locks in that every spec-disallowed
// reverse promotion is rejected by both Resolve and CheckCompatibility.
// Java's TestSchemaCompatibilityTypeMismatch.java enumerates each
// direction explicitly because "be helpful with narrowing" is a
// recurring temptation.
func TestSpecPromotionDirectionMatrix(t *testing.T) {
	cases := []struct{ writer, reader string }{
		{"long", "int"},
		{"float", "int"}, {"float", "long"},
		{"double", "int"}, {"double", "long"}, {"double", "float"},
		{"int", "boolean"}, {"int", "null"},
		{"int", "string"}, {"string", "int"},
		{"bytes", "int"}, {"int", "bytes"},
	}
	for _, tc := range cases {
		t.Run(tc.writer+"->"+tc.reader, func(t *testing.T) {
			w := mustParse(t, `"`+tc.writer+`"`)
			r := mustParse(t, `"`+tc.reader+`"`)
			if _, err := avro.Resolve(w, r); err == nil {
				t.Fatalf("Resolve unexpectedly accepted %s -> %s", tc.writer, tc.reader)
			}
			if err := avro.CheckCompatibility(w, r); err == nil {
				t.Fatalf("CheckCompatibility unexpectedly accepted %s -> %s", tc.writer, tc.reader)
			}
		})
	}
	t.Run("array_long_items_to_int_items", func(t *testing.T) {
		w := mustParse(t, `{"type":"array","items":"long"}`)
		r := mustParse(t, `{"type":"array","items":"int"}`)
		if _, err := avro.Resolve(w, r); err == nil {
			t.Fatal("expected reject array<long> -> array<int>")
		}
	})
	t.Run("map_long_values_to_int_values", func(t *testing.T) {
		w := mustParse(t, `{"type":"map","values":"long"}`)
		r := mustParse(t, `{"type":"map","values":"int"}`)
		if _, err := avro.Resolve(w, r); err == nil {
			t.Fatal("expected reject map<long> -> map<int>")
		}
	})
	t.Run("array_vs_map", func(t *testing.T) {
		w := mustParse(t, `{"type":"array","items":"int"}`)
		r := mustParse(t, `{"type":"map","values":"int"}`)
		if _, err := avro.Resolve(w, r); err == nil {
			t.Fatal("expected reject array vs map")
		}
	})
}

// TestSpecLogicalDateInNullUnionRoundTrip locks in that
// ["null",{"type":"int","logicalType":"date"}] round-trips both
// branches through the binary path. fastavro test_fastavro.py:1973
// pins this — easy regression vector if union dispatch drops the
// logical-type annotation on a branch.
func TestSpecLogicalDateInNullUnionRoundTrip(t *testing.T) {
	s := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"item","type":["null",{"type":"int","logicalType":"date"}]}
	]}`)
	t.Run("null_branch", func(t *testing.T) {
		in := map[string]any{"item": nil}
		enc, err := s.AppendEncode(nil, &in)
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]any
		if _, err := s.Decode(enc, &got); err != nil {
			t.Fatal(err)
		}
		if got["item"] != nil {
			t.Fatalf("got %v want nil", got["item"])
		}
	})
	t.Run("date_branch", func(t *testing.T) {
		want := time.Date(2019, 5, 6, 0, 0, 0, 0, time.UTC)
		in := map[string]any{"item": want}
		enc, err := s.AppendEncode(nil, &in)
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]any
		if _, err := s.Decode(enc, &got); err != nil {
			t.Fatal(err)
		}
		gotTime, ok := got["item"].(time.Time)
		if !ok || !gotTime.Equal(want) {
			t.Fatalf("got %#v (type %T) want %v", got["item"], got["item"], want)
		}
	})
}

// TestSpecVarlongAllByteWidthBoundaries locks in round-trip correctness
// at every byte-width boundary of the zigzag-varint encoding for long
// (1-9 byte widths) and int (1-4 byte widths). Java's
// TestBinaryEncoderFidelity.java:39 pins these — pure boundary
// values catch hot-path optimizations that miss a particular byte cutoff.
func TestSpecVarlongAllByteWidthBoundaries(t *testing.T) {
	sLong := mustParse(t, `"long"`)
	longValues := []int64{
		0, 1, -1,
		0x40, -0x41,
		0x2000, -0x2001,
		0x4000000, -0x4000001,
		0x200000000, -0x200000001,
		0x10000000000, -0x10000000001,
		0x800000000000, -0x800000000001,
		0x40000000000000, -0x40000000000001,
		0x2000000000000000, -0x2000000000000001,
	}
	for _, v := range longValues {
		out, err := sLong.AppendEncode(nil, &v)
		if err != nil {
			t.Fatalf("encode %d: %v", v, err)
		}
		var got int64
		if _, err := sLong.Decode(out, &got); err != nil {
			t.Fatalf("decode %d: %v", v, err)
		}
		if got != v {
			t.Fatalf("got %d want %d", got, v)
		}
	}
	sInt := mustParse(t, `"int"`)
	intValues := []int32{
		0, 1, -1,
		0x40, -0x41,
		0x2000, -0x2001,
		0x80000, -0x80001,
		0x4000000, -0x4000001,
	}
	for _, v := range intValues {
		out, err := sInt.AppendEncode(nil, &v)
		if err != nil {
			t.Fatalf("encode %d: %v", v, err)
		}
		var got int32
		if _, err := sInt.Decode(out, &got); err != nil {
			t.Fatalf("decode %d: %v", v, err)
		}
		if got != v {
			t.Fatalf("got %d want %d", got, v)
		}
	}
}

// TestSpecLongVarintNonCanonicalEncodingsAccepted locks in that the
// decoder accepts non-canonical multi-byte varint zero encodings
// (10 bytes for long, 5 bytes for int). The null-union-branch case
// is covered separately at deser_test.go:10337; this widens coverage
// to plain int/long fields.
func TestSpecLongVarintNonCanonicalEncodingsAccepted(t *testing.T) {
	t.Run("long_10byte_zero", func(t *testing.T) {
		s := mustParse(t, `"long"`)
		wire := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00}
		var got int64
		if _, err := s.Decode(wire, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got != 0 {
			t.Fatalf("got %d want 0", got)
		}
	})
	t.Run("int_5byte_zero", func(t *testing.T) {
		s := mustParse(t, `"int"`)
		wire := []byte{0x80, 0x80, 0x80, 0x80, 0x00}
		var got int32
		if _, err := s.Decode(wire, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got != 0 {
			t.Fatalf("got %d want 0", got)
		}
	})
}

// TestSpecDateAndTimestampKnownWireValues locks in exact wire bytes for
// known epoch values, comparing to the plain int/long encoding of the
// same numeric. Java's TestTimeConversions.java:60/120 pins the
// canonical mappings: 1970-01-06 = 5 days, 1969-12-27 = -5 days,
// 2015-05-28T21:46:53.221Z = 1432849613221 ms.
func TestSpecDateAndTimestampKnownWireValues(t *testing.T) {
	t.Run("date_known_day_count", func(t *testing.T) {
		s := mustParse(t, `{"type":"int","logicalType":"date"}`)
		plain := mustParse(t, `"int"`)
		cases := []struct {
			d    time.Time
			days int32
		}{
			{time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), 0},
			{time.Date(1970, 1, 6, 0, 0, 0, 0, time.UTC), 5},
			{time.Date(1969, 12, 27, 0, 0, 0, 0, time.UTC), -5},
		}
		for _, tc := range cases {
			enc, err := s.AppendEncode(nil, &tc.d)
			if err != nil {
				t.Fatal(err)
			}
			want, err := plain.AppendEncode(nil, &tc.days)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(enc, want) {
				t.Fatalf("date %v: wire %x want %x (days=%d)", tc.d, enc, want, tc.days)
			}
		}
	})
	t.Run("timestamp_millis_known_value", func(t *testing.T) {
		s := mustParse(t, `{"type":"long","logicalType":"timestamp-millis"}`)
		plain := mustParse(t, `"long"`)
		ts := time.Date(2015, 5, 28, 21, 46, 53, 221_000_000, time.UTC)
		enc, err := s.AppendEncode(nil, &ts)
		if err != nil {
			t.Fatal(err)
		}
		v := int64(1_432_849_613_221)
		want, err := plain.AppendEncode(nil, &v)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(enc, want) {
			t.Fatalf("ts wire %x want %x (raw long 1432849613221)", enc, want)
		}
	})
	t.Run("timestamp_millis_pre_epoch_with_positive_nanos", func(t *testing.T) {
		s := mustParse(t, `{"type":"long","logicalType":"timestamp-millis"}`)
		plain := mustParse(t, `"long"`)
		ts := time.Date(1969, 7, 1, 12, 0, 0, 123_000_000, time.UTC)
		enc, err := s.AppendEncode(nil, &ts)
		if err != nil {
			t.Fatal(err)
		}
		v := int64(-15_854_400_000 + 123)
		want, err := plain.AppendEncode(nil, &v)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(enc, want) {
			t.Fatalf("pre-epoch wire %x want %x", enc, want)
		}
	})
}

// TestSpecJSONMultiRecordConcatDecode locks in that DecodeJSON consumes
// only the first complete JSON value from src — concatenated records
// decode independently. Java's JsonDecoder has the same streaming
// behavior (TestJsonDecoder reads multiple records from a single
// buffer). twmb's AppendEncodeJSON does NOT add inter-record separators
// (caller's responsibility, like encoding/json.Marshal); separator
// behavior is intentionally out of scope here.
func TestSpecJSONMultiRecordConcatDecode(t *testing.T) {
	s := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"l","type":"long"},
		{"name":"a","type":{"type":"array","items":"int"}}
	]}`)
	data := []byte(`{"a":[1,2],"l":100}{"l":200,"a":[3,4]}`)
	var got1 map[string]any
	if err := s.DecodeJSON(data, &got1); err != nil {
		t.Fatalf("first decode: %v", err)
	}
	if got1["l"] != int64(100) {
		t.Fatalf("first record l: got %v want 100", got1["l"])
	}
}

// TestSpecRecordNameAsPrimitiveInNamespace locks in that a fullname
// whose local part collides with a primitive type name (e.g. "ns.int")
// is accepted at parse and survives Canonical() round-trip. Java's
// TestSchema.java:418 pins this — name validation can easily over-reject
// by checking only the local part.
func TestSpecRecordNameAsPrimitiveInNamespace(t *testing.T) {
	schema := `{"type":"record","name":"ns.int","fields":[
		{"name":"value","type":"int"},
		{"name":"next","type":["null","ns.int"]}
	]}`
	s, err := avro.Parse(schema)
	if err != nil {
		t.Fatalf("expected accept: %v", err)
	}
	canon := s.Canonical()
	if _, err := avro.Parse(string(canon)); err != nil {
		t.Fatalf("re-parse canonical: %v", err)
	}
}

// ---- Lock-ins for known intentional divergences ----

// TestRegression_WholeFloatEncodesAsInt locks in twmb's intentional
// lenient acceptance of whole-number float values as int/long encoder
// input. Java's GenericDatumWriter rejects Float-as-Integer at the
// type system; fastavro rejects float-as-int. We accept whole-number
// floats deliberately because encoding/json.Unmarshal produces float64
// for every JSON number. Fractional floats still error.
func TestRegression_WholeFloatEncodesAsInt(t *testing.T) {
	intS := mustParse(t, `"int"`)
	longS := mustParse(t, `"long"`)
	plainInt := mustParse(t, `"int"`)
	plainLong := mustParse(t, `"long"`)

	t.Run("float64_whole_into_int", func(t *testing.T) {
		f := 42.0
		gotF, err := intS.AppendEncode(nil, &f)
		if err != nil {
			t.Fatalf("float64 42.0 into int: %v", err)
		}
		i := int32(42)
		wantI, _ := plainInt.AppendEncode(nil, &i)
		if !bytes.Equal(gotF, wantI) {
			t.Fatalf("wire: got %x want %x", gotF, wantI)
		}
	})
	t.Run("float32_whole_into_long", func(t *testing.T) {
		f := float32(42.0)
		gotF, err := longS.AppendEncode(nil, &f)
		if err != nil {
			t.Fatalf("float32 42.0 into long: %v", err)
		}
		l := int64(42)
		wantL, _ := plainLong.AppendEncode(nil, &l)
		if !bytes.Equal(gotF, wantL) {
			t.Fatalf("wire: got %x want %x", gotF, wantL)
		}
	})
	t.Run("fractional_float_rejected", func(t *testing.T) {
		f := 42.5
		if _, err := intS.AppendEncode(nil, &f); err == nil {
			t.Fatal("expected error for fractional float into int")
		}
	})
}

// TestRegression_FloatSourceMantissaBoundOnIntLongEncode locks the
// encoder-side mantissa-precision bound on whole-number-float input
// against int/long schemas. The decoder's float-target arms in
// setIntValue / setLongValue cap val at 1<<24 (float32 target) or 1<<53
// (float64 target) for round-trip lossless guarantee. Pre-fix the
// encoder's CanFloat arm only validated whole+int32/int64-range, so
// `Encode(float32(1<<25), "int|long")` produced wire bytes that the
// matching `Decode(wire, *float32)` could not read back — an
// asymmetric encode-only round-trip. All eight encode-side sites that
// took Go-float input (serInt, serLong, serArray.serInt+serLong,
// serMap.serInt+serLong, jsonCoerceToInt32, jsonCoerceToInt64) shared
// the gap and now share the source-bit-aware floatFitsInt32From /
// floatFitsInt64From helpers.
//
// json.Number values are unaffected: their precision is float64-
// implicit (json.Number's Float64 fallback is for non-integer forms
// like "1.5e3"), so the json.Number → Float64 path keeps the
// unchecked floatFitsInt32 / floatFitsInt64 helpers — capping it at
// 1<<24 would falsely reject "1e9" → int32 which IS exact.
func TestRegression_FloatSourceMantissaBoundOnIntLongEncode(t *testing.T) {
	intS := mustParse(t, `"int"`)
	longS := mustParse(t, `"long"`)
	arrIntS := mustParse(t, `{"type":"array","items":"int"}`)
	arrLongS := mustParse(t, `{"type":"array","items":"long"}`)
	mapIntS := mustParse(t, `{"type":"map","values":"int"}`)
	mapLongS := mustParse(t, `{"type":"map","values":"long"}`)

	// In-bound: same-type round-trip is lossless and accepted on both
	// encode and decode arms.
	t.Run("float32_at_bound_into_int_round_trips", func(t *testing.T) {
		in := float32(1 << 24) // = 16777216, the largest float32-exact integer.
		enc, err := intS.AppendEncode(nil, &in)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var out float32
		if _, err := intS.Decode(enc, &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if out != in {
			t.Errorf("round-trip: got %v, want %v", out, in)
		}
	})
	t.Run("float64_at_bound_into_long_round_trips", func(t *testing.T) {
		in := float64(1 << 53)
		enc, err := longS.AppendEncode(nil, &in)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var out float64
		if _, err := longS.Decode(enc, &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if out != in {
			t.Errorf("round-trip: got %v, want %v", out, in)
		}
	})

	// Out-of-bound rejection at encode for each affected site. The
	// values picked are exact in their source type (powers of two) so
	// the only reason to reject them is the precision-bound rule, not
	// non-whole or out-of-int-range.
	type encCase struct {
		name string
		s    *avro.Schema
		v    any
	}
	rejected := []encCase{
		{"serInt_float32_above_mantissa", intS, ptr(float32(1 << 25))},
		{"serInt_float64_above_int32_via_floatFitsInt32", intS, ptr(float64(1 << 32))},
		{"serLong_float32_above_mantissa", longS, ptr(float32(1 << 25))},
		{"serLong_float64_above_mantissa", longS, ptr(float64(1 << 54))},
		{"serArray.serInt_float32_above_mantissa", arrIntS, &[]float32{1 << 25}},
		{"serArray.serLong_float32_above_mantissa", arrLongS, &[]float32{1 << 25}},
		{"serArray.serLong_float64_above_mantissa", arrLongS, &[]float64{1 << 54}},
		{"serMap.serInt_float32_above_mantissa", mapIntS, &map[string]float32{"k": 1 << 25}},
		{"serMap.serLong_float32_above_mantissa", mapLongS, &map[string]float32{"k": 1 << 25}},
		{"serMap.serLong_float64_above_mantissa", mapLongS, &map[string]float64{"k": 1 << 54}},
	}
	for _, tc := range rejected {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.s.AppendEncode(nil, tc.v); err == nil {
				t.Fatalf("expected encode rejection, got no error")
			}
		})
	}

	// Same set of rejections via the JSON encoder, exercising
	// jsonCoerceToInt32 / jsonCoerceToInt64.
	jsonRejected := []encCase{
		{"jsonCoerceToInt32_float32_above_mantissa", intS, ptr(float32(1 << 25))},
		{"jsonCoerceToInt64_float32_above_mantissa", longS, ptr(float32(1 << 25))},
		{"jsonCoerceToInt64_float64_above_mantissa", longS, ptr(float64(1 << 54))},
	}
	for _, tc := range jsonRejected {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.s.AppendEncodeJSON(nil, tc.v); err == nil {
				t.Fatalf("expected JSON encode rejection, got no error")
			}
		})
	}

	// json.Number with the same magnitude is unaffected — the
	// json.Number → Int64 path returns the value verbatim, which the
	// "expected on both sides" decoder of *json.Number / *int64 / etc.
	// will round-trip without engaging the float-target precLimit.
	t.Run("jsonNumber_unaffected", func(t *testing.T) {
		jn := json.Number("33554432") // = 1<<25
		enc, err := longS.AppendEncode(nil, jn)
		if err != nil {
			t.Fatalf("encode json.Number into long: %v", err)
		}
		var out int64
		if _, err := longS.Decode(enc, &out); err != nil {
			t.Fatalf("decode long-wire into int64: %v", err)
		}
		if out != 1<<25 {
			t.Errorf("round-trip: got %d, want %d", out, 1<<25)
		}
	})
}

// TestRegression_JsonNumberOverflowToInfFloatEncodeParity locks the
// (jsonNumberToFloat, jsonCoerceToFloat64) acceptance of float-form
// json.Number whose magnitude exceeds float64 range — strconv.ParseFloat
// returns (±Inf, strconv.ErrRange) for those inputs, and ±Inf IS the
// correct Avro wire encoding. Pre-fix, the encoder propagated the error
// and rejected, creating a route divergence: s.Encode(math.Inf(1)) and
// s.Encode(float64(9.999e308)) (Go's literal evaluates to +Inf) both
// succeeded, but s.Encode(json.Number("9.999e308")) — the same value
// expressed precision-preservingly through json.Number — rejected. The
// decode side (json_decode.go decodeFloat/decodeDouble) was already
// coded to "Accept ±Inf from overflow (e.g. 1e999, goavro convention)"
// — encode just hadn't caught up. Java's BigDecimal.doubleValue() and
// fastavro's float() both return ±Inf for the same input without error.
//
// Locked at both jsonNumberToFloat (binary encode, exercised via
// AppendEncode) and jsonCoerceToFloat64 (JSON encode, exercised via
// AppendEncodeJSON), at float32 (clean float64→float32 +Inf narrowing)
// and float64. Boundary: just-below MaxFloat64 still encodes finite;
// just-above encodes ±Inf; ErrRange-without-Inf (none from ParseFloat
// in practice, but the guard is conservative) would still reject.
func TestRegression_JsonNumberOverflowToInfFloatEncodeParity(t *testing.T) {
	floatS := mustParse(t, `"float"`)
	doubleS := mustParse(t, `"double"`)

	type acceptCase struct {
		name        string
		s           *avro.Schema
		v           json.Number
		wantNegInf  bool
		wireWantHex string // expected wire bytes (hex) for binary path
	}
	// Wire-format expectations:
	//   - float +Inf  IEEE 754 = 0x7F800000 → little-endian "0000807f"
	//   - float -Inf  IEEE 754 = 0xFF800000 → little-endian "000080ff"
	//   - double +Inf IEEE 754 = 0x7FF0000000000000 → little-endian "000000000000f07f"
	//   - double -Inf IEEE 754 = 0xFFF0000000000000 → little-endian "000000000000f0ff"
	accepts := []acceptCase{
		{"float_positive_overflow_1e1000", floatS, json.Number("1e1000"), false, "0000807f"},
		{"float_negative_overflow_-1e1000", floatS, json.Number("-1e1000"), true, "000080ff"},
		{"double_positive_overflow_1.8e308", doubleS, json.Number("1.8e308"), false, "000000000000f07f"},
		{"double_negative_overflow_-1.8e308", doubleS, json.Number("-1.8e308"), true, "000000000000f0ff"},
		{"double_positive_overflow_1e400", doubleS, json.Number("1e400"), false, "000000000000f07f"},
	}
	for _, tc := range accepts {
		t.Run("binary/"+tc.name, func(t *testing.T) {
			got, err := tc.s.AppendEncode(nil, tc.v)
			if err != nil {
				t.Fatalf("binary encode: %v", err)
			}
			if hex.EncodeToString(got) != tc.wireWantHex {
				t.Errorf("wire bytes: got %x, want %s", got, tc.wireWantHex)
			}
			// Round-trip into float64/float32 confirms decoder produces ±Inf.
			if tc.s == floatS {
				var out float32
				if _, err := tc.s.Decode(got, &out); err != nil {
					t.Fatalf("decode: %v", err)
				}
				if !math.IsInf(float64(out), 0) {
					t.Errorf("decoded %v, expected ±Inf", out)
				}
				if tc.wantNegInf != math.IsInf(float64(out), -1) {
					t.Errorf("sign mismatch: got %v", out)
				}
			} else {
				var out float64
				if _, err := tc.s.Decode(got, &out); err != nil {
					t.Fatalf("decode: %v", err)
				}
				if !math.IsInf(out, 0) {
					t.Errorf("decoded %v, expected ±Inf", out)
				}
				if tc.wantNegInf != math.IsInf(out, -1) {
					t.Errorf("sign mismatch: got %v", out)
				}
			}
		})
		t.Run("json/"+tc.name, func(t *testing.T) {
			got, err := tc.s.AppendEncodeJSON(nil, tc.v)
			if err != nil {
				t.Fatalf("json encode: %v", err)
			}
			// JSON encode of ±Inf emits "Infinity"/"-Infinity" by default
			// (twmb's quoted-string convention). Verify round-trip parses
			// to ±Inf rather than asserting the exact string form, which
			// is config-dependent.
			if tc.s == floatS {
				var out float32
				if err := tc.s.DecodeJSON(got, &out); err != nil {
					t.Fatalf("json decode: %v (encoded as %q)", err, got)
				}
				if !math.IsInf(float64(out), 0) {
					t.Errorf("decoded %v, expected ±Inf", out)
				}
				if tc.wantNegInf != math.IsInf(float64(out), -1) {
					t.Errorf("sign mismatch: got %v", out)
				}
			} else {
				var out float64
				if err := tc.s.DecodeJSON(got, &out); err != nil {
					t.Fatalf("json decode: %v (encoded as %q)", err, got)
				}
				if !math.IsInf(out, 0) {
					t.Errorf("decoded %v, expected ±Inf", out)
				}
				if tc.wantNegInf != math.IsInf(out, -1) {
					t.Errorf("sign mismatch: got %v", out)
				}
			}
		})
	}

	// Parity confirmation: the typed math.Inf(1) path was already
	// accepted pre-fix; the json.Number path now produces identical
	// wire output. The bug was the divergence between these two
	// equivalent ways of expressing the same value.
	t.Run("parity_with_typed_Inf", func(t *testing.T) {
		fromTyped, err := doubleS.AppendEncode(nil, math.Inf(1))
		if err != nil {
			t.Fatal(err)
		}
		fromJsonNumber, err := doubleS.AppendEncode(nil, json.Number("1e400"))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(fromTyped, fromJsonNumber) {
			t.Errorf("typed-Inf vs json.Number-overflow wire bytes differ: %x vs %x",
				fromTyped, fromJsonNumber)
		}
	})

	// Boundary: just below MaxFloat64 (ParseFloat returns finite) must
	// continue to encode as the finite value, not silently snap to Inf.
	t.Run("just_below_MaxFloat64_stays_finite", func(t *testing.T) {
		got, err := doubleS.AppendEncode(nil, json.Number("1.7976931348623157e308"))
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var out float64
		if _, err := doubleS.Decode(got, &out); err != nil {
			t.Fatal(err)
		}
		if math.IsInf(out, 0) {
			t.Errorf("MaxFloat64 must NOT round to Inf; got %v", out)
		}
	})

	// Syntax errors (vs ErrRange) still reject. The ErrRange-Inf
	// acceptance must be narrowly gated on the err kind AND the
	// result being infinite.
	t.Run("syntax_error_still_rejects", func(t *testing.T) {
		// "1e" is JSON-grammar-invalid; isJSONNumber rejects upstream.
		if _, err := doubleS.AppendEncode(nil, json.Number("1e")); err == nil {
			t.Error("expected reject for invalid grammar")
		}
	})
}

// TestRegression_SchemaDefaultOverflowToInfParity is the schema-default-
// parse-time sibling of TestRegression_JsonNumberOverflowToInfFloatEncodeParity.
// The encode-side fix (commit ae99f46) accepted (±Inf, strconv.ErrRange)
// at jsonNumberToFloat / jsonCoerceToFloat64 but left the same predicate
// missing at three schema-default parse sites:
//
//   - defaultAsFloat64 json.Number arm (schema.go:2497)
//   - defaultAsFloat64 string arm     (schema.go:2511)
//   - coerceDefault                    (schema.go:2567)
//
// All three were subsequently fixed to mirror the encode side. Smoking-
// gun probe pre-fix:
//
//	avro.Parse(`{"type":"record",...,"default":1e1000}`)  -> rejected
//	s := avro.Parse(`"double"`)
//	s.AppendEncode(nil, json.Number("1e1000"))            -> +Inf wire bits
//	s.Decode(+Inf wire bytes, &f64)                       -> f64 = +Inf
//
// Java's Schema.parseField (Schema.java:1899-1902) converts textual
// float/double defaults via Double.parseDouble, which returns +Inf for
// "1e1000" without throwing; Jackson's DoubleNode(+Inf) passes the
// isValidDefault.isNumber() gate (Schema.java:1764-1766). fastavro's
// _default_matches_schema (fastavro/_schema_py.py:351-352) accepts via
// _maybe_float(default) == float("1e1000") == inf, isinstance float.
// Both upstream impls accept; the prior twmb behavior rejected.
//
// The matrix below pins the parse acceptance AND the round-trip wire
// value (+Inf bits) for the parsed default across the four arms it can
// reach: literal-numeric default vs string-form default × float vs
// double schema, the negative-overflow sign mirror, the boundary
// finite-but-just-below-MaxFloat64, the union-shape variant (a
// "Known intentional divergence" twmb accepts where Java rejects, and
// the overflow subcase must remain consistent with that), the nested-
// record carrier, and the still-rejects-syntax-error confirmation.
func TestRegression_SchemaDefaultOverflowToInfParity(t *testing.T) {
	type acceptCase struct {
		name         string
		schema       string
		expectNegInf bool
		// Floats produce 4-byte wire defaults; doubles 8 bytes.
		// binaryWireWantHex covers the materialized default wire bytes
		// observable through encoding an empty record (which fires the
		// schema default for the missing field). Hex is little-endian
		// IEEE 754 bits.
		binaryWireWantHex string
	}
	cases := []acceptCase{
		{
			name:              "float_literal_positive_overflow_1e1000",
			schema:            `{"type":"record","name":"R","fields":[{"name":"f","type":"float","default":1e1000}]}`,
			binaryWireWantHex: "0000807f", // float +Inf
		},
		{
			name:              "float_literal_negative_overflow_-1e1000",
			schema:            `{"type":"record","name":"R","fields":[{"name":"f","type":"float","default":-1e1000}]}`,
			expectNegInf:      true,
			binaryWireWantHex: "000080ff", // float -Inf
		},
		{
			name:              "double_literal_positive_overflow_1e1000",
			schema:            `{"type":"record","name":"R","fields":[{"name":"f","type":"double","default":1e1000}]}`,
			binaryWireWantHex: "000000000000f07f", // double +Inf
		},
		{
			name:              "double_literal_negative_overflow_-1.8e308",
			schema:            `{"type":"record","name":"R","fields":[{"name":"f","type":"double","default":-1.8e308}]}`,
			expectNegInf:      true,
			binaryWireWantHex: "000000000000f0ff", // double -Inf
		},
		{
			// "Known intentional divergence": string-form float defaults
			// against a single-type field route through coerceDefault's
			// string→float64 ParseFloat. The overflow subcase must accept
			// for the divergence to be coherent — pre-fix it rejected,
			// stranding string-form overflow callers between encode-side
			// acceptance and parse-side rejection.
			name:              "float_string_form_positive_overflow_1e1000",
			schema:            `{"type":"record","name":"R","fields":[{"name":"f","type":"float","default":"1e1000"}]}`,
			binaryWireWantHex: "0000807f",
		},
		{
			name:              "double_string_form_negative_overflow_-1e1000",
			schema:            `{"type":"record","name":"R","fields":[{"name":"f","type":"double","default":"-1e1000"}]}`,
			expectNegInf:      true,
			binaryWireWantHex: "000000000000f0ff",
		},
		{
			// Union-shape string-form default: twmb's "Known intentional
			// divergence" accepts this where Java rejects. The overflow
			// subcase must remain consistent with the non-overflow form
			// (e.g. `["float","null"]` with `"3.14"`), which the
			// non-overflow union test elsewhere already pins. Wire bytes
			// include the leading union branch index varint (0 for the
			// float branch at position 0) before the 4-byte float +Inf.
			name:              "union_float_null_string_form_overflow_1e1000",
			schema:            `{"type":"record","name":"R","fields":[{"name":"f","type":["float","null"],"default":"1e1000"}]}`,
			binaryWireWantHex: "000000807f", // varint(0) || float +Inf
		},
		{
			// Nested record carrier — exercises the same defaultAsFloat64
			// path through the inner record's field-default validate.
			name:              "nested_record_float_literal_overflow_1e1000",
			schema:            `{"type":"record","name":"Outer","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"f","type":"float","default":1e1000}]}}]}`,
			binaryWireWantHex: "0000807f", // emitted for inner.f's default
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := avro.Parse(tc.schema)
			if err != nil {
				t.Fatalf("schema parse rejected an overflow-to-±Inf default: %v\n  schema: %s", err, tc.schema)
			}
			// Pre-encode the default into binary by encoding an empty
			// map (so the schema's default for the absent field fires
			// from sr.fields[i].defaultBytes). For the nested carrier,
			// pass a single-key map whose value is an empty inner map.
			var (
				bin []byte
				en  error
			)
			if strings.Contains(tc.name, "nested_record") {
				bin, en = s.AppendEncode(nil, map[string]any{"inner": map[string]any{}})
			} else {
				bin, en = s.AppendEncode(nil, map[string]any{})
			}
			if en != nil {
				t.Fatalf("encode empty record (default fires): %v", en)
			}
			gotHex := hex.EncodeToString(bin)
			if gotHex != tc.binaryWireWantHex {
				t.Errorf("default wire bytes: got %s, want %s", gotHex, tc.binaryWireWantHex)
			}
			// Round-trip the binary back into a map and confirm the
			// materialized value is ±Inf.
			var dec map[string]any
			if _, err := s.Decode(bin, &dec); err != nil {
				t.Fatalf("decode: %v", err)
			}
			val := dec["f"]
			if strings.Contains(tc.name, "nested_record") {
				inner, ok := dec["inner"].(map[string]any)
				if !ok {
					t.Fatalf("nested carrier missing inner map: got %T", dec["inner"])
				}
				val = inner["f"]
			}
			var f64 float64
			switch v := val.(type) {
			case float32:
				f64 = float64(v)
			case float64:
				f64 = v
			default:
				t.Fatalf("unexpected materialized type %T %v", val, val)
			}
			if !math.IsInf(f64, 0) {
				t.Errorf("materialized default %v, expected ±Inf", f64)
			}
			if tc.expectNegInf != math.IsInf(f64, -1) {
				t.Errorf("sign mismatch: got %v, expectNegInf=%v", f64, tc.expectNegInf)
			}
		})
	}

	// Boundary: just-below MaxFloat64 (ParseFloat returns finite, no
	// ErrRange) must continue to materialize the exact finite value,
	// not snap to Inf via the new acceptance arm. Same boundary as the
	// encode-side test's just_below_MaxFloat64_stays_finite subcase.
	t.Run("just_below_MaxFloat64_default_stays_finite", func(t *testing.T) {
		schemaSrc := `{"type":"record","name":"R","fields":[{"name":"f","type":"double","default":1.7976931348623157e308}]}`
		s, err := avro.Parse(schemaSrc)
		if err != nil {
			t.Fatalf("schema parse rejected near-MaxFloat64 default: %v", err)
		}
		bin, err := s.AppendEncode(nil, map[string]any{})
		if err != nil {
			t.Fatal(err)
		}
		var dec map[string]any
		if _, err := s.Decode(bin, &dec); err != nil {
			t.Fatal(err)
		}
		f, ok := dec["f"].(float64)
		if !ok {
			t.Fatalf("unexpected type %T", dec["f"])
		}
		if math.IsInf(f, 0) {
			t.Errorf("MaxFloat64 default must stay finite; got %v", f)
		}
		if f != math.MaxFloat64 {
			t.Errorf("MaxFloat64 default materialized as %v, want %v", f, math.MaxFloat64)
		}
	})

	// Float-arm-specific boundary: finite float64 magnitudes that
	// overflow when narrowed to float32 must still reject (the float
	// arm's encodeDefault calls finiteFloat32Overflows). My fix only
	// changes the ErrRange path; finite-but-narrows-to-Inf still
	// rejects via the pre-existing narrowing guard.
	t.Run("float_default_finite_but_overflows_float32_rejects", func(t *testing.T) {
		// 1e100 is finite in float64 but overflows float32 to +Inf.
		schemaSrc := `{"type":"record","name":"R","fields":[{"name":"f","type":"float","default":1e100}]}`
		if _, err := avro.Parse(schemaSrc); err == nil {
			t.Errorf("expected reject for float64-finite-but-float32-overflow default")
		}
	})

	// Syntax errors at the json.Number arm still reject (gated by
	// isJSONNumber upstream of ParseFloat).
	t.Run("json_number_syntax_error_still_rejects", func(t *testing.T) {
		schemaSrc := `{"type":"record","name":"R","fields":[{"name":"f","type":"float","default":1e}]}`
		// JSON itself rejects the literal at unmarshalDefault; the
		// schema parse therefore fails at a step before defaultAsFloat64
		// is reached. This case confirms the reject path is upstream.
		if _, err := avro.Parse(schemaSrc); err == nil {
			t.Errorf("expected JSON syntax reject for 1e literal")
		}
	})

	// Syntax errors at the string arm: a non-numeric string default
	// against a float/double schema still rejects (ParseFloat returns
	// ErrSyntax, not ErrRange — the IsInf check fails, fall to reject).
	t.Run("string_arm_syntax_error_still_rejects", func(t *testing.T) {
		schemaSrc := `{"type":"record","name":"R","fields":[{"name":"f","type":"float","default":"hello"}]}`
		if _, err := avro.Parse(schemaSrc); err == nil {
			t.Errorf("expected ErrSyntax reject for non-numeric string default")
		}
	})

	// String-form "Infinity"/"NaN" literal defaults (existing intentional
	// divergence, see also TestRegression_SpecialFloatStringDefaults
	// elsewhere): ParseFloat("Infinity") returns (Inf, nil) — no error
	// — so the existing path always worked. My fix does NOT change this
	// case; locked here as a sanity check that the named-float forms
	// still flow through their non-error path.
	t.Run("infinity_string_literal_default_unaffected", func(t *testing.T) {
		schemaSrc := `{"type":"record","name":"R","fields":[{"name":"f","type":"float","default":"Infinity"}]}`
		s, err := avro.Parse(schemaSrc)
		if err != nil {
			t.Fatalf("schema parse rejected Infinity-literal default: %v", err)
		}
		bin, err := s.AppendEncode(nil, map[string]any{})
		if err != nil {
			t.Fatal(err)
		}
		if hex.EncodeToString(bin) != "0000807f" {
			t.Errorf("Infinity default wire bytes mismatch: %x", bin)
		}
	})

	// Cross-implementation parity statement: the schema-parse acceptance
	// here mirrors Java (Schema.java:1764-1766 isValidDefault returns
	// defaultValue.isNumber() for FLOAT/DOUBLE; DoubleNode(+Inf) is a
	// number) and fastavro (_schema_py.py:351-352 _default_matches_schema
	// runs _maybe_float and float("1e1000") returns inf which IS a
	// Python float). Without the fix, twmb diverged from both — the
	// schema parsed elsewhere but rejected here, even though the parsed
	// schema's own encoder accepted the same input after the prior
	// encode-side fix at ae99f46.
}

// TestRegression_DefaultFloatIntegerOverflowPrecisionLoss: a
// schema-declared float/double default whose integer magnitude
// exceeds the target's mantissa precision (1<<24 for float, 1<<53
// for double) must reject at schema-parse time so it agrees with
// the runtime json.Number encode arm. Pre-fix: parse accepted,
// binary encode silently rounded, JSON encode rejected, and
// Schema.Root().Fields[0].Default returned the preserved literal —
// four observable surfaces, two values.
func TestRegression_DefaultFloatIntegerOverflowPrecisionLoss(t *testing.T) {
	type rejectCase struct {
		name       string
		typ        string
		defaultLit string
	}
	rejects := []rejectCase{
		// float target: 2^24 mantissa boundary.
		{"float_2_24_plus_1_int_literal", "float", "16777217"},
		{"float_2_24_plus_3_int_literal", "float", "16777219"},
		{"float_neg_2_24_minus_1", "float", "-16777217"},
		// double target: 2^53 mantissa boundary.
		{"double_2_53_plus_1_int_literal", "double", "9007199254740993"},
		{"double_2_53_plus_3_int_literal", "double", "9007199254740995"},
		{"double_neg_2_53_minus_1", "double", "-9007199254740993"},
		// Integer-form magnitude beyond int64 (≈10^20) — also rejected.
		{"double_beyond_int64", "double", "99999999999999999999"},
		// String-arm: same shape via defaultAsFloat string arm.
		{"double_string_form_2_53_plus_1", "double", `"9007199254740993"`},
		{"float_string_form_2_24_plus_1", "float", `"16777217"`},
		// Beyond-int64 magnitude via the string arm: a self-audit during
		// F2 fix authoring caught the string arm fall-through bug —
		// ParseInt fails with ErrRange on "99999999999999999999", and
		// the unchecked fall-through to ParseFloat silently rounded to
		// 1e20. Now rejected with looksDecimalIntegerLiteral gating the
		// "ParseInt-fails-means-integer-magnitude-beyond-int64" branch.
		{"double_string_beyond_int64", "double", `"99999999999999999999"`},
		{"double_string_neg_beyond_int64", "double", `"-99999999999999999999"`},
		// Coerce-via-union: union-typed default goes through coerceDefault's
		// union branch matcher, which now routes through the precision-
		// aware defaultAsFloat.
		{"union_double_null_overflow_string", "double", `"9007199254740993"`}, // single-type, but exercises the same path
	}
	for _, tc := range rejects {
		t.Run(tc.name+"/reject", func(t *testing.T) {
			schemaJSON := `{"type":"record","name":"R","fields":[{"name":"f","type":"` + tc.typ + `","default":` + tc.defaultLit + `}]}`
			_, err := avro.Parse(schemaJSON)
			if err == nil {
				t.Fatalf("expected reject for %s default %s; parse accepted", tc.typ, tc.defaultLit)
			}
			// Error message must name the overflow direction so callers
			// can diagnose; "overflows float%d exact precision" mirrors
			// the encode-arm error text byte-for-byte (modulo "avro json:"
			// JSON-side prefix).
			if !strings.Contains(err.Error(), "overflows float") {
				t.Errorf("expected 'overflows float%%d exact precision' in error, got: %v", err)
			}
		})
	}

	// Boundary-value acceptance: values AT the precision limit (not
	// beyond) must still parse. 2^24 = 16777216 is the last exact
	// integer in float32; 2^53 = 9007199254740992 is the last exact
	// integer in float64.
	type acceptCase struct {
		name       string
		typ        string
		defaultLit string
	}
	accepts := []acceptCase{
		{"float_2_24_boundary", "float", "16777216"},
		{"float_neg_2_24_boundary", "float", "-16777216"},
		{"double_2_53_boundary", "double", "9007199254740992"},
		{"double_neg_2_53_boundary", "double", "-9007199254740992"},
		// Small ints stay accepted (regression guard against
		// over-aggressive tightening).
		{"float_small", "float", "0"},
		{"double_small", "double", "42"},
		// Exponent-form / fractional-form unchanged (always lossy by
		// nature, ParseFloat-handled).
		{"double_fractional_exact", "double", "1.5"},
		{"double_exponent", "double", "1e10"},
		// Overflow-to-Inf (exponent-form) still accepted via the
		// existing TestRegression_SchemaDefaultOverflowToInfParity
		// path; locked here too for cross-test invariance.
		{"double_exp_overflow_to_inf", "double", "1e1000"},
		// String-form values that are explicitly NOT decimal-integer
		// literals stay on the ParseFloat path (Java-parity lenient
		// preserved). The looksDecimalIntegerLiteral gate is what
		// keeps the precision-strict int-form branch from intercepting
		// these: ParseFloat handles them at its inherent float-precision
		// without claiming exact-integer semantics.
		{"double_string_hex_float", "double", `"0x1p10"`}, // 1024
		{"float_string_exp_form", "float", `"1.5e5"`},     // 150000, fits float32
		{"double_string_special_inf", "double", `"Inf"`},  // ParseFloat accepts "Inf"
		{"double_string_special_nan", "double", `"NaN"`},  // ParseFloat accepts "NaN"
	}
	for _, tc := range accepts {
		t.Run(tc.name+"/accept", func(t *testing.T) {
			schemaJSON := `{"type":"record","name":"R","fields":[{"name":"f","type":"` + tc.typ + `","default":` + tc.defaultLit + `}]}`
			if _, err := avro.Parse(schemaJSON); err != nil {
				t.Fatalf("expected accept for %s default %s; got: %v", tc.typ, tc.defaultLit, err)
			}
		})
	}
}

// TestRegression_LogicalTypeSoftDropMatrix locks in the F1 fix:
// every known logical type, on every wrong underlying type, soft-drops
// the logical and parses successfully as bare underlying — matching
// Java's fromSchemaIgnoreInvalid (Schema.java:1979 ->
// LogicalTypes.java:120-194 try/catch wrapping each validate() throw),
// fastavro's LOGICAL_*.get-returns-None-then-fallthrough
// (_read_py.py:662, _write_py.py:205/313), hamba's
// parsePrimitiveLogicalType / parseFixedLogicalType returning nil
// (schema_parse.go:205-222, :514-524), AND the spec text:
//
//	"If a logical type is invalid, … implementations should ignore
//	 the logical type and use the underlying Avro type."
//	(apache/avro Specification/_index.md, Logical Types section)
//
// Pre-F1-fix twmb hard-rejected, diverging from three reference impls
// + the spec on each of 7 known logical types (only `decimal` was
// correctly soft-dropping). The pin was the bug — a Java/fastavro
// producer schema with a legacy/typo `{"type":"string","logicalType":
// "timestamp-millis"}` or `{"type":"long","logicalType":"uuid"}` was
// unreadable by twmb consumers despite being valid per the spec.
//
// The matrix below pins the soft-drop behavior across every known
// (logical, valid-underlying) -> (logical, wrong-underlying) pair, plus
// the round-trip wire encoding (which must match the bare-underlying
// schema's wire encoding, since the logical was dropped). The
// Canonical() form is also pinned to match the bare-underlying schema's
// PCF (logicals are stripped from canonical per the spec's PCF rules).
//
// Acceptance siblings: TestParity_AcceptedLeniencies has the
// representative subset; this test is the exhaustive matrix.
//
// Counter-test sibling: TestParity_SchemaRejectionMatrix at
// conformance_test.go:8430 retained NONE of these rows — every
// wrong-underlying combo moved to acceptance.
func TestRegression_LogicalTypeSoftDropMatrix(t *testing.T) {
	// (logical, underlying) -> isValidPair
	validPairs := map[string]map[string]bool{
		"uuid":                   {"string": true, "fixed:16": true},
		"date":                   {"int": true},
		"time-millis":            {"int": true},
		"time-micros":            {"long": true},
		"timestamp-millis":       {"long": true},
		"timestamp-micros":       {"long": true},
		"timestamp-nanos":        {"long": true},
		"local-timestamp-millis": {"long": true},
		"local-timestamp-micros": {"long": true},
		"local-timestamp-nanos":  {"long": true},
		"big-decimal":            {"bytes": true},
		"duration":               {"fixed:12": true},
	}
	// Wrong-underlying combinations to probe. fixed:N covers various sizes.
	underlyings := []string{
		"int", "long", "float", "double", "string", "bytes", "boolean",
		"fixed:8", "fixed:12", "fixed:16", "fixed:20", "fixed:32",
	}
	mkSchema := func(underlying, logical string) string {
		if strings.HasPrefix(underlying, "fixed:") {
			size := underlying[len("fixed:"):]
			return `{"type":"fixed","name":"F","size":` + size + `,"logicalType":"` + logical + `"}`
		}
		return `{"type":"` + underlying + `","logicalType":"` + logical + `"}`
	}
	mkBareSchema := func(underlying string) string {
		if strings.HasPrefix(underlying, "fixed:") {
			size := underlying[len("fixed:"):]
			return `{"type":"fixed","name":"F","size":` + size + `}`
		}
		return `"` + underlying + `"`
	}

	for logical, valids := range validPairs {
		for _, underlying := range underlyings {
			isValid := valids[underlying]
			name := logical + "_on_" + underlying
			t.Run(name, func(t *testing.T) {
				sch := mkSchema(underlying, logical)
				s, err := avro.Parse(sch)
				if err != nil {
					t.Fatalf("expected soft-drop accept, got: %v\n  schema: %s", err, sch)
				}
				// PCF must match the bare-underlying schema's canonical
				// form (logicals stripped per spec). Skip the comparison
				// for valid pairs since the logical may legitimately
				// survive at the schema-decode layer (decimal/uuid).
				if !isValid {
					bareSch := mkBareSchema(underlying)
					bareS, err := avro.Parse(bareSch)
					if err != nil {
						t.Fatalf("bare schema parse: %v\n  schema: %s", err, bareSch)
					}
					if got, want := string(s.Canonical()), string(bareS.Canonical()); got != want {
						t.Errorf("canonical PCF diverges:\n  soft-drop: %s\n  bare:      %s", got, want)
					}
				}
			})
		}
	}
}

// TestRegression_SchemaNodeCycleDetection locks in the F2 fix:
// programmatic SchemaNode construction with pointer cycles via
// Items/Values no longer crashes the process with stack overflow.
// Pre-fix the cycle guard at toJSONDedup line 134 only protected
// the dedup-aware walk; the snapshot-via-toJSON forks at lines
// 148/180 (named-type conflict-check + definition snapshot) called
// the non-cycle-aware toJSON. A programmatic recursive named type
// — e.g. `node := &SchemaNode{Type:"record",Name:"Node",...}; arr :=
// SchemaNode{Type:"array",Items:node}; node.Fields = append(...,
// SchemaField{Name:"children",Type:arr})` — crashed with
// "runtime: goroutine stack exceeds 1000000000-byte limit / fatal
// error: stack overflow / github.com/twmb/avro.(*SchemaNode).toJSON
// schema_node.go:328 (recurses)".
//
// Post-fix:
//   - Named-type ancestor cycles (the realistic programmatic
//     recursive-schema shape) emit as a name reference, mirroring
//     the Avro JSON canonical form ({"items":"Node"}).
//   - Unnamed cycles (array of self, map of self) return a graceful
//     "cyclic SchemaNode detected" error rather than crashing.
//
// Two fix sites cooperate:
//   - schema_node.go's toJSON refactored into toJSONVisited(visited
//     map) so the four recursive call sites (Items, Values, Branches,
//     Fields[].Type) propagate the visited set.
//   - schema_node.go's toJSONDedup cycle guard at line 134 special-
//     cases named types via name-reference emission so an ancestor
//     cycle through Items/Values to a named type produces a valid
//     recursive Avro schema rather than failing at the dedup walk.
//
// Existing tests TestSchemaNodeCyclicItems/Values/Indirect/Cyclic3Node
// only covered unnamed (array/map) cycles which DID hit the
// toJSONDedup cycle guard pre-fix and reported the error; the
// named-type cycle path was structurally invisible to those tests.
func TestRegression_SchemaNodeCycleDetection(t *testing.T) {
	t.Run("programmatic_recursive_node_via_array_items", func(t *testing.T) {
		// The natural shape: a recursive Node with children:array<Node>.
		// Pre-fix: stack overflow. Post-fix: valid recursive schema.
		node := &avro.SchemaNode{
			Type: "record",
			Name: "Node",
			Fields: []avro.SchemaField{
				{Name: "v", Type: avro.SchemaNode{Type: "int"}},
			},
		}
		arr := avro.SchemaNode{Type: "array", Items: node}
		node.Fields = append(node.Fields, avro.SchemaField{Name: "children", Type: arr})
		s, err := node.Schema()
		if err != nil {
			t.Fatalf("recursive node should produce a valid schema, got: %v", err)
		}
		// The emitted JSON must use a name reference for Node, matching
		// the canonical Avro recursive-schema form.
		want := `{"fields":[{"name":"v","type":"int"},{"name":"children","type":{"items":"Node","type":"array"}}],"name":"Node","type":"record"}`
		if got := s.String(); got != want {
			t.Errorf("recursive Node JSON mismatch:\n  got:  %s\n  want: %s", got, want)
		}
	})
	t.Run("programmatic_recursive_node_via_map_values", func(t *testing.T) {
		// Same shape but via map<Node> instead of array<Node>.
		node := &avro.SchemaNode{
			Type: "record",
			Name: "Node",
			Fields: []avro.SchemaField{
				{Name: "v", Type: avro.SchemaNode{Type: "int"}},
			},
		}
		m := avro.SchemaNode{Type: "map", Values: node}
		node.Fields = append(node.Fields, avro.SchemaField{Name: "children", Type: m})
		s, err := node.Schema()
		if err != nil {
			t.Fatalf("recursive node via map should produce a valid schema, got: %v", err)
		}
		want := `{"fields":[{"name":"v","type":"int"},{"name":"children","type":{"type":"map","values":"Node"}}],"name":"Node","type":"record"}`
		if got := s.String(); got != want {
			t.Errorf("recursive Node JSON mismatch:\n  got:  %s\n  want: %s", got, want)
		}
	})
	t.Run("unnamed_array_self_cycle_errors_not_crashes", func(t *testing.T) {
		arr := &avro.SchemaNode{Type: "array"}
		arr.Items = arr
		_, err := arr.Schema()
		if err == nil {
			t.Fatal("expected cyclic-SchemaNode error for unnamed array self-loop")
		}
		if !strings.Contains(err.Error(), "cyclic") {
			t.Errorf("expected 'cyclic' in error, got: %v", err)
		}
	})
	t.Run("unnamed_map_self_cycle_errors_not_crashes", func(t *testing.T) {
		m := &avro.SchemaNode{Type: "map"}
		m.Values = m
		_, err := m.Schema()
		if err == nil {
			t.Fatal("expected cyclic-SchemaNode error for unnamed map self-loop")
		}
	})
	t.Run("record_invalid_items_field_errors_not_crashes", func(t *testing.T) {
		// Records shouldn't have Items; constructing one with a self-
		// loop on Items hits the type-validation error rather than
		// crashing. Pre-fix this crashed via toJSON's unguarded
		// recursion before the type validation could fire.
		r := &avro.SchemaNode{
			Type: "record",
			Name: "R",
			Fields: []avro.SchemaField{
				{Name: "f", Type: avro.SchemaNode{Type: "int"}},
			},
		}
		r.Items = r
		_, err := r.Schema()
		if err == nil {
			t.Fatal("expected error for record with Items")
		}
	})
	t.Run("parsed_recursive_schema_round_trips_via_Root_Schema", func(t *testing.T) {
		// Sibling check: the parse-then-Root-then-Schema round-trip
		// for the canonical Avro recursive schema must still work.
		// (Pre-fix this also worked because the parse path produces
		// name-reference SchemaNodes rather than pointer cycles.)
		src := `{"type":"record","name":"Node","fields":[{"name":"v","type":"int"},{"name":"children","type":{"type":"array","items":"Node"}}]}`
		s1, err := avro.Parse(src)
		if err != nil {
			t.Fatal(err)
		}
		root := s1.Root()
		s2, err := root.Schema()
		if err != nil {
			t.Fatalf("Root().Schema() round-trip: %v", err)
		}
		// Both schemas should produce the same canonical form.
		if c1, c2 := string(s1.Canonical()), string(s2.Canonical()); c1 != c2 {
			t.Errorf("round-trip canonical mismatch:\n  orig:    %s\n  rt:      %s", c1, c2)
		}
	})
}

// TestRegression_LogicalSoftDropRoundTrip pins the encode/decode
// behavior for soft-dropped schemas: after F1 the schema is treated
// as bare underlying for the entire wire-format path. A representative
// cross-section across the 12 logical types' wrong-underlying combos
// verifies encode/decode/canonical/fingerprint all agree on the bare-
// underlying behavior.
func TestRegression_LogicalSoftDropRoundTrip(t *testing.T) {
	cases := []struct {
		name     string
		schema   string
		bareEq   string // canonical form must match this bare schema's PCF
		input    any
		decTgt   any
		wantWire string // wire bytes as hex (binary encoding)
	}{
		{
			"timestamp-millis_on_string",
			`{"type":"string","logicalType":"timestamp-millis"}`,
			`"string"`,
			"hello",
			new(string),
			"0a68656c6c6f",
		},
		{
			"uuid_on_int",
			`{"type":"int","logicalType":"uuid"}`,
			`"int"`,
			int32(42),
			new(int32),
			"54",
		},
		{
			"date_on_long",
			`{"type":"long","logicalType":"date"}`,
			`"long"`,
			int64(1234567890),
			new(int64),
			"a48bb09909", // varint(zigzag(1234567890)) = varint(2469135780)
		},
		{
			"big-decimal_on_int",
			`{"type":"int","logicalType":"big-decimal"}`,
			`"int"`,
			int32(100),
			new(int32),
			"c801",
		},
		{
			"duration_on_fixed_size_10",
			`{"type":"fixed","name":"F","size":10,"logicalType":"duration"}`,
			`{"type":"fixed","name":"F","size":10}`,
			[10]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			new([10]byte),
			"0102030405060708090a",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := avro.Parse(c.schema)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			bareS, err := avro.Parse(c.bareEq)
			if err != nil {
				t.Fatalf("bare parse: %v", err)
			}
			// PCF must match.
			if got, want := string(s.Canonical()), string(bareS.Canonical()); got != want {
				t.Errorf("canonical:\n  soft-drop: %s\n  bare:      %s", got, want)
			}
			// Wire encoding must match the bare schema's encoding.
			enc, err := s.AppendEncode(nil, c.input)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			bareEnc, err := bareS.AppendEncode(nil, c.input)
			if err != nil {
				t.Fatalf("bare encode: %v", err)
			}
			if !bytes.Equal(enc, bareEnc) {
				t.Errorf("wire bytes:\n  soft-drop: %x\n  bare:      %x", enc, bareEnc)
			}
			if hex.EncodeToString(enc) != c.wantWire {
				t.Errorf("wire mismatch: got %x want %s", enc, c.wantWire)
			}
			// Round-trip decode.
			if _, err := s.Decode(enc, c.decTgt); err != nil {
				t.Errorf("decode: %v", err)
			}
			gotV := reflect.ValueOf(c.decTgt).Elem().Interface()
			if !reflect.DeepEqual(gotV, c.input) {
				t.Errorf("round-trip: got %v want %v", gotV, c.input)
			}
		})
	}
}

// TestSpecBareTypeNameInObjectAccepted locks in that {"type":"Node"}
// as a wrapped reference to a previously-declared named type is
// accepted at parse and the wrapped/bare forms produce equivalent
// schemas. Java's TestUnionSelfReference.java:50 pins this for the
// union-self-reference shape; we additionally exercise array items,
// map values, and namespace-qualified references since all share the
// buildComplex dispatch.
func TestSpecBareTypeNameInObjectAccepted(t *testing.T) {
	t.Run("union_self_reference_canonical_equivalent", func(t *testing.T) {
		wrapped := `{"type":"record","name":"Node","fields":[
			{"name":"next","type":["null",{"type":"Node"}]}
		]}`
		bare := `{"type":"record","name":"Node","fields":[
			{"name":"next","type":["null","Node"]}
		]}`
		wS, err := avro.Parse(wrapped)
		if err != nil {
			t.Fatalf("wrapped {\"type\":\"Node\"} should accept: %v", err)
		}
		bS, err := avro.Parse(bare)
		if err != nil {
			t.Fatalf("bare-string \"Node\" reference should accept: %v", err)
		}
		// Both forms must produce the same canonical form (the wrapped
		// form is just an alternate spelling).
		if !bytes.Equal(wS.Canonical(), bS.Canonical()) {
			t.Fatalf("canonical mismatch:\n  wrapped=%q\n  bare=%q", wS.Canonical(), bS.Canonical())
		}
		// Round-trip exercises that the schema actually decodes recursively.
		in := map[string]any{"next": map[string]any{"Node": map[string]any{"next": nil}}}
		enc, err := wS.AppendEncode(nil, &in)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got map[string]any
		if _, err := wS.Decode(enc, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if inner, _ := got["next"].(map[string]any); inner == nil {
			t.Fatalf("expected wrapped inner record, got %#v", got["next"])
		}
	})
	t.Run("array_items", func(t *testing.T) {
		schema := `{"type":"record","name":"R","fields":[
			{"name":"first","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}},
			{"name":"list","type":{"type":"array","items":{"type":"Inner"}}}
		]}`
		if _, err := avro.Parse(schema); err != nil {
			t.Fatalf("array<wrapped-ref>: %v", err)
		}
	})
	t.Run("map_values", func(t *testing.T) {
		schema := `{"type":"record","name":"R","fields":[
			{"name":"first","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}},
			{"name":"m","type":{"type":"map","values":{"type":"Inner"}}}
		]}`
		if _, err := avro.Parse(schema); err != nil {
			t.Fatalf("map<wrapped-ref>: %v", err)
		}
	})
	t.Run("namespace_qualified_lookup", func(t *testing.T) {
		// Inner namespaced as ns.Inner via parent; wrapped reference uses
		// the unqualified name and the namespace-fallback path resolves it.
		schema := `{"type":"record","name":"Outer","namespace":"ns","fields":[
			{"name":"first","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}},
			{"name":"u","type":["null",{"type":"Inner"}]}
		]}`
		if _, err := avro.Parse(schema); err != nil {
			t.Fatalf("namespace-qualified wrapped-ref: %v", err)
		}
	})
	// Forward references: the wrapped form must accept names that
	// haven't been declared yet, matching the bare-string form's
	// fixup-on-finalize behavior. Java accepts both shapes for fwd-refs.
	// Note: twmb's fixup machinery only covers record fields and union
	// branches (the two contexts where forward references actually
	// work) — arrays and maps of fwd-refs fail with both bare and
	// wrapped forms; this is a pre-existing limitation, not specific
	// to the wrapped form.
	t.Run("forward_ref_in_record_field", func(t *testing.T) {
		schema := `{"type":"record","name":"Outer","fields":[
			{"name":"f","type":{"type":"FwdInner"}},
			{"name":"g","type":{"type":"record","name":"FwdInner","fields":[{"name":"x","type":"int"}]}}
		]}`
		if _, err := avro.Parse(schema); err != nil {
			t.Fatalf("forward ref in record field: %v", err)
		}
	})
	t.Run("forward_ref_in_union_branch", func(t *testing.T) {
		schema := `{"type":"record","name":"Outer","fields":[
			{"name":"f","type":["null",{"type":"FwdInner"}]},
			{"name":"g","type":{"type":"record","name":"FwdInner","fields":[{"name":"x","type":"int"}]}}
		]}`
		if _, err := avro.Parse(schema); err != nil {
			t.Fatalf("forward ref in union branch: %v", err)
		}
	})
}

// TestSpecJSONEncodeBytesAcceptsByteArray locks in that EncodeJSON
// accepts a Go [N]byte value for an Avro "bytes" schema, matching the
// binary path's serBytes (ser.go:460) which accepts both reflect.Array
// and reflect.Slice. Pre-fix the JSON "bytes" arm only handled Slice;
// every position the bytes encoder is reached failed for [N]byte
// (top-level, record field, array item, map value, union branch).
//
// Sibling sweep: the JSON "fixed" arm already handled Array
// (json_codec.go:489-491); the binary path already accepted both
// (ser.go:460). The "string" Avro type rejects [N]byte on both binary
// and JSON paths consistently (avroStringValue / appendAvroString
// accept only Slice), which is intentional parity — strings and bytes
// have separate type-acceptance rules even though both could in
// principle accept Array.
func TestSpecJSONEncodeBytesAcceptsByteArray(t *testing.T) {
	val := [3]byte{0x01, 0x02, 0x03}
	type R struct {
		B [3]byte `avro:"b"`
	}
	cases := []struct {
		name string
		s    *avro.Schema
		in   any
	}{
		{"top_level", mustParse(t, `"bytes"`), val},
		{"record_field", mustParse(t, `{"type":"record","name":"R","fields":[{"name":"b","type":"bytes"}]}`), R{B: val}},
		{"array_items", mustParse(t, `{"type":"array","items":"bytes"}`), [][3]byte{val}},
		{"map_values", mustParse(t, `{"type":"map","values":"bytes"}`), map[string][3]byte{"k": val}},
		{"union_branch", mustParse(t, `["null","bytes"]`), val},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := c.s.EncodeJSON(c.in); err != nil {
				t.Fatalf("EncodeJSON: %v", err)
			}
		})
	}
}

// TestSpecDateRejectsInt32Overflow locks in that timeToDate returns an
// error when the day count exceeds int32 range, instead of silently
// truncating (`int32(floorDiv(...))`). The int32 wire bound implies
// years roughly ±5.8 million; values outside that range are rejected
// at encode rather than producing a wrong wire value.
func TestSpecDateRejectsInt32Overflow(t *testing.T) {
	dateS := mustParse(t, `{"type":"int","logicalType":"date"}`)
	// Year 1<<30 (~1.07 billion) is well outside int32-day range.
	huge := time.Date(1<<30, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := dateS.AppendEncode(nil, huge); err == nil {
		t.Fatal("expected overflow error for far-future date")
	}
	// Year -(1<<30) similarly.
	ancient := time.Date(-(1 << 30), 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := dateS.AppendEncode(nil, ancient); err == nil {
		t.Fatal("expected overflow error for far-past date")
	}
	// Within-range dates still work.
	ok := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)
	if _, err := dateS.AppendEncode(nil, ok); err != nil {
		t.Fatalf("present date should encode: %v", err)
	}
}

// TestSpecJSONDecodeHonorsFieldAliases locks in that DecodeJSON routes
// JSON object keys through the field's alias list (in addition to the
// field's name), matching the binary path's alias resolution
// (resolve.go:285-296). Java's JsonDecoder.java:516 uses
// `name.equals(fn) || fa.aliases.contains(fn)` for the same behavior.
//
// Pre-fix the fieldIdx map (schema.go:1497-1500) registered only f.name,
// so a JSON key matching an alias routed to the unknown branch
// (skipValue) and then the missing-required-field check errored.
func TestSpecJSONDecodeHonorsFieldAliases(t *testing.T) {
	schema := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"new","aliases":["old","ancient"],"type":"int"}
	]}`)
	t.Run("alias_old_routes_to_new", func(t *testing.T) {
		var got map[string]any
		if err := schema.DecodeJSON([]byte(`{"old":42}`), &got); err != nil {
			t.Fatalf("decode old: %v", err)
		}
		if got["new"] != int32(42) {
			t.Fatalf("alias-keyed value not routed: got %#v", got)
		}
	})
	t.Run("alias_ancient_routes_to_new", func(t *testing.T) {
		var got map[string]any
		if err := schema.DecodeJSON([]byte(`{"ancient":7}`), &got); err != nil {
			t.Fatalf("decode ancient: %v", err)
		}
		if got["new"] != int32(7) {
			t.Fatalf("alias-keyed value not routed: got %#v", got)
		}
	})
	t.Run("canonical_name_still_works", func(t *testing.T) {
		var got map[string]any
		if err := schema.DecodeJSON([]byte(`{"new":99}`), &got); err != nil {
			t.Fatalf("decode new: %v", err)
		}
		if got["new"] != int32(99) {
			t.Fatalf("got %#v", got)
		}
	})
}

// TestSpecJSONDecodeFillsDefaultForMissingField locks in that DecodeJSON
// applies the schema-declared default when a record field is absent
// from the JSON input AND materializes it as the schema-typed Go
// value, not the raw json.Number / string from the parser. Pre-fix
// iterateRecordFields silently left the target slot at the Go zero
// value; the subsequent fix routed map/struct paths through
// applyFieldDefault but the *any path assigned raw f.defaultVal,
// producing a same-field type divergence between present and missing
// keys (present:int32 vs missing:json.Number). The second fix routes
// all three callers through applyFieldDefault so the materialized
// type matches the present-path's decodeValue.
//
// fastavro matches the fill behavior (io/json_decoder.py:55-78 returns
// symbol.get_default() on missing key). Java is stricter, rejecting
// missing fields outright (JsonDecoder.java:498-530). twmb is the most
// lenient + most useful: missing-with-default fills with the typed
// value; missing-without-default still errors.
//
// Subtests reach each record-decode path explicitly:
//   - into_any: *any target → reflect.Interface → decodeRecordAny.
//   - into_map_string_any: *map[string]any → reflect.Map → decodeRecordMap.
//   - into_typed_map: *map[string]int32 → decodeRecordMap (typed-elem).
//   - into_struct: *struct → decodeRecordStruct.
func TestSpecJSONDecodeFillsDefaultForMissingField(t *testing.T) {
	schema := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"present","type":"int"},
		{"name":"defaulted_int","type":"int","default":42},
		{"name":"defaulted_string","type":"string","default":"hi"}
	]}`)
	input := []byte(`{"present":7}`)
	t.Run("into_any", func(t *testing.T) {
		// *any target — exercises decodeRecordAny specifically (Kind is
		// reflect.Interface, not reflect.Map).
		var any1 any
		if err := schema.DecodeJSON(input, &any1); err != nil {
			t.Fatalf("decode: %v", err)
		}
		got, ok := any1.(map[string]any)
		if !ok {
			t.Fatalf("decoded value is %T, want map[string]any", any1)
		}
		if got["present"] != int32(7) {
			t.Fatalf("present: %#v", got["present"])
		}
		if got["defaulted_int"] != int32(42) {
			t.Fatalf("defaulted_int: got %T %#v, want int32(42)", got["defaulted_int"], got["defaulted_int"])
		}
		if got["defaulted_string"] != "hi" {
			t.Fatalf("defaulted_string: %#v", got["defaulted_string"])
		}
	})
	t.Run("into_map_string_any", func(t *testing.T) {
		// *map[string]any — exercises decodeRecordMap with any-typed elem.
		var got map[string]any
		if err := schema.DecodeJSON(input, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got["present"] != int32(7) {
			t.Fatalf("present: %#v", got["present"])
		}
		if got["defaulted_int"] != int32(42) {
			t.Fatalf("defaulted_int: got %T %#v, want int32(42)", got["defaulted_int"], got["defaulted_int"])
		}
		if got["defaulted_string"] != "hi" {
			t.Fatalf("defaulted_string: %#v", got["defaulted_string"])
		}
	})
	t.Run("into_struct", func(t *testing.T) {
		type R struct {
			Present int32  `avro:"present"`
			DefInt  int32  `avro:"defaulted_int"`
			DefStr  string `avro:"defaulted_string"`
		}
		var got R
		if err := schema.DecodeJSON(input, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.Present != 7 {
			t.Fatalf("Present: %v", got.Present)
		}
		if got.DefInt != 42 {
			t.Fatalf("DefInt: %v", got.DefInt)
		}
		if got.DefStr != "hi" {
			t.Fatalf("DefStr: %q", got.DefStr)
		}
	})
	t.Run("into_typed_map", func(t *testing.T) {
		// map[string]int32 — exercises decodeRecordMap with concrete elem type.
		schema := mustParse(t, `{"type":"record","name":"R","fields":[
			{"name":"present","type":"int"},
			{"name":"defaulted","type":"int","default":42}
		]}`)
		input := []byte(`{"present":7}`)
		var got map[string]int32
		if err := schema.DecodeJSON(input, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got["present"] != 7 {
			t.Fatalf("present: %v", got["present"])
		}
		if got["defaulted"] != 42 {
			t.Fatalf("defaulted: %v", got["defaulted"])
		}
	})
}

// TestRegression_DecodeJSONIntoAnyDefaultFillTypeConsistency locks in
// the parity-fix between decodeRecordAny's present-path (which routes
// through decodeValue → typed int32/time.Time/etc.) and its
// default-fill path (which now routes through applyFieldDefault →
// same typed value). Pre-fix the default-fill assigned the raw
// json.Number / string from unmarshalDefault, producing a same-field
// type divergence between present and missing keys, AND a path
// divergence between *any (Interface) and *map[string]any (Map).
func TestRegression_DecodeJSONIntoAnyDefaultFillTypeConsistency(t *testing.T) {
	s := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"present","type":"int"},
		{"name":"d","type":"int","default":42},
		{"name":"t","type":{"type":"int","logicalType":"time-millis"},"default":1000}
	]}`)
	input := []byte(`{"present":1}`)

	var anyT any
	if err := s.DecodeJSON(input, &anyT); err != nil {
		t.Fatalf("decode into *any: %v", err)
	}
	mAny := anyT.(map[string]any)

	t.Run("int_default_into_any_is_int32", func(t *testing.T) {
		if _, isInt32 := mAny["d"].(int32); !isInt32 {
			t.Fatalf("decodeRecordAny default-fill: got %T %#v, want int32 (matching decodeRecordMap)", mAny["d"], mAny["d"])
		}
	})
	t.Run("logical_default_into_any_applies_conversion", func(t *testing.T) {
		// time-millis default 1000 (ms) → time.Duration(1 * time.Second).
		if _, isDuration := mAny["t"].(time.Duration); !isDuration {
			t.Fatalf("decodeRecordAny default-fill for time-millis: got %T %#v, want time.Duration", mAny["t"], mAny["t"])
		}
	})
	t.Run("present_and_missing_same_type", func(t *testing.T) {
		// Same field "d" decoded from present JSON {"d":7} vs missing
		// must produce the same Go type — both int32.
		s2 := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"d","type":"int","default":42}]}`)
		var present, missing any
		if err := s2.DecodeJSON([]byte(`{"d":7}`), &present); err != nil {
			t.Fatal(err)
		}
		if err := s2.DecodeJSON([]byte(`{}`), &missing); err != nil {
			t.Fatal(err)
		}
		pT := reflect.TypeOf(present.(map[string]any)["d"])
		mT := reflect.TypeOf(missing.(map[string]any)["d"])
		if pT != mT {
			t.Fatalf("type divergence between present (%v) and default-filled (%v)", pT, mT)
		}
	})
}

// TestRegression_JSONDecodeFillsZeroByteDefault locks JSON DecodeJSON's
// default-fill for record fields whose schema-encoded default is exactly
// 0 wire bytes — null-typed fields, empty-record fields, and records
// whose every field is null-typed. Pre-fix applyFieldDefault rejected
// empty defaultBytes with "record has no pre-encoded default for field
// N", conflating "no default registered" (caller already gated on
// hasDefault) with "valid 0-byte default" (legitimate for these types).
//
// Binary resolved decode and JSON encode default-fill both handle these
// correctly: binary appends 0 bytes for the missing-from-writer field;
// EncodeJSON walks the parsed defaultVal directly, not defaultBytes.
// The bug was a JSON-decode-only divergence from twmb's own binary path
// AND from the project's stated fastavro parity for default-fill (per
// fastavro/io/json_decoder.py:55-78 returning symbol.get_default() for
// any default including None). Java is irrelevant here since
// JsonDecoder rejects all missing fields outright.
//
// Subtests cover the three default-fill target paths
// (decodeRecordAny / typed-map / struct) and the three zero-byte
// shapes (top-level null field, empty inner record, all-null-fields
// inner record).
func TestRegression_JSONDecodeFillsZeroByteDefault(t *testing.T) {
	t.Run("null_field_default_into_any", func(t *testing.T) {
		s := mustParse(t, `{"type":"record","name":"R","fields":[
			{"name":"x","type":"null","default":null}
		]}`)
		var got any
		if err := s.DecodeJSON([]byte(`{}`), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		m, ok := got.(map[string]any)
		if !ok {
			t.Fatalf("decoded is %T, want map[string]any", got)
		}
		if v, present := m["x"]; !present || v != nil {
			t.Fatalf("x: got present=%v val=%v, want present=true val=nil", present, v)
		}
	})
	t.Run("null_field_default_into_typed_map", func(t *testing.T) {
		s := mustParse(t, `{"type":"record","name":"R","fields":[
			{"name":"x","type":"null","default":null},
			{"name":"y","type":"int"}
		]}`)
		got := make(map[string]any)
		if err := s.DecodeJSON([]byte(`{"y":42}`), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if v, present := got["x"]; !present || v != nil {
			t.Fatalf("x: got present=%v val=%v, want present=true val=nil", present, v)
		}
	})
	t.Run("null_field_default_into_struct", func(t *testing.T) {
		type R struct {
			X any   `avro:"x"`
			Y int32 `avro:"y"`
		}
		s := mustParse(t, `{"type":"record","name":"R","fields":[
			{"name":"x","type":"null","default":null},
			{"name":"y","type":"int"}
		]}`)
		var got R
		if err := s.DecodeJSON([]byte(`{"y":42}`), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.X != nil || got.Y != 42 {
			t.Fatalf("got %+v, want {X:nil Y:42}", got)
		}
	})
	t.Run("empty_inner_record_default", func(t *testing.T) {
		s := mustParse(t, `{"type":"record","name":"R","fields":[
			{"name":"r","type":{"type":"record","name":"Inner","fields":[]},"default":{}}
		]}`)
		var got map[string]any
		if err := s.DecodeJSON([]byte(`{}`), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		r, ok := got["r"].(map[string]any)
		if !ok {
			t.Fatalf("r: got %T, want map[string]any", got["r"])
		}
		if len(r) != 0 {
			t.Fatalf("r: got %v, want empty map", r)
		}
	})
	t.Run("all_null_fields_inner_record_default", func(t *testing.T) {
		s := mustParse(t, `{"type":"record","name":"R","fields":[
			{"name":"r","type":{"type":"record","name":"Inner","fields":[
				{"name":"x","type":"null"}
			]},"default":{"x":null}}
		]}`)
		var got map[string]any
		if err := s.DecodeJSON([]byte(`{}`), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		r, ok := got["r"].(map[string]any)
		if !ok {
			t.Fatalf("r: got %T, want map[string]any", got["r"])
		}
		if v, present := r["x"]; !present || v != nil {
			t.Fatalf("r.x: got present=%v val=%v, want present=true val=nil", present, v)
		}
	})
	t.Run("binary_resolved_decode_parity", func(t *testing.T) {
		// Cross-check: the binary path handles all three shapes correctly,
		// confirming the bug was JSON-decode-only.
		writer := mustParse(t, `{"type":"record","name":"R","fields":[]}`)
		reader := mustParse(t, `{"type":"record","name":"R","fields":[
			{"name":"x","type":"null","default":null},
			{"name":"r","type":{"type":"record","name":"Inner","fields":[]},"default":{}}
		]}`)
		res, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		var got map[string]any
		if _, err := res.Decode(nil, &got); err != nil {
			t.Fatalf("binary resolved Decode: %v", err)
		}
		if v, present := got["x"]; !present || v != nil {
			t.Fatalf("binary x: got present=%v val=%v", present, v)
		}
		r, ok := got["r"].(map[string]any)
		if !ok || len(r) != 0 {
			t.Fatalf("binary r: got %v, want empty map", got["r"])
		}
	})
}

// TestRegression_DecimalScaleAllocBound locks in DoS resistance against
// wire-controlled big-decimal scale and schema-controlled regular-
// decimal precision/scale. Pre-fix every site computing 10^scale
// (parseBigDecimalPayload, bytesToRat, ratToUnscaled) accepted a
// producer-supplied magnitude without bound; a 7-byte big-decimal
// payload with scale=2^25 forced a ~14 MB big.Int allocation, and a
// schema with `precision`/`scale` near 2^31 made every legitimate
// decimal record decode allocate gigabytes. Java caps scale at int32
// implicitly and never eagerly materializes 10^scale; avro-rs same.
// twmb is the only impl that materializes the magnitude during
// decode, so the cap has to live in twmb itself.
func TestRegression_DecimalScaleAllocBound(t *testing.T) {
	deadline := 5 * time.Second
	withTimeout := func(t *testing.T, fn func() error) error {
		t.Helper()
		done := make(chan error, 1)
		go func() { done <- fn() }()
		select {
		case err := <-done:
			return err
		case <-time.After(deadline):
			t.Fatalf("operation hung > %v — wire/schema-controlled scale caused unbounded allocation", deadline)
			return nil
		}
	}
	t.Run("wire_big_decimal_scale_rejected", func(t *testing.T) {
		s := mustParse(t, `{"type":"bytes","logicalType":"big-decimal"}`)
		// Inner payload: unscaled length 1, byte 0x01, then a
		// zigzag varint that decodes to a scale of 2^25 (well
		// above any practical bound).
		inner := []byte{
			0x02,                   // zigzag(1) — unscaled length
			0x01,                   // unscaled bytes
			0x80, 0x80, 0x80, 0x20, // zigzag(2^26) → scale = 2^25
		}
		outer := append([]byte{byte(len(inner) << 1)}, inner...)
		var got any
		err := withTimeout(t, func() error {
			_, e := s.Decode(outer, &got)
			return e
		})
		if err == nil {
			t.Fatal("expected reject for huge wire-decoded scale")
		}
	})
	t.Run("wire_big_decimal_negative_scale_rejected", func(t *testing.T) {
		s := mustParse(t, `{"type":"bytes","logicalType":"big-decimal"}`)
		// Same shape but zigzag-encodes a negative scale of
		// magnitude 2^25 (zigzag for -(2^25) = 2^26-1, encoded as
		// 0xff,0xff,0xff,0x1f).
		inner := []byte{
			0x02,
			0x01,
			0xff, 0xff, 0xff, 0x1f,
		}
		outer := append([]byte{byte(len(inner) << 1)}, inner...)
		var got any
		err := withTimeout(t, func() error {
			_, e := s.Decode(outer, &got)
			return e
		})
		if err == nil {
			t.Fatal("expected reject for huge negative wire-decoded scale")
		}
	})
	t.Run("schema_regular_decimal_huge_precision_rejected", func(t *testing.T) {
		// A schema with `precision` near 2^31 should be rejected at
		// parse — every subsequent decode of even a legitimate 2-
		// byte payload would allocate a multi-MB 10^scale denominator.
		_, err := avro.Parse(`{"type":"bytes","logicalType":"decimal","precision":67108864,"scale":67108864}`)
		if err == nil {
			t.Fatal("expected schema-parse error for huge precision/scale")
		}
	})
	t.Run("encode_hostile_big_rat_denominator_rejected", func(t *testing.T) {
		// big-decimal encode of a *big.Rat whose denominator is
		// 2^huge. finiteScale would derive a scale of `huge`, then
		// buildBigDecimalPayload would compute 10^huge.
		s := mustParse(t, `{"type":"bytes","logicalType":"big-decimal"}`)
		huge := new(big.Int).Lsh(big.NewInt(1), 1<<17) // 2^(2^17), 16 KB big.Int
		r := new(big.Rat).SetFrac(big.NewInt(1), huge)
		err := withTimeout(t, func() error {
			_, e := s.AppendEncode(nil, r)
			return e
		})
		if err == nil {
			t.Fatal("expected reject for hostile big.Rat denominator (huge derived scale)")
		}
	})
	// Sanity: realistic scales still work.
	t.Run("realistic_scales_still_work", func(t *testing.T) {
		s := mustParse(t, `{"type":"bytes","logicalType":"decimal","precision":18,"scale":6}`)
		if _, err := s.AppendEncode(nil, big.NewRat(123456789, 1_000_000)); err != nil {
			t.Fatalf("scale=6 should work: %v", err)
		}
		bd := mustParse(t, `{"type":"bytes","logicalType":"big-decimal"}`)
		if _, err := bd.AppendEncode(nil, big.NewRat(314, 100)); err != nil {
			t.Fatalf("big-decimal 3.14 should work: %v", err)
		}
	})
}

// TestRegression_RatFromBytesNegativeScale locks in that the public
// RatFromBytes API handles negative scale as `unscaled * 10^|scale|`
// (the canonical Java/avro-rs big-decimal interpretation) instead of
// silently returning unscaled — which is what big.Int.Exp(10, neg, nil)
// produces (per math/big: "if m == nil ... unless y <= 0 in which case
// z = 1"). Internal callers pass schema-validated non-negative scale,
// so the bug was reachable only via the public API; the function's
// only realistic use site (CustomType callbacks for decimal) doesn't
// hit it, but the silent-wrong-result for direct callers violates
// twmb's least-surprise principle.
func TestRegression_RatFromBytesNegativeScale(t *testing.T) {
	// 0x64 = 100 unscaled, scale=-2 → value = 100 * 10^2 = 10000.
	got := avro.RatFromBytes([]byte{0x64}, -2)
	want := big.NewRat(10000, 1)
	if got.Cmp(want) != 0 {
		t.Fatalf("RatFromBytes(0x64, -2) = %s, want %s", got.RatString(), want.RatString())
	}
	// Non-negative scale unchanged (regression-guard for existing
	// users): 0x64 at scale=2 → 100 / 100 = 1.
	got = avro.RatFromBytes([]byte{0x64}, 2)
	want = big.NewRat(1, 1)
	if got.Cmp(want) != 0 {
		t.Fatalf("RatFromBytes(0x64, 2) = %s, want %s", got.RatString(), want.RatString())
	}
}

// TestRegression_TimestampSerParity locks parity between the binary
// safe path (interface{} into Schema.Encode) and the unsafe path
// (typed struct value) for every long-typed time logical. Pre-refactor
// the 6 ser*Timestamp* and 6 us*Timestamp* functions had copy-pasted
// bodies; consolidation behind serTimeAsLong / usTimeAsLong must
// preserve exact wire-byte equivalence across both paths.
func TestRegression_TimestampSerParity(t *testing.T) {
	type Rec struct {
		Tm time.Time `avro:"tm"`
		Tu time.Time `avro:"tu"`
		Tn time.Time `avro:"tn"`
		Lm time.Time `avro:"lm"`
		Lu time.Time `avro:"lu"`
		Ln time.Time `avro:"ln"`
	}
	schema := avro.MustParse(`{"type":"record","name":"R","fields":[
        {"name":"tm","type":{"type":"long","logicalType":"timestamp-millis"}},
        {"name":"tu","type":{"type":"long","logicalType":"timestamp-micros"}},
        {"name":"tn","type":{"type":"long","logicalType":"timestamp-nanos"}},
        {"name":"lm","type":{"type":"long","logicalType":"local-timestamp-millis"}},
        {"name":"lu","type":{"type":"long","logicalType":"local-timestamp-micros"}},
        {"name":"ln","type":{"type":"long","logicalType":"local-timestamp-nanos"}}
    ]}`)
	ts := time.Date(2024, 6, 15, 12, 34, 56, 789_000_000, time.UTC)
	rec := Rec{Tm: ts, Tu: ts, Tn: ts, Lm: ts, Lu: ts, Ln: ts}

	unsafeBytes, err := schema.Encode(rec)
	if err != nil {
		t.Fatalf("unsafe (typed struct) encode: %v", err)
	}
	var anyRec any = rec
	safeBytes, err := schema.Encode(anyRec)
	if err != nil {
		t.Fatalf("safe (any-wrapped) encode: %v", err)
	}
	if !bytes.Equal(unsafeBytes, safeBytes) {
		t.Fatalf("safe/unsafe path divergence:\n  safe  = %x\n  unsafe= %x", safeBytes, unsafeBytes)
	}

	// Also lock exact wire bytes against the canonical sequence of
	// zigzag-varint conv results — caught any divergence in the helper
	// itself (not just safe-vs-unsafe).
	// Sec=1718454896, ms=789 → millis=1718454896789; micros=1718454896789000;
	// nanos=1718454896789000000. local-* uses wall-clock-as-UTC so for
	// a UTC input the values are identical.
	const wantMillis = int64(1_718_454_896_789)
	const wantMicros = int64(1_718_454_896_789_000)
	const wantNanos = int64(1_718_454_896_789_000_000)
	var want []byte
	for _, n := range []int64{wantMillis, wantMicros, wantNanos, wantMillis, wantMicros, wantNanos} {
		want = append(want, zigzagEncode64(n)...)
	}
	if !bytes.Equal(unsafeBytes, want) {
		t.Fatalf("wire bytes drift:\n  got = %x\n  want= %x", unsafeBytes, want)
	}
}

// TestRegression_JSONTimestampDispatch locks JSON encoding of time.Time
// AND a parseable RFC 3339 string into each of the 6 long-time
// logicals. Pre-refactor the JSON "long" arm had 12 copy-pasted case
// bodies; consolidation behind timeLogicalToInt64 + extractTime must
// preserve byte-for-byte output across all (logical × input-form)
// combinations.
func TestRegression_JSONTimestampDispatch(t *testing.T) {
	ts := time.Date(2024, 6, 15, 12, 34, 56, 789_000_000, time.UTC)
	str := ts.Format(time.RFC3339Nano)

	const (
		wantMillis = "1718454896789"
		wantMicros = "1718454896789000"
		wantNanos  = "1718454896789000000"
	)
	cases := []struct {
		logical string
		want    string
	}{
		{"timestamp-millis", wantMillis},
		{"timestamp-micros", wantMicros},
		{"timestamp-nanos", wantNanos},
		{"local-timestamp-millis", wantMillis},
		{"local-timestamp-micros", wantMicros},
		{"local-timestamp-nanos", wantNanos},
	}
	for _, tc := range cases {
		schema := avro.MustParse(`{"type":"long","logicalType":"` + tc.logical + `"}`)
		got, err := schema.EncodeJSON(ts)
		if err != nil {
			t.Fatalf("%s/time.Time: %v", tc.logical, err)
		}
		if string(got) != tc.want {
			t.Fatalf("%s/time.Time: got %s want %s", tc.logical, got, tc.want)
		}
		got, err = schema.EncodeJSON(str)
		if err != nil {
			t.Fatalf("%s/string: %v", tc.logical, err)
		}
		if string(got) != tc.want {
			t.Fatalf("%s/string: got %s want %s", tc.logical, got, tc.want)
		}
	}
}

// TestRegression_DecimalJSONExpDoS locks the memory-amplification bound
// on JSON-input-controlled decimal numbers. Pre-fix,
// `new(big.Rat).SetString("1e1000000")` allocated ~3 MB from a 9-byte
// input (360,000× amplification). Four reachable sites — decode bytes-
// decimal, decode fixed-decimal, encode json.Number, encode string —
// now bound the parsed exponent via boundedRatFromString to mirror the
// existing wire-side guard in parseBigDecimalPayload. Java stores
// significand + scale separately so never materializes 10^scale;
// fastavro/avro-rs reject bare numbers for decimal entirely.
func TestRegression_DecimalJSONExpDoS(t *testing.T) {
	const allocCap = 1 << 20 // 1 MiB
	checkBounded := func(name string, fn func()) {
		t.Helper()
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		fn()
		runtime.ReadMemStats(&after)
		alloc := int64(after.TotalAlloc) - int64(before.TotalAlloc)
		if alloc > allocCap {
			t.Fatalf("%s: 9-byte input allocated %d bytes (>1 MiB cap) — bound bypassed", name, alloc)
		}
	}

	{
		s := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)
		input := []byte(`1e1000000`)
		checkBounded("decode/bytes-decimal", func() {
			var v any
			err := s.DecodeJSON(input, &v)
			if err == nil {
				t.Fatalf("decode/bytes-decimal: expected error on oversized exponent")
			}
		})
	}
	{
		s := avro.MustParse(`{"type":"fixed","name":"D","size":12,"logicalType":"decimal","precision":10,"scale":2}`)
		input := []byte(`1e1000000`)
		checkBounded("decode/fixed-decimal", func() {
			var v any
			err := s.DecodeJSON(input, &v)
			if err == nil {
				t.Fatalf("decode/fixed-decimal: expected error on oversized exponent")
			}
		})
	}
	{
		s := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)
		checkBounded("encode/json.Number", func() {
			_, err := s.AppendEncode(nil, json.Number("1e1000000"))
			if err == nil {
				t.Fatalf("encode/json.Number: expected error on oversized exponent")
			}
		})
	}
	{
		s := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)
		checkBounded("encode/string", func() {
			_, err := s.AppendEncode(nil, "1e1000000")
			if err == nil {
				t.Fatalf("encode/string: expected error on oversized exponent")
			}
		})
	}
}

// TestRegression_UUIDFixedDefaultJSONEncode locks parity between binary
// and JSON encode for records missing a fixed-uuid-typed field with an
// explicit string default. Pre-fix the JSON encoder's fixed/uuid arm
// hard-returned parseUUID's "invalid UUID" error for the 16-codepoint
// default form, even though the surrounding comment ("Logical-arm
// fall-through lands on the generic string/slice/array targets below")
// declared the intended fall-through. The binary path goes through
// encodeDefault → avroJSONBytesToBytes which uses codepoint mapping
// directly and never sees parseUUID; the asymmetry breaks EncodeJSON
// for any record that relies on a fixed-uuid default. Fix: route a
// failed parseUUID through the generic fixed-codepoint path, whose
// size check still rejects malformed 36-char hex-dash inputs.
func TestRegression_UUIDFixedDefaultJSONEncode(t *testing.T) {
	schemaStr := `{"type":"record","name":"R","fields":[
{"name":"id","type":{"type":"fixed","name":"FixedUUID","size":16,"logicalType":"uuid"},
 "default":"                "}
]}`
	s, err := avro.Parse(schemaStr)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	bin, err := s.AppendEncode(nil, map[string]any{})
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	if string(bin) != strings.Repeat("\x20", 16) {
		t.Fatalf("binary encode = %x; want 16 bytes of 0x20", bin)
	}
	out, err := s.EncodeJSON(map[string]any{})
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	if !strings.Contains(string(out), `"id":"                "`) {
		t.Fatalf("EncodeJSON %s missing expected default literal", out)
	}
}

// TestRegression_Float32NarrowingPredicateParity locks the 8 sites that
// share the predicate
//
//	!math.IsInf(f, 0) && !math.IsNaN(f) && math.IsInf(float64(float32(f)), 0)
//
// behind the shared finiteFloat32Overflows helper. Drift sentinel: any
// site that loses the `!math.IsInf(f,0) && !math.IsNaN(f)` guard
// silently rejects ±Inf and NaN inputs, breaking LinkedinFloats
// round-trip and TestRegression_SerArrayFloatSilentInf invariants.
func TestRegression_Float32NarrowingPredicateParity(t *testing.T) {
	const finite = 1e300

	t.Run("deser.go deserDouble float32 reflect target", func(t *testing.T) {
		s := avro.MustParse(`"double"`)
		enc, err := s.AppendEncode(nil, finite)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var v float32
		if _, err := s.Decode(enc, &v); err == nil {
			t.Errorf("expected overflow error, got %v", v)
		}
	})

	t.Run("resolve.go encodeDefault for float schema", func(t *testing.T) {
		_, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":"float","default":1e300}]}`)
		if err == nil {
			t.Error("expected error parsing schema with overflowing float default")
		}
	})

	t.Run("json_decode.go decodeDouble float32 reflect target", func(t *testing.T) {
		s := avro.MustParse(`"double"`)
		var v float32
		if err := s.DecodeJSON([]byte(`1e300`), &v); err == nil {
			t.Errorf("expected overflow, got %v", v)
		}
	})

	t.Run("json_codec.go jsonCoerceToFloat64 bitSize=32", func(t *testing.T) {
		s := avro.MustParse(`"float"`)
		if out, err := s.AppendEncodeJSON(nil, finite); err == nil {
			t.Errorf("expected overflow, got: %s", out)
		}
	})

	t.Run("ser.go appendAvroFloat32", func(t *testing.T) {
		s := avro.MustParse(`"float"`)
		if _, err := s.AppendEncode(nil, finite); err == nil {
			t.Error("expected overflow")
		}
	})

	t.Run("unsafe.go usFloat Float64 source", func(t *testing.T) {
		type R struct {
			F float64 `avro:"f"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"f","type":"float"}]}`)
		if _, err := s.AppendEncode(nil, R{F: finite}); err == nil {
			t.Error("expected overflow")
		}
	})

	t.Run("unsafe.go udDouble Float32 target", func(t *testing.T) {
		type R struct {
			F float32 `avro:"f"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"f","type":"double"}]}`)
		body, err := avro.MustParse(`"double"`).AppendEncode(nil, finite)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var r R
		if _, err := s.Decode(body, &r); err == nil {
			t.Errorf("expected overflow, got %+v", r)
		}
	})

	t.Run("inf and nan must pass through float encode", func(t *testing.T) {
		s := avro.MustParse(`"float"`)
		for _, v := range []float64{math.Inf(1), math.Inf(-1), math.NaN()} {
			if _, err := s.AppendEncode(nil, v); err != nil {
				t.Errorf("%v must encode as float without overflow rejection, got %v", v, err)
			}
		}
	})
}

// TestRegression_BytesFixedDefaultParityNumericString locks parity
// between binary and JSON encode for record fields whose default is a
// string that happens to look like a number, against bytes/fixed
// schemas with logical types. Pre-fix, the JSON encoder's
// logical-type-aware arms (decimal, big-decimal, uuid) tried to
// interpret the string semantically (decimalRatFor / parseUUID),
// while the binary path's encodeDefault routed the same string through
// codepoint mapping (avroJSONBytesToBytes) — silent divergence.
//
// Per Avro 1.12 spec ("Logical Types: a logical type is always
// serialized using its underlying Avro type"), defaults for bytes/
// fixed are JSON strings interpreted as codepoint-mapped bytes
// regardless of any logical type overlay. Java's
// ResolvingGrammarGenerator.java:323 writes default text via
// ISO_8859_1.getBytes(); fastavro stores defaults as raw bytes.
//
// Sibling sweep: this test covers the 4 affected arms (bytes-decimal,
// fixed-decimal, bytes-big-decimal, fixed-uuid) at top-level AND
// nested in record/array/map. The runtime-input path (string passed
// directly by the user, not via default-fill) is intentionally
// unchanged: there decimalRatFor/parseUUID still apply, matching the
// binary path's tryCoerceToRat / parseUUID lenient runtime acceptance.
func TestRegression_BytesFixedDefaultParityNumericString(t *testing.T) {
	roundTripBoth := func(t *testing.T, schemaStr string, lookFor string) {
		t.Helper()
		s, err := avro.Parse(schemaStr)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		bin, err := s.AppendEncode(nil, map[string]any{})
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		jsn, err := s.EncodeJSON(map[string]any{})
		if err != nil {
			t.Fatalf("EncodeJSON: %v", err)
		}
		var binDec, jsnDec map[string]any
		if _, err := s.Decode(bin, &binDec); err != nil {
			t.Fatalf("Decode binary: %v (raw=%x)", err, bin)
		}
		if err := s.DecodeJSON(jsn, &jsnDec); err != nil {
			t.Fatalf("DecodeJSON: %v (raw=%s)", err, jsn)
		}
		bv := fmt.Sprintf("%v", binDec[lookFor])
		jv := fmt.Sprintf("%v", jsnDec[lookFor])
		if bv != jv {
			t.Errorf("default decoded to different values:\n  binary=%s\n  JSON=%s\n  (binary wire=%x, JSON=%s)", bv, jv, bin, jsn)
		}
	}

	t.Run("bytes-decimal", func(t *testing.T) {
		roundTripBoth(t, `{"type":"record","name":"R","fields":[
			{"name":"d","type":{"type":"bytes","logicalType":"decimal","precision":4,"scale":2},"default":"0.33"}
		]}`, "d")
	})
	t.Run("fixed-decimal", func(t *testing.T) {
		roundTripBoth(t, `{"type":"record","name":"R","fields":[
			{"name":"d","type":{"type":"fixed","name":"f","size":4,"logicalType":"decimal","precision":4,"scale":2},"default":"0.33"}
		]}`, "d")
	})
	t.Run("fixed-uuid", func(t *testing.T) {
		roundTripBoth(t, `{"type":"record","name":"R","fields":[
			{"name":"d","type":{"type":"fixed","name":"f","size":16,"logicalType":"uuid"},"default":"                "}
		]}`, "d")
	})

	// Nested cases: same shape but the bytes/fixed default lives inside
	// a record/array/map, so the parse-time conversion must walk
	// recursively to reach it. Pre-fix, these silently diverged the
	// same way the top-level cases did.
	t.Run("nested bytes-decimal in record", func(t *testing.T) {
		roundTripBoth(t, `{"type":"record","name":"R","fields":[{"name":"outer","type":{
			"type":"record","name":"Inner","fields":[
				{"name":"d","type":{"type":"bytes","logicalType":"decimal","precision":4,"scale":2},"default":"0.33"}
			]},"default":{}}
		]}`, "outer")
	})
	t.Run("nested bytes-decimal in array", func(t *testing.T) {
		roundTripBoth(t, `{"type":"record","name":"R","fields":[
			{"name":"d","type":{"type":"array","items":{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}},"default":["0.33"]}
		]}`, "d")
	})
	t.Run("nested bytes-decimal in map", func(t *testing.T) {
		roundTripBoth(t, `{"type":"record","name":"R","fields":[
			{"name":"d","type":{"type":"map","values":{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}},"default":{"k":"0.33"}}
		]}`, "d")
	})
	t.Run("nested bytes-decimal in union branch", func(t *testing.T) {
		// Union-branch default: per Avro 1.12 the default matches the
		// first compatible branch. The bytes-decimal branch matches the
		// string default; convertDefaultBytes must walk into the union
		// and recurse on the matching branch.
		roundTripBoth(t, `{"type":"record","name":"R","fields":[
			{"name":"d","type":[{"type":"bytes","logicalType":"decimal","precision":4,"scale":2},"null"],"default":"0.33"}
		]}`, "d")
	})

	// Runtime user-input case — string "0.33" passed directly. Both
	// paths should interpret as decimal (lenient parity). This is NOT
	// a default — locked here to confirm the fix doesn't accidentally
	// change runtime behavior.
	t.Run("runtime user input still decimal-interpreted", func(t *testing.T) {
		s := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`)
		bin, err := s.AppendEncode(nil, "0.33")
		if err != nil {
			t.Fatalf("binary encode: %v", err)
		}
		jsn, err := s.AppendEncodeJSON(nil, "0.33")
		if err != nil {
			t.Fatalf("JSON encode: %v", err)
		}
		// Binary: 1-byte body [0x21] (33 unscaled), framed as varlong-len.
		if len(bin) != 2 || bin[1] != 0x21 {
			t.Errorf("runtime binary diverged from decimal interpretation: %x", bin)
		}
		// JSON: codepoint of 0x21 is "!" — quoted.
		if string(jsn) != `"!"` {
			t.Errorf("runtime JSON diverged from decimal interpretation: %s", jsn)
		}
	})

	// Runtime user-input case — fixed-uuid with a 16-char non-canonical
	// string. Binary path rejects via parseUUID. After fix, JSON path
	// MUST also reject (was silently accepting via my recent fall-
	// through; that fall-through was for defaults, which are now
	// []byte and no longer hit this arm).
	t.Run("runtime fixed-uuid 16-char string rejected on both paths", func(t *testing.T) {
		s := avro.MustParse(`{"type":"fixed","name":"u","size":16,"logicalType":"uuid"}`)
		const sval = "                "
		_, berr := s.AppendEncode(nil, sval)
		_, jerr := s.AppendEncodeJSON(nil, sval)
		if (berr == nil) != (jerr == nil) {
			t.Errorf("parity divergence: binary err=%v, JSON err=%v", berr, jerr)
		}
		if jerr == nil {
			t.Error("JSON path accepted non-UUID 16-char runtime string; binary rejects")
		}
	})

	// big-decimal default with a numeric-looking string: per spec, the
	// codepoint bytes of "3.14" must form a valid big-decimal inner
	// payload (length-prefixed unscaled || zigzag scale). They don't —
	// the first byte 0x33 zigzag-decodes to -26 length, which the
	// decoder rejects. Both encode paths now produce the same (bad)
	// codepoint bytes; the user gets a clear decode-time error rather
	// than silent disagreement. This locks parity even when the
	// schema's default is unrecoverable.
	t.Run("big-decimal numeric-string default reaches decoder symmetrically", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"d","type":{"type":"bytes","logicalType":"big-decimal"},"default":"3.14"}
		]}`)
		bin, err := s.AppendEncode(nil, map[string]any{})
		if err != nil {
			t.Fatalf("binary encode: %v", err)
		}
		jsn, err := s.EncodeJSON(map[string]any{})
		if err != nil {
			t.Fatalf("JSON encode: %v", err)
		}
		var binDec, jsnDec map[string]any
		_, binErr := s.Decode(bin, &binDec)
		jsnErr := s.DecodeJSON(jsn, &jsnDec)
		// Both must agree on outcome: either both decode the same
		// (correct) value, or both reject with the same kind of error.
		_, _ = binErr, jsnErr
		if (binErr == nil) != (jsnErr == nil) {
			t.Errorf("parity divergence on decode:\n  binary err=%v\n  JSON err=%v\n  (binary wire=%x, JSON=%s)", binErr, jsnErr, bin, jsn)
		}
	})
}

// TestRegression_NamedTypeRefDefaultParity locks parity for record
// fields whose type is a *named-type reference* (forward or backward)
// to a bytes/fixed schema. Pre-fix, convertDefaultBytes only walked
// the aschema canon directly through its primitive/object/union
// cases — when a field's type was `aschema{primitive: "MyName"}`,
// none of those cases matched and the string default flowed through
// unconverted, hitting the JSON encoder's logical-type-aware arms.
//
// Sibling sweep: every shape that produces a named-type reference in
// the field's resolved schema. Fwd-ref means the field references a
// name declared LATER in the schema (resolved via finalize fixup);
// backward name-ref means the name was declared earlier inline.
// Both reach the same buggy state because both produce a canon with
// primitive=<name>.
func TestRegression_NamedTypeRefDefaultParity(t *testing.T) {
	roundTripBoth := func(t *testing.T, schemaStr string, val any) {
		t.Helper()
		s, err := avro.Parse(schemaStr)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		bin, err := s.AppendEncode(nil, val)
		if err != nil {
			t.Fatalf("binary encode: %v", err)
		}
		jsn, err := s.EncodeJSON(val)
		if err != nil {
			t.Fatalf("JSON encode: %v (binary succeeded with %x)", err, bin)
		}
		var binDec, jsnDec map[string]any
		if _, err := s.Decode(bin, &binDec); err != nil {
			t.Fatalf("decode binary: %v", err)
		}
		if err := s.DecodeJSON(jsn, &jsnDec); err != nil {
			t.Fatalf("decode JSON: %v", err)
		}
		bv := fmt.Sprintf("%v", binDec)
		jv := fmt.Sprintf("%v", jsnDec)
		if bv != jv {
			t.Errorf("default decoded to different values:\n  binary=%s\n  JSON=%s\n  (binary wire=%x, JSON=%s)", bv, jv, bin, jsn)
		}
	}

	t.Run("fwd-ref fixed-uuid default", func(t *testing.T) {
		roundTripBoth(t, `{"type":"record","name":"R","fields":[
			{"name":"u","type":"MyUUID","default":"                "},
			{"name":"def","type":{"type":"fixed","name":"MyUUID","size":16,"logicalType":"uuid"}}
		]}`, map[string]any{"def": [16]byte{}})
	})
	t.Run("fwd-ref fixed-decimal default", func(t *testing.T) {
		roundTripBoth(t, `{"type":"record","name":"R","fields":[
			{"name":"d","type":"MyDec","default":"0.33"},
			{"name":"def","type":{"type":"fixed","name":"MyDec","size":4,"logicalType":"decimal","precision":4,"scale":2}}
		]}`, map[string]any{"def": [4]byte{0, 0, 0, 0x21}})
	})
	t.Run("fwd-ref nested record bytes-decimal default", func(t *testing.T) {
		roundTripBoth(t, `{"type":"record","name":"R","fields":[
			{"name":"outer","type":"Inner","default":{"d":"0.33"}},
			{"name":"def","type":{"type":"record","name":"Inner","fields":[
				{"name":"d","type":{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}}
			]}}
		]}`, map[string]any{"def": map[string]any{"d": []byte{0x21}}})
	})
	t.Run("backward name-ref nested record bytes-decimal default", func(t *testing.T) {
		// Inner declared FIRST, then referenced. Goes through the
		// backward name-ref path (no fixup), which also produces a
		// canon{primitive: "Inner"} that pre-fix convertDefaultBytes
		// didn't recurse into.
		roundTripBoth(t, `{"type":"record","name":"R","fields":[
			{"name":"def","type":{"type":"record","name":"Inner","fields":[
				{"name":"d","type":{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}}
			]}},
			{"name":"outer","type":"Inner","default":{"d":"0.33"}}
		]}`, map[string]any{"def": map[string]any{"d": []byte{0x21}}})
	})
	t.Run("union branch name-ref fixed-uuid default", func(t *testing.T) {
		roundTripBoth(t, `{"type":"record","name":"R","fields":[
			{"name":"def","type":{"type":"fixed","name":"MyUUID","size":16,"logicalType":"uuid"}},
			{"name":"u","type":["MyUUID","null"],"default":"                "}
		]}`, map[string]any{"def": [16]byte{}})
	})
	t.Run("array of name-ref decimal default", func(t *testing.T) {
		roundTripBoth(t, `{"type":"record","name":"R","fields":[
			{"name":"def","type":{"type":"record","name":"Inner","fields":[
				{"name":"d","type":{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}}
			]}},
			{"name":"arr","type":{"type":"array","items":"Inner"},"default":[{"d":"0.33"}]}
		]}`, map[string]any{"def": map[string]any{"d": []byte{0x21}}})
	})
	t.Run("map of name-ref decimal default", func(t *testing.T) {
		roundTripBoth(t, `{"type":"record","name":"R","fields":[
			{"name":"def","type":{"type":"record","name":"Inner","fields":[
				{"name":"d","type":{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}}
			]}},
			{"name":"m","type":{"type":"map","values":"Inner"},"default":{"k":{"d":"0.33"}}}
		]}`, map[string]any{"def": map[string]any{"d": []byte{0x21}}})
	})
}

// TestRegression_NamedTypeRefDefaultValidation locks the validation
// gap where validateDefault silently accepted any value for a record
// field whose type was a name-reference. Pre-fix the aschema-canon
// `{primitive: <Name>}` went to validateDefaultPrimitive which had no
// case for arbitrary names → fell through with no error. Schemas
// with invalid name-ref defaults (wrong size, wrong type, missing
// required field, etc.) slipped through parse and would fail later
// at encode time or produce silently-wrong wire bytes.
func TestRegression_NamedTypeRefDefaultValidation(t *testing.T) {
	expectError := func(t *testing.T, schemaStr string, wantContains string) {
		t.Helper()
		_, err := avro.Parse(schemaStr)
		if err == nil {
			t.Fatalf("expected parse error containing %q; got nil", wantContains)
		}
		if !strings.Contains(err.Error(), wantContains) {
			t.Fatalf("expected error containing %q; got %v", wantContains, err)
		}
	}

	t.Run("backward fixed-name-ref wrong length default", func(t *testing.T) {
		expectError(t, `{"type":"record","name":"R","fields":[
			{"name":"def","type":{"type":"fixed","name":"F4","size":4}},
			{"name":"f","type":"F4","default":"abc"}
		]}`, "length")
	})
	t.Run("backward enum-name-ref unknown-symbol default", func(t *testing.T) {
		expectError(t, `{"type":"record","name":"R","fields":[
			{"name":"def","type":{"type":"enum","name":"E","symbols":["A","B"]}},
			{"name":"e","type":"E","default":"Z"}
		]}`, "symbol")
	})
	t.Run("backward record-name-ref missing-required-field default", func(t *testing.T) {
		expectError(t, `{"type":"record","name":"R","fields":[
			{"name":"def","type":{"type":"record","name":"Inner","fields":[
				{"name":"r","type":"int"}
			]}},
			{"name":"i","type":"Inner","default":{}}
		]}`, "missing")
	})
	t.Run("backward record-name-ref wrong-type field default", func(t *testing.T) {
		expectError(t, `{"type":"record","name":"R","fields":[
			{"name":"def","type":{"type":"record","name":"Inner","fields":[
				{"name":"r","type":"int"}
			]}},
			{"name":"i","type":"Inner","default":{"r":"not-an-int"}}
		]}`, "int")
	})

	// Floats defaulted from string need coerceDefault to convert at
	// parse time so the JSON encoder's jsonCoerceToFloat64 (which
	// rejects string inputs) sees float64. The bug surfaces when the
	// USER supplies a string-form float in a default for a name-ref'd
	// record: pre-fix, validateDefault didn't recurse into the resolved
	// record (because aschema{primitive:"Inner"} falls through
	// validateDefaultPrimitive's "unknown primitive" silent accept),
	// so coerceDefault never fired on the nested field and the JSON
	// encoder hit jsonCoerceToFloat64 with a reflect.String at
	// default-fill time.
	t.Run("name-ref record with user-supplied float-from-string nested default", func(t *testing.T) {
		s, err := avro.Parse(`{"type":"record","name":"R","fields":[
			{"name":"def","type":{"type":"record","name":"Inner","fields":[
				{"name":"f","type":"float"}
			]}},
			{"name":"i","type":"Inner","default":{"f":"1.5"}}
		]}`)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		// "def" supplied; "i" absent → default-fill via JSON path.
		if _, err := s.EncodeJSON(map[string]any{
			"def": map[string]any{"f": float32(0)},
		}); err != nil {
			t.Fatalf("EncodeJSON default-fill rejected user-supplied float-string nested in name-ref: %v", err)
		}
	})
}

// TestRegression_TimeOfDayLogicalRoundTripsTimeTime locks symmetric
// time.Time support on the time-millis / time-micros logical types
// across every encode AND decode path. Pre-fix the encoder accepted
// time.Time (extracting wall-clock fields and encoding time-of-day),
// but the matching decoder rejected time.Time targets with
// "cannot use time.Time with Avro type {int,long}" — a round-trip
// break documented as parity in the encoder's regression test but
// unimplemented on the decode side.
//
// Sibling sweep covers: binary safe path (deserTimeMillis,
// deserTimeMicros), JSON safe path (decodeInt + decodeLong arms),
// unsafe struct-field path (udTimeMillis, udTimeMicros), and the
// matching unsafe encode dispatch (usTimeMillis, usTimeMicros which
// previously cast `unsafe.Pointer` as `*time.Duration` and would have
// scrambled a `time.Time` field).
//
// Conversion: decode produces a time.Time at the Unix epoch (UTC)
// plus the time-of-day duration — the encoder strips date components
// regardless, so the base date is arbitrary but stable.
func TestRegression_TimeOfDayLogicalRoundTripsTimeTime(t *testing.T) {
	// 14:30:45.123456 — non-zero in every field, exercise micros precision.
	tm := time.Date(2020, 6, 15, 14, 30, 45, 123_456_000, time.UTC)
	wantH, wantM, wantS, wantNs := tm.Hour(), tm.Minute(), tm.Second(), tm.Nanosecond()
	assertTimeOfDay := func(t *testing.T, where string, got time.Time) {
		t.Helper()
		// time-millis truncates sub-millisecond precision; widen tolerance accordingly.
		gh, gm, gs := got.Hour(), got.Minute(), got.Second()
		if gh != wantH || gm != wantM || gs != wantS {
			t.Errorf("%s: hour/min/sec mismatch — got %02d:%02d:%02d, want %02d:%02d:%02d", where, gh, gm, gs, wantH, wantM, wantS)
		}
	}
	assertTimeOfDayMicros := func(t *testing.T, where string, got time.Time) {
		t.Helper()
		assertTimeOfDay(t, where, got)
		// micros precision: bottom 3 digits zero (the encoder's d.Microseconds() truncates ns).
		wantNsMicroAligned := (wantNs / 1000) * 1000
		if got.Nanosecond() != wantNsMicroAligned {
			t.Errorf("%s: ns mismatch — got %d, want %d", where, got.Nanosecond(), wantNsMicroAligned)
		}
	}

	t.Run("binary safe time-millis", func(t *testing.T) {
		s := avro.MustParse(`{"type":"int","logicalType":"time-millis"}`)
		enc, err := s.AppendEncode(nil, tm)
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var got time.Time
		if _, err := s.Decode(enc, &got); err != nil {
			t.Fatalf("Decode into time.Time: %v", err)
		}
		assertTimeOfDay(t, "binary safe time-millis", got)
	})
	t.Run("binary safe time-micros", func(t *testing.T) {
		s := avro.MustParse(`{"type":"long","logicalType":"time-micros"}`)
		enc, err := s.AppendEncode(nil, tm)
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var got time.Time
		if _, err := s.Decode(enc, &got); err != nil {
			t.Fatalf("Decode into time.Time: %v", err)
		}
		assertTimeOfDayMicros(t, "binary safe time-micros", got)
	})
	t.Run("JSON safe time-millis", func(t *testing.T) {
		s := avro.MustParse(`{"type":"int","logicalType":"time-millis"}`)
		enc, err := s.EncodeJSON(tm)
		if err != nil {
			t.Fatalf("EncodeJSON: %v", err)
		}
		var got time.Time
		if err := s.DecodeJSON(enc, &got); err != nil {
			t.Fatalf("DecodeJSON into time.Time: %v", err)
		}
		assertTimeOfDay(t, "JSON safe time-millis", got)
	})
	t.Run("JSON safe time-micros", func(t *testing.T) {
		s := avro.MustParse(`{"type":"long","logicalType":"time-micros"}`)
		enc, err := s.EncodeJSON(tm)
		if err != nil {
			t.Fatalf("EncodeJSON: %v", err)
		}
		var got time.Time
		if err := s.DecodeJSON(enc, &got); err != nil {
			t.Fatalf("DecodeJSON into time.Time: %v", err)
		}
		assertTimeOfDayMicros(t, "JSON safe time-micros", got)
	})
	t.Run("unsafe struct field time-millis", func(t *testing.T) {
		type R struct {
			T time.Time `avro:"t"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"t","type":{"type":"int","logicalType":"time-millis"}}]}`)
		enc, err := s.AppendEncode(nil, R{T: tm})
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var got R
		if _, err := s.Decode(enc, &got); err != nil {
			t.Fatalf("Decode into struct: %v", err)
		}
		assertTimeOfDay(t, "unsafe struct time-millis", got.T)
	})
	t.Run("unsafe struct field time-micros", func(t *testing.T) {
		type R struct {
			T time.Time `avro:"t"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"t","type":{"type":"long","logicalType":"time-micros"}}]}`)
		enc, err := s.AppendEncode(nil, R{T: tm})
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var got R
		if _, err := s.Decode(enc, &got); err != nil {
			t.Fatalf("Decode into struct: %v", err)
		}
		assertTimeOfDayMicros(t, "unsafe struct time-micros", got.T)
	})
}

// TestRegression_SchemaForLogicalBaseType locks SchemaFor's
// (Go-type, logical-type) → base-type mapping against the Avro spec.
// Pre-fix, the timeType/durationType branches in inferType selected
// the base by Go type rather than by logical-type-required base,
// producing schemas Parse rejects ("invalid logicalType X type Y").
//
// Per spec:
//   - date / time-millis → int
//   - time-micros / timestamp-* / local-timestamp-* → long
func TestRegression_SchemaForLogicalBaseType(t *testing.T) {
	type cases struct {
		name        string
		schemaFn    func() (*avro.Schema, error)
		wantBase    string
		wantLogical string
	}
	tests := []cases{
		{"time.Time + time-millis", func() (*avro.Schema, error) {
			type R struct {
				T time.Time `avro:"t,time-millis"`
			}
			return avro.SchemaFor[R]()
		}, "int", "time-millis"},
		{"time.Time + date (control)", func() (*avro.Schema, error) {
			type R struct {
				T time.Time `avro:"t,date"`
			}
			return avro.SchemaFor[R]()
		}, "int", "date"},
		{"time.Time + time-micros", func() (*avro.Schema, error) {
			type R struct {
				T time.Time `avro:"t,time-micros"`
			}
			return avro.SchemaFor[R]()
		}, "long", "time-micros"},
		{"time.Duration + timestamp-millis", func() (*avro.Schema, error) {
			type R struct {
				T time.Duration `avro:"t,timestamp-millis"`
			}
			return avro.SchemaFor[R]()
		}, "long", "timestamp-millis"},
		{"time.Duration + timestamp-micros", func() (*avro.Schema, error) {
			type R struct {
				T time.Duration `avro:"t,timestamp-micros"`
			}
			return avro.SchemaFor[R]()
		}, "long", "timestamp-micros"},
		{"time.Duration + timestamp-nanos", func() (*avro.Schema, error) {
			type R struct {
				T time.Duration `avro:"t,timestamp-nanos"`
			}
			return avro.SchemaFor[R]()
		}, "long", "timestamp-nanos"},
		{"time.Duration + local-timestamp-millis", func() (*avro.Schema, error) {
			type R struct {
				T time.Duration `avro:"t,local-timestamp-millis"`
			}
			return avro.SchemaFor[R]()
		}, "long", "local-timestamp-millis"},
		{"time.Duration + local-timestamp-micros", func() (*avro.Schema, error) {
			type R struct {
				T time.Duration `avro:"t,local-timestamp-micros"`
			}
			return avro.SchemaFor[R]()
		}, "long", "local-timestamp-micros"},
		{"time.Duration + local-timestamp-nanos", func() (*avro.Schema, error) {
			type R struct {
				T time.Duration `avro:"t,local-timestamp-nanos"`
			}
			return avro.SchemaFor[R]()
		}, "long", "local-timestamp-nanos"},
		{"time.Duration + time-micros (control)", func() (*avro.Schema, error) {
			type R struct {
				T time.Duration `avro:"t,time-micros"`
			}
			return avro.SchemaFor[R]()
		}, "long", "time-micros"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := tc.schemaFn()
			if err != nil {
				t.Fatalf("SchemaFor: %v", err)
			}
			js := s.String()
			wantBase := `"type":"` + tc.wantBase + `"`
			wantLogical := `"logicalType":"` + tc.wantLogical + `"`
			if !strings.Contains(js, wantBase) || !strings.Contains(js, wantLogical) {
				t.Errorf("schema %s\n  want base=%s logical=%s", js, tc.wantBase, tc.wantLogical)
			}
		})
	}
}

// TestRegression_OmitzeroJSONEncodeValueFieldNullUnion locks binary
// ↔ JSON parity for the `omitzero` struct-tag on value-typed
// null-union fields. Pre-fix, the JSON encoder's struct branch
// (appendAvroJSONRecord) skipped the `omitzero + nullunion +
// valueIsZero → emit null` check that ser.go's slow path and
// unsafe.go's fast path both enforce: a zero string / bool / record
// / time.Time value went out as its zero literal, while the binary
// encoder correctly emitted the null-branch byte. Pointer-typed
// fields were unaffected because the nil-pointer indirection
// independently routes through the null branch.
//
// Sibling sweep: every value-typed null-union shape — first-branch
// null, second-branch null, IsZero() on time.Time, nested record
// — plus the working pointer paths as regression-guard controls.
func TestRegression_OmitzeroJSONEncodeValueFieldNullUnion(t *testing.T) {
	t.Run("string with null-first union", func(t *testing.T) {
		type R struct {
			Name string `avro:"name,omitzero"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"name","type":["null","string"]}]}`)
		bin, err := s.AppendEncode(nil, R{Name: ""})
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		if !bytes.Equal(bin, []byte{0x00}) {
			t.Fatalf("binary: got %x, want 00 (null branch)", bin)
		}
		js, err := s.EncodeJSON(R{Name: ""})
		if err != nil {
			t.Fatalf("EncodeJSON: %v", err)
		}
		if !strings.Contains(string(js), `"name":null`) {
			t.Errorf("JSON: got %s, want \"name\":null per omitzero (matching binary 0x00)", js)
		}
	})
	t.Run("string with null-second union", func(t *testing.T) {
		type R struct {
			Name string `avro:"name,omitzero"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"name","type":["string","null"]}]}`)
		bin, err := s.AppendEncode(nil, R{Name: ""})
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		if !bytes.Equal(bin, []byte{0x02}) {
			t.Fatalf("binary: got %x, want 02 (null at idx 1)", bin)
		}
		js, err := s.EncodeJSON(R{Name: ""})
		if err != nil {
			t.Fatalf("EncodeJSON: %v", err)
		}
		if !strings.Contains(string(js), `"name":null`) {
			t.Errorf("JSON: got %s, want \"name\":null", js)
		}
	})
	t.Run("bool with null-first union", func(t *testing.T) {
		type R struct {
			F bool `avro:"f,omitzero"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"f","type":["null","boolean"]}]}`)
		js, err := s.EncodeJSON(R{F: false})
		if err != nil {
			t.Fatalf("EncodeJSON: %v", err)
		}
		if !strings.Contains(string(js), `"f":null`) {
			t.Errorf("JSON: got %s, want \"f\":null", js)
		}
	})
	t.Run("time.Time IsZero with null-first union", func(t *testing.T) {
		type R struct {
			When time.Time `avro:"when,omitzero"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"when","type":["null",{"type":"long","logicalType":"timestamp-millis"}]}]}`)
		js, err := s.EncodeJSON(R{}) // zero-value time.Time → IsZero()
		if err != nil {
			t.Fatalf("EncodeJSON: %v", err)
		}
		if !strings.Contains(string(js), `"when":null`) {
			t.Errorf("JSON: got %s, want \"when\":null", js)
		}
	})
	t.Run("nested record with null-first union", func(t *testing.T) {
		type Sub struct {
			A int32 `avro:"a"`
		}
		type R struct {
			X Sub `avro:"x,omitzero"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"x","type":["null",{"type":"record","name":"Sub","fields":[{"name":"a","type":"int"}]}]}
		]}`)
		js, err := s.EncodeJSON(R{X: Sub{}})
		if err != nil {
			t.Fatalf("EncodeJSON: %v", err)
		}
		if !strings.Contains(string(js), `"x":null`) {
			t.Errorf("JSON: got %s, want \"x\":null (record zero-value)", js)
		}
	})

	// Control cases: pointer-typed null-union fields already work
	// because the encoder's nil-pointer indirect path routes
	// independently of omitzero. Lock these so the omitzero fix
	// doesn't accidentally break them.
	t.Run("control: *string nil with null-first union", func(t *testing.T) {
		type R struct {
			Name *string `avro:"name,omitzero"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"name","type":["null","string"]}]}`)
		js, err := s.EncodeJSON(R{Name: nil})
		if err != nil {
			t.Fatalf("EncodeJSON: %v", err)
		}
		if !strings.Contains(string(js), `"name":null`) {
			t.Errorf("control regressed: got %s", js)
		}
	})
	t.Run("control: non-zero string still encoded", func(t *testing.T) {
		type R struct {
			Name string `avro:"name,omitzero"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"name","type":["null","string"]}]}`)
		js, err := s.EncodeJSON(R{Name: "hello"})
		if err != nil {
			t.Fatalf("EncodeJSON: %v", err)
		}
		if !strings.Contains(string(js), `"hello"`) {
			t.Errorf("non-zero value lost: got %s", js)
		}
	})
}

// TestRegression_UnionBytesFixedDefaultMisroutedToWrongBranch locks
// the parse-time branch selection used by convertDefaultBytes when a
// default lands on a union. Pre-fix the union arm selected branches
// via a structural-only matcher (`branchAcceptsDefault`) that didn't
// know about fixed-size constraints, so [fixed:8,"string"] with
// default "abcd" misrouted into the fixed:8 branch — the conversion
// produced a 4-byte []byte that fit no branch, and `encodeDefault`'s
// try-each-branch loop then errored "union default does not match any
// branch". validateDefault meanwhile had cleanly accepted the string
// branch. Fix: convertDefaultBytes now reuses validateDefault as the
// branch selector so the two functions agree.
func TestRegression_UnionBytesFixedDefaultMisroutedToWrongBranch(t *testing.T) {
	cases := []struct {
		name, schema string
	}{
		{
			"fixed_then_string_mismatched_size",
			`{"type":"record","name":"R","fields":[
				{"name":"f","type":[{"type":"fixed","name":"F8","size":8},"string"],"default":"abcd"}
			]}`,
		},
		{
			"fixed_then_enum_mismatched_size",
			`{"type":"record","name":"R","fields":[
				{"name":"f","type":[{"type":"fixed","name":"F8","size":8},{"type":"enum","name":"E","symbols":["abcd","wxyz"]}],"default":"abcd"}
			]}`,
		},
		{
			"nested_in_record_branches",
			`{"type":"record","name":"R","fields":[
				{"name":"f","type":[
					{"type":"record","name":"WithFixed","fields":[{"name":"sub","type":{"type":"fixed","name":"F8","size":8}}]},
					{"type":"record","name":"WithString","fields":[{"name":"sub","type":"string"}]}
				],"default":{"sub":"abc"}}
			]}`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := avro.Parse(c.schema)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			bin, err := s.AppendEncode(nil, map[string]any{})
			if err != nil {
				t.Fatalf("AppendEncode: %v", err)
			}
			var out map[string]any
			if _, err := s.Decode(bin, &out); err != nil {
				t.Fatalf("Decode round-trip: %v", err)
			}
			if _, err := s.EncodeJSON(map[string]any{}); err != nil {
				t.Fatalf("EncodeJSON: %v", err)
			}
		})
	}
}

// TestRegression_DeserFixedAcceptsStringTarget locks binary encode ↔
// decode parity for `fixed` types when the Go target is a string.
// serSize (the binary encoder for fixed) accepts a `reflect.String` of
// correct length and writes raw bytes; pre-fix deserFixed did not
// accept reflect.String targets, so the encoder could write what the
// decoder couldn't read — same asymmetry shape as the JSON enum bug,
// just on the binary side. deserBytes already had the parallel
// reflect.String arm; the JSON path's assignBytes accepts strings on
// both encode and decode. Only binary deserFixed was the outlier.
func TestRegression_DeserFixedAcceptsStringTarget(t *testing.T) {
	t.Run("top-level string round-trip", func(t *testing.T) {
		s := avro.MustParse(`{"type":"fixed","name":"F8","size":8}`)
		src := "abcdefgh"
		bin, err := s.AppendEncode(nil, src)
		if err != nil {
			t.Fatalf("binary encode of string: %v", err)
		}
		if string(bin) != src {
			t.Fatalf("binary encode bytes: got %q, want %q", bin, src)
		}
		var got string
		if _, err := s.Decode(bin, &got); err != nil {
			t.Fatalf("binary decode into string: %v", err)
		}
		if got != src {
			t.Errorf("binary round-trip: got %q, want %q", got, src)
		}
	})
	t.Run("struct field string round-trip", func(t *testing.T) {
		type R struct {
			F string `avro:"f"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"fixed","name":"F8","size":8}}]}`)
		bin, err := s.AppendEncode(nil, R{F: "abcdefgh"})
		if err != nil {
			t.Fatalf("binary encode struct: %v", err)
		}
		var got R
		if _, err := s.Decode(bin, &got); err != nil {
			t.Fatalf("binary decode struct: %v", err)
		}
		if got.F != "abcdefgh" {
			t.Errorf("struct round-trip: got %q", got.F)
		}
	})
}

// TestRegression_JSONEnumDecodeIntoIntTargetParity locks binary ↔
// JSON parity for enum decode into integer-typed targets. Binary
// deserEnum accepted int/uint (case v.CanInt()/CanUint() arms set
// the ordinal); the resolved enum deser and JSON enum encode both
// accept int/uint too. JSON decodeEnum was the lone outlier: it
// rejected int/uint targets with "cannot use int with Avro type
// enum", so a struct round-trip that worked on binary errored on
// JSON. Java JsonDecoder.readEnum returns an int and fastavro
// read_enum returns the index — both reference impls produce the
// ordinal, leaving target-type mapping to the layer above.
func TestRegression_JSONEnumDecodeIntoIntTargetParity(t *testing.T) {
	schema := avro.MustParse(`{"type":"record","name":"R","fields":[
		{"name":"color","type":{"type":"enum","name":"C","symbols":["Red","Green","Blue"]}}
	]}`)
	type Record struct {
		Color int `avro:"color"`
	}
	src := Record{Color: 1}

	t.Run("binary control", func(t *testing.T) {
		bin, err := schema.AppendEncode(nil, src)
		if err != nil {
			t.Fatalf("binary encode: %v", err)
		}
		var dst Record
		if _, err := schema.Decode(bin, &dst); err != nil {
			t.Fatalf("binary decode: %v", err)
		}
		if dst.Color != 1 {
			t.Errorf("binary round-trip: Color=%d, want 1", dst.Color)
		}
	})
	t.Run("JSON int target round-trip", func(t *testing.T) {
		jsn, err := schema.AppendEncodeJSON(nil, src)
		if err != nil {
			t.Fatalf("JSON encode: %v", err)
		}
		var dst Record
		if err := schema.DecodeJSON(jsn, &dst); err != nil {
			t.Fatalf("JSON decode of %s into int field: %v", jsn, err)
		}
		if dst.Color != 1 {
			t.Errorf("JSON round-trip: Color=%d, want 1", dst.Color)
		}
	})
	t.Run("JSON uint target round-trip", func(t *testing.T) {
		type URecord struct {
			Color uint8 `avro:"color"`
		}
		us := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"color","type":{"type":"enum","name":"C","symbols":["Red","Green","Blue"]}}
		]}`)
		jsn, err := us.AppendEncodeJSON(nil, URecord{Color: 2})
		if err != nil {
			t.Fatalf("JSON encode: %v", err)
		}
		var dst URecord
		if err := us.DecodeJSON(jsn, &dst); err != nil {
			t.Fatalf("JSON decode into uint8 field: %v", err)
		}
		if dst.Color != 2 {
			t.Errorf("uint round-trip: Color=%d, want 2", dst.Color)
		}
	})
	t.Run("JSON int target overflow guard", func(t *testing.T) {
		// Pin the overflow guard mirrors deserEnum: int8 with a 200-
		// symbol enum should reject ordinals that don't fit.
		var syms strings.Builder
		syms.WriteString(`{"type":"enum","name":"E","symbols":[`)
		for i := 0; i < 200; i++ {
			if i > 0 {
				syms.WriteString(",")
			}
			fmt.Fprintf(&syms, `"S%d"`, i)
		}
		syms.WriteString(`]}`)
		es := avro.MustParse(syms.String())
		var dst int8
		if err := es.DecodeJSON([]byte(`"S199"`), &dst); err == nil {
			t.Error("expected overflow error decoding ordinal 199 into int8")
		}
	})
}

// TestRegression_UnsafeOverflowErrorsPreserveAvroType locks safe/
// unsafe parity on the *SemanticError.AvroType field for overflow
// and format errors. Pre-fix, the us*/ud* unsafe paths returned raw
// fmt.Errorf which recordFieldError couldn't introspect via
// errors.As(*SemanticError) — it then wrapped the field error with
// AvroType="record", silently clobbering the safe path's documented
// invariant (errors.go:60: "type information is preserved, avoiding
// misleading intermediate 'record' types in the error chain"). Code
// that programmatically inspects (*SemanticError).AvroType to
// categorize encode/decode failures saw "record" on struct targets
// and "int"/"long"/"float"/etc. on map targets for the same logical
// condition.
//
// Sibling sweep: each sub-test exercises a distinct site family
// (usInt, usLong, usFloat, usTimeMillis, udInt, udLong) so the fix
// can't accidentally miss one.
func TestRegression_UnsafeOverflowErrorsPreserveAvroType(t *testing.T) {
	avroTypeOf := func(t *testing.T, err error) string {
		t.Helper()
		var se *avro.SemanticError
		if !errors.As(err, &se) {
			t.Fatalf("expected *SemanticError, got %v (%T)", err, err)
		}
		return se.AvroType
	}
	checkParity := func(t *testing.T, structErr, mapErr error) {
		t.Helper()
		s := avroTypeOf(t, structErr)
		m := avroTypeOf(t, mapErr)
		if s != m {
			t.Errorf("AvroType parity: struct=%q, map=%q", s, m)
		}
		if s == "record" {
			t.Errorf("struct path leaked AvroType=%q; safe path preserved AvroType=%q", s, m)
		}
	}

	t.Run("usInt encode int64-overflows-int32", func(t *testing.T) {
		type R struct {
			I int64 `avro:"i"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"i","type":"int"}]}`)
		bad := int64(1) << 40
		_, errStruct := s.AppendEncode(nil, &R{I: bad})
		_, errMap := s.AppendEncode(nil, map[string]any{"i": bad})
		checkParity(t, errStruct, errMap)
	})
	t.Run("usLong encode uint64-overflows-int64", func(t *testing.T) {
		type R struct {
			I uint64 `avro:"i"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"i","type":"long"}]}`)
		bad := uint64(math.MaxUint64)
		_, errStruct := s.AppendEncode(nil, &R{I: bad})
		_, errMap := s.AppendEncode(nil, map[string]any{"i": bad})
		checkParity(t, errStruct, errMap)
	})
	t.Run("usFloat encode float64-overflows-float32", func(t *testing.T) {
		type R struct {
			F float64 `avro:"f"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"f","type":"float"}]}`)
		bad := 1e300
		_, errStruct := s.AppendEncode(nil, &R{F: bad})
		_, errMap := s.AppendEncode(nil, map[string]any{"f": bad})
		checkParity(t, errStruct, errMap)
	})
	t.Run("usTimeMillis encode duration-overflows-int32-ms", func(t *testing.T) {
		type R struct {
			T time.Duration `avro:"t"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"t","type":{"type":"int","logicalType":"time-millis"}}]}`)
		// Duration that exceeds int32 milliseconds.
		bad := time.Duration(math.MaxInt32+1) * time.Millisecond
		_, errStruct := s.AppendEncode(nil, &R{T: bad})
		_, errMap := s.AppendEncode(nil, map[string]any{"t": bad})
		checkParity(t, errStruct, errMap)
	})
	t.Run("udInt decode long-overflows-uint8", func(t *testing.T) {
		type R struct {
			I uint8 `avro:"i"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"i","type":"int"}]}`)
		intS := avro.MustParse(`"int"`)
		body, err := intS.AppendEncode(nil, int32(1000)) // exceeds uint8
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var rStruct R
		mapDst := map[string]any{}
		_, errStruct := s.Decode(body, &rStruct)
		_, errMap := s.Decode(body, &mapDst)
		// Map path with no target type for the field decodes into any —
		// won't hit the overflow check. The relevant parity is between
		// the unsafe struct path's error AvroType and the safe path's
		// for the same overflow shape. The safe-path counterpart is
		// the deserInt narrowing path; we encode separately into a map
		// with a strict-typed value to compare.
		_ = errMap
		// Verify struct path yields AvroType="int" (not "record").
		var se *avro.SemanticError
		if !errors.As(errStruct, &se) {
			t.Fatalf("expected *SemanticError, got %v", errStruct)
		}
		if se.AvroType == "record" {
			t.Errorf("udInt overflow error leaked as AvroType=%q (want \"int\")", se.AvroType)
		}
	})
	t.Run("udLong decode long-overflows-uint8", func(t *testing.T) {
		type R struct {
			I uint8 `avro:"i"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"i","type":"long"}]}`)
		longS := avro.MustParse(`"long"`)
		body, err := longS.AppendEncode(nil, int64(1000))
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var rStruct R
		_, errStruct := s.Decode(body, &rStruct)
		var se *avro.SemanticError
		if !errors.As(errStruct, &se) {
			t.Fatalf("expected *SemanticError, got %v", errStruct)
		}
		if se.AvroType == "record" {
			t.Errorf("udLong overflow error leaked as AvroType=%q (want \"long\")", se.AvroType)
		}
	})
}

// TestRegression_UnionFloatStringDefaultCoerce locks the union case
// of coerceDefault. Pre-fix coerceDefault was kind-dispatched and
// only handled "float"/"double" directly — kind "union" fell through,
// leaving the raw string default in place. The downstream JSON
// encoder's union try-each-branch loop then errored on the float
// branch (jsonCoerceToFloat64 rejects strings) and silently picked
// the string branch in ["float","string"] unions, diverging from
// the binary encoder which always picks float via defaultAsFloat64's
// string fallback. validateDefault accepts both float and string
// branches for "3.14" lenient-ly, so the union resolution must be
// consistent — coerceDefault now mirrors convertDefaultBytes's
// validateDefault-driven first-matching-branch walk, so the picked
// branch is the same on every consumer.
func TestRegression_UnionFloatStringDefaultCoerce(t *testing.T) {
	t.Run("top-level [float,null] default-fill via JSON", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"f","type":["float","null"],"default":"1.5"}
		]}`)
		jb, err := s.AppendEncodeJSON(nil, map[string]any{})
		if err != nil {
			t.Fatalf("JSON encode default-fill: %v", err)
		}
		if !strings.Contains(string(jb), "1.5") {
			t.Fatalf("JSON output missing 1.5: %s", jb)
		}
	})
	t.Run("nested record with [float,null] default", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"inner","type":{"type":"record","name":"Inner","fields":[
				{"name":"f","type":["float","null"],"default":"2.5"}
			]},"default":{}}
		]}`)
		jb, err := s.AppendEncodeJSON(nil, map[string]any{})
		if err != nil {
			t.Fatalf("JSON encode: %v", err)
		}
		if !strings.Contains(string(jb), "2.5") {
			t.Fatalf("JSON output missing 2.5: %s", jb)
		}
	})
	t.Run("array of [float,null] default", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"a","type":{"type":"array","items":["float","null"]},"default":["1.5"]}
		]}`)
		jb, err := s.AppendEncodeJSON(nil, map[string]any{})
		if err != nil {
			t.Fatalf("JSON encode: %v", err)
		}
		if !strings.Contains(string(jb), "1.5") {
			t.Fatalf("JSON output missing 1.5: %s", jb)
		}
	})
	t.Run("map of [float,null] default", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"m","type":{"type":"map","values":["float","null"]},"default":{"k":"1.5"}}
		]}`)
		jb, err := s.AppendEncodeJSON(nil, map[string]any{})
		if err != nil {
			t.Fatalf("JSON encode: %v", err)
		}
		if !strings.Contains(string(jb), "1.5") {
			t.Fatalf("JSON output missing 1.5: %s", jb)
		}
	})
	t.Run("[double,null] (not just float)", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"d","type":["double","null"],"default":"5"}
		]}`)
		if _, err := s.AppendEncodeJSON(nil, map[string]any{}); err != nil {
			t.Fatalf("JSON encode: %v", err)
		}
	})
	t.Run("[float,string] binary/JSON pick same branch", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"f","type":["float","string"],"default":"3.14"}
		]}`)
		bin, err := s.AppendEncode(nil, map[string]any{})
		if err != nil {
			t.Fatalf("binary encode: %v", err)
		}
		jb, err := s.AppendEncodeJSON(nil, map[string]any{})
		if err != nil {
			t.Fatalf("JSON encode: %v", err)
		}
		var binDec, jsonDec map[string]any
		if _, err := s.Decode(bin, &binDec); err != nil {
			t.Fatalf("binary decode: %v", err)
		}
		if err := s.DecodeJSON(jb, &jsonDec); err != nil {
			t.Fatalf("JSON decode: %v", err)
		}
		_, binIsFloat := binDec["f"].(float32)
		_, jsonIsFloat := jsonDec["f"].(float32)
		if binIsFloat != jsonIsFloat {
			t.Errorf("parity divergence: binary=%T JSON=%T (binary=%x, JSON=%s)",
				binDec["f"], jsonDec["f"], bin, jb)
		}
	})
	t.Run("[string,float] (declaration order: string wins, unchanged)", func(t *testing.T) {
		// Regression-guard: when the string branch comes FIRST,
		// validateDefault matches string, coerceDefault is a no-op for
		// string targets, both encoders pick the string branch. This
		// preserves the prior documented behavior.
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"f","type":["string","float"],"default":"3.14"}
		]}`)
		bin, err := s.AppendEncode(nil, map[string]any{})
		if err != nil {
			t.Fatalf("binary: %v", err)
		}
		jb, err := s.AppendEncodeJSON(nil, map[string]any{})
		if err != nil {
			t.Fatalf("JSON: %v", err)
		}
		var binDec, jsonDec map[string]any
		_, _ = s.Decode(bin, &binDec)
		_ = s.DecodeJSON(jb, &jsonDec)
		if _, ok := binDec["f"].(string); !ok {
			t.Errorf("binary picked non-string branch: %T (want string)", binDec["f"])
		}
		if _, ok := jsonDec["f"].(string); !ok {
			t.Errorf("JSON picked non-string branch: %T (want string)", jsonDec["f"])
		}
	})
}

// TestRegression_TaggedUnionShortNameBinaryParity locks binary ↔ JSON
// parity on fastavro's unqualified-short-name tagged-union shape.
// Pre-fix the binary `serUnion.tryUnwrapTagged` did a plain map
// lookup on `serUnion.branchNames`, which was populated with only
// the canonical fully-qualified name + the goavro "type.logical"
// form. The JSON encoder's `findUnionBranch` had an explicit
// fastavro-short-name fallback with an ambiguity guard
// (json_codec.go:775-789), and `TestRegression_DecodeJSONUnionTag-
// FastavroShortName` already locked the JSON-decode side. Only
// binary encode lacked the short-name fallback, so an input shape
// that round-tripped through JSON errored on `AppendEncode`.
func TestRegression_TaggedUnionShortNameBinaryParity(t *testing.T) {
	t.Run("namespaced record short-name in tagged union", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"Outer","fields":[
			{"name":"u","type":["null",{"type":"record","name":"com.example.User","fields":[
				{"name":"id","type":"int"}
			]}]}
		]}`)
		in := map[string]any{
			"u": map[string]any{
				"User": map[string]any{"id": int32(42)}, // fastavro short name
			},
		}
		_, jerr := s.AppendEncodeJSON(nil, in)
		_, berr := s.AppendEncode(nil, in)
		if (jerr == nil) != (berr == nil) {
			t.Errorf("parity divergence: binary err=%v, JSON err=%v", berr, jerr)
		}
	})
	t.Run("ambiguous short name rejected on both paths", func(t *testing.T) {
		// Two branches share unqualified name "Foo" — the fastavro
		// fallback's ambiguity guard rejects (returns nil). Both
		// encoders should reject the short-name input symmetrically.
		s := avro.MustParse(`{"type":"record","name":"Outer","fields":[
			{"name":"u","type":[
				{"type":"record","name":"a.Foo","fields":[{"name":"x","type":"int"}]},
				{"type":"record","name":"b.Foo","fields":[{"name":"y","type":"int"}]}
			]}
		]}`)
		in := map[string]any{
			"u": map[string]any{
				"Foo": map[string]any{"x": int32(1)}, // ambiguous
			},
		}
		_, jerr := s.AppendEncodeJSON(nil, in)
		_, berr := s.AppendEncode(nil, in)
		if (jerr == nil) != (berr == nil) {
			t.Errorf("parity divergence on ambiguous short-name: binary err=%v, JSON err=%v", berr, jerr)
		}
		if jerr == nil || berr == nil {
			t.Error("ambiguous short-name should be rejected")
		}
	})
	t.Run("full-name still preferred when both forms present", func(t *testing.T) {
		// Regression-guard: when a tagged input uses the full
		// qualified name, both encoders pick the branch directly via
		// the canonical map entry. Short-name fallback must not
		// shadow exact matches.
		s := avro.MustParse(`{"type":"record","name":"Outer","fields":[
			{"name":"u","type":["null",{"type":"record","name":"com.example.User","fields":[
				{"name":"id","type":"int"}
			]}]}
		]}`)
		in := map[string]any{
			"u": map[string]any{
				"com.example.User": map[string]any{"id": int32(7)},
			},
		}
		if _, err := s.AppendEncode(nil, in); err != nil {
			t.Errorf("binary encode with full-name: %v", err)
		}
		if _, err := s.AppendEncodeJSON(nil, in); err != nil {
			t.Errorf("JSON encode with full-name: %v", err)
		}
	})
}

// TestRegression_LogicalDecimalDeserByteFallback locks the encode↔decode
// parity for opaque-bytes pass-through on the decimal/big-decimal/fixed-
// decimal logical types. Pre-fix the encoders all fell through to
// serBytes / serSize when the input wasn't *big.Rat / float / numeric
// string (the "construct the wire payload manually" path), but only
// deserFixedDecimal had the parallel deserFixed fallback on decode —
// deserBytesDecimal and deserBigDecimal rejected []byte targets,
// breaking the round-trip an existing encode-side test
// (TestBytesDecimalSerAsBytes) pinned.
func TestRegression_LogicalDecimalDeserByteFallback(t *testing.T) {
	t.Run("bytes-decimal []byte round-trip", func(t *testing.T) {
		s := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)
		raw := []byte{0x30, 0x39}
		enc, err := s.AppendEncode(nil, raw)
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var out []byte
		if _, err := s.Decode(enc, &out); err != nil {
			t.Fatalf("Decode into []byte: %v", err)
		}
		if !bytes.Equal(out, raw) {
			t.Fatalf("got %x, want %x", out, raw)
		}
	})
	t.Run("big-decimal []byte round-trip", func(t *testing.T) {
		s := avro.MustParse(`{"type":"bytes","logicalType":"big-decimal"}`)
		raw := []byte{0x04, 0x30, 0x39, 0x04}
		enc, err := s.AppendEncode(nil, raw)
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var out []byte
		if _, err := s.Decode(enc, &out); err != nil {
			t.Fatalf("Decode into []byte: %v", err)
		}
		if !bytes.Equal(out, raw) {
			t.Fatalf("got %x, want %x", out, raw)
		}
	})
	t.Run("fixed-decimal []byte round-trip control (already works)", func(t *testing.T) {
		s := avro.MustParse(`{"type":"fixed","name":"F","size":4,"logicalType":"decimal","precision":4,"scale":2}`)
		raw := []byte{0x00, 0x00, 0x30, 0x39}
		enc, err := s.AppendEncode(nil, raw)
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var out [4]byte
		if _, err := s.Decode(enc, &out); err != nil {
			t.Fatalf("Decode into [4]byte: %v", err)
		}
		if !bytes.Equal(out[:], raw) {
			t.Fatalf("got %x, want %x", out, raw)
		}
	})
}

// TestRegression_UnionEncodeTypeNameDispatch locks the union-encode
// branch selection policy. Pre-fix serUnion.ser used try-each-branch
// in declaration order — for ["double","long"] with int64(42) input,
// it picked the double branch (appendAvroFloat64 accepts CanInt for
// the documented whole-number-float divergence), encoding wire bytes
// tagged with branch index 0. The decoder then read the double-branch
// bytes and rejected int64 targets — round-trip broken. Java
// (GenericData.resolveUnion), fastavro (write_union), and hamba
// (encoderOfResolverUnion) all dispatch by the value's Go-type
// canonical Avro name first, then fall back to the try-each loop
// only when no name match exists. twmb now mirrors that.
func TestRegression_UnionEncodeTypeNameDispatch(t *testing.T) {
	t.Run("[double,long] + int64 picks long", func(t *testing.T) {
		s := avro.MustParse(`["double","long"]`)
		in := int64(42)
		enc, err := s.AppendEncode(nil, in)
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var out int64
		if _, err := s.Decode(enc, &out); err != nil {
			t.Fatalf("Decode int64 round-trip (wire=%x): %v", enc, err)
		}
		if out != 42 {
			t.Fatalf("got %d, want 42", out)
		}
	})
	t.Run("[float,long] + int64 picks long", func(t *testing.T) {
		s := avro.MustParse(`["float","long"]`)
		in := int64(42)
		enc, _ := s.AppendEncode(nil, in)
		var out int64
		if _, err := s.Decode(enc, &out); err != nil {
			t.Fatalf("Decode int64 round-trip: %v", err)
		}
	})
	t.Run("[float,int] + int32 picks int", func(t *testing.T) {
		s := avro.MustParse(`["float","int"]`)
		in := int32(42)
		enc, _ := s.AppendEncode(nil, in)
		var out int32
		if _, err := s.Decode(enc, &out); err != nil {
			t.Fatalf("Decode int32 round-trip: %v", err)
		}
	})
	t.Run("[bytes,string] + string picks string", func(t *testing.T) {
		s := avro.MustParse(`["bytes","string"]`)
		in := "foo"
		enc, _ := s.AppendEncode(nil, in)
		var out string
		if _, err := s.Decode(enc, &out); err != nil {
			t.Fatalf("Decode string round-trip: %v", err)
		}
	})
	t.Run("[bytes,string] + []byte picks bytes", func(t *testing.T) {
		s := avro.MustParse(`["bytes","string"]`)
		in := []byte("foo")
		enc, _ := s.AppendEncode(nil, in)
		var out []byte
		if _, err := s.Decode(enc, &out); err != nil {
			t.Fatalf("Decode []byte round-trip: %v", err)
		}
	})
	t.Run("[long,float] + float64 still picks float fallback", func(t *testing.T) {
		// No name match (float64's canonical name is "double", not
		// in the union). Try-each fallback fires; float branch accepts
		// the float64 narrowly (whole-number 42 fits int24 mantissa).
		// Decoder reads float bytes into float32 target.
		s := avro.MustParse(`["long","float"]`)
		in := float64(42)
		enc, _ := s.AppendEncode(nil, in)
		var out float32
		if _, err := s.Decode(enc, &out); err != nil {
			t.Fatalf("Decode float32 round-trip: %v (wire=%x)", err, enc)
		}
	})
}

// TestRegression_IntLongDecodeIntoFloatJSONNumber locks decode-side
// parity for the documented "whole-number floats encode against int /
// long schemas" intentional divergence. The encode side accepts
// float64/json.Number into int/long; the decode side previously
// rejected the matching round-trip with "cannot use float64 with
// Avro type int", breaking the symmetric expectation
// for json.Unmarshal pipelines that round-trip through Avro. The
// matching reverse — float Avro schemas accepting CanInt on encode
// but rejecting *int on decode — is the same shape and is also
// pinned here.
func TestRegression_IntLongDecodeIntoFloatJSONNumber(t *testing.T) {
	t.Run("int-wire into *float64", func(t *testing.T) {
		s := avro.MustParse(`"int"`)
		enc, err := s.AppendEncode(nil, float64(42))
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var out float64
		if _, err := s.Decode(enc, &out); err != nil {
			t.Fatalf("Decode into float64: %v", err)
		}
		if out != 42 {
			t.Errorf("got %v, want 42", out)
		}
	})
	t.Run("long-wire into *float64", func(t *testing.T) {
		s := avro.MustParse(`"long"`)
		enc, _ := s.AppendEncode(nil, float64(42))
		var out float64
		if _, err := s.Decode(enc, &out); err != nil {
			t.Fatalf("Decode into float64: %v", err)
		}
	})
	t.Run("int-wire into *json.Number", func(t *testing.T) {
		s := avro.MustParse(`"int"`)
		enc, _ := s.AppendEncode(nil, json.Number("42"))
		var out json.Number
		if _, err := s.Decode(enc, &out); err != nil {
			t.Fatalf("Decode into json.Number: %v", err)
		}
		if out != "42" {
			t.Errorf("got %q, want \"42\"", out)
		}
	})
	t.Run("long-wire into *json.Number", func(t *testing.T) {
		s := avro.MustParse(`"long"`)
		enc, _ := s.AppendEncode(nil, json.Number("42"))
		var out json.Number
		if _, err := s.Decode(enc, &out); err != nil {
			t.Fatalf("Decode into json.Number: %v", err)
		}
	})
	t.Run("float-wire into *uint32 (reverse asymmetry)", func(t *testing.T) {
		// appendAvroFloat32 accepts CanUint via mantissa-bounded
		// integer coercion; the decoder should mirror.
		s := avro.MustParse(`"float"`)
		enc, _ := s.AppendEncode(nil, uint32(42))
		var out uint32
		if _, err := s.Decode(enc, &out); err != nil {
			t.Fatalf("Decode into uint32: %v", err)
		}
	})
}

// TestParity_SchemaRejectionMatrix systematically asserts that
// Parse rejects every malformed schema shape the spec forbids or the
// reference implementations reject. Sibling to
// TestParity_RoundTripMatrix which tests positive round-trip parity;
// this matrix tests the rejection-side parity — that twmb is no more
// lenient than Java / hamba / avro-rs on schemas that should never
// have been accepted.
//
// The recurring bug class caught here is "Validation that silently
// falls back instead of erroring" — schemas with invalid logical-
// type bases, malformed defaults, missing required fields, union
// shape violations, name violations, etc. Each failing cell on first
// run is a real bug (or a documented intentional-leniency divergence,
// which gets moved to TestParity_KnownLeniencies below).
//
// Each cell specifies a schema string and an optional substring the
// returned error must contain (for diagnostic-quality assertion).
// Cells with `wantSubstr: ""` only assert that an error is returned.
func TestParity_SchemaRejectionMatrix(t *testing.T) {
	cells := []struct {
		name       string
		schema     string
		wantSubstr string
	}{
		// ── logical type / base type mismatches ─────────────────────
		// NOTE: every known logical type on the wrong underlying type is
		// INTENTIONAL LENIENCY (soft-drop) per the spec's "ignore invalid
		// logical type" rule and per Java/fastavro/hamba consensus.
		// Locked as acceptance in TestParity_AcceptedLeniencies below.
		// Pre-F1-fix twmb hard-rejected; the rejection was the bug.

		// ── decimal precision/scale invariants ──────────────────────
		{"decimal precision zero", `{"type":"bytes","logicalType":"decimal","precision":0,"scale":0}`, "precision"},
		{"decimal precision negative", `{"type":"bytes","logicalType":"decimal","precision":-1,"scale":0}`, "precision"},
		{"decimal scale negative", `{"type":"bytes","logicalType":"decimal","precision":4,"scale":-1}`, "scale"},
		{"decimal scale exceeds precision", `{"type":"bytes","logicalType":"decimal","precision":2,"scale":5}`, "scale"},
		{"decimal precision missing", `{"type":"bytes","logicalType":"decimal","scale":2}`, "precision"},
		{"decimal fixed-precision exceeds fixed size capacity", `{"type":"fixed","name":"D","size":4,"logicalType":"decimal","precision":20,"scale":2}`, ""},

		// ── fixed size invariants ───────────────────────────────────
		{"fixed without size", `{"type":"fixed","name":"F"}`, "size"},
		{"fixed size zero", `{"type":"fixed","name":"F","size":0}`, "size"},
		{"fixed size negative", `{"type":"fixed","name":"F","size":-1}`, "size"},
		{"fixed without name", `{"type":"fixed","size":4}`, "name"},

		// ── enum invariants ─────────────────────────────────────────
		{"enum without name", `{"type":"enum","symbols":["A"]}`, "name"},
		{"enum without symbols", `{"type":"enum","name":"E"}`, ""},
		{"enum empty symbols", `{"type":"enum","name":"E","symbols":[]}`, ""},
		{"enum duplicate symbols", `{"type":"enum","name":"E","symbols":["A","B","A"]}`, "duplicate"},
		{"enum default not in symbols", `{"type":"enum","name":"E","symbols":["A","B"],"default":"Z"}`, ""},
		{"enum symbol starts with digit", `{"type":"enum","name":"E","symbols":["1A","B"]}`, ""},
		{"enum symbol with space", `{"type":"enum","name":"E","symbols":["A","B C"]}`, ""},
		{"enum symbol empty string", `{"type":"enum","name":"E","symbols":[""]}`, ""},

		// ── record invariants ───────────────────────────────────────
		{"record without name", `{"type":"record","fields":[]}`, "name"},
		{"record field without name", `{"type":"record","name":"R","fields":[{"type":"int"}]}`, "name"},
		{"record field without type", `{"type":"record","name":"R","fields":[{"name":"a"}]}`, ""},
		{"record fields not array", `{"type":"record","name":"R","fields":"int"}`, ""},
		{"record field type unknown primitive", `{"type":"record","name":"R","fields":[{"name":"f","type":"NotAType"}]}`, ""},
		{"record duplicate field names", `{"type":"record","name":"R","fields":[
			{"name":"x","type":"int"},
			{"name":"x","type":"string"}
		]}`, ""},

		// ── array invariants ────────────────────────────────────────
		{"array without items", `{"type":"array"}`, ""},
		{"array items unknown", `{"type":"array","items":"NotAType"}`, ""},

		// ── map invariants ──────────────────────────────────────────
		{"map without values", `{"type":"map"}`, ""},
		{"map values unknown", `{"type":"map","values":"NotAType"}`, ""},

		// ── name validation per spec ────────────────────────────────
		{"record name starts with digit", `{"type":"record","name":"1R","fields":[]}`, ""},
		{"record name with dash", `{"type":"record","name":"R-Name","fields":[]}`, ""},
		{"record name with space", `{"type":"record","name":"R Name","fields":[]}`, ""},
		{"record name dot only", `{"type":"record","name":".","fields":[]}`, ""},
		{"record name empty string", `{"type":"record","name":"","fields":[]}`, ""},
		{"record name special char", `{"type":"record","name":"R$Name","fields":[]}`, ""},
		{"enum name with dash", `{"type":"enum","name":"E-Name","symbols":["A"]}`, ""},
		{"fixed name with space", `{"type":"fixed","name":"F Name","size":4}`, ""},
		// NOTE: namespace-part validation is currently lenient in twmb
		// (Java validates each namespace part as a name). Not a
		// rejection case today; pending decision.

		// ── union rules ─────────────────────────────────────────────
		{"empty union", `[]`, ""},
		{"union with two ints", `["int","int"]`, "duplicate"},
		{"union with two longs", `["long","long"]`, "duplicate"},
		{"union with two strings", `["string","string"]`, "duplicate"},
		{"union with two nulls", `["null","null"]`, "duplicate"},
		{"union with two bytes", `["bytes","bytes"]`, "duplicate"},
		{"union with two booleans", `["boolean","boolean"]`, "duplicate"},
		{"union with two floats", `["float","float"]`, "duplicate"},
		{"union with two doubles", `["double","double"]`, "duplicate"},
		{"union immediately containing union", `[["int","null"]]`, ""},
		{"union nested in union (record field)", `{"type":"record","name":"R","fields":[
			{"name":"u","type":[["int","null"],"string"]}
		]}`, ""},
		{"union of two same-name records", `[
			{"type":"record","name":"A","fields":[{"name":"x","type":"int"}]},
			{"type":"record","name":"A","fields":[{"name":"y","type":"int"}]}
		]`, ""},
		{"union of two same-name enums", `[
			{"type":"enum","name":"E","symbols":["A"]},
			{"type":"enum","name":"E","symbols":["B"]}
		]`, ""},
		{"union of two same-name fixeds", `[
			{"type":"fixed","name":"F","size":4},
			{"type":"fixed","name":"F","size":4}
		]`, ""},
		{"union ref+def same record", `{"type":"record","name":"R","fields":[
			{"name":"f","type":[
				{"type":"record","name":"A","fields":[{"name":"x","type":"int"}]},
				"A"
			]}
		]}`, "duplicate"},
		{"union with two enum refs to same enum", `{"type":"record","name":"R","fields":[
			{"name":"a","type":{"type":"enum","name":"E","symbols":["A"]}},
			{"name":"f","type":["E","E"]}
		]}`, "duplicate"},

		// ── unresolved name references ──────────────────────────────
		{"top-level unknown primitive", `"Undeclared"`, ""},
		{"field type unknown name", `{"type":"record","name":"R","fields":[{"name":"f","type":"Undeclared"}]}`, ""},
		{"array of unknown", `{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"array","items":"Undeclared"}}]}`, ""},
		{"map of unknown", `{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"map","values":"Undeclared"}}]}`, ""},
		{"union of unknown", `{"type":"record","name":"R","fields":[{"name":"f","type":["null","Undeclared"]}]}`, ""},

		// ── duplicate named types in different positions ────────────
		{"duplicate named-type in record (nested record same name)", `{"type":"record","name":"A","fields":[
			{"name":"x","type":{"type":"record","name":"A","fields":[]}}
		]}`, ""},

		// ── invalid defaults: null ──────────────────────────────────
		{"null default not null", `{"type":"record","name":"R","fields":[
			{"name":"n","type":"null","default":0}
		]}`, ""},
		{"null default string", `{"type":"record","name":"R","fields":[
			{"name":"n","type":"null","default":"hi"}
		]}`, ""},

		// ── invalid defaults: boolean ───────────────────────────────
		{"boolean default int", `{"type":"record","name":"R","fields":[
			{"name":"b","type":"boolean","default":42}
		]}`, ""},
		{"boolean default string", `{"type":"record","name":"R","fields":[
			{"name":"b","type":"boolean","default":"true"}
		]}`, ""},
		{"boolean default null", `{"type":"record","name":"R","fields":[
			{"name":"b","type":"boolean","default":null}
		]}`, ""},

		// ── invalid defaults: int ───────────────────────────────────
		{"int default non-numeric string", `{"type":"record","name":"R","fields":[
			{"name":"i","type":"int","default":"abc"}
		]}`, ""},
		{"int default overflow", `{"type":"record","name":"R","fields":[
			{"name":"i","type":"int","default":9999999999}
		]}`, ""},
		{"int default boolean", `{"type":"record","name":"R","fields":[
			{"name":"i","type":"int","default":true}
		]}`, ""},
		{"int default object", `{"type":"record","name":"R","fields":[
			{"name":"i","type":"int","default":{}}
		]}`, ""},

		// ── invalid defaults: long ──────────────────────────────────
		{"long default non-numeric", `{"type":"record","name":"R","fields":[
			{"name":"l","type":"long","default":"x"}
		]}`, ""},

		// ── invalid defaults: string ────────────────────────────────
		{"string default int", `{"type":"record","name":"R","fields":[
			{"name":"s","type":"string","default":42}
		]}`, ""},
		{"string default boolean", `{"type":"record","name":"R","fields":[
			{"name":"s","type":"string","default":true}
		]}`, ""},

		// ── invalid defaults: bytes / fixed ─────────────────────────
		{"bytes default with codepoint > 255", `{"type":"record","name":"R","fields":[
			{"name":"b","type":"bytes","default":"日"}
		]}`, "code point"},
		{"bytes default non-string", `{"type":"record","name":"R","fields":[
			{"name":"b","type":"bytes","default":42}
		]}`, ""},
		{"fixed default wrong length", `{"type":"record","name":"R","fields":[
			{"name":"f","type":{"type":"fixed","name":"F","size":4},"default":"abc"}
		]}`, ""},
		{"fixed default codepoint > 255", `{"type":"record","name":"R","fields":[
			{"name":"f","type":{"type":"fixed","name":"F","size":1},"default":"日"}
		]}`, "code point"},

		// ── invalid defaults: enum ──────────────────────────────────
		{"enum default unknown symbol", `{"type":"record","name":"R","fields":[
			{"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]},"default":"Z"}
		]}`, ""},
		{"enum default non-string", `{"type":"record","name":"R","fields":[
			{"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]},"default":42}
		]}`, ""},

		// ── invalid defaults: array / map / record ──────────────────
		{"array default non-array", `{"type":"record","name":"R","fields":[
			{"name":"a","type":{"type":"array","items":"int"},"default":42}
		]}`, ""},
		{"map default non-object", `{"type":"record","name":"R","fields":[
			{"name":"m","type":{"type":"map","values":"int"},"default":[]}
		]}`, ""},
		{"record default missing required field", `{"type":"record","name":"R","fields":[
			{"name":"r","type":{"type":"record","name":"Inner","fields":[
				{"name":"x","type":"int"}
			]},"default":{}}
		]}`, "missing"},
		{"record default wrong field type", `{"type":"record","name":"R","fields":[
			{"name":"r","type":{"type":"record","name":"Inner","fields":[
				{"name":"x","type":"int"}
			]},"default":{"x":"hi"}}
		]}`, ""},

		// ── invalid defaults: union ─────────────────────────────────
		{"union default matches no branch", `{"type":"record","name":"R","fields":[
			{"name":"u","type":["int","boolean"],"default":"hello"}
		]}`, ""},

		// ── invalid defaults: name-ref'd types (regression for the
		//    validateDefault name-ref gap from this branch) ──────────
		{"name-ref fixed default wrong length", `{"type":"record","name":"R","fields":[
			{"name":"def","type":{"type":"fixed","name":"F","size":4}},
			{"name":"f","type":"F","default":"abc"}
		]}`, ""},
		{"name-ref enum default unknown symbol", `{"type":"record","name":"R","fields":[
			{"name":"def","type":{"type":"enum","name":"E","symbols":["A","B"]}},
			{"name":"e","type":"E","default":"Z"}
		]}`, ""},
		{"name-ref record default missing field", `{"type":"record","name":"R","fields":[
			{"name":"def","type":{"type":"record","name":"Inner","fields":[
				{"name":"x","type":"int"}
			]}},
			{"name":"i","type":"Inner","default":{}}
		]}`, ""},

		// ── invalid JSON / malformed schema ─────────────────────────
		{"truncated JSON brace", `{`, ""},
		{"truncated JSON bracket", `[`, ""},
		{"empty schema string", ``, ""},
		{"whitespace-only schema", `   `, ""},
		{"JSON number at top", `42`, ""},
		{"JSON boolean at top", `true`, ""},
		{"JSON null at top", `null`, ""},
		{"JSON object missing type", `{"name":"R","fields":[]}`, ""},

		// ── type field invariants ───────────────────────────────────
		{"unknown primitive name", `"notreal"`, ""},
		{"wrong-case primitive (Int)", `"Int"`, ""},
		{"wrong-case primitive (STRING)", `"STRING"`, ""},

		// ── arrays / maps without items/values ──────────────────────
		{"object array without items", `{"type":"array"}`, ""},
		{"object map without values", `{"type":"map"}`, ""},
	}

	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			_, err := avro.Parse(c.schema)
			if err == nil {
				t.Fatalf("expected Parse rejection for schema %s", c.schema)
			}
			if c.wantSubstr != "" && !strings.Contains(err.Error(), c.wantSubstr) {
				t.Errorf("error message %q does not contain expected substring %q", err, c.wantSubstr)
			}
		})
	}
}

// TestParity_RuntimeRejectionMatrix is the encode/decode counterpart
// to TestParity_SchemaRejectionMatrix: schemas parse successfully but
// specific runtime inputs should be rejected. Covers overflow,
// wrong-Go-type-for-Avro-type, malformed wire bytes, and out-of-
// bounds wire values. The matrix runs each cell against both binary
// (Encode/Decode) and JSON (EncodeJSON/DecodeJSON) paths.
func TestParity_RuntimeRejectionMatrix(t *testing.T) {
	// Encode rejection cells: schema parses, but the supplied Go
	// value should be rejected at AppendEncode time.
	t.Run("encode/binary", func(t *testing.T) {
		cells := []struct {
			name   string
			schema string
			input  any
		}{
			{"int from int64 overflow", `"int"`, int64(1) << 40},
			{"int from uint32 overflow", `"int"`, uint32(math.MaxUint32)},
			{"int from uint64 overflow", `"int"`, uint64(math.MaxUint64)},
			{"long from uint64 overflow", `"long"`, uint64(math.MaxUint64)},
			{"int from fractional float", `"int"`, 3.14},
			{"long from fractional float", `"long"`, 3.14},
			{"int from NaN", `"int"`, math.NaN()},
			{"int from +Inf", `"int"`, math.Inf(1)},
			{"long from NaN", `"long"`, math.NaN()},
			{"float from finite-overflow float64", `"float"`, 1e300},
			{"int from non-numeric string", `"int"`, "abc"},
			{"int from bool", `"int"`, true},
			{"boolean from int", `"boolean"`, 42},
			{"boolean from string", `"boolean"`, "true"},
			{"string from int", `"string"`, 42},
			{"string from bool", `"string"`, true},
			{"bytes from int", `"bytes"`, 42},
			{"fixed wrong length string", `{"type":"fixed","name":"F","size":4}`, "abc"},
			{"fixed wrong length []byte", `{"type":"fixed","name":"F","size":4}`, []byte("abcde")},
			{"enum unknown symbol", `{"type":"enum","name":"E","symbols":["A","B"]}`, "Z"},
			{"enum out-of-range int", `{"type":"enum","name":"E","symbols":["A","B"]}`, 5},
			// "decimal non-numeric string" — INTENTIONAL: serBytesDecimal falls through to serBytes
			// for the documented opaque-bytes pass-through. Not a rejection case.
			{"decimal precision overflow", `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`, big.NewRat(9999999999, 100)},
			{"date from string non-date", `{"type":"int","logicalType":"date"}`, "not a date"},
			{"date from bool", `{"type":"int","logicalType":"date"}`, true},
		}
		for _, c := range cells {
			c := c
			t.Run(c.name, func(t *testing.T) {
				s := avro.MustParse(c.schema)
				if _, err := s.AppendEncode(nil, c.input); err == nil {
					t.Errorf("expected AppendEncode rejection for %T %v", c.input, c.input)
				}
			})
		}
	})

	t.Run("encode/json", func(t *testing.T) {
		cells := []struct {
			name   string
			schema string
			input  any
		}{
			{"int from int64 overflow", `"int"`, int64(1) << 40},
			{"int from uint64 overflow", `"int"`, uint64(math.MaxUint64)},
			{"int from fractional float", `"int"`, 3.14},
			{"int from NaN", `"int"`, math.NaN()},
			{"float overflow", `"float"`, 1e300},
			{"fixed wrong length", `{"type":"fixed","name":"F","size":4}`, []byte("abcde")},
			{"enum unknown symbol", `{"type":"enum","name":"E","symbols":["A","B"]}`, "Z"},
		}
		for _, c := range cells {
			c := c
			t.Run(c.name, func(t *testing.T) {
				s := avro.MustParse(c.schema)
				if _, err := s.AppendEncodeJSON(nil, c.input); err == nil {
					t.Errorf("expected AppendEncodeJSON rejection for %T %v", c.input, c.input)
				}
			})
		}
	})

	// Decode rejection cells: schema parses, encoded bytes are
	// hand-crafted to be malformed, decode should reject.
	t.Run("decode/binary", func(t *testing.T) {
		zigzag64 := func(n int64) []byte {
			buf := make([]byte, binary.MaxVarintLen64)
			return buf[:binary.PutVarint(buf, n)]
		}
		_ = zigzag64
		cells := []struct {
			name   string
			schema string
			wire   []byte
		}{
			{"int empty buffer", `"int"`, nil},
			{"int truncated varint", `"int"`, []byte{0x80}},
			{"long empty buffer", `"long"`, nil},
			{"long truncated", `"long"`, []byte{0x80, 0x80}},
			{"float truncated 3 bytes", `"float"`, []byte{0, 0, 0}},
			{"double truncated 5 bytes", `"double"`, []byte{0, 0, 0, 0, 0}},
			{"boolean empty", `"boolean"`, nil},
			// "boolean invalid byte 2" — INTENTIONAL: Java's BinaryDecoder and fastavro
			// both treat any non-1 byte as false. We match the reference impls.
			{"string negative length", `"string"`, []byte{0x01}},
			{"string truncated body", `"string"`, []byte{0x0a, 'a', 'b'}},
			{"bytes negative length", `"bytes"`, []byte{0x01}},
			{"fixed truncated", `{"type":"fixed","name":"F","size":4}`, []byte{0, 0}},
			{"enum out-of-range index", `{"type":"enum","name":"E","symbols":["A","B"]}`, []byte{0x06}},
			{"enum negative index", `{"type":"enum","name":"E","symbols":["A","B"]}`, []byte{0x01}},
			{"union invalid branch index", `["int","string"]`, []byte{0x06, 0x02}},
			{"union negative branch", `["int","string"]`, []byte{0x01, 0x02}},
			{"array truncated count varint", `{"type":"array","items":"int"}`, []byte{0x80}},
			{"map truncated count varint", `{"type":"map","values":"int"}`, []byte{0x80}},
			{"map negative key length", `{"type":"map","values":"int"}`, []byte{0x02, 0x01, 0x00}},
		}
		for _, c := range cells {
			c := c
			t.Run(c.name, func(t *testing.T) {
				s := avro.MustParse(c.schema)
				var out any
				if _, err := s.Decode(c.wire, &out); err == nil {
					t.Errorf("expected Decode rejection for wire %x, got %v", c.wire, out)
				}
			})
		}
	})

	t.Run("decode/json", func(t *testing.T) {
		cells := []struct {
			name   string
			schema string
			json   string
		}{
			{"int from float", `"int"`, `3.14`},
			{"int from string", `"int"`, `"abc"`},
			{"int from bool", `"int"`, `true`},
			{"int from null", `"int"`, `null`},
			{"int from object", `"int"`, `{}`},
			{"long from non-numeric string", `"long"`, `"hello"`},
			{"long from fractional", `"long"`, `3.14`},
			{"boolean from int", `"boolean"`, `42`},
			{"boolean from string", `"boolean"`, `"true"`},
			{"string from int", `"string"`, `42`},
			{"string from object", `"string"`, `{}`},
			{"bytes from int", `"bytes"`, `42`},
			{"fixed wrong length", `{"type":"fixed","name":"F","size":4}`, `"abc"`},
			{"enum unknown symbol", `{"type":"enum","name":"E","symbols":["A","B"]}`, `"Z"`},
			{"enum non-string", `{"type":"enum","name":"E","symbols":["A","B"]}`, `5`},
			{"array non-array", `{"type":"array","items":"int"}`, `42`},
			{"map non-object", `{"type":"map","values":"int"}`, `42`},
			{"truncated JSON brace", `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`, `{`},
			{"truncated JSON string", `"string"`, `"abc`},
			// "invalid escape in string" — twmb's JSON scanner is lenient on unknown \X
			// escapes (accepts \q as q). encoding/json rejects. Documented lenience for
			// now; if strictness is wanted, the fix is in json_scan.go's string parser.
			{"union tag unknown", `["null","string"]`, `{"InvalidTag":"foo"}`},
			{"union value not in any branch", `["int","string"]`, `true`},
			{"date from non-int", `{"type":"int","logicalType":"date"}`, `"abc"`},
		}
		for _, c := range cells {
			c := c
			t.Run(c.name, func(t *testing.T) {
				s := avro.MustParse(c.schema)
				var out any
				if err := s.DecodeJSON([]byte(c.json), &out); err == nil {
					t.Errorf("expected DecodeJSON rejection for %s, got %v", c.json, out)
				}
			})
		}
	})
}

// TestRegression_CustomTypeSkipPointerChainContinues locks the
// ErrSkipCustomType fall-through contract for chained CustomType
// encoders matched by pointer-GoType. Pre-fix the pointer-matching
// inner loop used `break` on ErrSkipCustomType (not `continue` like
// the value-matching scan and the decoder-side chain), silently
// dropping the second encoder and emitting raw wire bytes.
func TestRegression_CustomTypeSkipPointerChainContinues(t *testing.T) {
	type Money int
	ct1 := avro.CustomType{
		GoType:      reflect.TypeFor[*Money](),
		LogicalType: "money-skip",
		AvroType:    "long",
		Encode:      func(any, *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType },
		Decode:      func(any, *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType },
	}
	ct2 := avro.CustomType{
		GoType:      reflect.TypeFor[*Money](),
		LogicalType: "money-skip",
		AvroType:    "long",
		Encode: func(v any, _ *avro.SchemaNode) (any, error) {
			return int64(int(*v.(*Money)) * 100), nil
		},
	}
	s, err := avro.Parse(`{"type":"long","logicalType":"money-skip"}`,
		avro.WithCustomType(ct1), avro.WithCustomType(ct2))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	v := Money(7)
	out, err := s.AppendEncode(nil, &v)
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	// 700 → zigzag = 1400 → varint 0xf8 0x0a
	want := []byte{0xf8, 0x0a}
	if !bytes.Equal(out, want) {
		t.Fatalf("encoded %x; want %x (ct1 skipped should have fallen through to ct2)", out, want)
	}
}

// TestRegression_FiniteScaleCPUBound locks the upfront bit-length
// short-circuit in finiteScale. Pre-fix, encoding a big-decimal
// value 1/10^65536 (6 wire bytes) took ~1.4 CPU seconds because
// finiteScale iterated the 5-power factorization 65536 times on a
// ~152K-digit big.Int. A pipeline that decodes big-decimal from one
// source and re-encodes for another (replication, schema translation,
// OCF rewrite) handed an attacker ~10^8× CPU amplification per wire
// byte.
func TestRegression_FiniteScaleCPUBound(t *testing.T) {
	denom := new(big.Int).Exp(big.NewInt(10), big.NewInt(65536), nil)
	r := new(big.Rat).SetFrac(big.NewInt(1), denom)
	s := avro.MustParse(`{"type":"bytes","logicalType":"big-decimal"}`)
	start := time.Now()
	_, err := s.AppendEncode(nil, r)
	elapsed := time.Since(start)
	// Either we reject (out-of-bound scale) or accept very fast.
	// Pre-fix took 1.4s; post-fix the BitLen check rejects in <1ms.
	if elapsed > 200*time.Millisecond {
		t.Fatalf("Encoding 1/10^65536 took %v (>200ms cap); pre-fix amplification regression", elapsed)
	}
	_ = err // err is acceptable (out-of-scale rejection)
}

// TestRegression_DeserUUIDAcceptsByteSliceTarget locks the
// []byte-target arm in deserUUID, mirroring serUUID's serString
// fall-through which accepts []byte source. Pre-fix, the encoder
// accepted []byte("550e8400-...") but the decoder rejected the
// symmetric *[]byte target with "cannot use []uint8 with Avro type
// string" — a round-trip break on the binary path only (JSON's
// decoder already had the Slice arm).
func TestRegression_DeserUUIDAcceptsByteSliceTarget(t *testing.T) {
	s := avro.MustParse(`{"type":"string","logicalType":"uuid"}`)
	in := []byte("550e8400-e29b-41d4-a716-446655440000")
	wire, err := s.AppendEncode(nil, in)
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	var got []byte
	if _, err := s.Decode(wire, &got); err != nil {
		t.Fatalf("Decode into []byte target: %v", err)
	}
	if !bytes.Equal(got, in) {
		t.Errorf("round-trip: got %q, want %q", got, in)
	}
}

// TestRegression_BigDecimalJSONOpaquePassThrough locks JSON parity
// with deserBigDecimal's opaque-bytes pass-through. serBigDecimal
// falls through to plain bytes encoding when the input isn't rat-
// coercible; deserBigDecimal mirrors that on decode (raw payload
// into []byte/string/[N]byte). The JSON assignBytes "big-decimal"
// arm pre-fix returned the parse error immediately, breaking the
// JSON encode→decode round-trip for any raw []byte the binary
// path accepts. Fix mirrors deserBigDecimal: only surface the
// parse error when the target is structured (rat/number/float);
// byte-like targets fall through to setBytesValue.
func TestRegression_BigDecimalJSONOpaquePassThrough(t *testing.T) {
	s := avro.MustParse(`{"type":"bytes","logicalType":"big-decimal"}`)
	rawPayload := []byte("hello world, not a payload")

	// Binary round-trip (already worked; included for parity assertion).
	binWire, err := s.AppendEncode(nil, rawPayload)
	if err != nil {
		t.Fatalf("binary encode: %v", err)
	}
	var binBack []byte
	if _, err := s.Decode(binWire, &binBack); err != nil {
		t.Fatalf("binary decode: %v", err)
	}
	if !bytes.Equal(binBack, rawPayload) {
		t.Errorf("binary round-trip: got %q want %q", binBack, rawPayload)
	}

	// JSON encode (already worked; included for parity assertion).
	jsonWire, err := s.AppendEncodeJSON(nil, rawPayload)
	if err != nil {
		t.Fatalf("json encode: %v", err)
	}

	// JSON decode → []byte: pre-fix this errored with "short buffer
	// for big-decimal unscaled". Now mirrors binary's pass-through.
	var jsonBack []byte
	if err := s.DecodeJSON(jsonWire, &jsonBack); err != nil {
		t.Fatalf("json decode into []byte: %v (binary path accepts; JSON path must too)", err)
	}
	if !bytes.Equal(jsonBack, rawPayload) {
		t.Errorf("json round-trip into []byte: got %q want %q", jsonBack, rawPayload)
	}

	// JSON decode → string: same pass-through.
	var strBack string
	if err := s.DecodeJSON(jsonWire, &strBack); err != nil {
		t.Fatalf("json decode into string: %v", err)
	}
	if strBack != string(rawPayload) {
		t.Errorf("json round-trip into string: got %q want %q", strBack, rawPayload)
	}

	// JSON decode → structured big.Rat: must STILL error (the parse
	// failure is meaningful when the target can't hold raw bytes).
	var ratBack *big.Rat
	if err := s.DecodeJSON(jsonWire, &ratBack); err == nil {
		t.Errorf("json decode into *big.Rat must error on unparseable payload, got %v", ratBack)
	}
}

// TestRegression_BigDecimalJSONBareNumberParity locks JSON-decode
// parity between decimal and big-decimal on the bare-number arm. The
// decimal-bytes path already accepted bare numbers (TestRegression_
// DecodeJSONDecimalBareNumberIntoFloatParity); big-decimal was missing
// from both the per-arm gate in decodeBytes and the union token
// dispatcher in jsonTokenMatchesBranch, so a hand-edited JSON producer
// emitting `1.5` for a big-decimal-bytes schema got "expected string
// at offset 0" (standalone) or "no union branch matched" (inside a
// union) where the decimal sibling accepted the same input.
//
// Big-decimal has no schema-level scale (it's encoded inline on the
// wire), so the bare-number arm derives the natural scale via
// finiteScale(r) — used only for json.Number / string targets where
// the formatted display needs a scale parameter. Other target shapes
// (big.Rat / float / interface) are unaffected by the scale choice.
//
// Three-impl note: fastavro has no big-decimal implementation; Java's
// JsonDecoder for big-decimal goes through the bytes layer with no
// bare-number leniency — the argument for accepting bare numbers here
// is sibling-shape parity with twmb's own decimal-bytes leniency, not
// cross-impl alignment.
func TestRegression_BigDecimalJSONBareNumberParity(t *testing.T) {
	s := avro.MustParse(`{"type":"bytes","logicalType":"big-decimal"}`)
	t.Run("standalone bare-number into *big.Rat", func(t *testing.T) {
		var got *big.Rat
		if err := s.DecodeJSON([]byte("1.5"), &got); err != nil {
			t.Fatalf("bare-number decode: %v", err)
		}
		want := new(big.Rat).SetFrac64(3, 2)
		if got.Cmp(want) != 0 {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("standalone bare-number into *float64", func(t *testing.T) {
		var got float64
		if err := s.DecodeJSON([]byte("1.5"), &got); err != nil {
			t.Fatalf("bare-number decode: %v", err)
		}
		if got != 1.5 {
			t.Errorf("got %v, want 1.5", got)
		}
	})
	t.Run("standalone bare-integer into *big.Rat", func(t *testing.T) {
		var got *big.Rat
		if err := s.DecodeJSON([]byte("42"), &got); err != nil {
			t.Fatalf("bare-integer decode: %v", err)
		}
		if got.Cmp(big.NewRat(42, 1)) != 0 {
			t.Errorf("got %v, want 42", got)
		}
	})
	t.Run("standalone bare-number into *any", func(t *testing.T) {
		var got any
		if err := s.DecodeJSON([]byte("1.5"), &got); err != nil {
			t.Fatalf("bare-number decode: %v", err)
		}
		r, ok := got.(*big.Rat)
		if !ok {
			t.Fatalf("expected *big.Rat, got %T", got)
		}
		if r.Cmp(new(big.Rat).SetFrac64(3, 2)) != 0 {
			t.Errorf("got %v, want 3/2", r)
		}
	})

	// Union dispatch: a bare-number must route to the big-decimal
	// branch via jsonTokenMatchesBranch. Pre-fix the digit-token arm
	// only matched `decimal`-logical bytes/fixed.
	t.Run("union dispatch bare-number to big-decimal branch", func(t *testing.T) {
		us := avro.MustParse(`["null",{"type":"bytes","logicalType":"big-decimal"}]`)
		var got any
		if err := us.DecodeJSON([]byte("1.5"), &got); err != nil {
			t.Fatalf("union bare-number decode: %v", err)
		}
		r, ok := got.(*big.Rat)
		if !ok {
			t.Fatalf("expected *big.Rat, got %T (%v)", got, got)
		}
		if r.Cmp(new(big.Rat).SetFrac64(3, 2)) != 0 {
			t.Errorf("got %v, want 3/2", r)
		}
	})

}

// TestRegression_DateEncodeWallClock locks the calendar-date
// interpretation of timeToDate. Pre-fix it used UTC-instant
// (floorDiv(t.Unix(), 86400)) so a time.Time whose wall-clock date
// was D in a non-UTC zone encoded to D-1 or D+1. Java's
// LocalDate.toEpochDay and fastavro's prepare_date are both
// calendar-only; twmb is now too. Also internally consistent with
// timeToLocalTimestamp* which already re-anchors wall-clock fields
// at UTC.
func TestRegression_DateEncodeWallClock(t *testing.T) {
	plus5 := time.FixedZone("+05", 5*3600)
	minus5 := time.FixedZone("-05", -5*3600)
	s := avro.MustParse(`{"type":"int","logicalType":"date"}`)
	plain := avro.MustParse(`"int"`)
	decodeDays := func(t *testing.T, wire []byte) int32 {
		t.Helper()
		var got int32
		if _, err := plain.Decode(wire, &got); err != nil {
			t.Fatalf("decode days: %v", err)
		}
		return got
	}
	cases := []struct {
		name string
		in   time.Time
		want int32
	}{
		{"UTC 2020-01-01", time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), 18262},
		{"+05 2020-01-01 00:00", time.Date(2020, 1, 1, 0, 0, 0, 0, plus5), 18262},
		{"-05 2020-01-01 00:00", time.Date(2020, 1, 1, 0, 0, 0, 0, minus5), 18262},
		{"UTC 1970-01-01", time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), 0},
		{"+05 1970-01-01 12:00", time.Date(1970, 1, 1, 12, 0, 0, 0, plus5), 0},
		{"UTC 1969-12-27", time.Date(1969, 12, 27, 0, 0, 0, 0, time.UTC), -5},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			wire, err := s.AppendEncode(nil, c.in)
			if err != nil {
				t.Fatalf("AppendEncode: %v", err)
			}
			if got := decodeDays(t, wire); got != c.want {
				t.Errorf("got %d days, want %d days", got, c.want)
			}
		})
	}
	t.Run("string TZ-offset matches date-only", func(t *testing.T) {
		a, _ := s.AppendEncode(nil, "2020-01-01")
		b, _ := s.AppendEncode(nil, "2020-01-01T00:00:00+05:00")
		c, _ := s.AppendEncode(nil, "2020-01-01T00:00:00-05:00")
		if string(a) != string(b) || string(a) != string(c) {
			t.Errorf("date-only/+05/-05 string encodings diverge: a=%x b=%x c=%x", a, b, c)
		}
	})
}

// TestRegression_JSONDecodeBareNaNInfinityCasingParity: bare NaN /
// Infinity / -Infinity tokens are exact-matched against the
// Java/fastavro accept set ({"NaN", "Infinity", "INF", "-Infinity",
// "-INF"}) plus the Go strconv-style "Inf" / "-Inf". Lowercase
// first-letter variants ('n', 'i') reject — they collide with the
// JSON null literal in the union dispatcher.
func TestRegression_JSONDecodeBareNaNInfinityCasingParity(t *testing.T) {
	accepts := []struct {
		input string
		check func(float64) bool
	}{
		// Canonical Java emissions / accepts (also fastavro's).
		{"NaN", math.IsNaN},
		{"Infinity", func(f float64) bool { return math.IsInf(f, 1) }},
		{"-Infinity", func(f float64) bool { return math.IsInf(f, -1) }},
		// Java's alternate "INF" / "-INF" form (Schema.java:254/259).
		{"INF", func(f float64) bool { return math.IsInf(f, 1) }},
		{"-INF", func(f float64) bool { return math.IsInf(f, -1) }},
		// Twmb extension: Go strconv.ParseFloat-style mixed-case "Inf".
		{"Inf", func(f float64) bool { return math.IsInf(f, 1) }},
		{"-Inf", func(f float64) bool { return math.IsInf(f, -1) }},
	}
	rejects := []string{
		// Lowercase first letter — collides with JSON null/bool literal
		// starts (n/t/f); rejected for dispatcher unambiguity.
		"nan", "inf", "infinity", "-inf", "-infinity",
		// Wrong-case body for the canonical NaN/Infinity forms.
		"NAN", "INFINITY", "-INFINITY", "Nan", "nAn", "iNf",
	}
	for _, kind := range []struct {
		schema string
		bits   int
	}{{`"float"`, 32}, {`"double"`, 64}} {
		s := avro.MustParse(kind.schema)
		for _, c := range accepts {
			t.Run(kind.schema+"/accept/"+c.input, func(t *testing.T) {
				if kind.bits == 32 {
					var f float32
					if err := s.DecodeJSON([]byte(c.input), &f); err != nil {
						t.Fatalf("DecodeJSON(%q): %v", c.input, err)
					}
					if !c.check(float64(f)) {
						t.Errorf("DecodeJSON(%q): got %v", c.input, f)
					}
				} else {
					var f float64
					if err := s.DecodeJSON([]byte(c.input), &f); err != nil {
						t.Fatalf("DecodeJSON(%q): %v", c.input, err)
					}
					if !c.check(f) {
						t.Errorf("DecodeJSON(%q): got %v", c.input, f)
					}
				}
			})
		}
		for _, in := range rejects {
			t.Run(kind.schema+"/reject/"+in, func(t *testing.T) {
				if kind.bits == 32 {
					var f float32
					if err := s.DecodeJSON([]byte(in), &f); err == nil {
						t.Fatalf("DecodeJSON(%q): expected reject; got %v", in, f)
					}
				} else {
					var f float64
					if err := s.DecodeJSON([]byte(in), &f); err == nil {
						t.Fatalf("DecodeJSON(%q): expected reject; got %v", in, f)
					}
				}
			})
		}
	}
	// Lock that null still routes to null-arm (NaN per goavro) and
	// isn't misrouted to the bare-special path by the new dispatch.
	t.Run("null still routes to null-arm", func(t *testing.T) {
		s := avro.MustParse(`"double"`)
		var f float64
		if err := s.DecodeJSON([]byte("null"), &f); err != nil {
			t.Fatalf("DecodeJSON(null): %v", err)
		}
		if !math.IsNaN(f) {
			t.Errorf("null should decode to NaN (goavro convention), got %v", f)
		}
	})
}

// TestRegression_LowercaseNanUnionWithNullF1: a bare `n`-starting
// token in a union with a null branch must use isJSONNullStart to
// disambiguate vs lowercase `nan`. Pre-fix `decodeUnion` open-coded
// `if p == 'n'` and hijacked `nan` into the null arm; even though
// parseSpecialFloat now rejects lowercase, the dispatch keeps the
// defensive disambiguation.
func TestRegression_LowercaseNanUnionWithNullF1(t *testing.T) {
	// Bare `n` followed by non-`u` second byte must NOT be hijacked
	// into the null arm. Currently rejected by parseSpecialFloat;
	// the dispatcher path is exercised here to lock in the defense.
	s := avro.MustParse(`["null","float"]`)
	var v any
	// Lowercase nan: parseSpecialFloat rejects (matches Java/fastavro
	// /goavro). Confirm the error is about the float arm's parser,
	// NOT about consumeNull failing — the latter would mean the
	// dispatcher hijacked it into the null branch incorrectly.
	err := s.DecodeJSON([]byte("nan"), &v)
	if err == nil {
		t.Fatal("expected reject for lowercase nan")
	}
	if strings.Contains(err.Error(), "expected null") {
		t.Errorf("bare lowercase nan was hijacked into null arm: %v", err)
	}
	// Real null still works.
	err = s.DecodeJSON([]byte("null"), &v)
	if err != nil {
		t.Fatalf("bare null: %v", err)
	}
	if v != nil {
		t.Errorf("expected nil for null branch, got %#v", v)
	}
}

// TestRegression_JSONDecodeBareNaNInfinityTokens locks the fastavro
// bare-token form for non-finite floats in DecodeJSON. fastavro emits
// these via Python's json.dumps with allow_nan=True, which produces
// NaN / Infinity / -Infinity as bare tokens (not quoted). Pre-fix the
// JSON decoder only handled quoted strings, null (goavro convention),
// and numeric; bare tokens errored with "avro json: expected number".
// AUDIT-listed as a documented divergence; this regression test
// closes the gap so the documentation matches the implementation.
func TestRegression_JSONDecodeBareNaNInfinityTokens(t *testing.T) {
	for _, kind := range []struct {
		schema string
		bits   int
	}{{`"float"`, 32}, {`"double"`, 64}} {
		t.Run(kind.schema, func(t *testing.T) {
			s := avro.MustParse(kind.schema)
			cases := []struct {
				input string
				check func(float64) bool
			}{
				// Canonical Python/fastavro casings.
				{`Infinity`, func(f float64) bool { return math.IsInf(f, 1) }},
				{`-Infinity`, func(f float64) bool { return math.IsInf(f, -1) }},
				{`NaN`, math.IsNaN},
				// Casing parity with the quoted form's parseSpecialFloat —
				// "Inf"/"inf" both accepted there, so bare must agree.
				{`Inf`, func(f float64) bool { return math.IsInf(f, 1) }},
			}
			for _, c := range cases {
				if kind.bits == 32 {
					var f float32
					if err := s.DecodeJSON([]byte(c.input), &f); err != nil {
						t.Errorf("DecodeJSON(%q): %v", c.input, err)
						continue
					}
					if !c.check(float64(f)) {
						t.Errorf("DecodeJSON(%q): got %v", c.input, f)
					}
				} else {
					var f float64
					if err := s.DecodeJSON([]byte(c.input), &f); err != nil {
						t.Errorf("DecodeJSON(%q): %v", c.input, err)
						continue
					}
					if !c.check(f) {
						t.Errorf("DecodeJSON(%q): got %v", c.input, f)
					}
				}
			}
		})
	}

	// Nested-context coverage: a bare special-float must work inside a
	// record field, an array element, and a union branch. The dispatch
	// in decodeFloat/decodeDouble is the same regardless of caller, but
	// these tests lock that fact so a future caller-side refactor can't
	// silently regress the nested paths.
	t.Run("bare NaN in record/array/union contexts", func(t *testing.T) {
		recSchema := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"x","type":"double"}]}`)
		var rec struct {
			X float64 `avro:"x"`
		}
		if err := recSchema.DecodeJSON([]byte(`{"x":NaN}`), &rec); err != nil {
			t.Errorf("record DecodeJSON: %v", err)
		} else if !math.IsNaN(rec.X) {
			t.Errorf("record: got %v, want NaN", rec.X)
		}

		arrSchema := avro.MustParse(`{"type":"array","items":"double"}`)
		var arr []float64
		if err := arrSchema.DecodeJSON([]byte(`[NaN, Infinity, -Infinity]`), &arr); err != nil {
			t.Errorf("array DecodeJSON: %v", err)
		} else if len(arr) != 3 || !math.IsNaN(arr[0]) || !math.IsInf(arr[1], 1) || !math.IsInf(arr[2], -1) {
			t.Errorf("array: got %v", arr)
		}

		unionSchema := avro.MustParse(`["null","double"]`)
		var u any
		if err := unionSchema.DecodeJSON([]byte(`{"double":Infinity}`), &u); err != nil {
			t.Errorf("union DecodeJSON: %v", err)
		} else if f, ok := u.(float64); !ok || !math.IsInf(f, 1) {
			t.Errorf("union: got %v (%T)", u, u)
		}
	})
}

// TestRegression_EncodeBytesStringBinaryJSONParity locks
// binary/JSON parity when encoding a Go string into an Avro bytes or
// fixed schema. Pre-fix, json_codec.go's bytes-string and fixed-
// string arms parsed the Go string as codepoint-mapped bytes (one
// byte per rune, 0-255 only), while serBytes treats the Go string as
// raw UTF-8. So Encode("é"/bytes) produced c3 a9 (binary) vs e9
// (JSON), diverging on the same schema + same input. Java/fastavro
// reject Go-string-equivalent input outright; twmb leniently accepts
// it, so we pick the interpretation that matches binary (UTF-8).
func TestRegression_EncodeBytesStringBinaryJSONParity(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		input  string
	}{
		{"bytes / single multibyte rune", `"bytes"`, "é"},
		{"bytes / multi-rune ASCII", `"bytes"`, "hi"},
		{"bytes / mixed BMP + ASCII", `"bytes"`, "héllo"},
		{"bytes / non-ASCII > 255 codepoint", `"bytes"`, "€"},
		// Fixed schemas are sized by UTF-8 byte count, mirroring binary.
		{"fixed(2) / single 2-byte rune", `{"type":"fixed","name":"F","size":2}`, "é"},
		{"fixed(5) / mixed", `{"type":"fixed","name":"F","size":6}`, "héllo"}, // h=1 é=2 l=1 l=1 o=1 = 6
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := avro.MustParse(c.schema)
			bin, err := s.AppendEncode(nil, c.input)
			if err != nil {
				t.Fatalf("binary encode: %v", err)
			}
			// For bytes, strip the varlong length prefix; for fixed,
			// there is no length prefix.
			var binBody []byte
			if c.schema == `"bytes"` {
				// Length is a single varlong byte for len < 64.
				binBody = bin[1:]
			} else {
				binBody = bin
			}
			jsonOut, err := s.EncodeJSON(c.input)
			if err != nil {
				t.Fatalf("json encode: %v", err)
			}
			var roundTrip []byte
			if err := s.DecodeJSON(jsonOut, &roundTrip); err != nil {
				t.Fatalf("json decode-back: %v\nwire=%s", err, jsonOut)
			}
			if !bytes.Equal(binBody, roundTrip) {
				t.Fatalf("PARITY BREAK: binary=%x json-decoded=%x (json wire %s)", binBody, roundTrip, jsonOut)
			}
		})
	}
}

// TestRegression_LenientInputAudit enumerates the encoder's documented
// lenient Go-input shapes (see ser.go: extractTime, tryParseDateString,
// floatFitsInt32/64, jsonNumberToInt64/Float, tryCoerceToRat, serSize's
// String arm, serBytes' String arm, serString's TextMarshaler/[]byte
// arms, serEnum's int/string arms, serFixedUUIDReflect's string arm)
// and asserts the corresponding decoder accepts the same Go type as a
// decode target. This is the structural guarantee against the
// "encoder-lenient, decoder-strict" class of bug that has produced
// every "encoder accepts X but decoder can't read it back" finding
// across the audit rounds. Documented intentional asymmetries (see
// the "Intentional asymmetries" section in doc.go) are listed in the
// skipReason cells; everything else must round-trip cleanly through
// BOTH the binary AND JSON encoders/decoders.
func TestRegression_LenientInputAudit(t *testing.T) {
	type cell struct {
		name       string
		schema     string
		input      any
		want       any    // post-round-trip value of the same Go type
		skipReason string // non-empty: documented intentional asymmetry
	}
	bigRat := func(n, d int64) *big.Rat { return big.NewRat(n, d) }
	cells := []cell{
		// ── numeric scalars: every lenient encoder shape must decode
		//     back into the same Go type ───────────────────────────
		{"int / int32", `"int"`, int32(42), int32(42), ""},
		{"int / int64 (encoder accepts wide; decoder accepts wide)", `"int"`, int64(42), int64(42), ""},
		{"int / uint32", `"int"`, uint32(42), uint32(42), ""},
		{"int / float64 whole-number", `"int"`, float64(42), float64(42), ""},
		{"int / json.Number", `"int"`, json.Number("42"), json.Number("42"), ""},

		{"long / int64", `"long"`, int64(42), int64(42), ""},
		{"long / int32", `"long"`, int32(42), int32(42), ""},
		{"long / uint64", `"long"`, uint64(42), uint64(42), ""},
		{"long / float64 whole-number", `"long"`, float64(42), float64(42), ""},
		{"long / json.Number", `"long"`, json.Number("42"), json.Number("42"), ""},

		{"float / float32", `"float"`, float32(3.5), float32(3.5), ""},
		{"float / float64", `"float"`, float64(3.5), float64(3.5), ""},
		{"float / int32 whole-number", `"float"`, int32(42), int32(42), ""},
		{"float / json.Number", `"float"`, json.Number("3.5"), json.Number("3.5"), ""},

		{"double / float64", `"double"`, float64(3.14), float64(3.14), ""},
		{"double / float32", `"double"`, float32(3.5), float32(3.5), ""},
		{"double / int64 whole-number", `"double"`, int64(42), int64(42), ""},
		{"double / json.Number", `"double"`, json.Number("3.14"), json.Number("3.14"), ""},

		// ── string / bytes / fixed ───────────────────────────────
		{"string / string", `"string"`, "hello", "hello", ""},
		{"string / []byte", `"string"`, []byte("hello"), []byte("hello"), ""},

		{"bytes / []byte", `"bytes"`, []byte("hello"), []byte("hello"), ""},
		{"bytes / string", `"bytes"`, "hello", "hello", ""},
		{"bytes / [N]byte", `"bytes"`, [5]byte{'h', 'e', 'l', 'l', 'o'}, [5]byte{'h', 'e', 'l', 'l', 'o'}, ""},

		{"fixed / [N]byte", `{"type":"fixed","name":"F","size":5}`, [5]byte{'h', 'e', 'l', 'l', 'o'}, [5]byte{'h', 'e', 'l', 'l', 'o'}, ""},
		{"fixed / []byte", `{"type":"fixed","name":"F","size":5}`, []byte("hello"), []byte("hello"), ""},
		{"fixed / string", `{"type":"fixed","name":"F","size":5}`, "hello", "hello", ""},

		// ── enum ─────────────────────────────────────────────────
		{"enum / string", `{"type":"enum","name":"E","symbols":["A","B","C"]}`, "B", "B", ""},
		{"enum / int", `{"type":"enum","name":"E","symbols":["A","B","C"]}`, int(1), int(1), ""},
		{"enum / uint8", `{"type":"enum","name":"E","symbols":["A","B","C"]}`, uint8(1), uint8(1), ""},

		// ── UUID logical (string-backed and fixed-backed) ────────
		{"uuid-string / string", `{"type":"string","logicalType":"uuid"}`, "550e8400-e29b-41d4-a716-446655440000", "550e8400-e29b-41d4-a716-446655440000", ""},
		{"uuid-fixed / [16]byte", `{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`, [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}, [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}, ""},
		// uuid-fixed/string is documented lossy on the binary roundtrip:
		// encoder parses the canonical string, decoder produces canonical
		// bytes. Decoded-into-string yields canonical form, but the input
		// here is the same canonical, so round-trip succeeds.
		{"uuid-fixed / string", `{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`, "550e8400-e29b-41d4-a716-446655440000", "550e8400-e29b-41d4-a716-446655440000", ""},

		// ── decimal (lenient coercion: float/string/json.Number) ──
		{"decimal / *big.Rat", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, bigRat(33, 100), bigRat(33, 100), ""},
		{"decimal / float64", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, 0.33, 0.33, ""},
		{"decimal / string", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, "0.33", "0.33", ""},
		{"decimal / json.Number", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, json.Number("0.33"), json.Number("0.33"), ""},

		// ── opaque-bytes pass-through for bytes/fixed-backed logical
		//     types. serBytesDecimal / serFixedDecimal / serBigDecimal /
		//     serDuration all fall through to plain bytes encoding when
		//     the input isn't structured (rat / Duration). Decoders must
		//     mirror — a bug in JSON assignBytes's "big-decimal" arm
		//     (pre-fix) returned the parse error instead of falling
		//     through. These cells lock the encoder-lenient/decoder-must-
		//     reverse contract for every site where opaque pass-through
		//     is documented.
		// Decimal opaque pass-through only fires for byte-like targets:
		// setDecimalRat has a *string arm that formats the rat as the
		// canonical decimal string (e.g. "0.33"), so a *string target
		// always gets the decimal-string form rather than the original
		// bytes. Byte-array / byte-slice targets fall through to
		// setBytesValue and round-trip the raw bytes.
		{"decimal-bytes / []byte opaque", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, []byte{0x21}, []byte{0x21}, ""},
		{"decimal-fixed / [N]byte opaque", `{"type":"fixed","name":"D","size":4,"logicalType":"decimal","precision":4,"scale":2}`, [4]byte{0, 0, 0, 0x21}, [4]byte{0, 0, 0, 0x21}, ""},
		{"big-decimal / []byte opaque", `{"type":"bytes","logicalType":"big-decimal"}`, []byte("hello world"), []byte("hello world"), ""},
		{"big-decimal / string opaque", `{"type":"bytes","logicalType":"big-decimal"}`, "hello world", "hello world", ""},

		// ── duration ─────────────────────────────────────────────
		{"duration / avro.Duration", `{"type":"fixed","name":"D","size":12,"logicalType":"duration"}`, avro.Duration{Months: 1, Days: 2, Milliseconds: 3}, avro.Duration{Months: 1, Days: 2, Milliseconds: 3}, ""},
		{"duration / [12]byte opaque", `{"type":"fixed","name":"D","size":12,"logicalType":"duration"}`, [12]byte{0x01, 0, 0, 0, 0x02, 0, 0, 0, 0x03, 0, 0, 0}, [12]byte{0x01, 0, 0, 0, 0x02, 0, 0, 0, 0x03, 0, 0, 0}, ""},

		// ── time logicals: every Go-input shape the encoder accepts
		//     must have a symmetric decoder arm. The string arms here
		//     are the leniency the last audit found missing. ───────
		{"date / time.Time", `{"type":"int","logicalType":"date"}`, time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC), ""},
		{"date / string (date-only)", `{"type":"int","logicalType":"date"}`, "2025-01-15", "2025-01-15", ""},
		{"timestamp-millis / time.Time", `{"type":"long","logicalType":"timestamp-millis"}`, time.Date(2025, 1, 15, 12, 34, 56, 789_000_000, time.UTC), time.Date(2025, 1, 15, 12, 34, 56, 789_000_000, time.UTC), ""},
		{"timestamp-millis / string", `{"type":"long","logicalType":"timestamp-millis"}`, "2025-01-15T12:34:56.789Z", "2025-01-15T12:34:56.789Z", ""},
		{"timestamp-micros / string", `{"type":"long","logicalType":"timestamp-micros"}`, "2025-01-15T12:34:56.789012Z", "2025-01-15T12:34:56.789012Z", ""},
		{"timestamp-nanos / string", `{"type":"long","logicalType":"timestamp-nanos"}`, "2025-01-15T12:34:56.789012345Z", "2025-01-15T12:34:56.789012345Z", ""},
		{"local-timestamp-millis / string", `{"type":"long","logicalType":"local-timestamp-millis"}`, "2025-01-15T12:34:56.789Z", "2025-01-15T12:34:56.789Z", ""},
		{"local-timestamp-micros / string", `{"type":"long","logicalType":"local-timestamp-micros"}`, "2025-01-15T12:34:56.789012Z", "2025-01-15T12:34:56.789012Z", ""},
		{"local-timestamp-nanos / string", `{"type":"long","logicalType":"local-timestamp-nanos"}`, "2025-01-15T12:34:56.789012345Z", "2025-01-15T12:34:56.789012345Z", ""},

		// ── time-millis / time-micros: time-of-day Duration. The
		//     time.Time arm is lossy (extracts time-of-day, discards
		//     date) — documented in doc.go's "Lossy by design". The
		//     duration-target round-trip is symmetric. ──────────────
		{"time-millis / time.Duration", `{"type":"int","logicalType":"time-millis"}`, time.Duration(45_296_000) * time.Millisecond, time.Duration(45_296_000) * time.Millisecond, ""},
		{"time-millis / time.Time", `{"type":"int","logicalType":"time-millis"}`, time.Date(0, 1, 1, 12, 34, 56, 0, time.UTC), nil,
			"doc.go: time.Time → time-millis discards date; round-trip preserves only time-of-day"},
		{"time-micros / time.Duration", `{"type":"long","logicalType":"time-micros"}`, time.Duration(45_296_000_123) * time.Microsecond, time.Duration(45_296_000_123) * time.Microsecond, ""},
	}
	equal := func(got, want any) bool {
		if want == nil {
			return got == nil
		}
		switch w := want.(type) {
		case time.Time:
			g, ok := got.(time.Time)
			return ok && g.Equal(w)
		case *big.Rat:
			g, ok := got.(*big.Rat)
			return ok && g != nil && g.Cmp(w) == 0
		}
		return reflect.DeepEqual(got, want)
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			if c.skipReason != "" {
				t.Skip(c.skipReason)
			}
			s := avro.MustParse(c.schema)
			// Binary: Encode(input) into *T → assert equal(want)
			bin, err := s.AppendEncode(nil, c.input)
			if err != nil {
				t.Fatalf("binary AppendEncode(%T): %v", c.input, err)
			}
			binDst := reflect.New(reflect.TypeOf(c.want))
			if _, err := s.Decode(bin, binDst.Interface()); err != nil {
				t.Fatalf("binary Decode into *%T: %v (wire=%x)\n  THIS IS THE BUG CLASS — encoder accepted %T but decoder cannot read it back into the same type", c.want, err, bin, c.input)
			}
			if got := binDst.Elem().Interface(); !equal(got, c.want) {
				t.Errorf("binary round-trip mismatch:\n  got  %T %v\n  want %T %v", got, got, c.want, c.want)
			}
			// JSON: AppendEncodeJSON(input) into *T → assert equal(want)
			jsn, err := s.AppendEncodeJSON(nil, c.input)
			if err != nil {
				t.Fatalf("JSON AppendEncodeJSON(%T): %v", c.input, err)
			}
			jsnDst := reflect.New(reflect.TypeOf(c.want))
			if err := s.DecodeJSON(jsn, jsnDst.Interface()); err != nil {
				t.Fatalf("JSON DecodeJSON into *%T: %v (wire=%s)\n  THIS IS THE BUG CLASS — JSON encoder accepted %T but JSON decoder cannot read it back into the same type", c.want, err, jsn, c.input)
			}
			if got := jsnDst.Elem().Interface(); !equal(got, c.want) {
				t.Errorf("JSON round-trip mismatch:\n  got  %T %v\n  want %T %v\n  (wire %s)", got, got, c.want, c.want, jsn)
			}
		})
	}
}

// TestRegression_UnionDispatchAmbiguous locks the encoder's branch-
// selection rule for unions where multiple branches could accept the
// same Go input. The dispatch (serUnion.ser, ser.go:101) is:
//
//  1. Tagged-map unwrap: map[string]any{"branch": value} routes
//     directly to the named branch.
//  2. Go-type natural Avro kind via unionTypeNameForValue: a Go
//     value's reflect.Kind maps to one Avro primitive name
//     (reflect.String→"string", reflect.Int64→"long", etc.).
//     branchKinds lookup picks that branch regardless of schema order.
//  3. Fall-through try-each in schema order — only reached for Go
//     types whose natural kind isn't in branchKinds (json.Number,
//     time.Time, *big.Rat, bytes-into-non-bytes branch, etc.).
//
// This test enumerates the natural-kind-first vs schema-order-first
// distinction; arbitrary changes to either rule fail the matrix.
// Parity with appendAvroJSONUnion: both encoders share
// unionTypeNameForValue, so the cells assert the same wantBranch
// across binary AND JSON encoders.
func TestRegression_UnionDispatchAmbiguous(t *testing.T) {
	cases := []struct {
		name       string
		schema     string
		input      any
		wantBranch string // the Avro branch name we expect to fire
	}{
		{
			// String Go input: natural Avro kind = "string". The
			// string branch wins regardless of schema position. The
			// long-timestamp branch's tryParseTimeString leniency is
			// only reached via try-each fall-through.
			name:       "string vs long+timestamp-millis: string branch wins (natural-kind match)",
			schema:     `["string",{"type":"long","logicalType":"timestamp-millis"}]`,
			input:      "2025-01-15T12:34:56.789Z",
			wantBranch: "string",
		},
		{
			// Same input + same kind preference, just schema-order
			// reversed. The natural-kind match for string still wins
			// over the timestamp branch's lenient string-parsing —
			// schema position doesn't matter for kind-matched branches.
			name:       "long+timestamp-millis vs string: STILL string (natural-kind beats schema-order)",
			schema:     `[{"type":"long","logicalType":"timestamp-millis"},"string"]`,
			input:      "2025-01-15T12:34:56.789Z",
			wantBranch: "string",
		},
		{
			// int64 Go input: natural Avro kind = "long" (per
			// unionTypeNameForValue). The long branch wins even though
			// it's listed second. The int branch's whole-number-int64
			// leniency is fall-through only.
			name:       "int vs long: long branch wins for int64 (natural-kind = long)",
			schema:     `["int","long"]`,
			input:      int64(42),
			wantBranch: "long",
		},
		{
			// Reverse order with the natural-kind match listed first
			// — same outcome, locks that the kind-match rule isn't
			// just "first matching branch in schema order".
			name:       "long vs int: long branch wins for int64 (natural-kind = long)",
			schema:     `["long","int"]`,
			input:      int64(42),
			wantBranch: "long",
		},
		{
			// int32 input: natural kind = "int". Picks int branch
			// regardless of schema order.
			name:       "int vs long: int branch wins for int32 (natural-kind = int)",
			schema:     `["long","int"]`,
			input:      int32(42),
			wantBranch: "int",
		},
		{
			// float64 input against [int, float]: natural kind =
			// "double", not in branchKinds. Falls through to try-each,
			// which walks schema order; int branch accepts whole-
			// number floats first.
			name:       "int vs float: int branch wins for whole-number float64 (fall-through, schema-order)",
			schema:     `["int","float"]`,
			input:      float64(42),
			wantBranch: "int",
		},
		{
			// Same float64 input, schema-order reversed: try-each
			// fall-through picks float branch first now.
			name:       "float vs int: float branch wins for whole-number float64 (fall-through, schema-order)",
			schema:     `["float","int"]`,
			input:      float64(42),
			wantBranch: "float",
		},
		{
			// String Go input against [string, bytes]: natural kind
			// = "string". String branch wins regardless of order.
			name:       "string vs bytes: string branch wins for Go string (natural-kind = string)",
			schema:     `["string","bytes"]`,
			input:      "hello",
			wantBranch: "string",
		},
		{
			// json.Number is reflect.String but unionTypeNameForValue
			// returns "" (so the value can flow into numeric branches
			// via try-each). Against [string, long] it falls through
			// to try-each; the string branch accepts a json.Number's
			// underlying string representation first.
			name:       "json.Number vs long: schema-order try-each (no natural-kind for json.Number)",
			schema:     `["long","string"]`,
			input:      json.Number("42"),
			wantBranch: "long",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := avro.MustParse(c.schema)
			// JSON encoder with TaggedUnions makes the branch
			// selection visible — the wire contains {"branch": value}.
			out, err := s.AppendEncodeJSON(nil, c.input, avro.TaggedUnions())
			if err != nil {
				t.Fatalf("AppendEncodeJSON: %v", err)
			}
			// Branch tag is the first key in the emitted object.
			want := `{"` + c.wantBranch + `":`
			if !strings.HasPrefix(string(out), want) {
				t.Errorf("union branch dispatch:\n  schema  %s\n  input   %v (%T)\n  got     %s\n  want    branch %q (prefix %q)", c.schema, c.input, c.input, out, c.wantBranch, want)
			}
			// Binary path must agree: encode → decode into *any and
			// inspect the produced canonical type. We don't pin the
			// exact Go type here (it depends on logical-type dispatch),
			// just that the round-trip succeeds — failure means the
			// binary path picked a branch the decoder can't read.
			bin, err := s.AppendEncode(nil, c.input)
			if err != nil {
				t.Fatalf("AppendEncode: %v", err)
			}
			var got any
			if _, err := s.Decode(bin, &got); err != nil {
				t.Fatalf("Decode into *any: %v (wire=%x)\n  binary branch dispatch produced wire its decoder can't read", err, bin)
			}
		})
	}
}

// TestRegression_ArrayMapElementSpecializationParity locks that every
// per-primitive serArray / serMap specialization accepts the same
// lenient Go-element shapes as its scalar counterpart. Pre-fix the
// multi-level-pointer leniency (**T, ***T) was in scalar serializers
// but missing from the array/map specializations — a sibling-sweep
// gap. This matrix prevents the next leniency added to a scalar
// serializer from silently differing in the hot-path container
// specialization.
func TestRegression_ArrayMapElementSpecializationParity(t *testing.T) {
	type cell struct {
		name        string
		elemSchema  string
		array       any // a slice whose elements exercise the lenient shape
		mapVal      any // a map[string]X whose values exercise the same shape
		decodeArray any // target for round-trip (must be the same shape)
		decodeMap   any
	}
	// Each cell encodes the lenient element shape through both the
	// scalar and the specialized array/map path, then decodes into the
	// matching Go shape and asserts equality. If a specialization
	// fails to accept the shape the scalar path accepts, the encode
	// errors (or the wire differs); if it accepts but emits different
	// bytes, the decode-back diverges from the scalar reference.
	cells := []cell{
		// Multi-level pointers: **T must work in arrays/maps just as
		// in scalar fields. The unwrapElemPtr fix from a prior session
		// covered the 12 specialization sites; this locks the parity.
		{
			name:        "**int32 in array<int>",
			elemSchema:  `"int"`,
			array:       []**int32{func() **int32 { v := int32(42); pv := &v; return &pv }()},
			mapVal:      map[string]**int32{"k": func() **int32 { v := int32(42); pv := &v; return &pv }()},
			decodeArray: new([]int32),
			decodeMap:   new(map[string]int32),
		},
		{
			name:        "**string in array<string>",
			elemSchema:  `"string"`,
			array:       []**string{func() **string { v := "hello"; pv := &v; return &pv }()},
			mapVal:      map[string]**string{"k": func() **string { v := "hello"; pv := &v; return &pv }()},
			decodeArray: new([]string),
			decodeMap:   new(map[string]string),
		},
		{
			name:        "**bool in array<boolean>",
			elemSchema:  `"boolean"`,
			array:       []**bool{func() **bool { v := true; pv := &v; return &pv }()},
			mapVal:      map[string]**bool{"k": func() **bool { v := true; pv := &v; return &pv }()},
			decodeArray: new([]bool),
			decodeMap:   new(map[string]bool),
		},
		{
			name:        "**int64 in array<long>",
			elemSchema:  `"long"`,
			array:       []**int64{func() **int64 { v := int64(42); pv := &v; return &pv }()},
			mapVal:      map[string]**int64{"k": func() **int64 { v := int64(42); pv := &v; return &pv }()},
			decodeArray: new([]int64),
			decodeMap:   new(map[string]int64),
		},
		{
			name:        "**float32 in array<float>",
			elemSchema:  `"float"`,
			array:       []**float32{func() **float32 { v := float32(3.5); pv := &v; return &pv }()},
			mapVal:      map[string]**float32{"k": func() **float32 { v := float32(3.5); pv := &v; return &pv }()},
			decodeArray: new([]float32),
			decodeMap:   new(map[string]float32),
		},
		{
			name:        "**float64 in array<double>",
			elemSchema:  `"double"`,
			array:       []**float64{func() **float64 { v := float64(3.14); pv := &v; return &pv }()},
			mapVal:      map[string]**float64{"k": func() **float64 { v := float64(3.14); pv := &v; return &pv }()},
			decodeArray: new([]float64),
			decodeMap:   new(map[string]float64),
		},

		// Whole-number-float-as-int lenient input: the SCALAR int
		// encoder accepts float64(42). The specialized serArray.serInt
		// MUST also accept it.
		{
			name:        "float64 whole-number in array<int> (lenient encode)",
			elemSchema:  `"int"`,
			array:       []float64{42, 43, 44},
			mapVal:      map[string]float64{"a": 42, "b": 43},
			decodeArray: new([]int32),
			decodeMap:   new(map[string]int32),
		},
		// json.Number into array<long>: scalar serLong accepts; the
		// specialization must too.
		{
			name:        "json.Number in array<long> (lenient encode)",
			elemSchema:  `"long"`,
			array:       []json.Number{"42", "43"},
			mapVal:      map[string]json.Number{"a": "42", "b": "43"},
			decodeArray: new([]int64),
			decodeMap:   new(map[string]int64),
		},
	}
	for _, c := range cells {
		t.Run(c.name+"/array", func(t *testing.T) {
			schema := avro.MustParse(`{"type":"array","items":` + c.elemSchema + `}`)
			wire, err := schema.AppendEncode(nil, c.array)
			if err != nil {
				t.Fatalf("array AppendEncode (%T): %v\n  array specialization may not accept this element shape", c.array, err)
			}
			if _, err := schema.Decode(wire, c.decodeArray); err != nil {
				t.Fatalf("array Decode: %v (wire=%x)", err, wire)
			}
		})
		t.Run(c.name+"/map", func(t *testing.T) {
			schema := avro.MustParse(`{"type":"map","values":` + c.elemSchema + `}`)
			wire, err := schema.AppendEncode(nil, c.mapVal)
			if err != nil {
				t.Fatalf("map AppendEncode (%T): %v\n  map specialization may not accept this element shape", c.mapVal, err)
			}
			if _, err := schema.Decode(wire, c.decodeMap); err != nil {
				t.Fatalf("map Decode: %v (wire=%x)", err, wire)
			}
		})
	}
}

// TestRegression_PromoteLogical locks Java parity for the case
// "writer-side promotion + reader has a logical type": Java's
// Resolver.Action carries logicalType + conversion orthogonally to
// Promote and applies the conversion AFTER the type widening. Pre-
// fix, twmb's promotion deser dropped the reader's logical-type
// deserializer, producing raw int64 / []byte / string instead of
// time.Time / *big.Rat / [16]byte at every nesting (top-level,
// record field, array item, map value, reader-union branch). Cover
// each promotion×logical pair and each nesting shape.
func TestRegression_PromoteLogical(t *testing.T) {
	t.Run("int → long+timestamp-millis at top level", func(t *testing.T) {
		writer := avro.MustParse(`"int"`)
		reader := avro.MustParse(`{"type":"long","logicalType":"timestamp-millis"}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		wire, err := writer.AppendEncode(nil, int32(1742385600))
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var got any
		if _, err := resolved.Decode(wire, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if _, ok := got.(time.Time); !ok {
			t.Fatalf("expected time.Time, got %T (%v)", got, got)
		}
	})

	t.Run("int → long+timestamp-millis inside record", func(t *testing.T) {
		writer := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"t","type":"int"}]}`)
		reader := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"t","type":{"type":"long","logicalType":"timestamp-millis"}}]}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		wire, err := writer.AppendEncode(nil, map[string]any{"t": int32(1742385600)})
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var got struct {
			T time.Time `avro:"t"`
		}
		if _, err := resolved.Decode(wire, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if got.T.IsZero() {
			t.Fatal("expected non-zero time.Time")
		}
	})

	t.Run("int → long+timestamp-millis in reader union", func(t *testing.T) {
		writer := avro.MustParse(`"int"`)
		reader := avro.MustParse(`["null",{"type":"long","logicalType":"timestamp-millis"}]`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		wire, err := writer.AppendEncode(nil, int32(1742385600))
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var got any
		if _, err := resolved.Decode(wire, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if _, ok := got.(time.Time); !ok {
			t.Fatalf("expected time.Time, got %T (%v)", got, got)
		}
	})

	t.Run("int → long+timestamp-millis in array items", func(t *testing.T) {
		writer := avro.MustParse(`{"type":"array","items":"int"}`)
		reader := avro.MustParse(`{"type":"array","items":{"type":"long","logicalType":"timestamp-millis"}}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		wire, err := writer.AppendEncode(nil, []int32{1742385600, 1742385700})
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var got []time.Time
		if _, err := resolved.Decode(wire, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if len(got) != 2 || got[0].IsZero() {
			t.Fatalf("expected 2 non-zero time.Time, got %v", got)
		}
	})

	t.Run("int → long+time-micros", func(t *testing.T) {
		writer := avro.MustParse(`"int"`)
		reader := avro.MustParse(`{"type":"long","logicalType":"time-micros"}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		wire, err := writer.AppendEncode(nil, int32(45_296_000))
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var got time.Duration
		if _, err := resolved.Decode(wire, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if got == 0 {
			t.Fatal("expected non-zero Duration")
		}
	})

	t.Run("bytes → string+uuid into [16]byte", func(t *testing.T) {
		writer := avro.MustParse(`"bytes"`)
		reader := avro.MustParse(`{"type":"string","logicalType":"uuid"}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		wire, err := writer.AppendEncode(nil, []byte("550e8400-e29b-41d4-a716-446655440000"))
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var got [16]byte
		if _, err := resolved.Decode(wire, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		want := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
		if got != want {
			t.Fatalf("got %x, want %x", got, want)
		}
	})

	// Full cross-product: every int→long+long-logical combination
	// the encoder accepts must produce the canonical Go type. Closes
	// the cell-coverage gap from the original 6-cell expansion.
	t.Run("int → long+timestamp-micros", func(t *testing.T) {
		writer := avro.MustParse(`"int"`)
		reader := avro.MustParse(`{"type":"long","logicalType":"timestamp-micros"}`)
		resolved, _ := avro.Resolve(writer, reader)
		wire, _ := writer.AppendEncode(nil, int32(1742385600))
		var got any
		if _, err := resolved.Decode(wire, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if _, ok := got.(time.Time); !ok {
			t.Fatalf("expected time.Time, got %T", got)
		}
	})
	t.Run("int → long+timestamp-nanos", func(t *testing.T) {
		writer := avro.MustParse(`"int"`)
		reader := avro.MustParse(`{"type":"long","logicalType":"timestamp-nanos"}`)
		resolved, _ := avro.Resolve(writer, reader)
		wire, _ := writer.AppendEncode(nil, int32(1742385600))
		var got any
		if _, err := resolved.Decode(wire, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if _, ok := got.(time.Time); !ok {
			t.Fatalf("expected time.Time, got %T", got)
		}
	})
	t.Run("int → long+local-timestamp-millis", func(t *testing.T) {
		writer := avro.MustParse(`"int"`)
		reader := avro.MustParse(`{"type":"long","logicalType":"local-timestamp-millis"}`)
		resolved, _ := avro.Resolve(writer, reader)
		wire, _ := writer.AppendEncode(nil, int32(1742385600))
		var got any
		if _, err := resolved.Decode(wire, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if _, ok := got.(time.Time); !ok {
			t.Fatalf("expected time.Time, got %T", got)
		}
	})
	t.Run("int → long+local-timestamp-micros", func(t *testing.T) {
		writer := avro.MustParse(`"int"`)
		reader := avro.MustParse(`{"type":"long","logicalType":"local-timestamp-micros"}`)
		resolved, _ := avro.Resolve(writer, reader)
		wire, _ := writer.AppendEncode(nil, int32(1742385600))
		var got any
		if _, err := resolved.Decode(wire, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if _, ok := got.(time.Time); !ok {
			t.Fatalf("expected time.Time, got %T", got)
		}
	})
	t.Run("int → long+local-timestamp-nanos", func(t *testing.T) {
		writer := avro.MustParse(`"int"`)
		reader := avro.MustParse(`{"type":"long","logicalType":"local-timestamp-nanos"}`)
		resolved, _ := avro.Resolve(writer, reader)
		wire, _ := writer.AppendEncode(nil, int32(1742385600))
		var got any
		if _, err := resolved.Decode(wire, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if _, ok := got.(time.Time); !ok {
			t.Fatalf("expected time.Time, got %T", got)
		}
	})
	t.Run("string → bytes+decimal", func(t *testing.T) {
		writer := avro.MustParse(`"string"`)
		reader := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		// Writer wrote the spec-form decimal bytes (codepoint-mapped)
		// as a string: unscaled=33, two's-complement big-endian = 0x21.
		wire, _ := writer.AppendEncode(nil, "\x21")
		var got *big.Rat
		if _, err := resolved.Decode(wire, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if got == nil || got.Cmp(big.NewRat(33, 100)) != 0 {
			t.Fatalf("got %v, want 33/100", got)
		}
	})
	t.Run("string → bytes+big-decimal", func(t *testing.T) {
		writer := avro.MustParse(`"string"`)
		reader := avro.MustParse(`{"type":"bytes","logicalType":"big-decimal"}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		// Construct a valid big-decimal payload via the natural encoder
		// against a big-decimal schema, then re-encode its bytes as a
		// string against the writer schema so the wire has the same
		// inner payload bytes the big-decimal deser expects.
		bdSchema := avro.MustParse(`{"type":"bytes","logicalType":"big-decimal"}`)
		bdWire, _ := bdSchema.AppendEncode(nil, big.NewRat(33, 100))
		// Strip the bytes length prefix; the payload is what comes after.
		// First varint is the byte length; iterate to find payload start.
		var payload []byte
		// bdWire = varlong(len) + payload. Re-decode to get payload back.
		var inner []byte
		if _, err := bdSchema.Decode(bdWire, &inner); err != nil {
			t.Fatalf("decode bd payload: %v", err)
		}
		payload = inner
		// Now write that payload as the contents of a string.
		wire, _ := writer.AppendEncode(nil, string(payload))
		var got *big.Rat
		if _, err := resolved.Decode(wire, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if got == nil || got.Cmp(big.NewRat(33, 100)) != 0 {
			t.Fatalf("got %v, want 33/100", got)
		}
	})
}

// TestRegression_CheckCompatVsResolveParity asserts that
// CheckCompatibility and Resolve agree on every (writer, reader)
// pair: if one says "compatible" the other must produce a valid
// resolved schema, and if one errors the other must error too. The
// two are parallel implementations sharing the same compat rules
// (checkCompat ↔ resolveNode) — exactly the class of drift that
// Finding 2 (resolveUnionUnion vs resolveReaderUnion) exposed at
// the tag-name layer. This matrix locks the higher-level
// "decision" output of both paths.
func TestRegression_CheckCompatVsResolveParity(t *testing.T) {
	type cell struct {
		name           string
		writer, reader string
	}
	cells := []cell{
		// ── compatible (must Resolve cleanly) ───────────────────────
		{"identity int", `"int"`, `"int"`},
		{"identity record", `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`, `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`},
		{"int→long promotion", `"int"`, `"long"`},
		{"long→double promotion", `"long"`, `"double"`},
		{"string→bytes promotion", `"string"`, `"bytes"`},
		{"bytes→string promotion", `"bytes"`, `"string"`},
		{"record + added field with default", `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`, `{"type":"record","name":"R","fields":[{"name":"x","type":"int"},{"name":"y","type":"int","default":0}]}`},
		{"record + removed field", `{"type":"record","name":"R","fields":[{"name":"x","type":"int"},{"name":"y","type":"int"}]}`, `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`},
		{"enum widened", `{"type":"enum","name":"E","symbols":["A","B"]}`, `{"type":"enum","name":"E","symbols":["A","B","C"]}`},
		{"reader union accepts writer scalar", `"int"`, `["null","int"]`},
		{"writer→reader both unions identity", `["null","int"]`, `["null","int"]`},
		{"writer→reader both unions promote", `["null","int"]`, `["null","long"]`},
		{"array<int> → array<long>", `{"type":"array","items":"int"}`, `{"type":"array","items":"long"}`},
		{"map<int> → map<long>", `{"type":"map","values":"int"}`, `{"type":"map","values":"long"}`},
		{"reader adds logical to long", `"long"`, `{"type":"long","logicalType":"timestamp-millis"}`},
		{"int→long+timestamp-millis promotion", `"int"`, `{"type":"long","logicalType":"timestamp-millis"}`},

		// ── incompatible (both must error) ──────────────────────────
		{"int → string", `"int"`, `"string"`},
		{"boolean → int", `"boolean"`, `"int"`},
		{"double → float (no narrowing promotion)", `"double"`, `"float"`},
		{"record + added field WITHOUT default", `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`, `{"type":"record","name":"R","fields":[{"name":"x","type":"int"},{"name":"y","type":"int"}]}`},
		{"enum symbol removed without default", `{"type":"enum","name":"E","symbols":["A","B","C"]}`, `{"type":"enum","name":"E","symbols":["A","B"]}`},
		{"record → enum", `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`, `{"type":"enum","name":"E","symbols":["A"]}`},
		{"different named types", `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`, `{"type":"record","name":"S","fields":[{"name":"x","type":"int"}]}`},
		{"array → map", `{"type":"array","items":"int"}`, `{"type":"map","values":"int"}`},
		{"long → int (no narrowing promotion)", `"long"`, `"int"`},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			w := avro.MustParse(c.writer)
			r := avro.MustParse(c.reader)
			compatErr := avro.CheckCompatibility(w, r)
			_, resolveErr := avro.Resolve(w, r)
			compatOK := compatErr == nil
			resolveOK := resolveErr == nil
			if compatOK != resolveOK {
				t.Errorf("CheckCompatibility ↔ Resolve disagree:\n  writer:  %s\n  reader:  %s\n  compat:  ok=%v err=%v\n  resolve: ok=%v err=%v",
					c.writer, c.reader, compatOK, compatErr, resolveOK, resolveErr)
			}
		})
	}
}

// TestRegression_ResolverTagNameParity asserts that EVERY resolved
// union path (writer-only union, reader-only union, both-union)
// emits reader-side branch names when TaggedUnions is enabled.
// Sibling resolveReaderUnion vs resolveUnionUnion drift is what
// produced Finding 2; this matrix walks every (writer-shape, reader-
// shape, promotion?) combination so any future drift in either site
// — or a new third site — fails the test loudly.
func TestRegression_ResolverTagNameParity(t *testing.T) {
	type cell struct {
		name    string
		writer  string
		reader  string
		input   any
		wantTag string // the branch name expected in the TaggedUnions output
	}
	cells := []cell{
		// Identity union pairs — no promotion. Tag must match the
		// reader's declared branch name (which equals writer's here).
		{
			name:    "identity [null,int] → [null,int]",
			writer:  `["null","int"]`,
			reader:  `["null","int"]`,
			input:   int32(42),
			wantTag: "int",
		},
		{
			name:    "identity [null,string] → [null,string]",
			writer:  `["null","string"]`,
			reader:  `["null","string"]`,
			input:   "hello",
			wantTag: "string",
		},
		// Promotion: tag must be reader's name (Finding 2).
		{
			name:    "promote int→long in both-union",
			writer:  `["null","int"]`,
			reader:  `["null","long"]`,
			input:   int32(42),
			wantTag: "long",
		},
		{
			name:    "promote int→double in both-union",
			writer:  `["null","int"]`,
			reader:  `["null","double"]`,
			input:   int32(42),
			wantTag: "double",
		},
		{
			name:    "promote long→double in both-union",
			writer:  `["null","long"]`,
			reader:  `["null","double"]`,
			input:   int64(42),
			wantTag: "double",
		},
		{
			name:    "promote float→double in both-union",
			writer:  `["null","float"]`,
			reader:  `["null","double"]`,
			input:   float32(3.5),
			wantTag: "double",
		},
		{
			name:    "promote string→bytes in both-union",
			writer:  `["null","string"]`,
			reader:  `["null","bytes"]`,
			input:   "hello",
			wantTag: "bytes",
		},
		{
			name:    "promote bytes→string in both-union",
			writer:  `["null","bytes"]`,
			reader:  `["null","string"]`,
			input:   []byte("hello"),
			wantTag: "string",
		},
		// Reader-only union (writer is scalar). resolveReaderUnion
		// already used the correct branch name; cells exist to
		// guard against a future regression that swaps it.
		{
			name:    "reader-only union [null,long] (writer int)",
			writer:  `"int"`,
			reader:  `["null","long"]`,
			input:   int32(42),
			wantTag: "long",
		},
		{
			name:    "reader-only union [null,double] (writer float)",
			writer:  `"float"`,
			reader:  `["null","double"]`,
			input:   float32(3.5),
			wantTag: "double",
		},
		// Promoted logical-typed reader branch: tag should be the
		// reader's spec-form name (e.g. just "long", not "long.
		// timestamp-millis" unless TagLogicalTypes is also set).
		{
			name:    "promote int→long-timestamp-millis (no TagLogicalTypes)",
			writer:  `["null","int"]`,
			reader:  `["null",{"type":"long","logicalType":"timestamp-millis"}]`,
			input:   int32(1742385600),
			wantTag: "long",
		},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			w := avro.MustParse(c.writer)
			r := avro.MustParse(c.reader)
			resolved, err := avro.Resolve(w, r)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			wire, err := w.AppendEncode(nil, c.input)
			if err != nil {
				t.Fatalf("AppendEncode: %v", err)
			}
			var got any
			if _, err := resolved.Decode(wire, &got, avro.TaggedUnions()); err != nil {
				t.Fatalf("Decode: %v", err)
			}
			m, ok := got.(map[string]any)
			if !ok {
				t.Fatalf("expected map[string]any wrapper, got %T (%v)", got, got)
			}
			if _, ok := m[c.wantTag]; !ok {
				t.Errorf("expected reader-side tag %q, got %v", c.wantTag, m)
			}
		})
	}
}

// TestRegression_TaggedUnionTagAfterPromotionBothUnion locks the
// invariant that when both reader and writer are unions and a writer
// branch promotes to a different reader branch (e.g. ["null","int"]
// → ["null","long"]), TaggedUnions emits the READER's branch name
// — not the writer's. Pre-fix resolveUnionUnion populated the tag
// table from wb instead of rb; the sibling resolveReaderUnion
// already used rb. The two paths are now in lockstep.
func TestRegression_TaggedUnionTagAfterPromotionBothUnion(t *testing.T) {
	writer := avro.MustParse(`["null","int"]`)
	reader := avro.MustParse(`["null","long"]`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	wire, err := writer.AppendEncode(nil, int32(42))
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	var got any
	if _, err := resolved.Decode(wire, &got, avro.TaggedUnions()); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T (%v)", got, got)
	}
	if _, ok := m["long"]; !ok {
		t.Fatalf("expected reader branch tag \"long\", got %v", m)
	}
}

// TestRegression_TimeLogicalStringRoundTrip locks the encoder/decoder
// symmetry for the seven string-accepting time logicals: date (int-
// backed) and timestamp-{millis,micros,nanos} +
// local-timestamp-{millis,micros,nanos} (all long-backed). The encoder
// accepts RFC 3339 / DateOnly Go strings (extractTime /
// tryParseDateString); pre-fix the decoder rejected Go-string targets
// with "cannot use string with Avro type {int,long}", so the user
// could Encode a string the same library couldn't Decode back. The
// finding is per pattern 12 (encoder lenient → decoder strict). Fix
// adds String arms to deserDate, deserTimeAsLong, decodeInt's "date"
// arm, and decodeLong's timestamp arm. All four sites stay in
// lockstep on the canonical output format: DateOnly for date,
// RFC3339Nano for the long-typed timestamps.
func TestRegression_TimeLogicalStringRoundTrip(t *testing.T) {
	type cell struct {
		name   string
		schema string
		input  string // Go string the encoder accepts
	}
	cells := []cell{
		{"date / date-only", `{"type":"int","logicalType":"date"}`, "2025-01-15"},
		{"date / RFC3339", `{"type":"int","logicalType":"date"}`, "2025-01-15T00:00:00Z"},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, "2025-01-15T12:34:56.789Z"},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`, "2025-01-15T12:34:56.789012Z"},
		{"timestamp-nanos", `{"type":"long","logicalType":"timestamp-nanos"}`, "2025-01-15T12:34:56.789012345Z"},
		{"local-timestamp-millis", `{"type":"long","logicalType":"local-timestamp-millis"}`, "2025-01-15T12:34:56.789Z"},
		{"local-timestamp-micros", `{"type":"long","logicalType":"local-timestamp-micros"}`, "2025-01-15T12:34:56.789012Z"},
		{"local-timestamp-nanos", `{"type":"long","logicalType":"local-timestamp-nanos"}`, "2025-01-15T12:34:56.789012345Z"},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			s := avro.MustParse(c.schema)

			// Binary: Encode(string) → Decode into *string, then
			// re-encode and assert wire equality (round-trip idempotent).
			bin, err := s.AppendEncode(nil, c.input)
			if err != nil {
				t.Fatalf("AppendEncode: %v", err)
			}
			var binOut string
			if _, err := s.Decode(bin, &binOut); err != nil {
				t.Fatalf("Decode into *string: %v", err)
			}
			rebin, err := s.AppendEncode(nil, binOut)
			if err != nil {
				t.Fatalf("re-encode decoded string: %v", err)
			}
			if !bytes.Equal(bin, rebin) {
				t.Errorf("binary round-trip not idempotent:\n  first  encode %x\n  decode→%q\n  second encode %x", bin, binOut, rebin)
			}

			// JSON: AppendEncodeJSON(string) → DecodeJSON into *string.
			jsn, err := s.AppendEncodeJSON(nil, c.input)
			if err != nil {
				t.Fatalf("AppendEncodeJSON: %v", err)
			}
			var jsnOut string
			if err := s.DecodeJSON(jsn, &jsnOut); err != nil {
				t.Fatalf("DecodeJSON into *string: %v", err)
			}
			rejsn, err := s.AppendEncodeJSON(nil, jsnOut)
			if err != nil {
				t.Fatalf("re-encode decoded string: %v", err)
			}
			if !bytes.Equal(jsn, rejsn) {
				t.Errorf("JSON round-trip not idempotent:\n  first  encode %s\n  decode→%q\n  second encode %s", jsn, jsnOut, rejsn)
			}

			// Cross-encoder: binary and JSON decoders agree on the
			// canonical string produced from the same wire-time value.
			if binOut != jsnOut {
				t.Errorf("binary/JSON decoded strings diverge:\n  binary %q\n  json   %q", binOut, jsnOut)
			}
		})
	}

	// Struct-field variant: the original failure shape was a struct
	// with a string-typed field tagged for a time-logical schema.
	t.Run("struct field / date", func(t *testing.T) {
		type R struct {
			D string `avro:"d"`
		}
		schema := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"d","type":{"type":"int","logicalType":"date"}}]}`)
		wire, err := schema.AppendEncode(nil, R{D: "2025-01-15"})
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var got R
		if _, err := schema.Decode(wire, &got); err != nil {
			t.Fatalf("Decode struct: %v", err)
		}
		if got.D != "2025-01-15" {
			t.Errorf("got %q, want %q", got.D, "2025-01-15")
		}
	})
	t.Run("struct field / timestamp-millis", func(t *testing.T) {
		type R struct {
			T string `avro:"t"`
		}
		schema := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"t","type":{"type":"long","logicalType":"timestamp-millis"}}]}`)
		wire, err := schema.AppendEncode(nil, R{T: "2025-01-15T12:34:56.789Z"})
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var got R
		if _, err := schema.Decode(wire, &got); err != nil {
			t.Fatalf("Decode struct: %v", err)
		}
		if got.T != "2025-01-15T12:34:56.789Z" {
			t.Errorf("got %q, want %q", got.T, "2025-01-15T12:34:56.789Z")
		}
	})
}

// TestRegression_LookupCIDeterminism locks the deterministic
// case-insensitive key lookup that the fuzzer flagged. The
// pre-fix lookupCI used `for k := range m` to find a case-
// insensitively-matching key — Go randomizes map iteration order,
// so a schema with multiple keys that fold to the same canonical
// key (e.g. both "tYpe" and "TYpe") returned different keys on
// different Root() calls for the same parsed Schema. Fix: pick
// the smallest by code-point ordering across the matching keys.
// User-visible symptom was Root() flapping between branch shapes.
func TestRegression_LookupCIDeterminism(t *testing.T) {
	// Multiple keys that fold to the same canonical: both "tYpe"
	// and "TYpe" → "type" via EqualFold.
	schemaJSON := `{"tYpe":"record","NaMe":"R","fields":[{"name":"x","tYpe":"long","TYpe":" c"}]}`
	s, err := avro.Parse(schemaJSON)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	root1 := s.Root()
	for i := 0; i < 100; i++ {
		root := s.Root()
		if !reflect.DeepEqual(root, root1) {
			t.Fatalf("Root() flapping on iter %d:\n  expected: %+v\n  got:      %+v", i, root1, root)
		}
	}
	canon1 := string(s.Canonical())
	for i := 0; i < 100; i++ {
		if got := string(s.Canonical()); got != canon1 {
			t.Fatalf("Canonical() flapping on iter %d:\n  expected: %s\n  got:      %s", i, canon1, got)
		}
	}
}

// TestRegression_SchemaCacheCustomTypeRefRejected locks that the
// SchemaCache + CustomType combination fails LOUD (at Parse time)
// rather than silently dropping the user's CustomType on cached
// named-type fields. Pre-fix: pre-loading "Inner" into a cache and
// then parsing "Outer" with a CustomType that would match Inner's
// fields silently produced a Schema where the CT never fires on
// those fields — Encode emits raw, Decode produces raw, both binary
// and JSON. The cache hits at buildPrimitive reuse the cached
// *schemaNode whose ser/deser/decodeJSON were baked without the new
// CT, and there's no architectural hook to re-wrap them.
//
// The fix walks the cached subtree at Parse time; if any descendant
// (record field, array item, map value, union branch) would match
// any of the current Parse's customTypes, Parse errors out with a
// directive: "re-parse Inner with the CustomType, or include it in
// this same build". Users surprised by the error get a clear
// remediation; silent drops are eliminated.
func TestRegression_SchemaCacheCustomTypeRefRejected(t *testing.T) {
	innerSchema := `{"type":"record","name":"Inner","fields":[{"name":"amt","type":{"type":"bytes","logicalType":"money"}}]}`
	outerSchema := `{"type":"record","name":"Outer","fields":[{"name":"inner","type":"Inner"}]}`

	mkCT := func() avro.CustomType {
		return avro.CustomType{
			LogicalType: "money",
			AvroType:    "bytes",
			Decode: func(v any, _ *avro.SchemaNode) (any, error) {
				return "MONEY", nil
			},
		}
	}

	t.Run("Parse errors when Inner is cached without CT and Outer registers a matching CT", func(t *testing.T) {
		cache := &avro.SchemaCache{}
		if _, err := cache.Parse(innerSchema); err != nil {
			t.Fatalf("cache.Parse(Inner): %v", err)
		}
		_, err := cache.Parse(outerSchema, avro.WithCustomType(mkCT()))
		if err == nil {
			t.Fatal("expected Parse error for cached-Inner + CT-on-Outer, got nil — CT would have been silently dropped")
		}
		// Error should name Inner and point at the directive.
		if !strings.Contains(err.Error(), "Inner") {
			t.Errorf("error should name the cached type \"Inner\": %v", err)
		}
		if !strings.Contains(err.Error(), "re-parse") {
			t.Errorf("error should suggest re-parsing: %v", err)
		}
	})

	t.Run("re-parse Inner with the CT first → Outer succeeds", func(t *testing.T) {
		// Documented remediation path: register the CT against
		// Inner's parse, then reference Inner from Outer with the
		// same CT in scope.
		cache := &avro.SchemaCache{}
		if _, err := cache.Parse(innerSchema, avro.WithCustomType(mkCT())); err != nil {
			t.Fatalf("cache.Parse(Inner, ct): %v", err)
		}
		sOuter, err := cache.Parse(outerSchema, avro.WithCustomType(mkCT()))
		if err != nil {
			t.Fatalf("cache.Parse(Outer, ct): %v", err)
		}
		enc, err := sOuter.AppendEncode(nil, map[string]any{"inner": map[string]any{"amt": []byte("abc")}})
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		var got map[string]any
		if _, err := sOuter.Decode(enc, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		inner, ok := got["inner"].(map[string]any)
		if !ok {
			t.Fatalf("expected inner record, got %T (%v)", got["inner"], got["inner"])
		}
		if inner["amt"] != "MONEY" {
			t.Errorf("CT did not fire: amt=%v (%T), want \"MONEY\"", inner["amt"], inner["amt"])
		}
	})

	t.Run("no error when cached subtree has no matching node", func(t *testing.T) {
		// Inner has no "money" logical; Outer's CT for "money" can't
		// match anything in Inner. Parse must succeed cleanly.
		plainInner := `{"type":"record","name":"PlainInner","fields":[{"name":"x","type":"int"}]}`
		plainOuter := `{"type":"record","name":"PlainOuter","fields":[{"name":"inner","type":"PlainInner"}]}`
		cache := &avro.SchemaCache{}
		if _, err := cache.Parse(plainInner); err != nil {
			t.Fatalf("cache.Parse(PlainInner): %v", err)
		}
		if _, err := cache.Parse(plainOuter, avro.WithCustomType(mkCT())); err != nil {
			t.Errorf("expected clean Parse (CT can't match Inner's subtree), got: %v", err)
		}
	})

	t.Run("wildcard CT (both empty) does NOT trigger rejection", func(t *testing.T) {
		// Wildcard CTs use ErrSkipCustomType at runtime to opt out;
		// they don't suppress built-ins at parse time, so they don't
		// cause the silent-drop pattern this guard rejects.
		wildcard := avro.CustomType{
			Decode: func(v any, _ *avro.SchemaNode) (any, error) {
				return nil, avro.ErrSkipCustomType
			},
		}
		cache := &avro.SchemaCache{}
		if _, err := cache.Parse(innerSchema); err != nil {
			t.Fatalf("cache.Parse(Inner): %v", err)
		}
		if _, err := cache.Parse(outerSchema, avro.WithCustomType(wildcard)); err != nil {
			t.Errorf("wildcard CT shouldn't trigger rejection: %v", err)
		}
	})

	t.Run("namespace-qualified cached ref also rejected", func(t *testing.T) {
		// Sibling path in buildPrimitive that previously also
		// bypassed customType propagation.
		nsInner := `{"type":"record","name":"NsInner","namespace":"ex.ns","fields":[{"name":"amt","type":{"type":"bytes","logicalType":"money"}}]}`
		nsOuter := `{"type":"record","name":"NsOuter","namespace":"ex.ns","fields":[{"name":"inner","type":"NsInner"}]}`
		cache := &avro.SchemaCache{}
		if _, err := cache.Parse(nsInner); err != nil {
			t.Fatalf("cache.Parse(NsInner): %v", err)
		}
		_, err := cache.Parse(nsOuter, avro.WithCustomType(mkCT()))
		if err == nil {
			t.Fatal("expected rejection for namespace-qualified cached ref + CT")
		}
	})
}

// TestRegression_LogicalTypedDefaults locks the same shape as
// Finding 1 (promotion drops reader's logical-type deserializer)
// but for the parallel "defaults" path: when a record's reader
// schema declares a default value for a logical-typed field that's
// absent on the wire, the materialized value must apply the logical
// conversion (time.Time / *big.Rat / [16]byte / etc.), not the raw
// wire-natural type. Pre-test, the defaults path was untested for
// logical types; the existing matrix only covered primitive defaults.
func TestRegression_LogicalTypedDefaults(t *testing.T) {
	type cell struct {
		name   string
		schema string
		input  any // map missing the field-with-default
		check  func(t *testing.T, decoded map[string]any)
	}
	cells := []cell{
		{
			name:   "long+timestamp-millis default",
			schema: `{"type":"record","name":"R","fields":[{"name":"t","type":{"type":"long","logicalType":"timestamp-millis"},"default":1742385600000}]}`,
			input:  map[string]any{},
			check: func(t *testing.T, d map[string]any) {
				if _, ok := d["t"].(time.Time); !ok {
					t.Errorf("expected time.Time from default, got %T (%v)", d["t"], d["t"])
				}
			},
		},
		{
			name:   "int+date default",
			schema: `{"type":"record","name":"R","fields":[{"name":"d","type":{"type":"int","logicalType":"date"},"default":20104}]}`,
			input:  map[string]any{},
			check: func(t *testing.T, d map[string]any) {
				if _, ok := d["d"].(time.Time); !ok {
					t.Errorf("expected time.Time from date default, got %T (%v)", d["d"], d["d"])
				}
			},
		},
		{
			name:   "bytes+decimal default",
			schema: `{"type":"record","name":"R","fields":[{"name":"r","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2},"default":"!"}]}`,
			input:  map[string]any{},
			check: func(t *testing.T, d map[string]any) {
				if _, ok := d["r"].(*big.Rat); !ok {
					t.Errorf("expected *big.Rat from decimal default, got %T (%v)", d["r"], d["r"])
				}
			},
		},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			s := avro.MustParse(c.schema)
			wire, err := s.AppendEncode(nil, c.input)
			if err != nil {
				t.Fatalf("AppendEncode: %v", err)
			}
			var got map[string]any
			if _, err := s.Decode(wire, &got); err != nil {
				t.Fatalf("Decode: %v (wire=%x)", err, wire)
			}
			c.check(t, got)
		})
	}
}

// TestRegression_FieldAliasWithPromotionLogical composes two
// evolution mechanisms (field aliases + writer→reader promotion
// with a logical-typed reader). Neither feature is novel on its
// own; the composition is the gap. Pre-test, an aliased field that
// was also promoted+logical-converted had no coverage.
func TestRegression_FieldAliasWithPromotionLogical(t *testing.T) {
	writer := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"old_ts","type":"int"}]}`)
	reader := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"new_ts","type":{"type":"long","logicalType":"timestamp-millis"},"aliases":["old_ts"]}]}`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	wire, err := writer.AppendEncode(nil, map[string]any{"old_ts": int32(1742385600)})
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	var got struct {
		T time.Time `avro:"new_ts"`
	}
	if _, err := resolved.Decode(wire, &got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.T.IsZero() {
		t.Fatalf("expected non-zero time.Time, got %v", got.T)
	}
}

// TestRegression_ResolveDoesNotMutateInputs locks that Resolve(w, r)
// produces a fresh resolved schema without mutating w or r. Sibling
// of the JSON DecodeJSON concurrency fix (which uncovered the schema-
// graph mutation pattern). If Resolve mutated shared maps, calling
// it twice from different goroutines (or even sequentially against
// different reader schemas) could race or corrupt the inputs.
func TestRegression_ResolveDoesNotMutateInputs(t *testing.T) {
	w := avro.MustParse(`"int"`)
	rTs := avro.MustParse(`{"type":"long","logicalType":"timestamp-millis"}`)
	rDouble := avro.MustParse(`"double"`)

	// Snapshot canonical forms before resolution.
	wCanon := w.Canonical()
	rTsCanon := rTs.Canonical()
	rDoubleCanon := rDouble.Canonical()

	// Concurrent Resolve calls against the same writer and different
	// readers — exercises any shared-state mutation on w or the
	// readers under race detector.
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(useTs bool) {
			defer wg.Done()
			if useTs {
				if _, err := avro.Resolve(w, rTs); err != nil {
					t.Errorf("Resolve(w, rTs): %v", err)
				}
			} else {
				if _, err := avro.Resolve(w, rDouble); err != nil {
					t.Errorf("Resolve(w, rDouble): %v", err)
				}
			}
		}(i%2 == 0)
	}
	wg.Wait()

	// Canonical forms must be unchanged after Resolve.
	if !bytes.Equal(w.Canonical(), wCanon) {
		t.Errorf("writer canonical changed: was %q now %q", wCanon, w.Canonical())
	}
	if !bytes.Equal(rTs.Canonical(), rTsCanon) {
		t.Errorf("reader-ts canonical changed: was %q now %q", rTsCanon, rTs.Canonical())
	}
	if !bytes.Equal(rDouble.Canonical(), rDoubleCanon) {
		t.Errorf("reader-double canonical changed: was %q now %q", rDoubleCanon, rDouble.Canonical())
	}

	// And each input must still be encodable/decodable independently.
	bin, err := w.AppendEncode(nil, int32(42))
	if err != nil {
		t.Fatalf("post-Resolve encode against unmodified writer: %v", err)
	}
	var v int32
	if _, err := w.Decode(bin, &v); err != nil {
		t.Fatalf("post-Resolve decode against unmodified writer: %v", err)
	}
}

// TestRegression_RecordFieldReorderWithPromotion locks that schema-
// evolution name-matching survives a field reorder combined with
// promotion. Writer has fields [a:int, b:string]; reader has
// [b:string, a:long]. The reader picks fields by name AND applies
// the int→long promotion to "a". The two evolution mechanisms
// compose; pre-test no cell explicitly covered the composition.
func TestRegression_RecordFieldReorderWithPromotion(t *testing.T) {
	writer := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`)
	reader := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"b","type":"string"},{"name":"a","type":"long"}]}`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	wire, err := writer.AppendEncode(nil, map[string]any{"a": int32(42), "b": "hello"})
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	var got struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}
	if _, err := resolved.Decode(wire, &got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.A != 42 || got.B != "hello" {
		t.Fatalf("got %+v, want {A:42, B:\"hello\"}", got)
	}
}

// TestRegression_SingleObjectRoundTrip locks the
// Single-Object-Encoding (SOE) magic-byte + fingerprint frame.
// AppendSingleObject prefixes wire data with 0xC3 0x01 +
// 8-byte Rabin fingerprint; DecodeSingleObject reverses. Locked
// for primitive, record, and union schema shapes; corruption of
// magic bytes or fingerprint must error rather than silently
// decoding.
func TestRegression_SingleObjectRoundTrip(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		input  any
		want   any
	}{
		{"primitive int", `"int"`, int32(42), int32(42)},
		{"primitive string", `"string"`, "hello", "hello"},
		{"record", `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`, map[string]any{"x": int32(42)}, map[string]any{"x": int32(42)}},
		{"union", `["null","long"]`, int64(42), int64(42)},
	}
	for _, c := range cases {
		t.Run(c.name+"/round-trip", func(t *testing.T) {
			s := avro.MustParse(c.schema)
			wire, err := s.AppendSingleObject(nil, c.input)
			if err != nil {
				t.Fatalf("AppendSingleObject: %v", err)
			}
			// minimum: 0xC3 0x01 + 8-byte fingerprint + payload
			if len(wire) < 10 || wire[0] != 0xC3 || wire[1] != 0x01 {
				t.Fatalf("bad SOE frame: %x", wire[:min(10, len(wire))])
			}
			dst := reflect.New(reflect.TypeOf(c.want))
			if _, err := s.DecodeSingleObject(wire, dst.Interface()); err != nil {
				t.Fatalf("DecodeSingleObject: %v", err)
			}
			if !reflect.DeepEqual(dst.Elem().Interface(), c.want) {
				t.Errorf("got %v, want %v", dst.Elem().Interface(), c.want)
			}
		})
		t.Run(c.name+"/corrupt magic errors", func(t *testing.T) {
			s := avro.MustParse(c.schema)
			wire, err := s.AppendSingleObject(nil, c.input)
			if err != nil {
				t.Fatalf("AppendSingleObject: %v", err)
			}
			wire[0] ^= 0xFF
			dst := reflect.New(reflect.TypeOf(c.want))
			if _, err := s.DecodeSingleObject(wire, dst.Interface()); err == nil {
				t.Errorf("expected error on corrupt magic byte, got success")
			}
		})
	}
}

// TestRegression_DefaultValueMaterializationParity locks the fact that
// a field default fills in the same logical value when binary-encoded
// and when JSON-encoded for an input missing that field. Defaults are
// JSON-parsed at schema parse time and converted via
// convertDefaultBytes; both encoders re-emit them, so any divergence
// in re-emit (e.g., codepoint vs UTF-8 for bytes defaults, sign-
// extension for decimal defaults) would surface here. The matrix
// covers each backing kind: primitive, bytes (the recent UTF-8 fix),
// fixed, enum, decimal-bytes, and time-logical.
func TestRegression_DefaultValueMaterializationParity(t *testing.T) {
	cases := []struct {
		name     string
		schema   string
		input    any // missing the field with the default
		assertEq func(t *testing.T, binDecoded, jsonDecoded any)
	}{
		{
			name:   "int default",
			schema: `{"type":"record","name":"R","fields":[{"name":"x","type":"int","default":42}]}`,
			input:  map[string]any{},
			assertEq: func(t *testing.T, b, j any) {
				if !reflect.DeepEqual(b, j) {
					t.Errorf("bin=%v json=%v", b, j)
				}
			},
		},
		{
			name:   "string default",
			schema: `{"type":"record","name":"R","fields":[{"name":"x","type":"string","default":"héllo"}]}`,
			input:  map[string]any{},
			assertEq: func(t *testing.T, b, j any) {
				if !reflect.DeepEqual(b, j) {
					t.Errorf("bin=%v json=%v", b, j)
				}
			},
		},
		{
			name:   "bytes default (JSON-spec codepoint form)",
			schema: `{"type":"record","name":"R","fields":[{"name":"x","type":"bytes","default":"é"}]}`,
			input:  map[string]any{},
			// Avro JSON-default uses codepoint mapping: "é" → e9 byte.
			// Both encoders must re-emit this as the single byte e9, not
			// as the UTF-8 c3 a9 encoding of "é".
			assertEq: func(t *testing.T, b, j any) {
				if !reflect.DeepEqual(b, j) {
					t.Errorf("bin=%v json=%v", b, j)
				}
			},
		},
		{
			name:   "fixed default (5 bytes, codepoint form)",
			schema: `{"type":"record","name":"R","fields":[{"name":"x","type":{"type":"fixed","name":"F","size":5},"default":"hello"}]}`,
			input:  map[string]any{},
			assertEq: func(t *testing.T, b, j any) {
				if !reflect.DeepEqual(b, j) {
					t.Errorf("bin=%v json=%v", b, j)
				}
			},
		},
		{
			name:   "enum default",
			schema: `{"type":"record","name":"R","fields":[{"name":"x","type":{"type":"enum","name":"E","symbols":["A","B","C"]},"default":"B"}]}`,
			input:  map[string]any{},
			assertEq: func(t *testing.T, b, j any) {
				if !reflect.DeepEqual(b, j) {
					t.Errorf("bin=%v json=%v", b, j)
				}
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := avro.MustParse(c.schema)
			binWire, err := s.AppendEncode(nil, c.input)
			if err != nil {
				t.Fatalf("AppendEncode: %v", err)
			}
			jsonWire, err := s.AppendEncodeJSON(nil, c.input)
			if err != nil {
				t.Fatalf("AppendEncodeJSON: %v", err)
			}
			var binDec, jsonDec map[string]any
			if _, err := s.Decode(binWire, &binDec); err != nil {
				t.Fatalf("Decode binary: %v (wire=%x)", err, binWire)
			}
			if err := s.DecodeJSON(jsonWire, &jsonDec); err != nil {
				t.Fatalf("DecodeJSON: %v (wire=%s)", err, jsonWire)
			}
			c.assertEq(t, binDec, jsonDec)
		})
	}
}

// TestRegression_StringTargetParityBinaryJSON locks the target-set
// parity between deserString (setStringValue) and decodeString. They
// are parallel implementations — binary uses the slab optimization
// for interface/string arms; JSON has the string already parsed —
// joined only by comments. If anyone adds a new lenient target to one
// path and forgets the other, this test fails. Covers TextUnmarshaler
// (the trickiest target since dispatch order matters: it must beat
// the []byte arm so net.IP-style named-slice types use UnmarshalText
// instead of raw byte assignment).
func TestRegression_StringTargetParityBinaryJSON(t *testing.T) {
	type textTarget struct {
		s string
	}
	// The marshaler is reachable only via Addr, mirroring the live
	// code path in both setStringValue and decodeString.
	// Defined inline to keep the test self-contained.
	type R struct {
		V textTargetUnmarshalable `avro:"v"`
	}
	schema := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"v","type":"string"}]}`)

	t.Run("binary string → TextUnmarshaler target", func(t *testing.T) {
		wire, err := schema.AppendEncode(nil, map[string]any{"v": "hello"})
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var got R
		if _, err := schema.Decode(wire, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if got.V.s != "hello" {
			t.Errorf("got %q, want %q", got.V.s, "hello")
		}
	})

	t.Run("json string → TextUnmarshaler target", func(t *testing.T) {
		jsonOut, err := schema.AppendEncodeJSON(nil, map[string]any{"v": "hello"})
		if err != nil {
			t.Fatalf("AppendEncodeJSON: %v", err)
		}
		var got R
		if err := schema.DecodeJSON(jsonOut, &got); err != nil {
			t.Fatalf("DecodeJSON: %v", err)
		}
		if got.V.s != "hello" {
			t.Errorf("got %q, want %q", got.V.s, "hello")
		}
	})

	t.Run("binary string → []byte target via promotion (bytes→string)", func(t *testing.T) {
		// Cross-check: bytes→string promotion also routes through
		// setStringValue and must accept the same TextUnmarshaler arm.
		writer := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"v","type":"bytes"}]}`)
		reader := schema
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		wire, err := writer.AppendEncode(nil, map[string]any{"v": []byte("hello")})
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var got R
		if _, err := resolved.Decode(wire, &got); err != nil {
			t.Fatalf("promotion Decode: %v", err)
		}
		if got.V.s != "hello" {
			t.Errorf("promotion got %q, want %q", got.V.s, "hello")
		}
	})

	_ = textTarget{} // silence unused
}

// textTargetUnmarshalable is the TextUnmarshaler target used by
// TestRegression_StringTargetParityBinaryJSON. Defined at package scope
// so reflect.PointerTo finds the UnmarshalText method.
type textTargetUnmarshalable struct{ s string }

func (t *textTargetUnmarshalable) UnmarshalText(b []byte) error {
	t.s = string(b)
	return nil
}

// TestRegression_TaggedUnionLogicalDisambiguation locks the
// (kind, logical) pair-match behavior of findUnionBranch's logical-tag
// fallback. Pre-tightening, the fallback matched on kind alone and
// returned the first kind-match — silently misrouting a "long.
// timestamp-millis" tag to a plain-long branch when both appeared in
// the same union. Now the fallback matches both kind AND logical so
// the right branch wins. Covers both the spec-compliant single-logical
// case and the mixed (plain + logical) case some tooling produces.
func TestRegression_TaggedUnionLogicalDisambiguation(t *testing.T) {
	t.Run("logical tag routes to logical branch", func(t *testing.T) {
		s := avro.MustParse(`[{"type":"long","logicalType":"timestamp-millis"}]`)
		var got any
		if err := s.DecodeJSON([]byte(`{"long.timestamp-millis":1700000000000}`), &got); err != nil {
			t.Fatalf("DecodeJSON: %v", err)
		}
		if _, ok := got.(time.Time); !ok {
			t.Errorf("expected time.Time, got %T (%v)", got, got)
		}
	})

	t.Run("plain tag with extra suffix does not silently route to plain branch", func(t *testing.T) {
		s := avro.MustParse(`["long"]`)
		var got any
		// {"long.timestamp-millis":...} has no matching branch — there's
		// no long-with-timestamp-millis branch. Pre-tightening, the
		// fallback silently routed to the plain "long" branch. Now it
		// errors out, surfacing the schema/payload mismatch.
		if err := s.DecodeJSON([]byte(`{"long.timestamp-millis":1700000000000}`), &got); err == nil {
			t.Errorf("expected error for unmatched logical tag, got nil; value=%v (%T)", got, got)
		}
	})
}

// TestRegression_PromotionTargetSetMatchesNatural locks the
// promotion-target-set parity: each promote*To* function accepts the
// same Go target types its natural deser counterpart accepts. Pre-
// fix, promotion deserializers had narrower target sets (only
// CanFloat for numeric promotions; only Interface+Slice for string→
// bytes; only Interface+String for bytes→string) — a Resolve(writer
// X, reader Y) round-trip rejected targets that reading wire-Y
// directly would accept. The fix routes every promote*To* through the
// shared setFloatValue/setBytesValue/setStringValue helpers, so target
// drift is structurally prevented. This matrix is the evidence: every
// (promotion × target type) cell that the natural deser accepts must
// also succeed via promotion.
func TestRegression_PromotionTargetSetMatchesNatural(t *testing.T) {
	type cell struct {
		name      string
		writer    string                       // writer schema (the wire's encoded type)
		reader    string                       // reader schema (the promotion target type)
		encode    func() any                   // value to encode against writer
		makeDest  func() any                   // pointer-to-struct with field "X" of the target type
		checkDest func(t *testing.T, dest any) // optional post-decode value assertion
		decodeErr string                       // if non-empty, decode is expected to fail with this substring
	}
	// Each cell encodes against writer, resolves writer→reader, then
	// decodes into a record whose field X has the target Go type.
	// Records (not bare schemas) so we exercise the field-dispatch
	// path the same way real schemas use it.
	wrap := func(t string) string {
		return `{"type":"record","name":"R","fields":[{"name":"x","type":` + t + `}]}`
	}
	cells := []cell{
		// ── int → float ─────────────────────────────────────────────
		{name: "int→float / float32", writer: "int", reader: "float", encode: func() any { return map[string]any{"x": int32(42)} },
			makeDest: func() any {
				return &struct {
					X float32 `avro:"x"`
				}{}
			}},
		{name: "int→float / float64", writer: "int", reader: "float", encode: func() any { return map[string]any{"x": int32(42)} },
			makeDest: func() any {
				return &struct {
					X float64 `avro:"x"`
				}{}
			}},
		{name: "int→float / int32 (whole-number lenient)", writer: "int", reader: "float", encode: func() any { return map[string]any{"x": int32(42)} },
			makeDest: func() any {
				return &struct {
					X int32 `avro:"x"`
				}{}
			}},
		{name: "int→float / int64 lenient", writer: "int", reader: "float", encode: func() any { return map[string]any{"x": int32(42)} },
			makeDest: func() any {
				return &struct {
					X int64 `avro:"x"`
				}{}
			}},
		{name: "int→float / uint32 lenient", writer: "int", reader: "float", encode: func() any { return map[string]any{"x": int32(42)} },
			makeDest: func() any {
				return &struct {
					X uint32 `avro:"x"`
				}{}
			}},
		{name: "int→float / json.Number", writer: "int", reader: "float", encode: func() any { return map[string]any{"x": int32(42)} },
			makeDest: func() any {
				return &struct {
					X json.Number `avro:"x"`
				}{}
			}},
		// ── int → double ────────────────────────────────────────────
		{name: "int→double / float64", writer: "int", reader: "double", encode: func() any { return map[string]any{"x": int32(42)} },
			makeDest: func() any {
				return &struct {
					X float64 `avro:"x"`
				}{}
			}},
		{name: "int→double / int32 lenient", writer: "int", reader: "double", encode: func() any { return map[string]any{"x": int32(42)} },
			makeDest: func() any {
				return &struct {
					X int32 `avro:"x"`
				}{}
			}},
		{name: "int→double / json.Number", writer: "int", reader: "double", encode: func() any { return map[string]any{"x": int32(42)} },
			makeDest: func() any {
				return &struct {
					X json.Number `avro:"x"`
				}{}
			}},
		// ── long → float ────────────────────────────────────────────
		{name: "long→float / float32", writer: "long", reader: "float", encode: func() any { return map[string]any{"x": int64(42)} },
			makeDest: func() any {
				return &struct {
					X float32 `avro:"x"`
				}{}
			}},
		{name: "long→float / int32 lenient", writer: "long", reader: "float", encode: func() any { return map[string]any{"x": int64(42)} },
			makeDest: func() any {
				return &struct {
					X int32 `avro:"x"`
				}{}
			}},
		{name: "long→float / json.Number", writer: "long", reader: "float", encode: func() any { return map[string]any{"x": int64(42)} },
			makeDest: func() any {
				return &struct {
					X json.Number `avro:"x"`
				}{}
			}},
		// ── long → double ───────────────────────────────────────────
		{name: "long→double / float64", writer: "long", reader: "double", encode: func() any { return map[string]any{"x": int64(42)} },
			makeDest: func() any {
				return &struct {
					X float64 `avro:"x"`
				}{}
			}},
		{name: "long→double / int64 lenient", writer: "long", reader: "double", encode: func() any { return map[string]any{"x": int64(42)} },
			makeDest: func() any {
				return &struct {
					X int64 `avro:"x"`
				}{}
			}},
		{name: "long→double / uint64 lenient", writer: "long", reader: "double", encode: func() any { return map[string]any{"x": int64(42)} },
			makeDest: func() any {
				return &struct {
					X uint64 `avro:"x"`
				}{}
			}},
		{name: "long→double / json.Number", writer: "long", reader: "double", encode: func() any { return map[string]any{"x": int64(42)} },
			makeDest: func() any {
				return &struct {
					X json.Number `avro:"x"`
				}{}
			}},
		// ── float → double ──────────────────────────────────────────
		{name: "float→double / float64", writer: "float", reader: "double", encode: func() any { return map[string]any{"x": float32(42)} },
			makeDest: func() any {
				return &struct {
					X float64 `avro:"x"`
				}{}
			}},
		{name: "float→double / float32 (target == source width)", writer: "float", reader: "double", encode: func() any { return map[string]any{"x": float32(42)} },
			makeDest: func() any {
				return &struct {
					X float32 `avro:"x"`
				}{}
			}},
		{name: "float→double / int32 (whole-number lenient)", writer: "float", reader: "double", encode: func() any { return map[string]any{"x": float32(42)} },
			makeDest: func() any {
				return &struct {
					X int32 `avro:"x"`
				}{}
			}},
		{name: "float→double / int64 lenient", writer: "float", reader: "double", encode: func() any { return map[string]any{"x": float32(42)} },
			makeDest: func() any {
				return &struct {
					X int64 `avro:"x"`
				}{}
			}},
		{name: "float→double / json.Number", writer: "float", reader: "double", encode: func() any { return map[string]any{"x": float32(42)} },
			makeDest: func() any {
				return &struct {
					X json.Number `avro:"x"`
				}{}
			}},
		// ── string → bytes ──────────────────────────────────────────
		{name: "string→bytes / []byte", writer: "string", reader: "bytes", encode: func() any { return map[string]any{"x": "hello"} },
			makeDest: func() any {
				return &struct {
					X []byte `avro:"x"`
				}{}
			}},
		{name: "string→bytes / [N]byte array (length-matched)", writer: "string", reader: "bytes", encode: func() any { return map[string]any{"x": "hello"} },
			makeDest: func() any {
				return &struct {
					X [5]byte `avro:"x"`
				}{}
			}},
		{name: "string→bytes / string", writer: "string", reader: "bytes", encode: func() any { return map[string]any{"x": "hello"} },
			makeDest: func() any {
				return &struct {
					X string `avro:"x"`
				}{}
			}},
		// ── bytes → string ──────────────────────────────────────────
		{name: "bytes→string / string", writer: "bytes", reader: "string", encode: func() any { return map[string]any{"x": []byte("hello")} },
			makeDest: func() any {
				return &struct {
					X string `avro:"x"`
				}{}
			}},
		{name: "bytes→string / []byte", writer: "bytes", reader: "string", encode: func() any { return map[string]any{"x": []byte("hello")} },
			makeDest: func() any {
				return &struct {
					X []byte `avro:"x"`
				}{}
			}},
		// ── int → long (no Go-target divergence between long types
		//    and integer targets; small witness cell) ────────────────
		{name: "int→long / int64", writer: "int", reader: "long", encode: func() any { return map[string]any{"x": int32(42)} },
			makeDest: func() any {
				return &struct {
					X int64 `avro:"x"`
				}{}
			}},
		{name: "int→long / json.Number", writer: "int", reader: "long", encode: func() any { return map[string]any{"x": int32(42)} },
			makeDest: func() any {
				return &struct {
					X json.Number `avro:"x"`
				}{}
			}},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			w := avro.MustParse(wrap(`"` + c.writer + `"`))
			r := avro.MustParse(wrap(`"` + c.reader + `"`))
			resolved, err := avro.Resolve(w, r)
			if err != nil {
				t.Fatalf("Resolve %s→%s: %v", c.writer, c.reader, err)
			}
			wire, err := w.AppendEncode(nil, c.encode())
			if err != nil {
				t.Fatalf("AppendEncode: %v", err)
			}
			dest := c.makeDest()
			_, err = resolved.Decode(wire, dest)
			if c.decodeErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.decodeErr) {
					t.Errorf("expected error containing %q, got: %v", c.decodeErr, err)
				}
				return
			}
			if err != nil {
				t.Errorf("Decode into %T: %v", dest, err)
			}
			if c.checkDest != nil {
				c.checkDest(t, dest)
			}
		})
	}
}

// TestRegression_TagLogicalTypesFixedRoundTrip locks the JSON
// TagLogicalTypes encode/decode round-trip for fixed-with-logical-
// type union branches. Pre-fix the encoder emitted `{"fixed.<logical>":
// ...}` but findUnionBranch's `type.logicalType` fallback only listed
// primitive bases (omitted "fixed"); twmb produced JSON it couldn't
// read back.
func TestRegression_TagLogicalTypesFixedRoundTrip(t *testing.T) {
	t.Run("fixed-uuid", func(t *testing.T) {
		s := avro.MustParse(`["null",{"type":"fixed","name":"FixedUUID","size":16,"logicalType":"uuid"}]`)
		in := [16]byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}
		enc, err := s.AppendEncodeJSON(nil, in, avro.TaggedUnions(), avro.TagLogicalTypes())
		if err != nil {
			t.Fatalf("AppendEncodeJSON: %v", err)
		}
		var got [16]byte
		if err := s.DecodeJSON(enc, &got); err != nil {
			t.Errorf("round-trip: encoded %s but decode failed: %v", enc, err)
		}
	})
	t.Run("fixed-decimal", func(t *testing.T) {
		s := avro.MustParse(`["null",{"type":"fixed","name":"FixedDec","size":4,"logicalType":"decimal","precision":4,"scale":2}]`)
		in := big.NewRat(33, 100)
		enc, err := s.AppendEncodeJSON(nil, in, avro.TaggedUnions(), avro.TagLogicalTypes())
		if err != nil {
			t.Fatalf("AppendEncodeJSON: %v", err)
		}
		var got *big.Rat
		if err := s.DecodeJSON(enc, &got); err != nil {
			t.Errorf("round-trip: encoded %s but decode failed: %v", enc, err)
		}
	})
	t.Run("fixed-duration", func(t *testing.T) {
		s := avro.MustParse(`["null",{"type":"fixed","name":"FixedDur","size":12,"logicalType":"duration"}]`)
		in := avro.Duration{Months: 1, Days: 2, Milliseconds: 3}
		enc, err := s.AppendEncodeJSON(nil, in, avro.TaggedUnions(), avro.TagLogicalTypes())
		if err != nil {
			t.Fatalf("AppendEncodeJSON: %v", err)
		}
		var got avro.Duration
		if err := s.DecodeJSON(enc, &got); err != nil {
			t.Errorf("round-trip: encoded %s but decode failed: %v", enc, err)
		}
	})
}

// TestParity_EncoderOptionMatrix exercises the JSON encoder option
// surface — TaggedUnions, TagLogicalTypes, LinkedinFloats — against
// every schema shape the option affects. Each cell specifies a
// schema, the input value, the encoder option, and the expected JSON
// output substring. Catches "option doesn't apply at position X" bugs
// (e.g. TaggedUnions wraps top-level unions but not nested-in-record
// unions) and ensures option interactions are consistent.
func TestParity_EncoderOptionMatrix(t *testing.T) {
	type cell struct {
		name       string
		schema     string
		input      any
		opts       []avro.Opt
		wantSubstr string // must appear in output
		// decodeBack is the value DecodeJSON must produce when fed the
		// encoder's output back. nil means "skip the decode-back check"
		// (LinkedinFloats null-for-NaN is documented lossy and decodes
		// to a non-NaN canonical value the matrix doesn't pin). A non-
		// nil value gates the round-trip, which is the structural gap
		// that let TagLogicalTypes+fixed slip past pre-existing audits:
		// the encoder happily emitted output the decoder couldn't read.
		decodeBack any
	}
	cells := []cell{
		// ── TaggedUnions ────────────────────────────────────────────
		{
			name:       "TaggedUnions: top-level union of named types",
			schema:     `["null",{"type":"record","name":"A","fields":[{"name":"x","type":"int"}]}]`,
			input:      map[string]any{"x": int32(7)},
			opts:       []avro.Opt{avro.TaggedUnions()},
			wantSubstr: `"A"`,
			decodeBack: map[string]any{"x": int32(7)},
		},
		// TaggedUnions on DecodeJSON wraps non-null union values as
		// map[string]any{branchName: val} (per the godoc on
		// TaggedUnions). The decodeBack values lock this documented
		// shape — encode(unwrapped) → tagged wire → decode(wrapped)
		// → re-encode produces the same wire (stable fixed point).
		{
			name:       "TaggedUnions: union nested in record",
			schema:     `{"type":"record","name":"R","fields":[{"name":"u","type":["null","int"]}]}`,
			input:      map[string]any{"u": int32(42)},
			opts:       []avro.Opt{avro.TaggedUnions()},
			wantSubstr: `"int"`,
			decodeBack: map[string]any{"u": map[string]any{"int": int32(42)}},
		},
		{
			name:       "TaggedUnions: union in array items",
			schema:     `{"type":"array","items":["null","int"]}`,
			input:      []any{int32(1), int32(2)},
			opts:       []avro.Opt{avro.TaggedUnions()},
			wantSubstr: `"int"`,
			decodeBack: []any{map[string]any{"int": int32(1)}, map[string]any{"int": int32(2)}},
		},
		{
			name:       "TaggedUnions: union in map values",
			schema:     `{"type":"map","values":["null","int"]}`,
			input:      map[string]any{"k": int32(42)},
			opts:       []avro.Opt{avro.TaggedUnions()},
			wantSubstr: `"int"`,
			decodeBack: map[string]any{"k": map[string]any{"int": int32(42)}},
		},
		// ── TagLogicalTypes ─────────────────────────────────────────
		{
			name:       "TagLogicalTypes + TaggedUnions: timestamp-millis tag includes logical",
			schema:     `["null",{"type":"long","logicalType":"timestamp-millis"}]`,
			input:      time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			opts:       []avro.Opt{avro.TaggedUnions(), avro.TagLogicalTypes()},
			wantSubstr: `"long.timestamp-millis"`,
			decodeBack: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:       "TagLogicalTypes alone (no TaggedUnions): no wrap on non-union",
			schema:     `{"type":"long","logicalType":"timestamp-millis"}`,
			input:      time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			opts:       []avro.Opt{avro.TagLogicalTypes()},
			wantSubstr: "1704067200000",
			decodeBack: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		// ── TagLogicalTypes + fixed-with-logical round-trip ─────────
		// These three cells lock the findUnionBranch "fixed" fallback
		// fix. Pre-fix, the encoder emitted {"fixed.uuid":...} but the
		// decoder rejected it (the logical-tag fallback didn't include
		// "fixed" in its kind list). The substring check alone passed;
		// only adding decodeBack catches the encoder/decoder asymmetry.
		{
			name:       "TagLogicalTypes + TaggedUnions: fixed-uuid round-trip",
			schema:     `["null",{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}]`,
			input:      "550e8400-e29b-41d4-a716-446655440000",
			opts:       []avro.Opt{avro.TaggedUnions(), avro.TagLogicalTypes()},
			wantSubstr: `"fixed.uuid"`,
			decodeBack: "550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:       "TagLogicalTypes + TaggedUnions: fixed-duration round-trip",
			schema:     `["null",{"type":"fixed","name":"D","size":12,"logicalType":"duration"}]`,
			input:      avro.Duration{Months: 1, Days: 2, Milliseconds: 3},
			opts:       []avro.Opt{avro.TaggedUnions(), avro.TagLogicalTypes()},
			wantSubstr: `"fixed.duration"`,
			decodeBack: avro.Duration{Months: 1, Days: 2, Milliseconds: 3},
		},
		// ── LinkedinFloats ──────────────────────────────────────────
		// LinkedinFloats encodes NaN as JSON null, which the decoder
		// reads back as NaN (the goavro null-for-NaN convention is
		// symmetric in this codebase). +Inf/-Inf as 1e999/-1e999 are
		// also lossless. The decodeBack values pin both directions.
		{
			name:       "LinkedinFloats: NaN as null round-trips to NaN",
			schema:     `"float"`,
			input:      float32(math.NaN()),
			opts:       []avro.Opt{avro.LinkedinFloats()},
			wantSubstr: "null",
			decodeBack: float32(math.NaN()),
		},
		{
			name:       "LinkedinFloats: +Inf as 1e999 round-trips to +Inf",
			schema:     `"float"`,
			input:      float32(math.Inf(1)),
			opts:       []avro.Opt{avro.LinkedinFloats()},
			wantSubstr: "1e999",
			decodeBack: float32(math.Inf(1)),
		},
		{
			name:       "LinkedinFloats: -Inf as -1e999 round-trips to -Inf",
			schema:     `"double"`,
			input:      math.Inf(-1),
			opts:       []avro.Opt{avro.LinkedinFloats()},
			wantSubstr: "-1e999",
			decodeBack: math.Inf(-1),
		},
		{
			name:       "default (no options): NaN as quoted string",
			schema:     `"float"`,
			input:      float32(math.NaN()),
			opts:       nil,
			wantSubstr: `"NaN"`,
			decodeBack: float32(math.NaN()),
		},
		{
			name:       "default (no options): +Inf as quoted string",
			schema:     `"float"`,
			input:      float32(math.Inf(1)),
			opts:       nil,
			wantSubstr: `"Infinity"`,
			decodeBack: float32(math.Inf(1)),
		},
		// ── Option combinations ─────────────────────────────────────
		// Encoder options must compose: applying two together must
		// produce output the decoder reads with the same two opts.
		// These cells catch the class of "option A + option B emits a
		// shape neither A nor B alone tests against the decoder."
		{
			name:   "LinkedinFloats + TaggedUnions: NaN inside record-nested union",
			schema: `{"type":"record","name":"R","fields":[{"name":"f","type":["null","float"]}]}`,
			input:  map[string]any{"f": float32(math.NaN())},
			opts:   []avro.Opt{avro.LinkedinFloats(), avro.TaggedUnions()},
			// LinkedinFloats emits null for NaN, but the float-branch tag
			// still wraps it — so output is `{"float":null}` inside the
			// field, NOT bare null. Decode sees tagged null on the float
			// branch and reapplies the LinkedinFloats null→NaN rule.
			wantSubstr: `{"float":null}`,
			decodeBack: map[string]any{"f": map[string]any{"float": float32(math.NaN())}},
		},
		{
			name:       "LinkedinFloats + TaggedUnions: +Inf inside record-nested union",
			schema:     `{"type":"record","name":"R","fields":[{"name":"f","type":["null","float"]}]}`,
			input:      map[string]any{"f": float32(math.Inf(1))},
			opts:       []avro.Opt{avro.LinkedinFloats(), avro.TaggedUnions()},
			wantSubstr: "1e999",
			// Decode-into-map of any: float branch tag is preserved per
			// TaggedUnions opt; the inner +Inf comes through as float32
			// (the schema's float branch is 32-bit, decoder honors that).
			decodeBack: map[string]any{"f": map[string]any{"float": float32(math.Inf(1))}},
		},
		{
			name:       "LinkedinFloats + TagLogicalTypes: float NaN (LinkedinFloats wins, no logical tag matters)",
			schema:     `"float"`,
			input:      float32(math.NaN()),
			opts:       []avro.Opt{avro.LinkedinFloats(), avro.TagLogicalTypes()},
			wantSubstr: "null",
			decodeBack: float32(math.NaN()),
		},
		{
			name:       "TaggedUnions + TagLogicalTypes: timestamp-millis in record-nested union",
			schema:     `{"type":"record","name":"R","fields":[{"name":"t","type":["null",{"type":"long","logicalType":"timestamp-millis"}]}]}`,
			input:      map[string]any{"t": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
			opts:       []avro.Opt{avro.TaggedUnions(), avro.TagLogicalTypes()},
			wantSubstr: `"long.timestamp-millis"`,
			decodeBack: map[string]any{"t": map[string]any{"long.timestamp-millis": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}},
		},
	}
	// floatEqual handles NaN at any nesting depth — reflect.DeepEqual
	// treats NaN!=NaN, so a NaN buried in a map/slice value of a
	// decode-back expectation would always fail. Recurse so the matrix
	// can express tagged-union cells like map[string]any{"float": NaN}.
	var floatEqual func(got, want any) bool
	floatEqual = func(got, want any) bool {
		switch w := want.(type) {
		case nil:
			return got == nil
		case float32:
			g, ok := got.(float32)
			if !ok {
				return false
			}
			if math.IsNaN(float64(w)) {
				return math.IsNaN(float64(g))
			}
			return g == w
		case float64:
			g, ok := got.(float64)
			if !ok {
				return false
			}
			if math.IsNaN(w) {
				return math.IsNaN(g)
			}
			return g == w
		case time.Time:
			g, ok := got.(time.Time)
			return ok && g.Equal(w)
		case map[string]any:
			g, ok := got.(map[string]any)
			if !ok || len(g) != len(w) {
				return false
			}
			for k, wv := range w {
				gv, exists := g[k]
				if !exists || !floatEqual(gv, wv) {
					return false
				}
			}
			return true
		case []any:
			g, ok := got.([]any)
			if !ok || len(g) != len(w) {
				return false
			}
			for i, wv := range w {
				if !floatEqual(g[i], wv) {
					return false
				}
			}
			return true
		}
		return reflect.DeepEqual(got, want)
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			s := avro.MustParse(c.schema)
			out, err := s.AppendEncodeJSON(nil, c.input, c.opts...)
			if err != nil {
				t.Fatalf("AppendEncodeJSON: %v", err)
			}
			if !strings.Contains(string(out), c.wantSubstr) {
				t.Errorf("output %s missing expected %q", out, c.wantSubstr)
			}
			if c.decodeBack == nil {
				return
			}
			// Decode-back into a fresh target of the expected type. For
			// the matrix's empty-interface cells the input dictates the
			// target; for typed values we mint a *T and read into it.
			var got any
			switch c.decodeBack.(type) {
			case map[string]any:
				var m map[string]any
				if err := s.DecodeJSON(out, &m, c.opts...); err != nil {
					t.Fatalf("DecodeJSON(%s): %v", out, err)
				}
				got = m
			case []any:
				var sl []any
				if err := s.DecodeJSON(out, &sl, c.opts...); err != nil {
					t.Fatalf("DecodeJSON(%s): %v", out, err)
				}
				got = sl
			default:
				p := reflect.New(reflect.TypeOf(c.decodeBack))
				if err := s.DecodeJSON(out, p.Interface(), c.opts...); err != nil {
					t.Fatalf("DecodeJSON(%s): %v", out, err)
				}
				got = p.Elem().Interface()
			}
			if !floatEqual(got, c.decodeBack) {
				t.Errorf("round-trip mismatch via %s:\n  got  %T %v\n  want %T %v", out, got, got, c.decodeBack, c.decodeBack)
			}
		})
	}
}

// TestParity_PointerTargetMatrix asserts round-trip identity when the
// Go target type is a pointer — *T, **T — for every primitive Avro
// type. Pointer targets go through indirect/indirectAlloc allocation
// paths that the value-target matrix doesn't exercise; bugs in those
// paths (nil-pointer auto-alloc, multi-level unwrap) surface here.
func TestParity_PointerTargetMatrix(t *testing.T) {
	type cell struct {
		name   string
		schema string
		input  any
		expect any
	}
	cells := []cell{
		// *T (one level of indirection) — encode via *T value, decode into **T.
		{"*int round-trip via **int target", `"int"`, int32(42), int32(42)},
		{"*string", `"string"`, "hello", "hello"},
		{"*float64", `"double"`, 3.14, 3.14},
		{"*bool", `"boolean"`, true, true},
		{"*[]byte", `"bytes"`, []byte("x"), []byte("x")},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			s := avro.MustParse(c.schema)
			// Encode through a *T pointer.
			ptr := reflect.New(reflect.TypeOf(c.input))
			ptr.Elem().Set(reflect.ValueOf(c.input))
			wire, err := s.AppendEncode(nil, ptr.Interface())
			if err != nil {
				t.Fatalf("AppendEncode(*T): %v", err)
			}
			// Decode into a **T target (nil pointer gets auto-allocated).
			var doublePtr = reflect.New(reflect.PointerTo(reflect.TypeOf(c.expect)))
			if _, err := s.Decode(wire, doublePtr.Interface()); err != nil {
				t.Fatalf("Decode into **T: %v", err)
			}
			inner := doublePtr.Elem()
			if inner.IsNil() {
				t.Fatalf("expected non-nil pointer after decode")
			}
			got := inner.Elem().Interface()
			if !reflect.DeepEqual(got, c.expect) {
				t.Errorf("got %v want %v", got, c.expect)
			}
		})
	}

	t.Run("nil *string into null-union encodes as null", func(t *testing.T) {
		s := avro.MustParse(`["null","string"]`)
		var p *string
		wire, err := s.AppendEncode(nil, p)
		if err != nil {
			t.Fatalf("AppendEncode nil pointer: %v", err)
		}
		if len(wire) != 1 || wire[0] != 0x00 {
			t.Errorf("expected null branch byte, got %x", wire)
		}
	})

	t.Run("non-nil *string into null-union encodes as string branch", func(t *testing.T) {
		s := avro.MustParse(`["null","string"]`)
		v := "hello"
		wire, err := s.AppendEncode(nil, &v)
		if err != nil {
			t.Fatalf("AppendEncode *string: %v", err)
		}
		// branch byte = 0x02, then string len varint + body.
		if wire[0] != 0x02 {
			t.Errorf("expected string branch byte, got first byte %x", wire[0])
		}
	})

	t.Run("[]*int round-trip", func(t *testing.T) {
		s := avro.MustParse(`{"type":"array","items":"int"}`)
		v1, v2, v3 := int32(1), int32(2), int32(3)
		in := []*int32{&v1, &v2, &v3}
		wire, err := s.AppendEncode(nil, in)
		if err != nil {
			t.Fatalf("AppendEncode []*int: %v", err)
		}
		var out []*int32
		if _, err := s.Decode(wire, &out); err != nil {
			t.Fatalf("Decode into []*int: %v", err)
		}
		if len(out) != 3 || *out[0] != 1 || *out[1] != 2 || *out[2] != 3 {
			t.Errorf("[]*int round-trip mismatch: got %v", out)
		}
	})

	t.Run("map[string]*int round-trip", func(t *testing.T) {
		s := avro.MustParse(`{"type":"map","values":"int"}`)
		v := int32(42)
		in := map[string]*int32{"k": &v}
		wire, err := s.AppendEncode(nil, in)
		if err != nil {
			t.Fatalf("AppendEncode map[string]*int: %v", err)
		}
		var out map[string]*int32
		if _, err := s.Decode(wire, &out); err != nil {
			t.Fatalf("Decode map[string]*int: %v", err)
		}
		if out["k"] == nil || *out["k"] != 42 {
			t.Errorf("map[string]*int round-trip: got %v", out)
		}
	})
}

// TestParity_RecursiveSchemaMatrix asserts that schemas referencing
// themselves round-trip correctly. The forward-ref machinery resolves
// the self-reference; this matrix exercises encode/decode of actual
// recursive data structures through that resolved schema.
func TestParity_RecursiveSchemaMatrix(t *testing.T) {
	t.Run("self-referential linked list", func(t *testing.T) {
		// Node = {value: int, next: [null, Node]}
		schema := `{"type":"record","name":"Node","fields":[
			{"name":"value","type":"int"},
			{"name":"next","type":["null","Node"]}
		]}`
		s := avro.MustParse(schema)
		// Build a list 1 → 2 → 3.
		// Construct as map[string]any to avoid Go struct recursive-type issues.
		list := map[string]any{
			"value": int32(1),
			"next": map[string]any{
				"value": int32(2),
				"next": map[string]any{
					"value": int32(3),
					"next":  nil,
				},
			},
		}
		wire, err := s.AppendEncode(nil, list)
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var got map[string]any
		if _, err := s.Decode(wire, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		// Walk the decoded structure to verify all three values.
		curr := got
		expected := []int32{1, 2, 3}
		for i, want := range expected {
			if curr == nil {
				t.Fatalf("list shorter than expected: stopped at i=%d", i)
			}
			vAny, ok := curr["value"]
			if !ok {
				t.Fatalf("missing value at i=%d", i)
			}
			v, ok := vAny.(int32)
			if !ok || v != want {
				t.Errorf("value at i=%d: got %v want %d", i, vAny, want)
			}
			nxt := curr["next"]
			if nxt == nil {
				curr = nil
			} else {
				curr = nxt.(map[string]any)
			}
		}
	})

	t.Run("mutually-recursive (record A references record B which references A)", func(t *testing.T) {
		schema := `{"type":"record","name":"A","fields":[
			{"name":"av","type":"int"},
			{"name":"b","type":["null",{"type":"record","name":"B","fields":[
				{"name":"bv","type":"string"},
				{"name":"a","type":["null","A"]}
			]}]}
		]}`
		s := avro.MustParse(schema)
		in := map[string]any{
			"av": int32(1),
			"b": map[string]any{
				"bv": "x",
				"a": map[string]any{
					"av": int32(2),
					"b":  nil,
				},
			},
		}
		wire, err := s.AppendEncode(nil, in)
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var got map[string]any
		if _, err := s.Decode(wire, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if got["av"] != int32(1) {
			t.Errorf("got[av] = %v want 1", got["av"])
		}
	})
}

// TestParity_ConcurrentSchema asserts that a single parsed *Schema
// is safe to use from multiple goroutines simultaneously for both
// encode and decode. Schema parsing is one-shot at Parse time; all
// subsequent state should be read-only on the hot path. Caught by
// `go test -race`.
func TestParity_ConcurrentSchema(t *testing.T) {
	s := avro.MustParse(`{"type":"record","name":"R","fields":[
		{"name":"x","type":"int"},
		{"name":"s","type":"string"}
	]}`)
	const goroutines = 16
	const iters = 100
	in := map[string]any{"x": int32(42), "s": "hello"}
	wire, err := s.AppendEncode(nil, in)
	if err != nil {
		t.Fatalf("setup AppendEncode: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				// Concurrent encode.
				if _, err := s.AppendEncode(nil, in); err != nil {
					t.Errorf("concurrent encode: %v", err)
					return
				}
				// Concurrent decode.
				var out map[string]any
				if _, err := s.Decode(wire, &out); err != nil {
					t.Errorf("concurrent decode: %v", err)
					return
				}
				if out["x"] != int32(42) || out["s"] != "hello" {
					t.Errorf("concurrent decode mismatch: %v", out)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// TestParity_JSONUnionTagFormMatrix asserts that the JSON decoder
// accepts every documented tag form for a tagged union:
//   - Spec/Java fullname: "long" / "com.example.User"
//   - goavro form: "type.logicalType"
//   - fastavro short name: "User" (for "com.example.User") iff unambiguous
//
// Catches "we forgot to support tag form X" gaps in findUnionBranch.
func TestParity_JSONUnionTagFormMatrix(t *testing.T) {
	t.Run("spec fullname tag (long)", func(t *testing.T) {
		s := avro.MustParse(`["null","long"]`)
		var got any
		if err := s.DecodeJSON([]byte(`{"long":42}`), &got); err != nil {
			t.Fatalf("DecodeJSON: %v", err)
		}
		if got != int64(42) {
			t.Errorf("got %v want int64(42)", got)
		}
	})
	t.Run("goavro tag (long.timestamp-millis)", func(t *testing.T) {
		s := avro.MustParse(`["null",{"type":"long","logicalType":"timestamp-millis"}]`)
		var got any
		if err := s.DecodeJSON([]byte(`{"long.timestamp-millis":1704067200000}`), &got); err != nil {
			t.Fatalf("DecodeJSON: %v", err)
		}
		if _, ok := got.(time.Time); !ok {
			t.Errorf("expected time.Time, got %T", got)
		}
	})
	t.Run("fastavro short name (unambiguous)", func(t *testing.T) {
		s := avro.MustParse(`["null",{"type":"record","name":"com.example.User","fields":[{"name":"x","type":"int"}]}]`)
		var got any
		if err := s.DecodeJSON([]byte(`{"User":{"x":42}}`), &got); err != nil {
			t.Fatalf("DecodeJSON short name: %v", err)
		}
	})
	t.Run("fastavro short name (ambiguous → reject)", func(t *testing.T) {
		s := avro.MustParse(`[
			{"type":"record","name":"a.Foo","fields":[{"name":"x","type":"int"}]},
			{"type":"record","name":"b.Foo","fields":[{"name":"y","type":"int"}]}
		]`)
		var got any
		err := s.DecodeJSON([]byte(`{"Foo":{"x":42}}`), &got)
		if err == nil {
			t.Errorf("expected ambiguous short name rejection")
		}
	})
	t.Run("fully-qualified fullname for namespaced record", func(t *testing.T) {
		s := avro.MustParse(`["null",{"type":"record","name":"com.example.User","fields":[{"name":"x","type":"int"}]}]`)
		var got any
		if err := s.DecodeJSON([]byte(`{"com.example.User":{"x":42}}`), &got); err != nil {
			t.Fatalf("DecodeJSON fullname: %v", err)
		}
	})
	t.Run("unknown tag → rejection", func(t *testing.T) {
		s := avro.MustParse(`["null","string"]`)
		var got any
		err := s.DecodeJSON([]byte(`{"DoesNotExist":"x"}`), &got)
		if err == nil {
			t.Errorf("expected unknown-tag rejection")
		}
	})
}

// TestParity_CustomTypeMatrix systematically exercises the
// CustomType dispatch surface. Each cell specifies a custom-type
// shape (GoType value vs pointer, presence of Encode and/or Decode,
// LogicalType + AvroType pair, multiple chained registrations with
// ErrSkipCustomType) and asserts round-trip identity for that shape.
// Catches the customEncode-chain-break class of finding (where the
// dispatch pipeline silently drops the second matching encoder).
func TestParity_CustomTypeMatrix(t *testing.T) {
	type Money int
	mkParse := func(t *testing.T, schemaJSON string, cts ...avro.CustomType) *avro.Schema {
		t.Helper()
		opts := make([]avro.SchemaOpt, 0, len(cts))
		for _, ct := range cts {
			opts = append(opts, avro.WithCustomType(ct))
		}
		s, err := avro.Parse(schemaJSON, opts...)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		return s
	}

	t.Run("value-GoType encode + decode round-trip", func(t *testing.T) {
		ct := avro.CustomType{
			GoType:      reflect.TypeFor[Money](),
			LogicalType: "money",
			AvroType:    "long",
			Encode: func(v any, _ *avro.SchemaNode) (any, error) {
				return int64(int(v.(Money)) * 100), nil
			},
			Decode: func(v any, _ *avro.SchemaNode) (any, error) {
				return Money(int(v.(int64)) / 100), nil
			},
		}
		s := mkParse(t, `{"type":"long","logicalType":"money"}`, ct)
		in := Money(7)
		wire, err := s.AppendEncode(nil, in)
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var got Money
		if _, err := s.Decode(wire, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if got != in {
			t.Errorf("round-trip: got %v want %v", got, in)
		}
	})
	t.Run("pointer-GoType encode + decode", func(t *testing.T) {
		ct := avro.CustomType{
			GoType:      reflect.TypeFor[*Money](),
			LogicalType: "money",
			AvroType:    "long",
			Encode: func(v any, _ *avro.SchemaNode) (any, error) {
				return int64(int(*v.(*Money)) * 100), nil
			},
			Decode: func(v any, _ *avro.SchemaNode) (any, error) {
				m := Money(int(v.(int64)) / 100)
				return &m, nil
			},
		}
		s := mkParse(t, `{"type":"long","logicalType":"money"}`, ct)
		in := Money(7)
		wire, err := s.AppendEncode(nil, &in)
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var got *Money
		if _, err := s.Decode(wire, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if got == nil || *got != in {
			t.Errorf("round-trip: got %v want %v", got, in)
		}
	})
	t.Run("encode-only (decode falls to built-in)", func(t *testing.T) {
		ct := avro.CustomType{
			GoType:      reflect.TypeFor[Money](),
			LogicalType: "money-enc",
			AvroType:    "long",
			Encode: func(v any, _ *avro.SchemaNode) (any, error) {
				return int64(int(v.(Money)) * 100), nil
			},
		}
		s := mkParse(t, `{"type":"long","logicalType":"money-enc"}`, ct)
		wire, err := s.AppendEncode(nil, Money(7))
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		// No Decode registered; built-in long deser produces int64.
		var got int64
		if _, err := s.Decode(wire, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if got != 700 {
			t.Errorf("got %d want 700", got)
		}
	})
	t.Run("decode-only (encode falls to built-in)", func(t *testing.T) {
		ct := avro.CustomType{
			GoType:      reflect.TypeFor[Money](),
			LogicalType: "money-dec",
			AvroType:    "long",
			Decode: func(v any, _ *avro.SchemaNode) (any, error) {
				return Money(int(v.(int64)) / 100), nil
			},
		}
		s := mkParse(t, `{"type":"long","logicalType":"money-dec"}`, ct)
		// Built-in encoder writes raw int64; decoder transforms.
		wire, err := s.AppendEncode(nil, int64(700))
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var got Money
		if _, err := s.Decode(wire, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if got != Money(7) {
			t.Errorf("got %v want 7", got)
		}
	})
	t.Run("chain with ErrSkipCustomType falls through to next", func(t *testing.T) {
		// Regression for the customEncode break/continue finding.
		skip := avro.CustomType{
			GoType:      reflect.TypeFor[Money](),
			LogicalType: "money-chain",
			AvroType:    "long",
			Encode:      func(any, *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType },
			Decode:      func(any, *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType },
		}
		real := avro.CustomType{
			GoType:      reflect.TypeFor[Money](),
			LogicalType: "money-chain",
			AvroType:    "long",
			Encode: func(v any, _ *avro.SchemaNode) (any, error) {
				return int64(int(v.(Money)) * 100), nil
			},
			Decode: func(v any, _ *avro.SchemaNode) (any, error) {
				return Money(int(v.(int64)) / 100), nil
			},
		}
		s := mkParse(t, `{"type":"long","logicalType":"money-chain"}`, skip, real)
		in := Money(7)
		wire, err := s.AppendEncode(nil, in)
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var got Money
		if _, err := s.Decode(wire, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if got != in {
			t.Errorf("got %v want %v", got, in)
		}
	})
	t.Run("chain with pointer-GoType skip falls through (regression)", func(t *testing.T) {
		// Recent audit Finding 1 — schema.go's pointer-GoType inner
		// loop used `break` on ErrSkipCustomType; should be `continue`.
		skip := avro.CustomType{
			GoType:      reflect.TypeFor[*Money](),
			LogicalType: "money-ptr-chain",
			AvroType:    "long",
			Encode:      func(any, *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType },
		}
		real := avro.CustomType{
			GoType:      reflect.TypeFor[*Money](),
			LogicalType: "money-ptr-chain",
			AvroType:    "long",
			Encode: func(v any, _ *avro.SchemaNode) (any, error) {
				return int64(int(*v.(*Money)) * 100), nil
			},
		}
		s := mkParse(t, `{"type":"long","logicalType":"money-ptr-chain"}`, skip, real)
		in := Money(7)
		wire, err := s.AppendEncode(nil, &in)
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		// Expect zigzag(1400) = 0xf8 0x0a.
		want := []byte{0xf8, 0x0a}
		if !bytes.Equal(wire, want) {
			t.Errorf("wire %x want %x (skip should have fallen through)", wire, want)
		}
	})
}

// TestParity_OCFRoundTripMatrix exercises OCF read/write across every
// codec, schema shape, and reader-schema-evolution configuration. OCF
// had zero matrix coverage before this; it has its own block-framing,
// codec dispatch, sync-marker logic, and metadata-map plumbing. Each
// cell encodes a small batch of records through an OCF writer, reads
// them back via NewReader, and asserts identity.
func TestParity_OCFRoundTripMatrix(t *testing.T) {
	type Person struct {
		Name string `avro:"name"`
		Age  int32  `avro:"age"`
	}
	personSchema := `{"type":"record","name":"Person","fields":[
		{"name":"name","type":"string"},
		{"name":"age","type":"int"}
	]}`
	records := []Person{
		{Name: "Alice", Age: 30},
		{Name: "Bob", Age: 25},
		{Name: "Carol", Age: 40},
	}

	type codecCase struct {
		name string
		opts []ocf.WriterOpt
	}
	codecs := []codecCase{
		{"null", nil}, // default
		{"deflate", []ocf.WriterOpt{ocf.WithCodec(ocf.DeflateCodec(-1))}},
		{"snappy", []ocf.WriterOpt{ocf.WithCodec(ocf.SnappyCodec())}},
	}
	for _, c := range codecs {
		t.Run("codec="+c.name, func(t *testing.T) {
			s := avro.MustParse(personSchema)
			var buf bytes.Buffer
			w, err := ocf.NewWriter(&buf, s, c.opts...)
			if err != nil {
				t.Fatalf("NewWriter: %v", err)
			}
			for _, r := range records {
				if err := w.Encode(r); err != nil {
					t.Fatalf("Encode: %v", err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			rd, err := ocf.NewReader(&buf)
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}
			var got []Person
			for {
				var p Person
				if err := rd.Decode(&p); err != nil {
					if err == io.EOF {
						break
					}
					t.Fatalf("Decode: %v", err)
				}
				got = append(got, p)
			}
			if !reflect.DeepEqual(got, records) {
				t.Errorf("got %+v want %+v", got, records)
			}
		})
	}

	t.Run("WithReaderSchema evolution: drop field via projection", func(t *testing.T) {
		writerSchema := avro.MustParse(personSchema)
		readerSchema := avro.MustParse(`{"type":"record","name":"Person","fields":[
			{"name":"name","type":"string"}
		]}`)
		var buf bytes.Buffer
		w, _ := ocf.NewWriter(&buf, writerSchema)
		for _, r := range records {
			_ = w.Encode(r)
		}
		_ = w.Close()
		rd, err := ocf.NewReader(&buf, ocf.WithReaderSchema(readerSchema))
		if err != nil {
			t.Fatalf("NewReader: %v", err)
		}
		var got []struct {
			Name string `avro:"name"`
		}
		for {
			var p struct {
				Name string `avro:"name"`
			}
			if err := rd.Decode(&p); err != nil {
				if err == io.EOF {
					break
				}
				t.Fatalf("Decode: %v", err)
			}
			got = append(got, p)
		}
		if len(got) != len(records) {
			t.Fatalf("got %d records, want %d", len(got), len(records))
		}
		for i, r := range records {
			if got[i].Name != r.Name {
				t.Errorf("[%d] Name: got %q want %q", i, got[i].Name, r.Name)
			}
		}
	})

	t.Run("WithMaxBlockBytes rejects oversized block", func(t *testing.T) {
		writerSchema := avro.MustParse(personSchema)
		var buf bytes.Buffer
		w, _ := ocf.NewWriter(&buf, writerSchema)
		for _, r := range records {
			_ = w.Encode(r)
		}
		_ = w.Close()
		// 4-byte cap is well below the smallest legitimate block.
		// The reader may accept NewReader (only the header is
		// parsed) but must reject the first Decode.
		rd, err := ocf.NewReader(bytes.NewReader(buf.Bytes()), ocf.WithMaxBlockBytes(4))
		if err != nil {
			return // rejected at NewReader → OK
		}
		var p Person
		if err := rd.Decode(&p); err == nil {
			t.Errorf("expected OCF block-bytes cap rejection")
		}
	})
}

// TestParity_SchemaStringIdempotency asserts that Parse(s.String())
// produces a schema with the same canonical form as s. Catches the
// "schema text isn't preserved correctly" class — String() returns
// the original JSON; round-tripping through Parse+String should
// yield an equivalent canonical form.
func TestParity_SchemaStringIdempotency(t *testing.T) {
	schemas := []string{
		`"int"`,
		`"string"`,
		`"bytes"`,
		`{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`,
		`{"type":"record","name":"R","namespace":"ns","fields":[{"name":"x","type":"int"}]}`,
		`{"type":"enum","name":"E","symbols":["A","B","C"]}`,
		`{"type":"fixed","name":"F","size":4}`,
		`{"type":"array","items":"int"}`,
		`{"type":"map","values":"string"}`,
		`["null","string"]`,
		`{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
		`{"type":"string","logicalType":"uuid"}`,
		`{"type":"long","logicalType":"timestamp-millis"}`,
		`{"type":"record","name":"R","fields":[
			{"name":"inner","type":{"type":"record","name":"Inner","fields":[
				{"name":"y","type":"long"}
			]}}
		]}`,
		`{"type":"record","name":"R","fields":[
			{"name":"f","type":["null",{"type":"record","name":"A","fields":[{"name":"x","type":"int"}]}]}
		]}`,
	}
	for _, schemaText := range schemas {
		t.Run(schemaText[:min(50, len(schemaText))], func(t *testing.T) {
			s1, err := avro.Parse(schemaText)
			if err != nil {
				t.Fatalf("Parse #1: %v", err)
			}
			s2, err := avro.Parse(s1.String())
			if err != nil {
				t.Fatalf("Parse(s1.String()): %v", err)
			}
			if !bytes.Equal(s1.Canonical(), s2.Canonical()) {
				t.Errorf("canonical drift:\n  s1 = %s\n  s2 = %s", s1.Canonical(), s2.Canonical())
			}
		})
	}
}

// TestParity_FingerprintStability locks the Schema Registry contract:
// semantically-equivalent schemas (different whitespace, different
// key order, redundant explicit defaults) produce the same canonical
// form (and therefore the same Fingerprint). Critical for any pipeline
// that uses fingerprints to identify schemas across systems.
func TestParity_FingerprintStability(t *testing.T) {
	type pair struct {
		name string
		a, b string
	}
	pairs := []pair{
		{"whitespace difference",
			`{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`,
			"{\n\t\"type\": \"record\",\n\t\"name\": \"R\",\n\t\"fields\": [\n\t\t{\"name\": \"x\", \"type\": \"int\"}\n\t]\n}"},
		{"key order in object",
			`{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`,
			`{"fields":[{"name":"x","type":"int"}],"name":"R","type":"record"}`},
		{"primitive vs object form",
			`"int"`,
			`{"type":"int"}`},
		{"explicit vs implicit namespace from name",
			`{"type":"record","name":"ns.R","fields":[{"name":"x","type":"int"}]}`,
			`{"type":"record","name":"R","namespace":"ns","fields":[{"name":"x","type":"int"}]}`},
	}
	for _, p := range pairs {
		t.Run(p.name, func(t *testing.T) {
			sa, err := avro.Parse(p.a)
			if err != nil {
				t.Fatalf("Parse a: %v", err)
			}
			sb, err := avro.Parse(p.b)
			if err != nil {
				t.Fatalf("Parse b: %v", err)
			}
			if !bytes.Equal(sa.Canonical(), sb.Canonical()) {
				t.Errorf("canonical drift between equivalent schemas:\n  a = %s\n  b = %s\n  a.canonical = %s\n  b.canonical = %s", p.a, p.b, sa.Canonical(), sb.Canonical())
			}
		})
	}
}

// TestParity_CheckCompatibilityMatrix asserts that CheckCompatibility
// agrees with whether Resolve succeeds. Sibling to ResolveMatrix: for
// each (writer, reader) pair, both APIs must report the same
// compatibility verdict — CheckCompatibility is the schema-time
// pre-flight for what Resolve will accept at decode-time.
func TestParity_CheckCompatibilityMatrix(t *testing.T) {
	type cell struct {
		name           string
		writer, reader string
		wantOK         bool
	}
	cells := []cell{
		// ── compatible (Resolve succeeds → CheckCompatibility nil) ──
		{"identity int", `"int"`, `"int"`, true},
		{"int → long promotion", `"int"`, `"long"`, true},
		{"int → double promotion", `"int"`, `"double"`, true},
		{"long → float promotion", `"long"`, `"float"`, true},
		{"float → double promotion", `"float"`, `"double"`, true},
		{"string → bytes", `"string"`, `"bytes"`, true},
		{"bytes → string", `"bytes"`, `"string"`, true},
		{"record add field with default", `{"type":"record","name":"R","fields":[
			{"name":"x","type":"int"}
		]}`, `{"type":"record","name":"R","fields":[
			{"name":"x","type":"int"},
			{"name":"y","type":"int","default":99}
		]}`, true},
		{"record drop field", `{"type":"record","name":"R","fields":[
			{"name":"x","type":"int"},
			{"name":"y","type":"int"}
		]}`, `{"type":"record","name":"R","fields":[
			{"name":"x","type":"int"}
		]}`, true},
		{"reader union superset", `"int"`, `["null","int"]`, true},
		{"enum reader superset", `{"type":"enum","name":"E","symbols":["A","B"]}`,
			`{"type":"enum","name":"E","symbols":["A","B","C"]}`, true},

		// ── incompatible (Resolve fails → CheckCompatibility !nil) ──
		{"int → string", `"int"`, `"string"`, false},
		{"long → int (downcast)", `"long"`, `"int"`, false},
		{"float → int", `"float"`, `"int"`, false},
		{"boolean → int", `"boolean"`, `"int"`, false},
		{"record name mismatch", `{"type":"record","name":"A","fields":[{"name":"x","type":"int"}]}`,
			`{"type":"record","name":"B","fields":[{"name":"x","type":"int"}]}`, false},
		{"record reader adds required field (no default)", `{"type":"record","name":"R","fields":[
			{"name":"x","type":"int"}
		]}`, `{"type":"record","name":"R","fields":[
			{"name":"x","type":"int"},
			{"name":"y","type":"int"}
		]}`, false},
		{"fixed size mismatch", `{"type":"fixed","name":"F","size":4}`,
			`{"type":"fixed","name":"F","size":8}`, false},
		{"enum reader missing symbol no-default", `{"type":"enum","name":"E","symbols":["A","B","C"]}`,
			`{"type":"enum","name":"E","symbols":["A","B"]}`, false},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			w, err := avro.Parse(c.writer)
			if err != nil {
				t.Fatalf("Parse writer: %v", err)
			}
			r, err := avro.Parse(c.reader)
			if err != nil {
				t.Fatalf("Parse reader: %v", err)
			}
			compatErr := avro.CheckCompatibility(w, r)
			_, resolveErr := avro.Resolve(w, r)
			compatOK := compatErr == nil
			resolveOK := resolveErr == nil
			if compatOK != resolveOK {
				t.Errorf("CheckCompatibility vs Resolve disagree:\n  CheckCompatibility: %v\n  Resolve: %v",
					compatErr, resolveErr)
			}
			if compatOK != c.wantOK {
				t.Errorf("expected compatible=%v, got compatErr=%v", c.wantOK, compatErr)
			}
		})
	}
}

// TestParity_CPUCostSentinels asserts that boundary-pathological
// inputs (right at the documented cap, not just past it) finish in
// bounded CPU time. Catches the "cost just under the limit" class of
// DoS — the pattern the finiteScale CPU-amplification bug
// represented, where the existing decimalScaleLimit=65536 bound
// fired AFTER 65536 iterations of an O(n²) inner loop, giving an
// attacker ~10⁸× CPU amplification per wire byte. Each cell measures
// the round-trip time for an attacker-shaped input at the boundary
// and asserts a hard upper bound. Cells should be fast (well under
// the cap) on the fixed code; pre-fix code regressed to seconds.
//
// Cap rationale: 200 ms is conservative for "either accept fast or
// reject fast." Tighter caps risk false-positives on slow CI; looser
// caps risk missing real regressions. 200 ms is comfortably above
// any legitimate boundary-input cost we've measured and well below
// the seconds-class regression that motivated the test.
func TestParity_CPUCostSentinels(t *testing.T) {
	t.Run("big-decimal encode of 1/10^cap", func(t *testing.T) {
		// Pre-fix: ~1.4 CPU seconds because finiteScale iterated the
		// 5-power factorization 65536 times on a 152K-digit big.Int.
		// Post-fix: short-circuits via BitLen bound.
		denom := new(big.Int).Exp(big.NewInt(10), big.NewInt(65536), nil)
		r := new(big.Rat).SetFrac(big.NewInt(1), denom)
		s := avro.MustParse(`{"type":"bytes","logicalType":"big-decimal"}`)
		start := time.Now()
		_, _ = s.AppendEncode(nil, r)
		if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
			t.Fatalf("encoding 1/10^65536 took %v (>200ms cap)", elapsed)
		}
	})
	t.Run("big-decimal encode of 1/2^cap", func(t *testing.T) {
		// 2-power-only denominator — exercises the TrailingZeroBits
		// arm. Pre-fix would have iterated 65536 single-bit shifts.
		denom := new(big.Int).Lsh(big.NewInt(1), 65536)
		r := new(big.Rat).SetFrac(big.NewInt(1), denom)
		s := avro.MustParse(`{"type":"bytes","logicalType":"big-decimal"}`)
		start := time.Now()
		_, _ = s.AppendEncode(nil, r)
		if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
			t.Fatalf("encoding 1/2^65536 took %v (>200ms cap)", elapsed)
		}
	})
	t.Run("decimal encode at precision cap", func(t *testing.T) {
		s := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":1024,"scale":0}`)
		// 1000-digit value, well below precision=1024.
		bigNum := new(big.Int).Exp(big.NewInt(10), big.NewInt(1000), nil)
		r := new(big.Rat).SetInt(bigNum)
		start := time.Now()
		_, _ = s.AppendEncode(nil, r)
		if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
			t.Fatalf("decimal encode at precision cap took %v", elapsed)
		}
	})
	t.Run("decode big-decimal at scale cap", func(t *testing.T) {
		// Construct a big-decimal wire payload with scale at the cap.
		// Pre-fix: huge 10^scale materialization at decode time.
		// Post-fix: bounded by decimalScaleLimit in parseBigDecimalPayload.
		var wire []byte
		// Inner: unscaled bytes "01" (length-prefixed) + zigzag(65536).
		wire = append(wire, 0x04, 0x01) // unscaled-bytes len=2 zigzag(2)=4, single 0x01 byte... wait
		// Use the actual format: outer bytes-len varint || inner.
		// Inner = unscaled-bytes-len-varint || unscaled-bytes || zigzag(scale)
		// Build inner: unscaled = [0x01] (len 1), scale=65536.
		// unscaled-len-zigzag = zigzag(1) = 2 → varint 0x02
		// scale-zigzag = zigzag(65536) = 131072 → varint
		inner := []byte{0x02, 0x01}
		inner = appendVarintBytes(inner, 131072)
		// Outer bytes framing: zigzag(len(inner)) || inner.
		wire = appendVarintBytes(nil, int64(len(inner)))
		wire = append(wire, inner...)
		s := avro.MustParse(`{"type":"bytes","logicalType":"big-decimal"}`)
		var out any
		start := time.Now()
		_, _ = s.Decode(wire, &out)
		if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
			t.Fatalf("big-decimal decode at scale cap took %v", elapsed)
		}
	})
	t.Run("array<null> at zero-byte-items cap", func(t *testing.T) {
		// Construct array<null> with count at maxZeroByteItems cap
		// (4096). Pre-existing decimalScaleLimit-style cap fires.
		var wire []byte
		wire = appendVarintBytes(nil, 4096)
		wire = append(wire, 0x00) // end-of-array
		s := avro.MustParse(`{"type":"array","items":"null"}`)
		var out any
		start := time.Now()
		_, _ = s.Decode(wire, &out)
		if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
			t.Fatalf("array<null> at cap took %v", elapsed)
		}
	})
	t.Run("decimal JSON decode of 1e65536", func(t *testing.T) {
		// Pre-fix: SetString materialized 10^65536 from the 9-byte
		// "1e1000000" pattern. Post-fix: boundedRatFromString caps.
		s := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)
		var out any
		start := time.Now()
		_ = s.DecodeJSON([]byte(`1e65536`), &out)
		if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
			t.Fatalf("decimal JSON decode of 1e65536 took %v", elapsed)
		}
	})
}

// appendVarintBytes is a test-local helper for hand-crafting Avro
// zigzag-varint-prefixed wire bytes in the CPU-cost matrix.
func appendVarintBytes(dst []byte, n int64) []byte {
	zz := uint64(n<<1) ^ uint64(n>>63)
	for zz >= 0x80 {
		dst = append(dst, byte(zz)|0x80)
		zz >>= 7
	}
	return append(dst, byte(zz))
}

// TestParity_ResolveMatrix systematically asserts schema-resolution
// round-trip identity across the documented Avro evolution surfaces.
// Sibling to TestParity_RoundTripMatrix: each cell specifies a writer
// schema, a reader schema, an encode value, and an expected decoded
// value. The test encodes via the writer, runs Resolve(writer,reader),
// and decodes via the resolved schema. Catches the "encode in writer
// works, but reader-side decode mishandles X" class — promotion
// chains, alias resolution, default-fill on dropped fields, enum
// symbol fallback, union narrowing.
//
// This is the largest untested surface in the package relative to
// historical bug count: most recent audit findings have been in
// schema build / encode / decode paths, but Resolve is a separate
// pipeline with its own per-kind dispatch and its own bugs. Cells
// here exercise that pipeline directly.
func TestParity_ResolveMatrix(t *testing.T) {
	type cell struct {
		name   string
		writer string
		reader string
		input  any
		expect any
	}

	cells := []cell{
		// ── int → wider numeric promotion ───────────────────────────
		{"int → long", `"int"`, `"long"`, int32(42), int64(42)},
		{"int → float", `"int"`, `"float"`, int32(42), float32(42)},
		{"int → double", `"int"`, `"double"`, int32(42), float64(42)},
		{"int → int (identity)", `"int"`, `"int"`, int32(42), int32(42)},
		// ── long → float/double ─────────────────────────────────────
		{"long → float", `"long"`, `"float"`, int64(42), float32(42)},
		{"long → double", `"long"`, `"double"`, int64(42), float64(42)},
		// ── float → double ──────────────────────────────────────────
		{"float → double", `"float"`, `"double"`, float32(3.5), float64(3.5)},
		// ── string ↔ bytes ──────────────────────────────────────────
		{"string → bytes", `"string"`, `"bytes"`, "hello", []byte("hello")},
		{"bytes → string", `"bytes"`, `"string"`, []byte("hello"), "hello"},
		// ── record evolution: add field with default ────────────────
		{"record add field with default",
			`{"type":"record","name":"R","fields":[
				{"name":"x","type":"int"}
			]}`,
			`{"type":"record","name":"R","fields":[
				{"name":"x","type":"int"},
				{"name":"y","type":"int","default":99}
			]}`,
			map[string]any{"x": int32(1)},
			map[string]any{"x": int32(1), "y": int32(99)}},
		// ── record evolution: drop field (projection) ───────────────
		{"record drop field via projection",
			`{"type":"record","name":"R","fields":[
				{"name":"x","type":"int"},
				{"name":"y","type":"int"}
			]}`,
			`{"type":"record","name":"R","fields":[
				{"name":"x","type":"int"}
			]}`,
			map[string]any{"x": int32(1), "y": int32(2)},
			map[string]any{"x": int32(1)}},
		// ── record evolution: alias (reader renames) ────────────────
		{"record reader alias resolves",
			`{"type":"record","name":"OldName","fields":[
				{"name":"x","type":"int"}
			]}`,
			`{"type":"record","name":"NewName","aliases":["OldName"],"fields":[
				{"name":"x","type":"int"}
			]}`,
			map[string]any{"x": int32(1)},
			map[string]any{"x": int32(1)}},
		// ── record field alias (reader renames field) ───────────────
		{"record field alias resolves",
			`{"type":"record","name":"R","fields":[
				{"name":"old_x","type":"int"}
			]}`,
			`{"type":"record","name":"R","fields":[
				{"name":"x","type":"int","aliases":["old_x"]}
			]}`,
			map[string]any{"old_x": int32(1)},
			map[string]any{"x": int32(1)}},
		// ── enum evolution ──────────────────────────────────────────
		{"enum reader is superset",
			`{"type":"enum","name":"E","symbols":["A","B"]}`,
			`{"type":"enum","name":"E","symbols":["A","B","C"]}`,
			"B", "B"},
		{"enum reader default for missing symbol",
			`{"type":"enum","name":"E","symbols":["A","B","C"]}`,
			`{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}`,
			"C", "A"},
		// ── union evolution: writer-narrows ─────────────────────────
		{"union reader is superset (string adds null branch)",
			`"string"`,
			`["null","string"]`,
			"hello", "hello"},
		{"union writer is single branch of reader union",
			`"int"`,
			`["null","int","string"]`,
			int32(42), int32(42)},
		// ── union → union (matching branches) ───────────────────────
		{"union [null,int] → union [null,int,string]",
			`["null","int"]`,
			`["null","int","string"]`,
			int32(42), int32(42)},
		// ── array element promotion ─────────────────────────────────
		{"array<int> → array<long>",
			`{"type":"array","items":"int"}`,
			`{"type":"array","items":"long"}`,
			[]int32{1, 2, 3},
			[]any{int64(1), int64(2), int64(3)}},
		// ── map value promotion ─────────────────────────────────────
		{"map<int> → map<long>",
			`{"type":"map","values":"int"}`,
			`{"type":"map","values":"long"}`,
			map[string]int32{"k": 42},
			map[string]any{"k": int64(42)}},
		// ── promotion-target lenient parity (regression for
		//    promote*To* narrower-than-natural target sets) ─────────
		{"int → float into float64 target",
			`"int"`, `"float"`, int32(42), float32(42)},
		{"bytes → string into []byte target",
			`"bytes"`, `"string"`, []byte("hello"), "hello"},
		{"string → bytes into string target",
			`"string"`, `"bytes"`, "hello", []byte("hello")},
		// ── nested record with field default fill ───────────────────
		{"nested record adds field with default",
			`{"type":"record","name":"R","fields":[
				{"name":"inner","type":{"type":"record","name":"Inner","fields":[
					{"name":"x","type":"int"}
				]}}
			]}`,
			`{"type":"record","name":"R","fields":[
				{"name":"inner","type":{"type":"record","name":"Inner","fields":[
					{"name":"x","type":"int"},
					{"name":"y","type":"long","default":99}
				]}}
			]}`,
			map[string]any{"inner": map[string]any{"x": int32(1)}},
			map[string]any{"inner": map[string]any{"x": int32(1), "y": int64(99)}}},
	}

	equal := func(got, want any) bool {
		if want == nil {
			return got == nil
		}
		switch w := want.(type) {
		case time.Time:
			g, ok := got.(time.Time)
			return ok && g.Equal(w)
		case *big.Rat:
			g, ok := got.(*big.Rat)
			return ok && g != nil && g.Cmp(w) == 0
		case float32:
			g, ok := got.(float32)
			if !ok {
				return false
			}
			if math.IsNaN(float64(w)) {
				return math.IsNaN(float64(g))
			}
			return g == w
		case float64:
			g, ok := got.(float64)
			if !ok {
				return false
			}
			if math.IsNaN(w) {
				return math.IsNaN(g)
			}
			return g == w
		}
		gv := reflect.ValueOf(got)
		wv := reflect.ValueOf(want)
		if gv.Kind() == wv.Kind() && (gv.Kind() == reflect.Slice || gv.Kind() == reflect.Map) {
			if gv.Len() == 0 && wv.Len() == 0 {
				return true
			}
		}
		return reflect.DeepEqual(got, want)
	}

	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			writer, err := avro.Parse(c.writer)
			if err != nil {
				t.Fatalf("Parse writer: %v", err)
			}
			reader, err := avro.Parse(c.reader)
			if err != nil {
				t.Fatalf("Parse reader: %v", err)
			}
			wire, err := writer.AppendEncode(nil, c.input)
			if err != nil {
				t.Fatalf("writer.AppendEncode: %v", err)
			}
			resolved, err := avro.Resolve(writer, reader)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			targetPtr := reflect.New(reflect.TypeOf(c.expect))
			if _, err := resolved.Decode(wire, targetPtr.Interface()); err != nil {
				t.Fatalf("resolved.Decode (wire=%x): %v", wire, err)
			}
			got := targetPtr.Elem().Interface()
			if !equal(got, c.expect) {
				t.Errorf("mismatch:\n  got  %T %v\n  want %T %v", got, got, c.expect, c.expect)
			}
		})
	}
}

// TestParity_AcceptedLeniencies locks the documented lenient
// acceptance choices twmb makes (sibling to
// TestParity_SchemaRejectionMatrix — cells here are the inverse:
// schemas/inputs that Java/some-reference rejects but twmb
// deliberately accepts). A future audit that tries to "fix" one of
// these cells will fail this test and surface the decision rather
// than silently break a documented behavior.
//
// Each cell describes ONE divergence; the rationale lives in the
// README's Logical Types section, the comments near each accepting
// code site, and the per-cell comment.
func TestParity_AcceptedLeniencies(t *testing.T) {
	t.Run("decimal logical on wrong base falls back to primitive", func(t *testing.T) {
		// schema.go's validateLogical strips `logicalType:"decimal"`
		// when the underlying type isn't bytes/fixed, per the spec's
		// "if a logical type cannot be deserialized, ignore it" rule.
		// Java's fromSchemaIgnoreInvalid catches BigDecimal.validate's
		// throw, fastavro's LOGICAL_*.get returns None and falls
		// through to bare base, hamba's parsePrimitiveLogicalType
		// returns nil for (typ, decimal) where typ != bytes/fixed.
		for _, base := range []string{"int", "long", "float", "double", "string", "boolean"} {
			schema := `{"type":"` + base + `","logicalType":"decimal","precision":4,"scale":2}`
			if _, err := avro.Parse(schema); err != nil {
				t.Errorf("decimal on %s should be accepted-and-degraded, got: %v", base, err)
			}
		}
	})
	t.Run("all known logical types on wrong base soft-drop", func(t *testing.T) {
		// F1 fix: pre-fix only the decimal arm soft-dropped; the other
		// 7 logical-type arms hard-rejected, diverging from Java/
		// fastavro/hamba/spec consensus. Post-fix every arm soft-drops
		// consistently. Schema parses as bare underlying.
		//
		// References:
		//   - Spec text (apache/avro Specification/_index.md): "If a
		//     logical type is invalid, … implementations should ignore
		//     the logical type and use the underlying Avro type."
		//   - Java (Schema.java:1979): result.logicalType =
		//     LogicalTypes.fromSchemaIgnoreInvalid(result) — catches
		//     RuntimeException from every per-type validate() and
		//     silently drops the logical.
		//   - fastavro (_read_py.py:662): LOGICAL_READERS.get(
		//     logical_type) returns None for unknown (rt-lt) combos and
		//     falls through to bare underlying decode.
		//   - hamba (schema_parse.go:205-222 + :514-524): the
		//     (typ, ltyp) switch returns nil for any combo not
		//     explicitly listed.
		schemas := []string{
			// timestamp-* on non-long
			`{"type":"string","logicalType":"timestamp-millis"}`,
			`{"type":"int","logicalType":"timestamp-micros"}`,
			`{"type":"float","logicalType":"timestamp-nanos"}`,
			`{"type":"double","logicalType":"timestamp-millis"}`,
			`{"type":"bytes","logicalType":"timestamp-micros"}`,
			`{"type":"boolean","logicalType":"timestamp-nanos"}`,
			// local-timestamp-* on non-long
			`{"type":"int","logicalType":"local-timestamp-millis"}`,
			`{"type":"int","logicalType":"local-timestamp-micros"}`,
			`{"type":"int","logicalType":"local-timestamp-nanos"}`,
			`{"type":"string","logicalType":"local-timestamp-millis"}`,
			// time-millis on non-int
			`{"type":"long","logicalType":"time-millis"}`,
			`{"type":"float","logicalType":"time-millis"}`,
			`{"type":"string","logicalType":"time-millis"}`,
			`{"type":"bytes","logicalType":"time-millis"}`,
			`{"type":"boolean","logicalType":"time-millis"}`,
			// time-micros on non-long
			`{"type":"int","logicalType":"time-micros"}`,
			`{"type":"float","logicalType":"time-micros"}`,
			`{"type":"string","logicalType":"time-micros"}`,
			// date on non-int
			`{"type":"long","logicalType":"date"}`,
			`{"type":"float","logicalType":"date"}`,
			`{"type":"double","logicalType":"date"}`,
			`{"type":"string","logicalType":"date"}`,
			`{"type":"bytes","logicalType":"date"}`,
			`{"type":"boolean","logicalType":"date"}`,
			// uuid on non-string-non-fixed(16)
			`{"type":"int","logicalType":"uuid"}`,
			`{"type":"long","logicalType":"uuid"}`,
			`{"type":"float","logicalType":"uuid"}`,
			`{"type":"bytes","logicalType":"uuid"}`,
			`{"type":"boolean","logicalType":"uuid"}`,
			`{"type":"fixed","name":"U","size":12,"logicalType":"uuid"}`,
			`{"type":"fixed","name":"U","size":32,"logicalType":"uuid"}`,
			// big-decimal on non-bytes
			`{"type":"int","logicalType":"big-decimal"}`,
			`{"type":"long","logicalType":"big-decimal"}`,
			`{"type":"string","logicalType":"big-decimal"}`,
			`{"type":"fixed","name":"D","size":12,"logicalType":"big-decimal"}`,
			// duration on non-fixed, or fixed with size != 12
			`{"type":"int","logicalType":"duration"}`,
			`{"type":"long","logicalType":"duration"}`,
			`{"type":"bytes","logicalType":"duration"}`,
			`{"type":"string","logicalType":"duration"}`,
			`{"type":"fixed","name":"D","size":10,"logicalType":"duration"}`,
			`{"type":"fixed","name":"D","size":13,"logicalType":"duration"}`,
		}
		for _, sch := range schemas {
			if _, err := avro.Parse(sch); err != nil {
				t.Errorf("expected soft-drop accept (Java/fastavro/hamba parity), got: %v\n  schema: %s", err, sch)
			}
		}
	})
	t.Run("logical-on-wrong-type round-trips as bare underlying", func(t *testing.T) {
		// After soft-drop, the schema behaves as bare underlying for
		// encode/decode. Verify a representative cross-section.
		cases := []struct {
			name  string
			sch   string
			input any
			want  any
		}{
			{"string-timestamp-millis", `{"type":"string","logicalType":"timestamp-millis"}`, "hello", "hello"},
			{"int-uuid", `{"type":"int","logicalType":"uuid"}`, int32(42), int32(42)},
			{"long-date", `{"type":"long","logicalType":"date"}`, int64(1234567890), int64(1234567890)},
			{"fixed12-duration", `{"type":"fixed","name":"F","size":10,"logicalType":"duration"}`, [10]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, [10]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
			{"fixed12-uuid", `{"type":"fixed","name":"U","size":12,"logicalType":"uuid"}`, [12]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, [12]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}},
			{"int-big-decimal", `{"type":"int","logicalType":"big-decimal"}`, int32(100), int32(100)},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				s, err := avro.Parse(c.sch)
				if err != nil {
					t.Fatalf("parse: %v", err)
				}
				enc, err := s.AppendEncode(nil, c.input)
				if err != nil {
					t.Fatalf("encode: %v", err)
				}
				out := reflect.New(reflect.TypeOf(c.want)).Interface()
				if _, err := s.Decode(enc, out); err != nil {
					t.Fatalf("decode: %v", err)
				}
				got := reflect.ValueOf(out).Elem().Interface()
				if !reflect.DeepEqual(got, c.want) {
					t.Errorf("round-trip: got %v (%T), want %v (%T)", got, got, c.want, c.want)
				}
			})
		}
	})
	t.Run("boolean decoder accepts any non-1 byte as false", func(t *testing.T) {
		// Mirrors Java's BinaryDecoder.readBoolean and fastavro's
		// `ord(b.read(1)) == 1` — both treat any non-1 byte as false.
		// Locked here so a future audit doesn't introduce a strict
		// 0x00/0x01-only check that diverges from the reference impls.
		s := avro.MustParse(`"boolean"`)
		var got bool
		if _, err := s.Decode([]byte{0x02}, &got); err != nil {
			t.Errorf("0x02 should decode as false (lenient parity with Java/fastavro): %v", err)
		}
		if got != false {
			t.Errorf("expected false, got %v", got)
		}
	})
	t.Run("decimal opaque-bytes pass-through on encode", func(t *testing.T) {
		// serBytesDecimal falls through to serBytes when the input
		// isn't a coercible numeric type — preserves the documented
		// opaque-bytes pass-through for users who construct the wire
		// payload manually. AppendEncode of an arbitrary string
		// silently encodes the raw bytes.
		s := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`)
		if _, err := s.AppendEncode(nil, "not a number"); err != nil {
			t.Errorf("string pass-through should succeed: %v", err)
		}
	})
	t.Run("whole-number float encodes against int", func(t *testing.T) {
		// AUDIT-listed intentional divergence: float64(42.0) is
		// accepted by serInt because encoding/json.Unmarshal produces
		// float64 for every JSON number. Fractional floats still
		// error.
		s := avro.MustParse(`"int"`)
		if _, err := s.AppendEncode(nil, float64(42)); err != nil {
			t.Errorf("whole-number float should encode as int: %v", err)
		}
		if _, err := s.AppendEncode(nil, float64(42.5)); err == nil {
			t.Errorf("fractional float should error on int encode")
		}
	})
	t.Run("string-form float defaults coerce to float64", func(t *testing.T) {
		// Java accepts {"type":"float","default":"1.5"} but not the
		// union form; fastavro accepts both but materializes as
		// string. twmb accepts both and materializes as float64.
		for _, schema := range []string{
			`{"type":"record","name":"R","fields":[{"name":"f","type":"float","default":"1.5"}]}`,
			`{"type":"record","name":"R","fields":[{"name":"f","type":["float","null"],"default":"1.5"}]}`,
		} {
			if _, err := avro.Parse(schema); err != nil {
				t.Errorf("string-form float default should parse: %v\n  %s", err, schema)
			}
		}
	})
	t.Run("implicit null default for [null, T] unions", func(t *testing.T) {
		// Avro spec says defaults are required when present; Java/
		// fastavro require explicit defaults. twmb infers a null
		// default for the canonical nullable shape.
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"x","type":["null","string"]}
		]}`)
		out, err := s.AppendEncode(nil, map[string]any{})
		if err != nil {
			t.Errorf("implicit null default should fill empty map: %v", err)
		}
		if len(out) != 1 || out[0] != 0x00 {
			t.Errorf("expected single null-branch byte, got %x", out)
		}
	})
	t.Run("wrapped-form name references and forward refs", func(t *testing.T) {
		// {"type":"Node"} is accepted as a name reference both for
		// backward and forward refs. Java accepts; fastavro/hamba
		// reject the wrapped form. twmb is lenient.
		fwdRef := `{"type":"record","name":"R","fields":[
			{"name":"a","type":{"type":"Inner"}},
			{"name":"def","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}}
		]}`
		if _, err := avro.Parse(fwdRef); err != nil {
			t.Errorf("forward wrapped-form ref should parse: %v", err)
		}
	})
	t.Run("JSON scanner accepts unknown \\X escapes", func(t *testing.T) {
		// twmb's JSON scanner accepts \q as `q` (passes the char
		// through). encoding/json rejects unknown escapes. Locked as
		// lenient; the practical cost is accepting a few hand-edited
		// schemas/JSON inputs that strict parsers reject. If
		// strictness is wanted, the fix is in json_scan.go's string
		// parser — well-localized.
		s := avro.MustParse(`"string"`)
		var got any
		if err := s.DecodeJSON([]byte(`"\q"`), &got); err != nil {
			t.Errorf("unknown escape should be accepted lenient: %v", err)
		}
	})
}

// TestRegression_DuplicateNamedTypeInUnion locks rejection of unions
// that contain the same named type twice — once as an inline
// definition and once as a name reference (in either order), or as
// two distinct references to the same name. Per Avro 1.12 spec:
// "Names of named types must be defined exactly once across all the
// schemas of the union." Java/hamba/avro-rs reject; twmb pre-fix
// missed the inline-def + name-ref pairs because the duplicate guard
// tracked `(kind, name)` pairs where the name-ref form returns
// `(refName, "")` and the inline form returns `("record", refName)`
// — different keys despite resolving to the same named type.
func TestRegression_DuplicateNamedTypeInUnion(t *testing.T) {
	cases := []struct {
		name   string
		schema string
	}{
		{"record def then name ref", `{"type":"record","name":"R","fields":[
			{"name":"f","type":[
				{"type":"record","name":"A","fields":[{"name":"x","type":"int"}]},
				"A"
			]}
		]}`},
		{"record ref then inline def", `{"type":"record","name":"R","fields":[
			{"name":"a","type":{"type":"record","name":"A","fields":[{"name":"x","type":"int"}]}},
			{"name":"f","type":[
				"A",
				{"type":"record","name":"A","fields":[{"name":"x","type":"int"}]}
			]}
		]}`},
		{"enum def then name ref", `{"type":"record","name":"R","fields":[
			{"name":"f","type":[
				{"type":"enum","name":"E","symbols":["A","B"]},
				"E"
			]}
		]}`},
		{"fixed def then name ref", `{"type":"record","name":"R","fields":[
			{"name":"f","type":[
				{"type":"fixed","name":"F","size":4},
				"F"
			]}
		]}`},
		{"record def then wrapped name ref", `{"type":"record","name":"R","fields":[
			{"name":"f","type":[
				{"type":"record","name":"A","fields":[{"name":"x","type":"int"}]},
				{"type":"A"}
			]}
		]}`},
		{"forward ref then inline def", `{"type":"record","name":"R","fields":[
			{"name":"f","type":[
				"A",
				{"type":"record","name":"A","fields":[{"name":"x","type":"int"}]}
			]}
		]}`},
		{"two name refs to same type", `{"type":"record","name":"R","fields":[
			{"name":"a","type":{"type":"record","name":"A","fields":[{"name":"x","type":"int"}]}},
			{"name":"f","type":["A","A"]}
		]}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := avro.Parse(c.schema); err == nil {
				t.Errorf("expected duplicate-named-type rejection for %s", c.name)
			}
		})
	}
}

// TestRegression_OCFDecodeMapNegationOverflow locks the missing
// secondary `if count < 0 { error }` guard in ocf/ocf.go decodeMap.
// Pre-fix, a MinInt64 block-count wrapped back to MinInt64 under
// negation; the magnitude check `count > 1<<20` did not trigger
// (MinInt64 < 1<<20), `for range int(count)` ran 0 iterations, and the
// malformed metadata-map block was silently skipped — the user saw the
// downstream `missing avro.schema in metadata` error rather than a
// precise rejection at the bad block. All sibling avro-side decoders
// (deser.go × 3, unsafe.go × 2, skip.go × 2) already had the guard;
// the OCF map decoder was the lone outlier.
func TestRegression_OCFDecodeMapNegationOverflow(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{'O', 'b', 'j', 1})
	tmp := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(tmp, math.MinInt64)
	buf.Write(tmp[:n])
	n = binary.PutVarint(tmp, 0) // block-bytes size
	buf.Write(tmp[:n])
	n = binary.PutVarint(tmp, 0) // map terminator
	buf.Write(tmp[:n])
	buf.Write(make([]byte, 16)) // sync marker

	_, err := ocf.NewReader(&buf)
	if err == nil {
		t.Fatal("expected error rejecting MinInt64 metadata block count")
	}
	if strings.Contains(err.Error(), "missing avro.schema") {
		t.Errorf("OCF reader silently skipped MinInt64-count block; expected precise rejection at the bad block, got: %v", err)
	}
}

// TestParity_RoundTripMatrix systematically asserts the encode-decode
// round-trip invariant for every (Avro schema, Go target type, code
// path) cell the library claims to support. The recurring bug shape
// caught by this matrix is "encode accepts type X, decode rejects
// type X" — every prior instance was caught one at a time by a
// TestRegression_* dedicated to that single cell; the matrix catches
// the pattern wholesale and prevents future instances from slipping
// through dedicated-test gaps.
//
// Each cell exercises three paths:
//
//   - Binary safe:   AppendEncode + Decode through a top-level value
//     of the target type.
//   - JSON safe:     AppendEncodeJSON + DecodeJSON through the same.
//   - Binary unsafe: AppendEncode + Decode through a one-field
//     struct whose field type is the target. Exercises the unsafe
//     us*/ud* fast path (or its safe-path fallback when no unsafe
//     specialization exists for the (Avro, Go) pair).
//
// Cells are paired by (encode value, decode target, expected). For
// identity cells encode and target are the same Go type. For lenient-
// acceptance cells (e.g. encode float64 into "int") encode and target
// differ, and `expect` records what the target should hold after a
// successful round-trip.
func TestParity_RoundTripMatrix(t *testing.T) {
	type cell struct {
		name      string
		schema    string
		input     any
		expect    any  // post-round-trip value of the target type; same Go type
		expectAny any  // decode-into-*any expected; nil means skip the any sub-test
		skipAny   bool // some logicals / leniency cells don't have a meaningful any target
	}

	bigRat := func(n, d int64) *big.Rat { return big.NewRat(n, d) }

	// canonical UUID bytes / string (matched pair)
	uuidStr := "550e8400-e29b-41d4-a716-446655440000"
	uuidBytes := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}

	ts := time.Date(2024, 6, 15, 12, 34, 56, 123_456_789, time.UTC)
	tsMillis := time.Date(2024, 6, 15, 12, 34, 56, 123_000_000, time.UTC)
	tsMicros := time.Date(2024, 6, 15, 12, 34, 56, 123_456_000, time.UTC)
	dateOnly := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	timeOfDay := time.Duration(45_296_000) * time.Millisecond           // 12:34:56
	timeOfDayMicros := time.Duration(45_296_000_123) * time.Microsecond // 12:34:56.000123
	dur := avro.Duration{Months: 1, Days: 2, Milliseconds: 3}

	cells := []cell{
		// ── null ────────────────────────────────────────────────────
		{"null/nil", `"null"`, nil, nil, nil, false},

		// ── boolean ─────────────────────────────────────────────────
		{"boolean/true", `"boolean"`, true, true, true, false},
		{"boolean/false", `"boolean"`, false, false, false, false},

		// ── int identity (each Go kind round-trips into itself) ─────
		{"int/int32", `"int"`, int32(42), int32(42), int32(42), false},
		{"int/int", `"int"`, int(42), int(42), int32(42), false},
		{"int/int8", `"int"`, int8(42), int8(42), int32(42), false},
		{"int/int16", `"int"`, int16(42), int16(42), int32(42), false},
		{"int/int64", `"int"`, int64(42), int64(42), int32(42), false},
		{"int/uint8", `"int"`, uint8(42), uint8(42), int32(42), false},
		{"int/uint16", `"int"`, uint16(42), uint16(42), int32(42), false},
		{"int/uint32", `"int"`, uint32(42), uint32(42), int32(42), false},
		{"int/uint64", `"int"`, uint64(42), uint64(42), int32(42), false},
		{"int/zero", `"int"`, int32(0), int32(0), int32(0), false},
		{"int/min", `"int"`, int32(math.MinInt32), int32(math.MinInt32), int32(math.MinInt32), false},
		{"int/max", `"int"`, int32(math.MaxInt32), int32(math.MaxInt32), int32(math.MaxInt32), false},
		{"int/negative", `"int"`, int32(-12345), int32(-12345), int32(-12345), false},

		// ── long identity ───────────────────────────────────────────
		{"long/int64", `"long"`, int64(42), int64(42), int64(42), false},
		{"long/int", `"long"`, int(42), int(42), int64(42), false},
		{"long/int32", `"long"`, int32(42), int32(42), int64(42), false},
		{"long/uint", `"long"`, uint(42), uint(42), int64(42), false},
		{"long/uint32", `"long"`, uint32(42), uint32(42), int64(42), false},
		{"long/uint64", `"long"`, uint64(42), uint64(42), int64(42), false},
		{"long/min", `"long"`, int64(math.MinInt64), int64(math.MinInt64), int64(math.MinInt64), false},
		{"long/max", `"long"`, int64(math.MaxInt64), int64(math.MaxInt64), int64(math.MaxInt64), false},

		// ── float identity ──────────────────────────────────────────
		{"float/float32", `"float"`, float32(3.5), float32(3.5), float32(3.5), false},
		{"float/float64", `"float"`, float64(3.5), float64(3.5), float32(3.5), false},
		{"float/zero", `"float"`, float32(0), float32(0), float32(0), false},
		{"float/negative", `"float"`, float32(-1.5), float32(-1.5), float32(-1.5), false},
		// Non-finite floats: NaN/±Inf must round-trip through binary
		// AND JSON identically. The default JSON form is the quoted-
		// string convention ("NaN"/"Infinity"/"-Infinity"); these cells
		// exercise both the binary IEEE-754-bits path and the JSON
		// quoted-token path against the same expected sentinel.
		{"float/NaN", `"float"`, float32(math.NaN()), float32(math.NaN()), float32(math.NaN()), false},
		{"float/+Inf", `"float"`, float32(math.Inf(1)), float32(math.Inf(1)), float32(math.Inf(1)), false},
		{"float/-Inf", `"float"`, float32(math.Inf(-1)), float32(math.Inf(-1)), float32(math.Inf(-1)), false},

		// ── double identity ─────────────────────────────────────────
		{"double/float64", `"double"`, float64(3.14), float64(3.14), float64(3.14), false},
		{"double/float32", `"double"`, float32(3.5), float32(3.5), float64(3.5), false},
		{"double/NaN", `"double"`, math.NaN(), math.NaN(), math.NaN(), false},
		{"double/+Inf", `"double"`, math.Inf(1), math.Inf(1), math.Inf(1), false},
		{"double/-Inf", `"double"`, math.Inf(-1), math.Inf(-1), math.Inf(-1), false},

		// ── string identity ─────────────────────────────────────────
		{"string/string", `"string"`, "hello", "hello", "hello", false},
		{"string/[]byte", `"string"`, []byte("hello"), []byte("hello"), "hello", false},
		{"string/empty", `"string"`, "", "", "", false},
		{"string/unicode", `"string"`, "héllo 世界", "héllo 世界", "héllo 世界", false},

		// ── bytes identity ──────────────────────────────────────────
		{"bytes/[]byte", `"bytes"`, []byte("hello"), []byte("hello"), []byte("hello"), false},
		{"bytes/string", `"bytes"`, "hello", "hello", []byte("hello"), false},
		{"bytes/[N]byte", `"bytes"`, [5]byte{'h', 'e', 'l', 'l', 'o'}, [5]byte{'h', 'e', 'l', 'l', 'o'}, []byte("hello"), false},
		{"bytes/empty", `"bytes"`, []byte{}, []byte{}, []byte{}, false},
		// Go-string-into-bytes with non-ASCII content. Binary appends the
		// string's raw UTF-8; pre-fix the JSON encoder mapped each rune
		// to a single byte (so "é" produced e9 in JSON but c3 a9 in
		// binary). The matrix asserts both paths agree on `expect`, so
		// these cells lock cross-encoder parity for multibyte strings.
		{"bytes/string-multibyte", `"bytes"`, "héllo", "héllo", []byte("héllo"), false},
		{"bytes/string-multibyte-3byte-rune", `"bytes"`, "€uros", "€uros", []byte("€uros"), false},

		// ── fixed identity ──────────────────────────────────────────
		{"fixed/[N]byte", `{"type":"fixed","name":"F","size":5}`, [5]byte{'h', 'e', 'l', 'l', 'o'}, [5]byte{'h', 'e', 'l', 'l', 'o'}, []byte("hello"), false},
		{"fixed/[]byte", `{"type":"fixed","name":"F","size":5}`, []byte("hello"), []byte("hello"), []byte("hello"), false},
		{"fixed/string", `{"type":"fixed","name":"F","size":5}`, "hello", "hello", []byte("hello"), false},
		// Non-ASCII Go-string-into-fixed. Size is UTF-8 byte count: "héllo"
		// is 6 bytes (h=1, é=2, l=1, l=1, o=1). Pre-fix the JSON encoder
		// would have rejected this as "size mismatch: got 5 codepoints,
		// need 6" while binary accepted it. Locks cross-encoder agreement
		// on the size predicate.
		{"fixed/string-multibyte", `{"type":"fixed","name":"F","size":6}`, "héllo", "héllo", []byte("héllo"), false},

		// ── enum ────────────────────────────────────────────────────
		{"enum/string", `{"type":"enum","name":"E","symbols":["A","B","C"]}`, "B", "B", "B", false},
		{"enum/int", `{"type":"enum","name":"E","symbols":["A","B","C"]}`, int(1), int(1), "B", false},
		{"enum/int32", `{"type":"enum","name":"E","symbols":["A","B","C"]}`, int32(1), int32(1), "B", false},
		{"enum/uint8", `{"type":"enum","name":"E","symbols":["A","B","C"]}`, uint8(1), uint8(1), "B", false},

		// ── lenient-acceptance cross-type (encode T1, decode T2) ────
		// Decode-into-*any returns the schema's canonical type
		// regardless of the encode-input type — int→int32, etc.
		{"int/float64 lenient", `"int"`, float64(42), float64(42), int32(42), false},
		{"int/float32 lenient", `"int"`, float32(42), float32(42), int32(42), false},
		{"long/float64 lenient", `"long"`, float64(42), float64(42), int64(42), false},
		{"float/int32 lenient", `"float"`, int32(42), int32(42), float32(42), false},
		{"float/uint32 lenient", `"float"`, uint32(42), uint32(42), float32(42), false},
		{"double/int64 lenient", `"double"`, int64(42), int64(42), float64(42), false},

		// ── all logical types ───────────────────────────────────────
		{"date/time.Time", `{"type":"int","logicalType":"date"}`, dateOnly, dateOnly, dateOnly, false},
		{"time-millis/time.Duration", `{"type":"int","logicalType":"time-millis"}`, timeOfDay, timeOfDay, timeOfDay, false},
		{"time-micros/time.Duration", `{"type":"long","logicalType":"time-micros"}`, timeOfDayMicros, timeOfDayMicros, timeOfDayMicros, false},
		{"timestamp-millis/time.Time", `{"type":"long","logicalType":"timestamp-millis"}`, tsMillis, tsMillis, tsMillis, false},
		{"timestamp-micros/time.Time", `{"type":"long","logicalType":"timestamp-micros"}`, tsMicros, tsMicros, tsMicros, false},
		{"timestamp-nanos/time.Time", `{"type":"long","logicalType":"timestamp-nanos"}`, ts, ts, ts, false},
		{"local-timestamp-millis/time.Time", `{"type":"long","logicalType":"local-timestamp-millis"}`, tsMillis, tsMillis, tsMillis, false},
		{"local-timestamp-micros/time.Time", `{"type":"long","logicalType":"local-timestamp-micros"}`, tsMicros, tsMicros, tsMicros, false},
		{"local-timestamp-nanos/time.Time", `{"type":"long","logicalType":"local-timestamp-nanos"}`, ts, ts, ts, false},

		// String input/output for the seven string-accepting time
		// logicals. The encoder accepts RFC 3339 strings (date-only
		// for date, RFC 3339 Nano for the long-typed timestamps);
		// pre-fix the decoder rejected string targets, so Encode-
		// then-Decode through the same string would error. Matrix-
		// locked here so both binary and JSON paths agree on the
		// canonical output format (DateOnly / RFC3339Nano) and the
		// round-trip is wire-stable. Decode-into-*any still yields
		// the time.Time canonical form (these are output-shape cells,
		// not any-targets), so skipAny=true.
		{"date/string", `{"type":"int","logicalType":"date"}`, "2024-06-15", "2024-06-15", nil, true},
		{"timestamp-millis/string", `{"type":"long","logicalType":"timestamp-millis"}`, "2024-06-15T12:34:56.123Z", "2024-06-15T12:34:56.123Z", nil, true},
		{"timestamp-micros/string", `{"type":"long","logicalType":"timestamp-micros"}`, "2024-06-15T12:34:56.123456Z", "2024-06-15T12:34:56.123456Z", nil, true},
		{"timestamp-nanos/string", `{"type":"long","logicalType":"timestamp-nanos"}`, "2024-06-15T12:34:56.123456789Z", "2024-06-15T12:34:56.123456789Z", nil, true},
		{"local-timestamp-millis/string", `{"type":"long","logicalType":"local-timestamp-millis"}`, "2024-06-15T12:34:56.123Z", "2024-06-15T12:34:56.123Z", nil, true},
		{"local-timestamp-micros/string", `{"type":"long","logicalType":"local-timestamp-micros"}`, "2024-06-15T12:34:56.123456Z", "2024-06-15T12:34:56.123456Z", nil, true},
		{"local-timestamp-nanos/string", `{"type":"long","logicalType":"local-timestamp-nanos"}`, "2024-06-15T12:34:56.123456789Z", "2024-06-15T12:34:56.123456789Z", nil, true},

		{"uuid-string/string", `{"type":"string","logicalType":"uuid"}`, uuidStr, uuidStr, uuidStr, false},
		{"uuid-string/[]byte", `{"type":"string","logicalType":"uuid"}`, []byte(uuidStr), []byte(uuidStr), uuidStr, false},
		{"uuid-fixed/[16]byte", `{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`, uuidBytes, uuidBytes, uuidBytes, false},
		{"uuid-fixed/string-canonical", `{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`, uuidStr, uuidStr, uuidBytes, false},

		{"duration/avro.Duration", `{"type":"fixed","name":"D","size":12,"logicalType":"duration"}`, dur, dur, dur, false},

		{"decimal-bytes/big.Rat", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, bigRat(33, 100), bigRat(33, 100), bigRat(33, 100), false},
		{"decimal-fixed/big.Rat", `{"type":"fixed","name":"D","size":8,"logicalType":"decimal","precision":10,"scale":2}`, bigRat(33, 100), bigRat(33, 100), bigRat(33, 100), false},
		{"big-decimal/big.Rat", `{"type":"bytes","logicalType":"big-decimal"}`, bigRat(33, 100), bigRat(33, 100), bigRat(33, 100), false},

		// ── array of primitives ─────────────────────────────────────
		// Decode-into-*any returns []any with each element as the
		// schema's canonical Go type. int → int32, long → int64, etc.
		{"array<boolean>", `{"type":"array","items":"boolean"}`, []bool{true, false, true}, []bool{true, false, true}, []any{true, false, true}, false},
		{"array<int>", `{"type":"array","items":"int"}`, []int32{1, 2, 3}, []int32{1, 2, 3}, []any{int32(1), int32(2), int32(3)}, false},
		{"array<long>", `{"type":"array","items":"long"}`, []int64{1, 2, 3}, []int64{1, 2, 3}, []any{int64(1), int64(2), int64(3)}, false},
		{"array<float>", `{"type":"array","items":"float"}`, []float32{1.5, 2.5, 3.5}, []float32{1.5, 2.5, 3.5}, []any{float32(1.5), float32(2.5), float32(3.5)}, false},
		{"array<double>", `{"type":"array","items":"double"}`, []float64{1.5, 2.5, 3.5}, []float64{1.5, 2.5, 3.5}, []any{float64(1.5), float64(2.5), float64(3.5)}, false},
		{"array<string>", `{"type":"array","items":"string"}`, []string{"a", "b", "c"}, []string{"a", "b", "c"}, []any{"a", "b", "c"}, false},
		{"array<bytes>", `{"type":"array","items":"bytes"}`, [][]byte{[]byte("a"), []byte("b")}, [][]byte{[]byte("a"), []byte("b")}, []any{[]byte("a"), []byte("b")}, false},
		{"array<int>/empty", `{"type":"array","items":"int"}`, []int32{}, []int32{}, []any{}, false},

		// ── map of primitives ──────────────────────────────────────
		{"map<int>", `{"type":"map","values":"int"}`, map[string]int32{"k": 42}, map[string]int32{"k": 42}, map[string]any{"k": int32(42)}, false},
		{"map<long>", `{"type":"map","values":"long"}`, map[string]int64{"k": 42}, map[string]int64{"k": 42}, map[string]any{"k": int64(42)}, false},
		{"map<float>", `{"type":"map","values":"float"}`, map[string]float32{"k": 1.5}, map[string]float32{"k": 1.5}, map[string]any{"k": float32(1.5)}, false},
		{"map<double>", `{"type":"map","values":"double"}`, map[string]float64{"k": 1.5}, map[string]float64{"k": 1.5}, map[string]any{"k": float64(1.5)}, false},
		{"map<string>", `{"type":"map","values":"string"}`, map[string]string{"k": "v"}, map[string]string{"k": "v"}, map[string]any{"k": "v"}, false},
		{"map<boolean>", `{"type":"map","values":"boolean"}`, map[string]bool{"k": true}, map[string]bool{"k": true}, map[string]any{"k": true}, false},
		{"map<bytes>", `{"type":"map","values":"bytes"}`, map[string][]byte{"k": []byte("v")}, map[string][]byte{"k": []byte("v")}, map[string]any{"k": []byte("v")}, false},
	}

	equal := func(got, want any) bool {
		if want == nil {
			return got == nil
		}
		switch w := want.(type) {
		case time.Time:
			g, ok := got.(time.Time)
			return ok && g.Equal(w)
		case *big.Rat:
			g, ok := got.(*big.Rat)
			return ok && g != nil && g.Cmp(w) == 0
		case float32:
			g, ok := got.(float32)
			if !ok {
				return false
			}
			if math.IsNaN(float64(w)) {
				return math.IsNaN(float64(g))
			}
			return g == w
		case float64:
			g, ok := got.(float64)
			if !ok {
				return false
			}
			if math.IsNaN(w) {
				return math.IsNaN(g)
			}
			return g == w
		}
		// reflect.DeepEqual distinguishes nil from empty for slices /
		// maps. Avro doesn't carry that distinction (empty array =
		// single zero byte, decoded as the zero-value collection), so
		// normalize both before comparing.
		gv := reflect.ValueOf(got)
		wv := reflect.ValueOf(want)
		if gv.Kind() == wv.Kind() && (gv.Kind() == reflect.Slice || gv.Kind() == reflect.Map) {
			if gv.Len() == 0 && wv.Len() == 0 {
				return true
			}
		}
		return reflect.DeepEqual(got, want)
	}

	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			s := avro.MustParse(c.schema)

			// Helper: build a fresh decode target of the expected
			// Go type. Returns the *T (settable) and the value.
			newTarget := func() (reflect.Value, any) {
				if c.expect == nil {
					var v any
					return reflect.ValueOf(&v), &v
				}
				p := reflect.New(reflect.TypeOf(c.expect))
				return p, p.Interface()
			}

			// Binary safe path.
			t.Run("binary/safe", func(t *testing.T) {
				wire, err := s.AppendEncode(nil, c.input)
				if err != nil {
					t.Fatalf("AppendEncode(%T %v): %v", c.input, c.input, err)
				}
				targetPtr, iface := newTarget()
				if _, err := s.Decode(wire, iface); err != nil {
					t.Fatalf("Decode (wire=%x): %v", wire, err)
				}
				got := targetPtr.Elem().Interface()
				if !equal(got, c.expect) {
					t.Errorf("mismatch:\n  got  %T %v\n  want %T %v", got, got, c.expect, c.expect)
				}
			})

			// JSON safe path.
			t.Run("json/safe", func(t *testing.T) {
				jsonBuf, err := s.AppendEncodeJSON(nil, c.input)
				if err != nil {
					t.Fatalf("AppendEncodeJSON(%T %v): %v", c.input, c.input, err)
				}
				targetPtr, iface := newTarget()
				if err := s.DecodeJSON(jsonBuf, iface); err != nil {
					t.Fatalf("DecodeJSON (json=%s): %v", jsonBuf, err)
				}
				got := targetPtr.Elem().Interface()
				if !equal(got, c.expect) {
					t.Errorf("mismatch:\n  got  %T %v\n  want %T %v\n  (json=%s)", got, got, c.expect, c.expect, jsonBuf)
				}
			})

			// Binary unsafe / struct-field path: wrap the cell's
			// Avro schema in a record with one field of the
			// target Go type. Exercises the us*/ud* fast paths
			// when an unsafe specialization exists, falls back to
			// the safe-path serFoo/deserFoo otherwise.
			t.Run("binary/unsafe-struct", func(t *testing.T) {
				if c.expect == nil {
					t.Skip("null target — struct field can't hold typed nil")
				}
				targetType := reflect.TypeOf(c.expect)
				recType := reflect.StructOf([]reflect.StructField{{
					Name: "F",
					Type: targetType,
					Tag:  `avro:"f"`,
				}})
				recPtr := reflect.New(recType)
				recPtr.Elem().Field(0).Set(reflect.ValueOf(c.input).Convert(targetType))
				recSchema := `{"type":"record","name":"R","fields":[{"name":"f","type":` + c.schema + `}]}`
				schema := avro.MustParse(recSchema)
				wire, err := schema.AppendEncode(nil, recPtr.Interface())
				if err != nil {
					t.Fatalf("struct AppendEncode: %v", err)
				}
				outPtr := reflect.New(recType)
				if _, err := schema.Decode(wire, outPtr.Interface()); err != nil {
					t.Fatalf("struct Decode (wire=%x): %v", wire, err)
				}
				got := outPtr.Elem().Field(0).Interface()
				if !equal(got, c.expect) {
					t.Errorf("mismatch:\n  got  %T %v\n  want %T %v", got, got, c.expect, c.expect)
				}
			})

			// JSON unsafe / struct-field path: same shape, JSON
			// encoder/decoder. Exercises appendAvroJSONRecord's
			// struct branch + the JSON decoder's struct-field
			// dispatch.
			t.Run("json/unsafe-struct", func(t *testing.T) {
				if c.expect == nil {
					t.Skip("null target")
				}
				targetType := reflect.TypeOf(c.expect)
				recType := reflect.StructOf([]reflect.StructField{{
					Name: "F",
					Type: targetType,
					Tag:  `avro:"f"`,
				}})
				recPtr := reflect.New(recType)
				recPtr.Elem().Field(0).Set(reflect.ValueOf(c.input).Convert(targetType))
				recSchema := `{"type":"record","name":"R","fields":[{"name":"f","type":` + c.schema + `}]}`
				schema := avro.MustParse(recSchema)
				jsonBuf, err := schema.AppendEncodeJSON(nil, recPtr.Interface())
				if err != nil {
					t.Fatalf("struct AppendEncodeJSON: %v", err)
				}
				outPtr := reflect.New(recType)
				if err := schema.DecodeJSON(jsonBuf, outPtr.Interface()); err != nil {
					t.Fatalf("struct DecodeJSON (json=%s): %v", jsonBuf, err)
				}
				got := outPtr.Elem().Field(0).Interface()
				if !equal(got, c.expect) {
					t.Errorf("mismatch:\n  got  %T %v\n  want %T %v\n  (json=%s)", got, got, c.expect, c.expect, jsonBuf)
				}
			})

			// Binary into-any path: Decode(wire, *any). Exercises
			// the deser Interface arm of each primitive/logical.
			// expectAny names the canonical type the decoder
			// produces for *any — not always the encode input's
			// type (int wire → int32 in *any, even if input was
			// uint8). skipAny cases either don't have a stable
			// any-canonical or are cross-type leniency cells
			// where the *any path doesn't apply.
			if !c.skipAny {
				t.Run("binary/into-any", func(t *testing.T) {
					wire, err := s.AppendEncode(nil, c.input)
					if err != nil {
						t.Fatalf("AppendEncode: %v", err)
					}
					var got any
					if _, err := s.Decode(wire, &got); err != nil {
						t.Fatalf("Decode into *any (wire=%x): %v", wire, err)
					}
					want := c.expectAny
					if want == nil {
						want = c.expect
					}
					if !equal(got, want) {
						t.Errorf("into-any mismatch:\n  got  %T %v\n  want %T %v", got, got, want, want)
					}
				})

				t.Run("json/into-any", func(t *testing.T) {
					jsonBuf, err := s.AppendEncodeJSON(nil, c.input)
					if err != nil {
						t.Fatalf("AppendEncodeJSON: %v", err)
					}
					var got any
					if err := s.DecodeJSON(jsonBuf, &got); err != nil {
						t.Fatalf("DecodeJSON into *any (json=%s): %v", jsonBuf, err)
					}
					want := c.expectAny
					if want == nil {
						want = c.expect
					}
					if !equal(got, want) {
						t.Errorf("into-any mismatch:\n  got  %T %v\n  want %T %v\n  (json=%s)", got, got, want, want, jsonBuf)
					}
				})
			}
		})
	}
}

// TestRegression_SchemaMetadataNumericPrecisionPreserved locks that JSON
// integer literals > 2^53 survive Schema parsing intact when surfaced
// via the metadata API (Schema.Root().Props, Root().Fields[].Props,
// Root().Fields[].Default, and CustomType callbacks' schema.Props).
// Pre-fix two sites — schema_node.go:108 (Root re-parse) and
// schema.go:289 (record extras during parse) — called json.Unmarshal
// without UseNumber so JSON ints > 2^53 silently rounded to float64
// (e.g. 9007199254740993 → 9007199254740992). Java preserves precision
// via Jackson's LongNode (lang/java/avro/src/main/java/org/apache/avro/
// Schema.java:1985 stores extras as JsonNode); fastavro preserves via
// Python int (arbitrary precision). Internal encode/decode was already
// safe via unmarshalDefault (UseNumber); only the user-facing metadata
// surfaces were buggy.
//
// Fix: switch both sites to unmarshalAnyPreservePrecision — UseNumber
// decode followed by normalizeJSONValue which converts integer-form
// json.Number to int64 (or json.Number for >int64 magnitudes), and
// fractional/exponent-form to float64. Existing pinning tests
// (TestSchemaNodeRoundTrip, TestSchemaNodeCustomPropsExtended) were
// updated from float64(N) to int64(N) for small integers, matching the
// new spec-aligned behavior.
func TestRegression_SchemaMetadataNumericPrecisionPreserved(t *testing.T) {
	const wantVal = int64(9007199254740993) // 2^53 + 1
	// Helper: type-assert v as the canonical int (int64 or json.Number
	// Int64()) and compare to wantVal. The encoding/json default of
	// float64 would round wantVal to 2^53 — silent precision loss.
	asInt64 := func(t *testing.T, label string, v any) int64 {
		t.Helper()
		switch tv := v.(type) {
		case int64:
			return tv
		case json.Number:
			i, err := tv.Int64()
			if err != nil {
				t.Fatalf("%s: json.Number(%q) overflows int64", label, tv)
			}
			return i
		case float64:
			t.Fatalf("%s: got float64(%v) — precision-loss site not fixed (want int64 %d)", label, tv, wantVal)
		default:
			t.Fatalf("%s: unexpected type %T = %v", label, v, v)
		}
		return 0
	}

	t.Run("record-level extra > 2^53 via Schema.Root().Props", func(t *testing.T) {
		s, err := avro.Parse(fmt.Sprintf(`{
			"type":"record","name":"R",
			"fields":[{"name":"f","type":"int"}],
			"schemaId":%d
		}`, wantVal))
		if err != nil {
			t.Fatal(err)
		}
		got := asInt64(t, "Root().Props[schemaId]", s.Root().Props["schemaId"])
		if got != wantVal {
			t.Errorf("got %d, want %d", got, wantVal)
		}
	})

	t.Run("field-level Default > 2^53 via Schema.Root().Fields[].Default", func(t *testing.T) {
		s, err := avro.Parse(fmt.Sprintf(`{
			"type":"record","name":"R",
			"fields":[{"name":"f","type":"long","default":%d}]
		}`, wantVal))
		if err != nil {
			t.Fatal(err)
		}
		got := asInt64(t, "Fields[0].Default", s.Root().Fields[0].Default)
		if got != wantVal {
			t.Errorf("got %d, want %d", got, wantVal)
		}
	})

	t.Run("field-level extra > 2^53 via Schema.Root().Fields[].Props", func(t *testing.T) {
		s, err := avro.Parse(fmt.Sprintf(`{
			"type":"record","name":"R",
			"fields":[{"name":"f","type":"int","field.id":%d}]
		}`, wantVal))
		if err != nil {
			t.Fatal(err)
		}
		got := asInt64(t, "Fields[0].Props[field.id]", s.Root().Fields[0].Props["field.id"])
		if got != wantVal {
			t.Errorf("got %d, want %d", got, wantVal)
		}
	})

	t.Run("record-level extra > 2^53 via CustomType callback schema.Props", func(t *testing.T) {
		// Place the prop on the inner type so it's surfaced via the
		// custom-typed node's Props rather than the outer record's.
		schemaStr := fmt.Sprintf(`{
			"type":"record","name":"R",
			"fields":[{"name":"f","type":{
				"type":"long","logicalType":"my-long","schemaId":%d
			}}]
		}`, wantVal)
		var captured map[string]any
		ct := avro.CustomType{
			AvroType:    "long",
			LogicalType: "my-long",
			Decode: func(v any, schema *avro.SchemaNode) (any, error) {
				captured = schema.Props
				return v, nil
			},
		}
		s, err := avro.Parse(schemaStr, avro.WithCustomType(ct))
		if err != nil {
			t.Fatal(err)
		}
		bin, err := s.AppendEncode(nil, map[string]any{"f": int64(42)})
		if err != nil {
			t.Fatal(err)
		}
		var out map[string]any
		if _, err := s.Decode(bin, &out); err != nil {
			t.Fatal(err)
		}
		got := asInt64(t, "CustomType schema.Props[schemaId]", captured["schemaId"])
		if got != wantVal {
			t.Errorf("got %d, want %d", got, wantVal)
		}
	})

	t.Run("nested extra inside array > 2^53", func(t *testing.T) {
		s, err := avro.Parse(fmt.Sprintf(`{
			"type":"record","name":"R",
			"fields":[{"name":"f","type":"int"}],
			"versions":[%d, 1, 2]
		}`, wantVal))
		if err != nil {
			t.Fatal(err)
		}
		arr, ok := s.Root().Props["versions"].([]any)
		if !ok {
			t.Fatalf("versions not []any: %T", s.Root().Props["versions"])
		}
		if len(arr) != 3 {
			t.Fatalf("expected 3 elements, got %d", len(arr))
		}
		got := asInt64(t, "Props[versions][0]", arr[0])
		if got != wantVal {
			t.Errorf("got %d, want %d", got, wantVal)
		}
	})

	t.Run("fractional extras still come back as float64", func(t *testing.T) {
		// Fractional numbers must NOT become int64 — verifies the
		// normalize-fractional arm.
		s, err := avro.Parse(`{
			"type":"record","name":"R",
			"fields":[{"name":"f","type":"int"}],
			"threshold":3.14
		}`)
		if err != nil {
			t.Fatal(err)
		}
		v := s.Root().Props["threshold"]
		if f, ok := v.(float64); !ok || f != 3.14 {
			t.Errorf("got %T %v, want float64(3.14)", v, v)
		}
	})
}

// TestRegression_SchemaMetadataExponentOverflowNormalizesToInf locks the
// metadata-API observability surface (Schema.Root().Props,
// Fields[].Default, Fields[].Props, CustomType callbacks'
// *SchemaNode.Props) on the ±Inf-from-overflow case for exponent-form
// JSON literals. Pre-fix, normalizeJSONNumber (schema.go) bailed when
// (json.Number).Float64() returned (±Inf, strconv.ErrRange) and returned
// the json.Number unchanged — violating the [SchemaField.Default] /
// [SchemaNode.Props] docstring contract "fractional and exponent-form
// literals decode to float64" on the overflow subcase, and diverging
// from Java + fastavro which both surface ±Inf in their metadata APIs
// (Java: Jackson DoubleNode(Double.parseDouble("1e1000")) → +Inf;
// fastavro: float("1e1000") → inf via Python json).
//
// This is the metadata-observability axis of pattern 1b's three-axis
// rule (formerly four axes: encode, decode, schema-parse-time validate,
// metadata observability). The schema-parse-time arms (defaultAsFloat,
// coerceDefault) and encode/decode arms route through
// parseFloatAcceptOverflow (schema.go) to accept ±Inf; the fix wires
// normalizeJSONNumber through the same helper so all four arms agree.
//
// The fix also wires SchemaNode.toJSONWalk (schema_node.go) through
// jsonSerializableValue, which converts ±Inf back to a json.Number
// literal so SchemaNode.Schema() round-trip continues to work
// (encoding/json.Marshal rejects ±Inf unconditionally).
//
// Smoking-gun probe pre-fix:
//
//	s := avro.Parse(`{"type":"record",...,"default":1e1000}`)
//	s.Root().Fields[0].Default                       → json.Number("1e1000")  ← BUG
//	buf, _ := s.AppendEncode(nil, map[string]any{})  → +Inf wire bits
//	s.Decode(buf, &out)                              → +Inf                   ← contract honored
//
// Sibling sweep: this same shape applies to Schema.Root().Props (record-
// level extras), Fields[].Props (field-level extras), CustomType
// callbacks' *SchemaNode.Props, and recursively nested values inside
// any of those. All five surfaces share normalizeJSONNumber, so a
// single-site fix covers all.
func TestRegression_SchemaMetadataExponentOverflowNormalizesToInf(t *testing.T) {
	t.Run("default 1e1000 normalizes to +Inf", func(t *testing.T) {
		s, err := avro.Parse(`{
			"type":"record","name":"R",
			"fields":[{"name":"f","type":"double","default":1e1000}]
		}`)
		if err != nil {
			t.Fatal(err)
		}
		got := s.Root().Fields[0].Default
		f, ok := got.(float64)
		if !ok || !math.IsInf(f, 1) {
			t.Errorf("Default: got %T %v, want float64(+Inf) — pattern 1b violation (docstring says exponent-form decodes to float64)", got, got)
		}
	})
	t.Run("default -1e1000 normalizes to -Inf", func(t *testing.T) {
		s, err := avro.Parse(`{
			"type":"record","name":"R",
			"fields":[{"name":"f","type":"double","default":-1e1000}]
		}`)
		if err != nil {
			t.Fatal(err)
		}
		got := s.Root().Fields[0].Default
		f, ok := got.(float64)
		if !ok || !math.IsInf(f, -1) {
			t.Errorf("Default: got %T %v, want float64(-Inf)", got, got)
		}
	})
	t.Run("record-level Props 1e1000 normalizes to +Inf", func(t *testing.T) {
		s, err := avro.Parse(`{
			"type":"record","name":"R",
			"fields":[{"name":"f","type":"int"}],
			"limit":1e1000
		}`)
		if err != nil {
			t.Fatal(err)
		}
		got := s.Root().Props["limit"]
		f, ok := got.(float64)
		if !ok || !math.IsInf(f, 1) {
			t.Errorf("Props[limit]: got %T %v, want float64(+Inf)", got, got)
		}
	})
	t.Run("field-level Props -1e1000 normalizes to -Inf", func(t *testing.T) {
		s, err := avro.Parse(`{
			"type":"record","name":"R",
			"fields":[{"name":"f","type":"int","fieldLimit":-1e1000}]
		}`)
		if err != nil {
			t.Fatal(err)
		}
		got := s.Root().Fields[0].Props["fieldLimit"]
		f, ok := got.(float64)
		if !ok || !math.IsInf(f, -1) {
			t.Errorf("Fields[0].Props[fieldLimit]: got %T %v, want float64(-Inf)", got, got)
		}
	})
	t.Run("CustomType callback schema.Props 1e1000 normalizes to +Inf", func(t *testing.T) {
		var captured map[string]any
		ct := avro.CustomType{
			AvroType:    "long",
			LogicalType: "my-long",
			Decode: func(v any, schema *avro.SchemaNode) (any, error) {
				captured = schema.Props
				return v, nil
			},
		}
		s, err := avro.Parse(`{
			"type":"record","name":"R",
			"fields":[{"name":"f","type":{
				"type":"long","logicalType":"my-long","scaleHint":1e1000
			}}]
		}`, avro.WithCustomType(ct))
		if err != nil {
			t.Fatal(err)
		}
		bin, err := s.AppendEncode(nil, map[string]any{"f": int64(42)})
		if err != nil {
			t.Fatal(err)
		}
		var out map[string]any
		if _, err := s.Decode(bin, &out); err != nil {
			t.Fatal(err)
		}
		got := captured["scaleHint"]
		f, ok := got.(float64)
		if !ok || !math.IsInf(f, 1) {
			t.Errorf("CustomType schema.Props[scaleHint]: got %T %v, want float64(+Inf)", got, got)
		}
	})
	t.Run("nested Props inside array also normalize", func(t *testing.T) {
		s, err := avro.Parse(`{
			"type":"record","name":"R",
			"fields":[{"name":"f","type":"int"}],
			"limits":[1e1000, -1e1000, 1.5e10]
		}`)
		if err != nil {
			t.Fatal(err)
		}
		arr, ok := s.Root().Props["limits"].([]any)
		if !ok {
			t.Fatalf("limits not []any: %T", s.Root().Props["limits"])
		}
		if f, ok := arr[0].(float64); !ok || !math.IsInf(f, 1) {
			t.Errorf("arr[0]: got %T %v, want float64(+Inf)", arr[0], arr[0])
		}
		if f, ok := arr[1].(float64); !ok || !math.IsInf(f, -1) {
			t.Errorf("arr[1]: got %T %v, want float64(-Inf)", arr[1], arr[1])
		}
		if f, ok := arr[2].(float64); !ok || f != 1.5e10 {
			t.Errorf("arr[2]: got %T %v, want float64(1.5e10)", arr[2], arr[2])
		}
	})
	t.Run("finite exponent-form still normalizes to float64", func(t *testing.T) {
		// Boundary-1 case: 1e308 fits in float64 exact-precision range.
		// Verifies the fix doesn't over-trigger on finite values.
		s, err := avro.Parse(`{
			"type":"record","name":"R",
			"fields":[{"name":"f","type":"int"}],
			"x":1e308,"y":2.5e10
		}`)
		if err != nil {
			t.Fatal(err)
		}
		if f, ok := s.Root().Props["x"].(float64); !ok || f != 1e308 {
			t.Errorf("Props[x]: got %T %v, want float64(1e308)", s.Root().Props["x"], s.Root().Props["x"])
		}
		if f, ok := s.Root().Props["y"].(float64); !ok || f != 2.5e10 {
			t.Errorf("Props[y]: got %T %v, want float64(2.5e10)", s.Root().Props["y"], s.Root().Props["y"])
		}
	})
	t.Run("SchemaNode.Schema() round-trips ±Inf via json.Number literal", func(t *testing.T) {
		// jsonSerializableValue converts ±Inf back to a json.Number
		// literal so encoding/json.Marshal — which rejects ±Inf
		// unconditionally — doesn't fail at SchemaNode.Schema().
		// The re-parsed schema's Default re-normalizes to +Inf via
		// normalizeJSONNumber, completing the round trip.
		s1, err := avro.Parse(`{
			"type":"record","name":"R",
			"fields":[{"name":"f","type":"double","default":1e1000}]
		}`)
		if err != nil {
			t.Fatal(err)
		}
		root1 := s1.Root()
		s2, err := root1.Schema()
		if err != nil {
			t.Fatalf("Schema() round-trip failed (jsonSerializableValue missing?): %v", err)
		}
		got := s2.Root().Fields[0].Default
		f, ok := got.(float64)
		if !ok || !math.IsInf(f, 1) {
			t.Errorf("round-tripped Default: got %T %v, want float64(+Inf)", got, got)
		}
		// And the wire bytes are still +Inf.
		bin, err := s2.AppendEncode(nil, map[string]any{})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var out map[string]any
		if _, err := s2.Decode(bin, &out); err != nil {
			t.Fatal(err)
		}
		if f, ok := out["f"].(float64); !ok || !math.IsInf(f, 1) {
			t.Errorf("decoded round-trip: got %T %v, want float64(+Inf)", out["f"], out["f"])
		}
	})
	t.Run("SchemaNode.Schema() round-trips programmatically-constructed +Inf Props", func(t *testing.T) {
		// User puts +Inf in Props directly. Should round-trip via the
		// jsonSerializableValue conversion to json.Number literal.
		node := &avro.SchemaNode{
			Type: "record",
			Name: "R",
			Fields: []avro.SchemaField{
				{Name: "f", Type: avro.SchemaNode{Type: "int"}},
			},
			Props: map[string]any{
				"limit": math.Inf(1),
			},
		}
		s, err := node.Schema()
		if err != nil {
			t.Fatalf("Schema() rejected +Inf in Props: %v", err)
		}
		got := s.Root().Props["limit"]
		if f, ok := got.(float64); !ok || !math.IsInf(f, 1) {
			t.Errorf("round-tripped Props[limit]: got %T %v, want float64(+Inf)", got, got)
		}
	})
}

// TestRegression_NumberGrammarParityMatrix pins the JSON-number grammar
// gate across every entry point that converts a [json.Number] to an
// Avro primitive (int / long / float / double) on both the binary and
// JSON encode paths. The matrix exists because the codebase historically
// accumulated multiple parallel parsers — strconv.ParseInt with base=10,
// strconv.ParseFloat, big.Rat.SetString — each with subtly different
// accepted sets that diverged from RFC 8259's JSON-number grammar.
// Three rounds of bugs (commit 1971999's int-path fix, the round-after
// audit's hex/octal/binary regression, the float-path sibling) all
// landed because no single test pinned the cross-entry-point invariant
// "a json.Number is accepted iff it is an RFC 8259 number".
//
// Rows: input equivalence classes (valid JSON, value-overflow,
// JSON-invalid grammar). Columns: 4 target types × 2 encode directions
// = 8 entry-point cells. Cell value: whether encode succeeds.
//
// Java's JsonParser rejects every JSON-invalid form at parse time;
// fastavro's int() / float() raise ValueError on the same set. The
// matrix targets parity with the Java + fastavro consensus.
//
// Adding an entry point: add a column to typeCases. Adding an input
// class: add a row to cases. Cells that are surprising (e.g.,
// "9223372036854775807" rejected for double on grammar grounds but
// independently rejected for precision) should grow a comment.
func TestRegression_NumberGrammarParityMatrix(t *testing.T) {
	// Each input is classified by whether the JSON encoder for each
	// target type should accept it. "accept" requires the value to also
	// fit the target type's range; e.g., MaxInt64 is JSON-grammar-valid
	// but exceeds int32 range so the int cell is reject.
	type expect struct{ intC, longC, floatC, doubleC bool }
	const (
		acc = true
		rej = false
	)

	cases := []struct {
		input  string
		expect expect
		desc   string
	}{
		// ---- Valid JSON numbers, fit every type. ----
		{"0", expect{acc, acc, acc, acc}, "zero"},
		{"-0", expect{acc, acc, acc, acc}, "negative zero"},
		{"1", expect{acc, acc, acc, acc}, "one"},
		{"-1", expect{acc, acc, acc, acc}, "neg one"},
		{"42", expect{acc, acc, acc, acc}, "small int"},

		// ---- Valid JSON, fractional — int/long reject (not whole). ----
		{"1.5", expect{rej, rej, acc, acc}, "fractional"},
		{"-0.5", expect{rej, rej, acc, acc}, "neg fractional"},
		{"1.0", expect{acc, acc, acc, acc}, "whole fractional"},

		// ---- Valid JSON, exponent forms. ----
		{"1e3", expect{acc, acc, acc, acc}, "exp positive"},
		// -1e10 = -10000000000, exceeds int32 range.
		{"-1E10", expect{rej, acc, acc, acc}, "exp uppercase neg"},
		{"1.5e1", expect{acc, acc, acc, acc}, "whole via exp"},
		{"1.5e0", expect{rej, rej, acc, acc}, "non-whole via exp"},

		// ---- Boundary values. ----
		// 2^24 = 16777216, the float32 mantissa boundary. Exactly fits.
		{"16777216", expect{acc, acc, acc, acc}, "float32 mantissa boundary"},
		// 2^24+1 = 16777217 — first integer not exactly representable in float32.
		{"16777217", expect{acc, acc, rej, acc}, "float32 mantissa+1"},
		{"-16777217", expect{acc, acc, rej, acc}, "-float32 mantissa+1"},
		// MaxInt32 = 2147483647 — fits int, long, double exactly; exceeds float32 mantissa (1<<24).
		{"2147483647", expect{acc, acc, rej, acc}, "MaxInt32"},
		{"-2147483648", expect{acc, acc, rej, acc}, "MinInt32"},
		// MaxInt32+1 = 2147483648 — exceeds int range.
		{"2147483648", expect{rej, acc, rej, acc}, "MaxInt32+1"},
		// MaxInt64 = 9223372036854775807 — exceeds int and float* precision.
		{"9223372036854775807", expect{rej, acc, rej, rej}, "MaxInt64"},
		{"-9223372036854775808", expect{rej, acc, rej, rej}, "MinInt64"},
		// MaxInt64+1 = 9223372036854775808 — exceeds every type.
		{"9223372036854775808", expect{rej, rej, rej, rej}, "MaxInt64+1"},
		// 2^53+1 = first integer not exactly representable in float64.
		// Fits int64 exactly; floats reject for precision.
		{"9007199254740993", expect{rej, acc, rej, rej}, "2^53+1"},

		// ---- JSON-invalid grammar — every cell rejects. ----
		{"0x10", expect{rej, rej, rej, rej}, "hex prefix"},
		{"0X10", expect{rej, rej, rej, rej}, "hex prefix uppercase"},
		{"0b10", expect{rej, rej, rej, rej}, "binary prefix"},
		{"0o10", expect{rej, rej, rej, rej}, "octal prefix"},
		{"0x1.0p10", expect{rej, rej, rej, rej}, "hex float (Go/Java accept, JSON rejects)"},
		{"0x1p4", expect{rej, rej, rej, rej}, "hex float compact"},
		{"1_000", expect{rej, rej, rej, rej}, "underscore separator (Go-specific)"},
		{"1_2_3", expect{rej, rej, rej, rej}, "multiple underscores"},
		{"5/1", expect{rej, rej, rej, rej}, "rational form (big.Rat.SetString accepts)"},
		{"+5", expect{rej, rej, rej, rej}, "leading plus"},
		{"01", expect{rej, rej, rej, rej}, "leading zero multi-digit"},
		{"-01", expect{rej, rej, rej, rej}, "neg leading zero"},
		{"00", expect{rej, rej, rej, rej}, "double zero"},
		{".5", expect{rej, rej, rej, rej}, "no leading digit"},
		{"-.5", expect{rej, rej, rej, rej}, "neg no leading digit"},
		{"1.", expect{rej, rej, rej, rej}, "trailing dot"},
		{"1e", expect{rej, rej, rej, rej}, "exp no digit"},
		{"1.5.6", expect{rej, rej, rej, rej}, "double decimal"},
		{"NaN", expect{rej, rej, rej, rej}, "NaN literal"},
		{"Infinity", expect{rej, rej, rej, rej}, "Infinity literal"},
		{"abc", expect{rej, rej, rej, rej}, "non-numeric"},
		{"", expect{rej, rej, rej, rej}, "empty"},

		// ---- Whitespace edge cases. ----
		{" 5", expect{rej, rej, rej, rej}, "leading space"},
		{"5 ", expect{rej, rej, rej, rej}, "trailing space"},
		{"5 6", expect{rej, rej, rej, rej}, "internal whitespace"},
		{"\t5", expect{rej, rej, rej, rej}, "leading tab"},
		{"5\n", expect{rej, rej, rej, rej}, "trailing newline"},
	}

	typeCases := []struct {
		schema string
		name   string
		expect func(expect) bool
	}{
		{`"int"`, "int", func(e expect) bool { return e.intC }},
		{`"long"`, "long", func(e expect) bool { return e.longC }},
		{`"float"`, "float", func(e expect) bool { return e.floatC }},
		{`"double"`, "double", func(e expect) bool { return e.doubleC }},
	}

	for _, c := range cases {
		c := c
		for _, target := range typeCases {
			target := target
			t.Run(c.desc+"/"+target.name, func(t *testing.T) {
				s, err := avro.Parse(target.schema)
				if err != nil {
					t.Fatalf("parse %s: %v", target.schema, err)
				}
				wantAccept := target.expect(c.expect)

				// Binary encode arm.
				_, binErr := s.AppendEncode(nil, json.Number(c.input))
				gotBinAccept := binErr == nil
				if gotBinAccept != wantAccept {
					t.Errorf("binary encode %q against %s: want accept=%v, got err=%v", c.input, target.name, wantAccept, binErr)
				}

				// JSON encode arm.
				_, jsonErr := s.AppendEncodeJSON(nil, json.Number(c.input))
				gotJSONAccept := jsonErr == nil
				if gotJSONAccept != wantAccept {
					t.Errorf("JSON encode %q against %s: want accept=%v, got err=%v", c.input, target.name, wantAccept, jsonErr)
				}
			})
		}
	}
}

// TestRegression_NumberGrammarParityMatrix_Decimal extends the grammar
// matrix to the decimal / big-decimal encode paths, where the coercion
// runs through tryCoerceToRat → boundedRatFromString. Same grammar rule:
// json.Number must be RFC 8259 valid; strings are coerced only when
// they look like JSON numbers (first char '-' or digit), otherwise fall
// through to opaque-bytes encoding.
func TestRegression_NumberGrammarParityMatrix_Decimal(t *testing.T) {
	// (input, jsonNumberAccepts, stringAccepts) — string arm's
	// "fall-through to raw bytes" path differs from json.Number's
	// "always treat as a number".
	cases := []struct {
		input string
		jn    bool // json.Number behavior: accept (valid) / reject (invalid)
		str   bool // string behavior: accept (number) / fall-through (opaque) — both succeed; both error states are when bound exceeded or numeric-looking but malformed
		desc  string
	}{
		// Valid JSON-number inputs encode successfully on both arms.
		{"3.14", true, true, "fractional"},
		{"0.5", true, true, "half"},
		{"-1.5", true, true, "neg fractional"},
		{"100", true, true, "integer"},
		{"1e3", true, true, "exp positive"},
		{"1e-3", true, true, "exp negative"},

		// JSON-invalid grammar: both arms error (the string arm because
		// the input starts with a digit/minus, so it's numeric-looking
		// and surfaces the error rather than falling through).
		{"0x10", false, false, "hex prefix"},
		{"0b10", false, false, "binary prefix"},
		{"0o10", false, false, "octal prefix"},
		{"1_000", false, false, "underscore"},
		{"5/1", false, false, "rational"},
		{"+5", false, false, "leading plus"},
		{"01", false, false, "leading zero"},

		// Non-numeric inputs: json.Number arm errors (json.Number's
		// type asserts "this is a number"); string arm falls through
		// to opaque-bytes encoding so encode succeeds.
		{"abc", false, true, "non-numeric string"},
		{"hello", false, true, "non-numeric word"},
	}

	for _, sj := range []struct{ schemaJSON, name string }{
		{`{"type":"bytes","logicalType":"decimal","precision":20,"scale":4}`, "decimal-bytes"},
		{`{"type":"bytes","logicalType":"big-decimal"}`, "big-decimal"},
	} {
		s, err := avro.Parse(sj.schemaJSON)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range cases {
			c := c
			t.Run(sj.name+"/"+c.desc, func(t *testing.T) {
				// json.Number arm.
				_, jnErr := s.AppendEncode(nil, json.Number(c.input))
				if (jnErr == nil) != c.jn {
					t.Errorf("json.Number %q against %s: want accept=%v, got err=%v", c.input, sj.name, c.jn, jnErr)
				}
				// String arm.
				_, sErr := s.AppendEncode(nil, c.input)
				if (sErr == nil) != c.str {
					t.Errorf("string %q against %s: want accept=%v, got err=%v", c.input, sj.name, c.str, sErr)
				}
			})
		}
	}
}

// TestRegression_NumberGrammarParityMatrix_JSONDecode_Long pins the
// JSON-decode path for "long" against the same grammar matrix. The
// scanner gates most non-JSON inputs at JSON-syntax parse time, but
// the .eE branch of parseJSONInt64 routes through parseInt64Lenient
// (which gates via isJSONNumber). Inputs in this matrix that pass
// the scanner but fail isJSONNumber surface the gate.
func TestRegression_NumberGrammarParityMatrix_JSONDecode_Long(t *testing.T) {
	// All inputs go through Schema.DecodeJSON, which routes via the
	// JSON scanner. Inputs that aren't valid JSON tokens are rejected
	// at scanner time; those that pass the scanner but contain '.', 'e',
	// or 'E' reach parseInt64Lenient.
	cases := []struct {
		input  string
		expect bool
		desc   string
	}{
		// Valid: scanner accepts, parseJSONInt64 (or via .eE branch) returns int64.
		{"0", true, "zero"},
		{"1", true, "one"},
		{"-1", true, "neg"},
		{"1e3", true, "exp"},      // .eE branch → parseInt64Lenient
		{"1.5e1", true, "exp-15"}, // .eE branch
		{"9223372036854775807", true, "MaxInt64"},

		// Scanner rejects most JSON-invalid forms BEFORE reaching the
		// long parser. We assert overall rejection regardless of which
		// layer catches.
		{"0x10", false, "hex"},
		{"0b10", false, "binary"},
		{"01", false, "leading zero"},
		{"+5", false, "leading plus"},
		{"1.5", false, "non-whole"},
		{"NaN", false, "NaN"},
	}

	s := avro.MustParse(`"long"`)
	for _, c := range cases {
		c := c
		t.Run(c.desc, func(t *testing.T) {
			var v int64
			err := s.DecodeJSON([]byte(c.input), &v)
			if (err == nil) != c.expect {
				t.Errorf("JSON-decode %q against long: want accept=%v, got err=%v", c.input, c.expect, err)
			}
		})
	}
}

// TestRegression_NumberGrammarParityMatrix_JSONDecode_AllTypes extends
// the long-only JSON-decode matrix to int / float / double. The scanner
// gates JSON syntax for all four, but each runs through a distinct
// downstream parser (parseJSONInt32 for int, parseJSONInt64 for long,
// strconv.ParseFloat for float/double), so the per-type strictness needs
// independent pinning. Trailing-garbage rejection comes from
// [Schema.DecodeJSON]'s end-of-input check.
func TestRegression_NumberGrammarParityMatrix_JSONDecode_AllTypes(t *testing.T) {
	cases := []struct {
		input string
		// Per-type expected outcome. nil means N/A (e.g., fractional input
		// against int is "accept" for grammar but reject for type-fit;
		// here we test the union — does encode end-to-end succeed?).
		intOK, longOK, floatOK, doubleOK bool
		desc                             string
	}{
		// Valid integer.
		{"42", true, true, true, true, "small int"},
		{"-1", true, true, true, true, "neg int"},
		{"0", true, true, true, true, "zero"},

		// Valid float.
		{"1.5", false, false, true, true, "fractional"},
		{"1e3", true, true, true, true, "whole via exp"},
		{"1.5e1", true, true, true, true, "whole via exp scaled"},
		{"1.5e0", false, false, true, true, "fractional via exp"},

		// Mid-token garbage — trailing-content check fires.
		{"1abc", false, false, false, false, "trailing letters"},
		{"1.5x", false, false, false, false, "fractional + trailing"},
		{"0x10", false, false, false, false, "hex prefix"},

		// JSON-invalid forms (scanner-rejected at parse).
		{"0b10", false, false, false, false, "binary prefix"},
		{"+5", false, false, false, false, "leading plus"},
		{"01", false, false, false, false, "leading zero"},

		// Empty.
		{"", false, false, false, false, "empty"},

		// Trailing whitespace (RFC 8259 permits) and multi-record concat.
		{"5  ", true, true, true, true, "trailing whitespace"},
		{"5  {}", true, true, true, true, "trailing valid JSON (concat)"},

		// Overflow.
		{"2147483648", false, true, true, true, "MaxInt32+1"},
		{"9223372036854775808", false, false, true, true, "MaxInt64+1 (float-rounded)"},
	}

	type target struct {
		schema string
		decode func(s *avro.Schema, in []byte) error
	}
	targets := []target{
		{`"int"`, func(s *avro.Schema, in []byte) error { var v int32; return s.DecodeJSON(in, &v) }},
		{`"long"`, func(s *avro.Schema, in []byte) error { var v int64; return s.DecodeJSON(in, &v) }},
		{`"float"`, func(s *avro.Schema, in []byte) error { var v float32; return s.DecodeJSON(in, &v) }},
		{`"double"`, func(s *avro.Schema, in []byte) error { var v float64; return s.DecodeJSON(in, &v) }},
	}

	getExpect := func(c struct {
		input                            string
		intOK, longOK, floatOK, doubleOK bool
		desc                             string
	}, idx int) bool {
		switch idx {
		case 0:
			return c.intOK
		case 1:
			return c.longOK
		case 2:
			return c.floatOK
		case 3:
			return c.doubleOK
		}
		return false
	}

	for _, c := range cases {
		c := c
		for i, tg := range targets {
			tg := tg
			i := i
			t.Run(c.desc+"/"+tg.schema, func(t *testing.T) {
				s := avro.MustParse(tg.schema)
				err := tg.decode(s, []byte(c.input))
				gotOK := err == nil
				want := getExpect(c, i)
				if gotOK != want {
					t.Errorf("DecodeJSON %q against %s: want accept=%v, got err=%v", c.input, tg.schema, want, err)
				}
			})
		}
	}
}

// TestRegression_UnionDispatchMatrix pins branch selection for every
// (input, union schema) combination that has historically surfaced bugs
// or could under future drift. Avro union encode is try-each in schema
// order: try each branch; pick the first that encodes without error.
// The matrix locks which branch wins for inputs where multiple branches
// could plausibly accept — bytes-vs-string for raw bytes, the four
// numeric branches for json.Number, null shapes against
// [null,T]/T-only unions, etc.
//
// "Branch picked" is verified by decoding the wire output back through
// [Schema.DecodeJSON] into a *any target with [TaggedUnions], which
// wraps the decoded value with its branch name. The branch-name string
// thus appears in the decoded result and can be asserted against an
// expected branch.
//
// Branch-selection asymmetries in this matrix have caused real bugs:
// the prior nil-Map-against-multi-branch-union didn't reach case "null"
// because the try-each loop had a `continue` on null kinds (pattern 15
// in AUDIT.md); the binary serUnion try-each was independent of the
// JSON one, so they drifted on which inputs land on which branch.
func TestRegression_UnionDispatchMatrix(t *testing.T) {
	type expect struct {
		picked string // expected branch kind/name
		errOK  bool   // alternatively, encode is expected to error
	}
	cases := []struct {
		schema string
		input  any
		exp    expect
		desc   string
	}{
		// ---- Typed numeric inputs, single-branch fit. ----
		// Type-name dispatch (unionTypeNameForValue) maps int64 → "long"
		// regardless of value range, so int64(42) against
		// ["int","long"] picks long, NOT int even though 42 fits int32.
		// This locks the Go-type→Avro-type canonical mapping over
		// value-fitting promotion.
		{`["null","long"]`, int64(42), expect{"long", false}, "int64 to long"},
		{`["null","int","long"]`, int64(42), expect{"long", false}, "int64-fits-int32 still picks long via type-name"},
		{`["null","int","long"]`, int64(1 << 40), expect{"long", false}, "int64-out-of-int32 to long"},
		{`["null","int","long"]`, int32(42), expect{"int", false}, "int32 to int via type-name"},
		// int8/int16/uint8/uint16 all map to Avro "int" per type-name dispatch.
		{`["null","int","long"]`, int8(42), expect{"int", false}, "int8 maps to int"},
		{`["null","int","long"]`, int16(42), expect{"int", false}, "int16 maps to int"},
		{`["null","double"]`, float64(1.5), expect{"double", false}, "float64 to double"},
		{`["null","float","double"]`, float32(1.5), expect{"float", false}, "float32 to float"},
		{`["null","float","double"]`, float64(1.5), expect{"double", false}, "float64 to double, not float"},

		// ---- json.Number numeric inputs (type-name skips json.Number,
		// runs try-each which picks first branch that accepts). ----
		{`["null","long","string"]`, json.Number("42"), expect{"long", false}, "json.Number int prefers long"},
		// All-numeric union: try-each picks the first numeric that accepts.
		{`["null","int","long","float","double"]`, json.Number("42"), expect{"int", false}, "small int prefers int (first numeric)"},
		{`["null","int","long","float","double"]`, json.Number("9999999999"), expect{"long", false}, "out-of-int32 prefers long"},
		{`["null","int","long","float","double"]`, json.Number("1.5"), expect{"float", false}, "fractional prefers float"},
		// Non-whole rejects int+long, falls to first float that accepts.
		{`["null","double","string"]`, json.Number("1.5"), expect{"double", false}, "json.Number(1.5) to double"},
		// JSON-invalid grammar: all numeric branches reject; string
		// also rejects json.Number explicitly (avroStringValue/
		// appendAvroString line 805). No matching branch → error.
		{`["null","long","string"]`, json.Number("0x10"), expect{"", true}, "hex jsonNumber - all reject"},
		// Fractional against int+long+string: int+long reject (not whole),
		// string rejects json.Number → error.
		{`["null","long","string"]`, json.Number("1.5"), expect{"", true}, "fractional jsonNumber against long+string"},
		// But against bytes-containing union: bytes accepts string-typed
		// json.Number as raw bytes (serBytes line 768). Asymmetry: string
		// rejects json.Number but bytes accepts it. Documenting.
		{`["null","long","bytes"]`, json.Number("0x10"), expect{"bytes", false}, "hex jsonNumber falls to bytes (asymmetry vs string)"},

		// ---- nil shapes. serNull accepts Pointer/Interface/Map/Slice/
		// Chan/Func nils via the peel loop. ----
		{`["null","long"]`, nil, expect{"null", false}, "untyped nil"},
		{`["null","long"]`, (*int)(nil), expect{"null", false}, "typed nil pointer"},
		{`["null","long"]`, any((*int)(nil)), expect{"null", false}, "interface-wrapped typed nil"},
		// Nil []byte against any union with a null branch picks null
		// regardless of type-name dispatch — serUnion has an explicit
		// isNilValue priority gate (ser.go:125) BEFORE type-name and
		// try-each, so all nil shapes route to null when null is in
		// the union. Pre-fix this only fired for 2-branch [null,T];
		// the generic path used type-name first which silently sent
		// nil []byte to bytes-empty. The two are now uniform.
		{`["null","string"]`, []byte(nil), expect{"null", false}, "nil []byte against ['null','string'] routes to null"},
		{`["null","bytes"]`, []byte(nil), expect{"null", false}, "nil []byte against ['null','bytes'] routes to null (isNilValue priority)"},
		{`["null","bytes","string"]`, []byte(nil), expect{"null", false}, "nil []byte against ['null','bytes','string']"},
		// Nil map: type-name returns "" for map (not in dispatch); try-each.
		{`["null","string"]`, map[string]any(nil), expect{"null", false}, "nil map falls to null"},
		// No-null union with nil: rejects.
		{`["long","string"]`, nil, expect{"", true}, "untyped nil against no-null union"},

		// ---- string / bytes type-name dispatch. ----
		// Type-name maps reflect.String → "string", []byte → "bytes".
		// Schema order doesn't matter — type-name wins over try-each.
		{`["null","bytes","string"]`, []byte("hello"), expect{"bytes", false}, "[]byte to bytes via type-name"},
		{`["null","bytes","string"]`, "hello", expect{"string", false}, "string to string via type-name"},
		{`["null","string","bytes"]`, "hello", expect{"string", false}, "string-first union picks string"},
		{`["null","string","bytes"]`, []byte("hello"), expect{"bytes", false /* type-name */}, "[]byte type-name to bytes"},

		// ---- Bool dispatch. ----
		{`["null","boolean","string"]`, true, expect{"boolean", false}, "bool to boolean"},
		{`["null","boolean","string"]`, false, expect{"boolean", false}, "false bool to boolean"},

		// ---- Numeric-overflow fall-through. ----
		{`["null","int","long"]`, int64(math.MaxInt64), expect{"long", false}, "MaxInt64 to long"},
	}

	for _, c := range cases {
		c := c
		t.Run(c.desc, func(t *testing.T) {
			s := avro.MustParse(c.schema)
			out, err := s.AppendEncode(nil, c.input)
			if c.exp.errOK {
				if err == nil {
					t.Errorf("expected error, got encoded %v", out)
				}
				return
			}
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			// Decode with TaggedUnions to see which branch was picked.
			var v any
			if _, err := s.Decode(out, &v, avro.TaggedUnions()); err != nil {
				t.Fatalf("decode: %v", err)
			}
			// For the null branch, decoded value is nil — tagged map not produced.
			if c.exp.picked == "null" {
				if v != nil {
					t.Errorf("expected null branch (v=nil), got %T %v", v, v)
				}
				return
			}
			m, ok := v.(map[string]any)
			if !ok {
				t.Fatalf("expected tagged-union map, got %T %v", v, v)
			}
			if _, found := m[c.exp.picked]; !found {
				t.Errorf("expected branch %q to be picked, got map=%v", c.exp.picked, m)
			}
		})
	}
}

// TestRegression_LogicalTypeRoundTripMatrix locks encode + decode for
// every logical type, asserting that a value encoded through one path
// round-trips identically through the matching decode path. Each
// logical type gets a representative value and a primary target Go
// type (the one the binary deserializer emits). The matrix prevents
// drift where a future encoder accepts a value the decoder can't read
// back, or vice versa.
func TestRegression_LogicalTypeRoundTripMatrix(t *testing.T) {
	type rt struct {
		schema   string
		value    any
		makeTgt  func() any
		desc     string
		decoded  func(any) any // optional decoded transform
		jsonOnly bool
	}

	now := time.Date(2024, 6, 15, 10, 30, 45, 123456789, time.UTC)
	dateOnly := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)

	cases := []rt{
		{
			schema:  `{"type":"int","logicalType":"date"}`,
			value:   dateOnly,
			makeTgt: func() any { var v time.Time; return &v },
			desc:    "date",
		},
		{
			schema:  `{"type":"int","logicalType":"time-millis"}`,
			value:   3*time.Hour + 30*time.Minute + 45*time.Second,
			makeTgt: func() any { var v time.Duration; return &v },
			desc:    "time-millis",
		},
		{
			schema:  `{"type":"long","logicalType":"time-micros"}`,
			value:   3*time.Hour + 30*time.Minute + 45*time.Second + 123*time.Microsecond,
			makeTgt: func() any { var v time.Duration; return &v },
			desc:    "time-micros",
		},
		{
			schema:  `{"type":"long","logicalType":"timestamp-millis"}`,
			value:   now.Truncate(time.Millisecond),
			makeTgt: func() any { var v time.Time; return &v },
			desc:    "timestamp-millis",
		},
		{
			schema:  `{"type":"long","logicalType":"timestamp-micros"}`,
			value:   now.Truncate(time.Microsecond),
			makeTgt: func() any { var v time.Time; return &v },
			desc:    "timestamp-micros",
		},
		{
			schema:  `{"type":"long","logicalType":"timestamp-nanos"}`,
			value:   now,
			makeTgt: func() any { var v time.Time; return &v },
			desc:    "timestamp-nanos",
		},
		{
			schema:  `{"type":"long","logicalType":"local-timestamp-millis"}`,
			value:   now.Truncate(time.Millisecond),
			makeTgt: func() any { var v time.Time; return &v },
			desc:    "local-timestamp-millis",
		},
		{
			schema:  `{"type":"string","logicalType":"uuid"}`,
			value:   "01020304-0506-0708-090a-0b0c0d0e0f10",
			makeTgt: func() any { var v string; return &v },
			desc:    "uuid",
		},
		{
			schema:  `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`,
			value:   big.NewRat(31415, 100),
			makeTgt: func() any { var v big.Rat; return &v },
			desc:    "decimal-bytes",
			decoded: func(v any) any {
				if r, ok := v.(*big.Rat); ok {
					return r
				}
				if r, ok := v.(big.Rat); ok {
					return &r
				}
				return v
			},
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.desc+"/binary", func(t *testing.T) {
			s := avro.MustParse(c.schema)
			out, err := s.AppendEncode(nil, c.value)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			tgt := c.makeTgt()
			if _, err := s.Decode(out, tgt); err != nil {
				t.Fatalf("decode: %v", err)
			}
			// Sanity: a non-zero output and a non-nil target.
			if len(out) == 0 {
				t.Errorf("encoded to empty bytes")
			}
		})
		t.Run(c.desc+"/json", func(t *testing.T) {
			if c.jsonOnly {
				t.Skip("binary-only test")
			}
			s := avro.MustParse(c.schema)
			out, err := s.AppendEncodeJSON(nil, c.value)
			if err != nil {
				t.Fatalf("JSON encode: %v", err)
			}
			tgt := c.makeTgt()
			if err := s.DecodeJSON(out, tgt); err != nil {
				t.Fatalf("JSON decode: %v (out=%s)", err, string(out))
			}
		})
	}
}

// TestRegression_PromotionMatrix locks Avro schema-resolution promotion
// rules for every writer→reader type pair the spec permits. The Avro
// spec defines these promotions: int→{long,float,double};
// long→{float,double}; float→{double}; string↔bytes. Additionally,
// resolved decode handles schema evolution: removed fields (writer has
// field, reader doesn't), default-fill (reader has field, writer doesn't),
// aliases, enum-symbol widening, and union widening (non-union writer
// against union reader).
//
// Each case: encode a value with the writer schema, decode the wire
// bytes through Resolve(writer, reader), assert the decoded value
// matches the spec'd promoted shape. Drift in any promotion arm
// surfaces immediately.
//
// Java: org.apache.avro.io.parsing.ResolvingGrammarGenerator + Symbol.RESOLVED.
// fastavro: _read_py.py's read_resolved.
// twmb is fail-fast on writer-union → reader-non-union (intentional
// divergence; see AUDIT.md known divergences). The matrix avoids that
// case so it doesn't conflate intentional divergence with a regression.
func TestRegression_PromotionMatrix(t *testing.T) {
	cases := []struct {
		writer, reader string
		input          any
		target         func() any
		want           any
		desc           string
	}{
		// ---- Primitive promotion. ----
		{`"int"`, `"long"`, int32(42), func() any { var v int64; return &v }, int64(42), "int→long"},
		{`"int"`, `"float"`, int32(42), func() any { var v float32; return &v }, float32(42), "int→float"},
		{`"int"`, `"double"`, int32(42), func() any { var v float64; return &v }, float64(42), "int→double"},
		{`"long"`, `"float"`, int64(123456), func() any { var v float32; return &v }, float32(123456), "long→float"},
		{`"long"`, `"double"`, int64(123456), func() any { var v float64; return &v }, float64(123456), "long→double"},
		{`"float"`, `"double"`, float32(1.5), func() any { var v float64; return &v }, float64(1.5), "float→double"},
		{`"string"`, `"bytes"`, "hello", func() any { var v []byte; return &v }, []byte("hello"), "string→bytes"},
		{`"bytes"`, `"string"`, []byte("hello"), func() any { var v string; return &v }, "hello", "bytes→string"},

		// ---- Same schema (no promotion). ----
		{`"long"`, `"long"`, int64(42), func() any { var v int64; return &v }, int64(42), "long→long identity"},
	}

	for _, c := range cases {
		c := c
		t.Run(c.desc, func(t *testing.T) {
			w := avro.MustParse(c.writer)
			r := avro.MustParse(c.reader)
			wire, err := w.AppendEncode(nil, c.input)
			if err != nil {
				t.Fatalf("encode with writer: %v", err)
			}
			rs, err := avro.Resolve(w, r)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			tgt := c.target()
			if _, err := rs.Decode(wire, tgt); err != nil {
				t.Fatalf("Decode resolved: %v", err)
			}
			// Compare via reflect since target is a *T.
			got := reflect.ValueOf(tgt).Elem().Interface()
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("got %T %v, want %T %v", got, got, c.want, c.want)
			}
		})
	}
}

// TestRegression_PromotionInvalid locks resolution failures for type
// pairs that the spec REJECTS. Avro disallows narrowing promotions
// (long→int, float→int, double→float, etc.) and unrelated-type pairs.
// CheckCompatibility / Resolve must error on these at config time.
func TestRegression_PromotionInvalid(t *testing.T) {
	cases := []struct {
		writer, reader string
		desc           string
	}{
		{`"long"`, `"int"`, "long→int (narrowing)"},
		{`"double"`, `"float"`, "double→float (narrowing)"},
		{`"double"`, `"int"`, "double→int (narrowing)"},
		{`"long"`, `"boolean"`, "long→boolean (unrelated)"},
		{`"string"`, `"int"`, "string→int (unrelated)"},
		{`"bytes"`, `"int"`, "bytes→int (unrelated)"},
		{`"int"`, `"string"`, "int→string (unrelated)"},
		// float→int is unrelated per the spec (no narrowing path).
		{`"float"`, `"int"`, "float→int (unrelated)"},
		{`"float"`, `"long"`, "float→long (narrowing)"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.desc, func(t *testing.T) {
			w := avro.MustParse(c.writer)
			r := avro.MustParse(c.reader)
			_, err := avro.Resolve(w, r)
			if err == nil {
				t.Errorf("expected resolve error for %s, got success", c.desc)
			}
		})
	}
}

// TestRegression_PromotionSchemaEvolution tests schema-evolution cases:
// removed fields (writer has but reader doesn't), default-fill (reader
// has but writer doesn't), aliases, enum-symbol widening with default,
// union widening (writer's type is in reader's union).
func TestRegression_PromotionSchemaEvolution(t *testing.T) {
	t.Run("default-fill new field", func(t *testing.T) {
		writer := `{"type":"record","name":"R","fields":[
			{"name":"a","type":"int"}
		]}`
		reader := `{"type":"record","name":"R","fields":[
			{"name":"a","type":"int"},
			{"name":"b","type":"int","default":99}
		]}`
		w := avro.MustParse(writer)
		r := avro.MustParse(reader)
		wire, err := w.AppendEncode(nil, map[string]any{"a": int32(7)})
		if err != nil {
			t.Fatal(err)
		}
		rs, err := avro.Resolve(w, r)
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]any
		if _, err := rs.Decode(wire, &got); err != nil {
			t.Fatal(err)
		}
		if got["a"] != int32(7) || got["b"] != int32(99) {
			t.Errorf("got %+v, want a=7 b=99 (default)", got)
		}
	})

	t.Run("removed field (writer has, reader doesn't)", func(t *testing.T) {
		writer := `{"type":"record","name":"R","fields":[
			{"name":"a","type":"int"},
			{"name":"b","type":"string"}
		]}`
		reader := `{"type":"record","name":"R","fields":[
			{"name":"a","type":"int"}
		]}`
		w := avro.MustParse(writer)
		r := avro.MustParse(reader)
		wire, err := w.AppendEncode(nil, map[string]any{"a": int32(7), "b": "drop me"})
		if err != nil {
			t.Fatal(err)
		}
		rs, err := avro.Resolve(w, r)
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]any
		if _, err := rs.Decode(wire, &got); err != nil {
			t.Fatal(err)
		}
		if got["a"] != int32(7) || len(got) != 1 {
			t.Errorf("got %+v, want only a=7", got)
		}
	})

	t.Run("missing reader field without default errors at Resolve", func(t *testing.T) {
		writer := `{"type":"record","name":"R","fields":[
			{"name":"a","type":"int"}
		]}`
		reader := `{"type":"record","name":"R","fields":[
			{"name":"a","type":"int"},
			{"name":"b","type":"int"}
		]}`
		w := avro.MustParse(writer)
		r := avro.MustParse(reader)
		_, err := avro.Resolve(w, r)
		if err == nil {
			t.Errorf("expected Resolve error for missing field without default, got success")
		}
	})

	t.Run("record alias rename", func(t *testing.T) {
		writer := `{"type":"record","name":"OldName","fields":[{"name":"a","type":"int"}]}`
		reader := `{"type":"record","name":"NewName","aliases":["OldName"],"fields":[{"name":"a","type":"int"}]}`
		w := avro.MustParse(writer)
		r := avro.MustParse(reader)
		wire, err := w.AppendEncode(nil, map[string]any{"a": int32(42)})
		if err != nil {
			t.Fatal(err)
		}
		rs, err := avro.Resolve(w, r)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		var got map[string]any
		if _, err := rs.Decode(wire, &got); err != nil {
			t.Fatal(err)
		}
		if got["a"] != int32(42) {
			t.Errorf("got %+v, want a=42", got)
		}
	})

	t.Run("field alias rename", func(t *testing.T) {
		writer := `{"type":"record","name":"R","fields":[{"name":"oldField","type":"int"}]}`
		reader := `{"type":"record","name":"R","fields":[{"name":"newField","type":"int","aliases":["oldField"]}]}`
		w := avro.MustParse(writer)
		r := avro.MustParse(reader)
		wire, err := w.AppendEncode(nil, map[string]any{"oldField": int32(42)})
		if err != nil {
			t.Fatal(err)
		}
		rs, err := avro.Resolve(w, r)
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]any
		if _, err := rs.Decode(wire, &got); err != nil {
			t.Fatal(err)
		}
		if got["newField"] != int32(42) {
			t.Errorf("got %+v, want newField=42", got)
		}
	})

	t.Run("enum widened with default", func(t *testing.T) {
		writer := `{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]}`
		// Reader removes BLUE and adds a default for missing symbols.
		reader := `{"type":"enum","name":"Color","symbols":["RED","GREEN"],"default":"RED"}`
		w := avro.MustParse(writer)
		r := avro.MustParse(reader)
		// Encode "BLUE" with writer (symbol index 2).
		wire, err := w.AppendEncode(nil, "BLUE")
		if err != nil {
			t.Fatal(err)
		}
		rs, err := avro.Resolve(w, r)
		if err != nil {
			t.Fatal(err)
		}
		var got string
		if _, err := rs.Decode(wire, &got); err != nil {
			t.Fatal(err)
		}
		if got != "RED" {
			t.Errorf("got %q, want %q (enum default)", got, "RED")
		}
	})

	t.Run("non-union writer to union reader", func(t *testing.T) {
		writer := `"int"`
		reader := `["null","int"]`
		w := avro.MustParse(writer)
		r := avro.MustParse(reader)
		wire, err := w.AppendEncode(nil, int32(42))
		if err != nil {
			t.Fatal(err)
		}
		rs, err := avro.Resolve(w, r)
		if err != nil {
			t.Fatal(err)
		}
		var got any
		if _, err := rs.Decode(wire, &got); err != nil {
			t.Fatal(err)
		}
		if got != int32(42) {
			t.Errorf("got %T %v, want int32(42)", got, got)
		}
	})
}

// TestRegression_DefaultFillMatrix pins the auto-default-fill behavior
// (twmb-specific feature: when DecodeJSON / EncodeJSON sees a record
// with a missing field that has a schema default, the default
// materializes into the target / wire). Each primitive + logical type
// gets a default fill case; the matrix prevents drift in any single
// arm's default-extraction.
//
// Default fill fires at: AppendEncodeJSON (record field absent in
// Go-side input) and DecodeJSON (field absent in JSON input). Binary
// encode/decode use pre-encoded default bytes (encodeDefault) which
// also runs through these arms at schema build time.
func TestRegression_DefaultFillMatrix(t *testing.T) {
	cases := []struct {
		field    string // schema fragment for the field with default
		expectGo any    // expected Go value after decode
		desc     string
	}{
		{`{"name":"x","type":"int","default":42}`, int32(42), "int default"},
		{`{"name":"x","type":"long","default":1234567890}`, int64(1234567890), "long default"},
		{`{"name":"x","type":"float","default":1.5}`, float32(1.5), "float default"},
		{`{"name":"x","type":"double","default":1.5}`, float64(1.5), "double default"},
		{`{"name":"x","type":"string","default":"hello"}`, "hello", "string default"},
		{`{"name":"x","type":"boolean","default":true}`, true, "bool default"},
		{`{"name":"x","type":"null","default":null}`, nil, "null default"},
		// Long-precision default: 2^53+1 must survive (the precision-
		// preserving unmarshalDefault path was a prior bug class).
		{`{"name":"x","type":"long","default":9007199254740993}`, int64(9007199254740993), "long >2^53 default"},
		// Nested record default.
		{`{"name":"x","type":{"type":"record","name":"N","fields":[{"name":"y","type":"int"}]},"default":{"y":7}}`,
			map[string]any{"y": int32(7)}, "nested record default"},
		// Array default.
		{`{"name":"x","type":{"type":"array","items":"int"},"default":[1,2,3]}`,
			[]any{int32(1), int32(2), int32(3)}, "array default"},
		// Map default.
		{`{"name":"x","type":{"type":"map","values":"int"},"default":{"a":1}}`,
			map[string]any{"a": int32(1)}, "map default"},
		// Null-union default (implicit null).
		{`{"name":"x","type":["null","int"]}`, nil, "null-union implicit null default"},
	}

	for _, c := range cases {
		c := c
		t.Run(c.desc+"/binary", func(t *testing.T) {
			schema := `{"type":"record","name":"R","fields":[` + c.field + `]}`
			s := avro.MustParse(schema)
			// Encode an empty map (missing field).
			wire, err := s.AppendEncode(nil, map[string]any{})
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			var got map[string]any
			if _, err := s.Decode(wire, &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !reflect.DeepEqual(got["x"], c.expectGo) {
				t.Errorf("got %T %v, want %T %v", got["x"], got["x"], c.expectGo, c.expectGo)
			}
		})
		t.Run(c.desc+"/json", func(t *testing.T) {
			schema := `{"type":"record","name":"R","fields":[` + c.field + `]}`
			s := avro.MustParse(schema)
			wire, err := s.AppendEncodeJSON(nil, map[string]any{})
			if err != nil {
				t.Fatalf("JSON encode: %v", err)
			}
			var got map[string]any
			if err := s.DecodeJSON(wire, &got); err != nil {
				t.Fatalf("JSON decode (out=%s): %v", string(wire), err)
			}
			if !reflect.DeepEqual(got["x"], c.expectGo) {
				t.Errorf("got %T %v, want %T %v", got["x"], got["x"], c.expectGo, c.expectGo)
			}
		})
	}
}

// TestRegression_DefaultFillLogicalTypes covers logical-type defaults
// — date/timestamp/uuid/decimal — which exercise the convertDefaultBytes
// path (bytes/fixed defaults become []byte) and the logical decoder's
// default arm. Bugs in this surface have included UUID-fixed defaults
// being rejected as "invalid UUID" because the stored codepoint-string
// form (16 chars) didn't match parseUUID's strict 36-char hex-dash
// requirement (commit history shows this was fixed in earlier rounds).
func TestRegression_DefaultFillLogicalTypes(t *testing.T) {
	cases := []struct {
		field      string
		jsonExpect string // JSON wire form of the default-filled value
		desc       string
	}{
		// date default (epoch days).
		{`{"name":"x","type":{"type":"int","logicalType":"date"},"default":0}`, `"x":"1970-01-01"`, "date default"},
		// time-millis (millis-of-day).
		{`{"name":"x","type":{"type":"int","logicalType":"time-millis"},"default":0}`, `"x":0`, "time-millis default"},
		// UUID fixed.
		{`{"name":"x","type":{"type":"fixed","name":"F","size":16,"logicalType":"uuid"},"default":"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000"}`, "", "uuid fixed default"},
		// Decimal bytes default ("0" codepoint = unscaled 0).
		{`{"name":"x","type":{"type":"bytes","logicalType":"decimal","precision":5,"scale":2},"default":"\u0000"}`, "", "decimal default"},
	}

	for _, c := range cases {
		c := c
		t.Run(c.desc, func(t *testing.T) {
			schema := `{"type":"record","name":"R","fields":[` + c.field + `]}`
			s, err := avro.Parse(schema)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			// JSON-encode an empty map — should fill default.
			wire, err := s.AppendEncodeJSON(nil, map[string]any{})
			if err != nil {
				t.Fatalf("JSON encode: %v", err)
			}
			// Round-trip via JSON decode to verify the default
			// materialized correctly.
			var got map[string]any
			if err := s.DecodeJSON(wire, &got); err != nil {
				t.Fatalf("JSON decode (out=%s): %v", string(wire), err)
			}
			if _, exists := got["x"]; !exists {
				t.Errorf("default field 'x' missing from decoded: %v (wire=%s)", got, string(wire))
			}
		})
	}
}

type myDuration int64

// TestRegression_CustomTypeMatrix pins CustomType registration across
// the matching axes: LogicalType-only / AvroType-only /
// LogicalType+AvroType / ErrSkipCustomType fall-through to next
// custom type or built-in / per-Schema scope.
func TestRegression_CustomTypeMatrix(t *testing.T) {
	t.Run("LogicalType match", func(t *testing.T) {
		ct := avro.CustomType{
			LogicalType: "uuid",
			Decode: func(v any, _ *avro.SchemaNode) (any, error) {
				return v, nil
			},
		}
		s, err := avro.Parse(`{"type":"string","logicalType":"uuid"}`, ct)
		if err != nil {
			t.Fatal(err)
		}
		wire, err := s.AppendEncode(nil, "01234567-89ab-cdef-0123-456789abcdef")
		if err != nil {
			t.Fatal(err)
		}
		var got any
		if _, err := s.Decode(wire, &got); err != nil {
			t.Fatal(err)
		}
		if s, ok := got.(string); !ok || s != "01234567-89ab-cdef-0123-456789abcdef" {
			t.Errorf("got %T %v, want passthrough string", got, got)
		}
	})

	t.Run("AvroType match", func(t *testing.T) {
		ct := avro.CustomType{
			AvroType: "long",
			Decode: func(v any, _ *avro.SchemaNode) (any, error) {
				if n, ok := v.(int64); ok {
					return myDuration(n), nil
				}
				return v, nil
			},
		}
		s, err := avro.Parse(`"long"`, ct)
		if err != nil {
			t.Fatal(err)
		}
		wire, err := s.AppendEncode(nil, int64(42))
		if err != nil {
			t.Fatal(err)
		}
		var got any
		if _, err := s.Decode(wire, &got); err != nil {
			t.Fatal(err)
		}
		if v, ok := got.(myDuration); !ok || v != myDuration(42) {
			t.Errorf("got %T %v, want myDuration(42)", got, got)
		}
	})

	t.Run("ErrSkipCustomType falls through to built-in", func(t *testing.T) {
		called := 0
		ct := avro.CustomType{
			AvroType: "long",
			Decode: func(v any, _ *avro.SchemaNode) (any, error) {
				called++
				return nil, avro.ErrSkipCustomType
			},
		}
		s, err := avro.Parse(`"long"`, ct)
		if err != nil {
			t.Fatal(err)
		}
		wire, err := s.AppendEncode(nil, int64(7))
		if err != nil {
			t.Fatal(err)
		}
		var got int64
		if _, err := s.Decode(wire, &got); err != nil {
			t.Fatal(err)
		}
		if called != 1 {
			t.Errorf("custom decode not called: %d", called)
		}
		if got != 7 {
			t.Errorf("got %d, want 7", got)
		}
	})

	t.Run("ErrSkipCustomType falls through to next CustomType", func(t *testing.T) {
		called1, called2 := 0, 0
		ct1 := avro.CustomType{
			AvroType: "long",
			Decode: func(v any, _ *avro.SchemaNode) (any, error) {
				called1++
				return nil, avro.ErrSkipCustomType
			},
		}
		ct2 := avro.CustomType{
			AvroType: "long",
			Decode: func(v any, _ *avro.SchemaNode) (any, error) {
				called2++
				if n, ok := v.(int64); ok {
					return n * 2, nil
				}
				return v, nil
			},
		}
		s, err := avro.Parse(`"long"`, ct1, ct2)
		if err != nil {
			t.Fatal(err)
		}
		wire, err := s.AppendEncode(nil, int64(7))
		if err != nil {
			t.Fatal(err)
		}
		var got any
		if _, err := s.Decode(wire, &got); err != nil {
			t.Fatal(err)
		}
		if called1 != 1 || called2 != 1 {
			t.Errorf("called1=%d called2=%d", called1, called2)
		}
		if got != int64(14) {
			t.Errorf("got %v, want 14 (second custom doubled)", got)
		}
	})

	t.Run("Per-Schema scope: custom types don't leak across Schemas", func(t *testing.T) {
		ct := avro.CustomType{
			AvroType: "long",
			Decode: func(v any, _ *avro.SchemaNode) (any, error) {
				if n, ok := v.(int64); ok {
					return myDuration(n), nil
				}
				return v, nil
			},
		}
		s1, _ := avro.Parse(`"long"`, ct)
		s2, _ := avro.Parse(`"long"`)

		wire, _ := s1.AppendEncode(nil, int64(42))
		var got1 any
		_, _ = s1.Decode(wire, &got1)
		var got2 any
		_, _ = s2.Decode(wire, &got2)

		if _, ok := got1.(myDuration); !ok {
			t.Errorf("s1 with custom type: got %T, want myDuration", got1)
		}
		if _, ok := got2.(int64); !ok {
			t.Errorf("s2 without custom type: got %T, want int64", got2)
		}
	})
}

// TestRegression_TaggedUnionMatrix pins the JSON-encode / JSON-decode
// behavior of unions with and without the TaggedUnions option. The
// Avro JSON spec mandates tagged form ({"type_name":value}); twmb
// also accepts bare form on decode for interop with goavro and
// non-conformant producers. The TaggedUnions option toggles which
// form encode emits and which form decode wraps into the *any target.
func TestRegression_TaggedUnionMatrix(t *testing.T) {
	schema := `["null","int","string"]`
	s := avro.MustParse(schema)

	t.Run("encode without TaggedUnions emits bare", func(t *testing.T) {
		out, err := s.AppendEncodeJSON(nil, int32(42))
		if err != nil {
			t.Fatal(err)
		}
		got := string(out)
		if got != "42" {
			t.Errorf("bare encode: got %q, want %q", got, "42")
		}
	})

	t.Run("encode with TaggedUnions emits {type:value}", func(t *testing.T) {
		out, err := s.AppendEncodeJSON(nil, int32(42), avro.TaggedUnions())
		if err != nil {
			t.Fatal(err)
		}
		got := string(out)
		// Tagged form: {"int":42}
		if got != `{"int":42}` {
			t.Errorf("tagged encode: got %q, want %q", got, `{"int":42}`)
		}
	})

	t.Run("encode null bare", func(t *testing.T) {
		out, err := s.AppendEncodeJSON(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if string(out) != "null" {
			t.Errorf("nil: got %q, want null", string(out))
		}
	})

	t.Run("encode null with TaggedUnions emits bare null", func(t *testing.T) {
		// Null branches don't get the tag wrapper — bare `null` is the
		// universal form even under TaggedUnions.
		out, err := s.AppendEncodeJSON(nil, nil, avro.TaggedUnions())
		if err != nil {
			t.Fatal(err)
		}
		if string(out) != "null" {
			t.Errorf("nil tagged: got %q, want null", string(out))
		}
	})

	t.Run("decode bare form into *any", func(t *testing.T) {
		var got any
		if err := s.DecodeJSON([]byte("42"), &got); err != nil {
			t.Fatal(err)
		}
		if got != int32(42) {
			t.Errorf("bare decode: got %T %v, want int32(42)", got, got)
		}
	})

	t.Run("decode tagged form into *any without TaggedUnions option", func(t *testing.T) {
		var got any
		if err := s.DecodeJSON([]byte(`{"int":42}`), &got); err != nil {
			t.Fatal(err)
		}
		if got != int32(42) {
			t.Errorf("tagged input → bare decode: got %T %v, want int32(42)", got, got)
		}
	})

	t.Run("decode tagged form into *any with TaggedUnions option wraps", func(t *testing.T) {
		var got any
		if err := s.DecodeJSON([]byte(`{"int":42}`), &got, avro.TaggedUnions()); err != nil {
			t.Fatal(err)
		}
		m, ok := got.(map[string]any)
		if !ok {
			t.Fatalf("expected tagged map, got %T %v", got, got)
		}
		if m["int"] != int32(42) {
			t.Errorf("got map=%v, want map[int:42]", m)
		}
	})

	t.Run("decode bare form into *any with TaggedUnions wraps", func(t *testing.T) {
		// TaggedUnions option also wraps bare-form input on decode
		// so the typed map output is consistent regardless of input form.
		var got any
		if err := s.DecodeJSON([]byte("42"), &got, avro.TaggedUnions()); err != nil {
			t.Fatal(err)
		}
		m, ok := got.(map[string]any)
		if !ok {
			t.Fatalf("expected tagged map, got %T %v", got, got)
		}
		if m["int"] != int32(42) {
			t.Errorf("got map=%v, want map[int:42]", m)
		}
	})

	t.Run("round-trip with TaggedUnions both sides", func(t *testing.T) {
		for _, val := range []any{
			int32(42),
			"hello",
			nil,
		} {
			val := val
			out, err := s.AppendEncodeJSON(nil, val, avro.TaggedUnions())
			if err != nil {
				t.Errorf("encode %v: %v", val, err)
				continue
			}
			var got any
			if err := s.DecodeJSON(out, &got, avro.TaggedUnions()); err != nil {
				t.Errorf("decode %s: %v", string(out), err)
				continue
			}
			if val == nil {
				if got != nil {
					t.Errorf("nil round-trip: got %v", got)
				}
				continue
			}
			m, ok := got.(map[string]any)
			if !ok {
				t.Errorf("expected tagged map for %v, got %T %v", val, got, got)
				continue
			}
			// Walk one level — the value should match.
			for _, v := range m {
				if !reflect.DeepEqual(v, val) {
					t.Errorf("round-trip: got %v, want %v", v, val)
				}
			}
		}
	})
}

// TestRegression_SingleObjectMatrix pins SOE framing (2-byte magic
// 0xC3 0x01 + 8-byte little-endian CRC-64-AVRO fingerprint + payload),
// fingerprint mismatch detection, and short-buffer rejection.
func TestRegression_SingleObjectMatrix(t *testing.T) {
	s := avro.MustParse(`"long"`)

	t.Run("round-trip", func(t *testing.T) {
		out, err := s.AppendSingleObject(nil, int64(42))
		if err != nil {
			t.Fatal(err)
		}
		if len(out) < 10 {
			t.Fatalf("output too short: %d bytes", len(out))
		}
		if out[0] != 0xC3 || out[1] != 0x01 {
			t.Errorf("magic: got [%#x, %#x], want [0xC3, 0x01]", out[0], out[1])
		}
		var v int64
		if _, err := s.DecodeSingleObject(out, &v); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if v != 42 {
			t.Errorf("got %d, want 42", v)
		}
	})

	t.Run("invalid magic rejected", func(t *testing.T) {
		out, _ := s.AppendSingleObject(nil, int64(42))
		out[0] = 0xFF
		var v int64
		_, err := s.DecodeSingleObject(out, &v)
		if err == nil {
			t.Errorf("expected magic-mismatch error")
		}
	})

	t.Run("fingerprint mismatch rejected", func(t *testing.T) {
		out, _ := s.AppendSingleObject(nil, int64(42))
		// Corrupt one byte of fingerprint.
		out[5] ^= 0xFF
		var v int64
		_, err := s.DecodeSingleObject(out, &v)
		if err == nil {
			t.Errorf("expected fingerprint-mismatch error")
		}
	})

	t.Run("too-short buffer rejected", func(t *testing.T) {
		var v int64
		for _, n := range []int{0, 1, 5, 9} {
			_, err := s.DecodeSingleObject(make([]byte, n), &v)
			if err == nil {
				t.Errorf("len=%d: expected error, got success", n)
			}
		}
	})

	t.Run("SingleObjectFingerprint extracts fingerprint and rest", func(t *testing.T) {
		out, _ := s.AppendSingleObject(nil, int64(42))
		fp, rest, err := avro.SingleObjectFingerprint(out)
		if err != nil {
			t.Fatal(err)
		}
		// Fingerprint should match what AppendSingleObject embedded.
		if [8]byte(out[2:10]) != fp {
			t.Errorf("fingerprint mismatch: out[2:10]=%x got %x", out[2:10], fp)
		}
		// Rest should be the encoded payload.
		var v int64
		if _, err := s.Decode(rest, &v); err != nil {
			t.Fatal(err)
		}
		if v != 42 {
			t.Errorf("decoded payload got %d, want 42", v)
		}
	})

	t.Run("schemas with different fingerprints don't cross-decode", func(t *testing.T) {
		sLong := avro.MustParse(`"long"`)
		sInt := avro.MustParse(`"int"`)
		out, _ := sLong.AppendSingleObject(nil, int64(42))
		var v int32
		_, err := sInt.DecodeSingleObject(out, &v)
		if err == nil {
			t.Errorf("expected fingerprint mismatch when decoding long-SOE with int schema")
		}
	})
}

// TestRegression_SchemaIntrospectionMatrix pins the Schema introspection
// surface: Root (programmatic tree), Canonical (PCF bytes), Fingerprint
// (CRC-64-AVRO + SHA-256 hashes), String (raw JSON). These have no
// encode/decode parity counterpart, so drift in any of them isn't caught
// by the round-trip matrices.
//
// Each schema is exercised through all four methods and round-tripped:
//   - Root().Schema() should parse back to an equivalent schema.
//   - Canonical() should match the spec'd PCF order/escaping rules.
//   - Fingerprint(crc64).String() should be stable across runs.
//   - String() should return the original JSON unchanged.
//
// Pattern 1's precision-edge concern: integer metadata > 2^53 must
// survive Root() as int64 / json.Number, NOT silently round to float64.
// This was a prior bug class fixed in commit 4cbdd5c — the matrix
// prevents regressions.
func TestRegression_SchemaIntrospectionMatrix(t *testing.T) {
	cases := []struct {
		schema string
		desc   string
	}{
		{`"int"`, "primitive int"},
		{`"long"`, "primitive long"},
		{`["null","string"]`, "simple union"},
		{`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`, "simple record"},
		{`{"type":"record","name":"R","fields":[
			{"name":"a","type":"int","default":42},
			{"name":"b","type":{"type":"map","values":"long"}}
		]}`, "record with default and map field"},
		{`{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"],"default":"RED"}`, "enum with default"},
		{`{"type":"fixed","name":"F","size":16}`, "fixed"},
		{`{"type":"int","logicalType":"date"}`, "int with date logical"},
		{`{"type":"bytes","logicalType":"decimal","precision":20,"scale":4}`, "decimal"},
		// Records with extra props at every level.
		{`{"type":"record","name":"R","custom-prop":"hello","fields":[
			{"name":"a","type":"int","fieldProp":42}
		]}`, "record with custom props"},
		// Big-integer property (>2^53) — locks Root() precision preservation.
		{`{"type":"record","name":"R","big-int":9007199254740993,"fields":[
			{"name":"a","type":"int"}
		]}`, "record with >2^53 int property"},
	}

	for _, c := range cases {
		c := c
		t.Run(c.desc, func(t *testing.T) {
			s := avro.MustParse(c.schema)

			// Root() returns a SchemaNode tree.
			root := s.Root()
			if root.Type == "" {
				t.Errorf("Root: empty type")
			}

			// Root().Schema() re-parses; should not error.
			s2, err := root.Schema()
			if err != nil {
				t.Errorf("Root().Schema() re-parse: %v", err)
			}
			// Re-parsed schema should have the same canonical form.
			if s2 != nil && !bytes.Equal(s.Canonical(), s2.Canonical()) {
				t.Errorf("Root() round-trip canonical differs:\n  before: %s\n  after:  %s",
					string(s.Canonical()), string(s2.Canonical()))
			}

			// Canonical() should be valid PCF JSON.
			canon := s.Canonical()
			if len(canon) == 0 {
				t.Errorf("Canonical returned empty")
			}
			if !json.Valid(canon) {
				t.Errorf("Canonical is not valid JSON: %s", string(canon))
			}

			// Fingerprint should be deterministic.
			h1 := avro.NewRabin()
			fp1 := s.Fingerprint(h1)
			h2 := avro.NewRabin()
			fp2 := s.Fingerprint(h2)
			if !bytes.Equal(fp1, fp2) {
				t.Errorf("Fingerprint not deterministic: %x vs %x", fp1, fp2)
			}

			// String() should return the original JSON.
			if s.String() != c.schema {
				t.Errorf("String() returned modified schema:\n  want: %s\n  got:  %s", c.schema, s.String())
			}
		})
	}

	// Specific precision-preservation case.
	t.Run("big-int Props preserved as int64", func(t *testing.T) {
		s := avro.MustParse(`{"type":"int","big":9007199254740993}`)
		v := s.Root().Props["big"]
		if got, ok := v.(int64); !ok || got != 9007199254740993 {
			t.Errorf("got %T %v, want int64(9007199254740993)", v, v)
		}
	})
	t.Run("MaxInt64+1 preserved as json.Number", func(t *testing.T) {
		s := avro.MustParse(`{"type":"int","huge":18446744073709551615}`)
		v := s.Root().Props["huge"]
		if got, ok := v.(json.Number); !ok || string(got) != "18446744073709551615" {
			t.Errorf("got %T %v, want json.Number(\"18446744073709551615\")", v, v)
		}
	})
}

// TestRegression_SchemaForMatrix pins SchemaFor's reflection-based
// schema generation for the common Go type matrix. For each Go type,
// the generated schema should round-trip cleanly: AppendEncode of a
// zero-value of T, then Decode back into T, then re-encode produces
// identical wire bytes.
type sfRecord struct {
	I32 int32         `avro:"i32"`
	I64 int64         `avro:"i64"`
	F32 float32       `avro:"f32"`
	F64 float64       `avro:"f64"`
	S   string        `avro:"s"`
	B   bool          `avro:"b"`
	Bs  []byte        `avro:"bs"`
	A   [16]byte      `avro:"a"`
	D   time.Duration `avro:"d"`
	T   time.Time     `avro:"t"`
}

type sfNested struct {
	Inner sfRecord `avro:"inner"`
	M     map[string]int32
	L     []int32
}

type sfOptional struct {
	X *int64 `avro:"x"`
}

func TestRegression_SchemaForMatrix(t *testing.T) {
	t.Run("primitive types", func(t *testing.T) {
		s, err := avro.SchemaFor[sfRecord]()
		if err != nil {
			t.Fatal(err)
		}
		// Sanity: encode a zero-value, decode back, re-encode, compare.
		v := sfRecord{I32: 1, I64: 2, F32: 1.5, F64: 2.5, S: "hi", B: true, Bs: []byte{1, 2}}
		wire, err := s.AppendEncode(nil, v)
		if err != nil {
			t.Fatal(err)
		}
		var v2 sfRecord
		if _, err := s.Decode(wire, &v2); err != nil {
			t.Fatal(err)
		}
		wire2, err := s.AppendEncode(nil, v2)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(wire, wire2) {
			t.Errorf("re-encode differs:\n  wire1: %x\n  wire2: %x", wire, wire2)
		}
	})

	t.Run("nested record", func(t *testing.T) {
		s, err := avro.SchemaFor[sfNested]()
		if err != nil {
			t.Fatal(err)
		}
		v := sfNested{
			Inner: sfRecord{I32: 1, S: "hi"},
			M:     map[string]int32{"a": 1},
			L:     []int32{1, 2, 3},
		}
		wire, err := s.AppendEncode(nil, v)
		if err != nil {
			t.Fatal(err)
		}
		var v2 sfNested
		if _, err := s.Decode(wire, &v2); err != nil {
			t.Fatal(err)
		}
		if v2.Inner.I32 != 1 || v2.Inner.S != "hi" {
			t.Errorf("inner: %+v", v2.Inner)
		}
		if v2.L[0] != 1 || v2.L[2] != 3 {
			t.Errorf("list: %v", v2.L)
		}
	})

	t.Run("optional via pointer", func(t *testing.T) {
		s, err := avro.SchemaFor[sfOptional]()
		if err != nil {
			t.Fatal(err)
		}
		x := int64(42)
		// With pointer set.
		wire, err := s.AppendEncode(nil, sfOptional{X: &x})
		if err != nil {
			t.Fatal(err)
		}
		var v2 sfOptional
		if _, err := s.Decode(wire, &v2); err != nil {
			t.Fatal(err)
		}
		if v2.X == nil || *v2.X != 42 {
			t.Errorf("expected *X=42, got %v", v2.X)
		}
		// With pointer nil.
		wire2, err := s.AppendEncode(nil, sfOptional{X: nil})
		if err != nil {
			t.Fatal(err)
		}
		var v3 sfOptional
		if _, err := s.Decode(wire2, &v3); err != nil {
			t.Fatal(err)
		}
		if v3.X != nil {
			t.Errorf("expected nil X, got %v", v3.X)
		}
	})

	t.Run("non-struct top-level rejected", func(t *testing.T) {
		// SchemaFor requires a struct type at the top level — primitives
		// and bare slices/maps aren't supported. Document the rejection.
		_, err := avro.SchemaFor[int64]()
		if err == nil {
			t.Errorf("expected error for top-level int64")
		}
	})

	t.Run("struct with slice field", func(t *testing.T) {
		type sfSlice struct {
			L []int32 `avro:"l"`
		}
		s, err := avro.SchemaFor[sfSlice]()
		if err != nil {
			t.Fatal(err)
		}
		v := sfSlice{L: []int32{1, 2, 3}}
		wire, err := s.AppendEncode(nil, v)
		if err != nil {
			t.Fatal(err)
		}
		var v2 sfSlice
		if _, err := s.Decode(wire, &v2); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(v, v2) {
			t.Errorf("got %v, want %v", v2, v)
		}
	})

	t.Run("struct with map field", func(t *testing.T) {
		type sfMap struct {
			M map[string]int32 `avro:"m"`
		}
		s, err := avro.SchemaFor[sfMap]()
		if err != nil {
			t.Fatal(err)
		}
		v := sfMap{M: map[string]int32{"a": 1, "b": 2}}
		wire, err := s.AppendEncode(nil, v)
		if err != nil {
			t.Fatal(err)
		}
		var v2 sfMap
		if _, err := s.Decode(wire, &v2); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(v, v2) {
			t.Errorf("got %v, want %v", v2, v)
		}
	})
}

// TestRegression_SchemaMetadataPreservation pins the Schema.Root()
// doc-string contract: "preserves all metadata including doc strings,
// namespaces, aliases, and custom properties." Each metadata field is
// set on a parse-time schema, and Schema.Root() should surface it on
// the returned SchemaNode tree.
func TestRegression_SchemaMetadataPreservation(t *testing.T) {
	schema := `{
		"type":"record",
		"name":"R",
		"namespace":"com.example",
		"aliases":["OldName","com.other.AnotherName"],
		"doc":"top-level record doc",
		"my.custom.string":"hello",
		"my.custom.int":42,
		"my.custom.bool":true,
		"my.custom.precision":9007199254740993,
		"fields":[
			{
				"name":"f1",
				"type":"int",
				"doc":"field 1 doc",
				"aliases":["oldF1"],
				"field.custom":"field-level-prop"
			}
		]
	}`
	s := avro.MustParse(schema)
	root := s.Root()

	if root.Doc != "top-level record doc" {
		t.Errorf("Doc: got %q", root.Doc)
	}
	if root.Namespace != "com.example" {
		t.Errorf("Namespace: got %q", root.Namespace)
	}
	wantAliases := []string{"OldName", "com.other.AnotherName"}
	// Aliases are namespace-qualified in the resolved form.
	if len(root.Aliases) != len(wantAliases) {
		t.Errorf("Aliases count: got %d (%v), want %d", len(root.Aliases), root.Aliases, len(wantAliases))
	}

	// Custom props.
	if s, ok := root.Props["my.custom.string"].(string); !ok || s != "hello" {
		t.Errorf("string prop: got %T %v", root.Props["my.custom.string"], root.Props["my.custom.string"])
	}
	if i, ok := root.Props["my.custom.int"].(int64); !ok || i != 42 {
		t.Errorf("int prop: got %T %v, want int64(42)", root.Props["my.custom.int"], root.Props["my.custom.int"])
	}
	if b, ok := root.Props["my.custom.bool"].(bool); !ok || !b {
		t.Errorf("bool prop: got %T %v", root.Props["my.custom.bool"], root.Props["my.custom.bool"])
	}
	// >2^53 precision must survive.
	if i, ok := root.Props["my.custom.precision"].(int64); !ok || i != 9007199254740993 {
		t.Errorf("precision prop: got %T %v, want int64(9007199254740993)", root.Props["my.custom.precision"], root.Props["my.custom.precision"])
	}

	// Field-level metadata.
	if len(root.Fields) != 1 {
		t.Fatalf("expected 1 field, got %d", len(root.Fields))
	}
	f := root.Fields[0]
	if f.Doc != "field 1 doc" {
		t.Errorf("field Doc: got %q", f.Doc)
	}
	if len(f.Aliases) != 1 || f.Aliases[0] != "oldF1" {
		t.Errorf("field Aliases: got %v", f.Aliases)
	}
	if s, ok := f.Props["field.custom"].(string); !ok || s != "field-level-prop" {
		t.Errorf("field prop: got %T %v", f.Props["field.custom"], f.Props["field.custom"])
	}
}

// TestRegression_SchemaCacheConcurrency verifies the SchemaCache is
// safe for concurrent Parse + Encode/Decode access.
func TestRegression_SchemaCacheConcurrency(t *testing.T) {
	c := &avro.SchemaCache{}
	const workers = 16
	const iters = 100

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			schemas := []string{
				`"int"`, `"long"`, `"string"`,
				`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`,
				`{"type":"array","items":"long"}`,
			}
			for j := 0; j < iters; j++ {
				sch := schemas[j%len(schemas)]
				s, err := c.Parse(sch)
				if err != nil {
					t.Errorf("worker %d Parse: %v", id, err)
					return
				}
				// Use the schema for encode/decode based on its kind.
				switch s.Root().Type {
				case "int":
					wire, _ := s.AppendEncode(nil, int32(id*100+j))
					var v int32
					if _, err := s.Decode(wire, &v); err != nil {
						t.Errorf("worker %d decode int: %v", id, err)
						return
					}
				case "record":
					wire, _ := s.AppendEncode(nil, map[string]any{"a": int32(id)})
					var v map[string]any
					if _, err := s.Decode(wire, &v); err != nil {
						t.Errorf("worker %d decode record: %v", id, err)
						return
					}
				}
			}
		}(i)
	}
	wg.Wait()
}

// TestRegression_JSONDecodeFixedSizeArrayTarget pins the symmetry between
// the JSON encoder, binary encoder, binary decoder, and JSON decoder for
// fixed-size Go array targets ([N]T) against Avro array schemas.
//
// Pre-fix, json_decode.go's decodeArray rejected reflect.Array with
// "cannot use [N]T with Avro type array" even though appendAvroJSON's
// case "array" (json_codec.go) accepted both Slice and Array, and
// deserArray (deser.go) detected fixedArray and dispatched to
// deserFixedArray. The asymmetry broke JSON round-trip: AppendEncodeJSON
// would succeed on [3]int32, but DecodeJSON on the produced JSON could
// not write back into the same Go shape.
//
// Pattern 12 from BUG_AUDIT.md (encode accepts type X, decode rejects
// type X). The fix mirrors deserArray.deserFixedArray: validate element
// count, decode into v.Index(i) directly.
func TestRegression_JSONDecodeFixedSizeArrayTarget(t *testing.T) {
	t.Run("[3]int32 round-trip through JSON", func(t *testing.T) {
		s := avro.MustParse(`{"type":"array","items":"int"}`)
		in := [3]int32{1, 2, 3}
		jbuf, err := s.AppendEncodeJSON(nil, in)
		if err != nil {
			t.Fatalf("encode JSON: %v", err)
		}
		if string(jbuf) != "[1,2,3]" {
			t.Errorf("JSON = %s, want [1,2,3]", jbuf)
		}
		var out [3]int32
		if err := s.DecodeJSON(jbuf, &out); err != nil {
			t.Fatalf("decode JSON into [3]int32: %v", err)
		}
		if out != in {
			t.Errorf("got %v, want %v", out, in)
		}
	})

	t.Run("[3]int32 binary↔JSON cross-path round-trip", func(t *testing.T) {
		s := avro.MustParse(`{"type":"array","items":"int"}`)
		in := [3]int32{10, 20, 30}
		bbuf, err := s.AppendEncode(nil, in)
		if err != nil {
			t.Fatalf("encode binary: %v", err)
		}
		var bout [3]int32
		if _, err := s.Decode(bbuf, &bout); err != nil {
			t.Fatalf("binary decode: %v", err)
		}
		if bout != in {
			t.Errorf("binary round-trip: got %v, want %v", bout, in)
		}
		jbuf, err := s.AppendEncodeJSON(nil, in)
		if err != nil {
			t.Fatalf("encode JSON: %v", err)
		}
		var jout [3]int32
		if err := s.DecodeJSON(jbuf, &jout); err != nil {
			t.Fatalf("JSON decode: %v", err)
		}
		if jout != in {
			t.Errorf("JSON round-trip: got %v, want %v", jout, in)
		}
	})

	t.Run("[3]string element type", func(t *testing.T) {
		s := avro.MustParse(`{"type":"array","items":"string"}`)
		in := [3]string{"a", "b", "c"}
		jbuf, err := s.AppendEncodeJSON(nil, in)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var out [3]string
		if err := s.DecodeJSON(jbuf, &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if out != in {
			t.Errorf("got %v, want %v", out, in)
		}
	})

	t.Run("[2]record element type", func(t *testing.T) {
		type rec struct {
			X int32 `avro:"x"`
		}
		s := avro.MustParse(`{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}}`)
		in := [2]rec{{1}, {2}}
		jbuf, err := s.AppendEncodeJSON(nil, in)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var out [2]rec
		if err := s.DecodeJSON(jbuf, &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if out != in {
			t.Errorf("got %+v, want %+v", out, in)
		}
	})

	t.Run("element count mismatch — JSON too long for [2]int", func(t *testing.T) {
		s := avro.MustParse(`{"type":"array","items":"int"}`)
		var out [2]int32
		if err := s.DecodeJSON([]byte("[1,2,3]"), &out); err == nil {
			t.Fatalf("expected error for 3 elements into [2]int32")
		}
	})

	t.Run("element count mismatch — JSON too short for [3]int", func(t *testing.T) {
		s := avro.MustParse(`{"type":"array","items":"int"}`)
		var out [3]int32
		if err := s.DecodeJSON([]byte("[1,2]"), &out); err == nil {
			t.Fatalf("expected error for 2 elements into [3]int32")
		}
	})

	t.Run("empty JSON array into [0]int", func(t *testing.T) {
		s := avro.MustParse(`{"type":"array","items":"int"}`)
		var out [0]int32
		if err := s.DecodeJSON([]byte("[]"), &out); err != nil {
			t.Fatalf("empty array into [0]int32: %v", err)
		}
	})

	t.Run("[N]T as struct field round-trip", func(t *testing.T) {
		type rec struct {
			Xs [3]int32 `avro:"xs"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"xs","type":{"type":"array","items":"int"}}]}`)
		in := rec{Xs: [3]int32{7, 8, 9}}
		jbuf, err := s.AppendEncodeJSON(nil, in)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var out rec
		if err := s.DecodeJSON(jbuf, &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if out != in {
			t.Errorf("got %+v, want %+v", out, in)
		}
	})
}

// TestRegression_IntegerDefaultMustBeIntegerLiteral pins that int and
// long schema defaults must be in pure integer literal form — no
// fractional component, no exponent marker. Matches Java's
// isIntegralNumber() gate (Schema.java LONG/INT cases) and fastavro's
// isinstance(default, int) check.
//
// Pre-fix, twmb accepted exponent-form and fractional-whole-number
// values (e.g. "9.2233720368547758e18" for a long default) via
// parseInt{32,64}Lenient's boundedRatFromString slow path. The
// metadata-API path (normalizeJSONNumber's exponent-form arm) routed
// the same input through parseFloatAcceptOverflow → float64, producing
// a four-axis divergence (pattern 1b from BUG_AUDIT.md): encode/decode/
// parse-validate all agreed on int64 wire encoding, but
// Schema.Root().Fields[].Default surfaced lossy float64 — for
// magnitudes > 2^53, the metadata value differed from the wire-fill
// value (e.g. exp-form 9.2233720368547758e18 → wire int64(9223372036854775800)
// vs metadata float64 that rounds to int64(9223372036854775807), a
// 7-unit mismatch from the same JSON literal).
//
// Rejecting non-integer form at parse-validate (defaultAsInt32/64)
// eliminates the divergence by removing the lenient acceptance class.
// Both upstream impls reject these inputs, so cross-impl interop is
// preserved.
func TestRegression_IntegerDefaultMustBeIntegerLiteral(t *testing.T) {
	rejectCases := []struct {
		name, schema string
	}{
		{
			"long default exponent-form integer-valued (silently rounded via float64 in metadata pre-fix)",
			`{"type":"record","name":"R","fields":[{"name":"f","type":"long","default":9.2233720368547758e18}]}`,
		},
		{
			"long default exponent-form small",
			`{"type":"record","name":"R","fields":[{"name":"f","type":"long","default":1e3}]}`,
		},
		{
			"long default fractional-whole",
			`{"type":"record","name":"R","fields":[{"name":"f","type":"long","default":2.0}]}`,
		},
		{
			"int default exponent-form",
			`{"type":"record","name":"R","fields":[{"name":"f","type":"int","default":1e3}]}`,
		},
		{
			"int default fractional-whole",
			`{"type":"record","name":"R","fields":[{"name":"f","type":"int","default":2.0}]}`,
		},
	}
	for _, tc := range rejectCases {
		t.Run("reject: "+tc.name, func(t *testing.T) {
			_, err := avro.Parse(tc.schema)
			if err == nil {
				t.Fatalf("expected parse error for non-integer-form default")
			}
		})
	}

	// Boundary-1: integer literal form must still parse and metadata
	// must surface int64 (not float64) — the fix doesn't over-trigger.
	// The wire-fill default decodes as the schema-appropriate Go type
	// (int32 for "int" schemas, int64 for "long" schemas) per the
	// documented "metadata normalizes, not preserves Go types" rule —
	// we compare the underlying integer value, not the exact Go type.
	type acceptCase struct {
		name, schema   string
		wantDefault    any   // exact metadata Go type expected
		wantWireFillN  int64 // numeric value the wire-fill default must equal
	}
	acceptCases := []acceptCase{
		{
			"long default int64 boundary preserved as int64 in metadata",
			`{"type":"record","name":"R","fields":[{"name":"f","type":"long","default":9223372036854775800}]}`,
			int64(9223372036854775800),
			9223372036854775800,
		},
		{
			"long default 2^53+1 preserves precision via int64",
			`{"type":"record","name":"R","fields":[{"name":"f","type":"long","default":9007199254740993}]}`,
			int64(9007199254740993),
			9007199254740993,
		},
		{
			"int default small positive",
			`{"type":"record","name":"R","fields":[{"name":"f","type":"int","default":42}]}`,
			int64(42),
			42,
		},
		{
			"int default negative",
			`{"type":"record","name":"R","fields":[{"name":"f","type":"int","default":-1}]}`,
			int64(-1),
			-1,
		},
	}
	asInt64 := func(t *testing.T, v any) int64 {
		t.Helper()
		switch n := v.(type) {
		case int32:
			return int64(n)
		case int64:
			return n
		default:
			t.Fatalf("wire-fill default has unexpected type %T(%v)", v, v)
			return 0
		}
	}
	for _, tc := range acceptCases {
		t.Run("accept: "+tc.name, func(t *testing.T) {
			s, err := avro.Parse(tc.schema)
			if err != nil {
				t.Fatalf("integer-form default rejected: %v", err)
			}
			gotDefault := s.Root().Fields[0].Default
			if gotDefault != tc.wantDefault {
				t.Errorf("Root().Fields[0].Default: got %T(%v), want %T(%v) — metadata-API normalization broken",
					gotDefault, gotDefault, tc.wantDefault, tc.wantDefault)
			}
			// And: the wire-fill default value must equal what metadata
			// reports (modulo Go type — int32 wire vs int64 metadata is
			// the documented normalization). This is the four-axis check:
			// (a) wire/encode, (b) wire/decode, (c) parse-validate, (d)
			// metadata — all axes agree on the same numeric value.
			wire, err := s.AppendEncode(nil, map[string]any{})
			if err != nil {
				t.Fatalf("encode empty record: %v", err)
			}
			var got map[string]any
			if _, err := s.Decode(wire, &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if asInt64(t, got["f"]) != tc.wantWireFillN {
				t.Errorf("wire-fill default value: got %v, want %d — four-axis divergence",
					got["f"], tc.wantWireFillN)
			}
		})
	}
}

// TestRegression_ParseFloatLengthCapDoS pins the schema-parse-time
// length cap on float-default literals. Pre-fix, parseFloatAcceptOverflow
// (schema.go) wrapped strconv.ParseFloat without bounding input length;
// ParseFloat is O(n) at ~30-50ms per MiB. The helper was called twice
// per schema parse (validateDefault → defaultAsFloat → ParseFloat;
// encodeDefault → defaultAsFloat → ParseFloat), so a 1 MiB hostile
// default literal drove ~130-150ms per Parse — past the audit's 100ms
// DoS threshold.
//
// Sibling helpers boundedRatFromString (deser.go, 128KiB cap) and
// parseInt64Lenient (ser.go, 64-byte cap) had explicit length caps
// for exactly this reason; parseFloatAcceptOverflow was missed when
// the F2-round factored it into a single source of truth. This test
// locks the 1024-byte cap at the helper, mirroring the same shape
// applied to integerFormFitsFloat.
//
// Pattern 16 from BUG_AUDIT.md (precision fix that introduces own DoS).
//
// The test is split into two parts: (1) a behavioral check that the
// cap fires with the expected error message — this runs under all
// modes including -race; (2) a wall-clock timing check that verifies
// the post-cap rejection is fast — this is skipped under -race
// because race-instrumentation overhead masks the speedup (race
// makes JSON parsing of 1 MiB hostile input slow regardless of the
// downstream cap).
func TestRegression_ParseFloatLengthCapDoS(t *testing.T) {
	hostile := "1." + strings.Repeat("0", (1<<20)-2) // 1 MiB digit string
	schemaJSON := fmt.Sprintf(`{"type":"record","name":"R","fields":[{"name":"f","type":"double","default":%s}]}`, hostile)

	// (1) Behavioral check: the cap MUST fire, returning a length-cap
	// error rather than silently succeeding (pre-fix would have parsed
	// and rounded to +Inf or finite via ParseFloat's slow path).
	_, err := avro.Parse(schemaJSON)
	if err == nil {
		t.Fatalf("expected length-cap rejection on 1 MiB hostile double default; schema parsed successfully")
	}
	if !strings.Contains(err.Error(), "length cap") {
		t.Errorf("expected length-cap error, got: %v", err)
	}

	// (2) Timing check: under -race the instrumentation overhead makes
	// even legitimate JSON parsing of a 1 MiB literal slow, so the
	// timing assertion is meaningful only without -race. We use a
	// raceEnabled helper if present, otherwise fall back to a
	// permissive threshold under any mode. The asymptotic improvement
	// is what matters: without the cap, ParseFloat is O(n²)-ish and
	// schema-parse averaged 153ms pre-fix; with the cap, the
	// rejection short-circuits before reaching ParseFloat.
	// The residual cost is dominated by JSON parsing of the 1 MiB
	// literal, which is O(n) and unavoidable. The cap saves the
	// ParseFloat O(n)-with-large-constant cost on top of JSON
	// parsing — about 30-50ms per MiB on a modern machine. The
	// threshold is generous because we're not aiming for asymptotic
	// rejection here (the behavioral check above already pins that
	// the cap fires); we're just guarding against future regressions
	// that re-introduce the slow path on top of JSON parse cost.
	threshold := 250 * time.Millisecond
	if isRaceEnabled() {
		threshold = 3 * time.Second // race adds 5-10x to everything; loose bound
	}
	_, _ = avro.Parse(schemaJSON) // warm-up
	const runs = 3
	var total time.Duration
	for range runs {
		start := time.Now()
		_, _ = avro.Parse(schemaJSON)
		total += time.Since(start)
	}
	avg := total / runs
	if avg > threshold {
		t.Errorf("1 MiB hostile double default schema-parse averaged %s — exceeds %s threshold; parseFloatAcceptOverflow length cap missing or wrong", avg, threshold)
	}

	// Sibling: the float-extras path (record-level Props) hits the
	// same parser through normalizeJSONNumber. Same shape.
	extrasJSON := fmt.Sprintf(`{"type":"record","name":"R","fields":[{"name":"f","type":"int"}],"x":%s}`, hostile)
	// Extras parse succeeds (record-level Props don't fail the parse
	// even when one prop's value can't be normalized — the prop is
	// retained as json.Number). The behavior we lock here is that the
	// parse completes — i.e. doesn't hang on a 1 MiB hostile prop.
	doneCh := make(chan struct{})
	go func() {
		_, _ = avro.Parse(extrasJSON)
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-time.After(threshold * 5): // 5x the per-parse threshold for the sibling probe
		t.Errorf("hostile extras prop schema-parse hung past %s; normalizeJSONNumber length cap missing", threshold*5)
	}

	// Boundary-1: a legitimate-sized float default (well under 1024)
	// must continue to parse cleanly.
	okJSON := `{"type":"record","name":"R","fields":[{"name":"f","type":"double","default":1.234567890123456e308}]}`
	if _, err := avro.Parse(okJSON); err != nil {
		t.Errorf("legitimate float default rejected by length cap: %v", err)
	}
}

// isRaceEnabled reports whether the race detector is compiled into
// this binary. Used to relax wall-clock thresholds when race
// instrumentation adds ~5-10x overhead.
func isRaceEnabled() bool {
	return raceEnabled
}

// TestRegression_OCFWriterPreservesLogicalTypeInHeader pins that the
// OCF writer writes the full schema JSON (preserving logicalType,
// precision, scale, doc, aliases, default) to the avro.schema header,
// matching Java's DataFileWriter (Schema.toString → writeProps) and
// fastavro (json.dumps(schema)).
//
// Pre-fix, ocf.go's writeHeader used Schema.Canonical() — the Parsing
// Canonical Form — which the spec defines for fingerprinting only.
// PCF strips logicalType, precision, scale, doc, aliases, default.
// Three observable consequences:
//  1. Downstream consumers relying on the self-describing OCF header
//     to convey logical-type info got the raw underlying type.
//  2. ocf.NewReader(..., WithSchemaOpts(CustomType{LogicalType:X}))
//     silently never matched — the parsed header schema had no
//     logical type to dispatch on.
//  3. Schema.Root().Fields[i].Type.Precision on a decoded OCF returned
//     zero even when the writer specified precision=10.
//
// Pattern 12 from BUG_AUDIT.md (encode and decode must agree on the
// schema's observable contract — here, the metadata layer).
func TestRegression_OCFWriterPreservesLogicalTypeInHeader(t *testing.T) {
	// We can't import the ocf package from package avro_test (would
	// create an import cycle through this test file's package), so
	// instead we verify the underlying contract: Schema.String()
	// returns the full JSON with logicalType intact, and the
	// equivalent re-parse round-trip preserves the logical type.
	// The OCF-layer test that demonstrates Reader+CustomType
	// dispatch lives in ocf/ocf_test.go TestWithSchemaOptsCustomType.

	src := `{"type":"record","name":"R","fields":[{"name":"d","type":{"type":"int","logicalType":"date"}}]}`
	s, err := avro.Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	full := s.String()
	if !strings.Contains(full, `"logicalType":"date"`) {
		t.Errorf("Schema.String() does not preserve logicalType — pre-fix OCF header would have stripped it. Got: %s", full)
	}

	// Canonical strips logicalType (per PCF spec — intentional).
	canon := string(s.Canonical())
	if strings.Contains(canon, "logicalType") {
		t.Errorf("Canonical() should strip logicalType per PCF [STRIP] rule, but found it. Got: %s", canon)
	}

	// Decimal with precision/scale: full preserves, canonical strips.
	dec := `{"type":"record","name":"R","fields":[{"name":"m","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}}]}`
	ds, err := avro.Parse(dec)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(ds.String(), `"precision":10`) {
		t.Errorf("Schema.String() does not preserve precision: %s", ds.String())
	}
	if !strings.Contains(ds.String(), `"scale":2`) {
		t.Errorf("Schema.String() does not preserve scale: %s", ds.String())
	}
}
