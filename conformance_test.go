package avro_test

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"math/big"
	"reflect"
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
	t.Run("decimal precision zero falls back to bytes", func(t *testing.T) {
		s, err := avro.Parse(`{"type":"bytes","logicalType":"decimal","precision":0,"scale":0}`)
		if err != nil {
			t.Fatalf("invalid logical type should fall back to bytes: %v", err)
		}
		in := []byte{0xaa, 0xbb}
		encoded, err := s.AppendEncode(nil, &in)
		if err != nil {
			t.Fatal(err)
		}
		var out []byte
		if _, err := s.Decode(encoded, &out); err != nil {
			t.Fatal(err)
		}
		if string(out) != string(in) {
			t.Fatalf("got %x, want %x", out, in)
		}
	})

	t.Run("decimal scale exceeds precision falls back to bytes", func(t *testing.T) {
		s, err := avro.Parse(`{"type":"bytes","logicalType":"decimal","precision":5,"scale":6}`)
		if err != nil {
			t.Fatalf("invalid logical type should fall back to bytes: %v", err)
		}
		in := []byte{0x01, 0x02, 0x03}
		encoded, err := s.AppendEncode(nil, &in)
		if err != nil {
			t.Fatal(err)
		}
		var out []byte
		if _, err := s.Decode(encoded, &out); err != nil {
			t.Fatal(err)
		}
		if string(out) != string(in) {
			t.Fatalf("got %x, want %x", out, in)
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
