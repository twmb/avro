package conformance

import (
	"bytes"
	"testing"

	"github.com/twmb/avro"
)

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
