package avro

import (
	"bytes"
	"crypto"
	"crypto/md5"
	"crypto/sha256"
	"crypto/sha512"
	"encoding"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// ---------- schema_test.go ----------

func TestCanonical(t *testing.T) {
	s := mustParse(t, `{"type":"record","name":"r","fields":[{"name":"a","type":"int"}]}`)
	got := string(s.Canonical())
	if !strings.Contains(got, `"name":"r"`) {
		t.Errorf("canonical form missing name: %s", got)
	}
}

func TestCanonicalStripsLogicalType(t *testing.T) {
	// Per the Avro spec STRIP rule, canonical form keeps only:
	// type, name, fields, symbols, items, values, size.
	// logicalType, precision, and scale must be stripped.
	tests := []struct {
		name   string
		schema string
		want   string
	}{
		{
			"decimal on fixed",
			`{"type":"fixed","name":"Money","size":8,"logicalType":"decimal","precision":16,"scale":2}`,
			`{"name":"Money","type":"fixed","size":8}`,
		},
		{
			"duration on fixed",
			`{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}`,
			`{"name":"dur","type":"fixed","size":12}`,
		},
		{
			"date on int",
			`{"type":"int","logicalType":"date"}`,
			`"int"`,
		},
		{
			// Java's SchemaNormalization.build and fastavro's
			// _to_parsing_canonical_form both emit "type":"record" for an
			// error-typed record — Java stores both as Type.RECORD with an isError
			// flag the canonical form ignores. Normalizing here is what makes
			// twmb's fingerprints match theirs for error-typed schemas;
			// Schema.Root().Type and Schema.String() still preserve the
			// JSON-as-written "error".
			"error normalizes to record",
			`{"type":"error","name":"E","fields":[{"name":"x","type":"int"}]}`,
			`{"name":"E","type":"record","fields":[{"name":"x","type":"int"}]}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustParse(t, tt.schema)
			got := string(s.Canonical())
			if got != tt.want {
				t.Errorf("got  %s\nwant %s", got, tt.want)
			}
		})
	}
}

// The PCF [PRIMITIVES] rule ("convert {"type":"X"} to X") and [STRIP] rule
// (keep only type/name/fields/symbols/items/values/size) apply recursively
// inside array items and map values, exactly as they do at the top level,
// in record fields, and in union branches. Java's SchemaNormalization.build
// recurses into getElementType()/getValueType(); fastavro's
// _to_parsing_canonical_form does the same. A schema whose items/values is
// written in wrapped or attribute-bearing form must canonicalize identically
// to the same schema written in bare form, so the fingerprint (and thus
// Single Object Encoding framing) matches every other implementation.
func TestCanonicalNormalizesArrayItemsAndMapValues(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		want   string
	}{
		{
			"array wrapped primitive items",
			`{"type":"array","items":{"type":"int"}}`,
			`{"type":"array","items":"int"}`,
		},
		{
			"map wrapped primitive values",
			`{"type":"map","values":{"type":"int"}}`,
			`{"type":"map","values":"int"}`,
		},
		{
			"array items strips logicalType",
			`{"type":"array","items":{"type":"long","logicalType":"timestamp-millis"}}`,
			`{"type":"array","items":"long"}`,
		},
		{
			"map values strips logicalType",
			`{"type":"map","values":{"type":"long","logicalType":"timestamp-millis"}}`,
			`{"type":"map","values":"long"}`,
		},
		{
			"nested array of map of wrapped primitive",
			`{"type":"array","items":{"type":"map","values":{"type":"string"}}}`,
			`{"type":"array","items":{"type":"map","values":"string"}}`,
		},
		{
			// Boundary: bare items already equals its canonical child, so
			// this case is unaffected by the bug and must keep working.
			"array bare primitive items unchanged",
			`{"type":"array","items":"int"}`,
			`{"type":"array","items":"int"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustParse(t, tt.schema)
			if got := string(s.Canonical()); got != tt.want {
				t.Errorf("got  %s\nwant %s", got, tt.want)
			}
		})
	}
}

// Official Apache Avro schema-tests.txt vector 031:
//
//	input { "items":{"type":"null"}, "type":"array"} canonicalizes to
//	{"type":"array","items":"null"} with CRC-64-AVRO fingerprint
//	-589620603366471059 (Java signed-int64). The fingerprint can only match
//	when array items are canonicalized per [PRIMITIVES].
func TestFingerprintArrayItemsMatchesSpecVector(t *testing.T) {
	s := mustParse(t, `{ "items":{"type":"null"}, "type":"array"}`)
	if got := string(s.Canonical()); got != `{"type":"array","items":"null"}` {
		t.Fatalf("canonical: got %s", got)
	}
	h := NewRabin()
	s.Fingerprint(h)
	var javaFP int64 = -589620603366471059 // spec vector 031, signed int64
	if got := h.Sum64(); got != uint64(javaFP) {
		t.Errorf("Sum64 = %d, want %d (spec vector 031)", got, uint64(javaFP))
	}
}

func TestFingerprint(t *testing.T) {
	s := mustParse(t, `"int"`)
	fp := s.Fingerprint(sha256.New())
	if len(fp) != 32 {
		t.Fatalf("expected 32 bytes, got %d", len(fp))
	}
	if bytes.Equal(fp, make([]byte, 32)) {
		t.Fatal("fingerprint is all zeros")
	}
}

func TestFingerprintRabin(t *testing.T) {
	tests := []struct {
		schema    string
		canonical string
		sum64     uint64
	}{
		{`"null"`, `"null"`, 7195948357588979594},
		{`{"type":"fixed","name":"foo","size":15}`, `{"name":"foo","type":"fixed","size":15}`, 1756455273707447556},
		{
			`{"type":"record","name":"foo","fields":[{"name":"f1","type":"boolean"}]}`,
			`{"name":"foo","type":"record","fields":[{"name":"f1","type":"boolean"}]}`,
			7843277075252814651,
		},
	}
	for _, tt := range tests {
		s, err := Parse(tt.schema)
		if err != nil {
			t.Fatalf("schema %s: %v", tt.schema, err)
		}
		if got := string(s.Canonical()); got != tt.canonical {
			t.Errorf("canonical: got %s, want %s", got, tt.canonical)
		}
		h := NewRabin()
		fp := s.Fingerprint(h)
		if got := h.Sum64(); got != tt.sum64 {
			t.Errorf("schema %s: Sum64 = %d, want %d", tt.schema, got, tt.sum64)
		}
		if len(fp) != 8 {
			t.Fatalf("expected 8 bytes, got %d", len(fp))
		}
	}

	// Verify Sum bytes for "null".
	h := NewRabin()
	h.Write([]byte(`"null"`))
	got := h.Sum(nil)
	want := []byte{0x63, 0xdd, 0x24, 0xe7, 0xcc, 0x25, 0x8f, 0x8a}
	if !bytes.Equal(got, want) {
		t.Errorf("Sum bytes = %x, want %x", got, want)
	}
}

func TestRabinReset(t *testing.T) {
	h := NewRabin()
	h.Write([]byte("hello"))
	before := h.Sum64()
	h.Reset()
	h.Write([]byte("hello"))
	after := h.Sum64()
	if before != after {
		t.Errorf("after Reset: got %d, want %d", after, before)
	}
	if h.Size() != 8 {
		t.Errorf("Size() = %d, want 8", h.Size())
	}
	if h.BlockSize() != 1 {
		t.Errorf("BlockSize() = %d, want 1", h.BlockSize())
	}
}

// canonicalBytes (schema_canonical.go) is the single-pass writer of the
// PCF form; these cases pin its key order and required-empty-array rules
// for the aschema/aobject shapes (the former aschema/aobject MarshalJSON
// methods it replaced).
func TestMarshalJSON(t *testing.T) {
	t.Run("primitive", func(t *testing.T) {
		b := appendCanonSchema(nil, &aschema{primitive: "int"})
		if string(b) != `"int"` {
			t.Errorf("got %s, want \"int\"", b)
		}
	})

	t.Run("object", func(t *testing.T) {
		b := appendCanonSchema(nil, &aschema{object: &aobject{Name: "r", Type: "record"}})
		if !strings.Contains(string(b), `"name":"r"`) {
			t.Errorf("got %s, want object with name r", b)
		}
	})

	// Non-PCF attribute branches: stripped from canonical form (the canon
	// tree zeroes namespace, aliases, default, logicalType, precision,
	// scale), so they only run when an unstripped aobject is written; the
	// writer emits them faithfully and in declaration order after the PCF
	// keys.
	t.Run("object full attrs", func(t *testing.T) {
		ns := "com.example"
		prec := 9
		scale := 2
		o := aobject{
			Name:      "r",
			Type:      "record",
			Namespace: &ns,
			Aliases:   []string{"old"},
			Default:   json.RawMessage(`null`),
			Logical:   "decimal",
			Precision: &prec,
			Scale:     &scale,
		}
		got := string(appendCanonObject(nil, &o))
		want := `{"name":"r","type":"record","fields":[],"namespace":"com.example","aliases":["old"],"default":null,"logicalType":"decimal","precision":9,"scale":2}`
		if got != want {
			t.Errorf("\n got %s\nwant %s", got, want)
		}
	})

	// Defensive: a non-record type with Fields, or a non-enum type with
	// Symbols, is nonsense per the spec but is still emitted.
	t.Run("object defensive fields on non-record", func(t *testing.T) {
		o := aobject{Type: "int", Fields: []afield{{Name: "x", Type: &aschema{primitive: "int"}}}}
		if b := appendCanonObject(nil, &o); !strings.Contains(string(b), `"fields":[`) {
			t.Errorf("got %s, want fields to be emitted", b)
		}
	})
	t.Run("object defensive symbols on non-enum", func(t *testing.T) {
		o := aobject{Type: "int", Symbols: []string{"A", "B"}}
		if b := appendCanonObject(nil, &o); !strings.Contains(string(b), `"symbols":["A","B"]`) {
			t.Errorf("got %s, want symbols to be emitted", b)
		}
	})

	// Enum with nil Symbols slice still emits "symbols":[] (required).
	t.Run("object nil enum symbols", func(t *testing.T) {
		o := aobject{Name: "E", Type: "enum"}
		if b := appendCanonObject(nil, &o); string(b) != `{"name":"E","type":"enum","symbols":[]}` {
			t.Errorf("got %s", b)
		}
	})

	t.Run("union", func(t *testing.T) {
		s := aschema{union: []aschema{{primitive: "null"}, {primitive: "int"}}}
		if b := appendCanonSchema(nil, &s); string(b) != `["null","int"]` {
			t.Errorf("got %s", b)
		}
	})
}

func TestUnmarshalJSONInvalid(t *testing.T) {
	// Invalid first byte (number).
	if _, err := parseSchemaTree(`123`); err == nil {
		t.Fatal("expected error")
	}
	// Empty data.
	if _, err := parseSchemaTree(``); err == nil {
		t.Fatal("expected error for empty data")
	}
}

func TestParseFixedStringSizeINTEGERS(t *testing.T) {
	// Per the Avro spec's [INTEGERS] canonical form rule, "size" may
	// appear as a quoted integer (e.g. "16" instead of 16).
	tests := []struct {
		name   string
		schema string
	}{
		{
			"string size",
			`{"type":"fixed","name":"F","size":"16"}`,
		},
		{
			"string size in record field",
			`{"type":"record","name":"R","fields":[
				{"name":"f","type":{"type":"fixed","name":"F","size":"4"}}
			]}`,
		},
		{
			"string size with leading zeros",
			`{"type":"fixed","name":"F","size":"016"}`,
		},
		{
			// Size 0 is legal (spec: "an integer"; Java rejects only
			// negatives) — in quoted form it flows through the same
			// laxInt path as any other quoted size.
			"string size zero",
			`{"type":"fixed","name":"F","size":"0"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustParse(t, tt.schema)
			if s == nil {
				t.Fatal("nil schema")
			}
		})
	}

	// Errors: non-numeric strings, empty strings, negative.
	errTests := []struct {
		name   string
		schema string
	}{
		{"non-numeric string", `{"type":"fixed","name":"F","size":"abc"}`},
		{"empty string", `{"type":"fixed","name":"F","size":""}`},
		{"negative string", `{"type":"fixed","name":"F","size":"-1"}`},
	}
	for _, tt := range errTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.schema)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestParseFixedStringSizeRoundTrip(t *testing.T) {
	// String size and int size should produce identical schemas.
	s1, err := Parse(`{"type":"fixed","name":"F","size":"4"}`)
	if err != nil {
		t.Fatalf("string size: %v", err)
	}
	s2, err := Parse(`{"type":"fixed","name":"F","size":4}`)
	if err != nil {
		t.Fatalf("int size: %v", err)
	}

	data := []byte{1, 2, 3, 4}
	b1, err := s1.Encode(data)
	if err != nil {
		t.Fatalf("encode string-size: %v", err)
	}
	b2, err := s2.Encode(data)
	if err != nil {
		t.Fatalf("encode int-size: %v", err)
	}
	if !bytes.Equal(b1, b2) {
		t.Errorf("encodings differ: %x vs %x", b1, b2)
	}
}

func TestParseFloatDefaultFromString(t *testing.T) {
	// Java's parseField at Schema.java:1899-1902 coerces TextNode → DoubleNode when
	// the OUTER fieldSchema.getType() is FLOAT or DOUBLE directly. Spec 1.12
	// §"Record" default-values table marks JSON string as invalid for float/double
	// defaults, so the coercion is a deployed-Java interop carveout that avro-rs and
	// goavro do not implement. For UNION outer types it does NOT fire — the TextNode
	// reaches isValidDefault (Schema.java:1751-1797) and rejects, no numeric branch's
	// isNumber()/isIntegralNumber() returning true for a TextNode.
	tests := []struct {
		name   string
		schema string
	}{
		{
			"float string default",
			`{"type":"record","name":"R","fields":[
				{"name":"f","type":"float","default":"3.14"}
			]}`,
		},
		{
			"double string default",
			`{"type":"record","name":"R","fields":[
				{"name":"d","type":"double","default":"2.718"}
			]}`,
		},
		{
			"float NaN string default",
			`{"type":"record","name":"R","fields":[
				{"name":"f","type":"float","default":"NaN"}
			]}`,
		},
		{
			"float Inf string default",
			`{"type":"record","name":"R","fields":[
				{"name":"f","type":"float","default":"Inf"}
			]}`,
		},
		{
			"nested record with float string default",
			`{"type":"record","name":"R","fields":[
				{"name":"inner","type":{"type":"record","name":"I","fields":[
					{"name":"f","type":"double"}
				]},"default":{"f":"2.5"}}
			]}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustParse(t, tt.schema)
			if s == nil {
				t.Fatal("nil schema")
			}
		})
	}

	// Invalid string defaults should still fail.
	_, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"f","type":"float","default":"not-a-number"}
	]}`)
	if err == nil {
		t.Fatal("expected error for invalid string float default")
	}

	// Union outer types reject string defaults for numeric branches —
	// Java parity (parseField's text→DoubleNode coercion does not
	// fire for UNION outer types). See
	// TestMatrix_UnionDefaultStringMatchesOnlyStringAcceptingBranches
	// for the full matrix.
	_, err = Parse(`{"type":"record","name":"R","fields":[
		{"name":"f","type":["float","null"],"default":"1.5"}
	]}`)
	if err == nil {
		t.Fatal("expected error for union+string-numeric default")
	}
}

func TestParseErrors(t *testing.T) {
	tests := []struct {
		name   string
		schema string
	}{
		{"invalid json", `{invalid`},
		{"nil schema", `null`},
		{"unknown primitive", `"foobar"`},
		{"unknown complex type", `{"type":"foobar"}`},
		{"record field with invalid union", `{"type":"record","name":"R","fields":[{"name":"f","type":["int","int"]}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.schema)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestParseFlatFieldFormat(t *testing.T) {
	// The "flat" field format puts complex-type attributes (symbols,
	// items, values, fields, size) at the field level rather than in a
	// nested type object. linkedin/goavro accepts this, and we must too
	// for migration compatibility.
	tests := []struct {
		name   string
		schema string
	}{
		// Basic flat types.
		{
			"flat enum",
			`{"type":"record","name":"R","fields":[
				{"name":"E","type":"enum","symbols":["A","B","C"]}
			]}`,
		},
		{
			"flat array",
			`{"type":"record","name":"R","fields":[
				{"name":"A","type":"array","items":"int"}
			]}`,
		},
		{
			"flat map",
			`{"type":"record","name":"R","fields":[
				{"name":"M","type":"map","values":"long"}
			]}`,
		},
		{
			"flat record",
			`{"type":"record","name":"R","fields":[
				{"name":"Inner","type":"record","fields":[
					{"name":"x","type":"int"}
				]}
			]}`,
		},
		{
			"flat fixed",
			`{"type":"record","name":"R","fields":[
				{"name":"F","type":"fixed","size":4}
			]}`,
		},
		{
			"flat error type",
			`{"type":"record","name":"R","fields":[
				{"name":"Err","type":"error","fields":[
					{"name":"msg","type":"string"}
				]}
			]}`,
		},

		// Field-level keys ("default", "order", "aliases") must not
		// leak into the lifted type object.
		{
			"flat enum with default",
			`{"type":"record","name":"R","fields":[
				{"name":"E","type":"enum","symbols":["A","B"],"default":"A"}
			]}`,
		},
		{
			"flat array with default",
			`{"type":"record","name":"R","fields":[
				{"name":"A","type":"array","items":"int","default":[]}
			]}`,
		},
		{
			"flat map with default",
			`{"type":"record","name":"R","fields":[
				{"name":"M","type":"map","values":"string","default":{}}
			]}`,
		},
		{
			"flat enum with order",
			`{"type":"record","name":"R","fields":[
				{"name":"E","type":"enum","symbols":["X","Y"],"order":"descending"}
			]}`,
		},
		{
			"flat array with aliases",
			`{"type":"record","name":"R","fields":[
				{"name":"A","type":"array","items":"string","aliases":["old_A"]}
			]}`,
		},

		// Namespace handling for named types.
		{
			"flat enum with namespace",
			`{"type":"record","name":"R","fields":[
				{"name":"E","type":"enum","namespace":"com.example","symbols":["A","B"]}
			]}`,
		},
		{
			"flat fixed with namespace",
			`{"type":"record","name":"R","fields":[
				{"name":"F","type":"fixed","namespace":"com.example","size":8}
			]}`,
		},
		{
			"flat record with namespace",
			`{"type":"record","name":"R","fields":[
				{"name":"Inner","type":"record","namespace":"com.example","fields":[
					{"name":"x","type":"int"}
				]}
			]}`,
		},

		// Mixed flat and nested in the same record.
		{
			"mixed flat and nested",
			`{"type":"record","name":"R","fields":[
				{"name":"E","type":"enum","symbols":["X","Y"]},
				{"name":"A","type":{"type":"array","items":"string"}},
				{"name":"M","type":"map","values":"boolean"},
				{"name":"I","type":"int"}
			]}`,
		},

		// Complex items/values in flat form.
		{
			"flat array with complex items",
			`{"type":"record","name":"R","fields":[
				{"name":"A","type":"array","items":{"type":"record","name":"Item","fields":[{"name":"x","type":"int"}]}}
			]}`,
		},
		{
			"flat map with complex values",
			`{"type":"record","name":"R","fields":[
				{"name":"M","type":"map","values":{"type":"array","items":"string"}}
			]}`,
		},
		{
			"flat array with union items",
			`{"type":"record","name":"R","fields":[
				{"name":"A","type":"array","items":["null","string"]}
			]}`,
		},

		// Nested flat: a flat record whose fields are also flat.
		{
			"nested flat records",
			`{"type":"record","name":"Outer","fields":[
				{"name":"Inner","type":"record","fields":[
					{"name":"E","type":"enum","symbols":["A","B"]},
					{"name":"A","type":"array","items":"int"}
				]}
			]}`,
		},

		// Extra/unknown properties should survive the lift.
		{
			"flat enum with doc",
			`{"type":"record","name":"R","fields":[
				{"name":"E","type":"enum","symbols":["A"],"doc":"an enum"}
			]}`,
		},

		// Flat enum with many symbols.
		{
			"flat enum single symbol",
			`{"type":"record","name":"R","fields":[
				{"name":"E","type":"enum","symbols":["ONLY"]}
			]}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustParse(t, tt.schema)
			if s == nil {
				t.Fatal("nil schema")
			}
		})
	}
}

func TestParseFlatFieldFormatErrors(t *testing.T) {
	// Flat format fields that are still invalid should produce errors.
	tests := []struct {
		name   string
		schema string
	}{
		{
			"flat enum no symbols",
			`{"type":"record","name":"R","fields":[
				{"name":"E","type":"enum"}
			]}`,
		},
		// NOTE: flat-form `{"name":"E","type":"enum","symbols":[]}` is no
		// longer an error: an empty symbols ARRAY is a legal enum (the
		// flat-form lift composes with TestRegression_EmptyEnumParses'
		// acceptance). A flat enum MISSING symbols stays an error above.
		{
			"flat array no items",
			`{"type":"record","name":"R","fields":[
				{"name":"A","type":"array"}
			]}`,
		},
		{
			"flat map no values",
			`{"type":"record","name":"R","fields":[
				{"name":"M","type":"map"}
			]}`,
		},
		{
			"flat fixed no size",
			`{"type":"record","name":"R","fields":[
				{"name":"F","type":"fixed"}
			]}`,
		},
		{
			"flat record no fields",
			`{"type":"record","name":"R","fields":[
				{"name":"Inner","type":"record"}
			]}`,
		},
		{
			"flat enum bad default",
			`{"type":"record","name":"R","fields":[
				{"name":"E","type":"enum","symbols":["A","B"],"default":"Z"}
			]}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.schema)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestParseFlatFieldNotTriggeredForPrimitives(t *testing.T) {
	// "type":"int" with no complex keys must NOT trigger flat lifting.
	// This is the normal case; ensure we don't break it.
	schema := `{"type":"record","name":"R","fields":[
		{"name":"x","type":"int"},
		{"name":"y","type":"string"},
		{"name":"z","type":"double"}
	]}`
	s := mustParse(t, schema)
	if s == nil {
		t.Fatal("nil schema")
	}
}

func TestParseFlatFieldRoundTrip(t *testing.T) {
	// Verify that the flat and nested forms produce equivalent schemas
	// by encoding and decoding the same data.
	flat := `{"type":"record","name":"R","fields":[
		{"name":"E","type":"enum","symbols":["MOO","WOOF"]},
		{"name":"A","type":"array","items":"boolean"},
		{"name":"M","type":"map","values":"long"}
	]}`
	nested := `{"type":"record","name":"R","fields":[
		{"name":"E","type":{"type":"enum","name":"E","symbols":["MOO","WOOF"]}},
		{"name":"A","type":{"type":"array","items":"boolean"}},
		{"name":"M","type":{"type":"map","values":"long"}}
	]}`

	sf, err := Parse(flat)
	if err != nil {
		t.Fatalf("flat Parse: %v", err)
	}
	sn, err := Parse(nested)
	if err != nil {
		t.Fatalf("nested Parse: %v", err)
	}

	// Encode with the flat schema, decode with the nested schema (and vice versa).
	datum := map[string]any{
		"E": "MOO",
		"A": []any{true, false},
		"M": map[string]any{"k": int64(42)},
	}
	buf, err := sf.Encode(datum)
	if err != nil {
		t.Fatalf("encode flat: %v", err)
	}
	var out any
	if _, err := sn.Decode(buf, &out); err != nil {
		t.Fatalf("decode nested: %v", err)
	}
	buf2, err := sn.Encode(datum)
	if err != nil {
		t.Fatalf("encode nested: %v", err)
	}
	if !bytes.Equal(buf, buf2) {
		t.Errorf("flat and nested encoded differently:\n  flat:   %x\n  nested: %x", buf, buf2)
	}
}

func TestParseFlatFieldRoundTripAllTypes(t *testing.T) {
	// Round-trip each flat type individually: encode, decode, re-encode.
	tests := []struct {
		name   string
		flat   string
		nested string
		datum  map[string]any
	}{
		{
			"enum",
			`{"type":"record","name":"R","fields":[{"name":"E","type":"enum","symbols":["A","B"]}]}`,
			`{"type":"record","name":"R","fields":[{"name":"E","type":{"type":"enum","name":"E","symbols":["A","B"]}}]}`,
			map[string]any{"E": "B"},
		},
		{
			"array",
			`{"type":"record","name":"R","fields":[{"name":"A","type":"array","items":"string"}]}`,
			`{"type":"record","name":"R","fields":[{"name":"A","type":{"type":"array","items":"string"}}]}`,
			map[string]any{"A": []any{"hello", "world"}},
		},
		{
			"map",
			`{"type":"record","name":"R","fields":[{"name":"M","type":"map","values":"int"}]}`,
			`{"type":"record","name":"R","fields":[{"name":"M","type":{"type":"map","values":"int"}}]}`,
			map[string]any{"M": map[string]any{"k": int32(7)}},
		},
		{
			"fixed",
			`{"type":"record","name":"R","fields":[{"name":"F","type":"fixed","size":4}]}`,
			`{"type":"record","name":"R","fields":[{"name":"F","type":{"type":"fixed","name":"F","size":4}}]}`,
			map[string]any{"F": []byte{1, 2, 3, 4}},
		},
		{
			"record",
			`{"type":"record","name":"R","fields":[{"name":"Sub","type":"record","fields":[{"name":"x","type":"int"}]}]}`,
			`{"type":"record","name":"R","fields":[{"name":"Sub","type":{"type":"record","name":"Sub","fields":[{"name":"x","type":"int"}]}}]}`,
			map[string]any{"Sub": map[string]any{"x": int32(99)}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sf, err := Parse(tt.flat)
			if err != nil {
				t.Fatalf("flat Parse: %v", err)
			}
			sn, err := Parse(tt.nested)
			if err != nil {
				t.Fatalf("nested Parse: %v", err)
			}

			buf, err := sf.Encode(tt.datum)
			if err != nil {
				t.Fatalf("encode flat: %v", err)
			}
			buf2, err := sn.Encode(tt.datum)
			if err != nil {
				t.Fatalf("encode nested: %v", err)
			}
			if !bytes.Equal(buf, buf2) {
				t.Errorf("flat and nested differ:\n  flat:   %x\n  nested: %x", buf, buf2)
			}
		})
	}
}

func TestUnknownPrimitiveErrorString(t *testing.T) {
	e := &unknownPrimitiveError{"foobar"}
	s := e.Error()
	if !strings.Contains(s, "foobar") {
		t.Errorf("error string missing primitive name: %s", s)
	}
}

func TestValidateLogical(t *testing.T) {
	intSize := laxInt(12)
	zeroPrec := 0
	somePrec := 10

	// wantErr=true → validateLogical returns a non-nil error.
	// wantDropped=true → validateLogical returns nil but clears Logical
	// to "" per the spec's "ignore invalid logical type" rule (matches
	// Java's fromSchemaIgnoreInvalid, fastavro's LOGICAL_*.get fallthrough,
	// hamba's parsePrimitiveLogicalType nil-return). Wrong-underlying-
	// type combinations soft-drop; precision/scale invariants on the
	// correct underlying still error (matches fastavro's strict decimal
	// validation; Java's behavior differs but twmb aligns with fastavro
	// for decimal precision/scale — see schema.go's decimal arm comment).
	tests := []struct {
		name        string
		obj         aobject
		wantErr     bool
		wantDropped bool
	}{
		{"no logical", aobject{Type: "int"}, false, false},

		// decimal
		{"decimal ok bytes", aobject{Type: "bytes", Logical: "decimal", Precision: &somePrec}, false, false},
		{"decimal ok fixed", aobject{Type: "fixed", Logical: "decimal", Precision: &somePrec, Size: &intSize}, false, false},
		{"decimal missing precision", aobject{Type: "bytes", Logical: "decimal"}, true, false},
		{"decimal wrong type", aobject{Type: "int", Logical: "decimal", Precision: &somePrec}, false, true},

		// uuid: wrong-type soft-drops (matches Java/fastavro/hamba).
		{"uuid ok", aobject{Type: "string", Logical: "uuid"}, false, false},
		{"uuid wrong type", aobject{Type: "int", Logical: "uuid"}, false, true},
		{"uuid wrong fixed size", aobject{Type: "fixed", Logical: "uuid", Size: ptr(laxInt(12))}, false, true},
		// scale/precision on uuid (correct underlying type): inert
		// metadata — the logical stays applied, the stray key surfaces
		// as a custom property (see TestMatrix_StrayPrecisionScaleParses).
		{"uuid with scale", aobject{Type: "string", Logical: "uuid", Scale: &zeroPrec}, false, false},

		// date / time-millis / time-micros / timestamp-* /
		// local-timestamp-* / big-decimal: wrong-underlying soft-drops.
		{"date ok", aobject{Type: "int", Logical: "date"}, false, false},
		{"date wrong type", aobject{Type: "long", Logical: "date"}, false, true},
		{"time-millis ok", aobject{Type: "int", Logical: "time-millis"}, false, false},
		{"time-millis wrong type", aobject{Type: "long", Logical: "time-millis"}, false, true},
		{"time-micros ok", aobject{Type: "long", Logical: "time-micros"}, false, false},
		{"time-micros wrong type", aobject{Type: "int", Logical: "time-micros"}, false, true},
		{"timestamp-millis ok", aobject{Type: "long", Logical: "timestamp-millis"}, false, false},
		{"timestamp-millis wrong type", aobject{Type: "int", Logical: "timestamp-millis"}, false, true},
		{"timestamp-micros ok", aobject{Type: "long", Logical: "timestamp-micros"}, false, false},
		{"timestamp-micros wrong type", aobject{Type: "int", Logical: "timestamp-micros"}, false, true},
		{"local-timestamp-millis ok", aobject{Type: "long", Logical: "local-timestamp-millis"}, false, false},
		{"local-timestamp-micros ok", aobject{Type: "long", Logical: "local-timestamp-micros"}, false, false},

		// duration: wrong-type AND wrong-size soft-drop (matches Java's
		// Duration.validate throw caught by fromSchemaIgnoreInvalid, plus
		// hamba's (Duration && size == 12) match-or-drop pattern).
		{"duration ok", aobject{Type: "fixed", Logical: "duration", Size: &intSize}, false, false},
		{"duration wrong type", aobject{Type: "int", Logical: "duration"}, false, true},
		{"duration no size", aobject{Type: "fixed", Logical: "duration"}, false, true},
		{"duration wrong size", aobject{Type: "fixed", Logical: "duration", Size: ptr(laxInt(10))}, false, true},

		// unknown logical types are ignored per spec.
		{"unknown logical", aobject{Type: "int", Logical: "foobar"}, false, true},

		// scale/precision on non-decimal (correct underlying): inert
		// metadata, logical stays applied.
		{"date with precision", aobject{Type: "int", Logical: "date", Precision: &somePrec}, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			origLogical := tt.obj.Logical
			err := tt.obj.validateLogical()
			if (err != nil) != tt.wantErr {
				t.Errorf("validateLogical() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				dropped := origLogical != "" && tt.obj.Logical == ""
				if dropped != tt.wantDropped {
					t.Errorf("dropped = %v (Logical %q -> %q), wantDropped %v", dropped, origLogical, tt.obj.Logical, tt.wantDropped)
				}
			}
		})
	}
}

func TestBuildUnionErrors(t *testing.T) {
	t.Run("duplicate type", func(t *testing.T) {
		_, err := Parse(`["int","int"]`)
		if err == nil {
			t.Fatal("expected error for duplicate union type")
		}
	})

	t.Run("union in union", func(t *testing.T) {
		// Can't test this through JSON directly since `[["null","int"],"string"]`
		// won't parse the inner union as a separate union element in the same way.
		// But we can test via unionTypeName.
		s := &aschema{union: []aschema{{primitive: "null"}}}
		_, _, err := s.unionTypeName()
		if err == nil {
			t.Fatal("expected error for union containing union")
		}
	})

	t.Run("duplicate named type", func(t *testing.T) {
		// Two records with the same name in a union is a duplicate.
		_, err := Parse(`[
			{"type":"record","name":"a","fields":[{"name":"x","type":"int"}]},
			{"type":"record","name":"a","fields":[{"name":"y","type":"int"}]}
		]`)
		if err == nil {
			t.Fatal("expected error for duplicate named types")
		}
	})

	t.Run("two records different names", func(t *testing.T) {
		// Two records with different names in union is OK.
		_, err := Parse(`[
			{"type":"record","name":"a","fields":[{"name":"x","type":"int"}]},
			{"type":"record","name":"b","fields":[{"name":"x","type":"int"}]}
		]`)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})
}

func TestBuildComplexErrors(t *testing.T) {
	t.Run("name with namespace only", func(t *testing.T) {
		// Name like "com.example" is treated as a fullname.
		_, err := Parse(`{"type":"record","name":"com.example","fields":[{"name":"a","type":"int"}]}`)
		if err != nil {
			t.Fatalf("expected no error for dotted name, got %v", err)
		}
	})

	t.Run("name and namespace", func(t *testing.T) {
		_, err := Parse(`{"type":"record","name":"r","namespace":"com.example","fields":[{"name":"a","type":"int"}]}`)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})

	t.Run("parent namespace inheritance", func(t *testing.T) {
		// Parent has a namespace, child inherits it.
		_, err := Parse(`{
			"type":"record","name":"parent","namespace":"com.example","fields":[
				{"name":"child","type":{"type":"record","name":"child","fields":[
					{"name":"x","type":"int"}
				]}}
			]
		}`)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})

	t.Run("named non-record", func(t *testing.T) {
		_, err := Parse(`{"type":"array","name":"x","items":"int"}`)
		if err == nil {
			t.Fatal("expected error for named array")
		}
	})

	t.Run("namespace on non-record", func(t *testing.T) {
		// A stray namespace on an unnamed kind is inert metadata (never
		// scoping, stripped from the canonical form), matching the
		// primitive type-object posture and both references; the full
		// placement matrix is TestMatrix_AttributePlacementCensus.
		s, err := Parse(`{"type":"array","namespace":"com","items":"int"}`)
		if err != nil {
			t.Fatalf("stray namespace on an array must parse as inert metadata: %v", err)
		}
		if got, want := string(s.Canonical()), `{"type":"array","items":"int"}`; got != want {
			t.Errorf("canonical form kept the inert namespace: %s", got)
		}
	})

	t.Run("record with extra fields", func(t *testing.T) {
		_, err := Parse(`{"type":"record","name":"r","fields":[{"name":"a","type":"int"}],"symbols":["x"]}`)
		if err == nil {
			t.Fatal("expected error for record with symbols")
		}
	})

	t.Run("record with items", func(t *testing.T) {
		_, err := Parse(`{"type":"record","name":"r","fields":[{"name":"a","type":"int"}],"items":"int"}`)
		if err == nil {
			t.Fatal("expected error for record with items")
		}
	})

	t.Run("record with values", func(t *testing.T) {
		_, err := Parse(`{"type":"record","name":"r","fields":[{"name":"a","type":"int"}],"values":"int"}`)
		if err == nil {
			t.Fatal("expected error for record with values")
		}
	})

	t.Run("record with size", func(t *testing.T) {
		_, err := Parse(`{"type":"record","name":"r","fields":[{"name":"a","type":"int"}],"size":4}`)
		if err == nil {
			t.Fatal("expected error for record with size")
		}
	})

	t.Run("invalid record field type", func(t *testing.T) {
		_, err := Parse(`{"type":"record","name":"r","fields":[{"name":"a","type":"unknown"}]}`)
		if err == nil {
			t.Fatal("expected error for invalid field type")
		}
	})

	t.Run("enum with extra fields", func(t *testing.T) {
		_, err := Parse(`{"type":"enum","name":"e","symbols":["a"],"fields":[{"name":"x","type":"int"}]}`)
		if err == nil {
			t.Fatal("expected error for enum with fields")
		}
	})

	t.Run("enum with items", func(t *testing.T) {
		_, err := Parse(`{"type":"enum","name":"e","symbols":["a"],"items":"int"}`)
		if err == nil {
			t.Fatal("expected error for enum with items")
		}
	})

	t.Run("enum with values", func(t *testing.T) {
		_, err := Parse(`{"type":"enum","name":"e","symbols":["a"],"values":"int"}`)
		if err == nil {
			t.Fatal("expected error for enum with values")
		}
	})

	t.Run("enum with size", func(t *testing.T) {
		_, err := Parse(`{"type":"enum","name":"e","symbols":["a"],"size":4}`)
		if err == nil {
			t.Fatal("expected error for enum with size")
		}
	})

	t.Run("array with extra fields", func(t *testing.T) {
		_, err := Parse(`{"type":"array","items":"int","symbols":["a"]}`)
		if err == nil {
			t.Fatal("expected error for array with symbols")
		}
	})

	t.Run("array missing items", func(t *testing.T) {
		_, err := Parse(`{"type":"array"}`)
		if err == nil {
			t.Fatal("expected error for array missing items")
		}
	})

	t.Run("array invalid items", func(t *testing.T) {
		_, err := Parse(`{"type":"array","items":"unknown"}`)
		if err == nil {
			t.Fatal("expected error for array with invalid items")
		}
	})

	t.Run("map with extra fields", func(t *testing.T) {
		_, err := Parse(`{"type":"map","values":"int","symbols":["a"]}`)
		if err == nil {
			t.Fatal("expected error for map with symbols")
		}
	})

	t.Run("map missing values", func(t *testing.T) {
		_, err := Parse(`{"type":"map"}`)
		if err == nil {
			t.Fatal("expected error for map missing values")
		}
	})

	t.Run("map invalid values", func(t *testing.T) {
		_, err := Parse(`{"type":"map","values":"unknown"}`)
		if err == nil {
			t.Fatal("expected error for map with invalid values")
		}
	})

	t.Run("fixed with extra fields", func(t *testing.T) {
		_, err := Parse(`{"type":"fixed","name":"f","size":4,"symbols":["a"]}`)
		if err == nil {
			t.Fatal("expected error for fixed with symbols")
		}
	})

	t.Run("fixed missing size", func(t *testing.T) {
		_, err := Parse(`{"type":"fixed","name":"f"}`)
		if err == nil {
			t.Fatal("expected error for fixed missing size")
		}
	})

	t.Run("fixed negative size", func(t *testing.T) {
		_, err := Parse(`{"type":"fixed","name":"f","size":-1}`)
		if err == nil {
			t.Fatal("expected error for negative fixed size")
		}
	})

	t.Run("primitive as object", func(t *testing.T) {
		// A primitive type name in object form is treated as a primitive.
		s, err := Parse(`{"type":"int"}`)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		dst, err := s.AppendEncode(nil, ptr(int32(42)))
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got int32
		_, err = s.Decode(dst, &got)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got != 42 {
			t.Errorf("got %d, want 42", got)
		}
	})
}

func TestFinalizeForwardRef(t *testing.T) {
	// Union references a type defined later in the schema.
	// This exercises finalize() resolving forward references.
	_, err := Parse(`{
		"type":"record","name":"outer","fields":[
			{"name":"u","type":["null","inner"]},
			{"name":"inner","type":{"type":"record","name":"inner","fields":[
				{"name":"x","type":"int"}
			]}}
		]
	}`)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestFinalizeUnknownRef(t *testing.T) {
	// Union references a type that is never defined → finalize error.
	_, err := Parse(`["null","neverDefined"]`)
	if err == nil {
		t.Fatal("expected error for unknown type reference in union")
	}
}

func TestUnionTypeName(t *testing.T) {
	// Array type in union returns ("array", "", nil).
	s := &aschema{object: &aobject{Type: "array"}}
	typ, name, err := s.unionTypeName()
	if err != nil {
		t.Fatal(err)
	}
	if typ != "array" || name != "" {
		t.Errorf("got (%s, %s)", typ, name)
	}

	// Map type in union returns ("map", "", nil).
	s = &aschema{object: &aobject{Type: "map"}}
	typ, name, err = s.unionTypeName()
	if err != nil {
		t.Fatal(err)
	}
	if typ != "map" || name != "" {
		t.Errorf("got (%s, %s)", typ, name)
	}
}

func TestBuildUnionInUnion(t *testing.T) {
	// A union directly containing another union is invalid.
	_, err := Parse(`["null", ["int","string"]]`)
	if err == nil {
		t.Fatal("expected error for union-in-union")
	}
}

func TestBuildComplexUnknownLogicalIgnored(t *testing.T) {
	// Per Avro spec, unknown logical types are ignored and the underlying type is used.
	s, err := Parse(`{"type":"int","logicalType":"unknown_logical"}`)
	if err != nil {
		t.Fatalf("expected unknown logical type to be ignored, got error: %v", err)
	}
	dst, err := s.AppendEncode(nil, ptr(int32(42)))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got int32
	mustDecode(t, s, dst, &got)
	if got != 42 {
		t.Errorf("got %d, want 42", got)
	}
}

func TestBuildComplexValidateLogicalSoftDrop(t *testing.T) {
	// Known logical type on wrong underlying type soft-drops the
	// logical and parses as bare underlying, matching Java/fastavro/
	// hamba and the spec's "ignore invalid logical type" rule.
	if _, err := Parse(`{"type":"string","logicalType":"date"}`); err != nil {
		t.Fatalf("expected soft-drop accept for date-on-string, got: %v", err)
	}
}

func TestMultiDotNamespace(t *testing.T) {
	// Namespace with multiple dot-segments should be valid.
	_, err := Parse(`{"type":"record","name":"r","namespace":"com.example.foo","fields":[{"name":"a","type":"int"}]}`)
	if err != nil {
		t.Fatalf("expected no error for multi-dot namespace, got %v", err)
	}
}

func TestMultiDotFullname(t *testing.T) {
	// Fullname with multiple dot-segments should be valid.
	_, err := Parse(`{"type":"record","name":"com.example.foo.Bar","fields":[{"name":"a","type":"int"}]}`)
	if err != nil {
		t.Fatalf("expected no error for multi-dot fullname, got %v", err)
	}
}

func TestDeepNamespaceInheritance(t *testing.T) {
	// Parent is "com.example.Parent", child should inherit "com.example" namespace.
	_, err := Parse(`{
		"type":"record","name":"Parent","namespace":"com.example","fields":[
			{"name":"child","type":{"type":"record","name":"Child","fields":[
				{"name":"x","type":"int"}
			]}}
		]
	}`)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestDuplicateRecordFieldName(t *testing.T) {
	_, err := Parse(`{"type":"record","name":"r","fields":[
		{"name":"a","type":"int"},
		{"name":"a","type":"string"}
	]}`)
	if err == nil {
		t.Fatal("expected error for duplicate field name")
	}
}

func TestDuplicateEnumSymbol(t *testing.T) {
	_, err := Parse(`{"type":"enum","name":"e","symbols":["a","b","a"]}`)
	if err == nil {
		t.Fatal("expected error for duplicate enum symbol")
	}
}

func TestBuildNilSchema(t *testing.T) {
	b := &builder{
		named: make(map[string]*namedType),
	}
	err := b.build("", nil)
	if err == nil {
		t.Fatal("expected error for nil schema")
	}
}

func TestBuildEmptySchema(t *testing.T) {
	b := &builder{
		named: make(map[string]*namedType),
	}
	err := b.build("", &aschema{})
	if err == nil {
		t.Fatal("expected error for empty schema")
	}
}

func TestNameValidation(t *testing.T) {
	t.Run("dashes rejected", func(t *testing.T) {
		// Per spec: names must match [A-Za-z_][A-Za-z0-9_]*
		_, err := Parse(`{"type":"record","name":"my-record","fields":[{"name":"my-field","type":"int"}]}`)
		if err == nil {
			t.Fatal("expected error for dashed record name")
		}
	})

	t.Run("fullname detection", func(t *testing.T) {
		// Fullnames (dot-separated) must be detected so namespace
		// handling works correctly.
		s := mustParse(t, `{"type":"record","name":"com.example.MyRecord","fields":[{"name":"x","type":"int"}]}`)
		canon := string(s.Canonical())
		if canon == "" {
			t.Fatal("expected non-empty canonical form")
		}
	})
}

func TestWithLaxNames(t *testing.T) {
	// Dashes are rejected by default.
	_, err := Parse(`{"type":"record","name":"my-record","fields":[{"name":"x","type":"int"}]}`)
	if err == nil {
		t.Fatal("expected error for dashed name in strict mode")
	}

	// WithLaxNames(nil) allows dashes.
	s, err := Parse(`{"type":"record","name":"my-record","fields":[{"name":"my-field","type":"int"}]}`, WithLaxNames(nil))
	if err != nil {
		t.Fatalf("lax: %v", err)
	}
	if s == nil {
		t.Fatal("expected non-nil schema")
	}

	// WithLaxNames(nil) still rejects empty names.
	_, err = Parse(`{"type":"record","name":"","fields":[{"name":"x","type":"int"}]}`, WithLaxNames(nil))
	if err == nil {
		t.Fatal("expected error for empty name in lax mode")
	}

	// WithLaxNames with custom validator.
	noDigitStart := func(s string) error {
		if s == "" {
			return errors.New("empty")
		}
		if s[0] >= '0' && s[0] <= '9' {
			return errors.New("starts with digit")
		}
		return nil
	}
	_, err = Parse(`{"type":"record","name":"my-record","fields":[{"name":"x","type":"int"}]}`, WithLaxNames(noDigitStart))
	if err != nil {
		t.Fatalf("custom validator: %v", err)
	}
	_, err = Parse(`{"type":"record","name":"0bad","fields":[{"name":"x","type":"int"}]}`, WithLaxNames(noDigitStart))
	if err == nil {
		t.Fatal("expected error for digit-start name with custom validator")
	}

	// Default (no option) is strict: dashed names are rejected.
	_, err = Parse(`{"type":"record","name":"my-record","fields":[{"name":"x","type":"int"}]}`)
	if err == nil {
		t.Fatal("expected error for dashed name in strict mode")
	}
}

func TestNamespaceFallback(t *testing.T) {
	// A record in a namespace can reference another type by unqualified name.
	schema := `{
		"type":"record","name":"Parent","namespace":"com.example","fields":[
			{"name":"child","type":{"type":"record","name":"Child","fields":[
				{"name":"x","type":"int"}
			]}},
			{"name":"ref","type":"Child"}
		]
	}`
	_, err := Parse(schema)
	if err != nil {
		t.Fatalf("expected namespace fallback to resolve unqualified ref, got %v", err)
	}
}

func TestForwardReferenceInRecord(t *testing.T) {
	// A record field references a type defined later in the same record.
	schema := `{
		"type":"record","name":"outer","fields":[
			{"name":"ref","type":"inner"},
			{"name":"inner_def","type":{"type":"record","name":"inner","fields":[
				{"name":"x","type":"int"}
			]}}
		]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatalf("expected forward reference to work, got %v", err)
	}
	// Verify round-trip works.
	type Outer struct {
		Ref      Inner `avro:"ref"`
		InnerDef Inner `avro:"inner_def"`
	}
	input := Outer{Ref: Inner{X: 42}, InnerDef: Inner{X: 99}}
	dst, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var output Outer
	rem, err := s.Decode(dst, &output)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("leftover bytes: %d", len(rem))
	}
	if output.Ref.X != 42 || output.InnerDef.X != 99 {
		t.Fatalf("unexpected output: %+v", output)
	}
}

func TestEmptyNamespace(t *testing.T) {
	// Explicit empty namespace clears inherited namespace.
	schema := `{
		"type":"record","name":"parent","namespace":"com.example","fields":[
			{"name":"child","type":{"type":"record","name":"child","namespace":"","fields":[
				{"name":"x","type":"int"}
			]}}
		]
	}`
	s := mustParse(t, schema)
	canon := string(s.Canonical())
	// The child should not have com.example prefix because namespace was
	// explicitly cleared.
	if strings.Contains(canon, "com.example.child") {
		t.Fatalf("expected empty namespace to clear parent, got %s", canon)
	}
}

func TestSchemaValidationErrors(t *testing.T) {
	tests := []struct {
		name   string
		schema string
	}{
		// (type/field aliases accept any string per Avro §Aliases — see
		// TestMatrix_AliasAcceptsAnyString — so they are NOT in this
		// expected-error table; names and symbols stay strictly validated.)
		{"empty field name", `{"type":"record","name":"R","fields":[{"name":"","type":"int"}]}`},
		{"invalid field name", `{"type":"record","name":"R","fields":[{"name":"bad-field!","type":"int"}]}`},
		// (an EMPTY symbols array is legal — TestRegression_EmptyEnumParses;
		// a missing symbols attribute still errors, covered elsewhere.)
		{"invalid enum symbol", `{"type":"enum","name":"E","symbols":["bad-sym!"]}`},
		{"enum default not in symbols", `{"type":"enum","name":"E","symbols":["A","B"],"default":"C"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.schema)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestDefaultValidationErrors(t *testing.T) {
	tests := []struct {
		name   string
		schema string
	}{
		{
			"record field invalid default",
			`{"type":"record","name":"R","fields":[
				{"name":"inner","type":{"type":"record","name":"I","fields":[
					{"name":"x","type":"int"}
				]},"default":{"x":"not_a_number"}}
			]}`,
		},
		{
			"record default missing field ok",
			`{"type":"record","name":"R","fields":[
				{"name":"inner","type":{"type":"record","name":"I","fields":[
					{"name":"x","type":"int","default":0},
					{"name":"y","type":"int","default":0}
				]},"default":{"x":1}}
			]}`,
		},
		{
			"null default for record type",
			`{"type":"record","name":"R","fields":[
				{"name":"inner","type":{"type":"record","name":"I","fields":[
					{"name":"x","type":"int"}
				]},"default":null}
			]}`,
		},
		{
			"record default omits field without own default",
			`{"type":"record","name":"R","fields":[
				{"name":"inner","type":{"type":"record","name":"I","fields":[
					{"name":"x","type":"int","default":0},
					{"name":"y","type":"int"}
				]},"default":{"x":1}}
			]}`,
		},
		{
			"enum field default not in symbols",
			`{"type":"record","name":"R","fields":[
				{"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]},"default":"C"}
			]}`,
		},
		{
			"array element invalid default",
			`{"type":"record","name":"R","fields":[
				{"name":"a","type":{"type":"array","items":"int"},"default":["not_a_number"]}
			]}`,
		},
		{
			"map value invalid default",
			`{"type":"record","name":"R","fields":[
				{"name":"m","type":{"type":"map","values":"int"},"default":{"k":"not_a_number"}}
			]}`,
		},
		{
			"fixed default wrong length",
			`{"type":"record","name":"R","fields":[
				{"name":"f","type":{"type":"fixed","name":"F","size":4},"default":"ab"}
			]}`,
		},
		{
			"bytes default code point above 255",
			`{"type":"record","name":"R","fields":[
				{"name":"b","type":"bytes","default":"\u0100"}
			]}`,
		},
		{
			"fixed default code point above 255",
			`{"type":"record","name":"R","fields":[
				{"name":"f","type":{"type":"fixed","name":"F","size":1},"default":"\u0100"}
			]}`,
		},
		{
			"forward-ref enum default invalid symbol",
			`{"type":"record","name":"outer","fields":[
				{"name":"color","type":"Color","default":"INVALID"},
				{"name":"dummy","type":{"type":"enum","name":"Color","symbols":["RED","GREEN"]}}
			]}`,
		},
		{
			"forward-ref fixed default code point above 255",
			`{"type":"record","name":"outer","fields":[
				{"name":"id","type":"F","default":"\u0100"},
				{"name":"dummy","type":{"type":"fixed","name":"F","size":1}}
			]}`,
		},
		{
			"int default not whole",
			`{"type":"record","name":"R","fields":[
				{"name":"x","type":"int","default":1.5}
			]}`,
		},
		{
			"int default out of range",
			`{"type":"record","name":"R","fields":[
				{"name":"x","type":"int","default":3000000000}
			]}`,
		},
		{
			"long default not number",
			`{"type":"record","name":"R","fields":[
				{"name":"x","type":"long","default":"foo"}
			]}`,
		},
		{
			"long default not whole",
			`{"type":"record","name":"R","fields":[
				{"name":"x","type":"long","default":1.5}
			]}`,
		},
		{
			"long default out of range",
			`{"type":"record","name":"R","fields":[
				{"name":"x","type":"long","default":1e19}
			]}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.schema)
			if tt.name == "record default missing field ok" {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

// TestFieldLevelLogicalType_RoundTrip exercises the Java/JDBC Avro idiom where
// the `logicalType` annotation (and, for decimal, `precision`/`scale`) sits as a
// sibling of `type` on the field object rather than nested inside the type
// definition. AVRO-2015 and AVRO-3014 document this as "a common error" users
// make; the Java reference warns (Schema.java:1874) but does not lift. The form
// is widely emitted by hand-written .avsc files, older Java tooling, and
// tutorial code. The on-wire encoding is identical between the two, so lifting
// is a strict superset of the spec-blessed behavior.
//
// Each case encodes a strongly-typed Go value through the flat-form schema and
// decodes back. Before the lift, encoding a time.Time produced "cannot use <Go
// type> with Avro type long/int/string" because the parser dropped the
// field-level annotation and built a plain-primitive schema.
func TestFieldLevelLogicalType_RoundTrip(t *testing.T) {
	// Each case asserts a schema parses successfully and that an encode/
	// decode round-trip via the schema produces the expected Go-side type
	// (verified by decoding into a *any and inspecting the result).
	cases := []struct {
		name   string
		schema string
	}{
		// Primitive type with field-level logicalType. Without the lift
		// these would have parsed as plain long / int / string and the
		// decoder would have produced int64 / int32 / string rather
		// than the logical Go types.
		{
			"primitive timestamp-millis",
			recTimestampMillisSchema,
		},
		{
			"primitive timestamp-micros",
			`{"type":"record","name":"R","fields":[
				{"name":"ts","type":"long","logicalType":"timestamp-micros"}
			]}`,
		},
		{
			"primitive local-timestamp-millis",
			`{"type":"record","name":"R","fields":[
				{"name":"ts","type":"long","logicalType":"local-timestamp-millis"}
			]}`,
		},
		{
			"primitive date",
			`{"type":"record","name":"R","fields":[
				{"name":"d","type":"int","logicalType":"date"}
			]}`,
		},
		{
			"primitive time-millis",
			`{"type":"record","name":"R","fields":[
				{"name":"t","type":"int","logicalType":"time-millis"}
			]}`,
		},
		{
			"primitive time-micros",
			`{"type":"record","name":"R","fields":[
				{"name":"t","type":"long","logicalType":"time-micros"}
			]}`,
		},
		{
			"primitive uuid",
			`{"type":"record","name":"R","fields":[
				{"name":"u","type":"string","logicalType":"uuid"}
			]}`,
		},
		{
			"primitive decimal with sibling precision and scale",
			`{"type":"record","name":"R","fields":[
				{"name":"amt","type":"bytes","logicalType":"decimal","precision":9,"scale":2}
			]}`,
		},

		// Nullable union with field-level logicalType — the shape most
		// commonly emitted by Debezium-style sources.
		{
			"union timestamp-millis",
			`{"type":"record","name":"R","fields":[
				{"name":"ts","type":["null","long"],"logicalType":"timestamp-millis"}
			]}`,
		},
		{
			"union date",
			`{"type":"record","name":"R","fields":[
				{"name":"d","type":["null","int"],"logicalType":"date"}
			]}`,
		},
		{
			"union uuid",
			`{"type":"record","name":"R","fields":[
				{"name":"u","type":["null","string"],"logicalType":"uuid"}
			]}`,
		},
		{
			"union decimal with sibling precision and scale",
			`{"type":"record","name":"R","fields":[
				{"name":"amt","type":["null","bytes"],"logicalType":"decimal","precision":18,"scale":4}
			]}`,
		},

		// Long-first union order — both branch orderings must work.
		{
			"union timestamp-millis with long first",
			`{"type":"record","name":"R","fields":[
				{"name":"ts","type":["long","null"],"logicalType":"timestamp-millis"}
			]}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := mustParse(t, tc.schema)

			// The first record field's effective logicalType must match
			// the field-level annotation, regardless of whether the
			// type is a primitive or a nullable union — the lift puts
			// the annotation in the spec-blessed location either way.
			if got := effectiveLogicalType(firstFieldNode(s)); got == "" {
				t.Fatalf("expected non-empty effective logicalType after lift, got empty")
			}
		})
	}
}

// TestFieldLevelLogicalType_RoundTripValue exercises the actual decoder
// across every time-typed logical type whose base primitive becomes a
// time.Time when decoded. Before the lift, encoding a time.Time against
// a flat-form schema errored with "cannot use time.Time with Avro type
// long" because the parser dropped the field-level annotation. With the
// lift, the schema recognises the logical type and the round-trip
// succeeds at the full declared precision.
func TestFieldLevelLogicalType_RoundTripValue(t *testing.T) {
	type Row struct {
		TS time.Time `avro:"ts"`
	}

	// Picks a concrete instant for each unit. timestamp-millis truncates
	// sub-millisecond precision; timestamp-micros preserves microseconds;
	// timestamp-nanos preserves nanoseconds. Test inputs are chosen so
	// that round-trip equality is non-trivial — a parser that quietly
	// fell back to long would lose the time.Time wrapping and the
	// Encode call would error.
	const (
		baseMillis = int64(1_700_000_000_000)
		baseMicros = int64(1_700_000_000_123_456)
		baseNanos  = int64(1_700_000_000_123_456_789)
	)

	cases := []struct {
		name   string
		schema string
		want   time.Time
	}{
		{
			"primitive timestamp-millis",
			recTimestampMillisSchema,
			time.UnixMilli(baseMillis).UTC(),
		},
		{
			"union timestamp-millis",
			`{"type":"record","name":"R","fields":[
				{"name":"ts","type":["null","long"],"logicalType":"timestamp-millis"}
			]}`,
			time.UnixMilli(baseMillis).UTC(),
		},
		{
			"primitive timestamp-micros",
			`{"type":"record","name":"R","fields":[
				{"name":"ts","type":"long","logicalType":"timestamp-micros"}
			]}`,
			time.UnixMicro(baseMicros).UTC(),
		},
		{
			"union timestamp-micros",
			`{"type":"record","name":"R","fields":[
				{"name":"ts","type":["null","long"],"logicalType":"timestamp-micros"}
			]}`,
			time.UnixMicro(baseMicros).UTC(),
		},
		{
			"primitive local-timestamp-millis",
			`{"type":"record","name":"R","fields":[
				{"name":"ts","type":"long","logicalType":"local-timestamp-millis"}
			]}`,
			time.UnixMilli(baseMillis).UTC(),
		},
		{
			"primitive local-timestamp-micros",
			`{"type":"record","name":"R","fields":[
				{"name":"ts","type":"long","logicalType":"local-timestamp-micros"}
			]}`,
			time.UnixMicro(baseMicros).UTC(),
		},
		{
			"primitive timestamp-nanos",
			`{"type":"record","name":"R","fields":[
				{"name":"ts","type":"long","logicalType":"timestamp-nanos"}
			]}`,
			time.Unix(0, baseNanos).UTC(),
		},
		{
			"primitive date",
			`{"type":"record","name":"R","fields":[
				{"name":"ts","type":"int","logicalType":"date"}
			]}`,
			time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := Parse(tc.schema)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			data, err := s.Encode(&Row{TS: tc.want})
			if err != nil {
				t.Fatalf("encode time.Time into flat-form schema: %v", err)
			}
			var got Row
			mustDecode(t, s, data, &got)
			if !got.TS.Equal(tc.want) {
				t.Fatalf("round-trip mismatch: got %v, want %v", got.TS, tc.want)
			}
		})
	}
}

// TestFieldLevelLogicalType_NestedAnnotationWins covers the edge case
// where both a nested and a field-level annotation are present. The
// closer-to-the-type annotation wins so that an explicit author choice
// is never overridden by an outer scope.
func TestFieldLevelLogicalType_NestedAnnotationWins(t *testing.T) {
	s := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"ts","type":{"type":"long","logicalType":"timestamp-micros"},"logicalType":"timestamp-millis"}
	]}`)
	got := effectiveLogicalType(firstFieldNode(s))
	if got != "timestamp-micros" {
		t.Fatalf("nested annotation must win; got %q, want timestamp-micros", got)
	}
}

// firstFieldNode returns the parsed internal schemaNode for the first
// record field of s. Tests use this to introspect what Parse actually
// built without going through the Root() re-parse path.
func firstFieldNode(s *Schema) *schemaNode {
	if s == nil || s.node == nil || len(s.node.fields) == 0 {
		return nil
	}
	return s.node.fields[0].node
}

// effectiveLogicalType returns the logical-type annotation that controls
// decode for a record field's parsed schemaNode. For a non-union type
// it lives directly on the node; for a nullable union it lives on the
// first non-null branch (the spec puts it on the type, not the union
// itself).
func effectiveLogicalType(n *schemaNode) string {
	if n == nil {
		return ""
	}
	if n.logical != "" {
		return n.logical
	}
	for _, branch := range n.branches {
		if branch == nil || branch.kind == "null" {
			continue
		}
		if branch.logical != "" {
			return branch.logical
		}
	}
	return ""
}

// TestFieldLevelLogicalType_DecimalRoundTrip exercises the value-side
// decoder against a flat-form decimal schema. Decimal is the most
// involved case for the lift because it also propagates field-level
// `precision` and `scale` — not just `logicalType`. Without the lift,
// the parser would drop all three and Encode/Decode of a *big.Rat
// would error with "cannot use *big.Rat with Avro type bytes".
func TestFieldLevelLogicalType_DecimalRoundTrip(t *testing.T) {
	type Row struct {
		Amt *big.Rat `avro:"amt"`
	}

	cases := []struct {
		name   string
		schema string
	}{
		{
			"primitive decimal",
			`{"type":"record","name":"R","fields":[
				{"name":"amt","type":"bytes","logicalType":"decimal","precision":9,"scale":2}
			]}`,
		},
		{
			"union decimal (null first)",
			`{"type":"record","name":"R","fields":[
				{"name":"amt","type":["null","bytes"],"logicalType":"decimal","precision":9,"scale":2}
			]}`,
		},
		{
			// Hybrid: precision/scale are nested inside the type object,
			// but logicalType sits as a field-level sibling. Exercises
			// `case f.Type.object != nil` in liftFieldLogicalIntoType
			// — the lift fills in only `Logical` since Precision/Scale
			// are already set on the inner object. A user adding the
			// logicalType "later" to an otherwise-canonical schema
			// (or a tool that emits precision/scale nested but logical
			// at field level) hits this arm.
			"hybrid: precision/scale nested, logicalType at field level",
			`{"type":"record","name":"R","fields":[
				{"name":"amt","type":{"type":"bytes","precision":10,"scale":2},"logicalType":"decimal"}
			]}`,
		},
	}

	want := new(big.Rat).SetFrac64(31415, 100) // 314.15
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := Parse(tc.schema)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			data, err := s.Encode(&Row{Amt: want})
			if err != nil {
				t.Fatalf("encode *big.Rat into flat-form schema: %v", err)
			}
			var got Row
			mustDecode(t, s, data, &got)
			if got.Amt == nil || got.Amt.Cmp(want) != 0 {
				t.Fatalf("round-trip mismatch: got %v, want %v", got.Amt, want)
			}
		})
	}
}

// TestFieldLevelLogicalType_CanonicalDoesNotDuplicate pins down the
// "clear the field-level copies after lift" contract. The canonical form
// must carry the annotation exactly once (in the nested location), never
// at both the field level and inside the type — otherwise downstream
// canonicalisation produces non-spec output and fingerprints would drift.
func TestFieldLevelLogicalType_CanonicalDoesNotDuplicate(t *testing.T) {
	cases := []struct {
		name   string
		schema string
	}{
		{
			"primitive timestamp-millis",
			recTimestampMillisSchema,
		},
		{
			"union timestamp-millis",
			`{"type":"record","name":"R","fields":[
				{"name":"ts","type":["null","long"],"logicalType":"timestamp-millis"}
			]}`,
		},
		{
			"primitive decimal",
			`{"type":"record","name":"R","fields":[
				{"name":"amt","type":"bytes","logicalType":"decimal","precision":9,"scale":2}
			]}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := mustParse(t, tc.schema)
			canon := string(s.Canonical())
			if c := strings.Count(canon, `"logicalType"`); c > 1 {
				t.Fatalf("canonical form must contain logicalType at most once, got %d:\n  %s", c, canon)
			}
			if c := strings.Count(canon, `"precision"`); c > 1 {
				t.Fatalf("canonical form must contain precision at most once, got %d:\n  %s", c, canon)
			}
			if c := strings.Count(canon, `"scale"`); c > 1 {
				t.Fatalf("canonical form must contain scale at most once, got %d:\n  %s", c, canon)
			}
		})
	}
}

// TestFieldLevelLogicalType_FingerprintsMatch pins the drop-in-
// compatibility invariant: flat-form and nested-form schemas must
// produce byte-identical canonical output (and therefore identical
// fingerprints) so that downstream tooling — schema registries,
// schema caches, anything keyed on fingerprint — treats them as the
// same schema.
func TestFieldLevelLogicalType_FingerprintsMatch(t *testing.T) {
	cases := []struct {
		name   string
		flat   string
		nested string
	}{
		{
			"primitive timestamp-millis",
			`{"type":"record","name":"R","fields":[{"name":"ts","type":"long","logicalType":"timestamp-millis"}]}`,
			`{"type":"record","name":"R","fields":[{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}]}`,
		},
		{
			"primitive timestamp-micros",
			`{"type":"record","name":"R","fields":[{"name":"ts","type":"long","logicalType":"timestamp-micros"}]}`,
			`{"type":"record","name":"R","fields":[{"name":"ts","type":{"type":"long","logicalType":"timestamp-micros"}}]}`,
		},
		{
			"primitive date",
			`{"type":"record","name":"R","fields":[{"name":"d","type":"int","logicalType":"date"}]}`,
			`{"type":"record","name":"R","fields":[{"name":"d","type":{"type":"int","logicalType":"date"}}]}`,
		},
		{
			"primitive uuid",
			`{"type":"record","name":"R","fields":[{"name":"u","type":"string","logicalType":"uuid"}]}`,
			`{"type":"record","name":"R","fields":[{"name":"u","type":{"type":"string","logicalType":"uuid"}}]}`,
		},
		{
			"primitive decimal",
			`{"type":"record","name":"R","fields":[{"name":"amt","type":"bytes","logicalType":"decimal","precision":9,"scale":2}]}`,
			`{"type":"record","name":"R","fields":[{"name":"amt","type":{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}}]}`,
		},
		{
			"union timestamp-millis",
			`{"type":"record","name":"R","fields":[{"name":"ts","type":["null","long"],"logicalType":"timestamp-millis"}]}`,
			`{"type":"record","name":"R","fields":[{"name":"ts","type":["null",{"type":"long","logicalType":"timestamp-millis"}]}]}`,
		},
		{
			"union decimal",
			`{"type":"record","name":"R","fields":[{"name":"amt","type":["null","bytes"],"logicalType":"decimal","precision":18,"scale":4}]}`,
			`{"type":"record","name":"R","fields":[{"name":"amt","type":["null",{"type":"bytes","logicalType":"decimal","precision":18,"scale":4}]}]}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			flat, err := Parse(tc.flat)
			if err != nil {
				t.Fatalf("flat parse: %v", err)
			}
			nested, err := Parse(tc.nested)
			if err != nil {
				t.Fatalf("nested parse: %v", err)
			}
			if !bytes.Equal(flat.Canonical(), nested.Canonical()) {
				t.Fatalf("canonical mismatch:\n  flat:   %s\n  nested: %s",
					flat.Canonical(), nested.Canonical())
			}
			flatFP := flat.Fingerprint(sha256.New())
			nestedFP := nested.Fingerprint(sha256.New())
			if !bytes.Equal(flatFP, nestedFP) {
				t.Fatalf("fingerprint mismatch:\n  flat:   %x\n  nested: %x", flatFP, nestedFP)
			}
		})
	}
}

// TestFieldLevelLogicalType_EncodeJSONMatchesNested verifies that the
// JSON encoder produces the same output for the flat and nested forms.
// EncodeJSON is a separate code path from binary Encode and exercises
// the same parsed schema's logical-type wiring; if the lift produced an
// inconsistent parsed schema, the two forms would emit different JSON
// representations of the same value.
func TestFieldLevelLogicalType_EncodeJSONMatchesNested(t *testing.T) {
	type Row struct {
		TS time.Time `avro:"ts"`
	}
	val := &Row{TS: time.UnixMilli(1_700_000_000_000).UTC()}

	flat, err := Parse(recTimestampMillisSchema)
	if err != nil {
		t.Fatal(err)
	}
	nested, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}
	]}`)
	if err != nil {
		t.Fatal(err)
	}

	flatJSON, err := flat.EncodeJSON(val)
	if err != nil {
		t.Fatalf("flat EncodeJSON: %v", err)
	}
	nestedJSON, err := nested.EncodeJSON(val)
	if err != nil {
		t.Fatalf("nested EncodeJSON: %v", err)
	}
	if !bytes.Equal(flatJSON, nestedJSON) {
		t.Fatalf("EncodeJSON differs between flat and nested forms:\n  flat:   %s\n  nested: %s", flatJSON, nestedJSON)
	}
}

// TestFieldLevelLogicalType_MultiNonNullUnion pins down the "first
// non-null branch wins" semantics for unusual unions with more than one
// non-null primitive. The annotation is applied only to the first
// matching branch; subsequent branches are unchanged. Validation downstream
// will catch base-type mismatches, but the lift itself remains predictable.
func TestFieldLevelLogicalType_MultiNonNullUnion(t *testing.T) {
	s := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"v","type":["null","long","string"],"logicalType":"timestamp-millis"}
	]}`)
	node := firstFieldNode(s)
	if node == nil || node.kind != "union" {
		t.Fatalf("expected union, got %+v", node)
	}
	// Branches: [null, long(timestamp-millis), string]
	if len(node.branches) != 3 {
		t.Fatalf("expected 3 branches, got %d", len(node.branches))
	}
	if node.branches[0].kind != "null" {
		t.Fatalf("branch 0: want null, got %q", node.branches[0].kind)
	}
	if node.branches[1].logical != "timestamp-millis" {
		t.Fatalf("branch 1 (first non-null): want timestamp-millis, got %q", node.branches[1].logical)
	}
	if node.branches[2].logical != "" {
		t.Fatalf("branch 2 must not inherit the annotation; got %q", node.branches[2].logical)
	}
}

// TestFieldLevelLogicalType_RealWorldFixtures exercises the lift against schemas
// pulled verbatim from public sources. Apache Avro has tracked this idiom across
// two JIRA tickets going back to 2017, and the Java reference now warns on it
// (Schema.java:1874, commit 72654bf73c) without lifting. The shape is real in
// the wild because users hand-write .avsc files and read the spec's "extra
// attributes permitted as metadata" rule literally.
//
// Each fixture is a verbatim public schema with origin cited so a future reader
// can verify it is real. The test pins both that logicalType reaches the field's
// schemaNode — without the lift it parses as a plain primitive and Encode
// against the logical Go type errors at use — and that the canonical form strips
// it per PCF [STRIP], so flat and nested forms canonicalize identically.
func TestFieldLevelLogicalType_RealWorldFixtures(t *testing.T) {
	cases := []struct {
		name        string
		origin      string
		schema      string
		wantLogical string
	}{
		{
			// Verbatim from OneCricketeer/kafka-connect-sandbox. Note
			// the namespace "io.confluent.example" — this is community
			// Confluent-ecosystem tutorial code. (Confluent's actual
			// production converter, AvroData.java, emits nested form;
			// the flat form here is hand-authored.)
			name:   "OneCricketeer kafka-connect-sandbox record_v3",
			origin: "https://github.com/OneCricketeer/kafka-connect-sandbox/blob/master/replicator/scripts/record_v3.avsc",
			schema: `{"type":"record","name":"Record","namespace":"io.confluent.example","fields":[
				{"name":"time","type":"long","logicalType":"timestamp-millis"},
				{"name":"desc","type":"string"},
				{"name":"counter","type":"int","default":-1},
				{"name":"remaining","type":["null","int"],"default":null}
			]}`,
			wantLogical: "timestamp-millis",
		},
		{
			// The canonical Apache JIRA reproducer. Mirrors
			// TestSchemaWarnings.warnWhenTheLogicalTypeIsOnTheField
			// in the Java reference: a field of type "int" with a
			// sibling "logicalType":"date". Java parses but warns and
			// discards; we lift.
			name:        "AVRO-3014 / AVRO-2015 Apache reproducer",
			origin:      "https://issues.apache.org/jira/browse/AVRO-3014 — mirrored in apache/avro lang/java/avro/src/test/java/org/apache/avro/TestSchemaWarnings.java",
			schema:      `{"type":"record","name":"A","fields":[{"name":"a1","type":"int","logicalType":"date"}]}`,
			wantLogical: "date",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := Parse(tc.schema)
			if err != nil {
				t.Fatalf("parse failed for %s: %v", tc.origin, err)
			}
			if got := effectiveLogicalType(firstFieldNode(s)); got != tc.wantLogical {
				t.Fatalf("origin %s: effectiveLogicalType after lift = %q, want %q",
					tc.origin, got, tc.wantLogical)
			}
			if bytes.Contains(s.Canonical(), []byte("logicalType")) {
				t.Fatalf("origin %s: canonical must strip logicalType per PCF [STRIP], got %s",
					tc.origin, s.Canonical())
			}
		})
	}
}

// TestFieldLevelLogicalType_OneCricketeerRoundTrip pins the full
// Encode/Decode path against the OneCricketeer fixture from above. This
// is the bug PR 38 fixes end-to-end: a Go time.Time round-tripping
// through a flat-form schema. Before the lift, Encode errored with
// "cannot use time.Time with Avro type long". After the lift it succeeds.
func TestFieldLevelLogicalType_OneCricketeerRoundTrip(t *testing.T) {
	s, err := Parse(`{"type":"record","name":"Record","namespace":"io.confluent.example","fields":[
		{"name":"time","type":"long","logicalType":"timestamp-millis"},
		{"name":"desc","type":"string"},
		{"name":"counter","type":"int","default":-1},
		{"name":"remaining","type":["null","int"],"default":null}
	]}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	type record struct {
		Time      time.Time `avro:"time"`
		Desc      string    `avro:"desc"`
		Counter   int32     `avro:"counter"`
		Remaining *int32    `avro:"remaining"`
	}
	want := record{
		Time:    time.UnixMilli(1700000000123).UTC(),
		Desc:    "hello",
		Counter: 7,
	}
	enc, err := s.Encode(&want)
	if err != nil {
		t.Fatalf("encode (would fail without the lift with %q): %v",
			"cannot use time.Time with Avro type long", err)
	}
	var got record
	mustDecode(t, s, enc, &got)
	if !got.Time.Equal(want.Time) {
		t.Fatalf("time: got %v, want %v", got.Time, want.Time)
	}
	if got.Desc != want.Desc || got.Counter != want.Counter {
		t.Fatalf("payload mismatch: got %+v want %+v", got, want)
	}
	if got.Remaining != nil {
		t.Fatalf("remaining: got %v, want nil", *got.Remaining)
	}
}

// TestFieldLevelLogicalType_UnionPreAnnotatedFirstBranch pins the lift's "first
// non-null branch only" semantics: the lift breaks unconditionally after the
// first non-null branch, so if that branch already has its own nested
// annotation, the field-level one is dropped (closer-to-the-type wins) and later
// branches are unaffected. Without the unconditional break the lift would fall
// through past the annotated branch and graft onto a later un-annotated one.
//
// To make the fall-through observable the schema uses different primitive types
// for the two non-null branches, avoiding the duplicate-union-type check:
// ["null", {"type":"int","logicalType":"date"}, "string"] with a field-level
// uuid, expecting [null, int+date, string].
func TestFieldLevelLogicalType_UnionPreAnnotatedFirstBranch(t *testing.T) {
	s := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"v","type":["null",{"type":"int","logicalType":"date"},"string"],"logicalType":"uuid"}
	]}`)
	node := firstFieldNode(s)
	if node == nil || node.kind != "union" {
		t.Fatalf("expected union, got %+v", node)
	}
	if len(node.branches) != 3 {
		t.Fatalf("expected 3 branches, got %d", len(node.branches))
	}
	if node.branches[0].kind != "null" {
		t.Fatalf("branch 0: want null, got %q", node.branches[0].kind)
	}
	// First non-null branch keeps its own nested annotation; the
	// field-level annotation is redundant and dropped.
	if got := node.branches[1].logical; got != "date" {
		t.Fatalf("branch 1: closer-to-type wins, want date, got %q", got)
	}
	// Second non-null branch must remain plain — the lift must not
	// silently graft the field-level annotation onto a later branch
	// after the first non-null branch already absorbed (or dropped) it.
	if got := node.branches[2].logical; got != "" {
		t.Fatalf("branch 2: must NOT inherit field-level annotation, got %q (lift fell through past pre-annotated branch 1)", got)
	}
}

// TestFieldLevelLogicalType_MismatchSoftDrops pins that a flat-form schema whose
// logicalType is structurally incompatible with the primitive type SOFT-DROPS
// the annotation, matching Java's fromSchemaIgnoreInvalid (Schema.java:1979),
// fastavro's LOGICAL_*.get-returns-None fallthrough (_read_py.py:662), hamba's
// parsePrimitiveLogicalType returning nil (schema_parse.go:205-222), and the
// spec's "ignore invalid logical type" rule. Strict rejection here would diverge
// from three references AND the spec text, and a producer that emitted any of
// these — legacy schema, developer mistake, evolution corner case — could not be
// parsed by a twmb consumer. Users wanting strict pre-parse validation add their
// own validator pass.
func TestFieldLevelLogicalType_MismatchSoftDrops(t *testing.T) {
	cases := []string{
		`{"type":"record","name":"R","fields":[{"name":"x","type":"long","logicalType":"date"}]}`,
		`{"type":"record","name":"R","fields":[{"name":"x","type":"int","logicalType":"uuid"}]}`,
		`{"type":"record","name":"R","fields":[{"name":"x","type":"string","logicalType":"timestamp-millis"}]}`,
		`{"type":"record","name":"R","fields":[{"name":"x","type":"int","logicalType":"time-micros"}]}`,
		`{"type":"record","name":"R","fields":[{"name":"x","type":"bytes","logicalType":"date"}]}`,
		// Union variant: same soft-drop through the union-lift path.
		`{"type":"record","name":"R","fields":[{"name":"x","type":["null","long"],"logicalType":"date"}]}`,
	}
	for _, sch := range cases {
		t.Run(sch, func(t *testing.T) {
			if _, err := Parse(sch); err != nil {
				t.Fatalf("expected soft-drop accept, got: %v\n  schema: %s", err, sch)
			}
		})
	}
}

// TestFieldLevelLogicalType_LiftedUnknownLogicalPreserved pins the composition
// of the flat-form lift with the unknownLogical preservation path. A flat-form
// schema whose logicalType has no built-in handler must parse successfully —
// validateLogical clears unrecognized logicals instead of erroring — and must
// preserve the original string in node.unknownLogical, so a later Parse
// registering a CustomType for the same logical can detect the silent-drop
// scenario via rejectCachedRefIfCustomTypeWouldMatch. Pre-lift this test could
// not exist: the flat-form annotation was dropped at JSON-parse time, before
// validateLogical ever saw it.
func TestFieldLevelLogicalType_LiftedUnknownLogicalPreserved(t *testing.T) {
	s := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"ts","type":"long","logicalType":"io.debezium.time.Timestamp"}
	]}`)
	node := firstFieldNode(s)
	if node == nil {
		t.Fatal("no first field node")
	}
	if node.kind != "long" {
		t.Fatalf("kind: want long, got %q", node.kind)
	}
	// validateLogical clears unrecognized logicals from `logical`.
	if node.logical != "" {
		t.Fatalf("node.logical: want \"\" after validateLogical strips unknown, got %q", node.logical)
	}
	// ...but preserves them in unknownLogical for the cache check.
	if node.unknownLogical != "io.debezium.time.Timestamp" {
		t.Fatalf("node.unknownLogical: want %q, got %q", "io.debezium.time.Timestamp", node.unknownLogical)
	}
}

// TestFieldLevelLogicalType_CacheRejectionAcrossFlatForm pins that
// rejectCachedRefIfCustomTypeWouldMatch fires when a Parse references a cached
// named type whose subtree contains a flat-form-lifted unknownLogical and the
// current Parse registers a CustomType that would have matched it. The
// composition matters because the lift happens at JSON-parse time, so by the
// time caching runs the lifted logical is indistinguishable from a nested one,
// and the rejection check correctly consults unknownLogical as a fallback.
// Without the lift this scenario silently succeeds and the user's CustomType
// never fires on cached fields.
func TestFieldLevelLogicalType_CacheRejectionAcrossFlatForm(t *testing.T) {
	var cache SchemaCache
	// Parse 1: cache a record with a flat-form unknown logical. No
	// CustomType registered; the logical is stripped from `logical`
	// but preserved in `unknownLogical` on the field's node.
	_, err := cache.Parse(`{"type":"record","name":"DebeziumRow","fields":[
		{"name":"ts","type":"long","logicalType":"money"}
	]}`)
	if err != nil {
		t.Fatalf("parse 1 (cache seed): %v", err)
	}
	// Parse 2: reference the cached "DebeziumRow" by name AND register
	// a CustomType for "money". The cached node was built without
	// money-CT wiring, so silently reusing it would drop the CT on
	// the cached fields. Expect the rejection error.
	_, err = cache.Parse(`{"type":"record","name":"Outer","fields":[
		{"name":"row","type":"DebeziumRow"}
	]}`, moneyCT)
	if err == nil {
		t.Fatal("parse 2 (cached ref with CT): expected rejection error, got nil")
	}
	if !strings.Contains(err.Error(), "DebeziumRow") || !strings.Contains(err.Error(), "money") {
		t.Fatalf("rejection error should mention both the cached type and the matching logical, got: %v", err)
	}
}

// TestFieldLevelLogicalType_CustomTypeFiresOnLiftedLogical pins that
// applyCustomTypes correctly wires a registered CustomType into the
// lifted schemaNode. The lift completes during afield.UnmarshalJSON,
// so by the time build() walks the resulting nested form to assign
// ser/deser/customEncode functions, the node looks identical to one
// that arrived in canonical nested form — and applyCustomTypes fires
// for both. The round-trip below transparently produces a testMoney
// instead of int64 when the CT is registered against the same flat-
// form schema.
func TestFieldLevelLogicalType_CustomTypeFiresOnLiftedLogical(t *testing.T) {
	s, err := Parse(`{"type":"record","name":"Order","fields":[
		{"name":"id","type":"long"},
		{"name":"price","type":"long","logicalType":"money"}
	]}`, moneyCT)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	type Order struct {
		ID    int64     `avro:"id"`
		Price testMoney `avro:"price"`
	}
	input := Order{ID: 7, Price: testMoney{Cents: 500, Currency: "USD"}}
	data, err := s.Encode(&input)
	if err != nil {
		t.Fatalf("encode (lift composes with CustomType ser): %v", err)
	}
	var got Order
	if _, err := s.Decode(data, &got); err != nil {
		t.Fatalf("decode (lift composes with CustomType deser): %v", err)
	}
	if got.ID != 7 {
		t.Fatalf("id: got %d", got.ID)
	}
	if got.Price.Cents != 500 {
		t.Fatalf("price.Cents: CustomType decoder did not fire on lifted logical; got %d (raw int64?) want 500", got.Price.Cents)
	}
	if got.Price.Currency != "USD" {
		t.Fatalf("price.Currency: CustomType decoder did not fire; got %q want USD", got.Price.Currency)
	}
}

// A leading-dot type alias (".OldName") is Java's explicit null-namespace
// form: Schema.java's Name(".OldName", null) splits at the last dot into
// the empty (null) space and the name "OldName", so the alias matches a
// writer type whose fullname is the bare "OldName" — NOT one qualified
// into the reader's namespace. The spec's Aliases section accepts any
// string as an alias; the dotted-empty-space form is the only way to
// alias a null-namespace name from inside a namespaced type.
func TestRegression_LeadingDotAliasNullNamespace(t *testing.T) {
	reader, err := Parse(`{"type":"record","name":"R","namespace":"new","aliases":[".OldR"],"fields":[
		{"name":"v","type":"int"}]}`)
	if err != nil {
		t.Fatalf("Parse reader with leading-dot alias: %v", err)
	}
	writer := MustParse(`{"type":"record","name":"OldR","fields":[{"name":"v","type":"int"}]}`)

	if err := CheckCompatibility(writer, reader); err != nil {
		t.Fatalf("CheckCompatibility via .OldR alias: %v", err)
	}
	res, err := Resolve(writer, reader)
	if err != nil {
		t.Fatalf("Resolve via .OldR alias: %v", err)
	}
	type rec struct {
		V int32 `avro:"v"`
	}
	wire, err := writer.AppendEncode(nil, rec{V: 7})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got rec
	if _, err := res.Decode(wire, &got); err != nil {
		t.Fatalf("resolved decode: %v", err)
	}
	if got.V != 7 {
		t.Fatalf("v: got %d", got.V)
	}

	// Metadata axis: the alias survives Root() as-written and the
	// re-emitted schema re-parses (the leading-dot form is valid input).
	node := reader.Root()
	if len(node.Aliases) != 1 || node.Aliases[0] != ".OldR" {
		t.Fatalf("Root aliases: %v", node.Aliases)
	}
	if _, err := node.Schema(); err != nil {
		t.Fatalf("Root().Schema() with leading-dot alias: %v", err)
	}

	// Aliases now accept any string (Avro §Aliases; see
	// TestMatrix_AliasAcceptsAnyString). The leading-dot null-namespace
	// escape still strips exactly one leading dot via qualifyAliases, so
	// these dotted forms parse (previously they were name-validated and
	// rejected) — the escape is a qualification rule, not a grammar gate.
	for _, a := range []string{".a.b", ".a..b", "."} {
		if _, err := Parse(`{"type":"record","name":"R","aliases":["` + a + `"],"fields":[{"name":"v","type":"int"}]}`); err != nil {
			t.Errorf("alias %q rejected; any string is a valid alias: %v", a, err)
		}
	}
}

// A bare (dot-free) name reference inside a namespaced scope binds to the
// enclosing-namespace type FIRST, falling back to the null-namespace type
// only when no in-scope type exists. Java's Names.get constructs
// Name(ref, enclosingSpace) and looks that up before the null-space
// fallback (Schema.java); fastavro qualifies a bare ref to the enclosing
// namespace unconditionally (_schema_py.py schema_name). Binding the
// null-namespace type first silently changes the wire contract of every
// field using the reference.
func TestRegression_BareNameRefBindsInScopeBeforeNullNamespace(t *testing.T) {
	type inScope struct {
		Only int32 `avro:"only"`
	}
	type nullNS struct {
		Na int32  `avro:"na"`
		Nb string `avro:"nb"`
	}
	type outer struct {
		A inScope `avro:"a"`
		B nullNS  `avro:"b"`
		R inScope `avro:"r"`
	}
	cases := []struct{ name, schema string }{
		{"backward ref", `{"type":"record","name":"Outer","namespace":"com.x","fields":[
			{"name":"a","type":{"type":"record","name":"Inner","fields":[{"name":"only","type":"int"}]}},
			{"name":"b","type":{"type":"record","name":"Inner","namespace":"","fields":[{"name":"na","type":"int"},{"name":"nb","type":"string"}]}},
			{"name":"r","type":"Inner"}]}`},
		{"forward ref", `{"type":"record","name":"Outer","namespace":"com.x","fields":[
			{"name":"r","type":"Inner"},
			{"name":"a","type":{"type":"record","name":"Inner","fields":[{"name":"only","type":"int"}]}},
			{"name":"b","type":{"type":"record","name":"Inner","namespace":"","fields":[{"name":"na","type":"int"},{"name":"nb","type":"string"}]}}]}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := Parse(c.schema)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			in := outer{A: inScope{1}, B: nullNS{2, "x"}, R: inScope{3}}
			wire, err := s.AppendEncode(nil, in)
			if err != nil {
				t.Fatalf("encode with in-scope shape for r: %v", err)
			}
			var got outer
			mustDecode(t, s, wire, &got)
			if got != in {
				t.Fatalf("round-trip: got %+v want %+v", got, in)
			}
			// Canonical resolves the bare ref to the in-scope fullname:
			// field r binds com.x.Inner, as a fullname reference or as the
			// first-occurrence full body. (This canonical does NOT re-parse —
			// the PCF [FULLNAMES] transform writes the null-namespace type's
			// fullname as bare "Inner", which inside the com.x scope re-reads
			// as inheriting; Java's SchemaNormalization emits the identical
			// ambiguity. PCF is a fingerprint surface, not a round-trip
			// surface.)
			canon := string(s.Canonical())
			if !strings.Contains(canon, `"name":"r","type":"com.x.Inner"`) &&
				!strings.Contains(canon, `"name":"r","type":{"name":"com.x.Inner"`) {
				t.Errorf("canonical r field not bound to com.x.Inner:\n%s", canon)
			}
		})
	}
}

// The metadata API binds bare name-references with the same in-scope-first
// precedence as the wire builder: when an in-scope type and a null-namespace
// type share a short name, a field whose type is the bare reference must
// materialize its Default against the type the WIRE bound — otherwise
// SchemaField.Default's Go type contradicts the wire contract (string enum
// symbol vs codepoint-decoded []byte here).
func TestRegression_MetadataDefaultBindsInScopeNameRef(t *testing.T) {
	const schema = `{"type":"record","name":"Outer","namespace":"com.x","fields":[
		{"name":"a","type":{"type":"enum","name":"Inner","symbols":["A","B"]}},
		{"name":"b","type":{"type":"fixed","name":"Inner","namespace":"","size":1}},
		{"name":"r","type":"Inner","default":"A"}]}`
	s := mustParse(t, schema)
	d := s.Root().Fields[2].Default
	// In-scope binding resolves r to the enum com.x.Inner: the default is
	// the symbol string "A", not the fixed branch's 1-codepoint []byte.
	got, ok := d.(string)
	if !ok || got != "A" {
		t.Fatalf("metadata Default bound to the wrong type: got %T(%v), want string A", d, d)
	}
}

// The metadata name-table must register exactly what the wire builder
// registers: namespaced types under their fullname only, null-namespace
// types under their bare name. Registering every type under its short
// name makes the bare-key binding last-walked-wins — a bare ref at
// null-namespace scope would then materialize its Default against
// whichever colliding type the tree walk saw last, contradicting the
// wire (which deterministically binds the null-namespace type) and
// making the metadata surface reference-order-dependent.
func TestRegression_MetadataDefaultShortNameCollisionWalkOrder(t *testing.T) {
	cases := []struct{ name, schema string }{
		{"namespaced walked last", `{"type":"record","name":"Top","fields":[
			{"name":"a","type":{"type":"enum","name":"Inner","symbols":["A","B"]}},
			{"name":"n","type":{"type":"fixed","name":"Inner","namespace":"ns","size":1}},
			{"name":"r","type":"Inner","default":"A"}]}`},
		{"namespaced walked first", `{"type":"record","name":"Top","fields":[
			{"name":"n","type":{"type":"fixed","name":"Inner","namespace":"ns","size":1}},
			{"name":"a","type":{"type":"enum","name":"Inner","symbols":["A","B"]}},
			{"name":"r","type":"Inner","default":"A"}]}`},
		{"union branch ref", `{"type":"record","name":"Top","fields":[
			{"name":"a","type":{"type":"enum","name":"Inner","symbols":["A","B"]}},
			{"name":"n","type":{"type":"fixed","name":"Inner","namespace":"ns","size":1}},
			{"name":"r","type":["Inner","null"],"default":"A"}]}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := mustParse(t, c.schema)
			var d any
			for _, f := range s.Root().Fields {
				if f.Name == "r" {
					d = f.Default
				}
			}
			// The wire binds the bare ref at null-namespace scope to the
			// null-namespace enum: the default is the symbol string "A".
			got, ok := d.(string)
			if !ok || got != "A" {
				t.Fatalf("metadata Default bound to the wrong type: got %T(%v), want string A", d, d)
			}
		})
	}
}

// A record/enum/fixed may NOT be named after an Avro primitive in the null
// namespace (spec §Names: "Primitive type names ... may not be defined in
// any namespace"); Java rejects it ("Schemas may not be named after
// primitives"). A NAMESPACED type whose short name equals a primitive
// (e.g. a.int) is fine — its fullname is not a primitive name.
func TestMatrix_NamedTypeNotPrimitiveName(t *testing.T) {
	for _, prim := range []string{"int", "long", "string", "bytes", "boolean", "float", "double", "null"} {
		for _, kind := range []string{"enum", "fixed", "record"} {
			var schema string
			switch kind {
			case "enum":
				schema = `{"type":"enum","name":"` + prim + `","symbols":["A"]}`
			case "fixed":
				schema = `{"type":"fixed","name":"` + prim + `","size":4}`
			case "record":
				schema = `{"type":"record","name":"` + prim + `","fields":[]}`
			}
			if _, err := Parse(schema); err == nil {
				t.Errorf("%s named %q (null namespace) accepted; spec/Java reject", kind, prim)
			}
		}
	}
	// Namespaced same short name is allowed (fullname a.int is not a primitive).
	if _, err := Parse(`{"type":"enum","name":"int","namespace":"a","symbols":["A"]}`); err != nil {
		t.Errorf("namespaced a.int enum should be accepted: %v", err)
	}
}

// Avro §Aliases: "any string is accepted as an alias" — so a reader can alias its
// valid name to a writer's illegal/legacy name during evolution. fastavro does no
// alias validation (observed 1.12.2), and Java stores FIELD aliases as raw strings
// (Field.addAlias, Schema.java:674-677), its default parser validating only TYPE
// aliases via NameValidator — Java's own spec divergence. twmb formerly rejected
// aliases that weren't valid Avro names, breaking interop with schemas the spec
// blesses; names themselves stay strictly validated, only aliases relax.
func TestMatrix_AliasAcceptsAnyString(t *testing.T) {
	t.Run("field aliases any string", func(t *testing.T) {
		for _, alias := range []string{"1stField", "com.example.legacy_x", "weird name!", "has.dots", ""} {
			schema := `{"type":"record","name":"R","fields":[{"name":"x","type":"long","aliases":["` + alias + `"]}]}`
			if _, err := Parse(schema); err != nil {
				t.Errorf("field alias %q rejected: %v", alias, err)
			}
		}
	})
	t.Run("type aliases any string", func(t *testing.T) {
		for _, alias := range []string{"1stRecord", "weird!", "a b c"} {
			schema := `{"type":"record","name":"R","aliases":["` + alias + `"],"fields":[{"name":"x","type":"long"}]}`
			if _, err := Parse(schema); err != nil {
				t.Errorf("type alias %q rejected: %v", alias, err)
			}
		}
	})
	// Type NAMES must still be validated strictly (only aliases relax).
	if _, err := Parse(`{"type":"record","name":"1stRecord","fields":[]}`); err == nil {
		t.Error("invalid type NAME should still be rejected")
	}
	// Resolution still matches a reader alias to the writer's field name.
	t.Run("alias resolution still renames", func(t *testing.T) {
		writer := MustParse(`{"type":"record","name":"R","fields":[{"name":"old","type":"long"}]}`)
		reader := MustParse(`{"type":"record","name":"R","fields":[{"name":"new","type":"long","aliases":["old"]}]}`)
		b := mustEncode(t, writer, map[string]any{"old": int64(7)})
		resolved := mustResolve(t, writer, reader)
		var got map[string]any
		mustDecode(t, resolved, b, &got)
		if got["new"] != int64(7) {
			t.Errorf("alias rename failed: got %v", got)
		}
	})
	// Aliases are stripped from the canonical form, so weird ones don't
	// affect fingerprints.
	t.Run("canonical strips aliases", func(t *testing.T) {
		withA := MustParse(`{"type":"record","name":"R","aliases":["1stRecord"],"fields":[{"name":"x","type":"long","aliases":["weird!"]}]}`)
		without := MustParse(`{"type":"record","name":"R","fields":[{"name":"x","type":"long"}]}`)
		if string(withA.Canonical()) != string(without.Canonical()) {
			t.Errorf("aliases leaked into canonical:\n %s\n %s", withA.Canonical(), without.Canonical())
		}
	})
}

// ---------- schema_node_test.go ----------

func TestSchemaNodeRoundTrip(t *testing.T) {
	// Build a SchemaNode, convert to Schema, get Root back, verify.
	node := &SchemaNode{
		Type:      "record",
		Name:      "User",
		Namespace: "com.example",
		Doc:       "A user record",
		Fields: []SchemaField{
			{Name: "name", Type: SchemaNode{Type: "string"}},
			{Name: "age", Type: SchemaNode{Type: "int"}, Default: float64(18)},
			{Name: "email", Type: SchemaNode{
				Type:     "union",
				Branches: []SchemaNode{{Type: "null"}, {Type: "string"}},
			}},
		},
	}

	s := mustNodeSchema(t, node)

	got := s.Root()

	if got.Type != "record" {
		t.Errorf("type: got %q, want record", got.Type)
	}
	if got.Name != "User" {
		t.Errorf("name: got %q, want User", got.Name)
	}
	if got.Namespace != "com.example" {
		t.Errorf("namespace: got %q, want com.example", got.Namespace)
	}
	if got.Doc != "A user record" {
		t.Errorf("doc: got %q, want 'A user record'", got.Doc)
	}
	if len(got.Fields) != 3 {
		t.Fatalf("fields: got %d, want 3", len(got.Fields))
	}
	if got.Fields[0].Name != "name" || got.Fields[0].Type.Type != "string" {
		t.Errorf("field 0: got %+v", got.Fields[0])
	}
	// Root() narrows defaults to the schema's wire width: int → int32,
	// long → int64, float → float32, double → float64. See
	// coerceMetadataDefault in schema_node.go and SchemaField.Default
	// docstring for the type table.
	if got.Fields[1].Name != "age" || got.Fields[1].Default != int32(18) {
		t.Errorf("field 1: got %+v", got.Fields[1])
	}
	if got.Fields[2].Type.Type != "union" || len(got.Fields[2].Type.Branches) != 2 {
		t.Errorf("field 2: got %+v", got.Fields[2])
	}
}

func TestSchemaNodePrimitives(t *testing.T) {
	for _, prim := range []string{"null", "boolean", "int", "long", "float", "double", "string", "bytes"} {
		t.Run(prim, func(t *testing.T) {
			node := &SchemaNode{Type: prim}
			s := mustNodeSchema(t, node)
			got := s.Root()
			if got.Type != prim {
				t.Errorf("got %q, want %q", got.Type, prim)
			}
		})
	}
}

func TestSchemaNodeLogicalTypes(t *testing.T) {
	tests := []struct {
		base    string
		logical string
	}{
		{"long", "timestamp-millis"},
		{"long", "timestamp-micros"},
		{"long", "timestamp-nanos"},
		{"int", "date"},
		{"int", "time-millis"},
		{"long", "time-micros"},
		{"string", "uuid"},
	}
	for _, tt := range tests {
		t.Run(tt.logical, func(t *testing.T) {
			node := &SchemaNode{Type: tt.base, LogicalType: tt.logical}
			s := mustNodeSchema(t, node)
			got := s.Root()
			if got.LogicalType != tt.logical {
				t.Errorf("logicalType: got %q, want %q", got.LogicalType, tt.logical)
			}
		})
	}
}

func TestSchemaNodeDecimal(t *testing.T) {
	node := &SchemaNode{
		Type:        "bytes",
		LogicalType: "decimal",
		Precision:   10,
		Scale:       2,
	}
	s := mustNodeSchema(t, node)
	got := s.Root()
	if got.Precision != 10 {
		t.Errorf("precision: got %d, want 10", got.Precision)
	}
	if got.Scale != 2 {
		t.Errorf("scale: got %d, want 2", got.Scale)
	}
}

func TestSchemaNodeEnum(t *testing.T) {
	node := &SchemaNode{
		Type:    "enum",
		Name:    "Color",
		Symbols: []string{"RED", "GREEN", "BLUE"},
	}
	s := mustNodeSchema(t, node)
	got := s.Root()
	if !reflect.DeepEqual(got.Symbols, []string{"RED", "GREEN", "BLUE"}) {
		t.Errorf("symbols: got %v", got.Symbols)
	}
}

// TestSchemaNodeEmptyRecord exercises the Avro spec requirement (Complex
// Types > Records) that "fields: a JSON array, listing fields (required)"
// — a record with zero user-declared fields must still emit "fields": [].
// Strict readers like Java Avro reject {"type":"record","name":"x"} with
// "Record has no fields".
func TestSchemaNodeEmptyRecord(t *testing.T) {
	node := &SchemaNode{
		Type: "record",
		Name: "Empty",
	}
	s := mustNodeSchema(t, node)
	got := s.Canonical()
	want := `{"name":"Empty","type":"record","fields":[]}`
	if string(got) != want {
		t.Errorf("canonical: got %s, want %s", got, want)
	}
}

// TestSchemaNodeEmptyRecordNested exercises the required-fields fix in
// every position an empty record can appear: as a named-type reference,
// as array items, as map values, as a union branch, and as a field
// type inside another record. Each must emit "fields":[] so Java Avro
// and other strict readers can parse it.
func TestSchemaNodeEmptyRecordNested(t *testing.T) {
	empty := SchemaNode{Type: "record", Name: "Inner"}

	cases := []struct {
		name string
		node SchemaNode
	}{
		{"as field type", SchemaNode{
			Type: "record", Name: "Outer",
			Fields: []SchemaField{{Name: "inner", Type: empty}},
		}},
		{"as array items", SchemaNode{
			Type: "array", Items: &empty,
		}},
		{"as map values", SchemaNode{
			Type: "map", Values: &empty,
		}},
		{"as union branch", SchemaNode{
			Type:     "union",
			Branches: []SchemaNode{{Type: "null"}, empty},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := mustNodeSchema(t, &tc.node)
			canon := string(s.Canonical())
			if !strings.Contains(canon, `"fields":[]`) {
				t.Errorf("canonical missing 'fields':[]: %s", canon)
			}
			// Re-parseability: Canonical() output must itself parse.
			if _, err := Parse(canon); err != nil {
				t.Errorf("Canonical() output failed to re-parse: %v\noutput: %s", err, canon)
			}
		})
	}
}

// TestSchemaNodeCanonicalIdempotent verifies that Canonical() is a
// fixed point: parsing the canonical form and re-canonicalizing must
// produce byte-identical output.
//
// A record MISSING the fields attribute is not in this table: fields is
// required (Java: "Record has no fields") and Parse rejects its absence —
// the empty record is spelled "fields":[]. The rejection is pinned by the
// record-missing-fields mutants in TestMatrix_AcceptanceMutantsRejectLocally.
func TestSchemaNodeCanonicalIdempotent(t *testing.T) {
	inputs := []string{
		`{"type":"record","name":"Empty","fields":[]}`,
		`{"type":"record","name":"Outer","fields":[{"name":"inner","type":{"type":"record","name":"I","fields":[]}}]}`,
		`{"type":"array","items":{"type":"record","name":"E","fields":[]}}`,
		`{"type":"map","values":{"type":"record","name":"E","fields":[]}}`,
		`["null",{"type":"record","name":"E","fields":[]}]`,
	}
	for _, in := range inputs {
		t.Run(in, func(t *testing.T) {
			s1, err := Parse(in)
			if err != nil {
				t.Fatal(err)
			}
			c1 := s1.Canonical()
			s2, err := Parse(string(c1))
			if err != nil {
				t.Fatalf("re-parse canonical failed: %v\ncanonical: %s", err, c1)
			}
			c2 := s2.Canonical()
			if string(c1) != string(c2) {
				t.Errorf("canonical not idempotent:\n  first:  %s\n  second: %s", c1, c2)
			}
		})
	}
}

// TestSchemaNodeCanonicalOrder verifies Parsing Canonical Form's [ORDER]
// rule: "name, type, fields, symbols, items, values, size". Since PCF
// strips all other attributes, a record's canonical form always has the
// key order: name, type, fields.
func TestSchemaNodeCanonicalOrder(t *testing.T) {
	node := &SchemaNode{
		Type:      "record",
		Name:      "Ordered",
		Namespace: "ns",
		Doc:       "doc",
		Aliases:   []string{"Old"},
		Fields: []SchemaField{
			{Name: "a", Type: SchemaNode{Type: "int"}},
		},
	}
	s := mustNodeSchema(t, node)
	got := string(s.Canonical())
	want := `{"name":"ns.Ordered","type":"record","fields":[{"name":"a","type":"int"}]}`
	if got != want {
		t.Errorf("canonical:\n got %s\nwant %s", got, want)
	}
}

func TestSchemaNodeFixed(t *testing.T) {
	node := &SchemaNode{
		Type: "fixed",
		Name: "Hash",
		Size: 32,
	}
	s := mustNodeSchema(t, node)
	got := s.Root()
	if got.Size != 32 {
		t.Errorf("size: got %d, want 32", got.Size)
	}
}

func TestSchemaNodeArray(t *testing.T) {
	node := &SchemaNode{
		Type:  "array",
		Items: &SchemaNode{Type: "string"},
	}
	s := mustNodeSchema(t, node)
	got := s.Root()
	if got.Items == nil || got.Items.Type != "string" {
		t.Errorf("items: got %+v", got.Items)
	}
}

func TestSchemaNodeMap(t *testing.T) {
	node := &SchemaNode{
		Type:   "map",
		Values: &SchemaNode{Type: "int"},
	}
	s := mustNodeSchema(t, node)
	got := s.Root()
	if got.Values == nil || got.Values.Type != "int" {
		t.Errorf("values: got %+v", got.Values)
	}
}

func TestSchemaNodeNestedRecords(t *testing.T) {
	node := &SchemaNode{
		Type: "record",
		Name: "Outer",
		Fields: []SchemaField{
			{Name: "inner", Type: SchemaNode{
				Type: "record",
				Name: "Inner",
				Fields: []SchemaField{
					{Name: "x", Type: SchemaNode{Type: "int"}},
					{Name: "y", Type: SchemaNode{Type: "string"}},
				},
			}},
			{Name: "z", Type: SchemaNode{Type: "long"}},
		},
	}
	s := mustNodeSchema(t, node)

	// Verify encode/decode works.
	type Inner struct {
		X int32  `avro:"x"`
		Y string `avro:"y"`
	}
	type Outer struct {
		Inner Inner `avro:"inner"`
		Z     int64 `avro:"z"`
	}
	v := Outer{Inner: Inner{X: 1, Y: "hello"}, Z: 42}
	data := mustEncode(t, s, &v)
	var got Outer
	mustDecode(t, s, data, &got)
	if got != v {
		t.Errorf("got %+v, want %+v", got, v)
	}
}

func TestSchemaNodeFourLevelWithReuse(t *testing.T) {
	inner := SchemaNode{
		Type: "record",
		Name: "Inner",
		Fields: []SchemaField{
			{Name: "v", Type: SchemaNode{Type: "int"}},
		},
	}
	node := &SchemaNode{
		Type: "record",
		Name: "Root",
		Fields: []SchemaField{
			{Name: "a", Type: SchemaNode{
				Type: "record",
				Name: "Mid",
				Fields: []SchemaField{
					{Name: "deep", Type: inner},
				},
			}},
			{Name: "b", Type: SchemaNode{Type: "Inner"}}, // reference
		},
	}
	s := mustNodeSchema(t, node)
	got := s.Root()
	// Field "b" should be a reference to "Inner".
	if got.Fields[1].Type.Type != "Inner" {
		t.Errorf("field b type: got %q, want Inner reference", got.Fields[1].Type.Type)
	}
}

func TestSchemaNodeCustomProps(t *testing.T) {
	schemaJSON := `{
		"type": "record",
		"name": "Event",
		"fields": [{
			"name": "ts",
			"type": "long",
			"connect.name": "io.debezium.time.Timestamp"
		}],
		"connect.name": "com.example.Event"
	}`
	s := mustParse(t, schemaJSON)
	got := s.Root()
	if got.Props["connect.name"] != "com.example.Event" {
		t.Errorf("record prop: got %q", got.Props["connect.name"])
	}
	// Field-level props are in the field's Type node... actually they're
	// on the field definition in JSON, not the type. Let me check.
}

func TestSchemaNodeFieldProps(t *testing.T) {
	schemaJSON := `{
		"type": "record",
		"name": "Event",
		"fields": [{
			"name": "ts",
			"type": "long",
			"connect.name": "io.debezium.time.Timestamp"
		}]
	}`
	s := mustParse(t, schemaJSON)
	got := s.Root()
	if got.Fields[0].Props["connect.name"] != "io.debezium.time.Timestamp" {
		t.Errorf("field prop: got %q", got.Fields[0].Props["connect.name"])
	}
}

func TestSchemaNodeFieldAliases(t *testing.T) {
	node := &SchemaNode{
		Type: "record",
		Name: "R",
		Fields: []SchemaField{
			{
				Name:    "new_name",
				Type:    SchemaNode{Type: "string"},
				Aliases: []string{"old_name"},
			},
		},
	}
	s := mustNodeSchema(t, node)
	got := s.Root()
	if len(got.Fields[0].Aliases) != 1 || got.Fields[0].Aliases[0] != "old_name" {
		t.Errorf("aliases: got %v", got.Fields[0].Aliases)
	}
}

func TestSchemaNodeFieldDoc(t *testing.T) {
	node := &SchemaNode{
		Type: "record",
		Name: "R",
		Fields: []SchemaField{
			{
				Name: "x",
				Type: SchemaNode{Type: "int"},
				Doc:  "the x coordinate",
			},
		},
	}
	s := mustNodeSchema(t, node)
	got := s.Root()
	if got.Fields[0].Doc != "the x coordinate" {
		t.Errorf("doc: got %q", got.Fields[0].Doc)
	}
}

func TestSchemaNodeInvalid(t *testing.T) {
	node := &SchemaNode{
		Type: "record",
		// Missing name.
		Fields: []SchemaField{
			{Name: "x", Type: SchemaNode{Type: "int"}},
		},
	}
	_, err := node.Schema()
	if err == nil {
		t.Fatal("expected error for record without name")
	}
}

func TestSchemaNodeEncodeDecodeRoundTrip(t *testing.T) {
	// Build schema from node, encode data, decode it back.
	node := &SchemaNode{
		Type:      "record",
		Name:      "Product",
		Namespace: "com.shop",
		Fields: []SchemaField{
			{Name: "name", Type: SchemaNode{Type: "string"}},
			{Name: "price", Type: SchemaNode{Type: "double"}},
			{Name: "tags", Type: SchemaNode{
				Type:  "array",
				Items: &SchemaNode{Type: "string"},
			}},
			{Name: "metadata", Type: SchemaNode{
				Type:   "map",
				Values: &SchemaNode{Type: "int"},
			}},
		},
	}
	s := mustNodeSchema(t, node)

	type Product struct {
		Name     string           `avro:"name"`
		Price    float64          `avro:"price"`
		Tags     []string         `avro:"tags"`
		Metadata map[string]int32 `avro:"metadata"`
	}
	p := Product{
		Name:     "Widget",
		Price:    9.99,
		Tags:     []string{"sale", "new"},
		Metadata: map[string]int32{"stock": 42},
	}
	data := mustEncode(t, s, &p)
	var got Product
	mustDecode(t, s, data, &got)
	if got.Name != p.Name || got.Price != p.Price {
		t.Errorf("got %+v, want %+v", got, p)
	}
}

func TestRootFromParsedSchema(t *testing.T) {
	// Parse a complex schema and verify Root() preserves everything.
	schemaJSON := `{
		"type": "record",
		"name": "Event",
		"namespace": "com.example",
		"doc": "An event",
		"fields": [
			{"name": "id", "type": "string", "doc": "unique id"},
			{"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
			{"name": "data", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
			{"name": "tags", "type": {"type": "array", "items": "string"}},
			{"name": "meta", "type": {"type": "map", "values": "int"}},
			{"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["ACTIVE", "DELETED"]}},
			{"name": "hash", "type": {"type": "fixed", "name": "Hash", "size": 16}},
			{"name": "extra", "type": ["null", "string"], "default": null, "aliases": ["old_extra"]}
		]
	}`
	s := mustParse(t, schemaJSON)
	got := s.Root()

	if got.Type != "record" {
		t.Errorf("type: %q", got.Type)
	}
	if got.Name != "Event" {
		t.Errorf("name: %q", got.Name)
	}
	if got.Namespace != "com.example" {
		t.Errorf("namespace: %q", got.Namespace)
	}
	if got.Doc != "An event" {
		t.Errorf("doc: %q", got.Doc)
	}
	if len(got.Fields) != 8 {
		t.Fatalf("fields: %d", len(got.Fields))
	}

	// id
	if got.Fields[0].Doc != "unique id" {
		t.Errorf("field 0 doc: %q", got.Fields[0].Doc)
	}
	// ts — logical type
	if got.Fields[1].Type.LogicalType != "timestamp-millis" {
		t.Errorf("field 1 logical: %q", got.Fields[1].Type.LogicalType)
	}
	// data — decimal
	if got.Fields[2].Type.Precision != 10 || got.Fields[2].Type.Scale != 2 {
		t.Errorf("field 2 decimal: p=%d s=%d", got.Fields[2].Type.Precision, got.Fields[2].Type.Scale)
	}
	// tags — array
	if got.Fields[3].Type.Items == nil || got.Fields[3].Type.Items.Type != "string" {
		t.Errorf("field 3 items: %+v", got.Fields[3].Type.Items)
	}
	// meta — map
	if got.Fields[4].Type.Values == nil || got.Fields[4].Type.Values.Type != "int" {
		t.Errorf("field 4 values: %+v", got.Fields[4].Type.Values)
	}
	// status — enum
	if len(got.Fields[5].Type.Symbols) != 2 {
		t.Errorf("field 5 symbols: %v", got.Fields[5].Type.Symbols)
	}
	// hash — fixed
	if got.Fields[6].Type.Size != 16 {
		t.Errorf("field 6 size: %d", got.Fields[6].Type.Size)
	}
	// extra — union with default and aliases
	if got.Fields[7].Type.Type != "union" || len(got.Fields[7].Type.Branches) != 2 {
		t.Errorf("field 7 union: %+v", got.Fields[7].Type)
	}
	if got.Fields[7].Default != nil {
		t.Errorf("field 7 default: got %v, want nil", got.Fields[7].Default)
	}
	if len(got.Fields[7].Aliases) != 1 || got.Fields[7].Aliases[0] != "old_extra" {
		t.Errorf("field 7 aliases: %v", got.Fields[7].Aliases)
	}
}

func TestSchemaNodeAliasesEnumDefaultOrderRoundTrip(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "R",
		"aliases": ["OldR", "AncientR"],
		"fields": [
			{"name": "status", "type": {
				"type": "enum",
				"name": "Status",
				"symbols": ["ACTIVE", "DELETED"],
				"default": "ACTIVE",
				"aliases": ["OldStatus"]
			}},
			{"name": "score", "type": "int", "order": "descending"},
			{"name": "tags", "type": {"type": "array", "items": "string"}, "order": "ignore"}
		]
	}`
	s := mustParse(t, schema)
	root := s.Root()

	// Record aliases
	if len(root.Aliases) != 2 || root.Aliases[0] != "OldR" {
		t.Errorf("record aliases: %v", root.Aliases)
	}

	// Enum aliases, default
	enumField := root.Fields[0]
	if len(enumField.Type.Aliases) != 1 || enumField.Type.Aliases[0] != "OldStatus" {
		t.Errorf("enum aliases: %v", enumField.Type.Aliases)
	}
	if !enumField.Type.HasEnumDefault || enumField.Type.EnumDefault != "ACTIVE" {
		t.Errorf("enum default: has=%v val=%q", enumField.Type.HasEnumDefault, enumField.Type.EnumDefault)
	}

	// Field order
	if root.Fields[1].Order != "descending" {
		t.Errorf("score order: %q", root.Fields[1].Order)
	}
	if root.Fields[2].Order != "ignore" {
		t.Errorf("tags order: %q", root.Fields[2].Order)
	}

	// Round-trip: SchemaNode → Schema → Root
	node := s.Root()
	s2 := mustNodeSchema(t, node)
	root2 := s2.Root()
	if len(root2.Aliases) != 2 {
		t.Errorf("round-trip aliases lost: %v", root2.Aliases)
	}
	if !root2.Fields[0].Type.HasEnumDefault {
		t.Error("round-trip enum default lost")
	}
	if root2.Fields[1].Order != "descending" {
		t.Error("round-trip order lost")
	}
}

func TestSchemaNodeCustomPropsExtended(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "R",
		"custom.tag": "hello",
		"custom.num": 42,
		"fields": [
			{"name": "x", "type": "int", "custom.field": true}
		]
	}`
	s := mustParse(t, schema)
	root := s.Root()
	if root.Props["custom.tag"] != "hello" {
		t.Errorf("record props: %v", root.Props)
	}
	// Integer JSON literals preserve precision and come back as int64,
	// not float64. See TestRegression_SchemaExtraNumberPrecisionLoss.
	if root.Props["custom.num"] != int64(42) {
		t.Errorf("record num prop: %v", root.Props["custom.num"])
	}
	if root.Fields[0].Props["custom.field"] != true {
		t.Errorf("field props: %v", root.Fields[0].Props)
	}
}

func TestSchemaNodeDedupNamedTypes(t *testing.T) {
	uuid := SchemaNode{Type: "fixed", Name: "uuid_f", Size: 16, LogicalType: "uuid"}
	node := SchemaNode{
		Type: "record",
		Name: "r",
		Fields: []SchemaField{
			{Name: "a", Type: SchemaNode{Type: "union", Branches: []SchemaNode{{Type: "null"}, uuid}}},
			{Name: "b", Type: SchemaNode{Type: "union", Branches: []SchemaNode{{Type: "null"}, uuid}}},
		},
	}
	s, err := node.Schema()
	if err != nil {
		t.Fatalf("Schema() with duplicate named type should succeed: %v", err)
	}
	// Round-trip: encode and decode to verify both fields work.
	input := map[string]any{"a": [16]byte{1}, "b": [16]byte{2}}
	enc, err := s.Encode(input)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	mustDecode(t, s, enc, &out)
	a, _ := out["a"].([16]byte)
	b, _ := out["b"].([16]byte)
	if a[0] != 1 || b[0] != 2 {
		t.Fatalf("round-trip failed: a=%v b=%v", a, b)
	}
}

func TestSchemaNodeDedupConflictingNameErrors(t *testing.T) {
	node := SchemaNode{
		Type: "record",
		Name: "r",
		Fields: []SchemaField{
			{Name: "a", Type: SchemaNode{Type: "fixed", Name: "f", Size: 16}},
			{Name: "b", Type: SchemaNode{Type: "fixed", Name: "f", Size: 8}}, // same name, different size
		},
	}
	_, err := node.Schema()
	if err == nil {
		t.Fatal("expected error for conflicting named type definitions")
	}
}

func TestSchemaNodeCyclicItems(t *testing.T) {
	outer := &SchemaNode{Type: "array"}
	outer.Items = outer
	_, err := outer.Schema()
	if err == nil {
		t.Fatal("expected error for cyclic SchemaNode via Items")
	}
}

func TestSchemaNodeCyclicValues(t *testing.T) {
	outer := &SchemaNode{Type: "map"}
	outer.Values = outer
	_, err := outer.Schema()
	if err == nil {
		t.Fatal("expected error for cyclic SchemaNode via Values")
	}
}

func TestSchemaNodeCyclicIndirect(t *testing.T) {
	// A.Items → B.Values → A
	a := &SchemaNode{Type: "array"}
	b := &SchemaNode{Type: "map", Values: a}
	a.Items = b
	if _, err := a.Schema(); err == nil {
		t.Fatal("expected error for indirect 2-node cycle")
	}
}

func TestSchemaNodeCyclic3Node(t *testing.T) {
	// A.Items → B.Items → C.Items → A
	a := &SchemaNode{Type: "array"}
	b := &SchemaNode{Type: "array"}
	c := &SchemaNode{Type: "array"}
	a.Items = b
	b.Items = c
	c.Items = a
	if _, err := a.Schema(); err == nil {
		t.Fatal("expected error for 3-node cycle")
	}
}

// A deep ACYCLIC SchemaNode chain (distinct node per level, so the
// cycle-detection visited map never fires) must bound its own walk: the
// toJSONWalk recursion has no pointer cycle to terminate it, so without a
// depth bound a hand-built array<array<…>> deep enough overflows the
// goroutine stack and kills the process uncatchably — before Schema's
// eventual Parse can reject it. The walk caps at maxSchemaJSONDepth, so a
// too-deep tree stops with this bounded error rather than crashing. This
// pins that the SchemaNode walk bounds depth ITSELF (message names the
// node tree) rather than relying on Parse's downstream JSON-bracket cap.
func TestRegression_SchemaNodeSchemaDeepAcyclicBounded(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked: %v", r)
		}
	}()
	const depth = maxSchemaJSONDepth + 50
	cur := &SchemaNode{Type: "long"}
	for range depth {
		cur = &SchemaNode{Type: "array", Items: cur}
	}
	_, err := cur.Schema()
	if err == nil {
		t.Fatal("expected a bounded error for an over-deep SchemaNode chain, got nil")
	}
	if !strings.Contains(err.Error(), "SchemaNode tree nests deeper") {
		t.Fatalf("expected the node-walk depth bound to fire, got %v", err)
	}

	// Boundary-1: a tree well below the cap (and below the wire codec's own
	// maxDepth, so genuinely usable) must still build — the bound must not
	// false-reject a legitimately deep hand-built tree.
	ok := &SchemaNode{Type: "long"}
	for range 500 {
		ok = &SchemaNode{Type: "array", Items: ok}
	}
	if _, err := ok.Schema(); err != nil {
		t.Fatalf("a 500-deep array tree should build, got %v", err)
	}
}

// The value channel — a Props value or a SchemaField.Default — is a SEPARATE
// recursion from the structural node walk: each value is handed to
// jsonSerializableValue (needsJSONFixup/applyJSONFixup) and then to
// json.Marshal at Schema(), none of which bounds depth. So a value nested
// deeply enough overflows the goroutine stack uncatchably (recover cannot
// catch a stack overflow) even when the node tree itself is one level deep —
// the structural depth bound never sees it. The walk bounds the value at the
// same maxSchemaJSONDepth ceiling, so an over-deep value stops with a bounded
// error rather than crashing.
func TestMatrix_SchemaNodeSchemaDeepValueBounded(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked: %v", r)
		}
	}()
	deepValue := func() any {
		var v any = "leaf"
		for range maxSchemaJSONDepth + 50 {
			v = map[string]any{"k": v}
		}
		return v
	}

	t.Run("props value", func(t *testing.T) {
		node := &SchemaNode{Type: "int", Props: map[string]any{"x": deepValue()}}
		_, err := node.Schema()
		if err == nil {
			t.Fatal("expected a bounded error for an over-deep Props value, got nil")
		}
		if !strings.Contains(err.Error(), "value nests deeper") {
			t.Fatalf("expected the value-channel depth bound to fire, got %v", err)
		}
	})

	t.Run("field default value", func(t *testing.T) {
		node := &SchemaNode{
			Type: "record",
			Name: "R",
			Fields: []SchemaField{
				{Name: "f", HasDefault: true, Default: deepValue(), Type: SchemaNode{Type: "int"}},
			},
		}
		_, err := node.Schema()
		if err == nil {
			t.Fatal("expected a bounded error for an over-deep Default value, got nil")
		}
		if !strings.Contains(err.Error(), "value nests deeper") {
			t.Fatalf("expected the value-channel depth bound to fire, got %v", err)
		}
	})

	// Boundary-1: a value nested well below the cap must still build AND
	// survive the round-trip — the bound must not false-reject or silently
	// drop a legitimately structured property.
	t.Run("usable depth builds and round-trips", func(t *testing.T) {
		var v any = "leaf"
		for range 500 {
			v = map[string]any{"k": v}
		}
		node := &SchemaNode{Type: "int", Props: map[string]any{"x": v}}
		s, err := node.Schema()
		if err != nil {
			t.Fatalf("a 500-deep Props value should build, got %v", err)
		}
		if got := s.Root().Props["x"]; got == nil {
			t.Fatal("the usable-depth Props value was dropped")
		}
	})

	// Sibling channel: SchemaFor embeds a hand-built CustomType.Schema via the
	// same value walk (the bare, d==nil path). A deep value there must also
	// terminate instead of crashing the process; the bare path truncates the
	// over-deep value rather than erroring (mirroring the structural walk's
	// documented bare-path behavior), so the assertion is simply that it
	// returns without a stack-overflow death. The CustomType.Schema is embedded
	// (and its value walk runs) ONLY when a struct FIELD matches the custom
	// GoType — SchemaFor over a non-struct errors before any walk — so this uses
	// a struct field, and a typed-container value to cover the broadened bound.
	t.Run("SchemaFor custom-schema deep value terminates", func(t *testing.T) {
		var v any = "leaf"
		for range maxSchemaJSONDepth + 50 {
			v = []map[string]any{{"k": v}}
		}
		type recForCustom struct{ F int32 }
		ct := CustomType{
			GoType: reflect.TypeOf(int32(0)),
			Schema: &SchemaNode{Type: "int", Props: map[string]any{"deep": v}},
		}
		_, _ = SchemaFor[recForCustom](WithCustomType(ct))
	})

	// json.Marshal recurses into EVERY container kind, not just the
	// map[string]any / []any shapes Root() produces. A hand-built node can store
	// any value the map[string]any field accepts; a TYPED container nests just
	// as deeply yet was invisible to a map[string]any/[]any-only depth check, so
	// it reached json.Marshal unbounded and overflowed the goroutine stack. The
	// bound must cover the typed shapes too.
	t.Run("typed-container props value rejected", func(t *testing.T) {
		var v any = "leaf"
		for range maxSchemaJSONDepth + 50 {
			v = []map[string]any{{"k": v}} // []map[string]any: NOT []any, NOT map[string]any
		}
		node := &SchemaNode{Type: "int", Props: map[string]any{"x": v}}
		_, err := node.Schema()
		if err == nil {
			t.Fatal("expected a bounded error for an over-deep typed-container Props value, got nil")
		}
		if !strings.Contains(err.Error(), "value nests deeper") {
			t.Fatalf("expected the value-channel depth bound to fire (not a post-marshal Parse error), got %v", err)
		}
	})

	// Boundary-1: a typed container nested well below the cap must still build —
	// the broadened walk must not false-reject legitimate typed values.
	t.Run("usable typed-container builds", func(t *testing.T) {
		var v any = "leaf"
		for range 200 {
			v = []map[string]any{{"k": v}}
		}
		node := &SchemaNode{Type: "int", Props: map[string]any{"x": v}}
		s, err := node.Schema()
		if err != nil {
			t.Fatalf("a 200-deep typed-container Props value should build, got %v", err)
		}
		if got := s.Root().Props["x"]; got == nil {
			t.Fatal("the usable-depth typed-container Props value was dropped")
		}
	})

	// A cyclic Go type (type P *P) has an infinite reflect-value chain. The walk
	// decrements its budget on every indirection, so it must TERMINATE with the
	// bound rather than recursing forever (the trap a reflect indirection walk
	// without a bound would fall into) — Schema returns, no hang, no crash.
	t.Run("cyclic pointer value terminates", func(t *testing.T) {
		type selfPtr *selfPtr
		var p selfPtr
		p = &p // p points to itself: p.Elem() == p, an unbounded pointer chain
		node := &SchemaNode{Type: "int", Props: map[string]any{"x": p}}
		_, _ = node.Schema() // must return (the bound stops the walk), not hang
	})
}

// The SchemaNode→JSON walk (toJSONWalk + the value channel) must bound DEPTH on
// EVERY channel a hand-built node can carry nesting through, or a refactor that
// drops one channel's depth charge ships an uncatchable stack overflow with a
// green battery. The structural recursions are four distinct sites
// (Items/Values/Branches/Fields), the value channel is three distinct
// boundedSerializableValue call sites (node Props, field Props, field Default),
// and the value walk must recurse into every container kind json.Marshal does
// (map, slice, array, struct, pointer/interface) — this pins each cell so its
// bound cannot be silently removed.
func TestMatrix_SchemaNodeWalkDepthAllChannels(t *testing.T) {
	const deep = maxSchemaJSONDepth + 50

	// Each structural recursion must charge depth. A too-deep chain through any
	// one of them stops with the structural depth error, never a crash.
	structural := map[string]func() *SchemaNode{
		"items": func() *SchemaNode {
			cur := &SchemaNode{Type: "long"}
			for range deep {
				cur = &SchemaNode{Type: "array", Items: cur}
			}
			return cur
		},
		"values": func() *SchemaNode {
			cur := &SchemaNode{Type: "long"}
			for range deep {
				cur = &SchemaNode{Type: "map", Values: cur}
			}
			return cur
		},
		"branches": func() *SchemaNode {
			cur := SchemaNode{Type: "long"}
			for range deep {
				cur = SchemaNode{Type: "union", Branches: []SchemaNode{cur}}
			}
			return &cur
		},
		"fields": func() *SchemaNode {
			cur := SchemaNode{Type: "long"}
			for i := range deep {
				// Distinct names per level: same-named records would dedup to a
				// reference and collapse the chain instead of nesting it.
				cur = SchemaNode{Type: "record", Name: fmt.Sprintf("R%d", i), Fields: []SchemaField{{Name: "f", Type: cur}}}
			}
			return &cur
		},
	}
	for name, build := range structural {
		t.Run("structural/"+name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panicked: %v", r)
				}
			}()
			_, err := build().Schema()
			if err == nil || !strings.Contains(err.Error(), "tree nests deeper") {
				t.Fatalf("want the structural depth bound to fire, got %v", err)
			}
		})
	}

	deepMap := func() any {
		var v any = "leaf"
		for range deep {
			v = map[string]any{"k": v}
		}
		return v
	}
	// Each of the three value sites routes its value through
	// boundedSerializableValue; all three must bound a too-deep value. node Props
	// and field Default were already pinned; field Props (the third call site)
	// was not — a dropped bound there crashes only on a field-level property.
	valueSites := map[string]func(any) *SchemaNode{
		"node-props": func(v any) *SchemaNode {
			return &SchemaNode{Type: "int", Props: map[string]any{"x": v}}
		},
		"field-props": func(v any) *SchemaNode {
			return &SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{
				{Name: "f", Type: SchemaNode{Type: "int"}, Props: map[string]any{"x": v}},
			}}
		},
		"field-default": func(v any) *SchemaNode {
			return &SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{
				{Name: "f", HasDefault: true, Default: v, Type: SchemaNode{Type: "int"}},
			}}
		},
	}
	for name, build := range valueSites {
		t.Run("value-site/"+name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panicked: %v", r)
				}
			}()
			_, err := build(deepMap()).Schema()
			if err == nil || !strings.Contains(err.Error(), "value nests deeper") {
				t.Fatalf("want the value depth bound to fire, got %v", err)
			}
		})
	}

	// The value walk mirrors json.Marshal's recursion into EVERY container kind,
	// not just the map[string]any/[]any shapes Root() emits — a hand-built node
	// can store any Go value the map[string]any field accepts. Each kind deep
	// enough must hit the value depth bound; removing its arm from valueWalkLimit
	// would let json.Marshal stack-overflow on that shape instead.
	type box struct{ X any }
	kinds := map[string]func() any{
		"typed-slice": func() any {
			var v any = "leaf"
			for range deep {
				v = []map[string]any{{"k": v}}
			}
			return v
		},
		"array": func() any {
			var v any = "leaf"
			for range deep {
				v = [1]any{v}
			}
			return v
		},
		"struct": func() any {
			var v any = "leaf"
			for range deep {
				v = box{X: v}
			}
			return v
		},
		"pointer": func() any {
			var v any = "leaf"
			for range deep {
				x := v
				v = &x
			}
			return v
		},
	}
	for name, build := range kinds {
		t.Run("value-kind/"+name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panicked: %v", r)
				}
			}()
			node := &SchemaNode{Type: "int", Props: map[string]any{"x": build()}}
			_, err := node.Schema()
			if err == nil || !strings.Contains(err.Error(), "value nests deeper") {
				t.Fatalf("want the value depth bound to fire for %s, got %v", name, err)
			}
		})
	}
}

// Depth is not the only unbounded axis: a shared-reference DAG nests SHALLOW yet
// fans out into a 2^depth tree when serialized, because neither toJSONWalk nor
// json.Marshal memoizes shared references. The depth bound (which caps the longest
// PATH) is blind to it — depth 60 here is ~120 allocated nodes but 2^60 emitted
// nodes, which would hang/OOM the process before Schema's eventual Parse runs. The
// node-count budget bounds the total emitted nodes across the whole walk, shared
// so the combined json.Marshal cost stays bounded. This pins the expansion axis on
// every channel — the cell the three prior depth bounds all missed.
func TestMatrix_SchemaNodeSharedDAGExpansionBounded(t *testing.T) {
	const fanout = 60 // 2^60 emitted nodes if unbounded; ~120 nodes in memory

	// Run the build in a goroutine so a REGRESSION (a removed budget → an
	// unbounded fan-out) fails this test via the timeout instead of hanging the
	// whole suite. Post-fix the budget rejects in well under a second.
	noHangReject := func(t *testing.T, build func() (*Schema, error), wantMsg string) {
		t.Helper()
		ch := make(chan error, 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					ch <- fmt.Errorf("panicked: %v", r)
				}
			}()
			_, err := build()
			ch <- err
		}()
		select {
		case err := <-ch:
			if err == nil || !strings.Contains(err.Error(), wantMsg) {
				t.Fatalf("want %q, got %v", wantMsg, err)
			}
		case <-time.After(hangDeadline):
			t.Fatalf("did not reject within 30s — the node budget is not bounding the fan-out")
		}
	}

	// Structural fan-out: each level's Items AND Values point at the SAME child,
	// so toJSONWalk re-walks it twice per level (the visited map is path-scoped,
	// so off-path sharing is not a cycle).
	t.Run("structural", func(t *testing.T) {
		noHangReject(t, func() (*Schema, error) {
			cur := &SchemaNode{Type: "long"}
			for range fanout {
				cur = &SchemaNode{Type: "array", Items: cur, Values: cur}
			}
			return cur.Schema()
		}, "tree expands to more")
	})

	// Value fan-out at each of the three value sites: a map whose two keys share
	// the same child value, compounded per level.
	valDAG := func() any {
		var v any = "leaf"
		for range fanout {
			inner := v
			v = map[string]any{"a": inner, "b": inner}
		}
		return v
	}
	valueSites := map[string]func(any) *SchemaNode{
		"node-props": func(v any) *SchemaNode {
			return &SchemaNode{Type: "int", Props: map[string]any{"x": v}}
		},
		"field-props": func(v any) *SchemaNode {
			return &SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{
				{Name: "f", Type: SchemaNode{Type: "int"}, Props: map[string]any{"x": v}},
			}}
		},
		"field-default": func(v any) *SchemaNode {
			return &SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{
				{Name: "f", HasDefault: true, Default: v, Type: SchemaNode{Type: "int"}},
			}}
		},
	}
	for name, build := range valueSites {
		t.Run("value-site/"+name, func(t *testing.T) {
			noHangReject(t, func() (*Schema, error) { return build(valDAG()).Schema() }, "value expands to more")
		})
	}

	// The bare path (SchemaFor's hand-built CustomType.Schema, walked via toJSON
	// with d==nil) truncates over-budget subtrees rather than erroring, so the
	// invariant is simply that it TERMINATES instead of fanning out forever.
	t.Run("schemafor-bare-path", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panicked: %v", r)
			}
		}()
		type recForCustom struct{ F int32 }
		ct := CustomType{
			GoType: reflect.TypeOf(int32(0)),
			Schema: &SchemaNode{Type: "int", Props: map[string]any{"x": valDAG()}},
		}
		done := make(chan struct{})
		go func() {
			_, _ = SchemaFor[recForCustom](WithCustomType(ct))
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(hangDeadline):
			t.Fatal("SchemaFor on a shared-DAG CustomType.Schema did not terminate")
		}
	})

	// Boundary: the bound rejects compounding FAN-OUT, not all sharing. A benign
	// shallow double-reference (json.Marshal expands it to two copies, cheaply)
	// must still build — and a structural reuse of a named sub-node must dedup to
	// a reference, not be rejected as fan-out.
	t.Run("benign-sharing-builds", func(t *testing.T) {
		shared := map[string]any{"k": "v"}
		node := &SchemaNode{Type: "int", Props: map[string]any{"a": shared, "b": shared}}
		if _, err := node.Schema(); err != nil {
			t.Fatalf("a benign shallow double-reference should build, got %v", err)
		}
		inner := &SchemaNode{Type: "record", Name: "Inner", Fields: []SchemaField{{Name: "n", Type: SchemaNode{Type: "long"}}}}
		reuse := &SchemaNode{Type: "record", Name: "Outer", Fields: []SchemaField{
			{Name: "a", Type: SchemaNode{Type: "array", Items: inner}},
			{Name: "b", Type: SchemaNode{Type: "array", Items: inner}},
		}}
		if _, err := reuse.Schema(); err != nil {
			t.Fatalf("reusing a named sub-node (deduped to a ref) should build, got %v", err)
		}
	})
}

// TestRegression_SchemaNodeDuplicateNamedDefinitionBounded pins that
// SchemaNode.Schema()'s dedup conflict check bounds its cost by the SHARED
// per-walk node budget. When a named type re-occurs as a DISTINCT pointer with
// an identical body, the body is marshal-compared against the first definition;
// that comparison must charge the same maxSchemaJSONNodes budget the rest of the
// walk uses, so k identical-bodied copies of a w-node definition cost
// O(maxSchemaJSONNodes), not O(k*w) — even though the emitted schema is tiny. If
// the comparison re-marshals each copy on a fresh budget, k*w can reach the
// budget SQUARED while k+w stays within it, reachable from a hand-built node via
// the public API.
func TestRegression_SchemaNodeDuplicateNamedDefinitionBounded(t *testing.T) {
	// "Dup": a record with w long fields. The outer record references it via k
	// distinct-pointer copies (value copies => &Fields[i].Type are distinct,
	// bodies byte-identical). Valid Avro: a record may have many fields of one
	// named type, so the dedup yields one Dup definition + k-1 references.
	buildDup := func(w int) SchemaNode {
		fields := make([]SchemaField, w)
		for i := range fields {
			fields[i] = SchemaField{Name: fmt.Sprintf("g%d", i), Type: SchemaNode{Type: "long"}}
		}
		return SchemaNode{Type: "record", Name: "Dup", Fields: fields}
	}
	buildOuter := func(dup SchemaNode, k int) *SchemaNode {
		fields := make([]SchemaField, k)
		for i := range fields {
			fields[i] = SchemaField{Name: fmt.Sprintf("f%d", i), Type: dup}
		}
		return &SchemaNode{Type: "record", Name: "Outer", Fields: fields}
	}

	t.Run("many-large-duplicates-bounded", func(t *testing.T) {
		// w*k = 2.25M; an unbounded conflict marshal would re-emit ~2*w*k nodes
		// (far past maxSchemaJSONNodes) while the output is one Dup def + refs.
		// The shared budget caps total work and rejects over-budget. Run in a
		// goroutine so a regression (fresh-budget re-marshal restored) surfaces
		// as the timeout instead of hanging the suite; the bounded reject
		// finishes well under it.
		node := buildOuter(buildDup(1500), 1500)
		ch := make(chan error, 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					ch <- fmt.Errorf("panicked: %v", r)
				}
			}()
			_, err := node.Schema()
			ch <- err
		}()
		select {
		case err := <-ch:
			if err == nil || !strings.Contains(err.Error(), "expands to more") {
				t.Fatalf("want an over-budget rejection (the conflict marshal must share the node budget), got %v", err)
			}
		case <-time.After(hangDeadline):
			t.Fatal("did not complete within 30s — the conflict-comparison marshal is not bounded by the shared node budget")
		}
	})

	t.Run("legit-duplication-builds", func(t *testing.T) {
		// A handful of distinct-pointer copies of a small named type build fine
		// and dedup to one definition + references — the boundary the bound
		// must NOT false-reject. (If dedup failed, Parse would reject the
		// repeated "Dup" name, so a successful build proves the dedup ran.)
		s, err := buildOuter(buildDup(3), 5).Schema()
		if err != nil {
			t.Fatalf("legitimate small duplication should build, got %v", err)
		}
		if _, err := Parse(s.String()); err != nil {
			t.Fatalf("deduped schema must re-parse, got %v", err)
		}
	})

	t.Run("conflicting-bodies-errors", func(t *testing.T) {
		// Distinct-pointer, same fullname, DIFFERENT bodies => a genuine
		// redefinition conflict the bound must still surface.
		a := SchemaNode{Type: "record", Name: "C", Fields: []SchemaField{{Name: "x", Type: SchemaNode{Type: "long"}}}}
		b := SchemaNode{Type: "record", Name: "C", Fields: []SchemaField{{Name: "y", Type: SchemaNode{Type: "string"}}}}
		node := &SchemaNode{Type: "record", Name: "Outer", Fields: []SchemaField{
			{Name: "a", Type: a}, {Name: "b", Type: b},
		}}
		if _, err := node.Schema(); err == nil || !strings.Contains(err.Error(), "conflicting definitions") {
			t.Fatalf("want a conflicting-definitions error, got %v", err)
		}
	})
}

func TestSchemaNodeUnmarshalablePropsErrors(t *testing.T) {
	// json.Marshal rejects channels, funcs, complex numbers.
	node := SchemaNode{
		Type:  "int",
		Props: map[string]any{"bad": make(chan int)},
	}
	if _, err := node.Schema(); err == nil {
		t.Fatal("expected error for unmarshalable Props value")
	}
}

// A fixed with a quoted-string size ("16", the spec [INTEGERS] form) must
// surface Size in the metadata tree and round-trip through Root().Schema().
// jsonNumericInt previously handled only numeric forms, so Root().Size was
// 0 and Root().Schema() failed "fixed is missing size" (a Pattern-13b
// metadata bug: Parse accepted the quoted size but no test read it back).
func TestRegression_RootQuotedFixedSize(t *testing.T) {
	for _, schema := range []string{
		`{"type":"fixed","name":"F","size":"16"}`,
		`{"type":"fixed","name":"F","size":"016"}`,
		`{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"fixed","name":"G","size":"4"}}]}`,
	} {
		s := MustParse(schema)
		node := s.Root()
		rt, err := node.Schema()
		if err != nil {
			t.Errorf("%s: Root().Schema(): %v", schema, err)
			continue
		}
		if string(s.Canonical()) != string(rt.Canonical()) {
			t.Errorf("%s: canonical changed across Root().Schema():\n %s\n %s", schema, s.Canonical(), rt.Canonical())
		}
	}
}

// TestMatrix_SchemaNodeWalkBudgetBattery is THE consolidated DoS battery for the
// SchemaNode→JSON metadata walk, reached via the public SchemaNode.Schema()
// (dedup path, d != nil, errors) and the bare toJSON() SchemaFor reaches via a
// hand-built CustomType.Schema (d == nil, truncates). The walk has THREE
// independently-unbounded axes, and a hand-built node drives cost through every
// recursion point, fan-out point, AND per-node payload on each:
//
//   - DEPTH (maxSchemaJSONDepth): the longest container PATH. Unbounded → the
//     fixup walk or json.Marshal overflows the goroutine stack uncatchably.
//   - NODES (maxSchemaJSONNodes): the COUNT of emitted JSON nodes. Unbounded → a
//     shared-reference DAG, tiny in memory, fans out into a 2^depth tree.
//   - BYTES (maxSchemaJSONBytes): the SIZE of every emitted scalar payload.
//     Unbounded → a huge or widely-shared string/slice, stored by reference and
//     invisible to the node count, blows the output past memory while the node
//     count stays tiny.
//
// Five prior rounds each dribbled ONE bound here (46d4dde, 7f13cf9, 01b0b32,
// 885e132, e76cd84) — a process failure. This drives the whole surface at once:
// a later schema_node-walk DoS find is expected to EXTEND it, not to be bounded
// from scratch in a fresh one-off. Every cell isolates ONE hostile payload to
// ONE charge site and asserts the bound-specific message — which no other code
// emits — so a cell cannot pass on an unrelated Parse error and a removed charge
// turns exactly its cell red. Boundary cells pin that a usable schema is never
// false-rejected.
func TestMatrix_SchemaNodeWalkBudgetBattery(t *testing.T) {
	wantBytes := fmt.Sprintf("supported %d bytes", maxSchemaJSONBytes)
	wantNodes := fmt.Sprintf("supported %d nodes", maxSchemaJSONNodes)

	// huge is one byte past the output-size budget: any single emission of it
	// trips the byte bound alone. Allocated ONCE and shared by reference across
	// cells, so the battery's footprint is one budget-sized string, not one per
	// cell. hugeBytes is its []byte sibling for the bytes/fixed value channel.
	huge := strings.Repeat("x", maxSchemaJSONBytes+1)
	hugeBytes := make([]byte, maxSchemaJSONBytes+1)

	// reject runs build() in a goroutine so a REGRESSION (a removed bound →
	// unbounded output/hang) surfaces as the timeout rather than wedging the
	// suite; the bounded reject returns in well under it. want is the bound-
	// specific fragment, so the cell fails unless THAT bound fired.
	reject := func(t *testing.T, want string, build func() (*Schema, error)) {
		t.Helper()
		ch := make(chan error, 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					ch <- fmt.Errorf("panicked: %v", r)
				}
			}()
			_, err := build()
			ch <- err
		}()
		select {
		case err := <-ch:
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("want a bounded error containing %q, got %v", want, err)
			}
		case <-time.After(hangDeadline):
			t.Fatalf("did not reject within 30s — the %q bound is not firing", want)
		}
	}

	// Axis BYTES — per-node scalar payload. Each cell carries `huge` (or its
	// []byte form) at exactly one emission/charge site; nothing else is large,
	// so only that site's byte charge can fire.
	t.Run("bytes/scalar-payload", func(t *testing.T) {
		intField := []SchemaField{{Name: "f", Type: SchemaNode{Type: "int"}}}
		cells := map[string]func() (*Schema, error){
			// toJSONWalk top charge (Type / Name / Namespace), charged before the
			// fullname is hashed into the dedup map or emitted as a reference.
			"node-type": func() (*Schema, error) { return (&SchemaNode{Type: huge}).Schema() },
			"node-name": func() (*Schema, error) { return (&SchemaNode{Type: "record", Name: huge, Fields: intField}).Schema() },
			"node-namespace": func() (*Schema, error) {
				return (&SchemaNode{Type: "record", Name: "R", Namespace: huge, Fields: intField}).Schema()
			},
			// node-level scalar strings.
			"node-doc":         func() (*Schema, error) { return (&SchemaNode{Type: "fixed", Name: "F", Size: 1, Doc: huge}).Schema() },
			"node-logicalType": func() (*Schema, error) { return (&SchemaNode{Type: "int", LogicalType: huge}).Schema() },
			"enum-default": func() (*Schema, error) {
				return (&SchemaNode{Type: "enum", Name: "E", Symbols: []string{"A"}, HasEnumDefault: true, EnumDefault: huge}).Schema()
			},
			// field-level scalar strings.
			"field-name": func() (*Schema, error) {
				return (&SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{{Name: huge, Type: SchemaNode{Type: "int"}}}}).Schema()
			},
			"field-order": func() (*Schema, error) {
				return (&SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{{Name: "f", Order: huge, Type: SchemaNode{Type: "int"}}}}).Schema()
			},
			"field-doc": func() (*Schema, error) {
				return (&SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{{Name: "f", Doc: huge, Type: SchemaNode{Type: "int"}}}}).Schema()
			},
			// Props object keys (the VALUE channel charges values; the KEY string
			// is charged separately as an emitted object key).
			"node-props-key": func() (*Schema, error) { return (&SchemaNode{Type: "int", Props: map[string]any{huge: "v"}}).Schema() },
			"field-props-key": func() (*Schema, error) {
				return (&SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{{Name: "f", Type: SchemaNode{Type: "int"}, Props: map[string]any{huge: "v"}}}}).Schema()
			},
			// Value leaves walked by valueWalkLimit (Props / Default channel).
			"value-string": func() (*Schema, error) { return (&SchemaNode{Type: "int", Props: map[string]any{"x": huge}}).Schema() },
			"value-jsonNumber": func() (*Schema, error) {
				return (&SchemaNode{Type: "int", Props: map[string]any{"x": json.Number(huge)}}).Schema()
			},
			"value-bytes": func() (*Schema, error) {
				return (&SchemaNode{Type: "int", Props: map[string]any{"x": hugeBytes}}).Schema()
			},
			"value-map-key": func() (*Schema, error) {
				return (&SchemaNode{Type: "int", Props: map[string]any{"x": map[string]any{huge: "v"}}}).Schema()
			},
			"value-struct-field-name": func() (*Schema, error) {
				// json.Marshal emits a struct field name as an object key; a
				// pathological StructOf type carries a huge one. Fan a 1 MiB
				// field name across enough instances to clear the budget.
				ft := reflect.StructOf([]reflect.StructField{{Name: "F" + strings.Repeat("a", 1<<20), Type: reflect.TypeOf(0)}})
				n := maxSchemaJSONBytes/(1<<20) + 8
				return (&SchemaNode{Type: "int", Props: map[string]any{"x": reflect.MakeSlice(reflect.SliceOf(ft), n, n).Interface()}}).Schema()
			},
		}
		for name, build := range cells {
			t.Run(name, func(t *testing.T) { reject(t, wantBytes, build) })
		}
	})

	// Axis NODES — string SLICES emit one array node per element, so their
	// element COUNT (not just the structural node) is charged. A slice one past
	// the node budget trips it on its own.
	t.Run("nodes/slice-payload", func(t *testing.T) {
		bigSlice := make([]string, maxSchemaJSONNodes+1) // empty strings: nodes axis, not bytes
		cells := map[string]func() (*Schema, error){
			"symbols": func() (*Schema, error) { return (&SchemaNode{Type: "enum", Name: "E", Symbols: bigSlice}).Schema() },
			"node-aliases": func() (*Schema, error) {
				return (&SchemaNode{Type: "fixed", Name: "F", Size: 1, Aliases: bigSlice}).Schema()
			},
			"field-aliases": func() (*Schema, error) {
				return (&SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{{Name: "f", Type: SchemaNode{Type: "int"}, Aliases: bigSlice}}}).Schema()
			},
		}
		for name, build := range cells {
			t.Run(name, func(t *testing.T) { reject(t, wantNodes, build) })
		}
	})

	// The shared-reference DAG — the exact shape that is O(K+L) in memory but
	// K*L in json.Marshal's output. The 885e132 node bound closed it for
	// STRUCTURE; these close it for leaf PAYLOAD (and confirm the dedup-
	// reference and dedup-map-hashing paths are bounded too).
	t.Run("shared-dag-amplification", func(t *testing.T) {
		shared := strings.Repeat("x", 1<<20) // 1 MiB, shared by reference
		refs := maxSchemaJSONBytes/len(shared) + 8

		// One shared 1 MiB doc fanned across many distinct named branches.
		t.Run("doc-across-branches", func(t *testing.T) {
			reject(t, wantBytes, func() (*Schema, error) {
				branches := make([]SchemaNode, refs)
				for i := range branches {
					branches[i] = SchemaNode{Type: "fixed", Name: fmt.Sprintf("F%d", i), Size: 1, Doc: shared}
				}
				return (&SchemaNode{Type: "union", Branches: branches}).Schema()
			})
		})

		// A 1 MiB-named record referenced via many distinct-pointer copies → one
		// definition + refs-1 bare references, each re-emitting (and re-hashing)
		// the 1 MiB fullname. Bounded by the top charge, which runs before the
		// dedup map touches the name.
		t.Run("name-via-dedup-references", func(t *testing.T) {
			reject(t, wantBytes, func() (*Schema, error) {
				dup := SchemaNode{Type: "record", Name: shared, Fields: []SchemaField{{Name: "g", Type: SchemaNode{Type: "long"}}}}
				fields := make([]SchemaField, refs)
				for i := range fields {
					fields[i] = SchemaField{Name: fmt.Sprintf("f%d", i), Type: dup}
				}
				return (&SchemaNode{Type: "record", Name: "Outer", Fields: fields}).Schema()
			})
		})

		// One shared symbols slice fanned across distinct-named enums → the node
		// budget (slice elements) prunes the fan-out.
		t.Run("symbols-slice-across-enums", func(t *testing.T) {
			reject(t, wantNodes, func() (*Schema, error) {
				sym := make([]string, maxSchemaJSONNodes/4)
				branches := make([]SchemaNode, 6) // 6 * (budget/4) > budget
				for i := range branches {
					branches[i] = SchemaNode{Type: "enum", Name: fmt.Sprintf("E%d", i), Symbols: sym}
				}
				return (&SchemaNode{Type: "union", Branches: branches}).Schema()
			})
		})
	})

	// Axis DEPTH — structural and value channels (the 46d4dde / 7f13cf9 cells,
	// driven here too so this battery covers the whole surface).
	t.Run("depth", func(t *testing.T) {
		const deep = maxSchemaJSONDepth + 50
		t.Run("structural", func(t *testing.T) {
			reject(t, "SchemaNode tree nests deeper", func() (*Schema, error) {
				cur := &SchemaNode{Type: "long"}
				for range deep {
					cur = &SchemaNode{Type: "array", Items: cur}
				}
				return cur.Schema()
			})
		})
		t.Run("value", func(t *testing.T) {
			reject(t, "value nests deeper", func() (*Schema, error) {
				var v any = "leaf"
				for range deep {
					v = map[string]any{"k": v}
				}
				return (&SchemaNode{Type: "int", Props: map[string]any{"x": v}}).Schema()
			})
		})
	})

	// Axis NODES — structural and value shared-DAG fan-out (the 885e132 cells).
	t.Run("fanout", func(t *testing.T) {
		t.Run("structural", func(t *testing.T) {
			reject(t, wantNodes, func() (*Schema, error) {
				cur := &SchemaNode{Type: "long"}
				for range 60 {
					cur = &SchemaNode{Type: "array", Items: cur, Values: cur}
				}
				return cur.Schema()
			})
		})
		t.Run("value", func(t *testing.T) {
			reject(t, "value expands", func() (*Schema, error) {
				var v any = "leaf"
				for range 60 {
					inner := v
					v = map[string]any{"a": inner, "b": inner}
				}
				return (&SchemaNode{Type: "int", Props: map[string]any{"x": v}}).Schema()
			})
		})
	})

	// The dedup conflict-comparison marshal (the e76cd84 axis): many large-bodied
	// distinct-pointer duplicates of one named type must be bounded by the SHARED
	// budget, not re-marshalled on a fresh one.
	t.Run("dedup-conflict-marshal", func(t *testing.T) {
		reject(t, "expands to more", func() (*Schema, error) {
			fields := make([]SchemaField, 1500)
			for i := range fields {
				fields[i] = SchemaField{Name: fmt.Sprintf("g%d", i), Type: SchemaNode{Type: "long"}}
			}
			dup := SchemaNode{Type: "record", Name: "Dup", Fields: fields}
			outer := make([]SchemaField, 1500)
			for i := range outer {
				outer[i] = SchemaField{Name: fmt.Sprintf("f%d", i), Type: dup}
			}
			return (&SchemaNode{Type: "record", Name: "Outer", Fields: outer}).Schema()
		})
	})

	// The bare path (SchemaFor's hand-built CustomType.Schema, toJSON with
	// d == nil) truncates over-budget output rather than erroring, so the
	// invariant is that it TERMINATES — no uncatchable stack overflow, no
	// unbounded fan-out, no multi-GB marshal — on every axis.
	t.Run("bare-path-terminates", func(t *testing.T) {
		type recForCustom struct{ F int32 }
		run := func(t *testing.T, sn *SchemaNode) {
			t.Helper()
			done := make(chan struct{})
			go func() {
				defer func() {
					_ = recover()
					close(done)
				}()
				_, _ = SchemaFor[recForCustom](WithCustomType(CustomType{GoType: reflect.TypeOf(int32(0)), Schema: sn}))
			}()
			select {
			case <-done:
			case <-time.After(hangDeadline):
				t.Fatal("bare-path walk did not terminate")
			}
		}
		t.Run("payload-bytes-doc", func(t *testing.T) { run(t, &SchemaNode{Type: "fixed", Name: "F", Size: 1, Doc: huge}) })
		t.Run("payload-bytes-value", func(t *testing.T) { run(t, &SchemaNode{Type: "int", Props: map[string]any{"x": huge}}) })
		t.Run("slice-symbols", func(t *testing.T) {
			run(t, &SchemaNode{Type: "enum", Name: "E", Symbols: make([]string, maxSchemaJSONNodes+1)})
		})
		t.Run("shared-dag-value", func(t *testing.T) {
			var v any = "leaf"
			for range 60 {
				inner := v
				v = map[string]any{"a": inner, "b": inner}
			}
			run(t, &SchemaNode{Type: "int", Props: map[string]any{"x": v}})
		})
	})

	// Boundary — a usable schema well under each bound must still build and
	// round-trip. The bounds reject the pathological, never the merely large.
	t.Run("boundary-usable-builds", func(t *testing.T) {
		t.Run("large-doc", func(t *testing.T) {
			doc := strings.Repeat("d", 1<<20) // 1 MiB, far under the 64 MiB byte budget
			s, err := (&SchemaNode{Type: "record", Name: "R", Doc: doc, Fields: []SchemaField{{Name: "f", Type: SchemaNode{Type: "int"}}}}).Schema()
			if err != nil {
				t.Fatalf("a 1 MiB doc should build, got %v", err)
			}
			if s.Root().Doc != doc {
				t.Fatal("the usable-size doc was dropped or truncated")
			}
		})
		t.Run("many-symbols", func(t *testing.T) {
			syms := make([]string, 10_000) // 10k symbols, far under the 1 MiB node budget
			for i := range syms {
				syms[i] = fmt.Sprintf("S%d", i)
			}
			if _, err := (&SchemaNode{Type: "enum", Name: "E", Symbols: syms}).Schema(); err != nil {
				t.Fatalf("a 10k-symbol enum should build, got %v", err)
			}
		})
		t.Run("benign-shared-payload", func(t *testing.T) {
			// A handful of copies of a moderate string: json.Marshal expands it to
			// a few copies, cheaply. The bound rejects compounding amplification,
			// not all sharing.
			shared := strings.Repeat("x", 1<<10)
			branches := make([]SchemaNode, 8)
			for i := range branches {
				branches[i] = SchemaNode{Type: "fixed", Name: fmt.Sprintf("F%d", i), Size: 1, Doc: shared}
			}
			if _, err := (&SchemaNode{Type: "union", Branches: branches}).Schema(); err != nil {
				t.Fatalf("a benign shared 1 KiB doc across 8 branches should build, got %v", err)
			}
		})
	})
}

// ---------- schema_node_namespace_test.go ----------

// fingerprintRoundTrip parses schema, runs it through the Root().Schema()
// metadata round-trip, and asserts the schema identity (fingerprint) is
// unchanged. The metadata API is the documented way to programmatically
// inspect and re-emit a schema; a fingerprint change means the re-emission
// silently described a different schema.
func fingerprintRoundTrip(t *testing.T, schema string) {
	t.Helper()
	s, err := Parse(schema)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	node := s.Root()
	rt, err := node.Schema()
	if err != nil {
		t.Fatalf("Root().Schema(): %v\ninput: %s", err, schema)
	}
	if want, got := s.Fingerprint(sha256.New()), rt.Fingerprint(sha256.New()); !bytes.Equal(want, got) {
		t.Errorf("fingerprint changed across Root().Schema():\n  orig canonical: %s\n  rt   canonical: %s",
			s.Canonical(), rt.Canonical())
	}
}

// A named child explicitly in the null namespace ("namespace":"") inside a
// namespaced parent is a DIFFERENT type than one inheriting the parent's
// namespace (spec: equality of names is defined on the fullname). The
// re-emission must escape inheritance the way Java's Schema.toString does
// (Name.writeName emits "namespace":"" for a null-namespace name inside a
// non-null enclosing namespace); dropping the escape silently moves the
// child into the parent's namespace.
func TestMatrix_SchemaNodeNullNamespaceEscapeRoundTrip(t *testing.T) {
	cases := []struct{ name, schema string }{
		{"record child", `{"type":"record","name":"P","namespace":"x","fields":[{"name":"c","type":{"type":"record","name":"Child","namespace":"","fields":[{"name":"v","type":"int"}]}}]}`},
		{"enum child", `{"type":"record","name":"P","namespace":"x","fields":[{"name":"e","type":{"type":"enum","name":"E","namespace":"","symbols":["A"]}}]}`},
		{"fixed child", `{"type":"record","name":"P","namespace":"x","fields":[{"name":"f","type":{"type":"fixed","name":"F","namespace":"","size":4}}]}`},
		// inheritance-relying shapes must keep round-tripping too.
		{"inheriting child", `{"type":"record","name":"P","namespace":"x","fields":[{"name":"c","type":{"type":"record","name":"Child","fields":[{"name":"v","type":"int"}]}}]}`},
		{"explicit different ns child", `{"type":"record","name":"P","namespace":"x","fields":[{"name":"c","type":{"type":"record","name":"Child","namespace":"y","fields":[{"name":"v","type":"int"}]}}]}`},
		{"null-ns parent namespaced child", `{"type":"record","name":"P","fields":[{"name":"c","type":{"type":"record","name":"Child","namespace":"y","fields":[{"name":"v","type":"int"}]}}]}`},
		{"deep reinheritance", `{"type":"record","name":"P","namespace":"x","fields":[{"name":"c","type":{"type":"record","name":"Mid","namespace":"","fields":[{"name":"d","type":{"type":"record","name":"Leaf","fields":[{"name":"v","type":"int"}]}}]}}]}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { fingerprintRoundTrip(t, c.schema) })
	}
}

// Two distinct named types may share a short name across namespaces
// (equality is on the fullname). The re-emission dedup must key on the
// fullname: keying on the short name either reports a false "conflicting
// definitions" error (different bodies) or emits a short name reference
// that re-binds to the wrong type (identical bodies).
func TestMatrix_SchemaNodeSameShortNameDistinctNamespaces(t *testing.T) {
	cases := []struct{ name, schema string }{
		{"different bodies", `{"type":"record","name":"P","namespace":"x","fields":[
			{"name":"a","type":{"type":"fixed","name":"T","size":4}},
			{"name":"b","type":{"type":"fixed","name":"T","namespace":"y","size":8}}]}`},
		{"identical bodies", `{"type":"record","name":"P","namespace":"x","fields":[
			{"name":"a","type":{"type":"record","name":"Q","namespace":"y","fields":[
				{"name":"i","type":{"type":"fixed","name":"T","size":4}}]}},
			{"name":"b","type":{"type":"fixed","name":"T","size":4}}]}`},
		{"null-ns vs namespaced", `{"type":"record","name":"P","namespace":"x","fields":[
			{"name":"a","type":{"type":"fixed","name":"T","namespace":"","size":4}},
			{"name":"b","type":{"type":"fixed","name":"T","namespace":"y","size":8}}]}`},
		// genuine same-fullname reuse must still dedup into a reference.
		{"same fullname reused", `{"type":"record","name":"P","namespace":"x","fields":[
			{"name":"a","type":{"type":"fixed","name":"T","size":4}},
			{"name":"b","type":"T"}]}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { fingerprintRoundTrip(t, c.schema) })
	}
}

// ---------- schema_parse_diff_test.go ----------

// buildFromAschema runs the build pipeline on an already-parsed aschema,
// mirroring parse()'s body after the unmarshal so the front-end (old
// json.Unmarshaler vs new parseSchemaTree) is the only variable.
func buildFromAschema(schema string, orig *aschema) (*Schema, error) {
	b := &builder{named: make(map[string]*namedType)}
	if err := b.build("", orig); err != nil {
		return nil, err
	}
	if err := b.finalize(); err != nil {
		return nil, err
	}
	s := &Schema{ser: b.ser, deser: b.deser, c: b.canon, node: b.node, full: schema, custom: b.custom}
	// No SOE header here, matching parse(): the header is hashed on first
	// use from c, which is already set.
	return s, nil
}

// TestDiff_ParseFrontEndEquivalence proves the new O(n) parseSchemaTree
// front-end builds schemas indistinguishable from the old json.Unmarshaler
// front-end across a corpus spanning every schema shape — same Canonical
// form, same fingerprint, same String(). Run BEFORE switching parse() over.
func TestDiff_ParseFrontEndEquivalence(t *testing.T) {
	corpus := []string{
		`"int"`, `"string"`, `"null"`, `"bytes"`, `"boolean"`, `"long"`, `"float"`, `"double"`,
		`{"type":"int"}`,
		`{"type":"fixed","name":"f","size":4}`,
		`{"type":"fixed","name":"f","size":"4"}`,
		`{"type":"enum","name":"e","symbols":["A","B","C"]}`,
		`{"type":"enum","name":"e","symbols":["A"],"default":"A"}`,
		`{"type":"array","items":"int"}`,
		`{"type":"array","items":{"type":"long","logicalType":"timestamp-millis"}}`,
		`{"type":"map","values":"string"}`,
		`["null","int"]`,
		`["null","string","long"]`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`,
		`{"type":"record","name":"R","namespace":"com.x","fields":[{"name":"a","type":"int"}]}`,
		`{"type":"record","name":"R","namespace":"com.x","fields":[{"name":"c","type":{"type":"record","name":"Inner","namespace":"","fields":[{"name":"v","type":"int"}]}}]}`,
		`{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]},{"name":"v","type":"int"}]}`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":"int","default":42},{"name":"b","type":"string","default":"hi"}]}`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":"long","default":9223372036854775807}]}`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":["null","int"],"default":null}]}`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"map","values":"int"},"default":{"x":1,"y":2}}]}`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"array","items":"int"},"default":[1,2,3]}]}`,
		`{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}`,
		`{"type":"fixed","name":"d","size":8,"logicalType":"decimal","precision":18,"scale":4}`,
		`{"type":"string","logicalType":"uuid"}`,
		`{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}`,
		// field-level logicalType lift (the Java/JDBC idiom)
		`{"type":"record","name":"R","fields":[{"name":"ts","type":"long","logicalType":"timestamp-millis"}]}`,
		`{"type":"record","name":"R","fields":[{"name":"ts","type":["null","long"],"logicalType":"timestamp-millis"}]}`,
		`{"type":"record","name":"R","fields":[{"name":"d","type":"bytes","logicalType":"decimal","precision":9,"scale":2}]}`,
		// flat (goavro) field format
		`{"type":"record","name":"R","fields":[{"name":"e","type":"enum","symbols":["A","B"]}]}`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":"array","items":"int"}]}`,
		`{"type":"record","name":"R","fields":[{"name":"m","type":"map","values":"long"}]}`,
		`{"type":"record","name":"R","fields":[{"name":"f","type":"fixed","size":4,"name":"Inner"}]}`,
		// extras / custom props
		`{"type":"record","name":"R","com.acme.tag":"hello","extra":123,"bignum":99999999999999999999,"fields":[{"name":"a","type":"int"}]}`,
		`{"type":"int","custom.float":1.5,"custom.bool":true,"custom.arr":[1,2],"custom.obj":{"k":"v"}}`,
		// case-insensitive keys
		`{"TYPE":"record","NAME":"R","FIELDS":[{"NAME":"a","TYPE":"int"}]}`,
		`{"Type":"fixed","Name":"f","Size":4}`,
		// doc (dropped by aobject), aliases, order
		`{"type":"record","name":"R","doc":"hi","aliases":["Old"],"fields":[{"name":"a","type":"int","doc":"fld","order":"descending","aliases":["x"]}]}`,
		// wrapped name ref
		`{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"fixed","name":"F","size":2}},{"name":"b","type":"F"}]}`,
		// duplicate keys (last-wins)
		`{"type":"int","type":"string"}`,
		// nested deep
		`{"type":"array","items":{"type":"array","items":{"type":"map","values":["null","int"]}}}`,
	}

	for _, schema := range corpus {
		t.Run(schema, func(t *testing.T) {
			// OLD front-end (current parse()).
			sOld, errOld := Parse(schema)
			// NEW front-end.
			treeNew, errNew := parseSchemaTree(schema)
			var sNew *Schema
			if errNew == nil {
				sNew, errNew = buildFromAschema(schema, treeNew)
			}

			if (errOld == nil) != (errNew == nil) {
				t.Fatalf("error mismatch: old=%v new=%v", errOld, errNew)
			}
			if errOld != nil {
				return // both errored; ok
			}
			if co, cn := string(sOld.Canonical()), string(sNew.Canonical()); co != cn {
				t.Errorf("canonical differs:\n old: %s\n new: %s", co, cn)
			}
			if fo, fn := fmt.Sprintf("%x", sOld.Fingerprint(sha256.New())), fmt.Sprintf("%x", sNew.Fingerprint(sha256.New())); fo != fn {
				t.Errorf("fingerprint differs: old=%s new=%s", fo, fn)
			}
			if so, sn := sOld.String(), sNew.String(); so != sn {
				t.Errorf("String differs:\n old: %s\n new: %s", so, sn)
			}
		})
	}
}

// ---------- compat_test.go ----------

func TestNodeKind(t *testing.T) {
	if got := nodeKind(nil); got != "<nil>" {
		t.Fatalf("expected <nil>, got %s", got)
	}
	n := &schemaNode{kind: "int"}
	if got := nodeKind(n); got != "int" {
		t.Fatalf("expected int, got %s", got)
	}
}

func TestCheckCompatNilSchema(t *testing.T) {
	seen := make(map[nodePair]bool)
	err := checkCompat(nil, &schemaNode{kind: "int"}, "", seen)
	if err == nil {
		t.Fatal("expected error for nil reader")
	}
	err = checkCompat(&schemaNode{kind: "int"}, nil, "", seen)
	if err == nil {
		t.Fatal("expected error for nil writer")
	}
}

func TestCheckCompatCycleDetection(t *testing.T) {
	// Pre-populate seen to trigger the cycle return.
	r := &schemaNode{kind: "record", name: "R", fields: nil}
	w := &schemaNode{kind: "record", name: "R", fields: nil}
	seen := make(map[nodePair]bool)
	seen[nodePair{r, w}] = true
	err := checkCompat(r, w, "", seen)
	if err != nil {
		t.Fatalf("expected nil for cycle, got %v", err)
	}
}

func TestCompatibilityErrorFormat(t *testing.T) {
	e := &CompatibilityError{
		Path:       "User.address.zip",
		ReaderType: "string",
		WriterType: "int",
		Detail:     "incompatible types",
	}
	want := "avro: incompatible at User.address.zip: reader string vs writer int: incompatible types"
	if got := e.Error(); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestCheckWriterUnionDeepRecursionFailure(t *testing.T) {
	// Both reader and writer are unions. kindsMatch passes for all branches
	// (records have the same name), but deep compatibility check fails because
	// a reader field has no default and is missing from writer.
	reader := mustParse(t, `["null", {"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}
	]}]`)
	writer := mustParse(t, `["null", {"type":"record","name":"R","fields":[
		{"name":"a","type":"int"}
	]}]`)
	err := CheckCompatibility(writer, reader)
	if err == nil {
		t.Fatal("expected error for deep record incompatibility in union")
	}
}

func TestCheckCompatibility(t *testing.T) {
	tests := []struct {
		name    string
		reader  string
		writer  string
		wantErr bool
	}{
		{
			name:   "identical primitives",
			reader: `"int"`,
			writer: `"int"`,
		},
		{
			name:   "int promoted to long",
			reader: `"long"`,
			writer: `"int"`,
		},
		{
			name:   "int promoted to float",
			reader: `"float"`,
			writer: `"int"`,
		},
		{
			name:   "int promoted to double",
			reader: `"double"`,
			writer: `"int"`,
		},
		{
			name:   "long promoted to float",
			reader: `"float"`,
			writer: `"long"`,
		},
		{
			name:   "long promoted to double",
			reader: `"double"`,
			writer: `"long"`,
		},
		{
			name:   "float promoted to double",
			reader: `"double"`,
			writer: `"float"`,
		},
		{
			name:   "string to bytes",
			reader: `"bytes"`,
			writer: `"string"`,
		},
		{
			name:   "bytes to string",
			reader: `"string"`,
			writer: `"bytes"`,
		},
		{
			name:    "incompatible primitives",
			reader:  `"int"`,
			writer:  `"string"`,
			wantErr: true,
		},
		{
			name:    "incompatible long to int (no demotion)",
			reader:  `"int"`,
			writer:  `"long"`,
			wantErr: true,
		},
		{
			name:   "identical records",
			reader: `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`,
			writer: `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`,
		},
		{
			name:   "reader has new field with default",
			reader: `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string","default":"hello"}]}`,
			writer: `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`,
		},
		{
			name:    "reader has new field without default",
			reader:  `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`,
			writer:  `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`,
			wantErr: true,
		},
		{
			name:   "writer has extra field (removed in reader)",
			reader: `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`,
			writer: `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`,
		},
		{
			name:   "field type promoted",
			reader: `{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}`,
			writer: `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`,
		},
		{
			name:    "record name mismatch",
			reader:  `{"type":"record","name":"A","fields":[{"name":"a","type":"int"}]}`,
			writer:  `{"type":"record","name":"B","fields":[{"name":"a","type":"int"}]}`,
			wantErr: true,
		},
		{
			name:   "record matched by alias",
			reader: `{"type":"record","name":"A","aliases":["B"],"fields":[{"name":"a","type":"int"}]}`,
			writer: `{"type":"record","name":"B","fields":[{"name":"a","type":"int"}]}`,
		},
		{
			name:   "identical enums",
			reader: `{"type":"enum","name":"E","symbols":["A","B","C"]}`,
			writer: `{"type":"enum","name":"E","symbols":["A","B","C"]}`,
		},
		{
			name:   "reader enum is superset",
			reader: `{"type":"enum","name":"E","symbols":["A","B","C","D"]}`,
			writer: `{"type":"enum","name":"E","symbols":["A","B","C"]}`,
		},
		{
			name:    "writer enum has unknown symbol, no default",
			reader:  `{"type":"enum","name":"E","symbols":["A","B"]}`,
			writer:  `{"type":"enum","name":"E","symbols":["A","B","C"]}`,
			wantErr: true,
		},
		{
			name:   "writer enum has unknown symbol with default",
			reader: `{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}`,
			writer: `{"type":"enum","name":"E","symbols":["A","B","C"]}`,
		},
		{
			name:   "identical arrays",
			reader: `{"type":"array","items":"int"}`,
			writer: `{"type":"array","items":"int"}`,
		},
		{
			name:   "array with promoted items",
			reader: `{"type":"array","items":"long"}`,
			writer: `{"type":"array","items":"int"}`,
		},
		{
			name:   "identical maps",
			reader: `{"type":"map","values":"string"}`,
			writer: `{"type":"map","values":"string"}`,
		},
		{
			name:   "map with promoted values",
			reader: `{"type":"map","values":"double"}`,
			writer: `{"type":"map","values":"float"}`,
		},
		{
			name:   "identical fixed",
			reader: `{"type":"fixed","name":"F","size":4}`,
			writer: `{"type":"fixed","name":"F","size":4}`,
		},
		{
			name:    "fixed size mismatch",
			reader:  `{"type":"fixed","name":"F","size":4}`,
			writer:  `{"type":"fixed","name":"F","size":8}`,
			wantErr: true,
		},
		{
			name:   "writer union, reader matches all branches",
			reader: `["null","int"]`,
			writer: `["null","int"]`,
		},
		{
			name:   "reader union, writer is branch member",
			reader: `["null","int","string"]`,
			writer: `"int"`,
		},
		{
			name:    "reader union, writer not in any branch",
			reader:  `["null","int"]`,
			writer:  `"string"`,
			wantErr: true,
		},
		{
			name:   "nested record compatibility",
			reader: `{"type":"record","name":"Outer","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"long"}]}}]}`,
			writer: `{"type":"record","name":"Outer","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}}]}`,
		},
		// Additional edge cases:
		{
			name:    "enum name mismatch",
			reader:  `{"type":"enum","name":"E1","symbols":["A","B"]}`,
			writer:  `{"type":"enum","name":"E2","symbols":["A","B"]}`,
			wantErr: true,
		},
		{
			name:    "fixed name mismatch",
			reader:  `{"type":"fixed","name":"F1","size":4}`,
			writer:  `{"type":"fixed","name":"F2","size":4}`,
			wantErr: true,
		},
		{
			name:    "writer union branch not in reader union",
			reader:  `["null","int"]`,
			writer:  `["null","string"]`,
			wantErr: true,
		},
		{
			name:   "self-referencing record compatibility",
			reader: `{"type":"record","name":"Node","fields":[{"name":"v","type":"int"},{"name":"n","type":["null","Node"]}]}`,
			writer: `{"type":"record","name":"Node","fields":[{"name":"v","type":"int"},{"name":"n","type":["null","Node"]}]}`,
		},
		{
			name:   "kindsMatch promotion in union branch",
			reader: `["null","long"]`,
			writer: `"int"`,
		},
		{
			name:    "decimal precision mismatch",
			reader:  `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`,
			writer:  `{"type":"bytes","logicalType":"decimal","precision":8,"scale":2}`,
			wantErr: true,
		},
		{
			name:    "decimal scale mismatch",
			reader:  `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`,
			writer:  `{"type":"bytes","logicalType":"decimal","precision":10,"scale":4}`,
			wantErr: true,
		},
		{
			name:   "decimal same precision and scale",
			reader: `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`,
			writer: `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader, err := Parse(tt.reader)
			if err != nil {
				t.Fatalf("reader schema: %v", err)
			}
			writer, err := Parse(tt.writer)
			if err != nil {
				t.Fatalf("writer schema: %v", err)
			}
			err = CheckCompatibility(writer, reader)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				var ce *CompatibilityError
				if !errors.As(err, &ce) {
					t.Fatalf("expected *CompatibilityError, got %T: %v", err, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestNamesMatchUnqualified(t *testing.T) {
	// Same unqualified name, different namespaces should be compatible.
	reader := mustParse(t, `{"type":"record","name":"R","namespace":"com.a","fields":[{"name":"x","type":"int"}]}`)
	writer := mustParse(t, `{"type":"record","name":"R","namespace":"com.b","fields":[{"name":"x","type":"int"}]}`)
	if err := CheckCompatibility(writer, reader); err != nil {
		t.Fatalf("expected compatible by unqualified name, got %v", err)
	}
}

// ---------- node_lookup_table_test.go ----------

// The per-value name lookups, and the two things that can go wrong with them.
//
// A union's tag table and an enum's symbol index are the tier rule and the
// symbol list applied ONCE, at parse time, so both encoders and the JSON decoder
// can answer "which branch / which ordinal is this name?" without walking the
// siblings per value. Two failures follow: the table ANSWERS DIFFERENTLY from
// the walk it stands in for, a correctness change wearing a performance change's
// clothes; or a node that carries the sibling slice does NOT carry its table,
// which is silent — every consumer still gets the right answer by scanning, and
// only the cost tells you. Nodes SYNTHESIZED during resolution are where that
// bites, copying the reader's siblings by reference while the table is a
// separate field. The first is checked by asking both; the second by walking
// every node a parse or resolve can produce and refusing a bare one.

// tagProbeNames returns every name worth asking a union about: each branch's
// own spelling in each tier's form, plus names nothing can claim.
func tagProbeNames(u *schemaNode) []string {
	names := []string{"", "nope", "x.nope", "int", "null", "long.timestamp-millis", "fixed.uuid", "bytes.decimal"}
	for _, b := range u.branches {
		if b == nil {
			continue
		}
		names = append(names, b.kind, b.name, unqualified(b.name))
		if b.logical != "" {
			names = append(names, b.kind+"."+b.logical, unqualified(b.name)+"."+b.logical)
		}
		names = append(names, b.aliases...)
		names = append(names, b.bareAliases...)
	}
	return names
}

// unionTagCorpus spans the shapes the tier walk distinguishes: plain kinds, a
// logical qualifier a sibling's exact name owns, namespaced names whose short
// forms collide, forward references (whose table is rebuilt at finalize, so it
// is the shape that can go stale), and recursion.
func unionTagCorpus() []string {
	return []string{
		`["null","int","string"]`,
		`["null",{"type":"long","logicalType":"timestamp-millis"}]`,
		// The qualifier a logical branch would emit is also a legal fullname,
		// so this pair puts the two spellings in one namespace. "decimal" is
		// the collision that can be WRITTEN — a hyphenated logical type has no
		// valid name spelling, so no fixed can claim its qualifier.
		`[{"type":"bytes","logicalType":"decimal","precision":4,"scale":2},{"type":"fixed","name":"bytes.decimal","size":2}]`,
		`[{"type":"fixed","name":"a.F","size":16,"logicalType":"uuid"},{"type":"fixed","name":"b.G","size":16,"logicalType":"uuid"}]`,
		`[{"type":"record","name":"a.R","fields":[]},{"type":"record","name":"b.R","fields":[]}]`,
		`[{"type":"record","name":"a.Q","aliases":["a.R","R"],"fields":[]}]`,
		`[{"type":"enum","name":"E","symbols":["A","B"]},"string"]`,
		`["null",{"type":"map","values":"int"},{"type":"array","items":"int"}]`,
		`[]`,
		// Forward reference: buildUnion tables it under the as-written name and
		// finalizeUnionNames rebuilds over the resolved node. The table the
		// consumers hold has to be the rebuilt one.
		`{"type":"record","name":"Top","fields":[
			{"name":"a","type":["null","Inner"]},
			{"name":"b","type":{"type":"record","name":"Inner","fields":[{"name":"q","type":"int"}]}}]}`,
		// Recursive: the union's branch is the enclosing record.
		`{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]}]}`,
		// An enum wide enough to cross the index threshold, and one below it,
		// so both arms of the symbol lookup are driven.
		wideEnumText("Wide", enumIndexMin+4, ""),
		wideEnumText("Narrow", 2, ""),
	}
}

func wideEnumText(name string, n int, extra string) string {
	out := `{"type":"enum","name":"` + name + `","symbols":[`
	for i := range n {
		if i > 0 {
			out += ","
		}
		out += fmt.Sprintf(`"S%d"`, i)
	}
	return out + `]` + extra + `}`
}

// TestInvariant_UnionTagTableMatchesTheTierWalk asks the table and the walk the
// same names and requires the same branch. The walk is the rule; the table is
// the rule precomputed, and precomputing it may not change what it accepts.
func TestInvariant_UnionTagTableMatchesTheTierWalk(t *testing.T) {
	unions, probes := 0, 0
	for _, text := range unionTagCorpus() {
		s, err := Parse(text)
		if err != nil {
			t.Errorf("corpus entry does not parse, so it drives nothing: %v\n  %s", err, text)
			continue
		}
		forEachSchemaNode(s.node, func(n *schemaNode) {
			if n.kind != "union" {
				return
			}
			unions++
			if n.tags == nil {
				t.Errorf("%s: a parsed union node has no tag table", text)
				return
			}
			for _, name := range tagProbeNames(n) {
				probes++
				want := scanUnionBranch(n, name)
				if got := findUnionBranch(n, name); got != want {
					t.Errorf("%s: name %q\n  tier walk -> %s\n  table     -> %s",
						text, name, nodeDesc(want), nodeDesc(got))
				}
			}
		})
	}
	if unions == 0 || probes == 0 {
		t.Fatalf("the corpus drove %d unions and %d probes; it is not exercising the table", unions, probes)
	}
	t.Logf("unions=%d probes=%d", unions, probes)
}

// TestInvariant_EnumSymbolIndexMatchesTheScan is the same claim for the enum
// half: the index and the symbol slice must name the same ordinal, on both
// sides of the size threshold that decides whether an index exists at all.
func TestInvariant_EnumSymbolIndexMatchesTheScan(t *testing.T) {
	enums, probes := 0, 0
	for _, text := range unionTagCorpus() {
		s, err := Parse(text)
		if err != nil {
			t.Errorf("corpus entry does not parse: %v\n  %s", err, text)
			continue
		}
		forEachSchemaNode(s.node, func(n *schemaNode) {
			if n.kind != "enum" {
				return
			}
			enums++
			for i, sym := range append(append([]string{}, n.symbols...), "nope", "") {
				probes++
				gotIdx, gotOK := n.symbolIndex(sym)
				wantIdx, wantOK := -1, false
				for j, s := range n.symbols {
					if s == sym {
						wantIdx, wantOK = j, true
						break
					}
				}
				if gotOK != wantOK || (wantOK && gotIdx != wantIdx) {
					t.Errorf("%s: symbol %q (probe %d): index says (%d,%v), the symbol slice says (%d,%v)",
						text, sym, i, gotIdx, gotOK, wantIdx, wantOK)
				}
			}
		})
	}
	if enums == 0 {
		t.Fatal("the corpus drove no enum node")
	}
	t.Logf("enums=%d probes=%d", enums, probes)
}

// resolveSynthesizedNode runs resolution at the NODE level, which is the only
// place the synthesized nodes are observable. Resolve returns a Schema whose node
// field is the READER's node and keeps only the resolved tree's deser closure
// (resolve.go), so a check that walks Resolve's result walks the PARSE's output:
// it would pass with every resolved node built bare, which is what a neuter of the
// carry proved before this helper existed. Driving resolveNode directly is what
// makes the assertion below about the nodes it names.
func resolveSynthesizedNode(t *testing.T, writer, reader *Schema) *schemaNode {
	t.Helper()
	ctx := &resolveCtx{seen: make(map[nodePair]*schemaNode), custom: reader.custom}
	nd, err := resolveNode(reader.node, writer.node, "", ctx)
	if err != nil {
		t.Fatalf("resolveNode: %v", err)
	}
	if nd == reader.node {
		t.Fatalf("resolution returned the reader's own node, so nothing was synthesized and this case checks the parse path")
	}
	return nd
}

// resolvedNodeCases are writer/reader pairs chosen so resolution SYNTHESIZES a
// union or enum node on each of its paths — union-vs-union, union-vs-non-union,
// and a symbol-remapping enum. Those nodes carry the reader's siblings, so they
// must carry the reader's tables too.
var resolvedNodeCases = []struct{ name, writer, reader string }{
	{
		"union writer, union reader",
		`["null","int"]`,
		`["null","long","string"]`,
	},
	{
		"non-union writer, union reader",
		`"int"`,
		`["null","long"]`,
	},
	{
		"union of named types both sides",
		`[{"type":"record","name":"a.R","fields":[{"name":"q","type":"int"}]}]`,
		`["null",{"type":"record","name":"a.R","fields":[{"name":"q","type":"long"}]}]`,
	},
	{
		"enum with a writer symbol the reader defaults",
		`{"type":"enum","name":"E","symbols":["A","B","C"]}`,
		`{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}`,
	},
	{
		"wide enum past the index threshold, resolved",
		wideEnumText("E", enumIndexMin+4, ""),
		wideEnumText("E", enumIndexMin+6, `,"default":"S0"`),
	},
	{
		"record field carrying a union, resolved",
		`{"type":"record","name":"R","fields":[{"name":"u","type":["null","int"]}]}`,
		`{"type":"record","name":"R","fields":[{"name":"u","type":["null","long","string"]}]}`,
	},
}

// TestInvariant_EveryUnionNodeCarriesItsTagTable is the ownership half. A node
// that holds the sibling slice but not the lookup sends every consumer back to
// scanning it — correct, and linear in a count the schema's author chooses.
// Resolution builds fresh nodes around the reader's slices, which is exactly
// where a table can be left behind.
func TestInvariant_EveryUnionNodeCarriesItsTagTable(t *testing.T) {
	check := func(t *testing.T, label string, root *schemaNode) {
		t.Helper()
		unions, enums := 0, 0
		forEachSchemaNode(root, func(n *schemaNode) {
			switch n.kind {
			case "union":
				unions++
				if n.tags == nil {
					t.Errorf("%s: union node carries %d branches and no tag table — every consumer falls back to the tier walk, once per value",
						label, len(n.branches))
					return
				}
				// Not merely present: the SAME answers a fresh build gives. A
				// stale table (one finalize never rebuilt) or one copied from a
				// different union is present and wrong.
				fresh := new(unionTags)
				std := unionStandardNames(n.branches)
				log := make([]string, len(n.branches))
				for i, b := range n.branches {
					if b != nil {
						_, log[i] = unionBranchNames(b)
					}
				}
				fillUnionTagTables(fresh, new(deserUnion), n.branches, std, log)
				if len(fresh.byName) != len(n.tags.byName) {
					t.Errorf("%s: union tag table holds %d names, a fresh build holds %d — the table is stale",
						label, len(n.tags.byName), len(fresh.byName))
					return
				}
				for name, idx := range fresh.byName {
					if got, ok := n.tags.byName[name]; !ok || got != idx {
						t.Errorf("%s: tag %q resolves to branch %d, a fresh build says %d (present=%v)",
							label, name, got, idx, ok)
					}
				}
			case "enum":
				enums++
				want := enumSymbolIndex(n.symbols)
				if (want == nil) != (n.symbolIdx == nil) {
					t.Errorf("%s: enum %q has %d symbols; symbolIdx present=%v, want present=%v (threshold %d)",
						label, n.name, len(n.symbols), n.symbolIdx != nil, want != nil, enumIndexMin)
					return
				}
				for sym, idx := range want {
					if got, ok := n.symbolIdx[sym]; !ok || got != idx {
						t.Errorf("%s: enum %q symbol %q -> %d, want %d (present=%v)", label, n.name, sym, got, idx, ok)
					}
				}
			}
		})
		if unions+enums == 0 {
			t.Errorf("%s: walked no union or enum node at all — the case is not reaching what it claims to check", label)
		}
	}

	for _, text := range unionTagCorpus() {
		s, err := Parse(text)
		if err != nil {
			t.Errorf("corpus entry does not parse: %v\n  %s", err, text)
			continue
		}
		check(t, "parsed "+text, s.node)
	}
	for _, tc := range resolvedNodeCases {
		t.Run(tc.name, func(t *testing.T) {
			w, r := MustParse(tc.writer), MustParse(tc.reader)
			if err := CheckCompatibility(w, r); err != nil {
				t.Fatalf("the pair does not resolve, so the case drives nothing: %v", err)
			}
			check(t, "resolved "+tc.name, resolveSynthesizedNode(t, w, r))
		})
	}
}

// TestRegression_ResolvedUnionCarriesTheReaderTagTable pins the same claim on
// one shape: the node resolveUnionUnion builds around the reader's branch slice.
// The assertion is on the TABLE rather than a decoded value because both answers
// are identical either way — only the cost differs, which a value assertion
// cannot see. The carry is defense in depth TODAY, since Resolve keeps the
// resolved tree's deser and no walker currently reaches this node, but it is
// pinned because the invariant a consumer will rely on is "a node that carries
// siblings carries their table".
func TestRegression_ResolvedUnionCarriesTheReaderTagTable(t *testing.T) {
	w := MustParse(`[{"type":"record","name":"a.R","fields":[{"name":"q","type":"int"}]},"null"]`)
	r := MustParse(`["null",{"type":"record","name":"a.R","fields":[{"name":"q","type":"long"}]},"string"]`)
	n := resolveSynthesizedNode(t, w, r)
	if n.kind != "union" {
		t.Fatalf("resolution produced a %q node, want a union — the probe is not reaching the synthesized node", n.kind)
	}
	if n.tags == nil {
		t.Fatal("the resolved union carries the reader's branches without the reader's tag table")
	}
	// The table addresses the slice the node holds, so a name must land on the
	// branch that slice has at that index.
	idx, ok := n.tags.byName["a.R"]
	if !ok {
		t.Fatal(`the resolved union's table does not resolve "a.R"`)
	}
	if idx < 0 || idx >= len(n.branches) {
		t.Fatalf("table index %d is out of range for %d branches — the table belongs to a different union", idx, len(n.branches))
	}
	if got := n.branches[idx]; got == nil || got.name != "a.R" {
		t.Fatalf(`table sent "a.R" to branch %d, which is %s`, idx, nodeDesc(got))
	}
	if findUnionBranch(n, "a.R") != scanUnionBranch(n, "a.R") {
		t.Error("the resolved union's table and the tier walk disagree")
	}
}

// forEachSchemaNode visits every schemaNode reachable from root exactly once.
// Recursive schemas point back at themselves, so the visited set is what makes
// this terminate rather than a depth bound.
func forEachSchemaNode(root *schemaNode, fn func(*schemaNode)) {
	seen := map[*schemaNode]bool{}
	var walk func(*schemaNode)
	walk = func(n *schemaNode) {
		if n == nil || seen[n] {
			return
		}
		seen[n] = true
		fn(n)
		walk(n.items)
		walk(n.values)
		for _, b := range n.branches {
			walk(b)
		}
		for i := range n.fields {
			walk(n.fields[i].node)
		}
	}
	walk(root)
}

// ---------- node_ref_schema_test.go ----------

// These tests pin issue #42: a SchemaNode extracted from Schema.Root whose
// Type is a NAME REFERENCE (the definition lives elsewhere in the enclosing
// schema — an earlier field, a prior SchemaCache.Parse, or the enclosing
// record itself) must still convert via SchemaNode.Schema. The resulting
// schema must be self-contained and equal, canonical-bytes and wire, to a
// from-scratch Parse of the equivalent standalone schema text.

// requireSubSchema converts node via Schema(), requires success, and
// requires canonical + wire equality with the standalone schema text want.
// val is a value encodable by the schema, exercised in both directions so
// a metadata-only match cannot mask a diverged codec.
func requireSubSchema(t *testing.T, node *SchemaNode, want string, val any) *Schema {
	t.Helper()
	got, err := node.Schema()
	if err != nil {
		t.Fatalf("SchemaNode.Schema() failed: %v", err)
	}
	ws := MustParse(want)
	if !bytes.Equal(got.Canonical(), ws.Canonical()) {
		t.Fatalf("canonical mismatch:\n got: %s\nwant: %s", got.Canonical(), ws.Canonical())
	}
	enc, err := got.Encode(val)
	if err != nil {
		t.Fatalf("encode with sub-schema: %v", err)
	}
	wantEnc, err := ws.Encode(val)
	if err != nil {
		t.Fatalf("encode with standalone schema: %v", err)
	}
	if !bytes.Equal(enc, wantEnc) {
		t.Fatalf("wire mismatch: got %x want %x", enc, wantEnc)
	}
	var rt any
	if _, err := ws.Decode(enc, &rt); err != nil {
		t.Fatalf("standalone schema cannot decode sub-schema bytes: %v", err)
	}
	return got
}

const addrDef = `{"type":"record","name":"com.example.Address","fields":[{"name":"street","type":"string"}]}`

var addrVal = map[string]any{"street": "main"}

// Case 1 (the issue's exact flow): reference to a type defined in a prior
// SchemaCache.Parse call.
func TestNodeRefSchema_CacheCrossParse(t *testing.T) {
	var c SchemaCache
	mustCacheParse(t, &c, addrDef)
	person := mustCacheParse(t, &c, `{"type":"record","name":"com.example.Person","fields":[
		{"name":"home","type":"com.example.Address"},
		{"name":"work","type":"com.example.Address"}]}`)
	root := person.Root()

	// Control: the first occurrence carries the spliced definition and
	// already works today.
	requireSubSchema(t, &root.Fields[0].Type, addrDef, addrVal)

	// The regression: the second occurrence is a bare reference.
	requireSubSchema(t, &root.Fields[1].Type, addrDef, addrVal)
}

// Case 2: same failure with no cache — second occurrence within one schema.
func TestNodeRefSchema_SecondOccurrence(t *testing.T) {
	s := MustParse(`{"type":"record","name":"P","fields":[
		{"name":"a","type":` + addrDef + `},
		{"name":"b","type":"com.example.Address"}]}`)
	root := s.Root()
	requireSubSchema(t, &root.Fields[1].Type, addrDef, addrVal)
}

// Case 3: recursive type — a union branch references its own container.
// Extracting the union must yield ["null", <full Node definition>], and
// extracting the branch must yield the Node definition itself.
func TestNodeRefSchema_Recursive(t *testing.T) {
	const nodeDef = `{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]}]}`
	s := MustParse(nodeDef)
	root := s.Root()
	val := map[string]any{"next": nil}

	union := &root.Fields[0].Type
	requireSubSchema(t, union, `["null",`+nodeDef+`]`, map[string]any{"null": nil})
	requireSubSchema(t, &union.Branches[1], nodeDef, val)
}

// Forward reference control: a reference appearing BEFORE its local
// definition parses today via SchemaNode.Schema on the root, and must keep
// doing so. Extracting the forward-ref field's type must also work.
func TestNodeRefSchema_ForwardRefControl(t *testing.T) {
	const fixedDef = `{"type":"fixed","name":"F","size":4}`
	s := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"F"},
		{"name":"b","type":` + fixedDef + `}]}`)
	root := s.Root()

	// Whole-root round trip (works today; must not regress).
	rs, err := root.Schema()
	if err != nil {
		t.Fatalf("root.Schema() with forward ref: %v", err)
	}
	if !bytes.Equal(rs.Canonical(), s.Canonical()) {
		t.Fatalf("root canonical drifted:\n got: %s\nwant: %s", rs.Canonical(), s.Canonical())
	}

	// The forward-ref field node itself.
	requireSubSchema(t, &root.Fields[0].Type, fixedDef, []byte{1, 2, 3, 4})
}

// Hand-built nodes have no enclosing schema; a dangling reference must
// keep failing loudly rather than resolving against anything.
func TestNodeRefSchema_HandBuiltStillDangles(t *testing.T) {
	n := SchemaNode{Type: "com.example.Address"}
	if _, err := n.Schema(); err == nil {
		t.Fatal("hand-built dangling reference unexpectedly parsed")
	}
}

// The refTarget stamp is hidden state that survives a struct copy, which is
// exactly how a caller extracts a sub-node. If the caller then edits the node's
// exported Type, the stamp is STALE: it still points at whatever the ORIGINAL
// spelling named, and honoring it would let hidden state silently beat the
// exported field the caller just set. So the stamp is used only while it still
// names the node's Type; an edited node behaves hand-built.
func TestNodeRefSchema_EditedTypeIgnoresStaleStamp(t *testing.T) {
	const twoNamed = `{"type":"record","name":"R","fields":[
		{"name":"f","type":{"type":"fixed","name":"Dec","size":8}},
		{"name":"g","type":"Dec"}]}`

	t.Run("retyped-to-primitive", func(t *testing.T) {
		root := MustParse(twoNamed).Root()
		g := root.Fields[1].Type // struct copy: carries the stamp
		if g.Type != "Dec" {
			t.Fatalf("precondition: extracted node Type = %q", g.Type)
		}
		g.Type = "int"
		sub, err := g.Schema()
		if err != nil {
			t.Fatalf("Schema() after retyping to a primitive: %v", err)
		}
		if got := string(sub.Canonical()); got != `"int"` {
			t.Fatalf("retyped node produced %s, want \"int\" — the stale stamp resurrected the old definition", got)
		}
		// And it must actually encode as an int.
		wire, err := sub.Encode(int32(1))
		if err != nil {
			t.Fatalf("encode int: %v", err)
		}
		if !bytes.Equal(wire, []byte{2}) {
			t.Fatalf("int wire = %v, want [2]", wire)
		}
	})

	t.Run("redirected-to-another-name", func(t *testing.T) {
		root := MustParse(`{"type":"record","name":"R","fields":[
			{"name":"a","type":{"type":"fixed","name":"A","size":1}},
			{"name":"b","type":{"type":"fixed","name":"B","size":2}},
			{"name":"c","type":"A"}]}`).Root()
		c := root.Fields[2].Type
		c.Type = "B" // the caller redirects the reference
		// The tree being converted defines neither name, so an as-written
		// reference dangles — the hand-built posture. What must NOT happen
		// is silently emitting A's definition under the new spelling.
		sub, err := c.Schema()
		if err == nil {
			t.Fatalf("redirected reference produced %s; it names B, which this tree does not define, so it must dangle loudly like a hand-built node", sub.Canonical())
		}
	})

	t.Run("unedited-still-splices", func(t *testing.T) {
		// The control: the whole point of the stamp must still work.
		root := MustParse(twoNamed).Root()
		requireSubSchema(t, &root.Fields[1].Type, `{"type":"fixed","name":"Dec","size":8}`, []byte{1, 2, 3, 4, 5, 6, 7, 8})
	})

	t.Run("retyped-to-a-name-the-tree-defines", func(t *testing.T) {
		// Editing Type to a name the CONVERTED TREE defines must bind to
		// that local definition, not to the stamp.
		root := MustParse(twoNamed).Root()
		rec := root // the whole record defines "Dec" locally
		rec.Fields[1].Type.Type = "Dec"
		sub := mustNodeSchema(t, rec)
		if !bytes.Equal(sub.Canonical(), MustParse(twoNamed).Canonical()) {
			t.Fatalf("locally-defined name drifted: %s", sub.Canonical())
		}
	})
}

// Whether an extracted reference node still names its stamped target is a
// question the name resolver already owns: scopedRefKeys decides which spellings
// bind, and it admits three — a fullname, a short name qualified by the
// enclosing namespace, and the leading-dot null-namespace escape — resolving the
// short form against the target's RESOLVED FULLNAME, which is not the same
// string as its Name field when the definition writes its name dotted. Every
// spelling the resolver binds must therefore convert; a guard that re-lists the
// accepted spellings by hand under-accepts, and the node it wrongly calls stale
// emits a dangling reference that fails its own re-parse. The scope asked at is
// the scope the reference was WRITTEN in, not the converted tree's.
func TestNodeRefSchema_EverySpellingTheResolverBindsConverts(t *testing.T) {
	fooVal := map[string]any{"x": int32(1)}

	for _, tc := range []struct {
		name    string
		enclose string // enclosing schema; field "b" holds the reference
		want    string // equivalent standalone definition
	}{
		{
			name: "fullname",
			enclose: `{"type":"record","name":"ns.Top","fields":[
				{"name":"a","type":{"type":"record","name":"Foo","namespace":"ns","fields":[{"name":"x","type":"int"}]}},
				{"name":"b","type":"ns.Foo"}]}`,
			want: `{"type":"record","name":"Foo","namespace":"ns","fields":[{"name":"x","type":"int"}]}`,
		},
		{
			name: "short-name-with-namespace-attribute",
			enclose: `{"type":"record","name":"ns.Top","fields":[
				{"name":"a","type":{"type":"record","name":"Foo","namespace":"ns","fields":[{"name":"x","type":"int"}]}},
				{"name":"b","type":"Foo"}]}`,
			want: `{"type":"record","name":"Foo","namespace":"ns","fields":[{"name":"x","type":"int"}]}`,
		},
		{
			// The definition's name is written as a dotted fullname, so its
			// Name field holds "ns.Foo" while the reference spells "Foo".
			// Comparing the reference against the Name field misses this;
			// comparing against the resolved fullname, as the resolver does,
			// binds it.
			name: "short-name-against-a-dotted-definition-name",
			enclose: `{"type":"record","name":"ns.Top","fields":[
				{"name":"a","type":{"type":"record","name":"ns.Foo","fields":[{"name":"x","type":"int"}]}},
				{"name":"b","type":"Foo"}]}`,
			want: `{"type":"record","name":"ns.Foo","fields":[{"name":"x","type":"int"}]}`,
		},
		{
			// ".Foo" is the explicit null-namespace escape: an exact lookup
			// of the null-namespace fullname, never qualified into the
			// enclosing namespace.
			name: "leading-dot-null-namespace-escape",
			enclose: `{"type":"record","name":"ns.Top","fields":[
				{"name":"a","type":{"type":"record","name":"Foo","namespace":"","fields":[{"name":"x","type":"int"}]}},
				{"name":"b","type":".Foo"}]}`,
			want: `{"type":"record","name":"Foo","fields":[{"name":"x","type":"int"}]}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := MustParse(tc.enclose).Root()
			requireSubSchema(t, &root.Fields[1].Type, tc.want, fooVal)
		})
	}
}

// TestNodeRefSchema_ConvertsOffTheRootExpression pins the other half of issue
// #42: not that a reference node converts, but that a caller can REACH one to
// convert it, without binding a temporary first.
//
// The root form is what the pointer return buys. A function result is not
// addressable, so were Schema.Root to return a value again, the pointer-receiver
// SchemaNode.Schema could not be called on it at all and the s.Root().Schema()
// line stops compiling — verified by reverting the signature: the build fails
// there and only there. The nested forms compile under EITHER signature, since
// indexing a slice yields an addressable element, so they pin that — which is
// why SchemaField.Type needs no pointer of its own — and that each reach shape
// still converts correctly.
func TestNodeRefSchema_ConvertsOffTheRootExpression(t *testing.T) {
	s := MustParse(`{
		"type": "record", "name": "Outer", "namespace": "ns",
		"fields": [
			{"name": "a", "type": {"type": "record", "name": "Inner", "fields": [{"name": "x", "type": "long"}]}},
			{"name": "b", "type": "Inner"},
			{"name": "c", "type": ["null", "Inner"]},
			{"name": "d", "type": {"type": "array", "items": "Inner"}}
		]
	}`)
	const inner = `{"type":"record","name":"Inner","namespace":"ns","fields":[{"name":"x","type":"long"}]}`
	innerVal := map[string]any{"x": int64(7)}

	// The root itself, off the call expression.
	mustNodeSchema(t, s.Root())
	// Reached through SchemaField.Type, a VALUE field: the definition
	// site, the bare name reference, and a reference one level deeper in
	// a union branch (Branches is a []SchemaNode, so its elements are
	// values too).
	requireSubSchema(t, &s.Root().Fields[0].Type, inner, innerVal)
	requireSubSchema(t, &s.Root().Fields[1].Type, inner, innerVal)
	requireSubSchema(t, &s.Root().Fields[2].Type.Branches[1], inner, innerVal)
	// Reached through Items, which is already a *SchemaNode: a different
	// shape, and the one that worked before this change.
	requireSubSchema(t, s.Root().Fields[3].Type.Items, inner, innerVal)
}

// TestNodeRefSchemaMatrix is the class-elimination net for reference-node
// extraction: kind × namespace spelling × extraction site × structure.
// Each cell builds an enclosing schema whose extraction site holds a NAME
// REFERENCE, extracts the site node from Root(), converts it with
// SchemaNode.Schema, and compares canonical bytes + wire bytes against a
// TWIN whose extraction site holds the definition INLINE — the
// long-standing pre-splice path — then re-parses the result's String()
// to pin self-containment. Neutering the refTarget splice in toJSONWalk
// must fail every cell here (verified when the net was built).
func TestNodeRefSchemaMatrix(t *testing.T) {
	kinds := []struct {
		name string
		def  func(name, nsAttr string) string // named-type definition JSON
		val  any                              // sample value encodable by it
	}{
		{"record", func(n, ns string) string {
			return `{"type":"record","name":"` + n + `"` + ns + `,"fields":[{"name":"s","type":"string"}]}`
		}, map[string]any{"s": "x"}},
		{"enum", func(n, ns string) string {
			return `{"type":"enum","name":"` + n + `"` + ns + `,"symbols":["A","B"]}`
		}, "B"},
		{"fixed", func(n, ns string) string {
			return `{"type":"fixed","name":"` + n + `"` + ns + `,"size":4}`
		}, []byte{1, 2, 3, 4}},
	}

	// Namespace spellings for the def / ref pair and the enclosing root.
	nsCases := []struct {
		name   string
		defN   string // definition's "name"
		nsAttr string // definition's explicit namespace attribute, if any
		ref    string // reference spelling at the site
		rootNS string // enclosing root record's namespace attribute
		decoy  string // extra same-short-name type in another namespace
	}{
		{name: "dotted", defN: "x.y.N", ref: `"x.y.N"`},
		{name: "inherit", defN: "N", ref: `"N"`, rootNS: `,"namespace":"x.y"`},
		{name: "nullns", defN: "N", ref: `"N"`},
		{name: "nullescape", defN: "N", nsAttr: `,"namespace":""`, ref: `"N"`, rootNS: `,"namespace":"x.y"`},
		// Two types share the short name N: the in-scope x.y.N must win
		// the short-spelled reference over the null-namespace decoy.
		{name: "shadow", defN: "N", ref: `"N"`, rootNS: `,"namespace":"x.y"`,
			decoy: `{"name":"decoy","type":{"type":"fixed","name":"N","namespace":"","size":2}},`},
	}

	// Extraction sites: how the reference is embedded in the second field,
	// and the sample value for the site-shaped schema.
	sites := []struct {
		name string
		wrap func(ref string) string
		val  func(kindVal any) any
	}{
		{"field", func(r string) string { return r }, func(v any) any { return v }},
		{"array", func(r string) string { return `{"type":"array","items":` + r + `}` }, func(v any) any { return []any{v} }},
		{"map", func(r string) string { return `{"type":"map","values":` + r + `}` }, func(v any) any { return map[string]any{"k": v} }},
		{"union", func(r string) string { return `["null",` + r + `]` }, func(any) any { return map[string]any{"null": nil} }},
		{"nested", func(r string) string {
			return `{"type":"record","name":"Inner","fields":[{"name":"f","type":` + r + `}]}`
		}, func(v any) any { return map[string]any{"f": v} }},
	}

	build := func(rootNS, decoy, first, siteType string) string {
		return `{"type":"record","name":"Root"` + rootNS + `,"fields":[` + decoy +
			`{"name":"one","type":` + first + `},{"name":"two","type":` + siteType + `}]}`
	}
	// site node = the "two" field's type; the decoy, when present, shifts
	// the field index by one.
	extract := func(t *testing.T, s *Schema, hasDecoy bool) *SchemaNode {
		t.Helper()
		root := s.Root()
		i := 1
		if hasDecoy {
			i = 2
		}
		return &root.Fields[i].Type
	}

	check := func(t *testing.T, got, twin *Schema, val any) {
		t.Helper()
		if !bytes.Equal(got.Canonical(), twin.Canonical()) {
			t.Fatalf("canonical mismatch:\n got: %s\ntwin: %s", got.Canonical(), twin.Canonical())
		}
		ge, err := got.Encode(val)
		if err != nil {
			t.Fatalf("encode with extracted schema: %v", err)
		}
		te, err := twin.Encode(val)
		if err != nil {
			t.Fatalf("encode with twin schema: %v", err)
		}
		if !bytes.Equal(ge, te) {
			t.Fatalf("wire mismatch: got %x twin %x", ge, te)
		}
		// Self-containment: the result's own text re-parses to itself.
		rp, err := Parse(got.String())
		if err != nil {
			t.Fatalf("extracted schema text does not re-parse: %v\ntext: %s", err, got.String())
		}
		if !bytes.Equal(rp.Canonical(), got.Canonical()) {
			t.Fatalf("re-parse canonical drift:\n got: %s\nre:  %s", got.Canonical(), rp.Canonical())
		}
	}

	// Structure: second occurrence (def in field one, ref in field two).
	for _, k := range kinds {
		for _, ns := range nsCases {
			for _, site := range sites {
				t.Run("second/"+k.name+"/"+ns.name+"/"+site.name, func(t *testing.T) {
					def := k.def(ns.defN, ns.nsAttr)
					test := MustParse(build(ns.rootNS, ns.decoy, def, site.wrap(ns.ref)))
					twin := MustParse(build(ns.rootNS, ns.decoy, `"string"`, site.wrap(def)))
					got, err := extract(t, test, ns.decoy != "").Schema()
					if err != nil {
						t.Fatalf("Schema() on reference site: %v", err)
					}
					want, err := extract(t, twin, ns.decoy != "").Schema()
					if err != nil {
						t.Fatalf("Schema() on inline twin site: %v", err)
					}
					check(t, got, want, site.val(k.val))
				})
			}
		}
	}

	// Structure: cache cross-parse (definition from a prior Parse call).
	for _, k := range kinds {
		for _, nsName := range []string{"dotted", "nullns"} {
			t.Run("cache/"+k.name+"/"+nsName, func(t *testing.T) {
				defN := "N"
				if nsName == "dotted" {
					defN = "x.y.N"
				}
				def := k.def(defN, "")
				var c SchemaCache
				mustCacheParse(t, &c, def)
				enclosing, err := c.Parse(build("", "", defN2ref(defN), defN2ref(defN)))
				if err != nil {
					t.Fatal(err)
				}
				got, err := extract(t, enclosing, false).Schema()
				if err != nil {
					t.Fatalf("Schema() on cache-referenced site: %v", err)
				}
				check(t, got, MustParse(def), k.val)
			})
		}
	}

	// Structure: diamond — C references D whose definition lives inside B.
	for _, k := range kinds {
		t.Run("diamond/"+k.name, func(t *testing.T) {
			dDef := k.def("x.y.D", "")
			s := MustParse(`{"type":"record","name":"Root","fields":[
				{"name":"b","type":{"type":"record","name":"B","fields":[{"name":"d","type":` + dDef + `}]}},
				{"name":"c","type":{"type":"record","name":"C","fields":[{"name":"d","type":"x.y.D"}]}}]}`)
			root := s.Root()
			got, err := root.Fields[1].Type.Schema()
			if err != nil {
				t.Fatalf("Schema() on diamond arm: %v", err)
			}
			twin := MustParse(`{"type":"record","name":"C","fields":[{"name":"d","type":` + dDef + `}]}`)
			check(t, got, twin, map[string]any{"d": k.val})
		})
	}

	// Structure: forward reference — extraction of the ref node resolves
	// to the definition appearing later in the enclosing schema.
	for _, k := range kinds {
		t.Run("forward/"+k.name, func(t *testing.T) {
			def := k.def("N", "")
			s := MustParse(build("", "", `"N"`, def))
			root := s.Root()
			got, err := root.Fields[0].Type.Schema()
			if err != nil {
				t.Fatalf("Schema() on forward reference: %v", err)
			}
			check(t, got, MustParse(def), k.val)
		})
	}

	// Structure: recursive, dotted and inherited namespace spellings.
	for _, ns := range []struct{ name, def, ref string }{
		{"dotted", "x.y.N", "x.y.N"},
		{"inherit", "N", "N"},
	} {
		t.Run("recursive/"+ns.name, func(t *testing.T) {
			nsAttr := ""
			if ns.name == "inherit" {
				nsAttr = `,"namespace":"x.y"`
			}
			def := `{"type":"record","name":"` + ns.def + `"` + nsAttr + `,"fields":[{"name":"next","type":["null","` + ns.ref + `"]}]}`
			s := MustParse(def)
			root := s.Root()
			// The union branch node: its Schema() is the full recursive
			// definition, canonically equal to the enclosing schema itself.
			got, err := root.Fields[0].Type.Branches[1].Schema()
			if err != nil {
				t.Fatalf("Schema() on recursive branch: %v", err)
			}
			check(t, got, s, map[string]any{"next": nil})
		})
	}

	// Wrapped reference with custom properties: the props ride onto the
	// spliced definition (canonical is prop-blind, so the twin comparison
	// holds) and survive on the result's root node.
	t.Run("wrapper-props", func(t *testing.T) {
		def := kinds[0].def("x.y.N", "")
		test := MustParse(build("", "", def, `{"type":"x.y.N","my.prop":123}`))
		got, err := extract(t, test, false).Schema()
		if err != nil {
			t.Fatalf("Schema() on wrapped reference: %v", err)
		}
		check(t, got, MustParse(def), kinds[0].val)
		if p := got.Root().Props["my.prop"]; p != int64(123) {
			t.Fatalf("wrapper prop lost on splice: got %v (%T)", p, p)
		}
	})
}

// defN2ref quotes a fullname as a JSON reference token.
func defN2ref(n string) string { return `"` + n + `"` }

// schemaNodeFieldRule classifies one exported SchemaNode field for the two
// invariants below. The zero rule is the ordinary case: the field BLOCKS the
// shortcut, and its value survives on the same field after a round trip.
// Losslessness is a CONJUNCTION, and a guard proving only the blocking half
// proves only half of it — the shortcut must decline to collapse a node that
// carries content, AND the longer form it falls through to must actually emit
// that content. EnumDefault is why both halves are checked: it blocked the
// collapse, the emitter keyed "default" off HasEnumDefault instead, and the
// value was dropped exactly as before with only the render changed.
type schemaNodeFieldRule struct {
	// exempt, when non-empty, is the reason this field has NO emitted form
	// at this site, so taking the shortcut cannot lose it. An exempt field
	// must NOT block — an exemption that blocks is a contradiction, and the
	// test says which half is wrong.
	exempt string
	// propsKey, when non-empty, names the JSON key the emission arm writes
	// and the Props entry the value comes back under, because the carrier
	// kind does not BIND that key. The value is preserved as inert metadata
	// on its only surface, not lost; the field itself reads back zero.
	propsKey string
	why      string
}

// There is deliberately no "dropped" classification. A reserved key the
// carrier kind does not bind has Props as its only surface, so an emitted
// value always comes back somewhere: on its own field, or under propsKey.
// A field that emits a key the re-parse then discards therefore fails here
// with "give it an emission arm, classify where it relocates, or classify it
// exempt" rather than being classified as an accepted loss — which is the
// routing rule stated as a test.

// bareEmissionFieldRules classifies every exported SchemaNode field whose
// treatment under nodeCarriesOnlyType is not the ordinary
// blocks-and-round-trips case. Everything absent from this map must block the
// collapse AND come back on its own field.
var bareEmissionFieldRules = map[string]schemaNodeFieldRule{
	"Branches":    {exempt: "no JSON key routes to Branches outside a union — the union arm returns before the collapse is reached — so a hand-built value on another kind is inert"},
	"EnumDefault": {exempt: "HasEnumDefault is the carrier the \"default\" key is emitted from; with the carrier false the node declares no default, so there is nothing to emit and nothing to lose"},
	"HasEnumDefault": {
		propsKey: "default",
		why: "the carrier emits \"default\", and only an enum BINDS that key at the type level — on any other carrier it is a " +
			"field attribute the kind does not bind, with no structural field to land on, so it rides to Props as its only " +
			"surface exactly like precision/scale off a decimal carrier. Pinned across the kind axis by " +
			"TestMatrix_AttributePlacementCensus and for the reference-wrapper spelling by TestRegression_EnumRefWrapperDefaultInert",
	},
	"Precision": {
		propsKey: "precision",
		why: "precision and scale are decimal PARAMETERS, bound only on a recognized decimal carrier " +
			"(decimalConsumesPrecisionScale); on any other carrier the pair is inert metadata that rides to Props verbatim, " +
			"pinned across the placement axis by TestMatrix_StrayPrecisionScalePlacement",
	},
	"Scale": {
		propsKey: "scale",
		why:      "the same clause as Precision: unconsumed off a decimal carrier, so Props is its only surface",
	},
}

// A schema node collapses to its bare type name only when it carries nothing
// else. That question used to be answered by two hand-written lists of the
// fields someone remembered, and both were missing the same members — a
// stray-surfaced Symbols, Size, Aliases or Name on a primitive survived String()
// and Root() and vanished through Root().Schema().
//
// The durable fix is not "add the missing ones": it is that the enumeration must
// check ITSELF. This sets every exported field in turn and requires BOTH halves
// of losslessness — nodeCarriesOnlyType declines to collapse, and the value then
// survives an emit → re-parse round trip, read back off the metadata FIELDS
// rather than the rendered text, since key order alone makes a text comparison
// report losses that did not happen.
func TestInvariant_BareEmissionCoversEverySchemaNodeField(t *testing.T) {
	base := SchemaNode{Type: "int"}
	if !nodeCarriesOnlyType(&base) {
		t.Fatal("a bare primitive must carry only its Type; the control is broken so nothing below means anything")
	}
	// Branches is exempt only OFF a union. On a union it carries the whole
	// schema, so the exemption must not leak there.
	if u := (SchemaNode{Type: "union", Branches: []SchemaNode{{Type: "null"}}}); nodeCarriesOnlyType(&u) {
		t.Error("Branches is exempt only outside a union; on a union it carries the branch list and must block")
	}

	rt := reflect.TypeFor[SchemaNode]()
	var checked, exempted, relocated int
	for i := range rt.NumField() {
		f := rt.Field(i)
		if !f.IsExported() || f.Name == "Type" {
			continue
		}
		rule := bareEmissionFieldRules[f.Name]
		n := SchemaNode{Type: "int"}
		fv := reflect.ValueOf(&n).Elem().Field(i)
		if !setNonZeroForTest(f.Name, fv) {
			t.Errorf("field %s has kind %s, which this test does not know how to populate — teach it, or the field is silently unchecked", f.Name, f.Type.Kind())
			continue
		}
		want := fv.Interface()

		if blocks := !nodeCarriesOnlyType(&n); rule.exempt != "" {
			exempted++
			if blocks {
				t.Errorf("field %s is classified exempt (%s) but blocks bare emission; either the exemption is wrong or the field gained an emitted form", f.Name, rule.exempt)
			}
			continue
		} else if !blocks {
			t.Errorf("setting %s does NOT block bare emission, so its value would be silently dropped by Root().Schema(). Give it an emission arm, or classify it exempt with the reason it cannot be emitted.", f.Name)
			continue
		}
		checked++

		// The other half: the object form it fell through to must carry the
		// value somewhere a reader can find it.
		s, err := n.Schema()
		if err != nil {
			t.Errorf("field %s: blocking is not enough — the emission failed outright: %v", f.Name, err)
			continue
		}
		back := s.Root()
		switch {
		case rule.propsKey != "":
			relocated++
			// Both halves of the classification are checked. The emission
			// arm must WRITE the key (otherwise the relocation never had a
			// value to carry and this rule is the wrong diagnosis)...
			if !strings.Contains(s.String(), `"`+rule.propsKey+`"`) {
				t.Errorf("field %s is classified as relocating to Props under %q, but the emission never wrote that key at all — the loss would be the emitter's, so give it an emission arm: %s",
					f.Name, rule.propsKey, s)
			}
			// ...and the re-parse must land it in Props rather than lose it.
			if _, ok := back.Props[rule.propsKey]; !ok {
				t.Errorf("field %s emits %q on a carrier that does not bind it, so the value must ride to Props as its only surface; Props came back %v from %s. Rule quoted: %s",
					f.Name, rule.propsKey, back.Props, s, rule.why)
			}
			// It is a RELOCATION, so the field itself must read back zero.
			// If it starts coming back on its own field the carrier began
			// binding the key and the classification is stale.
			if got := reflect.ValueOf(back).Elem().Field(i).Interface(); !reflect.DeepEqual(reflect.Zero(f.Type).Interface(), got) {
				t.Errorf("field %s now comes back on its OWN field (%#v) rather than in Props, so the carrier binds %q after all: reclassify it as an ordinary round-tripping field. Rule quoted: %s",
					f.Name, got, rule.propsKey, rule.why)
			}
		default:
			if got := reflect.ValueOf(back).Elem().Field(i).Interface(); !reflect.DeepEqual(want, got) {
				t.Errorf("field %s blocks the collapse but does not survive the rebuild: set %#v, emitted %s, read back %#v. The value is dropped with only the render changed — give it an emission arm, classify where it relocates, or classify it exempt.",
					f.Name, want, s, got)
			}
		}
		// Wherever the value landed, emission must be a FIXPOINT from there:
		// a second pass that drops it would mean the first round trip only
		// postponed the loss.
		s2, err := back.Schema()
		switch {
		case err != nil:
			t.Errorf("field %s: re-emitting the rebuilt node failed: %v", f.Name, err)
		case s2.String() != s.String():
			t.Errorf("field %s: emission is not a fixpoint, so something is lost on the second pass:\n first %s\nsecond %s", f.Name, s, s2)
		}
	}
	if checked < 12 {
		t.Fatalf("only %d fields were actually checked; the walk is not seeing SchemaNode", checked)
	}
	t.Logf("bare-emission coverage: %d fields block, of which %d round-trip on their own field and %d relocate to Props; %d classified exempt", checked, checked-relocated, relocated, exempted)
}

// presenceOnlyFields names the fields whose content can be entirely INVISIBLE in
// the field's value: an attribute written as the field's own zero. Each entry is a
// schema that writes exactly that, and the key the rebuild must still carry.
//
// This is the half the loop above structurally cannot reach: it populates every
// field with a NON-ZERO value, the only way it can tell "blocks" from "does not
// block", so the zero is the axis it holds constant and a node whose only content
// is presence looks empty to it. reachesCollapse marks the cells whose probe node
// carries NOTHING but the presence, so the shortcut is actually on the path.
var presenceOnlyFields = map[string]struct {
	src             string // a schema whose node carries the attribute written as the zero
	key             string // the JSON key the rebuild must still carry
	reachesCollapse bool
}{
	"Doc": {
		// doc is recorded only where Apache Avro reads one — the named
		// kinds and fields — and a named kind always carries its name, so
		// the collapse is structurally out of reach here. The emitter is
		// what this cell tests.
		src: `{"type":"record","name":"R","doc":"","fields":[]}`,
		key: "doc",
	},
	"LogicalType": {
		// logicalType rides on any kind, so a primitive carrying nothing
		// but an empty one is exactly the node the shortcut collapses.
		src:             `{"type":"int","logicalType":""}`,
		key:             "logicalType",
		reachesCollapse: true,
	},
}

// TestInvariant_BareEmissionCoversPresenceOnlyFields is the completeness guard's
// other half: for every field that can carry presence without carrying a value,
// the collapse must still be blocked and the attribute must still survive the
// rebuild. The emptiness walk and the emitter answer two different questions —
// "does this node carry anything" and "does this node emit anything" — and must
// read the same state. Teaching only the emitter about presence leaves the walk
// deciding the node is empty and collapsing it before the emitter is ever
// consulted.
func TestInvariant_BareEmissionCoversPresenceOnlyFields(t *testing.T) {
	rt := reflect.TypeFor[SchemaNode]()
	seen := 0
	for i := range rt.NumField() {
		f := rt.Field(i)
		if !f.IsExported() {
			continue
		}
		spec, ok := presenceOnlyFields[f.Name]
		if !ok {
			// Every OTHER field must have no presence state, or it belongs
			// in the table above with its own cell.
			var probe SchemaNode
			if nodePresenceSet(&probe, f.Name) {
				t.Errorf("field %s answers nodePresenceSet but has no presence-only cell; add one so its zero-valued form is checked", f.Name)
			}
			continue
		}
		seen++
		n := MustParse(spec.src).Root()
		fv := reflect.ValueOf(n).Elem().Field(i)
		if !fv.IsZero() {
			t.Errorf("field %s: the probe schema did not leave the field at its zero (%#v), so this cell is testing the ordinary value case", f.Name, fv.Interface())
			continue
		}
		if !nodePresenceSet(n, f.Name) {
			t.Errorf("field %s: the parse did not record the attribute as written, so the probe never reaches the question", f.Name)
			continue
		}
		if spec.reachesCollapse {
			// The precondition: strip the presence and the node IS empty,
			// so the shortcut would fire. Without this the "blocks" check
			// below could pass because of unrelated content the probe
			// happens to carry, and the walk would never be tested.
			bare := SchemaNode{Type: n.Type}
			if !nodeCarriesOnlyType(&bare) {
				t.Errorf("field %s: the probe's kind does not collapse even when empty, so this cell cannot reach the shortcut; clear reachesCollapse or pick another kind", f.Name)
				continue
			}
			if nodeCarriesOnlyType(n) {
				t.Errorf("field %s carries a written attribute whose value is the field's zero, and the emptiness walk still calls the node empty — the collapse drops it before the emitter is ever reached", f.Name)
				continue
			}
		}
		s, err := n.Schema()
		if err != nil {
			t.Errorf("field %s: rebuild failed: %v", f.Name, err)
			continue
		}
		if !strings.Contains(s.String(), `"`+spec.key+`"`) {
			t.Errorf("field %s: the rebuild dropped the written %q: %s", f.Name, spec.key, s)
			continue
		}
		// And it must be a FIXPOINT: the re-parse has to record the presence
		// again, or the attribute survives one rebuild and dies on the next.
		back := s.Root()
		if !nodePresenceSet(back, f.Name) {
			t.Errorf("field %s: the re-parse did not record the attribute, so a second rebuild would drop it", f.Name)
			continue
		}
		s2, err := back.Schema()
		if err != nil {
			t.Errorf("field %s: second rebuild failed: %v", f.Name, err)
			continue
		}
		if s2.String() != s.String() {
			t.Errorf("field %s: emission is not a fixpoint:\n first %s\nsecond %s", f.Name, s, s2)
		}
	}
	if seen != len(presenceOnlyFields) {
		t.Fatalf("checked %d presence-only fields, the table names %d — a name in the table no longer matches a SchemaNode field", seen, len(presenceOnlyFields))
	}
}

// The field-level twin. SchemaField carries its own presence state, and the
// bare-emission collapse is a node-level question, so the surface that can
// lose a field's empty doc is the field emitter rather than the walk.
func TestInvariant_FieldPresenceOnlyDocSurvivesTheRebuild(t *testing.T) {
	n := MustParse(`{"type":"record","name":"R","fields":[{"name":"f","type":"int","doc":""}]}`).Root()
	if got := n.Fields[0].Doc; got != "" {
		t.Fatalf("the probe did not leave the field doc at its zero: %q", got)
	}
	if !n.Fields[0].docSet {
		t.Fatal("the parse did not record the field doc as written")
	}
	s, err := n.Schema()
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if !strings.Contains(s.String(), `"doc"`) {
		t.Errorf("the rebuild dropped the written empty field doc: %s", s)
	}
	back := s.Root()
	if !back.Fields[0].docSet {
		t.Error("the re-parse did not record the field doc, so a second rebuild would drop it")
	}
	s2, err := back.Schema()
	if err != nil {
		t.Fatalf("second rebuild: %v", err)
	}
	if s2.String() != s.String() {
		t.Errorf("field doc emission is not a fixpoint:\n first %s\nsecond %s", s, s2)
	}
}

// nameRefSpliceFieldRules classifies every exported SchemaNode field whose
// treatment under nodeIsNameRefShape is not the ordinary blocking case. The
// exemptions are the reserved USAGE-SITE attributes a splice is already
// adjudicated to drop, plus the custom properties it merges; deriving the
// predicate without exactly these would turn an adjudicated silent drop into
// a hard "unknown complex type" error on the extraction feature.
var nameRefSpliceFieldRules = map[string]schemaNodeFieldRule{
	"Doc":         {exempt: "a definition cannot carry a second doc for one of its usage sites, so the splice drops it"},
	"Aliases":     {exempt: "usage-site aliases have no place on the spliced definition"},
	"Namespace":   {exempt: "a definition cannot carry a second namespace for one of its usage sites"},
	"LogicalType": {exempt: "a usage-site logicalType annotates the reference, not the definition it names"},
	"Props":       {exempt: "the wrapper's custom properties MERGE onto the spliced definition, definition-wins, rather than being discarded"},
}

// The sibling invariant, for the other predicate on the same walk. A stamped
// name-reference node splices the definition it names in place of itself, so every
// field the node carries that the splice does not merge is DISCARDED.
// nodeIsNameRefShape decides whether that is allowed, and it too used to be a
// hand-written list of eight fields, silently discarding the seven it did not name.
// The probe has to REACH the splice: the stamp must be present (extraction from
// Root, not a hand-built node), Type must be left alone, and the extracted sub-tree
// must not define the name locally, since a whole-schema walk never splices.
func TestInvariant_NameRefSpliceCoversEverySchemaNodeField(t *testing.T) {
	const src = `{"type":"record","name":"Root","namespace":"x.y","fields":[
		{"name":"a","type":{"type":"record","name":"Inner","fields":[{"name":"q","type":"int"}]}},
		{"name":"b","type":"x.y.Inner"}]}`
	base := MustParse(src)
	extract := func() SchemaNode { return base.Root().Fields[1].Type }

	unedited := extract()
	control, err := unedited.Schema()
	if err != nil {
		t.Fatalf("the unedited extraction must splice; the control is broken so nothing below means anything: %v", err)
	}
	if !strings.Contains(control.String(), `"fields"`) {
		t.Fatalf("control did not splice the definition: %s", control)
	}

	rt := reflect.TypeFor[SchemaNode]()
	var blocked, exempted int
	for i := range rt.NumField() {
		f := rt.Field(i)
		if !f.IsExported() || f.Name == "Type" {
			continue
		}
		rule := nameRefSpliceFieldRules[f.Name]
		n := extract()
		fv := reflect.ValueOf(&n).Elem().Field(i)
		if !setNonZeroForTest(f.Name, fv) {
			t.Errorf("field %s has kind %s, which this test does not know how to populate — teach it, or the field is silently unchecked", f.Name, f.Type.Kind())
			continue
		}
		splices := nodeIsNameRefShape(&n)
		if rule.exempt != "" {
			exempted++
			if !splices {
				t.Errorf("field %s is classified exempt (%s) but blocks the splice; blocking it converts an adjudicated usage-site drop into a hard parse error", f.Name, rule.exempt)
				continue
			}
			// The exemption's own claim, executed: the splice still happens.
			if s, err := n.Schema(); err != nil {
				t.Errorf("field %s is exempt, so the reference must still splice; it errored instead: %v", f.Name, err)
			} else if !strings.Contains(s.String(), `"fields"`) {
				t.Errorf("field %s is exempt, so the reference must still splice the definition; got %s", f.Name, s)
			}
			continue
		}
		blocked++
		if splices {
			t.Errorf("setting %s still lets the node splice, so its value is silently discarded in favor of the definition. Give it a place on the spliced form, or classify it exempt with the reason its loss is adjudicated.", f.Name)
			continue
		}
		// Blocking means rendering as-written. The re-parse must then JUDGE
		// the hybrid — a named error is the honest outcome, and a silent
		// success that dropped the field is the outcome this rules out.
		s, err := n.Schema()
		if err != nil {
			continue // loud, which is the contract
		}
		if got := reflect.ValueOf(s.Root()).Elem().Field(i).Interface(); !reflect.DeepEqual(fv.Interface(), got) {
			t.Errorf("field %s blocks the splice but the as-written render still lost it: set %#v, emitted %s, read back %#v",
				f.Name, fv.Interface(), s, got)
		}
	}
	if blocked < 8 {
		t.Fatalf("only %d fields were required to block; the walk is not seeing SchemaNode", blocked)
	}
	t.Logf("name-reference splice coverage: %d fields must block, %d classified exempt as usage-site attributes", blocked, exempted)
}

// setNonZeroForTest gives fv a non-zero, SCHEMA-VALID value, reporting false
// for kinds it does not handle so an unhandled kind is a loud failure rather
// than a silently skipped field. The values must be schema-valid because both
// invariants above emit the node and re-parse it: a zero SchemaNode child has
// Type "" and could never parse, which would report every container field as
// an emission failure rather than as the round trip it is meant to measure.
func setNonZeroForTest(name string, fv reflect.Value) bool {
	switch name {
	case "Items", "Values":
		fv.Set(reflect.ValueOf(&SchemaNode{Type: "int"}))
		return true
	case "Fields":
		fv.Set(reflect.ValueOf([]SchemaField{{Name: "f", Type: SchemaNode{Type: "int"}}}))
		return true
	case "Branches":
		fv.Set(reflect.ValueOf([]SchemaNode{{Type: "null"}, {Type: "int"}}))
		return true
	case "Symbols", "Aliases":
		fv.Set(reflect.ValueOf([]string{"A"}))
		return true
	case "Props":
		fv.Set(reflect.ValueOf(map[string]any{"my.p": "v"}))
		return true
	}
	switch fv.Kind() {
	case reflect.String:
		fv.SetString("x")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		fv.SetInt(1)
	case reflect.Bool:
		fv.SetBool(true)
	default:
		return false
	}
	return true
}

// ---------- caller_node_matrix_test.go ----------

// The caller-COMPOSED and caller-EDITED SchemaNode matrix.
//
// Every other node matrix drives the tree one direction: text → Parse → Root() →
// rebuild, compared against the parse. Two input shapes are outside that loop
// and are the ones a caller actually writes:
//
//   - HAND-BUILT: a SchemaNode the caller assembled, whose field combinations
//     Parse can never produce (a stray Symbols on an int, a Size on a map).
//   - EXTRACTED-THEN-EDITED: sub := s.Root().Fields[i].Type, then a write to an
//     exported field. The struct copy carries hidden state — the name-reference
//     stamp — so the edit and the stamp can disagree, and the node splices a
//     definition that never sees the edit.
//
// The cross is {hand-built, extracted-unedited, extracted-edited} x {every
// exported field} x {Schema, String, Canonical, Fingerprint, JSON}, over
// structures including the shapes a flat schema cannot exercise: a RECURSIVE
// definition and a DIAMOND. A flat schema only ever exercises a type as a
// DEFINITION; these exercise the second-occurrence REFERENCE path, where the
// stamp lives.
//
// Every cell asserts a value or a NAMED error, never a panic and never a silent
// drop. Which of those a cell is entitled to is read off the two classification
// tables the predicates derive from (bareEmissionFieldRules,
// nameRefSpliceFieldRules), so a field added to SchemaNode later cannot be
// silently absent from this cross either.

// callerNodeStructure is one enclosing schema plus the coordinates of a
// name-REFERENCE node inside it — the node whose Schema() must splice.
type callerNodeStructure struct {
	name string
	// build returns the parsed enclosing schema. Some structures need a
	// SchemaCache because the definition arrives from a prior Parse.
	build func(t *testing.T) *Schema
	// pick walks to the reference node. It returns a copy, which is exactly
	// what a caller gets and exactly what carries the stamp.
	pick func(SchemaNode) SchemaNode
	// def is the standalone text the reference names; splicing must produce
	// a schema canonically equal to it.
	def string
	// val encodes under def, so a spliced result can be exercised on both
	// wire formats rather than only compared as metadata.
	val any
}

func callerNodeStructures() []callerNodeStructure {
	const inner = `{"type":"record","name":"x.y.Inner","fields":[{"name":"q","type":"int"}]}`
	const nodeDef = `{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]}]}`
	const dDef = `{"type":"fixed","name":"x.y.D","size":4}`

	return []callerNodeStructure{
		{
			name: "second-occurrence",
			build: func(t *testing.T) *Schema {
				return MustParse(`{"type":"record","name":"x.y.Root","fields":[
					{"name":"a","type":` + inner + `},
					{"name":"b","type":"x.y.Inner"}]}`)
			},
			pick: func(r SchemaNode) SchemaNode { return r.Fields[1].Type },
			def:  inner,
			val:  map[string]any{"q": int32(7)},
		},
		{
			name: "forward-reference",
			build: func(t *testing.T) *Schema {
				return MustParse(`{"type":"record","name":"x.y.Root","fields":[
					{"name":"a","type":"x.y.Inner"},
					{"name":"b","type":` + inner + `}]}`)
			},
			pick: func(r SchemaNode) SchemaNode { return r.Fields[0].Type },
			def:  inner,
			val:  map[string]any{"q": int32(7)},
		},
		{
			// RECURSIVE: the branch references the record that encloses it,
			// so splicing it re-enters the union the outer walk is inside.
			name: "recursive",
			build: func(t *testing.T) *Schema {
				return MustParse(nodeDef)
			},
			pick: func(r SchemaNode) SchemaNode { return r.Fields[0].Type.Branches[1] },
			def:  nodeDef,
			val:  map[string]any{"next": nil},
		},
		{
			// DIAMOND: C's reference to D resolves to a definition that lives
			// inside B, a sibling subtree the extraction does not contain.
			name: "diamond",
			build: func(t *testing.T) *Schema {
				return MustParse(`{"type":"record","name":"Root","fields":[
					{"name":"b","type":{"type":"record","name":"B","fields":[{"name":"d","type":` + dDef + `}]}},
					{"name":"c","type":{"type":"record","name":"C","fields":[{"name":"d","type":"x.y.D"}]}}]}`)
			},
			pick: func(r SchemaNode) SchemaNode { return r.Fields[1].Type.Fields[0].Type },
			def:  dDef,
			val:  []byte{1, 2, 3, 4},
		},
		{
			// The definition arrives from a PRIOR Parse. The cache INLINES it
			// at its first occurrence, so the first field is the definition
			// itself and only the SECOND is a reference — picking the first
			// would measure a node that never splices.
			name: "cache-cross-parse",
			build: func(t *testing.T) *Schema {
				t.Helper()
				var c SchemaCache
				mustCacheParse(t, &c, inner)
				s := mustCacheParse(t, &c, `{"type":"record","name":"x.y.Outer","fields":[
					{"name":"a","type":"x.y.Inner"},
					{"name":"b","type":"x.y.Inner"}]}`)
				return s
			},
			pick: func(r SchemaNode) SchemaNode { return r.Fields[1].Type },
			def:  inner,
			val:  map[string]any{"q": int32(7)},
		},
	}
}

// surfaceReport is what one node's five surfaces produced. A nil err with an
// empty text is impossible; a panic is recorded as such and is always a
// failure, since the whole point of these inputs is that a caller can compose
// anything and must still get a value or a named error.
type surfaceReport struct {
	panicked any
	err      error
	text     string
	canon    []byte
	finger   []byte
	jsonWire []byte
}

// driveSurfaces runs every caller-reachable surface of a SchemaNode, catching
// panics so a panic becomes a reported failure rather than a dead test binary.
func driveSurfaces(n SchemaNode, val any) (rep surfaceReport) {
	defer func() {
		if r := recover(); r != nil {
			rep.panicked = r
		}
	}()
	s, err := n.Schema()
	if err != nil {
		rep.err = err
		return rep
	}
	rep.text = s.String()
	rep.canon = s.Canonical()
	rep.finger = s.Fingerprint(crc64.New(crc64.MakeTable(crc64.ECMA)))
	if val != nil {
		if j, jerr := s.EncodeJSON(val); jerr == nil {
			rep.jsonWire = j
		}
	}
	return rep
}

// checkSurfaces applies the invariants every cell owes regardless of which
// axis produced it.
func checkSurfaces(t *testing.T, where string, rep surfaceReport) bool {
	t.Helper()
	if rep.panicked != nil {
		t.Errorf("%s: PANIC %v — a caller-composed node must produce a value or a named error", where, rep.panicked)
		return false
	}
	if rep.err != nil {
		if msg := rep.err.Error(); msg == "" {
			t.Errorf("%s: an empty error message is not a named error", where)
		} else if len(msg) > 2048 {
			t.Errorf("%s: error message is %d bytes; a rejection must not echo the input unbounded", where, len(msg))
		}
		return false
	}
	if rep.text == "" || len(rep.canon) == 0 || len(rep.finger) == 0 {
		t.Errorf("%s: build succeeded but a surface came back empty (text %q, canon %q)", where, rep.text, rep.canon)
		return false
	}
	// The emitted text is the schema's own claim about itself: re-parsing it
	// must reproduce the same canonical form and the same fingerprint, or one
	// of the three surfaces is lying about the others.
	re, err := Parse(rep.text)
	if err != nil {
		t.Errorf("%s: String() emitted text that does not re-parse: %v\n%s", where, err, rep.text)
		return false
	}
	if !bytes.Equal(re.Canonical(), rep.canon) {
		t.Errorf("%s: canonical form drifts across the text round trip:\n emitted %s\nreparsed %s", where, rep.canon, re.Canonical())
	}
	if !bytes.Equal(re.Fingerprint(crc64.New(crc64.MakeTable(crc64.ECMA))), rep.finger) {
		t.Errorf("%s: fingerprint drifts across the text round trip", where)
	}
	return true
}

// TestMatrix_CallerComposedAndEditedNodes crosses the origin, field and
// surface axes over every structure.
func TestMatrix_CallerComposedAndEditedNodes(t *testing.T) {
	rt := reflect.TypeFor[SchemaNode]()
	exportedFields := func() []reflect.StructField {
		var out []reflect.StructField
		for i := range rt.NumField() {
			if f := rt.Field(i); f.IsExported() && f.Name != "Type" {
				out = append(out, f)
			}
		}
		return out
	}()
	if len(exportedFields) < 12 {
		t.Fatalf("only %d exported fields found; the walk is not seeing SchemaNode", len(exportedFields))
	}

	var cells int

	// ---- origin: EXTRACTED (unedited control, then one edit per field) ----
	for _, st := range callerNodeStructures() {
		t.Run("extracted/"+st.name, func(t *testing.T) {
			s := st.build(t)
			want := MustParse(st.def)

			// extracted-unedited: the control. The field axis does not apply —
			// nothing is written — so it runs once per structure, and it is what
			// proves every "blocked" verdict below is a real change of outcome
			// rather than a node that never spliced to begin with. The precondition
			// is the probe-reaches-the-path check, asserted rather than assumed: the
			// picked node must BE a stamped bare reference, since a structure whose
			// pick lands on the DEFINITION (the cache inlines its first occurrence)
			// never splices at all.
			ctrl := st.pick(*s.Root())
			if !nodeIsNameRefShape(&ctrl) || !nodeRefTargetAgrees(&ctrl) {
				t.Fatalf("structure %q does not pick a stamped bare reference (Type=%q shape=%v stamp=%v); the probe never reaches the splice",
					st.name, ctrl.Type, nodeIsNameRefShape(&ctrl), nodeRefTargetAgrees(&ctrl))
			}
			rep := driveSurfaces(ctrl, st.val)
			cells++
			if checkSurfaces(t, "unedited control", rep) {
				if !bytes.Equal(rep.canon, want.Canonical()) {
					t.Fatalf("unedited control did not splice the definition:\n got %s\nwant %s", rep.canon, want.Canonical())
				}
				if len(rep.jsonWire) == 0 {
					t.Errorf("unedited control produced no JSON wire form for %#v", st.val)
				}
			} else {
				t.Fatalf("unedited control must splice; nothing below means anything otherwise")
			}

			for _, f := range exportedFields {
				t.Run("edit/"+f.Name, func(t *testing.T) {
					n := st.pick(*s.Root())
					fv := reflect.ValueOf(&n).Elem().FieldByName(f.Name)
					if !setNonZeroForTest(f.Name, fv) {
						t.Fatalf("cannot populate %s (kind %s)", f.Name, f.Type.Kind())
					}
					rule := nameRefSpliceFieldRules[f.Name]
					splices := nodeIsNameRefShape(&n)
					if (rule.exempt != "") != splices {
						t.Fatalf("classification disagrees with the predicate for %s: exempt=%q splices=%v", f.Name, rule.exempt, splices)
					}
					rep := driveSurfaces(n, st.val)
					cells++
					ok := checkSurfaces(t, "edited "+f.Name, rep)
					if rule.exempt != "" {
						// Exempt: the splice still happens, the definition is
						// unchanged, and the usage-site value is dropped by
						// design. Every surface must still agree.
						if !ok {
							t.Fatalf("%s is exempt, so the reference must still splice: %v", f.Name, rep.err)
						}
						if f.Name != "Props" && !bytes.Equal(rep.canon, want.Canonical()) {
							t.Errorf("%s is a usage-site attribute, so the spliced definition must be unchanged:\n got %s\nwant %s", f.Name, rep.canon, want.Canonical())
						}
						return
					}
					// Non-exempt: the node renders AS-WRITTEN, so the outcome
					// is either a named error (the reference now dangles,
					// which is the loud judgment the contract promises) or a
					// schema that still carries the edit — and where it
					// carries it is the OTHER predicate's question, read off
					// the bare-emission table. Silence is what is ruled out.
					if !ok {
						return // named error, already checked
					}
					bare := bareEmissionFieldRules[f.Name]
					back := re(t, rep.text)
					switch {
					case bare.exempt != "":
						// Classified as carrying nothing on this carrier, so
						// there is nothing for the render to lose; the
						// exemption is adjudicated in that table, not here.
					case bare.propsKey != "":
						if _, has := back.Props[bare.propsKey]; !has {
							t.Errorf("%s renders as-written and must ride to Props under %q; Props came back %v from %s", f.Name, bare.propsKey, back.Props, rep.text)
						}
					default:
						got := reflect.ValueOf(back).FieldByName(f.Name).Interface()
						if reflect.DeepEqual(got, reflect.Zero(f.Type).Interface()) {
							t.Errorf("%s survived the build but is gone from the result — a caller's write was silently discarded: %s", f.Name, rep.text)
						}
					}
				})
			}
		})
	}

	// ---- origin: HAND-BUILT (no stamp; combinations Parse cannot produce) ----
	for _, carrier := range []struct {
		name string
		node SchemaNode
		val  any
	}{
		{"primitive", SchemaNode{Type: "int"}, int32(3)},
		{"array", SchemaNode{Type: "array", Items: &SchemaNode{Type: "int"}}, []any{int32(1)}},
		{"map", SchemaNode{Type: "map", Values: &SchemaNode{Type: "int"}}, map[string]any{"k": int32(1)}},
		{"record", SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{{Name: "f", Type: SchemaNode{Type: "int"}}}}, map[string]any{"f": int32(1)}},
		{"enum", SchemaNode{Type: "enum", Name: "E", Symbols: []string{"A", "B"}}, "A"},
		{"fixed", SchemaNode{Type: "fixed", Name: "F", Size: 2}, []byte{1, 2}},
		{"union", SchemaNode{Type: "union", Branches: []SchemaNode{{Type: "null"}, {Type: "int"}}}, nil},
	} {
		t.Run("hand-built/"+carrier.name, func(t *testing.T) {
			base := driveSurfaces(carrier.node, carrier.val)
			cells++
			if !checkSurfaces(t, "bare carrier", base) {
				t.Fatalf("the bare carrier must build; nothing below means anything otherwise")
			}

			for _, f := range exportedFields {
				t.Run("set/"+f.Name, func(t *testing.T) {
					n := carrier.node
					fv := reflect.ValueOf(&n).Elem().FieldByName(f.Name)
					if !alreadySetForTest(carrier.node, f.Name) {
						if !setNonZeroForTest(f.Name, fv) {
							t.Fatalf("cannot populate %s (kind %s)", f.Name, f.Type.Kind())
						}
					}
					rep := driveSurfaces(n, carrier.val)
					cells++
					if !checkSurfaces(t, "hand-built "+f.Name, rep) {
						return
					}
					// A key the carrier's kind does not bind is INERT: it may
					// change the rendered text, but never the canonical form,
					// the fingerprint, or the wire image. That is the same
					// inertness the attribute-placement census asserts for
					// parsed input, extended to input Parse cannot produce.
					if inertForCarrier(carrier.node.Type, f.Name) {
						if !bytes.Equal(rep.canon, base.canon) {
							t.Errorf("%s is inert on a %s, but it changed the canonical form:\n got %s\nwant %s", f.Name, carrier.node.Type, rep.canon, base.canon)
						}
						if !bytes.Equal(rep.finger, base.finger) {
							t.Errorf("%s is inert on a %s, but it changed the fingerprint", f.Name, carrier.node.Type)
						}
						if carrier.val != nil && !bytes.Equal(rep.jsonWire, base.jsonWire) {
							t.Errorf("%s is inert on a %s, but it changed the JSON wire image: %s vs %s", f.Name, carrier.node.Type, rep.jsonWire, base.jsonWire)
						}
					}
				})
			}
		})
	}

	t.Logf("caller-composed/edited coverage: %d cells across %d structures × %d exported fields × {Schema, String, Canonical, Fingerprint, JSON}",
		cells, len(callerNodeStructures()), len(exportedFields))
}

// re re-parses emitted text and returns its Root, failing the test rather
// than returning an unusable zero node.
func re(t *testing.T, text string) SchemaNode {
	t.Helper()
	s, err := Parse(text)
	if err != nil {
		t.Fatalf("re-parse of emitted text failed: %v\n%s", err, text)
	}
	return *s.Root()
}

// alreadySetForTest reports whether the carrier already populates this field
// as part of being a well-formed node of its kind, in which case overwriting
// it would test a different node rather than a stray addition.
func alreadySetForTest(carrier SchemaNode, field string) bool {
	v := reflect.ValueOf(carrier).FieldByName(field)
	return v.IsValid() && !v.IsZero()
}

// inertForCarrier reports whether a value in field is metadata the carrier's
// kind does not bind, so it can never reach the canonical form or the wire.
// The canonical form keeps only type, name, fields, symbols, items, values
// and size, and only where the kind actually binds them.
func inertForCarrier(kind, field string) bool {
	switch field {
	case "Doc", "Aliases", "LogicalType", "Precision", "Scale", "Props", "EnumDefault", "HasEnumDefault":
		return true
	case "Namespace":
		// A namespace on a NAMED kind is part of its fullname, which the
		// canonical form keeps.
		return !isNamedKind(kind)
	case "Name":
		return !isNamedKind(kind)
	case "Fields":
		return !isRecordKind(kind)
	case "Items":
		return kind != "array"
	case "Values":
		return kind != "map"
	case "Symbols":
		return kind != "enum"
	case "Size":
		return kind != "fixed"
	case "Branches":
		return kind != "union"
	}
	return false
}

// The classification tables the two predicates are derived from must stay in
// step with SchemaNode itself: a name in a table that is not a field is a
// stale classification, and it would silently exempt nothing while reading
// as though it exempted something.
func TestInvariant_NodeFieldRuleTablesNameRealFields(t *testing.T) {
	rt := reflect.TypeFor[SchemaNode]()
	for _, tbl := range []struct {
		name  string
		rules map[string]schemaNodeFieldRule
	}{
		{"bareEmissionFieldRules", bareEmissionFieldRules},
		{"nameRefSpliceFieldRules", nameRefSpliceFieldRules},
	} {
		for field, rule := range tbl.rules {
			f, ok := rt.FieldByName(field)
			if !ok || !f.IsExported() {
				t.Errorf("%s classifies %q, which is not an exported SchemaNode field", tbl.name, field)
				continue
			}
			if rule.exempt == "" && rule.propsKey == "" {
				t.Errorf("%s[%q] states no classification at all; the zero rule is the ordinary case and belongs OUT of the table", tbl.name, field)
			}
			if rule.propsKey != "" && rule.why == "" {
				t.Errorf("%s[%q] relocates a value to Props with no rule quoted; a field whose value does not come back on its own field must name the routing that moved it", tbl.name, field)
			}
			if rule.exempt != "" && rule.propsKey != "" {
				t.Errorf("%s[%q] is both exempt and routed; a field has exactly one classification", tbl.name, field)
			}
		}
	}
	// And the reverse: the splice table's exemptions are the adjudicated
	// usage-site attribute set. Widening it silently is how a caller's write
	// starts vanishing again, so the set itself is pinned.
	want := []string{"Aliases", "Doc", "LogicalType", "Namespace", "Props"}
	var got []string
	for k := range nameRefSpliceFieldRules {
		got = append(got, k)
	}
	if strings.Join(sortedStrings(got), ",") != strings.Join(want, ",") {
		t.Errorf("the name-reference exemption set changed to %v; it is exactly the reserved usage-site attributes a splice drops plus the props it merges, so a change here is a policy change", sortedStrings(got))
	}
}

func sortedStrings(in []string) []string {
	out := append([]string(nil), in...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

// ---------- caller_value_structure_test.go ----------

// ---------------------------------------------------------------------------
// Caller-supplied VALUE domain x STRUCTURE.
//
// Two nets exist on either side of this cross and neither spans it. The
// caller-node matrix drives which FIELD a caller writes across every structure
// but writes one ORDINARY value into each. The tree-value census drives the
// hostile Go value domain — erroring marshalers, cycles, non-string map keys,
// unmarshalable kinds — but at ONE flat position.
//
// The untested claim is that they compose: that a value failing cleanly at a
// flat node also fails cleanly when the node it sits on is a stamped reference
// about to be spliced, a definition inside a recursive cycle, or one arm of a
// diamond. Those paths do extra work with the node — merging props onto a
// spliced definition, walking a visited set, comparing bodies for a dedup
// conflict — and that work runs BEFORE any marshal error surfaces.
//
// The oracle is ABSOLUTE, and it has to be. An earlier version asserted only
// no-panic plus verdict-class AGREEMENT across the members, which any uniform
// change satisfies: removing the bad-map-key guard flipped two values
// reject->accept at every member INCLUDING the flat baseline, so the verdicts
// still agreed and the net stayed green while the map-key regression pin red
// through the same neuter. A baseline that is a member of the agreement set is
// not an anchor.
//
// So each value carries an EXPECTED verdict derived from an authority outside
// this package, and for almost all of them that authority is executed rather
// than written down: the package emits caller values through encoding/json, so
// whether json.Marshal accepts the value decides whether it can reach the wire,
// and the expectation is computed per cell by calling it. Two values have a
// documented package rule that overrides the stdlib and each says so —
// non-finite floats, which a documented fixup rewrites, and a deeply nested
// value, which the documented walk budget refuses.
//
// Agreement across structures is kept as a SECOND assertion, because it catches
// what the absolute one cannot: a verdict that depends on which structure the
// value sits in.
// ---------------------------------------------------------------------------

type cvErrMarshaler struct{}

func (cvErrMarshaler) MarshalJSON() ([]byte, error) { return nil, errors.New("cv-boom") }

type cvBadJSON struct{}

func (cvBadJSON) MarshalJSON() ([]byte, error) { return []byte("{oops"), nil }

type cvErrText struct{}

func (cvErrText) MarshalText() ([]byte, error) { return nil, errors.New("cv-text-boom") }

type cvBadKey struct{ X int }

func cvCyclicMap() any {
	m := map[string]any{}
	m["self"] = m
	return m
}

func cvCyclicSlice() any {
	s := make([]any, 1)
	s[0] = s
	return s
}

func cvDeep(n int) any {
	var v any = 1
	for range n {
		v = []any{v}
	}
	return v
}

// cvHostile is a value plus the authority that settles what must happen to it.
// override is empty when encoding/json decides — which is the usual case, and
// is EXECUTED per cell rather than recorded here, so the expectation tracks the
// stdlib instead of a snapshot of it.
type cvHostile struct {
	name     string
	val      any
	override string // "" | "ok" | "error", matching cvVerdict's vocabulary
	why      string
}

// cvHostileValues is the value domain, drawn from the shapes the tree-value
// census enumerates: a marshal that errors, one that emits invalid JSON, a text
// marshal that errors, unmarshalable kinds, cycles, map keys the stdlib cannot
// name, and sizes that reach the walk budgets.
func cvHostileValues() []cvHostile {
	return []cvHostile{
		{name: "errMarshaler", val: cvErrMarshaler{}},
		{name: "badJSONMarshaler", val: cvBadJSON{}},
		{name: "errTextMarshaler", val: cvErrText{}},
		{name: "func", val: func() {}},
		{name: "chan", val: make(chan int)},
		{name: "complex", val: complex(1, 2)},
		{name: "cyclicMap", val: cvCyclicMap()},
		{name: "cyclicSlice", val: cvCyclicSlice()},
		{name: "floatKeyMap", val: map[float64]string{1.5: "a"}},
		{name: "structKeyMap", val: map[cvBadKey]string{{X: 1}: "a"}},
		{name: "invalidRawMessage", val: json.RawMessage("{oops")},
		{name: "nonNumericJSONNumber", val: json.Number("notanumber")},
		{name: "hugeString", val: strings.Repeat("x", 1<<20)},
		// The depth pair straddles the documented walk budget, measured
		// rather than assumed: an earlier draft used 2000 and expected a
		// refusal, but the bound sits above that, so the cell asserted a
		// rejection that correctly never came.
		{
			name: "deepNest-underBudget", val: cvDeep(2000),
			// no override: the stdlib marshals it and so does this package
		},
		{
			name: "deepNest-overBudget", val: cvDeep(3000), override: "error",
			why: "the stdlib marshals it; this package's documented walk DEPTH budget refuses it first",
		},
		{
			name: "nan", val: math.NaN(), override: "ok",
			why: "the stdlib refuses NaN; this package's documented non-finite fixup rewrites it into a JSON-expressible form",
		},
		{
			name: "posInf", val: math.Inf(1), override: "ok",
			why: "the stdlib refuses +Inf; the same documented fixup emits it as an overflowing numeric literal that re-parses",
		},
		// Deliberately NOT here: a bare nil. It is a LEGAL default whose
		// verdict is decided by the field's TYPE — valid for a nullable
		// union, rejected otherwise — so its class legitimately differs
		// between structures whose first field differs, and holding it
		// constant would make this oracle wrong rather than strict. The
		// hostile domain is values no schema can accept, not values some
		// schema can.
	}
}

// cvExpect returns the required verdict and the authority behind it. With no
// override the authority is encoding/json, CALLED here rather than quoted.
func cvExpect(hv cvHostile, typeChecked bool) (want, authority string) {
	if typeChecked {
		return "error", "a field default is validated against the field's DECLARED TYPE, and no value in this " +
			"hostile domain is a valid instance of it — so marshalability decides nothing here"
	}
	if hv.override != "" {
		return hv.override, hv.why
	}
	if _, err := json.Marshal(hv.val); err != nil {
		return "error", "encoding/json refuses to marshal it, and this package emits caller values through it"
	}
	return "ok", "encoding/json marshals it, so nothing downstream has grounds to refuse it"
}

// cvVerdict reduces a surface report to a CLASS. It deliberately does not
// compare error text: two structures legitimately name different field paths,
// and this net asks whether the outcome KIND depends on structure, not whether
// the message does.
func cvVerdict(rep surfaceReport) string {
	switch {
	case rep.panicked != nil:
		return "PANIC"
	case rep.err != nil:
		return "error"
	default:
		return "ok"
	}
}

// cvSlots are the caller-writable positions that take an arbitrary Go value.
// Every other exported SchemaNode field is typed, so the hostile domain cannot
// reach it. Each slot returns THE NODE TO DRIVE, and that return is why the slot
// is a function rather than a field name: st.pick returns a COPY, exactly as a
// caller gets, so writing into the picked node and then driving the ROOT exercises
// nothing at all. The picked slot is also the only one that reaches the splice —
// a neuter that drops caller props at the splice reds it and nothing else.
var cvSlots = []struct {
	name string
	// typeChecked marks a position whose value is additionally validated
	// against the field's DECLARED TYPE, not just marshalled. A default is;
	// a custom property is not. That changes the authority: for a default,
	// marshalability decides nothing, because a value that marshals fine is
	// still refused unless it is a valid instance of the field's type — and
	// no value in this domain is.
	typeChecked bool
	put         func(root *SchemaNode, picked *SchemaNode, v any) SchemaNode
}{
	{"picked.Props (the spliced reference)", false, func(_ *SchemaNode, n *SchemaNode, v any) SchemaNode {
		if n.Props == nil {
			n.Props = map[string]any{}
		}
		n.Props["hostile"] = v
		return *n
	}},
	{"root.Props", false, func(root *SchemaNode, _ *SchemaNode, v any) SchemaNode {
		if root.Props == nil {
			root.Props = map[string]any{}
		}
		root.Props["hostile"] = v
		return *root
	}},
	{"root.Fields[0].Props", false, func(root *SchemaNode, _ *SchemaNode, v any) SchemaNode {
		if len(root.Fields) > 0 {
			if root.Fields[0].Props == nil {
				root.Fields[0].Props = map[string]any{}
			}
			root.Fields[0].Props["hostile"] = v
		}
		return *root
	}},
	{"root.Fields[0].Default", true, func(root *SchemaNode, _ *SchemaNode, v any) SchemaNode {
		if len(root.Fields) > 0 {
			root.Fields[0].Default = v
			root.Fields[0].HasDefault = true
		}
		return *root
	}},
}

// TestMatrix_CallerValueDomainAcrossStructures crosses the hostile value domain
// with every structure the caller-node matrix builds.
func TestMatrix_CallerValueDomainAcrossStructures(t *testing.T) {
	structures := callerNodeStructures()
	if len(structures) < 3 {
		t.Fatalf("only %d structures; this cross is meaningless without the splice shapes", len(structures))
	}
	var cells int
	for _, slot := range cvSlots {
		for _, hv := range cvHostileValues() {
			// Collect the verdict at every structure, then compare.
			verdicts := make(map[string]string, len(structures)+1)

			// The FLAT baseline is not optional and is not one of the
			// structures: every member of callerNodeStructures() splices, so
			// comparing them only to each other cannot see a change that
			// affects the splice UNIFORMLY. Without this control a neuter that
			// makes the splice drop caller props leaves all five agreeing on
			// "ok" and the net stays green over the exact regression it names.
			flat := MustParse(`{"type":"record","name":"Flat","fields":[{"name":"f","type":"int"}]}`).Root()
			flatPick := flat
			verdicts["flat-baseline"] = cvVerdict(driveSurfaces(slot.put(flat, flatPick, hv.val), nil))
			cells++

			for _, st := range structures {
				s := st.build(t)
				root := s.Root()
				picked := st.pick(*root)
				drive := slot.put(root, &picked, hv.val)
				verdicts[st.name] = cvVerdict(driveSurfaces(drive, st.val))
				cells++
			}
			want, authority := cvExpect(hv, slot.typeChecked)
			// ABSOLUTE first: every member must land on the required verdict.
			// This is what a uniform regression trips; agreement alone does not.
			for name, v := range verdicts {
				if v != "PANIC" && v != want {
					t.Errorf("%s / %s / %s: verdict %q, want %q — %s",
						slot.name, hv.name, name, v, want, authority)
				}
			}
			// Any panic is a failure on its own, named per structure.
			for name, v := range verdicts {
				if v == "PANIC" {
					t.Errorf("%s / %s / %s: PANICKED — a caller value must produce a value or a named error, never a panic",
						slot.name, hv.name, name)
				}
			}
			// The class must not depend on the structure.
			distinct := map[string][]string{}
			for name, v := range verdicts {
				distinct[v] = append(distinct[v], name)
			}
			if len(distinct) > 1 {
				t.Errorf("%s / %s: verdict CLASS depends on the structure — %v.\n"+
					"  A value rejected at one shape must not be accepted at another: the splice merges props onto a\n"+
					"  definition, the recursive walk carries a visited set, and the diamond compares bodies for a dedup\n"+
					"  conflict, all before any marshal error surfaces.", slot.name, hv.name, distinct)
			}
		}
	}
	t.Logf("cells: %d (%d structures × %d slots × %d values)", cells, len(structures), len(cvSlots), len(cvHostileValues()))
}

// ---------- matrix_tree_value_types_test.go ----------

// The values a caller stores in SchemaNode Props / SchemaField.Default (and
// the trees CustomType.Schema hands to SchemaFor) reach Parse through one
// json.Marshal, so their SEMANTICS are defined by their marshal shape — a
// named Go type (`type M map[string]any`, `type A []string`, `type B
// []byte`, a named float) marshals identically to its canonical twin. Every
// pre-marshal consumer (the composition walkers, the render fixups, the
// aliases merge) must therefore treat the named twin exactly like the
// canonical type; the tests in this file pin that parity per consumer.

// TestRegression_TypeAliasAliasesValueGoTypes pins that the type-alias
// tag's merge into an existing aliases attribute does not depend on the
// attribute value's Go dynamic type: a named []string and a [N]string
// array marshal to the identical JSON array of strings that the []any /
// []string forms do, so the merged result must match the []any control.
// (Pre-canonicalization these fell through the merge untouched and Parse
// accepted the marshal — the tag's aliases silently vanished.)
func TestRegression_TypeAliasAliasesValueGoTypes(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	fields := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f,type-alias=Old"`}}
	build := func(t *testing.T, aliasesVal any) []string {
		t.Helper()
		node := &SchemaNode{Type: "fixed", Name: "F", Size: 4,
			Props: map[string]any{"aliases": aliasesVal}}
		s, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: node}})
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		return findNodeAliases(*s.Root(), "F")
	}

	want := build(t, []any{"prior.P"})
	if len(want) != 2 {
		t.Fatalf("control aliases = %#v, want prior.P plus the tag's Old", want)
	}

	type namedStrings []string
	for _, tc := range []struct {
		name string
		v    any
	}{
		{"named_string_slice", namedStrings{"prior.P"}},
		{"string_array", [1]string{"prior.P"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := build(t, tc.v); !reflect.DeepEqual(got, want) {
				t.Errorf("aliases value %T: got %#v, want %#v", tc.v, got, want)
			}
		})
	}
}

// TestRegression_NamedMapItemsDefComposesCanonically pins that a
// Props-carried items definition composes identically whether its Go value
// is map[string]any or a named map type: both marshal to the same JSON
// object that Parse binds as the array's items, so the null-namespace pin
// at the custom frontier and the type-alias routing must see both. (Pre-
// canonicalization the named map was opaque: the "namespace":"" injection
// missed it — silently moving X into the build namespace — and the
// type-alias walk wrong-rejected.)
func TestRegression_NamedMapItemsDefComposesCanonically(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	type anyMap map[string]any
	def := func(wrap func(map[string]any) any) any {
		return wrap(map[string]any{"type": "record", "name": "X",
			"fields": []any{map[string]any{"name": "c", "type": "long"}}})
	}
	build := func(t *testing.T, tag, ns string, items any) (*Schema, error) {
		t.Helper()
		fields := []reflect.StructField{{Name: "F", Type: primary, Tag: reflect.StructTag(tag)}}
		node := &SchemaNode{Type: "array", Props: map[string]any{"items": items}}
		return schemaForScopeCell(t, fields, ns, []CustomType{{GoType: primary, Schema: node}})
	}

	t.Run("namespace_pin_parity", func(t *testing.T) {
		canon, err := build(t, `avro:"f"`, "com.x", def(func(m map[string]any) any { return m }))
		if err != nil {
			t.Fatalf("canonical build: %v", err)
		}
		cpcf := string(canon.Canonical())
		if !strings.Contains(cpcf, `"name":"X"`) {
			t.Fatalf("control lost the null-namespace pin on X: %s", cpcf)
		}
		named, err := build(t, `avro:"f"`, "com.x", def(func(m map[string]any) any { return anyMap(m) }))
		if err != nil {
			t.Fatalf("named-map build: %v", err)
		}
		if npcf := string(named.Canonical()); npcf != cpcf {
			t.Errorf("composed schema depends on the items value's Go dynamic type:\n map[string]any: %s\n named map:      %s", cpcf, npcf)
		}
	})

	t.Run("type_alias_verdict_parity", func(t *testing.T) {
		if _, err := build(t, `avro:"f,type-alias=Old"`, "", def(func(m map[string]any) any { return m })); err != nil {
			t.Fatalf("canonical build: %v", err)
		}
		if _, err := build(t, `avro:"f,type-alias=Old"`, "", def(func(m map[string]any) any { return anyMap(m) })); err != nil {
			t.Errorf("named-map items def wrong-rejects the type-alias tag: %v", err)
		}
	})
}

// TestRegression_NamedBytesPropsRebuildCodepointForm pins that a []byte
// Props value survives the SchemaNode.Schema() rebuild as the Avro
// codepoint-per-byte string regardless of the value's Go dynamic type. A
// named []byte reaching json.Marshal raw becomes base64 TEXT, which the
// re-parse reads as codepoints — silent content change.
func TestRegression_NamedBytesPropsRebuildCodepointForm(t *testing.T) {
	type namedBytes []byte
	build := func(t *testing.T, v any) any {
		t.Helper()
		s := mustNodeSchema(t, (&SchemaNode{Type: "int", Props: map[string]any{"x": v}}))
		return s.Root().Props["x"]
	}

	canon := build(t, []byte{0x01, 0x02, 0x03})
	if canon != "\x01\x02\x03" {
		t.Fatalf("control []byte Props = %#v, want the codepoint string", canon)
	}
	if named := build(t, namedBytes{0x01, 0x02, 0x03}); !reflect.DeepEqual(named, canon) {
		t.Errorf("named []byte Props rebuilds as %#v, canonical []byte as %#v", named, canon)
	}
}

// TestRegression_NamedFloatPropsRebuildSpecials pins the float-special
// fixups across Go dynamic types: the numeric-preserving conversions
// (-0.0, ±Inf) extend to named float kinds, while the type-CHANGING
// NaN→"NaN"-string conversion stays canonical-only — a NAMED float NaN
// keeps json.Marshal's loud unsupported-value error (never a silent
// stringification of a caller's own numeric type).
func TestRegression_NamedFloatPropsRebuildSpecials(t *testing.T) {
	type namedF64 float64
	build := func(t *testing.T, v any) (any, error) {
		t.Helper()
		s, err := (&SchemaNode{Type: "int", Props: map[string]any{"x": v}}).Schema()
		if err != nil {
			return nil, err
		}
		return s.Root().Props["x"], nil
	}

	t.Run("negative_zero", func(t *testing.T) {
		canon, err := build(t, math.Copysign(0, -1))
		if err != nil {
			t.Fatalf("canonical -0.0: %v", err)
		}
		cf, ok := canon.(float64)
		if !ok || cf != 0 || !math.Signbit(cf) {
			t.Fatalf("control -0.0 Props = %#v, want float64 negative zero", canon)
		}
		named, err := build(t, namedF64(math.Copysign(0, -1)))
		if err != nil {
			t.Fatalf("named -0.0: %v", err)
		}
		nf, ok := named.(float64)
		if !ok || nf != 0 || !math.Signbit(nf) {
			t.Errorf("named float -0.0 rebuilds as %#v; the sign must survive as it does for float64", named)
		}
	})

	t.Run("nan_posture", func(t *testing.T) {
		canon, err := build(t, math.NaN())
		if err != nil {
			t.Fatalf("canonical NaN: %v", err)
		}
		if canon != "NaN" {
			t.Fatalf("control NaN Props = %#v, want the documented \"NaN\" string", canon)
		}
		if got, err := build(t, namedF64(math.NaN())); err == nil {
			t.Errorf("named float NaN must keep the loud marshal error, got success with Props = %#v", got)
		}
	})
}

// TestRegression_NamedBytesFieldDefaultValue pins the same codepoint-form
// guarantee for a bytes FIELD DEFAULT — where corruption is wire-visible:
// the rebuilt default auto-fills for absent fields on JSON decode, so the
// materialized bytes must equal the caller's bytes for every Go dynamic
// type of the default value.
func TestRegression_NamedBytesFieldDefaultValue(t *testing.T) {
	type namedBytes []byte
	build := func(t *testing.T, v any) *Schema {
		t.Helper()
		s := mustNodeSchema(t, (&SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{
			{Name: "b", Type: SchemaNode{Type: "bytes"}, Default: v},
		}}))
		return s
	}

	want := []byte{0x01, 0x02, 0x03}
	canon := build(t, []byte{0x01, 0x02, 0x03})
	if got := canon.Root().Fields[0].Default; !reflect.DeepEqual(got, want) {
		t.Fatalf("control []byte default = %#v, want %#v", got, want)
	}
	named := build(t, namedBytes{0x01, 0x02, 0x03})
	if got := named.Root().Fields[0].Default; !reflect.DeepEqual(got, want) {
		t.Errorf("named []byte default rebuilds as %#v, want %#v", got, want)
	}

	var out map[string]any
	if err := named.DecodeJSON([]byte(`{}`), &out); err != nil {
		t.Fatalf("DecodeJSON default fill: %v", err)
	}
	if got, _ := out["b"].([]byte); !reflect.DeepEqual(got, want) {
		t.Errorf("default fill materialized %#v, want %#v", out["b"], want)
	}
}

// Marshal-opaque test types for the matrix below. Each has a CANONICALIZABLE
// kind (slice/map/string) so the exemption is observable: without it the
// kind canonicalization would rewrite the value and discard its marshal.
type tvAliasesMar []string

func (a tvAliasesMar) MarshalJSON() ([]byte, error) { return json.Marshal([]string(a)) }

type tvDefMar map[string]any

func (m tvDefMar) MarshalJSON() ([]byte, error) { return json.Marshal(map[string]any(m)) }

type tvStrMar string

func (s tvStrMar) MarshalJSON() ([]byte, error) { return json.Marshal(string(s) + "!") }

type tvTextStr string

func (s tvTextStr) MarshalText() ([]byte, error) { return []byte(string(s) + "?"), nil }

// TestMatrix_TreeValueGoTypes crosses the Go dynamic type of a caller
// value (Props / Default / rendered custom tree) with every pre-marshal
// consumer of that value. Oracle per cell family: the canonical-twin
// control is anchored to its expected value first, then each variant must
// match the control through a surface that CARRIES the attribute (Root()
// metadata, String(), or PCF where names are the observable — PCF strips
// aliases/doc/props, so those cells observe via Root()). Marshal-opaque
// values (own MarshalJSON/MarshalText) assert the EXEMPTION posture
// instead: their marshal wins, walkers and fixups leave them alone.
func TestMatrix_TreeValueGoTypes(t *testing.T) {
	type (
		namedStrings []string
		namedString  string
		namedSliceA  []any
		namedMap     map[string]any
		namedF64     float64
		namedF32     float32
		namedBytes   []byte
	)

	primary := reflect.TypeFor[scopeMatrixPrimary]()

	t.Run("aliases_merge", func(t *testing.T) {
		fields := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f,type-alias=Old"`}}
		build := func(t *testing.T, aliasesVal any) ([]string, error) {
			t.Helper()
			node := &SchemaNode{Type: "fixed", Name: "F", Size: 4,
				Props: map[string]any{"aliases": aliasesVal}}
			s, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: node}})
			if err != nil {
				return nil, err
			}
			return findNodeAliases(*s.Root(), "F"), nil
		}

		want, err := build(t, []any{"prior.P"})
		if err != nil || !reflect.DeepEqual(want, []string{"prior.P", "Old"}) {
			t.Fatalf("control []any = %#v (%v), want [prior.P Old]", want, err)
		}

		for _, tc := range []struct {
			name string
			v    any
		}{
			{"string_slice", []string{"prior.P"}},
			{"named_string_slice", namedStrings{"prior.P"}},
			{"slice_of_named_string", []namedString{"prior.P"}},
			{"string_array", [1]string{"prior.P"}},
			{"named_slice_of_any", namedSliceA{"prior.P"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got, err := build(t, tc.v)
				if err != nil {
					t.Fatalf("build: %v", err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("aliases value %T: got %#v, want %#v", tc.v, got, want)
				}
			})
		}

		t.Run("json_marshaler_opaque", func(t *testing.T) {
			// A value carrying its own MarshalJSON stays opaque: the merge
			// leaves it alone (a merge would have to marshal it early), so
			// the tag alias is NOT added and the marshal's content is the
			// whole attribute.
			got, err := build(t, tvAliasesMar{"prior.P"})
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			if !reflect.DeepEqual(got, []string{"prior.P"}) {
				t.Errorf("marshal-opaque aliases value: got %#v, want the marshal's [prior.P] untouched", got)
			}
		})

		t.Run("text_marshaler_string_form", func(t *testing.T) {
			// A TextMarshaler at the aliases key marshals as a JSON STRING,
			// not an array — Parse rejects it loudly; never a silent drop.
			if _, err := build(t, tvTextStr("prior.P")); err == nil {
				t.Errorf("TextMarshaler aliases value marshals as a string; Parse must reject")
			}
		})
	})

	t.Run("namespace_pin", func(t *testing.T) {
		fields := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f"`}}
		xDef := func() map[string]any {
			return map[string]any{"type": "record", "name": "X",
				"fields": []any{map[string]any{"name": "c", "type": "long"}}}
		}
		build := func(t *testing.T, items any) string {
			t.Helper()
			node := &SchemaNode{Type: "array", Props: map[string]any{"items": items}}
			s, err := schemaForScopeCell(t, fields, "com.x", []CustomType{{GoType: primary, Schema: node}})
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			return string(s.Canonical())
		}

		control := build(t, xDef())
		if !strings.Contains(control, `"name":"X"`) {
			t.Fatalf("control lost the null-namespace pin on X: %s", control)
		}
		if got := build(t, namedMap(xDef())); got != control {
			t.Errorf("named-map items def composes differently:\n control: %s\n named:   %s", control, got)
		}

		t.Run("json_marshaler_opaque", func(t *testing.T) {
			// An object-emitting MarshalJSON def is opaque to the frontier
			// pin: Parse binds its marshal under the enclosing namespace, so
			// X lands in com.x — the documented residual for marshal-opaque
			// values (use canonical shapes to keep a null namespace).
			got := build(t, tvDefMar(xDef()))
			if !strings.Contains(got, `"name":"com.x.X"`) {
				t.Errorf("marshal-opaque items def: want X bound under com.x (pin stays out of its marshal), got %s", got)
			}
		})
	})

	t.Run("dedup", func(t *testing.T) {
		// Two fields sharing the custom: the named def must dedup to ONE
		// definition plus a reference, identically for canonical and named
		// map values.
		fields := []reflect.StructField{
			{Name: "F", Type: primary, Tag: `avro:"f"`},
			{Name: "G", Type: primary, Tag: `avro:"g"`},
		}
		xDef := func() map[string]any {
			return map[string]any{"type": "record", "name": "X",
				"fields": []any{map[string]any{"name": "c", "type": "long"}}}
		}
		build := func(t *testing.T, items any) string {
			t.Helper()
			node := &SchemaNode{Type: "array", Props: map[string]any{"items": items}}
			s, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: node}})
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			return string(s.Canonical())
		}

		control := build(t, xDef())
		if got := strings.Count(control, `"fields":[{"name":"c"`); got != 1 {
			t.Fatalf("control must contain exactly one X definition, got %d: %s", got, control)
		}
		if got := build(t, namedMap(xDef())); got != control {
			t.Errorf("named-map def dedups differently:\n control: %s\n named:   %s", control, got)
		}
	})

	t.Run("rebuild_props", func(t *testing.T) {
		build := func(t *testing.T, v any) (any, error) {
			t.Helper()
			s, err := (&SchemaNode{Type: "int", Props: map[string]any{"x": v}}).Schema()
			if err != nil {
				return nil, err
			}
			return s.Root().Props["x"], nil
		}
		parity := func(t *testing.T, control, variant any) {
			t.Helper()
			cv, err := build(t, control)
			if err != nil {
				t.Fatalf("control %T: %v", control, err)
			}
			nv, err := build(t, variant)
			if err != nil {
				t.Fatalf("variant %T: %v", variant, err)
			}
			if !reflect.DeepEqual(nv, cv) {
				t.Errorf("%T rebuilds as %#v, canonical %T as %#v", variant, nv, control, cv)
			}
		}

		t.Run("named_bytes", func(t *testing.T) { parity(t, []byte{1, 2, 3}, namedBytes{1, 2, 3}) })
		t.Run("byte_array_as_numbers", func(t *testing.T) { parity(t, []any{1, 2}, [2]byte{1, 2}) })
		t.Run("named_string", func(t *testing.T) { parity(t, "hello", namedString("hello")) })
		t.Run("named_map", func(t *testing.T) {
			parity(t, map[string]any{"k": "v"}, namedMap{"k": "v"})
		})
		t.Run("json_number_number_parity", func(t *testing.T) { parity(t, 1.5, json.Number("1.5")) })
		t.Run("named_f64_negzero", func(t *testing.T) {
			parity(t, math.Copysign(0, -1), namedF64(math.Copysign(0, -1)))
		})
		t.Run("named_f64_posinf", func(t *testing.T) { parity(t, math.Inf(1), namedF64(math.Inf(1))) })
		t.Run("named_f64_neginf", func(t *testing.T) { parity(t, math.Inf(-1), namedF64(math.Inf(-1))) })
		t.Run("named_f32_negzero", func(t *testing.T) {
			parity(t, float32(math.Copysign(0, -1)), namedF32(math.Copysign(0, -1)))
		})
		t.Run("named_f32_posinf", func(t *testing.T) {
			parity(t, float32(math.Inf(1)), namedF32(math.Inf(1)))
		})

		t.Run("raw_message_opaque", func(t *testing.T) {
			// json.RawMessage is []byte-kinded but carries MarshalJSON: its
			// raw JSON splices into the tree — the byte-string fixup must
			// never capture it.
			got, err := build(t, json.RawMessage(`{"a":1}`))
			if err != nil {
				t.Fatalf("RawMessage: %v", err)
			}
			want, err := build(t, map[string]any{"a": 1})
			if err != nil {
				t.Fatalf("map control: %v", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("RawMessage splices its JSON: got %#v, want %#v", got, want)
			}
		})
		t.Run("string_marshaler_opaque", func(t *testing.T) {
			// A string-kinded MarshalJSON carrier keeps its own marshal —
			// canonicalizing it to a plain string would silently drop the
			// method's output.
			got, err := build(t, tvStrMar("hi"))
			if err != nil {
				t.Fatalf("tvStrMar: %v", err)
			}
			if got != "hi!" {
				t.Errorf("MarshalJSON-carrying string: got %#v, want its marshal %q", got, "hi!")
			}
		})
		t.Run("text_marshaler_opaque", func(t *testing.T) {
			got, err := build(t, tvTextStr("hi"))
			if err != nil {
				t.Fatalf("tvTextStr: %v", err)
			}
			if got != "hi?" {
				t.Errorf("MarshalText-carrying string: got %#v, want its marshal %q", got, "hi?")
			}
		})
	})

	t.Run("rebuild_default", func(t *testing.T) {
		// An array-of-strings default: [N]string and []any marshal to the
		// same JSON array, so the rebuilt Default must match.
		build := func(t *testing.T, v any) any {
			t.Helper()
			s, err := (&SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{
				{Name: "a", Type: SchemaNode{Type: "array", Items: &SchemaNode{Type: "string"}}, Default: v},
			}}).Schema()
			if err != nil {
				t.Fatalf("Schema() %T: %v", v, err)
			}
			return s.Root().Fields[0].Default
		}
		control := build(t, []any{"a", "b"})
		if got := build(t, [2]string{"a", "b"}); !reflect.DeepEqual(got, control) {
			t.Errorf("[2]string default rebuilds as %#v, []any control as %#v", got, control)
		}
	})

	t.Run("string_render", func(t *testing.T) {
		// The String() render of a rebuilt schema is type-blind too.
		render := func(t *testing.T, v any) string {
			t.Helper()
			s := mustNodeSchema(t, (&SchemaNode{Type: "int", Props: map[string]any{"x": v}}))
			return s.String()
		}
		control := render(t, map[string]any{"k": "v"})
		if got := render(t, namedMap{"k": "v"}); got != control {
			t.Errorf("String() differs by Props value Go type:\n control: %s\n named:   %s", control, got)
		}
	})

	t.Run("render_props_marshaler", func(t *testing.T) {
		// A marshal-opaque scalar in a custom tree's Props keeps its own
		// marshal through the SchemaFor render — the canonicalizing copy
		// must not rewrite it into its kind's plain form.
		fields := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f"`}}
		node := &SchemaNode{Type: "fixed", Name: "F", Size: 4,
			Props: map[string]any{"x": tvStrMar("hi")}}
		s, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: node}})
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		if got := s.Root().Fields[0].Type.Props["x"]; got != "hi!" {
			t.Errorf("marshal-opaque Props scalar through the render: got %#v, want its marshal %q", got, "hi!")
		}
	})
}

// TestRegression_CyclicNamedMapPropsBudgetError pins the ordering the
// canonicalizing copy relies on: a cyclic Props value — including one
// hiding behind a NAMED map type, which the budget walk must descend by
// KIND — errors out of the budgeted metadata walk before the copy or
// json.Marshal ever see it. Success or a hang here would mean the walk's
// kind dispatch lost the named-container descent.
func TestRegression_CyclicNamedMapPropsBudgetError(t *testing.T) {
	type cycMap map[string]any
	m := cycMap{}
	m["self"] = m
	if _, err := (&SchemaNode{Type: "int", Props: map[string]any{"x": m}}).Schema(); err == nil {
		t.Fatalf("cyclic named-map Props: want the walk's budget error, got success")
	}
	// The SchemaFor render shares the budgeted walk and must error before
	// its canonicalizing copy (which recurses unbudgeted) can see the cycle.
	node := &SchemaNode{Type: "fixed", Name: "F", Size: 4, Props: map[string]any{"x": m}}
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	fields := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f"`}}
	if _, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: node}}); err == nil {
		t.Fatalf("cyclic named-map Props through the SchemaFor render: want the walk's budget error, got success")
	}
}

// ---------------------------------------------------------------------------
// The caller-value domain, enumerated. Arbitrary Go values enter the tree in
// exactly three positions — SchemaNode.Props values, SchemaField.Default,
// SchemaField.Props values, plus whole trees of those via CustomType.Schema and
// via mutating a Root() result — and are consumed pre-marshal by exactly two
// pipelines: the Schema()/String()/Root() rebuild and the SchemaFor render. The
// invariant: the composed schema is a function of the value's MARSHAL IMAGE,
// never of its Go representation — two values with identical json.Marshal output
// must produce identical observable results — except where the marshal is the
// value author's contract (own MarshalJSON/MarshalText, json.Number) or a
// documented fixup owns the image. Controls are anchored to executed values
// before any twin diff, so a cell cannot pass vacuously.

type (
	tvNamedBool    bool
	tvNamedI8      int8
	tvNamedInt     int
	tvNamedU64     uint64
	tvNamedF32     float32
	tvNamedF64     float64
	tvNamedBytes   []byte
	tvNamedStrings []string
	tvNamedMap     map[string]any
	tvNamedSlice   []any
	tvNamedString  string
)

// treeValuePropsObserved composes v as a Props value through the given
// surface and returns the observed metadata value: the direct
// SchemaNode.Schema() rebuild, or the SchemaFor render of a custom tree
// (which adds the canonicalizing copy and the composition walkers).
func treeValuePropsObserved(t *testing.T, surface string, v any) (any, error) {
	t.Helper()
	if surface == "rebuild" {
		s, err := (&SchemaNode{Type: "int", Props: map[string]any{"x": v}}).Schema()
		if err != nil {
			return nil, err
		}
		return s.Root().Props["x"], nil
	}
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	fields := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f"`}}
	node := &SchemaNode{Type: "fixed", Name: "F", Size: 4, Props: map[string]any{"x": v}}
	s, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: node}})
	if err != nil {
		return nil, err
	}
	return s.Root().Fields[0].Type.Props["x"], nil
}

var treeValueSurfaces = []string{"rebuild", "schemafor"}

// TestMatrix_TreeValueLeafTwins crosses leaf-value Go dynamic types with
// both composition surfaces: every variant marshals identically to its
// row's control, so the observed Props value must be identical too. The
// control anchors to the documented read-back contract first
// (SchemaNode.Props: int64 for whole numbers, float64 for fractional,
// json.Number only past int64's range, the []byte codepoint string form).
func TestMatrix_TreeValueLeafTwins(t *testing.T) {
	rows := []struct {
		name     string
		expect   any // anchored control read-back
		control  any
		variants []any
		image    string // non-empty: assert every value's marshal image first
	}{
		{name: "named_bool", expect: true, control: true,
			variants: []any{tvNamedBool(true)}},
		{name: "int_widths_42", expect: int64(42), control: int64(42),
			variants: []any{int8(42), int16(42), int32(42), int(42),
				uint8(42), uint16(42), uint32(42), uint(42), uint64(42),
				json.Number("42"), tvNamedI8(42), tvNamedInt(42)},
			image: "42"},
		{name: "int64_max", expect: int64(math.MaxInt64), control: int64(math.MaxInt64),
			variants: []any{json.Number("9223372036854775807")}},
		{name: "int64_past_float53", expect: int64(1<<53 + 1), control: int64(1<<53 + 1),
			variants: []any{json.Number("9007199254740993")}},
		{name: "uint64_max", expect: json.Number("18446744073709551615"),
			control:  json.Number("18446744073709551615"),
			variants: []any{uint64(math.MaxUint64), tvNamedU64(math.MaxUint64)}},
		{name: "float_tenth", expect: float64(0.1), control: float64(0.1),
			variants: []any{float32(0.1), tvNamedF32(0.1), tvNamedF64(0.1)},
			image:    "0.1"},
		{name: "empty_json_number_is_zero", expect: int64(0), control: int64(0),
			variants: []any{json.Number("")}},
		{name: "typed_nils", expect: nil, control: nil,
			variants: []any{(*int)(nil), json.RawMessage(nil)}},
		{name: "nil_bytes_empty_codepoint", expect: "", control: []byte(nil),
			variants: []any{tvNamedBytes(nil)}},
		{name: "empty_map", expect: map[string]any{}, control: map[string]any{},
			variants: []any{tvNamedMap{}}},
		{name: "empty_slice", expect: []any{}, control: []any{},
			variants: []any{[0]string{}, tvNamedStrings{}, tvNamedSlice{}}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			if row.image != "" {
				for _, v := range append([]any{row.control}, row.variants...) {
					b, err := json.Marshal(v)
					if err != nil || string(b) != row.image {
						t.Fatalf("twin premise: %T marshals %s (%v), want %s", v, b, err, row.image)
					}
				}
			}
			for _, surface := range treeValueSurfaces {
				control, err := treeValuePropsObserved(t, surface, row.control)
				if err != nil {
					t.Fatalf("%s control %T: %v", surface, row.control, err)
				}
				if !reflect.DeepEqual(control, row.expect) {
					t.Fatalf("%s anchored control: got %#v, want %#v", surface, control, row.expect)
				}
				for _, v := range row.variants {
					got, err := treeValuePropsObserved(t, surface, v)
					if err != nil {
						t.Fatalf("%s %T: %v", surface, v, err)
					}
					if !reflect.DeepEqual(got, control) {
						t.Errorf("%s: %T observed %#v, control %T observed %#v",
							surface, v, got, row.control, control)
					}
				}
			}
		})
	}
}

// TestMatrix_TreeValueContainerTwins: container shapes whose marshal images
// coincide must compose identically through both surfaces, including
// fixup-carrying content under named or array wrappers, at any nesting
// depth.
func TestMatrix_TreeValueContainerTwins(t *testing.T) {
	rows := []struct {
		name          string
		control, twin any
		image         bool // both values marshal; assert identical images
	}{
		{name: "deep_named_nesting",
			control: map[string]any{
				"bs":   []any{[]byte{9}, []byte{8}},
				"deep": map[string]any{"ss": []any{"a"}, "m": map[string]any{"b": []byte{7}}},
			},
			twin: tvNamedMap{
				"bs":   []tvNamedBytes{{9}, {8}},
				"deep": tvNamedMap{"ss": tvNamedStrings{"a"}, "m": tvNamedMap{"b": tvNamedBytes{7}}},
			},
			image: true},
		{name: "slice_of_named_bytes",
			control: []any{[]byte{9}, []byte{8}}, twin: []tvNamedBytes{{9}, {8}}, image: true},
		{name: "array_carrying_inf",
			control: []any{math.Inf(1), "x"}, twin: [2]any{math.Inf(1), "x"}},
		{name: "one_elem_string_array",
			control: []any{"a"}, twin: [1]string{"a"}, image: true},
		{name: "array_of_named_string",
			control: []any{"a", "b"}, twin: [2]tvNamedString{"a", "b"}, image: true},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			if row.image {
				cb, cerr := json.Marshal(row.control)
				tb, terr := json.Marshal(row.twin)
				if cerr != nil || terr != nil || string(cb) != string(tb) {
					t.Fatalf("twin premise: images differ or fail: %s (%v) vs %s (%v)", cb, cerr, tb, terr)
				}
			}
			var acrossSurfaces []any
			for _, surface := range treeValueSurfaces {
				control, err := treeValuePropsObserved(t, surface, row.control)
				if err != nil {
					t.Fatalf("%s control: %v", surface, err)
				}
				if control == nil {
					t.Fatalf("%s control observed nil; the anchor is gone", surface)
				}
				got, err := treeValuePropsObserved(t, surface, row.twin)
				if err != nil {
					t.Fatalf("%s twin: %v", surface, err)
				}
				if !reflect.DeepEqual(got, control) {
					t.Errorf("%s: twin observed %#v, control %#v", surface, got, control)
				}
				acrossSurfaces = append(acrossSurfaces, control)
			}
			if !reflect.DeepEqual(acrossSurfaces[0], acrossSurfaces[1]) {
				t.Errorf("surfaces disagree on the control: rebuild %#v, schemafor %#v",
					acrossSurfaces[0], acrossSurfaces[1])
			}
		})
	}
}

// TestMatrix_TreeValueDefaultWire: field defaults with identical marshal
// images must materialize identical auto-filled values on JSON decode of an
// input missing the field — the wire-visible consequence of the composed
// default.
func TestMatrix_TreeValueDefaultWire(t *testing.T) {
	fill := func(t *testing.T, fieldType SchemaNode, def any) any {
		t.Helper()
		s, err := (&SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{
			{Name: "v", Type: fieldType, Default: def, HasDefault: true},
		}}).Schema()
		if err != nil {
			t.Fatalf("Schema() with %T default: %v", def, err)
		}
		var out map[string]any
		if err := s.DecodeJSON([]byte(`{}`), &out); err != nil {
			t.Fatalf("DecodeJSON fill with %T default: %v", def, err)
		}
		return out["v"]
	}

	t.Run("long_width_twins", func(t *testing.T) {
		long := SchemaNode{Type: "long"}
		control := fill(t, long, int64(42))
		if control != int64(42) {
			t.Fatalf("anchored control: long fill = %#v, want int64(42)", control)
		}
		for _, v := range []any{int8(42), json.Number("42"), tvNamedI8(42)} {
			if got := fill(t, long, v); !reflect.DeepEqual(got, control) {
				t.Errorf("%T default fills %#v, control %#v", v, got, control)
			}
		}
	})

	t.Run("string_array_twin", func(t *testing.T) {
		arr := SchemaNode{Type: "array", Items: &SchemaNode{Type: "string"}}
		control := fill(t, arr, []any{"a", "b"})
		if got := fill(t, arr, [2]string{"a", "b"}); !reflect.DeepEqual(got, control) {
			t.Errorf("[2]string default fills %#v, []any control %#v", got, control)
		}
	})

	t.Run("record_map_twin", func(t *testing.T) {
		rec := SchemaNode{Type: "record", Name: "S", Fields: []SchemaField{
			{Name: "c", Type: SchemaNode{Type: "long"}},
		}}
		control := fill(t, rec, map[string]any{"c": 7})
		if got := fill(t, rec, tvNamedMap{"c": 7}); !reflect.DeepEqual(got, control) {
			t.Errorf("named-map default fills %#v, map control %#v", got, control)
		}
	})
}

// TestMatrix_TreeValueFieldProps pins the SchemaField.Props position: field
// property values follow the same marshal-image contract as node Props.
func TestMatrix_TreeValueFieldProps(t *testing.T) {
	build := func(t *testing.T, v any) any {
		t.Helper()
		s, err := (&SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{
			{Name: "n", Type: SchemaNode{Type: "long"}, Props: map[string]any{"x": v}},
		}}).Schema()
		if err != nil {
			t.Fatalf("Schema() with %T field prop: %v", v, err)
		}
		return s.Root().Fields[0].Props["x"]
	}
	t.Run("named_bytes", func(t *testing.T) {
		control := build(t, []byte{1, 2, 3})
		if control != "\x01\x02\x03" {
			t.Fatalf("anchored control: field-Props []byte = %#v, want the codepoint string", control)
		}
		if got := build(t, tvNamedBytes{1, 2, 3}); !reflect.DeepEqual(got, control) {
			t.Errorf("named bytes field prop observed %#v, control %#v", got, control)
		}
	})
	t.Run("named_map", func(t *testing.T) {
		control := build(t, map[string]any{"k": "v"})
		if got := build(t, tvNamedMap{"k": "v"}); !reflect.DeepEqual(got, control) {
			t.Errorf("named map field prop observed %#v, control %#v", got, control)
		}
	})
}

// TestMatrix_TreeValueVerdictParity: where a tree value draws an
// accept/reject verdict, the verdict must not depend on the value's Go
// dynamic type, and must agree across both composition surfaces.
func TestMatrix_TreeValueVerdictParity(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	fields := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f"`}}

	t.Run("bad_long_default", func(t *testing.T) {
		mk := func(def any) *SchemaNode {
			return &SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{
				{Name: "n", Type: SchemaNode{Type: "long"}, Default: def, HasDefault: true},
			}}
		}
		for _, def := range []any{"x", tvNamedString("x")} {
			if _, err := mk(def).Schema(); err == nil {
				t.Errorf("%T string default for long via node.Schema(): want reject", def)
			}
			if _, err := schemaForScopeCell(t, fields, "",
				[]CustomType{{GoType: primary, Schema: mk(def)}}); err == nil {
				t.Errorf("%T string default for long via SchemaFor: want reject", def)
			}
		}
	})

	t.Run("unmarshalable_kinds_loud", func(t *testing.T) {
		for _, v := range []any{make(chan int), complex(1, 2)} {
			if _, err := (&SchemaNode{Type: "int", Props: map[string]any{"x": v}}).Schema(); err == nil {
				t.Errorf("%T Props via node.Schema(): want a loud error, got success", v)
			}
			node := &SchemaNode{Type: "fixed", Name: "F", Size: 4, Props: map[string]any{"x": v}}
			if _, err := schemaForScopeCell(t, fields, "",
				[]CustomType{{GoType: primary, Schema: node}}); err == nil {
				t.Errorf("%T Props via SchemaFor: want a loud error, got success", v)
			}
		}
	})

	t.Run("nil_map_at_structural_key", func(t *testing.T) {
		// A typed-nil map at a STRUCTURAL Props key composes as null,
		// which is never a valid schema there — the verdict is a loud
		// reject on both surfaces (nil-ness at structural positions can
		// change no accepted output).
		node := &SchemaNode{Type: "array", Props: map[string]any{"items": map[string]any(nil)}}
		if _, err := (&SchemaNode{Type: "array", Props: map[string]any{"items": map[string]any(nil)}}).Schema(); err == nil {
			t.Errorf("nil map as items via node.Schema(): want reject, got success")
		}
		if _, err := schemaForScopeCell(t, fields, "",
			[]CustomType{{GoType: primary, Schema: node}}); err == nil {
			t.Errorf("nil map as items via SchemaFor: want reject, got success")
		}
	})

	t.Run("reserved_key_clobber_twins", func(t *testing.T) {
		// Whatever the policy for a caller Props key that collides with a
		// reserved attribute, it cannot depend on the value's Go type.
		build := func(v any) (string, error) {
			node := &SchemaNode{Type: "fixed", Name: "F", Size: 4,
				Props: map[string]any{"name": v}}
			s, err := schemaForScopeCell(t, fields, "",
				[]CustomType{{GoType: primary, Schema: node}})
			if err != nil {
				return "", err
			}
			return string(s.Canonical()), nil
		}
		canon, cErr := build("Q")
		named, nErr := build(tvNamedString("Q"))
		if (cErr == nil) != (nErr == nil) {
			t.Fatalf("verdict diverges: plain err=%v, named err=%v", cErr, nErr)
		}
		if canon != named {
			t.Errorf("clobber result diverges:\n plain: %s\n named: %s", canon, named)
		}
	})
}

// TestRegression_TreeValueOwnershipBoundary pins the ownership contract at
// the composition boundary: a build never writes into caller storage (no
// namespace injection into a caller def map, no append into a caller
// slice's spare capacity), and a value SHARED across two Props keys
// composes exactly like two independent equal values.
func TestRegression_TreeValueOwnershipBoundary(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()

	t.Run("diamond_shared_def", func(t *testing.T) {
		fields := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f"`}}
		mkDef := func() map[string]any {
			return map[string]any{"type": "record", "name": "X",
				"fields": []any{map[string]any{"name": "c", "type": "long"}}}
		}
		shared := mkDef()
		node := &SchemaNode{Type: "array",
			Props: map[string]any{"items": shared, "alsoitems": shared}}
		s, err := schemaForScopeCell(t, fields, "com.x",
			[]CustomType{{GoType: primary, Schema: node}})
		if err != nil {
			t.Fatalf("diamond build: %v", err)
		}
		if _, leaked := shared["namespace"]; leaked {
			t.Errorf("build mutated the shared caller map: %#v", shared)
		}
		node2 := &SchemaNode{Type: "array",
			Props: map[string]any{"items": mkDef(), "alsoitems": mkDef()}}
		s2, err := schemaForScopeCell(t, fields, "com.x",
			[]CustomType{{GoType: primary, Schema: node2}})
		if err != nil {
			t.Fatalf("independent twin build: %v", err)
		}
		if a, b := s.String(), s2.String(); a != b {
			t.Errorf("shared-value diamond composes differently from independent copies:\n shared:      %s\n independent: %s", a, b)
		}
	})

	t.Run("aliases_spare_capacity", func(t *testing.T) {
		aliasFields := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f,type-alias=Old"`}}
		backing := make(tvNamedStrings, 1, 3)
		backing[0] = "prior.P"
		backing = backing[:3]
		backing[1], backing[2] = "SENTINEL1", "SENTINEL2"
		arg := backing[:1]

		build := func(v any) (string, error) {
			node := &SchemaNode{Type: "fixed", Name: "F", Size: 4,
				Props: map[string]any{"aliases": v}}
			s, err := schemaForScopeCell(t, aliasFields, "",
				[]CustomType{{GoType: primary, Schema: node}})
			if err != nil {
				return "", err
			}
			return s.String(), nil
		}
		got, err := build(arg)
		if err != nil {
			t.Fatalf("spare-capacity build: %v", err)
		}
		exact, err := build(tvNamedStrings{"prior.P"})
		if err != nil {
			t.Fatalf("exact-capacity build: %v", err)
		}
		if got != exact {
			t.Errorf("spare-capacity twin diverges:\n spare: %s\n exact: %s", got, exact)
		}
		if backing[1] != "SENTINEL1" || backing[2] != "SENTINEL2" {
			t.Errorf("build wrote into the caller backing array past len: %#v", backing)
		}
	})
}

// tvTwinGen interprets a fuzz byte program as a bounded value generator
// producing a (canonical, named-twin) pair whose marshal images are
// identical by construction — including nil and empty containers, whose
// nil-ness is part of the image (null vs {}/[]). Wrapper choices ride the
// program's high bits; wrapAll forces wrapping so short programs still
// produce named shapes. The domain deliberately excludes the documented
// image-owning shapes (values with their own MarshalJSON/MarshalText,
// json.Number, NaN).
type tvTwinGen struct {
	prog    []byte
	i       int
	wrapAll bool
}

func (g *tvTwinGen) next() byte {
	if g.i >= len(g.prog) {
		return 0
	}
	b := g.prog[g.i]
	g.i++
	return b
}

func (g *tvTwinGen) build(depth int, budget *int) (any, any) {
	if *budget <= 0 || depth >= 3 {
		return "leaf", "leaf"
	}
	*budget--
	op := g.next()
	wrap := g.wrapAll || op&0x80 != 0
	switch op % 9 {
	case 0:
		s := string(rune('a' + int(op>>4)%3))
		if wrap {
			return s, tvNamedString(s)
		}
		return s, s
	case 1:
		n := int64(int8(g.next()))
		if wrap {
			return n, int8(n)
		}
		return n, n
	case 2:
		// Finite float chosen float32-exact so width twins share an image.
		fv := float64(int8(g.next())) / 4
		if wrap {
			return fv, tvNamedF64(fv)
		}
		return fv, fv
	case 3:
		b := op&0x40 != 0
		if wrap {
			return b, tvNamedBool(b)
		}
		return b, b
	case 4:
		bs := []byte{g.next(), g.next()}
		if wrap {
			return bs, tvNamedBytes(bs)
		}
		return bs, bs
	case 5:
		// ±Inf: the numeric-preserving fixups extend to named float kinds.
		fv := math.Inf(1)
		if op&0x40 != 0 {
			fv = math.Inf(-1)
		}
		if wrap {
			return fv, tvNamedF64(fv)
		}
		return fv, fv
	case 6:
		n := int(op>>4) % 3
		if n == 0 {
			if op&0x40 != 0 {
				if wrap {
					return map[string]any(nil), tvNamedMap(nil)
				}
				return map[string]any(nil), map[string]any(nil)
			}
			if wrap {
				return map[string]any{}, tvNamedMap{}
			}
			return map[string]any{}, map[string]any{}
		}
		cm := make(map[string]any, n)
		nm := make(map[string]any, n)
		for i := range n {
			k := string(rune('k' + i))
			cv, nv := g.build(depth+1, budget)
			cm[k] = cv
			nm[k] = nv
		}
		if wrap {
			return cm, tvNamedMap(nm)
		}
		return cm, nm
	case 7:
		n := int(op>>4) % 3
		if n == 0 {
			if op&0x40 != 0 {
				if wrap {
					return []any(nil), tvNamedSlice(nil)
				}
				return []any(nil), []any(nil)
			}
			if wrap {
				return []any{}, tvNamedSlice{}
			}
			return []any{}, []any{}
		}
		cs := make([]any, n)
		ns := make([]any, n)
		for i := range n {
			cs[i], ns[i] = g.build(depth+1, budget)
		}
		if wrap {
			return cs, tvNamedSlice(ns)
		}
		return cs, ns
	default:
		n := int(op>>4) % 3
		if n == 0 {
			if op&0x40 != 0 {
				if wrap {
					return []string(nil), tvNamedStrings(nil)
				}
				return []string(nil), []string(nil)
			}
			if wrap {
				return []string{}, tvNamedStrings{}
			}
			return []string{}, []string{}
		}
		ss := make([]string, n)
		for i := range ss {
			ss[i] = string(rune('a' + i))
		}
		if wrap {
			return ss, tvNamedStrings(append([]string(nil), ss...))
		}
		return ss, append([]string(nil), ss...)
	}
}

// FuzzTreeValueTwinParity fuzzes the caller-value domain of the composition
// surface: a generated canonical value and its named twin (identical
// marshal images) must draw the same accept/reject verdict, produce the
// same rendered schema text and observed metadata, and the rendered text
// must be a Parse fixed point — through the Props rebuild, the field
// Default position, and the SchemaFor render.
func FuzzTreeValueTwinParity(f *testing.F) {
	f.Add([]byte{0}, true)
	f.Add([]byte{6, 2, 1, 7, 3, 0xC1, 5}, true)
	f.Add([]byte{7, 3, 0x86, 2, 0x81, 4}, false)
	f.Add([]byte{6, 1, 8, 2, 0x83, 0x84}, true)
	f.Add([]byte{5, 0xFF, 6, 1, 5, 1}, true)
	f.Add([]byte{4, 9, 8, 6, 2, 0, 1}, true)
	f.Add([]byte{0x69}, true)        // nil named map
	f.Add([]byte{0x08, 0x6B}, true)  // empty then nil []string, named twins
	f.Add([]byte{0x61, 0x33}, false) // nil []any; empty map
	f.Fuzz(func(t *testing.T, prog []byte, wrapAll bool) {
		if len(prog) > 48 {
			prog = prog[:48]
		}
		g := &tvTwinGen{prog: prog, wrapAll: wrapAll}
		budget := 20
		canon, named := g.build(0, &budget)
		cImg, cErr := json.Marshal(canon)
		nImg, nErr := json.Marshal(named)
		if (cErr == nil) != (nErr == nil) {
			t.Fatalf("twin marshal verdicts diverge: canonical %v, named %v", cErr, nErr)
		}
		if cErr == nil && string(cImg) != string(nImg) {
			t.Fatalf("generator twin premise broken:\n canon: %s\n named: %s", cImg, nImg)
		}

		check := func(label string, run func(v any) (string, any, error)) {
			t.Helper()
			cs, cObs, cErrr := run(canon)
			ns, nObs, nErrr := run(named)
			if (cErrr == nil) != (nErrr == nil) {
				t.Fatalf("%s verdict diverges: canonical %v, named %v", label, cErrr, nErrr)
			}
			if cErrr != nil {
				return
			}
			if cs != ns {
				t.Fatalf("%s rendered text diverges:\n canon: %s\n named: %s", label, cs, ns)
			}
			if !reflect.DeepEqual(cObs, nObs) {
				t.Fatalf("%s observed metadata diverges: %#v vs %#v", label, cObs, nObs)
			}
			s2, err := Parse(cs)
			if err != nil {
				t.Fatalf("%s rendered schema does not reparse: %v\n%s", label, err, cs)
			}
			if s2.String() != cs {
				t.Fatalf("%s String() is not a Parse fixed point:\n first: %s\n again: %s", label, cs, s2.String())
			}
		}

		check("props-rebuild", func(v any) (string, any, error) {
			s, err := (&SchemaNode{Type: "int", Props: map[string]any{"x": v}}).Schema()
			if err != nil {
				return "", nil, err
			}
			return s.String(), s.Root().Props["x"], nil
		})
		check("field-default", func(v any) (string, any, error) {
			s, err := (&SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{
				{Name: "v", Type: SchemaNode{Type: "long"}, Default: v, HasDefault: true},
			}}).Schema()
			if err != nil {
				return "", nil, err
			}
			return s.String(), s.Root().Fields[0].Default, nil
		})
		check("schemafor-render", func(v any) (string, any, error) {
			primary := reflect.TypeFor[scopeMatrixPrimary]()
			flds := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f"`}}
			node := &SchemaNode{Type: "fixed", Name: "F", Size: 4, Props: map[string]any{"x": v}}
			s, err := schemaForScopeCell(t, flds, "", []CustomType{{GoType: primary, Schema: node}})
			if err != nil {
				return "", nil, err
			}
			return s.String(), s.Root().Fields[0].Type.Props["x"], nil
		})
	})
}

// ---------------------------------------------------------------------------
// Nil-ness is part of the marshal image: a nil map/slice marshals as null
// and a non-nil empty one as {}/[], so the boundary copy must preserve
// nil-ness in BOTH directions — nil in, nil out; empty in, empty out — for
// the exact container arms and the named-kind canonicalization alike. The
// pins and matrix below hold that across surface (node.Schema() vs the
// SchemaFor render) and position (Props, field Default).

// treeValueSchemaForRecord composes a record-typed custom tree through
// SchemaFor and returns the composed schema (the record lands as the one
// field's type).
func treeValueSchemaForRecord(t *testing.T, node *SchemaNode) (*Schema, error) {
	t.Helper()
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	fields := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f"`}}
	return schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: node}})
}

// TestMatrix_NilContainerPropsPreserveNullImage: a nil container Props
// value (marshal image null) must survive the SchemaFor render exactly as
// it survives the direct rebuild — null in the composed JSON, nil in the
// re-read metadata.
func TestMatrix_NilContainerPropsPreserveNullImage(t *testing.T) {
	for _, tc := range []struct {
		name string
		v    any
	}{
		{"nil_map", map[string]any(nil)},
		{"nil_any_slice", []any(nil)},
		{"nil_string_slice", []string(nil)},
		{"nil_field_map_slice", []map[string]any(nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			direct, err := treeValuePropsObserved(t, "rebuild", tc.v)
			if err != nil {
				t.Fatalf("rebuild: %v", err)
			}
			if direct != nil {
				t.Fatalf("anchored control: rebuild of %T gives %#v, want nil (image null)", tc.v, direct)
			}
			composed, err := treeValuePropsObserved(t, "schemafor", tc.v)
			if err != nil {
				t.Fatalf("SchemaFor: %v", err)
			}
			if composed != nil {
				t.Errorf("SchemaFor render changes the nil image of %T: got %#v, want nil", tc.v, composed)
			}
		})
	}
}

// TestRegression_NilNamedContainerTwinImage: named nil containers marshal
// null exactly like their canonical twins, so both must compose to nil.
func TestRegression_NilNamedContainerTwinImage(t *testing.T) {
	for _, tc := range []struct {
		name           string
		canon, variant any
	}{
		{"named_nil_string_slice", []string(nil), tvNamedStrings(nil)},
		{"named_nil_map", map[string]any(nil), tvNamedMap(nil)},
		{"named_nil_any_slice", []any(nil), tvNamedSlice(nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, surface := range treeValueSurfaces {
				canon, err := treeValuePropsObserved(t, surface, tc.canon)
				if err != nil {
					t.Fatalf("%s canonical: %v", surface, err)
				}
				got, err := treeValuePropsObserved(t, surface, tc.variant)
				if err != nil {
					t.Fatalf("%s named: %v", surface, err)
				}
				if !reflect.DeepEqual(got, canon) {
					t.Errorf("%s: named nil observed %#v, canonical nil observed %#v", surface, got, canon)
				}
			}
		})
	}
}

// TestRegression_EmptyStringSlicePreservesEmptyImage: the inverse
// direction — a non-nil empty []string marshals as [], and the copy must
// not collapse it to nil (null).
func TestRegression_EmptyStringSlicePreservesEmptyImage(t *testing.T) {
	direct, err := treeValuePropsObserved(t, "rebuild", []string{})
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if !reflect.DeepEqual(direct, []any{}) {
		t.Fatalf("anchored control: rebuild of []string{} gives %#v, want []any{} (image [])", direct)
	}
	composed, err := treeValuePropsObserved(t, "schemafor", []string{})
	if err != nil {
		t.Fatalf("SchemaFor: %v", err)
	}
	if !reflect.DeepEqual(composed, direct) {
		t.Errorf("SchemaFor render changes the empty image of []string{}: got %#v, want %#v", composed, direct)
	}
}

// TestRegression_NilMapUnionDefaultBuildsBothSurfaces: the build verdict on
// an identical tree cannot depend on the surface. A ["null","long"] union
// field default of a nil map marshals as null — a valid default — so both
// the direct rebuild and the SchemaFor render must accept it, and the
// auto-filled JSON-decode value is nil.
func TestRegression_NilMapUnionDefaultBuildsBothSurfaces(t *testing.T) {
	mkNode := func() *SchemaNode {
		return &SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{
			{Name: "u", Type: SchemaNode{Type: "union", Branches: []SchemaNode{
				{Type: "null"}, {Type: "long"},
			}}, Default: map[string]any(nil), HasDefault: true},
		}}
	}
	s, err := mkNode().Schema()
	if err != nil {
		t.Fatalf("node.Schema() nil-map union default: %v", err)
	}
	var out map[string]any
	if err := s.DecodeJSON([]byte(`{}`), &out); err != nil {
		t.Fatalf("DecodeJSON fill: %v", err)
	}
	if got, ok := out["u"]; !ok || got != nil {
		t.Fatalf("anchored control: null default fills %#v, want nil", got)
	}
	if _, err := treeValueSchemaForRecord(t, mkNode()); err != nil {
		t.Errorf("SchemaFor rejects the identical nil-map union default the rebuild accepts: %v", err)
	}
}

// TestMatrix_TreeValueNilEmptyImage: container kind × {nil, empty} ×
// {canonical, named} × surface × position. Per cell: the two surfaces
// agree, the named twin matches the canonical, and the observed value is
// the anchored expectation (nil for image null; empty map/slice for image
// {}/[]).
func TestMatrix_TreeValueNilEmptyImage(t *testing.T) {
	type variant struct {
		name string
		v    any
	}
	rows := []struct {
		name     string
		expect   any // observed Props value; anchored
		variants []variant
		// Default-position field type and expected observed Default.
		defType   SchemaNode
		defExpect any
	}{
		{name: "nil_map", expect: nil,
			variants:  []variant{{"canonical", map[string]any(nil)}, {"named", tvNamedMap(nil)}},
			defType:   SchemaNode{Type: "union", Branches: []SchemaNode{{Type: "null"}, {Type: "long"}}},
			defExpect: nil},
		{name: "nil_any_slice", expect: nil,
			variants:  []variant{{"canonical", []any(nil)}, {"named", tvNamedSlice(nil)}},
			defType:   SchemaNode{Type: "union", Branches: []SchemaNode{{Type: "null"}, {Type: "long"}}},
			defExpect: nil},
		{name: "nil_string_slice", expect: nil,
			variants:  []variant{{"canonical", []string(nil)}, {"named", tvNamedStrings(nil)}},
			defType:   SchemaNode{Type: "union", Branches: []SchemaNode{{Type: "null"}, {Type: "long"}}},
			defExpect: nil},
		{name: "empty_map", expect: map[string]any{},
			variants:  []variant{{"canonical", map[string]any{}}, {"named", tvNamedMap{}}},
			defType:   SchemaNode{Type: "map", Values: &SchemaNode{Type: "long"}},
			defExpect: map[string]any{}},
		{name: "empty_any_slice", expect: []any{},
			variants:  []variant{{"canonical", []any{}}, {"named", tvNamedSlice{}}},
			defType:   SchemaNode{Type: "array", Items: &SchemaNode{Type: "string"}},
			defExpect: []any{}},
		{name: "empty_string_slice", expect: []any{},
			variants:  []variant{{"canonical", []string{}}, {"named", tvNamedStrings{}}},
			defType:   SchemaNode{Type: "array", Items: &SchemaNode{Type: "string"}},
			defExpect: []any{}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			t.Run("props", func(t *testing.T) {
				var perSurface [2]any
				for si, surface := range treeValueSurfaces {
					var first any
					for vi, va := range row.variants {
						got, err := treeValuePropsObserved(t, surface, va.v)
						if err != nil {
							t.Fatalf("%s %s: %v", surface, va.name, err)
						}
						if vi == 0 {
							first = got
							if !reflect.DeepEqual(got, row.expect) {
								t.Fatalf("%s anchored control: got %#v, want %#v", surface, got, row.expect)
							}
						} else if !reflect.DeepEqual(got, first) {
							t.Errorf("%s: %s observed %#v, canonical observed %#v", surface, va.name, got, first)
						}
					}
					perSurface[si] = first
				}
				if !reflect.DeepEqual(perSurface[0], perSurface[1]) {
					t.Errorf("surfaces disagree: rebuild %#v, schemafor %#v", perSurface[0], perSurface[1])
				}
			})
			t.Run("default", func(t *testing.T) {
				mkNode := func(def any) *SchemaNode {
					return &SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{
						{Name: "v", Type: row.defType, Default: def, HasDefault: true},
					}}
				}
				var perSurface [2]any
				for si, surface := range treeValueSurfaces {
					var first any
					for vi, va := range row.variants {
						var s *Schema
						var err error
						if surface == "rebuild" {
							s, err = mkNode(va.v).Schema()
						} else {
							s, err = treeValueSchemaForRecord(t, mkNode(va.v))
						}
						if err != nil {
							t.Fatalf("%s %s: %v", surface, va.name, err)
						}
						root := s.Root()
						var got any
						if surface == "rebuild" {
							got = root.Fields[0].Default
						} else {
							got = root.Fields[0].Type.Fields[0].Default
						}
						if vi == 0 {
							first = got
							if !reflect.DeepEqual(got, row.defExpect) {
								t.Fatalf("%s anchored control default: got %#v, want %#v", surface, got, row.defExpect)
							}
						} else if !reflect.DeepEqual(got, first) {
							t.Errorf("%s: %s default observed %#v, canonical observed %#v", surface, va.name, got, first)
						}
					}
					perSurface[si] = first
				}
				if !reflect.DeepEqual(perSurface[0], perSurface[1]) {
					t.Errorf("surfaces disagree on the default: rebuild %#v, schemafor %#v", perSurface[0], perSurface[1])
				}
			})
		})
	}
}

// ---------------------------------------------------------------------------
// Map keys of STRING KIND always marshal as their raw string under
// encoding/json — the key resolver checks the string kind before consulting
// TextMarshaler — so a map whose key type is string-kind, with or without a
// MarshalText method, is image-identical to the plain map[string]any twin and
// canonicalizes to it: the walkers see its defs, the fixups reach its values,
// and the composed output does not depend on which stdlib JSON implementation a
// future toolchain ships. NON-string-kind keys with MarshalText keep the method
// on every toolchain (executed both ways, with and without GOEXPERIMENT=jsonv2),
// so those maps remain marshal-opaque image-owners.

type tvIntTextKey int

func (k tvIntTextKey) MarshalText() ([]byte, error) {
	return []byte("i" + string(rune('0'+int(k)%10))), nil
}

type tvTextKeyPtr string

func (k *tvTextKeyPtr) MarshalText() ([]byte, error) { return []byte(string(*k) + "P"), nil }

// TestRegression_StringKindTextKeyMapComposesAsPlainMap pins the
// name-identity consequence: a def carried in a string-kind+MarshalText
// keyed map composes exactly like the plain-map twin, null-namespace pin
// included (name identity observed through Canonical(); String() renders
// namespaces relative).
func TestRegression_StringKindTextKeyMapComposesAsPlainMap(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	fields := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f"`}}
	xDef := func() map[string]any {
		return map[string]any{"type": "record", "name": "X",
			"fields": []any{map[string]any{"name": "c", "type": "long"}}}
	}
	build := func(items any) (string, error) {
		node := &SchemaNode{Type: "array", Props: map[string]any{"items": items}}
		s, err := schemaForScopeCell(t, fields, "com.x", []CustomType{{GoType: primary, Schema: node}})
		if err != nil {
			return "", err
		}
		return string(s.Canonical()), nil
	}
	control, err := build(xDef())
	if err != nil {
		t.Fatalf("plain map: %v", err)
	}
	if !strings.Contains(control, `"name":"X"`) {
		t.Fatalf("anchored control lost the null-namespace pin: %s", control)
	}
	tmDef := map[tvTextStr]any{}
	for k, v := range xDef() {
		tmDef[tvTextStr(k)] = v
	}
	got, err := build(tmDef)
	if err != nil {
		t.Fatalf("string-kind text-key map: %v", err)
	}
	if got != control {
		t.Errorf("image-identical maps compose differently:\n plain: %s\n text-key: %s", control, got)
	}
}

// TestRegression_StringKindTextKeyMapBytesFixup pins the content
// consequence on the plain rebuild surface: a []byte inside a string-kind
// text-keyed map gets the codepoint fixup exactly like the plain-map twin
// (base64 leaking through would silently change the re-read content).
func TestRegression_StringKindTextKeyMapBytesFixup(t *testing.T) {
	control, err := treeValuePropsObserved(t, "rebuild", map[string]any{"b": []byte{1, 2, 3}})
	if err != nil {
		t.Fatalf("plain map: %v", err)
	}
	want := map[string]any{"b": "\x01\x02\x03"}
	if !reflect.DeepEqual(control, want) {
		t.Fatalf("anchored control: plain map rebuilds as %#v, want %#v", control, want)
	}
	got, err := treeValuePropsObserved(t, "rebuild", map[tvTextStr]any{"b": []byte{1, 2, 3}})
	if err != nil {
		t.Fatalf("text-key map: %v", err)
	}
	if !reflect.DeepEqual(got, control) {
		t.Errorf("text-key map rebuilds as %#v, plain twin as %#v", got, control)
	}
}

// TestMatrix_TreeValueMapKeyShapes: key shape × consequence surface. The
// string-kind shapes (plain, MarshalText value receiver, MarshalText
// pointer receiver) are image-identical — asserted as an executed premise
// per cell family — and must compose identically: null-namespace pin
// applied, defs deduplicated, byte values codepoint-fixed, Props parity on
// rebuild. The int-kind MarshalText shape is the documented opaque
// image-owner: its marshal (method output keys, base64 bytes) is the
// contract, defs inside are invisible to the walkers, and a duplicated
// invisible def stays a loud Parse-side reject.
func TestMatrix_TreeValueMapKeyShapes(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	oneField := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f"`}}
	twoFields := []reflect.StructField{
		{Name: "F", Type: primary, Tag: `avro:"f"`},
		{Name: "G", Type: primary, Tag: `avro:"g"`},
	}
	xDef := func() map[string]any {
		return map[string]any{"type": "record", "name": "X",
			"fields": []any{map[string]any{"name": "c", "type": "long"}}}
	}
	asTextKey := func(m map[string]any) map[tvTextStr]any {
		out := map[tvTextStr]any{}
		for k, v := range m {
			out[tvTextStr(k)] = v
		}
		return out
	}
	asPtrTextKey := func(m map[string]any) map[tvTextKeyPtr]any {
		out := map[tvTextKeyPtr]any{}
		for k, v := range m {
			out[tvTextKeyPtr(k)] = v
		}
		return out
	}

	t.Run("premise_string_kind_keys_marshal_raw", func(t *testing.T) {
		plain := map[string]any{"a": 1}
		for _, v := range []any{map[tvTextStr]any{"a": 1}, map[tvTextKeyPtr]any{"a": 1}} {
			pb, perr := json.Marshal(plain)
			vb, verr := json.Marshal(v)
			if perr != nil || verr != nil || string(pb) != string(vb) {
				t.Fatalf("string-kind key premise: %T marshals %s (%v), plain %s (%v)", v, vb, verr, pb, perr)
			}
		}
	})
	t.Run("premise_int_kind_key_uses_marshal_text", func(t *testing.T) {
		b, err := json.Marshal(map[tvIntTextKey]any{7: 1})
		if err != nil || string(b) != `{"i7":1}` {
			t.Fatalf("int-kind key premise: got %s (%v), want the MarshalText key i7", b, err)
		}
	})

	shapes := []struct {
		name   string
		items  func() any // the pin/dedup def carrier
		bytes  any        // the fixup carrier
		opaque bool
	}{
		{name: "string_text_key",
			items: func() any { return asTextKey(xDef()) },
			bytes: map[tvTextStr]any{"b": []byte{1, 2, 3}}},
		{name: "string_ptr_text_key",
			items: func() any { return asPtrTextKey(xDef()) },
			bytes: map[tvTextKeyPtr]any{"b": []byte{1, 2, 3}}},
		{name: "int_text_key_opaque",
			items:  func() any { return map[tvIntTextKey]any{} },
			bytes:  map[tvIntTextKey]any{2: []byte{1, 2, 3}},
			opaque: true},
	}

	t.Run("pin", func(t *testing.T) {
		build := func(fields []reflect.StructField, items any) (string, error) {
			node := &SchemaNode{Type: "array", Props: map[string]any{"items": items}}
			s, err := schemaForScopeCell(t, fields, "com.x", []CustomType{{GoType: primary, Schema: node}})
			if err != nil {
				return "", err
			}
			return string(s.Canonical()), nil
		}
		control, err := build(oneField, xDef())
		if err != nil {
			t.Fatalf("plain control: %v", err)
		}
		if !strings.Contains(control, `"name":"X"`) {
			t.Fatalf("anchored control lost the null-namespace pin: %s", control)
		}
		for _, sh := range shapes {
			if sh.opaque {
				continue // the opaque def carrier has no X inside; posture covered below
			}
			t.Run(sh.name, func(t *testing.T) {
				got, err := build(oneField, sh.items())
				if err != nil {
					t.Fatalf("build: %v", err)
				}
				if got != control {
					t.Errorf("pin diverges from the plain twin:\n plain: %s\n shape: %s", control, got)
				}
			})
		}
		t.Run("int_text_key_opaque_posture", func(t *testing.T) {
			// An int-keyed map at a structural position marshals verbatim
			// as its own keyed object — never a valid schema there — so
			// the build is a loud Parse-side reject, not a silent rebind.
			if _, err := build(oneField, map[tvIntTextKey]any{1: xDef()}); err == nil {
				t.Errorf("opaque map as the items schema: want the loud Parse reject, got success")
			}
		})
	})

	t.Run("dedup", func(t *testing.T) {
		build := func(items func() any) (string, error) {
			node := &SchemaNode{Type: "array", Props: map[string]any{"items": items()}}
			s, err := schemaForScopeCell(t, twoFields, "", []CustomType{{GoType: primary, Schema: node}})
			if err != nil {
				return "", err
			}
			return string(s.Canonical()), nil
		}
		control, err := build(func() any { return xDef() })
		if err != nil {
			t.Fatalf("plain control: %v", err)
		}
		if got := strings.Count(control, `"fields":[{"name":"c"`); got != 1 {
			t.Fatalf("anchored control: want exactly one X definition, got %d: %s", got, control)
		}
		for _, sh := range shapes {
			if sh.opaque {
				continue
			}
			t.Run(sh.name, func(t *testing.T) {
				got, err := build(sh.items)
				if err != nil {
					t.Fatalf("build: %v", err)
				}
				if got != control {
					t.Errorf("dedup diverges from the plain twin:\n plain: %s\n shape: %s", control, got)
				}
			})
		}
	})

	t.Run("fixup", func(t *testing.T) {
		control, err := treeValuePropsObserved(t, "rebuild", map[string]any{"b": []byte{1, 2, 3}})
		if err != nil {
			t.Fatalf("plain control: %v", err)
		}
		if !reflect.DeepEqual(control, map[string]any{"b": "\x01\x02\x03"}) {
			t.Fatalf("anchored control: got %#v, want the codepoint form", control)
		}
		for _, sh := range shapes {
			t.Run(sh.name, func(t *testing.T) {
				got, err := treeValuePropsObserved(t, "rebuild", sh.bytes)
				if err != nil {
					t.Fatalf("rebuild: %v", err)
				}
				if sh.opaque {
					want := map[string]any{"i2": "AQID"}
					if !reflect.DeepEqual(got, want) {
						t.Errorf("opaque posture: got %#v, want the map's own marshal %#v", got, want)
					}
					return
				}
				if !reflect.DeepEqual(got, control) {
					t.Errorf("fixup diverges: got %#v, plain twin %#v", got, control)
				}
			})
		}
	})

	t.Run("rebuild", func(t *testing.T) {
		control, err := treeValuePropsObserved(t, "rebuild", map[string]any{"k": "v"})
		if err != nil {
			t.Fatalf("plain control: %v", err)
		}
		for _, sh := range shapes {
			if sh.opaque {
				continue
			}
			t.Run(sh.name, func(t *testing.T) {
				var m any
				switch sh.name {
				case "string_text_key":
					m = map[tvTextStr]any{"k": "v"}
				default:
					m = map[tvTextKeyPtr]any{"k": "v"}
				}
				got, err := treeValuePropsObserved(t, "rebuild", m)
				if err != nil {
					t.Fatalf("rebuild: %v", err)
				}
				if !reflect.DeepEqual(got, control) {
					t.Errorf("rebuild diverges: got %#v, plain twin %#v", got, control)
				}
			})
		}
		t.Run("int_text_key_opaque_posture", func(t *testing.T) {
			got, err := treeValuePropsObserved(t, "rebuild", map[tvIntTextKey]any{7: "v"})
			if err != nil {
				t.Fatalf("rebuild: %v", err)
			}
			if !reflect.DeepEqual(got, map[string]any{"i7": "v"}) {
				t.Errorf("opaque posture: got %#v, want the MarshalText-keyed map", got)
			}
		})
	})
}

// ---------- matrix_alias_resolution_test.go ----------

// TestMatrix_AliasResolutionCensus crosses reader-alias spelling x writer
// namespace x named kind x match site x API over the resolution matchers
// (namesMatch for a direct writer/reader pair, kindsMatchTier for reader-union
// branch selection; Resolve routes through CheckCompatibility, both APIs
// asserted per cell). The executed fastavro arm lives in
// matrix_alias_differential_test.go; Java citations are given where the two
// references disagree.
//
//   - an alias always matches the writer's exact FULLNAME. Aliases are stored
//     fully qualified — a bare alias qualifies into the reader type's own
//     namespace, a dotted alias stays verbatim, and a single leading dot is the
//     null-namespace escape (Java's Name constructor rule, Schema.java ~1455,
//     the same rule qualifyAliases applies) — so the exact tier covers the
//     same-namespace bare cell and both dotted cells.
//   - an alias DECLARED without any dot additionally short-name-matches the
//     writer's unqualified name in ANY namespace. This is fastavro's raw-string
//     tier (match_schemas, executed in the differential); Java has no short tier
//     (applyAliases renames through a fullname-keyed map, Schema.java ~2093),
//     and the permissive reference wins for a safely-decodable value.
//   - an explicitly-qualified alias NEVER short-matches: the spec gives a type
//     named "a.b" with aliases "c" and "x.y" the fully qualified names "a.c" and
//     "x.y". Both references reject the foreign-namespace pair.
//   - the leading-dot spelling matches ONLY the null-namespace writer
//     (Java-aligned; fastavro keeps the alias verbatim and matches nothing — the
//     documented divergence, recorded in NOT_BUGS with the executed evidence).
func TestMatrix_AliasResolutionCensus(t *testing.T) {
	writerName := map[string]string{"samens": "n1.Old", "foreignns": "n2.Old", "nullns": "Old"}
	aliasSpelling := map[string]string{
		"bare":          "Old",
		"dottedown":     "n1.Old",
		"dottedforeign": "n2.Old",
		"leadingdot":    ".Old",
	}
	// accept[spelling][writerNS]
	accept := map[string]map[string]bool{
		"bare":          {"samens": true, "foreignns": true, "nullns": true},
		"dottedown":     {"samens": true, "foreignns": false, "nullns": false},
		"dottedforeign": {"samens": false, "foreignns": true, "nullns": false},
		"leadingdot":    {"samens": false, "foreignns": false, "nullns": true},
	}

	kindSchema := func(kind, name, aliases string) string {
		aliasAttr := ""
		if aliases != "" {
			aliasAttr = fmt.Sprintf(`,"aliases":[%q]`, aliases)
		}
		switch kind {
		case "record":
			return fmt.Sprintf(`{"type":"record","name":%q%s,"fields":[{"name":"a","type":"int"}]}`, name, aliasAttr)
		case "enum":
			return fmt.Sprintf(`{"type":"enum","name":%q%s,"symbols":["A","B"]}`, name, aliasAttr)
		case "fixed":
			return fmt.Sprintf(`{"type":"fixed","name":%q%s,"size":2}`, name, aliasAttr)
		}
		panic("unknown kind")
	}
	value := map[string]any{
		"record": map[string]any{"a": int32(7)},
		"enum":   "A",
		"fixed":  []byte{1, 2},
	}

	for spelling, alias := range aliasSpelling {
		for wns, wname := range writerName {
			for _, kind := range []string{"record", "enum", "fixed"} {
				for _, site := range []string{"top", "union"} {
					want := accept[spelling][wns]
					name := fmt.Sprintf("%s/%s/%s/%s", spelling, wns, kind, site)
					t.Run(name, func(t *testing.T) {
						writer := MustParse(kindSchema(kind, wname, ""))
						readerJSON := kindSchema(kind, "n1.New", alias)
						if site == "union" {
							// A boolean decoy branch: no promotion reaches
							// boolean from any named kind, so branch
							// selection is decided by the alias rules alone.
							readerJSON = `["boolean",` + readerJSON + `]`
						}
						reader := MustParse(readerJSON)

						compatErr := CheckCompatibility(writer, reader)
						resolved, resolveErr := Resolve(writer, reader)
						if (compatErr == nil) != (resolveErr == nil) {
							t.Fatalf("CheckCompatibility (%v) and Resolve (%v) disagree", compatErr, resolveErr)
						}
						if got := compatErr == nil; got != want {
							t.Fatalf("accept=%v, want %v (CheckCompatibility: %v)", got, want, compatErr)
						}
						if !want {
							return
						}
						// Accepted cells must actually read: encode with the
						// writer, decode through the resolved schema.
						wire := mustEncode(t, writer, value[kind])
						var got any
						if _, err := resolved.Decode(wire, &got); err != nil {
							t.Fatalf("resolved decode: %v", err)
						}
						if got == nil {
							t.Fatalf("resolved decode produced nil")
						}
					})
				}
			}
		}
	}

	// Dotted-alias dot rule: aliases follow the names' rule (leadingDotName, the
	// shared helper) — a single leading dot with a DOTLESS remainder is the
	// null-namespace escape, and any other dotted spelling is a fullname VERBATIM
	// (Java's Name ctor nulls only an EMPTY space, so ".a.b" keeps space ".a";
	// fastavro compares raw alias strings, so ".a.b" matches only a writer
	// literally named ".a.b"). None of these spellings is dotless, so none is
	// bare-declared and none ever short-matches. Writers ".a.b" and "" are
	// lax-only names (#62, #60).
	dotAliases := map[string]string{"escape": ".x", "multidot": ".a.b", "doubledot": "..x", "dotonly": "."}
	dotWriters := map[string]struct {
		name string
		lax  bool
	}{
		"nullx":     {"x", false},
		"ab":        {"a.b", false},
		"laxdotab":  {".a.b", true},
		"emptyname": {"", true},
	}
	dotAccept := map[string]map[string]bool{
		"escape":    {"nullx": true, "ab": false, "laxdotab": false, "emptyname": false},
		"multidot":  {"nullx": false, "ab": false, "laxdotab": true, "emptyname": false},
		"doubledot": {"nullx": false, "ab": false, "laxdotab": false, "emptyname": false},
		"dotonly":   {"nullx": false, "ab": false, "laxdotab": false, "emptyname": true},
	}
	lax := WithLaxNames(func(string) error { return nil })
	for spelling, alias := range dotAliases {
		for wKey, w := range dotWriters {
			for _, site := range []string{"top", "union"} {
				want := dotAccept[spelling][wKey]
				t.Run(fmt.Sprintf("dotrule/%s/%s/%s", spelling, wKey, site), func(t *testing.T) {
					writerJSON := fmt.Sprintf(`{"type":"record","name":%q,"fields":[{"name":"a","type":"int"}]}`, w.name)
					var writer *Schema
					var err error
					if w.lax {
						writer, err = Parse(writerJSON, lax)
					} else {
						writer, err = Parse(writerJSON)
					}
					if err != nil {
						t.Fatalf("writer: %v", err)
					}
					readerJSON := fmt.Sprintf(`{"type":"record","name":"n1.New","aliases":[%q],"fields":[{"name":"a","type":"int"}]}`, alias)
					if site == "union" {
						readerJSON = `["boolean",` + readerJSON + `]`
					}
					reader := MustParse(readerJSON)

					compatErr := CheckCompatibility(writer, reader)
					resolved, resolveErr := Resolve(writer, reader)
					if (compatErr == nil) != (resolveErr == nil) {
						t.Fatalf("CheckCompatibility (%v) and Resolve (%v) disagree", compatErr, resolveErr)
					}
					if got := compatErr == nil; got != want {
						t.Fatalf("accept=%v, want %v (CheckCompatibility: %v)", got, want, compatErr)
					}
					if !want {
						return
					}
					wire, err := writer.Encode(map[string]any{"a": int32(7)})
					if err != nil {
						t.Fatalf("encode: %v", err)
					}
					var got any
					if _, err := resolved.Decode(wire, &got); err != nil {
						t.Fatalf("resolved decode: %v", err)
					}
				})
			}
		}
	}

	// Scan-past discrimination for the verbatim arm: a multi-dot
	// leading-dot alias must yield matchNone at the union tier so
	// selection scans past it — a dot-stripping arm would store "a.b" and
	// exact-match the writer, silently winning over the branch that
	// legitimately matches on its unqualified name.
	t.Run("dotrule/scanpast", func(t *testing.T) {
		writer := MustParse(`{"type":"record","name":"a.b","fields":[{"name":"a","type":"int"}]}`)
		reader := MustParse(`["boolean",
			{"type":"record","name":"n1.New","aliases":[".a.b"],"fields":[{"name":"a","type":"int"}]},
			{"type":"record","name":"n3.b","fields":[{"name":"a","type":"int"},{"name":"b","type":"string","default":"x"}]}]`)
		resolved, err := Resolve(writer, reader)
		if err != nil {
			t.Fatalf("selection must scan past the verbatim-alias branch to n3.b: %v", err)
		}
		wire, err := writer.Encode(map[string]any{"a": int32(7)})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got map[string]any
		if _, err := resolved.Decode(wire, &got); err != nil {
			t.Fatalf("resolved decode: %v", err)
		}
		if got["b"] != "x" {
			t.Fatalf("selected branch lacks n3.b's defaulted field (got %v); the verbatim alias branch was wrongly preferred", got)
		}
	})

	// Scan-past discrimination for the union-branch matcher: with a single
	// named candidate, a spurious tier-match on a qualified alias is
	// dominated by the direct matcher's recheck of the selected branch, so
	// single-candidate cells cannot see kindsMatchTier's own alias rule.
	// With TWO candidates, the rule decides WHICH branch wins: the
	// qualified-alias branch must yield matchNone so selection scans past
	// it to the later branch that legitimately matches on its unqualified
	// name. The reader default on the correct branch's extra field makes
	// the selected branch visible in the decoded value.
	t.Run("scanpast/unionqualifiedalias", func(t *testing.T) {
		writer := MustParse(`{"type":"record","name":"n2.Old","fields":[{"name":"a","type":"int"}]}`)
		reader := MustParse(`["boolean",
			{"type":"record","name":"n1.New","aliases":["n1.Old"],"fields":[{"name":"a","type":"int"}]},
			{"type":"record","name":"n3.Old","fields":[{"name":"a","type":"int"},{"name":"b","type":"string","default":"x"}]}]`)
		resolved, err := Resolve(writer, reader)
		if err != nil {
			t.Fatalf("selection must scan past the qualified-alias branch to n3.Old: %v", err)
		}
		wire, err := writer.Encode(map[string]any{"a": int32(7)})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got map[string]any
		if _, err := resolved.Decode(wire, &got); err != nil {
			t.Fatalf("resolved decode: %v", err)
		}
		if got["b"] != "x" {
			t.Fatalf("selected branch lacks n3.Old's defaulted field (got %v); the qualified-alias branch was wrongly preferred", got)
		}
	})

	// The alias tiers must not disturb the spec's unqualified-NAME match:
	// reader and writer sharing a short name across namespaces match with
	// no aliases at all ("both schemas are records with the same
	// (unqualified) name" — same wording for enum/fixed). One control per
	// kind per site.
	for _, kind := range []string{"record", "enum", "fixed"} {
		for _, site := range []string{"top", "union"} {
			t.Run(fmt.Sprintf("unqualifiednamecontrol/%s/%s", kind, site), func(t *testing.T) {
				writer := MustParse(kindSchema(kind, "n2.Same", ""))
				readerJSON := kindSchema(kind, "n1.Same", "")
				if site == "union" {
					readerJSON = `["boolean",` + readerJSON + `]`
				}
				reader := MustParse(readerJSON)
				if err := CheckCompatibility(writer, reader); err != nil {
					t.Errorf("unqualified name match must hold with no aliases: %v", err)
				}
			})
		}
	}

	// Field aliases are namespace-free strings matched exactly; the type-
	// alias qualification rules must not leak into them. A dotted FIELD
	// alias matches only a writer field literally named with the dot — the
	// alias-repair scenario the spec explicitly allows ("this allows schema
	// evolution to correct illegal names in old schemata"; the old
	// illegal-name schema itself parses only under WithLaxNames).
	t.Run("fieldaliascontrol", func(t *testing.T) {
		lax := WithLaxNames(func(string) error { return nil })
		writer, err := Parse(`{"type":"record","name":"R","fields":[{"name":"weird.name","type":"int"}]}`, lax)
		if err != nil {
			t.Fatalf("lax writer: %v", err)
		}
		reader := MustParse(`{"type":"record","name":"R","fields":[{"name":"clean","type":"int","aliases":["weird.name"]}]}`)
		if err := CheckCompatibility(writer, reader); err != nil {
			t.Errorf("dotted field alias must match the literal writer field name: %v", err)
		}
		readerBare := MustParse(`{"type":"record","name":"R","fields":[{"name":"clean","type":"int","aliases":["name"]}]}`)
		if err := CheckCompatibility(writer, readerBare); err == nil {
			t.Errorf(`field alias "name" must not short-match writer field "weird.name"`)
		}
	})

}

// ---------- fingerprint_purity_test.go ----------

// A fingerprint is the digest of a schema's canonical form, so its value must
// depend on the schema and the algorithm alone. Nothing else a caller did with the
// hash beforehand may reach the answer: not a previous fingerprint taken with the
// same hash, and not bytes the caller wrote into it directly. The expectation is
// not read off Fingerprint — each cell's oracle is a FRESH hash of the same
// algorithm fed the schema's canonical form, the definition of the digest,
// computed without the code under test.
func TestFingerprintIsAFunctionOfTheSchemaAlone(t *testing.T) {
	algos := []struct {
		name string
		mk   func() hash.Hash
	}{
		{"rabin", func() hash.Hash { return NewRabin() }},
		{"sha256", sha256.New},
		{"sha512", sha512.New},
		{"md5", md5.New},
		{"crc32-ieee", func() hash.Hash { return crc32.NewIEEE() }},
		{"crc64-ecma", func() hash.Hash { return crc64.New(crc64.MakeTable(crc64.ECMA)) }},
	}

	schemas := []struct {
		name string
		text string
	}{
		{"short", `"int"`},
		// Longer than every block size above, so the accumulated state spans
		// more than one compression block and a partial reset would show.
		{"multi-block", `{"type":"record","name":"com.example.Wide","fields":[` +
			`{"name":"alpha","type":"string"},{"name":"bravo","type":"long"},` +
			`{"name":"charlie","type":{"type":"array","items":"double"}},` +
			`{"name":"delta","type":{"type":"map","values":"bytes"}},` +
			`{"name":"echo","type":["null","string"]}]}`},
		{"recursive", `{"type":"record","name":"N","fields":[` +
			`{"name":"v","type":"int"},{"name":"next","type":["null","N"]}]}`},
	}

	// Prior states a caller's hash can be in when it reaches Fingerprint.
	priors := []struct {
		name string
		put  func(t *testing.T, h hash.Hash, self, other *Schema)
	}{
		{"fresh", func(*testing.T, hash.Hash, *Schema, *Schema) {}},
		{"after fingerprinting the same schema", func(t *testing.T, h hash.Hash, self, _ *Schema) {
			self.Fingerprint(h)
		}},
		{"after fingerprinting a different schema", func(t *testing.T, h hash.Hash, _, other *Schema) {
			other.Fingerprint(h)
		}},
		{"caller wrote bytes into it", func(t *testing.T, h hash.Hash, _, _ *Schema) {
			h.Write([]byte("caller's own payload"))
		}},
		{"caller wrote, then reset", func(t *testing.T, h hash.Hash, _, _ *Schema) {
			h.Write([]byte("caller's own payload"))
			h.Reset()
		}},
		{"fingerprinted twice over", func(t *testing.T, h hash.Hash, self, other *Schema) {
			self.Fingerprint(h)
			other.Fingerprint(h)
		}},
	}

	// A schema distinct from every cell's own, for the cross-contamination
	// priors.
	other := MustParse(`{"type":"enum","name":"Contaminant","symbols":["X","Y","Z"]}`)

	for _, a := range algos {
		for _, sc := range schemas {
			s := MustParse(sc.text)

			// Oracle: a fresh hash of this algorithm over the canonical form.
			oracle := a.mk()
			oracle.Write(s.Canonical())
			want := oracle.Sum(nil)

			for _, p := range priors {
				t.Run(a.name+"/"+sc.name+"/"+p.name, func(t *testing.T) {
					h := a.mk()
					p.put(t, h, s, other)
					got := s.Fingerprint(h)
					if !bytes.Equal(got, want) {
						t.Fatalf("fingerprint depends on the hash's prior state: got %x, want %x", got, want)
					}
					// The digest must also still be readable from the hash
					// after the call: callers take Sum64 off the hash they
					// passed, so the accumulated state stays put. Clearing on
					// the way out would buy determinism and break that.
					if after := h.Sum(nil); !bytes.Equal(after, want) {
						t.Fatalf("hash state after the call: got %x, want %x", after, want)
					}
				})
			}
		}
	}
}

// TestFingerprintRepeatsOnOneHash is the property in its plainest form: the
// same hash handed to the same schema twice answers the same thing.
func TestFingerprintRepeatsOnOneHash(t *testing.T) {
	s := MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	h := NewRabin()
	first := bytes.Clone(s.Fingerprint(h))
	second := s.Fingerprint(h)
	if !bytes.Equal(first, second) {
		t.Fatalf("reused hash gave %x then %x", first, second)
	}
}

// hashTakingAPIs rows every exported entry point that accepts a caller-owned
// hash. Such a parameter is a mutable accumulator the caller may have used, so
// each one owes the same purity rule; a new one must be rowed and driven.
var hashTakingAPIs = []string{"Schema.Fingerprint"}

// TestHashTakingAPIsAreRowed derives the set from source rather than trusting
// the list: any exported function or method with a parameter from the hash
// package is one. Fails in both directions — an unrowed entry point, and a row
// naming one the source no longer has.
func TestHashTakingAPIsAreRowed(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading package dir: %v", err)
	}
	fset := token.NewFileSet()
	var derived []string
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, n, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", n, err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || !fd.Name.IsExported() || fd.Type.Params == nil {
				continue
			}
			takesHash := false
			for _, p := range fd.Type.Params.List {
				// hash.Hash, hash.Hash32, hash.Hash64 — any type from the
				// hash package, however the parameter is spelled.
				if sel, ok := p.Type.(*ast.SelectorExpr); ok {
					if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "hash" {
						takesHash = true
					}
				}
			}
			if !takesHash {
				continue
			}
			name := fd.Name.Name
			if fd.Recv != nil && len(fd.Recv.List) > 0 {
				name = recvTypeName(fd.Recv.List[0].Type) + "." + name
			}
			derived = append(derived, name)
		}
	}
	if len(derived) == 0 {
		t.Fatal("derivation found no hash-taking entry point; the walk is broken, not the package")
	}

	rowed := map[string]bool{}
	for _, r := range hashTakingAPIs {
		rowed[r] = true
	}
	seen := map[string]bool{}
	for _, d := range derived {
		seen[d] = true
		if !rowed[d] {
			t.Errorf("%s takes a caller-owned hash but has no row: its result must not depend on "+
				"what the caller did with that hash beforehand, and a test must assert it", d)
		}
	}
	for _, r := range hashTakingAPIs {
		if !seen[r] {
			t.Errorf("row names %s, which the source no longer declares as a hash-taking entry point", r)
		}
	}
}

// recvTypeName renders a method receiver's type name, pointer or not.
func recvTypeName(e ast.Expr) string {
	if star, ok := e.(*ast.StarExpr); ok {
		e = star.X
	}
	if id, ok := e.(*ast.Ident); ok {
		return id.Name
	}
	return "?"
}

// ---------- soe_test.go ----------

func TestSingleObjectRoundTrip(t *testing.T) {
	t.Run("null", func(t *testing.T) {
		s := mustParse(t, `"null"`)
		encoded := mustAppendSingleObject(t, s, nil, (*int)(nil))
		if encoded[0] != 0xC3 || encoded[1] != 0x01 {
			t.Fatalf("bad magic: [%#x, %#x]", encoded[0], encoded[1])
		}
		var got *int
		rest := mustDecodeSingleObject(t, s, encoded, &got)
		if len(rest) != 0 {
			t.Fatalf("unexpected remaining bytes: %d", len(rest))
		}
	})

	tests := []struct {
		name   string
		schema string
		val    any
	}{
		{"boolean", `"boolean"`, new(bool)},
		{"int", `"int"`, new(int32)},
		{"long", `"long"`, new(int64)},
		{"string", `"string"`, new(string)},
		{
			"record", `{"type":"record","name":"r","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`,
			&map[string]any{"a": int32(7), "b": "world"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustParse(t, tt.schema)

			encoded := mustAppendSingleObject(t, s, nil, tt.val)

			if len(encoded) < 10 {
				t.Fatalf("encoded too short: %d", len(encoded))
			}
			if encoded[0] != 0xC3 || encoded[1] != 0x01 {
				t.Fatalf("bad magic: [%#x, %#x]", encoded[0], encoded[1])
			}

			var got any
			rest := mustDecodeSingleObject(t, s, encoded, &got)
			if len(rest) != 0 {
				t.Fatalf("unexpected remaining bytes: %d", len(rest))
			}
		})
	}
}

func TestSingleObjectFingerprint(t *testing.T) {
	s, err := Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := s.AppendSingleObject(nil, new(int32))
	if err != nil {
		t.Fatal(err)
	}

	fp, rest, err := SingleObjectFingerprint(encoded)
	if err != nil {
		t.Fatalf("SingleObjectFingerprint: %v", err)
	}

	// Verify fingerprint matches the schema's own one.
	var want [8]byte
	copy(want[:], s.soeHeader()[2:10])
	if fp != want {
		t.Fatalf("fingerprint mismatch: got %x, want %x", fp, want)
	}

	// Verify rest is the payload (binary encoded int 0).
	if len(rest) == 0 {
		t.Fatal("expected non-empty rest")
	}
}

func TestSingleObjectFingerprintMismatch(t *testing.T) {
	a := mustParse(t, `"int"`)
	b := mustParse(t, `"string"`)

	encoded := mustAppendSingleObject(t, a, nil, new(int32))

	var got string
	_, err := b.DecodeSingleObject(encoded, &got)
	if err == nil {
		t.Fatal("expected fingerprint mismatch error")
	}
}

func TestSingleObjectBadMagic(t *testing.T) {
	s := mustParse(t, `"int"`)

	encoded := mustAppendSingleObject(t, s, nil, new(int32))

	// Corrupt magic bytes.
	encoded[0] = 0x00
	encoded[1] = 0x00

	var got int32
	_, err := s.DecodeSingleObject(encoded, &got)
	if err == nil {
		t.Fatal("expected bad magic error")
	}

	// SingleObjectFingerprint should also fail.
	_, _, err = SingleObjectFingerprint(encoded)
	if err == nil {
		t.Fatal("expected bad magic error from SingleObjectFingerprint")
	}
}

func TestSingleObjectShortBuffer(t *testing.T) {
	s := mustParse(t, `"int"`)

	for _, n := range []int{0, 1, 5, 9} {
		data := make([]byte, n)
		var got int32
		_, err := s.DecodeSingleObject(data, &got)
		if err == nil {
			t.Fatalf("expected short buffer error for %d bytes", n)
		}

		_, _, err = SingleObjectFingerprint(data)
		if err == nil {
			t.Fatalf("expected short buffer error from SingleObjectFingerprint for %d bytes", n)
		}
	}
}

func TestSingleObjectFingerprintMatchesSpec(t *testing.T) {
	// Verify the fingerprint bytes are little-endian CRC-64-AVRO.
	s := mustParse(t, `"int"`)

	h := NewRabin()
	h.Write(s.Canonical())
	sum := h.Sum64()

	var want [8]byte
	binary.LittleEndian.PutUint64(want[:], sum)

	var got [8]byte
	copy(got[:], s.soeHeader()[2:10])

	if got != want {
		t.Fatalf("SOE fingerprint does not match LE CRC-64-AVRO: got %x, want %x", got, want)
	}
}

// TestSOEHeaderIsHashedOnFirstUseOnly is the non-vacuity half of the
// single-object matrix: that matrix proves the lazily hashed header is
// CORRECT, and would still pass if parse() went back to hashing eagerly.
// This one proves the hash is actually deferred — every surface that is not a
// single-object entry point must leave the header unhashed, including the two
// failure paths of DecodeSingleObject, which reject before they can need it.
func TestSOEHeaderIsHashedOnFirstUseOnly(t *testing.T) {
	unhashed := func(t *testing.T, s *Schema, after string) {
		t.Helper()
		if s.soe != ([10]byte{}) {
			t.Fatalf("%s hashed the single-object header (%x); it must wait for an SOE call", after, s.soe)
		}
	}
	schema := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`
	val := map[string]any{"a": int32(1), "b": "s"}

	s := mustParse(t, schema)
	unhashed(t, s, "Parse")

	// Every non-SOE surface of a Schema.
	_ = s.Canonical()
	_ = s.Fingerprint(NewRabin())
	_ = s.String()
	_ = s.Root()
	wire := mustAppendEncode(t, s, nil, val)
	var out map[string]any
	mustDecode(t, s, wire, &out)
	mustAppendEncodeJSON(t, s, nil, val)
	mustDecodeJSON(t, s, []byte(`{"a":1,"b":"s"}`), &out)
	unhashed(t, s, "the non-SOE API")

	// DecodeSingleObject rejects a short buffer and a bad magic before it
	// has any use for the header, so neither may hash it: that ordering is
	// what keeps the errors identical to the eager version.
	for _, bad := range [][]byte{nil, make([]byte, 9), append([]byte{0x00, 0x01}, make([]byte, 12)...)} {
		if _, err := s.DecodeSingleObject(bad, &out); err == nil {
			t.Fatalf("DecodeSingleObject(%x) accepted", bad)
		}
	}
	unhashed(t, s, "a rejected SOE header")

	// A fingerprint MISMATCH does need the header, so this one must hash.
	alien := mustParse(t, `"int"`)
	alienWire := mustAppendSingleObject(t, alien, nil, int32(1))
	if _, err := s.DecodeSingleObject(alienWire, &out); err == nil {
		t.Fatal("DecodeSingleObject accepted an alien fingerprint")
	}
	if s.soe == ([10]byte{}) {
		t.Fatal("a fingerprint comparison did not hash the header")
	}

	// Resolve copies neither header: the reader's is re-derived from the
	// canonical form it hands over, and the writer's is reached through the
	// writer schema itself.
	w := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"},{"name":"c","type":"long"}]}`)
	r := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	res := mustResolve(t, w, r)
	unhashed(t, w, "Resolve (writer)")
	unhashed(t, r, "Resolve (reader)")
	unhashed(t, res, "Resolve (resolved)")
	if res.soeWriter != w {
		t.Fatalf("resolved schema does not point at its writer")
	}

	// SchemaCache's splice replaces the canonical form after parse; the
	// header must follow it without being copied at the splice.
	var c SchemaCache
	if _, err := c.Parse(`{"type":"record","name":"Leaf","namespace":"n","fields":[{"name":"l","type":"int"}]}`); err != nil {
		t.Fatalf("cache parse leaf: %v", err)
	}
	parent, err := c.Parse(`{"type":"record","name":"Parent","namespace":"n","fields":[{"name":"p","type":"n.Leaf"}]}`)
	if err != nil {
		t.Fatalf("cache parse parent: %v", err)
	}
	unhashed(t, parent, "SchemaCache.Parse")
	if got, want := *parent.soeHeader(), *mustParse(t, parent.String()).soeHeader(); got != want {
		t.Fatalf("spliced header %x, want standalone %x", got, want)
	}
}

// soeFieldReaders names the only functions allowed to touch the raw soe
// field. Every other read must go through soeHeader, which is what makes the
// first use race-free on a type documented safe for concurrent use.
var soeFieldReaders = map[string]bool{"Schema.soeHeader": true, "Schema.hashSOEHeader": true}

// TestSOEFieldIsReadOnlyThroughAccessor derives the set of functions that
// mention the soe field from source, so a new bare read cannot be added
// without either failing here or being rowed above deliberately. Fails in
// both directions.
func TestSOEFieldIsReadOnlyThroughAccessor(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading package dir: %v", err)
	}
	fset := token.NewFileSet()
	found := map[string]bool{}
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, n, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", n, err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			name := fd.Name.Name
			if fd.Recv != nil && len(fd.Recv.List) > 0 {
				name = recvTypeName(fd.Recv.List[0].Type) + "." + name
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "soe" {
					found[name] = true
				}
				return true
			})
		}
	}
	if len(found) == 0 {
		t.Fatal("derivation found no soe field access; the walk is broken, not the package")
	}
	for name := range found {
		if !soeFieldReaders[name] {
			t.Errorf("%s reads the soe field directly; go through soeHeader or row it in soeFieldReaders", name)
		}
	}
	for name := range soeFieldReaders {
		if !found[name] {
			t.Errorf("soeFieldReaders names %s, which no longer touches the soe field", name)
		}
	}
}

// TestRegression_ResolvedDecodeSingleObjectAcceptsWriterFingerprint pins
// that a schema returned by Resolve(writer, reader) accepts SOE wire bytes
// bearing the WRITER schema's fingerprint. The SOE wire format puts the
// schema-that-produced-the-bytes' fingerprint on the wire (Avro spec),
// which is the writer; the resolved schema is the right thing to decode
// those bytes into a reader-shaped Go value. Java's BinaryMessageDecoder
// dispatches the wire fingerprint via a writer-fingerprint→codec registry;
// twmb stores the writer fingerprint on the resolved Schema so its
// DecodeSingleObject accepts both writer and reader fingerprints.
func TestRegression_ResolvedDecodeSingleObjectAcceptsWriterFingerprint(t *testing.T) {
	writer, err := Parse(recABSchema)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(recASchema)
	if err != nil {
		t.Fatal(err)
	}

	// Writer produces SOE wire bearing the writer's own header.
	wire, err := writer.AppendSingleObject(nil, map[string]any{
		"a": int32(7),
		"b": "hello",
	})
	if err != nil {
		t.Fatalf("writer.AppendSingleObject: %v", err)
	}
	if [10]byte(wire[:10]) != *writer.soeHeader() {
		t.Fatalf("wire header is not the writer's SOE header")
	}

	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	// Resolved schema must decode writer-fingerprinted wire (primary case).
	var got map[string]any
	rest, err := resolved.DecodeSingleObject(wire, &got)
	if err != nil {
		t.Fatalf("resolved.DecodeSingleObject(writer wire): %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("unexpected remaining bytes: %d", len(rest))
	}
	if got["a"] != int32(7) {
		t.Fatalf("a: got %v, want int32(7)", got["a"])
	}
	if _, present := got["b"]; present {
		t.Fatalf("b: expected projected out by reader, got %v", got["b"])
	}

	// A completely unrelated schema's fingerprint is still rejected.
	other := MustParse(`{"type":"record","name":"Other","fields":[{"name":"x","type":"int"}]}`)
	otherWire, err := other.AppendSingleObject(nil, map[string]any{"x": int32(1)})
	if err != nil {
		t.Fatalf("other.AppendSingleObject: %v", err)
	}
	if _, err := resolved.DecodeSingleObject(otherWire, &got); err == nil {
		t.Fatalf("resolved.DecodeSingleObject(unrelated wire) accepted; want fingerprint mismatch")
	}
}

// TestRegression_NonResolvedDecodeSingleObjectRejectsForeignFingerprint
// pins that a non-resolved schema continues to reject SOE wire whose
// fingerprint doesn't match its own — the nil soeWriter must never
// silently accept arbitrary input.
func TestRegression_NonResolvedDecodeSingleObjectRejectsForeignFingerprint(t *testing.T) {
	a := MustParse(`{"type":"record","name":"A","fields":[{"name":"f","type":"int"}]}`)
	b := MustParse(`{"type":"record","name":"B","fields":[{"name":"f","type":"int"}]}`)
	wire, err := a.AppendSingleObject(nil, map[string]any{"f": int32(1)})
	if err != nil {
		t.Fatalf("a.AppendSingleObject: %v", err)
	}
	var got map[string]any
	if _, err := b.DecodeSingleObject(wire, &got); err == nil {
		t.Fatalf("b.DecodeSingleObject(a-wire) accepted; want fingerprint mismatch")
	}
}

// ---------- cache_test.go ----------

func TestSchemaCacheBasic(t *testing.T) {
	cache := &SchemaCache{}

	// Parse a leaf schema.
	_, err := cache.Parse(telephoneSchema)
	if err != nil {
		t.Fatalf("parse Telephone: %v", err)
	}

	// Parse a parent that references the leaf.
	parent, err := cache.Parse(`{
		"type": "record",
		"name": "Person",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "phone", "type": "Telephone"}
		]
	}`)
	if err != nil {
		t.Fatalf("parse Person: %v", err)
	}

	// Encode and decode using the parent schema.
	input := map[string]any{
		"name": "alice",
		"phone": map[string]any{
			"number": float64(1234),
			"label":  "home",
		},
	}
	binary, err := parent.Encode(input)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded any
	rest, err := parent.Decode(binary, &decoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("unexpected remaining bytes: %v", rest)
	}
	m := decoded.(map[string]any)
	if m["name"] != "alice" {
		t.Errorf("name: got %v", m["name"])
	}
	phone := m["phone"].(map[string]any)
	if phone["number"] != int32(1234) {
		t.Errorf("phone.number: got %v (%T)", phone["number"], phone["number"])
	}
	if phone["label"] != "home" {
		t.Errorf("phone.label: got %v", phone["label"])
	}
}

func TestSchemaCacheMultipleRefs(t *testing.T) {
	cache := &SchemaCache{}

	_, err := cache.Parse(telephoneSchema)
	if err != nil {
		t.Fatal(err)
	}

	_, err = cache.Parse(`{
		"type": "record",
		"name": "Address",
		"fields": [
			{"name": "street", "type": "string"},
			{"name": "city", "type": "string"}
		]
	}`)
	if err != nil {
		t.Fatal(err)
	}

	// Parent references both.
	parent, err := cache.Parse(`{
		"type": "record",
		"name": "Contact",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "phone", "type": "Telephone"},
			{"name": "address", "type": "Address"}
		]
	}`)
	if err != nil {
		t.Fatalf("parse Contact: %v", err)
	}

	input := map[string]any{
		"name": "bob",
		"phone": map[string]any{
			"number": float64(5678),
			"label":  "work",
		},
		"address": map[string]any{
			"street": "123 Main St",
			"city":   "Springfield",
		},
	}
	binary, err := parent.Encode(input)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded any
	mustDecode(t, parent, binary, &decoded)
	m := decoded.(map[string]any)
	addr := m["address"].(map[string]any)
	if addr["city"] != "Springfield" {
		t.Errorf("address.city: got %v", addr["city"])
	}
}

func TestSchemaCacheNestedRefs(t *testing.T) {
	cache := &SchemaCache{}

	// Leaf: Owner.
	_, err := cache.Parse(`{
		"type": "record",
		"name": "Owner",
		"fields": [{"name": "lastname", "type": "string"}]
	}`)
	if err != nil {
		t.Fatal(err)
	}

	// Mid-level: TelephoneOwner references Owner.
	_, err = cache.Parse(`{
		"type": "record",
		"name": "TelephoneOwner",
		"fields": [
			{"name": "number", "type": "int"},
			{"name": "owner", "type": "Owner"}
		]
	}`)
	if err != nil {
		t.Fatal(err)
	}

	// Top-level: references TelephoneOwner.
	top, err := cache.Parse(`{
		"type": "record",
		"name": "Contact",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "phone", "type": "TelephoneOwner"}
		]
	}`)
	if err != nil {
		t.Fatalf("parse Contact: %v", err)
	}

	input := map[string]any{
		"name": "carol",
		"phone": map[string]any{
			"number": float64(9999),
			"owner": map[string]any{
				"lastname": "Smith",
			},
		},
	}
	binary, err := top.Encode(input)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded any
	mustDecode(t, top, binary, &decoded)
	m := decoded.(map[string]any)
	owner := m["phone"].(map[string]any)["owner"].(map[string]any)
	if owner["lastname"] != "Smith" {
		t.Errorf("owner.lastname: got %v", owner["lastname"])
	}
}

func TestSchemaCacheSharedBase(t *testing.T) {
	// Multiple schemas sharing a common base type.
	cache := &SchemaCache{}

	_, err := cache.Parse(`{
		"type": "record",
		"name": "Base",
		"fields": [{"name": "id", "type": "int"}]
	}`)
	if err != nil {
		t.Fatal(err)
	}

	s1, err := cache.Parse(`{
		"type": "record",
		"name": "TypeA",
		"fields": [
			{"name": "base", "type": "Base"},
			{"name": "a", "type": "string"}
		]
	}`)
	if err != nil {
		t.Fatal(err)
	}

	s2, err := cache.Parse(`{
		"type": "record",
		"name": "TypeB",
		"fields": [
			{"name": "base", "type": "Base"},
			{"name": "b", "type": "long"}
		]
	}`)
	if err != nil {
		t.Fatal(err)
	}

	// Both schemas should work independently.
	b1, err := s1.Encode(map[string]any{
		"base": map[string]any{"id": float64(1)},
		"a":    "hello",
	})
	if err != nil {
		t.Fatalf("encode TypeA: %v", err)
	}
	var d1 any
	if _, err := s1.Decode(b1, &d1); err != nil {
		t.Fatalf("decode TypeA: %v", err)
	}

	b2, err := s2.Encode(map[string]any{
		"base": map[string]any{"id": float64(2)},
		"b":    float64(42),
	})
	if err != nil {
		t.Fatalf("encode TypeB: %v", err)
	}
	var d2 any
	if _, err := s2.Decode(b2, &d2); err != nil {
		t.Fatalf("decode TypeB: %v", err)
	}
}

func TestSchemaCacheUnresolvedRef(t *testing.T) {
	cache := &SchemaCache{}

	// Parsing a schema that references an unknown type should fail.
	_, err := cache.Parse(`{
		"type": "record",
		"name": "Bad",
		"fields": [{"name": "x", "type": "Unknown"}]
	}`)
	if err == nil {
		t.Fatal("expected error for unresolved reference")
	}

	// The cache should not be corrupted by the failed parse.
	// A subsequent valid parse should still work.
	_, err = cache.Parse(`{
		"type": "record",
		"name": "Good",
		"fields": [{"name": "x", "type": "int"}]
	}`)
	if err != nil {
		t.Fatalf("expected success after failed parse, got: %v", err)
	}
}

func TestSchemaCacheJSONRoundtrip(t *testing.T) {
	// End-to-end test matching rpk's usage pattern:
	// parse refs → parse parent → json.Unmarshal → Encode → Decode → json.Marshal
	cache := &SchemaCache{}

	_, err := cache.Parse(`{
		"type": "record",
		"name": "telephone",
		"fields": [
			{"name": "number", "type": "int"},
			{"name": "identifier", "type": "string"}
		]
	}`)
	if err != nil {
		t.Fatal(err)
	}

	parent, err := cache.Parse(`{
		"type": "record",
		"name": "test",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "telephone", "type": "telephone"}
		]
	}`)
	if err != nil {
		t.Fatal(err)
	}

	input := `{"name":"redpanda","telephone":{"number":12341234,"identifier":"home"}}`

	var native any
	mustUnmarshal(t, []byte(input), &native)

	binary, err := parent.Encode(native)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	var decoded any
	rest, err := parent.Decode(binary, &decoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("remaining bytes: %v", rest)
	}

	// Marshal back to JSON and compare.
	out, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	// Compare by unmarshalling both to maps to avoid key-order issues.
	var got, want map[string]any
	json.Unmarshal(out, &got)
	json.Unmarshal([]byte(input), &want)

	if got["name"] != want["name"] {
		t.Errorf("name mismatch: got %v, want %v", got["name"], want["name"])
	}
	gotPhone := got["telephone"].(map[string]any)
	wantPhone := want["telephone"].(map[string]any)
	if gotPhone["identifier"] != wantPhone["identifier"] {
		t.Errorf("identifier mismatch: got %v, want %v", gotPhone["identifier"], wantPhone["identifier"])
	}
}

func TestSchemaCacheEnum(t *testing.T) {
	cache := &SchemaCache{}

	mustCacheParse(t, cache, `{
		"type": "enum",
		"name": "Color",
		"symbols": ["RED", "GREEN", "BLUE"]
	}`)

	s := mustCacheParse(t, cache, itemColorRefSchema)

	input := map[string]any{"name": "shirt", "color": "GREEN"}
	binary := mustEncode(t, s, input)
	var decoded any
	mustDecode(t, s, binary, &decoded)
	m := decoded.(map[string]any)
	if m["color"] != "GREEN" {
		t.Errorf("color: got %v", m["color"])
	}
}

func TestSchemaCacheDiamondDependency(t *testing.T) {
	// Diamond: A references B and C, both B and C reference D.
	// Parsing D twice (once for B's deps, once for C's deps) must succeed.
	cache := &SchemaCache{}

	schemaD := `{
		"type": "record",
		"name": "D",
		"fields": [{"name": "id", "type": "int"}]
	}`

	s1, err := cache.Parse(schemaD)
	if err != nil {
		t.Fatalf("first parse of D: %v", err)
	}

	s2, err := cache.Parse(schemaD)
	if err != nil {
		t.Fatalf("second parse of D: %v", err)
	}

	if s1 != s2 {
		t.Error("expected same *Schema pointer for duplicate parse")
	}

	// B references D.
	_, err = cache.Parse(`{
		"type": "record",
		"name": "B",
		"fields": [
			{"name": "d", "type": "D"},
			{"name": "b", "type": "string"}
		]
	}`)
	if err != nil {
		t.Fatalf("parse B: %v", err)
	}

	// C references D.
	_, err = cache.Parse(`{
		"type": "record",
		"name": "C",
		"fields": [
			{"name": "d", "type": "D"},
			{"name": "c", "type": "long"}
		]
	}`)
	if err != nil {
		t.Fatalf("parse C: %v", err)
	}

	// A references B and C.
	schemaA, err := cache.Parse(`{
		"type": "record",
		"name": "A",
		"fields": [
			{"name": "b", "type": "B"},
			{"name": "c", "type": "C"}
		]
	}`)
	if err != nil {
		t.Fatalf("parse A: %v", err)
	}

	// Verify the full graph works end-to-end.
	input := map[string]any{
		"b": map[string]any{
			"d": map[string]any{"id": float64(1)},
			"b": "hello",
		},
		"c": map[string]any{
			"d": map[string]any{"id": float64(2)},
			"c": float64(42),
		},
	}
	binary, err := schemaA.Encode(input)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded any
	mustDecode(t, schemaA, binary, &decoded)
	m := decoded.(map[string]any)
	bd := m["b"].(map[string]any)["d"].(map[string]any)
	if bd["id"] != int32(1) {
		t.Errorf("b.d.id: got %v", bd["id"])
	}
	cd := m["c"].(map[string]any)["d"].(map[string]any)
	if cd["id"] != int32(2) {
		t.Errorf("c.d.id: got %v", cd["id"])
	}
}

func TestSchemaCacheDiamondEnum(t *testing.T) {
	cache := &SchemaCache{}

	schemaColor := `{
		"type": "enum",
		"name": "Color",
		"symbols": ["RED", "GREEN", "BLUE"]
	}`

	s1, err := cache.Parse(schemaColor)
	if err != nil {
		t.Fatalf("first parse: %v", err)
	}
	s2, err := cache.Parse(schemaColor)
	if err != nil {
		t.Fatalf("second parse: %v", err)
	}
	if s1 != s2 {
		t.Error("expected same *Schema pointer")
	}

	s, err := cache.Parse(itemColorRefSchema)
	if err != nil {
		t.Fatalf("parse Item: %v", err)
	}

	binary, err := s.Encode(map[string]any{"name": "hat", "color": "RED"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded any
	mustDecode(t, s, binary, &decoded)
	if decoded.(map[string]any)["color"] != "RED" {
		t.Errorf("color: got %v", decoded.(map[string]any)["color"])
	}
}

func TestSchemaCacheDiamondFixed(t *testing.T) {
	cache := &SchemaCache{}

	schemaHash := `{
		"type": "fixed",
		"name": "Hash",
		"size": 16
	}`

	s1, err := cache.Parse(schemaHash)
	if err != nil {
		t.Fatalf("first parse: %v", err)
	}
	s2, err := cache.Parse(schemaHash)
	if err != nil {
		t.Fatalf("second parse: %v", err)
	}
	if s1 != s2 {
		t.Error("expected same *Schema pointer")
	}

	s, err := cache.Parse(`{
		"type": "record",
		"name": "Doc",
		"fields": [
			{"name": "id", "type": "string"},
			{"name": "hash", "type": "Hash"}
		]
	}`)
	if err != nil {
		t.Fatalf("parse Doc: %v", err)
	}

	hash := make([]byte, 16)
	hash[0] = 0xAB
	binary, err := s.Encode(map[string]any{"id": "doc1", "hash": hash})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded any
	mustDecode(t, s, binary, &decoded)
	got := decoded.(map[string]any)["hash"].([]byte)
	if got[0] != 0xAB {
		t.Errorf("hash[0]: got %x", got[0])
	}
}

func TestSchemaCacheFailedParseThenRetry(t *testing.T) {
	// A failed parse must not be cached by the dedup map.
	cache := &SchemaCache{}

	schema := `{
		"type": "record",
		"name": "R",
		"fields": [{"name": "f", "type": "Unknown"}]
	}`

	_, err := cache.Parse(schema)
	if err == nil {
		t.Fatal("expected error for unresolved reference")
	}

	// Add the missing type, then retry the same schema string.
	_, err = cache.Parse(`{
		"type": "record",
		"name": "Unknown",
		"fields": [{"name": "x", "type": "int"}]
	}`)
	if err != nil {
		t.Fatalf("parse Unknown: %v", err)
	}

	s, err := cache.Parse(schema)
	if err != nil {
		t.Fatalf("retry should succeed after adding Unknown: %v", err)
	}

	binary, err := s.Encode(map[string]any{
		"f": map[string]any{"x": float64(7)},
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded any
	mustDecode(t, s, binary, &decoded)
	if decoded.(map[string]any)["f"].(map[string]any)["x"] != int32(7) {
		t.Errorf("f.x: got %v", decoded.(map[string]any)["f"])
	}
}

func TestSchemaCacheDiamondWhitespace(t *testing.T) {
	// Same schema with different whitespace should dedup.
	cache := &SchemaCache{}

	_, err := cache.Parse(`{"type":"record","name":"W","fields":[{"name":"x","type":"int"}]}`)
	if err != nil {
		t.Fatalf("first parse: %v", err)
	}

	s, err := cache.Parse(`{
		"type": "record",
		"name": "W",
		"fields": [
			{"name": "x", "type": "int"}
		]
	}`)
	if err != nil {
		t.Fatalf("second parse with different whitespace: %v", err)
	}

	binary, err := s.Encode(map[string]any{"x": float64(42)})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded any
	mustDecode(t, s, binary, &decoded)
	if decoded.(map[string]any)["x"] != int32(42) {
		t.Errorf("x: got %v", decoded.(map[string]any)["x"])
	}
}

func TestSchemaCacheDiamondKeyOrder(t *testing.T) {
	// Same schema with different JSON key ordering should dedup.
	cache := &SchemaCache{}

	_, err := cache.Parse(`{"type":"record","name":"K","fields":[{"name":"x","type":"int"}]}`)
	if err != nil {
		t.Fatalf("first parse: %v", err)
	}

	// Keys reordered: name before type, field keys reordered too.
	s, err := cache.Parse(`{"name":"K","type":"record","fields":[{"type":"int","name":"x"}]}`)
	if err != nil {
		t.Fatalf("second parse with reordered keys: %v", err)
	}

	binary, err := s.Encode(map[string]any{"x": float64(99)})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded any
	mustDecode(t, s, binary, &decoded)
	if decoded.(map[string]any)["x"] != int32(99) {
		t.Errorf("x: got %v", decoded.(map[string]any)["x"])
	}
}

func TestSchemaCacheFieldOrderPreserved(t *testing.T) {
	// Field ARRAY order matters for Avro binary encoding.
	// Two schemas with the same fields in different order are different
	// schemas and must NOT be deduplicated.
	cache := &SchemaCache{}

	s1, err := cache.Parse(recABSchema)
	if err != nil {
		t.Fatalf("first parse: %v", err)
	}

	// Encode with field order a=1, b="hello".
	binary, err := s1.Encode(map[string]any{"a": float64(1), "b": "hello"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Parse a schema with swapped field order. This is a DIFFERENT schema
	// (different binary layout), so it must not dedup. It will error
	// because "R" is already in the cache — that's expected and correct.
	_, err = cache.Parse(recBASchema)
	if err == nil {
		t.Fatal("expected error: swapped field order is a different schema")
	}

	// Verify the original still decodes correctly.
	var decoded any
	mustDecode(t, s1, binary, &decoded)
	m := decoded.(map[string]any)
	if m["a"] != int32(1) {
		t.Errorf("a: got %v", m["a"])
	}
	if m["b"] != "hello" {
		t.Errorf("b: got %v", m["b"])
	}
}

func TestSchemaCacheDedupPreservesLargeNumbers(t *testing.T) {
	// Verify that JSON normalization preserves large integers exactly,
	// so two schemas with the same large "size" value dedup correctly.
	cache := &SchemaCache{}

	schema1 := `{"type":"fixed","name":"Big","size":9007199254740993}`
	schema2 := `{ "type": "fixed", "name": "Big", "size": 9007199254740993 }`

	s1, err := cache.Parse(schema1)
	if err != nil {
		t.Fatalf("first parse: %v", err)
	}
	s2, err := cache.Parse(schema2)
	if err != nil {
		t.Fatalf("second parse (whitespace only): %v", err)
	}
	if s1 != s2 {
		t.Error("expected same *Schema pointer for whitespace-only difference")
	}
}

func TestSchemaCacheConflictingDefinition(t *testing.T) {
	// Two different schemas defining the same name should still error.
	cache := &SchemaCache{}

	mustCacheParse(t, cache, `{
		"type": "record",
		"name": "Foo",
		"fields": [{"name": "x", "type": "int"}]
	}`)

	_, err := cache.Parse(`{
		"type": "record",
		"name": "Foo",
		"fields": [{"name": "y", "type": "string"}]
	}`)
	if err == nil {
		t.Fatal("expected error for conflicting definition of Foo")
	}
}

func TestSchemaCacheZeroValue(t *testing.T) {
	// A zero-value SchemaCache should work.
	var c SchemaCache
	s := mustCacheParse(t, &c, `"int"`)
	mustEncode(t, s, int32(42))
}

func TestSchemaCacheConcurrent(t *testing.T) {
	cache := &SchemaCache{}
	mustCacheParse(t, cache, `{
		"type": "record",
		"name": "Base",
		"fields": [{"name": "id", "type": "int"}]
	}`)
	const goroutines = 8
	errs := make(chan error, goroutines)
	for range goroutines {
		go func() {
			_, err := cache.Parse(`{
				"type": "record",
				"name": "Wrapper",
				"fields": [{"name": "base", "type": "Base"}]
			}`)
			errs <- err
		}()
	}
	for range goroutines {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

// A consistent CustomType registration resolves across the cache
// boundary for EVERY named-type shape: the guard's allow arm compares
// the cached type's custom-affected flag against this Parse's
// registrations, and that flag must be stamped AFTER custom wiring runs.
// Named types whose OWN node matches the CustomType (fixed/enum — their
// registration precedes applyCustomTypes) previously kept a stale false
// flag, so the forward arm rejected a registration the documented
// contract accepts ("a consistent registration resolves").
func TestMatrix_SchemaCacheConsistentCustomSelfMatch(t *testing.T) {
	dec := func(v any, sn *SchemaNode) (any, error) { return v, nil }
	cases := []struct {
		name, def, ref string
		ct             CustomType
	}{
		{
			"fixed self-match decimal",
			`{"type":"fixed","name":"X","size":4,"logicalType":"decimal","precision":9,"scale":2}`,
			`{"type":"record","name":"Y","fields":[{"name":"x","type":"X"}]}`,
			CustomType{LogicalType: "decimal", Decode: dec},
		},
		{
			"enum self-match AvroType",
			`{"type":"enum","name":"X","symbols":["A","B"]}`,
			`{"type":"record","name":"Y","fields":[{"name":"x","type":"X"}]}`,
			CustomType{AvroType: "enum", Decode: dec},
		},
		{
			"record subtree match",
			`{"type":"record","name":"X","fields":[{"name":"t","type":{"type":"long","logicalType":"timestamp-millis"}}]}`,
			`{"type":"record","name":"Y","fields":[{"name":"x","type":"X"}]}`,
			CustomType{LogicalType: "timestamp-millis", Decode: dec},
		},
		{
			"namespaced record subtree match",
			`{"type":"record","name":"X","namespace":"com.x","fields":[{"name":"t","type":{"type":"long","logicalType":"timestamp-millis"}}]}`,
			`{"type":"record","name":"Y","namespace":"com.x","fields":[{"name":"x","type":"X"}]}`,
			CustomType{LogicalType: "timestamp-millis", Decode: dec},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var cache SchemaCache
			if _, err := cache.Parse(c.def, WithCustomType(c.ct)); err != nil {
				t.Fatalf("defining parse: %v", err)
			}
			if _, err := cache.Parse(c.ref, WithCustomType(c.ct)); err != nil {
				t.Fatalf("consistent referencing parse: %v", err)
			}
		})
	}
}

// Re-parsing the same valid schema string under WithLaxNames must return
// success (the SchemaCache contract: "Parsing the same schema string
// multiple times is allowed"). WithLaxNames skips string-dedup (the
// compiled result isn't identified by the string alone), so the re-parse
// re-enters the builder and re-registers the inherited name — which needs
// allowReRegister, granted to the custom-types skip path but not the lax
// one. Without it, the second parse errors "duplicate named type".
func TestRegression_SchemaCacheLaxNamesReParse(t *testing.T) {
	const schema = `{"type":"record","name":"R","fields":[{"name":"v","type":"long"}]}`
	cases := []struct {
		name          string
		first, second []SchemaOpt
	}{
		{"lax then lax", []SchemaOpt{WithLaxNames(nil)}, []SchemaOpt{WithLaxNames(nil)}},
		{"strict then lax", nil, []SchemaOpt{WithLaxNames(nil)}},
		{"lax then strict", []SchemaOpt{WithLaxNames(nil)}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var cache SchemaCache
			if _, err := cache.Parse(schema, c.first...); err != nil {
				t.Fatalf("first parse: %v", err)
			}
			s, err := cache.Parse(schema, c.second...)
			if err != nil {
				t.Fatalf("re-parse same schema: %v", err)
			}
			// The re-parsed schema must still work.
			if _, err := s.AppendEncode(nil, struct {
				V int64 `avro:"v"`
			}{V: 7}); err != nil {
				t.Fatalf("encode after re-parse: %v", err)
			}
		})
	}

	// A genuine conflicting redefinition must still error.
	var cache SchemaCache
	if _, err := cache.Parse(schema, WithLaxNames(nil)); err != nil {
		t.Fatalf("first: %v", err)
	}
	conflict := `{"type":"record","name":"R","fields":[{"name":"w","type":"string"}]}`
	if _, err := cache.Parse(conflict, WithLaxNames(nil)); err == nil {
		t.Fatal("conflicting redefinition of R accepted; want duplicate error")
	}
}

// A custom-mode Parse of a NEW string that redefines an inherited name
// with a DIFFERENT body must error (duplicate named type), exactly as
// strict and lax do — re-registration is for re-parsing the SAME schema,
// never for silently overwriting a cached type with a conflicting one.
// allowReRegister formerly OR'd in hasCustomTypes, granting re-register
// to any custom parse including a conflicting redefinition.
func TestRegression_SchemaCacheCustomConflictRejected(t *testing.T) {
	const base = `{"type":"record","name":"R","fields":[{"name":"v","type":"long"}]}`
	ct := CustomType{LogicalType: "nope", AvroType: "long"} // matches nothing in R

	for _, c := range []struct {
		name, conflict string
	}{
		{"field change", `{"type":"record","name":"R","fields":[{"name":"w","type":"string"}]}`},
		{"kind change", `{"type":"enum","name":"R","symbols":["A"]}`},
		{"fixed size change vs record", `{"type":"fixed","name":"R","size":4}`},
	} {
		t.Run(c.name, func(t *testing.T) {
			var cache SchemaCache
			if _, err := cache.Parse(base, WithCustomType(ct)); err != nil {
				t.Fatalf("base parse: %v", err)
			}
			if _, err := cache.Parse(c.conflict, WithCustomType(ct)); err == nil {
				t.Errorf("conflicting redefinition under custom mode accepted; want duplicate-name error")
			}
		})
	}

	// The legitimate paths must still work: re-parsing the SAME custom
	// schema, and a consistent custom cross-reference.
	var cache SchemaCache
	if _, err := cache.Parse(base, WithCustomType(ct)); err != nil {
		t.Fatalf("base: %v", err)
	}
	if _, err := cache.Parse(base, WithCustomType(ct)); err != nil {
		t.Errorf("re-parse of same custom schema must succeed: %v", err)
	}
	if _, err := cache.Parse(`{"type":"record","name":"Outer","fields":[{"name":"r","type":"R"}]}`, WithCustomType(ct)); err != nil {
		t.Errorf("consistent custom cross-reference must succeed: %v", err)
	}
}

// TestRegression_SchemaCacheLocalShadowNotSplicedFromCache pins that the
// self-containment splice resolves a bare name reference exactly as the parser
// bound it: eager, in-scope-first, and POSITIONAL. A parse under namespace
// "myns" that locally defines T and also bare-references "T" binds the reference
// per its position — AFTER the local def binds to the local myns.T, BEFORE it
// binds to the cached null-namespace T, the local name not yet being in scope.
//
// The splice must reproduce whichever binding the wire codec used so the
// schema's own String()/Canonical() describe the SAME schema. The bug: the
// splice consulted a position-independent local set and a bare fallback key, so
// it always swapped in the cached T — making the metadata diverge from the wire,
// silent decode corruption for any consumer that re-parses the schema text. The
// asserted invariant is binding-agnostic: decoding the wire with
// Parse(s.String()) must produce exactly what decoding with s produces.
func TestRegression_SchemaCacheLocalShadowNotSplicedFromCache(t *testing.T) {
	cases := []struct {
		order string
		b     any // shape valid for b's actual (positional) binding
	}{
		{"ref-after-localdef", map[string]any{"y": "bb"}},      // b -> local string myns.T
		{"ref-before-localdef", map[string]any{"x": int32(2)}}, // b -> cached int T (eager)
	}
	for _, tc := range cases {
		t.Run(tc.order, func(t *testing.T) {
			var c SchemaCache
			mustCacheParse(t, &c, `{"type":"record","name":"T","fields":[{"name":"x","type":"int"}]}`)
			mustCacheParse(t, &c, `{"type":"record","name":"U","fields":[{"name":"u","type":"int"}]}`)
			localDef := `{"name":"a","type":{"type":"record","name":"T","fields":[{"name":"y","type":"string"}]}}`
			bareRef := `{"name":"b","type":"T"}`
			danglingU := `{"name":"c","type":"U"}`
			var fields string
			if tc.order == "ref-after-localdef" {
				fields = localDef + "," + bareRef + "," + danglingU
			} else {
				fields = bareRef + "," + localDef + "," + danglingU
			}
			s, err := c.Parse(`{"type":"record","name":"myns.R","fields":[` + fields + `]}`)
			if err != nil {
				t.Fatal(err)
			}

			val := map[string]any{
				"a": map[string]any{"y": "aa"},
				"b": tc.b,
				"c": map[string]any{"u": int32(7)},
			}
			wire, err := s.Encode(val)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			s2, err := Parse(s.String())
			if err != nil {
				t.Fatalf("Parse(String()): %v", err)
			}
			var viaSelf, viaString map[string]any
			if _, err := s.Decode(wire, &viaSelf); err != nil {
				t.Fatalf("decode via s: %v", err)
			}
			if _, err := s2.Decode(wire, &viaString); err != nil {
				t.Fatalf("decode via Parse(String()): %v", err)
			}
			selfJSON, _ := json.Marshal(viaSelf)
			stringJSON, _ := json.Marshal(viaString)
			if string(selfJSON) != string(stringJSON) {
				t.Errorf("String() describes a different schema than the wire codec:\n via s     : %s\n via String: %s", selfJSON, stringJSON)
			}
		})
	}
}

// TestRegression_SchemaCacheLaxNameStickyNotDangling pins that a type defined with
// WithLaxNames and referenced by a later strict Parse yields a SELF-CONTAINED
// metadata form (String/Canonical contain the spliced definition, not a dangling
// reference), re-parseable WITH WithLaxNames. Lax names are sticky: a schema
// containing one is not parseable without WithLaxNames whether or not a cache
// produced it. The strict re-parse used to inline the body then reject the lax
// NAME, silently falling back to a dangling reference no parser could resolve; the
// splice now retries permissively. Encode/Decode are unaffected throughout.
func TestRegression_SchemaCacheLaxNameStickyNotDangling(t *testing.T) {
	var c SchemaCache
	mustCacheParse(t, &c, `{"type":"record","name":"bad-name","fields":[{"name":"x","type":"int"}]}`, WithLaxNames(nil))
	s, err := c.Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":"bad-name"}]}`)
	if err != nil {
		t.Fatalf("strict parse referencing a lax-defined cached type: %v", err)
	}

	// Encode/Decode work regardless.
	wire, err := s.Encode(map[string]any{"f": map[string]any{"x": int32(3)}})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var back map[string]any
	mustDecode(t, s, wire, &back)

	// String()/Canonical() are self-contained: the bad-name body is present,
	// and they re-parse with WithLaxNames (a lax name needs it, cache or not).
	if !json.Valid([]byte(s.String())) || !containsField(s.String(), "x") {
		t.Errorf("String() not self-contained (bad-name body missing): %s", s.String())
	}
	if _, err := Parse(s.String(), WithLaxNames(nil)); err != nil {
		t.Errorf("Parse(String(), WithLaxNames) failed — not self-contained: %v", err)
	}
	if _, err := Parse(string(s.Canonical()), WithLaxNames(nil)); err != nil {
		t.Errorf("Parse(Canonical(), WithLaxNames) failed: %v", err)
	}
}

func containsField(s, field string) bool {
	return json.Valid([]byte(s)) && len(s) > 0 && stringContains(s, `"`+field+`"`)
}

func stringContains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// ---------- cache_straykey_test.go ----------

// A reserved container key on a kind that does not bind it — a stray
// "items"/"values"/"fields" on a primitive object — parses as inert as-written
// metadata: the wire parser never name-binds anything inside it. Every consumer
// that treats container keys as SCHEMA positions (collecting definitions for
// cross-parse reference, splicing cached definitions, registering names for
// metadata default coercion) must therefore enumerate only the keys the node's
// kind BINDS, or it consumes structure the parse never bound. The pins here lock
// that gate for the SchemaCache walkers and the metadata name table, and lock
// the one deliberate exception: the SchemaNode metadata walker surfaces stray
// container keys as-written, a read-only duty with no registration or mutation.

// cacheStrayRealGDef defines n.G with field "h" of type string — the
// definition the parser actually binds in these tests.
const cacheStrayRealGDef = `{"type":"record","name":"n.G","fields":[{"name":"h","type":"string"}]}`

// cacheStrayGDef returns a CONFLICTING definition of n.G (field "g" of the
// given type) — the shape planted under a stray structural key.
func cacheStrayGDef(fieldType string) string {
	return `{"type":"record","name":"n.G","fields":[{"name":"g","type":"` + fieldType + `"}]}`
}

// cacheStrayCarrier wraps payload under the given stray key on a
// primitive-kind object. For "items"/"values" the payload sits directly
// under the key; for "fields" it sits as a field's type, the position the
// fields walk would descend.
func cacheStrayCarrier(kind, key, payload string) string {
	if key == "fields" {
		return `{"type":"` + kind + `","fields":[{"name":"f","type":` + payload + `}]}`
	}
	return `{"type":"` + kind + `","` + key + `":` + payload + `}`
}

// cacheParseSeq parses texts in order into one fresh SchemaCache and
// returns the last schema.
func cacheParseSeq(t *testing.T, texts ...string) *Schema {
	t.Helper()
	var c SchemaCache
	var s *Schema
	var err error
	for _, text := range texts {
		if s, err = c.Parse(text); err != nil {
			t.Fatalf("cache parse of %s: %v", text, err)
		}
	}
	return s
}

// cacheSurfaceImage captures every metadata-derived surface of a schema:
// the canonical form, the Rabin fingerprint, the single-object-encoding
// header (of sample's encoding), the stored JSON text, and the metadata
// tree. Two schemas that must describe the same logical schema must agree
// on all five.
type cacheSurfaceImage struct {
	canonical, fp, soe, str string
	root                    SchemaNode
}

func cacheSurfaces(t *testing.T, s *Schema, sample any) cacheSurfaceImage {
	t.Helper()
	b := mustAppendSingleObject(t, s, nil, sample)
	return cacheSurfaceImage{
		canonical: string(s.Canonical()),
		fp:        fmt.Sprintf("%x", s.Fingerprint(NewRabin())),
		soe:       fmt.Sprintf("%x", b[:10]),
		str:       s.String(),
		root:      *s.Root(),
	}
}

func assertCacheSurfacesEqual(t *testing.T, got, want cacheSurfaceImage) {
	t.Helper()
	if got.canonical != want.canonical {
		t.Errorf("Canonical describes a different schema:\n got  %s\n want %s", got.canonical, want.canonical)
	}
	if got.fp != want.fp {
		t.Errorf("fingerprint %s, want %s", got.fp, want.fp)
	}
	if got.soe != want.soe {
		t.Errorf("single-object header %s, want %s", got.soe, want.soe)
	}
	if got.str != want.str {
		t.Errorf("String:\n got  %s\n want %s", got.str, want.str)
	}
	if !reflect.DeepEqual(got.root, want.root) {
		t.Errorf("Root trees differ:\n got  %+v\n want %+v", got.root, want.root)
	}
}

// A definition of n.G planted in a stray structural key must not enter the
// cache's cross-parse definition store: the store is first-wins, so an
// inert-position body occupying the fullname would shadow the REAL
// definition parsed later, and a cross-parse reference's metadata surfaces
// (Canonical / fingerprint / SOE header / String / Root) — rebuilt from a
// splice of the stored definition — would then describe a schema the wire
// codec (which resolves the parser-bound definition) rejects.
func TestRegression_CacheStrayKeyDefCrossParseSurfaces(t *testing.T) {
	t.Parallel()
	ref := `{"type":"array","items":"n.G"}`
	accept := []map[string]any{{"h": "x"}}
	reject := []map[string]any{{"g": int32(7)}}
	for _, key := range []string{"items", "values", "fields"} {
		t.Run(key, func(t *testing.T) {
			s := cacheParseSeq(t,
				cacheStrayCarrier("int", key, cacheStrayGDef("int")),
				cacheStrayRealGDef,
				ref,
			)
			control := cacheParseSeq(t, cacheStrayRealGDef, ref)

			// The wire codec resolves the parser-bound definition.
			if _, err := s.Encode(accept); err != nil {
				t.Fatalf("encode of the bound-definition value: %v", err)
			}
			if _, err := s.Encode(reject); err == nil {
				t.Fatalf("encode of the stray-shaped value unexpectedly accepted")
			}
			// Every metadata surface must describe that same schema.
			assertCacheSurfacesEqual(t, cacheSurfaces(t, s, accept), cacheSurfaces(t, control, accept))
		})
	}
}

// The splice twin of the same gate: a cached definition must not be
// spliced over a reference string sitting in a stray structural key. The
// as-written authority is a plain (cache-less) Parse of the same text —
// SchemaCache.Parse must surface identical metadata. The stored text is
// compared STRUCTURALLY (SchemaCache.Parse normalizes its input through a
// json.Marshal round trip, so key order differs from the caller's
// spelling by design); a spliced definition still fails the structural
// comparison because it replaces the reference string with an object.
func TestRegression_CacheSpliceStrayKeyAsWritten(t *testing.T) {
	t.Parallel()
	sample := int32(7)
	for _, key := range []string{"items", "values", "fields"} {
		t.Run(key, func(t *testing.T) {
			text := cacheStrayCarrier("int", key, `"n.G"`)
			plain, err := Parse(text)
			if err != nil {
				t.Fatalf("plain parse: %v", err)
			}
			cached := cacheParseSeq(t, cacheStrayRealGDef, text)
			got, want := cacheSurfaces(t, cached, sample), cacheSurfaces(t, plain, sample)
			var gotTree, wantTree any
			if err := json.Unmarshal([]byte(got.str), &gotTree); err != nil {
				t.Fatalf("stored text does not unmarshal: %v", err)
			}
			if err := json.Unmarshal([]byte(want.str), &wantTree); err != nil {
				t.Fatalf("plain text does not unmarshal: %v", err)
			}
			if !reflect.DeepEqual(gotTree, wantTree) {
				t.Errorf("stored text structure:\n got  %s\n want %s", got.str, want.str)
			}
			got.str, want.str = "", ""
			assertCacheSurfacesEqual(t, got, want)
		})
	}
}

// The metadata name table (name-reference default coercion) registers
// exactly what the wire builder registers. A conflicting n.G body inside a
// stray key must not enter the table in EITHER parse order — pre-gate the
// walk order decided which body a name-ref default coerced through, so a
// stray walked after the real definition silently flipped a string-field
// default into the stray's bytes materialization.
func TestRegression_MetadataNameTableIgnoresStrayKeyDef(t *testing.T) {
	t.Parallel()
	realDef := `{"name":"f1","type":{"type":"record","name":"n.G","fields":[{"name":"b","type":"string"}]}}`
	strayCarrier := `{"name":"f2","type":{"type":"int","items":{"type":"record","name":"n.G","fields":[{"name":"b","type":"bytes"}]}}}`
	refWithDefault := `{"name":"f3","type":"n.G","default":{"b":"AQ"}}`
	for name, order := range map[string][]string{
		"real_then_stray": {realDef, strayCarrier, refWithDefault},
		"stray_then_real": {strayCarrier, realDef, refWithDefault},
	} {
		t.Run(name, func(t *testing.T) {
			s := mustParse(t, `{"type":"record","name":"R","fields":[`+strings.Join(order, ",")+`]}`)
			var f3 *SchemaField
			root := s.Root()
			for i := range root.Fields {
				if root.Fields[i].Name == "f3" {
					f3 = &root.Fields[i]
				}
			}
			if f3 == nil {
				t.Fatal("field f3 missing from Root")
			}
			d, ok := f3.Default.(map[string]any)
			if !ok {
				t.Fatalf("f3 Default is %T, want map", f3.Default)
			}
			// The bound n.G's "b" is a string field: the default's value
			// stays the string "AQ" (the wire path coerces through the
			// bound definition; the metadata path must match it).
			if got, want := d["b"], any("AQ"); !reflect.DeepEqual(got, want) {
				t.Errorf("f3 Default[b] = %T %v, want the bound definition's string coercion %q", got, got, want)
			}
		})
	}
}

// The one deliberate exception to the binding-kind gate: the SchemaNode
// metadata walker surfaces stray container keys AS-WRITTEN on the
// matching structural field (and keeps them out of Props) — a read-only
// surfacing duty with no registration or mutation. This pin locks the
// asymmetry so a uniformity change that gates the metadata walker too
// fails here instead of silently dropping the surfacing.
func TestMatrix_MetadataStrayKeySurfacedAsWritten(t *testing.T) {
	t.Parallel()
	t.Run("items_ref", func(t *testing.T) {
		s := MustParse(`{"type":"int","items":"long"}`)
		root := s.Root()
		if root.Items == nil || root.Items.Type != "long" {
			t.Fatalf("stray items not surfaced as written: %+v", root.Items)
		}
		if _, ok := root.Props["items"]; ok {
			t.Errorf("stray items leaked into Props")
		}
	})
	t.Run("items_def", func(t *testing.T) {
		s := MustParse(cacheStrayCarrier("int", "items", cacheStrayGDef("int")))
		root := s.Root()
		if root.Items == nil || root.Items.Type != "record" || root.Items.Name != "n.G" || len(root.Items.Fields) != 1 {
			t.Fatalf("stray items definition not surfaced as written: %+v", root.Items)
		}
	})
	t.Run("values_ref", func(t *testing.T) {
		s := MustParse(`{"type":"string","values":"long"}`)
		root := s.Root()
		if root.Values == nil || root.Values.Type != "long" {
			t.Fatalf("stray values not surfaced as written: %+v", root.Values)
		}
	})
	t.Run("fields_def", func(t *testing.T) {
		s := MustParse(cacheStrayCarrier("int", "fields", cacheStrayGDef("int")))
		root := s.Root()
		if len(root.Fields) != 1 || root.Fields[0].Name != "f" || root.Fields[0].Type.Name != "n.G" {
			t.Fatalf("stray fields not surfaced as written: %+v", root.Fields)
		}
	})
}

// strayFieldElementRoute is one cell of the ELEMENT-SHAPE axis of a stray
// "fields" body. Every cell above spells its elements one way — a JSON
// object with a "type" key — so the body-shape axis they cross says
// nothing about what the walk does with the elements INSIDE a body that
// passed the shape check. walkNodeChildren routes each element to exactly
// one per-element callback, and route is the callback's name: the axis is
// that callback set, not a sample of element spellings.
type strayFieldElementRoute struct {
	// route names the nodeChildVisitor callback this element shape fires.
	// TestInvariant_StrayFieldElementRoutesAreSpanned derives the callback
	// set from the struct itself and requires a cell per member, so a new
	// per-element route cannot ship unexercised.
	route string
	elem  string
	// want is the as-written surface the element must produce. It is
	// spelled out per cell rather than derived from the element, so a
	// walk that fabricated a plausible-looking field still fails.
	want SchemaField
}

var strayFieldElementRoutes = []strayFieldElementRoute{
	{
		// The ordinary spelling: a JSON object naming its own type.
		route: "field",
		elem:  `{"name":"f","type":"int","doc":"normal"}`,
		want:  SchemaField{Name: "f", Doc: "normal", Type: SchemaNode{Type: "int"}},
	},
	{
		// A field with no "type" key. This never parses at a BOUND
		// position — the record build rejects a nil field type — so it
		// exists only inside a stray "fields", where the build never
		// runs. Its attributes must surface as written, on a field whose
		// Type is the zero node: the alternative is a fabricated zero
		// element left in the pre-sized slot, which would read as a field
		// the schema never wrote.
		route: "fieldNoType",
		elem:  `{"name":"x","doc":"typeless","myprop":1}`,
		want:  SchemaField{Name: "x", Doc: "typeless", Props: map[string]any{"myprop": int64(1)}},
	},
	{
		// Flat form: the element's own keys carry the lifted type
		// definition, named after the field. The stray walk must lift it
		// exactly as the parser would at a bound position.
		route: "flatField",
		elem:  `{"name":"g","type":"record","fields":[{"name":"y","type":"int"}]}`,
		want: SchemaField{Name: "g", Type: SchemaNode{
			Type: "record", Name: "g",
			Fields: []SchemaField{{Name: "y", Type: SchemaNode{Type: "int"}}},
		}},
	},
}

// TestMatrix_StrayFieldElementRoutes crosses the element-shape axis: each
// shape alone, and all of them together in one body. The mixed body is not
// a repeat of the singles — the walk pre-sizes the field slice from the raw
// array and fills slots by INDEX, so a route that declines to fill its slot
// shows up only when a filled slot sits next to it, and only the mixed cell
// can tell "surfaced as written" from "happened to land in slot 0".
func TestMatrix_StrayFieldElementRoutes(t *testing.T) {
	t.Parallel()
	check := func(t *testing.T, got, want SchemaField, where string) {
		t.Helper()
		if got.Name != want.Name || got.Doc != want.Doc {
			t.Errorf("%s: name/doc = %q/%q, want %q/%q", where, got.Name, got.Doc, want.Name, want.Doc)
		}
		if got.Type.Type != want.Type.Type || got.Type.Name != want.Type.Name {
			t.Errorf("%s: type = %q/%q, want %q/%q", where, got.Type.Type, got.Type.Name, want.Type.Type, want.Type.Name)
		}
		if len(got.Type.Fields) != len(want.Type.Fields) {
			t.Errorf("%s: lifted type has %d fields, want %d", where, len(got.Type.Fields), len(want.Type.Fields))
		}
		for k, wv := range want.Props {
			if gv, ok := got.Props[k]; !ok || !reflect.DeepEqual(gv, wv) {
				t.Errorf("%s: Props[%q] = %#v (present=%v), want %#v", where, k, gv, ok, wv)
			}
		}
		if len(got.Props) != len(want.Props) {
			t.Errorf("%s: Props = %#v, want exactly %#v", where, got.Props, want.Props)
		}
	}
	carrier := func(body string) string {
		return `{"type":"record","name":"Top","fields":[{"name":"a","type":{"type":"int","fields":[` + body + `]}}]}`
	}
	for _, c := range strayFieldElementRoutes {
		t.Run(c.route, func(t *testing.T) {
			s, err := Parse(carrier(c.elem))
			if err != nil {
				t.Fatalf("stray fields with a %s element rejected: %v", c.route, err)
			}
			n := s.Root().Fields[0].Type
			if len(n.Fields) != 1 {
				t.Fatalf("stray fields arity: got %d, want 1 (%+v)", len(n.Fields), n.Fields)
			}
			check(t, n.Fields[0], c.want, c.route)
			if _, inProps := n.Props["fields"]; inProps {
				t.Errorf("a shape-OK stray fields body also rode to Props: %v", n.Props)
			}
			// The stray is inert on the wire: the carrier still encodes
			// as the bare int it says it is.
			if _, err := s.Encode(map[string]any{"a": int32(3)}); err != nil {
				t.Errorf("stray fields changed the wire behavior: %v", err)
			}
		})
	}
	t.Run("mixed", func(t *testing.T) {
		var elems []string
		for _, c := range strayFieldElementRoutes {
			elems = append(elems, c.elem)
		}
		s, err := Parse(carrier(strings.Join(elems, ",")))
		if err != nil {
			t.Fatalf("mixed stray fields body rejected: %v", err)
		}
		n := s.Root().Fields[0].Type
		if len(n.Fields) != len(strayFieldElementRoutes) {
			t.Fatalf("mixed arity: got %d, want %d (%+v)", len(n.Fields), len(strayFieldElementRoutes), n.Fields)
		}
		for i, c := range strayFieldElementRoutes {
			check(t, n.Fields[i], c.want, "mixed["+strconv.Itoa(i)+"]="+c.route)
		}
		// The rebuild must carry every route's surface through a second
		// generation; a fabricated zero element would survive the first
		// rebuild and only then diverge.
		rb, err := n.Schema()
		if err != nil {
			t.Fatalf("rebuild: %v", err)
		}
		rn := rb.Root()
		if len(rn.Fields) != len(strayFieldElementRoutes) {
			t.Fatalf("rebuild arity: got %d, want %d", len(rn.Fields), len(strayFieldElementRoutes))
		}
		for i, c := range strayFieldElementRoutes {
			check(t, rn.Fields[i], c.want, "rebuilt["+strconv.Itoa(i)+"]="+c.route)
		}
	})
}

// TestInvariant_StrayFieldElementRoutesAreSpanned derives the per-element route
// set from nodeChildVisitor itself — a callback taking the element INDEX as its
// first argument is a per-element route, everything else fires once per node — and
// requires the matrix above to drive each one. It reds in both directions: a route
// added to the visitor with no cell, and a cell naming a route the visitor no
// longer has. That set is what the pins in this area kept proving one member of at
// a time; deriving it is the difference between a suite that covers the routes it
// happens to know about and one that cannot fall behind the code.
func TestInvariant_StrayFieldElementRoutesAreSpanned(t *testing.T) {
	t.Parallel()
	vt := reflect.TypeOf(nodeChildVisitor{})
	perElement := map[string]bool{}
	for i := range vt.NumField() {
		f := vt.Field(i)
		if f.Type.Kind() != reflect.Func || f.Type.NumIn() == 0 {
			continue
		}
		if f.Type.In(0).Kind() == reflect.Int {
			perElement[f.Name] = true
		}
	}
	if len(perElement) == 0 {
		t.Fatal("no per-element callbacks found on nodeChildVisitor; the derivation rule (first argument is the element index) no longer holds, so this guard is watching nothing")
	}
	driven := map[string]bool{}
	for _, c := range strayFieldElementRoutes {
		driven[c.route] = true
	}
	for name := range perElement {
		if !driven[name] {
			t.Errorf("walkNodeChildren route %q has no cell in strayFieldElementRoutes; a stray fields element taking that route is walked by nothing the suite asserts", name)
		}
	}
	for name := range driven {
		if !perElement[name] {
			t.Errorf("strayFieldElementRoutes drives %q, which is not a per-element callback on nodeChildVisitor; the cell names a route that no longer exists", name)
		}
	}
}

// TestMatrix_CacheStrayStructuralKey crosses the stray-key gate's full domain:
// carrier kind x stray key x definition relation, each cell asserting every
// metadata surface against a stray-free control plus the wire verdict. Carriers
// split three ways by parse posture: primitive carriers accept the stray as
// inert — the gate cells; fixed/enum reject a foreign structural key outright,
// pinning the reject that keeps such carriers structurally un-poisonable; and
// array/map/record BIND the key, the controls proving the gate does not block
// genuine definitions.
//
// Relations for the gate cells: the conflicting stray body parsed before and
// after the real definition (the store is first-wins, so order decided the
// winner pre-gate), a self-referencing stray body, and a diamond — two
// independent definitions sharing the real n.G, spliced into one referencing
// schema, so the first-define-then-reference rewrite must key off parser-bound
// definitions only.
func TestMatrix_CacheStrayStructuralKey(t *testing.T) {
	t.Parallel()
	accept := []map[string]any{{"h": "x"}}
	reject := []map[string]any{{"g": int32(7)}}
	ref := `{"type":"array","items":"n.G"}`

	strayBodies := map[string]string{
		"conflicting": cacheStrayGDef("int"),
		"recursive":   `{"type":"record","name":"n.G","fields":[{"name":"s","type":["null","n.G"]}]}`,
	}

	for _, carrier := range []string{"int", "string"} {
		for _, key := range []string{"items", "values", "fields"} {
			for bodyName, body := range strayBodies {
				for _, order := range []string{"stray_first", "real_first"} {
					name := fmt.Sprintf("%s_%s_%s_%s", carrier, key, bodyName, order)
					t.Run(name, func(t *testing.T) {
						seq := []string{cacheStrayCarrier(carrier, key, body), cacheStrayRealGDef, ref}
						if order == "real_first" {
							seq[0], seq[1] = seq[1], seq[0]
						}
						s := cacheParseSeq(t, seq...)
						control := cacheParseSeq(t, cacheStrayRealGDef, ref)
						if _, err := s.Encode(accept); err != nil {
							t.Fatalf("encode of the bound-definition value: %v", err)
						}
						if _, err := s.Encode(reject); err == nil {
							t.Fatalf("encode of the stray-shaped value unexpectedly accepted")
						}
						assertCacheSurfacesEqual(t, cacheSurfaces(t, s, accept), cacheSurfaces(t, control, accept))
					})
				}
			}

			t.Run(fmt.Sprintf("%s_%s_diamond", carrier, key), func(t *testing.T) {
				defL := `{"type":"record","name":"n.L","fields":[{"name":"g","type":"n.G"}]}`
				defR := `{"type":"record","name":"n.R","fields":[{"name":"g","type":"n.G"}]}`
				follow := `{"type":"record","name":"n.F","fields":[{"name":"l","type":"n.L"},{"name":"r","type":"n.R"}]}`
				sample := map[string]any{
					"l": map[string]any{"g": map[string]any{"h": "x"}},
					"r": map[string]any{"g": map[string]any{"h": "y"}},
				}
				bad := map[string]any{
					"l": map[string]any{"g": map[string]any{"g": int32(7)}},
					"r": map[string]any{"g": map[string]any{"h": "y"}},
				}
				s := cacheParseSeq(t,
					cacheStrayCarrier(carrier, key, cacheStrayGDef("int")),
					cacheStrayRealGDef, defL, defR, follow,
				)
				control := cacheParseSeq(t, cacheStrayRealGDef, defL, defR, follow)
				if _, err := s.Encode(sample); err != nil {
					t.Fatalf("encode of the bound-definition value: %v", err)
				}
				if _, err := s.Encode(bad); err == nil {
					t.Fatalf("encode of the stray-shaped value unexpectedly accepted")
				}
				assertCacheSurfacesEqual(t, cacheSurfaces(t, s, sample), cacheSurfaces(t, control, sample))
			})
		}
	}

	// Foreign structural keys on fixed/enum reject at parse — these
	// carriers are structurally un-poisonable, and the reject is the
	// guard that keeps them so.
	for name, carrier := range map[string]string{
		"fixed": `{"type":"fixed","name":"Fx","size":4,`,
		"enum":  `{"type":"enum","name":"E","symbols":["A"],`,
	} {
		for _, key := range []string{"items", "values", "fields"} {
			t.Run(fmt.Sprintf("%s_%s_rejects", name, key), func(t *testing.T) {
				var text string
				if key == "fields" {
					text = carrier + `"fields":[{"name":"f","type":` + cacheStrayGDef("int") + `}]}`
				} else {
					text = carrier + `"` + key + `":` + cacheStrayGDef("int") + `}`
				}
				_, err := Parse(text)
				if err == nil || !strings.Contains(err.Error(), "has schema for other types") {
					t.Fatalf("foreign structural key on %s: got %v, want the structural-exclusivity reject", name, err)
				}
			})
		}
	}

	// Genuinely-binding carriers: a definition in a BOUND container key
	// registers and a cross-parse reference to it both resolves and
	// splices — the gate must not block the bound positions.
	for name, tc := range map[string]struct {
		def    string
		sample any
	}{
		"array_items":   {`{"type":"array","items":` + cacheStrayRealGDef + `}`, map[string]any{"g": map[string]any{"h": "x"}}},
		"map_values":    {`{"type":"map","values":` + cacheStrayRealGDef + `}`, map[string]any{"g": map[string]any{"h": "x"}}},
		"record_fields": {`{"type":"record","name":"n.Outer","fields":[{"name":"f","type":` + cacheStrayRealGDef + `}]}`, map[string]any{"g": map[string]any{"h": "x"}}},
	} {
		t.Run(name+"_binds", func(t *testing.T) {
			s := cacheParseSeq(t, tc.def, `{"type":"record","name":"n.U","fields":[{"name":"g","type":"n.G"}]}`)
			if _, err := s.Encode(tc.sample); err != nil {
				t.Fatalf("cross-parse reference to a bound-position definition: %v", err)
			}
			if !strings.Contains(s.String(), `"h"`) {
				t.Errorf("bound-position definition not spliced into the metadata text: %s", s.String())
			}
		})
	}
}

// The rebuild walker (SchemaNode.Schema) descends stray container keys to
// render them as-written, but its dedup consult must not treat those
// positions as SCHEMA positions: the wire parser registers nothing there,
// so a named definition inside a stray key can neither conflict with nor
// stand in for the real definition of the same fullname.
func TestRegression_RenderDedupIgnoresStrayDefinitions(t *testing.T) {
	t.Parallel()
	real := `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`
	conflicting := `{"type":"record","name":"R","fields":[{"name":"x","type":"string"}]}`
	carrier := func(def string) string {
		return `{"type":"int","foo":1,"items":` + def + `}`
	}
	t.Run("conflicting_body", func(t *testing.T) {
		s := MustParse(`{"type":"record","name":"Top","fields":[
			{"name":"a","type":` + carrier(conflicting) + `},
			{"name":"b","type":` + real + `}]}`)
		root := s.Root()
		if _, err := root.Schema(); err != nil {
			t.Errorf("rebuild failed for a wire-valid schema: %v", err)
		}
	})
	t.Run("identical_body", func(t *testing.T) {
		s := MustParse(`{"type":"record","name":"Top","fields":[
			{"name":"a","type":` + carrier(real) + `},
			{"name":"b","type":` + real + `}]}`)
		root := s.Root()
		if _, err := root.Schema(); err != nil {
			t.Errorf("rebuild failed for a wire-valid schema: %v", err)
		}
	})
	t.Run("stray_after_real", func(t *testing.T) {
		s := MustParse(`{"type":"record","name":"Top","fields":[
			{"name":"b","type":` + real + `},
			{"name":"a","type":` + carrier(real) + `}]}`)
		root := s.Root()
		rb := mustNodeSchema(t, root)
		if strings.Contains(rb.String(), `"items":"R"`) {
			t.Errorf("stray definition rewritten to a reference: %s", rb.String())
		}
	})
}

// A field element inside a stray "fields" key surfaces as-written even
// when it has no "type" key: the record build (which requires a field
// type) never runs for stray positions, so such elements are parseable
// and their written attributes must appear on the surfaced SchemaField —
// never a fabricated zero element.
func TestRegression_StrayFieldElementSurfacedAsWritten(t *testing.T) {
	t.Parallel()
	s := MustParse(`{"type":"record","name":"Top","fields":[{"name":"a","type":
		{"type":"int","fields":[{"name":"x","doc":"d","myprop":1}]}}]}`)
	fs := s.Root().Fields[0].Type.Fields
	if len(fs) != 1 {
		t.Fatalf("stray fields arity: got %d, want 1", len(fs))
	}
	if fs[0].Name != "x" || fs[0].Doc != "d" {
		t.Errorf("stray field element not surfaced as written: %+v", fs[0])
	}
	if got, ok := fs[0].Props["myprop"]; !ok || got != int64(1) {
		t.Errorf("stray field element props: got %v, want myprop=1", fs[0].Props)
	}
	if fs[0].Type.Type != "" {
		t.Errorf("typeless stray element must surface a zero Type, got %q", fs[0].Type.Type)
	}
}

// A stray container key survives SchemaNode.Schema() regardless of
// whether the carrier also has custom props or a logical type: surfacing
// is as-written, so the rebuilt schema must keep the stray on every
// carrier shape, and a second generation must be stable.
func TestRegression_StrayKeySurvivesSchemaRebuild(t *testing.T) {
	t.Parallel()
	for _, carrier := range []string{
		`{"type":"int","items":"long"}`,
		`{"type":"int","foo":1,"items":"long"}`,
	} {
		s := MustParse(`{"type":"record","name":"Top","fields":[{"name":"a","type":` + carrier + `}]}`)
		root := s.Root()
		rb, err := root.Schema()
		if err != nil {
			t.Fatalf("%s: rebuild: %v", carrier, err)
		}
		if !strings.Contains(rb.String(), `"items":"long"`) {
			t.Errorf("%s: stray dropped by rebuild: %s", carrier, rb.String())
			continue
		}
		rbRoot := rb.Root()
		rb2, err := rbRoot.Schema()
		if err != nil {
			t.Fatalf("%s: second-generation rebuild: %v", carrier, err)
		}
		if rb.String() != rb2.String() {
			t.Errorf("%s: rebuild not stable across generations:\n gen1: %s\n gen2: %s", carrier, rb.String(), rb2.String())
		}
	}
}

// A reserved-key body that does not parse as the key's schema shape is
// inert on a kind that does not bind the key: it cannot define, scope, or
// bind anything, so it surfaces verbatim in Props — the same treatment
// every non-reserved key gets. (Java skips reserved keys wholesale on
// non-binding kinds — Schema.java's SCHEMA_RESERVED set — and fastavro
// ignores them; rejecting was a twmb-only strictness.) Schema-shaped
// bodies keep the structural-field surfacing.
func TestMatrix_MalformedStrayBodyAcceptedAsProps(t *testing.T) {
	t.Parallel()
	cases := []struct {
		carrier string
		key     string
		want    any
	}{
		{`{"type":"int","items":3}`, "items", int64(3)},
		{`{"type":"int","values":true}`, "values", true},
		{`{"type":"int","fields":[3]}`, "fields", nil},
		{`{"type":"int","fields":3}`, "fields", int64(3)},
		{`{"type":"int","symbols":3}`, "symbols", int64(3)},
		{`{"type":"int","size":"x"}`, "size", "x"},
		{`{"type":"int","name":3}`, "name", int64(3)},
		{`{"type":"int","namespace":3}`, "namespace", int64(3)},
		{`{"type":"int","aliases":3}`, "aliases", int64(3)},
		{`{"type":"int","precision":"abc"}`, "precision", "abc"},
		{`{"type":"int","scale":"abc"}`, "scale", "abc"},
		{`{"type":"string","items":{"type":3}}`, "items", nil},
	}
	for _, c := range cases {
		s, err := Parse(`{"type":"record","name":"Top","fields":[{"name":"a","type":` + c.carrier + `}]}`)
		if err != nil {
			t.Errorf("%s: rejected: %v", c.carrier, err)
			continue
		}
		n := s.Root().Fields[0].Type
		got, ok := n.Props[c.key]
		if !ok {
			t.Errorf("%s: stray %q not surfaced in Props: %v", c.carrier, c.key, n.Props)
			continue
		}
		if c.want != nil && !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: Props[%q] = %v (%T), want %v", c.carrier, c.key, got, got, c.want)
		}
		var enc []byte
		enc, err = s.Encode(map[string]any{"a": int32(7)})
		if c.carrier[9:15] == "string" {
			enc, err = s.Encode(map[string]any{"a": "v"})
		}
		if err != nil {
			t.Errorf("%s: encode: %v", c.carrier, err)
			continue
		}
		var out map[string]any
		if _, err := s.Decode(enc, &out); err != nil {
			t.Errorf("%s: decode: %v", c.carrier, err)
		}
	}
}

// A malformed reserved-key body on a wrapped named REFERENCE rides as a
// prop too: the reference guard consults only successfully parsed
// structural attributes, and a body that cannot parse as the key's shape
// cannot be an attempt to define a type.
func TestRegression_MalformedStrayBodyOnWrappedRef(t *testing.T) {
	t.Parallel()
	s, err := Parse(`{"type":"record","name":"Top","fields":[
		{"name":"b","type":{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}},
		{"name":"a","type":{"type":"R","items":3}}]}`)
	if err != nil {
		t.Fatalf("rejected: %v", err)
	}
	n := s.Root().Fields[1].Type
	if n.Type != "R" {
		t.Fatalf("reference not preserved: %+v", n)
	}
	if got, ok := n.Props["items"]; !ok || got != int64(3) {
		t.Errorf("malformed stray on reference not in Props: %v", n.Props)
	}
}

// The adjudicated reject boundaries do not loosen with the malformed-body
// acceptance: a BINDING kind still shape-validates its own key; a
// container kind still rejects another kind's schema-shaped defining key;
// a schema-shaped stray name on an unnamed container still rejects; and a
// schema-shaped structural key on a wrapped reference still rejects.
func TestMatrix_StrayShapeRejectBoundaries(t *testing.T) {
	t.Parallel()
	for _, c := range []struct{ schema, wantErr string }{
		{`{"type":"array","items":3}`, "invalid schema"},
		{`{"type":"map","values":true}`, "invalid schema"},
		{`{"type":"record","name":"N","fields":3}`, `"fields" must be a JSON array`},
		{`{"type":"enum","name":"N","symbols":3}`, "array"},
		{`{"type":"fixed","name":"N","size":"x"}`, ""},
		{`{"type":"array","items":"int","fields":[{"name":"x","type":"int"}]}`, "has schema for other types"},
		{`{"type":"array","items":"int","name":"x"}`, ""},
	} {
		_, err := Parse(`{"type":"record","name":"Top","fields":[{"name":"a","type":` + c.schema + `}]}`)
		if err == nil {
			t.Errorf("%s: accepted, want reject", c.schema)
			continue
		}
		if c.wantErr != "" && !strings.Contains(err.Error(), c.wantErr) {
			t.Errorf("%s: error %q does not mention %q", c.schema, err, c.wantErr)
		}
	}
	if _, err := Parse(`{"type":"record","name":"Top","fields":[
		{"name":"b","type":{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}},
		{"name":"a","type":{"type":"R","items":"long"}}]}`); err == nil {
		t.Errorf("schema-shaped structural key on a wrapped reference accepted, want reject")
	}
}

// cacheStrayCarrierProps is cacheStrayCarrier with an extra custom
// property on the carrier object — the carrier shape that forces the
// rebuild's object render (a bare primitive emits as its type string).
func cacheStrayCarrierProps(kind, key, payload string) string {
	if key == "fields" {
		return `{"type":"` + kind + `","foo":1,"fields":[{"name":"f","type":` + payload + `}]}`
	}
	return `{"type":"` + kind + `","foo":1,"` + key + `":` + payload + `}`
}

// TestMatrix_CacheStrayRebuildSurface crosses carrier kind × stray key ×
// definition relation × carrier props × order for a SINGLE parse holding
// both a stray-planted definition and the real definition of the same
// fullname: the metadata rebuild must succeed (the dedup consult skips
// stray positions), preserve the wire verdicts, and be stable across a
// second generation — independent of whether the carrier's props force
// the object render.
func TestMatrix_CacheStrayRebuildSurface(t *testing.T) {
	t.Parallel()
	strayBodies := map[string]string{
		"conflicting": cacheStrayGDef("int"),
		"recursive":   `{"type":"record","name":"n.G","fields":[{"name":"s","type":["null","n.G"]}]}`,
	}
	// A substring of each body's rebuilt image, proving the stray content
	// survived the rebuild verbatim rather than being dropped or
	// rewritten to a reference.
	strayMarkers := map[string]string{
		"conflicting": `"name":"g"`,
		"recursive":   `"name":"s"`,
	}
	carrierValue := map[string]any{"int": int32(7), "string": "v"}
	for _, carrier := range []string{"int", "string"} {
		for _, key := range []string{"items", "values", "fields"} {
			for bodyName, body := range strayBodies {
				for _, props := range []string{"bare", "withprop"} {
					for _, order := range []string{"stray_first", "real_first"} {
						name := fmt.Sprintf("%s_%s_%s_%s_%s", carrier, key, bodyName, props, order)
						t.Run(name, func(t *testing.T) {
							strayType := cacheStrayCarrier(carrier, key, body)
							if props == "withprop" {
								strayType = cacheStrayCarrierProps(carrier, key, body)
							}
							fa := `{"name":"a","type":` + strayType + `}`
							fb := `{"name":"b","type":` + cacheStrayRealGDef + `}`
							if order == "real_first" {
								fa, fb = fb, fa
							}
							s := MustParse(`{"type":"record","name":"Top","fields":[` + fa + `,` + fb + `]}`)
							accept := map[string]any{"a": carrierValue[carrier], "b": map[string]any{"h": "x"}}
							reject := map[string]any{"a": carrierValue[carrier], "b": map[string]any{"g": int32(9)}}
							if _, err := s.Encode(accept); err != nil {
								t.Fatalf("encode of the bound-definition value: %v", err)
							}
							if _, err := s.Encode(reject); err == nil {
								t.Fatalf("encode of the stray-shaped value unexpectedly accepted")
							}
							root := s.Root()
							rb, err := root.Schema()
							if err != nil {
								t.Fatalf("rebuild: %v", err)
							}
							if _, err := rb.Encode(accept); err != nil {
								t.Errorf("rebuilt schema rejects the bound-definition value: %v", err)
							}
							if _, err := rb.Encode(reject); err == nil {
								t.Errorf("rebuilt schema accepts the stray-shaped value")
							}
							if !strings.Contains(rb.String(), strayMarkers[bodyName]) {
								t.Errorf("stray body did not survive the rebuild: %s", rb.String())
							}
							if props == "withprop" && !strings.Contains(rb.String(), `"foo":1`) {
								t.Errorf("carrier props did not survive the rebuild: %s", rb.String())
							}
							rbRoot := rb.Root()
							rb2, err := rbRoot.Schema()
							if err != nil {
								t.Fatalf("second-generation rebuild: %v", err)
							}
							if rb.String() != rb2.String() {
								t.Errorf("rebuild unstable across generations:\n gen1 %s\n gen2 %s", rb.String(), rb2.String())
							}
						})
					}
				}
			}
		}
	}
}

// Defaults inside a stray-surfaced body get the same normalization every
// SchemaField.Default gets (string→float for float kinds, codepoint
// string→[]byte for bytes) — the default pipeline is uniform over the
// surfaced tree, and the render's inverse fixups keep the re-emitted
// image equal to the written one. The name table consulted for
// name-referenced defaults is built from BOUND positions only, so the
// normalization inside a stray can never register or resolve a name.
func TestRegression_StrayBodyDefaultNormalization(t *testing.T) {
	t.Parallel()
	s := MustParse(`{"type":"record","name":"Top","fields":[{"name":"a","type":
		{"type":"int","items":{"type":"record","name":"SB","fields":[
			{"name":"f","type":"bytes","default":"abc"},
			{"name":"g","type":"double","default":"1.5"}]}}}]}`)
	stray := s.Root().Fields[0].Type.Items
	if stray == nil {
		t.Fatalf("stray items not surfaced")
	}
	if got, ok := stray.Fields[0].Default.([]byte); !ok || string(got) != "abc" {
		t.Errorf("bytes default in a stray body: got %T %v, want []byte(\"abc\")", stray.Fields[0].Default, stray.Fields[0].Default)
	}
	if got, ok := stray.Fields[1].Default.(float64); !ok || got != 1.5 {
		t.Errorf("double default in a stray body: got %T %v, want float64(1.5)", stray.Fields[1].Default, stray.Fields[1].Default)
	}
	root := s.Root()
	rb := mustNodeSchema(t, root)
	if !strings.Contains(rb.String(), `"default":"abc"`) || !strings.Contains(rb.String(), `"default":1.5`) {
		t.Errorf("stray-body defaults did not re-emit their written images: %s", rb.String())
	}
}

// Branches on a non-union kind have no JSON spelling the parser could
// bind — only a hand-built tree can carry them — and every consumer
// treats them as inert: the render never descends them (a bare primitive
// emits its type string; the object render has no branches arm outside
// the union case), so the rebuild neither emits them, registers names
// from them, nor conflicts on them.
func TestRegression_NonUnionBranchesInertInRebuild(t *testing.T) {
	t.Parallel()
	branch := SchemaNode{Type: "record", Name: "R",
		Fields: []SchemaField{{Name: "x", Type: SchemaNode{Type: "string"}}}}
	node := SchemaNode{Type: "record", Name: "Top", Fields: []SchemaField{
		{Name: "a", Type: SchemaNode{Type: "int", Props: map[string]any{"p": int64(1)}, Branches: []SchemaNode{branch}}},
		{Name: "b", Type: SchemaNode{Type: "record", Name: "R",
			Fields: []SchemaField{{Name: "x", Type: SchemaNode{Type: "int"}}}}},
	}}
	s, err := node.Schema()
	if err != nil {
		t.Fatalf("rebuild with non-union Branches: %v", err)
	}
	if strings.Contains(s.String(), `"string"`) {
		t.Errorf("non-union Branches leaked into the rebuild: %s", s.String())
	}
	bare := SchemaNode{Type: "int", Branches: []SchemaNode{branch}}
	s2, err := bare.Schema()
	if err != nil {
		t.Fatalf("bare-primitive rebuild with Branches: %v", err)
	}
	if s2.String() != `"int"` {
		t.Errorf("bare primitive with non-union Branches: got %s, want \"int\"", s2.String())
	}
}

// ---------- schema_breadth_test.go ----------

// Schema BREADTH — cost against the number of SIBLINGS a schema declares.
//
// A schema's size grows two ways: it nests deeper, or it declares more siblings
// at one level. Depth is bounded by an explicit pre-scan and pinned by the
// deep-schema cost tests. Breadth has no cap and needs none — a union of 20000
// named branches, or a record of 20000 fields, is legal Avro that a schema
// registry, an RPC handshake, or an OCF header can hand a reader. What it does
// need is for every pass over those siblings to stay LINEAR in their count,
// because a pass that scans the sibling list once per sibling turns an O(n)
// input into O(n^2) work.
//
// The bounds are absolute wall-clock, not ratios: a ratio between two sizes is
// noise-sensitive on a loaded host, while the two complexity classes these
// separate are orders of magnitude apart at the sizes driven.

// breadthN is the sibling count every cell drives. It is chosen so that a
// quadratic pass takes seconds while a linear one takes tens of milliseconds,
// leaving room for a loaded host and for the race detector's instrumentation
// without either class being mistaken for the other.
const breadthN = 20000

// breadthBound is the per-cell ceiling. wantAcceptUnder raises it under -race.
const breadthBound = 500 * time.Millisecond

// breadthParseBound is the ceiling for the two cells that parse the schema TEXT
// rather than walking an already-parsed tree.
//
// A bound only separates linear from quadratic if it sits far above the linear
// cost, and those two do not qualify at breadthBound: a 20000-branch union is
// close to a megabyte of JSON, and parsing it is ~140ms (Parse) and ~300ms
// (SchemaCache.Parse) of measured-linear work, doubling with the branch count.
// At 500ms those cells sat within 1.7x of their own linear cost, so a merely
// BUSY host crossed the line and reported a complexity change that had not
// happened. The quadratic this column exists to catch measured 1.9s to 32s at
// this size, so the ceiling still separates the two classes by more than 2x on
// each side. Every other cell walks a parsed tree in tens of milliseconds and
// keeps the tighter bound.
const breadthParseBound = 1500 * time.Millisecond

//////////////////////////////////////////////////////////////////////////////
// The entry-point axis, derived from the battery's other columns
//////////////////////////////////////////////////////////////////////////////

// The set of public entry points this column has to cover is not listed here. It
// is READ OFF the cells the rest of the battery already drives, so an entry point
// added to any other column arrives here with no breadth cell and fails, rather
// than being covered by whoever remembers to add it. batteryCellLabel matches a
// battery cell's name argument; every cell is named "<entry point>/<case>", which
// is what makes the entry-point axis recoverable from source.
var batteryCellLabel = regexp.MustCompile(`want(?:Reject|RejectIs|Terminate|BoundedErr|AcceptUnder)\(t, "([^"/]+)`)

// breadthEntryAlias folds the spellings the battery uses for one entry point
// onto a single name. A cell label names the call the cell makes, so the same
// entry point is spelled a few ways across columns.
var breadthEntryAlias = map[string]string{
	"Root.Schema": "SchemaNode.Schema",
	"Schema":      "SchemaNode.Schema",
}

// batteryEntryPoints extracts the entry points src drives, normalizing the
// compound labels (a cell that exercises two calls names both, joined by "+").
func batteryEntryPoints(src string) map[string]bool {
	out := map[string]bool{}
	for _, m := range batteryCellLabel.FindAllStringSubmatch(src, -1) {
		for _, part := range strings.Split(strings.ReplaceAll(m[1], "()", ""), "+") {
			if alias, ok := breadthEntryAlias[part]; ok {
				part = alias
			}
			out[part] = true
		}
	}
	return out
}

// breadthExempt names entry points with no sibling axis to grow, and why. An
// exemption is a claim that the entry point's cost cannot scale with a
// schema's sibling count; it is checked against the derived set, so an
// exemption for something the battery no longer drives is reported as stale.
var breadthExempt = map[string]string{
	"RatFromBytes":            "takes wire bytes and a scale, never a schema — its cost axis is byte length, not sibling count",
	"DurationFromBytes":       "takes a fixed 12-byte value; there is no sibling count to grow",
	"SingleObjectFingerprint": "hashes a fixed-width fingerprint header, independent of the schema's shape",
	"SchemaFor": "its input is a Go TYPE supplied as a compile-time type parameter, so the field count is " +
		"authored in the caller's own source rather than received at runtime; there is no runtime-supplied " +
		"sibling count to drive, and a generic type parameter cannot be built from reflect.StructOf",
}

// TestInvariant_EveryBatteryEntryPointHasABreadthCell derives the entry-point
// axis from the battery's other columns. Breadth is a property of the SCHEMA,
// so every entry point that takes one carries the axis; the exemptions are the
// entry points that take bytes instead.
func TestInvariant_EveryBatteryEntryPointHasABreadthCell(t *testing.T) {
	otherColumns := testFileSection(t, "internal_nets_test.go", "dos_battery_test.go")
	thisColumn := testFileSection(t, "schema_test.go", "schema_breadth_test.go")
	derived := batteryEntryPoints(otherColumns)
	if len(derived) == 0 {
		t.Fatal("the scan found no battery cells at all — the cell-naming convention changed, and this guard is watching nothing")
	}
	covered := batteryEntryPoints(thisColumn)

	for ep := range derived {
		if covered[ep] {
			continue
		}
		if _, ok := breadthExempt[ep]; ok {
			continue
		}
		t.Errorf("the battery drives %s, but the breadth column has no cell for it.\n"+
			"  A schema's sibling count is chosen by whoever writes the schema, so any entry point that\n"+
			"  takes a schema carries the axis. Add a cell, or add a breadthExempt entry saying which\n"+
			"  input it takes instead.", ep)
	}
	for ep := range breadthExempt {
		if !derived[ep] {
			t.Errorf("breadthExempt names %s, which the battery no longer drives — the exemption is stale", ep)
		}
	}
	t.Logf("entry points derived from the battery: %d, breadth cells cover %d, exempt %d",
		len(derived), len(covered), len(breadthExempt))
}

//////////////////////////////////////////////////////////////////////////////
// The union tag namespace — one shape per TIER
//////////////////////////////////////////////////////////////////////////////

// A union's tag tables are built by offering every branch to every tier of
// unionTagTiers. A GUARDED tier additionally has to decide whether a claim is
// ambiguous, which is the step that can go quadratic: asking "does any other
// branch claim this name" once per branch is a scan inside a loop over the same
// slice.
//
// The EMIT tables' degrade has the same shape and is bounded for a structural
// reason rather than by its own construction, so it gets no cell: it runs only
// where a branch's emitted qualifier differs from its own name, which is only
// where the branch is an UNNAMED kind carrying a logical type, and a union may
// hold at most one branch per unnamed kind — a second is refused at parse as a
// duplicate union type. The eight unnamed kinds therefore cap that scan's outer
// loop at eight regardless of how many branches the union declares.
//
// The tiers are read from unionTagTiers rather than listed, so a tier added
// there without a shape below fails
// TestInvariant_EveryUnionTagTierHasAWideShape rather than shipping undriven.
type breadthTierShape struct {
	// tier is the unionTagTiers entry this shape drives, by name.
	tier string
	// build returns a union schema of n branches that this tier claims.
	build func(n int) string
	// distinctClaims records whether the n branches produce n DISTINCT claims
	// under this tier. It decides which cost the shape can observe: identical
	// claims let the ambiguity check stop at the first match, so only a shape
	// with distinct claims forces the full scan. Both are driven — the
	// identical-claim shape is what observes an ambiguity check that stopped
	// short-circuiting.
	distinctClaims bool
}

var breadthTierShapes = []breadthTierShape{
	{
		// Every branch is a named record in one namespace, so each branch's
		// own fullname is its claim and no two collide.
		tier:           "exact name",
		distinctClaims: true,
		build: func(n int) string {
			var sb strings.Builder
			sb.WriteString(`["null"`)
			for i := range n {
				fmt.Fprintf(&sb, `,{"type":"record","name":"a.R%d","fields":[]}`, i)
			}
			sb.WriteString(`]`)
			return sb.String()
		},
	},
	{
		// The qualifier tier claims "<kind>.<logicalType>", so its claim does
		// not carry the branch's name. A named fixed is the only shape that
		// can repeat it, and every such branch claims the SAME "fixed.uuid" —
		// the claim vocabulary is the (kind, logicalType) pairs, which is
		// fixed-size, so this tier cannot produce n distinct claims at all.
		// Driving it wide is still what observes an ambiguity check that
		// stopped stopping at the first match.
		tier:           "logical qualifier",
		distinctClaims: false,
		build: func(n int) string {
			var sb strings.Builder
			sb.WriteString(`["null"`)
			for i := range n {
				fmt.Fprintf(&sb, `,{"type":"fixed","name":"a.F%d","size":16,"logicalType":"uuid"}`, i)
			}
			sb.WriteString(`]`)
			return sb.String()
		},
	},
	{
		// Every branch is a namespaced named record, so each claims its own
		// unqualified short name and all n claims are distinct. This is the
		// shape that forces a per-branch ambiguity scan to walk the whole
		// sibling list every time.
		tier:           "unqualified short name",
		distinctClaims: true,
		build: func(n int) string {
			var sb strings.Builder
			sb.WriteString(`["null"`)
			for i := range n {
				fmt.Fprintf(&sb, `,{"type":"record","name":"ns%d.Short%d","fields":[]}`, i, i)
			}
			sb.WriteString(`]`)
			return sb.String()
		},
	},
}

func breadthTierShapeFor(tier string) (breadthTierShape, bool) {
	for _, s := range breadthTierShapes {
		if s.tier == tier {
			return s, true
		}
	}
	return breadthTierShape{}, false
}

// TestInvariant_EveryUnionTagTierHasAWideShape derives the tier set from
// unionTagTiers rather than restating it. A tier added there is a new
// per-branch claim, and if it is guarded it brings a new ambiguity scan with
// it; without a wide shape that tier's cost is never measured.
func TestInvariant_EveryUnionTagTierHasAWideShape(t *testing.T) {
	if len(unionTagTiers) == 0 {
		t.Fatal("unionTagTiers is empty — the tier set moved or was renamed, and this guard is watching nothing")
	}
	for _, tier := range unionTagTiers {
		if _, ok := breadthTierShapeFor(tier.name); !ok {
			t.Errorf("unionTagTiers contains tier %q, but no shape in breadthTierShapes drives it wide.\n"+
				"  Every tier is offered every branch, so a tier is a per-branch cost; a guarded one also\n"+
				"  brings an ambiguity check. Add a shape whose branches this tier claims.", tier.name)
		}
	}
	for _, s := range breadthTierShapes {
		if !slices.ContainsFunc(unionTagTiers, func(tr unionTagTier) bool { return tr.name == s.tier }) {
			t.Errorf("breadthTierShapes drives tier %q, which unionTagTiers no longer contains — the shape is stale", s.tier)
		}
	}
}

// TestRegression_UnionTagTierShapesReachTheirTier proves each shape actually
// makes its tier claim its branches. A cost cell whose input never reaches the
// pass it is timing measures nothing, and would stay green through any change
// to that pass.
func TestRegression_UnionTagTierShapesReachTheirTier(t *testing.T) {
	const n = 8
	for _, s := range breadthTierShapes {
		tierIdx := slices.IndexFunc(unionTagTiers, func(tr unionTagTier) bool { return tr.name == s.tier })
		if tierIdx < 0 {
			continue // reported by the invariant above
		}
		tier := unionTagTiers[tierIdx]

		sc, err := Parse(`{"type":"record","name":"Top","fields":[{"name":"f","type":` + s.build(n) + `}]}`)
		if err != nil {
			t.Errorf("tier %q: shape does not parse: %v", s.tier, err)
			continue
		}
		branches := sc.node.fields[0].node.branches
		claims := map[string]int{}
		claimed := 0
		for _, b := range branches {
			c, ok := tierClaim(tier, b)
			if !ok {
				continue
			}
			claimed++
			claims[c]++
		}
		// "null" is present in every shape and is claimed only by the exact
		// name tier, so the count is n or n+1 depending on the tier.
		if claimed < n {
			t.Errorf("tier %q: only %d of %d branches are claimed by this tier — the shape does not reach it",
				s.tier, claimed, n)
		}
		if got := len(claims) >= n; got != s.distinctClaims {
			t.Errorf("tier %q: distinctClaims=%v but the shape produced %d distinct claims over %d branches",
				s.tier, s.distinctClaims, len(claims), claimed)
		}
	}
}

// TestDoSBattery_C10a_UnionTagBreadth drives every tier's wide shape through
// every parse entry point. The tag tables are built during the parse, so the
// parse time is the observable.
func TestDoSBattery_C10a_UnionTagBreadth(t *testing.T) {
	for _, s := range breadthTierShapes {
		union := s.build(breadthN)
		schema := `{"type":"record","name":"Top","fields":[{"name":"f","type":` + union + `}]}`

		wantAcceptUnder(t, "Parse/wide-union-"+s.tier, breadthParseBound, func() error {
			_, err := Parse(schema)
			return err
		})
		wantAcceptUnder(t, "SchemaCache.Parse/wide-union-"+s.tier, breadthParseBound, func() error {
			var c SchemaCache
			_, err := c.Parse(schema)
			return err
		})
		// A forward-referenced branch leaves buildUnion with an unbound node,
		// so finalizeUnionNames rebuilds the tables over the resolved nodes —
		// a SECOND full build of the same tables, through the same tiers.
		fwd := `{"type":"record","name":"Top","fields":[` +
			`{"name":"a","type":"a.Fwd"},` +
			`{"name":"f","type":` + union[:len(union)-1] + `,{"type":"record","name":"a.Fwd","fields":[]}]}]}`
		wantAcceptUnder(t, "Parse/wide-union-forward-ref-"+s.tier, breadthParseBound, func() error {
			_, err := Parse(fwd)
			return err
		})
	}
}

//////////////////////////////////////////////////////////////////////////////
// Record field resolution — one shape per LOOKUP PATH
//////////////////////////////////////////////////////////////////////////////

// Matching a writer's record to a reader's is a per-writer-field lookup into
// the reader's fields. The lookup has three outcomes, and they cost
// differently: a name hit can stop early, an alias hit only after the whole
// name pass misses, and a miss pays for both passes in full.
type breadthFieldShape struct {
	name           string
	writer, reader func(n int) string
}

var breadthFieldShapes = []breadthFieldShape{
	{
		// Every writer field hits a reader field by NAME. The reader carries
		// one extra defaulted field so the two schemas are not canonically
		// equal: Resolve returns the reader untouched when they are, which
		// would skip the per-field matching entirely.
		name:   "name-hit",
		writer: breadthLongFields("f", nil, false),
		reader: breadthLongFieldsPlusExtra("f", nil, false),
	},
	{
		// Reader field i answers to alias f<i>; the writer names f<i>. Every
		// lookup misses the whole name pass before the alias pass finds it.
		name:   "alias-hit",
		writer: breadthLongFields("f", nil, false),
		reader: breadthLongFields("g", func(i int) string { return fmt.Sprintf("f%d", i) }, false),
	},
	{
		// No writer field name or alias appears in the reader, so every
		// lookup walks both passes to the end. The writer's fields are
		// skipped and the reader's are defaulted, so this resolves — the cost
		// of the two exhausted passes is what the cell observes.
		name:   "miss",
		writer: breadthLongFields("w", nil, false),
		reader: breadthLongFields("r", nil, true),
	},
}

// breadthLongFieldsPlusExtra is breadthLongFields with one additional
// defaulted field appended, so the record is compatible with the plain form
// but not canonically equal to it.
func breadthLongFieldsPlusExtra(prefix string, alias func(int) string, withDefault bool) func(n int) string {
	base := breadthLongFields(prefix, alias, withDefault)
	return func(n int) string {
		s := base(n)
		return s[:len(s)-len(`]}`)] + `,{"name":"zzExtra","type":"long","default":0}]}`
	}
}

// breadthLongFields builds a record of n long fields named <prefix><i>, each
// carrying the alias alias(i) when alias is non-nil and a default when
// withDefault is set.
func breadthLongFields(prefix string, alias func(int) string, withDefault bool) func(n int) string {
	return func(n int) string {
		var sb strings.Builder
		sb.WriteString(`{"type":"record","name":"Top","fields":[`)
		for i := range n {
			if i > 0 {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, `{"name":"%s%d"`, prefix, i)
			if alias != nil {
				fmt.Fprintf(&sb, `,"aliases":["%s"]`, alias(i))
			}
			sb.WriteString(`,"type":"long"`)
			if withDefault {
				sb.WriteString(`,"default":0`)
			}
			sb.WriteString(`}`)
		}
		sb.WriteString(`]}`)
		return sb.String()
	}
}

// enclosingFuncsCalling scans Go source for lines invoking ident and returns
// the names of the functions those lines sit in.
func enclosingFuncsCalling(src, ident string) []string {
	var out []string
	fn := ""
	funcRe := regexp.MustCompile(`^func (?:\([^)]*\) )?([A-Za-z_][A-Za-z0-9_]*)\(`)
	for line := range strings.SplitSeq(src, "\n") {
		if m := funcRe.FindStringSubmatch(line); m != nil {
			fn = m[1]
			continue
		}
		trimmed := strings.TrimSpace(line)
		if strings.Contains(line, ident+"(") && !strings.HasPrefix(trimmed, "//") &&
			!strings.HasPrefix(line, "func ") && fn != "" && !slices.Contains(out, fn) {
			out = append(out, fn)
		}
	}
	return out
}

// breadthFieldLookupEntryPoints maps each site that builds a reader-field
// lookup to the public entry point that reaches it. Building the lookup is
// what costs O(fields); the queries against it are constant. The KEYS are
// checked against the builders found in source, so a third builder added later
// has no entry point here and fails rather than shipping unmeasured.
var breadthFieldLookupEntryPoints = map[string]string{
	"resolveRecord":                "Resolve",
	"checkRecordFieldClaimsUnique": "CheckCompatibility",
}

// TestInvariant_EveryFieldLookupBuilderHasABreadthCell derives the build sites
// from source. A record's field count is set by the schema text, so every site
// that walks a reader's fields to match a writer's carries a breadth cost; a
// site with no cell is an unmeasured pass.
func TestInvariant_EveryFieldLookupBuilderHasABreadthCell(t *testing.T) {
	var builders []string
	for _, f := range []string{"resolve.go", "compat.go"} {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		builders = append(builders, enclosingFuncsCalling(string(src), "newReaderFieldLookup")...)
	}
	if len(builders) == 0 {
		t.Fatal("the scan found no builders of readerFieldLookup — the lookup moved or was renamed, and this guard is watching nothing")
	}
	for _, c := range builders {
		if _, ok := breadthFieldLookupEntryPoints[c]; !ok {
			t.Errorf("%s builds a readerFieldLookup but has no entry point in breadthFieldLookupEntryPoints.\n"+
				"  Building one walks every reader field, so this site carries the same breadth cost as the\n"+
				"  others. Name the public entry point that reaches it and give it a cell.", c)
		}
	}
	for c := range breadthFieldLookupEntryPoints {
		if !slices.Contains(builders, c) {
			t.Errorf("breadthFieldLookupEntryPoints names %s, which no longer builds a readerFieldLookup — the cell is stale", c)
		}
	}
	t.Logf("field-lookup builders derived from source: %v", builders)
}

// TestMatrix_ReaderFieldLookupPrefersNamesOverAliases pins the routing the
// lookup's two maps exist to preserve: a writer name that is one reader
// field's ALIAS and a different reader field's NAME resolves to the NAME. A
// single merged map resolves it to whichever entry was written last, which is
// a silent reversal — the writer's data lands in the wrong reader field.
func TestMatrix_ReaderFieldLookupPrefersNamesOverAliases(t *testing.T) {
	// Parse refuses a record whose field name collides with another field's
	// alias, so the deciding shape cannot be reached through Parse. It is
	// built directly: the ordering is the routing that the parse-time
	// rejection is justified by, and it has to hold on its own terms rather
	// than only because something upstream refuses the input.
	long := &schemaNode{kind: "long"}
	rec := func(fields ...fieldNode) *schemaNode {
		return &schemaNode{kind: "record", fields: fields}
	}
	for _, tc := range []struct {
		name  string
		node  *schemaNode
		query string
		want  int
	}{
		{
			// Field 0 is ALIASED "x"; field 1 is NAMED "x". The name wins
			// even though the alias appears first. This is the cell a merged
			// map gets wrong: inserting name-then-aliases per field in field
			// order writes the alias entry first, and first-write-wins then
			// routes "x" to field 0.
			name:  "alias-before-name",
			node:  rec(fieldNode{name: "a", aliases: []string{"x"}, node: long}, fieldNode{name: "x", node: long}),
			query: "x",
			want:  1,
		},
		{
			name:  "name-before-alias",
			node:  rec(fieldNode{name: "x", node: long}, fieldNode{name: "a", aliases: []string{"x"}, node: long}),
			query: "x",
			want:  0,
		},
		{
			name:  "alias-only",
			node:  rec(fieldNode{name: "a", node: long}, fieldNode{name: "b", aliases: []string{"x"}, node: long}),
			query: "x",
			want:  1,
		},
		{
			name:  "no-match",
			node:  rec(fieldNode{name: "a", node: long}, fieldNode{name: "b", node: long}),
			query: "x",
			want:  -1,
		},
	} {
		lk := newReaderFieldLookup(tc.node)
		if got := lk.index(tc.query); got != tc.want {
			t.Errorf("%s: writer field %q resolved to reader field %d, want %d", tc.name, tc.query, got, tc.want)
		}
	}
}

// TestRegression_BreadthFieldShapesReachTheResolvedPath proves every field
// shape actually reaches the per-field matching. Resolve returns the reader
// untouched when writer and reader are canonically equal, so a shape built
// from one schema text times the equality check and nothing else — a cell that
// stays green through any change to the matching it claims to measure.
func TestRegression_BreadthFieldShapesReachTheResolvedPath(t *testing.T) {
	const n = 4
	for _, s := range breadthFieldShapes {
		w, err := Parse(s.writer(n))
		if err != nil {
			t.Errorf("%s writer: %v", s.name, err)
			continue
		}
		r, err := Parse(s.reader(n))
		if err != nil {
			t.Errorf("%s reader: %v", s.name, err)
			continue
		}
		if bytes.Equal(w.Canonical(), r.Canonical()) {
			t.Errorf("%s: writer and reader are canonically equal, so Resolve short-circuits "+
				"before any per-field matching — this shape measures the equality check, not the lookup", s.name)
		}
	}
}

// TestDoSBattery_C10b_FieldLookupBreadth drives every lookup path through
// every entry point that reaches the lookup.
func TestDoSBattery_C10b_FieldLookupBreadth(t *testing.T) {
	for _, s := range breadthFieldShapes {
		w, err := Parse(s.writer(breadthN))
		if err != nil {
			t.Fatalf("%s writer: %v", s.name, err)
		}
		r, err := Parse(s.reader(breadthN))
		if err != nil {
			t.Fatalf("%s reader: %v", s.name, err)
		}
		wantAcceptUnder(t, "Resolve/wide-record-"+s.name, breadthBound, func() error {
			_, err := Resolve(w, r)
			return err
		})
		wantAcceptUnder(t, "CheckCompatibility/wide-record-"+s.name, breadthBound, func() error {
			// A reader field the writer lacks needs a default, so the miss
			// shape is legitimately incompatible; the cost is the question
			// here, not the verdict.
			CheckCompatibility(w, r)
			return nil
		})
	}
}

//////////////////////////////////////////////////////////////////////////////
// The rest of the entry points — one wide RECORD, every surface
//////////////////////////////////////////////////////////////////////////////

// TestDoSBattery_C10c_WideRecordSurfaces drives a record of breadthN fields
// through every remaining entry point the battery covers: the two wire
// directions, their JSON and single-object forms, and the schema surfaces that
// walk or re-emit the tree. A record's field count is chosen by whoever writes
// the schema, so each of these passes over the field list once per call and
// must stay linear in it.
func TestDoSBattery_C10c_WideRecordSurfaces(t *testing.T) {
	text := breadthLongFields("f", nil, false)(breadthN)
	s := mustParse(t, text)
	val := make(map[string]any, breadthN)
	for i := range breadthN {
		val[fmt.Sprintf("f%d", i)] = int64(i)
	}
	wire := mustEncode(t, s, val)
	jsonWire := mustEncodeJSON(t, s, val)
	soe := mustAppendSingleObject(t, s, nil, val)

	wantAcceptUnder(t, "Encode/wide-record", breadthBound, func() error {
		_, err := s.Encode(val)
		return err
	})
	wantAcceptUnder(t, "Decode/wide-record", breadthBound, func() error {
		var out map[string]any
		_, err := s.Decode(wire, &out)
		return err
	})
	wantAcceptUnder(t, "EncodeJSON/wide-record", breadthBound, func() error {
		_, err := s.EncodeJSON(val)
		return err
	})
	wantAcceptUnder(t, "DecodeJSON/wide-record", breadthBound, func() error {
		var out map[string]any
		return s.DecodeJSON(jsonWire, &out)
	})
	wantAcceptUnder(t, "AppendSingleObject/wide-record", breadthBound, func() error {
		_, err := s.AppendSingleObject(nil, val)
		return err
	})
	wantAcceptUnder(t, "DecodeSingleObject/wide-record", breadthBound, func() error {
		var out map[string]any
		_, err := s.DecodeSingleObject(soe, &out)
		return err
	})
	wantAcceptUnder(t, "Canonical/wide-record", breadthBound, func() error {
		if len(s.Canonical()) == 0 {
			return fmt.Errorf("empty canonical form")
		}
		return nil
	})
	wantAcceptUnder(t, "Fingerprint/wide-record", breadthBound, func() error {
		if len(s.Fingerprint(crypto.SHA256.New())) == 0 {
			return fmt.Errorf("empty fingerprint")
		}
		return nil
	})
	wantAcceptUnder(t, "String/wide-record", breadthBound, func() error {
		if len(s.String()) == 0 {
			return fmt.Errorf("empty string form")
		}
		return nil
	})
	wantAcceptUnder(t, "Root/wide-record", breadthBound, func() error {
		if len(s.Root().Fields) != breadthN {
			return fmt.Errorf("Root surfaced %d fields, want %d", len(s.Root().Fields), breadthN)
		}
		return nil
	})
	root := s.Root()
	wantAcceptUnder(t, "SchemaNode.Schema/wide-record", breadthBound, func() error {
		_, err := root.Schema()
		return err
	})
}

//////////////////////////////////////////////////////////////////////////////
// The sibling-KIND axis, derived from schemaNode's own slice fields
//////////////////////////////////////////////////////////////////////////////

// A schema declares siblings in more than one place. The column above drives
// exactly one of them — a record's fields — through every entry point, and a
// union's branches through Parse alone. That left the SHAPE hand-picked per
// cell, and a shape nobody picked is a shape nobody bounded: the union and enum
// containers carried per-value passes over their siblings on the JSON side for
// as long as the column existed.
//
// So the shape axis is derived too. Every schemaNode field whose length is set
// by the schema TEXT is a sibling kind, read out of the struct by reflection,
// and each must be driven or exempted with the reason it cannot grow.
//
// The second half is the VALUE count. A cell that encodes ONE value against a
// wide schema cannot see a pass that runs once per value — exactly the class the
// union tag and enum symbol lookups were in. Where a single value's own size is
// independent of the sibling count, the cells drive many values and place the
// answer LAST, because a table cannot tell first from last and a scan takes the
// whole list to reach the end.

// breadthValueN is the value count the per-value cells drive. Chosen with
// breadthN so a per-value scan of the siblings is seconds of work while a table
// lookup stays in the milliseconds.
const breadthValueN = 2000

// breadthSiblingKind is one sibling-bearing schemaNode field and the schemas
// that grow it.
type breadthSiblingKind struct {
	// field is the schemaNode field this kind grows, and is what ties the
	// table to the reflected set.
	field string
	// schema declares n siblings of this kind; twin resolves against it
	// without being canonically equal to it, so the resolve cells time the
	// matching rather than the equality short-circuit.
	schema func(n int) string
	twin   func(n int) string
	// value is a datum for schema(n). perValue says whether a single value's
	// own size is independent of the sibling count: when it is, the cells
	// drive breadthValueN of them, because that is where a once-per-value pass
	// over the siblings shows up. When it is not — a record value carries one
	// entry per field — driving many values would only be timing the values.
	value    func(n int) any
	perValue bool
}

func breadthAliasList(n int, qualified bool) string {
	var sb strings.Builder
	for i := range n {
		if i > 0 {
			sb.WriteByte(',')
		}
		if qualified {
			fmt.Fprintf(&sb, `"ns%d.A%d"`, i, i)
		} else {
			fmt.Fprintf(&sb, `"A%d"`, i)
		}
	}
	return sb.String()
}

// breadthAliasedRecord wraps the aliased record in an array so a cell can drive
// many values through it: the alias list's length is independent of a value's
// size, so a per-value pass over it is exactly the shape worth bounding.
func breadthAliasedRecord(n int, qualified bool, fieldType string) string {
	return fmt.Sprintf(`{"type":"array","items":{"type":"record","name":"x.R","aliases":[%s],"fields":[{"name":"f","type":%q}]}}`,
		breadthAliasList(n, qualified), fieldType)
}

// breadthWideUnionArray wraps a union of n named records in an array so a cell
// can drive many values through one schema. The LAST branch is the one the
// values name.
func breadthWideUnionArray(ns string, n int) string {
	var sb strings.Builder
	sb.WriteString(`{"type":"array","items":["null"`)
	for i := range n {
		fmt.Fprintf(&sb, `,{"type":"record","name":"%s.R%d","fields":[]}`, ns, i)
	}
	sb.WriteString(`]}`)
	return sb.String()
}

func breadthWideEnumArray(n int, defaulted bool) string {
	var sb strings.Builder
	sb.WriteString(`{"type":"array","items":{"type":"enum","name":"E","symbols":[`)
	for i := range n {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `"S%d"`, i)
	}
	sb.WriteString(`]`)
	if defaulted {
		sb.WriteString(`,"default":"S0"`)
	}
	sb.WriteString(`}}`)
	return sb.String()
}

// breadthSiblingKinds is the table the reflected field set is checked against.
var breadthSiblingKinds = []breadthSiblingKind{
	{
		field:  "fields",
		schema: breadthLongFields("f", nil, false),
		// int fields promote into the long ones, so the pair resolves and the
		// canonical forms differ.
		twin: func(n int) string {
			var sb strings.Builder
			sb.WriteString(`{"type":"record","name":"Top","fields":[`)
			for i := range n {
				if i > 0 {
					sb.WriteByte(',')
				}
				fmt.Fprintf(&sb, `{"name":"f%d","type":"int"}`, i)
			}
			sb.WriteString(`]}`)
			return sb.String()
		},
		value: func(n int) any {
			m := make(map[string]any, n)
			for i := range n {
				m[fmt.Sprintf("f%d", i)] = int64(i)
			}
			return m
		},
		perValue: false,
	},
	{
		field:  "branches",
		schema: func(n int) string { return breadthWideUnionArray("a", n) },
		// A different namespace, so the branches match on the unqualified
		// short name rather than short-circuiting on canonical equality.
		twin: func(n int) string { return breadthWideUnionArray("b", n) },
		value: func(n int) any {
			// The LAST branch: a scan has to walk every earlier one to reach it.
			tag := fmt.Sprintf("a.R%d", n-1)
			vals := make([]any, breadthValueN)
			for i := range vals {
				vals[i] = map[string]any{tag: map[string]any{}}
			}
			return vals
		},
		perValue: true,
	},
	{
		field:  "symbols",
		schema: func(n int) string { return breadthWideEnumArray(n, false) },
		// One fewer symbol on the writer side, all of them present in the
		// reader, so the pair resolves and is not canonically equal.
		twin: func(n int) string { return breadthWideEnumArray(n-1, false) },
		value: func(n int) any {
			last := fmt.Sprintf("S%d", n-1)
			vals := make([]string, breadthValueN)
			for i := range vals {
				vals[i] = last
			}
			return vals
		},
		perValue: true,
	},
	{
		field:  "aliases",
		schema: func(n int) string { return breadthAliasedRecord(n, true, "long") },
		twin:   func(n int) string { return breadthAliasedRecord(n, true, "int") },
		value: func(n int) any {
			vals := make([]any, breadthValueN)
			for i := range vals {
				vals[i] = map[string]any{"f": int64(i)}
			}
			return vals
		},
		perValue: true,
	},
	{
		field: "bareAliases",
		// An alias declared WITHOUT a dot lands in bareAliases as well, which
		// is the slice the short-name match tier reads.
		schema: func(n int) string { return breadthAliasedRecord(n, false, "long") },
		twin:   func(n int) string { return breadthAliasedRecord(n, false, "int") },
		value: func(n int) any {
			vals := make([]any, breadthValueN)
			for i := range vals {
				vals[i] = map[string]any{"f": int64(i)}
			}
			return vals
		},
		perValue: true,
	},
}

// breadthSiblingExempt names schemaNode slice fields with no schema-text length,
// and why. An exemption is a claim that the field cannot grow with what a caller
// writes; it is empty today, and that is the honest state rather than an
// oversight, since every slice schemaNode carries is filled from something the
// schema declares. The map stays because the next slice field added may not be,
// and an exemption without a reason is how a cell goes missing quietly. Slices on
// the SERIALIZERS are not schemaNode fields, so the reflection does not read them.
var breadthSiblingExempt = map[string]string{}

// breadthSiblingFieldSet reads every slice-valued schemaNode field out of the
// struct. Slice-valued is the mechanical form of "its length comes from the
// schema text": every one of them is filled by the builder from something the
// schema declares.
func breadthSiblingFieldSet() []string {
	rt := reflect.TypeFor[schemaNode]()
	var out []string
	for i := range rt.NumField() {
		if f := rt.Field(i); f.Type.Kind() == reflect.Slice {
			out = append(out, f.Name)
		}
	}
	return out
}

// TestInvariant_EveryBreadthSiblingKindIsCelled derives the sibling-kind axis
// from schemaNode instead of listing it. Adding a slice field to schemaNode
// declares a new way for a caller to make a schema wide; this fails until that
// way is either driven or exempted with the reason it cannot grow.
func TestInvariant_EveryBreadthSiblingKindIsCelled(t *testing.T) {
	derived := breadthSiblingFieldSet()
	if len(derived) == 0 {
		t.Fatal("the reflection found no slice fields on schemaNode at all — this guard is watching nothing")
	}
	celled := map[string]bool{}
	for _, k := range breadthSiblingKinds {
		if celled[k.field] {
			t.Errorf("two breadthSiblingKinds entries both drive %s", k.field)
		}
		celled[k.field] = true
	}
	for _, field := range derived {
		if celled[field] || breadthSiblingExempt[field] != "" {
			continue
		}
		t.Errorf("schemaNode.%s is a slice whose length comes from the schema text, and no breadth cell drives it.\n"+
			"  A caller chooses how many of these a schema declares, so every pass over them has to stay linear.\n"+
			"  Add a breadthSiblingKinds entry, or a breadthSiblingExempt entry saying why the length cannot grow.", field)
	}
	for field := range celled {
		if !slices.Contains(derived, field) {
			t.Errorf("breadthSiblingKinds drives %q, which is not a slice field on schemaNode — the cell is watching a field that moved or was renamed", field)
		}
	}
	for field := range breadthSiblingExempt {
		if !slices.Contains(derived, field) {
			t.Errorf("breadthSiblingExempt names %q, which schemaNode no longer has — the exemption is stale", field)
		}
	}
	t.Logf("schemaNode sibling slices: %d, celled %d, exempt %d", len(derived), len(celled), len(breadthSiblingExempt))
}

// TestDoSBattery_C10d_SiblingKindSurfaces crosses every entry point with every
// sibling kind. The record column above is one row of this cross; the rows that
// did not exist are where both of the per-value lookups hid.
func TestDoSBattery_C10d_SiblingKindSurfaces(t *testing.T) {
	for _, kind := range breadthSiblingKinds {
		t.Run(kind.field, func(t *testing.T) {
			text := kind.schema(breadthN)
			s, err := Parse(text)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			twinText := kind.twin(breadthN)
			twin, err := Parse(twinText)
			if err != nil {
				t.Fatalf("parse twin: %v", err)
			}
			// A twin canonically equal to the schema takes Resolve's equality
			// short-circuit, and the resolve cells would time that instead of
			// the sibling matching they exist to bound.
			if string(s.Canonical()) == string(twin.Canonical()) {
				t.Fatal("the twin is canonically equal to the schema, so the resolve cells would time the equality short-circuit rather than the match")
			}
			val := kind.value(breadthN)
			wire, err := s.Encode(val)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			jsonWire, err := s.EncodeJSON(val)
			if err != nil {
				t.Fatalf("encode json: %v", err)
			}
			soe, err := s.AppendSingleObject(nil, val)
			if err != nil {
				t.Fatalf("single object: %v", err)
			}

			label := func(entry string) string { return entry + "/wide-" + kind.field }
			wantAcceptUnder(t, label("Parse"), breadthParseBound, func() error {
				_, err := Parse(text)
				return err
			})
			wantAcceptUnder(t, label("SchemaCache.Parse"), breadthParseBound, func() error {
				var c SchemaCache
				_, err := c.Parse(text)
				return err
			})
			wantAcceptUnder(t, label("Resolve"), breadthBound, func() error {
				_, err := Resolve(twin, s)
				return err
			})
			wantAcceptUnder(t, label("CheckCompatibility"), breadthBound, func() error {
				return CheckCompatibility(twin, s)
			})
			wantAcceptUnder(t, label("Encode"), breadthBound, func() error {
				_, err := s.Encode(val)
				return err
			})
			wantAcceptUnder(t, label("Decode"), breadthBound, func() error {
				var out any
				_, err := s.Decode(wire, &out)
				return err
			})
			wantAcceptUnder(t, label("EncodeJSON"), breadthBound, func() error {
				_, err := s.EncodeJSON(val)
				return err
			})
			wantAcceptUnder(t, label("DecodeJSON"), breadthBound, func() error {
				var out any
				return s.DecodeJSON(jsonWire, &out)
			})
			// The tagged form routes every value through the union tag table
			// rather than through try-each, which is the consumer the bare
			// form never reaches.
			wantAcceptUnder(t, label("DecodeJSON+TaggedUnions"), breadthBound, func() error {
				var out any
				return s.DecodeJSON(jsonWire, &out, TaggedUnions())
			})
			wantAcceptUnder(t, label("AppendSingleObject"), breadthBound, func() error {
				_, err := s.AppendSingleObject(nil, val)
				return err
			})
			wantAcceptUnder(t, label("DecodeSingleObject"), breadthBound, func() error {
				var out any
				_, err := s.DecodeSingleObject(soe, &out)
				return err
			})
			wantAcceptUnder(t, label("Canonical"), breadthBound, func() error {
				if len(s.Canonical()) == 0 {
					return fmt.Errorf("empty canonical form")
				}
				return nil
			})
			wantAcceptUnder(t, label("Fingerprint"), breadthBound, func() error {
				if len(s.Fingerprint(crypto.SHA256.New())) == 0 {
					return fmt.Errorf("empty fingerprint")
				}
				return nil
			})
			wantAcceptUnder(t, label("String"), breadthBound, func() error {
				if len(s.String()) == 0 {
					return fmt.Errorf("empty string form")
				}
				return nil
			})
			var root SchemaNode
			wantAcceptUnder(t, label("Root"), breadthBound, func() error {
				root = *s.Root()
				return nil
			})
			wantAcceptUnder(t, label("SchemaNode.Schema"), breadthBound, func() error {
				_, err := root.Schema()
				return err
			})
		})
	}
}

// ---------- schema_dag_cost_test.go ----------

// Schema DAG cost — what a schema walk costs when the SAME node is reachable by
// more than one path.
//
// A named type referenced twice is not a tree, it is a DAG: both references bind
// to ONE *schemaNode, so a walk that re-descends per reference does 2^depth work
// on a schema whose text grows linearly. Nothing about that needs deep nesting —
// the same fan-out is expressible with every level declared as a sibling field
// wired by forward reference — so a nesting-depth bound cannot stand in for the
// memo.
//
// The walk this file guards, schemaMinBytes, is reached ONLY where a container
// asks for a per-element wire minimum, so a cell that parses a bare record DAG
// never enters it and would measure nothing. Every cost cell carries its trigger
// explicitly, and TestInvariant_MinBytesCallSites derives the trigger set from
// source rather than trusting this list.

// ---------- schema builders ----------

// dagNested builds a chain of `levels` records where every level has `fan`
// fields of the NEXT level: the first defines it inline, the rest reference
// it by name. Both spellings bind to one node, so the type graph is a DAG
// with 2^levels distinct root-to-leaf paths.
func dagNested(levels, fan int) string {
	inner := `"int"`
	for i := levels - 1; i >= 0; i-- {
		next := fmt.Sprintf("L%d", i+1)
		if i == levels-1 {
			next = "int"
		}
		var b strings.Builder
		fmt.Fprintf(&b, `{"type":"record","name":"L%d","fields":[{"name":"f0","type":%s}`, i, inner)
		for k := 1; k < fan; k++ {
			fmt.Fprintf(&b, `,{"name":"f%d","type":"%s"}`, k, next)
		}
		b.WriteString(`]}`)
		inner = b.String()
	}
	return inner
}

// dagFlat expresses the identical type graph with a JSON nesting depth of 4
// regardless of levels: every level is a sibling field of one record, and the
// references between them are forward references. A bracket pre-scan or any
// other depth bound sees nothing here.
func dagFlat(levels, fan int) string {
	var b strings.Builder
	b.WriteString(`{"type":"record","name":"Root","fields":[`)
	b.WriteString(`{"name":"z","type":{"type":"array","items":"L0"}}`)
	for i := range levels {
		next := fmt.Sprintf("L%d", i+1)
		if i == levels-1 {
			next = "int"
		}
		fmt.Fprintf(&b, `,{"name":"d%d","type":{"type":"record","name":"L%d","fields":[{"name":"f0","type":"%s"}`, i, i, next)
		for k := 1; k < fan; k++ {
			fmt.Fprintf(&b, `,{"name":"f%d","type":"%s"}`, k, next)
		}
		b.WriteString(`]}}`)
	}
	b.WriteString(`]}`)
	return b.String()
}

// dagSelfRecursive is dagNested with every level additionally referencing
// ITSELF through a nullable union. Every node then sits on a cycle of its
// own, so a memo that refuses to record any result reached through a
// back-edge records nothing here and the fan-out is untouched.
func dagSelfRecursive(levels, fan int) string {
	inner := `"int"`
	for i := levels - 1; i >= 0; i-- {
		next := fmt.Sprintf("L%d", i+1)
		if i == levels-1 {
			next = "int"
		}
		var b strings.Builder
		fmt.Fprintf(&b, `{"type":"record","name":"L%d","fields":[{"name":"f0","type":%s}`, i, inner)
		for k := 1; k < fan; k++ {
			fmt.Fprintf(&b, `,{"name":"f%d","type":"%s"}`, k, next)
		}
		fmt.Fprintf(&b, `,{"name":"self","type":["null","L%d"]}`, i)
		b.WriteString(`]}`)
		inner = b.String()
	}
	return inner
}

// dagSingleSCC wires the DEEPEST level back to the SHALLOWEST, so every
// level belongs to one strongly-connected component. This is the shape a
// memo keyed on "was this subtree cycle-free" cannot help at all, and it is
// the reason the walk carries a visit budget as well as a memo.
func dagSingleSCC(levels, fan int) string {
	var b strings.Builder
	b.WriteString(`{"type":"record","name":"Root","fields":[`)
	b.WriteString(`{"name":"z","type":{"type":"array","items":"L0"}}`)
	for i := range levels {
		next := fmt.Sprintf("L%d", i+1)
		if i == levels-1 {
			next = "L0" // close the cycle
		}
		fmt.Fprintf(&b, `,{"name":"d%d","type":{"type":"record","name":"L%d","fields":[{"name":"f0","type":["null","%s"]}`, i, i, next)
		for k := 1; k < fan; k++ {
			fmt.Fprintf(&b, `,{"name":"f%d","type":["null","%s"]}`, k, next)
		}
		b.WriteString(`]}}`)
	}
	b.WriteString(`]}`)
	return b.String()
}

// treeExpanded is the same type as dagNested with NO sharing: every
// occurrence is written out in full, so the schema text is exponential and
// the node graph is a tree. It exists to be the value oracle — sharing a
// node must not change what the walk computes.
func treeExpanded(levels, fan int) string {
	var build func(i int) string
	build = func(i int) string {
		if i == levels {
			return `"int"`
		}
		var b strings.Builder
		b.WriteString(`{"type":"record","fields":[`)
		for k := range fan {
			if k > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `{"name":"f%d","type":%s}`, k, build(i+1))
		}
		b.WriteString(`]}`)
		return b.String()
	}
	// Records need names, and every occurrence needs a DISTINCT one or the
	// parser rebinds them into the very sharing this oracle removes.
	out := build(0)
	n := 0
	for strings.Contains(out, `{"type":"record","fields":`) {
		out = strings.Replace(out, `{"type":"record","fields":`,
			fmt.Sprintf(`{"type":"record","name":"T%d","fields":`, n), 1)
		n++
	}
	return out
}

// ---------- triggers ----------

// minBytesTrigger names a container that demands a per-element wire minimum,
// which is the only way schemaMinBytes is entered. "none" is the control: it
// is a schema shape that reaches no caller, so a cell carrying it measures
// nothing, and the matrix asserts exactly that rather than leaving it to be
// rediscovered.
type minBytesTrigger struct {
	name  string
	wrap  func(inner string) string
	walks bool
}

var minBytesTriggers = []minBytesTrigger{
	{name: "array-items", walks: true, wrap: func(s string) string {
		return `{"type":"array","items":` + s + `}`
	}},
	{name: "map-values", walks: true, wrap: func(s string) string {
		return `{"type":"map","values":` + s + `}`
	}},
	{name: "array-in-record", walks: true, wrap: func(s string) string {
		return `{"type":"record","name":"Outer","fields":[{"name":"a","type":{"type":"array","items":` + s + `}}]}`
	}},
	{name: "bare-record", walks: false, wrap: func(s string) string { return s }},
}

// schemaAsksMinBytes reports whether the schema rooted at n contains an array
// or a map — the only shapes that make a caller derive a per-element wire
// minimum, and so the only way schemaMinBytes is entered at all. Memoized,
// because the graphs it walks are the same shared-node DAGs.
func schemaAsksMinBytes(n *schemaNode) bool {
	return asksMinBytesSeen(n, map[*schemaNode]bool{})
}

func asksMinBytesSeen(n *schemaNode, seen map[*schemaNode]bool) bool {
	if n == nil || seen[n] {
		return false
	}
	seen[n] = true
	switch n.kind {
	case "array", "map":
		return true
	case "union":
		for _, b := range n.branches {
			if asksMinBytesSeen(b, seen) {
				return true
			}
		}
	case "record", "error":
		for i := range n.fields {
			if asksMinBytesSeen(n.fields[i].node, seen) {
				return true
			}
		}
	}
	return false
}

// ---------- the cost pin ----------

// dagCostDepth / dagCostDepth3 are the fan-2 and fan-3 depths every cost cell
// uses. They are chosen so that a walk re-descending per reference is ~2^26
// descents — decisively past dosBudget, yet still finishing on its own so a
// failing cell does not leave a goroutine running for hours. A walk that
// visits each node once does these in microseconds, so the margin between the
// two verdicts is four orders of magnitude and no machine noise crosses it.
const (
	dagCostDepth  = 26
	dagCostDepth3 = 16
)

// TestInvariant_SharedSchemaNodeWalkedOnce pins that a node reachable by
// several paths costs what a node reachable by one path costs, across every
// shape that produces the sharing and every container that asks for the
// bound.
func TestInvariant_SharedSchemaNodeWalkedOnce(t *testing.T) {
	const cell = "TestInvariant_SharedSchemaNodeWalkedOnce"
	shapes := []struct {
		name  string
		build func(levels, fan int) string
		// A shape that supplies its own container does not take a wrapper.
		selfWrapped bool
	}{
		{name: "nested", build: dagNested},
		{name: "flat-forward-ref", build: dagFlat, selfWrapped: true},
		{name: "self-recursive", build: dagSelfRecursive},
		{name: "single-scc", build: dagSingleSCC, selfWrapped: true},
	}
	for _, sh := range shapes {
		for _, tr := range minBytesTriggers {
			if sh.selfWrapped && tr.name != "array-items" {
				continue // the shape already carries its trigger
			}
			for _, fan := range []int{2, 3} {
				name := fmt.Sprintf("%s/%s/fan%d", sh.name, tr.name, fan)
				t.Run(name, func(t *testing.T) {
					// The factor is the PATH COUNT, fan^levels, so a fan-3
					// shape reaches the same place in proportionally fewer
					// levels. The row's pair is SCALED, not replaced, so both
					// fans still drive two values of the one factor.
					levelsFor := func(v int) int {
						if fan == 3 {
							return v * dagCostDepth3 / dagCostDepth
						}
						return v
					}
					build := func(v int) string {
						s := sh.build(levelsFor(v), fan)
						if !sh.selfWrapped {
							s = tr.wrap(s)
						}
						return s
					}
					// The trigger claim, checked rather than asserted in a
					// comment: a cell whose schema contains no container
					// asking for a per-element minimum never enters the walk,
					// so it would measure nothing whatever the walk did.
					parsed := mustParse(t, build(dagCostDepth))
					if got := schemaAsksMinBytes(parsed.node); got != tr.walks {
						t.Fatalf("trigger %q is registered as walks=%v but the parsed schema %s a container that asks for a per-element minimum",
							tr.name, tr.walks, map[bool]string{true: "contains", false: "does not contain"}[got])
					}
					wantCostDoesNotScale(t, cell, name, func(v int) func() error {
						s := build(v)
						return func() error { _, err := Parse(s); return err }
					})
				})
			}
		}
	}
}

// TestInvariant_SharingDoesNotChangeMinBytes is the value oracle, and it is
// calibration-free: it never states what the minimum IS. Sharing a node is a
// property of how a schema is WRITTEN, so the same type written with
// references and written out in full must produce the same bound.
func TestInvariant_SharingDoesNotChangeMinBytes(t *testing.T) {
	for _, fan := range []int{2, 3} {
		maxLevels := 7
		if fan == 3 {
			maxLevels = 5
		}
		for levels := 1; levels <= maxLevels; levels++ {
			t.Run(fmt.Sprintf("fan%d/levels%d", fan, levels), func(t *testing.T) {
				dag, err := Parse(dagNested(levels, fan))
				if err != nil {
					t.Fatalf("parse dag: %v", err)
				}
				tree, err := Parse(treeExpanded(levels, fan))
				if err != nil {
					t.Fatalf("parse tree: %v", err)
				}
				got, want := schemaMinBytes(dag.node), schemaMinBytes(tree.node)
				if got != want {
					t.Errorf("shared-node form gives min %d, fully-expanded form gives %d; the two describe the same type",
						got, want)
				}
			})
		}
	}
}

// minBytesNoMemo is the walk written WITHOUT a memo: mark on entry, unmark on
// exit, recompute per reference. It is a transcription of the algorithm, not a
// copy of the production code — no memo, no low-water mark, no pending map —
// and it exists to be the oracle for which results the production walk is
// allowed to remember. Exponential by construction, so every corpus schema
// driven through it is small.
func minBytesNoMemo(n *schemaNode, path map[*schemaNode]bool) int {
	if n == nil {
		return 1
	}
	if path[n] {
		return 1
	}
	path[n] = true
	defer delete(path, n)
	switch n.kind {
	case "null":
		return 0
	case "boolean", "int", "long", "enum":
		return 1
	case "float":
		return 4
	case "double":
		return 8
	case "bytes", "string":
		return 1
	case "fixed":
		return saturateSchemaMagnitude(n.size)
	case "array", "map":
		return 1
	case "union":
		m, found := 0, false
		for _, b := range n.branches {
			if v := minBytesNoMemo(b, path); !found || v < m {
				m, found = v, true
			}
		}
		if !found {
			return 1
		}
		return saturateSchemaMagnitude(1 + m)
	case "record":
		var s int
		for i := range n.fields {
			s = saturateSchemaMagnitude(s + minBytesNoMemo(n.fields[i].node, path))
			if s == maxSchemaMagnitude {
				return maxSchemaMagnitude
			}
		}
		return s
	}
	return 1
}

// TestInvariant_DagMinBytesIsExactAtScale separates the two mechanisms that bound
// this walk, which no cost cell can tell apart. The visit budget bounds COST for
// every shape, including one the memo cannot help with, so with the memo removed
// the cost cells still pass — what the budget cannot do is answer correctly: past
// it the walk stops deriving and falls back to a stand-in, so the bound comes back
// far too loose, and the memo is what makes a schema of this size come back with
// the real number. The expected value is arithmetic on the schema's own
// definition: every level has `fan` fields of the next and the deepest level's
// fields are ints, so the minimum is fan^levels.
func TestInvariant_DagMinBytesIsExactAtScale(t *testing.T) {
	cases := []struct {
		levels, fan int
	}{
		{dagCostDepth, 2},
		{dagCostDepth3, 3},
		{10, 2},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("levels%d/fan%d", c.levels, c.fan), func(t *testing.T) {
			want := 1
			for range c.levels {
				want *= c.fan
			}
			if want >= maxSchemaMagnitude {
				t.Fatalf("cell is above the saturation ceiling, so it measures the clamp instead")
			}
			s := mustParse(t, dagNested(c.levels, c.fan))
			if got := schemaMinBytes(s.node); got != want {
				t.Errorf("minimum is %d, want %d (%d fields per level, %d levels, ints at the bottom); "+
					"a walk that re-descends per reference cannot reach the bottom within its visit budget "+
					"and falls back to a stand-in, which is what a number below this one means",
					got, want, c.fan, c.levels)
			}
		})
	}
}

// TestInvariant_MemoAgreesWithUnmemoizedWalk is the oracle for WHICH results may
// be remembered, and the only one that can see the distinction.
//
// A back-edge does not return the referenced node's minimum; it returns a
// conservative stand-in, because that computation is still running. So a result
// reached through one is a property of the PATH, not of the node, and
// remembering it answers a later entry's question with an earlier entry's
// answer. Cost oracles cannot see that — a wrong memo is faster, not slower —
// and the fully-expanded twin cannot either, these schemas being cyclic with no
// finite expansion. What settles it is the walk with no memory at all: whatever
// it computes is by definition entry-independent.
//
// The corpus is the shapes where the distinction exists: mutual recursion, a
// node whose true minimum is BELOW the cycle stand-in (an all-null record, where
// remembering the stand-in would make the bound too TIGHT and reject real data),
// and the DAG shapes for the direction where no cycle is involved.
func TestInvariant_MemoAgreesWithUnmemoizedWalk(t *testing.T) {
	mutual := `{"type":"record","name":"R","fields":[` +
		`{"name":"a","type":{"type":"record","name":"A","fields":[` +
		`{"name":"f","type":{"type":"record","name":"X","fields":[` +
		`{"name":"back","type":"A"},{"name":"pad","type":"double"}]}},` +
		`{"name":"pad2","type":"double"}]}},` +
		`{"name":"x1","type":"X"},{"name":"x2","type":"X"}]}`

	// Z's minimum is 0, BELOW the stand-in a back-edge returns, so a result
	// remembered from inside the cycle would be too large — the direction
	// that turns a loose bound into a refusal of real wire bytes.
	zeroInCycle := `{"type":"record","name":"R","fields":[` +
		`{"name":"c","type":{"type":"record","name":"C","fields":[` +
		`{"name":"z","type":{"type":"record","name":"Z","fields":[` +
		`{"name":"self","type":["null","C"]}]}},` +
		`{"name":"back","type":["null","C"]}]}},` +
		`{"name":"z2","type":"Z"}]}`

	corpus := []struct{ name, schema string }{
		{"mutual-recursion", mutual},
		{"zero-minimum-inside-cycle", zeroInCycle},
		{"dag-nested", dagNested(6, 2)},
		{"dag-nested-fan3", dagNested(4, 3)},
		{"dag-self-recursive", dagSelfRecursive(5, 2)},
		{"dag-single-scc", dagSingleSCC(5, 2)},
		{"dag-flat", dagFlat(5, 2)},
		{"all-null-record", `{"type":"record","name":"N","fields":[{"name":"a","type":"null"},{"name":"b","type":"null"}]}`},
	}
	for _, c := range corpus {
		t.Run(c.name, func(t *testing.T) {
			s := mustParse(t, c.schema)
			var walk func(n *schemaNode, seen map[*schemaNode]bool)
			walk = func(n *schemaNode, seen map[*schemaNode]bool) {
				if n == nil || seen[n] {
					return
				}
				seen[n] = true
				got := schemaMinBytes(n)
				want := minBytesNoMemo(n, make(map[*schemaNode]bool))
				if got != want {
					t.Errorf("node %q: memoized walk says %d, a walk with no memory at all says %d — "+
						"the memo is reusing a result that was computed for a different entry",
						n.kind, got, want)
				}
				walk(n.items, seen)
				walk(n.values, seen)
				for _, b := range n.branches {
					walk(b, seen)
				}
				for i := range n.fields {
					walk(n.fields[i].node, seen)
				}
			}
			// Every node in the schema is its own entry point, which is the
			// whole question: the walk must give one answer per node, not one
			// answer per route to it.
			walk(s.node, make(map[*schemaNode]bool))
		})
	}
}

// TestInvariant_MinBytesSelfReadable is the second calibration-free oracle:
// the bound may never refuse wire bytes this package's own encoder produced.
// A memo that recorded a value derived through a cycle back-edge would
// tighten the bound for some entry points and this is what would catch it.
func TestInvariant_MinBytesSelfReadable(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		val    any
	}{
		{"array-of-dag", `{"type":"array","items":` + dagNested(3, 2) + `}`, nil},
		{"map-of-dag", `{"type":"map","values":` + dagNested(3, 2) + `}`, nil},
		{"array-of-selfrec", `{"type":"array","items":` + dagSelfRecursive(3, 2) + `}`, nil},
		{"array-of-null", `{"type":"array","items":"null"}`, []any{nil, nil, nil}},
		{"array-of-empty-record", `{"type":"array","items":{"type":"record","name":"E","fields":[]}}`, []any{map[string]any{}, map[string]any{}}},
		{"map-of-null", `{"type":"map","values":"null"}`, map[string]any{"a": nil, "b": nil}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := mustParse(t, c.schema)
			val := c.val
			if val == nil {
				val = buildZeroValue(t, s.node)
			}
			b := mustEncode(t, s, val)
			var out any
			if _, err := s.Decode(b, &out); err != nil {
				t.Errorf("this package encoded %d bytes its own bound then refused: %v", len(b), err)
			}
		})
	}
}

// buildZeroValue produces one legal value for node, used only to feed the
// self-readability oracle above.
func buildZeroValue(t *testing.T, n *schemaNode) any {
	t.Helper()
	switch n.kind {
	case "null":
		return nil
	case "boolean":
		return false
	case "int", "long":
		return 0
	case "float", "double":
		return 0.0
	case "string", "bytes":
		return ""
	case "array":
		return []any{buildZeroValue(t, n.items), buildZeroValue(t, n.items)}
	case "map":
		return map[string]any{"k": buildZeroValue(t, n.values)}
	case "union":
		return buildZeroValue(t, n.branches[0])
	case "record", "error":
		m := make(map[string]any, len(n.fields))
		for i := range n.fields {
			m[n.fields[i].name] = buildZeroValue(t, n.fields[i].node)
		}
		return m
	}
	t.Fatalf("no zero value for kind %q", n.kind)
	return nil
}

// ---------- the carrier enumeration ----------

// minBytesCallSite is one place in source that asks for a per-element wire
// minimum. The set is DERIVED by TestInvariant_MinBytesCallSites scanning
// for the call, not read off this table: the table only supplies the reason
// and the entry point, and the guard fails when the two disagree in either
// direction.
type minBytesCallSite struct {
	file  string
	count int
	entry string // the public call that reaches it
	why   string
}

var minBytesCallSites = []minBytesCallSite{
	{file: "schema.go", count: 3, entry: "Parse / MustParse / SchemaCache.Parse / SchemaFor",
		why: "the parse-time derivations: an array's per-item minimum (ONE computation, assigned to both " +
			"the deserArray slot and its fieldMeta twin — they are the same question, and computing it twice " +
			"is how the two came to disagree when only one was patched by the fixup), a map's minEntryBytes, " +
			"and the container fixup that re-derives them once a forward reference resolves"},
	{file: "resolve.go", count: 2, entry: "Resolve, and ocf.NewReader when the file's schema differs from the reader's",
		why: "the resolver rebuilds the bound against the WRITER's wire format for a resolved array and map"},
	{file: "skip.go", count: 2, entry: "Resolve when a writer field is dropped",
		why: "the skip compiled for a dropped writer field derives the same two bounds"},
	{file: "deser.go", count: 4, entry: "n/a — not callers",
		why: "the definition plus two doc references (schemaMinBytes' own doc and deserMap's " +
			"field comment naming its bound), plus the one real delegation: schemaMinBytes " +
			"spins up a fresh walk and calls minBytesOf on it for a single standalone node"},
}

// TestInvariant_MinBytesCallSites derives the set of sites that demand a
// per-element minimum and requires every one to be rowed with the entry
// point that reaches it. This is what keeps the cost matrix honest: a cell
// whose schema reaches no site on this list measures nothing at all, and a
// NEW site added without a row would be a new entry point no cost cell
// drives.
func TestInvariant_MinBytesCallSites(t *testing.T) {
	files := censusSourceFiles(t)
	// A per-element minimum is asked for two ways now: the standalone
	// schemaMinBytes(n) (which spins up a fresh walk for one node), and
	// walk.minBytesOf(n) (which joins the shared walk of an operation). Both are
	// entry points into the shared-node walk and both must be rowed, so the site
	// set is the union — a caller that switched to the shared form must not
	// vanish from the guard.
	found := occurrences(t, files, "schemaMinBytes(")
	for f, lines := range occurrences(t, files, ".minBytesOf(") {
		found[f] = append(found[f], lines...)
	}
	rowed := make(map[string]minBytesCallSite, len(minBytesCallSites))
	for _, r := range minBytesCallSites {
		rowed[r.file] = r
	}
	for file, lines := range found {
		r, ok := rowed[file]
		if !ok {
			t.Errorf("schemaMinBytes is called in UNROWED file %s (lines %v).\n  A new caller is a new entry point into the shared-node walk; row it with the public call that reaches it, and give the cost matrix a cell that drives that entry point.",
				file, lines)
			continue
		}
		if len(lines) != r.count {
			t.Errorf("schemaMinBytes is called %d times in %s (lines %v), the table says %d.\n  If a call was ADDED, name its entry point; if one was REMOVED, bring the count down or this row guards code that is gone.",
				len(lines), file, lines, r.count)
		}
	}
	for _, r := range minBytesCallSites {
		if len(found[r.file]) == 0 {
			t.Errorf("%s is rowed with %d calls to schemaMinBytes but has none — the row has rotted", r.file, r.count)
		}
	}
	// The control cell is a claim about source, so check it against source:
	// no caller lives in a file that compiles plain records, which is why a
	// bare-record DAG never enters the walk.
	if len(found["json_codec.go"]) != 0 || len(found["json_decode.go"]) != 0 {
		t.Errorf("a JSON path now derives a per-element minimum; the cost matrix drives only the binary triggers and needs a JSON cell")
	}
}

// TestInvariant_DagCostMatrixDrivesEveryEntryPoint crosses the derived carrier set
// with the matrix's own cells, so an entry point that gains a caller cannot stay
// undriven. It reads the matrix's own source rather than trusting a count: the
// failure it prevents is a row added to minBytesCallSites with no cell behind it.
//
// It reads the SECTION, not the enclosing file. "cache.Parse(" is one of the
// cells, and the schema_test.go the matrix now lives in uses that string fifty-six
// more times outside this section — reading the whole file would keep this guard
// green with the cell deleted.
func TestInvariant_DagCostMatrixDrivesEveryEntryPoint(t *testing.T) {
	body := testFileSection(t, "schema_test.go", "schema_dag_cost_test.go")

	// Cut THIS function out of what is searched. The want list below sits inside
	// the very section being read, so every entry matched itself and the guard
	// could not go red in any circumstance: renaming all six real "ocf-header"
	// cells left it green, and one entry, "Parse(dag)", never had a second
	// occurrence at all — it asserted nothing from the day it was written. The
	// wants below are the cells' LABELS, which is what the matrix names them.
	if i := strings.Index(body, "func "+t.Name()); i >= 0 {
		if j := strings.Index(body[i:], "\n}\n"); j >= 0 {
			body = body[:i] + body[i+j:]
		}
	}

	for _, want := range []string{
		`t, cell, "Parse"`,                      // the parse-time derivation
		`t, cell, "SchemaCache.Parse"`,          // the cache's own parse
		`t, cell, "Resolve/dropped-field-skip"`, // the skip compiled for a dropped writer field
		`t, cell, "Resolve/kept-field"`,         // the resolver's rebuild of a kept container
		`t, cell, "ocf-header/schema-parse"`,    // the container reader's, schema read from the file
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the cost matrix no longer drives %q; every rowed entry point needs a cell", want)
		}
	}
}

// TestInvariant_EveryMinBytesEntryPointIsBounded drives each rowed entry
// point with the same DAG, so no single caller can regress on its own. The
// ocf cell is the one where the schema is supplied by the INPUT rather than
// by the caller, which is what sets this class's severity.
func TestInvariant_EveryMinBytesEntryPointIsBounded(t *testing.T) {
	const cell = "TestInvariant_EveryMinBytesEntryPointIsBounded"
	dagAt := func(depth int) string { return `{"type":"array","items":` + dagNested(depth, 2) + `}` }

	wantCostDoesNotScale(t, cell, "Parse", func(depth int) func() error {
		s := dagAt(depth)
		return func() error { _, err := Parse(s); return err }
	})

	wantCostDoesNotScale(t, cell, "SchemaCache.Parse", func(depth int) func() error {
		s := dagAt(depth)
		return func() error {
			// The cache memoizes by TEXT, so it saves a REPEATED parse and
			// nothing at all on the first one; this cell drives the first.
			var cache SchemaCache
			_, err := cache.Parse(s)
			return err
		}
	})

	wantCostDoesNotScale(t, cell, "Resolve/dropped-field-skip", func(depth int) func() error {
		wDrop := MustParse(fmt.Sprintf(
			`{"type":"record","name":"Top","fields":[{"name":"x","type":%s},{"name":"y","type":"int"}]}`, dagAt(depth)))
		rDrop := MustParse(`{"type":"record","name":"Top","fields":[{"name":"y","type":"int"}]}`)
		return func() error { _, err := Resolve(wDrop, rDrop); return err }
	})

	wantCostDoesNotScale(t, cell, "Resolve/kept-field", func(depth int) func() error {
		wKeep := MustParse(fmt.Sprintf(
			`{"type":"record","name":"Top","fields":[{"name":"x","type":%s}]}`, dagAt(depth)))
		rKeep := MustParse(fmt.Sprintf(
			`{"type":"record","name":"Top","fields":[{"name":"x","type":%s},{"name":"z","type":"int","default":0}]}`, dagAt(depth)))
		return func() error { _, err := Resolve(wKeep, rKeep); return err }
	})

	// ocf-header: the container reader derives the bound from a schema it
	// read out of the file, so the cost is driven by the input rather than
	// by the caller. The ocf package cannot be imported from package avro,
	// so the executable cell lives in ocf/dos_battery_test.go; this cell
	// pins the parse of the identical header schema that reaches it.
	wantCostDoesNotScale(t, cell, "ocf-header/schema-parse", func(depth int) func() error {
		s := dagAt(depth)
		return func() error { _, err := Parse(s); return err }
	})
}

// ---------- the WIDTH axis ----------

// dagWideSCC crosses the cyclic shapes with the axis the other cost cells hold
// constant: how many children ONE node has.
//
// dagSingleSCC and dagSelfRecursive fix fan at 2 or 3 because fan is what drives
// DEPTH — the number of distinct root-to-leaf paths — so every cyclic cell
// measures the visit allowance times two. But the work a single node costs is
// its own child count, a SECOND number the schema author picks independently.
// This shape separates them: the chain stays fan-narrow so the path count still
// exhausts the allowance, and the record every path ENDS at carries `width`
// extra fields, so each recomputation of that one record pays `width`.
//
// Three properties decide whether this shape measures anything at all, each
// MEASURED at a matched text size of ~124 KB rather than reasoned about — the
// point being that a plausible-looking variant costs milliseconds and proves the
// opposite of what it looks like:
//
//   - CYCLIC, the enabling one: the wide record closes back to L0, so every node
//     in the chain is in one strongly-connected component and nothing is
//     memoizable. Point that back-edge at "int" and the same 124 KB parses in
//     10.7 ms rather than 6.5 s — 600x — because the memo then answers each node
//     once.
//   - CONCENTRATED, the dominant one: a node is recomputed once per path
//     reaching it, so revisits peak at the node every path ENDS at. Spreading
//     the same total width evenly over the levels costs 435 ms against 6.5 s —
//     15x — since spreading width over D levels makes each computation cost
//     width/D.
//   - ZERO-MINIMUM fillers (`null`), worth 3x on top (6.5 s vs 2.2 s for
//     `double`). Not because a wide record saturates its own running sum — 4000
//     doubles is 32000, far under the ceiling — but because the CHAIN above it
//     doubles that figure per level and reaches the ceiling a dozen levels up,
//     and a saturated sum returns EARLY, before the field that continues the
//     fan-out.
func dagWideSCC(levels, fan, width int) string {
	var wide strings.Builder
	wide.WriteString(`{"type":"record","name":"W","fields":[{"name":"back","type":"L0"}`)
	for k := range width {
		fmt.Fprintf(&wide, `,{"name":"p%d","type":"null"}`, k)
	}
	wide.WriteString(`]}`)

	inner := wide.String()
	for i := levels - 1; i >= 0; i-- {
		next := fmt.Sprintf("L%d", i+1)
		if i == levels-1 {
			next = "W"
		}
		var b strings.Builder
		fmt.Fprintf(&b, `{"type":"record","name":"L%d","fields":[{"name":"f0","type":%s}`, i, inner)
		for k := 1; k < fan; k++ {
			fmt.Fprintf(&b, `,{"name":"f%d","type":"%s"}`, k, next)
		}
		b.WriteString(`]}`)
		inner = b.String()
	}
	return inner
}

// dagWideLevels / dagWideWidth are the width cell's two magnitudes. Levels is
// chosen so the path count reaches the walk's allowance (a narrower chain
// never exhausts it and the width has nothing to multiply); width is chosen so
// that allowance x width is decisively past dosBudget while the schema text
// stays a few hundred KB and the walk still finishes on its own.
const (
	dagWideLevels = 16
	dagWideWidth  = 8000
)

// TestInvariant_CyclicWalkCostIsBoundedByWork is the WIDTH half of the cost
// guard: an allowance spent per NODE ENTERED bounds how many nodes are
// entered, not how much work they do, so a cap counting entries is bounded
// only when every entry costs the same. Here they do not — a record's entry
// iterates its own fields — and both factors are chosen by whoever wrote the
// schema, so the guard has to be charged in the unit of the work.
func TestInvariant_CyclicWalkCostIsBoundedByWork(t *testing.T) {
	const cell = "TestInvariant_CyclicWalkCostIsBoundedByWork"
	for _, tr := range minBytesTriggers {
		t.Run(tr.name, func(t *testing.T) {
			// Same trigger claim the depth cells check: a shape that reaches
			// no caller of the walk measures nothing whatever the walk does.
			parsed := mustParse(t, tr.wrap(dagWideSCC(dagWideLevels, 2, 8)))
			if got := schemaAsksMinBytes(parsed.node); got != tr.walks {
				t.Fatalf("trigger %q is registered as walks=%v but the parsed schema %s a container that asks for a per-element minimum",
					tr.name, tr.walks, map[bool]string{true: "contains", false: "does not contain"}[got])
			}
			wantCostDoesNotScale(t, cell, "Parse/wide-scc/"+tr.name, func(width int) func() error {
				s := tr.wrap(dagWideSCC(dagWideLevels, 2, width))
				return func() error { _, err := Parse(s); return err }
			})
		})
	}
}

// TestInvariant_WideCyclicWalkReachesEveryEntryPoint drives the same shape
// through the entry points that do not take the schema from the caller.
func TestInvariant_WideCyclicWalkReachesEveryEntryPoint(t *testing.T) {
	const cell = "TestInvariant_WideCyclicWalkReachesEveryEntryPoint"
	at := func(width int) string {
		return `{"type":"array","items":` + dagWideSCC(dagWideLevels, 2, width) + `}`
	}

	wantCostDoesNotScale(t, cell, "SchemaCache.Parse/wide-scc", func(width int) func() error {
		s := at(width)
		return func() error {
			var c SchemaCache
			_, err := c.Parse(s)
			return err
		}
	})
	wantCostDoesNotScale(t, cell, "Resolve/wide-scc", func(width int) func() error {
		parsed := MustParse(at(width))
		return func() error { _, err := Resolve(parsed, parsed); return err }
	})
	// The writer field is DROPPED, which compiles a skip — a separate
	// derivation of the same per-element bound.
	wantCostDoesNotScale(t, cell, "Resolve/wide-scc-dropped-field", func(width int) func() error {
		s := at(width)
		w := MustParse(`{"type":"record","name":"T","fields":[{"name":"x","type":` + s + `},{"name":"y","type":"int"}]}`)
		r := MustParse(`{"type":"record","name":"T","fields":[{"name":"y","type":"int"}]}`)
		return func() error { _, err := Resolve(w, r); return err }
	})
	// ocf-header: the executable cell lives in ocf/dos_battery_test.go
	// (package avro cannot import ocf); this pins the parse that reaches it.
	wantCostDoesNotScale(t, cell, "ocf-header/wide-scc-schema-parse", func(width int) func() error {
		s := at(width)
		return func() error { _, err := Parse(s); return err }
	})
}

// TestInvariant_MinBytesChargeCoversEveryChildArm derives the charge's set from
// source instead of trusting the two switches to stay in step. The allowance is
// charged by the PARENT, for the whole child list, before it descends, so every
// child examination is paid for exactly once by whoever performs it. That
// accounting is complete only while minBytesChildren counts the children of
// exactly the kinds minBytesFromChildren descends into — two switches over the
// same vocabulary, where a kind added to one alone is silent: an unaccounted arm
// restores the unbounded product, and an over-counted one spends the allowance
// on descents that never happen.
func TestInvariant_MinBytesChargeCoversEveryChildArm(t *testing.T) {
	src, err := os.ReadFile("deser.go")
	if err != nil {
		t.Fatalf("reading deser.go: %v", err)
	}
	body := func(sig string) string {
		i := strings.Index(string(src), sig)
		if i < 0 {
			t.Fatalf("%q not found in deser.go — the guard is aimed at a function that no longer exists", sig)
		}
		rest := string(src)[i:]
		if j := strings.Index(rest[1:], "\nfunc "); j >= 0 {
			rest = rest[:j+1]
		}
		return rest
	}
	// caseKinds returns the quoted kind labels of every `case` arm in s for
	// which keep reports true of the arm's body.
	caseKinds := func(s string, keep func(arm string) bool) map[string]bool {
		out := map[string]bool{}
		parts := strings.Split(s, "\n\tcase ")
		for _, p := range parts[1:] {
			head, arm, _ := strings.Cut(p, ":")
			if !keep(arm) {
				continue
			}
			for _, lit := range strings.Split(head, ",") {
				out[strings.Trim(strings.TrimSpace(lit), `"`)] = true
			}
		}
		return out
	}
	charged := caseKinds(body("func minBytesChildren("), func(string) bool { return true })
	descends := caseKinds(body("func (w *minBytesWalk) minBytesFromChildren("),
		func(arm string) bool { return strings.Contains(arm, "w.minBytes(") })

	if len(charged) == 0 || len(descends) == 0 {
		t.Fatalf("extracted no arms (charged=%v descends=%v) — the guard cannot see what it is guarding", charged, descends)
	}
	for k := range descends {
		if !charged[k] {
			t.Errorf("minBytesFromChildren descends into %q's children but minBytesChildren does not count them: "+
				"entering such a node costs its child count and is charged as if it cost one, "+
				"which is the unbounded product this allowance exists to prevent", k)
		}
	}
	for k := range charged {
		if !descends[k] {
			t.Errorf("minBytesChildren counts %q's children but minBytesFromChildren never descends into them: "+
				"the allowance is spent on work that does not happen, tightening the bound for no reason", k)
		}
	}
}

// TestInvariant_MetadataSurfacesBoundedByWidth is the measured half of an immunity
// claim rather than a read of it. The SchemaNode->JSON walk carries its own
// allowance, and the reason it has no width residue is structural: it charges
// takeNode at the TOP of every entry, ahead of the cycle and dedup checks that can
// return early, so a child costs a unit whether or not the walk descends through
// it. The min-bytes walk charged AFTER its memo, which is exactly how a memo hit
// could examine a child for free. A claim like that is worth no more than the
// probe behind it, so the same wide cyclic shape is driven here.
func TestInvariant_MetadataSurfacesBoundedByWidth(t *testing.T) {
	const cell = "TestInvariant_MetadataSurfacesBoundedByWidth"
	at := func(width int) *Schema {
		return MustParse(`{"type":"array","items":` + dagWideSCC(dagWideLevels, 2, width) + `}`)
	}
	wantCostDoesNotScale(t, cell, "Root+Schema/wide-scc", func(width int) func() error {
		s := at(width)
		return func() error {
			root := s.Root()
			_, _ = root.Schema()
			return nil
		}
	})
	wantCostDoesNotScale(t, cell, "String+Canonical/wide-scc", func(width int) func() error {
		s := at(width)
		return func() error {
			_ = s.String()
			_ = s.Canonical()
			return nil
		}
	})
}

// ---------- the CONTAINER-COUNT axis ----------

// The two axes above — paths per walk and children per node — each hold the
// container count at ONE: every cell wraps a single array around a single DAG.
// But schemaMinBytes runs once per container, and a schema chooses how many
// containers point at one subtree independently of how deep or wide that subtree
// is. So the parse cost is a PRODUCT,
//
//	containers x paths-per-walk x children-per-node,
//
// and a bound capping any single factor leaves the other two to multiply. The
// walk's memo and allowance cap paths and children WITHIN one walk; what caps
// the container factor is that one walk is SHARED across all the containers of
// an operation (newMinBytesWalk, threaded through finalize, resolve, and each
// record's skip compile). These cells drive that shared walk with the container
// count raised and the other two held where the per-walk bounds engage.

// nContainersOverSCC builds a record with narrays array fields, every one of
// items "L0", above a cyclic SCC L0..L{levels-1} -> L0. The arrays come FIRST,
// so their items are FORWARD references resolved in finalize's fixup loop — this
// exercises the FINALIZE reaching-path. Each extra array is ~48 bytes, so the
// container count is caller-chosen; the SCC is un-memoizable, so a fresh walk
// per array pays the full allowance per array.
func nContainersOverSCC(narrays, levels int) string {
	var b strings.Builder
	b.WriteString(`{"type":"record","name":"Root","fields":[`)
	for j := 0; j < narrays; j++ {
		if j > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"name":"z%d","type":{"type":"array","items":"L0"}}`, j)
	}
	for i := 0; i < levels; i++ {
		next := fmt.Sprintf("L%d", i+1)
		if i == levels-1 {
			next = "L0"
		}
		fmt.Fprintf(&b, `,{"name":"d%d","type":{"type":"record","name":"L%d","fields":[{"name":"f0","type":["null","%s"]},{"name":"f1","type":["null","%s"]}]}}`, i, i, next, next)
	}
	b.WriteString(`]}`)
	return b.String()
}

// nContainersOverWiredSCC builds the SAME cost — many containers over one cyclic
// subtree — reached by the OTHER construction path. The cyclic node is defined
// FIRST and fully wired at BUILD: each level nests the next inline and
// references it a second time by name, a BACKWARD reference to the just-built
// inline, and the deepest closes to the enclosing "L0", so both spellings bind
// to one node and the whole cycle is wired before any container is built. The N
// arrays then reference "L0" backward and each resolves to the fully built node,
// so the per-element minimum is computed at BUILD, not finalize. A per-walk cost
// row cannot tell this path from the forward one. Without the shared build walk
// each array paid a full walk: 258 ms -> 7.4 s across 1..32 arrays before the
// fix.
func nContainersOverWiredSCC(narrays, levels int) string {
	inner := `["null","L0"]` // deepest closes the cycle to the enclosing L0
	for i := levels - 1; i >= 0; i-- {
		if i == levels-1 {
			inner = fmt.Sprintf(`{"type":"record","name":"L%d","fields":[{"name":"f0","type":["null","L0"]},{"name":"f1","type":["null","L0"]}]}`, i)
			continue
		}
		inner = fmt.Sprintf(`{"type":"record","name":"L%d","fields":[{"name":"f0","type":["null",%s]},{"name":"f1","type":["null","L%d"]}]}`, i, inner, i+1)
	}
	var b strings.Builder
	b.WriteString(`{"type":"record","name":"Root","fields":[{"name":"def","type":` + inner + `}`)
	for j := 0; j < narrays; j++ {
		fmt.Fprintf(&b, `,{"name":"z%d","type":{"type":"array","items":"L0"}}`, j)
	}
	b.WriteString(`]}`)
	return b.String()
}

// containerCountN / containerCountLevels are chosen so a fresh walk per
// container costs containers x allowance — decisively past dosBudget — while one
// shared walk pays the allowance once and finishes in well under it, four orders
// of magnitude apart so no machine noise crosses the line.
const (
	containerCountN      = 220
	containerCountLevels = 26
)

// TestInvariant_MinBytesContainerCountBounded is the container-count half of the
// cost guard, crossed through every entry point the walk is reached from. Each
// drives many containers over ONE shared cyclic SCC; the shared walk pays for it
// once, a fresh-walk-per-container regression pays for it per container.
func TestInvariant_MinBytesContainerCountBounded(t *testing.T) {
	const cell = "TestInvariant_MinBytesContainerCountBounded"

	// Parse, FORWARD refs: the arrays precede the SCC, so their items resolve in
	// finalize's container-fixup loop, which shares one walk across all of them.
	wantCostDoesNotScale(t, cell, "Parse/many-containers-forward", func(n int) func() error {
		s := nContainersOverSCC(n, containerCountLevels)
		return func() error { _, err := Parse(s); return err }
	})
	wantCostDoesNotScale(t, cell, "SchemaCache.Parse/many-containers-forward", func(n int) func() error {
		s := nContainersOverSCC(n, containerCountLevels)
		return func() error {
			var c SchemaCache
			_, err := c.Parse(s)
			return err
		}
	})

	// Parse, BACKWARD refs: the SCC is defined first and fully wired at build, so
	// each array's items resolves to the built cyclic node and the minimum is
	// computed on the BUILD reaching-path, not finalize. Reference DIRECTION is
	// its own axis, and it is crossed here WITH the count rather than instead
	// of it — varying only the direction is what left the count pinned.
	wantCostDoesNotScale(t, cell, "Parse/many-containers-backward", func(n int) func() error {
		s := nContainersOverWiredSCC(n, containerCountLevels)
		return func() error { _, err := Parse(s); return err }
	})
	wantCostDoesNotScale(t, cell, "SchemaCache.Parse/many-containers-backward", func(n int) func() error {
		s := nContainersOverWiredSCC(n, containerCountLevels)
		return func() error {
			var c SchemaCache
			_, err := c.Parse(s)
			return err
		}
	})

	// Resolve: a reader that differs (extra field) forces resolveRecord to
	// recurse into every array, each calling ctx.minBytes on the shared walk.
	wantCostDoesNotScale(t, cell, "Resolve/many-containers", func(n int) func() error {
		scc := nContainersOverSCC(n, containerCountLevels)
		w := MustParse(scc)
		r := MustParse(strings.Replace(scc,
			`{"type":"record","name":"Root","fields":[`,
			`{"type":"record","name":"Root","fields":[{"name":"extra","type":"int","default":0},`, 1))
		return func() error { _, err := Resolve(w, r); return err }
	})

	// Skip: a dropped writer field whose subtree is the many-containers record.
	// The skip is compiled lazily at first decode, on the resolution's own walk;
	// this drives that compile.
	wantCostDoesNotScale(t, cell, "Resolve+Decode/many-containers-dropped", func(n int) func() error {
		scc := nContainersOverSCC(n, containerCountLevels)
		wDrop := MustParse(`{"type":"record","name":"Top","fields":[{"name":"x","type":` + scc + `},{"name":"y","type":"int"}]}`)
		rDrop := MustParse(`{"type":"record","name":"Top","fields":[{"name":"y","type":"int"}]}`)
		// Minimal wire for wDrop: the SCC record's arrays empty, its records
		// all null-union index 0, then y. The skip compile fires inside.
		wire := manyContainersMinimalWire(n, containerCountLevels)
		return func() error {
			rs, err := Resolve(wDrop, rDrop)
			if err != nil {
				return err
			}
			var out map[string]any
			_, err = rs.Decode(wire, &out)
			return err
		}
	})

	// ocf-header: the executable cell lives in ocf/dos_battery_test.go; this
	// pins the parse of the identical header schema that reaches it.
	wantCostDoesNotScale(t, cell, "ocf-header/many-containers", func(n int) func() error {
		s := nContainersOverSCC(n, containerCountLevels)
		return func() error { _, err := Parse(s); return err }
	})
}

// manyContainersMinimalWire encodes a Top value for the skip cell: x = the SCC
// record (narrays empty arrays + levels records of two null-union fields), then
// y = 7. Every varint here is a single zigzag byte.
func manyContainersMinimalWire(narrays, levels int) []byte {
	var b []byte
	appendZig := func(v int64) { b = append(b, byte(uint64(v)<<1)) }
	for j := 0; j < narrays; j++ {
		appendZig(0) // empty array block
	}
	for i := 0; i < levels; i++ {
		appendZig(0) // f0 union index 0 (null)
		appendZig(0) // f1 union index 0 (null)
	}
	appendZig(7) // y
	return b
}

// ---------- budgeted_walk_census_test.go ----------

// Budgeted-walk census.
//
// A parse/resolve/skip/metadata operation's cost is a PRODUCT of factors the
// caller chooses independently — for the min-bytes walk it is
//
//	containers x paths-per-walk x children-per-node,
//
// and a bound capping any single factor leaves the rest to multiply. Three
// consecutive fixes each capped ONE factor of that one walk before the shape was
// seen whole. The lesson is not about that walk: EVERY budgeted walk in the
// package needs its cost written as a product and one bound that caps the
// product rather than a factor.
//
// budgetedWalks is the registry: one row per walk, naming the state it carries,
// what it traverses, the factors of its cost, and the single bound capping their
// product. The guards DERIVE the walk set from source two ways and fail when a
// walk appears that is not rowed, so a fourth factor or a wholly new walk cannot
// land without a cost expression:
//
//   - by COST MARKER: a walk carrying an allowance, a walkBudget, a
//     (reader,writer) pair memo, a visited/seen set over a graph node type, or a
//     defer-delete cycle mark — the states that bound a graph walk.
//   - by RECURSION: a function that recurses over the schema graph, marker or
//     not, so a new walk with no cost state at all is still caught.
//
// The discriminator deciding whether a bound is enough is WHAT the walk
// traverses:
//
//   - schemaDAG: the shared *schemaNode/*SchemaNode graph, where a named type
//     referenced twice is one node on two paths. Depth cannot bound this (the
//     fan-out is reachable flat); only a MEMO over the nodes, or a BUDGET over
//     the work, caps the paths factor. Every schemaDAG row must name one.
//   - goTypeDAG: a reflect.Type graph. Same shape, but fixed at COMPILE time and
//     amortized by a per-type sync.Map, so the bound is that it is not
//     attacker-grown at runtime (see G3).
//   - valueTree / wire / textTree: a caller VALUE, the wire bytes, or the schema
//     TEXT — none of which shares sub-structure, so node count IS input size and
//     a depth cap plus the input length bounds the walk.

type walkClass string

const (
	schemaDAG walkClass = "schemaDAG" // shared *schemaNode/*SchemaNode graph
	goTypeDAG walkClass = "goTypeDAG" // reflect.Type graph, compile-time
	valueTree walkClass = "valueTree" // a caller value (no sharing)
	textTree  walkClass = "textTree"  // schema text / parsed aschema (no sharing)
)

// Wire-decode/skip walks (deserRecord, deserArray, skipRecord, decodeValue, ...)
// are a fourth, uniform class not rowed individually: each consumes at least one
// wire byte per node and is capped by sl.depth>=maxDepth, so its node count is
// the input length and no caller-chosen product hides in it. The one place a
// wire walk touches a schema-graph cost — the skip compiler asking a per-element
// minimum — is the minBytes row, reached through skip's shared walk.

// budgetedWalk is one recursive traversal that carries cost-limiting state.
type budgetedWalk struct {
	fn      string    // the function or method the recursion is named by
	file    string    // where it lives
	class   walkClass // what it traverses — decides whether its bound is enough
	factors string    // the cost as a product of caller-chosen magnitudes
	binds   string    // the single bound that caps the PRODUCT, not one factor
	// reachingPaths names every CONSTRUCTION PATH that reaches this walk and the
	// bound each carries. One walk can be reached by more than one path (the
	// min-bytes walk is reached at build, at finalize, at resolve, and at skip),
	// and a bound that holds on one path but not another is invisible to the rows
	// above — factors/binds describe the walk, not the paths. For the multi-path
	// walks a guard derives the paths from source and checks each is bounded
	// (TestInvariant_MinBytesReachingPaths); a single-construction walk names its
	// one entry.
	reachingPaths string
}

// budgetedWalks is the registry. Every recursive schema-graph walk and every
// cost-marker-bearing walk in the package must appear here; the guards derive
// both sets from source and diff them against this list.
var budgetedWalks = []budgetedWalk{
	// ---- schemaDAG: must bind the paths factor with a memo or a budget ----
	{fn: "minBytes", file: "deser.go", class: schemaDAG,
		factors:       "containers x paths-per-walk x children-per-node",
		binds:         "one minBytesWalk SHARED per operation (containers) + done memo (paths) + per-child allowance charge (children)",
		reachingPaths: "THREE constructions, each shared across one operation: build (b.minBytes on the builder, forward refs fixed in finalize AND backward refs resolved to a fully-built node at build), finalize (one mbw before the container-fixup loop), and resolve (ctx.minBytes) — which also carries the SKIP path, since a dropped field's record compile is deferred to decode time but joins the resolution's walk rather than starting its own. The standalone schemaMinBytes is a fourth, single-node, outside any loop and with no production caller. Guarded by TestInvariant_MinBytesReachingPaths (the set, from source) and TestInvariant_EveryReachingPathBoundIsMeasured (each path's counts, driven at two values)"},
	{fn: "checkCompat", file: "compat.go", class: schemaDAG,
		factors:       "distinct (reader,writer) node pairs x children-per-pair",
		binds:         "the seen map[nodePair]bool memo, threaded from CheckCompatibility with no defer-delete, so each pair is walked once",
		reachingPaths: "one: seen created in CheckCompatibility, threaded through the whole recursive check"},
	{fn: "resolveNode", file: "resolve.go", class: schemaDAG,
		factors:       "distinct (reader,writer) pairs x (children + per-container min-bytes)",
		binds:         "ctx.seen pair memo (pairs) + ctx.minBytes shared walk (the container min-bytes factor)",
		reachingPaths: "one: ctx (seen + minBytes) created in Resolve, threaded through the whole resolution"},
	{fn: "toJSONWalk", file: "schema_node.go", class: schemaDAG,
		factors: "nodes emitted x bytes per node",
		binds: "walkBudget (nodes + bytes), charged by takeNode at the TOP of every entry so a DAG re-descent still spends budget; visited is only cycle detection. " +
			"MEASURED BY TestMatrix_SchemaNodeWalkBudgetBattery / TestRegression_SchemaNodeDuplicateNamedDefinitionBounded / TestMatrix_SchemaForCustomSchemaBudgetAxes, which hand-build the trees Parse cannot express — " +
			"a PARSED schema is deduped before it reaches this walk, so no parse-driven cell can red this bound, and one that claimed to was renamed",
		reachingPaths: "one walkBudget per metadata-API call (toJSONDedup), from Root().Schema()/String()/Canonical(); each walks the whole tree once"},
	{fn: "collectLocalNames", file: "schema_node.go", class: schemaDAG,
		factors:       "distinct nodes x names per node",
		binds:         "the visited map[*SchemaNode]struct{} memo (mark on entry, return on hit)",
		reachingPaths: "one: visited created per toJSONDedup, one walk of the tree"},
	{fn: "stampNameRefs", file: "schema_node.go", class: schemaDAG,
		factors:       "distinct nodes",
		binds:         "the visited memo (mark on entry, return on hit)",
		reachingPaths: "one: visited created per Root() name-ref stamping pass"},
	{fn: "collectNamedTypes", file: "schema_node.go", class: schemaDAG,
		factors:       "tree nodes (name-ref nodes are leaves, not followed)",
		binds:         "structural: a reference SchemaNode carries no children, so the walk is over the definition TREE, linear in it",
		reachingPaths: "one: table created in fixupNameRefDefaults, per Root()"},
	{fn: "coerceTreeDefaults", file: "schema_node.go", class: schemaDAG,
		factors:       "tree nodes (name-ref nodes are leaves)",
		binds:         "structural: references are leaves, so the walk is over the definition TREE, linear in it",
		reachingPaths: "one: same fixupNameRefDefaults pass, per Root()"},
	{fn: "overlayInheritedCustom", file: "schema.go", class: schemaDAG,
		factors:       "distinct nodes x custom lookups",
		binds:         "the visited map[*schemaNode]bool memo (mark on entry, return on hit)",
		reachingPaths: "one: b.overlayDone created per parse, walked at inherited-custom overlay (build/reference-time)"},
	{fn: "findCustomTypeMatchInSubtreeWalk", file: "schema.go", class: schemaDAG,
		factors:       "distinct nodes x registered custom types",
		binds:         "the visited map[*schemaNode]bool memo (mark on entry, return on hit)",
		reachingPaths: "one: visited created per findCustomTypeMatchInSubtree call at build"},
	{fn: "buildCustomWiring", file: "schema.go", class: schemaDAG,
		factors:       "distinct nodes",
		binds:         "the visited memo (mark on entry, return on hit)",
		reachingPaths: "one: visited created per applyCustomTypes pass at build"},
	{fn: "nodeAwaitsForwardRefSeen", file: "schema.go", class: schemaDAG,
		factors:       "distinct nodes",
		binds:         "the seen map[*schemaNode]struct{} memo (mark on entry, return on hit); the separate building set is defer-delete cycle detection",
		reachingPaths: "one: seen created per nodeAwaitsForwardRef call at build"},

	// ---- goTypeDAG: bound is compile-time fixedness + per-type sync.Map ----
	{fn: "collect", file: "reflect.go", class: goTypeDAG,
		factors:       "2^(embed depth) on a shared-embed type DAG (visited is defer-delete, so it re-descends)",
		binds:         "NOT a runtime bound: a Go type is fixed at COMPILE time and the result is amortized by a per-type sync.Map, so the fan-out is not attacker-grown (G3)",
		reachingPaths: "one: visited created per typeFieldMapping (per Go type; the sync.Map amortizes repeats across calls)"},
	{fn: "collectFieldsRaw", file: "schema_for.go", class: goTypeDAG,
		factors:       "2^(embed depth) on a shared-embed type DAG (visited is defer-delete)",
		binds:         "compile-time fixedness + collectFields' per-call visited; not attacker-grown at runtime (G3)",
		reachingPaths: "one: visited created per collectFields, per SchemaFor of a Go type"},
	{fn: "inferType", file: "schema_for.go", class: goTypeDAG,
		factors:       "type nodes x ptr chains, bounded by depth and maxIndirectDepth",
		binds:         "seen map[reflect.Type]seenForm memo + depth/ptrChain caps; compile-time type",
		reachingPaths: "one: seen created per SchemaFor, per Go type"},

	// ---- valueTree / wire / textTree: node count IS input size ----
	{fn: "walkDefault", file: "schema.go", class: valueTree,
		factors:       "default value nodes",
		binds:         "the walk follows the concrete default VALUE (a finite JSON tree), linear in it",
		reachingPaths: "one: per default-encode pass, following the value"},
	{fn: "coerceDefault", file: "schema.go", class: valueTree,
		factors:       "default value nodes, bounded by depth",
		binds:         "value-guided recursion + the depth>=maxDepth cap",
		reachingPaths: "one: per default coercion at parse, following the value"},
	{fn: "coerceMetadataDefault", file: "schema_node.go", class: valueTree,
		factors:       "default value nodes",
		binds:         "value-guided recursion over the concrete default (name-ref follows are one hop, guided by the value)",
		reachingPaths: "one: per Root() metadata default coercion, following the value"},
	{fn: "branchAcceptsDefault", file: "schema_node.go", class: valueTree,
		factors:       "default value nodes",
		binds:         "value-guided recursion over the concrete default",
		reachingPaths: "one: per branch-acceptance check, following the value"},
	{fn: "encodeDefaultDepth", file: "resolve.go", class: valueTree,
		factors:       "default value nodes, bounded by depth",
		binds:         "value-guided recursion + the depth>=maxDepth cap",
		reachingPaths: "one: per default encode, following the value"},
	{fn: "appendAvroJSON", file: "json_codec.go", class: valueTree,
		factors:       "encoded value nodes, bounded by depth",
		binds:         "value-guided recursion + the depth>=maxDepth cap",
		reachingPaths: "one: per EncodeJSON/AppendEncodeJSON call, following the value"},
	{fn: "valueWalkLimit", file: "schema_node.go", class: valueTree,
		factors:       "value nodes x depth",
		binds:         "walkBudget + the depthLeft cap",
		reachingPaths: "one walkBudget per Props/value bounding pass (shared with toJSONWalk's budget)"},
	{fn: "inlineTreeDefs", file: "cache.go", class: textTree,
		factors:       "JSON tree nodes (each definition inlined once)",
		binds:         "the seen/inlined map[string]bool sets: a name already inlined is emitted as a reference, so the output is linear in the definition set",
		reachingPaths: "one: seen/inlined created per SchemaCache self-containment splice"},
	{fn: "build", file: "schema.go", class: textTree,
		factors:       "aschema text nodes, bounded by depth",
		binds:         "the parsed aschema is a TREE (each occurrence is its own text); depth>=maxDepth caps recursion",
		reachingPaths: "one: per Parse; nested builders share depth but each occurrence is its own text"},
}

// graphCostMarker is a source pattern that marks a walk carrying graph-cost
// state. Every occurrence in a source line (not a comment) must be attributable
// to a rowed walk or an allow-listed non-walk datum.
var graphCostMarkers = []string{
	"allowance",
	"walkBudget",
	"map[nodePair]",
	"map[*schemaNode]bool",
	"map[*schemaNode]struct{}",
	"map[*SchemaNode]struct{}",
	"map[reflect.Type]bool",
	"map[reflect.Type]seenForm",
	"defer delete(",
}

// nonWalkMarkerUses are source substrings that match a graphCostMarker pattern
// but are NOT budgeted walks: data maps carried on the builder/ctx, per-Go-type
// caches, and the like. Each is allow-listed with the reason it is not a walk.
// A new marker occurrence that matches neither a rowed walk nor one of these
// fails the completeness guard — which is the point.
var nonWalkMarkerUses = map[string]string{
	"custom map[*schemaNode]*customWiring":          "a DATA map of node->custom wiring, not a visited set",
	"custom      map[*schemaNode]*customWiring":     "a DATA map of node->custom wiring, not a visited set",
	"customMatch map[*schemaNode]string":            "a DATA map of node->matched-custom-name, not a visited set",
	"overlayDone map[*schemaNode]bool":              "presence state recording which nodes' overlay ran, carried on the builder across nests; the WALK that fills it (overlayInheritedCustom) is rowed",
	"building     map[*schemaNode]struct{}":         "the record-in-progress set for build-time cycle detection, carried on the builder; not a per-walk visited",
	"building:   make(map[*schemaNode]struct{})":    "initializes the builder's record-in-progress set (two sites: schema.go and cache.go)",
	"b.overlayDone = make(map[*schemaNode]bool)":    "re-inits the builder overlay presence state",
	"b.customMatch = make(map[*schemaNode]string)":  "re-inits the builder custom-match data map",
	"b.custom = make(map[*schemaNode]*customWiring": "re-inits the builder custom data map",
	"b.building = make(map[*schemaNode]struct{})":   "re-inits the builder record-in-progress set",
	"seen := make(map[reflect.Type]seenForm)":       "SchemaFor's inferType memo init; the walk (inferType) is rowed",
	"seen map[reflect.Type]seenForm":                "inferType/inferRecord/inferField memo parameter; inferType is rowed",
	"collectFields(t, make(map[reflect.Type]bool))": "inits collectFieldsRaw's visited; the walk is rowed",
	"visited map[reflect.Type]bool":                 "collect/collectFieldsRaw visited parameter; both rowed",
	"make(map[reflect.Type]bool)":                   "inits a Go-type walk visited set; the walks (collect/collectFieldsRaw) are rowed",
	"sync.Map // map[reflect.Type]":                 "a per-Go-type compiled-codec cache, amortized and keyed by fixed types; not a walk",
}

// TestInvariant_BudgetedWalkCensus is the enumeration guard. It derives the walk
// set from source two ways and requires every member to carry a cost expression,
// and it enforces that a schemaDAG walk's bound caps the PRODUCT (a memo or a
// budget), never a lone factor like depth.
func TestInvariant_BudgetedWalkCensus(t *testing.T) {
	files := censusSourceFiles(t)
	rowByFn := make(map[string]budgetedWalk, len(budgetedWalks))
	for _, w := range budgetedWalks {
		if _, dup := rowByFn[w.fn]; dup {
			t.Fatalf("duplicate census row for %q", w.fn)
		}
		rowByFn[w.fn] = w
	}

	// Guard A — rot: every rowed walk's function still exists in its file. A row
	// whose walk was renamed or deleted guards nothing.
	for _, w := range budgetedWalks {
		src := readFile(t, w.file)
		q := regexp.QuoteMeta(w.fn)
		// Match a top-level func, a method, or a recursive closure
		// (`var name func(` / `name = func(`) — collect in reflect.go is the last.
		defined := regexp.MustCompile(`func (\([^)]*\) )?`+q+`\(`).MatchString(src) ||
			regexp.MustCompile(`\b`+q+` func\(`).MatchString(src) ||
			regexp.MustCompile(`\b`+q+` = func\(`).MatchString(src)
		if !defined {
			t.Errorf("census rows %q in %s but no such func is defined there — the row rotted (renamed or removed?)", w.fn, w.file)
		}
		// Every row must name its reaching paths: a walk reached by more than one
		// construction path can be bounded on one and not another, invisible to
		// factors/binds. A blank reachingPaths is an incomplete row.
		if strings.TrimSpace(w.reachingPaths) == "" {
			t.Errorf("census row %q has no reachingPaths — name every construction path that reaches it and the bound each carries", w.fn)
		}
	}

	// Guard B — product binding: a schemaDAG walk MUST cap the product with a
	// memo or a budget; depth alone cannot bound a DAG (the fan-out is reachable
	// flat). goTypeDAG must justify with compile-time fixedness.
	for _, w := range budgetedWalks {
		switch w.class {
		case schemaDAG:
			b := strings.ToLower(w.binds)
			if !strings.Contains(b, "memo") && !strings.Contains(b, "budget") &&
				!strings.Contains(b, "shared") && !strings.Contains(b, "leaves") &&
				!strings.Contains(b, "tree") {
				t.Errorf("schemaDAG walk %q binds with %q — a shared graph walk must name a MEMO or a BUDGET (or justify a tree/leaf structure), not a lone factor", w.fn, w.binds)
			}
			if strings.TrimSpace(strings.ToLower(w.binds)) == "the depth>=maxdepth cap" {
				t.Errorf("schemaDAG walk %q is bound by depth alone; depth does not bound a DAG", w.fn)
			}
		case goTypeDAG:
			if !strings.Contains(strings.ToLower(w.binds), "compile") {
				t.Errorf("goTypeDAG walk %q must justify its bound by compile-time fixedness (attacker cannot grow a Go type at runtime)", w.fn)
			}
		}
	}

	// Guard C — recursion completeness: every function that recurses over the
	// schema graph (*schemaNode / *SchemaNode) must be rowed. This catches a new
	// walk that carries NO cost marker at all (the coerceTreeDefaults shape).
	for _, fn := range selfRecursiveSchemaWalks(t, files) {
		if _, ok := rowByFn[fn]; !ok {
			t.Errorf("function %q recurses over the schema graph but is not in the budgeted-walk census; add a row naming its cost factors and the bound that caps their product", fn)
		}
	}

	// Guard D — marker completeness: every graph-cost marker occurrence belongs
	// to a rowed walk or an allow-listed non-walk datum. This catches a new walk
	// that carries cost state (the dangerous case) even under mutual recursion
	// the recursion scan cannot see (minBytes, checkCompat, resolveNode).
	rowedFiles := make(map[string]bool)
	for _, w := range budgetedWalks {
		rowedFiles[w.file] = true
	}
	for _, f := range files {
		src := readFile(t, f)
		for i, line := range strings.Split(src, "\n") {
			code := line
			if c := strings.Index(code, "//"); c >= 0 {
				code = code[:c] // ignore comments
			}
			for _, m := range graphCostMarkers {
				if !strings.Contains(code, m) {
					continue
				}
				if attributed(code, rowedFiles[f], nonWalkMarkerUses) {
					continue
				}
				t.Errorf("%s:%d carries graph-cost marker %q but is not attributable to a rowed walk or an allow-listed non-walk:\n    %s\n  Row the walk with its cost factors, or allow-list the datum with why it is not a walk.", f, i+1, m, strings.TrimSpace(line))
			}
		}
	}
}

// attributed reports whether a marker-bearing code line belongs to a rowed walk
// (the file hosts one) or an explicitly allow-listed non-walk datum.
func attributed(code string, fileHostsRowedWalk bool, allow map[string]string) bool {
	for substr := range allow {
		if strings.Contains(code, substr) {
			return true
		}
	}
	// A line in a file that hosts a rowed walk, declaring/using that walk's
	// state (allowance, walkBudget, path/done/seen/visited maps over graph
	// nodes), is attributed to the walk. The allow-list above carves out the
	// NON-walk data maps in those same files, so this does not blanket-accept.
	return fileHostsRowedWalk
}

// selfRecursiveSchemaWalks derives, from source, the set of functions that take
// a *schemaNode or *SchemaNode and call themselves — the schema-graph walks a
// marker scan would miss if they carried no cost state. Mutual-recursion walks
// (minBytes, checkCompat, resolveNode) are caught by the marker guard instead.
func selfRecursiveSchemaWalks(t *testing.T, files []string) []string {
	t.Helper()
	sig := regexp.MustCompile(`func (?:\([^)]*\) )?(\w+)\([^)]*\*(?:schemaNode|SchemaNode)\b`)
	var out []string
	seen := map[string]bool{}
	for _, f := range files {
		src := readFile(t, f)
		for _, m := range sig.FindAllStringSubmatchIndex(src, -1) {
			name := src[m[2]:m[3]]
			body := src[m[1]:]
			if nxt := strings.Index(body, "\nfunc "); nxt >= 0 {
				body = body[:nxt]
			}
			if regexp.MustCompile(`\b` + regexp.QuoteMeta(name) + `\(`).MatchString(body) {
				if !seen[name] {
					seen[name] = true
					out = append(out, name)
				}
			}
		}
	}
	return out
}

func readFile(t *testing.T, f string) string {
	t.Helper()
	b, err := os.ReadFile(f)
	if err != nil {
		t.Fatalf("reading %s: %v", f, err)
	}
	return string(b)
}

// sectionBanner matches the marker that delimits one merged-in file inside a
// consolidated test file. It requires a *_test.go NAME: the same
// `// ---------- x ----------` shape is also used for ordinary subsection
// headings within a file ("the shapes", "triggers", "the WIDTH axis"), and
// terminating on one of those truncates a section to a few lines — which reads
// as "the claim is absent" or, worse, as a claim satisfied by a neighbour.
var sectionBanner = regexp.MustCompile(`(?m)^// -{10} (\S+_test\.go) -{10}$`)

// testFileSection returns the part of a consolidated test file that came from
// orig, i.e. the text between orig's banner and the next file banner.
//
// A few guards below derive a claim from ONE file's source. Those files are now
// sections of larger ones, and reading the whole enclosing file instead would let
// an unrelated section satisfy the claim: the DAG-cost guard looks for
// "cache.Parse(", which its own section uses twice and the rest of schema_test.go
// uses fifty-six more times. So the section is what gets read, and a missing
// banner is fatal rather than silently the whole file.
func testFileSection(t *testing.T, file, orig string) string {
	t.Helper()
	src := readFile(t, file)
	ms := sectionBanner.FindAllStringSubmatchIndex(src, -1)
	for i, m := range ms {
		if src[m[2]:m[3]] != orig {
			continue
		}
		end := len(src)
		if i+1 < len(ms) {
			end = ms[i+1][0]
		}
		return src[m[1]:end]
	}
	t.Fatalf("%s has no %q section banner — the consolidated-file markers moved, and a guard reading the wrong section reads as coverage", file, orig)
	return ""
}

// minBytesConstructionSite rows one place the min-bytes walk STATE is constructed
// — a reaching path of the walk. The walk is one, but a schema reaches it by
// building a container, finalizing a forward reference, resolving, or compiling a
// dropped-field skip, and the container FACTOR is bounded only if each of those
// constructs the walk once per OPERATION and shares it across that operation's
// containers; a fresh construction per container is the bug the forward/backward
// split exposed. The `context` is a source substring the construction line must
// contain, so a construction that drifts to a per-container scope fails to match.
type minBytesConstructionSite struct {
	file    string
	context string // a substring uniquely identifying the construction line
	scope   string // what operation the constructed walk is shared across
	// factors is what this path's ONE walk is shared ACROSS, and every entry
	// carries the cell that MEASURES it. A row may not state its bound in prose: a
	// sentence is a claim, and the whole purpose of this census is to reject claims.
	// The seventh row of this table once read "cross-record cost is wire-bounded" —
	// true, and it bounded nothing, because reaching a record costs O(1) wire bytes
	// while draining a full allowance, and no cell drove a record count.
	//
	// Empty only when exempt is set.
	factors []reachFactor
	// exempt records why this construction needs no measured factor. Legal only
	// for a site that is not shared across anything, and the guard checks that
	// premise itself rather than believing this string.
	exempt string
}

// reachFactor is one caller-chosen count a shared walk is spread across, with the
// cell that drives it.
//
// values must hold at least TWO distinct numbers. One value can only ask "does
// this finish?" — it cannot tell a bound from a cost that is merely linear with a
// small constant, and it cannot see a factor it never varies. The cell reads its
// values FROM here rather than from its own constant, so "the cell drives two
// values" is the same fact read once, not a second claim to be checked.
type reachFactor struct {
	name   string
	values []int
	// drive runs the reaching path at one value of the factor. It returns an
	// error rather than taking a *testing.T because it executes inside the
	// watchdog goroutine, where t.Fatal would be illegal.
	drive func(n int) error
}

// reachCounts are the two values every reaching-path factor is driven at. The
// low one establishes the single-unit cost; the high one is far enough above it
// that a per-unit walk shows up as a multiple rather than as noise.
var reachCounts = []int{1, 48}

const reachLevels = 26 // SCC depth: deep enough that one walk exhausts its allowance

var minBytesConstructionSites = []minBytesConstructionSite{
	{file: "deser.go", context: "return newMinBytesWalk().minBytesOf(n)",
		scope:  "standalone schemaMinBytes: ONE node, outside any container loop (the only fresh-per-call form)",
		exempt: "shared across nothing — it is the fresh-per-call form itself, and the guard below derives from source that no production code calls it, so there is no count for a cell to drive"},

	{file: "schema.go", context: "minBytes:   newMinBytesWalk()",
		scope: "the builder's b.minBytes seeded in Parse — the BUILD path (backward refs resolve to a built node here)",
		factors: []reachFactor{{name: "containers per parse (backward refs)", values: reachCounts,
			drive: func(n int) error {
				_, err := Parse(nContainersOverWiredSCC(n, reachLevels))
				return err
			}}}},

	{file: "schema.go", context: "b.minBytes = newMinBytesWalk()",
		scope: "lazy seed at the root's first build, before any nest, so a directly-constructed (white-box) builder still shares one walk across the build path",
		factors: []reachFactor{{name: "containers per parse (build path, same factor the Parse seed carries)", values: reachCounts,
			drive: func(n int) error {
				_, err := Parse(nContainersOverWiredSCC(n, reachLevels))
				return err
			}}}},

	{file: "schema.go", context: "mbw := newMinBytesWalk()",
		scope: "one walk before finalize's container-fixup loop — the FINALIZE path (forward refs)",
		factors: []reachFactor{{name: "containers per parse (forward refs)", values: reachCounts,
			drive: func(n int) error {
				_, err := Parse(nContainersOverSCC(n, reachLevels))
				return err
			}}}},

	{file: "cache.go", context: "minBytes:   newMinBytesWalk()",
		scope: "SchemaCache's builder b.minBytes — the build path via the cache",
		factors: []reachFactor{{name: "containers per SchemaCache.Parse", values: reachCounts,
			drive: func(n int) error {
				var c SchemaCache
				_, err := c.Parse(nContainersOverWiredSCC(n, reachLevels))
				return err
			}}}},

	{file: "resolve.go", context: "minBytes: newMinBytesWalk()",
		scope: "resolveCtx.minBytes, shared across one Resolve AND across every dropped-field skip that resolution compiles — including the record compiles deferred to decode time, which join this same walk rather than starting their own",
		factors: []reachFactor{
			{name: "containers per resolution", values: reachCounts,
				drive: func(n int) error {
					scc := nContainersOverSCC(n, reachLevels)
					w := MustParse(scc)
					r := MustParse(strings.Replace(scc,
						`{"type":"record","name":"Root","fields":[`,
						`{"type":"record","name":"Root","fields":[{"name":"extra","type":"int","default":0},`, 1))
					_, err := Resolve(w, r)
					return err
				}},
			// The factor the old skip row asserted instead of measuring. Each
			// reference to one record compiles its own skipRecordFields, so a
			// per-record walk multiplied the allowance by a count the schema
			// picks while each compile was reached with a single wire byte.
			{name: "records compiled per resolution (lazy, at decode)", values: reachCounts,
				drive: func(n int) error {
					top := nRecordsOverSCC(n, reachLevels)
					w := MustParse(`{"type":"record","name":"Outer","fields":[{"name":"drop","type":` + top + `},{"name":"keep","type":"int"}]}`)
					r := MustParse(`{"type":"record","name":"Outer","fields":[{"name":"keep","type":"int"}]}`)
					res, err := Resolve(w, r)
					if err != nil {
						return err
					}
					var out struct {
						Keep int32 `avro:"keep"`
					}
					_, err = res.Decode(nRecordsOverSCCWire(n, reachLevels), &out)
					return err
				}},
		}},
}

// nRecordsOverSCC references ONE record definition nrecs times, each reference
// holding one array over a shared cyclic SCC. Every reference compiles its own
// skipRecordFields, so this drives the RECORD count with the container count
// held at one — the axis the container cell holds constant.
func nRecordsOverSCC(nrecs, levels int) string {
	var b strings.Builder
	b.WriteString(`{"type":"record","name":"Top","fields":[`)
	for i := range levels {
		next := fmt.Sprintf("L%d", i+1)
		if i == levels-1 {
			next = "L0"
		}
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"name":"d%d","type":{"type":"record","name":"L%d","fields":[{"name":"f0","type":["null","%s"]},{"name":"f1","type":["null","%s"]}]}}`, i, i, next, next)
	}
	b.WriteString(`,{"name":"r0","type":{"type":"record","name":"R","fields":[{"name":"z","type":{"type":"array","items":"L0"}}]}}`)
	for j := 1; j < nrecs; j++ {
		fmt.Fprintf(&b, `,{"name":"r%d","type":"R"}`, j)
	}
	b.WriteString(`]}`)
	return b.String()
}

// nRecordsOverSCCWire is the minimal wire that reaches every record: two null
// union bytes per SCC level, then one empty-array byte per reference. Reaching
// the nth compile costs ONE byte, which is exactly why "the wire bounds how
// many compile" bounds the count and not the work.
func nRecordsOverSCCWire(nrecs, levels int) []byte {
	w := make([]byte, 0, 2*levels+nrecs+1)
	for range levels {
		w = append(w, 0, 0)
	}
	for range nrecs {
		w = append(w, 0)
	}
	return append(w, 2) // keep = 1
}

// reachScaleTol and reachScaleFloor separate "the walk is shared" from "the
// walk is rebuilt per unit". A shared walk pays its allowance once, so the high
// and low cells differ only by the genuinely linear part (a longer schema to
// parse, more wire bytes to read). A per-unit walk multiplies the allowance by
// the factor, which at reachCounts' spread is more than an order of magnitude
// past this. The floor keeps a fast cell from being judged on a ratio of noise.
const (
	reachScaleTol   = 4
	reachScaleFloor = 400 * time.Millisecond
)

// TestInvariant_EveryReachingPathBoundIsMeasured is the rule that a bound must
// be MEASURED, not stated. Every reaching path names the counts its one walk is
// shared across, and every count names a cell that drives it at two or more
// distinct values. Both directions are mechanical, and the second is the one
// that bites:
//
//   - a row with no cell fails. Prose describing why a cost is bounded is a
//     claim, and this census exists to reject claims.
//   - a cell that holds its row's factor CONSTANT fails. One value asks only
//     "does this finish?", which a cost merely linear in the factor also
//     answers. The skip path was rowed with a true sentence about the wire and
//     no cell that varied a record count, and the unbounded factor lived under
//     it.
//
// Attack it both ways: delete a factor's second value and the constant-factor
// arm fires; drop a row's factors for a sentence and the no-cell arm fires;
// rebuild any shared walk per unit and the scale arm fires.
func TestInvariant_EveryReachingPathBoundIsMeasured(t *testing.T) {
	for _, site := range minBytesConstructionSites {
		if site.exempt != "" {
			if len(site.factors) != 0 {
				t.Errorf("%s (%s) is exempt AND names factors — say which it is", site.file, site.context)
			}
			// The exemption's premise is not taken on trust: the guard below
			// derives from source that nothing in production calls the
			// standalone form, which is what makes "shared across nothing" true.
			continue
		}
		if len(site.factors) == 0 {
			t.Errorf("%s (%s) states its bound only in prose (%q).\nA reaching path must name the count its walk is shared across and a cell that drives it; a sentence is not a measurement.",
				site.file, site.context, site.scope)
			continue
		}
		for _, f := range site.factors {
			name := site.file + "/" + f.name
			seen := make(map[int]bool, len(f.values))
			for _, v := range f.values {
				seen[v] = true
			}
			if len(seen) < 2 {
				t.Errorf("%s: cell drives %d distinct value(s) of its own factor %v.\nA bound is a claim about how cost RESPONDS to this count, and one value cannot tell a bound from a linear cost.",
					name, len(seen), f.values)
				continue
			}
			if f.drive == nil {
				t.Errorf("%s: factor has values but no cell to drive them", name)
				continue
			}
			lo, hi := f.values[0], f.values[0]
			for _, v := range f.values {
				lo = min(lo, v)
				hi = max(hi, v)
			}
			times := make(map[int]time.Duration, len(f.values))
			for _, v := range f.values {
				start := time.Now()
				// Each value must also be bounded on its own — the watchdog
				// catches an unbounded path that would never return to be timed.
				wantTerminate(t, fmt.Sprintf("%s=%d", name, v), func() error { return f.drive(v) })
				times[v] = time.Since(start)
			}
			floor := raceInflated(reachScaleFloor)
			if lim := max(reachScaleTol*times[lo], floor); times[hi] > lim {
				t.Errorf("%s: cost scales with the factor — %v at %d vs %v at %d (limit %v).\nA walk shared across this count pays its allowance once; this is the shape of one walk per unit.",
					name, times[hi], hi, times[lo], lo, lim)
			}
			t.Logf("%s: %v at %d, %v at %d", name, times[lo], lo, times[hi], hi)
		}
	}
}

// TestInvariant_MinBytesReachingPaths is the reaching-path guard for the one
// budgeted walk reached by more than one construction path. It derives every
// newMinBytesWalk() construction from source and requires each to be a rowed,
// per-operation site; it forbids a fresh-walk-per-call anywhere but the
// standalone schemaMinBytes; and it forbids a production caller of schemaMinBytes
// (which would rebuild the walk on every call). Attack it both ways: add a
// newMinBytesWalk() at a new site -> unrowed; call schemaMinBytes in production
// -> a fresh-per-call path.
func TestInvariant_MinBytesReachingPaths(t *testing.T) {
	files := censusSourceFiles(t)

	// Derive every construction (a newMinBytesWalk() CALL, not the func decl) and
	// require each to match a rowed site's context substring.
	for _, f := range files {
		src := readFile(t, f)
		for i, line := range strings.Split(src, "\n") {
			if !strings.Contains(line, "newMinBytesWalk()") || strings.Contains(line, "func newMinBytesWalk(") {
				continue
			}
			rowed := false
			for _, s := range minBytesConstructionSites {
				if s.file == f && strings.Contains(line, s.context) {
					rowed = true
					break
				}
			}
			if !rowed {
				t.Errorf("%s:%d constructs a min-bytes walk that is not a rowed reaching path:\n    %s\n  Row it in minBytesConstructionSites with the operation it is shared across, or (if it is per-container) share an existing per-operation walk instead.", f, i+1, strings.TrimSpace(line))
			}
		}
	}

	// Every rowed construction must still exist (rot check, the other direction).
	for _, s := range minBytesConstructionSites {
		src := readFile(t, s.file)
		if !strings.Contains(src, s.context) {
			t.Errorf("minBytesConstructionSites rows %q in %s but the line is gone — the path rotted", s.context, s.file)
		}
	}

	// The fresh-walk-per-CALL form (constructor immediately consumed) is legal
	// only for the standalone single-node schemaMinBytes. Anywhere else it is a
	// per-container path — exactly what the build finding was.
	fresh := occurrences(t, files, "newMinBytesWalk().minBytesOf(")
	total := 0
	for f, lines := range fresh {
		for _, ln := range lines {
			total++
			if f != "deser.go" {
				t.Errorf("%s:%d consumes a FRESH min-bytes walk in one call — the container factor reappears if this runs per container; share a per-operation walk", f, ln)
			}
		}
	}
	if total != 1 {
		t.Errorf("expected exactly ONE fresh-per-call min-bytes walk (the standalone schemaMinBytes); found %d — a new one is a per-container path unless proven otherwise", total)
	}

	// schemaMinBytes is that fresh-per-call standalone; a PRODUCTION caller would
	// rebuild the walk each call, so there must be none (tests may use it for a
	// single node). The set of production callers is derived, not assumed.
	for _, f := range files {
		src := readFile(t, f)
		for i, line := range strings.Split(src, "\n") {
			code := line
			if c := strings.Index(code, "//"); c >= 0 {
				code = code[:c]
			}
			if strings.Contains(code, "schemaMinBytes(") && !strings.Contains(code, "func schemaMinBytes(") {
				t.Errorf("%s:%d calls schemaMinBytes in production — it builds a fresh walk per call; use a shared per-operation walk (b.minBytes / ctx.minBytes / the finalize or skip mbw)", f, i+1)
			}
		}
	}
}

// ---- cost cells: the same measured-bound rule, one level out ---------------

// The reaching-path rule above says a bound must be MEASURED — a cell driving
// its factor at two or more values, because one value cannot tell a bound from a
// cost that is merely linear. That rule was stated for the walk CONSTRUCTION
// sites and then not applied to the wall-clock cost cells, which is the same
// defect the rule exists to catch: five cells drove one value each and the suite
// was green. costCell is the registry that closes it — a cell's magnitudes live
// HERE and the cell reads them, so the two cannot disagree, and the source
// derivation below means a new cost cell cannot quietly skip the registry.
type costCell struct {
	fn     string // the test function
	factor string // the caller-chosen magnitude its bound claims to cap
	values []int  // what the cell drives — at least two distinct, unless exempt
	// exempt is why one magnitude suffices. It is a CLAIM like any other, so it
	// must name what the cell asserts INSTEAD of a wall-clock bound; the guard
	// cross-checks it against whether the cell actually takes the wall-clock
	// harness, so an exemption cannot be pasted onto a timing cell.
	exempt string
	// carrier names what carries this cell's magnitude when it is NOT schema text.
	// The derivation below finds cells by their calls to a cost GENERATOR (a func
	// turning an int into schema text), which is the shape almost every magnitude
	// here takes — but not all: a Go type, a hand-built node, a wire buffer. Those
	// cells cannot be DISCOVERED by that derivation, so they are rowed by hand and
	// this field is the declaration that they were. It is the one thing in this
	// registry the source cannot check for us; everything else about such a row is
	// checked exactly as for any other cell.
	carrier string
	// scaleTol bounds cost(max)/cost(min). Every one of these factors measured
	// FLAT with a correct bound even where the schema TEXT grows with the factor
	// (width 80 -> 8000 grows the text 65x and the parse 1.4x, because the walk
	// dominates and the allowance caps it), so a small tolerance is honest.
	scaleTol int
	// floor is the largest cost the BOUND ITSELF permits at the top of the range,
	// and the limit is max(scaleTol*cost(min), floor). For a cell whose shapes are
	// all memoizable it is just machine noise; for one that includes an
	// UN-memoizable shape it is one exhausted allowance (~120ms measured), because
	// a cyclic subtree cannot be cached and legitimately walks until maxMinBytesWork
	// stops it — that is the bound ENGAGING, not the cost scaling, and a cell
	// spanning both regimes has to be judged against the looser of them. It stays
	// orders of magnitude under an unbounded walk, which is seconds.
	floor time.Duration
}

var costCells = []costCell{
	{fn: "TestInvariant_EveryMinBytesEntryPointIsBounded",
		factor: "dagNested DEPTH — the PATHS factor: without the memo this is 2^depth, so 13 vs 26 is a 8192x separation",
		values: []int{13, 26}, scaleTol: 8, floor: 25 * time.Millisecond},

	{fn: "TestInvariant_CyclicWalkCostIsBoundedByWork",
		factor: "dagWideSCC WIDTH — the CHILDREN factor: a per-NODE charge makes cost allowance x width, a per-CHILD charge makes it flat",
		values: []int{80, 8000}, scaleTol: 4, floor: 400 * time.Millisecond},

	{fn: "TestInvariant_WideCyclicWalkReachesEveryEntryPoint",
		factor: "dagWideSCC WIDTH, across the entry points that do not take the schema from the caller",
		values: []int{80, 8000}, scaleTol: 4, floor: 400 * time.Millisecond},

	{fn: "TestInvariant_MetadataSurfacesBoundedByWidth",
		// Named for what it drives, after the old name was executed and found
		// false. It does NOT measure the metadata walk's node budget: a PARSED
		// schema is deduped before it reaches that walk, so disabling takeNode
		// entirely leaves this cell green. What it exercises is the Root+Schema
		// ROUND TRIP, whose last step is a re-Parse of the rendered text, putting
		// the min-bytes charge on its path — neutering that charge reds it. The
		// node budget's own cells hand-build the trees Parse cannot express.
		factor: "dagWideSCC WIDTH through the metadata surfaces — Root+Schema (render, marshal, re-Parse), String and Canonical",
		values: []int{80, 8000}, scaleTol: 4, floor: 400 * time.Millisecond},

	{fn: "TestInvariant_MinBytesContainerCountBounded",
		factor: "CONTAINER count. Its two generator calls vary reference DIRECTION (forward/backward), which is a different axis — the count itself was pinned at 220",
		values: []int{1, 220}, scaleTol: 4, floor: 400 * time.Millisecond},

	{fn: "TestInvariant_EmbedDiamondCostFactors",
		// The goTypeDAG walk. Its cost is paths x CALLS and the two collectors
		// differ on the second factor — the decode mapping is memoized per
		// reflect.Type, SchemaFor's collector is not — so a cell driving depth
		// alone measures the factor they AGREE on and misses the one they do
		// not. That is what closed this gap: not the depth number, which was
		// already measured, but the second factor.
		factor:  "sibling-embed DAG depth (the paths factor, 2^depth) crossed with CALL COUNT (the amortization factor, where the two collectors diverge)",
		carrier: "a Go TYPE — the depth is carried by which declared embed-diamond type the cell instantiates, so no schema-text generator appears and the derivation cannot discover this cell. Rowed by hand; the depths the row names are asserted against the types inside the cell",
		values:  []int{8, 12}, scaleTol: 32, floor: 2 * time.Second},

	{fn: "TestDoSBattery_C9_CustomTypeParseCost",
		factor: "backward-reference chain LENGTH — the per-parse custom-match memo's factor. Without it every stamp walk from Ri reaches R0..R(i-1), so the total is quadratic in this count while the schema text is linear in it",
		values: []int{3000, 6000}, scaleTol: 4, floor: 200 * time.Millisecond},

	{fn: "TestDoSBattery_OCF_C1_Header",
		// In package ocf, which cannot import this registry. Its magnitudes are
		// named vocabularies in that file and the guard checks these values
		// appear there; see the cross-package arm of the guard for why the tie
		// is checked that way round rather than by the cell reading its row.
		factor: "the header DAG's three magnitudes — reference DEPTH, cyclic record WIDTH, and CONTAINER count — each driven at two values",
		values: []int{26, 30, 8000, 16000, 220, 440}, scaleTol: 4, floor: 400 * time.Millisecond},

	{fn: "TestMatrix_NestedStrayContainerKeyLinearCost",
		// Times inline rather than through a named helper, which is why the
		// harness vocabulary includes time.Since: a cell that measures its own
		// clock is still a timing cell, and an exemption must not be able to
		// sit on one by avoiding the helpers.
		factor: "nested stray-key DEPTH — the metadata walker's per-ancestor re-validation, whose absence is exponential. The absolute-ceiling half runs at depth 20 and the growth half compares 400 against 800",
		values: []int{20, 400, 800}, scaleTol: 3, floor: 100 * time.Millisecond},

	// Value oracles. Named explicitly rather than left to a reader to re-derive:
	// each varies shapes to check an ANSWER and asserts equality, never
	// wall-clock, so a second magnitude would measure nothing about a bound.
	{fn: "TestInvariant_MemoAgreesWithUnmemoizedWalk",
		exempt: "value oracle: compares the memoized walk's result against an un-memoized recomputation per node. Its oracle is equality of VALUES, and a wrong memo is FASTER, so timing is exactly what cannot settle it"},
	{fn: "TestInvariant_DagMinBytesIsExactAtScale",
		exempt: "value oracle: asserts the minimum a shared DAG reports equals the minimum its expanded TREE reports. Equality, not cost"},
	{fn: "TestInvariant_MinBytesSelfReadable",
		exempt: "value oracle: asserts a bound derived from the walk still admits wire this package's own encoder produces. Accept/reject, not cost"},
	{fn: "TestInvariant_SharingDoesNotChangeMinBytes",
		exempt: "value oracle: asserts sharing one walk across containers does not change the ANSWER. It already sweeps fan x levels; the sweep is over SHAPES to find a disagreement, not magnitudes to time"},
	{fn: "TestRegression_ZeroByteItemCapStillHolds",
		exempt: "value oracle: asserts the zero-byte-item CAP still rejects an over-cap count both with and without a drained stand-in. Its use of the SCC generator is to exhaust the allowance — a state, not a magnitude — and its assertion is accept/reject, so a second depth would measure nothing about a bound"},
	{fn: "TestRegression_ZeroMinimumContainerAfterDrainedAllowance",
		exempt: "value oracle: encode-implies-decode across the two field ORDERS that decide whether the zero-minimum container is built before or after the allowance drains. The axis is order, not magnitude; the SCC depth only has to be enough to drain"},
	{fn: "TestInvariant_SharedSchemaNodeWalkedOnce",
		factor: "dagNested/dagFlat/dagSelfRecursive/dagSingleSCC DEPTH — the PATHS factor across all four sharing shapes and both fans. Its name reads like a value oracle, and the hand derivation classified it as one; it takes the wall-clock harness, so the exemption cross-check caught it",
		// Two of its four shapes are CYCLIC and cannot be memoized at all, so
		// they climb to one exhausted allowance between the two depths (~1.9ms
		// at 13, ~120ms at 26) while the memoizable two stay flat at ~200us.
		// The floor is that allowance; without the charge the same shapes run
		// for seconds.
		values: []int{13, 26}, scaleTol: 8, floor: 500 * time.Millisecond},

	{fn: "TestDoSBattery_C6_MetadataWalk",
		factor: "dagNested/dagFlat DEPTH — the PATHS factor through the metadata + resolve + compat entry points. Missed by the hand derivation entirely; only the source scan found it",
		values: []int{13, 26}, scaleTol: 8, floor: 25 * time.Millisecond},
}

// costFactorValues returns the magnitudes the named cost cell must drive. A cell
// calls it with its OWN name, so its values cannot drift from the row the guard
// reads; TestInvariant_EveryCostCellDrivesItsFactor checks from source that
// every rowed timing cell does exactly that.
func costFactorValues(t *testing.T, fn string) []int {
	t.Helper()
	for _, c := range costCells {
		if c.fn == fn {
			if c.exempt != "" {
				t.Fatalf("%s is rowed EXEMPT but is asking for factor values", fn)
			}
			return c.values
		}
	}
	t.Fatalf("%s is not rowed in costCells — a cost cell must declare the factor it drives", fn)
	return nil
}

// wantCostDoesNotScale asserts that the named cell's operation costs about the
// same at the top of its factor's range as at the bottom. build takes the
// magnitude and returns the thunk to TIME: everything the magnitude needs but
// the bound does not own — generating a schema whose TEXT is linear in the
// factor, parsing it when the bound under test is downstream of the parse —
// belongs in build, outside the returned closure. Putting it inside is not a
// rounding error: the metadata cell had its MustParse in the timed region, and
// since the parse of a width-8000 schema dominates the walk that follows, the
// cell moved when the PARSE's bound was neutered and sat still when its own was.
func wantCostDoesNotScale(t *testing.T, fn, label string, build func(n int) func() error) {
	t.Helper()
	var row costCell
	for _, c := range costCells {
		if c.fn == fn {
			row = c
		}
	}
	vals := costFactorValues(t, fn)
	lo, hi := vals[0], vals[0]
	for _, v := range vals {
		lo, hi = min(lo, v), max(hi, v)
	}
	times := make(map[int]time.Duration, len(vals))
	for _, v := range vals {
		run := build(v)
		start := time.Now()
		wantTerminate(t, fmt.Sprintf("%s/%s=%d", label, row.factor, v), run)
		times[v] = time.Since(start)
	}
	floor := raceInflated(row.floor)
	if lim := max(time.Duration(row.scaleTol)*times[lo], floor); times[hi] > lim {
		t.Errorf("%s: cost scales with the factor — %v at %d vs %v at %d (limit %v).\nThe bound claims to cap this magnitude; a cost that grows with it is the bound missing, not a slow machine.",
			label, times[hi], hi, times[lo], lo, lim)
	}
}

// costGenerators derives the cost-generator vocabulary BY SHAPE, and returns it
// with the file each was found in.
//
// A cost generator is a function that turns a caller-chosen MAGNITUDE into AVRO
// SCHEMA TEXT: at least one int parameter, exactly one string result, and a body
// that writes an Avro `type` key. All three are structural. The `type` key is
// what separates a schema builder from an integer formatter of the same
// signature — there are several of those, and they drive nothing.
//
// SCOPE, stated because the previous three attempts at this class each fixed one
// level and hand-scoped the next. It reads every *_test.go file of this module,
// in every package. What it therefore CANNOT see:
//
//   - a magnitude that reaches production as something other than schema text —
//     a []byte wire builder, a reflect.Type, a hand-built *schemaNode. Three of
//     the value oracles rowed below are exactly that shape.
//   - a generator that composes schema text without writing a `type` key itself.
//   - a magnitude spelled as a package-level constant with no int parameter,
//     which is a generator of ONE size and so cannot drive a factor.
//   - anything outside this module.
//
// The guard against the derivation silently collapsing is a floor on what it
// finds AND a requirement that more than one package is represented — the
// single-package scope is the specific way the previous version failed.
func costGenerators(t *testing.T, src map[string]string) (map[string]bool, map[string]string) {
	t.Helper()
	decl := regexp.MustCompile(`(?m)^func ([A-Za-z_][A-Za-z0-9_]*)\(([^)]*)\) string \{`)
	intParam := regexp.MustCompile(`\bint\b`)
	gens := map[string]bool{}
	where := map[string]string{}
	for f, s := range src {
		code := blankCode(s)
		for _, m := range decl.FindAllStringSubmatchIndex(code, -1) {
			name := code[m[2]:m[3]]
			if !intParam.MatchString(code[m[4]:m[5]]) {
				continue
			}
			// Body extent by brace matching over the blanked view, then read
			// the RAW bytes for the `type` key — the key lives in a string
			// literal, which the blanked view has erased.
			depth, end := 0, -1
			for k := m[1] - 1; k < len(code) && end < 0; k++ {
				switch code[k] {
				case '{':
					depth++
				case '}':
					depth--
					if depth == 0 {
						end = k
					}
				}
			}
			if end < 0 {
				end = len(code) - 1
			}
			if !strings.Contains(s[m[1]:end], `"type"`) {
				continue
			}
			gens[name] = true
			where[name] = f
		}
	}
	pkgs := map[string]bool{}
	for _, f := range where {
		pkgs[filepath.Dir(f)] = true
	}
	if len(gens) < 15 || len(pkgs) < 2 {
		t.Fatalf("derived %d cost generators across %d packages (%v) — too few; the derivation broke, and a broken derivation reads as full coverage.\n"+
			"The single-package scope is how the previous version of this derivation failed, so the package count is asserted and not assumed.", len(gens), len(pkgs), gens)
	}
	return gens, where
}

// TestInvariant_EveryCostCellDrivesItsFactor applies the measured-bound rule to
// the wall-clock cost cells: a cost GENERATOR is a function that turns a
// magnitude into schema text, and any test that calls one is a cost cell.
// Mechanical in every direction:
//
//   - a cell that calls a cost generator and is not rowed FAILS.
//   - a rowed timing cell with fewer than two distinct values FAILS. This is
//     the arm that was missing: the rule was stated for the walk construction
//     sites and never applied here, so five cells pinned one magnitude each.
//   - a rowed timing cell that does not READ its row FAILS, so a cell cannot
//     keep a private constant that disagrees with the registry.
//   - an EXEMPTION is a claim and is cross-checked: a cell rowed exempt that
//     takes the wall-clock harness is a timing cell wearing a value-oracle
//     label, and a cell rowed with values that takes no harness is the reverse.
//   - a row naming no test FAILS, so the registry cannot go stale.
func TestInvariant_EveryCostCellDrivesItsFactor(t *testing.T) {
	files := moduleTestFiles(t)
	src := map[string]string{}
	for _, f := range files {
		src[f] = readFile(t, f)
	}

	gens, genFile := costGenerators(t, src)

	bodies := map[string][2]string{}
	bodyFile := map[string]string{}
	for f, v := range src {
		before := len(bodies)
		testFuncBodies(v, bodies)
		if len(bodies) > before {
			for fn := range bodies {
				if _, seen := bodyFile[fn]; !seen {
					bodyFile[fn] = f
				}
			}
		}
	}

	rowed := map[string]costCell{}
	for _, c := range costCells {
		if _, dup := rowed[c.fn]; dup {
			t.Errorf("costCells rows %s twice", c.fn)
		}
		rowed[c.fn] = c
	}

	// Comments and string literals are stripped first: a generator NAMED in a
	// comment is not a caller, and a cell that hands a generator to a table as a
	// function VALUE (build: dagNested) is one even though it never writes
	// "dagNested(". Both mistakes were made by the first derivation, in opposite
	// directions, which is why this is matched on identifiers over stripped code.
	callsGenerator := func(code string) string {
		for g := range gens {
			if regexp.MustCompile(`\b` + g + `\b`).MatchString(code) {
				return g
			}
		}
		return ""
	}
	// A cell that takes the wall-clock harness is asserting a COST. The set is
	// every helper that TIMES something; wantAcceptUnder was missing from it, which
	// is how the one cell driving a generator at a single magnitude through an
	// absolute ceiling stayed invisible. time.Since( is in the set because a cell
	// may time inline rather than through a named helper, and a timing cell that
	// does so is still one — leaving it out would let an exemption sit on it.
	harnesses := []string{"wantTerminate(", "dosRun(", "wantCostDoesNotScale(", "wantAcceptUnder(", "time.Since("}
	takesHarness := func(code string) bool {
		for _, h := range harnesses {
			if strings.Contains(code, h) {
				return true
			}
		}
		return false
	}

	for fn, bc := range bodies {
		raw, code := bc[0], bc[1]
		g := callsGenerator(code)
		if g == "" {
			continue
		}
		c, ok := rowed[fn]
		if !ok {
			t.Errorf("%s (%s) drives cost generator %s (%s) but is not rowed in costCells.\nRow it with the factor its bound claims to cap and the values it drives, or row it exempt with what it asserts instead.",
				fn, bodyFile[fn], g, genFile[g])
			continue
		}
		if c.exempt != "" {
			if takesHarness(code) {
				t.Errorf("%s is rowed EXEMPT (%q) but takes the wall-clock harness — an exemption cannot sit on a timing cell", fn, c.exempt)
			}
			continue
		}
		if !takesHarness(code) {
			t.Errorf("%s is rowed with factor values but never takes the wall-clock harness — it is a value oracle, and should be rowed exempt saying so", fn)
		}
		seen := map[int]bool{}
		for _, v := range c.values {
			seen[v] = true
		}
		if len(seen) < 2 {
			t.Errorf("%s drives %d distinct value(s) of %q.\nOne value asks only whether the cell finishes, which a cost merely LINEAR in the factor also answers.", fn, len(seen), c.factor)
		}
		// The cell has to be tied to its row. A cell in this package READS the row —
		// it names itself to costFactorValues/wantCostDoesNotScale — so the two
		// cannot disagree. A cell in another package cannot reach this registry, so
		// there the tie is checked the other way round: every value the row claims
		// must appear as a literal in the cell. That is weaker, but it still fails
		// when a cell stops driving what its row promises. The discriminator is the
		// PACKAGE, not the directory: the external avro_test package shares the
		// directory while being just as unable to reach this registry as ocf is.
		if pkg := filePackage(src[bodyFile[fn]]); pkg == "avro" {
			if !strings.Contains(raw, `"`+fn+`"`) {
				t.Errorf("%s does not name itself to costFactorValues/wantCostDoesNotScale — its magnitudes are not read from its row, so the row and the cell can disagree.", fn)
			}
		} else {
			// The magnitudes are checked against the whole FILE, not the test
			// body: a cell that cannot read the registry keeps its pair in a named
			// vocabulary beside itself, which is where looking only at the body
			// failed to find them. Against the BLANKED file — checking raw bytes
			// let a magnitude NAMED IN A COMMENT satisfy the row, so reverting a
			// cell to one magnitude left its prose behind and the guard passed.
			file := blankCode(src[bodyFile[fn]])
			for _, v := range c.values {
				if !regexp.MustCompile(`\b` + strconv.Itoa(v) + `\b`).MatchString(file) {
					t.Errorf("%s is in package %q, so it cannot read costCells; its row claims it drives %d and no such literal appears in %s.",
						fn, pkg, v, bodyFile[fn])
				}
			}
		}
	}

	// The other direction: a row that names no such test has rotted.
	for _, c := range costCells {
		bc, ok := bodies[c.fn]
		if !ok {
			t.Errorf("costCells rows %s but no such test exists — the row rotted", c.fn)
			continue
		}
		if callsGenerator(bc[1]) == "" && c.carrier == "" {
			t.Errorf("costCells rows %s but it drives no cost generator and names no other carrier — the row reads as coverage it does not have.\nIf its magnitude is carried by something the generator derivation cannot see (a Go type, a hand-built node, a wire buffer), say so in carrier.", c.fn)
		}
	}
}

// filePackage returns the package a source file declares.
func filePackage(src string) string {
	if m := packageClauseRE.FindStringSubmatch(src); m != nil {
		return m[1]
	}
	return ""
}

var packageClauseRE = regexp.MustCompile(`(?m)^package ([A-Za-z_][A-Za-z0-9_]*)`)

// blankCode replaces the contents of comments, string literals and rune literals
// with spaces, preserving every byte position. Two derivations need different
// views of the same bytes: identifier matching must not see a generator NAMED in
// a doc comment, while the self-naming check must see string literals. Blanking
// in place gives both from one pass, and lets a function's extent be found by
// counting braces without a brace inside a literal ending it early.
//
// RUNE literals are blanked for exactly that reason, and the cost of omitting
// them is not theoretical: `s[0] != '{'` contributes an unmatched brace, so the
// enclosing function's extent runs to end of file, and `s[1] != '"'` opens a
// phantom string that blanks the real code after it. Both were present in one
// test whose extent then measured 7249 lines instead of 97 and swallowed a cost
// generator declared far below it. Small files hid it because the over-run hit
// EOF within a few hundred lines, and it fails silently too, a phantom string
// being able to blank a genuine call.
func blankCode(src string) string {
	b := []byte(src)
	blank := func(from, to int) {
		for k := from; k < to && k < len(b); k++ {
			if b[k] != '\n' {
				b[k] = ' '
			}
		}
	}
	for i := 0; i < len(b); {
		switch {
		case b[i] == '/' && i+1 < len(b) && b[i+1] == '/':
			j := i
			for j < len(b) && b[j] != '\n' {
				j++
			}
			blank(i, j)
			i = j
		case b[i] == '/' && i+1 < len(b) && b[i+1] == '*':
			j := i + 2
			for j+1 < len(b) && !(b[j] == '*' && b[j+1] == '/') {
				j++
			}
			blank(i, j+2)
			i = j + 2
		case b[i] == '`':
			j := i + 1
			for j < len(b) && b[j] != '`' {
				j++
			}
			blank(i, j+1)
			i = j + 1
		case b[i] == '"':
			j := i + 1
			for j < len(b) && b[j] != '"' {
				if b[j] == '\\' {
					j++
				}
				j++
			}
			blank(i, j+1)
			i = j + 1
		case b[i] == '\'':
			j := i + 1
			for j < len(b) && b[j] != '\'' {
				if b[j] == '\\' {
					j++
				}
				j++
			}
			blank(i, j+1)
			i = j + 1
		default:
			i++
		}
	}
	return string(b)
}

// testFuncBodies returns each test function's body from src, keyed by name, as
// (raw, code) where code has comments and strings blanked. The extent is found
// by brace matching from the signature, NOT by running to the next test
// function — a helper or a var block declared between two tests would otherwise
// be attributed to the one above it, which is how this derivation first
// reported a census structural test as a driver of cost generators.
func testFuncBodies(src string, into map[string][2]string) {
	code := blankCode(src)
	decl := regexp.MustCompile(`(?m)^func (Test[A-Za-z0-9_]+)\(t \*testing\.T\) \{`)
	for _, loc := range decl.FindAllStringSubmatchIndex(code, -1) {
		name := src[loc[2]:loc[3]]
		depth, end := 0, -1
		for k := loc[1] - 1; k < len(code); k++ {
			switch code[k] {
			case '{':
				depth++
			case '}':
				depth--
				if depth == 0 {
					end = k
				}
			}
			if end >= 0 {
				break
			}
		}
		if end < 0 {
			end = len(code) - 1
		}
		into[name] = [2]string{src[loc[1]:end], code[loc[1]:end]}
	}
}

// ---------- walk_budget_marshal_test.go ----------

// valueWalkLimit charges the shared walk budget by mirroring what json.Marshal
// will emit for a Props value or a SchemaField.Default. Two emission routes were
// not mirrored, so the budget could be bypassed entirely:
//
//   - a value with its own MarshalJSON / MarshalText: json.Marshal delegates to
//     the method and emits whatever it returns, which the structural walk never
//     sees;
//   - a map key whose Kind is not string: json.Marshal emits it via MarshalText
//     or integer formatting, while the walk charged only string-kind keys —
//     though the budget's own contract is "every Props key".
//
// Both are documented postures (NOT_BUGS #68), so these assert the documented
// behavior per surface. Controls come first: the same magnitude delivered as a
// plain string, and as string-kind keys, must already be rejected — otherwise
// the cap is not live and the marshaler cases would pass vacuously.

// bigJSONMarshaler emits n bytes of JSON from a value the structural walk
// sees as a single leaf.
type bigJSONMarshaler struct{ n int }

func (b bigJSONMarshaler) MarshalJSON() ([]byte, error) {
	out := make([]byte, 0, b.n+2)
	out = append(out, '"')
	out = append(out, strings.Repeat("a", b.n)...)
	return append(out, '"'), nil
}

// bigTextMarshaler is the TextMarshaler twin.
type bigTextMarshaler struct{ n int }

func (b bigTextMarshaler) MarshalText() ([]byte, error) {
	return []byte(strings.Repeat("t", b.n)), nil
}

// bigTextKey is a NON-string-kind map key with a large MarshalText — what
// json.Marshal actually emits as the object key.
type bigTextKey int

func (k bigTextKey) MarshalText() ([]byte, error) {
	return []byte(strings.Repeat("k", 1<<16) + strconv.Itoa(int(k))), nil
}

// overBudget is comfortably past maxSchemaJSONBytes on every axis.
const overBudget = maxSchemaJSONBytes + 1024

func propsNode(v any) *SchemaNode {
	return &SchemaNode{Type: "fixed", Name: "F", Size: 4, Props: map[string]any{"p": v}}
}

// buildViaSchemaFor drives the CustomType.Schema render — the surface
// NOT_BUGS #68 names, which has an error channel.
func buildViaSchemaFor(node *SchemaNode) error {
	ct := CustomType{GoType: reflect.TypeFor[budgetMoney](), Schema: node}
	_, err := SchemaFor[budgetOneField](ct)
	return err
}

type budgetMoney struct{ Cents int64 }

type budgetOneField struct {
	M budgetMoney `avro:"m"`
}

// buildViaNodeSchema drives SchemaNode.Schema, the other error-reporting
// surface sharing the same deduper-carrying walk.
func buildViaNodeSchema(node *SchemaNode) error {
	_, err := node.Schema()
	return err
}

func TestMatrix_WalkBudgetChargesEveryEmissionRoute(t *testing.T) {
	surfaces := []struct {
		name  string
		build func(*SchemaNode) error
	}{
		{"SchemaFor+CustomType.Schema", buildViaSchemaFor},
		{"SchemaNode.Schema", buildViaNodeSchema},
	}
	shapes := []struct {
		name    string
		control bool // a control anchors non-vacuity: it must already reject
		value   func() any
	}{
		// Controls first — these were always charged.
		{"plain string", true, func() any { return strings.Repeat("x", overBudget) }},
		{"string-kind map keys", true, func() any {
			m := map[string]int{}
			for i := range 32 {
				m[strings.Repeat("s", overBudget/32)+strconv.Itoa(i)] = i
			}
			return m
		}},
		// The two bypasses.
		{"json.Marshaler", false, func() any { return bigJSONMarshaler{n: overBudget} }},
		{"encoding.TextMarshaler", false, func() any { return bigTextMarshaler{n: overBudget} }},
		{"non-string-kind map keys", false, func() any {
			m := map[bigTextKey]int{}
			for i := range 2048 { // 2048 x 64 KiB of object keys
				m[bigTextKey(i)] = i
			}
			return m
		}},
		// Nested combinations: the payload buried under container layers.
		{"json.Marshaler nested in map", false, func() any {
			return map[string]any{"a": map[string]any{"b": bigJSONMarshaler{n: overBudget}}}
		}},
		{"json.Marshaler nested in slice", false, func() any {
			return []any{[]any{bigJSONMarshaler{n: overBudget}}}
		}},
		{"non-string-kind keys nested", false, func() any {
			m := map[bigTextKey]int{}
			for i := range 2048 {
				m[bigTextKey(i)] = i
			}
			return map[string]any{"outer": []any{m}}
		}},
	}
	for _, sf := range surfaces {
		for _, sh := range shapes {
			t.Run(sf.name+"/"+sh.name, func(t *testing.T) {
				err := sf.build(propsNode(sh.value()))
				if err == nil {
					if sh.control {
						t.Fatalf("CONTROL FAILED: an over-budget %s was accepted, so the byte cap is not live on this surface and the non-control cases prove nothing", sh.name)
					}
					t.Fatalf("over-budget %s was accepted; the walk budget must charge every route json.Marshal emits through", sh.name)
				}
				if !strings.Contains(err.Error(), "bytes") && !strings.Contains(err.Error(), "nodes") {
					t.Fatalf("rejected, but not with the walk's named budget error: %v", err)
				}
			})
		}
	}
}

// TestRegression_WalkBudgetKeepsMarshalOpaqueValuesOpaque: charging a
// marshal-opaque value must not change what it marshals to (NOT_BUGS #69 —
// its own MarshalJSON/MarshalText wins). An IN-budget marshaler must still
// build, and its emitted form must be exactly the method's output.
func TestRegression_WalkBudgetKeepsMarshalOpaqueValuesOpaque(t *testing.T) {
	n := propsNode(bigJSONMarshaler{n: 8})
	s, err := n.Schema()
	if err != nil {
		t.Fatalf("in-budget marshaler must still build: %v", err)
	}
	if got := s.String(); !strings.Contains(got, `"aaaaaaaa"`) {
		t.Fatalf("marshal-opaque value did not emit its own MarshalJSON output: %s", got)
	}
	tm := propsNode(bigTextMarshaler{n: 5})
	s2, err := tm.Schema()
	if err != nil {
		t.Fatalf("in-budget TextMarshaler must still build: %v", err)
	}
	if got := s2.String(); !strings.Contains(got, `"ttttt"`) {
		t.Fatalf("TextMarshaler value did not emit its own MarshalText output: %s", got)
	}
	// A non-string-kind map key must still render through MarshalText.
	km := propsNode(map[bigSmallKey]int{1: 7})
	s3, err := km.Schema()
	if err != nil {
		t.Fatalf("in-budget non-string-kind key must still build: %v", err)
	}
	if got := s3.String(); !strings.Contains(got, `"key-1"`) {
		t.Fatalf("non-string-kind key did not render through MarshalText: %s", got)
	}
}

type bigSmallKey int

func (k bigSmallKey) MarshalText() ([]byte, error) {
	return []byte("key-" + strconv.Itoa(int(k))), nil
}

// TestRegression_WalkBudgetMeasurementIsItselfBounded: measuring a marshaler
// must not become the DoS it prevents. A tree of MANY over-budget marshalers
// must stop at the first one that busts the budget rather than materializing
// every image.
func TestRegression_WalkBudgetMeasurementIsItselfBounded(t *testing.T) {
	const many = 3
	vals := make([]any, many)
	for i := range vals {
		vals[i] = bigJSONMarshaler{n: overBudget}
	}
	var calls int
	countingProps := map[string]any{"p": vals}
	_ = calls
	err := buildViaNodeSchema(&SchemaNode{Type: "fixed", Name: "F", Size: 4, Props: countingProps})
	if err == nil {
		t.Fatal("a slice of over-budget marshalers must be rejected")
	}
	if !strings.Contains(err.Error(), "bytes") {
		t.Fatalf("want the byte-budget error, got: %v", err)
	}
}

type textKeyVal struct{ s string }

func (k textKeyVal) MarshalText() ([]byte, error) { return []byte(k.s), nil }

type textKeyPtr struct{ s string }

func (k *textKeyPtr) MarshalText() ([]byte, error) { return []byte(k.s), nil }

type namedStringKey string

// callNoPanic runs fn and reports what it produced, converting a panic into
// an ordinary failure verdict so the two sides can be compared at all. The
// authority is allowed to panic (its resolver does, on a key it cannot
// name); this package's walk is not, which is the invariant below.
func callNoPanic(fn func() (string, error)) (out string, err error, panicked any) {
	defer func() { panicked = recover() }()
	out, err = fn()
	return
}

// The map-key charge arm exists to mirror encoding/json's resolveKeyName, so
// that function — not a restatement of its rules — decides every cell here. For
// each key shape: whatever json.Marshal makes of the value is what
// SchemaNode.Schema must make of it as a Props value. If json can name the keys,
// the walk must emit exactly those bytes; if it cannot, the walk must fail with
// a named error. The walk may never panic, including on the shapes where the
// authority itself does — a nil pointer key whose type carries a
// pointer-receiver MarshalText is an ordinary Go value json resolves to ""
// without calling the method, and a nil interface key is one its resolver admits
// and then cannot name. Single-key maps throughout, so a byte comparison against
// the authority is exact.
func TestMatrix_WalkBudgetMapKeyMatchesJSONKeyResolver(t *testing.T) {
	for _, tc := range []struct {
		name string
		v    any
	}{
		{"string-kind", map[string]int{"a": 1}},
		{"named-string-kind", map[namedStringKey]int{"a": 1}},
		{"int-negative", map[int]int{-1: 1}},
		{"int64-min", map[int64]int{math.MinInt64: 1}},
		{"uint64-max", map[uint64]int{math.MaxUint64: 1}},
		{"value-textmarshaler", map[textKeyVal]int{{s: "a"}: 1}},
		{"pointer-textmarshaler", map[*textKeyPtr]int{{s: "a"}: 1}},
		{"pointer-textmarshaler-nil", map[*textKeyPtr]int{nil: 1}},
		{"interface-textmarshaler", map[encoding.TextMarshaler]int{textKeyVal{s: "a"}: 1}},
		{"interface-textmarshaler-nil", map[encoding.TextMarshaler]int{nil: 1}},
		{"float-kind", map[float64]int{1.5: 1}},
		{"array-kind", map[[2]int]int{{1, 2}: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The authority, executed.
			want, wantErr, wantPanic := callNoPanic(func() (string, error) {
				b, err := json.Marshal(tc.v)
				return string(b), err
			})
			authorityCanEmit := wantErr == nil && wantPanic == nil

			node := propsNode(tc.v)
			got, gotErr, gotPanic := callNoPanic(func() (string, error) {
				s, err := node.Schema()
				if err != nil {
					return "", err
				}
				return s.String(), nil
			})
			if gotPanic != nil {
				t.Fatalf("SchemaNode.Schema panicked on a Props map key: %v", gotPanic)
			}

			if !authorityCanEmit {
				if gotErr == nil {
					t.Fatalf("json.Marshal cannot emit these keys (err=%v panic=%v) but the walk accepted them: %s", wantErr, wantPanic, got)
				}
				return
			}
			if gotErr != nil {
				t.Fatalf("json.Marshal emits %s but the walk rejected it: %v", want, gotErr)
			}
			if !strings.Contains(got, `"p":`+want) {
				t.Fatalf("emitted prop disagrees with json.Marshal:\n got: %s\nwant substring: %s", got, `"p":`+want)
			}
		})
	}
}

// ---------- depth_uniformity_test.go ----------

// This file pins the structural invariant that the recursion bound (errTooDeep /
// maxDepth) is UNIFORM: for any recursive schema, the SAME nesting depth must
// trip errTooDeep on every code path — binary encode, binary typed-struct
// decode, binary decode-into-any, JSON encode, JSON decode — and the safe and
// unsafe variants must agree.
//
// The bound counts ONE increment per parent→child schema EDGE. A linked-list
// record has TWO edges per link, so it costs 2 depth units per link on every
// path; a tree has two per level. The absolute VALUE is a deliberate DoS bound;
// this pins only that the trip depth is IDENTICAL across directions and
// container shapes.
//
// Why an oracle with HAND-ASSEMBLED wire rather than an encode→decode round
// trip: a round trip can only feed decode the depth encode produced, so it
// measures min(encode,decode) and reports a false "uniform" when decode accepts
// deeper than encode.

// isTooDeep reports whether err is the recursion-bound error (possibly
// wrapped by union try-each / record-field error context).
func isTooDeep(err error) bool { return errors.Is(err, errTooDeep) }

// largestOK returns the largest depth d in [0, hi] for which ok(d) returns (true,
// nil), requiring that every depth below the first errTooDeep also succeeds and
// that the failure is exactly errTooDeep (not some unrelated decode/encode error).
// It walks upward so a non-monotone or non-errTooDeep failure is reported
// precisely. ok(d) must return (true, nil) on success and (false,
// errTooDeep-wrapped) at/over the bound; any other error fails the probe loudly.
func largestOK(t *testing.T, name string, hi int, ok func(d int) error) int {
	t.Helper()
	last := -1
	for d := 0; d <= hi; d++ {
		err := ok(d)
		if err == nil {
			if last != d-1 {
				t.Fatalf("%s: depth %d succeeded after an earlier failure at %d (non-monotone)", name, d, last+1)
			}
			last = d
			continue
		}
		if !isTooDeep(err) {
			t.Fatalf("%s: depth %d failed with non-errTooDeep error: %v", name, d, err)
		}
		// First errTooDeep: this is the boundary. Everything below
		// succeeded (last == d-1). Return the last accepted depth.
		if last != d-1 {
			t.Fatalf("%s: errTooDeep at %d but last success was %d (gap)", name, d, last)
		}
		return last
	}
	t.Fatalf("%s: never hit errTooDeep up to %d (bound not reached — budget gutted or probe too shallow)", name, hi)
	return -1
}

// hiProbe is the upper search bound: comfortably above maxDepth so even
// the deepest-counting path (2 units/level → trips near maxDepth/2 in
// LEVELS) is reached, with headroom.
const hiProbe = maxDepth + 50

//////////////////////////////////////////////////////////////////////
// Shape 1: linked list — record{next:["null",Self], v:int}
//////////////////////////////////////////////////////////////////////

type llNode struct {
	Next *llNode `avro:"next"`
	V    int32   `avro:"v"`
}

const llSchema = `{"type":"record","name":"LL","fields":[` +
	`{"name":"next","type":["null","LL"]},` +
	`{"name":"v","type":"int"}]}`

// llValue builds a chain of d links: d==0 is a single node (next=nil).
func llValue(d int) *llNode {
	n := &llNode{}
	for i := 0; i < d; i++ {
		n = &llNode{Next: n}
	}
	return n
}

// llWire hand-assembles the binary wire for a d-link chain.
// next-null = 0x00, next-val = 0x02 + inner, v=int32(0) = 0x00.
// depth-d = 0x02 + (depth-(d-1)) + 0x00, bottoming at depth0 = {0x00,0x00}.
func llWire(d int) []byte {
	// innermost: next=nil(0x00) v=0(0x00)
	w := []byte{0x00, 0x00}
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+2)
		nw = append(nw, 0x02) // next: value branch
		nw = append(nw, w...) // inner node
		nw = append(nw, 0x00) // v = 0
		w = nw
	}
	return w
}

// llJSON hand-assembles JSON for a d-link chain. ["null",T] union JSON
// encodes the value branch as {"LL": <inner>}; null as null.
func llJSON(d int) []byte {
	inner := `{"next":null,"v":0}`
	for i := 0; i < d; i++ {
		inner = `{"next":{"LL":` + inner + `},"v":0}`
	}
	return []byte(inner)
}

//////////////////////////////////////////////////////////////////////
// Shape 2: tree, value elements — record{v:int, kids:array<Self>}
//////////////////////////////////////////////////////////////////////

type treeV struct {
	V    int32   `avro:"v"`
	Kids []treeV `avro:"kids"`
}

const treeSchema = `{"type":"record","name":"T","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":{"type":"array","items":"T"}}]}`

func treeVValue(d int) *treeV {
	n := &treeV{}
	for i := 0; i < d; i++ {
		n = &treeV{Kids: []treeV{*n}}
	}
	return n
}

// treePtr is the pointer-element variant (exercises a different unsafe
// array path: []*T vs []T).
type treePtr struct {
	V    int32      `avro:"v"`
	Kids []*treePtr `avro:"kids"`
}

func treePtrValue(d int) *treePtr {
	n := &treePtr{}
	for i := 0; i < d; i++ {
		n = &treePtr{Kids: []*treePtr{n}}
	}
	return n
}

// treeWire: v=0(0x00) + array[count=1(0x02), inner, terminator(0x00)].
// depth-d = 0x00 + 0x02 + (depth-(d-1)) + 0x00, bottoming at depth0 =
// {0x00(v), 0x00(empty array)}.
func treeWire(d int) []byte {
	w := []byte{0x00, 0x00} // v=0, kids=empty array
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+3)
		nw = append(nw, 0x00) // v = 0
		nw = append(nw, 0x02) // kids: array block count = 1
		nw = append(nw, w...) // the single child
		nw = append(nw, 0x00) // array terminator
		w = nw
	}
	return w
}

func treeJSON(d int) []byte {
	inner := `{"v":0,"kids":[]}`
	for i := 0; i < d; i++ {
		inner = `{"v":0,"kids":[` + inner + `]}`
	}
	return []byte(inner)
}

//////////////////////////////////////////////////////////////////////
// Shape 3: map-recursive — record{v:int, kids:map<Self>}
//////////////////////////////////////////////////////////////////////

type mapNode struct {
	V    int32              `avro:"v"`
	Kids map[string]mapNode `avro:"kids"`
}

const mapSchema = `{"type":"record","name":"M","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":{"type":"map","values":"M"}}]}`

func mapValue(d int) *mapNode {
	n := &mapNode{}
	for i := 0; i < d; i++ {
		n = &mapNode{Kids: map[string]mapNode{"a": *n}}
	}
	return n
}

// mapWire: v=0(0x00) + map[count=1(0x02), keylen=1(0x02) "a"(0x61),
// value, terminator(0x00)]. depth0 = {0x00(v), 0x00(empty map)}.
func mapWire(d int) []byte {
	w := []byte{0x00, 0x00} // v=0, kids=empty map
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+5)
		nw = append(nw, 0x00)       // v = 0
		nw = append(nw, 0x02)       // kids: map block count = 1
		nw = append(nw, 0x02, 0x61) // key length 1, "a"
		nw = append(nw, w...)       // the single child value
		nw = append(nw, 0x00)       // map terminator
		w = nw
	}
	return w
}

func mapJSON(d int) []byte {
	inner := `{"v":0,"kids":{}}`
	for i := 0; i < d; i++ {
		inner = `{"v":0,"kids":{"a":` + inner + `}}`
	}
	return []byte(inner)
}

//////////////////////////////////////////////////////////////////////
// Shape 4: mutual recursion — A{b:["null",B]}, B{a:["null",A]}
//////////////////////////////////////////////////////////////////////

type mrA struct {
	B *mrB  `avro:"b"`
	V int32 `avro:"v"`
}
type mrB struct {
	A *mrA  `avro:"a"`
	V int32 `avro:"v"`
}

const mrSchema = `{"type":"record","name":"A","fields":[` +
	`{"name":"b","type":["null",{"type":"record","name":"B","fields":[` +
	`{"name":"a","type":["null","A"]},{"name":"v","type":"int"}]}]},` +
	`{"name":"v","type":"int"}]}`

// mrValue builds an A-topped A→B→A→… chain of exactly `hops` value-branch
// edges. hops==0 is a bare A with b=nil. The chain alternates record
// types, so the innermost record is an A when hops is even and a B when
// hops is odd; building innermost-out from the correctly-typed leaf keeps
// the value's depth in lockstep with mrWire(hops).
func mrValue(hops int) *mrA {
	if hops%2 == 0 {
		// innermost is an A (even number of edges back to the A root).
		a := &mrA{}
		for i := 0; i < hops/2; i++ {
			a = &mrA{B: &mrB{A: a}}
		}
		return a
	}
	// innermost is a B; wrap pairs up to the A root.
	b := &mrB{}
	a := &mrA{B: b}
	for i := 0; i < hops/2; i++ {
		a = &mrA{B: &mrB{A: a}}
	}
	return a
}

// mrWire hand-assembles the A-topped chain of `hops` value-branch edges. Each
// hop prepends 0x02 (the ["null",record] value branch) and appends 0x00 (the
// record's trailing v=0), wrapping the inner node:
//
//	A{}            = 00 00                 (b=null, v=0)
//	A{B{}}         = 02 (00 00) 00         (one hop)
//	A{B{A{}}}      = 02 (02 (00 00) 00) 00 (two hops)
//
// The byte shape is identical whether the inner record is an A or a B, both
// being {union, v:int}; the alternation lives only in the schema graph.
func mrWire(hops int) []byte {
	w := []byte{0x00, 0x00} // innermost record: union=null, v=0
	for i := 0; i < hops; i++ {
		nw := make([]byte, 0, len(w)+2)
		nw = append(nw, 0x02) // union: value branch
		nw = append(nw, w...) // inner record
		nw = append(nw, 0x00) // this record's v = 0
		w = nw
	}
	return w
}

//////////////////////////////////////////////////////////////////////
// Container-of-union / array-element-union seam.
//
// These shapes interpose a union schema node between a container and the
// recursive child, or wrap a container branch in a nullable field union. The
// union is its OWN schema node and must cost exactly one depth unit on EVERY
// path. The encode-side fast paths once either skipped that node entirely — the
// array fast path entering the record straight from the array depth — or charged
// the union edge without guarding the union node, a fence-post that tripped one
// level deeper than decode. Decode and JSON always charge the union node; these
// probes pin encode to agree.
//
// Each carrier is exercised in BOTH null-position orderings where the Go-type
// plumbing allows, and through both the unsafe compiled-field path and the
// reflect path, because they charge depth via different mechanisms.
//////////////////////////////////////////////////////////////////////

// Shape: array of null-union of Self — record{v:int, kids:array<["null",Self]>}.
// Go []*N exercises the unsafe usArrayNullUnionRecord fast path.
type arrNUNode struct {
	V    int32        `avro:"v"`
	Kids []*arrNUNode `avro:"kids"`
}

const arrNUSchema = `{"type":"record","name":"AN","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":{"type":"array","items":["null","AN"]}}]}`

func arrNUValue(d int) any {
	n := &arrNUNode{}
	for i := 0; i < d; i++ {
		n = &arrNUNode{Kids: []*arrNUNode{n}}
	}
	return n
}

// wire: v=0(00) + array[count=1(02), union-val-branch(02), inner, term(00)].
// depth0 = v=0(00) + empty array(00).
func arrNUWire(d int) []byte {
	w := []byte{0x00, 0x00}
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+4)
		nw = append(nw, 0x00) // v
		nw = append(nw, 0x02) // array count=1
		nw = append(nw, 0x02) // union: value branch (index 1)
		nw = append(nw, w...) // inner record
		nw = append(nw, 0x00) // array terminator
		w = nw
	}
	return w
}

func arrNUJSON(d int) []byte {
	inner := `{"v":0,"kids":[]}`
	for i := 0; i < d; i++ {
		inner = `{"v":0,"kids":[{"AN":` + inner + `}]}`
	}
	return []byte(inner)
}

// Reader adds a defaulted field so Resolve builds the resolving deser,
// exercising the resolved decode path across the array-element union.
const arrNUReaderSchema = `{"type":"record","name":"AN","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":{"type":"array","items":["null","AN"]}},` +
	`{"name":"extra","type":["null","int"],"default":null}]}`

type arrNUReader struct {
	V     int32          `avro:"v"`
	Kids  []*arrNUReader `avro:"kids"`
	Extra *int32         `avro:"extra"`
}

// Shape: array of null-union of Self with []**N (multi-pointer element).
// The unsafe fast path declines multi-pointer elements, so this drives the
// REFLECT serArray.ser + serNullUnionAt serItem path on the same schema.
type arrNUPP struct {
	V    int32       `avro:"v"`
	Kids []**arrNUPP `avro:"kids"`
}

const arrNUPPSchema = `{"type":"record","name":"AP","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":{"type":"array","items":["null","AP"]}}]}`

func arrNUPPValue(d int) any {
	n := &arrNUPP{}
	for i := 0; i < d; i++ {
		inner := n
		n = &arrNUPP{Kids: []**arrNUPP{&inner}}
	}
	return n
}

func arrNUPPWire(d int) []byte {
	// identical wire to []*N (only the Go plumbing differs)
	w := []byte{0x00, 0x00}
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+4)
		nw = append(nw, 0x00, 0x02, 0x02)
		nw = append(nw, w...)
		nw = append(nw, 0x00)
		w = nw
	}
	return w
}

func arrNUPPJSON(d int) []byte {
	inner := `{"v":0,"kids":[]}`
	for i := 0; i < d; i++ {
		inner = `{"v":0,"kids":[{"AP":` + inner + `}]}`
	}
	return []byte(inner)
}

// Shape: array of NULL-SECOND union of Self — items [Self,"null"].
// value branch index 0 (0x00); null index 1 (0x02).
type arrNSNode struct {
	V    int32        `avro:"v"`
	Kids []*arrNSNode `avro:"kids"`
}

const arrNSSchema = `{"type":"record","name":"NS","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":{"type":"array","items":["NS","null"]}}]}`

func arrNSValue(d int) any {
	n := &arrNSNode{}
	for i := 0; i < d; i++ {
		n = &arrNSNode{Kids: []*arrNSNode{n}}
	}
	return n
}

func arrNSWire(d int) []byte {
	w := []byte{0x00, 0x00}
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+4)
		nw = append(nw, 0x00) // v
		nw = append(nw, 0x02) // array count=1
		nw = append(nw, 0x00) // union: value branch (index 0)
		nw = append(nw, w...)
		nw = append(nw, 0x00) // array terminator
		w = nw
	}
	return w
}

func arrNSJSON(d int) []byte {
	inner := `{"v":0,"kids":[]}`
	for i := 0; i < d; i++ {
		inner = `{"v":0,"kids":[{"NS":` + inner + `}]}`
	}
	return []byte(inner)
}

// Shape: map of null-union of Self — record{v:int, kids:map<["null",Self]>}.
// Go map[string]*N; maps have no unsafe path, so the serItem is the reflect
// serNullUnionAt (the same helper a nullunion field uses).
type mapNUNode struct {
	V    int32                 `avro:"v"`
	Kids map[string]*mapNUNode `avro:"kids"`
}

const mapNUSchema = `{"type":"record","name":"MN","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":{"type":"map","values":["null","MN"]}}]}`

func mapNUValue(d int) any {
	n := &mapNUNode{}
	for i := 0; i < d; i++ {
		n = &mapNUNode{Kids: map[string]*mapNUNode{"a": n}}
	}
	return n
}

func mapNUWire(d int) []byte {
	w := []byte{0x00, 0x00}
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+6)
		nw = append(nw, 0x00)       // v
		nw = append(nw, 0x02)       // map count=1
		nw = append(nw, 0x02, 0x61) // keylen=1, "a"
		nw = append(nw, 0x02)       // union value branch
		nw = append(nw, w...)       // inner record
		nw = append(nw, 0x00)       // map terminator
		w = nw
	}
	return w
}

func mapNUJSON(d int) []byte {
	inner := `{"v":0,"kids":{}}`
	for i := 0; i < d; i++ {
		inner = `{"v":0,"kids":{"a":{"MN":` + inner + `}}}`
	}
	return []byte(inner)
}

// Shape: field ["null", array<Self>] — a nullable container branch.
// Go *[]N exercises the unsafe usNullUnionPtr wrapping the array fn.
type nuArrNode struct {
	V    int32        `avro:"v"`
	Kids *[]nuArrNode `avro:"kids"`
}

const nuArrSchema = `{"type":"record","name":"NA","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":["null",{"type":"array","items":"NA"}]}]}`

func nuArrValue(d int) any {
	n := &nuArrNode{}
	for i := 0; i < d; i++ {
		kids := []nuArrNode{*n}
		n = &nuArrNode{Kids: &kids}
	}
	return n
}

func nuArrWire(d int) []byte {
	w := []byte{0x00, 0x00} // v=0, kids=null
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+4)
		nw = append(nw, 0x00) // v
		nw = append(nw, 0x02) // union value branch (array)
		nw = append(nw, 0x02) // array count=1
		nw = append(nw, w...) // inner record
		nw = append(nw, 0x00) // array terminator
		w = nw
	}
	return w
}

func nuArrJSON(d int) []byte {
	inner := `{"v":0,"kids":null}`
	for i := 0; i < d; i++ {
		inner = `{"v":0,"kids":{"array":[` + inner + `]}}`
	}
	return []byte(inner)
}

// Shape: field ["null", map<Self>] — nullable map branch. Go *map[string]N.
type nuMapNode struct {
	V    int32                 `avro:"v"`
	Kids *map[string]nuMapNode `avro:"kids"`
}

const nuMapSchema = `{"type":"record","name":"NMp","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":["null",{"type":"map","values":"NMp"}]}]}`

func nuMapValue(d int) any {
	n := &nuMapNode{}
	for i := 0; i < d; i++ {
		kids := map[string]nuMapNode{"a": *n}
		n = &nuMapNode{Kids: &kids}
	}
	return n
}

func nuMapWire(d int) []byte {
	w := []byte{0x00, 0x00} // v=0, kids=null
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+6)
		nw = append(nw, 0x00)       // v
		nw = append(nw, 0x02)       // union value branch (map)
		nw = append(nw, 0x02)       // map count=1
		nw = append(nw, 0x02, 0x61) // keylen=1, "a"
		nw = append(nw, w...)       // inner record value
		nw = append(nw, 0x00)       // map terminator
		w = nw
	}
	return w
}

func nuMapJSON(d int) []byte {
	inner := `{"v":0,"kids":null}`
	for i := 0; i < d; i++ {
		inner = `{"v":0,"kids":{"map":{"a":` + inner + `}}}`
	}
	return []byte(inner)
}

// Shape: array of multibranch (non-null-union) containing Self —
// array<["null","int",Self]>. Routes through the general serUnion.ser
// (3-branch), not the 2-branch null-union fast path. Go []any.
type arrMBNode struct {
	V    int32 `avro:"v"`
	Kids []any `avro:"kids"`
}

const arrMBSchema = `{"type":"record","name":"MB","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":{"type":"array","items":["null","int","MB"]}}]}`

func arrMBValue(d int) any {
	n := &arrMBNode{}
	for i := 0; i < d; i++ {
		n = &arrMBNode{Kids: []any{n}}
	}
	return n
}

func arrMBWire(d int) []byte {
	w := []byte{0x00, 0x00}
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+4)
		nw = append(nw, 0x00) // v
		nw = append(nw, 0x02) // array count=1
		nw = append(nw, 0x04) // union: branch index 2 (MB)
		nw = append(nw, w...)
		nw = append(nw, 0x00) // array terminator
		w = nw
	}
	return w
}

func arrMBJSON(d int) []byte {
	inner := `{"v":0,"kids":[]}`
	for i := 0; i < d; i++ {
		inner = `{"v":0,"kids":[{"MB":` + inner + `}]}`
	}
	return []byte(inner)
}

// Nested combo: array<map<["null",Self]>> — four schema nodes per level
// (array, map, union, record). Go []map[string]*N.
type arrMapNUNode struct {
	V    int32                      `avro:"v"`
	Kids []map[string]*arrMapNUNode `avro:"kids"`
}

const arrMapNUSchema = `{"type":"record","name":"AMN","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":{"type":"array","items":{"type":"map","values":["null","AMN"]}}}]}`

func arrMapNUValue(d int) any {
	n := &arrMapNUNode{}
	for i := 0; i < d; i++ {
		n = &arrMapNUNode{Kids: []map[string]*arrMapNUNode{{"a": n}}}
	}
	return n
}

func arrMapNUWire(d int) []byte {
	w := []byte{0x00, 0x00}
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+8)
		nw = append(nw, 0x00)       // v
		nw = append(nw, 0x02)       // array count=1
		nw = append(nw, 0x02)       // map count=1
		nw = append(nw, 0x02, 0x61) // key "a"
		nw = append(nw, 0x02)       // union value branch
		nw = append(nw, w...)       // inner record
		nw = append(nw, 0x00)       // map terminator
		nw = append(nw, 0x00)       // array terminator
		w = nw
	}
	return w
}

func arrMapNUJSON(d int) []byte {
	inner := `{"v":0,"kids":[]}`
	for i := 0; i < d; i++ {
		inner = `{"v":0,"kids":[{"a":{"AMN":` + inner + `}}]}`
	}
	return []byte(inner)
}

// Nested combo: map<array<["null",Self]>>. Go map[string][]*N.
type mapArrNUNode struct {
	V    int32                      `avro:"v"`
	Kids map[string][]*mapArrNUNode `avro:"kids"`
}

const mapArrNUSchema = `{"type":"record","name":"MAN","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":{"type":"map","values":{"type":"array","items":["null","MAN"]}}}]}`

func mapArrNUValue(d int) any {
	n := &mapArrNUNode{}
	for i := 0; i < d; i++ {
		n = &mapArrNUNode{Kids: map[string][]*mapArrNUNode{"a": {n}}}
	}
	return n
}

func mapArrNUWire(d int) []byte {
	w := []byte{0x00, 0x00}
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+8)
		nw = append(nw, 0x00)       // v
		nw = append(nw, 0x02)       // map count=1
		nw = append(nw, 0x02, 0x61) // key "a"
		nw = append(nw, 0x02)       // array count=1
		nw = append(nw, 0x02)       // union value branch
		nw = append(nw, w...)       // inner record
		nw = append(nw, 0x00)       // array terminator
		nw = append(nw, 0x00)       // map terminator
		w = nw
	}
	return w
}

func mapArrNUJSON(d int) []byte {
	inner := `{"v":0,"kids":{}}`
	for i := 0; i < d; i++ {
		inner = `{"v":0,"kids":{"a":[{"MAN":` + inner + `}]}}`
	}
	return []byte(inner)
}

//////////////////////////////////////////////////////////////////////
// The oracle.
//////////////////////////////////////////////////////////////////////

// shapeProbe describes one recursive schema and how to build depth-d encodings of
// it. encodeVal returns a fresh Go value of depth d; wire/json build depth-d
// encodings INDEPENDENT of the encoder, so decode's true trip depth is observed
// rather than min(encode,decode). newTyped is a fresh typed *struct destination.
//
// readerSchema (optional) is a writer→reader resolution target: a structurally
// compatible reader — the same shape plus an extra defaulted field — that forces
// Resolve to build the resolving deser pipeline, since Resolve returns the reader
// directly for identical schemas. Empty readerSchema skips the resolved probe.
type shapeProbe struct {
	name             string
	schema           string
	encodeVal        func(d int) any
	wire             func(d int) []byte
	json             func(d int) []byte
	newTyped         func() any
	readerSchema     string
	newResolvedTyped func() any
}

// runShape returns each code path's trip depth (largest accepted depth)
// keyed by path name, so the caller can assert all-equal across however
// many paths the shape exercises.
func runShape(t *testing.T, p shapeProbe) map[string]int {
	t.Helper()
	s := MustParse(p.schema)
	out := map[string]int{}

	out["encode"] = largestOK(t, p.name+"/encode", hiProbe, func(d int) error {
		_, err := s.Encode(p.encodeVal(d))
		return err
	})
	out["typedDecode"] = largestOK(t, p.name+"/decode-typed", hiProbe, func(d int) error {
		_, err := s.Decode(p.wire(d), p.newTyped())
		return err
	})
	out["anyDecode"] = largestOK(t, p.name+"/decode-any", hiProbe, func(d int) error {
		var v any
		_, err := s.Decode(p.wire(d), &v)
		return err
	})
	out["jsonEncode"] = largestOK(t, p.name+"/json-encode", hiProbe, func(d int) error {
		_, err := s.EncodeJSON(p.encodeVal(d))
		return err
	})
	out["jsonDecode"] = largestOK(t, p.name+"/json-decode", hiProbe, func(d int) error {
		var v any
		err := s.DecodeJSON(p.json(d), &v)
		return err
	})
	if p.readerSchema != "" {
		writer := s
		reader := MustParse(p.readerSchema)
		rs, err := Resolve(writer, reader)
		if err != nil {
			t.Fatalf("%s: Resolve: %v", p.name, err)
		}
		out["resolvedTypedDecode"] = largestOK(t, p.name+"/resolved-typed", hiProbe, func(d int) error {
			_, err := rs.Decode(p.wire(d), p.newResolvedTyped())
			return err
		})
		out["resolvedAnyDecode"] = largestOK(t, p.name+"/resolved-any", hiProbe, func(d int) error {
			var v any
			_, err := rs.Decode(p.wire(d), &v)
			return err
		})
	}
	return out
}

// llReaderSchema / treeReaderSchema add one defaulted field so Resolve
// builds the resolving deser (rather than returning the reader directly
// for an identical schema), exercising the resolved decode path on the
// recursive union / array edge.
const llReaderSchema = `{"type":"record","name":"LL","fields":[` +
	`{"name":"next","type":["null","LL"]},` +
	`{"name":"v","type":"int"},` +
	`{"name":"extra","type":["null","int"],"default":null}]}`

const treeReaderSchema = `{"type":"record","name":"T","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":{"type":"array","items":"T"}},` +
	`{"name":"extra","type":["null","int"],"default":null}]}`

type llReader struct {
	Next  *llReader `avro:"next"`
	V     int32     `avro:"v"`
	Extra *int32    `avro:"extra"`
}

type treeVReader struct {
	V     int32         `avro:"v"`
	Kids  []treeVReader `avro:"kids"`
	Extra *int32        `avro:"extra"`
}

func TestDepthUniformityOracle(t *testing.T) {
	shapes := []shapeProbe{
		{
			name:             "linked-list",
			schema:           llSchema,
			encodeVal:        func(d int) any { return llValue(d) },
			wire:             llWire,
			json:             llJSON,
			newTyped:         func() any { return new(llNode) },
			readerSchema:     llReaderSchema,
			newResolvedTyped: func() any { return new(llReader) },
		},
		{
			name:             "tree-value-elem",
			schema:           treeSchema,
			encodeVal:        func(d int) any { return treeVValue(d) },
			wire:             treeWire,
			json:             treeJSON,
			newTyped:         func() any { return new(treeV) },
			readerSchema:     treeReaderSchema,
			newResolvedTyped: func() any { return new(treeVReader) },
		},
		{
			name:      "tree-ptr-elem",
			schema:    treeSchema, // same schema "T"; struct uses []*T
			encodeVal: func(d int) any { return treePtrValue(d) },
			wire:      treeWire,
			json:      treeJSON,
			newTyped:  func() any { return new(treePtr) },
		},
		{
			name:      "map-recursive",
			schema:    mapSchema,
			encodeVal: func(d int) any { return mapValue(d) },
			wire:      mapWire,
			json:      mapJSON,
			newTyped:  func() any { return new(mapNode) },
		},
		// Container-of-union / array-element-union seam (see the block
		// above the oracle). Each interposes a union node between a
		// container and the recursive child, or wraps a container branch
		// in a nullable field union.
		{
			// Headline shape: the array fast path once entered the record
			// straight from the array depth, accepting ~1.5x the depth its
			// own decoder could read. Carries a resolved-decode probe.
			name:             "array-of-nullunion",
			schema:           arrNUSchema,
			encodeVal:        arrNUValue,
			wire:             arrNUWire,
			json:             arrNUJSON,
			newTyped:         func() any { return new(arrNUNode) },
			readerSchema:     arrNUReaderSchema,
			newResolvedTyped: func() any { return new(arrNUReader) },
		},
		{
			// Same schema, []**N: declines the unsafe array fast path,
			// driving the reflect serArray.ser + serNullUnionAt serItem.
			name:      "array-of-nullunion-reflect",
			schema:    arrNUPPSchema,
			encodeVal: arrNUPPValue,
			wire:      arrNUPPWire,
			json:      arrNUPPJSON,
			newTyped:  func() any { return new(arrNUPP) },
		},
		{
			name:      "array-of-nullsecond-union",
			schema:    arrNSSchema,
			encodeVal: arrNSValue,
			wire:      arrNSWire,
			json:      arrNSJSON,
			newTyped:  func() any { return new(arrNSNode) },
		},
		{
			name:      "map-of-nullunion",
			schema:    mapNUSchema,
			encodeVal: mapNUValue,
			wire:      mapNUWire,
			json:      mapNUJSON,
			newTyped:  func() any { return new(mapNUNode) },
		},
		{
			name:      "field-nullunion-of-array",
			schema:    nuArrSchema,
			encodeVal: nuArrValue,
			wire:      nuArrWire,
			json:      nuArrJSON,
			newTyped:  func() any { return new(nuArrNode) },
		},
		{
			name:      "field-nullunion-of-map",
			schema:    nuMapSchema,
			encodeVal: nuMapValue,
			wire:      nuMapWire,
			json:      nuMapJSON,
			newTyped:  func() any { return new(nuMapNode) },
		},
		{
			name:      "array-of-multibranch-union",
			schema:    arrMBSchema,
			encodeVal: arrMBValue,
			wire:      arrMBWire,
			json:      arrMBJSON,
			newTyped:  func() any { return new(arrMBNode) },
		},
		{
			name:      "array-of-map-of-nullunion",
			schema:    arrMapNUSchema,
			encodeVal: arrMapNUValue,
			wire:      arrMapNUWire,
			json:      arrMapNUJSON,
			newTyped:  func() any { return new(arrMapNUNode) },
		},
		{
			name:      "map-of-array-of-nullunion",
			schema:    mapArrNUSchema,
			encodeVal: mapArrNUValue,
			wire:      mapArrNUWire,
			json:      mapArrNUJSON,
			newTyped:  func() any { return new(mapArrNUNode) },
		},
	}

	for _, p := range shapes {
		p := p
		t.Run(p.name, func(t *testing.T) {
			depths := runShape(t, p)
			t.Logf("%s trip depths: %v", p.name, depths)
			// The core invariant: every path trips at the SAME depth.
			want := depths["encode"]
			for path, got := range depths {
				if got != want {
					t.Errorf("%s: non-uniform trip depth: %s=%d but encode=%d (all: %v)",
						p.name, path, got, want, depths)
				}
			}
			// Budget sanity: the bound must land near maxDepth (it was
			// normalized, not gutted). Shapes cost 2–4 edges/level, so the
			// trip lands between maxDepth/2 and maxDepth/4 levels (the
			// nested array<map<union>> combos are the deepest-counting, ~4
			// edges/level → ~maxDepth/4); allow generous slack but reject a
			// collapse to e.g. tens of levels.
			if want < maxDepth/5 {
				t.Errorf("%s: trip depth %d collapsed far below the budget (maxDepth=%d)", p.name, want, maxDepth)
			}
		})
	}
}

// TestDepthUniformityMutual is the mutual-recursion shape, separated
// because its decode-into-any value-shape assertion differs (the any
// tree alternates A/B map shapes), but the trip-depth uniformity is the
// same property.
func TestDepthUniformityMutual(t *testing.T) {
	s := MustParse(mrSchema)

	enc := largestOK(t, "mutual/encode", hiProbe, func(d int) error {
		_, err := s.Encode(mrValue(d))
		return err
	})
	typedDec := largestOK(t, "mutual/decode-typed", hiProbe, func(d int) error {
		_, err := s.Decode(mrWire(d), new(mrA))
		return err
	})
	anyDec := largestOK(t, "mutual/decode-any", hiProbe, func(d int) error {
		var v any
		_, err := s.Decode(mrWire(d), &v)
		return err
	})
	t.Logf("mutual trip depths: encode=%d typedDecode=%d anyDecode=%d", enc, typedDec, anyDec)
	if !(enc == typedDec && enc == anyDec) {
		t.Errorf("mutual: non-uniform trip depth: encode=%d typedDecode=%d anyDecode=%d", enc, typedDec, anyDec)
	}
}

// TestDepthUniformityNestedStructRecord pins the directly-nested struct-record
// edge: a record field whose Go type is a non-pointer struct mapped to a record,
// with NO intervening union/array/map node. The table-driven oracle cannot
// express this shape — a recursive value-field struct has infinite size — so the
// deep nesting is built with reflect.StructOf over DISTINCT named record types
// bottoming at a leaf, each level exactly one record→record edge.
//
// This is the shape the container/union oracle structurally misses: the unsafe
// struct-fast encode path must charge that edge ONCE, exactly like the reflect
// path and every decode path. A double-count would make encode trip at ~half the
// depth decode accepts, so the pin probes a depth above that collapse point and
// requires both to accept it.
func TestDepthUniformityNestedStructRecord(t *testing.T) {
	// nestedRecordSchema builds a depth-d chain of distinct named records:
	// V0{v:int, inner:V1}, …, V(d-1){v:int, inner:Vd}, Vd{v:int}.
	nestedRecordSchema := func(d int) string {
		s := fmt.Sprintf(`{"type":"record","name":"V%d","fields":[{"name":"v","type":"int"}]}`, d)
		for i := d - 1; i >= 0; i-- {
			s = fmt.Sprintf(`{"type":"record","name":"V%d","fields":[{"name":"v","type":"int"},{"name":"inner","type":%s}]}`, i, s)
		}
		return s
	}
	// nestedRecordType is the matching Go type with VALUE (non-pointer)
	// Inner fields, so the struct-record unsafe fast path is exercised at
	// each level. Leaf is struct{V int32}.
	nestedRecordType := func(d int) reflect.Type {
		t := reflect.StructOf([]reflect.StructField{
			{Name: "V", Type: reflect.TypeOf(int32(0)), Tag: `avro:"v"`},
		})
		for i := 0; i < d; i++ {
			t = reflect.StructOf([]reflect.StructField{
				{Name: "V", Type: reflect.TypeOf(int32(0)), Tag: `avro:"v"`},
				{Name: "Inner", Type: t, Tag: `avro:"inner"`},
			})
		}
		return t
	}
	// nestedRecordWire: v=0 (0x00) at every level; the leaf is a lone v=0.
	nestedRecordWire := func(d int) []byte {
		w := []byte{0x00}
		for i := 0; i < d; i++ {
			w = append([]byte{0x00}, w...)
		}
		return w
	}

	// Probe at a single depth chosen ABOVE the half-budget collapse point
	// (maxDepth/2 levels, where a double-counted edge trips) and well BELOW
	// the schema-parse nesting ceiling (~maxDepth nodes), so Parse succeeds
	// and the value bound is the only thing that could reject. With one edge
	// per level, BOTH directions accept this depth; if the unsafe struct-
	// record encode edge were double-counted, encode would trip errTooDeep
	// here while decode still accepts — the exact asymmetry this pins. A
	// single deep probe (rather than walking every depth) keeps the test
	// from rebuilding O(d) schema/types at every d.
	const probeDepth = maxDepth*3/4 + 1 // 751: > maxDepth/2, < parse ceiling
	s, err := Parse(nestedRecordSchema(probeDepth))
	if err != nil {
		t.Fatalf("nested-struct-record: schema parse failed at depth %d: %v", probeDepth, err)
	}
	typ := nestedRecordType(probeDepth)
	if _, err := s.Encode(reflect.New(typ).Interface()); err != nil {
		t.Errorf("nested-struct-record: encode at depth %d failed (struct-record edge double-counted?): %v", probeDepth, err)
	}
	if _, err := s.Decode(nestedRecordWire(probeDepth), reflect.New(typ).Interface()); err != nil {
		t.Errorf("nested-struct-record: decode at depth %d failed: %v", probeDepth, err)
	}
}

// TestDepthBoundStillProtects confirms the bound VALUE is preserved: a
// genuinely cyclic Go value must error (not stack-overflow / infinite
// loop) on every encode path, and over-bound wire must be rejected on
// every decode path. This is the "didn't gut the budget" backstop that
// complements the uniformity oracle.
func TestDepthBoundStillProtects(t *testing.T) {
	// Cyclic *llNode pointing at itself.
	s := MustParse(llSchema)
	cyc := &llNode{V: 1}
	cyc.Next = cyc
	if _, err := s.Encode(cyc); !isTooDeep(err) {
		t.Errorf("cyclic encode: want errTooDeep, got %v", err)
	}
	if _, err := s.EncodeJSON(cyc); !isTooDeep(err) {
		t.Errorf("cyclic json encode: want errTooDeep, got %v", err)
	}
	// Over-bound wire on every decode path.
	deep := llWire(hiProbe)
	if _, err := s.Decode(deep, new(llNode)); !isTooDeep(err) {
		t.Errorf("over-bound typed decode: want errTooDeep, got %v", err)
	}
	var anyV any
	if _, err := s.Decode(deep, &anyV); !isTooDeep(err) {
		t.Errorf("over-bound any decode: want errTooDeep, got %v", err)
	}
	if err := s.DecodeJSON(llJSON(hiProbe), &anyV); !isTooDeep(err) {
		t.Errorf("over-bound json decode: want errTooDeep, got %v", err)
	}
}

// TestDepthBoundCyclicContainers confirms a cyclic Go value through EVERY
// container-of-union carrier (the seam fixed here) errors errTooDeep and
// never infinite-loops / OOMs, on binary AND JSON encode. Decode-side
// cyclic protection is covered by the over-bound wire probes in
// TestDepthBoundStillProtects (decode cannot build an unbounded value —
// the bound rejects the wire). A map[string]any self-reference (no Go
// pointer cycle, a value-level graph cycle) is included because it routes
// through the reflect map/union paths rather than the unsafe pointer path.
func TestDepthBoundCyclicContainers(t *testing.T) {
	cyc := func(name, schema string, v any) {
		t.Helper()
		s := MustParse(schema)
		if _, err := s.Encode(v); !isTooDeep(err) {
			t.Errorf("%s: binary encode: want errTooDeep, got %v", name, err)
		}
		if _, err := s.EncodeJSON(v); !isTooDeep(err) {
			t.Errorf("%s: json encode: want errTooDeep, got %v", name, err)
		}
	}

	an := &arrNUNode{V: 1}
	an.Kids = []*arrNUNode{an}
	cyc("array-of-nullunion", arrNUSchema, an)

	ns := &arrNSNode{V: 1}
	ns.Kids = []*arrNSNode{ns}
	cyc("array-of-nullsecond-union", arrNSSchema, ns)

	mn := &mapNUNode{V: 1}
	mn.Kids = map[string]*mapNUNode{"a": mn}
	cyc("map-of-nullunion", mapNUSchema, mn)

	{
		na := &nuArrNode{V: 1}
		kids := []nuArrNode{{V: 2}}
		na.Kids = &kids
		(*na.Kids)[0].Kids = na.Kids // slice element references the same slice
		cyc("field-nullunion-of-array", nuArrSchema, na)
	}
	{
		nm := &nuMapNode{V: 1}
		m := map[string]nuMapNode{}
		nm.Kids = &m
		m["self"] = nuMapNode{V: 2, Kids: nm.Kids}
		cyc("field-nullunion-of-map", nuMapSchema, nm)
	}

	amn := &arrMapNUNode{V: 1}
	amn.Kids = []map[string]*arrMapNUNode{{"a": amn}}
	cyc("array-of-map-of-nullunion", arrMapNUSchema, amn)

	man := &mapArrNUNode{V: 1}
	man.Kids = map[string][]*mapArrNUNode{"a": {man}}
	cyc("map-of-array-of-nullunion", mapArrNUSchema, man)

	mb := &arrMBNode{V: 1}
	mb.Kids = []any{mb}
	cyc("array-of-multibranch-union", arrMBSchema, mb)

	// map[string]any self-reference against the recursive linked-list
	// schema (tagged-union self-ref) — a value-graph cycle, not a Go
	// pointer cycle.
	{
		s := MustParse(llSchema)
		m := map[string]any{"v": int32(1)}
		m["next"] = map[string]any{"LL": m}
		if _, err := s.Encode(m); !isTooDeep(err) {
			t.Errorf("map[string]any self-ref binary encode: want errTooDeep, got %v", err)
		}
		if _, err := s.EncodeJSON(m); !isTooDeep(err) {
			t.Errorf("map[string]any self-ref json encode: want errTooDeep, got %v", err)
		}
	}
}

// Compile-time assert the hand-built wire matches the encoder at a
// shallow depth (guards against a wire-builder typo silently measuring
// the wrong thing).
func TestDepthOracleWireMatchesEncoder(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		val    any
		wire   []byte
	}{
		{"linked-list", llSchema, llValue(3), llWire(3)},
		{"tree-value", treeSchema, treeVValue(3), treeWire(3)},
		{"map", mapSchema, mapValue(2), mapWire(2)},
		{"mutual-even", mrSchema, mrValue(2), mrWire(2)},
		{"mutual-odd", mrSchema, mrValue(3), mrWire(3)},
		{"array-of-nullunion", arrNUSchema, arrNUValue(3), arrNUWire(3)},
		{"array-of-nullunion-reflect", arrNUPPSchema, arrNUPPValue(3), arrNUPPWire(3)},
		{"array-of-nullsecond-union", arrNSSchema, arrNSValue(3), arrNSWire(3)},
		{"map-of-nullunion", mapNUSchema, mapNUValue(3), mapNUWire(3)},
		{"field-nullunion-of-array", nuArrSchema, nuArrValue(3), nuArrWire(3)},
		{"field-nullunion-of-map", nuMapSchema, nuMapValue(3), nuMapWire(3)},
		{"array-of-multibranch-union", arrMBSchema, arrMBValue(3), arrMBWire(3)},
		{"array-of-map-of-nullunion", arrMapNUSchema, arrMapNUValue(2), arrMapNUWire(2)},
		{"map-of-array-of-nullunion", mapArrNUSchema, mapArrNUValue(2), mapArrNUWire(2)},
	}
	for _, c := range cases {
		s := MustParse(c.schema)
		got, err := s.Encode(c.val)
		if err != nil {
			t.Fatalf("%s: encode: %v", c.name, err)
		}
		if fmt.Sprintf("% x", got) != fmt.Sprintf("% x", c.wire) {
			t.Errorf("%s: hand-wire mismatch\n encoder: % x\n hand:    % x", c.name, got, c.wire)
		}
	}
}

// ---------- degenerate_cardinality_test.go ----------

// Degenerate-cardinality types: zero-size fixed, zero-symbol enums, and
// zero-branch unions. The spec sets no minimum for any of the three, and Java,
// fastavro and avro-rs all parse them (Java's checkMaxBytesLength rejects only
// negative sizes, EnumSchema's constructor does per-symbol checks only, and
// UnionSchema's constructor loop no-ops on empty). A size-0 fixed is a usable
// type whose every value is the empty byte string; empty enums and unions are
// unusable-but-parseable, which matters for schema passthrough: a reader must be
// able to parse a foreign schema carrying a degenerate type in a position the
// data never exercises.

func TestRegression_FixedSizeZeroParses(t *testing.T) {
	for _, schema := range []string{
		`{"type":"fixed","name":"F","size":0}`,
		`{"type":"fixed","name":"F","size":"0"}`, // quoted-integer [INTEGERS] form
	} {
		if _, err := Parse(schema); err != nil {
			t.Errorf("Parse(%s) rejected size-0 fixed (Java/fastavro/avro-rs accept): %v", schema, err)
		}
	}
	// Negative sizes stay rejected on every form (Java parity).
	for _, schema := range []string{
		`{"type":"fixed","name":"F","size":-1}`,
		`{"type":"fixed","name":"F","size":"-1"}`,
	} {
		if _, err := Parse(schema); err == nil {
			t.Errorf("Parse(%s) accepted negative fixed size", schema)
		}
	}
}

func TestRegression_FixedSizeZeroWire(t *testing.T) {
	s, err := Parse(`{"type":"fixed","name":"F","size":0}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Every value is the empty byte string: 0 wire bytes.
	enc, err := s.AppendEncode(nil, []byte{})
	if err != nil {
		t.Fatalf("encode []byte{}: %v", err)
	}
	if len(enc) != 0 {
		t.Fatalf("size-0 fixed encoded to %d bytes, want 0", len(enc))
	}
	if _, err := s.AppendEncode(nil, [0]byte{}); err != nil {
		t.Errorf("encode [0]byte{}: %v", err)
	}
	if _, err := s.AppendEncode(nil, ""); err != nil {
		t.Errorf("encode \"\": %v", err)
	}
	// Wrong-length values reject, exactly like any other fixed.
	if _, err := s.AppendEncode(nil, []byte{1}); err == nil {
		t.Error("encode 1-byte value against size-0 fixed should error")
	}
	if _, err := s.AppendEncode(nil, "x"); err == nil {
		t.Error("encode 1-char string against size-0 fixed should error")
	}

	// Decode into every fixed-compatible target.
	var bs []byte
	if _, err := s.Decode(enc, &bs); err != nil {
		t.Errorf("decode []byte: %v", err)
	} else if len(bs) != 0 {
		t.Errorf("decode []byte: got %v, want empty", bs)
	}
	var arr [0]byte
	if _, err := s.Decode(enc, &arr); err != nil {
		t.Errorf("decode [0]byte: %v", err)
	}
	var str string
	if _, err := s.Decode(enc, &str); err != nil {
		t.Errorf("decode string: %v", err)
	} else if str != "" {
		t.Errorf("decode string: got %q, want empty", str)
	}
	var a any
	if _, err := s.Decode(enc, &a); err != nil {
		t.Errorf("decode any: %v", err)
	}

	// JSON wire form is the empty codepoint string.
	j, err := s.AppendEncodeJSON(nil, []byte{})
	if err != nil {
		t.Fatalf("encodeJSON: %v", err)
	}
	if string(j) != `""` {
		t.Errorf("encodeJSON: got %s, want \"\"", j)
	}
	var jb []byte
	if err := s.DecodeJSON([]byte(`""`), &jb); err != nil {
		t.Errorf("decodeJSON: %v", err)
	} else if len(jb) != 0 {
		t.Errorf("decodeJSON: got %v, want empty", jb)
	}

	// Canonical form keeps size 0; the schema fingerprints stably.
	if got := string(s.Canonical()); got != `{"name":"F","type":"fixed","size":0}` {
		t.Errorf("canonical: got %s", got)
	}

	// Metadata surfaces the zero size.
	if root := s.Root(); root.Size != 0 || root.Type != "fixed" {
		t.Errorf("Root(): Type=%q Size=%d", root.Type, root.Size)
	}

	// Metadata REBUILD: size is a required fixed attribute, so
	// Root().Schema() must re-emit "size":0 (not omit it as a zero
	// value) — at top level and nested in a union.
	for _, schema := range []string{
		`{"type":"fixed","name":"F","size":0}`,
		`["null",{"type":"fixed","name":"F","size":0}]`,
	} {
		ss := MustParse(schema)
		root := ss.Root()
		rebuilt, err := root.Schema()
		if err != nil {
			t.Errorf("Root().Schema() for %s: %v", schema, err)
			continue
		}
		if !bytes.Equal(ss.Fingerprint(NewRabin()), rebuilt.Fingerprint(NewRabin())) {
			t.Errorf("rebuild fingerprint mismatch for %s: rebuilt %s", schema, rebuilt.String())
		}
	}

	// A "" default on a size-0 fixed field validates (length 0 == size 0)
	// and fills on JSON decode.
	rs, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"f","type":{"type":"fixed","name":"F0","size":0},"default":""}]}`)
	if err != nil {
		t.Fatalf("parse record with size-0 fixed default: %v", err)
	}
	var out map[string]any
	if err := rs.DecodeJSON([]byte(`{}`), &out); err != nil {
		t.Fatalf("default fill: %v", err)
	}
}

func TestRegression_FixedSizeZeroArrayBounded(t *testing.T) {
	s := mustParse(t, `{"type":"array","items":{"type":"fixed","name":"F","size":0}}`)

	// A legitimate small array of zero-byte items round-trips.
	enc := mustAppendEncode(t, s, nil, [][]byte{{}, {}, {}})
	var got [][]byte
	mustDecode(t, s, enc, &got)
	if len(got) != 3 {
		t.Fatalf("got %d items, want 3", len(got))
	}

	// A hostile block count of zero-byte items hits the absolute
	// maxZeroByteItems cap instead of looping count times.
	hostile := appendVarlong(nil, 1<<40) // block count
	hostile = append(hostile, 0x00)      // terminator (never reached)
	var sink any
	if _, err := s.Decode(hostile, &sink); err == nil {
		t.Fatal("hostile zero-byte-item count must be rejected")
	} else if !strings.Contains(err.Error(), "zero-byte items") {
		t.Fatalf("expected zero-byte cap error, got: %v", err)
	}
}

func TestRegression_SchemaForZeroLenByteArrayField(t *testing.T) {
	type R struct {
		A [0]byte `avro:"a"`
		B int32   `avro:"b"`
	}
	s, err := SchemaFor[R]()
	if err != nil {
		t.Fatalf("SchemaFor rejected a valid Go type with a [0]byte field: %v", err)
	}
	enc, err := s.AppendEncode(nil, R{B: 7})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out R
	mustDecode(t, s, enc, &out)
	if out.B != 7 {
		t.Errorf("round-trip: got %+v", out)
	}
}

func TestRegression_EmptyEnumParses(t *testing.T) {
	s, err := Parse(`{"type":"enum","name":"E","symbols":[]}`)
	if err != nil {
		t.Fatalf("Parse rejected empty enum (Java/fastavro/avro-rs accept): %v", err)
	}
	if root := s.Root(); root.Type != "enum" || len(root.Symbols) != 0 {
		t.Errorf("Root(): Type=%q Symbols=%v", root.Type, root.Symbols)
	}
	if got := string(s.Canonical()); got != `{"name":"E","type":"enum","symbols":[]}` {
		t.Errorf("canonical: got %s", got)
	}

	// No valid values exist: every encode/decode errors, never panics.
	if _, err := s.AppendEncode(nil, "A"); err == nil {
		t.Error("encode symbol against empty enum should error")
	}
	if _, err := s.AppendEncode(nil, 0); err == nil {
		t.Error("encode ordinal against empty enum should error")
	}
	var str string
	if _, err := s.Decode([]byte{0x00}, &str); err == nil {
		t.Error("decode ordinal 0 against empty enum should error")
	}
	if _, err := s.AppendEncodeJSON(nil, "A"); err == nil {
		t.Error("encodeJSON against empty enum should error")
	}
	if err := s.DecodeJSON([]byte(`"A"`), &str); err == nil {
		t.Error("decodeJSON against empty enum should error")
	}

	// An enum-typed default on an empty enum rejects: no symbol is a
	// member (Java: EnumSchema constructor / isValidDefault containment).
	if _, err := Parse(`{"type":"enum","name":"E","symbols":[],"default":"A"}`); err == nil {
		t.Error("enum-level default on empty enum should reject")
	}
	if _, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"e","type":{"type":"enum","name":"E","symbols":[]},"default":"A"}]}`); err == nil {
		t.Error("field default on empty enum should reject")
	}

	// Union-default branch selection skips the empty-enum branch (Java's
	// isValidDefault anyMatch: no symbol matches, the string branch does).
	us, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"u","type":[{"type":"enum","name":"E","symbols":[]},"string"],"default":"A"}]}`)
	if err != nil {
		t.Fatalf("union default should pick the string branch: %v", err)
	}
	enc, err := us.AppendEncode(nil, map[string]any{})
	if err != nil {
		t.Fatalf("encode default: %v", err)
	}
	// Branch index 1 (string), zig-zag varint 0x02, then length-1 "A".
	if want := []byte{0x02, 0x02, 'A'}; !bytes.Equal(enc, want) {
		t.Errorf("default wire: got %x, want %x", enc, want)
	}

	// The metadata surface must pick the same branch as the wire. The
	// [empty-enum, bytes] pair discriminates: both branches' defaults are
	// JSON strings, but the bytes branch materializes []byte while a
	// vacuously-accepting empty-enum branch would surface string — so a
	// metadata-side branch-selection drift is visible in the Go type.
	bs, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"u","type":[{"type":"enum","name":"E","symbols":[]},"bytes"],"default":"Z"}]}`)
	if err != nil {
		t.Fatalf("union [empty-enum, bytes] default should pick bytes: %v", err)
	}
	benc, err := bs.AppendEncode(nil, map[string]any{})
	if err != nil {
		t.Fatalf("encode bytes default: %v", err)
	}
	// Branch index 1 (bytes), then length-1 0x5A.
	if want := []byte{0x02, 0x02, 'Z'}; !bytes.Equal(benc, want) {
		t.Errorf("bytes default wire: got %x, want %x", benc, want)
	}
	if d, ok := bs.Root().Fields[0].Default.([]byte); !ok || !bytes.Equal(d, []byte("Z")) {
		t.Errorf("metadata Default = %T %v, want []byte Z (same branch as wire)",
			bs.Root().Fields[0].Default, bs.Root().Fields[0].Default)
	}

	// In a null union the empty enum parses and nil round-trips.
	ns, err := Parse(`["null",{"type":"enum","name":"E","symbols":[]}]`)
	if err != nil {
		t.Fatalf("null union with empty enum: %v", err)
	}
	nenc, err := ns.AppendEncode(nil, nil)
	if err != nil {
		t.Fatalf("encode nil: %v", err)
	}
	var av any = "sentinel"
	if _, err := ns.Decode(nenc, &av); err != nil || av != nil {
		t.Errorf("nil round-trip: v=%v err=%v", av, err)
	}
}

func TestRegression_EmptyEnumResolve(t *testing.T) {
	full := MustParse(`{"type":"enum","name":"E","symbols":["A"]}`)
	empty := MustParse(`{"type":"enum","name":"E","symbols":[]}`)

	// Writer has symbols the empty reader can never map, and an empty
	// enum cannot declare a default: eager-fail at Resolve.
	if _, err := Resolve(full, empty); err == nil {
		t.Error("Resolve(full→empty) should fail: unmappable symbols, no default possible")
	}
	// Writer empty → reader full: no wire symbol can ever arrive;
	// vacuously compatible.
	if _, err := Resolve(empty, full); err != nil {
		t.Errorf("Resolve(empty→full) should be vacuously compatible: %v", err)
	}
	if _, err := Resolve(empty, empty); err != nil {
		t.Errorf("Resolve(empty→empty): %v", err)
	}
}

func TestMatrix_EmptyUnionParses(t *testing.T) {
	s, err := Parse(`[]`)
	if err != nil {
		t.Fatalf("Parse rejected empty union (Java/fastavro/avro-rs accept): %v", err)
	}
	root := s.Root()
	if root.Type != "union" || len(root.Branches) != 0 {
		t.Errorf("Root(): Type=%q Branches=%d", root.Type, len(root.Branches))
	}
	if got := string(s.Canonical()); got != `[]` {
		t.Errorf("canonical: got %s, want []", got)
	}
	// SchemaNode.Schema() re-emits a parseable empty union.
	rt, err := root.Schema()
	if err != nil {
		t.Fatalf("Root().Schema(): %v", err)
	}
	if got := string(rt.Canonical()); got != `[]` {
		t.Errorf("round-trip canonical: got %s", got)
	}

	// No value can encode or decode; every path errors, never panics.
	for _, v := range []any{nil, int32(1), "x", []byte{1}, map[string]any{"int": 1}} {
		if _, err := s.AppendEncode(nil, v); err == nil {
			t.Errorf("encode %#v against empty union should error", v)
		}
		if _, err := s.AppendEncodeJSON(nil, v); err == nil {
			t.Errorf("encodeJSON %#v against empty union should error", v)
		}
	}
	var a any
	if _, err := s.Decode([]byte{0x00}, &a); err == nil {
		t.Error("decode index 0 against empty union should error")
	}
	for _, j := range []string{`null`, `1`, `"x"`, `{"int":1}`} {
		if err := s.DecodeJSON([]byte(j), &a); err == nil {
			t.Errorf("decodeJSON %s against empty union should error", j)
		}
	}

	// A union may not immediately contain another union — including an
	// empty one (Java: "Nested union"). Must error, not panic.
	if _, err := Parse(`[["int","null"]]`); err == nil {
		t.Error("union containing a union must reject")
	}
	if _, err := Parse(`[[]]`); err == nil {
		t.Error("union containing an empty union must reject")
	}

	// No default can match a zero-branch union; absent default is fine.
	if _, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"u","type":[],"default":null}]}`); err == nil {
		t.Error("default on empty-union field should reject (no branch accepts)")
	}
	rs, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"u","type":[]}]}`)
	if err != nil {
		t.Fatalf("record with empty-union field should parse: %v", err)
	}
	if _, err := rs.AppendEncode(nil, map[string]any{"u": 1}); err == nil {
		t.Error("encoding a record with an empty-union field should error")
	}
}

func TestRegression_EmptyUnionContainers(t *testing.T) {
	as, err := Parse(`{"type":"array","items":[]}`)
	if err != nil {
		t.Fatalf("array of empty union: %v", err)
	}
	// The empty array is the only inhabitable value.
	enc, err := as.AppendEncode(nil, []any{})
	if err != nil {
		t.Fatalf("encode empty array: %v", err)
	}
	var got []any
	if _, err := as.Decode(enc, &got); err != nil {
		t.Fatalf("decode empty array: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %v", got)
	}
	if _, err := as.AppendEncode(nil, []any{int32(1)}); err == nil {
		t.Error("non-empty array of empty union should error")
	}
	// Wire claiming items must error (first item has no valid branch),
	// without panicking or spinning.
	hostile := appendVarlong(nil, 3)
	hostile = append(hostile, 0x00, 0x00, 0x00, 0x00)
	var sink any
	if _, err := as.Decode(hostile, &sink); err == nil {
		t.Error("array wire with empty-union items must error")
	}

	ms, err := Parse(`{"type":"map","values":[]}`)
	if err != nil {
		t.Fatalf("map of empty union: %v", err)
	}
	menc, err := ms.AppendEncode(nil, map[string]any{})
	if err != nil {
		t.Fatalf("encode empty map: %v", err)
	}
	var mgot map[string]any
	if _, err := ms.Decode(menc, &mgot); err != nil {
		t.Fatalf("decode empty map: %v", err)
	}
	// A wire block claiming an entry must error cleanly.
	mhostile := appendVarlong(nil, 1)
	mhostile = append(mhostile, 0x02, 'k', 0x00, 0x00)
	if _, err := ms.Decode(mhostile, &sink); err == nil {
		t.Error("map wire with empty-union values must error")
	}
}

func TestRegression_EmptyUnionResolve(t *testing.T) {
	empty := MustParse(`[]`)
	intS := MustParse(`"int"`)

	// Writer empty union: no branch can ever appear on the wire, so any
	// reader is vacuously compatible (Java's WriterUnion builds per-branch
	// actions over zero branches and can never error at decode).
	if _, err := Resolve(empty, intS); err != nil {
		t.Errorf("Resolve(empty union → int) should be vacuously compatible: %v", err)
	}
	if err := CheckCompatibility(empty, intS); err != nil {
		t.Errorf("CheckCompatibility(empty union → int): %v", err)
	}
	// Reader empty union: no branch can accept the writer's values.
	if _, err := Resolve(intS, empty); err == nil {
		t.Error("Resolve(int → empty union) should fail: no reader branch matches")
	}
	if err := CheckCompatibility(intS, empty); err == nil {
		t.Error("CheckCompatibility(int → empty union) should fail")
	}
	// Empty ↔ empty: vacuous.
	if _, err := Resolve(empty, empty); err != nil {
		t.Errorf("Resolve(empty → empty): %v", err)
	}
}

// ---------- union_forwardref_test.go ----------

// A union may not contain the same named type twice (spec, "Unions":
// "Unions may not contain more than one schema with the same type, except
// for the named types record, fixed and enum ... two types with different
// names are permitted"). The duplicate check must be reference-order
// independent: a short-name forward reference and a later inline
// definition of the same type are the same union member, exactly as the
// backward-ordered spelling is.
func TestMatrix_UnionForwardRefDuplicateOrderIndependent(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		ok     bool
		// lossyCanonical marks schemas with a null-namespace type nested
		// inside a namespaced scope: the PCF [FULLNAMES] transform writes
		// that type's fullname as a bare name, which re-reads as
		// inheriting the enclosing namespace — so the canonical form does
		// not re-parse. Java's SchemaNormalization emits the identical
		// ambiguity; PCF is a fingerprint surface, not a round-trip
		// surface.
		lossyCanonical bool
	}{
		{
			// forward short-name ref + inline definition: duplicate.
			name: "fwd ref then inline",
			schema: `{"type":"record","name":"R","namespace":"n","fields":[
				{"name":"f","type":["Inner",{"type":"fixed","name":"Inner","size":4}]}
			]}`,
			ok: false,
		},
		{
			// the branch-swapped spelling of the same union: duplicate.
			name: "inline then backward ref",
			schema: `{"type":"record","name":"R","namespace":"n","fields":[
				{"name":"f","type":[{"type":"fixed","name":"Inner","size":4},"Inner"]}
			]}`,
			ok: false,
		},
		{
			// full-name forward ref + inline definition: duplicate.
			name: "full-name fwd ref then inline",
			schema: `{"type":"record","name":"R","namespace":"n","fields":[
				{"name":"f","type":["n.Inner",{"type":"fixed","name":"Inner","size":4}]}
			]}`,
			ok: false,
		},
		{
			// a forward reference whose definition lives in a LATER field is
			// not a duplicate: the union holds the type once.
			name: "fwd ref defined in sibling field",
			schema: `{"type":"record","name":"R","namespace":"n","fields":[
				{"name":"f","type":["null","Inner"]},
				{"name":"g","type":{"type":"fixed","name":"Inner","size":4}}
			]}`,
			ok: true,
		},
		{
			// two distinct named types sharing a short name across
			// namespaces are NOT duplicates.
			name: "same short name distinct namespaces",
			schema: `{"type":"record","name":"R","namespace":"n","fields":[
				{"name":"f","type":[{"type":"fixed","name":"Inner","namespace":"a","size":4},{"type":"fixed","name":"Inner","namespace":"b","size":4}]}
			]}`,
			ok: true,
		},
		{
			// two identically-spelled forward refs resolve to the same
			// type: duplicate (caught after resolution).
			name: "two identical fwd refs",
			schema: `{"type":"record","name":"R","namespace":"n","fields":[
				{"name":"f","type":["Inner","Inner"]},
				{"name":"g","type":{"type":"fixed","name":"Inner","size":4}}
			]}`,
			ok: false,
		},
		{
			// References bind EAGERLY at the point of reference: with the
			// null-namespace Inner already defined (earlier branch) and
			// n.Inner not yet defined, the bare ref binds the null-ns
			// type — a genuine duplicate of the sibling branch. A later
			// definition does not retroactively rebind (old-Java
			// Names.get semantics; only never-resolvable refs defer to
			// finalize).
			name: "bare ref eagerly binds existing null-ns sibling",
			schema: `{"type":"record","name":"R","namespace":"n","fields":[
				{"name":"f","type":[{"type":"fixed","name":"Inner","namespace":"","size":8},"Inner"]},
				{"name":"g","type":{"type":"fixed","name":"Inner","size":4}}
			]}`,
			ok: false,
		},
		{
			// The same union with the ref FIRST: nothing named Inner
			// exists at reference time, so the ref defers to finalize,
			// where in-scope-first binding picks n.Inner (the later
			// sibling field's type) — two distinct types, not a
			// duplicate.
			name: "deferred fwd ref binds in-scope over null-ns sibling",
			schema: `{"type":"record","name":"R","namespace":"n","fields":[
				{"name":"f","type":["Inner",{"type":"fixed","name":"Inner","namespace":"","size":8}]},
				{"name":"g","type":{"type":"fixed","name":"Inner","size":4}}
			]}`,
			ok:             true,
			lossyCanonical: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := Parse(c.schema)
			if c.ok {
				if err != nil {
					t.Fatalf("Parse: %v", err)
				}
				// Every accepted schema's canonical form must re-parse
				// (Canonical() output is what registries store) — except
				// the documented PCF null-namespace lossiness class.
				if !c.lossyCanonical {
					if _, err := Parse(string(s.Canonical())); err != nil {
						t.Fatalf("Parse(s.Canonical()): %v\ncanonical: %s", err, s.Canonical())
					}
				}
				return
			}
			if err == nil {
				t.Fatalf("Parse accepted a union containing the same named type twice\ncanonical: %s", s.Canonical())
			}
		})
	}
}

// TaggedUnions branch names are the RESOLVED full names regardless of
// whether the branch was a forward reference or an in-order reference:
// a named reference is position-independent in Avro, so the tagged
// envelope key and the tagged-map encode acceptance cannot depend on
// where the definition appeared. Binary and JSON must agree.
func TestRegression_UnionForwardRefTaggedNamesResolved(t *testing.T) {
	const fwd = `{"type":"record","name":"R","namespace":"n","fields":[
		{"name":"f","type":["null","Inner"]},
		{"name":"g","type":{"type":"fixed","name":"Inner","size":4}}]}`
	const ord = `{"type":"record","name":"R","namespace":"n","fields":[
		{"name":"g","type":{"type":"fixed","name":"Inner","size":4}},
		{"name":"f","type":["null","Inner"]}]}`

	type rec struct {
		F *[4]byte `avro:"f"`
		G [4]byte  `avro:"g"`
	}
	val := rec{F: &[4]byte{1, 2, 3, 4}, G: [4]byte{9, 9, 9, 9}}

	sf := MustParse(fwd)
	so := MustParse(ord)

	// (a) binary TaggedUnions decode envelope: full name on both schemas.
	envelope := func(s *Schema) any {
		t.Helper()
		wire := mustAppendEncode(t, s, nil, val)
		var out any
		if _, err := s.Decode(wire, &out, TaggedUnions()); err != nil {
			t.Fatalf("decode tagged: %v", err)
		}
		return out.(map[string]any)["f"]
	}
	envF, envO := envelope(sf), envelope(so)
	want := map[string]any{"n.Inner": []byte{1, 2, 3, 4}}
	if !reflect.DeepEqual(envF, want) {
		t.Errorf("forward-ref schema binary envelope: got %#v, want %#v", envF, want)
	}
	if !reflect.DeepEqual(envO, want) {
		t.Errorf("in-order schema binary envelope: got %#v, want %#v", envO, want)
	}

	// (b) JSON TaggedUnions envelope agrees with binary on the fwd schema.
	js, err := sf.EncodeJSON(val)
	if err != nil {
		t.Fatalf("encodeJSON: %v", err)
	}
	var outJ any
	if err := sf.DecodeJSON(js, &outJ, TaggedUnions()); err != nil {
		t.Fatalf("decodeJSON tagged: %v", err)
	}
	if envJ := outJ.(map[string]any)["f"]; !reflect.DeepEqual(envJ, want) {
		t.Errorf("forward-ref schema JSON envelope: got %#v, want %#v", envJ, want)
	}

	// (c) tagged-map binary encode accepts the full name AND the unique
	// short name on both schemas, producing identical wire bytes.
	type recTagged struct {
		F map[string]any `avro:"f"`
		G [4]byte        `avro:"g"`
	}
	wireTyped, err := sf.AppendEncode(nil, val)
	if err != nil {
		t.Fatalf("encode typed: %v", err)
	}
	for _, s := range []*Schema{sf, so} {
		for _, tag := range []string{"n.Inner", "Inner"} {
			got, err := s.AppendEncode(nil, recTagged{F: map[string]any{tag: [4]byte{1, 2, 3, 4}}, G: [4]byte{9, 9, 9, 9}})
			if err != nil {
				t.Errorf("tagged-map encode with key %q: %v", tag, err)
				continue
			}
			if s == sf && !bytes.Equal(got, wireTyped) {
				t.Errorf("tagged-map encode with key %q: wire differs from typed encode", tag)
			}
		}
	}
}

// TagLogicalTypes branch names resolve through forward references too:
// a logical-bearing NAMED branch (a fixed carrying a logical type) tags
// under its NAME — not "<kind>.<logical>" — regardless of reference order.
// This exercises the forward-reference path (finalizeUnionNames), which
// re-derives the branch-name tables after the fwd-ref branch resolves; the
// resolved name must match what an in-order reference produces.
func TestRegression_UnionForwardRefTagLogicalNamesResolved(t *testing.T) {
	const fwd = `{"type":"record","name":"R","namespace":"n","fields":[
		{"name":"f","type":["null","Dec"]},
		{"name":"g","type":{"type":"fixed","name":"Dec","size":4,"logicalType":"decimal","precision":9,"scale":2}}]}`
	s := MustParse(fwd)

	type rec struct {
		F *[4]byte `avro:"f"`
		G [4]byte  `avro:"g"`
	}
	val := rec{F: &[4]byte{0, 0, 0, 1}, G: [4]byte{0, 0, 0, 2}}
	wire := mustAppendEncode(t, s, nil, val)
	var out any
	if _, err := s.Decode(wire, &out, TaggedUnions(), TagLogicalTypes()); err != nil {
		t.Fatalf("decode tag-logical: %v", err)
	}
	env, ok := out.(map[string]any)["f"].(map[string]any)
	if !ok {
		t.Fatalf("envelope shape: %#v", out.(map[string]any)["f"])
	}
	// The fixed is defined in namespace "n", so its tagged-union key is the
	// fully-qualified name "n.Dec" — matching goavro's typeName.fullName and
	// Java's getFullName(), both of which qualify with the namespace.
	if _, ok := env["n.Dec"]; !ok {
		t.Errorf("logical tag: got keys %v, want n.Dec (the fixed's fullname)", reflect.ValueOf(env).MapKeys())
	}
}
