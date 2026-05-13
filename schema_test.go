package avro

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestCanonical(t *testing.T) {
	s, err := Parse(`{"type":"record","name":"r","fields":[{"name":"a","type":"int"}]}`)
	if err != nil {
		t.Fatal(err)
	}
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
			"error preserves type",
			`{"type":"error","name":"E","fields":[{"name":"x","type":"int"}]}`,
			`{"name":"E","type":"error","fields":[{"name":"x","type":"int"}]}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatal(err)
			}
			got := string(s.Canonical())
			if got != tt.want {
				t.Errorf("got  %s\nwant %s", got, tt.want)
			}
		})
	}
}

func TestFingerprint(t *testing.T) {
	s, err := Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
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

func TestMarshalJSON(t *testing.T) {
	t.Run("primitive", func(t *testing.T) {
		s := aschema{primitive: "int"}
		b, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != `"int"` {
			t.Errorf("got %s, want \"int\"", b)
		}
	})

	t.Run("object", func(t *testing.T) {
		s := aschema{object: &aobject{Name: "r", Type: "record"}}
		b, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(b), `"name":"r"`) {
			t.Errorf("got %s, want object with name r", b)
		}
	})

	// Exercise aobject.MarshalJSON's non-PCF attribute branches. These
	// paths are stripped from canonical form (Canonical() zeroes
	// namespace, aliases, default, logicalType, precision, scale before
	// calling MarshalJSON), so they only run when aobject is serialized
	// directly. We keep them so the MarshalJSON is a proper full-schema
	// marshal rather than a canonical-only marshal, and cover them here.
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
		b, err := json.Marshal(o)
		if err != nil {
			t.Fatal(err)
		}
		got := string(b)
		// PCF-ordered keys first (name, type, fields), then non-PCF
		// attributes in declaration order.
		want := `{"name":"r","type":"record","fields":[],"namespace":"com.example","aliases":["old"],"default":null,"logicalType":"decimal","precision":9,"scale":2}`
		if got != want {
			t.Errorf("\n got %s\nwant %s", got, want)
		}
	})

	// Defensive branches: a non-record type with populated Fields, or a
	// non-enum type with populated Symbols, is nonsense per the Avro
	// spec, but MarshalJSON still emits them for debuggability.
	t.Run("object defensive fields on non-record", func(t *testing.T) {
		o := aobject{
			Type:   "int",
			Fields: []afield{{Name: "x", Type: &aschema{primitive: "int"}}},
		}
		b, err := json.Marshal(o)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(b), `"fields":[`) {
			t.Errorf("got %s, want fields to be emitted", b)
		}
	})
	t.Run("object defensive symbols on non-enum", func(t *testing.T) {
		o := aobject{
			Type:    "int",
			Symbols: []string{"A", "B"},
		}
		b, err := json.Marshal(o)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(b), `"symbols":["A","B"]`) {
			t.Errorf("got %s, want symbols to be emitted", b)
		}
	})

	// Enum with nil Symbols slice: MarshalJSON emits "symbols":[] to
	// satisfy the spec's required attribute.
	t.Run("object nil enum symbols", func(t *testing.T) {
		o := aobject{
			Name: "E",
			Type: "enum",
		}
		b, err := json.Marshal(o)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != `{"name":"E","type":"enum","symbols":[]}` {
			t.Errorf("got %s", b)
		}
	})

	t.Run("union", func(t *testing.T) {
		s := aschema{union: []aschema{{primitive: "null"}, {primitive: "int"}}}
		b, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != `["null","int"]` {
			t.Errorf("got %s", b)
		}
	})

	t.Run("empty", func(t *testing.T) {
		s := aschema{}
		_, err := json.Marshal(s)
		if err == nil {
			t.Fatal("expected error for empty schema")
		}
	})
}

func TestUnmarshalJSONInvalid(t *testing.T) {
	var s aschema
	// Invalid first byte (number).
	err := s.UnmarshalJSON([]byte(`123`))
	if err == nil {
		t.Fatal("expected error")
	}

	// Empty data.
	err = s.UnmarshalJSON([]byte(``))
	if err == nil {
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}
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
		{"zero string", `{"type":"fixed","name":"F","size":"0"}`},
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
	// Java's schema parser coerces string defaults for float/double fields.
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
			"nullable float string default",
			`{"type":"record","name":"R","fields":[
				{"name":"f","type":["float","null"],"default":"1.5"}
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
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}
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
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}
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
		{
			"flat enum empty symbols",
			`{"type":"record","name":"R","fields":[
				{"name":"E","type":"enum","symbols":[]}
			]}`,
		},
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
	s, err := Parse(schema)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
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

	tests := []struct {
		name    string
		obj     aobject
		wantErr bool
	}{
		{"no logical", aobject{Type: "int"}, false},

		// decimal
		{"decimal ok bytes", aobject{Type: "bytes", Logical: "decimal", Precision: &somePrec}, false},
		{"decimal ok fixed", aobject{Type: "fixed", Logical: "decimal", Precision: &somePrec, Size: &intSize}, false},
		{"decimal missing precision", aobject{Type: "bytes", Logical: "decimal"}, false},
		{"decimal wrong type", aobject{Type: "int", Logical: "decimal", Precision: &somePrec}, false},

		// uuid
		{"uuid ok", aobject{Type: "string", Logical: "uuid"}, false},
		{"uuid wrong type", aobject{Type: "int", Logical: "uuid"}, true},
		{"uuid with scale", aobject{Type: "string", Logical: "uuid", Scale: &zeroPrec}, true},

		// date
		{"date ok", aobject{Type: "int", Logical: "date"}, false},
		{"date wrong type", aobject{Type: "long", Logical: "date"}, true},

		// time-millis
		{"time-millis ok", aobject{Type: "int", Logical: "time-millis"}, false},
		{"time-millis wrong type", aobject{Type: "long", Logical: "time-millis"}, true},

		// time-micros
		{"time-micros ok", aobject{Type: "long", Logical: "time-micros"}, false},
		{"time-micros wrong type", aobject{Type: "int", Logical: "time-micros"}, true},

		// timestamp-millis
		{"timestamp-millis ok", aobject{Type: "long", Logical: "timestamp-millis"}, false},
		{"timestamp-millis wrong type", aobject{Type: "int", Logical: "timestamp-millis"}, true},

		// timestamp-micros
		{"timestamp-micros ok", aobject{Type: "long", Logical: "timestamp-micros"}, false},
		{"timestamp-micros wrong type", aobject{Type: "int", Logical: "timestamp-micros"}, true},

		// local-timestamp-millis
		{"local-timestamp-millis ok", aobject{Type: "long", Logical: "local-timestamp-millis"}, false},

		// local-timestamp-micros
		{"local-timestamp-micros ok", aobject{Type: "long", Logical: "local-timestamp-micros"}, false},

		// duration
		{"duration ok", aobject{Type: "fixed", Logical: "duration", Size: &intSize}, false},
		{"duration wrong type", aobject{Type: "int", Logical: "duration"}, true},
		{"duration no size", aobject{Type: "fixed", Logical: "duration"}, true},
		{"duration wrong size", aobject{Type: "fixed", Logical: "duration", Size: ptr(laxInt(10))}, true},

		// unknown logical types are ignored per spec
		{"unknown logical", aobject{Type: "int", Logical: "foobar"}, false},

		// scale/precision on non-decimal
		{"date with precision", aobject{Type: "int", Logical: "date", Precision: &somePrec}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.obj.validateLogical()
			if (err != nil) != tt.wantErr {
				t.Errorf("validateLogical() error = %v, wantErr %v", err, tt.wantErr)
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
		_, err := Parse(`{"type":"array","namespace":"com","items":"int"}`)
		if err == nil {
			t.Fatal("expected error for namespaced array")
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
	if _, err := s.Decode(dst, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != 42 {
		t.Errorf("got %d, want 42", got)
	}
}

func TestBuildComplexValidateLogicalError(t *testing.T) {
	// Known logical type on wrong underlying type should error through buildComplex.
	_, err := Parse(`{"type":"string","logicalType":"date"}`)
	if err == nil {
		t.Fatal("expected error for date on string type")
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
		s, err := Parse(`{"type":"record","name":"com.example.MyRecord","fields":[{"name":"x","type":"int"}]}`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
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
	type Inner struct {
		X int32 `avro:"x"`
	}
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
	s, err := Parse(schema)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
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
		{"invalid type alias", `{"type":"record","name":"R","aliases":["bad-alias"],"fields":[{"name":"x","type":"int"}]}`},
		{"empty field name", `{"type":"record","name":"R","fields":[{"name":"","type":"int"}]}`},
		{"invalid field name", `{"type":"record","name":"R","fields":[{"name":"bad-field!","type":"int"}]}`},
		{"invalid field alias", `{"type":"record","name":"R","fields":[{"name":"x","type":"int","aliases":["bad-alias!"]}]}`},
		{"empty enum symbols", `{"type":"enum","name":"E","symbols":[]}`},
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

// TestFieldLevelLogicalType_RoundTrip exercises the Java/JDBC Avro idiom
// where the `logicalType` annotation (and, for decimal, `precision`/
// `scale`) sits as a sibling of `type` on the field object rather than
// nested inside the type definition. Confluent's Java code generator,
// kafka-connect-avro-converter, and most Debezium CDC sources (Oracle,
// MySQL, PostgreSQL) emit schemas in this shape. The on-wire encoding
// is identical to the nested form; only the JSON layout differs.
//
// Each case constructs a strongly-typed Go value, encodes through the
// flat-form schema, and decodes back through the same schema. Before
// the lift, encoding a `time.Time` (or `time.Duration` etc.) against a
// flat-form schema produced "avro: field x: cannot use <Go type> with
// Avro type long/int/string" because the parser dropped the field-level
// annotation and built a plain-primitive schema. After the lift the
// schema knows the field is logical and the round-trip succeeds.
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
			`{"type":"record","name":"R","fields":[
				{"name":"ts","type":"long","logicalType":"timestamp-millis"}
			]}`,
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
			s, err := Parse(tc.schema)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}

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

// TestFieldLevelLogicalType_RoundTripValue exercises the actual decoder.
// Before the lift, encoding a time.Time against a flat-form schema
// errored with "cannot use time.Time with Avro type long" because the
// parser dropped the field-level annotation. With the lift, the schema
// recognises the field as timestamp-millis and the round-trip succeeds.
//
// We only assert this for timestamp-millis (primitive and union) because
// the value-side decoder already has unit tests for every other logical
// type via the nested form; this test's purpose is to prove the flat
// form reaches the same decoder path, not to re-cover every type.
func TestFieldLevelLogicalType_RoundTripValue(t *testing.T) {
	type Row struct {
		TS time.Time `avro:"ts"`
	}

	cases := []struct {
		name   string
		schema string
	}{
		{
			"primitive timestamp-millis",
			`{"type":"record","name":"R","fields":[
				{"name":"ts","type":"long","logicalType":"timestamp-millis"}
			]}`,
		},
		{
			"union timestamp-millis (null first)",
			`{"type":"record","name":"R","fields":[
				{"name":"ts","type":["null","long"],"logicalType":"timestamp-millis"}
			]}`,
		},
	}

	want := time.UnixMilli(1_700_000_000_000).UTC()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := Parse(tc.schema)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			data, err := s.Encode(&Row{TS: want})
			if err != nil {
				t.Fatalf("encode time.Time into flat-form schema: %v", err)
			}
			var got Row
			if _, err := s.Decode(data, &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !got.TS.Equal(want) {
				t.Fatalf("round-trip mismatch: got %v, want %v", got.TS, want)
			}
		})
	}
}

// TestFieldLevelLogicalType_NestedAnnotationWins covers the edge case
// where both a nested and a field-level annotation are present. The
// closer-to-the-type annotation wins so that an explicit author choice
// is never overridden by an outer scope.
func TestFieldLevelLogicalType_NestedAnnotationWins(t *testing.T) {
	s, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"ts","type":{"type":"long","logicalType":"timestamp-micros"},"logicalType":"timestamp-millis"}
	]}`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
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
// load-bearing case for the lift because it also propagates field-level
// `precision` and `scale` — not just `logicalType`. Before the lift the
// parser dropped all three and Encode/Decode of a *big.Rat errored with
// "cannot use *big.Rat with Avro type bytes".
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
			if _, err := s.Decode(data, &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
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
			`{"type":"record","name":"R","fields":[
				{"name":"ts","type":"long","logicalType":"timestamp-millis"}
			]}`,
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
			s, err := Parse(tc.schema)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
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

// TestFieldLevelLogicalType_FingerprintsMatch is the load-bearing
// drop-in-compatibility invariant: flat-form and nested-form schemas
// must produce byte-identical canonical output (and therefore identical
// fingerprints) so that downstream tooling — schema registries, schema
// caches, anything keyed on fingerprint — treats them as the same
// schema.
func TestFieldLevelLogicalType_FingerprintsMatch(t *testing.T) {
	cases := []struct {
		name        string
		flat        string
		nested      string
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

	flat, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"ts","type":"long","logicalType":"timestamp-millis"}
	]}`)
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
	s, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"v","type":["null","long","string"],"logicalType":"timestamp-millis"}
	]}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
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
