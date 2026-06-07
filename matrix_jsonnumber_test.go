package avro_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// Generative json.Number policy net.
//
// The documented policy (doc.go "Encoding from JSON input") is: json.Number
// is a NUMERIC carrier — accepted for numeric Avro types (int/long/float/
// double and their logical variants), REJECTED for stringy types (string/
// bytes/fixed/enum) on BOTH encode and decode, with map keys the one
// content-validated exception. Before this net that policy was asserted only
// by ~12 hand-written TestRegression_JSONNumber* pins at specific call sites;
// neutering any single guard (the encode rejects, the decode rejects, the
// per-key validation, or the Pattern-14c fast-path gates) was caught ONLY by
// those pins — the combinatorial matrix, property, and invariant nets all
// stayed green, because the matrix carried json.Number as a numeric ACCEPT
// target but never swept the REJECT direction across positions. A json.Number
// bug in a position nobody pinned was therefore invisible.
//
// This net sweeps the policy as a cross-product: {numeric, stringy} schema ×
// {top, field, array-item, map-value} position × {encode-source,
// decode-target} direction × {binary, JSON} wire. Numeric cells must accept
// (and round-trip); stringy cells must REJECT on both wires. New schema
// fragments or positions inherit the invariant automatically.
// ---------------------------------------------------------------------------

var jnNumericSchemas = []struct {
	label  string
	schema string
}{
	{"int", `"int"`},
	{"long", `"long"`},
	{"float", `"float"`},
	{"double", `"double"`},
	{"date", `{"type":"int","logicalType":"date"}`},
	{"time-millis", `{"type":"int","logicalType":"time-millis"}`},
	{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`},
	{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`},
}

var jnStringySchemas = []struct {
	label  string
	schema string
}{
	{"string", `"string"`},
	{"bytes", `"bytes"`},
	{"fixed", `{"type":"fixed","name":"JNF","size":2}`},
	{"enum", `{"type":"enum","name":"JNE","symbols":["A","B"]}`},
}

// jnPositions wrap a leaf schema and a leaf value into a composed schema and
// value, and describe how to build a typed decode target carrying
// json.Number at the leaf.
var jnPositions = []struct {
	label     string
	schema    func(leaf string) string
	encodeVal func(leaf any) any                 // leaf is a json.Number
	target    func(t reflect.Type) reflect.Value // ptr to a value with json.Number leaves
}{
	{"top",
		func(leaf string) string { return leaf },
		func(leaf any) any { return leaf },
		func(t reflect.Type) reflect.Value { return reflect.New(t) }},
	{"field",
		func(leaf string) string {
			return fmt.Sprintf(`{"type":"record","name":"JNR","fields":[{"name":"f","type":%s}]}`, leaf)
		},
		func(leaf any) any { return map[string]any{"f": leaf} },
		func(t reflect.Type) reflect.Value {
			return reflect.New(reflect.MapOf(reflect.TypeFor[string](), t))
		}},
	{"array-item",
		func(leaf string) string { return fmt.Sprintf(`{"type":"array","items":%s}`, leaf) },
		func(leaf any) any { return []any{leaf} },
		func(t reflect.Type) reflect.Value { return reflect.New(reflect.SliceOf(t)) }},
	{"map-value",
		func(leaf string) string { return fmt.Sprintf(`{"type":"map","values":%s}`, leaf) },
		func(leaf any) any { return map[string]any{"k": leaf} },
		func(t reflect.Type) reflect.Value {
			return reflect.New(reflect.MapOf(reflect.TypeFor[string](), t))
		}},
}

func TestMatrix_JSONNumberPolicy(t *testing.T) {
	jnType := reflect.TypeFor[json.Number]()

	for _, pos := range jnPositions {
		// ---- NUMERIC schemas: json.Number must ACCEPT (encode + decode). ----
		for _, sc := range jnNumericSchemas {
			t.Run("numeric/"+sc.label+"/"+pos.label, func(t *testing.T) {
				s := avro.MustParse(pos.schema(sc.schema))
				in := pos.encodeVal(json.Number("42"))
				// Encode source: both wires must accept.
				wire, err := s.AppendEncode(nil, in)
				if err != nil {
					t.Fatalf("binary encode of json.Number source rejected for numeric schema: %v", err)
				}
				if _, err := s.AppendEncodeJSON(nil, in); err != nil {
					t.Fatalf("JSON encode of json.Number source rejected for numeric schema: %v", err)
				}
				// Decode target: a json.Number-leaf target must accept the
				// numeric wire on both formats.
				tgt := pos.target(jnType)
				if _, err := s.Decode(wire, tgt.Interface()); err != nil {
					t.Fatalf("binary decode into json.Number target rejected for numeric schema: %v", err)
				}
				jwire, _ := s.AppendEncodeJSON(nil, in)
				jtgt := pos.target(jnType)
				if err := s.DecodeJSON(jwire, jtgt.Interface()); err != nil {
					t.Fatalf("JSON decode into json.Number target rejected for numeric schema: %v", err)
				}
			})
		}

		// ---- STRINGY schemas: json.Number must REJECT (encode + decode). ----
		for _, sc := range jnStringySchemas {
			t.Run("stringy/"+sc.label+"/"+pos.label, func(t *testing.T) {
				s := avro.MustParse(pos.schema(sc.schema))

				// Encode source: a json.Number leaf must be rejected on both
				// wires (it is a numeric carrier; a text/binary target is a
				// type mismatch).
				in := pos.encodeVal(json.Number("42"))
				if _, err := s.AppendEncode(nil, in); err == nil {
					t.Errorf("binary encode of json.Number ACCEPTED for stringy schema %s (must reject)", sc.label)
				}
				if _, err := s.AppendEncodeJSON(nil, in); err == nil {
					t.Errorf("JSON encode of json.Number ACCEPTED for stringy schema %s (must reject)", sc.label)
				}

				// Decode target: a valid stringy wire decoded INTO a
				// json.Number leaf must reject on both wires. Build the wire
				// from a string-typed source the schema accepts.
				strLeaf := jnStringSample(sc.label)
				goodIn := pos.encodeVal(strLeaf)
				wire, err := s.AppendEncode(nil, goodIn)
				if err != nil {
					t.Fatalf("setup: encoding a valid stringy value failed: %v", err)
				}
				if _, err := s.Decode(wire, pos.target(jnType).Interface()); err == nil {
					t.Errorf("binary decode of stringy wire INTO json.Number target ACCEPTED for %s (must reject)", sc.label)
				}
				jwire, err := s.AppendEncodeJSON(nil, goodIn)
				if err != nil {
					t.Fatalf("setup: JSON-encoding a valid stringy value failed: %v", err)
				}
				if err := s.DecodeJSON(jwire, pos.target(jnType).Interface()); err == nil {
					t.Errorf("JSON decode of stringy wire INTO json.Number target ACCEPTED for %s (must reject)", sc.label)
				}
			})
		}
	}

	// ---- map[json.Number]V KEY: the documented content-validated exception. ----
	// Numeric-content keys round-trip; non-numeric keys reject — on both wires.
	t.Run("map-key-numeric-roundtrips", func(t *testing.T) {
		s := avro.MustParse(`{"type":"map","values":"int"}`)
		in := map[json.Number]int32{"7": 1, "42": 2}
		wire, err := s.AppendEncode(nil, in)
		if err != nil {
			t.Fatalf("encode map[json.Number]int32 with numeric keys: %v", err)
		}
		var out map[json.Number]int32
		if _, err := s.Decode(wire, &out); err != nil {
			t.Fatalf("decode into map[json.Number]int32: %v", err)
		}
		if out["7"] != 1 || out["42"] != 2 {
			t.Fatalf("map-key round-trip: %v", out)
		}
		// JSON parity.
		jwire, _ := s.AppendEncodeJSON(nil, in)
		var jout map[json.Number]int32
		if err := s.DecodeJSON(jwire, &jout); err != nil {
			t.Fatalf("JSON decode into map[json.Number]int32: %v", err)
		}
	})

	// map[json.Number]V with NON-NUMERIC key content must REJECT — the
	// per-key validation, distinct from the round-trip above (which uses
	// valid keys and passes regardless of the guard). A json.Number whose
	// underlying string is not a valid number violates the type's own RFC
	// 8259 contract, so it cannot be a map key.
	t.Run("map-key-nonnumeric-rejects", func(t *testing.T) {
		s := avro.MustParse(`{"type":"map","values":"int"}`)

		// Encode source: a non-numeric json.Number key rejects on both wires.
		bad := map[json.Number]int32{"notanumber": 1}
		if _, err := s.AppendEncode(nil, bad); err == nil {
			t.Error("binary encode of map[json.Number]int32 with non-numeric key ACCEPTED (must reject)")
		}
		if _, err := s.AppendEncodeJSON(nil, bad); err == nil {
			t.Error("JSON encode of map[json.Number]int32 with non-numeric key ACCEPTED (must reject)")
		}

		// Decode target: a valid map wire whose KEY is a non-numeric string,
		// decoded INTO map[json.Number]V, must reject (the wire key has no
		// json.Number representation). This is the path the fast-path gate
		// (deser.go: mapTyp.Key() != jsonNumberType) routes to the validating
		// slow loop — neutering that gate is caught here.
		wire, err := s.AppendEncode(nil, map[string]int32{"notanumber": 5})
		if err != nil {
			t.Fatalf("setup: encode valid string-key map: %v", err)
		}
		var out map[json.Number]int32
		if _, err := s.Decode(wire, &out); err == nil {
			t.Error("binary decode of non-numeric-key wire INTO map[json.Number]int32 ACCEPTED (must reject)")
		}
		jwire, _ := s.AppendEncodeJSON(nil, map[string]int32{"notanumber": 5})
		var jout map[json.Number]int32
		if err := s.DecodeJSON(jwire, &jout); err == nil {
			t.Error("JSON decode of non-numeric-key wire INTO map[json.Number]int32 ACCEPTED (must reject)")
		}
	})
}

// jnStringSample returns a value the given stringy schema accepts, used to
// build a valid wire that is then (mis-)decoded into a json.Number target.
func jnStringSample(label string) any {
	switch label {
	case "bytes", "fixed":
		return []byte{0x41, 0x42}
	case "enum":
		return "A"
	default: // string
		return "ab"
	}
}
