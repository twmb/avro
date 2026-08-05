package avro_test

import (
	"bytes"
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
	// decimal/big-decimal are the EXEMPT bytes-backed case: json.Number
	// carries the RatString (RFC-8259-valid by construction), so they are
	// numeric-accept, not stringy-reject. scale 0 / precision 18 lets the
	// integer content battery round-trip exactly.
	{"decimal", `{"type":"bytes","logicalType":"decimal","precision":18,"scale":0}`},
	{"decimal-fixed", `{"type":"fixed","name":"JNDF","size":9,"logicalType":"decimal","precision":18,"scale":0}`},
	{"big-decimal", `{"type":"bytes","logicalType":"big-decimal"}`},
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
	// struct-field exercises the ADDRESSABLE unsafe struct-field fast path
	// (unsafe.go gates on stringFastPathEligible{Encode,Decode}) — a code
	// path the map/slice/typed-map targets above never reach.
	{"struct-field",
		func(leaf string) string {
			return fmt.Sprintf(`{"type":"record","name":"JNSR","fields":[{"name":"f","type":%s}]}`, leaf)
		},
		func(leaf any) any {
			st := reflect.StructOf([]reflect.StructField{
				{Name: "F", Type: reflect.TypeOf(leaf), Tag: `avro:"f"`},
			})
			p := reflect.New(st) // POINTER → addressable → unsafe encode path
			p.Elem().Field(0).Set(reflect.ValueOf(leaf))
			return p.Interface()
		},
		func(t reflect.Type) reflect.Value {
			st := reflect.StructOf([]reflect.StructField{
				{Name: "F", Type: t, Tag: `avro:"f"`},
			})
			return reflect.New(st) // addressable struct → unsafe path
		}},
	// nullable-union exercises union branch dispatch: a numeric branch must
	// accept json.Number; a stringy-only union must reject it.
	{"nullable-union",
		func(leaf string) string { return fmt.Sprintf(`["null",%s]`, leaf) },
		func(leaf any) any { return leaf },
		func(t reflect.Type) reflect.Value { return reflect.New(reflect.PointerTo(t)) }},
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

				// Resolved-decode path (identity resolution): a json.Number
				// target must decode the same through resolve.go's resolved
				// deser, not just the natural deser.
				res, rerr := avro.Resolve(avro.MustParse(pos.schema(sc.schema)), avro.MustParse(pos.schema(sc.schema)))
				if rerr != nil {
					t.Fatalf("identity Resolve: %v", rerr)
				}
				if _, err := res.Decode(wire, pos.target(jnType).Interface()); err != nil {
					t.Fatalf("resolved decode into json.Number target rejected for numeric schema: %v", err)
				}

				// Non-numeric / malformed json.Number content must REJECT on
				// encode (the type's RFC-8259 invariant: its underlying
				// string must be a valid number). This exercises the
				// content-validating arms — e.g. the decimal encode arm's
				// boundedRatFromString — which an integer-only battery never
				// reaches (a numerically-valid value coerces identically with
				// or without the validation).
				for _, bad := range []string{"notanumber", "", "1.2.3"} {
					if _, err := s.AppendEncode(nil, pos.encodeVal(json.Number(bad))); err == nil {
						t.Errorf("binary encode of malformed json.Number(%q) ACCEPTED for numeric schema (must reject)", bad)
					}
					if _, err := s.AppendEncodeJSON(nil, pos.encodeVal(json.Number(bad))); err == nil {
						t.Errorf("JSON encode of malformed json.Number(%q) ACCEPTED for numeric schema (must reject)", bad)
					}
				}

				// Content variety with a WIRE-STABLE round-trip: encode
				// json.Number(content) -> decode into a json.Number target ->
				// re-encode -> must reproduce the ORIGINAL wire. This is
				// calibration-free (no hardcoded expected string — date/
				// timestamp/decimal each transform the content differently)
				// and catches CONTENT corruption a success-only check misses:
				// neutering the json.Number numeric-setter / decimal arms lets
				// decode still succeed but produce a wrong value, which then
				// re-encodes to different bytes.
				for _, content := range []string{"0", "-1", "127", "2147483647"} {
					cin := pos.encodeVal(json.Number(content))
					cw, cerr := s.AppendEncode(nil, cin)
					if cerr != nil {
						t.Errorf("encode json.Number(%q) rejected for numeric schema: %v", content, cerr)
						continue
					}
					ctgt := pos.target(jnType)
					if _, err := s.Decode(cw, ctgt.Interface()); err != nil {
						t.Errorf("decode json.Number(%q) wire into json.Number target failed: %v", content, err)
						continue
					}
					// Re-encode the decoded json.Number tree; must match cw.
					reW, reErr := s.AppendEncode(nil, ctgt.Elem().Interface())
					if reErr != nil {
						t.Errorf("re-encode of decoded json.Number(%q) failed: %v", content, reErr)
						continue
					}
					if !bytes.Equal(reW, cw) {
						t.Errorf("json.Number(%q) NOT wire-stable through json.Number target:\n in=%x\n out=%x", content, cw, reW)
					}
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

// ---------------------------------------------------------------------------
// Class-elimination differential net: a logical-on-numeric type must treat a
// json.Number encode SOURCE identically to its underlying numeric type.
//
// json.Number is a numeric carrier (NOT_BUGS #35): its content must be a valid
// RFC 8259 number, so a logical layered on a numeric base — date on int;
// time-*/timestamp-*/local-timestamp-* on int/long — must never be MORE LENIENT
// about non-numeric json.Number content than the plain int/long it wraps. The
// ORACLE is calibration-free: the underlying numeric schema's own accept/reject
// verdict for the same json.Number. No hardcoded "what is a number" list, so it
// cannot rot as the numeric parser's grammar evolves.
//
// The discriminating input is content that is a valid TEMPORAL STRING but an
// INVALID number ("2024-01-01", "2024-01-01T00:00:00Z"): the date/timestamp
// encode string-convenience arms (tryParseDateString / tryParseTimeString) once
// fired for json.Number (whose Kind() is reflect.String), encoding it as a
// date/timestamp where the numeric twin rejects it. A generic non-numeric
// battery ("xyz", "1.2.3") never reaches that arm — those fail time.Parse too,
// so they reject with or without the leniency; only a temporal-shaped string
// separates the buggy path from the correct one. This net is the differential
// complement to TestMatrix_JSONNumberPolicy (which asserts the numeric base's
// ABSOLUTE reject of non-numeric content); together they pin both "the base
// rejects" and "the logical matches the base," across every encode context.
func TestMatrix_JSONNumberLogicalMatchesNumericTwin(t *testing.T) {
	logicals := []struct {
		label, schema, twin string
	}{
		{"date", `{"type":"int","logicalType":"date"}`, `"int"`},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`, `"int"`},
		{"time-micros", `{"type":"long","logicalType":"time-micros"}`, `"long"`},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, `"long"`},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`, `"long"`},
		{"timestamp-nanos", `{"type":"long","logicalType":"timestamp-nanos"}`, `"long"`},
		{"local-timestamp-millis", `{"type":"long","logicalType":"local-timestamp-millis"}`, `"long"`},
		{"local-timestamp-micros", `{"type":"long","logicalType":"local-timestamp-micros"}`, `"long"`},
		{"local-timestamp-nanos", `{"type":"long","logicalType":"local-timestamp-nanos"}`, `"long"`},
	}
	// valid-number (both accept), two temporal-shaped strings that are invalid
	// numbers (the discriminators — both must reject after the fix), and garbage
	// (both reject via the numeric parser regardless).
	contents := []string{"19723", "2024-01-01", "2024-01-01T00:00:00Z", "xyz"}

	// verdicts returns whether (binary, JSON) encode of val against schemaJSON
	// succeeds.
	verdicts := func(schemaJSON string, val any) (binOK, jsonOK bool) {
		s := avro.MustParse(schemaJSON)
		_, be := s.AppendEncode(nil, val)
		_, je := s.AppendEncodeJSON(nil, val)
		return be == nil, je == nil
	}

	// Reuse jnPositions for the ENCODE-CONTEXT axis (top / record field / array
	// element / map value / addressable struct field / nullable-union branch) —
	// a json.Number at a struct field or container element can reach a different
	// encode path than a top-level value.
	for _, pos := range jnPositions {
		for _, lg := range logicals {
			for _, content := range contents {
				t.Run(pos.label+"/"+lg.label+"/"+content, func(t *testing.T) {
					val := pos.encodeVal(json.Number(content))
					logBin, logJSON := verdicts(pos.schema(lg.schema), val)
					twBin, twJSON := verdicts(pos.schema(lg.twin), val)
					if logBin != twBin {
						t.Errorf("binary encode verdict divergence: %s(json.Number(%q))=%v but numeric twin %s=%v — a logical must match its numeric base for a json.Number source",
							lg.label, content, logBin, lg.twin, twBin)
					}
					if logJSON != twJSON {
						t.Errorf("JSON encode verdict divergence: %s(json.Number(%q))=%v but numeric twin %s=%v",
							lg.label, content, logJSON, lg.twin, twJSON)
					}
				})
			}
		}
	}
}
