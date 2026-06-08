package avro_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// JSON strictness-parity net — the standing guard for the "skip path is a
// second parser" class.
//
// DecodeJSON has TWO JSON parsers: the VALUE path (known reader fields, fully
// validating) and the SKIP path (unknown reader fields, json_scan.skipValue).
// Whether a byte sequence is "valid JSON" must NOT depend on which one
// processes it — but the skip path silently drifted lax (it accepted 1.2.3,
// "\q", [}], missing commas) because every strictness test drove only the
// value path. This is the same shape as the reflect/unsafe encode twins
// (TestMatrix_ReflectUnsafePathParity) and the scale axis: two paths that
// must agree, tested on only one.
//
// The invariant is calibration-free: for each fragment, the SKIP-path verdict
// must EQUAL the VALUE-path verdict. (The fragment's static type matches the
// known field's schema, so a verdict difference can only come from JSON-
// grammar strictness, not type-checking — the skip path is schema-less and
// correctly does not type-check.) Driven both as a leaf field and nested
// inside a skipped container, so the recursive skip validators are exercised.
// ---------------------------------------------------------------------------

func TestMatrix_JSONStrictnessParityKnownVsSkip(t *testing.T) {
	// Each fragment is paired with a reader field type that ACCEPTS its
	// well-formed form, so any known-vs-skip verdict difference is purely a
	// JSON-grammar-strictness difference.
	corpus := []struct {
		frag      string
		knownType string
	}{
		// numbers (malformed + valid) against "double"
		{`1.2.3`, `"double"`}, {`1e`, `"double"`}, {`5.`, `"double"`}, {`01`, `"double"`},
		{`-`, `"double"`}, {`.5`, `"double"`}, {`1.`, `"double"`}, {`1e+`, `"double"`},
		{`-3.14e10`, `"double"`}, {`0`, `"double"`}, {`0.5`, `"double"`}, {`123`, `"double"`},
		// strings (malformed + valid) against "string"
		{`"\q"`, `"string"`}, {`"\x41"`, `"string"`}, {`"\u00"`, `"string"`}, {`"abc`, `"string"`},
		{`"ok"`, `"string"`}, {`"A"`, `"string"`}, {`"with \"quote\""`, `"string"`}, {`""`, `"string"`},
		// arrays (malformed + valid) against array<long>
		{`[}]`, `{"type":"array","items":"long"}`},
		{`[1 2 3]`, `{"type":"array","items":"long"}`},
		{`[1,2,]`, `{"type":"array","items":"long"}`},
		{`[,1]`, `{"type":"array","items":"long"}`},
		{`[1,2,3]`, `{"type":"array","items":"long"}`},
		{`[]`, `{"type":"array","items":"long"}`},
		// objects/maps (malformed + valid) against map<long>
		{`{]}`, `{"type":"map","values":"long"}`},
		{`{"a" 1}`, `{"type":"map","values":"long"}`},
		{`{"a"::1}`, `{"type":"map","values":"long"}`},
		{`{"a":1,}`, `{"type":"map","values":"long"}`},
		{`{a:1}`, `{"type":"map","values":"long"}`},
		{`{"a":1}`, `{"type":"map","values":"long"}`},
		{`{}`, `{"type":"map","values":"long"}`},
		// booleans / null
		{`true`, `"boolean"`}, {`tru`, `"boolean"`}, {`null`, `["null","long"]`},
	}

	for _, c := range corpus {
		t.Run(c.frag, func(t *testing.T) {
			// Reader A KNOWS field "f" (value path); reader B does NOT, so "f"
			// is skipped (skip path). Same document fed to both.
			known := avro.MustParse(fmt.Sprintf(
				`{"type":"record","name":"R","fields":[{"name":"f","type":%s}]}`, c.knownType))
			skip := avro.MustParse(
				`{"type":"record","name":"R","fields":[{"name":"other","type":["null","long"],"default":null}]}`)

			doc := []byte(fmt.Sprintf(`{"f":%s}`, c.frag))
			var a, b any
			valueRejects := known.DecodeJSON(doc, &a) != nil
			skipRejects := skip.DecodeJSON(doc, &b) != nil

			if valueRejects != skipRejects {
				t.Fatalf("STRICTNESS DIVERGENCE for %s: value-path rejects=%v, skip-path rejects=%v (the two JSON parsers disagree)",
					c.frag, valueRejects, skipRejects)
			}

			// Cross-check the verdict against encoding/json for the leaf
			// fragment, so the parity isn't "both wrong the same way".
			if jv := json.Valid([]byte(c.frag)); jv == valueRejects {
				t.Errorf("%s: json.Valid=%v but DecodeJSON value-path rejects=%v — both twmb parsers may agree but disagree with stdlib",
					c.frag, jv, valueRejects)
			}
		})
	}
}

// TestMatrix_JSONStrictnessParityNested drives the same corpus NESTED inside a
// skipped container, exercising skipArrayStrict/skipObjectStrict recursion —
// the malformed fragment is buried one level down in an unknown field.
func TestMatrix_JSONStrictnessParityNested(t *testing.T) {
	skip := avro.MustParse(
		`{"type":"record","name":"R","fields":[{"name":"keep","type":"long"}]}`)

	frags := []struct {
		frag      string
		malformed bool
	}{
		{`1.2.3`, true}, {`"\q"`, true}, {`[}]`, true}, {`{"a" 1}`, true}, {`[1,2,]`, true},
		{`42`, false}, {`"ok"`, false}, {`[1,2,3]`, false}, {`{"a":1}`, false},
	}
	for _, c := range frags {
		t.Run(c.frag, func(t *testing.T) {
			// The malformed fragment is the value of an unknown field's
			// nested array and object, so the recursive skip validators
			// must reach it.
			for _, wrap := range []string{
				`{"keep":1,"x":[%s]}`,     // inside skipped array
				`{"keep":1,"x":{"y":%s}}`, // inside skipped object
				`{"keep":1,"x":[[%s]]}`,   // doubly nested
			} {
				doc := []byte(fmt.Sprintf(wrap, c.frag))
				var out any
				err := skip.DecodeJSON(doc, &out)
				if c.malformed && err == nil {
					t.Errorf("nested skipped malformed %s in %q ACCEPTED (skip recursion not validating)", c.frag, wrap)
				}
				if !c.malformed && err != nil {
					t.Errorf("nested skipped valid %s in %q REJECTED: %v", c.frag, wrap, err)
				}
			}
		})
	}
}
