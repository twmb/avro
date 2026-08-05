package avro_test

import (
	"encoding/hex"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/twmb/avro"
)

// nullSpellDiffMarker is the Go type the differential's CustomType matches on.
type nullSpellDiffMarker struct{ A int64 }

type nullSpellDiffRec struct {
	F nullSpellDiffMarker
}

// The four spellings of one null branch. wrapped_plain renders back to bare
// (the renderer emits a carrier-free wrapped null bare), so it is a control
// that the axis itself is wired up; the carrier-bearing spellings are the
// ones that reach the composition walkers as objects.
var nullSpellDiffUnions = []struct{ name, union string }{
	{"bare", `["null","string"]`},
	{"wrapped_plain", `[{"type":"null"},"string"]`},
	{"wrapped_props", `[{"type":"null","x":1},"string"]`},
	{"wrapped_logicaltype", `[{"type":"null","logicalType":"nope"},"string"]`},
}

// TestDifferentialFastavroSchemaForNullSpelling drives a foreign
// implementation over the schemas SchemaFor EMITS for each null spelling.
// The emitted text is the artifact a caller publishes — to a registry, or
// straight to another implementation — so a foreign reader is the oracle
// that matters, and it is the only one that can see the consequence of a
// dropped "default":null: twmb synthesizes an implicit null default for a
// nullable union at parse, while Java and fastavro require it written.
//
// Two arms per spelling:
//
//   - canonical: fastavro must accept the emitted schema, and its own
//     parsing canonical form must match twmb's Canonical() — which subsumes
//     fingerprint equality with no byte-order presentation trap.
//   - evolution (readresolve): data written by a writer that predates the
//     field must still read through the emitted schema as the reader. This
//     is the sharpest oracle for the default fill; with the default dropped
//     fastavro raises SchemaResolutionError ("No default value for field F")
//     while the bare spelling reads back a null.
//
// Skips without AVRO_FASTAVRO_PYTHON, like every differential.
func TestDifferentialFastavroSchemaForNullSpelling(t *testing.T) {
	o := startOracle(t)

	// The writer predates field F: an empty record, so the datum is zero
	// bytes and the reader must supply F from its own default. The record
	// name must be the one SchemaFor derives from the Go type, since
	// resolution matches records by name before it looks at fields.
	writerSchema := `{"type":"record","name":"nullSpellDiffRec","fields":[]}`
	writer := avro.MustParse(writerSchema)
	wire, err := writer.Encode(map[string]any{})
	if err != nil {
		t.Fatalf("encode pre-field datum: %v", err)
	}

	for _, tc := range nullSpellDiffUnions {
		t.Run(tc.name, func(t *testing.T) {
			cs, err := avro.Parse(tc.union)
			if err != nil {
				t.Fatalf("parse custom union: %v", err)
			}
			root := cs.Root()
			ct := avro.CustomType{GoType: reflect.TypeFor[nullSpellDiffMarker](), Schema: root}

			s, err := avro.SchemaFor[nullSpellDiffRec](ct)
			if err != nil {
				t.Fatalf("SchemaFor: %v", err)
			}
			emitted := s.String()

			// Arm 1: fastavro accepts the emitted schema and agrees on the
			// canonical form.
			resp := o.call(oracleJob{Op: "canonical", Schema: json.RawMessage(emitted)})
			if !resp.OK {
				t.Fatalf("fastavro rejected the emitted schema: %s\n%s", resp.Err, emitted)
			}
			if want := string(s.Canonical()); resp.Canonical != want {
				t.Fatalf("canonical differs from fastavro:\n twmb     %s\n fastavro %s\n emitted  %s", want, resp.Canonical, emitted)
			}

			// Arm 2: a foreign reader resolves pre-field data through the
			// emitted schema. A dropped "default":null fails here.
			resp = o.call(oracleJob{
				Op:     "readresolve",
				Schema: json.RawMessage(writerSchema),
				Reader: json.RawMessage(emitted),
				Hex:    hex.EncodeToString(wire),
			})
			if !resp.OK {
				t.Fatalf("fastavro cannot read pre-field data through the emitted schema: %s\n emitted %s",
					resp.Err, emitted)
			}
			if len(resp.Values) != 1 {
				t.Fatalf("want 1 resolved value, got %d", len(resp.Values))
			}
			got, ok := resp.Values[0].(map[string]any)
			if !ok {
				t.Fatalf("resolved value is %T, want an object: %#v", resp.Values[0], resp.Values[0])
			}
			if v, present := got["F"]; !present || v != nil {
				t.Fatalf("fastavro resolved F = %#v (present=%v), want a filled null", v, present)
			}

			// twmb's own resolved read must agree with the foreign one.
			resolved, err := avro.Resolve(writer, s)
			if err != nil {
				t.Fatalf("twmb Resolve: %v", err)
			}
			var out map[string]any
			if _, err := resolved.Decode(wire, &out); err != nil {
				t.Fatalf("twmb resolved decode: %v", err)
			}
			if v, present := out["F"]; !present || v != nil {
				t.Fatalf("twmb resolved F = %#v (present=%v), want a filled null", v, present)
			}
		})
	}
}
