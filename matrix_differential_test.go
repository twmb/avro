package avro_test

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// External-oracle matrix: every (fragment × context) cell is validated
// against a real fastavro process. The relational matrix core cannot catch
// a bug that is SYMMETRIC across twmb's encoder and decoder (both agreeing
// on the same wrong bytes); an independent implementation can. Two checks
// per cell, neither needing any cross-language value comparison:
//
//	rt        — twmb encodes; fastavro decodes those bytes and re-encodes;
//	            the bytes must come back identical. Catches wire-layout
//	            divergence (varints, lengths, union indices, logical byte
//	            forms, container framing) in either implementation.
//	canonical — fastavro's Parsing Canonical Form of the composed schema
//	            must equal twmb's Canonical(), extending the vendored
//	            vector oracle to every schema the matrix can compose.
//
// Skips (does not fail) when fastavro is unavailable; CI provides it via
// AVRO_FASTAVRO_PYTHON.
// ---------------------------------------------------------------------------

func TestDifferentialMatrix(t *testing.T) {
	o := startOracle(t)

	for _, fr := range matFrags() {
		for _, cx := range matCtxs() {
			if cx.skip != nil && cx.skip(fr.kind) {
				continue
			}
			t.Run(fr.label+"/"+cx.label, func(t *testing.T) {
				u := &uniq{}
				schemaJSON := cx.schema(fr.schema(u), fr.kind, u)
				s, err := avro.Parse(schemaJSON)
				if err != nil {
					t.Fatalf("Parse: %v", err)
				}
				vin := cx.wrap(fr.values[0])
				w1, err := s.AppendEncode(nil, vin)
				if err != nil {
					t.Fatalf("encode: %v", err)
				}

				// Round-trip through fastavro.
				resp := o.call(oracleJob{
					Op:     "rt",
					Schema: json.RawMessage(schemaJSON),
					Hex:    hex.EncodeToString(w1),
				})
				if !resp.OK {
					t.Fatalf("fastavro could not round-trip twmb's bytes: %s\nschema: %s\nwire: %x", resp.Err, schemaJSON, w1)
				}
				if resp.Hex != hex.EncodeToString(w1) {
					t.Fatalf("fastavro re-encode differs from twmb:\n twmb=%x\n fast=%s\nschema: %s", w1, resp.Hex, schemaJSON)
				}

				// Canonical form parity.
				cresp := o.call(oracleJob{
					Op:     "canonical",
					Schema: json.RawMessage(schemaJSON),
				})
				if !cresp.OK {
					t.Fatalf("fastavro canonical failed: %s\nschema: %s", cresp.Err, schemaJSON)
				}
				if cresp.Canonical != string(s.Canonical()) {
					t.Fatalf("canonical form diverges:\n twmb=%s\n fast=%s\nschema: %s", s.Canonical(), cresp.Canonical, schemaJSON)
				}
			})
		}
	}
}

// The recursion shapes through the same oracle: recursive wire layouts and
// the canonical forms of self/mutually/forward-referential schemas.
func TestDifferentialMatrixRecursion(t *testing.T) {
	o := startOracle(t)
	for _, sh := range recShapes() {
		if sh.label == "fwd-ref-union" {
			// Forward references are a twmb+Java extension beyond the
			// spec's "forward references are not permitted"; fastavro
			// rejects the schema outright (KeyError on the name), so the
			// oracle cannot validate this shape. Java-side parity for
			// fwd-refs is covered by the cisuite Java differential.
			continue
		}
		for _, d := range []int{0, 3} {
			t.Run(fmt.Sprintf("%s/depth%d", sh.label, d), func(t *testing.T) {
				s := avro.MustParse(sh.schema)
				w1, err := s.AppendEncode(nil, sh.value(d))
				if err != nil {
					t.Fatalf("encode: %v", err)
				}
				resp := o.call(oracleJob{
					Op:     "rt",
					Schema: json.RawMessage(sh.schema),
					Hex:    hex.EncodeToString(w1),
				})
				if !resp.OK {
					t.Fatalf("fastavro rt: %s\nschema: %s", resp.Err, sh.schema)
				}
				if resp.Hex != hex.EncodeToString(w1) {
					t.Fatalf("recursive wire diverges:\n twmb=%x\n fast=%s", w1, resp.Hex)
				}
				cresp := o.call(oracleJob{Op: "canonical", Schema: json.RawMessage(sh.schema)})
				if !cresp.OK {
					t.Fatalf("fastavro canonical: %s", cresp.Err)
				}
				if cresp.Canonical != string(s.Canonical()) {
					t.Fatalf("recursive canonical diverges:\n twmb=%s\n fast=%s", s.Canonical(), cresp.Canonical)
				}
			})
		}
	}
}
