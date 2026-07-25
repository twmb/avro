package avro_test

// Avro spells a union's null branch two ways — the bare `"null"` primitive
// and the wrapped object `{"type":"null"}` — and the two are the same type:
// identical wire bytes, identical decoded values. Every decision made by
// matching a branch's WRITTEN spelling must therefore reach the same verdict
// for both, or semantically identical schemas take different code paths in
// the places that spelling feeds: which serializer/deserializer arm is
// selected, the field metadata that drives the omitzero and missing-key
// fills, where a field-level logicalType lifts, whether a field-level
// precision/scale pair is consumed, and the encode-error identity each wire
// surfaces.
//
// Wire bytes are identical across spellings BY CONSTRUCTION, so these tests
// assert the DERIVED artifacts — that is where a spelling-sensitive
// predicate shows up.

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	avro "github.com/twmb/avro"
)

// wrapNullBranches rewrites every bare `"null"` union branch in a schema into
// the wrapped `{"type":"null"}` object, structurally (decode → rewrite →
// re-encode) rather than textually, so a `"null"` appearing anywhere that is
// not a union branch is left alone. extra, when non-nil, is merged into each
// wrapped null object so the props / logicalType carriers can be driven
// through the same helper.
func wrapNullBranches(t *testing.T, schema string, extra map[string]any) string {
	t.Helper()
	var tree any
	if err := json.Unmarshal([]byte(schema), &tree); err != nil {
		t.Fatalf("wrapNullBranches: unmarshal %s: %v", schema, err)
	}
	var walk func(v any) any
	walk = func(v any) any {
		switch tv := v.(type) {
		case []any: // a union: rewrite bare null branches
			out := make([]any, len(tv))
			for i, br := range tv {
				if s, ok := br.(string); ok && s == "null" {
					m := map[string]any{"type": "null"}
					for k, val := range extra {
						m[k] = val
					}
					out[i] = m
					continue
				}
				out[i] = walk(br)
			}
			return out
		case map[string]any:
			out := make(map[string]any, len(tv))
			for k, val := range tv {
				out[k] = walk(val)
			}
			return out
		}
		return v
	}
	b, err := json.Marshal(walk(tree))
	if err != nil {
		t.Fatalf("wrapNullBranches: marshal: %v", err)
	}
	return string(b)
}

// nullSpellings are the spellings of a union's null branch that must all be
// treated as a null branch. Props and a logicalType are inert metadata on a
// null (there is no null logical type in the spec, and neither key changes
// the branch's type or its wire form), so a carrier-bearing wrapped null is
// still a null branch.
func nullSpellings() []struct {
	label string
	extra map[string]any
} {
	return []struct {
		label string
		extra map[string]any
	}{
		{"bare", nil}, // extra==nil AND handled by the caller as "leave as written"
		{"wrapped", map[string]any{}},
		{"wrapped+props", map[string]any{"mine": "keepme"}},
		{"wrapped+logicalType", map[string]any{"logicalType": "nope"}},
		{"wrapped+nonstring-logicalType", map[string]any{"logicalType": float64(123)}},
	}
}

// spell returns schema with its null branches in the named spelling.
func spell(t *testing.T, schema, label string, extra map[string]any) string {
	t.Helper()
	if label == "bare" {
		return schema
	}
	return wrapNullBranches(t, schema, extra)
}

type omitRec struct {
	A string `avro:"a,omitzero"`
}

// TestRegression_OmitzeroNullBranchSpellingAgnostic pins the wire bytes and
// the decoded nullness, not merely that the encode succeeded: under the
// wrapped spelling the field previously encoded the VALUE branch (an empty
// string), which is indistinguishable on the wire from an explicit "".
//
// doc.go, "Struct tags": "a zero value encodes the field's default, or null
// for a nullable field that has no default"; and the documented single
// difference from map fill is precisely this [T, "null"] shape, where
// "omitzero encodes null where map fill instead errors on the missing key".
func TestRegression_OmitzeroNullBranchSpellingAgnostic(t *testing.T) {
	for _, branches := range []string{`["string","null"]`, `["null","string"]`} {
		base := fmt.Sprintf(`{"type":"record","name":"R","fields":[{"name":"a","type":%s}]}`, branches)
		var wantWire []byte
		for _, sp := range nullSpellings() {
			schema := spell(t, base, sp.label, sp.extra)
			s, err := avro.Parse(schema)
			if err != nil {
				t.Fatalf("%s/%s: parse %s: %v", branches, sp.label, schema, err)
			}
			wire, err := s.Encode(omitRec{})
			if err != nil {
				t.Fatalf("%s/%s: omitzero encode: %v (schema %s)", branches, sp.label, err, schema)
			}
			var back map[string]any
			if _, err := s.Decode(wire, &back); err != nil {
				t.Fatalf("%s/%s: decode: %v", branches, sp.label, err)
			}
			if back["a"] != nil {
				t.Errorf("%s/%s: omitzero on a zero-valued nullable field encoded %#v (wire %v); a zero value with no default must encode NULL",
					branches, sp.label, back["a"], wire)
			}
			// The null branch must stay distinguishable from an explicit
			// empty string, or nullness is unrecoverable by the reader.
			explicit, err := s.Encode(map[string]any{"a": ""})
			if err != nil {
				t.Fatalf("%s/%s: explicit empty-string encode: %v", branches, sp.label, err)
			}
			if string(wire) == string(explicit) {
				t.Errorf("%s/%s: omitzero wire %v equals the explicit empty-string wire; the null branch is unreachable via omitzero",
					branches, sp.label, wire)
			}
			if sp.label == "bare" {
				wantWire = wire
				continue
			}
			if string(wire) != string(wantWire) {
				t.Errorf("%s/%s: omitzero wire %v != bare spelling's %v; the two schemas are the same Avro type",
					branches, sp.label, wire, wantWire)
			}
		}
	}
}

// TestRegression_FieldLogicalLiftNullBranchSpellingAgnostic: the field-level
// logicalType lifts onto the first NON-null branch. A wrapped null branch is
// still a null branch, so it must be skipped exactly like the bare one —
// otherwise the annotation lands on null and the intended branch never gets
// it, so a time.Time no longer encodes at all.
func TestRegression_FieldLogicalLiftNullBranchSpellingAgnostic(t *testing.T) {
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, branches := range []string{`["null","long"]`, `["long","null"]`} {
		base := fmt.Sprintf(
			`{"type":"record","name":"R","fields":[{"name":"ts","type":%s,"logicalType":"timestamp-millis"}]}`, branches)
		var wantWire []byte
		for _, sp := range nullSpellings() {
			schema := spell(t, base, sp.label, sp.extra)
			s, err := avro.Parse(schema)
			if err != nil {
				t.Fatalf("%s/%s: parse: %v", branches, sp.label, err)
			}
			wire, err := s.Encode(map[string]any{"ts": ts})
			if err != nil {
				t.Fatalf("%s/%s: encoding time.Time into the lifted field failed: %v (schema %s)",
					branches, sp.label, err, schema)
			}
			var back map[string]any
			if _, err := s.Decode(wire, &back); err != nil {
				t.Fatalf("%s/%s: decode: %v", branches, sp.label, err)
			}
			got, ok := back["ts"].(time.Time)
			if !ok {
				t.Fatalf("%s/%s: decoded %T (%v), want time.Time — the lift did not reach the non-null branch",
					branches, sp.label, back["ts"], back["ts"])
			}
			if !got.Equal(ts) {
				t.Errorf("%s/%s: round-tripped %v, want %v", branches, sp.label, got, ts)
			}
			if sp.label == "bare" {
				wantWire = wire
				continue
			}
			if string(wire) != string(wantWire) {
				t.Errorf("%s/%s: wire %v != bare spelling's %v", branches, sp.label, wire, wantWire)
			}
		}
	}
}

// TestRegression_FieldDecimalLiftNullBranchSpellingAgnostic is the
// precision/scale twin: whether the field-level pair is CONSUMED by the
// decimal lift is decided by the same first-non-null-branch scan.
func TestRegression_FieldDecimalLiftNullBranchSpellingAgnostic(t *testing.T) {
	rat := big.NewRat(12345, 100)
	for _, branches := range []string{`["null","bytes"]`, `["bytes","null"]`} {
		base := fmt.Sprintf(
			`{"type":"record","name":"R","fields":[{"name":"d","type":%s,"logicalType":"decimal","precision":10,"scale":2}]}`,
			branches)
		var wantWire []byte
		for _, sp := range nullSpellings() {
			schema := spell(t, base, sp.label, sp.extra)
			s, err := avro.Parse(schema)
			if err != nil {
				t.Fatalf("%s/%s: parse: %v", branches, sp.label, err)
			}
			wire, err := s.Encode(map[string]any{"d": rat})
			if err != nil {
				t.Fatalf("%s/%s: encoding *big.Rat into the lifted decimal field failed: %v (schema %s)",
					branches, sp.label, err, schema)
			}
			if sp.label == "bare" {
				wantWire = wire
				continue
			}
			if string(wire) != string(wantWire) {
				t.Errorf("%s/%s: wire %v != bare spelling's %v", branches, sp.label, wire, wantWire)
			}
		}
	}
}

type noBranchValue struct{ X int }

func encodeErrIdentity(err error) string {
	if err == nil {
		return "nil"
	}
	var se *avro.SemanticError
	if errors.As(err, &se) {
		return fmt.Sprintf("SemanticError{AvroType:%q}", se.AvroType)
	}
	return "plain"
}

// TestRegression_UnionNoMatchIdentityNullBranchSpellingAgnostic: the union
// no-match error identity is arity-split — a 2-branch null union surfaces the
// value branch's own error, every other shape wraps in the union's
// *SemanticError. The binary and JSON encoders must reach the same verdict,
// and the split must not depend on how the null branch is spelled.
func TestRegression_UnionNoMatchIdentityNullBranchSpellingAgnostic(t *testing.T) {
	bases := []string{
		`["null","string"]`,
		`["string","null"]`,
		`["null","string","int"]`, // 3-branch: wraps in the union's error
	}
	for _, base := range bases {
		var wantID string
		for _, sp := range nullSpellings() {
			schema := spell(t, base, sp.label, sp.extra)
			s, err := avro.Parse(schema)
			if err != nil {
				t.Fatalf("%s/%s: parse: %v", base, sp.label, err)
			}
			v := noBranchValue{X: 1} // matches no branch on either wire
			_, binErr := s.Encode(v)
			_, jsonErr := s.EncodeJSON(v)
			binID, jsonID := encodeErrIdentity(binErr), encodeErrIdentity(jsonErr)
			if binID == "nil" || jsonID == "nil" {
				t.Fatalf("%s/%s: expected a no-match error on both wires, got binary=%v json=%v",
					base, sp.label, binErr, jsonErr)
			}
			if binID != jsonID {
				t.Errorf("%s/%s: encode-error identity differs by WIRE:\n  binary: %s (%v)\n  json:   %s (%v)",
					base, sp.label, binID, binErr, jsonID, jsonErr)
			}
			if sp.label == "bare" {
				wantID = binID
				continue
			}
			if binID != wantID {
				t.Errorf("%s/%s: encode-error identity %s differs from the bare spelling's %s",
					base, sp.label, binID, wantID)
			}
		}
	}
}

// TestRegression_MissingKeyFillNullBranchSpellingAgnostic: the implicit null
// default for the canonical ["null", T] nullable pattern, and the loud
// missing-key error for [T, "null"], must both be spelling-agnostic.
func TestRegression_MissingKeyFillNullBranchSpellingAgnostic(t *testing.T) {
	for _, branches := range []string{`["null","string"]`, `["string","null"]`} {
		base := fmt.Sprintf(`{"type":"record","name":"R","fields":[{"name":"a","type":%s}]}`, branches)
		var wantErr bool
		var wantWire []byte
		for _, sp := range nullSpellings() {
			s, err := avro.Parse(spell(t, base, sp.label, sp.extra))
			if err != nil {
				t.Fatalf("%s/%s: parse: %v", branches, sp.label, err)
			}
			wire, encErr := s.Encode(map[string]any{})
			if sp.label == "bare" {
				wantErr, wantWire = encErr != nil, wire
				continue
			}
			if (encErr != nil) != wantErr {
				t.Errorf("%s/%s: missing-key encode err=%v but the bare spelling's err-ness was %v",
					branches, sp.label, encErr, wantErr)
				continue
			}
			if encErr == nil && string(wire) != string(wantWire) {
				t.Errorf("%s/%s: missing-key fill wire %v != bare spelling's %v",
					branches, sp.label, wire, wantWire)
			}
		}
	}
}

// respellNulls is wrapNullBranches plus a "did anything change" verdict,
// derived by re-normalizing the input through the same marshal round trip so
// key ordering cannot masquerade as a change.
func respellNulls(t *testing.T, schema string, extra map[string]any) (string, bool) {
	t.Helper()
	normalized := wrapNullBranches(t, schema, nil /* rewrite nothing */)
	respelled := wrapNullBranches(t, schema, extra)
	return respelled, respelled != normalized
}

// TestMatrix_NullBranchSpellingParity is the class net for the
// bare-vs-wrapped null spelling axis: it crosses that axis into the existing
// combinatorial tables (matFrags x matCtxs) rather than forking them, and
// asserts on every union-bearing cell that the spellings agree on every
// DERIVED artifact. Wire bytes are identical across spellings by
// construction, which is exactly why the derived artifacts are what a
// spelling-sensitive predicate corrupts: the selected ser/deser arm, the
// field metadata, the JSON form, the canonical form and fingerprint, and the
// encode-error identity.
//
// Non-vacuity: reverting isNullBranch to `s.primitive == "null"` reddens this
// test (see the per-finding pins for the exact user-visible symptoms).
func TestMatrix_NullBranchSpellingParity(t *testing.T) {
	u := &uniq{}
	var cells, checks int
	for _, fr := range matFrags() {
		for _, cx := range matCtxs() {
			if cx.skip != nil && cx.skip(fr.kind) {
				continue
			}
			bare := cx.schema(fr.schema(u), fr.kind, u)
			baseSchema, err := avro.Parse(bare)
			if err != nil {
				continue // contexts calibrate their own acceptance elsewhere
			}
			// Reference artifacts from the bare spelling.
			baseCanon := string(baseSchema.Canonical())
			baseFP := baseSchema.Fingerprint(avro.NewRabin())

			for _, sp := range nullSpellings() {
				if sp.label == "bare" {
					continue
				}
				respelled, changed := respellNulls(t, bare, sp.extra)
				if !changed {
					continue // no null branch in this cell
				}
				alt, err := avro.Parse(respelled)
				if err != nil {
					t.Errorf("%s/%s/%s: bare spelling parses but the wrapped one does not: %v\n  bare:    %s\n  wrapped: %s",
						fr.label, cx.label, sp.label, err, bare, respelled)
					continue
				}
				cells++

				// Canonical form and fingerprint collapse the wrapped form,
				// so both must be spelling-independent.
				if got := string(alt.Canonical()); got != baseCanon {
					t.Errorf("%s/%s/%s: canonical %s != bare's %s", fr.label, cx.label, sp.label, got, baseCanon)
				}
				if got := alt.Fingerprint(avro.NewRabin()); string(got) != string(baseFP) {
					t.Errorf("%s/%s/%s: fingerprint differs from the bare spelling", fr.label, cx.label, sp.label)
				}

				for _, val := range fr.values {
					wv := cx.wrap(val)
					baseWire, baseErr := baseSchema.Encode(wv)
					altWire, altErr := alt.Encode(wv)
					if (baseErr == nil) != (altErr == nil) {
						t.Errorf("%s/%s/%s: binary encode verdict differs: bare=%v wrapped=%v",
							fr.label, cx.label, sp.label, baseErr, altErr)
						continue
					}
					if baseErr != nil {
						if a, b := encodeErrIdentity(baseErr), encodeErrIdentity(altErr); a != b {
							t.Errorf("%s/%s/%s: binary encode-error identity %s != bare's %s",
								fr.label, cx.label, sp.label, b, a)
						}
						continue
					}
					if string(altWire) != string(baseWire) {
						t.Errorf("%s/%s/%s value %#v: wire %v != bare's %v",
							fr.label, cx.label, sp.label, wv, altWire, baseWire)
					}
					// Decoded value parity through the wrapped schema.
					var baseGot, altGot any
					if _, err := baseSchema.Decode(baseWire, &baseGot); err != nil {
						t.Fatalf("%s/%s: bare decode: %v", fr.label, cx.label, err)
					}
					if _, err := alt.Decode(altWire, &altGot); err != nil {
						t.Errorf("%s/%s/%s: wrapped decode: %v", fr.label, cx.label, sp.label, err)
						continue
					}
					if !matEqual(baseGot, altGot) {
						t.Errorf("%s/%s/%s value %#v: decoded %#v != bare's %#v",
							fr.label, cx.label, sp.label, wv, altGot, baseGot)
					}
					// JSON form parity.
					baseJSON, baseJErr := baseSchema.EncodeJSON(wv)
					altJSON, altJErr := alt.EncodeJSON(wv)
					if (baseJErr == nil) != (altJErr == nil) {
						t.Errorf("%s/%s/%s: JSON encode verdict differs: bare=%v wrapped=%v",
							fr.label, cx.label, sp.label, baseJErr, altJErr)
					} else if baseJErr == nil && string(altJSON) != string(baseJSON) {
						t.Errorf("%s/%s/%s value %#v: JSON %s != bare's %s",
							fr.label, cx.label, sp.label, wv, altJSON, baseJSON)
					}
					checks++
				}

				// A value no branch accepts: the encode-error identity must
				// agree across wires AND across spellings.
				bBin := encodeErrIdentity(mustErr(baseSchema.Encode(cx.wrap(noBranchValue{X: 1}))))
				aBin := encodeErrIdentity(mustErr(alt.Encode(cx.wrap(noBranchValue{X: 1}))))
				bJSON := encodeErrIdentity(mustErr(baseSchema.EncodeJSON(cx.wrap(noBranchValue{X: 1}))))
				aJSON := encodeErrIdentity(mustErr(alt.EncodeJSON(cx.wrap(noBranchValue{X: 1}))))
				if aBin != bBin {
					t.Errorf("%s/%s/%s: binary no-match identity %s != bare's %s", fr.label, cx.label, sp.label, aBin, bBin)
				}
				if aJSON != bJSON {
					t.Errorf("%s/%s/%s: JSON no-match identity %s != bare's %s", fr.label, cx.label, sp.label, aJSON, bJSON)
				}
				if aBin != aJSON {
					t.Errorf("%s/%s/%s: no-match identity differs by WIRE: binary=%s json=%s", fr.label, cx.label, sp.label, aBin, aJSON)
				}
			}
		}
	}
	if cells == 0 || checks == 0 {
		t.Fatalf("vacuous net: %d respelled cells, %d value checks — the spelling axis must actually fire", cells, checks)
	}
	t.Logf("null-branch spelling parity: %d respelled cells, %d value checks", cells, checks)
}

// mustErr discards an encode's byte result, keeping only the error, so the
// identity comparisons above read cleanly.
func mustErr(_ []byte, err error) error { return err }
