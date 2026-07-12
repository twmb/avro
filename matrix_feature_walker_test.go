package avro_test

import (
	"bytes"
	"reflect"
	"slices"
	"testing"

	"github.com/twmb/avro"
)

// The SCHEMA-FEATURE × WALKER parity net.
//
// The library has one wire parser and many parallel schema consumers
// ("walkers"): String()'s re-parse, Canonical() and fingerprints, the Root()
// metadata tree and its Schema() rebuild, the SchemaCache self-containment
// walkers (definition collection and reference splicing), schema resolution
// (including the custom-free writer view built for resolved JSON decoding),
// resolved DecodeJSON, CheckCompatibility, and single-object encoding. Every
// schema FEATURE the wire parser understands — alternate spellings, lifts,
// normalizations, acceptances — must mean the same thing to every walker.
// Historically, each feature × walker intersection that no test crossed has
// drifted independently: a walker gates on its own re-derivation of the
// parser's rule and covers a subset of the feature's reach.
//
// Structure: a table of feature rows × a table of walker drivers, run as the
// full cross product. Each row carries a schema spelled WITH the feature and
// its vanilla ("twin") spelling of the same logical schema, plus fragments
// for the cache directions (a cross-parse reference INTO the feature's
// subtree, and a named definition INSIDE the feature's subtree) and a
// resolve-compatible variant. Each driver asserts one consumer treats
// feature and twin identically. The invariant in every cell is PARITY with
// the wire parser / the vanilla twin — never a hardcoded expectation, so
// rows stay cheap to add.
//
// Adding a feature to the net = adding a row. Feature families still to be
// seeded as rows: lax names (WithLaxNames spellings, via opts),
// field-level logicalType lift shapes, case-variant object keys,
// wrapped ({"type":"X"}) and forward references, aliases, degenerate
// cardinalities (empty fields/symbols/branches), duplicate-key last-wins
// spellings, and implicit null defaults.
type featureWalkerRow struct {
	name string

	// feature and twin spell the SAME logical schema with and without the
	// feature. Both parse standalone (with opts). sample is a value both
	// spellings Encode.
	feature, twin string
	opts          []avro.SchemaOpt
	sample        map[string]any

	// resolveAgainst, when non-empty, is a vanilla-spelled schema that is
	// resolve-compatible with feature/twin in BOTH directions (fields
	// added only with defaults) but canonically different, so Resolve's
	// identical-canonical fast path cannot short-circuit and resolution
	// actually recurses the feature's subtree. resolveSample is a value
	// resolveAgainst Encodes.
	resolveAgainst string
	resolveSample  map[string]any

	// Cache cross-parse REFERENCE direction: refDefs are registered first
	// (in a fresh cache per spelling); refFeature/refTwin then hold a
	// reference to a cached name INSIDE the feature's subtree position;
	// refSample encodes against them. Empty refFeature marks a feature
	// with no reference-bearing subtree (flat enum/fixed carry none).
	refDefs             []string
	refFeature, refTwin string
	refSample           map[string]any

	// Cache DEFINITION direction: defFeature DEFINES a named type inside
	// the feature's subtree position (defTwin is its vanilla twin);
	// defFollow, parsed next in the same cache, references that type and
	// must splice its definition; defFollowSample encodes against it.
	defFeature, defTwin string
	defFollow           string
	defFollowSample     map[string]any
}

const (
	fwElemDef      = `{"type":"record","name":"ns.Elem","fields":[{"name":"x","type":"int"}]}`
	fwDecoyElemDef = `{"type":"record","name":"decoy.Elem","fields":[{"name":"y","type":"long"}]}`
)

var featureWalkerRows = []featureWalkerRow{
	{
		name:    "flat-record",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"rec","type":"record","fields":[{"name":"x","type":"int"}]}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"rec","type":{"type":"record","name":"rec","fields":[{"name":"x","type":"int"}]}}]}`,
		sample:  map[string]any{"rec": map[string]any{"x": int32(7)}},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"rec","type":{"type":"record","name":"rec","fields":[{"name":"x","type":"int"},{"name":"pad","type":"int","default":5}]}}]}`,
		resolveSample:  map[string]any{"rec": map[string]any{"x": int32(7), "pad": int32(5)}},

		refDefs:    []string{fwElemDef},
		refFeature: `{"type":"record","name":"ns.Top","fields":[{"name":"rec","type":"record","fields":[{"name":"e","type":"Elem"}]}]}`,
		refTwin:    `{"type":"record","name":"ns.Top","fields":[{"name":"rec","type":{"type":"record","name":"rec","fields":[{"name":"e","type":"Elem"}]}}]}`,
		refSample:  map[string]any{"rec": map[string]any{"e": map[string]any{"x": int32(1)}}},

		defFeature:      `{"type":"record","name":"ns.H1","fields":[{"name":"drec","type":"record","fields":[{"name":"x","type":"int"}]}]}`,
		defTwin:         `{"type":"record","name":"ns.H1","fields":[{"name":"drec","type":{"type":"record","name":"drec","fields":[{"name":"x","type":"int"}]}}]}`,
		defFollow:       `{"type":"record","name":"ns.F1","fields":[{"name":"d","type":"drec"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
	{
		name:    "flat-error",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"e","type":"error","fields":[{"name":"x","type":"int"}]}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"e","type":{"type":"error","name":"e","fields":[{"name":"x","type":"int"}]}}]}`,
		sample:  map[string]any{"e": map[string]any{"x": int32(3)}},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"e","type":{"type":"error","name":"e","fields":[{"name":"x","type":"int"},{"name":"pad","type":"int","default":5}]}}]}`,
		resolveSample:  map[string]any{"e": map[string]any{"x": int32(3), "pad": int32(5)}},

		refDefs:    []string{fwElemDef},
		refFeature: `{"type":"record","name":"ns.Top","fields":[{"name":"e","type":"error","fields":[{"name":"el","type":"Elem"}]}]}`,
		refTwin:    `{"type":"record","name":"ns.Top","fields":[{"name":"e","type":{"type":"error","name":"e","fields":[{"name":"el","type":"Elem"}]}}]}`,
		refSample:  map[string]any{"e": map[string]any{"el": map[string]any{"x": int32(1)}}},

		defFeature:      `{"type":"record","name":"ns.H2","fields":[{"name":"derr","type":"error","fields":[{"name":"x","type":"int"}]}]}`,
		defTwin:         `{"type":"record","name":"ns.H2","fields":[{"name":"derr","type":{"type":"error","name":"derr","fields":[{"name":"x","type":"int"}]}}]}`,
		defFollow:       `{"type":"record","name":"ns.F2","fields":[{"name":"d","type":"derr"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
	{
		name:    "flat-enum",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"c","type":"enum","symbols":["A","B"]},{"name":"w","type":"int"}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"c","type":{"type":"enum","name":"c","symbols":["A","B"]}},{"name":"w","type":"int"}]}`,
		sample:  map[string]any{"c": "B", "w": int32(1)},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"c","type":{"type":"enum","name":"c","symbols":["A","B"]}},{"name":"w","type":"int"},{"name":"extra","type":"int","default":42}]}`,
		resolveSample:  map[string]any{"c": "B", "w": int32(1), "extra": int32(42)},

		// A flat enum field carries no sub-schema position, so there is no
		// reference INTO the feature; the definition direction below covers
		// the feature as a cross-parse definition.

		defFeature:      `{"type":"record","name":"ns.H3","fields":[{"name":"col","type":"enum","symbols":["R","G"]}]}`,
		defTwin:         `{"type":"record","name":"ns.H3","fields":[{"name":"col","type":{"type":"enum","name":"col","symbols":["R","G"]}}]}`,
		defFollow:       `{"type":"record","name":"ns.F3","fields":[{"name":"k","type":"col"}]}`,
		defFollowSample: map[string]any{"k": "G"},
	},
	{
		name:    "flat-fixed",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"fx","type":"fixed","size":2},{"name":"w","type":"int"}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"fx","type":{"type":"fixed","name":"fx","size":2}},{"name":"w","type":"int"}]}`,
		sample:  map[string]any{"fx": []byte{1, 2}, "w": int32(1)},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"fx","type":{"type":"fixed","name":"fx","size":2}},{"name":"w","type":"int"},{"name":"extra","type":"int","default":42}]}`,
		resolveSample:  map[string]any{"fx": []byte{1, 2}, "w": int32(1), "extra": int32(42)},

		// No sub-schema position (see flat-enum).

		defFeature:      `{"type":"record","name":"ns.H4","fields":[{"name":"dfx","type":"fixed","size":3}]}`,
		defTwin:         `{"type":"record","name":"ns.H4","fields":[{"name":"dfx","type":{"type":"fixed","name":"dfx","size":3}}]}`,
		defFollow:       `{"type":"record","name":"ns.F4","fields":[{"name":"d","type":"dfx"}]}`,
		defFollowSample: map[string]any{"d": []byte{1, 2, 3}},
	},
	{
		name:    "flat-array",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"list","type":"array","items":{"type":"record","name":"E5","fields":[{"name":"x","type":"int"}]}}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"list","type":{"type":"array","items":{"type":"record","name":"E5","fields":[{"name":"x","type":"int"}]}}}]}`,
		sample:  map[string]any{"list": []any{map[string]any{"x": int32(7)}}},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"list","type":{"type":"array","items":{"type":"record","name":"E5","fields":[{"name":"x","type":"int"},{"name":"pad","type":"int","default":5}]}}}]}`,
		resolveSample:  map[string]any{"list": []any{map[string]any{"x": int32(7), "pad": int32(5)}}},

		refDefs:    []string{fwElemDef},
		refFeature: `{"type":"record","name":"ns.Top","fields":[{"name":"list","type":"array","items":"Elem"}]}`,
		refTwin:    `{"type":"record","name":"ns.Top","fields":[{"name":"list","type":{"type":"array","items":"Elem"}}]}`,
		refSample:  map[string]any{"list": []any{map[string]any{"x": int32(1)}}},

		defFeature:      `{"type":"record","name":"ns.H5","fields":[{"name":"list","type":"array","items":{"type":"record","name":"D5","fields":[{"name":"x","type":"int"}]}}]}`,
		defTwin:         `{"type":"record","name":"ns.H5","fields":[{"name":"list","type":{"type":"array","items":{"type":"record","name":"D5","fields":[{"name":"x","type":"int"}]}}}]}`,
		defFollow:       `{"type":"record","name":"ns.F5","fields":[{"name":"d","type":"D5"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
	{
		name:    "flat-map",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"m","type":"map","values":{"type":"record","name":"E6","fields":[{"name":"x","type":"int"}]}}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"m","type":{"type":"map","values":{"type":"record","name":"E6","fields":[{"name":"x","type":"int"}]}}}]}`,
		sample:  map[string]any{"m": map[string]any{"k": map[string]any{"x": int32(7)}}},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"m","type":{"type":"map","values":{"type":"record","name":"E6","fields":[{"name":"x","type":"int"},{"name":"pad","type":"int","default":5}]}}}]}`,
		resolveSample:  map[string]any{"m": map[string]any{"k": map[string]any{"x": int32(7), "pad": int32(5)}}},

		refDefs:    []string{fwElemDef},
		refFeature: `{"type":"record","name":"ns.Top","fields":[{"name":"m","type":"map","values":"Elem"}]}`,
		refTwin:    `{"type":"record","name":"ns.Top","fields":[{"name":"m","type":{"type":"map","values":"Elem"}}]}`,
		refSample:  map[string]any{"m": map[string]any{"k": map[string]any{"x": int32(1)}}},

		defFeature:      `{"type":"record","name":"ns.H6","fields":[{"name":"m","type":"map","values":{"type":"record","name":"D6","fields":[{"name":"x","type":"int"}]}}]}`,
		defTwin:         `{"type":"record","name":"ns.H6","fields":[{"name":"m","type":{"type":"map","values":{"type":"record","name":"D6","fields":[{"name":"x","type":"int"}]}}}]}`,
		defFollow:       `{"type":"record","name":"ns.F6","fields":[{"name":"d","type":"D6"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
	{
		// The lift composes: a flat record field whose own fields hold a
		// flat array field. Walkers that handle only the first lift level
		// miss the inner one.
		name:    "flat-array-in-flat-record",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"rec","type":"record","fields":[{"name":"list","type":"array","items":{"type":"record","name":"E7","fields":[{"name":"x","type":"int"}]}}]}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"rec","type":{"type":"record","name":"rec","fields":[{"name":"list","type":{"type":"array","items":{"type":"record","name":"E7","fields":[{"name":"x","type":"int"}]}}}]}}]}`,
		sample:  map[string]any{"rec": map[string]any{"list": []any{map[string]any{"x": int32(7)}}}},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"rec","type":{"type":"record","name":"rec","fields":[{"name":"list","type":{"type":"array","items":{"type":"record","name":"E7","fields":[{"name":"x","type":"int"},{"name":"pad","type":"int","default":5}]}}}]}}]}`,
		resolveSample:  map[string]any{"rec": map[string]any{"list": []any{map[string]any{"x": int32(7), "pad": int32(5)}}}},

		refDefs:    []string{fwElemDef},
		refFeature: `{"type":"record","name":"ns.Top","fields":[{"name":"rec","type":"record","fields":[{"name":"list","type":"array","items":"Elem"}]}]}`,
		refTwin:    `{"type":"record","name":"ns.Top","fields":[{"name":"rec","type":{"type":"record","name":"rec","fields":[{"name":"list","type":{"type":"array","items":"Elem"}}]}}]}`,
		refSample:  map[string]any{"rec": map[string]any{"list": []any{map[string]any{"x": int32(1)}}}},

		defFeature:      `{"type":"record","name":"ns.H7","fields":[{"name":"drec","type":"record","fields":[{"name":"list","type":"array","items":{"type":"record","name":"D7","fields":[{"name":"x","type":"int"}]}}]}]}`,
		defTwin:         `{"type":"record","name":"ns.H7","fields":[{"name":"drec","type":{"type":"record","name":"drec","fields":[{"name":"list","type":{"type":"array","items":{"type":"record","name":"D7","fields":[{"name":"x","type":"int"}]}}}]}}]}`,
		defFollow:       `{"type":"record","name":"ns.F7","fields":[{"name":"d","type":"D7"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
	{
		// A stray "namespace" key on a flat array field is a FIELD prop:
		// the lift drops name/namespace keys for unnamed kinds, so the
		// items sit in the RECORD's namespace scope and a short reference
		// resolves there — never in the stray namespace. decoy.Elem (a
		// different shape) is registered alongside ns.Elem so a walker
		// that wrongly honors the stray namespace binds the WRONG type
		// and diverges from the twin, rather than merely dangling.
		name:    "flat-array-ns-decoy",
		feature: `{"type":"record","name":"ns.Top","fields":[{"name":"list","type":"array","items":{"type":"record","name":"E8","fields":[{"name":"x","type":"int"}]},"namespace":"decoy"}]}`,
		twin:    `{"type":"record","name":"ns.Top","fields":[{"name":"list","type":{"type":"array","items":{"type":"record","name":"E8","fields":[{"name":"x","type":"int"}]}},"namespace":"decoy"}]}`,
		sample:  map[string]any{"list": []any{map[string]any{"x": int32(7)}}},

		resolveAgainst: `{"type":"record","name":"ns.Top","fields":[{"name":"list","type":{"type":"array","items":{"type":"record","name":"E8","fields":[{"name":"x","type":"int"},{"name":"pad","type":"int","default":5}]}}}]}`,
		resolveSample:  map[string]any{"list": []any{map[string]any{"x": int32(7), "pad": int32(5)}}},

		refDefs:    []string{fwElemDef, fwDecoyElemDef},
		refFeature: `{"type":"record","name":"ns.Top","fields":[{"name":"list","type":"array","items":"Elem","namespace":"decoy"}]}`,
		refTwin:    `{"type":"record","name":"ns.Top","fields":[{"name":"list","type":{"type":"array","items":"Elem"},"namespace":"decoy"}]}`,
		refSample:  map[string]any{"list": []any{map[string]any{"x": int32(1)}}},

		defFeature:      `{"type":"record","name":"ns.H8","fields":[{"name":"list","type":"array","items":{"type":"record","name":"D8","fields":[{"name":"x","type":"int"}]},"namespace":"decoy"}]}`,
		defTwin:         `{"type":"record","name":"ns.H8","fields":[{"name":"list","type":{"type":"array","items":{"type":"record","name":"D8","fields":[{"name":"x","type":"int"}]}},"namespace":"decoy"}]}`,
		defFollow:       `{"type":"record","name":"ns.F8","fields":[{"name":"d","type":"D8"}]}`,
		defFollowSample: map[string]any{"d": map[string]any{"x": int32(2)}},
	},
}

// fwParse parses a schema with the row's opts, failing the test on error.
func fwParse(t *testing.T, schema string, row featureWalkerRow, extra ...avro.SchemaOpt) *avro.Schema {
	t.Helper()
	opts := append(slices.Clone(row.opts), extra...)
	s, err := avro.Parse(schema, opts...)
	if err != nil {
		t.Fatalf("parse: %v\n schema=%s", err, schema)
	}
	return s
}

// fwCacheParse registers defs and then parses main in one fresh SchemaCache.
func fwCacheParse(t *testing.T, defs []string, main string, row featureWalkerRow) *avro.Schema {
	t.Helper()
	var c avro.SchemaCache
	for i, d := range defs {
		if _, err := c.Parse(d, row.opts...); err != nil {
			t.Fatalf("cache def %d: %v", i, err)
		}
	}
	s, err := c.Parse(main, row.opts...)
	if err != nil {
		t.Fatalf("cache parse: %v\n schema=%s", err, main)
	}
	return s
}

// fwAssertTwinParity asserts the full self-containment / parity contract
// between a feature-spelled schema and its twin-spelled equivalent: byte-equal
// wire encoding of val, equal Canonical() and Rabin fingerprints, a String()
// and Canonical() that re-parse standalone to the same canonical bytes, and a
// Root() tree that rebuilds to the same fingerprint.
func fwAssertTwinParity(t *testing.T, sF, sT *avro.Schema, val map[string]any, row featureWalkerRow) {
	t.Helper()

	encF, err := sF.Encode(val)
	if err != nil {
		t.Fatalf("feature encode: %v", err)
	}
	encT, err := sT.Encode(val)
	if err != nil {
		t.Fatalf("twin encode: %v", err)
	}
	if !bytes.Equal(encF, encT) {
		t.Errorf("wire bytes diverge:\n feature=%x\n twin   =%x", encF, encT)
	}

	if !bytes.Equal(sF.Canonical(), sT.Canonical()) {
		t.Errorf("Canonical() diverges:\n feature=%s\n twin   =%s", sF.Canonical(), sT.Canonical())
	}
	if !bytes.Equal(sF.Fingerprint(avro.NewRabin()), sT.Fingerprint(avro.NewRabin())) {
		t.Errorf("Rabin fingerprint diverges for the same logical schema")
	}

	rp, err := avro.Parse(sF.String(), row.opts...)
	if err != nil {
		t.Errorf("Parse(feature.String()) FAILS — not self-contained: %v", err)
	} else if !bytes.Equal(rp.Canonical(), sF.Canonical()) {
		t.Errorf("feature String() re-parses to a DIFFERENT schema:\n reparse=%s\n orig   =%s", rp.Canonical(), sF.Canonical())
	}
	if _, err := avro.Parse(string(sF.Canonical()), row.opts...); err != nil {
		t.Errorf("Parse(feature.Canonical()) FAILS — not self-contained: %v", err)
	}

	root := sF.Root()
	rebuilt, err := root.Schema(row.opts...)
	if err != nil {
		t.Errorf("feature Root().Schema() rebuild FAILS: %v", err)
	} else if !bytes.Equal(rebuilt.Fingerprint(avro.NewRabin()), sT.Fingerprint(avro.NewRabin())) {
		t.Errorf("feature Root().Schema() rebuild fingerprint diverges from twin")
	}
}

// fwCountNodes walks a Root() tree, counting nodes. Root() represents an
// already-defined named type as a bare reference node (no re-definition), so
// the walk terminates without cycle tracking for these rows.
func fwCountNodes(n *avro.SchemaNode) int {
	if n == nil {
		return 0
	}
	c := 1
	for i := range n.Fields {
		c += fwCountNodes(&n.Fields[i].Type)
	}
	c += fwCountNodes(n.Items)
	c += fwCountNodes(n.Values)
	for i := range n.Branches {
		c += fwCountNodes(&n.Branches[i])
	}
	return c
}

var featureWalkerDrivers = []struct {
	name string
	run  func(t *testing.T, row featureWalkerRow)
}{
	{
		// Control: feature and twin are the same logical schema on the
		// wire — byte-equal binary and JSON encodes, equal decodes.
		name: "wire-parity",
		run: func(t *testing.T, row featureWalkerRow) {
			sF := fwParse(t, row.feature, row)
			sT := fwParse(t, row.twin, row)
			encF, err := sF.Encode(row.sample)
			if err != nil {
				t.Fatalf("feature encode: %v", err)
			}
			encT, err := sT.Encode(row.sample)
			if err != nil {
				t.Fatalf("twin encode: %v", err)
			}
			if !bytes.Equal(encF, encT) {
				t.Fatalf("binary wire diverges:\n feature=%x\n twin   =%x", encF, encT)
			}
			jF, err := sF.EncodeJSON(row.sample)
			if err != nil {
				t.Fatalf("feature EncodeJSON: %v", err)
			}
			jT, err := sT.EncodeJSON(row.sample)
			if err != nil {
				t.Fatalf("twin EncodeJSON: %v", err)
			}
			if !bytes.Equal(jF, jT) {
				t.Fatalf("JSON wire diverges:\n feature=%s\n twin   =%s", jF, jT)
			}
			var gotF, gotT map[string]any
			if _, err := sF.Decode(encT, &gotF); err != nil {
				t.Fatalf("feature decode of twin bytes: %v", err)
			}
			if _, err := sT.Decode(encF, &gotT); err != nil {
				t.Fatalf("twin decode of feature bytes: %v", err)
			}
			if !reflect.DeepEqual(gotF, gotT) {
				t.Errorf("cross-decoded values diverge:\n feature=%#v\n twin   =%#v", gotF, gotT)
			}
		},
	},
	{
		name: "string-reparse",
		run: func(t *testing.T, row featureWalkerRow) {
			for _, spelled := range []struct {
				which  string
				schema string
			}{{"feature", row.feature}, {"twin", row.twin}} {
				s := fwParse(t, spelled.schema, row)
				rp, err := avro.Parse(s.String(), row.opts...)
				if err != nil {
					t.Errorf("%s: Parse(String()) fails: %v", spelled.which, err)
					continue
				}
				if !bytes.Equal(rp.Canonical(), s.Canonical()) {
					t.Errorf("%s: String() re-parses to a different schema:\n reparse=%s\n orig   =%s", spelled.which, rp.Canonical(), s.Canonical())
				}
			}
		},
	},
	{
		name: "canonical-rabin",
		run: func(t *testing.T, row featureWalkerRow) {
			sF := fwParse(t, row.feature, row)
			sT := fwParse(t, row.twin, row)
			if !bytes.Equal(sF.Canonical(), sT.Canonical()) {
				t.Errorf("Canonical() diverges:\n feature=%s\n twin   =%s", sF.Canonical(), sT.Canonical())
			}
			if !bytes.Equal(sF.Fingerprint(avro.NewRabin()), sT.Fingerprint(avro.NewRabin())) {
				t.Errorf("Rabin fingerprint diverges")
			}
			if _, err := avro.Parse(string(sF.Canonical()), row.opts...); err != nil {
				t.Errorf("Parse(feature.Canonical()) fails — not self-contained: %v", err)
			}
		},
	},
	{
		name: "root-rebuild",
		run: func(t *testing.T, row featureWalkerRow) {
			sF := fwParse(t, row.feature, row)
			sT := fwParse(t, row.twin, row)
			rootF, rootT := sF.Root(), sT.Root()
			if nF, nT := fwCountNodes(&rootF), fwCountNodes(&rootT); nF != nT {
				t.Errorf("Root() tree size diverges: feature=%d twin=%d nodes", nF, nT)
			}
			for _, spelled := range []struct {
				which string
				s     *avro.Schema
				root  *avro.SchemaNode
			}{{"feature", sF, &rootF}, {"twin", sT, &rootT}} {
				rebuilt, err := spelled.root.Schema(row.opts...)
				if err != nil {
					t.Errorf("%s: Root().Schema() rebuild fails: %v", spelled.which, err)
					continue
				}
				if !bytes.Equal(rebuilt.Fingerprint(avro.NewRabin()), spelled.s.Fingerprint(avro.NewRabin())) {
					t.Errorf("%s: Root().Schema() rebuild fingerprint diverges from original", spelled.which)
				}
			}
		},
	},
	{
		// Cross-parse reference INTO the feature's subtree: the cache's
		// splice walkers must produce self-contained JSON forms equal to
		// the twin spelling's.
		name: "cache-ref-into",
		run: func(t *testing.T, row featureWalkerRow) {
			if row.refFeature == "" {
				t.Skip("feature has no reference-bearing subtree")
			}
			sF := fwCacheParse(t, row.refDefs, row.refFeature, row)
			sT := fwCacheParse(t, row.refDefs, row.refTwin, row)
			fwAssertTwinParity(t, sF, sT, row.refSample, row)
		},
	},
	{
		// Named definition INSIDE the feature's subtree: the cache's
		// collection walker must capture it so a later parse's reference
		// splices.
		name: "cache-def-inside",
		run: func(t *testing.T, row featureWalkerRow) {
			if row.defFeature == "" {
				t.Skip("feature carries no definition position")
			}
			sF := fwCacheParse(t, []string{row.defFeature}, row.defFollow, row)
			sT := fwCacheParse(t, []string{row.defTwin}, row.defFollow, row)
			fwAssertTwinParity(t, sF, sT, row.defFollowSample, row)
		},
	},
	{
		// Schema resolution recursing the feature's subtree, both
		// directions (feature as writer, feature as reader).
		name: "resolve-both-directions",
		run: func(t *testing.T, row featureWalkerRow) {
			if row.resolveAgainst == "" {
				t.Skip("row has no resolve variant")
			}
			sF := fwParse(t, row.feature, row)
			sT := fwParse(t, row.twin, row)
			mod := fwParse(t, row.resolveAgainst, row)

			wire, err := sF.Encode(row.sample)
			if err != nil {
				t.Fatalf("feature encode: %v", err)
			}
			rsF, err := avro.Resolve(sF, mod)
			if err != nil {
				t.Fatalf("Resolve(feature, mod): %v", err)
			}
			rsT, err := avro.Resolve(sT, mod)
			if err != nil {
				t.Fatalf("Resolve(twin, mod): %v", err)
			}
			var gotF, gotT map[string]any
			if _, err := rsF.Decode(wire, &gotF); err != nil {
				t.Fatalf("resolved(feature-writer) decode: %v", err)
			}
			if _, err := rsT.Decode(wire, &gotT); err != nil {
				t.Fatalf("resolved(twin-writer) decode: %v", err)
			}
			if !reflect.DeepEqual(gotF, gotT) {
				t.Errorf("feature-as-writer resolved decode diverges:\n feature=%#v\n twin   =%#v", gotF, gotT)
			}

			wireMod, err := mod.Encode(row.resolveSample)
			if err != nil {
				t.Fatalf("mod encode: %v", err)
			}
			rs2F, err := avro.Resolve(mod, sF)
			if err != nil {
				t.Fatalf("Resolve(mod, feature): %v", err)
			}
			rs2T, err := avro.Resolve(mod, sT)
			if err != nil {
				t.Fatalf("Resolve(mod, twin): %v", err)
			}
			var got2F, got2T map[string]any
			if _, err := rs2F.Decode(wireMod, &got2F); err != nil {
				t.Fatalf("resolved(feature-reader) decode: %v", err)
			}
			if _, err := rs2T.Decode(wireMod, &got2T); err != nil {
				t.Fatalf("resolved(twin-reader) decode: %v", err)
			}
			if !reflect.DeepEqual(got2F, got2T) {
				t.Errorf("feature-as-reader resolved decode diverges:\n feature=%#v\n twin   =%#v", got2F, got2T)
			}
		},
	},
	{
		// Resolved JSON decoding: writer-shaped JSON through the resolving
		// schema, feature vs twin as the writer.
		name: "resolved-decode-json",
		run: func(t *testing.T, row featureWalkerRow) {
			if row.resolveAgainst == "" {
				t.Skip("row has no resolve variant")
			}
			sF := fwParse(t, row.feature, row)
			sT := fwParse(t, row.twin, row)
			mod := fwParse(t, row.resolveAgainst, row)
			jsonWire, err := sF.EncodeJSON(row.sample)
			if err != nil {
				t.Fatalf("feature EncodeJSON: %v", err)
			}
			rsF, err := avro.Resolve(sF, mod)
			if err != nil {
				t.Fatalf("Resolve(feature, mod): %v", err)
			}
			rsT, err := avro.Resolve(sT, mod)
			if err != nil {
				t.Fatalf("Resolve(twin, mod): %v", err)
			}
			var gotF, gotT map[string]any
			if err := rsF.DecodeJSON(jsonWire, &gotF); err != nil {
				t.Fatalf("resolved(feature-writer) DecodeJSON: %v", err)
			}
			if err := rsT.DecodeJSON(jsonWire, &gotT); err != nil {
				t.Fatalf("resolved(twin-writer) DecodeJSON: %v", err)
			}
			if !reflect.DeepEqual(gotF, gotT) {
				t.Errorf("resolved DecodeJSON diverges:\n feature=%#v\n twin   =%#v", gotF, gotT)
			}
		},
	},
	{
		// Custom types through resolution: a custom-baked WRITER forces
		// Resolve to build the custom-free writer view (an internal
		// re-parse of the feature spelling) for resolved JSON decoding,
		// and a custom-carrying READER applies its decoders through the
		// resolved decode. Both must treat the spellings identically. The
		// decode transform is value-changing (×10) so a skipped custom
		// cannot masquerade as a fired one.
		name: "resolve-custom-views",
		run: func(t *testing.T, row featureWalkerRow) {
			if row.resolveAgainst == "" {
				t.Skip("row has no resolve variant")
			}
			xform := avro.WithCustomType(avro.NewCustomType(
				"",
				(func(int32, *avro.SchemaNode) (int32, error))(nil),
				func(a int32, _ *avro.SchemaNode) (int32, error) { return a * 10, nil },
			))
			sFC := fwParse(t, row.feature, row, xform)
			sTC := fwParse(t, row.twin, row, xform)
			mod := fwParse(t, row.resolveAgainst, row)

			// Custom-baked writer: the custom-free view re-parses the
			// feature spelling internally.
			jsonWire, err := sFC.EncodeJSON(row.sample)
			if err != nil {
				t.Fatalf("feature EncodeJSON: %v", err)
			}
			rsF, err := avro.Resolve(sFC, mod)
			if err != nil {
				t.Fatalf("Resolve(feature+custom, mod): %v", err)
			}
			rsT, err := avro.Resolve(sTC, mod)
			if err != nil {
				t.Fatalf("Resolve(twin+custom, mod): %v", err)
			}
			var gotF, gotT map[string]any
			if err := rsF.DecodeJSON(jsonWire, &gotF); err != nil {
				t.Fatalf("resolved(custom-writer) DecodeJSON: %v", err)
			}
			if err := rsT.DecodeJSON(jsonWire, &gotT); err != nil {
				t.Fatalf("resolved(custom-twin-writer) DecodeJSON: %v", err)
			}
			if !reflect.DeepEqual(gotF, gotT) {
				t.Errorf("custom-free writer view decode diverges:\n feature=%#v\n twin   =%#v", gotF, gotT)
			}

			// Custom-carrying reader: reader customs fire through the
			// resolved binary decode.
			wireMod, err := mod.Encode(row.resolveSample)
			if err != nil {
				t.Fatalf("mod encode: %v", err)
			}
			rs2F, err := avro.Resolve(mod, sFC)
			if err != nil {
				t.Fatalf("Resolve(mod, feature+custom): %v", err)
			}
			rs2T, err := avro.Resolve(mod, sTC)
			if err != nil {
				t.Fatalf("Resolve(mod, twin+custom): %v", err)
			}
			var got2F, got2T map[string]any
			if _, err := rs2F.Decode(wireMod, &got2F); err != nil {
				t.Fatalf("resolved(custom-reader) decode: %v", err)
			}
			if _, err := rs2T.Decode(wireMod, &got2T); err != nil {
				t.Fatalf("resolved(custom-twin-reader) decode: %v", err)
			}
			if !reflect.DeepEqual(got2F, got2T) {
				t.Errorf("custom-reader resolved decode diverges:\n feature=%#v\n twin   =%#v", got2F, got2T)
			}
		},
	},
	{
		name: "compat",
		run: func(t *testing.T, row featureWalkerRow) {
			sF := fwParse(t, row.feature, row)
			sT := fwParse(t, row.twin, row)
			for _, pair := range []struct {
				which          string
				writer, reader *avro.Schema
			}{
				{"feature-self", sF, sF},
				{"twin-self", sT, sT},
				{"feature-writer", sF, sT},
				{"feature-reader", sT, sF},
			} {
				if err := avro.CheckCompatibility(pair.writer, pair.reader); err != nil {
					t.Errorf("CheckCompatibility(%s): %v", pair.which, err)
				}
			}
		},
	},
	{
		// Single-object encoding: byte-identical framing (the header
		// carries the writer fingerprint) and cross-spelling decode.
		name: "soe-roundtrip",
		run: func(t *testing.T, row featureWalkerRow) {
			sF := fwParse(t, row.feature, row)
			sT := fwParse(t, row.twin, row)
			bF, err := sF.AppendSingleObject(nil, row.sample)
			if err != nil {
				t.Fatalf("feature AppendSingleObject: %v", err)
			}
			bT, err := sT.AppendSingleObject(nil, row.sample)
			if err != nil {
				t.Fatalf("twin AppendSingleObject: %v", err)
			}
			if !bytes.Equal(bF, bT) {
				t.Fatalf("single-object bytes diverge:\n feature=%x\n twin   =%x", bF, bT)
			}
			var gotF, gotT map[string]any
			if _, err := sT.DecodeSingleObject(bF, &gotT); err != nil {
				t.Fatalf("twin DecodeSingleObject(feature bytes): %v", err)
			}
			if _, err := sF.DecodeSingleObject(bT, &gotF); err != nil {
				t.Fatalf("feature DecodeSingleObject(twin bytes): %v", err)
			}
			if !reflect.DeepEqual(gotF, gotT) {
				t.Errorf("single-object cross-decodes diverge:\n feature=%#v\n twin   =%#v", gotF, gotT)
			}
		},
	},
}

func TestMatrix_FeatureWalkerParity(t *testing.T) {
	for _, row := range featureWalkerRows {
		t.Run(row.name, func(t *testing.T) {
			for _, d := range featureWalkerDrivers {
				t.Run(d.name, func(t *testing.T) { d.run(t, row) })
			}
		})
	}
}
