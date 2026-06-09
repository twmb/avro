package avro_test

import (
	"testing"

	"github.com/twmb/avro"
)

// TestRegression_SchemaCacheCanonicalSelfContained pins that a schema built via
// SchemaCache that references a type registered in a PRIOR Parse produces a
// self-contained Canonical()/Fingerprint()/Root()/String() — identical to the
// logically-equal inline-defined schema. The cache stores only the resolved
// node, so the JSON forms used to held a dangling bare reference ("ns.Inner"),
// giving a non-re-parseable canonical form and a cross-language-divergent
// fingerprint (breaking single-object-encoding interop).
func TestRegression_SchemaCacheCanonicalSelfContained(t *testing.T) {
	var c avro.SchemaCache
	if _, err := c.Parse(`{"type":"record","name":"ns.Inner","fields":[{"name":"x","type":"int"}]}`); err != nil {
		t.Fatalf("register Inner: %v", err)
	}
	viaCache, err := c.Parse(`{"type":"record","name":"Outer","fields":[` +
		`{"name":"i","type":"ns.Inner"},{"name":"j","type":"ns.Inner"}]}`)
	if err != nil {
		t.Fatalf("parse Outer via cache: %v", err)
	}
	inline := avro.MustParse(`{"type":"record","name":"Outer","fields":[` +
		`{"name":"i","type":{"type":"record","name":"ns.Inner","fields":[{"name":"x","type":"int"}]}},` +
		`{"name":"j","type":"ns.Inner"}]}`)

	// Control: identical wire (same logical schema).
	val := map[string]any{"i": map[string]any{"x": int32(1)}, "j": map[string]any{"x": int32(2)}}
	wc, err := viaCache.Encode(val)
	if err != nil {
		t.Fatalf("cache encode: %v", err)
	}
	wi, err := inline.Encode(val)
	if err != nil {
		t.Fatalf("inline encode: %v", err)
	}
	if string(wc) != string(wi) {
		t.Fatalf("control: wire differs (not the same logical schema)")
	}

	// Canonical form must be self-contained and equal to the inline schema's.
	if string(viaCache.Canonical()) != string(inline.Canonical()) {
		t.Errorf("Canonical() diverges:\n cache : %s\n inline: %s", viaCache.Canonical(), inline.Canonical())
	}
	if _, err := avro.Parse(string(viaCache.Canonical())); err != nil {
		t.Errorf("Parse(cache.Canonical()) FAILS — canonical form is not a valid schema: %v", err)
	}

	// Fingerprint must match (cross-language / SOE interop).
	if string(viaCache.Fingerprint(avro.NewRabin())) != string(inline.Fingerprint(avro.NewRabin())) {
		t.Errorf("Fingerprint() diverges for the same logical schema")
	}

	// Root() must rebuild a self-contained tree.
	root := viaCache.Root()
	if _, err := root.Schema(); err != nil {
		t.Errorf("Root().Schema() FAILS to rebuild a cache-built schema: %v", err)
	}
}

// TestRegression_SchemaCacheSOEInterop pins the user-visible consequence: a
// single-object-encoded message from a cache-built producer round-trips through
// a consumer holding the logically-identical inline schema (fingerprints match).
func TestRegression_SchemaCacheSOEInterop(t *testing.T) {
	var c avro.SchemaCache
	if _, err := c.Parse(`{"type":"record","name":"ns.Inner","fields":[{"name":"x","type":"int"}]}`); err != nil {
		t.Fatalf("register Inner: %v", err)
	}
	producer, err := c.Parse(`{"type":"record","name":"Outer","fields":[` +
		`{"name":"i","type":"ns.Inner"},{"name":"j","type":"ns.Inner"}]}`)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	consumer := avro.MustParse(`{"type":"record","name":"Outer","fields":[` +
		`{"name":"i","type":{"type":"record","name":"ns.Inner","fields":[{"name":"x","type":"int"}]}},` +
		`{"name":"j","type":"ns.Inner"}]}`)

	val := map[string]any{"i": map[string]any{"x": int32(1)}, "j": map[string]any{"x": int32(2)}}
	msg, err := producer.AppendSingleObject(nil, val)
	if err != nil {
		t.Fatalf("AppendSingleObject: %v", err)
	}
	var out map[string]any
	if _, err := consumer.DecodeSingleObject(msg, &out); err != nil {
		t.Errorf("DecodeSingleObject across cache/inline of the same schema FAILS: %v", err)
	}
}

// TestRegression_SchemaCacheSelfContainedEdgeCases exercises the converter's
// delicate paths: a recursive cache type (cycle handling), a cache type with a
// field default (default round-trip), and enum/fixed cache refs (the bug is
// kind-agnostic). Each cache-built schema must have canonical form and
// fingerprint identical to the inline-defined equivalent, and re-parse.
func TestRegression_SchemaCacheSelfContainedEdgeCases(t *testing.T) {
	cases := []struct {
		name   string
		defs   []string // types to register first
		ref    string   // schema referencing them (cache-built)
		inline string   // logically-identical inline schema
	}{
		{
			name: "recursive",
			defs: []string{`{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]},{"name":"v","type":"int"}]}`},
			ref:  `{"type":"record","name":"Wrap","fields":[{"name":"head","type":"Node"}]}`,
			inline: `{"type":"record","name":"Wrap","fields":[{"name":"head","type":` +
				`{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]},{"name":"v","type":"int"}]}}]}`,
		},
		{
			name: "field-default",
			defs: []string{`{"type":"record","name":"D","fields":[{"name":"x","type":"int","default":7}]}`},
			ref:  `{"type":"record","name":"DW","fields":[{"name":"d","type":"D"}]}`,
			inline: `{"type":"record","name":"DW","fields":[{"name":"d","type":` +
				`{"type":"record","name":"D","fields":[{"name":"x","type":"int","default":7}]}}]}`,
		},
		{
			name: "enum-and-fixed",
			defs: []string{`{"type":"enum","name":"E","symbols":["A","B"]}`, `{"type":"fixed","name":"F","size":4}`},
			ref:  `{"type":"record","name":"EF","fields":[{"name":"e","type":"E"},{"name":"f","type":"F"}]}`,
			inline: `{"type":"record","name":"EF","fields":[` +
				`{"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]}},` +
				`{"name":"f","type":{"type":"fixed","name":"F","size":4}}]}`,
		},
		{
			name: "namespaced",
			defs: []string{`{"type":"record","name":"Inner","namespace":"a.b","fields":[{"name":"x","type":"int"}]}`},
			ref:  `{"type":"record","name":"Outer","namespace":"a.b","fields":[{"name":"i","type":"Inner"}]}`,
			inline: `{"type":"record","name":"Outer","namespace":"a.b","fields":[{"name":"i","type":` +
				`{"type":"record","name":"Inner","namespace":"a.b","fields":[{"name":"x","type":"int"}]}}]}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var c avro.SchemaCache
			for _, d := range tc.defs {
				if _, err := c.Parse(d); err != nil {
					t.Fatalf("register %s: %v", d, err)
				}
			}
			viaCache, err := c.Parse(tc.ref)
			if err != nil {
				t.Fatalf("parse ref via cache: %v", err)
			}
			inline := avro.MustParse(tc.inline)
			if string(viaCache.Canonical()) != string(inline.Canonical()) {
				t.Errorf("Canonical diverges:\n cache : %s\n inline: %s", viaCache.Canonical(), inline.Canonical())
			}
			if string(viaCache.Fingerprint(avro.NewRabin())) != string(inline.Fingerprint(avro.NewRabin())) {
				t.Errorf("Fingerprint diverges")
			}
			if _, err := avro.Parse(string(viaCache.Canonical())); err != nil {
				t.Errorf("Parse(cache.Canonical()) FAILS: %v", err)
			}
			root := viaCache.Root()
			if _, err := root.Schema(); err != nil {
				t.Errorf("Root().Schema() FAILS: %v", err)
			}
		})
	}
}
