package avro_test

import (
	"fmt"
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

// TestRegression_SchemaCacheRebuildPreservesMetadata pins that making a
// cache-referenced schema self-contained preserves every original attribute —
// node doc, field doc/order/props, at both the outer and the inlined inner
// level — exactly as the logically-identical inline schema does. (The first
// self-containment fix rebuilt from the attribute-poor node tree and dropped
// these; the JSON-inline approach preserves them.)
func TestRegression_SchemaCacheRebuildPreservesMetadata(t *testing.T) {
	innerDef := `{"type":"record","name":"ns.Inner","doc":"inner doc","fields":[` +
		`{"name":"x","type":"int","doc":"x field doc","order":"descending","ns.fprop":"xfp"}]}`
	outer := func(ref string) string {
		return `{"type":"record","name":"Outer","doc":"outer doc","fields":[` +
			`{"name":"i","type":` + ref + `,"doc":"i field doc","order":"ignore","ns.iprop":"ifp"}]}`
	}
	var c avro.SchemaCache
	if _, err := c.Parse(innerDef); err != nil {
		t.Fatalf("register Inner: %v", err)
	}
	viaCache, err := c.Parse(outer(`"ns.Inner"`))
	if err != nil {
		t.Fatalf("parse Outer via cache: %v", err)
	}
	inline := avro.MustParse(outer(innerDef))

	rc, ri := viaCache.Root(), inline.Root()
	if rc.Doc != ri.Doc {
		t.Errorf("Outer.Doc: cache=%q inline=%q", rc.Doc, ri.Doc)
	}
	if rc.Fields[0].Doc != ri.Fields[0].Doc {
		t.Errorf("Outer.i.Doc: cache=%q inline=%q", rc.Fields[0].Doc, ri.Fields[0].Doc)
	}
	if rc.Fields[0].Order != ri.Fields[0].Order {
		t.Errorf("Outer.i.Order: cache=%q inline=%q", rc.Fields[0].Order, ri.Fields[0].Order)
	}
	if fmt.Sprint(rc.Fields[0].Props) != fmt.Sprint(ri.Fields[0].Props) {
		t.Errorf("Outer.i.Props: cache=%v inline=%v", rc.Fields[0].Props, ri.Fields[0].Props)
	}
	// The inlined inner type's own metadata must survive too.
	ci, ii := rc.Fields[0].Type, ri.Fields[0].Type
	if ci.Doc != ii.Doc {
		t.Errorf("Inner.Doc: cache=%q inline=%q", ci.Doc, ii.Doc)
	}
	if ci.Fields[0].Doc != ii.Fields[0].Doc {
		t.Errorf("Inner.x.Doc: cache=%q inline=%q", ci.Fields[0].Doc, ii.Fields[0].Doc)
	}
	if ci.Fields[0].Order != ii.Fields[0].Order {
		t.Errorf("Inner.x.Order: cache=%q inline=%q", ci.Fields[0].Order, ii.Fields[0].Order)
	}
	if fmt.Sprint(ci.Fields[0].Props) != fmt.Sprint(ii.Fields[0].Props) {
		t.Errorf("Inner.x.Props: cache=%v inline=%v", ci.Fields[0].Props, ii.Fields[0].Props)
	}
}

// TestRegression_SchemaCacheTransitiveRefs pins transitive cross-parse
// references: C → B → A, each defined in its own Parse. C's self-contained form
// must inline B (which itself inlines A), matching the fully-inline schema.
func TestRegression_SchemaCacheTransitiveRefs(t *testing.T) {
	var c avro.SchemaCache
	aDef := `{"type":"record","name":"A","fields":[{"name":"x","type":"int"}]}`
	bDef := `{"type":"record","name":"B","fields":[{"name":"a","type":"A"}]}`
	if _, err := c.Parse(aDef); err != nil {
		t.Fatalf("A: %v", err)
	}
	if _, err := c.Parse(bDef); err != nil {
		t.Fatalf("B: %v", err)
	}
	viaCache, err := c.Parse(`{"type":"record","name":"C","fields":[{"name":"b","type":"B"}]}`)
	if err != nil {
		t.Fatalf("C: %v", err)
	}
	inline := avro.MustParse(`{"type":"record","name":"C","fields":[{"name":"b","type":` +
		`{"type":"record","name":"B","fields":[{"name":"a","type":` +
		`{"type":"record","name":"A","fields":[{"name":"x","type":"int"}]}}]}}]}`)
	if string(viaCache.Canonical()) != string(inline.Canonical()) {
		t.Errorf("transitive Canonical diverges:\n cache : %s\n inline: %s", viaCache.Canonical(), inline.Canonical())
	}
	if string(viaCache.Fingerprint(avro.NewRabin())) != string(inline.Fingerprint(avro.NewRabin())) {
		t.Errorf("transitive Fingerprint diverges")
	}
	if _, err := avro.Parse(string(viaCache.Canonical())); err != nil {
		t.Errorf("Parse(transitive cache.Canonical()) FAILS: %v", err)
	}
}

// TestRegression_SchemaCacheCrossNamespaceSplice pins that splicing an inherited
// definition into a referencing schema preserves the definition's resolved
// namespace, regardless of the enclosing namespace at the reference site. A
// definition that inherited its namespace (or sat in the null namespace) is
// stored with no explicit "namespace"; splicing it verbatim into a different
// scope would re-inherit that scope's namespace and resolve to the wrong
// fullname (e.g. com.a.Inner becoming com.b.Inner) — a self-contained-but-WRONG
// form whose canonical/fingerprint silently diverge from the wire schema and
// from every other Avro implementation. Stored definitions therefore carry an
// explicit namespace. Each case must match the logically-identical inline
// schema on wire (control), canonical, and fingerprint, and re-parse cleanly.
func TestRegression_SchemaCacheCrossNamespaceSplice(t *testing.T) {
	cases := []struct {
		name   string
		defs   []string
		ref    string
		inline string
		value  any
	}{
		{
			name:   "inherited-ns-referenced-from-other-ns",
			defs:   []string{`{"type":"record","name":"P","namespace":"com.a","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}}]}`},
			ref:    `{"type":"record","name":"Q","namespace":"com.b","fields":[{"name":"y","type":"com.a.Inner"}]}`,
			inline: `{"type":"record","name":"Q","namespace":"com.b","fields":[{"name":"y","type":{"type":"record","name":"Inner","namespace":"com.a","fields":[{"name":"x","type":"int"}]}}]}`,
			value:  map[string]any{"y": map[string]any{"x": int32(1)}},
		},
		{
			name:   "null-ns-referenced-from-namespaced",
			defs:   []string{`{"type":"record","name":"X","fields":[{"name":"v","type":"int"}]}`},
			ref:    `{"type":"record","name":"Q","namespace":"com.b","fields":[{"name":"x","type":"X"}]}`,
			inline: `{"type":"record","name":"Q","namespace":"com.b","fields":[{"name":"x","type":{"type":"record","name":"X","namespace":"","fields":[{"name":"v","type":"int"}]}}]}`,
			value:  map[string]any{"x": map[string]any{"v": int32(2)}},
		},
		{
			name:   "deep-inherited-chain-into-other-ns",
			defs:   []string{`{"type":"record","name":"Root","namespace":"x.y","fields":[{"name":"m","type":{"type":"record","name":"Mid","fields":[{"name":"l","type":{"type":"record","name":"Leaf","fields":[{"name":"z","type":"int"}]}}]}}]}`},
			ref:    `{"type":"record","name":"Q","namespace":"other","fields":[{"name":"mid","type":"x.y.Mid"}]}`,
			inline: `{"type":"record","name":"Q","namespace":"other","fields":[{"name":"mid","type":{"type":"record","name":"Mid","namespace":"x.y","fields":[{"name":"l","type":{"type":"record","name":"Leaf","namespace":"x.y","fields":[{"name":"z","type":"int"}]}}]}}]}`,
			value:  map[string]any{"mid": map[string]any{"l": map[string]any{"z": int32(4)}}},
		},
		{
			name:   "recursive-inherited-ns-into-other-ns",
			defs:   []string{`{"type":"record","name":"Holder","namespace":"r.s","fields":[{"name":"node","type":{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]},{"name":"v","type":"int"}]}}]}`},
			ref:    `{"type":"record","name":"W","namespace":"diff","fields":[{"name":"head","type":"r.s.Node"}]}`,
			inline: `{"type":"record","name":"W","namespace":"diff","fields":[{"name":"head","type":{"type":"record","name":"Node","namespace":"r.s","fields":[{"name":"next","type":["null","Node"]},{"name":"v","type":"int"}]}}]}`,
			value:  map[string]any{"head": map[string]any{"next": nil, "v": int32(8)}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var c avro.SchemaCache
			for _, d := range tc.defs {
				if _, err := c.Parse(d); err != nil {
					t.Fatalf("register %q: %v", d, err)
				}
			}
			viaCache, err := c.Parse(tc.ref)
			if err != nil {
				t.Fatalf("parse ref via cache: %v", err)
			}
			inline := avro.MustParse(tc.inline)

			// Control: identical wire confirms the node tree resolved the same
			// fullnames — so canonical/fingerprint MUST match too.
			wc, errc := viaCache.Encode(tc.value)
			wi, erri := inline.Encode(tc.value)
			if errc != nil || erri != nil {
				t.Fatalf("encode err: cache=%v inline=%v", errc, erri)
			}
			if fmt.Sprintf("%x", wc) != fmt.Sprintf("%x", wi) {
				t.Fatalf("control wire mismatch:\n cache=%x\n inline=%x", wc, wi)
			}
			if string(viaCache.Canonical()) != string(inline.Canonical()) {
				t.Errorf("Canonical diverges:\n cache : %s\n inline: %s", viaCache.Canonical(), inline.Canonical())
			}
			if string(viaCache.Fingerprint(avro.NewRabin())) != string(inline.Fingerprint(avro.NewRabin())) {
				t.Errorf("Fingerprint diverges (namespace lost on splice)")
			}
			if _, err := avro.Parse(string(viaCache.Canonical())); err != nil {
				t.Errorf("Parse(cache.Canonical()) FAILS: %v\n  %s", err, viaCache.Canonical())
			}
		})
	}
}
