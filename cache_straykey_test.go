package avro

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// A reserved container key on a kind that does not bind it — a stray
// "items"/"values"/"fields" on a primitive object — parses as inert
// as-written metadata: the wire parser never name-binds anything inside
// it. Every consumer that treats container keys as SCHEMA positions
// (collecting definitions for cross-parse reference, splicing cached
// definitions over references, registering names for metadata default
// coercion) must therefore enumerate only the keys the node's kind BINDS
// (fields → record/error, items → array, values → map), or it consumes
// structure the parse never bound. The pins in this file lock that gate
// for the SchemaCache walkers and the metadata name table, and lock the
// one deliberate exception: the SchemaNode metadata walker surfaces stray
// container keys as-written (a read-only duty with no registration or
// mutation).

// cacheStrayRealGDef defines n.G with field "h" of type string — the
// definition the parser actually binds in these tests.
const cacheStrayRealGDef = `{"type":"record","name":"n.G","fields":[{"name":"h","type":"string"}]}`

// cacheStrayGDef returns a CONFLICTING definition of n.G (field "g" of the
// given type) — the shape planted under a stray structural key.
func cacheStrayGDef(fieldType string) string {
	return `{"type":"record","name":"n.G","fields":[{"name":"g","type":"` + fieldType + `"}]}`
}

// cacheStrayCarrier wraps payload under the given stray key on a
// primitive-kind object. For "items"/"values" the payload sits directly
// under the key; for "fields" it sits as a field's type, the position the
// fields walk would descend.
func cacheStrayCarrier(kind, key, payload string) string {
	if key == "fields" {
		return `{"type":"` + kind + `","fields":[{"name":"f","type":` + payload + `}]}`
	}
	return `{"type":"` + kind + `","` + key + `":` + payload + `}`
}

// cacheParseSeq parses texts in order into one fresh SchemaCache and
// returns the last schema.
func cacheParseSeq(t *testing.T, texts ...string) *Schema {
	t.Helper()
	var c SchemaCache
	var s *Schema
	var err error
	for _, text := range texts {
		if s, err = c.Parse(text); err != nil {
			t.Fatalf("cache parse of %s: %v", text, err)
		}
	}
	return s
}

// cacheSurfaceImage captures every metadata-derived surface of a schema:
// the canonical form, the Rabin fingerprint, the single-object-encoding
// header (of sample's encoding), the stored JSON text, and the metadata
// tree. Two schemas that must describe the same logical schema must agree
// on all five.
type cacheSurfaceImage struct {
	canonical, fp, soe, str string
	root                    SchemaNode
}

func cacheSurfaces(t *testing.T, s *Schema, sample any) cacheSurfaceImage {
	t.Helper()
	b, err := s.AppendSingleObject(nil, sample)
	if err != nil {
		t.Fatalf("AppendSingleObject: %v", err)
	}
	return cacheSurfaceImage{
		canonical: string(s.Canonical()),
		fp:        fmt.Sprintf("%x", s.Fingerprint(NewRabin())),
		soe:       fmt.Sprintf("%x", b[:10]),
		str:       s.String(),
		root:      *s.Root(),
	}
}

func assertCacheSurfacesEqual(t *testing.T, got, want cacheSurfaceImage) {
	t.Helper()
	if got.canonical != want.canonical {
		t.Errorf("Canonical describes a different schema:\n got  %s\n want %s", got.canonical, want.canonical)
	}
	if got.fp != want.fp {
		t.Errorf("fingerprint %s, want %s", got.fp, want.fp)
	}
	if got.soe != want.soe {
		t.Errorf("single-object header %s, want %s", got.soe, want.soe)
	}
	if got.str != want.str {
		t.Errorf("String:\n got  %s\n want %s", got.str, want.str)
	}
	if !reflect.DeepEqual(got.root, want.root) {
		t.Errorf("Root trees differ:\n got  %+v\n want %+v", got.root, want.root)
	}
}

// A definition of n.G planted in a stray structural key must not enter the
// cache's cross-parse definition store: the store is first-wins, so an
// inert-position body occupying the fullname would shadow the REAL
// definition parsed later, and a cross-parse reference's metadata surfaces
// (Canonical / fingerprint / SOE header / String / Root) — rebuilt from a
// splice of the stored definition — would then describe a schema the wire
// codec (which resolves the parser-bound definition) rejects.
func TestRegression_CacheStrayKeyDefCrossParseSurfaces(t *testing.T) {
	t.Parallel()
	ref := `{"type":"array","items":"n.G"}`
	accept := []map[string]any{{"h": "x"}}
	reject := []map[string]any{{"g": int32(7)}}
	for _, key := range []string{"items", "values", "fields"} {
		t.Run(key, func(t *testing.T) {
			s := cacheParseSeq(t,
				cacheStrayCarrier("int", key, cacheStrayGDef("int")),
				cacheStrayRealGDef,
				ref,
			)
			control := cacheParseSeq(t, cacheStrayRealGDef, ref)

			// The wire codec resolves the parser-bound definition.
			if _, err := s.Encode(accept); err != nil {
				t.Fatalf("encode of the bound-definition value: %v", err)
			}
			if _, err := s.Encode(reject); err == nil {
				t.Fatalf("encode of the stray-shaped value unexpectedly accepted")
			}
			// Every metadata surface must describe that same schema.
			assertCacheSurfacesEqual(t, cacheSurfaces(t, s, accept), cacheSurfaces(t, control, accept))
		})
	}
}

// The splice twin of the same gate: a cached definition must not be
// spliced over a reference string sitting in a stray structural key. The
// as-written authority is a plain (cache-less) Parse of the same text —
// SchemaCache.Parse must surface identical metadata. The stored text is
// compared STRUCTURALLY (SchemaCache.Parse normalizes its input through a
// json.Marshal round trip, so key order differs from the caller's
// spelling by design); a spliced definition still fails the structural
// comparison because it replaces the reference string with an object.
func TestRegression_CacheSpliceStrayKeyAsWritten(t *testing.T) {
	t.Parallel()
	sample := int32(7)
	for _, key := range []string{"items", "values", "fields"} {
		t.Run(key, func(t *testing.T) {
			text := cacheStrayCarrier("int", key, `"n.G"`)
			plain, err := Parse(text)
			if err != nil {
				t.Fatalf("plain parse: %v", err)
			}
			cached := cacheParseSeq(t, cacheStrayRealGDef, text)
			got, want := cacheSurfaces(t, cached, sample), cacheSurfaces(t, plain, sample)
			var gotTree, wantTree any
			if err := json.Unmarshal([]byte(got.str), &gotTree); err != nil {
				t.Fatalf("stored text does not unmarshal: %v", err)
			}
			if err := json.Unmarshal([]byte(want.str), &wantTree); err != nil {
				t.Fatalf("plain text does not unmarshal: %v", err)
			}
			if !reflect.DeepEqual(gotTree, wantTree) {
				t.Errorf("stored text structure:\n got  %s\n want %s", got.str, want.str)
			}
			got.str, want.str = "", ""
			assertCacheSurfacesEqual(t, got, want)
		})
	}
}

// The metadata name table (name-reference default coercion) registers
// exactly what the wire builder registers. A conflicting n.G body inside a
// stray key must not enter the table in EITHER parse order — pre-gate the
// walk order decided which body a name-ref default coerced through, so a
// stray walked after the real definition silently flipped a string-field
// default into the stray's bytes materialization.
func TestRegression_MetadataNameTableIgnoresStrayKeyDef(t *testing.T) {
	t.Parallel()
	realDef := `{"name":"f1","type":{"type":"record","name":"n.G","fields":[{"name":"b","type":"string"}]}}`
	strayCarrier := `{"name":"f2","type":{"type":"int","items":{"type":"record","name":"n.G","fields":[{"name":"b","type":"bytes"}]}}}`
	refWithDefault := `{"name":"f3","type":"n.G","default":{"b":"AQ"}}`
	for name, order := range map[string][]string{
		"real_then_stray": {realDef, strayCarrier, refWithDefault},
		"stray_then_real": {strayCarrier, realDef, refWithDefault},
	} {
		t.Run(name, func(t *testing.T) {
			s, err := Parse(`{"type":"record","name":"R","fields":[` + strings.Join(order, ",") + `]}`)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			var f3 *SchemaField
			root := s.Root()
			for i := range root.Fields {
				if root.Fields[i].Name == "f3" {
					f3 = &root.Fields[i]
				}
			}
			if f3 == nil {
				t.Fatal("field f3 missing from Root")
			}
			d, ok := f3.Default.(map[string]any)
			if !ok {
				t.Fatalf("f3 Default is %T, want map", f3.Default)
			}
			// The bound n.G's "b" is a string field: the default's value
			// stays the string "AQ" (the wire path coerces through the
			// bound definition; the metadata path must match it).
			if got, want := d["b"], any("AQ"); !reflect.DeepEqual(got, want) {
				t.Errorf("f3 Default[b] = %T %v, want the bound definition's string coercion %q", got, got, want)
			}
		})
	}
}

// The one deliberate exception to the binding-kind gate: the SchemaNode
// metadata walker surfaces stray container keys AS-WRITTEN on the
// matching structural field (and keeps them out of Props) — a read-only
// surfacing duty with no registration or mutation. This pin locks the
// asymmetry so a uniformity change that gates the metadata walker too
// fails here instead of silently dropping the surfacing.
func TestRegression_MetadataStrayKeySurfacedAsWritten(t *testing.T) {
	t.Parallel()
	t.Run("items_ref", func(t *testing.T) {
		s := MustParse(`{"type":"int","items":"long"}`)
		root := s.Root()
		if root.Items == nil || root.Items.Type != "long" {
			t.Fatalf("stray items not surfaced as written: %+v", root.Items)
		}
		if _, ok := root.Props["items"]; ok {
			t.Errorf("stray items leaked into Props")
		}
	})
	t.Run("items_def", func(t *testing.T) {
		s := MustParse(cacheStrayCarrier("int", "items", cacheStrayGDef("int")))
		root := s.Root()
		if root.Items == nil || root.Items.Type != "record" || root.Items.Name != "n.G" || len(root.Items.Fields) != 1 {
			t.Fatalf("stray items definition not surfaced as written: %+v", root.Items)
		}
	})
	t.Run("values_ref", func(t *testing.T) {
		s := MustParse(`{"type":"string","values":"long"}`)
		root := s.Root()
		if root.Values == nil || root.Values.Type != "long" {
			t.Fatalf("stray values not surfaced as written: %+v", root.Values)
		}
	})
	t.Run("fields_def", func(t *testing.T) {
		s := MustParse(cacheStrayCarrier("int", "fields", cacheStrayGDef("int")))
		root := s.Root()
		if len(root.Fields) != 1 || root.Fields[0].Name != "f" || root.Fields[0].Type.Name != "n.G" {
			t.Fatalf("stray fields not surfaced as written: %+v", root.Fields)
		}
	})
}

// TestMatrix_CacheStrayStructuralKey crosses the stray-key gate's full
// domain: carrier kind × stray key × definition relation, each cell
// asserting every metadata surface against a stray-free control plus the
// wire verdict. Carriers split three ways by parse posture:
//
//   - primitive carriers (int, string) accept the stray as inert — the
//     gate cells;
//   - fixed/enum reject a foreign structural key outright ("has schema
//     for other types") — those cells pin the reject that keeps such
//     carriers structurally un-poisonable;
//   - array/map/record BIND the key — control cells proving the gate
//     does not block genuine definitions.
//
// Relations for the gate cells: the conflicting stray body parsed before
// and after the real definition (the store is first-wins, so order is the
// axis that decided the winner pre-gate), a self-referencing stray body
// (the definition-shaped value with a reference back to its own name),
// and a diamond (two independent definitions sharing the real n.G, spliced
// into one referencing schema — the first-define-then-reference rewrite
// must key off parser-bound definitions only).
func TestMatrix_CacheStrayStructuralKey(t *testing.T) {
	t.Parallel()
	accept := []map[string]any{{"h": "x"}}
	reject := []map[string]any{{"g": int32(7)}}
	ref := `{"type":"array","items":"n.G"}`

	strayBodies := map[string]string{
		"conflicting": cacheStrayGDef("int"),
		"recursive":   `{"type":"record","name":"n.G","fields":[{"name":"s","type":["null","n.G"]}]}`,
	}

	for _, carrier := range []string{"int", "string"} {
		for _, key := range []string{"items", "values", "fields"} {
			for bodyName, body := range strayBodies {
				for _, order := range []string{"stray_first", "real_first"} {
					name := fmt.Sprintf("%s_%s_%s_%s", carrier, key, bodyName, order)
					t.Run(name, func(t *testing.T) {
						seq := []string{cacheStrayCarrier(carrier, key, body), cacheStrayRealGDef, ref}
						if order == "real_first" {
							seq[0], seq[1] = seq[1], seq[0]
						}
						s := cacheParseSeq(t, seq...)
						control := cacheParseSeq(t, cacheStrayRealGDef, ref)
						if _, err := s.Encode(accept); err != nil {
							t.Fatalf("encode of the bound-definition value: %v", err)
						}
						if _, err := s.Encode(reject); err == nil {
							t.Fatalf("encode of the stray-shaped value unexpectedly accepted")
						}
						assertCacheSurfacesEqual(t, cacheSurfaces(t, s, accept), cacheSurfaces(t, control, accept))
					})
				}
			}

			t.Run(fmt.Sprintf("%s_%s_diamond", carrier, key), func(t *testing.T) {
				defL := `{"type":"record","name":"n.L","fields":[{"name":"g","type":"n.G"}]}`
				defR := `{"type":"record","name":"n.R","fields":[{"name":"g","type":"n.G"}]}`
				follow := `{"type":"record","name":"n.F","fields":[{"name":"l","type":"n.L"},{"name":"r","type":"n.R"}]}`
				sample := map[string]any{
					"l": map[string]any{"g": map[string]any{"h": "x"}},
					"r": map[string]any{"g": map[string]any{"h": "y"}},
				}
				bad := map[string]any{
					"l": map[string]any{"g": map[string]any{"g": int32(7)}},
					"r": map[string]any{"g": map[string]any{"h": "y"}},
				}
				s := cacheParseSeq(t,
					cacheStrayCarrier(carrier, key, cacheStrayGDef("int")),
					cacheStrayRealGDef, defL, defR, follow,
				)
				control := cacheParseSeq(t, cacheStrayRealGDef, defL, defR, follow)
				if _, err := s.Encode(sample); err != nil {
					t.Fatalf("encode of the bound-definition value: %v", err)
				}
				if _, err := s.Encode(bad); err == nil {
					t.Fatalf("encode of the stray-shaped value unexpectedly accepted")
				}
				assertCacheSurfacesEqual(t, cacheSurfaces(t, s, sample), cacheSurfaces(t, control, sample))
			})
		}
	}

	// Foreign structural keys on fixed/enum reject at parse — these
	// carriers are structurally un-poisonable, and the reject is the
	// guard that keeps them so.
	for name, carrier := range map[string]string{
		"fixed": `{"type":"fixed","name":"Fx","size":4,`,
		"enum":  `{"type":"enum","name":"E","symbols":["A"],`,
	} {
		for _, key := range []string{"items", "values", "fields"} {
			t.Run(fmt.Sprintf("%s_%s_rejects", name, key), func(t *testing.T) {
				var text string
				if key == "fields" {
					text = carrier + `"fields":[{"name":"f","type":` + cacheStrayGDef("int") + `}]}`
				} else {
					text = carrier + `"` + key + `":` + cacheStrayGDef("int") + `}`
				}
				_, err := Parse(text)
				if err == nil || !strings.Contains(err.Error(), "has schema for other types") {
					t.Fatalf("foreign structural key on %s: got %v, want the structural-exclusivity reject", name, err)
				}
			})
		}
	}

	// Genuinely-binding carriers: a definition in a BOUND container key
	// registers and a cross-parse reference to it both resolves and
	// splices — the gate must not block the bound positions.
	for name, tc := range map[string]struct {
		def    string
		sample any
	}{
		"array_items":   {`{"type":"array","items":` + cacheStrayRealGDef + `}`, map[string]any{"g": map[string]any{"h": "x"}}},
		"map_values":    {`{"type":"map","values":` + cacheStrayRealGDef + `}`, map[string]any{"g": map[string]any{"h": "x"}}},
		"record_fields": {`{"type":"record","name":"n.Outer","fields":[{"name":"f","type":` + cacheStrayRealGDef + `}]}`, map[string]any{"g": map[string]any{"h": "x"}}},
	} {
		t.Run(name+"_binds", func(t *testing.T) {
			s := cacheParseSeq(t, tc.def, `{"type":"record","name":"n.U","fields":[{"name":"g","type":"n.G"}]}`)
			if _, err := s.Encode(tc.sample); err != nil {
				t.Fatalf("cross-parse reference to a bound-position definition: %v", err)
			}
			if !strings.Contains(s.String(), `"h"`) {
				t.Errorf("bound-position definition not spliced into the metadata text: %s", s.String())
			}
		})
	}
}

// The rebuild walker (SchemaNode.Schema) descends stray container keys to
// render them as-written, but its dedup consult must not treat those
// positions as SCHEMA positions: the wire parser registers nothing there,
// so a named definition inside a stray key can neither conflict with nor
// stand in for the real definition of the same fullname.
func TestRegression_RenderDedupIgnoresStrayDefinitions(t *testing.T) {
	t.Parallel()
	real := `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`
	conflicting := `{"type":"record","name":"R","fields":[{"name":"x","type":"string"}]}`
	carrier := func(def string) string {
		return `{"type":"int","foo":1,"items":` + def + `}`
	}
	t.Run("conflicting_body", func(t *testing.T) {
		s := MustParse(`{"type":"record","name":"Top","fields":[
			{"name":"a","type":` + carrier(conflicting) + `},
			{"name":"b","type":` + real + `}]}`)
		root := s.Root()
		if _, err := root.Schema(); err != nil {
			t.Errorf("rebuild failed for a wire-valid schema: %v", err)
		}
	})
	t.Run("identical_body", func(t *testing.T) {
		s := MustParse(`{"type":"record","name":"Top","fields":[
			{"name":"a","type":` + carrier(real) + `},
			{"name":"b","type":` + real + `}]}`)
		root := s.Root()
		if _, err := root.Schema(); err != nil {
			t.Errorf("rebuild failed for a wire-valid schema: %v", err)
		}
	})
	t.Run("stray_after_real", func(t *testing.T) {
		s := MustParse(`{"type":"record","name":"Top","fields":[
			{"name":"b","type":` + real + `},
			{"name":"a","type":` + carrier(real) + `}]}`)
		root := s.Root()
		rb, err := root.Schema()
		if err != nil {
			t.Fatalf("rebuild: %v", err)
		}
		if strings.Contains(rb.String(), `"items":"R"`) {
			t.Errorf("stray definition rewritten to a reference: %s", rb.String())
		}
	})
}

// A field element inside a stray "fields" key surfaces as-written even
// when it has no "type" key: the record build (which requires a field
// type) never runs for stray positions, so such elements are parseable
// and their written attributes must appear on the surfaced SchemaField —
// never a fabricated zero element.
func TestRegression_StrayFieldElementSurfacedAsWritten(t *testing.T) {
	t.Parallel()
	s := MustParse(`{"type":"record","name":"Top","fields":[{"name":"a","type":
		{"type":"int","fields":[{"name":"x","doc":"d","myprop":1}]}}]}`)
	fs := s.Root().Fields[0].Type.Fields
	if len(fs) != 1 {
		t.Fatalf("stray fields arity: got %d, want 1", len(fs))
	}
	if fs[0].Name != "x" || fs[0].Doc != "d" {
		t.Errorf("stray field element not surfaced as written: %+v", fs[0])
	}
	if got, ok := fs[0].Props["myprop"]; !ok || got != int64(1) {
		t.Errorf("stray field element props: got %v, want myprop=1", fs[0].Props)
	}
	if fs[0].Type.Type != "" {
		t.Errorf("typeless stray element must surface a zero Type, got %q", fs[0].Type.Type)
	}
}

// A stray container key survives SchemaNode.Schema() regardless of
// whether the carrier also has custom props or a logical type: surfacing
// is as-written, so the rebuilt schema must keep the stray on every
// carrier shape, and a second generation must be stable.
func TestRegression_StrayKeySurvivesSchemaRebuild(t *testing.T) {
	t.Parallel()
	for _, carrier := range []string{
		`{"type":"int","items":"long"}`,
		`{"type":"int","foo":1,"items":"long"}`,
	} {
		s := MustParse(`{"type":"record","name":"Top","fields":[{"name":"a","type":` + carrier + `}]}`)
		root := s.Root()
		rb, err := root.Schema()
		if err != nil {
			t.Fatalf("%s: rebuild: %v", carrier, err)
		}
		if !strings.Contains(rb.String(), `"items":"long"`) {
			t.Errorf("%s: stray dropped by rebuild: %s", carrier, rb.String())
			continue
		}
		rbRoot := rb.Root()
		rb2, err := rbRoot.Schema()
		if err != nil {
			t.Fatalf("%s: second-generation rebuild: %v", carrier, err)
		}
		if rb.String() != rb2.String() {
			t.Errorf("%s: rebuild not stable across generations:\n gen1: %s\n gen2: %s", carrier, rb.String(), rb2.String())
		}
	}
}

// A reserved-key body that does not parse as the key's schema shape is
// inert on a kind that does not bind the key: it cannot define, scope, or
// bind anything, so it surfaces verbatim in Props — the same treatment
// every non-reserved key gets. (Java skips reserved keys wholesale on
// non-binding kinds — Schema.java's SCHEMA_RESERVED set — and fastavro
// ignores them; rejecting was a twmb-only strictness.) Schema-shaped
// bodies keep the structural-field surfacing.
func TestRegression_MalformedStrayBodyAcceptedAsProps(t *testing.T) {
	t.Parallel()
	cases := []struct {
		carrier string
		key     string
		want    any
	}{
		{`{"type":"int","items":3}`, "items", int64(3)},
		{`{"type":"int","values":true}`, "values", true},
		{`{"type":"int","fields":[3]}`, "fields", nil},
		{`{"type":"int","fields":3}`, "fields", int64(3)},
		{`{"type":"int","symbols":3}`, "symbols", int64(3)},
		{`{"type":"int","size":"x"}`, "size", "x"},
		{`{"type":"int","name":3}`, "name", int64(3)},
		{`{"type":"int","namespace":3}`, "namespace", int64(3)},
		{`{"type":"int","aliases":3}`, "aliases", int64(3)},
		{`{"type":"int","precision":"abc"}`, "precision", "abc"},
		{`{"type":"int","scale":"abc"}`, "scale", "abc"},
		{`{"type":"string","items":{"type":3}}`, "items", nil},
	}
	for _, c := range cases {
		s, err := Parse(`{"type":"record","name":"Top","fields":[{"name":"a","type":` + c.carrier + `}]}`)
		if err != nil {
			t.Errorf("%s: rejected: %v", c.carrier, err)
			continue
		}
		n := s.Root().Fields[0].Type
		got, ok := n.Props[c.key]
		if !ok {
			t.Errorf("%s: stray %q not surfaced in Props: %v", c.carrier, c.key, n.Props)
			continue
		}
		if c.want != nil && !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: Props[%q] = %v (%T), want %v", c.carrier, c.key, got, got, c.want)
		}
		var enc []byte
		enc, err = s.Encode(map[string]any{"a": int32(7)})
		if c.carrier[9:15] == "string" {
			enc, err = s.Encode(map[string]any{"a": "v"})
		}
		if err != nil {
			t.Errorf("%s: encode: %v", c.carrier, err)
			continue
		}
		var out map[string]any
		if _, err := s.Decode(enc, &out); err != nil {
			t.Errorf("%s: decode: %v", c.carrier, err)
		}
	}
}

// A malformed reserved-key body on a wrapped named REFERENCE rides as a
// prop too: the reference guard consults only successfully parsed
// structural attributes, and a body that cannot parse as the key's shape
// cannot be an attempt to define a type.
func TestRegression_MalformedStrayBodyOnWrappedRef(t *testing.T) {
	t.Parallel()
	s, err := Parse(`{"type":"record","name":"Top","fields":[
		{"name":"b","type":{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}},
		{"name":"a","type":{"type":"R","items":3}}]}`)
	if err != nil {
		t.Fatalf("rejected: %v", err)
	}
	n := s.Root().Fields[1].Type
	if n.Type != "R" {
		t.Fatalf("reference not preserved: %+v", n)
	}
	if got, ok := n.Props["items"]; !ok || got != int64(3) {
		t.Errorf("malformed stray on reference not in Props: %v", n.Props)
	}
}

// The adjudicated reject boundaries do not loosen with the malformed-body
// acceptance: a BINDING kind still shape-validates its own key; a
// container kind still rejects another kind's schema-shaped defining key;
// a schema-shaped stray name on an unnamed container still rejects; and a
// schema-shaped structural key on a wrapped reference still rejects.
func TestRegression_StrayShapeRejectBoundaries(t *testing.T) {
	t.Parallel()
	for _, c := range []struct{ schema, wantErr string }{
		{`{"type":"array","items":3}`, "invalid schema"},
		{`{"type":"map","values":true}`, "invalid schema"},
		{`{"type":"record","name":"N","fields":3}`, `"fields" must be a JSON array`},
		{`{"type":"enum","name":"N","symbols":3}`, "array"},
		{`{"type":"fixed","name":"N","size":"x"}`, ""},
		{`{"type":"array","items":"int","fields":[{"name":"x","type":"int"}]}`, "has schema for other types"},
		{`{"type":"array","items":"int","name":"x"}`, ""},
	} {
		_, err := Parse(`{"type":"record","name":"Top","fields":[{"name":"a","type":` + c.schema + `}]}`)
		if err == nil {
			t.Errorf("%s: accepted, want reject", c.schema)
			continue
		}
		if c.wantErr != "" && !strings.Contains(err.Error(), c.wantErr) {
			t.Errorf("%s: error %q does not mention %q", c.schema, err, c.wantErr)
		}
	}
	if _, err := Parse(`{"type":"record","name":"Top","fields":[
		{"name":"b","type":{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}},
		{"name":"a","type":{"type":"R","items":"long"}}]}`); err == nil {
		t.Errorf("schema-shaped structural key on a wrapped reference accepted, want reject")
	}
}

// cacheStrayCarrierProps is cacheStrayCarrier with an extra custom
// property on the carrier object — the carrier shape that forces the
// rebuild's object render (a bare primitive emits as its type string).
func cacheStrayCarrierProps(kind, key, payload string) string {
	if key == "fields" {
		return `{"type":"` + kind + `","foo":1,"fields":[{"name":"f","type":` + payload + `}]}`
	}
	return `{"type":"` + kind + `","foo":1,"` + key + `":` + payload + `}`
}

// TestMatrix_CacheStrayRebuildSurface crosses carrier kind × stray key ×
// definition relation × carrier props × order for a SINGLE parse holding
// both a stray-planted definition and the real definition of the same
// fullname: the metadata rebuild must succeed (the dedup consult skips
// stray positions), preserve the wire verdicts, and be stable across a
// second generation — independent of whether the carrier's props force
// the object render.
func TestMatrix_CacheStrayRebuildSurface(t *testing.T) {
	t.Parallel()
	strayBodies := map[string]string{
		"conflicting": cacheStrayGDef("int"),
		"recursive":   `{"type":"record","name":"n.G","fields":[{"name":"s","type":["null","n.G"]}]}`,
	}
	// A substring of each body's rebuilt image, proving the stray content
	// survived the rebuild verbatim rather than being dropped or
	// rewritten to a reference.
	strayMarkers := map[string]string{
		"conflicting": `"name":"g"`,
		"recursive":   `"name":"s"`,
	}
	carrierValue := map[string]any{"int": int32(7), "string": "v"}
	for _, carrier := range []string{"int", "string"} {
		for _, key := range []string{"items", "values", "fields"} {
			for bodyName, body := range strayBodies {
				for _, props := range []string{"bare", "withprop"} {
					for _, order := range []string{"stray_first", "real_first"} {
						name := fmt.Sprintf("%s_%s_%s_%s_%s", carrier, key, bodyName, props, order)
						t.Run(name, func(t *testing.T) {
							strayType := cacheStrayCarrier(carrier, key, body)
							if props == "withprop" {
								strayType = cacheStrayCarrierProps(carrier, key, body)
							}
							fa := `{"name":"a","type":` + strayType + `}`
							fb := `{"name":"b","type":` + cacheStrayRealGDef + `}`
							if order == "real_first" {
								fa, fb = fb, fa
							}
							s := MustParse(`{"type":"record","name":"Top","fields":[` + fa + `,` + fb + `]}`)
							accept := map[string]any{"a": carrierValue[carrier], "b": map[string]any{"h": "x"}}
							reject := map[string]any{"a": carrierValue[carrier], "b": map[string]any{"g": int32(9)}}
							if _, err := s.Encode(accept); err != nil {
								t.Fatalf("encode of the bound-definition value: %v", err)
							}
							if _, err := s.Encode(reject); err == nil {
								t.Fatalf("encode of the stray-shaped value unexpectedly accepted")
							}
							root := s.Root()
							rb, err := root.Schema()
							if err != nil {
								t.Fatalf("rebuild: %v", err)
							}
							if _, err := rb.Encode(accept); err != nil {
								t.Errorf("rebuilt schema rejects the bound-definition value: %v", err)
							}
							if _, err := rb.Encode(reject); err == nil {
								t.Errorf("rebuilt schema accepts the stray-shaped value")
							}
							if !strings.Contains(rb.String(), strayMarkers[bodyName]) {
								t.Errorf("stray body did not survive the rebuild: %s", rb.String())
							}
							if props == "withprop" && !strings.Contains(rb.String(), `"foo":1`) {
								t.Errorf("carrier props did not survive the rebuild: %s", rb.String())
							}
							rbRoot := rb.Root()
							rb2, err := rbRoot.Schema()
							if err != nil {
								t.Fatalf("second-generation rebuild: %v", err)
							}
							if rb.String() != rb2.String() {
								t.Errorf("rebuild unstable across generations:\n gen1 %s\n gen2 %s", rb.String(), rb2.String())
							}
						})
					}
				}
			}
		}
	}
}

// Defaults inside a stray-surfaced body get the same normalization every
// SchemaField.Default gets (string→float for float kinds, codepoint
// string→[]byte for bytes) — the default pipeline is uniform over the
// surfaced tree, and the render's inverse fixups keep the re-emitted
// image equal to the written one. The name table consulted for
// name-referenced defaults is built from BOUND positions only, so the
// normalization inside a stray can never register or resolve a name.
func TestRegression_StrayBodyDefaultNormalization(t *testing.T) {
	t.Parallel()
	s := MustParse(`{"type":"record","name":"Top","fields":[{"name":"a","type":
		{"type":"int","items":{"type":"record","name":"SB","fields":[
			{"name":"f","type":"bytes","default":"abc"},
			{"name":"g","type":"double","default":"1.5"}]}}}]}`)
	stray := s.Root().Fields[0].Type.Items
	if stray == nil {
		t.Fatalf("stray items not surfaced")
	}
	if got, ok := stray.Fields[0].Default.([]byte); !ok || string(got) != "abc" {
		t.Errorf("bytes default in a stray body: got %T %v, want []byte(\"abc\")", stray.Fields[0].Default, stray.Fields[0].Default)
	}
	if got, ok := stray.Fields[1].Default.(float64); !ok || got != 1.5 {
		t.Errorf("double default in a stray body: got %T %v, want float64(1.5)", stray.Fields[1].Default, stray.Fields[1].Default)
	}
	root := s.Root()
	rb, err := root.Schema()
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if !strings.Contains(rb.String(), `"default":"abc"`) || !strings.Contains(rb.String(), `"default":1.5`) {
		t.Errorf("stray-body defaults did not re-emit their written images: %s", rb.String())
	}
}

// Branches on a non-union kind have no JSON spelling the parser could
// bind — only a hand-built tree can carry them — and every consumer
// treats them as inert: the render never descends them (a bare primitive
// emits its type string; the object render has no branches arm outside
// the union case), so the rebuild neither emits them, registers names
// from them, nor conflicts on them.
func TestRegression_NonUnionBranchesInertInRebuild(t *testing.T) {
	t.Parallel()
	branch := SchemaNode{Type: "record", Name: "R",
		Fields: []SchemaField{{Name: "x", Type: SchemaNode{Type: "string"}}}}
	node := SchemaNode{Type: "record", Name: "Top", Fields: []SchemaField{
		{Name: "a", Type: SchemaNode{Type: "int", Props: map[string]any{"p": int64(1)}, Branches: []SchemaNode{branch}}},
		{Name: "b", Type: SchemaNode{Type: "record", Name: "R",
			Fields: []SchemaField{{Name: "x", Type: SchemaNode{Type: "int"}}}}},
	}}
	s, err := node.Schema()
	if err != nil {
		t.Fatalf("rebuild with non-union Branches: %v", err)
	}
	if strings.Contains(s.String(), `"string"`) {
		t.Errorf("non-union Branches leaked into the rebuild: %s", s.String())
	}
	bare := SchemaNode{Type: "int", Branches: []SchemaNode{branch}}
	s2, err := bare.Schema()
	if err != nil {
		t.Fatalf("bare-primitive rebuild with Branches: %v", err)
	}
	if s2.String() != `"int"` {
		t.Errorf("bare primitive with non-union Branches: got %s, want \"int\"", s2.String())
	}
}
