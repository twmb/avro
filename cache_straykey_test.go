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
		root:      s.Root(),
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
