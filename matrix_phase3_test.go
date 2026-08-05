package avro_test

import (
	"bytes"
	"fmt"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// Phase 3: defaults pipeline per kind × positions, same-token-class tagged
// unions, SchemaCache cross-parse references, and option axes.
// ---------------------------------------------------------------------------

// Per-kind defaulted fields: parse → JSON fill → binary auto-fill, and the
// two fills must land on the same wire as explicitly encoding the filled
// value. Default literals use the field type's JSON encoding per the spec
// (underlying form for logical types).
func TestMatrix_DefaultsPerKind(t *testing.T) {
	cases := []struct {
		label      string
		fieldType  string
		defaultLit string
	}{
		{"null", `"null"`, `null`},
		{"boolean", `"boolean"`, `true`},
		{"int", `"int"`, `7`},
		{"int-neg", `"int"`, `-2147483648`},
		{"long", `"long"`, `9007199254740993`},
		{"float", `"float"`, `1.5`},
		{"double", `"double"`, `-2.25`},
		{"string", `"string"`, `"dflt"`},
		{"string-empty", `"string"`, `""`},
		{"bytes", `"bytes"`, `"\u0001\u00ff"`},
		{"bytes-empty", `"bytes"`, `""`},
		{"enum", `{"type":"enum","name":"DE","symbols":["A","B"]}`, `"B"`},
		{"fixed1", `{"type":"fixed","name":"DF1","size":1}`, `"\u00ab"`},
		{"fixed0", `{"type":"fixed","name":"DF0","size":0}`, `""`},
		{"date", `{"type":"int","logicalType":"date"}`, `19723`},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`, `3600000`},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, `1717243496789`},
		{"decimal", `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`, `"\u00d2"`},
		{"nullunion", `["null","int"]`, `null`},
		{"union-int-first", `["int","string"]`, `42`},
		{"array", `{"type":"array","items":"int"}`, `[1,2]`},
		{"array-empty", `{"type":"array","items":"int"}`, `[]`},
		{"map", `{"type":"map","values":"string"}`, `{"k":"v"}`},
		{"map-empty", `{"type":"map","values":"string"}`, `{}`},
		{"record", `{"type":"record","name":"DR","fields":[{"name":"i","type":"int"}]}`, `{"i":3}`},
		{"empty-record", `{"type":"record","name":"DER","fields":[]}`, `{}`},
	}
	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			schema := fmt.Sprintf(`{"type":"record","name":"W","fields":[
				{"name":"pre","type":"string"},
				{"name":"f","type":%s,"default":%s}]}`, c.fieldType, c.defaultLit)
			s, err := avro.Parse(schema)
			if err != nil {
				t.Fatalf("Parse: %v\nschema: %s", err, schema)
			}
			// JSON fill: absent field materializes the default.
			var filled map[string]any
			if err := s.DecodeJSON([]byte(`{"pre":"p"}`), &filled); err != nil {
				t.Fatalf("DecodeJSON fill: %v", err)
			}
			// Binary auto-fill on encode of a map missing the field.
			wFill, err := s.AppendEncode(nil, map[string]any{"pre": "p"})
			if err != nil {
				t.Fatalf("binary auto-fill encode: %v", err)
			}
			// Explicitly encoding the JSON-filled value must give the
			// same wire bytes.
			wExpl, err := s.AppendEncode(nil, filled)
			if err != nil {
				t.Fatalf("encode filled value %#v: %v", filled, err)
			}
			if !bytes.Equal(wFill, wExpl) {
				t.Fatalf("auto-fill wire differs from filled-value wire:\n fill=%x\n expl=%x\nfilled: %#v", wFill, wExpl, filled)
			}
			// Decode the auto-filled wire and re-encode: stable.
			var back any
			if _, err := s.Decode(wFill, &back); err != nil {
				t.Fatalf("decode auto-filled wire: %v", err)
			}
			w2, err := s.AppendEncode(nil, back)
			if err != nil || !bytes.Equal(w2, wFill) {
				t.Fatalf("re-encode of auto-filled wire differs: err=%v\n w=%x\n w2=%x", err, wFill, w2)
			}
			// The metadata Default round-trips through Root().Schema().
			root := s.Root()
			rebuilt, err := root.Schema()
			if err != nil {
				t.Fatalf("Root().Schema(): %v", err)
			}
			wReb, err := rebuilt.AppendEncode(nil, map[string]any{"pre": "p"})
			if err != nil || !bytes.Equal(wReb, wFill) {
				t.Fatalf("rebuilt-schema auto-fill differs: err=%v\n w=%x\n reb=%x\nrebuilt: %s", err, wFill, wReb, rebuilt.String())
			}
		})
	}
}

// Same-token-class union pairs are information-preserving only in TAGGED
// form (documented untagged first-match loss): the full core must hold
// with TaggedUnions on both wires.
func TestMatrix_TaggedSameClassUnions(t *testing.T) {
	cases := []struct {
		label  string
		schema string
		values []any
	}{
		{"int-long", `["int","long"]`,
			[]any{map[string]any{"int": int32(7)}, map[string]any{"long": int64(7)}}},
		{"float-double", `["float","double"]`,
			[]any{map[string]any{"float": float32(1.5)}, map[string]any{"double": float64(1.5)}}},
		{"string-bytes", `["string","bytes"]`,
			[]any{map[string]any{"string": "x"}, map[string]any{"bytes": []byte("x")}}},
		{"two-records", `[{"type":"record","name":"R1","fields":[{"name":"a","type":"int"}]},{"type":"record","name":"R2","fields":[{"name":"a","type":"int"}]}]`,
			[]any{map[string]any{"R1": map[string]any{"a": int32(1)}}, map[string]any{"R2": map[string]any{"a": int32(2)}}}},
		{"enum-string", `[{"type":"enum","name":"TE","symbols":["A"]},"string"]`,
			[]any{map[string]any{"TE": "A"}, map[string]any{"string": "A"}}},
		{"fixed-bytes", `[{"type":"fixed","name":"TF","size":2},"bytes"]`,
			[]any{map[string]any{"TF": []byte{1, 2}}, map[string]any{"bytes": []byte{1, 2}}}},
		{"map-record", `[{"type":"map","values":"int"},{"type":"record","name":"MR","fields":[{"name":"a","type":"int"}]}]`,
			[]any{map[string]any{"map": map[string]any{"k": int32(1)}}, map[string]any{"MR": map[string]any{"a": int32(1)}}}},
	}
	for _, c := range cases {
		for vi, v := range c.values {
			t.Run(fmt.Sprintf("%s/v%d", c.label, vi), func(t *testing.T) {
				runCore(t, c.schema, v, avro.TaggedUnions())
			})
		}
	}
}

// SchemaCache: a named type defined by one Parse and referenced by name from
// a second Parse must behave identically to the inline definition, across
// both wires, rebuild, and resolve.
func TestMatrix_SchemaCacheCrossRef(t *testing.T) {
	defs := []struct {
		label  string
		def    string
		ref    string
		inline string
		value  any
	}{
		{"record",
			`{"type":"record","name":"CR","fields":[{"name":"a","type":"int"},{"name":"b","type":["null","string"],"default":null}]}`,
			`{"type":"array","items":"CR"}`,
			`{"type":"array","items":{"type":"record","name":"CR","fields":[{"name":"a","type":"int"},{"name":"b","type":["null","string"],"default":null}]}}`,
			[]any{map[string]any{"a": int32(1), "b": "x"}, map[string]any{"a": int32(2), "b": nil}}},
		{"enum",
			`{"type":"enum","name":"CE","symbols":["X","Y"]}`,
			`{"type":"map","values":"CE"}`,
			`{"type":"map","values":{"type":"enum","name":"CE","symbols":["X","Y"]}}`,
			map[string]any{"k": "Y"}},
		{"fixed0",
			`{"type":"fixed","name":"CF0","size":0}`,
			`["null","CF0"]`,
			`["null",{"type":"fixed","name":"CF0","size":0}]`,
			[]byte{}},
		{"recursive",
			`{"type":"record","name":"CN","fields":[{"name":"v","type":"int"},{"name":"next","type":["null","CN"],"default":null}]}`,
			`{"type":"array","items":"CN"}`,
			`{"type":"array","items":{"type":"record","name":"CN","fields":[{"name":"v","type":"int"},{"name":"next","type":["null","CN"],"default":null}]}}`,
			[]any{map[string]any{"v": int32(1), "next": map[string]any{"v": int32(2), "next": nil}}}},
	}
	for _, d := range defs {
		t.Run(d.label, func(t *testing.T) {
			var cache avro.SchemaCache
			if _, err := cache.Parse(d.def); err != nil {
				t.Fatalf("cache.Parse(def): %v", err)
			}
			viaCache, err := cache.Parse(d.ref)
			if err != nil {
				t.Fatalf("cache.Parse(ref): %v", err)
			}
			inline := avro.MustParse(d.inline)

			wC, err := viaCache.AppendEncode(nil, d.value)
			if err != nil {
				t.Fatalf("cache-ref encode: %v", err)
			}
			wI, err := inline.AppendEncode(nil, d.value)
			if err != nil {
				t.Fatalf("inline encode: %v", err)
			}
			if !bytes.Equal(wC, wI) {
				t.Fatalf("cache-ref wire differs from inline:\n c=%x\n i=%x", wC, wI)
			}
			var aC, aI any
			if _, err := viaCache.Decode(wC, &aC); err != nil {
				t.Fatalf("cache-ref decode: %v", err)
			}
			if _, err := inline.Decode(wI, &aI); err != nil {
				t.Fatalf("inline decode: %v", err)
			}
			if !matEqual(aC, aI) {
				t.Fatalf("decoded values differ:\n c=%#v\n i=%#v", aC, aI)
			}
			// JSON parity.
			jC, err := viaCache.AppendEncodeJSON(nil, aC)
			if err != nil {
				t.Fatalf("cache-ref encodeJSON: %v", err)
			}
			jI, err := inline.AppendEncodeJSON(nil, aI)
			if err != nil || !bytes.Equal(jC, jI) {
				t.Fatalf("JSON differs: err=%v\n c=%s\n i=%s", err, jC, jI)
			}
			// Resolve cache-ref ↔ inline (identical structure).
			if _, err := avro.Resolve(viaCache, inline); err != nil {
				t.Fatalf("Resolve(cache→inline): %v", err)
			}
			if _, err := avro.Resolve(inline, viaCache); err != nil {
				t.Fatalf("Resolve(inline→cache): %v", err)
			}
			// Rebuild parity: the cache-referenced schema's metadata forms must
			// be self-contained and identical to the inline schema's — the
			// canonical form (hence the Rabin fingerprint, the cross-language /
			// single-object-encoding identity) byte-for-byte, the canonical must
			// re-parse, and Root().Schema() must rebuild. The cache stores only
			// the resolved node, so without inlining the inherited definition
			// these forms keep a dangling bare reference.
			if !bytes.Equal(viaCache.Canonical(), inline.Canonical()) {
				t.Fatalf("canonical differs:\n c=%s\n i=%s", viaCache.Canonical(), inline.Canonical())
			}
			if !bytes.Equal(viaCache.Fingerprint(avro.NewRabin()), inline.Fingerprint(avro.NewRabin())) {
				t.Fatalf("fingerprint differs (cache-ref not self-contained)")
			}
			if _, err := avro.Parse(string(viaCache.Canonical())); err != nil {
				t.Fatalf("cache-ref canonical not self-contained: %v", err)
			}
			root := viaCache.Root()
			if _, err := root.Schema(); err != nil {
				t.Fatalf("Root().Schema() rebuild failed: %v", err)
			}
		})
	}
}

// Option axes over representative fragments: LinkedinFloats float forms and
// TagLogicalTypes envelopes must round-trip within their own convention.
func TestMatrix_OptionAxes(t *testing.T) {
	t.Run("linkedin-floats", func(t *testing.T) {
		s := avro.MustParse(`{"type":"array","items":"double"}`)
		vin := []any{1.5, -2.25}
		runCore(t, `{"type":"array","items":"double"}`, vin, avro.LinkedinFloats())
		// Non-finite specials under the goavro convention: NaN→null is
		// not value-preserving by design, so only the finite path runs
		// through runCore; the specials get a one-way encode check.
		j, err := s.AppendEncodeJSON(nil, []any{math.Inf(1), math.Inf(-1)}, avro.LinkedinFloats())
		_ = j
		if err != nil {
			t.Fatalf("encode specials: %v", err)
		}
	})
	t.Run("tag-logical-types", func(t *testing.T) {
		schema := `["null",{"type":"long","logicalType":"timestamp-millis"}]`
		v := time.UnixMilli(1717243496789).UTC()
		runCore(t, schema, v, avro.TaggedUnions(), avro.TagLogicalTypes())
	})
	t.Run("tag-logical-named-fixed", func(t *testing.T) {
		schema := `["null",{"type":"fixed","name":"NU","size":16,"logicalType":"uuid"}]`
		v := "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
		runCore(t, schema, v, avro.TaggedUnions(), avro.TagLogicalTypes())
	})
}

// Deep × wide stress at the boundary of interesting structure: a 5-level
// alternating record/array/map/union tower over every leaf kind.
func TestMatrix_FiveLevelTower(t *testing.T) {
	leaves := []struct {
		label  string
		schema string
		value  any
	}{
		{"int", `"int"`, int32(9)},
		{"string", `"string"`, "s"},
		{"bytes", `"bytes"`, []byte{1}},
		{"decimal", `{"type":"bytes","logicalType":"decimal","precision":4,"scale":1}`, big.NewRat(15, 10)},
		{"fixed0", `{"type":"fixed","name":"TF0","size":0}`, []byte{}},
		{"enum", `{"type":"enum","name":"TE5","symbols":["A","B"]}`, "B"},
	}
	for _, leaf := range leaves {
		t.Run(leaf.label, func(t *testing.T) {
			schema := fmt.Sprintf(`{"type":"record","name":"L1","fields":[{"name":"a","type":
				{"type":"array","items":
					{"type":"map","values":
						["null",{"type":"record","name":"L4","fields":[
							{"name":"leaf","type":%s},
							{"name":"sib","type":"long"}]}]}}}]}`, leaf.schema)
			value := map[string]any{"a": []any{
				map[string]any{"k": map[string]any{"leaf": leaf.value, "sib": int64(5)}},
				map[string]any{"e": nil},
				map[string]any{},
			}}
			runCore(t, schema, value)
		})
	}
}
