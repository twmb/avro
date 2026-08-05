package avro_test

import (
	"fmt"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// Generative int→float promotion-precision net.
//
// Documented rule (BUG_AUDIT "Precision: the READER schema is the contract"):
// when the writer is int/long and the reader is float/double, the value is
// converted through the reader's float width — int/long → FLOAT rounds at the
// float32 mantissa (24 bits), long → DOUBLE at the float64 mantissa (53 bits)
// — and that rounding is PRESERVED when decoding into an any/float64 target.
// promoteIntFloatMantissa does `float64(float32(n))` for a 32-bit-wire reader.
//
// The existing TestMatrix_PromotionPairsByContext misses a bug in that
// rounding two ways: its values are small (exactly float-representable, so
// float64(float32(n)) == float64(n)), and it re-encodes the promoted value
// against the reader's FLOAT wire (which rounds both sides identically,
// hiding a wrong intermediate). A per-site neuter confirmed
// `float64(float32(n))` → `float64(n)` is caught only by the hand-written
// TestResolutionPromotionMatrix, not by the generative nets.
//
// This net drives values ACROSS the mantissa boundary and asserts the
// decoded-into-any VALUE (where the rounding is observable), across
// positions and through both the natural resolved path.
// ---------------------------------------------------------------------------

func TestMatrix_PromotionPrecision(t *testing.T) {
	const f32boundary = 1 << 24 // 16777216; +1 is not exactly float32-representable
	const f64boundary = 1 << 53 // 9007199254740992; +1 is not exactly float64-representable

	cases := []struct {
		label        string
		wKind, rKind string
		wVal         any
		want         float64 // decoded into a float64 target (reveals the intermediate rounding)
	}{
		// Decoded into a float64 target: the reader-width rounding of the
		// INTERMEDIATE is observable (an any target would be float32 for a
		// float reader and re-round, hiding it).
		// int/long → float: 2^24+1 rounds at the float32 mantissa -> 2^24.
		{"int→float@mantissa", "int", "float", int32(f32boundary + 1), float64(float32(f32boundary + 1))},
		{"long→float@mantissa", "long", "float", int64(f32boundary + 1), float64(float32(f32boundary + 1))},
		// long → double: 2^53+1 rounds at the float64 mantissa.
		{"long→double@mantissa", "long", "double", int64(f64boundary + 1), float64(f64boundary + 1)},
		// int → double is exact (every int32 fits the float64 mantissa).
		{"int→double-exact", "int", "double", int32(f32boundary + 1), float64(f32boundary + 1)},
	}

	positions := []struct {
		label  string
		wrap   func(leaf string) string
		val    func(v any) any
		target func() any // ptr to a tree of float64 leaves
		leaf   func(tgt any) float64
	}{
		{"top", func(l string) string { return l },
			func(v any) any { return v },
			func() any { return new(float64) },
			func(t any) float64 { return *(t.(*float64)) }},
		{"field", func(l string) string {
			return fmt.Sprintf(`{"type":"record","name":"PP","fields":[{"name":"f","type":%s}]}`, l)
		}, func(v any) any { return map[string]any{"f": v} },
			func() any {
				return &struct {
					F float64 `avro:"f"`
				}{}
			},
			func(t any) float64 {
				return t.(*struct {
					F float64 `avro:"f"`
				}).F
			}},
		{"array", func(l string) string { return fmt.Sprintf(`{"type":"array","items":%s}`, l) },
			func(v any) any { return []any{v} },
			func() any { return &[]float64{} },
			func(t any) float64 { return (*(t.(*[]float64)))[0] }},
	}

	for _, c := range cases {
		for _, pos := range positions {
			t.Run(c.label+"/"+pos.label, func(t *testing.T) {
				w := avro.MustParse(pos.wrap(fmt.Sprintf("%q", c.wKind)))
				r := avro.MustParse(pos.wrap(fmt.Sprintf("%q", c.rKind)))
				res, err := avro.Resolve(w, r)
				if err != nil {
					t.Fatalf("Resolve: %v", err)
				}
				wire, err := w.AppendEncode(nil, pos.val(c.wVal))
				if err != nil {
					t.Fatalf("writer encode: %v", err)
				}
				// Decode into any: the promoted value's PRECISION is
				// observable here (re-encoding against the reader's float
				// wire would re-round and hide a wrong intermediate).
				tgt := pos.target()
				if _, err := res.Decode(wire, tgt); err != nil {
					t.Fatalf("resolved decode: %v", err)
				}
				if leaf := pos.leaf(tgt); leaf != c.want {
					t.Fatalf("%s: promoted value %v, want %v (reader-width-rounded intermediate). A wrong mantissa conversion shows here.",
						c.label, leaf, c.want)
				}
			})
		}
	}
}
