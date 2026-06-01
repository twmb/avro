package avro_test

import (
	"math/big"
	"testing"

	"github.com/twmb/avro"
)

// Tier-2 decimal round-trip matrix (CORRECTNESS_PLAN.md decimal gap). Decimal
// is a recurring hot spot (scale derivation, source-precision, coercion). The
// generalized invariant: a value exactly representable at a schema's scale
// must survive Encode -> Decode unchanged, across bytes+decimal / fixed+decimal
// and a span of (precision, scale). Expectations are exact (*big.Rat.Cmp), so
// scale-rounding or unscaled-int corruption cannot hide. A float32 source arm
// pins that float32 inputs format with float32's shortest-decimal rule, not
// the float64 widening's IEEE noise (the regression that rejected
// float32(0.33) at scale 2).

func TestDecimalRoundTripMatrix(t *testing.T) {
	schemas := []struct {
		name      string
		json      string
		precision int
		scale     int
	}{
		{"bytes/p10s2", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, 10, 2},
		{"bytes/p18s4", `{"type":"bytes","logicalType":"decimal","precision":18,"scale":4}`, 18, 4},
		{"bytes/p4s0", `{"type":"bytes","logicalType":"decimal","precision":4,"scale":0}`, 4, 0},
		{"fixed/p8s6", `{"type":"fixed","name":"Dec16","size":16,"logicalType":"decimal","precision":8,"scale":6}`, 8, 6},
		{"fixed/p38s10", `{"type":"fixed","name":"Dec32","size":32,"logicalType":"decimal","precision":38,"scale":10}`, 38, 10},
	}

	for _, sc := range schemas {
		t.Run(sc.name, func(t *testing.T) {
			s := avro.MustParse(sc.json)
			den := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(sc.scale)), nil)

			// Unscaled integers whose digit count fits the precision, plus the
			// largest in-precision magnitude (all-nines) and its negation.
			maxUnscaled := new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(sc.precision)), nil), big.NewInt(1))
			unscaled := []*big.Int{
				big.NewInt(0), big.NewInt(1), big.NewInt(-1),
				big.NewInt(5), big.NewInt(-5), big.NewInt(99), big.NewInt(-99),
				new(big.Int).Set(maxUnscaled), new(big.Int).Neg(maxUnscaled),
			}
			for _, u := range unscaled {
				want := new(big.Rat).SetFrac(u, den) // value = u / 10^scale, exact at this scale
				wire, err := s.Encode(want)
				if err != nil {
					t.Fatalf("Encode(%v): %v", want, err)
				}
				var got *big.Rat
				if _, err := s.Decode(wire, &got); err != nil {
					t.Fatalf("Decode(%v wire): %v", want, err)
				}
				if got == nil || got.Cmp(want) != 0 {
					t.Errorf("unscaled %v: round-trip got %v, want %v", u, got, want)
				}
			}
		})
	}
}

// TestDecimalFloat32SourcePrecision pins that a float32 decimal input is
// formatted with float32's shortest-decimal rule, so values like 0.33 land on
// their scale exactly instead of being rejected for the float64-widening tail.
// Encode failing here is the regression signature (hardcoded float64 bitSize).
func TestDecimalFloat32SourcePrecision(t *testing.T) {
	s := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)
	cases := []struct {
		in   float32
		want *big.Rat
	}{
		{0.33, big.NewRat(33, 100)},
		{1.5, big.NewRat(150, 100)},
		{-12.34, big.NewRat(-1234, 100)},
		{0.07, big.NewRat(7, 100)},
		{99.99, big.NewRat(9999, 100)},
	}
	for _, c := range cases {
		wire, err := s.Encode(c.in)
		if err != nil {
			t.Fatalf("Encode(float32 %v) at scale 2: %v [float64-widening noise leaked into the scale]", c.in, err)
		}
		var got *big.Rat
		if _, err := s.Decode(wire, &got); err != nil {
			t.Fatalf("Decode(float32 %v wire): %v", c.in, err)
		}
		if got == nil || got.Cmp(c.want) != 0 {
			t.Errorf("float32 %v -> %v, want %v", c.in, got, c.want)
		}
	}
}
