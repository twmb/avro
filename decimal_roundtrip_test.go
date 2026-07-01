package avro_test

import (
	"encoding/json"
	"fmt"
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

// TestRegression_DecimalStringCarrierIsNumericTextOnly pins that a Go string
// carrier for a decimal logical is the numeric-text form ONLY on encode, making
// encode symmetric with decode — whose string target always reads the wire as
// numeric decimal text (setDecimalRat). A non-numeric string is rejected on
// BOTH wire formats for decimal-on-bytes and decimal-on-fixed rather than
// silently written as opaque raw bytes: that fall-through emitted a wire the
// decoder read back as a decimal number ("abcxyz" -> "107075203529.082"),
// breaking the round trip. []byte remains the sole opaque escape hatch
// (symmetric on both sides); a numeric string and a numeric json.Number still
// encode. big-decimal is excluded — its string decode target falls through to
// raw bytes, so its string carrier is opaque-symmetric and must stay accepted
// (asserted here as a control that the fix did not over-reach).
func TestRegression_DecimalStringCarrierIsNumericTextOnly(t *testing.T) {
	const nonNumeric = "abcxyz" // 6 bytes: fits a fixed[6] as raw bytes, but is not a number
	const numeric = "0.312"     // unscaled 312 at scale 3

	// A decimal (bytes and fixed) rejects a non-numeric string on both wires,
	// while numeric string / numeric json.Number / opaque []byte all work.
	for _, d := range []struct{ name, schema string }{
		{"bytes", `{"type":"bytes","logicalType":"decimal","precision":12,"scale":3}`},
		{"fixed", `{"type":"fixed","name":"DF","size":6,"logicalType":"decimal","precision":12,"scale":3}`},
	} {
		t.Run(d.name, func(t *testing.T) {
			s := avro.MustParse(d.schema)

			// Reject: a non-numeric string on BOTH wire formats.
			if _, err := s.AppendEncode(nil, nonNumeric); err == nil {
				t.Errorf("binary Encode(non-numeric string) accepted; a decimal string carrier is numeric-text-only")
			}
			if _, err := s.AppendEncodeJSON(nil, nonNumeric); err == nil {
				t.Errorf("EncodeJSON(non-numeric string) accepted; a decimal string carrier is numeric-text-only")
			}

			// Control: a numeric string round-trips as text on both wires.
			for _, bin := range []bool{true, false} {
				wire, err := encodeWire(s, numeric, bin)
				if err != nil {
					t.Fatalf("%s Encode(numeric string) rejected: %v", wireName(bin), err)
				}
				var back string
				if err := decodeWire(s, wire, &back, bin); err != nil {
					t.Fatalf("%s decode numeric string: %v", wireName(bin), err)
				}
				if back != numeric {
					t.Errorf("%s numeric string round-trip: got %q want %q", wireName(bin), back, numeric)
				}
			}

			// Control: []byte is the opaque escape hatch and round-trips (binary).
			bw, err := s.AppendEncode(nil, []byte(nonNumeric))
			if err != nil {
				t.Fatalf("Encode([]byte opaque): %v", err)
			}
			var bback []byte
			if _, err := s.Decode(bw, &bback); err != nil {
				t.Fatalf("Decode []byte opaque: %v", err)
			}
			if string(bback) != nonNumeric {
				t.Errorf("[]byte opaque round-trip: got %q want %q", bback, nonNumeric)
			}

			// Control: a numeric json.Number still encodes on both wires; a
			// non-numeric one rejects identically to a non-numeric string.
			if _, err := s.AppendEncode(nil, json.Number(numeric)); err != nil {
				t.Errorf("binary Encode(numeric json.Number) rejected: %v", err)
			}
			if _, err := s.AppendEncodeJSON(nil, json.Number(numeric)); err != nil {
				t.Errorf("EncodeJSON(numeric json.Number) rejected: %v", err)
			}
			if _, err := s.AppendEncode(nil, json.Number(nonNumeric)); err == nil {
				t.Errorf("binary Encode(non-numeric json.Number) accepted; want reject")
			}
		})
	}

	// Control: big-decimal's string carrier stays opaque-symmetric on both
	// wires (the fix must NOT reach big-decimal).
	bd := avro.MustParse(`{"type":"bytes","logicalType":"big-decimal"}`)
	for _, bin := range []bool{true, false} {
		wire, err := encodeWire(bd, nonNumeric, bin)
		if err != nil {
			t.Fatalf("big-decimal %s Encode(string): %v (must stay opaque-accepted)", wireName(bin), err)
		}
		var back string
		if err := decodeWire(bd, wire, &back, bin); err != nil {
			t.Fatalf("big-decimal %s decode: %v", wireName(bin), err)
		}
		if back != nonNumeric {
			t.Errorf("big-decimal %s string round-trip broke: got %q want %q", wireName(bin), back, nonNumeric)
		}
	}
}

// TestMatrix_DecimalCarrierNumericTextContract is the generative net for the
// decimal carrier contract: for a decimal logical (bytes AND fixed) a Go string
// and a json.Number are the numeric-text form ONLY — a non-numeric one is
// REJECTED on encode, on both wire formats, in EVERY encode context — rather
// than silently written as opaque raw bytes, keeping encode symmetric with
// decode (whose string target always reads numeric decimal text). []byte is the
// sole opaque carrier and encodes in every cell. big-decimal is excluded (its
// string decode target falls through to raw bytes, so its string carrier is
// opaque-symmetric; TestRegression_DecimalStringCarrierIsNumericTextOnly pins
// that separately).
//
// Axes: carrier {string, []byte, json.Number} × content {numeric, non-numeric}
// × backing {bytes, fixed} × wire {binary, JSON} × encode context {top-level,
// record field, array element, map value} (the path-divergence axis — a decimal
// leaf is reachable at each). The oracle is calibration-free: string and
// json.Number reject a non-numeric carrier identically and accept a numeric
// one; []byte always encodes. Neuter: reverting rejectNonNumericDecimalString
// (ser.go + json_codec.go) reds every string/json.Number + non-numeric cell
// across contexts, backings, and wires; the numeric and []byte controls stay
// green.
func TestMatrix_DecimalCarrierNumericTextContract(t *testing.T) {
	// leaf schemas: a fixed sized to hold both a numeric value's unscaled bytes
	// and the 6-byte non-numeric raw string.
	backings := []struct{ name, leaf string }{
		{"bytes", `{"type":"bytes","logicalType":"decimal","precision":12,"scale":3}`},
		{"fixed", `{"type":"fixed","name":"DF","size":6,"logicalType":"decimal","precision":12,"scale":3}`},
	}
	// carriers: the value placed at the decimal leaf, plus whether encode must
	// be REJECTED (a non-numeric string / json.Number) or ACCEPTED.
	carriers := []struct {
		name   string
		val    any
		reject bool
	}{
		{"string_numeric", "0.312", false},
		{"string_nonnumeric", "abcxyz", true},
		{"jsonnumber_numeric", json.Number("0.312"), false},
		{"jsonnumber_nonnumeric", json.Number("abcxyz"), true},
		{"bytes_opaque", []byte("abcxyz"), false}, // []byte: always opaque, always accepts
	}
	// encode contexts: wrap the leaf and place the carrier at it.
	contexts := []struct {
		name string
		wrap func(leaf string) string
		val  func(carrier any) any
	}{
		{"top", func(l string) string { return l }, func(c any) any { return c }},
		{"record_field",
			func(l string) string {
				return fmt.Sprintf(`{"type":"record","name":"R","fields":[{"name":"f","type":%s}]}`, l)
			},
			func(c any) any { return map[string]any{"f": c} }},
		{"array_element",
			func(l string) string { return fmt.Sprintf(`{"type":"array","items":%s}`, l) },
			func(c any) any { return []any{c} }},
		{"map_value",
			func(l string) string { return fmt.Sprintf(`{"type":"map","values":%s}`, l) },
			func(c any) any { return map[string]any{"k": c} }},
	}

	for _, b := range backings {
		for _, car := range carriers {
			for _, ctx := range contexts {
				for _, bin := range []bool{true, false} {
					t.Run(fmt.Sprintf("%s/%s/%s/%s", b.name, car.name, ctx.name, wireName(bin)), func(t *testing.T) {
						s := avro.MustParse(ctx.wrap(b.leaf))
						_, err := encodeWireAny(s, ctx.val(car.val), bin)
						if car.reject && err == nil {
							t.Errorf("%s encode accepted a non-numeric %s carrier; want reject (decimal carrier is numeric-text-only, []byte is the sole opaque form)", wireName(bin), car.name)
						}
						if !car.reject && err != nil {
							t.Errorf("%s encode rejected a valid %s carrier: %v", wireName(bin), car.name, err)
						}
					})
				}
			}
		}
	}
}

// encodeWire / decodeWire / encodeWireAny / wireName are shared helpers for the
// decimal carrier tests: one place selects the binary vs JSON entry point.
func wireName(bin bool) string {
	if bin {
		return "binary"
	}
	return "json"
}

func encodeWire(s *avro.Schema, v any, bin bool) ([]byte, error) { return encodeWireAny(s, v, bin) }

func encodeWireAny(s *avro.Schema, v any, bin bool) ([]byte, error) {
	if bin {
		return s.AppendEncode(nil, v)
	}
	return s.AppendEncodeJSON(nil, v)
}

func decodeWire(s *avro.Schema, wire []byte, v any, bin bool) error {
	if bin {
		_, err := s.Decode(wire, v)
		return err
	}
	return s.DecodeJSON(wire, v)
}
