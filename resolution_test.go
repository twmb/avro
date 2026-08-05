package avro_test

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// Tier-2 schema-resolution matrix (CORRECTNESS_PLAN.md resolution gap).
// Resolution (reader schema != writer schema) is a belief-heavy path. The
// numeric promotion arms (int/long -> float/double) must round EXACTLY as a
// direct Go conversion would, and crucially must SINGLE-round: an int64 widens
// straight to float32, never int64 -> double -> float32, which double-rounds
// and is off by one ULP for magnitudes past 2^53. Expectations are computed
// with Go's own numeric conversions (independent of the library's promote.go)
// and compared on the raw bits, so a one-ULP drift cannot hide behind == on
// floats (where float32(n) and float32(float64(n)) often print the same).

func resResolve(t *testing.T, w, r *avro.Schema) *avro.Schema {
	t.Helper()
	rs, err := avro.Resolve(w, r)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	return rs
}

func resEncode(t *testing.T, s *avro.Schema, v any) []byte {
	t.Helper()
	b, err := s.Encode(v)
	if err != nil {
		t.Fatalf("Encode(%v): %v", v, err)
	}
	return b
}

func TestResolutionPromotionMatrix(t *testing.T) {
	intS := avro.MustParse(`"int"`)
	longS := avro.MustParse(`"long"`)
	floatS := avro.MustParse(`"float"`)
	doubleS := avro.MustParse(`"double"`)

	// int32 writer values, including the float32-rounding edges (2^24±1)
	// and both signed extremes.
	intVals := []int32{
		0, 1, -1, 2, -2,
		1 << 23, 1 << 24, (1 << 24) + 1, -((1 << 24) + 1),
		math.MaxInt32, math.MinInt32,
	}
	// int64 writer values. dr is a constructed double-rounding witness:
	// float32(dr) and float32(float64(dr)) differ by one float32 ULP, so an
	// int64 -> double -> float32 promotion is provably wrong on this input.
	// (float32(dr) = 0x5a000001, float32(float64(dr)) = 0x5a000002.)
	const dr = int64(9007200865353727)
	longVals := []int64{
		0, 1, -1, 1 << 24, (1 << 24) + 1,
		1 << 52, 1 << 53, (1 << 53) + 1,
		dr, -dr,
		math.MaxInt64, math.MinInt64,
	}

	checkF32 := func(t *testing.T, rs *avro.Schema, wire []byte, want float32, label string) {
		t.Helper()
		var got float32
		if _, err := rs.Decode(wire, &got); err != nil {
			t.Fatalf("%s: decode: %v", label, err)
		}
		if math.Float32bits(got) != math.Float32bits(want) {
			t.Errorf("%s = %v (%#08x), want %v (%#08x) [one-ULP / double-rounding drift]",
				label, got, math.Float32bits(got), want, math.Float32bits(want))
		}
	}
	checkF64 := func(t *testing.T, rs *avro.Schema, wire []byte, want float64, label string) {
		t.Helper()
		var got float64
		if _, err := rs.Decode(wire, &got); err != nil {
			t.Fatalf("%s: decode: %v", label, err)
		}
		if math.Float64bits(got) != math.Float64bits(want) {
			t.Errorf("%s = %v (%#016x), want %v (%#016x)",
				label, got, math.Float64bits(got), want, math.Float64bits(want))
		}
	}

	t.Run("int->long", func(t *testing.T) {
		rs := resResolve(t, intS, longS)
		for _, v := range intVals {
			wire := resEncode(t, intS, v)
			var got int64
			if _, err := rs.Decode(wire, &got); err != nil {
				t.Fatalf("v=%d: decode: %v", v, err)
			}
			if got != int64(v) {
				t.Errorf("int %d -> long = %d, want %d", v, got, int64(v))
			}
		}
	})

	t.Run("int->float", func(t *testing.T) {
		rs := resResolve(t, intS, floatS)
		for _, v := range intVals {
			wire := resEncode(t, intS, v)
			checkF32(t, rs, wire, float32(v), fmt.Sprintf("int %d -> float", v))
		}
	})

	t.Run("int->double", func(t *testing.T) {
		rs := resResolve(t, intS, doubleS)
		for _, v := range intVals {
			wire := resEncode(t, intS, v)
			checkF64(t, rs, wire, float64(v), fmt.Sprintf("int %d -> double", v))
		}
	})

	t.Run("long->float", func(t *testing.T) {
		rs := resResolve(t, longS, floatS)
		for _, n := range longVals {
			wire := resEncode(t, longS, n)
			// Go's int64 -> float32 conversion is correctly rounded (single
			// round). The library must match it bit-for-bit, including dr.
			checkF32(t, rs, wire, float32(n), fmt.Sprintf("long %d -> float", n))
		}
	})

	t.Run("long->double", func(t *testing.T) {
		rs := resResolve(t, longS, doubleS)
		for _, n := range longVals {
			wire := resEncode(t, longS, n)
			checkF64(t, rs, wire, float64(n), fmt.Sprintf("long %d -> double", n))
		}
	})

	t.Run("float->double", func(t *testing.T) {
		rs := resResolve(t, floatS, doubleS)
		f32 := func(b uint32) float32 { return math.Float32frombits(b) }
		floatVals := []float32{
			0, 1, -1, 1.5, -2.5,
			float32(math.Inf(1)), float32(math.Inf(-1)),
			math.MaxFloat32, math.SmallestNonzeroFloat32,
			f32(0x7f800001), // signaling NaN
			f32(0x7fc00000), // quiet NaN
			f32(0x80000000), // -0.0
			f32(0x00000001), // smallest denormal
		}
		for _, f := range floatVals {
			wire := resEncode(t, floatS, f)
			// float32 -> float64 widening is lossless; want the exact widening.
			checkF64(t, rs, wire, float64(f), fmt.Sprintf("float %#08x -> double", math.Float32bits(f)))
		}
	})
}

// TestResolutionLogicalPromotion pins that promoting int -> long with a
// logical-typed reader (timestamp-millis / -micros) applies the SAME logical
// conversion a native long+logical decode applies. int and long share the
// zigzag-varint wire, so one set of writer bytes feeds both the resolved
// promotion path and a native long+logical decode of the identical value; the
// two must agree. This path (a logical-typed promotion target) had no
// dedicated coverage, so a promotion that dropped or mis-scaled the logical
// conversion would have gone unnoticed.
func TestResolutionLogicalPromotion(t *testing.T) {
	intS := avro.MustParse(`"int"`)
	cases := []struct {
		name   string
		reader string
	}{
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`},
	}
	// int32-range values; the int wire bytes equal the long wire bytes for the
	// same numeric value, so the native reader decodes the identical instant.
	vals := []int32{0, 1, 1000, -1000, 1 << 20, math.MaxInt32, math.MinInt32}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			readerS := avro.MustParse(c.reader)
			rs := resResolve(t, intS, readerS)
			for _, v := range vals {
				wire := resEncode(t, intS, v) // int wire == long wire for v

				var gotResolved, gotNative time.Time
				if _, err := rs.Decode(wire, &gotResolved); err != nil {
					t.Fatalf("v=%d: resolved decode: %v", v, err)
				}
				if _, err := readerS.Decode(wire, &gotNative); err != nil {
					t.Fatalf("v=%d: native decode: %v", v, err)
				}
				if !gotResolved.Equal(gotNative) {
					t.Errorf("v=%d: resolved promotion gave %v, native long+%s decode gave %v",
						v, gotResolved, c.name, gotNative)
				}
			}
		})
	}
}
