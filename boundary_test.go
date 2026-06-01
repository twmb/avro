package avro_test

import (
	"math"
	"math/big"
	"reflect"
	"testing"

	"github.com/twmb/avro"
)

// Tier-2 boundary matrix (CORRECTNESS_PLAN.md §T2d). The recurring class here
// is numeric boundary / overflow handling across paths, where the value is
// exactly AT a representability edge (2^53±1, 2^63, 2^64, 2^24±1). Example
// caught by reverting its fix: float→integer decode must bound in FLOAT space,
// not via an int64(f) round-trip (the conversion is implementation-defined on
// overflow — arm64 saturates, amd64 wraps — so the round-trip check silently
// accepted an out-of-range whole float and stored an off-by-one value).
//
// Expectations are computed with big.Float/big.Int (exact), NOT via float
// comparison — float64(2^63-1) rounds to 2^63, which is exactly how the bug
// hid from a naive round-trip assertion.

func intTypeBounds(t reflect.Type) (lo, hi *big.Int) {
	bits := t.Bits()
	if t.Kind() >= reflect.Int && t.Kind() <= reflect.Int64 {
		// signed: [-2^(bits-1), 2^(bits-1)-1]
		hi = new(big.Int).Lsh(big.NewInt(1), uint(bits-1))
		lo = new(big.Int).Neg(hi)
		hi.Sub(hi, big.NewInt(1))
		return lo, hi
	}
	// unsigned: [0, 2^bits-1]
	hi = new(big.Int).Lsh(big.NewInt(1), uint(bits))
	hi.Sub(hi, big.NewInt(1))
	return big.NewInt(0), hi
}

func bigOfReflect(v reflect.Value) *big.Int {
	if v.CanInt() {
		return big.NewInt(v.Int())
	}
	return new(big.Int).SetUint64(v.Uint())
}

func TestBoundaryMatrix_FloatToInteger(t *testing.T) {
	doubleS := avro.MustParse(`"double"`)
	floatS := avro.MustParse(`"float"`)

	p63 := math.Ldexp(1, 63) // 2^63  (MaxInt64+1; NOT representable in int64)
	p64 := math.Ldexp(1, 64) // 2^64  (NOT representable in uint64)
	p53 := math.Ldexp(1, 53) // 2^53
	p24 := math.Ldexp(1, 24) // 2^24
	values := []float64{
		0, 1, -1, 2, -2,
		p24, p24 + 1, -(p24 + 1),
		p53, p53 + 1, -(p53 + 1),
		p63, -p63, // -2^63 == MinInt64 (representable); +2^63 is not
		p63 + p63, p64, // 2^64 boundary for uint64
		1.5, -2.5, 1e300, // non-whole / way out of range
	}
	intTypes := []reflect.Type{
		reflect.TypeFor[int8](), reflect.TypeFor[int16](), reflect.TypeFor[int32](), reflect.TypeFor[int64](),
		reflect.TypeFor[uint8](), reflect.TypeFor[uint16](), reflect.TypeFor[uint32](), reflect.TypeFor[uint64](),
	}

	type variant struct {
		name string
		s    *avro.Schema
		// wireVal is the value actually on the wire (double keeps v; float
		// narrows v to float32), used to compute the exact expectation.
		wire func(v float64) (enc any, wireVal float64)
	}
	variants := []variant{
		{"double", doubleS, func(v float64) (any, float64) { return v, v }},
		{"float", floatS, func(v float64) (any, float64) { return float32(v), float64(float32(v)) }},
	}

	for _, va := range variants {
		for _, v := range values {
			enc, wireVal := va.wire(v)
			wire, err := va.s.Encode(enc)
			if err != nil {
				t.Fatalf("%s: Encode(%v): %v", va.name, enc, err)
			}
			// Exact expectation: decode succeeds iff wireVal is a whole number
			// within the target type's range. Computed with big.Float — exact.
			bf := new(big.Float).SetFloat64(wireVal)
			whole := bf.IsInt()
			var exact *big.Int
			if whole {
				exact, _ = bf.Int(nil)
			}
			for _, it := range intTypes {
				lo, hi := intTypeBounds(it)
				expectOK := whole && exact.Cmp(lo) >= 0 && exact.Cmp(hi) <= 0

				ptr := reflect.New(it)
				_, derr := va.s.Decode(wire, ptr.Interface())

				if (derr == nil) != expectOK {
					t.Errorf("%s wire %v -> %s: decode err=%v, want ok=%v (exact=%v range [%v,%v])",
						va.name, wireVal, it, derr, expectOK, exact, lo, hi)
					continue
				}
				if expectOK {
					if got := bigOfReflect(ptr.Elem()); got.Cmp(exact) != 0 {
						t.Errorf("%s wire %v -> %s: decoded %v, want exact %v (silent precision loss)",
							va.name, wireVal, it, got, exact)
					}
				}
			}
		}
	}
}
