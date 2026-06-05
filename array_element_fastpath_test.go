package avro

import (
	"bytes"
	"math"
	"reflect"
	"testing"
)

// The array/map primitive encoders fast-path the exact natural Go element
// type (int32 for "int", int64/int for "long", float32/float64, bool,
// string) with a direct read+emit loop, bypassing the per-element
// appendAvro* dispatch. A named element type of the same underlying kind
// routes through the GENERAL appendAvro* path instead. This pins the
// invariant that the two produce byte-identical wire — the fast loop must
// never diverge from the general path, including at boundary values.

type fpInt32 int32
type fpInt64 int64
type fpInt int
type fpFloat32 float32
type fpFloat64 float64
type fpBool bool
type fpString string

func TestRegression_ArrayElementFastPathMatchesGeneral(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		fast   any // builtin element type → fast loop
		gen    any // named element type → general appendAvro* path
	}{
		{"int", `{"type":"array","items":"int"}`,
			[]int32{0, 1, -1, math.MaxInt32, math.MinInt32},
			[]fpInt32{0, 1, -1, math.MaxInt32, math.MinInt32}},
		{"long_int64", `{"type":"array","items":"long"}`,
			[]int64{0, 1, -1, math.MaxInt64, math.MinInt64},
			[]fpInt64{0, 1, -1, math.MaxInt64, math.MinInt64}},
		{"long_int", `{"type":"array","items":"long"}`,
			// Platform int extremes: identical to MaxInt64/MinInt64 on
			// 64-bit, and the representable extremes (so the file still
			// compiles) on 32-bit platforms.
			[]int{0, 1, -1, math.MaxInt, math.MinInt},
			[]fpInt{0, 1, -1, math.MaxInt, math.MinInt}},
		{"float", `{"type":"array","items":"float"}`,
			[]float32{0, 1, -1, math.MaxFloat32, float32(math.Inf(1)), float32(math.NaN())},
			[]fpFloat32{0, 1, -1, math.MaxFloat32, fpFloat32(math.Inf(1)), fpFloat32(math.NaN())}},
		{"double", `{"type":"array","items":"double"}`,
			[]float64{0, 1, -1, math.MaxFloat64, math.Inf(-1), math.NaN()},
			[]fpFloat64{0, 1, -1, math.MaxFloat64, fpFloat64(math.Inf(-1)), fpFloat64(math.NaN())}},
		{"boolean", `{"type":"array","items":"boolean"}`,
			[]bool{true, false, true},
			[]fpBool{true, false, true}},
		{"string", `{"type":"array","items":"string"}`,
			[]string{"", "a", "héllo", "x\x00y"},
			[]fpString{"", "a", "héllo", "x\x00y"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := MustParse(c.schema)
			fast, err := s.Encode(c.fast)
			if err != nil {
				t.Fatalf("fast encode: %v", err)
			}
			gen, err := s.Encode(c.gen)
			if err != nil {
				t.Fatalf("general encode: %v", err)
			}
			if !bytes.Equal(fast, gen) {
				t.Fatalf("fast-loop wire diverges from general path:\n fast=% x\n gen =% x", fast, gen)
			}
			// Round-trip the fast wire back into the builtin slice type.
			out := reflect.New(reflect.TypeOf(c.fast)).Interface()
			if _, err := s.Decode(fast, out); err != nil {
				t.Fatalf("decode: %v", err)
			}
			re, err := s.Encode(reflect.ValueOf(out).Elem().Interface())
			if err != nil {
				t.Fatalf("re-encode: %v", err)
			}
			if !bytes.Equal(re, fast) {
				t.Fatalf("round-trip wire mismatch:\n in =% x\n out=% x", fast, re)
			}
		})
	}
}

// Decode-side native path: a builtin []V decodes via the concrete-slice loop
// (s[i]=v); a named slice type and a named-element type must fall back to the
// reflect loop (the unnamed-[]V assertion returns handled=false) and still
// decode correctly with src untouched on the fallthrough.
func TestRegression_ArrayDecodeNamedFallback(t *testing.T) {
	type namedSlice []int32
	type namedElem int32
	s := MustParse(`{"type":"array","items":"int"}`)
	want := []int32{0, 1, -1, math.MaxInt32, math.MinInt32}
	wire, err := s.Encode(want)
	if err != nil {
		t.Fatal(err)
	}
	var builtin []int32 // native loop
	if _, err := s.Decode(wire, &builtin); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(builtin, want) {
		t.Fatalf("native []int32: %v != %v", builtin, want)
	}
	var ns namedSlice // named slice type → fallback
	if _, err := s.Decode(wire, &ns); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual([]int32(ns), want) {
		t.Fatalf("named slice fallback: %v", ns)
	}
	var ne []namedElem // named elem type → fallback
	if _, err := s.Decode(wire, &ne); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ne, []namedElem{0, 1, -1, math.MaxInt32, math.MinInt32}) {
		t.Fatalf("named elem fallback: %v", ne)
	}
}

// Map builtin-string value fast path must match the general (named-value) path.
func TestRegression_MapValueFastPathMatchesGeneral(t *testing.T) {
	s := MustParse(`{"type":"map","values":"string"}`)
	fast, _ := s.Encode(map[string]string{"k": "v"})
	gen, _ := s.Encode(map[string]fpString{"k": "v"})
	if !bytes.Equal(fast, gen) {
		t.Fatalf("map value fast-loop diverges: fast=% x gen=% x", fast, gen)
	}
}
