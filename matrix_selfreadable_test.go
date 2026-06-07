package avro_test

import (
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// Generative self-readability net (the SCALE axis).
//
// The combinatorial matrix sweeps SHAPE at small scale (collections of size
// 0..4, small schemas). Every DoS cap, by contrast, lives at LARGE scale
// (maxZeroByteItems=4096, ocfMetadataSafetyLimit=1 MiB, decimalScaleLimit=
// 65536, errTooDeep=1000), so a small-value generator structurally never
// reaches it. That blind spot is where reader-side caps with no producer-side
// compliance hide — an encoder that emits wire its own decoder rejects (a
// silent self-incompatible round-trip).
//
// The invariant here is calibration-free and the exact inverse of that bug:
// for every value, if Encode SUCCEEDS, Decode of that wire MUST also succeed
// (encode-accepts ⟹ decode-accepts-own-output) — on BOTH wires. A clean
// encode-time rejection is always fine; the only forbidden outcome is a wire
// the producer emits and the consumer refuses. Each generator drives a
// degenerate shape ACROSS its cap boundary (cap-1, cap, cap+1, and well
// past).
// ---------------------------------------------------------------------------

func TestMatrix_SelfReadableAtScale(t *testing.T) {
	zeroByteItem := func(label string) (string, any) {
		switch label {
		case "null":
			return `"null"`, nil
		case "emptyrecord":
			return `{"type":"record","name":"E","fields":[]}`, map[string]any{}
		case "size0fixed":
			return `{"type":"fixed","name":"Z","size":0}`, []byte{}
		}
		panic(label)
	}

	type gen struct {
		label  string
		schema string
		value  func() any
	}
	var gens []gen

	// Zero-byte-item arrays across the maxZeroByteItems boundary.
	for _, item := range []string{"null", "emptyrecord", "size0fixed"} {
		itemSchema, itemVal := zeroByteItem(item)
		for _, n := range []int{4095, 4096, 4097, 10000} {
			gens = append(gens, gen{
				label:  fmt.Sprintf("array<%s>×%d", item, n),
				schema: fmt.Sprintf(`{"type":"array","items":%s}`, itemSchema),
				value: func() any {
					a := make([]any, n)
					for i := range a {
						a[i] = itemVal
					}
					return a
				},
			})
		}
	}

	// Maps of zero-byte values across the same boundary (finding-1 claims
	// maps are immune because a key is ≥1 byte; this proves it by sweep).
	for _, n := range []int{4096, 4097, 10000} {
		gens = append(gens, gen{
			label:  fmt.Sprintf("map<null>×%d", n),
			schema: `{"type":"map","values":"null"}`,
			value: func() any {
				m := make(map[string]any, n)
				for i := range n {
					m[fmt.Sprintf("k%d", i)] = nil
				}
				return m
			},
		})
	}

	// Large strings / bytes / fixed (single-value scale, not collection).
	for _, sz := range []int{1 << 20, 4 << 20} {
		gens = append(gens,
			gen{fmt.Sprintf("string@%d", sz), `"string"`, func() any { return strings.Repeat("x", sz) }},
			gen{fmt.Sprintf("bytes@%d", sz), `"bytes"`, func() any { return make([]byte, sz) }},
		)
	}

	// Decimal scale across decimalScaleLimit (65536): a *big.Rat whose
	// denominator forces a large scale.
	for _, scale := range []int{65535, 65536, 65537} {
		gens = append(gens, gen{
			label:  fmt.Sprintf("decimal@scale%d", scale),
			schema: fmt.Sprintf(`{"type":"bytes","logicalType":"decimal","precision":%d,"scale":%d}`, scale+2, scale),
			value: func() any {
				// 1 / 10^scale → needs `scale` fractional digits.
				den := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
				return new(big.Rat).SetFrac(big.NewInt(1), den)
			},
		})
	}

	// Deeply nested arrays around errTooDeep (1000).
	for _, depth := range []int{998, 1000, 1002} {
		schema := "\"long\""
		for range depth {
			schema = fmt.Sprintf(`{"type":"array","items":%s}`, schema)
		}
		d := depth
		gens = append(gens, gen{
			label:  fmt.Sprintf("nested-array@%d", depth),
			schema: schema,
			value: func() any {
				var v any = int64(1)
				for range d {
					v = []any{v}
				}
				return v
			},
		})
	}

	check := func(t *testing.T, label string, v any,
		enc func([]byte, any) ([]byte, error), dec func([]byte, any) error, wire string) {
		data, encErr := enc(nil, v)
		if encErr != nil {
			return // encode-time rejection is always acceptable
		}
		var sink any
		if decErr := dec(data, &sink); decErr != nil {
			t.Errorf("SELF-INCOMPATIBLE [%s wire]: %s — Encode produced %d bytes the decoder REJECTS: %v",
				wire, label, len(data), decErr)
		}
	}

	for _, g := range gens {
		t.Run(g.label, func(t *testing.T) {
			s, err := avro.Parse(g.schema)
			if err != nil {
				return // schema itself rejected at parse — fine
			}
			check(t, g.label, g.value(),
				func(b []byte, v any) ([]byte, error) { return s.AppendEncode(b, v) },
				func(b []byte, tgt any) error { _, e := s.Decode(b, tgt); return e }, "binary")
			check(t, g.label, g.value(),
				func(b []byte, v any) ([]byte, error) { return s.AppendEncodeJSON(b, v) },
				func(b []byte, tgt any) error { return s.DecodeJSON(b, tgt) }, "json")
		})
	}

	// UNSAFE struct-field path. The generators above pass top-level []any /
	// map[string]any values, which route through the REFLECT encoders. A
	// zero-byte array that is an addressable struct field instead routes
	// through the UNSAFE encoders (usArrayRecord / usArrayPtrRecord /
	// usArrayDirect) — a structurally distinct code path that the first
	// producer-compliance fix missed, and which this net was blind to until
	// it drove typed struct fields. Each wrapper holds the same zero-byte
	// array element type as a TYPED slice field, swept across the cap.
	for _, uc := range unsafeArrayCases() {
		for _, n := range []int{4096, 4097, 10000} {
			t.Run(fmt.Sprintf("unsafe-field/%s×%d", uc.label, n), func(t *testing.T) {
				s := avro.MustParse(uc.schema)
				ptr := uc.value(n) // &struct{ A []ElemT }{...}, addressable → unsafe path
				check(t, fmt.Sprintf("unsafe-field/%s×%d", uc.label, n), ptr,
					func(b []byte, v any) ([]byte, error) { return s.AppendEncode(b, v) },
					func(b []byte, tgt any) error { _, e := s.Decode(b, tgt); return e }, "binary")
			})
		}
	}
}

// srEmptyRec maps to an empty record; the typed slices below force the unsafe
// array encoders (a []any would stay on the reflect path).
type srEmptyRec struct{}

func unsafeArrayCases() []struct {
	label  string
	schema string
	value  func(n int) any
} {
	const recField = `{"type":"record","name":"H","fields":[{"name":"a","type":{"type":"array","items":{"type":"record","name":"E","fields":[]}}}]}`
	const fixedField = `{"type":"record","name":"H","fields":[{"name":"a","type":{"type":"array","items":{"type":"fixed","name":"Z","size":0}}}]}`
	return []struct {
		label  string
		schema string
		value  func(n int) any
	}{
		{"slice-empty-record", recField, func(n int) any {
			return &struct {
				A []srEmptyRec `avro:"a"`
			}{A: make([]srEmptyRec, n)}
		}},
		{"slice-ptr-empty-record", recField, func(n int) any {
			a := make([]*srEmptyRec, n)
			for i := range a {
				a[i] = &srEmptyRec{}
			}
			return &struct {
				A []*srEmptyRec `avro:"a"`
			}{A: a}
		}},
		{"slice-size0-fixed", fixedField, func(n int) any {
			return &struct {
				A [][0]byte `avro:"a"`
			}{A: make([][0]byte, n)}
		}},
	}
}
