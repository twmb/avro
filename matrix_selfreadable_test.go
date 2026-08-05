package avro_test

import (
	"bytes"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
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

	// Decimal UNSCALED-LENGTH axis (maxDecimalUnscaledBytes = 32 KiB), the
	// bound orthogonal to the scale generator above.
	//
	// This axis has to sweep the CARRIER, because the carrier is what decides
	// whether any upstream gate is reached at all — and the gates differ:
	//
	//   - a numeric carrier on "decimal" is bounded by the DECLARED PRECISION,
	//     itself parse-capped, so it cannot reach the length bound;
	//   - a numeric carrier on "big-decimal" is bounded by NOTHING, because
	//     that logical type has no precision attribute to declare;
	//   - the opaque []byte escape hatch is bounded by neither, on either.
	//
	// The fixed container is a third route again: it pads to the schema's SIZE
	// whatever the value, so the size alone decides the emitted width and every
	// carrier lands in the same place. A net that drove only *big.Rat on a
	// bytes/decimal would see the precision gate fire and conclude the bound
	// was unreachable from the producer side.
	//
	// The single-object and OCF wires are here because they re-frame the same
	// encoder output: an escape that reaches them ships a FILE whose reader
	// cannot open it, which is strictly worse than a rejected call.
	const unscaledCap = 32 << 10
	rawOf := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = 0x01
		}
		return b
	}
	// The opaque carrier's payload must be the shape whose UNSCALED part is n
	// bytes, and that shape differs per logical: on "decimal" the payload IS
	// the unscaled value, while on "big-decimal" it is a framing that WRAPS it.
	// Handing big-decimal n raw bytes would test the framing grammar instead —
	// 0x01 reads as a zigzag -1 and dies on the length before the bound is ever
	// consulted, so the cell would red for a reason that has nothing to do with
	// this axis and would never exercise it.
	bigDecFramingOf := func(n int) []byte {
		out := zigzagEncode64(int64(n))
		out = append(out, rawOf(n)...)
		return append(out, zigzagEncode64(0)...)
	}
	// ratOfLen returns a rational whose minimal two's-complement unscaled form
	// is exactly n bytes: 2^(8n-9) has bit length 8n-8, so its magnitude fills
	// n-1 bytes with the top bit set, and the sign byte makes it n.
	ratOfLen := func(n int) *big.Rat {
		return new(big.Rat).SetInt(new(big.Int).Lsh(big.NewInt(1), uint(8*n-9)))
	}

	type decCell struct {
		label  string
		schema string
		value  any
	}
	var decCells []decCell
	for _, n := range []int{unscaledCap - 1, unscaledCap, unscaledCap + 1} {
		bytesDec := `{"type":"bytes","logicalType":"decimal","precision":65536,"scale":0}`
		bigDec := `{"type":"bytes","logicalType":"big-decimal"}`
		fixedDec := fmt.Sprintf(`{"type":"fixed","name":"F","size":%d,"logicalType":"decimal","precision":65536,"scale":0}`, n)
		for _, c := range []struct {
			carrier string
			schema  string
			value   any
		}{
			{"rat", bytesDec, ratOfLen(n)},
			{"opaque", bytesDec, rawOf(n)},
			{"text", bytesDec, ratOfLen(n).RatString()},
			{"rat", bigDec, ratOfLen(n)},
			{"opaque", bigDec, bigDecFramingOf(n)},
			{"text", bigDec, ratOfLen(n).RatString()},
			{"rat", fixedDec, big.NewRat(5, 1)},
			{"opaque", fixedDec, rawOf(n)},
			{"text", fixedDec, "5"},
		} {
			logical := "decimal"
			if c.schema == bigDec {
				logical = "big-decimal"
			}
			container := "bytes"
			if c.schema == fixedDec {
				container = fmt.Sprintf("fixed%+d", n-unscaledCap)
			}
			base := fmt.Sprintf("%s/%s/%s@%+d", logical, container, c.carrier, n-unscaledCap)
			decCells = append(decCells, decCell{base, c.schema, c.value})
			// The same value delivered through an `any`-typed record field, so
			// the record dispatch is crossed too and not just the top level.
			decCells = append(decCells, decCell{
				label:  base + "/in-record",
				schema: fmt.Sprintf(`{"type":"record","name":"R","fields":[{"name":"d","type":%s}]}`, c.schema),
				value:  map[string]any{"d": c.value},
			})
		}
	}
	for _, g := range decCells {
		t.Run("decimal-unscaled-length/"+g.label, func(t *testing.T) {
			s, err := avro.Parse(g.schema)
			if err != nil {
				return // schema itself rejected at parse — fine
			}
			check(t, g.label, g.value,
				func(b []byte, v any) ([]byte, error) { return s.AppendEncode(b, v) },
				func(b []byte, tgt any) error { _, e := s.Decode(b, tgt); return e }, "binary")
			check(t, g.label, g.value,
				func(b []byte, v any) ([]byte, error) { return s.AppendEncodeJSON(b, v) },
				func(b []byte, tgt any) error { return s.DecodeJSON(b, tgt) }, "json")
			check(t, g.label, g.value,
				func(b []byte, v any) ([]byte, error) { return s.AppendSingleObject(b, v) },
				func(b []byte, tgt any) error { _, e := s.DecodeSingleObject(b, tgt); return e }, "single-object")
			checkOCF(t, g.label, s, g.value)
		})
	}

	// The DEFAULT fill is a distinct emit route to the same bytes, and it is
	// the one route where the caller never chose a carrier: a bytes/fixed
	// default is []byte by construction. It is also pre-encoded at PARSE, so
	// its verdict has to travel to encode rather than being raised where it is
	// computed — a schema whose default cannot be written must still parse,
	// because a reader that DROPS the field never writes it.
	//
	// Four fill routes reach it and they are not one path: an absent key in a
	// map[string]any, an absent key in a typed map, a struct field tagged
	// omitzero (reflect), and the same field through the COMPILED unsafe
	// record path, which copies the pre-encoded bytes at compile time and so
	// can emit what its reflect twin refuses if the verdict does not travel
	// with them.
	for _, n := range []int{unscaledCap - 1, unscaledCap, unscaledCap + 1} {
		for _, c := range []struct {
			label string
			inner string
		}{
			{"bytes/decimal", `{"type":"bytes","logicalType":"decimal","precision":65536,"scale":0}`},
			{"bytes/big-decimal", `{"type":"bytes","logicalType":"big-decimal"}`},
			{"fixed/decimal", fmt.Sprintf(`{"type":"fixed","name":"DF","size":%d,"logicalType":"decimal","precision":65536,"scale":0}`, n)},
		} {
			payload := rawOf(n)
			if c.label == "bytes/big-decimal" {
				payload = bigDecFramingOf(n)
			}
			schema := fmt.Sprintf(
				`{"type":"record","name":"R","fields":[{"name":"d","type":%s,"default":%s},{"name":"keep","type":"int"}]}`,
				c.inner, codepointLit(payload))
			label := fmt.Sprintf("default/%s@%+d", c.label, n-unscaledCap)
			t.Run("decimal-unscaled-length/"+label, func(t *testing.T) {
				s, err := avro.Parse(schema)
				if err != nil {
					t.Fatalf("a schema whose default cannot be WRITTEN must still PARSE: %v", err)
				}
				absent := map[string]any{"keep": int32(7)}
				check(t, label, absent,
					func(b []byte, v any) ([]byte, error) { return s.AppendEncode(b, v) },
					func(b []byte, tgt any) error { _, e := s.Decode(b, tgt); return e }, "binary")
				check(t, label, absent,
					func(b []byte, v any) ([]byte, error) { return s.AppendEncodeJSON(b, v) },
					func(b []byte, tgt any) error { return s.DecodeJSON(b, tgt) }, "json")
				check(t, label, absent,
					func(b []byte, v any) ([]byte, error) { return s.AppendSingleObject(b, v) },
					func(b []byte, tgt any) error { _, e := s.DecodeSingleObject(b, tgt); return e }, "single-object")
				checkOCF(t, label, s, absent)
				// omitzero, reflect and compiled-unsafe both: an addressable
				// struct pointer is what routes into the compiled path.
				if c.label != "fixed/decimal" {
					check(t, label+"/omitzero", &srOmitBytes{Keep: 7},
						func(b []byte, v any) ([]byte, error) { return s.AppendEncode(b, v) },
						func(b []byte, tgt any) error { _, e := s.Decode(b, tgt); return e }, "binary-omitzero")
				}
			})
		}
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

// checkOCF is the container-wire arm of the self-readability invariant: a
// value the OCF writer accepts must be one the OCF reader can read back. It is
// a separate closure because the container re-frames encoder output rather
// than being another (encode, decode) pair — a wire an encoder emits and a
// reader refuses becomes a FILE on disk here.
func checkOCF(t *testing.T, label string, s *avro.Schema, v any) {
	t.Helper()
	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, s)
	if err != nil {
		return // writer construction rejected — acceptable
	}
	if err := w.Encode(v); err != nil {
		return // encode-time rejection is always acceptable
	}
	if err := w.Close(); err != nil {
		return
	}
	size := buf.Len()
	r, err := ocf.NewReader(&buf)
	if err != nil {
		t.Errorf("SELF-INCOMPATIBLE [ocf wire]: %s — the writer produced a %d-byte file NewReader REJECTS: %v",
			label, size, err)
		return
	}
	defer r.Close()
	var sink any
	if err := r.Decode(&sink); err != nil {
		t.Errorf("SELF-INCOMPATIBLE [ocf wire]: %s — the writer produced a %d-byte file the reader REJECTS: %v",
			label, size, err)
	}
}

// srOmitBytes routes a zero-valued defaulted field through the omitzero arm,
// as an addressable struct pointer so the COMPILED unsafe record path is the
// one that fills the default.
type srOmitBytes struct {
	D    []byte `avro:"d,omitzero"`
	Keep int32  `avro:"keep"`
}

// codepointLit renders bytes as an Avro-JSON codepoint default literal using
// \u escapes, so the source carries no raw control bytes.
func codepointLit(b []byte) string {
	var sb strings.Builder
	sb.Grow(len(b)*6 + 2)
	sb.WriteByte('"')
	for _, c := range b {
		fmt.Fprintf(&sb, "\\u%04x", c)
	}
	sb.WriteByte('"')
	return sb.String()
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
