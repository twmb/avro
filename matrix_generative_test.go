package avro_test

import (
	"bytes"
	"fmt"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// ===========================================================================
// THE axis-complete generative codec matrix.
//
// One generator, not hand-cases. The axes are tables; a new failing cell is a
// generator gap fixed by extending a table, never a hand-written round. This
// file RECONCILES (consumes + extends) the existing generative infrastructure
// rather than duplicating it:
//
//   - matFrags / matCtxs / runCore / matEqual / uniq  (matrix_core_test.go)
//   - typedFrags / typedPositions                     (matrix_typed_test.go)
//   - customFrags / customPositions                   (matrix_custom_test.go)
//   - recShapes                                       (matrix_recursion_test.go)
//   - resurrectionCells                               (custom_resurrection_parity_test.go)
//
// and adds the axes those omit: underlying-validity {valid, wrong-kind,
// wrong-size}, CustomType config {absent, passive, encode-only, decode-only,
// both}, and the boundary values current generators skip {2^53±1, MaxInt64/
// MinInt64, ±Inf, signaling-NaN, empty, large}.
//
// Per generated cell the matrix asserts THREE things:
//   (a) every codec path — binary-safe (generic), binary-unsafe (addressable
//       struct), JSON, resolved — plus the []T / map[string]T container
//       specializations agree byte-identically (within a wire format) and
//       value-identically (across formats), AND match an INDEPENDENT wire
//       oracle where one can be computed (the calibration-free runCore cannot
//       see encode-side canonicalization; the oracle can — e.g. a float32
//       encoder that quiets a signaling NaN);
//   (b) the wire round-trips through the schema's OWN reader (natural and
//       identity-resolved);
//   (c) parse-time / metadata-API observations (Root().Props, Fields[].Default,
//       Canonical, Fingerprint) agree with the wire and are deterministic.
// ===========================================================================

// gval is one boundary-classified value for a type: the generic (any-tree)
// form the reflect path consumes, plus an optional strongly-typed Go form for
// the unsafe/typed path, plus an optional independent wire oracle (the exact
// Avro binary bytes for the value at TOP context, computed without the code
// under test).
type gval struct {
	boundary string // normal | 2^53 | maxint | inf | snan | nzero | empty | large | ...
	generic  any    // the form the generic/reflect path encodes
	typed    any    // strongly-typed Go form (nil => same as generic)
	oracle   []byte // independent top-context wire bytes (nil => no oracle)
	// jsonLossy marks a value whose exact BINARY wire is provably not
	// representable in Avro JSON text: every NaN — quiet, signaling, any
	// payload — encodes to the single token "NaN" and decodes back to one
	// canonical quiet NaN (Java convention). The binary path stays bit-exact
	// (the oracle checks that); the JSON path round-trips only to a
	// value-equal NaN, never the same wire. This is a fact about the format,
	// not an assumption about inputs.
	jsonLossy bool
}

// gtype is one Avro type in its spec-VALID placement, enriched with the
// boundary-value axis the legacy frag tables omit.
type gtype struct {
	label  string
	kind   string // ctx.skip + token-class key (same vocabulary as matFrags kinds)
	schema func(u *uniq) string
	values []gval
}

// ---- independent wire-oracle builders (no code-under-test) -----------------

func leF32(f float32) []byte {
	b := math.Float32bits(f)
	return []byte{byte(b), byte(b >> 8), byte(b >> 16), byte(b >> 24)}
}

func leF64(f float64) []byte {
	b := math.Float64bits(f)
	return []byte{byte(b), byte(b >> 8), byte(b >> 16), byte(b >> 24), byte(b >> 32), byte(b >> 40), byte(b >> 48), byte(b >> 56)}
}

// avroLen prefixes b with its zigzag-varlong length, as a bytes/string wire.
func avroLen(b []byte) []byte { return append(appendZig(nil, int64(len(b))), b...) }

// signaling NaNs: exponent all ones, top mantissa bit CLEAR, a low bit set.
// math.NaN() is quiet (top mantissa bit set); these are a distinct bit pattern
// a NaN-canonicalizing encoder would silently rewrite.
var (
	sNaN32 = math.Float32frombits(0x7f800001)
	sNaN64 = math.Float64frombits(0x7ff0000000000001)
)

func gtypes() []gtype {
	return []gtype{
		{"null", "null", func(*uniq) string { return `"null"` }, []gval{
			{boundary: "normal", generic: nil, oracle: []byte{}},
		}},
		{"boolean", "boolean", func(*uniq) string { return `"boolean"` }, []gval{
			{boundary: "true", generic: true, oracle: []byte{0x01}},
			{boundary: "false", generic: false, oracle: []byte{0x00}},
		}},
		{"int", "int", func(*uniq) string { return `"int"` }, []gval{
			{boundary: "zero", generic: int32(0), oracle: appendZig(nil, 0)},
			{boundary: "one", generic: int32(1), oracle: appendZig(nil, 1)},
			{boundary: "neg", generic: int32(-1), oracle: appendZig(nil, -1)},
			{boundary: "maxint", generic: int32(math.MaxInt32), oracle: appendZig(nil, math.MaxInt32)},
			{boundary: "minint", generic: int32(math.MinInt32), oracle: appendZig(nil, math.MinInt32)},
		}},
		{"long", "long", func(*uniq) string { return `"long"` }, []gval{
			{boundary: "zero", generic: int64(0), oracle: appendZig(nil, 0)},
			{boundary: "neg", generic: int64(-1), oracle: appendZig(nil, -1)},
			{boundary: "2^53-1", generic: int64(1<<53 - 1), oracle: appendZig(nil, 1<<53-1)},
			{boundary: "2^53", generic: int64(1 << 53), oracle: appendZig(nil, 1<<53)},
			{boundary: "2^53+1", generic: int64(1<<53 + 1), oracle: appendZig(nil, 1<<53+1)},
			{boundary: "maxint", generic: int64(math.MaxInt64), oracle: appendZig(nil, math.MaxInt64)},
			{boundary: "minint", generic: int64(math.MinInt64), oracle: appendZig(nil, math.MinInt64)},
		}},
		{"float", "float", func(*uniq) string { return `"float"` }, []gval{
			{boundary: "zero", generic: float32(0), oracle: leF32(0)},
			{boundary: "nzero", generic: float32(math.Copysign(0, -1)), oracle: leF32(float32(math.Copysign(0, -1)))},
			{boundary: "normal", generic: float32(1.5), oracle: leF32(1.5)},
			{boundary: "inf", generic: float32(math.Inf(1)), oracle: leF32(float32(math.Inf(1)))},
			{boundary: "ninf", generic: float32(math.Inf(-1)), oracle: leF32(float32(math.Inf(-1)))},
			{boundary: "qnan", generic: float32(math.NaN()), oracle: leF32(float32(math.NaN()))},
			{boundary: "snan", generic: sNaN32, oracle: leF32(sNaN32), jsonLossy: true},
			{boundary: "smallest", generic: float32(math.SmallestNonzeroFloat32), oracle: leF32(math.SmallestNonzeroFloat32)},
			{boundary: "max", generic: float32(math.MaxFloat32), oracle: leF32(math.MaxFloat32)},
		}},
		{"double", "double", func(*uniq) string { return `"double"` }, []gval{
			{boundary: "zero", generic: float64(0), oracle: leF64(0)},
			{boundary: "nzero", generic: math.Copysign(0, -1), oracle: leF64(math.Copysign(0, -1))},
			{boundary: "normal", generic: 1.5, oracle: leF64(1.5)},
			{boundary: "inf", generic: math.Inf(1), oracle: leF64(math.Inf(1))},
			{boundary: "ninf", generic: math.Inf(-1), oracle: leF64(math.Inf(-1))},
			{boundary: "qnan", generic: math.NaN(), oracle: leF64(math.NaN())},
			{boundary: "snan", generic: sNaN64, oracle: leF64(sNaN64), jsonLossy: true},
			{boundary: "smallest", generic: math.SmallestNonzeroFloat64, oracle: leF64(math.SmallestNonzeroFloat64)},
			{boundary: "max", generic: math.MaxFloat64, oracle: leF64(math.MaxFloat64)},
		}},
		{"string", "string", func(*uniq) string { return `"string"` }, []gval{
			{boundary: "empty", generic: "", oracle: avroLen(nil)},
			{boundary: "ascii", generic: "a", oracle: avroLen([]byte("a"))},
			{boundary: "unicode", generic: "héllo 日本 🎉", oracle: avroLen([]byte("héllo 日本 🎉"))},
			{boundary: "nul", generic: "\x00nul", oracle: avroLen([]byte("\x00nul"))},
			{boundary: "large", generic: strings.Repeat("x", 70000), oracle: avroLen([]byte(strings.Repeat("x", 70000)))},
		}},
		{"bytes", "bytes", func(*uniq) string { return `"bytes"` }, []gval{
			{boundary: "empty", generic: []byte{}, oracle: avroLen(nil)},
			{boundary: "highbytes", generic: []byte{0xFF, 0x00, 0x7F}, oracle: avroLen([]byte{0xFF, 0x00, 0x7F})},
			{boundary: "large", generic: bytes.Repeat([]byte{0xAB}, 70000), oracle: avroLen(bytes.Repeat([]byte{0xAB}, 70000))},
		}},
		{"enum3", "enum", func(u *uniq) string {
			return fmt.Sprintf(`{"type":"enum","name":%q,"symbols":["A","B","C"]}`, u.name("GE"))
		}, []gval{
			{boundary: "first", generic: "A", oracle: appendZig(nil, 0)},
			{boundary: "last", generic: "C", oracle: appendZig(nil, 2)},
		}},
		{"fixed0", "fixed", func(u *uniq) string {
			return fmt.Sprintf(`{"type":"fixed","name":%q,"size":0}`, u.name("GF0"))
		}, []gval{
			{boundary: "empty", generic: []byte{}, oracle: []byte{}},
		}},
		{"fixed16", "fixed", func(u *uniq) string {
			return fmt.Sprintf(`{"type":"fixed","name":%q,"size":16}`, u.name("GF16"))
		}, []gval{
			{boundary: "zero", generic: make([]byte, 16), oracle: make([]byte, 16)},
			{boundary: "high", generic: bytes.Repeat([]byte{0xFF}, 16), oracle: bytes.Repeat([]byte{0xFF}, 16)},
		}},
		// ---- logicals in their SPEC-VALID placement (enriched round-trip) ----
		{"uuid-string", "uuid", func(*uniq) string { return `{"type":"string","logicalType":"uuid"}` }, []gval{
			{boundary: "normal", generic: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", oracle: avroLen([]byte("6ba7b810-9dad-11d1-80b4-00c04fd430c8"))},
		}},
		{"date", "date", func(*uniq) string { return `{"type":"int","logicalType":"date"}` }, []gval{
			{boundary: "epoch", generic: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), oracle: appendZig(nil, 0)},
			{boundary: "pre-epoch", generic: time.Date(1969, 7, 20, 0, 0, 0, 0, time.UTC)},
			{boundary: "far", generic: time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC)},
		}},
		{"timestamp-millis", "timestamp-millis", func(*uniq) string { return `{"type":"long","logicalType":"timestamp-millis"}` }, []gval{
			{boundary: "normal", generic: time.UnixMilli(1717243496789).UTC()},
			{boundary: "epoch", generic: time.UnixMilli(0).UTC(), oracle: appendZig(nil, 0)},
		}},
		{"timestamp-micros", "timestamp-micros", func(*uniq) string { return `{"type":"long","logicalType":"timestamp-micros"}` }, []gval{
			{boundary: "normal", generic: time.UnixMicro(1717243496789012).UTC()},
		}},
		{"decimal-bytes", "decimal", func(*uniq) string {
			return `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
		}, []gval{
			{boundary: "normal", generic: big.NewRat(12345, 100)},
			{boundary: "neg", generic: big.NewRat(-1, 4)},
			{boundary: "zero", generic: big.NewRat(0, 1)},
		}},
		{"decimal-fixed", "decimal", func(u *uniq) string {
			return fmt.Sprintf(`{"type":"fixed","name":%q,"size":8,"logicalType":"decimal","precision":10,"scale":2}`, u.name("GDF"))
		}, []gval{
			{boundary: "normal", generic: big.NewRat(99999, 100)},
			{boundary: "neg", generic: big.NewRat(-12345, 100)},
		}},
		{"duration", "duration", func(u *uniq) string {
			return fmt.Sprintf(`{"type":"fixed","name":%q,"size":12,"logicalType":"duration"}`, u.name("GDUR"))
		}, []gval{
			{boundary: "normal", generic: avro.Duration{Months: 1, Days: 2, Milliseconds: 3}},
			{boundary: "zero", generic: avro.Duration{}},
			{boundary: "maxu32", generic: avro.Duration{Months: math.MaxUint32, Days: math.MaxUint32, Milliseconds: math.MaxUint32}},
		}},
		{"big-decimal", "big-decimal", func(*uniq) string { return `{"type":"bytes","logicalType":"big-decimal"}` }, []gval{
			{boundary: "normal", generic: big.NewRat(314159, 100000)},
			{boundary: "neg", generic: big.NewRat(-7, 8)},
		}},
		// ---- containers ----
		{"rec2", "record", func(u *uniq) string {
			return fmt.Sprintf(`{"type":"record","name":%q,"fields":[{"name":"x","type":"int"},{"name":"y","type":"string"}]}`, u.name("GRec"))
		}, []gval{
			{boundary: "normal", generic: map[string]any{"x": int32(7), "y": "v"}},
		}},
		{"rec0", "record", func(u *uniq) string {
			return fmt.Sprintf(`{"type":"record","name":%q,"fields":[]}`, u.name("GEmpty"))
		}, []gval{
			{boundary: "empty", generic: map[string]any{}, oracle: []byte{}},
		}},
		{"arr-int", "array", func(*uniq) string { return `{"type":"array","items":"int"}` }, []gval{
			{boundary: "empty", generic: []any{}, oracle: []byte{0x00}},
			{boundary: "some", generic: []any{int32(1), int32(2), int32(3)}},
		}},
		{"map-str", "map", func(*uniq) string { return `{"type":"map","values":"string"}` }, []gval{
			{boundary: "empty", generic: map[string]any{}, oracle: []byte{0x00}},
			{boundary: "one", generic: map[string]any{"k": "v"}},
		}},
	}
}

// gMetadata asserts (c) for the parse-time / metadata-API surface: Canonical()
// is deterministic and Canonical-equality implies Fingerprint-equality, and the
// metadata rebuild Root().Schema() preserves both (runCore already checks the
// rebuild's wire; this adds the canonical/fingerprint determinism axis).
func gMetadata(t *testing.T, schemaJSON string) {
	t.Helper()
	s := avro.MustParse(schemaJSON)
	c1 := s.Canonical()
	s2 := avro.MustParse(schemaJSON)
	c2 := s2.Canonical()
	if !bytes.Equal(c1, c2) {
		t.Fatalf("Canonical() not deterministic:\n a=%s\n b=%s\nschema: %s", c1, c2, schemaJSON)
	}
	// Same canonical form => same Rabin fingerprint.
	if !bytes.Equal(s.Fingerprint(avro.NewRabin()), s2.Fingerprint(avro.NewRabin())) {
		t.Fatalf("equal Canonical but different Fingerprint\nschema: %s", schemaJSON)
	}
	// The metadata rebuild's canonical form must match the original's: Root()
	// observed structure agrees with the parsed wire schema.
	root := s.Root()
	rebuilt, err := root.Schema()
	if err != nil {
		t.Fatalf("Root().Schema(): %v\nschema: %s", err, schemaJSON)
	}
	if !bytes.Equal(c1, rebuilt.Canonical()) {
		t.Fatalf("rebuilt Canonical differs:\n orig=%s\n reb =%s\nschema: %s", c1, rebuilt.Canonical(), schemaJSON)
	}
}

// gNaNCell runs the binary-bit-exact battery for a jsonLossy (NaN-payload)
// value: binary is asserted bit-exact (independent oracle + stable re-encode +
// SOE + identity-resolved), JSON is asserted only value-equal (a NaN), never
// wire-stable. This is the provable split runCore's stricter JSON-wire check
// cannot express.
func gNaNCell(t *testing.T, schemaJSON string, top bool, mv gval, vin any) {
	t.Helper()
	s := avro.MustParse(schemaJSON)
	w1, err := s.AppendEncode(nil, vin)
	if err != nil {
		t.Fatalf("binEnc: %v", err)
	}
	if top && mv.oracle != nil && !bytes.Equal(w1, mv.oracle) {
		t.Fatalf("binary wire diverges from independent NaN oracle (canonicalized?):\n got=%x\nwant=%x", w1, mv.oracle)
	}
	var a1 any
	if _, err := s.Decode(w1, &a1); err != nil {
		t.Fatalf("binDec: %v", err)
	}
	w2, err := s.AppendEncode(nil, a1)
	if err != nil || !bytes.Equal(w2, w1) {
		t.Fatalf("binary re-encode not bit-stable for NaN payload: err=%v\n w1=%x\n w2=%x", err, w1, w2)
	}
	// SOE preserves the exact binary body.
	soe, err := s.AppendSingleObject(nil, a1)
	if err != nil {
		t.Fatalf("AppendSingleObject: %v", err)
	}
	var aSoe any
	if _, err := s.DecodeSingleObject(soe, &aSoe); err != nil || !matEqual(aSoe, a1) {
		t.Fatalf("SOE NaN round-trip: err=%v", err)
	}
	// Identity-resolved binary decode is bit-exact too.
	res, err := avro.Resolve(s, s)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	var ar any
	if _, err := res.Decode(w1, &ar); err != nil {
		t.Fatalf("resolved decode: %v", err)
	}
	wr, err := s.AppendEncode(nil, ar)
	if err != nil || !bytes.Equal(wr, w1) {
		t.Fatalf("resolved NaN re-encode not bit-stable: err=%v\n w1=%x\n wr=%x", err, w1, wr)
	}
	// JSON: round-trips to a value-equal NaN (the achievable invariant), not
	// the same wire — the format cannot carry the payload.
	j1, err := s.AppendEncodeJSON(nil, a1)
	if err != nil {
		t.Fatalf("jsonEnc: %v", err)
	}
	var aj any
	if err := s.DecodeJSON(j1, &aj); err != nil {
		t.Fatalf("jsonDec: %v\n j=%s", err, j1)
	}
	if !matEqual(aj, a1) {
		t.Fatalf("JSON NaN round-trip not value-equal:\n bin=%#v\njson=%#v", a1, aj)
	}
}

// TestMatrix_Generative is the master cross: every type (boundary-rich) × every
// composition context × every boundary value, run through the calibration-free
// core battery (runCore: binary/JSON/resolved/rebuild/SOE/stream/append) plus
// the independent wire oracle and the metadata-API agreement.
func TestMatrix_Generative(t *testing.T) {
	ctxs := matCtxs()
	for _, gt := range gtypes() {
		for _, cx := range ctxs {
			if cx.skip != nil && cx.skip(gt.kind) {
				continue
			}
			t.Run(gt.label+"/"+cx.label, func(t *testing.T) {
				for _, mv := range gt.values {
					t.Run(mv.boundary, func(t *testing.T) {
						u := &uniq{}
						schema := cx.schema(gt.schema(u), gt.kind, u)
						vin := cx.wrap(mv.generic)
						if mv.jsonLossy {
							gNaNCell(t, schema, cx.label == "top", mv, vin)
							gMetadata(t, schema)
							return
						}
						runCore(t, schema, vin)
						gMetadata(t, schema)
						// Independent wire oracle: only at top context, where no
						// framing intervenes, and only when the value carries one.
						if cx.label == "top" && mv.oracle != nil {
							s := avro.MustParse(schema)
							w, err := s.AppendEncode(nil, mv.generic)
							if err != nil {
								t.Fatalf("encode for oracle: %v", err)
							}
							if !bytes.Equal(w, mv.oracle) {
								t.Fatalf("wire diverges from independent oracle (encode-side rewrite?):\n got=%x\nwant=%x\ntype=%s boundary=%s", w, mv.oracle, gt.label, mv.boundary)
							}
						}
					})
				}
			})
		}
	}
}
