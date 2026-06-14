package avro_test

import (
	"bytes"
	"fmt"
	"math"
	"math/big"
	"reflect"
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

// ===========================================================================
// Layer 2 — the typed/unsafe path and container specializations.
//
// Assertion (a) demands all four codec paths agree byte-identically. The
// binary-unsafe path (addressable struct fields, unsafe.go) and the per-element
// container fast paths ([]T, map[string]T) are reached only with strongly-typed
// Go targets. This layer drives every typed scalar through five positions —
// bare top, struct field (the unsafe fast path), []T and map[string]T (the
// container specializations), and *T (the pointer path) — at the boundary
// values the legacy typed table omits. The float32 signaling-NaN cell is the
// sharp one: the float32 encoder has a documented fast/slow split keyed on
// "float32→float64→float32 is bit-exact for all NON-NaN values", so a signaling
// NaN takes the slow path, and every typed position must still emit the exact
// payload — caught by the independent oracle, not calibration.
// ===========================================================================

// gtyped is one typed scalar: the field/element schema, the Go type for the
// unsafe/typed targets, and boundary-tagged values in typed + generic form.
type gtyped struct {
	label  string
	schema string
	goType reflect.Type
	values []gtval
}

type gtval struct {
	boundary  string
	typed     any    // assignable to goType
	generic   any    // generic-path equivalent (map[string]any/[]any/scalar)
	oracle    []byte // bare top-context wire bytes (nil => no independent oracle)
	jsonLossy bool   // NaN payload: binary bit-exact, JSON value-equal only
}

func gtypedTypes() []gtyped {
	rat := big.NewRat(123, 4)
	ts := time.Date(2024, 6, 1, 12, 34, 56, 789000000, time.UTC)
	uuid16 := [16]byte{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}
	return []gtyped{
		{"boolean", `"boolean"`, reflect.TypeOf(true), []gtval{
			{boundary: "true", typed: true, generic: true, oracle: []byte{0x01}},
			{boundary: "false", typed: false, generic: false, oracle: []byte{0x00}},
		}},
		{"int", `"int"`, reflect.TypeOf(int32(0)), []gtval{
			{boundary: "neg", typed: int32(-5), generic: int32(-5), oracle: appendZig(nil, -5)},
			{boundary: "maxint", typed: int32(math.MaxInt32), generic: int32(math.MaxInt32), oracle: appendZig(nil, math.MaxInt32)},
			{boundary: "minint", typed: int32(math.MinInt32), generic: int32(math.MinInt32), oracle: appendZig(nil, math.MinInt32)},
		}},
		{"int-as-int16", `"int"`, reflect.TypeOf(int16(0)), []gtval{
			{boundary: "normal", typed: int16(300), generic: int32(300), oracle: appendZig(nil, 300)},
			{boundary: "minint16", typed: int16(math.MinInt16), generic: int32(math.MinInt16), oracle: appendZig(nil, math.MinInt16)},
		}},
		{"long", `"long"`, reflect.TypeOf(int64(0)), []gtval{
			{boundary: "2^53+1", typed: int64(1<<53 + 1), generic: int64(1<<53 + 1), oracle: appendZig(nil, 1<<53+1)},
			{boundary: "maxint", typed: int64(math.MaxInt64), generic: int64(math.MaxInt64), oracle: appendZig(nil, math.MaxInt64)},
			{boundary: "minint", typed: int64(math.MinInt64), generic: int64(math.MinInt64), oracle: appendZig(nil, math.MinInt64)},
		}},
		{"long-as-uint32", `"long"`, reflect.TypeOf(uint32(0)), []gtval{
			{boundary: "big", typed: uint32(4000000000), generic: int64(4000000000), oracle: appendZig(nil, 4000000000)},
		}},
		{"float", `"float"`, reflect.TypeOf(float32(0)), []gtval{
			{boundary: "normal", typed: float32(2.5), generic: float32(2.5), oracle: leF32(2.5)},
			{boundary: "nzero", typed: float32(math.Copysign(0, -1)), generic: float32(math.Copysign(0, -1)), oracle: leF32(float32(math.Copysign(0, -1)))},
			{boundary: "inf", typed: float32(math.Inf(1)), generic: float32(math.Inf(1)), oracle: leF32(float32(math.Inf(1)))},
			{boundary: "qnan", typed: float32(math.NaN()), generic: float32(math.NaN()), oracle: leF32(float32(math.NaN()))},
			{boundary: "snan", typed: sNaN32, generic: sNaN32, oracle: leF32(sNaN32), jsonLossy: true},
			{boundary: "max", typed: float32(math.MaxFloat32), generic: float32(math.MaxFloat32), oracle: leF32(math.MaxFloat32)},
		}},
		{"double", `"double"`, reflect.TypeOf(float64(0)), []gtval{
			{boundary: "normal", typed: 6.25, generic: 6.25, oracle: leF64(6.25)},
			{boundary: "nzero", typed: math.Copysign(0, -1), generic: math.Copysign(0, -1), oracle: leF64(math.Copysign(0, -1))},
			{boundary: "inf", typed: math.Inf(-1), generic: math.Inf(-1), oracle: leF64(math.Inf(-1))},
			{boundary: "snan", typed: sNaN64, generic: sNaN64, oracle: leF64(sNaN64), jsonLossy: true},
			{boundary: "max", typed: math.MaxFloat64, generic: math.MaxFloat64, oracle: leF64(math.MaxFloat64)},
		}},
		{"string", `"string"`, reflect.TypeOf(""), []gtval{
			{boundary: "normal", typed: "typ", generic: "typ", oracle: avroLen([]byte("typ"))},
			{boundary: "empty", typed: "", generic: "", oracle: avroLen(nil)},
		}},
		{"bytes", `"bytes"`, reflect.TypeOf([]byte(nil)), []gtval{
			{boundary: "normal", typed: []byte{9, 8}, generic: []byte{9, 8}, oracle: avroLen([]byte{9, 8})},
			{boundary: "empty", typed: []byte{}, generic: []byte{}, oracle: avroLen(nil)},
		}},
		{"enum", `{"type":"enum","name":"GTYE","symbols":["A","B"]}`, reflect.TypeOf(""), []gtval{
			{boundary: "B", typed: "B", generic: "B", oracle: appendZig(nil, 1)},
		}},
		{"fixed2", `{"type":"fixed","name":"GTYF","size":2}`, reflect.TypeOf([2]byte{}), []gtval{
			{boundary: "normal", typed: [2]byte{1, 2}, generic: []byte{1, 2}, oracle: []byte{1, 2}},
		}},
		{"uuid-fixed16", `{"type":"fixed","name":"GTYU","size":16,"logicalType":"uuid"}`, reflect.TypeOf([16]byte{}), []gtval{
			{boundary: "normal", typed: uuid16, generic: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", oracle: uuid16[:]},
		}},
		{"date", `{"type":"int","logicalType":"date"}`, reflect.TypeOf(time.Time{}), []gtval{
			{boundary: "normal", typed: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), generic: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		}},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`, reflect.TypeOf(time.Duration(0)), []gtval{
			{boundary: "normal", typed: 3 * time.Hour, generic: 3 * time.Hour},
		}},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, reflect.TypeOf(time.Time{}), []gtval{
			{boundary: "normal", typed: ts, generic: ts},
		}},
		{"decimal", `{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}`, reflect.TypeOf(&big.Rat{}), []gtval{
			{boundary: "normal", typed: rat, generic: rat},
		}},
		{"duration", `{"type":"fixed","name":"GTYD","size":12,"logicalType":"duration"}`, reflect.TypeOf(avro.Duration{}), []gtval{
			{boundary: "normal", typed: avro.Duration{Months: 3, Days: 1, Milliseconds: 9}, generic: avro.Duration{Months: 3, Days: 1, Milliseconds: 9}},
		}},
	}
}

// gEncEq encodes v against s on the binary path and asserts equality to want,
// returning the wire for further checks.
func gEncEq(t *testing.T, s *avro.Schema, v any, want []byte, what string) []byte {
	t.Helper()
	w, err := s.AppendEncode(nil, v)
	if err != nil {
		t.Fatalf("%s: encode: %v", what, err)
	}
	if want != nil && !bytes.Equal(w, want) {
		t.Fatalf("%s: wire mismatch:\n got=%x\nwant=%x", what, w, want)
	}
	return w
}

// gTypedCell drives one typed scalar value through the bare, struct (unsafe),
// []T, map[string]T, and *T positions, asserting byte-identity across the safe,
// unsafe, generic, and container paths plus the independent oracle, and the
// JSON twin parity. The jsonLossy (NaN payload) split drops only the JSON→wire
// re-encode step, never the binary bit-exactness.
func gTypedCell(t *testing.T, gd gtyped, tv gtval) {
	t.Helper()

	// ---- P1: bare top scalar (the typed scalar encoder; float32 slow path). ----
	sTop := avro.MustParse(gd.schema)
	wTop := gEncEq(t, sTop, tv.typed, tv.oracle, "bare-typed")
	wGen := gEncEq(t, sTop, tv.generic, tv.oracle, "bare-generic")
	if !bytes.Equal(wTop, wGen) {
		t.Fatalf("bare typed vs generic differ:\n t=%x\n g=%x", wTop, wGen)
	}
	// Decode into a fresh typed target, re-encode byte-stable.
	backTop := reflect.New(gd.goType)
	if _, err := sTop.Decode(wTop, backTop.Interface()); err != nil {
		t.Fatalf("bare typed decode: %v", err)
	}
	gEncEq(t, sTop, backTop.Elem().Interface(), wTop, "bare typed re-encode")

	// ---- P2: struct field (addressable => unsafe fast path; non-addr => reflect). ----
	st := reflect.StructOf([]reflect.StructField{{Name: "F", Type: gd.goType, Tag: `avro:"f"`}})
	recSchema := fmt.Sprintf(`{"type":"record","name":"GS","fields":[{"name":"f","type":%s}]}`, gd.schema)
	sRec := avro.MustParse(recSchema)
	pStruct := reflect.New(st)
	pStruct.Elem().Field(0).Set(reflect.ValueOf(tv.typed))
	wAddr := gEncEq(t, sRec, pStruct.Interface(), nil, "struct addressable (unsafe)")     // *struct => addressable
	wNon := gEncEq(t, sRec, pStruct.Elem().Interface(), nil, "struct non-addressable")    // struct value => reflect
	wRecGen := gEncEq(t, sRec, map[string]any{"f": tv.generic}, nil, "struct generic")
	if !bytes.Equal(wAddr, wNon) || !bytes.Equal(wAddr, wRecGen) {
		t.Fatalf("struct safe/unsafe/generic diverge:\n addr=%x\n non =%x\n gen =%x", wAddr, wNon, wRecGen)
	}
	backStruct := reflect.New(st)
	if _, err := sRec.Decode(wAddr, backStruct.Interface()); err != nil {
		t.Fatalf("struct typed decode: %v", err)
	}
	gEncEq(t, sRec, backStruct.Interface(), wAddr, "struct decode→re-encode")

	// ---- P3: []T container specialization. ----
	arrSchema := avro.MustParse(fmt.Sprintf(`{"type":"array","items":%s}`, gd.schema))
	slice := reflect.MakeSlice(reflect.SliceOf(gd.goType), 0, 2)
	slice = reflect.Append(slice, reflect.ValueOf(tv.typed), reflect.ValueOf(tv.typed))
	wSlice := gEncEq(t, arrSchema, slice.Interface(), nil, "[]T")
	wSliceGen := gEncEq(t, arrSchema, []any{tv.generic, tv.generic}, nil, "[]any")
	if !bytes.Equal(wSlice, wSliceGen) {
		t.Fatalf("[]T vs []any diverge:\n t=%x\n g=%x", wSlice, wSliceGen)
	}
	backSlice := reflect.New(reflect.SliceOf(gd.goType))
	if _, err := arrSchema.Decode(wSlice, backSlice.Interface()); err != nil {
		t.Fatalf("[]T decode: %v", err)
	}
	gEncEq(t, arrSchema, backSlice.Interface(), wSlice, "[]T decode→re-encode")

	// ---- P4: map[string]T container specialization. ----
	mapSchema := avro.MustParse(fmt.Sprintf(`{"type":"map","values":%s}`, gd.schema))
	mt := reflect.MapOf(reflect.TypeOf(""), gd.goType)
	m := reflect.MakeMap(mt)
	m.SetMapIndex(reflect.ValueOf("k"), reflect.ValueOf(tv.typed))
	wMap := gEncEq(t, mapSchema, m.Interface(), nil, "map[string]T")
	wMapGen := gEncEq(t, mapSchema, map[string]any{"k": tv.generic}, nil, "map[string]any")
	if !bytes.Equal(wMap, wMapGen) {
		t.Fatalf("map[string]T vs map[string]any diverge:\n t=%x\n g=%x", wMap, wMapGen)
	}
	backMap := reflect.New(mt)
	if _, err := mapSchema.Decode(wMap, backMap.Interface()); err != nil {
		t.Fatalf("map[string]T decode: %v", err)
	}
	gEncEq(t, mapSchema, backMap.Interface(), wMap, "map[string]T decode→re-encode")

	// ---- P5: *T via a ["null",T] union (pointer typed path). ----
	if gd.label != "bytes" { // a nil []byte is the null branch already; *[]byte is redundant
		ptrUnionSchema := avro.MustParse(fmt.Sprintf(`["null",%s]`, gd.schema))
		p := reflect.New(gd.goType)
		p.Elem().Set(reflect.ValueOf(tv.typed))
		wPtr := gEncEq(t, ptrUnionSchema, p.Interface(), nil, "*T")
		wPtrGen := gEncEq(t, ptrUnionSchema, tv.generic, nil, "*T generic")
		if !bytes.Equal(wPtr, wPtrGen) {
			t.Fatalf("*T vs generic diverge:\n t=%x\n g=%x", wPtr, wPtrGen)
		}
	}

	// ---- JSON twins: typed-JSON == generic-JSON byte-identical (true even for
	// NaN — both emit "NaN"); for non-lossy also assert JSON→binary lands on the
	// original wire. ----
	jTyped, err := sRec.AppendEncodeJSON(nil, pStruct.Interface())
	if err != nil {
		t.Fatalf("struct typed encodeJSON: %v", err)
	}
	jGen, err := sRec.AppendEncodeJSON(nil, map[string]any{"f": tv.generic})
	if err != nil || !bytes.Equal(jTyped, jGen) {
		t.Fatalf("typed vs generic JSON differ: err=%v\n t=%s\n g=%s", err, jTyped, jGen)
	}
	jBack := reflect.New(st)
	if err := sRec.DecodeJSON(jTyped, jBack.Interface()); err != nil {
		t.Fatalf("struct typed decodeJSON: %v", err)
	}
	if !tv.jsonLossy {
		wj, err := sRec.AppendEncode(nil, jBack.Interface())
		if err != nil || !bytes.Equal(wj, wAddr) {
			t.Fatalf("typed JSON round-trip wire differs: err=%v\n w=%x\n j=%x", err, wAddr, wj)
		}
	}
}

func TestMatrix_GenerativeTyped(t *testing.T) {
	for _, gd := range gtypedTypes() {
		for _, tv := range gd.values {
			t.Run(gd.label+"/"+tv.boundary, func(t *testing.T) {
				gTypedCell(t, gd, tv)
			})
		}
	}
}

// ===========================================================================
// Layer 3a — resurrection regime × CONTEXT axis.
//
// A logical placed on an underlying it is not spec-valid for is soft-dropped to
// the bare underlying (validateLogical) UNLESS a CustomType with the matching
// LogicalType resurrects it. The contract: a resurrected wrong-kind/wrong-size
// logical must fall through to the RAW kind/size-checked codec on EVERY axis.
// custom_resurrection_parity_test.go proves this at TOP level across encode/
// decode × binary/JSON × natural/resolved × targets × three matching shapes.
//
// This layer adds the axis that file omits: COMPOSITION CONTEXT. A wrong-kind
// logical as an array element, map value, union branch, record field, or nested
// field reaches the per-element / per-branch fast paths — a different dispatch
// than the top-level codec — where a re-applied logical ser/deser would surface
// as a wire-byte or value divergence from the plain (soft-dropped) schema.
//
// Oracle: the PLAIN schema (same JSON, no CustomType). For every resurrecting
// shape the custom schema must be encode/decode-identical to plain in every
// context, and its wire must read back through its own natural and identity-
// resolved readers. Reuses resurrectionCells() and the encResult/decBin/decJSON
// helpers; *any decode targets catch a wrongly-enriched value (it appears as a
// logical Go type in the tree where plain yields the raw underlying).
// ===========================================================================

func TestRegression_CustomResurrectedLogicalInContext(t *testing.T) {
	ctxs := []struct {
		label  string
		schema func(inner string) string
		wrap   func(v any) any
	}{
		{"field", func(in string) string {
			return fmt.Sprintf(`{"type":"record","name":"RC","fields":[{"name":"a","type":"long"},{"name":"f","type":%s}]}`, in)
		}, func(v any) any { return map[string]any{"a": int64(3), "f": v} }},
		{"array", func(in string) string {
			return fmt.Sprintf(`{"type":"array","items":%s}`, in)
		}, func(v any) any { return []any{v, v} }},
		{"map", func(in string) string {
			return fmt.Sprintf(`{"type":"map","values":%s}`, in)
		}, func(v any) any { return map[string]any{"k": v} }},
		{"union", func(in string) string {
			return fmt.Sprintf(`["null",%s]`, in)
		}, func(v any) any { return v }},
		{"nested", func(in string) string {
			return fmt.Sprintf(`{"type":"record","name":"RO","fields":[{"name":"o","type":{"type":"record","name":"RI","fields":[{"name":"f","type":%s}]}}]}`, in)
		}, func(v any) any { return map[string]any{"o": map[string]any{"f": v}} }},
	}
	anyTgt := func() any { return new(any) }
	for _, c := range resurrectionCells() {
		for _, cx := range ctxs {
			schema := cx.schema(c.schema)
			// Skip a context that cannot hold this cell's underlying (e.g. a
			// composed schema that fails to parse); none expected, but guard.
			if _, err := avro.Parse(schema); err != nil {
				continue
			}
			for _, sh := range []struct {
				name string
				opt  avro.SchemaOpt
			}{
				{"wildcard", avro.CustomType{LogicalType: c.logical}},
				{"avrotype-match", avro.CustomType{LogicalType: c.logical, AvroType: c.kind}},
				{"avrotype-mismatch", avro.CustomType{LogicalType: c.logical, AvroType: "boolean"}},
			} {
				t.Run(c.name+"/"+cx.label+"/"+sh.name, func(t *testing.T) {
					plain := avro.MustParse(schema)
					cs, err := avro.Parse(schema, sh.opt)
					if err != nil {
						t.Fatalf("parse custom: %v\nschema: %s", err, schema)
					}
					plainR := mustIdentityResolve(t, plain)
					csR := mustIdentityResolve(t, cs)
					for _, in := range c.inputs {
						v := cx.wrap(in)
						pbin, peb := plain.Encode(v)
						cbin, ceb := cs.Encode(v)
						if got, want := encResult(cbin, ceb), encResult(pbin, peb); got != want {
							t.Errorf("binary encode %T in %s: custom=%s plain=%s — logical ser applied to wrong kind/size", in, cx.label, got, want)
						}
						pjsn, pej := plain.EncodeJSON(v)
						cjsn, cej := cs.EncodeJSON(v)
						if got, want := encResult(cjsn, cej), encResult(pjsn, pej); got != want {
							t.Errorf("JSON encode %T in %s: custom=%q plain=%q — logical ser applied to wrong kind/size", in, cx.label, got, want)
						}
						if peb == nil && ceb == nil {
							if got, want := decBin(cs, cbin, anyTgt), decBin(plain, pbin, anyTgt); got != want {
								t.Errorf("binary decode natural %T in %s: custom=%s plain=%s — logical deser applied to wrong kind/size", in, cx.label, got, want)
							}
							if got, want := decBin(csR, cbin, anyTgt), decBin(plainR, pbin, anyTgt); got != want {
								t.Errorf("binary decode RESOLVED %T in %s: custom=%s plain=%s", in, cx.label, got, want)
							}
							var sink any
							if _, err := cs.Decode(cbin, &sink); err != nil {
								t.Errorf("custom binary wire (%T in %s) not self-readable: %v", in, cx.label, err)
							}
						}
						if pej == nil && cej == nil {
							if got, want := decJSON(cs, cjsn, anyTgt), decJSON(plain, pjsn, anyTgt); got != want {
								t.Errorf("JSON decode natural %T in %s: custom=%s plain=%s — logical deser applied to wrong kind/size", in, cx.label, got, want)
							}
							if got, want := decJSON(csR, cjsn, anyTgt), decJSON(plainR, pjsn, anyTgt); got != want {
								t.Errorf("JSON decode RESOLVED %T in %s: custom=%s plain=%s", in, cx.label, got, want)
							}
							var sink any
							if err := cs.DecodeJSON(cjsn, &sink); err != nil {
								t.Errorf("custom JSON wire (%T in %s) not self-readable: %v", in, cx.label, err)
							}
						}
					}
				})
			}
		}
	}
}

// ===========================================================================
// Layer 3b — the CustomType callback-config axis on VALID logicals.
//
// The five configs are {absent, passive, encode-only, decode-only, both}.
// absent (built-in both ways) is covered by the gtypes round-trip; passive
// (suppress), both (box), and count are covered by matrix_custom_test.go. This
// layer adds the two it omits — encode-only and decode-only — which exercise
// the ASYMMETRIC suppression gates (hasMatchingCustomTypeWithEncode keys on
// Encode!=nil for the encoder; hasMatchingCustomType keys on any non-wildcard
// match for the decoder).
//
// Oracles, anchored so the fixed/decimal JSON-suppression nuance can't false-
// fail: the PLAIN schema's wire (built-in) and the PASSIVE schema's raw decode
// (calibration). A cbox callback proves the custom side actually fired.
// ===========================================================================

func TestMatrix_GenerativeCustomConfigs(t *testing.T) {
	notEnriched := func(t *testing.T, v any, what string) {
		t.Helper()
		switch v.(type) {
		case time.Time, time.Duration, *big.Rat, avro.Duration:
			t.Fatalf("%s: raw value is enriched %T (logical deser fired where it must not)", what, v)
		}
	}
	for _, fr := range customFrags() {
		for _, pos := range customPositions() {
			posSchema := pos.schema(fr.schema)
			if pos.label == "multibranch" {
				posSchema = fmt.Sprintf(`["null","boolean",%s,%s]`, fr.schema, customPad(fr))
				if _, err := avro.Parse(posSchema); err != nil {
					continue
				}
			}
			plain := avro.MustParse(posSchema)
			vin := pos.wrap(fr.enriched)
			plainWire, err := plain.AppendEncode(nil, vin)
			if err != nil {
				t.Fatalf("%s/%s plain encode: %v", fr.label, pos.label, err)
			}
			plainJSON, err := plain.AppendEncodeJSON(nil, vin)
			if err != nil {
				t.Fatalf("%s/%s plain encodeJSON: %v", fr.label, pos.label, err)
			}
			// Calibrate the raw underlying tree via a passive (suppress) decode.
			passive := avro.MustParse(posSchema, avro.CustomType{LogicalType: fr.logical})
			var rawTree any
			if _, err := passive.Decode(plainWire, &rawTree); err != nil {
				t.Fatalf("%s/%s raw calibration: %v", fr.label, pos.label, err)
			}

			// ---- decode-only: built-in encode (byte-identical to plain),
			// custom Decode boxes the RAW underlying. ----
			t.Run(fr.label+"/"+pos.label+"/decode-only", func(t *testing.T) {
				ct := avro.CustomType{
					LogicalType: fr.logical,
					Decode:      func(v any, _ *avro.SchemaNode) (any, error) { return cbox{Raw: v}, nil },
				}
				s := avro.MustParse(posSchema, ct)
				// Encode is built-in => byte-identical wire and JSON to plain.
				if w, err := s.AppendEncode(nil, vin); err != nil || !bytes.Equal(w, plainWire) {
					t.Fatalf("decode-only encode not built-in: err=%v\n got=%x\nwant=%x", err, w, plainWire)
				}
				if j, err := s.AppendEncodeJSON(nil, vin); err != nil || !bytes.Equal(j, plainJSON) {
					t.Fatalf("decode-only encodeJSON not built-in: err=%v\n got=%s\nwant=%s", err, j, plainJSON)
				}
				// Decode boxes the raw underlying on both wire formats, equally.
				var aBin any
				if _, err := s.Decode(plainWire, &aBin); err != nil {
					t.Fatalf("decode-only binary decode: %v", err)
				}
				boxBin, ok := pos.unwrap(aBin).(cbox)
				if !ok {
					t.Fatalf("decode-only did not box (binary): %T", pos.unwrap(aBin))
				}
				notEnriched(t, boxBin.Raw, "decode-only binary")
				var aJSON any
				if err := s.DecodeJSON(plainJSON, &aJSON); err != nil {
					t.Fatalf("decode-only JSON decode: %v", err)
				}
				boxJSON, ok := pos.unwrap(aJSON).(cbox)
				if !ok {
					t.Fatalf("decode-only did not box (JSON): %T", pos.unwrap(aJSON))
				}
				if !matEqual(boxBin.Raw, boxJSON.Raw) {
					t.Fatalf("decode-only binary/JSON raw diverge:\n bin=%#v\njson=%#v", boxBin.Raw, boxJSON.Raw)
				}
			})

			// ---- encode-only: custom Encode unboxes to raw (built-in encode
			// suppressed), Decode is raw (suppressed). ----
			t.Run(fr.label+"/"+pos.label+"/encode-only", func(t *testing.T) {
				ct := avro.CustomType{
					LogicalType: fr.logical,
					Encode: func(v any, _ *avro.SchemaNode) (any, error) {
						if b, ok := v.(cbox); ok {
							return b.Raw, nil
						}
						return nil, avro.ErrSkipCustomType
					},
				}
				s := avro.MustParse(posSchema, ct)
				// Decode is suppressed => raw, identical to the passive schema.
				var aBin any
				if _, err := s.Decode(plainWire, &aBin); err != nil {
					t.Fatalf("encode-only binary decode: %v", err)
				}
				notEnriched(t, pos.unwrap(aBin), "encode-only binary decode")
				if !matEqual(aBin, rawTree) {
					t.Fatalf("encode-only decode not raw:\n got=%#v\nraw=%#v", aBin, rawTree)
				}
				// Encode the boxed raw tree: unbox => base encode => plain wire.
				boxed := boxRawTree(pos, rawTree)
				if w, err := s.AppendEncode(nil, boxed); err != nil || !bytes.Equal(w, plainWire) {
					t.Fatalf("encode-only boxed encode not raw-equivalent: err=%v\n got=%x\nwant=%x", err, w, plainWire)
				}
				// JSON encode of the boxed raw tree round-trips back to raw.
				jb, err := s.AppendEncodeJSON(nil, boxed)
				if err != nil {
					t.Fatalf("encode-only boxed encodeJSON: %v", err)
				}
				var jBack any
				if err := s.DecodeJSON(jb, &jBack); err != nil {
					t.Fatalf("encode-only JSON round-trip decode: %v\n j=%s", err, jb)
				}
				if !matEqual(jBack, rawTree) {
					t.Fatalf("encode-only JSON round-trip not raw:\n got=%#v\nraw=%#v", jBack, rawTree)
				}
			})
		}
	}
}

// boxRawTree wraps the inner (unwrapped) raw value of a position's raw tree in a
// cbox, leaving the surrounding structure intact — the encode-only callback
// unboxes exactly that inner value.
func boxRawTree(pos customPos, rawTree any) any {
	inner := pos.unwrap(rawTree)
	boxedInner := cbox{Raw: inner}
	switch pos.label {
	case "top", "nullunion", "multibranch":
		return boxedInner
	case "field":
		return map[string]any{"a": int64(4), "f": boxedInner}
	case "array":
		return []any{boxedInner, boxedInner}
	}
	return boxedInner
}

// ===========================================================================
// Layer 4 — metadata-API agreement with the wire (assertion (c) completion).
//
// gMetadata pins Canonical/Fingerprint determinism. This layer pins the two
// remaining metadata surfaces the user named: Fields[].Default and Root().Props.
//
// Fields[].Default: the contract is that the metadata default value, used AS a
// field value, encodes to the SAME wire as resolution/auto fill — i.e. the
// observed default agrees with the wire. Crossed with the resolution default-
// fill (writer lacks the field) and Resolve⇔CheckCompatibility agreement.
// ===========================================================================

func TestMatrix_GenerativeDefaultFill(t *testing.T) {
	kinds := []struct {
		label      string
		fieldType  string
		defaultLit string
	}{
		{"boolean", `"boolean"`, `true`},
		{"int", `"int"`, `7`},
		{"long", `"long"`, `9007199254740993`},
		{"float", `"float"`, `1.5`},
		{"double", `"double"`, `-2.25`},
		{"string", `"string"`, `"d"`},
		{"bytes", `"bytes"`, `"\u00ff"`},
		{"bytes-empty", `"bytes"`, `""`},
		{"enum", `{"type":"enum","name":"GDE","symbols":["A","B"]}`, `"B"`},
		{"fixed1", `{"type":"fixed","name":"GDF","size":1}`, `"\u00ab"`},
		{"fixed0", `{"type":"fixed","name":"GDF0","size":0}`, `""`},
		{"date", `{"type":"int","logicalType":"date"}`, `19723`},
		{"timestamp", `{"type":"long","logicalType":"timestamp-millis"}`, `1717243496789`},
		{"nullunion", `["null","int"]`, `null`},
		{"union-int-first", `["int","string"]`, `42`},
		{"array", `{"type":"array","items":"int"}`, `[1,2]`},
		{"map", `{"type":"map","values":"string"}`, `{"k":"v"}`},
		{"record", `{"type":"record","name":"GDR","fields":[{"name":"i","type":"int"}]}`, `{"i":3}`},
		{"empty-record", `{"type":"record","name":"GDER","fields":[]}`, `{}`},
	}
	for _, k := range kinds {
		t.Run(k.label, func(t *testing.T) {
			rSchema := fmt.Sprintf(`{"type":"record","name":"R","fields":[
				{"name":"pre","type":"string"},
				{"name":"f","type":%s,"default":%s}]}`, k.fieldType, k.defaultLit)
			wSchema := `{"type":"record","name":"R","fields":[{"name":"pre","type":"string"}]}`
			r := avro.MustParse(rSchema)
			w := avro.MustParse(wSchema)
			res, err := resolveBoth(t, w, r)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}

			// Metadata observation: locate field "f" and its typed Default.
			root := r.Root()
			var fld *avro.SchemaField
			for i := range root.Fields {
				if root.Fields[i].Name == "f" {
					fld = &root.Fields[i]
				}
			}
			if fld == nil || !fld.HasDefault {
				t.Fatalf("metadata: field f missing or has no default; HasDefault must be true")
			}

			// (c) The metadata Default, used as the field value, must encode to
			// the SAME wire as the reader's own auto-fill of the missing field.
			autoWire, err := r.AppendEncode(nil, map[string]any{"pre": "p"})
			if err != nil {
				t.Fatalf("reader auto-fill encode: %v", err)
			}
			explicitWire, err := r.AppendEncode(nil, map[string]any{"pre": "p", "f": fld.Default})
			if err != nil {
				t.Fatalf("encode with metadata Default as the value: %v\ndefault=%#v", err, fld.Default)
			}
			if !bytes.Equal(autoWire, explicitWire) {
				t.Fatalf("metadata Fields[].Default disagrees with the wire:\n auto    =%x\n explicit=%x\n default =%#v", autoWire, explicitWire, fld.Default)
			}

			// Resolution default-fill lands on the same auto-fill wire, and the
			// reader's JSON fill agrees too (the three fill paths converge).
			wWire, err := w.AppendEncode(nil, map[string]any{"pre": "p"})
			if err != nil {
				t.Fatalf("writer encode: %v", err)
			}
			var got map[string]any
			if _, err := res.Decode(wWire, &got); err != nil {
				t.Fatalf("resolved default fill: %v", err)
			}
			gotWire, err := r.AppendEncode(nil, got)
			if err != nil || !bytes.Equal(gotWire, autoWire) {
				t.Fatalf("resolution fill wire differs from auto-fill: err=%v\n got =%x\n auto=%x", err, gotWire, autoWire)
			}
			var jfill map[string]any
			if err := r.DecodeJSON([]byte(`{"pre":"p"}`), &jfill); err != nil {
				t.Fatalf("reader JSON fill: %v", err)
			}
			if !matEqual(got, jfill) {
				t.Fatalf("resolution fill diverges from JSON fill:\n res =%#v\n json=%#v", got, jfill)
			}
		})
	}
}

// TestMatrix_GenerativeProps pins Root().Props / Fields[].Props: they observe
// the parsed custom attributes, survive the metadata rebuild, and are stripped
// by Parsing Canonical Form (so they never perturb the fingerprint).
func TestMatrix_GenerativeProps(t *testing.T) {
	schema := `{"type":"record","name":"R","myrec":"v1","fields":[
		{"name":"f","type":"int","myfield":42},
		{"name":"g","type":{"type":"array","items":"long"},"tags":["a","b"]}]}`
	s := avro.MustParse(schema)
	root := s.Root()
	if root.Props["myrec"] != "v1" {
		t.Fatalf("Root().Props[myrec]=%#v want \"v1\"", root.Props["myrec"])
	}
	if len(root.Fields) != 2 || root.Fields[0].Props["myfield"] == nil {
		t.Fatalf("Fields[0].Props[myfield] missing: %#v", root.Fields[0].Props)
	}
	// PCF strips props: the fingerprint of the propful schema equals that of the
	// prop-stripped one.
	bare := avro.MustParse(`{"type":"record","name":"R","fields":[
		{"name":"f","type":"int"},
		{"name":"g","type":{"type":"array","items":"long"}}]}`)
	if !bytes.Equal(s.Fingerprint(avro.NewRabin()), bare.Fingerprint(avro.NewRabin())) {
		t.Fatalf("props perturb the canonical fingerprint:\n propful=%s\n bare   =%s", s.Canonical(), bare.Canonical())
	}
	// The rebuild preserves the props (observation survives a round-trip).
	rebuilt, err := root.Schema()
	if err != nil {
		t.Fatalf("Root().Schema(): %v", err)
	}
	rr := rebuilt.Root()
	if rr.Props["myrec"] != "v1" || rr.Fields[0].Props["myfield"] == nil {
		t.Fatalf("rebuild dropped props: root=%#v field=%#v", rr.Props, rr.Fields[0].Props)
	}
}

// ===========================================================================
// Layer 6 — promotion decode-flavor × boundary values.
//
// The resolved/promoted decode-flavor with NORMAL values is covered by
// matrix_evolution (PromotionPairsByContext) and matrix_typed (PromotionIn
// EveryContext). This layer adds the boundary axis those omit, where promotion
// is width-changing and so value-lossy by design:
//
//   - int→float of MaxInt32 rounds (float32 cannot hold 2^31-1);
//   - long→double of 2^53+1 rounds (double cannot hold it);
//   - float→double of a signaling NaN quiets the payload (a width conversion,
//     exactly the float32→float64 case the codebase reasons about).
//
// The invariant is calibration-anchored, not bit-preserving: the resolved read
// of the writer wire, re-encoded against the reader, must equal the reader's own
// encoding of the GO-level promotion of the same value — i.e. the codec promotes
// exactly as a plain Go conversion would, with no extra corruption.
// ===========================================================================

func goPromote(wk, rk string, v any) any {
	switch wk + "->" + rk {
	case "int->long":
		return int64(v.(int32))
	case "int->float":
		return float32(v.(int32))
	case "int->double":
		return float64(v.(int32))
	case "long->float":
		return float32(v.(int64))
	case "long->double":
		return float64(v.(int64))
	case "float->double":
		return float64(v.(float32))
	case "string->bytes":
		return []byte(v.(string))
	case "bytes->string":
		return string(v.([]byte))
	}
	panic("no promotion " + wk + "->" + rk)
}

func TestMatrix_GenerativePromotionBoundary(t *testing.T) {
	pairs := []struct {
		wk, rk string
		vals   []any // writer-typed boundary values
	}{
		{"int", "long", []any{int32(math.MaxInt32), int32(math.MinInt32), int32(0)}},
		{"int", "float", []any{int32(math.MaxInt32), int32(math.MinInt32), int32(1 << 24), int32(1<<24 + 1)}},
		{"int", "double", []any{int32(math.MaxInt32), int32(math.MinInt32)}},
		{"long", "float", []any{int64(math.MaxInt64), int64(math.MinInt64)}},
		{"long", "double", []any{int64(1<<53 + 1), int64(math.MaxInt64), int64(math.MinInt64)}},
		{"float", "double", []any{float32(1.5), float32(math.Inf(1)), float32(math.Inf(-1)), float32(math.NaN()), sNaN32, float32(math.MaxFloat32), float32(math.Copysign(0, -1))}},
		{"string", "bytes", []any{"", "x", strings.Repeat("s", 70000)}},
		{"bytes", "string", []any{[]byte{}, []byte("y"), bytes.Repeat([]byte{0x41}, 70000)}},
	}
	// Representative contexts: top plus the per-element/per-field dispatches.
	ctxs := []struct {
		label  string
		schema func(kind string) string
		wrap   func(v any) any
	}{
		{"top", func(k string) string { return fmt.Sprintf("%q", k) }, func(v any) any { return v }},
		{"array", func(k string) string { return fmt.Sprintf(`{"type":"array","items":%q}`, k) }, func(v any) any { return []any{v, v} }},
		{"field", func(k string) string {
			return fmt.Sprintf(`{"type":"record","name":"PR","fields":[{"name":"f","type":%q}]}`, k)
		}, func(v any) any { return map[string]any{"f": v} }},
	}
	for _, p := range pairs {
		for _, cx := range ctxs {
			t.Run(fmt.Sprintf("%s->%s/%s", p.wk, p.rk, cx.label), func(t *testing.T) {
				w := avro.MustParse(cx.schema(p.wk))
				r := avro.MustParse(cx.schema(p.rk))
				res, err := resolveBoth(t, w, r)
				if err != nil {
					t.Fatalf("Resolve: %v", err)
				}
				for _, v := range p.vals {
					wire, err := w.AppendEncode(nil, cx.wrap(v))
					if err != nil {
						t.Fatalf("writer encode %v: %v", v, err)
					}
					var got any
					if _, err := res.Decode(wire, &got); err != nil {
						t.Fatalf("resolved decode %v: %v", v, err)
					}
					// Oracle: the reader's own encoding of the Go-level promotion.
					wantWire, err := r.AppendEncode(nil, cx.wrap(goPromote(p.wk, p.rk, v)))
					if err != nil {
						t.Fatalf("reader encode promoted %v: %v", v, err)
					}
					gotWire, err := r.AppendEncode(nil, got)
					if err != nil {
						t.Fatalf("re-encode promoted %v: %v", v, err)
					}
					if !bytes.Equal(gotWire, wantWire) {
						t.Fatalf("promotion %s->%s of %v (%s): wire diverges from Go-level promotion:\n got =%x\n want=%x\n value=%#v", p.wk, p.rk, v, cx.label, gotWire, wantWire, got)
					}
				}
			})
		}
	}
}
