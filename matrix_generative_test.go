package avro_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
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

func jsonNum(s string) json.Number { return json.Number(s) }

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
			{boundary: "controls", generic: "with\nnewline\ttab", oracle: avroLen([]byte("with\nnewline\ttab"))},
			{boundary: "spaces", generic: "  ", oracle: avroLen([]byte("  "))},
			{boundary: "large", generic: strings.Repeat("x", 70000), oracle: avroLen([]byte(strings.Repeat("x", 70000)))},
		}},
		{"bytes", "bytes", func(*uniq) string { return `"bytes"` }, []gval{
			{boundary: "empty", generic: []byte{}, oracle: avroLen(nil)},
			{boundary: "zerobyte", generic: []byte{0x00}, oracle: avroLen([]byte{0x00})},
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
		// Every remaining logical (axis-completeness: "every logical").
		{"time-millis", "time-millis", func(*uniq) string { return `{"type":"int","logicalType":"time-millis"}` }, []gval{
			{boundary: "zero", generic: time.Duration(0), oracle: appendZig(nil, 0)},
			{boundary: "max", generic: 23*time.Hour + 59*time.Minute + 59*time.Second + 999*time.Millisecond},
		}},
		{"time-micros", "time-micros", func(*uniq) string { return `{"type":"long","logicalType":"time-micros"}` }, []gval{
			{boundary: "zero", generic: time.Duration(0), oracle: appendZig(nil, 0)},
			{boundary: "max", generic: 23*time.Hour + 59*time.Minute + 59*time.Second + 999999*time.Microsecond},
		}},
		{"timestamp-nanos", "timestamp-nanos", func(*uniq) string { return `{"type":"long","logicalType":"timestamp-nanos"}` }, []gval{
			{boundary: "normal", generic: time.Unix(0, 1717243496789012345).UTC()},
			{boundary: "maxnanos", generic: time.Unix(0, math.MaxInt64).UTC()},
			{boundary: "minnanos", generic: time.Unix(0, math.MinInt64).UTC()},
		}},
		{"local-ts-millis", "local-timestamp-millis", func(*uniq) string { return `{"type":"long","logicalType":"local-timestamp-millis"}` }, []gval{
			{boundary: "normal", generic: time.UnixMilli(1717243496789).UTC()},
		}},
		{"local-ts-micros", "local-timestamp-micros", func(*uniq) string { return `{"type":"long","logicalType":"local-timestamp-micros"}` }, []gval{
			{boundary: "normal", generic: time.UnixMicro(1717243496789012).UTC()},
		}},
		{"local-ts-nanos", "local-timestamp-nanos", func(*uniq) string { return `{"type":"long","logicalType":"local-timestamp-nanos"}` }, []gval{
			{boundary: "normal", generic: time.Unix(0, 1717243496789012345).UTC()},
		}},
		{"uuid-fixed", "uuid", func(u *uniq) string {
			return fmt.Sprintf(`{"type":"fixed","name":%q,"size":16,"logicalType":"uuid"}`, u.name("GUF"))
		}, []gval{
			{boundary: "normal", generic: "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		}},
		// Cardinality boundaries of enum/fixed the legacy gtypes set omits.
		{"enum1", "enum", func(u *uniq) string {
			return fmt.Sprintf(`{"type":"enum","name":%q,"symbols":["Only"]}`, u.name("GE1"))
		}, []gval{
			{boundary: "only", generic: "Only", oracle: appendZig(nil, 0)},
		}},
		{"fixed1", "fixed", func(u *uniq) string {
			return fmt.Sprintf(`{"type":"fixed","name":%q,"size":1}`, u.name("GF1"))
		}, []gval{
			{boundary: "zero", generic: []byte{0x00}, oracle: []byte{0x00}},
			{boundary: "high", generic: []byte{0xFF}, oracle: []byte{0xFF}},
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
	wAddr := gEncEq(t, sRec, pStruct.Interface(), nil, "struct addressable (unsafe)")  // *struct => addressable
	wNon := gEncEq(t, sRec, pStruct.Elem().Interface(), nil, "struct non-addressable") // struct value => reflect
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

// TestMatrix_GenerativeUnionContainerDefaultFill is the durable net for the
// union-default metadata↔wire class: when a union field's branches are CONTAINERS
// (record/array/map) holding a leaf and the field has a default, the branch+value
// the metadata selector (branchAcceptsDefault → coerceMetadataDefault) reports must
// match the branch+value the wire auto-fill produces, on BOTH wire formats. Two
// findings landed in this class from the SAME uncovered cells — a float string→float
// coercion in a nested branch, then an int64→int32 overflow wrap — both hidden
// because the prior nets (the flat TestMatrix_GenerativeDefaultFill above, and the
// matFrags×matCtxs core matrix) drove only IN-RANGE values. This matrix crosses the
// boundary/overflow value classes those miss.
//
// Wire-as-oracle (the AUDIT_CORE matrix contract — the binary auto-fill decode is
// canonical). Per cell × container:
//   - Root().Schema() rebuild re-encodes the auto-fill BYTE-IDENTICALLY on binary
//     AND JSON. This is the severe surface: a wrapped or wrong-branch default
//     silently changes the schema's own wire through the documented "Root preserves
//     all metadata" round-trip. Representation-agnostic, so it holds for a logical
//     leaf whose Default surfaces the raw Avro-native value (NOT_BUGS #30) while the
//     wire decodes the transformed value.
//   - the direct JSON decode auto-fill (DecodeJSON of an empty object, which
//     materializes the stored default via applyFieldDefault) agrees with the binary
//     auto-fill (matEqual).
//   - for non-logical leaves, Root().Fields[].Default equals the binary auto-fill
//     decode type-exactly (matEqual) — the direct metadata pin the int64→int32 wrap
//     violated (int32(-1294967296) where the wire decoded float64(3e9)).
//
// Each cell pairs a leaf branch with a VALUE-ADMITTING wider/other sibling branch so
// the schema parses: an overflow default the leaf rejects is held by the sibling and
// the divergence surfaces. A same-class-rejecting sibling would reject the schema at
// parse and hide the cell behind a parse error — exactly how these escaped (see
// TestRegression_UnionContainerNestedIntDefaultOverflowMatchesWire and
// TestRegression_UnionContainerNestedFloatDefaultSelectionMatchesWire, the
// single-shape pins this matrix generalizes).
func TestMatrix_GenerativeUnionContainerDefaultFill(t *testing.T) {
	for _, c := range udfCells() {
		for _, cont := range udfContainers() {
			t.Run(c.name+"/"+cont.name, func(t *testing.T) {
				branchA := fmt.Sprintf(`{"type":"record","name":"A","fields":[{"name":"x","type":%s}]}`, cont.wrap(c.leaf))
				branchB := fmt.Sprintf(`{"type":"record","name":"B","fields":[{"name":"x","type":%s}]}`, cont.wrap(c.sib))
				schema := fmt.Sprintf(`{"type":"record","name":"Outer","fields":[{"name":"f","type":[%s,%s],"default":%s}]}`,
					branchA, branchB, cont.def(c.defLit))
				udInvariant(t, schema, `{}`, map[string]any{}, udfIsLogical(c.leaf),
					func(s *avro.Schema, a1 map[string]any) [][2]any {
						return [][2]any{{s.Root().Fields[0].Default, a1["f"]}}
					})
			})
		}
	}
}

// TestMatrix_GenerativeUnionContainerDefaultFillRecursive runs the same union-
// default invariant when the leaf-bearing branch is a SELF-REFERENTIAL record
// (N{x:<leaf>, next:["null","N"]}), so the default-fill and the metadata coercion
// run through a type that references itself — the second-occurrence / self-ref
// path flat schemas and the matFrags×matCtxs core matrix never reach. Two default
// depths: a shallow one ("next":null — the self-reference declared but not
// traversed) and a one-level-deep one ("next":{...,"next":null} — the recursion
// actually walked, so the coercion fires at BOTH levels). The leaf-bearing branch
// N pairs with a value-admitting self-referential sibling S whose x is the wider
// type, so a boundary default the leaf rejects is held by S and the cell is
// reachable (the value-admitting-sibling rule).
func TestMatrix_GenerativeUnionContainerDefaultFillRecursive(t *testing.T) {
	// addNext appends a "next" field value to a {"x":...} default object.
	addNext := func(obj, next string) string { return strings.TrimSuffix(obj, "}") + `,"next":` + next + "}" }
	for _, c := range udfCells() {
		for _, cont := range udfContainers() {
			for _, depth := range []string{"shallow", "deep"} {
				t.Run(c.name+"/"+cont.name+"/"+depth, func(t *testing.T) {
					branchN := fmt.Sprintf(`{"type":"record","name":"N","fields":[{"name":"x","type":%s},{"name":"next","type":["null","N"]}]}`, cont.wrap(c.leaf))
					branchS := fmt.Sprintf(`{"type":"record","name":"S","fields":[{"name":"x","type":%s},{"name":"next","type":["null","S"]}]}`, cont.wrap(c.sib))
					inner := cont.def(c.defLit) // {"x":<container form>}
					def := addNext(inner, "null")
					if depth == "deep" {
						def = addNext(inner, addNext(inner, "null"))
					}
					schema := fmt.Sprintf(`{"type":"record","name":"Outer","fields":[{"name":"f","type":[%s,%s],"default":%s}]}`,
						branchN, branchS, def)
					udInvariant(t, schema, `{}`, map[string]any{}, udfIsLogical(c.leaf),
						func(s *avro.Schema, a1 map[string]any) [][2]any {
							return [][2]any{{s.Root().Fields[0].Default, a1["f"]}}
						})
				})
			}
		}
	}
}

// TestMatrix_GenerativeUnionContainerDefaultFillDiamond runs the same invariant
// when the union-default-bearing record DiaT is referenced from TWO positions
// (Outer{a:DiaT, b:DiaT}): DiaT is DEFINED at field a and a bare NAME REFERENCE at
// field b. This exercises the second-occurrence reference path — where the cache
// self-ref and type-alias cross-record bugs lived — for the default-fill class.
// The wire must fill BOTH a.f and b.f from DiaT's default (the reference resolves
// to the definition's default); Root() carries the default on the DEFINITION
// (Fields[0]; a bare reference correctly surfaces as a name node with no inline
// fields, so it is not separately asserted); and Root().Schema() must re-emit DiaT
// as ONE definition + a reference (not duplicated or renamed), byte-identically.
func TestMatrix_GenerativeUnionContainerDefaultFillDiamond(t *testing.T) {
	for _, c := range udfCells() {
		for _, cont := range udfContainers() {
			t.Run(c.name+"/"+cont.name, func(t *testing.T) {
				recA := fmt.Sprintf(`{"type":"record","name":"DiaA","fields":[{"name":"x","type":%s}]}`, cont.wrap(c.leaf))
				recB := fmt.Sprintf(`{"type":"record","name":"DiaB","fields":[{"name":"x","type":%s}]}`, cont.wrap(c.sib))
				tdef := fmt.Sprintf(`{"type":"record","name":"DiaT","fields":[{"name":"f","type":[%s,%s],"default":%s}]}`,
					recA, recB, cont.def(c.defLit))
				schema := fmt.Sprintf(`{"type":"record","name":"Outer","fields":[{"name":"a","type":%s},{"name":"b","type":"DiaT"}]}`, tdef)
				fill := map[string]any{"a": map[string]any{}, "b": map[string]any{}}
				_, a1, rebuilt := udInvariant(t, schema, `{"a":{},"b":{}}`, fill, udfIsLogical(c.leaf),
					func(s *avro.Schema, a1 map[string]any) [][2]any {
						// The DEFINITION (Fields[0]=a → DiaT → f) carries the default.
						defT := s.Root().Fields[0].Type.Fields[0].Default
						return [][2]any{{defT, a1["a"].(map[string]any)["f"]}}
					})
				// Reference path (every leaf, incl. logical): b.f must auto-fill from
				// the SAME default the definition's a.f did.
				aw := a1["a"].(map[string]any)["f"]
				if bw := a1["b"].(map[string]any)["f"]; !matEqual(bw, aw) {
					t.Errorf("reference branch b.f auto-fill diverges from definition a.f:\n  a.f = %#v\n  b.f = %#v", aw, bw)
				}
				// Structural: the rebuild defines DiaT exactly once (one definition + a
				// reference, not duplicated/renamed) and re-parses.
				if n := strings.Count(rebuilt.String(), `"name":"DiaT"`); n != 1 {
					t.Errorf("rebuild defines DiaT %d times, want 1 (one definition + a reference):\n  %s", n, rebuilt.String())
				}
				if _, err := avro.Parse(rebuilt.String()); err != nil {
					t.Errorf("rebuilt schema does not re-parse (duplicate/renamed type?): %v", err)
				}
			})
		}
	}
}

// udfCell is a leaf branch paired with a value-admitting wider/other sibling and a
// default literal spanning the in-range and boundary/overflow value classes. The
// three shape tests above share this table so flat, recursive, and diamond cross
// the identical (leaf × value-class × container) axes.
type udfCell struct{ name, leaf, sib, defLit string }

func udfCells() []udfCell {
	return []udfCell{
		// int: in-range, both int32 boundaries, and the two overflow forms of the
		// int64→int32 wrap — MaxInt32+1 wraps to a negative, 2^32 to a deceptively
		// valid 0 — both of which the leaf branch must REJECT (sibling long holds it).
		{"int_in_range", `"int"`, `"long"`, `42`},
		{"int_max", `"int"`, `"long"`, `2147483647`},
		{"int_min", `"int"`, `"long"`, `-2147483648`},
		{"int_overflow_negwrap", `"int"`, `"long"`, `2147483648`},
		{"int_overflow_zerowrap", `"int"`, `"long"`, `4294967296`},
		// long: in-range, both int64 boundaries, beyond-int64 (sibling double holds it).
		{"long_in_range", `"long"`, `"double"`, `42`},
		{"long_max", `"long"`, `"double"`, `9223372036854775807`},
		{"long_min", `"long"`, `"double"`, `-9223372036854775808`},
		{"long_beyond_int64", `"long"`, `"double"`, `99999999999999999999`},
		// float/double overflow is lossy-by-destination (float32→±Inf), CONSISTENT on
		// both surfaces — a control that must NOT be "fixed" to reject like the int arm.
		{"float_in_range", `"float"`, `"double"`, `1.5`},
		{"float_overflow_inf", `"float"`, `"double"`, `1e300`},
		{"double_in_range", `"double"`, `"string"`, `1.5`},
		{"double_large", `"double"`, `"string"`, `1e300`},
		// stringy leaves vs a string sibling: in-range picks the leaf, the boundary
		// (codepoint>0xFF / wrong fixed size / enum non-member) picks the sibling.
		{"bytes_in_range", `"bytes"`, `"string"`, `"Aÿ"`},
		{"bytes_codepoint_over_0xFF", `"bytes"`, `"string"`, `"Ā"`},
		{"fixed_right_size", `{"type":"fixed","name":"FX","size":2}`, `"string"`, `"AB"`},
		{"fixed_wrong_size", `{"type":"fixed","name":"FX","size":4}`, `"string"`, `"AB"`},
		{"enum_member", `{"type":"enum","name":"EN","symbols":["A","B"]}`, `"string"`, `"A"`},
		{"enum_nonmember", `{"type":"enum","name":"EN","symbols":["A","B"]}`, `"string"`, `"Z"`},
		{"string_any", `"string"`, `"bytes"`, `"hello"`},
		// logical leaf (long-backed): Default surfaces the raw long, the wire decodes
		// time.Time — checked by the representation-agnostic rebuild + JSON agreement.
		{"timestamp_millis_in_range", `{"type":"long","logicalType":"timestamp-millis"}`, `"double"`, `1717243496789`},
		{"timestamp_millis_beyond_int64", `{"type":"long","logicalType":"timestamp-millis"}`, `"double"`, `99999999999999999999`},
	}
}

func udfIsLogical(leaf string) bool { return strings.Contains(leaf, "logicalType") }

// udfContainer holds the leaf as a record field "x"'s value (so a union of two
// such records stays legal — a union cannot hold two arrays or two maps directly)
// at one of three container depths.
type udfContainer struct {
	name string
	wrap func(leaf string) string // the branch field "x"'s type
	def  func(lit string) string  // the default literal in field-"x" container form
}

func udfContainers() []udfContainer {
	return []udfContainer{
		{"record_field", func(l string) string { return l }, func(lit string) string { return `{"x":` + lit + `}` }},
		{"array_element", func(l string) string { return `{"type":"array","items":` + l + `}` }, func(lit string) string { return `{"x":[` + lit + `]}` }},
		{"map_value", func(l string) string { return `{"type":"map","values":` + l + `}` }, func(lit string) string { return `{"x":{"k":` + lit + `}}` }},
	}
}

// udInvariant runs the wire-as-oracle union-default invariant for one composed
// default-fill schema, shared by the flat/recursive/diamond shape tests above. The
// binary auto-fill decode of fillVal (an empty outer, whose missing nested defaults
// materialize) is canonical; fillJSON is its JSON form for the direct DecodeJSON
// fill. metaPairs returns, per metadata surface under test, the (Root-derived
// Default, wire-decoded value) pair that must match type-exactly for a NON-logical
// leaf (a logical leaf surfaces the raw Avro-native value per NOT_BUGS #30, so it
// is covered by the rebuild + JSON-decode checks instead). Returns the parsed
// schema, the canonical decode, and the metadata rebuild so a caller can add
// shape-specific checks (the diamond's reference path + one-definition structure).
func udInvariant(t *testing.T, schema, fillJSON string, fillVal map[string]any, logical bool,
	metaPairs func(s *avro.Schema, a1 map[string]any) [][2]any) (*avro.Schema, map[string]any, *avro.Schema) {
	t.Helper()
	s, err := avro.Parse(schema)
	if err != nil {
		t.Fatalf("parse: %v\n  %s", err, schema)
	}
	w1, err := s.Encode(fillVal)
	if err != nil {
		t.Fatalf("binary auto-fill encode: %v", err)
	}
	var a1 map[string]any
	if _, err := s.Decode(w1, &a1); err != nil {
		t.Fatalf("binary decode: %v", err)
	}
	j1, err := s.AppendEncodeJSON(nil, fillVal)
	if err != nil {
		t.Fatalf("json auto-fill encode: %v", err)
	}

	// Non-logical: Root().Default equals the binary auto-fill decode, type-exactly
	// (the direct metadata pin the int64→int32 wrap violated).
	if !logical {
		for _, pr := range metaPairs(s, a1) {
			if meta, wire := pr[0], pr[1]; !matEqual(wire, meta) {
				t.Errorf("Root().Default disagrees with the binary auto-fill (wrong branch/value):\n  wire = %#v\n  meta = %#v", wire, meta)
			}
		}
	}

	// Direct JSON decode auto-fill agrees with the binary auto-fill. A DIRECT
	// DecodeJSON of the empty outer materializes the stored default via
	// applyFieldDefault — NOT a JSON encode→decode round-trip, which on these
	// overlapping record branches would hit the documented bare untagged-union
	// first-match loss (NOT_BUGS #5).
	var dj map[string]any
	if err := s.DecodeJSON([]byte(fillJSON), &dj); err != nil {
		t.Fatalf("json decode auto-fill: %v", err)
	}
	if !matEqual(dj, a1) {
		t.Errorf("JSON decode auto-fill disagrees with binary:\n  bin  = %#v\n  json = %#v", a1, dj)
	}

	// Root().Schema() rebuild re-encodes the auto-fill BYTE-IDENTICALLY on both wire
	// formats — the severe surface: a wrapped/wrong-branch default silently changes
	// the schema's own wire through the documented metadata round-trip. This is
	// representation-agnostic, so it covers logical leaves too.
	rn := s.Root()
	rebuilt, err := rn.Schema()
	if err != nil {
		t.Fatalf("Root().Schema(): %v", err)
	}
	rw, err := rebuilt.Encode(fillVal)
	if err != nil {
		t.Fatalf("rebuilt binary auto-fill: %v", err)
	}
	if !bytes.Equal(rw, w1) {
		t.Errorf("Root().Schema() binary rebuild auto-fill = %x, want %x (rebuilt default selects a different branch/value)", rw, w1)
	}
	rj, err := rebuilt.AppendEncodeJSON(nil, fillVal)
	if err != nil {
		t.Fatalf("rebuilt json auto-fill: %v", err)
	}
	if !bytes.Equal(rj, j1) {
		t.Errorf("Root().Schema() JSON rebuild auto-fill = %s, want %s (original)", rj, j1)
	}
	return s, a1, rebuilt
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

// ===========================================================================
// Layer 7 — the json.Number overflow boundary input (the "1e1000" axis value).
//
// gtypes covers the ±Inf VALUE; this covers the textual overflow INPUT that
// must narrow TO ±Inf. json.Number is a valid encode input and a bare JSON
// number is a valid decode input, so "1e1000" exercises a distinct narrow-to-Inf
// path on the float/double arms and an exact-or-reject path on the int/long
// arms. Oracle: the Go-parsed value's bits (float) or exact zigzag (integer),
// computed independently of the codec.
// ===========================================================================

func TestMatrix_GenerativeJSONNumberBoundary(t *testing.T) {
	parseF64 := func(s string) float64 { f, _ := strconv.ParseFloat(s, 64); return f }
	floatCases := []struct {
		schema string
		oracle func(s string) []byte
	}{
		{`"double"`, func(s string) []byte { return leF64(parseF64(s)) }},
		{`"float"`, func(s string) []byte { return leF32(float32(parseF64(s))) }},
	}
	floatInputs := []string{"1e1000", "-1e1000", "1.5", "0", "9007199254740993"}
	for _, fc := range floatCases {
		s := avro.MustParse(fc.schema)
		for _, in := range floatInputs {
			t.Run(fc.schema+"/"+in, func(t *testing.T) {
				// Binary encode of json.Number narrows to the IEEE bits.
				w, err := s.AppendEncode(nil, jsonNum(in))
				if err != nil {
					t.Fatalf("encode json.Number(%s): %v", in, err)
				}
				if want := fc.oracle(in); !bytes.Equal(w, want) {
					t.Fatalf("json.Number(%s) binary diverges from Go-parsed oracle:\n got=%x\nwant=%x", in, w, want)
				}
				// Bare-number JSON decode into *any narrows the same way and the
				// decoded value re-encodes onto the same wire.
				var dec any
				if err := s.DecodeJSON([]byte(in), &dec); err != nil {
					t.Fatalf("decodeJSON bare number %s: %v", in, err)
				}
				w2, err := s.AppendEncode(nil, dec)
				if err != nil || !bytes.Equal(w2, w) {
					t.Fatalf("bare-number JSON decode re-encode differs: err=%v\n got=%x\n want=%x", err, w2, w)
				}
			})
		}
	}

	// Integer arms: 2^53+1 is exact; 1e1000 (a non-integer overflow) must REJECT
	// with a bounded error, not silently truncate.
	t.Run("long/2^53+1-exact", func(t *testing.T) {
		s := avro.MustParse(`"long"`)
		w, err := s.AppendEncode(nil, jsonNum("9007199254740993"))
		if err != nil || !bytes.Equal(w, appendZig(nil, 1<<53+1)) {
			t.Fatalf("long json.Number 2^53+1 not exact: err=%v w=%x", err, w)
		}
	})
	for _, sc := range []string{`"int"`, `"long"`} {
		t.Run(sc+"/1e1000-rejects", func(t *testing.T) {
			s := avro.MustParse(sc)
			if _, err := s.AppendEncode(nil, jsonNum("1e1000")); err == nil {
				t.Fatalf("%s accepted overflow json.Number 1e1000 (must reject)", sc)
			}
		})
	}
}

// ===========================================================================
// Pointer-indirection depth × container context.
//
// The codec peels a pointer/interface chain on BOTH the encode input and the
// decode target, capped at maxIndirectDepth levels: a chain bottoming at a
// non-pointer base WITHIN the cap is accepted, one level deeper is rejected.
// That single cap must hold at EVERY context a value can sit in and across
// EVERY path — binary encode, JSON encode, and natural + identity-resolved
// decode of both wires — or a wire one path emits is a wire another path (or
// the same path's own reader) refuses: a binary↔JSON / encode↔decode
// round-trip break. The bug shape is a context-local peel that drifts from the
// cap by re-indirecting an already-peeled value: a union target indirected at
// two stages (unionTarget then the branch decode), or a container element
// unwrapped one level inline then handed to a full-budget indirect — each
// accepting up to one-or-two-times maxIndirectDepth where every leaf accepts
// exactly the cap. Such a drift is invisible to round-trip-from-typed-input
// (the input never nests past the cap) and to value/wire sweeps (depth carries
// no wire bytes); only crossing the depth axis with the context axis the peel
// is supposed to be identical across exposes it.
//
// Crosses pointer depth {0, 1, at-cap, past-cap} × context {top, record field,
// 2-branch null union, 3+-branch union, array, map} × base primitive ×
// {binary, JSON} encode × {binary, JSON} wire × {natural, identity-resolved}
// decode, asserting (a) all six paths agree on accept/reject at each depth
// AGAINST THE EXPLICIT CAP (so a "reject everything" regression is caught too,
// not merely mutual agreement), (b) an accepted value round-trips to the
// identical base on every path, and (c) pointer wrapping is wire-invariant — a
// deep input encodes to the same bytes as the bare base.
// ===========================================================================

// ptrIndirectCap mirrors the package-internal maxIndirectDepth (reflect.go):
// the deepest pointer/interface chain the codec peels. A chain bottoming at a
// non-pointer base within this many levels round-trips on every path; one level
// deeper is rejected on every path. If the internal cap changes, the past-cap
// entry in the depth list below changes with it.
const ptrIndirectCap = 5

// ptrTypeOf wraps base in depth pointer levels (depth 0 => base unchanged).
func ptrTypeOf(base reflect.Type, depth int) reflect.Type {
	for range depth {
		base = reflect.PointerTo(base)
	}
	return base
}

// ptrChain wraps sample in depth non-nil pointer levels (depth 0 => sample).
func ptrChain(sample reflect.Value, depth int) reflect.Value {
	for range depth {
		p := reflect.New(sample.Type())
		p.Elem().Set(sample)
		sample = p
	}
	return sample
}

// derefAll fully dereferences a pointer/interface chain to its base value.
func derefAll(v reflect.Value) reflect.Value {
	for v.IsValid() && (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) {
		if v.IsNil() {
			return v
		}
		v = v.Elem()
	}
	return v
}

// ptrValEq compares a peeled base against the expected sample by type+value.
func ptrValEq(got, want reflect.Value) bool {
	return got.IsValid() && got.Type() == want.Type() && got.Interface() == want.Interface()
}

// ptrBase is one primitive base type for the pointer-indirection axis.
type ptrBase struct {
	avro   string        // the Avro schema text for this primitive
	sample reflect.Value // a representative non-zero value of the Go base type
	pad    string        // a union padding branch, token-class-distinct from avro
}

func ptrBases() []ptrBase {
	// pad is token-class-distinct from the base so a 3+-branch union dispatches
	// the sample to its OWN branch, never the pad (digit-class bases pad with a
	// string; string/boolean bases pad with a long).
	return []ptrBase{
		{`"int"`, reflect.ValueOf(int32(7)), `"string"`},
		{`"long"`, reflect.ValueOf(int64(7)), `"string"`},
		{`"float"`, reflect.ValueOf(float32(1.5)), `"string"`},
		{`"double"`, reflect.ValueOf(float64(1.5)), `"string"`},
		{`"string"`, reflect.ValueOf("v"), `"long"`},
		{`"boolean"`, reflect.ValueOf(true), `"long"`},
	}
}

// ptrCtx composes a base primitive into a context that holds it in one or more
// pointer-depth-D slots, building the encode input, a fresh decode target, and
// a round-trip checker for that context.
type ptrCtx struct {
	label  string
	schema func(b ptrBase) string
	input  func(b ptrBase, depth int) reflect.Value // context-shaped encode input
	target func(b ptrBase, depth int) reflect.Value // a *context-shaped fresh decode target
	check  func(t *testing.T, b ptrBase, target reflect.Value)
}

// ptrFieldStruct is a one-field record struct whose field is a depth-deep
// pointer chain over the base, avro-tagged to the schema field "f".
func ptrFieldStruct(b ptrBase, depth int) reflect.Type {
	return reflect.StructOf([]reflect.StructField{
		{Name: "F", Type: ptrTypeOf(b.sample.Type(), depth), Tag: `avro:"f"`},
	})
}

func ptrCtxs() []ptrCtx {
	stringType := reflect.TypeOf("")
	// top / 2-branch null union / 3+-branch union all carry the chain as the
	// value itself; only the schema (and thus the decode dispatch) differs.
	chainTop := func(b ptrBase, depth int) reflect.Value { return ptrChain(b.sample, depth) }
	newChain := func(b ptrBase, depth int) reflect.Value { return reflect.New(ptrTypeOf(b.sample.Type(), depth)) }
	checkTop := func(t *testing.T, b ptrBase, target reflect.Value) {
		t.Helper()
		if got := derefAll(target.Elem()); !ptrValEq(got, b.sample) {
			t.Fatalf("round-trip mismatch: got %v, want %v", safeIface(got), b.sample)
		}
	}
	return []ptrCtx{
		{
			label:  "top",
			schema: func(b ptrBase) string { return b.avro },
			input:  chainTop, target: newChain, check: checkTop,
		},
		{
			label:  "union-null2",
			schema: func(b ptrBase) string { return fmt.Sprintf(`["null",%s]`, b.avro) },
			input:  chainTop, target: newChain, check: checkTop,
		},
		{
			label:  "union-multi",
			schema: func(b ptrBase) string { return fmt.Sprintf(`["null",%s,%s]`, b.avro, b.pad) },
			input:  chainTop, target: newChain, check: checkTop,
		},
		{
			label: "field",
			schema: func(b ptrBase) string {
				return fmt.Sprintf(`{"type":"record","name":"PtrRec","fields":[{"name":"f","type":%s}]}`, b.avro)
			},
			input: func(b ptrBase, depth int) reflect.Value {
				v := reflect.New(ptrFieldStruct(b, depth)).Elem()
				v.Field(0).Set(ptrChain(b.sample, depth))
				return v
			},
			target: func(b ptrBase, depth int) reflect.Value { return reflect.New(ptrFieldStruct(b, depth)) },
			check: func(t *testing.T, b ptrBase, target reflect.Value) {
				t.Helper()
				if got := derefAll(target.Elem().Field(0)); !ptrValEq(got, b.sample) {
					t.Fatalf("field round-trip mismatch: got %v, want %v", safeIface(got), b.sample)
				}
			},
		},
		{
			label:  "array",
			schema: func(b ptrBase) string { return fmt.Sprintf(`{"type":"array","items":%s}`, b.avro) },
			input: func(b ptrBase, depth int) reflect.Value {
				st := reflect.SliceOf(ptrTypeOf(b.sample.Type(), depth))
				sl := reflect.MakeSlice(st, 2, 2)
				sl.Index(0).Set(ptrChain(b.sample, depth))
				sl.Index(1).Set(ptrChain(b.sample, depth))
				return sl
			},
			target: func(b ptrBase, depth int) reflect.Value {
				return reflect.New(reflect.SliceOf(ptrTypeOf(b.sample.Type(), depth)))
			},
			check: func(t *testing.T, b ptrBase, target reflect.Value) {
				t.Helper()
				sl := target.Elem()
				if sl.Len() != 2 {
					t.Fatalf("array round-trip length: got %d, want 2", sl.Len())
				}
				for i := range sl.Len() {
					if got := derefAll(sl.Index(i)); !ptrValEq(got, b.sample) {
						t.Fatalf("array[%d] round-trip mismatch: got %v, want %v", i, safeIface(got), b.sample)
					}
				}
			},
		},
		{
			label:  "map",
			schema: func(b ptrBase) string { return fmt.Sprintf(`{"type":"map","values":%s}`, b.avro) },
			input: func(b ptrBase, depth int) reflect.Value {
				mt := reflect.MapOf(stringType, ptrTypeOf(b.sample.Type(), depth))
				m := reflect.MakeMap(mt)
				m.SetMapIndex(reflect.ValueOf("k"), ptrChain(b.sample, depth))
				return m
			},
			target: func(b ptrBase, depth int) reflect.Value {
				return reflect.New(reflect.MapOf(stringType, ptrTypeOf(b.sample.Type(), depth)))
			},
			check: func(t *testing.T, b ptrBase, target reflect.Value) {
				t.Helper()
				got := derefAll(target.Elem().MapIndex(reflect.ValueOf("k")))
				if !ptrValEq(got, b.sample) {
					t.Fatalf("map[k] round-trip mismatch: got %v, want %v", safeIface(got), b.sample)
				}
			},
		},
	}
}

// safeIface renders a possibly-invalid/nil reflect.Value for an error message
// without panicking on the .Interface() of an unexported/invalid Value.
func safeIface(v reflect.Value) any {
	if !v.IsValid() {
		return "<invalid>"
	}
	if !v.CanInterface() {
		return v.String()
	}
	return v.Interface()
}

func TestMatrix_GenerativePointerIndirection(t *testing.T) {
	depths := []int{0, 1, ptrIndirectCap, ptrIndirectCap + 1} // {0, 1, at-cap, past-cap}
	for _, b := range ptrBases() {
		for _, pc := range ptrCtxs() {
			t.Run(strings.Trim(b.avro, `"`)+"/"+pc.label, func(t *testing.T) {
				s := avro.MustParse(pc.schema(b))
				res, err := avro.Resolve(s, s)
				if err != nil {
					t.Fatalf("identity Resolve: %v\nschema: %s", err, pc.schema(b))
				}
				// Canonical wires from the bare (depth-0) base in this context;
				// depth 0 is always within the cap, so both encodes must succeed.
				cbin, err := s.AppendEncode(nil, pc.input(b, 0).Interface())
				if err != nil {
					t.Fatalf("canonical binary encode: %v", err)
				}
				cjson, err := s.AppendEncodeJSON(nil, pc.input(b, 0).Interface())
				if err != nil {
					t.Fatalf("canonical JSON encode: %v", err)
				}
				for _, depth := range depths {
					t.Run(fmt.Sprintf("depth=%d", depth), func(t *testing.T) {
						accept := depth <= ptrIndirectCap

						// --- encode parity: binary and JSON agree with the cap ---
						binW, binErr := s.AppendEncode(nil, pc.input(b, depth).Interface())
						jsonW, jsonErr := s.AppendEncodeJSON(nil, pc.input(b, depth).Interface())
						if (binErr == nil) != accept {
							t.Fatalf("binary encode accept=%v, want %v (err=%v)", binErr == nil, accept, binErr)
						}
						if (jsonErr == nil) != accept {
							t.Fatalf("JSON encode accept=%v, want %v (err=%v)", jsonErr == nil, accept, jsonErr)
						}
						// An accepted deep input encodes to the SAME bytes as the
						// bare base: pointer wrapping is transparent on the wire.
						if accept {
							if !bytes.Equal(binW, cbin) {
								t.Fatalf("deep binary wire differs from bare base:\n got=%x\nwant=%x", binW, cbin)
							}
							if !bytes.Equal(jsonW, cjson) {
								t.Fatalf("deep JSON wire differs from bare base:\n got=%s\nwant=%s", jsonW, cjson)
							}
						}

						// --- decode parity: every {wire}×{natural,resolved} path
						// agrees with the cap, and an accepted value round-trips ---
						decPaths := []struct {
							name string
							dec  func(target any) error
						}{
							{"binary/natural", func(target any) error { _, e := s.Decode(cbin, target); return e }},
							{"binary/resolved", func(target any) error { _, e := res.Decode(cbin, target); return e }},
							{"json/natural", func(target any) error { return s.DecodeJSON(cjson, target) }},
							{"json/resolved", func(target any) error { return res.DecodeJSON(cjson, target) }},
						}
						for _, dp := range decPaths {
							target := pc.target(b, depth)
							err := dp.dec(target.Interface())
							if (err == nil) != accept {
								t.Fatalf("%s decode accept=%v, want %v (err=%v)", dp.name, err == nil, accept, err)
							}
							if accept {
								pc.check(t, b, target)
							}
						}
					})
				}
			})
		}
	}
}

// ===========================================================================
// Pointer-indirection depth × FIELD-OF-CONTAINER context — the unsafe struct-
// field CONTAINER fast paths.
//
// TestMatrix_GenerativePointerIndirection crosses depth × context, but its
// contexts all carry the chain at TOP LEVEL or as a SCALAR struct field, so they
// reach only the reflect serArray/serMap and the unsafe SCALAR field-pointer fast
// path — never the unsafe struct-field CONTAINER fast paths
// (tryCompileFieldSer/tryCompileFieldDeser's usArrayRecord→usArrayPtrRecord /
// udArrayPtrRecord, usNullUnionRecord / udNullUnionRecord, and
// usArrayNullUnionRecord / usArrayNullUnionPtr). Those arms fire ONLY for an
// array / null-union / array-of-null-union element INSIDE an ADDRESSABLE struct
// field. Each hand-peels exactly one pointer level inline (the element or the
// null-union optional) and delegates the remainder to a full-budget indirect
// (rec.ser / rec.deser); the recurring family bug is a MISSING multi-level-
// pointer decline at one such arm, so it accepts 1+maxIndirectDepth levels where
// the reflect element handler, the encode side, and every other context cap at
// maxIndirectDepth — emitting a wire the same struct encoded as a non-addressable
// VALUE (reflect), a top-level encode, and the wire's own reader all refuse.
//
// This net adds a RECORD base (so []*…*record / *…*record reach the record arms)
// and FIELD-OF-CONTAINER contexts that encode BOTH the addressable *struct (=>
// the unsafe container fast path) and the same struct as a non-addressable value
// (=> reflect), asserting both agree with each other, with the generic any-tree
// encode, and with the explicit cap at every depth {0,1,2,at-cap,past-cap}. A
// double-peeling arm accepts the past-cap depth on the *struct path while the
// reflect-value path rejects it: the divergence the scalar-field matrix above is
// structurally blind to.
// ===========================================================================

// ptrIndRec is the record base for the field-of-container pointer net: a minimal
// fully-unsafe-compileable struct, so a []*…*ptrIndRec / *…*ptrIndRec struct
// field reaches the unsafe record container arms (usArrayPtrRecord,
// usNullUnionRecord, usArrayNullUnionRecord, and their decode twins) until the
// element/optional pointer depth forces a decline back to reflect.
type ptrIndRec struct {
	X int32 `avro:"x"`
}

const ptrIndRecSchema = `{"type":"record","name":"PIRec","fields":[{"name":"x","type":"int"}]}`

// ptrCBase is one base type for the field-of-container pointer-depth net: its
// Avro text, Go base type, a representative typed sample, and the generic
// (any-tree) form. The generic form carries NO Go pointer wrapping, so it always
// encodes (depth-0-equivalent) and is the canonical wire every accepted typed
// depth must match and every decode reads.
type ptrCBase struct {
	label   string
	avro    string
	goType  reflect.Type
	sample  reflect.Value
	generic any
}

func ptrCBases() []ptrCBase {
	// A primitive base exercises the usArrayDirect / usNullUnionPtr /
	// usArrayNullUnionPtr arms (+ the scalar field-pointer fast path on the
	// element); a record base exercises the usArrayPtrRecord / usNullUnionRecord
	// / usArrayNullUnionRecord arms the family bug recurred in. string is a
	// second, differently-shaped primitive.
	return []ptrCBase{
		{"int", `"int"`, reflect.TypeOf(int32(0)), reflect.ValueOf(int32(7)), int32(7)},
		{"string", `"string"`, reflect.TypeOf(""), reflect.ValueOf("v"), "v"},
		{"record", ptrIndRecSchema, reflect.TypeOf(ptrIndRec{}), reflect.ValueOf(ptrIndRec{X: 7}), map[string]any{"x": int32(7)}},
	}
}

// ptrFieldCtx composes a base into an addressable struct field holding a
// container whose element / value / optional sits at pointer depth D. minDepth is
// the shallowest valid depth (0 for array/map; 1 for the null-union arms, whose
// optional IS the first pointer level).
type ptrFieldCtx struct {
	label        string
	minDepth     int
	fieldSchema  func(baseAvro string) string
	fieldType    func(baseGo reflect.Type, depth int) reflect.Type
	setField     func(field reflect.Value, baseSample reflect.Value, depth int)
	genericField func(baseGeneric any) any
	checkField   func(t *testing.T, field reflect.Value, baseSample reflect.Value)
}

func ptrFieldCtxs() []ptrFieldCtx {
	stringType := reflect.TypeOf("")
	// field-array and field-array-nullunion share the []*…*base field shape and
	// the two-element-slice input/check; only the schema and minDepth differ (the
	// null-union element's pointer IS the optional, so depth 0 is value-only).
	sliceType := func(baseGo reflect.Type, depth int) reflect.Type {
		return reflect.SliceOf(ptrTypeOf(baseGo, depth))
	}
	setSlice := func(field reflect.Value, baseSample reflect.Value, depth int) {
		sl := reflect.MakeSlice(field.Type(), 2, 2)
		sl.Index(0).Set(ptrChain(baseSample, depth))
		sl.Index(1).Set(ptrChain(baseSample, depth))
		field.Set(sl)
	}
	genSlice := func(baseGeneric any) any { return []any{baseGeneric, baseGeneric} }
	checkSlice := func(t *testing.T, field reflect.Value, baseSample reflect.Value) {
		t.Helper()
		if field.Len() != 2 {
			t.Fatalf("array field round-trip length: got %d, want 2", field.Len())
		}
		for i := range field.Len() {
			if got := derefAll(field.Index(i)); !ptrValEq(got, baseSample) {
				t.Fatalf("array field[%d] round-trip mismatch: got %v, want %v", i, safeIface(got), safeIface(baseSample))
			}
		}
	}
	return []ptrFieldCtx{
		{
			label:        "field-array",
			minDepth:     0,
			fieldSchema:  func(b string) string { return fmt.Sprintf(`{"type":"array","items":%s}`, b) },
			fieldType:    sliceType,
			setField:     setSlice,
			genericField: genSlice,
			checkField:   checkSlice,
		},
		{
			label:        "field-array-nullunion",
			minDepth:     1,
			fieldSchema:  func(b string) string { return fmt.Sprintf(`{"type":"array","items":["null",%s]}`, b) },
			fieldType:    sliceType,
			setField:     setSlice,
			genericField: genSlice,
			checkField:   checkSlice,
		},
		{
			label:        "field-nullunion",
			minDepth:     1,
			fieldSchema:  func(b string) string { return fmt.Sprintf(`["null",%s]`, b) },
			fieldType:    func(baseGo reflect.Type, depth int) reflect.Type { return ptrTypeOf(baseGo, depth) },
			setField:     func(field reflect.Value, baseSample reflect.Value, depth int) { field.Set(ptrChain(baseSample, depth)) },
			genericField: func(baseGeneric any) any { return baseGeneric },
			checkField: func(t *testing.T, field reflect.Value, baseSample reflect.Value) {
				t.Helper()
				if got := derefAll(field); !ptrValEq(got, baseSample) {
					t.Fatalf("null-union field round-trip mismatch: got %v, want %v", safeIface(got), safeIface(baseSample))
				}
			},
		},
		{
			label:       "field-map",
			minDepth:    0,
			fieldSchema: func(b string) string { return fmt.Sprintf(`{"type":"map","values":%s}`, b) },
			fieldType: func(baseGo reflect.Type, depth int) reflect.Type {
				return reflect.MapOf(stringType, ptrTypeOf(baseGo, depth))
			},
			setField: func(field reflect.Value, baseSample reflect.Value, depth int) {
				m := reflect.MakeMap(field.Type())
				m.SetMapIndex(reflect.ValueOf("k"), ptrChain(baseSample, depth))
				field.Set(m)
			},
			genericField: func(baseGeneric any) any { return map[string]any{"k": baseGeneric} },
			checkField: func(t *testing.T, field reflect.Value, baseSample reflect.Value) {
				t.Helper()
				if got := derefAll(field.MapIndex(reflect.ValueOf("k"))); !ptrValEq(got, baseSample) {
					t.Fatalf("map field[k] round-trip mismatch: got %v, want %v", safeIface(got), safeIface(baseSample))
				}
			},
		},
	}
}

func TestMatrix_GenerativePointerIndirectionUnsafeContainers(t *testing.T) {
	depths := []int{0, 1, 2, ptrIndirectCap, ptrIndirectCap + 1} // {0,1,2,at-cap,past-cap}
	for _, b := range ptrCBases() {
		for _, cx := range ptrFieldCtxs() {
			t.Run(b.label+"/"+cx.label, func(t *testing.T) {
				recSchema := fmt.Sprintf(`{"type":"record","name":"PtrCOuter","fields":[{"name":"f","type":%s}]}`, cx.fieldSchema(b.avro))
				s := avro.MustParse(recSchema)
				res, err := avro.Resolve(s, s)
				if err != nil {
					t.Fatalf("identity Resolve: %v\nschema: %s", err, recSchema)
				}
				// Canonical wires from the generic any-tree input: it carries no Go
				// pointer wrapping, so it always encodes (depth-0-equivalent) and is
				// both the wire every accepted typed depth must match and the wire
				// every decode reads — valid even at the reject depths (whose typed
				// encode fails, so they cannot supply their own wire).
				genVal := map[string]any{"f": cx.genericField(b.generic)}
				cbin, err := s.AppendEncode(nil, genVal)
				if err != nil {
					t.Fatalf("canonical binary encode: %v\nschema: %s", err, recSchema)
				}
				cjson, err := s.AppendEncodeJSON(nil, genVal)
				if err != nil {
					t.Fatalf("canonical JSON encode: %v\nschema: %s", err, recSchema)
				}
				for _, depth := range depths {
					if depth < cx.minDepth {
						continue
					}
					t.Run(fmt.Sprintf("depth=%d", depth), func(t *testing.T) {
						accept := depth <= ptrIndirectCap
						st := reflect.StructOf([]reflect.StructField{
							{Name: "F", Type: cx.fieldType(b.goType, depth), Tag: `avro:"f"`},
						})
						ps := reflect.New(st)
						cx.setField(ps.Elem().Field(0), b.sample, depth)

						// Encode parity: the addressable *struct (=> unsafe struct-field
						// container fast path) and the same struct as a non-addressable
						// VALUE (=> reflect) must agree with each other, with the generic
						// wire, and with the explicit cap. A double-peeling arm accepts
						// the past-cap depth on the *struct path alone.
						for _, fm := range []struct {
							name string
							in   any
						}{
							{"unsafe(*struct)", ps.Interface()},
							{"reflect(struct-value)", ps.Elem().Interface()},
						} {
							binW, binErr := s.AppendEncode(nil, fm.in)
							jsonW, jsonErr := s.AppendEncodeJSON(nil, fm.in)
							if (binErr == nil) != accept {
								t.Fatalf("%s binary encode accept=%v, want %v (err=%v)", fm.name, binErr == nil, accept, binErr)
							}
							if (jsonErr == nil) != accept {
								t.Fatalf("%s JSON encode accept=%v, want %v (err=%v)", fm.name, jsonErr == nil, accept, jsonErr)
							}
							if accept {
								if !bytes.Equal(binW, cbin) {
									t.Fatalf("%s binary wire != generic canonical (pointer wrapping not transparent):\n got=%x\nwant=%x", fm.name, binW, cbin)
								}
								if !bytes.Equal(jsonW, cjson) {
									t.Fatalf("%s JSON wire != generic canonical:\n got=%s\nwant=%s", fm.name, jsonW, cjson)
								}
							}
						}

						// Decode parity: each {wire}×{natural,resolved} path agrees with
						// the cap, decoding into a fresh depth-D typed *struct (=> the
						// unsafe container deser arm); an accepted value round-trips.
						for _, dp := range []struct {
							name string
							dec  func(target any) error
						}{
							{"binary/natural", func(target any) error { _, e := s.Decode(cbin, target); return e }},
							{"binary/resolved", func(target any) error { _, e := res.Decode(cbin, target); return e }},
							{"json/natural", func(target any) error { return s.DecodeJSON(cjson, target) }},
							{"json/resolved", func(target any) error { return res.DecodeJSON(cjson, target) }},
						} {
							target := reflect.New(st)
							err := dp.dec(target.Interface())
							if (err == nil) != accept {
								t.Fatalf("%s decode accept=%v, want %v (err=%v)", dp.name, err == nil, accept, err)
							}
							if accept {
								cx.checkField(t, target.Elem().Field(0), b.sample)
							}
						}
					})
				}
			})
		}
	}
}

// ===========================================================================
// Null-union nil-equivalence parity net (field-of-container).
//
// The 2-branch ["null",T] / [T,"null"] optimization picks the null branch
// exactly when isNilValue reports the value nil — which peels pointer/interface
// layers then nil-checks the bottom kind, so a non-nil pointer to a nil
// slice/map/interface/pointer is null. Three encode paths must agree on the
// branch for the SAME value: the unsafe struct fast path (reached only when the
// struct is addressable, Encode(&v)), the reflect path (Encode(v)), and JSON
// (EncodeJSON). This net crosses nil-equivalent base kind × container context ×
// union position and asserts all three pick the same branch. The unsafe fast
// path makes its nil decision on the outer pointer alone, so it must DECLINE
// every isNilableKind inner to the reflect path; this net is what proves it.
// ===========================================================================

// nilEqThreeWayParity asserts that addr (addressable -> unsafe fast path) and
// val (by value -> reflect path) encode to byte-identical binary, that the two
// JSON encodings agree, and that the binary and JSON wires decode to the same
// value (cross-format branch agreement). target1/target2 are fresh decode
// destinations of the value's concrete type.
func nilEqThreeWayParity(t *testing.T, schemaJSON string, addr, val, target1, target2 any) {
	t.Helper()
	s := avro.MustParse(schemaJSON)

	wAddr, err := s.AppendEncode(nil, addr)
	if err != nil {
		t.Fatalf("Encode(&v) [unsafe]: %v", err)
	}
	wVal, err := s.AppendEncode(nil, val)
	if err != nil {
		t.Fatalf("Encode(v) [reflect]: %v", err)
	}
	if !bytes.Equal(wAddr, wVal) {
		t.Errorf("binary addressable-vs-value branch divergence: Encode(&v)=% x  Encode(v)=% x", wAddr, wVal)
	}

	jAddr, err := s.AppendEncodeJSON(nil, addr)
	if err != nil {
		t.Fatalf("EncodeJSON(&v): %v", err)
	}
	jVal, err := s.AppendEncodeJSON(nil, val)
	if err != nil {
		t.Fatalf("EncodeJSON(v): %v", err)
	}
	if !bytes.Equal(jAddr, jVal) {
		t.Errorf("JSON addressable-vs-value branch divergence: %s vs %s", jAddr, jVal)
	}

	if _, err := s.Decode(wAddr, target1); err != nil {
		t.Fatalf("Decode(binary wire % x): %v", wAddr, err)
	}
	if err := s.DecodeJSON(jAddr, target2); err != nil {
		t.Fatalf("DecodeJSON(%s): %v", jAddr, err)
	}
	if !reflect.DeepEqual(target1, target2) {
		t.Errorf("binary<->JSON branch divergence: binary=%#v  json=%#v  (binWire=% x jsonWire=%s)", target1, target2, wAddr, jAddr)
	}
}

func TestMatrix_NullUnionNilEquivalenceParity(t *testing.T) {
	recField := func(inner string) string {
		return `{"type":"record","name":"R","fields":[{"name":"f","type":` + inner + `}]}`
	}
	recArr := func(items string) string {
		return `{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"array","items":` + items + `}}]}`
	}
	recMapVal := func(values string) string {
		return `{"type":"record","name":"R","fields":[{"name":"m","type":{"type":"map","values":` + values + `}}]}`
	}
	nf := func(x string) string { return `["null",` + x + `]` }
	ns := func(x string) string { return `[` + x + `,"null"]` }

	cases := []struct {
		name string
		run  func(t *testing.T)
	}{
		// ----- FIELD context: *Inner, slice base, both positions -----
		{"field/slice/null-first/nil", func(t *testing.T) {
			type rec struct {
				F *[]string `avro:"f"`
			}
			var x []string
			nilEqThreeWayParity(t, recField(nf(`{"type":"array","items":"string"}`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		{"field/slice/null-second/nil", func(t *testing.T) {
			type rec struct {
				F *[]string `avro:"f"`
			}
			var x []string
			nilEqThreeWayParity(t, recField(ns(`{"type":"array","items":"string"}`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		{"field/slice/null-first/nonnil-control", func(t *testing.T) {
			type rec struct {
				F *[]string `avro:"f"`
			}
			x := []string{"a", "b"}
			nilEqThreeWayParity(t, recField(nf(`{"type":"array","items":"string"}`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		// ----- FIELD context: bytes base (also a Slice inner) -----
		{"field/bytes/null-first/nil", func(t *testing.T) {
			type rec struct {
				F *[]byte `avro:"f"`
			}
			var x []byte
			nilEqThreeWayParity(t, recField(nf(`"bytes"`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		{"field/bytes/null-first/nonnil-control", func(t *testing.T) {
			type rec struct {
				F *[]byte `avro:"f"`
			}
			x := []byte{1, 2, 3}
			nilEqThreeWayParity(t, recField(nf(`"bytes"`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		// ----- FIELD context: map base -----
		{"field/map/null-first/nil", func(t *testing.T) {
			type rec struct {
				F *map[string]string `avro:"f"`
			}
			var x map[string]string
			nilEqThreeWayParity(t, recField(nf(`{"type":"map","values":"string"}`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		{"field/map/null-first/nonnil-control", func(t *testing.T) {
			type rec struct {
				F *map[string]string `avro:"f"`
			}
			x := map[string]string{"k": "v"}
			nilEqThreeWayParity(t, recField(nf(`{"type":"map","values":"string"}`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		// ----- FIELD context: **T (pointer inner), both positions -----
		{"field/ptrptr/null-first/nil", func(t *testing.T) {
			type rec struct {
				F **int `avro:"f"`
			}
			var x *int
			nilEqThreeWayParity(t, recField(nf(`"int"`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		{"field/ptrptr/null-second/nil", func(t *testing.T) {
			type rec struct {
				F **int `avro:"f"`
			}
			var x *int
			nilEqThreeWayParity(t, recField(ns(`"int"`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		{"field/ptrptr/null-first/nonnil-control", func(t *testing.T) {
			type rec struct {
				F **int `avro:"f"`
			}
			n := 7
			x := &n
			nilEqThreeWayParity(t, recField(nf(`"int"`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		// ----- FIELD context: deep chain ***int -----
		{"field/deep-ptr/null-first/nil", func(t *testing.T) {
			type rec struct {
				F ***int `avro:"f"`
			}
			var x **int
			nilEqThreeWayParity(t, recField(nf(`"int"`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		// ----- FIELD context: interface inner (*any) -----
		{"field/iface/null-first/nil", func(t *testing.T) {
			type rec struct {
				F *any `avro:"f"`
			}
			var x any
			nilEqThreeWayParity(t, recField(nf(`"int"`)), &rec{F: &x}, rec{F: &x}, &rec{}, &rec{})
		}},
		// ----- ARRAY-ELEMENT context: []*Inner -----
		{"array-elem/slice/null-first/nil", func(t *testing.T) {
			type rec struct {
				A []*[]string `avro:"a"`
			}
			var x []string
			nilEqThreeWayParity(t, recArr(nf(`{"type":"array","items":"string"}`)), &rec{A: []*[]string{&x}}, rec{A: []*[]string{&x}}, &rec{}, &rec{})
		}},
		{"array-elem/slice/null-second/nil", func(t *testing.T) {
			type rec struct {
				A []*[]string `avro:"a"`
			}
			var x []string
			nilEqThreeWayParity(t, recArr(ns(`{"type":"array","items":"string"}`)), &rec{A: []*[]string{&x}}, rec{A: []*[]string{&x}}, &rec{}, &rec{})
		}},
		{"array-elem/bytes/null-first/nil", func(t *testing.T) {
			type rec struct {
				A []*[]byte `avro:"a"`
			}
			var x []byte
			nilEqThreeWayParity(t, recArr(nf(`"bytes"`)), &rec{A: []*[]byte{&x}}, rec{A: []*[]byte{&x}}, &rec{}, &rec{})
		}},
		{"array-elem/ptrptr/null-first/nil", func(t *testing.T) {
			type rec struct {
				A []**int `avro:"a"`
			}
			var x *int
			nilEqThreeWayParity(t, recArr(nf(`"int"`)), &rec{A: []**int{&x}}, rec{A: []**int{&x}}, &rec{}, &rec{})
		}},
		{"array-elem/slice/null-first/nonnil-control", func(t *testing.T) {
			type rec struct {
				A []*[]string `avro:"a"`
			}
			x := []string{"z"}
			nilEqThreeWayParity(t, recArr(nf(`{"type":"array","items":"string"}`)), &rec{A: []*[]string{&x}}, rec{A: []*[]string{&x}}, &rec{}, &rec{})
		}},
		// ----- MAP-VALUE context: map[string]*Inner (declines to reflect both) -----
		{"map-value/slice/null-first/nil", func(t *testing.T) {
			type rec struct {
				M map[string]*[]string `avro:"m"`
			}
			var x []string
			nilEqThreeWayParity(t, recMapVal(nf(`{"type":"array","items":"string"}`)), &rec{M: map[string]*[]string{"k": &x}}, rec{M: map[string]*[]string{"k": &x}}, &rec{}, &rec{})
		}},
		// ----- NESTED context: nullunion field inside a nested record -----
		{"nested-record-field/slice/null-first/nil", func(t *testing.T) {
			type inner struct {
				F *[]string `avro:"f"`
			}
			type outer struct {
				M inner `avro:"m"`
			}
			var x []string
			sch := `{"type":"record","name":"O","fields":[{"name":"m","type":{"type":"record","name":"M","fields":[{"name":"f","type":["null",{"type":"array","items":"string"}]}]}}]}`
			nilEqThreeWayParity(t, sch, &outer{M: inner{F: &x}}, outer{M: inner{F: &x}}, &outer{}, &outer{})
		}},
	}

	for _, c := range cases {
		t.Run(c.name, c.run)
	}
}
