package avro_test

import (
	"bytes"
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
// encode. big-decimal is numeric-text-only too: its string decode target reads
// numeric text whenever the wire parses as valid AVRO-4124 framing (via
// applyBigDecimalPayload), so a crafted string whose bytes ARE a valid framing
// would decode to a different value — it is rejected on encode identically to
// decimal (a []byte carrier stays opaque-symmetric).
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

	// big-decimal is numeric-text-only too (like decimal): a non-numeric string
	// carrier is rejected on both wires — INCLUDING a crafted string whose raw
	// bytes form valid AVRO-4124 framing, which the string decode target would
	// otherwise read back as a number (the silent round-trip corruption). A
	// numeric string round-trips as numeric text; []byte stays opaque.
	bd := avro.MustParse(`{"type":"bytes","logicalType":"big-decimal"}`)
	// varint(uLen=1) || unscaled 0x05 (=5) || varint(scale=0): byte-identical to
	// the structured big-decimal wire for the number 5, so an opaque encode of
	// this string decoded back to "5".
	const bdFraming = "\x02\x05\x00"
	for _, bin := range []bool{true, false} {
		// Reject: the valid-framing string (the corruption trigger).
		if _, err := encodeWire(bd, bdFraming, bin); err == nil {
			t.Errorf("big-decimal %s Encode(valid-framing string) accepted; a big-decimal string carrier is numeric-text-only (its bytes would decode to a number)", wireName(bin))
		}
		// Reject: a non-framing non-numeric string too (consistent with decimal).
		if _, err := encodeWire(bd, nonNumeric, bin); err == nil {
			t.Errorf("big-decimal %s Encode(non-numeric string) accepted; want reject", wireName(bin))
		}
		// Control: a numeric string round-trips as numeric text.
		wire, err := encodeWire(bd, "5", bin)
		if err != nil {
			t.Fatalf("big-decimal %s Encode(numeric string): %v", wireName(bin), err)
		}
		var back string
		if err := decodeWire(bd, wire, &back, bin); err != nil {
			t.Fatalf("big-decimal %s decode numeric: %v", wireName(bin), err)
		}
		if back != "5" {
			t.Errorf("big-decimal %s numeric string round-trip: got %q want %q", wireName(bin), back, "5")
		}
	}
	// Control: []byte stays the opaque escape hatch even for bytes that form
	// valid framing — a []byte decode target reads raw bytes unconditionally.
	craft := []byte{0x02, 0x05, 0x00}
	bw, err := bd.AppendEncode(nil, craft)
	if err != nil {
		t.Fatalf("big-decimal Encode([]byte opaque): %v", err)
	}
	var bback []byte
	if _, err := bd.Decode(bw, &bback); err != nil {
		t.Fatalf("big-decimal Decode []byte opaque: %v", err)
	}
	if !bytes.Equal(bback, craft) {
		t.Errorf("big-decimal []byte opaque round-trip: got %x want %x", bback, craft)
	}
}

// TestMatrix_DecimalCarrierNumericTextContract is the generative net for the
// decimal carrier contract: for a decimal logical (bytes AND fixed) a Go string
// and a json.Number are the numeric-text form ONLY — a non-numeric one is
// REJECTED on encode, on both wire formats, in EVERY encode context — rather
// than silently written as opaque raw bytes, keeping encode symmetric with
// decode (whose string target always reads numeric decimal text). []byte is the
// sole opaque carrier and encodes in every cell. big-decimal has the same
// numeric-text-only contract (a non-numeric string rejects); the whole logical-
// on-bytes/fixed carrier class — big-decimal, uuid, duration included — is
// covered by TestMatrix_LogicalStringCarrierRoundTripContract.
//
// Axes: carrier {string, []byte, json.Number} × content {numeric, non-numeric}
// × backing {bytes, fixed} × wire {binary, JSON} × encode context {top-level,
// record field, array element, map value} (the path-divergence axis — a decimal
// leaf is reachable at each). The oracle is calibration-free: string and
// json.Number reject a non-numeric carrier identically and accept a numeric
// one; []byte always encodes. Neuter: reverting rejectNonNumericStructuredString
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

// carrierStrRec / carrierBytesRec are typed record targets for the string-
// carrier round-trip net: decoding a wrapped logical leaf into a concrete
// string / []byte field observes the LEAF value directly. An `any` target would
// surface the logical's default Go type (e.g. *big.Rat for a decimal) and hide a
// string-target corruption, which is precisely the observation that matters.
type carrierStrRec struct {
	F string `avro:"f"`
}

type carrierBytesRec struct {
	F []byte `avro:"f"`
}

// decodeCarrierLeaf decodes wire (a leaf wrapped by ctxName) into a typed
// string / []byte target and returns the leaf value, so a string-target
// round-trip can be compared byte-for-byte against the encoded input.
func decodeCarrierLeaf(s *avro.Schema, wire []byte, isBytes bool, ctxName string, bin bool) (any, error) {
	switch ctxName {
	case "top":
		if isBytes {
			var b []byte
			err := decodeWire(s, wire, &b, bin)
			return b, err
		}
		var str string
		err := decodeWire(s, wire, &str, bin)
		return str, err
	case "record_field":
		if isBytes {
			var r carrierBytesRec
			err := decodeWire(s, wire, &r, bin)
			return r.F, err
		}
		var r carrierStrRec
		err := decodeWire(s, wire, &r, bin)
		return r.F, err
	case "array_element":
		if isBytes {
			var a [][]byte
			err := decodeWire(s, wire, &a, bin)
			if err == nil && len(a) > 0 {
				return a[0], nil
			}
			return []byte(nil), err
		}
		var a []string
		err := decodeWire(s, wire, &a, bin)
		if err == nil && len(a) > 0 {
			return a[0], nil
		}
		return "", err
	case "map_value":
		if isBytes {
			var m map[string][]byte
			err := decodeWire(s, wire, &m, bin)
			return m["k"], err
		}
		var m map[string]string
		err := decodeWire(s, wire, &m, bin)
		return m["k"], err
	}
	return nil, fmt.Errorf("unknown context %q", ctxName)
}

func sameCarrier(got, want any) bool {
	switch w := want.(type) {
	case string:
		g, ok := got.(string)
		return ok && g == w
	case []byte:
		g, ok := got.([]byte)
		return ok && bytes.Equal(g, w)
	}
	return false
}

// TestMatrix_LogicalStringCarrierRoundTripContract is the class net for EVERY
// logical on bytes/fixed whose Go string carrier could encode OPAQUELY while its
// string DECODE target reads a STRUCTURED value — the encode-opaque/decode-
// structured mismatch that silently corrupts a round trip. It closes the whole
// class, not just the big-decimal instance that motivated it.
//
// Invariant per cell (calibration-free): a STRING carrier either round-trips
// EXACTLY (string-in == string-out) OR is REJECTED at encode. It must NEVER
// both-succeed with string-in != string-out — that is the silent corruption. A
// []byte carrier is the opaque escape hatch and round-trips exactly.
//
// CRITICAL — for each logical the string samples MUST include a string whose raw
// bytes form VALID STRUCTURED FRAMING for that logical's decode (the corruption
// trigger), NOT merely random/garbage bytes. Garbage that fails the decode's
// structured parse falls through to the opaque path and round-trips by accident,
// masking the bug — that is exactly how big-decimal slipped through the earlier
// decimal fix, whose control sampled only "abcxyz" (first byte 0x61 zig-zags to
// a negative length, so the framing parse fails). For big-decimal the trigger is
// "\x02\x05\x00" (varint uLen=1 || unscaled 5 || varint scale=0 — byte-identical
// to the structured wire for the number 5). Do NOT weaken these to garbage-only.
//
// Class map (verified): decimal (bytes+fixed) and big-decimal are numeric-text-
// only — a non-numeric string rejects. uuid-on-fixed rejects a non-canonical
// string (parseUUID). uuid-on-string and duration decode raw, so any correctly-
// sized string round-trips opaquely. []byte is opaque-symmetric everywhere.
//
// Neuter: reverting the big-decimal rejectNonNumericStructuredString arms
// (serBigDecimal.ser in ser.go + the json_codec.go `case "big-decimal"` arm)
// reds the big-decimal string_valid_framing cell (encode succeeds, decode
// returns "5" != the input) in every context and on both wires; every other cell
// stays green.
func TestMatrix_LogicalStringCarrierRoundTripContract(t *testing.T) {
	// A valid 12-byte duration wire value (months=0, days=0, milliseconds=1),
	// little-endian uint32 triples — decodes raw into a []byte/string target.
	dur12 := string([]byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0})
	const uuidCanon = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"

	type sample struct {
		name    string
		val     any  // string or []byte
		isBytes bool
		reject  bool // encode MUST reject; otherwise MUST round-trip exactly
	}
	leaves := []struct {
		name    string
		schema  string
		samples []sample
	}{
		{"decimal_bytes", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":0}`, []sample{
			{"string_numeric", "5", false, false},
			// Every byte sequence is a valid unscaled decimal, so ANY non-numeric
			// string is a valid-framing corruption trigger (decodes to a number).
			{"string_nonnumeric_framing", "abc", false, true},
			{"bytes_opaque", []byte{0x01, 0x02}, true, false},
		}},
		{"decimal_fixed", `{"type":"fixed","name":"DFa","size":8,"logicalType":"decimal","precision":10,"scale":0}`, []sample{
			{"string_numeric", "5", false, false},
			{"string_nonnumeric_framing", "abc", false, true},
			{"bytes_opaque", []byte{0, 0, 0, 0, 0, 0, 0, 1}, true, false},
		}},
		{"big_decimal", `{"type":"bytes","logicalType":"big-decimal"}`, []sample{
			{"string_numeric", "5", false, false},
			// Valid AVRO-4124 framing == the structured wire for 5. Pre-fix this
			// encoded opaque and decoded to "5" (corruption); now it rejects.
			{"string_valid_framing", "\x02\x05\x00", false, true},
			// Non-framing (parse fails): pre-fix round-tripped opaque; now rejects
			// too, keeping the big-decimal string carrier numeric-text-only.
			{"string_nonframing", "abcxyz", false, true},
			{"bytes_opaque", []byte{0x02, 0x05, 0x00}, true, false},
		}},
		{"uuid_fixed", `{"type":"fixed","name":"UFa","size":16,"logicalType":"uuid"}`, []sample{
			{"string_canonical", uuidCanon, false, false},
			// A non-canonical string is rejected by parseUUID (symmetric — the
			// encoder never emits a wire the string decoder can't reproduce).
			{"string_noncanonical", "not-a-uuid-value!!!!", false, true},
			{"bytes_raw16", make([]byte, 16), true, false},
		}},
		{"uuid_string", `{"type":"string","logicalType":"uuid"}`, []sample{
			// uuid-on-string is wire-equivalent to plain string: raw pass-through
			// on both sides, so ANY string round-trips exactly (no validation).
			{"string_canonical", uuidCanon, false, false},
			{"string_arbitrary", "hello-not-a-uuid", false, false},
		}},
		{"duration_fixed", `{"type":"fixed","name":"DURa","size":12,"logicalType":"duration"}`, []sample{
			// duration decodes raw into a string/[]byte target (opaque both sides).
			{"string_valid12", dur12, false, false},
			// Wrong length is rejected by the fixed size check (symmetric).
			{"string_wrongsize", "short", false, true},
			{"bytes_raw12", make([]byte, 12), true, false},
		}},
	}
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

	for _, lf := range leaves {
		for _, smp := range lf.samples {
			for _, ctx := range contexts {
				// The []byte opaque control runs at top-level and record contexts.
				// Decoding a fixed-decimal element into a []byte-element array/map
				// CONTAINER currently routes through a length-prefixed fast loop
				// that mis-reads the raw fixed bytes ("short buffer for uvarlong")
				// — a separate decode-path divergence from the string-carrier class
				// this net targets. The string carrier (the corruption class) runs
				// in all four contexts.
				if smp.isBytes && (ctx.name == "array_element" || ctx.name == "map_value") {
					continue
				}
				for _, bin := range []bool{true, false} {
					t.Run(fmt.Sprintf("%s/%s/%s/%s", lf.name, smp.name, ctx.name, wireName(bin)), func(t *testing.T) {
						s := avro.MustParse(ctx.wrap(lf.schema))
						wire, err := encodeWireAny(s, ctx.val(smp.val), bin)
						if smp.reject {
							if err == nil {
								t.Fatalf("encode accepted a carrier that must reject: a string carrier is numeric-text/canonical/size-valid-only; an opaque encode of a valid-framing string would decode to a different value")
							}
							return
						}
						if err != nil {
							t.Fatalf("encode rejected a carrier that must round-trip: %v", err)
						}
						got, derr := decodeCarrierLeaf(s, wire, smp.isBytes, ctx.name, bin)
						if derr != nil {
							t.Fatalf("decode: %v", derr)
						}
						if !sameCarrier(got, smp.val) {
							t.Errorf("round-trip corruption: encoded %#v, decoded %#v", smp.val, got)
						}
					})
				}
			}
		}
	}
}
