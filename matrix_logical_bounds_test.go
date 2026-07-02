package avro_test

import (
	"bytes"
	"encoding/json"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// Logical-type boundary axis: every time/decimal logical at the edges of its
// representable range. Two relational invariants, no policy assumptions:
//
//   - known-good extremes round-trip EXACTLY (typed in, typed out, both
//     wires, byte-stable re-encode);
//   - for raw boundary WIRES (MaxInt64/MinInt64 and ±1 around each unit
//     conversion), decode either errors or yields a value that re-encodes
//     onto the identical wire — silent value corruption is the only
//     forbidden outcome.
// ---------------------------------------------------------------------------

func TestMatrix_LogicalTimeExtremes(t *testing.T) {
	cases := []struct {
		label  string
		schema string
		values []any // typed extremes that must round-trip exactly
	}{
		{"date", `{"type":"int","logicalType":"date"}`, []any{
			time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC),
		}},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`, []any{
			time.Duration(0),
			23*time.Hour + 59*time.Minute + 59*time.Second + 999*time.Millisecond,
		}},
		{"time-micros", `{"type":"long","logicalType":"time-micros"}`, []any{
			time.Duration(0),
			23*time.Hour + 59*time.Minute + 59*time.Second + 999999*time.Microsecond,
		}},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, []any{
			time.UnixMilli(0).UTC(),
			time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(9999, 12, 31, 23, 59, 59, 999000000, time.UTC),
		}},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`, []any{
			time.UnixMicro(0).UTC(),
			time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC),
		}},
		// timestamp-nanos: int64 nanoseconds bound the instant range to
		// ~[1677, 2262]; both edges exactly.
		{"timestamp-nanos", `{"type":"long","logicalType":"timestamp-nanos"}`, []any{
			time.Unix(0, math.MaxInt64).UTC(),
			time.Unix(0, math.MinInt64).UTC(),
			time.Unix(0, 0).UTC(),
		}},
		{"local-timestamp-millis", `{"type":"long","logicalType":"local-timestamp-millis"}`, []any{
			time.Date(9999, 12, 31, 23, 59, 59, 999000000, time.UTC),
		}},
	}
	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			for _, v := range c.values {
				runCore(t, c.schema, v)
			}
		})
	}
}

// Raw boundary wires through every long/int-backed logical: decode-then-
// re-encode must be the identity wherever decode succeeds.
func TestMatrix_LogicalBoundaryWires(t *testing.T) {
	longWires := [][]byte{
		appendZig(nil, math.MaxInt64),
		appendZig(nil, math.MinInt64),
		appendZig(nil, math.MaxInt64-1),
		appendZig(nil, math.MinInt64+1),
		appendZig(nil, 0),
	}
	intWires := [][]byte{
		appendZig(nil, math.MaxInt32),
		appendZig(nil, math.MinInt32),
		appendZig(nil, 0),
	}
	schemas := []struct {
		label  string
		schema string
		wires  [][]byte
	}{
		{"date", `{"type":"int","logicalType":"date"}`, intWires},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`, intWires},
		{"time-micros", `{"type":"long","logicalType":"time-micros"}`, longWires},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, longWires},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`, longWires},
		{"timestamp-nanos", `{"type":"long","logicalType":"timestamp-nanos"}`, longWires},
		{"local-timestamp-millis", `{"type":"long","logicalType":"local-timestamp-millis"}`, longWires},
		{"local-timestamp-micros", `{"type":"long","logicalType":"local-timestamp-micros"}`, longWires},
		{"local-timestamp-nanos", `{"type":"long","logicalType":"local-timestamp-nanos"}`, longWires},
	}
	for _, sc := range schemas {
		t.Run(sc.label, func(t *testing.T) {
			s := avro.MustParse(sc.schema)
			for _, w := range sc.wires {
				var a any
				rest, err := s.Decode(w, &a)
				if err != nil {
					continue // a bounded reject is a legal outcome
				}
				if len(rest) != 0 {
					t.Fatalf("wire %x: %d leftover bytes", w, len(rest))
				}
				re, err := s.AppendEncode(nil, a)
				if err != nil {
					t.Fatalf("wire %x decoded to %#v which cannot re-encode: %v", w, a, err)
				}
				if !bytes.Equal(re, w) {
					t.Fatalf("silent boundary corruption:\n wire=%x\n re  =%x\n via %#v", w, re, a)
				}
			}
		})
	}
}

// appendZig writes a zigzag varint (test-local; mirrors the wire format).
func appendZig(dst []byte, n int64) []byte {
	u := uint64(n)<<1 ^ uint64(n>>63)
	for u >= 0x80 {
		dst = append(dst, byte(u)|0x80)
		u >>= 7
	}
	return append(dst, byte(u))
}

// Duration at the uint32 edges, and decimal at the precision boundary.
func TestMatrix_DurationAndDecimalEdges(t *testing.T) {
	t.Run("duration-uint32-max", func(t *testing.T) {
		schema := `{"type":"fixed","name":"DBE","size":12,"logicalType":"duration"}`
		for _, v := range []any{
			avro.Duration{Months: math.MaxUint32, Days: math.MaxUint32, Milliseconds: math.MaxUint32},
			avro.Duration{},
			avro.Duration{Months: 1},
		} {
			runCore(t, schema, v)
		}
	})
	t.Run("decimal-precision-boundary", func(t *testing.T) {
		s := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`)
		// 99.99 = 4 digits at scale 2: the maximum magnitude.
		for _, ok := range []*big.Rat{
			big.NewRat(9999, 100), big.NewRat(-9999, 100), big.NewRat(0, 1),
		} {
			if _, err := s.AppendEncode(nil, ok); err != nil {
				t.Errorf("at-precision value %v rejected: %v", ok, err)
			}
		}
		// 100.00 needs 5 digits: one past the boundary rejects.
		for _, bad := range []*big.Rat{
			big.NewRat(10000, 100), big.NewRat(-10000, 100),
		} {
			if _, err := s.AppendEncode(nil, bad); err == nil {
				t.Errorf("over-precision value %v accepted", bad)
			}
		}
		// And the at-boundary value round-trips both wires exactly.
		runCore(t, `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`, big.NewRat(9999, 100))
	})
	t.Run("decimal-fixed-capacity-boundary", func(t *testing.T) {
		// fixed(2) holds 15 bits of two's complement: ±~3.27e4 unscaled.
		s := avro.MustParse(`{"type":"fixed","name":"DCF","size":2,"logicalType":"decimal","precision":4,"scale":0}`)
		for _, ok := range []*big.Rat{big.NewRat(9999, 1), big.NewRat(-9999, 1)} {
			if _, err := s.AppendEncode(nil, ok); err != nil {
				t.Errorf("fits-in-fixed value %v rejected: %v", ok, err)
			}
		}
	})
	t.Run("decimal-over-precision-wire-decodes", func(t *testing.T) {
		// VALUE precision — the unscaled magnitude's digit count against the
		// declared "precision" — is an ENCODE-side check only, matching Java's
		// Conversions.DecimalConversion: toBytes/toFixed run validate(), while
		// fromBytes/fromFixed build the BigDecimal unchecked. So a wire whose
		// unscaled value EXCEEDS the declared precision but still fits the byte
		// container must DECODE to a *big.Rat on both wire formats, and
		// re-encoding that same value must reject with the precision error.
		// Both directions are pinned: adding a decode-side precision reject
		// would refuse valid foreign wire Java accepts; relaxing the encode
		// check would diverge from Java's validate().
		overUnscaled := []byte{0x41, 0x41, 0x41, 0x41} // 1094795585: 10 digits > precision 9
		overRat := big.NewRat(1094795585, 100)         // at scale 2
		for _, tc := range []struct {
			name    string
			schema  string
			binWire []byte // binary wire carrying overUnscaled
		}{
			{"fixed", `{"type":"fixed","name":"DOP","size":4,"logicalType":"decimal","precision":9,"scale":2}`,
				overUnscaled},
			{"bytes", `{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}`,
				append([]byte{0x08}, overUnscaled...)}, // zigzag(len 4) prefix
		} {
			t.Run(tc.name, func(t *testing.T) {
				s := avro.MustParse(tc.schema)
				var fromBin big.Rat
				if _, err := s.Decode(tc.binWire, &fromBin); err != nil {
					t.Fatalf("binary decode of over-precision wire: %v", err)
				}
				if fromBin.Cmp(overRat) != 0 {
					t.Fatalf("binary decode: got %v, want %v", &fromBin, overRat)
				}
				// JSON wire: the spec codepoint-per-byte string of the unscaled
				// bytes (0x41 = "A").
				var fromJSON big.Rat
				if err := s.DecodeJSON([]byte(`"AAAA"`), &fromJSON); err != nil {
					t.Fatalf("JSON decode of over-precision wire: %v", err)
				}
				if fromJSON.Cmp(overRat) != 0 {
					t.Fatalf("JSON decode: got %v, want %v", &fromJSON, overRat)
				}
				// Re-encoding the decoded value rejects on BOTH wire formats,
				// specifically via the precision check.
				if _, err := s.AppendEncode(nil, &fromBin); err == nil || !strings.Contains(err.Error(), "exceeds schema precision") {
					t.Fatalf("binary re-encode of over-precision value: want precision reject, got %v", err)
				}
				if _, err := s.AppendEncodeJSON(nil, &fromBin); err == nil || !strings.Contains(err.Error(), "exceeds schema precision") {
					t.Fatalf("JSON re-encode of over-precision value: want precision reject, got %v", err)
				}
				// One digit narrower (9 digits = the declared precision) both
				// decodes and re-encodes: the boundary sits between the two
				// values, so the asymmetry above is precision-driven, not
				// container-driven.
				within := big.NewRat(123456789, 100)
				wbin, err := s.AppendEncode(nil, within)
				if err != nil {
					t.Fatalf("at-precision encode: %v", err)
				}
				var back big.Rat
				if _, err := s.Decode(wbin, &back); err != nil || back.Cmp(within) != 0 {
					t.Fatalf("at-precision round-trip: err=%v got=%v want=%v", err, &back, within)
				}
			})
		}
	})
}

// The decimalScaleLimit magnitude gate in boundedRatFromString must sit at
// EXACTLY ±65536 for string-form decimals whose exponent interacts with a
// fractional part: for "1.5e<E>" the net magnitude is E-1 (one fractional
// digit), so E=65537 is the last value the gate passes and E=65538 the
// first it rejects (mirrored on the negative side). The two sides are
// distinguished by WHICH error fires — the gate's "magnitude exceeds"
// versus the schema's downstream precision/scale rejection — so a shifted
// boundary (mis-derived fractional length) flips an assertion even though
// every input here errors. Pins the gate position itself, which no
// round-trip or oracle axis can see (the cap is twmb defense-in-depth).
func TestMatrix_DecimalStringMagnitudeBoundary(t *testing.T) {
	s := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}`)
	cases := []struct {
		in           string
		magnitudeErr bool // true: the ±65536 gate fires; false: it must NOT
	}{
		{"1.5e65538", true},   // netExp 65537: one past the limit
		{"1.5e65537", false},  // netExp 65536: at the limit — gate passes
		{"1.5e-65536", true},  // netExp -65537: one past, negative side
		{"1.5e-65535", false}, // netExp -65536: at the limit, negative side
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			// Encode-side caller (string/json.Number → decimal coercion).
			_, err := s.AppendEncode(nil, json.Number(c.in))
			if err == nil {
				t.Fatalf("encode %s: expected an error (precision 6 cannot hold it)", c.in)
			}
			if got := strings.Contains(err.Error(), "magnitude exceeds"); got != c.magnitudeErr {
				t.Fatalf("encode %s: magnitude-gate fired=%v want %v (err: %v)", c.in, got, c.magnitudeErr, err)
			}
			// JSON-decode caller (bare-number decimal form). Decode has no
			// precision check on this leniency path, so the discriminator
			// is sharper: at-limit values SUCCEED outright, past-limit
			// values fail with the gate's error.
			var sink any
			derr := s.DecodeJSON([]byte(c.in), &sink)
			if c.magnitudeErr {
				if derr == nil || !strings.Contains(derr.Error(), "magnitude exceeds") {
					t.Fatalf("decodeJSON %s: want magnitude-gate error, got %v", c.in, derr)
				}
			} else if derr != nil {
				t.Fatalf("decodeJSON %s: at-limit value must decode, got %v", c.in, derr)
			}
		})
	}
}
