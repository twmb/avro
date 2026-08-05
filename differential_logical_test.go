package avro_test

import (
	"encoding/hex"
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// TestDifferentialFastavroBinaryLogical extends the fastavro differential
// (differential_test.go) to the binary- and logical-typed values JSON cannot
// carry directly: bytes, fixed, decimal, uuid, and timestamps. These are the
// belief-heavy types a self-consistent round-trip cannot police -- a symmetric
// encode/decode bug round-trips cleanly yet writes the wrong wire. fastavro
// reconstructs each value INDEPENDENTLY from the Kind-tagged transport (a
// base64 string, a decimal string, an epoch integer) and encodes it, so byte
// parity here means twmb and fastavro independently agree on the wire -- the
// check that catches a symmetric bug. Skips (does not fail) without fastavro;
// CI sets AVRO_FASTAVRO_PYTHON. See differential_test.go and the oracle README.

type diffTypedSeed struct {
	name    string
	schema  string
	goValue any    // value handed to twmb.Encode
	kind    string // transport kind for the oracle ("" = plain JSON of oracle)
	oracle  any    // value sent to the oracle; nil => marshal goValue directly
}

func diffTypedSeeds() []diffTypedSeed {
	return []diffTypedSeed{
		// bytes: goValue []byte marshals to a base64 string; kind tells the
		// oracle to base64-decode it back to Python bytes.
		{"bytes", `"bytes"`, []byte{0x00, 0x01, 0x7f, 0x80, 0xff}, "bytes", nil},
		{"bytes empty", `"bytes"`, []byte{}, "bytes", nil},
		{"bytes high", `"bytes"`, []byte{0xde, 0xad, 0xbe, 0xef}, "bytes", nil},

		// fixed: raw N bytes, no length prefix.
		{"fixed8", `{"type":"fixed","name":"F8","size":8}`, []byte{1, 2, 3, 4, 5, 6, 7, 8}, "fixed", nil},
		{"fixed1", `{"type":"fixed","name":"F1","size":1}`, []byte{0xff}, "fixed", nil},

		// decimal (bytes): twmb encodes a *big.Rat; fastavro builds the same
		// value from a decimal string and encodes independently.
		{"decimal 123.45", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, big.NewRat(12345, 100), "decimal", "123.45"},
		{"decimal -0.01", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, big.NewRat(-1, 100), "decimal", "-0.01"},
		{"decimal 0.00", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, big.NewRat(0, 1), "decimal", "0.00"},
		{"decimal 99999999.99", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, big.NewRat(9999999999, 100), "decimal", "99999999.99"},

		// uuid logical on string: the wire is the plain string, so no special
		// transport is needed (kind "").
		{"uuid string", `{"type":"string","logicalType":"uuid"}`, "550e8400-e29b-41d4-a716-446655440000", "", nil},

		// timestamp logical on long: twmb encodes a time.Time; fastavro builds
		// the same instant from an epoch integer.
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, time.UnixMilli(1600000000123).UTC(), "timestamp-millis", int64(1600000000123)},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`, time.UnixMicro(1600000000123456).UTC(), "timestamp-micros", int64(1600000000123456)},
		{"timestamp-millis epoch", `{"type":"long","logicalType":"timestamp-millis"}`, time.UnixMilli(0).UTC(), "timestamp-millis", int64(0)},
	}
}

func TestDifferentialFastavroBinaryLogical(t *testing.T) {
	o := startOracle(t)
	var encParity, faRead, twmbRead int
	for _, sd := range diffTypedSeeds() {
		t.Run(sd.name, func(t *testing.T) {
			s, err := avro.Parse(sd.schema)
			if err != nil {
				t.Fatalf("twmb Parse: %v", err)
			}

			oracleVal := sd.oracle
			if oracleVal == nil {
				oracleVal = sd.goValue
			}
			valJSON, err := json.Marshal(oracleVal)
			if err != nil {
				t.Fatalf("marshal oracle value: %v", err)
			}

			// twmb encode.
			bTwmb, err := s.Encode(sd.goValue)
			if err != nil {
				t.Fatalf("twmb Encode(%#v): %v", sd.goValue, err)
			}

			// fastavro encode, reconstructing the value from the transport.
			enc := o.call(oracleJob{Op: "encode", Schema: json.RawMessage(sd.schema), Value: valJSON, Kind: sd.kind})
			if !enc.OK {
				t.Fatalf("fastavro encode failed: %s", enc.Err)
			}

			// (1) encode byte parity — twmb and fastavro independently agree.
			if got := hex.EncodeToString(bTwmb); got != enc.Hex {
				t.Errorf("encode byte mismatch vs fastavro:\n twmb     %s\n fastavro %s", got, enc.Hex)
			} else {
				encParity++
			}

			// (2) fastavro reads twmb's bytes.
			if d := o.call(oracleJob{Op: "decode", Schema: json.RawMessage(sd.schema), Hex: hex.EncodeToString(bTwmb)}); !d.OK {
				t.Errorf("fastavro cannot decode twmb's bytes: %s", d.Err)
			} else {
				faRead++
			}

			// (3) twmb reads fastavro's bytes.
			faBytes, err := hex.DecodeString(enc.Hex)
			if err != nil {
				t.Fatalf("decode fastavro hex: %v", err)
			}
			var out any
			if _, err := s.Decode(faBytes, &out); err != nil {
				t.Errorf("twmb cannot decode fastavro's bytes: %v", err)
			} else {
				twmbRead++
			}
		})
	}
	t.Logf("fastavro binary/logical differential: %d encode-parity, %d fastavro-reads-twmb, %d twmb-reads-fastavro", encParity, faRead, twmbRead)
}
