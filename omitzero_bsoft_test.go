package avro_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/twmb/avro"
)

// TestRegression_OmitzeroFillsSchemaDefault pins the b-soft omitzero contract:
// on a zero/IsZero value, omitzero encodes the field's default if it has one,
// else null if the field is nullable, else nothing (encode the zero — a forced
// no-op). It therefore matches map[string]any default-fill wherever a default
// exists; it deliberately diverges for a nullable field with NO default, where
// omitzero encodes null while map-fill errors ("missing key").
func TestRegression_OmitzeroFillsSchemaDefault(t *testing.T) {
	type R struct {
		Count int `avro:"Count,omitzero"`
	}
	cases := []struct {
		name, schema, wantHex string
		mapParity             bool // struct-omitzero wire must equal map{} default-fill wire
	}{
		// WITH a default → fill it (was buggy: emitted 0 / null). Matches map-fill.
		{"non-union int default", `{"type":"record","name":"R","fields":[{"name":"Count","type":"int","default":10}]}`, "14", true},
		{"null-second union default", `{"type":"record","name":"R","fields":[{"name":"Count","type":["int","null"],"default":5}]}`, "000a", true},
		// nullable NO default → null (preserved; diverges from map-fill, which errors).
		{"null-second union no default", `{"type":"record","name":"R","fields":[{"name":"Count","type":["int","null"]}]}`, "02", false},
		// non-union NO default → no-op, encode the zero (preserved).
		{"non-union int no default", `{"type":"record","name":"R","fields":[{"name":"Count","type":"int"}]}`, "00", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := avro.MustParse(tc.schema)
			structWire, err := s.AppendEncode(nil, R{Count: 0})
			if err != nil {
				t.Fatalf("encode struct: %v", err)
			}
			if got := fmt.Sprintf("%x", structWire); got != tc.wantHex {
				t.Errorf("omitzero wire = %s, want %s", got, tc.wantHex)
			}
			// The unsafe (addressable) path must agree with the reflect path
			// — it delegates actionable omitzero to the reflect slow path, so
			// this pins that the delegation stays correct.
			ptrWire, err := s.AppendEncode(nil, &R{Count: 0})
			if err != nil {
				t.Fatalf("encode &struct (unsafe path): %v", err)
			}
			if !bytes.Equal(structWire, ptrWire) {
				t.Errorf("unsafe path diverges from reflect: value=%x ptr=%x", structWire, ptrWire)
			}
			if tc.mapParity {
				// Binary parity with map[string]any default-fill (the oracle).
				mapWire, err := s.AppendEncode(nil, map[string]any{})
				if err != nil {
					t.Fatalf("encode map{}: %v", err)
				}
				if !bytes.Equal(structWire, mapWire) {
					t.Errorf("omitzero != map default-fill (binary): struct=%x map=%x", structWire, mapWire)
				}
				// JSON path parity with the same oracle.
				sj, err := s.EncodeJSON(R{Count: 0})
				if err != nil {
					t.Fatalf("encode struct JSON: %v", err)
				}
				mj, err := s.EncodeJSON(map[string]any{})
				if err != nil {
					t.Fatalf("encode map{} JSON: %v", err)
				}
				if !bytes.Equal(sj, mj) {
					t.Errorf("omitzero != map default-fill (JSON): struct=%s map=%s", sj, mj)
				}
			}
		})
	}
}
