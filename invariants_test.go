package avro_test

import (
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// Tier-4 executable invariant (CORRECTNESS_PLAN.md): the encode/decode
// target-type parity rule from BUG_AUDIT.md pattern 12 -- "for every Go type
// the encoder accepts as input to a schema, the decoder MUST accept it as a
// target, else a value round-trips one way but not back." This is the single
// most recurring bug shape in the audit history. Rather than hand-pick cases,
// it drives the actual encode and decode paths across a schema x Go-type
// matrix.
//
// The sound form is round-trip-PER-TYPE: encode a type's own sample value,
// then decode THAT wire back into the same type. Value-consistent by
// construction, so a flag is a genuine "encoded the value, cannot read it
// back into the type that produced it" -- never a content/range artifact (a
// naive "mint one wire, try all target types" matrix instead conflates type
// rejection with value rejection: decoding a 1000ms timestamp into int8
// overflows, decoding raw 0x01 into json.Number is not a number literal, etc.
// -- all value-driven, not type-driven). Asymmetries that are DOCUMENTED
// intentional policy (BUG_AUDIT.md "Known intentional divergences") live in
// the allowlist with a citation; anything else fails the build.

type goTypeCand struct {
	name   string
	sample any        // value to attempt encoding
	newPtr func() any // fresh pointer target for decoding
}

func goTypeCands() []goTypeCand {
	return []goTypeCand{
		{"bool", true, func() any { return new(bool) }},
		{"int", int(1), func() any { return new(int) }},
		{"int8", int8(1), func() any { return new(int8) }},
		{"int16", int16(1), func() any { return new(int16) }},
		{"int32", int32(1), func() any { return new(int32) }},
		{"int64", int64(1), func() any { return new(int64) }},
		{"uint", uint(1), func() any { return new(uint) }},
		{"uint32", uint32(1), func() any { return new(uint32) }},
		{"uint64", uint64(1), func() any { return new(uint64) }},
		{"float32", float32(1), func() any { return new(float32) }},
		{"float64", float64(1), func() any { return new(float64) }},
		{"string", "1", func() any { return new(string) }},
		{"bytes", []byte{1}, func() any { return new([]byte) }},
		{"jsonNumber", json.Number("1"), func() any { return new(json.Number) }},
		{"time.Time", time.Unix(1, 0).UTC(), func() any { return new(time.Time) }},
		{"bigRat", big.NewRat(1, 1), func() any { return new(*big.Rat) }},
	}
}

type paritySchema struct {
	name string
	json string
}

func paritySchemas() []paritySchema {
	return []paritySchema{
		{"int", `"int"`},
		{"long", `"long"`},
		{"float", `"float"`},
		{"double", `"double"`},
		{"string", `"string"`},
		{"bytes", `"bytes"`},
		{"boolean", `"boolean"`},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`},
		{"decimal", `{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}`},
		{"uuid", `{"type":"string","logicalType":"uuid"}`},
	}
}

// allowedAsymmetry holds schema/type pairs where a type encodes but its own
// wire will not decode back into that same type BY DOCUMENTED, PINNED POLICY.
// Keyed "schema/type". Each entry must cite the pin(s) that document the
// intentional asymmetry; anything NOT listed here fails the invariant.
//
// Currently empty: json.Number is numeric-only (rejected for string, bytes,
// fixed, and enum on BOTH encode and decode — see
// TestRegression_JSONNumberStringSourceRejectedOnEncode), so no stringy
// encode/decode round-trip asymmetry remains.
var allowedAsymmetry = map[string]string{}

func TestInvariant_EncodeDecodeTargetParity(t *testing.T) {
	cands := goTypeCands()
	var (
		breaks    []string // encode(sample) OK but decode(that wire)->*T rejects
		checked   int
		allowHits []string
	)

	for _, sc := range paritySchemas() {
		s, err := avro.Parse(sc.json)
		if err != nil {
			t.Fatalf("%s: Parse: %v", sc.name, err)
		}
		for _, c := range cands {
			wireT, encErr := s.Encode(c.sample)
			if encErr != nil {
				continue // type not encode-accepted for this schema; not a round-trip concern
			}
			checked++
			if _, decErr := s.Decode(wireT, c.newPtr()); decErr != nil {
				key := sc.name + "/" + c.name
				msg := fmt.Sprintf("%s: Encode(%s) OK -> Decode(its own wire)->*%s rejects: %v", sc.name, c.name, c.name, decErr)
				if reason, ok := allowedAsymmetry[key]; ok {
					allowHits = append(allowHits, msg+"  [allowed: "+reason+"]")
				} else {
					breaks = append(breaks, msg)
				}
			}
		}
	}

	sort.Strings(breaks)
	sort.Strings(allowHits)
	for _, m := range allowHits {
		t.Logf("documented asymmetry: %s", m)
	}
	for _, m := range breaks {
		t.Errorf("round-trip break: %s", m)
	}
	if len(breaks) == 0 {
		t.Logf("round-trip parity holds: %d (schema, Go type) pairs encode AND decode back into the same type", checked)
	}
}
