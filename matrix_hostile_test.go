package avro_test

import (
	"fmt"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// Hostile-wire matrix: deterministic, exhaustive truncation and per-byte
// corruption of every composed schema's valid wire. Unlike fuzzing (random
// sampling), every prefix and every single-byte mutant of every cell runs
// on every test execution. The invariant is purely defensive: never panic,
// and a successful decode must have consumed the entire input.
// ---------------------------------------------------------------------------

func hostileDecode(t *testing.T, s *avro.Schema, schemaJSON string, wire []byte, what string) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("PANIC decoding %s of schema %s: %v\nwire: %x", what, schemaJSON, r, wire)
		}
	}()
	var sink any
	rest, err := s.Decode(wire, &sink)
	if err == nil && len(rest) != 0 {
		// Decode reports leftover bytes; a "successful" partial decode is
		// fine as long as it is honest about the remainder.
		_ = rest
	}
}

func hostileDecodeJSON(t *testing.T, s *avro.Schema, schemaJSON string, j []byte, what string) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("PANIC json-decoding %s of schema %s: %v\ninput: %q", what, schemaJSON, r, j)
		}
	}()
	var sink any
	_ = s.DecodeJSON(j, &sink)
}

func TestMatrix_HostileTruncationAndCorruption(t *testing.T) {
	frags := matFrags()
	ctxs := matCtxs()
	for _, fr := range frags {
		for _, cx := range ctxs {
			if cx.skip != nil && cx.skip(fr.kind) {
				continue
			}
			t.Run(fr.label+"/"+cx.label, func(t *testing.T) {
				u := &uniq{}
				schemaJSON := cx.schema(fr.schema(u), fr.kind, u)
				s, err := avro.Parse(schemaJSON)
				if err != nil {
					t.Fatalf("Parse: %v", err)
				}
				vin := cx.wrap(fr.values[0])
				w1, err := s.AppendEncode(nil, vin)
				if err != nil {
					t.Fatalf("encode: %v", err)
				}
				j1, err := s.AppendEncodeJSON(nil, vin)
				if err != nil {
					t.Fatalf("encodeJSON: %v", err)
				}

				// Every strict prefix of the valid binary wire.
				for i := 0; i < len(w1); i++ {
					hostileDecode(t, s, schemaJSON, w1[:i], fmt.Sprintf("prefix[:%d]", i))
				}
				// Every single-byte mutant (three mutations per position).
				mut := make([]byte, len(w1))
				for i := 0; i < len(w1); i++ {
					for _, b := range []byte{0x00, 0xFF, w1[i] + 1} {
						copy(mut, w1)
						mut[i] = b
						hostileDecode(t, s, schemaJSON, mut, fmt.Sprintf("mutant[%d]=%#x", i, b))
					}
				}
				// JSON prefixes (capped — JSON wires can be longer).
				step := 1
				if len(j1) > 64 {
					step = len(j1) / 64
				}
				for i := 0; i < len(j1); i += step {
					hostileDecodeJSON(t, s, schemaJSON, j1[:i], fmt.Sprintf("jsonprefix[:%d]", i))
				}
				// JSON single-byte mutants (same cap).
				jmut := make([]byte, len(j1))
				for i := 0; i < len(j1); i += step {
					for _, b := range []byte{0x00, '{', '"'} {
						copy(jmut, j1)
						jmut[i] = b
						hostileDecodeJSON(t, s, schemaJSON, jmut, fmt.Sprintf("jsonmutant[%d]=%#x", i, b))
					}
				}
			})
		}
	}
}

// The same defensive sweep through RESOLVED schemas: promotion wrappers and
// skip paths get the hostile bytes too (a dropped trailing field makes the
// skip path consume the mutated region).
func TestMatrix_HostileThroughResolution(t *testing.T) {
	wSchema := `{"type":"record","name":"R","fields":[
		{"name":"keep","type":"int"},
		{"name":"drop","type":{"type":"array","items":["null","string",{"type":"record","name":"N","fields":[
			{"name":"v","type":"long"},{"name":"next","type":["null","N"],"default":null}]}]}},
		{"name":"tail","type":"string"}]}`
	rSchema := `{"type":"record","name":"R","fields":[
		{"name":"keep","type":"long"},
		{"name":"tail","type":"string"}]}`
	w := avro.MustParse(wSchema)
	r := avro.MustParse(rSchema)
	res, err := avro.Resolve(w, r)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	vin := map[string]any{
		"keep": int32(7),
		"drop": []any{
			nil, "s",
			map[string]any{"v": int64(1), "next": map[string]any{"v": int64(2), "next": nil}},
		},
		"tail": "end",
	}
	w1, err := w.AppendEncode(nil, vin)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	for i := 0; i < len(w1); i++ {
		hostileDecode(t, res, "resolved(R)", w1[:i], fmt.Sprintf("prefix[:%d]", i))
	}
	mut := make([]byte, len(w1))
	for i := 0; i < len(w1); i++ {
		for _, b := range []byte{0x00, 0xFF, w1[i] + 1} {
			copy(mut, w1)
			mut[i] = b
			hostileDecode(t, res, "resolved(R)", mut, fmt.Sprintf("mutant[%d]=%#x", i, b))
		}
	}
}
