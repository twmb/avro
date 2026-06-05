package avro_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// Rejection-parity matrix: for values that do NOT fit a schema, the binary
// and JSON encoders must AGREE on rejection, and for any wire the two
// decoders must agree on target rejection. Historically the largest bug
// class (encode accepts X / decode rejects X, or one wire format accepts
// what the other rejects); this asserts the parity generatively instead of
// pinning single instances.
// ---------------------------------------------------------------------------

func TestMatrix_EncodeRejectionParity(t *testing.T) {
	cases := []struct {
		label  string
		schema string
		bad    []any
	}{
		{"int", `"int"`, []any{"s", true, []byte{1}, 1.5, float64(math.MaxInt32) * 4, map[string]any{}, math.NaN()}},
		{"long", `"long"`, []any{"s", true, 2.5, math.Inf(1), []any{}}},
		{"float", `"float"`, []any{"s", true, []byte{1}, map[string]any{}}},
		{"double", `"double"`, []any{"s", true, []byte{1}, []any{}}},
		{"boolean", `"boolean"`, []any{int32(1), "true", 0.0, []byte{1}}},
		{"string", `"string"`, []any{true, int32(1), 1.5, []any{}, map[string]any{}}},
		{"bytes", `"bytes"`, []any{true, int32(1), 1.5, []any{1}, map[string]any{}}},
		{"null", `"null"`, []any{int32(0), "", false, []byte{}}},
		{"enum", `{"type":"enum","name":"RJE","symbols":["A","B"]}`,
			[]any{"Z", "", int32(2), int32(-1), true, 1.5}},
		{"fixed2", `{"type":"fixed","name":"RJF","size":2}`,
			[]any{[]byte{1}, []byte{1, 2, 3}, "x", "xyz", true, int32(1)}},
		{"fixed0", `{"type":"fixed","name":"RJF0","size":0}`,
			[]any{[]byte{1}, "x", int32(0)}},
		{"date", `{"type":"int","logicalType":"date"}`, []any{"2024-13-45", true, []byte{1}}},
		{"timestamp", `{"type":"long","logicalType":"timestamp-millis"}`, []any{true, []byte{1}, map[string]any{}}},
		{"uuid-fixed", `{"type":"fixed","name":"RJU","size":16,"logicalType":"uuid"}`,
			[]any{"not-a-uuid", "6ba7b810", true, int32(1)}},
		// NOTE: a NON-numeric string against bytes+decimal is ACCEPTED as
		// raw bytes (the documented bytes/fixed encode-side string-source
		// leniency; numeric strings coerce to decimal instead), so it is
		// not in the bad set.
		{"decimal", `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
			[]any{true, []any{}, map[string]any{}}},
		{"array", `{"type":"array","items":"int"}`, []any{int32(1), "s", map[string]any{"k": int32(1)}, []any{"s"}}},
		{"map", `{"type":"map","values":"int"}`, []any{int32(1), "s", []any{int32(1)}, map[string]any{"k": "s"}}},
		{"record", `{"type":"record","name":"RJR","fields":[{"name":"a","type":"int"}]}`,
			[]any{int32(1), "s", []any{}, map[string]any{"a": "s"}}},
		{"nullunion", `["null","int"]`, []any{"s", true, 1.5, []byte{1}}},
		{"multibranch", `["null","boolean","int"]`, []any{"s", []byte{1}, []any{}, 2.5}},
	}
	positions := []struct {
		label  string
		schema func(in string) string
		wrap   func(v any) any
	}{
		{"top", func(in string) string { return in }, func(v any) any { return v }},
		{"field", func(in string) string {
			return fmt.Sprintf(`{"type":"record","name":"RJW","fields":[{"name":"f","type":%s}]}`, in)
		}, func(v any) any { return map[string]any{"f": v} }},
		{"array-item", func(in string) string { return fmt.Sprintf(`{"type":"array","items":%s}`, in) },
			func(v any) any { return []any{v} }},
	}
	for _, c := range cases {
		for _, pos := range positions {
			t.Run(c.label+"/"+pos.label, func(t *testing.T) {
				s := avro.MustParse(pos.schema(c.schema))
				for i, bad := range c.bad {
					vin := pos.wrap(bad)
					_, binErr := s.AppendEncode(nil, vin)
					_, jsonErr := s.AppendEncodeJSON(nil, vin)
					if (binErr == nil) != (jsonErr == nil) {
						t.Errorf("bad[%d] %#v: encode rejection diverges: binary=%v json=%v",
							i, bad, binErr, jsonErr)
					}
					if binErr == nil {
						t.Errorf("bad[%d] %#v: unexpectedly accepted by both encoders", i, bad)
					}
				}
			})
		}
	}
}

// Decode-target rejection parity: for one valid wire, decoding into a
// mismatched Go target must reject on the binary and JSON paths alike.
func TestMatrix_DecodeTargetRejectionParity(t *testing.T) {
	mkTargets := func() map[string]any {
		return map[string]any{
			"int32":   new(int32),
			"int64":   new(int64),
			"float64": new(float64),
			"bool":    new(bool),
			"string":  new(string),
			"bytes":   new([]byte),
			"arr2":    new([2]byte),
			"slice":   new([]int32),
			"map":     new(map[string]int32),
		}
	}
	cases := []struct {
		label   string
		schema  string
		value   any
		accepts map[string]bool // target keys that must accept; all others must reject
	}{
		{"int", `"int"`, int32(7),
			map[string]bool{"int32": true, "int64": true, "float64": true}},
		{"boolean", `"boolean"`, true,
			map[string]bool{"bool": true}},
		{"string", `"string"`, "sv",
			map[string]bool{"string": true, "bytes": true}},
		// [2]byte is a legal exact-length target for 2-byte bytes values
		// (setBytesValue's array arm).
		{"bytes", `"bytes"`, []byte{1, 2},
			map[string]bool{"bytes": true, "string": true, "arr2": true}},
		{"fixed2", `{"type":"fixed","name":"DTF","size":2}`, []byte{1, 2},
			map[string]bool{"bytes": true, "string": true, "arr2": true}},
		// []byte is []uint8 — a legitimate typed slice target for
		// array<int> whose values fit uint8.
		{"array-int", `{"type":"array","items":"int"}`, []any{int32(1)},
			map[string]bool{"slice": true, "bytes": true}},
		{"map-int", `{"type":"map","values":"int"}`, map[string]any{"k": int32(1)},
			map[string]bool{"map": true}},
	}
	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			s := avro.MustParse(c.schema)
			wire, err := s.AppendEncode(nil, c.value)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			j, err := s.AppendEncodeJSON(nil, c.value)
			if err != nil {
				t.Fatalf("encodeJSON: %v", err)
			}
			for name, target := range mkTargets() {
				_, binErr := s.Decode(wire, target)
				jsonTargets := mkTargets() // fresh, undamaged by the binary pass
				jsonErr := s.DecodeJSON(j, jsonTargets[name])
				if (binErr == nil) != (jsonErr == nil) {
					t.Errorf("target %s: decode rejection diverges: binary=%v json=%v", name, binErr, jsonErr)
					continue
				}
				if want := c.accepts[name]; (binErr == nil) != want {
					t.Errorf("target %s: accept=%v want=%v (binErr=%v)", name, binErr == nil, want, binErr)
				}
			}
		})
	}
}
