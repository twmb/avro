package avro_test

import (
	"bytes"
	"fmt"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// CustomType matrix: every logical-type fragment × five positions × three
// custom configurations, asserting binary↔JSON parity of what the callbacks
// see, what suppression yields, and how often callbacks fire.
//
// The raw Avro-native form is CALIBRATED, never hand-computed: a suppressing
// schema (no-callback CustomType match) decodes the plain schema's wire, and
// whatever it returns IS the raw form. Configs:
//
//	suppress — CustomType{LogicalType: L} (no callbacks): both decoders must
//	           yield the same RAW value tree, and that tree must re-encode
//	           onto the plain schema's exact binary wire.
//	box      — Decode wraps raw into cbox{...}; Encode unboxes. The boxed
//	           tree must round-trip identically through binary and JSON,
//	           and the binary wire must equal the plain schema's.
//	count    — wildcard CustomType{} whose callbacks just count and skip:
//	           the number of invocations must agree between the binary and
//	           JSON paths, per direction (a side-effect parity that value
//	           asserts can't see).
// ---------------------------------------------------------------------------

type cbox struct{ Raw any }

type customFrag struct {
	label    string
	schema   string
	logical  string
	enriched any
}

func customFrags() []customFrag {
	return []customFrag{
		{"date", `{"type":"int","logicalType":"date"}`, "date",
			time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`, "time-millis",
			3*time.Hour + 7*time.Millisecond},
		{"time-micros", `{"type":"long","logicalType":"time-micros"}`, "time-micros",
			23*time.Hour + 5*time.Microsecond},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, "timestamp-millis",
			time.Date(2024, 6, 1, 12, 34, 56, 789000000, time.UTC)},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`, "timestamp-micros",
			time.Date(2024, 6, 1, 12, 34, 56, 789012000, time.UTC)},
		{"timestamp-nanos", `{"type":"long","logicalType":"timestamp-nanos"}`, "timestamp-nanos",
			time.Date(2024, 6, 1, 12, 34, 56, 789012345, time.UTC)},
		{"local-ts-millis", `{"type":"long","logicalType":"local-timestamp-millis"}`, "local-timestamp-millis",
			time.Date(2024, 6, 1, 12, 34, 56, 789000000, time.UTC)},
		{"uuid-string", `{"type":"string","logicalType":"uuid"}`, "uuid",
			"6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		{"uuid-fixed", `{"type":"fixed","name":"CUF","size":16,"logicalType":"uuid"}`, "uuid",
			"6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		{"decimal-bytes", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, "decimal",
			big.NewRat(12345, 100)},
		{"decimal-fixed", `{"type":"fixed","name":"CDF","size":8,"logicalType":"decimal","precision":10,"scale":2}`, "decimal",
			big.NewRat(-9999, 100)},
		{"duration", `{"type":"fixed","name":"CDU","size":12,"logicalType":"duration"}`, "duration",
			avro.Duration{Months: 1, Days: 2, Milliseconds: 3}},
		{"big-decimal", `{"type":"bytes","logicalType":"big-decimal"}`, "big-decimal",
			big.NewRat(314, 100)},
	}
}

// customPos wraps a fragment schema/value into a position and can pull the
// inner value back out of a decoded tree.
type customPos struct {
	label  string
	skip   func(class string) bool
	schema func(inner string) string
	wrap   func(v any) any
	unwrap func(v any) any
}

func customPositions() []customPos {
	id := func(v any) any { return v }
	return []customPos{
		{"top", nil,
			func(in string) string { return in },
			id, id},
		{"field",
			nil,
			func(in string) string {
				return fmt.Sprintf(`{"type":"record","name":"CW","fields":[{"name":"a","type":"long"},{"name":"f","type":%s}]}`, in)
			},
			func(v any) any { return map[string]any{"a": int64(4), "f": v} },
			func(v any) any { return v.(map[string]any)["f"] }},
		{"array",
			nil,
			func(in string) string { return fmt.Sprintf(`{"type":"array","items":%s}`, in) },
			func(v any) any { return []any{v, v} },
			func(v any) any { return v.([]any)[0] }},
		{"nullunion",
			nil,
			func(in string) string { return fmt.Sprintf(`["null",%s]`, in) },
			id, id},
		{"multibranch",
			nil,
			func(in string) string { return fmt.Sprintf(`["null","boolean",%s,%s]`, in, `"long"`) },
			id, id},
	}
}

// customClass mirrors tokenClass for the multibranch pad: fragments whose
// bare JSON token is a digit collide with the "long" pad branch, so those
// swap the pad to "string"; string-class fragments keep "long".
func customPad(frag customFrag) string {
	switch frag.label {
	case "uuid-string", "uuid-fixed", "decimal-bytes", "decimal-fixed", "duration", "big-decimal":
		return `"long"`
	default:
		return `"string"`
	}
}

func TestMatrix_CustomTypes(t *testing.T) {
	for _, fr := range customFrags() {
		for _, pos := range customPositions() {
			posSchema := pos.schema(fr.schema)
			if pos.label == "multibranch" {
				posSchema = fmt.Sprintf(`["null","boolean",%s,%s]`, fr.schema, customPad(fr))
			}
			// The "long" pad collides with long-backed logicals' type in
			// unions (duplicate union type); those swap to "string" via
			// customPad, but a string pad collides with uuid-string's
			// type. Skip the genuinely uncomposable pairs.
			if pos.label == "multibranch" {
				if _, err := avro.Parse(posSchema); err != nil {
					continue
				}
			}

			plain := avro.MustParse(posSchema)
			vin := pos.wrap(fr.enriched)
			plainWire, err := plain.AppendEncode(nil, vin)
			if err != nil {
				t.Fatalf("%s/%s: plain encode: %v", fr.label, pos.label, err)
			}
			plainJSON, err := plain.AppendEncodeJSON(nil, vin)
			if err != nil {
				t.Fatalf("%s/%s: plain encodeJSON: %v", fr.label, pos.label, err)
			}

			t.Run(fr.label+"/"+pos.label+"/suppress", func(t *testing.T) {
				sup, err := avro.Parse(posSchema, avro.CustomType{LogicalType: fr.logical})
				if err != nil {
					t.Fatalf("Parse: %v", err)
				}
				var aBin, aJSON any
				if _, err := sup.Decode(plainWire, &aBin); err != nil {
					t.Fatalf("suppressed binary decode: %v", err)
				}
				if err := sup.DecodeJSON(plainJSON, &aJSON); err != nil {
					t.Fatalf("suppressed JSON decode: %v", err)
				}
				if !matEqual(aBin, aJSON) {
					t.Fatalf("suppressed decode diverges:\n bin=%#v\njson=%#v", aBin, aJSON)
				}
				// Suppression means RAW: the enriched Go type must be absent.
				switch pos.unwrap(aBin).(type) {
				case time.Time, time.Duration, *big.Rat, avro.Duration:
					t.Fatalf("suppressed decode yielded enriched %T", pos.unwrap(aBin))
				}
				// The raw tree re-encodes onto the plain schema's exact wire.
				w2, err := sup.AppendEncode(nil, aBin)
				if err != nil || !bytes.Equal(w2, plainWire) {
					t.Fatalf("raw re-encode differs: err=%v\n plain=%x\n raw=%x", err, plainWire, w2)
				}
			})

			t.Run(fr.label+"/"+pos.label+"/box", func(t *testing.T) {
				ct := avro.CustomType{
					LogicalType: fr.logical,
					Decode: func(v any, _ *avro.SchemaNode) (any, error) {
						return cbox{Raw: v}, nil
					},
					Encode: func(v any, _ *avro.SchemaNode) (any, error) {
						if b, ok := v.(cbox); ok {
							return b.Raw, nil
						}
						return nil, avro.ErrSkipCustomType
					},
				}
				bs, err := avro.Parse(posSchema, ct)
				if err != nil {
					t.Fatalf("Parse: %v", err)
				}
				// Custom decode fires on the plain wire (same bytes).
				var boxed any
				if _, err := bs.Decode(plainWire, &boxed); err != nil {
					t.Fatalf("boxed decode: %v", err)
				}
				inner := pos.unwrap(boxed)
				if _, ok := inner.(cbox); !ok {
					t.Fatalf("decode did not box: %T", inner)
				}
				// The boxed tree re-encodes to the IDENTICAL binary wire.
				w2, err := bs.AppendEncode(nil, boxed)
				if err != nil || !bytes.Equal(w2, plainWire) {
					t.Fatalf("boxed re-encode differs: err=%v\n plain=%x\n boxed=%x", err, plainWire, w2)
				}
				// JSON round-trip within the custom schema agrees with the
				// binary decode (suppressed logical → raw JSON forms).
				jb, err := bs.AppendEncodeJSON(nil, boxed)
				if err != nil {
					t.Fatalf("boxed encodeJSON: %v", err)
				}
				var jBack any
				if err := bs.DecodeJSON(jb, &jBack); err != nil {
					t.Fatalf("boxed decodeJSON: %v\n j=%s", err, jb)
				}
				if !matEqual(jBack, boxed) {
					t.Fatalf("boxed JSON round-trip diverges:\n bin=%#v\njson=%#v\n j=%s", boxed, jBack, jb)
				}
				// The metadata rebuild can re-wire the custom by passing it
				// through Schema(opts...): the rebuilt schema must box and
				// re-encode identically.
				root := bs.Root()
				rebuilt, err := root.Schema(ct)
				if err != nil {
					t.Fatalf("Root().Schema(ct): %v", err)
				}
				var reboxed any
				if _, err := rebuilt.Decode(plainWire, &reboxed); err != nil {
					t.Fatalf("rebuilt boxed decode: %v", err)
				}
				if !matEqual(reboxed, boxed) {
					t.Fatalf("rebuilt custom decode diverges:\n orig=%#v\n reb=%#v", boxed, reboxed)
				}
				w3, err := rebuilt.AppendEncode(nil, reboxed)
				if err != nil || !bytes.Equal(w3, plainWire) {
					t.Fatalf("rebuilt boxed re-encode differs: err=%v\n plain=%x\n reb=%x", err, plainWire, w3)
				}
			})

			t.Run(fr.label+"/"+pos.label+"/count", func(t *testing.T) {
				var encN, decN atomic.Int64
				ct := avro.CustomType{
					LogicalType: fr.logical,
					Encode: func(v any, _ *avro.SchemaNode) (any, error) {
						encN.Add(1)
						return nil, avro.ErrSkipCustomType
					},
					Decode: func(v any, _ *avro.SchemaNode) (any, error) {
						decN.Add(1)
						return nil, avro.ErrSkipCustomType
					},
				}
				cs, err := avro.Parse(posSchema, ct)
				if err != nil {
					t.Fatalf("Parse: %v", err)
				}
				// A matching custom WITH Encode suppresses the built-in
				// logical encoder on fixed/decimal builds (documented
				// per-build suppression), so enriched inputs reject there
				// once the callback skips. Drive the RAW tree instead —
				// calibrated by a suppressing decode of the plain wire —
				// which every build accepts.
				supCal := avro.MustParse(posSchema, avro.CustomType{LogicalType: fr.logical})
				var rawTree any
				if _, err := supCal.Decode(plainWire, &rawTree); err != nil {
					t.Fatalf("raw calibration decode: %v", err)
				}
				encN.Store(0)
				if _, err := cs.AppendEncode(nil, rawTree); err != nil {
					t.Fatalf("count encode: %v", err)
				}
				encBin := encN.Load()
				encN.Store(0)
				if _, err := cs.AppendEncodeJSON(nil, rawTree); err != nil {
					t.Fatalf("count encodeJSON: %v", err)
				}
				encJSON := encN.Load()
				if encBin != encJSON {
					t.Fatalf("encode callback count diverges: binary=%d json=%d", encBin, encJSON)
				}
				var sink any
				decN.Store(0)
				if _, err := cs.Decode(plainWire, &sink); err != nil {
					t.Fatalf("count decode: %v", err)
				}
				decBin := decN.Load()
				decN.Store(0)
				if err := cs.DecodeJSON(plainJSON, &sink); err != nil {
					t.Fatalf("count decodeJSON: %v", err)
				}
				decJSON := decN.Load()
				if decBin != decJSON {
					t.Fatalf("decode callback count diverges: binary=%d json=%d", decBin, decJSON)
				}
			})
		}
	}
}
