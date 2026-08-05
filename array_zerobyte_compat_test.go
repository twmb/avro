package avro

import (
	"strings"
	"testing"
)

// TestRegression_ArrayZeroByteProducerCompliance pins producer-side compliance
// with the decoder's zero-byte-item cap (checkArrayBlockBounds /
// maxZeroByteItems). The decoder rejects an array of more than maxZeroByteItems
// zero-byte items (array<null>, array<EmptyRecord>, array<size-0-fixed>) as a
// deliberate DoS defense (BUG_AUDIT "DOS-resistance defense-in-depth"). The
// core array ENCODER had no matching check, so s.Encode produced a tiny wire
// (a count with no body) that s.Decode then rejected — a silent self-
// incompatible round-trip. This is the same class as the OCF zero-byte writer
// bound (TestWriterZeroByteDatumsSelfReadable): every reader-side cap needs a
// producer-side compliance check. The encoder now rejects at encode time with
// a clear error, and everything at or below the cap still round-trips.
func TestRegression_ArrayZeroByteProducerCompliance(t *testing.T) {
	zeroByteItemSchemas := []struct {
		label  string
		schema string
		item   any // a single zero-byte item value for this schema
	}{
		{"null", `{"type":"array","items":"null"}`, nil},
		{"empty-record", `{"type":"array","items":{"type":"record","name":"E","fields":[]}}`, map[string]any{}},
		{"size-0-fixed", `{"type":"array","items":{"type":"fixed","name":"Z","size":0}}`, []byte{}},
	}

	fill := func(item any, n int) []any {
		a := make([]any, n)
		for i := range a {
			a[i] = item
		}
		return a
	}

	for _, zb := range zeroByteItemSchemas {
		t.Run(zb.label, func(t *testing.T) {
			s := MustParse(zb.schema)

			// At the cap: must encode AND round-trip (self-readable).
			atCap := fill(zb.item, maxZeroByteItems)
			wire, err := s.AppendEncode(nil, atCap)
			if err != nil {
				t.Fatalf("encode at the cap (%d) rejected: %v", maxZeroByteItems, err)
			}
			var back []any
			if _, err := s.Decode(wire, &back); err != nil {
				t.Fatalf("SELF-INCOMPATIBILITY: encoded %d zero-byte items it cannot decode: %v", maxZeroByteItems, err)
			}
			if len(back) != maxZeroByteItems {
				t.Fatalf("round-trip length: got %d want %d", len(back), maxZeroByteItems)
			}

			// One past the cap: the encoder must REJECT (producer compliance),
			// not emit a wire the decoder rejects.
			over := fill(zb.item, maxZeroByteItems+1)
			if _, err := s.AppendEncode(nil, over); err == nil {
				t.Fatalf("encoder produced a %d zero-byte-item array the decoder rejects (self-incompatible); want an encode-time error", maxZeroByteItems+1)
			} else if !strings.Contains(err.Error(), "zero-byte") {
				t.Fatalf("over-cap encode rejected, but not with the zero-byte-cap reason: %v", err)
			}
		})
	}
}

// TestRegression_ArrayZeroByteSkipPathCompliance covers the resolution skip
// path: a writer record with an array<null> field the reader drops. Because
// the encoder now refuses to PRODUCE an over-cap zero-byte array, no such wire
// reaches the skip path from a twmb writer — the self-incompatibility is
// resolved at its source. The under-cap case still resolves+skips cleanly.
func TestRegression_ArrayZeroByteSkipPathCompliance(t *testing.T) {
	wSchema := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"drop","type":{"type":"array","items":"null"}},
		{"name":"keep","type":"int"}]}`)
	rSchema := MustParse(`{"type":"record","name":"R","fields":[{"name":"keep","type":"int"}]}`)
	res, err := Resolve(wSchema, rSchema)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	// Under cap: encode, resolve-skip the dropped array, keep survives.
	under := map[string]any{"drop": make([]any, maxZeroByteItems), "keep": int32(7)}
	wire, err := wSchema.AppendEncode(nil, under)
	if err != nil {
		t.Fatalf("under-cap encode: %v", err)
	}
	var got map[string]any
	if _, err := res.Decode(wire, &got); err != nil {
		t.Fatalf("SELF-INCOMPATIBILITY (skip path): cannot skip a dropped array<null> field it produced: %v", err)
	}
	if got["keep"] != int32(7) {
		t.Fatalf("keep field after skip: %v", got["keep"])
	}

	// Over cap: the encoder refuses to produce the unreadable wire.
	over := map[string]any{"drop": make([]any, maxZeroByteItems+1), "keep": int32(7)}
	if _, err := wSchema.AppendEncode(nil, over); err == nil {
		t.Fatal("encoder produced a record whose dropped array<null> field exceeds the decoder cap; want an encode-time error")
	}
}
