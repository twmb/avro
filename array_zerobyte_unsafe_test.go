package avro

import (
	"strings"
	"testing"
)

// TestRegression_ArrayZeroByteUnsafePathCompliance covers the UNSAFE array
// encoders (usArrayRecord / usArrayPtrRecord / usArrayDirect), reached when an
// array of zero-byte items is an addressable struct field. The first fix added
// the producer-side maxZeroByteItems check only to the reflect serArray.ser;
// its unsafe twins write the count + body with no guard, so a struct field
// []EmptyRecord / []*EmptyRecord / [][0]byte of more than maxZeroByteItems
// elements still encoded to a tiny wire the decoder rejects. (Sibling sweep:
// the reflect and unsafe array encoders must share one compliance helper so
// they cannot drift — that is what this pins.)
func TestRegression_ArrayZeroByteUnsafePathCompliance(t *testing.T) {
	type emptyRec struct{}

	cases := []struct {
		label  string
		schema string
		atCap  any
		over   any
	}{
		{
			"slice-of-empty-record",
			`{"type":"record","name":"H","fields":[{"name":"a","type":{"type":"array","items":{"type":"record","name":"E","fields":[]}}}]}`,
			&struct {
				A []emptyRec `avro:"a"`
			}{A: make([]emptyRec, maxZeroByteItems)},
			&struct {
				A []emptyRec `avro:"a"`
			}{A: make([]emptyRec, maxZeroByteItems+1)},
		},
		{
			"slice-of-ptr-empty-record",
			`{"type":"record","name":"H","fields":[{"name":"a","type":{"type":"array","items":{"type":"record","name":"E","fields":[]}}}]}`,
			func() any {
				a := make([]*emptyRec, maxZeroByteItems)
				for i := range a {
					a[i] = &emptyRec{}
				}
				return &struct {
					A []*emptyRec `avro:"a"`
				}{A: a}
			}(),
			func() any {
				a := make([]*emptyRec, maxZeroByteItems+1)
				for i := range a {
					a[i] = &emptyRec{}
				}
				return &struct {
					A []*emptyRec `avro:"a"`
				}{A: a}
			}(),
		},
		{
			"slice-of-size0-fixed",
			`{"type":"record","name":"H","fields":[{"name":"a","type":{"type":"array","items":{"type":"fixed","name":"Z","size":0}}}]}`,
			&struct {
				A [][0]byte `avro:"a"`
			}{A: make([][0]byte, maxZeroByteItems)},
			&struct {
				A [][0]byte `avro:"a"`
			}{A: make([][0]byte, maxZeroByteItems+1)},
		},
	}

	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			s := MustParse(c.schema)

			// At the cap: encodes and round-trips (self-readable).
			wire, err := s.AppendEncode(nil, c.atCap)
			if err != nil {
				t.Fatalf("encode at cap: %v", err)
			}
			var sink any
			if _, err := s.Decode(wire, &sink); err != nil {
				t.Fatalf("SELF-INCOMPATIBILITY (unsafe path): encoded at the cap but cannot decode: %v", err)
			}

			// Over the cap: the unsafe encoder must REJECT, not emit a wire
			// the decoder refuses.
			if _, err := s.AppendEncode(nil, c.over); err == nil {
				t.Fatal("unsafe array encoder produced an over-cap zero-byte array the decoder rejects; want an encode-time error")
			} else if !strings.Contains(err.Error(), "zero-byte") {
				t.Fatalf("over-cap unsafe encode rejected, but not with the zero-byte reason: %v", err)
			}
		})
	}
}
