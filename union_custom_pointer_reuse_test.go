package avro_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// A CustomType whose Decode returns a POINTER, registered on a logical type at a
// UNION branch, must decode identically on the binary and JSON wires even when
// the decode target is reused and already holds a non-nil pointer (the
// streaming-decode-into-a-reused-struct pattern). The binary union deser passes
// the un-indirected target to the custom wrapper (setCustomResult assigns the
// pointer into an interface / walks a pointer target); the JSON union decoder
// pre-dereferenced a reused *T held in an interface before dispatching to the
// branch, so the fresh pointer result was rejected ("cannot use T with Avro type
// long") from the 2nd datum onward — a binary↔JSON divergence on a value the
// binary path decodes fine. These pin per-branch indirection parity.

type ucpEvent struct{ T time.Time }

func ucpCustom() avro.CustomType {
	return avro.CustomType{
		LogicalType: "timestamp-millis",
		AvroType:    "long",
		GoType:      reflect.TypeFor[*ucpEvent](),
		Decode: func(v any, _ *avro.SchemaNode) (any, error) {
			return &ucpEvent{T: time.UnixMilli(v.(int64)).UTC()}, nil
		},
		Encode: func(v any, _ *avro.SchemaNode) (any, error) {
			return v.(*ucpEvent).T.UnixMilli(), nil
		},
	}
}

func TestRegression_UnionCustomDecodePointerReusedTargetParity(t *testing.T) {
	const schema = `{"type":"record","name":"R","fields":[{"name":"when","type":["null",{"type":"long","logicalType":"timestamp-millis"}]}]}`
	s, err := avro.Parse(schema, avro.WithCustomType(ucpCustom()))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	type Event struct {
		When any `avro:"when"`
	}
	want := time.UnixMilli(1700000000000).UTC()
	mk := func() Event { return Event{When: &ucpEvent{T: want}} }

	// Pre-encode three identical datums on each wire.
	var bin [][]byte
	var jsn [][]byte
	for i := 0; i < 3; i++ {
		b, err := s.AppendEncode(nil, mk())
		if err != nil {
			t.Fatalf("binary encode %d: %v", i, err)
		}
		bin = append(bin, b)
		j, err := s.AppendEncodeJSON(nil, mk())
		if err != nil {
			t.Fatalf("json encode %d: %v", i, err)
		}
		jsn = append(jsn, j)
	}

	check := func(name string, got Event, i int) {
		ev, ok := got.When.(*ucpEvent)
		if !ok {
			t.Fatalf("%s datum %d: got %T, want *ucpEvent", name, i, got.When)
		}
		if !ev.T.Equal(want) {
			t.Fatalf("%s datum %d: got %v want %v", name, i, ev.T, want)
		}
	}

	// Reused target (the streaming pattern): the same struct value decoded into
	// repeatedly, its any field carrying the prior *ucpEvent.
	var evB Event
	for i, b := range bin {
		if _, err := s.Decode(b, &evB); err != nil {
			t.Fatalf("binary decode (reused) %d: %v", i, err)
		}
		check("binary-reused", evB, i)
	}
	var evJ Event
	for i, j := range jsn {
		if err := s.DecodeJSON(j, &evJ); err != nil {
			t.Fatalf("json decode (reused) %d: %v", i, err)
		}
		check("json-reused", evJ, i)
	}

	// Fresh nil-interface target (first decode) must also produce the *ucpEvent
	// on both wires — the boundary the reuse case builds on.
	var freshB, freshJ Event
	if _, err := s.Decode(bin[0], &freshB); err != nil {
		t.Fatalf("binary decode (fresh): %v", err)
	}
	check("binary-fresh", freshB, 0)
	if err := s.DecodeJSON(jsn[0], &freshJ); err != nil {
		t.Fatalf("json decode (fresh): %v", err)
	}
	check("json-fresh", freshJ, 0)

	// TaggedUnions decode: the {branchName: value} envelope must wrap the custom
	// result identically — and still survive target reuse — on both wires. The
	// envelope value is the (pointer) custom result; assert it key-agnostically.
	taggedVal := func(name string, v any) *ucpEvent {
		m, ok := v.(map[string]any)
		if !ok || len(m) != 1 {
			t.Fatalf("%s: got %T (%v), want single-entry map envelope", name, v, v)
		}
		for _, e := range m {
			ev, ok := e.(*ucpEvent)
			if !ok {
				t.Fatalf("%s: envelope value %T, want *ucpEvent", name, e)
			}
			return ev
		}
		return nil
	}
	var tagB, tagJ Event
	for i := range bin {
		if _, err := s.Decode(bin[i], &tagB, avro.TaggedUnions()); err != nil {
			t.Fatalf("binary tagged decode %d: %v", i, err)
		}
		if err := s.DecodeJSON(jsn[i], &tagJ, avro.TaggedUnions()); err != nil {
			t.Fatalf("json tagged decode %d: %v", i, err)
		}
		eb := taggedVal("binary-tagged", tagB.When)
		ej := taggedVal("json-tagged", tagJ.When)
		if !eb.T.Equal(want) || !ej.T.Equal(want) {
			t.Fatalf("tagged %d: binary=%v json=%v want %v", i, eb.T, ej.T, want)
		}
	}
}

// A CustomType.Decode returning a pointer must decode into a concrete *T field
// target through a union branch on EVERY union shape and BOTH wires. The binary
// general union deser (deserUnion.deser) passes the un-indirected target to the
// branch fn, so setCustomResult lands the *T result; but the 2-branch null-union
// fast path (deserNullUnionAt) pre-dereferenced a concrete pointer before the
// branch fn, so a *T target FAILED in a ["null", customLong] union while
// SUCCEEDING in a 3+-branch union — an arbitrary 2-branch-vs-general
// inconsistency that rejected a target the general path decodes. The JSON
// unionTarget likewise derefed concrete pointers. These pin that a *T custom
// target decodes uniformly across union arity and wire format.
func TestRegression_UnionCustomDecodePointerFieldTarget(t *testing.T) {
	type EventPtr struct {
		When *ucpEvent `avro:"when"`
	}
	want := time.UnixMilli(1700000000000).UTC()

	cases := []struct {
		name   string
		schema string
	}{
		{"2-branch-null-union", `{"type":"record","name":"R","fields":[{"name":"when","type":["null",{"type":"long","logicalType":"timestamp-millis"}]}]}`},
		// A 3+-branch union routes through the general deserUnion.deser path.
		{"3-branch-union", `{"type":"record","name":"R","fields":[{"name":"when","type":["null",{"type":"long","logicalType":"timestamp-millis"},"string"]}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := avro.Parse(tc.schema, avro.WithCustomType(ucpCustom()))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			in := EventPtr{When: &ucpEvent{T: want}}
			b, err := s.AppendEncode(nil, in)
			if err != nil {
				t.Fatalf("binary encode: %v", err)
			}
			var gotB EventPtr
			if _, err := s.Decode(b, &gotB); err != nil {
				t.Fatalf("binary decode into *T field: %v", err)
			}
			if gotB.When == nil || !gotB.When.T.Equal(want) {
				t.Fatalf("binary *T field: got %v", gotB.When)
			}
			j, err := s.AppendEncodeJSON(nil, in)
			if err != nil {
				t.Fatalf("json encode: %v", err)
			}
			var gotJ EventPtr
			if err := s.DecodeJSON(j, &gotJ); err != nil {
				t.Fatalf("json decode into *T field: %v", err)
			}
			if gotJ.When == nil || !gotJ.When.T.Equal(want) {
				t.Fatalf("json *T field: got %v", gotJ.When)
			}
		})
	}
}

// Boundary-1 control: NON-custom union decode into a reused interface that holds
// a manually pre-populated *T must keep doing in-place reuse (the result's
// dynamic type stays *T) identically on binary and JSON — the per-branch fix
// must NOT regress this to a boxed value.
func TestRegression_UnionNonCustomReuseInPlaceUnchanged(t *testing.T) {
	s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"v","type":["null","long"]}]}`)
	type Rec struct {
		V any `avro:"v"`
	}
	bw, _ := s.AppendEncode(nil, Rec{V: int64(42)})
	jw, _ := s.AppendEncodeJSON(nil, Rec{V: int64(42)})

	// Manual *int64 pre-population → in-place reuse → result holds *int64 on both.
	for _, tc := range []struct {
		name   string
		decode func(target *Rec) error
		wire   []byte
	}{
		{"binary", func(r *Rec) error { _, e := s.Decode(bw, r); return e }, bw},
		{"json", func(r *Rec) error { return s.DecodeJSON(jw, r) }, jw},
	} {
		p := int64(7)
		r := Rec{V: &p}
		if err := tc.decode(&r); err != nil {
			t.Fatalf("%s decode: %v", tc.name, err)
		}
		pp, ok := r.V.(*int64)
		if !ok {
			t.Fatalf("%s: in-place reuse lost: got %T, want *int64", tc.name, r.V)
		}
		if *pp != 42 {
			t.Fatalf("%s: got %d want 42", tc.name, *pp)
		}
	}

	// Fresh nil interface → boxed value (int64) on both.
	for _, tc := range []struct {
		name   string
		decode func(target *Rec) error
	}{
		{"binary", func(r *Rec) error { _, e := s.Decode(bw, r); return e }},
		{"json", func(r *Rec) error { return s.DecodeJSON(jw, r) }},
	} {
		var r Rec
		if err := tc.decode(&r); err != nil {
			t.Fatalf("%s fresh decode: %v", tc.name, err)
		}
		if got, ok := r.V.(int64); !ok || got != 42 {
			t.Fatalf("%s fresh: got %T %v, want int64 42", tc.name, r.V, r.V)
		}
	}
}
