package avro_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// Target-reuse matrix: decoding into already-populated targets. The matrix
// core always decodes into fresh targets; the reuse semantics — map targets
// retain non-schema keys while schema fields overwrite (encoding/json
// parity), slices are rewritten to exactly the new length, pointers are
// reused — have their own documented contracts and historically their own
// bugs. Every fragment kind drives a decode-twice cycle on the same target,
// on both wire formats.
// ---------------------------------------------------------------------------

func TestMatrix_TargetReusePerKind(t *testing.T) {
	for _, fr := range matFrags() {
		if fr.kind == "null" {
			continue // a null field decodes to nil regardless of reuse
		}
		if len(fr.values) < 2 {
			continue // reuse needs two distinct values
		}
		t.Run(fr.label, func(t *testing.T) {
			u := &uniq{}
			schema := fmt.Sprintf(`{"type":"record","name":"RU","fields":[{"name":"f","type":%s}]}`, fr.schema(u))
			s := avro.MustParse(schema)
			w1, err := s.AppendEncode(nil, map[string]any{"f": fr.values[0]})
			if err != nil {
				t.Fatalf("encode v0: %v", err)
			}
			w2, err := s.AppendEncode(nil, map[string]any{"f": fr.values[1]})
			if err != nil {
				t.Fatalf("encode v1: %v", err)
			}
			var want1, want2 any
			if _, err := s.Decode(w1, &want1); err != nil {
				t.Fatalf("fresh decode v0: %v", err)
			}
			if _, err := s.Decode(w2, &want2); err != nil {
				t.Fatalf("fresh decode v1: %v", err)
			}

			// Sequential binary decodes into the SAME *any: second value
			// fully replaces the schema field; a pre-seeded non-schema key
			// is retained (the documented stale-key contract).
			var reused any = map[string]any{"stale": "keepme"}
			if _, err := s.Decode(w1, &reused); err != nil {
				t.Fatalf("reuse decode #1: %v", err)
			}
			if _, err := s.Decode(w2, &reused); err != nil {
				t.Fatalf("reuse decode #2: %v", err)
			}
			m := reused.(map[string]any)
			if m["stale"] != "keepme" {
				t.Fatalf("non-schema key dropped on reuse: %#v", m)
			}
			if !matEqual(m["f"], want2.(map[string]any)["f"]) {
				t.Fatalf("reused decode field stale:\n got=%#v\nwant=%#v", m["f"], want2.(map[string]any)["f"])
			}
			// The reused tree re-encodes onto w2's wire after dropping the
			// foreign key (schema-driven encode ignores extra keys).
			re, err := s.AppendEncode(nil, m)
			if err != nil || !bytes.Equal(re, w2) {
				t.Fatalf("reused tree re-encode: err=%v\n re=%x\n w2=%x", err, re, w2)
			}

			// JSON decode into the same pre-populated map behaves alike.
			j2, err := s.AppendEncodeJSON(nil, want2)
			if err != nil {
				t.Fatalf("encodeJSON: %v", err)
			}
			var jreused any = map[string]any{"stale": "keepme", "f": "overwrite-me"}
			if err := s.DecodeJSON(j2, &jreused); err != nil {
				t.Fatalf("JSON reuse decode: %v", err)
			}
			jm := jreused.(map[string]any)
			if jm["stale"] != "keepme" {
				t.Fatalf("JSON reuse dropped non-schema key: %#v", jm)
			}
			if !matEqual(jm["f"], want2.(map[string]any)["f"]) {
				t.Fatalf("JSON reuse field stale: %#v", jm["f"])
			}
		})
	}
}

// Typed-container reuse: slices shrink and grow to exactly the decoded
// length; map targets accumulate per the documented retain semantics;
// pointer chains are reused rather than reallocated where pinned.
func TestMatrix_TypedContainerReuse(t *testing.T) {
	t.Run("slice-shrinks-and-grows", func(t *testing.T) {
		s := avro.MustParse(`{"type":"array","items":"int"}`)
		big, _ := s.AppendEncode(nil, []int32{1, 2, 3, 4, 5})
		small, _ := s.AppendEncode(nil, []int32{9})
		var target []int32
		if _, err := s.Decode(big, &target); err != nil {
			t.Fatalf("decode big: %v", err)
		}
		if _, err := s.Decode(small, &target); err != nil {
			t.Fatalf("decode small into used slice: %v", err)
		}
		if len(target) != 1 || target[0] != 9 {
			t.Fatalf("slice reuse left stale elements: %v", target)
		}
		if _, err := s.Decode(big, &target); err != nil {
			t.Fatalf("decode big into shrunk slice: %v", err)
		}
		if len(target) != 5 || target[4] != 5 {
			t.Fatalf("slice regrow failed: %v", target)
		}
	})
	t.Run("slice-of-pointers", func(t *testing.T) {
		s := avro.MustParse(`{"type":"array","items":["null","int"]}`)
		w1, _ := s.AppendEncode(nil, []any{int32(1), nil, int32(3)})
		w2, _ := s.AppendEncode(nil, []any{nil, int32(7), nil})
		var target []*int32
		if _, err := s.Decode(w1, &target); err != nil {
			t.Fatalf("decode #1: %v", err)
		}
		if _, err := s.Decode(w2, &target); err != nil {
			t.Fatalf("decode #2 into used []*int32: %v", err)
		}
		if target[0] != nil || target[1] == nil || *target[1] != 7 || target[2] != nil {
			t.Fatalf("pointer-slice reuse wrong: %v", target)
		}
	})
	t.Run("typed-map-accumulates", func(t *testing.T) {
		s := avro.MustParse(`{"type":"map","values":"int"}`)
		w1, _ := s.AppendEncode(nil, map[string]int32{"a": 1})
		w2, _ := s.AppendEncode(nil, map[string]int32{"b": 2})
		var target map[string]int32
		if _, err := s.Decode(w1, &target); err != nil {
			t.Fatalf("decode #1: %v", err)
		}
		if _, err := s.Decode(w2, &target); err != nil {
			t.Fatalf("decode #2 into used map: %v", err)
		}
		// Documented retain semantics: existing keys persist, new keys add.
		if target["a"] != 1 || target["b"] != 2 {
			t.Fatalf("typed map reuse: %v", target)
		}
	})
	t.Run("struct-field-reuse", func(t *testing.T) {
		type R struct {
			N *int32 `avro:"n"`
			S string `avro:"s"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"n","type":["null","int"]},{"name":"s","type":"string"}]}`)
		w1, _ := s.AppendEncode(nil, map[string]any{"n": int32(5), "s": "one"})
		w2, _ := s.AppendEncode(nil, map[string]any{"n": nil, "s": "two"})
		var r R
		if _, err := s.Decode(w1, &r); err != nil {
			t.Fatalf("decode #1: %v", err)
		}
		if r.N == nil || *r.N != 5 {
			t.Fatalf("first decode: %+v", r)
		}
		if _, err := s.Decode(w2, &r); err != nil {
			t.Fatalf("decode #2: %v", err)
		}
		if r.N != nil || r.S != "two" {
			t.Fatalf("struct reuse stale: %+v ptr=%v", r, r.N)
		}
		if _, err := s.Decode(w1, &r); err != nil {
			t.Fatalf("decode #3: %v", err)
		}
		if r.N == nil || *r.N != 5 || r.S != "one" {
			t.Fatalf("struct re-reuse: %+v", r)
		}
	})
}

type reuseArrayElemP struct {
	A int64 `avro:"a"`
}

type reuseArrayElemFast struct {
	F []*reuseArrayElemP `avro:"f"`
}

// The embedded-pointer twin routes every field through the reflect slow path
// (computeFieldOffset declines fields reached through an embedded pointer),
// so a value of this type decodes via the reflect array path while a plain
// reuseArrayElemFast decodes via the unsafe struct fast path.
type reuseArrayElemReflect struct {
	*reuseArrayElemFast
}

// TestRegression_ArrayPointerElementReuseAcrossDecodePaths pins that decoding a
// []*P struct field into a reused target reuses the retained non-nil element
// pointers identically on the unsafe struct fast path and the reflect path —
// the documented pointer-reuse contract (matrix header: "pointers are
// reused"). The unsafe path batch-allocated backing only for nil slots and
// wrote through retained pointers; the reflect path unconditionally installed
// fresh backing, so an aliased element from a prior decode was updated in
// place on one arm and orphaned on the other.
func TestRegression_ArrayPointerElementReuseAcrossDecodePaths(t *testing.T) {
	const schema = `{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"array","items":{"type":"record","name":"P","fields":[{"name":"a","type":"long"}]}}}]}`
	s := avro.MustParse(schema)
	w1, err := s.Encode(map[string]any{"f": []any{map[string]any{"a": int64(1)}}})
	if err != nil {
		t.Fatalf("encode w1: %v", err)
	}
	w2, err := s.Encode(map[string]any{"f": []any{map[string]any{"a": int64(2)}}})
	if err != nil {
		t.Fatalf("encode w2: %v", err)
	}

	// Unsafe struct fast path: a directly addressable struct target.
	var fast reuseArrayElemFast
	if _, err := s.Decode(w1, &fast); err != nil {
		t.Fatalf("fast decode w1: %v", err)
	}
	fastKeep := fast.F[0]
	if _, err := s.Decode(w2, &fast); err != nil {
		t.Fatalf("fast decode w2: %v", err)
	}

	// Reflect path: same logical target through the embedded-pointer twin.
	refl := reuseArrayElemReflect{reuseArrayElemFast: &reuseArrayElemFast{}}
	if _, err := s.Decode(w1, &refl); err != nil {
		t.Fatalf("reflect decode w1: %v", err)
	}
	reflKeep := refl.F[0]
	if _, err := s.Decode(w2, &refl); err != nil {
		t.Fatalf("reflect decode w2: %v", err)
	}

	if fastKeep.A != reflKeep.A {
		t.Fatalf("arm divergence: retained pointer reads %d (unsafe fast path) vs %d (reflect path)", fastKeep.A, reflKeep.A)
	}
	// Both paths reuse the slot, so the retained alias observes the second
	// decode's value.
	if fastKeep.A != 2 {
		t.Fatalf("retained pointer should observe the reused decode (want 2); got %d", fastKeep.A)
	}
}

type reuseNullUnionFast struct {
	F []*int64 `avro:"f"`
}

type reuseNullUnionReflect struct {
	*reuseNullUnionFast
}

// Sibling of the record-element case: null-union array elements ([]*int64 over
// array<["null","long"]>) flow through the same pointer-element branch, so the
// unsafe path (udNullUnionEnter) and reflect path must reuse retained element
// pointers identically.
func TestRegression_ArrayNullUnionPointerElementReuseAcrossDecodePaths(t *testing.T) {
	const schema = `{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"array","items":["null","long"]}}]}`
	s := avro.MustParse(schema)
	one, two := int64(1), int64(2)
	w1, err := s.Encode(map[string]any{"f": []any{&one}})
	if err != nil {
		t.Fatalf("encode w1: %v", err)
	}
	w2, err := s.Encode(map[string]any{"f": []any{&two}})
	if err != nil {
		t.Fatalf("encode w2: %v", err)
	}

	var fast reuseNullUnionFast
	if _, err := s.Decode(w1, &fast); err != nil {
		t.Fatalf("fast decode w1: %v", err)
	}
	fastKeep := fast.F[0]
	if _, err := s.Decode(w2, &fast); err != nil {
		t.Fatalf("fast decode w2: %v", err)
	}

	refl := reuseNullUnionReflect{reuseNullUnionFast: &reuseNullUnionFast{}}
	if _, err := s.Decode(w1, &refl); err != nil {
		t.Fatalf("reflect decode w1: %v", err)
	}
	reflKeep := refl.F[0]
	if _, err := s.Decode(w2, &refl); err != nil {
		t.Fatalf("reflect decode w2: %v", err)
	}

	if fastKeep == nil || reflKeep == nil {
		t.Fatalf("retained element pointers must be non-nil (value branch): fast=%v reflect=%v", fastKeep, reflKeep)
	}
	if *fastKeep != *reflKeep {
		t.Fatalf("arm divergence: retained pointer reads %d (unsafe) vs %d (reflect)", *fastKeep, *reflKeep)
	}
	if *fastKeep != 2 {
		t.Fatalf("retained pointer should observe the reused decode (want 2); got %d", *fastKeep)
	}
}
