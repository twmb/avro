package avro

import (
	"reflect"
	"testing"
)

// TestRegression_EmbedFieldSelectionMatchesGoPromotion pins twmb's embedded-
// field selection against Go's OWN field promotion — reflect.Value.FieldByName,
// which is exactly the resolver Go uses for `v.X`. This is the narrow, correct
// oracle for the "shallowest wins" contract: NOT encoding/json (whose tag
// namespace, tag options, and case-insensitive decode all differ from avro's),
// but Go promotion itself, which is tag-independent and is what doc.go promises.
//
// Scope that makes the oracle valid: every field's avro name equals its Go
// field name (no `avro:"rename"`), so "which field does name N resolve to" is
// a pure Go-promotion question both twmb and reflect.FieldByName answer the
// same way. Each case sets a DISTINCT value in every physical occurrence of
// the shadowed field, so a wrong selection is observable as the wrong value.
func TestRegression_EmbedFieldSelectionMatchesGoPromotion(t *testing.T) {
	// Named base types — embedding requires named types, so the same type can
	// be reachable through more than one path (the shape the bug lived in).
	type C struct {
		X int32 `avro:"X"`
	}
	type D struct{ C } // D.C.X at one more level of depth
	type E struct{ D } // E.D.C.X, deeper still

	cases := []struct {
		name  string
		value any // a struct, fields pre-set distinctly per physical occurrence
	}{
		// Repeated type, different depths: shallow C.X (depth 1) must win over
		// deep D.C.X (depth 2). The bug picked the deep one.
		{"repeat-2-level", func() any {
			type R struct {
				D
				C
			}
			var r R
			r.D.C.X = 11 // deep
			r.C.X = 22   // shallow
			return r
		}()},
		// Repeated type, three levels vs one: E.D.C.X (depth 3) vs C.X (depth 1).
		{"repeat-3-level", func() any {
			type R struct {
				E
				C
			}
			var r R
			r.E.D.C.X = 33 // deepest
			r.C.X = 44     // shallow
			return r
		}()},
		// Direct field shadows an embed of the same name (the shape the
		// pre-existing pins covered — still must hold).
		{"direct-shadows-embed", func() any {
			type R struct {
				X int32 `avro:"X"`
				C
			}
			var r R
			r.X = 55   // direct, depth 0
			r.C.X = 66 // embedded, depth 1
			return r
		}()},
		// Deep repeat where the SHALLOW path is itself one level (D) and the
		// deep is two (E): D.C.X (depth 2) vs E.D.C.X (depth 3).
		{"two-embeds-different-depth", func() any {
			type R struct {
				E
				D
			}
			var r R
			r.E.D.C.X = 77 // depth 3
			r.D.C.X = 88   // depth 2 — shallower, wins
			return r
		}()},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rv := reflect.ValueOf(c.value)

			// Go's promotion oracle. FieldByName returns the zero Value when
			// the name is absent OR ambiguous; we only assert agreement where
			// Go resolves it unambiguously (the equal-depth ambiguous case is
			// a separate documented policy, below).
			of := rv.FieldByName("X")
			if !of.IsValid() {
				t.Skipf("Go promotion is ambiguous for %s — equal-depth policy, see the ambiguity test", c.name)
			}
			want := of.Int()

			s := MustParse(`{"type":"record","name":"R","fields":[{"name":"X","type":"int"}]}`)

			// Encode must read the Go-promoted field.
			data, err := s.AppendEncode(nil, c.value)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			var out map[string]any
			if _, err := s.Decode(data, &out); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if int64(out["X"].(int32)) != want {
				t.Fatalf("encode selected a field disagreeing with Go promotion: twmb X=%v, reflect.FieldByName(X)=%d", out["X"], want)
			}

			// Decode must WRITE the Go-promoted field. Build a fresh zero
			// value of the same type, decode X=99 into it, and confirm
			// FieldByName(X) on the result is 99 (the promoted field got it).
			zero := reflect.New(rv.Type())
			wire, _ := s.AppendEncode(nil, map[string]any{"X": int32(99)})
			if _, err := s.Decode(wire, zero.Interface()); err != nil {
				t.Fatalf("decode into struct: %v", err)
			}
			if got := zero.Elem().FieldByName("X").Int(); got != 99 {
				t.Fatalf("decode wrote a field disagreeing with Go promotion: FieldByName(X)=%d, want 99", got)
			}
		})
	}
}

// TestRegression_EmbedEqualDepthAmbiguity documents the one case the Go-
// promotion oracle CANNOT adjudicate: a type reachable at EQUAL depth through
// two different embeds. Go makes `r.X` a compile error (ambiguous selector)
// and reflect.FieldByName returns the zero Value; encoding/json drops the
// field. doc.go's "shallowest wins" rule is silent on equal-depth ties, so
// twmb's behavior here is a deliberate policy choice (first-wins), pinned so a
// future change to it is a conscious decision, not an accident.
func TestRegression_EmbedEqualDepthAmbiguity(t *testing.T) {
	type C struct {
		X int32 `avro:"X"`
	}
	type A struct{ C }
	type B struct{ C }
	type R struct {
		A
		B
	}

	// Confirm Go itself considers this ambiguous (the oracle abstains).
	if _, ok := reflect.TypeFor[R]().FieldByName("X"); ok {
		t.Fatal("precondition: expected Go to treat R.X as ambiguous")
	}

	var r R
	r.A.C.X = 1
	r.B.C.X = 2
	s := MustParse(`{"type":"record","name":"R","fields":[{"name":"X","type":"int"}]}`)

	// Documented policy: twmb does NOT error; it first-wins (the A.C.X path).
	// This is not Go-promotion-correct (Go would reject), but doc.go only
	// promises shallowest-wins for DIFFERENT depths. Pin the current choice.
	data, err := s.AppendEncode(nil, &r)
	if err != nil {
		t.Fatalf("equal-depth encode unexpectedly errored: %v", err)
	}
	var out map[string]any
	if _, err := s.Decode(data, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out["X"] != int32(1) {
		t.Fatalf("equal-depth first-wins policy changed: X=%v, want 1 (A.C.X). If intentional, update this pin.", out["X"])
	}
}
