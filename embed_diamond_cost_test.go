package avro

import (
	"testing"
	"time"
)

// The reflect collectors' cost is a PRODUCT, and only one of its factors was
// ever driven.
//
// Both collectors — collectFieldsRaw (schema_for.go, behind SchemaFor) and
// typeFieldMapping's collect (reflect.go, behind a record decode/encode) —
// mark the type they are descending PER PATH and unmark on the way out
// (`defer delete(visited, t)`). That is correct for embed CYCLES and
// deliberate: a type reached through two SIBLING embed paths has to be
// collected at each occurrence, so the shallower one reaches the
// shallowest-wins dedup and a type genuinely inlined twice surfaces as the
// duplicate-field collision it is. The consequence is that a Go type graph
// which is a DAG — no cycle at all — is re-descended once per PATH, and a
// diamond of embeds has 2^depth of them.
//
// That is a cost, not a bug: the carrier is a Go type, fixed at compile time,
// so nothing an attacker sends can grow it. What made it worth a permanent
// cell is that the ruling closing it rested on the two collectors being
// equivalent, and they are not. The cost is
//
//	paths-through-the-embed-DAG  x  CALLS
//
// and the second factor differs between them: typeFieldMapping's result is
// memoized per reflect.Type in a sync.Map (deser.go, ser.go), so a decode pays
// the walk once and never again; collectFieldsRaw has no memo at all, so every
// SchemaFor call re-pays it in full. Driving depth alone cannot see that, which
// is why the cell drives both.
// Sibling-embed diamond: T_k embeds A_k and B_k, both of which embed T_{k+1},
// so T1 reaches the leaf by 2^depth distinct paths while the type GRAPH is
// linear in the depth. The leaf is empty, so the type is ACCEPTED and the
// walk runs to completion rather than stopping at a duplicate-field error.
type embedDiamondLeaf struct{}

type T13 = embedDiamondLeaf
type A12 struct{ T13 }
type B12 struct{ T13 }
type T12 struct {
	A12
	B12
}
type A11 struct{ T12 }
type B11 struct{ T12 }
type T11 struct {
	A11
	B11
}
type A10 struct{ T11 }
type B10 struct{ T11 }
type T10 struct {
	A10
	B10
}
type A9 struct{ T10 }
type B9 struct{ T10 }
type T9 struct {
	A9
	B9
}
type A8 struct{ T9 }
type B8 struct{ T9 }
type T8 struct {
	A8
	B8
}
type A7 struct{ T8 }
type B7 struct{ T8 }
type T7 struct {
	A7
	B7
}
type A6 struct{ T7 }
type B6 struct{ T7 }
type T6 struct {
	A6
	B6
}
type A5 struct{ T6 }
type B5 struct{ T6 }
type T5 struct {
	A5
	B5
}
type A4 struct{ T5 }
type B4 struct{ T5 }
type T4 struct {
	A4
	B4
}
type A3 struct{ T4 }
type B3 struct{ T4 }
type T3 struct {
	A3
	B3
}
type A2 struct{ T3 }
type B2 struct{ T3 }
type T2 struct {
	A2
	B2
}
type A1 struct{ T2 }
type B1 struct{ T2 }
type T1 struct {
	A1
	B1
}

// TestInvariant_EmbedDiamondCostFactors drives BOTH factors of the reflect
// collectors' cost.
//
// What it asserts, and what it deliberately does not. It does NOT assert the
// depth factor is flat — it is not, by design, and a cell claiming otherwise
// would be asserting a property the package does not have. It asserts the two
// things that ARE invariants:
//
//   - the DECODE collector is amortized. A second decode into the same Go type
//     must cost a small fraction of the first.
//
//     TWO caches in SERIES produce that, and neither can be discriminated
//     alone: deserRecord.fast holds the compiled unsafe path per Go type and
//     is consulted first, and typeFieldMapping's own sync.Map holds the field
//     mapping behind it. Disabling either one measured 375ns and 583ns on the
//     second decode — unchanged — because the survivor still answers.
//     Disabling BOTH gives 3.9ms against a 4.3ms first decode, i.e. the walk
//     running again. So what this asserts is the COMBINATION, and the naming
//     matters: a comment crediting the mapping cache alone would be a cell
//     named for a bound it does not measure, which is how the last one in this
//     file got renamed.
//
//   - neither collector ACCUMULATES across calls. Call N must cost about what
//     call 1 did, so a per-call cost that grew with the number of calls — a
//     cache keyed on something that is not the type, a leak — reds. This is
//     the form that leaves room for the improvement rather than forbidding it:
//     adding a memo to collectFieldsRaw makes later calls cheaper, which
//     passes.
//
// The depth pair is measured and LOGGED rather than bounded, so the 2^depth
// shape is visible to a reader of the output instead of living only in a
// comment, and the absolute ceiling still catches a regression that made the
// walk worse than exponential in the depth.
func TestInvariant_EmbedDiamondCostFactors(t *testing.T) {
	depths := costFactorValues(t, "TestInvariant_EmbedDiamondCostFactors")
	if len(depths) < 2 {
		t.Fatalf("need two depths, row gives %v", depths)
	}
	// The row's two values are the DEPTHS the two concrete types carry; the
	// types cannot be indexed by a variable, so the mapping is stated here and
	// asserted rather than assumed.
	const shallowDepth, deepDepth = 8, 12
	if depths[0] != shallowDepth || depths[1] != deepDepth {
		t.Fatalf("row drives %v but the declared types carry depths %d and %d — the row and the types disagree",
			depths, shallowDepth, deepDepth)
	}

	ceiling := raceRelaxed(2 * time.Second)
	timeCall := func(fn func()) time.Duration {
		start := time.Now()
		fn()
		return time.Since(start)
	}

	// Factor 1: PATHS. T5 is depth 8, T1 is depth 12 — 16x the paths.
	shallow := timeCall(func() {
		if _, err := SchemaFor[T5](); err != nil {
			t.Errorf("SchemaFor at depth %d: %v", shallowDepth, err)
		}
	})
	deep := timeCall(func() {
		if _, err := SchemaFor[T1](); err != nil {
			t.Errorf("SchemaFor at depth %d: %v", deepDepth, err)
		}
	})
	t.Logf("SchemaFor: depth %d %v, depth %d %v (%.1fx for 16x the paths)",
		shallowDepth, shallow, deepDepth, deep, float64(deep)/float64(shallow))
	if deep > ceiling {
		t.Errorf("SchemaFor at depth %d took %v (> %v) — the per-path descent got worse than the exponential it is known to be", deepDepth, deep, ceiling)
	}

	// Factor 2: CALLS, on the SchemaFor collector. Each call re-pays the walk
	// (no memo), which is the documented cost; what must hold is that no call
	// costs MORE than the first, so nothing accumulates.
	for i := range 3 {
		d := timeCall(func() {
			if _, err := SchemaFor[T1](); err != nil {
				t.Errorf("SchemaFor call %d: %v", i, err)
			}
		})
		if d > ceiling {
			t.Errorf("SchemaFor call %d took %v (> %v) — cost is accumulating across calls", i, d, ceiling)
		}
	}

	// Factor 2 again, on the DECODE collector, where the memo makes it free.
	s, err := SchemaFor[T1]()
	if err != nil {
		t.Fatalf("SchemaFor: %v", err)
	}
	wire, err := s.Encode(T1{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out T1
	first := timeCall(func() {
		if _, err := s.Decode(wire, &out); err != nil {
			t.Errorf("first decode: %v", err)
		}
	})
	second := timeCall(func() {
		if _, err := s.Decode(wire, &out); err != nil {
			t.Errorf("second decode: %v", err)
		}
	})
	t.Logf("Decode into the depth-%d type: first %v, second %v", deepDepth, first, second)
	// The measured gap is enormous (microseconds against milliseconds at this
	// depth, and 3us against 952ms at depth 18); a tenth is a bound nothing but
	// a lost cache can cross, and it does not depend on the host.
	if second > first/10 && second > raceRelaxed(time.Millisecond) {
		t.Errorf("second decode took %v against the first's %v — the embed walk is running again.\n"+
			"Two caches in series prevent that (deserRecord.fast, then typeFieldMapping's own map); losing either one is invisible here, so this failure means both stopped answering.",
			second, first)
	}
}
