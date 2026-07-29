package avro

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Caller-supplied VALUE domain × STRUCTURE.
//
// Two nets already exist on either side of this cross and neither spans it. The
// caller-node matrix drives which FIELD a caller writes across every structure
// (second occurrence, forward reference, recursive, diamond, cache cross-parse)
// but writes one ORDINARY value into each. The tree-value census drives the
// hostile Go value domain — erroring marshalers, cycles, non-string map keys,
// unmarshalable kinds — but at ONE flat position.
//
// The untested claim is that they compose: that a value which fails cleanly at
// a flat node also fails cleanly when the node it sits on is a stamped
// reference about to be spliced, a definition inside a recursive cycle, or one
// arm of a diamond. Those paths do extra work with the node — merging props
// onto a spliced definition, walking a visited set, comparing bodies for a
// dedup conflict — and that work runs BEFORE any marshal error surfaces.
//
// The oracle is calibration-free and does not name any particular error: for a
// given hostile value, the VERDICT CLASS must be the same at every structure.
// A value that is rejected flat must not be silently accepted at a splice, and
// nothing may panic. Which class it is (accepted, or which error) is the
// existing nets' business; that it does not depend on the STRUCTURE is this
// net's.
// ---------------------------------------------------------------------------

type cvErrMarshaler struct{}

func (cvErrMarshaler) MarshalJSON() ([]byte, error) { return nil, errors.New("cv-boom") }

type cvBadJSON struct{}

func (cvBadJSON) MarshalJSON() ([]byte, error) { return []byte("{oops"), nil }

type cvErrText struct{}

func (cvErrText) MarshalText() ([]byte, error) { return nil, errors.New("cv-text-boom") }

type cvBadKey struct{ X int }

func cvCyclicMap() any {
	m := map[string]any{}
	m["self"] = m
	return m
}

func cvCyclicSlice() any {
	s := make([]any, 1)
	s[0] = s
	return s
}

func cvDeep(n int) any {
	var v any = 1
	for range n {
		v = []any{v}
	}
	return v
}

// cvHostileValues is the value domain, drawn from the shapes the tree-value
// census already enumerates: a marshal that errors, one that emits invalid
// JSON, a text marshal that errors, unmarshalable kinds, cycles, map keys the
// stdlib cannot resolve, and sizes that reach the walk budgets.
func cvHostileValues() []struct {
	name string
	val  any
} {
	return []struct {
		name string
		val  any
	}{
		{"errMarshaler", cvErrMarshaler{}},
		{"badJSONMarshaler", cvBadJSON{}},
		{"errTextMarshaler", cvErrText{}},
		{"func", func() {}},
		{"chan", make(chan int)},
		{"complex", complex(1, 2)},
		{"cyclicMap", cvCyclicMap()},
		{"cyclicSlice", cvCyclicSlice()},
		{"floatKeyMap", map[float64]string{1.5: "a"}},
		{"structKeyMap", map[cvBadKey]string{{X: 1}: "a"}},
		{"deepNest2000", cvDeep(2000)},
		{"invalidRawMessage", json.RawMessage("{oops")},
		{"nonNumericJSONNumber", json.Number("notanumber")},
		{"nan", math.NaN()},
		{"posInf", math.Inf(1)},
		{"hugeString", strings.Repeat("x", 1<<20)},
		// Deliberately NOT here: a bare nil. It is a LEGAL default whose
		// verdict is decided by the field's TYPE — valid for a nullable
		// union, rejected otherwise — so its class legitimately differs
		// between structures whose first field differs, and holding it
		// constant would make this oracle wrong rather than strict. The
		// hostile domain is values no schema can accept, not values some
		// schema can.
	}
}

// cvVerdict reduces a surface report to a CLASS. It deliberately does not
// compare error text: two structures legitimately name different field paths,
// and this net asks whether the outcome KIND depends on structure, not whether
// the message does.
func cvVerdict(rep surfaceReport) string {
	switch {
	case rep.panicked != nil:
		return "PANIC"
	case rep.err != nil:
		return "error"
	default:
		return "ok"
	}
}

// cvSlots are the caller-writable positions that take an arbitrary Go value.
// Every other exported SchemaNode field is typed, so the hostile domain cannot
// reach it — which is why this cross is these positions wide.
//
// Each slot returns THE NODE TO DRIVE, and that return is the whole reason the
// slot is a function rather than a field name: st.pick returns a COPY, exactly
// as a caller gets, so writing into the picked node and then driving the ROOT
// exercises nothing at all. The picked slot must be driven on the picked node —
// which is also the only slot that reaches the splice, the path this cross
// exists to test. A neuter that drops caller props at the splice reds it and
// nothing else.
var cvSlots = []struct {
	name string
	put  func(root *SchemaNode, picked *SchemaNode, v any) SchemaNode
}{
	{"picked.Props (the spliced reference)", func(_ *SchemaNode, n *SchemaNode, v any) SchemaNode {
		if n.Props == nil {
			n.Props = map[string]any{}
		}
		n.Props["hostile"] = v
		return *n
	}},
	{"root.Props", func(root *SchemaNode, _ *SchemaNode, v any) SchemaNode {
		if root.Props == nil {
			root.Props = map[string]any{}
		}
		root.Props["hostile"] = v
		return *root
	}},
	{"root.Fields[0].Props", func(root *SchemaNode, _ *SchemaNode, v any) SchemaNode {
		if len(root.Fields) > 0 {
			if root.Fields[0].Props == nil {
				root.Fields[0].Props = map[string]any{}
			}
			root.Fields[0].Props["hostile"] = v
		}
		return *root
	}},
	{"root.Fields[0].Default", func(root *SchemaNode, _ *SchemaNode, v any) SchemaNode {
		if len(root.Fields) > 0 {
			root.Fields[0].Default = v
			root.Fields[0].HasDefault = true
		}
		return *root
	}},
}

// TestMatrix_CallerValueDomainAcrossStructures crosses the hostile value domain
// with every structure the caller-node matrix builds.
func TestMatrix_CallerValueDomainAcrossStructures(t *testing.T) {
	structures := callerNodeStructures()
	if len(structures) < 3 {
		t.Fatalf("only %d structures; this cross is meaningless without the splice shapes", len(structures))
	}
	var cells int
	for _, slot := range cvSlots {
		for _, hv := range cvHostileValues() {
			// Collect the verdict at every structure, then compare.
			verdicts := make(map[string]string, len(structures)+1)

			// The FLAT baseline is not optional and is not one of the
			// structures: every member of callerNodeStructures() splices, so
			// comparing them only to each other cannot see a change that
			// affects the splice UNIFORMLY. Without this control a neuter that
			// makes the splice drop caller props leaves all five agreeing on
			// "ok" and the net stays green over the exact regression it names.
			flat := MustParse(`{"type":"record","name":"Flat","fields":[{"name":"f","type":"int"}]}`).Root()
			flatPick := flat
			verdicts["flat-baseline"] = cvVerdict(driveSurfaces(slot.put(&flat, &flatPick, hv.val), nil))
			cells++

			for _, st := range structures {
				s := st.build(t)
				root := s.Root()
				picked := st.pick(root)
				drive := slot.put(&root, &picked, hv.val)
				verdicts[st.name] = cvVerdict(driveSurfaces(drive, st.val))
				cells++
			}
			// Any panic is a failure on its own, named per structure.
			for name, v := range verdicts {
				if v == "PANIC" {
					t.Errorf("%s / %s / %s: PANICKED — a caller value must produce a value or a named error, never a panic",
						slot.name, hv.name, name)
				}
			}
			// The class must not depend on the structure.
			distinct := map[string][]string{}
			for name, v := range verdicts {
				distinct[v] = append(distinct[v], name)
			}
			if len(distinct) > 1 {
				t.Errorf("%s / %s: verdict CLASS depends on the structure — %v.\n"+
					"  A value rejected at one shape must not be accepted at another: the splice merges props onto a\n"+
					"  definition, the recursive walk carries a visited set, and the diamond compares bodies for a dedup\n"+
					"  conflict, all before any marshal error surfaces.", slot.name, hv.name, distinct)
			}
		}
	}
	t.Logf("cells: %d (%d structures × %d slots × %d values)", cells, len(structures), len(cvSlots), len(cvHostileValues()))
}
