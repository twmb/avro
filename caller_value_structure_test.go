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
// The oracle is ABSOLUTE, and it has to be. An earlier version asserted only
// no-panic plus verdict-class AGREEMENT across the members, which is a relative
// property that any uniform change satisfies: removing the bad-map-key guard
// flipped two values reject->accept at every member INCLUDING the flat baseline,
// so the verdicts still agreed and the net stayed green while the map-key
// regression pin red through the same neuter. A baseline that is a member of the
// agreement set is not an anchor.
//
// So each value carries an EXPECTED verdict derived from an authority outside
// this package. For almost all of them that authority is executed rather than
// written down: the package emits caller values through encoding/json, so
// whether json.Marshal accepts the value decides whether it can reach the wire
// at all, and the expectation is computed per cell by calling it. Two values
// have a documented package rule that overrides the stdlib, and each says so —
// non-finite floats, which a documented fixup rewrites into JSON-expressible
// form, and a deeply nested value, which the documented walk budget refuses
// even though the stdlib would marshal it.
//
// Agreement across structures is kept as a SECOND assertion, because it catches
// something the absolute one cannot: a verdict that depends on which structure
// the value sits in.
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

// cvHostile is a value plus the authority that settles what must happen to it.
// override is empty when encoding/json decides — which is the usual case, and
// is EXECUTED per cell rather than recorded here, so the expectation tracks the
// stdlib instead of a snapshot of it.
type cvHostile struct {
	name     string
	val      any
	override string // "" | "ok" | "error", matching cvVerdict's vocabulary
	why      string
}

// cvHostileValues is the value domain, drawn from the shapes the tree-value
// census enumerates: a marshal that errors, one that emits invalid JSON, a text
// marshal that errors, unmarshalable kinds, cycles, map keys the stdlib cannot
// name, and sizes that reach the walk budgets.
func cvHostileValues() []cvHostile {
	return []cvHostile{
		{name: "errMarshaler", val: cvErrMarshaler{}},
		{name: "badJSONMarshaler", val: cvBadJSON{}},
		{name: "errTextMarshaler", val: cvErrText{}},
		{name: "func", val: func() {}},
		{name: "chan", val: make(chan int)},
		{name: "complex", val: complex(1, 2)},
		{name: "cyclicMap", val: cvCyclicMap()},
		{name: "cyclicSlice", val: cvCyclicSlice()},
		{name: "floatKeyMap", val: map[float64]string{1.5: "a"}},
		{name: "structKeyMap", val: map[cvBadKey]string{{X: 1}: "a"}},
		{name: "invalidRawMessage", val: json.RawMessage("{oops")},
		{name: "nonNumericJSONNumber", val: json.Number("notanumber")},
		{name: "hugeString", val: strings.Repeat("x", 1<<20)},
		// The depth pair straddles the documented walk budget, measured
		// rather than assumed: an earlier draft used 2000 and expected a
		// refusal, but the bound sits above that, so the cell asserted a
		// rejection that correctly never came.
		{
			name: "deepNest-underBudget", val: cvDeep(2000),
			// no override: the stdlib marshals it and so does this package
		},
		{
			name: "deepNest-overBudget", val: cvDeep(3000), override: "error",
			why: "the stdlib marshals it; this package's documented walk DEPTH budget refuses it first",
		},
		{
			name: "nan", val: math.NaN(), override: "ok",
			why: "the stdlib refuses NaN; this package's documented non-finite fixup rewrites it into a JSON-expressible form",
		},
		{
			name: "posInf", val: math.Inf(1), override: "ok",
			why: "the stdlib refuses +Inf; the same documented fixup emits it as an overflowing numeric literal that re-parses",
		},
		// Deliberately NOT here: a bare nil. It is a LEGAL default whose
		// verdict is decided by the field's TYPE — valid for a nullable
		// union, rejected otherwise — so its class legitimately differs
		// between structures whose first field differs, and holding it
		// constant would make this oracle wrong rather than strict. The
		// hostile domain is values no schema can accept, not values some
		// schema can.
	}
}

// cvExpect returns the required verdict and the authority behind it. With no
// override the authority is encoding/json, CALLED here rather than quoted.
func cvExpect(hv cvHostile, typeChecked bool) (want, authority string) {
	if typeChecked {
		return "error", "a field default is validated against the field's DECLARED TYPE, and no value in this " +
			"hostile domain is a valid instance of it — so marshalability decides nothing here"
	}
	if hv.override != "" {
		return hv.override, hv.why
	}
	if _, err := json.Marshal(hv.val); err != nil {
		return "error", "encoding/json refuses to marshal it, and this package emits caller values through it"
	}
	return "ok", "encoding/json marshals it, so nothing downstream has grounds to refuse it"
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
	// typeChecked marks a position whose value is additionally validated
	// against the field's DECLARED TYPE, not just marshalled. A default is;
	// a custom property is not. That changes the authority: for a default,
	// marshalability decides nothing, because a value that marshals fine is
	// still refused unless it is a valid instance of the field's type — and
	// no value in this domain is.
	typeChecked bool
	put         func(root *SchemaNode, picked *SchemaNode, v any) SchemaNode
}{
	{"picked.Props (the spliced reference)", false, func(_ *SchemaNode, n *SchemaNode, v any) SchemaNode {
		if n.Props == nil {
			n.Props = map[string]any{}
		}
		n.Props["hostile"] = v
		return *n
	}},
	{"root.Props", false, func(root *SchemaNode, _ *SchemaNode, v any) SchemaNode {
		if root.Props == nil {
			root.Props = map[string]any{}
		}
		root.Props["hostile"] = v
		return *root
	}},
	{"root.Fields[0].Props", false, func(root *SchemaNode, _ *SchemaNode, v any) SchemaNode {
		if len(root.Fields) > 0 {
			if root.Fields[0].Props == nil {
				root.Fields[0].Props = map[string]any{}
			}
			root.Fields[0].Props["hostile"] = v
		}
		return *root
	}},
	{"root.Fields[0].Default", true, func(root *SchemaNode, _ *SchemaNode, v any) SchemaNode {
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
			verdicts["flat-baseline"] = cvVerdict(driveSurfaces(slot.put(flat, flatPick, hv.val), nil))
			cells++

			for _, st := range structures {
				s := st.build(t)
				root := s.Root()
				picked := st.pick(*root)
				drive := slot.put(root, &picked, hv.val)
				verdicts[st.name] = cvVerdict(driveSurfaces(drive, st.val))
				cells++
			}
			want, authority := cvExpect(hv, slot.typeChecked)
			// ABSOLUTE first: every member must land on the required verdict.
			// This is what a uniform regression trips; agreement alone does not.
			for name, v := range verdicts {
				if v != "PANIC" && v != want {
					t.Errorf("%s / %s / %s: verdict %q, want %q — %s",
						slot.name, hv.name, name, v, want, authority)
				}
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
