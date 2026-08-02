package avro

import (
	"fmt"
	"strings"
	"testing"
)

// A per-element minimum selects which block-count RULE applies, and the rules
// are not ordered: zero takes the zero-byte cap, positive takes the
// buffer-relative bound, and neither is uniformly looser. So the walk may never
// round a minimum UP when it cannot compute one — reporting 1 for a type whose
// true minimum is 0 does not loosen the bound, it moves a legitimately
// zero-byte container onto a rule it cannot satisfy.
//
// The walk has two places it cannot compute: an unwired forward reference
// (nil child) and an exhausted allowance. Both used to report 1.

// standInSCC is a cyclic SCC deep enough that one walk over it exhausts the
// min-bytes allowance. Defined first and fully wired, so a later container over
// "L0" resolves to a built node on the BUILD path.
func standInSCC(levels int) string {
	inner := `["null","L0"]`
	for i := levels - 1; i >= 0; i-- {
		if i == levels-1 {
			inner = fmt.Sprintf(`{"type":"record","name":"L%d","fields":[{"name":"f0","type":["null","L0"]},{"name":"f1","type":["null","L0"]}]}`, i)
			continue
		}
		inner = fmt.Sprintf(`{"type":"record","name":"L%d","fields":[{"name":"f0","type":["null",%s]},{"name":"f1","type":["null","L%d"]}]}`, i, inner, i+1)
	}
	return inner
}

const standInSCCLevels = 26

// TestRegression_ZeroMinimumContainerAfterDrainedAllowance pins the exhaustion
// stand-in. Two schemas differing ONLY in field order: the shared walk is
// drained by the SCC container before the zero-byte container is reached in one
// and after it in the other. Both must accept the array their own encoder
// produced — an uncomputed minimum may change how loose a bound is, never
// whether a valid wire passes.
func TestRegression_ZeroMinimumContainerAfterDrainedAllowance(t *testing.T) {
	mk := func(drainFirst bool) string {
		var b strings.Builder
		b.WriteString(`{"type":"record","name":"Root","fields":[{"name":"def","type":` + standInSCC(standInSCCLevels) + `}`)
		zero := `,{"name":"z","type":{"type":"array","items":"null"}}`
		drain := `,{"name":"a","type":{"type":"array","items":"L0"}}`
		if drainFirst {
			b.WriteString(drain + zero)
		} else {
			b.WriteString(zero + drain)
		}
		b.WriteString(`]}`)
		return b.String()
	}
	for _, drainFirst := range []bool{false, true} {
		name := map[bool]string{false: "zero-min container built first", true: "zero-min container built after the drain"}[drainFirst]
		t.Run(name, func(t *testing.T) {
			s := MustParse(mk(drainFirst))
			val := map[string]any{
				"def": map[string]any{"f0": nil, "f1": nil},
				"a":   []any{},
				"z":   make([]any, maxZeroByteItems),
			}
			wire, err := s.Encode(val)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			var out map[string]any
			if _, err := s.Decode(wire, &out); err != nil {
				t.Fatalf("own encoder produced %d bytes its own decoder rejects: %v", len(wire), err)
			}
			if got := len(out["z"].([]any)); got != maxZeroByteItems {
				t.Fatalf("decoded %d zero-byte items, want %d", got, maxZeroByteItems)
			}
		})
	}
}

// TestRegression_ZeroMinimumContainerBehindForwardRef pins the nil-child
// stand-in, which needs no adversarial schema at all: a plain forward reference
// to an empty record. The container's element is only reachable after finalize
// wires it, so at build the walk sees a nil child.
func TestRegression_ZeroMinimumContainerBehindForwardRef(t *testing.T) {
	const arr = `{"type":"array","items":{"type":"record","name":"Inner","fields":[{"name":"g","type":"Later"}]}}`
	const later = `{"type":"record","name":"Later","fields":[]}`
	for _, c := range []struct{ name, src string }{
		{"element defined after the container (forward ref)",
			`{"type":"record","name":"Root","fields":[{"name":"z","type":` + arr + `},{"name":"d","type":` + later + `}]}`},
		{"element defined before the container (backward ref)",
			`{"type":"record","name":"Root","fields":[{"name":"d","type":` + later + `},{"name":"z","type":` + arr + `}]}`},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := MustParse(c.src)
			items := make([]any, maxZeroByteItems)
			for i := range items {
				items[i] = map[string]any{"g": map[string]any{}}
			}
			wire, err := s.Encode(map[string]any{"z": items, "d": map[string]any{}})
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			var out map[string]any
			if _, err := s.Decode(wire, &out); err != nil {
				t.Fatalf("own encoder produced %d bytes its own decoder rejects: %v", len(wire), err)
			}
		})
	}
}

// ---- the class matrix ----------------------------------------------------

// standInCase is one cell: a schema whose container's per-element minimum is
// reached through a named stand-in source, with a known true minimum.
type standInCase struct {
	name     string
	src      string
	value    func(n int) map[string]any // the record value carrying n elements
	zeroMin  bool                       // the element's TRUE minimum is 0
	countKey string                     // field holding the container under test
}

// standInCases crosses STAND-IN SOURCE x CONTAINER x ELEMENT-TRUE-MINIMUM.
// The stand-in source is the axis the bug turned on; the element's true minimum
// is the axis that decides which rule is correct, and holding it at "positive"
// is why an over-reporting stand-in looked harmless.
func standInCases() []standInCase {
	scc := standInSCC(standInSCCLevels)
	drainPrefix := `{"name":"def","type":` + scc + `},{"name":"a","type":{"type":"array","items":"L0"}},`
	drainVal := func(m map[string]any) map[string]any {
		m["def"] = map[string]any{"f0": nil, "f1": nil}
		m["a"] = []any{}
		return m
	}
	emptyRec := `{"type":"record","name":"E","fields":[]}`

	arrVal := func(elem func() any) func(int) map[string]any {
		return func(n int) map[string]any {
			items := make([]any, n)
			for i := range items {
				items[i] = elem()
			}
			return map[string]any{"z": items}
		}
	}
	mapVal := func(elem func() any) func(int) map[string]any {
		return func(n int) map[string]any {
			m := make(map[string]any, n)
			for i := range n {
				m[fmt.Sprintf("k%d", i)] = elem()
			}
			return map[string]any{"z": m}
		}
	}
	nilElem := func() any { return nil }
	recElem := func() any { return map[string]any{} }
	intElem := func() any { return int32(1) }

	rec := func(fields string) string {
		return `{"type":"record","name":"Root","fields":[` + fields + `]}`
	}

	var cs []standInCase
	for _, container := range []struct {
		kind string
		wrap func(elem string) string
		val  func(func() any) func(int) map[string]any
	}{
		{"array", func(e string) string { return `{"type":"array","items":` + e + `}` }, arrVal},
		{"map", func(e string) string { return `{"type":"map","values":` + e + `}` }, mapVal},
	} {
		for _, elem := range []struct {
			kind string
			src  string
			mk   func() any
			zero bool
		}{
			{"zero-min/null", `"null"`, nilElem, true},
			{"zero-min/empty-record", emptyRec, recElem, true},
			{"positive-min/int", `"int"`, intElem, false},
		} {
			z := `{"name":"z","type":` + container.wrap(elem.src) + `}`
			// none: the control — nothing prevents the walk from computing.
			cs = append(cs, standInCase{
				name: "none/" + container.kind + "/" + elem.kind, src: rec(z),
				value: container.val(elem.mk), zeroMin: elem.zero, countKey: "z",
			})
			// drained: the container is built after a walk-exhausting sibling.
			cs = append(cs, standInCase{
				name: "drained/" + container.kind + "/" + elem.kind, src: rec(drainPrefix + z),
				value:   func(n int) map[string]any { return drainVal(container.val(elem.mk)(n)) },
				zeroMin: elem.zero, countKey: "z",
			})
		}
		// nil-child: the element's own subtree holds an unwired forward
		// reference at build. Crossed against BOTH true minima, because the
		// stand-in is only wrong for one of them.
		for _, elem := range []struct {
			kind   string
			fields string
			mk     func() any
			zero   bool
		}{
			{"zero-min/fwd-ref-to-empty", `{"name":"g","type":"Later"}`,
				func() any { return map[string]any{"g": map[string]any{}} }, true},
			{"positive-min/fwd-ref-plus-int", `{"name":"p","type":"int"},{"name":"g","type":"Later"}`,
				func() any { return map[string]any{"p": int32(1), "g": map[string]any{}} }, false},
		} {
			inner := `{"type":"record","name":"Inner","fields":[` + elem.fields + `]}`
			z := `{"name":"z","type":` + container.wrap(inner) + `}`
			later := `{"name":"d","type":{"type":"record","name":"Later","fields":[]}}`
			cs = append(cs, standInCase{
				name: "nil-child/" + container.kind + "/" + elem.kind, src: rec(z + "," + later),
				value: func(n int) map[string]any {
					m := container.val(elem.mk)(n)
					m["d"] = map[string]any{}
					return m
				},
				zeroMin: elem.zero, countKey: "z",
			})
		}
	}
	return cs
}

// TestMatrix_MinBytesStandInNeverOverReports is the class net. Its oracle is
// ENCODE-IMPLIES-DECODE — this package's own encoder produces the wire, so its
// own decoder must accept it — which is calibration-free and reads nothing off
// the walk's current behavior.
//
// The DoS half runs on every cell too: a bound that stopped false-rejecting by
// disappearing would pass the accept half alone.
func TestMatrix_MinBytesStandInNeverOverReports(t *testing.T) {
	for _, c := range standInCases() {
		t.Run(c.name, func(t *testing.T) {
			s, err := Parse(c.src)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			// A count that exercises the rule: for a zero-minimum element the
			// documented zero-byte cap is the only thing bounding it, so sit
			// exactly ON the cap.
			n := maxZeroByteItems
			if !c.zeroMin {
				n = 512
			}
			wire, err := s.Encode(c.value(n))
			if err != nil {
				t.Fatalf("encode %d elements: %v", n, err)
			}
			var out map[string]any
			if _, err := s.Decode(wire, &out); err != nil {
				t.Fatalf("ACCEPT half: own encoder produced %d bytes its own decoder rejects: %v", len(wire), err)
			}

			// DoS half: a declared count far past anything the buffer could
			// hold must still be refused, whatever rule the cell landed on.
			hostile := append([]byte(nil), wire...)
			if i := indexOfBlockCount(wire, n); i >= 0 {
				hostile = append(append(append([]byte(nil), wire[:i]...), dosVarlong(1<<40)...), wire[i+len(appendVarlong(nil, int64(n))):]...)
				var sink map[string]any
				if _, err := s.Decode(hostile, &sink); err == nil {
					t.Fatalf("DoS half: a block count of 2^40 in a %d-byte buffer was accepted", len(hostile))
				}
			}
		})
	}
}

// indexOfBlockCount locates the container's block-count varint in the encoded
// record so the DoS half can overwrite it. The count is the first occurrence of
// n's own varint encoding, which is unambiguous here because every cell's
// element count is far larger than any other number in the wire.
func indexOfBlockCount(wire []byte, n int) int {
	want := appendVarlong(nil, int64(n))
	for i := 0; i+len(want) <= len(wire); i++ {
		if string(wire[i:i+len(want)]) == string(want) {
			return i
		}
	}
	return -1
}

// TestRegression_ZeroByteItemCapStillHolds is the boundary-1 half of the pins
// above: the fix must not have bought acceptance by dropping the cap. AT the cap
// accepts, one past it rejects — on a schema with no stand-in and on one where
// the walk was drained, so the unknown RULE is held to the same limit as the
// computed one.
//
// The over-cap wire is hand-built because the encoder enforces the same cap
// (encoding 4097 zero-byte items is refused), which is itself the property that
// makes the accept half's encode-implies-decode oracle meaningful.
func TestRegression_ZeroByteItemCapStillHolds(t *testing.T) {
	plain := `{"type":"record","name":"Root","fields":[{"name":"z","type":{"type":"array","items":"null"}}]}`
	drained := `{"type":"record","name":"Root","fields":[{"name":"def","type":` + standInSCC(standInSCCLevels) +
		`},{"name":"a","type":{"type":"array","items":"L0"}},{"name":"z","type":{"type":"array","items":"null"}}]}`
	for _, c := range []struct {
		name, src string
		prefix    []byte // def (two null union indexes) + a (empty array), when present
	}{
		{"no stand-in", plain, nil},
		{"drained allowance", drained, []byte{0, 0, 0}},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := MustParse(c.src)
			wireFor := func(n int) []byte {
				w := append([]byte(nil), c.prefix...)
				w = appendVarlong(w, int64(n)) // one block of n zero-byte items
				return append(w, 0)            // terminating zero block count
			}
			var out map[string]any
			if _, err := s.Decode(wireFor(maxZeroByteItems), &out); err != nil {
				t.Fatalf("AT the %d-element cap must accept: %v", maxZeroByteItems, err)
			}
			if got := len(out["z"].([]any)); got != maxZeroByteItems {
				t.Fatalf("decoded %d items at the cap, want %d", got, maxZeroByteItems)
			}
			var sink map[string]any
			if _, err := s.Decode(wireFor(maxZeroByteItems+1), &sink); err == nil {
				t.Fatalf("one past the %d-element cap must reject, but decode succeeded", maxZeroByteItems)
			}
		})
	}
}
