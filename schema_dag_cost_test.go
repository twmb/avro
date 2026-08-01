package avro

// Schema DAG cost — what a schema walk costs when the SAME node is reachable
// by more than one path.
//
// A named type referenced twice is not a tree, it is a DAG: both references
// bind to ONE *schemaNode, so a walk that re-descends per reference does
// 2^depth work on a schema whose text grows linearly. Nothing about that
// needs deep nesting — the same fan-out is expressible with every level
// declared as a sibling field wired by forward reference — so a
// nesting-depth bound cannot stand in for the memo.
//
// The walk this file guards, schemaMinBytes, is reached ONLY where a
// container asks for a per-element wire minimum. A cell that parses a bare
// record DAG never enters it and would measure nothing, so every cost cell
// here carries its trigger explicitly, and TestInvariant_MinBytesCallSites
// derives the trigger set from source rather than trusting this list.

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// ---------- schema builders ----------

// dagNested builds a chain of `levels` records where every level has `fan`
// fields of the NEXT level: the first defines it inline, the rest reference
// it by name. Both spellings bind to one node, so the type graph is a DAG
// with 2^levels distinct root-to-leaf paths.
func dagNested(levels, fan int) string {
	inner := `"int"`
	for i := levels - 1; i >= 0; i-- {
		next := fmt.Sprintf("L%d", i+1)
		if i == levels-1 {
			next = "int"
		}
		var b strings.Builder
		fmt.Fprintf(&b, `{"type":"record","name":"L%d","fields":[{"name":"f0","type":%s}`, i, inner)
		for k := 1; k < fan; k++ {
			fmt.Fprintf(&b, `,{"name":"f%d","type":"%s"}`, k, next)
		}
		b.WriteString(`]}`)
		inner = b.String()
	}
	return inner
}

// dagFlat expresses the identical type graph with a JSON nesting depth of 4
// regardless of levels: every level is a sibling field of one record, and the
// references between them are forward references. A bracket pre-scan or any
// other depth bound sees nothing here.
func dagFlat(levels, fan int) string {
	var b strings.Builder
	b.WriteString(`{"type":"record","name":"Root","fields":[`)
	b.WriteString(`{"name":"z","type":{"type":"array","items":"L0"}}`)
	for i := range levels {
		next := fmt.Sprintf("L%d", i+1)
		if i == levels-1 {
			next = "int"
		}
		fmt.Fprintf(&b, `,{"name":"d%d","type":{"type":"record","name":"L%d","fields":[{"name":"f0","type":"%s"}`, i, i, next)
		for k := 1; k < fan; k++ {
			fmt.Fprintf(&b, `,{"name":"f%d","type":"%s"}`, k, next)
		}
		b.WriteString(`]}}`)
	}
	b.WriteString(`]}`)
	return b.String()
}

// dagSelfRecursive is dagNested with every level additionally referencing
// ITSELF through a nullable union. Every node then sits on a cycle of its
// own, so a memo that refuses to record any result reached through a
// back-edge records nothing here and the fan-out is untouched.
func dagSelfRecursive(levels, fan int) string {
	inner := `"int"`
	for i := levels - 1; i >= 0; i-- {
		next := fmt.Sprintf("L%d", i+1)
		if i == levels-1 {
			next = "int"
		}
		var b strings.Builder
		fmt.Fprintf(&b, `{"type":"record","name":"L%d","fields":[{"name":"f0","type":%s}`, i, inner)
		for k := 1; k < fan; k++ {
			fmt.Fprintf(&b, `,{"name":"f%d","type":"%s"}`, k, next)
		}
		fmt.Fprintf(&b, `,{"name":"self","type":["null","L%d"]}`, i)
		b.WriteString(`]}`)
		inner = b.String()
	}
	return inner
}

// dagSingleSCC wires the DEEPEST level back to the SHALLOWEST, so every
// level belongs to one strongly-connected component. This is the shape a
// memo keyed on "was this subtree cycle-free" cannot help at all, and it is
// the reason the walk carries a visit budget as well as a memo.
func dagSingleSCC(levels, fan int) string {
	var b strings.Builder
	b.WriteString(`{"type":"record","name":"Root","fields":[`)
	b.WriteString(`{"name":"z","type":{"type":"array","items":"L0"}}`)
	for i := range levels {
		next := fmt.Sprintf("L%d", i+1)
		if i == levels-1 {
			next = "L0" // close the cycle
		}
		fmt.Fprintf(&b, `,{"name":"d%d","type":{"type":"record","name":"L%d","fields":[{"name":"f0","type":["null","%s"]}`, i, i, next)
		for k := 1; k < fan; k++ {
			fmt.Fprintf(&b, `,{"name":"f%d","type":["null","%s"]}`, k, next)
		}
		b.WriteString(`]}}`)
	}
	b.WriteString(`]}`)
	return b.String()
}

// treeExpanded is the same type as dagNested with NO sharing: every
// occurrence is written out in full, so the schema text is exponential and
// the node graph is a tree. It exists to be the value oracle — sharing a
// node must not change what the walk computes.
func treeExpanded(levels, fan int) string {
	var build func(i int) string
	build = func(i int) string {
		if i == levels {
			return `"int"`
		}
		var b strings.Builder
		b.WriteString(`{"type":"record","fields":[`)
		for k := range fan {
			if k > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `{"name":"f%d","type":%s}`, k, build(i+1))
		}
		b.WriteString(`]}`)
		return b.String()
	}
	// Records need names, and every occurrence needs a DISTINCT one or the
	// parser rebinds them into the very sharing this oracle removes.
	out := build(0)
	n := 0
	for strings.Contains(out, `{"type":"record","fields":`) {
		out = strings.Replace(out, `{"type":"record","fields":`,
			fmt.Sprintf(`{"type":"record","name":"T%d","fields":`, n), 1)
		n++
	}
	return out
}

// ---------- triggers ----------

// minBytesTrigger names a container that demands a per-element wire minimum,
// which is the only way schemaMinBytes is entered. "none" is the control: it
// is a schema shape that reaches no caller, so a cell carrying it measures
// nothing, and the matrix asserts exactly that rather than leaving it to be
// rediscovered.
type minBytesTrigger struct {
	name  string
	wrap  func(inner string) string
	walks bool
}

var minBytesTriggers = []minBytesTrigger{
	{name: "array-items", walks: true, wrap: func(s string) string {
		return `{"type":"array","items":` + s + `}`
	}},
	{name: "map-values", walks: true, wrap: func(s string) string {
		return `{"type":"map","values":` + s + `}`
	}},
	{name: "array-in-record", walks: true, wrap: func(s string) string {
		return `{"type":"record","name":"Outer","fields":[{"name":"a","type":{"type":"array","items":` + s + `}}]}`
	}},
	{name: "bare-record", walks: false, wrap: func(s string) string { return s }},
}

// schemaAsksMinBytes reports whether the schema rooted at n contains an array
// or a map — the only shapes that make a caller derive a per-element wire
// minimum, and so the only way schemaMinBytes is entered at all. Memoized,
// because the graphs it walks are the same shared-node DAGs.
func schemaAsksMinBytes(n *schemaNode) bool {
	return asksMinBytesSeen(n, map[*schemaNode]bool{})
}

func asksMinBytesSeen(n *schemaNode, seen map[*schemaNode]bool) bool {
	if n == nil || seen[n] {
		return false
	}
	seen[n] = true
	switch n.kind {
	case "array", "map":
		return true
	case "union":
		for _, b := range n.branches {
			if asksMinBytesSeen(b, seen) {
				return true
			}
		}
	case "record", "error":
		for i := range n.fields {
			if asksMinBytesSeen(n.fields[i].node, seen) {
				return true
			}
		}
	}
	return false
}

// ---------- the cost pin ----------

// dagCostDepth / dagCostDepth3 are the fan-2 and fan-3 depths every cost cell
// uses. They are chosen so that a walk re-descending per reference is ~2^26
// descents — decisively past dosBudget, yet still finishing on its own so a
// failing cell does not leave a goroutine running for hours. A walk that
// visits each node once does these in microseconds, so the margin between the
// two verdicts is four orders of magnitude and no machine noise crosses it.
const (
	dagCostDepth  = 26
	dagCostDepth3 = 16
)

// TestInvariant_SharedSchemaNodeWalkedOnce pins that a node reachable by
// several paths costs what a node reachable by one path costs, across every
// shape that produces the sharing and every container that asks for the
// bound.
func TestInvariant_SharedSchemaNodeWalkedOnce(t *testing.T) {
	shapes := []struct {
		name  string
		build func(levels, fan int) string
		// A shape that supplies its own container does not take a wrapper.
		selfWrapped bool
	}{
		{name: "nested", build: dagNested},
		{name: "flat-forward-ref", build: dagFlat, selfWrapped: true},
		{name: "self-recursive", build: dagSelfRecursive},
		{name: "single-scc", build: dagSingleSCC, selfWrapped: true},
	}
	for _, sh := range shapes {
		for _, tr := range minBytesTriggers {
			if sh.selfWrapped && tr.name != "array-items" {
				continue // the shape already carries its trigger
			}
			for _, fan := range []int{2, 3} {
				name := fmt.Sprintf("%s/%s/fan%d", sh.name, tr.name, fan)
				t.Run(name, func(t *testing.T) {
					levels := dagCostDepth
					if fan == 3 {
						levels = dagCostDepth3
					}
					s := sh.build(levels, fan)
					if !sh.selfWrapped {
						s = tr.wrap(s)
					}
					parsed, err := Parse(s)
					if err != nil {
						t.Fatalf("parse: %v", err)
					}
					// The trigger claim, checked rather than asserted in a
					// comment: a cell whose schema contains no container
					// asking for a per-element minimum never enters the walk,
					// so it would measure nothing whatever the walk did.
					if got := schemaAsksMinBytes(parsed.node); got != tr.walks {
						t.Fatalf("trigger %q is registered as walks=%v but the parsed schema %s a container that asks for a per-element minimum",
							tr.name, tr.walks, map[bool]string{true: "contains", false: "does not contain"}[got])
					}
					wantTerminate(t, name, func() error {
						_, err := Parse(s)
						return err
					})
				})
			}
		}
	}
}

// TestInvariant_SharingDoesNotChangeMinBytes is the value oracle, and it is
// calibration-free: it never states what the minimum IS. Sharing a node is a
// property of how a schema is WRITTEN, so the same type written with
// references and written out in full must produce the same bound.
func TestInvariant_SharingDoesNotChangeMinBytes(t *testing.T) {
	for _, fan := range []int{2, 3} {
		maxLevels := 7
		if fan == 3 {
			maxLevels = 5
		}
		for levels := 1; levels <= maxLevels; levels++ {
			t.Run(fmt.Sprintf("fan%d/levels%d", fan, levels), func(t *testing.T) {
				dag, err := Parse(dagNested(levels, fan))
				if err != nil {
					t.Fatalf("parse dag: %v", err)
				}
				tree, err := Parse(treeExpanded(levels, fan))
				if err != nil {
					t.Fatalf("parse tree: %v", err)
				}
				got, want := schemaMinBytes(dag.node), schemaMinBytes(tree.node)
				if got != want {
					t.Errorf("shared-node form gives min %d, fully-expanded form gives %d; the two describe the same type",
						got, want)
				}
			})
		}
	}
}

// minBytesNoMemo is the walk written WITHOUT a memo: mark on entry, unmark on
// exit, recompute per reference. It is a transcription of the algorithm, not a
// copy of the production code — no memo, no low-water mark, no pending map —
// and it exists to be the oracle for which results the production walk is
// allowed to remember. Exponential by construction, so every corpus schema
// driven through it is small.
func minBytesNoMemo(n *schemaNode, path map[*schemaNode]bool) int {
	if n == nil {
		return 1
	}
	if path[n] {
		return 1
	}
	path[n] = true
	defer delete(path, n)
	switch n.kind {
	case "null":
		return 0
	case "boolean", "int", "long", "enum":
		return 1
	case "float":
		return 4
	case "double":
		return 8
	case "bytes", "string":
		return 1
	case "fixed":
		return saturateSchemaMagnitude(n.size)
	case "array", "map":
		return 1
	case "union":
		m, found := 0, false
		for _, b := range n.branches {
			if v := minBytesNoMemo(b, path); !found || v < m {
				m, found = v, true
			}
		}
		if !found {
			return 1
		}
		return saturateSchemaMagnitude(1 + m)
	case "record":
		var s int
		for i := range n.fields {
			s = saturateSchemaMagnitude(s + minBytesNoMemo(n.fields[i].node, path))
			if s == maxSchemaMagnitude {
				return maxSchemaMagnitude
			}
		}
		return s
	}
	return 1
}

// TestInvariant_DagMinBytesIsExactAtScale separates the two mechanisms that
// bound this walk, which no cost cell can tell apart.
//
// The visit budget bounds COST for every shape, including one the memo cannot
// help with, so with the memo removed the cost cells still pass — the budget
// stops the walk either way. What the budget cannot do is answer correctly:
// past it the walk stops deriving and falls back to a stand-in, so the bound
// comes back far too loose. The memo is what makes a schema of this size come
// back with the real number, and this is the cell that says so.
//
// The expected value is arithmetic on the schema's own definition rather than
// anything read off this package: every level has `fan` fields of the next
// level and the deepest level's fields are ints, so the minimum is fan^levels.
func TestInvariant_DagMinBytesIsExactAtScale(t *testing.T) {
	cases := []struct {
		levels, fan int
	}{
		{dagCostDepth, 2},
		{dagCostDepth3, 3},
		{10, 2},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("levels%d/fan%d", c.levels, c.fan), func(t *testing.T) {
			want := 1
			for range c.levels {
				want *= c.fan
			}
			if want >= maxSchemaMagnitude {
				t.Fatalf("cell is above the saturation ceiling, so it measures the clamp instead")
			}
			s, err := Parse(dagNested(c.levels, c.fan))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got := schemaMinBytes(s.node); got != want {
				t.Errorf("minimum is %d, want %d (%d fields per level, %d levels, ints at the bottom); "+
					"a walk that re-descends per reference cannot reach the bottom within its visit budget "+
					"and falls back to a stand-in, which is what a number below this one means",
					got, want, c.fan, c.levels)
			}
		})
	}
}

// TestInvariant_MemoAgreesWithUnmemoizedWalk is the oracle for WHICH results
// may be remembered, and it is the only one that can see the distinction.
//
// A back-edge does not return the referenced node's minimum; it returns a
// conservative stand-in, because that computation is still running. So a
// result reached through one is a property of the PATH, not of the node, and
// remembering it answers a later entry's question with an earlier entry's
// answer. Cost oracles cannot see that — a wrong memo is faster, not slower —
// and the fully-expanded twin cannot either, since these schemas are cyclic
// and have no finite expansion. What settles it is the walk with no memory at
// all: whatever it computes is by definition entry-independent, because it
// never carries anything between entries.
//
// The corpus is the shapes where the distinction exists: mutual recursion
// (a node reached from inside its own cycle AND from outside it), a node
// whose true minimum is BELOW the cycle stand-in (an all-null record, minimum
// zero, where remembering the stand-in would make the bound too TIGHT and
// reject real data), and the DAG shapes for the direction where no cycle is
// involved at all.
func TestInvariant_MemoAgreesWithUnmemoizedWalk(t *testing.T) {
	mutual := `{"type":"record","name":"R","fields":[` +
		`{"name":"a","type":{"type":"record","name":"A","fields":[` +
		`{"name":"f","type":{"type":"record","name":"X","fields":[` +
		`{"name":"back","type":"A"},{"name":"pad","type":"double"}]}},` +
		`{"name":"pad2","type":"double"}]}},` +
		`{"name":"x1","type":"X"},{"name":"x2","type":"X"}]}`

	// Z's minimum is 0, BELOW the stand-in a back-edge returns, so a result
	// remembered from inside the cycle would be too large — the direction
	// that turns a loose bound into a refusal of real wire bytes.
	zeroInCycle := `{"type":"record","name":"R","fields":[` +
		`{"name":"c","type":{"type":"record","name":"C","fields":[` +
		`{"name":"z","type":{"type":"record","name":"Z","fields":[` +
		`{"name":"self","type":["null","C"]}]}},` +
		`{"name":"back","type":["null","C"]}]}},` +
		`{"name":"z2","type":"Z"}]}`

	corpus := []struct{ name, schema string }{
		{"mutual-recursion", mutual},
		{"zero-minimum-inside-cycle", zeroInCycle},
		{"dag-nested", dagNested(6, 2)},
		{"dag-nested-fan3", dagNested(4, 3)},
		{"dag-self-recursive", dagSelfRecursive(5, 2)},
		{"dag-single-scc", dagSingleSCC(5, 2)},
		{"dag-flat", dagFlat(5, 2)},
		{"all-null-record", `{"type":"record","name":"N","fields":[{"name":"a","type":"null"},{"name":"b","type":"null"}]}`},
	}
	for _, c := range corpus {
		t.Run(c.name, func(t *testing.T) {
			s, err := Parse(c.schema)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			var walk func(n *schemaNode, seen map[*schemaNode]bool)
			walk = func(n *schemaNode, seen map[*schemaNode]bool) {
				if n == nil || seen[n] {
					return
				}
				seen[n] = true
				got := schemaMinBytes(n)
				want := minBytesNoMemo(n, make(map[*schemaNode]bool))
				if got != want {
					t.Errorf("node %q: memoized walk says %d, a walk with no memory at all says %d — "+
						"the memo is reusing a result that was computed for a different entry",
						n.kind, got, want)
				}
				walk(n.items, seen)
				walk(n.values, seen)
				for _, b := range n.branches {
					walk(b, seen)
				}
				for i := range n.fields {
					walk(n.fields[i].node, seen)
				}
			}
			// Every node in the schema is its own entry point, which is the
			// whole question: the walk must give one answer per node, not one
			// answer per route to it.
			walk(s.node, make(map[*schemaNode]bool))
		})
	}
}

// TestInvariant_MinBytesSelfReadable is the second calibration-free oracle:
// the bound may never refuse wire bytes this package's own encoder produced.
// A memo that recorded a value derived through a cycle back-edge would
// tighten the bound for some entry points and this is what would catch it.
func TestInvariant_MinBytesSelfReadable(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		val    any
	}{
		{"array-of-dag", `{"type":"array","items":` + dagNested(3, 2) + `}`, nil},
		{"map-of-dag", `{"type":"map","values":` + dagNested(3, 2) + `}`, nil},
		{"array-of-selfrec", `{"type":"array","items":` + dagSelfRecursive(3, 2) + `}`, nil},
		{"array-of-null", `{"type":"array","items":"null"}`, []any{nil, nil, nil}},
		{"array-of-empty-record", `{"type":"array","items":{"type":"record","name":"E","fields":[]}}`, []any{map[string]any{}, map[string]any{}}},
		{"map-of-null", `{"type":"map","values":"null"}`, map[string]any{"a": nil, "b": nil}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := Parse(c.schema)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			val := c.val
			if val == nil {
				val = buildZeroValue(t, s.node)
			}
			b, err := s.Encode(val)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			var out any
			if _, err := s.Decode(b, &out); err != nil {
				t.Errorf("this package encoded %d bytes its own bound then refused: %v", len(b), err)
			}
		})
	}
}

// buildZeroValue produces one legal value for node, used only to feed the
// self-readability oracle above.
func buildZeroValue(t *testing.T, n *schemaNode) any {
	t.Helper()
	switch n.kind {
	case "null":
		return nil
	case "boolean":
		return false
	case "int", "long":
		return 0
	case "float", "double":
		return 0.0
	case "string", "bytes":
		return ""
	case "array":
		return []any{buildZeroValue(t, n.items), buildZeroValue(t, n.items)}
	case "map":
		return map[string]any{"k": buildZeroValue(t, n.values)}
	case "union":
		return buildZeroValue(t, n.branches[0])
	case "record", "error":
		m := make(map[string]any, len(n.fields))
		for i := range n.fields {
			m[n.fields[i].name] = buildZeroValue(t, n.fields[i].node)
		}
		return m
	}
	t.Fatalf("no zero value for kind %q", n.kind)
	return nil
}

// ---------- the carrier enumeration ----------

// minBytesCallSite is one place in source that asks for a per-element wire
// minimum. The set is DERIVED by TestInvariant_MinBytesCallSites scanning
// for the call, not read off this table: the table only supplies the reason
// and the entry point, and the guard fails when the two disagree in either
// direction.
type minBytesCallSite struct {
	file  string
	count int
	entry string // the public call that reaches it
	why   string
}

var minBytesCallSites = []minBytesCallSite{
	{file: "schema.go", count: 4, entry: "Parse / MustParse / SchemaCache.Parse / SchemaFor",
		why: "the parse-time derivations: an array's minItemBytes and its fieldMeta twin, a map's minEntryBytes, and the container fixup that re-derives both once a forward reference resolves"},
	{file: "resolve.go", count: 2, entry: "Resolve, and ocf.NewReader when the file's schema differs from the reader's",
		why: "the resolver rebuilds the bound against the WRITER's wire format for a resolved array and map"},
	{file: "skip.go", count: 2, entry: "Resolve when a writer field is dropped",
		why: "the skip compiled for a dropped writer field derives the same two bounds"},
	{file: "deser.go", count: 3, entry: "n/a — not callers",
		why: "the definition itself plus two doc references to it: schemaMinBytes' own body " +
			"delegating to the memoized form, and deserMap's field comment naming where its bound comes from"},
}

// TestInvariant_MinBytesCallSites derives the set of sites that demand a
// per-element minimum and requires every one to be rowed with the entry
// point that reaches it. This is what keeps the cost matrix honest: a cell
// whose schema reaches no site on this list measures nothing at all, and a
// NEW site added without a row would be a new entry point no cost cell
// drives.
func TestInvariant_MinBytesCallSites(t *testing.T) {
	files := censusSourceFiles(t)
	found := occurrences(t, files, "schemaMinBytes(")
	rowed := make(map[string]minBytesCallSite, len(minBytesCallSites))
	for _, r := range minBytesCallSites {
		rowed[r.file] = r
	}
	for file, lines := range found {
		r, ok := rowed[file]
		if !ok {
			t.Errorf("schemaMinBytes is called in UNROWED file %s (lines %v).\n  A new caller is a new entry point into the shared-node walk; row it with the public call that reaches it, and give the cost matrix a cell that drives that entry point.",
				file, lines)
			continue
		}
		if len(lines) != r.count {
			t.Errorf("schemaMinBytes is called %d times in %s (lines %v), the table says %d.\n  If a call was ADDED, name its entry point; if one was REMOVED, bring the count down or this row guards code that is gone.",
				len(lines), file, lines, r.count)
		}
	}
	for _, r := range minBytesCallSites {
		if len(found[r.file]) == 0 {
			t.Errorf("%s is rowed with %d calls to schemaMinBytes but has none — the row has rotted", r.file, r.count)
		}
	}
	// The control cell is a claim about source, so check it against source:
	// no caller lives in a file that compiles plain records, which is why a
	// bare-record DAG never enters the walk.
	if len(found["json_codec.go"]) != 0 || len(found["json_decode.go"]) != 0 {
		t.Errorf("a JSON path now derives a per-element minimum; the cost matrix drives only the binary triggers and needs a JSON cell")
	}
}

// TestInvariant_DagCostMatrixDrivesEveryEntryPoint crosses the derived
// carrier set with the matrix's own cells, so an entry point that gains a
// caller cannot stay undriven. It reads this file rather than trusting a
// count: the failure it prevents is a row added to minBytesCallSites with no
// cell behind it.
func TestInvariant_DagCostMatrixDrivesEveryEntryPoint(t *testing.T) {
	src, err := os.ReadFile("schema_dag_cost_test.go")
	if err != nil {
		t.Fatalf("reading this file: %v", err)
	}
	body := string(src)
	for _, want := range []string{
		"Parse(dag)",     // the parse-time derivation
		"cache.Parse(",   // the cache's own parse
		"Resolve(wDrop,", // the skip compiled for a dropped writer field
		"Resolve(wKeep,", // the resolver's rebuild of a kept container
		"ocf-header",     // the container reader's, schema read from the file
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the cost matrix no longer drives %q; every rowed entry point needs a cell", want)
		}
	}
}

// TestInvariant_EveryMinBytesEntryPointIsBounded drives each rowed entry
// point with the same DAG, so no single caller can regress on its own. The
// ocf cell is the one where the schema is supplied by the INPUT rather than
// by the caller, which is what sets this class's severity.
func TestInvariant_EveryMinBytesEntryPointIsBounded(t *testing.T) {
	dag := `{"type":"array","items":` + dagNested(dagCostDepth, 2) + `}`

	wantTerminate(t, "Parse", func() error {
		_, err := Parse(dag)
		return err
	})

	wantTerminate(t, "SchemaCache.Parse", func() error {
		var cache SchemaCache
		// The cache memoizes by TEXT, so it saves a REPEATED parse and
		// nothing at all on the first one; this cell drives the first.
		_, err := cache.Parse(dag)
		return err
	})

	wDrop := MustParse(fmt.Sprintf(
		`{"type":"record","name":"Top","fields":[{"name":"x","type":%s},{"name":"y","type":"int"}]}`, dag))
	rDrop := MustParse(`{"type":"record","name":"Top","fields":[{"name":"y","type":"int"}]}`)
	wantTerminate(t, "Resolve/dropped-field-skip", func() error {
		_, err := Resolve(wDrop, rDrop)
		return err
	})

	wKeep := MustParse(fmt.Sprintf(
		`{"type":"record","name":"Top","fields":[{"name":"x","type":%s}]}`, dag))
	rKeep := MustParse(fmt.Sprintf(
		`{"type":"record","name":"Top","fields":[{"name":"x","type":%s},{"name":"z","type":"int","default":0}]}`, dag))
	wantTerminate(t, "Resolve/kept-field", func() error {
		_, err := Resolve(wKeep, rKeep)
		return err
	})

	// ocf-header: the container reader derives the bound from a schema it
	// read out of the file, so the cost is driven by the input rather than
	// by the caller. The ocf package cannot be imported from package avro,
	// so the executable cell lives in ocf/dos_battery_test.go; this cell
	// pins the parse of the identical header schema that reaches it.
	wantTerminate(t, "ocf-header/schema-parse", func() error {
		_, err := Parse(dag)
		return err
	})
}

// ---------- the WIDTH axis ----------

// dagWideSCC crosses the cyclic shapes with the axis the other cost cells
// hold constant: how many children ONE node has.
//
// dagSingleSCC and dagSelfRecursive fix fan at 2 or 3 because fan is what
// drives DEPTH — the number of distinct root-to-leaf paths — so every cyclic
// cell measures the visit allowance times two. But the work a single node
// costs is its own child count, and that is a SECOND number the schema author
// picks independently. This shape separates them: the chain stays fan-narrow
// so the path count still exhausts the allowance, and the record every path
// ENDS at carries `width` extra fields, so each recomputation of that one
// record pays `width`.
//
// Three properties decide whether this shape measures anything at all, and
// each was MEASURED at a matched text size of ~124 KB rather than reasoned
// about — the whole point being that a plausible-looking variant of this
// schema costs milliseconds and proves the opposite of what it looks like:
//
//   - CYCLIC, and that is the enabling one: the wide record closes back to L0,
//     so every node in the chain is in one strongly-connected component and
//     nothing is memoizable. Point that back-edge at "int" instead and the
//     same 124 KB parses in 10.7 ms rather than 6.5 s — 600x — because the
//     memo then answers each node once and there is no repetition for the
//     width to multiply.
//   - CONCENTRATED, and that is the dominant one: a node is recomputed once
//     per path that reaches it, so revisits are highest at the node every
//     root-to-leaf path ENDS at. Putting the whole width there is what makes
//     the most-revisited node also the widest. Spreading the same total width
//     evenly over the levels instead costs 435 ms against 6.5 s — 15x — since
//     the allowance is spent on computations, and spreading width over D
//     levels makes each computation cost width/D.
//   - ZERO-MINIMUM fillers (`null`), worth 3x on top of the other two (6.5 s
//     vs 2.2 s for `double`). Not, as it first appears, because a wide record
//     saturates its own running sum — 4000 doubles is 32000, far under the
//     ceiling — but because the CHAIN above it doubles that figure per level
//     and reaches the ceiling a dozen levels up, and a saturated sum returns
//     EARLY, before the field that continues the fan-out. Nulls keep every
//     level's minimum small enough that no level short-circuits.
func dagWideSCC(levels, fan, width int) string {
	var wide strings.Builder
	wide.WriteString(`{"type":"record","name":"W","fields":[{"name":"back","type":"L0"}`)
	for k := range width {
		fmt.Fprintf(&wide, `,{"name":"p%d","type":"null"}`, k)
	}
	wide.WriteString(`]}`)

	inner := wide.String()
	for i := levels - 1; i >= 0; i-- {
		next := fmt.Sprintf("L%d", i+1)
		if i == levels-1 {
			next = "W"
		}
		var b strings.Builder
		fmt.Fprintf(&b, `{"type":"record","name":"L%d","fields":[{"name":"f0","type":%s}`, i, inner)
		for k := 1; k < fan; k++ {
			fmt.Fprintf(&b, `,{"name":"f%d","type":"%s"}`, k, next)
		}
		b.WriteString(`]}`)
		inner = b.String()
	}
	return inner
}

// dagWideLevels / dagWideWidth are the width cell's two magnitudes. Levels is
// chosen so the path count reaches the walk's allowance (a narrower chain
// never exhausts it and the width has nothing to multiply); width is chosen so
// that allowance x width is decisively past dosBudget while the schema text
// stays a few hundred KB and the walk still finishes on its own.
const (
	dagWideLevels = 16
	dagWideWidth  = 8000
)

// TestInvariant_CyclicWalkCostIsBoundedByWork is the WIDTH half of the cost
// guard: an allowance spent per NODE ENTERED bounds how many nodes are
// entered, not how much work they do, so a cap counting entries is bounded
// only when every entry costs the same. Here they do not — a record's entry
// iterates its own fields — and both factors are chosen by whoever wrote the
// schema, so the guard has to be charged in the unit of the work.
func TestInvariant_CyclicWalkCostIsBoundedByWork(t *testing.T) {
	for _, tr := range minBytesTriggers {
		t.Run(tr.name, func(t *testing.T) {
			s := tr.wrap(dagWideSCC(dagWideLevels, 2, dagWideWidth))
			parsed, err := Parse(s)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			// Same trigger claim the depth cells check: a shape that reaches
			// no caller of the walk measures nothing whatever the walk does.
			if got := schemaAsksMinBytes(parsed.node); got != tr.walks {
				t.Fatalf("trigger %q is registered as walks=%v but the parsed schema %s a container that asks for a per-element minimum",
					tr.name, tr.walks, map[bool]string{true: "contains", false: "does not contain"}[got])
			}
			wantTerminate(t, "Parse/wide-scc/"+tr.name, func() error {
				_, err := Parse(s)
				return err
			})
		})
	}
}

// TestInvariant_WideCyclicWalkReachesEveryEntryPoint drives the same shape
// through the entry points that do not take the schema from the caller.
func TestInvariant_WideCyclicWalkReachesEveryEntryPoint(t *testing.T) {
	inner := dagWideSCC(dagWideLevels, 2, dagWideWidth)
	s := `{"type":"array","items":` + inner + `}`

	wantTerminate(t, "SchemaCache.Parse/wide-scc", func() error {
		var c SchemaCache
		_, err := c.Parse(s)
		return err
	})
	parsed := MustParse(s)
	wantTerminate(t, "Resolve/wide-scc", func() error {
		_, err := Resolve(parsed, parsed)
		return err
	})
	// The writer field is DROPPED, which compiles a skip — a separate
	// derivation of the same per-element bound.
	w := MustParse(`{"type":"record","name":"T","fields":[{"name":"x","type":` + s + `},{"name":"y","type":"int"}]}`)
	r := MustParse(`{"type":"record","name":"T","fields":[{"name":"y","type":"int"}]}`)
	wantTerminate(t, "Resolve/wide-scc-dropped-field", func() error {
		_, err := Resolve(w, r)
		return err
	})
	// ocf-header: the executable cell lives in ocf/dos_battery_test.go
	// (package avro cannot import ocf); this pins the parse that reaches it.
	wantTerminate(t, "ocf-header/wide-scc-schema-parse", func() error {
		_, err := Parse(s)
		return err
	})
}

// TestInvariant_MinBytesChargeCoversEveryChildArm derives the charge's set
// from source instead of trusting the two switches to stay in step.
//
// The allowance is charged by the PARENT, for the whole child list, before it
// descends — so every child examination is paid for exactly once by whoever
// performs it. That accounting is complete only while minBytesChildren counts
// the children of exactly the kinds minBytesFromChildren descends into. They
// are two switches over the same vocabulary, and a kind added to one alone is
// silent: an unaccounted arm restores the unbounded product, and an
// over-counted one spends the allowance on descents that never happen. So the
// arms are read out of the source and compared rather than reviewed.
func TestInvariant_MinBytesChargeCoversEveryChildArm(t *testing.T) {
	src, err := os.ReadFile("deser.go")
	if err != nil {
		t.Fatalf("reading deser.go: %v", err)
	}
	body := func(sig string) string {
		i := strings.Index(string(src), sig)
		if i < 0 {
			t.Fatalf("%q not found in deser.go — the guard is aimed at a function that no longer exists", sig)
		}
		rest := string(src)[i:]
		if j := strings.Index(rest[1:], "\nfunc "); j >= 0 {
			rest = rest[:j+1]
		}
		return rest
	}
	// caseKinds returns the quoted kind labels of every `case` arm in s for
	// which keep reports true of the arm's body.
	caseKinds := func(s string, keep func(arm string) bool) map[string]bool {
		out := map[string]bool{}
		parts := strings.Split(s, "\n\tcase ")
		for _, p := range parts[1:] {
			head, arm, _ := strings.Cut(p, ":")
			if !keep(arm) {
				continue
			}
			for _, lit := range strings.Split(head, ",") {
				out[strings.Trim(strings.TrimSpace(lit), `"`)] = true
			}
		}
		return out
	}
	charged := caseKinds(body("func minBytesChildren("), func(string) bool { return true })
	descends := caseKinds(body("func (w *minBytesWalk) minBytesFromChildren("),
		func(arm string) bool { return strings.Contains(arm, "w.minBytes(") })

	if len(charged) == 0 || len(descends) == 0 {
		t.Fatalf("extracted no arms (charged=%v descends=%v) — the guard cannot see what it is guarding", charged, descends)
	}
	for k := range descends {
		if !charged[k] {
			t.Errorf("minBytesFromChildren descends into %q's children but minBytesChildren does not count them: "+
				"entering such a node costs its child count and is charged as if it cost one, "+
				"which is the unbounded product this allowance exists to prevent", k)
		}
	}
	for k := range charged {
		if !descends[k] {
			t.Errorf("minBytesChildren counts %q's children but minBytesFromChildren never descends into them: "+
				"the allowance is spent on work that does not happen, tightening the bound for no reason", k)
		}
	}
}

// TestInvariant_MetadataWalkChargesPerChild is the measured half of an
// immunity claim rather than a read of it. The SchemaNode->JSON walk carries
// its own allowance, and the reason it has no width residue is structural: it
// charges takeNode at the TOP of every entry, ahead of the cycle and dedup
// checks that can return early, so a child costs a unit whether or not the
// walk descends through it. The min-bytes walk charged AFTER its memo, which
// is exactly how a memo hit could examine a child for free.
//
// A claim like that is worth no more than the probe behind it, so the same
// wide cyclic shape that took the min-bytes walk past the budget is driven
// through the metadata surfaces here.
func TestInvariant_MetadataWalkChargesPerChild(t *testing.T) {
	s := MustParse(`{"type":"array","items":` + dagWideSCC(dagWideLevels, 2, dagWideWidth) + `}`)
	wantTerminate(t, "Root+Schema/wide-scc", func() error {
		root := s.Root()
		_, _ = root.Schema()
		return nil
	})
	wantTerminate(t, "String+Canonical/wide-scc", func() error {
		_ = s.String()
		_ = s.Canonical()
		return nil
	})
}
