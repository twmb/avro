package avro

import (
	"fmt"
	"testing"
)

// The per-value name lookups, and the two things that can go wrong with them.
//
// A union's tag table and an enum's symbol index are the tier rule and the
// symbol list applied ONCE, at parse time, so both encoders and the JSON
// decoder can answer "which branch / which ordinal is this name?" without
// walking the siblings per value. Two failures follow from that shape:
//
//   - the table ANSWERS DIFFERENTLY from the walk it stands in for, which is a
//     correctness change wearing a performance change's clothes; and
//   - a node that carries the sibling slice does NOT carry its table, which is
//     silent — every consumer still gets the right answer, by scanning, and
//     only the cost tells you. Nodes SYNTHESIZED during resolution are where
//     that bites, because they copy the reader's siblings by reference while
//     the table is a separate field.
//
// The first is checked by asking both; the second by walking every node a parse
// or a resolve can produce and refusing a bare one.

// tagProbeNames returns every name worth asking a union about: each branch's
// own spelling in each tier's form, plus names nothing can claim.
func tagProbeNames(u *schemaNode) []string {
	names := []string{"", "nope", "x.nope", "int", "null", "long.timestamp-millis", "fixed.uuid", "bytes.decimal"}
	for _, b := range u.branches {
		if b == nil {
			continue
		}
		names = append(names, b.kind, b.name, unqualified(b.name))
		if b.logical != "" {
			names = append(names, b.kind+"."+b.logical, unqualified(b.name)+"."+b.logical)
		}
		names = append(names, b.aliases...)
		names = append(names, b.bareAliases...)
	}
	return names
}

// unionTagCorpus spans the shapes the tier walk distinguishes: plain kinds, a
// logical qualifier a sibling's exact name owns, namespaced names whose short
// forms collide, forward references (whose table is rebuilt at finalize, so it
// is the shape that can go stale), and recursion.
func unionTagCorpus() []string {
	return []string{
		`["null","int","string"]`,
		`["null",{"type":"long","logicalType":"timestamp-millis"}]`,
		// The qualifier a logical branch would emit is also a legal fullname,
		// so this pair puts the two spellings in one namespace. "decimal" is
		// the collision that can be WRITTEN — a hyphenated logical type has no
		// valid name spelling, so no fixed can claim its qualifier.
		`[{"type":"bytes","logicalType":"decimal","precision":4,"scale":2},{"type":"fixed","name":"bytes.decimal","size":2}]`,
		`[{"type":"fixed","name":"a.F","size":16,"logicalType":"uuid"},{"type":"fixed","name":"b.G","size":16,"logicalType":"uuid"}]`,
		`[{"type":"record","name":"a.R","fields":[]},{"type":"record","name":"b.R","fields":[]}]`,
		`[{"type":"record","name":"a.Q","aliases":["a.R","R"],"fields":[]}]`,
		`[{"type":"enum","name":"E","symbols":["A","B"]},"string"]`,
		`["null",{"type":"map","values":"int"},{"type":"array","items":"int"}]`,
		`[]`,
		// Forward reference: buildUnion tables it under the as-written name and
		// finalizeUnionNames rebuilds over the resolved node. The table the
		// consumers hold has to be the rebuilt one.
		`{"type":"record","name":"Top","fields":[
			{"name":"a","type":["null","Inner"]},
			{"name":"b","type":{"type":"record","name":"Inner","fields":[{"name":"q","type":"int"}]}}]}`,
		// Recursive: the union's branch is the enclosing record.
		`{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]}]}`,
		// An enum wide enough to cross the index threshold, and one below it,
		// so both arms of the symbol lookup are driven.
		wideEnumText("Wide", enumIndexMin+4, ""),
		wideEnumText("Narrow", 2, ""),
	}
}

func wideEnumText(name string, n int, extra string) string {
	out := `{"type":"enum","name":"` + name + `","symbols":[`
	for i := range n {
		if i > 0 {
			out += ","
		}
		out += fmt.Sprintf(`"S%d"`, i)
	}
	return out + `]` + extra + `}`
}

// TestInvariant_UnionTagTableMatchesTheTierWalk asks the table and the walk the
// same names and requires the same branch. The walk is the rule; the table is
// the rule precomputed, and precomputing it may not change what it accepts.
func TestInvariant_UnionTagTableMatchesTheTierWalk(t *testing.T) {
	unions, probes := 0, 0
	for _, text := range unionTagCorpus() {
		s, err := Parse(text)
		if err != nil {
			t.Errorf("corpus entry does not parse, so it drives nothing: %v\n  %s", err, text)
			continue
		}
		forEachSchemaNode(s.node, func(n *schemaNode) {
			if n.kind != "union" {
				return
			}
			unions++
			if n.tags == nil {
				t.Errorf("%s: a parsed union node has no tag table", text)
				return
			}
			for _, name := range tagProbeNames(n) {
				probes++
				want := scanUnionBranch(n, name)
				if got := findUnionBranch(n, name); got != want {
					t.Errorf("%s: name %q\n  tier walk -> %s\n  table     -> %s",
						text, name, nodeDesc(want), nodeDesc(got))
				}
			}
		})
	}
	if unions == 0 || probes == 0 {
		t.Fatalf("the corpus drove %d unions and %d probes; it is not exercising the table", unions, probes)
	}
	t.Logf("unions=%d probes=%d", unions, probes)
}

// TestInvariant_EnumSymbolIndexMatchesTheScan is the same claim for the enum
// half: the index and the symbol slice must name the same ordinal, on both
// sides of the size threshold that decides whether an index exists at all.
func TestInvariant_EnumSymbolIndexMatchesTheScan(t *testing.T) {
	enums, probes := 0, 0
	for _, text := range unionTagCorpus() {
		s, err := Parse(text)
		if err != nil {
			t.Errorf("corpus entry does not parse: %v\n  %s", err, text)
			continue
		}
		forEachSchemaNode(s.node, func(n *schemaNode) {
			if n.kind != "enum" {
				return
			}
			enums++
			for i, sym := range append(append([]string{}, n.symbols...), "nope", "") {
				probes++
				gotIdx, gotOK := n.symbolIndex(sym)
				wantIdx, wantOK := -1, false
				for j, s := range n.symbols {
					if s == sym {
						wantIdx, wantOK = j, true
						break
					}
				}
				if gotOK != wantOK || (wantOK && gotIdx != wantIdx) {
					t.Errorf("%s: symbol %q (probe %d): index says (%d,%v), the symbol slice says (%d,%v)",
						text, sym, i, gotIdx, gotOK, wantIdx, wantOK)
				}
			}
		})
	}
	if enums == 0 {
		t.Fatal("the corpus drove no enum node")
	}
	t.Logf("enums=%d probes=%d", enums, probes)
}

// resolveSynthesizedNode runs resolution at the NODE level, which is the only
// place the synthesized nodes are observable.
//
// Resolve returns a Schema whose node field is the READER's node and keeps only
// the resolved tree's deser closure (resolve.go). A check that walks Resolve's
// result therefore walks the PARSE's output: it would pass with every resolved
// node built bare, which is what a neuter of the carry proved before this
// helper existed. Driving resolveNode directly is what makes the assertion
// below about the nodes it names.
func resolveSynthesizedNode(t *testing.T, writer, reader *Schema) *schemaNode {
	t.Helper()
	ctx := &resolveCtx{seen: make(map[nodePair]*schemaNode), custom: reader.custom}
	nd, err := resolveNode(reader.node, writer.node, "", ctx)
	if err != nil {
		t.Fatalf("resolveNode: %v", err)
	}
	if nd == reader.node {
		t.Fatalf("resolution returned the reader's own node, so nothing was synthesized and this case checks the parse path")
	}
	return nd
}

// resolvedNodeCases are writer/reader pairs chosen so resolution SYNTHESIZES a
// union or enum node on each of its paths — union-vs-union, union-vs-non-union,
// and a symbol-remapping enum. Those nodes carry the reader's siblings, so they
// must carry the reader's tables too.
var resolvedNodeCases = []struct{ name, writer, reader string }{
	{
		"union writer, union reader",
		`["null","int"]`,
		`["null","long","string"]`,
	},
	{
		"non-union writer, union reader",
		`"int"`,
		`["null","long"]`,
	},
	{
		"union of named types both sides",
		`[{"type":"record","name":"a.R","fields":[{"name":"q","type":"int"}]}]`,
		`["null",{"type":"record","name":"a.R","fields":[{"name":"q","type":"long"}]}]`,
	},
	{
		"enum with a writer symbol the reader defaults",
		`{"type":"enum","name":"E","symbols":["A","B","C"]}`,
		`{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}`,
	},
	{
		"wide enum past the index threshold, resolved",
		wideEnumText("E", enumIndexMin+4, ""),
		wideEnumText("E", enumIndexMin+6, `,"default":"S0"`),
	},
	{
		"record field carrying a union, resolved",
		`{"type":"record","name":"R","fields":[{"name":"u","type":["null","int"]}]}`,
		`{"type":"record","name":"R","fields":[{"name":"u","type":["null","long","string"]}]}`,
	},
}

// TestInvariant_EveryUnionNodeCarriesItsTagTable is the ownership half. A node
// that holds the sibling slice but not the lookup sends every consumer back to
// scanning it — correct, and linear in a count the schema's author chooses.
// Resolution builds fresh nodes around the reader's slices, which is exactly
// where a table can be left behind.
func TestInvariant_EveryUnionNodeCarriesItsTagTable(t *testing.T) {
	check := func(t *testing.T, label string, root *schemaNode) {
		t.Helper()
		unions, enums := 0, 0
		forEachSchemaNode(root, func(n *schemaNode) {
			switch n.kind {
			case "union":
				unions++
				if n.tags == nil {
					t.Errorf("%s: union node carries %d branches and no tag table — every consumer falls back to the tier walk, once per value",
						label, len(n.branches))
					return
				}
				// Not merely present: the SAME answers a fresh build gives. A
				// stale table (one finalize never rebuilt) or one copied from a
				// different union is present and wrong.
				fresh := new(unionTags)
				std := unionStandardNames(n.branches)
				log := make([]string, len(n.branches))
				for i, b := range n.branches {
					if b != nil {
						_, log[i] = unionBranchNames(b)
					}
				}
				fillUnionTagTables(fresh, new(deserUnion), n.branches, std, log)
				if len(fresh.byName) != len(n.tags.byName) {
					t.Errorf("%s: union tag table holds %d names, a fresh build holds %d — the table is stale",
						label, len(n.tags.byName), len(fresh.byName))
					return
				}
				for name, idx := range fresh.byName {
					if got, ok := n.tags.byName[name]; !ok || got != idx {
						t.Errorf("%s: tag %q resolves to branch %d, a fresh build says %d (present=%v)",
							label, name, got, idx, ok)
					}
				}
			case "enum":
				enums++
				want := enumSymbolIndex(n.symbols)
				if (want == nil) != (n.symbolIdx == nil) {
					t.Errorf("%s: enum %q has %d symbols; symbolIdx present=%v, want present=%v (threshold %d)",
						label, n.name, len(n.symbols), n.symbolIdx != nil, want != nil, enumIndexMin)
					return
				}
				for sym, idx := range want {
					if got, ok := n.symbolIdx[sym]; !ok || got != idx {
						t.Errorf("%s: enum %q symbol %q -> %d, want %d (present=%v)", label, n.name, sym, got, idx, ok)
					}
				}
			}
		})
		if unions+enums == 0 {
			t.Errorf("%s: walked no union or enum node at all — the case is not reaching what it claims to check", label)
		}
	}

	for _, text := range unionTagCorpus() {
		s, err := Parse(text)
		if err != nil {
			t.Errorf("corpus entry does not parse: %v\n  %s", err, text)
			continue
		}
		check(t, "parsed "+text, s.node)
	}
	for _, tc := range resolvedNodeCases {
		t.Run(tc.name, func(t *testing.T) {
			w, r := MustParse(tc.writer), MustParse(tc.reader)
			if err := CheckCompatibility(w, r); err != nil {
				t.Fatalf("the pair does not resolve, so the case drives nothing: %v", err)
			}
			check(t, "resolved "+tc.name, resolveSynthesizedNode(t, w, r))
		})
	}
}

// TestRegression_ResolvedUnionCarriesTheReaderTagTable pins the same claim on
// one shape: the node resolveUnionUnion builds around the reader's branch
// slice. The assertion is on the TABLE rather than on a decoded value because
// both answers are identical either way — only the cost differs, and a value
// assertion cannot see that.
//
// The carry is defense in depth TODAY: Resolve keeps the resolved tree's deser
// and returns the reader's node, so no walker currently reaches this node, and
// the JSON path on a resolved schema decodes against the WRITER's parsed node.
// It is pinned anyway because the invariant a consumer will rely on is "a node
// that carries siblings carries their table" — a synthesized node that holds
// the slice and not the lookup is a scan waiting for its first reader.
func TestRegression_ResolvedUnionCarriesTheReaderTagTable(t *testing.T) {
	w := MustParse(`[{"type":"record","name":"a.R","fields":[{"name":"q","type":"int"}]},"null"]`)
	r := MustParse(`["null",{"type":"record","name":"a.R","fields":[{"name":"q","type":"long"}]},"string"]`)
	n := resolveSynthesizedNode(t, w, r)
	if n.kind != "union" {
		t.Fatalf("resolution produced a %q node, want a union — the probe is not reaching the synthesized node", n.kind)
	}
	if n.tags == nil {
		t.Fatal("the resolved union carries the reader's branches without the reader's tag table")
	}
	// The table addresses the slice the node holds, so a name must land on the
	// branch that slice has at that index.
	idx, ok := n.tags.byName["a.R"]
	if !ok {
		t.Fatal(`the resolved union's table does not resolve "a.R"`)
	}
	if idx < 0 || idx >= len(n.branches) {
		t.Fatalf("table index %d is out of range for %d branches — the table belongs to a different union", idx, len(n.branches))
	}
	if got := n.branches[idx]; got == nil || got.name != "a.R" {
		t.Fatalf(`table sent "a.R" to branch %d, which is %s`, idx, nodeDesc(got))
	}
	if findUnionBranch(n, "a.R") != scanUnionBranch(n, "a.R") {
		t.Error("the resolved union's table and the tier walk disagree")
	}
}

// forEachSchemaNode visits every schemaNode reachable from root exactly once.
// Recursive schemas point back at themselves, so the visited set is what makes
// this terminate rather than a depth bound.
func forEachSchemaNode(root *schemaNode, fn func(*schemaNode)) {
	seen := map[*schemaNode]bool{}
	var walk func(*schemaNode)
	walk = func(n *schemaNode) {
		if n == nil || seen[n] {
			return
		}
		seen[n] = true
		fn(n)
		walk(n.items)
		walk(n.values)
		for _, b := range n.branches {
			walk(b)
		}
		for i := range n.fields {
			walk(n.fields[i].node)
		}
	}
	walk(root)
}
