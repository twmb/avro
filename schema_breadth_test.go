package avro

// Schema BREADTH — cost against the number of SIBLINGS a schema declares.
//
// A schema's size grows two ways: it nests deeper, or it declares more
// siblings at one level. Depth is bounded by an explicit pre-scan and pinned
// by the deep-schema cost tests. Breadth has no cap and needs none — a union
// of 20000 named branches, or a record of 20000 fields, is legal Avro that a
// schema registry, an RPC handshake, or an OCF file header can hand a reader.
// What it does need is for every pass over those siblings to stay LINEAR in
// their count, because a pass that scans the sibling list once per sibling
// turns an O(n) input into O(n^2) work.
//
// The bounds here are absolute wall-clock, not ratios: a ratio between two
// sizes is noise-sensitive on a loaded host, while the two complexity classes
// these tests separate are orders of magnitude apart at the sizes driven. Each
// bound sits far above the linear cost and far below the quadratic one.

import (
	"bytes"
	"crypto"
	_ "crypto/sha256"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

// breadthN is the sibling count every cell drives. It is chosen so that a
// quadratic pass takes seconds while a linear one takes tens of milliseconds,
// leaving room for a loaded host and for the race detector's instrumentation
// without either class being mistaken for the other.
const breadthN = 20000

// breadthBound is the per-cell ceiling. wantAcceptUnder raises it under -race.
const breadthBound = 500 * time.Millisecond

// breadthParseBound is the ceiling for the two cells that parse the schema TEXT
// rather than walking an already-parsed tree.
//
// A bound only separates linear from quadratic if it sits far above the linear
// cost, and those two do not qualify at breadthBound: a 20000-branch union is
// close to a megabyte of JSON, and parsing it is ~140ms (Parse) and ~300ms
// (SchemaCache.Parse) of legitimate, measured-linear work — doubling the branch
// count doubles both (x2.03/x2.04/x2.03 and x1.98/x1.87/x2.34 across
// 5k/10k/20k/40k). At 500ms those cells sat within 1.7x of their own linear
// cost, so a merely BUSY host crossed the line and the cell reported a
// complexity change that had not happened. The quadratic passes this column
// exists to catch measured 1.9s to 32s at this size, so the ceiling below still
// separates the two classes by more than 2x on each side. Every other cell
// walks a parsed tree in tens of milliseconds and keeps the tighter bound.
const breadthParseBound = 1500 * time.Millisecond

//////////////////////////////////////////////////////////////////////////////
// The entry-point axis, derived from the battery's other columns
//////////////////////////////////////////////////////////////////////////////

// The set of public entry points this column has to cover is not listed here.
// It is READ OFF the cells the rest of the battery already drives, so an entry
// point added to any other column arrives here with no breadth cell and fails,
// rather than being covered by whoever remembers to add it.
//
// batteryCellLabel matches a battery cell's name argument. Every cell is
// named "<entry point>/<case>", which is what makes the entry-point axis
// recoverable from source.
var batteryCellLabel = regexp.MustCompile(`want(?:Reject|RejectIs|Terminate|BoundedErr|AcceptUnder)\(t, "([^"/]+)`)

// breadthEntryAlias folds the spellings the battery uses for one entry point
// onto a single name. A cell label names the call the cell makes, so the same
// entry point is spelled a few ways across columns.
var breadthEntryAlias = map[string]string{
	"Root.Schema": "SchemaNode.Schema",
	"Schema":      "SchemaNode.Schema",
}

// batteryEntryPoints extracts the entry points src drives, normalizing the
// compound labels (a cell that exercises two calls names both, joined by "+").
func batteryEntryPoints(src string) map[string]bool {
	out := map[string]bool{}
	for _, m := range batteryCellLabel.FindAllStringSubmatch(src, -1) {
		for _, part := range strings.Split(strings.ReplaceAll(m[1], "()", ""), "+") {
			if alias, ok := breadthEntryAlias[part]; ok {
				part = alias
			}
			out[part] = true
		}
	}
	return out
}

// breadthExempt names entry points with no sibling axis to grow, and why. An
// exemption is a claim that the entry point's cost cannot scale with a
// schema's sibling count; it is checked against the derived set, so an
// exemption for something the battery no longer drives is reported as stale.
var breadthExempt = map[string]string{
	"RatFromBytes":            "takes wire bytes and a scale, never a schema — its cost axis is byte length, not sibling count",
	"DurationFromBytes":       "takes a fixed 12-byte value; there is no sibling count to grow",
	"SingleObjectFingerprint": "hashes a fixed-width fingerprint header, independent of the schema's shape",
	"SchemaFor": "its input is a Go TYPE supplied as a compile-time type parameter, so the field count is " +
		"authored in the caller's own source rather than received at runtime; there is no runtime-supplied " +
		"sibling count to drive, and a generic type parameter cannot be built from reflect.StructOf",
}

// TestInvariant_EveryBatteryEntryPointHasABreadthCell derives the entry-point
// axis from the battery's other columns. Breadth is a property of the SCHEMA,
// so every entry point that takes one carries the axis; the exemptions are the
// entry points that take bytes instead.
func TestInvariant_EveryBatteryEntryPointHasABreadthCell(t *testing.T) {
	otherColumns, err := os.ReadFile("dos_battery_test.go")
	if err != nil {
		t.Fatalf("read dos_battery_test.go: %v", err)
	}
	thisColumn, err := os.ReadFile("schema_breadth_test.go")
	if err != nil {
		t.Fatalf("read schema_breadth_test.go: %v", err)
	}
	derived := batteryEntryPoints(string(otherColumns))
	if len(derived) == 0 {
		t.Fatal("the scan found no battery cells at all — the cell-naming convention changed, and this guard is watching nothing")
	}
	covered := batteryEntryPoints(string(thisColumn))

	for ep := range derived {
		if covered[ep] {
			continue
		}
		if _, ok := breadthExempt[ep]; ok {
			continue
		}
		t.Errorf("the battery drives %s, but the breadth column has no cell for it.\n"+
			"  A schema's sibling count is chosen by whoever writes the schema, so any entry point that\n"+
			"  takes a schema carries the axis. Add a cell, or add a breadthExempt entry saying which\n"+
			"  input it takes instead.", ep)
	}
	for ep := range breadthExempt {
		if !derived[ep] {
			t.Errorf("breadthExempt names %s, which the battery no longer drives — the exemption is stale", ep)
		}
	}
	t.Logf("entry points derived from the battery: %d, breadth cells cover %d, exempt %d",
		len(derived), len(covered), len(breadthExempt))
}

//////////////////////////////////////////////////////////////////////////////
// The union tag namespace — one shape per TIER
//////////////////////////////////////////////////////////////////////////////

// A union's tag tables are built by offering every branch to every tier of
// unionTagTiers. A GUARDED tier additionally has to decide whether a claim is
// ambiguous, which is the step that can go quadratic: asking "does any other
// branch claim this name" once per branch is a scan inside a loop over the
// same slice.
//
// The EMIT tables' degrade (unionLogicalTagOwnedElsewhere) has the same shape
// — a scan of the branch names, run once per branch — and is bounded for a
// structural reason rather than by its own construction, so it gets no cell
// here. It runs only where a branch's emitted qualifier differs from its own
// name, which is only where the branch is an UNNAMED kind carrying a logical
// type; a union may hold at most one branch per unnamed kind, because a
// second is refused at parse as a duplicate union type (verified: a union of
// two `long` branches differing only in logicalType is rejected). The eight
// unnamed kinds therefore cap that scan's outer loop at eight regardless of
// how many branches the union declares.
//
// The tiers are not listed here — they are read from unionTagTiers, so a tier
// added there without a shape below fails TestInvariant_EveryUnionTagTierHasA
// WideShape rather than shipping undriven.
type breadthTierShape struct {
	// tier is the unionTagTiers entry this shape drives, by name.
	tier string
	// build returns a union schema of n branches that this tier claims.
	build func(n int) string
	// distinctClaims records whether the n branches produce n DISTINCT claims
	// under this tier. It decides which cost the shape can observe: identical
	// claims let the ambiguity check stop at the first match, so only a shape
	// with distinct claims forces the full scan. Both are driven — the
	// identical-claim shape is what observes an ambiguity check that stopped
	// short-circuiting.
	distinctClaims bool
}

var breadthTierShapes = []breadthTierShape{
	{
		// Every branch is a named record in one namespace, so each branch's
		// own fullname is its claim and no two collide.
		tier:           "exact name",
		distinctClaims: true,
		build: func(n int) string {
			var sb strings.Builder
			sb.WriteString(`["null"`)
			for i := range n {
				fmt.Fprintf(&sb, `,{"type":"record","name":"a.R%d","fields":[]}`, i)
			}
			sb.WriteString(`]`)
			return sb.String()
		},
	},
	{
		// The qualifier tier claims "<kind>.<logicalType>", so its claim does
		// not carry the branch's name. A named fixed is the only shape that
		// can repeat it, and every such branch claims the SAME "fixed.uuid" —
		// the claim vocabulary is the (kind, logicalType) pairs, which is
		// fixed-size, so this tier cannot produce n distinct claims at all.
		// Driving it wide is still what observes an ambiguity check that
		// stopped stopping at the first match.
		tier:           "logical qualifier",
		distinctClaims: false,
		build: func(n int) string {
			var sb strings.Builder
			sb.WriteString(`["null"`)
			for i := range n {
				fmt.Fprintf(&sb, `,{"type":"fixed","name":"a.F%d","size":16,"logicalType":"uuid"}`, i)
			}
			sb.WriteString(`]`)
			return sb.String()
		},
	},
	{
		// Every branch is a namespaced named record, so each claims its own
		// unqualified short name and all n claims are distinct. This is the
		// shape that forces a per-branch ambiguity scan to walk the whole
		// sibling list every time.
		tier:           "unqualified short name",
		distinctClaims: true,
		build: func(n int) string {
			var sb strings.Builder
			sb.WriteString(`["null"`)
			for i := range n {
				fmt.Fprintf(&sb, `,{"type":"record","name":"ns%d.Short%d","fields":[]}`, i, i)
			}
			sb.WriteString(`]`)
			return sb.String()
		},
	},
}

func breadthTierShapeFor(tier string) (breadthTierShape, bool) {
	for _, s := range breadthTierShapes {
		if s.tier == tier {
			return s, true
		}
	}
	return breadthTierShape{}, false
}

// TestInvariant_EveryUnionTagTierHasAWideShape derives the tier set from
// unionTagTiers rather than restating it. A tier added there is a new
// per-branch claim, and if it is guarded it brings a new ambiguity scan with
// it; without a wide shape that tier's cost is never measured.
func TestInvariant_EveryUnionTagTierHasAWideShape(t *testing.T) {
	if len(unionTagTiers) == 0 {
		t.Fatal("unionTagTiers is empty — the tier set moved or was renamed, and this guard is watching nothing")
	}
	for _, tier := range unionTagTiers {
		if _, ok := breadthTierShapeFor(tier.name); !ok {
			t.Errorf("unionTagTiers contains tier %q, but no shape in breadthTierShapes drives it wide.\n"+
				"  Every tier is offered every branch, so a tier is a per-branch cost; a guarded one also\n"+
				"  brings an ambiguity check. Add a shape whose branches this tier claims.", tier.name)
		}
	}
	for _, s := range breadthTierShapes {
		if !slices.ContainsFunc(unionTagTiers, func(tr unionTagTier) bool { return tr.name == s.tier }) {
			t.Errorf("breadthTierShapes drives tier %q, which unionTagTiers no longer contains — the shape is stale", s.tier)
		}
	}
}

// TestRegression_UnionTagTierShapesReachTheirTier proves each shape actually
// makes its tier claim its branches. A cost cell whose input never reaches the
// pass it is timing measures nothing, and would stay green through any change
// to that pass.
func TestRegression_UnionTagTierShapesReachTheirTier(t *testing.T) {
	const n = 8
	for _, s := range breadthTierShapes {
		tierIdx := slices.IndexFunc(unionTagTiers, func(tr unionTagTier) bool { return tr.name == s.tier })
		if tierIdx < 0 {
			continue // reported by the invariant above
		}
		tier := unionTagTiers[tierIdx]

		sc, err := Parse(`{"type":"record","name":"Top","fields":[{"name":"f","type":` + s.build(n) + `}]}`)
		if err != nil {
			t.Errorf("tier %q: shape does not parse: %v", s.tier, err)
			continue
		}
		branches := sc.node.fields[0].node.branches
		claims := map[string]int{}
		claimed := 0
		for _, b := range branches {
			c, ok := tierClaim(tier, b)
			if !ok {
				continue
			}
			claimed++
			claims[c]++
		}
		// "null" is present in every shape and is claimed only by the exact
		// name tier, so the count is n or n+1 depending on the tier.
		if claimed < n {
			t.Errorf("tier %q: only %d of %d branches are claimed by this tier — the shape does not reach it",
				s.tier, claimed, n)
		}
		if got := len(claims) >= n; got != s.distinctClaims {
			t.Errorf("tier %q: distinctClaims=%v but the shape produced %d distinct claims over %d branches",
				s.tier, s.distinctClaims, len(claims), claimed)
		}
	}
}

// TestDoSBattery_C10a_UnionTagBreadth drives every tier's wide shape through
// every parse entry point. The tag tables are built during the parse, so the
// parse time is the observable.
func TestDoSBattery_C10a_UnionTagBreadth(t *testing.T) {
	for _, s := range breadthTierShapes {
		union := s.build(breadthN)
		schema := `{"type":"record","name":"Top","fields":[{"name":"f","type":` + union + `}]}`

		wantAcceptUnder(t, "Parse/wide-union-"+s.tier, breadthParseBound, func() error {
			_, err := Parse(schema)
			return err
		})
		wantAcceptUnder(t, "SchemaCache.Parse/wide-union-"+s.tier, breadthParseBound, func() error {
			var c SchemaCache
			_, err := c.Parse(schema)
			return err
		})
		// A forward-referenced branch leaves buildUnion with an unbound node,
		// so finalizeUnionNames rebuilds the tables over the resolved nodes —
		// a SECOND full build of the same tables, through the same tiers.
		fwd := `{"type":"record","name":"Top","fields":[` +
			`{"name":"a","type":"a.Fwd"},` +
			`{"name":"f","type":` + union[:len(union)-1] + `,{"type":"record","name":"a.Fwd","fields":[]}]}]}`
		wantAcceptUnder(t, "Parse/wide-union-forward-ref-"+s.tier, breadthParseBound, func() error {
			_, err := Parse(fwd)
			return err
		})
	}
}

//////////////////////////////////////////////////////////////////////////////
// Record field resolution — one shape per LOOKUP PATH
//////////////////////////////////////////////////////////////////////////////

// Matching a writer's record to a reader's is a per-writer-field lookup into
// the reader's fields. The lookup has three outcomes, and they cost
// differently: a name hit can stop early, an alias hit only after the whole
// name pass misses, and a miss pays for both passes in full.
type breadthFieldShape struct {
	name           string
	writer, reader func(n int) string
}

var breadthFieldShapes = []breadthFieldShape{
	{
		// Every writer field hits a reader field by NAME. The reader carries
		// one extra defaulted field so the two schemas are not canonically
		// equal: Resolve returns the reader untouched when they are, which
		// would skip the per-field matching entirely.
		name:   "name-hit",
		writer: breadthLongFields("f", nil, false),
		reader: breadthLongFieldsPlusExtra("f", nil, false),
	},
	{
		// Reader field i answers to alias f<i>; the writer names f<i>. Every
		// lookup misses the whole name pass before the alias pass finds it.
		name:   "alias-hit",
		writer: breadthLongFields("f", nil, false),
		reader: breadthLongFields("g", func(i int) string { return fmt.Sprintf("f%d", i) }, false),
	},
	{
		// No writer field name or alias appears in the reader, so every
		// lookup walks both passes to the end. The writer's fields are
		// skipped and the reader's are defaulted, so this resolves — the cost
		// of the two exhausted passes is what the cell observes.
		name:   "miss",
		writer: breadthLongFields("w", nil, false),
		reader: breadthLongFields("r", nil, true),
	},
}

// breadthLongFieldsPlusExtra is breadthLongFields with one additional
// defaulted field appended, so the record is compatible with the plain form
// but not canonically equal to it.
func breadthLongFieldsPlusExtra(prefix string, alias func(int) string, withDefault bool) func(n int) string {
	base := breadthLongFields(prefix, alias, withDefault)
	return func(n int) string {
		s := base(n)
		return s[:len(s)-len(`]}`)] + `,{"name":"zzExtra","type":"long","default":0}]}`
	}
}

// breadthLongFields builds a record of n long fields named <prefix><i>, each
// carrying the alias alias(i) when alias is non-nil and a default when
// withDefault is set.
func breadthLongFields(prefix string, alias func(int) string, withDefault bool) func(n int) string {
	return func(n int) string {
		var sb strings.Builder
		sb.WriteString(`{"type":"record","name":"Top","fields":[`)
		for i := range n {
			if i > 0 {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, `{"name":"%s%d"`, prefix, i)
			if alias != nil {
				fmt.Fprintf(&sb, `,"aliases":["%s"]`, alias(i))
			}
			sb.WriteString(`,"type":"long"`)
			if withDefault {
				sb.WriteString(`,"default":0`)
			}
			sb.WriteString(`}`)
		}
		sb.WriteString(`]}`)
		return sb.String()
	}
}

// enclosingFuncsCalling scans Go source for lines invoking ident and returns
// the names of the functions those lines sit in.
func enclosingFuncsCalling(src, ident string) []string {
	var out []string
	fn := ""
	funcRe := regexp.MustCompile(`^func (?:\([^)]*\) )?([A-Za-z_][A-Za-z0-9_]*)\(`)
	for line := range strings.SplitSeq(src, "\n") {
		if m := funcRe.FindStringSubmatch(line); m != nil {
			fn = m[1]
			continue
		}
		trimmed := strings.TrimSpace(line)
		if strings.Contains(line, ident+"(") && !strings.HasPrefix(trimmed, "//") &&
			!strings.HasPrefix(line, "func ") && fn != "" && !slices.Contains(out, fn) {
			out = append(out, fn)
		}
	}
	return out
}

// breadthFieldLookupEntryPoints maps each site that builds a reader-field
// lookup to the public entry point that reaches it. Building the lookup is
// what costs O(fields); the queries against it are constant. The KEYS are
// checked against the builders found in source, so a third builder added later
// has no entry point here and fails rather than shipping unmeasured.
var breadthFieldLookupEntryPoints = map[string]string{
	"resolveRecord":                "Resolve",
	"checkRecordFieldClaimsUnique": "CheckCompatibility",
}

// TestInvariant_EveryFieldLookupBuilderHasABreadthCell derives the build sites
// from source. A record's field count is set by the schema text, so every site
// that walks a reader's fields to match a writer's carries a breadth cost; a
// site with no cell is an unmeasured pass.
func TestInvariant_EveryFieldLookupBuilderHasABreadthCell(t *testing.T) {
	var builders []string
	for _, f := range []string{"resolve.go", "compat.go"} {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		builders = append(builders, enclosingFuncsCalling(string(src), "newReaderFieldLookup")...)
	}
	if len(builders) == 0 {
		t.Fatal("the scan found no builders of readerFieldLookup — the lookup moved or was renamed, and this guard is watching nothing")
	}
	for _, c := range builders {
		if _, ok := breadthFieldLookupEntryPoints[c]; !ok {
			t.Errorf("%s builds a readerFieldLookup but has no entry point in breadthFieldLookupEntryPoints.\n"+
				"  Building one walks every reader field, so this site carries the same breadth cost as the\n"+
				"  others. Name the public entry point that reaches it and give it a cell.", c)
		}
	}
	for c := range breadthFieldLookupEntryPoints {
		if !slices.Contains(builders, c) {
			t.Errorf("breadthFieldLookupEntryPoints names %s, which no longer builds a readerFieldLookup — the cell is stale", c)
		}
	}
	t.Logf("field-lookup builders derived from source: %v", builders)
}

// TestRegression_ReaderFieldLookupPrefersNamesOverAliases pins the routing the
// lookup's two maps exist to preserve: a writer name that is one reader
// field's ALIAS and a different reader field's NAME resolves to the NAME. A
// single merged map resolves it to whichever entry was written last, which is
// a silent reversal — the writer's data lands in the wrong reader field.
func TestRegression_ReaderFieldLookupPrefersNamesOverAliases(t *testing.T) {
	// Parse refuses a record whose field name collides with another field's
	// alias, so the deciding shape cannot be reached through Parse. It is
	// built directly: the ordering is the routing that the parse-time
	// rejection is justified by, and it has to hold on its own terms rather
	// than only because something upstream refuses the input.
	long := &schemaNode{kind: "long"}
	rec := func(fields ...fieldNode) *schemaNode {
		return &schemaNode{kind: "record", fields: fields}
	}
	for _, tc := range []struct {
		name  string
		node  *schemaNode
		query string
		want  int
	}{
		{
			// Field 0 is ALIASED "x"; field 1 is NAMED "x". The name wins
			// even though the alias appears first. This is the cell a merged
			// map gets wrong: inserting name-then-aliases per field in field
			// order writes the alias entry first, and first-write-wins then
			// routes "x" to field 0.
			name:  "alias-before-name",
			node:  rec(fieldNode{name: "a", aliases: []string{"x"}, node: long}, fieldNode{name: "x", node: long}),
			query: "x",
			want:  1,
		},
		{
			name:  "name-before-alias",
			node:  rec(fieldNode{name: "x", node: long}, fieldNode{name: "a", aliases: []string{"x"}, node: long}),
			query: "x",
			want:  0,
		},
		{
			name:  "alias-only",
			node:  rec(fieldNode{name: "a", node: long}, fieldNode{name: "b", aliases: []string{"x"}, node: long}),
			query: "x",
			want:  1,
		},
		{
			name:  "no-match",
			node:  rec(fieldNode{name: "a", node: long}, fieldNode{name: "b", node: long}),
			query: "x",
			want:  -1,
		},
	} {
		lk := newReaderFieldLookup(tc.node)
		if got := lk.index(tc.query); got != tc.want {
			t.Errorf("%s: writer field %q resolved to reader field %d, want %d", tc.name, tc.query, got, tc.want)
		}
	}
}

// TestRegression_BreadthFieldShapesReachTheResolvedPath proves every field
// shape actually reaches the per-field matching. Resolve returns the reader
// untouched when writer and reader are canonically equal, so a shape built
// from one schema text times the equality check and nothing else — a cell that
// stays green through any change to the matching it claims to measure.
func TestRegression_BreadthFieldShapesReachTheResolvedPath(t *testing.T) {
	const n = 4
	for _, s := range breadthFieldShapes {
		w, err := Parse(s.writer(n))
		if err != nil {
			t.Errorf("%s writer: %v", s.name, err)
			continue
		}
		r, err := Parse(s.reader(n))
		if err != nil {
			t.Errorf("%s reader: %v", s.name, err)
			continue
		}
		if bytes.Equal(w.Canonical(), r.Canonical()) {
			t.Errorf("%s: writer and reader are canonically equal, so Resolve short-circuits "+
				"before any per-field matching — this shape measures the equality check, not the lookup", s.name)
		}
	}
}

// TestDoSBattery_C10b_FieldLookupBreadth drives every lookup path through
// every entry point that reaches the lookup.
func TestDoSBattery_C10b_FieldLookupBreadth(t *testing.T) {
	for _, s := range breadthFieldShapes {
		w, err := Parse(s.writer(breadthN))
		if err != nil {
			t.Fatalf("%s writer: %v", s.name, err)
		}
		r, err := Parse(s.reader(breadthN))
		if err != nil {
			t.Fatalf("%s reader: %v", s.name, err)
		}
		wantAcceptUnder(t, "Resolve/wide-record-"+s.name, breadthBound, func() error {
			_, err := Resolve(w, r)
			return err
		})
		wantAcceptUnder(t, "CheckCompatibility/wide-record-"+s.name, breadthBound, func() error {
			// A reader field the writer lacks needs a default, so the miss
			// shape is legitimately incompatible; the cost is the question
			// here, not the verdict.
			CheckCompatibility(w, r)
			return nil
		})
	}
}

//////////////////////////////////////////////////////////////////////////////
// The rest of the entry points — one wide RECORD, every surface
//////////////////////////////////////////////////////////////////////////////

// TestDoSBattery_C10c_WideRecordSurfaces drives a record of breadthN fields
// through every remaining entry point the battery covers: the two wire
// directions, their JSON and single-object forms, and the schema surfaces that
// walk or re-emit the tree. A record's field count is chosen by whoever writes
// the schema, so each of these passes over the field list once per call and
// must stay linear in it.
func TestDoSBattery_C10c_WideRecordSurfaces(t *testing.T) {
	text := breadthLongFields("f", nil, false)(breadthN)
	s, err := Parse(text)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	val := make(map[string]any, breadthN)
	for i := range breadthN {
		val[fmt.Sprintf("f%d", i)] = int64(i)
	}
	wire, err := s.Encode(val)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	jsonWire, err := s.EncodeJSON(val)
	if err != nil {
		t.Fatalf("encode json: %v", err)
	}
	soe, err := s.AppendSingleObject(nil, val)
	if err != nil {
		t.Fatalf("single object: %v", err)
	}

	wantAcceptUnder(t, "Encode/wide-record", breadthBound, func() error {
		_, err := s.Encode(val)
		return err
	})
	wantAcceptUnder(t, "Decode/wide-record", breadthBound, func() error {
		var out map[string]any
		_, err := s.Decode(wire, &out)
		return err
	})
	wantAcceptUnder(t, "EncodeJSON/wide-record", breadthBound, func() error {
		_, err := s.EncodeJSON(val)
		return err
	})
	wantAcceptUnder(t, "DecodeJSON/wide-record", breadthBound, func() error {
		var out map[string]any
		return s.DecodeJSON(jsonWire, &out)
	})
	wantAcceptUnder(t, "AppendSingleObject/wide-record", breadthBound, func() error {
		_, err := s.AppendSingleObject(nil, val)
		return err
	})
	wantAcceptUnder(t, "DecodeSingleObject/wide-record", breadthBound, func() error {
		var out map[string]any
		_, err := s.DecodeSingleObject(soe, &out)
		return err
	})
	wantAcceptUnder(t, "Canonical/wide-record", breadthBound, func() error {
		if len(s.Canonical()) == 0 {
			return fmt.Errorf("empty canonical form")
		}
		return nil
	})
	wantAcceptUnder(t, "Fingerprint/wide-record", breadthBound, func() error {
		if len(s.Fingerprint(crypto.SHA256.New())) == 0 {
			return fmt.Errorf("empty fingerprint")
		}
		return nil
	})
	wantAcceptUnder(t, "String/wide-record", breadthBound, func() error {
		if len(s.String()) == 0 {
			return fmt.Errorf("empty string form")
		}
		return nil
	})
	wantAcceptUnder(t, "Root/wide-record", breadthBound, func() error {
		if len(s.Root().Fields) != breadthN {
			return fmt.Errorf("Root surfaced %d fields, want %d", len(s.Root().Fields), breadthN)
		}
		return nil
	})
	root := s.Root()
	wantAcceptUnder(t, "SchemaNode.Schema/wide-record", breadthBound, func() error {
		_, err := root.Schema()
		return err
	})
}

//////////////////////////////////////////////////////////////////////////////
// The sibling-KIND axis, derived from schemaNode's own slice fields
//////////////////////////////////////////////////////////////////////////////

// A schema declares siblings in more than one place. The column above drives
// exactly one of them — a record's fields — through every entry point, and a
// union's branches through Parse alone. That left the SHAPE hand-picked per
// cell, and a shape nobody picked is a shape nobody bounded: the union and enum
// containers carried per-value passes over their siblings on the JSON side for
// as long as the column existed.
//
// So the shape axis is derived too. Every schemaNode field whose length is set
// by the schema TEXT is a sibling kind, those fields are read out of the struct
// by reflection, and each one must be driven or exempted with the reason it
// cannot grow. A sibling-bearing field added to schemaNode later arrives here
// with no cell and fails.
//
// The second half is the VALUE count. A cell that encodes ONE value against a
// wide schema cannot see a pass that runs once per value — which is exactly the
// class the union tag and enum symbol lookups were in. Where a single value's
// own size is independent of the sibling count, the cells drive many values and
// place the answer LAST, because a table cannot tell first from last and a scan
// takes the whole list to reach the end.

// breadthValueN is the value count the per-value cells drive. Chosen with
// breadthN so a per-value scan of the siblings is seconds of work while a table
// lookup stays in the milliseconds.
const breadthValueN = 2000

// breadthSiblingKind is one sibling-bearing schemaNode field and the schemas
// that grow it.
type breadthSiblingKind struct {
	// field is the schemaNode field this kind grows, and is what ties the
	// table to the reflected set.
	field string
	// schema declares n siblings of this kind; twin resolves against it
	// without being canonically equal to it, so the resolve cells time the
	// matching rather than the equality short-circuit.
	schema func(n int) string
	twin   func(n int) string
	// value is a datum for schema(n). perValue says whether a single value's
	// own size is independent of the sibling count: when it is, the cells
	// drive breadthValueN of them, because that is where a once-per-value pass
	// over the siblings shows up. When it is not — a record value carries one
	// entry per field — driving many values would only be timing the values.
	value    func(n int) any
	perValue bool
}

func breadthAliasList(n int, qualified bool) string {
	var sb strings.Builder
	for i := range n {
		if i > 0 {
			sb.WriteByte(',')
		}
		if qualified {
			fmt.Fprintf(&sb, `"ns%d.A%d"`, i, i)
		} else {
			fmt.Fprintf(&sb, `"A%d"`, i)
		}
	}
	return sb.String()
}

// breadthAliasedRecord wraps the aliased record in an array so a cell can drive
// many values through it: the alias list's length is independent of a value's
// size, so a per-value pass over it is exactly the shape worth bounding.
func breadthAliasedRecord(n int, qualified bool, fieldType string) string {
	return fmt.Sprintf(`{"type":"array","items":{"type":"record","name":"x.R","aliases":[%s],"fields":[{"name":"f","type":%q}]}}`,
		breadthAliasList(n, qualified), fieldType)
}

// breadthWideUnionArray wraps a union of n named records in an array so a cell
// can drive many values through one schema. The LAST branch is the one the
// values name.
func breadthWideUnionArray(ns string, n int) string {
	var sb strings.Builder
	sb.WriteString(`{"type":"array","items":["null"`)
	for i := range n {
		fmt.Fprintf(&sb, `,{"type":"record","name":"%s.R%d","fields":[]}`, ns, i)
	}
	sb.WriteString(`]}`)
	return sb.String()
}

func breadthWideEnumArray(n int, defaulted bool) string {
	var sb strings.Builder
	sb.WriteString(`{"type":"array","items":{"type":"enum","name":"E","symbols":[`)
	for i := range n {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `"S%d"`, i)
	}
	sb.WriteString(`]`)
	if defaulted {
		sb.WriteString(`,"default":"S0"`)
	}
	sb.WriteString(`}}`)
	return sb.String()
}

// breadthSiblingKinds is the table the reflected field set is checked against.
var breadthSiblingKinds = []breadthSiblingKind{
	{
		field:  "fields",
		schema: breadthLongFields("f", nil, false),
		// int fields promote into the long ones, so the pair resolves and the
		// canonical forms differ.
		twin: func(n int) string {
			var sb strings.Builder
			sb.WriteString(`{"type":"record","name":"Top","fields":[`)
			for i := range n {
				if i > 0 {
					sb.WriteByte(',')
				}
				fmt.Fprintf(&sb, `{"name":"f%d","type":"int"}`, i)
			}
			sb.WriteString(`]}`)
			return sb.String()
		},
		value: func(n int) any {
			m := make(map[string]any, n)
			for i := range n {
				m[fmt.Sprintf("f%d", i)] = int64(i)
			}
			return m
		},
		perValue: false,
	},
	{
		field:  "branches",
		schema: func(n int) string { return breadthWideUnionArray("a", n) },
		// A different namespace, so the branches match on the unqualified
		// short name rather than short-circuiting on canonical equality.
		twin: func(n int) string { return breadthWideUnionArray("b", n) },
		value: func(n int) any {
			// The LAST branch: a scan has to walk every earlier one to reach it.
			tag := fmt.Sprintf("a.R%d", n-1)
			vals := make([]any, breadthValueN)
			for i := range vals {
				vals[i] = map[string]any{tag: map[string]any{}}
			}
			return vals
		},
		perValue: true,
	},
	{
		field:  "symbols",
		schema: func(n int) string { return breadthWideEnumArray(n, false) },
		// One fewer symbol on the writer side, all of them present in the
		// reader, so the pair resolves and is not canonically equal.
		twin: func(n int) string { return breadthWideEnumArray(n-1, false) },
		value: func(n int) any {
			last := fmt.Sprintf("S%d", n-1)
			vals := make([]string, breadthValueN)
			for i := range vals {
				vals[i] = last
			}
			return vals
		},
		perValue: true,
	},
	{
		field:  "aliases",
		schema: func(n int) string { return breadthAliasedRecord(n, true, "long") },
		twin:   func(n int) string { return breadthAliasedRecord(n, true, "int") },
		value: func(n int) any {
			vals := make([]any, breadthValueN)
			for i := range vals {
				vals[i] = map[string]any{"f": int64(i)}
			}
			return vals
		},
		perValue: true,
	},
	{
		field: "bareAliases",
		// An alias declared WITHOUT a dot lands in bareAliases as well, which
		// is the slice the short-name match tier reads.
		schema: func(n int) string { return breadthAliasedRecord(n, false, "long") },
		twin:   func(n int) string { return breadthAliasedRecord(n, false, "int") },
		value: func(n int) any {
			vals := make([]any, breadthValueN)
			for i := range vals {
				vals[i] = map[string]any{"f": int64(i)}
			}
			return vals
		},
		perValue: true,
	},
}

// breadthSiblingExempt names schemaNode slice fields with no schema-text
// length, and why. An exemption is a claim that the field cannot grow with what
// a caller writes.
//
// It is empty today, and that is the honest state rather than an oversight:
// every slice schemaNode carries is filled from something the schema declares,
// so every one of them is driven. The map stays because the next slice field
// added may not be — and an exemption without a reason is how a cell goes
// missing quietly. Slices on the SERIALIZERS (deserUnion's index→name lists)
// are sized by the branch count and are grown by the branches row; they are not
// schemaNode fields and so are outside what the reflection reads.
var breadthSiblingExempt = map[string]string{}

// breadthSiblingFieldSet reads every slice-valued schemaNode field out of the
// struct. Slice-valued is the mechanical form of "its length comes from the
// schema text": every one of them is filled by the builder from something the
// schema declares.
func breadthSiblingFieldSet() []string {
	rt := reflect.TypeFor[schemaNode]()
	var out []string
	for i := range rt.NumField() {
		if f := rt.Field(i); f.Type.Kind() == reflect.Slice {
			out = append(out, f.Name)
		}
	}
	return out
}

// TestInvariant_EveryBreadthSiblingKindIsCelled derives the sibling-kind axis
// from schemaNode instead of listing it. Adding a slice field to schemaNode
// declares a new way for a caller to make a schema wide; this fails until that
// way is either driven or exempted with the reason it cannot grow.
func TestInvariant_EveryBreadthSiblingKindIsCelled(t *testing.T) {
	derived := breadthSiblingFieldSet()
	if len(derived) == 0 {
		t.Fatal("the reflection found no slice fields on schemaNode at all — this guard is watching nothing")
	}
	celled := map[string]bool{}
	for _, k := range breadthSiblingKinds {
		if celled[k.field] {
			t.Errorf("two breadthSiblingKinds entries both drive %s", k.field)
		}
		celled[k.field] = true
	}
	for _, field := range derived {
		if celled[field] || breadthSiblingExempt[field] != "" {
			continue
		}
		t.Errorf("schemaNode.%s is a slice whose length comes from the schema text, and no breadth cell drives it.\n"+
			"  A caller chooses how many of these a schema declares, so every pass over them has to stay linear.\n"+
			"  Add a breadthSiblingKinds entry, or a breadthSiblingExempt entry saying why the length cannot grow.", field)
	}
	for field := range celled {
		if !slices.Contains(derived, field) {
			t.Errorf("breadthSiblingKinds drives %q, which is not a slice field on schemaNode — the cell is watching a field that moved or was renamed", field)
		}
	}
	for field := range breadthSiblingExempt {
		if !slices.Contains(derived, field) {
			t.Errorf("breadthSiblingExempt names %q, which schemaNode no longer has — the exemption is stale", field)
		}
	}
	t.Logf("schemaNode sibling slices: %d, celled %d, exempt %d", len(derived), len(celled), len(breadthSiblingExempt))
}

// TestDoSBattery_C10d_SiblingKindSurfaces crosses every entry point with every
// sibling kind. The record column above is one row of this cross; the rows that
// did not exist are where both of the per-value lookups hid.
func TestDoSBattery_C10d_SiblingKindSurfaces(t *testing.T) {
	for _, kind := range breadthSiblingKinds {
		t.Run(kind.field, func(t *testing.T) {
			text := kind.schema(breadthN)
			s, err := Parse(text)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			twinText := kind.twin(breadthN)
			twin, err := Parse(twinText)
			if err != nil {
				t.Fatalf("parse twin: %v", err)
			}
			// A twin canonically equal to the schema takes Resolve's equality
			// short-circuit, and the resolve cells would time that instead of
			// the sibling matching they exist to bound.
			if string(s.Canonical()) == string(twin.Canonical()) {
				t.Fatal("the twin is canonically equal to the schema, so the resolve cells would time the equality short-circuit rather than the match")
			}
			val := kind.value(breadthN)
			wire, err := s.Encode(val)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			jsonWire, err := s.EncodeJSON(val)
			if err != nil {
				t.Fatalf("encode json: %v", err)
			}
			soe, err := s.AppendSingleObject(nil, val)
			if err != nil {
				t.Fatalf("single object: %v", err)
			}

			label := func(entry string) string { return entry + "/wide-" + kind.field }
			wantAcceptUnder(t, label("Parse"), breadthParseBound, func() error {
				_, err := Parse(text)
				return err
			})
			wantAcceptUnder(t, label("SchemaCache.Parse"), breadthParseBound, func() error {
				var c SchemaCache
				_, err := c.Parse(text)
				return err
			})
			wantAcceptUnder(t, label("Resolve"), breadthBound, func() error {
				_, err := Resolve(twin, s)
				return err
			})
			wantAcceptUnder(t, label("CheckCompatibility"), breadthBound, func() error {
				return CheckCompatibility(twin, s)
			})
			wantAcceptUnder(t, label("Encode"), breadthBound, func() error {
				_, err := s.Encode(val)
				return err
			})
			wantAcceptUnder(t, label("Decode"), breadthBound, func() error {
				var out any
				_, err := s.Decode(wire, &out)
				return err
			})
			wantAcceptUnder(t, label("EncodeJSON"), breadthBound, func() error {
				_, err := s.EncodeJSON(val)
				return err
			})
			wantAcceptUnder(t, label("DecodeJSON"), breadthBound, func() error {
				var out any
				return s.DecodeJSON(jsonWire, &out)
			})
			// The tagged form routes every value through the union tag table
			// rather than through try-each, which is the consumer the bare
			// form never reaches.
			wantAcceptUnder(t, label("DecodeJSON+TaggedUnions"), breadthBound, func() error {
				var out any
				return s.DecodeJSON(jsonWire, &out, TaggedUnions())
			})
			wantAcceptUnder(t, label("AppendSingleObject"), breadthBound, func() error {
				_, err := s.AppendSingleObject(nil, val)
				return err
			})
			wantAcceptUnder(t, label("DecodeSingleObject"), breadthBound, func() error {
				var out any
				_, err := s.DecodeSingleObject(soe, &out)
				return err
			})
			wantAcceptUnder(t, label("Canonical"), breadthBound, func() error {
				if len(s.Canonical()) == 0 {
					return fmt.Errorf("empty canonical form")
				}
				return nil
			})
			wantAcceptUnder(t, label("Fingerprint"), breadthBound, func() error {
				if len(s.Fingerprint(crypto.SHA256.New())) == 0 {
					return fmt.Errorf("empty fingerprint")
				}
				return nil
			})
			wantAcceptUnder(t, label("String"), breadthBound, func() error {
				if len(s.String()) == 0 {
					return fmt.Errorf("empty string form")
				}
				return nil
			})
			var root SchemaNode
			wantAcceptUnder(t, label("Root"), breadthBound, func() error {
				root = *s.Root()
				return nil
			})
			wantAcceptUnder(t, label("SchemaNode.Schema"), breadthBound, func() error {
				_, err := root.Schema()
				return err
			})
		})
	}
}
