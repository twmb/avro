package avro

import (
	"os"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// The tag namespace's TIER SET is derived, not listed.
//
// Two consumers read it: findUnionBranch resolves a caller-written name, and
// fillUnionTagTables builds the binary tagged-map lookup. Both walk
// unionTagTiers, so neither can grow a tier the other lacks. What remains
// possible is someone adding a tier by HAND inside one of them, or adding one
// to the slice that no test ever reaches — and those are what these guards
// refuse.
// ---------------------------------------------------------------------------

// unionTagTierCount is the number of tiers the suite knows how to reach. It is
// stated here so that adding a tier is a DECISION: the count fails, and the
// person adding it has to extend the corpus in
// TestInvariant_EveryUnionTagTierIsReachable rather than let a tier ship
// unexercised.
const unionTagTierCount = 3

func TestInvariant_UnionTagTierCountIsStated(t *testing.T) {
	if len(unionTagTiers) != unionTagTierCount {
		t.Fatalf("unionTagTiers has %d tiers, the suite is written for %d.\n"+
			"A tier is a new rule by which a caller's tag names a branch: extend the corpus in "+
			"TestInvariant_EveryUnionTagTierIsReachable so the new tier is actually exercised on every "+
			"wire, then raise this count.", len(unionTagTiers), unionTagTierCount)
	}
	seen := map[string]bool{}
	for i, tier := range unionTagTiers {
		if tier.name == "" {
			t.Errorf("tier %d has no name; the guards report by name", i)
		}
		if seen[tier.name] {
			t.Errorf("two tiers are both named %q", tier.name)
		}
		seen[tier.name] = true
		if tier.claim == nil {
			t.Errorf("tier %q has no claim function", tier.name)
		}
	}
}

// funcBody returns the source of the named top-level function in file.
func funcBody(t *testing.T, file, fn string) string {
	t.Helper()
	src, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	start := strings.Index(string(src), "\nfunc "+fn+"(")
	if start < 0 {
		t.Fatalf("%s: no top-level func %s", file, fn)
	}
	rest := string(src)[start+1:]
	end := strings.Index(rest, "\n}\n")
	if end < 0 {
		t.Fatalf("%s: func %s has no terminator", file, fn)
	}
	return rest[:end]
}

// TestInvariant_UnionTagTiersAreDerived is the source-level half: both
// consumers must reach the tier set by WALKING it. A tier open-coded inside
// either one is invisible to the other, which is the drift this whole
// structure exists to remove — and it is exactly how the legacy
// "<kind>.<logicalType>" spelling came to be honored by the resolver and not
// by the lookup table.
func TestInvariant_UnionTagTiersAreDerived(t *testing.T) {
	resolver := funcBody(t, "json_codec.go", "findUnionBranch")
	if n := strings.Count(resolver, "range unionTagTiers"); n != 1 {
		t.Errorf("findUnionBranch walks the tier slice %d times, want exactly 1", n)
	}
	// One scan over the branches, and it must be the one INSIDE the tier walk.
	// A second scan is a hand-written tier: it answers names the lookup table
	// will never register.
	if n := strings.Count(resolver, "range union.branches"); n != 1 {
		t.Errorf("findUnionBranch scans union.branches %d times, want exactly 1 (inside the tier walk).\n"+
			"A scan outside the walk is a tier only the resolver knows about; move it into unionTagTiers "+
			"so fillUnionTagTables honors it too.", n)
	}
	builder := funcBody(t, "schema.go", "fillUnionTagTables")
	if n := strings.Count(builder, "range unionTagTiers"); n != 1 {
		t.Errorf("fillUnionTagTables walks the tier slice %d times, want exactly 1", n)
	}
	// The kind vocabulary the logical-qualifier tier is defined over must have
	// exactly one copy, and it must live in the tier. A second copy is the
	// same set written twice, which is how the two sides drifted before.
	const kindList = `case "null", "boolean", "int", "long", "float", "double", "string", "bytes", "fixed":`
	src, err := os.ReadFile("json_codec.go")
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(src), kindList); n != 1 {
		t.Errorf("the logical-qualifier kind vocabulary appears %d times in json_codec.go, want 1", n)
	}
	if !strings.Contains(tierSource(t, "logical qualifier"), kindList) {
		t.Errorf("the logical-qualifier kind vocabulary is not inside its own tier")
	}
}

// tierSource returns the source text of the named tier's literal.
func tierSource(t *testing.T, name string) string {
	t.Helper()
	src, err := os.ReadFile("json_codec.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	i := strings.Index(s, "unionTagTiers = []unionTagTier{")
	if i < 0 {
		t.Fatal("no unionTagTiers literal")
	}
	j := strings.Index(s[i:], "\n}\n")
	block := s[i : i+j]
	k := strings.Index(block, "name:    "+`"`+name+`"`)
	if k < 0 {
		k = strings.Index(block, "name: "+`"`+name+`"`)
	}
	if k < 0 {
		t.Fatalf("tier %q not found in the literal", name)
	}
	return block[k:]
}

// tierAnswering reports which tier resolves name against union, or -1.
// It re-walks the tiers the way findUnionBranch does, so it can attribute a
// resolution rather than guess at it.
func tierAnswering(union *schemaNode, name string) int {
	for ti, tier := range unionTagTiers {
		var match bool
		var found int
		for _, b := range union.branches {
			if b == nil {
				continue
			}
			if !tierMatches(tier, b, name) {
				continue
			}
			if !tier.guarded {
				return ti
			}
			if match {
				found = -1
				break
			}
			match, found = true, ti
		}
		if match && found >= 0 {
			return ti
		}
		if found == -1 {
			return -1 // refused as ambiguous by this tier
		}
	}
	return -1
}

// TestInvariant_EveryUnionTagTierIsReachable is the behavioral half: every
// tier in the slice must actually answer for some (union, tag) in the corpus.
// A tier nothing reaches is a rule nothing tests, and its guard would neuter
// green. This is the assertion that has to be extended when a tier is added.
func TestInvariant_EveryUnionTagTierIsReachable(t *testing.T) {
	type cell struct{ schema, tag string }
	corpus := []cell{
		// exact name
		{`["null","int"]`, "int"},
		{`["null",{"type":"record","name":"R","namespace":"ns","fields":[{"name":"x","type":"int"}]}]`, "ns.R"},
		// logical qualifier, primitive-backed and named-fixed-backed
		{`["null",{"type":"long","logicalType":"timestamp-millis"}]`, "long.timestamp-millis"},
		{`["null",{"type":"fixed","name":"F","namespace":"n","size":16,"logicalType":"uuid"}]`, "fixed.uuid"},
		// unqualified short name
		{`["null",{"type":"record","name":"R","namespace":"ns","fields":[{"name":"x","type":"int"}]}]`, "R"},
	}
	reached := make([]bool, len(unionTagTiers))
	for _, c := range corpus {
		s, err := Parse(c.schema)
		if err != nil {
			t.Fatalf("%s: %v", c.schema, err)
		}
		ti := tierAnswering(s.node, c.tag)
		if ti < 0 {
			t.Errorf("no tier answers %q for %s", c.tag, c.schema)
			continue
		}
		reached[ti] = true
		// The attribution must agree with the real resolver.
		if got := findUnionBranch(s.node, c.tag); got == nil {
			t.Errorf("tier %d attributed %q for %s, but findUnionBranch refuses it",
				ti, c.tag, c.schema)
		}
	}
	for i, ok := range reached {
		if !ok {
			t.Errorf("tier %q (index %d) is never reached by the corpus — it ships unexercised",
				unionTagTiers[i].name, i)
		}
	}
}

// TestInvariant_GuardedTiersRefuseAmbiguity states the rule the guarded flag
// encodes, over every guarded tier: a name two branches claim within one tier
// resolves NOWHERE. Silently taking the first is a coin flip between two
// branches the caller may have meant either of, and the two wires would have
// to make the same coin flip to stay in agreement.
func TestInvariant_GuardedTiersRefuseAmbiguity(t *testing.T) {
	cells := []struct {
		tier   string
		schema string
		tag    string
	}{
		{"logical qualifier",
			`["null",{"type":"fixed","name":"A","size":16,"logicalType":"uuid"},{"type":"fixed","name":"B","size":16,"logicalType":"uuid"}]`,
			"fixed.uuid"},
		{"unqualified short name",
			`["null",{"type":"record","name":"R","namespace":"n1","fields":[{"name":"x","type":"int"}]},{"type":"record","name":"R","namespace":"n2","fields":[{"name":"y","type":"int"}]}]`,
			"R"},
	}
	guarded := map[string]bool{}
	for _, tier := range unionTagTiers {
		if tier.guarded {
			guarded[tier.name] = true
		}
	}
	covered := map[string]bool{}
	for _, c := range cells {
		if !guarded[c.tier] {
			t.Errorf("cell names tier %q, which is not guarded", c.tier)
			continue
		}
		covered[c.tier] = true
		s, err := Parse(c.schema)
		if err != nil {
			t.Fatalf("%s: %v", c.schema, err)
		}
		if b := findUnionBranch(s.node, c.tag); b != nil {
			t.Errorf("tier %q: %q is claimed by two branches yet resolved to %q — a guarded tier must refuse it",
				c.tier, c.tag, b.name)
		}
		// The other wire has to refuse it too, or a caller gets a value on one
		// and an error on the other. Probed through the public encoder rather
		// than the table so the assertion is about what a caller sees.
		if _, err := s.Encode(map[string]any{c.tag: []byte("0123456789abcdef")}); err == nil {
			t.Errorf("tier %q: %q is claimed by two branches yet the binary tagged-map encode accepted it — "+
				"the two wires disagree", c.tier, c.tag)
		}
	}
	for name := range guarded {
		if !covered[name] {
			t.Errorf("guarded tier %q has no ambiguity cell; its guard is unexercised", name)
		}
	}
}

// TestInvariant_UnionTagResolveDoesNotAllocate locks the property that made it
// safe to route the resolver through a shared tier slice rather than leaving
// the rules open-coded: a tier appends its claim into a stack buffer and the
// comparison `string(claimed) == name` is a compare, not a conversion, so
// resolving a tag allocates nothing however many tiers exist. A tier that
// builds its claim some other way — fmt, a map lookup, strings.Join — would
// put an allocation on a per-value JSON path, and this is where that shows up.
func TestInvariant_UnionTagResolveDoesNotAllocate(t *testing.T) {
	s := MustParse(`["null",{"type":"long","logicalType":"timestamp-millis"},` +
		`{"type":"fixed","name":"F","namespace":"n","size":16,"logicalType":"uuid"},` +
		`{"type":"record","name":"R","namespace":"ns","fields":[{"name":"x","type":"int"}]}]`)
	for _, tag := range []string{
		"long.timestamp-millis", // logical qualifier, primitive
		"fixed.uuid",            // logical qualifier, named fixed
		"n.F",                   // exact
		"R",                     // unqualified short name
		"absent",                // no tier claims it: every tier runs
	} {
		got := testing.AllocsPerRun(50, func() {
			findUnionBranch(s.node, tag)
		})
		if got != 0 {
			t.Errorf("resolving %q allocates %.0f times per call; the tier walk must stay allocation-free", tag, got)
		}
	}
}
