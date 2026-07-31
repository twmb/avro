package avro

import (
	"fmt"
	"strings"
	"testing"
)

// Union-branch selection: the index must give the SCAN's verdict.
//
// Which reader branch a writer node selects is a rule with four ranks — full
// name, alias, unqualified short name, bare-alias short name — plus numeric
// and string/bytes promotion, and a fixed's SIZE folded into the match rather
// than checked after it. Answering it by ranking every reader branch is a scan
// inside the loop over writer branches that both Resolve and CheckCompatibility
// run, so the answer is now indexed ahead of the questions.
//
// Indexing a rule is where a rule quietly changes. Java's
// Resolver.firstMatchingBranch scans per writer branch too, so there is no
// reference to re-derive the verdict from and no interop pressure that would
// surface a drift — the only thing that can catch one is the scan itself,
// stated independently and asked the same questions.

// matchTierOracle ranks how strongly a reader branch matches a writer node.
// This is the rule written out longhand, from the spec clauses and NOT_BUGS
// #44's ruling, rather than read off branchMatchTiers — so it is an
// independent statement of what the index is supposed to encode, and a
// disagreement means the index changed a verdict rather than only its cost.
type matchTierOracle int

const (
	oracleNone matchTierOracle = iota
	oraclePromotion
	oracleUnqualified
	oracleExact
)

func (t matchTierOracle) String() string {
	switch t {
	case oracleExact:
		return "exact"
	case oracleUnqualified:
		return "unqualified"
	case oraclePromotion:
		return "promotion"
	}
	return "none"
}

func oracleTier(r, w *schemaNode) matchTierOracle {
	if r.kind == w.kind {
		switch r.kind {
		case "record", "enum", "fixed":
			// Size is part of the MATCH predicate for fixed, not a
			// post-selection check: a wrong-size same-name fixed must not
			// match, so selection keeps looking and a later size-matching
			// branch wins.
			if r.kind == "fixed" && r.size != w.size {
				return oracleNone
			}
			if r.name == w.name {
				return oracleExact
			}
			for _, a := range r.aliases {
				if a == w.name {
					return oracleExact
				}
			}
			if unqualified(r.name) == unqualified(w.name) {
				return oracleUnqualified
			}
			for _, a := range r.bareAliases {
				if a == unqualified(w.name) {
					return oracleUnqualified
				}
			}
			return oracleNone
		default:
			return oracleExact
		}
	}
	if promotionDeser(w.kind, r.kind) != nil {
		return oraclePromotion
	}
	return oracleNone
}

// oracleMatch is the best-tier scan: rank every reader branch, keep the best,
// ties resolve by declaration order.
func oracleMatch(r, w *schemaNode) (*schemaNode, matchTierOracle) {
	best, bestTier := (*schemaNode)(nil), oracleNone
	for _, rb := range r.branches {
		if rb == nil {
			continue
		}
		if t := oracleTier(rb, w); t > bestTier {
			bestTier, best = t, rb
		}
	}
	return best, bestTier
}

// branchMatchCorpus returns reader unions spanning every rank the rule has,
// including the shapes where two ranks compete and where declaration order is
// the only thing separating two candidates.
func branchMatchCorpus() []string {
	return []string{
		`["null","int","string"]`,
		`["long","double"]`,
		`["double","long"]`,
		`["string","bytes"]`,
		`["float","double","long"]`,
		// Same short name in two namespaces: exact must beat unqualified, and
		// the reversed pair proves the winner is the NAME and not the order.
		`[{"type":"record","name":"a.R","fields":[]},{"type":"record","name":"b.R","fields":[]}]`,
		`[{"type":"record","name":"b.R","fields":[]},{"type":"record","name":"a.R","fields":[]}]`,
		// A qualified alias matches a writer fullname exactly; a bare alias
		// short-matches any namespace. Both live beside a plain branch so the
		// tier that answers is observable.
		`[{"type":"record","name":"a.Q","aliases":["a.R"],"fields":[]}]`,
		`[{"type":"record","name":"a.Q","aliases":["R"],"fields":[]}]`,
		`[{"type":"record","name":"z.Z","fields":[]},{"type":"record","name":"a.Q","aliases":["a.R"],"fields":[]}]`,
		// Same short name, different sizes: the 4-size writer must skip PAST
		// the 8-size branch rather than match and fail later.
		`[{"type":"fixed","name":"b.F","size":8},{"type":"fixed","name":"a.F","size":4}]`,
		`[{"type":"fixed","name":"a.F","size":4},{"type":"fixed","name":"b.F","size":8}]`,
		`[{"type":"enum","name":"a.E","symbols":["A"]},"string"]`,
		`[{"type":"enum","name":"b.E","symbols":["A"]}]`,
		`["null",{"type":"map","values":"int"},{"type":"array","items":"int"}]`,
		`[{"type":"record","name":"R","fields":[]}]`,
		// A named branch that fails both name tiers must NOT fall through to
		// promotion, so a union of only records answers nothing for a record
		// writer of an unrelated name.
		`[{"type":"record","name":"q.Q","fields":[]}]`,
		`["int"]`,
		`[]`,
	}
}

func branchMatchWriters() []string {
	return []string{
		`"null"`, `"boolean"`, `"int"`, `"long"`, `"float"`, `"double"`, `"string"`, `"bytes"`,
		`{"type":"record","name":"a.R","fields":[]}`,
		`{"type":"record","name":"b.R","fields":[]}`,
		`{"type":"record","name":"c.R","fields":[]}`,
		`{"type":"record","name":"R","fields":[]}`,
		`{"type":"fixed","name":"a.F","size":4}`,
		`{"type":"fixed","name":"c.F","size":4}`,
		`{"type":"fixed","name":"c.F","size":8}`,
		`{"type":"enum","name":"a.E","symbols":["A"]}`,
		`{"type":"enum","name":"c.E","symbols":["A"]}`,
		`{"type":"map","values":"int"}`,
		`{"type":"array","items":"int"}`,
	}
}

// TestInvariant_ReaderBranchLookupMatchesTheScan is the verdict half. The
// lookup exists to make selection constant-time per writer branch; the one
// thing it may not do is select differently.
func TestInvariant_ReaderBranchLookupMatchesTheScan(t *testing.T) {
	tierHits := map[matchTierOracle]int{}
	cells := 0
	for _, readerText := range branchMatchCorpus() {
		r := MustParse(readerText).node
		lk := newReaderBranchLookup(r)
		for _, writerText := range branchMatchWriters() {
			w := MustParse(writerText).node
			wantNode, wantTier := oracleMatch(r, w)
			gotNode := lk.match(w)
			cells++
			tierHits[wantTier]++
			if gotNode != wantNode {
				t.Errorf("reader %s\nwriter %s\n  scan  → %s\n  index → %s\n  (tier %s)",
					readerText, writerText, nodeDesc(wantNode), nodeDesc(gotNode), wantTier)
			}
		}
	}
	// A net that never reaches a rank cannot notice that rank changing. Every
	// rank the rule has, plus the no-match verdict, has to appear.
	for _, tier := range []matchTierOracle{oracleExact, oracleUnqualified, oraclePromotion, oracleNone} {
		if tierHits[tier] == 0 {
			t.Errorf("no corpus cell resolves at the %s rank — that rank ships undriven", tier)
		}
	}
	t.Logf("cells=%d exact=%d unqualified=%d promotion=%d none=%d",
		cells, tierHits[oracleExact], tierHits[oracleUnqualified], tierHits[oraclePromotion], tierHits[oracleNone])
}

func nodeDesc(n *schemaNode) string {
	if n == nil {
		return "<no branch>"
	}
	if n.name != "" {
		return fmt.Sprintf("%s(%s)", n.kind, n.name)
	}
	return n.kind
}

// TestInvariant_EveryBranchMatchTierIsDriven derives the rank set from
// branchMatchTiers rather than listing it, so a rank added there without a
// corpus shape fails here instead of shipping unexercised. It also asserts each
// rank actually ANSWERS for some cell: a rank whose writerName never returns a
// registered key is present in source and absent from behavior.
func TestInvariant_EveryBranchMatchTierIsDriven(t *testing.T) {
	answered := make([]int, len(branchMatchTiers))
	for _, readerText := range branchMatchCorpus() {
		r := MustParse(readerText).node
		lk := newReaderBranchLookup(r)
		for _, writerText := range branchMatchWriters() {
			w := MustParse(writerText).node
			for ti, tier := range branchMatchTiers {
				name, ok := tier.writerName(w)
				if !ok {
					continue
				}
				if _, hit := lk.byTier[ti][branchMatchKey{kind: w.kind, name: name, size: branchSizeKey(w)}]; hit {
					answered[ti]++
				}
			}
		}
	}
	for ti, tier := range branchMatchTiers {
		if answered[ti] == 0 {
			t.Errorf("rank %q (branchMatchTiers[%d]) answers no corpus cell — add a reader/writer pair that it selects, or the rank is unexercised", tier.name, ti)
		}
	}
	// The promotion rank has no tier entry (it is keyed by kind alone), so its
	// vocabulary is checked against the table it derives from.
	if len(promotionTargetKinds) == 0 {
		t.Fatal("promotionTargetKinds derived nothing from the promotions table")
	}
	for writerKind, readerKinds := range promotionTargetKinds {
		for _, readerKind := range readerKinds {
			if promotionDeser(writerKind, readerKind) == nil {
				t.Errorf("promotionTargetKinds says %s→%s but the promotions table has no such entry", writerKind, readerKind)
			}
		}
	}
	for key := range promotions {
		writerKind, readerKind, _ := strings.Cut(key, ">")
		found := false
		for _, k := range promotionTargetKinds[writerKind] {
			if k == readerKind {
				found = true
			}
		}
		if !found {
			t.Errorf("the promotions table has %s but promotionTargetKinds dropped it — the promotion rank would never select that reader kind", key)
		}
	}
}

// TestRegression_UnionBranchSelectionSurvivesIndexing pins the individual
// verdicts the ranks exist to produce, so a future change that collapses two
// ranks fails with the shape it broke rather than only as a corpus diff.
func TestRegression_UnionBranchSelectionSurvivesIndexing(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reader string
		writer string
		want   string // the selected branch, or "" for no match
	}{
		{"exact fullname beats same-short-name sibling",
			`[{"type":"record","name":"b.R","fields":[]},{"type":"record","name":"a.R","fields":[]}]`,
			`{"type":"record","name":"a.R","fields":[]}`, "record(a.R)"},
		{"unqualified short name when no fullname matches",
			`[{"type":"record","name":"b.R","fields":[]}]`,
			`{"type":"record","name":"c.R","fields":[]}`, "record(b.R)"},
		{"qualified alias matches the writer fullname exactly",
			`[{"type":"record","name":"a.Q","aliases":["a.R"],"fields":[]}]`,
			`{"type":"record","name":"a.R","fields":[]}`, "record(a.Q)"},
		{"bare alias short-matches across namespaces",
			`[{"type":"record","name":"a.Q","aliases":["R"],"fields":[]}]`,
			`{"type":"record","name":"z.R","fields":[]}`, "record(a.Q)"},
		{"fixed size is part of the match, so selection skips past a wrong-size sibling",
			`[{"type":"fixed","name":"b.F","size":8},{"type":"fixed","name":"a.F","size":4}]`,
			`{"type":"fixed","name":"c.F","size":4}`, "fixed(a.F)"},
		{"no fixed of the writer's size matches at all",
			`[{"type":"fixed","name":"b.F","size":8}]`,
			`{"type":"fixed","name":"c.F","size":2}`, ""},
		{"promotion is the last rank",
			`["long","double"]`, `"int"`, "long"},
		{"promotion takes the earliest promotable branch",
			`["double","long"]`, `"int"`, "double"},
		{"same kind outranks promotion",
			`["long","int"]`, `"int"`, "int"},
		{"a named branch that fails both name ranks does not fall through to promotion",
			`[{"type":"record","name":"q.Q","fields":[]}]`,
			`{"type":"record","name":"a.R","fields":[]}`, ""},
		{"string and bytes promote to each other",
			`["bytes"]`, `"string"`, "bytes"},
		{"an empty union answers nothing",
			`[]`, `"int"`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := MustParse(tc.reader).node
			w := MustParse(tc.writer).node
			got := ""
			if n := newReaderBranchLookup(r).match(w); n != nil {
				got = nodeDesc(n)
			}
			if got != tc.want {
				t.Errorf("selected %q, want %q", got, tc.want)
			}
		})
	}
}
