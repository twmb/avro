package avro

// The predicate-agreement census.
//
// A schema question — "is this branch the null type", "does this Type name
// this definition", "what will json.Marshal emit for this value" — usually
// has to be answered in more than one place, because the same schema exists
// in several REPRESENTATIONS at once: the as-written parse tree (aschema),
// the compiled wire tree (schemaNode), the metadata tree (SchemaNode), the
// pre-Parse `any` tree SchemaFor composes, and the cache's raw JSON tree.
// Every answerer is written by hand, and a hand-written answer is a snapshot
// of the rule at the moment it was typed.
//
// Two failure modes follow, and both have shipped:
//
//   - two answerers of one question DISAGREE, so semantically identical
//     inputs take different paths depending on which representation the code
//     happened to consult;
//   - an answerer restates an EXTERNAL authority's accept-set — the name
//     resolver's binding spellings, encoding/json's key resolver — and the
//     restatement is narrower than the authority, so valid input is refused
//     or an unguarded case panics.
//
// Neither is reachable by generating more INPUTS, which is what every other
// net in this package does: an input matrix is derived from the bug that
// motivated it, so it holds the set of implementations constant and cannot
// see a new one. This census generates over IMPLEMENTATIONS instead. For
// each question it names the canonical predicate (or the external
// authority), every answerer across every representation, and a corpus
// spanning the question's domain; the driver runs every answerer over the
// whole corpus and requires identical verdicts, and where the authority is
// external it is EXECUTED and compared against, never restated.
//
// The drift guard (TestCensus_NoUnregisteredAnswerers) reads the package
// sources and requires every syntactic occurrence of a question's tell to be
// a registered site, so a new hand-written answerer cannot land unexamined.
//
// Adding a predicate, or editing one, means updating this registry. That is
// the point: the registry is the list of places an answer can drift.

import (
	"encoding/json"

	"os"
	"path/filepath"
	"strings"
	"testing"
)

// censusAnswerer is one site that answers a question. note is empty when the
// site routes through the question's canonical predicate; a non-empty note
// is the documented reason this site CANNOT collapse into it. An
// undocumented second answerer is a finding, so "" plus a site that does not
// call the canonical predicate is exactly what review should catch.
type censusAnswerer struct {
	repr string // which representation of the schema this site reads
	site string // function or identifier, for the failure message
	file string
	note string
}

// censusTell is a source substring whose every occurrence in non-test code
// must be a registered site of the question. counts is file → number of
// registered occurrences; both a new file and a new occurrence in a known
// file fail the guard.
type censusTell struct {
	pattern string
	counts  map[string]int
}

type censusQuestion struct {
	id        string
	question  string
	authority string // the canonical predicate, or the external authority
	answerers []censusAnswerer
	tells     []censusTell
}

// censusRegistry is the enumeration. It grows one question per commit; the
// floor in the vacuity check below rises with it, so the registry cannot
// silently shrink back.
var censusRegistry = []censusQuestion{
	{
		id:        "Q1",
		question:  "Is this union branch / type the null type?",
		authority: "one predicate per representation: aschema.isNullBranch (as-written), isNullBranchTree (pre-Parse any tree), and the builder-normalized schemaNode.kind (compiled + metadata)",
		answerers: []censusAnswerer{
			{repr: "as-written aschema", site: "aschema.isNullBranch", file: "schema.go"},
			{repr: "pre-Parse any tree", site: "isNullBranchTree", file: "schema_for.go"},
			{
				repr: "compiled schemaNode", site: "kind == \"null\" (json_codec.go, json_decode.go, resolve.go)", file: "json_codec.go",
				note: "different-by-design: the BUILDER normalizes both spellings into schemaNode.kind, so by the time these sites run there is only one spelling left to compare. A shared predicate here would wrap a field read that is already canonical. The census driver is what proves the normalization actually holds — it asks these sites the same question as the two as-written predicates and requires the same answer.",
			},
		},
		tells: []censusTell{
			{pattern: `== "null"`, counts: map[string]int{
				"json_codec.go":  7,
				"json_decode.go": 2,
				"resolve.go":     1,
				"schema_for.go":  2, // both inside isNullBranchTree
				"schema.go":      2, // both inside isNullBranch
			}},
			{pattern: `!= "null"`, counts: map[string]int{
				"json_codec.go": 1,
				// Not answerers of this question, and registered so the guard
				// does not flag them: json_scan.go matches the four bytes of
				// the JSON literal `null` in a VALUE stream, and ocf.go
				// compares a compression CODEC's name.
				"json_scan.go": 1,
				"ocf/ocf.go":   1,
			}},
		},
	},
}

// censusSourceFiles returns every non-test .go file the census scans, as
// paths relative to the package root.
func censusSourceFiles(t *testing.T) []string {
	t.Helper()
	var out []string
	for _, dir := range []string{".", "ocf", "atype"} {
		ents, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("reading %s: %v", dir, err)
		}
		for _, e := range ents {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			p := name
			if dir != "." {
				p = filepath.ToSlash(filepath.Join(dir, name))
			}
			out = append(out, p)
		}
	}
	if len(out) < 25 {
		t.Fatalf("only found %d source files; the scan is not seeing the package", len(out))
	}
	return out
}

// occurrences reports, per file, the line numbers where pattern appears.
func occurrences(t *testing.T, files []string, pattern string) map[string][]int {
	t.Helper()
	found := make(map[string][]int)
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("reading %s: %v", f, err)
		}
		for i, line := range strings.Split(string(b), "\n") {
			for rest, off := line, 0; ; {
				j := strings.Index(rest, pattern)
				if j < 0 {
					break
				}
				found[f] = append(found[f], i+1)
				off += j + len(pattern)
				rest = line[off:]
			}
		}
	}
	return found
}

// TestCensus_NoUnregisteredAnswerers fails when a hand-written answerer of a
// registered question appears outside the registry — a new file, or a new
// occurrence in a file already listed. It also fails when a registered site
// VANISHES, which is the vacuity direction: a tell that matches less than it
// claims means the registry is describing code that no longer exists, and
// every driver keyed to it is guarding nothing.
func TestCensus_NoUnregisteredAnswerers(t *testing.T) {
	files := censusSourceFiles(t)
	for _, q := range censusRegistry {
		if len(q.tells) == 0 {
			t.Errorf("%s (%s) registers no tells, so nothing guards it", q.id, q.question)
		}
		for _, tell := range q.tells {
			found := occurrences(t, files, tell.pattern)
			if len(found) == 0 {
				t.Errorf("%s: tell %q matches nothing in the package — the registry has rotted", q.id, tell.pattern)
				continue
			}
			for file, lines := range found {
				want, registered := tell.counts[file]
				if !registered {
					t.Errorf("%s: tell %q appears in UNREGISTERED file %s (lines %v).\n  A new site answering %q must be added to the census registry with its representation, and either routed through %s or given the documented reason it cannot be.",
						q.id, tell.pattern, file, lines, q.question, q.authority)
					continue
				}
				if len(lines) != want {
					t.Errorf("%s: tell %q appears %d times in %s (lines %v), registry says %d.\n  If a site was ADDED it must be registered (see %s); if one was REMOVED the count must come down, or this guard is watching code that is gone.",
						q.id, tell.pattern, len(lines), file, lines, want, q.authority)
				}
			}
			for file, want := range tell.counts {
				if len(found[file]) == 0 && want > 0 {
					t.Errorf("%s: tell %q is registered %d times in %s but matches nothing there — the site moved or was deleted, and the registry no longer describes the code",
						q.id, tell.pattern, want, file)
				}
			}
		}
	}
	// The registry must not silently empty out.
	if len(censusRegistry) < 1 {
		t.Fatal("census registry is empty")
	}
}

// ---------------------------------------------------------------------
// Q1 — is this union branch the null type?
// ---------------------------------------------------------------------

// nullSpellingCell is one branch spelling plus the answer every
// representation owes for it.
type nullSpellingCell struct {
	name   string
	branch string // the branch as written inside a union
	isNull bool
}

// The corpus spans the question's domain: both spellings Avro admits for
// null, the wrapped form carrying each kind of inert metadata (Avro defines
// no null logical type, so neither props nor logicalType can make a wrapped
// null stop being null), and near-misses that must answer false — including
// a named type whose name merely contains "null", which a substring-minded
// answerer would get wrong.
var nullSpellingCorpus = []nullSpellingCell{
	{name: "bare", branch: `"null"`, isNull: true},
	{name: "wrapped", branch: `{"type":"null"}`, isNull: true},
	{name: "wrapped-with-props", branch: `{"type":"null","x":1}`, isNull: true},
	{name: "wrapped-with-logicaltype", branch: `{"type":"null","logicalType":"nope"}`, isNull: true},
	{name: "wrapped-with-doc", branch: `{"type":"null","doc":"d"}`, isNull: true},
	{name: "bare-int", branch: `"int"`, isNull: false},
	{name: "wrapped-int", branch: `{"type":"int"}`, isNull: false},
	{name: "bare-string", branch: `"string"`, isNull: false},
	{name: "record", branch: `{"type":"record","name":"R","fields":[]}`, isNull: false},
	{name: "fixed-named-nullish", branch: `{"type":"fixed","name":"nullable","size":1}`, isNull: false},
	{name: "enum-with-null-symbol", branch: `{"type":"enum","name":"E","symbols":["null"]}`, isNull: false},
}

// TestCensus_Q1_NullBranchAgreement runs every representation's answerer
// over the whole corpus and requires identical verdicts. Disagreement here
// is the bug class directly: the wire bytes of the two null spellings are
// IDENTICAL, so nothing in a round-trip net can see the two sides diverge —
// only a derived artifact does (which encoder arm is selected, which branch
// a lift targets, which error identity surfaces).
func TestCensus_Q1_NullBranchAgreement(t *testing.T) {
	for _, cell := range nullSpellingCorpus {
		t.Run(cell.name, func(t *testing.T) {
			// The branch sits second so the union is never null-first,
			// keeping the three representations comparable at a fixed index
			// (a null-first 2-branch union takes a different builder arm).
			// The anchor is boolean because no corpus cell is boolean —
			// a union may not repeat a type.
			text := `["boolean",` + cell.branch + `]`
			const idx = 1

			var tree any
			if err := json.Unmarshal([]byte(text), &tree); err != nil {
				t.Fatalf("corpus cell is not valid JSON: %v", err)
			}
			preParse := isNullBranchTree(tree.([]any)[idx])

			a, err := parseSchemaTree(text)
			if err != nil {
				t.Fatalf("parseSchemaTree: %v", err)
			}
			asWritten := a.union[idx].isNullBranch()

			s, err := Parse(text)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			compiled := s.node.branches[idx].kind == "null"

			got := map[string]bool{
				"pre-Parse any tree":  preParse,
				"as-written aschema":  asWritten,
				"compiled schemaNode": compiled,
			}
			for repr, v := range got {
				if v != cell.isNull {
					t.Errorf("%s answered %v for branch %s, want %v — the representations disagree: %v",
						repr, v, cell.branch, cell.isNull, got)
				}
			}
		})
	}
}

// TestCensus_Q1_CorpusIsNotVacuous proves the corpus actually exercises both
// answers and both spellings; a corpus that drifted to all-true (or all
// bare) would let a broken answerer pass.
func TestCensus_Q1_CorpusIsNotVacuous(t *testing.T) {
	var nulls, nonNulls, wrappedNulls int
	for _, c := range nullSpellingCorpus {
		switch {
		case c.isNull && strings.HasPrefix(c.branch, "{"):
			wrappedNulls++
			nulls++
		case c.isNull:
			nulls++
		default:
			nonNulls++
		}
	}
	if nulls < 2 || nonNulls < 2 || wrappedNulls < 2 {
		t.Fatalf("corpus is too thin to discriminate: %d null (%d wrapped), %d non-null", nulls, wrappedNulls, nonNulls)
	}
}
