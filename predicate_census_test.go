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
	"bytes"
	"encoding/json"
	"errors"

	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
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
	{
		id:        "Q2",
		question:  "What fullname does this named-type DEFINITION occupy?",
		authority: "the wire builder's registration (schema.go, `o.Name = ns + \".\" + o.Name` feeding registerNamed) — every other representation must land on the same string, because the fullname is what a reference binds to",
		answerers: []censusAnswerer{
			{repr: "as-written aschema → compiled schemaNode", site: "builder namespace qualification → schemaNode.name", file: "schema.go"},
			{repr: "metadata SchemaNode", site: "nodeFullname", file: "schema_node.go"},
			{repr: "cache raw JSON tree", site: "nodeFullnameTree", file: "cache.go"},
			{
				repr: "pre-Parse any tree", site: "SchemaFor's namespace joins", file: "schema_for.go",
				note: "not driven by the census: SchemaFor COMPOSES a tree rather than reading one, so it has no definition in hand to ask about. Its output is checked instead by handing the emitted schema to Parse — the authority above — in the SchemaFor round-trip suites.",
			},
		},
		tells: []censusTell{
			{pattern: `+ "." +`, counts: map[string]int{
				"cache.go":       1, // nodeFullnameTree
				"schema_node.go": 1, // nodeFullname
				"schema_for.go":  2, // SchemaFor composition
				"schema.go":      2, // builder qualification (2727); logical key (2165) — not this question
				// Not answerers: an error FIELD PATH join, not a schema name.
				"compat.go": 1,
				"errors.go": 1,
				// Not an answerer: a kind.logicalType lookup key.
				"json_codec.go": 1,
			}},
			{pattern: `+"."+`, counts: map[string]int{
				"schema.go": 1, // scopedRefKeys — the binding side of the same rule
				"cache.go":  1, // dupDefRef — which spelling re-binds after a splice
			}},
		},
	},
	{
		id:       "Q3",
		question: "What does json.Marshal emit as the object key for this Go map key?",
		authority: "EXTERNAL: encoding/json's resolveKeyName. It is executed per corpus cell by " +
			"TestRegression_WalkBudgetMapKeyMatchesJSONKeyResolver, which compares against json.Marshal's " +
			"actual output rather than any restatement of its rules — the whole point, since the two bugs " +
			"in this area were both a restatement that was narrower than the authority",
		answerers: []censusAnswerer{
			{repr: "caller `any` tree, budget walk", site: "mapKeyEmitLen", file: "schema_node.go"},
			{
				repr: "caller `any` tree, fixup + canonicalize walk", site: "canonicalStringKeyMap", file: "schema_node.go",
				note: "different-by-design: this arm asks a NARROWER question — 'do these keys canonicalize to their plain string value', which is true only for the string KIND. A non-string key's object-key form comes from its MarshalText, so such maps stay marshal-opaque and are never rewritten. It must not be collapsed into mapKeyEmitLen, whose question is 'how many bytes does this key cost'; the two agree on the string-kind arm and are deliberately different elsewhere.",
			},
			// SchemaFor's tree canonicalizer (canonicalizeTreeValue,
			// schema_for.go) is NOT a separate answerer: it calls
			// canonicalStringKeyMap. That is the shape this census wants —
			// a second representation consuming the one predicate instead
			// of restating it — and it is recorded here so a later edit
			// that inlines the check reads as the regression it would be.
		},
		tells: []censusTell{
			{pattern: `.Key().Kind()`, counts: map[string]int{
				"schema_node.go": 1, // canonicalStringKeyMap
				// Not answerers of this question: these decide the Go type of
				// an AVRO map's keys for encode/decode/resolve/SchemaFor,
				// where the key form is Avro's own string wire, not a JSON
				// object key.
				"deser.go":       2,
				"json_codec.go":  2,
				"json_decode.go": 2,
				"resolve.go":     1,
				"schema_for.go":  1,
				"ser.go":         3,
			}},
			{pattern: `k.Kind() == reflect.String`, counts: map[string]int{
				"schema_node.go": 1, // mapKeyEmitLen's string-kind arm
			}},
		},
	},
	{
		id:       "Q9",
		question: "What will json.Marshal emit for this caller-supplied value?",
		authority: "EXTERNAL: encoding/json. Executed per cell by TestCensus_Q9_EmissionRouteChargeTracksJSON, " +
			"which compares the walk's charge against json.Marshal's ACTUAL output on the same value. " +
			"This is the authority behind both of the fixes that motivated the census, and the widest " +
			"answerer set in the package: the schema-tree budget models json.Marshal's recursion, and " +
			"anything it fails to model is emitted for free",
		answerers: []censusAnswerer{
			{repr: "caller `any` tree, budget walk", site: "valueWalkLimit + marshalEmitLen + mapKeyEmitLen", file: "schema_node.go"},
			{
				repr: "caller `any` tree, escaped-length scan", site: "jsonEscapedLen / jsonEscapedLenBytes / asciiEscapedLen / avroCodepointEscapedLen / compactedEmitLen", file: "schema_node.go",
				note: "RESTATES the authority rather than delegating, which this census otherwise treats as the bug. Permitted only because delegation is impossible for MEASUREMENT: asking the emitter how long its output is means producing that output, which is the allocation the budget exists to prevent. The licence is conditional on the executed differential over the authority's COMPLETE domain — every one of the 256 single-byte values, the multi-byte runes, invalid UTF-8, the HTML trio and the two-character escapes — derived from marshalSchemaTree, the package's own emitter, so a future SetEscapeHTML(false) moves the expectation and reds this until the restatement follows. Escaping below utf8.RuneSelf is byte-LOCAL, so per-byte totality is a proof over that part of the domain, not a sample of it.",
			},
			{
				repr: "caller `any` tree, fixup detection", site: "treeValueMarshalOpaque / needsJSONFixupKind", file: "schema_node.go",
				note: "different-by-design: asks whether a value's JSON form is SELF-DEFINED (so the fixups must leave it alone), not how many bytes it costs. Same authority, different projection of it — a value can be opaque and cheap, or transparent and huge.",
			},
			{
				repr: "caller `any` tree, canonicalization", site: "canonicalByteSliceKind / sliceElemMarshalPositionDependent", file: "schema_node.go",
				note: "different-by-design: asks whether a value's marshal is INDISTINGUISHABLE from its canonical twin's, so the tree can be rewritten without changing the emission. Mirrors newSliceEncoder's byte-slice rule and json's addressability rule respectively.",
			},
			{
				repr: "SchemaFor pre-Parse tree", site: "deepCopyJSONTree's slice arm", file: "schema_for.go",
				note: "different-by-design: the same canonicalization question on the tree SchemaFor composes. It consults the same jsonMarshalerType/textMarshalerType reflect vars rather than re-deriving the rule.",
			},
		},
		tells: []censusTell{
			{pattern: `jsonMarshalerType`, counts: map[string]int{
				"schema_node.go": 4,
				"schema_for.go":  1,
			}},
			{pattern: `json.Marshaler`, counts: map[string]int{
				"schema_node.go": 6,
				// Not an answerer: a COMMENT recording that aschema is
				// deliberately NOT a json.Marshaler (so the stdlib decoder does
				// not re-scan each nested subtree).
				"schema.go": 1,
			}},
			// Rejected tells, recorded so the next question's design starts
			// from evidence rather than from scratch:
			//   `MarshalText()`        — 4 hits, but reflect.go:161 and
			//     ser.go:1063 are the AVRO ENCODE path (Q13, text-interface
			//     precedence). A tell that spans two questions cannot fail
			//     for one of them.
			//   `encoding.TextMarshaler` — 16 hits across doc.go, reflect.go
			//     and ser.go, dominated by that same encode question. Same
			//     defect, larger.
			//   `reflect.Kind` switches — matches nearly every walker in the
			//     package; a tell that matches everything reports nothing.
			// The usable pair above is narrow because both names exist ONLY
			// to answer "does this type define its own JSON form".
		},
	},
	{
		id:       "Q14",
		question: "Does this registered CustomType match this schema node (kind + logicalType)?",
		authority: "no external authority. hasMatchingCustomTypeCond (schema.go) is the build-time answer that decides " +
			"codec suppression; CustomType.matches is the runtime answer. The two must agree on the BASE rule -- an " +
			"empty field means \"any\", a set field must equal the node's -- and they implement it separately",
		answerers: []censusAnswerer{
			{repr: "build-time suppression", site: "hasMatchingCustomTypeCond", file: "schema.go"},
			{repr: "runtime dispatch", site: "CustomType.matches", file: "custom_type.go"},
		},
		tells: []censusTell{
			{pattern: `.AvroType != "" &&`, counts: map[string]int{
				"schema.go":      1, // hasMatchingCustomTypeCond
				"custom_type.go": 1, // CustomType.matches
			}},
			{pattern: `.LogicalType != "" &&`, counts: map[string]int{
				"schema.go":      1,
				"custom_type.go": 1,
			}},
			// Rejected tell: `matches(` — too generic, and it names only one
			// of the two answerers, so the site most likely to drift away
			// (the hand-rolled clause pair inside the build-time loop) would
			// not be watched at all.
		},
	},
	{
		id:       "Q5",
		question: "Does a field-level decimal lift CONSUME precision/scale — so a malformed body must reject rather than ride to Props?",
		authority: "decimalConsumesPrecisionScale (schema_node.go) owns the carrier test and both sides delegate to it. " +
			"What is answered TWICE is the NAVIGATION to the lift target: fieldDecimalLiftConsumesPrecisionScale " +
			"computes the verdict, liftFieldLogicalIntoType performs the move, and the verdict's comment claims to " +
			"mirror the lift. They drifted before, on the wrapped-null branch",
		answerers: []censusAnswerer{
			{repr: "as-written aschema, verdict", site: "fieldDecimalLiftConsumesPrecisionScale", file: "schema.go"},
			{repr: "as-written aschema, mutation", site: "liftFieldLogicalIntoType", file: "schema.go"},
			{
				repr: "compiled schemaNode + metadata", site: "decimalConsumesPrecisionScale call sites", file: "schema_node.go",
				note: "not a separate answerer: the shared carrier test, consulted by the render and Props routing. Registered so the guard watches its count — a new hand-rolled bytes/fixed check beside it would be the drift.",
			},
		},
		tells: []censusTell{
			{pattern: `decimalConsumesPrecisionScale`, counts: map[string]int{
				"schema_node.go":  5,
				"schema_parse.go": 2,
				"schema.go":       6,
			}},
			// Rejected tell: `Logical == ""` — 6 hits in schema.go, three of
			// them the lift's closer-to-the-type gates and three unrelated
			// (canonical emission, logical dispatch). It spans two questions,
			// so it can never fail cleanly for either.
		},
	},
	{
		id:       "Q11",
		question: "What IDENTITY does a failure carry — is it errors.As-able to *SemanticError, and what Field path does it report?",
		authority: "no external authority: the contract is doc.go's \"# Errors\" section, and the invariant the " +
			"drivers assert is AGREEMENT between the wire formats. Which identity a family carries is policy; a " +
			"caller who only changes format finding errors.As change its answer is not",
		answerers: []censusAnswerer{
			{repr: "binary encode", site: "ser.go's SemanticError construction + semErr", file: "ser.go"},
			{repr: "JSON encode", site: "json_codec.go's SemanticError construction + semErr", file: "json_codec.go"},
			{repr: "binary decode", site: "deser.go's SemanticError construction + semErr", file: "deser.go"},
			{repr: "JSON decode", site: "json_decode.go's SemanticError construction + semErr", file: "json_decode.go"},
			{
				repr: "unsafe struct fast paths", site: "unsafe.go's SemanticError construction", file: "unsafe.go",
				note: "different-by-design as a SITE, not as an answer: the unsafe paths are a compiled specialization of the reflect ones and must produce the IDENTICAL identity. That is asserted by the dual-path suites rather than here, because a census cell cannot choose which path a decode takes — the builder does, by target shape.",
			},
			{
				repr: "resolved decode", site: "resolve.go + promote.go", file: "resolve.go",
				note: "different-by-design as a SITE: a resolved decode adds writer→reader translation, so its failures include families the natural path has no equivalent of. Its parity with the natural path is the resolved-vs-natural suites' question, not this one.",
			},
		},
		tells: []censusTell{
			{pattern: `&SemanticError{`, counts: map[string]int{
				"ser.go":         32,
				"json_codec.go":  10,
				"deser.go":       30,
				"json_decode.go": 5,
				"unsafe.go":      11,
				"resolve.go":     1,
				"promote.go":     1,
				"reflect.go":     6,
				"custom_type.go": 2,
				"errors.go":      4,
			}},
			// Rejected tell: `semErr(` — it is the CONSTRUCTOR most of these
			// sites call, so counting it double-counts the same answerers and
			// misses the ones that build the struct literally. `&SemanticError{`
			// is the shape a NEW hand-written identity decision takes.
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

// ---------------------------------------------------------------------
// Q2 — what fullname does a named-type definition occupy?
// ---------------------------------------------------------------------

// A definition's fullname is the string a reference has to bind to, so
// every representation that computes one has to land on the same answer.
// The name can arrive dotted, split across a "namespace" attribute,
// inherited from the enclosing scope, or explicitly escaped back to the
// null namespace — and the rules interact (a dotted name outranks both the
// attribute and the enclosing scope).
type fullnameCell struct {
	name     string // the definition's "name" as written
	nsAttr   string // its "namespace" attribute; "" means the key is absent
	hasNSKey bool   // whether "namespace" is present at all (present-and-empty differs)
	lax      bool   // needs an accept-all name validator to parse at all
	want     string
}

var fullnameCorpus = []fullnameCell{
	{name: "Foo", want: "ns.Foo"},                                     // inherited from the enclosing scope
	{name: "Foo", nsAttr: "other", hasNSKey: true, want: "other.Foo"}, // attribute overrides the enclosing scope
	{name: "Foo", nsAttr: "", hasNSKey: true, want: "Foo"},            // present-and-empty: the null-namespace escape
	{name: "x.Foo", want: "x.Foo"},                                    // dotted name outranks the enclosing scope
	{name: "x.Foo", nsAttr: "other", hasNSKey: true, want: "x.Foo"},   // dotted name outranks the attribute
	{name: "a.b.c.Foo", want: "a.b.c.Foo"},                            // multi-component
	// The leading-dot escape is normalized at parse into the
	// null-namespace fullname. In a DEFINITION it carries an empty
	// namespace component, which the strict grammar rejects, so these
	// cells only exist under an accept-all validator — which is exactly
	// where a normalization that only one representation performs would
	// go unnoticed.
	{name: ".Foo", lax: true, want: "Foo"},
	{name: ".Foo", nsAttr: "other", hasNSKey: true, lax: true, want: "Foo"},
}

// TestCensus_Q2_DefinitionFullnameAgreement builds one enclosing schema per
// cell and asks each representation what fullname the inner definition
// occupies. The compiled tree is the authority — it is the name the wire
// builder registers, and therefore the name a reference actually binds to —
// so a metadata or cache-tree answer that differs means a reference resolves
// to one type on the wire and another in the surface that re-emits it.
func TestCensus_Q2_DefinitionFullnameAgreement(t *testing.T) {
	for _, cell := range fullnameCorpus {
		label := cell.name
		if cell.hasNSKey {
			label += "+ns=" + strconv.Quote(cell.nsAttr)
		}
		t.Run(label, func(t *testing.T) {
			inner := `{"type":"record","name":` + strconv.Quote(cell.name)
			if cell.hasNSKey {
				inner += `,"namespace":` + strconv.Quote(cell.nsAttr)
			}
			inner += `,"fields":[{"name":"x","type":"int"}]}`
			// The enclosing record establishes the "ns" scope the inner
			// definition may or may not inherit.
			text := `{"type":"record","name":"ns.Top","fields":[{"name":"a","type":` + inner + `}]}`

			var opts []SchemaOpt
			if cell.lax {
				opts = append(opts, WithLaxNames(func(string) error { return nil }))
			}
			s, err := Parse(text, opts...)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			compiled := s.node.fields[0].node.name

			root := s.Root()
			metadata := nodeFullname(&root.Fields[0].Type)

			var tree any
			if err := json.Unmarshal([]byte(inner), &tree); err != nil {
				t.Fatalf("inner is not valid JSON: %v", err)
			}
			cacheTree := nodeFullnameTree(tree.(map[string]any), "ns")

			got := map[string]string{
				"compiled schemaNode (authority)": compiled,
				"metadata SchemaNode":             metadata,
				"cache raw JSON tree":             cacheTree,
			}
			for repr, v := range got {
				if v != cell.want {
					t.Errorf("%s answered %q for definition %s, want %q — the representations disagree: %v",
						repr, v, inner, cell.want, got)
				}
			}
		})
	}
}

// The corpus must exercise every way a namespace can arrive, or a
// representation that mishandles one of them passes by not being asked.
func TestCensus_Q2_CorpusIsNotVacuous(t *testing.T) {
	var inherited, attr, nullEscape, dotted int
	for _, c := range fullnameCorpus {
		switch {
		case strings.HasPrefix(c.name, "."):
			nullEscape++
		case strings.Contains(c.name, "."):
			dotted++
		case c.hasNSKey && c.nsAttr == "":
			nullEscape++
		case c.hasNSKey:
			attr++
		default:
			inherited++
		}
	}
	if inherited < 1 || attr < 1 || nullEscape < 2 || dotted < 2 {
		t.Fatalf("corpus misses a namespace arrival form: inherited=%d attr=%d nullEscape=%d dotted=%d", inherited, attr, nullEscape, dotted)
	}
}

// ---------------------------------------------------------------------
// Q9 — what will json.Marshal emit for this value?
// ---------------------------------------------------------------------

// chargedBytes runs the schema-tree budget walk over v and reports how many
// BYTES it charged — the walk's own answer to "how much will json.Marshal
// emit for this".
func chargedBytes(t *testing.T, v any) int {
	t.Helper()
	b := newWalkBudget()
	before := b.bytes
	if r := valueWalkLimit(reflect.ValueOf(v), maxSchemaJSONDepth, &b); r != valueWalkOK {
		t.Fatalf("walk did not complete over %T (code %d) — the corpus cell must fit the budget", v, r)
	}
	return before - b.bytes
}

// emissionRouteCell is one ROUTE by which content reaches json.Marshal's
// output, given as a small value and a larger twin that differs ONLY in how
// much content travels that route. Comparing the two isolates the route: a
// walk that does not model it charges the same for both while json.Marshal
// emits the difference.
type emissionRouteCell struct {
	name         string
	small, large any
	// openRuling, when set, records that this route is KNOWN to under-charge
	// and the question of what to do about it is with the maintainer. Such a
	// cell asserts the under-charge still holds, so whichever way the ruling
	// goes the cell reds and forces this registry to be updated — the
	// disagreement cannot be silently resolved in either direction.
	openRuling string
}

// The openRuling machinery below stays even though no question currently
// uses it: it is how a disagreement with two defensible resolutions gets
// recorded without either leaving the suite red or letting the question
// close itself silently. Its first use was the escaped-vs-content byte
// charge, now FIXED — those cells are ordinary agreement cells again, which
// is the mechanism working as designed.

func emissionRouteCorpus() []emissionRouteCell {
	const (
		lo = 64
		hi = 4096
	)
	big := func(n int) string { return strings.Repeat("v", n) }
	return []emissionRouteCell{
		{name: "plain-string", small: big(lo), large: big(hi)},
		{name: "named-string-kind", small: namedStringKey(big(lo)), large: namedStringKey(big(hi))},
		// json.Marshal ESCAPES a string's contents, so a byte of content is
		// not a byte of output: a control byte becomes \u00XX (six), and the
		// HTML-escaped set becomes < and friends. An all-printable cell
		// cannot see the difference between charging content and charging
		// emission.
		{name: "string-control-bytes", small: strings.Repeat("\x01", lo), large: strings.Repeat("\x01", hi)},
		{name: "string-html-escaped", small: strings.Repeat("<", lo), large: strings.Repeat("<", hi)},
		{name: "string-map-key-control", small: map[string]int{strings.Repeat("\x01", lo): 1}, large: map[string]int{strings.Repeat("\x01", hi): 1}},
		// []byte reaches json.Marshal as the Avro codepoint STRING, not as a
		// byte slice, so its emitted size depends on the byte VALUES: ASCII
		// costs one byte, 0x80-0xFF two (UTF-8), and a control byte six
		// (\u00XX). The walk charges the raw length, so the three classes
		// are separate cells — a single ASCII cell would never see it.
		{name: "byte-slice-ascii", small: []byte(big(lo)), large: []byte(big(hi))},
		{name: "byte-slice-high", small: bytes.Repeat([]byte{0xff}, lo), large: bytes.Repeat([]byte{0xff}, hi)},
		{name: "byte-slice-control", small: bytes.Repeat([]byte{0x01}, lo), large: bytes.Repeat([]byte{0x01}, hi)},
		{name: "string-kind-map-key", small: map[string]int{big(lo): 1}, large: map[string]int{big(hi): 1}},
		{name: "map-value", small: map[string]any{"k": big(lo)}, large: map[string]any{"k": big(hi)}},
		{name: "slice-element", small: []any{big(lo)}, large: []any{big(hi)}},
		{name: "json-marshaler", small: bigJSONMarshaler{n: lo}, large: bigJSONMarshaler{n: hi}},
		{name: "text-marshaler", small: bigTextMarshaler{n: lo}, large: bigTextMarshaler{n: hi}},
		{name: "json-marshaler-in-map", small: map[string]any{"k": bigJSONMarshaler{n: lo}}, large: map[string]any{"k": bigJSONMarshaler{n: hi}}},
		{name: "json-marshaler-in-slice", small: []any{bigJSONMarshaler{n: lo}}, large: []any{bigJSONMarshaler{n: hi}}},
		{name: "text-marshaler-map-key", small: map[textKeyVal]int{{s: big(lo)}: 1}, large: map[textKeyVal]int{{s: big(hi)}: 1}},
		{name: "struct-field-value", small: struct{ F string }{big(lo)}, large: struct{ F string }{big(hi)}},
		// Routes the escape-aware charge newly has to model: a Marshaler's
		// output is re-scanned by the compactor, which expands the HTML trio
		// one byte into six, and a TextMarshaler's output goes through the
		// same string escaper a plain string does.
		{name: "json-marshaler-html", small: htmlJSONMarshaler{n: lo}, large: htmlJSONMarshaler{n: hi}},
		{name: "text-marshaler-control", small: ctrlTextMarshaler{n: lo}, large: ctrlTextMarshaler{n: hi}},
		{name: "text-marshaler-key-control", small: map[textKeyVal]int{{s: strings.Repeat("\x01", lo)}: 1}, large: map[textKeyVal]int{{s: strings.Repeat("\x01", hi)}: 1}},
		{name: "nested-two-levels", small: map[string]any{"a": []any{big(lo)}}, large: map[string]any{"a": []any{big(hi)}}},
	}
}

// TestCensus_Q9_EmissionRouteChargeTracksJSON asserts the walk's model of
// json.Marshal against json.Marshal itself, per route. The budget exists to
// bound what json.Marshal will emit, so for every route the charge must grow
// at least as fast as the real output does: an under-charge means that route
// is FREE, which is precisely how a value with its own MarshalJSON once
// cost one node and zero bytes while emitting megabytes.
//
// Over-charging is allowed (the walk may be conservative); under-charging is
// the bug. The comparison is a DELTA rather than an absolute, because the
// walk deliberately does not charge for structural punctuation — but it
// cannot decline to charge for content without the delta collapsing.
func TestCensus_Q9_EmissionRouteChargeTracksJSON(t *testing.T) {
	for _, cell := range emissionRouteCorpus() {
		t.Run(cell.name, func(t *testing.T) {
			// The authority is json.Marshal of what the pipeline actually
			// hands it: boundedSerializableValue charges the budget and then
			// returns jsonSerializableValue(v), so the fixups (a []byte
			// becoming the Avro codepoint string, ±Inf becoming a literal)
			// are part of the emission the budget is supposed to bound.
			// Marshaling the raw value instead would compare against bytes
			// this package never emits.
			smallOut, err := json.Marshal(jsonSerializableValue(cell.small))
			if err != nil {
				t.Fatalf("authority could not marshal the small twin: %v", err)
			}
			largeOut, err := json.Marshal(jsonSerializableValue(cell.large))
			if err != nil {
				t.Fatalf("authority could not marshal the large twin: %v", err)
			}
			authorityDelta := len(largeOut) - len(smallOut)
			if authorityDelta <= 0 {
				t.Fatalf("corpus cell is not a growth pair: json.Marshal emitted %d then %d bytes", len(smallOut), len(largeOut))
			}

			chargedDelta := chargedBytes(t, cell.large) - chargedBytes(t, cell.small)

			if cell.openRuling != "" {
				// A recorded disagreement. Assert it STILL holds, so that
				// resolving it either way reds this cell and forces the
				// registry to be updated — an open question must not be able
				// to close itself silently.
				if chargedDelta >= authorityDelta {
					t.Errorf("route %q no longer under-charges (json +%d, charged +%d) — the recorded open ruling has been resolved in the code; update the registry and delete openRuling.\n  Recorded question: %s",
						cell.name, authorityDelta, chargedDelta, cell.openRuling)
				} else {
					t.Logf("route %q under-charges by %.1fx (json +%d, charged +%d) — RECORDED OPEN RULING: %s",
						cell.name, float64(authorityDelta)/float64(chargedDelta), authorityDelta, chargedDelta, cell.openRuling)
				}
				return
			}

			if chargedDelta < authorityDelta {
				t.Errorf("route %q is under-charged: json.Marshal emits %d more bytes for the large twin, the walk charged only %d more.\n  A route the budget does not model is emitted for free, so the cap it advertises does not bound json.Marshal's output.",
					cell.name, authorityDelta, chargedDelta)
			}
		})
	}
}

// TestCensus_Q9_CorpusIsNotVacuous proves the corpus reaches the routes that
// matter: the two DELEGATING routes (a value's own MarshalJSON / MarshalText,
// which the structural walk never descends into) and the container positions
// that carry them, plus a non-string map key. Without those, the corpus only
// exercises the plain structural recursion that was never the bug.
func TestCensus_Q9_CorpusIsNotVacuous(t *testing.T) {
	var delegating, nested, mapKey int
	for _, c := range emissionRouteCorpus() {
		switch {
		case strings.Contains(c.name, "map-key"):
			mapKey++
		case strings.Contains(c.name, "-in-"), strings.Contains(c.name, "nested"):
			nested++
			if strings.Contains(c.name, "marshaler") {
				delegating++
			}
		case strings.Contains(c.name, "marshaler"):
			delegating++
		}
	}
	if delegating < 3 || nested < 3 || mapKey < 2 {
		t.Fatalf("corpus misses a route class: delegating=%d nested=%d mapKey=%d", delegating, nested, mapKey)
	}
	q9NotVacuousTail(t)
}

func q9NotVacuousTail(t *testing.T) {
	t.Helper()
	// And the walk must actually be measuring something: a cell whose charge
	// is zero for both twins would pass the delta test vacuously only if the
	// authority delta were zero, which is separately guarded — but a corpus
	// where NOTHING is charged means chargedBytes is broken.
	if got := chargedBytes(t, strings.Repeat("x", 128)); got < 128 {
		t.Fatalf("chargedBytes reports %d for a 128-byte string; the measurement itself is broken", got)
	}
}

// ---------------------------------------------------------------------
// Q11 — what identity does a failure carry?
// ---------------------------------------------------------------------

// A caller's only programmatic handle on a failure is its IDENTITY: whether
// it is errors.As-able to *SemanticError, and what that error's Field path
// says. The same failure reached through the binary and the JSON decoder
// must present the same handle, or `errors.As` succeeds on one wire and
// fails on the other for a caller who only changed format.
//
// The ENCODE half of this question is already driven by
// TestMatrix_EncodeErrorIdentityCensus (encode_error_identity_census_test.go).
// This is the DECODE half, which had only three spot subtests: the same
// schema and the same VALUE presented on both wires, decoded into a Go
// target that cannot hold it, so both decoders reach a target-type failure
// from equivalent input.
type decodeIdentityCell struct {
	name   string
	schema string
	value  any // encoded to both wires with the schema itself
	target func() any
}

func decodeIdentityCorpus() []decodeIdentityCell {
	return []decodeIdentityCell{
		{"int-into-string", `"int"`, int32(1), func() any { return new(string) }},
		{"string-into-int", `"string"`, "s", func() any { return new(int32) }},
		{"bool-into-string", `"boolean"`, true, func() any { return new(string) }},
		{"double-into-string", `"double"`, 1.5, func() any { return new(string) }},
		{"bytes-into-int", `"bytes"`, []byte{1}, func() any { return new(int32) }},
		{
			"array-into-string", `{"type":"array","items":"int"}`,
			[]any{int32(1)}, func() any { return new(string) },
		},
		{
			"map-into-int", `{"type":"map","values":"int"}`,
			map[string]any{"k": int32(1)}, func() any { return new(int32) },
		},
		{
			"record-into-int",
			`{"type":"record","name":"R","fields":[{"name":"f","type":"int"}]}`,
			map[string]any{"f": int32(1)}, func() any { return new(int32) },
		},
		{
			// NOT *int32: decoding an enum into an integer target is the
			// documented ORDINAL behavior and succeeds on both wires, so it
			// is not a failure family at all. A bool cannot hold a symbol
			// under any rule.
			"enum-into-bool",
			`{"type":"enum","name":"E","symbols":["A","B"]}`,
			"A", func() any { return new(bool) },
		},
		{
			"fixed-into-int",
			`{"type":"fixed","name":"F","size":2}`,
			[]byte{1, 2}, func() any { return new(int32) },
		},
		{
			"record-field-into-string",
			`{"type":"record","name":"R","fields":[{"name":"f","type":"int"}]}`,
			map[string]any{"f": int32(1)},
			func() any {
				return new(struct {
					F string `avro:"f"`
				})
			},
		},
	}
}

// identityOf reduces an error to the handle a caller can act on.
func identityOf(err error) (semantic bool, field string) {
	var se *SemanticError
	if errors.As(err, &se) {
		return true, se.Field
	}
	return false, ""
}

// TestCensus_Q11_DecodeFailureIdentityAgreesAcrossWires presents the same
// value on both wires and decodes it into a target that cannot hold it. The
// invariant is AGREEMENT, not a particular verdict: which identity a family
// carries is policy (doc.go "# Errors"), but a caller who switches format
// must not find errors.As changing its answer.
func TestCensus_Q11_DecodeFailureIdentityAgreesAcrossWires(t *testing.T) {
	for _, cell := range decodeIdentityCorpus() {
		t.Run(cell.name, func(t *testing.T) {
			s, err := Parse(cell.schema)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			bin, err := s.Encode(cell.value)
			if err != nil {
				t.Fatalf("corpus value does not encode to binary: %v", err)
			}
			jsn, err := s.EncodeJSON(cell.value)
			if err != nil {
				t.Fatalf("corpus value does not encode to JSON: %v", err)
			}

			_, errB := s.Decode(bin, cell.target())
			errJ := s.DecodeJSON(jsn, cell.target())
			if errB == nil || errJ == nil {
				t.Fatalf("both wires must reject this target; binary=%v json=%v", errB, errJ)
			}

			semB, fieldB := identityOf(errB)
			semJ, fieldJ := identityOf(errJ)
			if semB != semJ {
				t.Errorf("identity disagrees across wires: binary errors.As(*SemanticError)=%v, json=%v\n  binary: %v\n  json:   %v\n  A caller who only changed wire format finds errors.As changing its answer.",
					semB, semJ, errB, errJ)
			}
			if semB && semJ && fieldB != fieldJ {
				t.Errorf("SemanticError.Field disagrees across wires: binary=%q json=%q\n  binary: %v\n  json:   %v",
					fieldB, fieldJ, errB, errJ)
			}
		})
	}
}

// The corpus must span both scalar and CONTAINER targets, and must include a
// record-FIELD position: the field path is where the two decoders most
// plausibly diverge, since only one of them threads a field name.
func TestCensus_Q11_CorpusIsNotVacuous(t *testing.T) {
	var containers, fieldPos int
	for _, c := range decodeIdentityCorpus() {
		if strings.HasPrefix(c.name, "array") || strings.HasPrefix(c.name, "map") || strings.HasPrefix(c.name, "record") {
			containers++
		}
		if strings.Contains(c.name, "field") {
			fieldPos++
		}
	}
	if containers < 3 || fieldPos < 1 {
		t.Fatalf("corpus misses a position class: containers=%d fieldPos=%d", containers, fieldPos)
	}
}

// ---------------------------------------------------------------------
// Q9 differential: the escape-length restatement vs the real emitter
// ---------------------------------------------------------------------

// jsonEscapedLen restates encoding/json's escape rules instead of delegating
// to them, because delegation is impossible for MEASUREMENT: asking the
// emitter how long its output is means producing that output, which is the
// allocation the budget exists to prevent. A restatement is only allowed
// with an executed differential over the authority's COMPLETE domain, and
// that is what this is.
//
// Expectations come from marshalSchemaTree — the package's own emitter —
// not from json.Marshal named directly, so if this package ever switches to
// an Encoder with SetEscapeHTML(false) the expected values move with it and
// this test fails until the restatement is updated to match.
//
// Escaping below utf8.RuneSelf is byte-LOCAL: a byte's emitted cost never
// depends on its neighbours. Testing all 256 single-byte values is therefore
// a proof over that part of the domain rather than a sample of it; the
// multi-byte cases are enumerated separately below.
func emittedContentLen(t *testing.T, s string) int {
	t.Helper()
	out, err := marshalSchemaTree(s)
	if err != nil {
		t.Fatalf("the package emitter rejected %q: %v", s, err)
	}
	if len(out) < 2 || out[0] != '"' || out[len(out)-1] != '"' {
		t.Fatalf("emitter produced a non-string form for %q: %s", s, out)
	}
	return len(out) - 2 // strip the quotes; the charge is for content
}

func TestCensus_Q9_EscapedLenMatchesEmitterOverEveryByte(t *testing.T) {
	const noLimit = 1 << 30
	for v := 0; v < 256; v++ {
		s := string([]byte{byte(v)})
		want := emittedContentLen(t, s)
		if got := jsonEscapedLen(s, noLimit); got != want {
			t.Errorf("byte 0x%02x: jsonEscapedLen=%d, emitter wrote %d", v, got, want)
		}
		if got := jsonEscapedLenBytes([]byte{byte(v)}, noLimit); got != want {
			t.Errorf("byte 0x%02x: jsonEscapedLenBytes=%d, emitter wrote %d", v, got, want)
		}
	}
}

func TestCensus_Q9_EscapedLenMatchesEmitterOnMultiByte(t *testing.T) {
	const noLimit = 1 << 30
	for _, s := range []string{
		"", "plain ascii",
		"é",          // 2-byte rune
		"€",          // 3-byte rune
		"\U0001d11e", // 4-byte rune
		" ", " ",     // escaped unconditionally
		"a b c",         // interleaved with plain text
		"<>&", `"`, `\`, // the HTML trio and the two-character escapes
		"\x00\x01\x1f", // control run
		"\n\r\t\b\f",   // the named escapes
		"\x80",         // lone continuation byte: invalid UTF-8
		"\xe2\x80",     // truncated 3-byte sequence
		"\xff\xfe",     // never-valid bytes
		"a\x80b",       // invalid byte between valid ones
		"\xf0\x9f\x92", // truncated 4-byte sequence
		"héllo <world> & \"quotes\"\n",
	} {
		want := emittedContentLen(t, s)
		if got := jsonEscapedLen(s, noLimit); got != want {
			t.Errorf("%q: jsonEscapedLen=%d, emitter wrote %d", s, got, want)
		}
		if got := jsonEscapedLenBytes([]byte(s), noLimit); got != want {
			t.Errorf("%q: jsonEscapedLenBytes=%d, emitter wrote %d", s, got, want)
		}
	}
}

// The []byte arm charges the value's json-FACING image — the Avro codepoint
// string the fixup produces — so its differential runs through that fixup.
func TestCensus_Q9_CodepointEscapedLenMatchesEmitterOverEveryByte(t *testing.T) {
	const noLimit = 1 << 30
	for v := 0; v < 256; v++ {
		raw := []byte{byte(v)}
		fixed, ok := jsonSerializableValue(raw).(string)
		if !ok {
			t.Fatalf("byte 0x%02x: the fixup did not produce a string", v)
		}
		want := emittedContentLen(t, fixed)
		if got := avroCodepointEscapedLen(reflect.ValueOf(raw), noLimit); got != want {
			t.Errorf("byte 0x%02x: avroCodepointEscapedLen=%d, emitter wrote %d for the codepoint form", v, got, want)
		}
	}
}

// The early exit is what makes the scan bounded by the BUDGET rather than by
// the input. Proven deterministically rather than by timing: escaping never
// shrinks, so the running total passes the limit within limit+1 input bytes,
// and the returned value is therefore the same for inputs of wildly
// different sizes. A scan without the exit would return a total proportional
// to its input.
func TestCensus_Q9_EscapedLenScanIsBoundedByTheLimit(t *testing.T) {
	const limit = 100
	var prev int
	for _, size := range []int{1 << 10, 1 << 16, 1 << 20, 1 << 23} {
		got := jsonEscapedLen(strings.Repeat("\x01", size), limit)
		if got > limit+6 {
			t.Fatalf("size %d: scan returned %d, past limit %d by more than one byte's cost — it did not stop early", size, got, limit)
		}
		if prev != 0 && got != prev {
			t.Fatalf("size %d returned %d but size before returned %d; the result must not depend on input length once the limit is passed", size, got, prev)
		}
		prev = got
	}
	// And the cost of the whole walk over a value far larger than the budget
	// stays small, since the scan abandons it.
	huge := strings.Repeat("\x01", 32<<20) // 32 MiB of 6x-escaping content
	start := time.Now()
	b := newWalkBudget()
	if r := valueWalkLimit(reflect.ValueOf(huge), maxSchemaJSONDepth, &b); r != valueWalkTooLarge {
		t.Fatalf("a 32 MiB control-byte string must bust the byte budget, got code %d", r)
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("rejecting an over-budget string took %v; the scan is not bounded by the budget", el)
	}
}

// htmlJSONMarshaler emits JSON whose string content is the HTML trio, which
// the compactor expands one byte into six.
type htmlJSONMarshaler struct{ n int }

func (h htmlJSONMarshaler) MarshalJSON() ([]byte, error) {
	return []byte(`"` + strings.Repeat("<", h.n) + `"`), nil
}

// ctrlTextMarshaler returns text the string escaper expands six-fold.
type ctrlTextMarshaler struct{ n int }

func (c ctrlTextMarshaler) MarshalText() ([]byte, error) {
	return []byte(strings.Repeat("\x01", c.n)), nil
}

// ---------------------------------------------------------------------
// Q14 — does this CustomType match this node?
// ---------------------------------------------------------------------

// customMatchCell crosses a CustomType's declared shape with a node's actual
// kind and logical type. The base rule is the same on both sides: an empty
// field means "any", a set field must equal the node's.
type customMatchCell struct {
	ctAvro, ctLogical string
	nodeKind, nodeLog string
}

func customMatchCorpus() []customMatchCell {
	shapes := []struct{ avro, logical string }{
		{"", ""},             // wildcard
		{"bytes", ""},        // kind only
		{"", "decimal"},      // logical only
		{"bytes", "decimal"}, // both
		{"long", "timestamp-millis"},
	}
	nodes := []struct{ kind, log string }{
		{"bytes", "decimal"},
		{"bytes", ""},
		{"long", "timestamp-millis"},
		{"long", ""},
		{"string", "uuid"},
	}
	var out []customMatchCell
	for _, sh := range shapes {
		for _, n := range nodes {
			out = append(out, customMatchCell{sh.avro, sh.logical, n.kind, n.log})
		}
	}
	return out
}

// TestCensus_Q14_CustomTypeMatchAgreesAcrossAnswerers runs both answerers
// over the cross. They must agree everywhere except on the WILDCARD, where
// the build-time answerer deliberately declines: a CustomType that names
// neither a kind nor a logical type must not suppress the built-in handlers,
// because it decides per value at runtime via ErrSkipCustomType. That
// exception is asserted explicitly rather than skipped, so if the build-time
// side ever stops excluding wildcards this cell reds.
func TestCensus_Q14_CustomTypeMatchAgreesAcrossAnswerers(t *testing.T) {
	for _, c := range customMatchCorpus() {
		name := "ct(" + c.ctAvro + "/" + c.ctLogical + ")-node(" + c.nodeKind + "/" + c.nodeLog + ")"
		t.Run(name, func(t *testing.T) {
			ct := CustomType{AvroType: c.ctAvro, LogicalType: c.ctLogical}
			runtimeAns := ct.matches(&schemaNode{kind: c.nodeKind, logical: c.nodeLog})

			b := &builder{customTypes: []CustomType{ct}}
			buildAns := b.hasMatchingCustomTypeCond(c.nodeKind, c.nodeLog, false)

			isWildcard := c.ctAvro == "" && c.ctLogical == ""
			if isWildcard {
				if !runtimeAns {
					t.Errorf("a wildcard CustomType must match at RUNTIME (it decides per value via ErrSkipCustomType), got false")
				}
				if buildAns {
					t.Errorf("a wildcard CustomType must NOT suppress built-in handlers at BUILD time, got true")
				}
				return
			}
			if runtimeAns != buildAns {
				t.Errorf("the two answerers disagree: CustomType.matches=%v, hasMatchingCustomTypeCond=%v.\n  The base rule (empty means any, set must equal) is implemented twice; a node reaching one answer at build time and the other at runtime is wired to a codec that does not handle it.",
					runtimeAns, buildAns)
			}
		})
	}
}

// The cross must contain cells that answer BOTH ways on each axis, or the
// agreement is trivial.
func TestCensus_Q14_CorpusIsNotVacuous(t *testing.T) {
	var match, nonMatch, wildcard int
	for _, c := range customMatchCorpus() {
		ct := CustomType{AvroType: c.ctAvro, LogicalType: c.ctLogical}
		switch {
		case c.ctAvro == "" && c.ctLogical == "":
			wildcard++
		case ct.matches(&schemaNode{kind: c.nodeKind, logical: c.nodeLog}):
			match++
		default:
			nonMatch++
		}
	}
	if match < 3 || nonMatch < 3 || wildcard < 3 {
		t.Fatalf("corpus is trivial: match=%d nonMatch=%d wildcard=%d", match, nonMatch, wildcard)
	}
}

// ---------------------------------------------------------------------
// Q5 — does a field-level decimal lift CONSUME precision/scale?
// ---------------------------------------------------------------------

// Two answerers navigate the field's type to find where a field-level
// logicalType lands. fieldDecimalLiftConsumesPrecisionScale decides whether
// the pair is CONSUMED — which makes a malformed body reject loudly instead
// of riding to Props — and liftFieldLogicalIntoType decides where the
// annotation actually goes. The verdict's own comment says it mirrors the
// lift, and that is a claim: they have drifted before, when one skipped a
// wrapped null branch and the other did not.
//
// Both run inside parseSchemaTree, so neither is callable on a pre-lift
// tree. Each is observed through the consequence it owns instead: the
// verdict through whether a MALFORMED pair rejects, the lift through where
// the parsed metadata ended up carrying the decimal annotation.
type liftTargetCell struct {
	name      string
	fieldType string // the field's "type" as written
	// byDesign, when set, is the documented reason the two navigations
	// deliberately disagree here. Such a cell asserts BOTH directions, so
	// either side changing reds it.
	byDesign string
}

func liftTargetCorpus() []liftTargetCell {
	return []liftTargetCell{
		{name: "bare-bytes", fieldType: `"bytes"`},
		{name: "bare-long-not-a-carrier", fieldType: `"long"`},
		{name: "bare-string-not-a-carrier", fieldType: `"string"`},
		{name: "wrapped-bytes", fieldType: `{"type":"bytes"}`},
		{name: "fixed", fieldType: `{"type":"fixed","name":"F","size":4}`},
		{name: "record-not-a-carrier", fieldType: `{"type":"record","name":"R2","fields":[]}`},
		{name: "union-null-first-bytes", fieldType: `["null","bytes"]`},
		{name: "union-wrapped-null-first-bytes", fieldType: `[{"type":"null"},"bytes"]`},
		{name: "union-null-first-long", fieldType: `["null","long"]`},
		{name: "union-bytes-first", fieldType: `["bytes","null"]`},
		{name: "union-null-then-wrapped-bytes", fieldType: `["null",{"type":"bytes"}]`},
		// The suspect shape: the lift's "closer-to-the-type wins" rule
		// declines to overwrite an annotation the target already has, so the
		// field's "decimal" never lands — but the verdict reads the FIELD's
		// logical, not the one that survives.
		{name: "target-already-annotated", fieldType: `{"type":"bytes","logicalType":"uuid"}`, byDesign: annotationIndependent},
		{name: "union-target-already-annotated", fieldType: `["null",{"type":"bytes","logicalType":"uuid"}]`, byDesign: annotationIndependent},
	}
}

// annotationIndependent is the documented reason the two navigations part
// company on a target that already carries its own logicalType: consumption
// follows the lift TARGET's carrier KIND as written, deliberately
// independent of the target's own annotation, while the lift's
// closer-to-the-type rule declines to overwrite that annotation. So the pair
// is validated as decimal parameters on a carrier the field's "decimal"
// never reaches. Ruled and pinned (NOT_BUGS #71,
// TestRegression_FieldDecimalConsumedMalformedParamReject's
// union-annotated-carrier cell); the alternative would treat a malformed
// scale as absent and parse as decimal(p,0), a silent wire-semantics change.
const annotationIndependent = "consumption follows the lift target's carrier KIND as written, independent of the target's own annotation (NOT_BUGS #71)"

func liftFieldSchema(fieldType, precision string) string {
	return `{"type":"record","name":"R","fields":[{"name":"f","type":` + fieldType +
		`,"logicalType":"decimal","precision":` + precision + `,"scale":2}]}`
}

// consumedByRejection asks the VERDICT's question: a malformed precision
// body rejects only where the pair is consumed.
//
// "Parse failed" is NOT the signal — it is confounded. When the pair is
// unconsumed the malformed value is dropped, and if the lift still put a
// decimal annotation on a carrier, the TYPE-level decimal validation then
// fails for a missing precision instead. Both paths return non-nil. The
// discriminator is the field gate's own message, which names the key it
// refused; that is the only error the verdict itself produces.
func consumedByRejection(t *testing.T, fieldType string) bool {
	t.Helper()
	if _, err := Parse(liftFieldSchema(fieldType, "4")); err != nil {
		t.Fatalf("the valid control must parse for %s: %v", fieldType, err)
	}
	_, err := Parse(liftFieldSchema(fieldType, "3.7"))
	return err != nil && strings.Contains(err.Error(), `record field "precision"`)
}

// consumedByLift asks the LIFT's question: after parsing, is the field's
// decimal annotation actually sitting on a bytes/fixed carrier — the only
// place precision/scale mean anything?
func consumedByLift(t *testing.T, fieldType string) bool {
	t.Helper()
	s, err := Parse(liftFieldSchema(fieldType, "4"))
	if err != nil {
		t.Fatalf("valid control: %v", err)
	}
	// The COMPILED tree is where the lift's effect lives: the metadata tree
	// preserves the schema as written, keeping the field-level annotation on
	// the FIELD, so reading it would report that no lift ever happened.
	target := s.node.fields[0].node
	// A union's annotation lands on its first non-null branch.
	if len(target.branches) > 0 {
		for _, br := range target.branches {
			if br.kind != "null" {
				target = br
				break
			}
		}
	}
	return decimalConsumesPrecisionScale(target.kind, target.logical)
}

// TestCensus_Q5_DecimalLiftConsumeVerdictMatchesWhereTheLiftLanded requires
// the two navigations to agree. A verdict of "consumed" over a target the
// lift did not annotate as decimal means a malformed pair rejects loudly on
// a schema where the pair is inert metadata — the opposite of the
// unconsumed-is-a-custom-property rule.
func TestCensus_Q5_DecimalLiftConsumeVerdictMatchesWhereTheLiftLanded(t *testing.T) {
	for _, cell := range liftTargetCorpus() {
		t.Run(cell.name, func(t *testing.T) {
			byVerdict := consumedByRejection(t, cell.fieldType)
			byLift := consumedByLift(t, cell.fieldType)

			if cell.byDesign != "" {
				// Both directions asserted, so either side changing reds this.
				if !byVerdict {
					t.Errorf("the verdict must still CONSUME here (a malformed pair must reject): %s", cell.byDesign)
				}
				if byLift {
					t.Errorf("the lift must still DECLINE to overwrite the target's own annotation: %s", cell.byDesign)
				}
				return
			}

			if byVerdict != byLift {
				t.Errorf("the two navigations disagree for field type %s:\n  verdict (malformed pair rejects) = %v\n  lift    (decimal landed on a carrier) = %v\n  One of them is reading a type the other never addressed.",
					cell.fieldType, byVerdict, byLift)
			}
		})
	}
}

// The corpus must contain carriers AND non-carriers, both bare and wrapped,
// unions with both null spellings, and a target that already carries its own
// annotation — otherwise the navigation is never actually exercised.
func TestCensus_Q5_CorpusIsNotVacuous(t *testing.T) {
	var unions, wrappedNull, preAnnotated, nonCarrier int
	for _, c := range liftTargetCorpus() {
		if strings.HasPrefix(c.fieldType, "[") {
			unions++
		}
		if strings.Contains(c.fieldType, `{"type":"null"}`) {
			wrappedNull++
		}
		if strings.Contains(c.fieldType, `"logicalType"`) {
			preAnnotated++
		}
		if strings.Contains(c.name, "not-a-carrier") {
			nonCarrier++
		}
	}
	if unions < 4 || wrappedNull < 1 || preAnnotated < 2 || nonCarrier < 3 {
		t.Fatalf("corpus misses a navigation class: unions=%d wrappedNull=%d preAnnotated=%d nonCarrier=%d",
			unions, wrappedNull, preAnnotated, nonCarrier)
	}
}
