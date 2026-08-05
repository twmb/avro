package avro

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// Wall-clock assertions have to account for the race detector, and the rule
// for how is stated ONCE here. It used to be stated six times: a shared
// helper applying an absolute FLOOR, a second copy of that floor written
// inline, three hand-written ceilings using three different multipliers
// (6x, 10x, 30x), and a pair of budget constants whose comment claimed to
// mirror the floor and did not. Six statements of one rule agree only until
// one of them is edited, and the floor is the form that fails silently:
// raising a cell's NORMAL bound — which is done precisely when its
// legitimate cost is large — shrinks the headroom the floor leaves it,
// until the cell reds on correct code.
//
// So the rule is a MULTIPLIER with an absolute floor under it, and both
// numbers live here. A cell asks; nothing restates.

// raceCostMultiplier is how much the detector inflates this suite's own timed
// work. It is MEASURED, not chosen: running the DoS battery with and without
// -race gives per-cell ratios of 2.3x (C1 deep nesting), 2.9x (C9 custom-type
// parse), 3.3x (C10a union-tag breadth), 5.0x (C5 error echo), 6.0x (C8 direct
// byte APIs), 6.1x (C10b field lookup, C10d sibling kinds), 6.3x (C3 number
// CPU) and 8.3x (C10c wide-record surfaces); a per-call measurement of the
// widest parse cell gives 6.2x. Ten covers the measured maximum with margin
// and matches what the suite's older hand-written relaxations assumed
// ("race adds 5-10x").
//
// It stays far below what it has to stay below. The class these ceilings
// separate is a complexity CHANGE, and the quadratic the widest cell exists
// to catch measured 1.9s to 32s unraced at that size — so even multiplied,
// the ceiling sits more than 2x from the healthy cost on one side and the
// broken cost on the other.
const raceCostMultiplier = 10

// raceCeilingFloor is the headroom a timed cell gets under -race no matter how
// tight its normal bound. A cell asserting a microsecond reject needs ABSOLUTE
// headroom, not proportional: ten times a 100ms bound is still a second, and
// process startup, GC and host load are not proportional to the work. The
// floor is what serves those cells; the multiplier is what serves the cells
// whose legitimate cost is already large. Taking the larger of the two is what
// lets one rule serve both, and it is why this change loosens nothing below
// a 300ms normal bound.
const raceCeilingFloor = 3 * time.Second

// raceRelaxed returns the wall-clock CEILING to enforce for a normal bound.
// It never tightens: the result is >= normal in every mode, since the
// multiplier is >= 1 and the floor only ever raises.
func raceRelaxed(normal time.Duration) time.Duration {
	if !raceEnabled {
		return normal
	}
	return max(raceCeilingFloor, raceCostMultiplier*normal)
}

// raceInflated scales a MEASURED-cost allowance by the same inflation, with no
// absolute floor. The distinction from raceRelaxed is the question being
// asked. A ceiling asks "is this cost acceptable", so a generous absolute
// minimum is harmless. A scale-comparison floor asks "is this cost FLAT", and
// an absolute 3s floor would swallow the comparison whole — every cell would
// pass by measuring nothing. Proportional inflation is the only correct
// relaxation for a comparison.
func raceInflated(allowance time.Duration) time.Duration {
	if !raceEnabled {
		return allowance
	}
	return raceCostMultiplier * allowance
}

// hangDeadline is the wall-clock backstop the schema-node budget batteries use
// to turn a HANG into a failure. It is a liveness detector, never a performance
// assertion: the property under test is that an over-budget walk REJECTS, and
// the goroutine plus deadline exist only so a regression that stopped bounding
// the walk surfaces as a failure instead of wedging the suite.
//
// Those batteries are the one place in the suite whose work is at the budget by
// construction — a cell must EXCEED maxSchemaJSONNodes or nothing is over
// budget — so they are the slowest thing here and the detector multiplies that:
// measured in isolation under -race, two of them run 21s and 33s, and under the
// full suite's parallelism they go higher. It was a build-tagged 30s/4min pair,
// which is the same rule stated an additional time with an additional number;
// it asks the authority now.
var hangDeadline = raceInflated(30 * time.Second)

//////////////////////////////////////////////////////////////////////////////
// The enumeration guard
//////////////////////////////////////////////////////////////////////////////

// raceRelaxation rows one FILE that decides something by asking whether the
// race detector is on. Rows are per file with a site COUNT rather than per
// line, so they do not rot on an unrelated edit while still failing in BOTH
// directions: a new consult raises the count, a removed one lowers it.
type raceRelaxation struct {
	file string
	// sites is how many times the file consults the race predicate.
	sites int
	// kind is what the file does with the answer. "authority" is this file;
	// everything else has to say why it is not simply asking the authority.
	kind string
	why  string
}

// The set is DERIVED from source below, not from this list; this list is what
// the derivation is checked against. A consult that appears in no row fails,
// and a row naming a file that no longer consults fails.
var raceRelaxations = []raceRelaxation{
	{file: "race_bounds_test.go", sites: 3, kind: "authority",
		why: "raceRelaxed and raceInflated — the two forms of the rule and the only place either number appears — plus the invariant that asserts neither ever tightens"},

	{file: "export_test.go", sites: 1, kind: "authority",
		why: "the bridge READS the predicate to hand it to package avro_test, so the two packages share one build-tagged mechanism instead of declaring one each"},

	{file: "conformance_test.go", sites: 3, kind: "authority+skip",
		why: "isRaceEnabled forwards the bridged value (1); the two remaining consults SKIP their budgets rather than relax them, each for a reason recorded at the site. Every wall-clock CEILING in this file asks raceRelaxed — three of them used to compute their own, with three different multipliers"},

	{file: "audit_regression_test.go", sites: 1, kind: "skip",
		why: "SKIPS its 2s deep-schema-reject budget under -race rather than relaxing it. A skip is a different decision from a ceiling and is kept as one: the quadratic it guards is seconds in the untraced run, which always executes"},

	{file: "error_bound_test.go", sites: 1, kind: "skip",
		why: "SKIPS a growth-RATIO check (linear lands near 2, quadratic near 4). Instrumentation distorts the ratio itself, not just the magnitude, so no multiplier can correct it; the absolute ceilings in the same test still run under -race"},
}

// raceConstrained reports whether src carries a build constraint mentioning
// the race tag. Matched on a word boundary so an unrelated tag containing the
// letters (a "trace" build, say) is not mistaken for one.
var raceTagRE = regexp.MustCompile(`\brace\b`)

// declaredNameRE captures the identifier a line DECLARES, so an occurrence can
// be told from the declaration it names.
var declaredNameRE = regexp.MustCompile(`^\s*(?:const|var|func)\s+([A-Za-z_][A-Za-z0-9_]*)`)

func raceConstrained(src string) bool {
	for line := range strings.SplitSeq(src, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "//go:build") {
			if line != "" && !strings.HasPrefix(line, "//") {
				return false // past the header
			}
			continue
		}
		return raceTagRE.MatchString(line)
	}
	return false
}

// raceAnswerers derives the identifiers that ANSWER "is the detector on".
//
// The predicate is identified by its defining SHAPE, not by which file it sits
// in: a boolean declared `true` under a race build constraint and `false` under
// the negated one. Keying on the file instead was the first thing tried here
// and it was wrong in the expensive direction — it swept in every constant of
// any race-tagged file, so an unrelated build-tagged DURATION became an
// "answerer" and the guard reported five phantom consults in a file that
// mentions none of this. A predicate is a build-switched BOOL; that is the rule.
//
// The set is then closed transitively over the two ways an answer is passed on:
// an identifier declared equal to an answerer (the bridge that hands the value
// to the other test package), and a niladic bool function returning one (the
// wrapper, including a qualified `pkg.Ident` form). Without the closure a
// consult can hide one alias-hop from the declaration, which is exactly where
// the two test packages put theirs.
func raceAnswerers(t *testing.T, files []string) map[string]bool {
	t.Helper()
	boolDecl := regexp.MustCompile(`(?m)^(?:const|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(true|false)\s*$`)
	sawTrue, sawFalse := map[string]bool{}, map[string]bool{}
	for _, f := range files {
		src := readFile(t, f)
		if !raceConstrained(src) {
			continue
		}
		for _, m := range boolDecl.FindAllStringSubmatch(blankCode(src), -1) {
			if m[2] == "true" {
				sawTrue[m[1]] = true
			} else {
				sawFalse[m[1]] = true
			}
		}
	}
	out := map[string]bool{}
	for name := range sawTrue {
		if sawFalse[name] {
			out[name] = true
		}
	}
	if len(out) == 0 {
		t.Fatal("derived no build-switched boolean predicate — the derivation broke, and a broken derivation reads as full coverage")
	}
	aliasDecl := regexp.MustCompile(`(?m)^(?:const|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?:[A-Za-z_][A-Za-z0-9_]*\.)?([A-Za-z_][A-Za-z0-9_]*)\s*$`)
	wrapDecl := regexp.MustCompile(`(?m)^func ([A-Za-z_][A-Za-z0-9_]*)\(\) bool \{\s*\n\s*return (?:[A-Za-z_][A-Za-z0-9_]*\.)?([A-Za-z_][A-Za-z0-9_]*)\s*\n\}`)
	for grew := true; grew; {
		grew = false
		for _, f := range files {
			code := blankCode(readFile(t, f))
			for _, re := range []*regexp.Regexp{aliasDecl, wrapDecl} {
				for _, m := range re.FindAllStringSubmatch(code, -1) {
					if out[m[2]] && !out[m[1]] {
						out[m[1]] = true
						grew = true
					}
				}
			}
		}
	}
	return out
}

// TestInvariant_EveryRaceRelaxationIsRowed derives every place the suite
// decides something by asking whether the race detector is on, and requires
// each to be rowed.
//
// SCOPE OF THIS DERIVATION, stated because it has one. It finds occurrences of
// the identifiers that ANSWER the question — derived from what the build-tagged
// mechanism files declare, plus any wrapper that returns one of them — inside
// every *_test.go file the module walk reaches. It therefore cannot see a
// wall-clock bound relaxed by any other means: a bound chosen generously enough
// that -race never trips it, one keyed on GOMAXPROCS or an environment
// variable, one hidden behind a build tag of its own, or a cell that simply
// does not assert a time. Those are outside what this guard can promise, and
// the promise is worth only what it names.
func TestInvariant_EveryRaceRelaxationIsRowed(t *testing.T) {
	files := moduleTestFiles(t)
	answerers := raceAnswerers(t, files)

	rowed := map[string]raceRelaxation{}
	for _, r := range raceRelaxations {
		if _, dup := rowed[r.file]; dup {
			t.Errorf("raceRelaxations rows %s twice", r.file)
		}
		if r.why == "" || r.kind == "" {
			t.Errorf("raceRelaxations row for %s states no kind/why — a row that explains nothing is not a classification", r.file)
		}
		rowed[r.file] = r
	}

	counted := map[string]int{}
	for _, f := range files {
		src := readFile(t, f)
		if raceConstrained(src) {
			continue
		}
		code := blankCode(src)
		n := 0
		for id := range answerers {
			// A DECLARATION of an answerer is not a consult of one — the
			// bridge and the wrapper each name one on their own signature
			// line. Only the identifier IN THE DECLARED POSITION is exempt,
			// not the whole line: skipping any line that begins with
			// const/var/func let a genuine consult hide on a declaration line
			// (`var _ = func() bool { if raceEnabled ... }`), and the guard
			// then passed a newly added relaxation — the same shape it exists
			// to catch. Attacking it by ADDING a member is the only reason
			// that surfaced; removing one had always redded.
			for _, loc := range regexp.MustCompile(`\b`+id+`\b`).FindAllStringIndex(code, -1) {
				lineStart := strings.LastIndex(code[:loc[0]], "\n") + 1
				lineEnd := lineStart + strings.IndexByte(code[lineStart:]+"\n", '\n')
				if m := declaredNameRE.FindStringSubmatchIndex(code[lineStart:lineEnd]); m != nil &&
					lineStart+m[2] == loc[0] {
					continue
				}
				n++
			}
		}
		if n > 0 {
			counted[filepath.Base(f)] = n
		}
	}

	for f, n := range counted {
		r, ok := rowed[f]
		if !ok {
			t.Errorf("%s consults the race predicate %d time(s) but is not rowed in raceRelaxations.\nRow it saying what it does with the answer — and if it is a wall-clock ceiling, it should be calling raceRelaxed instead of deciding for itself.", f, n)
			continue
		}
		if r.sites != n {
			t.Errorf("%s consults the race predicate %d time(s), row says %d.\nA changed count is a new decision or a removed one; either way the row has to say which.", f, n, r.sites)
		}
	}
	for f := range rowed {
		if _, ok := counted[f]; !ok {
			t.Errorf("raceRelaxations rows %s, which no longer consults the race predicate — the row rotted", f)
		}
	}
}

// TestInvariant_RaceRelaxationNeverTightens pins the two properties the whole
// scheme rests on, in both build modes, so neither can be lost to a future
// edit of the arithmetic: a relaxation never returns LESS than the bound it
// was given, and under -race a cell whose normal bound is large gets headroom
// proportional to it rather than a fixed ceiling that shrinks as the bound
// grows. The second is the property the floor form did not have, which is why
// it is asserted rather than left to the constant's documentation.
func TestInvariant_RaceRelaxationNeverTightens(t *testing.T) {
	for _, normal := range []time.Duration{
		time.Microsecond, time.Millisecond, 100 * time.Millisecond,
		500 * time.Millisecond, 1500 * time.Millisecond, 4 * time.Second, time.Minute,
	} {
		if got := raceRelaxed(normal); got < normal {
			t.Errorf("raceRelaxed(%v) = %v — tightened", normal, got)
		}
		if got := raceInflated(normal); got < normal {
			t.Errorf("raceInflated(%v) = %v — tightened", normal, got)
		}
	}
	if !raceEnabled {
		for _, normal := range []time.Duration{time.Millisecond, time.Minute} {
			if got := raceRelaxed(normal); got != normal {
				t.Errorf("raceRelaxed(%v) = %v without -race — the tight bound must stay in effect", normal, got)
			}
		}
		return
	}
	// Under -race, headroom is proportional once the bound is past the floor.
	small, large := 100*time.Millisecond, 10*time.Second
	if ratio := raceRelaxed(large) / large; ratio < 2 {
		t.Errorf("raceRelaxed(%v) leaves only %vx headroom — the ceiling stops scaling with the bound, which is the shape that reds a correct cell", large, ratio)
	}
	if raceRelaxed(small) < raceCeilingFloor {
		t.Errorf("raceRelaxed(%v) = %v — below the absolute floor a tight cell needs", small, raceRelaxed(small))
	}
}

//////////////////////////////////////////////////////////////////////////////
// Shared source derivation
//////////////////////////////////////////////////////////////////////////////

// moduleTestFiles returns every _test.go file in the module, in every package.
//
// SCOPE, stated once for every guard that uses it: the root is the directory
// holding go.mod, found by walking up from the working directory, and the walk
// takes every subdirectory except testdata, vendor and dot-directories. So the
// set is "test files of this module", derived from the filesystem — not a list
// of package directories, which is what it replaced and which is why a guard
// built on it could not see the ocf package at all. What it still cannot see:
// a test in a DIFFERENT module (the oracle harnesses under testdata are
// deliberately excluded), source generated at run time, and anything a
// non-test file does.
func moduleTestFiles(t *testing.T) []string {
	t.Helper()
	root, err := filepath.Abs(".")
	if err != nil {
		t.Fatalf("resolving working directory: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(root)
		if parent == root {
			t.Fatal("no go.mod above the working directory — the module root derivation broke")
		}
		root = parent
	}
	var out []string
	err = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if p != root && (name == "testdata" || name == "vendor" || strings.HasPrefix(name, ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(d.Name(), "_test.go") {
			rel, rerr := filepath.Rel(root, p)
			if rerr != nil {
				return rerr
			}
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking the module: %v", err)
	}
	// A walk that finds one package is a walk that silently lost the others,
	// which is the exact failure this replaced. Require more than one
	// directory to be represented.
	dirs := map[string]bool{}
	for _, f := range out {
		dirs[filepath.Dir(f)] = true
	}
	if len(out) < 25 || len(dirs) < 2 {
		t.Fatalf("module walk found %d test files across %d directories — too few; the walk broke and a broken walk reads as full coverage", len(out), len(dirs))
	}
	return out
}
