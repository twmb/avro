package avro

// Budgeted-walk census.
//
// A parse/resolve/skip/metadata operation's cost is a PRODUCT of factors the
// caller chooses independently — for the min-bytes walk it is
//
//	containers x paths-per-walk x children-per-node,
//
// and a bound that caps any single factor leaves the rest to multiply. Three
// consecutive fixes each capped ONE factor of that one walk (the paths, then the
// children, then the containers) before the shape was seen whole. The lesson is
// not about that walk: it is that EVERY budgeted walk in the package needs its
// cost written as a product and needs one bound that caps the product rather
// than a factor.
//
// This file is that enumeration, made executable. budgetedWalks is the registry:
// one row per walk, naming the state it carries, what it traverses, the factors
// of its cost, and the single bound that caps their product. The guards below
// DERIVE the walk set from source two ways and fail when a walk appears that is
// not rowed — so a fourth factor, or a wholly new walk, cannot land without a
// cost expression:
//
//   - by COST MARKER: a walk that carries an allowance, a walkBudget, a
//     (reader,writer) pair memo, a visited/seen set over a graph node type, or a
//     defer-delete cycle mark. These are the states that bound a graph walk, and
//     every occurrence must belong to a rowed walk or an allow-listed non-walk.
//   - by RECURSION: a function that recurses over the schema graph
//     (*schemaNode / *SchemaNode). These are the walks themselves, marker or not,
//     so a new one with no cost state at all is still caught.
//
// The discriminator that decides whether a bound is enough is WHAT the walk
// traverses:
//
//   - schemaDAG: the shared *schemaNode/*SchemaNode graph, where a named type
//     referenced twice is one node on two paths. Depth cannot bound this (the
//     fan-out is reachable flat); only a MEMO over the nodes, or a BUDGET over
//     the work, caps the paths factor. Every schemaDAG row must name one.
//   - goTypeDAG: a reflect.Type graph. Same DAG shape, but the type is fixed at
//     COMPILE time and its result is amortized by a per-type sync.Map, so the
//     bound is that it is not attacker-grown at runtime (see G3).
//   - valueTree / wire / textTree: a caller VALUE, the wire bytes, or the schema
//     TEXT — none of which shares sub-structure, so node count IS input size and
//     a depth cap plus the input length bounds the walk. No product hides here.

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

type walkClass string

const (
	schemaDAG walkClass = "schemaDAG" // shared *schemaNode/*SchemaNode graph
	goTypeDAG walkClass = "goTypeDAG" // reflect.Type graph, compile-time
	valueTree walkClass = "valueTree" // a caller value (no sharing)
	textTree  walkClass = "textTree"  // schema text / parsed aschema (no sharing)
)

// Wire-decode/skip walks (deserRecord, deserArray, skipRecord, decodeValue, ...)
// are a fourth, uniform class not rowed individually: each consumes at least one
// wire byte per node and is capped by sl.depth>=maxDepth, so its node count is
// the input length and no caller-chosen product hides in it. The one place a
// wire walk touches a schema-graph cost — the skip compiler asking a per-element
// minimum — is the minBytes row, reached through skip's shared walk.

// budgetedWalk is one recursive traversal that carries cost-limiting state.
type budgetedWalk struct {
	fn      string    // the function or method the recursion is named by
	file    string    // where it lives
	class   walkClass // what it traverses — decides whether its bound is enough
	factors string    // the cost as a product of caller-chosen magnitudes
	binds   string    // the single bound that caps the PRODUCT, not one factor
	// reachingPaths names every CONSTRUCTION PATH that reaches this walk and the
	// bound each carries. One walk can be reached by more than one path (the
	// min-bytes walk is reached at build, at finalize, at resolve, and at skip),
	// and a bound that holds on one path but not another is invisible to the rows
	// above — factors/binds describe the walk, not the paths. For the multi-path
	// walks a guard derives the paths from source and checks each is bounded
	// (TestInvariant_MinBytesReachingPaths); a single-construction walk names its
	// one entry.
	reachingPaths string
}

// budgetedWalks is the registry. Every recursive schema-graph walk and every
// cost-marker-bearing walk in the package must appear here; the guards derive
// both sets from source and diff them against this list.
var budgetedWalks = []budgetedWalk{
	// ---- schemaDAG: must bind the paths factor with a memo or a budget ----
	{fn: "minBytes", file: "deser.go", class: schemaDAG,
		factors:       "containers x paths-per-walk x children-per-node",
		binds:         "one minBytesWalk SHARED per operation (containers) + done memo (paths) + per-child allowance charge (children)",
		reachingPaths: "THREE constructions, each shared across one operation: build (b.minBytes on the builder, forward refs fixed in finalize AND backward refs resolved to a fully-built node at build), finalize (one mbw before the container-fixup loop), and resolve (ctx.minBytes) — which also carries the SKIP path, since a dropped field's record compile is deferred to decode time but joins the resolution's walk rather than starting its own. The standalone schemaMinBytes is a fourth, single-node, outside any loop and with no production caller. Guarded by TestInvariant_MinBytesReachingPaths (the set, from source) and TestInvariant_EveryReachingPathBoundIsMeasured (each path's counts, driven at two values)"},
	{fn: "checkCompat", file: "compat.go", class: schemaDAG,
		factors:       "distinct (reader,writer) node pairs x children-per-pair",
		binds:         "the seen map[nodePair]bool memo, threaded from CheckCompatibility with no defer-delete, so each pair is walked once",
		reachingPaths: "one: seen created in CheckCompatibility, threaded through the whole recursive check"},
	{fn: "resolveNode", file: "resolve.go", class: schemaDAG,
		factors:       "distinct (reader,writer) pairs x (children + per-container min-bytes)",
		binds:         "ctx.seen pair memo (pairs) + ctx.minBytes shared walk (the container min-bytes factor)",
		reachingPaths: "one: ctx (seen + minBytes) created in Resolve, threaded through the whole resolution"},
	{fn: "toJSONWalk", file: "schema_node.go", class: schemaDAG,
		factors: "nodes emitted x bytes per node",
		binds: "walkBudget (nodes + bytes), charged by takeNode at the TOP of every entry so a DAG re-descent still spends budget; visited is only cycle detection. " +
			"MEASURED BY TestRegression_SchemaNodeWalkBudgetBattery / _DuplicateNamedDefinitionBounded / TestRegression_SchemaForCustomSchemaBudgetAxes, which hand-build the trees Parse cannot express — " +
			"a PARSED schema is deduped before it reaches this walk, so no parse-driven cell can red this bound, and one that claimed to was renamed",
		reachingPaths: "one walkBudget per metadata-API call (toJSONDedup), from Root().Schema()/String()/Canonical(); each walks the whole tree once"},
	{fn: "collectLocalNames", file: "schema_node.go", class: schemaDAG,
		factors:       "distinct nodes x names per node",
		binds:         "the visited map[*SchemaNode]struct{} memo (mark on entry, return on hit)",
		reachingPaths: "one: visited created per toJSONDedup, one walk of the tree"},
	{fn: "stampNameRefs", file: "schema_node.go", class: schemaDAG,
		factors:       "distinct nodes",
		binds:         "the visited memo (mark on entry, return on hit)",
		reachingPaths: "one: visited created per Root() name-ref stamping pass"},
	{fn: "collectNamedTypes", file: "schema_node.go", class: schemaDAG,
		factors:       "tree nodes (name-ref nodes are leaves, not followed)",
		binds:         "structural: a reference SchemaNode carries no children, so the walk is over the definition TREE, linear in it",
		reachingPaths: "one: table created in fixupNameRefDefaults, per Root()"},
	{fn: "coerceTreeDefaults", file: "schema_node.go", class: schemaDAG,
		factors:       "tree nodes (name-ref nodes are leaves)",
		binds:         "structural: references are leaves, so the walk is over the definition TREE, linear in it",
		reachingPaths: "one: same fixupNameRefDefaults pass, per Root()"},
	{fn: "overlayInheritedCustom", file: "schema.go", class: schemaDAG,
		factors:       "distinct nodes x custom lookups",
		binds:         "the visited map[*schemaNode]bool memo (mark on entry, return on hit)",
		reachingPaths: "one: b.overlayDone created per parse, walked at inherited-custom overlay (build/reference-time)"},
	{fn: "findCustomTypeMatchInSubtreeWalk", file: "schema.go", class: schemaDAG,
		factors:       "distinct nodes x registered custom types",
		binds:         "the visited map[*schemaNode]bool memo (mark on entry, return on hit)",
		reachingPaths: "one: visited created per findCustomTypeMatchInSubtree call at build"},
	{fn: "buildCustomWiring", file: "schema.go", class: schemaDAG,
		factors:       "distinct nodes",
		binds:         "the visited memo (mark on entry, return on hit)",
		reachingPaths: "one: visited created per applyCustomTypes pass at build"},
	{fn: "nodeAwaitsForwardRefSeen", file: "schema.go", class: schemaDAG,
		factors:       "distinct nodes",
		binds:         "the seen map[*schemaNode]struct{} memo (mark on entry, return on hit); the separate building set is defer-delete cycle detection",
		reachingPaths: "one: seen created per nodeAwaitsForwardRef call at build"},

	// ---- goTypeDAG: bound is compile-time fixedness + per-type sync.Map ----
	{fn: "collect", file: "reflect.go", class: goTypeDAG,
		factors:       "2^(embed depth) on a shared-embed type DAG (visited is defer-delete, so it re-descends)",
		binds:         "NOT a runtime bound: a Go type is fixed at COMPILE time and the result is amortized by a per-type sync.Map, so the fan-out is not attacker-grown (G3)",
		reachingPaths: "one: visited created per typeFieldMapping (per Go type; the sync.Map amortizes repeats across calls)"},
	{fn: "collectFieldsRaw", file: "schema_for.go", class: goTypeDAG,
		factors:       "2^(embed depth) on a shared-embed type DAG (visited is defer-delete)",
		binds:         "compile-time fixedness + collectFields' per-call visited; not attacker-grown at runtime (G3)",
		reachingPaths: "one: visited created per collectFields, per SchemaFor of a Go type"},
	{fn: "inferType", file: "schema_for.go", class: goTypeDAG,
		factors:       "type nodes x ptr chains, bounded by depth and maxIndirectDepth",
		binds:         "seen map[reflect.Type]seenForm memo + depth/ptrChain caps; compile-time type",
		reachingPaths: "one: seen created per SchemaFor, per Go type"},

	// ---- valueTree / wire / textTree: node count IS input size ----
	{fn: "walkDefault", file: "schema.go", class: valueTree,
		factors:       "default value nodes",
		binds:         "the walk follows the concrete default VALUE (a finite JSON tree), linear in it",
		reachingPaths: "one: per default-encode pass, following the value"},
	{fn: "coerceDefault", file: "schema.go", class: valueTree,
		factors:       "default value nodes, bounded by depth",
		binds:         "value-guided recursion + the depth>=maxDepth cap",
		reachingPaths: "one: per default coercion at parse, following the value"},
	{fn: "coerceMetadataDefault", file: "schema_node.go", class: valueTree,
		factors:       "default value nodes",
		binds:         "value-guided recursion over the concrete default (name-ref follows are one hop, guided by the value)",
		reachingPaths: "one: per Root() metadata default coercion, following the value"},
	{fn: "branchAcceptsDefault", file: "schema_node.go", class: valueTree,
		factors:       "default value nodes",
		binds:         "value-guided recursion over the concrete default",
		reachingPaths: "one: per branch-acceptance check, following the value"},
	{fn: "encodeDefaultDepth", file: "resolve.go", class: valueTree,
		factors:       "default value nodes, bounded by depth",
		binds:         "value-guided recursion + the depth>=maxDepth cap",
		reachingPaths: "one: per default encode, following the value"},
	{fn: "appendAvroJSON", file: "json_codec.go", class: valueTree,
		factors:       "encoded value nodes, bounded by depth",
		binds:         "value-guided recursion + the depth>=maxDepth cap",
		reachingPaths: "one: per EncodeJSON/AppendEncodeJSON call, following the value"},
	{fn: "valueWalkLimit", file: "schema_node.go", class: valueTree,
		factors:       "value nodes x depth",
		binds:         "walkBudget + the depthLeft cap",
		reachingPaths: "one walkBudget per Props/value bounding pass (shared with toJSONWalk's budget)"},
	{fn: "inlineTreeDefs", file: "cache.go", class: textTree,
		factors:       "JSON tree nodes (each definition inlined once)",
		binds:         "the seen/inlined map[string]bool sets: a name already inlined is emitted as a reference, so the output is linear in the definition set",
		reachingPaths: "one: seen/inlined created per SchemaCache self-containment splice"},
	{fn: "build", file: "schema.go", class: textTree,
		factors:       "aschema text nodes, bounded by depth",
		binds:         "the parsed aschema is a TREE (each occurrence is its own text); depth>=maxDepth caps recursion",
		reachingPaths: "one: per Parse; nested builders share depth but each occurrence is its own text"},
}

// graphCostMarker is a source pattern that marks a walk carrying graph-cost
// state. Every occurrence in a source line (not a comment) must be attributable
// to a rowed walk or an allow-listed non-walk datum.
var graphCostMarkers = []string{
	"allowance",
	"walkBudget",
	"map[nodePair]",
	"map[*schemaNode]bool",
	"map[*schemaNode]struct{}",
	"map[*SchemaNode]struct{}",
	"map[reflect.Type]bool",
	"map[reflect.Type]seenForm",
	"defer delete(",
}

// nonWalkMarkerUses are source substrings that match a graphCostMarker pattern
// but are NOT budgeted walks: data maps carried on the builder/ctx, per-Go-type
// caches, and the like. Each is allow-listed with the reason it is not a walk.
// A new marker occurrence that matches neither a rowed walk nor one of these
// fails the completeness guard — which is the point.
var nonWalkMarkerUses = map[string]string{
	"custom map[*schemaNode]*customWiring":          "a DATA map of node->custom wiring, not a visited set",
	"custom      map[*schemaNode]*customWiring":     "a DATA map of node->custom wiring, not a visited set",
	"customMatch map[*schemaNode]string":            "a DATA map of node->matched-custom-name, not a visited set",
	"overlayDone map[*schemaNode]bool":              "presence state recording which nodes' overlay ran, carried on the builder across nests; the WALK that fills it (overlayInheritedCustom) is rowed",
	"building     map[*schemaNode]struct{}":         "the record-in-progress set for build-time cycle detection, carried on the builder; not a per-walk visited",
	"building:   make(map[*schemaNode]struct{})":    "initializes the builder's record-in-progress set (two sites: schema.go and cache.go)",
	"b.overlayDone = make(map[*schemaNode]bool)":    "re-inits the builder overlay presence state",
	"b.customMatch = make(map[*schemaNode]string)":  "re-inits the builder custom-match data map",
	"b.custom = make(map[*schemaNode]*customWiring": "re-inits the builder custom data map",
	"b.building = make(map[*schemaNode]struct{})":   "re-inits the builder record-in-progress set",
	"seen := make(map[reflect.Type]seenForm)":       "SchemaFor's inferType memo init; the walk (inferType) is rowed",
	"seen map[reflect.Type]seenForm":                "inferType/inferRecord/inferField memo parameter; inferType is rowed",
	"collectFields(t, make(map[reflect.Type]bool))": "inits collectFieldsRaw's visited; the walk is rowed",
	"visited map[reflect.Type]bool":                 "collect/collectFieldsRaw visited parameter; both rowed",
	"make(map[reflect.Type]bool)":                   "inits a Go-type walk visited set; the walks (collect/collectFieldsRaw) are rowed",
	"sync.Map // map[reflect.Type]":                 "a per-Go-type compiled-codec cache, amortized and keyed by fixed types; not a walk",
}

// TestInvariant_BudgetedWalkCensus is the enumeration guard. It derives the walk
// set from source two ways and requires every member to carry a cost expression,
// and it enforces that a schemaDAG walk's bound caps the PRODUCT (a memo or a
// budget), never a lone factor like depth.
func TestInvariant_BudgetedWalkCensus(t *testing.T) {
	files := censusSourceFiles(t)
	rowByFn := make(map[string]budgetedWalk, len(budgetedWalks))
	for _, w := range budgetedWalks {
		if _, dup := rowByFn[w.fn]; dup {
			t.Fatalf("duplicate census row for %q", w.fn)
		}
		rowByFn[w.fn] = w
	}

	// Guard A — rot: every rowed walk's function still exists in its file. A row
	// whose walk was renamed or deleted guards nothing.
	for _, w := range budgetedWalks {
		src := readFile(t, w.file)
		q := regexp.QuoteMeta(w.fn)
		// Match a top-level func, a method, or a recursive closure
		// (`var name func(` / `name = func(`) — collect in reflect.go is the last.
		defined := regexp.MustCompile(`func (\([^)]*\) )?`+q+`\(`).MatchString(src) ||
			regexp.MustCompile(`\b`+q+` func\(`).MatchString(src) ||
			regexp.MustCompile(`\b`+q+` = func\(`).MatchString(src)
		if !defined {
			t.Errorf("census rows %q in %s but no such func is defined there — the row rotted (renamed or removed?)", w.fn, w.file)
		}
		// Every row must name its reaching paths: a walk reached by more than one
		// construction path can be bounded on one and not another, invisible to
		// factors/binds. A blank reachingPaths is an incomplete row.
		if strings.TrimSpace(w.reachingPaths) == "" {
			t.Errorf("census row %q has no reachingPaths — name every construction path that reaches it and the bound each carries", w.fn)
		}
	}

	// Guard B — product binding: a schemaDAG walk MUST cap the product with a
	// memo or a budget; depth alone cannot bound a DAG (the fan-out is reachable
	// flat). goTypeDAG must justify with compile-time fixedness.
	for _, w := range budgetedWalks {
		switch w.class {
		case schemaDAG:
			b := strings.ToLower(w.binds)
			if !strings.Contains(b, "memo") && !strings.Contains(b, "budget") &&
				!strings.Contains(b, "shared") && !strings.Contains(b, "leaves") &&
				!strings.Contains(b, "tree") {
				t.Errorf("schemaDAG walk %q binds with %q — a shared graph walk must name a MEMO or a BUDGET (or justify a tree/leaf structure), not a lone factor", w.fn, w.binds)
			}
			if strings.TrimSpace(strings.ToLower(w.binds)) == "the depth>=maxdepth cap" {
				t.Errorf("schemaDAG walk %q is bound by depth alone; depth does not bound a DAG", w.fn)
			}
		case goTypeDAG:
			if !strings.Contains(strings.ToLower(w.binds), "compile") {
				t.Errorf("goTypeDAG walk %q must justify its bound by compile-time fixedness (attacker cannot grow a Go type at runtime)", w.fn)
			}
		}
	}

	// Guard C — recursion completeness: every function that recurses over the
	// schema graph (*schemaNode / *SchemaNode) must be rowed. This catches a new
	// walk that carries NO cost marker at all (the coerceTreeDefaults shape).
	for _, fn := range selfRecursiveSchemaWalks(t, files) {
		if _, ok := rowByFn[fn]; !ok {
			t.Errorf("function %q recurses over the schema graph but is not in the budgeted-walk census; add a row naming its cost factors and the bound that caps their product", fn)
		}
	}

	// Guard D — marker completeness: every graph-cost marker occurrence belongs
	// to a rowed walk or an allow-listed non-walk datum. This catches a new walk
	// that carries cost state (the dangerous case) even under mutual recursion
	// the recursion scan cannot see (minBytes, checkCompat, resolveNode).
	rowedFiles := make(map[string]bool)
	for _, w := range budgetedWalks {
		rowedFiles[w.file] = true
	}
	for _, f := range files {
		src := readFile(t, f)
		for i, line := range strings.Split(src, "\n") {
			code := line
			if c := strings.Index(code, "//"); c >= 0 {
				code = code[:c] // ignore comments
			}
			for _, m := range graphCostMarkers {
				if !strings.Contains(code, m) {
					continue
				}
				if attributed(code, rowedFiles[f], nonWalkMarkerUses) {
					continue
				}
				t.Errorf("%s:%d carries graph-cost marker %q but is not attributable to a rowed walk or an allow-listed non-walk:\n    %s\n  Row the walk with its cost factors, or allow-list the datum with why it is not a walk.", f, i+1, m, strings.TrimSpace(line))
			}
		}
	}
}

// attributed reports whether a marker-bearing code line belongs to a rowed walk
// (the file hosts one) or an explicitly allow-listed non-walk datum.
func attributed(code string, fileHostsRowedWalk bool, allow map[string]string) bool {
	for substr := range allow {
		if strings.Contains(code, substr) {
			return true
		}
	}
	// A line in a file that hosts a rowed walk, declaring/using that walk's
	// state (allowance, walkBudget, path/done/seen/visited maps over graph
	// nodes), is attributed to the walk. The allow-list above carves out the
	// NON-walk data maps in those same files, so this does not blanket-accept.
	return fileHostsRowedWalk
}

// selfRecursiveSchemaWalks derives, from source, the set of functions that take
// a *schemaNode or *SchemaNode and call themselves — the schema-graph walks a
// marker scan would miss if they carried no cost state. Mutual-recursion walks
// (minBytes, checkCompat, resolveNode) are caught by the marker guard instead.
func selfRecursiveSchemaWalks(t *testing.T, files []string) []string {
	t.Helper()
	sig := regexp.MustCompile(`func (?:\([^)]*\) )?(\w+)\([^)]*\*(?:schemaNode|SchemaNode)\b`)
	var out []string
	seen := map[string]bool{}
	for _, f := range files {
		src := readFile(t, f)
		for _, m := range sig.FindAllStringSubmatchIndex(src, -1) {
			name := src[m[2]:m[3]]
			body := src[m[1]:]
			if nxt := strings.Index(body, "\nfunc "); nxt >= 0 {
				body = body[:nxt]
			}
			if regexp.MustCompile(`\b` + regexp.QuoteMeta(name) + `\(`).MatchString(body) {
				if !seen[name] {
					seen[name] = true
					out = append(out, name)
				}
			}
		}
	}
	return out
}

func readFile(t *testing.T, f string) string {
	t.Helper()
	b, err := os.ReadFile(f)
	if err != nil {
		t.Fatalf("reading %s: %v", f, err)
	}
	return string(b)
}

// minBytesConstructionSite rows one place the min-bytes walk STATE is
// constructed (newMinBytesWalk) — a reaching path of the minBytes walk. The
// walk is one, but a schema reaches it by building a container, by finalizing a
// forward reference, by resolving, or by compiling a dropped-field skip, and the
// container FACTOR is bounded only if each of those paths constructs the walk
// once per OPERATION and shares it across that operation's containers. A fresh
// construction per container is the bug the forward/backward split exposed: the
// build path resolved a backward name reference to a fully-built cyclic node and
// walked it per container. The `context` is a source substring the construction
// line must contain, so a construction that drifts to a per-container scope
// fails to match its row.
type minBytesConstructionSite struct {
	file    string
	context string // a substring uniquely identifying the construction line
	scope   string // what operation the constructed walk is shared across
	// factors is what this path's ONE walk is shared ACROSS, and every entry
	// carries the cell that MEASURES it. A row may not state its bound in
	// prose: a sentence is a claim, and the whole purpose of this census is to
	// reject claims. The seventh row of this table once read "cross-record cost
	// is wire-bounded" — true, and it bounded nothing, because reaching a record
	// costs O(1) wire bytes while draining a full allowance. No cell drove a
	// record count, so nothing contradicted it.
	//
	// Empty only when exempt is set.
	factors []reachFactor
	// exempt records why this construction needs no measured factor. Legal only
	// for a site that is not shared across anything, and the guard checks that
	// premise itself rather than believing this string.
	exempt string
}

// reachFactor is one caller-chosen count a shared walk is spread across, with
// the cell that drives it.
//
// values must hold at least TWO distinct numbers. One value can only ask
// "does this finish?" — it cannot tell a bound from a cost that is merely
// linear with a small constant, and it cannot see a factor it never varies.
// The cell reads its values FROM here rather than from its own constant, so
// "the cell drives two values" is not a second claim to be checked against the
// first; it is the same fact read once.
type reachFactor struct {
	name   string
	values []int
	// drive runs the reaching path at one value of the factor. It returns an
	// error rather than taking a *testing.T because it executes inside the
	// watchdog goroutine, where t.Fatal would be illegal.
	drive func(n int) error
}

// reachCounts are the two values every reaching-path factor is driven at. The
// low one establishes the single-unit cost; the high one is far enough above it
// that a per-unit walk shows up as a multiple rather than as noise.
var reachCounts = []int{1, 48}

const reachLevels = 26 // SCC depth: deep enough that one walk exhausts its allowance

var minBytesConstructionSites = []minBytesConstructionSite{
	{file: "deser.go", context: "return newMinBytesWalk().minBytesOf(n)",
		scope:  "standalone schemaMinBytes: ONE node, outside any container loop (the only fresh-per-call form)",
		exempt: "shared across nothing — it is the fresh-per-call form itself, and the guard below derives from source that no production code calls it, so there is no count for a cell to drive"},

	{file: "schema.go", context: "minBytes:   newMinBytesWalk()",
		scope: "the builder's b.minBytes seeded in Parse — the BUILD path (backward refs resolve to a built node here)",
		factors: []reachFactor{{name: "containers per parse (backward refs)", values: reachCounts,
			drive: func(n int) error {
				_, err := Parse(nContainersOverWiredSCC(n, reachLevels))
				return err
			}}}},

	{file: "schema.go", context: "b.minBytes = newMinBytesWalk()",
		scope: "lazy seed at the root's first build, before any nest, so a directly-constructed (white-box) builder still shares one walk across the build path",
		factors: []reachFactor{{name: "containers per parse (build path, same factor the Parse seed carries)", values: reachCounts,
			drive: func(n int) error {
				_, err := Parse(nContainersOverWiredSCC(n, reachLevels))
				return err
			}}}},

	{file: "schema.go", context: "mbw := newMinBytesWalk()",
		scope: "one walk before finalize's container-fixup loop — the FINALIZE path (forward refs)",
		factors: []reachFactor{{name: "containers per parse (forward refs)", values: reachCounts,
			drive: func(n int) error {
				_, err := Parse(nContainersOverSCC(n, reachLevels))
				return err
			}}}},

	{file: "cache.go", context: "minBytes:   newMinBytesWalk()",
		scope: "SchemaCache's builder b.minBytes — the build path via the cache",
		factors: []reachFactor{{name: "containers per SchemaCache.Parse", values: reachCounts,
			drive: func(n int) error {
				var c SchemaCache
				_, err := c.Parse(nContainersOverWiredSCC(n, reachLevels))
				return err
			}}}},

	{file: "resolve.go", context: "minBytes: newMinBytesWalk()",
		scope: "resolveCtx.minBytes, shared across one Resolve AND across every dropped-field skip that resolution compiles — including the record compiles deferred to decode time, which join this same walk rather than starting their own",
		factors: []reachFactor{
			{name: "containers per resolution", values: reachCounts,
				drive: func(n int) error {
					scc := nContainersOverSCC(n, reachLevels)
					w := MustParse(scc)
					r := MustParse(strings.Replace(scc,
						`{"type":"record","name":"Root","fields":[`,
						`{"type":"record","name":"Root","fields":[{"name":"extra","type":"int","default":0},`, 1))
					_, err := Resolve(w, r)
					return err
				}},
			// The factor the old skip row asserted instead of measuring. Each
			// reference to one record compiles its own skipRecordFields, so a
			// per-record walk multiplied the allowance by a count the schema
			// picks while each compile was reached with a single wire byte.
			{name: "records compiled per resolution (lazy, at decode)", values: reachCounts,
				drive: func(n int) error {
					top := nRecordsOverSCC(n, reachLevels)
					w := MustParse(`{"type":"record","name":"Outer","fields":[{"name":"drop","type":` + top + `},{"name":"keep","type":"int"}]}`)
					r := MustParse(`{"type":"record","name":"Outer","fields":[{"name":"keep","type":"int"}]}`)
					res, err := Resolve(w, r)
					if err != nil {
						return err
					}
					var out struct {
						Keep int32 `avro:"keep"`
					}
					_, err = res.Decode(nRecordsOverSCCWire(n, reachLevels), &out)
					return err
				}},
		}},
}

// nRecordsOverSCC references ONE record definition nrecs times, each reference
// holding one array over a shared cyclic SCC. Every reference compiles its own
// skipRecordFields, so this drives the RECORD count with the container count
// held at one — the axis the container cell holds constant.
func nRecordsOverSCC(nrecs, levels int) string {
	var b strings.Builder
	b.WriteString(`{"type":"record","name":"Top","fields":[`)
	for i := range levels {
		next := fmt.Sprintf("L%d", i+1)
		if i == levels-1 {
			next = "L0"
		}
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"name":"d%d","type":{"type":"record","name":"L%d","fields":[{"name":"f0","type":["null","%s"]},{"name":"f1","type":["null","%s"]}]}}`, i, i, next, next)
	}
	b.WriteString(`,{"name":"r0","type":{"type":"record","name":"R","fields":[{"name":"z","type":{"type":"array","items":"L0"}}]}}`)
	for j := 1; j < nrecs; j++ {
		fmt.Fprintf(&b, `,{"name":"r%d","type":"R"}`, j)
	}
	b.WriteString(`]}`)
	return b.String()
}

// nRecordsOverSCCWire is the minimal wire that reaches every record: two null
// union bytes per SCC level, then one empty-array byte per reference. Reaching
// the nth compile costs ONE byte, which is exactly why "the wire bounds how
// many compile" bounds the count and not the work.
func nRecordsOverSCCWire(nrecs, levels int) []byte {
	w := make([]byte, 0, 2*levels+nrecs+1)
	for range levels {
		w = append(w, 0, 0)
	}
	for range nrecs {
		w = append(w, 0)
	}
	return append(w, 2) // keep = 1
}

// reachScaleTol and reachScaleFloor separate "the walk is shared" from "the
// walk is rebuilt per unit". A shared walk pays its allowance once, so the high
// and low cells differ only by the genuinely linear part (a longer schema to
// parse, more wire bytes to read). A per-unit walk multiplies the allowance by
// the factor, which at reachCounts' spread is more than an order of magnitude
// past this. The floor keeps a fast cell from being judged on a ratio of noise.
const (
	reachScaleTol   = 4
	reachScaleFloor = 400 * time.Millisecond
)

// TestInvariant_EveryReachingPathBoundIsMeasured is the rule that a bound must
// be MEASURED, not stated. Every reaching path names the counts its one walk is
// shared across, and every count names a cell that drives it at two or more
// distinct values.
//
// Both directions are mechanical, and the second is the one that bites:
//
//   - a row with no cell fails. Prose describing why a cost is bounded is a
//     claim, and this census exists to reject claims.
//   - a cell that holds its row's factor CONSTANT fails. One value asks only
//     "does this finish?", which a cost that is merely linear in the factor
//     also answers. The skip path was rowed with a true sentence about the wire
//     and no cell that varied a record count, and the unbounded factor lived
//     under it. Driving one value would not have found it; driving two does.
//
// Attack it both ways: delete a factor's second value -> the constant-factor
// arm fires; drop a row's factors for a sentence -> the no-cell arm fires;
// rebuild any shared walk per unit -> the scale arm fires.
func TestInvariant_EveryReachingPathBoundIsMeasured(t *testing.T) {
	for _, site := range minBytesConstructionSites {
		if site.exempt != "" {
			if len(site.factors) != 0 {
				t.Errorf("%s (%s) is exempt AND names factors — say which it is", site.file, site.context)
			}
			// The exemption's premise is not taken on trust: the guard below
			// derives from source that nothing in production calls the
			// standalone form, which is what makes "shared across nothing" true.
			continue
		}
		if len(site.factors) == 0 {
			t.Errorf("%s (%s) states its bound only in prose (%q).\nA reaching path must name the count its walk is shared across and a cell that drives it; a sentence is not a measurement.",
				site.file, site.context, site.scope)
			continue
		}
		for _, f := range site.factors {
			name := site.file + "/" + f.name
			seen := make(map[int]bool, len(f.values))
			for _, v := range f.values {
				seen[v] = true
			}
			if len(seen) < 2 {
				t.Errorf("%s: cell drives %d distinct value(s) of its own factor %v.\nA bound is a claim about how cost RESPONDS to this count, and one value cannot tell a bound from a linear cost.",
					name, len(seen), f.values)
				continue
			}
			if f.drive == nil {
				t.Errorf("%s: factor has values but no cell to drive them", name)
				continue
			}
			lo, hi := f.values[0], f.values[0]
			for _, v := range f.values {
				lo = min(lo, v)
				hi = max(hi, v)
			}
			times := make(map[int]time.Duration, len(f.values))
			for _, v := range f.values {
				start := time.Now()
				// Each value must also be bounded on its own — the watchdog
				// catches an unbounded path that would never return to be timed.
				wantTerminate(t, fmt.Sprintf("%s=%d", name, v), func() error { return f.drive(v) })
				times[v] = time.Since(start)
			}
			floor := reachScaleFloor
			if raceEnabled {
				floor = reachScaleFloor * 5
			}
			if lim := max(reachScaleTol*times[lo], floor); times[hi] > lim {
				t.Errorf("%s: cost scales with the factor — %v at %d vs %v at %d (limit %v).\nA walk shared across this count pays its allowance once; this is the shape of one walk per unit.",
					name, times[hi], hi, times[lo], lo, lim)
			}
			t.Logf("%s: %v at %d, %v at %d", name, times[lo], lo, times[hi], hi)
		}
	}
}

// TestInvariant_MinBytesReachingPaths is the reaching-path guard for the one
// budgeted walk reached by more than one construction path. It derives every
// newMinBytesWalk() construction from source and requires each to be a rowed,
// per-operation site; it forbids a fresh-walk-per-call anywhere but the
// standalone schemaMinBytes; and it forbids a production caller of schemaMinBytes
// (which would rebuild the walk on every call). Attack it both ways: add a
// newMinBytesWalk() at a new site -> unrowed; call schemaMinBytes in production
// -> a fresh-per-call path.
func TestInvariant_MinBytesReachingPaths(t *testing.T) {
	files := censusSourceFiles(t)

	// Derive every construction (a newMinBytesWalk() CALL, not the func decl) and
	// require each to match a rowed site's context substring.
	for _, f := range files {
		src := readFile(t, f)
		for i, line := range strings.Split(src, "\n") {
			if !strings.Contains(line, "newMinBytesWalk()") || strings.Contains(line, "func newMinBytesWalk(") {
				continue
			}
			rowed := false
			for _, s := range minBytesConstructionSites {
				if s.file == f && strings.Contains(line, s.context) {
					rowed = true
					break
				}
			}
			if !rowed {
				t.Errorf("%s:%d constructs a min-bytes walk that is not a rowed reaching path:\n    %s\n  Row it in minBytesConstructionSites with the operation it is shared across, or (if it is per-container) share an existing per-operation walk instead.", f, i+1, strings.TrimSpace(line))
			}
		}
	}

	// Every rowed construction must still exist (rot check, the other direction).
	for _, s := range minBytesConstructionSites {
		src := readFile(t, s.file)
		if !strings.Contains(src, s.context) {
			t.Errorf("minBytesConstructionSites rows %q in %s but the line is gone — the path rotted", s.context, s.file)
		}
	}

	// The fresh-walk-per-CALL form (constructor immediately consumed) is legal
	// only for the standalone single-node schemaMinBytes. Anywhere else it is a
	// per-container path — exactly what the build finding was.
	fresh := occurrences(t, files, "newMinBytesWalk().minBytesOf(")
	total := 0
	for f, lines := range fresh {
		for _, ln := range lines {
			total++
			if f != "deser.go" {
				t.Errorf("%s:%d consumes a FRESH min-bytes walk in one call — the container factor reappears if this runs per container; share a per-operation walk", f, ln)
			}
		}
	}
	if total != 1 {
		t.Errorf("expected exactly ONE fresh-per-call min-bytes walk (the standalone schemaMinBytes); found %d — a new one is a per-container path unless proven otherwise", total)
	}

	// schemaMinBytes is that fresh-per-call standalone; a PRODUCTION caller would
	// rebuild the walk each call, so there must be none (tests may use it for a
	// single node). The set of production callers is derived, not assumed.
	for _, f := range files {
		src := readFile(t, f)
		for i, line := range strings.Split(src, "\n") {
			code := line
			if c := strings.Index(code, "//"); c >= 0 {
				code = code[:c]
			}
			if strings.Contains(code, "schemaMinBytes(") && !strings.Contains(code, "func schemaMinBytes(") {
				t.Errorf("%s:%d calls schemaMinBytes in production — it builds a fresh walk per call; use a shared per-operation walk (b.minBytes / ctx.minBytes / the finalize or skip mbw)", f, i+1)
			}
		}
	}
}

// ---- cost cells: the same measured-bound rule, one level out ---------------

// The reaching-path rule above says a bound must be MEASURED — a cell that
// drives its factor at two or more values, because one value cannot tell a
// bound from a cost that is merely linear. That rule was stated for the walk
// CONSTRUCTION sites and then not applied to the wall-clock cost cells, which
// is the same defect the rule exists to catch: a stated requirement whose guard
// passes its known violators. Five cells drove one value each and the suite was
// green.
//
// costCell is the registry that closes it. A cell's magnitudes live HERE and the
// cell reads them, so the two cannot disagree, and the source derivation below
// means a new cost cell cannot quietly skip the registry.
type costCell struct {
	fn     string // the test function
	factor string // the caller-chosen magnitude its bound claims to cap
	values []int  // what the cell drives — at least two distinct, unless exempt
	// exempt is why one magnitude suffices. It is a CLAIM like any other, so it
	// must name what the cell asserts INSTEAD of a wall-clock bound; the guard
	// cross-checks it against whether the cell actually takes the wall-clock
	// harness, so an exemption cannot be pasted onto a timing cell.
	exempt string
	// scaleTol bounds cost(max)/cost(min). Every one of these factors measured
	// FLAT with a correct bound even where the schema TEXT grows with the factor
	// (width 80 -> 8000 grows the text 65x and the parse 1.4x, because the walk
	// dominates and the allowance caps it), so a small tolerance is honest.
	scaleTol int
	// floor is the largest cost the BOUND ITSELF permits at the top of the
	// range, and the limit is max(scaleTol*cost(min), floor). For a cell whose
	// shapes are all memoizable it is just machine noise. For one that includes
	// an UN-memoizable shape it is one exhausted allowance (~120ms measured),
	// because a cyclic subtree cannot be cached and legitimately walks until
	// maxMinBytesWork stops it — that is the bound ENGAGING, not the cost
	// scaling, and a cell spanning both regimes has to be judged against the
	// looser of them. It stays orders of magnitude under an unbounded walk,
	// which is seconds.
	floor time.Duration
}

var costCells = []costCell{
	{fn: "TestInvariant_EveryMinBytesEntryPointIsBounded",
		factor: "dagNested DEPTH — the PATHS factor: without the memo this is 2^depth, so 13 vs 26 is a 8192x separation",
		values: []int{13, 26}, scaleTol: 8, floor: 25 * time.Millisecond},

	{fn: "TestInvariant_CyclicWalkCostIsBoundedByWork",
		factor: "dagWideSCC WIDTH — the CHILDREN factor: a per-NODE charge makes cost allowance x width, a per-CHILD charge makes it flat",
		values: []int{80, 8000}, scaleTol: 4, floor: 400 * time.Millisecond},

	{fn: "TestInvariant_WideCyclicWalkReachesEveryEntryPoint",
		factor: "dagWideSCC WIDTH, across the entry points that do not take the schema from the caller",
		values: []int{80, 8000}, scaleTol: 4, floor: 400 * time.Millisecond},

	{fn: "TestInvariant_MetadataSurfacesBoundedByWidth",
		// Named for what it drives, after the old name was executed and found
		// false. It does NOT measure the metadata walk's node budget: a PARSED
		// schema is deduped before it reaches that walk, so disabling takeNode
		// entirely leaves this cell green. What it does exercise is the
		// Root+Schema ROUND TRIP, whose last step is a re-Parse of the rendered
		// text (SchemaNode.Schema), which puts the min-bytes charge on its path
		// — neutering that charge reds it. The node budget's own cells are
		// TestRegression_SchemaNodeWalkBudgetBattery,
		// TestRegression_SchemaNodeDuplicateNamedDefinitionBounded and
		// TestRegression_SchemaForCustomSchemaBudgetAxes, which hand-build the
		// trees Parse cannot express; all three red when takeNode stops charging.
		factor: "dagWideSCC WIDTH through the metadata surfaces — Root+Schema (render, marshal, re-Parse), String and Canonical",
		values: []int{80, 8000}, scaleTol: 4, floor: 400 * time.Millisecond},

	{fn: "TestInvariant_MinBytesContainerCountBounded",
		factor: "CONTAINER count. Its two generator calls vary reference DIRECTION (forward/backward), which is a different axis — the count itself was pinned at 220",
		values: []int{1, 220}, scaleTol: 4, floor: 400 * time.Millisecond},

	// Value oracles. Named explicitly rather than left to a reader to re-derive:
	// each varies shapes to check an ANSWER and asserts equality, never
	// wall-clock, so a second magnitude would measure nothing about a bound.
	{fn: "TestInvariant_MemoAgreesWithUnmemoizedWalk",
		exempt: "value oracle: compares the memoized walk's result against an un-memoized recomputation per node. Its oracle is equality of VALUES, and a wrong memo is FASTER, so timing is exactly what cannot settle it"},
	{fn: "TestInvariant_DagMinBytesIsExactAtScale",
		exempt: "value oracle: asserts the minimum a shared DAG reports equals the minimum its expanded TREE reports. Equality, not cost"},
	{fn: "TestInvariant_MinBytesSelfReadable",
		exempt: "value oracle: asserts a bound derived from the walk still admits wire this package's own encoder produces. Accept/reject, not cost"},
	{fn: "TestInvariant_SharingDoesNotChangeMinBytes",
		exempt: "value oracle: asserts sharing one walk across containers does not change the ANSWER. It already sweeps fan x levels; the sweep is over SHAPES to find a disagreement, not magnitudes to time"},
	{fn: "TestInvariant_SharedSchemaNodeWalkedOnce",
		factor: "dagNested/dagFlat/dagSelfRecursive/dagSingleSCC DEPTH — the PATHS factor across all four sharing shapes and both fans. Its name reads like a value oracle, and the hand derivation classified it as one; it takes the wall-clock harness, so the exemption cross-check caught it",
		// Two of its four shapes are CYCLIC and cannot be memoized at all, so
		// they climb to one exhausted allowance between the two depths (~1.9ms
		// at 13, ~120ms at 26) while the memoizable two stay flat at ~200us.
		// The floor is that allowance; without the charge the same shapes run
		// for seconds.
		values: []int{13, 26}, scaleTol: 8, floor: 500 * time.Millisecond},

	{fn: "TestDoSBattery_C6_MetadataWalk",
		factor: "dagNested/dagFlat DEPTH — the PATHS factor through the metadata + resolve + compat entry points. Missed by the hand derivation entirely; only the source scan found it",
		values: []int{13, 26}, scaleTol: 8, floor: 25 * time.Millisecond},
}

// costFactorValues returns the magnitudes the named cost cell must drive. A cell
// calls it with its OWN name, so its values cannot drift from the row the guard
// reads; TestInvariant_EveryCostCellDrivesItsFactor checks from source that
// every rowed timing cell does exactly that.
func costFactorValues(t *testing.T, fn string) []int {
	t.Helper()
	for _, c := range costCells {
		if c.fn == fn {
			if c.exempt != "" {
				t.Fatalf("%s is rowed EXEMPT but is asking for factor values", fn)
			}
			return c.values
		}
	}
	t.Fatalf("%s is not rowed in costCells — a cost cell must declare the factor it drives", fn)
	return nil
}

// wantCostDoesNotScale asserts that the named cell's operation costs about the
// same at the top of its factor's range as at the bottom.
//
// build takes the magnitude and returns the thunk to TIME. Everything the
// magnitude needs but the bound does not own — generating a schema whose TEXT
// is linear in the factor, parsing it when the bound under test is downstream
// of the parse — belongs in build, outside the returned closure. Putting it
// inside is not a rounding error: the metadata cell had its MustParse in the
// timed region, and since the parse of a width-8000 schema dominates the walk
// that follows it, the cell moved when the PARSE's bound was neutered and sat
// still when its own was.
func wantCostDoesNotScale(t *testing.T, fn, label string, build func(n int) func() error) {
	t.Helper()
	var row costCell
	for _, c := range costCells {
		if c.fn == fn {
			row = c
		}
	}
	vals := costFactorValues(t, fn)
	lo, hi := vals[0], vals[0]
	for _, v := range vals {
		lo, hi = min(lo, v), max(hi, v)
	}
	times := make(map[int]time.Duration, len(vals))
	for _, v := range vals {
		run := build(v)
		start := time.Now()
		wantTerminate(t, fmt.Sprintf("%s/%s=%d", label, row.factor, v), run)
		times[v] = time.Since(start)
	}
	floor := row.floor
	if raceEnabled {
		floor *= 5
	}
	if lim := max(time.Duration(row.scaleTol)*times[lo], floor); times[hi] > lim {
		t.Errorf("%s: cost scales with the factor — %v at %d vs %v at %d (limit %v).\nThe bound claims to cap this magnitude; a cost that grows with it is the bound missing, not a slow machine.",
			label, times[hi], hi, times[lo], lo, lim)
	}
}

// TestInvariant_EveryCostCellDrivesItsFactor applies the measured-bound rule to
// the wall-clock cost cells, deriving the set the same way it was derived by
// hand: a cost GENERATOR is a function in the census sources returning a schema
// string from a magnitude, and any test that calls one is a cost cell.
//
// Mechanical in every direction:
//
//   - a cell that calls a cost generator and is not rowed FAILS (add a
//     generator or a caller and it fires).
//   - a rowed timing cell with fewer than two distinct values FAILS. This is
//     the arm that was missing: the rule was stated for the walk construction
//     sites and never applied here, so five cells pinned one magnitude each and
//     the suite stayed green.
//   - a rowed timing cell that does not READ its row FAILS, so a cell cannot
//     keep a private constant that disagrees with the registry.
//   - an EXEMPTION is a claim and is cross-checked: a cell rowed exempt that
//     takes the wall-clock harness is a timing cell wearing a value-oracle
//     label, and a cell rowed with values that takes no harness is the reverse.
//   - a row naming no test FAILS, so the registry cannot go stale.
func TestInvariant_EveryCostCellDrivesItsFactor(t *testing.T) {
	files := censusSourceFiles(t)
	src := map[string]string{}
	for _, f := range files {
		src[f] = readFile(t, f)
	}
	for _, f := range []string{"schema_dag_cost_test.go", "budgeted_walk_census_test.go", "dos_battery_test.go"} {
		src[f] = readFile(t, f)
	}

	// Derive the generator vocabulary: a func taking magnitudes and returning a
	// schema string. Listing them would be the "written doc list is NOT an
	// enumeration" trap.
	genDecl := regexp.MustCompile(`(?m)^func ((?:dag|nContainers)[A-Za-z]*)\([^)]*int\) string \{`)
	gens := map[string]bool{}
	for _, m := range genDecl.FindAllStringSubmatch(src["schema_dag_cost_test.go"], -1) {
		gens[m[1]] = true
	}
	if len(gens) < 4 {
		t.Fatalf("derived only %d cost generators (%v) — the derivation broke, and a broken derivation reads as full coverage", len(gens), gens)
	}

	bodies := map[string][2]string{}
	for _, v := range src {
		testFuncBodies(v, bodies)
	}

	rowed := map[string]costCell{}
	for _, c := range costCells {
		if _, dup := rowed[c.fn]; dup {
			t.Errorf("costCells rows %s twice", c.fn)
		}
		rowed[c.fn] = c
	}

	// Comments and string literals are stripped first: a generator NAMED in a
	// comment is not a caller, and a cell that hands a generator to a table as a
	// function VALUE (build: dagNested) is one even though it never writes
	// "dagNested(". Both mistakes were made by the first derivation, in opposite
	// directions, which is why this is matched on identifiers over stripped code.
	callsGenerator := func(code string) bool {
		for g := range gens {
			if regexp.MustCompile(`\b` + g + `\b`).MatchString(code) {
				return true
			}
		}
		return false
	}
	// A cell that takes the wall-clock harness is asserting a COST.
	takesHarness := func(code string) bool {
		return strings.Contains(code, "wantTerminate(") || strings.Contains(code, "dosRun(") ||
			strings.Contains(code, "wantCostDoesNotScale(")
	}

	for fn, bc := range bodies {
		raw, code := bc[0], bc[1]
		if !callsGenerator(code) {
			continue
		}
		c, ok := rowed[fn]
		if !ok {
			t.Errorf("%s drives a cost generator but is not rowed in costCells.\nRow it with the factor its bound claims to cap and the values it drives, or row it exempt with what it asserts instead.", fn)
			continue
		}
		if c.exempt != "" {
			if takesHarness(code) {
				t.Errorf("%s is rowed EXEMPT (%q) but takes the wall-clock harness — an exemption cannot sit on a timing cell", fn, c.exempt)
			}
			continue
		}
		if !takesHarness(code) {
			t.Errorf("%s is rowed with factor values but never takes the wall-clock harness — it is a value oracle, and should be rowed exempt saying so", fn)
		}
		seen := map[int]bool{}
		for _, v := range c.values {
			seen[v] = true
		}
		if len(seen) < 2 {
			t.Errorf("%s drives %d distinct value(s) of %q.\nOne value asks only whether the cell finishes, which a cost merely LINEAR in the factor also answers.", fn, len(seen), c.factor)
		}
		if !strings.Contains(raw, `"`+fn+`"`) {
			t.Errorf("%s does not name itself to costFactorValues/wantCostDoesNotScale — its magnitudes are not read from its row, so the row and the cell can disagree.", fn)
		}
	}

	// The other direction: a row that names no such test has rotted.
	for _, c := range costCells {
		bc, ok := bodies[c.fn]
		if !ok {
			t.Errorf("costCells rows %s but no such test exists — the row rotted", c.fn)
			continue
		}
		if !callsGenerator(bc[1]) {
			t.Errorf("costCells rows %s but it drives no cost generator — the row reads as coverage it does not have", c.fn)
		}
	}
}

// blankCode replaces the contents of comments and string literals with spaces,
// preserving every byte position. Two derivations need it and they need
// different views of the same bytes: identifier matching must not see a
// generator NAMED in a doc comment, while the self-naming check must see string
// literals. Blanking in place gives both from one pass, and lets a function's
// extent be found by counting braces without a brace inside a string ending it
// early.
func blankCode(src string) string {
	b := []byte(src)
	blank := func(from, to int) {
		for k := from; k < to && k < len(b); k++ {
			if b[k] != '\n' {
				b[k] = ' '
			}
		}
	}
	for i := 0; i < len(b); {
		switch {
		case b[i] == '/' && i+1 < len(b) && b[i+1] == '/':
			j := i
			for j < len(b) && b[j] != '\n' {
				j++
			}
			blank(i, j)
			i = j
		case b[i] == '/' && i+1 < len(b) && b[i+1] == '*':
			j := i + 2
			for j+1 < len(b) && !(b[j] == '*' && b[j+1] == '/') {
				j++
			}
			blank(i, j+2)
			i = j + 2
		case b[i] == '`':
			j := i + 1
			for j < len(b) && b[j] != '`' {
				j++
			}
			blank(i, j+1)
			i = j + 1
		case b[i] == '"':
			j := i + 1
			for j < len(b) && b[j] != '"' {
				if b[j] == '\\' {
					j++
				}
				j++
			}
			blank(i, j+1)
			i = j + 1
		default:
			i++
		}
	}
	return string(b)
}

// testFuncBodies returns each test function's body from src, keyed by name, as
// (raw, code) where code has comments and strings blanked. The extent is found
// by brace matching from the signature, NOT by running to the next test
// function — a helper or a var block declared between two tests would otherwise
// be attributed to the one above it, which is how this derivation first
// reported a census structural test as a driver of cost generators.
func testFuncBodies(src string, into map[string][2]string) {
	code := blankCode(src)
	decl := regexp.MustCompile(`(?m)^func (Test[A-Za-z0-9_]+)\(t \*testing\.T\) \{`)
	for _, loc := range decl.FindAllStringSubmatchIndex(code, -1) {
		name := src[loc[2]:loc[3]]
		depth, end := 0, -1
		for k := loc[1] - 1; k < len(code); k++ {
			switch code[k] {
			case '{':
				depth++
			case '}':
				depth--
				if depth == 0 {
					end = k
				}
			}
			if end >= 0 {
				break
			}
		}
		if end < 0 {
			end = len(code) - 1
		}
		into[name] = [2]string{src[loc[1]:end], code[loc[1]:end]}
	}
}
