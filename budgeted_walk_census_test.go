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
	"os"
	"regexp"
	"strings"
	"testing"
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
		factors: "containers x paths-per-walk x children-per-node",
		binds: "one minBytesWalk SHARED per operation (containers) + done memo (paths) + per-child allowance charge (children)",
		reachingPaths: "FOUR, each sharing one walk per operation: build (b.minBytes on the builder, forward refs fixed in finalize AND backward refs resolved to a fully-built node at build), finalize (one mbw before the container-fixup loop), resolve (ctx.minBytes), skip (one mbw per record's once.Do compile). The standalone schemaMinBytes is a fifth, single-node, outside any loop. Guarded by TestInvariant_MinBytesReachingPaths, which derives every newMinBytesWalk() site from source"},
	{fn: "checkCompat", file: "compat.go", class: schemaDAG,
		factors: "distinct (reader,writer) node pairs x children-per-pair",
		binds: "the seen map[nodePair]bool memo, threaded from CheckCompatibility with no defer-delete, so each pair is walked once",
		reachingPaths: "one: seen created in CheckCompatibility, threaded through the whole recursive check"},
	{fn: "resolveNode", file: "resolve.go", class: schemaDAG,
		factors: "distinct (reader,writer) pairs x (children + per-container min-bytes)",
		binds: "ctx.seen pair memo (pairs) + ctx.minBytes shared walk (the container min-bytes factor)",
		reachingPaths: "one: ctx (seen + minBytes) created in Resolve, threaded through the whole resolution"},
	{fn: "toJSONWalk", file: "schema_node.go", class: schemaDAG,
		factors: "nodes emitted x bytes per node",
		binds: "walkBudget (nodes + bytes), charged by takeNode at the TOP of every entry so a DAG re-descent still spends budget; visited is only cycle detection",
		reachingPaths: "one walkBudget per metadata-API call (toJSONDedup), from Root().Schema()/String()/Canonical(); each walks the whole tree once"},
	{fn: "collectLocalNames", file: "schema_node.go", class: schemaDAG,
		factors: "distinct nodes x names per node",
		binds: "the visited map[*SchemaNode]struct{} memo (mark on entry, return on hit)",
		reachingPaths: "one: visited created per toJSONDedup, one walk of the tree"},
	{fn: "stampNameRefs", file: "schema_node.go", class: schemaDAG,
		factors: "distinct nodes",
		binds: "the visited memo (mark on entry, return on hit)",
		reachingPaths: "one: visited created per Root() name-ref stamping pass"},
	{fn: "collectNamedTypes", file: "schema_node.go", class: schemaDAG,
		factors: "tree nodes (name-ref nodes are leaves, not followed)",
		binds: "structural: a reference SchemaNode carries no children, so the walk is over the definition TREE, linear in it",
		reachingPaths: "one: table created in fixupNameRefDefaults, per Root()"},
	{fn: "coerceTreeDefaults", file: "schema_node.go", class: schemaDAG,
		factors: "tree nodes (name-ref nodes are leaves)",
		binds: "structural: references are leaves, so the walk is over the definition TREE, linear in it",
		reachingPaths: "one: same fixupNameRefDefaults pass, per Root()"},
	{fn: "overlayInheritedCustom", file: "schema.go", class: schemaDAG,
		factors: "distinct nodes x custom lookups",
		binds: "the visited map[*schemaNode]bool memo (mark on entry, return on hit)",
		reachingPaths: "one: b.overlayDone created per parse, walked at inherited-custom overlay (build/reference-time)"},
	{fn: "findCustomTypeMatchInSubtreeWalk", file: "schema.go", class: schemaDAG,
		factors: "distinct nodes x registered custom types",
		binds: "the visited map[*schemaNode]bool memo (mark on entry, return on hit)",
		reachingPaths: "one: visited created per findCustomTypeMatchInSubtree call at build"},
	{fn: "buildCustomWiring", file: "schema.go", class: schemaDAG,
		factors: "distinct nodes",
		binds: "the visited memo (mark on entry, return on hit)",
		reachingPaths: "one: visited created per applyCustomTypes pass at build"},
	{fn: "nodeAwaitsForwardRefSeen", file: "schema.go", class: schemaDAG,
		factors: "distinct nodes",
		binds: "the seen map[*schemaNode]struct{} memo (mark on entry, return on hit); the separate building set is defer-delete cycle detection",
		reachingPaths: "one: seen created per nodeAwaitsForwardRef call at build"},

	// ---- goTypeDAG: bound is compile-time fixedness + per-type sync.Map ----
	{fn: "collect", file: "reflect.go", class: goTypeDAG,
		factors: "2^(embed depth) on a shared-embed type DAG (visited is defer-delete, so it re-descends)",
		binds: "NOT a runtime bound: a Go type is fixed at COMPILE time and the result is amortized by a per-type sync.Map, so the fan-out is not attacker-grown (G3)",
		reachingPaths: "one: visited created per typeFieldMapping (per Go type; the sync.Map amortizes repeats across calls)"},
	{fn: "collectFieldsRaw", file: "schema_for.go", class: goTypeDAG,
		factors: "2^(embed depth) on a shared-embed type DAG (visited is defer-delete)",
		binds: "compile-time fixedness + collectFields' per-call visited; not attacker-grown at runtime (G3)",
		reachingPaths: "one: visited created per collectFields, per SchemaFor of a Go type"},
	{fn: "inferType", file: "schema_for.go", class: goTypeDAG,
		factors: "type nodes x ptr chains, bounded by depth and maxIndirectDepth",
		binds: "seen map[reflect.Type]seenForm memo + depth/ptrChain caps; compile-time type",
		reachingPaths: "one: seen created per SchemaFor, per Go type"},

	// ---- valueTree / wire / textTree: node count IS input size ----
	{fn: "walkDefault", file: "schema.go", class: valueTree,
		factors: "default value nodes",
		binds: "the walk follows the concrete default VALUE (a finite JSON tree), linear in it",
		reachingPaths: "one: per default-encode pass, following the value"},
	{fn: "coerceDefault", file: "schema.go", class: valueTree,
		factors: "default value nodes, bounded by depth",
		binds: "value-guided recursion + the depth>=maxDepth cap",
		reachingPaths: "one: per default coercion at parse, following the value"},
	{fn: "coerceMetadataDefault", file: "schema_node.go", class: valueTree,
		factors: "default value nodes",
		binds: "value-guided recursion over the concrete default (name-ref follows are one hop, guided by the value)",
		reachingPaths: "one: per Root() metadata default coercion, following the value"},
	{fn: "branchAcceptsDefault", file: "schema_node.go", class: valueTree,
		factors: "default value nodes",
		binds: "value-guided recursion over the concrete default",
		reachingPaths: "one: per branch-acceptance check, following the value"},
	{fn: "encodeDefaultDepth", file: "resolve.go", class: valueTree,
		factors: "default value nodes, bounded by depth",
		binds: "value-guided recursion + the depth>=maxDepth cap",
		reachingPaths: "one: per default encode, following the value"},
	{fn: "appendAvroJSON", file: "json_codec.go", class: valueTree,
		factors: "encoded value nodes, bounded by depth",
		binds: "value-guided recursion + the depth>=maxDepth cap",
		reachingPaths: "one: per EncodeJSON/AppendEncodeJSON call, following the value"},
	{fn: "valueWalkLimit", file: "schema_node.go", class: valueTree,
		factors: "value nodes x depth",
		binds: "walkBudget + the depthLeft cap",
		reachingPaths: "one walkBudget per Props/value bounding pass (shared with toJSONWalk's budget)"},
	{fn: "inlineTreeDefs", file: "cache.go", class: textTree,
		factors: "JSON tree nodes (each definition inlined once)",
		binds: "the seen/inlined map[string]bool sets: a name already inlined is emitted as a reference, so the output is linear in the definition set",
		reachingPaths: "one: seen/inlined created per SchemaCache self-containment splice"},
	{fn: "build", file: "schema.go", class: textTree,
		factors: "aschema text nodes, bounded by depth",
		binds: "the parsed aschema is a TREE (each occurrence is its own text); depth>=maxDepth caps recursion",
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
	"custom map[*schemaNode]*customWiring":        "a DATA map of node->custom wiring, not a visited set",
	"custom      map[*schemaNode]*customWiring":   "a DATA map of node->custom wiring, not a visited set",
	"customMatch map[*schemaNode]string":          "a DATA map of node->matched-custom-name, not a visited set",
	"overlayDone map[*schemaNode]bool":            "presence state recording which nodes' overlay ran, carried on the builder across nests; the WALK that fills it (overlayInheritedCustom) is rowed",
	"building     map[*schemaNode]struct{}":       "the record-in-progress set for build-time cycle detection, carried on the builder; not a per-walk visited",
	"building:   make(map[*schemaNode]struct{})":  "initializes the builder's record-in-progress set (two sites: schema.go and cache.go)",
	"b.overlayDone = make(map[*schemaNode]bool)":  "re-inits the builder overlay presence state",
	"b.customMatch = make(map[*schemaNode]string)": "re-inits the builder custom-match data map",
	"b.custom = make(map[*schemaNode]*customWiring":"re-inits the builder custom data map",
	"b.building = make(map[*schemaNode]struct{})":  "re-inits the builder record-in-progress set",
	"seen := make(map[reflect.Type]seenForm)":      "SchemaFor's inferType memo init; the walk (inferType) is rowed",
	"seen map[reflect.Type]seenForm":               "inferType/inferRecord/inferField memo parameter; inferType is rowed",
	"collectFields(t, make(map[reflect.Type]bool))":"inits collectFieldsRaw's visited; the walk is rowed",
	"visited map[reflect.Type]bool":                "collect/collectFieldsRaw visited parameter; both rowed",
	"make(map[reflect.Type]bool)":                  "inits a Go-type walk visited set; the walks (collect/collectFieldsRaw) are rowed",
	"sync.Map // map[reflect.Type]":                "a per-Go-type compiled-codec cache, amortized and keyed by fixed types; not a walk",
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
}

var minBytesConstructionSites = []minBytesConstructionSite{
	{"deser.go", "return newMinBytesWalk().minBytesOf(n)", "standalone schemaMinBytes: ONE node, outside any container loop (the only fresh-per-call form)"},
	{"schema.go", "minBytes:   newMinBytesWalk()", "the builder's b.minBytes seeded in Parse — the BUILD path (backward refs resolve to a built node here)"},
	{"schema.go", "b.minBytes = newMinBytesWalk()", "lazy seed at the root's first build, before any nest, so a directly-constructed (white-box) builder still shares one walk across the build path"},
	{"schema.go", "mbw := newMinBytesWalk()", "one walk before finalize's container-fixup loop — the FINALIZE path (forward refs)"},
	{"cache.go", "minBytes:   newMinBytesWalk()", "SchemaCache's builder b.minBytes — the build path via the cache"},
	{"resolve.go", "minBytes: newMinBytesWalk()", "resolveCtx.minBytes, shared across one Resolve — the RESOLVE path"},
	{"skip.go", "mbw := newMinBytesWalk()", "one walk per record's once.Do skip compile — the SKIP path (cross-record cost is wire-bounded)"},
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
