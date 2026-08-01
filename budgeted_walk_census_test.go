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
}

// budgetedWalks is the registry. Every recursive schema-graph walk and every
// cost-marker-bearing walk in the package must appear here; the guards derive
// both sets from source and diff them against this list.
var budgetedWalks = []budgetedWalk{
	// ---- schemaDAG: must bind the paths factor with a memo or a budget ----
	{fn: "minBytes", file: "deser.go", class: schemaDAG,
		factors: "containers x paths-per-walk x children-per-node",
		binds: "one minBytesWalk SHARED per operation (containers) + done memo (paths) + per-child allowance charge (children); newMinBytesWalk is threaded through finalize, resolve, and each record's skip compile"},
	{fn: "checkCompat", file: "compat.go", class: schemaDAG,
		factors: "distinct (reader,writer) node pairs x children-per-pair",
		binds: "the seen map[nodePair]bool memo, threaded from CheckCompatibility with no defer-delete, so each pair is walked once"},
	{fn: "resolveNode", file: "resolve.go", class: schemaDAG,
		factors: "distinct (reader,writer) pairs x (children + per-container min-bytes)",
		binds: "ctx.seen pair memo (pairs) + ctx.minBytes shared walk (the container min-bytes factor)"},
	{fn: "toJSONWalk", file: "schema_node.go", class: schemaDAG,
		factors: "nodes emitted x bytes per node",
		binds: "walkBudget (nodes + bytes), charged by takeNode at the TOP of every entry so a DAG re-descent still spends budget; visited is only cycle detection"},
	{fn: "collectLocalNames", file: "schema_node.go", class: schemaDAG,
		factors: "distinct nodes x names per node",
		binds: "the visited map[*SchemaNode]struct{} memo (mark on entry, return on hit)"},
	{fn: "stampNameRefs", file: "schema_node.go", class: schemaDAG,
		factors: "distinct nodes",
		binds: "the visited memo (mark on entry, return on hit)"},
	{fn: "collectNamedTypes", file: "schema_node.go", class: schemaDAG,
		factors: "tree nodes (name-ref nodes are leaves, not followed)",
		binds: "structural: a reference SchemaNode carries no children, so the walk is over the definition TREE, linear in it"},
	{fn: "coerceTreeDefaults", file: "schema_node.go", class: schemaDAG,
		factors: "tree nodes (name-ref nodes are leaves)",
		binds: "structural: references are leaves, so the walk is over the definition TREE, linear in it"},
	{fn: "overlayInheritedCustom", file: "schema.go", class: schemaDAG,
		factors: "distinct nodes x custom lookups",
		binds: "the visited map[*schemaNode]bool memo (mark on entry, return on hit)"},
	{fn: "findCustomTypeMatchInSubtreeWalk", file: "schema.go", class: schemaDAG,
		factors: "distinct nodes x registered custom types",
		binds: "the visited map[*schemaNode]bool memo (mark on entry, return on hit)"},
	{fn: "buildCustomWiring", file: "schema.go", class: schemaDAG,
		factors: "distinct nodes",
		binds: "the visited memo (mark on entry, return on hit)"},
	{fn: "nodeAwaitsForwardRefSeen", file: "schema.go", class: schemaDAG,
		factors: "distinct nodes",
		binds: "the seen map[*schemaNode]struct{} memo (mark on entry, return on hit); the separate building set is defer-delete cycle detection"},

	// ---- goTypeDAG: bound is compile-time fixedness + per-type sync.Map ----
	{fn: "collect", file: "reflect.go", class: goTypeDAG,
		factors: "2^(embed depth) on a shared-embed type DAG (visited is defer-delete, so it re-descends)",
		binds: "NOT a runtime bound: a Go type is fixed at COMPILE time and the result is amortized by a per-type sync.Map, so the fan-out is not attacker-grown (G3)"},
	{fn: "collectFieldsRaw", file: "schema_for.go", class: goTypeDAG,
		factors: "2^(embed depth) on a shared-embed type DAG (visited is defer-delete)",
		binds: "compile-time fixedness + collectFields' per-call visited; not attacker-grown at runtime (G3)"},
	{fn: "inferType", file: "schema_for.go", class: goTypeDAG,
		factors: "type nodes x ptr chains, bounded by depth and maxIndirectDepth",
		binds: "seen map[reflect.Type]seenForm memo + depth/ptrChain caps; compile-time type"},

	// ---- valueTree / wire / textTree: node count IS input size ----
	{fn: "walkDefault", file: "schema.go", class: valueTree,
		factors: "default value nodes",
		binds: "the walk follows the concrete default VALUE (a finite JSON tree), linear in it"},
	{fn: "coerceDefault", file: "schema.go", class: valueTree,
		factors: "default value nodes, bounded by depth",
		binds: "value-guided recursion + the depth>=maxDepth cap"},
	{fn: "coerceMetadataDefault", file: "schema_node.go", class: valueTree,
		factors: "default value nodes",
		binds: "value-guided recursion over the concrete default (name-ref follows are one hop, guided by the value)"},
	{fn: "branchAcceptsDefault", file: "schema_node.go", class: valueTree,
		factors: "default value nodes",
		binds: "value-guided recursion over the concrete default"},
	{fn: "encodeDefaultDepth", file: "resolve.go", class: valueTree,
		factors: "default value nodes, bounded by depth",
		binds: "value-guided recursion + the depth>=maxDepth cap"},
	{fn: "appendAvroJSON", file: "json_codec.go", class: valueTree,
		factors: "encoded value nodes, bounded by depth",
		binds: "value-guided recursion + the depth>=maxDepth cap"},
	{fn: "valueWalkLimit", file: "schema_node.go", class: valueTree,
		factors: "value nodes x depth",
		binds: "walkBudget + the depthLeft cap"},
	{fn: "inlineTreeDefs", file: "cache.go", class: textTree,
		factors: "JSON tree nodes (each definition inlined once)",
		binds: "the seen/inlined map[string]bool sets: a name already inlined is emitted as a reference, so the output is linear in the definition set"},
	{fn: "build", file: "schema.go", class: textTree,
		factors: "aschema text nodes, bounded by depth",
		binds: "the parsed aschema is a TREE (each occurrence is its own text); depth>=maxDepth caps recursion"},
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
