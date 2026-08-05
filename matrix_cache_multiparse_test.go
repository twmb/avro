package avro_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// This file is the generative MULTI-PARSE SchemaCache net (Family 7:
// 807c6d9 → 7cab9bd → de3dca3 → 254eee0). A schema built via a SchemaCache that
// references named types defined in PRIOR Parse calls resolves those references
// in the node tree (Encode/Decode work), but the JSON-derived metadata forms
// (Fingerprint / Canonical / Root / String) used to keep a dangling bare
// reference — non-self-contained, fingerprint-divergent, SOE/registry-interop-
// broken. The four-commit dribble fixed it one shape at a time: splice the
// inherited def in (807c6d9), preserve doc/order/props (7cab9bd), keep the
// spliced def namespace-stable (de3dca3), and dedupe overlapping/diamond defs
// (254eee0).
//
// The existing regressions pin those shapes as POINT cases, and
// TestMatrix_CacheSelfContainedNamespaces crosses a SINGLE cross-parse
// reference against namespace/position/kind. This net is the missing
// CROSS-PRODUCT over the reference-graph TOPOLOGY — single, transitive chains,
// diamonds, wide overlap, diamond-with-a-chain-arm, a nested type referenced
// before its container, and a single type referencing the same leaf TWICE
// (repeat2 / repeat_chain) — each crossed with the namespace regime
// (null / single-namespace / split-namespace), the position the shared leaf
// sits in (record field / array items / map values / union branch), the
// leaf kind (record / enum / fixed), AND the cross-parse reference SPELLING
// (bare "X" vs wrapped {"type":"X"}). Crossing topology with the other axes is
// where a gap in the dribble would hide; a point test per topology cannot
// reach it.
//
// The SPELLING axis is the one a later fix (collapse a non-splicing wrapped
// reference to bare) exposed: a cross-parse reference is accepted both bare and
// wrapped (NOT_BUGS #23), so the self-contained metadata must be identical for
// either. The bug shape it catches has two layers — (1) a wrapped cross-parse
// reference that splices must be replaced as a WHOLE (else it self-contains as
// the invalid {"type":{X-def}} and the rebuild silently falls back to a dangling
// reference); (2) a LATER wrapped occurrence of a type whose first occurrence was
// inlined must COLLAPSE to bare "X", else {"type":"X"} survives in String() where
// the canonical bare form belongs. Layer 2 is invisible to every single-reference
// topology and is exactly what repeat2 / repeat_chain / local_forwardref add.
//
// For every shape the cache-built schema is compared to a logically-identical
// inline twin emitted by an INDEPENDENT first-occurrence oracle (mpEmitTwin —
// DFS pre-order, full at first occurrence, bare fullname after, exactly Java's
// NamedSchema.writeNameRef rule). The twin is spelling-INDEPENDENT, so it anchors
// both spellings to one canonical form. The wire bytes for a sample value are the
// oracle-independent anchor: equal wire proves the two schemas ARE the same
// logical schema (the node tree resolved identically), so ANY divergence in
// Fingerprint / Canonical / Root / String is then provably a metadata-form bug,
// not a different schema. Non-vacuity is verified by neutering each fix in
// cache.go and observing the failures (see the test's closing comment).

// mpNode is one named type in an abstract multi-parse reference graph.
type mpNode struct {
	full  string   // fullname ("D" in the null namespace, else "ns.D")
	kind  string   // "record" | "enum" | "fixed"
	edges []mpEdge // record children, in field order (records only)
}

// mpEdge is a reference from a record to a named child, sitting in some
// position (a plain field, or inside an array / map / union).
type mpEdge struct {
	to    string // child fullname
	field string // field name carrying the reference
	pos   string // "field" | "array" | "map" | "union"
}

// mpGraph is a reference DAG plus its root and any types defined NESTED inside
// another type's parse (rather than registered standalone).
type mpGraph struct {
	nodes  map[string]*mpNode
	root   string
	nested map[string]string // child fullname → container it is defined inside
}

func mpShort(full string) string {
	if i := strings.LastIndex(full, "."); i >= 0 {
		return full[i+1:]
	}
	return full
}

func mpNS(full string) string {
	if i := strings.LastIndex(full, "."); i >= 0 {
		return full[:i]
	}
	return ""
}

func mpFull(ns, short string) string {
	if ns == "" {
		return short
	}
	return ns + "." + short
}

// mpPosWrap places x (a type definition object or a bare fullname-reference
// string) into the chosen reference position.
func mpPosWrap(pos string, x any) any {
	switch pos {
	case "array":
		return map[string]any{"type": "array", "items": x}
	case "map":
		return map[string]any{"type": "map", "values": x}
	case "union":
		return []any{"null", x}
	default: // field
		return x
	}
}

// mpNamedObj builds the JSON object for a named type. Every named object
// carries an EXPLICIT namespace (including "" — the null-namespace escape), so
// the spelling is identical to what the cache stores via
// defWithExplicitNamespace and the normalized String() forms compare
// byte-for-byte regardless of enclosing scope. childType supplies each record
// field's type (a nested def for a first occurrence, a bare fullname after).
//
// Every type and field also carries doc / a custom prop, and every field an
// "order" — the non-canonical attributes commit 7cab9bd preserves through the
// splice. They are stripped from Canonical (so it still matches) but must
// survive in String()/Root(); a splice that rebuilt from the attribute-poor
// node tree would drop them and fail this net.
func mpNamedObj(n *mpNode, childType func(e mpEdge) any) map[string]any {
	o := map[string]any{
		"type": n.kind, "name": mpShort(n.full), "namespace": mpNS(n.full),
		"doc": mpShort(n.full) + " doc", "io.tag": mpShort(n.full) + "-tag",
	}
	switch n.kind {
	case "record":
		if len(n.edges) == 0 {
			o["fields"] = []any{mpField("n", "int")}
		} else {
			fields := make([]any, len(n.edges))
			for i, e := range n.edges {
				fields[i] = mpField(e.field, childType(e))
			}
			o["fields"] = fields
		}
	case "enum":
		o["symbols"] = []any{"A", "B"}
	case "fixed":
		o["size"] = 2
	}
	return o
}

// mpField builds a record field object carrying doc / order / a custom prop —
// field-level attributes that live on the field, not the type, and must survive
// the splice (which rewrites only the field's "type" value).
func mpField(name string, typ any) map[string]any {
	return map[string]any{
		"name": name, "type": typ,
		"doc": name + " fdoc", "order": "ignore", "io.fprop": name + "-fp",
	}
}

// mpEmitTwin is the independent oracle: the single self-contained inline schema
// logically identical to the cache-built one. It walks the reference graph from
// the root in DFS pre-order, emitting each named type's full definition at its
// FIRST occurrence and a bare fullname reference at every later one — the
// canonical first-occurrence form (Java's writeNameRef; the cache's splice
// reaches the same shape by deduping). The oracle never consults the cache
// machinery, so a cache↔twin divergence is a real metadata-form bug.
func mpEmitTwin(g *mpGraph) string {
	seen := map[string]bool{}
	var emit func(full string) any
	emit = func(full string) any {
		if seen[full] {
			return full // already defined earlier: a bare fullname reference
		}
		seen[full] = true
		return mpNamedObj(g.nodes[full], func(e mpEdge) any { return mpPosWrap(e.pos, emit(e.to)) })
	}
	b, err := json.Marshal(emit(g.root))
	if err != nil {
		panic(err)
	}
	return string(b)
}

// mpRefSpell renders a cross-parse name reference in the chosen SPELLING — the
// axis the topology cross had missed. Avro accepts a name reference written
// two ways (NOT_BUGS #23): the bare fullname string "X", and the wrapped form
// {"type":"X"} whose sole key is "type". Both resolve to the same node, so the
// wire is identical; the splice that self-contains a cache schema must reach the
// same metadata for either spelling. The bug surface is the wrapped form: a
// wrapped cross-parse reference once hit the splice's general map path (recursing
// INTO the "type" value → invalid {"type":{X-def}}), so String()/Canonical()
// silently fell back to a dangling reference while the bare form self-contained.
func mpRefSpell(spelling, ref string) any {
	if spelling == "wrapped" {
		return map[string]any{"type": ref}
	}
	return ref
}

// mpEmitStandalone emits one type's standalone schema string for a cache Parse.
// References to other (already-registered) types are rendered in the given
// spelling (bare fullname or wrapped {"type":...}); a child marked
// nested-in-this-type is emitted inline (it is registered by THIS parse).
func mpEmitStandalone(full string, g *mpGraph, spelling string) string {
	n := g.nodes[full]
	tree := mpNamedObj(n, func(e mpEdge) any {
		if g.nested[e.to] == full {
			child := mpNamedObj(g.nodes[e.to], func(ce mpEdge) any { return mpPosWrap(ce.pos, mpRefSpell(spelling, ce.to)) })
			return mpPosWrap(e.pos, child)
		}
		return mpPosWrap(e.pos, mpRefSpell(spelling, e.to))
	})
	b, err := json.Marshal(tree)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// mpSampleValue builds a value the schema accepts, for the wire-equality anchor.
// Union positions take the non-null branch so the referenced type is exercised.
func mpSampleValue(full string, g *mpGraph) any {
	n := g.nodes[full]
	switch n.kind {
	case "enum":
		return "A"
	case "fixed":
		return make([]byte, 2)
	default: // record
		m := map[string]any{}
		if len(n.edges) == 0 {
			m["n"] = int32(0)
			return m
		}
		for _, e := range n.edges {
			cv := mpSampleValue(e.to, g)
			switch e.pos {
			case "array":
				m[e.field] = []any{cv}
			case "map":
				m[e.field] = map[string]any{"k": cv}
			default: // field, union (non-null branch)
				m[e.field] = cv
			}
		}
		return m
	}
}

// mpNormJSON re-marshals a JSON string into a canonical key-sorted form, so two
// schema strings that differ only in key order / whitespace compare equal.
// UseNumber keeps numeric literals verbatim, matching the cache's
// precision-preserving rebuild.
func mpNormJSON(t *testing.T, s string) string {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("normalize %q: %v", s, err)
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("normalize marshal: %v", err)
	}
	return string(b)
}

// mpBuildTopo constructs one reference-graph topology in the given namespace
// regime, with the shared leaf of the given kind, and the position axis applied
// to the single edge that points AT the shared leaf (so the leaf is exercised
// in a field / array / map / union). It returns the graph and the standalone
// parse order (dependencies first); ok is false for an unsupported combination.
func mpBuildTopo(name, regime, leafKind, pos string) (g *mpGraph, parseOrder []string, ok bool) {
	carrierNS := map[string]string{"null": "", "ns": "ns", "mixed": "a"}[regime]
	leafNS := map[string]string{"null": "", "ns": "ns", "mixed": "b"}[regime]

	mk := func(root string, nodes ...*mpNode) *mpGraph {
		g := &mpGraph{nodes: map[string]*mpNode{}, root: root, nested: map[string]string{}}
		for _, n := range nodes {
			g.nodes[n.full] = n
		}
		return g
	}
	rec := func(full string, edges ...mpEdge) *mpNode { return &mpNode{full: full, kind: "record", edges: edges} }
	leaf := func(full string) *mpNode { return &mpNode{full: full, kind: leafKind} }
	e := func(to, field, p string) mpEdge { return mpEdge{to: to, field: field, pos: p} }

	R := mpFull(carrierNS, "R")
	D := mpFull(leafNS, "D")

	switch name {
	case "single":
		return mk(R, rec(R, e(D, "d", pos)), leaf(D)), []string{D}, true

	case "chain2":
		B := mpFull(carrierNS, "B")
		return mk(R, rec(R, e(B, "b", "field")), rec(B, e(D, "d", pos)), leaf(D)),
			[]string{D, B}, true

	case "chain3":
		B, C := mpFull(carrierNS, "B"), mpFull(carrierNS, "C")
		return mk(R, rec(R, e(B, "b", "field")), rec(B, e(C, "c", "field")), rec(C, e(D, "d", pos)), leaf(D)),
			[]string{D, C, B}, true

	case "diamond":
		B, C := mpFull(carrierNS, "B"), mpFull(carrierNS, "C")
		return mk(R, rec(R, e(B, "b", "field"), e(C, "c", "field")),
				rec(B, e(D, "d", pos)), rec(C, e(D, "d", "field")), leaf(D)),
			[]string{D, B, C}, true

	case "wide3":
		B, C, E := mpFull(carrierNS, "B"), mpFull(carrierNS, "C"), mpFull(carrierNS, "E")
		return mk(R, rec(R, e(B, "b", "field"), e(C, "c", "field"), e(E, "e", "field")),
				rec(B, e(D, "d", pos)), rec(C, e(D, "d", "field")), rec(E, e(D, "d", "field")), leaf(D)),
			[]string{D, B, C, E}, true

	case "diamond_chain":
		// R→{B,C}; B→M→D, C→D. D is shared at depth 2 (via M) and depth 1 (via C).
		B, C, M := mpFull(carrierNS, "B"), mpFull(carrierNS, "C"), mpFull(carrierNS, "M")
		return mk(R, rec(R, e(B, "b", "field"), e(C, "c", "field")),
				rec(B, e(M, "m", "field")), rec(M, e(D, "d", pos)), rec(C, e(D, "d", "field")), leaf(D)),
			[]string{D, M, C, B}, true

	case "nested_before":
		// Outer is defined standalone with a NESTED Inner; R references Inner
		// (first, in `pos`) then Outer. Inner's def thus arrives twice — via the
		// standalone reference and inside Outer — so the splice must dedupe it.
		Inner := mpFull(leafNS, "Inner")
		Outer := mpFull(carrierNS, "Outer")
		g := mk(R, rec(R, e(Inner, "f1", pos), e(Outer, "f2", "field")),
			rec(Outer, e(Inner, "inner", "field")), leaf(Inner))
		g.nested[Inner] = Outer
		return g, []string{Outer}, true

	case "repeat2":
		// R references the SAME cached leaf D twice: d0 at a plain field (the
		// first occurrence, which the splice inlines as a full definition) and
		// d1 at `pos` (a LATER occurrence the splice must leave as a bare
		// reference). Under the wrapped spelling d1 is {"type":"D"} and reaches
		// the splice's no-splice fall-through (already-inlined) — exactly the
		// path the single-reference topologies never exercise. The wrapper must
		// collapse to bare "D" there, else String() diverges from the bare-
		// spelled / inline twin (whose later occurrence is bare "D").
		return mk(R, rec(R, e(D, "d0", "field"), e(D, "d1", pos)), leaf(D)), []string{D}, true

	case "repeat_chain":
		// The same repeated reference one level down: a cross-parse carrier B
		// references D twice (b0 field, b1 at `pos`), and R references B. B's
		// stored self-contained definition therefore already carries a later
		// reference to D; the wrapper on that stored reference must be collapsed
		// when B itself is self-contained AND it must survive re-splicing into R
		// as a bare reference, not a re-wrapped one.
		B := mpFull(carrierNS, "B")
		return mk(R, rec(R, e(B, "b", "field")), rec(B, e(D, "b0", "field"), e(D, "b1", pos)), leaf(D)),
			[]string{D, B}, true
	}
	return nil, nil, false
}

// mpRootEqual deep-compares two Root() SchemaNode trees. Root() resolves
// namespaces and preserves every attribute (doc/order/props/aliases/defaults),
// so equal trees mean the cache and inline String()/Root() forms describe the
// same schema down to every attribute — independent of bare-vs-dotted or
// inherited-vs-explicit spelling.
func mpRootEqual(a, b avro.SchemaNode) bool { return reflect.DeepEqual(a, b) }

// mpRunCache parses deps into a fresh SchemaCache (in order) and returns the
// schema for the final root parse.
func mpRunCache(t *testing.T, deps []string, root string) *avro.Schema {
	t.Helper()
	var c avro.SchemaCache
	for _, d := range deps {
		if _, err := c.Parse(d); err != nil {
			t.Fatalf("register %q: %v", d, err)
		}
	}
	s, err := c.Parse(root)
	if err != nil {
		t.Fatalf("cache parse root %q: %v", root, err)
	}
	return s
}

// mpAssertSelfContained is the four-form differential: a cache-built schema must
// match its logically-identical inline twin on Fingerprint / Canonical / Root /
// String, and every metadata form must re-parse standalone. The wire bytes (and
// a decode round-trip) are the oracle-independent anchor: equal wire proves the
// two are the same logical schema, so any metadata divergence is a real bug.
//
// canonReparses says whether Parse(Canonical()) is expected to succeed.
// Canonical (PCF) drops namespace attributes and writes fullnames, so a null-
// namespace type nested in a namespaced scope re-reads as inheriting that scope
// — an intentionally non-re-parseable, fingerprint-faithful form (NOT_BUGS #25,
// Java emits byte-identical ambiguity). The four forms still match the twin and
// the fingerprint is still correct; only standalone re-parse of the canonical
// is given up. String() keeps the explicit "namespace":"" escape, so it always
// re-parses.
func mpAssertSelfContained(t *testing.T, viaCache, inline *avro.Schema, val any, cacheSchema, twinSchema string, canonReparses bool) {
	t.Helper()

	wc, err := viaCache.AppendEncode(nil, val)
	if err != nil {
		t.Fatalf("cache encode: %v\n schema=%s", err, cacheSchema)
	}
	wi, err := inline.AppendEncode(nil, val)
	if err != nil {
		t.Fatalf("inline encode: %v\n twin=%s", err, twinSchema)
	}
	if !bytes.Equal(wc, wi) {
		t.Fatalf("twin is not logically identical (wire differs):\n cache =%x\n inline=%x\n cacheSchema=%s\n twin=%s", wc, wi, cacheSchema, twinSchema)
	}
	var ac, ai any
	if _, err := viaCache.Decode(wc, &ac); err != nil {
		t.Fatalf("cache decode: %v", err)
	}
	if _, err := inline.Decode(wi, &ai); err != nil {
		t.Fatalf("inline decode: %v", err)
	}
	if !matEqual(ac, ai) {
		t.Errorf("decoded values differ:\n c=%#v\n i=%#v", ac, ai)
	}

	// Canonical + Fingerprint (the cross-language / SOE interop forms).
	if cc, ic := string(viaCache.Canonical()), string(inline.Canonical()); cc != ic {
		t.Errorf("Canonical diverges:\n cache : %s\n inline: %s", cc, ic)
	}
	if !bytes.Equal(viaCache.Fingerprint(avro.NewRabin()), inline.Fingerprint(avro.NewRabin())) {
		t.Errorf("Fingerprint diverges (SOE/registry interop break)")
	}

	// String: normalized byte-equality (every preserved attribute, every named
	// type at its first occurrence).
	if cs, is := mpNormJSON(t, viaCache.String()), mpNormJSON(t, inline.String()); cs != is {
		t.Errorf("String diverges:\n cache : %s\n inline: %s", cs, is)
	}

	// Root: attribute-complete, namespace-resolved structural form.
	if !mpRootEqual(*viaCache.Root(), *inline.Root()) {
		t.Errorf("Root diverges:\n cache : %+v\n inline: %+v", viaCache.Root(), inline.Root())
	}

	// Self-containment: every metadata form re-parses standalone (except a
	// canonical with PCF-lossy null-ns-in-namespaced nesting — see canonReparses).
	if canonReparses {
		if _, err := avro.Parse(string(viaCache.Canonical())); err != nil {
			t.Errorf("Parse(cache.Canonical()) FAILS — not self-contained: %v\n %s", err, viaCache.Canonical())
		}
	}
	if _, err := avro.Parse(viaCache.String()); err != nil {
		t.Errorf("Parse(cache.String()) FAILS — not self-contained: %v\n %s", err, viaCache.String())
	}
	croot := viaCache.Root()
	if _, err := croot.Schema(); err != nil {
		t.Errorf("cache.Root().Schema() FAILS to rebuild: %v", err)
	}
}

// mpJSON marshals a generic tree to a compact JSON string.
func mpJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// mpRecObj builds an attributed record object with the given field objects.
func mpRecObj(full string, fields ...any) map[string]any {
	return map[string]any{
		"type": "record", "name": mpShort(full), "namespace": mpNS(full),
		"doc": mpShort(full) + " doc", "io.tag": mpShort(full) + "-tag",
		"fields": fields,
	}
}

// mpFwdObj builds an attributed named def for the forward-ref family: a record
// with nf int fields (v0..), or an enum / fixed. Two same-short-name types are
// made wire-distinguishable by their field count, so a mis-bound reference
// shows up in the wire-equality anchor.
func mpFwdObj(full, kind string, nf int) map[string]any {
	o := map[string]any{
		"type": kind, "name": mpShort(full), "namespace": mpNS(full),
		"doc": mpShort(full) + " doc", "io.tag": mpShort(full) + "-tag",
	}
	switch kind {
	case "record":
		fs := make([]any, nf)
		for i := range nf {
			fs[i] = mpField(fmt.Sprintf("v%d", i), "int")
		}
		o["fields"] = fs
	case "enum":
		o["symbols"] = []any{"A", "B"}
	case "fixed":
		o["size"] = 2
	}
	return o
}

// mpFwdVal builds a value for an mpFwdObj of the given kind / field count.
func mpFwdVal(kind string, nf int) any {
	switch kind {
	case "enum":
		return "A"
	case "fixed":
		return make([]byte, 2)
	default:
		m := map[string]any{}
		for i := range nf {
			m[fmt.Sprintf("v%d", i)] = int32(0)
		}
		return m
	}
}

// mpEmitInheritedWrapper registers leafFull NESTED inside a wrapper record whose
// namespace the leaf INHERITS — the leaf definition itself carries no explicit
// "namespace". This is the de3dca3 surface: such a definition, stored for later
// cross-parse splicing, must keep its resolved (inherited) fullname when spliced
// into a DIFFERENTLY-namespaced reference site, not re-inherit that site's
// namespace. (mpBuildTopo's "mixed" regime puts the leaf in namespace "b" and
// the carriers/root in "a", so the splice site's namespace differs.)
func mpEmitInheritedWrapper(leafFull, kind string) string {
	leafDef := mpNamedObj(&mpNode{full: leafFull, kind: kind}, func(mpEdge) any { return "int" })
	delete(leafDef, "namespace") // drop the explicit namespace → inherit the wrapper's
	wrap := map[string]any{
		"type": "record", "name": "Wrap", "namespace": mpNS(leafFull),
		"doc": "Wrap doc", "io.tag": "Wrap-tag",
		"fields": []any{map[string]any{
			"name": "w", "type": leafDef,
			"doc": "w fdoc", "order": "ignore", "io.fprop": "w-fp",
		}},
	}
	return mpJSON(wrap)
}

func TestMatrix_SchemaCacheMultiParseSelfContained(t *testing.T) {
	topos := []string{"single", "chain2", "chain3", "diamond", "wide3", "diamond_chain", "nested_before", "repeat2", "repeat_chain"}
	regimes := []string{"null", "ns", "mixed"}
	positions := []string{"field", "array", "map", "union"}
	kinds := []string{"record", "enum", "fixed"}
	// The spelling axis the net had missed. A cross-parse reference is written
	// either as the bare fullname "X" or the wrapped {"type":"X"} (both accepted,
	// NOT_BUGS #23); the self-contained metadata must be identical for either,
	// since the wire is. The twin is spelling-INDEPENDENT (always the canonical
	// first-occurrence inline form), so it anchors both spellings: bare is the
	// control that already self-contained, wrapped is the form whose splice was
	// the bug. The repeat2 / repeat_chain topologies make the wrapped column
	// non-vacuous — a LATER wrapped occurrence of an inlined type must collapse
	// to bare in String(), not survive as {"type":"X"}.
	spellings := []string{"bare", "wrapped"}

	var cells int

	// --- core net: spelling × topology × namespace regime × position × leaf kind ---
	for _, spelling := range spellings {
		for _, topo := range topos {
			for _, regime := range regimes {
				for _, pos := range positions {
					for _, kind := range kinds {
						g, parseOrder, ok := mpBuildTopo(topo, regime, kind, pos)
						if !ok {
							continue
						}
						name := fmt.Sprintf("%s/%s/%s/%s/%s", spelling, topo, regime, pos, kind)
						t.Run(name, func(t *testing.T) {
							cells++
							deps := make([]string, len(parseOrder))
							for i, fn := range parseOrder {
								deps[i] = mpEmitStandalone(fn, g, spelling)
							}
							rootSchema := mpEmitStandalone(g.root, g, spelling)
							viaCache := mpRunCache(t, deps, rootSchema)

							twinSchema := mpEmitTwin(g)
							inline, err := avro.Parse(twinSchema)
							if err != nil {
								t.Fatalf("inline twin parse %q: %v", twinSchema, err)
							}
							// Core net never nests a null-ns type in a namespaced scope
							// (a regime is all-null or all-namespaced), so the canonical
							// always re-parses.
							mpAssertSelfContained(t, viaCache, inline, mpSampleValue(g.root, g), rootSchema, twinSchema, true)
						})
					}
				}
			}
		}
	}

	// --- forward-ref family: positional binding of a reference relative to a
	// LOCAL definition, including the namespace-shadow corner ---
	//
	// inlineTreeDefs binds a reference the way the parser does: eager, in-scope-
	// first, and POSITIONAL — a local definition wins only for references AFTER
	// it; a reference BEFORE it binds the cache-inherited type. The cases below
	// cross the reference/def order with a distinct-name vs same-short-name-
	// across-namespaces collision; each expressible case carries an inline twin
	// for the four-form differential.
	type fwd struct {
		name           string
		deps           []string
		root           string
		twin           string // "" → inexpressible corner: binding-safe assertion only
		value          any
		canonNoReparse bool // canonical is PCF-lossy (null-ns nested in a namespaced scope)
	}
	var cases []fwd

	// The cached cross-parse reference is crossed with the spelling axis: it
	// splices in place (the local def is a distinct type or not yet in scope),
	// so the rebuild normalizes either spelling and the four forms match the
	// full-def twin. Wrapped here is the splice surface reached at the
	// positional / shadow corner the single-spelling family never crossed.
	for _, spelling := range spellings {
		for _, k := range kinds {
			P, L, R := "x.P", "x.L", "x.R"
			depP := mpJSON(mpFwdObj(P, k, 1))
			// Reference to the cached type BEFORE a local def of a distinct type:
			// the ref splices, the local def stays.
			cases = append(cases, fwd{
				name:  "distinct_ref_before_def/" + spelling + "/" + k,
				deps:  []string{depP},
				root:  mpJSON(mpRecObj(R, mpField("f1", mpRefSpell(spelling, P)), mpField("f2", mpFwdObj(L, k, 1)))),
				twin:  mpJSON(mpRecObj(R, mpField("f1", mpFwdObj(P, k, 1)), mpField("f2", mpFwdObj(L, k, 1)))),
				value: map[string]any{"f1": mpFwdVal(k, 1), "f2": mpFwdVal(k, 1)},
			})
			// Local def of a distinct type BEFORE the cached reference.
			cases = append(cases, fwd{
				name:  "distinct_def_before_ref/" + spelling + "/" + k,
				deps:  []string{depP},
				root:  mpJSON(mpRecObj(R, mpField("f1", mpFwdObj(L, k, 1)), mpField("f2", mpRefSpell(spelling, P)))),
				twin:  mpJSON(mpRecObj(R, mpField("f1", mpFwdObj(L, k, 1)), mpField("f2", mpFwdObj(P, k, 1)))),
				value: map[string]any{"f1": mpFwdVal(k, 1), "f2": mpFwdVal(k, 1)},
			})
		}
	}

	// Shadow corner, expressible direction: a bare ref to a cached NULL-namespace
	// type appears BEFORE a local same-short-name type in the enclosing
	// namespace. Eager positional binding sends the bare ref to the cached
	// null-namespace type (the local x.T is not yet in scope); the splice must
	// inline the 1-field null-ns T, not the 2-field x.T. Field counts differ so a
	// mis-bind would change the wire.
	{
		T, xT, R := "T", "x.T", "x.R"
		depT := mpJSON(mpFwdObj(T, "record", 1))
		posVal := func(pos string, v any) any {
			switch pos {
			case "array":
				return []any{v}
			case "map":
				return map[string]any{"k": v}
			default: // field, union (non-null branch)
				return v
			}
		}
		// Cross the shadowed reference over every position AND spelling:
		// inlineTreeDefs walks fields, array items, map values, and union branches
		// through distinct arms, so the forward-shadow splice must fire in each;
		// the wrapped spelling must reach the splice as a whole at the shadow
		// corner too (the bare ref binds the cached null-ns T because the local
		// x.T is not yet in scope).
		for _, spelling := range spellings {
			for _, pos := range []string{"field", "union", "array", "map"} {
				cases = append(cases, fwd{
					name:  "shadow_nullref_before_nsdef/" + spelling + "/" + pos,
					deps:  []string{depT},
					root:  mpJSON(mpRecObj(R, mpField("f1", mpPosWrap(pos, mpRefSpell(spelling, T))), mpField("f2", mpFwdObj(xT, "record", 2)))),
					twin:  mpJSON(mpRecObj(R, mpField("f1", mpPosWrap(pos, mpFwdObj(T, "record", 1))), mpField("f2", mpFwdObj(xT, "record", 2)))),
					value: map[string]any{"f1": posVal(pos, mpFwdVal("record", 1)), "f2": mpFwdVal("record", 2)},
					// f1's spliced null-ns T nests in the namespaced x.R: its PCF form
					// is the documented lossy-but-fingerprint-faithful kind (NOT_BUGS #25).
					canonNoReparse: true,
				})
			}
		}
		// Reverse: the local x.T is defined FIRST, so the later bare "T" binds the
		// LOCAL x.T (in scope), and the cached null-ns T is never referenced — no
		// splice, self-contained as written.
		cases = append(cases, fwd{
			name:  "shadow_nsdef_before_nullref",
			deps:  []string{depT},
			root:  mpJSON(mpRecObj(R, mpField("f1", mpFwdObj(xT, "record", 2)), mpField("f2", T))),
			twin:  mpJSON(mpRecObj(R, mpField("f1", mpFwdObj(xT, "record", 2)), mpField("f2", T))),
			value: map[string]any{"f1": mpFwdVal("record", 2), "f2": mpFwdVal("record", 2)},
		})
	}

	// Pure within-parse forward reference crossed with spelling: a parse that
	// references a cached type C (cross-parse, so the self-containment rebuild
	// runs) AND references a LOCAL type Q before Q's own definition. The forward
	// reference must NOT splice (Q is local, not inherited) — it stays a bare
	// reference to the later definition. The wrapped spelling exercises the
	// splice's no-splice fall-through directly: {"type":"Q"} must collapse to the
	// bare "Q" the canonical / inline twin carries, not survive the rebuild as a
	// wrapped object. The twin is written in the SAME reference-before-definition
	// order (the cache preserves source order; it does not reorder to def-first),
	// so the four forms compare field-for-field.
	for _, spelling := range spellings {
		for _, k := range kinds {
			C, Q, R := "x.C", "x.Q", "x.R"
			depC := mpJSON(mpFwdObj(C, k, 1))
			cases = append(cases, fwd{
				name: "local_forwardref/" + spelling + "/" + k,
				deps: []string{depC},
				root: mpJSON(mpRecObj(R,
					mpField("f0", mpRefSpell(spelling, C)), // cross-parse ref → splices, triggers rebuild
					mpField("f1", mpRefSpell(spelling, Q)), // forward ref to local Q (defined at f2)
					mpField("f2", mpFwdObj(Q, k, 1)))),     // Q defined here
				twin: mpJSON(mpRecObj(R,
					mpField("f0", mpFwdObj(C, k, 1)),
					mpField("f1", Q), // canonical bare reference
					mpField("f2", mpFwdObj(Q, k, 1)))),
				value: map[string]any{"f0": mpFwdVal(k, 1), "f1": mpFwdVal(k, 1), "f2": mpFwdVal(k, 1)},
			})
		}
	}

	// Shadow corner, INEXPRESSIBLE: a null-namespace D is carried (self-contained)
	// by two namespaced records F and G, while an enclosing-namespace x.D shadows
	// its short name. When the splice inlines x.D, then F (with null-ns D), then G
	// (with null-ns D again), the duplicate null-ns D inside G's subtree has NO
	// reference spelling — a bare "D" would re-bind to x.D at that position
	// (scopedRefKeys binds enclosing-first), and Avro has no absolute-null
	// reference. dupDefRef therefore declines, so NO re-parseable inline twin
	// exists; the four-form check is skipped with that provable reason and the
	// binding-safe check is asserted instead (matches
	// TestRegression_SchemaCacheShortNameShadowNoMisbind, generated here).
	{
		D, xF, xG, xD, A := "D", "x.F", "x.G", "x.D", "x.A"
		cases = append(cases, fwd{
			name: "shadow_inexpressible_corner",
			deps: []string{
				mpJSON(mpFwdObj(D, "record", 1)),      // null-ns D (1 field)
				mpJSON(mpRecObj(xF, mpField("d", D))), // x.F.d → bare "D" (null-ns, x.D not yet defined)
				mpJSON(mpRecObj(xG, mpField("d", D))), // x.G.d → bare "D"
				mpJSON(mpFwdObj(xD, "record", 2)),     // shadowing x.D (2 fields)
			},
			root: mpJSON(mpRecObj(A, mpField("p", xD), mpField("f", xF), mpField("g", xG))),
			twin: "",
			value: map[string]any{
				"p": mpFwdVal("record", 2),
				"f": map[string]any{"d": mpFwdVal("record", 1)},
				"g": map[string]any{"d": mpFwdVal("record", 1)},
			},
		})
	}

	for _, fc := range cases {
		t.Run("forward/"+fc.name, func(t *testing.T) {
			cells++
			s := mpRunCache(t, fc.deps, fc.root)
			if fc.twin != "" {
				inline, err := avro.Parse(fc.twin)
				if err != nil {
					t.Fatalf("twin parse %q: %v", fc.twin, err)
				}
				mpAssertSelfContained(t, s, inline, fc.value, fc.root, fc.twin, !fc.canonNoReparse)
				return
			}
			// Inexpressible corner: assert binding-safety. The node-tree wire
			// codec must work, and if String() happens to re-parse it must
			// describe the SAME schema (identical wire) — never the shadowed type.
			wire, err := s.Encode(fc.value)
			if err != nil {
				t.Fatalf("wire encode (node tree must work): %v", err)
			}
			var dec any
			if _, err := s.Decode(wire, &dec); err != nil {
				t.Fatalf("wire decode: %v", err)
			}
			if reparsed, err := avro.Parse(s.String()); err == nil {
				w2, err := reparsed.Encode(fc.value)
				if err != nil {
					t.Errorf("String() re-parses but rejects a wire-valid value (mis-bound shadow ref): %v", err)
				} else if !bytes.Equal(w2, wire) {
					t.Errorf("String() re-parses but yields different wire (mis-bound shadow ref)")
				}
			}
		})
	}

	// --- inherited-namespace family: the cross-parse-referenced leaf DERIVES its
	// namespace from an enclosing wrapper in its defining parse (no explicit
	// "namespace"), then is referenced from a DIFFERENT namespace (mixed regime:
	// leaf in "b", carriers/root in "a"). The stored definition must keep its
	// resolved fullname (b.D) when spliced into the "a" scope, not re-inherit "a"
	// (de3dca3). Crossed with topology so the inherited def is reached directly,
	// through a chain, and through both diamond arms — coverage the single-
	// reference TestMatrix_CacheSelfContainedNamespaces does not have. Crossed
	// with spelling so a WRAPPED reference to an inherited-namespace cached leaf
	// splices the explicit-fullname def too. ---
	for _, spelling := range spellings {
		for _, topo := range []string{"single", "chain2", "diamond"} {
			for _, kind := range kinds {
				for _, pos := range []string{"field", "union"} {
					g, parseOrder, ok := mpBuildTopo(topo, "mixed", kind, pos)
					if !ok {
						continue
					}
					name := fmt.Sprintf("inherited_ns/%s/%s/%s/%s", spelling, topo, pos, kind)
					t.Run(name, func(t *testing.T) {
						cells++
						deps := make([]string, len(parseOrder))
						for i, fn := range parseOrder {
							deps[i] = mpEmitStandalone(fn, g, spelling)
						}
						// Register the leaf (parseOrder[0]) via an inherited-namespace
						// wrapper rather than a standalone explicit-namespace def.
						deps[0] = mpEmitInheritedWrapper(parseOrder[0], kind)
						rootSchema := mpEmitStandalone(g.root, g, spelling)
						viaCache := mpRunCache(t, deps, rootSchema)
						twinSchema := mpEmitTwin(g)
						inline, err := avro.Parse(twinSchema)
						if err != nil {
							t.Fatalf("inline twin parse %q: %v", twinSchema, err)
						}
						mpAssertSelfContained(t, viaCache, inline, mpSampleValue(g.root, g), rootSchema, twinSchema, true)
					})
				}
			}
		}
	}

	t.Logf("multi-parse self-containment net: %d cells", cells)
}

// Non-vacuity (neuter cache.go's inlineTreeDefs, observe the failures):
//
//   - Collapse the wrapper only when the wrapped reference SPLICES (restore the
//     `if _, stayedBare := spliced.(string); !stayedBare { return spliced }`
//     guard so a non-splicing wrapped reference falls through unchanged): the 76
//     repeat2 / repeat_chain / local_forwardref wrapped cells fail with "String
//     diverges" — the later/forward wrapped {"type":"X"} survives where the twin
//     carries bare "X". Canonical/Fingerprint/Root still match (PCF emits bare),
//     so String is the only surface — exactly the layer the single-reference
//     topologies cannot reach.
//
//   - Remove the whole wrapped-reference detection (so a wrapped reference hits
//     the general map path): all 356 wrapped cells across every topology fail
//     with Canonical/Fingerprint/Root/String diverging AND "not self-contained"
//     — the splice produced the invalid {"type":{X-def}} and the metadata fell
//     back to a dangling reference. The bare cells stay green throughout, proving
//     the spelling axis (not some shared regression) is what catches the bug.
