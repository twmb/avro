package avro

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// TestMatrix_SchemaForReservedKeyCaseFold pins the contract that SchemaFor's
// composition walkers (resolveNameScope, pinCustomSchemaScope,
// dedupNamedTypes, normalizeSchemaScope) read reserved attribute keys the
// way the Parse they feed does: case-insensitively, exact-case winning
// (lookupCI). A Props key differing from a reserved name only by ASCII case
// IS that reserved attribute (see Schema.Root's doc), so the walkers must
// key definitions, descend containers, and skip the namespace-pin injection
// identically for every spelling of a key.
//
// Axes: reserved key {namespace — consumed by resolveNameScope's keying and
// the pin's injection-skip; items / values / a union slice under items —
// the descent routes of pinCustomSchemaScope and dedupNamedTypes; fields —
// dedupNamedTypes' and normalizeSchemaScope's field descent} × spelling
// {exact-case control, UPPER, mIxEd} × occurrences {1, 2} × SchemaFor scope
// {default, WithNamespace}.
//
// Oracles per cell family:
//   - namespace: the declared identity x.y.F holds for every spelling
//     (canonical-visible), one definition + a dotted reference at two
//     occurrences, and all three spellings' canonicals are byte-identical.
//     Exact-case always wins in lookupCI, so the exact-case control cells —
//     and with them every natural-spelling cell of the scope matrix — are
//     unaffected by folding.
//   - items/values/union-slice/fields: spelling parity — every spelling
//     produces the same verdict, the same canonical bytes, and (success
//     cells) the same number of inline definition bodies in the full
//     String() output (strays are canonical-stripped, so body counts are
//     what distinguish a walked stray from an ignored one).
//
// The name and type keys have no divergence-reachable variant cell: the
// render always emits them exact-case (toJSONWalk builds "type" and "name"
// literally), so a case-variant can only ride along as an extra key that
// exact-first lookup ignores. Those are pinned as inert-control cells.
func TestMatrix_SchemaForReservedKeyCaseFold(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	spellings := map[string]func(string) string{
		"exact": func(k string) string { return k },
		"upper": strings.ToUpper,
		"mixed": func(k string) string {
			// First letter upper, rest as-is: "Namespace", "Items", ...
			return strings.ToUpper(k[:1]) + k[1:]
		},
	}

	// namespace × occurrences × scope: the identity axis.
	for _, occ := range []int{1, 2} {
		for _, ns := range []string{"", "b"} {
			canonicals := map[string]string{}
			for spell, f := range spellings {
				t.Run(fmt.Sprintf("namespace/%s/occ%d/ns=%q", spell, occ, ns), func(t *testing.T) {
					node := &SchemaNode{Type: "fixed", Name: "F", Size: 4,
						Props: map[string]any{f("namespace"): "x.y"}}
					s, err := schemaForScopeCell(t, scopeCellFields(occ, primary), ns, []CustomType{{GoType: primary, Schema: node}})
					if err != nil {
						t.Fatalf("cell errored: %v", err)
					}
					assertScopeFullnames(t, s, []string{topName(ns), "x.y.F"})
					if !strings.Contains(string(s.Canonical()), `"x.y.F"`) {
						t.Errorf("declared identity x.y.F missing from canonical: %s", s.Canonical())
					}
					if occ == 2 {
						if n := strings.Count(s.String(), `"size"`); n != 1 {
							t.Errorf("want one inline definition + a reference at two occurrences, found %d bodies: %s", n, s.String())
						}
					}
					canonicals[spell] = string(s.Canonical())
				})
			}
			assertOneCanonical(t, fmt.Sprintf("namespace/occ%d/ns=%q", occ, ns), canonicals)
		}
	}

	// Stray-carried descent routes: items, values, union slice, fields.
	// The carrier is an unnamed node whose Props hold a named definition
	// under a (possibly case-variant) container key; the walkers must
	// descend every spelling identically. Parse keeps the stray attribute
	// inert (structural-key posture), so the observable is spelling parity:
	// same verdict, same canonical, same inline-body count.
	routes := []struct {
		route string
		key   string // reserved key the carried value sits under
		build func(spelledKey string) *SchemaNode
	}{
		{"items", "items", func(k string) *SchemaNode {
			return &SchemaNode{Type: "string", Props: map[string]any{
				k: map[string]any{"type": "fixed", "name": "G", "size": 1}}}
		}},
		{"values", "values", func(k string) *SchemaNode {
			return &SchemaNode{Type: "string", Props: map[string]any{
				k: map[string]any{"type": "fixed", "name": "G", "size": 1}}}
		}},
		{"unionslice", "items", func(k string) *SchemaNode {
			return &SchemaNode{Type: "string", Props: map[string]any{
				k: []any{map[string]any{"type": "fixed", "name": "G", "size": 1}}}}
		}},
		{"fields", "items", func(k string) *SchemaNode {
			// A record definition carried under exact-case items, with its
			// FIELDS key case-varied: the fields-descent axis.
			return &SchemaNode{Type: "string", Props: map[string]any{
				"items": map[string]any{"type": "record", "name": "R", "namespace": "x.y",
					k: []map[string]any{{"name": "f", "type": "int"}}}}}
		}},
	}
	for _, r := range routes {
		for _, occ := range []int{1, 2} {
			for _, ns := range []string{"", "b"} {
				verdicts := map[string]string{}
				canonicals := map[string]string{}
				bodies := map[string]int{}
				for spell, f := range spellings {
					spelledKey := f(r.key)
					if r.route == "fields" {
						spelledKey = f("fields")
					}
					t.Run(fmt.Sprintf("%s/%s/occ%d/ns=%q", r.route, spell, occ, ns), func(t *testing.T) {
						node := r.build(spelledKey)
						s, err := schemaForScopeCell(t, scopeCellFields(occ, primary), ns, []CustomType{{GoType: primary, Schema: node}})
						if err != nil {
							verdicts[spell] = err.Error()
							return
						}
						verdicts[spell] = "ok"
						canonicals[spell] = string(s.Canonical())
						// Inline-body marker, spelling-neutral: the carried
						// fixed G always emits "size"; the carried record R
						// always emits its field "f" (the container KEY
						// spelling varies by cell, the body content never
						// does).
						marker := `"size"`
						if r.route == "fields" {
							marker = `"name":"f"`
						}
						bodies[spell] = strings.Count(s.String(), marker)
					})
				}
				name := fmt.Sprintf("%s/occ%d/ns=%q", r.route, occ, ns)
				assertOneValue(t, name+" verdict", verdicts)
				if verdicts["exact"] == "ok" {
					assertOneCanonical(t, name, canonicals)
					assertOneIntValue(t, name+" inline bodies", bodies)
				}
			}
		}
	}

	// Inert controls: the render always emits exact-case "name" and "type",
	// so a case-variant of either can only be an extra key that exact-first
	// lookup ignores — on the walkers AND on Parse. The composed output
	// must equal the variant-free control's canonical.
	for _, extra := range []string{"NAME", "TYPE"} {
		t.Run("inertcontrol/"+extra, func(t *testing.T) {
			control := &SchemaNode{Type: "fixed", Name: "F", Namespace: "x.y", Size: 4}
			varied := &SchemaNode{Type: "fixed", Name: "F", Namespace: "x.y", Size: 4,
				Props: map[string]any{extra: "Zed"}}
			sControl, err := schemaForScopeCell(t, scopeCellFields(2, primary), "b", []CustomType{{GoType: primary, Schema: control}})
			if err != nil {
				t.Fatalf("control: %v", err)
			}
			sVaried, err := schemaForScopeCell(t, scopeCellFields(2, primary), "b", []CustomType{{GoType: primary, Schema: varied}})
			if err != nil {
				t.Fatalf("varied: %v", err)
			}
			if string(sControl.Canonical()) != string(sVaried.Canonical()) {
				t.Errorf("case-variant %s prop is not inert:\n control: %s\n varied:  %s", extra, sControl.Canonical(), sVaried.Canonical())
			}
		})
	}
}

// findNodeAliases returns the Aliases slice of the named-type definition
// called name anywhere in the tree, or nil if no definition carries it
// (references are bare type-name nodes with no Name, so only the
// definition matches).
func findNodeAliases(n SchemaNode, name string) []string {
	if n.Name == name {
		return n.Aliases
	}
	if n.Items != nil {
		if a := findNodeAliases(*n.Items, name); a != nil {
			return a
		}
	}
	if n.Values != nil {
		if a := findNodeAliases(*n.Values, name); a != nil {
			return a
		}
	}
	for i := range n.Branches {
		if a := findNodeAliases(n.Branches[i], name); a != nil {
			return a
		}
	}
	for i := range n.Fields {
		if a := findNodeAliases(n.Fields[i].Type, name); a != nil {
			return a
		}
	}
	return nil
}

// typeAliasCaseFoldDefX is the named record definition the binding-key
// cells park behind a (possibly case-variant) container key.
func typeAliasCaseFoldDefX() map[string]any {
	return map[string]any{"type": "record", "name": "X",
		"fields": []any{map[string]any{"name": "c", "type": "long"}}}
}

// TestRegression_TypeAliasBindingKeyCaseFold pins that the type-alias walk
// reads a container's binding key (items here) the way Parse does:
// case-insensitively (see Schema.Root's doc and the composition walkers'
// shared lookupCI posture). The custom array's items exist only as a
// Props-carried key, which Parse binds as the real items either way the
// key is spelled — so the walk must find the named item type X and apply
// the alias for every spelling, not just the exact-case one.
func TestRegression_TypeAliasBindingKeyCaseFold(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	fields := []reflect.StructField{{Name: "L", Type: primary, Tag: `avro:"l,type-alias=Old"`}}
	for _, spell := range []string{"items", "Items", "ITEMS"} {
		t.Run(spell, func(t *testing.T) {
			node := &SchemaNode{Type: "array", Props: map[string]any{spell: typeAliasCaseFoldDefX()}}
			s, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: node}})
			if err != nil {
				t.Fatalf("build errored for spelling %q (exact-case control succeeds): %v", spell, err)
			}
			if got := findNodeAliases(s.Root(), "X"); !reflect.DeepEqual(got, []string{"Old"}) {
				t.Errorf("spelling %q: alias not applied to X: got %#v, want [Old]", spell, got)
			}
		})
	}
}

// TestRegression_TypeAliasUnionPlacementCaseFold pins WHERE the alias
// lands in a union: on the first named type in walk order — here record X
// behind the array branch's items — for every spelling of the binding
// key. A case-variant spelling must not silently reroute the alias to a
// later named branch (Y): Parse binds the variant key as the real items,
// so X is the first named type either way.
func TestRegression_TypeAliasUnionPlacementCaseFold(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	fields := []reflect.StructField{{Name: "U", Type: primary, Tag: `avro:"u,type-alias=Old"`}}
	for _, spell := range []string{"items", "Items"} {
		t.Run(spell, func(t *testing.T) {
			node := &SchemaNode{Type: "union", Branches: []SchemaNode{
				{Type: "array", Props: map[string]any{spell: typeAliasCaseFoldDefX()}},
				{Type: "record", Name: "Y", Fields: []SchemaField{{Name: "n", Type: SchemaNode{Type: "int"}}}},
			}}
			s, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: node}})
			if err != nil {
				t.Fatalf("build errored for spelling %q: %v", spell, err)
			}
			root := s.Root()
			if got := findNodeAliases(root, "X"); !reflect.DeepEqual(got, []string{"Old"}) {
				t.Errorf("spelling %q: alias not on first named type X: got %#v, want [Old]", spell, got)
			}
			if got := findNodeAliases(root, "Y"); got != nil {
				t.Errorf("spelling %q: alias misdirected to later branch Y: %#v", spell, got)
			}
		})
	}
}

// TestRegression_TypeAliasExtendsCaseVariantAliases pins that the
// type-alias tag EXTENDS an existing aliases attribute rather than
// shadowing it, for every route the attribute can arrive by: the
// SchemaNode.Aliases field (rendered as exact-case "aliases") and a
// Props-carried case-variant key ("Aliases"), which Parse folds onto the
// same attribute. An exact-case write alongside the variant would leave
// two spellings in the composed object and silently drop the caller's
// aliases at Parse (duplicate-key resolution keeps only one).
func TestRegression_TypeAliasExtendsCaseVariantAliases(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	fields := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f,type-alias=Old"`}}

	control := &SchemaNode{Type: "fixed", Name: "F", Size: 4, Aliases: []string{"prior.P"}}
	sc, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: control}})
	if err != nil {
		t.Fatalf("control build: %v", err)
	}
	want := findNodeAliases(sc.Root(), "F")
	if len(want) != 2 {
		t.Fatalf("control aliases: got %#v, want both prior.P and Old", want)
	}

	variant := &SchemaNode{Type: "fixed", Name: "F", Size: 4,
		Props: map[string]any{"Aliases": []any{"prior.P"}}}
	sv, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: variant}})
	if err != nil {
		t.Fatalf("variant build: %v", err)
	}
	if got := findNodeAliases(sv.Root(), "F"); !reflect.DeepEqual(got, want) {
		t.Errorf("case-variant aliases route diverges from the field route:\n got:  %#v\n want: %#v", got, want)
	}
}

// TestMatrix_TypeAliasCaseFold extends the reserved-key case-fold contract
// (TestMatrix_SchemaForReservedKeyCaseFold) with the type-alias axis: the
// type-alias tag's walk must route through a container's binding key and
// read/extend the aliases attribute exactly as Parse binds them —
// case-insensitively — so CI-equivalent inputs produce canonical-identical
// schemas with the alias applied to the SAME type.
//
// Axes:
//   - binding-key routing: carrier {array, map, union whose first named
//     type sits behind the carrier's binding key} × spelling {exact,
//     upper, mixed} × structural-field {nil — the spelled Props key is the
//     only spelling and Parse binds it; set — the real field renders
//     exact-case and the spelled Props key rides along with an identical
//     body}. Oracle per carrier family: every cell agrees on verdict,
//     canonical bytes, the alias landing on X, and (union) no alias on the
//     later branch Y.
//   - named-carrier attribute routes: the aliases attribute arriving via
//     the SchemaNode.Aliases field vs Props under each spelling (the tag
//     must EXTEND all routes identically), and name/namespace case-variant
//     Props riding beside the real attributes (inert for the walk: exact
//     spelling wins in both the walk and Parse).
//   - two tagged fields sharing the custom type: the composed output must
//     be one definition (carrying the alias) plus one reference,
//     identically for the namespace-field route and the Props-variant
//     route (refName is per-build bookkeeping; the CI reads keep it
//     consistent across occurrences).
func TestMatrix_TypeAliasCaseFold(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	tagged := []reflect.StructField{{Name: "L", Type: primary, Tag: `avro:"l,type-alias=Old"`}}
	spellings := map[string]func(string) string{
		"exact": func(k string) string { return k },
		"upper": strings.ToUpper,
		"mixed": func(k string) string { return strings.ToUpper(k[:1]) + k[1:] },
	}
	itemsX := func() *SchemaNode {
		return &SchemaNode{Type: "record", Name: "X",
			Fields: []SchemaField{{Name: "c", Type: SchemaNode{Type: "long"}}}}
	}

	// Binding-key routing: array / map / union carriers.
	type carrierShape struct {
		name  string
		key   string // binding key the carrier's kind consumes
		build func(spelledKey string, structuralSet bool) *SchemaNode
	}
	carriers := []carrierShape{
		{"array", "items", func(k string, set bool) *SchemaNode {
			n := &SchemaNode{Type: "array", Props: map[string]any{k: typeAliasCaseFoldDefX()}}
			if set {
				n.Items = itemsX()
			}
			return n
		}},
		{"map", "values", func(k string, set bool) *SchemaNode {
			n := &SchemaNode{Type: "map", Props: map[string]any{k: typeAliasCaseFoldDefX()}}
			if set {
				n.Values = itemsX()
			}
			return n
		}},
		{"union", "items", func(k string, set bool) *SchemaNode {
			arr := SchemaNode{Type: "array", Props: map[string]any{k: typeAliasCaseFoldDefX()}}
			if set {
				arr.Items = itemsX()
			}
			return &SchemaNode{Type: "union", Branches: []SchemaNode{arr,
				{Type: "record", Name: "Y", Fields: []SchemaField{{Name: "n", Type: SchemaNode{Type: "int"}}}}}}
		}},
	}
	for _, c := range carriers {
		verdicts := map[string]string{}
		canonicals := map[string]string{}
		placements := map[string]string{}
		for spell, f := range spellings {
			for _, set := range []bool{false, true} {
				cell := fmt.Sprintf("%s/%v", spell, set)
				t.Run(fmt.Sprintf("route/%s/%s/structural=%v", c.name, spell, set), func(t *testing.T) {
					node := c.build(f(c.key), set)
					s, err := schemaForScopeCell(t, tagged, "", []CustomType{{GoType: primary, Schema: node}})
					if err != nil {
						verdicts[cell] = err.Error()
						return
					}
					verdicts[cell] = "ok"
					canonicals[cell] = string(s.Canonical())
					root := s.Root()
					placements[cell] = fmt.Sprintf("X=%v Y=%v",
						findNodeAliases(root, "X"), findNodeAliases(root, "Y"))
					if got := findNodeAliases(root, "X"); !reflect.DeepEqual(got, []string{"Old"}) {
						t.Errorf("alias not on X: %#v", got)
					}
				})
			}
		}
		assertOneValue(t, "route/"+c.name+" verdict", verdicts)
		assertOneValue(t, "route/"+c.name+" canonical", canonicals)
		assertOneValue(t, "route/"+c.name+" placement", placements)
	}

	// Aliases-attribute routes: the tag extends every arrival route of the
	// existing aliases identically.
	{
		canonicals := map[string]string{}
		aliasSets := map[string]string{}
		build := func(name string, node *SchemaNode) {
			t.Run("aliases/"+name, func(t *testing.T) {
				s, err := schemaForScopeCell(t, tagged, "", []CustomType{{GoType: primary, Schema: node}})
				if err != nil {
					t.Fatalf("build: %v", err)
				}
				canonicals[name] = string(s.Canonical())
				got := findNodeAliases(s.Root(), "F")
				aliasSets[name] = fmt.Sprintf("%v", got)
				if len(got) != 2 {
					t.Errorf("want caller alias + tag alias, got %#v", got)
				}
			})
		}
		build("field", &SchemaNode{Type: "fixed", Name: "F", Size: 4, Aliases: []string{"prior.P"}})
		for spell, f := range spellings {
			build("props-"+spell, &SchemaNode{Type: "fixed", Name: "F", Size: 4,
				Props: map[string]any{f("aliases"): []any{"prior.P"}}})
		}
		assertOneValue(t, "aliases canonical", canonicals)
		assertOneValue(t, "aliases set", aliasSets)
	}

	// name / namespace case-variant Props riding beside the real
	// attributes: inert for the walk and for Parse (exact spelling wins),
	// so the composed output equals the variant-free control's.
	{
		for _, extra := range []struct{ key, val string }{{"NAME", "Zed"}, {"NAMESPACE", "zed"}} {
			t.Run("inert/"+extra.key, func(t *testing.T) {
				control := &SchemaNode{Type: "fixed", Name: "F", Namespace: "x.y", Size: 4}
				varied := &SchemaNode{Type: "fixed", Name: "F", Namespace: "x.y", Size: 4,
					Props: map[string]any{extra.key: extra.val}}
				sc, err := schemaForScopeCell(t, tagged, "", []CustomType{{GoType: primary, Schema: control}})
				if err != nil {
					t.Fatalf("control: %v", err)
				}
				sv, err := schemaForScopeCell(t, tagged, "", []CustomType{{GoType: primary, Schema: varied}})
				if err != nil {
					t.Fatalf("varied: %v", err)
				}
				if string(sc.Canonical()) != string(sv.Canonical()) {
					t.Errorf("case-variant %s not inert under a type-alias tag:\n control: %s\n varied:  %s",
						extra.key, sc.Canonical(), sv.Canonical())
				}
				if got := findNodeAliases(sv.Root(), "F"); !reflect.DeepEqual(got, []string{"Old"}) {
					t.Errorf("alias not applied: %#v", got)
				}
			})
		}
	}

	// Two tagged fields sharing the custom type: one definition carrying
	// the alias, one reference — identically for the namespace-field route
	// and the Props case-variant route.
	{
		twoTagged := []reflect.StructField{
			{Name: "F1", Type: primary, Tag: `avro:"f1,type-alias=Old"`},
			{Name: "F2", Type: primary, Tag: `avro:"f2,type-alias=Old"`},
		}
		canonicals := map[string]string{}
		shapes := map[string]string{}
		for name, node := range map[string]*SchemaNode{
			"nsfield": {Type: "record", Name: "X", Namespace: "x.y",
				Fields: []SchemaField{{Name: "c", Type: SchemaNode{Type: "long"}}}},
			"nsprops": {Type: "record", Name: "X",
				Fields: []SchemaField{{Name: "c", Type: SchemaNode{Type: "long"}}},
				Props:  map[string]any{"NameSpace": "x.y"}},
		} {
			t.Run("twofields/"+name, func(t *testing.T) {
				s, err := schemaForScopeCell(t, twoTagged, "", []CustomType{{GoType: primary, Schema: node}})
				if err != nil {
					t.Fatalf("build: %v", err)
				}
				canonicals[name] = string(s.Canonical())
				defs := strings.Count(s.String(), `"c"`) // field c appears once per inline body
				refs := strings.Count(s.String(), `"x.y.X"`)
				shapes[name] = fmt.Sprintf("defs=%d refs=%d aliases=%v", defs, refs,
					findNodeAliases(s.Root(), "X"))
				if defs != 1 || refs != 1 {
					t.Errorf("want one definition + one dotted reference, got %d defs %d refs: %s", defs, refs, s.String())
				}
			})
		}
		assertOneValue(t, "twofields canonical", canonicals)
		assertOneValue(t, "twofields shape", shapes)
	}
}

// TestRegression_CaseVariantStructuralKeyParsePosture pins the bare-Parse
// side of the reserved-key case-fold policy (maintainer-adjudicated; see
// NOT_BUGS #46) on the one input class where the major implementations
// give three different answers: a structural key present ONLY as a
// case-variant spelling.
//
//	{"type":"array","Items":"int"}            twmb+hamba: array of int (bound)
//	                                          fastavro 1.12.2: KeyError reject (executed)
//	                                          goavro: "Array ought to have items key" (array.go:23)
//	{"type":"record","name":"R","Fields":[…]} twmb+hamba: record WITH the fields (bound; hamba executed)
//	                                          fastavro 1.12.2: ZERO-field record, "Fields" kept as a prop (executed)
//	                                          Java: reject — "fields" missing (SCHEMA_RESERVED is exact-case,
//	                                          Schema.java:175-176, so "Fields" is a custom prop)
//
// twmb binds — hamba-compatible by ruling — and these cells assert the
// EXACT posture (canonical bytes + wire round-trip) so any future change
// to the fold is a visible policy flip, not a silent drift.
func TestRegression_CaseVariantStructuralKeyParsePosture(t *testing.T) {
	arr, err := Parse(`{"type":"array","Items":"int"}`)
	if err != nil {
		t.Fatalf("Items-only array: %v", err)
	}
	if got, want := string(arr.Canonical()), `{"type":"array","items":"int"}`; got != want {
		t.Errorf("array canonical: got %s want %s", got, want)
	}
	wire, err := arr.Encode([]int32{7})
	if err != nil {
		t.Fatalf("array encode: %v", err)
	}
	var back []int32
	if _, err := arr.Decode(wire, &back); err != nil || len(back) != 1 || back[0] != 7 {
		t.Errorf("array round-trip: %v %v", back, err)
	}

	rec, err := Parse(`{"type":"record","name":"R","Fields":[{"name":"f","type":"int"}]}`)
	if err != nil {
		t.Fatalf("Fields-only record: %v", err)
	}
	if got, want := string(rec.Canonical()), `{"name":"R","type":"record","fields":[{"name":"f","type":"int"}]}`; got != want {
		t.Errorf("record canonical: got %s want %s", got, want)
	}
	wire, err = rec.Encode(map[string]any{"f": int32(3)})
	if err != nil {
		t.Fatalf("record encode: %v", err)
	}
	var m map[string]any
	if _, err := rec.Decode(wire, &m); err != nil || m["f"] != int32(3) {
		t.Errorf("record round-trip: %v %v", m, err)
	}
	if n := len(rec.Root().Fields); n != 1 {
		t.Errorf("record field count: got %d want 1 (the case-variant key IS the fields attribute)", n)
	}
}

func topName(ns string) string {
	if ns == "" {
		return "Top"
	}
	return ns + ".Top"
}

func assertOneCanonical(t *testing.T, name string, got map[string]string) {
	t.Helper()
	assertOneValue(t, name+" canonical", got)
}

func assertOneValue(t *testing.T, name string, got map[string]string) {
	t.Helper()
	var first string
	var firstKey string
	for k, v := range got {
		if firstKey == "" {
			firstKey, first = k, v
			continue
		}
		if v != first {
			t.Errorf("%s diverges across spellings:\n %s: %s\n %s: %s", name, firstKey, first, k, v)
		}
	}
}

func assertOneIntValue(t *testing.T, name string, got map[string]int) {
	t.Helper()
	asStr := make(map[string]string, len(got))
	for k, v := range got {
		asStr[k] = fmt.Sprint(v)
	}
	assertOneValue(t, name, asStr)
}
