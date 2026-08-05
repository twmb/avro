package avro

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// TestMatrix_SchemaForReservedKeyExactCase pins the contract that SchemaFor's
// composition walkers (resolveNameScope, pinCustomSchemaScope,
// dedupNamedTypes, normalizeSchemaScope) read reserved attribute keys the
// way the Parse they feed does: by exact lowercase name only. A Props key
// differing from a reserved name only by letter case is an ordinary custom
// property (see Schema.Root's doc) — the walkers must neither key, descend,
// nor inject through it, and it must survive composition verbatim.
//
// Axes: reserved key {namespace — the identity axis: only the exact
// spelling scopes the type; items / values / a union slice under items —
// the descent routes; fields — the field-descent axis} × spelling
// {exact-case, UPPER, mIxed} × occurrences {1, 2} × SchemaFor scope
// {default, WithNamespace}.
//
// Oracles per cell family:
//   - namespace: the EXACT spelling declares identity x.y.F
//     (canonical-visible, one definition + a dotted reference at two
//     occurrences). A VARIANT spelling declares nothing: the identity is
//     the null-namespace F for every variant cell — byte-identical to the
//     no-namespace control — and the variant key rides to the composed
//     definition's Props verbatim. The exact and variant identities MUST
//     diverge; asserting that divergence is what makes a reintroduced
//     case-fold visible.
//   - items/values/union-slice/fields: an exact-spelled stray keeps the
//     structural-key inertness posture (composition passes it through
//     untouched), and a variant spelling is a plain prop — both compose
//     verbatim with identical verdicts, canonicals, and inline-body
//     counts, because NO spelling of a key on a kind that does not bind it
//     may be walked, registered, or deduped.
func TestMatrix_SchemaForReservedKeyExactCase(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	variants := map[string]func(string) string{
		"upper": strings.ToUpper,
		"mixed": func(k string) string {
			// First letter upper, rest as-is: "Namespace", "Items", ...
			return strings.ToUpper(k[:1]) + k[1:]
		},
	}

	// namespace × occurrences × scope: the identity axis.
	for _, occ := range []int{1, 2} {
		for _, ns := range []string{"", "b"} {
			// Exact spelling: the namespace attribute, identity x.y.F.
			t.Run(fmt.Sprintf("namespace/exact/occ%d/ns=%q", occ, ns), func(t *testing.T) {
				node := &SchemaNode{Type: "fixed", Name: "F", Size: 4,
					Props: map[string]any{"namespace": "x.y"}}
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
			})
			// Variant spellings: inert props; the identity is the
			// null-namespace F, verdict- and byte-identical to the
			// no-namespace control — including the control's documented
			// reject when a null-namespace type recurs under
			// WithNamespace (no reference spelling can denote it) — with
			// the variant preserved on the definition in success cells.
			control := &SchemaNode{Type: "fixed", Name: "F", Size: 4}
			sControl, controlErr := schemaForScopeCell(t, scopeCellFields(occ, primary), ns, []CustomType{{GoType: primary, Schema: control}})
			for spell, f := range variants {
				t.Run(fmt.Sprintf("namespace/%s/occ%d/ns=%q", spell, occ, ns), func(t *testing.T) {
					key := f("namespace")
					node := &SchemaNode{Type: "fixed", Name: "F", Size: 4,
						Props: map[string]any{key: "x.y"}}
					s, err := schemaForScopeCell(t, scopeCellFields(occ, primary), ns, []CustomType{{GoType: primary, Schema: node}})
					if controlErr != nil {
						if err == nil || err.Error() != controlErr.Error() {
							t.Fatalf("variant %q verdict diverges from the no-namespace control:\n control: %v\n varied:  %v", key, controlErr, err)
						}
						return
					}
					if err != nil {
						t.Fatalf("cell errored where the control built: %v", err)
					}
					assertScopeFullnames(t, s, []string{topName(ns), "F"})
					if got, want := string(s.Canonical()), string(sControl.Canonical()); got != want {
						t.Errorf("variant %q canonical diverges from the no-namespace control (the variant must be inert):\n control: %s\n varied:  %s", key, want, got)
					}
					def := findNodeByTypeName(*s.Root(), "fixed", "F")
					if def == nil {
						t.Fatalf("definition F not found")
					}
					if got := def.Props[key]; !reflect.DeepEqual(got, "x.y") {
						t.Errorf("Props[%q] = %#v; want the variant preserved verbatim", key, got)
					}
				})
			}
		}
	}

	// Stray-carried routes: items, values, union slice, fields. The
	// carrier is an unnamed node whose Props hold a named definition
	// under a container key the carrier's kind does not bind. NO spelling
	// may be walked as a schema position (exact: the stray-key inertness
	// posture; variant: not a reserved key at all), so every spelling
	// composes verbatim: same verdict, same canonical, same inline-body
	// count.
	spellings := map[string]func(string) string{
		"exact": func(k string) string { return k },
		"upper": strings.ToUpper,
		"mixed": func(k string) string { return strings.ToUpper(k[:1]) + k[1:] },
	}
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
		{"fields", "fields", func(k string) *SchemaNode {
			// A record body carried under an exact-case stray items, with
			// its FIELDS key case-varied: only the exact spelling makes
			// the body a well-formed record, but the body sits at a stray
			// position either way, so every spelling stays inert.
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
	// so a case-variant of either is an extra custom property that neither
	// the walkers nor Parse bind. The composed output must equal the
	// variant-free control's canonical.
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
	if f := findNodeByTypeName(n, "", name); f != nil {
		return f.Aliases
	}
	return nil
}

// findNodeByTypeName walks a SchemaNode tree for the first node with the
// given Name (and Type, when non-empty).
func findNodeByTypeName(n SchemaNode, typ, name string) *SchemaNode {
	if n.Name == name && (typ == "" || n.Type == typ) {
		return &n
	}
	if n.Items != nil {
		if f := findNodeByTypeName(*n.Items, typ, name); f != nil {
			return f
		}
	}
	if n.Values != nil {
		if f := findNodeByTypeName(*n.Values, typ, name); f != nil {
			return f
		}
	}
	for i := range n.Branches {
		if f := findNodeByTypeName(n.Branches[i], typ, name); f != nil {
			return f
		}
	}
	for i := range n.Fields {
		if f := findNodeByTypeName(n.Fields[i].Type, typ, name); f != nil {
			return f
		}
	}
	return nil
}

// typeAliasExactCaseDefX is the named record definition the binding-key
// cells park behind a container key.
func typeAliasExactCaseDefX() map[string]any {
	return map[string]any{"type": "record", "name": "X",
		"fields": []any{map[string]any{"name": "c", "type": "long"}}}
}

// TestRegression_TypeAliasBindingKeyExactCase pins that the type-alias walk
// reads a container's binding key (items here) the way Parse does: by
// exact name only. A custom array whose items exist only as an
// exact-spelled Props key binds (the rendered "items" IS the array's
// items, so the walk descends it and the alias lands on X); a case-variant
// spelling is an ordinary prop, so the array has NO items to route the
// alias through and the build fails loudly at the walk's own diagnosis —
// there is no named type behind the tagged field.
func TestRegression_TypeAliasBindingKeyExactCase(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	fields := []reflect.StructField{{Name: "L", Type: primary, Tag: `avro:"l,type-alias=Old"`}}

	t.Run("items", func(t *testing.T) {
		node := &SchemaNode{Type: "array", Props: map[string]any{"items": typeAliasExactCaseDefX()}}
		s, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: node}})
		if err != nil {
			t.Fatalf("exact-case items: %v", err)
		}
		if got := findNodeAliases(*s.Root(), "X"); !reflect.DeepEqual(got, []string{"Old"}) {
			t.Errorf("alias not applied to X: got %#v, want [Old]", got)
		}
	})
	for _, spell := range []string{"Items", "ITEMS"} {
		t.Run(spell, func(t *testing.T) {
			node := &SchemaNode{Type: "array", Props: map[string]any{spell: typeAliasExactCaseDefX()}}
			_, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: node}})
			if err == nil {
				t.Fatalf("spelling %q built; the array has no items so the type-alias has no named type to land on", spell)
			}
			if !strings.Contains(err.Error(), "type is not a named type") {
				t.Errorf("spelling %q error = %q; want the no-named-type diagnosis", spell, err.Error())
			}
		})
	}
}

// TestRegression_TypeAliasUnionPlacementExactCase pins WHERE the alias
// lands in a union under exact-case reads: on the first named type in walk
// order — record X behind the array branch's exact-spelled items. With a
// case-variant spelling the array branch has no items at all and the
// build fails loudly rather than silently rerouting the alias to a later
// named branch.
func TestRegression_TypeAliasUnionPlacementExactCase(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	fields := []reflect.StructField{{Name: "U", Type: primary, Tag: `avro:"u,type-alias=Old"`}}
	build := func(itemsKey string) (*Schema, error) {
		node := &SchemaNode{Type: "union", Branches: []SchemaNode{
			{Type: "array", Props: map[string]any{itemsKey: typeAliasExactCaseDefX()}},
			{Type: "record", Name: "Y", Fields: []SchemaField{{Name: "n", Type: SchemaNode{Type: "int"}}}},
		}}
		return schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: node}})
	}
	s, err := build("items")
	if err != nil {
		t.Fatalf("exact-case items: %v", err)
	}
	root := s.Root()
	if got := findNodeAliases(*root, "X"); !reflect.DeepEqual(got, []string{"Old"}) {
		t.Errorf("alias not on first named type X: got %#v, want [Old]", got)
	}
	if got := findNodeAliases(*root, "Y"); got != nil {
		t.Errorf("alias misdirected to later branch Y: %#v", got)
	}
	if _, err := build("Items"); err == nil || !strings.Contains(err.Error(), "array is missing items schema") {
		t.Errorf("variant Items: got %v; want the missing-items reject", err)
	}
}

// TestRegression_TypeAliasVariantAliasesInert pins that the type-alias tag
// EXTENDS only the real aliases attribute routes — the SchemaNode.Aliases
// field and an exact-spelled Props "aliases" key. A case-variant Props
// spelling ("Aliases") is an ordinary custom property: the tag writes a
// fresh exact-case "aliases" beside it, Parse binds only the exact key,
// and the variant survives verbatim in Props, un-merged.
func TestRegression_TypeAliasVariantAliasesInert(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	fields := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f,type-alias=Old"`}}

	control := &SchemaNode{Type: "fixed", Name: "F", Size: 4, Aliases: []string{"prior.P"}}
	sc, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: control}})
	if err != nil {
		t.Fatalf("control build: %v", err)
	}
	if got := findNodeAliases(*sc.Root(), "F"); len(got) != 2 {
		t.Fatalf("control aliases: got %#v, want both prior.P and Old", got)
	}

	variant := &SchemaNode{Type: "fixed", Name: "F", Size: 4,
		Props: map[string]any{"Aliases": []any{"prior.P"}}}
	sv, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: variant}})
	if err != nil {
		t.Fatalf("variant build: %v", err)
	}
	def := findNodeByTypeName(*sv.Root(), "fixed", "F")
	if def == nil {
		t.Fatalf("definition F not found")
	}
	if !reflect.DeepEqual(def.Aliases, []string{"Old"}) {
		t.Errorf("Aliases = %#v; want [Old] alone (the variant key is not the aliases attribute)", def.Aliases)
	}
	if got := def.Props["Aliases"]; !reflect.DeepEqual(got, []any{"prior.P"}) {
		t.Errorf(`Props["Aliases"] = %#v; want the variant preserved verbatim, un-merged`, got)
	}
}

// TestMatrix_TypeAliasExactCase extends the reserved-key exact-case
// contract (TestMatrix_SchemaForReservedKeyExactCase) with the type-alias
// axis: the type-alias tag's walk routes through a container's binding key
// and reads/extends the aliases attribute exactly as Parse binds them — by
// exact name only.
//
// Axes:
//   - binding-key routing: carrier {array, map, union whose first named
//     type sits behind the carrier's binding key} × spelling {exact,
//     upper, mixed} × structural-field {nil — only the spelled Props key
//     exists; set — the real field renders exact-case and the spelled
//     Props key rides along}. Exact-spelling cells and every
//     structural=set cell build with the alias on X; a variant-only cell
//     (structural=nil, variant spelling) has no binding key and fails its
//     parse loudly. All accepting cells of one carrier agree on canonical
//     bytes (props are canonical-stripped).
//   - aliases-attribute routes: the field route and the exact-Props route
//     are EXTENDED identically; a variant-Props route gets a fresh exact
//     "aliases" with the variant preserved verbatim.
//   - name/namespace case-variant Props riding beside the real attributes:
//     inert (the exact attributes win; the variants ride as props).
//   - two tagged fields sharing the custom type: the namespace-field route
//     composes one x.y.X definition + one dotted reference; a "NameSpace"
//     variant-Props route declares nothing — the type is null-namespace X,
//     one definition + one bare reference, variant preserved.
func TestMatrix_TypeAliasExactCase(t *testing.T) {
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
		name    string
		key     string // binding key the carrier's kind consumes
		missing string // the missing-structural-key reject for the kind
		build   func(spelledKey string, structuralSet bool) *SchemaNode
	}
	// The variant-only reject differs by carrier: with no named type
	// reachable at all (array/map), the type-alias walk itself fails
	// loudly first; the union's later named branch satisfies the walk, so
	// the itemless array branch is caught by the composed schema's parse.
	carriers := []carrierShape{
		{"array", "items", "type is not a named type", func(k string, set bool) *SchemaNode {
			n := &SchemaNode{Type: "array", Props: map[string]any{k: typeAliasExactCaseDefX()}}
			if set {
				n.Items = itemsX()
			}
			return n
		}},
		{"map", "values", "type is not a named type", func(k string, set bool) *SchemaNode {
			n := &SchemaNode{Type: "map", Props: map[string]any{k: typeAliasExactCaseDefX()}}
			if set {
				n.Values = itemsX()
			}
			return n
		}},
		{"union", "items", "array is missing items schema", func(k string, set bool) *SchemaNode {
			arr := SchemaNode{Type: "array", Props: map[string]any{k: typeAliasExactCaseDefX()}}
			if set {
				arr.Items = itemsX()
			}
			return &SchemaNode{Type: "union", Branches: []SchemaNode{arr,
				{Type: "record", Name: "Y", Fields: []SchemaField{{Name: "n", Type: SchemaNode{Type: "int"}}}}}}
		}},
	}
	for _, c := range carriers {
		canonicals := map[string]string{}
		for spell, f := range spellings {
			for _, set := range []bool{false, true} {
				cell := fmt.Sprintf("%s/%v", spell, set)
				t.Run(fmt.Sprintf("route/%s/%s/structural=%v", c.name, spell, set), func(t *testing.T) {
					node := c.build(f(c.key), set)
					s, err := schemaForScopeCell(t, tagged, "", []CustomType{{GoType: primary, Schema: node}})
					if spell != "exact" && !set {
						// The only spelling present is a variant: an
						// ordinary prop, so the container has no binding
						// key and the composed schema fails its parse.
						if err == nil || !strings.Contains(err.Error(), c.missing) {
							t.Errorf("variant-only cell: got %v; want the %q reject", err, c.missing)
						}
						return
					}
					if err != nil {
						t.Fatalf("cell errored: %v", err)
					}
					canonicals[cell] = string(s.Canonical())
					root := s.Root()
					if got := findNodeAliases(*root, "X"); !reflect.DeepEqual(got, []string{"Old"}) {
						t.Errorf("alias not on X: %#v", got)
					}
					if got := findNodeAliases(*root, "Y"); got != nil {
						t.Errorf("alias misdirected to later branch Y: %#v", got)
					}
				})
			}
		}
		assertOneValue(t, "route/"+c.name+" canonical", canonicals)
	}

	// Aliases-attribute routes.
	{
		aliasSets := map[string]string{}
		build := func(name string, node *SchemaNode, wantLen int) {
			t.Run("aliases/"+name, func(t *testing.T) {
				s, err := schemaForScopeCell(t, tagged, "", []CustomType{{GoType: primary, Schema: node}})
				if err != nil {
					t.Fatalf("build: %v", err)
				}
				got := findNodeAliases(*s.Root(), "F")
				aliasSets[name] = fmt.Sprintf("%v", got)
				if len(got) != wantLen {
					t.Errorf("aliases = %#v; want %d entries", got, wantLen)
				}
			})
		}
		// The real attribute routes are extended: caller alias + tag alias.
		build("field", &SchemaNode{Type: "fixed", Name: "F", Size: 4, Aliases: []string{"prior.P"}}, 2)
		build("props-exact", &SchemaNode{Type: "fixed", Name: "F", Size: 4,
			Props: map[string]any{"aliases": []any{"prior.P"}}}, 2)
		if aliasSets["field"] != aliasSets["props-exact"] {
			t.Errorf("field and exact-Props aliases routes diverge: %v vs %v", aliasSets["field"], aliasSets["props-exact"])
		}
		// Variant routes are inert: only the tag's alias binds.
		build("props-upper", &SchemaNode{Type: "fixed", Name: "F", Size: 4,
			Props: map[string]any{"ALIASES": []any{"prior.P"}}}, 1)
		build("props-mixed", &SchemaNode{Type: "fixed", Name: "F", Size: 4,
			Props: map[string]any{"Aliases": []any{"prior.P"}}}, 1)
	}

	// name / namespace case-variant Props riding beside the real
	// attributes: inert for the walk and for Parse (the exact attributes
	// win), so the composed output equals the variant-free control's.
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
			if got := findNodeAliases(*sv.Root(), "F"); !reflect.DeepEqual(got, []string{"Old"}) {
				t.Errorf("alias not applied: %#v", got)
			}
		})
	}

	// Two tagged fields sharing the custom type.
	{
		twoTagged := []reflect.StructField{
			{Name: "F1", Type: primary, Tag: `avro:"f1,type-alias=Old"`},
			{Name: "F2", Type: primary, Tag: `avro:"f2,type-alias=Old"`},
		}
		// The exact namespace-field route: one x.y.X definition + one
		// dotted reference.
		t.Run("twofields/nsfield", func(t *testing.T) {
			node := &SchemaNode{Type: "record", Name: "X", Namespace: "x.y",
				Fields: []SchemaField{{Name: "c", Type: SchemaNode{Type: "long"}}}}
			s, err := schemaForScopeCell(t, twoTagged, "", []CustomType{{GoType: primary, Schema: node}})
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			defs := strings.Count(s.String(), `"c"`)
			refs := strings.Count(s.String(), `"x.y.X"`)
			if defs != 1 || refs != 1 {
				t.Errorf("want one definition + one dotted reference, got %d defs %d refs: %s", defs, refs, s.String())
			}
			if got := findNodeAliases(*s.Root(), "X"); !reflect.DeepEqual(got, []string{"Old"}) {
				t.Errorf("alias not applied: %#v", got)
			}
		})
		// A "NameSpace" variant-Props route declares nothing: the type is
		// null-namespace X — one definition + one bare reference — and
		// the variant rides on the definition verbatim.
		t.Run("twofields/nsprops", func(t *testing.T) {
			node := &SchemaNode{Type: "record", Name: "X",
				Fields: []SchemaField{{Name: "c", Type: SchemaNode{Type: "long"}}},
				Props:  map[string]any{"NameSpace": "x.y"}}
			s, err := schemaForScopeCell(t, twoTagged, "", []CustomType{{GoType: primary, Schema: node}})
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			if strings.Contains(s.String(), `"x.y.X"`) {
				t.Errorf("variant NameSpace scoped the type: %s", s.String())
			}
			if defs := strings.Count(s.String(), `"c"`); defs != 1 {
				t.Errorf("want one inline definition, got %d bodies: %s", defs, s.String())
			}
			def := findNodeByTypeName(*s.Root(), "record", "X")
			if def == nil {
				t.Fatalf("definition X not found")
			}
			if got := def.Props["NameSpace"]; !reflect.DeepEqual(got, "x.y") {
				t.Errorf(`Props["NameSpace"] = %#v; want the variant preserved verbatim`, got)
			}
			if got := findNodeAliases(*s.Root(), "X"); !reflect.DeepEqual(got, []string{"Old"}) {
				t.Errorf("alias not applied: %#v", got)
			}
		})
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
