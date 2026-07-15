package avro

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// TestMatrix_SchemaForCustomSchemaScope crosses the namespace-composition
// space of CustomType.Schema embedding: a custom schema is an independently
// authored tree with its own namespace scoping, and SchemaFor must preserve
// every declared fullname when composing it into the inferred tree.
//
// Axes: custom-schema spelling {split Root()-derived, dotted hand-built
// SchemaNode, null-namespace} × kind {record, enum, fixed} × occurrences
// {one, two fields} × SchemaFor scope {default, WithNamespace} × shape
// {flat; recursive — the custom schema references itself, so its internal
// references must still bind after embedding; a nested named type in a
// DIFFERENT namespace inside the custom subtree}, plus coexistence cells
// (a.X + null-namespace X; a.X + b.X; two customs carrying IDENTICAL
// definitions dedup to one definition + a reference) and the
// unrepresentable corner: a null-namespace type recurring under
// WithNamespace has no reference spelling (a bare name binds in the
// enclosing namespace; references have no "namespace":"" escape), so that
// cell must produce exactly the named error — never a dangling reference or
// a namespace capture.
//
// Oracle per cell: the SchemaFor pipeline succeeds (or hits exactly the
// corner error); the output re-parses; the parsed metadata preserves every
// declared fullname; split and dotted spellings of the same schema produce
// byte-identical Canonical() — the spec ("Names") makes the two spellings
// one name, so their canonical forms must agree; and an EXECUTED fastavro
// arm parses representative outputs (which carry dotted references and
// "namespace":"" escapes) and must agree on the full parsing canonical
// form, which subsumes fingerprint equality without any byte-order
// presentation trap.

// Marker Go types the matrix's CustomTypes match on. Identity only matters
// within one cell, so two markers cover every layout.
type (
	scopeMatrixPrimary struct{ A int64 }
	scopeMatrixPartner struct{ B int64 }
)

// schemaForScopeCell mirrors SchemaFor's pipeline (inferRecord →
// dedupNamedTypes → Marshal → Parse with the same opts) over a
// reflect.StructOf-built struct, so cells can vary field layout at runtime
// where the compile-time-generic SchemaFor[T] cannot.
//
// Every cell doubles as a mutation probe: each CustomType.Schema is
// deep-snapshotted before the build and deep-compared after, pinning the
// contract that a build never writes into caller-owned SchemaNode storage
// (the metadata render hands Props containers over by reference, and the
// composition walkers mutate the tree they are given — the boundary copy
// in renderCustomSchemaTree is what keeps those writes off the caller's
// maps). The comparison runs whether or not the build errors: a mutation
// on an error path is just as much a contract break.
func schemaForScopeCell(t *testing.T, fields []reflect.StructField, namespace string, customs []CustomType) (*Schema, error) {
	t.Helper()
	snaps := make([]*SchemaNode, len(customs))
	for i, ct := range customs {
		snaps[i] = snapshotSchemaNode(ct.Schema, make(map[*SchemaNode]*SchemaNode))
	}
	defer func() {
		for i, ct := range customs {
			if !reflect.DeepEqual(snaps[i], ct.Schema) {
				t.Errorf("build mutated caller-owned CustomType.Schema storage (custom %d):\n before: %#v\n after:  %#v", i, snaps[i], ct.Schema)
			}
		}
	}()
	st := reflect.StructOf(fields)
	seen := make(map[reflect.Type]seenForm)
	s, err := inferRecord(st, "Top", namespace, seen, customs, make(appliedTypeAliases))
	if err != nil {
		return nil, err
	}
	s, err = dedupNamedTypes(s, make(map[string]string), "")
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	opts := make([]SchemaOpt, len(customs))
	for i, ct := range customs {
		opts[i] = ct
	}
	return Parse(string(b), opts...)
}

// snapshotSchemaNode deep-copies a SchemaNode tree, including the dynamic
// containers reachable through Props and Default values, so a post-build
// reflect.DeepEqual against the original detects any write the build made
// into caller-owned storage. visited maps original Items/Values pointers to
// their copies so pointer-built cycles copy with their topology intact.
func snapshotSchemaNode(n *SchemaNode, visited map[*SchemaNode]*SchemaNode) *SchemaNode {
	if n == nil {
		return nil
	}
	if c, ok := visited[n]; ok {
		return c
	}
	c := &SchemaNode{}
	visited[n] = c
	*c = *n
	c.Aliases = append([]string(nil), n.Aliases...)
	c.Symbols = append([]string(nil), n.Symbols...)
	c.Items = snapshotSchemaNode(n.Items, visited)
	c.Values = snapshotSchemaNode(n.Values, visited)
	if n.Props != nil {
		c.Props = snapshotAnyValue(n.Props).(map[string]any)
	}
	if n.Branches != nil {
		c.Branches = make([]SchemaNode, len(n.Branches))
		for i := range n.Branches {
			c.Branches[i] = *snapshotSchemaNode(&n.Branches[i], visited)
		}
	}
	if n.Fields != nil {
		c.Fields = make([]SchemaField, len(n.Fields))
		for i, f := range n.Fields {
			cf := f
			cf.Aliases = append([]string(nil), f.Aliases...)
			cf.Type = *snapshotSchemaNode(&f.Type, visited)
			cf.Default = snapshotAnyValue(f.Default)
			if f.Props != nil {
				cf.Props = snapshotAnyValue(f.Props).(map[string]any)
			}
			c.Fields[i] = cf
		}
	}
	return c
}

// snapshotAnyValue deep-copies the JSON-shaped dynamic containers a Props
// or Default value can hold; scalars are immutable and copy by value.
func snapshotAnyValue(v any) any {
	switch v := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, val := range v {
			out[k] = snapshotAnyValue(val)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, e := range v {
			out[i] = snapshotAnyValue(e)
		}
		return out
	case []map[string]any:
		out := make([]map[string]any, len(v))
		for i, m := range v {
			out[i] = snapshotAnyValue(m).(map[string]any)
		}
		return out
	case []byte:
		return append([]byte(nil), v...)
	}
	return v
}

// buildScopeCustomNode returns the custom schema for one (spelling, kind,
// shape) combination plus the fullnames it declares. The spelling axis also
// varies the construction route: split and null-namespace schemas arrive
// via Parse(...).Root() (the metadata-derived path), the dotted spelling is
// a hand-built SchemaNode (the literal-construction path).
func buildScopeCustomNode(t *testing.T, spelling, kind, shape string) (*SchemaNode, []string) {
	t.Helper()
	if kind != "record" && shape != "flat" {
		t.Fatalf("shape %q applies to records only", shape)
	}
	// The declared name per spelling: base short name with namespace "a"
	// for split/dotted, bare for null-namespace. Recursive cells use a
	// distinct short name so the corner error's identity is visible.
	short := "X"
	if shape == "recursive" {
		short = "N"
	}
	if spelling == "dotted" {
		n := &SchemaNode{Type: kind, Name: "a." + short}
		switch kind {
		case "enum":
			n.Symbols = []string{"A", "B"}
		case "fixed":
			n.Size = 4
		case "record":
			switch shape {
			case "flat":
				n.Fields = []SchemaField{{Name: "n", Type: SchemaNode{Type: "int"}}}
			case "recursive":
				n.Fields = []SchemaField{{Name: "next", Type: SchemaNode{
					Type: "union", Branches: []SchemaNode{{Type: "null"}, {Type: "a." + short}},
				}}}
			case "nestedforeign":
				n.Fields = []SchemaField{{Name: "inner", Type: SchemaNode{
					Type: "record", Name: "q.Inner",
					Fields: []SchemaField{{Name: "m", Type: SchemaNode{Type: "int"}}},
				}}}
			}
		}
		full := []string{"a." + short}
		if shape == "nestedforeign" {
			full = append(full, "q.Inner")
		}
		return n, full
	}

	nsAttr := `,"namespace":"a"`
	fullPrefix := "a."
	if spelling == "nullns" {
		nsAttr = ""
		fullPrefix = ""
	}
	var body string
	switch kind {
	case "enum":
		body = fmt.Sprintf(`{"type":"enum","name":"%s"%s,"symbols":["A","B"]}`, short, nsAttr)
	case "fixed":
		body = fmt.Sprintf(`{"type":"fixed","name":"%s"%s,"size":4}`, short, nsAttr)
	case "record":
		switch shape {
		case "flat":
			body = fmt.Sprintf(`{"type":"record","name":"%s"%s,"fields":[{"name":"n","type":"int"}]}`, short, nsAttr)
		case "recursive":
			body = fmt.Sprintf(`{"type":"record","name":"%s"%s,"fields":[{"name":"next","type":["null","%s"]}]}`, short, nsAttr, short)
		case "nestedforeign":
			body = fmt.Sprintf(`{"type":"record","name":"%s"%s,"fields":[{"name":"inner","type":{"type":"record","name":"Inner","namespace":"q","fields":[{"name":"m","type":"int"}]}}]}`, short, nsAttr)
		}
	}
	s, err := Parse(body)
	if err != nil {
		t.Fatalf("parse custom schema %s: %v", body, err)
	}
	root := s.Root()
	full := []string{fullPrefix + short}
	if shape == "nestedforeign" {
		full = append(full, "q.Inner")
	}
	return &root, full
}

// collectScopeNames walks the metadata tree with the parser's scope rules,
// gathering every named DEFINITION's resolved fullname into defs and every
// name-reference spelling with its enclosing namespace into refs. Root()
// resolves a definition's Namespace field, so a definition's fullname reads
// directly off the node; a reference surfaces as a bare node whose Type
// holds the spelling as written, whose meaning depends on the enclosing
// scope.
func collectScopeNames(n SchemaNode, enclosingNS string, defs map[string]bool, refs *[][2]string) {
	switch n.Type {
	case "record", "error", "enum", "fixed":
		full := n.Name
		if n.Namespace != "" && !strings.Contains(n.Name, ".") {
			full = n.Namespace + "." + n.Name
		}
		defs[full] = true
		childNS := ""
		if i := strings.LastIndex(full, "."); i >= 0 {
			childNS = full[:i]
		}
		for i := range n.Fields {
			collectScopeNames(n.Fields[i].Type, childNS, defs, refs)
		}
	case "array":
		if n.Items != nil {
			collectScopeNames(*n.Items, enclosingNS, defs, refs)
		}
	case "map":
		if n.Values != nil {
			collectScopeNames(*n.Values, enclosingNS, defs, refs)
		}
	case "union":
		for i := range n.Branches {
			collectScopeNames(n.Branches[i], enclosingNS, defs, refs)
		}
	case "null", "boolean", "int", "long", "float", "double", "string", "bytes":
	default:
		*refs = append(*refs, [2]string{n.Type, enclosingNS})
	}
}

// assertScopeFullnames asserts the schema's DEFINITION fullname set equals
// want exactly (a namespace capture or a duplicated definition both change
// the set), and that every name reference binds to one of those
// definitions under the parser's rules: enclosing-namespace-qualified
// first, then the null-namespace fallback for a bare spelling.
func assertScopeFullnames(t *testing.T, s *Schema, want []string) {
	t.Helper()
	defs := make(map[string]bool)
	var refs [][2]string
	root := s.Root()
	collectScopeNames(root, "", defs, &refs)
	wantSet := make(map[string]bool, len(want))
	for _, w := range want {
		wantSet[w] = true
	}
	for w := range wantSet {
		if !defs[w] {
			t.Errorf("fullname %q missing from output definitions (got %v)", w, defs)
		}
	}
	for d := range defs {
		if !wantSet[d] {
			t.Errorf("unexpected definition %q in output (want %v)", d, want)
		}
	}
	for _, r := range refs {
		spelling, scope := r[0], r[1]
		switch {
		case strings.Contains(spelling, "."):
			if !defs[spelling] {
				t.Errorf("dotted reference %q does not bind any definition (%v)", spelling, defs)
			}
		case scope != "" && defs[scope+"."+spelling]:
			// binds in the enclosing namespace
		case defs[spelling]:
			// null-namespace fallback
		default:
			t.Errorf("bare reference %q in scope %q does not bind any definition (%v)", spelling, scope, defs)
		}
	}
}

func scopeCellFields(occurrences int, goType reflect.Type) []reflect.StructField {
	fields := []reflect.StructField{{Name: "F1", Type: goType}}
	if occurrences == 2 {
		fields = append(fields, reflect.StructField{Name: "F2", Type: goType})
	}
	return fields
}

func TestMatrix_SchemaForCustomSchemaScope(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	partner := reflect.TypeFor[scopeMatrixPartner]()

	kindShapes := []struct{ kind, shape string }{
		{"record", "flat"},
		{"record", "recursive"},
		{"record", "nestedforeign"},
		{"enum", "flat"},
		{"fixed", "flat"},
	}

	for _, spelling := range []string{"split", "dotted", "nullns"} {
		for _, ks := range kindShapes {
			for _, occurrences := range []int{1, 2} {
				for _, ns := range []string{"", "b"} {
					name := fmt.Sprintf("%s/%s_%s/occ%d/ns=%q", spelling, ks.kind, ks.shape, occurrences, ns)
					t.Run(name, func(t *testing.T) {
						node, fullnames := buildScopeCustomNode(t, spelling, ks.kind, ks.shape)
						ct := CustomType{GoType: primary, Schema: node}
						s, err := schemaForScopeCell(t, scopeCellFields(occurrences, primary), ns, []CustomType{ct})

						// The one unrepresentable combination: a
						// null-namespace type recurring inside a namespaced
						// scope has no reference spelling.
						if spelling == "nullns" && occurrences == 2 && ns != "" {
							if err == nil {
								t.Fatalf("null-namespace type recurring under WithNamespace must error; got schema %s", s.String())
							}
							want := fmt.Sprintf("the null-namespace type %q recurs inside namespace %q", fullnames[0], ns)
							if !strings.Contains(err.Error(), want) {
								t.Fatalf("error %q does not name the corner (%q)", err, want)
							}
							return
						}
						if err != nil {
							t.Fatalf("cell errored: %v", err)
						}
						if _, err := Parse(s.String()); err != nil {
							t.Fatalf("output does not re-parse: %v", err)
						}
						top := "Top"
						if ns != "" {
							top = ns + ".Top"
						}
						assertScopeFullnames(t, s, append([]string{top}, fullnames...))
					})
				}
			}
		}
	}

	// Coexistence cells: distinct fullnames sharing a short name must
	// coexist; identical definitions supplied by two DIFFERENT customs
	// must dedup to one definition plus a reference.
	for _, ns := range []string{"", "b"} {
		nsName := fmt.Sprintf("ns=%q", ns)
		top := "Top"
		if ns != "" {
			top = ns + ".Top"
		}
		t.Run("coexist/aX_nullX/"+nsName, func(t *testing.T) {
			aNode, _ := buildScopeCustomNode(t, "split", "record", "flat")
			nullNode, _ := buildScopeCustomNode(t, "nullns", "record", "flat")
			fields := []reflect.StructField{
				{Name: "F1", Type: primary},
				{Name: "F2", Type: partner},
			}
			customs := []CustomType{
				{GoType: primary, Schema: aNode},
				{GoType: partner, Schema: nullNode},
			}
			s, err := schemaForScopeCell(t, fields, ns, customs)
			if err != nil {
				t.Fatalf("a.X + null-namespace X must coexist: %v", err)
			}
			assertScopeFullnames(t, s, []string{top, "a.X", "X"})
		})
		t.Run("coexist/aX_bX/"+nsName, func(t *testing.T) {
			aNode, _ := buildScopeCustomNode(t, "split", "record", "flat")
			bSchema, err := Parse(`{"type":"record","name":"X","namespace":"b","fields":[{"name":"n","type":"int"}]}`)
			if err != nil {
				t.Fatal(err)
			}
			bRoot := bSchema.Root()
			fields := []reflect.StructField{
				{Name: "F1", Type: primary},
				{Name: "F2", Type: partner},
			}
			customs := []CustomType{
				{GoType: primary, Schema: aNode},
				{GoType: partner, Schema: &bRoot},
			}
			s, err := schemaForScopeCell(t, fields, ns, customs)
			if err != nil {
				t.Fatalf("a.X + b.X must coexist: %v", err)
			}
			assertScopeFullnames(t, s, []string{top, "a.X", "b.X"})
		})
		t.Run("coexist/identical_dedup/"+nsName, func(t *testing.T) {
			n1, _ := buildScopeCustomNode(t, "split", "record", "flat")
			n2, _ := buildScopeCustomNode(t, "dotted", "record", "flat")
			// Two distinct customs carry the SAME definition of a.X — one
			// split-derived, one dotted hand-built — so the dedup must
			// treat the spellings as one name and emit one definition
			// plus a reference, exercising the scope-normalized equality.
			fields := []reflect.StructField{
				{Name: "F1", Type: primary},
				{Name: "F2", Type: partner},
			}
			customs := []CustomType{
				{GoType: primary, Schema: n1},
				{GoType: partner, Schema: n2},
			}
			s, err := schemaForScopeCell(t, fields, ns, customs)
			if err != nil {
				t.Fatalf("identical a.X definitions from two customs must dedup: %v", err)
			}
			assertScopeFullnames(t, s, []string{top, "a.X"})
			// Exactly one inline definition: the second occurrence is a
			// reference, so the schema text contains a single "fields"
			// body for a.X.
			if n := strings.Count(s.String(), `"name":"n"`); n != 1 {
				t.Errorf("want exactly one inline a.X definition, found %d bodies in %s", n, s.String())
			}
		})
	}

	// Wrong-bind decoy: a null-namespace custom X used before AND after a
	// Go-inferred record that owns fullname b.X. The recurrence of the
	// null-namespace X inside scope b must hit the corner error — a bare
	// "X" reference would silently bind the DIFFERENT type b.X.
	t.Run("corner/wrongbind_decoy", func(t *testing.T) {
		nullNode, _ := buildScopeCustomNode(t, "nullns", "record", "flat")
		type X struct{ M int32 }
		fields := []reflect.StructField{
			{Name: "F1", Type: primary},
			{Name: "F2", Type: reflect.TypeFor[X]()},
			{Name: "F3", Type: primary},
		}
		customs := []CustomType{{GoType: primary, Schema: nullNode}}
		_, err := schemaForScopeCell(t, fields, "b", customs)
		if err == nil || !strings.Contains(err.Error(), `the null-namespace type "X" recurs inside namespace "b"`) {
			t.Fatalf("decoy cell must hit the corner error, got: %v", err)
		}
	})

	// Spelling equivalence: the spec makes the split and dotted spellings
	// one name, so for every (kind, shape, occurrences, scope) the two
	// spellings' outputs must agree byte-for-byte on Canonical().
	for _, ks := range kindShapes {
		for _, occurrences := range []int{1, 2} {
			for _, ns := range []string{"", "b"} {
				name := fmt.Sprintf("equiv/%s_%s/occ%d/ns=%q", ks.kind, ks.shape, occurrences, ns)
				t.Run(name, func(t *testing.T) {
					splitNode, _ := buildScopeCustomNode(t, "split", ks.kind, ks.shape)
					dottedNode, _ := buildScopeCustomNode(t, "dotted", ks.kind, ks.shape)
					sSplit, err := schemaForScopeCell(t, scopeCellFields(occurrences, primary), ns, []CustomType{{GoType: primary, Schema: splitNode}})
					if err != nil {
						t.Fatalf("split: %v", err)
					}
					sDotted, err := schemaForScopeCell(t, scopeCellFields(occurrences, primary), ns, []CustomType{{GoType: primary, Schema: dottedNode}})
					if err != nil {
						t.Fatalf("dotted: %v", err)
					}
					if string(sSplit.Canonical()) != string(sDotted.Canonical()) {
						t.Errorf("split and dotted spellings disagree:\n split: %s\ndotted: %s", sSplit.Canonical(), sDotted.Canonical())
					}
				})
			}
		}
	}

	// Props-carried container routes: a Props VALUE shaped like (or
	// containing) a named definition is reachable by the composition
	// walkers through the items/values keys and union slices, and the
	// metadata render hands it over BY REFERENCE when it needs no JSON
	// fixup. Every route × scope must leave the caller's storage untouched
	// — the cell helper's snapshot asserts that — and the direct map check
	// below re-asserts it on the user's own map object, independent of the
	// snapshot machinery.
	for _, route := range []string{"items", "values", "unionslice"} {
		for _, ns := range []string{"", "b"} {
			t.Run(fmt.Sprintf("propscarried/%s/ns=%q", route, ns), func(t *testing.T) {
				userOwned := map[string]any{"type": "fixed", "name": "G", "size": 1}
				want := map[string]any{"type": "fixed", "name": "G", "size": 1}
				var carried any = userOwned
				if route == "unionslice" {
					carried = []any{userOwned}
				}
				key := route
				if route == "unionslice" {
					key = "items"
				}
				node := &SchemaNode{Type: "string", Props: map[string]any{key: carried}}
				_, err := schemaForScopeCell(t, scopeCellFields(1, primary), ns, []CustomType{{GoType: primary, Schema: node}})
				if err != nil {
					t.Fatalf("cell errored: %v", err)
				}
				if !reflect.DeepEqual(userOwned, want) {
					t.Errorf("caller-owned Props map changed: %v, want %v", userOwned, want)
				}
			})
		}
	}
}

// The EXECUTED fastavro arm for this matrix lives in
// matrix_schemafor_scope_differential_test.go (package avro_test, where the
// oracle harness lives), driving representative cells through the public
// SchemaFor entry point.
