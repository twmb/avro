package avro

import (
	"bytes"
	"hash/crc64"
	"reflect"
	"strings"
	"testing"
)

// The caller-COMPOSED and caller-EDITED SchemaNode matrix.
//
// Every other node matrix drives the tree one direction: text → Parse →
// Root() → rebuild, compared against the parse. Two input shapes are outside
// that loop entirely and are the ones a caller actually writes:
//
//   - HAND-BUILT: a SchemaNode the caller assembled, whose field combinations
//     Parse can never produce (a stray Symbols on an int, a Size on a map).
//   - EXTRACTED-THEN-EDITED: sub := s.Root().Fields[i].Type, then a write to
//     an exported field. The struct copy carries hidden state — the
//     name-reference stamp — so the edit and the stamp can disagree, and the
//     node splices a definition that never sees the edit.
//
// The cross is {hand-built, extracted-unedited, extracted-edited} × {every
// exported field} × {Schema, String, Canonical, Fingerprint, JSON}, over
// structures that include the shapes a flat schema cannot exercise: a
// RECURSIVE definition (the reference reaches back through the extraction
// point) and a DIAMOND (two records referencing one definition that lives
// inside a third). A flat schema only ever exercises a type as a DEFINITION;
// these exercise the second-occurrence REFERENCE path, which is where the
// stamp lives.
//
// What every cell asserts: a value or a NAMED error, never a panic, and
// never a silent drop. Which of those a cell is entitled to is read off the
// two classification tables the predicates are derived from
// (bareEmissionFieldRules, nameRefSpliceFieldRules), so a field added to
// SchemaNode later cannot be silently absent from this cross either.

// callerNodeStructure is one enclosing schema plus the coordinates of a
// name-REFERENCE node inside it — the node whose Schema() must splice.
type callerNodeStructure struct {
	name string
	// build returns the parsed enclosing schema. Some structures need a
	// SchemaCache because the definition arrives from a prior Parse.
	build func(t *testing.T) *Schema
	// pick walks to the reference node. It returns a copy, which is exactly
	// what a caller gets and exactly what carries the stamp.
	pick func(SchemaNode) SchemaNode
	// def is the standalone text the reference names; splicing must produce
	// a schema canonically equal to it.
	def string
	// val encodes under def, so a spliced result can be exercised on both
	// wire formats rather than only compared as metadata.
	val any
}

func callerNodeStructures() []callerNodeStructure {
	const inner = `{"type":"record","name":"x.y.Inner","fields":[{"name":"q","type":"int"}]}`
	const nodeDef = `{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]}]}`
	const dDef = `{"type":"fixed","name":"x.y.D","size":4}`

	return []callerNodeStructure{
		{
			name: "second-occurrence",
			build: func(t *testing.T) *Schema {
				return MustParse(`{"type":"record","name":"x.y.Root","fields":[
					{"name":"a","type":` + inner + `},
					{"name":"b","type":"x.y.Inner"}]}`)
			},
			pick: func(r SchemaNode) SchemaNode { return r.Fields[1].Type },
			def:  inner,
			val:  map[string]any{"q": int32(7)},
		},
		{
			name: "forward-reference",
			build: func(t *testing.T) *Schema {
				return MustParse(`{"type":"record","name":"x.y.Root","fields":[
					{"name":"a","type":"x.y.Inner"},
					{"name":"b","type":` + inner + `}]}`)
			},
			pick: func(r SchemaNode) SchemaNode { return r.Fields[0].Type },
			def:  inner,
			val:  map[string]any{"q": int32(7)},
		},
		{
			// RECURSIVE: the branch references the record that encloses it,
			// so splicing it re-enters the union the outer walk is inside.
			name: "recursive",
			build: func(t *testing.T) *Schema {
				return MustParse(nodeDef)
			},
			pick: func(r SchemaNode) SchemaNode { return r.Fields[0].Type.Branches[1] },
			def:  nodeDef,
			val:  map[string]any{"next": nil},
		},
		{
			// DIAMOND: C's reference to D resolves to a definition that lives
			// inside B, a sibling subtree the extraction does not contain.
			name: "diamond",
			build: func(t *testing.T) *Schema {
				return MustParse(`{"type":"record","name":"Root","fields":[
					{"name":"b","type":{"type":"record","name":"B","fields":[{"name":"d","type":` + dDef + `}]}},
					{"name":"c","type":{"type":"record","name":"C","fields":[{"name":"d","type":"x.y.D"}]}}]}`)
			},
			pick: func(r SchemaNode) SchemaNode { return r.Fields[1].Type.Fields[0].Type },
			def:  dDef,
			val:  []byte{1, 2, 3, 4},
		},
		{
			// The definition arrives from a PRIOR Parse. The cache INLINES it
			// at its first occurrence, so the first field is the definition
			// itself and only the SECOND is a reference — picking the first
			// would measure a node that never splices.
			name: "cache-cross-parse",
			build: func(t *testing.T) *Schema {
				t.Helper()
				var c SchemaCache
				if _, err := c.Parse(inner); err != nil {
					t.Fatal(err)
				}
				s, err := c.Parse(`{"type":"record","name":"x.y.Outer","fields":[
					{"name":"a","type":"x.y.Inner"},
					{"name":"b","type":"x.y.Inner"}]}`)
				if err != nil {
					t.Fatal(err)
				}
				return s
			},
			pick: func(r SchemaNode) SchemaNode { return r.Fields[1].Type },
			def:  inner,
			val:  map[string]any{"q": int32(7)},
		},
	}
}

// surfaceReport is what one node's five surfaces produced. A nil err with an
// empty text is impossible; a panic is recorded as such and is always a
// failure, since the whole point of these inputs is that a caller can compose
// anything and must still get a value or a named error.
type surfaceReport struct {
	panicked any
	err      error
	text     string
	canon    []byte
	finger   []byte
	jsonWire []byte
}

// driveSurfaces runs every caller-reachable surface of a SchemaNode, catching
// panics so a panic becomes a reported failure rather than a dead test binary.
func driveSurfaces(n SchemaNode, val any) (rep surfaceReport) {
	defer func() {
		if r := recover(); r != nil {
			rep.panicked = r
		}
	}()
	s, err := n.Schema()
	if err != nil {
		rep.err = err
		return rep
	}
	rep.text = s.String()
	rep.canon = s.Canonical()
	rep.finger = s.Fingerprint(crc64.New(crc64.MakeTable(crc64.ECMA)))
	if val != nil {
		if j, jerr := s.EncodeJSON(val); jerr == nil {
			rep.jsonWire = j
		}
	}
	return rep
}

// checkSurfaces applies the invariants every cell owes regardless of which
// axis produced it.
func checkSurfaces(t *testing.T, where string, rep surfaceReport) bool {
	t.Helper()
	if rep.panicked != nil {
		t.Errorf("%s: PANIC %v — a caller-composed node must produce a value or a named error", where, rep.panicked)
		return false
	}
	if rep.err != nil {
		if msg := rep.err.Error(); msg == "" {
			t.Errorf("%s: an empty error message is not a named error", where)
		} else if len(msg) > 2048 {
			t.Errorf("%s: error message is %d bytes; a rejection must not echo the input unbounded", where, len(msg))
		}
		return false
	}
	if rep.text == "" || len(rep.canon) == 0 || len(rep.finger) == 0 {
		t.Errorf("%s: build succeeded but a surface came back empty (text %q, canon %q)", where, rep.text, rep.canon)
		return false
	}
	// The emitted text is the schema's own claim about itself: re-parsing it
	// must reproduce the same canonical form and the same fingerprint, or one
	// of the three surfaces is lying about the others.
	re, err := Parse(rep.text)
	if err != nil {
		t.Errorf("%s: String() emitted text that does not re-parse: %v\n%s", where, err, rep.text)
		return false
	}
	if !bytes.Equal(re.Canonical(), rep.canon) {
		t.Errorf("%s: canonical form drifts across the text round trip:\n emitted %s\nreparsed %s", where, rep.canon, re.Canonical())
	}
	if !bytes.Equal(re.Fingerprint(crc64.New(crc64.MakeTable(crc64.ECMA))), rep.finger) {
		t.Errorf("%s: fingerprint drifts across the text round trip", where)
	}
	return true
}

// TestMatrix_CallerComposedAndEditedNodes crosses the origin, field and
// surface axes over every structure.
func TestMatrix_CallerComposedAndEditedNodes(t *testing.T) {
	rt := reflect.TypeFor[SchemaNode]()
	exportedFields := func() []reflect.StructField {
		var out []reflect.StructField
		for i := range rt.NumField() {
			if f := rt.Field(i); f.IsExported() && f.Name != "Type" {
				out = append(out, f)
			}
		}
		return out
	}()
	if len(exportedFields) < 12 {
		t.Fatalf("only %d exported fields found; the walk is not seeing SchemaNode", len(exportedFields))
	}

	var cells int

	// ---- origin: EXTRACTED (unedited control, then one edit per field) ----
	for _, st := range callerNodeStructures() {
		t.Run("extracted/"+st.name, func(t *testing.T) {
			s := st.build(t)
			want := MustParse(st.def)

			// extracted-unedited: the control. The field axis does not apply
			// — nothing is written — so it runs once per structure, and it is
			// what proves every "blocked" verdict below is a real change of
			// outcome rather than a node that never spliced to begin with.
			//
			// The precondition is the probe-reaches-the-path check, asserted
			// rather than assumed: the picked node must BE a stamped bare
			// reference. A structure whose pick lands on the DEFINITION (the
			// cache inlines its first occurrence) never splices at all, so
			// every verdict below would be answering a different question.
			ctrl := st.pick(s.Root())
			if !nodeIsNameRefShape(&ctrl) || !nodeRefTargetAgrees(&ctrl) {
				t.Fatalf("structure %q does not pick a stamped bare reference (Type=%q shape=%v stamp=%v); the probe never reaches the splice",
					st.name, ctrl.Type, nodeIsNameRefShape(&ctrl), nodeRefTargetAgrees(&ctrl))
			}
			rep := driveSurfaces(ctrl, st.val)
			cells++
			if checkSurfaces(t, "unedited control", rep) {
				if !bytes.Equal(rep.canon, want.Canonical()) {
					t.Fatalf("unedited control did not splice the definition:\n got %s\nwant %s", rep.canon, want.Canonical())
				}
				if len(rep.jsonWire) == 0 {
					t.Errorf("unedited control produced no JSON wire form for %#v", st.val)
				}
			} else {
				t.Fatalf("unedited control must splice; nothing below means anything otherwise")
			}

			for _, f := range exportedFields {
				t.Run("edit/"+f.Name, func(t *testing.T) {
					n := st.pick(s.Root())
					fv := reflect.ValueOf(&n).Elem().FieldByName(f.Name)
					if !setNonZeroForTest(f.Name, fv) {
						t.Fatalf("cannot populate %s (kind %s)", f.Name, f.Type.Kind())
					}
					rule := nameRefSpliceFieldRules[f.Name]
					splices := nodeIsNameRefShape(&n)
					if (rule.exempt != "") != splices {
						t.Fatalf("classification disagrees with the predicate for %s: exempt=%q splices=%v", f.Name, rule.exempt, splices)
					}
					rep := driveSurfaces(n, st.val)
					cells++
					ok := checkSurfaces(t, "edited "+f.Name, rep)
					if rule.exempt != "" {
						// Exempt: the splice still happens, the definition is
						// unchanged, and the usage-site value is dropped by
						// design. Every surface must still agree.
						if !ok {
							t.Fatalf("%s is exempt, so the reference must still splice: %v", f.Name, rep.err)
						}
						if f.Name != "Props" && !bytes.Equal(rep.canon, want.Canonical()) {
							t.Errorf("%s is a usage-site attribute, so the spliced definition must be unchanged:\n got %s\nwant %s", f.Name, rep.canon, want.Canonical())
						}
						return
					}
					// Non-exempt: the node renders AS-WRITTEN, so the outcome
					// is either a named error (the reference now dangles,
					// which is the loud judgment the contract promises) or a
					// schema that still carries the edit — and where it
					// carries it is the OTHER predicate's question, read off
					// the bare-emission table. Silence is what is ruled out.
					if !ok {
						return // named error, already checked
					}
					bare := bareEmissionFieldRules[f.Name]
					back := re(t, rep.text)
					switch {
					case bare.exempt != "" || bare.droppedKey != "":
						// Classified as carrying nothing on this carrier, or
						// as dropped by the reserved-name routing; either way
						// the loss is adjudicated in that table, not here.
					case bare.propsKey != "":
						if _, has := back.Props[bare.propsKey]; !has {
							t.Errorf("%s renders as-written and must ride to Props under %q; Props came back %v from %s", f.Name, bare.propsKey, back.Props, rep.text)
						}
					default:
						got := reflect.ValueOf(back).FieldByName(f.Name).Interface()
						if reflect.DeepEqual(got, reflect.Zero(f.Type).Interface()) {
							t.Errorf("%s survived the build but is gone from the result — a caller's write was silently discarded: %s", f.Name, rep.text)
						}
					}
				})
			}
		})
	}

	// ---- origin: HAND-BUILT (no stamp; combinations Parse cannot produce) ----
	for _, carrier := range []struct {
		name string
		node SchemaNode
		val  any
	}{
		{"primitive", SchemaNode{Type: "int"}, int32(3)},
		{"array", SchemaNode{Type: "array", Items: &SchemaNode{Type: "int"}}, []any{int32(1)}},
		{"map", SchemaNode{Type: "map", Values: &SchemaNode{Type: "int"}}, map[string]any{"k": int32(1)}},
		{"record", SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{{Name: "f", Type: SchemaNode{Type: "int"}}}}, map[string]any{"f": int32(1)}},
		{"enum", SchemaNode{Type: "enum", Name: "E", Symbols: []string{"A", "B"}}, "A"},
		{"fixed", SchemaNode{Type: "fixed", Name: "F", Size: 2}, []byte{1, 2}},
		{"union", SchemaNode{Type: "union", Branches: []SchemaNode{{Type: "null"}, {Type: "int"}}}, nil},
	} {
		t.Run("hand-built/"+carrier.name, func(t *testing.T) {
			base := driveSurfaces(carrier.node, carrier.val)
			cells++
			if !checkSurfaces(t, "bare carrier", base) {
				t.Fatalf("the bare carrier must build; nothing below means anything otherwise")
			}

			for _, f := range exportedFields {
				t.Run("set/"+f.Name, func(t *testing.T) {
					n := carrier.node
					fv := reflect.ValueOf(&n).Elem().FieldByName(f.Name)
					if !alreadySetForTest(carrier.node, f.Name) {
						if !setNonZeroForTest(f.Name, fv) {
							t.Fatalf("cannot populate %s (kind %s)", f.Name, f.Type.Kind())
						}
					}
					rep := driveSurfaces(n, carrier.val)
					cells++
					if !checkSurfaces(t, "hand-built "+f.Name, rep) {
						return
					}
					// A key the carrier's kind does not bind is INERT: it may
					// change the rendered text, but never the canonical form,
					// the fingerprint, or the wire image. That is the same
					// inertness the attribute-placement census asserts for
					// parsed input, extended to input Parse cannot produce.
					if inertForCarrier(carrier.node.Type, f.Name) {
						if !bytes.Equal(rep.canon, base.canon) {
							t.Errorf("%s is inert on a %s, but it changed the canonical form:\n got %s\nwant %s", f.Name, carrier.node.Type, rep.canon, base.canon)
						}
						if !bytes.Equal(rep.finger, base.finger) {
							t.Errorf("%s is inert on a %s, but it changed the fingerprint", f.Name, carrier.node.Type)
						}
						if carrier.val != nil && !bytes.Equal(rep.jsonWire, base.jsonWire) {
							t.Errorf("%s is inert on a %s, but it changed the JSON wire image: %s vs %s", f.Name, carrier.node.Type, rep.jsonWire, base.jsonWire)
						}
					}
				})
			}
		})
	}

	t.Logf("caller-composed/edited coverage: %d cells across %d structures × %d exported fields × {Schema, String, Canonical, Fingerprint, JSON}",
		cells, len(callerNodeStructures()), len(exportedFields))
}

// re re-parses emitted text and returns its Root, failing the test rather
// than returning an unusable zero node.
func re(t *testing.T, text string) SchemaNode {
	t.Helper()
	s, err := Parse(text)
	if err != nil {
		t.Fatalf("re-parse of emitted text failed: %v\n%s", err, text)
	}
	return s.Root()
}

// alreadySetForTest reports whether the carrier already populates this field
// as part of being a well-formed node of its kind, in which case overwriting
// it would test a different node rather than a stray addition.
func alreadySetForTest(carrier SchemaNode, field string) bool {
	v := reflect.ValueOf(carrier).FieldByName(field)
	return v.IsValid() && !v.IsZero()
}

// inertForCarrier reports whether a value in field is metadata the carrier's
// kind does not bind, so it can never reach the canonical form or the wire.
// The canonical form keeps only type, name, fields, symbols, items, values
// and size, and only where the kind actually binds them.
func inertForCarrier(kind, field string) bool {
	switch field {
	case "Doc", "Aliases", "LogicalType", "Precision", "Scale", "Props", "EnumDefault", "HasEnumDefault":
		return true
	case "Namespace":
		// A namespace on a NAMED kind is part of its fullname, which the
		// canonical form keeps.
		return !isNamedKind(kind)
	case "Name":
		return !isNamedKind(kind)
	case "Fields":
		return !isRecordKind(kind)
	case "Items":
		return kind != "array"
	case "Values":
		return kind != "map"
	case "Symbols":
		return kind != "enum"
	case "Size":
		return kind != "fixed"
	case "Branches":
		return kind != "union"
	}
	return false
}

// The classification tables the two predicates are derived from must stay in
// step with SchemaNode itself: a name in a table that is not a field is a
// stale classification, and it would silently exempt nothing while reading
// as though it exempted something.
func TestInvariant_NodeFieldRuleTablesNameRealFields(t *testing.T) {
	rt := reflect.TypeFor[SchemaNode]()
	for _, tbl := range []struct {
		name  string
		rules map[string]schemaNodeFieldRule
	}{
		{"bareEmissionFieldRules", bareEmissionFieldRules},
		{"nameRefSpliceFieldRules", nameRefSpliceFieldRules},
	} {
		for field, rule := range tbl.rules {
			f, ok := rt.FieldByName(field)
			if !ok || !f.IsExported() {
				t.Errorf("%s classifies %q, which is not an exported SchemaNode field", tbl.name, field)
				continue
			}
			if rule.exempt == "" && rule.propsKey == "" && rule.droppedKey == "" {
				t.Errorf("%s[%q] states no classification at all; the zero rule is the ordinary case and belongs OUT of the table", tbl.name, field)
			}
			if rule.droppedKey != "" && rule.why == "" {
				t.Errorf("%s[%q] records a dropped key with no policy quoted; a documented loss must name the policy that makes it one", tbl.name, field)
			}
			if rule.exempt != "" && (rule.propsKey != "" || rule.droppedKey != "") {
				t.Errorf("%s[%q] is both exempt and routed; a field has exactly one classification", tbl.name, field)
			}
		}
	}
	// And the reverse: the splice table's exemptions are the adjudicated
	// usage-site attribute set. Widening it silently is how a caller's write
	// starts vanishing again, so the set itself is pinned.
	want := []string{"Aliases", "Doc", "LogicalType", "Namespace", "Props"}
	var got []string
	for k := range nameRefSpliceFieldRules {
		got = append(got, k)
	}
	if strings.Join(sortedStrings(got), ",") != strings.Join(want, ",") {
		t.Errorf("the name-reference exemption set changed to %v; it is exactly the reserved usage-site attributes a splice drops plus the props it merges, so a change here is a policy change", sortedStrings(got))
	}
}

func sortedStrings(in []string) []string {
	out := append([]string(nil), in...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}
