package avro

import (
	"bytes"
	"testing"
)

// These tests pin issue #42: a SchemaNode extracted from Schema.Root whose
// Type is a NAME REFERENCE (the definition lives elsewhere in the enclosing
// schema — an earlier field, a prior SchemaCache.Parse, or the enclosing
// record itself) must still convert via SchemaNode.Schema. The resulting
// schema must be self-contained and equal, canonical-bytes and wire, to a
// from-scratch Parse of the equivalent standalone schema text.

// requireSubSchema converts node via Schema(), requires success, and
// requires canonical + wire equality with the standalone schema text want.
// val is a value encodable by the schema, exercised in both directions so
// a metadata-only match cannot mask a diverged codec.
func requireSubSchema(t *testing.T, node *SchemaNode, want string, val any) *Schema {
	t.Helper()
	got, err := node.Schema()
	if err != nil {
		t.Fatalf("SchemaNode.Schema() failed: %v", err)
	}
	ws := MustParse(want)
	if !bytes.Equal(got.Canonical(), ws.Canonical()) {
		t.Fatalf("canonical mismatch:\n got: %s\nwant: %s", got.Canonical(), ws.Canonical())
	}
	enc, err := got.Encode(val)
	if err != nil {
		t.Fatalf("encode with sub-schema: %v", err)
	}
	wantEnc, err := ws.Encode(val)
	if err != nil {
		t.Fatalf("encode with standalone schema: %v", err)
	}
	if !bytes.Equal(enc, wantEnc) {
		t.Fatalf("wire mismatch: got %x want %x", enc, wantEnc)
	}
	var rt any
	if _, err := ws.Decode(enc, &rt); err != nil {
		t.Fatalf("standalone schema cannot decode sub-schema bytes: %v", err)
	}
	return got
}

const addrDef = `{"type":"record","name":"com.example.Address","fields":[{"name":"street","type":"string"}]}`

var addrVal = map[string]any{"street": "main"}

// Case 1 (the issue's exact flow): reference to a type defined in a prior
// SchemaCache.Parse call.
func TestNodeRefSchema_CacheCrossParse(t *testing.T) {
	var c SchemaCache
	if _, err := c.Parse(addrDef); err != nil {
		t.Fatal(err)
	}
	person, err := c.Parse(`{"type":"record","name":"com.example.Person","fields":[
		{"name":"home","type":"com.example.Address"},
		{"name":"work","type":"com.example.Address"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	root := person.Root()

	// Control: the first occurrence carries the spliced definition and
	// already works today.
	requireSubSchema(t, &root.Fields[0].Type, addrDef, addrVal)

	// The regression: the second occurrence is a bare reference.
	requireSubSchema(t, &root.Fields[1].Type, addrDef, addrVal)
}

// Case 2: same failure with no cache — second occurrence within one schema.
func TestNodeRefSchema_SecondOccurrence(t *testing.T) {
	s := MustParse(`{"type":"record","name":"P","fields":[
		{"name":"a","type":` + addrDef + `},
		{"name":"b","type":"com.example.Address"}]}`)
	root := s.Root()
	requireSubSchema(t, &root.Fields[1].Type, addrDef, addrVal)
}

// Case 3: recursive type — a union branch references its own container.
// Extracting the union must yield ["null", <full Node definition>], and
// extracting the branch must yield the Node definition itself.
func TestNodeRefSchema_Recursive(t *testing.T) {
	const nodeDef = `{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]}]}`
	s := MustParse(nodeDef)
	root := s.Root()
	val := map[string]any{"next": nil}

	union := &root.Fields[0].Type
	requireSubSchema(t, union, `["null",`+nodeDef+`]`, map[string]any{"null": nil})
	requireSubSchema(t, &union.Branches[1], nodeDef, val)
}

// Forward reference control: a reference appearing BEFORE its local
// definition parses today via SchemaNode.Schema on the root, and must keep
// doing so. Extracting the forward-ref field's type must also work.
func TestNodeRefSchema_ForwardRefControl(t *testing.T) {
	const fixedDef = `{"type":"fixed","name":"F","size":4}`
	s := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"F"},
		{"name":"b","type":` + fixedDef + `}]}`)
	root := s.Root()

	// Whole-root round trip (works today; must not regress).
	rs, err := root.Schema()
	if err != nil {
		t.Fatalf("root.Schema() with forward ref: %v", err)
	}
	if !bytes.Equal(rs.Canonical(), s.Canonical()) {
		t.Fatalf("root canonical drifted:\n got: %s\nwant: %s", rs.Canonical(), s.Canonical())
	}

	// The forward-ref field node itself.
	requireSubSchema(t, &root.Fields[0].Type, fixedDef, []byte{1, 2, 3, 4})
}

// Hand-built nodes have no enclosing schema; a dangling reference must
// keep failing loudly rather than resolving against anything.
func TestNodeRefSchema_HandBuiltStillDangles(t *testing.T) {
	n := SchemaNode{Type: "com.example.Address"}
	if _, err := n.Schema(); err == nil {
		t.Fatal("hand-built dangling reference unexpectedly parsed")
	}
}

// TestNodeRefSchemaMatrix is the class-elimination net for reference-node
// extraction: kind × namespace spelling × extraction site × structure.
// Each cell builds an enclosing schema whose extraction site holds a NAME
// REFERENCE, extracts the site node from Root(), converts it with
// SchemaNode.Schema, and compares canonical bytes + wire bytes against a
// TWIN whose extraction site holds the definition INLINE — the
// long-standing pre-splice path — then re-parses the result's String()
// to pin self-containment. Neutering the refTarget splice in toJSONWalk
// must fail every cell here (verified when the net was built).
func TestNodeRefSchemaMatrix(t *testing.T) {
	kinds := []struct {
		name string
		def  func(name, nsAttr string) string // named-type definition JSON
		val  any                              // sample value encodable by it
	}{
		{"record", func(n, ns string) string {
			return `{"type":"record","name":"` + n + `"` + ns + `,"fields":[{"name":"s","type":"string"}]}`
		}, map[string]any{"s": "x"}},
		{"enum", func(n, ns string) string {
			return `{"type":"enum","name":"` + n + `"` + ns + `,"symbols":["A","B"]}`
		}, "B"},
		{"fixed", func(n, ns string) string {
			return `{"type":"fixed","name":"` + n + `"` + ns + `,"size":4}`
		}, []byte{1, 2, 3, 4}},
	}

	// Namespace spellings for the def / ref pair and the enclosing root.
	nsCases := []struct {
		name   string
		defN   string // definition's "name"
		nsAttr string // definition's explicit namespace attribute, if any
		ref    string // reference spelling at the site
		rootNS string // enclosing root record's namespace attribute
		decoy  string // extra same-short-name type in another namespace
	}{
		{name: "dotted", defN: "x.y.N", ref: `"x.y.N"`},
		{name: "inherit", defN: "N", ref: `"N"`, rootNS: `,"namespace":"x.y"`},
		{name: "nullns", defN: "N", ref: `"N"`},
		{name: "nullescape", defN: "N", nsAttr: `,"namespace":""`, ref: `"N"`, rootNS: `,"namespace":"x.y"`},
		// Two types share the short name N: the in-scope x.y.N must win
		// the short-spelled reference over the null-namespace decoy.
		{name: "shadow", defN: "N", ref: `"N"`, rootNS: `,"namespace":"x.y"`,
			decoy: `{"name":"decoy","type":{"type":"fixed","name":"N","namespace":"","size":2}},`},
	}

	// Extraction sites: how the reference is embedded in the second field,
	// and the sample value for the site-shaped schema.
	sites := []struct {
		name string
		wrap func(ref string) string
		val  func(kindVal any) any
	}{
		{"field", func(r string) string { return r }, func(v any) any { return v }},
		{"array", func(r string) string { return `{"type":"array","items":` + r + `}` }, func(v any) any { return []any{v} }},
		{"map", func(r string) string { return `{"type":"map","values":` + r + `}` }, func(v any) any { return map[string]any{"k": v} }},
		{"union", func(r string) string { return `["null",` + r + `]` }, func(any) any { return map[string]any{"null": nil} }},
		{"nested", func(r string) string {
			return `{"type":"record","name":"Inner","fields":[{"name":"f","type":` + r + `}]}`
		}, func(v any) any { return map[string]any{"f": v} }},
	}

	build := func(rootNS, decoy, first, siteType string) string {
		return `{"type":"record","name":"Root"` + rootNS + `,"fields":[` + decoy +
			`{"name":"one","type":` + first + `},{"name":"two","type":` + siteType + `}]}`
	}
	// site node = the "two" field's type; the decoy, when present, shifts
	// the field index by one.
	extract := func(t *testing.T, s *Schema, hasDecoy bool) *SchemaNode {
		t.Helper()
		root := s.Root()
		i := 1
		if hasDecoy {
			i = 2
		}
		return &root.Fields[i].Type
	}

	check := func(t *testing.T, got, twin *Schema, val any) {
		t.Helper()
		if !bytes.Equal(got.Canonical(), twin.Canonical()) {
			t.Fatalf("canonical mismatch:\n got: %s\ntwin: %s", got.Canonical(), twin.Canonical())
		}
		ge, err := got.Encode(val)
		if err != nil {
			t.Fatalf("encode with extracted schema: %v", err)
		}
		te, err := twin.Encode(val)
		if err != nil {
			t.Fatalf("encode with twin schema: %v", err)
		}
		if !bytes.Equal(ge, te) {
			t.Fatalf("wire mismatch: got %x twin %x", ge, te)
		}
		// Self-containment: the result's own text re-parses to itself.
		rp, err := Parse(got.String())
		if err != nil {
			t.Fatalf("extracted schema text does not re-parse: %v\ntext: %s", err, got.String())
		}
		if !bytes.Equal(rp.Canonical(), got.Canonical()) {
			t.Fatalf("re-parse canonical drift:\n got: %s\nre:  %s", got.Canonical(), rp.Canonical())
		}
	}

	// Structure: second occurrence (def in field one, ref in field two).
	for _, k := range kinds {
		for _, ns := range nsCases {
			for _, site := range sites {
				t.Run("second/"+k.name+"/"+ns.name+"/"+site.name, func(t *testing.T) {
					def := k.def(ns.defN, ns.nsAttr)
					test := MustParse(build(ns.rootNS, ns.decoy, def, site.wrap(ns.ref)))
					twin := MustParse(build(ns.rootNS, ns.decoy, `"string"`, site.wrap(def)))
					got, err := extract(t, test, ns.decoy != "").Schema()
					if err != nil {
						t.Fatalf("Schema() on reference site: %v", err)
					}
					want, err := extract(t, twin, ns.decoy != "").Schema()
					if err != nil {
						t.Fatalf("Schema() on inline twin site: %v", err)
					}
					check(t, got, want, site.val(k.val))
				})
			}
		}
	}

	// Structure: cache cross-parse (definition from a prior Parse call).
	for _, k := range kinds {
		for _, nsName := range []string{"dotted", "nullns"} {
			t.Run("cache/"+k.name+"/"+nsName, func(t *testing.T) {
				defN := "N"
				if nsName == "dotted" {
					defN = "x.y.N"
				}
				def := k.def(defN, "")
				var c SchemaCache
				if _, err := c.Parse(def); err != nil {
					t.Fatal(err)
				}
				enclosing, err := c.Parse(build("", "", defN2ref(defN), defN2ref(defN)))
				if err != nil {
					t.Fatal(err)
				}
				got, err := extract(t, enclosing, false).Schema()
				if err != nil {
					t.Fatalf("Schema() on cache-referenced site: %v", err)
				}
				check(t, got, MustParse(def), k.val)
			})
		}
	}

	// Structure: diamond — C references D whose definition lives inside B.
	for _, k := range kinds {
		t.Run("diamond/"+k.name, func(t *testing.T) {
			dDef := k.def("x.y.D", "")
			s := MustParse(`{"type":"record","name":"Root","fields":[
				{"name":"b","type":{"type":"record","name":"B","fields":[{"name":"d","type":` + dDef + `}]}},
				{"name":"c","type":{"type":"record","name":"C","fields":[{"name":"d","type":"x.y.D"}]}}]}`)
			root := s.Root()
			got, err := root.Fields[1].Type.Schema()
			if err != nil {
				t.Fatalf("Schema() on diamond arm: %v", err)
			}
			twin := MustParse(`{"type":"record","name":"C","fields":[{"name":"d","type":` + dDef + `}]}`)
			check(t, got, twin, map[string]any{"d": k.val})
		})
	}

	// Structure: forward reference — extraction of the ref node resolves
	// to the definition appearing later in the enclosing schema.
	for _, k := range kinds {
		t.Run("forward/"+k.name, func(t *testing.T) {
			def := k.def("N", "")
			s := MustParse(build("", "", `"N"`, def))
			root := s.Root()
			got, err := root.Fields[0].Type.Schema()
			if err != nil {
				t.Fatalf("Schema() on forward reference: %v", err)
			}
			check(t, got, MustParse(def), k.val)
		})
	}

	// Structure: recursive, dotted and inherited namespace spellings.
	for _, ns := range []struct{ name, def, ref string }{
		{"dotted", "x.y.N", "x.y.N"},
		{"inherit", "N", "N"},
	} {
		t.Run("recursive/"+ns.name, func(t *testing.T) {
			nsAttr := ""
			if ns.name == "inherit" {
				nsAttr = `,"namespace":"x.y"`
			}
			def := `{"type":"record","name":"` + ns.def + `"` + nsAttr + `,"fields":[{"name":"next","type":["null","` + ns.ref + `"]}]}`
			s := MustParse(def)
			root := s.Root()
			// The union branch node: its Schema() is the full recursive
			// definition, canonically equal to the enclosing schema itself.
			got, err := root.Fields[0].Type.Branches[1].Schema()
			if err != nil {
				t.Fatalf("Schema() on recursive branch: %v", err)
			}
			check(t, got, s, map[string]any{"next": nil})
		})
	}

	// Wrapped reference with custom properties: the props ride onto the
	// spliced definition (canonical is prop-blind, so the twin comparison
	// holds) and survive on the result's root node.
	t.Run("wrapper-props", func(t *testing.T) {
		def := kinds[0].def("x.y.N", "")
		test := MustParse(build("", "", def, `{"type":"x.y.N","my.prop":123}`))
		got, err := extract(t, test, false).Schema()
		if err != nil {
			t.Fatalf("Schema() on wrapped reference: %v", err)
		}
		check(t, got, MustParse(def), kinds[0].val)
		if p := got.Root().Props["my.prop"]; p != int64(123) {
			t.Fatalf("wrapper prop lost on splice: got %v (%T)", p, p)
		}
	})
}

// defN2ref quotes a fullname as a JSON reference token.
func defN2ref(n string) string { return `"` + n + `"` }
