package avro

import (
	"bytes"
	"reflect"
	"strings"
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

// The refTarget stamp is hidden state that survives a struct copy, which is
// exactly how a caller extracts a sub-node. If the caller then edits the
// node's exported Type, the stamp is STALE: it still points at whatever the
// ORIGINAL spelling named. Honoring it would let hidden state silently beat
// the exported field the caller just set, so the stamp is used only while it
// still names the node's Type. An edited node behaves hand-built — it
// resolves against the tree being converted, or dangles loudly.
func TestNodeRefSchema_EditedTypeIgnoresStaleStamp(t *testing.T) {
	const twoNamed = `{"type":"record","name":"R","fields":[
		{"name":"f","type":{"type":"fixed","name":"Dec","size":8}},
		{"name":"g","type":"Dec"}]}`

	t.Run("retyped-to-primitive", func(t *testing.T) {
		root := MustParse(twoNamed).Root()
		g := root.Fields[1].Type // struct copy: carries the stamp
		if g.Type != "Dec" {
			t.Fatalf("precondition: extracted node Type = %q", g.Type)
		}
		g.Type = "int"
		sub, err := g.Schema()
		if err != nil {
			t.Fatalf("Schema() after retyping to a primitive: %v", err)
		}
		if got := string(sub.Canonical()); got != `"int"` {
			t.Fatalf("retyped node produced %s, want \"int\" — the stale stamp resurrected the old definition", got)
		}
		// And it must actually encode as an int.
		wire, err := sub.Encode(int32(1))
		if err != nil {
			t.Fatalf("encode int: %v", err)
		}
		if !bytes.Equal(wire, []byte{2}) {
			t.Fatalf("int wire = %v, want [2]", wire)
		}
	})

	t.Run("redirected-to-another-name", func(t *testing.T) {
		root := MustParse(`{"type":"record","name":"R","fields":[
			{"name":"a","type":{"type":"fixed","name":"A","size":1}},
			{"name":"b","type":{"type":"fixed","name":"B","size":2}},
			{"name":"c","type":"A"}]}`).Root()
		c := root.Fields[2].Type
		c.Type = "B" // the caller redirects the reference
		// The tree being converted defines neither name, so an as-written
		// reference dangles — the hand-built posture. What must NOT happen
		// is silently emitting A's definition under the new spelling.
		sub, err := c.Schema()
		if err == nil {
			t.Fatalf("redirected reference produced %s; it names B, which this tree does not define, so it must dangle loudly like a hand-built node", sub.Canonical())
		}
	})

	t.Run("unedited-still-splices", func(t *testing.T) {
		// The control: the whole point of the stamp must still work.
		root := MustParse(twoNamed).Root()
		requireSubSchema(t, &root.Fields[1].Type, `{"type":"fixed","name":"Dec","size":8}`, []byte{1, 2, 3, 4, 5, 6, 7, 8})
	})

	t.Run("retyped-to-a-name-the-tree-defines", func(t *testing.T) {
		// Editing Type to a name the CONVERTED TREE defines must bind to
		// that local definition, not to the stamp.
		root := MustParse(twoNamed).Root()
		rec := root // the whole record defines "Dec" locally
		rec.Fields[1].Type.Type = "Dec"
		sub, err := rec.Schema()
		if err != nil {
			t.Fatalf("Schema(): %v", err)
		}
		if !bytes.Equal(sub.Canonical(), MustParse(twoNamed).Canonical()) {
			t.Fatalf("locally-defined name drifted: %s", sub.Canonical())
		}
	})
}

// Whether an extracted reference node still names its stamped target is a
// question the name resolver already owns: scopedRefKeys decides which
// spellings bind, and it admits three — a fullname, a short name qualified
// by the enclosing namespace, and the leading-dot null-namespace escape —
// resolving the short form against the target's RESOLVED FULLNAME, which is
// not the same string as its Name field when the definition writes its name
// dotted. Every spelling the resolver binds must therefore convert; a guard
// that re-lists the accepted spellings by hand instead of asking the
// resolver under-accepts, and the node it wrongly calls stale emits a
// dangling reference that fails its own re-parse.
//
// The scope the question is asked at is the scope the reference was WRITTEN
// in, not the converted tree's: extraction re-roots the node at the null
// namespace, so a short-name reference lifted out of its namespace is still
// the same unedited reference.
func TestNodeRefSchema_EverySpellingTheResolverBindsConverts(t *testing.T) {
	fooVal := map[string]any{"x": int32(1)}

	for _, tc := range []struct {
		name    string
		enclose string // enclosing schema; field "b" holds the reference
		want    string // equivalent standalone definition
	}{
		{
			name: "fullname",
			enclose: `{"type":"record","name":"ns.Top","fields":[
				{"name":"a","type":{"type":"record","name":"Foo","namespace":"ns","fields":[{"name":"x","type":"int"}]}},
				{"name":"b","type":"ns.Foo"}]}`,
			want: `{"type":"record","name":"Foo","namespace":"ns","fields":[{"name":"x","type":"int"}]}`,
		},
		{
			name: "short-name-with-namespace-attribute",
			enclose: `{"type":"record","name":"ns.Top","fields":[
				{"name":"a","type":{"type":"record","name":"Foo","namespace":"ns","fields":[{"name":"x","type":"int"}]}},
				{"name":"b","type":"Foo"}]}`,
			want: `{"type":"record","name":"Foo","namespace":"ns","fields":[{"name":"x","type":"int"}]}`,
		},
		{
			// The definition's name is written as a dotted fullname, so its
			// Name field holds "ns.Foo" while the reference spells "Foo".
			// Comparing the reference against the Name field misses this;
			// comparing against the resolved fullname, as the resolver does,
			// binds it.
			name: "short-name-against-a-dotted-definition-name",
			enclose: `{"type":"record","name":"ns.Top","fields":[
				{"name":"a","type":{"type":"record","name":"ns.Foo","fields":[{"name":"x","type":"int"}]}},
				{"name":"b","type":"Foo"}]}`,
			want: `{"type":"record","name":"ns.Foo","fields":[{"name":"x","type":"int"}]}`,
		},
		{
			// ".Foo" is the explicit null-namespace escape: an exact lookup
			// of the null-namespace fullname, never qualified into the
			// enclosing namespace.
			name: "leading-dot-null-namespace-escape",
			enclose: `{"type":"record","name":"ns.Top","fields":[
				{"name":"a","type":{"type":"record","name":"Foo","namespace":"","fields":[{"name":"x","type":"int"}]}},
				{"name":"b","type":".Foo"}]}`,
			want: `{"type":"record","name":"Foo","fields":[{"name":"x","type":"int"}]}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := MustParse(tc.enclose).Root()
			requireSubSchema(t, &root.Fields[1].Type, tc.want, fooVal)
		})
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

// schemaNodeFieldRule classifies one exported SchemaNode field for the two
// invariants below. The zero rule is the ordinary case: the field BLOCKS the
// shortcut, and its value survives on the same field after a round trip.
//
// Losslessness is a CONJUNCTION, and a guard that proves only the blocking
// half proves only half of it. The shortcut must decline to collapse a node
// that carries content, AND the longer form it falls through to must actually
// emit that content. EnumDefault is why both halves are checked: it blocked
// the collapse, the emitter keyed "default" off HasEnumDefault rather than
// off it, and the value was dropped exactly as before with only the render
// changed.
type schemaNodeFieldRule struct {
	// exempt, when non-empty, is the reason this field has NO emitted form
	// at this site, so taking the shortcut cannot lose it. An exempt field
	// must NOT block — an exemption that blocks is a contradiction, and the
	// test says which half is wrong.
	exempt string
	// propsKey, when non-empty, names the JSON key the emission arm writes
	// and the Props entry the value comes back under, because the carrier
	// kind does not BIND that key. The value is preserved as inert metadata
	// on its only surface, not lost; the field itself reads back zero.
	propsKey string
	// droppedKey, when non-empty, names a JSON key the emission arm writes
	// that the RE-PARSE then drops: it is a reserved name, so it never
	// enters Props, and the carrier kind has no structural field to capture
	// it into. The value IS lost, and the loss is policy rather than an
	// oversight, so the rule quotes the policy and the test asserts the drop
	// still happens — the day the policy changes, this cell reds and has to
	// be reclassified rather than silently passing.
	droppedKey string
	why        string
}

// bareEmissionFieldRules classifies every exported SchemaNode field whose
// treatment under nodeCarriesOnlyType is not the ordinary
// blocks-and-round-trips case. Everything absent from this map must block the
// collapse AND come back on its own field.
var bareEmissionFieldRules = map[string]schemaNodeFieldRule{
	"Branches":    {exempt: "no JSON key routes to Branches outside a union — the union arm returns before the collapse is reached — so a hand-built value on another kind is inert"},
	"EnumDefault": {exempt: "HasEnumDefault is the carrier the \"default\" key is emitted from; with the carrier false the node declares no default, so there is nothing to emit and nothing to lose"},
	"HasEnumDefault": {
		droppedKey: "default",
		why: "\"default\" is a reserved name, so it never enters Props, and only an enum has a structural field to capture it into. " +
			"On every other carrier the reserved-name-capture rule drops it from the metadata tree — the same treatment \"order\" gets " +
			"on every kind, pinned across the kind axis by TestMatrix_AttributePlacementCensus and by " +
			"TestRegression_EnumRefWrapperDefaultInert for the reference-wrapper spelling. Setting this field on a non-enum node is " +
			"therefore lossy, and changing that is a routing-policy decision, not a fix to make here",
	},
	"Precision": {propsKey: "precision"},
	"Scale":     {propsKey: "scale"},
}

// A schema node collapses to its bare type name only when it carries nothing
// else. That question used to be answered by two hand-written lists of the
// fields someone remembered, and both were missing the same members — a
// stray-surfaced Symbols, Size, Aliases or Name on a primitive survived
// String() and Root() and vanished through Root().Schema().
//
// The durable fix is not "add the missing ones": it is that the enumeration
// must check ITSELF. This sets every exported field of SchemaNode in turn and
// requires BOTH halves of losslessness — nodeCarriesOnlyType declines to
// collapse, and the value then survives an emit → re-parse round trip, read
// back off the metadata FIELDS rather than off the rendered text (key order
// alone makes a text comparison report losses that did not happen). A field
// added later fails here until someone classifies it.
func TestInvariant_BareEmissionCoversEverySchemaNodeField(t *testing.T) {
	base := SchemaNode{Type: "int"}
	if !nodeCarriesOnlyType(&base) {
		t.Fatal("a bare primitive must carry only its Type; the control is broken so nothing below means anything")
	}
	// Branches is exempt only OFF a union. On a union it carries the whole
	// schema, so the exemption must not leak there.
	if u := (SchemaNode{Type: "union", Branches: []SchemaNode{{Type: "null"}}}); nodeCarriesOnlyType(&u) {
		t.Error("Branches is exempt only outside a union; on a union it carries the branch list and must block")
	}

	rt := reflect.TypeFor[SchemaNode]()
	var checked, exempted, relocated, dropped int
	for i := range rt.NumField() {
		f := rt.Field(i)
		if !f.IsExported() || f.Name == "Type" {
			continue
		}
		rule := bareEmissionFieldRules[f.Name]
		n := SchemaNode{Type: "int"}
		fv := reflect.ValueOf(&n).Elem().Field(i)
		if !setNonZeroForTest(f.Name, fv) {
			t.Errorf("field %s has kind %s, which this test does not know how to populate — teach it, or the field is silently unchecked", f.Name, f.Type.Kind())
			continue
		}
		want := fv.Interface()

		if blocks := !nodeCarriesOnlyType(&n); rule.exempt != "" {
			exempted++
			if blocks {
				t.Errorf("field %s is classified exempt (%s) but blocks bare emission; either the exemption is wrong or the field gained an emitted form", f.Name, rule.exempt)
			}
			continue
		} else if !blocks {
			t.Errorf("setting %s does NOT block bare emission, so its value would be silently dropped by Root().Schema(). Give it an emission arm, or classify it exempt with the reason it cannot be emitted.", f.Name)
			continue
		}
		checked++

		// The other half: the object form it fell through to must carry the
		// value somewhere a reader can find it.
		s, err := n.Schema()
		if err != nil {
			t.Errorf("field %s: blocking is not enough — the emission failed outright: %v", f.Name, err)
			continue
		}
		back := s.Root()
		switch {
		case rule.propsKey != "":
			relocated++
			if _, ok := back.Props[rule.propsKey]; !ok {
				t.Errorf("field %s emits %q on a carrier that does not bind it, so the value must ride to Props as its only surface; Props came back %v from %s",
					f.Name, rule.propsKey, back.Props, s)
			}
		case rule.droppedKey != "":
			dropped++
			// Both halves of the classification are checked. The emission
			// arm must WRITE the key (otherwise the loss is the emitter's,
			// not the routing's, and this rule is the wrong diagnosis)...
			if !strings.Contains(s.String(), `"`+rule.droppedKey+`"`) {
				t.Errorf("field %s is classified as dropped by the reserved-name routing of %q, but the emission never wrote that key at all — the loss is in the emitter, so give it an emission arm: %s",
					f.Name, rule.droppedKey, s)
			}
			// ...and the re-parse must still drop it. If this stops
			// failing, the routing policy changed and the classification
			// has to be revisited, not silently kept.
			if got := reflect.ValueOf(back).Field(i).Interface(); !reflect.DeepEqual(reflect.Zero(f.Type).Interface(), got) {
				t.Errorf("field %s now SURVIVES the round trip (%#v), so the documented drop no longer happens: reclassify it as an ordinary round-tripping field. Rule quoted: %s",
					f.Name, got, rule.why)
			}
			if _, ok := back.Props[rule.droppedKey]; ok {
				t.Errorf("field %s: %q now reaches Props, so the reserved-name-capture rule changed; reclassify with propsKey. Rule quoted: %s",
					f.Name, rule.droppedKey, rule.why)
			}
		default:
			if got := reflect.ValueOf(back).Field(i).Interface(); !reflect.DeepEqual(want, got) {
				t.Errorf("field %s blocks the collapse but does not survive the rebuild: set %#v, emitted %s, read back %#v. The value is dropped with only the render changed — give it an emission arm, classify where it relocates, or classify it exempt.",
					f.Name, want, s, got)
			}
		}
		// Wherever the value landed, emission must be a FIXPOINT from there:
		// a second pass that drops it would mean the first round trip only
		// postponed the loss. The one classification exempt from this is the
		// dropped key, whose whole content is that the loss happens on the
		// FIRST re-parse; there the second pass must instead land exactly on
		// the untouched control, proving the drop is total rather than
		// leaving a half-emitted residue.
		s2, err := back.Schema()
		switch {
		case err != nil:
			t.Errorf("field %s: re-emitting the rebuilt node failed: %v", f.Name, err)
		case rule.droppedKey != "":
			ctrl, err := base.Schema()
			if err != nil {
				t.Fatalf("the untouched control must emit: %v", err)
			}
			if s2.String() != ctrl.String() {
				t.Errorf("field %s: the drop left a residue — second pass %s, untouched control %s", f.Name, s2, ctrl)
			}
		case s2.String() != s.String():
			t.Errorf("field %s: emission is not a fixpoint, so something is lost on the second pass:\n first %s\nsecond %s", f.Name, s, s2)
		}
	}
	if checked < 12 {
		t.Fatalf("only %d fields were actually checked; the walk is not seeing SchemaNode", checked)
	}
	t.Logf("bare-emission coverage: %d fields block, of which %d round-trip on their own field, %d relocate to Props and %d are dropped by the reserved-name routing; %d classified exempt", checked, checked-relocated-dropped, relocated, dropped, exempted)
}

// nameRefSpliceFieldRules classifies every exported SchemaNode field whose
// treatment under nodeIsNameRefShape is not the ordinary blocking case. The
// exemptions are the reserved USAGE-SITE attributes a splice is already
// adjudicated to drop, plus the custom properties it merges; deriving the
// predicate without exactly these would turn an adjudicated silent drop into
// a hard "unknown complex type" error on the extraction feature.
var nameRefSpliceFieldRules = map[string]schemaNodeFieldRule{
	"Doc":         {exempt: "a definition cannot carry a second doc for one of its usage sites, so the splice drops it"},
	"Aliases":     {exempt: "usage-site aliases have no place on the spliced definition"},
	"Namespace":   {exempt: "a definition cannot carry a second namespace for one of its usage sites"},
	"LogicalType": {exempt: "a usage-site logicalType annotates the reference, not the definition it names"},
	"Props":       {exempt: "the wrapper's custom properties MERGE onto the spliced definition, definition-wins, rather than being discarded"},
}

// The sibling invariant, for the other predicate on the same walk. A stamped
// name-reference node splices the definition it names in place of itself, so
// every field the node carries that the splice does not merge is DISCARDED.
// nodeIsNameRefShape is what decides whether that is allowed, and it too used
// to be a hand-written list — of eight fields, silently discarding the seven
// it did not name.
//
// The probe has to REACH the splice: the stamp must be present (extraction
// from Root, not a hand-built node), Type must be left alone (an edited Type
// makes the stamp stale, a different question), and the extracted sub-tree
// must not define the name locally, since a whole-schema walk never splices.
func TestInvariant_NameRefSpliceCoversEverySchemaNodeField(t *testing.T) {
	const src = `{"type":"record","name":"Root","namespace":"x.y","fields":[
		{"name":"a","type":{"type":"record","name":"Inner","fields":[{"name":"q","type":"int"}]}},
		{"name":"b","type":"x.y.Inner"}]}`
	base := MustParse(src)
	extract := func() SchemaNode { return base.Root().Fields[1].Type }

	unedited := extract()
	control, err := unedited.Schema()
	if err != nil {
		t.Fatalf("the unedited extraction must splice; the control is broken so nothing below means anything: %v", err)
	}
	if !strings.Contains(control.String(), `"fields"`) {
		t.Fatalf("control did not splice the definition: %s", control)
	}

	rt := reflect.TypeFor[SchemaNode]()
	var blocked, exempted int
	for i := range rt.NumField() {
		f := rt.Field(i)
		if !f.IsExported() || f.Name == "Type" {
			continue
		}
		rule := nameRefSpliceFieldRules[f.Name]
		n := extract()
		fv := reflect.ValueOf(&n).Elem().Field(i)
		if !setNonZeroForTest(f.Name, fv) {
			t.Errorf("field %s has kind %s, which this test does not know how to populate — teach it, or the field is silently unchecked", f.Name, f.Type.Kind())
			continue
		}
		splices := nodeIsNameRefShape(&n)
		if rule.exempt != "" {
			exempted++
			if !splices {
				t.Errorf("field %s is classified exempt (%s) but blocks the splice; blocking it converts an adjudicated usage-site drop into a hard parse error", f.Name, rule.exempt)
				continue
			}
			// The exemption's own claim, executed: the splice still happens.
			if s, err := n.Schema(); err != nil {
				t.Errorf("field %s is exempt, so the reference must still splice; it errored instead: %v", f.Name, err)
			} else if !strings.Contains(s.String(), `"fields"`) {
				t.Errorf("field %s is exempt, so the reference must still splice the definition; got %s", f.Name, s)
			}
			continue
		}
		blocked++
		if splices {
			t.Errorf("setting %s still lets the node splice, so its value is silently discarded in favor of the definition. Give it a place on the spliced form, or classify it exempt with the reason its loss is adjudicated.", f.Name)
			continue
		}
		// Blocking means rendering as-written. The re-parse must then JUDGE
		// the hybrid — a named error is the honest outcome, and a silent
		// success that dropped the field is the outcome this rules out.
		s, err := n.Schema()
		if err != nil {
			continue // loud, which is the contract
		}
		if got := reflect.ValueOf(s.Root()).Field(i).Interface(); !reflect.DeepEqual(fv.Interface(), got) {
			t.Errorf("field %s blocks the splice but the as-written render still lost it: set %#v, emitted %s, read back %#v",
				f.Name, fv.Interface(), s, got)
		}
	}
	if blocked < 8 {
		t.Fatalf("only %d fields were required to block; the walk is not seeing SchemaNode", blocked)
	}
	t.Logf("name-reference splice coverage: %d fields must block, %d classified exempt as usage-site attributes", blocked, exempted)
}

// setNonZeroForTest gives fv a non-zero, SCHEMA-VALID value, reporting false
// for kinds it does not handle so an unhandled kind is a loud failure rather
// than a silently skipped field. The values must be schema-valid because both
// invariants above emit the node and re-parse it: a zero SchemaNode child has
// Type "" and could never parse, which would report every container field as
// an emission failure rather than as the round trip it is meant to measure.
func setNonZeroForTest(name string, fv reflect.Value) bool {
	switch name {
	case "Items", "Values":
		fv.Set(reflect.ValueOf(&SchemaNode{Type: "int"}))
		return true
	case "Fields":
		fv.Set(reflect.ValueOf([]SchemaField{{Name: "f", Type: SchemaNode{Type: "int"}}}))
		return true
	case "Branches":
		fv.Set(reflect.ValueOf([]SchemaNode{{Type: "null"}, {Type: "int"}}))
		return true
	case "Symbols", "Aliases":
		fv.Set(reflect.ValueOf([]string{"A"}))
		return true
	case "Props":
		fv.Set(reflect.ValueOf(map[string]any{"my.p": "v"}))
		return true
	}
	switch fv.Kind() {
	case reflect.String:
		fv.SetString("x")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		fv.SetInt(1)
	case reflect.Bool:
		fv.SetBool(true)
	default:
		return false
	}
	return true
}
