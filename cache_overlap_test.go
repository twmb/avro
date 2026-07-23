package avro_test

import (
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// TestRegression_SchemaCacheOverlappingSpliceDefs pins self-containment when
// two cache-inherited references carry overlapping definitions. Each cached
// definition is stored self-contained (transitive definitions inlined), so a
// schema referencing two types that share a transitive type — the diamond
// A→{B,C}→D — or referencing a nested type and then its container would, with
// a naive splice, define the shared name twice. The splice must instead keep
// the first definition and rewrite later occurrences to name references,
// exactly as the parser's node tree shares one resolved type (and as Java's
// Schema toString emits via writeNameRef). The oracle is the logically
// identical inline-defined twin: identical wire bytes, identical canonical
// form and fingerprint, and re-parseable String()/Canonical()/Root().Schema().
func TestRegression_SchemaCacheOverlappingSpliceDefs(t *testing.T) {
	cases := []struct {
		name   string
		defs   []string // parsed into the cache first, in order
		ref    string   // the schema under test (cache-built)
		inline string   // logically-identical self-contained twin
		val    map[string]any
	}{
		{
			// Diamond with a shared record: D defined once (inside the first
			// reference's splice), referenced from the second.
			name: "diamond_record",
			defs: []string{
				`{"type":"record","name":"x.D","fields":[{"name":"n","type":"int"}]}`,
				`{"type":"record","name":"x.B","fields":[{"name":"d","type":"x.D"}]}`,
				`{"type":"record","name":"x.C","fields":[{"name":"d","type":"x.D"}]}`,
			},
			ref: `{"type":"record","name":"x.A","fields":[{"name":"b","type":"x.B"},{"name":"c","type":"x.C"}]}`,
			inline: `{"type":"record","name":"x.A","fields":[
				{"name":"b","type":{"type":"record","name":"x.B","fields":[
					{"name":"d","type":{"type":"record","name":"x.D","fields":[{"name":"n","type":"int"}]}}]}},
				{"name":"c","type":{"type":"record","name":"x.C","fields":[{"name":"d","type":"x.D"}]}}]}`,
			val: map[string]any{
				"b": map[string]any{"d": map[string]any{"n": 1}},
				"c": map[string]any{"d": map[string]any{"n": 2}},
			},
		},
		{
			// The mechanism is kind-agnostic: shared enum.
			name: "diamond_enum",
			defs: []string{
				`{"type":"enum","name":"x.D","symbols":["A","B"]}`,
				`{"type":"record","name":"x.B","fields":[{"name":"d","type":"x.D"}]}`,
				`{"type":"record","name":"x.C","fields":[{"name":"d","type":"x.D"}]}`,
			},
			ref: `{"type":"record","name":"x.A","fields":[{"name":"b","type":"x.B"},{"name":"c","type":"x.C"}]}`,
			inline: `{"type":"record","name":"x.A","fields":[
				{"name":"b","type":{"type":"record","name":"x.B","fields":[
					{"name":"d","type":{"type":"enum","name":"x.D","symbols":["A","B"]}}]}},
				{"name":"c","type":{"type":"record","name":"x.C","fields":[{"name":"d","type":"x.D"}]}}]}`,
			val: map[string]any{
				"b": map[string]any{"d": "A"},
				"c": map[string]any{"d": "B"},
			},
		},
		{
			// Shared fixed.
			name: "diamond_fixed",
			defs: []string{
				`{"type":"fixed","name":"x.D","size":2}`,
				`{"type":"record","name":"x.B","fields":[{"name":"d","type":"x.D"}]}`,
				`{"type":"record","name":"x.C","fields":[{"name":"d","type":"x.D"}]}`,
			},
			ref: `{"type":"record","name":"x.A","fields":[{"name":"b","type":"x.B"},{"name":"c","type":"x.C"}]}`,
			inline: `{"type":"record","name":"x.A","fields":[
				{"name":"b","type":{"type":"record","name":"x.B","fields":[
					{"name":"d","type":{"type":"fixed","name":"x.D","size":2}}]}},
				{"name":"c","type":{"type":"record","name":"x.C","fields":[{"name":"d","type":"x.D"}]}}]}`,
			val: map[string]any{
				"b": map[string]any{"d": []byte("xy")},
				"c": map[string]any{"d": []byte("zw")},
			},
		},
		{
			// Null-namespace diamond: the duplicate's rewrite must emit a bare
			// short-name reference (the only spelling a null-namespace type has).
			name: "diamond_null_namespace",
			defs: []string{
				`{"type":"record","name":"D","fields":[{"name":"n","type":"int"}]}`,
				`{"type":"record","name":"B","fields":[{"name":"d","type":"D"}]}`,
				`{"type":"record","name":"C","fields":[{"name":"d","type":"D"}]}`,
			},
			ref: `{"type":"record","name":"A","fields":[{"name":"b","type":"B"},{"name":"c","type":"C"}]}`,
			inline: `{"type":"record","name":"A","fields":[
				{"name":"b","type":{"type":"record","name":"B","fields":[
					{"name":"d","type":{"type":"record","name":"D","fields":[{"name":"n","type":"int"}]}}]}},
				{"name":"c","type":{"type":"record","name":"C","fields":[{"name":"d","type":"D"}]}}]}`,
			val: map[string]any{
				"b": map[string]any{"d": map[string]any{"n": 1}},
				"c": map[string]any{"d": map[string]any{"n": 2}},
			},
		},
		{
			// A nested type referenced BEFORE the container whose definition
			// carries it: the standalone splice lands first, so the copy inside
			// the container's splice is the duplicate.
			name: "nested_ref_before_container",
			defs: []string{
				`{"type":"record","name":"x.Outer","fields":[{"name":"i","type":{"type":"record","name":"Inner","fields":[{"name":"n","type":"int"}]}}]}`,
			},
			ref: `{"type":"record","name":"x.Wrap","fields":[{"name":"f1","type":"x.Inner"},{"name":"f2","type":"x.Outer"}]}`,
			inline: `{"type":"record","name":"x.Wrap","fields":[
				{"name":"f1","type":{"type":"record","name":"x.Inner","fields":[{"name":"n","type":"int"}]}},
				{"name":"f2","type":{"type":"record","name":"x.Outer","fields":[{"name":"i","type":"x.Inner"}]}}]}`,
			val: map[string]any{
				"f1": map[string]any{"n": 1},
				"f2": map[string]any{"i": map[string]any{"n": 2}},
			},
		},
		{
			// Control (must keep working): container referenced first — the
			// nested definition arrives with it, and the later standalone
			// reference stays bare.
			name: "container_ref_before_nested",
			defs: []string{
				`{"type":"record","name":"x.Outer","fields":[{"name":"i","type":{"type":"record","name":"Inner","fields":[{"name":"n","type":"int"}]}}]}`,
			},
			ref: `{"type":"record","name":"x.Wrap","fields":[{"name":"f2","type":"x.Outer"},{"name":"f1","type":"x.Inner"}]}`,
			inline: `{"type":"record","name":"x.Wrap","fields":[
				{"name":"f2","type":{"type":"record","name":"x.Outer","fields":[
					{"name":"i","type":{"type":"record","name":"x.Inner","fields":[{"name":"n","type":"int"}]}}]}},
				{"name":"f1","type":"x.Inner"}]}`,
			val: map[string]any{
				"f1": map[string]any{"n": 1},
				"f2": map[string]any{"i": map[string]any{"n": 2}},
			},
		},
		{
			// A flat-form ("linkedin/goavro") field definition arriving as the
			// duplicate: the field must rewrite to normal form with a name
			// reference (a field object cannot be replaced by a bare string).
			name: "flat_form_duplicate",
			defs: []string{
				// H's flat field both defines x.B and registers it in the
				// cache (the def store captures the lifted form), so a later
				// schema can reference x.B standalone AND through x.H.
				`{"type":"record","name":"x.H","fields":[{"name":"B","type":"record","fields":[{"name":"v","type":"int"}]}]}`,
			},
			ref: `{"type":"record","name":"x.Wrap","fields":[{"name":"f1","type":"x.B"},{"name":"f2","type":"x.H"}]}`,
			inline: `{"type":"record","name":"x.Wrap","fields":[
				{"name":"f1","type":{"type":"record","name":"x.B","fields":[{"name":"v","type":"int"}]}},
				{"name":"f2","type":{"type":"record","name":"x.H","fields":[{"name":"B","type":"x.B"}]}}]}`,
			val: map[string]any{
				"f1": map[string]any{"v": 1},
				"f2": map[string]any{"B": map[string]any{"v": 2}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var c avro.SchemaCache
			for i, d := range tc.defs {
				if _, err := c.Parse(d); err != nil {
					t.Fatalf("parse def %d: %v", i, err)
				}
			}
			s, err := c.Parse(tc.ref)
			if err != nil {
				t.Fatalf("parse referencing schema: %v", err)
			}
			inline, err := avro.Parse(tc.inline)
			if err != nil {
				t.Fatalf("parse inline twin: %v", err)
			}
			assertSelfContained(t, s, inline, tc.val)
		})
	}
}

// TestRegression_SchemaCacheSpliceCascade pins that a self-contained schema
// built from overlapping splices is itself a usable cache definition: a later
// parse referencing it splices the (now coherent) definition and stays
// self-contained. Before the duplicate-definition rewrite, the diamond's
// failed rebuild recorded a DANGLING definition into the cache's def store,
// so the breakage cascaded into every downstream referencing schema.
func TestRegression_SchemaCacheSpliceCascade(t *testing.T) {
	var c avro.SchemaCache
	for i, d := range []string{
		`{"type":"record","name":"x.D","fields":[{"name":"n","type":"int"}]}`,
		`{"type":"record","name":"x.B","fields":[{"name":"d","type":"x.D"}]}`,
		`{"type":"record","name":"x.C","fields":[{"name":"d","type":"x.D"}]}`,
		`{"type":"record","name":"x.A","fields":[{"name":"b","type":"x.B"},{"name":"c","type":"x.C"}]}`,
	} {
		if _, err := c.Parse(d); err != nil {
			t.Fatalf("parse def %d: %v", i, err)
		}
	}
	e, err := c.Parse(`{"type":"record","name":"x.E","fields":[{"name":"a","type":"x.A"}]}`)
	if err != nil {
		t.Fatalf("parse E: %v", err)
	}
	inline, err := avro.Parse(`{"type":"record","name":"x.E","fields":[{"name":"a","type":
		{"type":"record","name":"x.A","fields":[
			{"name":"b","type":{"type":"record","name":"x.B","fields":[
				{"name":"d","type":{"type":"record","name":"x.D","fields":[{"name":"n","type":"int"}]}}]}},
			{"name":"c","type":{"type":"record","name":"x.C","fields":[{"name":"d","type":"x.D"}]}}]}}]}`)
	if err != nil {
		t.Fatalf("parse inline twin: %v", err)
	}
	assertSelfContained(t, e, inline, map[string]any{
		"a": map[string]any{
			"b": map[string]any{"d": map[string]any{"n": 1}},
			"c": map[string]any{"d": map[string]any{"n": 2}},
		},
	})
}

// TestRegression_SchemaCacheShortNameShadowNoMisbind pins that the duplicate-
// definition rewrite never emits a reference that would re-bind to a DIFFERENT
// type. A null-namespace type's only reference spelling is its bare short
// name, which the parser binds enclosing-namespace-first (scopedRefKeys) — so
// when a same-short-name namespaced type is defined earlier in the document,
// rewriting the null-namespace duplicate to a bare reference would silently
// re-bind it to the namespaced type, making the metadata forms describe a
// different schema than the wire codec. The rewrite must decline in exactly
// that case; the metadata forms may then stay non-self-contained (the format
// has no absolute-reference spelling for null-namespace names — Java has the
// same limitation), but they must never describe the wrong schema.
func TestRegression_SchemaCacheShortNameShadowNoMisbind(t *testing.T) {
	var c avro.SchemaCache
	for i, d := range []string{
		// Null-namespace D, referenced from namespaced carriers F and G
		// (legal at their parse time: x.D does not exist yet, so the bare
		// "D" falls through to the null-namespace type).
		`{"type":"record","name":"D","fields":[{"name":"n","type":"int"}]}`,
		`{"type":"record","name":"x.F","fields":[{"name":"d","type":"D"}]}`,
		`{"type":"record","name":"x.G","fields":[{"name":"d","type":"D"}]}`,
		// A DIFFERENT type that shadows D's short name inside namespace x.
		`{"type":"record","name":"x.D","fields":[{"name":"z","type":"string"}]}`,
	} {
		if _, err := c.Parse(d); err != nil {
			t.Fatalf("parse def %d: %v", i, err)
		}
	}
	// p splices x.D first (registering its name), then f and g each carry the
	// null-namespace D definition: the duplicate inside g's splice cannot be
	// rewritten to "D" (it would re-bind to x.D at that position).
	a, err := c.Parse(`{"type":"record","name":"x.A","fields":[
		{"name":"p","type":"x.D"},
		{"name":"f","type":"x.F"},
		{"name":"g","type":"x.G"}]}`)
	if err != nil {
		t.Fatalf("parse A: %v", err)
	}

	val := map[string]any{
		"p": map[string]any{"z": "s"},
		"f": map[string]any{"d": map[string]any{"n": 1}},
		"g": map[string]any{"d": map[string]any{"n": 2}},
	}
	wire, err := a.Encode(val)
	if err != nil {
		t.Fatalf("wire path must be unaffected: %v", err)
	}
	var decoded any
	if _, err := a.Decode(wire, &decoded); err != nil {
		t.Fatalf("wire decode: %v", err)
	}

	// The correct schema here has NO self-contained JSON spelling: the
	// null-namespace D inside g's subtree can only be written as a second
	// definition (duplicate, rejected) or as the bare reference "D" (which
	// re-binds to x.D at that position). The metadata forms are therefore
	// allowed to stay non-self-contained — but if they DO re-parse, they
	// must describe the same schema the wire codec implements: encoding the
	// same value must produce the same wire bytes. A rewrite that emitted
	// the bare "D" would re-parse successfully with g.d bound to x.D and
	// fail this value-level check.
	if reparsed, err := avro.Parse(a.String()); err == nil {
		wire2, err := reparsed.Encode(val)
		if err != nil {
			t.Errorf("String() re-parses but rejects a value the wire codec accepts (mis-bound short-name reference): %v", err)
		} else if string(wire2) != string(wire) {
			t.Errorf("String() re-parses but produces different wire bytes for the same value (mis-bound short-name reference)")
		}
	}
}

// TestRegression_SchemaCacheWrappedFormCrossParseRefSelfContains pins that a
// cross-parse reference spelled in the WRAPPED form {"type":"X"} self-contains
// in the metadata exactly like the bare-string form "X". {"type":"X"} is a
// documented-accepted name-reference spelling (including forward refs). The
// splice replaces the whole wrapped object with the referenced definition; the
// earlier bug recursed INTO the "type" value, producing the invalid
// {"type":{X-def}}, so the rebuild Parse failed and String()/Canonical() fell
// back to a dangling reference (wire codec worked, but Parse(s.String()) and
// the fingerprint surface did not). The oracle is the inline-defined twin:
// identical wire, identical canonical form + fingerprint, re-parseable
// String()/Canonical(). Crosses every nesting position the splice walks.
func TestRegression_SchemaCacheWrappedFormCrossParseRefSelfContains(t *testing.T) {
	const xDef = `{"type":"record","name":"X","fields":[{"name":"n","type":"int"}]}`
	cases := []struct {
		name    string
		wrapped string // Y referencing X via {"type":"X"}, cache-built after X
		inline  string // logically-identical twin with X defined inline
		val     map[string]any
	}{
		{
			name:    "field",
			wrapped: `{"type":"record","name":"Y","fields":[{"name":"f","type":{"type":"X"}}]}`,
			inline:  `{"type":"record","name":"Y","fields":[{"name":"f","type":` + xDef + `}]}`,
			val:     map[string]any{"f": map[string]any{"n": int32(5)}},
		},
		{
			name:    "union_branch",
			wrapped: `{"type":"record","name":"Y","fields":[{"name":"f","type":["null",{"type":"X"}]}]}`,
			inline:  `{"type":"record","name":"Y","fields":[{"name":"f","type":["null",` + xDef + `]}]}`,
			val:     map[string]any{"f": map[string]any{"n": int32(6)}},
		},
		{
			name:    "array_items",
			wrapped: `{"type":"record","name":"Y","fields":[{"name":"f","type":{"type":"array","items":{"type":"X"}}}]}`,
			inline:  `{"type":"record","name":"Y","fields":[{"name":"f","type":{"type":"array","items":` + xDef + `}}]}`,
			val:     map[string]any{"f": []any{map[string]any{"n": int32(7)}}},
		},
		{
			name:    "map_values",
			wrapped: `{"type":"record","name":"Y","fields":[{"name":"f","type":{"type":"map","values":{"type":"X"}}}]}`,
			inline:  `{"type":"record","name":"Y","fields":[{"name":"f","type":{"type":"map","values":` + xDef + `}}]}`,
			val:     map[string]any{"f": map[string]any{"k": map[string]any{"n": int32(8)}}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &avro.SchemaCache{}
			if _, err := c.Parse(xDef); err != nil {
				t.Fatalf("parse X into cache: %v", err)
			}
			y, err := c.Parse(tc.wrapped)
			if err != nil {
				t.Fatalf("parse wrapped Y: %v", err)
			}
			twin := avro.MustParse(tc.inline)

			// Wire codec already worked pre-fix; assert it still matches the twin.
			wire, err := y.Encode(tc.val)
			if err != nil {
				t.Fatalf("Y.Encode: %v", err)
			}
			twinWire, err := twin.Encode(tc.val)
			if err != nil {
				t.Fatalf("twin.Encode: %v", err)
			}
			if string(wire) != string(twinWire) {
				t.Errorf("wrapped-form wire != inline-twin wire")
			}

			// Self-containment: String()/Canonical() must re-parse and match the
			// twin's canonical form + fingerprint (the surfaces the bug broke).
			if _, err := avro.Parse(y.String()); err != nil {
				t.Errorf("Parse(Y.String()) failed (dangling metadata): %v\n  %s", err, y.String())
			}
			if _, err := avro.Parse(string(y.Canonical())); err != nil {
				t.Errorf("Parse(Y.Canonical()) failed (dangling metadata): %v\n  %s", err, y.Canonical())
			}
			// Canonical-form equality is the fingerprint surface (the Rabin/SHA
			// fingerprint is a hash of these bytes), so matching the inline twin
			// here pins the SOE / schema-registry interop the bug broke.
			if string(y.Canonical()) != string(twin.Canonical()) {
				t.Errorf("wrapped-form Canonical != inline-twin Canonical:\n got:  %s\n want: %s", y.Canonical(), twin.Canonical())
			}
		})
	}

	// Boundary-1 control: the bare-string form already self-contains; it must
	// stay correct (the fix must not regress it).
	t.Run("bare_form_control", func(t *testing.T) {
		c := &avro.SchemaCache{}
		if _, err := c.Parse(xDef); err != nil {
			t.Fatalf("parse X: %v", err)
		}
		y, err := c.Parse(`{"type":"record","name":"Y2","fields":[{"name":"f","type":"X"}]}`)
		if err != nil {
			t.Fatalf("parse bare Y2: %v", err)
		}
		if _, err := avro.Parse(y.String()); err != nil {
			t.Errorf("control: Parse(bare Y2.String()) must succeed: %v", err)
		}
	})

	// A type referencing the same cached type TWICE via the wrapped form. The
	// FIRST occurrence inlines X's definition; a LATER wrapped occurrence resolves
	// to an already-inlined type and so does not splice. Its wrapper must collapse
	// to the bare "X" the inline twin carries — otherwise {"type":"X"} survives in
	// String() where the canonical bare reference belongs (Canonical/PCF already
	// emits bare, so only String saw it). Single-reference cases above can never
	// reach this later-occurrence path.
	t.Run("repeated_ref_collapses_in_string", func(t *testing.T) {
		c := &avro.SchemaCache{}
		if _, err := c.Parse(xDef); err != nil {
			t.Fatalf("parse X: %v", err)
		}
		y, err := c.Parse(`{"type":"record","name":"Y3","fields":[{"name":"f1","type":{"type":"X"}},{"name":"f2","type":{"type":"X"}}]}`)
		if err != nil {
			t.Fatalf("parse wrapped Y3: %v", err)
		}
		twin := avro.MustParse(`{"type":"record","name":"Y3","fields":[{"name":"f1","type":` + xDef + `},{"name":"f2","type":"X"}]}`)

		// The surviving-wrapper signature is the VALUE {"type":"X"} (a wrapped
		// name reference). f1 inlines X's full record definition (whose "type" is
		// "record" and whose only "X" is "name":"X"), and f2's bare reference is
		// the string "X" — so {"type":"X"} appears nowhere unless a wrapper
		// survived the rebuild.
		if strings.Contains(y.String(), `{"type":"X"}`) {
			t.Errorf("String() kept a wrapped {\"type\":\"X\"} reference; the later occurrence must collapse to bare \"X\":\n  %s", y.String())
		}
		if !strings.Contains(y.String(), `"name":"X"`) {
			t.Errorf("String() lost X's inlined definition entirely:\n  %s", y.String())
		}
		// Wire (logical-identity anchor) and Canonical/fingerprint must match the
		// inline twin — they always did; String is the surface the wrapper broke.
		val := map[string]any{"f1": map[string]any{"n": int32(1)}, "f2": map[string]any{"n": int32(2)}}
		yw, err := y.Encode(val)
		if err != nil {
			t.Fatalf("Y3.Encode: %v", err)
		}
		tw, err := twin.Encode(val)
		if err != nil {
			t.Fatalf("twin.Encode: %v", err)
		}
		if string(yw) != string(tw) {
			t.Errorf("wrapped repeated-ref wire != inline-twin wire")
		}
		if string(y.Canonical()) != string(twin.Canonical()) {
			t.Errorf("Canonical diverges:\n got:  %s\n want: %s", y.Canonical(), twin.Canonical())
		}
		if _, err := avro.Parse(y.String()); err != nil {
			t.Errorf("Parse(Y3.String()) must succeed: %v\n  %s", err, y.String())
		}
	})
}

// TestMatrix_SpliceWrapperReservedKeyMerge drives the SchemaCache splice
// merge's reserved-key routing (the shared schemaReservedKeyForObject
// predicate at its cache call site) with wrapper props on cached
// definitions. A wrapper key the def's kind/logical CONSUMES never survives
// the splice (reserved usage-site attributes drop, matching Java's
// reference arms, which return the found schema with no properties pass);
// an UNCONSUMED key merges onto the definition as an ordinary custom
// property, definition-wins on collision. A non-string logicalType is
// unconsumed everywhere; precision/scale are consumed exactly on a decimal
// carrier def. The decimal def omits "scale" on purpose (spec default 0):
// a consumed wrapper "scale" must be dropped by the ROUTING, not masked by
// the def-wins presence check. fastavro rejects the props-carrying
// wrapped-reference spelling outright, so these cells have no differential
// arm; Java is the reference (usage-site extras drop at reference sites).
func TestMatrix_SpliceWrapperReservedKeyMerge(t *testing.T) {
	plainDef := `{"type":"fixed","name":"F","size":4}`
	decimalDef := `{"type":"fixed","name":"D","size":4,"logicalType":"decimal","precision":2}`

	cases := []struct {
		name    string
		def     string
		wrapper string
		check   func(t *testing.T, n avro.SchemaNode)
	}{
		{
			"nonstring-logicaltype-numeric-merges",
			plainDef,
			`{"type":"F","logicalType":123}`,
			func(t *testing.T, n avro.SchemaNode) {
				if got := n.Props["logicalType"]; got != int64(123) {
					t.Errorf("Props[logicalType] = %#v; want int64(123) merged as ordinary prop", got)
				}
				if n.LogicalType != "" {
					t.Errorf("non-string logicalType activated: %q", n.LogicalType)
				}
			},
		},
		{
			"nonstring-logicaltype-null-merges",
			plainDef,
			`{"type":"F","logicalType":null}`,
			func(t *testing.T, n avro.SchemaNode) {
				if got, ok := n.Props["logicalType"]; !ok || got != nil {
					t.Errorf("Props[logicalType] = %#v (present=%v); want JSON null merged as nil prop", got, ok)
				}
			},
		},
		{
			"string-logicaltype-consumed-drops",
			plainDef,
			`{"type":"F","logicalType":"decimal"}`,
			func(t *testing.T, n avro.SchemaNode) {
				if got, ok := n.Props["logicalType"]; ok {
					t.Errorf("consumed usage-site logicalType survived the splice as a prop: %#v", got)
				}
				if n.LogicalType != "" {
					t.Errorf("usage-site logicalType activated on the def: %q", n.LogicalType)
				}
			},
		},
		{
			"unconsumed-precision-valid-merges",
			plainDef,
			`{"type":"F","precision":3}`,
			func(t *testing.T, n avro.SchemaNode) {
				if got := n.Props["precision"]; got != int64(3) {
					t.Errorf("Props[precision] = %#v; want int64(3)", got)
				}
				if n.Precision != 0 {
					t.Errorf("unconsumed precision landed structurally: %d", n.Precision)
				}
			},
		},
		{
			"unconsumed-precision-malformed-merges-verbatim",
			plainDef,
			`{"type":"F","precision":"x"}`,
			func(t *testing.T, n avro.SchemaNode) {
				if got := n.Props["precision"]; got != "x" {
					t.Errorf("Props[precision] = %#v; want verbatim \"x\"", got)
				}
				if n.Precision != 0 {
					t.Errorf("malformed precision landed structurally: %d", n.Precision)
				}
			},
		},
		{
			"consumed-scale-malformed-drops",
			decimalDef,
			`{"type":"D","scale":"bogus"}`,
			func(t *testing.T, n avro.SchemaNode) {
				if got, ok := n.Props["scale"]; ok {
					t.Errorf("consumed usage-site scale survived the splice as a prop: %#v", got)
				}
				if n.Scale != 0 {
					t.Errorf("usage-site scale mutated the def: Scale = %d; want spec-default 0", n.Scale)
				}
			},
		},
		{
			"consumed-scale-valid-drops-def-wins",
			decimalDef,
			`{"type":"D","scale":1}`,
			func(t *testing.T, n avro.SchemaNode) {
				if got, ok := n.Props["scale"]; ok {
					t.Errorf("consumed usage-site scale survived the splice as a prop: %#v", got)
				}
				if n.Scale != 0 {
					t.Errorf("usage-site scale mutated the def: Scale = %d; want spec-default 0", n.Scale)
				}
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cache := &avro.SchemaCache{}
			if _, err := cache.Parse(c.def); err != nil {
				t.Fatalf("def Parse: %v", err)
			}
			s, err := cache.Parse(c.wrapper)
			if err != nil {
				t.Fatalf("wrapper Parse: %v", err)
			}
			n := s.Root()
			c.check(t, n)

			// Wrapper props are metadata: the wire image is the def's own
			// ([]byte is the opaque carrier for both plain and decimal
			// fixed).
			def := avro.MustParse(c.def)
			got, err := s.Encode([]byte{1, 2, 3, 4})
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			want, err := def.Encode([]byte{1, 2, 3, 4})
			if err != nil {
				t.Fatalf("def Encode: %v", err)
			}
			if string(got) != string(want) {
				t.Errorf("wrapper props changed the wire: %x vs %x", got, want)
			}

			// The spliced, self-contained metadata tree must rebuild to a
			// schema that reparses and keeps the same routing.
			rb, err := n.Schema()
			if err != nil {
				t.Fatalf("Root().Schema() rebuild: %v", err)
			}
			c.check(t, rb.Root())
		})
	}
}

// A cached definition whose RECORD field carries an unconsumed malformed
// precision splices through by-subtree: the field rides verbatim inside the
// inlined definition, the spliced tree rebuilds, and the pair stays on the
// field's Props — the splice merge touches only the WRAPPER's own keys,
// never field attributes inside the definition.
func TestRegression_SpliceDefFieldMalformedPrecisionRidesThrough(t *testing.T) {
	cache := &avro.SchemaCache{}
	if _, err := cache.Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":"int","precision":"x"}]}`); err != nil {
		t.Fatalf("def Parse: %v", err)
	}
	s, err := cache.Parse(`{"type":"R","myprop":1}`)
	if err != nil {
		t.Fatalf("wrapper Parse: %v", err)
	}
	n := s.Root()
	if got := n.Fields[0].Props["precision"]; got != "x" {
		t.Errorf("spliced def's field Props[precision] = %#v; want verbatim \"x\"", got)
	}
	if got := n.Props["myprop"]; got != int64(1) {
		t.Errorf("wrapper prop lost: %#v", got)
	}
	rb, err := n.Schema()
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if got := rb.Root().Fields[0].Props["precision"]; got != "x" {
		t.Errorf("rebuild field Props[precision] = %#v; want \"x\"", got)
	}
}
