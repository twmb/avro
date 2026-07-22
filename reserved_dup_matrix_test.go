package avro_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// Reserved keys are matched case-insensitively, and two case-variant
// spellings of one reserved key ("items" and "ITEMS") are distinct JSON
// keys the parser accepts. The routing invariant: exactly ONE spelling of
// each reserved key — the exact-case-preferred, else lexicographically
// smallest case-insensitive pick, the same selection every reader uses —
// is consulted for structural binding and consumed; EVERY other raw map
// key that is not itself a consumed pick, including case-variants of
// reserved keys and unpicked valid duplicates, rides to Props verbatim.
// Props == all raw keys minus the picked reserved keys consumed into
// structural fields — deterministic, with no per-shape branching on the
// unpicked spelling's body. Both reading surfaces (the parse-side props
// handed to CustomType callbacks, and the Root() metadata walk) apply the
// same rule and must agree; the Root().Schema() rebuild preserves the
// preserved keys. Java treats reserved keys case-sensitively (the
// SCHEMA_RESERVED skip set is exact-lowercase, Schema.java:175-176), so a
// case-variant spelling is an ordinary preserved property there; fastavro
// likewise reads known keys by exact name and preserves the rest.

// propsCaptureCustom returns a match-all CustomType whose Encode records
// the Props of the node matching typ (+ name, when non-empty) and then
// falls through to the built-in encoder, plus the capture destination.
func propsCaptureCustom(typ, name string, dst *map[string]any) avro.CustomType {
	return avro.CustomType{
		Encode: func(v any, schema *avro.SchemaNode) (any, error) {
			// The callback node's Name may be the short name or the
			// namespace-qualified fullname; match either.
			if schema.Type == typ && (name == "" || schema.Name == name || strings.HasSuffix(schema.Name, "."+name)) {
				*dst = schema.Props
			}
			return nil, avro.ErrSkipCustomType
		},
	}
}

// propsValue parses a JSON body the way schema props are surfaced:
// integral numbers as int64, other numbers as float64, containers
// recursively.
func propsValue(t *testing.T, body string) any {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(body))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("bad body %q: %v", body, err)
	}
	var norm func(any) any
	norm = func(v any) any {
		switch x := v.(type) {
		case json.Number:
			if n, err := x.Int64(); err == nil {
				return n
			}
			f, _ := x.Float64()
			return f
		case []any:
			for i := range x {
				x[i] = norm(x[i])
			}
			return x
		case map[string]any:
			for k := range x {
				x[k] = norm(x[k])
			}
			return x
		}
		return v
	}
	return norm(v)
}

type reservedDupCarrier struct {
	label   string                    // "stray" (non-binding) or "binding"
	build   func(extra string) string // cell JSON; extra is "" or `,"<VARIANT>":<body>`
	value   any                       // an encodable value for the carrier
	typ     string                    // carrier node's Type, for the props capture
	typName string                    // carrier node's Name for named kinds
}

type reservedDupRow struct {
	key      string // canonical spelling; variant spelling is ToUpper(key)
	carriers []reservedDupCarrier
	// variant bodies: one valid for the key's shape, one malformed
	// container/content, one wrong-JSON-kind scalar. The unpicked
	// spelling's routing must be IDENTICAL for all three.
	bodies []string
}

func reservedDupRows() []reservedDupRow {
	c := func(label, pre, post string, value any, typ, name string) reservedDupCarrier {
		return reservedDupCarrier{label, func(extra string) string { return pre + extra + post }, value, typ, name}
	}
	return []reservedDupRow{
		{"items", []reservedDupCarrier{
			c("stray", `{"type":"int","items":"int"`, `}`, int32(1), "int", ""),
			c("binding", `{"type":"array","items":"int"`, `}`, []any{}, "array", ""),
		}, []string{`"long"`, `{"type":"nosuch"}`, `12`}},
		{"values", []reservedDupCarrier{
			c("stray", `{"type":"int","values":"int"`, `}`, int32(1), "int", ""),
			c("binding", `{"type":"map","values":"int"`, `}`, map[string]any{}, "map", ""),
		}, []string{`"long"`, `{"type":"nosuch"}`, `12`}},
		{"fields", []reservedDupCarrier{
			c("stray", `{"type":"int","fields":[{"name":"f","type":"int"}]`, `}`, int32(1), "int", ""),
			c("binding", `{"type":"record","name":"RecB","fields":[{"name":"f","type":"int"}]`, `}`, map[string]any{"f": int32(1)}, "record", "RecB"),
		}, []string{`[{"name":"g","type":"long"}]`, `[12]`, `12`}},
		{"symbols", []reservedDupCarrier{
			c("stray", `{"type":"int","symbols":["A"]`, `}`, int32(1), "int", ""),
			c("binding", `{"type":"enum","name":"EnumB","symbols":["A"]`, `}`, "A", "enum", "EnumB"),
		}, []string{`["B"]`, `[1]`, `12`}},
		{"size", []reservedDupCarrier{
			c("stray", `{"type":"int","size":4`, `}`, int32(1), "int", ""),
			c("binding", `{"type":"fixed","name":"FixB","size":4`, `}`, []byte{0, 0, 0, 0}, "fixed", "FixB"),
		}, []string{`8`, `"x"`, `[1]`}},
		{"name", []reservedDupCarrier{
			c("stray", `{"type":"int","name":"x"`, `}`, int32(1), "int", ""),
			c("binding", `{"type":"record","name":"RecN","fields":[]`, `}`, map[string]any{}, "record", "RecN"),
		}, []string{`"y"`, `12`, `[1]`}},
		{"namespace", []reservedDupCarrier{
			c("stray", `{"type":"int","namespace":"x"`, `}`, int32(1), "int", ""),
			c("binding", `{"type":"record","name":"RecNS","namespace":"nsb","fields":[]`, `}`, map[string]any{}, "record", "RecNS"),
		}, []string{`"z"`, `12`, `[1]`}},
		{"aliases", []reservedDupCarrier{
			c("stray", `{"type":"int","aliases":["a"]`, `}`, int32(1), "int", ""),
			c("binding", `{"type":"record","name":"RecA","aliases":["AA"],"fields":[]`, `}`, map[string]any{}, "record", "RecA"),
		}, []string{`["b"]`, `[1]`, `12`}},
		// type and logicalType are consumed on every kind, so the carrier
		// axis has a single (universal) value.
		{"type", []reservedDupCarrier{
			c("binding", `{"type":"int"`, `}`, int32(1), "int", ""),
		}, []string{`"long"`, `12`, `[1]`}},
		{"logicalType", []reservedDupCarrier{
			c("binding", `{"type":"int","logicalType":"date"`, `}`, int32(1), "int", ""),
		}, []string{`"time-millis"`, `12`, `[1]`}},
	}
}

// checkReservedDupCell parses cell, asserts the three reading surfaces
// agree on Props, and (when wantKey is non-empty) that Props carries
// wantKey with wantVal on all of: Root().Props, the CustomType-callback
// node's Props, and the Root().Schema() rebuild's Props. When wantKey is
// "", asserts Props carries NO spelling of dropKey on any surface (the
// consumed-pick control).
func checkReservedDupCell(t *testing.T, cell string, carrier reservedDupCarrier, wantKey string, wantVal any, dropKey string) {
	t.Helper()
	var captured map[string]any
	s, err := avro.Parse(cell, avro.WithCustomType(propsCaptureCustom(carrier.typ, carrier.typName, &captured)))
	if err != nil {
		t.Fatalf("Parse(%s): %v", cell, err)
	}
	if _, err := s.AppendEncode(nil, carrier.value); err != nil {
		t.Fatalf("encode probe value: %v", err)
	}
	root := s.Root()
	node := findNodeByType(&root, carrier.typ, carrier.typName)
	if node == nil {
		t.Fatalf("carrier node %s/%s not found in Root()", carrier.typ, carrier.typName)
	}
	if !reflect.DeepEqual(captured, node.Props) {
		t.Errorf("parse-side Props and Root().Props disagree:\n callback: %#v\n Root():   %#v\ncell: %s", captured, node.Props, cell)
	}
	check := func(surface string, props map[string]any) {
		if wantKey != "" {
			got, ok := props[wantKey]
			if !ok {
				t.Errorf("%s missing %q (unpicked reserved-key spellings are ordinary properties, preserved verbatim): %#v\ncell: %s", surface, wantKey, props, cell)
			} else if !reflect.DeepEqual(got, wantVal) {
				t.Errorf("%s[%q] = %#v (%T); want %#v (%T)\ncell: %s", surface, wantKey, got, got, wantVal, wantVal, cell)
			}
		} else {
			for k := range props {
				if strings.EqualFold(k, dropKey) {
					t.Errorf("%s carries consumed pick spelling %q: %#v\ncell: %s", surface, k, props, cell)
				}
			}
		}
	}
	check("Root().Props", node.Props)
	check("callback Props", captured)
	rb, err := root.Schema()
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	rbRoot := rb.Root()
	rbNode := findNodeByType(&rbRoot, carrier.typ, carrier.typName)
	if rbNode == nil {
		t.Fatalf("carrier node %s/%s not found in rebuild", carrier.typ, carrier.typName)
	}
	check("rebuild Props", rbNode.Props)
}

// findNodeByType walks a SchemaNode tree for the first node with the given
// Type (and Name, when non-empty). The cells in this file are at most one
// level deep, so a shallow walk suffices.
func findNodeByType(n *avro.SchemaNode, typ, name string) *avro.SchemaNode {
	if n.Type == typ && (name == "" || n.Name == name) {
		return n
	}
	if n.Items != nil {
		if f := findNodeByType(n.Items, typ, name); f != nil {
			return f
		}
	}
	if n.Values != nil {
		if f := findNodeByType(n.Values, typ, name); f != nil {
			return f
		}
	}
	for i := range n.Branches {
		if f := findNodeByType(&n.Branches[i], typ, name); f != nil {
			return f
		}
	}
	for i := range n.Fields {
		if f := findNodeByType(&n.Fields[i].Type, typ, name); f != nil {
			return f
		}
	}
	return nil
}

// TestMatrix_ReservedKeyDuplicateSpellings crosses every reserved key that
// has a structural consumption arm × spellings-present {pick-only,
// pick+case-variant} × the variant's body {valid for the key's shape,
// malformed, non-schema scalar} × carrier {binding kind, non-binding
// stray placement} × reading surface {parse-side callback Props, Root()
// Props, Root().Schema() rebuild}. The unpicked spelling must ride to
// Props verbatim in every cell — its body's shape must not change the
// routing — and the pick-only controls prove the consumed pick never
// leaks into Props.
func TestMatrix_ReservedKeyDuplicateSpellings(t *testing.T) {
	t.Parallel()
	for _, row := range reservedDupRows() {
		variant := strings.ToUpper(row.key)
		for _, carrier := range row.carriers {
			t.Run(row.key+"/"+carrier.label+"/pick-only", func(t *testing.T) {
				checkReservedDupCell(t, carrier.build(""), carrier, "", nil, row.key)
			})
			for _, body := range row.bodies {
				t.Run(row.key+"/"+carrier.label+"/variant="+body, func(t *testing.T) {
					cell := carrier.build(`,"` + variant + `":` + body)
					checkReservedDupCell(t, cell, carrier, variant, propsValue(t, body), "")
				})
			}
		}
	}
}

// The unpicked-spelling rule is body-independent in the other direction
// too: when the PICK's body is malformed on a non-binding kind (so the
// pick itself rides to Props), the unpicked valid spelling still rides to
// Props — nothing is silently dropped and nothing is promoted to the
// structural field.
func TestMatrix_ReservedKeyDuplicatePickMalformed(t *testing.T) {
	t.Parallel()
	s, err := avro.Parse(`{"type":"int","items":12,"ITEMS":"int"}`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	root := s.Root()
	if root.Items != nil {
		t.Errorf("malformed pick body must not surface structurally: %+v", root.Items)
	}
	props := root.Props
	if got := props["items"]; !reflect.DeepEqual(got, int64(12)) {
		t.Errorf(`Props["items"] = %#v; want 12 (malformed pick body rides to Props)`, got)
	}
	if got := props["ITEMS"]; !reflect.DeepEqual(got, "int") {
		t.Errorf(`Props["ITEMS"] = %#v; want "int" (unpicked spelling rides to Props verbatim)`, got)
	}
}

// A malformed body under an unpicked case-variant spelling must ride to
// Props on every surface; it must not inherit the picked spelling's
// shape-OK verdict and be consumed.
func TestRegression_ReservedDupMalformedVariantPreserved(t *testing.T) {
	t.Parallel()
	carrier := reservedDupCarrier{"stray", func(extra string) string {
		return `{"type":"int","items":"int"` + extra + `}`
	}, int32(1), "int", ""}
	checkReservedDupCell(t, carrier.build(`,"ITEMS":12`), carrier, "ITEMS", int64(12), "")
}

// An unpicked case-variant spelling whose body is ITSELF a valid schema
// shape also rides to Props: only the picked spelling is consulted for
// structural surfacing, and the single structural slot is the pick's.
// Uniform preservation is the adjudicated rule — previously the unpicked
// valid spelling was silently dropped (consumed as reserved but never
// surfaced), losing user data with no observable trace.
func TestRegression_ReservedDupUnpickedValidVariantPreserved(t *testing.T) {
	t.Parallel()
	carrier := reservedDupCarrier{"stray", func(extra string) string {
		return `{"type":"int","items":"int"` + extra + `}`
	}, int32(1), "int", ""}
	checkReservedDupCell(t, carrier.build(`,"ITEMS":"long"`), carrier, "ITEMS", "long", "")
	// The structural slot carries the pick's body, not the variant's.
	s, err := avro.Parse(`{"type":"int","items":"int","ITEMS":"long"}`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	root := s.Root()
	if root.Items == nil || root.Items.Type != "int" {
		t.Errorf("structural Items = %+v; want the picked body (int)", root.Items)
	}
}

// The parse-side props (surfaced to CustomType callbacks) and the Root()
// metadata walk must apply the identical routing rule — including for the
// reserved keys with non-recursive bodies (name/namespace/symbols/size/
// aliases), where the two surfaces have historically used separate code.
func TestRegression_ReservedDupParseMetadataPropsParity(t *testing.T) {
	t.Parallel()
	var captured map[string]any
	s, err := avro.Parse(`{"type":"int","name":"x","NAME":12}`,
		avro.WithCustomType(propsCaptureCustom("int", "", &captured)))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if _, err := s.AppendEncode(nil, int32(5)); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	rootProps := s.Root().Props
	want := map[string]any{"NAME": int64(12)}
	if !reflect.DeepEqual(rootProps, want) {
		t.Errorf("Root().Props = %#v; want %#v", rootProps, want)
	}
	if !reflect.DeepEqual(captured, want) {
		t.Errorf("callback Props = %#v; want %#v (must equal Root().Props)", captured, want)
	}
}

// Field-level reserved keys follow the same rule: the picked spelling is
// consumed into the SchemaField attribute, every other spelling is an
// ordinary field property preserved in SchemaField.Props and by the
// rebuild.
func TestRegression_FieldReservedDupVariantPreserved(t *testing.T) {
	t.Parallel()
	s, err := avro.Parse(`{"type":"record","name":"FR","fields":[
		{"name":"f","type":"int","doc":"d","DOC":12}]}`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	f := s.Root().Fields[0]
	if f.Doc != "d" {
		t.Errorf("Doc = %q; want the picked spelling's body", f.Doc)
	}
	if got := f.Props["DOC"]; !reflect.DeepEqual(got, int64(12)) {
		t.Errorf(`Props["DOC"] = %#v; want 12 (unpicked field reserved-key spelling preserved)`, got)
	}
	root := s.Root()
	rb, err := root.Schema()
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if got := rb.Root().Fields[0].Props["DOC"]; !reflect.DeepEqual(got, int64(12)) {
		t.Errorf(`rebuild Props["DOC"] = %#v; want 12`, got)
	}
}

// TestMatrix_FieldReservedKeyDuplicateSpellings is the field-level arm of
// the duplicate-spelling matrix: every field reserved key × {pick-only,
// pick+case-variant} × {valid-shaped, non-schema} variant bodies, on
// SchemaField.Props and its rebuild.
func TestMatrix_FieldReservedKeyDuplicateSpellings(t *testing.T) {
	t.Parallel()
	base := `"name":"f","type":"int","doc":"d","default":1,"aliases":["fa"],"order":"ascending"`
	host := func(extra string) string {
		return `{"type":"record","name":"FRM","fields":[{` + base + extra + `}]}`
	}
	fieldBodies := map[string][]string{
		"name":    {`"g"`, `12`},
		"type":    {`"long"`, `12`},
		"default": {`2`, `[1]`},
		"doc":     {`"e"`, `12`},
		"aliases": {`["fb"]`, `12`},
		"order":   {`"descending"`, `12`},
	}
	fieldProps := func(t *testing.T, schema string) (map[string]any, map[string]any) {
		t.Helper()
		s, err := avro.Parse(schema)
		if err != nil {
			t.Fatalf("Parse(%s): %v", schema, err)
		}
		root := s.Root()
		props := root.Fields[0].Props
		rb, err := root.Schema()
		if err != nil {
			t.Fatalf("rebuild: %v", err)
		}
		return props, rb.Root().Fields[0].Props
	}
	t.Run("pick-only", func(t *testing.T) {
		props, rbProps := fieldProps(t, host(""))
		for _, p := range []map[string]any{props, rbProps} {
			for k := range p {
				if _, reserved := fieldBodies[strings.ToLower(k)]; reserved {
					t.Errorf("consumed field pick spelling %q leaked into Props: %#v", k, p)
				}
			}
		}
	})
	for key, bodies := range fieldBodies {
		variant := strings.ToUpper(key)
		for _, body := range bodies {
			t.Run(key+"/variant="+body, func(t *testing.T) {
				props, rbProps := fieldProps(t, host(`,"`+variant+`":`+body))
				want := propsValue(t, body)
				if got, ok := props[variant]; !ok || !reflect.DeepEqual(got, want) {
					t.Errorf("SchemaField.Props[%q] = %#v (present=%v); want %#v", variant, got, ok, want)
				}
				if got, ok := rbProps[variant]; !ok || !reflect.DeepEqual(got, want) {
					t.Errorf("rebuild SchemaField.Props[%q] = %#v (present=%v); want %#v", variant, got, ok, want)
				}
			})
		}
	}
}

// TestDifferentialFastavroReservedDupSpellings drives every matrix cell
// through fastavro's parser: fastavro reads reserved keys by exact
// (lowercase) name and preserves unknown keys, so each duplicate-spelling
// cell must parse there too.
func TestDifferentialFastavroReservedDupSpellings(t *testing.T) {
	o := startOracle(t)
	host := func(carrier string) string {
		return `{"type":"record","name":"TopDup","fields":[{"name":"a","type":` + carrier + `}]}`
	}
	for _, row := range reservedDupRows() {
		variant := strings.ToUpper(row.key)
		for _, carrier := range row.carriers {
			for _, body := range append([]string{""}, row.bodies...) {
				extra := ""
				if body != "" {
					extra = `,"` + variant + `":` + body
				}
				cell := host(carrier.build(extra))
				if _, err := avro.Parse(cell); err != nil {
					t.Errorf("twmb rejected a duplicate-spelling cell: %v\n%s", err, cell)
					continue
				}
				resp := o.call(oracleJob{Op: "parse", Schema: json.RawMessage(cell)})
				if !resp.OK {
					t.Errorf("fastavro rejected an accepted duplicate-spelling cell: %s\n%s", resp.Err, cell)
				}
			}
		}
	}
}

// The cache splice-merge applies the same rule when a props-carrying
// wrapped reference is replaced by its definition: the wrapper's picked
// reserved spellings die (definition-wins; Java drops usage-site extras
// at reference sites entirely), while unpicked spellings and picked stray
// spellings with non-shape bodies merge onto the spliced definition as
// ordinary props. Two wrapper props that collide case-insensitively only
// with EACH OTHER both merge, deterministically — the definition-wins
// check is against the definition's own (pre-merge) keys, never against
// what an earlier random-order iteration already merged. A merged variant
// that the DEFINITION's own next parse consumes (a def-side reserved key
// like doc, where the merged spelling becomes the sole pick) is consumed
// by that reparse — the merge is deterministic, and String() keeps the
// self-contained text verbatim, exactly like a plain parse whose raw text
// carries a consumed spelling.
func TestRegression_CacheSpliceWrapperVariantPropsPreserved(t *testing.T) {
	t.Parallel()
	// Repeat to shake out map-iteration-order dependence: every parse of
	// the same input must produce the identical spliced surface.
	for range 8 {
		var c avro.SchemaCache
		if _, err := c.Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":"int"}]}`); err != nil {
			t.Fatalf("cache def: %v", err)
		}
		s, err := c.Parse(`{"type":"record","name":"Top","fields":[
			{"name":"a","type":{"type":"R","doc":"x","DOC":12,"symbols":[1],"SYMBOLS":["B"],"foo":1,"FOO":2}}]}`)
		if err != nil {
			t.Fatalf("wrapped ref: %v", err)
		}
		spliced := s.Root().Fields[0].Type
		if spliced.Type != "record" || spliced.Name != "R" {
			t.Fatalf("wrapper did not splice to the definition: %+v", spliced)
		}
		props := spliced.Props
		want := map[string]any{
			// Picked stray spelling with a non-shape body: a prop.
			"symbols": []any{int64(1)},
			// Unpicked spelling: a prop, regardless of its body's shape.
			"SYMBOLS": []any{"B"},
			// Non-reserved props colliding only with each other: both merge.
			"foo": int64(1),
			"FOO": int64(2),
			// doc (picked; definition-wins) and DOC (merged, then consumed
			// by the definition's own reparse as its sole doc spelling) are
			// both absent.
		}
		if !reflect.DeepEqual(props, want) {
			t.Fatalf("spliced Props = %#v; want %#v", props, want)
		}
		if spliced.Doc != "" {
			t.Fatalf("Doc = %q; the definition has no doc and the wrapper's dies", spliced.Doc)
		}
	}
}
