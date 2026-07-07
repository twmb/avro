package avro_test

import (
	"bytes"
	"fmt"
	"math/big"
	"testing"

	"github.com/twmb/avro"
)

// The flat (goavro-style) field format puts a complex kind's defining key
// (symbols / items / values / fields / size) on the FIELD object alongside a
// bare string type. The wire parser lifts those keys into a nested type
// definition (liftFlatFieldType, schema_parse.go), and the metadata walker
// applies the same lift through the same shared helpers (flatFieldNeedsLift /
// flatLiftTypeMap), so Root() describes the post-lift schema. The tests in
// this file drive that contract as a matrix:
//
//   - TestMatrix_FlatFieldLift: kind × namespace-mode. Per cell: the flat
//     form and its handwritten nested twin are wire-identical (Canonical +
//     Rabin — the lift lives entirely in the parser, so the metadata walk
//     must not affect the wire tree); Root()'s type node carries the name
//     and defining content; routed keys (defining key, doc, custom props,
//     namespace-for-named) do NOT appear in SchemaField.Props;
//     Root().Schema() rebuilds canonical-identically; and the rebuilt
//     schema's wire bytes match the original's.
//   - TestMatrix_FlatFieldLiftLogicals: logicalType / precision / scale
//     route into the lifted type (flat fixed + duration, flat fixed +
//     decimal).
//   - TestMatrix_FlatFieldLiftNameRefDefaults: a lifted named type is
//     registered in the metadata name table, so name-referencing fields'
//     defaults coerce per the SchemaField.Default contract (fixed →
//     []byte), across sibling / cross-record diamond / recursive /
//     SchemaCache cross-parse reference shapes.
//   - TestMatrix_FlatFieldLiftNoLiftParity: the boundary cases where the
//     lift must NOT fire — on either side. The wire parser and the metadata
//     walker share one predicate, so a field the wire treats as a name
//     reference (or rejects) is never half-lifted in the metadata tree.
//   - TestMatrix_FlatFieldLiftDegenerate: degenerate-cardinality content
//     (an empty symbols list) lifts and round-trips like any other.
func TestMatrix_FlatFieldLift(t *testing.T) {
	type kindCell struct {
		kind      string // SchemaNode.Type expected on the lifted node
		fieldName string
		flatAttrs string // the defining attrs as written on the flat field
		named     bool
		datum     map[string]any
		check     func(t *testing.T, f avro.SchemaField)
	}
	cells := []kindCell{
		{
			kind: "enum", fieldName: "E",
			flatAttrs: `"symbols":["A","B"]`,
			named:     true,
			datum:     map[string]any{"E": "A"},
			check: func(t *testing.T, f avro.SchemaField) {
				if len(f.Type.Symbols) != 2 || f.Type.Symbols[0] != "A" {
					t.Errorf("Symbols = %v, want [A B]", f.Type.Symbols)
				}
			},
		},
		{
			kind: "fixed", fieldName: "F",
			flatAttrs: `"size":4`,
			named:     true,
			datum:     map[string]any{"F": []byte("abcd")},
			check: func(t *testing.T, f avro.SchemaField) {
				if f.Type.Size != 4 {
					t.Errorf("Size = %d, want 4", f.Type.Size)
				}
			},
		},
		{
			kind: "array", fieldName: "A",
			flatAttrs: `"items":"int"`,
			datum:     map[string]any{"A": []int32{1, 2}},
			check: func(t *testing.T, f avro.SchemaField) {
				if f.Type.Items == nil || f.Type.Items.Type != "int" {
					t.Errorf("Items = %+v, want int", f.Type.Items)
				}
			},
		},
		{
			kind: "map", fieldName: "M",
			flatAttrs: `"values":"long"`,
			datum:     map[string]any{"M": map[string]int64{"k": 5}},
			check: func(t *testing.T, f avro.SchemaField) {
				if f.Type.Values == nil || f.Type.Values.Type != "long" {
					t.Errorf("Values = %+v, want long", f.Type.Values)
				}
			},
		},
		{
			kind: "record", fieldName: "Sub",
			flatAttrs: `"fields":[{"name":"x","type":"int"}]`,
			named:     true,
			datum:     map[string]any{"Sub": map[string]any{"x": int32(9)}},
			check: func(t *testing.T, f avro.SchemaField) {
				if len(f.Type.Fields) != 1 || f.Type.Fields[0].Name != "x" {
					t.Errorf("Fields = %+v, want [x]", f.Type.Fields)
				}
			},
		},
		{
			kind: "error", fieldName: "Sub",
			flatAttrs: `"fields":[{"name":"x","type":"int"}]`,
			named:     true,
			datum:     map[string]any{"Sub": map[string]any{"x": int32(9)}},
			check: func(t *testing.T, f avro.SchemaField) {
				if len(f.Type.Fields) != 1 || f.Type.Fields[0].Name != "x" {
					t.Errorf("Fields = %+v, want [x]", f.Type.Fields)
				}
			},
		},
	}

	type nsMode struct {
		name     string
		rootNS   string // "namespace" attr on the enclosing record, "" = none
		fieldNS  string // explicit "namespace" attr on the flat field, "" = none
		expectNS string // expected SchemaNode.Namespace on a lifted NAMED type
	}
	modes := []nsMode{
		{name: "ns-absent"},
		{name: "ns-inherited", rootNS: "q", expectNS: "q"},
		{name: "ns-explicit", rootNS: "q", fieldNS: "x.y", expectNS: "x.y"},
	}

	for _, c := range cells {
		for _, m := range modes {
			if m.fieldNS != "" && !c.named {
				// An explicit namespace on an unnamed flat kind is not
				// propagated by the lift (the wire parser drops it); the
				// as-written-fidelity half is pinned separately in
				// TestMatrix_FlatFieldLiftNoLiftParity.
				continue
			}
			t.Run(c.kind+"/"+m.name, func(t *testing.T) {
				rootAttr, twinTypeNS := "", ""
				if m.rootNS != "" {
					rootAttr = fmt.Sprintf(`"namespace":%q,`, m.rootNS)
				}
				fieldAttr := ""
				if m.fieldNS != "" {
					fieldAttr = fmt.Sprintf(`"namespace":%q,`, m.fieldNS)
					twinTypeNS = fieldAttr
				}
				// Every flat cell carries a doc and a custom property so the
				// cell also pins their routing (both belong to the TYPE, as
				// the wire lift routes them).
				flat := fmt.Sprintf(
					`{"type":"record","name":"R",%s"fields":[{"name":%q,"type":%q,%s%s,"doc":"d","x-tag":"v"}]}`,
					rootAttr, c.fieldName, c.kind, fieldAttr, c.flatAttrs)
				var twinType string
				if c.named {
					twinType = fmt.Sprintf(`{"type":%q,"name":%q,%s%s,"doc":"d","x-tag":"v"}`,
						c.kind, c.fieldName, twinTypeNS, c.flatAttrs)
				} else {
					twinType = fmt.Sprintf(`{"type":%q,%s,"doc":"d","x-tag":"v"}`, c.kind, c.flatAttrs)
				}
				nested := fmt.Sprintf(
					`{"type":"record","name":"R",%s"fields":[{"name":%q,"type":%s}]}`,
					rootAttr, c.fieldName, twinType)

				sFlat, err := avro.Parse(flat)
				if err != nil {
					t.Fatalf("Parse flat: %v", err)
				}
				sNested, err := avro.Parse(nested)
				if err != nil {
					t.Fatalf("Parse nested twin: %v", err)
				}

				// Wire-tree guard: the lift is a parse-time transform, so the
				// flat form and the handwritten nested twin are one schema —
				// canonical form and Rabin fingerprint byte-identical.
				if !bytes.Equal(sFlat.Canonical(), sNested.Canonical()) {
					t.Fatalf("canonical(flat) != canonical(nested):\n %s\n %s",
						sFlat.Canonical(), sNested.Canonical())
				}
				if !bytes.Equal(sFlat.Fingerprint(avro.NewRabin()), sNested.Fingerprint(avro.NewRabin())) {
					t.Fatal("rabin(flat) != rabin(nested)")
				}

				root := sFlat.Root()
				f := root.Fields[0]
				if f.Type.Type != c.kind {
					t.Fatalf("Type.Type = %q, want %q", f.Type.Type, c.kind)
				}
				if c.named {
					if f.Type.Name != c.fieldName {
						t.Errorf("Type.Name = %q, want %q", f.Type.Name, c.fieldName)
					}
					if f.Type.Namespace != m.expectNS {
						t.Errorf("Type.Namespace = %q, want %q", f.Type.Namespace, m.expectNS)
					}
				}
				c.check(t, f)
				// Routed keys belong to the type, not the field: the doc and
				// the custom property surface on the type node, and the
				// field's own Props are empty.
				if f.Doc != "" {
					t.Errorf("field Doc = %q, want empty (routed to the type)", f.Doc)
				}
				if f.Type.Doc != "d" {
					t.Errorf("Type.Doc = %q, want %q", f.Type.Doc, "d")
				}
				if got := f.Type.Props["x-tag"]; got != "v" {
					t.Errorf("Type.Props[x-tag] = %v, want v", got)
				}
				if len(f.Props) != 0 {
					t.Errorf("field Props = %v, want empty (all keys routed)", f.Props)
				}

				rebuilt, err := root.Schema()
				if err != nil {
					t.Fatalf("Root().Schema(): %v", err)
				}
				if !bytes.Equal(rebuilt.Canonical(), sFlat.Canonical()) {
					t.Fatalf("canonical(rebuilt) != canonical(flat):\n %s\n %s",
						rebuilt.Canonical(), sFlat.Canonical())
				}

				wantWire, err := sFlat.Encode(c.datum)
				if err != nil {
					t.Fatalf("Encode flat: %v", err)
				}
				gotWire, err := rebuilt.Encode(c.datum)
				if err != nil {
					t.Fatalf("Encode rebuilt: %v", err)
				}
				if !bytes.Equal(wantWire, gotWire) {
					t.Fatalf("wire mismatch: flat %x, rebuilt %x", wantWire, gotWire)
				}
			})
		}
	}
}

// logicalType / precision / scale on a flat fixed field route into the
// lifted type definition (they are type attributes, exactly as the wire
// lift routes them), and the logical is live on both the original and the
// rebuilt schema.
func TestMatrix_FlatFieldLiftLogicals(t *testing.T) {
	t.Run("duration", func(t *testing.T) {
		s, err := avro.Parse(`{"type":"record","name":"R","fields":[
			{"name":"D","type":"fixed","size":12,"logicalType":"duration"}]}`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		root := s.Root()
		f := root.Fields[0]
		if f.Type.LogicalType != "duration" || f.Type.Size != 12 || f.Type.Name != "D" {
			t.Fatalf("lifted duration fixed: %+v", f.Type)
		}
		if len(f.Props) != 0 {
			t.Fatalf("field Props = %v, want empty", f.Props)
		}
		rebuilt, err := root.Schema()
		if err != nil {
			t.Fatalf("Root().Schema(): %v", err)
		}
		if !bytes.Equal(rebuilt.Canonical(), s.Canonical()) {
			t.Fatal("canonical mismatch")
		}
		datum := map[string]any{"D": avro.Duration{Months: 1, Days: 2, Milliseconds: 3}}
		want, err := s.Encode(datum)
		if err != nil {
			t.Fatalf("Encode flat: %v", err)
		}
		got, err := rebuilt.Encode(datum)
		if err != nil {
			t.Fatalf("Encode rebuilt: %v", err)
		}
		if !bytes.Equal(want, got) {
			t.Fatalf("wire mismatch: %x vs %x", want, got)
		}
	})
	t.Run("decimal", func(t *testing.T) {
		s, err := avro.Parse(`{"type":"record","name":"R","fields":[
			{"name":"C","type":"fixed","size":8,"logicalType":"decimal","precision":4,"scale":2}]}`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		root := s.Root()
		f := root.Fields[0]
		if f.Type.LogicalType != "decimal" || f.Type.Precision != 4 || f.Type.Scale != 2 || f.Type.Size != 8 {
			t.Fatalf("lifted decimal fixed: %+v", f.Type)
		}
		if len(f.Props) != 0 {
			t.Fatalf("field Props = %v, want empty", f.Props)
		}
		rebuilt, err := root.Schema()
		if err != nil {
			t.Fatalf("Root().Schema(): %v", err)
		}
		if !bytes.Equal(rebuilt.Canonical(), s.Canonical()) {
			t.Fatal("canonical mismatch")
		}
		datum := map[string]any{"C": big.NewRat(123, 100)}
		want, err := s.Encode(datum)
		if err != nil {
			t.Fatalf("Encode flat: %v", err)
		}
		got, err := rebuilt.Encode(datum)
		if err != nil {
			t.Fatalf("Encode rebuilt: %v", err)
		}
		if !bytes.Equal(want, got) {
			t.Fatalf("wire mismatch: %x vs %x", want, got)
		}
	})
}

// A lifted named type carries the field's name, so it registers in the
// metadata name table exactly like a nested-form definition: fields that
// reference it by name coerce their defaults per the SchemaField.Default
// contract ("bytes and fixed schemas give []byte"; enum defaults stay the
// member string). Reference shapes: same-record sibling, cross-record
// diamond, recursive self-reference, and a SchemaCache cross-parse
// reference (which travels via the cache's self-containment splice in
// nested form).
func TestMatrix_FlatFieldLiftNameRefDefaults(t *testing.T) {
	t.Run("sibling-fixed", func(t *testing.T) {
		s, err := avro.Parse(`{"type":"record","name":"R","fields":[
			{"name":"F","type":"fixed","size":4},
			{"name":"F2","type":"F","default":"abcd"}]}`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		b, ok := s.Root().Fields[1].Default.([]byte)
		if !ok || string(b) != "abcd" {
			t.Fatalf("F2 default = %T(%v), want []byte(abcd)", s.Root().Fields[1].Default, s.Root().Fields[1].Default)
		}
	})
	t.Run("sibling-enum", func(t *testing.T) {
		// Contract row: an enum default is already the member string on
		// both surfaces; the lift must leave it exactly as written.
		s, err := avro.Parse(`{"type":"record","name":"R","fields":[
			{"name":"E","type":"enum","symbols":["A","B"]},
			{"name":"E2","type":"E","default":"A"}]}`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		if got, ok := s.Root().Fields[1].Default.(string); !ok || got != "A" {
			t.Fatalf("E2 default = %T(%v), want string A", s.Root().Fields[1].Default, s.Root().Fields[1].Default)
		}
	})
	t.Run("diamond", func(t *testing.T) {
		// The flat definition lives inside one nested record; a second
		// nested record references it by name with a default.
		s, err := avro.Parse(`{"type":"record","name":"R","fields":[
			{"name":"s1","type":{"type":"record","name":"Sub1","fields":[
				{"name":"F","type":"fixed","size":4}]}},
			{"name":"s2","type":{"type":"record","name":"Sub2","fields":[
				{"name":"f","type":"F","default":"wxyz"}]}}]}`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		f := s.Root().Fields[1].Type.Fields[0]
		b, ok := f.Default.([]byte)
		if !ok || string(b) != "wxyz" {
			t.Fatalf("diamond ref default = %T(%v), want []byte(wxyz)", f.Default, f.Default)
		}
		root := s.Root()
		rebuilt, err := root.Schema()
		if err != nil {
			t.Fatalf("Root().Schema(): %v", err)
		}
		if !bytes.Equal(rebuilt.Canonical(), s.Canonical()) {
			t.Fatal("canonical mismatch")
		}
	})
	t.Run("recursive", func(t *testing.T) {
		// A flat-defined record that references itself through a union
		// branch: the lifted definition must be whole so the self-reference
		// re-binds on the rebuilt schema.
		s, err := avro.Parse(`{"type":"record","name":"R","fields":[
			{"name":"Node","type":"record","fields":[
				{"name":"v","type":"int"},
				{"name":"next","type":["null","Node"],"default":null}]}]}`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		root := s.Root()
		node := root.Fields[0].Type
		if node.Name != "Node" || len(node.Fields) != 2 {
			t.Fatalf("lifted recursive record: %+v", node)
		}
		if br := node.Fields[1].Type.Branches; len(br) != 2 || br[1].Type != "Node" {
			t.Fatalf("self-reference branches: %+v", node.Fields[1].Type.Branches)
		}
		rebuilt, err := root.Schema()
		if err != nil {
			t.Fatalf("Root().Schema(): %v", err)
		}
		if !bytes.Equal(rebuilt.Canonical(), s.Canonical()) {
			t.Fatal("canonical mismatch")
		}
		datum := map[string]any{"Node": map[string]any{
			"v": int32(1), "next": map[string]any{"Node": map[string]any{"v": int32(2), "next": nil}},
		}}
		want, err := s.Encode(datum)
		if err != nil {
			t.Fatalf("Encode flat: %v", err)
		}
		got, err := rebuilt.Encode(datum)
		if err != nil {
			t.Fatalf("Encode rebuilt: %v", err)
		}
		if !bytes.Equal(want, got) {
			t.Fatalf("wire mismatch: %x vs %x", want, got)
		}
	})
	t.Run("schemacache-cross-parse", func(t *testing.T) {
		var c avro.SchemaCache
		w, err := c.Parse(`{"type":"record","name":"W","namespace":"x","fields":[
			{"name":"F","type":"fixed","size":4}]}`)
		if err != nil {
			t.Fatalf("cache Parse defining doc: %v", err)
		}
		v, err := c.Parse(`{"type":"record","name":"V","namespace":"x","fields":[
			{"name":"g","type":"F","default":"mnop"}]}`)
		if err != nil {
			t.Fatalf("cache Parse referencing doc: %v", err)
		}
		// The defining doc's lifted fixed inherits the record's namespace.
		fw := w.Root().Fields[0]
		if fw.Type.Name != "F" || fw.Type.Namespace != "x" || fw.Type.Size != 4 {
			t.Fatalf("flat-defined fixed across cache: %+v", fw.Type)
		}
		wr := w.Root()
		rw, err := wr.Schema()
		if err != nil {
			t.Fatalf("defining doc Root().Schema(): %v", err)
		}
		if !bytes.Equal(rw.Canonical(), w.Canonical()) {
			t.Fatal("defining doc canonical mismatch")
		}
		// The referencing doc received the definition via the cache's
		// self-containment splice (nested form), so its default coerces and
		// its own rebuild round-trips.
		fv := v.Root().Fields[0]
		b, ok := fv.Default.([]byte)
		if !ok || string(b) != "mnop" {
			t.Fatalf("cross-parse ref default = %T(%v), want []byte(mnop)", fv.Default, fv.Default)
		}
		vr := v.Root()
		rv, err := vr.Schema()
		if err != nil {
			t.Fatalf("referencing doc Root().Schema(): %v", err)
		}
		if !bytes.Equal(rv.Canonical(), v.Canonical()) {
			t.Fatal("referencing doc canonical mismatch")
		}
	})
}

// The boundary cases where the flat lift must NOT fire. The wire parser and
// the metadata walker share one predicate (flatFieldNeedsLift), so each cell
// asserts the two sides agree: either the schema rejects at parse (neither
// side ever lifts) or it parses with the field's stray keys preserved
// as-written in SchemaField.Props and the rebuild canonical-stable.
func TestMatrix_FlatFieldLiftNoLiftParity(t *testing.T) {
	t.Run("defining-key-absent", func(t *testing.T) {
		// A bare "enum" type with no defining key is not the flat format;
		// it is an (undefined) name reference and rejects at parse.
		_, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"e","type":"enum"}]}`)
		if err == nil {
			t.Fatal("Parse accepted a bare complex-kind type with no defining key")
		}
	})
	t.Run("wrong-kind-defining-key", func(t *testing.T) {
		// "items" defines arrays, not enums: the lift does not fire, so the
		// bare "enum" stays an undefined name reference.
		_, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"e","type":"enum","items":"int"}]}`)
		if err == nil {
			t.Fatal("Parse accepted a mismatched defining key as a flat field")
		}
	})
	t.Run("object-type-never-lifts", func(t *testing.T) {
		// A nested type OBJECT is already a definition; a stray field-level
		// defining key alongside it is a custom field property on both
		// sides, never a lift input.
		s, err := avro.Parse(`{"type":"record","name":"R","fields":[
			{"name":"e","type":{"type":"enum","name":"X","symbols":["A"]},"symbols":["B","C"]}]}`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		f := s.Root().Fields[0]
		if f.Type.Name != "X" || len(f.Type.Symbols) != 1 {
			t.Fatalf("nested type mangled: %+v", f.Type)
		}
		if _, ok := f.Props["symbols"]; !ok {
			t.Fatalf("stray field-level symbols missing from Props: %v", f.Props)
		}
		root := s.Root()
		rebuilt, err := root.Schema()
		if err != nil {
			t.Fatalf("Root().Schema(): %v", err)
		}
		if !bytes.Equal(rebuilt.Canonical(), s.Canonical()) {
			t.Fatal("canonical mismatch")
		}
	})
	t.Run("name-ref-type-never-lifts", func(t *testing.T) {
		// A name reference with a stray defining key is a reference plus a
		// custom field property on both sides — "MyEnum" is not a liftable
		// kind name.
		s, err := avro.Parse(`{"type":"record","name":"R","fields":[
			{"name":"d","type":{"type":"enum","name":"MyEnum","symbols":["B"]}},
			{"name":"e","type":"MyEnum","symbols":["Z"]}]}`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		f := s.Root().Fields[1]
		if f.Type.Type != "MyEnum" {
			t.Fatalf("reference field type = %+v, want bare MyEnum ref", f.Type)
		}
		if _, ok := f.Props["symbols"]; !ok {
			t.Fatalf("stray symbols missing from Props: %v", f.Props)
		}
		root := s.Root()
		rebuilt, err := root.Schema()
		if err != nil {
			t.Fatalf("Root().Schema(): %v", err)
		}
		if !bytes.Equal(rebuilt.Canonical(), s.Canonical()) {
			t.Fatal("canonical mismatch")
		}
	})
	t.Run("primitive-type-never-lifts", func(t *testing.T) {
		s, err := avro.Parse(`{"type":"record","name":"R","fields":[
			{"name":"p","type":"int","symbols":["A"]}]}`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		f := s.Root().Fields[0]
		if f.Type.Type != "int" {
			t.Fatalf("field type = %+v, want int", f.Type)
		}
		if _, ok := f.Props["symbols"]; !ok {
			t.Fatalf("stray symbols missing from Props: %v", f.Props)
		}
		root := s.Root()
		rebuilt, err := root.Schema()
		if err != nil {
			t.Fatalf("Root().Schema(): %v", err)
		}
		if !bytes.Equal(rebuilt.Canonical(), s.Canonical()) {
			t.Fatal("canonical mismatch")
		}
	})
	t.Run("unnamed-explicit-namespace-stays-on-field", func(t *testing.T) {
		// The lift propagates "namespace" only for named kinds; on an
		// unnamed flat kind the wire parser drops it, and the metadata
		// walker preserves it as-written in the field's Props (the parser
		// ignores it on re-parse, so the rebuild is canonical-stable).
		s, err := avro.Parse(`{"type":"record","name":"R","fields":[
			{"name":"a","type":"array","items":"int","namespace":"x.y"}]}`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		f := s.Root().Fields[0]
		if f.Type.Type != "array" || f.Type.Items == nil {
			t.Fatalf("lifted array: %+v", f.Type)
		}
		if got := f.Props["namespace"]; got != "x.y" {
			t.Fatalf("field Props[namespace] = %v, want x.y", got)
		}
		root := s.Root()
		rebuilt, err := root.Schema()
		if err != nil {
			t.Fatalf("Root().Schema(): %v", err)
		}
		if !bytes.Equal(rebuilt.Canonical(), s.Canonical()) {
			t.Fatal("canonical mismatch")
		}
	})
}

// Degenerate-cardinality content lifts like any other: an empty symbols
// list is parseable (degenerate types parse; no value of the enum can ever
// encode, but the schema itself round-trips), and the lifted node plus its
// rebuild carry the empty list faithfully.
func TestMatrix_FlatFieldLiftDegenerate(t *testing.T) {
	s, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"E","type":"enum","symbols":[]}]}`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	f := s.Root().Fields[0]
	if f.Type.Type != "enum" || f.Type.Name != "E" || f.Type.Symbols == nil || len(f.Type.Symbols) != 0 {
		t.Fatalf("lifted empty enum: %+v", f.Type)
	}
	root := s.Root()
	rebuilt, err := root.Schema()
	if err != nil {
		t.Fatalf("Root().Schema(): %v", err)
	}
	if !bytes.Equal(rebuilt.Canonical(), s.Canonical()) {
		t.Fatalf("canonical mismatch:\n %s\n %s", rebuilt.Canonical(), s.Canonical())
	}
}
