package avro_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/twmb/avro"
)

// strayRouteCase enumerates one reserved key's shape battery: a body that
// parses as the key's schema shape (structural surfacing), malformed
// bodies (Props verbatim), and the key's binding kind (whose shape errors
// must keep rejecting).
type strayRouteCase struct {
	key       string
	okBody    string   // shape-OK body (structural route on a non-binding kind)
	malformed []string // non-schema-shaped bodies (Props route)
	binding   string   // a carrier that BINDS the key (malformed must reject)
}

// The malformed bodies deliberately include shapes that pass a LAX capture
// but fail the key's schema shape — a mixed-type array is a []any (a lax
// []any assert accepts it) but not a []string; 3.7 is a JSON number (a lax
// numeric read truncates it) but not an integer. These wedge bodies pin
// that the capture predicate and the shape verdict are the SAME predicate.
var strayRouteCases = []strayRouteCase{
	{"items", `"int"`, []string{`3`, `{"type":3}`}, `{"type":"array","items":%s}`},
	{"values", `"int"`, []string{`true`, `[3]`}, `{"type":"map","values":%s}`},
	{"fields", `[{"name":"f","type":"int"}]`, []string{`3`, `[3]`, `[{"name":"f","type":"int","aliases":[1]}]`}, `{"type":"record","name":"BN","fields":%s}`},
	{"symbols", `["A"]`, []string{`3`, `[3]`, `["a",1]`}, `{"type":"enum","name":"BN","symbols":%s}`},
	{"size", `4`, []string{`"x"`, `true`, `3.7`, `99999999999999999999`}, `{"type":"fixed","name":"BN","size":%s}`},
	{"name", `"nm"`, []string{`3`, `[3]`}, `{"type":"record","name":%s,"fields":[]}`},
	{"namespace", `"ns"`, []string{`3`, `{}`}, `{"type":"record","namespace":%s,"name":"BN","fields":[]}`},
	{"aliases", `["al"]`, []string{`3`, `[3]`, `["a",1]`}, `{"type":"record","name":"BN","aliases":%s,"fields":[]}`},
	{"precision", `3`, []string{`"abc"`, `3.5`}, `{"type":"bytes","logicalType":"decimal","scale":0,"precision":%s}`},
	{"scale", `0`, []string{`"abc"`, `1.5`}, `{"type":"bytes","logicalType":"decimal","precision":3,"scale":%s}`},
}

// assertStrayStructuralZero asserts the structural surface for key is the
// zero value on n: a malformed stray body's ONLY surface is Props verbatim.
// The metadata walker's capture and the Props routing share one shape
// verdict, so a body that rides to Props can never also surface a coerced
// image on the structural field (structural-field-set ⟺ consumed-out-of-
// Props).
func assertStrayStructuralZero(t *testing.T, key string, n avro.SchemaNode, when string) {
	t.Helper()
	zero := true
	var got any
	switch key {
	case "items":
		zero, got = n.Items == nil, n.Items
	case "values":
		zero, got = n.Values == nil, n.Values
	case "fields":
		zero, got = n.Fields == nil, n.Fields
	case "symbols":
		zero, got = n.Symbols == nil, n.Symbols
	case "aliases":
		zero, got = n.Aliases == nil, n.Aliases
	case "size":
		zero, got = n.Size == 0, n.Size
	case "name":
		zero, got = n.Name == "", n.Name
	case "namespace":
		zero, got = n.Namespace == "", n.Namespace
	case "precision":
		zero, got = n.Precision == 0, n.Precision
	case "scale":
		zero, got = n.Scale == 0, n.Scale
	}
	if !zero {
		t.Errorf("%s: malformed stray %q fabricated a structural surface: %#v (want zero; Props verbatim is the only route)", when, key, got)
	}
}

// TestMatrix_StrayBodyShapeRouting crosses reserved key × body shape ×
// carrier against the surfacing route: on a kind that does not bind the
// key, a schema-shaped body surfaces structurally (as-written) and a
// non-schema-shaped body rides in Props exactly like a custom property
// (verbatim — asserted against a twin schema carrying the same value
// under a non-reserved key); on the binding kind the shape error keeps
// rejecting. Wire codecs ignore both stray routes; the metadata rebuild
// round-trips them. fastavro accepts every accept cell (executed when
// AVRO_FASTAVRO_PYTHON is set); Java skips reserved keys wholesale on
// non-binding kinds (SCHEMA_RESERVED, Schema.java:175-176).
func TestMatrix_StrayBodyShapeRouting(t *testing.T) {
	t.Parallel()
	host := func(carrier string) string {
		return `{"type":"record","name":"Top","fields":[{"name":"a","type":` + carrier + `}]}`
	}
	for _, c := range strayRouteCases {
		for i, mal := range c.malformed {
			t.Run(fmt.Sprintf("%s_malformed%d_props", c.key, i), func(t *testing.T) {
				s, err := avro.Parse(host(`{"type":"int","` + c.key + `":` + mal + `}`))
				if err != nil {
					t.Fatalf("malformed stray on a non-binding kind rejected: %v", err)
				}
				n := s.Root().Fields[0].Type
				got, ok := n.Props[c.key]
				if !ok {
					t.Fatalf("stray %q not in Props: %v", c.key, n.Props)
				}
				twin := avro.MustParse(host(`{"type":"int","myprop":` + mal + `}`))
				want := twin.Root().Fields[0].Type.Props["myprop"]
				if !reflect.DeepEqual(got, want) {
					t.Errorf("Props[%q] = %#v, want the custom-prop image %#v", c.key, got, want)
				}
				assertStrayStructuralZero(t, c.key, n, "carrier")
				enc, err := s.Encode(map[string]any{"a": int32(7)})
				if err != nil {
					t.Fatalf("encode: %v", err)
				}
				var out map[string]any
				if _, err := s.Decode(enc, &out); err != nil {
					t.Fatalf("decode: %v", err)
				}
				root := s.Root()
				rb, err := root.Schema()
				if err != nil {
					t.Fatalf("rebuild: %v", err)
				}
				if got := rb.Root().Fields[0].Type.Props[c.key]; !reflect.DeepEqual(got, want) {
					t.Errorf("rebuild lost the Props route: %#v, want %#v", got, want)
				}
			})
			t.Run(fmt.Sprintf("%s_malformed%d_binding_rejects", c.key, i), func(t *testing.T) {
				if _, err := avro.Parse(host(fmt.Sprintf(c.binding, mal))); err == nil {
					t.Errorf("malformed %q on its binding kind accepted, want reject", c.key)
				}
			})
			t.Run(fmt.Sprintf("%s_malformed%d_ref_props", c.key, i), func(t *testing.T) {
				s, err := avro.Parse(`{"type":"record","name":"Top","fields":[
					{"name":"b","type":{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}},
					{"name":"a","type":{"type":"R","` + c.key + `":` + mal + `}}]}`)
				if err != nil {
					t.Fatalf("malformed stray on a wrapped reference rejected: %v", err)
				}
				n := s.Root().Fields[1].Type
				if n.Type != "R" {
					t.Fatalf("reference not preserved: %+v", n)
				}
				if _, ok := n.Props[c.key]; !ok {
					t.Errorf("stray %q not in the reference's Props: %v", c.key, n.Props)
				}
				assertStrayStructuralZero(t, c.key, n, "wrapped reference")
			})
		}
		t.Run(c.key+"_shapeok_structural", func(t *testing.T) {
			s, err := avro.Parse(host(`{"type":"int","` + c.key + `":` + c.okBody + `}`))
			if err != nil {
				t.Fatalf("shape-OK stray rejected: %v", err)
			}
			n := s.Root().Fields[0].Type
			structural := true
			switch c.key {
			case "items":
				structural = n.Items != nil && n.Items.Type == "int"
			case "values":
				structural = n.Values != nil && n.Values.Type == "int"
			case "fields":
				structural = len(n.Fields) == 1 && n.Fields[0].Name == "f"
			case "symbols":
				structural = len(n.Symbols) == 1 && n.Symbols[0] == "A"
			case "name":
				structural = n.Name == "nm"
			case "namespace":
				structural = n.Namespace == "ns"
			case "aliases":
				structural = len(n.Aliases) == 1 && n.Aliases[0] == "al"
			case "precision", "scale":
				// Unconsumed off a decimal carrier: the props route is
				// the documented placement for valid values too.
				if _, ok := n.Props[c.key]; !ok {
					t.Errorf("valid %s off a decimal carrier not in Props: %v", c.key, n.Props)
				}
				return
			case "size":
				// Consumed nowhere on a primitive: reserved (not a
				// custom property), no structural field carries it.
			}
			if !structural {
				t.Errorf("shape-OK stray %q not surfaced structurally: %+v", c.key, n)
			}
			if _, ok := n.Props[c.key]; ok {
				t.Errorf("shape-OK stray %q leaked into Props", c.key)
			}
		})
	}
}

// TestDifferentialFastavroStrayBodyShapes drives every accept cell of the
// shape-routing matrix through fastavro's parser: fastavro ignores
// reserved keys on kinds that do not consume them, so each cell must
// parse there too.
func TestDifferentialFastavroStrayBodyShapes(t *testing.T) {
	o := startOracle(t)
	host := func(carrier string) string {
		return `{"type":"record","name":"Top","fields":[{"name":"a","type":` + carrier + `}]}`
	}
	for _, c := range strayRouteCases {
		bodies := append([]string{c.okBody}, c.malformed...)
		for _, body := range bodies {
			cell := host(`{"type":"int","` + c.key + `":` + body + `}`)
			if _, err := avro.Parse(cell); err != nil {
				// Cells twmb rejects are covered by the routing matrix;
				// the differential asserts agreement on accepts only.
				continue
			}
			resp := o.call(oracleJob{Op: "parse", Schema: json.RawMessage(cell)})
			if !resp.OK {
				t.Errorf("fastavro rejected an accepted stray-shape cell: %s\n%s", resp.Err, cell)
			}
		}
	}
}

// The three pins below lock the malformed-stray single-surface invariant at
// the top level (no host record): a body that does not parse as the reserved
// key's schema shape rides in Props verbatim and sets NO structural field.
// The wedge bodies are exactly the shapes a lax capture would coerce — a
// mixed-type array type-asserts as []any (element-wise assert fabricates ""
// for non-strings), and a non-integral number truncates under a plain
// numeric conversion — so a fabricated value here means the capture
// predicate has drifted from the shape verdict.

func TestRegression_StrayAliasesMalformedNotStructurallySurfaced(t *testing.T) {
	t.Parallel()
	s, err := avro.Parse(`{"type":"int","aliases":["a",1]}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	n := s.Root()
	if _, ok := n.Props["aliases"]; !ok {
		t.Fatalf("malformed stray aliases not in Props: %#v", n.Props)
	}
	if n.Aliases != nil {
		t.Errorf("malformed stray aliases fabricated a structural surface: %#v (want nil)", n.Aliases)
	}
}

func TestRegression_StraySymbolsMalformedNotStructurallySurfaced(t *testing.T) {
	t.Parallel()
	s, err := avro.Parse(`{"type":"int","symbols":["a",1]}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	n := s.Root()
	if _, ok := n.Props["symbols"]; !ok {
		t.Fatalf("malformed stray symbols not in Props: %#v", n.Props)
	}
	if n.Symbols != nil {
		t.Errorf("malformed stray symbols fabricated a structural surface: %#v (want nil)", n.Symbols)
	}
}

func TestRegression_StraySizeMalformedNotStructurallySurfaced(t *testing.T) {
	t.Parallel()
	s, err := avro.Parse(`{"type":"int","size":3.7}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	n := s.Root()
	if _, ok := n.Props["size"]; !ok {
		t.Fatalf("malformed stray size not in Props: %#v", n.Props)
	}
	if n.Size != 0 {
		t.Errorf("malformed stray size fabricated a structural surface: %d (want 0)", n.Size)
	}
}
