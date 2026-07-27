package avro_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// The ATTRIBUTE x PLACEMENT acceptance census.
//
// Every attribute the Avro spec defines somewhere (precision, scale, items,
// values, symbols, size, fields, order, default, aliases, namespace,
// logicalType, and the enum-level default) placed on every OTHER kind, at
// both levels (a type object and a record-field object), skipping only the
// placements where the attribute is defined. Per accepted cell the census
// asserts the routing (where the stray attribute surfaces on the metadata
// API), the String() reparse and Root().Schema() rebuild round trips, and
// wire/Canonical()/Rabin identity with the stray-free twin — a stray
// attribute must be INERT. Reject cells assert their documented policy
// (NOT_BUGS #63 structural-key exclusivity). The fastavro differential arm
// executes every cell against fastavro's parser; the cisuite arm drives a
// representative subset through the Java oracle.
//
// The routing rule (one rule, both parse surfaces — aobjectFromMap extras
// and the Root() metadata walker share schemaReservedKeyForObject):
//
//   - Reserved keys the node's kind CONSUMES are consumed (items on array,
//     symbols on enum, size on fixed, fields on record/error, values on
//     map, namespace/aliases on named kinds, default on enum — #54,
//     precision/scale on recognized decimal carriers — #55).
//   - precision/scale anywhere else are custom properties (Props), at both
//     levels — including malformed (non-int) bodies at the FIELD level
//     when no decimal lift consumes them; the body-shape axis for the
//     pair lives in the strayPS placement matrix
//     (stray_precision_scale_test.go, NOT_BUGS #71).
//   - Every OTHER reserved key on a kind that does not consume it is
//     captured and surfaced AS-WRITTEN on the matching SchemaNode field
//     where one exists (items/values/symbols/size/fields; name/namespace;
//     aliases; logicalType), and rides to Props verbatim where none does
//     (order on every kind, default on every kind but enum — the two
//     field attributes, which no type object has a structural field for).
//     Exactly one surface per key, never both and never neither. None of
//     them reach the wire, the canonical form strips them, and the
//     Root().Schema() rebuild emits the defined-placement attributes plus
//     the Props-routed ones — so the rebuild is canonical-identical, not
//     text-identical. NOT_BUGS #63 records the structural-key edges.
//   - A container kind carrying ANOTHER container kind's defining
//     structural key hard-rejects ("invalid <kind> has schema for other
//     types") — NOT_BUGS #63, the cache/metadata walkers' kind-keyed
//     soundness premise; fastavro and Java accept these as props (the
//     documented divergence). A stray "name" on an unnamed CONTAINER kind
//     rejects for the same walker-parity reason (the metadata walkers
//     deliberately scope children by any non-empty Name); a stray
//     "namespace" there is inert and accepted.
//   - At the FIELD level every non-field-reserved key is a field custom
//     property (SchemaField.Props), as written; the field's type node
//     stays clean. (The flat goavro-style lift consumes defining keys only
//     when the field's "type" is a bare string naming the matching kind —
//     that family is netted by flat_field_lift_test.go and the
//     FEATURE x WALKER harness; the census spells field types nested or
//     bare-primitive so no cell is lift-eligible.)
//
// Field-level order/default/aliases are DEFINED field attributes (skipped);
// field-level logicalType is the #33 lift (netted by the harness's
// field-logical rows; skipped here with that citation).
// ---------------------------------------------------------------------------

// censusKinds are the type-level kinds; censusFieldKinds adds union (which
// has no type-object form — a union is a JSON array and cannot carry
// attributes at all, so its type-level placement is structurally skipped).
var censusKinds = []string{
	"null", "boolean", "int", "long", "float", "double", "string", "bytes",
	"fixed", "enum", "record", "error", "array", "map",
}

var censusPrimitives = map[string]bool{
	"null": true, "boolean": true, "int": true, "long": true,
	"float": true, "double": true, "string": true, "bytes": true,
}

var censusNamedKinds = map[string]bool{
	"fixed": true, "enum": true, "record": true, "error": true,
}

// censusVerdict classifies a cell's expected twmb behavior.
type censusVerdict int

const (
	censusSkip     censusVerdict = iota // defined placement — not a stray
	censusProps                         // accepted; key surfaces in Props
	censusCaptured                      // accepted; key surfaces on the matching SchemaNode field (or nowhere), never Props
	censusReject63                      // rejected: structural-key exclusivity (NOT_BUGS #63)
)

type censusAttr struct {
	key string
	// val returns the attribute's raw JSON value for a cell (kind-plausible
	// where it matters; the value axis is held fixed-valid — value-shape
	// acceptance is a different, already-netted axis: #41, #54, #55).
	val func(kind string) string
	// verdict returns the type-level expectation for the kind.
	verdict func(kind string) censusVerdict
	// fieldLevel reports whether the attribute is stray at the field level
	// (false = defined there, or covered by a dedicated net).
	fieldLevel bool
	// surfaced asserts kind-specific as-written surfacing on the metadata
	// node for censusCaptured cells (nil = key surfaces nowhere).
	surfaced func(t *testing.T, n *avro.SchemaNode, when string)
	// propsVal is the normalized Props value expected for censusProps and
	// field-level cells.
	propsVal any
	// propsValFor overrides propsVal per kind, for the attributes whose
	// value axis is kind-plausible (a "default" body has to be a value the
	// kind can take). nil falls back to propsVal.
	propsValFor func(kind string) any
}

// propsValue is the expected Props entry for a cell, preferring the per-kind
// override where the attribute has one.
func (a censusAttr) propsValue(kind string) any {
	if a.propsValFor != nil {
		return a.propsValFor(kind)
	}
	return a.propsVal
}

// structuralExclusive returns the #63 verdict table for one structural key:
// defined on definedKind, captured-dropped on primitives, rejected on every
// other container kind.
func structuralExclusive(definedKinds ...string) func(string) censusVerdict {
	defined := map[string]bool{}
	for _, k := range definedKinds {
		defined[k] = true
	}
	return func(kind string) censusVerdict {
		switch {
		case defined[kind]:
			return censusSkip
		case censusPrimitives[kind]:
			return censusCaptured
		default:
			return censusReject63
		}
	}
}

func censusAttrs() []censusAttr {
	constVal := func(v string) func(string) string { return func(string) string { return v } }
	always := func(v censusVerdict) func(string) censusVerdict { return func(string) censusVerdict { return v } }
	return []censusAttr{
		{
			key: "precision", val: constVal(`3`),
			// Bare precision (no decimal logical alongside) is stray on
			// EVERY kind: consumption requires the recognized decimal
			// carrier (decimalConsumesPrecisionScale), whose placement
			// matrix is TestMatrix_StrayPrecisionScalePlacement.
			verdict: always(censusProps), fieldLevel: true, propsVal: int64(3),
		},
		{
			key: "scale", val: constVal(`1`),
			verdict: always(censusProps), fieldLevel: true, propsVal: int64(1),
		},
		{
			key: "items", val: constVal(`"long"`),
			verdict: structuralExclusive("array"), fieldLevel: true, propsVal: "long",
			surfaced: func(t *testing.T, n *avro.SchemaNode, when string) {
				t.Helper()
				if n.Items == nil || n.Items.Type != "long" {
					t.Errorf("%s: captured items not surfaced as-written on Items (got %+v)", when, n.Items)
				}
			},
		},
		{
			key: "values", val: constVal(`"long"`),
			verdict: structuralExclusive("map"), fieldLevel: true, propsVal: "long",
			surfaced: func(t *testing.T, n *avro.SchemaNode, when string) {
				t.Helper()
				if n.Values == nil || n.Values.Type != "long" {
					t.Errorf("%s: captured values not surfaced as-written on Values (got %+v)", when, n.Values)
				}
			},
		},
		{
			key: "symbols", val: constVal(`["CENSUS"]`),
			verdict: structuralExclusive("enum"), fieldLevel: true, propsVal: []any{"CENSUS"},
			surfaced: func(t *testing.T, n *avro.SchemaNode, when string) {
				t.Helper()
				if !reflect.DeepEqual(n.Symbols, []string{"CENSUS"}) {
					t.Errorf("%s: captured symbols not surfaced as-written (got %v)", when, n.Symbols)
				}
			},
		},
		{
			key: "size", val: constVal(`4`),
			verdict: structuralExclusive("fixed"), fieldLevel: true, propsVal: int64(4),
			surfaced: func(t *testing.T, n *avro.SchemaNode, when string) {
				t.Helper()
				if n.Size != 4 {
					t.Errorf("%s: captured size not surfaced as-written (got %d)", when, n.Size)
				}
			},
		},
		{
			key: "fields", val: constVal(`[{"name":"cf","type":"int"}]`),
			verdict: structuralExclusive("record", "error"), fieldLevel: true,
			propsVal: []any{map[string]any{"name": "cf", "type": "int"}},
			surfaced: func(t *testing.T, n *avro.SchemaNode, when string) {
				t.Helper()
				if len(n.Fields) != 1 || n.Fields[0].Name != "cf" {
					t.Errorf("%s: captured fields not surfaced as-written (got %v)", when, n.Fields)
				}
			},
		},
		{
			// order is a FIELD attribute; no type-level kind consumes it and
			// no SchemaNode field exists for it to land on, so Props is its
			// only surface. Java keeps it as a schema property on every kind
			// (SCHEMA_RESERVED omits it, Schema.java:175-176; ENUM_RESERVED
			// adds only "default", :178-180) and fastavro 1.12.2 keeps it on
			// every kind too (executed).
			key: "order", val: constVal(`"ascending"`),
			verdict: always(censusProps), fieldLevel: false, propsVal: "ascending",
		},
		{
			// default is a FIELD attribute plus the enum-level evolution
			// default (defined there, membership-validated — #54; that is
			// the census's "enum-default" attribute). Every other type-level
			// placement binds nothing and has no SchemaNode field, so it
			// rides to Props — Java's ENUM_RESERVED is the same split.
			key: "default",
			val: func(kind string) string {
				switch kind {
				case "null":
					return `null`
				case "boolean":
					return `true`
				case "int", "long":
					return `3`
				case "float", "double":
					return `1.5`
				case "string", "bytes":
					return `"s"`
				case "fixed":
					return `"AAAA"`
				case "record", "error", "map":
					return `{}`
				case "array":
					return `[]`
				}
				return `null`
			},
			verdict: func(kind string) censusVerdict {
				if kind == "enum" {
					return censusSkip // enum-default: defined, #54-validated
				}
				return censusProps
			},
			fieldLevel: false,
			propsValFor: func(kind string) any {
				switch kind {
				case "null":
					return nil
				case "boolean":
					return true
				case "int", "long":
					return int64(3)
				case "float", "double":
					return 1.5
				case "string", "bytes":
					return "s"
				case "fixed":
					return "AAAA"
				case "record", "error", "map":
					return map[string]any{}
				case "array":
					return []any{}
				}
				return nil
			},
		},
		{
			// aliases are defined on named kinds (any string accepted, #27)
			// and on fields; on unnamed kinds they are captured and surface
			// as-written on SchemaNode.Aliases.
			key: "aliases", val: constVal(`["CensusAlias"]`),
			verdict: func(kind string) censusVerdict {
				if censusNamedKinds[kind] {
					return censusSkip
				}
				return censusCaptured
			},
			fieldLevel: false,
			surfaced: func(t *testing.T, n *avro.SchemaNode, when string) {
				t.Helper()
				if !reflect.DeepEqual(n.Aliases, []string{"CensusAlias"}) {
					t.Errorf("%s: captured aliases not surfaced as-written (got %v)", when, n.Aliases)
				}
			},
		},
		{
			// namespace is defined on named kinds. On primitives AND on the
			// unnamed containers (array, map) it is inert metadata: accepted,
			// surfaced as-written on SchemaNode.Namespace, never consulted
			// for name scoping (children keep the ENCLOSING scope — the
			// parser's rule, nodeChildScope's rule, and fastavro's executed
			// behavior agree; TestMatrix_AttributePlacementNamespaceInert
			// pins the scoping). Java treats it as reserved-and-ignored on
			// every schema object (SCHEMA_RESERVED, Schema.java).
			key: "namespace", val: constVal(`"census.ns"`),
			verdict: func(kind string) censusVerdict {
				if censusNamedKinds[kind] {
					return censusSkip
				}
				return censusCaptured
			},
			fieldLevel: true, propsVal: "census.ns",
			surfaced: func(t *testing.T, n *avro.SchemaNode, when string) {
				t.Helper()
				if n.Namespace != "census.ns" {
					t.Errorf("%s: captured namespace not surfaced as-written (got %q)", when, n.Namespace)
				}
			},
		},
		{
			// A recognized logical on a kind it is not valid for soft-drops
			// off the wire codec (raw encoding) but the metadata keeps the
			// as-written spelling. date is defined on int only.
			key: "logicalType", val: constVal(`"date"`),
			verdict: func(kind string) censusVerdict {
				if kind == "int" {
					return censusSkip
				}
				return censusCaptured
			},
			// Field-level logicalType is the #33 lift family, netted by the
			// FEATURE x WALKER harness's field-logical rows.
			fieldLevel: false,
			surfaced: func(t *testing.T, n *avro.SchemaNode, when string) {
				t.Helper()
				if n.LogicalType != "date" {
					t.Errorf("%s: soft-dropped logicalType not surfaced as-written (got %q)", when, n.LogicalType)
				}
			},
		},
	}
}

// censusTypeSchema builds a type-level cell schema; withAttr=false builds
// the stray-free twin.
func censusTypeSchema(kind, key, val string, withAttr bool) string {
	attr := ""
	if withAttr {
		attr = fmt.Sprintf(`,%q:%s`, key, val)
	}
	switch kind {
	case "fixed":
		return fmt.Sprintf(`{"type":"fixed","name":"CF","size":8%s}`, attr)
	case "enum":
		return fmt.Sprintf(`{"type":"enum","name":"CE","symbols":["A","B"]%s}`, attr)
	case "record":
		return fmt.Sprintf(`{"type":"record","name":"CR","fields":[{"name":"x","type":"int"}]%s}`, attr)
	case "error":
		return fmt.Sprintf(`{"type":"error","name":"CErr","fields":[{"name":"x","type":"int"}]%s}`, attr)
	case "array":
		return fmt.Sprintf(`{"type":"array","items":"int"%s}`, attr)
	case "map":
		return fmt.Sprintf(`{"type":"map","values":"int"%s}`, attr)
	default:
		return fmt.Sprintf(`{"type":%q%s}`, kind, attr)
	}
}

// censusFieldType spells the field's type for a field-level cell: bare
// strings for primitives, nested objects for complex kinds, a JSON array
// for union — never a lift-eligible flat spelling.
func censusFieldType(kind string) string {
	switch kind {
	case "fixed":
		return `{"type":"fixed","name":"CF","size":8}`
	case "enum":
		return `{"type":"enum","name":"CE","symbols":["A","B"]}`
	case "record":
		return `{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}`
	case "error":
		return `{"type":"error","name":"IErr","fields":[{"name":"x","type":"int"}]}`
	case "array":
		return `{"type":"array","items":"int"}`
	case "map":
		return `{"type":"map","values":"int"}`
	case "union":
		return `["null","int"]`
	default:
		return fmt.Sprintf("%q", kind)
	}
}

func censusFieldSchema(kind, key, val string, withAttr bool) string {
	attr := ""
	if withAttr {
		attr = fmt.Sprintf(`,%q:%s`, key, val)
	}
	return fmt.Sprintf(`{"type":"record","name":"Host","fields":[{"name":"a","type":%s%s}]}`, censusFieldType(kind), attr)
}

// censusValue returns a wire-encodable value for the kind.
func censusValue(kind string) any {
	switch kind {
	case "null":
		return nil
	case "boolean":
		return true
	case "int":
		return int32(7)
	case "long":
		return int64(7)
	case "float":
		return float32(1.5)
	case "double":
		return float64(1.5)
	case "string":
		return "s"
	case "bytes":
		return []byte{1, 2}
	case "fixed":
		return []byte{1, 2, 3, 4, 5, 6, 7, 8}
	case "enum":
		return "A"
	case "record", "error":
		return map[string]any{"x": int32(7)}
	case "array":
		return []int32{1, 2}
	case "map":
		return map[string]any{"k": int32(1)}
	case "union":
		return int32(7)
	}
	panic("unknown kind " + kind)
}

// assertCensusInert asserts the wire, canonical form, and Rabin fingerprint
// are identical to the stray-free twin, and that String() reparse and
// Root().Schema() rebuild stay canonical-stable.
func assertCensusInert(t *testing.T, s *avro.Schema, twinSrc string, value any) {
	t.Helper()
	twin, err := avro.Parse(twinSrc)
	if err != nil {
		t.Fatalf("twin Parse(%s): %v", twinSrc, err)
	}
	enc, err := s.AppendEncode(nil, value)
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	twinEnc, err := twin.AppendEncode(nil, value)
	if err != nil {
		t.Fatalf("twin AppendEncode: %v", err)
	}
	if !bytes.Equal(enc, twinEnc) {
		t.Errorf("wire diverges from stray-free twin:\n got %x\nwant %x", enc, twinEnc)
	}
	if !bytes.Equal(s.Canonical(), twin.Canonical()) {
		t.Errorf("Canonical diverges from stray-free twin:\n got %s\nwant %s", s.Canonical(), twin.Canonical())
	}
	if got, want := s.Fingerprint(avro.NewRabin()), twin.Fingerprint(avro.NewRabin()); !bytes.Equal(got, want) {
		t.Errorf("Rabin diverges from stray-free twin: got %x want %x", got, want)
	}
	s2, err := avro.Parse(s.String())
	if err != nil {
		t.Fatalf("String() reparse: %v", err)
	}
	if !bytes.Equal(s2.Canonical(), s.Canonical()) {
		t.Errorf("String() reparse changed Canonical: %s -> %s", s.Canonical(), s2.Canonical())
	}
	root := s.Root()
	rb, err := root.Schema()
	if err != nil {
		t.Fatalf("Root().Schema() rebuild: %v", err)
	}
	if !bytes.Equal(rb.Canonical(), s.Canonical()) {
		t.Errorf("rebuild changed Canonical: %s -> %s", s.Canonical(), rb.Canonical())
	}
}

func TestMatrix_AttributePlacementCensus(t *testing.T) {
	for _, attr := range censusAttrs() {
		for _, kind := range censusKinds {
			verdict := attr.verdict(kind)
			if verdict == censusSkip {
				continue
			}
			t.Run("type/"+attr.key+"/"+kind, func(t *testing.T) {
				val := attr.val(kind)
				src := censusTypeSchema(kind, attr.key, val, true)
				s, err := avro.Parse(src)

				if verdict == censusReject63 {
					if err == nil {
						t.Fatalf("Parse(%s) accepted; want the #63 structural-key exclusivity reject", src)
					}
					if !strings.Contains(err.Error(), "has schema for other types") {
						t.Errorf("Parse(%s) rejected with %q; want the #63 exclusivity error", src, err)
					}
					return
				}
				if err != nil {
					t.Fatalf("Parse(%s): %v", src, err)
				}

				root := s.Root()
				if verdict == censusProps {
					want := map[string]any{attr.key: attr.propsValue(kind)}
					if !reflect.DeepEqual(root.Props, want) {
						t.Errorf("Props = %#v; want %#v", root.Props, want)
					}
				} else {
					if _, ok := root.Props[attr.key]; ok {
						t.Errorf("captured reserved key %q leaked into Props: %#v", attr.key, root.Props)
					}
					if attr.surfaced != nil {
						attr.surfaced(t, &root, "parsed")
					}
				}
				if attr.key != "precision" && attr.key != "scale" {
					if root.Precision != 0 || root.Scale != 0 {
						t.Errorf("Precision/Scale = %d/%d; want 0/0", root.Precision, root.Scale)
					}
				}
				assertCensusInert(t, s, censusTypeSchema(kind, attr.key, val, false), censusValue(kind))
			})
		}

		if !attr.fieldLevel {
			continue
		}
		for _, kind := range append(append([]string{}, censusKinds...), "union") {
			t.Run("field/"+attr.key+"/"+kind, func(t *testing.T) {
				val := attr.val(kind)
				src := censusFieldSchema(kind, attr.key, val, true)
				s, err := avro.Parse(src)
				if err != nil {
					t.Fatalf("Parse(%s): %v", src, err)
				}
				f := s.Root().Fields[0]
				want := map[string]any{attr.key: attr.propsValue(kind)}
				if !reflect.DeepEqual(f.Props, want) {
					t.Errorf("field Props = %#v; want %#v", f.Props, want)
				}
				if f.Type.Precision != 0 || f.Type.Scale != 0 || len(f.Type.Props) != 0 {
					t.Errorf("field's type node polluted: Precision=%d Scale=%d Props=%#v",
						f.Type.Precision, f.Type.Scale, f.Type.Props)
				}
				assertCensusInert(t, s, censusFieldSchema(kind, attr.key, val, false),
					map[string]any{"a": censusValue(kind)})
			})
		}
	}
}

// A stray "namespace" attribute on the unnamed container kinds (array, map)
// parses as inert metadata, exactly like every other non-consumed reserved
// attribute and exactly like the same key on primitive type objects:
// surfaced as-written on the metadata node, never consulted for name
// scoping, invisible to the wire, the canonical form, and the fingerprint.
// Java ignores it via SCHEMA_RESERVED (Schema.java:175 - namespace is
// reserved on EVERY schema object; arrays and maps parse via
// parsePropertiesAndLogicalType with that set); fastavro 1.12.2 accepts it
// and resolves a named type defined UNDER an array carrying
// "namespace":"x" in the ENCLOSING scope (executed: top.Inner resolves,
// x.Inner does not). Rejecting it made twmb disagree with both references
// AND with itself across kinds ({"type":"int","namespace":"x"} always
// parsed).
func TestRegression_StrayNamespaceOnUnnamedComplexParses(t *testing.T) {
	cases := []struct {
		name string
		src  string
		twin string
	}{
		{"namespace-on-array", `{"type":"array","items":"int","namespace":"census.ns"}`, `{"type":"array","items":"int"}`},
		{"namespace-on-map", `{"type":"map","values":"int","namespace":"census.ns"}`, `{"type":"map","values":"int"}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := avro.Parse(c.src)
			if err != nil {
				t.Fatalf("Parse(%s) rejected a stray namespace attribute: %v", c.src, err)
			}
			root := s.Root()
			if len(root.Props) != 0 {
				t.Errorf("reserved namespace leaked into Props: %#v", root.Props)
			}
			if root.Namespace != "census.ns" {
				t.Errorf("stray namespace not surfaced as-written: %q", root.Namespace)
			}
			var v any
			if strings.Contains(c.name, "array") {
				v = []int32{1}
			} else {
				v = map[string]any{"k": int32(1)}
			}
			assertCensusInert(t, s, c.twin, v)
		})
	}
}

// A stray "name" attribute on the unnamed CONTAINER kinds keeps rejecting
// ("only record, enum, and fixed can have a name") while primitives keep
// their childless accept-and-surface posture. This is a deliberate,
// documented acceptance divergence (fastavro and Java accept the key as
// inert): the metadata walkers deliberately scope children by ANY non-empty
// SchemaNode.Name (nsForChildren's hand-built-tree posture), so a PARSED
// stray name on a kind that HAS children would make Root() scope named
// descendants differently than the wire parser — the same walker-parity
// soundness rationale as the #63 structural-key rejects. Primitives carry
// no child positions, so their accept is safe.
func TestRegression_StrayNameOnUnnamedComplexKeepsRejecting(t *testing.T) {
	for _, src := range []string{
		`{"type":"array","items":"int","name":"strayName"}`,
		`{"type":"map","values":"int","name":"strayName"}`,
		`{"type":"array","items":"int","name":"strayName","namespace":"census.ns"}`,
	} {
		if _, err := avro.Parse(src); err == nil {
			t.Errorf("Parse(%s) accepted a stray name on an unnamed container", src)
		} else if !strings.Contains(err.Error(), "only record, enum, and fixed can have a name") {
			t.Errorf("Parse(%s) rejected with %q; want the name-placement error", src, err)
		}
	}
	// The childless primitive posture is the boundary: same key, no child
	// positions for the walkers' name-scope arm to act on.
	s, err := avro.Parse(`{"type":"int","name":"strayName"}`)
	if err != nil {
		t.Fatalf("primitive-object stray name must keep parsing: %v", err)
	}
	if got := s.Root().Name; got != "strayName" {
		t.Errorf("primitive stray name not surfaced as-written: %q", got)
	}
}

// A stray "namespace" attribute on an array/map does NOT open a namespace
// scope: named types defined under it register in the ENCLOSING scope, and
// references resolve there — in the parser, in the SchemaCache splice
// walkers, and in fastavro (executed: the same schema resolves top.Inner
// and rejects x.Inner). The kind-keyed scope rule (nodeChildScope) and the
// parser agree by construction; these cells prove the agreement holds for
// the newly inert attribute.
func TestMatrix_AttributePlacementNamespaceInert(t *testing.T) {
	const defInner = `{"name":"f","type":{"type":"array","namespace":"x","items":
		{"type":"record","name":"Inner","fields":[{"name":"i","type":"int"}]}}}`

	t.Run("same-parse-enclosing-scope-resolves", func(t *testing.T) {
		src := `{"type":"record","name":"top.R","fields":[` + defInner + `,{"name":"g","type":"top.Inner"}]}`
		if _, err := avro.Parse(src); err != nil {
			t.Fatalf("enclosing-scope reference must resolve (namespace attr is inert): %v", err)
		}
	})
	t.Run("same-parse-attr-scope-dangles", func(t *testing.T) {
		src := `{"type":"record","name":"top.R","fields":[` + defInner + `,{"name":"g","type":"x.Inner"}]}`
		if _, err := avro.Parse(src); err == nil {
			t.Fatalf("x.Inner resolved: the stray namespace attribute scoped a child")
		}
	})
	t.Run("cross-parse-cache-splice-enclosing-scope", func(t *testing.T) {
		var c avro.SchemaCache
		if _, err := c.Parse(`{"type":"record","name":"top.R","fields":[` + defInner + `]}`); err != nil {
			t.Fatalf("parse-1: %v", err)
		}
		s, err := c.Parse(`{"type":"record","name":"top.R2","fields":[{"name":"g","type":"top.Inner"}]}`)
		if err != nil {
			t.Fatalf("cross-parse reference to the enclosing-scoped fullname must splice: %v", err)
		}
		if _, err := avro.Parse(s.String()); err != nil {
			t.Fatalf("spliced String() must be self-contained: %v", err)
		}
		if _, err := c.Parse(`{"type":"record","name":"top.R3","fields":[{"name":"g","type":"x.Inner"}]}`); err == nil {
			t.Fatalf("x.Inner spliced: a cache walker scoped a child by the stray namespace attribute")
		}
	})
	t.Run("stray-name-cannot-define", func(t *testing.T) {
		// A stray "name" on a container cannot become a definition: the
		// shape itself keeps rejecting at parse (see
		// TestRegression_StrayNameOnUnnamedComplexKeepsRejecting), so no
		// walker ever sees it.
		var c avro.SchemaCache
		if _, err := c.Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"array","items":"int","name":"strayName"}}]}`); err == nil {
			t.Fatalf("stray name on an array parsed; the walker-parity guard is gone")
		}
	})
}

// The primitive-object capture-drop family (#63): a named type spelled
// INSIDE a captured-then-dropped structural subtree ({"type":"int","items":
// {...record D...}}) is invisible to the parser — a same-parse reference
// does not resolve — and the walkers agree: the metadata tree surfaces the
// subtree as-written (Root().Items on the int node), the rebuild collapses
// it, and the SchemaCache walkers never collect D for cross-parse splicing.
// Walker-parity (the tombstoned B7 rule) holds for the dropped shape.
func TestMatrix_AttributePlacementDroppedSubtreeAgreement(t *testing.T) {
	const droppedDef = `{"name":"f","type":{"type":"int","items":
		{"type":"record","name":"D","fields":[{"name":"d","type":"int"}]}}}`

	t.Run("same-parse-ref-rejects", func(t *testing.T) {
		src := `{"type":"record","name":"Outer","fields":[` + droppedDef + `,{"name":"g","type":"D"}]}`
		if _, err := avro.Parse(src); err == nil {
			t.Fatalf("a definition inside a dropped primitive-object subtree resolved in the parser")
		}
	})
	t.Run("cross-parse-cache-ref-rejects", func(t *testing.T) {
		var c avro.SchemaCache
		if _, err := c.Parse(`{"type":"record","name":"Outer","fields":[` + droppedDef + `]}`); err != nil {
			t.Fatalf("parse-1: %v", err)
		}
		if _, err := c.Parse(`{"type":"record","name":"O2","fields":[{"name":"g","type":"D"}]}`); err == nil {
			t.Fatalf("a cache walker collected a definition from a dropped primitive-object subtree")
		}
	})
	t.Run("fields-arm-agrees", func(t *testing.T) {
		var c avro.SchemaCache
		if _, err := c.Parse(`{"type":"record","name":"Outer","fields":[{"name":"f","type":{"type":"boolean","fields":[{"name":"x","type":{"type":"record","name":"D2","fields":[{"name":"d","type":"int"}]}}]}}]}`); err != nil {
			t.Fatalf("parse-1: %v", err)
		}
		if _, err := c.Parse(`{"type":"record","name":"O3","fields":[{"name":"g","type":"D2"}]}`); err == nil {
			t.Fatalf("a cache walker collected a definition from a dropped fields subtree")
		}
	})
}

// TestDifferentialFastavroAttributePlacement executes EVERY census cell
// against fastavro's parser. fastavro accepts every placement — including
// the structural-key cells twmb rejects (the #63 documented divergence,
// asserted here so a fastavro release that starts rejecting flips this
// calibration loudly) and the name/namespace-on-unnamed cells. Runs with
// AVRO_FASTAVRO_PYTHON, like every differential.
func TestDifferentialFastavroAttributePlacement(t *testing.T) {
	o := startOracle(t)
	check := func(t *testing.T, src string, twmbRejects bool) {
		t.Helper()
		resp := o.call(oracleJob{Op: "parse", Schema: json.RawMessage(src)})
		if !resp.OK {
			if twmbRejects {
				t.Errorf("fastavro now REJECTS a #63 cell it accepted at calibration (%s): %s", src, resp.Err)
			} else {
				t.Errorf("fastavro rejected a schema twmb accepts: %s\n%s", resp.Err, src)
			}
		}
	}
	for _, attr := range censusAttrs() {
		for _, kind := range censusKinds {
			verdict := attr.verdict(kind)
			if verdict == censusSkip {
				continue
			}
			t.Run("type/"+attr.key+"/"+kind, func(t *testing.T) {
				check(t, censusTypeSchema(kind, attr.key, attr.val(kind), true), verdict == censusReject63)
			})
		}
		if !attr.fieldLevel {
			continue
		}
		for _, kind := range append(append([]string{}, censusKinds...), "union") {
			t.Run("field/"+attr.key+"/"+kind, func(t *testing.T) {
				check(t, censusFieldSchema(kind, attr.key, attr.val(kind), true), false)
			})
		}
	}

	// Scoping calibration (executed): a stray namespace on an array does
	// not scope in fastavro either — the enclosing-scope fullname resolves,
	// the attribute-scope one does not.
	t.Run("namespace-scoping", func(t *testing.T) {
		const def = `{"name":"f","type":{"type":"array","namespace":"x","items":{"type":"record","name":"Inner","fields":[{"name":"i","type":"int"}]}}}`
		ok := o.call(oracleJob{Op: "parse", Schema: json.RawMessage(`{"type":"record","name":"top.R","fields":[` + def + `,{"name":"g","type":"top.Inner"}]}`)})
		if !ok.OK {
			t.Errorf("fastavro: enclosing-scope reference should resolve: %s", ok.Err)
		}
		bad := o.call(oracleJob{Op: "parse", Schema: json.RawMessage(`{"type":"record","name":"top.R","fields":[` + def + `,{"name":"g","type":"x.Inner"}]}`)})
		if bad.OK {
			t.Errorf("fastavro: attribute-scope reference resolved — namespace-on-array scopes there now; recalibrate")
		}
	})
}
