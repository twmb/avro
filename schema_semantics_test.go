package avro_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc64"
	"math"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

// ---------- attribute_placement_census_test.go ----------

// ---------------------------------------------------------------------------
// The ATTRIBUTE x PLACEMENT acceptance census.
//
// Every attribute the Avro spec defines somewhere (precision, scale, items,
// values, symbols, size, fields, order, default, aliases, namespace,
// logicalType, and the enum-level default) placed on every OTHER kind, at both
// levels (a type object and a record-field object), skipping only the placements
// where the attribute is defined. Per accepted cell the census asserts the
// routing, the String() reparse and Root().Schema() rebuild round trips, and
// wire/Canonical()/Rabin identity with the stray-free twin — a stray attribute
// must be INERT. Reject cells assert their documented policy (NOT_BUGS #63). The
// fastavro differential arm executes every cell; the cisuite arm drives a
// representative subset through the Java oracle.
//
// The routing rule — one rule, both parse surfaces, since aobjectFromMap extras
// and the Root() metadata walker share schemaReservedKeyForObject:
//
//   - Reserved keys the node's kind CONSUMES are consumed (items on array,
//     symbols on enum, size on fixed, fields on record/error, values on map,
//     namespace/aliases on named kinds, default on enum — #54, precision/scale
//     on recognized decimal carriers — #55).
//   - precision/scale anywhere else are custom properties at both levels,
//     including malformed bodies at the FIELD level when no decimal lift
//     consumes them; the body-shape axis lives in the strayPS placement matrix
//     (NOT_BUGS #71).
//   - Every OTHER reserved key on a kind that does not consume it is captured
//     and surfaced AS-WRITTEN on the matching SchemaNode field where one exists,
//     and rides to Props verbatim where none does (order on every kind, default
//     on every kind but enum). Exactly one surface per key, never both and never
//     neither. None reach the wire, the canonical form strips them, and the
//     rebuild emits the defined-placement attributes plus the Props-routed ones
//     — so the rebuild is canonical-identical, not text-identical.
//   - A container kind carrying ANOTHER container kind's defining structural key
//     hard-rejects — NOT_BUGS #63, the cache/metadata walkers' kind-keyed
//     soundness premise; fastavro and Java accept these as props (the documented
//     divergence). A stray "name" on an unnamed CONTAINER kind rejects for the
//     same walker-parity reason; a stray "namespace" there is inert.
//   - At the FIELD level every non-field-reserved key is a field custom property
//     as written, and the field's type node stays clean. (The flat goavro-style
//     lift consumes defining keys only when the field's "type" is a bare string
//     naming the matching kind; the census spells field types nested or
//     bare-primitive so no cell is lift-eligible.)
//
// Field-level order/default/aliases are DEFINED field attributes (skipped);
// field-level logicalType is the #33 lift, netted by the harness's
// field-logical rows.
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
						attr.surfaced(t, root, "parsed")
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

// A stray "namespace" attribute on the unnamed container kinds parses as inert
// metadata, exactly like every other non-consumed reserved attribute and like
// the same key on primitive type objects: surfaced as-written on the metadata
// node, never consulted for name scoping, invisible to the wire, the canonical
// form, and the fingerprint. Java ignores it via SCHEMA_RESERVED
// (Schema.java:175 — namespace is reserved on EVERY schema object); fastavro
// 1.12.2 accepts it and resolves a named type defined under such an array in the
// ENCLOSING scope (executed). Rejecting it made twmb disagree with both
// references AND with itself across kinds.
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

// ---------- reserved_attribute_matrix_test.go ----------

// ---------------------------------------------------------------------------
// The reserved-attribute enumeration.
//
// The question is a CLASS, not a list of remembered cases: for a reserved
// attribute A with body shape B at level L on kind K, what is the outcome on
// every surface? The corpus is that cross product, and every expected value in
// it was DERIVED FROM THE REFERENCE IMPLEMENTATIONS rather than read off this
// package — an expectation copied from the code under test asserts only that
// nothing changed, which is what let each of these families drift.
//
// How each expectation was obtained, and how to redo it:
//
//   - Apache Avro (Java) is a SOURCE-DERIVED model of Schema.java on
//     apache/avro:main, because the check runs without a JVM. Every rule carries
//     the line it came from, and the model is validated against the
//     already-adjudicated cells before any of its new answers are used. The
//     rules that matter most are the EMISSION conditions, which differ per
//     attribute: doc emits when non-null (:1039/:1154/:1367/:1062), aliases when
//     non-empty (:886/:1070), order is decided on the JSON node (:1895-1897),
//     and anything absent from SCHEMA_RESERVED (:175-176) survives through
//     parseProperties (:1983).
//   - fastavro 1.12.2 was EXECUTED per cell.
//   - Where the two disagree, the expectation is one of THEIR answers and the
//     provenance table records which. This package never invents a third.
//   - Where neither can adjudicate — a stray structural key has no analogue in
//     either — the standing rulings govern and the provenance says so.
//   - Where NEITHER reference has the placement at all, PLACEMENT AUTHORITY
//     decides: whichever reference actually HAS it governs, for the empty and
//     the non-empty body alike. Where neither has it, this package's own
//     adjudicated posture governs, and the provenance says so.
//
// Every cell the enumeration produces is now settled. The UNRULED code stays in
// the vocabulary because a later round widening an axis will produce cells
// nothing rules yet; what may not happen is a cell quietly acquiring today's
// behavior as its expectation.
//
// The surfaces are read through the REBUILD (Root().Schema()), because that is
// the one that loses things. String() is the schema's own source text and
// carries every written key whatever the parse did, so asking it would report
// every accepted cell as preserved and prove nothing.
// ---------------------------------------------------------------------------

// Outcome codes, one character per body class in reservedBodyClasses order.
const (
	outReject  = 'R' // the parse rejects
	outKeep    = 'K' // the attribute survives to the rebuild
	outDropped = 'D' // accepted, and the attribute reaches no surface
	outAbsent  = 'A' // nothing was written
	outUnruled = '?' // recorded, not asserted: see the header
	outNA      = '-' // the body class does not apply to this attribute
)

var reservedBodyClasses = []string{"absent", "valid", "zero", "null", "wrong", "quoted"}

var reservedAttrs = []string{"type", "name", "namespace", "doc", "aliases", "fields",
	"items", "values", "symbols", "size", "default", "order", "logicalType",
	"precision", "scale"}

var reservedKinds = []string{"null", "boolean", "int", "long", "float", "double",
	"bytes", "string", "record", "error", "enum", "array", "map", "fixed"}

// reservedCellRow is one (attribute, level, kind) row; Outcomes holds one code
// per body class, in reservedBodyClasses order.
type reservedCellRow struct {
	Attr     string
	Level    string
	Kind     string
	Outcomes string
}

var reservedCellTable = []reservedCellRow{
	{"type", "type", "null", "RKRRR-"},
	{"type", "type", "boolean", "RKRRR-"},
	{"type", "type", "int", "RKRRR-"},
	{"type", "type", "long", "RKRRR-"},
	{"type", "type", "float", "RKRRR-"},
	{"type", "type", "double", "RKRRR-"},
	{"type", "type", "bytes", "RKRRR-"},
	{"type", "type", "string", "RKRRR-"},
	{"type", "type", "record", "RKRRR-"},
	{"type", "type", "error", "RKRRR-"},
	{"type", "type", "enum", "RKRRR-"},
	{"type", "type", "array", "RKRRR-"},
	{"type", "type", "map", "RKRRR-"},
	{"type", "type", "fixed", "RKRRR-"},
	{"type", "field", "null", "RKRRR-"},
	{"type", "field", "boolean", "RKRRR-"},
	{"type", "field", "int", "RKRRR-"},
	{"type", "field", "long", "RKRRR-"},
	{"type", "field", "float", "RKRRR-"},
	{"type", "field", "double", "RKRRR-"},
	{"type", "field", "bytes", "RKRRR-"},
	{"type", "field", "string", "RKRRR-"},
	{"type", "field", "record", "RRRRR-"},
	{"type", "field", "error", "RRRRR-"},
	{"type", "field", "enum", "RRRRR-"},
	{"type", "field", "array", "RRRRR-"},
	{"type", "field", "map", "RRRRR-"},
	{"type", "field", "fixed", "RRRRR-"},
	{"name", "type", "null", "AKKKK-"},
	{"name", "type", "boolean", "AKKKK-"},
	{"name", "type", "int", "AKKKK-"},
	{"name", "type", "long", "AKKKK-"},
	{"name", "type", "float", "AKKKK-"},
	{"name", "type", "double", "AKKKK-"},
	{"name", "type", "bytes", "AKKKK-"},
	{"name", "type", "string", "AKKKK-"},
	{"name", "type", "record", "RKRRR-"},
	{"name", "type", "error", "RKRRR-"},
	{"name", "type", "enum", "RKRRR-"},
	{"name", "type", "array", "ARKKK-"},
	{"name", "type", "map", "ARKKK-"},
	{"name", "type", "fixed", "RKRRR-"},
	{"name", "field", "null", "RKRRR-"},
	{"name", "field", "boolean", "RKRRR-"},
	{"name", "field", "int", "RKRRR-"},
	{"name", "field", "long", "RKRRR-"},
	{"name", "field", "float", "RKRRR-"},
	{"name", "field", "double", "RKRRR-"},
	{"name", "field", "bytes", "RKRRR-"},
	{"name", "field", "string", "RKRRR-"},
	{"name", "field", "record", "RKRRR-"},
	{"name", "field", "error", "RKRRR-"},
	{"name", "field", "enum", "RKRRR-"},
	{"name", "field", "array", "RKRRR-"},
	{"name", "field", "map", "RKRRR-"},
	{"name", "field", "fixed", "RKRRR-"},
	{"namespace", "type", "null", "AKKKK-"},
	{"namespace", "type", "boolean", "AKKKK-"},
	{"namespace", "type", "int", "AKKKK-"},
	{"namespace", "type", "long", "AKKKK-"},
	{"namespace", "type", "float", "AKKKK-"},
	{"namespace", "type", "double", "AKKKK-"},
	{"namespace", "type", "bytes", "AKKKK-"},
	{"namespace", "type", "string", "AKKKK-"},
	{"namespace", "type", "record", "AKDRR-"},
	{"namespace", "type", "error", "AKDRR-"},
	{"namespace", "type", "enum", "AKDRR-"},
	{"namespace", "type", "array", "AKKKK-"},
	{"namespace", "type", "map", "AKKKK-"},
	{"namespace", "type", "fixed", "AKDRR-"},
	{"namespace", "field", "null", "AKKKK-"},
	{"namespace", "field", "boolean", "AKKKK-"},
	{"namespace", "field", "int", "AKKKK-"},
	{"namespace", "field", "long", "AKKKK-"},
	{"namespace", "field", "float", "AKKKK-"},
	{"namespace", "field", "double", "AKKKK-"},
	{"namespace", "field", "bytes", "AKKKK-"},
	{"namespace", "field", "string", "AKKKK-"},
	{"namespace", "field", "record", "AKKKK-"},
	{"namespace", "field", "error", "AKKKK-"},
	{"namespace", "field", "enum", "AKKKK-"},
	{"namespace", "field", "array", "AKKKK-"},
	{"namespace", "field", "map", "AKKKK-"},
	{"namespace", "field", "fixed", "AKKKK-"},
	{"doc", "type", "null", "AKKDD-"},
	{"doc", "type", "boolean", "AKKDD-"},
	{"doc", "type", "int", "AKKDD-"},
	{"doc", "type", "long", "AKKDD-"},
	{"doc", "type", "float", "AKKDD-"},
	{"doc", "type", "double", "AKKDD-"},
	{"doc", "type", "bytes", "AKKDD-"},
	{"doc", "type", "string", "AKKDD-"},
	{"doc", "type", "record", "AKKDD-"},
	{"doc", "type", "error", "AKKDD-"},
	{"doc", "type", "enum", "AKKDD-"},
	{"doc", "type", "array", "AKKDD-"},
	{"doc", "type", "map", "AKKDD-"},
	{"doc", "type", "fixed", "AKKDD-"},
	{"doc", "field", "null", "AKKDD-"},
	{"doc", "field", "boolean", "AKKDD-"},
	{"doc", "field", "int", "AKKDD-"},
	{"doc", "field", "long", "AKKDD-"},
	{"doc", "field", "float", "AKKDD-"},
	{"doc", "field", "double", "AKKDD-"},
	{"doc", "field", "bytes", "AKKDD-"},
	{"doc", "field", "string", "AKKDD-"},
	{"doc", "field", "record", "AKKDD-"},
	{"doc", "field", "error", "AKKDD-"},
	{"doc", "field", "enum", "AKKDD-"},
	{"doc", "field", "array", "AKKDD-"},
	{"doc", "field", "map", "AKKDD-"},
	{"doc", "field", "fixed", "AKKDD-"},
	{"aliases", "type", "null", "AKKKK-"},
	{"aliases", "type", "boolean", "AKKKK-"},
	{"aliases", "type", "int", "AKKKK-"},
	{"aliases", "type", "long", "AKKKK-"},
	{"aliases", "type", "float", "AKKKK-"},
	{"aliases", "type", "double", "AKKKK-"},
	{"aliases", "type", "bytes", "AKKKK-"},
	{"aliases", "type", "string", "AKKKK-"},
	{"aliases", "type", "record", "AKDRR-"},
	{"aliases", "type", "error", "AKDRR-"},
	{"aliases", "type", "enum", "AKDRR-"},
	{"aliases", "type", "array", "AKKKK-"},
	{"aliases", "type", "map", "AKKKK-"},
	{"aliases", "type", "fixed", "AKDRR-"},
	{"aliases", "field", "null", "AKDRR-"},
	{"aliases", "field", "boolean", "AKDRR-"},
	{"aliases", "field", "int", "AKDRR-"},
	{"aliases", "field", "long", "AKDRR-"},
	{"aliases", "field", "float", "AKDRR-"},
	{"aliases", "field", "double", "AKDRR-"},
	{"aliases", "field", "bytes", "AKDRR-"},
	{"aliases", "field", "string", "AKDRR-"},
	{"aliases", "field", "record", "AKDRR-"},
	{"aliases", "field", "error", "AKDRR-"},
	{"aliases", "field", "enum", "AKDRR-"},
	{"aliases", "field", "array", "AKDRR-"},
	{"aliases", "field", "map", "AKDRR-"},
	{"aliases", "field", "fixed", "AKDRR-"},
	{"fields", "type", "null", "AKKKK-"},
	{"fields", "type", "boolean", "AKKKK-"},
	{"fields", "type", "int", "AKKKK-"},
	{"fields", "type", "long", "AKKKK-"},
	{"fields", "type", "float", "AKKKK-"},
	{"fields", "type", "double", "AKKKK-"},
	{"fields", "type", "bytes", "AKKKK-"},
	{"fields", "type", "string", "AKKKK-"},
	{"fields", "type", "record", "RKKRR-"},
	{"fields", "type", "error", "RKKRR-"},
	{"fields", "type", "enum", "ARKKK-"},
	{"fields", "type", "array", "ARKKK-"},
	{"fields", "type", "map", "ARKKK-"},
	{"fields", "type", "fixed", "ARKKK-"},
	{"fields", "field", "null", "AKKKK-"},
	{"fields", "field", "boolean", "AKKKK-"},
	{"fields", "field", "int", "AKKKK-"},
	{"fields", "field", "long", "AKKKK-"},
	{"fields", "field", "float", "AKKKK-"},
	{"fields", "field", "double", "AKKKK-"},
	{"fields", "field", "bytes", "AKKKK-"},
	{"fields", "field", "string", "AKKKK-"},
	{"fields", "field", "record", "AKKKK-"},
	{"fields", "field", "error", "AKKKK-"},
	{"fields", "field", "enum", "AKKKK-"},
	{"fields", "field", "array", "AKKKK-"},
	{"fields", "field", "map", "AKKKK-"},
	{"fields", "field", "fixed", "AKKKK-"},
	{"items", "type", "null", "AKKKK-"},
	{"items", "type", "boolean", "AKKKK-"},
	{"items", "type", "int", "AKKKK-"},
	{"items", "type", "long", "AKKKK-"},
	{"items", "type", "float", "AKKKK-"},
	{"items", "type", "double", "AKKKK-"},
	{"items", "type", "bytes", "AKKKK-"},
	{"items", "type", "string", "AKKKK-"},
	{"items", "type", "record", "ARRKK-"},
	{"items", "type", "error", "ARRKK-"},
	{"items", "type", "enum", "ARRKK-"},
	{"items", "type", "array", "RKRRR-"},
	{"items", "type", "map", "ARRKK-"},
	{"items", "type", "fixed", "ARRKK-"},
	{"items", "field", "null", "AKKKK-"},
	{"items", "field", "boolean", "AKKKK-"},
	{"items", "field", "int", "AKKKK-"},
	{"items", "field", "long", "AKKKK-"},
	{"items", "field", "float", "AKKKK-"},
	{"items", "field", "double", "AKKKK-"},
	{"items", "field", "bytes", "AKKKK-"},
	{"items", "field", "string", "AKKKK-"},
	{"items", "field", "record", "AKKKK-"},
	{"items", "field", "error", "AKKKK-"},
	{"items", "field", "enum", "AKKKK-"},
	{"items", "field", "array", "AKKKK-"},
	{"items", "field", "map", "AKKKK-"},
	{"items", "field", "fixed", "AKKKK-"},
	{"values", "type", "null", "AKKKK-"},
	{"values", "type", "boolean", "AKKKK-"},
	{"values", "type", "int", "AKKKK-"},
	{"values", "type", "long", "AKKKK-"},
	{"values", "type", "float", "AKKKK-"},
	{"values", "type", "double", "AKKKK-"},
	{"values", "type", "bytes", "AKKKK-"},
	{"values", "type", "string", "AKKKK-"},
	{"values", "type", "record", "ARRKK-"},
	{"values", "type", "error", "ARRKK-"},
	{"values", "type", "enum", "ARRKK-"},
	{"values", "type", "array", "ARRKK-"},
	{"values", "type", "map", "RKRRR-"},
	{"values", "type", "fixed", "ARRKK-"},
	{"values", "field", "null", "AKKKK-"},
	{"values", "field", "boolean", "AKKKK-"},
	{"values", "field", "int", "AKKKK-"},
	{"values", "field", "long", "AKKKK-"},
	{"values", "field", "float", "AKKKK-"},
	{"values", "field", "double", "AKKKK-"},
	{"values", "field", "bytes", "AKKKK-"},
	{"values", "field", "string", "AKKKK-"},
	{"values", "field", "record", "AKKKK-"},
	{"values", "field", "error", "AKKKK-"},
	{"values", "field", "enum", "AKKKK-"},
	{"values", "field", "array", "AKKKK-"},
	{"values", "field", "map", "AKKKK-"},
	{"values", "field", "fixed", "AKKKK-"},
	{"symbols", "type", "null", "AKKKK-"},
	{"symbols", "type", "boolean", "AKKKK-"},
	{"symbols", "type", "int", "AKKKK-"},
	{"symbols", "type", "long", "AKKKK-"},
	{"symbols", "type", "float", "AKKKK-"},
	{"symbols", "type", "double", "AKKKK-"},
	{"symbols", "type", "bytes", "AKKKK-"},
	{"symbols", "type", "string", "AKKKK-"},
	{"symbols", "type", "record", "ARKKK-"},
	{"symbols", "type", "error", "ARKKK-"},
	{"symbols", "type", "enum", "RKKRR-"},
	{"symbols", "type", "array", "ARKKK-"},
	{"symbols", "type", "map", "ARKKK-"},
	{"symbols", "type", "fixed", "ARKKK-"},
	{"symbols", "field", "null", "AKKKK-"},
	{"symbols", "field", "boolean", "AKKKK-"},
	{"symbols", "field", "int", "AKKKK-"},
	{"symbols", "field", "long", "AKKKK-"},
	{"symbols", "field", "float", "AKKKK-"},
	{"symbols", "field", "double", "AKKKK-"},
	{"symbols", "field", "bytes", "AKKKK-"},
	{"symbols", "field", "string", "AKKKK-"},
	{"symbols", "field", "record", "AKKKK-"},
	{"symbols", "field", "error", "AKKKK-"},
	{"symbols", "field", "enum", "AKKKK-"},
	{"symbols", "field", "array", "AKKKK-"},
	{"symbols", "field", "map", "AKKKK-"},
	{"symbols", "field", "fixed", "AKKKK-"},
	{"size", "type", "null", "AKKKKK"},
	{"size", "type", "boolean", "AKKKKK"},
	{"size", "type", "int", "AKKKKK"},
	{"size", "type", "long", "AKKKKK"},
	{"size", "type", "float", "AKKKKK"},
	{"size", "type", "double", "AKKKKK"},
	{"size", "type", "bytes", "AKKKKK"},
	{"size", "type", "string", "AKKKKK"},
	{"size", "type", "record", "ARRKKR"},
	{"size", "type", "error", "ARRKKR"},
	{"size", "type", "enum", "ARRKKR"},
	{"size", "type", "array", "ARRKKR"},
	{"size", "type", "map", "ARRKKR"},
	{"size", "type", "fixed", "RKKRRK"},
	{"size", "field", "null", "AKKKKK"},
	{"size", "field", "boolean", "AKKKKK"},
	{"size", "field", "int", "AKKKKK"},
	{"size", "field", "long", "AKKKKK"},
	{"size", "field", "float", "AKKKKK"},
	{"size", "field", "double", "AKKKKK"},
	{"size", "field", "bytes", "AKKKKK"},
	{"size", "field", "string", "AKKKKK"},
	{"size", "field", "record", "AKKKKK"},
	{"size", "field", "error", "AKKKKK"},
	{"size", "field", "enum", "AKKKKK"},
	{"size", "field", "array", "AKKKKK"},
	{"size", "field", "map", "AKKKKK"},
	{"size", "field", "fixed", "AKKKKK"},
	{"default", "type", "null", "AKKKK-"},
	{"default", "type", "boolean", "AKKKK-"},
	{"default", "type", "int", "AKKKK-"},
	{"default", "type", "long", "AKKKK-"},
	{"default", "type", "float", "AKKKK-"},
	{"default", "type", "double", "AKKKK-"},
	{"default", "type", "bytes", "AKKKK-"},
	{"default", "type", "string", "AKKKK-"},
	{"default", "type", "record", "AKKKK-"},
	{"default", "type", "error", "AKKKK-"},
	{"default", "type", "enum", "AKRRR-"},
	{"default", "type", "array", "AKKKK-"},
	{"default", "type", "map", "AKKKK-"},
	{"default", "type", "fixed", "AKKKK-"},
	{"default", "field", "null", "AKKKR-"},
	{"default", "field", "boolean", "AKKRR-"},
	{"default", "field", "int", "AKKRR-"},
	{"default", "field", "long", "AKKRR-"},
	{"default", "field", "float", "AKKRR-"},
	{"default", "field", "double", "AKKRR-"},
	{"default", "field", "bytes", "AKKRR-"},
	{"default", "field", "string", "AKKRR-"},
	{"default", "field", "record", "AKRRR-"},
	{"default", "field", "error", "AKRRR-"},
	{"default", "field", "enum", "AKRRR-"},
	{"default", "field", "array", "AKKRR-"},
	{"default", "field", "map", "AKKRR-"},
	{"default", "field", "fixed", "AKRRR-"},
	{"order", "type", "null", "AKKKK-"},
	{"order", "type", "boolean", "AKKKK-"},
	{"order", "type", "int", "AKKKK-"},
	{"order", "type", "long", "AKKKK-"},
	{"order", "type", "float", "AKKKK-"},
	{"order", "type", "double", "AKKKK-"},
	{"order", "type", "bytes", "AKKKK-"},
	{"order", "type", "string", "AKKKK-"},
	{"order", "type", "record", "AKKKK-"},
	{"order", "type", "error", "AKKKK-"},
	{"order", "type", "enum", "AKKKK-"},
	{"order", "type", "array", "AKKKK-"},
	{"order", "type", "map", "AKKKK-"},
	{"order", "type", "fixed", "AKKKK-"},
	{"order", "field", "null", "AKRRR-"},
	{"order", "field", "boolean", "AKRRR-"},
	{"order", "field", "int", "AKRRR-"},
	{"order", "field", "long", "AKRRR-"},
	{"order", "field", "float", "AKRRR-"},
	{"order", "field", "double", "AKRRR-"},
	{"order", "field", "bytes", "AKRRR-"},
	{"order", "field", "string", "AKRRR-"},
	{"order", "field", "record", "AKRRR-"},
	{"order", "field", "error", "AKRRR-"},
	{"order", "field", "enum", "AKRRR-"},
	{"order", "field", "array", "AKRRR-"},
	{"order", "field", "map", "AKRRR-"},
	{"order", "field", "fixed", "AKRRR-"},
	{"logicalType", "type", "null", "AKKKK-"},
	{"logicalType", "type", "boolean", "AKKKK-"},
	{"logicalType", "type", "int", "AKKKK-"},
	{"logicalType", "type", "long", "AKKKK-"},
	{"logicalType", "type", "float", "AKKKK-"},
	{"logicalType", "type", "double", "AKKKK-"},
	{"logicalType", "type", "bytes", "ARKKK-"},
	{"logicalType", "type", "string", "AKKKK-"},
	{"logicalType", "type", "record", "AKKKK-"},
	{"logicalType", "type", "error", "AKKKK-"},
	{"logicalType", "type", "enum", "AKKKK-"},
	{"logicalType", "type", "array", "AKKKK-"},
	{"logicalType", "type", "map", "AKKKK-"},
	{"logicalType", "type", "fixed", "AKKKK-"},
	{"logicalType", "field", "null", "AKKKK-"},
	{"logicalType", "field", "boolean", "AKKKK-"},
	{"logicalType", "field", "int", "AKKKK-"},
	{"logicalType", "field", "long", "AKKKK-"},
	{"logicalType", "field", "float", "AKKKK-"},
	{"logicalType", "field", "double", "AKKKK-"},
	{"logicalType", "field", "bytes", "ARKKK-"},
	{"logicalType", "field", "string", "AKKKK-"},
	{"logicalType", "field", "record", "AKKKK-"},
	{"logicalType", "field", "error", "AKKKK-"},
	{"logicalType", "field", "enum", "AKKKK-"},
	{"logicalType", "field", "array", "AKKKK-"},
	{"logicalType", "field", "map", "AKKKK-"},
	{"logicalType", "field", "fixed", "AKKKK-"},
	{"precision", "type", "null", "AKKKKK"},
	{"precision", "type", "boolean", "AKKKKK"},
	{"precision", "type", "int", "AKKKKK"},
	{"precision", "type", "long", "AKKKKK"},
	{"precision", "type", "float", "AKKKKK"},
	{"precision", "type", "double", "AKKKKK"},
	{"precision", "type", "bytes", "AKKKKK"},
	{"precision", "type", "string", "AKKKKK"},
	{"precision", "type", "record", "AKKKKK"},
	{"precision", "type", "error", "AKKKKK"},
	{"precision", "type", "enum", "AKKKKK"},
	{"precision", "type", "array", "AKKKKK"},
	{"precision", "type", "map", "AKKKKK"},
	{"precision", "type", "fixed", "AKKKKK"},
	{"precision", "field", "null", "AKKKKK"},
	{"precision", "field", "boolean", "AKKKKK"},
	{"precision", "field", "int", "AKKKKK"},
	{"precision", "field", "long", "AKKKKK"},
	{"precision", "field", "float", "AKKKKK"},
	{"precision", "field", "double", "AKKKKK"},
	{"precision", "field", "bytes", "AKKKKK"},
	{"precision", "field", "string", "AKKKKK"},
	{"precision", "field", "record", "AKKKKK"},
	{"precision", "field", "error", "AKKKKK"},
	{"precision", "field", "enum", "AKKKKK"},
	{"precision", "field", "array", "AKKKKK"},
	{"precision", "field", "map", "AKKKKK"},
	{"precision", "field", "fixed", "AKKKKK"},
	{"scale", "type", "null", "AKKKKK"},
	{"scale", "type", "boolean", "AKKKKK"},
	{"scale", "type", "int", "AKKKKK"},
	{"scale", "type", "long", "AKKKKK"},
	{"scale", "type", "float", "AKKKKK"},
	{"scale", "type", "double", "AKKKKK"},
	{"scale", "type", "bytes", "AKKKKK"},
	{"scale", "type", "string", "AKKKKK"},
	{"scale", "type", "record", "AKKKKK"},
	{"scale", "type", "error", "AKKKKK"},
	{"scale", "type", "enum", "AKKKKK"},
	{"scale", "type", "array", "AKKKKK"},
	{"scale", "type", "map", "AKKKKK"},
	{"scale", "type", "fixed", "AKKKKK"},
	{"scale", "field", "null", "AKKKKK"},
	{"scale", "field", "boolean", "AKKKKK"},
	{"scale", "field", "int", "AKKKKK"},
	{"scale", "field", "long", "AKKKKK"},
	{"scale", "field", "float", "AKKKKK"},
	{"scale", "field", "double", "AKKKKK"},
	{"scale", "field", "bytes", "AKKKKK"},
	{"scale", "field", "string", "AKKKKK"},
	{"scale", "field", "record", "AKKKKK"},
	{"scale", "field", "error", "AKKKKK"},
	{"scale", "field", "enum", "AKKKKK"},
	{"scale", "field", "array", "AKKKKK"},
	{"scale", "field", "map", "AKKKKK"},
	{"scale", "field", "fixed", "AKKKKK"},
}

// reservedProvenance records, per (attribute, body class, level), what settled
// the expectation. It is data, not decoration: a cell whose provenance says
// "unruled" must not be treated as pinned behavior, and a cell that says
// "follows-fastavro" is one where this package chose the permissive side of a
// reference disagreement and can be revisited on new evidence.
type reservedProvRow struct {
	Attr, Body, Level, By string
}

var reservedProvenance = []reservedProvRow{
	{"aliases", "absent", "field", "java-model"},
	{"aliases", "absent", "type", "java-model"},
	{"aliases", "null", "field", "both-references"},
	{"aliases", "null", "type", "follows-java|standing-ruling"},
	{"aliases", "valid", "field", "both-references"},
	{"aliases", "valid", "type", "both-references|standing-ruling"},
	{"aliases", "wrong", "field", "both-references"},
	{"aliases", "wrong", "type", "follows-java|standing-ruling"},
	{"aliases", "zero", "field", "follows-java"},
	{"aliases", "zero", "type", "placement-authority(stray-routing)"},
	{"default", "absent", "field", "java-model"},
	{"default", "absent", "type", "java-model"},
	{"default", "null", "field", "both-references|follows-fastavro"},
	{"default", "null", "type", "follows-fastavro|standing-ruling"},
	{"default", "valid", "field", "both-references"},
	{"default", "valid", "type", "both-references|standing-ruling"},
	{"default", "wrong", "field", "follows-fastavro"},
	{"default", "wrong", "type", "follows-fastavro|standing-ruling"},
	{"default", "zero", "field", "both-references|documented-divergence|follows-java"},
	{"default", "zero", "type", "both-references|standing-ruling"},
	{"doc", "absent", "field", "java-model"},
	{"doc", "absent", "type", "java-model"},
	{"doc", "null", "field", "follows-java"},
	{"doc", "null", "type", "follows-java|placement-authority(fastavro-has-the-placement)"},
	{"doc", "valid", "field", "both-references"},
	{"doc", "valid", "type", "both-references|placement-authority(fastavro-has-the-placement)"},
	{"doc", "wrong", "field", "follows-java"},
	{"doc", "wrong", "type", "follows-java|placement-authority(fastavro-has-the-placement)"},
	{"doc", "zero", "field", "both-references"},
	{"doc", "zero", "type", "both-references|placement-authority(fastavro-has-the-placement)"},
	{"fields", "absent", "field", "java-model"},
	{"fields", "absent", "type", "java-model"},
	{"fields", "null", "field", "standing-ruling"},
	{"fields", "null", "type", "both-references|standing-ruling"},
	{"fields", "valid", "field", "standing-ruling"},
	{"fields", "valid", "type", "both-references|standing-ruling"},
	{"fields", "wrong", "field", "standing-ruling"},
	{"fields", "wrong", "type", "both-references|standing-ruling"},
	{"fields", "zero", "field", "standing-ruling"},
	{"fields", "zero", "type", "placement-authority(stray-routing)"},
	{"items", "absent", "field", "java-model"},
	{"items", "absent", "type", "java-model"},
	{"items", "null", "field", "standing-ruling"},
	{"items", "null", "type", "both-references|standing-ruling"},
	{"items", "valid", "field", "standing-ruling"},
	{"items", "valid", "type", "both-references|standing-ruling"},
	{"items", "wrong", "field", "standing-ruling"},
	{"items", "wrong", "type", "both-references|standing-ruling"},
	{"items", "zero", "field", "standing-ruling"},
	{"items", "zero", "type", "both-references|standing-ruling"},
	{"logicalType", "absent", "field", "java-model"},
	{"logicalType", "absent", "type", "java-model"},
	{"logicalType", "null", "field", "standing-ruling"},
	{"logicalType", "null", "type", "both-references"},
	{"logicalType", "valid", "field", "standing-ruling"},
	{"logicalType", "valid", "type", "both-references|documented-divergence"},
	{"logicalType", "wrong", "field", "standing-ruling"},
	{"logicalType", "wrong", "type", "both-references"},
	{"logicalType", "zero", "field", "standing-ruling"},
	{"logicalType", "zero", "type", "both-references"},
	{"name", "absent", "field", "java-model"},
	{"name", "absent", "type", "java-model"},
	{"name", "null", "field", "follows-java"},
	{"name", "null", "type", "both-references|standing-ruling"},
	{"name", "valid", "field", "both-references"},
	{"name", "valid", "type", "both-references|standing-ruling"},
	{"name", "wrong", "field", "follows-java"},
	{"name", "wrong", "type", "both-references|standing-ruling"},
	{"name", "zero", "field", "follows-java"},
	{"name", "zero", "type", "placement-authority(stray-routing)"},
	{"namespace", "absent", "field", "java-model"},
	{"namespace", "absent", "type", "java-model"},
	{"namespace", "null", "field", "standing-ruling"},
	{"namespace", "null", "type", "ruled(uniform-name-strictness)"},
	{"namespace", "valid", "field", "standing-ruling"},
	{"namespace", "valid", "type", "both-references|standing-ruling"},
	{"namespace", "wrong", "field", "standing-ruling"},
	{"namespace", "wrong", "type", "ruled(uniform-name-strictness)"},
	{"namespace", "zero", "field", "standing-ruling"},
	{"namespace", "zero", "type", "placement-authority(stray-routing)"},
	{"order", "absent", "field", "java-model"},
	{"order", "absent", "type", "java-model"},
	{"order", "null", "field", "follows-java"},
	{"order", "null", "type", "standing-ruling"},
	{"order", "valid", "field", "both-references"},
	{"order", "valid", "type", "standing-ruling"},
	{"order", "wrong", "field", "follows-java"},
	{"order", "wrong", "type", "standing-ruling"},
	{"order", "zero", "field", "follows-java"},
	{"order", "zero", "type", "standing-ruling"},
	{"precision", "absent", "field", "java-model"},
	{"precision", "absent", "type", "java-model"},
	{"precision", "null", "field", "standing-ruling"},
	{"precision", "null", "type", "both-references"},
	{"precision", "quoted", "field", "standing-ruling"},
	{"precision", "quoted", "type", "both-references"},
	{"precision", "valid", "field", "standing-ruling"},
	{"precision", "valid", "type", "both-references"},
	{"precision", "wrong", "field", "standing-ruling"},
	{"precision", "wrong", "type", "both-references"},
	{"precision", "zero", "field", "standing-ruling"},
	{"precision", "zero", "type", "both-references"},
	{"scale", "absent", "field", "java-model"},
	{"scale", "absent", "type", "java-model"},
	{"scale", "null", "field", "standing-ruling"},
	{"scale", "null", "type", "both-references"},
	{"scale", "quoted", "field", "standing-ruling"},
	{"scale", "quoted", "type", "both-references"},
	{"scale", "valid", "field", "standing-ruling"},
	{"scale", "valid", "type", "both-references"},
	{"scale", "wrong", "field", "standing-ruling"},
	{"scale", "wrong", "type", "both-references"},
	{"scale", "zero", "field", "standing-ruling"},
	{"scale", "zero", "type", "both-references"},
	{"size", "absent", "field", "java-model"},
	{"size", "absent", "type", "java-model"},
	{"size", "null", "field", "standing-ruling"},
	{"size", "null", "type", "follows-java|standing-ruling"},
	{"size", "quoted", "field", "standing-ruling"},
	{"size", "quoted", "type", "follows-fastavro|standing-ruling"},
	{"size", "valid", "field", "standing-ruling"},
	{"size", "valid", "type", "both-references|standing-ruling"},
	{"size", "wrong", "field", "standing-ruling"},
	{"size", "wrong", "type", "follows-java|standing-ruling"},
	{"size", "zero", "field", "standing-ruling"},
	{"size", "zero", "type", "placement-authority(stray-routing)"},
	{"symbols", "absent", "field", "java-model"},
	{"symbols", "absent", "type", "java-model"},
	{"symbols", "null", "field", "standing-ruling"},
	{"symbols", "null", "type", "both-references|standing-ruling"},
	{"symbols", "valid", "field", "standing-ruling"},
	{"symbols", "valid", "type", "both-references|standing-ruling"},
	{"symbols", "wrong", "field", "standing-ruling"},
	{"symbols", "wrong", "type", "both-references|standing-ruling"},
	{"symbols", "zero", "field", "standing-ruling"},
	{"symbols", "zero", "type", "placement-authority(stray-routing)"},
	{"type", "absent", "field", "java-model"},
	{"type", "absent", "type", "java-model"},
	{"type", "null", "field", "both-references"},
	{"type", "null", "type", "both-references"},
	{"type", "valid", "field", "both-references"},
	{"type", "valid", "type", "both-references"},
	{"type", "wrong", "field", "both-references"},
	{"type", "wrong", "type", "both-references"},
	{"type", "zero", "field", "both-references"},
	{"type", "zero", "type", "both-references"},
	{"values", "absent", "field", "java-model"},
	{"values", "absent", "type", "java-model"},
	{"values", "null", "field", "standing-ruling"},
	{"values", "null", "type", "both-references|standing-ruling"},
	{"values", "valid", "field", "standing-ruling"},
	{"values", "valid", "type", "both-references|standing-ruling"},
	{"values", "wrong", "field", "standing-ruling"},
	{"values", "wrong", "type", "both-references|standing-ruling"},
	{"values", "zero", "field", "standing-ruling"},
	{"values", "zero", "type", "both-references|standing-ruling"},
}

// reservedBody renders the attribute's body for one class, and reports
// whether the class applies to this attribute at all. The ZERO body is the
// JSON zero of the attribute's DESTINATION — "" for a string, [] for an
// array, {} for an object, 0 for an int — which is the whole point of the
// axis: an attribute written as its destination's zero is written, and a
// reader that tests the value alone cannot see it.
func reservedBody(attr, kind, class string) (string, bool) {
	valid, zero, wrong, quoted := "", "", "", ""
	switch attr {
	case "type":
		valid, zero, wrong = `"`+kind+`"`, `""`, "5"
	case "name":
		valid, zero, wrong = `"Nm"`, `""`, "5"
	case "namespace":
		valid, zero, wrong = `"ns"`, `""`, "5"
	case "doc":
		valid, zero, wrong = `"d"`, `""`, "5"
	case "aliases", "symbols":
		valid, zero, wrong = `["A"]`, "[]", "5"
	case "fields":
		valid, zero, wrong = `[{"name":"z","type":"int"}]`, "[]", "5"
	case "items", "values":
		valid, zero, wrong = `"int"`, "{}", "5"
	case "size", "scale":
		valid, zero, wrong, quoted = "2", "0", "[]", `"2"`
	case "precision":
		valid, zero, wrong, quoted = "4", "0", "[]", `"4"`
	case "order":
		valid, zero, wrong = `"ignore"`, `""`, "5"
	case "logicalType":
		valid, zero, wrong = reservedLogicalFor(kind), `""`, "5"
	case "default":
		valid, zero, wrong = reservedDefaultFor(kind)
	}
	switch class {
	case "absent":
		return "", true
	case "valid":
		return valid, true
	case "zero":
		return zero, true
	case "null":
		return "null", true
	case "wrong":
		return wrong, true
	case "quoted":
		return quoted, quoted != ""
	}
	return "", false
}

func reservedLogicalFor(kind string) string {
	switch kind {
	case "long":
		return `"timestamp-millis"`
	case "bytes":
		return `"decimal"`
	case "fixed":
		return `"duration"`
	case "string":
		return `"uuid"`
	}
	return `"date"`
}

// reservedDefaultFor gives a value the kind can take, the kind's zero value,
// and a token of a JSON class it can never take.
func reservedDefaultFor(kind string) (valid, zero, wrong string) {
	wrong = "[]"
	switch kind {
	case "null":
		return "null", "null", wrong
	case "boolean":
		return "true", "false", wrong
	case "int", "long":
		return "3", "0", wrong
	case "float", "double":
		return "1.5", "0", wrong
	case "bytes", "string":
		return `"s"`, `""`, wrong
	case "record", "error":
		return `{"z":0}`, "{}", wrong
	case "enum":
		return `"A"`, `""`, wrong
	case "array":
		return "[]", "[]", `"s"`
	case "map":
		return "{}", "{}", wrong
	case "fixed":
		return `"AAAA"`, `""`, wrong
	}
	return "null", "null", wrong
}

// reservedBaseKeys is the kind's own object, as an ordered key list so the
// generated text is deterministic. The ABSENT body DELETES the attribute from
// it: several of these keys are required by their kind, so leaving the base's
// own copy in place would silently test the valid body twice.
func reservedBaseKeys(kind string) [][2]string {
	switch kind {
	case "record", "error":
		return [][2]string{{"type", `"` + kind + `"`}, {"name", `"K"`},
			{"fields", `[{"name":"z","type":"int"}]`}}
	case "enum":
		return [][2]string{{"type", `"enum"`}, {"name", `"K"`}, {"symbols", `["A","B"]`}}
	case "fixed":
		return [][2]string{{"type", `"fixed"`}, {"name", `"K"`}, {"size", "4"}}
	case "array":
		return [][2]string{{"type", `"array"`}, {"items", `"int"`}}
	case "map":
		return [][2]string{{"type", `"map"`}, {"values", `"int"`}}
	}
	return [][2]string{{"type", `"` + kind + `"`}}
}

// reservedCellSchema builds the whole schema for one cell. Every cell is a
// host record with one field whose type is the kind under test, so the
// type-level and field-level spellings differ only in WHICH object carries
// the attribute.
func reservedCellSchema(attr, kind, level, class string) (string, bool) {
	body, applies := reservedBody(attr, kind, class)
	if !applies {
		return "", false
	}
	typeKeys := reservedBaseKeys(kind)
	fieldKeys := [][2]string{{"name", `"a"`}}
	set := func(keys [][2]string) [][2]string {
		out := keys[:0:0]
		for _, kv := range keys {
			if kv[0] != attr {
				out = append(out, kv)
			}
		}
		if class != "absent" {
			out = append(out, [2]string{attr, body})
		}
		return out
	}
	if level == "type" {
		typeKeys = set(typeKeys)
	} else {
		fieldKeys = set(fieldKeys)
	}
	obj := func(keys [][2]string) string {
		var b strings.Builder
		b.WriteByte('{')
		for i, kv := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "%q:%s", kv[0], kv[1])
		}
		b.WriteByte('}')
		return b.String()
	}
	// The field's "type" key is the field's schema, so a field-level cell
	// writing "type" REPLACES it rather than adding a second attribute —
	// which is the real question at that level.
	inner := obj(typeKeys)
	hasOwnType := false
	for _, kv := range fieldKeys {
		if kv[0] == "type" {
			hasOwnType = true
		}
	}
	if !hasOwnType && !(level == "field" && attr == "type" && class == "absent") {
		fieldKeys = append(fieldKeys, [2]string{"type", inner})
	}
	return `{"type":"record","name":"Host","fields":[` + obj(fieldKeys) + `]}`, true
}

// reservedCellOutcome reads the cell's outcome off the surface that can lose
// an attribute.
func reservedCellOutcome(t *testing.T, attr, level, src, class string) byte {
	t.Helper()
	s, err := avro.Parse(src)
	if err != nil {
		return outReject
	}
	if class == "absent" {
		return outAbsent
	}
	root := s.Root()
	rb, err := root.Schema()
	if err != nil {
		return outDropped
	}
	if reservedCarrierHasKey(rb.String(), attr, level) {
		return outKeep
	}
	return outDropped
}

// reservedCarrierHasKey walks a rendered schema down to the object the cell
// wrote its attribute on and reports whether that object still carries it.
func reservedCarrierHasKey(text, attr, level string) bool {
	var host map[string]any
	if json.Unmarshal([]byte(text), &host) != nil {
		return false
	}
	fs, _ := host["fields"].([]any)
	if len(fs) == 0 {
		return false
	}
	f, _ := fs[0].(map[string]any)
	if f == nil {
		return false
	}
	obj := f
	if level == "type" {
		if _, bare := f["type"].(string); bare {
			// The type object collapsed to its bare name: that IS the type
			// attribute surviving, and nothing else did.
			return attr == "type"
		}
		obj, _ = f["type"].(map[string]any)
		if obj == nil {
			return false
		}
	}
	_, ok := obj[attr]
	return ok
}

// TestMatrix_ReservedAttributeEnumeration drives every cell of the cross
// product against the reference-derived expectation.
func TestMatrix_ReservedAttributeEnumeration(t *testing.T) {
	var checked, unruled int
	for _, row := range reservedCellTable {
		if len(row.Outcomes) != len(reservedBodyClasses) {
			t.Fatalf("row %s/%s/%s has %d outcomes, want one per body class",
				row.Attr, row.Level, row.Kind, len(row.Outcomes))
		}
		for i, class := range reservedBodyClasses {
			want := row.Outcomes[i]
			src, applies := reservedCellSchema(row.Attr, row.Kind, row.Level, class)
			if want == outNA {
				if applies {
					t.Errorf("%s/%s/%s/%s is marked not-applicable but the corpus produces a schema for it",
						row.Attr, row.Level, row.Kind, class)
				}
				continue
			}
			if !applies {
				t.Errorf("%s/%s/%s/%s expects %q but the corpus produces no schema for it",
					row.Attr, row.Level, row.Kind, class, string(want))
				continue
			}
			got := reservedCellOutcome(t, row.Attr, row.Level, src, class)
			if want == outUnruled {
				unruled++
				// Recorded, not asserted: the cell is real and its answer is
				// open. Pinning today's behavior would convert a question
				// nothing settles into a decision nobody made.
				continue
			}
			checked++
			if got != want {
				t.Errorf("%s/%s/%s/%s = %q, want %q\n  schema: %s",
					row.Attr, row.Level, row.Kind, class, string(got), string(want), src)
			}
		}
	}
	t.Logf("reserved-attribute enumeration: %d cells asserted, %d recorded unruled", checked, unruled)
	if checked < 2000 {
		t.Fatalf("only %d cells were asserted; the corpus is not spanning the cross product", checked)
	}
	if unruled != 0 {
		t.Errorf("%d cells are unruled; every cell this corpus produces is settled, so a new one is a ruling to make rather than a gap to leave", unruled)
	}
}

// TestMatrix_ReservedAttributeEnumerationIsNotVacuous fails when the corpus
// stops spanning the axes, or when its outcomes collapse toward one answer —
// either would let an implementation pass by never being asked.
func TestMatrix_ReservedAttributeEnumerationIsNotVacuous(t *testing.T) {
	attrs, kinds, levels := map[string]int{}, map[string]int{}, map[string]int{}
	codes := map[byte]int{}
	for _, row := range reservedCellTable {
		attrs[row.Attr]++
		kinds[row.Kind]++
		levels[row.Level]++
		for i := range reservedBodyClasses {
			codes[row.Outcomes[i]]++
		}
	}
	if len(attrs) != len(reservedAttrs) {
		t.Errorf("corpus covers %d attributes, the axis names %d", len(attrs), len(reservedAttrs))
	}
	if len(kinds) != len(reservedKinds) {
		t.Errorf("corpus covers %d kinds, the axis names %d", len(kinds), len(reservedKinds))
	}
	if len(levels) != 2 {
		t.Errorf("corpus covers %d levels, want both", len(levels))
	}
	// Every outcome must be REACHED. A table that only ever expects "keep"
	// would pass against an implementation that accepted and preserved
	// everything, which is exactly the failure this enumeration exists to
	// catch on the other side.
	for _, c := range []byte{outReject, outKeep, outDropped, outAbsent} {
		if codes[c] < 50 {
			t.Errorf("outcome %q appears %d times; the table has collapsed toward one answer", string(c), codes[c])
		}
	}
	// Every cell is settled. A table that grew an unruled one has produced a
	// question, which is fine — but it must be answered rather than left, so
	// the driver above reports it.
	if codes[outUnruled] != 0 {
		t.Errorf("%d cells carry the unruled code", codes[outUnruled])
	}
	// Provenance must exist for every (attribute, body, level) family the
	// corpus produces, or a cell's authority is unrecorded.
	have := map[string]bool{}
	for _, p := range reservedProvenance {
		have[p.Attr+"/"+p.Body+"/"+p.Level] = true
	}
	for _, row := range reservedCellTable {
		for i, class := range reservedBodyClasses {
			if row.Outcomes[i] == outNA {
				continue
			}
			if !have[row.Attr+"/"+class+"/"+row.Level] {
				t.Errorf("no provenance recorded for %s/%s/%s", row.Attr, class, row.Level)
			}
		}
	}
}

// ---------- reserved_dup_matrix_test.go ----------

// Reserved keys match ONLY their exact lowercase spelling, so a case-variant
// ("ITEMS" beside "items") is an ordinary custom property — two distinct JSON
// keys with no contention over the reserved slot. The routing invariant: the
// exact spelling alone is consulted for structural binding and consumed, and
// EVERY other raw map key rides to Props verbatim with no branching on its body,
// so Props == all raw keys minus the consumed exact-lowercase ones. Both reading
// surfaces — the parse-side props handed to CustomType callbacks and the Root()
// metadata walk — apply the same rule and must agree. Java's reserved sets are
// exact-lowercase HashSets, and fastavro and goavro read known keys by exact name.

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
	// container/content, one wrong-JSON-kind scalar. The variant
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
// consumed-exact-key control).
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
	node := findNodeByType(root, carrier.typ, carrier.typName)
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
				t.Errorf("%s missing %q (case-variant reserved-key spellings are ordinary properties, preserved verbatim): %#v\ncell: %s", surface, wantKey, props, cell)
			} else if !reflect.DeepEqual(got, wantVal) {
				t.Errorf("%s[%q] = %#v (%T); want %#v (%T)\ncell: %s", surface, wantKey, got, got, wantVal, wantVal, cell)
			}
		} else {
			for k := range props {
				if strings.EqualFold(k, dropKey) {
					t.Errorf("%s carries a spelling of consumed key %q: %#v\ncell: %s", surface, k, props, cell)
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
	rbNode := findNodeByType(rbRoot, carrier.typ, carrier.typName)
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
// has a structural consumption arm × spellings-present {exact-only,
// exact+case-variant} × the variant's body {valid for the key's shape,
// malformed, non-schema scalar} × carrier {binding kind, non-binding
// stray placement} × reading surface {parse-side callback Props, Root()
// Props, Root().Schema() rebuild}. The variant spelling must ride to
// Props verbatim in every cell — its body's shape must not change the
// routing — and the exact-only controls prove the consumed exact key
// never leaks into Props.
func TestMatrix_ReservedKeyDuplicateSpellings(t *testing.T) {
	t.Parallel()
	for _, row := range reservedDupRows() {
		variant := strings.ToUpper(row.key)
		for _, carrier := range row.carriers {
			t.Run(row.key+"/"+carrier.label+"/exact-only", func(t *testing.T) {
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

// The variant-spelling rule is body-independent in the other direction
// too: when the EXACT key's body is malformed on a non-binding kind (so
// the exact key itself rides to Props), the variant's valid body still
// rides to Props — nothing is silently dropped and nothing is promoted
// to the structural field.
func TestMatrix_ReservedKeyDuplicateExactMalformed(t *testing.T) {
	t.Parallel()
	s := mustParse(t, `{"type":"int","items":12,"ITEMS":"int"}`)
	root := s.Root()
	if root.Items != nil {
		t.Errorf("malformed exact-key body must not surface structurally: %+v", root.Items)
	}
	props := root.Props
	if got := props["items"]; !reflect.DeepEqual(got, int64(12)) {
		t.Errorf(`Props["items"] = %#v; want 12 (malformed exact-key body rides to Props)`, got)
	}
	if got := props["ITEMS"]; !reflect.DeepEqual(got, "int") {
		t.Errorf(`Props["ITEMS"] = %#v; want "int" (variant spelling rides to Props verbatim)`, got)
	}
}

// A malformed body under a case-variant spelling must ride to Props on
// every surface; the exact key's shape-OK verdict must never leak onto
// it.
func TestRegression_ReservedDupMalformedVariantPreserved(t *testing.T) {
	t.Parallel()
	carrier := reservedDupCarrier{"stray", func(extra string) string {
		return `{"type":"int","items":"int"` + extra + `}`
	}, int32(1), "int", ""}
	checkReservedDupCell(t, carrier.build(`,"ITEMS":12`), carrier, "ITEMS", int64(12), "")
}

// A case-variant spelling whose body is ITSELF a valid schema shape also
// rides to Props: only the exact spelling is consulted for structural
// surfacing, and the single structural slot is the exact key's. Uniform
// preservation is the rule — a valid-shaped variant body must not be
// silently dropped or promoted.
func TestRegression_ReservedDupValidVariantPreserved(t *testing.T) {
	t.Parallel()
	carrier := reservedDupCarrier{"stray", func(extra string) string {
		return `{"type":"int","items":"int"` + extra + `}`
	}, int32(1), "int", ""}
	checkReservedDupCell(t, carrier.build(`,"ITEMS":"long"`), carrier, "ITEMS", "long", "")
	// The structural slot carries the exact key's body, not the variant's.
	s := mustParse(t, `{"type":"int","items":"int","ITEMS":"long"}`)
	root := s.Root()
	if root.Items == nil || root.Items.Type != "int" {
		t.Errorf("structural Items = %+v; want the exact key's body (int)", root.Items)
	}
}

// The parse-side props (surfaced to CustomType callbacks) and the Root()
// metadata walk must apply the identical routing rule — including for the
// reserved keys with non-recursive bodies (name/namespace/symbols/size/
// aliases), where the two surfaces have historically used separate code.
func TestRegression_ReservedDupParseMetadataPropsParity(t *testing.T) {
	t.Parallel()
	var captured map[string]any
	s := mustParse(t, `{"type":"int","name":"x","NAME":12}`, avro.WithCustomType(propsCaptureCustom("int", "", &captured)))
	mustAppendEncode(t, s, nil, int32(5))
	rootProps := s.Root().Props
	want := map[string]any{"NAME": int64(12)}
	if !reflect.DeepEqual(rootProps, want) {
		t.Errorf("Root().Props = %#v; want %#v", rootProps, want)
	}
	if !reflect.DeepEqual(captured, want) {
		t.Errorf("callback Props = %#v; want %#v (must equal Root().Props)", captured, want)
	}
}

// Field-level reserved keys follow the same rule: the exact spelling is
// consumed into the SchemaField attribute, every other spelling is an
// ordinary field property preserved in SchemaField.Props and by the
// rebuild.
func TestRegression_FieldReservedDupVariantPreserved(t *testing.T) {
	t.Parallel()
	s := mustParse(t, `{"type":"record","name":"FR","fields":[
		{"name":"f","type":"int","doc":"d","DOC":12}]}`)
	f := s.Root().Fields[0]
	if f.Doc != "d" {
		t.Errorf("Doc = %q; want the exact spelling's body", f.Doc)
	}
	if got := f.Props["DOC"]; !reflect.DeepEqual(got, int64(12)) {
		t.Errorf(`Props["DOC"] = %#v; want 12 (case-variant field reserved-key spelling preserved)`, got)
	}
	root := s.Root()
	rb := mustNodeSchema(t, root)
	if got := rb.Root().Fields[0].Props["DOC"]; !reflect.DeepEqual(got, int64(12)) {
		t.Errorf(`rebuild Props["DOC"] = %#v; want 12`, got)
	}
}

// TestMatrix_FieldReservedKeyDuplicateSpellings is the field-level arm of
// the duplicate-spelling matrix: every field reserved key × {exact-only,
// exact+case-variant} × {valid-shaped, non-schema} variant bodies, on
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
	t.Run("exact-only", func(t *testing.T) {
		props, rbProps := fieldProps(t, host(""))
		for _, p := range []map[string]any{props, rbProps} {
			for k := range p {
				if _, reserved := fieldBodies[strings.ToLower(k)]; reserved {
					t.Errorf("consumed field key spelling %q leaked into Props: %#v", k, p)
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
// wrapped reference is replaced by its definition: the wrapper's
// exact-lowercase reserved spellings die (consumed; Java drops usage-site
// extras at reference sites entirely), while case-variant spellings —
// ordinary custom properties — and exact stray spellings with non-shape
// bodies merge onto the spliced definition as ordinary props.
// Definition-wins is an exact-key presence check, so the merge is
// order-independent: map keys are unique and merging one wrapper prop can
// never change another's verdict.
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
			// Exact stray spelling with a non-shape body: a prop.
			"symbols": []any{int64(1)},
			// Case-variant spellings: ordinary props, preserved verbatim
			// regardless of body shape.
			"SYMBOLS": []any{"B"},
			"DOC":     int64(12),
			// Non-reserved props: both merge.
			"foo": int64(1),
			"FOO": int64(2),
			// The wrapper's exact "doc" is a consumed reserved usage-site
			// attribute: it dies at the splice and is absent.
		}
		if !reflect.DeepEqual(props, want) {
			t.Fatalf("spliced Props = %#v; want %#v", props, want)
		}
		if spliced.Doc != "" {
			t.Fatalf("Doc = %q; the definition has no doc and the wrapper's dies", spliced.Doc)
		}
	}
}

// ---------- reserved_exactcase_test.go ----------

// Reserved Avro attribute names match ONLY their exact lowercase spelling, on
// every reading surface: a key differing only by letter case is an ordinary
// custom property, never bound, never consumed, preserved verbatim in Props.
// This matches the spec — attribute names are literal JSON keys — and the three
// executed references: Java's reserved-key sets are exact-lowercase HashSets and
// its structural reads use exact Jackson lookups; fastavro 1.12.2 reads known
// keys by exact name and preserves the rest; goavro reads exact keys. hamba/avro
// is the one case-folding implementation, so a schema that parsed there but
// fails here has a miscased reserved key — a miscased STRUCTURAL key fails
// loudly at parse, a miscased non-structural key is a harmless custom property.

// TestMatrix_CaseVariantStructuralKeyRejects pins the consequence of
// exact-case matching for structural keys: a case-variant spelling does
// NOT bind, so the real structural attribute is absent and the parse fails
// with the kind's ordinary missing-attribute error — loudly, at parse
// time, never as a silently different schema.
func TestMatrix_CaseVariantStructuralKeyRejects(t *testing.T) {
	t.Parallel()
	cases := []struct {
		schema  string
		wantErr string
	}{
		{`{"type":"array","ITEMS":"int"}`, "array is missing items schema"},
		{`{"type":"array","Items":"int"}`, "array is missing items schema"},
		{`{"type":"map","VALUES":"int"}`, "map is missing values schema"},
		{`{"type":"record","name":"R","FIELDS":[{"name":"f","type":"int"}]}`, "record is missing fields"},
		{`{"type":"record","name":"R","Fields":[{"name":"f","type":"int"}]}`, "record is missing fields"},
		{`{"type":"enum","name":"E","SYMBOLS":["A","B"]}`, "enum is missing symbols"},
		{`{"type":"fixed","name":"F","SIZE":4}`, "fixed is missing size"},
	}
	for _, c := range cases {
		_, err := avro.Parse(c.schema)
		if err == nil {
			t.Errorf("Parse(%s) accepted; want error containing %q (a case-variant key is a custom property, so the structural attribute is missing)", c.schema, c.wantErr)
			continue
		}
		if !strings.Contains(err.Error(), c.wantErr) {
			t.Errorf("Parse(%s) error = %q; want it to contain %q", c.schema, err.Error(), c.wantErr)
		}
	}
}

// TestRegression_CaseVariantNamingKeyInert pins exact-case matching for the
// naming and logical-annotation keys: a case-variant of name / namespace /
// aliases / logicalType is an ordinary custom property — it never renames,
// re-scopes, aliases, or annotates the type — and it is preserved verbatim
// on Root().Props, the parse-side CustomType-callback Props, and the
// Root().Schema() rebuild.
func TestRegression_CaseVariantNamingKeyInert(t *testing.T) {
	t.Parallel()

	// NAMESPACE variant: the record's fullname is its bare name.
	var captured map[string]any
	s, err := avro.Parse(`{"type":"record","name":"R","NAMESPACE":"zed","fields":[{"name":"f","type":"int"}]}`,
		avro.WithCustomType(propsCaptureCustom("record", "R", &captured)))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	mustAppendEncode(t, s, nil, map[string]any{"f": int32(1)})
	root := s.Root()
	if root.Name != "R" || root.Namespace != "" {
		t.Errorf("Name=%q Namespace=%q; a NAMESPACE case-variant must not scope the type", root.Name, root.Namespace)
	}
	if !strings.Contains(string(s.Canonical()), `"name":"R"`) || strings.Contains(string(s.Canonical()), "zed") {
		t.Errorf("canonical carries the variant namespace: %s", s.Canonical())
	}
	want := map[string]any{"NAMESPACE": "zed"}
	if !reflect.DeepEqual(root.Props, want) {
		t.Errorf("Root().Props = %#v; want %#v", root.Props, want)
	}
	if !reflect.DeepEqual(captured, want) {
		t.Errorf("callback Props = %#v; want %#v (must equal Root().Props)", captured, want)
	}
	rb, err := root.Schema()
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if !reflect.DeepEqual(rb.Root().Props, want) {
		t.Errorf("rebuild Props = %#v; want %#v", rb.Root().Props, want)
	}

	// LOGICALTYPE variant: no logical annotation is applied.
	s2, err := avro.Parse(`{"type":"long","LOGICALTYPE":"timestamp-millis"}`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if lt := s2.Root().LogicalType; lt != "" {
		t.Errorf("LogicalType = %q; a LOGICALTYPE case-variant must not annotate", lt)
	}
	if got := s2.Root().Props["LOGICALTYPE"]; !reflect.DeepEqual(got, "timestamp-millis") {
		t.Errorf(`Props["LOGICALTYPE"] = %#v; want the variant preserved verbatim`, got)
	}

	// ALIASES variant on a named type: no alias is registered (resolution
	// via the variant spelling fails), the key is a preserved prop.
	s3, err := avro.Parse(`{"type":"record","name":"New","ALIASES":["Old"],"fields":[{"name":"f","type":"int"}]}`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if a := s3.Root().Aliases; a != nil {
		t.Errorf("Aliases = %#v; an ALIASES case-variant must not alias", a)
	}
	writer, err := avro.Parse(`{"type":"record","name":"Old","fields":[{"name":"f","type":"int"}]}`)
	if err != nil {
		t.Fatalf("Parse writer: %v", err)
	}
	if _, err := avro.Resolve(writer, s3); err == nil {
		t.Errorf("Resolve(Old → New) succeeded; the ALIASES case-variant must not register an alias")
	}
}

// TestRegression_CaseVariantStrayBodyStaysProp pins the boundary against
// the stray-key routing (an exact-lowercase reserved key on a kind that
// does not bind it keeps its shape-conditional structural surfacing): a
// CASE-VARIANT spelling gets no shape routing at all — even a body that
// parses perfectly as the key's schema shape rides to Props verbatim,
// because the key is simply not a reserved key.
func TestRegression_CaseVariantStrayBodyStaysProp(t *testing.T) {
	t.Parallel()
	s := mustParse(t, `{"type":"int","ITEMS":"long"}`)
	root := s.Root()
	if root.Items != nil {
		t.Errorf("Items = %+v; a case-variant key must not surface structurally", root.Items)
	}
	if got := root.Props["ITEMS"]; !reflect.DeepEqual(got, "long") {
		t.Errorf(`Props["ITEMS"] = %#v; want "long" verbatim`, got)
	}

	// The exact-lowercase stray keeps its structural surfacing (the
	// boundary-1 control: the stray routing is about placement, not case).
	s2 := mustParse(t, `{"type":"int","items":"long"}`)
	root2 := s2.Root()
	if root2.Items == nil || root2.Items.Type != "long" {
		t.Errorf("exact-case stray items lost its structural surfacing: %+v", root2.Items)
	}
	if _, inProps := root2.Props["items"]; inProps {
		t.Errorf("exact-case shape-OK stray leaked into Props: %#v", root2.Props)
	}
}

// TestRegression_FieldCaseVariantKeyInert pins the field level: a
// case-variant of a field reserved key (default here) is an ordinary field
// property — the field has no default — preserved on SchemaField.Props and
// by the rebuild.
func TestRegression_FieldCaseVariantKeyInert(t *testing.T) {
	t.Parallel()
	s := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"f","type":"int","DEFAULT":7}]}`)
	f := s.Root().Fields[0]
	if f.HasDefault {
		t.Errorf("HasDefault = true; a DEFAULT case-variant must not set a default")
	}
	if got := f.Props["DEFAULT"]; !reflect.DeepEqual(got, int64(7)) {
		t.Errorf(`Props["DEFAULT"] = %#v; want 7 verbatim`, got)
	}
	// A field whose name exists only as a case-variant has no name: reject.
	if _, err := avro.Parse(`{"type":"record","name":"R2","fields":[{"NAME":"f","type":"int"}]}`); err == nil {
		t.Errorf("field with only a NAME case-variant accepted; the field has no name and must reject")
	}
	// A field whose type exists only as a case-variant has no type: reject.
	if _, err := avro.Parse(`{"type":"record","name":"R3","fields":[{"name":"f","TYPE":"int"}]}`); err == nil {
		t.Errorf("field with only a TYPE case-variant accepted; the field has no type and must reject")
	}
}

// TestRegression_CaseVariantTypeKeyRejects pins the dispatch key itself: an
// object whose "type" exists only as a case-variant has no type attribute
// at all and must reject (executed parity: fastavro KeyError 'type';
// goavro "missing type"; hamba rejects this shape too).
func TestRegression_CaseVariantTypeKeyRejects(t *testing.T) {
	t.Parallel()
	if _, err := avro.Parse(`{"tYpe":"record","name":"R","fields":[{"name":"f","type":"int"}]}`); err == nil {
		t.Errorf("object with only a tYpe case-variant accepted; want reject (no type attribute)")
	}
	if _, err := avro.Parse(`{"TYPE":"int"}`); err == nil {
		t.Errorf("object with only a TYPE case-variant accepted; want reject (no type attribute)")
	}
}

// exactCaseVariantOnlyRows enumerate the discriminating spelling axis of the
// exact-case contract: a reserved key present ONLY as a case-variant. Because
// the variant is an ordinary custom property, the reserved attribute is absent —
// a REQUIRED attribute's absence rejects with the kind's ordinary
// missing-attribute error, and an optional attribute's absence leaves it unset
// with the variant riding to Props. The exact+variant-both-present axis is
// TestMatrix_ReservedKeyDuplicateSpellings, where the exact-only controls live
// too.
//
// Each row records the executed fastavro verdict. Two cells diverge for reasons
// that predate and outlive the case rule: fastavro accepts a fields-less record
// as zero fields, and a precision-less decimal by dropping the logical, where
// twmb requires both.
type exactCaseVariantOnlyRow struct {
	label   string
	schema  string
	wantErr string // non-empty: twmb rejects with this substring
	favroOK bool   // executed fastavro parse verdict for the same text

	// Accept-cell assertions: the variant key expected in Props, and an
	// optional extra attribute check.
	variantKey string
	wantVal    any
	check      func(t *testing.T, root avro.SchemaNode)
}

func exactCaseVariantOnlyRows() []exactCaseVariantOnlyRow {
	return []exactCaseVariantOnlyRow{
		// Required structural keys: absent → the kind's loud reject.
		{label: "items/binding", schema: `{"type":"array","ITEMS":"int"}`, wantErr: "array is missing items schema"},
		{label: "items/binding-mixed", schema: `{"type":"array","Items":"int"}`, wantErr: "array is missing items schema"},
		{label: "values/binding", schema: `{"type":"map","VALUES":"int"}`, wantErr: "map is missing values schema"},
		{label: "fields/binding", schema: `{"type":"record","name":"R","FIELDS":[{"name":"f","type":"int"}]}`, wantErr: "record is missing fields", favroOK: true},
		{label: "symbols/binding", schema: `{"type":"enum","name":"E","SYMBOLS":["A","B"]}`, wantErr: "enum is missing symbols"},
		{label: "size/binding", schema: `{"type":"fixed","name":"F","SIZE":4}`, wantErr: "fixed is missing size"},
		{label: "name/binding", schema: `{"type":"record","NAME":"R","fields":[{"name":"f","type":"int"}]}`, wantErr: `invalid record name ""`},
		{label: "type/binding", schema: `{"TYPE":"int"}`, wantErr: `unknown primitive ""`},
		{label: "precision/decimal", schema: `{"type":"fixed","name":"D","size":16,"logicalType":"decimal","PRECISION":6,"scale":2}`, wantErr: "decimal logical type requires precision", favroOK: true},

		// Optional attributes: absent → unset, variant → Props.
		{label: "namespace/named", schema: `{"type":"record","name":"NR","NAMESPACE":"zed","fields":[{"name":"f","type":"int"}]}`, favroOK: true,
			variantKey: "NAMESPACE", wantVal: "zed", check: func(t *testing.T, root avro.SchemaNode) {
				if root.Namespace != "" || root.Name != "NR" {
					t.Errorf("Name=%q Namespace=%q; the variant must not scope the type", root.Name, root.Namespace)
				}
			}},
		{label: "aliases/named", schema: `{"type":"record","name":"AR","ALIASES":["Old"],"fields":[{"name":"f","type":"int"}]}`, favroOK: true,
			variantKey: "ALIASES", wantVal: []any{"Old"}, check: func(t *testing.T, root avro.SchemaNode) {
				if root.Aliases != nil {
					t.Errorf("Aliases = %#v; the variant must not alias", root.Aliases)
				}
			}},
		{label: "doc/named", schema: `{"type":"record","name":"DR","DOC":"note","fields":[{"name":"f","type":"int"}]}`, favroOK: true,
			variantKey: "DOC", wantVal: "note", check: func(t *testing.T, root avro.SchemaNode) {
				if root.Doc != "" {
					t.Errorf("Doc = %q; the variant must not document", root.Doc)
				}
			}},
		{label: "logicalType/long", schema: `{"type":"long","LOGICALTYPE":"timestamp-millis"}`, favroOK: true,
			variantKey: "LOGICALTYPE", wantVal: "timestamp-millis", check: func(t *testing.T, root avro.SchemaNode) {
				if root.LogicalType != "" {
					t.Errorf("LogicalType = %q; the variant must not annotate", root.LogicalType)
				}
			}},
		{label: "default/enum", schema: `{"type":"enum","name":"ED","symbols":["A","B"],"DEFAULT":"B"}`, favroOK: true,
			variantKey: "DEFAULT", wantVal: "B", check: func(t *testing.T, root avro.SchemaNode) {
				if root.HasEnumDefault || root.EnumDefault != "" {
					t.Errorf("EnumDefault = %q (%v); the variant must not set the enum default", root.EnumDefault, root.HasEnumDefault)
				}
			}},
		{label: "scale/decimal", schema: `{"type":"fixed","name":"SD","size":16,"logicalType":"decimal","precision":6,"SCALE":2}`, favroOK: true,
			variantKey: "SCALE", wantVal: int64(2), check: func(t *testing.T, root avro.SchemaNode) {
				if root.Scale != 0 {
					t.Errorf("Scale = %d; the variant must not set the scale (spec default 0)", root.Scale)
				}
			}},

		// Non-binding carriers: the same variant keys on an int host are
		// plain props with no structural surfacing — the boundary against
		// the exact-lowercase stray routing, which surfaces shape-OK
		// bodies structurally on these hosts.
		{label: "items/nonbinding", schema: `{"type":"int","ITEMS":"long"}`, favroOK: true,
			variantKey: "ITEMS", wantVal: "long", check: func(t *testing.T, root avro.SchemaNode) {
				if root.Items != nil {
					t.Errorf("Items = %+v; a variant key must not surface structurally", root.Items)
				}
			}},
		{label: "values/nonbinding", schema: `{"type":"int","VALUES":"long"}`, favroOK: true,
			variantKey: "VALUES", wantVal: "long", check: func(t *testing.T, root avro.SchemaNode) {
				if root.Values != nil {
					t.Errorf("Values = %+v; a variant key must not surface structurally", root.Values)
				}
			}},
		{label: "fields/nonbinding", schema: `{"type":"int","FIELDS":[{"name":"f","type":"int"}]}`, favroOK: true,
			variantKey: "FIELDS", wantVal: []any{map[string]any{"name": "f", "type": "int"}}, check: func(t *testing.T, root avro.SchemaNode) {
				if root.Fields != nil {
					t.Errorf("Fields = %+v; a variant key must not surface structurally", root.Fields)
				}
			}},
		{label: "symbols/nonbinding", schema: `{"type":"int","SYMBOLS":["A"]}`, favroOK: true,
			variantKey: "SYMBOLS", wantVal: []any{"A"}, check: func(t *testing.T, root avro.SchemaNode) {
				if root.Symbols != nil {
					t.Errorf("Symbols = %+v; a variant key must not surface structurally", root.Symbols)
				}
			}},
		{label: "size/nonbinding", schema: `{"type":"int","SIZE":4}`, favroOK: true,
			variantKey: "SIZE", wantVal: int64(4), check: func(t *testing.T, root avro.SchemaNode) {
				if root.Size != 0 {
					t.Errorf("Size = %d; a variant key must not surface structurally", root.Size)
				}
			}},
		{label: "name/nonbinding", schema: `{"type":"int","NAME":"x"}`, favroOK: true,
			variantKey: "NAME", wantVal: "x", check: func(t *testing.T, root avro.SchemaNode) {
				if root.Name != "" {
					t.Errorf("Name = %q; a variant key must not surface structurally", root.Name)
				}
			}},
		{label: "namespace/nonbinding", schema: `{"type":"int","NAMESPACE":"x"}`, favroOK: true,
			variantKey: "NAMESPACE", wantVal: "x", check: func(t *testing.T, root avro.SchemaNode) {
				if root.Namespace != "" {
					t.Errorf("Namespace = %q; a variant key must not surface structurally", root.Namespace)
				}
			}},
		{label: "aliases/nonbinding", schema: `{"type":"int","ALIASES":["a"]}`, favroOK: true,
			variantKey: "ALIASES", wantVal: []any{"a"}, check: func(t *testing.T, root avro.SchemaNode) {
				if root.Aliases != nil {
					t.Errorf("Aliases = %+v; a variant key must not surface structurally", root.Aliases)
				}
			}},
	}
}

// TestMatrix_ReservedKeyVariantOnly drives the variant-only spelling axis:
// reserved key × carrier {binding/required, optional-attribute, non-binding
// host} × surface {Parse verdict, Root() attributes + Props, the
// Root().Schema() rebuild, self-resolution}. Reject cells pin the loud
// missing-attribute error; accept cells pin the attribute NOT binding, the
// variant preserved verbatim on Root().Props and the rebuild, and Resolve
// succeeding with the variant riding along (props never obstruct
// resolution). Parse-side callback Props parity for variant keys is pinned
// by TestMatrix_ReservedKeyDuplicateSpellings and
// TestRegression_CaseVariantNamingKeyInert.
func TestMatrix_ReservedKeyVariantOnly(t *testing.T) {
	t.Parallel()
	for _, row := range exactCaseVariantOnlyRows() {
		t.Run(row.label, func(t *testing.T) {
			s, err := avro.Parse(row.schema)
			if row.wantErr != "" {
				if err == nil {
					t.Fatalf("Parse(%s) accepted; want error containing %q", row.schema, row.wantErr)
				}
				if !strings.Contains(err.Error(), row.wantErr) {
					t.Fatalf("Parse(%s) error = %q; want it to contain %q", row.schema, err.Error(), row.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%s): %v", row.schema, err)
			}
			root := s.Root()
			if row.check != nil {
				row.check(t, *root)
			}
			if got, ok := root.Props[row.variantKey]; !ok || !reflect.DeepEqual(got, row.wantVal) {
				t.Errorf("Root().Props[%q] = %#v (present=%v); want %#v verbatim", row.variantKey, got, ok, row.wantVal)
			}
			rb, err := root.Schema()
			if err != nil {
				t.Fatalf("rebuild: %v", err)
			}
			if got, ok := rb.Root().Props[row.variantKey]; !ok || !reflect.DeepEqual(got, row.wantVal) {
				t.Errorf("rebuild Props[%q] = %#v (present=%v); want %#v verbatim", row.variantKey, got, ok, row.wantVal)
			}
			if _, err := avro.Resolve(s, s); err != nil {
				t.Errorf("self-resolution failed: %v (a variant prop must never obstruct resolution)", err)
			}
		})
	}
}

// TestMatrix_FieldReservedKeyVariantOnly is the field-level arm: a field
// reserved key present only as a case-variant. name and type are required
// (loud reject); default/doc/aliases/order are optional (attribute unset,
// variant preserved on SchemaField.Props and the rebuild).
func TestMatrix_FieldReservedKeyVariantOnly(t *testing.T) {
	t.Parallel()
	rejects := []struct{ label, schema, wantErr string }{
		{"name", `{"type":"record","name":"R","fields":[{"NAME":"f","type":"int"}]}`, `invalid field name ""`},
		{"type", `{"type":"record","name":"R","fields":[{"name":"f","TYPE":"int"}]}`, "invalid record field: schema is not a primitive, complex, nor union"},
	}
	for _, c := range rejects {
		t.Run(c.label, func(t *testing.T) {
			_, err := avro.Parse(c.schema)
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("Parse(%s): got %v; want error containing %q", c.schema, err, c.wantErr)
			}
		})
	}
	accepts := []struct {
		label   string
		variant string
		body    string
		wantVal any
		check   func(t *testing.T, f avro.SchemaField)
	}{
		{"default", "DEFAULT", `7`, int64(7), func(t *testing.T, f avro.SchemaField) {
			if f.HasDefault {
				t.Errorf("HasDefault = true; the variant must not set a default")
			}
		}},
		{"doc", "DOC", `"note"`, "note", func(t *testing.T, f avro.SchemaField) {
			if f.Doc != "" {
				t.Errorf("Doc = %q; the variant must not document", f.Doc)
			}
		}},
		{"aliases", "ALIASES", `["g"]`, []any{"g"}, func(t *testing.T, f avro.SchemaField) {
			if f.Aliases != nil {
				t.Errorf("Aliases = %#v; the variant must not alias", f.Aliases)
			}
		}},
		{"order", "ORDER", `"descending"`, "descending", func(t *testing.T, f avro.SchemaField) {
			if f.Order != "" {
				t.Errorf("Order = %q; the variant must not order", f.Order)
			}
		}},
	}
	for _, c := range accepts {
		t.Run(c.label, func(t *testing.T) {
			schema := `{"type":"record","name":"FR","fields":[{"name":"f","type":"int","` + c.variant + `":` + c.body + `}]}`
			s, err := avro.Parse(schema)
			if err != nil {
				t.Fatalf("Parse(%s): %v", schema, err)
			}
			root := s.Root()
			f := root.Fields[0]
			c.check(t, f)
			if got, ok := f.Props[c.variant]; !ok || !reflect.DeepEqual(got, c.wantVal) {
				t.Errorf("SchemaField.Props[%q] = %#v (present=%v); want %#v verbatim", c.variant, got, ok, c.wantVal)
			}
			rb, err := root.Schema()
			if err != nil {
				t.Fatalf("rebuild: %v", err)
			}
			if got, ok := rb.Root().Fields[0].Props[c.variant]; !ok || !reflect.DeepEqual(got, c.wantVal) {
				t.Errorf("rebuild Props[%q] = %#v (present=%v); want %#v verbatim", c.variant, got, ok, c.wantVal)
			}
		})
	}
}

// TestDifferentialFastavroReservedExactCase executes every variant-only
// cell through fastavro's parser and asserts the recorded verdict:
// fastavro reads reserved keys by exact lowercase name (rejecting when a
// required one is thereby absent, preserving the variant otherwise), so
// twmb's verdicts match except the two per-cell-documented laxities
// (fields-less record; precision-less decimal).
func TestDifferentialFastavroReservedExactCase(t *testing.T) {
	o := startOracle(t)
	for _, row := range exactCaseVariantOnlyRows() {
		resp := o.call(oracleJob{Op: "parse", Schema: json.RawMessage(row.schema)})
		wantOK := row.wantErr == "" || row.favroOK
		if resp.OK != wantOK {
			t.Errorf("%s: fastavro ok=%v err=%q; want ok=%v\nschema: %s", row.label, resp.OK, resp.Err, wantOK, row.schema)
		}
	}
	fieldCells := []struct {
		schema string
		wantOK bool
	}{
		{`{"type":"record","name":"R","fields":[{"NAME":"f","type":"int"}]}`, false},
		{`{"type":"record","name":"R","fields":[{"name":"f","TYPE":"int"}]}`, false},
		{`{"type":"record","name":"R","fields":[{"name":"f","type":"int","DEFAULT":7}]}`, true},
		{`{"type":"record","name":"R","fields":[{"name":"f","type":"int","DOC":"note"}]}`, true},
		{`{"type":"record","name":"R","fields":[{"name":"f","type":"int","ALIASES":["g"]}]}`, true},
		{`{"type":"record","name":"R","fields":[{"name":"f","type":"int","ORDER":"descending"}]}`, true},
	}
	for _, c := range fieldCells {
		resp := o.call(oracleJob{Op: "parse", Schema: json.RawMessage(c.schema)})
		if resp.OK != c.wantOK {
			t.Errorf("field cell: fastavro ok=%v err=%q; want ok=%v\nschema: %s", resp.OK, resp.Err, c.wantOK, c.schema)
		}
	}
}

// ---------- stray_precision_scale_test.go ----------

// Stray "precision"/"scale" schema attributes are inert metadata, not parse
// errors. Per the spec ("Attributes not defined in this document are permitted
// as metadata", Specification/_index.md:43), an attribute the parser does not
// consume is a plain custom property. twmb consumes precision/scale as decimal
// parameters exactly when the node is a recognized decimal carrier — logicalType
// "decimal" on bytes or fixed, where NOT_BUGS #55 validates them — and every
// other placement surfaces them in Props, matching the field level and both
// references (fastavro 1.12.2 executed 9/9 accepts; Java's
// LogicalTypes.fromSchemaImpl returns null when logicalType is absent so
// precision is never consulted, LogicalTypes.java:127-130).
//
// The FIELD level follows the same consumed-conditional rule with the consumer
// being the decimal lift: the pair is consumed exactly when the field declares
// logicalType "decimal" and the lift target is a bytes/fixed carrier as-written,
// where a malformed body rejects loudly naming the key (treating it as absent
// would silently parse as decimal(p,0), scale being optional); everywhere else
// any body shape is an ordinary SchemaField.Props property (NOT_BUGS #71; Java's
// FIELD_RESERVED never includes the pair, Schema.java:503-504). The matrix below
// crosses a BODY-SHAPE axis over every placement for exactly this reason.
//
// Pre-fix, validateLogical's tail rejected any leftover precision/scale exactly
// when NO logical — or a valid non-decimal logical — accompanied them, while the
// SAME stray keys parsed when the logical placement was invalid, and the field
// level already treated them as inert props: twmb disagreed with itself across
// levels and across placements.
func TestMatrix_StrayPrecisionScaleParses(t *testing.T) {
	cases := []struct {
		name  string
		src   string
		props map[string]any
	}{
		{"int-precision", `{"type":"int","precision":3}`, map[string]any{"precision": int64(3)}},
		{"uuid-string-precision", `{"type":"string","logicalType":"uuid","precision":3}`, map[string]any{"precision": int64(3)}},
		{"timestamp-long-precision", `{"type":"long","logicalType":"timestamp-millis","precision":3}`, map[string]any{"precision": int64(3)}},
		{"record-precision", `{"type":"record","name":"R","precision":3,"fields":[{"name":"a","type":"int"}]}`, map[string]any{"precision": int64(3)}},
		{"fixed-precision", `{"type":"fixed","name":"F","size":4,"precision":3}`, map[string]any{"precision": int64(3)}},
		{"array-scale", `{"type":"array","items":"int","scale":1}`, map[string]any{"scale": int64(1)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := avro.Parse(c.src)
			if err != nil {
				t.Fatalf("Parse(%s) rejected stray precision/scale: %v", c.src, err)
			}
			root := s.Root()
			if root.Precision != 0 || root.Scale != 0 {
				t.Errorf("stray keys consumed into Precision=%d/Scale=%d; want both 0 (Props-only)", root.Precision, root.Scale)
			}
			if !reflect.DeepEqual(root.Props, c.props) {
				t.Errorf("Root().Props = %#v; want %#v", root.Props, c.props)
			}
		})
	}
}

// The today-accepted invalid-logical placements (unknown logical,
// decimal-on-a-wrong-carrier — including via a named reference) used to
// consume stray precision/scale into SchemaNode.Precision/Scale — fields
// documented as validated decimal parameters — without any validation
// (precision -5 surfaced there), leaving Props empty. Under the uniform
// routing rule they are plain Props like every other non-consumed
// placement, on the Root() metadata surface AND the CustomType callback
// surface; Precision/Scale hold only #55-validated decimal parameters.
func TestMatrix_BogusLogicalStrayKeysSurfaceAsProps(t *testing.T) {
	type want struct {
		precision, scale int
		props            map[string]any
	}
	cases := []struct {
		name string
		src  string
		want want
	}{
		{
			"decimal-on-int",
			`{"type":"int","logicalType":"decimal","precision":3}`,
			want{0, 0, map[string]any{"precision": int64(3)}},
		},
		{
			"decimal-on-int-unvalidated-value",
			`{"type":"int","logicalType":"decimal","precision":-5}`,
			want{0, 0, map[string]any{"precision": int64(-5)}},
		},
		{
			"unknown-logical-on-int",
			`{"type":"int","logicalType":"myLogical","precision":3}`,
			want{0, 0, map[string]any{"precision": int64(3)}},
		},
		{
			// Control: the recognized decimal carrier keeps consuming.
			"decimal-on-bytes-consumed",
			`{"type":"bytes","logicalType":"decimal","precision":2,"scale":1}`,
			want{2, 1, nil},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := avro.Parse(c.src)
			if err != nil {
				t.Fatalf("Parse(%s): %v", c.src, err)
			}
			root := s.Root()
			if root.Precision != c.want.precision || root.Scale != c.want.scale {
				t.Errorf("Precision/Scale = %d/%d; want %d/%d", root.Precision, root.Scale, c.want.precision, c.want.scale)
			}
			if !reflect.DeepEqual(root.Props, c.want.props) {
				t.Errorf("Root().Props = %#v; want %#v", root.Props, c.want.props)
			}

			// Root().Schema() must rebuild the same schema: the stray keys
			// re-emit (from Props post-fix), and the rebuilt schema routes
			// identically.
			s2, err := root.Schema()
			if err != nil {
				t.Fatalf("Root().Schema() rebuild: %v", err)
			}
			root2 := s2.Root()
			if root2.Precision != c.want.precision || root2.Scale != c.want.scale || !reflect.DeepEqual(root2.Props, c.want.props) {
				t.Errorf("rebuilt Precision/Scale/Props = %d/%d/%#v; want %d/%d/%#v",
					root2.Precision, root2.Scale, root2.Props, c.want.precision, c.want.scale, c.want.props)
			}
		})
	}

	t.Run("reference-decimal-precision", func(t *testing.T) {
		s := mustParse(t, `{"type":"record","name":"R","fields":[
			{"name":"F","type":{"type":"fixed","name":"Fx","size":4}},
			{"name":"b","type":{"type":"Fx","logicalType":"decimal","precision":3}}]}`)
		ref := s.Root().Fields[1].Type
		if ref.Precision != 0 {
			t.Errorf("reference node consumed stray precision into Precision=%d; want 0", ref.Precision)
		}
		wantProps := map[string]any{"precision": int64(3)}
		if !reflect.DeepEqual(ref.Props, wantProps) {
			t.Errorf("reference node Props = %#v; want %#v", ref.Props, wantProps)
		}
	})

	// Policy pin (green by construction): the Resolve surface follows the
	// inert rule too. A CustomType-resurrected decimal on a wrong carrier
	// no longer carries stray precision into the internal node, so
	// checkCompat's decimal precision/scale comparison sees 0/0 on both
	// sides and differing STRAY values cannot fail a resolve; consumed
	// decimals with differing parameters keep rejecting.
	t.Run("resolve-inert-vs-consumed", func(t *testing.T) {
		ct := avro.CustomType{
			LogicalType: "decimal",
			Decode:      func(v any, _ *avro.SchemaNode) (any, error) { return v, nil },
		}
		w, err := avro.Parse(`{"type":"int","logicalType":"decimal","precision":3}`, avro.WithCustomType(ct))
		if err != nil {
			t.Fatalf("writer Parse: %v", err)
		}
		r, err := avro.Parse(`{"type":"int","logicalType":"decimal","precision":4}`, avro.WithCustomType(ct))
		if err != nil {
			t.Fatalf("reader Parse: %v", err)
		}
		if _, err := avro.Resolve(w, r); err != nil {
			t.Errorf("Resolve with differing STRAY precision must succeed (inert metadata): %v", err)
		}

		// Control: consumed decimal parameters still gate resolution.
		wc := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":3}`)
		rc := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":4}`)
		if _, err := avro.Resolve(wc, rc); err == nil {
			t.Errorf("Resolve with differing CONSUMED precision must keep rejecting")
		}
	})

	t.Run("customtype-callback-surface", func(t *testing.T) {
		var sawPrecision, sawScale int
		var sawProps map[string]any
		ct := avro.CustomType{
			LogicalType: "decimal",
			Decode: func(v any, n *avro.SchemaNode) (any, error) {
				sawPrecision, sawScale, sawProps = n.Precision, n.Scale, n.Props
				return v, nil
			},
		}
		s := mustParse(t, `{"type":"int","logicalType":"decimal","precision":3}`, avro.WithCustomType(ct))
		enc := mustAppendEncode(t, s, nil, int32(7))
		var out any
		mustDecode(t, s, enc, &out)
		if sawPrecision != 0 || sawScale != 0 {
			t.Errorf("callback saw Precision=%d/Scale=%d; want 0/0 (Props-only)", sawPrecision, sawScale)
		}
		wantProps := map[string]any{"precision": int64(3)}
		if !reflect.DeepEqual(sawProps, wantProps) {
			t.Errorf("callback saw Props = %#v; want %#v", sawProps, wantProps)
		}
	})
}

// ---------------------------------------------------------------------------
// Field-level pins: malformed precision/scale bodies.

// A field-level "precision"/"scale" whose body fails the int shape is an inert
// custom property wherever the pair is UNCONSUMED — no field logicalType, a
// non-decimal one, or a decimal whose lift target is not a bytes/fixed carrier —
// riding to SchemaField.Props verbatim, exactly like a valid-int unconsumed
// pair. Java never validates field-level precision/scale (FIELD_RESERVED omits
// them, Schema.java:503-504, so parseProperties preserves them) and fastavro
// 1.12.2 accepts and preserves them verbatim. Only a CONSUMED placement keeps
// the loud shape reject.
func TestMatrix_FieldPrecisionScaleMalformedUnconsumedInert(t *testing.T) {
	cases := []struct {
		name  string
		src   string
		props map[string]any
	}{
		{
			"no-logical-string-precision",
			`{"type":"record","name":"R","fields":[{"name":"f","type":"int","precision":"x"}]}`,
			map[string]any{"precision": "x"},
		},
		{
			"no-logical-float-scale",
			`{"type":"record","name":"R","fields":[{"name":"f","type":"int","scale":3.5}]}`,
			map[string]any{"scale": 3.5},
		},
		{
			"non-decimal-logical-string-precision",
			`{"type":"record","name":"R","fields":[{"name":"f","type":"int","logicalType":"myLogical","precision":"x"}]}`,
			map[string]any{"logicalType": "myLogical", "precision": "x"},
		},
		{
			"decimal-non-carrier-string-precision",
			`{"type":"record","name":"R","fields":[{"name":"f","type":"int","logicalType":"decimal","precision":"x"}]}`,
			map[string]any{"logicalType": "decimal", "precision": "x"},
		},
		{
			"decimal-union-non-carrier-float-scale",
			`{"type":"record","name":"R","fields":[{"name":"f","type":["null","int"],"logicalType":"decimal","scale":1.5}]}`,
			map[string]any{"logicalType": "decimal", "scale": 1.5},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := avro.Parse(c.src)
			if err != nil {
				t.Fatalf("Parse(%s) rejected an unconsumed malformed field precision/scale: %v", c.src, err)
			}
			f := s.Root().Fields[0]
			if !reflect.DeepEqual(f.Props, c.props) {
				t.Errorf("SchemaField.Props = %#v; want verbatim %#v", f.Props, c.props)
			}
			if f.Type.Precision != 0 || f.Type.Scale != 0 {
				t.Errorf("field's type node consumed the pair: Precision/Scale = %d/%d; want 0/0", f.Type.Precision, f.Type.Scale)
			}
			// String() re-emits the as-written form; the reparse agrees.
			s2, err := avro.Parse(s.String())
			if err != nil {
				t.Fatalf("String() reparse: %v", err)
			}
			if !reflect.DeepEqual(s2.Root().Fields[0].Props, c.props) {
				t.Errorf("String() reparse Props = %#v; want %#v", s2.Root().Fields[0].Props, c.props)
			}
			// The rebuild keeps the pair field-level on the rebuilt surface.
			root := s.Root()
			rb, err := root.Schema()
			if err != nil {
				t.Fatalf("Root().Schema() rebuild: %v", err)
			}
			if !reflect.DeepEqual(rb.Root().Fields[0].Props, c.props) {
				t.Errorf("rebuild Props = %#v; want %#v", rb.Root().Fields[0].Props, c.props)
			}
		})
	}
}

// Flat (goavro-style) complex fields MEAN their lifted nested form (#56), so
// the malformed-pair verdict must equal the nested twin's on every cell: the
// pair flows into the lifted type object (flatLiftTypeMap routes it) and the
// TYPE-level consumed gate rules there — inert on a non-decimal lifted kind,
// reject on a lifted decimal carrier. The field arms must not pre-empt the
// lift. fastavro rejects the flat spelling outright (UnknownType), so the
// nested twin is the cross-impl anchor.
func TestRegression_FlatFieldMalformedPrecisionMatchesNestedTwin(t *testing.T) {
	cases := []struct {
		name         string
		flat, nested string
	}{
		{
			"enum-inert",
			`{"type":"record","name":"R","fields":[{"name":"E","type":"enum","symbols":["A"],"precision":"x"}]}`,
			`{"type":"record","name":"R","fields":[{"name":"E","type":{"type":"enum","name":"E","symbols":["A"],"precision":"x"}}]}`,
		},
		{
			"fixed-decimal-reject",
			`{"type":"record","name":"R","fields":[{"name":"D","type":"fixed","size":8,"logicalType":"decimal","precision":"x"}]}`,
			`{"type":"record","name":"R","fields":[{"name":"D","type":{"type":"fixed","name":"D","size":8,"logicalType":"decimal","precision":"x"}}]}`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fs, ferr := avro.Parse(c.flat)
			ns, nerr := avro.Parse(c.nested)
			if (ferr == nil) != (nerr == nil) {
				t.Fatalf("flat/nested twin verdicts diverge:\n flat   %v\n nested %v", ferr, nerr)
			}
			if ferr != nil {
				return
			}
			// Accepted: the pair surfaces on the lifted TYPE node's Props on
			// both spellings (#56: flat surfaces post-lift; the defining keys
			// and the routed pair never sit in SchemaField.Props).
			for who, s := range map[string]*avro.Schema{"flat": fs, "nested": ns} {
				tp := s.Root().Fields[0].Type
				if got := tp.Props["precision"]; got != "x" {
					t.Errorf("%s: lifted type Props[precision] = %#v; want verbatim \"x\"", who, got)
				}
				if tp.Precision != 0 {
					t.Errorf("%s: lifted type consumed malformed precision: %d", who, tp.Precision)
				}
			}
		})
	}
}

// Consumed placements keep the loud shape reject, fired from the recorded per-key
// shape error and naming the key. The scale cell is the guard against "treat
// malformed as absent": scale is OPTIONAL, so silently dropping a malformed one
// beside a valid precision would parse as decimal(p,0), a silent wire-semantics
// change (#55 anti-silent-drop). Consumption follows what the lift LANDS, not
// where it points: the pair is consumed exactly where the target's EFFECTIVE
// logical — its own when it has one, else the field's — is "decimal" on a
// bytes/fixed carrier, and the own-logical-is-decimal cells keep that rule from
// being loosened into "any annotation of its own is inert".
func TestMatrix_FieldDecimalConsumedMalformedParamReject(t *testing.T) {
	cases := []struct{ name, src, key string }{
		{
			"bytes-primitive-precision",
			`{"type":"record","name":"R","fields":[{"name":"f","type":"bytes","logicalType":"decimal","precision":"x","scale":0}]}`,
			"precision",
		},
		{
			"scale-optional-guard",
			`{"type":"record","name":"R","fields":[{"name":"f","type":"bytes","logicalType":"decimal","precision":4,"scale":"x"}]}`,
			"scale",
		},
		{
			"union-carrier-precision",
			`{"type":"record","name":"R","fields":[{"name":"f","type":["null","bytes"],"logicalType":"decimal","precision":"x"}]}`,
			"precision",
		},
		{
			"type-object-carrier-precision",
			`{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"bytes"},"logicalType":"decimal","precision":"x"}]}`,
			"precision",
		},
		{
			// DISCRIMINATOR: the target's OWN logical is decimal, so the
			// field's parameters land on a real decimal carrier and are
			// genuinely consumed. Proven by wire below, not assumed.
			"target-own-logical-decimal-precision",
			`{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"bytes","logicalType":"decimal"},"logicalType":"decimal","precision":"x","scale":2}]}`,
			"precision",
		},
		{
			"fixed-target-own-logical-decimal-precision",
			`{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"fixed","name":"F","size":4,"logicalType":"decimal"},"logicalType":"decimal","precision":"x","scale":2}]}`,
			"precision",
		},
		{
			"nested-type-level-control",
			`{"type":"bytes","logicalType":"decimal","precision":"x","scale":0}`,
			"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := avro.Parse(c.src)
			if err == nil {
				t.Fatalf("Parse(%s) accepted; want consumed-placement shape reject", c.src)
			}
			if c.key != "" && !strings.Contains(err.Error(), c.key) {
				t.Errorf("reject does not name the malformed key %q: %v", c.key, err)
			}
		})
	}
}

// Valid-int unconsumed pair: observable on SchemaField.Props, the String()
// render, and the rebuild — the surfaces the malformed forms must match.
func TestRegression_FieldPrecisionValidUnconsumedSurfacesInProps(t *testing.T) {
	s := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"f","type":"int","precision":3}]}`)
	f := s.Root().Fields[0]
	if got := f.Props["precision"]; got != int64(3) {
		t.Errorf("SchemaField.Props[precision] = %#v; want int64(3)", got)
	}
	if !strings.Contains(s.String(), `"precision":3`) {
		t.Errorf("String() dropped the unconsumed field precision: %s", s.String())
	}
	root := s.Root()
	rb := mustNodeSchema(t, root)
	if got := rb.Root().Fields[0].Props["precision"]; got != int64(3) {
		t.Errorf("rebuild Props[precision] = %#v; want int64(3)", got)
	}
}

// ---------------------------------------------------------------------------
// Class matrix: stray-precision/scale placement x level x kind.
//
// Locks the whole class the stray-precision fix belongs to, not just the filed
// instance: for every placement, at both levels, across every kind class the
// parser routes differently. Per accepted cell it asserts the verdict, the
// Props-vs-Precision/Scale routing, the B15 axes-3&4 round trips, and — for
// cells whose precision/scale are inert — that the wire bytes and the
// Canonical()/Rabin fingerprint are identical to the stray-free twin, asserted
// rather than assumed and calibrated against fastavro's PCF by execution in
// TestMatrix_StrayPrecisionScaleFastavroPCF. Reject cells are the #55 controls:
// recognized-decimal carriers with invalid parameters keep hard-rejecting.

type strayPSCell struct {
	placement string // no-logical | unknown-logical | valid-logical | decimal-valid | decimal-invalid
	kind      string // int | bytes | string | long | fixed | record | array
	level     string // type | field | union-field
	body      string // valid | quoted | float | nonnum | overflow | array — the params' JSON shape
}

// strayPSBodies are the JSON shapes injected for each param value: the valid
// int form plus every malformed class a lax reader could coerce — quoted
// digits (a string), a non-integral float (truncation bait), a non-numeric
// string, an int64-overflow literal, and an array. Where the pair is
// unconsumed, every shape is an inert custom property surfacing verbatim;
// where it is consumed, every malformed shape hard-rejects (#41's quoted
// form included).
var strayPSBodies = []string{"valid", "quoted", "float", "nonnum", "overflow", "array"}

const strayPSOverflowLit = "99999999999999999999"

// strayPSBodyJSON renders the raw JSON body for a param whose valid-int
// value is n.
func strayPSBodyJSON(body string, n int) string {
	switch body {
	case "valid":
		return strconv.Itoa(n)
	case "quoted":
		return fmt.Sprintf("%q", strconv.Itoa(n))
	case "float":
		return "3.7"
	case "nonnum":
		return `"x"`
	case "overflow":
		return strayPSOverflowLit
	case "array":
		return "[3]"
	}
	panic("unknown body " + body)
}

// strayPSBodyProps is the Props image of the injected body under the
// documented numeric normalization (value-based: int64 for whole numbers
// in range, json.Number for pure-digit integers beyond int64, float64 for
// non-integers; everything else the natural Go type, verbatim).
func strayPSBodyProps(body string, n int) any {
	switch body {
	case "valid":
		return int64(n)
	case "quoted":
		return strconv.Itoa(n)
	case "float":
		return 3.7
	case "nonnum":
		return "x"
	case "overflow":
		return json.Number(strayPSOverflowLit)
	case "array":
		return []any{int64(3)}
	}
	panic("unknown body " + body)
}

// strayPSAttrs returns the logicalType value ("" if none) and the
// precision/scale params injected for a cell.
func strayPSAttrs(placement, kind string) (logical string, params [][2]any) {
	switch placement {
	case "no-logical":
		return "", [][2]any{{"precision", 3}, {"scale", 1}}
	case "unknown-logical":
		return "myLogical", [][2]any{{"precision", 3}}
	case "valid-logical":
		return map[string]string{
			"int":    "date",
			"long":   "timestamp-millis",
			"string": "uuid",
			"bytes":  "big-decimal",
			"fixed":  "duration",
			// No logical is valid on record/array: the KNOWN name
			// soft-drops off the wrong carrier, keys stay stray.
			"record": "timestamp-millis",
			"array":  "timestamp-millis",
		}[kind], [][2]any{{"precision", 3}}
	case "decimal-valid":
		return "decimal", [][2]any{{"precision", 2}, {"scale", 1}}
	case "decimal-invalid":
		if kind == "fixed" {
			// Over fixed(1) capacity — the #55 capacity arm.
			return "decimal", [][2]any{{"precision", 99}}
		}
		// precision must be positive — the #55 positivity arm.
		return "decimal", [][2]any{{"precision", 0}}
	}
	panic("unknown placement " + placement)
}

func (c strayPSCell) fixedSize() int {
	switch c.placement {
	case "valid-logical":
		return 12 // duration requires fixed(12)
	case "decimal-invalid":
		return 1 // capacity 2 digits < precision 99
	}
	return 4
}

// carrier reports whether the cell's kind can consume decimal params.
func (c strayPSCell) carrier() bool { return c.kind == "bytes" || c.kind == "fixed" }

// malformed reports whether the injected bodies fail the int shape.
func (c strayPSCell) malformed() bool { return c.body != "valid" }

// consumedPlacement reports whether this cell's placement/kind make
// precision/scale decimal-CONSUMED: logicalType "decimal" on a bytes/fixed
// carrier. At the type level the carrier is the node's own kind; at the
// field levels it is the lift target's kind — the same rule
// (fieldDecimalLiftConsumesPrecisionScale mirrors
// decimalConsumesPrecisionScale), which is what makes the field verdict
// equal the type verdict on every cell.
func (c strayPSCell) consumedPlacement() bool {
	return (c.placement == "decimal-valid" || c.placement == "decimal-invalid") && c.carrier()
}

// consumed reports whether the params land in Precision/Scale (validated
// decimal parameters): a consumed placement holding well-shaped values.
func (c strayPSCell) consumed() bool {
	return c.placement == "decimal-valid" && c.carrier() && !c.malformed()
}

// rejects reports whether the cell must hard-reject at parse: the #55
// semantic rejects (well-shaped but invalid parameters) and the
// consumed-placement shape rejects (malformed bodies where consumed).
func (c strayPSCell) rejects() bool {
	return c.consumedPlacement() && (c.malformed() || c.placement == "decimal-invalid")
}

// attrsJSON renders `,"logicalType":"x","precision":3,...` (or "" when
// stripParams is set and there is no logical). stripParams builds the
// stray-free twin: the logical stays, only precision/scale go. body picks
// the params' JSON shape.
func strayPSAttrsJSON(logical string, params [][2]any, stripParams bool, body string) string {
	var sb strings.Builder
	if logical != "" {
		fmt.Fprintf(&sb, `,"logicalType":%q`, logical)
	}
	if !stripParams {
		for _, p := range params {
			fmt.Fprintf(&sb, `,%q:%s`, p[0], strayPSBodyJSON(body, p[1].(int)))
		}
	}
	return sb.String()
}

// schemaJSON builds the cell's schema; twin strips precision/scale.
func (c strayPSCell) schemaJSON(twin bool) string {
	logical, params := strayPSAttrs(c.placement, c.kind)
	attrs := strayPSAttrsJSON(logical, params, twin, c.body)
	if c.level == "type" {
		switch c.kind {
		case "int", "long", "string", "bytes":
			return fmt.Sprintf(`{"type":%q%s}`, c.kind, attrs)
		case "fixed":
			return fmt.Sprintf(`{"type":"fixed","name":"F","size":%d%s}`, c.fixedSize(), attrs)
		case "record":
			return fmt.Sprintf(`{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]%s}`, attrs)
		case "array":
			return fmt.Sprintf(`{"type":"array","items":"int"%s}`, attrs)
		}
	}
	var typ string
	switch c.kind {
	case "int", "long", "string", "bytes":
		typ = fmt.Sprintf("%q", c.kind)
	case "fixed":
		typ = fmt.Sprintf(`{"type":"fixed","name":"F","size":%d}`, c.fixedSize())
	case "record":
		typ = `{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}`
	case "array":
		typ = `{"type":"array","items":"int"}`
	}
	if c.level == "union-field" {
		// The attrs sit on the FIELD beside a ["null", T] union type: the
		// lift's union arm targets the first non-null branch, so the
		// consumed rule follows that branch's kind.
		return fmt.Sprintf(`{"type":"record","name":"Host","fields":[{"name":"a","type":["null",%s]%s}]}`, typ, attrs)
	}
	return fmt.Sprintf(`{"type":"record","name":"Host","fields":[{"name":"a","type":%s%s}]}`, typ, attrs)
}

// value returns a wire-encodable value for the cell, compatible with
// whichever logical actually applies (uuid wants UUID-format text,
// big-decimal and consumed decimal want *big.Rat, duration wants Duration).
func (c strayPSCell) value() any {
	if c.consumed() {
		v := any(big.NewRat(3, 2)) // 1.5: precision 2, scale 1
		if c.level == "field" || c.level == "union-field" {
			return map[string]any{"a": v}
		}
		return v
	}
	var v any
	switch c.kind {
	case "int":
		v = int32(7)
	case "long":
		v = int64(7)
	case "string":
		v = "12345678-1234-5678-1234-567812345678"
	case "bytes":
		if c.placement == "valid-logical" { // big-decimal applies
			v = big.NewRat(1, 2)
		} else {
			v = []byte{1, 2}
		}
	case "fixed":
		if c.placement == "valid-logical" { // duration applies
			v = avro.Duration{Months: 1, Days: 2, Milliseconds: 3}
		} else {
			v = []byte{1, 2, 3, 4}
		}
	case "record":
		v = map[string]any{"x": int32(7)}
	case "array":
		v = []int32{1, 2}
	}
	if c.level == "field" || c.level == "union-field" {
		return map[string]any{"a": v}
	}
	return v
}

// paramsPropsMap converts the injected params to the exact Props content
// expected on the surfaced node, per the injected body shape (whole numbers
// are int64 per the Props contract; other shapes surface as their natural
// Go type, verbatim).
func strayPSParamsProps(params [][2]any, body string) map[string]any {
	m := make(map[string]any, len(params))
	for _, p := range params {
		m[p[0].(string)] = strayPSBodyProps(body, p[1].(int))
	}
	return m
}

// assertRouting checks the Props-vs-Precision/Scale routing on the surfaced
// metadata of s for the cell.
func (c strayPSCell) assertRouting(t *testing.T, s *avro.Schema, when string) {
	t.Helper()
	logical, params := strayPSAttrs(c.placement, c.kind)
	root := s.Root()
	if c.level == "type" {
		if c.consumed() {
			if root.Precision != 2 || root.Scale != 1 {
				t.Errorf("%s: consumed cell Precision/Scale = %d/%d; want 2/1", when, root.Precision, root.Scale)
			}
			if len(root.Props) != 0 {
				t.Errorf("%s: consumed cell Props = %#v; want empty", when, root.Props)
			}
		} else {
			if root.Precision != 0 || root.Scale != 0 {
				t.Errorf("%s: stray keys consumed into Precision/Scale = %d/%d; want 0/0", when, root.Precision, root.Scale)
			}
			if want := strayPSParamsProps(params, c.body); !reflect.DeepEqual(root.Props, want) {
				t.Errorf("%s: Props = %#v; want %#v", when, root.Props, want)
			}
			if root.LogicalType != logical {
				t.Errorf("%s: LogicalType = %q; want as-written %q", when, root.LogicalType, logical)
			}
		}
		return
	}
	// Field levels (plain and union-typed): every attribute stays in the
	// FIELD's Props as written (the wire lift is a codec concession, never
	// extended into the metadata API), and the field's TYPE node — the
	// concrete type or the union node — never consumes them.
	f := root.Fields[0]
	want := strayPSParamsProps(params, c.body)
	if logical != "" {
		want["logicalType"] = logical
	}
	if !reflect.DeepEqual(f.Props, want) {
		t.Errorf("%s: field Props = %#v; want %#v", when, f.Props, want)
	}
	if f.Type.Precision != 0 || f.Type.Scale != 0 {
		t.Errorf("%s: field's type node Precision/Scale = %d/%d; want 0/0", when, f.Type.Precision, f.Type.Scale)
	}
	if len(f.Type.Props) != 0 {
		t.Errorf("%s: field's type node Props = %#v; want empty", when, f.Type.Props)
	}
}

func TestMatrix_StrayPrecisionScalePlacement(t *testing.T) {
	placements := []string{"no-logical", "unknown-logical", "valid-logical", "decimal-valid", "decimal-invalid"}
	kinds := []string{"int", "bytes", "string", "long", "fixed", "record", "array"}
	levels := []string{"type", "field", "union-field"}

	for _, level := range levels {
		for _, placement := range placements {
			for _, kind := range kinds {
				for _, body := range strayPSBodies {
					c := strayPSCell{placement: placement, kind: kind, level: level, body: body}
					t.Run(level+"/"+placement+"/"+kind+"/"+body, func(t *testing.T) {
						src := c.schemaJSON(false)
						s, err := avro.Parse(src)

						if c.rejects() {
							if err == nil {
								t.Fatalf("Parse(%s) accepted; want consumed-placement reject (#55 params / malformed shape)", src)
							}
							return
						}
						if err != nil {
							t.Fatalf("Parse(%s): %v", src, err)
						}

						// Routing on the as-parsed schema.
						c.assertRouting(t, s, "parsed")

						// Wire: the cell must encode, and for inert-key cells the
						// bytes must equal the stray-free twin's.
						enc, err := s.AppendEncode(nil, c.value())
						if err != nil {
							t.Fatalf("AppendEncode: %v", err)
						}
						if !c.consumed() {
							twin, err := avro.Parse(c.schemaJSON(true))
							if err != nil {
								t.Fatalf("twin Parse(%s): %v", c.schemaJSON(true), err)
							}
							twinEnc, err := twin.AppendEncode(nil, c.value())
							if err != nil {
								t.Fatalf("twin AppendEncode: %v", err)
							}
							if !bytes.Equal(enc, twinEnc) {
								t.Errorf("wire bytes diverge from stray-free twin:\n got %x\nwant %x", enc, twinEnc)
							}
							if !bytes.Equal(s.Canonical(), twin.Canonical()) {
								t.Errorf("Canonical diverges from stray-free twin:\n got %s\nwant %s", s.Canonical(), twin.Canonical())
							}
							gotFP := s.Fingerprint(avro.NewRabin())
							wantFP := twin.Fingerprint(avro.NewRabin())
							if !bytes.Equal(gotFP, wantFP) {
								t.Errorf("Rabin fingerprint diverges from stray-free twin: got %x want %x", gotFP, wantFP)
							}
						}

						// B15 axis 3: String() must reparse (schema-parse-time
						// validation accepts the as-written form it printed).
						s2, err := avro.Parse(s.String())
						if err != nil {
							t.Fatalf("reparse of String(): %v", err)
						}
						if !bytes.Equal(s2.Canonical(), s.Canonical()) {
							t.Errorf("String() reparse changed Canonical: %s -> %s", s.Canonical(), s2.Canonical())
						}
						c.assertRouting(t, s2, "String() reparse")

						// B15 axis 4: the metadata rebuild must produce a schema
						// that parses — the newly accepted shapes must rebuild.
						root := s.Root()
						rb, err := root.Schema()
						if err != nil {
							t.Fatalf("Root().Schema() rebuild: %v", err)
						}
						if !bytes.Equal(rb.Canonical(), s.Canonical()) {
							t.Errorf("rebuild changed Canonical: %s -> %s", s.Canonical(), rb.Canonical())
						}
						c.assertRouting(t, rb, "Root().Schema() rebuild")
					})
				}
			}
		}
	}
}

// TestMatrix_StrayPrecisionScaleFastavroPCF drives every twmb-ACCEPTED cell of
// the placement x kind x level x body grid through fastavro by execution:
// fastavro must parse the same spelling — it validates decimal parameters only
// where they are consumed — and its to_parsing_canonical_form must equal twmb's
// Canonical(), PCF stripping attributes it does not define, which is also why
// the Rabin fingerprints agree cross-impl. Reject cells are not driven: they are
// the documented keep-strict side. The flat field spelling is likewise absent —
// fastavro rejects it outright, so its cross-impl anchor is the nested twin.
func TestMatrix_StrayPrecisionScaleFastavroPCF(t *testing.T) {
	o := startOracle(t)
	check := func(t *testing.T, src string) {
		t.Helper()
		parse := o.call(oracleJob{Op: "parse", Schema: json.RawMessage(src)})
		if !parse.OK {
			t.Fatalf("fastavro rejected a twmb-accepted spelling: %s\n%s", parse.Err, src)
		}
		resp := o.call(oracleJob{Op: "canonical", Schema: json.RawMessage(src)})
		if !resp.OK {
			t.Fatalf("fastavro canonical failed for %s: %s", src, resp.Err)
		}
		got := string(avro.MustParse(src).Canonical())
		if got != resp.Canonical {
			t.Errorf("PCF diverges from fastavro for %s:\n twmb: %s\n fast: %s", src, got, resp.Canonical)
		}
	}
	for _, src := range []string{
		`{"type":"int","precision":3}`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"long","logicalType":"timestamp-millis","precision":3,"scale":1}}]}`,
	} {
		check(t, src)
	}
	// fastavro validates decimal parameters on ANY type-level object carrying
	// logicalType "decimal", carrier kind or not, and its validation demands
	// numeric values (a truthiness gate skips falsy ones, which is why 0 and huge
	// positive ints pass). Java instead drops the whole logical on a non-carrier
	// via fromSchemaIgnoreInvalid without ever reading the params, so it accepts
	// these spellings — twmb follows Java and the general unconsumed-is-inert
	// rule. The divergence is pinned by DIRECTION: these cells must keep rejecting
	// in fastavro, so a release that relaxes flips this loudly.
	fastavroRejects := func(c strayPSCell) bool {
		if c.level != "type" || c.carrier() {
			return false
		}
		if c.placement != "decimal-valid" && c.placement != "decimal-invalid" {
			return false
		}
		switch c.body {
		case "quoted", "float", "nonnum", "array":
			return true
		}
		return false
	}
	for _, level := range []string{"type", "field", "union-field"} {
		for _, placement := range []string{"no-logical", "unknown-logical", "valid-logical", "decimal-valid", "decimal-invalid"} {
			for _, kind := range []string{"int", "bytes", "string", "long", "fixed", "record", "array"} {
				for _, body := range strayPSBodies {
					c := strayPSCell{placement: placement, kind: kind, level: level, body: body}
					if c.rejects() {
						continue
					}
					t.Run(level+"/"+placement+"/"+kind+"/"+body, func(t *testing.T) {
						if fastavroRejects(c) {
							resp := o.call(oracleJob{Op: "parse", Schema: json.RawMessage(c.schemaJSON(false))})
							if resp.OK {
								t.Errorf("fastavro now ACCEPTS a spelling it used to reject (%s); re-check the recorded divergence rationale", c.schemaJSON(false))
							}
							return
						}
						check(t, c.schemaJSON(false))
					})
				}
			}
		}
	}
}

// The stray-"fields" shape verdict IS afieldFromAny's acceptance (one
// predicate defines shape-OK), so the consumed-conditional rule propagates
// into stray routing: a stray fields body whose element carries an
// UNCONSUMED malformed precision parses as fields and surfaces structurally
// as-written (the element pair in the element field's Props), while an
// element with a CONSUMED malformed pair keeps the whole body malformed —
// Props verbatim is its only surface.
func TestRegression_StrayFieldsElementPrecisionRouting(t *testing.T) {
	t.Run("unconsumed-element-shape-ok", func(t *testing.T) {
		// Carrier is a PRIMITIVE: a container kind carrying another kind's
		// defining key is the exclusivity hard-reject, a different rule.
		s := mustParse(t, `{"type":"int","fields":[{"name":"f","type":"int","precision":"x"}]}`)
		n := s.Root()
		if _, ok := n.Props["fields"]; ok {
			t.Errorf("shape-OK stray fields leaked into Props: %#v", n.Props)
		}
		if len(n.Fields) != 1 {
			t.Fatalf("stray shape-OK fields not surfaced structurally: %#v", n.Fields)
		}
		if got := n.Fields[0].Props["precision"]; got != "x" {
			t.Errorf("element Props[precision] = %#v; want verbatim \"x\"", got)
		}
	})
	t.Run("consumed-element-still-malformed", func(t *testing.T) {
		s := mustParse(t, `{"type":"int","fields":[{"name":"f","type":"bytes","logicalType":"decimal","precision":"x"}]}`)
		n := s.Root()
		if len(n.Fields) != 0 {
			t.Errorf("malformed-element stray fields surfaced structurally: %#v", n.Fields)
		}
		if _, ok := n.Props["fields"]; !ok {
			t.Errorf("malformed stray fields body missing from Props: %#v", n.Props)
		}
	})
}

// A field-level "decimal" whose lift TARGET already carries its own logical type
// never lands: closer-to-the-type wins, so the target keeps its own annotation
// and the field's precision/scale annotate nothing. The pair is therefore inert
// metadata and rides to Props like any custom property, malformed or not.
//
// Inertness is PROVEN on the wire rather than asserted: the encoding is
// byte-identical with scale 0, with scale 2, and with no field-level logical at
// all. The discriminator is the sibling case where the target's own logical IS
// decimal — there the parameters DO land and scale 0 versus 2 diverges, which is
// what keeps this from being loosened into "a target with any annotation of its
// own is inert".
func TestRegression_FieldDecimalNotLandingIsInert(t *testing.T) {
	field := func(target, fieldAttrs string) string {
		return `{"type":"record","name":"R","fields":[{"name":"f","type":` + target + fieldAttrs + `}]}`
	}
	const bigDec = `{"type":"bytes","logicalType":"big-decimal"}`

	t.Run("malformed params are inert and reach Props", func(t *testing.T) {
		for _, target := range []string{
			bigDec,
			`["null",{"type":"fixed","name":"F","size":4,"logicalType":"uuid"}]`,
			`{"type":"bytes","logicalType":"uuid"}`,
		} {
			src := field(target, `,"logicalType":"decimal","precision":"x"`)
			s, err := avro.Parse(src)
			if err != nil {
				t.Fatalf("Parse(%s): a pair that annotates nothing must be inert, not a reject: %v", src, err)
			}
			if got := s.Root().Fields[0].Props["precision"]; got != "x" {
				t.Errorf("target %s: Props[precision] = %#v; an unconsumed pair rides verbatim, want \"x\"", target, got)
			}
		}
	})

	t.Run("wire proves the field decimal never landed", func(t *testing.T) {
		val := map[string]any{"f": big.NewRat(123, 100)}
		var wires []string
		for _, attrs := range []string{
			`,"logicalType":"decimal","precision":4,"scale":2`,
			`,"logicalType":"decimal","precision":4,"scale":0`,
			``, // no field-level logical at all
		} {
			s, err := avro.Parse(field(bigDec, attrs))
			if err != nil {
				t.Fatalf("Parse(%s): %v", attrs, err)
			}
			b, err := s.Encode(val)
			if err != nil {
				t.Fatalf("encode(%s): %v", attrs, err)
			}
			wires = append(wires, hex.EncodeToString(b))
		}
		for i := range wires {
			if wires[i] != wires[0] {
				t.Fatalf("wire differs across field-level params (%v); the field decimal DID land, so the pair is not inert", wires)
			}
		}
	})

	t.Run("discriminator: when the target's own logical IS decimal the params land", func(t *testing.T) {
		const ownDec = `{"type":"bytes","logicalType":"decimal"}`
		val := map[string]any{"f": big.NewRat(123, 100)}

		s2, err := avro.Parse(field(ownDec, `,"logicalType":"decimal","precision":4,"scale":2`))
		if err != nil {
			t.Fatalf("scale 2: %v", err)
		}
		if _, err := s2.Encode(val); err != nil {
			t.Fatalf("scale 2 must encode 123/100 exactly: %v", err)
		}

		s0, err := avro.Parse(field(ownDec, `,"logicalType":"decimal","precision":4,"scale":0`))
		if err != nil {
			t.Fatalf("scale 0: %v", err)
		}
		if _, err := s0.Encode(val); err == nil {
			t.Fatal("scale 0 must refuse 123/100 (it cannot be represented without rounding); if it does not, the field's scale never landed and this cell no longer discriminates")
		}
	})
}

// A field-level logicalType annotation reaches its target through one
// navigation, and the target is written in one of four spellings: a bare
// primitive, a type object, a bare primitive as the first non-null union branch,
// or an object as that branch. All four describe the same post-lift schema, so
// all four must reach the same verdict and the same wire.
//
// The arms disagreed before: the object arm completed missing precision/scale
// onto a target that already declared its own logicalType while the union arm
// declined, so a bytes+decimal with field-level parameters built and its
// ["null", ...] twin rejected. Annotation and parameters are separate questions
// — closer to the type wins the annotation, and the field still completes
// parameters where the effective logical is decimal.
func TestRegression_FieldLogicalLiftSpellingParity(t *testing.T) {
	const fieldAttrs = `,"logicalType":"decimal","precision":4,"scale":2`
	val := map[string]any{"f": big.NewRat(123, 100)}

	build := func(t *testing.T, fieldType string) (string, error) {
		t.Helper()
		s, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":` + fieldType + fieldAttrs + `}]}`)
		if err != nil {
			return "", err
		}
		b, err := s.Encode(val)
		if err != nil {
			return "", err
		}
		return hex.EncodeToString(b), nil
	}

	// Group 1: the target declares its own "decimal" and the field supplies
	// the parameters. Every spelling must build, and the two union spellings
	// must agree with each other (they carry a branch index the nested forms
	// do not).
	t.Run("target declares decimal, field supplies params", func(t *testing.T) {
		nested, err := build(t, `{"type":"bytes","logicalType":"decimal"}`)
		if err != nil {
			t.Fatalf("nested object spelling: %v", err)
		}
		union, err := build(t, `["null",{"type":"bytes","logicalType":"decimal"}]`)
		if err != nil {
			t.Fatalf("union spelling rejected while its nested twin built — the arms disagree: %v", err)
		}
		if union != "02"+nested {
			t.Errorf("union wire %s is not the null-union framing of the nested wire %s", union, nested)
		}
	})

	// Group 2: the field carries the annotation and the target declares
	// none. The bare-primitive and object spellings are the same schema.
	t.Run("field supplies annotation and params", func(t *testing.T) {
		bare, err := build(t, `"bytes"`)
		if err != nil {
			t.Fatalf("bare primitive: %v", err)
		}
		object, err := build(t, `{"type":"bytes"}`)
		if err != nil {
			t.Fatalf("type object: %v", err)
		}
		if bare != object {
			t.Errorf("bare primitive %s and type object %s describe the same schema but encode differently", bare, object)
		}
		unionBare, err := build(t, `["null","bytes"]`)
		if err != nil {
			t.Fatalf("union of bare primitive: %v", err)
		}
		unionObject, err := build(t, `["null",{"type":"bytes"}]`)
		if err != nil {
			t.Fatalf("union of type object: %v", err)
		}
		if unionBare != unionObject {
			t.Errorf("union spellings diverge: bare branch %s, object branch %s", unionBare, unionObject)
		}
		if unionBare != "02"+bare {
			t.Errorf("union wire %s is not the null-union framing of %s", unionBare, bare)
		}
	})
}

// Completing precision/scale only where the effective logical is "decimal" also
// stops them being written into a type whose logical is something else. That is
// unobservable, and this pins the claim rather than reasoning about it: the
// parameters are absent from the canonical form either way, and the wire is
// byte-identical, because nothing but the decimal codec reads them. The pin
// cannot fail by neutering the gate — both behaviors produce the same bytes,
// which is precisely what it asserts — but it WOULD fail if some future consumer
// started reading precision/scale off a non-decimal type.
func TestMatrix_FieldParamsOnNonDecimalTargetAreUnobservable(t *testing.T) {
	for _, c := range []struct{ name, fieldType, logical string }{
		{"bare-primitive-uuid", `"string"`, "uuid"},
		{"bare-primitive-unknown", `"long"`, "nonsense"},
		{"object-big-decimal", `{"type":"bytes","logicalType":"big-decimal"}`, "decimal"},
		{"object-uuid", `{"type":"bytes","logicalType":"uuid"}`, "decimal"},
	} {
		t.Run(c.name, func(t *testing.T) {
			mk := func(params string) *avro.Schema {
				t.Helper()
				s, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":` +
					c.fieldType + `,"logicalType":"` + c.logical + `"` + params + `}]}`)
				if err != nil {
					t.Fatalf("Parse(params=%q): %v", params, err)
				}
				return s
			}
			with, without := mk(`,"precision":4,"scale":2`), mk(``)
			if !bytes.Equal(with.Canonical(), without.Canonical()) {
				t.Errorf("canonical form differs with/without the parameters:\n with: %s\n  w/o: %s", with.Canonical(), without.Canonical())
			}
			// Same for the scale, which is the parameter that would change a
			// decimal encoding if it were ever read here.
			other := mk(`,"precision":4,"scale":0`)
			if !bytes.Equal(with.Canonical(), other.Canonical()) {
				t.Errorf("canonical form depends on the scale on a non-decimal target: %s vs %s", with.Canonical(), other.Canonical())
			}
		})
	}
}

// ---------- straykey_routing_matrix_test.go ----------

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
				mustDecode(t, s, enc, &out)
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
	s := mustParse(t, `{"type":"int","aliases":["a",1]}`)
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
	s := mustParse(t, `{"type":"int","symbols":["a",1]}`)
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
	s := mustParse(t, `{"type":"int","size":3.7}`)
	n := s.Root()
	if _, ok := n.Props["size"]; !ok {
		t.Fatalf("malformed stray size not in Props: %#v", n.Props)
	}
	if n.Size != 0 {
		t.Errorf("malformed stray size fabricated a structural surface: %d (want 0)", n.Size)
	}
}

// ---------- written_zero_test.go ----------

// ---------------------------------------------------------------------------
// An attribute WRITTEN AS ITS DESTINATION'S ZERO is written.
//
// A schema attribute read into a plain string or int has no companion saying
// whether the key appeared, so `x != ""` means two things at once: "the caller
// chose a value" and "the caller chose the value that happens to be my zero".
// Every reader guarded that way is silently wrong for exactly one input, and
// which way it is wrong depends on what the reader does with the answer — a
// validator SKIPS the zero, an emitter DROPS it.
//
// Apache Avro has no such gap, because it decides on the JSON NODE rather than
// on the parsed value, and its emission condition DIFFERS PER ATTRIBUTE:
//
//   - doc emits when non-NULL (:1039 record, :1154 enum, :1367 fixed, :1062
//     field), so a doc written as "" survives;
//   - aliases emits when non-EMPTY (:886 named, :1070 field), so an alias list
//     written as [] is dropped;
//   - order is decided on the node — `if (orderNode != null)
//     Order.valueOf(node.textValue().toUpperCase())` (:1895-1897) — so an order
//     written as "" reaches valueOf and throws;
//   - logicalType is not reserved at all (:175-176), so parseProperties keeps it
//     as an ordinary schema property whatever its content, "" included.
//
// One blanket "was it written" rule for every attribute would therefore ship a
// divergence rather than fix one. These pin the four answers separately, each
// against the reference behavior that decides it.
//
// fastavro 1.12.2 preserves every one of these bodies verbatim (executed), so
// where Java and fastavro disagree the entry naming the deciding rule is the
// authority, and the cases this package deliberately keeps dropping are pinned
// as such rather than left to look like oversights.
// ---------------------------------------------------------------------------

// TestMatrix_EmptyOrderRejected pins the validator half: presence and
// validity are one question, so an order written as the empty string is a
// written order that is not one of the three the spec defines.
func TestMatrix_EmptyOrderRejected(t *testing.T) {
	const host = `{"type":"record","name":"R","fields":[{"name":"f","type":"int"%s}]}`
	if _, err := avro.Parse(strings.Replace(host, "%s", `,"order":""`, 1)); err == nil {
		t.Error(`"order":"" parsed; it is a written order and not one of ascending/descending/ignore`)
	} else if !strings.Contains(err.Error(), "order") {
		t.Errorf("the reject does not name the offending attribute: %v", err)
	}

	// The three legal orders keep parsing, and an ABSENT order stays legal —
	// the check must key on written-ness, not on non-emptiness, or it would
	// reject every field that does not spell one.
	for _, ok := range []string{`,"order":"ascending"`, `,"order":"descending"`, `,"order":"ignore"`, ``} {
		if _, err := avro.Parse(strings.Replace(host, "%s", ok, 1)); err != nil {
			t.Errorf("%q must stay legal: %v", ok, err)
		}
	}

	// Case variants stay rejected. Apache Avro upper-cases before its own
	// lookup, but reserved attribute VALUES are matched by exact spelling
	// here: a variant is a different string, not a different case of the
	// same one.
	for _, bad := range []string{`,"order":"ASCENDING"`, `,"order":"Ignore"`, `,"order":"asc"`} {
		if _, err := avro.Parse(strings.Replace(host, "%s", bad, 1)); err == nil {
			t.Errorf("%q parsed; the order comparison is exact-case", bad)
		}
	}
}

// TestMatrix_EmptyDocSurvivesWhereJavaEmitsOne pins the five placements
// Apache Avro carries a doc on — the four named kinds and the record field —
// where an empty doc is a doc and is emitted.
func TestMatrix_EmptyDocSurvivesWhereJavaEmitsOne(t *testing.T) {
	cases := []struct{ name, src string }{
		{"record", `{"type":"record","name":"R","doc":"","fields":[]}`},
		{"error", `{"type":"error","name":"E","doc":"","fields":[]}`},
		{"enum", `{"type":"enum","name":"En","doc":"","symbols":["A"]}`},
		{"fixed", `{"type":"fixed","name":"F","doc":"","size":1}`},
		{"field", `{"type":"record","name":"R","fields":[{"name":"f","type":"int","doc":""}]}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := avro.Parse(c.src)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			n := s.Root()
			rb, err := n.Schema()
			if err != nil {
				t.Fatalf("rebuild: %v", err)
			}
			if !strings.Contains(rb.String(), `"doc":""`) {
				t.Errorf("the rebuild dropped a written empty doc: %s", rb)
			}
			// A second pass must keep it, or the attribute survives one
			// rebuild and dies on the next.
			rbRoot := rb.Root()
			rb2, err := rbRoot.Schema()
			if err != nil {
				t.Fatalf("second rebuild: %v", err)
			}
			if rb2.String() != rb.String() {
				t.Errorf("emission is not a fixpoint:\n first %s\nsecond %s", rb, rb2)
			}
			// Nothing on the wire side may move: neither the canonical form
			// nor the fingerprint carries doc at all.
			twin := avro.MustParse(strings.Replace(c.src, `"doc":"",`, "", 1))
			if !bytes.Equal(s.Canonical(), twin.Canonical()) {
				t.Errorf("canonical form differs from the doc-free twin: %s vs %s", s.Canonical(), twin.Canonical())
			}
			if !bytes.Equal(s.Fingerprint(avro.NewRabin()), twin.Fingerprint(avro.NewRabin())) {
				t.Error("fingerprint differs from the doc-free twin")
			}
		})
	}
}

// TestMatrix_EmptyLogicalTypeSurvives pins the other preserved attribute.
// logicalType is absent from Apache Avro's reserved set, so it is an ordinary
// schema property there and survives on every kind whatever its content —
// including on a primitive, which is the node the bare-emission shortcut
// collapses and therefore the one that has to consult presence.
func TestMatrix_EmptyLogicalTypeSurvives(t *testing.T) {
	for _, src := range []string{
		`{"type":"int","logicalType":""}`,
		`{"type":"string","logicalType":""}`,
		`{"type":"record","name":"R","logicalType":"","fields":[]}`,
		`{"type":"array","items":"int","logicalType":""}`,
	} {
		s, err := avro.Parse(src)
		if err != nil {
			t.Fatalf("Parse(%s): %v", src, err)
		}
		n := s.Root()
		if n.LogicalType != "" {
			t.Errorf("LogicalType = %q, want the written empty string (%s)", n.LogicalType, src)
		}
		rb, err := n.Schema()
		if err != nil {
			t.Fatalf("rebuild(%s): %v", src, err)
		}
		if !strings.Contains(rb.String(), `"logicalType":""`) {
			t.Errorf("the rebuild dropped a written empty logicalType: %s (from %s)", rb, src)
		}
		rbRoot := rb.Root()
		rb2, err := rbRoot.Schema()
		if err != nil {
			t.Fatalf("second rebuild: %v", err)
		}
		if rb2.String() != rb.String() {
			t.Errorf("emission is not a fixpoint:\n first %s\nsecond %s", rb, rb2)
		}
	}
}

// TestMatrix_EmptyAliasesStayDropped is the other side of the per-attribute
// rule, and the reason a blanket presence mechanism would be wrong. An alias
// list written as [] is EMPTY, and Apache Avro's emission condition for aliases
// is non-EMPTY rather than non-null (Schema.java:886 for a named type, :1070 for
// a field), so where a kind BINDS the key, this package and Apache Avro agree on
// dropping it; fastavro 1.12.2 preserves it, and two of three drop. The scope is
// the BINDING placement — on a kind that does not bind aliases there is no
// Apache Avro condition to follow and the stray-routing posture governs.
func TestMatrix_EmptyAliasesStayDropped(t *testing.T) {
	drops := []struct{ name, src string }{
		{"type-level", `{"type":"record","name":"R","aliases":[],"fields":[]}`},
		{"field-level", `{"type":"record","name":"R","fields":[{"name":"f","type":"int","aliases":[]}]}`},
	}
	for _, c := range drops {
		t.Run(c.name, func(t *testing.T) {
			s := avro.MustParse(c.src)
			root := s.Root()
			rb := mustNodeSchema(t, root)
			if strings.Contains(rb.String(), `"aliases"`) {
				t.Errorf("an empty alias list survived where the kind BINDS the key; Apache Avro's condition there is non-empty: %s", rb)
			}
		})
	}
	// The control: a non-empty list survives, so the drop is about the BODY
	// and not about the attribute being unsupported.
	keeps := []struct{ name, src string }{
		{"type-level", `{"type":"record","name":"R","aliases":["X"],"fields":[]}`},
		{"field-level", `{"type":"record","name":"R","fields":[{"name":"f","type":"int","aliases":["x"]}]}`},
	}
	for _, c := range keeps {
		t.Run("control/"+c.name, func(t *testing.T) {
			s := avro.MustParse(c.src)
			root := s.Root()
			rb := mustNodeSchema(t, root)
			if !strings.Contains(rb.String(), `"aliases"`) {
				t.Errorf("the non-empty control lost the aliases too, so the drop above is not about the body: %s", rb)
			}
		})
	}
}

// TestMatrix_PrimitiveDocSurvivesEitherWay pins the placement-authority rule,
// which is what decides a cell no single reference can. Apache Avro has no doc
// slot on a primitive or a container at all — parseDoc is called from
// parseRecord/parseEnum/parseFixed and parseField and nowhere else — so it
// neither keeps nor drops one; fastavro does have the placement and preserves
// the attribute. This package already followed fastavro there for a NON-EMPTY
// doc, so the empty twin follows the same authority: deriving it from Apache
// Avro's absence would split one placement between two references and make the
// two bodies of one attribute disagree for no reason a caller could name.
func TestMatrix_PrimitiveDocSurvivesEitherWay(t *testing.T) {
	for _, kind := range []string{`{"type":"int"%s}`, `{"type":"string"%s}`,
		`{"type":"array","items":"int"%s}`, `{"type":"map","values":"int"%s}`} {
		for _, doc := range []string{`,"doc":""`, `,"doc":"d"`} {
			src := strings.Replace(kind, "%s", doc, 1)
			s, err := avro.Parse(src)
			if err != nil {
				t.Fatalf("Parse(%s): %v", src, err)
			}
			root := s.Root()
			rb, err := root.Schema()
			if err != nil {
				t.Fatalf("rebuild(%s): %v", src, err)
			}
			if !strings.Contains(rb.String(), `"doc"`) {
				t.Errorf("the rebuild dropped the doc: %s (from %s)", rb, src)
			}
			// Fixpoint, and the wire side untouched.
			rbRoot := rb.Root()
			rb2, err := rbRoot.Schema()
			if err != nil {
				t.Fatalf("second rebuild: %v", err)
			}
			if rb2.String() != rb.String() {
				t.Errorf("emission is not a fixpoint:\n first %s\nsecond %s", rb, rb2)
			}
			twin := avro.MustParse(strings.Replace(kind, "%s", "", 1))
			if !bytes.Equal(s.Canonical(), twin.Canonical()) {
				t.Errorf("canonical form differs from the doc-free twin: %s vs %s", s.Canonical(), twin.Canonical())
			}
		}
	}
}

// TestMatrix_StrayZeroBodySurvivesTheRebuild covers the placements no reference
// can adjudicate: a structural key written as its destination's ZERO on a kind
// that does not bind it. Apache Avro skips every stray key as reserved and keeps
// none; fastavro keeps every stray key as a property and drops none. Neither is
// answering the question this package's metadata tree poses, so its own
// stray-routing posture governs — the same basis the field-lift and
// consumed-parameter rulings rest on — and that posture says as-written is the
// key's ONLY surface.
//
// The trap this pins, and why each cell asserts the EMITTED FORM's round trip
// rather than only the field's survival: the exclusivity rule is decided on the
// VALUE, so `symbols:["A"]` on an array rejects while `symbols:[]` is accepted.
// Preserving the empty body therefore emits a schema whose own re-parse has to
// be checked — if exclusivity ever became presence-decided, the rebuild would
// start emitting schemas this package rejects.
func TestMatrix_StrayZeroBodySurvivesTheRebuild(t *testing.T) {
	// Each cell: a kind that does NOT bind the key, and the key written as
	// its destination's zero.
	cells := []struct{ name, src, key string }{
		{"name/int", `{"type":"int","name":""}`, "name"},
		{"name/array", `{"type":"array","items":"int","name":""}`, "name"},
		{"namespace/int", `{"type":"int","namespace":""}`, "namespace"},
		{"namespace/map", `{"type":"map","values":"int","namespace":""}`, "namespace"},
		{"aliases/int", `{"type":"int","aliases":[]}`, "aliases"},
		{"aliases/array", `{"type":"array","items":"int","aliases":[]}`, "aliases"},
		{"symbols/int", `{"type":"int","symbols":[]}`, "symbols"},
		{"symbols/array", `{"type":"array","items":"int","symbols":[]}`, "symbols"},
		{"symbols/record", `{"type":"record","name":"R","fields":[],"symbols":[]}`, "symbols"},
		{"size/int", `{"type":"int","size":0}`, "size"},
		{"size/string", `{"type":"string","size":0}`, "size"},
		{"fields/int", `{"type":"int","fields":[]}`, "fields"},
		{"fields/enum", `{"type":"enum","name":"E","symbols":["A"],"fields":[]}`, "fields"},
		{"fields/map", `{"type":"map","values":"int","fields":[]}`, "fields"},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			s, err := avro.Parse(c.src)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			root := s.Root()
			rb, err := root.Schema()
			if err != nil {
				t.Fatalf("rebuild: %v", err)
			}
			if !strings.Contains(rb.String(), `"`+c.key+`"`) {
				t.Errorf("the as-written %q reached NEITHER surface — it is not in Props (the kind consumed it) and the rebuild dropped it: %s",
					c.key, rb)
			}
			// The trap, in three legs. SchemaNode.Schema() parses the tree
			// it marshals, so an emitted body the parser rejects already
			// fails ABOVE, at the rebuild — that leg is why the error check
			// there is a Fatalf and not a skip. What the re-parse adds is
			// the RENDERED TEXT: String() is written independently of the
			// tree Schema() handed the parser, so a divergence between them
			// shows up only here.
			back, err := avro.Parse(rb.String())
			if err != nil {
				t.Fatalf("the rendered text fails its own re-parse: %v\n  emitted: %s", err, rb)
			}
			backRoot := back.Root()
			rb2, err := backRoot.Schema()
			if err != nil {
				t.Fatalf("second rebuild: %v", err)
			}
			if rb2.String() != rb.String() {
				t.Errorf("emission is not a fixpoint:\n first %s\nsecond %s", rb, rb2)
			}
			// The wire side must not move: none of these keys reaches the
			// canonical form on a kind that does not bind it.
			twin := avro.MustParse(stripKey(c.src, c.key))
			if !bytes.Equal(s.Canonical(), twin.Canonical()) {
				t.Errorf("canonical form differs from the attribute-free twin: %s vs %s",
					s.Canonical(), twin.Canonical())
			}
		})
	}
}

// TestMatrix_StrayReadableBodyStillRejectsOnExclusivity is the boundary
// the preservation above must not cross: a stray defining key that parsed as
// a REAL definition still hard-rejects on a kind that binds another one. The
// exclusivity rule is about a key that defines something, and an empty body
// defines nothing — which is exactly why the two verdicts differ.
func TestMatrix_StrayReadableBodyStillRejectsOnExclusivity(t *testing.T) {
	for _, src := range []string{
		`{"type":"array","items":"int","symbols":["A"]}`,
		`{"type":"record","name":"R","fields":[],"symbols":["A"]}`,
		`{"type":"enum","name":"E","symbols":["A"],"fields":[{"name":"f","type":"int"}]}`,
		`{"type":"map","values":"int","fields":[{"name":"f","type":"int"}]}`,
		`{"type":"record","name":"R","fields":[],"size":2}`,
	} {
		if _, err := avro.Parse(src); err == nil {
			t.Errorf("a readable defining key on a kind that binds another one stopped rejecting: %s", src)
		}
	}
}

// stripKey removes `,"key":<body>` from a compact schema literal, giving the
// attribute-free twin the wire comparisons above need.
func stripKey(src, key string) string {
	i := strings.Index(src, `,"`+key+`":`)
	if i < 0 {
		return src
	}
	j := i + 1
	depth := 0
	for ; j < len(src); j++ {
		switch src[j] {
		case '[', '{':
			depth++
		case ']', '}':
			if depth == 0 {
				return src[:i] + src[j:]
			}
			depth--
		case ',':
			if depth == 0 {
				return src[:i] + src[j:]
			}
		}
	}
	return src[:i]
}

// TestMatrix_NamespaceStrictnessIsUniform records why a non-string namespace
// rejecting here — where Apache Avro silently ignores it and fastavro keeps it —
// is coherence rather than an accidental third answer.
//
// The tempting analogy is the non-string logicalType, which this package routes
// to Props instead of rejecting. That analogy fails, and the reason is visible in
// one probe: this package also rejects garbage STRING namespaces. A rule that
// accepted every string and rejected every non-string would be judging the JSON
// token type while ignoring the content, which is the shape that made the
// logicalType case incoherent to reject. Here the content is judged too.
//
// Names and namespaces are the one part of the grammar that stays strictly
// validated; only ALIASES relax, because a reader has to be able to alias a
// writer's illegal legacy name.
func TestMatrix_NamespaceStrictnessIsUniform(t *testing.T) {
	const host = `{"type":"record","name":"R","namespace":%s,"fields":[]}`

	// The coherence proof: garbage STRINGS reject too, so the strictness is
	// about what a namespace can be, not about the JSON token class.
	for _, ns := range []string{`"123bad"`, `"has space"`, `"a..b"`, `"weird!"`} {
		if _, err := avro.Parse(fmt.Sprintf(host, ns)); err == nil {
			t.Errorf("namespace %s parsed; if garbage strings were accepted, rejecting non-strings would be judging the token class alone", ns)
		}
	}

	// The cells this pins: a non-string namespace rejects, on the same rule.
	for _, ns := range []string{`null`, `5`, `[]`, `{}`, `true`} {
		if _, err := avro.Parse(fmt.Sprintf(host, ns)); err == nil {
			t.Errorf("namespace %s parsed", ns)
		}
	}

	// The controls: legal namespaces parse, including the explicit-empty
	// null-namespace escape and the dotted form.
	for _, ns := range []string{`"a"`, `"a.b"`, `"a.b.c"`, `""`} {
		if _, err := avro.Parse(fmt.Sprintf(host, ns)); err != nil {
			t.Errorf("namespace %s must stay legal: %v", ns, err)
		}
	}

	// And the contrast that makes the rule a rule: an ALIAS accepts any
	// string, so the strictness is scoped to names and namespaces rather
	// than applied to every string attribute.
	if _, err := avro.Parse(`{"type":"record","name":"R","aliases":["123bad","has space"],"fields":[]}`); err != nil {
		t.Errorf("aliases must stay unvalidated — a reader has to be able to alias a writer's illegal name: %v", err)
	}
}

// ---------- type_level_binding_routing_test.go ----------

// ---------------------------------------------------------------------------
// "default" and "order" at the TYPE level.
//
// Both keys are FIELD attributes: a record field binds each of them, and the
// spec's only type-level binding of either is the enum evolution default. A type
// object of any other kind binds neither, so neither has a structural field to
// land on — and the routing rule the reserved-key rulings share is a
// biconditional: the structural field is set IFF the key was consumed, and Props
// holds exactly the raw keys that were not.
//
// Both references agree on preserving them where the kind does not bind:
//
//   - Java's SCHEMA_RESERVED (Schema.java:175-176) omits BOTH keys, so
//     parsePropertiesAndLogicalType keeps each as a schema PROPERTY on
//     primitives (:1857), records (:1880), arrays (:1940), maps (:1950) and
//     fixed (:1963). ENUM_RESERVED (:178-180) is SCHEMA_RESERVED plus "default"
//     alone, applied at :1928 — so an enum consumes "default" and keeps "order".
//   - fastavro 1.12.2 keeps both keys on the parsed schema of every kind
//     (executed; TestDifferentialFastavroTypeLevelBindingRouting drives the
//     matrix through it per accepted cell).
//
// The FIELD level is the boundary this must not cross: Java's FIELD_RESERVED
// (:503-504) binds both, and so does twmb — a field's "default" and "order" are
// consumed into SchemaField.Default / SchemaField.Order and must never appear in
// SchemaField.Props. The matrix crosses the binding axis precisely so that
// boundary is asserted rather than assumed.
//
// Neither key reaches the wire, the Parsing Canonical Form, or the fingerprint
// on either side of the change: PCF keeps only
// type/name/fields/symbols/items/values/size, and no codec reads an unconsumed
// attribute. Every cell asserts that identity against a twin spelled without the
// key.
// ---------------------------------------------------------------------------

// bindingLevel is where a cell writes the attribute.
type bindingLevel int

const (
	levelType  bindingLevel = iota // on the type object
	levelField                     // on the enclosing record field object
)

func (l bindingLevel) String() string {
	if l == levelType {
		return "type"
	}
	return "field"
}

// bindingCell is one {key} x {kind} x {level} cell of the matrix.
type bindingCell struct {
	key   string
	kind  string
	level bindingLevel
}

// binds states, from the RULINGS rather than from the code, whether the
// placement consumes the attribute into a structural field. A cell that binds
// must keep the key out of Props; a cell that does not must put it there.
func (c bindingCell) binds() bool {
	if c.level == levelField {
		return true // FIELD_RESERVED binds both, on every field
	}
	return c.key == "default" && c.kind == "enum" // the enum evolution default
}

// body is the attribute's JSON value: a legal sort order for "order", and for
// "default" a value the kind can actually take, so a BINDING cell reaches the
// consumer's validation instead of failing for an unrelated reason.
func (c bindingCell) body() string {
	if c.key == "order" {
		return `"ignore"`
	}
	switch c.kind {
	case "null":
		return `null`
	case "boolean":
		return `true`
	case "int", "long":
		return `3`
	case "float", "double":
		return `1.5`
	case "string":
		return `"s"`
	case "bytes":
		return `"AB"`
	case "fixed":
		return `"AAAA"` // len == the size spelled by bindingTypeSchema
	case "enum":
		return `"A"`
	case "record", "error":
		return `{"x":0}`
	case "map":
		return `{}`
	case "array":
		return `[]`
	case "union":
		return `null`
	}
	return `null`
}

// bindingTypeSchema spells the kind as a type object, optionally carrying the
// attribute. Fixed is size 4 so the "fixed" default body above is a legal
// 4-codepoint string.
func bindingTypeSchema(kind, attr string) string {
	switch kind {
	case "fixed":
		return fmt.Sprintf(`{"type":"fixed","name":"BF","size":4%s}`, attr)
	case "enum":
		return fmt.Sprintf(`{"type":"enum","name":"BE","symbols":["A","B"]%s}`, attr)
	case "record":
		return fmt.Sprintf(`{"type":"record","name":"BR","fields":[{"name":"x","type":"int"}]%s}`, attr)
	case "error":
		return fmt.Sprintf(`{"type":"error","name":"BErr","fields":[{"name":"x","type":"int"}]%s}`, attr)
	case "array":
		return fmt.Sprintf(`{"type":"array","items":"int"%s}`, attr)
	case "map":
		return fmt.Sprintf(`{"type":"map","values":"int"%s}`, attr)
	default:
		return fmt.Sprintf(`{"type":%q%s}`, kind, attr)
	}
}

// bindingValue is a wire-encodable value for the kind, matched to
// bindingTypeSchema's spelling (fixed is 4 bytes here, not the census's 8).
func bindingValue(kind string) any {
	if kind == "fixed" {
		return []byte{1, 2, 3, 4}
	}
	return censusValue(kind)
}

// schema returns the cell's schema source, with the attribute present or
// absent. Every cell is wrapped in a host record so the type-level and
// field-level spellings differ only in WHICH object carries the key.
func (c bindingCell) schema(withAttr bool) string {
	typeAttr, fieldAttr := "", ""
	if withAttr {
		if c.level == levelType {
			typeAttr = fmt.Sprintf(`,%q:%s`, c.key, c.body())
		} else {
			fieldAttr = fmt.Sprintf(`,%q:%s`, c.key, c.body())
		}
	}
	return fmt.Sprintf(`{"type":"record","name":"BindHost","fields":[{"name":"a","type":%s%s}]}`,
		bindingTypeSchema(c.kind, typeAttr), fieldAttr)
}

func (c bindingCell) name() string {
	return c.level.String() + "/" + c.key + "/" + c.kind
}

func bindingCells() []bindingCell {
	var cells []bindingCell
	for _, key := range []string{"default", "order"} {
		for _, kind := range censusKinds {
			for _, level := range []bindingLevel{levelType, levelField} {
				cells = append(cells, bindingCell{key: key, kind: kind, level: level})
			}
		}
	}
	return cells
}

// bindingStructuralSurface reads the structural field the attribute lands on
// when it IS consumed, so "consumed" is measured off the metadata API rather
// than inferred from the routing predicate the test is checking.
func bindingStructuralSurface(f avro.SchemaField, c bindingCell) bool {
	switch {
	case c.level == levelField && c.key == "default":
		return f.HasDefault
	case c.level == levelField && c.key == "order":
		return f.Order != ""
	case c.key == "default":
		return f.Type.HasEnumDefault
	default:
		// No type-level kind binds "order": there is no structural field
		// for it to reach, which is exactly why it must ride to Props.
		return false
	}
}

// bindingProps reads the Props map of the object that carried the attribute.
func bindingProps(f avro.SchemaField, c bindingCell) map[string]any {
	if c.level == levelField {
		return f.Props
	}
	return f.Type.Props
}

// TestMatrix_TypeLevelBindingRouting crosses {default, order} x every kind x
// {type object, record field} and asserts the biconditional on both surfaces,
// plus the inertness of the attribute on every representation that is not the
// metadata tree.
func TestMatrix_TypeLevelBindingRouting(t *testing.T) {
	for _, c := range bindingCells() {
		t.Run(c.name(), func(t *testing.T) {
			src := c.schema(true)
			s, err := avro.Parse(src)
			if err != nil {
				t.Fatalf("Parse(%s): %v", src, err)
			}
			f := s.Root().Fields[0]

			structural := bindingStructuralSurface(f, c)
			props := bindingProps(f, c)
			_, inProps := props[c.key]

			// The biconditional: consumed IFF structural, and Props is
			// exactly the raw keys that were not consumed.
			if structural != c.binds() {
				t.Errorf("structural field set = %v, want %v (schema %s)", structural, c.binds(), src)
			}
			if inProps == c.binds() {
				t.Errorf("key in Props = %v, want %v (schema %s, props %#v)", inProps, !c.binds(), src, props)
			}
			if structural && inProps {
				t.Errorf("%q surfaced BOTH structurally and in Props; the routing picks exactly one", c.key)
			}
			if !structural && !inProps {
				t.Errorf("%q surfaced on NEITHER surface — the as-written attribute was dropped (schema %s)", c.key, src)
			}
			// The unconsumed value rides verbatim, not coerced.
			if !c.binds() {
				var want any
				if err := json.Unmarshal([]byte(c.body()), &want); err != nil {
					t.Fatalf("cell body: %v", err)
				}
				if got := props[c.key]; !bindingValueEqual(got, want) {
					t.Errorf("Props[%q] = %#v, want the as-written %#v", c.key, got, want)
				}
			}
			// The other object must stay clean: a type-level key must not
			// leak onto the field, and a field-level key must not leak onto
			// the type.
			other := f.Props
			if c.level == levelField {
				other = f.Type.Props
			}
			if _, leaked := other[c.key]; leaked {
				t.Errorf("%q leaked onto the other object's Props: %#v", c.key, other)
			}

			assertBindingInert(t, s, c)
		})
	}
}

// bindingValueEqual compares a Props value against the JSON literal it was
// written as. Integer literals come back as int64 from the precision-
// preserving decode where a plain json.Unmarshal produces float64, at every
// depth, so both sides are numerically normalized before comparison rather
// than pinning one Go type per nesting level.
func bindingValueEqual(got, want any) bool {
	return reflect.DeepEqual(bindingNumNorm(got), bindingNumNorm(want))
}

func bindingNumNorm(v any) any {
	switch t := v.(type) {
	case int64:
		return float64(t)
	case int:
		return float64(t)
	case json.Number:
		f, err := t.Float64()
		if err != nil {
			return t.String()
		}
		return f
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = bindingNumNorm(e)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = bindingNumNorm(e)
		}
		return out
	}
	return v
}

// assertBindingInert holds the attribute to the metadata tree: the wire bytes,
// the Avro-JSON encoding, the canonical form and the fingerprint must equal
// the twin spelled without it, and String() / Root().Schema() must stay
// canonical-stable. The comparisons are made non-vacuous by requiring both
// sides to be non-empty for the surfaces that can be.
func assertBindingInert(t *testing.T, s *avro.Schema, c bindingCell) {
	t.Helper()
	twinSrc := c.schema(false)
	twin, err := avro.Parse(twinSrc)
	if err != nil {
		t.Fatalf("twin Parse(%s): %v", twinSrc, err)
	}
	val := map[string]any{"a": bindingValue(c.kind)}

	enc, err := s.AppendEncode(nil, val)
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	twinEnc, err := twin.AppendEncode(nil, val)
	if err != nil {
		t.Fatalf("twin AppendEncode: %v", err)
	}
	if !bytes.Equal(enc, twinEnc) {
		t.Errorf("wire diverges from the attribute-free twin:\n got %x\nwant %x", enc, twinEnc)
	}

	jenc, err := s.AppendEncodeJSON(nil, val)
	if err != nil {
		t.Fatalf("AppendEncodeJSON: %v", err)
	}
	twinJSON, err := twin.AppendEncodeJSON(nil, val)
	if err != nil {
		t.Fatalf("twin AppendEncodeJSON: %v", err)
	}
	if !bytes.Equal(jenc, twinJSON) {
		t.Errorf("Avro-JSON diverges from the attribute-free twin:\n got %s\nwant %s", jenc, twinJSON)
	}
	if len(jenc) == 0 {
		t.Error("Avro-JSON came back empty, so the comparison proved nothing")
	}

	canon, twinCanon := s.Canonical(), twin.Canonical()
	if len(canon) == 0 || len(twinCanon) == 0 {
		t.Fatalf("canonical form came back empty (%q / %q), so the comparison proved nothing", canon, twinCanon)
	}
	if !bytes.Equal(canon, twinCanon) {
		t.Errorf("Canonical diverges from the attribute-free twin:\n got %s\nwant %s", canon, twinCanon)
	}
	fp, twinFP := s.Fingerprint(avro.NewRabin()), twin.Fingerprint(avro.NewRabin())
	if len(fp) == 0 || len(twinFP) == 0 {
		t.Fatalf("fingerprint came back empty, so the comparison proved nothing")
	}
	if !bytes.Equal(fp, twinFP) {
		t.Errorf("Rabin diverges from the attribute-free twin: got %x want %x", fp, twinFP)
	}

	// String() is the source text; its reparse must describe the same schema.
	if !strings.Contains(s.String(), c.key) {
		t.Errorf("String() lost the as-written attribute %q: %s", c.key, s.String())
	}
	s2, err := avro.Parse(s.String())
	if err != nil {
		t.Fatalf("String() reparse: %v", err)
	}
	if !bytes.Equal(s2.Canonical(), canon) {
		t.Errorf("String() reparse changed Canonical: %s -> %s", canon, s2.Canonical())
	}

	// Root().Schema() rebuilds from the metadata tree, so it is the surface
	// that loses an attribute the tree never captured. It must round-trip
	// the attribute to the same place the original parse put it.
	root := s.Root()
	rb, err := root.Schema()
	if err != nil {
		t.Fatalf("Root().Schema(): %v", err)
	}
	if !bytes.Equal(rb.Canonical(), canon) {
		t.Errorf("rebuild changed Canonical: %s -> %s", canon, rb.Canonical())
	}
	rf := rb.Root().Fields[0]
	if got := bindingStructuralSurface(rf, c); got != c.binds() {
		t.Errorf("rebuild changed the structural verdict for %q: %v -> %v (%s)", c.key, c.binds(), got, rb)
	}
	if _, got := bindingProps(rf, c)[c.key]; got == c.binds() {
		t.Errorf("rebuild changed the Props verdict for %q: %s", c.key, rb)
	}
}

// TestMatrix_TypeLevelBindingRoutingIsNotVacuous fails when the corpus stops
// spanning the axes the rule distinguishes, so a routing that ignored one of
// them could not pass by never being asked.
func TestMatrix_TypeLevelBindingRoutingIsNotVacuous(t *testing.T) {
	var binding, nonBinding, typeLevel, fieldLevel int
	keys := map[string]int{}
	kinds := map[string]int{}
	for _, c := range bindingCells() {
		if c.binds() {
			binding++
		} else {
			nonBinding++
		}
		if c.level == levelType {
			typeLevel++
		} else {
			fieldLevel++
		}
		keys[c.key]++
		kinds[c.kind]++
	}
	if binding < 15 || nonBinding < 15 {
		t.Errorf("corpus misses a binding verdict: binding=%d nonBinding=%d", binding, nonBinding)
	}
	if typeLevel < 14 || fieldLevel < 14 {
		t.Errorf("corpus misses a level: type=%d field=%d", typeLevel, fieldLevel)
	}
	if len(keys) != 2 || len(kinds) != len(censusKinds) {
		t.Errorf("corpus misses a key or a kind: keys=%v kinds=%d", keys, len(kinds))
	}
	// The one type-level BINDING cell is the enum default; without it the
	// matrix would never exercise a type-level consumer at all.
	var enumDefault bool
	for _, c := range bindingCells() {
		if c.level == levelType && c.binds() {
			enumDefault = c.key == "default" && c.kind == "enum"
		}
	}
	if !enumDefault {
		t.Error("corpus has no type-level BINDING cell; the enum evolution default is the only one and must be present")
	}
}

// TestMatrix_TypeLevelDefaultOrderSurviveTheRebuild pins the specific
// as-written loss the routing rule closes: a type-level attribute the kind
// does not bind has Props as its ONLY metadata surface, so a tree that drops
// it makes Root().Schema() describe a different schema than the input.
//
// Recursive and diamond type graphs are included because a named type's
// SECOND occurrence is a reference rather than a definition, and a reference
// carrying the attribute reaches the metadata splice rather than the plain
// object emitter.
func TestMatrix_TypeLevelDefaultOrderSurviveTheRebuild(t *testing.T) {
	cases := []struct {
		name string
		src  string
		// node picks the node whose Props must carry the attribute.
		node func(avro.SchemaNode) avro.SchemaNode
		key  string
		val  any
	}{
		{
			name: "order-on-primitive",
			src:  `{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"int","order":"ignore"}}]}`,
			node: func(r avro.SchemaNode) avro.SchemaNode { return r.Fields[0].Type },
			key:  "order", val: "ignore",
		},
		{
			name: "default-on-primitive",
			src:  `{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"int","default":3}}]}`,
			node: func(r avro.SchemaNode) avro.SchemaNode { return r.Fields[0].Type },
			key:  "default", val: int64(3),
		},
		{
			name: "order-on-enum-which-binds-only-default",
			src:  `{"type":"enum","name":"E","symbols":["A","B"],"default":"A","order":"ignore"}`,
			node: func(r avro.SchemaNode) avro.SchemaNode { return r },
			key:  "order", val: "ignore",
		},
		{
			name: "order-on-array",
			src:  `{"type":"array","items":"int","order":"ignore"}`,
			node: func(r avro.SchemaNode) avro.SchemaNode { return r },
			key:  "order", val: "ignore",
		},
		{
			// A recursive definition: the attribute sits on the DEFINITION,
			// which the rebuild emits once and references thereafter.
			name: "order-on-recursive-definition",
			src: `{"type":"record","name":"Node","order":"ignore","fields":[
				{"name":"next","type":["null","Node"]}]}`,
			node: func(r avro.SchemaNode) avro.SchemaNode { return r },
			key:  "order", val: "ignore",
		},
		{
			// A diamond: two fields reach the same named type, the second
			// through a wrapped reference carrying the attribute. The
			// wrapper's props merge onto the spliced definition, so the
			// attribute must survive on the rebuilt tree.
			name: "order-on-diamond-reference",
			src: `{"type":"record","name":"Top","fields":[
				{"name":"l","type":{"type":"record","name":"D","fields":[{"name":"v","type":"int"}]}},
				{"name":"r","type":{"type":"D","order":"ignore"}}]}`,
			node: func(r avro.SchemaNode) avro.SchemaNode { return r.Fields[1].Type },
			key:  "order", val: "ignore",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := mustParse(t, c.src)
			n := c.node(*s.Root())
			got, ok := n.Props[c.key]
			if !ok {
				t.Fatalf("%q is absent from the node's only metadata surface: Props=%#v", c.key, n.Props)
			}
			if !reflect.DeepEqual(got, c.val) {
				t.Errorf("Props[%q] = %#v (%T), want %#v", c.key, got, got, c.val)
			}
			root := s.Root()
			rb := mustNodeSchema(t, root)
			if !strings.Contains(rb.String(), `"`+c.key+`"`) {
				t.Errorf("the rebuild dropped the as-written %q: %s", c.key, rb)
			}
			if !bytes.Equal(rb.Canonical(), s.Canonical()) {
				t.Errorf("rebuild changed Canonical: %s -> %s", s.Canonical(), rb.Canonical())
			}
		})
	}
}

// TestRegression_FieldLevelDefaultOrderStayConsumed is the boundary control:
// a record field binds both attributes, so they are consumed into
// SchemaField.Default / SchemaField.Order and must never reach
// SchemaField.Props — matching Java's FIELD_RESERVED (Schema.java:503-504,
// {default, doc, name, order, type, aliases}). Without this the type-level
// routing could be "fixed" by routing everywhere.
func TestRegression_FieldLevelDefaultOrderStayConsumed(t *testing.T) {
	s := mustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"a","type":"int","default":3,"order":"descending"}]}`)
	f := s.Root().Fields[0]
	if !f.HasDefault || f.Default != int32(3) {
		t.Errorf("field default not consumed: HasDefault=%v Default=%#v", f.HasDefault, f.Default)
	}
	if f.Order != "descending" {
		t.Errorf("field order not consumed: %q", f.Order)
	}
	if len(f.Props) != 0 {
		t.Errorf("a field-bound reserved key reached SchemaField.Props: %#v", f.Props)
	}
	if len(f.Type.Props) != 0 {
		t.Errorf("a field attribute leaked onto the field's type node: %#v", f.Type.Props)
	}
}

// ---------------------------------------------------------------------------
// "doc" is the one reserved key that BINDS on every kind while its capture
// silently declines a non-string body — so a non-string doc reaches neither
// structural field nor Props, the single documented exception to the "never
// neither" half of the reserved-key routing rule.
//
// This is exact Apache Avro behavior, and the two tests pin both directions so
// neither can drift alone. Java reads doc ONLY through parseDoc
// (Schema.java:1996-1998, called at :1864/:1912/:1956 and, for a field, :1888),
// which is getOptionalText (:2039-2042) = jsonNode.textValue(), null for ANY
// non-text node including an explicit JSON null; and doc is a member of
// SCHEMA_RESERVED (:176) and FIELD_RESERVED (:504), so parseProperties skips it
// at every call site.
//
// The same membership fact makes a non-string logicalType behave the OPPOSITE
// way: logicalType is absent from SCHEMA_RESERVED, so parseProperties keeps it
// as an ordinary property. One reserved-set membership test predicts both
// routings, which is why the two must not be "made consistent" with each other.
//
// fastavro 1.12.2 preserves a non-string doc verbatim at both levels; the
// references disagree and this package follows Java. Nothing observable on the
// wire depends on it, which each case asserts against a doc-free twin.
// ---------------------------------------------------------------------------

// docBodiesNonString spans the JSON token classes a doc can be written as
// while not being a string. An explicit null is included because it is the
// one shape where a lenient reader could plausibly treat the key as absent
// rather than as present-and-unusable, and Java's textValue() maps both to
// the same null.
var docBodiesNonString = []string{`5`, `[]`, `null`, `{"a":1}`, `true`}

func TestMatrix_NonStringDocDroppedAtBothLevels(t *testing.T) {
	for _, body := range docBodiesNonString {
		t.Run("type-level/"+body, func(t *testing.T) {
			s := mustParse(t, `{"type":"int","doc":`+body+`}`)
			n := s.Root()
			if n.Doc != "" {
				t.Errorf("Doc = %q, want empty: a non-text body cannot become documentation", n.Doc)
			}
			if _, ok := n.Props["doc"]; ok {
				t.Errorf(`"doc" reached Props: %#v — the key is bound on every kind, so Props is not its surface`, n.Props)
			}
			rb := mustNodeSchema(t, n)
			if strings.Contains(rb.String(), `"doc"`) {
				t.Errorf("the rebuild emitted a doc that never landed: %s", rb)
			}
			twin := avro.MustParse(`{"type":"int"}`)
			if !bytes.Equal(s.Canonical(), twin.Canonical()) {
				t.Errorf("canonical form differs from the doc-free twin: %s vs %s", s.Canonical(), twin.Canonical())
			}
			if !bytes.Equal(s.Fingerprint(avro.NewRabin()), twin.Fingerprint(avro.NewRabin())) {
				t.Errorf("fingerprint differs from the doc-free twin: %x vs %x",
					s.Fingerprint(avro.NewRabin()), twin.Fingerprint(avro.NewRabin()))
			}
		})

		t.Run("field-level/"+body, func(t *testing.T) {
			s := mustParse(t, `{"type":"record","name":"R","fields":[
				{"name":"f","type":"int","doc":`+body+`}]}`)
			f := s.Root().Fields[0]
			if f.Doc != "" {
				t.Errorf("SchemaField.Doc = %q, want empty", f.Doc)
			}
			if _, ok := f.Props["doc"]; ok {
				t.Errorf(`"doc" reached SchemaField.Props: %#v — FIELD_RESERVED binds it, so Props is not its surface`, f.Props)
			}
			hostRoot := s.Root()
			rb := mustNodeSchema(t, hostRoot)
			if strings.Contains(rb.String(), `"doc"`) {
				t.Errorf("the rebuild emitted a field doc that never landed: %s", rb)
			}
			twin := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"f","type":"int"}]}`)
			if !bytes.Equal(s.Canonical(), twin.Canonical()) {
				t.Errorf("canonical form differs from the doc-free twin: %s vs %s", s.Canonical(), twin.Canonical())
			}
		})
	}
}

// TestMatrix_StringDocConsumedAndSurfaced is the other direction, and the
// control that keeps the drop above from being "fixed" by never reading doc at
// all: with a string body the key is consumed into the structural field on
// every level and kind that has one, stays out of Props, and survives the
// metadata rebuild.
//
// The body is deliberately non-empty. SchemaNode.Doc has no present/absent
// companion, so an EMPTY doc string is indistinguishable from an absent one
// on the structural field — a separate question about the zero value of a
// string field, not about the token type this pair of tests fixes.
func TestMatrix_StringDocConsumedAndSurfaced(t *testing.T) {
	cases := []struct {
		name string
		src  string
		// doc reads the documentation off the surface that should hold it.
		doc func(avro.SchemaNode) string
	}{
		{
			name: "primitive-type-object",
			src:  `{"type":"int","doc":"d"}`,
			doc:  func(n avro.SchemaNode) string { return n.Doc },
		},
		{
			name: "record",
			src:  `{"type":"record","name":"R","doc":"d","fields":[]}`,
			doc:  func(n avro.SchemaNode) string { return n.Doc },
		},
		{
			name: "enum",
			src:  `{"type":"enum","name":"E","doc":"d","symbols":["A"]}`,
			doc:  func(n avro.SchemaNode) string { return n.Doc },
		},
		{
			name: "fixed",
			src:  `{"type":"fixed","name":"F","doc":"d","size":2}`,
			doc:  func(n avro.SchemaNode) string { return n.Doc },
		},
		{
			name: "array",
			src:  `{"type":"array","items":"int","doc":"d"}`,
			doc:  func(n avro.SchemaNode) string { return n.Doc },
		},
		{
			name: "map",
			src:  `{"type":"map","values":"int","doc":"d"}`,
			doc:  func(n avro.SchemaNode) string { return n.Doc },
		},
		{
			name: "record-field",
			src:  `{"type":"record","name":"R","fields":[{"name":"f","type":"int","doc":"d"}]}`,
			doc:  func(n avro.SchemaNode) string { return n.Fields[0].Doc },
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := avro.Parse(c.src)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			root := s.Root()
			if got := c.doc(*root); got != "d" {
				t.Errorf("doc = %q, want %q — a string body must be consumed into the structural field", got, "d")
			}
			if _, ok := root.Props["doc"]; ok {
				t.Errorf(`a consumed "doc" also reached Props: %#v`, root.Props)
			}
			if !strings.Contains(s.String(), `"doc"`) {
				t.Errorf("String() dropped the doc: %s", s)
			}
			rb, err := root.Schema()
			if err != nil {
				t.Fatalf("rebuild: %v", err)
			}
			if !strings.Contains(rb.String(), `"doc":"d"`) {
				t.Errorf("the rebuild dropped the consumed doc: %s", rb)
			}
			// Re-parsing the rebuild must reach the same surface, or the
			// emitter and the reader disagree about where doc lives.
			back, err := avro.Parse(rb.String())
			if err != nil {
				t.Fatalf("re-parse of the rebuild: %v", err)
			}
			if got := c.doc(*back.Root()); got != "d" {
				t.Errorf("doc did not round-trip through the rebuild: %q", got)
			}
		})
	}
}

// TestDifferentialFastavroTypeLevelBindingRouting executes every accepted cell
// against fastavro and asserts the PRESERVATION claim rather than mere
// acceptance: fastavro 1.12.2 keeps both keys on the parsed schema of every
// kind, which is what makes routing them to Props the cross-implementation
// answer rather than a twmb invention. A release that starts dropping them
// flips these cells loudly.
func TestDifferentialFastavroTypeLevelBindingRouting(t *testing.T) {
	o := startOracle(t)
	for _, c := range bindingCells() {
		if c.level != levelType {
			continue // fastavro's parsed FIELD objects are not top-level dicts
		}
		t.Run(c.name(), func(t *testing.T) {
			src := bindingTypeSchema(c.kind, fmt.Sprintf(`,%q:%s`, c.key, c.body()))
			resp := o.call(oracleJob{Op: "parsedump", Schema: json.RawMessage(src)})
			if !resp.OK {
				t.Fatalf("fastavro rejected a schema twmb accepts: %s\n%s", resp.Err, src)
			}
			var parsed map[string]any
			if err := json.Unmarshal([]byte(resp.Parsed), &parsed); err != nil {
				t.Fatalf("oracle parsedump did not return an object: %v (%s)", err, resp.Parsed)
			}
			if len(parsed) == 0 {
				t.Fatalf("fastavro returned an empty parsed schema, so the comparison proved nothing: %s", resp.Parsed)
			}
			if _, ok := parsed[c.key]; !ok {
				t.Errorf("fastavro no longer preserves %q on %s — the calibration this routing rests on changed: %s",
					c.key, c.kind, resp.Parsed)
			}
		})
	}
}

// ---------- logicaltype_value_test.go ----------

// A logicalType attribute whose JSON value is not a string is inert metadata,
// preserved verbatim in Props — the same treatment an unknown STRING logical
// already gets. No reference rejects the spelling: Java reads only textual
// logicalType props, so a non-text value yields no logical and the prop is
// preserved; fastavro parses and preserves the key verbatim (executed below);
// goavro switches on the value and falls through to the plain type. Only a
// string activates the logical dispatch, so anything else can never name a
// logical and its only coherent reading is a custom property.
func TestMatrix_NonStringLogicalTypeInert(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		schema string
	}{
		{"schema_numeric", `{"type":"int","logicalType":123}`},
		{"schema_null", `{"type":"int","logicalType":null}`},
		{"field_numeric", `{"type":"record","name":"R","fields":[{"name":"f","type":"int","logicalType":123}]}`},
		{"field_null", `{"type":"record","name":"R","fields":[{"name":"f","type":"int","logicalType":null}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := avro.Parse(tc.schema); err != nil {
				t.Errorf("Parse(%s) = %v, want inert accept", tc.schema, err)
			}
		})
	}
}

// TestMatrix_LogicalTypeValueTypes crosses the logicalType attribute's
// VALUE type (valid string / unknown string / numeric / null) with its
// placement (type object, record field object) and asserts the routing:
// a valid string activates the logical (LogicalType field, wire codec
// engaged); an unknown string is inert but still surfaces as-written on
// LogicalType; a non-string is inert and rides in Props (type level) or
// SchemaField.Props (field level) verbatim, exactly like a custom
// property, and never activates a codec — the wire bytes match the
// logical-free twin. Root().Schema() rebuilds preserve every form.
func TestMatrix_LogicalTypeValueTypes(t *testing.T) {
	t.Parallel()

	type cell struct {
		name      string
		val       string // raw JSON for the logicalType value
		wantField string // expected SchemaNode.LogicalType
		wantProps any    // expected Props["logicalType"] (nil = absent)
		wireInert bool   // int encode must match the logical-free twin
	}
	cells := []cell{
		{"valid_string", `"date"`, "date", nil, false},
		{"unknown_string", `"not-a-logical"`, "not-a-logical", nil, true},
		{"numeric", `123`, "", int64(123), true},
		{"null", `null`, "", nil, true}, // Props stores JSON null as Go nil
	}

	for _, c := range cells {
		t.Run("type_level_"+c.name, func(t *testing.T) {
			s := mustParse(t, `{"type":"int","logicalType":`+c.val+`}`)
			n := s.Root()
			if n.LogicalType != c.wantField {
				t.Errorf("LogicalType = %q, want %q", n.LogicalType, c.wantField)
			}
			if c.wantField == "" {
				// Inert non-string: Props carries the raw value verbatim.
				got, ok := n.Props["logicalType"]
				if !ok {
					t.Fatalf("non-string logicalType not in Props: %#v", n.Props)
				}
				if !reflect.DeepEqual(got, c.wantProps) {
					t.Errorf("Props[logicalType] = %#v, want %#v", got, c.wantProps)
				}
			} else if _, ok := n.Props["logicalType"]; ok {
				t.Errorf("string logicalType leaked into Props: %#v", n.Props)
			}
			if c.wireInert {
				// The logical must not engage any codec: the wire image of a
				// plain int value is byte-identical to the logical-free twin.
				twin := avro.MustParse(`"int"`)
				got, err := s.Encode(int32(7))
				if err != nil {
					t.Fatalf("encode with inert logicalType: %v", err)
				}
				want, err := twin.Encode(int32(7))
				if err != nil {
					t.Fatalf("twin encode: %v", err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("inert logicalType changed wire bytes: %x vs %x", got, want)
				}
			}
			// The rebuild preserves the attribute (on LogicalType or Props).
			rb := mustNodeSchema(t, n)
			rn := rb.Root()
			if rn.LogicalType != c.wantField {
				t.Errorf("rebuild LogicalType = %q, want %q", rn.LogicalType, c.wantField)
			}
			if c.wantField == "" {
				got, ok := rn.Props["logicalType"]
				if !ok {
					t.Errorf("rebuild dropped the inert logicalType from Props: %#v", rn.Props)
				} else if !reflect.DeepEqual(got, c.wantProps) {
					t.Errorf("rebuild Props[logicalType] = %#v, want %#v", got, c.wantProps)
				}
			}
		})
		t.Run("field_level_"+c.name, func(t *testing.T) {
			s := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"f","type":"int","logicalType":`+c.val+`}]}`)
			f := s.Root().Fields[0]
			// Field-level logicalType always rides in SchemaField.Props
			// as-written on the metadata surface (the wire-side lift onto
			// the field's type is a separate, string-only concession); a
			// non-string spelling takes the identical Props route and lifts
			// nothing — the field's type node stays a plain int.
			if c.wantField == "" && c.wantProps != nil {
				if got := f.Props["logicalType"]; !reflect.DeepEqual(got, c.wantProps) {
					t.Errorf("field Props[logicalType] = %#v, want %#v", got, c.wantProps)
				}
			}
			if c.wantField == "" && f.Type.LogicalType != "" {
				t.Errorf("non-string field logicalType lifted onto the type: %q", f.Type.LogicalType)
			}
		})
	}
}

// TestDifferentialFastavroLogicalTypeValueTypes drives every accepted
// logicalType value-type cell through fastavro's parser: fastavro reads
// logical annotations lazily by string lookup, so every value type
// parses there.
func TestDifferentialFastavroLogicalTypeValueTypes(t *testing.T) {
	o := startOracle(t)
	for _, val := range []string{`"date"`, `"not-a-logical"`, `123`, `null`} {
		for _, cell := range []string{
			`{"type":"int","logicalType":` + val + `}`,
			`{"type":"record","name":"R","fields":[{"name":"f","type":"int","logicalType":` + val + `}]}`,
		} {
			if _, err := avro.Parse(cell); err != nil {
				t.Errorf("twmb rejected a logicalType value-type cell: %v\n%s", err, cell)
				continue
			}
			resp := o.call(oracleJob{Op: "parse", Schema: json.RawMessage(cell)})
			if !resp.OK {
				t.Errorf("fastavro rejected an accepted logicalType cell: %s\n%s", resp.Err, cell)
			}
		}
	}
}

// ---------- nil_quoted_logicalname_test.go ----------

// ─────────────────────────────────────────────────────────────────────────
// A nil *Schema is invalid and methods panic on it (programming error,
// idiomatic Go). This pins that the panic is CONSISTENT across the exported
// method set. The three methods that validate an argument BEFORE touching the
// receiver are exercised twice — once with a valid argument, which must panic
// reaching the receiver deref, and once with the bad argument, which returns the
// arg-validation error, correctly about the argument rather than the receiver.
// See BUG_AUDIT.md §Known intentional divergences.
// ─────────────────────────────────────────────────────────────────────────

// outcome runs fn and reports whether it panicked or returned an error.
func outcome(fn func() error) (panicked bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
		}
	}()
	err = fn()
	return
}

func mustPanic(t *testing.T, name string, fn func() error) {
	t.Helper()
	panicked, err := outcome(fn)
	if !panicked {
		t.Errorf("%s: expected panic on nil *Schema, got err=%v (no panic)", name, err)
	}
}

func mustErrorNotPanic(t *testing.T, name, wantSubstr string, fn func() error) {
	t.Helper()
	panicked, err := outcome(fn)
	if panicked {
		t.Errorf("%s: expected arg-validation error, got panic", name)
		return
	}
	if err == nil || !strings.Contains(err.Error(), wantSubstr) {
		t.Errorf("%s: expected error containing %q, got %v", name, wantSubstr, err)
	}
}

func TestRegression_NilSchemaPanicsConsistently(t *testing.T) {
	var s *avro.Schema // nil receiver
	validPtr := new(int)
	good := avro.MustParse(`"int"`)

	// Receiver-dereferencing methods: every one panics on a nil receiver
	// when given otherwise-valid arguments.
	mustPanic(t, "AppendEncode", func() error { _, e := s.AppendEncode(nil, 1); return e })
	mustPanic(t, "Encode", func() error { _, e := s.Encode(1); return e })
	mustPanic(t, "EncodeJSON", func() error { _, e := s.EncodeJSON(1); return e })
	mustPanic(t, "AppendEncodeJSON", func() error { _, e := s.AppendEncodeJSON(nil, 1); return e })
	mustPanic(t, "Canonical", func() error { _ = s.Canonical(); return nil })
	mustPanic(t, "Fingerprint", func() error { _ = s.Fingerprint(sha256.New()); return nil })
	mustPanic(t, "String", func() error { _ = s.String(); return nil })
	mustPanic(t, "Root", func() error { _ = s.Root(); return nil })
	mustPanic(t, "AppendSingleObject", func() error { _, e := s.AppendSingleObject(nil, 1); return e })

	// Decode / DecodeJSON / DecodeSingleObject validate the ARGUMENT first.
	// With a VALID argument they reach the receiver deref and panic; with the
	// BAD argument they surface the arg-validation error (correct — about the
	// argument, not the receiver).
	mustPanic(t, "Decode(valid target)", func() error { _, e := s.Decode([]byte{0}, validPtr); return e })
	mustErrorNotPanic(t, "Decode(nil target)", "non-nil pointer", func() error { _, e := s.Decode([]byte{0}, nil); return e })

	mustPanic(t, "DecodeJSON(valid target)", func() error { return s.DecodeJSON([]byte("1"), validPtr) })
	mustErrorNotPanic(t, "DecodeJSON(nil target)", "non-nil pointer", func() error { return s.DecodeJSON([]byte("1"), nil) })

	validHeader := append([]byte{0xC3, 0x01}, make([]byte, 8)...)
	mustPanic(t, "DecodeSingleObject(valid header)", func() error { _, e := s.DecodeSingleObject(validHeader, validPtr); return e })
	mustErrorNotPanic(t, "DecodeSingleObject(short header)", "too short", func() error { _, e := s.DecodeSingleObject([]byte{0x01}, validPtr); return e })

	// Resolve / CheckCompatibility panic when EITHER *Schema argument is nil
	// (each dereferences writer.node / reader.node before any guard runs).
	mustPanic(t, "Resolve(nil writer)", func() error { _, e := avro.Resolve(nil, good); return e })
	mustPanic(t, "Resolve(nil reader)", func() error { _, e := avro.Resolve(good, nil); return e })
	mustPanic(t, "CheckCompatibility(nil writer)", func() error { return avro.CheckCompatibility(nil, good) })
	mustPanic(t, "CheckCompatibility(nil reader)", func() error { return avro.CheckCompatibility(good, nil) })

	// *SchemaNode.Schema panics on a nil *SchemaNode receiver.
	var sn *avro.SchemaNode
	mustPanic(t, "SchemaNode.Schema", func() error { _, e := sn.Schema(); return e })

	// SchemaCache.Parse panics on a nil *SchemaCache receiver.
	var sc *avro.SchemaCache
	mustPanic(t, "SchemaCache.Parse", func() error { _, e := sc.Parse(`"int"`); return e })
}

// ─────────────────────────────────────────────────────────────────────────
// Quoted "size" / "precision" / "scale" at parse, mirroring Apache Avro. Java
// REJECTS all three when quoted: size via `!sizeNode.isInt()`, and
// precision/scale via Decimal.getInt's `obj instanceof Integer`, a quoted value
// deserializing to a String. twmb mirrors Java on precision/scale and is
// intentionally MORE lenient on size, accepting a quoted one per the spec's
// [INTEGERS] Parsing-Canonical-Form rule ("Eliminate quotes around ... JSON
// integer literals (which appear in the size attributes of fixed schemas)"), via
// laxInt. This pins the exact accept/reject at BOTH parse and Root()
// metadata-read. See BUG_AUDIT.md §Known intentional divergences.
// ─────────────────────────────────────────────────────────────────────────

func TestRegression_QuotedSizePrecisionScaleMirrorsJava(t *testing.T) {
	accept := func(name, schema string) {
		t.Helper()
		if _, err := avro.Parse(schema); err != nil {
			t.Errorf("%s: expected ACCEPT, got reject: %v", name, err)
		}
	}
	reject := func(name, schema string) {
		t.Helper()
		if _, err := avro.Parse(schema); err == nil {
			t.Errorf("%s: expected REJECT, got accept", name)
		}
	}

	// size: numeric accepted; quoted accepted (twmb is more lenient than
	// Java here, per spec [INTEGERS]).
	accept("size numeric", `{"type":"fixed","name":"F","size":16}`)
	accept("size quoted", `{"type":"fixed","name":"F","size":"16"}`)
	accept("size quoted leading-zero", `{"type":"fixed","name":"F","size":"016"}`)

	// precision/scale: numeric accepted; quoted REJECTED (mirrors Java).
	accept("decimal numeric prec/scale", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)
	reject("decimal quoted precision", `{"type":"bytes","logicalType":"decimal","precision":"10","scale":2}`)
	reject("decimal quoted scale", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":"2"}`)
	reject("decimal both quoted", `{"type":"bytes","logicalType":"decimal","precision":"10","scale":"2"}`)
	reject("fixed-decimal quoted precision", `{"type":"fixed","name":"D","size":8,"logicalType":"decimal","precision":"10","scale":2}`)

	// Root() metadata-read agrees with parse for the accepted shapes: a
	// quoted size reads back as the numeric Size, and a numeric decimal
	// surfaces Precision/Scale. (Quoted precision/scale never reach Root()
	// because parse rejects them first — there is no parsed schema to read.)
	sQuotedSize := avro.MustParse(`{"type":"fixed","name":"F","size":"16"}`)
	if got := sQuotedSize.Root().Size; got != 16 {
		t.Errorf("Root().Size for quoted size: got %d, want 16", got)
	}
	sDecimal := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)
	if r := sDecimal.Root(); r.Precision != 10 || r.Scale != 2 {
		t.Errorf("Root() decimal: got precision=%d scale=%d, want 10/2", r.Precision, r.Scale)
	}
}

// ─────────────────────────────────────────────────────────────────────────
// Under TagLogicalTypes, a NAMED fixed carrying a logical type tags its
// tagged-union branch under the fixed's fully-qualified NAME, not
// "fixed.<logicalType>" — matching linkedin/goavro, where the envelope key is
// the branch codec's typeName.fullName, and Apache Avro's JsonEncoder, which
// uses getFullName(). The "<kind>.<logicalType>" qualifier is retained ONLY for
// primitive-backed logicals, which is goavro's convention and the reason
// TagLogicalTypes exists. The encoding stays binary/JSON uniform, round-trips,
// and the decoder still ACCEPTS the legacy form. See BUG_AUDIT.md §Known
// intentional divergences.
// ─────────────────────────────────────────────────────────────────────────

func TestMatrix_NamedFixedLogicalTaggedUnionName(t *testing.T) {
	// keyOf returns the single key of a tagged JSON union envelope
	// {"key":...} produced by EncodeJSON.
	keyOf := func(t *testing.T, b []byte) string {
		t.Helper()
		s := string(b)
		if len(s) < 4 || s[0] != '{' || s[1] != '"' {
			t.Fatalf("not a tagged envelope: %s", s)
		}
		end := strings.IndexByte(s[2:], '"')
		if end < 0 {
			t.Fatalf("malformed envelope: %s", s)
		}
		return s[2 : 2+end]
	}

	uuidVal := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	decVal := big.NewRat(123, 100)
	durVal := avro.Duration{Months: 1, Days: 2, Milliseconds: 3}
	tsVal := time.Unix(0, 0).UTC()

	cases := []struct {
		name       string
		schema     string
		input      any
		tagLogical bool
		wantKey    string // exact tagged key under the given options
	}{
		// Named fixed + logical: name wins regardless of TagLogicalTypes.
		{"fixed-uuid TaggedUnions", `["null",{"type":"fixed","name":"F","size":16,"logicalType":"uuid"}]`, uuidVal, false, "F"},
		{"fixed-uuid +TagLogical", `["null",{"type":"fixed","name":"F","size":16,"logicalType":"uuid"}]`, uuidVal, true, "F"},
		{"fixed-decimal TaggedUnions", `["null",{"type":"fixed","name":"D","size":8,"logicalType":"decimal","precision":10,"scale":2}]`, decVal, false, "D"},
		{"fixed-decimal +TagLogical", `["null",{"type":"fixed","name":"D","size":8,"logicalType":"decimal","precision":10,"scale":2}]`, decVal, true, "D"},
		{"fixed-duration TaggedUnions", `["null",{"type":"fixed","name":"Dur","size":12,"logicalType":"duration"}]`, durVal, false, "Dur"},
		{"fixed-duration +TagLogical", `["null",{"type":"fixed","name":"Dur","size":12,"logicalType":"duration"}]`, durVal, true, "Dur"},
		// Unnamed primitive-backed logical: keeps the <kind>.<logical> form
		// only under TagLogicalTypes, else the bare kind.
		{"long-timestamp TaggedUnions", `["null",{"type":"long","logicalType":"timestamp-millis"}]`, tsVal, false, "long"},
		{"long-timestamp +TagLogical", `["null",{"type":"long","logicalType":"timestamp-millis"}]`, tsVal, true, "long.timestamp-millis"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := avro.MustParse(c.schema)
			opts := []avro.Opt{avro.TaggedUnions()}
			if c.tagLogical {
				opts = append(opts, avro.TagLogicalTypes())
			}

			// JSON encode emits the expected key.
			jb := mustEncodeJSON(t, s, c.input, opts...)
			if got := keyOf(t, jb); got != c.wantKey {
				t.Errorf("EncodeJSON tagged key: got %q, want %q (%s)", got, c.wantKey, jb)
			}

			// Binary decode into *any wraps under the SAME key — binary↔JSON
			// uniformity for the tagged-union name.
			wire := mustEncode(t, s, c.input)
			var decoded any
			mustDecode(t, s, wire, &decoded, opts...)
			m, ok := decoded.(map[string]any)
			if !ok {
				t.Fatalf("decoded not a tagged map: %#v", decoded)
			}
			if _, ok := m[c.wantKey]; !ok {
				t.Errorf("binary decode wrap key: got %v, want %q", mapKeys(m), c.wantKey)
			}

			// JSON round-trip through the emitted form.
			var jround any
			if err := s.DecodeJSON(jb, &jround, opts...); err != nil {
				t.Errorf("DecodeJSON round-trip of emitted form failed: %v", err)
			}
		})
	}

	// Backward compatibility: the decoder still ACCEPTS the legacy
	// "fixed.<logicalType>" tagged key even though it is no longer emitted.
	t.Run("legacy fixed.uuid still decodes", func(t *testing.T) {
		s := avro.MustParse(`["null",{"type":"fixed","name":"F","size":16,"logicalType":"uuid"}]`)
		// Obtain a valid 16-byte codepoint-string body from the encoder.
		jb, _ := s.EncodeJSON([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, avro.TaggedUnions())
		body := string(jb[strings.IndexByte(string(jb), ':')+1 : len(jb)-1])
		legacy := `{"fixed.uuid":` + body + `}`
		var out any
		if err := s.DecodeJSON([]byte(legacy), &out, avro.TaggedUnions()); err != nil {
			t.Errorf("legacy fixed.uuid tagged JSON must still decode: %v", err)
		}
	})
}

func mapKeys(m map[string]any) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}

// ---------- null_body_presence_test.go ----------

// ---------------------------------------------------------------------------
// An explicit JSON null is a PRESENT attribute whose body names no value, and it
// is the one body shape a typed decode accepts in silence.
//
// encoding/json documents it: unmarshaling null into a pointer sets the pointer
// to nil, and into any non-pointer destination "has no effect on the value and
// produces no error". So a reader that decides presence by asking "did the
// decode fail" reads a present-but-undecodable attribute as ABSENT and hands
// back the destination's zero.
//
// That zero is not neutral for Avro schema attributes. A fixed's size 0 is a
// legal, distinct schema, and a decimal's scale 0 is a legal, distinct scale
// that changes the wire meaning of every value written against it. Coercing a
// null body to either silently substitutes a different schema for the one
// written.
//
// The references never produce those schemas:
//
//   - Java REJECTS a null size outright: parseFixed requires sizeNode.isInt()
//     (Schema.java:1957-1960), and NullNode.isInt() is false.
//   - Java never builds decimal(p,0) from a null scale either: the Decimal
//     logical reads each parameter through getInt, which throws unless the prop
//     is an Integer (LogicalTypes.java:414-421), and parse calls
//     fromSchemaIgnoreInvalid (Schema.java:1979), which swallows the throw and
//     drops the logical, leaving plain bytes. Silently dropping a decimal
//     annotation is the hazard this package declined to copy, so it rejects
//     loudly where Java soft-drops — but neither produces decimal(p,0).
//   - fastavro 1.12.2 accepts these at parse and then FAILS every write against
//     the result (executed). Its accept is not a usable accept, so the
//     permissive lean has nothing to lean toward.
//     TestDifferentialFastavroNullBodyIsNotUsable executes that calibration.
//
// The rule: a null body is a MALFORMED body, never an absent one, so it reaches
// exactly the verdict its wrong-typed siblings reach at every key and placement.
// The boundary that must NOT move: a written 0 is a value, not a null, and every
// zero-valued attribute keeps parsing.
// ---------------------------------------------------------------------------

// TestRegression_NullSizeRejectedOnFixed holds the fixed-size boundary from
// both sides: an explicit null size is rejected because it names no width,
// while the written zero width that Java, fastavro and avro-rs all accept
// keeps parsing and keeps encoding.
func TestRegression_NullSizeRejectedOnFixed(t *testing.T) {
	if _, err := avro.Parse(`{"type":"fixed","name":"F","size":null}`); err == nil {
		t.Error("a null size parsed; it names no width, so there is no fixed to build")
	} else if !strings.Contains(err.Error(), "size") {
		t.Errorf("the reject does not name the offending key: %v", err)
	}

	// A written zero is a VALUE, and a legal one: a zero-width fixed is
	// reference-legal and usable. The reject above must not reach it.
	zero, err := avro.Parse(`{"type":"fixed","name":"F","size":0}`)
	if err != nil {
		t.Fatalf(`"size":0 must stay legal, it is a width: %v`, err)
	}
	if got := zero.Root().Size; got != 0 {
		t.Errorf("zero-width fixed Size = %d, want 0", got)
	}
	b, err := zero.Encode([]byte{})
	if err != nil || len(b) != 0 {
		t.Errorf("zero-width fixed encode = %x, %v; want no bytes and no error", b, err)
	}

	// The quoted integer form stays accepted: the Parsing Canonical Form
	// rule blesses a quoted size, and quoting is a spelling of a value, not
	// an absence.
	quoted, err := avro.Parse(`{"type":"fixed","name":"F","size":"2"}`)
	if err != nil {
		t.Fatalf(`quoted "size" must stay accepted: %v`, err)
	}
	if got := quoted.Root().Size; got != 2 {
		t.Errorf("quoted size read back as %d, want 2", got)
	}
	if _, err := avro.Parse(`{"type":"fixed","name":"F","size":"0"}`); err != nil {
		t.Errorf(`quoted "0" must stay accepted, it is the same width as 0: %v`, err)
	}

	// A quoted null is a STRING whose content is not an integer, so it fails
	// for the ordinary reason and not through the null path.
	if _, err := avro.Parse(`{"type":"fixed","name":"F","size":"null"}`); err == nil {
		t.Error(`"size":"null" parsed; the string "null" is not an integer`)
	}
}

// TestMatrix_NullDecimalParamsRejectedWhereConsumed pins the decimal
// parameters at the placements that consume them. A null scale must not become
// scale 0: the two schemas encode different bytes for the same value, so
// accepting one as the other silently rewrites the wire contract.
func TestMatrix_NullDecimalParamsRejectedWhereConsumed(t *testing.T) {
	carriers := []struct {
		name string
		src  func(precision, scale string) string
	}{{
		name: "bytes-type-object",
		src: func(p, s string) string {
			return `{"type":"bytes","logicalType":"decimal","precision":` + p + `,"scale":` + s + `}`
		},
	}, {
		name: "fixed-type-object",
		src: func(p, s string) string {
			return `{"type":"fixed","name":"D","size":8,"logicalType":"decimal","precision":` + p + `,"scale":` + s + `}`
		},
	}, {
		name: "field-lift-onto-bytes",
		src: func(p, s string) string {
			return `{"type":"record","name":"R","fields":[{"name":"f","type":"bytes",` +
				`"logicalType":"decimal","precision":` + p + `,"scale":` + s + `}]}`
		},
	}, {
		name: "field-lift-onto-union-branch",
		src: func(p, s string) string {
			return `{"type":"record","name":"R","fields":[{"name":"f","type":["null","bytes"],` +
				`"logicalType":"decimal","precision":` + p + `,"scale":` + s + `}]}`
		},
	}}

	for _, c := range carriers {
		t.Run(c.name, func(t *testing.T) {
			_, err := avro.Parse(c.src("4", "null"))
			if err == nil {
				t.Error("a null scale parsed; scale 0 is a different schema than the one written")
			} else if !strings.Contains(err.Error(), "scale") {
				t.Errorf("the reject does not name the offending key: %v", err)
			}

			// precision is the sibling, and its reject must come from the
			// null itself. A zero precision is separately illegal (it can
			// hold no digits), so a guard that only checked positivity would
			// mask a missing null check here: the error must name the key as
			// a JSON-shape problem rather than report a precision of 0.
			_, err = avro.Parse(c.src("null", "2"))
			if err == nil {
				t.Fatal("a null precision parsed")
			}
			if !strings.Contains(err.Error(), "precision") {
				t.Errorf("the reject does not name the offending key: %v", err)
			}
			if strings.Contains(err.Error(), "positive") {
				t.Errorf("the null precision was rejected by the positivity check on a coerced 0, "+
					"not by a check on the body that was written: %v", err)
			}

			// The valid twin must still parse, or the rejects above prove
			// nothing about the null in particular.
			if _, err := avro.Parse(c.src("4", "2")); err != nil {
				t.Fatalf("the valid twin stopped parsing: %v", err)
			}
			// Zero is a value on both parameters where it is legal: scale 0
			// is the spec's own default, and it must keep parsing.
			if _, err := avro.Parse(c.src("4", "0")); err != nil {
				t.Errorf(`"scale":0 must stay legal, it is the spec default: %v`, err)
			}
		})
	}
}

// TestRegression_NullScaleWouldChangeTheWire is the consequence that makes the
// reject above a correctness matter rather than a strictness preference: the
// same value encodes to different bytes at scale 2 and scale 0, and at scale 0
// a two-decimal value cannot be represented at all.
func TestRegression_NullScaleWouldChangeTheWire(t *testing.T) {
	type rec struct {
		F float64 `avro:"f"`
	}
	scaled := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"f","type":` +
		`{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}}]}`)
	want, err := scaled.Encode(rec{F: 1.23})
	if err != nil {
		t.Fatalf("scale 2 encode: %v", err)
	}
	if len(want) == 0 {
		t.Fatal("scale 2 encoded to nothing, so the comparison would prove nothing")
	}

	zeroScale := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"f","type":` +
		`{"type":"bytes","logicalType":"decimal","precision":4,"scale":0}}]}`)
	if _, err := zeroScale.Encode(rec{F: 1.23}); err == nil {
		t.Fatal("scale 0 accepted a two-decimal value; the two scales must differ observably " +
			"or the null-scale reject would be arbitrary")
	}

	// With the null rejected at parse there is no schema to encode against,
	// which is the point: the caller learns at parse time instead of holding
	// a schema whose scale nobody wrote.
	if _, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":` +
		`{"type":"bytes","logicalType":"decimal","precision":4,"scale":null}}]}`); err == nil {
		t.Error("a null scale parsed inside a nested field type")
	}
}

// TestMatrix_NullStrayBodyRidesToProps is the other half of the rule. At a
// placement the kind does not bind, a malformed body is inert metadata whose
// ONLY surface is Props, as-written — so a null must land there like every
// other malformed body instead of vanishing from both surfaces.
//
// The container and named hosts also exercise the exclusivity rule, which
// rejects a kind carrying ANOTHER kind's defining key only when that key
// parsed as a real definition; a body that names no value defines nothing, so
// it must route like the other malformed bodies rather than trip exclusivity.
func TestMatrix_NullStrayBodyRidesToProps(t *testing.T) {
	hosts := []struct{ name, src string }{
		{"int", `{"type":"int","size":null}`},
		{"string", `{"type":"string","size":null}`},
		{"array", `{"type":"array","items":"int","size":null}`},
		{"map", `{"type":"map","values":"int","size":null}`},
		{"record", `{"type":"record","name":"R","fields":[],"size":null}`},
		{"enum", `{"type":"enum","name":"E","symbols":["A"],"size":null}`},
	}
	for _, h := range hosts {
		t.Run(h.name, func(t *testing.T) {
			s, err := avro.Parse(h.src)
			if err != nil {
				t.Fatalf("a stray null body must be inert metadata, not a reject: %v", err)
			}
			n := s.Root()
			v, ok := n.Props["size"]
			if !ok {
				t.Fatalf("the stray null reached NEITHER surface; Props is its only surface: %#v", n.Props)
			}
			if v != nil {
				t.Errorf("Props[%q] = %#v, want the as-written null", "size", v)
			}
			if n.Size != 0 {
				t.Errorf("a declined body still set the structural Size = %d", n.Size)
			}
			// The rebuild reads only the metadata tree, so it is where an
			// attribute the tree never captured disappears.
			rb, err := n.Schema()
			if err != nil {
				t.Fatalf("Root().Schema(): %v", err)
			}
			if !strings.Contains(rb.String(), `"size":null`) {
				t.Errorf("the rebuild lost the as-written null: %s", rb)
			}
			// The malformed sibling must reach the same place, or "null" is
			// being treated as a body class of its own.
			sib, err := avro.Parse(strings.Replace(h.src, `"size":null`, `"size":[]`, 1))
			if err != nil {
				t.Fatalf("the malformed sibling stopped parsing: %v", err)
			}
			if _, ok := sib.Root().Props["size"]; !ok {
				t.Errorf("the malformed sibling did not ride to Props: %#v", sib.Root().Props)
			}

			// The boundary on the other side: a READABLE size is a real
			// width, and a container or named kind carrying another kind's
			// defining key as a real definition still hard-rejects. Routing
			// the unreadable bodies to Props must not soften that.
			readable := strings.Replace(h.src, `"size":null`, `"size":2`, 1)
			switch h.name {
			case "int", "string":
				// A primitive object surfaces a readable stray as-written.
				ps, err := avro.Parse(readable)
				if err != nil {
					t.Fatalf("a readable stray size on a primitive must surface as-written: %v", err)
				}
				if ps.Root().Size != 2 {
					t.Errorf("a readable stray size did not reach the structural field: %d", ps.Root().Size)
				}
			default:
				if _, err := avro.Parse(readable); err == nil {
					t.Errorf("a readable size on a %s stopped rejecting; the exclusivity rule is "+
						"about keys that parsed as a real definition", h.name)
				}
			}
		})
	}
}

// TestMatrix_NullUnconsumedDecimalParamsRideToProps is the same rule for
// the decimal parameters: where no decimal consumes them they are ordinary
// metadata, so a null body rides through verbatim rather than rejecting.
func TestMatrix_NullUnconsumedDecimalParamsRideToProps(t *testing.T) {
	for _, src := range []string{
		`{"type":"int","scale":null,"precision":null}`,
		`{"type":"bytes","scale":null,"precision":null}`,
		`{"type":"bytes","logicalType":"date","scale":null,"precision":null}`,
		`{"type":"record","name":"R","fields":[{"name":"f","type":"int","scale":null,"precision":null}]}`,
	} {
		s, err := avro.Parse(src)
		if err != nil {
			t.Fatalf("an unconsumed null param must stay inert metadata: %v (%s)", err, src)
		}
		props := s.Root().Props
		if len(s.Root().Fields) > 0 {
			props = s.Root().Fields[0].Props
		}
		for _, k := range []string{"scale", "precision"} {
			v, ok := props[k]
			if !ok {
				t.Errorf("%q did not reach Props: %#v (%s)", k, props, src)
			} else if v != nil {
				t.Errorf("Props[%q] = %#v, want the as-written null (%s)", k, v, src)
			}
		}
	}
}

// TestRegression_NullDefaultIsAValue holds the boundary the matrix below
// excludes: a default's body is kept as raw JSON rather than decoded into a
// typed destination, so a null default is the null VALUE and must stay
// accepted and distinguishable from an absent default.
func TestRegression_NullDefaultIsAValue(t *testing.T) {
	s, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":["null","int"],"default":null}]}`)
	if err != nil {
		t.Fatalf("a null default is a legal value for a null-first union: %v", err)
	}
	f := s.Root().Fields[0]
	if !f.HasDefault {
		t.Error("HasDefault = false for a written null default; presence is not the value")
	}
	if f.Default != nil {
		t.Errorf("Default = %#v, want the null value", f.Default)
	}
	// The enum-LEVEL default is the one place a default's body IS read into
	// a typed destination, and it decides by token type before membership, so
	// a null rejects there. That check is the shape every other typed read
	// has to match.
	if _, err := avro.Parse(`{"type":"enum","name":"E","symbols":["A"],"default":null}`); err == nil {
		t.Error("a null enum-level default parsed; it names no symbol")
	}
}

// TestRegression_NullBodyRejectIsBounded keeps the reject off the cost path a
// hostile schema could exploit: a null body is four bytes, so the decision
// must not depend on the size of the rest of the schema, and the error must
// not echo it.
func TestRegression_NullBodyRejectIsBounded(t *testing.T) {
	pad := strings.Repeat("a", 1<<20)
	src := `{"type":"fixed","name":"F","size":null,"pad":"` + pad + `"}`
	_, err := avro.Parse(src)
	if err == nil {
		t.Fatal("a null size parsed")
	}
	if len(err.Error()) > 200 {
		t.Errorf("the reject echoed %d bytes of the input", len(err.Error()))
	}
}

// ---------------------------------------------------------------------------
// The matrix.
//
// Every reserved key whose body is decoded into a TYPED destination, crossed
// with the body classes a caller can write, at both levels, read off every
// surface. The rule it drives: for a given key and placement, a NULL body
// reaches the same verdict as a body of the wrong JSON type. Equality with the
// wrong-typed twin is what stops null from being its own body class; the valid
// twin's differing verdict is what keeps the equality from holding vacuously.
//
// "default" is deliberately absent: its body is kept as raw JSON rather than
// decoded into a typed destination, so a null default is a VALUE, not an
// undecodable body.
// ---------------------------------------------------------------------------

// malformedRoute is where a body the placement cannot read ends up. Each cell
// states its route from the rulings, not from the code.
type malformedRoute int

const (
	// routeReject: the kind binds the key and its read is strict, so an
	// unreadable body fails the parse.
	routeReject malformedRoute = iota
	// routeProps: the kind does not bind the key (or binds it only for a
	// body of the right JSON type), so an unreadable body is inert metadata
	// riding to Props verbatim as its only surface.
	routeProps
	// routeDrop: the key is bound on every kind but its capture is a
	// silently-declining read, so an unreadable body reaches NEITHER
	// surface. Exactly one key behaves this way ("doc"), matching Apache
	// Avro, and it is spelled as an outcome here so it stays counted and
	// cannot widen unnoticed.
	routeDrop
)

func (r malformedRoute) String() string {
	switch r {
	case routeReject:
		return "reject"
	case routeProps:
		return "props"
	}
	return "drop"
}

// presenceCell is one reserved key at one placement.
type presenceCell struct {
	key   string
	level string // "type" or "field": which object carries the attribute
	// host spells the whole schema with the attribute clause substituted in.
	host func(attr string) string
	// route is where a body this placement cannot read must end up.
	route malformedRoute
	// validStructural states whether a READABLE body lands on a structural
	// field. It is false for the two keys a non-binding kind surfaces only
	// through Props (precision/scale, which have no stray structural route).
	validStructural bool
	// validRejects marks the exclusivity cells: a container kind carrying
	// another kind's DEFINING key rejects when that key parsed as a real
	// definition, so there the readable body is the one that fails and the
	// unreadable one rides to Props. Stating it keeps the rule visible
	// instead of steering the corpus around it.
	validRejects bool
	valid        string
	wrong        string
	quoted       string // "" when the key's valid body is already a string
	// structural reads the destination a readable body lands on, so
	// "consumed" is measured off the metadata API rather than assumed.
	structural func(avro.SchemaNode) bool
	// props reads the Props map of the object that carried the attribute.
	props func(avro.SchemaNode) map[string]any
}

func (c presenceCell) name() string {
	return fmt.Sprintf("%s/%s/%s", c.level, c.key, c.route)
}

// The three Props surfaces a cell can carry its attribute on. Every host below
// wraps the subject in one record field, so the three are always present.
func pcFieldTypeProps(n avro.SchemaNode) map[string]any { return n.Fields[0].Type.Props }
func pcFieldProps(n avro.SchemaNode) map[string]any     { return n.Fields[0].Props }

// pcTypeHost puts the attribute clause on the field's TYPE object.
func pcTypeHost(typeSrc string) func(string) string {
	return func(attr string) string {
		return fmt.Sprintf(`{"type":"record","name":"PHost","fields":[{"name":"a","type":%s}]}`,
			fmt.Sprintf(typeSrc, attr))
	}
}

// pcFieldHost puts the attribute clause on the record FIELD object.
func pcFieldHost(typeSrc string) func(string) string {
	return func(attr string) string {
		return fmt.Sprintf(`{"type":"record","name":"PHost","fields":[{"name":"a","type":%s%s}]}`,
			typeSrc, attr)
	}
}

func pcType(n avro.SchemaNode) avro.SchemaNode { return n.Fields[0].Type }

func presenceCells() []presenceCell {
	return []presenceCell{
		// ---- the laxInt destination ----
		{
			key: "size", level: "type", host: pcTypeHost(`{"type":"fixed","name":"PF"%s}`),
			route: routeReject, validStructural: true,
			valid: "2", wrong: "[]", quoted: `"2"`,
			structural: func(n avro.SchemaNode) bool { return pcType(n).Size != 0 },
			props:      pcFieldTypeProps,
		},
		{
			key: "size", level: "type", host: pcTypeHost(`{"type":"int"%s}`),
			route: routeProps, validStructural: true,
			valid: "2", wrong: "[]", quoted: `"2"`,
			structural: func(n avro.SchemaNode) bool { return pcType(n).Size != 0 },
			props:      pcFieldTypeProps,
		},
		{
			key: "size", level: "type", host: pcTypeHost(`{"type":"map","values":"int"%s}`),
			route: routeProps, validStructural: true, validRejects: true,
			valid: "2", wrong: "[]", quoted: `"2"`,
			structural: func(n avro.SchemaNode) bool { return pcType(n).Size != 0 },
			props:      pcFieldTypeProps,
		},
		// The flat ("linkedin/goavro") field form writes the defining key
		// beside the field's own keys, so the lift carries the body into the
		// type object it builds — the field-level reach of the same decode.
		{
			key: "size", level: "field", host: pcFieldHost(`"fixed","name":"PFF"`),
			route: routeReject, validStructural: true,
			valid: "2", wrong: "[]", quoted: `"2"`,
			structural: func(n avro.SchemaNode) bool { return pcType(n).Size != 0 },
			props:      pcFieldProps,
		},
		// ---- the *int destinations ----
		{
			key: "scale", level: "type", host: pcTypeHost(`{"type":"bytes","logicalType":"decimal","precision":4%s}`),
			route: routeReject, validStructural: true,
			valid: "2", wrong: "[]", quoted: `"2"`,
			structural: func(n avro.SchemaNode) bool { return pcType(n).Scale != 0 },
			props:      pcFieldTypeProps,
		},
		{
			key: "precision", level: "type", host: pcTypeHost(`{"type":"bytes","logicalType":"decimal","scale":2%s}`),
			route: routeReject, validStructural: true,
			valid: "4", wrong: "[]", quoted: `"4"`,
			structural: func(n avro.SchemaNode) bool { return pcType(n).Precision != 0 },
			props:      pcFieldTypeProps,
		},
		{
			key: "scale", level: "type", host: pcTypeHost(`{"type":"bytes"%s}`),
			route: routeProps, validStructural: false,
			valid: "2", wrong: "[]", quoted: `"2"`,
			structural: func(n avro.SchemaNode) bool { return pcType(n).Scale != 0 },
			props:      pcFieldTypeProps,
		},
		{
			key: "precision", level: "type", host: pcTypeHost(`{"type":"int"%s}`),
			route: routeProps, validStructural: false,
			valid: "4", wrong: "[]", quoted: `"4"`,
			structural: func(n avro.SchemaNode) bool { return pcType(n).Precision != 0 },
			props:      pcFieldTypeProps,
		},
		{
			key: "scale", level: "field", host: pcFieldHost(`"int"`),
			route: routeProps, validStructural: false,
			valid: "2", wrong: "[]", quoted: `"2"`,
			structural: func(n avro.SchemaNode) bool { return pcType(n).Scale != 0 },
			props:      pcFieldProps,
		},
		// ---- the string destinations ----
		{
			key: "name", level: "type", host: pcTypeHost(`{"type":"fixed","size":1%s}`),
			route: routeReject, validStructural: true,
			valid: `"PN"`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return pcType(n).Name != "" },
			props:      pcFieldTypeProps,
		},
		{
			key: "namespace", level: "type", host: pcTypeHost(`{"type":"fixed","name":"PNS","size":1%s}`),
			route: routeReject, validStructural: true,
			valid: `"ns"`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return pcType(n).Namespace != "" },
			props:      pcFieldTypeProps,
		},
		{
			key: "namespace", level: "type", host: pcTypeHost(`{"type":"int"%s}`),
			route: routeProps, validStructural: true,
			valid: `"ns"`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return pcType(n).Namespace != "" },
			props:      pcFieldTypeProps,
		},
		{
			// logicalType binds only for a STRING body: a non-string can
			// name no logical type, so it is an ordinary property.
			key: "logicalType", level: "type", host: pcTypeHost(`{"type":"int"%s}`),
			route: routeProps, validStructural: true,
			valid: `"date"`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return pcType(n).LogicalType != "" },
			props:      pcFieldTypeProps,
		},
		{
			key: "order", level: "field", host: pcFieldHost(`"int"`),
			route: routeReject, validStructural: true,
			valid: `"ignore"`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return n.Fields[0].Order != "" },
			props:      pcFieldProps,
		},
		{
			key: "name", level: "field", host: pcFieldHostNoName(`"int"`),
			route: routeReject, validStructural: true,
			valid: `"a"`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return n.Fields[0].Name != "" },
			props:      pcFieldProps,
		},
		// ---- the []string destinations ----
		{
			key: "aliases", level: "type", host: pcTypeHost(`{"type":"fixed","name":"PA","size":1%s}`),
			route: routeReject, validStructural: true,
			valid: `["x"]`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return len(pcType(n).Aliases) > 0 },
			props:      pcFieldTypeProps,
		},
		{
			key: "aliases", level: "type", host: pcTypeHost(`{"type":"int"%s}`),
			route: routeProps, validStructural: true,
			valid: `["x"]`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return len(pcType(n).Aliases) > 0 },
			props:      pcFieldTypeProps,
		},
		{
			key: "aliases", level: "field", host: pcFieldHost(`"int"`),
			route: routeReject, validStructural: true,
			valid: `["x"]`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return len(n.Fields[0].Aliases) > 0 },
			props:      pcFieldProps,
		},
		{
			key: "symbols", level: "type", host: pcTypeHost(`{"type":"enum","name":"PE"%s}`),
			route: routeReject, validStructural: true,
			valid: `["A"]`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return len(pcType(n).Symbols) > 0 },
			props:      pcFieldTypeProps,
		},
		{
			key: "symbols", level: "type", host: pcTypeHost(`{"type":"int"%s}`),
			route: routeProps, validStructural: true,
			valid: `["A"]`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return len(pcType(n).Symbols) > 0 },
			props:      pcFieldTypeProps,
		},
		// ---- the schema-shaped destinations ----
		{
			key: "items", level: "type", host: pcTypeHost(`{"type":"array"%s}`),
			route: routeReject, validStructural: true,
			valid: `"int"`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return pcType(n).Items != nil },
			props:      pcFieldTypeProps,
		},
		{
			key: "values", level: "type", host: pcTypeHost(`{"type":"map"%s}`),
			route: routeReject, validStructural: true,
			valid: `"int"`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return pcType(n).Values != nil },
			props:      pcFieldTypeProps,
		},
		{
			key: "items", level: "type", host: pcTypeHost(`{"type":"int"%s}`),
			route: routeProps, validStructural: true,
			valid: `"int"`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return pcType(n).Items != nil },
			props:      pcFieldTypeProps,
		},
		{
			key: "fields", level: "type", host: pcTypeHost(`{"type":"record","name":"PR"%s}`),
			route: routeReject, validStructural: true,
			valid: `[{"name":"z","type":"int"}]`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return len(pcType(n).Fields) > 0 },
			props:      pcFieldTypeProps,
		},
		// ---- doc: bound on every kind, with the declining read that keeps
		// an unreadable body off BOTH surfaces ----
		{
			key: "doc", level: "type", host: pcTypeHost(`{"type":"int"%s}`),
			route: routeDrop, validStructural: true,
			valid: `"d"`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return pcType(n).Doc != "" },
			props:      pcFieldTypeProps,
		},
		{
			key: "doc", level: "field", host: pcFieldHost(`"int"`),
			route: routeDrop, validStructural: true,
			valid: `"d"`, wrong: "5",
			structural: func(n avro.SchemaNode) bool { return n.Fields[0].Doc != "" },
			props:      pcFieldProps,
		},
	}
}

// pcFieldHostNoName spells the field WITHOUT its own name key, so a cell can
// write the "name" attribute itself.
func pcFieldHostNoName(typeSrc string) func(string) string {
	return func(attr string) string {
		return fmt.Sprintf(`{"type":"record","name":"PHost","fields":[{"type":%s%s}]}`, typeSrc, attr)
	}
}

// bodyClass is how a matrix cell writes the attribute.
type bodyClass int

const (
	bodyAbsent bodyClass = iota
	bodyValid
	bodyNull
	bodyWrong  // a body of the wrong JSON type, never null
	bodyQuoted // the valid body written as a JSON string
)

func (b bodyClass) String() string {
	switch b {
	case bodyAbsent:
		return "absent"
	case bodyValid:
		return "valid"
	case bodyNull:
		return "null"
	case bodyWrong:
		return "wrong"
	}
	return "quoted"
}

// body renders the attribute clause for one body class, and reports whether
// the class applies to this cell at all.
func (c presenceCell) body(b bodyClass) (string, bool) {
	switch b {
	case bodyAbsent:
		return "", true
	case bodyValid:
		return fmt.Sprintf(`,%q:%s`, c.key, c.valid), true
	case bodyNull:
		return fmt.Sprintf(`,%q:null`, c.key), true
	case bodyWrong:
		return fmt.Sprintf(`,%q:%s`, c.key, c.wrong), true
	case bodyQuoted:
		if c.quoted == "" {
			return "", false
		}
		return fmt.Sprintf(`,%q:%s`, c.key, c.quoted), true
	}
	return "", false
}

// presenceOutcome is what one cell produced, read off every surface. Every
// field is a comparable scalar so two outcomes can be compared directly; the
// Props value is carried as its JSON rendering so an array or object body
// does not make the struct uncomparable.
type presenceOutcome struct {
	rejected   bool
	structural bool
	inProps    bool
	propJSON   string
	canonical  string
	wire       string
}

func (o presenceOutcome) String() string {
	if o.rejected {
		return "REJECT"
	}
	return fmt.Sprintf("structural=%v props=%v(%s) canonical=%s wire=%s",
		o.structural, o.inProps, o.propJSON, o.canonical, o.wire)
}

// presenceRouting is the outcome WITHOUT the attribute's value: which surface
// the body reached, and what the schema compiled to. Two bodies of different
// JSON types must route identically while still riding through as-written, so
// the value is what distinguishes them and the routing is what must match.
type presenceRouting struct {
	rejected   bool
	structural bool
	inProps    bool
	canonical  string
	wire       string
}

func (o presenceOutcome) routing() presenceRouting {
	return presenceRouting{
		rejected:   o.rejected,
		structural: o.structural,
		inProps:    o.inProps,
		canonical:  o.canonical,
		wire:       o.wire,
	}
}

// run parses one cell and reads every surface. The wire column encodes a value
// against the schema when it can, so a routing change that altered the
// compiled codec shows up as a different (or newly failing) encoding rather
// than only as different metadata.
func (c presenceCell) run(t *testing.T, b bodyClass) (presenceOutcome, string) {
	t.Helper()
	attr, ok := c.body(b)
	if !ok {
		return presenceOutcome{}, ""
	}
	src := c.host(attr)
	s, err := avro.Parse(src)
	if err != nil {
		return presenceOutcome{rejected: true}, src
	}
	n := s.Root()
	props := c.props(*n)
	v, in := props[c.key]
	raw, merr := json.Marshal(v)
	if merr != nil {
		raw = []byte(fmt.Sprintf("<unmarshalable %T>", v))
	}
	out := presenceOutcome{
		structural: c.structural(*n),
		inProps:    in,
		propJSON:   string(raw),
		canonical:  string(s.Canonical()),
	}
	if enc, err := s.AppendEncode(nil, map[string]any{n.Fields[0].Name: presenceWireValue(n.Fields[0].Type)}); err != nil {
		out.wire = "err"
	} else {
		out.wire = fmt.Sprintf("%x", enc)
	}
	return out, src
}

// presenceWireValue produces a value the compiled field type accepts, chosen
// off the metadata so it follows whatever the cell's routing produced. A
// decimal is deliberately not fed a value: its error text is the observable.
func presenceWireValue(n avro.SchemaNode) any {
	switch n.Type {
	case "fixed":
		return make([]byte, n.Size)
	case "enum":
		if len(n.Symbols) > 0 {
			return n.Symbols[0]
		}
		return ""
	case "array":
		return []any{}
	case "map":
		return map[string]any{}
	case "record", "error":
		m := make(map[string]any, len(n.Fields))
		for _, f := range n.Fields {
			m[f.Name] = presenceWireValue(f.Type)
		}
		return m
	case "bytes", "string":
		if n.LogicalType != "" {
			return nil
		}
		return []byte{}
	}
	return 0
}

// TestMatrix_ReservedKeyBodyPresence drives the rule over the whole
// cross-product: a null body reaches the same verdict as a wrong-typed body,
// on every surface, for every reserved key with a typed destination.
func TestMatrix_ReservedKeyBodyPresence(t *testing.T) {
	for _, c := range presenceCells() {
		t.Run(c.name(), func(t *testing.T) {
			nullOut, nullSrc := c.run(t, bodyNull)
			wrongOut, wrongSrc := c.run(t, bodyWrong)
			validOut, validSrc := c.run(t, bodyValid)
			absentOut, absentSrc := c.run(t, bodyAbsent)

			// The rule: same ROUTING. The value each body rides through with
			// is necessarily different — that is the as-written contract —
			// so it is asserted separately below and excluded here.
			if nullOut.routing() != wrongOut.routing() {
				t.Errorf("a null body reached a different verdict than a wrong-typed body:\n"+
					"  null  %s\n        %s\n  wrong %s\n        %s",
					nullOut, nullSrc, wrongOut, wrongSrc)
			}
			if !nullOut.rejected && nullOut.inProps && nullOut.propJSON == wrongOut.propJSON {
				t.Errorf("both malformed bodies surfaced the same value %s; each must ride "+
					"through as-written (%s vs %s)", nullOut.propJSON, nullSrc, wrongSrc)
			}

			// Non-vacuity: the readable body must reach a different verdict,
			// or the equality above would hold for an implementation that
			// treated every body alike.
			if validOut.rejected != c.validRejects {
				t.Fatalf("the readable body was %s, want the opposite; this cell tests nothing as written: %s",
					map[bool]string{true: "rejected", false: "accepted"}[validOut.rejected], validSrc)
			}
			if nullOut == validOut {
				t.Errorf("a null body reached the SAME verdict as the valid body %s; "+
					"a body that names no value cannot mean the value: %s", c.valid, nullSrc)
			}
			if !c.validRejects && validOut.structural != c.validStructural {
				t.Errorf("a readable body landed structurally = %v, want %v (%s)",
					validOut.structural, c.validStructural, validSrc)
			}

			// The declared route.
			switch c.route {
			case routeReject:
				if !nullOut.rejected {
					t.Errorf("a bound placement accepted a body it cannot read: %s (%s)", nullOut, nullSrc)
				}
			case routeProps:
				if nullOut.rejected {
					t.Errorf("an unbound placement rejected a body it never reads; Props is its "+
						"only surface (%s)", nullSrc)
				}
				if !nullOut.inProps {
					t.Errorf("an unreadable body reached neither surface: %s (%s)", nullOut, nullSrc)
				}
				if nullOut.propJSON != "null" {
					t.Errorf("Props[%q] = %s, want the as-written null (%s)", c.key, nullOut.propJSON, nullSrc)
				}
				if nullOut.structural {
					t.Errorf("an unreadable body still set a structural field: %s (%s)", nullOut, nullSrc)
				}
			case routeDrop:
				if nullOut.rejected || nullOut.inProps || nullOut.structural {
					t.Errorf("the documented declining read must reach NEITHER surface: %s (%s)",
						nullOut, nullSrc)
				}
			}

			// Absence is its own verdict. Where the placement reads the key,
			// a written null must not be indistinguishable from having
			// written nothing — that confusion is the whole point of the
			// rule. The one exception is the declining read, whose ruling IS
			// that the two coincide, so it is stated rather than silent.
			if c.route != routeDrop && !absentOut.rejected && absentOut == nullOut {
				t.Errorf("a written null is indistinguishable from an absent attribute: %s (%s vs %s)",
					nullOut, nullSrc, absentSrc)
			}
			if c.route == routeDrop && !absentOut.rejected && absentOut != nullOut {
				t.Errorf("the declining read is documented to leave no trace, so it must match the "+
					"absent verdict:\n  null   %s\n  absent %s", nullOut, absentOut)
			}

			// Quoting is a spelling of a value, so wherever it applies the
			// verdict must be the key's own policy and never the null one.
			if quotedOut, quotedSrc := c.run(t, bodyQuoted); quotedSrc != "" {
				if !quotedOut.rejected && quotedOut == nullOut {
					t.Errorf("a quoted body collapsed into the null verdict: %s (%s)", quotedOut, quotedSrc)
				}
			}
		})
	}
}

// TestMatrix_ReservedKeyBodyPresenceIsNotVacuous fails when the corpus stops
// spanning the axes the rule distinguishes, so an implementation that ignored
// one of them could not pass by never being asked.
func TestMatrix_ReservedKeyBodyPresenceIsNotVacuous(t *testing.T) {
	routes := map[malformedRoute]int{}
	levels := map[string]int{}
	dests := map[string]int{}
	keys := map[string]int{}
	quoted := 0
	for _, c := range presenceCells() {
		routes[c.route]++
		levels[c.level]++
		keys[c.key]++
		if c.quoted != "" {
			quoted++
		}
		switch c.key {
		case "size", "precision", "scale":
			dests["int"]++
		case "name", "namespace", "doc", "logicalType", "order":
			dests["string"]++
		case "aliases", "symbols":
			dests["[]string"]++
		case "items", "values", "fields":
			dests["schema"]++
		}
	}
	if routes[routeReject] < 8 || routes[routeProps] < 6 {
		t.Fatalf("the routing split is not exercised: reject=%d props=%d", routes[routeReject], routes[routeProps])
	}
	// The declining read is documented as exactly one key. More cells than
	// that would mean the exception had quietly become a second rule.
	dropKeys := map[string]bool{}
	for _, c := range presenceCells() {
		if c.route == routeDrop {
			dropKeys[c.key] = true
		}
	}
	if len(dropKeys) != 1 || !dropKeys["doc"] {
		t.Fatalf("the declining read is meant to be exactly one key (%q); got %v", "doc", dropKeys)
	}
	if routes[routeDrop] < 2 {
		t.Fatalf("the declining read must be exercised at both levels, got %d cells", routes[routeDrop])
	}
	if levels["type"] < 15 || levels["field"] < 5 {
		t.Fatalf("the level axis is not exercised: type=%d field=%d", levels["type"], levels["field"])
	}
	if quoted < 6 {
		t.Fatalf("the quoted-body class is barely exercised: %d cells", quoted)
	}
	// Every typed destination the parser decodes into must appear: the
	// hazard belongs to the DESTINATION's decode, not to the key.
	for _, d := range []string{"int", "string", "[]string", "schema"} {
		if dests[d] == 0 {
			t.Fatalf("no cell decodes into a %s destination; the hazard is per-destination", d)
		}
	}
	if len(keys) < 10 {
		t.Fatalf("the corpus covers only %d reserved keys", len(keys))
	}
}

// TestDifferentialFastavroNullBodyIsNotUsable records the reference
// calibration this posture rests on: fastavro ACCEPTS a null-bodied size or
// decimal scale at parse and then fails every write against the result, so its
// acceptance is not an acceptance the permissive lean can follow.
func TestDifferentialFastavroNullBodyIsNotUsable(t *testing.T) {
	o := startOracle(t)
	cases := []struct {
		name string
		// nullSchema carries the null body; twinSchema is the same schema
		// with a written value in its place. The twin runs the IDENTICAL
		// value plumbing, so a write that fails for both would be a broken
		// probe rather than evidence about the null.
		nullSchema string
		twinSchema string
		value      json.RawMessage
		kind       string
	}{
		{
			name:       "fixed-null-size",
			nullSchema: `{"type":"fixed","name":"FNull","size":null}`,
			twinSchema: `{"type":"fixed","name":"FNull","size":0}`,
			value:      json.RawMessage(`""`), // base64 of zero bytes
			kind:       "fixed",
		},
		{
			name:       "decimal-null-scale",
			nullSchema: `{"type":"bytes","logicalType":"decimal","precision":4,"scale":null}`,
			twinSchema: `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
			value:      json.RawMessage(`"1.23"`),
			kind:       "decimal",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			twin := o.call(oracleJob{Op: "encode", Schema: json.RawMessage(c.twinSchema),
				Value: c.value, Kind: c.kind})
			if !twin.OK {
				t.Fatalf("fastavro could not write the written-value twin either (%s); the probe "+
					"proves nothing about the null body", twin.Err)
			}

			parsed := o.call(oracleJob{Op: "parsedump", Schema: json.RawMessage(c.nullSchema)})
			if !parsed.OK {
				t.Logf("fastavro now rejects %s at parse (%s), which agrees with this package "+
					"more closely than the recorded calibration", c.name, parsed.Err)
			} else if enc := o.call(oracleJob{Op: "encode", Schema: json.RawMessage(c.nullSchema),
				Value: c.value, Kind: c.kind}); enc.OK {
				t.Errorf("fastavro now WRITES against a null-bodied attribute (hex %q); its accept "+
					"has become a usable accept, which is new evidence for the permissive lean "+
					"this package declined", enc.Hex)
			}

			// twmb rejects at parse, which is the difference that matters:
			// the caller learns before it holds a schema, not at the first
			// write.
			if _, err := avro.Parse(c.nullSchema); err == nil {
				t.Errorf("twmb accepted %s at parse", c.name)
			}
			if _, err := avro.Parse(c.twinSchema); err != nil {
				t.Errorf("twmb rejected the written-value twin: %v", err)
			}
		})
	}
}

// ---------- null_branch_spelling_test.go ----------

// Avro spells a union's null branch two ways — the bare `"null"` primitive and
// the wrapped object `{"type":"null"}` — and the two are the same type:
// identical wire bytes, identical decoded values. Every decision made by
// matching a branch's WRITTEN spelling must therefore reach the same verdict for
// both, or semantically identical schemas take different code paths in the
// places that spelling feeds: which ser/deser arm is selected, the field
// metadata driving the omitzero and missing-key fills, where a field-level
// logicalType lifts, whether a field-level precision/scale pair is consumed, and
// the encode-error identity each wire surfaces. Wire bytes are identical BY
// CONSTRUCTION, so these assert the DERIVED artifacts.

// wrapNullBranches rewrites every bare `"null"` union branch in a schema into
// the wrapped `{"type":"null"}` object, structurally (decode → rewrite →
// re-encode) rather than textually, so a `"null"` appearing anywhere that is
// not a union branch is left alone. extra, when non-nil, is merged into each
// wrapped null object so the props / logicalType carriers can be driven
// through the same helper.
func wrapNullBranches(t *testing.T, schema string, extra map[string]any) string {
	t.Helper()
	var tree any
	if err := json.Unmarshal([]byte(schema), &tree); err != nil {
		t.Fatalf("wrapNullBranches: unmarshal %s: %v", schema, err)
	}
	var walk func(v any) any
	walk = func(v any) any {
		switch tv := v.(type) {
		case []any: // a union: rewrite bare null branches
			out := make([]any, len(tv))
			for i, br := range tv {
				if s, ok := br.(string); ok && s == "null" {
					m := map[string]any{"type": "null"}
					for k, val := range extra {
						m[k] = val
					}
					out[i] = m
					continue
				}
				out[i] = walk(br)
			}
			return out
		case map[string]any:
			out := make(map[string]any, len(tv))
			for k, val := range tv {
				out[k] = walk(val)
			}
			return out
		}
		return v
	}
	b, err := json.Marshal(walk(tree))
	if err != nil {
		t.Fatalf("wrapNullBranches: marshal: %v", err)
	}
	return string(b)
}

// nullSpellings are the spellings of a union's null branch that must all be
// treated as a null branch. Props and a logicalType are inert metadata on a
// null (there is no null logical type in the spec, and neither key changes
// the branch's type or its wire form), so a carrier-bearing wrapped null is
// still a null branch.
func nullSpellings() []struct {
	label string
	extra map[string]any
} {
	return []struct {
		label string
		extra map[string]any
	}{
		{"bare", nil}, // extra==nil AND handled by the caller as "leave as written"
		{"wrapped", map[string]any{}},
		{"wrapped+props", map[string]any{"mine": "keepme"}},
		{"wrapped+logicalType", map[string]any{"logicalType": "nope"}},
		{"wrapped+nonstring-logicalType", map[string]any{"logicalType": float64(123)}},
	}
}

// spell returns schema with its null branches in the named spelling.
func spell(t *testing.T, schema, label string, extra map[string]any) string {
	t.Helper()
	if label == "bare" {
		return schema
	}
	return wrapNullBranches(t, schema, extra)
}

type omitRec struct {
	A string `avro:"a,omitzero"`
}

// TestMatrix_OmitzeroNullBranchSpellingAgnostic pins the wire bytes and
// the decoded nullness, not merely that the encode succeeded: under the
// wrapped spelling the field previously encoded the VALUE branch (an empty
// string), which is indistinguishable on the wire from an explicit "".
//
// doc.go, "Struct tags": "a zero value encodes the field's default, or null
// for a nullable field that has no default"; and the documented single
// difference from map fill is precisely this [T, "null"] shape, where
// "omitzero encodes null where map fill instead errors on the missing key".
func TestMatrix_OmitzeroNullBranchSpellingAgnostic(t *testing.T) {
	for _, branches := range []string{`["string","null"]`, `["null","string"]`} {
		base := fmt.Sprintf(`{"type":"record","name":"R","fields":[{"name":"a","type":%s}]}`, branches)
		var wantWire []byte
		for _, sp := range nullSpellings() {
			schema := spell(t, base, sp.label, sp.extra)
			s, err := avro.Parse(schema)
			if err != nil {
				t.Fatalf("%s/%s: parse %s: %v", branches, sp.label, schema, err)
			}
			wire, err := s.Encode(omitRec{})
			if err != nil {
				t.Fatalf("%s/%s: omitzero encode: %v (schema %s)", branches, sp.label, err, schema)
			}
			var back map[string]any
			if _, err := s.Decode(wire, &back); err != nil {
				t.Fatalf("%s/%s: decode: %v", branches, sp.label, err)
			}
			if back["a"] != nil {
				t.Errorf("%s/%s: omitzero on a zero-valued nullable field encoded %#v (wire %v); a zero value with no default must encode NULL",
					branches, sp.label, back["a"], wire)
			}
			// The null branch must stay distinguishable from an explicit
			// empty string, or nullness is unrecoverable by the reader.
			explicit, err := s.Encode(map[string]any{"a": ""})
			if err != nil {
				t.Fatalf("%s/%s: explicit empty-string encode: %v", branches, sp.label, err)
			}
			if string(wire) == string(explicit) {
				t.Errorf("%s/%s: omitzero wire %v equals the explicit empty-string wire; the null branch is unreachable via omitzero",
					branches, sp.label, wire)
			}
			if sp.label == "bare" {
				wantWire = wire
				continue
			}
			if string(wire) != string(wantWire) {
				t.Errorf("%s/%s: omitzero wire %v != bare spelling's %v; the two schemas are the same Avro type",
					branches, sp.label, wire, wantWire)
			}
		}
	}
}

// TestMatrix_FieldLogicalLiftNullBranchSpellingAgnostic: the field-level
// logicalType lifts onto the first NON-null branch. A wrapped null branch is
// still a null branch, so it must be skipped exactly like the bare one —
// otherwise the annotation lands on null and the intended branch never gets
// it, so a time.Time no longer encodes at all.
func TestMatrix_FieldLogicalLiftNullBranchSpellingAgnostic(t *testing.T) {
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, branches := range []string{`["null","long"]`, `["long","null"]`} {
		base := fmt.Sprintf(
			`{"type":"record","name":"R","fields":[{"name":"ts","type":%s,"logicalType":"timestamp-millis"}]}`, branches)
		var wantWire []byte
		for _, sp := range nullSpellings() {
			schema := spell(t, base, sp.label, sp.extra)
			s, err := avro.Parse(schema)
			if err != nil {
				t.Fatalf("%s/%s: parse: %v", branches, sp.label, err)
			}
			wire, err := s.Encode(map[string]any{"ts": ts})
			if err != nil {
				t.Fatalf("%s/%s: encoding time.Time into the lifted field failed: %v (schema %s)",
					branches, sp.label, err, schema)
			}
			var back map[string]any
			if _, err := s.Decode(wire, &back); err != nil {
				t.Fatalf("%s/%s: decode: %v", branches, sp.label, err)
			}
			got, ok := back["ts"].(time.Time)
			if !ok {
				t.Fatalf("%s/%s: decoded %T (%v), want time.Time — the lift did not reach the non-null branch",
					branches, sp.label, back["ts"], back["ts"])
			}
			if !got.Equal(ts) {
				t.Errorf("%s/%s: round-tripped %v, want %v", branches, sp.label, got, ts)
			}
			if sp.label == "bare" {
				wantWire = wire
				continue
			}
			if string(wire) != string(wantWire) {
				t.Errorf("%s/%s: wire %v != bare spelling's %v", branches, sp.label, wire, wantWire)
			}
		}
	}
}

// TestMatrix_FieldDecimalLiftNullBranchSpellingAgnostic is the
// precision/scale twin: whether the field-level pair is CONSUMED by the
// decimal lift is decided by the same first-non-null-branch scan.
func TestMatrix_FieldDecimalLiftNullBranchSpellingAgnostic(t *testing.T) {
	rat := big.NewRat(12345, 100)
	for _, branches := range []string{`["null","bytes"]`, `["bytes","null"]`} {
		base := fmt.Sprintf(
			`{"type":"record","name":"R","fields":[{"name":"d","type":%s,"logicalType":"decimal","precision":10,"scale":2}]}`,
			branches)
		var wantWire []byte
		for _, sp := range nullSpellings() {
			schema := spell(t, base, sp.label, sp.extra)
			s, err := avro.Parse(schema)
			if err != nil {
				t.Fatalf("%s/%s: parse: %v", branches, sp.label, err)
			}
			wire, err := s.Encode(map[string]any{"d": rat})
			if err != nil {
				t.Fatalf("%s/%s: encoding *big.Rat into the lifted decimal field failed: %v (schema %s)",
					branches, sp.label, err, schema)
			}
			if sp.label == "bare" {
				wantWire = wire
				continue
			}
			if string(wire) != string(wantWire) {
				t.Errorf("%s/%s: wire %v != bare spelling's %v", branches, sp.label, wire, wantWire)
			}
		}
	}
}

type noBranchValue struct{ X int }

func encodeErrIdentity(err error) string {
	if err == nil {
		return "nil"
	}
	var se *avro.SemanticError
	if errors.As(err, &se) {
		return fmt.Sprintf("SemanticError{AvroType:%q}", se.AvroType)
	}
	return "plain"
}

// TestMatrix_UnionNoMatchIdentityNullBranchSpellingAgnostic: the union
// no-match error identity is arity-split — a 2-branch null union surfaces the
// value branch's own error, every other shape wraps in the union's
// *SemanticError. The binary and JSON encoders must reach the same verdict,
// and the split must not depend on how the null branch is spelled.
func TestMatrix_UnionNoMatchIdentityNullBranchSpellingAgnostic(t *testing.T) {
	bases := []string{
		`["null","string"]`,
		`["string","null"]`,
		`["null","string","int"]`, // 3-branch: wraps in the union's error
	}
	for _, base := range bases {
		var wantID string
		for _, sp := range nullSpellings() {
			schema := spell(t, base, sp.label, sp.extra)
			s, err := avro.Parse(schema)
			if err != nil {
				t.Fatalf("%s/%s: parse: %v", base, sp.label, err)
			}
			v := noBranchValue{X: 1} // matches no branch on either wire
			_, binErr := s.Encode(v)
			_, jsonErr := s.EncodeJSON(v)
			binID, jsonID := encodeErrIdentity(binErr), encodeErrIdentity(jsonErr)
			if binID == "nil" || jsonID == "nil" {
				t.Fatalf("%s/%s: expected a no-match error on both wires, got binary=%v json=%v",
					base, sp.label, binErr, jsonErr)
			}
			if binID != jsonID {
				t.Errorf("%s/%s: encode-error identity differs by WIRE:\n  binary: %s (%v)\n  json:   %s (%v)",
					base, sp.label, binID, binErr, jsonID, jsonErr)
			}
			if sp.label == "bare" {
				wantID = binID
				continue
			}
			if binID != wantID {
				t.Errorf("%s/%s: encode-error identity %s differs from the bare spelling's %s",
					base, sp.label, binID, wantID)
			}
		}
	}
}

// TestMatrix_MissingKeyFillNullBranchSpellingAgnostic: the implicit null
// default for the canonical ["null", T] nullable pattern, and the loud
// missing-key error for [T, "null"], must both be spelling-agnostic.
func TestMatrix_MissingKeyFillNullBranchSpellingAgnostic(t *testing.T) {
	for _, branches := range []string{`["null","string"]`, `["string","null"]`} {
		base := fmt.Sprintf(`{"type":"record","name":"R","fields":[{"name":"a","type":%s}]}`, branches)
		var wantErr bool
		var wantWire []byte
		for _, sp := range nullSpellings() {
			s, err := avro.Parse(spell(t, base, sp.label, sp.extra))
			if err != nil {
				t.Fatalf("%s/%s: parse: %v", branches, sp.label, err)
			}
			wire, encErr := s.Encode(map[string]any{})
			if sp.label == "bare" {
				wantErr, wantWire = encErr != nil, wire
				continue
			}
			if (encErr != nil) != wantErr {
				t.Errorf("%s/%s: missing-key encode err=%v but the bare spelling's err-ness was %v",
					branches, sp.label, encErr, wantErr)
				continue
			}
			if encErr == nil && string(wire) != string(wantWire) {
				t.Errorf("%s/%s: missing-key fill wire %v != bare spelling's %v",
					branches, sp.label, wire, wantWire)
			}
		}
	}
}

// respellNulls is wrapNullBranches plus a "did anything change" verdict,
// derived by re-normalizing the input through the same marshal round trip so
// key ordering cannot masquerade as a change.
func respellNulls(t *testing.T, schema string, extra map[string]any) (string, bool) {
	t.Helper()
	normalized := wrapNullBranches(t, schema, nil /* rewrite nothing */)
	respelled := wrapNullBranches(t, schema, extra)
	return respelled, respelled != normalized
}

// TestMatrix_NullBranchSpellingParity is the class net for the bare-vs-wrapped
// null spelling axis: it crosses that axis into the existing combinatorial
// tables rather than forking them, and asserts on every union-bearing cell that
// the spellings agree on every DERIVED artifact — the selected ser/deser arm,
// the field metadata, the JSON form, the canonical form and fingerprint, and the
// encode-error identity. Wire bytes are identical by construction, which is
// exactly why the derived artifacts are what a spelling-sensitive predicate
// corrupts. Non-vacuity: reverting isNullBranch to `s.primitive == "null"`
// reddens this test.
func TestMatrix_NullBranchSpellingParity(t *testing.T) {
	u := &uniq{}
	var cells, checks int
	for _, fr := range matFrags() {
		for _, cx := range matCtxs() {
			if cx.skip != nil && cx.skip(fr.kind) {
				continue
			}
			bare := cx.schema(fr.schema(u), fr.kind, u)
			baseSchema, err := avro.Parse(bare)
			if err != nil {
				continue // contexts calibrate their own acceptance elsewhere
			}
			// Reference artifacts from the bare spelling.
			baseCanon := string(baseSchema.Canonical())
			baseFP := baseSchema.Fingerprint(avro.NewRabin())

			for _, sp := range nullSpellings() {
				if sp.label == "bare" {
					continue
				}
				respelled, changed := respellNulls(t, bare, sp.extra)
				if !changed {
					continue // no null branch in this cell
				}
				alt, err := avro.Parse(respelled)
				if err != nil {
					t.Errorf("%s/%s/%s: bare spelling parses but the wrapped one does not: %v\n  bare:    %s\n  wrapped: %s",
						fr.label, cx.label, sp.label, err, bare, respelled)
					continue
				}
				cells++

				// Canonical form and fingerprint collapse the wrapped form,
				// so both must be spelling-independent.
				if got := string(alt.Canonical()); got != baseCanon {
					t.Errorf("%s/%s/%s: canonical %s != bare's %s", fr.label, cx.label, sp.label, got, baseCanon)
				}
				if got := alt.Fingerprint(avro.NewRabin()); string(got) != string(baseFP) {
					t.Errorf("%s/%s/%s: fingerprint differs from the bare spelling", fr.label, cx.label, sp.label)
				}

				for _, val := range fr.values {
					wv := cx.wrap(val)
					baseWire, baseErr := baseSchema.Encode(wv)
					altWire, altErr := alt.Encode(wv)
					if (baseErr == nil) != (altErr == nil) {
						t.Errorf("%s/%s/%s: binary encode verdict differs: bare=%v wrapped=%v",
							fr.label, cx.label, sp.label, baseErr, altErr)
						continue
					}
					if baseErr != nil {
						if a, b := encodeErrIdentity(baseErr), encodeErrIdentity(altErr); a != b {
							t.Errorf("%s/%s/%s: binary encode-error identity %s != bare's %s",
								fr.label, cx.label, sp.label, b, a)
						}
						continue
					}
					if string(altWire) != string(baseWire) {
						t.Errorf("%s/%s/%s value %#v: wire %v != bare's %v",
							fr.label, cx.label, sp.label, wv, altWire, baseWire)
					}
					// Decoded value parity through the wrapped schema.
					var baseGot, altGot any
					if _, err := baseSchema.Decode(baseWire, &baseGot); err != nil {
						t.Fatalf("%s/%s: bare decode: %v", fr.label, cx.label, err)
					}
					if _, err := alt.Decode(altWire, &altGot); err != nil {
						t.Errorf("%s/%s/%s: wrapped decode: %v", fr.label, cx.label, sp.label, err)
						continue
					}
					if !matEqual(baseGot, altGot) {
						t.Errorf("%s/%s/%s value %#v: decoded %#v != bare's %#v",
							fr.label, cx.label, sp.label, wv, altGot, baseGot)
					}
					// JSON form parity.
					baseJSON, baseJErr := baseSchema.EncodeJSON(wv)
					altJSON, altJErr := alt.EncodeJSON(wv)
					if (baseJErr == nil) != (altJErr == nil) {
						t.Errorf("%s/%s/%s: JSON encode verdict differs: bare=%v wrapped=%v",
							fr.label, cx.label, sp.label, baseJErr, altJErr)
					} else if baseJErr == nil && string(altJSON) != string(baseJSON) {
						t.Errorf("%s/%s/%s value %#v: JSON %s != bare's %s",
							fr.label, cx.label, sp.label, wv, altJSON, baseJSON)
					}
					checks++
				}

				// A value no branch accepts: the encode-error identity must
				// agree across wires AND across spellings.
				bBin := encodeErrIdentity(mustErr(baseSchema.Encode(cx.wrap(noBranchValue{X: 1}))))
				aBin := encodeErrIdentity(mustErr(alt.Encode(cx.wrap(noBranchValue{X: 1}))))
				bJSON := encodeErrIdentity(mustErr(baseSchema.EncodeJSON(cx.wrap(noBranchValue{X: 1}))))
				aJSON := encodeErrIdentity(mustErr(alt.EncodeJSON(cx.wrap(noBranchValue{X: 1}))))
				if aBin != bBin {
					t.Errorf("%s/%s/%s: binary no-match identity %s != bare's %s", fr.label, cx.label, sp.label, aBin, bBin)
				}
				if aJSON != bJSON {
					t.Errorf("%s/%s/%s: JSON no-match identity %s != bare's %s", fr.label, cx.label, sp.label, aJSON, bJSON)
				}
				if aBin != aJSON {
					t.Errorf("%s/%s/%s: no-match identity differs by WIRE: binary=%s json=%s", fr.label, cx.label, sp.label, aBin, aJSON)
				}
			}
		}
	}
	if cells == 0 || checks == 0 {
		t.Fatalf("vacuous net: %d respelled cells, %d value checks — the spelling axis must actually fire", cells, checks)
	}
	t.Logf("null-branch spelling parity: %d respelled cells, %d value checks", cells, checks)
}

// mustErr discards an encode's byte result, keeping only the error, so the
// identity comparisons above read cleanly.
func mustErr(_ []byte, err error) error { return err }

// ---------- namespace_validation_test.go ----------

// The namespace attribute is a dot-separated sequence of names and must satisfy
// the same grammar as a name. Strict mode validated only the (possibly bare)
// name attribute, so a namespace spelled via the attribute skipped validation
// entirely while the identical fullname spelled inline was rejected — an
// incoherence made worse because the parsing canonical form inlines the
// namespace into the fullname, so the accepted schema's Canonical() could not
// re-parse in the same mode.
func TestRegression_NamespaceAttributeValidatedLikeFullname(t *testing.T) {
	// Both spellings of the same illegal fullname must be rejected uniformly.
	if _, err := avro.Parse(`{"type":"record","name":"bad ns.R","fields":[]}`); err == nil {
		t.Fatal("control: dotted illegal fullname must be rejected")
	}
	if _, err := avro.Parse(`{"type":"record","name":"R","namespace":"bad ns","fields":[]}`); err == nil {
		t.Fatal("namespace attribute with an illegal component must be rejected (was silently accepted)")
	}

	// A dotted namespace with one illegal component is rejected too.
	if _, err := avro.Parse(`{"type":"record","name":"R","namespace":"a.b c.d","fields":[]}`); err == nil {
		t.Fatal("dotted namespace with an illegal component must be rejected")
	}

	// A valid namespace still parses AND its Canonical() re-parses in strict
	// mode (the coherence the bug broke).
	s, err := avro.Parse(`{"type":"record","name":"R","namespace":"a.b","fields":[{"name":"f","type":"int"}]}`)
	if err != nil {
		t.Fatalf("valid namespace must parse: %v", err)
	}
	if !json.Valid(s.Canonical()) {
		t.Fatalf("Canonical() is not valid JSON: %s", s.Canonical())
	}
	if _, err := avro.Parse(string(s.Canonical())); err != nil {
		t.Fatalf("Canonical() of an accepted schema must re-parse in strict mode: %v\ncanonical: %s", err, s.Canonical())
	}

	// The explicit empty namespace (null-namespace escape) is exempt.
	if _, err := avro.Parse(`{"type":"record","name":"R","namespace":"","fields":[]}`); err != nil {
		t.Fatalf("explicit empty namespace must remain accepted: %v", err)
	}
}

// WithLaxNames documents that its validator fn is "called for each name
// component". Namespace attribute components route through it now too — without
// the validation site they were never offered to the fn.
func TestRegression_LaxNamesValidatorSeesNamespaceComponents(t *testing.T) {
	var seen []string
	fn := func(s string) error {
		seen = append(seen, s)
		return nil
	}
	_, err := avro.Parse(`{"type":"record","name":"Rec","namespace":"ns1.ns2","fields":[{"name":"fld","type":"int"}]}`, avro.WithLaxNames(fn))
	if err != nil {
		t.Fatalf("parse with lax fn: %v", err)
	}
	for _, comp := range []string{"ns1", "ns2"} {
		if !slices.Contains(seen, comp) {
			t.Errorf("WithLaxNames validator never saw namespace component %q (saw %v)", comp, seen)
		}
	}
	// A permissive fn must let an otherwise-illegal namespace through (the fn
	// is the sole authority under WithLaxNames).
	if _, err := avro.Parse(`{"type":"record","name":"R","namespace":"weird-ns","fields":[]}`, avro.WithLaxNames(func(string) error { return nil })); err != nil {
		t.Fatalf("permissive WithLaxNames must accept a non-standard namespace: %v", err)
	}
}

// SemanticError.Field is built from parsed (registry/remote-controlled) schema
// field names and is length-unbounded. Error() must render-truncate it so a
// hostile field name cannot amplify into an equally large error string on every
// type-mismatched datum — while the public Field keeps its full value.
func TestRegression_SemanticErrorFieldRenderBounded(t *testing.T) {
	bigName := "F" + strings.Repeat("A", 1<<20)
	schema := `{"type":"record","name":"R","fields":[{"name":"` + bigName + `","type":"int"}]}`
	s := mustParse(t, schema)
	_, err := s.Encode(map[string]any{bigName: "not-an-int"})
	if err == nil {
		t.Fatal("expected a type-mismatch error")
	}
	if got := len(err.Error()); got > 4096 {
		t.Errorf("error message is %d bytes — the schema-controlled field name is echoed unbounded", got)
	}

	// The public Field still carries the full value for callers that inspect it.
	var se *avro.SemanticError
	if !errors.As(err, &se) {
		t.Fatalf("error is not a *SemanticError: %v", err)
	}
	if se.Field != bigName {
		t.Errorf("public SemanticError.Field was truncated (len=%d); render-truncation must not mutate the struct field", len(se.Field))
	}

	// A short field name still renders informatively.
	s2 := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"age","type":"int"}]}`)
	_, err = s2.Encode(map[string]any{"age": "not-an-int"})
	if err == nil || !strings.Contains(err.Error(), "age") {
		t.Fatalf("short field name must still appear in the error: %v", err)
	}
}

// ---------- empty_name_component_test.go ----------

// ---------------------------------------------------------------------------
// The empty string is a legal NAME COMPONENT.
//
// [avro.WithLaxNames] takes a caller-supplied validator called per name
// component; one that accepts "" makes the empty string a legal record name,
// field name, namespace component, enum symbol, or alias. The package relies on
// that being carriable: its own internal re-parses of library-produced schema
// text use an accept-everything validator precisely so a name the caller's
// validator accepted cannot be rejected later.
//
// That makes "" a value like any other, and it forbids a common shortcut: using
// a name string's ZERO VALUE as an absence sentinel. A guard written as
// `claimedName[i] != ""` cannot tell "no one claimed slot i" from "the field
// named "" claimed slot i", so it silently skips its own check. Presence and
// identity have to be separate variables.
//
// The nets put "" in every name-shaped position and hold the package to
// invariants needing no reference implementation:
//
//   - CheckCompatibility and Resolve must AGREE. They are two independent
//     implementations of the same rules, so they are each other's oracle. Resolve
//     calls CheckCompatibility first, so the only reachable disagreement is
//     "CheckCompatibility accepts, Resolve rejects" — a caller using it as an
//     admission gate is told a pair is fine and then fails at Resolve.
//   - A schema's own String() must re-parse, and Root().Schema() must rebuild,
//     to the same canonical form and fingerprint.
//
// Every schema here needs the lax validator to parse at all, which is exactly
// the axis the ordinary compatibility corpora hold constant.
// ---------------------------------------------------------------------------

var laxAny = avro.WithLaxNames(func(string) error { return nil })

func fingerprintOf(t *testing.T, s *avro.Schema) []byte {
	t.Helper()
	return s.Fingerprint(crc64.New(crc64.MakeTable(crc64.ECMA)))
}

// emptyNamePositions places "" in one name-shaped position each.
func emptyNamePositions() []struct{ name, schema string } {
	return []struct{ name, schema string }{
		{"record name", `{"type":"record","name":"","fields":[{"name":"f","type":"int"}]}`},
		{"field name", `{"type":"record","name":"R","fields":[{"name":"","type":"int"}]}`},
		{"enum name", `{"type":"enum","name":"","symbols":["A"]}`},
		{"fixed name", `{"type":"fixed","name":"","size":4}`},
		{"namespace component", `{"type":"record","name":"R","namespace":"a..b","fields":[{"name":"f","type":"int"}]}`},
		{"trailing dot fullname", `{"type":"record","name":"a.","fields":[{"name":"f","type":"int"}]}`},
		{"nested empty-named record", `{"type":"record","name":"Outer","fields":[{"name":"f","type":{"type":"record","name":"","fields":[{"name":"g","type":"int"}]}}]}`},
		{"enum symbol", `{"type":"enum","name":"E","symbols":[""]}`},
		{"field alias", `{"type":"record","name":"R","fields":[{"name":"f","type":"int","aliases":[""]}]}`},
		{"type alias", `{"type":"record","name":"R","aliases":[""],"fields":[{"name":"f","type":"int"}]}`},
		{"empty name and empty namespace", `{"type":"record","name":"","namespace":"","fields":[{"name":"f","type":"int"}]}`},
		{"empty-named record inside a union", `["null",{"type":"record","name":"","fields":[{"name":"f","type":"int"}]}]`},
		{"empty-named field inside a union branch", `["null",{"type":"record","name":"R","fields":[{"name":"","type":"int"}]}]`},
	}
}

// TestMatrix_EmptyNameComponentSelfConsistency holds each position to the two
// self-consistency invariants: the emitted text and the rebuilt metadata tree
// are the schema's own claims about itself, so both must reproduce it.
func TestMatrix_EmptyNameComponentSelfConsistency(t *testing.T) {
	for _, p := range emptyNamePositions() {
		t.Run(p.name, func(t *testing.T) {
			s, err := avro.Parse(p.schema, laxAny)
			if err != nil {
				t.Fatalf("a permissive validator must accept this name: %v", err)
			}
			text, canon, finger := s.String(), s.Canonical(), fingerprintOf(t, s)

			re, err := avro.Parse(text, laxAny)
			if err != nil {
				t.Fatalf("String() emitted text that does not re-parse: %v\n  emitted %s", err, text)
			}
			if !bytes.Equal(re.Canonical(), canon) {
				t.Errorf("canonical form drifts across the String() round trip:\n  before %s\n  after  %s", canon, re.Canonical())
			}
			if !bytes.Equal(fingerprintOf(t, re), finger) {
				t.Errorf("fingerprint drifts across the String() round trip (emitted %s)", text)
			}

			// SchemaNode.Schema documents that the options the original parse
			// used must be supplied again, so the validator is passed here too.
			root := s.Root()
			rb, err := root.Schema(laxAny)
			if err != nil {
				t.Fatalf("Root().Schema() cannot rebuild the tree it produced: %v", err)
			}
			if !bytes.Equal(rb.Canonical(), canon) {
				t.Errorf("canonical form drifts across the metadata rebuild:\n  before %s\n  after  %s", canon, rb.Canonical())
			}
			if !bytes.Equal(fingerprintOf(t, rb), finger) {
				t.Errorf("fingerprint drifts across the metadata rebuild")
			}
		})
	}
}

// TestMatrix_EmptyNameComponentCompatResolveAgree crosses every position with
// the evolution shapes, asserting the cross-path agreement. The corpora that
// already assert this agreement all parse with the default validator, so no
// cell of theirs can carry an empty name component.
func TestMatrix_EmptyNameComponentCompatResolveAgree(t *testing.T) {
	var cells int
	for _, p := range emptyNamePositions() {
		s, err := avro.Parse(p.schema, laxAny)
		if err != nil {
			t.Fatalf("%s: %v", p.name, err)
		}
		for _, pair := range []struct {
			name           string
			writer, reader *avro.Schema
		}{
			{"identity", s, s},
		} {
			cells++
			compatErr := avro.CheckCompatibility(pair.writer, pair.reader)
			_, resolveErr := avro.Resolve(pair.writer, pair.reader)
			if (compatErr == nil) != (resolveErr == nil) {
				t.Errorf("%s/%s: CheckCompatibility and Resolve disagree\n  CheckCompatibility: %v\n  Resolve:            %v",
					p.name, pair.name, compatErr, resolveErr)
			}
			if compatErr != nil {
				t.Errorf("%s/%s: a schema is not compatible with itself: %v", p.name, pair.name, compatErr)
			}
		}
	}
	t.Logf("cells: %d", cells)
}

// TestMatrix_EmptyFieldNameAliasClaimAgreement is the shape that a name-valued
// absence sentinel hides. Two writer fields resolve to ONE reader slot — one
// by the reader field's name, one by its alias — which both the resolver and
// the compatibility check must refuse. Crossing the reader field's name with
// the empty string, and crossing the DECLARATION ORDER of the two writer
// fields, is what separates a real presence flag from a sentinel: with the
// sentinel, the collision is detected only when the ""-named writer field
// comes second, because by then some other name has overwritten the slot.
func TestMatrix_EmptyFieldNameAliasClaimAgreement(t *testing.T) {
	var cells int
	for _, readerName := range []string{"", "a"} {
		for _, aliasName := range []string{"b", ""} {
			if readerName == aliasName {
				continue // a field name colliding with its own alias is rejected at parse
			}
			reader := fmt.Sprintf(`{"type":"record","name":"R","fields":[{"name":%q,"type":"int","aliases":[%q]}]}`,
				readerName, aliasName)
			r, err := avro.Parse(reader, laxAny)
			if err != nil {
				t.Fatalf("reader %s: %v", reader, err)
			}
			for _, order := range []struct {
				name  string
				first string
				last  string
			}{
				{"name-then-alias", readerName, aliasName},
				{"alias-then-name", aliasName, readerName},
			} {
				cells++
				writer := fmt.Sprintf(`{"type":"record","name":"R","fields":[{"name":%q,"type":"int"},{"name":%q,"type":"int"}]}`,
					order.first, order.last)
				w, err := avro.Parse(writer, laxAny)
				if err != nil {
					t.Fatalf("writer %s: %v", writer, err)
				}
				compatErr := avro.CheckCompatibility(w, r)
				_, resolveErr := avro.Resolve(w, r)
				where := fmt.Sprintf("reader field %q alias %q / writer %s", readerName, aliasName, order.name)
				if (compatErr == nil) != (resolveErr == nil) {
					t.Errorf("%s: CheckCompatibility and Resolve disagree\n  CheckCompatibility: %v\n  Resolve:            %v",
						where, compatErr, resolveErr)
				}
				if compatErr == nil {
					t.Errorf("%s: two writer fields resolve to one reader field and neither API refused it", where)
				}
			}
		}
	}
	t.Logf("cells: %d", cells)
}

// TestMatrix_EmptyFieldNameNonCollidingControl is the boundary: the same
// shapes with only ONE writer field claiming the reader slot must be ACCEPTED,
// by both APIs. Without this, refusing every writer that mentions an empty
// name would satisfy the matrix above.
func TestMatrix_EmptyFieldNameNonCollidingControl(t *testing.T) {
	for _, readerName := range []string{"", "a"} {
		for _, writerName := range []string{"", "a", "b"} {
			reader := fmt.Sprintf(`{"type":"record","name":"R","fields":[{"name":%q,"type":"int","aliases":["b"]}]}`, readerName)
			writer := fmt.Sprintf(`{"type":"record","name":"R","fields":[{"name":%q,"type":"int"}]}`, writerName)
			r, err := avro.Parse(reader, laxAny)
			if err != nil {
				t.Fatalf("reader %s: %v", reader, err)
			}
			w, err := avro.Parse(writer, laxAny)
			if err != nil {
				t.Fatalf("writer %s: %v", writer, err)
			}
			where := fmt.Sprintf("reader field %q alias \"b\" / single writer field %q", readerName, writerName)
			compatErr := avro.CheckCompatibility(w, r)
			_, resolveErr := avro.Resolve(w, r)
			if (compatErr == nil) != (resolveErr == nil) {
				t.Errorf("%s: CheckCompatibility and Resolve disagree\n  CheckCompatibility: %v\n  Resolve:            %v",
					where, compatErr, resolveErr)
			}
			// One writer field can claim at most one reader slot, so the only
			// reason to refuse is a genuinely absent required reader field.
			matches := writerName == readerName || writerName == "b"
			if matches && compatErr != nil {
				t.Errorf("%s: a single claiming writer field must be accepted: %v", where, compatErr)
			}
		}
	}
}

// ---------- enum_default_token_test.go ----------

// The enum-LEVEL "default" attribute must be a JSON STRING token naming a member
// symbol, and the token-type verdict is decided BEFORE the membership check: a
// non-string body can never name a symbol, and deciding by membership alone
// would let the json.Unmarshal zero value "" flow into the membership test —
// under a WithLaxNames validator accepting empty name components, a schema whose
// symbols legitimately include "" would then silently BIND the garbage default
// to it while the metadata surface reports no default at all.
//
// References: fastavro rejects every non-member (hence every non-string) enum
// default at parse; Java binds NO default for a non-text token
// (Schema.java:1921-1925 — textValue() is null for non-text nodes, skipping the
// containment check). Neither ever binds a default from a non-string token.

func laxAllEnumNames(string) error { return nil }

// enumResolutionFill encodes a writer symbol absent from reader r and
// resolved-decodes it, returning the filled reader default. The writer
// shares the reader's fullname (enum resolution matches by name) and
// carries the unknown symbol "UNKNOWN__".
func enumResolutionFill(t *testing.T, r *avro.Schema) (string, error) {
	t.Helper()
	w := avro.MustParse(`{"type":"enum","name":"E","symbols":["A","B","UNKNOWN__"]}`)
	res, err := avro.Resolve(w, r)
	if err != nil {
		return "", err
	}
	wire, err := w.Encode("UNKNOWN__")
	if err != nil {
		t.Fatalf("writer encode: %v", err)
	}
	var got string
	if _, err := res.Decode(wire, &got); err != nil {
		return "", err
	}
	return got, nil
}

// A non-string enum default under a lax-name validator with "" in symbols
// must reject by token type — never parse and bind the "" symbol.
func TestRegression_EnumDefaultLaxPhantomBindRejected(t *testing.T) {
	s, err := avro.Parse(
		`{"type":"enum","name":"E","symbols":["","A"],"default":5}`,
		avro.WithLaxNames(laxAllEnumNames))
	if err != nil {
		if !strings.Contains(err.Error(), "is not a string") {
			t.Fatalf("reject reason must be the token type, got: %v", err)
		}
		return
	}
	// Parse accepted: demonstrate the full phantom bind — metadata denies
	// the default while resolution fills it.
	root := s.Root()
	filled, ferr := enumResolutionFill(t, s)
	t.Fatalf("non-string enum default parsed under lax names: HasEnumDefault=%v EnumDefault=%q, resolution filled %q (err=%v) — must reject (default is not a string)",
		root.HasEnumDefault, root.EnumDefault, filled, ferr)
}

// An explicit null default is a non-string token like any other — it must
// reject in BOTH modes, even though json.Unmarshal(null, *string) is a
// no-error no-op that leaves the zero value in place.
func TestRegression_EnumDefaultExplicitNullRejected(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		opts   []avro.SchemaOpt
	}{
		{"strict", `{"type":"enum","name":"E","symbols":["A"],"default":null}`, nil},
		{"lax-empty-symbol", `{"type":"enum","name":"E","symbols":["","A"],"default":null}`,
			[]avro.SchemaOpt{avro.WithLaxNames(laxAllEnumNames)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := avro.Parse(c.schema, c.opts...)
			if err == nil {
				t.Fatal("null enum default parsed; must reject (not a string)")
			}
			if !strings.Contains(err.Error(), "is not a string") {
				t.Fatalf("reject reason must be the token type, got: %v", err)
			}
		})
	}
}

// The strict-mode reject of a non-string default must name the offending
// token — not echo the empty string the failed Unmarshal left behind.
func TestRegression_EnumDefaultNonStringEchoNamesToken(t *testing.T) {
	_, err := avro.Parse(`{"type":"enum","name":"E","symbols":["A"],"default":5}`)
	if err == nil {
		t.Fatal("non-string enum default parsed; must reject")
	}
	msg := err.Error()
	if !strings.Contains(msg, "is not a string") || !strings.Contains(msg, "5") {
		t.Fatalf("error must name the non-string token, got: %v", err)
	}
	if strings.Contains(msg, `default ""`) {
		t.Fatalf("error echoes the Unmarshal zero value instead of the token: %v", err)
	}
}

// Boundary controls for the token-type check.
func TestEnumDefaultEmptySymbolBoundary(t *testing.T) {
	// An explicit "" default is a legal STRING token; with "" a member
	// (lax names) it binds, both surfaces agree, and resolution fills it.
	t.Run("explicit-empty-binds", func(t *testing.T) {
		s, err := avro.Parse(
			`{"type":"enum","name":"E","symbols":["","A"],"default":""}`,
			avro.WithLaxNames(laxAllEnumNames))
		if err != nil {
			t.Fatalf("explicit \"\" default with \"\" a member must parse: %v", err)
		}
		root := s.Root()
		if !root.HasEnumDefault || root.EnumDefault != "" {
			t.Fatalf("metadata: HasEnumDefault=%v EnumDefault=%q, want true/\"\"", root.HasEnumDefault, root.EnumDefault)
		}
		filled, err := enumResolutionFill(t, s)
		if err != nil || filled != "" {
			t.Fatalf("resolution fill = %q, %v; want \"\"", filled, err)
		}
	})
	// The same "" default WITHOUT "" in symbols is a membership reject —
	// the string token passes the type check and fails containment.
	t.Run("empty-not-member-rejects", func(t *testing.T) {
		for _, c := range []struct {
			name string
			opts []avro.SchemaOpt
		}{
			{"strict", nil},
			{"lax", []avro.SchemaOpt{avro.WithLaxNames(laxAllEnumNames)}},
		} {
			_, err := avro.Parse(`{"type":"enum","name":"E","symbols":["A"],"default":""}`, c.opts...)
			if err == nil || !strings.Contains(err.Error(), "not a member") {
				t.Fatalf("%s: want membership reject, got: %v", c.name, err)
			}
		}
	})
}

// enumDefaultToken is one default-token cell of the class-elimination
// matrix: its JSON spelling, whether it is a JSON string token, and (for
// strings) the symbol it names.
type enumDefaultToken struct {
	name     string
	raw      string // as written in the schema JSON
	isString bool
	strVal   string // meaningful only when isString
}

var enumDefaultTokens = []enumDefaultToken{
	{"member-string", `"A"`, true, "A"},
	{"non-member-string", `"Z"`, true, "Z"},
	{"empty-string", `""`, true, ""},
	{"number", `5`, false, ""},
	{"null", `null`, false, ""},
	{"bool", `true`, false, ""},
	{"array", `["A"]`, false, ""},
	{"object", `{"a":1}`, false, ""},
}

type enumDefaultMode struct {
	name string
	lax  bool
}

var enumDefaultModes = []enumDefaultMode{{"strict", false}, {"lax", true}}

type enumDefaultSymbols struct {
	name          string
	json          string
	members       []string
	containsEmpty bool
}

var enumDefaultSymbolSets = []enumDefaultSymbols{
	{"plain", `["A","B"]`, []string{"A", "B"}, false},
	{"with-empty", `["","A","B"]`, []string{"", "A", "B"}, true},
}

// enumDefaultCellSchema renders the cell's schema JSON.
func enumDefaultCellSchema(sym enumDefaultSymbols, tok enumDefaultToken) string {
	return fmt.Sprintf(`{"type":"enum","name":"E","symbols":%s,"default":%s}`, sym.json, tok.raw)
}

// enumDefaultCellExpect derives the cell's verdict from the RULE, not the
// implementation: the schema parses iff the symbol set is legal in the
// mode AND the default is a string token naming a member. Reject class:
// symbol-set failures reject on the symbol; string non-members reject on
// membership; every non-string token rejects on token type.
func enumDefaultCellExpect(mode enumDefaultMode, sym enumDefaultSymbols, tok enumDefaultToken) (accept bool, rejectContains string) {
	if sym.containsEmpty && !mode.lax {
		return false, "symbol"
	}
	if !tok.isString {
		return false, "is not a string"
	}
	if slices.Contains(sym.members, tok.strVal) {
		return true, ""
	}
	return false, "not a member"
}

// TestMatrix_EnumDefaultTokenClassElimination crosses default token type
// x name mode x symbol set and checks the parse verdict plus, for
// accepted cells: metadata parity (HasEnumDefault/EnumDefault), the
// render round-trip (Root().Schema() re-parse preserves the default),
// canonical stripping (PCF never carries "default"), and the resolution
// fill (an unknown writer symbol resolves to the default).
func TestMatrix_EnumDefaultTokenClassElimination(t *testing.T) {
	for _, mode := range enumDefaultModes {
		for _, sym := range enumDefaultSymbolSets {
			for _, tok := range enumDefaultTokens {
				t.Run(mode.name+"/"+sym.name+"/"+tok.name, func(t *testing.T) {
					var opts []avro.SchemaOpt
					if mode.lax {
						opts = append(opts, avro.WithLaxNames(laxAllEnumNames))
					}
					schema := enumDefaultCellSchema(sym, tok)
					s, err := avro.Parse(schema, opts...)
					accept, rejectContains := enumDefaultCellExpect(mode, sym, tok)
					if !accept {
						if err == nil {
							t.Fatalf("parse accepted, want reject containing %q", rejectContains)
						}
						if !strings.Contains(err.Error(), rejectContains) {
							t.Fatalf("reject %v, want it to contain %q", err, rejectContains)
						}
						return
					}
					if err != nil {
						t.Fatalf("parse rejected an accept cell: %v", err)
					}

					root := s.Root()
					if !root.HasEnumDefault || root.EnumDefault != tok.strVal {
						t.Fatalf("metadata HasEnumDefault=%v EnumDefault=%q, want true/%q",
							root.HasEnumDefault, root.EnumDefault, tok.strVal)
					}
					if strings.Contains(string(s.Canonical()), `"default"`) {
						t.Fatalf("canonical form carries the stripped default: %s", s.Canonical())
					}
					rebuilt, err := root.Schema(opts...)
					if err != nil {
						t.Fatalf("render re-parse: %v", err)
					}
					rr := rebuilt.Root()
					if !rr.HasEnumDefault || rr.EnumDefault != tok.strVal {
						t.Fatalf("render round-trip HasEnumDefault=%v EnumDefault=%q, want true/%q",
							rr.HasEnumDefault, rr.EnumDefault, tok.strVal)
					}
					filled, err := enumResolutionFill(t, s)
					if err != nil {
						t.Fatalf("resolution fill: %v", err)
					}
					if filled != tok.strVal {
						t.Fatalf("resolution filled %q, want %q", filled, tok.strVal)
					}
				})
			}
		}
	}
}

// FIELD-level enum defaults go through the record default pipeline, whose
// enum arm requires a string (validateLeaf) and validates membership at
// parse — deliberately stricter than the references (a non-member default
// can never encode; fastavro parses non-member field defaults outright,
// Java's isValidDefault ENUM arm is isTextual-only). The token classes
// eliminate the same way at this second consumption site.
func TestMatrix_EnumFieldDefaultTokenTypes(t *testing.T) {
	cells := []struct {
		name    string
		raw     string
		wantErr string // "" accepts
	}{
		{"member-string", `"A"`, ""},
		{"non-member-string", `"Z"`, "not a member"},
		{"number", `5`, "expected string"},
		{"null", `null`, "expected string"},
		{"bool", `true`, "expected string"},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			schema := fmt.Sprintf(`{"type":"record","name":"R","fields":[
				{"name":"f","type":{"type":"enum","name":"E","symbols":["A","B"]},"default":%s}]}`, c.raw)
			s, err := avro.Parse(schema)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("parse: %v", err)
				}
				root := s.Root()
				if d, ok := root.Fields[0].Default.(string); !ok || d != "A" {
					t.Fatalf("field default = %#v, want \"A\"", root.Fields[0].Default)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("want reject containing %q, got: %v", c.wantErr, err)
			}
		})
	}
}

// A "default" key riding a WRAPPER around an enum REFERENCE never BINDS: the
// enum-level default is read only at the definition site, and a reference
// wrapper's own kind is the referenced NAME, which binds nothing. The key is
// therefore an ordinary custom property of the usage site for EVERY token type —
// it rides in Props as its only surface, the rebuild preserves it as written,
// and the enum it names still declares no default. Binding is decided by
// placement, never by the body.
//
// The SchemaCache cross-parse spelling differs by design, and the difference is
// the splice rather than the routing: the cache materializes the DEFINITION in
// place of the reference, and the merge is definition-wins on consumed-ness, so
// an enum definition swallows the wrapper's "default".
func TestRegression_EnumRefWrapperDefaultInert(t *testing.T) {
	for _, tok := range []struct {
		src  string
		want any
	}{
		{`"B"`, "B"},
		{`5`, int64(5)},
	} {
		t.Run("direct-"+tok.src, func(t *testing.T) {
			s, err := avro.Parse(fmt.Sprintf(`{"type":"record","name":"R","fields":[
				{"name":"a","type":{"type":"enum","name":"E","symbols":["A","B"]}},
				{"name":"b","type":{"type":"E","default":%s}}]}`, tok.src))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			root := s.Root()
			b := root.Fields[1].Type
			// Not bound: the usage site declares no enum default.
			if b.HasEnumDefault || b.EnumDefault != "" {
				t.Fatalf("wrapper default BOUND at a reference: HasEnumDefault=%v EnumDefault=%q",
					b.HasEnumDefault, b.EnumDefault)
			}
			// Preserved: Props is its only surface, as written.
			if got, ok := b.Props["default"]; !ok || !reflect.DeepEqual(got, tok.want) {
				t.Fatalf("wrapper default not preserved as written: Props=%#v, want %#v", b.Props, tok.want)
			}
			// And the definition it names is untouched.
			if a := root.Fields[0].Type; a.HasEnumDefault {
				t.Fatalf("the usage site's default reached the DEFINITION: %+v", a)
			}
			rebuilt, err := root.Schema()
			if err != nil {
				t.Fatalf("rebuild: %v", err)
			}
			if !strings.Contains(rebuilt.String(), "default") {
				t.Fatalf("rebuild dropped the as-written wrapper default: %s", rebuilt.String())
			}
			// Re-parsing the rebuild still binds no enum default anywhere:
			// preservation is not promotion.
			again, err := avro.Parse(rebuilt.String())
			if err != nil {
				t.Fatalf("reparse: %v", err)
			}
			for i, f := range again.Root().Fields {
				if f.Type.HasEnumDefault {
					t.Fatalf("field %d bound an enum default after the round trip: %s", i, rebuilt)
				}
			}
		})
	}
	t.Run("cache-splice", func(t *testing.T) {
		var c avro.SchemaCache
		mustCacheParse(t, &c, `{"type":"enum","name":"E2","symbols":["A","B"]}`)
		s, err := c.Parse(`{"type":"record","name":"R2","fields":[{"name":"f","type":{"type":"E2","default":"B"}}]}`)
		if err != nil {
			t.Fatalf("cache parse: %v", err)
		}
		root := s.Root()
		f := root.Fields[0].Type
		// The splice replaced the reference with the enum definition, which
		// CONSUMES "default" — definition-wins, so the usage-site copy does
		// not ride along and does not become the definition's default.
		if f.HasEnumDefault || f.EnumDefault != "" || len(f.Props) != 0 {
			t.Fatalf("spliced wrapper default leaked: HasEnumDefault=%v EnumDefault=%q Props=%v",
				f.HasEnumDefault, f.EnumDefault, f.Props)
		}
		rebuilt, err := root.Schema()
		if err != nil {
			t.Fatalf("splice rebuild: %v", err)
		}
		if strings.Contains(rebuilt.String(), "default") {
			t.Fatalf("splice rebuild carries the inert wrapper default: %s", rebuilt.String())
		}
	})
}

// TestDifferentialFastavroEnumDefaultToken drives every enum-LEVEL matrix cell
// through fastavro's parser and asserts VERDICT parity wherever fastavro can
// parse the cell's schema shape at all — self-calibrated: a cell whose
// no-default twin fastavro rejects is schema-blocked and skipped. fastavro
// rejects every non-member default, which subsumes every non-string token, so
// parity holds cell-for-cell on the parseable shapes.
//
// The FIELD-level cells pin fastavro's LAXER observed verdict instead: 1.12.2
// parses non-member and non-string enum FIELD defaults outright, a calibrated
// divergence where twmb is deliberately stricter. A release that starts
// rejecting flips these cells and forces recalibration.
func TestDifferentialFastavroEnumDefaultToken(t *testing.T) {
	o := startOracle(t)

	for _, mode := range enumDefaultModes {
		for _, sym := range enumDefaultSymbolSets {
			for _, tok := range enumDefaultTokens {
				t.Run("level/"+mode.name+"/"+sym.name+"/"+tok.name, func(t *testing.T) {
					twin := fmt.Sprintf(`{"type":"enum","name":"E","symbols":%s}`, sym.json)
					if !o.call(oracleJob{Op: "parse", Schema: []byte(twin)}).OK {
						t.Skipf("schema shape blocked for fastavro (no-default twin rejects)")
					}
					schema := enumDefaultCellSchema(sym, tok)
					fast := o.call(oracleJob{Op: "parse", Schema: []byte(schema)})

					var opts []avro.SchemaOpt
					if mode.lax {
						opts = append(opts, avro.WithLaxNames(laxAllEnumNames))
					}
					_, err := avro.Parse(schema, opts...)
					if fast.OK != (err == nil) {
						t.Fatalf("verdict divergence: twmb err=%v, fastavro ok=%v err=%s", err, fast.OK, fast.Err)
					}
				})
			}
		}
	}

	// Field-level calibration, executed 1.12.2: fastavro TYPE-checks a
	// field default against the enum (number/null reject: "Default value
	// <5> must match schema type: enum") but does NOT check MEMBERSHIP
	// (a non-member string parses outright). twmb rejects all three —
	// stricter only on the membership half. A fastavro release changing
	// either half flips its cell and forces recalibration.
	for _, c := range []struct {
		name   string
		raw    string
		fastOK bool
	}{
		{"non-member-string", `"Z"`, true},
		{"number", `5`, false},
		{"null", `null`, false},
	} {
		t.Run("field/"+c.name, func(t *testing.T) {
			schema := fmt.Sprintf(`{"type":"record","name":"R","fields":[
				{"name":"f","type":{"type":"enum","name":"E","symbols":["A","B"]},"default":%s}]}`, c.raw)
			fast := o.call(oracleJob{Op: "parse", Schema: []byte(schema)})
			if fast.OK != c.fastOK {
				t.Fatalf("fastavro verdict flipped for enum FIELD default %s: ok=%v err=%s — recalibrate", c.name, fast.OK, fast.Err)
			}
			if _, err := avro.Parse(schema); err == nil {
				t.Fatal("twmb accepted a non-member/non-string enum field default; the stricter posture pin broke")
			}
		})
	}

	// Wrapper-reference calibration, executed 1.12.2: fastavro rejects
	// the WRAPPED named-reference form itself (UnknownType for
	// {"type":"E"} with or without extra keys), so the inert-default
	// posture is untestable there — the shape is fastavro-blocked. twmb
	// and Java both accept the wrapped form (Java TestUnionSelfReference)
	// and neither consumes the riding "default".
	t.Run("wrapper-ref", func(t *testing.T) {
		clean := `{"type":"record","name":"R","fields":[
			{"name":"a","type":{"type":"enum","name":"E","symbols":["A","B"]}},
			{"name":"b","type":{"type":"E"}}]}`
		withDefault := `{"type":"record","name":"R","fields":[
			{"name":"a","type":{"type":"enum","name":"E","symbols":["A","B"]}},
			{"name":"b","type":{"type":"E","default":5}}]}`
		if o.call(oracleJob{Op: "parse", Schema: []byte(clean)}).OK {
			t.Fatal("fastavro now ACCEPTS the wrapped named-reference form — recalibrate (the default-inert posture becomes testable)")
		}
		if o.call(oracleJob{Op: "parse", Schema: []byte(withDefault)}).OK {
			t.Fatal("fastavro accepts the default-bearing wrapper while rejecting the clean one — recalibrate")
		}
		if _, err := avro.Parse(withDefault); err != nil {
			t.Fatalf("twmb rejects the inert wrapper default: %v", err)
		}
	})
}

// ---------- union_default_order_test.go ----------

// Per Avro 1.12 (AVRO-3649) a union-field default may match ANY branch, so
// whether a schema PARSES — and which branch its default selects — must not
// depend on the textual order the branches are written in. Java's isValidDefault
// is anyMatch over an immutable JsonNode (Schema.java:1786), strictly
// order-independent.
//
// The branch matcher selects via validateDefault, whose container arms coerce
// record/array/map fields IN PLACE (the documented outer-float carveout). If each
// branch trial observes the SAME value a prior FAILED branch already coerced, a
// later string-typed branch rejects a float64 it should never have seen, making
// acceptance order-dependent. These pin order-independence of both the parse
// result and the selected branch. Generative: cross every branch-order
// permutation with leak shapes whose earlier record branch coerces a field and
// then fails.
func TestGenerative_UnionDefaultBranchOrderIndependent(t *testing.T) {
	type shape struct {
		name      string
		fail      string // a record branch that coerces a field then fails the default
		match     string // the record branch the default actually matches (all-string)
		def       string // default value JSON; matches `match`, triggers `fail`'s coercion
		wantField string // a field of `match` whose decoded value is asserted
		wantVal   any    // the expected decoded value of wantField
	}
	shapes := []shape{
		{
			name:      "double-field-leak",
			fail:      `{"type":"record","name":"Fa","fields":[{"name":"f","type":"double"},{"name":"g","type":"double"}]}`,
			match:     `{"type":"record","name":"Mb","fields":[{"name":"f","type":"string"},{"name":"g","type":"string"}]}`,
			def:       `{"f":"5","g":"z"}`,
			wantField: "f", wantVal: "5",
		},
		{
			name:      "array-element-leak",
			fail:      `{"type":"record","name":"Fa","fields":[{"name":"a","type":{"type":"array","items":"double"}},{"name":"g","type":"double"}]}`,
			match:     `{"type":"record","name":"Mb","fields":[{"name":"a","type":{"type":"array","items":"string"}},{"name":"g","type":"string"}]}`,
			def:       `{"a":["5"],"g":"z"}`,
			wantField: "g", wantVal: "z",
		},
		{
			name:      "map-value-leak",
			fail:      `{"type":"record","name":"Fa","fields":[{"name":"m","type":{"type":"map","values":"double"}},{"name":"g","type":"double"}]}`,
			match:     `{"type":"record","name":"Mb","fields":[{"name":"m","type":{"type":"map","values":"string"}},{"name":"g","type":"string"}]}`,
			def:       `{"m":{"k":"5"},"g":"z"}`,
			wantField: "g", wantVal: "z",
		},
	}

	for _, sh := range shapes {
		t.Run(sh.name, func(t *testing.T) {
			// Two branch sets: {fail, match} and {null, fail, match}; the
			// object default never matches null, so null is just a present
			// non-matching branch that must not perturb selection.
			for _, base := range [][]string{
				{sh.fail, sh.match},
				{`"null"`, sh.fail, sh.match},
			} {
				var firstDecoded any
				var firstMeta any
				orders := permuteStrings(base)
				for oi, order := range orders {
					schema := fmt.Sprintf(
						`{"type":"record","name":"O","fields":[{"name":"u","type":[%s],"default":%s}]}`,
						strings.Join(order, ","), sh.def)
					s, err := avro.Parse(schema)
					if err != nil {
						t.Fatalf("order %d %v: parse FAILED (order-dependent acceptance): %v\nschema=%s", oi, order, err, schema)
					}
					// Selected branch: auto-fill the absent field, decode, and
					// compare the value across every ordering — selection must
					// be identical regardless of branch order.
					wire, err := s.Encode(map[string]any{})
					if err != nil {
						t.Fatalf("order %d %v: auto-fill encode: %v", oi, order, err)
					}
					var got any
					if _, err := s.Decode(wire, &got); err != nil {
						t.Fatalf("order %d %v: decode: %v", oi, order, err)
					}
					u, ok := got.(map[string]any)["u"].(map[string]any)
					if !ok {
						t.Fatalf("order %d %v: decoded u is not a record map: %#v", oi, order, got)
					}
					if u[sh.wantField] != sh.wantVal {
						t.Fatalf("order %d %v: selected branch field %q = %#v, want %#v",
							oi, order, sh.wantField, u[sh.wantField], sh.wantVal)
					}
					if oi == 0 {
						firstDecoded = got
						firstMeta = s.Root().Fields[0].Default
					} else {
						if !reflect.DeepEqual(got, firstDecoded) {
							t.Fatalf("order %v: decoded default %#v differs from first ordering %#v", order, got, firstDecoded)
						}
						// Metadata-side sibling: branchAcceptsDefault is a pure
						// predicate (no in-place coercion), so Root().Default must
						// likewise be order-independent.
						if !reflect.DeepEqual(s.Root().Fields[0].Default, firstMeta) {
							t.Fatalf("order %v: metadata Default %#v differs from first ordering %#v",
								order, s.Root().Fields[0].Default, firstMeta)
						}
					}
				}
			}
		})
	}
}

// The minimal hand-written repro that motivates the generative matrix above.
func TestRegression_UnionDefaultLeakDoesNotRejectValidSchema(t *testing.T) {
	recA := `{"type":"record","name":"A","fields":[{"name":"x","type":"double"},{"name":"y","type":"int"}]}`
	recB := `{"type":"record","name":"B","fields":[{"name":"x","type":"string"},{"name":"y","type":"string"}]}`
	def := `{"x":"5","y":"z"}` // matches B; A coerces x:"5"->float64 then fails on y
	for _, order := range [][]string{{recA, recB}, {recB, recA}} {
		schema := fmt.Sprintf(`{"type":"record","name":"O","fields":[{"name":"u","type":[%s],"default":%s}]}`,
			strings.Join(order, ","), def)
		if _, err := avro.Parse(schema); err != nil {
			t.Fatalf("valid union default rejected (order leak): %v", err)
		}
	}
}

func permuteStrings(in []string) [][]string {
	if len(in) <= 1 {
		return [][]string{append([]string(nil), in...)}
	}
	var out [][]string
	for i := range in {
		rest := make([]string, 0, len(in)-1)
		rest = append(rest, in[:i]...)
		rest = append(rest, in[i+1:]...)
		for _, p := range permuteStrings(rest) {
			out = append(out, append([]string{in[i]}, p...))
		}
	}
	return out
}

// ---------- union_fixed_size_select_test.go ----------

// Per the Avro spec, a reader-union branch matches a writer fixed only when "both
// schemas are fixed whose sizes and (unqualified) names match", and resolution
// selects "the first schema in the reader's union that matches" — so fixed SIZE
// is part of the match predicate, and a wrong-size same-name branch does NOT match
// and selection must continue to a later size-matching branch. fastavro implements
// this; branch selection that matched on name alone and only rejected on size
// afterward let a wrong-size branch MASK a correct-size one, erroring on a value
// that is fully decodable. This pins that size is folded into selection for both
// Resolve and CheckCompatibility, both wires.
func TestRegression_UnionFixedSizeFoldedIntoSelection(t *testing.T) {
	writer := avro.MustParse(`{"type":"fixed","name":"F","namespace":"ns0","size":4}`)
	// The wrong-size branch (size 8) is declared BEFORE the correct-size (size 4):
	// it must be skipped, not select-then-reject and mask the size-4 branch.
	reader := avro.MustParse(`["null",{"type":"fixed","name":"F","namespace":"ns1","size":8},{"type":"fixed","name":"F","namespace":"ns2","size":4}]`)

	if err := avro.CheckCompatibility(writer, reader); err != nil {
		t.Fatalf("CheckCompatibility rejected a writer that matches the size-4 branch: %v", err)
	}
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatalf("Resolve rejected a writer that matches the size-4 branch: %v", err)
	}

	wire, err := writer.AppendEncode(nil, [4]byte{1, 2, 3, 4})
	if err != nil {
		t.Fatalf("encode writer value: %v", err)
	}
	// Binary: the resolved union decodes the writer's 4-byte fixed via the
	// size-4 branch.
	var got any
	if _, err := resolved.Decode(wire, &got); err != nil {
		t.Fatalf("resolved binary decode: %v", err)
	}
	if !fixedBytesEqual(got, []byte{1, 2, 3, 4}) {
		t.Fatalf("resolved binary decode value: got %T %v, want fixed [1 2 3 4]", got, got)
	}

	// JSON: the resolved DecodeJSON consumes writer-shaped JSON and resolves the
	// same way (NOT_BUGS #2). The writer encodes its fixed as a codepoint string.
	jwire, err := writer.AppendEncodeJSON(nil, [4]byte{1, 2, 3, 4})
	if err != nil {
		t.Fatalf("encode writer JSON: %v", err)
	}
	var gotJ any
	if err := resolved.DecodeJSON(jwire, &gotJ); err != nil {
		t.Fatalf("resolved JSON decode: %v", err)
	}
	if !fixedBytesEqual(gotJ, []byte{1, 2, 3, 4}) {
		t.Fatalf("resolved JSON decode value: got %T %v, want fixed [1 2 3 4]", gotJ, gotJ)
	}

	// Order-independence control: the size-4 branch declared first also resolves.
	readerRev := avro.MustParse(`["null",{"type":"fixed","name":"F","namespace":"ns2","size":4},{"type":"fixed","name":"F","namespace":"ns1","size":8}]`)
	if _, err := avro.Resolve(writer, readerRev); err != nil {
		t.Fatalf("Resolve (size-4 first) unexpectedly rejected: %v", err)
	}

	// Boundary: NO size-matching branch present must still reject (no silent
	// wrong-branch selection).
	readerNoMatch := avro.MustParse(`["null",{"type":"fixed","name":"F","namespace":"ns1","size":8}]`)
	if _, err := avro.Resolve(writer, readerNoMatch); err == nil {
		t.Fatal("Resolve must reject when no reader branch matches the writer's fixed size")
	}
}

func fixedBytesEqual(v any, want []byte) bool {
	switch b := v.(type) {
	case []byte:
		return string(b) == string(want)
	case [4]byte:
		return string(b[:]) == string(want)
	}
	return false
}

// ---------- union_tag_table_test.go ----------

// ---------------------------------------------------------------------------
// Union tag tables — one namespace, three consumers, one precedence rule.
//
// A union branch is addressed on the tagged wires by a NAME, and two different
// spellings can produce the same one: the "<kind>.<logicalType>" qualifier
// TagLogicalTypes emits for a primitive-backed logical branch, and the
// "<namespace>.<name>" fullname of a named type. "bytes.decimal" is both the
// qualifier of a decimal-on-bytes branch and the fullname of a fixed named
// "decimal" in namespace "bytes"; every name involved is valid under the strict
// Avro name regex, and the union is legal Avro that the references parse.
//
// Three tables read that one namespace — the JSON emitter, the decoder's
// tagged-map wrap, and the encoder's tagged-map lookup — so all three must agree
// on which branch owns a tag. The oracle is calibration-free: A VALUE'S JSON
// TAGGED ROUND TRIP MUST LAND ON THE BRANCH IT LEFT FROM. The binary branch
// index is the observable, read straight off the wire, so a tag resolving to a
// different branch shows up as a changed index rather than being inferred from a
// Go type.
//
// The second half is the over-correction guard: dropping the qualifier
// everywhere would satisfy the round trip too, and silently undo
// TagLogicalTypes. So the unambiguous case is pinned to still emit the qualified
// form.
// ---------------------------------------------------------------------------

// unionBranchIndexOf reads the leading zig-zag varint of an Avro union wire,
// which is the selected branch index.
func unionBranchIndexOf(t *testing.T, wire []byte) int64 {
	t.Helper()
	if len(wire) == 0 {
		t.Fatal("empty wire has no branch index")
	}
	var u uint64
	var shift uint
	for _, b := range wire {
		u |= uint64(b&0x7f) << shift
		if b < 0x80 {
			break
		}
		shift += 7
		if shift > 63 {
			t.Fatalf("branch index varint does not terminate in %x", wire)
		}
	}
	return int64(u>>1) ^ -int64(u&1)
}

// A collision family is a logical branch whose qualifier is spelled exactly
// like a named branch's fullname, given in both declaration orders because
// the tables are built by iterating branches and a last-write-wins map made
// the answer depend on that order.
type tagCollisionFamily struct {
	name      string
	logical   string // the logical-carrying branch, whose qualifier collides
	named     string // the named branch whose fullname is that same spelling
	namedFull string // that fullname, usable as an explicit tag
	values    []any  // values reaching one branch or the other
}

func tagCollisionFamilies() []tagCollisionFamily {
	return []tagCollisionFamily{
		{
			name:      "bytes.decimal",
			logical:   `{"type":"bytes","logicalType":"decimal","precision":20,"scale":2}`,
			named:     `{"type":"fixed","name":"decimal","namespace":"bytes","size":4}`,
			namedFull: "bytes.decimal",
			values:    []any{big.NewRat(1, 4), []byte{1, 2, 3, 4}},
		},
		{
			name:      "string.uuid",
			logical:   `{"type":"string","logicalType":"uuid"}`,
			named:     `{"type":"fixed","name":"uuid","namespace":"string","size":4}`,
			namedFull: "string.uuid",
			values:    []any{"6ba7b810-9dad-11d1-80b4-00c04fd430c8", []byte{1, 2, 3, 4}},
		},
		{
			name:      "int.date",
			logical:   `{"type":"int","logicalType":"date"}`,
			named:     `{"type":"fixed","name":"date","namespace":"int","size":4}`,
			namedFull: "int.date",
			values:    []any{int32(19000), []byte{1, 2, 3, 4}},
		},
	}
}

var tagOptionCombos = []struct {
	name string
	opts []avro.Opt
}{
	{"TaggedUnions", []avro.Opt{avro.TaggedUnions()}},
	{"TaggedUnions+TagLogicalTypes", []avro.Opt{avro.TaggedUnions(), avro.TagLogicalTypes()}},
}

// TestMatrix_UnionTagRoundTripPreservesBranch crosses the collision families
// with branch declaration order, the tag-affecting option combinations, and
// the values that reach each branch — including the tagged-map form, which is
// the only way a caller addresses the named branch explicitly.
func TestMatrix_UnionTagRoundTripPreservesBranch(t *testing.T) {
	var cells int
	for _, fam := range tagCollisionFamilies() {
		for _, order := range []struct {
			name   string
			schema string
		}{
			{"logical-declared-first", fmt.Sprintf(`["null",%s,%s]`, fam.logical, fam.named)},
			{"named-declared-first", fmt.Sprintf(`["null",%s,%s]`, fam.named, fam.logical)},
		} {
			s, err := avro.Parse(order.schema)
			if err != nil {
				t.Fatalf("%s/%s: this union is legal Avro and must parse: %v", fam.name, order.name, err)
			}
			// The tagged-map form addressing the NAMED branch by its exact
			// fullname is part of the value domain: it is the caller's only
			// handle on that branch, and it is the input the encoder's tag
			// lookup resolves.
			values := append([]any{}, fam.values...)
			values = append(values, map[string]any{fam.namedFull: []byte{1, 2, 3, 4}})

			for vi, v := range values {
				for _, combo := range tagOptionCombos {
					cells++
					wire, err := s.Encode(v)
					if err != nil {
						// A value no branch accepts is not a cell; the
						// round trip has nothing to preserve.
						continue
					}
					before := unionBranchIndexOf(t, wire)

					j, err := s.EncodeJSON(v, combo.opts...)
					if err != nil {
						t.Errorf("%s/%s/value#%d/%s: a value the binary wire accepts must encode as JSON: %v",
							fam.name, order.name, vi, combo.name, err)
						continue
					}
					var back any
					if err := s.DecodeJSON(j, &back, combo.opts...); err != nil {
						t.Errorf("%s/%s/value#%d/%s: the schema's OWN JSON output does not decode against it: %v\n  emitted %s",
							fam.name, order.name, vi, combo.name, err, j)
						continue
					}
					after, err := s.Encode(back)
					if err != nil {
						t.Errorf("%s/%s/value#%d/%s: the decoded value does not re-encode: %v\n  emitted %s",
							fam.name, order.name, vi, combo.name, err, j)
						continue
					}
					if got := unionBranchIndexOf(t, after); got != before {
						t.Errorf("%s/%s/value#%d/%s: the JSON tagged round trip MOVED the branch: %d -> %d\n"+
							"  emitted %s\n  the tag it emitted resolves to a different branch than the one that produced it",
							fam.name, order.name, vi, combo.name, before, got, j)
					}

					// The BINARY decode wrap is a SEPARATE consumer of the tag
					// namespace: it reads a table built at parse time rather
					// than computing the tag per value, so the JSON round trip
					// above cannot see it. Its envelope has to name the same
					// branch, because the value it produces is a tagged map a
					// caller hands straight back to Encode.
					var wrapped any
					if _, err := s.Decode(wire, &wrapped, combo.opts...); err != nil {
						t.Errorf("%s/%s/value#%d/%s: binary decode of the schema's own wire: %v",
							fam.name, order.name, vi, combo.name, err)
						continue
					}
					rewire, err := s.Encode(wrapped)
					if err != nil {
						t.Errorf("%s/%s/value#%d/%s: the binary decoder's tagged envelope does not re-encode: %v\n"+
							"  envelope %#v — its tag names a branch that will not take the value inside it",
							fam.name, order.name, vi, combo.name, err, wrapped)
						continue
					}
					if got := unionBranchIndexOf(t, rewire); got != before {
						t.Errorf("%s/%s/value#%d/%s: the BINARY tagged wrap MOVED the branch: %d -> %d\n"+
							"  envelope %#v", fam.name, order.name, vi, combo.name, before, got, wrapped)
					}
				}
			}
		}
	}
	t.Logf("cells: %d", cells)
}

// TestMatrix_UnionQualifiedTagStillEmittedWhenUnambiguous is the
// over-correction guard. Dropping the "<kind>.<logicalType>" qualifier
// unconditionally would satisfy the round-trip matrix above while silently
// disabling TagLogicalTypes, so the unambiguous case is pinned: with no
// branch owning the qualified spelling as its exact name, the qualified form
// is what gets emitted, and the unqualified form is what gets emitted without
// the option.
func TestMatrix_UnionQualifiedTagStillEmittedWhenUnambiguous(t *testing.T) {
	cells := []struct {
		name       string
		schema     string
		value      any
		wantLogTag string // under TaggedUnions+TagLogicalTypes
		wantStdTag string // under TaggedUnions alone
	}{
		{
			"long timestamp-millis, nothing owns the qualifier",
			`["null",{"type":"long","logicalType":"timestamp-millis"}]`,
			int64(1600000000000), "long.timestamp-millis", "long",
		},
		{
			"bytes decimal, nothing owns the qualifier",
			`["null",{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}]`,
			big.NewRat(1, 4), "bytes.decimal", "bytes",
		},
		{
			"int date, nothing owns the qualifier",
			`["null",{"type":"int","logicalType":"date"}]`,
			int32(19000), "int.date", "int",
		},
		{
			"string uuid, a fixed with an UNRELATED fullname is present",
			`["null",{"type":"string","logicalType":"uuid"},{"type":"fixed","name":"other","namespace":"elsewhere","size":4}]`,
			"6ba7b810-9dad-11d1-80b4-00c04fd430c8", "string.uuid", "string",
		},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			s := avro.MustParse(c.schema)
			j, err := s.EncodeJSON(c.value, avro.TaggedUnions(), avro.TagLogicalTypes())
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if want := `{"` + c.wantLogTag + `":`; !strings.HasPrefix(string(j), want) {
				t.Errorf("qualified tag was dropped where nothing collides with it:\n  got  %s\n  want prefix %s", j, want)
			}
			j2, err := s.EncodeJSON(c.value, avro.TaggedUnions())
			if err != nil {
				t.Fatalf("encode without TagLogicalTypes: %v", err)
			}
			if want := `{"` + c.wantStdTag + `":`; !strings.HasPrefix(string(j2), want) {
				t.Errorf("unqualified tag is wrong:\n  got  %s\n  want prefix %s", j2, want)
			}
		})
	}
}

// TestInvariant_UnionTagOwnerIsUniquePerSchema states the property the tables
// exist to hold, over every union any other cell in this file builds plus the
// ordinary shapes: no two branches may EMIT the same tag under the same
// options. This is the table-level statement of the round-trip property, and
// it fails on a schema for which no value happens to be in the value domain
// above.
func TestInvariant_UnionTagOwnerIsUniquePerSchema(t *testing.T) {
	var schemas []string
	for _, fam := range tagCollisionFamilies() {
		schemas = append(schemas,
			fmt.Sprintf(`["null",%s,%s]`, fam.logical, fam.named),
			fmt.Sprintf(`["null",%s,%s]`, fam.named, fam.logical),
		)
	}
	schemas = append(schemas,
		`["null","int","string"]`,
		`["null",{"type":"long","logicalType":"timestamp-millis"},{"type":"int","logicalType":"date"}]`,
		`["null",{"type":"fixed","name":"F","size":16,"logicalType":"uuid"},{"type":"string","logicalType":"uuid"}]`,
		`[{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]},{"type":"enum","name":"E","symbols":["A"]}]`,
	)
	for _, sc := range schemas {
		t.Run(sc, func(t *testing.T) {
			s := mustParse(t, sc)
			root := s.Root()
			if root.Type != "union" {
				t.Fatalf("expected a union, got %q", root.Type)
			}
			for _, tagLogical := range []bool{false, true} {
				seen := map[string]int{}
				for i := range root.Branches {
					tag := unionEmitTagForTest(t, s, i, tagLogical)
					if tag == "" {
						continue // null branch is never wrapped
					}
					if prev, dup := seen[tag]; dup {
						t.Errorf("tagLogical=%v: branches %d and %d both emit the tag %q; a tag must name exactly one branch",
							tagLogical, prev, i, tag)
					}
					seen[tag] = i
				}
			}
		})
	}
}

// unionEmitTagForTest recovers the tag a branch emits using only the public
// API: encode a value onto that branch and read the key of the tagged
// envelope. Branches no probe value reaches report "".
func unionEmitTagForTest(t *testing.T, s *avro.Schema, branch int, tagLogical bool) string {
	t.Helper()
	opts := []avro.Opt{avro.TaggedUnions()}
	if tagLogical {
		opts = append(opts, avro.TagLogicalTypes())
	}
	probes := []any{
		big.NewRat(1, 4), []byte{1, 2, 3, 4}, int32(19000), int64(1600000000000),
		"6ba7b810-9dad-11d1-80b4-00c04fd430c8", "A",
		map[string]any{"x": int32(1)}, [16]byte{}, true, 1.5,
	}
	for _, p := range probes {
		wire, err := s.Encode(p)
		if err != nil || unionBranchIndexOf(t, wire) != int64(branch) {
			continue
		}
		j, err := s.EncodeJSON(p, opts...)
		if err != nil {
			continue
		}
		str := string(j)
		if !strings.HasPrefix(str, `{"`) {
			continue // bare null
		}
		if end := strings.Index(str[2:], `"`); end >= 0 {
			return str[2 : 2+end]
		}
	}
	return ""
}

// TestMatrix_UnionNamedTypeSpelledLikeAKindIsRejected pins the sibling ruling:
// the DUPLICATE-BRANCH check keys a named branch by its fullname and an unnamed
// one by its kind, in one namespace, so a null-namespace named type spelled like
// an unnamed complex kind collides with a branch of that kind. The rejection is
// deliberate and NOT the same question as the tag tables above: Apache Avro
// rejects the identical shape for the identical reason — UnionSchema keys
// indexByName by getFullName(), which for an unnamed schema returns the kind
// string — and the Avro JSON encoding would give both branches the same envelope
// name. A logical qualifier colliding with a named branch's fullname is the
// OTHER case: legal Avro that stays accepted, with only the emitted tag
// degrading.
func TestMatrix_UnionNamedTypeSpelledLikeAKindIsRejected(t *testing.T) {
	reject := []string{
		`[{"type":"record","name":"map","fields":[{"name":"x","type":"int"}]},{"type":"map","values":"int"}]`,
		`[{"type":"record","name":"array","fields":[{"name":"x","type":"int"}]},{"type":"array","items":"int"}]`,
		`[{"type":"fixed","name":"map","size":4},{"type":"map","values":"int"}]`,
		`[{"type":"enum","name":"array","symbols":["A"]},{"type":"array","items":"int"}]`,
		// Declaration order does not change the answer.
		`[{"type":"map","values":"int"},{"type":"record","name":"map","fields":[{"name":"x","type":"int"}]}]`,
	}
	for _, sc := range reject {
		if _, err := avro.Parse(sc); err == nil {
			t.Errorf("two branches would share one JSON envelope name, so this must be refused:\n  %s", sc)
		} else if !strings.Contains(err.Error(), "duplicate union type") {
			t.Errorf("refused for the wrong reason (%v):\n  %s", err, sc)
		}
	}
	// Controls. A NAMESPACE keeps the fullname off the kind's spelling, and a
	// name spelled like a kind is fine when no branch of that kind is present.
	accept := []string{
		`[{"type":"record","name":"map","namespace":"ns","fields":[{"name":"x","type":"int"}]},{"type":"map","values":"int"}]`,
		`[{"type":"record","name":"array","namespace":"ns","fields":[{"name":"x","type":"int"}]},{"type":"array","items":"int"}]`,
		`[{"type":"record","name":"map","fields":[{"name":"x","type":"int"}]},"int"]`,
		`[{"type":"record","name":"array","fields":[{"name":"x","type":"int"}]},"int"]`,
	}
	for _, sc := range accept {
		if _, err := avro.Parse(sc); err != nil {
			t.Errorf("no two branches share an envelope name here, so this must parse: %v\n  %s", err, sc)
		}
	}
}

// TestRegression_ResolvedWriterUnionEmitsNoTag is the pinned control behind
// the one site that still builds its wrap tables from the raw branch names
// rather than the collision-aware tag: a writer-only union resolved against a
// NON-union reader. Its tables are never read, because the reader has no union
// to dispatch through and the wrap is suppressed outright — so leaving them
// raw is safe only for as long as that suppression holds, which is what this
// asserts. If the wrap is ever enabled there, the decoded value gains an
// envelope and this fails, which is the signal to route that site through the
// same tag rule as the others.
func TestRegression_ResolvedWriterUnionEmitsNoTag(t *testing.T) {
	writer := avro.MustParse(`["int","long"]`)
	reader := avro.MustParse(`"long"`)
	res, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	wire, err := writer.Encode(int32(7))
	if err != nil {
		t.Fatalf("writer encode: %v", err)
	}
	for _, combo := range tagOptionCombos {
		var back any
		if _, err := res.Decode(wire, &back, combo.opts...); err != nil {
			t.Fatalf("%s: resolved decode: %v", combo.name, err)
		}
		if m, wrapped := back.(map[string]any); wrapped {
			t.Errorf("%s: a writer-only union resolved to a non-union reader must decode BARE, got the envelope %v",
				combo.name, m)
		}
	}
}

// TestMatrix_UnionTagTierAcrossConsumers crosses every tier of the tag
// namespace with every consumer that reads it. Three consumers resolve a
// caller-written tag — the binary tagged-map encoder, the JSON tagged-map
// encoder, and the JSON tagged decoder — and a tier honored by some of them
// and not others is precisely the shape that let the legacy
// "<kind>.<logicalType>" spelling work on JSON and fail on binary.
//
// The oracle is agreement plus an explicit verdict per cell, so "all three
// reject everything" cannot pass.
func TestMatrix_UnionTagTierAcrossConsumers(t *testing.T) {
	const fixedUUID = `{"type":"fixed","name":"F","namespace":"n","size":16,"logicalType":"uuid"}`
	const recNS = `{"type":"record","name":"R","namespace":"ns","fields":[{"name":"x","type":"int"}]}`
	cells := []struct {
		tier   string
		schema string
		tag    string
		value  any
		want   bool // must every consumer accept it?
	}{
		{"exact name / kind", `["null","int"]`, "int", int32(7), true},
		{"exact name / fullname", `["null",` + recNS + `]`, "ns.R", map[string]any{"x": int32(1)}, true},
		{"exact name / named fixed", `["null",` + fixedUUID + `]`, "n.F", []byte("0123456789abcdef"), true},
		{"logical qualifier / primitive", `["null",{"type":"long","logicalType":"timestamp-millis"}]`,
			"long.timestamp-millis", int64(1600000000000), true},
		{"logical qualifier / named fixed", `["null",` + fixedUUID + `]`,
			"fixed.uuid", []byte("0123456789abcdef"), true},
		{"unqualified short name", `["null",` + recNS + `]`, "R", map[string]any{"x": int32(1)}, true},
		// Guarded tiers refuse a name two branches claim, on every consumer.
		{"logical qualifier / ambiguous",
			`["null",{"type":"fixed","name":"A","size":16,"logicalType":"uuid"},{"type":"fixed","name":"B","size":16,"logicalType":"uuid"}]`,
			"fixed.uuid", []byte("0123456789abcdef"), false},
		{"unqualified short name / ambiguous",
			`["null",{"type":"record","name":"R","namespace":"n1","fields":[{"name":"x","type":"int"}]},{"type":"record","name":"R","namespace":"n2","fields":[{"name":"y","type":"int"}]}]`,
			"R", map[string]any{"x": int32(1)}, false},
		// A tag no tier claims is refused everywhere.
		{"no tier claims it", `["null","int"]`, "nope", int32(7), false},
	}
	for _, c := range cells {
		t.Run(c.tier+"/"+c.tag, func(t *testing.T) {
			s := mustParse(t, c.schema)
			tagged := map[string]any{c.tag: c.value}

			_, binErr := s.Encode(tagged)
			_, jsonEncErr := s.EncodeJSON(tagged)
			// The JSON DECODER is driven from the JSON encoder's own output
			// when there is one, so the body shape is always right for the
			// branch; when the encoder refused, the tag is what is under test
			// and a minimal body is enough to see the routing verdict.
			jsonDecErr := error(nil)
			jsonBody, jerr := s.EncodeJSON(c.value)
			if jerr != nil {
				jsonBody = []byte("null")
			}
			var back any
			jsonDecErr = s.DecodeJSON([]byte(`{"`+c.tag+`":`+string(jsonBody)+`}`), &back, avro.TaggedUnions())

			got := map[string]error{"binary encode": binErr, "json encode": jsonEncErr, "json decode": jsonDecErr}
			for name, err := range got {
				if c.want && err != nil {
					t.Errorf("%s REFUSED a tag every consumer must accept: %v", name, err)
				}
				if !c.want && err == nil {
					t.Errorf("%s ACCEPTED a tag every consumer must refuse", name)
				}
			}
		})
	}
}

// ---------- default_forwardref_test.go ----------

// A record/array/map field may carry a default whose type (or its
// element/value/nested-field type) is a forward reference to a named type
// declared later in the same schema. Such schemas parse fine without a
// default, so adding a default must not turn Parse into a nil-pointer panic:
// the default-encode pipeline must run only after every forward-referenced
// child node is wired. Both the build-time deferral (container items/values)
// and the finalize-time ordering (nested record fields) are exercised, and
// the encoded default value is verified — not just the absence of a panic.
func TestMatrix_ForwardRefFieldDefaultEncodes(t *testing.T) {
	t.Run("array_items_forward_ref", func(t *testing.T) {
		s, err := avro.Parse(`{"type":"record","name":"R","fields":[
			{"name":"arr","type":{"type":"array","items":"Inner"},"default":[{"v":9}]},
			{"name":"l","type":{"type":"record","name":"Inner","fields":[{"name":"v","type":"int"}]}}
		]}`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		buf, err := s.AppendEncode(nil, map[string]any{"l": map[string]any{"v": 1}})
		if err != nil {
			t.Fatalf("encode (arr default fills): %v", err)
		}
		out := map[string]any{}
		mustDecode(t, s, buf, &out)
		arr, _ := out["arr"].([]any)
		if len(arr) != 1 {
			t.Fatalf("arr default: got %#v, want one element", out["arr"])
		}
		if inner, _ := arr[0].(map[string]any); inner == nil || inner["v"].(int32) != 9 {
			t.Errorf("arr[0].v = %#v, want int32(9)", arr[0])
		}
	})

	t.Run("map_values_forward_ref", func(t *testing.T) {
		s, err := avro.Parse(`{"type":"record","name":"R","fields":[
			{"name":"m","type":{"type":"map","values":"Inner"},"default":{"k":{"v":3}}},
			{"name":"l","type":{"type":"record","name":"Inner","fields":[{"name":"v","type":"int"}]}}
		]}`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		buf, err := s.AppendEncode(nil, map[string]any{"l": map[string]any{"v": 0}})
		if err != nil {
			t.Fatalf("encode (m default fills): %v", err)
		}
		out := map[string]any{}
		mustDecode(t, s, buf, &out)
		m, _ := out["m"].(map[string]any)
		inner, _ := m["k"].(map[string]any)
		if inner == nil || inner["v"].(int32) != 3 {
			t.Errorf("m[k].v = %#v, want int32(3)", m["k"])
		}
	})

	// Chained forward references: a's default {} must fill x with its default
	// {}, which must fill y with its default 7. This only works if every
	// field's default VALUE is resolved before any default's bytes are
	// encoded (encodeDefault fills an absent nested field from its resolved
	// f.defaultVal); resolving + encoding in a single pass would read x's
	// not-yet-set default and mis-encode.
	t.Run("nested_chained_forward_refs", func(t *testing.T) {
		s, err := avro.Parse(`{"type":"record","name":"Outer","fields":[
			{"name":"a","type":"Inner","default":{}},
			{"name":"b","type":{"type":"record","name":"Inner","fields":[
				{"name":"x","type":"Late","default":{}}
			]}},
			{"name":"c","type":{"type":"record","name":"Late","fields":[
				{"name":"y","type":"long","default":7}
			]}}
		]}`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		buf, err := s.AppendEncode(nil, map[string]any{
			"b": map[string]any{"x": map[string]any{"y": int64(1)}},
			"c": map[string]any{"y": int64(2)},
		})
		if err != nil {
			t.Fatalf("encode (a default fills): %v", err)
		}
		out := map[string]any{}
		mustDecode(t, s, buf, &out)
		a, _ := out["a"].(map[string]any)
		x, _ := a["x"].(map[string]any)
		if x == nil || x["y"].(int64) != 7 {
			t.Errorf("a.x.y = %#v, want int64(7) (chained default)", a)
		}
	})

	// Boundary: the forward-referenced shape WITHOUT a default must keep
	// parsing — proves the forward reference itself is accepted and that the
	// default is the only thing the fix changed.
	t.Run("forward_ref_without_default_still_parses", func(t *testing.T) {
		if _, err := avro.Parse(`{"type":"record","name":"R","fields":[
			{"name":"arr","type":{"type":"array","items":"Inner"}},
			{"name":"l","type":{"type":"record","name":"Inner","fields":[{"name":"v","type":"int"}]}}
		]}`); err != nil {
			t.Fatalf("no-default forward-ref array should parse: %v", err)
		}
	})

	// A forward-referenced default and the equivalent backward-referenced
	// default (named type declared first) must produce identical results —
	// the backward-ref path is the long-standing, already-correct one.
	t.Run("forward_matches_backward_ref", func(t *testing.T) {
		fwd := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"m","type":{"type":"map","values":"Inner"},"default":{"k":{"v":3}}},
			{"name":"l","type":{"type":"record","name":"Inner","fields":[{"name":"v","type":"int"}]}}
		]}`)
		bwd := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"l","type":{"type":"record","name":"Inner","fields":[{"name":"v","type":"int"}]}},
			{"name":"m","type":{"type":"map","values":"Inner"},"default":{"k":{"v":3}}}
		]}`)
		decodeM := func(s *avro.Schema) any {
			buf := mustAppendEncode(t, s, nil, map[string]any{"l": map[string]any{"v": 0}})
			out := map[string]any{}
			mustDecode(t, s, buf, &out)
			return out["m"]
		}
		if a, b := decodeM(fwd), decodeM(bwd); !reflect.DeepEqual(a, b) {
			t.Errorf("forward-ref default %#v != backward-ref default %#v", a, b)
		}
	})
}

// A field default whose type subtree references a record still under construction
// — a self- or mutual-recursive reference — must encode its binary defaultBytes
// against the COMPLETE record node, not the partial node that exists while the
// enclosing record's field loop is still running. Encoding inline at build time
// sees only the fields declared before the current one and silently drops the
// rest, producing truncated wire the same schema cannot decode, so the
// default-encode defers to finalize exactly as a not-yet-wired forward-ref child
// already does. EncodeJSON re-encodes the default at runtime against the complete
// node and was already correct, so it is the parity oracle.
func TestMatrix_SelfRefContainerDefaultEncodes(t *testing.T) {
	// roundTrip encodes a record that omits the defaulted field (triggering
	// binary default-fill from the precomputed bytes) and asserts the bytes
	// decode back via the same schema. Returns the decoded value.
	roundTrip := func(t *testing.T, s *avro.Schema, present map[string]any) map[string]any {
		t.Helper()
		buf, err := s.AppendEncode(nil, present)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		out := map[string]any{}
		rest, err := s.Decode(buf, &out)
		if err != nil {
			t.Fatalf("decode of Encode's own default-filled output failed: %v (wire=%x)", err, buf)
		}
		if len(rest) != 0 {
			t.Fatalf("decode left %d trailing bytes (wire=%x)", len(rest), buf)
		}
		return out
	}

	t.Run("self_array", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"tag","type":"int"},
			{"name":"kids","type":{"type":"array","items":"R"},"default":[{"tag":9,"kids":[]}]}]}`)
		// The exact wire of the default-filled record: tag=1 (0x02), then
		// kids = [ R{tag:9,kids:[]} ] = count 1 (0x02), item tag=9 (0x12),
		// item kids empty (0x00), array terminator (0x00). The pre-fix bug
		// dropped the inner kids and outer terminator, emitting 0x02021200.
		buf := mustAppendEncode(t, s, nil, map[string]any{"tag": int32(1)})
		if got, want := buf, []byte{0x02, 0x02, 0x12, 0x00, 0x00}; !reflect.DeepEqual(got, want) {
			t.Errorf("default-filled wire = %x, want %x", got, want)
		}
		out := roundTrip(t, s, map[string]any{"tag": int32(1)})
		kids, _ := out["kids"].([]any)
		if len(kids) != 1 {
			t.Fatalf("default kids = %#v, want one element", out["kids"])
		}
		if k0, _ := kids[0].(map[string]any); k0 == nil || k0["tag"].(int32) != 9 {
			t.Errorf("default kid = %#v, want {tag:9,kids:[]}", kids[0])
		}
		// Binary Encode must match what EncodeJSON (runtime re-encode, already
		// correct) produces for the same default-fill.
		jb := mustEncodeJSON(t, s, map[string]any{"tag": int32(1)})
		if want := `{"tag":1,"kids":[{"tag":9,"kids":[]}]}`; string(jb) != want {
			t.Errorf("EncodeJSON = %s, want %s", jb, want)
		}
	})

	t.Run("self_map", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"tag","type":"int"},
			{"name":"m","type":{"type":"map","values":"R"},"default":{"k":{"tag":9,"m":{}}}}]}`)
		out := roundTrip(t, s, map[string]any{"tag": int32(1)})
		m, _ := out["m"].(map[string]any)
		v, _ := m["k"].(map[string]any)
		if v == nil || v["tag"].(int32) != 9 {
			t.Errorf("default m[k] = %#v, want {tag:9,m:{}}", m["k"])
		}
	})

	// Two-level nesting: the inner kids [{tag:8,kids:[]}] must survive. The
	// pre-fix bug encoded the item against the partial record (tag only), so
	// the 1- and 2-level defaults produced IDENTICAL truncated bytes; this
	// subtest fails on the buggy code for a reason the 1-level case can't show.
	t.Run("self_array_two_levels", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"tag","type":"int"},
			{"name":"kids","type":{"type":"array","items":"R"},"default":[{"tag":9,"kids":[{"tag":8,"kids":[]}]}]}]}`)
		out := roundTrip(t, s, map[string]any{"tag": int32(1)})
		kids, _ := out["kids"].([]any)
		if len(kids) != 1 {
			t.Fatalf("level-1 kids = %#v, want one element", out["kids"])
		}
		k0, _ := kids[0].(map[string]any)
		if k0 == nil || k0["tag"].(int32) != 9 {
			t.Fatalf("level-1 kid = %#v, want tag 9", kids[0])
		}
		inner, _ := k0["kids"].([]any)
		if len(inner) != 1 {
			t.Fatalf("level-2 kids = %#v, want one element (pre-fix bug dropped it)", k0["kids"])
		}
		if k1, _ := inner[0].(map[string]any); k1 == nil || k1["tag"].(int32) != 8 {
			t.Errorf("level-2 kid = %#v, want tag 8", inner[0])
		}
	})

	// The recursive field is FIRST, so the record has NO prior fields when the
	// default is processed — the partial node is entirely empty.
	t.Run("self_array_recursive_field_first", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"kids","type":{"type":"array","items":"R"},"default":[{"kids":[]}]}]}`)
		out := roundTrip(t, s, map[string]any{})
		if kids, _ := out["kids"].([]any); len(kids) != 1 {
			t.Errorf("default kids = %#v, want one element", out["kids"])
		}
	})

	// Mutual recursion: Inner is built while Outer is still under construction,
	// and Inner.outers' default references Outer. This exercises the shared
	// in-construction set across the nested builder (Inner is built by a nested
	// builder; Outer lives in the parent's set).
	t.Run("mutual_recursion", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"Outer","fields":[
			{"name":"tag","type":"int"},
			{"name":"inner","type":{"type":"record","name":"Inner","fields":[
				{"name":"outers","type":{"type":"array","items":"Outer"},"default":[{"tag":7,"inner":{"outers":[]}}]}]}}]}`)
		out := roundTrip(t, s, map[string]any{"tag": int32(1), "inner": map[string]any{}})
		inner, _ := out["inner"].(map[string]any)
		outers, _ := inner["outers"].([]any)
		if len(outers) != 1 {
			t.Fatalf("inner.outers default = %#v, want one element", inner["outers"])
		}
		if o0, _ := outers[0].(map[string]any); o0 == nil || o0["tag"].(int32) != 7 {
			t.Errorf("inner.outers[0] = %#v, want {tag:7,...}", outers[0])
		}
	})

	// Controls — shapes the deferral must NOT break:
	t.Run("self_array_empty_default", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"tag","type":"int"},
			{"name":"kids","type":{"type":"array","items":"R"},"default":[]}]}`)
		out := roundTrip(t, s, map[string]any{"tag": int32(1)})
		if kids, _ := out["kids"].([]any); len(kids) != 0 {
			t.Errorf("empty default kids = %#v, want empty", out["kids"])
		}
	})

	t.Run("self_nullunion_default_null", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"tag","type":"int"},
			{"name":"kids","type":["null",{"type":"array","items":"R"}],"default":null}]}`)
		out := roundTrip(t, s, map[string]any{"tag": int32(1)})
		if out["kids"] != nil {
			t.Errorf("null default kids = %#v, want nil", out["kids"])
		}
	})

	// Non-recursive array-of-record default (a backward reference to a complete
	// type) must still round-trip — it was correct before and after the fix.
	t.Run("non_recursive_still_works", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"tag","type":"int"},
			{"name":"kids","type":{"type":"array","items":{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}},"default":[{"x":5}]}]}`)
		out := roundTrip(t, s, map[string]any{"tag": int32(1)})
		kids, _ := out["kids"].([]any)
		if len(kids) != 1 {
			t.Fatalf("default kids = %#v, want one element", out["kids"])
		}
		if k0, _ := kids[0].(map[string]any); k0 == nil || k0["x"].(int32) != 5 {
			t.Errorf("default kid = %#v, want {x:5}", kids[0])
		}
	})
}

// A default with no finite encoding — a required field whose default recurses
// into its own type — must be rejected at Parse, not stack-overflow and not
// silently produce truncated bytes. encodeDefault fills absent nested fields from
// their own defaults (unlike validateDefault, which skips absent fields and
// terminates vacuously), so without a recursion bound such a default recurses
// until the goroutine stack overflows and the process dies; the maxDepth ceiling
// turns it into an errTooDeep parse error instead.
func TestMatrix_InfiniteRecursiveDefaultRejected(t *testing.T) {
	cases := []struct{ name, schema string }{
		{"self_record", `{"type":"record","name":"R","fields":[
			{"name":"self","type":"R","default":{}}]}`},
		{"self_array", `{"type":"record","name":"R","fields":[
			{"name":"kids","type":{"type":"array","items":"R"},"default":[{}]}]}`},
		{"self_map", `{"type":"record","name":"R","fields":[
			{"name":"m","type":{"type":"map","values":"R"},"default":{"k":{}}}]}`},
		{"mutual", `{"type":"record","name":"A","fields":[
			{"name":"b","type":{"type":"record","name":"B","fields":[
				{"name":"a","type":"A","default":{}}]},"default":{}}]}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Must complete (the bound stops the recursion) and must reject.
			start := time.Now()
			_, err := avro.Parse(c.schema)
			if d, bound := time.Since(start), raceRelaxed(time.Second); d > bound {
				t.Fatalf("Parse took %v (>%v; recursion not bounded)", d, bound)
			}
			if err == nil {
				t.Fatalf("Parse accepted a default with no finite encoding; want rejection")
			}
		})
	}
}

// SchemaField.Default's documented contract: "Union defaults pick the first
// branch that accepts the value; the Go type tells you which branch was
// chosen." For a union whose first branch is a NAME-REFERENCED enum followed
// by a bytes branch, a valid-member string default must surface as a Go
// string (enum branch) — matching the wire encoder/decoder, which fills the
// enum branch — not as []byte (bytes branch). An inline enum already behaves
// correctly; the name-referenced enum must too.
func TestRegression_NameRefEnumUnionDefaultMetadata(t *testing.T) {
	// wireBranchByte encodes a record omitting the union field so its default
	// fills, then returns the union branch index byte the wire chose.
	wireBranchByte := func(t *testing.T, s *avro.Schema, omitField string, present map[string]any) byte {
		buf := mustAppendEncode(t, s, nil, present)
		// The enum-typed "def" field encodes first as a single ordinal byte
		// (0x00 for symbol "A"); the union field's default follows.
		return buf[1]
	}

	t.Run("name_ref_enum_member_default_picks_enum", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"def","type":{"type":"enum","name":"E","symbols":["A","B"]}},
			{"name":"f","type":["E","bytes"],"default":"A"}
		]}`)
		if b := wireBranchByte(t, s, "f", map[string]any{"def": "A"}); b != 0x00 {
			t.Fatalf("wire f-default branch byte = 0x%02x, want 0x00 (enum)", b)
		}
		if d := s.Root().Fields[1].Default; !isStr(d) {
			t.Errorf("Root().Fields[1].Default = %T %#v, want string (enum branch, matching wire)", d, d)
		}
	})

	// Boundary: a NON-member default ("Z") is rejected by the enum branch on
	// both surfaces, so both pick the later bytes branch — metadata []byte.
	t.Run("name_ref_enum_non_member_default_picks_bytes", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"def","type":{"type":"enum","name":"E","symbols":["A","B"]}},
			{"name":"f","type":["E","bytes"],"default":"Z"}
		]}`)
		if b := wireBranchByte(t, s, "f", map[string]any{"def": "A"}); b != 0x02 {
			t.Fatalf("wire f-default branch byte = 0x%02x, want 0x02 (bytes)", b)
		}
		if d := s.Root().Fields[1].Default; !isBytes(d) {
			t.Errorf("Root().Fields[1].Default = %T %#v, want []byte (bytes branch, matching wire)", d, d)
		}
	})

	// Boundary: the inline-enum form (no name reference) was already correct
	// and must stay correct.
	t.Run("inline_enum_member_default_picks_enum", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"f","type":[{"type":"enum","name":"E","symbols":["A","B"]},"bytes"],"default":"A"}
		]}`)
		if d := s.Root().Fields[0].Default; !isStr(d) {
			t.Errorf("inline enum: Default = %T %#v, want string", d, d)
		}
	})
}

func isStr(v any) bool   { _, ok := v.(string); return ok }
func isBytes(v any) bool { _, ok := v.([]byte); return ok }

// A union branch may be a forward reference to a named type declared later
// in the same schema (a shape twmb deliberately accepts and round-trips on
// the binary path). The builder leaves node.branches[i] nil for such a
// branch and patches only the ser/deser function tables in finalize; every
// path that walks node.branches directly — JSON encode, JSON decode, schema
// resolution, and union-default validation — must still see the resolved
// branch node, not the nil placeholder. These pin that finalize writes the
// resolved node back into the union's branch slice so none of those paths
// dereferences nil.
func TestRegression_ForwardRefUnionBranchAllPaths(t *testing.T) {
	type later struct {
		V int32 `avro:"v"`
	}
	type rec struct {
		Opt *later `avro:"opt"`
		L   later  `avro:"l"`
	}
	// "opt" references "Later" before it is defined in field "l".
	const sc = `{"type":"record","name":"R","fields":[
	  {"name":"opt","type":["null","Later"]},
	  {"name":"l","type":{"type":"record","name":"Later","fields":[{"name":"v","type":"int"}]}}]}`
	s, err := avro.Parse(sc)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	in := rec{Opt: &later{V: 9}, L: later{V: 1}}

	// Binary already worked; guard against regression.
	bin, err := s.AppendEncode(nil, in)
	if err != nil {
		t.Fatalf("binary encode: %v", err)
	}
	var binOut rec
	mustDecode(t, s, bin, &binOut)
	if !reflect.DeepEqual(in, binOut) {
		t.Fatalf("binary round-trip: got %+v want %+v", binOut, in)
	}

	// JSON encode + decode must not nil-panic and must round-trip.
	js, err := s.AppendEncodeJSON(nil, in)
	if err != nil {
		t.Fatalf("json encode: %v", err)
	}
	var jsOut rec
	mustDecodeJSON(t, s, js, &jsOut)
	if !reflect.DeepEqual(in, jsOut) {
		t.Fatalf("json round-trip: got %+v want %+v", jsOut, in)
	}

	// Schema resolution / compatibility must not nil-panic on the writer's
	// forward-ref union branch.
	mustResolve(t, s, s)
	if err := avro.CheckCompatibility(s, s); err != nil {
		t.Fatalf("CheckCompatibility: %v", err)
	}

	// Null branch (the other arm) must still round-trip through JSON too.
	inNull := rec{Opt: nil, L: later{V: 2}}
	js2, err := s.AppendEncodeJSON(nil, inNull)
	if err != nil {
		t.Fatalf("json encode (null arm): %v", err)
	}
	var jsOut2 rec
	if err := s.DecodeJSON(js2, &jsOut2); err != nil {
		t.Fatalf("json decode (null arm): %v", err)
	}
	if !reflect.DeepEqual(inNull, jsOut2) {
		t.Fatalf("json round-trip (null arm): got %+v want %+v", jsOut2, inNull)
	}
}

// A union field whose first branch is a forward reference can carry a
// default that matches that branch. The default validator walks
// node.branches; a nil forward-ref branch made it report "no branch
// matched" and reject a schema that is byte-equivalent to a backward-ordered
// one that parses. Pins that field order does not change acceptance.
func TestRegression_ForwardRefUnionDefaultParses(t *testing.T) {
	const forward = `{"type":"record","name":"R","fields":[
	  {"name":"u","type":["E","string"],"default":"A"},
	  {"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]}}]}`
	const backward = `{"type":"record","name":"R","fields":[
	  {"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]}},
	  {"name":"u","type":["E","string"],"default":"A"}]}`
	if _, err := avro.Parse(backward); err != nil {
		t.Fatalf("backward-ordered control should parse: %v", err)
	}
	sf, err := avro.Parse(forward)
	if err != nil {
		t.Fatalf("forward-ref union default should parse (byte-equivalent to backward): %v", err)
	}
	// The default "A" resolves to the enum branch; it surfaces in metadata.
	if got := sf.Root().Fields[0].Default; !isStr(got) || got.(string) != "A" {
		t.Fatalf("union default = %#v, want \"A\"", got)
	}
}

// ---------- negative_zero_default_test.go ----------

// doubleDefaultWireSignbit encodes the auto-filled single-double-field default
// and reports whether the wire double has its IEEE sign bit set.
func doubleDefaultWireSignbit(t *testing.T, s *avro.Schema) bool {
	t.Helper()
	wire, err := s.Encode(map[string]any{})
	if err != nil {
		t.Fatalf("encode default-fill: %v", err)
	}
	if len(wire) != 8 {
		t.Fatalf("expected 8 wire bytes for one double field, got %d (%x)", len(wire), wire)
	}
	return wire[7]&0x80 != 0 // little-endian double: sign bit in the top byte
}

// A negative zero written in FLOAT syntax ("-0.0", "-0e0") must keep its IEEE
// sign coherently across the wire, the metadata API (Root().Fields[].Default),
// and a Root().Schema() rebuild. Before the fix the metadata pipeline collapsed
// "-0.0" to int64(0) (a big.Rat has no signed zero), so Default reported +0.0
// while the wire wrote -0.0, and the rebuild re-emitted +0.0 (Go's json.Marshal
// renders -0.0 as the integer token "-0"). The fix preserves the sign in
// normalizeJSONNumber and re-emits float syntax ("-0.0") on rebuild.
func TestRegression_NegativeZeroFloatSyntaxDefaultCoherence(t *testing.T) {
	for _, lit := range []string{"-0.0", "-0e0"} {
		t.Run(lit, func(t *testing.T) {
			s, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"d","type":"double","default":` + lit + `}]}`)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if !doubleDefaultWireSignbit(t, s) {
				t.Errorf("%s: wire lost the negative sign", lit)
			}
			def, ok := s.Root().Fields[0].Default.(float64)
			if !ok {
				t.Fatalf("%s: Default is %T, want float64", lit, s.Root().Fields[0].Default)
			}
			if !math.Signbit(def) {
				t.Errorf("%s: metadata Default lost the negative sign: %v", lit, def)
			}
			// Rebuild must not flip the default wire.
			root := s.Root()
			s2, err := root.Schema()
			if err != nil {
				t.Fatalf("%s: rebuild: %v", lit, err)
			}
			w1, _ := s.Encode(map[string]any{})
			w2, _ := s2.Encode(map[string]any{})
			if string(w1) != string(w2) {
				t.Errorf("%s: Root().Schema() rebuild changed the default wire: %x -> %x", lit, w1, w2)
			}
		})
	}
}

// Positive zero (either syntax) is unsigned everywhere — the control that the
// fix's negative-zero detection does not over-trigger.
func TestNegativeZeroPositiveControls(t *testing.T) {
	for _, lit := range []string{"0.0", "0"} {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"d","type":"double","default":` + lit + `}]}`)
		if doubleDefaultWireSignbit(t, s) {
			t.Errorf("%s: positive zero wrote a sign bit", lit)
		}
		def := s.Root().Fields[0].Default.(float64)
		if math.Signbit(def) {
			t.Errorf("%s: positive zero metadata has a sign bit: %v", lit, def)
		}
	}
}

// Documented residual: a negative zero written in INTEGER syntax ("-0") is the
// integer 0. twmb's wire pipeline parses it via strconv.ParseFloat, which keeps
// the sign to -0.0 on both wires, while the metadata pipeline collapses it to
// int64(0) → +0.0 and the rebuild re-emits that. The references treat "-0" as
// sign-less 0 everywhere. Reconciling the wire to +0.0 would require changing
// the shared json.Number→float parser, which also drives runtime json.Number
// encode/decode and JSON float formatting, rippling into round-trip stability
// for genuine -0.0. twmb keeps the wire internally consistent and accepts the
// metadata-vs-wire divergence on this degenerate literal.
func TestNegativeZeroIntegerLiteralResidual(t *testing.T) {
	s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"d","type":"double","default":-0}]}`)
	if !doubleDefaultWireSignbit(t, s) {
		t.Error("integer -0: wire is expected to carry the ParseFloat sign (-0.0) today")
	}
	def := s.Root().Fields[0].Default.(float64)
	if math.Signbit(def) {
		t.Error("integer -0: metadata is expected to be +0.0 (int64 collapse) today")
	}
	// Binary and JSON wire must agree (the invariant twmb protects).
	binWire, _ := s.Encode(map[string]any{})
	jsonWire, _ := s.AppendEncodeJSON(nil, map[string]any{})
	var got map[string]any
	mustDecodeJSON(t, s, jsonWire, &got)
	reBin, _ := s.Encode(got)
	if string(binWire) != string(reBin) {
		t.Errorf("integer -0: binary (%x) and JSON-roundtrip (%x) wire diverge", binWire, reBin)
	}
}

// The float32 field arm and the Props metadata arm carry the same float-syntax
// rule. A "-0.0" default on a float field surfaces as float32(-0.0), and a
// "-0.0" property surfaces as float64(-0.0) (Java's Jackson DoubleNode), each
// stable through a Root().Schema() rebuild.
func TestRegression_NegativeZeroFloat32AndProps(t *testing.T) {
	t.Run("float32_field", func(t *testing.T) {
		s := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"f","type":"float","default":-0.0}]}`)
		def, ok := s.Root().Fields[0].Default.(float32)
		if !ok {
			t.Fatalf("Default is %T, want float32", s.Root().Fields[0].Default)
		}
		if !math.Signbit(float64(def)) {
			t.Errorf("float32 default lost its sign: %v", def)
		}
		wire, _ := s.Encode(map[string]any{})
		if len(wire) != 4 || wire[3]&0x80 == 0 {
			t.Errorf("float32 default wire not negative zero: %x", wire)
		}
	})

	t.Run("props_neg_zero", func(t *testing.T) {
		s := mustParse(t, `{"type":"record","name":"R","namespace":"ns","x":-0.0,"fields":[]}`)
		f, ok := s.Root().Props["x"].(float64)
		if !ok {
			t.Fatalf("Props[x] is %T, want float64", s.Root().Props["x"])
		}
		if !math.Signbit(f) {
			t.Errorf("Props negative zero lost its sign: %v", f)
		}
		root := s.Root()
		s2 := mustNodeSchema(t, root)
		f2, ok := s2.Root().Props["x"].(float64)
		if !ok || !math.Signbit(f2) {
			t.Errorf("rebuilt Props[x] = %v (%T), sign lost", s2.Root().Props["x"], s2.Root().Props["x"])
		}
	})
}

// ---------- flat_field_lift_test.go ----------

// The flat (goavro-style) field format puts a complex kind's defining key
// (symbols / items / values / fields / size) on the FIELD object alongside a
// bare string type. The wire parser lifts those keys into a nested type
// definition (liftFlatFieldType), and the metadata walker applies the same lift
// through the same shared helpers, so Root() describes the post-lift schema. The
// tests here drive that contract as a matrix:
//
//   - TestMatrix_FlatFieldLift: kind x namespace-mode. Per cell the flat form
//     and its handwritten nested twin are wire-identical (Canonical + Rabin —
//     the lift lives entirely in the parser, so the metadata walk must not
//     affect the wire tree); Root()'s type node carries the name and defining
//     content; routed keys do NOT appear in SchemaField.Props; Root().Schema()
//     rebuilds canonical-identically; and the rebuild's wire matches.
//   - TestMatrix_FlatFieldLiftLogicals: logicalType / precision / scale route
//     into the lifted type.
//   - TestMatrix_FlatFieldLiftNameRefDefaults: a lifted named type is registered
//     in the metadata name table, so name-referencing fields' defaults coerce
//     per the SchemaField.Default contract, across sibling / cross-record
//     diamond / recursive / SchemaCache cross-parse shapes.
//   - TestMatrix_FlatFieldLiftNoLiftParity: the boundary cases where the lift
//     must NOT fire, on either side. Parser and metadata walker share one
//     predicate, so a field the wire treats as a name reference is never
//     half-lifted in the metadata tree.
//   - TestMatrix_FlatFieldLiftDegenerate: degenerate-cardinality content lifts
//     and round-trips like any other.
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
		s := mustParse(t, `{"type":"record","name":"R","fields":[
			{"name":"F","type":"fixed","size":4},
			{"name":"F2","type":"F","default":"abcd"}]}`)
		b, ok := s.Root().Fields[1].Default.([]byte)
		if !ok || string(b) != "abcd" {
			t.Fatalf("F2 default = %T(%v), want []byte(abcd)", s.Root().Fields[1].Default, s.Root().Fields[1].Default)
		}
	})
	t.Run("sibling-enum", func(t *testing.T) {
		// Contract row: an enum default is already the member string on
		// both surfaces; the lift must leave it exactly as written.
		s := mustParse(t, `{"type":"record","name":"R","fields":[
			{"name":"E","type":"enum","symbols":["A","B"]},
			{"name":"E2","type":"E","default":"A"}]}`)
		if got, ok := s.Root().Fields[1].Default.(string); !ok || got != "A" {
			t.Fatalf("E2 default = %T(%v), want string A", s.Root().Fields[1].Default, s.Root().Fields[1].Default)
		}
	})
	t.Run("diamond", func(t *testing.T) {
		// The flat definition lives inside one nested record; a second
		// nested record references it by name with a default.
		s := mustParse(t, `{"type":"record","name":"R","fields":[
			{"name":"s1","type":{"type":"record","name":"Sub1","fields":[
				{"name":"F","type":"fixed","size":4}]}},
			{"name":"s2","type":{"type":"record","name":"Sub2","fields":[
				{"name":"f","type":"F","default":"wxyz"}]}}]}`)
		f := s.Root().Fields[1].Type.Fields[0]
		b, ok := f.Default.([]byte)
		if !ok || string(b) != "wxyz" {
			t.Fatalf("diamond ref default = %T(%v), want []byte(wxyz)", f.Default, f.Default)
		}
		root := s.Root()
		rebuilt := mustNodeSchema(t, root)
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
		s := mustParse(t, `{"type":"record","name":"R","fields":[
			{"name":"e","type":{"type":"enum","name":"X","symbols":["A"]},"symbols":["B","C"]}]}`)
		f := s.Root().Fields[0]
		if f.Type.Name != "X" || len(f.Type.Symbols) != 1 {
			t.Fatalf("nested type mangled: %+v", f.Type)
		}
		if _, ok := f.Props["symbols"]; !ok {
			t.Fatalf("stray field-level symbols missing from Props: %v", f.Props)
		}
		root := s.Root()
		rebuilt := mustNodeSchema(t, root)
		if !bytes.Equal(rebuilt.Canonical(), s.Canonical()) {
			t.Fatal("canonical mismatch")
		}
	})
	t.Run("name-ref-type-never-lifts", func(t *testing.T) {
		// A name reference with a stray defining key is a reference plus a
		// custom field property on both sides — "MyEnum" is not a liftable
		// kind name.
		s := mustParse(t, `{"type":"record","name":"R","fields":[
			{"name":"d","type":{"type":"enum","name":"MyEnum","symbols":["B"]}},
			{"name":"e","type":"MyEnum","symbols":["Z"]}]}`)
		f := s.Root().Fields[1]
		if f.Type.Type != "MyEnum" {
			t.Fatalf("reference field type = %+v, want bare MyEnum ref", f.Type)
		}
		if _, ok := f.Props["symbols"]; !ok {
			t.Fatalf("stray symbols missing from Props: %v", f.Props)
		}
		root := s.Root()
		rebuilt := mustNodeSchema(t, root)
		if !bytes.Equal(rebuilt.Canonical(), s.Canonical()) {
			t.Fatal("canonical mismatch")
		}
	})
	t.Run("primitive-type-never-lifts", func(t *testing.T) {
		s := mustParse(t, `{"type":"record","name":"R","fields":[
			{"name":"p","type":"int","symbols":["A"]}]}`)
		f := s.Root().Fields[0]
		if f.Type.Type != "int" {
			t.Fatalf("field type = %+v, want int", f.Type)
		}
		if _, ok := f.Props["symbols"]; !ok {
			t.Fatalf("stray symbols missing from Props: %v", f.Props)
		}
		root := s.Root()
		rebuilt := mustNodeSchema(t, root)
		if !bytes.Equal(rebuilt.Canonical(), s.Canonical()) {
			t.Fatal("canonical mismatch")
		}
	})
	t.Run("unnamed-explicit-namespace-stays-on-field", func(t *testing.T) {
		// The lift propagates "namespace" only for named kinds; on an
		// unnamed flat kind the wire parser drops it, and the metadata
		// walker preserves it as-written in the field's Props (the parser
		// ignores it on re-parse, so the rebuild is canonical-stable).
		s := mustParse(t, `{"type":"record","name":"R","fields":[
			{"name":"a","type":"array","items":"int","namespace":"x.y"}]}`)
		f := s.Root().Fields[0]
		if f.Type.Type != "array" || f.Type.Items == nil {
			t.Fatalf("lifted array: %+v", f.Type)
		}
		if got := f.Props["namespace"]; got != "x.y" {
			t.Fatalf("field Props[namespace] = %v, want x.y", got)
		}
		root := s.Root()
		rebuilt := mustNodeSchema(t, root)
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
	s := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"E","type":"enum","symbols":[]}]}`)
	f := s.Root().Fields[0]
	if f.Type.Type != "enum" || f.Type.Name != "E" || f.Type.Symbols == nil || len(f.Type.Symbols) != 0 {
		t.Fatalf("lifted empty enum: %+v", f.Type)
	}
	root := s.Root()
	rebuilt := mustNodeSchema(t, root)
	if !bytes.Equal(rebuilt.Canonical(), s.Canonical()) {
		t.Fatalf("canonical mismatch:\n %s\n %s", rebuilt.Canonical(), s.Canonical())
	}
}

// ---------- tag_contract_test.go ----------

// This file is the doc-contract net for doc.go's "# Struct tags" section: one
// oracle test per documented claim, so the documented behavior and the
// implementation cannot drift — the class that produced the omitzero and embed
// bugs. The oracle for the encode/decode claims is map[string]any, the
// separately-tested documented record encoding, plus a reflect/unsafe parity
// check, the unsafe path having its own field handling.
//
//   - avro:"name" / empty-name      -> TestTagContract_FieldNameMapping
//   - avro:"-" (exclude)            -> TestTagContract_ExcludeField
//   - avro:",inline"                -> TestTagContract_Inline
//   - avro:",omitzero"              -> TestMatrix_OmitzeroFillsSchemaDefault
//   - embedded inlining/precedence  -> embed_selection_test.go
//   - IsZero()                      -> ser_test.go / deser_test.go
//   - SchemaFor inference options   -> TestTagContract_SchemaForOptions

// encodeBoth returns the reflect-path wire (R value) and asserts the unsafe
// path (&R, addressable) produces the identical wire — catching a path that
// handles a tag differently (as the unsafe omitzero emit did).
func encodeBoth[T any](t *testing.T, s *avro.Schema, v T) []byte {
	t.Helper()
	valWire, err := s.AppendEncode(nil, v)
	if err != nil {
		t.Fatalf("encode value: %v", err)
	}
	ptrWire, err := s.AppendEncode(nil, &v)
	if err != nil {
		t.Fatalf("encode &value (unsafe path): %v", err)
	}
	if !bytes.Equal(valWire, ptrWire) {
		t.Fatalf("reflect vs unsafe path diverge: value=%x ptr=%x", valWire, ptrWire)
	}
	return valWire
}

// TestTagContract_FieldNameMapping: avro:"name" maps the Go field to that Avro
// field name; an empty tag uses the Go field name. Oracle: a map[string]any
// keyed by the Avro names encodes to the same wire, and decode round-trips.
func TestTagContract_FieldNameMapping(t *testing.T) {
	type R struct {
		Renamed int32 `avro:"actualName"` // explicit name
		Plain   int32 // no tag -> Go field name "Plain"
	}
	s := avro.MustParse(`{"type":"record","name":"R","fields":[
		{"name":"actualName","type":"int"},{"name":"Plain","type":"int"}]}`)

	structWire := encodeBoth(t, s, R{Renamed: 7, Plain: 9})
	mapWire, err := s.AppendEncode(nil, map[string]any{"actualName": int32(7), "Plain": int32(9)})
	if err != nil {
		t.Fatalf("map encode: %v", err)
	}
	if !bytes.Equal(structWire, mapWire) {
		t.Errorf("name mapping != map oracle: struct=%x map=%x", structWire, mapWire)
	}

	var got R
	mustDecode(t, s, structWire, &got)
	if got.Renamed != 7 || got.Plain != 9 {
		t.Errorf("decode round-trip: got %+v, want {7 9}", got)
	}
}

// TestTagContract_ExcludeField: avro:"-" excludes the field from the record on
// encode, decode, and SchemaFor. Oracle: a map with only the kept field.
func TestTagContract_ExcludeField(t *testing.T) {
	type R struct {
		Secret int32 `avro:"-"`    // excluded
		Kept   int32 `avro:"kept"` // present
	}
	s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"kept","type":"int"}]}`)

	structWire := encodeBoth(t, s, R{Secret: 99, Kept: 7})
	mapWire, err := s.AppendEncode(nil, map[string]any{"kept": int32(7)})
	if err != nil {
		t.Fatalf("map encode: %v", err)
	}
	if !bytes.Equal(structWire, mapWire) {
		t.Errorf("excluded field leaked into the wire: struct=%x map=%x", structWire, mapWire)
	}

	var got R
	mustDecode(t, s, structWire, &got)
	if got.Kept != 7 {
		t.Errorf("decode kept: got %d want 7", got.Kept)
	}

	// SchemaFor must omit the excluded field.
	inferred, err := avro.SchemaFor[R]()
	if err != nil {
		t.Fatalf("SchemaFor: %v", err)
	}
	if got := inferred.String(); bytes.Contains([]byte(got), []byte("Secret")) {
		t.Errorf("SchemaFor included the excluded field: %s", got)
	}
}

// TestTagContract_Inline: avro:",inline" on a NAMED nested-struct field
// flattens its fields into the parent record (like anonymous embedding).
// Oracle: a map with the flattened keys, and SchemaFor produces a flat record.
func TestTagContract_Inline(t *testing.T) {
	type Inner struct {
		A int32 `avro:"a"`
		B int32 `avro:"b"`
	}
	type Outer struct {
		I Inner `avro:",inline"`
		C int32 `avro:"c"`
	}
	s := avro.MustParse(`{"type":"record","name":"Outer","fields":[
		{"name":"a","type":"int"},{"name":"b","type":"int"},{"name":"c","type":"int"}]}`)

	structWire := encodeBoth(t, s, Outer{I: Inner{A: 1, B: 2}, C: 3})
	mapWire, err := s.AppendEncode(nil, map[string]any{"a": int32(1), "b": int32(2), "c": int32(3)})
	if err != nil {
		t.Fatalf("map encode: %v", err)
	}
	if !bytes.Equal(structWire, mapWire) {
		t.Errorf("inline != flattened map oracle: struct=%x map=%x", structWire, mapWire)
	}

	var got Outer
	mustDecode(t, s, structWire, &got)
	if got.I.A != 1 || got.I.B != 2 || got.C != 3 {
		t.Errorf("inline decode round-trip: got %+v, want {{1 2} 3}", got)
	}

	// SchemaFor must produce a flat record (a, b, c), not a nested one.
	inferred, err := avro.SchemaFor[Outer]()
	if err != nil {
		t.Fatalf("SchemaFor: %v", err)
	}
	if bytes.Contains([]byte(inferred.String()), []byte(`"type":"record","name":"Inner"`)) {
		t.Errorf("inline did not flatten in SchemaFor: %s", inferred.String())
	}
}

// TestTagContract_SchemaForOptions pins each documented SchemaFor inference
// option: the tag produces the expected attribute in the inferred schema.
func TestTagContract_SchemaForOptions(t *testing.T) {
	has := func(t *testing.T, s *avro.Schema, want string) {
		t.Helper()
		if got := s.String(); !strings.Contains(got, want) {
			t.Errorf("inferred schema missing %q:\n%s", want, got)
		}
	}
	t.Run("default", func(t *testing.T) {
		type R struct {
			X int32 `avro:"X,default=5"`
		}
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		has(t, s, `"default":5`)
	})
	t.Run("alias", func(t *testing.T) {
		type R struct {
			X int32 `avro:"X,alias=oldX"`
		}
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		has(t, s, `"aliases":["oldX"]`)
	})
	t.Run("type-alias", func(t *testing.T) {
		type Named struct {
			V int32 `avro:"v"`
		}
		type R struct {
			N Named `avro:"n,type-alias=oldName"`
		}
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		has(t, s, `"aliases":["oldName"]`)
	})
	t.Run("logical-override", func(t *testing.T) {
		type R struct {
			T time.Time `avro:"t,timestamp-micros"`
		}
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		has(t, s, `"logicalType":"timestamp-micros"`)
	})
	t.Run("decimal", func(t *testing.T) {
		type R struct {
			D *big.Rat `avro:"d,decimal(10,2)"`
		}
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		has(t, s, `"logicalType":"decimal"`)
		has(t, s, `"precision":10`)
		has(t, s, `"scale":2`)
	})
	t.Run("uuid", func(t *testing.T) {
		type R struct {
			U [16]byte `avro:"u,uuid"`
		}
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatal(err)
		}
		has(t, s, `"logicalType":"uuid"`)
	})
}

// ---------- tag_grammar_runtime_test.go ----------

// The runtime field mapper must tokenize the avro struct tag with the SAME
// grammar SchemaFor uses: a default= value takes the rest of the tag verbatim,
// and a bracketed alias=[...] value is not split on its internal commas. A
// naive comma split mis-reads a comma inside such a value as a separate option,
// so a chunk that happens to equal "omitzero"/"inline" spuriously activates
// that option — corrupting the zero value's wire form or making SchemaFor's own
// schema unencodable for the type that produced it.

type tagDefaultWithKeyword struct {
	// Per doc.go's grammar, default= takes the rest of the tag, so the default
	// value is the literal string "red,omitzero" — there is NO omitzero option.
	F string `avro:"f,default=red,omitzero"`
}

func TestRegression_RuntimeTagDefaultValueWithKeyword(t *testing.T) {
	s, err := avro.SchemaFor[tagDefaultWithKeyword]()
	if err != nil {
		t.Fatalf("SchemaFor: %v", err)
	}
	for _, tc := range []struct {
		name   string
		encode func(any) ([]byte, error)
		decode func([]byte, any) error
	}{
		{"binary", func(v any) ([]byte, error) { return s.Encode(v) }, func(b []byte, v any) error { _, e := s.Decode(b, v); return e }},
		{"json", func(v any) ([]byte, error) { return s.AppendEncodeJSON(nil, v) }, func(b []byte, v any) error { return s.DecodeJSON(b, v) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// omitzero is NOT a real option here, so the zero value must encode
			// as itself ("") and survive the round-trip, not be replaced by the
			// default.
			wire, err := tc.encode(&tagDefaultWithKeyword{F: ""})
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			var got tagDefaultWithKeyword
			if err := tc.decode(wire, &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got.F != "" {
				t.Fatalf("zero value corrupted: got %q, want %q (a comma in the default= value was mis-parsed as an omitzero option)", got.F, "")
			}
		})
	}
}

type tagAliasSub struct {
	A int32 `avro:"a"`
}

// The alias list contains "inline" as an element. Aliases accept any string, so
// this is legal; the runtime must not treat the alias element as an inline
// option (which would flatten the field's subfields and make field "f" missing).
type tagAliasListWithKeyword struct {
	F tagAliasSub `avro:"f,alias=[x,inline,y]"`
}

func TestRegression_RuntimeTagAliasListWithKeyword(t *testing.T) {
	s, err := avro.SchemaFor[tagAliasListWithKeyword]()
	if err != nil {
		t.Fatalf("SchemaFor: %v", err)
	}
	in := &tagAliasListWithKeyword{F: tagAliasSub{A: 7}}
	if _, err := s.Encode(in); err != nil {
		t.Fatalf("binary: SchemaFor-built schema cannot encode its own source type: %v", err)
	}
	if _, err := s.AppendEncodeJSON(nil, in); err != nil {
		t.Fatalf("json: SchemaFor-built schema cannot encode its own source type: %v", err)
	}
}

// A MALFORMED tag (unbalanced bracket) whose comma-separated tail happens to
// contain "inline"/"omitzero" must not fire that option. splitTag's grammar
// cannot tokenize an unbalanced bracket, so the runtime mapper falls back to a
// lenient form that maps the field by name with NO options — a hand-written-
// schema user's malformed tag stays usable (no new error) but a bracket typo
// never silently flips a field between nested-record and inline-flattened (or
// toggles omitzero). The only difference from the well-formed alias=[x,inline]
// case above is the missing ']'; the wire shape must be identical.
type tagMalformedInlineSub struct {
	X int32 `avro:"x"`
	Y int32 `avro:"y"`
}

func TestRegression_RuntimeMalformedTagFiresNoOption(t *testing.T) {
	// Field "f" is a nested record in the hand-written schema; the runtime must
	// map it as such, never flatten its subfields by spuriously firing inline.
	nested := `{"type":"record","name":"Outer","fields":[
		{"name":"f","type":{"type":"record","name":"Inner","fields":[
			{"name":"x","type":"int"},{"name":"y","type":"int"}]}}]}`
	s, err := avro.Parse(nested)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	type clean struct {
		F tagMalformedInlineSub `avro:"f"`
	}
	type closedBracket struct { // well-formed control: inline is an alias element
		F tagMalformedInlineSub `avro:"f,alias=[a,inline]"`
	}
	type unclosedInline struct { // malformed: missing ']', "inline" trails
		F tagMalformedInlineSub `avro:"f,alias=[a,inline"`
	}
	type unclosedOmitzero struct { // malformed: missing ']', "omitzero" trails
		F tagMalformedInlineSub `avro:"f,alias=[a,omitzero"`
	}

	want, err := s.Encode(clean{F: tagMalformedInlineSub{X: 1, Y: 2}})
	if err != nil {
		t.Fatalf("encode clean control: %v", err)
	}
	for _, tc := range []struct {
		name string
		wire func() ([]byte, error)
	}{
		{"closed_bracket_control", func() ([]byte, error) { return s.Encode(closedBracket{F: tagMalformedInlineSub{X: 1, Y: 2}}) }},
		{"unclosed_inline", func() ([]byte, error) { return s.Encode(unclosedInline{F: tagMalformedInlineSub{X: 1, Y: 2}}) }},
		{"unclosed_omitzero", func() ([]byte, error) { return s.Encode(unclosedOmitzero{F: tagMalformedInlineSub{X: 1, Y: 2}}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.wire()
			if err != nil {
				t.Fatalf("encode: a malformed/aliased tag corrupted the field mapping (option spuriously fired): %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("wire %x != clean %x: a tag option fired that must not have", got, want)
			}
		})
	}
}

// Controls: the documented options must still work when they ARE present, and a
// plain default= value (no embedded keyword) must round-trip the zero value to
// the default it actually fills only under omitzero.
func TestRuntimeTagOptionsStillFire(t *testing.T) {
	t.Run("omitzero_active_fills_default", func(t *testing.T) {
		type R struct {
			F string `avro:"f,omitzero"`
		}
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatalf("SchemaFor: %v", err)
		}
		// omitzero IS a real option here. SchemaFor gives the field no
		// default, so a nullable... actually no default → omitzero on a
		// non-nullable no-default field encodes the zero value itself. Just
		// assert it encodes without error on both formats (option recognized,
		// no spurious behavior change).
		if _, err := s.Encode(&R{}); err != nil {
			t.Fatalf("binary encode with omitzero: %v", err)
		}
	})
	t.Run("plain_default_no_keyword", func(t *testing.T) {
		type R struct {
			F string `avro:"f,default=plainvalue"`
		}
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatalf("SchemaFor: %v", err)
		}
		wire, err := s.Encode(&R{F: "kept"})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got R
		mustDecode(t, s, wire, &got)
		if got.F != "kept" {
			t.Fatalf("plain default field corrupted: got %q want %q", got.F, "kept")
		}
	})
}

// ---------- schemafor_oneway_text_test.go ----------

// SchemaFor infers a "string" schema for a Go type that implements a text
// interface only when a string schema ROUND-TRIPS for that type: a string-kind
// or []byte-slice type round-trips via its kind regardless of which text
// methods it has, but any OTHER type round-trips only if it implements BOTH an
// encode-side method (TextMarshaler/AppendText) AND TextUnmarshaler. A
// non-string type implementing exactly one direction would yield a one-
// directional "string" schema whose unsupported direction fails at Encode or
// Decode far from the SchemaFor call; SchemaFor cannot guess which direction
// the caller wants, so it refuses at build time (the same strict-reject posture
// as logical-type tags on incompatible Go types).

// --- refused: non-string types with exactly one text direction ---

type sfTextDecodeOnly struct{ S string }

func (c *sfTextDecodeOnly) UnmarshalText(b []byte) error { c.S = string(b); return nil }

type sfTextEncodeOnly struct{ S string }

func (c sfTextEncodeOnly) MarshalText() ([]byte, error) { return []byte(c.S), nil }

// --- accepted: round-trippable text types ---

type sfTextBoth struct{ S string }

func (c sfTextBoth) MarshalText() ([]byte, error)  { return []byte(c.S), nil }
func (c *sfTextBoth) UnmarshalText(b []byte) error { c.S = string(b); return nil }

type sfStrEncodeOnly string // string KIND: round-trips via the reflect.String fallback

func (s sfStrEncodeOnly) MarshalText() ([]byte, error) { return []byte("v:" + string(s)), nil }

type sfBytesDecodeOnly []byte // []byte KIND: round-trips via the []byte fallback

func (b *sfBytesDecodeOnly) UnmarshalText(t []byte) error { *b = append((*b)[:0], t...); return nil }

func TestRegression_SchemaForOneWayTextRefused(t *testing.T) {
	t.Run("decode-only-struct-refused", func(t *testing.T) {
		type R struct{ V sfTextDecodeOnly }
		_, err := avro.SchemaFor[R]()
		if err == nil {
			t.Fatal("SchemaFor must refuse a non-string type implementing only TextUnmarshaler (it could decode from but not encode to a string schema)")
		}
		if !strings.Contains(err.Error(), "TextUnmarshaler") {
			t.Fatalf("error should name the missing/present text direction, got: %v", err)
		}
	})
	t.Run("encode-only-struct-refused", func(t *testing.T) {
		type R struct{ V sfTextEncodeOnly }
		_, err := avro.SchemaFor[R]()
		if err == nil {
			t.Fatal("SchemaFor must refuse a non-string type implementing only TextMarshaler (it could encode to but not decode from a string schema)")
		}
		if !strings.Contains(err.Error(), "TextMarshaler") {
			t.Fatalf("error should name the missing/present text direction, got: %v", err)
		}
	})
	t.Run("decode-only-struct-uuid-tag-refused", func(t *testing.T) {
		// The ,uuid arm has the same one-directional hazard.
		type R struct {
			V sfTextDecodeOnly `avro:"v,uuid"`
		}
		_, err := avro.SchemaFor[R]()
		if err == nil {
			t.Fatal("SchemaFor must refuse a ,uuid-tagged non-string type implementing only one text direction")
		}
	})
}

func TestMatrix_SchemaForRoundTrippableTextStillBuilds(t *testing.T) {
	// Boundary-1: every type for which a string schema DOES round-trip must
	// still build and encode/decode. These must not regress when the
	// one-directional refusal above is added.
	t.Run("both-directions-struct", func(t *testing.T) {
		type R struct{ V sfTextBoth }
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatalf("a type implementing BOTH text directions must build: %v", err)
		}
		assertStringField(t, s)
		w, err := s.Encode(&R{V: sfTextBoth{S: "hi"}})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got R
		mustDecode(t, s, w, &got)
		if got.V.S != "hi" {
			t.Fatalf("round-trip: got %q want %q", got.V.S, "hi")
		}
	})
	t.Run("string-kind-encode-only", func(t *testing.T) {
		type R struct{ V sfStrEncodeOnly }
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatalf("a string-KIND type round-trips via the kind fallback and must build: %v", err)
		}
		assertStringField(t, s)
		mustEncode(t, s, &R{V: "x"})
	})
	t.Run("byte-slice-decode-only", func(t *testing.T) {
		type R struct{ V sfBytesDecodeOnly }
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatalf("a []byte-slice type round-trips via the []byte fallback and must build: %v", err)
		}
		assertStringField(t, s)
		w, err := s.Encode(&R{V: sfBytesDecodeOnly("abc")})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got R
		mustDecode(t, s, w, &got)
	})
	t.Run("net.IP-both-directions", func(t *testing.T) {
		type R struct{ IP net.IP }
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatalf("net.IP (both text directions, []byte kind) must build: %v", err)
		}
		in := R{IP: net.ParseIP("192.168.1.7")}
		w, err := s.Encode(&in)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got R
		mustDecode(t, s, w, &got)
		if !in.IP.Equal(got.IP) {
			t.Fatalf("net.IP round-trip: got %v want %v", got.IP, in.IP)
		}
	})
}

func assertStringField(t *testing.T, s *avro.Schema) {
	t.Helper()
	if got := s.String(); !strings.Contains(got, `"string"`) {
		t.Fatalf("expected a string field schema, got: %s", got)
	}
}

// ---------- json_escape_position_test.go ----------

// The Avro-JSON scanner is hand-rolled (json_scan.go), not encoding/json, so its
// string grammar is a second implementation of one thing and can drift from the
// stdlib's in either direction. The escape half was pinned by nothing: the
// rejection table in the conformance suite carried a COMMENT saying this package
// accepted unknown escapes and left the row out on that basis. The comment was
// false — every unknown escape is rejected — so the table had no row for correct
// behavior, and a regression that started accepting them would have passed.
//
// This is the class rather than the instance. The axes are the ESCAPE and the
// POSITION the string occupies, because the scanner is entered from many places
// and a single top-level probe cannot tell whether they share one string reader.
// The expectation per cell comes from encoding/json on the same escape in the
// same JSON string, so no verdict is read off current behavior.
type jsonEscapeCase struct {
	name   string
	escape string // the two-character escape as it appears in the JSON text
}

var jsonEscapeCases = []jsonEscapeCase{
	// Unknown escapes: encoding/json rejects each.
	{"unknown-letter", `\q`},
	{"hex-style", `\x41`},
	{"c-style-bell", `\a`},
	{"c-style-vtab", `\v`},
	{"c-style-nul", `\0`},
	{"escaped-space", `\ `},
	// Legal escapes, so the matrix pins the boundary in BOTH directions: a
	// scanner made strict enough to reject the six above must still accept
	// these, or the fix is a new rejection bug.
	{"newline", `\n`},
	{"quote", `\"`},
	{"backslash", `\\`},
	{"solidus", `\/`},
	{"unicode", `A`},
}

// jsonEscapePosition places a string carrying the escape somewhere a JSON
// document can hold one, and says which schema reads that document.
type jsonEscapePosition struct {
	name   string
	schema string
	// doc builds the JSON document with esc embedded in a string.
	doc func(esc string) string
}

var jsonEscapePositions = []jsonEscapePosition{
	{"top-level string", `"string"`,
		func(e string) string { return `"a` + e + `b"` }},
	{"record field value", `{"type":"record","name":"R","fields":[{"name":"x","type":"string"}]}`,
		func(e string) string { return `{"x":"a` + e + `b"}` }},
	{"record field NAME", `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`,
		func(e string) string { return `{"a` + e + `b":1}` }},
	{"map key", `{"type":"map","values":"int"}`,
		func(e string) string { return `{"a` + e + `b":1}` }},
	{"map value", `{"type":"map","values":"string"}`,
		func(e string) string { return `{"k":"a` + e + `b"}` }},
	{"array item", `{"type":"array","items":"string"}`,
		func(e string) string { return `["a` + e + `b"]` }},
	{"enum symbol", `{"type":"enum","name":"E","symbols":["A"]}`,
		func(e string) string { return `"a` + e + `b"` }},
	{"bytes", `"bytes"`,
		func(e string) string { return `"a` + e + `b"` }},
	{"union branch", `["null","string"]`,
		func(e string) string { return `{"string":"a` + e + `b"}` }},
	{"skipped unknown field", `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`,
		func(e string) string { return `{"x":1,"zz":"a` + e + `b"}` }},
	{"nested record field value", `{"type":"record","name":"R","fields":[{"name":"in","type":{"type":"record","name":"I","fields":[{"name":"y","type":"string"}]}}]}`,
		func(e string) string { return `{"in":{"y":"a` + e + `b"}}` }},
}

func TestMatrix_JSONEscapeRejectedAtEveryStringPosition(t *testing.T) {
	for _, pos := range jsonEscapePositions {
		s, err := avro.Parse(pos.schema)
		if err != nil {
			t.Fatalf("%s: schema does not parse: %v", pos.name, err)
		}
		for _, esc := range jsonEscapeCases {
			t.Run(pos.name+"/"+esc.name, func(t *testing.T) {
				doc := pos.doc(esc.escape)

				// The oracle: encoding/json's verdict on the SAME escape in a
				// string, decided outside this package. A bare string literal
				// is used rather than the whole document so the oracle answers
				// only the grammar question and not a schema question.
				var probe string
				wantReject := json.Unmarshal([]byte(`"a`+esc.escape+`b"`), &probe) != nil

				var out any
				err := s.DecodeJSON([]byte(doc), &out)

				// The verdict compared is the ESCAPE GRAMMAR's, not the
				// document's. Some positions reject a perfectly well-escaped
				// string for a SCHEMA reason — no enum symbol may contain an
				// escape at all, a field named `a\nb` is not the field the
				// record declares — so comparing overall accept/reject would
				// make those cells measure the schema and score a correct
				// grammar as a divergence. What the oracle knows is whether
				// the escape is legal, so that is what is asked of both sides:
				// an illegal escape must produce an ESCAPE error, and a legal
				// one must not, whatever the schema then decides.
				gotEscapeErr := err != nil && strings.Contains(err.Error(), "escape")
				if gotEscapeErr != wantReject {
					verb := map[bool]string{true: "an escape error", false: "no escape error"}
					t.Errorf("%s at %s: this package gives %s, encoding/json calls the escape %s.\ndoc=%s err=%v",
						esc.escape, pos.name, verb[gotEscapeErr],
						map[bool]string{true: "invalid", false: "valid"}[wantReject], doc, err)
				}
			})
		}
	}
}

// The matrix above asks whether the verdicts AGREE. Agreement alone is blind to
// a change that moves both sides, and one side is a stdlib this package does not
// control — so the count of rejecting escapes is pinned absolutely as well. If
// the scanner started accepting the unknown six, agreement would break; if some
// future stdlib accepted them too, agreement would hold and this would not.
func TestInvariant_JSONEscapeRejectionCountIsAbsolute(t *testing.T) {
	const wantRejected = 6
	s := avro.MustParse(`"string"`)
	rejected := 0
	for _, esc := range jsonEscapeCases {
		if s.DecodeJSON([]byte(`"a`+esc.escape+`b"`), new(string)) != nil {
			rejected++
		}
	}
	if rejected != wantRejected {
		var got []string
		for _, esc := range jsonEscapeCases {
			if s.DecodeJSON([]byte(`"a`+esc.escape+`b"`), new(string)) != nil {
				got = append(got, esc.escape)
			}
		}
		t.Errorf("%d of %d escapes rejected, want %d (%s)", rejected, len(jsonEscapeCases), wantRejected, fmt.Sprint(got))
	}
}

// ---------- json_skip_strict_test.go ----------

// TestMatrix_JSONSkipUnknownFieldRejectsMalformed pins that DecodeJSON
// validates malformed JSON in UNKNOWN (skipped) record fields, matching its
// own value path, Java, fastavro, and encoding/json. The skip path
// (skipValue/skipCompound) was a SECOND hand-rolled parser that delimited but
// did not validate — the framework's "a hand-rolled parser that replaced a
// stdlib parser silently dropped the stdlib's rejections" class: the number
// arm accepted 1.2.3/1e/5., the string arm skipped escapes blindly so \q
// passed, and skipCompound counted only bracket depth so [}]/{"a" 1}/[1,2,]
// balanced. The same bytes in a KNOWN field reject.
func TestMatrix_JSONSkipUnknownFieldRejectsMalformed(t *testing.T) {
	reader := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"known","type":"long"}]}`)

	malformed := []struct{ name, frag string }{
		{"invalid_escape", `"\q"`},
		{"hex_escape", `"\x41"`},
		{"double_dot_number", `1.2.3`},
		{"bare_exponent", `1e`},
		{"trailing_dot", `5.`},
		{"bracket_mismatch_array", `[}]`},
		{"bracket_mismatch_object", `{]}`},
		{"missing_commas_array", `[1 2 3]`},
		{"missing_colon_object", `{"a" 1}`},
		{"trailing_comma_array", `[1,2,]`},
		{"trailing_comma_object", `{"a":1,}`},
		{"leading_comma", `[,1]`},
		{"unquoted_key", `{a:1}`},
		{"double_colon", `{"a"::1}`},
	}

	for _, c := range malformed {
		t.Run(c.name, func(t *testing.T) {
			if json.Valid([]byte(c.frag)) {
				t.Fatalf("test bug: %s is valid JSON", c.frag)
			}
			doc := fmt.Sprintf(`{"known":42,"x":%s}`, c.frag)
			var out map[string]any
			if err := reader.DecodeJSON([]byte(doc), &out); err == nil {
				t.Errorf("skip SILENTLY ACCEPTED malformed JSON %s -> out=%v", c.frag, out)
			}
		})
	}
}

// TestMatrix_JSONSkipUnknownFieldAcceptsValid is the control: well-formed
// JSON in skipped fields must STILL skip cleanly (the strict validator must
// not reject valid input), including nesting, escapes, and whitespace.
func TestMatrix_JSONSkipUnknownFieldAcceptsValid(t *testing.T) {
	reader := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"known","type":"long"}]}`)
	valid := []string{
		`"plain"`, `"with \"escapes\" and é"`, `42`, `-3.14e10`, `0`, `0.5`,
		`true`, `false`, `null`,
		`[]`, `{}`, `[1,2,3]`, `{"a":1,"b":[2,3]}`,
		`{ "a" : 1 , "b" : [ 2 , { "c" : "d" } ] }`,
		`[[[[]]]]`, `{"nested":{"deep":{"v":1}}}`,
	}
	for _, v := range valid {
		t.Run(v, func(t *testing.T) {
			doc := fmt.Sprintf(`{"x":%s,"known":42}`, v)
			var out map[string]any
			if err := reader.DecodeJSON([]byte(doc), &out); err != nil {
				t.Errorf("skip REJECTED valid JSON %s: %v", v, err)
			} else if out["known"] != int64(42) {
				t.Errorf("known field lost after skipping %s: %v", v, out)
			}
		})
	}
}

// TestRegression_JSONSkipDepthBounded confirms the strict (now recursive)
// skip validator keeps the old iterative skipCompound's DoS resistance: a
// pathologically deep skipped value errors rather than overflowing the stack.
func TestRegression_JSONSkipDepthBounded(t *testing.T) {
	reader := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"known","type":"long"}]}`)
	deep := strings.Repeat("[", 100000) + strings.Repeat("]", 100000)
	doc := fmt.Sprintf(`{"known":42,"x":%s}`, deep)
	var out map[string]any
	if err := reader.DecodeJSON([]byte(doc), &out); err == nil {
		t.Fatal("deeply-nested skipped value accepted; expected a depth-limit error")
	}
}

// ---------- reader_grammar_census_test.go ----------

// ---------------------------------------------------------------------------
// Reader-grammar boundary matrices: hand-framed wire for the index and
// block-header productions, driven through every consuming path — natural
// decode, resolved decode, and the resolution SKIP path. The framing matrix
// covers the spec's legal container framings; these cover the index VALUE space
// and the hostile block-header values that no twmb writer produces.
//
// The invariant per cell: a loud error or a value-faithful accept — never a
// silent truncation, silent wrong value, or panic. Accept cells re-encode
// canonically. The skip path may be MORE lenient than the value path only where
// the discarded content does not affect framing, and that leniency must match
// the references: Java's ResolvingDecoder skips an enum via readEnum() =
// readInt() with no symbol check, and fastavro's skip_enum is a bare
// read_long(). Union indices DO affect framing, so the skip path validates them
// exactly like the value path.
// ---------------------------------------------------------------------------

// censusKeep is the trailing "keep" field value every skip cell asserts
// survived the skipped hostile field. zigzag(21) = 0x2A, one wire byte.
const censusKeep = int32(21)

// censusSkipWire frames a writer-record wire: the hostile payload for the
// dropped field, then the keep field.
func censusSkipWire(dropPayload []byte) []byte {
	wire := append([]byte{}, dropPayload...)
	return putZigzag(wire, int64(censusKeep))
}

// censusResolve builds Resolve(writer{drop,keep}, reader{keep}) for a given
// dropped-field schema.
func censusResolve(t *testing.T, dropSchema string) *avro.Schema {
	t.Helper()
	w := avro.MustParse(`{"type":"record","name":"R","fields":[
		{"name":"drop","type":` + dropSchema + `},
		{"name":"keep","type":"int"}]}`)
	r := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"keep","type":"int"}]}`)
	res := mustResolve(t, w, r)
	return res
}

// censusAssertSkip decodes a resolved skip wire and asserts the consistent-
// skip contract: no error, keep intact, nothing left over.
func censusAssertSkip(t *testing.T, res *avro.Schema, wire []byte) {
	t.Helper()
	var got map[string]any
	rest, err := res.Decode(wire, &got)
	if err != nil {
		t.Fatalf("skip decode: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("skip decode left %d bytes", len(rest))
	}
	if got["keep"] != censusKeep {
		t.Fatalf("keep after skip: %#v", got["keep"])
	}
}

// censusAssertSkipErr decodes a resolved skip wire and asserts a loud error
// containing wantErr.
func censusAssertSkipErr(t *testing.T, res *avro.Schema, wire []byte, wantErr string) {
	t.Helper()
	var got map[string]any
	_, err := res.Decode(wire, &got)
	if err == nil {
		t.Fatalf("skip decode accepted hostile wire (got %#v), want error containing %q", got, wantErr)
	}
	if !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("skip decode error %q does not contain %q", err, wantErr)
	}
}

// putVarintWidthOverflow returns a 5-byte varint whose final byte carries
// value bits beyond 32 (0x10 > 0x0f) — the width-overflow form readUvarint
// rejects ("uvarint overflows 32 bits"). Both int-typed productions (enum
// index, union index) read through readVarint, so the same bytes probe both.
func putVarintWidthOverflow() []byte {
	return []byte{0x80, 0x80, 0x80, 0x80, 0x10}
}

// TestMatrix_EnumIndexWireGrammar drives the enum-index production's value space
// through natural decode, resolved-identity decode, and the skip path. The value
// paths validate the index against the symbol table; the SKIP path deliberately
// does not, a discarded enum's index being a self-contained varint that cannot
// affect framing, and neither reference validates it on skip. Width-overflow and
// truncated varints reject on EVERY path: they are varint-grammar errors, not
// value errors.
//
// fastavro calibration (executed, 1.12.2): its VALUE path rejects out-of-range
// but silently ACCEPTS a negative index via Python list wraparound — an
// accidental leniency twmb does not copy, Java rejecting and wraparound being
// silent wrong output.
func TestMatrix_EnumIndexWireGrammar(t *testing.T) {
	const enumSchema = `{"type":"enum","name":"E","symbols":["A","B","C"]}`
	s := avro.MustParse(enumSchema)
	resIdentity, err := avro.Resolve(s, avro.MustParse(enumSchema))
	if err != nil {
		t.Fatalf("Resolve identity: %v", err)
	}
	canonicalLast, err := s.AppendEncode(nil, "C")
	if err != nil {
		t.Fatalf("encode C: %v", err)
	}

	cells := []struct {
		name string
		wire []byte
		// want: expected symbol for accept cells; "" means reject.
		want    string
		wantErr string
		// skipConsistent: the skip path discards the index without
		// validating it, so the cell's reject is value-path-only.
		skipConsistent bool
	}{
		{name: "canonical-last-symbol", wire: putZigzag(nil, 2), want: "C"},
		{name: "index-eq-symbol-count", wire: putZigzag(nil, 3),
			wantErr: "enum index 3 out of range [0, 3)", skipConsistent: true},
		{name: "negative-index", wire: putZigzag(nil, -1),
			wantErr: "out of range", skipConsistent: true},
		{name: "overlong-varint-of-valid", wire: putZigzagOverlong(nil, 2), want: "C"},
		{name: "width-overflow-varint", wire: putVarintWidthOverflow(),
			wantErr: "overflows 32 bits"},
		{name: "truncated-varint", wire: []byte{0x80}, wantErr: "ShortBuffer"},
	}

	resSkip := censusResolve(t, enumSchema)
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			// Natural and resolved-identity value paths must agree.
			for _, path := range []struct {
				label string
				s     *avro.Schema
			}{{"natural", s}, {"resolved-identity", resIdentity}} {
				var got any
				_, err := path.s.Decode(c.wire, &got)
				if c.want != "" {
					if err != nil {
						t.Fatalf("%s decode: %v", path.label, err)
					}
					if got != c.want {
						t.Fatalf("%s decode: got %#v want %q", path.label, got, c.want)
					}
					re, err := path.s.AppendEncode(nil, got)
					if err != nil || !bytes.Equal(re, canonicalLast) {
						t.Fatalf("%s re-encode not canonical: err=%v re=%x want=%x", path.label, err, re, canonicalLast)
					}
					continue
				}
				if err == nil {
					t.Fatalf("%s decode accepted %x (got %#v), want error", path.label, c.wire, got)
				}
				if c.wantErr == "ShortBuffer" {
					var sbe *avro.ShortBufferError
					if !errors.As(err, &sbe) {
						t.Fatalf("%s decode error %q, want ShortBufferError", path.label, err)
					}
				} else if !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("%s decode error %q does not contain %q", path.label, err, c.wantErr)
				}
			}

			// Skip path.
			skipWire := censusSkipWire(c.wire)
			switch {
			case c.want != "" || c.skipConsistent:
				// Valid indices and value-only rejects both skip consistently.
				censusAssertSkip(t, resSkip, skipWire)
			default:
				// Varint-grammar errors stay loud through skip. The
				// "ShortBuffer" sentinel asserts the typed error on the value
				// paths; on the skip path match its rendered message.
				wantErr := c.wantErr
				if wantErr == "ShortBuffer" {
					wantErr = "short buffer"
				}
				censusAssertSkipErr(t, resSkip, skipWire, wantErr)
			}
		})
	}
}

// TestMatrix_UnionIndexWireGrammar drives the union-index production's value
// space through natural decode, resolved decode (writer union resolved
// against a wider reader union, so the resolved deserializer indexes the
// WRITER's branch table), and the skip path. Unlike enum indices, a union
// index selects the branch (de)serializer — it affects framing — so ALL
// three paths validate it identically (skipUnion carries the same
// [0, branches) guard as deserUnion; fastavro's skip_union likewise indexes
// writer_schema[index] and rejects out-of-range).
func TestMatrix_UnionIndexWireGrammar(t *testing.T) {
	const unionSchema = `["int","string","boolean"]`
	s := avro.MustParse(unionSchema)
	// Wider reader: every writer branch resolves, and the resolved decoder's
	// branch table has the writer's arity (3), putting the boundary at 3.
	resWider, err := avro.Resolve(s, avro.MustParse(`["int","string","boolean","long"]`))
	if err != nil {
		t.Fatalf("Resolve wider: %v", err)
	}

	boolPayload := []byte{0x01}
	strPayload := func() []byte {
		p := putZigzag(nil, 2)
		return append(p, "hi"...)
	}()

	canonicalBool, err := s.AppendEncode(nil, true)
	if err != nil {
		t.Fatalf("encode bool: %v", err)
	}
	canonicalStr, err := s.AppendEncode(nil, "hi")
	if err != nil {
		t.Fatalf("encode string: %v", err)
	}

	cells := []struct {
		name      string
		wire      []byte
		want      any    // non-nil for accept cells
		canonical []byte // expected re-encode for accept cells
		wantErr   string
	}{
		{name: "canonical-last-branch", wire: append(putZigzag(nil, 2), boolPayload...),
			want: true, canonical: canonicalBool},
		{name: "index-eq-branch-count", wire: putZigzag(nil, 3),
			wantErr: "union index 3 out of range [0, 3)"},
		{name: "negative-index", wire: putZigzag(nil, -1),
			wantErr: "out of range"},
		{name: "overlong-varint-of-valid", wire: append(putZigzagOverlong(nil, 1), strPayload...),
			want: "hi", canonical: canonicalStr},
		{name: "width-overflow-varint", wire: putVarintWidthOverflow(),
			wantErr: "overflows 32 bits"},
		{name: "truncated-varint", wire: []byte{0x80}, wantErr: ""},
	}

	resSkip := censusResolve(t, unionSchema)
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			for _, path := range []struct {
				label string
				s     *avro.Schema
			}{{"natural", s}, {"resolved-wider", resWider}} {
				var got any
				_, err := path.s.Decode(c.wire, &got)
				if c.want != nil {
					if err != nil {
						t.Fatalf("%s decode: %v", path.label, err)
					}
					if got != c.want {
						t.Fatalf("%s decode: got %#v want %#v", path.label, got, c.want)
					}
					if path.label == "natural" {
						re, err := path.s.AppendEncode(nil, got)
						if err != nil || !bytes.Equal(re, c.canonical) {
							t.Fatalf("re-encode not canonical: err=%v re=%x want=%x", err, re, c.canonical)
						}
					}
					continue
				}
				if err == nil {
					t.Fatalf("%s decode accepted %x (got %#v), want error", path.label, c.wire, got)
				}
				if c.wantErr != "" && !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("%s decode error %q does not contain %q", path.label, err, c.wantErr)
				}
			}

			// Skip path: union indices are validated identically (they select
			// the branch skipper), so verdicts mirror the value path.
			skipWire := censusSkipWire(c.wire)
			if c.want != nil {
				censusAssertSkip(t, resSkip, skipWire)
			} else {
				censusAssertSkipErr(t, resSkip, skipWire, c.wantErr)
			}
		})
	}
}

// TestMatrix_NullUnionIndexWireGrammar is the union-SHAPE arm of the index
// grammar above. That matrix drives one shape — a three-branch union of non-null
// types — and a two-branch null union does not merely take a narrower version of
// the same code: it takes a DIFFERENT decoder, with its own single-byte fast
// path for the two canonical spellings and its own varint fallback. The grammar
// was proved on the general path and the specialization's copy of it was never
// read.
//
// Crossed with the null POSITION, because the specialized decoder does not store
// which index means null; it stores the VALUE branch's index and derives the
// other by subtraction, so a cell that only ever put null first cannot tell a
// correct derivation from one that ignores the position. The cells mirror the
// general matrix's names one for one, and the guard below fails if the two drift
// apart.
func TestMatrix_NullUnionIndexWireGrammar(t *testing.T) {
	t.Parallel()
	shapes := []struct {
		name    string
		schema  string
		nullIdx int64
		valIdx  int64
	}{
		{"null-first", `["null","int"]`, 0, 1},
		{"null-second", `["int","null"]`, 1, 0},
	}
	intPayload := putZigzag(nil, 7)

	for _, sh := range shapes {
		s := avro.MustParse(sh.schema)
		canonicalNull, err := s.AppendEncode(nil, nil)
		if err != nil {
			t.Fatalf("%s: encode null: %v", sh.name, err)
		}
		canonicalVal, err := s.AppendEncode(nil, int32(7))
		if err != nil {
			t.Fatalf("%s: encode int: %v", sh.name, err)
		}

		cells := []struct {
			name      string
			wire      []byte
			want      any
			isNull    bool
			canonical []byte
			wantErr   string
		}{
			{name: "canonical-null", wire: putZigzag(nil, sh.nullIdx),
				isNull: true, canonical: canonicalNull},
			{name: "canonical-value", wire: append(putZigzag(nil, sh.valIdx), intPayload...),
				want: int32(7), canonical: canonicalVal},
			// The two spellings the fast path cannot serve: an overlong
			// varint has no single byte, so both branches fall through to
			// the varint arm — the null branch being the one the value
			// index does not name.
			{name: "overlong-null", wire: putZigzagOverlong(nil, sh.nullIdx),
				isNull: true, canonical: canonicalNull},
			{name: "overlong-value", wire: append(putZigzagOverlong(nil, sh.valIdx), intPayload...),
				want: int32(7), canonical: canonicalVal},
			// A single-byte out-of-range index is rejected by the FAST
			// path, which reports the offending byte; the overlong
			// spelling of the same index reaches the varint arm and
			// reports the decoded index. Two arms, two messages, and the
			// pair is what shows the fast path is not simply falling
			// through to the general one.
			{name: "index-eq-branch-count", wire: putZigzag(nil, 2), wantErr: "invalid null-union index byte"},
			{name: "negative-index", wire: putZigzag(nil, -1), wantErr: "invalid null-union index byte"},
			{name: "overlong-index-eq-branch-count", wire: putZigzagOverlong(nil, 2), wantErr: "union index 2 out of range"},
			{name: "overlong-negative-index", wire: putZigzagOverlong(nil, -1), wantErr: "union index -1 out of range"},
			{name: "width-overflow-varint", wire: putVarintWidthOverflow(), wantErr: ""},
			{name: "truncated-varint", wire: []byte{0x80}, wantErr: ""},
		}

		// Liveness floor: the overlong cells are the reason this matrix
		// exists, and an encoder change that made them canonical would
		// leave the varint arm unread while every assertion still passed.
		overlongFellThrough := 0

		for _, c := range cells {
			t.Run(sh.name+"/"+c.name, func(t *testing.T) {
				var got any
				_, err := s.Decode(c.wire, &got)
				if c.want == nil && !c.isNull {
					if err == nil {
						t.Fatalf("accepted %x (got %#v), want an error", c.wire, got)
					}
					if c.wantErr != "" && !strings.Contains(err.Error(), c.wantErr) {
						t.Fatalf("error %q does not contain %q", err, c.wantErr)
					}
					return
				}
				if err != nil {
					t.Fatalf("decode %x: %v", c.wire, err)
				}
				if c.isNull {
					if got != nil {
						t.Fatalf("decode %x = %#v, want nil (the null branch)", c.wire, got)
					}
				} else if got != c.want {
					t.Fatalf("decode %x = %#v, want %#v", c.wire, got, c.want)
				}
				// A non-canonical spelling must re-encode canonically:
				// the decoder recovered the branch, not merely a value.
				re, err := s.AppendEncode(nil, got)
				if err != nil || !bytes.Equal(re, c.canonical) {
					t.Fatalf("re-encode not canonical: err=%v re=%x want=%x", err, re, c.canonical)
				}
				if strings.HasPrefix(c.name, "overlong-") {
					if len(c.wire) < 2 {
						t.Fatalf("%s is not actually overlong: %x", c.name, c.wire)
					}
					overlongFellThrough++
				}
			})
		}
		if overlongFellThrough != 2 {
			t.Errorf("%s: %d of 2 overlong cells reached the varint arm; the fast path is swallowing them", sh.name, overlongFellThrough)
		}
	}
}

// TestMatrix_SkipHostileBlockFraming drives hostile array/map block-header
// values through the resolution SKIP path (and, where the natural path has
// no cell of its own, through natural decode too). The skip path has its own
// block walker (skipBlocks) with two arms the framing matrix's legal
// variants never stress: the validateByteSize guard on size-prefixed blocks
// (the skip walker jumps by the wire's byte size, so a negative or
// over-buffer size must reject loudly BEFORE the jump) and the shared
// count-vs-buffer / zero-byte-item bounds (checkArrayBlockBounds /
// checkMapBlockBounds, the same helpers the value path uses).
func TestMatrix_SkipHostileBlockFraming(t *testing.T) {
	minInt64 := int64(-1) << 63

	t.Run("array-of-string", func(t *testing.T) {
		res := censusResolve(t, `{"type":"array","items":"string"}`)
		natural := avro.MustParse(`{"type":"array","items":"string"}`)

		cells := []struct {
			name    string
			payload []byte
			wantErr string
			// alsoNatural: drive the bare payload through natural decode
			// too and require the same reject.
			alsoNatural bool
		}{
			{
				// MinInt64's negation is itself: readBlockHeader's double-
				// negative guard must reject before the count is used. The
				// negative-count grammar says a byte size follows, but the
				// reject fires first — the payload deliberately ends here.
				name:        "minint64-count",
				payload:     putZigzag(nil, minInt64),
				wantErr:     "invalid array block count",
				alsoNatural: true,
			},
			{
				// A positive count far beyond the remaining bytes must
				// reject via the buffer-relative bound (string items take
				// >=1 wire byte each), not iterate.
				name:    "count-over-buffer",
				payload: putZigzag(nil, 100000),
				wantErr: "exceeds remaining buffer",
			},
			{
				// Size-prefixed block with a negative byte size: the skip
				// walker would jump by it; validateByteSize rejects first.
				name:    "negative-bytesize",
				payload: putZigzag(putZigzag(nil, -2), -5),
				wantErr: "short buffer for array",
			},
			{
				// Size-prefixed block whose byte size exceeds the buffer.
				name:    "bytesize-over-buffer",
				payload: putZigzag(putZigzag(nil, -1), 100000),
				wantErr: "short buffer for array",
			},
			{
				// A negative string length INSIDE a skipped block: the
				// per-item skip shares readLength with the value path.
				name:    "negative-item-length-in-block",
				payload: putZigzag(putZigzag(nil, 1), -1),
				wantErr: "invalid negative",
			},
		}
		for _, c := range cells {
			t.Run(c.name, func(t *testing.T) {
				censusAssertSkipErr(t, res, censusSkipWire(c.payload), c.wantErr)
				if c.alsoNatural {
					var got any
					if _, err := natural.Decode(c.payload, &got); err == nil ||
						!strings.Contains(err.Error(), c.wantErr) {
						t.Fatalf("natural decode: err=%v, want error containing %q", err, c.wantErr)
					}
				}
			})
		}
	})

	t.Run("map-of-string", func(t *testing.T) {
		res := censusResolve(t, `{"type":"map","values":"string"}`)
		cells := []struct {
			name    string
			payload []byte
			wantErr string
		}{
			{name: "minint64-count", payload: putZigzag(nil, minInt64),
				wantErr: "invalid map block count"},
			{name: "count-over-buffer", payload: putZigzag(nil, 100000),
				wantErr: "exceeds remaining buffer"},
			{name: "negative-bytesize", payload: putZigzag(putZigzag(nil, -2), -5),
				wantErr: "short buffer for map"},
			{name: "bytesize-over-buffer", payload: putZigzag(putZigzag(nil, -1), 100000),
				wantErr: "short buffer for map"},
			{name: "negative-key-length-in-block", payload: putZigzag(putZigzag(nil, 1), -1),
				wantErr: "invalid negative"},
		}
		for _, c := range cells {
			t.Run(c.name, func(t *testing.T) {
				censusAssertSkipErr(t, res, censusSkipWire(c.payload), c.wantErr)
			})
		}
	})

	t.Run("array-of-null-zero-byte-cap", func(t *testing.T) {
		// Zero-byte items make the count the only cost driver; the absolute
		// cap (maxZeroByteItems = 4096) must hold on the skip path exactly
		// as on the value path — a foreign writer's wire, since twmb's own
		// encoder refuses to produce an over-cap array.
		res := censusResolve(t, `{"type":"array","items":"null"}`)
		natural := avro.MustParse(`{"type":"array","items":"null"}`)

		atCap := putZigzag(putZigzag(nil, 4096), 0)
		censusAssertSkip(t, res, censusSkipWire(atCap))
		var got []any
		if _, err := natural.Decode(atCap, &got); err != nil || len(got) != 4096 {
			t.Fatalf("natural at-cap: err=%v len=%d", err, len(got))
		}

		overCap := putZigzag(putZigzag(nil, 4097), 0)
		censusAssertSkipErr(t, res, censusSkipWire(overCap), "zero-byte items exceeds")
		if _, err := natural.Decode(overCap, &got); err == nil ||
			!strings.Contains(err.Error(), "zero-byte items exceeds") {
			t.Fatalf("natural over-cap: err=%v", err)
		}

		// The cap is CUMULATIVE across blocks: 4096 in one block plus 1 in
		// the next must reject (a per-block check would pass both).
		cumulative := putZigzag(putZigzag(putZigzag(nil, 4096), 1), 0)
		censusAssertSkipErr(t, res, censusSkipWire(cumulative), "zero-byte items exceeds")
	})
}

// TestMatrix_SkipByteSizeAuthority pins which authority each path trusts when a
// size-prefixed block's byte size DISAGREES with its items — an inconsistency
// only a corrupt or adversarial writer produces, where the spec's dual framing
// genuinely diverges. twmb's VALUE path decodes |count| items and ignores the
// size, like Java's readArrayStart/arrayNext and fastavro's read path; its SKIP
// path jumps the declared size and ignores the items, like Java's skip
// (BinaryDecoder.java:436-444) and compiled fastavro's (whose PURE-PYTHON
// fallback is instead item-driven, so a no-C-extension install lands where the
// items end).
//
// On a lying-size wire each authority lands at a different offset, so the two
// twmb paths read DIFFERENT trailing fields — pinned so a future "fix" aligning
// one onto the other trips this cell and forces the cross-impl discussion.
func TestMatrix_SkipByteSizeAuthority(t *testing.T) {
	item, err := avro.MustParse(`"string"`).AppendEncode(nil, "x")
	if err != nil {
		t.Fatal(err)
	}
	const keepA, keepB = int32(7), int32(9)

	// Block: count -1, size = len(item)+1+1 — the size annexes the item,
	// the array terminator, and keepA's byte.
	//
	//   [-1][size][item "x"][0x00 terminator][keepA][0x00 terminator][keepB]
	//   value path: 1 item ─┘  end of array ─┘ keep=A
	//   skip path:  ───────── jump size ──────────┘ end of array ─┘ keep=B
	var payload []byte
	payload = putZigzag(payload, -1)
	payload = putZigzag(payload, int64(len(item))+2)
	payload = append(payload, item...)
	payload = append(payload, 0x00)
	payload = putZigzag(payload, int64(keepA))
	payload = append(payload, 0x00)
	payload = putZigzag(payload, int64(keepB))

	w := avro.MustParse(`{"type":"record","name":"R","fields":[
		{"name":"drop","type":{"type":"array","items":"string"}},
		{"name":"keep","type":"int"}]}`)

	// Value path (writer schema): items are the authority → keep = keepA.
	var full map[string]any
	rest, err := w.Decode(payload, &full)
	if err != nil {
		t.Fatalf("value-path decode: %v", err)
	}
	if full["keep"] != keepA {
		t.Fatalf("value path read keep=%#v, want %d (items are the authority)", full["keep"], keepA)
	}
	if want := []any{"x"}; len(full["drop"].([]any)) != 1 || full["drop"].([]any)[0] != want[0] {
		t.Fatalf("value path drop=%#v", full["drop"])
	}
	// The value path stops where the items end; keepB's trailing bytes are
	// honest leftover.
	if len(rest) == 0 {
		t.Fatal("value path consumed the skip-authority tail")
	}

	// Skip path (resolved, drop dropped): the size is the authority → the
	// jump lands past keepA, and the walker reads the SECOND terminator and
	// keepB.
	r := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"keep","type":"int"}]}`)
	res, err := avro.Resolve(w, r)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	var skipped map[string]any
	if _, err := res.Decode(payload, &skipped); err != nil {
		t.Fatalf("skip-path decode: %v", err)
	}
	if skipped["keep"] != keepB {
		t.Fatalf("skip path read keep=%#v, want %d (the declared size is the authority, matching Java's doSkipItems)", skipped["keep"], keepB)
	}
}

// TestMatrix_SkipNestedContainerFraming extends the flat skip-framing net
// (TestMatrix_ForeignFramingThroughSkip) to NESTED containers: every legal
// outer framing of an array<array<map<string>>> the reader drops must be
// consumed exactly, with the trailing field intact. The nested walkers
// (skipArray → skipArray → skipMap → skipString) each re-enter the shared
// block grammar, so a framing mishandled at any depth mis-positions every
// byte after it.
func TestMatrix_SkipNestedContainerFraming(t *testing.T) {
	inner := avro.MustParse(`{"type":"array","items":{"type":"map","values":"string"}}`)
	items := make([][]byte, 3)
	for i, v := range []any{
		[]any{map[string]any{"a": "x"}},
		[]any{map[string]any{"b": "yy"}, map[string]any{}},
		[]any{},
	} {
		b, err := inner.AppendEncode(nil, v)
		if err != nil {
			t.Fatalf("inner encode %d: %v", i, err)
		}
		items[i] = b
	}

	res := censusResolve(t, `{"type":"array","items":{"type":"array","items":{"type":"map","values":"string"}}}`)
	for name, outer := range frameVariants(items) {
		t.Run(name, func(t *testing.T) {
			censusAssertSkip(t, res, censusSkipWire(outer))
		})
	}
}

// TestDifferentialFastavroReaderGrammar executes the fastavro side of the
// census matrices' calibrated claims — each cell pins fastavro's OBSERVED
// verdict (1.12.2) so an upgrade that changes it flips the cell and forces a
// deliberate recalibration rather than a silently rotting comment.
func TestDifferentialFastavroReaderGrammar(t *testing.T) {
	o := startOracle(t)

	const enumSchema = `{"type":"enum","name":"E","symbols":["A","B","C"]}`
	writerRec := `{"type":"record","name":"R","fields":[
		{"name":"drop","type":` + enumSchema + `},
		{"name":"keep","type":"int"}]}`
	readerRec := `{"type":"record","name":"R","fields":[{"name":"keep","type":"int"}]}`

	hexOf := func(b []byte) string {
		const digits = "0123456789abcdef"
		out := make([]byte, 0, len(b)*2)
		for _, x := range b {
			out = append(out, digits[x>>4], digits[x&0xf])
		}
		return string(out)
	}

	t.Run("enum value path rejects out-of-range", func(t *testing.T) {
		resp := o.call(oracleJob{Op: "decode", Schema: json.RawMessage(enumSchema),
			Hex: hexOf(putZigzag(nil, 3))})
		if resp.OK {
			t.Errorf("fastavro accepted enum index 3 of 3 symbols: %v", resp.Values)
		}
	})

	t.Run("enum value path wraps negative index (calibration)", func(t *testing.T) {
		// fastavro's read_enum indexes the Python symbol list, so -1 WRAPS to
		// the last symbol — an accidental leniency twmb does not copy (Java
		// rejects; silent wraparound is wrong output under cross-impl rule 1).
		// Pinned at the observed verdict: if a fastavro release starts
		// rejecting, this cell flips and the census comments recalibrate.
		resp := o.call(oracleJob{Op: "decode", Schema: json.RawMessage(enumSchema),
			Hex: hexOf(putZigzag(nil, -1))})
		if !resp.OK {
			t.Errorf("fastavro now REJECTS a negative enum index (%s) — recalibrate the census's wraparound note", resp.Err)
		} else if len(resp.Values) != 1 || resp.Values[0] != "C" {
			t.Errorf("fastavro negative-index enum decoded %v, want wraparound to \"C\"", resp.Values)
		}
	})

	t.Run("enum skip path discards out-of-range and negative", func(t *testing.T) {
		// fastavro's skip_enum is read_long with no symbol lookup — the
		// reference behavior twmb's skipEnum mirrors.
		for _, idx := range []int64{3, -1} {
			wire := censusSkipWire(putZigzag(nil, idx))
			resp := o.call(oracleJob{Op: "readresolve", Schema: json.RawMessage(writerRec),
				Reader: json.RawMessage(readerRec), Hex: hexOf(wire)})
			if !resp.OK {
				t.Errorf("fastavro skip of enum index %d: %s", idx, resp.Err)
				continue
			}
			m, _ := resp.Values[0].(map[string]any)
			if got, _ := m["keep"].(float64); got != float64(censusKeep) {
				t.Errorf("fastavro keep after skipping enum index %d: %v", idx, resp.Values[0])
			}
		}
	})

	t.Run("union skip path rejects out-of-range", func(t *testing.T) {
		// fastavro's skip_union indexes writer_schema[index] — same loud
		// reject as twmb's skipUnion.
		writerU := `{"type":"record","name":"R","fields":[
			{"name":"drop","type":["int","string","boolean"]},
			{"name":"keep","type":"int"}]}`
		wire := censusSkipWire(putZigzag(nil, 3))
		resp := o.call(oracleJob{Op: "readresolve", Schema: json.RawMessage(writerU),
			Reader: json.RawMessage(readerRec), Hex: hexOf(wire)})
		if resp.OK {
			t.Errorf("fastavro skipped union index 3 of 3 branches: %v", resp.Values)
		}
	})

	t.Run("bytesize-lie: fastavro compiled skip is size-driven (calibration)", func(t *testing.T) {
		// The TestMatrix_SkipByteSizeAuthority wire: fastavro's COMPILED
		// skip (_read.pyx skip_array) jumps the declared size exactly like
		// twmb's skip and Java's doSkipItems, landing on keep=9. (Its
		// pure-Python fallback would read keep=7 — item-driven; this cell
		// pins the compiled implementation every normal install runs.)
		item, _ := avro.MustParse(`"string"`).AppendEncode(nil, "x")
		var payload []byte
		payload = putZigzag(payload, -1)
		payload = putZigzag(payload, int64(len(item))+2)
		payload = append(payload, item...)
		payload = append(payload, 0x00)
		payload = putZigzag(payload, 7)
		payload = append(payload, 0x00)
		payload = putZigzag(payload, 9)
		writerA := `{"type":"record","name":"R","fields":[
			{"name":"drop","type":{"type":"array","items":"string"}},
			{"name":"keep","type":"int"}]}`
		resp := o.call(oracleJob{Op: "readresolve", Schema: json.RawMessage(writerA),
			Reader: json.RawMessage(readerRec), Hex: hexOf(payload)})
		if !resp.OK {
			t.Errorf("fastavro rejected the size-lie wire: %s (recalibrate the authority table)", resp.Err)
			return
		}
		m, _ := resp.Values[0].(map[string]any)
		if got, _ := m["keep"].(float64); got != 9 {
			t.Errorf("fastavro read keep=%v on the size-lie wire, want 9 (size-driven compiled skip, matching Java and twmb); recalibrate the authority table", resp.Values[0])
		}
	})

	t.Run("zero-byte over-cap: fastavro reads uncapped (calibration)", func(t *testing.T) {
		// fastavro has no zero-byte-item cap — it reads 4097 nulls happily.
		// twmb's reject is the documented DOS-resistance divergence
		// (maxZeroByteItems); this cell witnesses the reference's accept so
		// the divergence stays an executed fact, not a stale claim.
		writerN := `{"type":"record","name":"R","fields":[
			{"name":"drop","type":{"type":"array","items":"null"}},
			{"name":"keep","type":"int"}]}`
		wire := censusSkipWire(putZigzag(putZigzag(nil, 4097), 0))
		resp := o.call(oracleJob{Op: "readresolve", Schema: json.RawMessage(writerN),
			Reader: json.RawMessage(readerRec), Hex: hexOf(wire)})
		if !resp.OK {
			t.Errorf("fastavro rejected 4097 zero-byte items (%s) — it may have grown a cap; recalibrate the divergence note", resp.Err)
			return
		}
		m, _ := resp.Values[0].(map[string]any)
		if got, _ := m["keep"].(float64); got != float64(censusKeep) {
			t.Errorf("fastavro keep after 4097-null skip: %v", resp.Values[0])
		}
	})
}

// TestDifferentialFastavroPromotion executes every spec promotion pair, plus the
// two value-level resolution features, against fastavro's resolved read. Per
// cell the writer wire is byte-parity-checked, then the SAME wire is
// resolved-read by both and the values compared. A third leg drives the
// WRITER-SHAPED JSON through the resolved schema's DecodeJSON, which must land
// exactly where the binary resolved read lands — the wire format cannot change
// resolution semantics.
//
// Mantissa-boundary values make the reader-width contract observable: twmb
// converts through the reader's width, matching Java's ResolvingDecoder
// readDouble(), while fastavro (observed 1.12.2) returns the writer's value at
// full precision. That divergence and the bytes→string one — twmb preserves the
// raw bytes in the Go string as Java's Utf8 does, fastavro's strict utf-8 decode
// rejects — are pinned at the observed verdicts so a release that changes either
// forces a deliberate recalibration.
func TestDifferentialFastavroPromotion(t *testing.T) {
	o := startOracle(t)

	const f32b = 1 << 24 // 2^24: +1 is the smallest positive int not exactly float32-representable
	const f64b = 1 << 53 // 2^53: +1 is the smallest positive int not exactly float64-representable

	cells := []struct {
		name    string
		writer  string
		reader  string
		val     any    // writer value, twmb-typed
		valJSON string // the same value in the oracle's JSON transport form
		kind    string // oracle Kind tag ("bytes" for base64 transport)
		wJSON   string // the writer-SHAPED Avro-JSON text of val (bytes are codepoint strings)
		want    any    // twmb resolved-decode expectation (into an any target)
		// wantF64: non-nil for float-reader cells — ALSO decode into a
		// float64 target and assert this value. An any target materializes
		// the reader's float32 and re-rounds at assignment, which would MASK
		// a conversion arm that lost the float32 narrowing; the float64
		// target observes the intermediate directly.
		wantF64 any

		// fastavro's resolved read, exactly one of:
		//   fastWant  — assert values[0] equals this (JSON-decoded form:
		//               numbers arrive as float64, records as map[string]any)
		//   fastBytes — the datum is Python bytes: resolution ACCEPT is
		//               classified via the oracle's "not JSON serializable"
		//               transport error (schemaless_reader returned; a
		//               resolution reject raises before the response dumps)
		//   fastErr   — assert NOT ok and the error contains this (a
		//               calibrated divergence: fastavro rejects wire twmb
		//               and Java accept)
		fastWant  any
		fastBytes bool
		fastErr   string
	}{
		{name: "int-to-long", writer: `"int"`, reader: `"long"`,
			val: int32(math.MinInt32), valJSON: `-2147483648`, wJSON: `-2147483648`,
			want: int64(math.MinInt32), fastWant: float64(math.MinInt32)},
		{name: "int-to-float-mantissa", writer: `"int"`, reader: `"float"`,
			val: int32(f32b + 1), valJSON: `16777217`, wJSON: `16777217`,
			want: float32(f32b), wantF64: float64(float32(f32b + 1)),
			fastWant: float64(f32b + 1)}, // twmb+Java float32-round; fastavro full precision
		{name: "int-to-double", writer: `"int"`, reader: `"double"`,
			val: int32(f32b + 1), valJSON: `16777217`, wJSON: `16777217`,
			want: float64(f32b + 1), fastWant: float64(f32b + 1)}, // every int32 is float64-exact
		{name: "long-to-float-mantissa", writer: `"long"`, reader: `"float"`,
			val: int64(f32b + 1), valJSON: `16777217`, wJSON: `16777217`,
			want: float32(f32b), wantF64: float64(float32(f32b + 1)),
			fastWant: float64(f32b + 1)}, // twmb+Java float32-round; fastavro full precision
		{name: "long-to-double-mantissa", writer: `"long"`, reader: `"double"`,
			val: int64(f64b + 1), valJSON: `9007199254740993`, wJSON: `9007199254740993`,
			// 2^53+1 rounds to 2^53 at the float64 mantissa on BOTH sides
			// (twmb converts; fastavro's raw int collapses identically in
			// the JSON transport), so the cell agrees while still proving
			// the wire carried the unrounded long.
			want: float64(f64b), fastWant: float64(f64b)},
		{name: "float-to-double", writer: `"float"`, reader: `"double"`,
			// 0.1 is not float32-exact: the widened double must be the
			// float32 value 0.10000000149011612, not 0.1 — both impls widen
			// the wire's float32 bit pattern.
			val: float32(0.1), valJSON: `0.1`, wJSON: `0.1`,
			want: float64(float32(0.1)), fastWant: float64(float32(0.1))},
		{name: "string-to-bytes", writer: `"string"`, reader: `"bytes"`,
			val: "h✓i", valJSON: `"h✓i"`, wJSON: `"h✓i"`,
			want: []byte("h✓i"), fastBytes: true},
		{name: "bytes-to-string-valid-utf8", writer: `"bytes"`, reader: `"string"`,
			val: []byte("ok✓"), valJSON: `"b2vinJM="`, kind: "bytes",
			// bytes as Avro-JSON: each BYTE codepoint-mapped (utf-8 e2 9c 93).
			wJSON: `"ok\u00e2\u009c\u0093"`,
			want:  "ok✓", fastWant: "ok✓"},
		{name: "bytes-to-string-invalid-utf8", writer: `"bytes"`, reader: `"string"`,
			val: []byte{0x68, 0xff, 0x69}, valJSON: `"aP9p"`, kind: "bytes", wJSON: `"h\u00ffi"`,
			want:    "h\xffi", // raw bytes preserved, Java Utf8 semantics
			fastErr: "can't decode"},
		{name: "enum-reader-default", writer: `{"type":"enum","name":"E","symbols":["A","B","C"]}`,
			reader: `{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}`,
			val:    "C", valJSON: `"C"`, wJSON: `"C"`,
			want: "A", fastWant: "A"},
		{name: "field-default-fill", writer: `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`,
			reader: `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string","default":"d"}]}`,
			val:    map[string]any{"a": int32(7)}, valJSON: `{"a":7}`, wJSON: `{"a":7}`,
			want: map[string]any{"a": int32(7), "b": "d"}, fastWant: map[string]any{"a": float64(7), "b": "d"}},
	}

	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			w := avro.MustParse(c.writer)
			r := avro.MustParse(c.reader)

			wire, err := w.Encode(c.val)
			if err != nil {
				t.Fatalf("twmb encode: %v", err)
			}

			// Writer-wire byte parity: both impls must produce the same
			// bytes for the writer value, so the resolved reads below
			// consume ONE agreed wire (none of these schemas contain maps,
			// whose entry order would legitimately differ).
			enc := o.call(oracleJob{Op: "encode", Schema: json.RawMessage(c.writer),
				Value: json.RawMessage(c.valJSON), Kind: c.kind})
			if !enc.OK {
				t.Fatalf("fastavro encode: %s", enc.Err)
			}
			if got := hex.EncodeToString(wire); got != enc.Hex {
				t.Fatalf("writer wire mismatch:\n twmb     %s\n fastavro %s", got, enc.Hex)
			}

			// twmb resolved read.
			res, err := avro.Resolve(w, r)
			if err != nil {
				t.Fatalf("twmb Resolve: %v", err)
			}
			var got any
			rest, err := res.Decode(wire, &got)
			if err != nil {
				t.Fatalf("twmb resolved decode: %v", err)
			}
			if len(rest) != 0 {
				t.Fatalf("twmb resolved decode left %d bytes", len(rest))
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("twmb resolved decode: got %T %#v, want %T %#v", got, got, c.want, c.want)
			}
			if c.wantF64 != nil {
				var f64 float64
				if _, err := res.Decode(wire, &f64); err != nil {
					t.Fatalf("twmb resolved decode into float64: %v", err)
				}
				if f64 != c.wantF64.(float64) {
					t.Fatalf("twmb resolved decode into float64: got %v, want %v (reader-width rounding must happen at the conversion, not the target assignment)", f64, c.wantF64)
				}
			}

			// twmb resolved JSON read of the writer-shaped JSON: same
			// landing point as the binary resolved read, both targets.
			var jgot any
			if err := res.DecodeJSON([]byte(c.wJSON), &jgot); err != nil {
				t.Fatalf("twmb resolved DecodeJSON: %v", err)
			}
			if !reflect.DeepEqual(jgot, c.want) {
				t.Fatalf("twmb resolved DecodeJSON: got %T %#v, want %T %#v", jgot, jgot, c.want, c.want)
			}
			if c.wantF64 != nil {
				var f64 float64
				if err := res.DecodeJSON([]byte(c.wJSON), &f64); err != nil {
					t.Fatalf("twmb resolved DecodeJSON into float64: %v", err)
				}
				if f64 != c.wantF64.(float64) {
					t.Fatalf("twmb resolved DecodeJSON into float64: got %v, want %v (reader-width rounding must happen at the conversion, not the target assignment)", f64, c.wantF64)
				}
			}

			// fastavro resolved read of the same wire.
			resp := o.call(oracleJob{Op: "readresolve", Schema: json.RawMessage(c.writer),
				Reader: json.RawMessage(c.reader), Hex: hex.EncodeToString(wire)})
			switch {
			case c.fastErr != "":
				if resp.OK {
					t.Fatalf("fastavro accepted (%v), want reject containing %q — recalibrate the divergence note", resp.Values, c.fastErr)
				}
				if !strings.Contains(resp.Err, c.fastErr) {
					t.Fatalf("fastavro error %q does not contain %q", resp.Err, c.fastErr)
				}
			case c.fastBytes:
				if !resp.OK && !strings.Contains(resp.Err, "not JSON serializable") {
					t.Fatalf("fastavro resolved read failed: %s", resp.Err)
				}
			default:
				if !resp.OK {
					t.Fatalf("fastavro resolved read failed: %s", resp.Err)
				}
				if len(resp.Values) != 1 || !reflect.DeepEqual(resp.Values[0], c.fastWant) {
					t.Fatalf("fastavro resolved value %#v, want %#v", resp.Values, c.fastWant)
				}
			}
		})
	}
}

// ---------- encode_error_identity_census_test.go ----------

// Error-identity contract (doc.go "# Errors"): encode-side USER-VALUE failures
// are errors.As-able to *SemanticError on BOTH wires; decode-side WIRE-CONTENT
// failures are plain errors on both. Three encode-side families deliberately
// agree as PLAIN on both wires instead: a TYPED nil pointer, where both surface
// the shared indirection sentinel; a CustomType.Encode callback error, where the
// user's own error value is returned verbatim; and an invalid UUID string
// against fixed(16)+uuid, where parseUUID's error is bare.
//
// Identity must be asserted at TOP LEVEL: at record positions recordFieldError
// wraps EVERY field error in a *SemanticError, so a record-position probe cannot
// distinguish a wrapped arm from an unwrapped one — which the record-position
// subtest documents.

// encodeIdentityBothWires runs v through Encode and EncodeJSON and
// asserts both reject it with the same *SemanticError verdict.
func encodeIdentityBothWires(t *testing.T, schema string, v any, wantSemantic bool) {
	t.Helper()
	s := mustParse(t, schema)
	_, errB := s.Encode(v)
	_, errJ := s.EncodeJSON(v)
	if errB == nil || errJ == nil {
		t.Fatalf("want both wires to reject; binary=%v json=%v", errB, errJ)
	}
	var seB, seJ *avro.SemanticError
	asB := errors.As(errB, &seB)
	asJ := errors.As(errJ, &seJ)
	if asB != asJ {
		t.Errorf("SemanticError identity diverges across wires: binary=%v json=%v (binary err %q, json err %q)",
			asB, asJ, errB, errJ)
	}
	if asB != wantSemantic {
		t.Errorf("binary SemanticError=%v, want %v (err %q)", asB, wantSemantic, errB)
	}
	if asJ != wantSemantic {
		t.Errorf("json SemanticError=%v, want %v (err %q)", asJ, wantSemantic, errJ)
	}
}

// TestMatrix_UntypedNilEncodeSemanticErrorBothWires pins that an
// UNTYPED nil at top level against a non-nullable schema is an
// encode-side user-value failure carrying *SemanticError identity on
// both wire formats — Encode wraps it at the entry guard (and via the
// union serializer), and EncodeJSON must match. A TYPED nil pointer is
// a different family: both wires surface the plain indirection
// sentinel, and nil against a null schema or a union with a null
// branch succeeds on both wires.
func TestMatrix_UntypedNilEncodeSemanticErrorBothWires(t *testing.T) {
	t.Run("non-nullable primitive", func(t *testing.T) {
		encodeIdentityBothWires(t, `"string"`, nil, true)
	})
	t.Run("union without a null branch", func(t *testing.T) {
		encodeIdentityBothWires(t, `["int","string"]`, nil, true)
	})
	t.Run("control: typed nil pointer is plain on both wires", func(t *testing.T) {
		encodeIdentityBothWires(t, `"string"`, (*string)(nil), false)
	})
	t.Run("control: nil against null schema succeeds on both wires", func(t *testing.T) {
		s := mustParse(t, `"null"`)
		if _, err := s.Encode(nil); err != nil {
			t.Errorf("binary: %v", err)
		}
		out, err := s.EncodeJSON(nil)
		if err != nil || string(out) != "null" {
			t.Errorf("json: out %q err %v, want null", out, err)
		}
	})
	t.Run("control: nil against nullable union succeeds on both wires", func(t *testing.T) {
		s := mustParse(t, `["null","int"]`)
		if _, err := s.Encode(nil); err != nil {
			t.Errorf("binary: %v", err)
		}
		out, err := s.EncodeJSON(nil)
		if err != nil || string(out) != "null" {
			t.Errorf("json: out %q err %v, want null", out, err)
		}
	})
}

// TestMatrix_EncodeErrorIdentityCensus drives one triggering input per
// error-return family at TOP LEVEL through both encoders and asserts
// the family's identity contract. The census exists so a new or edited
// error return in either encoder that drops (or spuriously adds)
// *SemanticError identity for a whole family fails here rather than
// surfacing as a per-wire errors.As difference in user code.
func TestMatrix_EncodeErrorIdentityCensus(t *testing.T) {
	semantic := []struct {
		name   string
		schema string
		v      any
	}{
		{"untyped nil, non-nullable", `"string"`, nil},
		{"untyped nil, no-null union", `["int","string"]`, nil},
		{"type mismatch, string", `"string"`, 42},
		{"type mismatch, int", `"int"`, "hello"},
		{"type mismatch, bytes", `"bytes"`, 42},
		{"type mismatch, boolean", `"boolean"`, "x"},
		{"json.Number content, fractional into int", `"int"`, json.Number("1.5")},
		{"enum unknown symbol", `{"type":"enum","name":"E","symbols":["A","B"]}`, "C"},
		{"enum ordinal out of range", `{"type":"enum","name":"E","symbols":["A","B"]}`, 99},
		{"fixed size mismatch", `{"type":"fixed","name":"F","size":4}`, []byte{1, 2, 3}},
		{"missing defaultless field", `{"type":"record","name":"R","fields":[{"name":"f","type":"int"}]}`, map[string]any{}},
		{"union no branch matched", `["int","string"]`, struct{ X int }{1}},
		// The no-match wrap must be UNCONDITIONAL (as serUnion's is), not
		// inherited from the last branch error's chain: a typed nil's
		// per-branch failure is the PLAIN indirection sentinel, so this
		// row fails if the union dispatcher forwards it bare.
		{"union no branch matched, typed nil", `["int","string"]`, (*string)(nil)},
		{"decimal precision exceeded", `{"type":"bytes","logicalType":"decimal","precision":2,"scale":0}`, big.NewRat(12345, 1)},
		{"decimal non-numeric string", `{"type":"bytes","logicalType":"decimal","precision":4,"scale":0}`, "12x"},
		{"map key not a JSON number", `{"type":"map","values":"int"}`, map[json.Number]int32{json.Number("abc"): 1}},
		{"timestamp-millis out of range", `{"type":"long","logicalType":"timestamp-millis"}`, time.Date(300000000, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"date out of range", `{"type":"int","logicalType":"date"}`, time.Date(6000000, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"time-millis duration out of range", `{"type":"int","logicalType":"time-millis"}`, 700 * time.Hour},
	}
	for _, row := range semantic {
		t.Run("semantic/"+row.name, func(t *testing.T) {
			encodeIdentityBothWires(t, row.schema, row.v, true)
		})
	}

	plain := []struct {
		name   string
		schema string
		v      any
	}{
		{"typed nil pointer", `"string"`, (*string)(nil)},
		{"invalid UUID string into fixed-uuid", `{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`, "zz"},
	}
	for _, row := range plain {
		t.Run("plain/"+row.name, func(t *testing.T) {
			encodeIdentityBothWires(t, row.schema, row.v, false)
		})
	}

	t.Run("plain/CustomType.Encode error returned verbatim", func(t *testing.T) {
		type myStr string
		boom := errors.New("boom")
		ct := avro.CustomType{
			AvroType: "string",
			GoType:   reflect.TypeOf(myStr("")),
			Encode:   func(v any, _ *avro.SchemaNode) (any, error) { return nil, boom },
		}
		s := mustParse(t, `"string"`, avro.WithCustomType(ct))
		_, errB := s.Encode(myStr("x"))
		_, errJ := s.EncodeJSON(myStr("x"))
		if !errors.Is(errB, boom) || !errors.Is(errJ, boom) {
			t.Fatalf("want the callback's own error on both wires; binary=%v json=%v", errB, errJ)
		}
		var se *avro.SemanticError
		if errors.As(errB, &se) || errors.As(errJ, &se) {
			t.Errorf("callback errors are returned verbatim, not wrapped: binary As=%v json As=%v",
				errors.As(errB, &se), errors.As(errJ, &se))
		}
	})

	// Decode-side WIRE-CONTENT failures stay plain on both wire formats:
	// the wire named a symbol/index that the schema does not have.
	t.Run("decode/binary enum ordinal out of range is plain", func(t *testing.T) {
		s := mustParse(t, `{"type":"enum","name":"E","symbols":["A","B"]}`)
		var out any
		_, err := s.Decode([]byte{0xC6, 0x01}, &out) // zigzag varint 99
		var se *avro.SemanticError
		if err == nil || errors.As(err, &se) {
			t.Errorf("want plain wire-content error, got %v (SemanticError=%v)", err, errors.As(err, &se))
		}
	})
	t.Run("decode/json enum unknown symbol is plain", func(t *testing.T) {
		s := mustParse(t, `{"type":"enum","name":"E","symbols":["A","B"]}`)
		var out any
		err := s.DecodeJSON([]byte(`"C"`), &out)
		var se *avro.SemanticError
		if err == nil || errors.As(err, &se) {
			t.Errorf("want plain wire-content error, got %v (SemanticError=%v)", err, errors.As(err, &se))
		}
	})
	t.Run("decode/binary union index out of range is plain", func(t *testing.T) {
		s := mustParse(t, `["int","string"]`)
		var out any
		_, err := s.Decode([]byte{0xC6, 0x01}, &out) // union index 99
		var se *avro.SemanticError
		if err == nil || errors.As(err, &se) {
			t.Errorf("want plain wire-content error, got %v (SemanticError=%v)", err, errors.As(err, &se))
		}
	})

	// Record positions mask family identity: recordFieldError wraps every
	// field error into a *SemanticError with the field path, so even the
	// families that are plain at top level carry SemanticError identity
	// here. This is why every row above asserts at top level.
	t.Run("record position wraps every family", func(t *testing.T) {
		s := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"f","type":"string"}]}`)
		in := map[string]any{"f": nil} // untyped nil field value
		_, errB := s.Encode(in)
		_, errJ := s.EncodeJSON(in)
		var seB, seJ *avro.SemanticError
		if !errors.As(errB, &seB) || seB.Field != "f" {
			t.Errorf("binary: want SemanticError with Field=f, got %v", errB)
		}
		if !errors.As(errJ, &seJ) || seJ.Field != "f" {
			t.Errorf("json: want SemanticError with Field=f, got %v", errJ)
		}
	})
}

// ---------- cap_compliance_table_test.go ----------

// ---------------------------------------------------------------------------
// The DoS-cap PRODUCER-COMPLIANCE table.
//
// The rule: every reader-side cap needs a producer-side compliance check, with
// one documented exception. What the table adds is that the rule is asked of
// every cap and of every CARRIER, because a cap is not one question — the same
// bound is reachable through a wire VALUE and through a schema DEFAULT, and
// defaults are pre-encoded by a separate walk sharing no code with the
// serializers. Four rounds in a row found the value-carrier face of a cap, fixed
// it, and left the default-carrier face for the next round.
//
// Three things make this a table rather than a pile of pins:
//
//   - Every cap carries an APPLICABILITY verdict with its reason. Not every cap
//     bounds a wire value: one bounds a Go target type, another a pre-allocation
//     hint that refuses nothing. "Not applicable, because X" is a real cell.
//   - Expectations come from the RULE, not from what the code does today. The
//     invariant is PER-WIRE self-consistency — if Encode on a wire succeeds,
//     Decode on that same wire must — and no more. It is deliberately NOT "the
//     cap rejects on every wire": maxZeroByteItems is binary-only, because JSON
//     text cannot amplify, and demanding uniform rejection would encode a false
//     invariant and fail a correct implementation.
//   - A cap added later lands with no row and FAILS until someone classifies it
//     (TestInvariant_EveryCapIsClassified). Without that the table is a snapshot
//     of today's caps and the next one repeats the last four rounds.
// ---------------------------------------------------------------------------

type capApplicability string

const (
	// capWireValue: the cap refuses wire content, so both carriers must comply.
	capWireValue capApplicability = "wire-value"
	// capReaderOnly: reader-only BY DESIGN — the documented exception. The
	// table asserts the exception rather than closing it.
	capReaderOnly capApplicability = "reader-only-by-design"
	// capNotApplicable: the cap has no wire-value face at all.
	capNotApplicable capApplicability = "not-applicable"
	// capUnruled: nothing settles this cell yet. Reported, never guessed.
	capUnruled capApplicability = "UNRULED"
)

// capDriver builds an over-cap and an at-cap case for one cap at one nesting,
// for one carrier. A nil driver means the row is classified but not driven.
type capDriver struct {
	// inner is the leaf schema the cap lives on.
	inner string
	// overCapDefault / atCapDefault render the leaf's JSON default literal.
	overCapDefault func() string
	atCapDefault   func() string
	// overCapValue / atCapValue render the leaf's Go value.
	overCapValue func() any
	atCapValue   func() any
}

type capRow struct {
	// konst is the identifier in the package sources. The completeness guard
	// matches on this, so a renamed constant fails loudly rather than silently
	// leaving the cap unwatched.
	konst         string
	applicability capApplicability
	reason        string
	driver        *capDriver
}

func codepointDefault(b []byte) string {
	var sb strings.Builder
	sb.Grow(len(b)*6 + 2)
	sb.WriteByte('"')
	for _, c := range b {
		fmt.Fprintf(&sb, "\\u%04x", c)
	}
	sb.WriteByte('"')
	return sb.String()
}

func capRows() []capRow {
	const zeroByteCap = 4 << 10
	const unscaledCap = 32 << 10
	raw := func(n int) []byte { return bytes.Repeat([]byte{0x01}, n) }
	nulls := func(n int) string {
		return "[" + strings.TrimSuffix(strings.Repeat("null,", n), ",") + "]"
	}
	anyNulls := func(n int) any {
		out := make([]any, n)
		return out
	}

	return []capRow{
		{
			konst:         "maxDecimalUnscaledBytes",
			applicability: capWireValue,
			reason:        "bounds the unscaled byte length the decoder will base-convert; refuses wire content, so both carriers must comply",
			driver: &capDriver{
				inner:          `{"type":"bytes","logicalType":"decimal","precision":65536,"scale":0}`,
				overCapDefault: func() string { return codepointDefault(raw(unscaledCap + 1)) },
				atCapDefault:   func() string { return codepointDefault(raw(unscaledCap)) },
				overCapValue:   func() any { return raw(unscaledCap + 1) },
				atCapValue:     func() any { return raw(unscaledCap) },
			},
		},
		{
			konst:         "maxZeroByteItems",
			applicability: capWireValue,
			reason:        "bounds the cumulative count of zero-byte array items the decoder accepts; refuses wire content, so both carriers must comply",
			driver: &capDriver{
				inner:          `{"type":"array","items":"null"}`,
				overCapDefault: func() string { return nulls(zeroByteCap + 1) },
				atCapDefault:   func() string { return nulls(zeroByteCap) },
				overCapValue:   func() any { return anyNulls(zeroByteCap + 1) },
				atCapValue:     func() any { return anyNulls(zeroByteCap) },
			},
		},
		{
			konst:         "maxOCFZeroByteSlack",
			applicability: capWireValue,
			reason: "bounds consecutive zero-byte datums per OCF block; the WRITER enforces the same bound through its flush discipline, " +
				"so a default filling a zero-byte field reaches it through the ordinary encode path the OCF wire column already drives",
		},
		{
			konst:         "decimalScaleLimit",
			applicability: capWireValue,
			reason: "bounds decimal scale/precision at BOTH parse and decode, and a default's scale is the schema's declared scale — " +
				"so the producer-side check is the parse-time one, and no default can carry a scale the parse did not already admit",
		},
		{
			konst:         "maxRatInputLen",
			applicability: capWireValue,
			reason: "bounds the string->big.Rat parse; the producer side is boundedRatFromString itself, which every encode-side " +
				"numeric-text carrier routes through. A stored decimal default is []byte, not text, so it does not reach this parse",
		},
		{
			konst:         "maxDepth",
			applicability: capWireValue,
			reason: "bounds recursion on every walk; the default pre-encode carries its own depth counter against this same constant " +
				"(encodeDefaultDepth), which is what makes a self-referential default an errTooDeep parse error rather than a stack overflow",
		},
		{
			konst:         "maxMapPreAllocSize",
			applicability: capNotApplicable,
			reason: "NOT a reject: it caps the size hint passed to reflect.MakeMapWithSize, and larger maps still grow dynamically. " +
				"No wire is refused, so there is nothing for a producer to comply with",
		},
		{
			konst:         "maxIndirectDepth",
			applicability: capNotApplicable,
			reason: "bounds Go POINTER depth on a target type, not wire content. It has no default-carrier face at all — a schema " +
				"default is Avro-native data and names no Go type",
		},
		{
			konst:         "ocfMetadataSafetyLimit",
			applicability: capWireValue,
			reason: "bounds OCF user-metadata size, and the writer carries the matching producer check. No default-carrier face: " +
				"container metadata is not schema-default data",
		},
		{
			konst:         "ocfSchemaSafetyLimit",
			applicability: capWireValue,
			reason: "bounds the OCF header's avro.schema size, writer-checked. No default-carrier face, for the same reason as the " +
				"metadata limit",
		},
		{
			konst:         "maxSchemaJSONNodes",
			applicability: capWireValue,
			reason: "the node-count half of the schema-tree walk budget, paired with maxSchemaJSONBytes — producer-side by " +
				"construction for the same reason: it bounds what the walk EMITS",
		},
		{
			konst:         "maxLaxIntDataLen",
			applicability: capWireValue,
			reason: "bounds lax integer TEXT at parse; same shape as maxInt64LenientLen and maxParseFloatLen — the producer side " +
				"is bounded integer formatting",
		},
		{
			konst:         "maxParseErrorLen",
			applicability: capNotApplicable,
			reason: "bounds an ERROR MESSAGE's length (the error-echo amplification family), not accepted input. It refuses no " +
				"wire, so the producer-compliance rule has nothing to say about it",
		},
		{
			konst:         "maxConsecutiveEmptyReads",
			applicability: capNotApplicable,
			reason: "bounds a misbehaving io.Reader's empty-read livelock, a trust-boundary guard on the CALLER's reader rather " +
				"than on wire content; there is no producer of it to comply",
		},
		{
			konst:         "defaultMaxDecompressedBytes",
			applicability: capReaderOnly,
			reason: "the OCF block-size pair is READER-ONLY BY DESIGN — the documented exception to the producer-compliance rule. " +
				"Producer enforcement was implemented once and REVERTED: it traps data at flush and leaves an unclosable " +
				"compressed-size residual. The table asserts the exception; it must not be closed",
		},
		{
			konst:         "defaultMaxBlockBytes",
			applicability: capReaderOnly,
			reason: "the COMPRESSED half of the same reader-only pair. It was an inline literal until this table asked for it: a " +
				"bound with no name cannot be classified by a guard keyed on names, so the row recorded a hole rather than " +
				"covering one. Naming it also made ocfEagerBlockAllocLimit DERIVE from it instead of restating the same number " +
				"in a second spelling under a comment asserting the two were equal",
		},

		// Below: bounds the #11 entry does not enumerate. The completeness
		// guard surfaced them, which is the guard doing its job — the written
		// list was never the set of caps.
		{
			konst:         "maxSchemaJSONBytes",
			applicability: capWireValue,
			reason: "bounds what the schema-tree walk EMITS. It is itself the producer-side check for the schema-TEXT channel, " +
				"paired with maxSchemaJSONDepth on the reading side; the rule is satisfied by construction here rather than by a separate charge",
		},
		{
			konst:         "maxSchemaJSONDepth",
			applicability: capWireValue,
			reason: "bounds schema-JSON nesting at parse; its producer counterpart is the walk depth budget that governs emission, " +
				"so a tree this package renders cannot exceed what it will re-read",
		},
		{
			konst:         "maxParseFloatLen",
			applicability: capWireValue,
			reason: "bounds float TEXT at parse; the producer counterpart is strconv formatting, whose output is bounded by the " +
				"float format itself, so no value this package emits can exceed it",
		},
		{
			konst:         "maxInt64LenientLen",
			applicability: capWireValue,
			reason: "bounds lenient int64 TEXT at parse; same shape as maxParseFloatLen — the producer side is bounded integer " +
				"formatting, so the text this package emits is always inside it",
		},
		{
			konst:         "maxFixedLogicalLen",
			applicability: capNotApplicable,
			reason: "NOT a reject: it bounds a parse-time PROBE BUFFER so a hostile fixed size cannot drive a large allocation. " +
				"No wire is refused, so there is nothing for a producer to comply with",
		},
		{
			konst:         "defaultBlockBytes",
			applicability: capNotApplicable,
			reason: "the OCF writer's FLUSH THRESHOLD, not a bound on accepted input — it decides when a block is cut, and refuses " +
				"nothing. Distinct from the reader-only block-size pair despite the similar name",
		},
	}
}

// nestings are the shapes the leaf can sit at. This is the axis the
// default-carrier bug turned on: the charge was asked of the FIELD's kind, so
// every nested leaf went unasked, and no existing generator varied it.
var capNestings = []struct {
	name string
	// arm is the encodeDefaultDepth case this nesting drives, or "" for the
	// flat shape which drives no composite arm. The completeness guard matches
	// on it, so a composite arm with no nesting here fails loudly.
	arm string
	// field renders the record field holding the leaf, given the leaf schema
	// and a leaf default literal (empty for the value carrier).
	field func(inner, deflt string) string
	// value renders the Go value for the leaf at this nesting.
	value func(leaf any) any
}{
	{"flat", "", func(inner, d string) string { return fieldOf("d", inner, d) },
		func(leaf any) any { return leaf }},
	{"in-record", "record", func(inner, d string) string {
		// The default has to sit on the OUTER field: a record-typed field
		// defaults as a whole object. Hanging it on the inner field instead
		// leaves the outer one defaultless, and the cell then measures
		// "missing key" rather than the cap.
		return fieldOf("d", `{"type":"record","name":"Inner","fields":[`+fieldOf("x", inner, "")+`]}`, objKeyOf("x", d))
	}, func(leaf any) any { return map[string]any{"x": leaf} }},
	{"in-array", "array", func(inner, d string) string {
		return fieldOf("d", `{"type":"array","items":`+inner+`}`, arrOf(d))
	}, func(leaf any) any { return []any{leaf} }},
	{"in-map", "map", func(inner, d string) string {
		return fieldOf("d", `{"type":"map","values":`+inner+`}`, objOf(d))
	}, func(leaf any) any { return map[string]any{"k": leaf} }},
	// A union default corresponds to the FIRST branch, so the leaf's own
	// literal is the union's literal. This arm hid every cap until it was
	// driven: it selects a branch by trying each and keeping the first that
	// encodes, and it charged nothing at all.
	{"in-union", "union", func(inner, d string) string {
		return fieldOf("d", `[`+inner+`,"null"]`, d)
	}, func(leaf any) any { return leaf }},
}

func fieldOf(name, typ, deflt string) string {
	if deflt == "" {
		return fmt.Sprintf(`{"name":%q,"type":%s}`, name, typ)
	}
	return fmt.Sprintf(`{"name":%q,"type":%s,"default":%s}`, name, typ, deflt)
}
func arrOf(d string) string {
	if d == "" {
		return ""
	}
	return "[" + d + "]"
}
func objKeyOf(k, d string) string {
	if d == "" {
		return ""
	}
	return `{"` + k + `":` + d + `}`
}
func objOf(d string) string {
	if d == "" {
		return ""
	}
	return `{"k":` + d + `}`
}

// TestMatrix_CapProducerCompliance drives cap x carrier x nesting x wire.
func TestMatrix_CapProducerCompliance(t *testing.T) {
	var driven, unruled int
	for _, row := range capRows() {
		if row.applicability == capUnruled {
			t.Logf("UNRULED: %s — %s", row.konst, row.reason)
			unruled++
			continue
		}
		if row.driver == nil {
			t.Logf("classified, not driven: %s [%s] — %s", row.konst, row.applicability, row.reason)
			continue
		}
		d := row.driver
		for _, nest := range capNestings {
			for _, carrier := range []string{"value", "default"} {
				for _, over := range []bool{false, true} {
					label := fmt.Sprintf("%s/%s/%s/%s", row.konst, carrier, nest.name, map[bool]string{false: "at-cap", true: "over-cap"}[over])
					t.Run(label, func(t *testing.T) {
						driven++
						var schema string
						var val any
						if carrier == "default" {
							lit := d.atCapDefault
							if over {
								lit = d.overCapDefault
							}
							schema = recordOf(nest.field(d.inner, lit()))
							val = map[string]any{"keep": int32(7)}
						} else {
							schema = recordOf(nest.field(d.inner, ""))
							leaf := d.atCapValue
							if over {
								leaf = d.overCapValue
							}
							val = map[string]any{"d": nest.value(leaf()), "keep": int32(7)}
						}
						s, err := avro.Parse(schema)
						if err != nil {
							// The rule forbids a PARSE reject for an
							// unwritable default: a reader that drops the
							// field must still be able to read its data.
							t.Fatalf("schema must parse; the bound belongs on encode: %v", err)
						}
						capCheckWires(t, s, val, over)
					})
				}
			}
		}
	}
	t.Logf("driven cells: %d, unruled rows: %d", driven, unruled)
}

func recordOf(field string) string {
	return `{"type":"record","name":"R","fields":[` + field + `,{"name":"keep","type":"int"}]}`
}

// capCheckWires asserts PER-WIRE self-consistency: on each wire independently,
// if Encode succeeded then Decode of that same output must succeed. It does NOT
// require the cap to reject on every wire — some caps are binary-only because
// the JSON representation cannot amplify, and demanding uniform rejection would
// encode an invariant the rule does not state.
func capCheckWires(t *testing.T, s *avro.Schema, val any, over bool) {
	t.Helper()
	if b, err := s.Encode(val); err == nil {
		var sink any
		if _, derr := s.Decode(b, &sink); derr != nil {
			t.Errorf("binary: Encode produced %d bytes its own Decode refuses: %v", len(b), derr)
		}
	}
	if j, err := s.EncodeJSON(val); err == nil {
		var sink any
		if derr := s.DecodeJSON(j, &sink); derr != nil {
			t.Errorf("json: EncodeJSON produced %d bytes its own DecodeJSON refuses: %v", len(j), derr)
		}
	}
	if so, err := s.AppendSingleObject(nil, val); err == nil {
		var sink any
		if _, derr := s.DecodeSingleObject(so, &sink); derr != nil {
			t.Errorf("single-object: Encode produced %d bytes its own Decode refuses: %v", len(so), derr)
		}
	}
	var buf bytes.Buffer
	if w, err := ocf.NewWriter(&buf, s); err == nil {
		if err := w.Encode(val); err == nil && w.Close() == nil {
			size := buf.Len()
			if r, rerr := ocf.NewReader(&buf); rerr != nil {
				t.Errorf("ocf: writer produced a %d-byte file NewReader refuses: %v", size, rerr)
			} else {
				var sink any
				if derr := r.Decode(&sink); derr != nil {
					t.Errorf("ocf: writer produced a %d-byte file the reader refuses: %v", size, derr)
				}
				r.Close()
			}
		}
	}
	// At or under the bound the value must actually make it onto the wire, or
	// the check moved the boundary inward and the cells above pass vacuously.
	if !over {
		if _, err := s.Encode(val); err != nil {
			t.Fatalf("at-cap must still encode: %v", err)
		}
	}
}

// TestInvariant_OCFBlockCapsStayReaderOnly asserts the EXCEPTION as a cell. The
// block-size pair is reader-only BY DESIGN, and the type system already says so:
// WithMaxBlockBytes is a ReaderOpt, so it cannot even be handed to NewWriter —
// a stronger statement of the exception than any behavioral probe, since producer
// enforcement is not merely absent but unexpressible. It was implemented once and
// reverted (it traps data at flush and leaves an unclosable compressed-size
// residual), so this cell exists to fail if a later round re-adds it.
func TestInvariant_OCFBlockCapsStayReaderOnly(t *testing.T) {
	var _ ocf.ReaderOpt = ocf.WithMaxBlockBytes(1 << 10)
	var _ ocf.ReaderOpt = ocf.WithMaxDecompressedBlockBytes(1 << 10)

	// And behaviorally: a writer given a datum far larger than any reader
	// bound still WRITES it, because the bound governs reading.
	s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"b","type":"bytes"}]}`)
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, s)
	if err := w.Encode(map[string]any{"b": bytes.Repeat([]byte{0x01}, 1<<20)}); err != nil {
		t.Fatalf("the block-size cap is reader-only by design; the writer must not enforce it: %v", err)
	}
	mustClose(t, w)
	// The matching reader bound then refuses that file — the exception is a
	// working reader-side bound, not an absent one.
	if _, err := ocf.NewReader(bytes.NewReader(buf.Bytes()), ocf.WithMaxBlockBytes(1<<10)); err == nil {
		var sink any
		r, _ := ocf.NewReader(bytes.NewReader(buf.Bytes()), ocf.WithMaxBlockBytes(1<<10))
		if r != nil && r.Decode(&sink) == nil {
			t.Error("the reader-side block bound accepted a block far above it; the exception rests on that bound working")
		}
	}
}

// capNamePattern matches the identifiers this family names its bounds with.
// It is applied only to CONST declarations — an assignment like
// `maxBlockBytes = o.n` inside a constructor is a local binding of a bound
// declared elsewhere, not a new bound, and matching it would make the guard
// demand rows for things that are not caps.
var capNamePattern = regexp.MustCompile(`^(max[A-Z]\w*|default[A-Z]\w*Bytes|\w*SafetyLimit|\w*ScaleLimit)$`)

var capConstDecl = regexp.MustCompile(`^\s*(?:const\s+)?([A-Za-z_]\w*)\s+=`)

// scanCapConsts returns every cap-shaped CONSTANT declared in path.
func scanCapConsts(src string) []string {
	var out []string
	inBlock := false
	for _, line := range strings.Split(src, "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trimmed, "const ("):
			inBlock = true
			continue
		case inBlock && trimmed == ")":
			inBlock = false
			continue
		}
		isConst := strings.HasPrefix(trimmed, "const ") || inBlock
		if !isConst {
			continue
		}
		m := capConstDecl.FindStringSubmatch(line)
		if m == nil || !capNamePattern.MatchString(m[1]) {
			continue
		}
		out = append(out, m[1])
	}
	return out
}

// capNotABound lists identifiers the pattern catches that are not reader-side
// DoS caps, each with the reason. An entry here is a classification too: it
// says someone looked.
var capNotABound = map[string]string{
	"maxVarintLen":  "an encoding width, not a bound on accepted input",
	"maxVarlongLen": "an encoding width, not a bound on accepted input",
	// Saturating a magnitude REFUSES nothing: a schema declaring a fixed
	// larger than this still parses, encodes and decodes exactly as before.
	// The ceiling only keeps arithmetic on that magnitude inside the integer
	// range, so there is no over-cap input to build and no producer-side
	// compliance to check. Its own guards are the ones that watch the
	// arithmetic (magnitude_arithmetic_test.go).
	"maxSchemaMagnitude": "an arithmetic ceiling, not a bound on accepted input — nothing is refused for exceeding it",
	// Exhausting the walk allowance REFUSES nothing either: the walk stops
	// deriving a tighter per-element minimum and falls back to the same
	// conservative stand-in it uses for a cycle, making the block bound LOOSER. A
	// schema past the allowance parses, encodes and decodes exactly as before, so
	// there is no over-allowance input to build and no producer-side compliance to
	// check. Two guards watch it instead: agreement with an un-memoized walk,
	// where an allowance low enough to change an answer would surface, and the
	// width cells, where an allowance counting the wrong UNIT would — this one is
	// charged per child examined, since entering a node costs its child count.
	"maxMinBytesWork": "a walk allowance that loosens a derived bound, not a bound on accepted input — nothing is refused for exceeding it",
}

// TestInvariant_EveryCapIsClassified is the completeness half: a cap added
// later lands with no row and fails here until someone classifies it. Without
// this the table is a snapshot, and the next cap repeats the rounds that found
// this one.
func TestInvariant_EveryCapIsClassified(t *testing.T) {
	classified := map[string]bool{}
	for _, r := range capRows() {
		classified[r.konst] = true
	}
	roots := []string{".", "ocf"}
	found := map[string]string{}
	for _, root := range roots {
		entries, err := os.ReadDir(root)
		if err != nil {
			t.Fatalf("read %s: %v", root, err)
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(root, e.Name())
			src, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			for _, name := range scanCapConsts(string(src)) {
				found[name] = path
			}
		}
	}
	if len(found) == 0 {
		t.Fatal("the scan matched no cap constants at all — the pattern has rotted and this guard is watching nothing")
	}
	for name, path := range found {
		if classified[name] {
			continue
		}
		if why, ok := capNotABound[name]; ok {
			t.Logf("not a bound: %s (%s) — %s", name, path, why)
			continue
		}
		t.Errorf("cap %s (%s) has no row in the producer-compliance table.\n"+
			"  Classify it: wire-value (and drive both carriers), reader-only-by-design (assert the exception),\n"+
			"  not-applicable (with the reason), or UNRULED — or add it to capNotABound if it is not a reader-side bound.",
			name, path)
	}
	for name := range classified {
		if _, ok := found[name]; !ok {
			t.Errorf("row %s names no constant in the sources — it was renamed or deleted, and this row now watches nothing", name)
		}
	}
}

// scanDefaultWalkCompositeArms returns the kinds encodeDefaultDepth RECURSES
// through — the arms that can nest a cap's carrier below the field's own node.
// It is derived from the source rather than listed, because a hand-listed axis
// is what let the union arm hide: it was a composite the table never drove, and
// a longer hand list would only move the blind spot to the next arm.
func scanDefaultWalkCompositeArms(src string) []string {
	start := strings.Index(src, "func encodeDefaultDepth(")
	if start < 0 {
		return nil
	}
	body := src[start:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}
	caseLine := regexp.MustCompile(`^\tcase ("[a-z]+"(?:, "[a-z]+")*):`)
	quoted := regexp.MustCompile(`"([a-z]+)"`)
	var out []string
	var current []string
	recursed := false
	flush := func() {
		if recursed {
			out = append(out, current...)
		}
	}
	for _, line := range strings.Split(body, "\n") {
		if m := caseLine.FindStringSubmatch(line); m != nil {
			flush()
			current, recursed = nil, false
			for _, q := range quoted.FindAllStringSubmatch(m[1], -1) {
				current = append(current, q[1])
			}
			continue
		}
		if current != nil && strings.Contains(line, "encodeDefaultDepth(") {
			recursed = true
		}
	}
	flush()
	return out
}

// TestInvariant_EveryDefaultWalkArmHasANestingCell is the completeness half of
// the NESTING axis, mirroring the cap classifier: every composite arm of the
// default walk must be driven by a nesting, or land with no cell and FAIL. The
// union arm is why this exists — it recursed like the other three, behaved
// differently from all of them (it selects a branch by trying each), and was
// simply absent from a hand-written axis, so the table stayed green over an open
// hole. An axis that omits the shape a bug lives in is worse than no axis,
// because it reads as coverage.
func TestInvariant_EveryDefaultWalkArmHasANestingCell(t *testing.T) {
	src, err := os.ReadFile("resolve.go")
	if err != nil {
		t.Fatalf("read resolve.go: %v", err)
	}
	arms := scanDefaultWalkCompositeArms(string(src))
	if len(arms) == 0 {
		t.Fatal("the scan found no recursing arms at all — encodeDefaultDepth moved or was renamed, and this guard is watching nothing")
	}
	driven := map[string]bool{}
	for _, n := range capNestings {
		if n.arm != "" {
			driven[n.arm] = true
		}
	}
	for _, arm := range arms {
		if !driven[arm] {
			t.Errorf("encodeDefaultDepth recurses through the %q arm, but no nesting in capNestings drives it.\n"+
				"  A composite arm can nest a cap's carrier below the field's own node, which is exactly where the\n"+
				"  charge is asked. Add a nesting with arm: %q, or this axis reads as coverage it does not have.", arm, arm)
		}
	}
	for arm := range driven {
		if !slices.Contains(arms, arm) {
			t.Errorf("nesting drives the %q arm, but encodeDefaultDepth no longer recurses through it — the cell is stale", arm)
		}
	}
	t.Logf("composite arms derived from source: %v", arms)
}

// ---------- error_bound_test.go ----------

// Tier-2 error-message DoS bound (CORRECTNESS_PLAN.md DoS gap). Every
// fmt.Errorf("...%q...", x) interpolating wire- or schema-controlled content is a
// 1:1 amplification vector: a hostile N-byte input rejected into an N-byte error
// message floods logging pipelines, RPC error channels, metric labels and traces.
// The trunc*ForError helpers exist to cap that echo, but the recurring regression
// is a call site that forgets to use them. The invariant pinned here: the error
// from a rejected hostile input is bounded by a small constant, INDEPENDENT of
// input size, so a call site that drops its trunc wrapper trips the cap.
func TestErrorMessageBounded(t *testing.T) {
	const hostileLen = 1 << 20 // 1 MiB
	// Legit messages from these paths are ~100 bytes (an ~80-char truncated
	// echo plus template text). The cap is far below the 1 MiB input so any
	// amplification regression trips it, and far above any legitimate message.
	const cap = 4096

	cases := []struct {
		name    string
		trigger func() error
	}{
		{
			// Schema parse echoing an unknown named-type reference.
			name: "parse unknown type reference",
			trigger: func() error {
				huge := strings.Repeat("A", hostileLen) // valid name chars, unknown type
				_, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":"` + huge + `"}]}`)
				return err
			},
		},
		{
			// JSON decode echoing an unknown enum symbol.
			name: "json unknown enum symbol",
			trigger: func() error {
				s := avro.MustParse(`{"type":"enum","name":"E","symbols":["A","B"]}`)
				huge := strings.Repeat("Z", hostileLen)
				var out string
				return s.DecodeJSON([]byte(`"`+huge+`"`), &out)
			},
		},
		{
			// JSON decode echoing an out-of-range integer literal.
			name: "json integer overflow",
			trigger: func() error {
				s := avro.MustParse(`"int"`)
				huge := strings.Repeat("9", hostileLen)
				var out int32
				return s.DecodeJSON([]byte(huge), &out)
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.trigger()
			if err == nil {
				t.Fatalf("expected an error for the hostile input")
			}
			if n := len(err.Error()); n > cap {
				t.Errorf("error message is %d bytes for a %d-byte hostile input (cap %d): the echo is unbounded\nfirst 200: %.200s",
					n, hostileLen, cap, err.Error())
			}
		})
	}
}

// A deeply nested schema that the builder rejects (past the recursion
// limit) must not produce an unbounded error message: each nesting level
// wraps the inner error, so without a cap a 1500-deep array yields a
// ~15 KB message from a ~37 KB input — the same amplification the
// per-value trunc helpers prevent, accumulated over the wrap chain.
func TestRegression_DeepSchemaErrorBounded(t *testing.T) {
	for _, depth := range []int{1100, 1500, 3000} {
		schema := strings.Repeat(`{"type":"array","items":`, depth) + `"int"` + strings.Repeat(`}`, depth)
		_, err := avro.Parse(schema)
		if err == nil {
			t.Fatalf("depth %d: expected rejection", depth)
		}
		if len(err.Error()) > 4096 {
			t.Errorf("depth %d: error %d bytes exceeds 4096 bound", depth, len(err.Error()))
		}
	}
}

// Parse is O(n) in schema size, not O(depth*size): a valid deeply-nested
// schema (under the build's maxDepth) must parse in time linear in its
// bytes. The former json.Unmarshaler front-end re-scanned each node's
// full subtree (O(n^2)); a 999-deep array took ~0.4s, and Parse also fed
// Canonical() whose nested MarshalJSON re-copied each subtree (a second
// O(n^2)). Both are now single-pass.
func TestRegression_DeepValidSchemaParsesLinear(t *testing.T) {
	deep := strings.Repeat(`{"type":"array","items":`, 900) + `"int"` + strings.Repeat(`}`, 900)
	t0 := time.Now()
	s, err := avro.Parse(deep)
	if err != nil {
		t.Fatalf("parse valid deep schema: %v", err)
	}
	if d, bound := time.Since(t0), raceRelaxed(200*time.Millisecond); d > bound {
		t.Errorf("valid 900-deep schema parsed in %v; want <%v (O(n^2) regression?)", d, bound)
	}
	// Canonical()/Fingerprint must also be linear (it is on the hot Parse
	// path for the SOE fingerprint).
	t1 := time.Now()
	_ = s.Canonical()
	if d, bound := time.Since(t1), raceRelaxed(200*time.Millisecond); d > bound {
		t.Errorf("Canonical() of 900-deep schema took %v; want <%v", d, bound)
	}
}

// Canonical() must emit valid JSON (and a sound fingerprint) for a name
// containing a literal backslash, reachable via WithLaxNames. The former
// path HTML-escaped then bytes.ReplaceAll-un-escaped, which collapsed the
// \uXXXX target inside a \\uXXXX escape, producing invalid JSON.
func TestMatrix_CanonicalBackslashNameValid(t *testing.T) {
	for _, name := range []string{`a&b`, `x<y`, `p q`, `back\\slash`} {
		schema := `{"type":"record","name":"` + jsonEscapeForTest(name) + `","fields":[]}`
		s, err := avro.Parse(schema, avro.WithLaxNames(nil))
		if err != nil {
			t.Fatalf("parse %q: %v", name, err)
		}
		c := s.Canonical()
		if !json.Valid(c) {
			t.Errorf("Canonical() for name %q is invalid JSON: %s", name, c)
		}
		// The PCF must round-trip-parse (registries re-parse canonical form).
		if _, err := avro.Parse(string(c), avro.WithLaxNames(nil)); err != nil {
			t.Errorf("Parse(Canonical()) for name %q: %v\ncanonical: %s", name, err, c)
		}
	}
}

func jsonEscapeForTest(s string) string {
	b, _ := json.Marshal(s)
	return string(b[1 : len(b)-1]) // strip surrounding quotes
}

// SchemaNode.Schema() (the Root().Schema() metadata round-trip) must be O(n) in
// schema size, not O(depth*subtree). toJSONWalk snapshotted every named type's
// full marshaled body for conflict detection, so on a nested record chain each
// enclosing record re-marshaled everything below it (O(n^2)) even though the
// snapshot map is only ever read on a duplicate fullname. Parse() and Canonical()
// of the same schema are already linear, and this pins the metadata emitter to
// match: a 900-deep, ~318KB record chain that parses in ~12ms regressed to >1.3s.
func TestRegression_RootSchemaEmitterLinearOnDeepNesting(t *testing.T) {
	const depth = 900
	var sb strings.Builder
	for i := 0; i < depth; i++ {
		fmt.Fprintf(&sb, `{"type":"record","name":"R%d","fields":[{"name":"f","type":`, i)
	}
	sb.WriteString(`{"type":"record","name":"Leaf","doc":"` + strings.Repeat("x", 256*1024) + `","fields":[{"name":"v","type":"int"}]}`)
	for i := 0; i < depth; i++ {
		sb.WriteString(`}]}`)
	}
	s, err := avro.Parse(sb.String())
	if err != nil {
		t.Fatalf("parse deep record chain: %v", err)
	}
	root := s.Root()
	t0 := time.Now()
	mustNodeSchema(t, root)
	if d, bound := time.Since(t0), raceRelaxed(500*time.Millisecond); d > bound {
		t.Errorf("Root().Schema() of a %d-deep record chain took %v; want <%v (O(depth*subtree) regression in toJSONWalk)", depth, d, bound)
	}
}

// A schema field name has no length cap at parse — validName is pure grammar and
// WithLaxNames permits any non-empty string — so a registry schema can carry a
// multi-megabyte name, and runtime per-datum errors that echo it amplify 1:N:
// one oversized error string on every Encode/Decode call. The binary
// type-mismatch path routes the name through SemanticError.Field, which Error()
// render-truncates; these four paths — JSON missing-field on encode and decode,
// JSON alias collision, and the binary struct-mapping missing-field whose name
// rides in .Err rather than the truncated .Field — must apply the same bound at
// construction. The control proves the asymmetry: the .Field path is already
// bounded, so a failure here is a missed sibling of that bound.
func TestMatrix_FieldNameErrorEchoBounded(t *testing.T) {
	const hostileLen = 1 << 20 // 1 MiB
	const cap = 4096

	hugeNameSchema := func(t *testing.T) *avro.Schema {
		t.Helper()
		huge := strings.Repeat("A", hostileLen)
		s := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"`+huge+`","type":"int"}]}`)
		return s
	}
	assertBounded := func(t *testing.T, err error) {
		t.Helper()
		if err == nil {
			t.Fatal("expected an error")
		}
		if n := len(err.Error()); n > cap {
			t.Errorf("error message is %d bytes for a %d-byte hostile name (cap %d): echo unbounded\nfirst 160: %.160s",
				n, hostileLen, cap, err.Error())
		}
	}

	t.Run("json encode missing required field", func(t *testing.T) {
		s := hugeNameSchema(t)
		_, err := s.EncodeJSON(map[string]any{})
		assertBounded(t, err)
	})
	t.Run("json decode missing required field", func(t *testing.T) {
		s := hugeNameSchema(t)
		var out map[string]any
		assertBounded(t, s.DecodeJSON([]byte(`{}`), &out))
	})
	t.Run("binary encode struct mapping missing field", func(t *testing.T) {
		s := hugeNameSchema(t)
		type empty struct{}
		_, err := s.Encode(empty{})
		assertBounded(t, err)
	})
	t.Run("json decode alias collision echoes two wire keys", func(t *testing.T) {
		huge := strings.Repeat("B", hostileLen)
		s := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"f","aliases":["`+huge+`"],"type":"int"}]}`)
		var out map[string]any
		assertBounded(t, s.DecodeJSON([]byte(`{"f":1,"`+huge+`":2}`), &out))
	})

	// Control: the binary type-mismatch path routes the field name through
	// SemanticError.Field (render-truncated in errors.go). It is already
	// bounded — present so a future change that breaks the render truncation
	// is caught here, and to document that the four cases above are an
	// asymmetry with this path, not field names being inherently echo-safe.
	t.Run("control: binary type mismatch already bounded", func(t *testing.T) {
		huge := strings.Repeat("A", hostileLen)
		s := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"`+huge+`","type":"int"}]}`)
		_, err := s.Encode(map[string]any{huge: "not-an-int"})
		assertBounded(t, err)
	})
}

// nestStrayContainer builds a stray-container-key chain d levels deep on a
// non-binding kind ("int"). key is one of the recursive stray-routed keys:
// "items"/"values" wrap a schema, "fields" wraps a one-field record shape.
// Every level's key is a stray (int binds none of them), so the whole input
// is accepted as inert metadata — and each level's body is a valid schema
// shape, so the parser and the Root() metadata walker both descend it.
func nestStrayContainer(key string, d int) string {
	open, closeStr := `{"type":"int","`+key+`":`, `}`
	if key == "fields" {
		open, closeStr = `{"type":"int","fields":[{"name":"f","type":`, `}]}`
	}
	var sb strings.Builder
	for range d {
		sb.WriteString(open)
	}
	sb.WriteString(`"int"`)
	for range d {
		sb.WriteString(closeStr)
	}
	return sb.String()
}

func bestOfDuration(n int, fn func()) time.Duration {
	best := time.Duration(1) << 62
	for range n {
		t0 := time.Now()
		fn()
		if d := time.Since(t0); d < best {
			best = d
		}
	}
	return best
}

// A reserved structural/naming key on a kind that does not bind it is decoded
// once by the parser's arm to decide whether it surfaces as-written or rides in
// Props. That decode must not be repeated: the props-routing loop and the Root()
// metadata walk each ROUTE the same bodies, and a second decode re-enters the
// recursive schema decode, so two decodes per level compound to O(2^depth) — a
// sub-KB input that hangs Parse for seconds. This pins BOTH the parse path and
// the Root().Schema() rebuild to linear cost: an absolute sub-KB ceiling on each
// entry point, plus a growth-shape assertion that catches a superlinear
// regression a fast machine would sail past under the ceiling.
func TestMatrix_NestedStrayContainerKeyLinearCost(t *testing.T) {
	t.Parallel()

	// Sub-KB ceiling at every parse entry point + the metadata rebuild, for
	// each recursive stray-routed key. At depth 20 the pre-fix exponential
	// cost was multiple SECONDS on this <1KB input; linear cost is
	// microseconds, so a generous 100ms ceiling catches any doubling
	// regression by orders of magnitude.
	const ceilDepth = 20
	ceiling := raceRelaxed(100 * time.Millisecond)
	for _, key := range []string{"items", "values", "fields"} {
		schema := nestStrayContainer(key, ceilDepth)
		if len(schema) >= 1024 {
			t.Fatalf("%s depth %d schema is %d bytes, not sub-KB", key, ceilDepth, len(schema))
		}
		entryPoints := []struct {
			name string
			run  func()
		}{
			{"Parse", func() {
				if _, err := avro.Parse(schema); err != nil {
					t.Errorf("Parse(%s): %v", key, err)
				}
			}},
			{"MustParse", func() { _ = avro.MustParse(schema) }},
			{"SchemaCache.Parse", func() {
				var c avro.SchemaCache
				if _, err := c.Parse(schema); err != nil {
					t.Errorf("SchemaCache.Parse(%s): %v", key, err)
				}
			}},
			{"Root().Schema()", func() {
				s := avro.MustParse(schema)
				root := s.Root()
				if _, err := root.Schema(); err != nil {
					t.Errorf("Root().Schema()(%s): %v", key, err)
				}
			}},
		}
		for _, ep := range entryPoints {
			t0 := time.Now()
			ep.run()
			if d := time.Since(t0); d > ceiling {
				t.Errorf("%s of a %d-deep stray %q schema (%d bytes) took %v; want <%v (exponential re-decode regression?)",
					ep.name, ceilDepth, key, len(schema), d, ceiling)
			}
		}
	}

	// Growth shape: doubling the depth must ~double the time (linear), not square
	// or explode it. Measured at ms scale (best-of-N minimum, so a scheduler hiccup
	// on one sample doesn't skew the ratio) where the signal is clean: the pre-fix
	// exponential blows this ratio to astronomical, and a quadratic regression (the
	// metadata walker re-validating each subtree per enclosing level) lands near 4.
	// Skipped under -race, where instrumentation distorts the ratio and the absolute
	// ceilings above still catch the exponential.
	if isRaceEnabled() {
		return
	}
	// Deep enough that fixed per-call overhead does not dilute the growth
	// signal: a linear impl lands near 2, a quadratic one near 4, so the 3.0
	// bound separates them with margin on both sides.
	const dLo, dHi = 400, 800
	// Warm caches/JIT-like effects out.
	for range 20 {
		s := avro.MustParse(nestStrayContainer("items", dLo))
		r := s.Root()
		_, _ = r.Schema()
	}
	sLo, sHi := nestStrayContainer("items", dLo), nestStrayContainer("items", dHi)
	assertLinear := func(name string, lo, hi func(string)) {
		tLo := bestOfDuration(25, func() { lo(sLo) })
		tHi := bestOfDuration(25, func() { hi(sHi) })
		if ratio := float64(tHi) / float64(tLo); ratio > 3.0 {
			t.Errorf("%s: depth %d took %v, depth %d took %v — ratio %.2f > 3.0 for a 2x depth increase (superlinear regression)",
				name, dLo, tLo, dHi, tHi, ratio)
		}
	}
	assertLinear("Parse",
		func(s string) { _, _ = avro.Parse(s) },
		func(s string) { _, _ = avro.Parse(s) })
	rootSchema := func(s string) {
		sc := avro.MustParse(s)
		r := sc.Root()
		_, _ = r.Schema()
	}
	assertLinear("Root().Schema()", rootSchema, rootSchema)
}
