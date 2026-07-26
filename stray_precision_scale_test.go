package avro_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// Stray "precision"/"scale" schema attributes are inert metadata, not parse
// errors. Per the Avro spec ("Attributes not defined in this document are
// permitted as metadata", Specification/_index.md:43), an attribute the
// parser does not consume is a plain custom property. twmb consumes
// precision/scale as decimal parameters exactly when the node is a
// recognized decimal carrier — logicalType "decimal" on bytes or fixed,
// where NOT_BUGS #55 validates them — and every other placement surfaces
// them in Props, matching the field level and both references (fastavro
// 1.12.2 executed 9/9 accepts; Java's LogicalTypes.fromSchemaImpl returns
// null when logicalType is absent so precision is never consulted,
// LogicalTypes.java:127-130, and extra attributes are props).
//
// The FIELD level follows the same consumed-conditional rule with the
// consumer being the decimal lift: the pair is consumed exactly when the
// field declares logicalType "decimal" and the lift target is a
// bytes/fixed carrier as-written; there a malformed body rejects loudly
// naming the key (treating it as absent would silently parse as
// decimal(p,0), scale being optional), and everywhere else any body shape
// is an ordinary SchemaField.Props property (NOT_BUGS #71; Java's
// FIELD_RESERVED never includes the pair, Schema.java:503-504). The
// placement matrix below crosses a BODY-SHAPE axis over every placement,
// kind, and level for exactly this reason.
//
// Pre-fix, validateLogical's tail rejected any leftover precision/scale
// ("invalid scale or precision specified") exactly when NO logical — or a
// valid non-decimal logical — accompanied them, while the SAME stray keys
// parsed when the logical placement was invalid (unknown logical,
// decimal-on-int), and the field level already treated them as inert
// props: twmb disagreed with itself across levels and across placements.
func TestRegression_StrayPrecisionScaleParses(t *testing.T) {
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
func TestRegression_BogusLogicalStrayKeysSurfaceAsProps(t *testing.T) {
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
		s, err := avro.Parse(`{"type":"record","name":"R","fields":[
			{"name":"F","type":{"type":"fixed","name":"Fx","size":4}},
			{"name":"b","type":{"type":"Fx","logicalType":"decimal","precision":3}}]}`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
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
		s, err := avro.Parse(`{"type":"int","logicalType":"decimal","precision":3}`, avro.WithCustomType(ct))
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		enc, err := s.AppendEncode(nil, int32(7))
		if err != nil {
			t.Fatalf("AppendEncode: %v", err)
		}
		var out any
		if _, err := s.Decode(enc, &out); err != nil {
			t.Fatalf("Decode: %v", err)
		}
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

// A field-level "precision"/"scale" whose body fails the int shape is an
// inert custom property wherever the pair is UNCONSUMED — no field
// logicalType, a non-decimal field logicalType, or a decimal whose lift
// target is not a bytes/fixed carrier — riding to SchemaField.Props
// verbatim, exactly like a valid-int unconsumed pair. Java never validates
// field-level precision/scale (FIELD_RESERVED = {default, doc, name, order,
// type, aliases}, Schema.java:503-504, so parseProperties preserves them as
// plain props, Schema.java:1905) and fastavro 1.12.2 accepts and preserves
// them verbatim. Only a CONSUMED placement — field logicalType "decimal"
// whose lift target is a bytes/fixed carrier — keeps the loud shape reject
// (TestRegression_FieldDecimalConsumedMalformedParamReject).
func TestRegression_FieldPrecisionScaleMalformedUnconsumedInert(t *testing.T) {
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

// Consumed placements keep the loud shape reject, fired from the recorded
// per-key shape error and naming the key. The scale cell is the guard
// against "treat malformed as absent": scale is OPTIONAL (spec default 0),
// so silently dropping a malformed scale beside a valid precision would
// parse as decimal(p,0) — a silent wire-semantics change (#55
// anti-silent-drop). Consumption follows what the lift LANDS, not
// where it points: the pair is consumed exactly where the target's EFFECTIVE
// logical — its own when it has one, else the field's — is "decimal" on a
// bytes/fixed carrier. The pre-annotated-target cells therefore live in the
// INERT test below, and the own-logical-is-decimal cells here are what keep
// the rule from being loosened into "any annotation of its own is inert".
func TestRegression_FieldDecimalConsumedMalformedParamReject(t *testing.T) {
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
	s, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":"int","precision":3}]}`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	f := s.Root().Fields[0]
	if got := f.Props["precision"]; got != int64(3) {
		t.Errorf("SchemaField.Props[precision] = %#v; want int64(3)", got)
	}
	if !strings.Contains(s.String(), `"precision":3`) {
		t.Errorf("String() dropped the unconsumed field precision: %s", s.String())
	}
	root := s.Root()
	rb, err := root.Schema()
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if got := rb.Root().Fields[0].Props["precision"]; got != int64(3) {
		t.Errorf("rebuild Props[precision] = %#v; want int64(3)", got)
	}
}

// ---------------------------------------------------------------------------
// Class matrix: stray-precision/scale placement × level × kind.
//
// Locks the whole class the stray-precision fix belongs to, not just the
// filed instance: for every attribute placement, at both the type level and
// the field level, across every kind class the parser routes differently.
// Per accepted cell it asserts the verdict, the Props-vs-Precision/Scale
// routing, the B15 axes-3&4 round trips (String() reparse and Root().Schema()
// rebuild — newly accepted shapes must rebuild to schemas that reparse), and
// — for cells whose precision/scale are inert — that the wire bytes and the
// Canonical()/Rabin fingerprint are identical to the stray-free twin (PCF
// strips attributes it does not define; asserted, not assumed, and calibrated
// against fastavro's PCF by execution in
// TestMatrix_StrayPrecisionScaleFastavroPCF). Reject cells are the #55
// controls: recognized-decimal carriers with invalid parameters keep
// hard-rejecting.

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

// TestMatrix_StrayPrecisionScaleFastavroPCF drives every twmb-ACCEPTED cell
// of the placement × kind × level × body grid through fastavro by
// execution: fastavro must parse the same spelling (it validates decimal
// parameters only where they are consumed, and preserves everything else),
// and its to_parsing_canonical_form must equal twmb's Canonical() — PCF
// strips attributes it does not define, which is also why the Rabin
// fingerprints agree cross-impl. Reject cells are not driven: they are the
// documented keep-strict side (#55 params, #41 quoted form, and the
// malformed-shape rejects on consumed placements). The flat field spelling
// is likewise not here — fastavro rejects it outright (UnknownType), so its
// cross-impl anchor is the nested twin
// (TestRegression_FlatFieldMalformedPrecisionMatchesNestedTwin). Skips
// without a fastavro python (AVRO_FASTAVRO_PYTHON), like every differential.
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
	// fastavro validates decimal parameters on ANY type-level object
	// carrying logicalType "decimal", carrier kind or not, and its
	// validation demands numeric values (a truthiness gate skips falsy
	// ones, which is why 0 and huge positive ints pass). Java instead
	// drops the whole logical on a non-carrier via
	// LogicalTypes.fromSchemaIgnoreInvalid (LogicalTypes.java:120, called
	// at Schema.java:1979) without ever reading the params, so it accepts
	// these spellings — twmb follows Java (and the general
	// unconsumed-is-inert rule). The divergence is pinned by DIRECTION:
	// these cells must keep rejecting in fastavro, so a fastavro release
	// that relaxes flips this loudly and the recorded rationale gets
	// re-checked.
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
		s, err := avro.Parse(`{"type":"int","fields":[{"name":"f","type":"int","precision":"x"}]}`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
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
		s, err := avro.Parse(`{"type":"int","fields":[{"name":"f","type":"bytes","logicalType":"decimal","precision":"x"}]}`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		n := s.Root()
		if len(n.Fields) != 0 {
			t.Errorf("malformed-element stray fields surfaced structurally: %#v", n.Fields)
		}
		if _, ok := n.Props["fields"]; !ok {
			t.Errorf("malformed stray fields body missing from Props: %#v", n.Props)
		}
	})
}

// A field-level "decimal" whose lift TARGET already carries its own logical
// type never lands: closer-to-the-type wins, so the target keeps its own
// annotation and the field's precision/scale annotate nothing. The pair is
// therefore inert metadata there and rides to Props like any custom
// property, malformed or not.
//
// Inertness is PROVEN on the wire rather than asserted: the encoding is
// byte-identical with scale 0, with scale 2, and with no field-level logical
// at all. The discriminator is the sibling case where the target's own
// logical IS decimal — there the same parameters DO land, and scale 0 versus
// 2 diverges, which is what keeps this rule from being loosened into "a
// target with any annotation of its own is inert".
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
