package avro_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
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
	level     string // type | field
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

// consumed reports whether precision/scale are decimal-consumed (validated
// parameters, not stray metadata) for this cell.
func (c strayPSCell) consumed() bool { return c.placement == "decimal-valid" && c.carrier() }

// rejects reports whether the cell must hard-reject at parse (#55 control).
func (c strayPSCell) rejects() bool { return c.placement == "decimal-invalid" && c.carrier() }

// attrsJSON renders `,"logicalType":"x","precision":3,...` (or "" when
// stripParams is set and there is no logical). stripParams builds the
// stray-free twin: the logical stays, only precision/scale go.
func strayPSAttrsJSON(logical string, params [][2]any, stripParams bool) string {
	var sb strings.Builder
	if logical != "" {
		fmt.Fprintf(&sb, `,"logicalType":%q`, logical)
	}
	if !stripParams {
		for _, p := range params {
			fmt.Fprintf(&sb, `,%q:%d`, p[0], p[1])
		}
	}
	return sb.String()
}

// schemaJSON builds the cell's schema; twin strips precision/scale.
func (c strayPSCell) schemaJSON(twin bool) string {
	logical, params := strayPSAttrs(c.placement, c.kind)
	attrs := strayPSAttrsJSON(logical, params, twin)
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
	return fmt.Sprintf(`{"type":"record","name":"Host","fields":[{"name":"a","type":%s%s}]}`, typ, attrs)
}

// value returns a wire-encodable value for the cell, compatible with
// whichever logical actually applies (uuid wants UUID-format text,
// big-decimal and consumed decimal want *big.Rat, duration wants Duration).
func (c strayPSCell) value() any {
	if c.consumed() {
		v := any(big.NewRat(3, 2)) // 1.5: precision 2, scale 1
		if c.level == "field" {
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
	if c.level == "field" {
		return map[string]any{"a": v}
	}
	return v
}

// paramsPropsMap converts the injected params to the exact Props content
// expected on the surfaced node (values are int64 per the Props contract).
func strayPSParamsProps(params [][2]any) map[string]any {
	m := make(map[string]any, len(params))
	for _, p := range params {
		m[p[0].(string)] = int64(p[1].(int))
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
			if want := strayPSParamsProps(params); !reflect.DeepEqual(root.Props, want) {
				t.Errorf("%s: Props = %#v; want %#v", when, root.Props, want)
			}
			if root.LogicalType != logical {
				t.Errorf("%s: LogicalType = %q; want as-written %q", when, root.LogicalType, logical)
			}
		}
		return
	}
	// Field level: every attribute stays in the FIELD's Props as written
	// (the wire lift is a codec concession, never extended into the
	// metadata API), and the field's TYPE node never consumes them.
	f := root.Fields[0]
	want := strayPSParamsProps(params)
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
	levels := []string{"type", "field"}

	for _, level := range levels {
		for _, placement := range placements {
			for _, kind := range kinds {
				c := strayPSCell{placement: placement, kind: kind, level: level}
				t.Run(level+"/"+placement+"/"+kind, func(t *testing.T) {
					src := c.schemaJSON(false)
					s, err := avro.Parse(src)

					if c.rejects() {
						if err == nil {
							t.Fatalf("Parse(%s) accepted; want #55 decimal-param reject", src)
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

// TestMatrix_StrayPrecisionScaleFastavroPCF calibrates the PCF-strips-
// undefined-attributes assumption against fastavro by execution: twmb's
// Canonical() must equal fastavro's to_parsing_canonical_form for
// stray-precision schemas (both bare and nested in a record), which is
// also why the Rabin fingerprints agree cross-impl. Skips without a
// fastavro python (AVRO_FASTAVRO_PYTHON), like every differential.
func TestMatrix_StrayPrecisionScaleFastavroPCF(t *testing.T) {
	o := startOracle(t)
	for _, src := range []string{
		`{"type":"int","precision":3}`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"long","logicalType":"timestamp-millis","precision":3,"scale":1}}]}`,
	} {
		resp := o.call(oracleJob{Op: "canonical", Schema: json.RawMessage(src)})
		if !resp.OK {
			t.Fatalf("fastavro rejected %s: %s", src, resp.Err)
		}
		got := string(avro.MustParse(src).Canonical())
		if got != resp.Canonical {
			t.Errorf("PCF diverges from fastavro for %s:\n twmb: %s\n fast: %s", src, got, resp.Canonical)
		}
	}
}
