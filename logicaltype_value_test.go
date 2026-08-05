package avro_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/twmb/avro"
)

// A logicalType attribute whose JSON value is not a string is inert
// metadata, preserved verbatim in Props — the same treatment an unknown
// STRING logical already gets (which surfaces on SchemaNode.LogicalType
// as-written, also inert). No reference implementation rejects the
// spelling: Java reads only textual logicalType props (a non-text value
// yields no logical and the prop is preserved — LogicalTypes.java
// fromSchemaImpl via Schema.getProp; field-level logicalType is
// warn-and-ignored, Schema.java parseFields), fastavro parses and
// preserves the key verbatim (executed below), and goavro switches on the
// value and falls through to the plain type. Only a string activates the
// logical dispatch; anything else can never name a logical, so its only
// coherent reading is a custom property.
func TestRegression_NonStringLogicalTypeInert(t *testing.T) {
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
			s, err := avro.Parse(`{"type":"int","logicalType":` + c.val + `}`)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
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
			rb, err := n.Schema()
			if err != nil {
				t.Fatalf("rebuild: %v", err)
			}
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
			s, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":"int","logicalType":` + c.val + `}]}`)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
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
