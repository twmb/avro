package avro_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// Reserved Avro attribute names match ONLY their exact lowercase spelling,
// on every reading surface. A key differing from a reserved name only by
// letter case is an ordinary custom property: never bound, never consumed,
// preserved verbatim in Props on parse and on every metadata surface. This
// matches the spec (attribute names are literal JSON keys) and the three
// executed references — Java's reserved-key sets are exact-lowercase
// HashSets (SCHEMA_RESERVED, Schema.java:175-176; FIELD_RESERVED :503) and
// its structural reads use exact Jackson lookups; fastavro 1.12.2 reads
// known keys by exact name and preserves the rest (executed: KeyError
// 'items' for {"type":"array","ITEMS":"int"}); goavro reads exact keys
// ("Array ought to have items key", executed). hamba/avro is the one
// case-folding implementation; a schema that parsed there but fails here
// has a miscased reserved key — a miscased STRUCTURAL key fails loudly at
// parse (fix the casing), a miscased non-structural key is a harmless
// custom property.

// TestRegression_CaseVariantStructuralKeyRejects pins the consequence of
// exact-case matching for structural keys: a case-variant spelling does
// NOT bind, so the real structural attribute is absent and the parse fails
// with the kind's ordinary missing-attribute error — loudly, at parse
// time, never as a silently different schema.
func TestRegression_CaseVariantStructuralKeyRejects(t *testing.T) {
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
	if _, err := s.AppendEncode(nil, map[string]any{"f": int32(1)}); err != nil {
		t.Fatalf("encode: %v", err)
	}
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
	s, err := avro.Parse(`{"type":"int","ITEMS":"long"}`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	root := s.Root()
	if root.Items != nil {
		t.Errorf("Items = %+v; a case-variant key must not surface structurally", root.Items)
	}
	if got := root.Props["ITEMS"]; !reflect.DeepEqual(got, "long") {
		t.Errorf(`Props["ITEMS"] = %#v; want "long" verbatim`, got)
	}

	// The exact-lowercase stray keeps its structural surfacing (the
	// boundary-1 control: the stray routing is about placement, not case).
	s2, err := avro.Parse(`{"type":"int","items":"long"}`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
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
	s, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":"int","DEFAULT":7}]}`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
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

// exactCaseVariantOnlyRows enumerates the discriminating spelling axis of
// the exact-case contract: a reserved key present ONLY as a case-variant.
// Because the variant is an ordinary custom property, the reserved
// attribute is absent — a REQUIRED attribute's absence rejects with the
// kind's ordinary missing-attribute error, and an optional attribute's
// absence leaves the attribute unset with the variant riding to Props.
// The exact+variant-both-present axis (the variant inert beside the
// consumed exact key) is TestMatrix_ReservedKeyDuplicateSpellings; the
// exact-only controls live there too.
//
// Each row records the executed fastavro verdict for the same cell
// (testdata/oracle, TestDifferentialFastavroReservedExactCase). Two cells
// diverge for reasons that predate and outlive the case rule, recorded
// per cell: fastavro accepts a fields-less record as zero fields, and
// accepts a precision-less decimal by dropping the logical, where twmb
// requires fields (like Java's "Record has no fields") and requires
// decimal precision.
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
