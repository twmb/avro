package avro_test

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

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
	s, err := avro.Parse(schema)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = s.Encode(map[string]any{bigName: "not-an-int"})
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
