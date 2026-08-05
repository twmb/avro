package avro

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// Avro spells the null type two ways — the bare primitive string "null" and
// the wrapped object {"type":"null"} — and they denote the same type: same
// branch, same wire bytes, same canonical form. Props and a logicalType on a
// wrapped null are inert (Avro defines no null logical type), so a
// carrier-bearing wrapped null is still a null branch.
//
// SchemaFor decides "is this union branch null?" on a PRE-PARSE tree of
// `any` — a representation distinct from the parsed aschema and the compiled
// node — at two points: the pointer collapse (a nullable T inside a nullable
// T must not nest a union inside a union) and the null-first default fill.
// Both decisions must see both spellings, because the tree they decide on is
// handed straight to the very parser that treats the two as one type.
//
// The renderer emits a wrapped null bare when it carries nothing, so only a
// carrier-bearing wrapped null (props, or a logicalType) survives the render
// as an object — those are the spellings these tests use.

// nullSpellUnions returns the union spellings that must behave identically,
// keyed by a subtest-safe name. "bare" is the control: it exercised the
// pre-fix code path, so a test whose control fails is measuring the wrong
// thing.
func nullSpellUnions() []struct{ name, union string } {
	return []struct{ name, union string }{
		{"bare", `["null","string"]`},
		{"wrapped_plain", `[{"type":"null"},"string"]`},
		{"wrapped_props", `[{"type":"null","x":1},"string"]`},
		{"wrapped_logicaltype", `[{"type":"null","logicalType":"nope"},"string"]`},
	}
}

// nullSpellMarker is the Go type the spelling tests' CustomTypes match on.
type nullSpellMarker struct{ A int64 }

// nullSpellCustom builds a CustomType whose Schema is the parsed union.
func nullSpellCustom(t *testing.T, union string) CustomType {
	t.Helper()
	s, err := Parse(union)
	if err != nil {
		t.Fatalf("parse custom union %s: %v", union, err)
	}
	root := s.Root()
	return CustomType{GoType: reflect.TypeFor[nullSpellMarker](), Schema: root}
}

// TestRegression_SchemaForPointerCollapseWrappedNullBranch pins that the
// pointer arm's union collapse recognizes a null first branch in either
// spelling. A *T field whose CustomType supplies a null-first union must
// collapse to that union; keying the collapse on the bare spelling alone
// emits ["null", [<union>]], which Avro forbids — the build then fails on a
// schema whose bare-spelled twin builds fine.
func TestRegression_SchemaForPointerCollapseWrappedNullBranch(t *testing.T) {
	ptrTo := reflect.PointerTo(reflect.TypeFor[nullSpellMarker]())
	fields := []reflect.StructField{{Name: "F", Type: ptrTo}}

	var want string
	for _, tc := range nullSpellUnions() {
		t.Run(tc.name, func(t *testing.T) {
			s, err := schemaForScopeCell(t, fields, "", []CustomType{nullSpellCustom(t, tc.union)})
			if err != nil {
				t.Fatalf("build failed for a null-first union: %v", err)
			}
			if strings.Contains(s.String(), `[["null"`) || strings.Contains(s.String(), `,["null"`) {
				t.Fatalf("emitted a union nested directly inside a union: %s", s.String())
			}
			// Every spelling denotes one type, so the canonical forms —
			// which strip the inert carriers — must be byte-identical.
			if want == "" {
				want = string(s.Canonical())
			} else if got := string(s.Canonical()); got != want {
				t.Fatalf("canonical form differs by null spelling:\n got %s\nwant %s", got, want)
			}
		})
	}
}

// TestRegression_SchemaForNullFirstDefaultWrappedNullBranch pins that the
// null-first default fill recognizes both spellings. The assertion is on the
// EMITTED SCHEMA TEXT, not on twmb's decode behavior: twmb synthesizes an
// implicit null default for a nullable union at parse, so the omission is
// invisible in-process, but the emitted text is what a caller publishes to a
// registry or hands to another implementation — and Java and fastavro do not
// infer the default. Without "default":null those readers cannot read data
// written before the field existed.
func TestRegression_SchemaForNullFirstDefaultWrappedNullBranch(t *testing.T) {
	fields := []reflect.StructField{{Name: "F", Type: reflect.TypeFor[nullSpellMarker]()}}

	for _, tc := range nullSpellUnions() {
		t.Run(tc.name, func(t *testing.T) {
			s, err := schemaForScopeCell(t, fields, "", []CustomType{nullSpellCustom(t, tc.union)})
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			var doc struct {
				Fields []map[string]json.RawMessage `json:"fields"`
			}
			if err := json.Unmarshal([]byte(s.String()), &doc); err != nil {
				t.Fatalf("emitted schema does not unmarshal: %v", err)
			}
			if len(doc.Fields) != 1 {
				t.Fatalf("want 1 field, got %d: %s", len(doc.Fields), s.String())
			}
			raw, ok := doc.Fields[0]["default"]
			if !ok {
				t.Fatalf("emitted schema omits the null-first union's \"default\":null: %s", s.String())
			}
			if string(raw) != "null" {
				t.Fatalf("default is %s, want null: %s", raw, s.String())
			}
			// The metadata surface must agree with the emitted text.
			if f := s.Root().Fields[0]; !f.HasDefault || f.Default != nil {
				t.Fatalf("Root() reports HasDefault=%v Default=%#v, want true/nil", f.HasDefault, f.Default)
			}
		})
	}
}

// TestMatrix_SchemaForNullBranchSpellingParity crosses the null-SPELLING
// axis into the SchemaFor composition space: for every union-bearing cell,
// respelling the null branch must not change the built schema.
//
// Axes: spelling {bare, wrapped-plain, wrapped-props, wrapped-logicalType} ×
// union shape {null-first 2-branch, null-first 3-branch, null-SECOND
// 2-branch} × field shape {value, pointer} × occurrences {1, 2} × SchemaFor
// scope {default, WithNamespace}.
//
// The oracle is per-cell equivalence against the bare spelling, which is the
// control the pre-fix code already handled: identical build verdict (both
// succeed or both fail), identical Canonical() (PCF strips the inert
// carriers, so the four spellings collapse to one form — a calibration-free
// comparison), identical fingerprint, identical per-field default presence,
// and identical wire bytes for a probe value. Cells whose bare form is
// itself an error (a null-SECOND union at a pointer field nests a union in a
// union in every spelling) must fail the same way in every spelling — the
// invariant is agreement, not success.
func TestMatrix_SchemaForNullBranchSpellingParity(t *testing.T) {
	marker := reflect.TypeFor[nullSpellMarker]()
	ptrTo := reflect.PointerTo(marker)

	// Each shape names how to spell its null branch: %s is substituted with
	// the spelling under test.
	shapes := []struct{ name, tmpl string }{
		{"nullfirst2", `[%s,"string"]`},
		{"nullfirst3", `[%s,"string","long"]`},
		{"nullsecond2", `["string",%s]`},
	}
	spellings := []struct{ name, null string }{
		{"bare", `"null"`},
		{"wrapped_plain", `{"type":"null"}`},
		{"wrapped_props", `{"type":"null","x":1}`},
		{"wrapped_logicaltype", `{"type":"null","logicalType":"nope"}`},
	}

	type outcome struct {
		errored     bool
		canonical   string
		fingerprint string
		defaults    string
		emitted     string
	}

	cells := 0
	for _, shape := range shapes {
		for _, fieldShape := range []string{"value", "pointer"} {
			for _, occurrences := range []int{1, 2} {
				for _, ns := range []string{"", "b"} {
					goType := marker
					if fieldShape == "pointer" {
						goType = ptrTo
					}
					var control outcome
					for i, sp := range spellings {
						name := fmt.Sprintf("%s/%s/occ%d/ns=%q/%s", shape.name, fieldShape, occurrences, ns, sp.name)
						t.Run(name, func(t *testing.T) {
							cells++
							union := fmt.Sprintf(shape.tmpl, sp.null)
							s, err := schemaForScopeCell(t, scopeCellFields(occurrences, goType), ns, []CustomType{nullSpellCustom(t, union)})
							got := outcome{errored: err != nil}
							if err == nil {
								got.canonical = string(s.Canonical())
								got.fingerprint = fmt.Sprintf("%x", s.Fingerprint(NewRabin()))
								got.defaults = nullSpellDefaults(t, s)
								got.emitted = s.String()
								if _, perr := Parse(got.emitted); perr != nil {
									t.Fatalf("emitted schema does not re-parse: %v\n%s", perr, got.emitted)
								}
							}
							if i == 0 {
								control = got
								return
							}
							if got.errored != control.errored {
								t.Fatalf("build verdict differs from the bare control: errored=%v (control %v); emitted %s",
									got.errored, control.errored, got.emitted)
							}
							if got.errored {
								return // both spellings reject: agreement is the invariant
							}
							if got.canonical != control.canonical {
								t.Fatalf("canonical differs from the bare control:\n got %s\nwant %s", got.canonical, control.canonical)
							}
							if got.fingerprint != control.fingerprint {
								t.Fatalf("fingerprint differs from the bare control: got %s want %s", got.fingerprint, control.fingerprint)
							}
							if got.defaults != control.defaults {
								t.Fatalf("field defaults differ from the bare control:\n got %s\nwant %s\nemitted %s",
									got.defaults, control.defaults, got.emitted)
							}
						})
					}
				}
			}
		}
	}
	t.Logf("cells=%d", cells)
}

// nullSpellDefaults renders each field's default presence and value from the
// EMITTED text, so the comparison sees exactly what a caller publishes.
func nullSpellDefaults(t *testing.T, s *Schema) string {
	t.Helper()
	var doc struct {
		Fields []map[string]json.RawMessage `json:"fields"`
	}
	if err := json.Unmarshal([]byte(s.String()), &doc); err != nil {
		t.Fatalf("emitted schema does not unmarshal: %v", err)
	}
	var b strings.Builder
	for i, f := range doc.Fields {
		if i > 0 {
			b.WriteByte(';')
		}
		if raw, ok := f["default"]; ok {
			fmt.Fprintf(&b, "default=%s", raw)
		} else {
			b.WriteString("absent")
		}
	}
	return b.String()
}
