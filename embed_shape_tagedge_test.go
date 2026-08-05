package avro

import (
	"fmt"
	"math/big"
	"reflect"
	"testing"
)

// ===========================================================================
// The tag-edge half of the generative net: malformed / edge struct tags.
//
// SchemaFor's parser (collectFields -> parseSchemaTag/splitTag, then
// inferField/inferType) is STRICT: it rejects inline-on-non-struct, inline with
// an explicit name, a decimal tag with trailing junk, a "-" skip carrying
// options, an unknown option, a default that overflows the Go field's narrow
// integer kind, a logical type on an incompatible Go type, an empty alias list.
// The runtime field-mapper (typeFieldMapping -> splitFieldTag/parseTagOptions)
// is LENIENT: it needs only the field name, inline, and omitzero, and ignores
// everything else; on an unbalanced-bracket tag splitTag rejects but
// splitFieldTag falls back to a naive split so the runtime never NEWLY errors
// on a tag a hand-written-schema user already relies on.
//
// That strict/lenient split is the tag-dimension analog of the eager/lazy
// ambiguity split, and it is SAFE only as long as it never becomes a
// both-succeed-DISAGREE: the two walkers share splitTag's tokenization and
// extract name/inline/omitzero with identical logic, so whenever SchemaFor
// builds a field the runtime must map the SAME name to the SAME Go field. This
// family proves that across the cross-product (defect x placement): for every
// shape where collectFields succeeds, typeFieldMapping agrees on every name; and
// the documented SchemaFor verdict (accept/reject) is pinned so a regression in
// the strict parser is caught. Where collectFields rejects, the runtime is
// asserted non-corrupting — it errors loudly or maps a syntactically-valid name
// to a real field, never silently picks a contradictory winner.
// ===========================================================================

type GUUID [16]byte

func ratType() reflect.Type { return reflect.TypeFor[*big.Rat]() }

// a valid struct to attach an (invalid) inline+name to.
func innerNamedStruct() reflect.Type {
	return reflect.StructOf([]reflect.StructField{
		{Name: "A", Type: reflect.TypeFor[int32](), Tag: `avro:"a"`},
	})
}

type tagDefect struct {
	label       string
	field       reflect.StructField
	schemaForOK bool     // does the full SchemaFor pipeline accept it?
	probes      []string // names a user might reference; typeFieldMapping must stay non-corrupting
}

func tagDefects() []tagDefect {
	i32 := reflect.TypeFor[int32]()
	i8 := reflect.TypeFor[int8]()
	str := reflect.TypeFor[string]()
	return []tagDefect{
		// --- rejected by the strict tag parser (collectFields errors) ---
		{"inline-on-nonstruct", reflect.StructField{Name: "F", Type: i32, Tag: `avro:",inline"`}, false, []string{"F"}},
		{"inline-with-name", reflect.StructField{Name: "F", Type: innerNamedStruct(), Tag: `avro:"foo,inline"`}, false, []string{"foo", "a"}},
		{"decimal-trailing-junk", reflect.StructField{Name: "F", Type: ratType(), Tag: `avro:"f,decimal(9,2,3)"`}, false, []string{"f"}},
		{"dash-with-options", reflect.StructField{Name: "F", Type: i32, Tag: `avro:"-,omitzero"`}, false, []string{"-", "F"}},
		{"unknown-option", reflect.StructField{Name: "F", Type: i32, Tag: `avro:"f,bogus"`}, false, []string{"f"}},
		{"empty-alias-bracket", reflect.StructField{Name: "F", Type: i32, Tag: `avro:"f,alias=[]"`}, false, []string{"f"}},
		// --- parsed fine, rejected later by inferField/inferType (collectFields succeeds) ---
		{"narrow-int-default-overflow", reflect.StructField{Name: "F", Type: i8, Tag: `avro:"f,default=9999"`}, false, []string{"f"}},
		{"uuid-on-wrong-kind", reflect.StructField{Name: "F", Type: i32, Tag: `avro:",uuid"`}, false, []string{"F"}},
		{"decimal-on-non-bigrat", reflect.StructField{Name: "F", Type: i32, Tag: `avro:"f,decimal(9,2)"`}, false, []string{"f"}},
		// --- valid controls: both walkers succeed and must agree ---
		{"valid-omitzero", reflect.StructField{Name: "F", Type: i32, Tag: `avro:"f,omitzero"`}, true, []string{"f"}},
		{"valid-alias", reflect.StructField{Name: "F", Type: i32, Tag: `avro:"f,alias=old"`}, true, []string{"f"}},
		{"valid-decimal", reflect.StructField{Name: "F", Type: ratType(), Tag: `avro:"f,decimal(9,2)"`}, true, []string{"f"}},
		{"valid-uuid-on-string", reflect.StructField{Name: "F", Type: str, Tag: `avro:"f,uuid"`}, true, []string{"f"}},
		{"valid-narrow-int-default-ok", reflect.StructField{Name: "F", Type: i8, Tag: `avro:"f,default=5"`}, true, []string{"f"}},
	}
}

type tagEdgeShape struct {
	label       string
	t           reflect.Type
	schemaForOK bool
	probes      []string
}

// genTagEdgeShapes crosses every defect with three placements: the defect field
// alone; alongside a clean sibling (the defect must not poison the clean
// field's mapping); and nested one level inside an inlined struct (the parse
// path must behave identically at depth).
func genTagEdgeShapes() []tagEdgeShape {
	keep := reflect.StructField{Name: "Keep", Type: reflect.TypeFor[int32](), Tag: `avro:"keep"`}
	var shapes []tagEdgeShape
	for _, d := range tagDefects() {
		// alone
		shapes = append(shapes, tagEdgeShape{
			label: d.label + "/alone", schemaForOK: d.schemaForOK, probes: d.probes,
			t: reflect.StructOf([]reflect.StructField{d.field}),
		})
		// with a clean sibling
		shapes = append(shapes, tagEdgeShape{
			label: d.label + "/with-keep", schemaForOK: d.schemaForOK, probes: append([]string{"keep"}, d.probes...),
			t: reflect.StructOf([]reflect.StructField{d.field, keep}),
		})
		// nested one level inside an inlined wrapper
		inner := reflect.StructOf([]reflect.StructField{d.field})
		shapes = append(shapes, tagEdgeShape{
			label: d.label + "/nested-inline", schemaForOK: d.schemaForOK, probes: d.probes,
			t: reflect.StructOf([]reflect.StructField{
				{Name: "Wrap", Type: inner, Tag: `avro:",inline"`},
			}),
		})
	}
	return shapes
}

func TestGenerative_TagEdgeWalkerAgreement(t *testing.T) {
	shapes := genTagEdgeShapes()
	var verdictPins, twoWalkerAgreements, nonCorruptionProbes, bothSucceedDisagree int

	for _, sh := range shapes {
		// (1) SchemaFor verdict pin (independent: the documented accept/reject).
		_, sfErr := schemaForType(sh.t, WithName("R"))
		if (sfErr == nil) != sh.schemaForOK {
			t.Fatalf("%s: SchemaFor verdict mismatch: got err=%v, want accept=%v", sh.label, sfErr, sh.schemaForOK)
		}
		verdictPins++

		cf, cfErr := collectFields(sh.t, make(map[reflect.Type]bool))
		if cfErr == nil {
			// (2) Two-walker agreement: every field collectFields produced must
			// map to the SAME Go field under typeFieldMapping. A both-succeed-
			// disagree here is a Family-5 divergence.
			for _, f := range cf {
				m, err := typeFieldMapping([]string{f.name}, nil, sh.t)
				if err != nil {
					bothSucceedDisagree++
					t.Fatalf("%s: collectFields produced field %q@%v but typeFieldMapping rejected it: %v",
						sh.label, f.name, f.index, err)
				}
				if !reflect.DeepEqual(m.indices[0], f.index) {
					bothSucceedDisagree++
					t.Fatalf("%s: BOTH-SUCCEED-DISAGREE on %q: collectFields=%v typeFieldMapping=%v",
						sh.label, f.name, f.index, m.indices[0])
				}
				twoWalkerAgreements++
			}
		}

		// (3) Non-corruption: probing any name on the runtime mapper must either
		// error (loud — missing/ambiguous) or return a valid, in-bounds field
		// index (FieldByIndex lands on a real field; never a panic, never a path
		// that does not exist in the type).
		for _, p := range sh.probes {
			m, err := typeFieldMapping([]string{p}, nil, sh.t)
			if err != nil {
				nonCorruptionProbes++
				continue
			}
			assertValidIndex(t, sh.label, p, sh.t, m.indices[0])
			nonCorruptionProbes++
		}
	}

	if bothSucceedDisagree != 0 {
		t.Fatalf("found %d both-succeed-disagree tag divergences", bothSucceedDisagree)
	}
	t.Logf("tag-edge net: %d shapes | %d verdict pins | %d two-walker agreements | %d non-corruption probes | 0 both-succeed-disagree",
		len(shapes), verdictPins, twoWalkerAgreements, nonCorruptionProbes)
}

// assertValidIndex confirms an index path returned by the runtime mapper points
// at a real field of t (navigating embeds/pointers), so a "successful" mapping
// can never be a fabricated or out-of-bounds path.
func assertValidIndex(t *testing.T, label, name string, typ reflect.Type, index []int) {
	t.Helper()
	cur := typ
	for _, i := range index {
		for cur.Kind() == reflect.Pointer {
			cur = cur.Elem()
		}
		if cur.Kind() != reflect.Struct || i >= cur.NumField() {
			t.Fatalf("%s: typeFieldMapping(%q) returned invalid index %v (overran %s)", label, name, index, cur)
		}
		cur = cur.Field(i).Type
	}
}

// TestGenerative_UUIDPlainDedup pins the resolved-schema corner the task names:
// the SAME [16]byte Go type used once ,uuid-tagged and once plain is two
// distinct Avro fixed types (they differ by logicalType), so SchemaFor must
// emit BOTH definitions (the name-guarded seen[t] dedup must not collapse them),
// the schema must Parse, and the runtime mapper must round-trip both fields to
// their distinct Go fields. Crossed over field order so neither "first
// occurrence defines" path is privileged.
func TestGenerative_UUIDPlainDedup(t *testing.T) {
	u16 := reflect.TypeFor[GUUID]()
	uuidField := reflect.StructField{Name: "U", Type: u16, Tag: `avro:"u,uuid"`}
	plainField := reflect.StructField{Name: "P", Type: u16, Tag: `avro:"p"`}

	for _, order := range [][]reflect.StructField{
		{uuidField, plainField},
		{plainField, uuidField},
	} {
		st := reflect.StructOf(order)
		s, err := schemaForType(st, WithName("R"))
		if err != nil {
			t.Fatalf("order %v: uuid/plain dedup must build a schema: %v", fieldNamesOf(st), err)
		}
		// Two-walker agreement on both names.
		cf, err := collectFields(st, make(map[reflect.Type]bool))
		if err != nil {
			t.Fatalf("order %v: collectFields: %v", fieldNamesOf(st), err)
		}
		for _, f := range cf {
			m, err := typeFieldMapping([]string{f.name}, nil, st)
			if err != nil || !reflect.DeepEqual(m.indices[0], f.index) {
				t.Fatalf("order %v: walker disagree on %q: cf=%v tfm=%v err=%v", fieldNamesOf(st), f.name, f.index, m, err)
			}
		}
		// Round-trip: distinct 16-byte values land in their distinct fields.
		pv := reflect.New(st)
		var a, b GUUID
		for i := range a {
			a[i] = byte(i)
			b[i] = byte(255 - i)
		}
		setUUIDField(pv.Elem(), "u", "U", a)
		setUUIDField(pv.Elem(), "p", "P", b)
		_ = cf
		data, err := s.AppendEncode(nil, pv.Interface())
		if err != nil {
			t.Fatalf("order %v: encode: %v", fieldNamesOf(st), err)
		}
		dst := reflect.New(st)
		if _, err := s.Decode(data, dst.Interface()); err != nil {
			t.Fatalf("order %v: decode: %v", fieldNamesOf(st), err)
		}
		gotU := dst.Elem().FieldByName("U").Interface().(GUUID)
		gotP := dst.Elem().FieldByName("P").Interface().(GUUID)
		if gotU != a {
			t.Fatalf("order %v: uuid field round-trip: got %v want %v", fieldNamesOf(st), gotU, a)
		}
		if gotP != b {
			t.Fatalf("order %v: plain field round-trip: got %v want %v", fieldNamesOf(st), gotP, b)
		}
	}
}

func setUUIDField(structVal reflect.Value, _ string, goName string, v GUUID) {
	structVal.FieldByName(goName).Set(reflect.ValueOf(v))
}

func fieldNamesOf(t reflect.Type) []string {
	var n []string
	for i := 0; i < t.NumField(); i++ {
		n = append(n, fmt.Sprintf("%s(%s)", t.Field(i).Name, t.Field(i).Tag))
	}
	return n
}
