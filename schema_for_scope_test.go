package avro

import (
	"reflect"
	"strings"
	"testing"
)

// A CustomType.Schema is an independently-authored schema tree with its own
// namespace scoping. SchemaFor embeds it into the tree it infers, so the
// composed schema must preserve every declared fullname exactly: the Avro
// spec ("Names") defines a type's identity as its FULLNAME, with the dotted
// name and the split name+namespace spellings denoting the same name, and
// bare references resolving in the namespace of the enclosing definition.
// These pins hold SchemaFor to that contract for the three composition
// shapes that exercise it: a namespaced type shared across fields (the
// second occurrence must reference the first by a spelling that re-binds to
// the same fullname), distinct fullnames sharing a short name (they must
// coexist), and a null-namespace type embedded under WithNamespace (its
// identity must not be captured by the surrounding namespace).

type scopePinMoney struct{ Cents int64 }

type scopePinTwoFields struct {
	F1 scopePinMoney
	F2 scopePinMoney
}

// customSchemaFor builds the CustomType wiring for a Schema-carrying custom
// used by the pins below: GoType matches the struct field, Schema supplies
// the emitted definition.
func customSchemaFor(t *testing.T, goType reflect.Type, schemaJSON string) CustomType {
	t.Helper()
	s, err := Parse(schemaJSON)
	if err != nil {
		t.Fatalf("parse custom schema: %v", err)
	}
	root := s.Root()
	return CustomType{GoType: goType, Schema: &root}
}

// namedFullname reports the fullname a field's type denotes: for a named
// definition it joins the declared namespace and name; for a name REFERENCE
// (which the metadata API surfaces as a bare node whose Type holds the
// reference spelling) it is the spelling itself — a dotted reference is a
// fullname, and a bare reference is emitted only where it equals the
// referent's null-namespace fullname.
func namedFullname(n SchemaNode) string {
	switch n.Type {
	case "record", "error", "enum", "fixed":
		if n.Namespace == "" || strings.Contains(n.Name, ".") {
			return n.Name
		}
		return n.Namespace + "." + n.Name
	case "null", "boolean", "int", "long", "float", "double", "string", "bytes", "array", "map", "union":
		return ""
	default:
		return n.Type
	}
}

// A namespaced custom schema written in the SPLIT spelling ("name":"X",
// "namespace":"a"), used on two fields: one definition plus a reference
// that re-binds to the same fullname a.X. The spec makes the split and
// dotted spellings the same name, so this must behave exactly like the
// dotted-control pin below.
func TestRegression_SchemaForCustomSchemaSplitNamespaceSharedType(t *testing.T) {
	ct := customSchemaFor(t, reflect.TypeFor[scopePinMoney](),
		`{"type":"record","name":"X","namespace":"a","fields":[{"name":"n","type":"int"}]}`)
	s, err := SchemaFor[scopePinTwoFields](ct)
	if err != nil {
		t.Fatalf("SchemaFor with a split-namespace custom schema on two fields: %v", err)
	}
	root := s.Root()
	for i := range root.Fields {
		if got := namedFullname(root.Fields[i].Type); got != "a.X" {
			t.Errorf("field %q type fullname = %q, want %q", root.Fields[i].Name, got, "a.X")
		}
	}
	if _, err := Parse(s.String()); err != nil {
		t.Errorf("SchemaFor output does not re-parse: %v", err)
	}
}

type scopePinOther struct{ N int32 }

type scopePinCoexist struct {
	F1 scopePinMoney
	F2 scopePinOther
}

// Distinct fullnames that share a short name — "a.X" (split spelling) and
// null-namespace "X" — are different Avro types and must coexist in one
// SchemaFor output with both identities intact.
func TestRegression_SchemaForCustomSchemaShortNameAcrossNamespaces(t *testing.T) {
	ctA := customSchemaFor(t, reflect.TypeFor[scopePinMoney](),
		`{"type":"record","name":"X","namespace":"a","fields":[{"name":"n","type":"int"}]}`)
	ctNull := customSchemaFor(t, reflect.TypeFor[scopePinOther](),
		`{"type":"record","name":"X","fields":[{"name":"n","type":"int"}]}`)
	s, err := SchemaFor[scopePinCoexist](ctA, ctNull)
	if err != nil {
		t.Fatalf("SchemaFor with fullnames a.X and X coexisting: %v", err)
	}
	root := s.Root()
	if got := namedFullname(root.Fields[0].Type); got != "a.X" {
		t.Errorf("field F1 type fullname = %q, want %q", got, "a.X")
	}
	if got := namedFullname(root.Fields[1].Type); got != "X" {
		t.Errorf("field F2 type fullname = %q, want %q", got, "X")
	}
	if _, err := Parse(s.String()); err != nil {
		t.Errorf("SchemaFor output does not re-parse: %v", err)
	}
}

type scopePinOneField struct {
	F1 scopePinMoney
}

// A null-namespace custom schema embedded under WithNamespace: the user's
// Schema declares fullname "X" (null namespace), and embedding it inside a
// namespaced record must not let namespace inheritance capture it into
// "b.X" — that would be a wire-visible identity change breaking resolution
// against the user's own schema. The emitted definition needs the
// "namespace":"" inheritance escape.
func TestRegression_SchemaForNullNamespaceCustomUnderWithNamespace(t *testing.T) {
	ct := customSchemaFor(t, reflect.TypeFor[scopePinMoney](),
		`{"type":"record","name":"X","fields":[{"name":"n","type":"int"}]}`)
	s, err := SchemaFor[scopePinOneField](WithNamespace("b"), ct)
	if err != nil {
		t.Fatalf("SchemaFor with a null-namespace custom under WithNamespace: %v", err)
	}
	f := s.Root().Fields[0].Type
	if f.Namespace != "" || strings.Contains(f.Name, ".") {
		t.Errorf("null-namespace custom type captured into namespace %q (name %q); the Schema declared null-namespace \"X\"", f.Namespace, f.Name)
	}
	if _, err := Parse(s.String()); err != nil {
		t.Errorf("SchemaFor output does not re-parse: %v", err)
	}
}

type scopePinRecursive struct{ Next *scopePinRecursive }

type scopePinFixed [4]byte

type scopePinTwoFixed struct {
	A scopePinFixed
	B scopePinFixed
}

// Control rows for the INFERENCE-side name spellings, which use a different
// mechanism than the custom-schema dedup: seen[] registers a record under
// its fullname, so a recursive struct's self-reference must be the DOTTED
// fullname (position-independent); an inferred fixed definition carries no
// namespace attribute (it inherits the SchemaFor namespace) and its repeat
// reference is the bare short name, which binds in that same inherited
// scope. Both spellings must survive a re-parse under WithNamespace.
func TestRegression_SchemaForInferenceNameSpellings(t *testing.T) {
	s, err := SchemaFor[scopePinRecursive](WithNamespace("ns"))
	if err != nil {
		t.Fatalf("recursive struct under WithNamespace: %v", err)
	}
	if !strings.Contains(s.String(), `"ns.scopePinRecursive"`) {
		t.Errorf("recursive self-reference is not the dotted fullname: %s", s.String())
	}
	if _, err := Parse(s.String()); err != nil {
		t.Errorf("recursive output does not re-parse: %v", err)
	}

	s, err = SchemaFor[scopePinTwoFixed](WithNamespace("ns"))
	if err != nil {
		t.Fatalf("repeated fixed under WithNamespace: %v", err)
	}
	root := s.Root()
	if got := namedFullname(root.Fields[0].Type); got != "ns.scopePinFixed" {
		t.Errorf("fixed definition fullname = %q, want %q", got, "ns.scopePinFixed")
	}
	if got := root.Fields[1].Type.Type; got != "scopePinFixed" {
		t.Errorf("fixed repeat reference = %q, want the bare short name binding in the inherited scope", got)
	}
	if _, err := Parse(s.String()); err != nil {
		t.Errorf("fixed output does not re-parse: %v", err)
	}
}

type scopePinTagged struct {
	F scopePinMoney `avro:"f,uuid"`
}

type scopePinTaggedDecimal struct {
	F scopePinMoney `avro:"f,decimal(9,2)"`
}

// A logical-type tag on a field whose type matches a CustomType has no
// effect — the custom supplies the schema — so accepting it would silently
// drop the user's tag, the exact lying-schema outcome the logical-tag
// strictness rejects everywhere else (a tag that cannot be honored is an
// error, mirroring the avro.Duration and uuid/decimal wrong-kind rejects).
func TestRegression_SchemaForLogicalTagOnCustomMatchedFieldRejected(t *testing.T) {
	ct := customSchemaFor(t, reflect.TypeFor[scopePinMoney](),
		`{"type":"record","name":"M","fields":[{"name":"c","type":"long"}]}`)
	if _, err := SchemaFor[scopePinTagged](ct); err == nil || !strings.Contains(err.Error(), "has no effect") {
		t.Errorf("uuid tag on a custom-matched field must be rejected, got: %v", err)
	}
	if _, err := SchemaFor[scopePinTaggedDecimal](ct); err == nil || !strings.Contains(err.Error(), "has no effect") {
		t.Errorf("decimal tag on a custom-matched field must be rejected, got: %v", err)
	}
	// Control: the same custom without a tag still builds.
	type plain struct{ F scopePinMoney }
	if _, err := SchemaFor[plain](ct); err != nil {
		t.Errorf("untagged custom-matched field must build: %v", err)
	}
}

// Control: the DOTTED spelling of the shared-type pin. The parser stores a
// dotted name verbatim, so this spelling worked before the split spelling
// did; it must keep working, and per the spec the two spellings must agree.
func TestRegression_SchemaForDottedCustomSchemaControl(t *testing.T) {
	ct := customSchemaFor(t, reflect.TypeFor[scopePinMoney](),
		`{"type":"record","name":"a.X","fields":[{"name":"n","type":"int"}]}`)
	s, err := SchemaFor[scopePinTwoFields](ct)
	if err != nil {
		t.Fatalf("SchemaFor with a dotted-name custom schema on two fields: %v", err)
	}
	root := s.Root()
	for i := range root.Fields {
		if got := namedFullname(root.Fields[i].Type); got != "a.X" {
			t.Errorf("field %q type fullname = %q, want %q", root.Fields[i].Name, got, "a.X")
		}
	}
	if _, err := Parse(s.String()); err != nil {
		t.Errorf("SchemaFor output does not re-parse: %v", err)
	}
}
