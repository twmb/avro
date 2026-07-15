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

// SchemaFor builds on a private copy of a CustomType.Schema's rendered
// tree: the metadata walk hands Props container values over by reference
// when they need no JSON fixup, and the composition walkers (namespace
// pinning, named-type dedup) write into the tree they are given — so
// without the copy a build would write into the caller's own storage.
func TestRegression_SchemaForLeavesCallerSchemaStorageUnmutated(t *testing.T) {
	userOwned := map[string]any{"type": "fixed", "name": "F", "size": 1}
	want := map[string]any{"type": "fixed", "name": "F", "size": 1}
	ct := CustomType{
		GoType: reflect.TypeFor[scopePinMoney](),
		Schema: &SchemaNode{Type: "string", Props: map[string]any{"items": userOwned}},
	}
	if _, err := SchemaFor[scopePinOneField](WithNamespace("com.example"), ct); err != nil {
		t.Fatalf("build: %v", err)
	}
	if !reflect.DeepEqual(userOwned, want) {
		t.Fatalf("SchemaFor mutated caller-owned Props storage:\n got:  %v\n want: %v", userOwned, want)
	}
}

// Parse matches reserved attribute names case-insensitively (see
// Schema.Root's doc): a Props key differing from "namespace" only by ASCII
// case IS the namespace attribute. The SchemaFor composition walkers must
// apply the same fold — keying the dedup by the fullname Parse will bind —
// or the reference they emit for a second occurrence dangles.
func TestRegression_SchemaForCaseVariantNamespaceKeySharedType(t *testing.T) {
	ct := CustomType{
		GoType: reflect.TypeFor[scopePinMoney](),
		Schema: &SchemaNode{Type: "fixed", Name: "F", Size: 4, Props: map[string]any{"NAMESPACE": "x.y"}},
	}
	s, err := SchemaFor[scopePinTwoFields](ct)
	if err != nil {
		t.Fatalf("case-variant-namespaced custom on two fields: %v", err)
	}
	root := s.Root()
	for i := range root.Fields {
		if got := namedFullname(root.Fields[i].Type); got != "x.y.F" {
			t.Errorf("field %q type fullname = %q, want %q", root.Fields[i].Name, got, "x.y.F")
		}
	}
	if _, err := Parse(s.String()); err != nil {
		t.Errorf("SchemaFor output does not re-parse: %v", err)
	}
}

// Under WithNamespace the frontier pin must SEE a case-variant namespace
// key as the namespace declaration it is (Parse folds it onto the
// attribute) and leave the node alone: injecting an exact-case
// "namespace":"" would shadow the declared namespace at parse — a silent,
// wire-visible identity change (x.y.F would become F).
func TestRegression_SchemaForCaseVariantNamespaceUnderWithNamespace(t *testing.T) {
	ct := CustomType{
		GoType: reflect.TypeFor[scopePinMoney](),
		Schema: &SchemaNode{Type: "fixed", Name: "F", Size: 4, Props: map[string]any{"NAMESPACE": "x.y"}},
	}
	s, err := SchemaFor[scopePinOneField](WithNamespace("com.example"), ct)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if got := namedFullname(s.Root().Fields[0].Type); got != "x.y.F" {
		t.Errorf("fixed fullname = %q, want %q (the declared namespace must survive WithNamespace)", got, "x.y.F")
	}
}

// A CustomType.Schema whose rendered tree exceeds the schema-tree budgets
// must fail the build with the budget error. SchemaFor has an error
// channel, so the silent truncate-to-nil posture of the error-less
// surfaces (Schema.String, MarshalJSON) does not apply here: silently
// replacing an over-budget Props value with null would alter the user's
// schema, and the composed output still parses (a null prop is valid), so
// no downstream Parse catches it.
func TestRegression_SchemaForOverBudgetCustomSchemaErrors(t *testing.T) {
	huge := strings.Repeat("x", 1<<26+1024) // just over the tree byte budget
	ct := CustomType{
		GoType: reflect.TypeFor[scopePinMoney](),
		Schema: &SchemaNode{Type: "fixed", Name: "F", Size: 4, Props: map[string]any{"p": huge}},
	}
	if _, err := SchemaFor[scopePinOneField](ct); err == nil || !strings.Contains(err.Error(), "bytes") {
		t.Fatalf("over-budget custom schema must fail the build with the budget error, got: %v", err)
	}
}

// Every axis of the schema-tree walk budget must surface as a build error
// from SchemaFor, matching the error-reporting posture of SchemaNode.Schema
// (the same deduper-carrying walk): the BYTES axis (scalar payload), the
// NODES axis (emitted node count), and the unnamed-cycle detection. A
// modest schema stays well under every budget (the success control).
func TestRegression_SchemaForCustomSchemaBudgetAxes(t *testing.T) {
	build := func(node *SchemaNode) error {
		ct := CustomType{GoType: reflect.TypeFor[scopePinMoney](), Schema: node}
		_, err := SchemaFor[scopePinOneField](ct)
		return err
	}

	t.Run("bytes", func(t *testing.T) {
		huge := strings.Repeat("x", 1<<26+1024)
		err := build(&SchemaNode{Type: "fixed", Name: "F", Size: 4, Props: map[string]any{"p": huge}})
		if err == nil || !strings.Contains(err.Error(), "bytes") {
			t.Fatalf("bytes-axis overflow must fail the build with the budget error, got: %v", err)
		}
	})
	t.Run("nodes", func(t *testing.T) {
		wide := make([]any, 1<<20+1024)
		for i := range wide {
			wide[i] = 0
		}
		err := build(&SchemaNode{Type: "fixed", Name: "F", Size: 4, Props: map[string]any{"p": wide}})
		if err == nil || !strings.Contains(err.Error(), "nodes") {
			t.Fatalf("nodes-axis overflow must fail the build with the budget error, got: %v", err)
		}
	})
	t.Run("cycle", func(t *testing.T) {
		n := &SchemaNode{Type: "array"}
		n.Items = n
		err := build(n)
		if err == nil || !strings.Contains(err.Error(), "cyclic") {
			t.Fatalf("an unnamed pointer cycle must fail the build with the cycle error, got: %v", err)
		}
	})
	t.Run("control", func(t *testing.T) {
		if err := build(&SchemaNode{Type: "fixed", Name: "F", Size: 4,
			Props: map[string]any{"p": strings.Repeat("x", 1<<10)}}); err != nil {
			t.Fatalf("a modest custom schema must build: %v", err)
		}
	})
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
