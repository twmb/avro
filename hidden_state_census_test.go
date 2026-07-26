package avro

// Hidden state on a user-composable public struct is a correctness hazard:
// a caller who sets an exported field expects that field to decide the
// outcome, so unexported state must never silently win over it. A struct is
// "user-composable" only if it has exported fields a caller sets; the rest
// carry unexported state that no caller can contradict.
//
// This census freezes the enumeration. When a new exported struct gains both
// exported fields and unexported state, this test fails and forces the same
// analysis rather than letting the hazard land unexamined.

import (
	"reflect"
	"strings"
	"testing"
)

type myMillis int64

func fieldSplit(t reflect.Type) (exported, hidden []string) {
	for i := range t.NumField() {
		f := t.Field(i)
		if f.IsExported() {
			exported = append(exported, f.Name)
		} else {
			hidden = append(hidden, f.Name)
		}
	}
	return
}

func TestInvariant_HiddenStateOnPublicStructs(t *testing.T) {
	// Every exported struct type in the package (and the two in ocf are
	// covered by the same reasoning: zero exported fields).
	types := []reflect.Type{
		reflect.TypeFor[Schema](),
		reflect.TypeFor[SchemaNode](),
		reflect.TypeFor[SchemaField](),
		reflect.TypeFor[SchemaCache](),
		reflect.TypeFor[CustomType](),
		reflect.TypeFor[SemanticError](),
		reflect.TypeFor[CompatibilityError](),
		reflect.TypeFor[ShortBufferError](),
		reflect.TypeFor[Duration](),
	}
	// The ONLY types where a caller-set exported field coexists with
	// unexported state. Each is justified below and pinned by a behavior
	// test; adding a name here requires doing the same.
	composableWithHiddenState := map[string]string{
		"CustomType": "needsAvroType is fail-loud only: it can make Parse REJECT (when AvroType is empty), never silently substitute a value — pinned by TestInvariant_CustomTypeHiddenStateFailsLoud",
		"SchemaNode": "refTarget (with refNS, the scope it was resolved in — the two are one stamp and are only meaningful together) is consulted only while the name resolver still binds the node's exported Type to it (nodeRefTargetAgrees), so an edited Type always wins — pinned by TestNodeRefSchema_EditedTypeIgnoresStaleStamp",
	}
	for _, ty := range types {
		exported, hidden := fieldSplit(ty)
		if len(hidden) == 0 || len(exported) == 0 {
			continue // not user-composable, or no hidden state: no hazard
		}
		why, ok := composableWithHiddenState[ty.Name()]
		if !ok {
			t.Errorf("%s has BOTH exported fields (%v) and unexported state (%v), so hidden state could silently override a caller's edit. Prove it cannot, add it to composableWithHiddenState with the reason, and pin the behavior.",
				ty.Name(), exported, hidden)
			continue
		}
		t.Logf("%-12s exported=%d hidden=%v — %s", ty.Name(), len(exported), hidden, why)
	}
	// The census must not silently go vacuous if the types list rots.
	if len(types) < 9 {
		t.Fatal("types list shrank; the census only covers what it lists")
	}
}

// TestInvariant_CustomTypeHiddenStateFailsLoud executes the claim that
// CustomType's one unexported field cannot silently win: NewCustomType sets
// needsAvroType, and a caller who copies that value and clears the exported
// AvroType gets a loud parse error rather than a stale conversion.
func TestInvariant_CustomTypeHiddenStateFailsLoud(t *testing.T) {
	ct := NewCustomType("",
		func(v myMillis, _ *SchemaNode) (int64, error) { return int64(v), nil },
		func(v int64, _ *SchemaNode) (myMillis, error) { return myMillis(v), nil },
	)
	if ct.AvroType == "" {
		// NewCustomType infers the Avro type from the Go types; if it did
		// not set one, the "cleared" case below is not distinguishable.
		t.Skip("NewCustomType did not infer an AvroType for this pair")
	}
	cleared := ct // struct copy: carries needsAvroType
	cleared.AvroType = ""
	_, err := Parse(`"string"`, cleared)
	if err == nil {
		t.Fatal("clearing AvroType on a NewCustomType value parsed silently; the hidden needsAvroType must make this fail loudly")
	}
	if !strings.Contains(err.Error(), "unsupported Avro native type") {
		t.Fatalf("unexpected error %v; want the loud unsupported-Avro-native-type reject", err)
	}
	t.Logf("fails loud as required: %v", err)
}
