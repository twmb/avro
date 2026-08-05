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
		"SchemaNode": "refTarget (with refNS, the scope it was resolved in — the two are one stamp and are only meaningful together) is consulted only while the name resolver still binds the node's exported Type to it (nodeRefTargetAgrees), so an edited Type always wins — pinned by TestNodeRefSchema_EditedTypeIgnoresStaleStamp. " +
			"present is PRESENCE-ONLY and value-transparent: one bit per attribute whose body can be its own destination's zero, deciding whether such an attribute gets written at all, never what any attribute says — so the value a caller sets is the value that comes back for every input, pinned by TestInvariant_PresenceStateIsValueTransparent",
		"SchemaField": "docSet is the field-level twin of SchemaNode's, and carries the same proof: presence-only and value-transparent — pinned by TestInvariant_PresenceStateIsValueTransparent",
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

// TestInvariant_PresenceStateIsValueTransparent executes the claim that the
// presence flags cannot win over a caller.
//
// They differ in kind from refTarget, which selects a DEFINITION and so could
// substitute one schema for another. A presence flag decides one thing only:
// whether an attribute whose value is the field's own zero is written at all.
// It never chooses a value, so for every value a caller can set, the value
// that comes back is the value they set — with the flag set and with it
// clear. That is the property proved here, over both a node extracted from a
// parse (flags set) and the same node hand-composed (flags clear), including
// the case a caller cannot otherwise reach: clearing the field to "".
//
// The wire, the canonical form and the fingerprint must be identical across
// the pair as well: presence is a metadata-fidelity question, and none of
// those surfaces carries doc or logicalType at all.
func TestInvariant_PresenceStateIsValueTransparent(t *testing.T) {
	// extracted carries every presence flag set; composed carries none.
	extracted := MustParse(`{"type":"record","name":"R","doc":"","fields":[` +
		`{"name":"f","type":{"type":"int","logicalType":""},"doc":""}]}`).Root()
	if !extracted.present.has(presDoc) {
		t.Fatal("the extracted node did not record a written doc; the control is broken")
	}
	if !extracted.Fields[0].docSet {
		t.Fatal("the extracted field did not record a written doc; the control is broken")
	}
	if !extracted.Fields[0].Type.present.has(presLogicalType) {
		t.Fatal("the extracted type did not record a written logicalType; the control is broken")
	}

	for _, docValue := range []string{"", "x", "a longer doc string"} {
		for _, ltValue := range []string{"", "date", "not-a-logical"} {
			withState := extracted
			withState.Fields = append([]SchemaField(nil), extracted.Fields...)
			withState.Fields[0].Type = extracted.Fields[0].Type
			withState.Doc = docValue
			withState.Fields[0].Doc = docValue
			withState.Fields[0].Type.LogicalType = ltValue

			clean := SchemaNode{Type: "record", Name: "R", Doc: docValue,
				Fields: []SchemaField{{Name: "f", Doc: docValue,
					Type: SchemaNode{Type: "int", LogicalType: ltValue}}}}

			for label, n := range map[string]SchemaNode{"extracted": *withState, "composed": clean} {
				s, err := n.Schema()
				if err != nil {
					t.Fatalf("%s doc=%q lt=%q: Schema(): %v", label, docValue, ltValue, err)
				}
				back := s.Root()
				if back.Doc != docValue {
					t.Errorf("%s: node Doc set to %q came back %q — hidden state changed a caller's value",
						label, docValue, back.Doc)
				}
				if back.Fields[0].Doc != docValue {
					t.Errorf("%s: field Doc set to %q came back %q", label, docValue, back.Fields[0].Doc)
				}
				if back.Fields[0].Type.LogicalType != ltValue {
					t.Errorf("%s: LogicalType set to %q came back %q",
						label, ltValue, back.Fields[0].Type.LogicalType)
				}
			}

			// The surfaces presence must not reach.
			sa, err := withState.Schema()
			if err != nil {
				t.Fatalf("extracted Schema(): %v", err)
			}
			sb, err := clean.Schema()
			if err != nil {
				t.Fatalf("composed Schema(): %v", err)
			}
			if string(sa.Canonical()) != string(sb.Canonical()) {
				t.Errorf("presence state changed the canonical form:\n %s\n %s", sa.Canonical(), sb.Canonical())
			}
			if len(sa.Canonical()) == 0 {
				t.Fatal("canonical form came back empty, so the comparison proved nothing")
			}
			fa, fb := sa.Fingerprint(NewRabin()), sb.Fingerprint(NewRabin())
			if string(fa) != string(fb) {
				t.Errorf("presence state changed the fingerprint: %x vs %x", fa, fb)
			}
			val := map[string]any{"f": 0}
			ea, err := sa.AppendEncode(nil, val)
			if err != nil {
				t.Fatalf("extracted encode: %v", err)
			}
			eb, err := sb.AppendEncode(nil, val)
			if err != nil {
				t.Fatalf("composed encode: %v", err)
			}
			if string(ea) != string(eb) {
				t.Errorf("presence state changed the wire: %x vs %x", ea, eb)
			}
		}
	}
}
