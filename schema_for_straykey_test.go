package avro

import (
	"encoding/json"
	"reflect"
	"testing"
)

// Marker Go types for the stray-structural-key pins. Identity only matters
// within one test.
type (
	strayKeyCarrier struct{ X int64 }
	strayKeyRealA   struct{ Y int64 }
	strayKeyRealB   struct{ Z int64 }
)

// strayNXDef returns a fresh named-record definition tree, the shape a
// caller can legally park under a reserved structural key in Props (the
// parser captures such a key as inert metadata on kinds that do not bind
// it — see the structural-key routing on aobjectFromMap — so the value is
// never a name-binding definition).
func strayNXDef(fieldType string) map[string]any {
	return map[string]any{
		"type": "record", "name": "n.X",
		"fields": []any{map[string]any{"name": "a", "type": fieldType}},
	}
}

// realNXNode returns a caller SchemaNode defining n.X with an int field.
func realNXNode() *SchemaNode {
	return &SchemaNode{
		Type: "record", Name: "n.X",
		Fields: []SchemaField{{Name: "a", Type: SchemaNode{Type: "int"}}},
	}
}

// jsonReencode round-trips v through JSON so trees built with different
// container types (e.g. []any vs []map[string]any) and numeric widths
// compare structurally.
func jsonReencode(t *testing.T, v any) any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out
}

// strayUnderF1 extracts fields[0].type[key] from a schema's stored text.
func strayUnderF1(t *testing.T, s *Schema, key string) any {
	t.Helper()
	var root map[string]any
	if err := json.Unmarshal([]byte(s.String()), &root); err != nil {
		t.Fatalf("composed schema text does not unmarshal: %v", err)
	}
	fields, _ := root["fields"].([]any)
	if len(fields) == 0 {
		t.Fatalf("composed schema has no fields: %s", s.String())
	}
	f0, _ := fields[0].(map[string]any)
	typ, _ := f0["type"].(map[string]any)
	if typ == nil {
		t.Fatalf("composed F1 type is not an object: %s", s.String())
	}
	return typ[key]
}

// A named definition parked under a stray "items" key on a primitive-kind
// CustomType.Schema is inert to Parse (never name-bound), so it must not
// register in the composition's dedup table. Here the stray body differs
// from the real definition of the same fullname: the build must accept —
// the parser binds n.X exactly once, at F2 — not report a false duplicate.
func TestRegression_StrayStructuralKeyFalseDuplicate(t *testing.T) {
	stray := strayNXDef("long") // differs from the real def (int)
	ct1 := CustomType{
		GoType: reflect.TypeFor[strayKeyCarrier](),
		Schema: &SchemaNode{Type: "int", Props: map[string]any{"items": stray}},
	}
	ct2 := CustomType{GoType: reflect.TypeFor[strayKeyRealA](), Schema: realNXNode()}
	type S struct {
		F1 strayKeyCarrier
		F2 strayKeyRealA
	}

	counterfactual := `{"type":"record","name":"S","fields":[
		{"name":"F1","type":{"type":"int","items":{"type":"record","name":"n.X","fields":[{"name":"a","type":"long"}]}}},
		{"name":"F2","type":{"type":"record","name":"n.X","fields":[{"name":"a","type":"int"}]}}]}`
	if _, err := Parse(counterfactual); err != nil {
		t.Fatalf("hand-composed counterfactual does not parse — the stray is not inert as documented: %v", err)
	}

	s, err := SchemaFor[S](WithCustomType(ct1), WithCustomType(ct2))
	if err != nil {
		t.Fatalf("build rejected a composition whose composed tree is Parse-valid: %v", err)
	}
	if got, want := strayUnderF1(t, s, "items"), jsonReencode(t, strayNXDef("long")); !reflect.DeepEqual(got, want) {
		t.Errorf("stray value altered by composition:\n got:  %#v\n want: %#v", got, want)
	}
}

// Same shape with the stray body IDENTICAL to the real definition: the
// real definition at F2 must stay a full inline definition. Deduping it
// into a reference to the stray-parked copy dangles — Parse never binds a
// definition inside an inert stray.
func TestRegression_StrayStructuralKeyDanglingRef(t *testing.T) {
	stray := strayNXDef("int")
	ct1 := CustomType{
		GoType: reflect.TypeFor[strayKeyCarrier](),
		Schema: &SchemaNode{Type: "int", Props: map[string]any{"items": stray}},
	}
	ct2 := CustomType{GoType: reflect.TypeFor[strayKeyRealA](), Schema: realNXNode()}
	type S struct {
		F1 strayKeyCarrier
		F2 strayKeyRealA
	}

	counterfactual := `{"type":"record","name":"S","fields":[
		{"name":"F1","type":{"type":"int","items":{"type":"record","name":"n.X","fields":[{"name":"a","type":"int"}]}}},
		{"name":"F2","type":{"type":"record","name":"n.X","fields":[{"name":"a","type":"int"}]}}]}`
	if _, err := Parse(counterfactual); err != nil {
		t.Fatalf("hand-composed counterfactual does not parse — the stray is not inert as documented: %v", err)
	}

	s, err := SchemaFor[S](WithCustomType(ct1), WithCustomType(ct2))
	if err != nil {
		t.Fatalf("build rejected a composition whose composed tree is Parse-valid: %v", err)
	}
	if got, want := strayUnderF1(t, s, "items"), jsonReencode(t, strayNXDef("int")); !reflect.DeepEqual(got, want) {
		t.Errorf("stray value altered by composition:\n got:  %#v\n want: %#v", got, want)
	}
}

// Under WithNamespace, the scope pin must not inject "namespace":"" into a
// named-kind-shaped value inside an inert stray — the parser treats the
// stray as captured metadata, so the injection is a silent alteration of
// caller metadata in the stored schema text.
func TestRegression_StrayStructuralKeyPinInjection(t *testing.T) {
	stray := map[string]any{
		"type": "record", "name": "Bare",
		"fields": []any{map[string]any{"name": "a", "type": "int"}},
	}
	ct := CustomType{
		GoType: reflect.TypeFor[strayKeyCarrier](),
		Schema: &SchemaNode{Type: "int", Props: map[string]any{"items": stray}},
	}
	type S struct{ F1 strayKeyCarrier }
	s, err := SchemaFor[S](WithCustomType(ct), WithNamespace("x.y"))
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	want := jsonReencode(t, map[string]any{
		"type": "record", "name": "Bare",
		"fields": []any{map[string]any{"name": "a", "type": "int"}},
	})
	if got := strayUnderF1(t, s, "items"); !reflect.DeepEqual(got, want) {
		t.Errorf("stray value altered by composition (scope pin wrote into it):\n got:  %#v\n want: %#v", got, want)
	}
}

// Stray values are inert as-written metadata, so the dedup identity
// compare treats them VERBATIM: two same-fullname definitions differing
// only in the spelling of a stray value (dotted name vs name+namespace
// attribute, parked on an inner primitive where the parser accepts the
// stray as inert) are different definitions. Collapsing them to one would
// silently discard one spelling; keeping both inline cannot parse (a
// fullname defines once). The build must reject exactly as Parse rejects
// the inline pair.
func TestRegression_StrayStructuralKeyVerbatimCompare(t *testing.T) {
	strayAttr := map[string]any{"type": "record", "name": "Bare", "namespace": "q",
		"fields": []any{map[string]any{"name": "a", "type": "int"}}}
	strayDotted := map[string]any{"type": "record", "name": "q.Bare",
		"fields": []any{map[string]any{"name": "a", "type": "int"}}}

	nodeWith := func(stray map[string]any) *SchemaNode {
		return &SchemaNode{
			Type: "record", Name: "n.X",
			Fields: []SchemaField{
				{Name: "a", Type: SchemaNode{Type: "int"}},
				{Name: "b", Type: SchemaNode{Type: "int", Props: map[string]any{"items": stray}}},
			},
		}
	}
	ct1 := CustomType{GoType: reflect.TypeFor[strayKeyRealA](), Schema: nodeWith(strayAttr)}
	ct2 := CustomType{GoType: reflect.TypeFor[strayKeyRealB](), Schema: nodeWith(strayDotted)}

	// A single occurrence is fine: the stray on the inner int is inert.
	type S1 struct{ F1 strayKeyRealA }
	if _, err := SchemaFor[S1](WithCustomType(ct1)); err != nil {
		t.Fatalf("single-occurrence control failed — the stray is not inert where placed: %v", err)
	}

	// The inline pair cannot parse: n.X defines twice.
	counterfactual := `{"type":"record","name":"S","fields":[
		{"name":"F1","type":{"type":"record","name":"n.X","fields":[{"name":"a","type":"int"},{"name":"b","type":{"type":"int","items":{"type":"record","name":"Bare","namespace":"q","fields":[{"name":"a","type":"int"}]}}}]}},
		{"name":"F2","type":{"type":"record","name":"n.X","fields":[{"name":"a","type":"int"},{"name":"b","type":{"type":"int","items":{"type":"record","name":"q.Bare","fields":[{"name":"a","type":"int"}]}}}]}}]}`
	if _, err := Parse(counterfactual); err == nil {
		t.Fatalf("counterfactual inline pair unexpectedly parsed; the scenario premise is wrong")
	}

	type S struct {
		F1 strayKeyRealA
		F2 strayKeyRealB
	}
	if _, err := SchemaFor[S](WithCustomType(ct1), WithCustomType(ct2)); err == nil {
		t.Fatalf("build accepted two same-fullname definitions differing in an inert stray value's as-written spelling; the parser cannot represent both")
	}
}

// Control: the identical builds without the stray succeed.
func TestStrayStructuralKeyControl(t *testing.T) {
	ct1 := CustomType{GoType: reflect.TypeFor[strayKeyCarrier](), Schema: &SchemaNode{Type: "int"}}
	ct2 := CustomType{GoType: reflect.TypeFor[strayKeyRealA](), Schema: realNXNode()}
	type S struct {
		F1 strayKeyCarrier
		F2 strayKeyRealA
	}
	if _, err := SchemaFor[S](WithCustomType(ct1), WithCustomType(ct2)); err != nil {
		t.Fatalf("no-stray control failed: %v", err)
	}
	type S2 struct{ F1 strayKeyRealA }
	if _, err := SchemaFor[S2](WithCustomType(ct2), WithNamespace("x.y")); err != nil {
		t.Fatalf("no-stray namespaced control failed: %v", err)
	}
}

// strayMatrixThird is the matrix's third marker type (alongside
// scopeMatrixPrimary / scopeMatrixPartner) so one cell can carry a
// stray-key custom plus two same-definition customs.
type strayMatrixThird struct{ C int64 }

// TestMatrix_SchemaForStrayStructuralKey crosses every carrier kind with
// every structural key the kind does NOT bind, a spread of stray bodies,
// both build scopes, and one-vs-two occurrences of a genuine same-fullname
// definition. The per-cell oracle is the parser itself:
//
//   - verdict parity: SchemaFor's accept/reject equals Parse's verdict on
//     the hand-composed counterfactual tree carrying the same stray
//     verbatim (kind-keyed grammar: a container kind carrying another
//     kind's defining key hard-rejects; a primitive captures it inert);
//   - preservation: on accepted cells the stray survives byte-identical
//     in the composed schema text — never walked, never rewritten, never
//     injected into;
//   - genuine behavior: the real definition stays a full inline body and
//     a second occurrence still dedups to a name reference.
//
// The cells where the key IS the kind's defining key (array items, map
// values, record fields) are the genuine-schema controls pinned by the
// scope and casefold matrices, so they are skipped here.
func TestMatrix_SchemaForStrayStructuralKey(t *testing.T) {
	// bodyJSON builds a FRESH tree per call: the planted copy and the
	// counterfactual copy must be independent so a (hypothetically)
	// misbehaving walker mutating one cannot corrupt the other's oracle.
	bodyJSON := func(body string) any {
		switch body {
		case "identdef":
			return map[string]any{"type": "record", "name": "n.X",
				"fields": []any{map[string]any{"name": "a", "type": "int"}}}
		case "diffdef":
			return map[string]any{"type": "record", "name": "n.X",
				"fields": []any{map[string]any{"name": "a", "type": "long"}}}
		case "baredef":
			// Bare-named: the shape the scope pin's injection arm targets
			// (a dotted name pins its own scope and is skipped).
			return map[string]any{"type": "record", "name": "Bare",
				"fields": []any{map[string]any{"name": "a", "type": "int"}}}
		case "plain":
			return map[string]any{"type": "array", "items": "long"}
		case "nonschema":
			return 42
		}
		return nil
	}

	carrierNode := func(kind string) *SchemaNode {
		switch kind {
		case "fixed":
			return &SchemaNode{Type: "fixed", Name: "FX", Size: 2}
		case "enum":
			return &SchemaNode{Type: "enum", Name: "EN", Symbols: []string{"A"}}
		case "record":
			return &SchemaNode{Type: "record", Name: "RC",
				Fields: []SchemaField{{Name: "a", Type: SchemaNode{Type: "int"}}}}
		case "array":
			return &SchemaNode{Type: "array", Items: &SchemaNode{Type: "int"}}
		case "map":
			return &SchemaNode{Type: "map", Values: &SchemaNode{Type: "int"}}
		}
		return &SchemaNode{Type: kind}
	}
	carrierJSON := func(kind string) map[string]any {
		switch kind {
		case "fixed":
			return map[string]any{"type": "fixed", "name": "FX", "size": 2}
		case "enum":
			return map[string]any{"type": "enum", "name": "EN", "symbols": []any{"A"}}
		case "record":
			return map[string]any{"type": "record", "name": "RC",
				"fields": []any{map[string]any{"name": "a", "type": "int"}}}
		case "array":
			return map[string]any{"type": "array", "items": "int"}
		case "map":
			return map[string]any{"type": "map", "values": "int"}
		}
		return map[string]any{"type": kind}
	}

	definingKey := map[string]string{"array": "items", "map": "values", "record": "fields"}

	for _, kind := range []string{"int", "string", "fixed", "enum", "record", "array", "map"} {
		for _, key := range []string{"items", "values", "fields"} {
			if definingKey[kind] == key {
				continue // the genuine schema position, not a stray
			}
			for _, body := range []string{"identdef", "diffdef", "baredef", "plain", "nonschema"} {
				for _, ns := range []string{"", "b"} {
					for _, occ := range []int{1, 2} {
						name := kind + "/" + key + "/" + body + "/occ" + string(rune('0'+occ))
						if ns != "" {
							name += "/ns"
						}
						t.Run(name, func(t *testing.T) {
							// Plant the stray through Props — the caller's
							// route for a key the node's kind does not bind
							// (the render emits Props keys verbatim; typed
							// Items/Values/Fields on a bare primitive are
							// dropped by the render's defined-placement
							// posture, so Props is the reachable carrier).
							// A "fields" stray wraps its body in a proper
							// field list so the stray itself decodes.
							strayFor := func() any {
								switch {
								case body == "nonschema":
									return 42
								case key == "fields":
									return []any{map[string]any{"name": "f", "type": bodyJSON(body)}}
								}
								return bodyJSON(body)
							}
							carrier := carrierNode(kind)
							carrier.Props = map[string]any{key: strayFor()}
							strayJSON := strayFor()

							customs := []CustomType{
								{GoType: reflect.TypeFor[scopeMatrixPrimary](), Schema: carrier},
								{GoType: reflect.TypeFor[scopeMatrixPartner](), Schema: realNXNode()},
							}
							fields := []reflect.StructField{
								{Name: "F1", Type: reflect.TypeFor[scopeMatrixPrimary]()},
								{Name: "F2", Type: reflect.TypeFor[scopeMatrixPartner]()},
							}
							if occ == 2 {
								customs = append(customs,
									CustomType{GoType: reflect.TypeFor[strayMatrixThird](), Schema: realNXNode()})
								fields = append(fields,
									reflect.StructField{Name: "F3", Type: reflect.TypeFor[strayMatrixThird]()})
							}

							// Hand-composed counterfactual: same carrier +
							// stray verbatim, the real definition inline
							// once, a reference at the second occurrence.
							cfCarrier := carrierJSON(kind)
							cfCarrier[key] = strayJSON
							cfFields := []any{
								map[string]any{"name": "F1", "type": cfCarrier},
								map[string]any{"name": "F2", "type": bodyJSON("identdef")},
							}
							if occ == 2 {
								cfFields = append(cfFields, map[string]any{"name": "F3", "type": "n.X"})
							}
							cfRoot := map[string]any{"type": "record", "name": "Top", "fields": cfFields}
							if ns != "" {
								cfRoot["namespace"] = ns
							}
							cfText, err := json.Marshal(cfRoot)
							if err != nil {
								t.Fatalf("marshal counterfactual: %v", err)
							}
							_, cfErr := Parse(string(cfText))

							s, err := schemaForScopeCell(t, fields, ns, customs)
							if (err == nil) != (cfErr == nil) {
								t.Fatalf("verdict parity broken:\n build: %v\n parse of counterfactual: %v", err, cfErr)
							}
							if err != nil {
								return // reject cell: parity established
							}

							var root map[string]any
							if err := json.Unmarshal([]byte(s.String()), &root); err != nil {
								t.Fatalf("composed text: %v", err)
							}
							composed, _ := root["fields"].([]any)
							if len(composed) < 2 {
								t.Fatalf("composed fields missing: %s", s.String())
							}
							f1type, _ := composed[0].(map[string]any)["type"].(map[string]any)
							if f1type == nil {
								t.Fatalf("composed F1 type not an object: %s", s.String())
							}
							if got, want := f1type[key], jsonReencode(t, strayJSON); !reflect.DeepEqual(got, want) {
								t.Errorf("stray not preserved verbatim:\n got:  %#v\n want: %#v", got, want)
							}
							f2type, _ := composed[1].(map[string]any)["type"].(map[string]any)
							if f2type == nil || f2type["name"] != "n.X" {
								t.Errorf("real definition not inline at F2: %s", s.String())
							}
							if occ == 2 {
								if ref, _ := composed[2].(map[string]any)["type"].(string); ref != "n.X" {
									t.Errorf("second genuine occurrence did not dedup to a reference: %s", s.String())
								}
							}
						})
					}
				}
			}
		}
	}
}

type strayKeyFixed16 [16]byte

// A build never writes into caller-owned SchemaNode storage — including
// the [len:cap) region of a caller's Aliases backing array, which a
// deep-equal snapshot cannot see. The type-alias tag appends to the
// rendered type's aliases; with spare capacity in the caller's slice that
// append must land in the build's own copy, not the caller's array.
func TestRegression_TypeAliasSpareCapacityOwnership(t *testing.T) {
	backing := []string{"Old", "KeepMe"}
	node := &SchemaNode{
		Type: "fixed", Name: "F16", Size: 16,
		Aliases: backing[:1:2], // len 1, cap 2: one spare slot over caller memory
	}
	ct := CustomType{GoType: reflect.TypeFor[strayKeyFixed16](), Schema: node}
	type S struct {
		F strayKeyFixed16 `avro:",type-alias=NewAlias"`
	}
	s, err := SchemaFor[S](WithCustomType(ct))
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	if backing[1] != "KeepMe" {
		t.Fatalf("build wrote past len into the caller's aliases backing array: %q", backing[1])
	}
	// The tag still applies: the composed type carries both aliases.
	var root map[string]any
	if err := json.Unmarshal([]byte(s.String()), &root); err != nil {
		t.Fatalf("unmarshal composed: %v", err)
	}
	fields := root["fields"].([]any)
	typ := fields[0].(map[string]any)["type"].(map[string]any)
	if got := typ["aliases"]; !reflect.DeepEqual(got, []any{"Old", "NewAlias"}) {
		t.Fatalf("composed aliases = %#v, want [Old NewAlias]", got)
	}
}

// TestMatrix_TypeAliasAliasOwnership drives a type-alias'd field through
// every named kind whose CustomType.Schema carries caller []string inputs
// (type aliases; enum symbols; record field aliases), at both build
// scopes. The harness plants a sentinel past the length of every such
// slice, so any append the build makes into caller-owned backing memory —
// rather than into its own copy — fails the cell; the composed type must
// still carry the declared aliases plus the tag alias.
func TestMatrix_TypeAliasAliasOwnership(t *testing.T) {
	for _, kind := range []string{"fixed", "enum", "record"} {
		for _, ns := range []string{"", "b"} {
			name := kind
			if ns != "" {
				name += "/ns"
			}
			t.Run(name, func(t *testing.T) {
				var node *SchemaNode
				var declared string
				switch kind {
				case "fixed":
					declared = "OldF"
					node = &SchemaNode{Type: "fixed", Name: "FX", Size: 2,
						Aliases: []string{declared}}
				case "enum":
					declared = "OldE"
					node = &SchemaNode{Type: "enum", Name: "EN", Symbols: []string{"A"},
						Aliases: []string{declared}}
				case "record":
					declared = "OldR"
					node = &SchemaNode{Type: "record", Name: "RC",
						Aliases: []string{declared},
						Fields: []SchemaField{{Name: "a", Type: SchemaNode{Type: "int"},
							Aliases: []string{"olda"}}}}
				}
				customs := []CustomType{{GoType: reflect.TypeFor[scopeMatrixPrimary](), Schema: node}}
				fields := []reflect.StructField{{
					Name: "F1",
					Type: reflect.TypeFor[scopeMatrixPrimary](),
					Tag:  `avro:",type-alias=Extra"`,
				}}
				s, err := schemaForScopeCell(t, fields, ns, customs)
				if err != nil {
					t.Fatalf("build failed: %v", err)
				}
				var root map[string]any
				if err := json.Unmarshal([]byte(s.String()), &root); err != nil {
					t.Fatalf("composed text: %v", err)
				}
				typ := root["fields"].([]any)[0].(map[string]any)["type"].(map[string]any)
				if got := typ["aliases"]; !reflect.DeepEqual(got, []any{declared, "Extra"}) {
					t.Errorf("composed aliases = %#v, want [%s Extra]", got, declared)
				}
			})
		}
	}
}
