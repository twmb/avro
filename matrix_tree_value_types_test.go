package avro

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
)

// The values a caller stores in SchemaNode Props / SchemaField.Default (and
// the trees CustomType.Schema hands to SchemaFor) reach Parse through one
// json.Marshal, so their SEMANTICS are defined by their marshal shape — a
// named Go type (`type M map[string]any`, `type A []string`, `type B
// []byte`, a named float) marshals identically to its canonical twin. Every
// pre-marshal consumer (the composition walkers, the render fixups, the
// aliases merge) must therefore treat the named twin exactly like the
// canonical type; the tests in this file pin that parity per consumer.

// TestRegression_TypeAliasAliasesValueGoTypes pins that the type-alias
// tag's merge into an existing aliases attribute does not depend on the
// attribute value's Go dynamic type: a named []string and a [N]string
// array marshal to the identical JSON array of strings that the []any /
// []string forms do, so the merged result must match the []any control.
// (Pre-canonicalization these fell through the merge untouched and Parse
// accepted the marshal — the tag's aliases silently vanished.)
func TestRegression_TypeAliasAliasesValueGoTypes(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	fields := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f,type-alias=Old"`}}
	build := func(t *testing.T, aliasesVal any) []string {
		t.Helper()
		node := &SchemaNode{Type: "fixed", Name: "F", Size: 4,
			Props: map[string]any{"aliases": aliasesVal}}
		s, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: node}})
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		return findNodeAliases(s.Root(), "F")
	}

	want := build(t, []any{"prior.P"})
	if len(want) != 2 {
		t.Fatalf("control aliases = %#v, want prior.P plus the tag's Old", want)
	}

	type namedStrings []string
	for _, tc := range []struct {
		name string
		v    any
	}{
		{"named_string_slice", namedStrings{"prior.P"}},
		{"string_array", [1]string{"prior.P"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := build(t, tc.v); !reflect.DeepEqual(got, want) {
				t.Errorf("aliases value %T: got %#v, want %#v", tc.v, got, want)
			}
		})
	}
}

// TestRegression_NamedMapItemsDefComposesCanonically pins that a
// Props-carried items definition composes identically whether its Go value
// is map[string]any or a named map type: both marshal to the same JSON
// object that Parse binds as the array's items, so the null-namespace pin
// at the custom frontier and the type-alias routing must see both. (Pre-
// canonicalization the named map was opaque: the "namespace":"" injection
// missed it — silently moving X into the build namespace — and the
// type-alias walk wrong-rejected.)
func TestRegression_NamedMapItemsDefComposesCanonically(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	type anyMap map[string]any
	def := func(wrap func(map[string]any) any) any {
		return wrap(map[string]any{"type": "record", "name": "X",
			"fields": []any{map[string]any{"name": "c", "type": "long"}}})
	}
	build := func(t *testing.T, tag, ns string, items any) (*Schema, error) {
		t.Helper()
		fields := []reflect.StructField{{Name: "F", Type: primary, Tag: reflect.StructTag(tag)}}
		node := &SchemaNode{Type: "array", Props: map[string]any{"items": items}}
		return schemaForScopeCell(t, fields, ns, []CustomType{{GoType: primary, Schema: node}})
	}

	t.Run("namespace_pin_parity", func(t *testing.T) {
		canon, err := build(t, `avro:"f"`, "com.x", def(func(m map[string]any) any { return m }))
		if err != nil {
			t.Fatalf("canonical build: %v", err)
		}
		cpcf := string(canon.Canonical())
		if !strings.Contains(cpcf, `"name":"X"`) {
			t.Fatalf("control lost the null-namespace pin on X: %s", cpcf)
		}
		named, err := build(t, `avro:"f"`, "com.x", def(func(m map[string]any) any { return anyMap(m) }))
		if err != nil {
			t.Fatalf("named-map build: %v", err)
		}
		if npcf := string(named.Canonical()); npcf != cpcf {
			t.Errorf("composed schema depends on the items value's Go dynamic type:\n map[string]any: %s\n named map:      %s", cpcf, npcf)
		}
	})

	t.Run("type_alias_verdict_parity", func(t *testing.T) {
		if _, err := build(t, `avro:"f,type-alias=Old"`, "", def(func(m map[string]any) any { return m })); err != nil {
			t.Fatalf("canonical build: %v", err)
		}
		if _, err := build(t, `avro:"f,type-alias=Old"`, "", def(func(m map[string]any) any { return anyMap(m) })); err != nil {
			t.Errorf("named-map items def wrong-rejects the type-alias tag: %v", err)
		}
	})
}

// TestRegression_NamedBytesPropsRebuildCodepointForm pins that a []byte
// Props value survives the SchemaNode.Schema() rebuild as the Avro
// codepoint-per-byte string regardless of the value's Go dynamic type. A
// named []byte reaching json.Marshal raw becomes base64 TEXT, which the
// re-parse reads as codepoints — silent content change.
func TestRegression_NamedBytesPropsRebuildCodepointForm(t *testing.T) {
	type namedBytes []byte
	build := func(t *testing.T, v any) any {
		t.Helper()
		s, err := (&SchemaNode{Type: "int", Props: map[string]any{"x": v}}).Schema()
		if err != nil {
			t.Fatalf("Schema(): %v", err)
		}
		return s.Root().Props["x"]
	}

	canon := build(t, []byte{0x01, 0x02, 0x03})
	if canon != "\x01\x02\x03" {
		t.Fatalf("control []byte Props = %#v, want the codepoint string", canon)
	}
	if named := build(t, namedBytes{0x01, 0x02, 0x03}); !reflect.DeepEqual(named, canon) {
		t.Errorf("named []byte Props rebuilds as %#v, canonical []byte as %#v", named, canon)
	}
}

// TestRegression_NamedFloatPropsRebuildSpecials pins the float-special
// fixups across Go dynamic types: the numeric-preserving conversions
// (-0.0, ±Inf) extend to named float kinds, while the type-CHANGING
// NaN→"NaN"-string conversion stays canonical-only — a NAMED float NaN
// keeps json.Marshal's loud unsupported-value error (never a silent
// stringification of a caller's own numeric type).
func TestRegression_NamedFloatPropsRebuildSpecials(t *testing.T) {
	type namedF64 float64
	build := func(t *testing.T, v any) (any, error) {
		t.Helper()
		s, err := (&SchemaNode{Type: "int", Props: map[string]any{"x": v}}).Schema()
		if err != nil {
			return nil, err
		}
		return s.Root().Props["x"], nil
	}

	t.Run("negative_zero", func(t *testing.T) {
		canon, err := build(t, math.Copysign(0, -1))
		if err != nil {
			t.Fatalf("canonical -0.0: %v", err)
		}
		cf, ok := canon.(float64)
		if !ok || cf != 0 || !math.Signbit(cf) {
			t.Fatalf("control -0.0 Props = %#v, want float64 negative zero", canon)
		}
		named, err := build(t, namedF64(math.Copysign(0, -1)))
		if err != nil {
			t.Fatalf("named -0.0: %v", err)
		}
		nf, ok := named.(float64)
		if !ok || nf != 0 || !math.Signbit(nf) {
			t.Errorf("named float -0.0 rebuilds as %#v; the sign must survive as it does for float64", named)
		}
	})

	t.Run("nan_posture", func(t *testing.T) {
		canon, err := build(t, math.NaN())
		if err != nil {
			t.Fatalf("canonical NaN: %v", err)
		}
		if canon != "NaN" {
			t.Fatalf("control NaN Props = %#v, want the documented \"NaN\" string", canon)
		}
		if got, err := build(t, namedF64(math.NaN())); err == nil {
			t.Errorf("named float NaN must keep the loud marshal error, got success with Props = %#v", got)
		}
	})
}

// TestRegression_NamedBytesFieldDefaultValue pins the same codepoint-form
// guarantee for a bytes FIELD DEFAULT — where corruption is wire-visible:
// the rebuilt default auto-fills for absent fields on JSON decode, so the
// materialized bytes must equal the caller's bytes for every Go dynamic
// type of the default value.
func TestRegression_NamedBytesFieldDefaultValue(t *testing.T) {
	type namedBytes []byte
	build := func(t *testing.T, v any) *Schema {
		t.Helper()
		s, err := (&SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{
			{Name: "b", Type: SchemaNode{Type: "bytes"}, Default: v},
		}}).Schema()
		if err != nil {
			t.Fatalf("Schema(): %v", err)
		}
		return s
	}

	want := []byte{0x01, 0x02, 0x03}
	canon := build(t, []byte{0x01, 0x02, 0x03})
	if got := canon.Root().Fields[0].Default; !reflect.DeepEqual(got, want) {
		t.Fatalf("control []byte default = %#v, want %#v", got, want)
	}
	named := build(t, namedBytes{0x01, 0x02, 0x03})
	if got := named.Root().Fields[0].Default; !reflect.DeepEqual(got, want) {
		t.Errorf("named []byte default rebuilds as %#v, want %#v", got, want)
	}

	var out map[string]any
	if err := named.DecodeJSON([]byte(`{}`), &out); err != nil {
		t.Fatalf("DecodeJSON default fill: %v", err)
	}
	if got, _ := out["b"].([]byte); !reflect.DeepEqual(got, want) {
		t.Errorf("default fill materialized %#v, want %#v", out["b"], want)
	}
}

// Marshal-opaque test types for the matrix below. Each has a CANONICALIZABLE
// kind (slice/map/string) so the exemption is observable: without it the
// kind canonicalization would rewrite the value and discard its marshal.
type tvAliasesMar []string

func (a tvAliasesMar) MarshalJSON() ([]byte, error) { return json.Marshal([]string(a)) }

type tvDefMar map[string]any

func (m tvDefMar) MarshalJSON() ([]byte, error) { return json.Marshal(map[string]any(m)) }

type tvStrMar string

func (s tvStrMar) MarshalJSON() ([]byte, error) { return json.Marshal(string(s) + "!") }

type tvTextStr string

func (s tvTextStr) MarshalText() ([]byte, error) { return []byte(string(s) + "?"), nil }

// TestMatrix_TreeValueGoTypes crosses the Go dynamic type of a caller
// value (Props / Default / rendered custom tree) with every pre-marshal
// consumer of that value. Oracle per cell family: the canonical-twin
// control is anchored to its expected value first, then each variant must
// match the control through a surface that CARRIES the attribute (Root()
// metadata, String(), or PCF where names are the observable — PCF strips
// aliases/doc/props, so those cells observe via Root()). Marshal-opaque
// values (own MarshalJSON/MarshalText) assert the EXEMPTION posture
// instead: their marshal wins, walkers and fixups leave them alone.
func TestMatrix_TreeValueGoTypes(t *testing.T) {
	type (
		namedStrings []string
		namedString  string
		namedSliceA  []any
		namedMap     map[string]any
		namedF64     float64
		namedF32     float32
		namedBytes   []byte
	)

	primary := reflect.TypeFor[scopeMatrixPrimary]()

	t.Run("aliases_merge", func(t *testing.T) {
		fields := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f,type-alias=Old"`}}
		build := func(t *testing.T, aliasesVal any) ([]string, error) {
			t.Helper()
			node := &SchemaNode{Type: "fixed", Name: "F", Size: 4,
				Props: map[string]any{"aliases": aliasesVal}}
			s, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: node}})
			if err != nil {
				return nil, err
			}
			return findNodeAliases(s.Root(), "F"), nil
		}

		want, err := build(t, []any{"prior.P"})
		if err != nil || !reflect.DeepEqual(want, []string{"prior.P", "Old"}) {
			t.Fatalf("control []any = %#v (%v), want [prior.P Old]", want, err)
		}

		for _, tc := range []struct {
			name string
			v    any
		}{
			{"string_slice", []string{"prior.P"}},
			{"named_string_slice", namedStrings{"prior.P"}},
			{"slice_of_named_string", []namedString{"prior.P"}},
			{"string_array", [1]string{"prior.P"}},
			{"named_slice_of_any", namedSliceA{"prior.P"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got, err := build(t, tc.v)
				if err != nil {
					t.Fatalf("build: %v", err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("aliases value %T: got %#v, want %#v", tc.v, got, want)
				}
			})
		}

		t.Run("json_marshaler_opaque", func(t *testing.T) {
			// A value carrying its own MarshalJSON stays opaque: the merge
			// leaves it alone (a merge would have to marshal it early), so
			// the tag alias is NOT added and the marshal's content is the
			// whole attribute.
			got, err := build(t, tvAliasesMar{"prior.P"})
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			if !reflect.DeepEqual(got, []string{"prior.P"}) {
				t.Errorf("marshal-opaque aliases value: got %#v, want the marshal's [prior.P] untouched", got)
			}
		})

		t.Run("text_marshaler_string_form", func(t *testing.T) {
			// A TextMarshaler at the aliases key marshals as a JSON STRING,
			// not an array — Parse rejects it loudly; never a silent drop.
			if _, err := build(t, tvTextStr("prior.P")); err == nil {
				t.Errorf("TextMarshaler aliases value marshals as a string; Parse must reject")
			}
		})
	})

	t.Run("namespace_pin", func(t *testing.T) {
		fields := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f"`}}
		xDef := func() map[string]any {
			return map[string]any{"type": "record", "name": "X",
				"fields": []any{map[string]any{"name": "c", "type": "long"}}}
		}
		build := func(t *testing.T, items any) string {
			t.Helper()
			node := &SchemaNode{Type: "array", Props: map[string]any{"items": items}}
			s, err := schemaForScopeCell(t, fields, "com.x", []CustomType{{GoType: primary, Schema: node}})
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			return string(s.Canonical())
		}

		control := build(t, xDef())
		if !strings.Contains(control, `"name":"X"`) {
			t.Fatalf("control lost the null-namespace pin on X: %s", control)
		}
		if got := build(t, namedMap(xDef())); got != control {
			t.Errorf("named-map items def composes differently:\n control: %s\n named:   %s", control, got)
		}

		t.Run("json_marshaler_opaque", func(t *testing.T) {
			// An object-emitting MarshalJSON def is opaque to the frontier
			// pin: Parse binds its marshal under the enclosing namespace, so
			// X lands in com.x — the documented residual for marshal-opaque
			// values (use canonical shapes to keep a null namespace).
			got := build(t, tvDefMar(xDef()))
			if !strings.Contains(got, `"name":"com.x.X"`) {
				t.Errorf("marshal-opaque items def: want X bound under com.x (pin stays out of its marshal), got %s", got)
			}
		})
	})

	t.Run("dedup", func(t *testing.T) {
		// Two fields sharing the custom: the named def must dedup to ONE
		// definition plus a reference, identically for canonical and named
		// map values.
		fields := []reflect.StructField{
			{Name: "F", Type: primary, Tag: `avro:"f"`},
			{Name: "G", Type: primary, Tag: `avro:"g"`},
		}
		xDef := func() map[string]any {
			return map[string]any{"type": "record", "name": "X",
				"fields": []any{map[string]any{"name": "c", "type": "long"}}}
		}
		build := func(t *testing.T, items any) string {
			t.Helper()
			node := &SchemaNode{Type: "array", Props: map[string]any{"items": items}}
			s, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: node}})
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			return string(s.Canonical())
		}

		control := build(t, xDef())
		if got := strings.Count(control, `"fields":[{"name":"c"`); got != 1 {
			t.Fatalf("control must contain exactly one X definition, got %d: %s", got, control)
		}
		if got := build(t, namedMap(xDef())); got != control {
			t.Errorf("named-map def dedups differently:\n control: %s\n named:   %s", control, got)
		}
	})

	t.Run("rebuild_props", func(t *testing.T) {
		build := func(t *testing.T, v any) (any, error) {
			t.Helper()
			s, err := (&SchemaNode{Type: "int", Props: map[string]any{"x": v}}).Schema()
			if err != nil {
				return nil, err
			}
			return s.Root().Props["x"], nil
		}
		parity := func(t *testing.T, control, variant any) {
			t.Helper()
			cv, err := build(t, control)
			if err != nil {
				t.Fatalf("control %T: %v", control, err)
			}
			nv, err := build(t, variant)
			if err != nil {
				t.Fatalf("variant %T: %v", variant, err)
			}
			if !reflect.DeepEqual(nv, cv) {
				t.Errorf("%T rebuilds as %#v, canonical %T as %#v", variant, nv, control, cv)
			}
		}

		t.Run("named_bytes", func(t *testing.T) { parity(t, []byte{1, 2, 3}, namedBytes{1, 2, 3}) })
		t.Run("byte_array_as_numbers", func(t *testing.T) { parity(t, []any{1, 2}, [2]byte{1, 2}) })
		t.Run("named_string", func(t *testing.T) { parity(t, "hello", namedString("hello")) })
		t.Run("named_map", func(t *testing.T) {
			parity(t, map[string]any{"k": "v"}, namedMap{"k": "v"})
		})
		t.Run("json_number_number_parity", func(t *testing.T) { parity(t, 1.5, json.Number("1.5")) })
		t.Run("named_f64_negzero", func(t *testing.T) {
			parity(t, math.Copysign(0, -1), namedF64(math.Copysign(0, -1)))
		})
		t.Run("named_f64_posinf", func(t *testing.T) { parity(t, math.Inf(1), namedF64(math.Inf(1))) })
		t.Run("named_f64_neginf", func(t *testing.T) { parity(t, math.Inf(-1), namedF64(math.Inf(-1))) })
		t.Run("named_f32_negzero", func(t *testing.T) {
			parity(t, float32(math.Copysign(0, -1)), namedF32(math.Copysign(0, -1)))
		})
		t.Run("named_f32_posinf", func(t *testing.T) {
			parity(t, float32(math.Inf(1)), namedF32(math.Inf(1)))
		})

		t.Run("raw_message_opaque", func(t *testing.T) {
			// json.RawMessage is []byte-kinded but carries MarshalJSON: its
			// raw JSON splices into the tree — the byte-string fixup must
			// never capture it.
			got, err := build(t, json.RawMessage(`{"a":1}`))
			if err != nil {
				t.Fatalf("RawMessage: %v", err)
			}
			want, err := build(t, map[string]any{"a": 1})
			if err != nil {
				t.Fatalf("map control: %v", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("RawMessage splices its JSON: got %#v, want %#v", got, want)
			}
		})
		t.Run("string_marshaler_opaque", func(t *testing.T) {
			// A string-kinded MarshalJSON carrier keeps its own marshal —
			// canonicalizing it to a plain string would silently drop the
			// method's output.
			got, err := build(t, tvStrMar("hi"))
			if err != nil {
				t.Fatalf("tvStrMar: %v", err)
			}
			if got != "hi!" {
				t.Errorf("MarshalJSON-carrying string: got %#v, want its marshal %q", got, "hi!")
			}
		})
		t.Run("text_marshaler_opaque", func(t *testing.T) {
			got, err := build(t, tvTextStr("hi"))
			if err != nil {
				t.Fatalf("tvTextStr: %v", err)
			}
			if got != "hi?" {
				t.Errorf("MarshalText-carrying string: got %#v, want its marshal %q", got, "hi?")
			}
		})
	})

	t.Run("rebuild_default", func(t *testing.T) {
		// An array-of-strings default: [N]string and []any marshal to the
		// same JSON array, so the rebuilt Default must match.
		build := func(t *testing.T, v any) any {
			t.Helper()
			s, err := (&SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{
				{Name: "a", Type: SchemaNode{Type: "array", Items: &SchemaNode{Type: "string"}}, Default: v},
			}}).Schema()
			if err != nil {
				t.Fatalf("Schema() %T: %v", v, err)
			}
			return s.Root().Fields[0].Default
		}
		control := build(t, []any{"a", "b"})
		if got := build(t, [2]string{"a", "b"}); !reflect.DeepEqual(got, control) {
			t.Errorf("[2]string default rebuilds as %#v, []any control as %#v", got, control)
		}
	})

	t.Run("string_render", func(t *testing.T) {
		// The String() render of a rebuilt schema is type-blind too.
		render := func(t *testing.T, v any) string {
			t.Helper()
			s, err := (&SchemaNode{Type: "int", Props: map[string]any{"x": v}}).Schema()
			if err != nil {
				t.Fatalf("Schema(): %v", err)
			}
			return s.String()
		}
		control := render(t, map[string]any{"k": "v"})
		if got := render(t, namedMap{"k": "v"}); got != control {
			t.Errorf("String() differs by Props value Go type:\n control: %s\n named:   %s", control, got)
		}
	})

	t.Run("render_props_marshaler", func(t *testing.T) {
		// A marshal-opaque scalar in a custom tree's Props keeps its own
		// marshal through the SchemaFor render — the canonicalizing copy
		// must not rewrite it into its kind's plain form.
		fields := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f"`}}
		node := &SchemaNode{Type: "fixed", Name: "F", Size: 4,
			Props: map[string]any{"x": tvStrMar("hi")}}
		s, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: node}})
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		if got := s.Root().Fields[0].Type.Props["x"]; got != "hi!" {
			t.Errorf("marshal-opaque Props scalar through the render: got %#v, want its marshal %q", got, "hi!")
		}
	})
}

// TestRegression_CyclicNamedMapPropsBudgetError pins the ordering the
// canonicalizing copy relies on: a cyclic Props value — including one
// hiding behind a NAMED map type, which the budget walk must descend by
// KIND — errors out of the budgeted metadata walk before the copy or
// json.Marshal ever see it. Success or a hang here would mean the walk's
// kind dispatch lost the named-container descent.
func TestRegression_CyclicNamedMapPropsBudgetError(t *testing.T) {
	type cycMap map[string]any
	m := cycMap{}
	m["self"] = m
	if _, err := (&SchemaNode{Type: "int", Props: map[string]any{"x": m}}).Schema(); err == nil {
		t.Fatalf("cyclic named-map Props: want the walk's budget error, got success")
	}
}
