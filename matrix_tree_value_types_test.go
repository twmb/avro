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
	// The SchemaFor render shares the budgeted walk and must error before
	// its canonicalizing copy (which recurses unbudgeted) can see the cycle.
	node := &SchemaNode{Type: "fixed", Name: "F", Size: 4, Props: map[string]any{"x": m}}
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	fields := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f"`}}
	if _, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: node}}); err == nil {
		t.Fatalf("cyclic named-map Props through the SchemaFor render: want the walk's budget error, got success")
	}
}

// ---------------------------------------------------------------------------
// The caller-value domain, enumerated. Arbitrary Go values enter the tree in
// exactly three positions — SchemaNode.Props values, SchemaField.Default,
// SchemaField.Props values (plus whole trees of those via CustomType.Schema,
// and again via mutating a Schema.Root() result) — and are consumed
// pre-marshal by exactly two pipelines: the Schema()/String()/Root() rebuild
// (budget walk → JSON fixups → json.Marshal → Parse) and the SchemaFor
// render (the same walk plus the canonicalizing copy and the composition
// walkers). The invariant the cells below pin: the composed schema is a
// function of the value's MARSHAL IMAGE, never of its Go representation —
// two values with identical json.Marshal output must produce identical
// observable results — except where the marshal is the value author's
// contract (own MarshalJSON/MarshalText, json.Number) or a documented fixup
// owns the image (the []byte codepoint form, ±Inf, −0.0, the canonical-only
// NaN string). Controls are anchored to executed values before any twin
// diff, so a cell cannot pass vacuously.

type (
	tvNamedBool    bool
	tvNamedI8      int8
	tvNamedInt     int
	tvNamedU64     uint64
	tvNamedF32     float32
	tvNamedF64     float64
	tvNamedBytes   []byte
	tvNamedStrings []string
	tvNamedMap     map[string]any
	tvNamedSlice   []any
	tvNamedString  string
)

// treeValuePropsObserved composes v as a Props value through the given
// surface and returns the observed metadata value: the direct
// SchemaNode.Schema() rebuild, or the SchemaFor render of a custom tree
// (which adds the canonicalizing copy and the composition walkers).
func treeValuePropsObserved(t *testing.T, surface string, v any) (any, error) {
	t.Helper()
	if surface == "rebuild" {
		s, err := (&SchemaNode{Type: "int", Props: map[string]any{"x": v}}).Schema()
		if err != nil {
			return nil, err
		}
		return s.Root().Props["x"], nil
	}
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	fields := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f"`}}
	node := &SchemaNode{Type: "fixed", Name: "F", Size: 4, Props: map[string]any{"x": v}}
	s, err := schemaForScopeCell(t, fields, "", []CustomType{{GoType: primary, Schema: node}})
	if err != nil {
		return nil, err
	}
	return s.Root().Fields[0].Type.Props["x"], nil
}

var treeValueSurfaces = []string{"rebuild", "schemafor"}

// TestMatrix_TreeValueLeafTwins crosses leaf-value Go dynamic types with
// both composition surfaces: every variant marshals identically to its
// row's control, so the observed Props value must be identical too. The
// control anchors to the documented read-back contract first
// (SchemaNode.Props: int64 for whole numbers, float64 for fractional,
// json.Number only past int64's range, the []byte codepoint string form).
func TestMatrix_TreeValueLeafTwins(t *testing.T) {
	rows := []struct {
		name     string
		expect   any // anchored control read-back
		control  any
		variants []any
		image    string // non-empty: assert every value's marshal image first
	}{
		{name: "named_bool", expect: true, control: true,
			variants: []any{tvNamedBool(true)}},
		{name: "int_widths_42", expect: int64(42), control: int64(42),
			variants: []any{int8(42), int16(42), int32(42), int(42),
				uint8(42), uint16(42), uint32(42), uint(42), uint64(42),
				json.Number("42"), tvNamedI8(42), tvNamedInt(42)},
			image: "42"},
		{name: "int64_max", expect: int64(math.MaxInt64), control: int64(math.MaxInt64),
			variants: []any{json.Number("9223372036854775807")}},
		{name: "int64_past_float53", expect: int64(1<<53 + 1), control: int64(1<<53 + 1),
			variants: []any{json.Number("9007199254740993")}},
		{name: "uint64_max", expect: json.Number("18446744073709551615"),
			control:  json.Number("18446744073709551615"),
			variants: []any{uint64(math.MaxUint64), tvNamedU64(math.MaxUint64)}},
		{name: "float_tenth", expect: float64(0.1), control: float64(0.1),
			variants: []any{float32(0.1), tvNamedF32(0.1), tvNamedF64(0.1)},
			image:    "0.1"},
		{name: "empty_json_number_is_zero", expect: int64(0), control: int64(0),
			variants: []any{json.Number("")}},
		{name: "typed_nils", expect: nil, control: nil,
			variants: []any{(*int)(nil), json.RawMessage(nil)}},
		{name: "nil_bytes_empty_codepoint", expect: "", control: []byte(nil),
			variants: []any{tvNamedBytes(nil)}},
		{name: "empty_map", expect: map[string]any{}, control: map[string]any{},
			variants: []any{tvNamedMap{}}},
		{name: "empty_slice", expect: []any{}, control: []any{},
			variants: []any{[0]string{}, tvNamedStrings{}, tvNamedSlice{}}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			if row.image != "" {
				for _, v := range append([]any{row.control}, row.variants...) {
					b, err := json.Marshal(v)
					if err != nil || string(b) != row.image {
						t.Fatalf("twin premise: %T marshals %s (%v), want %s", v, b, err, row.image)
					}
				}
			}
			for _, surface := range treeValueSurfaces {
				control, err := treeValuePropsObserved(t, surface, row.control)
				if err != nil {
					t.Fatalf("%s control %T: %v", surface, row.control, err)
				}
				if !reflect.DeepEqual(control, row.expect) {
					t.Fatalf("%s anchored control: got %#v, want %#v", surface, control, row.expect)
				}
				for _, v := range row.variants {
					got, err := treeValuePropsObserved(t, surface, v)
					if err != nil {
						t.Fatalf("%s %T: %v", surface, v, err)
					}
					if !reflect.DeepEqual(got, control) {
						t.Errorf("%s: %T observed %#v, control %T observed %#v",
							surface, v, got, row.control, control)
					}
				}
			}
		})
	}
}

// TestMatrix_TreeValueContainerTwins: container shapes whose marshal images
// coincide must compose identically through both surfaces, including
// fixup-carrying content under named or array wrappers, at any nesting
// depth.
func TestMatrix_TreeValueContainerTwins(t *testing.T) {
	rows := []struct {
		name          string
		control, twin any
		image         bool // both values marshal; assert identical images
	}{
		{name: "deep_named_nesting",
			control: map[string]any{
				"bs":   []any{[]byte{9}, []byte{8}},
				"deep": map[string]any{"ss": []any{"a"}, "m": map[string]any{"b": []byte{7}}},
			},
			twin: tvNamedMap{
				"bs":   []tvNamedBytes{{9}, {8}},
				"deep": tvNamedMap{"ss": tvNamedStrings{"a"}, "m": tvNamedMap{"b": tvNamedBytes{7}}},
			},
			image: true},
		{name: "slice_of_named_bytes",
			control: []any{[]byte{9}, []byte{8}}, twin: []tvNamedBytes{{9}, {8}}, image: true},
		{name: "array_carrying_inf",
			control: []any{math.Inf(1), "x"}, twin: [2]any{math.Inf(1), "x"}},
		{name: "one_elem_string_array",
			control: []any{"a"}, twin: [1]string{"a"}, image: true},
		{name: "array_of_named_string",
			control: []any{"a", "b"}, twin: [2]tvNamedString{"a", "b"}, image: true},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			if row.image {
				cb, cerr := json.Marshal(row.control)
				tb, terr := json.Marshal(row.twin)
				if cerr != nil || terr != nil || string(cb) != string(tb) {
					t.Fatalf("twin premise: images differ or fail: %s (%v) vs %s (%v)", cb, cerr, tb, terr)
				}
			}
			var acrossSurfaces []any
			for _, surface := range treeValueSurfaces {
				control, err := treeValuePropsObserved(t, surface, row.control)
				if err != nil {
					t.Fatalf("%s control: %v", surface, err)
				}
				if control == nil {
					t.Fatalf("%s control observed nil; the anchor is gone", surface)
				}
				got, err := treeValuePropsObserved(t, surface, row.twin)
				if err != nil {
					t.Fatalf("%s twin: %v", surface, err)
				}
				if !reflect.DeepEqual(got, control) {
					t.Errorf("%s: twin observed %#v, control %#v", surface, got, control)
				}
				acrossSurfaces = append(acrossSurfaces, control)
			}
			if !reflect.DeepEqual(acrossSurfaces[0], acrossSurfaces[1]) {
				t.Errorf("surfaces disagree on the control: rebuild %#v, schemafor %#v",
					acrossSurfaces[0], acrossSurfaces[1])
			}
		})
	}
}

// TestMatrix_TreeValueDefaultWire: field defaults with identical marshal
// images must materialize identical auto-filled values on JSON decode of an
// input missing the field — the wire-visible consequence of the composed
// default.
func TestMatrix_TreeValueDefaultWire(t *testing.T) {
	fill := func(t *testing.T, fieldType SchemaNode, def any) any {
		t.Helper()
		s, err := (&SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{
			{Name: "v", Type: fieldType, Default: def, HasDefault: true},
		}}).Schema()
		if err != nil {
			t.Fatalf("Schema() with %T default: %v", def, err)
		}
		var out map[string]any
		if err := s.DecodeJSON([]byte(`{}`), &out); err != nil {
			t.Fatalf("DecodeJSON fill with %T default: %v", def, err)
		}
		return out["v"]
	}

	t.Run("long_width_twins", func(t *testing.T) {
		long := SchemaNode{Type: "long"}
		control := fill(t, long, int64(42))
		if control != int64(42) {
			t.Fatalf("anchored control: long fill = %#v, want int64(42)", control)
		}
		for _, v := range []any{int8(42), json.Number("42"), tvNamedI8(42)} {
			if got := fill(t, long, v); !reflect.DeepEqual(got, control) {
				t.Errorf("%T default fills %#v, control %#v", v, got, control)
			}
		}
	})

	t.Run("string_array_twin", func(t *testing.T) {
		arr := SchemaNode{Type: "array", Items: &SchemaNode{Type: "string"}}
		control := fill(t, arr, []any{"a", "b"})
		if got := fill(t, arr, [2]string{"a", "b"}); !reflect.DeepEqual(got, control) {
			t.Errorf("[2]string default fills %#v, []any control %#v", got, control)
		}
	})

	t.Run("record_map_twin", func(t *testing.T) {
		rec := SchemaNode{Type: "record", Name: "S", Fields: []SchemaField{
			{Name: "c", Type: SchemaNode{Type: "long"}},
		}}
		control := fill(t, rec, map[string]any{"c": 7})
		if got := fill(t, rec, tvNamedMap{"c": 7}); !reflect.DeepEqual(got, control) {
			t.Errorf("named-map default fills %#v, map control %#v", got, control)
		}
	})
}

// TestMatrix_TreeValueFieldProps pins the SchemaField.Props position: field
// property values follow the same marshal-image contract as node Props.
func TestMatrix_TreeValueFieldProps(t *testing.T) {
	build := func(t *testing.T, v any) any {
		t.Helper()
		s, err := (&SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{
			{Name: "n", Type: SchemaNode{Type: "long"}, Props: map[string]any{"x": v}},
		}}).Schema()
		if err != nil {
			t.Fatalf("Schema() with %T field prop: %v", v, err)
		}
		return s.Root().Fields[0].Props["x"]
	}
	t.Run("named_bytes", func(t *testing.T) {
		control := build(t, []byte{1, 2, 3})
		if control != "\x01\x02\x03" {
			t.Fatalf("anchored control: field-Props []byte = %#v, want the codepoint string", control)
		}
		if got := build(t, tvNamedBytes{1, 2, 3}); !reflect.DeepEqual(got, control) {
			t.Errorf("named bytes field prop observed %#v, control %#v", got, control)
		}
	})
	t.Run("named_map", func(t *testing.T) {
		control := build(t, map[string]any{"k": "v"})
		if got := build(t, tvNamedMap{"k": "v"}); !reflect.DeepEqual(got, control) {
			t.Errorf("named map field prop observed %#v, control %#v", got, control)
		}
	})
}

// TestMatrix_TreeValueVerdictParity: where a tree value draws an
// accept/reject verdict, the verdict must not depend on the value's Go
// dynamic type, and must agree across both composition surfaces.
func TestMatrix_TreeValueVerdictParity(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()
	fields := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f"`}}

	t.Run("bad_long_default", func(t *testing.T) {
		mk := func(def any) *SchemaNode {
			return &SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{
				{Name: "n", Type: SchemaNode{Type: "long"}, Default: def, HasDefault: true},
			}}
		}
		for _, def := range []any{"x", tvNamedString("x")} {
			if _, err := mk(def).Schema(); err == nil {
				t.Errorf("%T string default for long via node.Schema(): want reject", def)
			}
			if _, err := schemaForScopeCell(t, fields, "",
				[]CustomType{{GoType: primary, Schema: mk(def)}}); err == nil {
				t.Errorf("%T string default for long via SchemaFor: want reject", def)
			}
		}
	})

	t.Run("unmarshalable_kinds_loud", func(t *testing.T) {
		for _, v := range []any{make(chan int), complex(1, 2)} {
			if _, err := (&SchemaNode{Type: "int", Props: map[string]any{"x": v}}).Schema(); err == nil {
				t.Errorf("%T Props via node.Schema(): want a loud error, got success", v)
			}
			node := &SchemaNode{Type: "fixed", Name: "F", Size: 4, Props: map[string]any{"x": v}}
			if _, err := schemaForScopeCell(t, fields, "",
				[]CustomType{{GoType: primary, Schema: node}}); err == nil {
				t.Errorf("%T Props via SchemaFor: want a loud error, got success", v)
			}
		}
	})

	t.Run("reserved_key_clobber_twins", func(t *testing.T) {
		// Whatever the policy for a caller Props key that collides with a
		// reserved attribute, it cannot depend on the value's Go type.
		build := func(v any) (string, error) {
			node := &SchemaNode{Type: "fixed", Name: "F", Size: 4,
				Props: map[string]any{"name": v}}
			s, err := schemaForScopeCell(t, fields, "",
				[]CustomType{{GoType: primary, Schema: node}})
			if err != nil {
				return "", err
			}
			return string(s.Canonical()), nil
		}
		canon, cErr := build("Q")
		named, nErr := build(tvNamedString("Q"))
		if (cErr == nil) != (nErr == nil) {
			t.Fatalf("verdict diverges: plain err=%v, named err=%v", cErr, nErr)
		}
		if canon != named {
			t.Errorf("clobber result diverges:\n plain: %s\n named: %s", canon, named)
		}
	})
}

// TestRegression_TreeValueOwnershipBoundary pins the ownership contract at
// the composition boundary: a build never writes into caller storage (no
// namespace injection into a caller def map, no append into a caller
// slice's spare capacity), and a value SHARED across two Props keys
// composes exactly like two independent equal values.
func TestRegression_TreeValueOwnershipBoundary(t *testing.T) {
	primary := reflect.TypeFor[scopeMatrixPrimary]()

	t.Run("diamond_shared_def", func(t *testing.T) {
		fields := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f"`}}
		mkDef := func() map[string]any {
			return map[string]any{"type": "record", "name": "X",
				"fields": []any{map[string]any{"name": "c", "type": "long"}}}
		}
		shared := mkDef()
		node := &SchemaNode{Type: "array",
			Props: map[string]any{"items": shared, "alsoitems": shared}}
		s, err := schemaForScopeCell(t, fields, "com.x",
			[]CustomType{{GoType: primary, Schema: node}})
		if err != nil {
			t.Fatalf("diamond build: %v", err)
		}
		if _, leaked := shared["namespace"]; leaked {
			t.Errorf("build mutated the shared caller map: %#v", shared)
		}
		node2 := &SchemaNode{Type: "array",
			Props: map[string]any{"items": mkDef(), "alsoitems": mkDef()}}
		s2, err := schemaForScopeCell(t, fields, "com.x",
			[]CustomType{{GoType: primary, Schema: node2}})
		if err != nil {
			t.Fatalf("independent twin build: %v", err)
		}
		if a, b := s.String(), s2.String(); a != b {
			t.Errorf("shared-value diamond composes differently from independent copies:\n shared:      %s\n independent: %s", a, b)
		}
	})

	t.Run("aliases_spare_capacity", func(t *testing.T) {
		aliasFields := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f,type-alias=Old"`}}
		backing := make(tvNamedStrings, 1, 3)
		backing[0] = "prior.P"
		backing = backing[:3]
		backing[1], backing[2] = "SENTINEL1", "SENTINEL2"
		arg := backing[:1]

		build := func(v any) (string, error) {
			node := &SchemaNode{Type: "fixed", Name: "F", Size: 4,
				Props: map[string]any{"aliases": v}}
			s, err := schemaForScopeCell(t, aliasFields, "",
				[]CustomType{{GoType: primary, Schema: node}})
			if err != nil {
				return "", err
			}
			return s.String(), nil
		}
		got, err := build(arg)
		if err != nil {
			t.Fatalf("spare-capacity build: %v", err)
		}
		exact, err := build(tvNamedStrings{"prior.P"})
		if err != nil {
			t.Fatalf("exact-capacity build: %v", err)
		}
		if got != exact {
			t.Errorf("spare-capacity twin diverges:\n spare: %s\n exact: %s", got, exact)
		}
		if backing[1] != "SENTINEL1" || backing[2] != "SENTINEL2" {
			t.Errorf("build wrote into the caller backing array past len: %#v", backing)
		}
	})
}

// tvTwinGen interprets a fuzz byte program as a bounded value generator
// producing a (canonical, named-twin) pair whose marshal images are
// identical by construction. Wrapper choices ride the program's high bits;
// wrapAll forces wrapping so short programs still produce named shapes. The
// domain deliberately excludes the documented image-owning shapes (values
// with their own MarshalJSON/MarshalText, json.Number, NaN) and nil/empty
// []string-kind containers, whose image handling is pinned separately.
type tvTwinGen struct {
	prog    []byte
	i       int
	wrapAll bool
}

func (g *tvTwinGen) next() byte {
	if g.i >= len(g.prog) {
		return 0
	}
	b := g.prog[g.i]
	g.i++
	return b
}

func (g *tvTwinGen) build(depth int, budget *int) (any, any) {
	if *budget <= 0 || depth >= 3 {
		return "leaf", "leaf"
	}
	*budget--
	op := g.next()
	wrap := g.wrapAll || op&0x80 != 0
	switch op % 9 {
	case 0:
		s := string(rune('a' + int(op>>4)%3))
		if wrap {
			return s, tvNamedString(s)
		}
		return s, s
	case 1:
		n := int64(int8(g.next()))
		if wrap {
			return n, int8(n)
		}
		return n, n
	case 2:
		// Finite float chosen float32-exact so width twins share an image.
		fv := float64(int8(g.next())) / 4
		if wrap {
			return fv, tvNamedF64(fv)
		}
		return fv, fv
	case 3:
		b := op&0x40 != 0
		if wrap {
			return b, tvNamedBool(b)
		}
		return b, b
	case 4:
		bs := []byte{g.next(), g.next()}
		if wrap {
			return bs, tvNamedBytes(bs)
		}
		return bs, bs
	case 5:
		// ±Inf: the numeric-preserving fixups extend to named float kinds.
		fv := math.Inf(1)
		if op&0x40 != 0 {
			fv = math.Inf(-1)
		}
		if wrap {
			return fv, tvNamedF64(fv)
		}
		return fv, fv
	case 6:
		n := 1 + int(op>>4)%2
		cm := make(map[string]any, n)
		nm := make(map[string]any, n)
		for i := range n {
			k := string(rune('k' + i))
			cv, nv := g.build(depth+1, budget)
			cm[k] = cv
			nm[k] = nv
		}
		if wrap {
			return cm, tvNamedMap(nm)
		}
		return cm, nm
	case 7:
		n := 1 + int(op>>4)%2
		cs := make([]any, n)
		ns := make([]any, n)
		for i := range n {
			cs[i], ns[i] = g.build(depth+1, budget)
		}
		if wrap {
			return cs, tvNamedSlice(ns)
		}
		return cs, ns
	default:
		// Non-empty []string; the nil/empty []string image handling is
		// pinned separately.
		n := 1 + int(op>>4)%2
		ss := make([]string, n)
		for i := range ss {
			ss[i] = string(rune('a' + i))
		}
		if wrap {
			return ss, tvNamedStrings(append([]string(nil), ss...))
		}
		return ss, append([]string(nil), ss...)
	}
}

// FuzzTreeValueTwinParity fuzzes the caller-value domain of the composition
// surface: a generated canonical value and its named twin (identical
// marshal images) must draw the same accept/reject verdict, produce the
// same rendered schema text and observed metadata, and the rendered text
// must be a Parse fixed point — through the Props rebuild, the field
// Default position, and the SchemaFor render.
func FuzzTreeValueTwinParity(f *testing.F) {
	f.Add([]byte{0}, true)
	f.Add([]byte{6, 2, 1, 7, 3, 0xC1, 5}, true)
	f.Add([]byte{7, 3, 0x86, 2, 0x81, 4}, false)
	f.Add([]byte{6, 1, 8, 2, 0x83, 0x84}, true)
	f.Add([]byte{5, 0xFF, 6, 1, 5, 1}, true)
	f.Add([]byte{4, 9, 8, 6, 2, 0, 1}, true)
	f.Fuzz(func(t *testing.T, prog []byte, wrapAll bool) {
		if len(prog) > 48 {
			prog = prog[:48]
		}
		g := &tvTwinGen{prog: prog, wrapAll: wrapAll}
		budget := 20
		canon, named := g.build(0, &budget)
		cImg, cErr := json.Marshal(canon)
		nImg, nErr := json.Marshal(named)
		if (cErr == nil) != (nErr == nil) {
			t.Fatalf("twin marshal verdicts diverge: canonical %v, named %v", cErr, nErr)
		}
		if cErr == nil && string(cImg) != string(nImg) {
			t.Fatalf("generator twin premise broken:\n canon: %s\n named: %s", cImg, nImg)
		}

		check := func(label string, run func(v any) (string, any, error)) {
			t.Helper()
			cs, cObs, cErrr := run(canon)
			ns, nObs, nErrr := run(named)
			if (cErrr == nil) != (nErrr == nil) {
				t.Fatalf("%s verdict diverges: canonical %v, named %v", label, cErrr, nErrr)
			}
			if cErrr != nil {
				return
			}
			if cs != ns {
				t.Fatalf("%s rendered text diverges:\n canon: %s\n named: %s", label, cs, ns)
			}
			if !reflect.DeepEqual(cObs, nObs) {
				t.Fatalf("%s observed metadata diverges: %#v vs %#v", label, cObs, nObs)
			}
			s2, err := Parse(cs)
			if err != nil {
				t.Fatalf("%s rendered schema does not reparse: %v\n%s", label, err, cs)
			}
			if s2.String() != cs {
				t.Fatalf("%s String() is not a Parse fixed point:\n first: %s\n again: %s", label, cs, s2.String())
			}
		}

		check("props-rebuild", func(v any) (string, any, error) {
			s, err := (&SchemaNode{Type: "int", Props: map[string]any{"x": v}}).Schema()
			if err != nil {
				return "", nil, err
			}
			return s.String(), s.Root().Props["x"], nil
		})
		check("field-default", func(v any) (string, any, error) {
			s, err := (&SchemaNode{Type: "record", Name: "R", Fields: []SchemaField{
				{Name: "v", Type: SchemaNode{Type: "long"}, Default: v, HasDefault: true},
			}}).Schema()
			if err != nil {
				return "", nil, err
			}
			return s.String(), s.Root().Fields[0].Default, nil
		})
		check("schemafor-render", func(v any) (string, any, error) {
			primary := reflect.TypeFor[scopeMatrixPrimary]()
			flds := []reflect.StructField{{Name: "F", Type: primary, Tag: `avro:"f"`}}
			node := &SchemaNode{Type: "fixed", Name: "F", Size: 4, Props: map[string]any{"x": v}}
			s, err := schemaForScopeCell(t, flds, "", []CustomType{{GoType: primary, Schema: node}})
			if err != nil {
				return "", nil, err
			}
			return s.String(), s.Root().Fields[0].Type.Props["x"], nil
		})
	})
}
