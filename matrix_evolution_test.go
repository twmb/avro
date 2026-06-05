package avro_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// Evolution matrix: writer→reader schema changes across every promotion
// pair × composition context, per-kind field add (resolution default fill) /
// drop (the skip path, per kind) / reorder, union evolution (reorder,
// widening, two-pass exact-before-promotion, fail-fast narrowing), enum
// reader-defaults, and alias renames — with Resolve ⇔ CheckCompatibility
// agreement asserted on every pair.
// ---------------------------------------------------------------------------

// resolveBoth asserts Resolve and CheckCompatibility agree on success.
func resolveBoth(t *testing.T, w, r *avro.Schema) (*avro.Schema, error) {
	t.Helper()
	res, rerr := avro.Resolve(w, r)
	cerr := avro.CheckCompatibility(w, r)
	if (rerr == nil) != (cerr == nil) {
		t.Fatalf("Resolve and CheckCompatibility disagree: resolve=%v compat=%v", rerr, cerr)
	}
	return res, rerr
}

func TestMatrix_PromotionPairsByContext(t *testing.T) {
	pairs := []struct {
		wKind, rKind string
		wVal, rVal   any
	}{
		{"int", "long", int32(-77), int64(-77)},
		{"int", "float", int32(123), float32(123)},
		{"int", "double", int32(-9), float64(-9)},
		{"long", "float", int64(1 << 10), float32(1 << 10)},
		{"long", "double", int64(-5), float64(-5)},
		{"float", "double", float32(1.5), float64(1.5)},
		{"string", "bytes", "sb", []byte("sb")},
		{"bytes", "string", []byte("bs"), "bs"},
	}
	for _, p := range pairs {
		for _, cx := range matCtxs() {
			if cx.skip != nil && (cx.skip(p.wKind) || cx.skip(p.rKind)) {
				continue
			}
			t.Run(fmt.Sprintf("%s→%s/%s", p.wKind, p.rKind, cx.label), func(t *testing.T) {
				uw, ur := &uniq{}, &uniq{}
				wSchema := cx.schema(fmt.Sprintf("%q", p.wKind), p.wKind, uw)
				rSchema := cx.schema(fmt.Sprintf("%q", p.rKind), p.rKind, ur)
				w := avro.MustParse(wSchema)
				r := avro.MustParse(rSchema)
				res, err := resolveBoth(t, w, r)
				if err != nil {
					t.Fatalf("Resolve: %v\nw: %s\nr: %s", err, wSchema, rSchema)
				}
				wire, err := w.AppendEncode(nil, cx.wrap(p.wVal))
				if err != nil {
					t.Fatalf("writer encode: %v", err)
				}
				var got any
				if _, err := res.Decode(wire, &got); err != nil {
					t.Fatalf("resolved decode: %v", err)
				}
				wantWire, err := r.AppendEncode(nil, cx.wrap(p.rVal))
				if err != nil {
					t.Fatalf("reader encode: %v", err)
				}
				gotWire, err := r.AppendEncode(nil, got)
				if err != nil || !bytes.Equal(gotWire, wantWire) {
					t.Fatalf("promoted tree wire differs: err=%v\n got=%x\nwant=%x\nvalue: %#v", err, gotWire, wantWire, got)
				}
			})
		}
	}
}

// Field DROP per kind: the writer carries a field of every kind that the
// reader lacks — the resolved decode must SKIP it (exercising every skipfn)
// and preserve the surrounding fields. Also nested inside an array.
func TestMatrix_FieldDropPerKind(t *testing.T) {
	kinds := []struct {
		label  string
		schema string
		value  any
	}{
		{"null", `"null"`, nil},
		{"boolean", `"boolean"`, true},
		{"int", `"int"`, int32(7)},
		{"long", `"long"`, int64(1 << 60)},
		{"float", `"float"`, float32(1.5)},
		{"double", `"double"`, 2.25},
		{"string", `"string"`, "drop"},
		{"bytes", `"bytes"`, []byte{1, 2}},
		{"enum", `{"type":"enum","name":"SKE","symbols":["A","B"]}`, "B"},
		{"fixed2", `{"type":"fixed","name":"SKF","size":2}`, []byte{3, 4}},
		{"fixed0", `{"type":"fixed","name":"SKF0","size":0}`, []byte{}},
		{"array", `{"type":"array","items":"int"}`, []any{int32(1), int32(2)}},
		{"array-empty", `{"type":"array","items":"int"}`, []any{}},
		{"map", `{"type":"map","values":"string"}`, map[string]any{"k": "v"}},
		{"record", `{"type":"record","name":"SKR","fields":[{"name":"i","type":"int"},{"name":"s","type":"string"}]}`,
			map[string]any{"i": int32(9), "s": "x"}},
		{"nullunion", `["null","int"]`, int32(5)},
		{"nullunion-nil", `["null","int"]`, nil},
		{"multibranch", `["null","boolean","int","string"]`, "u"},
		{"logical-ts", `{"type":"long","logicalType":"timestamp-millis"}`, int64(1717243496789)},
		{"decimal", `{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}`, []byte{0x30, 0x39}},
		{"recursive", `{"type":"record","name":"SKN","fields":[{"name":"v","type":"int"},{"name":"next","type":["null","SKN"],"default":null}]}`,
			map[string]any{"v": int32(1), "next": map[string]any{"v": int32(2), "next": nil}}},
	}
	for _, k := range kinds {
		wSchema := fmt.Sprintf(`{"type":"record","name":"R","fields":[
			{"name":"pre","type":"string"},
			{"name":"dropme","type":%s},
			{"name":"post","type":"long"}]}`, k.schema)
		rSchema := `{"type":"record","name":"R","fields":[
			{"name":"pre","type":"string"},
			{"name":"post","type":"long"}]}`
		t.Run(k.label, func(t *testing.T) {
			w := avro.MustParse(wSchema)
			r := avro.MustParse(rSchema)
			res, err := resolveBoth(t, w, r)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			wire, err := w.AppendEncode(nil, map[string]any{"pre": "p", "dropme": k.value, "post": int64(11)})
			if err != nil {
				t.Fatalf("writer encode: %v", err)
			}
			var got map[string]any
			if _, err := res.Decode(wire, &got); err != nil {
				t.Fatalf("resolved decode (skip %s): %v", k.label, err)
			}
			if got["pre"] != "p" || got["post"] != int64(11) || len(got) != 2 {
				t.Fatalf("surrounding fields corrupted by skip: %#v", got)
			}
		})
		// Same drop with the record as an array item (skip inside blocks).
		t.Run(k.label+"/in-array", func(t *testing.T) {
			w := avro.MustParse(fmt.Sprintf(`{"type":"array","items":%s}`, wSchema))
			r := avro.MustParse(fmt.Sprintf(`{"type":"array","items":%s}`, rSchema))
			res, err := resolveBoth(t, w, r)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			item := map[string]any{"pre": "p", "dropme": k.value, "post": int64(3)}
			wire, err := w.AppendEncode(nil, []any{item, item})
			if err != nil {
				t.Fatalf("writer encode: %v", err)
			}
			var got []any
			if _, err := res.Decode(wire, &got); err != nil {
				t.Fatalf("resolved decode: %v", err)
			}
			if len(got) != 2 || got[1].(map[string]any)["post"] != int64(3) {
				t.Fatalf("array skip corrupted items: %#v", got)
			}
		})
	}
}

// Field ADD per kind: the reader declares a defaulted field the writer
// lacks — the resolution fill must agree with the reader's own JSON fill
// and re-encode onto the reader's auto-fill wire.
func TestMatrix_FieldAddPerKind(t *testing.T) {
	kinds := []struct {
		label      string
		fieldType  string
		defaultLit string
	}{
		{"boolean", `"boolean"`, `true`},
		{"int", `"int"`, `7`},
		{"long", `"long"`, `9007199254740993`},
		{"float", `"float"`, `1.5`},
		{"double", `"double"`, `-2.25`},
		{"string", `"string"`, `"d"`},
		{"bytes", `"bytes"`, `"\u00ff"`},
		{"bytes-empty", `"bytes"`, `""`},
		{"enum", `{"type":"enum","name":"ADE","symbols":["A","B"]}`, `"B"`},
		{"fixed1", `{"type":"fixed","name":"ADF","size":1}`, `"\u00ab"`},
		{"fixed0", `{"type":"fixed","name":"ADF0","size":0}`, `""`},
		{"date", `{"type":"int","logicalType":"date"}`, `19723`},
		{"timestamp", `{"type":"long","logicalType":"timestamp-millis"}`, `1717243496789`},
		{"nullunion", `["null","int"]`, `null`},
		{"union-int-first", `["int","string"]`, `42`},
		{"array", `{"type":"array","items":"int"}`, `[1,2]`},
		{"map", `{"type":"map","values":"string"}`, `{"k":"v"}`},
		{"record", `{"type":"record","name":"ADR","fields":[{"name":"i","type":"int"}]}`, `{"i":3}`},
		{"empty-record", `{"type":"record","name":"ADER","fields":[]}`, `{}`},
	}
	for _, k := range kinds {
		t.Run(k.label, func(t *testing.T) {
			wSchema := `{"type":"record","name":"R","fields":[{"name":"pre","type":"string"}]}`
			rSchema := fmt.Sprintf(`{"type":"record","name":"R","fields":[
				{"name":"pre","type":"string"},
				{"name":"f","type":%s,"default":%s}]}`, k.fieldType, k.defaultLit)
			w := avro.MustParse(wSchema)
			r := avro.MustParse(rSchema)
			res, err := resolveBoth(t, w, r)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			wire, err := w.AppendEncode(nil, map[string]any{"pre": "p"})
			if err != nil {
				t.Fatalf("writer encode: %v", err)
			}
			var got any
			if _, err := res.Decode(wire, &got); err != nil {
				t.Fatalf("resolution default fill: %v", err)
			}
			// The reader's own JSON fill is the reference.
			var jfill map[string]any
			if err := r.DecodeJSON([]byte(`{"pre":"p"}`), &jfill); err != nil {
				t.Fatalf("reader JSON fill: %v", err)
			}
			if !matEqual(got, jfill) {
				t.Fatalf("resolution fill diverges from JSON fill:\n res=%#v\njson=%#v", got, jfill)
			}
			// And re-encodes onto the reader's auto-fill wire.
			wantWire, err := r.AppendEncode(nil, map[string]any{"pre": "p"})
			if err != nil {
				t.Fatalf("reader auto-fill encode: %v", err)
			}
			gotWire, err := r.AppendEncode(nil, got)
			if err != nil || !bytes.Equal(gotWire, wantWire) {
				t.Fatalf("filled tree wire differs: err=%v\n got=%x\nwant=%x", err, gotWire, wantWire)
			}
		})
	}
}

// Field REORDER: same fields, different declaration order — resolution maps
// by name; every value must land on the right reader field.
func TestMatrix_FieldReorder(t *testing.T) {
	w := avro.MustParse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"},
		{"name":"c","type":"bytes"},
		{"name":"d","type":["null","long"],"default":null}]}`)
	r := avro.MustParse(`{"type":"record","name":"R","fields":[
		{"name":"d","type":["null","long"],"default":null},
		{"name":"c","type":"bytes"},
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}]}`)
	res, err := resolveBoth(t, w, r)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	wire, err := w.AppendEncode(nil, map[string]any{
		"a": int32(1), "b": "two", "c": []byte{3}, "d": int64(4)})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got map[string]any
	if _, err := res.Decode(wire, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["a"] != int32(1) || got["b"] != "two" || !bytes.Equal(got["c"].([]byte), []byte{3}) || got["d"] != int64(4) {
		t.Fatalf("reordered fields mismatched: %#v", got)
	}
}

func TestMatrix_UnionEvolution(t *testing.T) {
	t.Run("branch-reorder", func(t *testing.T) {
		w := avro.MustParse(`["int","string"]`)
		r := avro.MustParse(`["string","int"]`)
		res, err := resolveBoth(t, w, r)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		for _, v := range []any{int32(5), "s"} {
			wire, _ := w.AppendEncode(nil, v)
			var got any
			if _, err := res.Decode(wire, &got); err != nil {
				t.Fatalf("decode %v: %v", v, err)
			}
			if !matEqual(got, v) {
				t.Fatalf("reordered union value mismatch: %#v vs %#v", got, v)
			}
			// Re-encoding against the reader uses the READER's indices.
			wantWire, _ := r.AppendEncode(nil, v)
			gotWire, err := r.AppendEncode(nil, got)
			if err != nil || !bytes.Equal(gotWire, wantWire) {
				t.Fatalf("reader re-encode differs: err=%v got=%x want=%x", err, gotWire, wantWire)
			}
		}
	})
	t.Run("widening", func(t *testing.T) {
		w := avro.MustParse(`["int"]`)
		r := avro.MustParse(`["null","int","string"]`)
		res, err := resolveBoth(t, w, r)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		wire, _ := w.AppendEncode(nil, int32(9))
		var got any
		if _, err := res.Decode(wire, &got); err != nil || got != int32(9) {
			t.Fatalf("widening decode: %v %#v", err, got)
		}
	})
	t.Run("narrowing-fails-fast", func(t *testing.T) {
		w := avro.MustParse(`["int","string"]`)
		r := avro.MustParse(`["int"]`)
		if _, err := resolveBoth(t, w, r); err == nil {
			t.Fatal("narrowing union must fail Resolve eagerly (documented fail-fast)")
		}
	})
	t.Run("two-pass-exact-beats-promotion", func(t *testing.T) {
		w := avro.MustParse(`"int"`)
		r := avro.MustParse(`["double","int"]`)
		res, err := resolveBoth(t, w, r)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		wire, _ := w.AppendEncode(nil, int32(3))
		var got any
		if _, err := res.Decode(wire, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if _, ok := got.(int32); !ok {
			t.Fatalf("exact int branch must win over double promotion, got %T", got)
		}
	})
	t.Run("promotion-fallback-into-union", func(t *testing.T) {
		w := avro.MustParse(`"long"`)
		r := avro.MustParse(`["int","double"]`)
		res, err := resolveBoth(t, w, r)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		wire, _ := w.AppendEncode(nil, int64(12))
		var got any
		if _, err := res.Decode(wire, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if _, ok := got.(float64); !ok {
			t.Fatalf("long should promote into the double branch, got %T", got)
		}
	})
}

func TestMatrix_EnumEvolutionByPosition(t *testing.T) {
	wEnum := `{"type":"enum","name":"EE","symbols":["A","B","C"]}`
	rEnum := `{"type":"enum","name":"EE","symbols":["A","B"],"default":"A"}`
	rEnumNoDefault := `{"type":"enum","name":"EE","symbols":["A","B"]}`
	positions := []struct {
		label string
		wrap  func(inner string) string
		val   func(sym string) any
		out   func(decoded any) any
	}{
		{"top", func(in string) string { return in },
			func(s string) any { return s }, func(d any) any { return d }},
		{"field", func(in string) string {
			return fmt.Sprintf(`{"type":"record","name":"ER","fields":[{"name":"e","type":%s}]}`, in)
		},
			func(s string) any { return map[string]any{"e": s} },
			func(d any) any { return d.(map[string]any)["e"] }},
		{"array", func(in string) string { return fmt.Sprintf(`{"type":"array","items":%s}`, in) },
			func(s string) any { return []any{s} },
			func(d any) any { return d.([]any)[0] }},
	}
	for _, pos := range positions {
		t.Run(pos.label, func(t *testing.T) {
			w := avro.MustParse(pos.wrap(wEnum))
			r := avro.MustParse(pos.wrap(rEnum))
			res, err := resolveBoth(t, w, r)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			// Mapped symbol survives; unmapped symbol takes the reader default.
			for _, tc := range []struct{ in, want string }{{"B", "B"}, {"C", "A"}} {
				wire, _ := w.AppendEncode(nil, pos.val(tc.in))
				var got any
				if _, err := res.Decode(wire, &got); err != nil {
					t.Fatalf("decode %s: %v", tc.in, err)
				}
				if pos.out(got) != tc.want {
					t.Fatalf("symbol %s: got %v want %s", tc.in, pos.out(got), tc.want)
				}
			}
			// Without a reader default the unmappable symbol fails Resolve
			// eagerly (documented fail-fast).
			rn := avro.MustParse(pos.wrap(rEnumNoDefault))
			if _, err := resolveBoth(t, w, rn); err == nil {
				t.Fatal("unmappable enum symbol without default must fail Resolve")
			}
		})
	}
}

func TestMatrix_AliasEvolution(t *testing.T) {
	cases := []struct {
		label   string
		wSchema string
		rSchema string
		value   any
	}{
		{"type-alias",
			`{"type":"record","name":"Old","fields":[{"name":"a","type":"int"}]}`,
			`{"type":"record","name":"New","aliases":["Old"],"fields":[{"name":"a","type":"int"}]}`,
			map[string]any{"a": int32(1)}},
		{"type-alias-namespaced",
			`{"type":"record","name":"Old","namespace":"n1","fields":[{"name":"a","type":"int"}]}`,
			`{"type":"record","name":"New","namespace":"n2","aliases":["n1.Old"],"fields":[{"name":"a","type":"int"}]}`,
			map[string]any{"a": int32(2)}},
		{"field-alias",
			`{"type":"record","name":"R","fields":[{"name":"old","type":"string"}]}`,
			`{"type":"record","name":"R","fields":[{"name":"new","type":"string","aliases":["old"]}]}`,
			map[string]any{"old": "v"}},
		{"enum-alias",
			`{"type":"enum","name":"OldE","symbols":["A"]}`,
			`{"type":"enum","name":"NewE","aliases":["OldE"],"symbols":["A"]}`,
			"A"},
		{"fixed-alias",
			`{"type":"fixed","name":"OldF","size":2}`,
			`{"type":"fixed","name":"NewF","aliases":["OldF"],"size":2}`,
			[]byte{7, 8}},
	}
	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			w := avro.MustParse(c.wSchema)
			r := avro.MustParse(c.rSchema)
			res, err := resolveBoth(t, w, r)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			wire, err := w.AppendEncode(nil, c.value)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			var got any
			if _, err := res.Decode(wire, &got); err != nil {
				t.Fatalf("aliased decode: %v", err)
			}
		})
		// The same alias pair nested in an array still resolves.
		t.Run(c.label+"/in-array", func(t *testing.T) {
			w := avro.MustParse(fmt.Sprintf(`{"type":"array","items":%s}`, c.wSchema))
			r := avro.MustParse(fmt.Sprintf(`{"type":"array","items":%s}`, c.rSchema))
			res, err := resolveBoth(t, w, r)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			wire, err := w.AppendEncode(nil, []any{c.value})
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			var got any
			if _, err := res.Decode(wire, &got); err != nil {
				t.Fatalf("aliased decode: %v", err)
			}
		})
	}
}
