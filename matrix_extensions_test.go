package avro_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// Extension axes: lenient encode-input forms (every accepted Go form of a
// value must produce the identical wire), metadata preservation through the
// rebuild (doc/aliases/props survive Root().Schema()), lax-name schemas, and
// nil-equivalent encode shapes across union arities.
// ---------------------------------------------------------------------------

// Every accepted input form for the same logical value must produce
// byte-identical wires — in every position.
func TestMatrix_LenientInputForms(t *testing.T) {
	cases := []struct {
		label  string
		schema string
		forms  []any
	}{
		{"int", `"int"`, []any{int32(42), int64(42), int16(42), uint8(42), int(42), float64(42), json.Number("42")}},
		{"long", `"long"`, []any{int64(42), int32(42), int(42), uint32(42), float64(42), json.Number("42")}},
		{"float", `"float"`, []any{float32(1.5), float64(1.5), json.Number("1.5")}},
		{"double", `"double"`, []any{float64(1.5), float32(1.5), json.Number("1.5")}},
		{"string", `"string"`, []any{"sv", []byte("sv")}},
		{"bytes", `"bytes"`, []any{[]byte("bv"), "bv"}},
		{"fixed", `{"type":"fixed","name":"LF","size":2}`, []any{[]byte{0x61, 0x62}, "ab", [2]byte{0x61, 0x62}}},
		{"enum-symbol-or-ordinal", `{"type":"enum","name":"LE","symbols":["A","B","C"]}`, []any{"C", 2, uint8(2), int64(2)}},
		{"timestamp-forms", `{"type":"long","logicalType":"timestamp-millis"}`,
			[]any{time.UnixMilli(1717243496789).UTC(), int64(1717243496789), json.Number("1717243496789")}},
		{"decimal-forms", `{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}`,
			[]any{big.NewRat(3, 2), float64(1.5), json.Number("1.5")}},
	}
	positions := []struct {
		label  string
		schema func(in string) string
		wrap   func(v any) any
	}{
		{"top", func(in string) string { return in }, func(v any) any { return v }},
		{"field", func(in string) string {
			return fmt.Sprintf(`{"type":"record","name":"LR","fields":[{"name":"f","type":%s}]}`, in)
		}, func(v any) any { return map[string]any{"f": v} }},
		{"array", func(in string) string { return fmt.Sprintf(`{"type":"array","items":%s}`, in) },
			func(v any) any { return []any{v} }},
	}
	for _, c := range cases {
		for _, pos := range positions {
			t.Run(c.label+"/"+pos.label, func(t *testing.T) {
				s := avro.MustParse(pos.schema(c.schema))
				var want []byte
				for i, form := range c.forms {
					got, err := s.AppendEncode(nil, pos.wrap(form))
					if err != nil {
						t.Fatalf("form %d (%T): %v", i, form, err)
					}
					if i == 0 {
						want = got
						continue
					}
					if !bytes.Equal(got, want) {
						t.Fatalf("form %d (%T) wire differs:\n got=%x\nwant=%x", i, form, got, want)
					}
				}
				// JSON wires agree across forms too.
				var wantJ []byte
				for i, form := range c.forms {
					got, err := s.AppendEncodeJSON(nil, pos.wrap(form))
					if err != nil {
						t.Fatalf("json form %d (%T): %v", i, form, err)
					}
					if i == 0 {
						wantJ = got
						continue
					}
					if !bytes.Equal(got, wantJ) {
						t.Fatalf("json form %d (%T) differs:\n got=%s\nwant=%s", i, form, got, wantJ)
					}
				}
			})
		}
	}
}

// Doc strings, aliases, and custom properties survive the metadata rebuild
// — at the type level, field level, and on named branch types.
func TestMatrix_MetadataPreservedThroughRebuild(t *testing.T) {
	schema := `{"type":"record","name":"R","namespace":"meta.ns","doc":"record doc",
		"aliases":["OldR","other.AliasedR"],"custom.top":"tv",
		"fields":[
			{"name":"f","type":"int","doc":"field doc","aliases":["oldf"],"order":"descending","custom.fld":7,"default":3},
			{"name":"e","type":{"type":"enum","name":"E","doc":"enum doc","symbols":["A","B"],"default":"B","custom.enum":true}},
			{"name":"x","type":{"type":"fixed","name":"F","size":2,"custom.fixed":"fv"}}]}`
	s := avro.MustParse(schema)
	check := func(t *testing.T, root avro.SchemaNode, tag string) {
		t.Helper()
		if root.Doc != "record doc" || root.Namespace != "meta.ns" {
			t.Fatalf("%s: doc/ns lost: %q %q", tag, root.Doc, root.Namespace)
		}
		if len(root.Aliases) != 2 {
			t.Fatalf("%s: aliases lost: %v", tag, root.Aliases)
		}
		if root.Props["custom.top"] != "tv" {
			t.Fatalf("%s: type prop lost: %v", tag, root.Props)
		}
		f := root.Fields[0]
		if f.Doc != "field doc" || len(f.Aliases) != 1 || f.Order != "descending" {
			t.Fatalf("%s: field metadata lost: %+v", tag, f)
		}
		if f.Props["custom.fld"] != int64(7) {
			t.Fatalf("%s: field prop lost or retyped: %#v", tag, f.Props["custom.fld"])
		}
		if f.Default != int32(3) || !f.HasDefault {
			t.Fatalf("%s: default lost: %#v", tag, f.Default)
		}
		e := root.Fields[1].Type
		if e.Doc != "enum doc" || e.Props["custom.enum"] != true {
			t.Fatalf("%s: enum metadata lost: %+v", tag, e)
		}
		x := root.Fields[2].Type
		if x.Props["custom.fixed"] != "fv" {
			t.Fatalf("%s: fixed prop lost: %+v", tag, x)
		}
	}
	root := s.Root()
	check(t, *root, "first Root()")
	rebuilt, err := root.Schema()
	if err != nil {
		t.Fatalf("Root().Schema(): %v", err)
	}
	check(t, *rebuilt.Root(), "rebuilt Root()")
	// Second-generation rebuild is stable too.
	rb2root := rebuilt.Root()
	rebuilt2, err := rb2root.Schema()
	if err != nil {
		t.Fatalf("second rebuild: %v", err)
	}
	check(t, *rebuilt2.Root(), "second rebuilt Root()")
}

// Lax-name schemas: the wire paths work; the names survive Canonical()
// (escaped correctly) and the metadata rebuild.
func TestMatrix_LaxNames(t *testing.T) {
	lax := avro.WithLaxNames(nil)
	for _, name := range []string{
		"with space", "tab\tname", `back\slash`, `qu"ote`, "uni🎉code", "1starts-digit",
	} {
		t.Run(name, func(t *testing.T) {
			nameJSON, _ := json.Marshal(name)
			schema := fmt.Sprintf(`{"type":"record","name":%s,"fields":[
				{"name":"f","type":{"type":"enum","name":%s,"symbols":["A"]}}]}`,
				nameJSON, string(mustJSON(name+"E")))
			s, err := avro.Parse(schema, lax)
			if err != nil {
				t.Fatalf("lax Parse: %v", err)
			}
			vin := map[string]any{"f": "A"}
			w1, err := s.AppendEncode(nil, vin)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			var a1 any
			if _, err := s.Decode(w1, &a1); err != nil || !matEqual(a1, vin) {
				t.Fatalf("decode: %v %#v", err, a1)
			}
			j1, err := s.AppendEncodeJSON(nil, a1)
			if err != nil {
				t.Fatalf("encodeJSON: %v", err)
			}
			var aj any
			if err := s.DecodeJSON(j1, &aj); err != nil || !matEqual(aj, a1) {
				t.Fatalf("decodeJSON: %v", err)
			}
			// Canonical() must stay valid JSON with the name escaped.
			canon := s.Canonical()
			var any1 any
			if err := json.Unmarshal(canon, &any1); err != nil {
				t.Fatalf("canonical not valid JSON: %v\n%s", err, canon)
			}
			// The metadata rebuild must accept the same SchemaOpts the
			// original Parse needed: a lax-named schema is rebuildable by
			// passing WithLaxNames through Schema().
			root := s.Root()
			rebuilt, err := root.Schema(lax)
			if err != nil {
				t.Fatalf("Root().Schema(lax): %v", err)
			}
			w2, err := rebuilt.AppendEncode(nil, vin)
			if err != nil || !bytes.Equal(w2, w1) {
				t.Fatalf("rebuilt lax schema wire differs: err=%v\n w1=%x\n w2=%x", err, w1, w2)
			}
		})
	}
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// Nil-equivalent encode shapes: a typed nil pointer, an interface-wrapped
// nil pointer, and a non-nil pointer to a nil pointer all route to the null
// branch — on both wires, across union arities and positions.
func TestMatrix_NilShapesAcrossUnions(t *testing.T) {
	var nilPtr *int32
	shapes := []struct {
		label string
		value any
	}{
		{"nil", nil},
		{"typed-nil-ptr", nilPtr},
		{"iface-nil-ptr", any(nilPtr)},
		{"ptr-to-nil-ptr", &nilPtr},
		{"nil-byte-slice", []byte(nil)},
		{"nil-map", map[string]any(nil)},
		{"nil-any-slice", []any(nil)},
	}
	unions := []struct {
		label   string
		schema  string
		nullIdx int32
	}{
		{"null-first-2", `["null","int"]`, 0},
		{"null-second-2", `["int","null"]`, 1},
		{"null-first-3", `["null","int","string"]`, 0},
		{"null-mid-3", `["int","null","string"]`, 1},
	}
	for _, un := range unions {
		s := avro.MustParse(un.schema)
		wantWire, err := s.AppendEncode(nil, nil)
		if err != nil {
			t.Fatalf("%s: encode nil: %v", un.label, err)
		}
		wantJSON, err := s.AppendEncodeJSON(nil, nil)
		if err != nil {
			t.Fatalf("%s: encodeJSON nil: %v", un.label, err)
		}
		for _, sh := range shapes {
			t.Run(un.label+"/"+sh.label, func(t *testing.T) {
				got, err := s.AppendEncode(nil, sh.value)
				if err != nil {
					t.Fatalf("encode: %v", err)
				}
				if !bytes.Equal(got, wantWire) {
					t.Fatalf("binary nil-shape diverges: got=%x want=%x", got, wantWire)
				}
				gotJ, err := s.AppendEncodeJSON(nil, sh.value)
				if err != nil {
					t.Fatalf("encodeJSON: %v", err)
				}
				if !bytes.Equal(gotJ, wantJSON) {
					t.Fatalf("JSON nil-shape diverges: got=%s want=%s", gotJ, wantJSON)
				}
			})
		}
		// The same shapes inside a record field and array items.
		fs := avro.MustParse(fmt.Sprintf(`{"type":"record","name":"NR","fields":[{"name":"u","type":%s}]}`, un.schema))
		fWant, _ := fs.AppendEncode(nil, map[string]any{"u": nil})
		for _, sh := range shapes {
			t.Run(un.label+"/field/"+sh.label, func(t *testing.T) {
				got, err := fs.AppendEncode(nil, map[string]any{"u": sh.value})
				if err != nil {
					t.Fatalf("encode: %v", err)
				}
				if !bytes.Equal(got, fWant) {
					t.Fatalf("field nil-shape diverges: got=%x want=%x", got, fWant)
				}
			})
		}
	}
}
