package avro_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// A record/array/map field may carry a default whose type (or its
// element/value/nested-field type) is a forward reference to a named type
// declared later in the same schema. Such schemas parse fine without a
// default, so adding a default must not turn Parse into a nil-pointer panic:
// the default-encode pipeline must run only after every forward-referenced
// child node is wired. Both the build-time deferral (container items/values)
// and the finalize-time ordering (nested record fields) are exercised, and
// the encoded default value is verified — not just the absence of a panic.
func TestRegression_ForwardRefFieldDefaultEncodes(t *testing.T) {
	t.Run("array_items_forward_ref", func(t *testing.T) {
		s, err := avro.Parse(`{"type":"record","name":"R","fields":[
			{"name":"arr","type":{"type":"array","items":"Inner"},"default":[{"v":9}]},
			{"name":"l","type":{"type":"record","name":"Inner","fields":[{"name":"v","type":"int"}]}}
		]}`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		buf, err := s.AppendEncode(nil, map[string]any{"l": map[string]any{"v": 1}})
		if err != nil {
			t.Fatalf("encode (arr default fills): %v", err)
		}
		out := map[string]any{}
		if _, err := s.Decode(buf, &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		arr, _ := out["arr"].([]any)
		if len(arr) != 1 {
			t.Fatalf("arr default: got %#v, want one element", out["arr"])
		}
		if inner, _ := arr[0].(map[string]any); inner == nil || inner["v"].(int32) != 9 {
			t.Errorf("arr[0].v = %#v, want int32(9)", arr[0])
		}
	})

	t.Run("map_values_forward_ref", func(t *testing.T) {
		s, err := avro.Parse(`{"type":"record","name":"R","fields":[
			{"name":"m","type":{"type":"map","values":"Inner"},"default":{"k":{"v":3}}},
			{"name":"l","type":{"type":"record","name":"Inner","fields":[{"name":"v","type":"int"}]}}
		]}`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		buf, err := s.AppendEncode(nil, map[string]any{"l": map[string]any{"v": 0}})
		if err != nil {
			t.Fatalf("encode (m default fills): %v", err)
		}
		out := map[string]any{}
		if _, err := s.Decode(buf, &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		m, _ := out["m"].(map[string]any)
		inner, _ := m["k"].(map[string]any)
		if inner == nil || inner["v"].(int32) != 3 {
			t.Errorf("m[k].v = %#v, want int32(3)", m["k"])
		}
	})

	// Chained forward references: a's default {} must fill x with its default
	// {}, which must fill y with its default 7. This only works if every
	// field's default VALUE is resolved before any default's bytes are
	// encoded (encodeDefault fills an absent nested field from its resolved
	// f.defaultVal); resolving + encoding in a single pass would read x's
	// not-yet-set default and mis-encode.
	t.Run("nested_chained_forward_refs", func(t *testing.T) {
		s, err := avro.Parse(`{"type":"record","name":"Outer","fields":[
			{"name":"a","type":"Inner","default":{}},
			{"name":"b","type":{"type":"record","name":"Inner","fields":[
				{"name":"x","type":"Late","default":{}}
			]}},
			{"name":"c","type":{"type":"record","name":"Late","fields":[
				{"name":"y","type":"long","default":7}
			]}}
		]}`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		buf, err := s.AppendEncode(nil, map[string]any{
			"b": map[string]any{"x": map[string]any{"y": int64(1)}},
			"c": map[string]any{"y": int64(2)},
		})
		if err != nil {
			t.Fatalf("encode (a default fills): %v", err)
		}
		out := map[string]any{}
		if _, err := s.Decode(buf, &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		a, _ := out["a"].(map[string]any)
		x, _ := a["x"].(map[string]any)
		if x == nil || x["y"].(int64) != 7 {
			t.Errorf("a.x.y = %#v, want int64(7) (chained default)", a)
		}
	})

	// Boundary: the forward-referenced shape WITHOUT a default must keep
	// parsing — proves the forward reference itself is accepted and that the
	// default is the only thing the fix changed.
	t.Run("forward_ref_without_default_still_parses", func(t *testing.T) {
		if _, err := avro.Parse(`{"type":"record","name":"R","fields":[
			{"name":"arr","type":{"type":"array","items":"Inner"}},
			{"name":"l","type":{"type":"record","name":"Inner","fields":[{"name":"v","type":"int"}]}}
		]}`); err != nil {
			t.Fatalf("no-default forward-ref array should parse: %v", err)
		}
	})

	// A forward-referenced default and the equivalent backward-referenced
	// default (named type declared first) must produce identical results —
	// the backward-ref path is the long-standing, already-correct one.
	t.Run("forward_matches_backward_ref", func(t *testing.T) {
		fwd := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"m","type":{"type":"map","values":"Inner"},"default":{"k":{"v":3}}},
			{"name":"l","type":{"type":"record","name":"Inner","fields":[{"name":"v","type":"int"}]}}
		]}`)
		bwd := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"l","type":{"type":"record","name":"Inner","fields":[{"name":"v","type":"int"}]}},
			{"name":"m","type":{"type":"map","values":"Inner"},"default":{"k":{"v":3}}}
		]}`)
		decodeM := func(s *avro.Schema) any {
			buf, err := s.AppendEncode(nil, map[string]any{"l": map[string]any{"v": 0}})
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			out := map[string]any{}
			if _, err := s.Decode(buf, &out); err != nil {
				t.Fatalf("decode: %v", err)
			}
			return out["m"]
		}
		if a, b := decodeM(fwd), decodeM(bwd); !reflect.DeepEqual(a, b) {
			t.Errorf("forward-ref default %#v != backward-ref default %#v", a, b)
		}
	})
}

// A field default whose type subtree references a record still under
// construction — a self- or mutual-recursive reference — must encode its
// binary defaultBytes against the COMPLETE record node, not the partial node
// that exists while the enclosing record's field loop is still running.
// Encoding inline at build time sees only the fields declared before the
// current one and silently drops the rest, producing truncated wire that the
// same schema cannot decode. The default-encode must defer to finalize (where
// every in-construction record is whole), exactly as a not-yet-wired
// forward-ref child already does. EncodeJSON re-encodes the default at runtime
// against the complete node and was already correct, so it is the parity oracle.
func TestRegression_SelfRefContainerDefaultEncodes(t *testing.T) {
	// roundTrip encodes a record that omits the defaulted field (triggering
	// binary default-fill from the precomputed bytes) and asserts the bytes
	// decode back via the same schema. Returns the decoded value.
	roundTrip := func(t *testing.T, s *avro.Schema, present map[string]any) map[string]any {
		t.Helper()
		buf, err := s.AppendEncode(nil, present)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		out := map[string]any{}
		rest, err := s.Decode(buf, &out)
		if err != nil {
			t.Fatalf("decode of Encode's own default-filled output failed: %v (wire=%x)", err, buf)
		}
		if len(rest) != 0 {
			t.Fatalf("decode left %d trailing bytes (wire=%x)", len(rest), buf)
		}
		return out
	}

	t.Run("self_array", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"tag","type":"int"},
			{"name":"kids","type":{"type":"array","items":"R"},"default":[{"tag":9,"kids":[]}]}]}`)
		// The exact wire of the default-filled record: tag=1 (0x02), then
		// kids = [ R{tag:9,kids:[]} ] = count 1 (0x02), item tag=9 (0x12),
		// item kids empty (0x00), array terminator (0x00). The pre-fix bug
		// dropped the inner kids and outer terminator, emitting 0x02021200.
		buf, err := s.AppendEncode(nil, map[string]any{"tag": int32(1)})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		if got, want := buf, []byte{0x02, 0x02, 0x12, 0x00, 0x00}; !reflect.DeepEqual(got, want) {
			t.Errorf("default-filled wire = %x, want %x", got, want)
		}
		out := roundTrip(t, s, map[string]any{"tag": int32(1)})
		kids, _ := out["kids"].([]any)
		if len(kids) != 1 {
			t.Fatalf("default kids = %#v, want one element", out["kids"])
		}
		if k0, _ := kids[0].(map[string]any); k0 == nil || k0["tag"].(int32) != 9 {
			t.Errorf("default kid = %#v, want {tag:9,kids:[]}", kids[0])
		}
		// Binary Encode must match what EncodeJSON (runtime re-encode, already
		// correct) produces for the same default-fill.
		jb, err := s.EncodeJSON(map[string]any{"tag": int32(1)})
		if err != nil {
			t.Fatalf("EncodeJSON: %v", err)
		}
		if want := `{"tag":1,"kids":[{"tag":9,"kids":[]}]}`; string(jb) != want {
			t.Errorf("EncodeJSON = %s, want %s", jb, want)
		}
	})

	t.Run("self_map", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"tag","type":"int"},
			{"name":"m","type":{"type":"map","values":"R"},"default":{"k":{"tag":9,"m":{}}}}]}`)
		out := roundTrip(t, s, map[string]any{"tag": int32(1)})
		m, _ := out["m"].(map[string]any)
		v, _ := m["k"].(map[string]any)
		if v == nil || v["tag"].(int32) != 9 {
			t.Errorf("default m[k] = %#v, want {tag:9,m:{}}", m["k"])
		}
	})

	// Two-level nesting: the inner kids [{tag:8,kids:[]}] must survive. The
	// pre-fix bug encoded the item against the partial record (tag only), so
	// the 1- and 2-level defaults produced IDENTICAL truncated bytes; this
	// subtest fails on the buggy code for a reason the 1-level case can't show.
	t.Run("self_array_two_levels", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"tag","type":"int"},
			{"name":"kids","type":{"type":"array","items":"R"},"default":[{"tag":9,"kids":[{"tag":8,"kids":[]}]}]}]}`)
		out := roundTrip(t, s, map[string]any{"tag": int32(1)})
		kids, _ := out["kids"].([]any)
		if len(kids) != 1 {
			t.Fatalf("level-1 kids = %#v, want one element", out["kids"])
		}
		k0, _ := kids[0].(map[string]any)
		if k0 == nil || k0["tag"].(int32) != 9 {
			t.Fatalf("level-1 kid = %#v, want tag 9", kids[0])
		}
		inner, _ := k0["kids"].([]any)
		if len(inner) != 1 {
			t.Fatalf("level-2 kids = %#v, want one element (pre-fix bug dropped it)", k0["kids"])
		}
		if k1, _ := inner[0].(map[string]any); k1 == nil || k1["tag"].(int32) != 8 {
			t.Errorf("level-2 kid = %#v, want tag 8", inner[0])
		}
	})

	// The recursive field is FIRST, so the record has NO prior fields when the
	// default is processed — the partial node is entirely empty.
	t.Run("self_array_recursive_field_first", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"kids","type":{"type":"array","items":"R"},"default":[{"kids":[]}]}]}`)
		out := roundTrip(t, s, map[string]any{})
		if kids, _ := out["kids"].([]any); len(kids) != 1 {
			t.Errorf("default kids = %#v, want one element", out["kids"])
		}
	})

	// Mutual recursion: Inner is built while Outer is still under construction,
	// and Inner.outers' default references Outer. This exercises the shared
	// in-construction set across the nested builder (Inner is built by a nested
	// builder; Outer lives in the parent's set).
	t.Run("mutual_recursion", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"Outer","fields":[
			{"name":"tag","type":"int"},
			{"name":"inner","type":{"type":"record","name":"Inner","fields":[
				{"name":"outers","type":{"type":"array","items":"Outer"},"default":[{"tag":7,"inner":{"outers":[]}}]}]}}]}`)
		out := roundTrip(t, s, map[string]any{"tag": int32(1), "inner": map[string]any{}})
		inner, _ := out["inner"].(map[string]any)
		outers, _ := inner["outers"].([]any)
		if len(outers) != 1 {
			t.Fatalf("inner.outers default = %#v, want one element", inner["outers"])
		}
		if o0, _ := outers[0].(map[string]any); o0 == nil || o0["tag"].(int32) != 7 {
			t.Errorf("inner.outers[0] = %#v, want {tag:7,...}", outers[0])
		}
	})

	// Controls — shapes the deferral must NOT break:
	t.Run("self_array_empty_default", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"tag","type":"int"},
			{"name":"kids","type":{"type":"array","items":"R"},"default":[]}]}`)
		out := roundTrip(t, s, map[string]any{"tag": int32(1)})
		if kids, _ := out["kids"].([]any); len(kids) != 0 {
			t.Errorf("empty default kids = %#v, want empty", out["kids"])
		}
	})

	t.Run("self_nullunion_default_null", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"tag","type":"int"},
			{"name":"kids","type":["null",{"type":"array","items":"R"}],"default":null}]}`)
		out := roundTrip(t, s, map[string]any{"tag": int32(1)})
		if out["kids"] != nil {
			t.Errorf("null default kids = %#v, want nil", out["kids"])
		}
	})

	// Non-recursive array-of-record default (a backward reference to a complete
	// type) must still round-trip — it was correct before and after the fix.
	t.Run("non_recursive_still_works", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"tag","type":"int"},
			{"name":"kids","type":{"type":"array","items":{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}},"default":[{"x":5}]}]}`)
		out := roundTrip(t, s, map[string]any{"tag": int32(1)})
		kids, _ := out["kids"].([]any)
		if len(kids) != 1 {
			t.Fatalf("default kids = %#v, want one element", out["kids"])
		}
		if k0, _ := kids[0].(map[string]any); k0 == nil || k0["x"].(int32) != 5 {
			t.Errorf("default kid = %#v, want {x:5}", kids[0])
		}
	})
}

// A default with no finite encoding — a required field whose default recurses
// into its own type — must be rejected at Parse, not stack-overflow and not
// silently produce truncated bytes. encodeDefault fills absent nested fields
// from their own defaults (unlike validateDefault, which skips absent fields
// and terminates vacuously), so without a recursion bound such a default
// recurses until the goroutine stack overflows and the process dies. The
// maxDepth ceiling turns it into an errTooDeep parse error instead. Each case
// is a schema whose default can never be finitely materialized.
func TestRegression_InfiniteRecursiveDefaultRejected(t *testing.T) {
	cases := []struct{ name, schema string }{
		{"self_record", `{"type":"record","name":"R","fields":[
			{"name":"self","type":"R","default":{}}]}`},
		{"self_array", `{"type":"record","name":"R","fields":[
			{"name":"kids","type":{"type":"array","items":"R"},"default":[{}]}]}`},
		{"self_map", `{"type":"record","name":"R","fields":[
			{"name":"m","type":{"type":"map","values":"R"},"default":{"k":{}}}]}`},
		{"mutual", `{"type":"record","name":"A","fields":[
			{"name":"b","type":{"type":"record","name":"B","fields":[
				{"name":"a","type":"A","default":{}}]},"default":{}}]}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Must complete (the bound stops the recursion) and must reject.
			start := time.Now()
			_, err := avro.Parse(c.schema)
			if d := time.Since(start); d > time.Second {
				t.Fatalf("Parse took %v (recursion not bounded)", d)
			}
			if err == nil {
				t.Fatalf("Parse accepted a default with no finite encoding; want rejection")
			}
		})
	}
}

// SchemaField.Default's documented contract: "Union defaults pick the first
// branch that accepts the value; the Go type tells you which branch was
// chosen." For a union whose first branch is a NAME-REFERENCED enum followed
// by a bytes branch, a valid-member string default must surface as a Go
// string (enum branch) — matching the wire encoder/decoder, which fills the
// enum branch — not as []byte (bytes branch). An inline enum already behaves
// correctly; the name-referenced enum must too.
func TestRegression_NameRefEnumUnionDefaultMetadata(t *testing.T) {
	// wireBranchByte encodes a record omitting the union field so its default
	// fills, then returns the union branch index byte the wire chose.
	wireBranchByte := func(t *testing.T, s *avro.Schema, omitField string, present map[string]any) byte {
		buf, err := s.AppendEncode(nil, present)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		// The enum-typed "def" field encodes first as a single ordinal byte
		// (0x00 for symbol "A"); the union field's default follows.
		return buf[1]
	}

	t.Run("name_ref_enum_member_default_picks_enum", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"def","type":{"type":"enum","name":"E","symbols":["A","B"]}},
			{"name":"f","type":["E","bytes"],"default":"A"}
		]}`)
		if b := wireBranchByte(t, s, "f", map[string]any{"def": "A"}); b != 0x00 {
			t.Fatalf("wire f-default branch byte = 0x%02x, want 0x00 (enum)", b)
		}
		if d := s.Root().Fields[1].Default; !isStr(d) {
			t.Errorf("Root().Fields[1].Default = %T %#v, want string (enum branch, matching wire)", d, d)
		}
	})

	// Boundary: a NON-member default ("Z") is rejected by the enum branch on
	// both surfaces, so both pick the later bytes branch — metadata []byte.
	t.Run("name_ref_enum_non_member_default_picks_bytes", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"def","type":{"type":"enum","name":"E","symbols":["A","B"]}},
			{"name":"f","type":["E","bytes"],"default":"Z"}
		]}`)
		if b := wireBranchByte(t, s, "f", map[string]any{"def": "A"}); b != 0x02 {
			t.Fatalf("wire f-default branch byte = 0x%02x, want 0x02 (bytes)", b)
		}
		if d := s.Root().Fields[1].Default; !isBytes(d) {
			t.Errorf("Root().Fields[1].Default = %T %#v, want []byte (bytes branch, matching wire)", d, d)
		}
	})

	// Boundary: the inline-enum form (no name reference) was already correct
	// and must stay correct.
	t.Run("inline_enum_member_default_picks_enum", func(t *testing.T) {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[
			{"name":"f","type":[{"type":"enum","name":"E","symbols":["A","B"]},"bytes"],"default":"A"}
		]}`)
		if d := s.Root().Fields[0].Default; !isStr(d) {
			t.Errorf("inline enum: Default = %T %#v, want string", d, d)
		}
	})
}

func isStr(v any) bool   { _, ok := v.(string); return ok }
func isBytes(v any) bool { _, ok := v.([]byte); return ok }

// A union branch may be a forward reference to a named type declared later
// in the same schema (a shape twmb deliberately accepts and round-trips on
// the binary path). The builder leaves node.branches[i] nil for such a
// branch and patches only the ser/deser function tables in finalize; every
// path that walks node.branches directly — JSON encode, JSON decode, schema
// resolution, and union-default validation — must still see the resolved
// branch node, not the nil placeholder. These pin that finalize writes the
// resolved node back into the union's branch slice so none of those paths
// dereferences nil.
func TestRegression_ForwardRefUnionBranchAllPaths(t *testing.T) {
	type later struct {
		V int32 `avro:"v"`
	}
	type rec struct {
		Opt *later `avro:"opt"`
		L   later  `avro:"l"`
	}
	// "opt" references "Later" before it is defined in field "l".
	const sc = `{"type":"record","name":"R","fields":[
	  {"name":"opt","type":["null","Later"]},
	  {"name":"l","type":{"type":"record","name":"Later","fields":[{"name":"v","type":"int"}]}}]}`
	s, err := avro.Parse(sc)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	in := rec{Opt: &later{V: 9}, L: later{V: 1}}

	// Binary already worked; guard against regression.
	bin, err := s.AppendEncode(nil, in)
	if err != nil {
		t.Fatalf("binary encode: %v", err)
	}
	var binOut rec
	if _, err := s.Decode(bin, &binOut); err != nil {
		t.Fatalf("binary decode: %v", err)
	}
	if !reflect.DeepEqual(in, binOut) {
		t.Fatalf("binary round-trip: got %+v want %+v", binOut, in)
	}

	// JSON encode + decode must not nil-panic and must round-trip.
	js, err := s.AppendEncodeJSON(nil, in)
	if err != nil {
		t.Fatalf("json encode: %v", err)
	}
	var jsOut rec
	if err := s.DecodeJSON(js, &jsOut); err != nil {
		t.Fatalf("json decode: %v", err)
	}
	if !reflect.DeepEqual(in, jsOut) {
		t.Fatalf("json round-trip: got %+v want %+v", jsOut, in)
	}

	// Schema resolution / compatibility must not nil-panic on the writer's
	// forward-ref union branch.
	if _, err := avro.Resolve(s, s); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if err := avro.CheckCompatibility(s, s); err != nil {
		t.Fatalf("CheckCompatibility: %v", err)
	}

	// Null branch (the other arm) must still round-trip through JSON too.
	inNull := rec{Opt: nil, L: later{V: 2}}
	js2, err := s.AppendEncodeJSON(nil, inNull)
	if err != nil {
		t.Fatalf("json encode (null arm): %v", err)
	}
	var jsOut2 rec
	if err := s.DecodeJSON(js2, &jsOut2); err != nil {
		t.Fatalf("json decode (null arm): %v", err)
	}
	if !reflect.DeepEqual(inNull, jsOut2) {
		t.Fatalf("json round-trip (null arm): got %+v want %+v", jsOut2, inNull)
	}
}

// A union field whose first branch is a forward reference can carry a
// default that matches that branch. The default validator walks
// node.branches; a nil forward-ref branch made it report "no branch
// matched" and reject a schema that is byte-equivalent to a backward-ordered
// one that parses. Pins that field order does not change acceptance.
func TestRegression_ForwardRefUnionDefaultParses(t *testing.T) {
	const forward = `{"type":"record","name":"R","fields":[
	  {"name":"u","type":["E","string"],"default":"A"},
	  {"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]}}]}`
	const backward = `{"type":"record","name":"R","fields":[
	  {"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]}},
	  {"name":"u","type":["E","string"],"default":"A"}]}`
	if _, err := avro.Parse(backward); err != nil {
		t.Fatalf("backward-ordered control should parse: %v", err)
	}
	sf, err := avro.Parse(forward)
	if err != nil {
		t.Fatalf("forward-ref union default should parse (byte-equivalent to backward): %v", err)
	}
	// The default "A" resolves to the enum branch; it surfaces in metadata.
	if got := sf.Root().Fields[0].Default; !isStr(got) || got.(string) != "A" {
		t.Fatalf("union default = %#v, want \"A\"", got)
	}
}
