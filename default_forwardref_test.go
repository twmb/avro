package avro_test

import (
	"reflect"
	"testing"

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
