package avro_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// Names matrix: namespaces (explicit, inherited, dotted-fullname,
// same-shortname-across-namespaces), forward references at every fixup
// position, and the documented field-level logicalType lift shapes.
// ---------------------------------------------------------------------------

func TestMatrix_Namespaces(t *testing.T) {
	cases := []struct {
		label  string
		schema string
		value  any
	}{
		{"explicit-ns",
			`{"type":"record","name":"R","namespace":"com.example","fields":[
				{"name":"e","type":{"type":"enum","name":"E","namespace":"com.example.sub","symbols":["A"]}}]}`,
			map[string]any{"e": "A"}},
		{"inherited-ns-shortref",
			`{"type":"record","name":"R","namespace":"n1","fields":[
				{"name":"a","type":{"type":"fixed","name":"F","size":1}},
				{"name":"b","type":"F"}]}`,
			map[string]any{"a": []byte{1}, "b": []byte{2}}},
		{"inherited-ns-fullref",
			`{"type":"record","name":"R","namespace":"n1","fields":[
				{"name":"a","type":{"type":"fixed","name":"F","size":1}},
				{"name":"b","type":"n1.F"}]}`,
			map[string]any{"a": []byte{1}, "b": []byte{2}}},
		{"dotted-fullname",
			`{"type":"record","name":"com.example.R","fields":[
				{"name":"x","type":"int"}]}`,
			map[string]any{"x": int32(5)}},
		{"same-shortname-two-ns",
			`{"type":"record","name":"R","namespace":"o","fields":[
				{"name":"a","type":{"type":"enum","name":"T","namespace":"n1","symbols":["X"]}},
				{"name":"b","type":{"type":"enum","name":"T","namespace":"n2","symbols":["Y","Z"]}},
				{"name":"c","type":"n1.T"},
				{"name":"d","type":"n2.T"}]}`,
			map[string]any{"a": "X", "b": "Z", "c": "X", "d": "Y"}},
		{"namespaced-recursive",
			`{"type":"record","name":"Node","namespace":"tree","fields":[
				{"name":"v","type":"int"},
				{"name":"next","type":["null","tree.Node"],"default":null}]}`,
			map[string]any{"v": int32(1), "next": map[string]any{"v": int32(2), "next": nil}}},
		{"namespaced-recursive-shortref",
			`{"type":"record","name":"Node","namespace":"tree","fields":[
				{"name":"v","type":"int"},
				{"name":"next","type":["null","Node"],"default":null}]}`,
			map[string]any{"v": int32(1), "next": map[string]any{"v": int32(2), "next": nil}}},
	}
	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			runCore(t, c.schema, c.value)
		})
		t.Run(c.label+"/tagged", func(t *testing.T) {
			runCore(t, c.schema, c.value, avro.TaggedUnions())
		})
	}
}

// Forward references at every finalize-fixup position: the named type is
// used by an EARLIER field than the one defining it.
func TestMatrix_ForwardRefPositions(t *testing.T) {
	def := `{"type":"record","name":"Inner","fields":[{"name":"i","type":"int"}]}`
	inner := map[string]any{"i": int32(7)}
	cases := []struct {
		label  string
		early  string // the forward-referencing field type
		value  any    // value for the early field
		tagged bool
	}{
		{"union-branch", `["null","Inner"]`, inner, true},
		{"direct-field", `"Inner"`, inner, false},
		{"array-items", `{"type":"array","items":"Inner"}`, []any{inner, inner}, false},
		{"map-values", `{"type":"map","values":"Inner"}`, map[string]any{"k": inner}, false},
		{"union-of-array-of-ref", `["null",{"type":"array","items":"Inner"}]`, []any{inner}, true},
	}
	for _, c := range cases {
		schema := fmt.Sprintf(`{"type":"record","name":"W","fields":[
			{"name":"early","type":%s},
			{"name":"def","type":%s}]}`, c.early, def)
		value := map[string]any{"early": c.value, "def": inner}
		t.Run(c.label, func(t *testing.T) {
			runCore(t, schema, value)
		})
		if c.tagged {
			t.Run(c.label+"/tagged", func(t *testing.T) {
				runCore(t, schema, value, avro.TaggedUnions())
			})
		}
	}
}

// Namespaced forward reference: the early short-name reference resolves
// in-scope to the later definition (documented eager in-scope-first rule).
func TestMatrix_ForwardRefNamespaced(t *testing.T) {
	schema := `{"type":"record","name":"W","namespace":"ns","fields":[
		{"name":"early","type":["null","Inner"],"default":null},
		{"name":"def","type":{"type":"record","name":"Inner","fields":[{"name":"i","type":"int"}]}}]}`
	value := map[string]any{
		"early": map[string]any{"i": int32(1)},
		"def":   map[string]any{"i": int32(2)},
	}
	runCore(t, schema, value)
	runCore(t, schema, value, avro.TaggedUnions())
}

// The three documented field-level logicalType lift shapes must produce
// wire bytes identical to the canonical nested form, accept the enriched
// Go type, and survive the metadata rebuild.
func TestMatrix_FieldLevelLogicalLift(t *testing.T) {
	nested := `{"type":"record","name":"R","fields":[
		{"name":"ts","type":["null",{"type":"long","logicalType":"timestamp-millis"}],"default":null}]}`
	canonical := avro.MustParse(nested)
	v := map[string]any{"ts": time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)}
	wantWire, err := canonical.AppendEncode(nil, v)
	if err != nil {
		t.Fatalf("canonical encode: %v", err)
	}

	shapes := []struct {
		label  string
		schema string
	}{
		{"string-form-union", `{"type":"record","name":"R","fields":[
			{"name":"ts","type":["null","long"],"logicalType":"timestamp-millis","default":null}]}`},
		{"primitive-form", `{"type":"record","name":"R","fields":[
			{"name":"ts","type":"long","logicalType":"timestamp-millis"}]}`},
		{"single-object-form", `{"type":"record","name":"R","fields":[
			{"name":"ts","type":{"type":"long"},"logicalType":"timestamp-millis"}]}`},
	}
	for _, sh := range shapes {
		t.Run(sh.label, func(t *testing.T) {
			s, err := avro.Parse(sh.schema)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			got, err := s.AppendEncode(nil, v)
			if err != nil {
				t.Fatalf("lifted encode of time.Time: %v", err)
			}
			if sh.label != "string-form-union" {
				// Non-union shapes have no union index byte; compare against
				// their own canonical nested form instead.
				nestedFlat := `{"type":"record","name":"R","fields":[
					{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}]}`
				want, _ := avro.MustParse(nestedFlat).AppendEncode(nil, v)
				if !bytes.Equal(got, want) {
					t.Fatalf("lifted wire differs from nested form:\n got=%x\nwant=%x", got, want)
				}
			} else if !bytes.Equal(got, wantWire) {
				t.Fatalf("lifted wire differs from nested form:\n got=%x\nwant=%x", got, wantWire)
			}
			// Decode parity: the enriched type comes back on both wires.
			var back map[string]any
			if _, err := s.Decode(got, &back); err != nil {
				t.Fatalf("lifted decode: %v", err)
			}
			if _, ok := back["ts"].(time.Time); !ok {
				t.Fatalf("lifted decode yielded %T, want time.Time", back["ts"])
			}
			j, err := s.AppendEncodeJSON(nil, v)
			if err != nil {
				t.Fatalf("lifted encodeJSON: %v", err)
			}
			var jback map[string]any
			if err := s.DecodeJSON(j, &jback); err != nil {
				t.Fatalf("lifted decodeJSON: %v", err)
			}
			if !matEqual(jback["ts"], back["ts"]) {
				t.Fatalf("lifted JSON decode diverges: %#v vs %#v", jback["ts"], back["ts"])
			}
			// Metadata rebuild: the field-level annotation survives in
			// Props, re-parses, re-lifts, and encodes identically.
			root := s.Root()
			rebuilt, err := root.Schema()
			if err != nil {
				t.Fatalf("Root().Schema(): %v", err)
			}
			got2, err := rebuilt.AppendEncode(nil, v)
			if err != nil || !bytes.Equal(got2, got) {
				t.Fatalf("rebuilt lifted schema wire differs: err=%v\n got=%x\nreb=%x\nrebuilt: %s", err, got, got2, rebuilt.String())
			}
		})
	}
}
