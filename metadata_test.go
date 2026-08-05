package avro_test

import (
	"fmt"
	"testing"

	"github.com/twmb/avro"
)

// Tier-2 metadata-API <-> wire consistency (CORRECTNESS_PLAN.md metadata gap).
// The recurring class: Schema.Root().Fields[i].Default (the metadata view)
// drifting from the value the wire actually materializes for that default --
// wrong Go type, lost precision, or a different union branch. The invariant
// is that the metadata Default equals the default the decoder fills for an
// absent field (DecodeJSON of an empty object fills schema defaults).
func TestProperty_MetadataDefaultMatchesWire(t *testing.T) {
	enum := `{"type":"enum","name":"E","symbols":["A","B","C"]}`
	cases := []struct {
		name      string
		fieldType string
		def       string
	}{
		{"int", `"int"`, `42`},
		{"long exact >2^53", `"long"`, `9007199254740993`}, // must stay exact int64
		{"float", `"float"`, `42`},
		{"double exp-overflow to +Inf", `"double"`, `1e1000`},
		{"double", `"double"`, `1.5`},
		{"string", `"string"`, `"hi"`},
		{"boolean", `"boolean"`, `true`},
		{"bytes codepoint default", `"bytes"`, `"AB"`},
		{"enum", enum, `"B"`},
		{"union float,int picks float", `["float","int"]`, `42`},
		{"union int,float picks int", `["int","float"]`, `42`},
		{"union null,int default null", `["null","int"]`, `null`},
		{"union null,string default string", `["null","string"]`, `"x"`},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			schema := fmt.Sprintf(`{"type":"record","name":"R","fields":[{"name":"f","type":%s,"default":%s}]}`, c.fieldType, c.def)
			s, err := avro.Parse(schema)
			if err != nil {
				t.Fatalf("Parse(%s): %v", schema, err)
			}

			root := s.Root()
			if len(root.Fields) != 1 || !root.Fields[0].HasDefault {
				t.Fatalf("expected one field with a default, got %+v", root.Fields)
			}
			metaDefault := root.Fields[0].Default

			// Wire view: DecodeJSON of an empty object fills the field default.
			var filled map[string]any
			if err := s.DecodeJSON([]byte(`{}`), &filled); err != nil {
				t.Fatalf("DecodeJSON({}): %v", err)
			}
			wireDefault, ok := filled["f"]
			if !ok && metaDefault != nil {
				t.Fatalf("decoder did not fill field f (got %+v)", filled)
			}

			if !equalAvro(metaDefault, wireDefault) {
				t.Errorf("metadata Default (%T %v) != wire-filled default (%T %v) for %s",
					metaDefault, metaDefault, wireDefault, wireDefault, schema)
			}
		})
	}
}
