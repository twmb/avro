package avro_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/twmb/avro"
)

// TestDifferentialFastavroSchemaForScope drives representative SchemaFor
// outputs from the custom-schema scope matrix (TestMatrix_SchemaForCustomSchemaScope)
// through fastavro. The composed schemas carry dotted fullname references
// and "namespace":"" inheritance escapes — both standard spellings — and
// fastavro must both parse them and agree on the full parsing canonical
// form. PCF string equality subsumes fingerprint equality (the fingerprint
// is a pure function of the PCF bytes) without any byte-order presentation
// comparison.
func TestDifferentialFastavroSchemaForScope(t *testing.T) {
	o := startOracle(t)

	type scopeDiffMarker struct{ A int64 }
	type oneField struct{ F1 scopeDiffMarker }
	type twoFields struct {
		F1 scopeDiffMarker
		F2 scopeDiffMarker
	}

	customFor := func(schemaJSON string) avro.CustomType {
		t.Helper()
		s, err := avro.Parse(schemaJSON)
		if err != nil {
			t.Fatalf("parse custom schema: %v", err)
		}
		root := s.Root()
		return avro.CustomType{GoType: reflect.TypeFor[scopeDiffMarker](), Schema: root}
	}

	cells := []struct {
		name  string
		build func() (*avro.Schema, error)
	}{
		{"split_record_two_withns", func() (*avro.Schema, error) {
			ct := customFor(`{"type":"record","name":"X","namespace":"a","fields":[{"name":"n","type":"int"}]}`)
			return avro.SchemaFor[twoFields](avro.WithNamespace("b"), ct)
		}},
		{"split_record_recursive_two", func() (*avro.Schema, error) {
			ct := customFor(`{"type":"record","name":"N","namespace":"a","fields":[{"name":"next","type":["null","N"]}]}`)
			return avro.SchemaFor[twoFields](ct)
		}},
		{"nullns_record_one_withns", func() (*avro.Schema, error) {
			ct := customFor(`{"type":"record","name":"X","fields":[{"name":"n","type":"int"}]}`)
			return avro.SchemaFor[oneField](avro.WithNamespace("b"), ct)
		}},
		{"nullns_record_two_default", func() (*avro.Schema, error) {
			ct := customFor(`{"type":"record","name":"X","fields":[{"name":"n","type":"int"}]}`)
			return avro.SchemaFor[twoFields](ct)
		}},
		{"dotted_nestedforeign_two_withns", func() (*avro.Schema, error) {
			ct := customFor(`{"type":"record","name":"a.X","fields":[{"name":"inner","type":{"type":"record","name":"q.Inner","fields":[{"name":"m","type":"int"}]}}]}`)
			return avro.SchemaFor[twoFields](avro.WithNamespace("b"), ct)
		}},
		{"split_fixed_two_withns", func() (*avro.Schema, error) {
			ct := customFor(`{"type":"fixed","name":"X","namespace":"a","size":4}`)
			return avro.SchemaFor[twoFields](avro.WithNamespace("b"), ct)
		}},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			s, err := c.build()
			if err != nil {
				t.Fatalf("SchemaFor: %v", err)
			}
			schemaJSON := json.RawMessage(s.String())
			if resp := o.call(oracleJob{Op: "parse", Schema: schemaJSON, Hex: ""}); !resp.OK {
				t.Fatalf("fastavro rejects the composed schema %s: %s", schemaJSON, resp.Err)
			}
			resp := o.call(oracleJob{Op: "canonical", Schema: schemaJSON, Hex: ""})
			if !resp.OK {
				t.Fatalf("fastavro canonical failed for %s: %s", schemaJSON, resp.Err)
			}
			if got, want := string(s.Canonical()), resp.Canonical; got != want {
				t.Errorf("parsing canonical form diverges:\n  twmb:     %s\n  fastavro: %s", got, want)
			}
		})
	}
}
