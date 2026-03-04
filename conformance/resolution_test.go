package conformance

import (
	"reflect"
	"testing"

	"github.com/twmb/avro"
)

// -----------------------------------------------------------------------
// Schema Evolution / Resolution
// Spec: "Schema Resolution" — rules for reading data written with a
// different but compatible schema.
//   - Enum symbol reordering and unknown-symbol defaults
//   - Type promotion within records, arrays, maps, and unions
//   - Self-referencing record evolution (added fields with defaults)
//   - Named type matching by fullname or unqualified name
//   - Field alias resolution
//   - Reader union selects first matching branch
// https://avro.apache.org/docs/1.12.0/specification/#schema-resolution
// -----------------------------------------------------------------------

func TestSpecEnumSymbolReordering(t *testing.T) {
	writer := mustParse(t, `{"type":"enum","name":"E","symbols":["B","C","D"]}`)
	reader := mustParse(t, `{"type":"enum","name":"E","symbols":["A","B","C","D"]}`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		writerSym string
		wantSym   string
	}{
		{"B", "B"},
		{"C", "C"},
		{"D", "D"},
	} {
		t.Run(tc.writerSym, func(t *testing.T) {
			encoded, err := writer.Encode(&tc.writerSym)
			if err != nil {
				t.Fatal(err)
			}
			var got string
			_, err = resolved.Decode(encoded, &got)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.wantSym {
				t.Fatalf("writer %q decoded as %q, want %q", tc.writerSym, got, tc.wantSym)
			}
		})
	}
}

func TestSpecEnumMultipleUnknownSymbols(t *testing.T) {
	writer := mustParse(t, `{"type":"enum","name":"E","symbols":["A","B","X","Y"]}`)
	reader := mustParse(t, `{"type":"enum","name":"E","symbols":["A","B","C"],"default":"C"}`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		writerSym string
		wantSym   string
	}{
		{"A", "A"},
		{"B", "B"},
		{"X", "C"},
		{"Y", "C"},
	} {
		t.Run(tc.writerSym, func(t *testing.T) {
			encoded, err := writer.Encode(&tc.writerSym)
			if err != nil {
				t.Fatal(err)
			}
			var got string
			_, err = resolved.Decode(encoded, &got)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.wantSym {
				t.Fatalf("writer %q → %q, want %q", tc.writerSym, got, tc.wantSym)
			}
		})
	}
}

func TestSpecPromotionInNestedContext(t *testing.T) {
	t.Run("record field int to long", func(t *testing.T) {
		writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`)
		reader := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"x","type":"long"}]}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}

		encoded, err := writer.Encode(map[string]any{"x": int32(42)})
		if err != nil {
			t.Fatal(err)
		}
		var result any
		_, err = resolved.Decode(encoded, &result)
		if err != nil {
			t.Fatal(err)
		}
		m := result.(map[string]any)
		if m["x"] != int64(42) {
			t.Fatalf("got x=%v (%T), want int64(42)", m["x"], m["x"])
		}
	})

	t.Run("array items int to long", func(t *testing.T) {
		writer := mustParse(t, `{"type":"array","items":"int"}`)
		reader := mustParse(t, `{"type":"array","items":"long"}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}

		input := []int32{1, 2, 3}
		encoded, err := writer.Encode(&input)
		if err != nil {
			t.Fatal(err)
		}
		var result []int64
		_, err = resolved.Decode(encoded, &result)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(result, []int64{1, 2, 3}) {
			t.Fatalf("got %v, want [1 2 3]", result)
		}
	})

	t.Run("map values int to long", func(t *testing.T) {
		writer := mustParse(t, `{"type":"map","values":"int"}`)
		reader := mustParse(t, `{"type":"map","values":"long"}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}

		input := map[string]int32{"k": 99}
		encoded, err := writer.Encode(&input)
		if err != nil {
			t.Fatal(err)
		}
		var result map[string]int64
		_, err = resolved.Decode(encoded, &result)
		if err != nil {
			t.Fatal(err)
		}
		if result["k"] != 99 {
			t.Fatalf("got k=%v, want 99", result["k"])
		}
	})
}

func TestSpecSelfRefRecordEvolution(t *testing.T) {
	writerSchema := `{
		"type": "record",
		"name": "Node",
		"fields": [
			{"name": "value", "type": "int"},
			{"name": "next", "type": ["null", "Node"]}
		]
	}`
	readerSchema := `{
		"type": "record",
		"name": "Node",
		"fields": [
			{"name": "value", "type": "int"},
			{"name": "label", "type": "string", "default": ""},
			{"name": "next", "type": ["null", "Node"]}
		]
	}`

	writer := mustParse(t, writerSchema)
	reader := mustParse(t, readerSchema)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	type Node struct {
		Value int32 `avro:"value"`
		Next  *Node `avro:"next"`
	}
	input := &Node{Value: 1, Next: &Node{Value: 2}}
	encoded, err := writer.Encode(input)
	if err != nil {
		t.Fatal(err)
	}

	var result any
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	m := result.(map[string]any)
	if m["value"] != int32(1) {
		t.Fatalf("root value: got %v, want 1", m["value"])
	}
	if m["label"] != "" {
		t.Fatalf("root label: got %v, want empty", m["label"])
	}
	next := m["next"].(map[string]any)
	if next["value"] != int32(2) {
		t.Fatalf("next value: got %v, want 2", next["value"])
	}
	if next["label"] != "" {
		t.Fatalf("next label: got %v, want empty", next["label"])
	}
	if next["next"] != nil {
		t.Fatalf("next.next: got %v, want nil", next["next"])
	}
}

func TestSpecUnionEvolutionBranches(t *testing.T) {
	writerSchema := `{
		"type": "record",
		"name": "R",
		"fields": [{"name": "v", "type": ["null","int"]}]
	}`
	readerSchema := `{
		"type": "record",
		"name": "R",
		"fields": [{"name": "v", "type": ["null","long"]}]
	}`

	writer := mustParse(t, writerSchema)
	reader := mustParse(t, readerSchema)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("null branch", func(t *testing.T) {
		encoded, err := writer.Encode(map[string]any{"v": nil})
		if err != nil {
			t.Fatal(err)
		}
		var result any
		_, err = resolved.Decode(encoded, &result)
		if err != nil {
			t.Fatal(err)
		}
		m := result.(map[string]any)
		if m["v"] != nil {
			t.Fatalf("got %v, want nil", m["v"])
		}
	})

	t.Run("int promoted to long", func(t *testing.T) {
		encoded, err := writer.Encode(map[string]any{"v": int32(42)})
		if err != nil {
			t.Fatal(err)
		}
		var result any
		_, err = resolved.Decode(encoded, &result)
		if err != nil {
			t.Fatal(err)
		}
		m := result.(map[string]any)
		if m["v"] != int64(42) {
			t.Fatalf("got %v (%T), want int64(42)", m["v"], m["v"])
		}
	})
}

func TestSpecNamedTypesMatchByUnqualifiedName(t *testing.T) {
	t.Run("record", func(t *testing.T) {
		writer := mustParse(t, `{"type":"record","name":"a.Foo","fields":[{"name":"a","type":"int"}]}`)
		reader := mustParse(t, `{"type":"record","name":"b.Foo","fields":[{"name":"a","type":"int"}]}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := writer.Encode(map[string]any{"a": int32(42)})
		if err != nil {
			t.Fatal(err)
		}
		var got any
		if _, err := resolved.Decode(encoded, &got); err != nil {
			t.Fatal(err)
		}
		if got.(map[string]any)["a"] != int32(42) {
			t.Fatalf("got %+v, want a=42", got)
		}
	})

	t.Run("enum", func(t *testing.T) {
		writer := mustParse(t, `{"type":"enum","name":"a.E","symbols":["A","B"]}`)
		reader := mustParse(t, `{"type":"enum","name":"b.E","symbols":["A","B"]}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}
		var got string
		encoded, err := writer.Encode("B")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := resolved.Decode(encoded, &got); err != nil {
			t.Fatal(err)
		}
		if got != "B" {
			t.Fatalf("got %q, want B", got)
		}
	})

	t.Run("fixed", func(t *testing.T) {
		writer := mustParse(t, `{"type":"fixed","name":"a.Id","size":4}`)
		reader := mustParse(t, `{"type":"fixed","name":"b.Id","size":4}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}
		in := [4]byte{1, 2, 3, 4}
		encoded, err := writer.Encode(&in)
		if err != nil {
			t.Fatal(err)
		}
		var out [4]byte
		if _, err := resolved.Decode(encoded, &out); err != nil {
			t.Fatal(err)
		}
		if out != in {
			t.Fatalf("got %x, want %x", out, in)
		}
	})
}

func TestSpecFieldAliasResolution(t *testing.T) {
	writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"old_name","type":"int"}]}`)
	reader := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"new_name","type":"int","aliases":["old_name"]}]}`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{"old_name": int32(7)})
	if err != nil {
		t.Fatal(err)
	}
	var got any
	if _, err := resolved.Decode(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if got.(map[string]any)["new_name"] != int32(7) {
		t.Fatalf("got %+v, want new_name=7", got)
	}
}

func TestSpecReaderUnionSelectsFirstMatchingBranch(t *testing.T) {
	writer := mustParse(t, `"int"`)
	reader := mustParse(t, `["long","double"]`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	in := int32(42)
	encoded, err := writer.Encode(&in)
	if err != nil {
		t.Fatal(err)
	}
	var got any
	if _, err := resolved.Decode(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if got != int64(42) {
		t.Fatalf("got %v (%T), want int64(42)", got, got)
	}
}
