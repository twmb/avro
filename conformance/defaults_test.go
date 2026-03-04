package conformance

import (
	"encoding/json"
	"testing"

	"github.com/twmb/avro"
)

// -----------------------------------------------------------------------
// Defaults & Canonical Form
// Spec: "Schema Resolution" (field defaults) and "Parsing Canonical
// Form for Schemas" (canonical JSON representation).
//   - Defaults: applied at read time only, not write time
//   - Union defaults: must match the first branch type
//   - Complex defaults: arrays, maps, nested records
//   - Bytes defaults: decoded from JSON \uXXXX escapes
//   - Canonical form: strips doc/aliases/defaults, expands fullnames,
//     deterministic key ordering, collapses primitive object form
// https://avro.apache.org/docs/1.12.0/specification/#schema-resolution
// https://avro.apache.org/docs/1.12.0/specification/#parsing-canonical-form-for-schemas
// -----------------------------------------------------------------------

func TestSpecNullUnionDefault(t *testing.T) {
	writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	reader := mustParse(t, `{
		"type":"record","name":"R",
		"fields":[
			{"name":"a","type":"int"},
			{"name":"b","type":["null","string"],"default":null}
		]
	}`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{"a": 42})
	if err != nil {
		t.Fatal(err)
	}
	var result any
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	m := result.(map[string]any)
	if m["a"] != int32(42) {
		t.Fatalf("a: got %v, want 42", m["a"])
	}
	if m["b"] != nil {
		t.Fatalf("b: got %v, want nil", m["b"])
	}
}

func TestSpecComplexDefaults(t *testing.T) {
	t.Run("array default", func(t *testing.T) {
		writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
		reader := mustParse(t, `{
			"type":"record","name":"R",
			"fields":[
				{"name":"a","type":"int"},
				{"name":"arr","type":{"type":"array","items":"int"},"default":[1,2,3]}
			]
		}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}

		encoded, err := writer.Encode(map[string]any{"a": 1})
		if err != nil {
			t.Fatal(err)
		}
		var result any
		_, err = resolved.Decode(encoded, &result)
		if err != nil {
			t.Fatal(err)
		}
		m := result.(map[string]any)
		arr := m["arr"].([]any)
		if len(arr) != 3 {
			t.Fatalf("arr length: got %d, want 3", len(arr))
		}
		if arr[0] != int32(1) || arr[1] != int32(2) || arr[2] != int32(3) {
			t.Fatalf("arr: got %v, want [1 2 3]", arr)
		}
	})

	t.Run("map default", func(t *testing.T) {
		writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
		reader := mustParse(t, `{
			"type":"record","name":"R",
			"fields":[
				{"name":"a","type":"int"},
				{"name":"m","type":{"type":"map","values":"string"},"default":{}}
			]
		}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}

		encoded, err := writer.Encode(map[string]any{"a": 1})
		if err != nil {
			t.Fatal(err)
		}
		var result any
		_, err = resolved.Decode(encoded, &result)
		if err != nil {
			t.Fatal(err)
		}
		m := result.(map[string]any)
		dm := m["m"].(map[string]any)
		if len(dm) != 0 {
			t.Fatalf("map: got %v, want empty", dm)
		}
	})

	t.Run("nested record default", func(t *testing.T) {
		writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
		reader := mustParse(t, `{
			"type":"record","name":"R",
			"fields":[
				{"name":"a","type":"int"},
				{"name":"inner","type":{
					"type":"record","name":"Inner",
					"fields":[
						{"name":"x","type":"int","default":0},
						{"name":"y","type":"string","default":"default"}
					]
				},"default":{"x":99}}
			]
		}`)
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatal(err)
		}

		encoded, err := writer.Encode(map[string]any{"a": 1})
		if err != nil {
			t.Fatal(err)
		}
		var result any
		_, err = resolved.Decode(encoded, &result)
		if err != nil {
			t.Fatal(err)
		}
		m := result.(map[string]any)
		inner := m["inner"].(map[string]any)
		if inner["x"] != int32(99) {
			t.Fatalf("inner.x: got %v, want 99", inner["x"])
		}
		if inner["y"] != "default" {
			t.Fatalf("inner.y: got %v, want default", inner["y"])
		}
	})
}

func TestSpecDefaultBytesUnicode(t *testing.T) {
	writer := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	reader := mustParse(t, `{
		"type":"record","name":"R",
		"fields":[
			{"name":"a","type":"int"},
			{"name":"data","type":"bytes","default":"\u00FF\u0001\u0000"}
		]
	}`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{"a": 1})
	if err != nil {
		t.Fatal(err)
	}
	var result any
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	m := result.(map[string]any)
	b := m["data"].([]byte)
	if len(b) != 3 || b[0] != 0xFF || b[1] != 0x01 || b[2] != 0x00 {
		t.Fatalf("bytes default: got %x, want ff0100", b)
	}
}

func TestSpecDefaultsAreReadTimeOnly(t *testing.T) {
	s := mustParse(t, `{
		"type":"record","name":"R",
		"fields":[
			{"name":"a","type":"int","default":42},
			{"name":"b","type":"string"}
		]
	}`)

	if _, err := s.AppendEncode(nil, map[string]any{"b": "hello"}); err == nil {
		t.Fatal("expected encode error for missing writer field, even with default")
	}
}

func TestSpecUnionDefaultMustMatchFirstBranch(t *testing.T) {
	_, err := avro.Parse(`{
		"type":"record","name":"R",
		"fields":[
			{"name":"u","type":["string","null"],"default":null}
		]
	}`)
	if err == nil {
		t.Fatal("expected error when union default does not match the first branch")
	}
}

func TestSpecNumericDefaultValidation(t *testing.T) {
	t.Run("int default must be integer", func(t *testing.T) {
		_, err := avro.Parse(`{
			"type":"record","name":"R",
			"fields":[{"name":"a","type":"int","default":1.5}]
		}`)
		if err == nil {
			t.Fatal("expected parse error for fractional int default")
		}
	})

	t.Run("int default must be in range", func(t *testing.T) {
		_, err := avro.Parse(`{
			"type":"record","name":"R",
			"fields":[{"name":"a","type":"int","default":2147483648}]
		}`)
		if err == nil {
			t.Fatal("expected parse error for out-of-range int default")
		}
	})

	t.Run("long default must be integer", func(t *testing.T) {
		_, err := avro.Parse(`{
			"type":"record","name":"R",
			"fields":[{"name":"a","type":"long","default":2.25}]
		}`)
		if err == nil {
			t.Fatal("expected parse error for fractional long default")
		}
	})
}

func TestSpecCanonicalFormSpec(t *testing.T) {
	t.Run("strip doc", func(t *testing.T) {
		s := mustParse(t, `{
			"type":"record",
			"name":"R",
			"doc":"This is a test record",
			"fields":[{"name":"a","type":"int","doc":"an integer"}]
		}`)
		canonical := string(s.Canonical())
		var m map[string]any
		if err := json.Unmarshal([]byte(canonical), &m); err != nil {
			t.Fatal(err)
		}
		if _, exists := m["doc"]; exists {
			t.Fatal("canonical form should not include doc")
		}
	})

	t.Run("strip aliases", func(t *testing.T) {
		s := mustParse(t, `{
			"type":"record",
			"name":"R",
			"aliases":["OldR"],
			"fields":[{"name":"a","type":"int","aliases":["old_a"]}]
		}`)
		canonical := string(s.Canonical())
		var m map[string]any
		if err := json.Unmarshal([]byte(canonical), &m); err != nil {
			t.Fatal(err)
		}
		if _, exists := m["aliases"]; exists {
			t.Fatal("canonical form should not include aliases")
		}
	})

	t.Run("strip defaults", func(t *testing.T) {
		s := mustParse(t, `{
			"type":"record",
			"name":"R",
			"fields":[{"name":"a","type":"int","default":42}]
		}`)
		canonical := string(s.Canonical())
		var m map[string]any
		if err := json.Unmarshal([]byte(canonical), &m); err != nil {
			t.Fatal(err)
		}
		fields := m["fields"].([]any)
		field := fields[0].(map[string]any)
		if _, exists := field["default"]; exists {
			t.Fatal("canonical form should not include default")
		}
	})

	t.Run("preserve enum symbols", func(t *testing.T) {
		s := mustParse(t, `{"type":"enum","name":"E","symbols":["A","B","C"],"doc":"test"}`)
		canonical := string(s.Canonical())
		var m map[string]any
		if err := json.Unmarshal([]byte(canonical), &m); err != nil {
			t.Fatal(err)
		}
		syms := m["symbols"].([]any)
		if len(syms) != 3 {
			t.Fatalf("expected 3 symbols, got %d", len(syms))
		}
		if _, exists := m["doc"]; exists {
			t.Fatal("canonical form should not include doc")
		}
	})

	t.Run("primitive canonical", func(t *testing.T) {
		s := mustParse(t, `"string"`)
		canonical := string(s.Canonical())
		if canonical != `"string"` {
			t.Fatalf("got %s, want \"string\"", canonical)
		}
	})
}

func TestSpecCanonicalExactVectors(t *testing.T) {
	t.Run("primitive object form collapses", func(t *testing.T) {
		s := mustParse(t, `{"type":"string"}`)
		if got := string(s.Canonical()); got != `"string"` {
			t.Fatalf("got %s, want \"string\"", got)
		}
	})

	t.Run("record fullname expansion", func(t *testing.T) {
		s := mustParse(t, `{
			"type":"record",
			"name":"Outer",
			"namespace":"com.example",
			"doc":"ignored",
			"fields":[
				{"name":"inner","type":{
					"type":"record",
					"name":"Inner",
					"fields":[{"name":"x","type":"int","default":1}]
				}}
			]
		}`)
		want := `{"name":"com.example.Outer","type":"record","fields":[{"name":"inner","type":{"name":"com.example.Inner","type":"record","fields":[{"name":"x","type":"int"}]}}]}`
		if got := string(s.Canonical()); got != want {
			t.Fatalf("got %s, want %s", got, want)
		}
	})

	t.Run("canonical is deterministic across key order", func(t *testing.T) {
		s1 := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
		s2 := mustParse(t, `{"fields":[{"type":"int","name":"a"}],"name":"R","type":"record"}`)
		if got1, got2 := string(s1.Canonical()), string(s2.Canonical()); got1 != got2 {
			t.Fatalf("canonical mismatch: %s vs %s", got1, got2)
		}
	})
}
