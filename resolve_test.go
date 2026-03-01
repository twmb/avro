package avro

import (
	"math"
	"reflect"
	"testing"
)

func TestResolveIdenticalSchemas(t *testing.T) {
	s, err := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(s, s)
	if err != nil {
		t.Fatal(err)
	}
	if resolved != s {
		t.Fatal("expected identical schemas to return reader directly")
	}
}

func TestResolveFieldAddedWithDefault(t *testing.T) {
	writer, err := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string","default":"hello"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	// Encode with writer schema.
	encoded, err := writer.Encode(map[string]any{"a": 42})
	if err != nil {
		t.Fatal(err)
	}

	// Decode with resolved schema into interface.
	var result any
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", result)
	}
	if m["a"] != int32(42) {
		t.Fatalf("expected a=42, got %v (%T)", m["a"], m["a"])
	}
	if m["b"] != "hello" {
		t.Fatalf("expected b=hello, got %v", m["b"])
	}
}

func TestResolveFieldRemoved(t *testing.T) {
	writer, err := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{"a": 42, "b": "drop me"})
	if err != nil {
		t.Fatal(err)
	}

	var result any
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", result)
	}
	if m["a"] != int32(42) {
		t.Fatalf("expected a=42, got %v", m["a"])
	}
	if _, exists := m["b"]; exists {
		t.Fatal("expected field b to be absent")
	}
}

func TestResolveFieldRenamedViaAlias(t *testing.T) {
	writer, err := Parse(`{"type":"record","name":"R","fields":[{"name":"old_name","type":"int"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"record","name":"R","fields":[{"name":"new_name","type":"int","aliases":["old_name"]}]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{"old_name": 99})
	if err != nil {
		t.Fatal(err)
	}

	var result any
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	m := result.(map[string]any)
	if m["new_name"] != int32(99) {
		t.Fatalf("expected new_name=99, got %v", m["new_name"])
	}
}

func TestResolveTypePromotion(t *testing.T) {
	tests := []struct {
		name       string
		writerType string
		readerType string
		writerVal  any
		expectVal  any
	}{
		{"int to long", `"int"`, `"long"`, int32(42), int64(42)},
		{"int to float", `"int"`, `"float"`, int32(7), float32(7)},
		{"int to double", `"int"`, `"double"`, int32(42), float64(42)},
		{"long to float", `"long"`, `"float"`, int64(9), float32(9)},
		{"long to double", `"long"`, `"double"`, int64(100), float64(100)},
		{"float to double", `"float"`, `"double"`, float32(1.5), float64(float32(1.5))},
		{"string to bytes", `"string"`, `"bytes"`, "abc", []byte("abc")},
		{"bytes to string", `"bytes"`, `"string"`, []byte("xyz"), "xyz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer, err := Parse(tt.writerType)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := Parse(tt.readerType)
			if err != nil {
				t.Fatal(err)
			}
			resolved, err := Resolve(writer, reader)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := writer.Encode(tt.writerVal)
			if err != nil {
				t.Fatal(err)
			}
			var result any
			_, err = resolved.Decode(encoded, &result)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(result, tt.expectVal) {
				t.Fatalf("expected %v (%T), got %v (%T)", tt.expectVal, tt.expectVal, result, result)
			}
		})
	}
}

func TestResolveEnumEvolution(t *testing.T) {
	writer, err := Parse(`{"type":"enum","name":"E","symbols":["A","B","C"]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	// Encode "C" with writer.
	encoded, err := writer.Encode("C")
	if err != nil {
		t.Fatal(err)
	}

	var result string
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	if result != "A" {
		t.Fatalf("expected default A, got %s", result)
	}

	// Known symbol should pass through.
	encoded, err = writer.Encode("B")
	if err != nil {
		t.Fatal(err)
	}
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	if result != "B" {
		t.Fatalf("expected B, got %s", result)
	}
}

func TestResolveNestedRecords(t *testing.T) {
	writer, err := Parse(`{
		"type":"record","name":"Outer","fields":[
			{"name":"inner","type":{
				"type":"record","name":"Inner","fields":[
					{"name":"x","type":"int"}
				]
			}}
		]
	}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{
		"type":"record","name":"Outer","fields":[
			{"name":"inner","type":{
				"type":"record","name":"Inner","fields":[
					{"name":"x","type":"long"},
					{"name":"y","type":"string","default":"default_y"}
				]
			}}
		]
	}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{
		"inner": map[string]any{"x": 10},
	})
	if err != nil {
		t.Fatal(err)
	}

	var result any
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	outer := result.(map[string]any)
	inner := outer["inner"].(map[string]any)
	if inner["x"] != int64(10) {
		t.Fatalf("expected x=10 (int64), got %v (%T)", inner["x"], inner["x"])
	}
	if inner["y"] != "default_y" {
		t.Fatalf("expected y=default_y, got %v", inner["y"])
	}
}

func TestResolveUnionEvolution(t *testing.T) {
	writer, err := Parse(`["null","int"]`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`["null","long"]`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	// Encode int value 42 (index 1 in writer union) using a pointer.
	val := int32(42)
	encoded, err := writer.Encode(&val)
	if err != nil {
		t.Fatal(err)
	}

	var result *int64
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || *result != 42 {
		t.Fatalf("expected *int64(42), got %v", result)
	}

	// Encode null.
	encoded, err = writer.Encode((*int32)(nil))
	if err != nil {
		t.Fatal(err)
	}
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Fatalf("expected nil, got %v", *result)
	}
}

func TestResolveSelfReferencingRecord(t *testing.T) {
	schema := `{
		"type":"record","name":"Node","fields":[
			{"name":"value","type":"int"},
			{"name":"next","type":["null","Node"]}
		]
	}`
	writer, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{
		"value": 1,
		"next": map[string]any{
			"value": 2,
			"next":  nil,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var result any
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	root := result.(map[string]any)
	if root["value"] != int32(1) {
		t.Fatalf("expected root value=1, got %v", root["value"])
	}
	next := root["next"].(map[string]any)
	if next["value"] != int32(2) {
		t.Fatalf("expected next value=2, got %v", next["value"])
	}
}

func TestResolveDecodeIntoStruct(t *testing.T) {
	writer, err := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"},{"name":"c","type":"double","default":3.14}]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{"a": 42, "b": "hello"})
	if err != nil {
		t.Fatal(err)
	}

	type Result struct {
		A int64   `avro:"a"`
		B string  `avro:"b"`
		C float64 `avro:"c"`
	}
	var result Result
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	if result.A != 42 {
		t.Fatalf("expected A=42, got %d", result.A)
	}
	if result.B != "hello" {
		t.Fatalf("expected B=hello, got %s", result.B)
	}
	if result.C != 3.14 {
		t.Fatalf("expected C=3.14, got %f", result.C)
	}
}

func TestResolveDecodeIntoMap(t *testing.T) {
	writer, err := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"int","default":99}]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{"a": 10})
	if err != nil {
		t.Fatal(err)
	}

	result := make(map[string]int32)
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	if result["a"] != 10 {
		t.Fatalf("expected a=10, got %d", result["a"])
	}
	if result["b"] != 99 {
		t.Fatalf("expected b=99, got %d", result["b"])
	}
}

func TestResolveArrayEvolution(t *testing.T) {
	writer, err := Parse(`{"type":"array","items":"int"}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"array","items":"long"}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode([]int32{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}

	var result []int64
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 || result[0] != 1 || result[1] != 2 || result[2] != 3 {
		t.Fatalf("unexpected result: %v", result)
	}
}

func TestResolveMapEvolution(t *testing.T) {
	writer, err := Parse(`{"type":"map","values":"float"}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"map","values":"double"}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]float32{"x": 1.5})
	if err != nil {
		t.Fatal(err)
	}

	var result map[string]float64
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	expected := float64(float32(1.5))
	if math.Abs(result["x"]-expected) > 1e-10 {
		t.Fatalf("expected x=%v, got %v", expected, result["x"])
	}
}

func TestEncodeDefault(t *testing.T) {
	tests := []struct {
		name     string
		val      any
		schema   string
		checkFn  func(t *testing.T, encoded []byte)
	}{
		{
			name:   "null",
			val:    nil,
			schema: `"null"`,
			checkFn: func(t *testing.T, encoded []byte) {
				if len(encoded) != 0 {
					t.Fatalf("expected empty, got %v", encoded)
				}
			},
		},
		{
			name:   "boolean true",
			val:    true,
			schema: `"boolean"`,
			checkFn: func(t *testing.T, encoded []byte) {
				if len(encoded) != 1 || encoded[0] != 1 {
					t.Fatalf("expected [1], got %v", encoded)
				}
			},
		},
		{
			name:   "int",
			val:    float64(42),
			schema: `"int"`,
			checkFn: func(t *testing.T, encoded []byte) {
				s, err := Parse(`"int"`)
				if err != nil {
					t.Fatal(err)
				}
				var v int32
				_, err = s.Decode(encoded, &v)
				if err != nil {
					t.Fatal(err)
				}
				if v != 42 {
					t.Fatalf("expected 42, got %d", v)
				}
			},
		},
		{
			name:   "string",
			val:    "hello",
			schema: `"string"`,
			checkFn: func(t *testing.T, encoded []byte) {
				s, err := Parse(`"string"`)
				if err != nil {
					t.Fatal(err)
				}
				var v string
				_, err = s.Decode(encoded, &v)
				if err != nil {
					t.Fatal(err)
				}
				if v != "hello" {
					t.Fatalf("expected hello, got %s", v)
				}
			},
		},
		{
			name: "record",
			val:  map[string]any{"x": float64(10), "y": "test"},
			schema: `{"type":"record","name":"R","fields":[
				{"name":"x","type":"int"},
				{"name":"y","type":"string"}
			]}`,
			checkFn: func(t *testing.T, encoded []byte) {
				s, err := Parse(`{"type":"record","name":"R","fields":[{"name":"x","type":"int"},{"name":"y","type":"string"}]}`)
				if err != nil {
					t.Fatal(err)
				}
				var v any
				_, err = s.Decode(encoded, &v)
				if err != nil {
					t.Fatal(err)
				}
				m := v.(map[string]any)
				if m["x"] != int32(10) {
					t.Fatalf("expected x=10, got %v", m["x"])
				}
				if m["y"] != "test" {
					t.Fatalf("expected y=test, got %v", m["y"])
				}
			},
		},
		{
			name:   "enum",
			val:    "B",
			schema: `{"type":"enum","name":"E","symbols":["A","B","C"]}`,
			checkFn: func(t *testing.T, encoded []byte) {
				s, err := Parse(`{"type":"enum","name":"E","symbols":["A","B","C"]}`)
				if err != nil {
					t.Fatal(err)
				}
				var v string
				_, err = s.Decode(encoded, &v)
				if err != nil {
					t.Fatal(err)
				}
				if v != "B" {
					t.Fatalf("expected B, got %s", v)
				}
			},
		},
		{
			name:   "array",
			val:    []any{float64(1), float64(2)},
			schema: `{"type":"array","items":"int"}`,
			checkFn: func(t *testing.T, encoded []byte) {
				s, err := Parse(`{"type":"array","items":"int"}`)
				if err != nil {
					t.Fatal(err)
				}
				var v []int32
				_, err = s.Decode(encoded, &v)
				if err != nil {
					t.Fatal(err)
				}
				if len(v) != 2 || v[0] != 1 || v[1] != 2 {
					t.Fatalf("expected [1,2], got %v", v)
				}
			},
		},
		{
			name:   "union (null first branch)",
			val:    nil,
			schema: `["null","int"]`,
			checkFn: func(t *testing.T, encoded []byte) {
				s, err := Parse(`["null","int"]`)
				if err != nil {
					t.Fatal(err)
				}
				var v any
				_, err = s.Decode(encoded, &v)
				if err != nil {
					t.Fatal(err)
				}
				if v != nil {
					t.Fatalf("expected nil, got %v", v)
				}
			},
		},
		{
			name:   "long",
			val:    float64(100000),
			schema: `"long"`,
			checkFn: func(t *testing.T, encoded []byte) {
				s, err := Parse(`"long"`)
				if err != nil {
					t.Fatal(err)
				}
				var v int64
				_, err = s.Decode(encoded, &v)
				if err != nil {
					t.Fatal(err)
				}
				if v != 100000 {
					t.Fatalf("expected 100000, got %d", v)
				}
			},
		},
		{
			name:   "float",
			val:    float64(1.5),
			schema: `"float"`,
			checkFn: func(t *testing.T, encoded []byte) {
				s, err := Parse(`"float"`)
				if err != nil {
					t.Fatal(err)
				}
				var v float32
				_, err = s.Decode(encoded, &v)
				if err != nil {
					t.Fatal(err)
				}
				if v != 1.5 {
					t.Fatalf("expected 1.5, got %f", v)
				}
			},
		},
		{
			name:   "double",
			val:    float64(2.718),
			schema: `"double"`,
			checkFn: func(t *testing.T, encoded []byte) {
				s, err := Parse(`"double"`)
				if err != nil {
					t.Fatal(err)
				}
				var v float64
				_, err = s.Decode(encoded, &v)
				if err != nil {
					t.Fatal(err)
				}
				if v != 2.718 {
					t.Fatalf("expected 2.718, got %f", v)
				}
			},
		},
		{
			name:   "bytes",
			val:    "raw",
			schema: `"bytes"`,
			checkFn: func(t *testing.T, encoded []byte) {
				s, err := Parse(`"bytes"`)
				if err != nil {
					t.Fatal(err)
				}
				var v []byte
				_, err = s.Decode(encoded, &v)
				if err != nil {
					t.Fatal(err)
				}
				if string(v) != "raw" {
					t.Fatalf("expected raw, got %s", v)
				}
			},
		},
		{
			name:   "fixed",
			val:    "abcd",
			schema: `{"type":"fixed","name":"F","size":4}`,
			checkFn: func(t *testing.T, encoded []byte) {
				s, err := Parse(`{"type":"fixed","name":"F","size":4}`)
				if err != nil {
					t.Fatal(err)
				}
				var v [4]byte
				_, err = s.Decode(encoded, &v)
				if err != nil {
					t.Fatal(err)
				}
				if string(v[:]) != "abcd" {
					t.Fatalf("expected abcd, got %s", v[:])
				}
			},
		},
		{
			name:   "map with entries",
			val:    map[string]any{"x": float64(1), "y": float64(2)},
			schema: `{"type":"map","values":"int"}`,
			checkFn: func(t *testing.T, encoded []byte) {
				s, err := Parse(`{"type":"map","values":"int"}`)
				if err != nil {
					t.Fatal(err)
				}
				var v map[string]int32
				_, err = s.Decode(encoded, &v)
				if err != nil {
					t.Fatal(err)
				}
				if v["x"] != 1 || v["y"] != 2 {
					t.Fatalf("expected {x:1,y:2}, got %v", v)
				}
			},
		},
		{
			name:   "boolean false",
			val:    false,
			schema: `"boolean"`,
			checkFn: func(t *testing.T, encoded []byte) {
				if len(encoded) != 1 || encoded[0] != 0 {
					t.Fatalf("expected [0], got %v", encoded)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := encodeDefault(tt.val, s.node)
			if err != nil {
				t.Fatal(err)
			}
			tt.checkFn(t, encoded)
		})
	}
}

func TestSkipUnion(t *testing.T) {
	s, err := Parse(`["null","int","string"]`)
	if err != nil {
		t.Fatal(err)
	}

	// Encode an int value (index 1) via a generic union.
	// The union ser tries each branch; int should succeed.
	encoded, err := s.Encode(int32(42))
	if err != nil {
		t.Fatal(err)
	}
	sentinel := byte(0xFE)
	data := append(encoded, sentinel)

	skip := buildSkip(s.node)
	rem, err := skip(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(rem) != 1 || rem[0] != sentinel {
		t.Fatalf("expected sentinel, got %v", rem)
	}
}

func TestSkipFunctions(t *testing.T) {
	// Encode various types and verify skip advances past them correctly.
	tests := []struct {
		name   string
		schema string
		val    any
	}{
		{"boolean", `"boolean"`, true},
		{"int", `"int"`, 42},
		{"long", `"long"`, int64(12345)},
		{"float", `"float"`, float32(1.5)},
		{"double", `"double"`, float64(2.5)},
		{"string", `"string"`, "hello"},
		{"bytes", `"bytes"`, []byte("world")},
		{"array", `{"type":"array","items":"int"}`, []int32{1, 2, 3}},
		{"map", `{"type":"map","values":"string"}`, map[string]string{"k": "v"}},
		{"enum", `{"type":"enum","name":"E","symbols":["A","B"]}`, "B"},
		{"fixed", `{"type":"fixed","name":"F","size":4}`, [4]byte{1, 2, 3, 4}},
		{"record", `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`,
			map[string]any{"a": 1, "b": "x"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := s.Encode(tt.val)
			if err != nil {
				t.Fatal(err)
			}
			sentinel := byte(0xFE)
			data := append(encoded, sentinel)

			skip := buildSkip(s.node)
			rem, err := skip(data)
			if err != nil {
				t.Fatal(err)
			}
			if len(rem) != 1 || rem[0] != sentinel {
				t.Fatalf("expected sentinel byte remaining, got %v", rem)
			}
		})
	}

	// Null is zero bytes, test directly.
	t.Run("null", func(t *testing.T) {
		sentinel := byte(0xFE)
		data := []byte{sentinel}
		rem, err := skipNull(data)
		if err != nil {
			t.Fatal(err)
		}
		if len(rem) != 1 || rem[0] != sentinel {
			t.Fatalf("expected sentinel byte remaining, got %v", rem)
		}
	})
}

func TestResolveRecordFieldReorder(t *testing.T) {
	// Writer has fields in different order than reader.
	writer, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"b","type":"string"},
		{"name":"a","type":"int"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{"b": "hello", "a": 42})
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
		t.Fatalf("expected a=42, got %v", m["a"])
	}
	if m["b"] != "hello" {
		t.Fatalf("expected b=hello, got %v", m["b"])
	}
}

func TestResolveComplexDefault(t *testing.T) {
	writer, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"x","type":"int"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"x","type":"int"},
		{"name":"tags","type":{"type":"array","items":"string"},"default":[]},
		{"name":"meta","type":{"type":"map","values":"int"},"default":{}}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{"x": 1})
	if err != nil {
		t.Fatal(err)
	}

	var result any
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	m := result.(map[string]any)
	if m["x"] != int32(1) {
		t.Fatalf("expected x=1, got %v", m["x"])
	}
	tags := m["tags"].([]any)
	if len(tags) != 0 {
		t.Fatalf("expected empty tags, got %v", tags)
	}
	meta := m["meta"].(map[string]any)
	if len(meta) != 0 {
		t.Fatalf("expected empty meta, got %v", meta)
	}
}

func TestResolvePromotionInRecord(t *testing.T) {
	writer, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"float"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"long"},
		{"name":"b","type":"double"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{"a": 42, "b": float32(1.5)})
	if err != nil {
		t.Fatal(err)
	}

	type R struct {
		A int64   `avro:"a"`
		B float64 `avro:"b"`
	}
	var result R
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	if result.A != 42 {
		t.Fatalf("expected A=42, got %d", result.A)
	}
	expected := float64(float32(1.5))
	if math.Abs(result.B-expected) > 1e-10 {
		t.Fatalf("expected B=%v, got %v", expected, result.B)
	}
}

func TestResolveRecordDefault(t *testing.T) {
	// Test default for a record field whose type is also a record.
	writer, err := Parse(`{"type":"record","name":"Outer","fields":[
		{"name":"x","type":"int"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"record","name":"Outer","fields":[
		{"name":"x","type":"int"},
		{"name":"inner","type":{"type":"record","name":"Inner","fields":[
			{"name":"a","type":"int","default":0},
			{"name":"b","type":"string","default":""}
		]},"default":{"a":10,"b":"def"}}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{"x": 1})
	if err != nil {
		t.Fatal(err)
	}

	var result any
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	outer := result.(map[string]any)
	inner := outer["inner"].(map[string]any)
	if inner["a"] != int32(10) {
		t.Fatalf("expected inner.a=10, got %v", inner["a"])
	}
	if inner["b"] != "def" {
		t.Fatalf("expected inner.b=def, got %v", inner["b"])
	}
}

func TestResolveWriterUnionReaderNonUnion(t *testing.T) {
	// Writer is ["null","int"], reader is just "int".
	// All writer branches must be compatible with reader.
	writer, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":["null","int"]}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	// This should fail: null branch is incompatible with int reader.
	_, err = Resolve(writer, reader)
	if err == nil {
		t.Fatal("expected error for null branch incompatible with int reader")
	}
}

func TestResolveWriterUnionReaderNonUnionSuccess(t *testing.T) {
	// Writer is ["int","long"], reader is "double". Both promote to double.
	writer, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":["int","long"]}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"double"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	// Encode int32(7) through the writer union (will pick index 0 = int).
	encoded, err := writer.Encode(map[string]any{"a": int32(7)})
	if err != nil {
		t.Fatal(err)
	}

	var result any
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	m := result.(map[string]any)
	if m["a"] != float64(7) {
		t.Fatalf("expected a=7.0, got %v (%T)", m["a"], m["a"])
	}
}

func TestResolveReaderUnionWriterNonUnion(t *testing.T) {
	// Writer is "int", reader is ["null","long"].
	writer, err := Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`["null","long"]`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(int32(42))
	if err != nil {
		t.Fatal(err)
	}

	var result any
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	if result != int64(42) {
		t.Fatalf("expected int64(42), got %v (%T)", result, result)
	}
}

// --- Direct skip function error path tests ---

func TestSkipBooleanShortBuffer(t *testing.T) {
	_, err := skipBoolean(nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSkipFloatShortBuffer(t *testing.T) {
	_, err := skipFloat([]byte{1, 2})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSkipDoubleShortBuffer(t *testing.T) {
	_, err := skipDouble([]byte{1, 2, 3, 4})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSkipBytesErrors(t *testing.T) {
	// Empty buffer: readVarlong fails.
	_, err := skipBytes(nil)
	if err == nil {
		t.Fatal("expected error for empty input")
	}

	// Short buffer after reading length.
	data := appendVarlong(nil, 100) // length=100 but no data
	_, err = skipBytes(data)
	if err == nil {
		t.Fatal("expected error for short data")
	}
}

func TestSkipFixedShortBuffer(t *testing.T) {
	skip := skipFixed(8)
	_, err := skip([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSkipArrayErrors(t *testing.T) {
	intNode := &schemaNode{kind: "int"}
	arrNode := &schemaNode{kind: "array", items: intNode}
	skip := buildSkip(arrNode)

	// Empty buffer: readVarlong fails.
	_, err := skip(nil)
	if err == nil {
		t.Fatal("expected error for empty input")
	}

	// Negative block count with byte-size skip.
	// Encode: count=-3 (zigzag), byteSize=2, then 2 bytes of data, then 0 terminator.
	var data []byte
	data = appendVarlong(data, -3)       // negative count => abs(3) items, but skip by byte size
	data = appendVarlong(data, 2)        // byte size = 2
	data = append(data, 0x01, 0x02)      // 2 bytes
	data = appendVarlong(data, 0)        // terminator
	rem, err := skip(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("expected empty remainder, got %d bytes", len(rem))
	}

	// Negative count byte-size exceeds available data.
	data = data[:0]
	data = appendVarlong(data, -2)
	data = appendVarlong(data, 100) // byte size > available
	_, err = skip(data)
	if err == nil {
		t.Fatal("expected error for byte-size exceeding data")
	}

	// Negative count: error reading byte size.
	data = data[:0]
	data = appendVarlong(data, -2)
	// No byte size follows.
	_, err = skip(data)
	if err == nil {
		t.Fatal("expected error for missing byte size")
	}

	// Positive count with item skip error (truncated int).
	data = data[:0]
	data = appendVarlong(data, 1) // 1 item
	// No int data for the item.
	_, err = skip(data)
	if err == nil {
		t.Fatal("expected error for truncated item")
	}
}

func TestSkipMapErrors(t *testing.T) {
	intNode := &schemaNode{kind: "int"}
	mapNode := &schemaNode{kind: "map", values: intNode}
	skip := buildSkip(mapNode)

	// Empty buffer.
	_, err := skip(nil)
	if err == nil {
		t.Fatal("expected error for empty input")
	}

	// Negative count with byte-size skip.
	var data []byte
	data = appendVarlong(data, -2)
	data = appendVarlong(data, 3)
	data = append(data, 0x01, 0x02, 0x03)
	data = appendVarlong(data, 0)
	rem, err := skip(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("expected empty remainder, got %d bytes", len(rem))
	}

	// Negative count byte-size exceeds data.
	data = data[:0]
	data = appendVarlong(data, -1)
	data = appendVarlong(data, 100)
	_, err = skip(data)
	if err == nil {
		t.Fatal("expected error for oversized byte-size")
	}

	// Negative count: error reading byte size.
	data = data[:0]
	data = appendVarlong(data, -1)
	_, err = skip(data)
	if err == nil {
		t.Fatal("expected error for missing byte size")
	}

	// Positive count: key skip error.
	data = data[:0]
	data = appendVarlong(data, 1)
	// No key string data.
	_, err = skip(data)
	if err == nil {
		t.Fatal("expected error for truncated key")
	}

	// Positive count: value skip error (key ok, value truncated).
	data = data[:0]
	data = appendVarlong(data, 1)
	data = appendVarlong(data, 1) // key length=1
	data = append(data, 'k')     // key data
	// No value int data.
	_, err = skip(data)
	if err == nil {
		t.Fatal("expected error for truncated value")
	}
}

func TestSkipUnionErrors(t *testing.T) {
	node := &schemaNode{
		kind: "union",
		branches: []*schemaNode{
			{kind: "null"},
			{kind: "int"},
		},
	}
	skip := buildSkip(node)

	// Empty buffer: readVarint error.
	_, err := skip(nil)
	if err == nil {
		t.Fatal("expected error for empty input")
	}

	// Out of range index.
	data := appendVarint(nil, 5)
	_, err = skip(data)
	if err == nil {
		t.Fatal("expected error for out-of-range index")
	}

	// Negative index.
	data = appendVarint(nil, -1)
	_, err = skip(data)
	if err == nil {
		t.Fatal("expected error for negative index")
	}
}

func TestBuildSkipUnknownType(t *testing.T) {
	node := &schemaNode{kind: "unknown_type"}
	skip := buildSkip(node)
	_, err := skip([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error for unknown type")
	}
}

func TestSkipToDeser(t *testing.T) {
	deser := skipToDeser(skipBoolean)
	rem, err := deser([]byte{1, 2, 3}, reflect.Value{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rem) != 2 {
		t.Fatalf("expected 2 remaining bytes, got %d", len(rem))
	}
}

func TestSkipRecordFieldError(t *testing.T) {
	// Record with an int field, but pass truncated data.
	node := &schemaNode{
		kind: "record",
		fields: []fieldNode{
			{name: "a", node: &schemaNode{kind: "int"}},
		},
	}
	skip := buildSkip(node)
	_, err := skip(nil)
	if err == nil {
		t.Fatal("expected error for truncated record field")
	}
}

// --- Direct promotion function typed-target and error path tests ---

func TestPromoteIntToLongTyped(t *testing.T) {
	data := appendVarint(nil, 42)

	// CanInt path.
	var i64 int64
	v := reflect.ValueOf(&i64).Elem()
	_, err := promoteIntToLong(data, v)
	if err != nil {
		t.Fatal(err)
	}
	if i64 != 42 {
		t.Fatalf("expected 42, got %d", i64)
	}

	// CanUint path.
	var u64 uint64
	v = reflect.ValueOf(&u64).Elem()
	_, err = promoteIntToLong(data, v)
	if err != nil {
		t.Fatal(err)
	}
	if u64 != 42 {
		t.Fatalf("expected 42, got %d", u64)
	}

	// SemanticError: wrong type.
	var s string
	v = reflect.ValueOf(&s).Elem()
	_, err = promoteIntToLong(data, v)
	if err == nil {
		t.Fatal("expected error for string target")
	}

	// readVarint error.
	_, err = promoteIntToLong(nil, v)
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestPromoteIntToFloatTyped(t *testing.T) {
	data := appendVarint(nil, 7)

	// SetFloat path.
	var f32 float32
	v := reflect.ValueOf(&f32).Elem()
	_, err := promoteIntToFloat(data, v)
	if err != nil {
		t.Fatal(err)
	}
	if f32 != 7 {
		t.Fatalf("expected 7, got %f", f32)
	}

	// SemanticError.
	var s string
	v = reflect.ValueOf(&s).Elem()
	_, err = promoteIntToFloat(data, v)
	if err == nil {
		t.Fatal("expected error for string target")
	}

	// readVarint error.
	_, err = promoteIntToFloat(nil, v)
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestPromoteIntToDoubleTyped(t *testing.T) {
	data := appendVarint(nil, 42)

	var f64 float64
	v := reflect.ValueOf(&f64).Elem()
	_, err := promoteIntToDouble(data, v)
	if err != nil {
		t.Fatal(err)
	}
	if f64 != 42 {
		t.Fatalf("expected 42, got %f", f64)
	}

	var s string
	v = reflect.ValueOf(&s).Elem()
	_, err = promoteIntToDouble(data, v)
	if err == nil {
		t.Fatal("expected error for string target")
	}

	_, err = promoteIntToDouble(nil, v)
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestPromoteLongToFloatTyped(t *testing.T) {
	data := appendVarlong(nil, 9)

	var f32 float32
	v := reflect.ValueOf(&f32).Elem()
	_, err := promoteLongToFloat(data, v)
	if err != nil {
		t.Fatal(err)
	}
	if f32 != 9 {
		t.Fatalf("expected 9, got %f", f32)
	}

	var s string
	v = reflect.ValueOf(&s).Elem()
	_, err = promoteLongToFloat(data, v)
	if err == nil {
		t.Fatal("expected error for string target")
	}

	_, err = promoteLongToFloat(nil, v)
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestPromoteLongToDoubleTyped(t *testing.T) {
	data := appendVarlong(nil, 100)

	var f64 float64
	v := reflect.ValueOf(&f64).Elem()
	_, err := promoteLongToDouble(data, v)
	if err != nil {
		t.Fatal(err)
	}
	if f64 != 100 {
		t.Fatalf("expected 100, got %f", f64)
	}

	var s string
	v = reflect.ValueOf(&s).Elem()
	_, err = promoteLongToDouble(data, v)
	if err == nil {
		t.Fatal("expected error for string target")
	}

	_, err = promoteLongToDouble(nil, v)
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestPromoteFloatToDoubleErrors(t *testing.T) {
	// readUint32 error.
	var f64 float64
	v := reflect.ValueOf(&f64).Elem()
	_, err := promoteFloatToDouble([]byte{1}, v)
	if err == nil {
		t.Fatal("expected error for short buffer")
	}

	// SetFloat typed path.
	data := appendUint32(nil, math.Float32bits(2.5))
	_, err = promoteFloatToDouble(data, v)
	if err != nil {
		t.Fatal(err)
	}
	if f64 != float64(float32(2.5)) {
		t.Fatalf("expected %f, got %f", float64(float32(2.5)), f64)
	}

	// SemanticError.
	var s string
	v = reflect.ValueOf(&s).Elem()
	_, err = promoteFloatToDouble(data, v)
	if err == nil {
		t.Fatal("expected error for string target")
	}
}

func TestPromoteStringToBytesTyped(t *testing.T) {
	var data []byte
	data = appendVarlong(data, 3)
	data = append(data, "abc"...)

	// SetBytes slice path.
	var b []byte
	v := reflect.ValueOf(&b).Elem()
	_, err := promoteStringToBytes(data, v)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "abc" {
		t.Fatalf("expected abc, got %s", b)
	}

	// SemanticError (wrong type).
	var i int
	v = reflect.ValueOf(&i).Elem()
	_, err = promoteStringToBytes(data, v)
	if err == nil {
		t.Fatal("expected error for int target")
	}

	// readVarlong error.
	_, err = promoteStringToBytes(nil, v)
	if err == nil {
		t.Fatal("expected error for empty input")
	}

	// Short buffer after length.
	short := appendVarlong(nil, 100) // length=100 but no data
	_, err = promoteStringToBytes(short, v)
	if err == nil {
		t.Fatal("expected error for short buffer")
	}
}

func TestPromoteBytesToStringTyped(t *testing.T) {
	var data []byte
	data = appendVarlong(data, 3)
	data = append(data, "xyz"...)

	// SetString path.
	var s string
	v := reflect.ValueOf(&s).Elem()
	_, err := promoteBytesToString(data, v)
	if err != nil {
		t.Fatal(err)
	}
	if s != "xyz" {
		t.Fatalf("expected xyz, got %s", s)
	}

	// SemanticError (wrong type).
	var i int
	v = reflect.ValueOf(&i).Elem()
	_, err = promoteBytesToString(data, v)
	if err == nil {
		t.Fatal("expected error for int target")
	}

	// readVarlong error.
	_, err = promoteBytesToString(nil, v)
	if err == nil {
		t.Fatal("expected error for empty input")
	}

	// Short buffer after length.
	short := appendVarlong(nil, 100)
	_, err = promoteBytesToString(short, v)
	if err == nil {
		t.Fatal("expected error for short buffer")
	}
}

// --- Resolve edge case tests ---

func TestResolveFixedIdentity(t *testing.T) {
	writer, err := Parse(`{"type":"fixed","name":"F","size":4}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"fixed","name":"F","size":4}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := writer.Encode([4]byte{1, 2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}
	var result [4]byte
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	if result != [4]byte{1, 2, 3, 4} {
		t.Fatalf("expected [1,2,3,4], got %v", result)
	}
}

func TestResolveEnumIdentity(t *testing.T) {
	// Identical enums should return reader directly (identity path).
	writer, err := Parse(`{"type":"enum","name":"E","symbols":["A","B","C"]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"enum","name":"E","symbols":["A","B","C"]}`)
	if err != nil {
		t.Fatal(err)
	}
	// Make canonical forms different so Resolve doesn't short-circuit.
	// Actually they'll be the same... so we need to use the resolveEnum directly.
	// Let's test via resolveEnum.
	resolved, err := resolveEnum(reader.node, writer.node)
	if err != nil {
		t.Fatal(err)
	}
	// Identity: should return reader.node directly.
	if resolved != reader.node {
		t.Fatal("expected identity path to return reader node")
	}
}

func TestResolveEnumDeserTyped(t *testing.T) {
	// Non-identity enum to exercise the closure branches.
	writer, err := Parse(`{"type":"enum","name":"E","symbols":["A","B","C"]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"enum","name":"E","symbols":["B","A","C"],"default":"A"}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	// Encode "A" (index 0 in writer).
	encoded, err := writer.Encode("A")
	if err != nil {
		t.Fatal(err)
	}

	// Decode into string (SetString path).
	var s string
	_, err = resolved.Decode(encoded, &s)
	if err != nil {
		t.Fatal(err)
	}
	if s != "A" {
		t.Fatalf("expected A, got %s", s)
	}

	// Decode into int (CanInt path).
	var i int
	_, err = resolved.Decode(encoded, &i)
	if err != nil {
		t.Fatal(err)
	}
	// "A" maps to reader index 1 (B=0, A=1, C=2).
	if i != 1 {
		t.Fatalf("expected reader index 1, got %d", i)
	}

	// Decode into uint (CanUint path).
	var u uint
	_, err = resolved.Decode(encoded, &u)
	if err != nil {
		t.Fatal(err)
	}
	if u != 1 {
		t.Fatalf("expected reader index 1, got %d", u)
	}

	// Decode into incompatible type (SemanticError path).
	var f float64
	_, err = resolved.Decode(encoded, &f)
	if err == nil {
		t.Fatal("expected error for float64 target")
	}
}

func TestResolveEnumDeserErrors(t *testing.T) {
	writer, err := Parse(`{"type":"enum","name":"E","symbols":["A","B","C"]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"enum","name":"E","symbols":["B","A","C"],"default":"A"}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	// readVarint error: empty input.
	var s string
	_, err = resolved.Decode(nil, &s)
	if err == nil {
		t.Fatal("expected error for empty input")
	}

	// Out-of-range index.
	data := appendVarint(nil, 10)
	_, err = resolved.Decode(data, &s)
	if err == nil {
		t.Fatal("expected error for out-of-range enum index")
	}
}

func TestResolveArrayIdentity(t *testing.T) {
	// Array with same items type should return reader node (identity path).
	r := &schemaNode{kind: "int", deser: nil}
	rArr := &schemaNode{kind: "array", items: r}
	// resolveArray with same items node.
	resolved, err := resolveArray(rArr, rArr, "", make(map[nodePair]*schemaNode))
	if err != nil {
		t.Fatal(err)
	}
	if resolved != rArr {
		t.Fatal("expected identity path")
	}
}

func TestResolveMapIdentity(t *testing.T) {
	r := &schemaNode{kind: "string", deser: nil}
	rMap := &schemaNode{kind: "map", values: r}
	resolved, err := resolveMap(rMap, rMap, "", make(map[nodePair]*schemaNode))
	if err != nil {
		t.Fatal(err)
	}
	if resolved != rMap {
		t.Fatal("expected identity path")
	}
}

func TestResolveBuildDeserSemanticError(t *testing.T) {
	// buildDeser with unsupported target type (e.g. slice).
	writer, err := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{"a": 42})
	if err != nil {
		t.Fatal(err)
	}

	// Decode into a slice (unsupported target for record).
	var result []int
	_, err = resolved.Decode(encoded, &result)
	if err == nil {
		t.Fatal("expected error for slice target")
	}
}

func TestResolveSelfReferencingRecordDivergent(t *testing.T) {
	// Self-referencing record where reader and writer differ to exercise cycle detection placeholder.
	writerSchema := `{
		"type":"record","name":"Node","fields":[
			{"name":"value","type":"int"},
			{"name":"next","type":["null","Node"]}
		]
	}`
	readerSchema := `{
		"type":"record","name":"Node","fields":[
			{"name":"value","type":"long"},
			{"name":"next","type":["null","Node"]}
		]
	}`
	writer, err := Parse(writerSchema)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(readerSchema)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{
		"value": 1,
		"next": map[string]any{
			"value": 2,
			"next":  nil,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var result any
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	root := result.(map[string]any)
	if root["value"] != int64(1) {
		t.Fatalf("expected root value=1 (int64), got %v (%T)", root["value"], root["value"])
	}
}

func TestResolveWriterUnionNullUnionOptimization(t *testing.T) {
	// Writer ["null","int"], reader "long" — exercises null-union optimization path in resolveWriterUnion.
	writer, err := Parse(`["null","int"]`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`["null","long"]`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	// Encode non-null int.
	val := int32(7)
	encoded, err := writer.Encode(&val)
	if err != nil {
		t.Fatal(err)
	}

	var result *int64
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || *result != 7 {
		t.Fatalf("expected *int64(7), got %v", result)
	}
}

func TestEncodeDefaultErrors(t *testing.T) {
	tests := []struct {
		name   string
		val    any
		schema string
	}{
		{
			name:   "boolean wrong type",
			val:    "notbool",
			schema: `"boolean"`,
		},
		{
			name:   "int wrong type",
			val:    "notnum",
			schema: `"int"`,
		},
		{
			name:   "long wrong type",
			val:    true,
			schema: `"long"`,
		},
		{
			name:   "float wrong type",
			val:    "x",
			schema: `"float"`,
		},
		{
			name:   "double wrong type",
			val:    []int{1},
			schema: `"double"`,
		},
		{
			name:   "string wrong type",
			val:    42,
			schema: `"string"`,
		},
		{
			name:   "bytes wrong type",
			val:    42,
			schema: `"bytes"`,
		},
		{
			name:   "enum wrong type",
			val:    42,
			schema: `{"type":"enum","name":"E","symbols":["A","B"]}`,
		},
		{
			name:   "enum unknown symbol",
			val:    "UNKNOWN",
			schema: `{"type":"enum","name":"E","symbols":["A","B"]}`,
		},
		{
			name:   "fixed wrong type",
			val:    42,
			schema: `{"type":"fixed","name":"F","size":4}`,
		},
		{
			name:   "fixed wrong length",
			val:    "ab",
			schema: `{"type":"fixed","name":"F","size":4}`,
		},
		{
			name:   "array wrong type",
			val:    "notarray",
			schema: `{"type":"array","items":"int"}`,
		},
		{
			name:   "map wrong type",
			val:    "notmap",
			schema: `{"type":"map","values":"int"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatal(err)
			}
			_, err = encodeDefault(tt.val, s.node)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestEncodeDefaultUnsupportedType(t *testing.T) {
	// Unknown node kind.
	node := &schemaNode{kind: "unknown_kind"}
	_, err := encodeDefault(nil, node)
	if err == nil {
		t.Fatal("expected error for unsupported type")
	}
}

func TestEncodeDefaultEmptyUnion(t *testing.T) {
	node := &schemaNode{kind: "union", branches: nil}
	_, err := encodeDefault(nil, node)
	if err == nil {
		t.Fatal("expected error for empty union")
	}
}

func TestSkipBytesNegativeLength(t *testing.T) {
	// Encode a negative varint as the length.
	data := appendVarlong(nil, -5)
	_, err := skipBytes(data)
	if err == nil {
		t.Fatal("expected error for negative bytes length")
	}
}

func TestPromoteStringToBytesNegativeLength(t *testing.T) {
	data := appendVarlong(nil, -1)
	var b []byte
	v := reflect.ValueOf(&b).Elem()
	_, err := promoteStringToBytes(data, v)
	if err == nil {
		t.Fatal("expected error for negative length")
	}
}

func TestPromoteBytesToStringNegativeLength(t *testing.T) {
	data := appendVarlong(nil, -1)
	var s string
	v := reflect.ValueOf(&s).Elem()
	_, err := promoteBytesToString(data, v)
	if err == nil {
		t.Fatal("expected error for negative length")
	}
}

func TestResolveDeserTruncatedData(t *testing.T) {
	// Set up a resolved schema where writer has fields A (kept) and B (skipped),
	// reader has fields A (promoted) and C (default).
	writer, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"long"},
		{"name":"c","type":"int","default":0}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	// Test with truncated data (read error on first wire field).
	var result any
	_, err = resolved.Decode(nil, &result)
	if err == nil {
		t.Fatal("expected error for empty data")
	}

	// Test with data that has first field but skip of second field fails.
	data := appendVarint(nil, 42) // field a ok
	// field b (string) is missing -> skip error
	_, err = resolved.Decode(data, &result)
	if err == nil {
		t.Fatal("expected error for truncated skip data")
	}

	// Same tests decoding into map.
	resultMap := make(map[string]int64)
	_, err = resolved.Decode(nil, &resultMap)
	if err == nil {
		t.Fatal("expected error for empty data into map")
	}
	_, err = resolved.Decode(data, &resultMap)
	if err == nil {
		t.Fatal("expected error for truncated skip data into map")
	}

	// Same tests decoding into struct.
	type R struct {
		A int64 `avro:"a"`
		C int32 `avro:"c"`
	}
	var resultStruct R
	_, err = resolved.Decode(nil, &resultStruct)
	if err == nil {
		t.Fatal("expected error for empty data into struct")
	}
	_, err = resolved.Decode(data, &resultStruct)
	if err == nil {
		t.Fatal("expected error for truncated skip data into struct")
	}
}

func TestResolveDeserReadError(t *testing.T) {
	// Writer and reader both have field A but promoted (int→long).
	// Test with truncated data to trigger read error.
	writer, err := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	// Empty data: read error.
	var result any
	_, err = resolved.Decode(nil, &result)
	if err == nil {
		t.Fatal("expected read error for empty data into interface")
	}

	m := make(map[string]int64)
	_, err = resolved.Decode(nil, &m)
	if err == nil {
		t.Fatal("expected read error for empty data into map")
	}

	type R struct {
		A int64 `avro:"a"`
	}
	var s R
	_, err = resolved.Decode(nil, &s)
	if err == nil {
		t.Fatal("expected read error for empty data into struct")
	}
}

func TestResolveDeserDefaultError(t *testing.T) {
	// Set up a resolved schema where reader has a field with a default
	// whose deser will fail due to bad encoded data. This is hard to trigger
	// normally, so we test the deserMap/deserStruct error paths for defaults
	// by using valid data that exercises the default code path.
	writer, err := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string","default":"hello"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{"a": 1})
	if err != nil {
		t.Fatal(err)
	}

	// Default into map[string]int32: type mismatch (string default into int32 value).
	badMap := make(map[string]int32)
	_, err = resolved.Decode(encoded, &badMap)
	if err == nil {
		t.Fatal("expected error for string default into int32 map")
	}

	// Default into struct with wrong field type.
	type BadR struct {
		A int32 `avro:"a"`
		B int32 `avro:"b"` // wrong type for string default
	}
	var badStruct BadR
	_, err = resolved.Decode(encoded, &badStruct)
	if err == nil {
		t.Fatal("expected error for string default into int32 struct field")
	}
}

func TestResolveDeserMapNilInit(t *testing.T) {
	// Test that a nil map gets initialized during deserialization.
	writer, err := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{"a": 5})
	if err != nil {
		t.Fatal(err)
	}

	// Pass nil map pointer (v.IsNil() path).
	var m map[string]int64
	_, err = resolved.Decode(encoded, &m)
	if err != nil {
		t.Fatal(err)
	}
	if m["a"] != 5 {
		t.Fatalf("expected a=5, got %v", m["a"])
	}
}

func TestEncodeDefaultArrayNil(t *testing.T) {
	node := &schemaNode{kind: "array", items: &schemaNode{kind: "int"}}
	encoded, err := encodeDefault(nil, node)
	if err != nil {
		t.Fatal(err)
	}
	// Nil array encodes as count=0.
	if len(encoded) != 1 || encoded[0] != 0 {
		t.Fatalf("expected [0], got %v", encoded)
	}
}

func TestEncodeDefaultMapNil(t *testing.T) {
	node := &schemaNode{kind: "map", values: &schemaNode{kind: "int"}}
	encoded, err := encodeDefault(nil, node)
	if err != nil {
		t.Fatal(err)
	}
	// Nil map encodes as count=0.
	if len(encoded) != 1 || encoded[0] != 0 {
		t.Fatalf("expected [0], got %v", encoded)
	}
}

func TestEncodeDefaultArrayItemError(t *testing.T) {
	node := &schemaNode{kind: "array", items: &schemaNode{kind: "int"}}
	// Array with wrong-type item.
	_, err := encodeDefault([]any{"not_a_number"}, node)
	if err == nil {
		t.Fatal("expected error for wrong item type in array")
	}
}

func TestEncodeDefaultMapValueError(t *testing.T) {
	node := &schemaNode{kind: "map", values: &schemaNode{kind: "int"}}
	// Map with wrong-type value.
	_, err := encodeDefault(map[string]any{"k": "not_a_number"}, node)
	if err == nil {
		t.Fatal("expected error for wrong value type in map")
	}
}

func TestEncodeDefaultRecordNilVal(t *testing.T) {
	node := &schemaNode{
		kind: "record",
		name: "R",
		fields: []fieldNode{
			{name: "a", node: &schemaNode{kind: "int"}, defaultVal: float64(0), hasDefault: true},
		},
	}
	// nil val → uses field defaults.
	encoded, err := encodeDefault(nil, node)
	if err != nil {
		t.Fatal(err)
	}
	// Should encode field "a" with default 0.
	s, err := Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	var v int32
	_, err = s.Decode(encoded, &v)
	if err != nil {
		t.Fatal(err)
	}
	if v != 0 {
		t.Fatalf("expected 0, got %d", v)
	}
}

func TestEncodeDefaultRecordMissingFieldNoDefault(t *testing.T) {
	node := &schemaNode{
		kind: "record",
		name: "R",
		fields: []fieldNode{
			{name: "a", node: &schemaNode{kind: "int"}, hasDefault: false},
		},
	}
	_, err := encodeDefault(map[string]any{}, node)
	if err == nil {
		t.Fatal("expected error for missing field with no default")
	}
}

func TestEncodeDefaultRecordFieldSubError(t *testing.T) {
	node := &schemaNode{
		kind: "record",
		name: "R",
		fields: []fieldNode{
			{name: "a", node: &schemaNode{kind: "int"}},
		},
	}
	// Provide wrong type for field.
	_, err := encodeDefault(map[string]any{"a": "not_a_number"}, node)
	if err == nil {
		t.Fatal("expected error for wrong field type")
	}
}

func TestEncodeDefaultRecordFieldDefault(t *testing.T) {
	node := &schemaNode{
		kind: "record",
		name: "R",
		fields: []fieldNode{
			{name: "a", node: &schemaNode{kind: "int"}, defaultVal: float64(42), hasDefault: true},
			{name: "b", node: &schemaNode{kind: "string"}},
		},
	}
	// Provide "b" but not "a" — "a" has a default.
	encoded, err := encodeDefault(map[string]any{"b": "hello"}, node)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) == 0 {
		t.Fatal("expected non-empty encoded output")
	}
}

func TestResolveFieldRemovedIntoMap(t *testing.T) {
	// Writer has fields [a, b], reader has field [a]. Field b gets skipped.
	// Decode into map to exercise deserMap's skip-continue path.
	writer, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{"a": 42, "b": "drop me"})
	if err != nil {
		t.Fatal(err)
	}

	m := make(map[string]int32)
	_, err = resolved.Decode(encoded, &m)
	if err != nil {
		t.Fatal(err)
	}
	if m["a"] != 42 {
		t.Fatalf("expected a=42, got %v", m["a"])
	}
}

func TestResolveFieldRemovedIntoStruct(t *testing.T) {
	// Same setup but decode into struct to exercise deserStruct skip-continue path.
	writer, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{"a": 42, "b": "drop me"})
	if err != nil {
		t.Fatal(err)
	}

	type R struct {
		A int32 `avro:"a"`
	}
	var result R
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	if result.A != 42 {
		t.Fatalf("expected A=42, got %d", result.A)
	}
}

func TestResolveEnumDeserInterface(t *testing.T) {
	// Non-identity enum decoded into interface.
	writer, err := Parse(`{"type":"enum","name":"E","symbols":["A","B","C"]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"enum","name":"E","symbols":["B","A","C"],"default":"A"}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode("B")
	if err != nil {
		t.Fatal(err)
	}

	var result any
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	if result != "B" {
		t.Fatalf("expected B, got %v", result)
	}
}

func TestSkipArrayMinInt64(t *testing.T) {
	// Craft a varint encoding of math.MinInt64 as a zigzag-encoded block count.
	// zigzag(math.MinInt64) = math.MaxUint64 (a 10-byte varint).
	// After reading as negative and negating, -math.MinInt64 overflows to math.MinInt64 (still negative).
	intNode := &schemaNode{kind: "int"}
	arrNode := &schemaNode{kind: "array", items: intNode}
	skip := buildSkip(arrNode)

	// math.MinInt64 zigzag-encoded as varint.
	data := appendVarlong(nil, math.MinInt64)
	_, err := skip(data)
	if err == nil {
		t.Fatal("expected error for math.MinInt64 block count")
	}
}

func TestSkipMapMinInt64(t *testing.T) {
	intNode := &schemaNode{kind: "int"}
	mapNode := &schemaNode{kind: "map", values: intNode}
	skip := buildSkip(mapNode)

	data := appendVarlong(nil, math.MinInt64)
	_, err := skip(data)
	if err == nil {
		t.Fatal("expected error for math.MinInt64 block count")
	}
}

func TestDoResolveFixed(t *testing.T) {
	// Call doResolve directly for fixed schemas (unreachable through Resolve
	// because fixed schemas with same name/size have identical canonical forms).
	r := &schemaNode{kind: "fixed", name: "F", size: 4}
	w := &schemaNode{kind: "fixed", name: "F", size: 4}
	resolved, err := doResolve(r, w, "", make(map[nodePair]*schemaNode))
	if err != nil {
		t.Fatal(err)
	}
	if resolved != r {
		t.Fatal("expected reader node returned directly for fixed")
	}
}

func TestDoResolveIncompatible(t *testing.T) {
	// Call doResolve directly for incompatible types.
	r := &schemaNode{kind: "int"}
	w := &schemaNode{kind: "string"}
	_, err := doResolve(r, w, "", make(map[nodePair]*schemaNode))
	if err == nil {
		t.Fatal("expected error for incompatible types")
	}
}

func TestResolveNodeError(t *testing.T) {
	// Call resolveNode directly to trigger the doResolve error path.
	r := &schemaNode{kind: "int"}
	w := &schemaNode{kind: "boolean"}
	_, err := resolveNode(r, w, "", make(map[nodePair]*schemaNode))
	if err == nil {
		t.Fatal("expected error for incompatible types")
	}
}

func TestResolveRecordFieldError(t *testing.T) {
	// Call resolveRecord directly with incompatible field types.
	r := &schemaNode{
		kind: "record", name: "R",
		fields: []fieldNode{
			{name: "a", node: &schemaNode{kind: "int"}},
		},
	}
	w := &schemaNode{
		kind: "record", name: "R",
		fields: []fieldNode{
			{name: "a", node: &schemaNode{kind: "boolean"}},
		},
	}
	_, err := resolveRecord(r, w, "", make(map[nodePair]*schemaNode))
	if err == nil {
		t.Fatal("expected error for incompatible field types")
	}
}

func TestResolveRecordDefaultError(t *testing.T) {
	// Call resolveRecord directly with a field that has a bad default.
	r := &schemaNode{
		kind: "record", name: "R",
		fields: []fieldNode{
			{name: "a", node: &schemaNode{kind: "int"}, defaultVal: "not_a_number", hasDefault: true},
		},
	}
	w := &schemaNode{
		kind: "record", name: "R",
		fields: nil,
	}
	_, err := resolveRecord(r, w, "", make(map[nodePair]*schemaNode))
	if err == nil {
		t.Fatal("expected error for bad default value")
	}
}

func TestResolveEnumNoDefault(t *testing.T) {
	// Call resolveEnum directly: writer has symbol not in reader, no default.
	r := &schemaNode{kind: "enum", name: "E", symbols: []string{"A", "B"}}
	w := &schemaNode{kind: "enum", name: "E", symbols: []string{"A", "B", "C"}}
	_, err := resolveEnum(r, w)
	if err == nil {
		t.Fatal("expected error for writer symbol not in reader")
	}
}

func TestResolveEnumBadDefault(t *testing.T) {
	// Call resolveEnum directly: reader has default not in its own symbols.
	r := &schemaNode{kind: "enum", name: "E", symbols: []string{"A", "B"}, enumDef: "MISSING", hasEnumDef: true}
	w := &schemaNode{kind: "enum", name: "E", symbols: []string{"A", "B", "C"}}
	_, err := resolveEnum(r, w)
	if err == nil {
		t.Fatal("expected error for invalid enum default")
	}
}

func TestResolveArrayError(t *testing.T) {
	// Call resolveArray directly with incompatible items.
	r := &schemaNode{kind: "array", items: &schemaNode{kind: "int"}}
	w := &schemaNode{kind: "array", items: &schemaNode{kind: "boolean"}}
	_, err := resolveArray(r, w, "", make(map[nodePair]*schemaNode))
	if err == nil {
		t.Fatal("expected error for incompatible array items")
	}
}

func TestResolveMapError(t *testing.T) {
	// Call resolveMap directly with incompatible values.
	r := &schemaNode{kind: "map", values: &schemaNode{kind: "int"}}
	w := &schemaNode{kind: "map", values: &schemaNode{kind: "boolean"}}
	_, err := resolveMap(r, w, "", make(map[nodePair]*schemaNode))
	if err == nil {
		t.Fatal("expected error for incompatible map values")
	}
}

func TestResolveWriterUnionError(t *testing.T) {
	// Call resolveWriterUnion directly: a branch doesn't resolve.
	r := &schemaNode{kind: "int"}
	w := &schemaNode{kind: "union", branches: []*schemaNode{
		{kind: "boolean"},
	}}
	_, err := resolveWriterUnion(r, w, "", make(map[nodePair]*schemaNode))
	if err == nil {
		t.Fatal("expected error for incompatible writer union branch")
	}
}

func TestResolveReaderUnionError(t *testing.T) {
	// Call resolveReaderUnion directly: no matching branch.
	r := &schemaNode{kind: "union", branches: []*schemaNode{
		{kind: "null"},
		{kind: "int"},
	}}
	w := &schemaNode{kind: "boolean"}
	_, err := resolveReaderUnion(r, w, "", make(map[nodePair]*schemaNode))
	if err == nil {
		t.Fatal("expected error for no matching reader union branch")
	}
}

func TestResolveReaderUnionBranchError(t *testing.T) {
	// Call resolveReaderUnion directly: matching branch found but resolveNode fails.
	r := &schemaNode{kind: "union", branches: []*schemaNode{
		{kind: "record", name: "R", fields: []fieldNode{
			{name: "a", node: &schemaNode{kind: "int"}},
		}},
	}}
	w := &schemaNode{kind: "record", name: "R", fields: []fieldNode{
		{name: "a", node: &schemaNode{kind: "boolean"}},
	}}
	_, err := resolveReaderUnion(r, w, "", make(map[nodePair]*schemaNode))
	if err == nil {
		t.Fatal("expected error for incompatible record in reader union")
	}
}

func TestResolveUnionUnionNoMatch(t *testing.T) {
	// Call resolveUnionUnion directly: writer branch has no match in reader.
	r := &schemaNode{kind: "union", branches: []*schemaNode{
		{kind: "null"},
		{kind: "int"},
	}}
	w := &schemaNode{kind: "union", branches: []*schemaNode{
		{kind: "boolean"},
	}}
	_, err := resolveUnionUnion(r, w, "", make(map[nodePair]*schemaNode))
	if err == nil {
		t.Fatal("expected error for unmatched writer union branch")
	}
}

func TestResolveUnionUnionBranchError(t *testing.T) {
	// Call resolveUnionUnion directly: branch matches by kind but resolveNode fails.
	r := &schemaNode{kind: "union", branches: []*schemaNode{
		{kind: "record", name: "R", fields: []fieldNode{
			{name: "a", node: &schemaNode{kind: "int"}},
		}},
	}}
	w := &schemaNode{kind: "union", branches: []*schemaNode{
		{kind: "record", name: "R", fields: []fieldNode{
			{name: "a", node: &schemaNode{kind: "boolean"}},
		}},
	}}
	_, err := resolveUnionUnion(r, w, "", make(map[nodePair]*schemaNode))
	if err == nil {
		t.Fatal("expected error for incompatible records in union-union")
	}
}

func TestResolveDeserStructMissingField(t *testing.T) {
	// Struct is missing a field the reader schema expects → typeFieldMapping error.
	// Schemas must differ so Resolve doesn't short-circuit.
	writer, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"long"},
		{"name":"b","type":"string"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{"a": 1, "b": "x"})
	if err != nil {
		t.Fatal(err)
	}

	// Struct only has field A, missing field B.
	type Partial struct {
		A int64 `avro:"a"`
	}
	var result Partial
	_, err = resolved.Decode(encoded, &result)
	if err == nil {
		t.Fatal("expected error for struct missing field b")
	}
}

func TestResolveNamespacedAlias(t *testing.T) {
	// Reader uses a namespaced name with aliases, writer uses one of the aliases.
	// This exercises qualifyAliases with a dot in fullname and unqualified aliases.
	reader, err := Parse(`{
		"type":"record",
		"name":"com.example.NewName",
		"aliases":["OldName"],
		"fields":[{"name":"a","type":"int"}]
	}`)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := Parse(`{
		"type":"record",
		"name":"com.example.OldName",
		"fields":[{"name":"a","type":"int"}]
	}`)
	if err != nil {
		t.Fatal(err)
	}
	err = CheckCompatibility(writer, reader)
	if err != nil {
		t.Fatalf("expected compatible via alias: %v", err)
	}
}

func TestResolveFullyQualifiedAlias(t *testing.T) {
	// Alias already contains a dot (fully qualified).
	reader, err := Parse(`{
		"type":"record",
		"name":"com.example.NewName",
		"aliases":["com.other.OldName"],
		"fields":[{"name":"a","type":"int"}]
	}`)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := Parse(`{
		"type":"record",
		"name":"com.other.OldName",
		"fields":[{"name":"a","type":"int"}]
	}`)
	if err != nil {
		t.Fatal(err)
	}
	err = CheckCompatibility(writer, reader)
	if err != nil {
		t.Fatalf("expected compatible via fully-qualified alias: %v", err)
	}
}

func TestResolveNullUnionDefault(t *testing.T) {
	writer, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"x","type":"int"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"x","type":"int"},
		{"name":"opt","type":["null","string"],"default":null}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := writer.Encode(map[string]any{"x": 5})
	if err != nil {
		t.Fatal(err)
	}

	var result any
	_, err = resolved.Decode(encoded, &result)
	if err != nil {
		t.Fatal(err)
	}
	m := result.(map[string]any)
	if m["x"] != int32(5) {
		t.Fatalf("expected x=5, got %v", m["x"])
	}
	// opt should be nil (null default).
	if m["opt"] != nil {
		t.Fatalf("expected opt=nil, got %v", m["opt"])
	}
}
