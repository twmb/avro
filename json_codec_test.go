package avro

import (
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestEncodeJSON(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		value  any
		want   string
	}{
		{"null", `"null"`, nil, `null`},
		{"boolean", `"boolean"`, true, `true`},
		{"int", `"int"`, int32(42), `42`},
		{"long", `"long"`, int64(123456789), `123456789`},
		{"float", `"float"`, float32(1.5), `1.5`},
		{"double", `"double"`, float64(3.14), `3.14`},
		{"string", `"string"`, "hello", `"hello"`},
		{"bytes", `"bytes"`, []byte{0x00, 0xFF, 0x41}, `"\u0000\u00FFA"`},
		{"enum", `{"type":"enum","name":"Color","symbols":["RED","GREEN"]}`, "RED", `"RED"`},
		{
			"fixed",
			`{"type":"fixed","name":"F","size":3}`,
			[3]byte{0x01, 0x02, 0x03},
			`"\u0001\u0002\u0003"`,
		},
		{
			"array",
			`{"type":"array","items":"int"}`,
			[]any{int32(1), int32(2), int32(3)},
			`[1,2,3]`,
		},
		{
			"map",
			`{"type":"map","values":"int"}`,
			map[string]any{"a": int32(1)},
			`{"a":1}`,
		},
		{
			"union null",
			`["null","string"]`,
			nil,
			`null`,
		},
		{
			"union string",
			`["null","string"]`,
			"hello",
			`"hello"`,
		},
		{
			"union int",
			`["null","int","string"]`,
			int32(42),
			`42`,
		},
		{
			"record",
			`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`,
			map[string]any{"a": int32(1), "b": "hello"},
			`{"a":1,"b":"hello"}`,
		},
		{
			"nested record with union",
			`{"type":"record","name":"R","fields":[
				{"name":"name","type":"string"},
				{"name":"email","type":["null","string"]}
			]}`,
			map[string]any{"name": "Alice", "email": "a@b.com"},
			`{"name":"Alice","email":"a@b.com"}`,
		},
		{
			"nested record with null union",
			`{"type":"record","name":"R","fields":[
				{"name":"name","type":"string"},
				{"name":"email","type":["null","string"]}
			]}`,
			map[string]any{"name": "Bob", "email": nil},
			`{"name":"Bob","email":null}`,
		},
		{
			"float NaN",
			`"float"`,
			float32(math.Float32frombits(0x7fc00000)),
			`"NaN"`,
		},
		{
			"double Infinity",
			`"double"`,
			math.Inf(1),
			`"Infinity"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatal(err)
			}
			got, err := s.EncodeJSON(tt.value)
			if err != nil {
				t.Fatalf("EncodeJSON: %v", err)
			}
			if string(got) != tt.want {
				t.Errorf("got %s, want %s", got, tt.want)
			}
		})
	}
}

func TestDecodeJSON(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		input  string
		want   any
	}{
		{"null", `"null"`, `null`, nil},
		{"boolean", `"boolean"`, `true`, true},
		{"int", `"int"`, `42`, int32(42)},
		{"long", `"long"`, `123456789`, int64(123456789)},
		{"float", `"float"`, `1.5`, float32(1.5)},
		{"double", `"double"`, `3.14`, 3.14},
		{"string", `"string"`, `"hello"`, "hello"},
		{"bytes", `"bytes"`, `"\u0000\u00FFA"`, []byte{0x00, 0xFF, 0x41}},
		{"enum", `{"type":"enum","name":"Color","symbols":["RED","GREEN"]}`, `"RED"`, "RED"},
		{
			"array",
			`{"type":"array","items":"int"}`,
			`[1,2,3]`,
			[]any{int32(1), int32(2), int32(3)},
		},
		{
			"union null",
			`["null","string"]`,
			`null`,
			nil,
		},
		{
			"union string",
			`["null","string"]`,
			`{"string":"hello"}`,
			"hello",
		},
		{
			"record with union",
			`{"type":"record","name":"R","fields":[
				{"name":"name","type":"string"},
				{"name":"email","type":["null","string"]}
			]}`,
			`{"name":"Alice","email":{"string":"a@b.com"}}`,
			map[string]any{"name": "Alice", "email": "a@b.com"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatal(err)
			}
			var got any
			if err := s.DecodeJSON([]byte(tt.input), &got); err != nil {
				t.Fatalf("DecodeJSON: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestAvroJSONRoundTrip(t *testing.T) {
	schema := `{
		"type":"record","name":"Event",
		"fields":[
			{"name":"id","type":"string"},
			{"name":"ts","type":"long"},
			{"name":"data","type":"bytes"},
			{"name":"tags","type":{"type":"array","items":"string"}},
			{"name":"meta","type":{"type":"map","values":"int"}},
			{"name":"status","type":{"type":"enum","name":"Status","symbols":["ACTIVE","DELETED"]}},
			{"name":"extra","type":["null","string","int"]}
		]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}

	original := map[string]any{
		"id":     "abc",
		"ts":     int64(1000),
		"data":   []byte{0x01, 0x02},
		"tags":   []any{"go", "avro"},
		"meta":   map[string]any{"x": int32(1)},
		"status": "ACTIVE",
		"extra":  "hello",
	}

	// Encode to Avro JSON.
	encoded, err := s.EncodeJSON(original)
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}

	// Verify it's valid JSON.
	var parsed any
	if err := json.Unmarshal(encoded, &parsed); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, encoded)
	}

	// Decode back.
	var decoded any
	if err := s.DecodeJSON(encoded, &decoded); err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}

	m := decoded.(map[string]any)
	if m["id"] != "abc" {
		t.Errorf("id: got %v", m["id"])
	}
	if m["ts"] != int64(1000) {
		t.Errorf("ts: got %v", m["ts"])
	}
	if m["status"] != "ACTIVE" {
		t.Errorf("status: got %v", m["status"])
	}
	if m["extra"] != "hello" {
		t.Errorf("extra: got %v", m["extra"])
	}
}

func TestAvroJSONNamedUnionBranch(t *testing.T) {
	schema := `{
		"type":"record","name":"Wrapper",
		"fields":[{
			"name":"value",
			"type":["null",{"type":"record","name":"Inner","fields":[
				{"name":"x","type":"int"}
			]}]
		}]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}

	// Encode: with TaggedUnions, non-null branch should use the record name.
	data := map[string]any{
		"value": map[string]any{"x": int32(42)},
	}
	encoded, err := s.EncodeJSON(data, TaggedUnions())
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	// Should contain "Inner" as the union type name.
	var parsed map[string]any
	json.Unmarshal(encoded, &parsed)
	valueObj, ok := parsed["value"].(map[string]any)
	if !ok {
		t.Fatalf("value: expected object, got %T: %s", parsed["value"], encoded)
	}
	if _, ok := valueObj["Inner"]; !ok {
		t.Errorf("expected Inner key in union, got: %s", encoded)
	}

	// Decode back.
	var decoded any
	if err := s.DecodeJSON(encoded, &decoded); err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	m := decoded.(map[string]any)
	inner := m["value"].(map[string]any)
	if inner["x"] != int32(42) {
		t.Errorf("x: got %v", inner["x"])
	}
}

func TestDecodeJSONIntoStruct(t *testing.T) {
	type Record struct {
		Name  string  `avro:"name"`
		Email *string `avro:"email"`
	}
	schema := `{"type":"record","name":"Record","fields":[
		{"name":"name","type":"string"},
		{"name":"email","type":["null","string"]}
	]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}

	input := `{"name":"Alice","email":{"string":"a@b.com"}}`
	var got Record
	if err := s.DecodeJSON([]byte(input), &got); err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	if got.Name != "Alice" {
		t.Errorf("name: got %q", got.Name)
	}
	if got.Email == nil || *got.Email != "a@b.com" {
		t.Errorf("email: got %v", got.Email)
	}
}

func TestDecodeJSONInvalidUnion(t *testing.T) {
	s, err := Parse(`["null","string"]`)
	if err != nil {
		t.Fatal(err)
	}
	// Wrong branch name.
	var v any
	if err := s.DecodeJSON([]byte(`{"int":42}`), &v); err == nil {
		t.Fatal("expected error for unknown union branch")
	}
}

func TestAvroJSONNamespacedUnionBranch(t *testing.T) {
	schema := `["null",{"type":"enum","name":"Status","namespace":"com.example","symbols":["ACTIVE","DELETED"]}]`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}

	// Encode with TaggedUnions: should use fully qualified name.
	encoded, err := s.EncodeJSON("ACTIVE", TaggedUnions())
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	want := `{"com.example.Status":"ACTIVE"}`
	if string(encoded) != want {
		t.Errorf("got %s, want %s", encoded, want)
	}

	// Decode back.
	var got any
	if err := s.DecodeJSON(encoded, &got); err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	if got != "ACTIVE" {
		t.Errorf("got %v, want ACTIVE", got)
	}
}

func TestAvroJSONNestedUnionRecord(t *testing.T) {
	// Three-level nested record with union fields (like goavro's LongList test).
	schema := `{
		"type":"record","name":"Node",
		"fields":[
			{"name":"value","type":"int"},
			{"name":"next","type":["null","Node"]}
		]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}

	data := map[string]any{
		"value": int32(1),
		"next": map[string]any{
			"value": int32(2),
			"next": map[string]any{
				"value": int32(3),
				"next":  nil,
			},
		},
	}
	// Tagged: should have Node wrapping at each level.
	encoded, err := s.EncodeJSON(data, TaggedUnions())
	if err != nil {
		t.Fatalf("EncodeJSON(TaggedUnions()): %v", err)
	}
	var parsed any
	json.Unmarshal(encoded, &parsed)
	m := parsed.(map[string]any)
	next := m["next"].(map[string]any)
	if _, ok := next["Node"]; !ok {
		t.Errorf("expected Node key in tagged union, got: %s", encoded)
	}
	var got any
	if err := s.DecodeJSON(encoded, &got); err != nil {
		t.Fatalf("DecodeJSON tagged: %v", err)
	}
	gm := got.(map[string]any)
	if gm["value"] != int32(1) {
		t.Errorf("value: got %v", gm["value"])
	}

	// Bare: should produce nested records without wrapping.
	bare, err := s.EncodeJSON(data)
	if err != nil {
		t.Fatalf("EncodeJSON(bare): %v", err)
	}
	var got2 any
	if err := s.DecodeJSON(bare, &got2); err != nil {
		t.Fatalf("DecodeJSON bare: %v", err)
	}
	gm2 := got2.(map[string]any)
	if gm2["value"] != int32(1) {
		t.Errorf("value: got %v", gm2["value"])
	}
}

func TestAvroJSONBytesEdgeCases(t *testing.T) {
	s, err := Parse(`"bytes"`)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name  string
		input []byte
	}{
		{"empty", []byte{}},
		{"ascii", []byte("hello")},
		{"quote", []byte(`a"b`)},
		{"backslash", []byte(`a\b`)},
		{"control", []byte{0x00, 0x01, 0x0A}},
		{"high bytes", []byte{0x80, 0xFF, 0xFE}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := s.EncodeJSON(tt.input)
			if err != nil {
				t.Fatalf("EncodeJSON: %v", err)
			}
			var got any
			if err := s.DecodeJSON(encoded, &got); err != nil {
				t.Fatalf("DecodeJSON: %v", err)
			}
			if !reflect.DeepEqual(got.([]byte), tt.input) {
				t.Errorf("got %v, want %v", got, tt.input)
			}
		})
	}
}

func TestAvroJSONArrayOfUnions(t *testing.T) {
	schema := `{"type":"array","items":["null","string","int"]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}

	// First verify binary encoding works.
	data := []any{"hello", int32(42)}
	binary, err := s.Encode(data)
	if err != nil {
		t.Fatalf("Encode binary: %v", err)
	}
	t.Logf("binary: %v", binary)

	// Bare (default): unwrapped values.
	encoded, err := s.EncodeJSON(data)
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	want := `["hello",42]`
	if string(encoded) != want {
		t.Errorf("got %s, want %s", encoded, want)
	}

	// Decode back from bare.
	var got any
	if err := s.DecodeJSON(encoded, &got); err != nil {
		t.Fatalf("DecodeJSON bare: %v", err)
	}
	arr := got.([]any)
	if len(arr) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(arr))
	}

	// Tagged: wrapped values.
	tagged, err := s.EncodeJSON(data, TaggedUnions())
	if err != nil {
		t.Fatalf("EncodeJSON(TaggedUnions()): %v", err)
	}
	wantTagged := `[{"string":"hello"},{"int":42}]`
	if string(tagged) != wantTagged {
		t.Errorf("got %s, want %s", tagged, wantTagged)
	}
}

func TestAvroJSONArrayOfUnionsWithNull(t *testing.T) {
	schema := `{"type":"array","items":["null","string"]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	// Encode with nil in the array.
	data := []any{nil, "hello", nil}
	encoded, err := s.EncodeJSON(data)
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	want := `[null,"hello",null]`
	if string(encoded) != want {
		t.Errorf("got %s, want %s", encoded, want)
	}
}

func TestDecodeJSONArrayOfUnionsWithNull(t *testing.T) {
	schema := `{"type":"array","items":["null","string"]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	// Decode Avro JSON with null in union array.
	input := `[null,{"string":"hello"},null]`
	var got any
	if err := s.DecodeJSON([]byte(input), &got); err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	arr := got.([]any)
	if len(arr) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(arr))
	}
	if arr[0] != nil {
		t.Errorf("arr[0]: got %v, want nil", arr[0])
	}
	if arr[1] != "hello" {
		t.Errorf("arr[1]: got %v, want hello", arr[1])
	}
	if arr[2] != nil {
		t.Errorf("arr[2]: got %v, want nil", arr[2])
	}
}

func TestDecodeJSONFixed(t *testing.T) {
	s, err := Parse(`{"type":"fixed","name":"F","size":3}`)
	if err != nil {
		t.Fatal(err)
	}
	var got any
	if err := s.DecodeJSON([]byte(`"\u0001\u0002\u0003"`), &got); err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	b, ok := got.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", got)
	}
	if len(b) != 3 || b[0] != 1 || b[1] != 2 || b[2] != 3 {
		t.Errorf("got %v, want [1 2 3]", b)
	}
}

func TestDecodeJSONNull(t *testing.T) {
	s, err := Parse(`"null"`)
	if err != nil {
		t.Fatal(err)
	}
	var got any
	if err := s.DecodeJSON([]byte(`null`), &got); err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	if got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

func TestDecodeJSONMapMultipleKeys(t *testing.T) {
	s, err := Parse(`{"type":"map","values":"int"}`)
	if err != nil {
		t.Fatal(err)
	}
	var got any
	if err := s.DecodeJSON([]byte(`{"a":1,"b":2,"c":3}`), &got); err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	m := got.(map[string]any)
	if len(m) != 3 {
		t.Errorf("expected 3 keys, got %d", len(m))
	}
}

func TestDecodeJSONRecordMissingField(t *testing.T) {
	s, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int","default":0},
		{"name":"b","type":"string"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	// "a" is missing from the JSON; it has a default so Encode fills it.
	var got any
	if err := s.DecodeJSON([]byte(`{"b":"hello"}`), &got); err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	m := got.(map[string]any)
	if m["b"] != "hello" {
		t.Errorf("b: got %v", m["b"])
	}
}

func TestDecodeJSONUnionNull(t *testing.T) {
	s, err := Parse(`["null","string"]`)
	if err != nil {
		t.Fatal(err)
	}
	var got any
	if err := s.DecodeJSON([]byte(`null`), &got); err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	if got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

func TestEncodeJSONMapMultipleEntries(t *testing.T) {
	s, err := Parse(`{"type":"map","values":"int"}`)
	if err != nil {
		t.Fatal(err)
	}
	data := map[string]any{"a": int32(1), "b": int32(2)}
	encoded, err := s.EncodeJSON(data)
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	// Verify it's valid JSON with 2 entries.
	var parsed map[string]any
	if err := json.Unmarshal(encoded, &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if len(parsed) != 2 {
		t.Errorf("expected 2 entries, got %d", len(parsed))
	}
}

func TestEncodeJSONNegativeInfinity(t *testing.T) {
	s, err := Parse(`"double"`)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := s.EncodeJSON(math.Inf(-1))
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	if string(encoded) != `"-Infinity"` {
		t.Errorf("got %s, want \"-Infinity\"", encoded)
	}
}

func TestDecodeJSONTypeErrors(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		input  string
	}{
		{"bool expects bool", `"boolean"`, `42`},
		{"bytes expects string", `"bytes"`, `42`},
		{"fixed expects string", `{"type":"fixed","name":"F","size":2}`, `42`},
		{"array expects array", `{"type":"array","items":"int"}`, `42`},
		{"map expects object", `{"type":"map","values":"int"}`, `42`},
		{"record expects object", `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`, `42`},
		{"union expects object", `["null","string"]`, `42`},
		{"union wrong key count", `["null","string"]`, `{"a":1,"b":2}`},
		{"union unknown branch", `["null","string"]`, `{"int":42}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatal(err)
			}
			var got any
			if err := s.DecodeJSON([]byte(tt.input), &got); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestAppendAvroJSONTypeErrors(t *testing.T) {
	// Test error paths in appendAvroJSON directly.
	tests := []struct {
		name string
		kind string
		val  any
	}{
		{"bool wrong type", "boolean", 42},
		{"int wrong type", "int", "not int"},
		{"long wrong type", "long", "not long"},
		{"float wrong type", "float", "not float"},
		{"double wrong type", "double", "not double"},
		{"string wrong type", "string", 42},
		{"bytes wrong type", "bytes", 42},
		{"enum wrong type", "enum", 42},
		{"array wrong type", "array", 42},
		{"map wrong type", "map", 42},
		{"record wrong type", "record", 42},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &schemaNode{kind: tt.kind}
			if tt.kind == "array" {
				node.items = &schemaNode{kind: "int"}
			}
			if tt.kind == "map" {
				node.values = &schemaNode{kind: "int"}
			}
			_, err := appendAvroJSON(nil, reflect.ValueOf(tt.val), node, &optConfig{})
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestAppendAvroJSONFixedReflect(t *testing.T) {
	node := &schemaNode{kind: "fixed", size: 3}
	buf, err := appendAvroJSON(nil, reflect.ValueOf([3]byte{1, 2, 3}), node, &optConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if string(buf) != `"\u0001\u0002\u0003"` {
		t.Errorf("got %s", buf)
	}

	// Non-byte array should error.
	_, err = appendAvroJSON(nil, reflect.ValueOf([3]int{1, 2, 3}), node, &optConfig{})
	if err == nil {
		t.Fatal("expected error for non-byte array")
	}
}

func TestAppendAvroJSONUnionNoMatch(t *testing.T) {
	node := &schemaNode{
		kind:     "union",
		branches: []*schemaNode{{kind: "null"}, {kind: "string"}},
	}
	_, err := appendAvroJSON(nil, reflect.ValueOf(int32(42)), node, &optConfig{})
	if err == nil {
		t.Fatal("expected error for unmatched union")
	}
}

func TestAppendAvroJSONUnknownKind(t *testing.T) {
	node := &schemaNode{kind: "bogus"}
	_, err := appendAvroJSON(nil, reflect.ValueOf(42), node, &optConfig{})
	if err == nil {
		t.Fatal("expected error for unknown kind")
	}
}

func TestFromAvroJSONNullStandalone(t *testing.T) {
	node := &schemaNode{kind: "null"}
	got, err := fromAvroJSON(nil, node)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

func TestFromAvroJSONErrorPropagation(t *testing.T) {
	// Array with bad item type.
	node := &schemaNode{kind: "array", items: &schemaNode{kind: "int"}}
	_, err := fromAvroJSON([]any{"not int"}, node)
	// This won't error in fromAvroJSON since it just passes through.
	// But encoding will fail later. That's fine — fromAvroJSON is lenient.
	_ = err

	// Map with error in value.
	mapNode := &schemaNode{kind: "map", values: &schemaNode{kind: "bogus"}}
	_, err = fromAvroJSON(map[string]any{"k": "v"}, mapNode)
	if err == nil {
		t.Fatal("expected error for bogus map value type")
	}

	// Record with error in field.
	recNode := &schemaNode{
		kind: "record",
		fields: []fieldNode{
			{name: "a", node: &schemaNode{kind: "bogus"}},
		},
	}
	_, err = fromAvroJSON(map[string]any{"a": "v"}, recNode)
	if err == nil {
		t.Fatal("expected error for bogus record field type")
	}

	// Unknown kind.
	_, err = fromAvroJSON("v", &schemaNode{kind: "bogus"})
	if err == nil {
		t.Fatal("expected error for unknown kind")
	}
}

func TestFromAvroJSONUnionNull(t *testing.T) {
	node := &schemaNode{
		kind:     "union",
		branches: []*schemaNode{{kind: "null"}, {kind: "string"}},
	}
	got, err := fromAvroJSON(nil, node)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

func TestSchemaNodeErrors(t *testing.T) {
	// Schema() with invalid node.
	n := &SchemaNode{Type: "record"} // missing name
	_, err := n.Schema()
	if err == nil {
		t.Fatal("expected error for record without name")
	}

	// Root() on a schema — the JSON re-parse can't actually fail since
	// Schema.full is always valid JSON set by Parse. But let's verify
	// Root works on all schema types.
	for _, schema := range []string{
		`"null"`,
		`"int"`,
		`["null","string"]`,
		`{"type":"array","items":"int"}`,
		`{"type":"map","values":"string"}`,
	} {
		s, err := Parse(schema)
		if err != nil {
			t.Fatal(err)
		}
		_ = s.Root() // should not panic
	}
}

func TestEncodeJSONStruct(t *testing.T) {
	type Inner struct {
		X int32 `avro:"x"`
	}
	type Record struct {
		Name   string   `avro:"name"`
		Age    int32    `avro:"age"`
		Score  float64  `avro:"score"`
		Active bool     `avro:"active"`
		Tags   []string `avro:"tags"`
		Inner  Inner    `avro:"inner"`
		Email  *string  `avro:"email"`
	}
	s, err := Parse(`{
		"type":"record","name":"Record",
		"fields":[
			{"name":"name","type":"string"},
			{"name":"age","type":"int"},
			{"name":"score","type":"double"},
			{"name":"active","type":"boolean"},
			{"name":"tags","type":{"type":"array","items":"string"}},
			{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}},
			{"name":"email","type":["null","string"]}
		]
	}`)
	if err != nil {
		t.Fatal(err)
	}
	email := "a@b.com"
	r := Record{
		Name:   "Alice",
		Age:    30,
		Score:  98.6,
		Active: true,
		Tags:   []string{"go"},
		Inner:  Inner{X: 42},
		Email:  &email,
	}
	encoded, err := s.EncodeJSON(&r)
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	// Verify it's valid JSON and round-trips.
	var decoded any
	if err := s.DecodeJSON(encoded, &decoded); err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	m := decoded.(map[string]any)
	if m["name"] != "Alice" {
		t.Errorf("name: got %v", m["name"])
	}
	if m["age"] != int32(30) {
		t.Errorf("age: got %v", m["age"])
	}
	if m["email"] != "a@b.com" {
		t.Errorf("email: got %v", m["email"])
	}
}

func TestEncodeJSONStructNilPointer(t *testing.T) {
	type Record struct {
		Name  string  `avro:"name"`
		Email *string `avro:"email"`
	}
	s, err := Parse(`{
		"type":"record","name":"Record",
		"fields":[
			{"name":"name","type":"string"},
			{"name":"email","type":["null","string"]}
		]
	}`)
	if err != nil {
		t.Fatal(err)
	}
	r := Record{Name: "Bob", Email: nil}
	encoded, err := s.EncodeJSON(&r)
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	var parsed map[string]any
	json.Unmarshal(encoded, &parsed)
	if parsed["email"] != nil {
		t.Errorf("email: got %v, want null", parsed["email"])
	}
}

func TestEncodeJSONTimestamp(t *testing.T) {
	s, err := Parse(`{"type":"long","logicalType":"timestamp-millis"}`)
	if err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)
	encoded, err := s.EncodeJSON(ts)
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	want := strconv.FormatInt(ts.UnixMilli(), 10)
	if string(encoded) != want {
		t.Errorf("got %s, want %s", encoded, want)
	}
}

func TestEncodeJSONReflectErrors(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		value  any
	}{
		{"bool wrong", `"boolean"`, 42},
		{"int wrong", `"int"`, "nope"},
		{"long wrong", `"long"`, "nope"},
		{"float wrong", `"float"`, "nope"},
		{"double wrong", `"double"`, "nope"},
		{"string wrong", `"string"`, 42},
		{"bytes wrong", `"bytes"`, 42},
		{"fixed wrong", `{"type":"fixed","name":"F","size":2}`, 42},
		{"enum wrong", `{"type":"enum","name":"E","symbols":["A"]}`, 42},
		{"array wrong", `{"type":"array","items":"int"}`, 42},
		{"map wrong", `{"type":"map","values":"int"}`, 42},
		{"record wrong", `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`, 42},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.EncodeJSON(tt.value); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestEncodeJSONTimestampVariants(t *testing.T) {
	ts := time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		schema string
		want   int64
	}{
		{"micros", `{"type":"long","logicalType":"timestamp-micros"}`, ts.UnixMicro()},
		{"nanos", `{"type":"long","logicalType":"timestamp-nanos"}`, ts.Unix()*1e9 + int64(ts.Nanosecond())},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := s.EncodeJSON(ts)
			if err != nil {
				t.Fatalf("EncodeJSON: %v", err)
			}
			if string(encoded) != strconv.FormatInt(tt.want, 10) {
				t.Errorf("got %s, want %d", encoded, tt.want)
			}
		})
	}
}

func TestEncodeJSONUintValues(t *testing.T) {
	s, _ := Parse(`"int"`)
	encoded, err := s.EncodeJSON(uint16(42))
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != "42" {
		t.Errorf("got %s, want 42", encoded)
	}

	s2, _ := Parse(`"long"`)
	encoded, err = s2.EncodeJSON(uint32(100))
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != "100" {
		t.Errorf("got %s, want 100", encoded)
	}
}

func TestEncodeJSONFixedAsSlice(t *testing.T) {
	s, err := Parse(`{"type":"fixed","name":"F","size":3}`)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := s.EncodeJSON([]byte{1, 2, 3})
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	if string(encoded) != `"\u0001\u0002\u0003"` {
		t.Errorf("got %s", encoded)
	}
}

func TestEncodeJSONMissingMapKey(t *testing.T) {
	s, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int","default":0},
		{"name":"b","type":"string"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	// "a" is missing from the map — should encode as null/default.
	encoded, err := s.EncodeJSON(map[string]any{"b": "hello"})
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	var parsed map[string]any
	json.Unmarshal(encoded, &parsed)
	if parsed["b"] != "hello" {
		t.Errorf("b: got %v", parsed["b"])
	}
}

func TestEncodeJSONNilInUnion(t *testing.T) {
	type R struct {
		V *string `avro:"v"`
	}
	s, err := Parse(`{"type":"record","name":"R","fields":[{"name":"v","type":["null","string"]}]}`)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := s.EncodeJSON(&R{V: nil})
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	var parsed map[string]any
	json.Unmarshal(encoded, &parsed)
	if parsed["v"] != nil {
		t.Errorf("v: got %v, want null", parsed["v"])
	}
}

func TestAvroJSONBinaryRoundTrip(t *testing.T) {
	// Encode to Avro JSON, decode to any, encode to binary, decode to any.
	// Verify the values match.
	schema := `{
		"type":"record","name":"Event",
		"fields":[
			{"name":"id","type":"string"},
			{"name":"ts","type":"long"},
			{"name":"data","type":"bytes"},
			{"name":"status","type":{"type":"enum","name":"Status","symbols":["A","B"]}},
			{"name":"extra","type":["null","string","int"]}
		]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}

	original := map[string]any{
		"id":     "abc",
		"ts":     int64(1000),
		"data":   []byte{0x01, 0x02},
		"status": "A",
		"extra":  "hello",
	}

	// Path 1: binary encode → binary decode
	binary, err := s.Encode(original)
	if err != nil {
		t.Fatal(err)
	}
	var fromBinary any
	if _, err := s.Decode(binary, &fromBinary); err != nil {
		t.Fatal(err)
	}

	// Path 2: avro JSON encode → avro JSON decode
	jsonBytes, err := s.EncodeJSON(original)
	if err != nil {
		t.Fatal(err)
	}
	var fromJSON any
	if err := s.DecodeJSON(jsonBytes, &fromJSON); err != nil {
		t.Fatal(err)
	}

	// Both paths should produce the same result.
	mb := fromBinary.(map[string]any)
	mj := fromJSON.(map[string]any)
	if mb["id"] != mj["id"] {
		t.Errorf("id mismatch: binary=%v json=%v", mb["id"], mj["id"])
	}
	if mb["ts"] != mj["ts"] {
		t.Errorf("ts mismatch: binary=%v json=%v", mb["ts"], mj["ts"])
	}
	if mb["status"] != mj["status"] {
		t.Errorf("status mismatch: binary=%v json=%v", mb["status"], mj["status"])
	}
	if mb["extra"] != mj["extra"] {
		t.Errorf("extra mismatch: binary=%v json=%v", mb["extra"], mj["extra"])
	}
}

func TestAvroJSONStructRoundTrip(t *testing.T) {
	type Record struct {
		Name  string  `avro:"name"`
		Age   int32   `avro:"age"`
		Email *string `avro:"email"`
	}
	s, err := Parse(`{"type":"record","name":"Record","fields":[
		{"name":"name","type":"string"},
		{"name":"age","type":"int"},
		{"name":"email","type":["null","string"]}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	email := "a@b.com"
	original := Record{Name: "Alice", Age: 30, Email: &email}

	// Struct → Avro JSON → struct
	jsonBytes, err := s.EncodeJSON(&original)
	if err != nil {
		t.Fatal(err)
	}
	var got Record
	if err := s.DecodeJSON(jsonBytes, &got); err != nil {
		t.Fatal(err)
	}
	if got.Name != original.Name || got.Age != original.Age {
		t.Errorf("got %+v, want %+v", got, original)
	}
	if got.Email == nil || *got.Email != *original.Email {
		t.Errorf("email: got %v, want %v", got.Email, original.Email)
	}
}

func TestAvroJSONUnionArrayNilRoundTrip(t *testing.T) {
	// Array of nullable unions with nil elements.
	s, err := Parse(`{"type":"array","items":["null","string"]}`)
	if err != nil {
		t.Fatal(err)
	}
	original := []any{nil, "hello", nil, "world"}
	jsonBytes, err := s.EncodeJSON(original)
	if err != nil {
		t.Fatal(err)
	}
	var got any
	if err := s.DecodeJSON(jsonBytes, &got); err != nil {
		t.Fatal(err)
	}
	arr := got.([]any)
	if len(arr) != 4 {
		t.Fatalf("expected 4 elements, got %d", len(arr))
	}
	if arr[0] != nil || arr[1] != "hello" || arr[2] != nil || arr[3] != "world" {
		t.Errorf("got %v", arr)
	}
}

func TestDecodeJSONIntOverflow(t *testing.T) {
	s, _ := Parse(`"int"`)
	// 3 billion exceeds int32 max.
	var got any
	err := s.DecodeJSON([]byte(`3000000000`), &got)
	if err == nil {
		t.Fatal("expected error for int32 overflow")
	}
}

func TestDecodeJSONBytesHighUnicode(t *testing.T) {
	s, _ := Parse(`"bytes"`)
	// \u0100 exceeds byte range.
	var got any
	err := s.DecodeJSON([]byte(`"\u0100"`), &got)
	if err == nil {
		t.Fatal("expected error for code point > 255")
	}
}

func TestSchemaForAnonymousStruct(t *testing.T) {
	type Outer struct {
		Inner struct{ X int } `avro:"inner"`
	}
	_, err := SchemaFor[Outer]()
	if err == nil {
		t.Fatal("expected error for anonymous struct field")
	}
}

func TestEncodeJSONNil(t *testing.T) {
	s, _ := Parse(`"null"`)
	encoded, err := s.EncodeJSON(nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != "null" {
		t.Errorf("got %s, want null", encoded)
	}
}

func TestDecodeJSONInvalidJSON(t *testing.T) {
	s, err := Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	var v any
	if err := s.DecodeJSON([]byte(`{not json`), &v); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestEncodeJSONLinkedinFloats(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		value  any
		want   string
	}{
		{"float NaN", `"float"`, float32(math.Float32frombits(0x7fc00000)), `null`},
		{"float +Inf", `"float"`, float32(math.Inf(1)), `1e999`},
		{"float -Inf", `"float"`, float32(math.Inf(-1)), `-1e999`},
		{"double NaN", `"double"`, math.NaN(), `null`},
		{"double +Inf", `"double"`, math.Inf(1), `1e999`},
		{"double -Inf", `"double"`, math.Inf(-1), `-1e999`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatal(err)
			}
			got, err := s.EncodeJSON(tt.value, LinkedinFloats())
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tt.want {
				t.Errorf("got %s, want %s", got, tt.want)
			}
		})
	}
}

func TestEncodeJSONTaggedUnions(t *testing.T) {
	s, err := Parse(`["null","string","int"]`)
	if err != nil {
		t.Fatal(err)
	}
	// Tagged: should wrap.
	got, err := s.EncodeJSON("hello", TaggedUnions())
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"string":"hello"}` {
		t.Errorf("tagged: got %s", got)
	}
	// Bare (default): should not wrap.
	got, err = s.EncodeJSON("hello")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `"hello"` {
		t.Errorf("bare: got %s", got)
	}
}

func TestDecodeTaggedUnions(t *testing.T) {
	schema := `{"type":"record","name":"R","fields":[
		{"name":"v","type":["null","string","int"]}
	]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	bin, err := s.Encode(map[string]any{"v": "hello"})
	if err != nil {
		t.Fatal(err)
	}

	// Without TaggedUnions: bare.
	var bare any
	if _, err := s.Decode(bin, &bare); err != nil {
		t.Fatal(err)
	}
	m := bare.(map[string]any)
	if m["v"] != "hello" {
		t.Errorf("bare: got %v", m["v"])
	}

	// With TaggedUnions: wrapped.
	var tagged any
	if _, err := s.Decode(bin, &tagged, TaggedUnions()); err != nil {
		t.Fatal(err)
	}
	m = tagged.(map[string]any)
	wrapper, ok := m["v"].(map[string]any)
	if !ok {
		t.Fatalf("tagged: expected map wrapper, got %T: %v", m["v"], m["v"])
	}
	if wrapper["string"] != "hello" {
		t.Errorf("tagged: got %v", wrapper)
	}
}

func TestDecodeTaggedUnionsComplex(t *testing.T) {
	schema := `{"type":"record","name":"R","fields":[
		{"name":"u_bool","type":["null","boolean"]},
		{"name":"u_int","type":["null","int"]},
		{"name":"u_long","type":["null","long"]},
		{"name":"u_float","type":["null","float"]},
		{"name":"u_double","type":["null","double"]},
		{"name":"u_string","type":["null","string"]},
		{"name":"u_bytes","type":["null","bytes"]},
		{"name":"u_null","type":["null","string"]},
		{"name":"arr","type":{"type":"array","items":["null","string"]}},
		{"name":"m","type":{"type":"map","values":["null","int"]}}
	]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	input := map[string]any{
		"u_bool":   true,
		"u_int":    int32(42),
		"u_long":   int64(100),
		"u_float":  float32(1.5),
		"u_double": float64(3.14),
		"u_string": "hello",
		"u_bytes":  []byte{0x01, 0x02},
		"u_null":   nil,
		"arr":      []any{nil, "a"},
		"m":        map[string]any{"k": int32(1)},
	}
	bin, err := s.Encode(input)
	if err != nil {
		t.Fatal(err)
	}
	var got any
	if _, err := s.Decode(bin, &got, TaggedUnions()); err != nil {
		t.Fatal(err)
	}
	m := got.(map[string]any)

	// Check each union is wrapped.
	check := func(field, branch string) {
		t.Helper()
		wrapper, ok := m[field].(map[string]any)
		if !ok {
			if m[field] == nil {
				return // null union values stay nil
			}
			t.Errorf("%s: expected map wrapper, got %T: %v", field, m[field], m[field])
			return
		}
		if _, ok := wrapper[branch]; !ok {
			t.Errorf("%s: expected branch %q, got keys %v", field, branch, wrapper)
		}
	}
	check("u_bool", "boolean")
	check("u_int", "int")
	check("u_long", "long")
	check("u_float", "float")
	check("u_double", "double")
	check("u_string", "string")
	check("u_bytes", "bytes")
	if m["u_null"] != nil {
		t.Errorf("u_null: expected nil, got %v", m["u_null"])
	}

	// Array items should be wrapped.
	arr := m["arr"].([]any)
	if arr[0] != nil {
		t.Errorf("arr[0]: expected nil, got %v", arr[0])
	}
	arrItem, ok := arr[1].(map[string]any)
	if !ok {
		t.Fatalf("arr[1]: expected map, got %T", arr[1])
	}
	if arrItem["string"] != "hello" && arrItem["string"] != "a" {
		t.Errorf("arr[1]: got %v", arrItem)
	}

	// Map values should be wrapped.
	mv := m["m"].(map[string]any)
	kv, ok := mv["k"].(map[string]any)
	if !ok {
		t.Fatalf("m[k]: expected map, got %T", mv["k"])
	}
	if _, ok := kv["int"]; !ok {
		t.Errorf("m[k]: expected int branch, got %v", kv)
	}
}

func TestDecodeTaggedUnionsNullAtRoot(t *testing.T) {
	s, err := Parse(`["null","string"]`)
	if err != nil {
		t.Fatal(err)
	}
	bin, err := s.Encode(nil)
	if err != nil {
		t.Fatal(err)
	}
	var got any
	if _, err := s.Decode(bin, &got, TaggedUnions()); err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestDecodeTaggedUnionsWithLogicalNames(t *testing.T) {
	schema := `{"type":"record","name":"R","fields":[
		{"name":"ts","type":["null",{"type":"long","logicalType":"timestamp-millis"}]}
	]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	now := time.UnixMilli(1687221496000).UTC()
	bin, err := s.Encode(map[string]any{"ts": now})
	if err != nil {
		t.Fatal(err)
	}

	// TaggedUnions only: branch name is "long".
	var std any
	if _, err := s.Decode(bin, &std, TaggedUnions()); err != nil {
		t.Fatal(err)
	}
	m := std.(map[string]any)
	wrapper := m["ts"].(map[string]any)
	if _, ok := wrapper["long"]; !ok {
		t.Errorf("expected 'long' key, got %v", wrapper)
	}

	// TaggedUnions + TagLogicalTypes: branch name is "long.timestamp-millis".
	var logical any
	if _, err := s.Decode(bin, &logical, TaggedUnions(), TagLogicalTypes()); err != nil {
		t.Fatal(err)
	}
	m = logical.(map[string]any)
	wrapper = m["ts"].(map[string]any)
	if _, ok := wrapper["long.timestamp-millis"]; !ok {
		t.Errorf("expected 'long.timestamp-millis' key, got %v", wrapper)
	}
}

func TestEncodeJSONTaggedUnionsWithLogicalNames(t *testing.T) {
	s, err := Parse(`["null",{"type":"long","logicalType":"timestamp-millis"}]`)
	if err != nil {
		t.Fatal(err)
	}
	now := time.UnixMilli(1687221496000).UTC()

	// Without TagLogicalTypes: "long".
	got, err := s.EncodeJSON(now, TaggedUnions())
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"long":1687221496000}` {
		t.Errorf("got %s", got)
	}

	// With TagLogicalTypes: "long.timestamp-millis".
	got, err = s.EncodeJSON(now, TaggedUnions(), TagLogicalTypes())
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"long.timestamp-millis":1687221496000}` {
		t.Errorf("got %s", got)
	}
}

func TestDecodeJSONTaggedUnions(t *testing.T) {
	s, err := Parse(`["null","string"]`)
	if err != nil {
		t.Fatal(err)
	}
	var bare any
	if err := s.DecodeJSON([]byte(`"hello"`), &bare); err != nil {
		t.Fatal(err)
	}
	if bare != "hello" {
		t.Errorf("bare: got %v", bare)
	}

	var tagged any
	if err := s.DecodeJSON([]byte(`"hello"`), &tagged, TaggedUnions()); err != nil {
		t.Fatal(err)
	}
	wrapper, ok := tagged.(map[string]any)
	if !ok {
		t.Fatalf("tagged: expected map, got %T: %v", tagged, tagged)
	}
	if wrapper["string"] != "hello" {
		t.Errorf("tagged: got %v", wrapper)
	}
}

func TestDecodeJSONNaNInfRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		input  string
	}{
		{"float NaN string", `"float"`, `"NaN"`},
		{"float Inf string", `"float"`, `"Infinity"`},
		{"float -Inf string", `"float"`, `"-Infinity"`},
		{"float INF string", `"float"`, `"INF"`},
		{"float -INF string", `"float"`, `"-INF"`},
		{"double NaN string", `"double"`, `"NaN"`},
		{"double nan lowercase", `"double"`, `"nan"`},
		{"double Inf string", `"double"`, `"Infinity"`},
		{"double -Inf string", `"double"`, `"-Infinity"`},
		{"float null → NaN", `"float"`, `null`},
		{"double null → NaN", `"double"`, `null`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatal(err)
			}
			var got any
			if err := s.DecodeJSON([]byte(tt.input), &got); err != nil {
				t.Fatalf("DecodeJSON: %v", err)
			}
			switch v := got.(type) {
			case float32:
				if tt.input == `null` || tt.input == `"NaN"` || tt.input == `"nan"` {
					if !math.IsNaN(float64(v)) {
						t.Errorf("expected NaN, got %v", v)
					}
				} else if !math.IsInf(float64(v), 0) {
					t.Errorf("expected Inf, got %v", v)
				}
			case float64:
				if tt.input == `null` || tt.input == `"NaN"` || tt.input == `"nan"` {
					if !math.IsNaN(v) {
						t.Errorf("expected NaN, got %v", v)
					}
				} else if !math.IsInf(v, 0) {
					t.Errorf("expected Inf, got %v", v)
				}
			default:
				t.Fatalf("unexpected type %T", got)
			}
		})
	}
}

func TestDecodeJSONBadFloatString(t *testing.T) {
	s, err := Parse(`"float"`)
	if err != nil {
		t.Fatal(err)
	}
	var v any
	if err := s.DecodeJSON([]byte(`"bogus"`), &v); err == nil {
		t.Fatal("expected error for unknown float string")
	}
}

func TestDecodeJSONBadDoubleString(t *testing.T) {
	s, err := Parse(`"double"`)
	if err != nil {
		t.Fatal(err)
	}
	var v any
	if err := s.DecodeJSON([]byte(`"bogus"`), &v); err == nil {
		t.Fatal("expected error for unknown double string")
	}
}

func TestEncodeJSONBareUnionRecord(t *testing.T) {
	schema := `{
		"type":"record","name":"R",
		"fields":[{"name":"v","type":["null",{"type":"record","name":"Inner","fields":[
			{"name":"x","type":"int"}
		]}]}]
	}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	data := map[string]any{"v": map[string]any{"x": int32(42)}}
	// Bare: record without type wrapper.
	bare, err := s.EncodeJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	// Decode back from bare.
	var got any
	if err := s.DecodeJSON(bare, &got); err != nil {
		t.Fatalf("DecodeJSON bare: %v", err)
	}
	m := got.(map[string]any)
	inner := m["v"].(map[string]any)
	if inner["x"] != int32(42) {
		t.Errorf("x: got %v", inner["x"])
	}
}

func TestEncodeJSONRecordMissingRequiredField(t *testing.T) {
	schema := `{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}
	]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	// Missing required field "b".
	_, err = s.EncodeJSON(map[string]any{"a": int32(1)})
	if err == nil {
		t.Fatal("expected error for missing required field")
	}
}

func TestEncodeJSONRecordOptionalField(t *testing.T) {
	schema := `{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string","default":"hi"}
	]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	// Missing field "b" with default — should succeed with the default value.
	got, err := s.EncodeJSON(map[string]any{"a": int32(1)})
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	if string(got) != `{"a":1,"b":"hi"}` {
		t.Errorf("got %s", got)
	}
}

func TestEncodeJSONBytesFromString(t *testing.T) {
	s, err := Parse(`"bytes"`)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.EncodeJSON("hello")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `"hello"` {
		t.Errorf("got %s", got)
	}
}

func TestBareUnionMultiRecordRoundTrip(t *testing.T) {
	schema := `["null",
		{"type":"record","name":"Foo","fields":[{"name":"x","type":"int"}]},
		{"type":"record","name":"Bar","fields":[{"name":"y","type":"string"}]}
	]`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}

	// Tagged round-trip should work.
	var v1 any
	if err := s.DecodeJSON([]byte(`{"Bar":{"y":"hello"}}`), &v1); err != nil {
		t.Fatalf("tagged DecodeJSON: %v", err)
	}

	// Bare round-trip: encode Bar to binary, decode, EncodeJSON bare, DecodeJSON back.
	bar := map[string]any{"y": "hello"}
	bin, err := s.Encode(bar)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var native any
	if _, err := s.Decode(bin, &native); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	jb, err := s.EncodeJSON(native)
	if err != nil {
		t.Fatalf("EncodeJSON bare: %v", err)
	}
	t.Logf("bare JSON: %s", jb)
	var rt any
	if err := s.DecodeJSON(jb, &rt); err != nil {
		t.Fatalf("DecodeJSON bare: %v", err)
	}

	// Direct Encode of bare map should also work via branch matching.
	if _, err := s.Encode(map[string]any{"y": "hello"}); err != nil {
		t.Fatalf("direct Encode of Bar map: %v", err)
	}
}

func TestDecodeJSONBareUnionFallthrough(t *testing.T) {
	// Union ["null","string"] with input {"int":42} — not a valid branch.
	// Should still error even with bare matching.
	s, err := Parse(`["null","string"]`)
	if err != nil {
		t.Fatal(err)
	}
	var v any
	if err := s.DecodeJSON([]byte(`{"int":42}`), &v); err == nil {
		t.Fatal("expected error for unmatched bare union value")
	}
}

func TestDecodeJSONTaggedFloatNull(t *testing.T) {
	// {"float": null} in a ["null","float"] union — the null is inside
	// the float branch, which decodes as NaN (goavro convention).
	s, err := Parse(`["null","float"]`)
	if err != nil {
		t.Fatal(err)
	}
	var v any
	if err := s.DecodeJSON([]byte(`{"float":null}`), &v); err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	f, ok := v.(float32)
	if !ok {
		t.Fatalf("expected float32, got %T: %v", v, v)
	}
	if !math.IsNaN(float64(f)) {
		t.Fatalf("expected NaN, got %v", f)
	}
}

func TestSerStringJsonNumberInUnion(t *testing.T) {
	// json.Number in a ["null","string"] union — no numeric branch,
	// should error since string rejects json.Number.
	s, err := Parse(`["null","string"]`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Encode(json.Number("42"))
	if err == nil {
		t.Fatal("expected error: json.Number should not match string branch")
	}

	// json.Number in a ["null","int","string"] union — should match int.
	s2, err := Parse(`["null","int","string"]`)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s2.Encode(json.Number("42"))
	if err != nil {
		t.Fatalf("json.Number should match int branch: %v", err)
	}
	if len(b) == 0 {
		t.Fatal("expected non-empty output")
	}
}

func TestDecodeJSONBareUnionStringVsRecord(t *testing.T) {
	// Union ["null","string",record] — a map should match the record, not string.
	schema := `["null","string",{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}]`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	var v any
	if err := s.DecodeJSON([]byte(`{"x":42}`), &v); err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T: %v", v, v)
	}
	if m["x"] != int32(42) {
		t.Errorf("x: got %v", m["x"])
	}
}

func TestEncodeJSONFixedFromString(t *testing.T) {
	s, err := Parse(`{"type":"fixed","name":"F","size":5}`)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.EncodeJSON("hello")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `"hello"` {
		t.Errorf("got %s", got)
	}
}

func TestEncodeJSONLogicalTypeRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		value  any
		want   string
	}{
		{"date", `{"type":"int","logicalType":"date"}`, time.Date(1977, 5, 12, 0, 0, 0, 0, time.UTC), "2688"},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`, time.Duration(35245000) * time.Millisecond, "35245000"},
		{"time-micros", `{"type":"long","logicalType":"time-micros"}`, time.Duration(20192000) * time.Microsecond, "20192000"},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, time.UnixMilli(1687221496000).UTC(), "1687221496000"},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`, time.UnixMicro(1687221496000000).UTC(), "1687221496000000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatal(err)
			}
			bin, err := s.Encode(tt.value)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			var native any
			if _, err := s.Decode(bin, &native); err != nil {
				t.Fatalf("Decode: %v", err)
			}
			got, err := s.EncodeJSON(native)
			if err != nil {
				t.Fatalf("EncodeJSON: %v", err)
			}
			if string(got) != tt.want {
				t.Errorf("got %s, want %s", got, tt.want)
			}
		})
	}
}

func TestEncodeJSONDurationRoundTrip(t *testing.T) {
	schema := `{"type":"fixed","name":"dur","size":12,"logicalType":"duration"}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	d := Duration{Months: 3, Days: 15, Milliseconds: 86400000}
	bin, err := s.Encode(d)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var native any
	if _, err := s.Decode(bin, &native); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got := native.(Duration); got != d {
		t.Fatalf("Decode: got %+v, want %+v", got, d)
	}
	j, err := s.EncodeJSON(native)
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	var rt any
	if err := s.DecodeJSON(j, &rt); err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	if got := rt.(Duration); got != d {
		t.Fatalf("round-trip: got %+v, want %+v", got, d)
	}
}

func TestEncodeJSONStructCacheSharing(t *testing.T) {
	// Verify that binary Encode and EncodeJSON share the typeFieldMapping
	// cache on serRecord: calling one warms the cache for the other.
	type R struct {
		Name string `avro:"name"`
		Age  int32  `avro:"age"`
	}
	schema := `{"type":"record","name":"R","fields":[
		{"name":"name","type":"string"},
		{"name":"age","type":"int"}
	]}`
	s, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	v := R{Name: "Alice", Age: 30}

	// Binary encode first — warms the cache.
	bin, err := s.Encode(&v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(bin) == 0 {
		t.Fatal("empty binary output")
	}

	// JSON encode second — should reuse the cached mapping.
	j, err := s.EncodeJSON(&v)
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	if string(j) != `{"name":"Alice","age":30}` {
		t.Errorf("got %s", j)
	}

	// Reverse order: JSON first, binary second, fresh schema.
	s2, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	j2, err := s2.EncodeJSON(&v)
	if err != nil {
		t.Fatalf("EncodeJSON first: %v", err)
	}
	if string(j2) != `{"name":"Alice","age":30}` {
		t.Errorf("got %s", j2)
	}
	bin2, err := s2.Encode(&v)
	if err != nil {
		t.Fatalf("Encode second: %v", err)
	}
	if len(bin2) == 0 {
		t.Fatal("empty binary output")
	}

	// Concurrent: both paths simultaneously.
	s3, err := Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	const goroutines = 8
	errs := make(chan error, goroutines*2)
	for range goroutines {
		go func() {
			_, err := s3.Encode(&v)
			errs <- err
		}()
		go func() {
			_, err := s3.EncodeJSON(&v)
			errs <- err
		}()
	}
	for range goroutines * 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

func TestEncodeJSONTimeAsDate(t *testing.T) {
	s, _ := Parse(`{"type":"int","logicalType":"date"}`)
	d := time.Date(2026, 3, 19, 0, 0, 0, 0, time.UTC)
	got, err := s.EncodeJSON(d)
	if err != nil {
		t.Fatal(err)
	}
	// days since epoch
	want := strconv.FormatInt(d.Unix()/86400, 10)
	if string(got) != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestEncodeJSONTimeAsTimeMillis(t *testing.T) {
	s, _ := Parse(`{"type":"int","logicalType":"time-millis"}`)
	// Duration input (from Decode).
	d := time.Duration(35245000) * time.Millisecond
	got, err := s.EncodeJSON(d)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "35245000" {
		t.Errorf("duration: got %s", got)
	}
	// time.Time input (manually constructed time-of-day).
	tod := time.Date(0, 1, 1, 9, 47, 25, 0, time.UTC)
	got, err = s.EncodeJSON(tod)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "35245000" {
		t.Errorf("time.Time: got %s", got)
	}
}

func TestEncodeJSONTimestampNanos(t *testing.T) {
	s, _ := Parse(`{"type":"long","logicalType":"timestamp-nanos"}`)
	now := time.Date(2026, 3, 19, 10, 0, 0, 123456789, time.UTC)
	got, err := s.EncodeJSON(now)
	if err != nil {
		t.Fatal(err)
	}
	want := strconv.FormatInt(now.UnixNano(), 10)
	if string(got) != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestFromAvroJSONIntTruncation(t *testing.T) {
	s, _ := Parse(`"int"`)
	var v any
	// Non-whole number for int should error.
	if err := s.DecodeJSON([]byte(`3.14`), &v); err == nil {
		t.Fatal("expected error for non-whole int")
	}
	// Overflow should error.
	if err := s.DecodeJSON([]byte(`3000000000`), &v); err == nil {
		t.Fatal("expected error for int32 overflow")
	}
}

func TestFromAvroJSONLongTruncation(t *testing.T) {
	s, _ := Parse(`"long"`)
	var v any
	if err := s.DecodeJSON([]byte(`1.5`), &v); err == nil {
		t.Fatal("expected error for non-whole long")
	}
}

func TestFromAvroJSONFloatTypeCheck(t *testing.T) {
	s, _ := Parse(`"float"`)
	var v any
	if err := s.DecodeJSON([]byte(`"not a number"`), &v); err == nil {
		t.Fatal("expected error for string as float")
	}
}

func TestFromAvroJSONDoubleTypeCheck(t *testing.T) {
	s, _ := Parse(`"double"`)
	var v any
	if err := s.DecodeJSON([]byte(`true`), &v); err == nil {
		t.Fatal("expected error for bool as double")
	}
}

func TestAppendJSONStringEscaping(t *testing.T) {
	s, _ := Parse(`"string"`)
	// Test control characters and special escapes.
	tests := []struct {
		in   string
		want string
	}{
		{`hello`, `"hello"`},
		{"a\"b", `"a\"b"`},
		{"a\\b", `"a\\b"`},
		{"a\nb", `"a\u000ab"`},
		{"a\x00b", `"a\u0000b"`},
	}
	for _, tt := range tests {
		got, err := s.EncodeJSON(tt.in)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != tt.want {
			t.Errorf("EncodeJSON(%q) = %s, want %s", tt.in, got, tt.want)
		}
	}
}
