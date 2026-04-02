package avro

import (
	"bytes"
	"encoding/json"
	"math"
	"math/big"
	"strconv"
	"testing"
	"time"
)

// TestEncodeFromJSONUnmarshal tests that data from json.Unmarshal (which
// produces float64 for all numbers) can be encoded for every schema type.
// This catches coercion gaps in the json.Unmarshal → Encode pipeline.
func TestEncodeFromJSONUnmarshal(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		json   string
	}{
		// Primitives.
		{"null", `"null"`, `null`},
		{"boolean", `"boolean"`, `true`},
		{"int", `"int"`, `42`},
		{"long", `"long"`, `100000`},
		{"float", `"float"`, `1.5`},
		{"double", `"double"`, `3.14`},
		{"string", `"string"`, `"hello"`},
		{"bytes from string", `"bytes"`, `"hello"`},

		// Arrays of primitives (exercises specialized array serializers).
		{"array of int", `{"type":"array","items":"int"}`, `[1,2,3]`},
		{"array of long", `{"type":"array","items":"long"}`, `[100,200]`},
		{"array of float", `{"type":"array","items":"float"}`, `[1.5,2.5]`},
		{"array of double", `{"type":"array","items":"double"}`, `[3.14]`},
		{"array of string", `{"type":"array","items":"string"}`, `["a","b"]`},
		{"array of boolean", `{"type":"array","items":"boolean"}`, `[true,false]`},

		// Maps of primitives (exercises specialized map serializers).
		{"map of int", `{"type":"map","values":"int"}`, `{"k":42}`},
		{"map of long", `{"type":"map","values":"long"}`, `{"k":100}`},
		{"map of float", `{"type":"map","values":"float"}`, `{"k":1.5}`},
		{"map of double", `{"type":"map","values":"double"}`, `{"k":3.14}`},
		{"map of string", `{"type":"map","values":"string"}`, `{"k":"v"}`},
		{"map of boolean", `{"type":"map","values":"boolean"}`, `{"k":true}`},

		// Unions.
		{"nullable string null", `["null","string"]`, `null`},
		{"nullable string value", `["null","string"]`, `"hello"`},
		{"nullable int null", `["null","int"]`, `null`},
		{"nullable int value", `["null","int"]`, `42`},

		// Enum.
		{"enum", `{"type":"enum","name":"Color","symbols":["RED","GREEN"]}`, `"RED"`},

		// Records.
		{"simple record", `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`, `{"a":1,"b":"hello"}`},
		{"record with nullable", `{"type":"record","name":"R","fields":[{"name":"a","type":"string"},{"name":"b","type":["null","int"]}]}`, `{"a":"x","b":42}`},
		{"record with nullable null", `{"type":"record","name":"R","fields":[{"name":"a","type":"string"},{"name":"b","type":["null","int"]}]}`, `{"a":"x","b":null}`},
		{"record with default", `{"type":"record","name":"R","fields":[{"name":"a","type":"int","default":0},{"name":"b","type":"string"}]}`, `{"b":"hello"}`},
		{"record with bytes field", `{"type":"record","name":"R","fields":[{"name":"data","type":"bytes"}]}`, `{"data":"hello"}`},

		// Nested records.
		{"nested record", `{"type":"record","name":"O","fields":[{"name":"inner","type":{"type":"record","name":"I","fields":[{"name":"x","type":"int"}]}}]}`, `{"inner":{"x":1}}`},

		// Logical types.
		{"timestamp-millis from number", `{"type":"long","logicalType":"timestamp-millis"}`, `1742385600000`},
		{"timestamp-millis from string", `{"type":"long","logicalType":"timestamp-millis"}`, `"2026-03-19T10:00:00Z"`},
		{"timestamp-micros from string", `{"type":"long","logicalType":"timestamp-micros"}`, `"2026-03-19T10:00:00Z"`},
		{"timestamp-nanos from string", `{"type":"long","logicalType":"timestamp-nanos"}`, `"2026-03-19T10:00:00Z"`},
		{"date from number", `{"type":"int","logicalType":"date"}`, `19435`},
		{"date from RFC3339", `{"type":"int","logicalType":"date"}`, `"2026-03-19T00:00:00Z"`},
		{"date from YYYY-MM-DD", `{"type":"int","logicalType":"date"}`, `"2026-03-19"`},

		// Arrays/maps inside records.
		{"record with array", `{"type":"record","name":"R","fields":[{"name":"tags","type":{"type":"array","items":"string"}}]}`, `{"tags":["a","b"]}`},
		{"record with map", `{"type":"record","name":"R","fields":[{"name":"meta","type":{"type":"map","values":"int"}}]}`, `{"meta":{"k":1}}`},
		{"record with array of int", `{"type":"record","name":"R","fields":[{"name":"nums","type":{"type":"array","items":"int"}}]}`, `{"nums":[1,2,3]}`},
		{"record with map of long", `{"type":"record","name":"R","fields":[{"name":"counts","type":{"type":"map","values":"long"}}]}`, `{"counts":{"a":100}}`},

		// Union inside array.
		{"array of nullable string", `{"type":"array","items":["null","string"]}`, `["hello",null,"world"]`},

		// Deeply nested.
		{"deep nesting", `{
			"type":"record","name":"L1","fields":[
				{"name":"l2","type":{"type":"record","name":"L2","fields":[
					{"name":"l3","type":{"type":"record","name":"L3","fields":[
						{"name":"val","type":"int"}
					]}}
				]}}
			]}`, `{"l2":{"l3":{"val":42}}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			var native any
			if err := json.Unmarshal([]byte(tt.json), &native); err != nil {
				t.Fatalf("json.Unmarshal: %v", err)
			}
			binary, err := s.Encode(native)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			// Verify it decodes back.
			var decoded any
			if _, err := s.Decode(binary, &decoded); err != nil {
				t.Fatalf("Decode: %v", err)
			}
		})
	}
}

// TestEncodeStringBytesCoercionInCollections tests []byte → string and
// string → bytes coercion in arrays and maps.
func TestEncodeStringBytesCoercionInCollections(t *testing.T) {
	// []byte in array of strings.
	t.Run("array of string from bytes", func(t *testing.T) {
		s, _ := Parse(`{"type":"array","items":"string"}`)
		data := []any{[]byte("hello"), []byte("world")}
		binary, err := s.Encode(data)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		var decoded any
		s.Decode(binary, &decoded)
		arr := decoded.([]any)
		if arr[0] != "hello" || arr[1] != "world" {
			t.Errorf("got %v", arr)
		}
	})

	// []byte in map of strings.
	t.Run("map of string from bytes", func(t *testing.T) {
		s, _ := Parse(`{"type":"map","values":"string"}`)
		data := map[string]any{"k": []byte("hello")}
		binary, err := s.Encode(data)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		var decoded any
		s.Decode(binary, &decoded)
		m := decoded.(map[string]any)
		if m["k"] != "hello" {
			t.Errorf("got %v", m["k"])
		}
	})

	// string in array of bytes.
	t.Run("array of bytes from string", func(t *testing.T) {
		s, _ := Parse(`{"type":"array","items":"bytes"}`)
		data := []any{"hello", "world"}
		binary, err := s.Encode(data)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		var decoded any
		s.Decode(binary, &decoded)
		arr := decoded.([]any)
		if string(arr[0].([]byte)) != "hello" {
			t.Errorf("got %v", arr)
		}
	})
}

// TestEncodeJSONEdgeCases covers paths in appendAvroJSON and DecodeJSON
// that normal integration tests don't reach.
func TestEncodeJSONEdgeCases(t *testing.T) {
	// Null standalone.
	t.Run("encode null", func(t *testing.T) {
		s, _ := Parse(`"null"`)
		b, err := s.EncodeJSON(nil)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != "null" {
			t.Errorf("got %s", b)
		}
	})

	// Nil pointer in union.
	t.Run("nil pointer union", func(t *testing.T) {
		s, _ := Parse(`["null","string"]`)
		var p *string
		b, err := s.EncodeJSON(p)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != "null" {
			t.Errorf("got %s", b)
		}
	})

	// DecodeJSON with null union.
	t.Run("decode null union", func(t *testing.T) {
		s, _ := Parse(`["null","string"]`)
		var got any
		if err := s.DecodeJSON([]byte(`null`), &got); err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Errorf("got %v", got)
		}
	})

	// DecodeJSON float/double passthrough.
	t.Run("decode float", func(t *testing.T) {
		s, _ := Parse(`"float"`)
		var got any
		if err := s.DecodeJSON([]byte(`1.5`), &got); err != nil {
			t.Fatal(err)
		}
		if got != float32(1.5) {
			t.Errorf("got %v (%T)", got, got)
		}
	})

	t.Run("decode double", func(t *testing.T) {
		s, _ := Parse(`"double"`)
		var got any
		if err := s.DecodeJSON([]byte(`3.14`), &got); err != nil {
			t.Fatal(err)
		}
		if got != 3.14 {
			t.Errorf("got %v (%T)", got, got)
		}
	})

	// DecodeJSON long overflow.
	t.Run("decode long overflow", func(t *testing.T) {
		s, _ := Parse(`"long"`)
		var got any
		err := s.DecodeJSON([]byte(`1e25`), &got)
		if err == nil {
			t.Fatal("expected overflow error")
		}
	})

	// Uint values for int field via EncodeJSON.
	t.Run("encode uint16 as int", func(t *testing.T) {
		s, _ := Parse(`"int"`)
		b, err := s.EncodeJSON(uint16(42))
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != "42" {
			t.Errorf("got %s", b)
		}
	})

	t.Run("encode uint32 as long", func(t *testing.T) {
		s, _ := Parse(`"long"`)
		b, err := s.EncodeJSON(uint32(100))
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != "100" {
			t.Errorf("got %s", b)
		}
	})
}

// TestEncodeJSONCoercionPaths exercises type coercion paths in EncodeJSON
// that aren't covered by the json.Unmarshal tests (which always produce
// float64, not float32/uint/etc).
func TestEncodeJSONCoercionPaths(t *testing.T) {
	// float32 for int field.
	t.Run("float32 to int", func(t *testing.T) {
		s, _ := Parse(`"int"`)
		b, err := s.EncodeJSON(float32(42))
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != "42" {
			t.Errorf("got %s", b)
		}
	})

	// float32 for long field.
	t.Run("float32 to long", func(t *testing.T) {
		s, _ := Parse(`"long"`)
		b, err := s.EncodeJSON(float32(100))
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != "100" {
			t.Errorf("got %s", b)
		}
	})

	// local-timestamp-millis (hits the default case in timestamp switch).
	t.Run("local-timestamp-millis", func(t *testing.T) {
		s, _ := Parse(`{"type":"long","logicalType":"local-timestamp-millis"}`)
		ts := time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)
		b, err := s.EncodeJSON(ts)
		if err != nil {
			t.Fatal(err)
		}
		want := strconv.FormatInt(ts.UnixMilli(), 10)
		if string(b) != want {
			t.Errorf("got %s, want %s", b, want)
		}
	})

	// nil for non-nullable type.
	t.Run("nil for int", func(t *testing.T) {
		s, _ := Parse(`"int"`)
		_, err := s.EncodeJSON(nil)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	// null schema.
	t.Run("null schema", func(t *testing.T) {
		s, _ := Parse(`"null"`)
		b, err := s.EncodeJSON(nil)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != "null" {
			t.Errorf("got %s", b)
		}
	})

	// json.Number for bytes-backed decimal (from Decode → EncodeJSON round-trip).
	t.Run("decimal round-trip", func(t *testing.T) {
		s, _ := Parse(`{"type":"record","name":"R","fields":[
			{"name":"v","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}}
		]}`)
		r := new(big.Rat).SetFrac64(314, 100)
		binary, _ := s.Encode(map[string]any{"v": r})
		var decoded any
		s.Decode(binary, &decoded)
		jb, err := s.EncodeJSON(decoded)
		if err != nil {
			t.Fatal(err)
		}
		if !json.Valid(jb) {
			t.Fatalf("invalid JSON: %s", jb)
		}
	})

	// []byte for string field — encode and decode round-trips.
	t.Run("bytes to string to bytes", func(t *testing.T) {
		s, _ := Parse(`{"type":"record","name":"R","fields":[{"name":"s","type":"string"}]}`)
		type R struct {
			S []byte `avro:"s"`
		}
		binary, err := s.Encode(&R{S: []byte("hello")})
		if err != nil {
			t.Fatal(err)
		}
		var decoded R
		s.Decode(binary, &decoded)
		if string(decoded.S) != "hello" {
			t.Errorf("got %s", decoded.S)
		}
	})

	// string for bytes field — encode and decode round-trips.
	t.Run("string to bytes to string", func(t *testing.T) {
		s, _ := Parse(`{"type":"record","name":"R","fields":[{"name":"b","type":"bytes"}]}`)
		type R struct {
			B string `avro:"b"`
		}
		binary, err := s.Encode(&R{B: "hello"})
		if err != nil {
			t.Fatal(err)
		}
		var decoded R
		s.Decode(binary, &decoded)
		if decoded.B != "hello" {
			t.Errorf("got %s", decoded.B)
		}
	})
}

// TestDecodeStringIntoBytes tests deserString → []byte directly.
func TestDecodeStringIntoBytes(t *testing.T) {
	s, _ := Parse(`"string"`)
	binary, _ := s.Encode("hello")
	var got []byte
	if _, err := s.Decode(binary, &got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello" {
		t.Errorf("got %s", got)
	}
}

// TestDecodeBytesIntoString tests deserBytes → string directly.
func TestDecodeBytesIntoString(t *testing.T) {
	s, _ := Parse(`"bytes"`)
	binary, _ := s.Encode([]byte("hello"))
	var got string
	if _, err := s.Decode(binary, &got); err != nil {
		t.Fatal(err)
	}
	if got != "hello" {
		t.Errorf("got %s", got)
	}
}

func TestDecodeBytesIntoWrongType(t *testing.T) {
	s, _ := Parse(`"bytes"`)
	binary, _ := s.Encode([]byte{1, 2})
	var got int
	_, err := s.Decode(binary, &got)
	if err == nil {
		t.Fatal("expected error decoding bytes into int")
	}
}

func TestEncodeJSONNullSchema(t *testing.T) {
	s, _ := Parse(`"null"`)
	// Non-nil value with null schema — should still write "null".
	b, err := s.EncodeJSON("ignored")
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "null" {
		t.Errorf("got %s", b)
	}
}

func TestEncodeJSONDecimalFixedRoundTrip(t *testing.T) {
	s, _ := Parse(`{"type":"record","name":"R","fields":[
		{"name":"v","type":{"type":"fixed","name":"d","size":8,"logicalType":"decimal","precision":10,"scale":2}}
	]}`)
	r := new(big.Rat).SetFrac64(314, 100)
	binary, _ := s.Encode(map[string]any{"v": r})
	var decoded any
	s.Decode(binary, &decoded)
	// decoded has json.Number for the decimal — EncodeJSON should handle it.
	jb, err := s.EncodeJSON(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(jb) {
		t.Fatalf("invalid JSON: %s", jb)
	}
}

func TestEncodeJSONNilPointerUnion(t *testing.T) {
	type R struct {
		V *string `avro:"v"`
	}
	s, _ := Parse(`{"type":"record","name":"R","fields":[{"name":"v","type":["null","string"]}]}`)
	b, err := s.EncodeJSON(&R{V: nil})
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(b) {
		t.Fatalf("invalid JSON: %s", b)
	}
}

func TestEncodeJSONStructFieldError(t *testing.T) {
	type R struct {
		A bool `avro:"a"` // schema says int — will fail
	}
	s, _ := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	_, err := s.EncodeJSON(&R{A: true})
	if err == nil {
		t.Fatal("expected error encoding bool as int in struct")
	}
}

func TestEncodeJSONNilPointerTopLevel(t *testing.T) {
	// Nil *string directly for a union — hits appendAvroJSONUnion with nil pointer.
	s, _ := Parse(`["null","string"]`)
	var p *string
	b, err := s.EncodeJSON(p)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "null" {
		t.Errorf("got %s", b)
	}
}

func TestEncodeJSONNilInterfaceInUnion(t *testing.T) {
	// Map with nil interface value for a union field — hits the IsNil check
	// in appendAvroJSONUnion.
	s, _ := Parse(`{"type":"record","name":"R","fields":[{"name":"v","type":["null","string"]}]}`)
	data := map[string]any{"v": nil}
	b, err := s.EncodeJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	var parsed map[string]any
	json.Unmarshal(b, &parsed)
	if parsed["v"] != nil {
		t.Errorf("expected null, got %v", parsed["v"])
	}
}

func TestEncodeJSONStructMappingError(t *testing.T) {
	// Struct missing a required field — hits typeFieldMapping error in record encoder.
	type R struct {
		A int `avro:"a"`
	}
	s, _ := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`)
	_, err := s.EncodeJSON(&R{A: 1})
	if err == nil {
		t.Fatal("expected error for struct missing field b")
	}
}

func TestEncodeJSONNestedError(t *testing.T) {
	// Array with wrong element type triggers error propagation.
	s, _ := Parse(`{"type":"array","items":"int"}`)
	_, err := s.EncodeJSON([]any{true}) // bool can't encode as int
	if err == nil {
		t.Fatal("expected error")
	}

	// Map with wrong value type.
	s2, _ := Parse(`{"type":"map","values":"int"}`)
	_, err = s2.EncodeJSON(map[string]any{"k": true})
	if err == nil {
		t.Fatal("expected error")
	}

	// Record with wrong field type.
	s3, _ := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	_, err = s3.EncodeJSON(map[string]any{"a": true})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDecodeJSONNullStandalone(t *testing.T) {
	s, _ := Parse(`"null"`)
	var got any
	err := s.DecodeJSON([]byte(`null`), &got)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDecodeJSONNullUnionTyped(t *testing.T) {
	s, _ := Parse(`["null","string"]`)
	input := `null`
	var got *string
	err := s.DecodeJSON([]byte(input), &got)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestDecodeJSONArrayError(t *testing.T) {
	s, _ := Parse(`{"type":"array","items":"int"}`)
	// "hello" can't be an int.
	var got any
	err := s.DecodeJSON([]byte(`[1, "hello"]`), &got)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDeserFixedArrayNegativeBlock(t *testing.T) {
	s, err := Parse(`{"type":"array","items":"int"}`)
	if err != nil {
		t.Fatal(err)
	}
	// Craft binary with negative block count (indicates byte-size follows).
	var elems []byte
	elems = appendVarint(elems, 10)
	elems = appendVarint(elems, 20)
	elems = appendVarint(elems, 30)
	var data []byte
	data = appendVarlong(data, -3)                // negative count
	data = appendVarlong(data, int64(len(elems))) // byte size
	data = append(data, elems...)
	data = append(data, 0) // terminator

	var got [3]int32
	if _, err := s.Decode(data, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != [3]int32{10, 20, 30} {
		t.Errorf("got %v", got)
	}
}

func TestDeserFixedArrayTruncated(t *testing.T) {
	s, err := Parse(`{"type":"array","items":"int"}`)
	if err != nil {
		t.Fatal(err)
	}
	// Truncated data — readVarlong fails.
	var got [3]int32
	_, err = s.Decode([]byte{}, &got)
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestDeserFixedArrayNegBlockOverflow(t *testing.T) {
	s, _ := Parse(`{"type":"array","items":"int"}`)
	// MinInt64 zigzag-encoded: negating it still gives negative.
	data := []byte{0x01} // zigzag for -1... actually need MinInt64.
	// zigzag(MinInt64) = MaxUint64 which is 0xFF 0xFF ... 0xFF 0x01 (10 bytes)
	data = appendVarlong(nil, math.MinInt64)
	var got [1]int32
	_, err := s.Decode(data, &got)
	if err == nil {
		t.Fatal("expected error for MinInt64 block count")
	}
}

func TestDeserFixedArrayNegBlockTruncatedSize(t *testing.T) {
	s, _ := Parse(`{"type":"array","items":"int"}`)
	// Negative count but truncated byte-size varlong.
	data := appendVarlong(nil, -3) // count=-3
	// No byte-size follows — truncated.
	var got [3]int32
	_, err := s.Decode(data, &got)
	if err == nil {
		t.Fatal("expected error for truncated byte size")
	}
}

func TestDeserFixedArrayItemError(t *testing.T) {
	s, err := Parse(`{"type":"array","items":"string"}`)
	if err != nil {
		t.Fatal(err)
	}
	// Craft a block with count=1 but truncated string data.
	var data []byte
	data = appendVarlong(data, 1)       // 1 element
	data = appendVarlong(data, 1000000) // string length: huge, will fail
	var got [1]string
	_, err = s.Decode(data, &got)
	if err == nil {
		t.Fatal("expected error for truncated string in fixed array")
	}
}

func TestDecodeJSONNullWithNonNilInput(t *testing.T) {
	// Passing a non-null JSON value with a null schema.
	// DecodeJSON sees 42 with a null schema.
	s, _ := Parse(`"null"`)
	var got any
	err := s.DecodeJSON([]byte(`42`), &got)
	// This may error during Encode(nil) or succeed with nil.
	_ = err
	// Just verify no panic.
}

func TestDecodeJSONArrayItemError(t *testing.T) {
	// Array of union where an item has an unknown branch name.
	s, _ := Parse(`{"type":"array","items":["null","int"]}`)
	var got any
	err := s.DecodeJSON([]byte(`[{"bogus":42}]`), &got)
	if err == nil {
		t.Fatal("expected error for unknown union branch in array item")
	}
}

// TestDecodeJSONCoercionPaths exercises DecodeJSON coercion paths that normal
// tests don't reach.
func TestDecodeJSONCoercionPaths(t *testing.T) {
	// Null for null schema.
	t.Run("null schema", func(t *testing.T) {
		s, _ := Parse(`"null"`)
		var got any
		if err := s.DecodeJSON([]byte(`null`), &got); err != nil {
			t.Fatal(err)
		}
	})

	// Null for union.
	t.Run("null union", func(t *testing.T) {
		s, _ := Parse(`["null","string"]`)
		var got any
		if err := s.DecodeJSON([]byte(`null`), &got); err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Errorf("got %v", got)
		}
	})

	// Float passthrough.
	t.Run("float", func(t *testing.T) {
		s, _ := Parse(`"float"`)
		var got any
		if err := s.DecodeJSON([]byte(`1.5`), &got); err != nil {
			t.Fatal(err)
		}
	})

	// Double passthrough.
	t.Run("double", func(t *testing.T) {
		s, _ := Parse(`"double"`)
		var got any
		if err := s.DecodeJSON([]byte(`3.14`), &got); err != nil {
			t.Fatal(err)
		}
	})
}

// TestEncodeFromJSONUseNumber tests that data from json.Decoder.UseNumber()
// (which produces json.Number instead of float64) can be encoded for every
// numeric schema type. This catches json.Number gaps in specialized serializers.
func TestEncodeFromJSONUseNumber(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		json   string
	}{
		// Primitives.
		{"int", `"int"`, `42`},
		{"long", `"long"`, `100000`},
		{"float", `"float"`, `1.5`},
		{"double", `"double"`, `3.14`},

		// Arrays.
		{"array of int", `{"type":"array","items":"int"}`, `[1,2,3]`},
		{"array of long", `{"type":"array","items":"long"}`, `[100,200]`},
		{"array of float", `{"type":"array","items":"float"}`, `[1.5]`},
		{"array of double", `{"type":"array","items":"double"}`, `[3.14]`},

		// Maps.
		{"map of int", `{"type":"map","values":"int"}`, `{"k":42}`},
		{"map of long", `{"type":"map","values":"long"}`, `{"k":100}`},
		{"map of float", `{"type":"map","values":"float"}`, `{"k":1.5}`},
		{"map of double", `{"type":"map","values":"double"}`, `{"k":3.14}`},

		// Record with numeric fields.
		{"record with int", `{"type":"record","name":"R","fields":[{"name":"n","type":"int"}]}`, `{"n":42}`},
		{"record with long", `{"type":"record","name":"R","fields":[{"name":"n","type":"long"}]}`, `{"n":100}`},
		{"record with float", `{"type":"record","name":"R","fields":[{"name":"n","type":"float"}]}`, `{"n":1.5}`},
		{"record with double", `{"type":"record","name":"R","fields":[{"name":"n","type":"double"}]}`, `{"n":3.14}`},

		// Logical types.
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, `1742385600000`},

		// Nullable numeric.
		{"nullable int", `["null","int"]`, `42`},
		{"nullable long", `["null","long"]`, `100`},

		// Record with map of long (the exact case that broke connect).
		{"record with map of long", `{"type":"record","name":"R","fields":[{"name":"m","type":{"type":"map","values":"long"}}]}`, `{"m":{"i":3}}`},

		// Nested record with numeric fields.
		{"nested numeric", `{"type":"record","name":"O","fields":[{"name":"inner","type":{"type":"record","name":"I","fields":[{"name":"x","type":"int"},{"name":"y","type":"long"}]}}]}`, `{"inner":{"x":1,"y":2}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			// Use json.Decoder.UseNumber() to produce json.Number values.
			dec := json.NewDecoder(bytes.NewReader([]byte(tt.json)))
			dec.UseNumber()
			var native any
			if err := dec.Decode(&native); err != nil {
				t.Fatalf("json.Decode: %v", err)
			}
			binary, err := s.Encode(native)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			var decoded any
			if _, err := s.Decode(binary, &decoded); err != nil {
				t.Fatalf("Decode: %v", err)
			}
		})
	}
}

// TestEncodeJSONFromDecoded tests the full pipeline: binary decode → EncodeJSON.
// This catches gaps where decoded types (json.Number for decimals, int64 for
// timestamps) aren't handled by the JSON encoder.
func TestEncodeJSONFromDecoded(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		value  any // Go value to encode to binary first
	}{
		{"simple record", `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`,
			map[string]any{"a": int32(1), "b": "hello"}},
		{"record with nullable", `{"type":"record","name":"R","fields":[{"name":"v","type":["null","string"]}]}`,
			map[string]any{"v": "hello"}},
		{"record with nullable null", `{"type":"record","name":"R","fields":[{"name":"v","type":["null","string"]}]}`,
			map[string]any{"v": nil}},
		{"record with array of unions", `{"type":"record","name":"R","fields":[{"name":"items","type":{"type":"array","items":["null","string"]}}]}`,
			map[string]any{"items": []any{nil, "a", nil, "b"}}},
		{"record with map of int", `{"type":"record","name":"R","fields":[{"name":"m","type":{"type":"map","values":"int"}}]}`,
			map[string]any{"m": map[string]any{"k": int32(1)}}},
		{"nested union record", `{"type":"record","name":"R","fields":[{"name":"inner","type":["null",{"type":"record","name":"I","fields":[{"name":"x","type":"int"}]}]}]}`,
			map[string]any{"inner": map[string]any{"x": int32(42)}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			// Encode to binary.
			binary, err := s.Encode(tt.value)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			// Decode to any.
			var decoded any
			if _, err := s.Decode(binary, &decoded); err != nil {
				t.Fatalf("Decode: %v", err)
			}
			// EncodeJSON from the decoded value.
			jb, err := s.EncodeJSON(decoded)
			if err != nil {
				t.Fatalf("EncodeJSON: %v", err)
			}
			// Verify valid JSON.
			if !json.Valid(jb) {
				t.Fatalf("invalid JSON: %s", jb)
			}
			// DecodeJSON back and re-encode to binary — full round trip.
			var rt any
			if err := s.DecodeJSON(jb, &rt); err != nil {
				t.Fatalf("DecodeJSON: %v", err)
			}
			binary2, err := s.Encode(rt)
			if err != nil {
				t.Fatalf("re-Encode: %v", err)
			}
			// Binary should match.
			if !bytes.Equal(binary, binary2) {
				t.Errorf("binary mismatch:\n  original: %v\n  roundtrip: %v", binary, binary2)
			}
		})
	}
}
