package avro

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
)

// fuzzSchemas contains pre-compiled schemas covering all Avro types for use
// in fuzz targets that exercise decoding.
var fuzzSchemas []*Schema

func init() {
	schemas := []string{
		// 0-7: 8 primitives
		`"null"`,
		`"boolean"`,
		`"int"`,
		`"long"`,
		`"float"`,
		`"double"`,
		`"bytes"`,
		`"string"`,
		// 8: enum
		`{"type":"enum","name":"E","symbols":["A","B","C"]}`,
		// 9: fixed
		`{"type":"fixed","name":"F","size":4}`,
		// 10: array of int
		`{"type":"array","items":"int"}`,
		// 11: map of string
		`{"type":"map","values":"string"}`,
		// 12: null union
		`["null","string"]`,
		// 13: general union
		`["null","int","string","boolean"]`,
		// 14: multi-field record
		`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"},{"name":"c","type":"boolean"},{"name":"d","type":"double"}]}`,
		// 15: nested record
		`{"type":"record","name":"Outer","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"},{"name":"y","type":"string"}]}},{"name":"z","type":"long"}]}`,
		// 16: record with logical types
		`{"type":"record","name":"Logical","fields":[{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"d","type":{"type":"int","logicalType":"date"}},{"name":"id","type":{"type":"string","logicalType":"uuid"}}]}`,

		// 17-21: arrays of all specialized primitive types
		`{"type":"array","items":"boolean"}`,
		`{"type":"array","items":"long"}`,
		`{"type":"array","items":"float"}`,
		`{"type":"array","items":"double"}`,
		`{"type":"array","items":"string"}`,
		// 22-26: maps of all specialized primitive types
		`{"type":"map","values":"int"}`,
		`{"type":"map","values":"boolean"}`,
		`{"type":"map","values":"long"}`,
		`{"type":"map","values":"float"}`,
		`{"type":"map","values":"double"}`,
		// 27: fixed(16) UUID — exercises deserFixedUUIDReflect path
		`{"type":"fixed","name":"UUID","size":16,"logicalType":"uuid"}`,
		// 28: record with nullable fields (exercises implicit null default)
		`{"type":"record","name":"N","fields":[
			{"name":"a","type":"int"},
			{"name":"b","type":["null","int"]},
			{"name":"c","type":["null","string"]}
		]}`,
		// 29: record with reused named type (exercises dedup path)
		`{"type":"record","name":"D","fields":[
			{"name":"u1","type":{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}},
			{"name":"u2","type":"U"}
		]}`,
		// 30: recursive record (linked list via nullable self-reference)
		`{"type":"record","name":"Node","fields":[
			{"name":"value","type":"int"},
			{"name":"next","type":["null","Node"]}
		]}`,
		// 31: multi-level nested records (3 levels deep)
		`{"type":"record","name":"L1","fields":[
			{"name":"a","type":"int"},
			{"name":"l2","type":{"type":"record","name":"L2","fields":[
				{"name":"b","type":"string"},
				{"name":"l3","type":{"type":"record","name":"L3","fields":[
					{"name":"c","type":"double"},
					{"name":"items","type":{"type":"array","items":"long"}}
				]}}
			]}}
		]}`,
	}
	for _, s := range schemas {
		fuzzSchemas = append(fuzzSchemas, MustParse(s))
	}
}

// fuzzSeed encodes v using the given schema and returns the raw bytes.
// It panics on error, so it should only be called from init or seed setup.
func fuzzSeed(s *Schema, v any) []byte {
	b, err := s.Encode(v)
	if err != nil {
		panic(err)
	}
	return b
}

// fuzzEqual is like reflect.DeepEqual but treats NaN == NaN as true,
// recursing into maps, slices, and arrays.
func fuzzEqual(a, b any) bool {
	va := reflect.ValueOf(a)
	vb := reflect.ValueOf(b)
	return fuzzDeepEqual(va, vb)
}

func fuzzDeepEqual(a, b reflect.Value) bool {
	if !a.IsValid() && !b.IsValid() {
		return true
	}
	if !a.IsValid() || !b.IsValid() {
		return false
	}
	if a.Type() != b.Type() {
		return false
	}
	switch a.Kind() {
	case reflect.Float32, reflect.Float64:
		af, bf := a.Float(), b.Float()
		if math.IsNaN(af) && math.IsNaN(bf) {
			return true
		}
		return af == bf
	case reflect.Map:
		if a.Len() != b.Len() {
			return false
		}
		for _, k := range a.MapKeys() {
			va := a.MapIndex(k)
			vb := b.MapIndex(k)
			if !vb.IsValid() || !fuzzDeepEqual(va, vb) {
				return false
			}
		}
		return true
	case reflect.Slice, reflect.Array:
		if a.Len() != b.Len() {
			return false
		}
		for i := range a.Len() {
			if !fuzzDeepEqual(a.Index(i), b.Index(i)) {
				return false
			}
		}
		return true
	case reflect.Interface:
		return fuzzDeepEqual(a.Elem(), b.Elem())
	default:
		return reflect.DeepEqual(a.Interface(), b.Interface())
	}
}

func FuzzParse(f *testing.F) {
	// Primitives.
	for _, s := range []string{
		`"null"`, `"boolean"`, `"int"`, `"long"`,
		`"float"`, `"double"`, `"bytes"`, `"string"`,
	} {
		f.Add(s)
	}

	// Complex types.
	f.Add(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	f.Add(`{"type":"enum","name":"E","symbols":["X","Y"]}`)
	f.Add(`{"type":"array","items":"string"}`)
	f.Add(`{"type":"map","values":"int"}`)
	f.Add(`{"type":"fixed","name":"F","size":8}`)
	f.Add(`["null","string"]`)
	f.Add(`["null","int","string","boolean"]`)

	// Logical types.
	f.Add(`{"type":"long","logicalType":"timestamp-millis"}`)
	f.Add(`{"type":"int","logicalType":"date"}`)
	f.Add(`{"type":"string","logicalType":"uuid"}`)
	f.Add(`{"type":"int","logicalType":"time-millis"}`)

	// Aliases, namespaces, defaults.
	f.Add(`{"type":"record","name":"R","namespace":"com.example","fields":[{"name":"a","type":"int","default":0}]}`)
	f.Add(`{"type":"record","name":"R","fields":[{"name":"a","type":"int","aliases":["b"]}]}`)
	f.Add(`{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}`)

	// Nested.
	f.Add(`{"type":"record","name":"O","fields":[{"name":"inner","type":{"type":"record","name":"I","fields":[{"name":"x","type":"int"}]}}]}`)

	// Invalid inputs.
	f.Add(``)
	f.Add(`{}`)
	f.Add(`[]`)
	f.Add(`{"type":"bogus"}`)
	f.Add(`not json at all`)
	f.Add(`{"type":"record"}`)
	f.Add(`{"type":"record","name":"R","fields":[{"name":"a","type":"nonexistent"}]}`)

	f.Fuzz(func(t *testing.T, schema string) {
		Parse(schema)
	})
}

func FuzzDecode(f *testing.F) {
	// Seed: for each schema, add valid encoded bytes and empty bytes.
	seeds := []struct {
		idx  uint8
		data []byte
	}{
		// null (null is zero bytes in Avro, no encoding needed)
		{0, []byte{}},
		{0, nil},
		// boolean
		{1, fuzzSeed(fuzzSchemas[1], true)},
		{1, nil},
		// int
		{2, fuzzSeed(fuzzSchemas[2], int32(42))},
		{2, fuzzSeed(fuzzSchemas[2], int32(-1))},
		{2, nil},
		// long
		{3, fuzzSeed(fuzzSchemas[3], int64(1234567890))},
		{3, nil},
		// float
		{4, fuzzSeed(fuzzSchemas[4], float32(3.14))},
		{4, nil},
		// double
		{5, fuzzSeed(fuzzSchemas[5], float64(2.718281828))},
		{5, nil},
		// bytes
		{6, fuzzSeed(fuzzSchemas[6], []byte("hello"))},
		{6, nil},
		// string
		{7, fuzzSeed(fuzzSchemas[7], "hello world")},
		{7, nil},
		// enum
		{8, fuzzSeed(fuzzSchemas[8], "A")},
		{8, nil},
		// fixed
		{9, fuzzSeed(fuzzSchemas[9], [4]byte{1, 2, 3, 4})},
		{9, nil},
		// array
		{10, fuzzSeed(fuzzSchemas[10], []int32{1, 2, 3})},
		{10, nil},
		// map
		{11, fuzzSeed(fuzzSchemas[11], map[string]string{"k": "v"})},
		{11, nil},
		// null union
		{12, fuzzSeed(fuzzSchemas[12], (*string)(nil))},
		{12, fuzzSeed(fuzzSchemas[12], "test")},
		{12, nil},
		// general union
		{13, fuzzSeed(fuzzSchemas[13], (*int)(nil))},
		{13, fuzzSeed(fuzzSchemas[13], int32(7))},
		{13, nil},
		// multi-field record
		{14, fuzzSeed(fuzzSchemas[14], map[string]any{"a": int32(1), "b": "x", "c": true, "d": 1.5})},
		{14, nil},
		// nested record
		{15, fuzzSeed(fuzzSchemas[15], map[string]any{"inner": map[string]any{"x": int32(1), "y": "s"}, "z": int64(2)})},
		{15, nil},
		// logical types record
		{16, fuzzSeed(fuzzSchemas[16], map[string]any{"ts": int64(1000), "d": int32(19000), "id": "550e8400-e29b-41d4-a716-446655440000"})},
		{16, nil},
		// array of boolean
		{17, fuzzSeed(fuzzSchemas[17], []bool{true, false, true})},
		{17, nil},
		// array of long
		{18, fuzzSeed(fuzzSchemas[18], []int64{100, -200, 300})},
		{18, nil},
		// array of float
		{19, fuzzSeed(fuzzSchemas[19], []float32{1.5, -2.5})},
		{19, nil},
		// array of double
		{20, fuzzSeed(fuzzSchemas[20], []float64{3.14, 2.718})},
		{20, nil},
		// array of string
		{21, fuzzSeed(fuzzSchemas[21], []string{"hello", "world"})},
		{21, nil},
		// map of int
		{22, fuzzSeed(fuzzSchemas[22], map[string]int32{"a": 1, "b": 2})},
		{22, nil},
		// map of boolean
		{23, fuzzSeed(fuzzSchemas[23], map[string]bool{"t": true, "f": false})},
		{23, nil},
		// map of long
		{24, fuzzSeed(fuzzSchemas[24], map[string]int64{"x": 999})},
		{24, nil},
		// map of float
		{25, fuzzSeed(fuzzSchemas[25], map[string]float32{"pi": 3.14})},
		{25, nil},
		// map of double
		{26, fuzzSeed(fuzzSchemas[26], map[string]float64{"e": 2.718})},
		{26, nil},
		// fixed UUID
		{27, fuzzSeed(fuzzSchemas[27], [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})},
		{27, nil},
		// record with nullable fields (implicit null default)
		{28, fuzzSeed(fuzzSchemas[28], map[string]any{"a": int32(1), "b": nil, "c": nil})},
		{28, fuzzSeed(fuzzSchemas[28], map[string]any{"a": int32(1), "b": int32(2), "c": "hi"})},
		{28, nil},
		// record with reused named type
		{29, fuzzSeed(fuzzSchemas[29], map[string]any{
			"u1": [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			"u2": [16]byte{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		})},
		{29, nil},
		// recursive linked list: 3 nodes
		{30, fuzzSeed(fuzzSchemas[30], map[string]any{
			"value": int32(1),
			"next": map[string]any{
				"value": int32(2),
				"next": map[string]any{
					"value": int32(3),
					"next":  nil,
				},
			},
		})},
		{30, fuzzSeed(fuzzSchemas[30], map[string]any{"value": int32(42), "next": nil})},
		{30, nil},
		// 3-level nested record
		{31, fuzzSeed(fuzzSchemas[31], map[string]any{
			"a": int32(1),
			"l2": map[string]any{
				"b": "x",
				"l3": map[string]any{
					"c":     3.14,
					"items": []int64{10, 20, 30},
				},
			},
		})},
		{31, nil},
	}

	// Adversarial patterns.
	seeds = append(seeds,
		struct {
			idx  uint8
			data []byte
		}{2, bytes.Repeat([]byte{0xFF}, 16)}, // varint overflow for int
		struct {
			idx  uint8
			data []byte
		}{3, bytes.Repeat([]byte{0xFF}, 16)}, // varint overflow for long
		struct {
			idx  uint8
			data []byte
		}{7, []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x01}}, // huge string length
	)

	for _, s := range seeds {
		f.Add(s.idx, s.data)
	}

	f.Fuzz(func(t *testing.T, idx uint8, data []byte) {
		s := fuzzSchemas[int(idx)%len(fuzzSchemas)]
		var v any
		s.Decode(data, &v)
	})
}

func FuzzDecodeEncodeRoundTrip(f *testing.F) {
	// Seed: one valid encoding per schema.
	type seed struct {
		idx  uint8
		data []byte
	}
	seeds := []seed{
		{0, []byte{}}, // null is zero bytes
		{1, fuzzSeed(fuzzSchemas[1], true)},
		{2, fuzzSeed(fuzzSchemas[2], int32(42))},
		{3, fuzzSeed(fuzzSchemas[3], int64(99))},
		{4, fuzzSeed(fuzzSchemas[4], float32(1.5))},
		{5, fuzzSeed(fuzzSchemas[5], float64(2.5))},
		{6, fuzzSeed(fuzzSchemas[6], []byte("abc"))},
		{7, fuzzSeed(fuzzSchemas[7], "hello")},
		{8, fuzzSeed(fuzzSchemas[8], "B")},
		{9, fuzzSeed(fuzzSchemas[9], [4]byte{1, 2, 3, 4})},
		{10, fuzzSeed(fuzzSchemas[10], []int32{10, 20})},
		{11, fuzzSeed(fuzzSchemas[11], map[string]string{"key": "val"})},
		{12, fuzzSeed(fuzzSchemas[12], "test")},
		{13, fuzzSeed(fuzzSchemas[13], int32(5))},
		{14, fuzzSeed(fuzzSchemas[14], map[string]any{"a": int32(1), "b": "x", "c": false, "d": 3.14})},
		{15, fuzzSeed(fuzzSchemas[15], map[string]any{"inner": map[string]any{"x": int32(9), "y": "z"}, "z": int64(8)})},
		{16, fuzzSeed(fuzzSchemas[16], map[string]any{"ts": int64(0), "d": int32(0), "id": "550e8400-e29b-41d4-a716-446655440000"})},
		{17, fuzzSeed(fuzzSchemas[17], []bool{true, false})},
		{18, fuzzSeed(fuzzSchemas[18], []int64{100, -200})},
		{19, fuzzSeed(fuzzSchemas[19], []float32{1.5})},
		{20, fuzzSeed(fuzzSchemas[20], []float64{3.14})},
		{21, fuzzSeed(fuzzSchemas[21], []string{"hello"})},
		{22, fuzzSeed(fuzzSchemas[22], map[string]int32{"a": 1})},
		{23, fuzzSeed(fuzzSchemas[23], map[string]bool{"t": true})},
		{24, fuzzSeed(fuzzSchemas[24], map[string]int64{"x": 99})},
		{25, fuzzSeed(fuzzSchemas[25], map[string]float32{"pi": 3.14})},
		{26, fuzzSeed(fuzzSchemas[26], map[string]float64{"e": 2.718})},
		{27, fuzzSeed(fuzzSchemas[27], [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})},
		{28, fuzzSeed(fuzzSchemas[28], map[string]any{"a": int32(1), "b": int32(2), "c": "x"})},
		{29, fuzzSeed(fuzzSchemas[29], map[string]any{
			"u1": [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			"u2": [16]byte{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		})},
		{30, fuzzSeed(fuzzSchemas[30], map[string]any{
			"value": int32(1),
			"next": map[string]any{
				"value": int32(2),
				"next":  nil,
			},
		})},
		{31, fuzzSeed(fuzzSchemas[31], map[string]any{
			"a": int32(1),
			"l2": map[string]any{
				"b": "x",
				"l3": map[string]any{
					"c":     3.14,
					"items": []int64{10, 20, 30},
				},
			},
		})},
	}

	for _, s := range seeds {
		f.Add(s.idx, s.data)
	}

	f.Fuzz(func(t *testing.T, idx uint8, data []byte) {
		s := fuzzSchemas[int(idx)%len(fuzzSchemas)]

		var v1 any
		rem, err := s.Decode(data, &v1)
		if err != nil || len(rem) != 0 {
			return
		}

		encoded, err := s.Encode(v1)
		if err != nil {
			return // some decoded-into-any types can't re-encode (null, fixed)
		}

		var v2 any
		rem, err = s.Decode(encoded, &v2)
		if err != nil {
			t.Fatalf("re-decode failed: %v", err)
		}
		if len(rem) != 0 {
			t.Fatalf("re-decode left %d trailing bytes", len(rem))
		}

		if !fuzzEqual(v1, v2) {
			t.Fatalf("round-trip mismatch:\n  v1: %#v\n  v2: %#v", v1, v2)
		}
	})
}

func FuzzSingleObject(f *testing.F) {
	// Valid single-object encoded values for several schemas.
	for i, s := range fuzzSchemas {
		var val any
		switch i {
		case 0: // null
			val = nil
		case 1: // boolean
			val = true
		case 2: // int
			val = int32(42)
		case 3: // long
			val = int64(99)
		case 4: // float
			val = float32(1.5)
		case 5: // double
			val = float64(2.5)
		case 6: // bytes
			val = []byte("abc")
		case 7: // string
			val = "hello"
		case 8: // enum
			val = "A"
		case 9: // fixed
			val = [4]byte{1, 2, 3, 4}
		case 10: // array
			val = []int32{1, 2}
		case 11: // map
			val = map[string]string{"k": "v"}
		case 12: // null union
			val = "test"
		case 13: // general union
			val = int32(5)
		case 14: // multi-field record
			val = map[string]any{"a": int32(1), "b": "x", "c": true, "d": 1.5}
		case 15: // nested record
			val = map[string]any{"inner": map[string]any{"x": int32(1), "y": "s"}, "z": int64(2)}
		case 16: // logical types record
			val = map[string]any{"ts": int64(0), "d": int32(0), "id": "550e8400-e29b-41d4-a716-446655440000"}
		case 17: // array of boolean
			val = []bool{true, false}
		case 18: // array of long
			val = []int64{100, -200}
		case 19: // array of float
			val = []float32{1.5}
		case 20: // array of double
			val = []float64{3.14}
		case 21: // array of string
			val = []string{"hello"}
		case 22: // map of int
			val = map[string]int32{"a": 1}
		case 23: // map of boolean
			val = map[string]bool{"t": true}
		case 24: // map of long
			val = map[string]int64{"x": 99}
		case 25: // map of float
			val = map[string]float32{"pi": 3.14}
		case 26: // map of double
			val = map[string]float64{"e": 2.718}
		case 27: // fixed(16) UUID
			val = [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
		case 28: // nullable record
			val = map[string]any{"a": int32(1), "b": int32(2), "c": "x"}
		case 29: // reused named type
			val = map[string]any{
				"u1": [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
				"u2": [16]byte{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
			}
		case 30: // recursive linked list
			val = map[string]any{
				"value": int32(1),
				"next":  map[string]any{"value": int32(2), "next": nil},
			}
		case 31: // 3-level nested record
			val = map[string]any{
				"a": int32(1),
				"l2": map[string]any{
					"b": "x",
					"l3": map[string]any{
						"c":     3.14,
						"items": []int64{10, 20, 30},
					},
				},
			}
		}
		soe, err := s.AppendSingleObject(nil, val)
		if err != nil {
			continue
		}
		f.Add(soe)
	}

	// Truncated: just the magic.
	f.Add([]byte{0xC3, 0x01})
	// Too short for fingerprint.
	f.Add([]byte{0xC3, 0x01, 0x00, 0x00})
	// Wrong magic bytes.
	f.Add([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	// Empty.
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		SingleObjectFingerprint(data)
		for _, s := range fuzzSchemas {
			var v any
			s.DecodeSingleObject(data, &v)
		}
	})
}

// FuzzDecodeJSON feeds random JSON strings into the streaming JSON decoder.
// It exercises the byte scanner, schema-guided parsing, and error paths.
func FuzzDecodeJSON(f *testing.F) {
	seeds := []struct {
		idx   uint8
		input string
	}{
		// Primitives.
		{0, `null`},
		{1, `true`}, {1, `false`},
		{2, `42`}, {2, `-1`}, {2, `0`},
		{3, `1234567890`}, {3, `-9999`},
		{4, `3.14`}, {4, `"NaN"`}, {4, `"Infinity"`}, {4, `null`},
		{5, `2.718`}, {5, `"NaN"`}, {5, `"-Infinity"`}, {5, `null`},
		{6, `"hello"`}, {6, `""`},
		{7, `"world"`}, {7, `"line1\nline2"`},
		// Enum.
		{8, `"A"`}, {8, `"B"`}, {8, `"C"`},
		// Fixed.
		{9, `"abcd"`},
		// Array.
		{10, `[1,2,3]`}, {10, `[]`},
		// Map.
		{11, `{"k":"v"}`}, {11, `{}`},
		// Null union.
		{12, `null`}, {12, `{"string":"hello"}`}, {12, `"bare"`},
		// General union.
		{13, `null`}, {13, `42`}, {13, `"hello"`}, {13, `true`},
		{13, `{"int":42}`}, {13, `{"string":"tagged"}`},
		// Multi-field record.
		{14, `{"a":1,"b":"x","c":true,"d":3.14}`},
		{14, `{"a":1,"b":"x","c":true,"d":3.14,"extra":"skip"}`},
		// Nested record.
		{15, `{"inner":{"x":1,"y":"s"},"z":2}`},
		// Logical types.
		{16, `{"ts":1700000000000,"d":19700,"id":"550e8400-e29b-41d4-a716-446655440000"}`},
		// Invalid inputs.
		{2, `"notanumber"`}, {2, ``}, {2, `{}`},
		{7, `42`}, {1, `42`},
		{14, `{"a":"wrong"}`}, {14, `{}`},
		{10, `"notarray"`},
		{11, `"notmap"`},
	}
	for _, s := range seeds {
		f.Add(s.idx, s.input)
	}

	f.Fuzz(func(t *testing.T, idx uint8, input string) {
		s := fuzzSchemas[int(idx)%len(fuzzSchemas)]
		var v any
		s.DecodeJSON([]byte(input), &v)
	})
}

// FuzzDecodeJSONRoundTrip verifies that valid JSON → DecodeJSON → EncodeJSON
// produces output that re-decodes to the same value.
func FuzzDecodeJSONRoundTrip(f *testing.F) {
	seeds := []struct {
		idx   uint8
		input string
	}{
		{2, `42`},
		{3, `99`},
		{4, `3.14`},
		{5, `2.718`},
		{7, `"hello"`},
		{1, `true`},
		{10, `[1,2,3]`},
		{11, `{"k":"v"}`},
		{12, `{"string":"test"}`},
		{12, `null`},
		{14, `{"a":1,"b":"x","c":true,"d":3.14}`},
	}
	for _, s := range seeds {
		f.Add(s.idx, s.input)
	}

	f.Fuzz(func(t *testing.T, idx uint8, input string) {
		s := fuzzSchemas[int(idx)%len(fuzzSchemas)]
		// Postel: input may be non-canonical (lenient decode is OK).
		// Re-encoding produces canonical output. Test canonical
		// idempotence: encode → decode → encode must be stable.
		// Asserting bit-exact equality with the original bytes is
		// wrong under Postel — non-canonical input legitimately
		// canonicalizes on the first encode.
		var v1 any
		if err := s.DecodeJSON([]byte(input), &v1); err != nil {
			return
		}
		encoded1, err := s.EncodeJSON(v1)
		if err != nil {
			return
		}
		var v2 any
		if err := s.DecodeJSON(encoded1, &v2); err != nil {
			t.Fatalf("re-decode of canonical encoded failed: %v\n  input: %s\n  encoded: %s", err, input, encoded1)
		}
		encoded2, err := s.EncodeJSON(v2)
		if err != nil {
			t.Fatalf("re-encode of canonical value failed: %v", err)
		}
		if !bytes.Equal(encoded1, encoded2) {
			t.Fatalf("encode is not idempotent on canonical input:\n  encoded1: %s\n  encoded2: %s\n  input:    %s", encoded1, encoded2, input)
		}
	})
}

// FuzzEncodeTaggedUnion verifies that Encode accepts tagged union maps
// from Decode(TaggedUnions) and produces canonical binary that is
// stable across additional encode/decode passes (canonical idempotence
// — Postel: lenient decode + strict canonical encode means
// non-canonical input legitimately canonicalizes on the first encode,
// so bit-exact-with-input comparison is the wrong assertion).
func FuzzEncodeTaggedUnion(f *testing.F) {
	seeds := []struct {
		idx  uint8
		data []byte
	}{
		{12, fuzzSeed(fuzzSchemas[12], "hello")},
		{12, fuzzSeed(fuzzSchemas[12], (*string)(nil))},
		{13, fuzzSeed(fuzzSchemas[13], int32(7))},
		{13, fuzzSeed(fuzzSchemas[13], "test")},
		{13, fuzzSeed(fuzzSchemas[13], true)},
		{13, fuzzSeed(fuzzSchemas[13], (*int)(nil))},
		{14, fuzzSeed(fuzzSchemas[14], map[string]any{"a": int32(1), "b": "x", "c": true, "d": 1.5})},
	}
	for _, s := range seeds {
		f.Add(s.idx, s.data)
	}

	f.Fuzz(func(t *testing.T, idx uint8, data []byte) {
		s := fuzzSchemas[int(idx)%len(fuzzSchemas)]
		var tagged1 any
		rem, err := s.Decode(data, &tagged1, TaggedUnions())
		if err != nil || len(rem) != 0 {
			return
		}
		encoded1, err := s.Encode(tagged1)
		if err != nil {
			return
		}
		// Canonical idempotence: re-decoding the encoded bytes and
		// re-encoding must produce the same bytes. Comparing to the
		// ORIGINAL data is wrong under Postel — e.g. boolean byte
		// 0x30 decodes to true and encodes to 0x01.
		var tagged2 any
		if _, err := s.Decode(encoded1, &tagged2, TaggedUnions()); err != nil {
			t.Fatalf("re-decode of canonical encoded failed: %v\n  encoded1: %x", err, encoded1)
		}
		encoded2, err := s.Encode(tagged2)
		if err != nil {
			t.Fatalf("re-encode of canonical value failed: %v", err)
		}
		if !bytes.Equal(encoded1, encoded2) {
			t.Fatalf("encode is not idempotent on canonical input:\n  encoded1: %x\n  encoded2: %x", encoded1, encoded2)
		}
	})
}

// FuzzDecodeJSONTyped decodes random JSON into typed Go targets.
func FuzzDecodeJSONTyped(f *testing.F) {
	type Record struct {
		A int32   `avro:"a"`
		B string  `avro:"b"`
		C bool    `avro:"c"`
		D float64 `avro:"d"`
	}
	recordSchema := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"},
		{"name":"c","type":"boolean"},
		{"name":"d","type":"double"}
	]}`)

	f.Add(`{"a":1,"b":"x","c":true,"d":3.14}`)
	f.Add(`{"a":0,"b":"","c":false,"d":0}`)
	f.Add(`{}`)
	f.Add(`{"a":"wrong"}`)
	f.Add(`not json`)
	f.Add(`{"a":1,"b":"x","c":true,"d":3.14,"extra":{"nested":true}}`)

	f.Fuzz(func(t *testing.T, input string) {
		var r Record
		recordSchema.DecodeJSON([]byte(input), &r)
	})
}

// FuzzDecodeTyped decodes random bytes into typed Go targets, exercising
// the unsafe fast path and fixed-size array decoding.
func FuzzDecodeTyped(f *testing.F) {
	type Record struct {
		A int32   `avro:"a"`
		B string  `avro:"b"`
		C bool    `avro:"c"`
		D float64 `avro:"d"`
	}
	recordSchema := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"},
		{"name":"c","type":"boolean"},
		{"name":"d","type":"double"}
	]}`)

	arraySchema := MustParse(`{"type":"array","items":"int"}`)

	// Seeds: valid encodings.
	f.Add(uint8(0), fuzzSeed(recordSchema, &Record{A: 1, B: "x", C: true, D: 3.14}))
	f.Add(uint8(1), fuzzSeed(arraySchema, []int32{1, 2, 3}))
	f.Add(uint8(0), []byte{})
	f.Add(uint8(1), []byte{})
	f.Add(uint8(0), bytes.Repeat([]byte{0xFF}, 32))
	f.Add(uint8(1), bytes.Repeat([]byte{0xFF}, 32))

	f.Fuzz(func(t *testing.T, mode uint8, data []byte) {
		switch mode % 3 {
		case 0:
			var r Record
			recordSchema.Decode(data, &r)
		case 1:
			var sl []int32
			arraySchema.Decode(data, &sl)
		case 2:
			var arr [4]int32
			arraySchema.Decode(data, &arr)
		}
	})
}

// FuzzEncodeMap exercises encoding from map[string]any with defaults,
// timestamp strings, json.Number, and decimal coercion.
func FuzzEncodeMap(f *testing.F) {
	schema := MustParse(`{
		"type":"record","name":"R",
		"fields":[
			{"name":"a","type":"int","default":0},
			{"name":"b","type":"string","default":""},
			{"name":"c","type":{"type":"long","logicalType":"timestamp-millis"},"default":0},
			{"name":"d","type":"double","default":0}
		]
	}`)

	f.Add(`{}`)
	f.Add(`{"a":42}`)
	f.Add(`{"a":1,"b":"hello","c":"2026-03-19T10:00:00Z","d":3.14}`)
	f.Add(`{"a":1,"b":"hello","c":1742385600000,"d":3.14}`)
	f.Add(`{"a":1,"b":"hello","c":"not-a-timestamp","d":3.14}`)
	f.Add(`{"extra":"ignored","a":1,"b":"x","c":0,"d":0}`)
	f.Add(`not json`)

	f.Fuzz(func(t *testing.T, input string) {
		var m any
		if err := json.Unmarshal([]byte(input), &m); err != nil {
			return
		}
		schema.Encode(m)
	})
}

// FuzzSchemaNode exercises [SchemaNode.Schema] by feeding random JSON
// through [Schema.Root] → mutate → Schema(). This is the closest we can
// get to fuzzing programmatic construction without hand-rolling a
// SchemaNode generator. Exercises toJSONDedup, cycle detection, named
// type dedup, and implicit null default wiring.
func FuzzSchemaNode(f *testing.F) {
	seeds := []string{
		`"int"`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`,
		// Empty record. The spec requires "fields":[] but twmb/avro's
		// parser is lenient and accepts the missing attribute too; both
		// variants must round-trip identically through Canonical().
		`{"type":"record","name":"Empty","fields":[]}`,
		`{"type":"record","name":"Empty"}`,
		// Nested empty records at various positions.
		`{"type":"record","name":"Outer","fields":[{"name":"inner","type":{"type":"record","name":"I","fields":[]}}]}`,
		`{"type":"array","items":{"type":"record","name":"E","fields":[]}}`,
		`{"type":"map","values":{"type":"record","name":"E","fields":[]}}`,
		`["null",{"type":"record","name":"E","fields":[]}]`,
		`{"type":"record","name":"R","fields":[
			{"name":"a","type":{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}},
			{"name":"b","type":"U"}
		]}`,
		`{"type":"record","name":"R","fields":[
			{"name":"a","type":["null","int"]},
			{"name":"b","type":["null","string"]}
		]}`,
		`{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}}`,
		`{"type":"map","values":{"type":"enum","name":"E","symbols":["A","B"]}}`,
		`["null","int","string",{"type":"fixed","name":"F","size":4}]`,
		// Recursive linked list via self-reference.
		`{"type":"record","name":"Node","fields":[
			{"name":"value","type":"int"},
			{"name":"next","type":["null","Node"]}
		]}`,
		// 3-level nested records.
		`{"type":"record","name":"L1","fields":[
			{"name":"l2","type":{"type":"record","name":"L2","fields":[
				{"name":"l3","type":{"type":"record","name":"L3","fields":[
					{"name":"x","type":"int"}
				]}}
			]}}
		]}`,
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		s, err := Parse(input)
		if err != nil {
			return
		}
		root := s.Root()
		// Round-trip: Root().Schema() must succeed for any schema Parse
		// accepted, and produce the same canonical form.
		s2, err := root.Schema()
		if err != nil {
			t.Fatalf("Root().Schema() failed for valid schema %q: %v", input, err)
		}
		if !bytes.Equal(s.Canonical(), s2.Canonical()) {
			t.Fatalf("canonical form changed through Root()/Schema() round-trip:\n  orig: %s\n  new:  %s",
				s.Canonical(), s2.Canonical())
		}
	})
}

// FuzzEncodeMapMissingKeys exercises the implicit null default path by
// encoding random map subsets of a record with nullable fields.
func FuzzEncodeMapMissingKeys(f *testing.F) {
	schema := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":["null","int"]},
		{"name":"c","type":["null","string"]},
		{"name":"d","type":"string","default":"hi"}
	]}`)

	// Seeds: various combinations of present/missing keys.
	seeds := []string{
		`{"a":1,"b":2,"c":"x","d":"y"}`,
		`{"a":1}`,                   // b, c, d all missing
		`{"a":1,"b":5}`,             // c, d missing
		`{"a":1,"c":"only c"}`,      // b, d missing
		`{"a":1,"b":null,"c":null}`, // explicit nulls
		`{"b":1,"c":"x"}`,           // missing required 'a'
		`{"a":"wrong type"}`,        // wrong type
		`{"a":1,"extra":"ignored"}`, // extra key
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		var m any
		if err := json.Unmarshal([]byte(input), &m); err != nil {
			return
		}
		mm, ok := m.(map[string]any)
		if !ok {
			return
		}
		// Coerce float64 (from json.Unmarshal) to int32 for field "a".
		if v, ok := mm["a"]; ok {
			if f, ok := v.(float64); ok {
				mm["a"] = int32(f)
			}
		}
		if v, ok := mm["b"]; ok {
			if f, ok := v.(float64); ok {
				mm["b"] = int32(f)
			}
		}
		schema.Encode(mm)
	})
}

func FuzzResolve(f *testing.F) {
	type seed struct {
		reader string
		writer string
		data   []byte
	}

	// Identity: same record schema.
	recSchema := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`
	writerS := MustParse(recSchema)
	identityData := fuzzSeed(writerS, map[string]any{"a": int32(1), "b": "x"})

	// Field addition with default.
	writerAdd := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`
	readerAdd := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string","default":"hi"}]}`
	writerAddS := MustParse(writerAdd)
	addData := fuzzSeed(writerAddS, map[string]any{"a": int32(7)})

	// Type promotion: int -> long.
	writerProm := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`
	readerProm := `{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}`
	writerPromS := MustParse(writerProm)
	promData := fuzzSeed(writerPromS, map[string]any{"a": int32(100)})

	// Field removal.
	writerRem := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`
	readerRem := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`
	writerRemS := MustParse(writerRem)
	remData := fuzzSeed(writerRemS, map[string]any{"a": int32(3), "b": "drop"})

	// Incompatible: int vs string.
	writerIncompat := `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`
	readerIncompat := `{"type":"record","name":"R","fields":[{"name":"a","type":"string"}]}`

	seeds := []seed{
		{recSchema, recSchema, identityData},
		{readerAdd, writerAdd, addData},
		{readerProm, writerProm, promData},
		{readerRem, writerRem, remData},
		{readerIncompat, writerIncompat, nil},
		// Primitives.
		{`"int"`, `"int"`, fuzzSeed(MustParse(`"int"`), int32(42))},

		// Enum: writer adds new symbol, reader has default.
		{
			`{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}`,
			`{"type":"enum","name":"E","symbols":["A","B","C"]}`,
			fuzzSeed(MustParse(`{"type":"enum","name":"E","symbols":["A","B","C"]}`), "C"),
		},
		// Array: item type promotion int -> long.
		{
			`{"type":"array","items":"long"}`,
			`{"type":"array","items":"int"}`,
			fuzzSeed(MustParse(`{"type":"array","items":"int"}`), []int32{1, 2, 3}),
		},
		// Map: value type promotion int -> long.
		{
			`{"type":"map","values":"long"}`,
			`{"type":"map","values":"int"}`,
			fuzzSeed(MustParse(`{"type":"map","values":"int"}`), map[string]int32{"k": 10}),
		},
		// Union: writer has subset of reader branches.
		{
			`["null","int","string"]`,
			`["null","int"]`,
			fuzzSeed(MustParse(`["null","int"]`), int32(7)),
		},
		// Primitive promotions.
		{`"float"`, `"int"`, fuzzSeed(MustParse(`"int"`), int32(5))},
		{`"double"`, `"long"`, fuzzSeed(MustParse(`"long"`), int64(99))},
		{`"long"`, `"int"`, fuzzSeed(MustParse(`"int"`), int32(42))},
		{`"double"`, `"float"`, fuzzSeed(MustParse(`"float"`), float32(1.5))},
	}

	for _, s := range seeds {
		f.Add(s.reader, s.writer, s.data)
	}

	f.Fuzz(func(t *testing.T, readerJSON, writerJSON string, data []byte) {
		reader, err := Parse(readerJSON)
		if err != nil {
			return
		}
		writer, err := Parse(writerJSON)
		if err != nil {
			return
		}
		resolved, err := Resolve(writer, reader)
		if err != nil {
			return
		}
		var v any
		resolved.Decode(data, &v)
	})
}

// FuzzDecodeVariedTargets fuzzes binary decode against many target shapes,
// not just *any. The pre-existing FuzzDecode only used `var v any` and
// missed:
//   - panics when decoding into *interface{Foo()} / *error / etc.
//   - panics when decoding into a struct with non-empty-interface fields
//   - panics on re-decode into a populated *any (the inner unwraps to
//     unaddressable Value)
// The driver's `mode` byte selects the target shape; data bytes are the
// wire input. All schemas come from fuzzSchemas. No panic is ever
// expected — every target/data combo must surface as a returned error.
func FuzzDecodeVariedTargets(f *testing.F) {
	type IfaceField struct {
		X interface{ Foo() } `avro:"x"`
	}
	type ErrorField struct {
		X error `avro:"x"`
	}

	makeTarget := func(mode uint8) any {
		switch mode % 12 {
		case 0:
			var v any
			return &v
		case 1:
			var v interface{ Foo() }
			return &v
		case 2:
			var v error
			return &v
		case 3:
			var v map[string]any
			return &v
		case 4:
			var v []any
			return &v
		case 5:
			var v IfaceField
			return &v
		case 6:
			var v ErrorField
			return &v
		case 7:
			var v int32
			return &v
		case 8:
			var v *int32
			return &v
		case 9:
			var v string
			return &v
		case 10:
			// Pre-populated *any so the inner-Value path runs.
			v := any(int32(99))
			return &v
		case 11:
			// Pre-populated *any holding a slice (not a map — exercises
			// the unwrap-only-Map rule).
			v := any([]any{int32(1)})
			return &v
		}
		var v any
		return &v
	}

	// Seed every (schema, target_kind) pair with a valid binary encoding
	// plus an empty buffer.
	for i := range fuzzSchemas {
		for mode := uint8(0); mode < 12; mode++ {
			f.Add(uint8(i), mode, []byte{})
			f.Add(uint8(i), mode, []byte{0})
			f.Add(uint8(i), mode, []byte{2, 'x'})
		}
	}

	f.Fuzz(func(t *testing.T, schemaIdx, mode uint8, data []byte) {
		s := fuzzSchemas[int(schemaIdx)%len(fuzzSchemas)]
		tgt := makeTarget(mode)
		// Decode in either tagged or untagged mode based on a low bit of
		// mode — every option combo must be panic-free.
		if mode&0x80 != 0 {
			s.Decode(data, tgt, TaggedUnions())
		} else {
			s.Decode(data, tgt)
		}
	})
}

// FuzzDecodeJSONVariedTargets is the JSON-decode counterpart to
// FuzzDecodeVariedTargets.
func FuzzDecodeJSONVariedTargets(f *testing.F) {
	type IfaceField struct {
		X interface{ Foo() } `avro:"x"`
	}

	makeTarget := func(mode uint8) any {
		switch mode % 9 {
		case 0:
			var v any
			return &v
		case 1:
			var v interface{ Foo() }
			return &v
		case 2:
			var v error
			return &v
		case 3:
			var v map[string]any
			return &v
		case 4:
			var v IfaceField
			return &v
		case 5:
			var v int32
			return &v
		case 6:
			var v string
			return &v
		case 7:
			v := any(int32(0))
			return &v
		case 8:
			v := any(map[string]any{})
			return &v
		}
		var v any
		return &v
	}

	for i := range fuzzSchemas {
		for mode := uint8(0); mode < 9; mode++ {
			for _, src := range []string{
				`null`, `42`, `"x"`, `true`, `[]`, `{}`,
				`{"int":1}`, `{"null":null}`, `{"x":1}`,
			} {
				f.Add(uint8(i), mode, src)
			}
		}
	}

	f.Fuzz(func(t *testing.T, schemaIdx, mode uint8, src string) {
		s := fuzzSchemas[int(schemaIdx)%len(fuzzSchemas)]
		tgt := makeTarget(mode)
		if mode&0x80 != 0 {
			s.DecodeJSON([]byte(src), tgt, TaggedUnions())
		} else {
			s.DecodeJSON([]byte(src), tgt)
		}
	})
}

// FuzzDecodeReuse repeatedly decodes into the same *any target.
// This is the common streaming pattern (OCF reader, batch consumer) and
// the pre-existing fuzzers all created a fresh target per iteration.
// That blind spot hid the indirectAlloc panic where the *any's inner
// becomes unaddressable on the second decode.
func FuzzDecodeReuse(f *testing.F) {
	for i := range fuzzSchemas {
		f.Add(uint8(i), []byte{0}, []byte{0})
		f.Add(uint8(i), []byte{}, []byte{})
	}

	f.Fuzz(func(t *testing.T, schemaIdx uint8, data1, data2 []byte) {
		s := fuzzSchemas[int(schemaIdx)%len(fuzzSchemas)]
		var v any
		s.Decode(data1, &v)
		// Second call into same target — the bug manifests here.
		s.Decode(data2, &v)
		// Third for good measure.
		s.Decode(data1, &v)
	})
}

// FuzzDecodeJSONReuse: JSON counterpart to FuzzDecodeReuse.
func FuzzDecodeJSONReuse(f *testing.F) {
	for i := range fuzzSchemas {
		f.Add(uint8(i), `null`, `null`)
		f.Add(uint8(i), `42`, `43`)
		f.Add(uint8(i), `{}`, `{}`)
	}

	f.Fuzz(func(t *testing.T, schemaIdx uint8, src1, src2 string) {
		s := fuzzSchemas[int(schemaIdx)%len(fuzzSchemas)]
		var v any
		s.DecodeJSON([]byte(src1), &v)
		s.DecodeJSON([]byte(src2), &v)
	})
}

// FuzzEncodeHostile fuzzes the encoder with values that mix nils, weird
// types, and tagged-union maps with bogus branch keys against every
// schema. None should panic — only return errors.
func FuzzEncodeHostile(f *testing.F) {
	type S struct {
		X any            `avro:"x"`
		A []any          `avro:"a"`
		M map[string]any `avro:"m"`
	}

	makeValue := func(mode uint8) any {
		switch mode % 16 {
		case 0:
			return nil
		case 1:
			return any(nil)
		case 2:
			return (*int)(nil)
		case 3:
			return map[string]any{"x": nil}
		case 4:
			return []any{nil, int32(1), nil}
		case 5:
			return map[string]any{"int": nil}
		case 6:
			return map[string]any{"null": nil}
		case 7:
			return map[string]any{"unknown_branch": int32(1)}
		case 8:
			return map[string]any{"x": []any{nil}}
		case 9:
			return map[string]any{"x": map[string]any{"k": nil}}
		case 10:
			return S{X: nil, A: []any{nil}, M: map[string]any{"k": nil}}
		case 11:
			return map[int]int{1: 1} // non-string-keyed map
		case 12:
			return map[any]any{1: 1}
		case 13:
			return json.Number("garbage")
		case 14:
			return map[string]any{"x": json.Number("not-a-number")}
		case 15:
			return []any{int32(1), "string", nil, true, 3.14}
		}
		return nil
	}

	for i := range fuzzSchemas {
		for mode := uint8(0); mode < 16; mode++ {
			f.Add(uint8(i), mode)
		}
	}

	f.Fuzz(func(t *testing.T, schemaIdx, mode uint8) {
		s := fuzzSchemas[int(schemaIdx)%len(fuzzSchemas)]
		v := makeValue(mode)
		// Both binary and JSON, both option modes.
		s.AppendEncode(nil, v)
		s.AppendEncode(nil, v, TaggedUnions())
		s.AppendEncodeJSON(nil, v)
		s.AppendEncodeJSON(nil, v, TaggedUnions())
	})
}

// FuzzResolveBroad fuzzes Resolve across many reader/writer pairs.
// FuzzResolve already exists but its seed corpus is narrow. This one
// pairs every fuzzSchemas entry with every other entry to surface
// resolution edge cases (alias mismatches, recursive cycle handling,
// promote chain panics).
func FuzzResolveBroad(f *testing.F) {
	for i := range fuzzSchemas {
		for j := range fuzzSchemas {
			f.Add(uint8(i), uint8(j), []byte{})
			f.Add(uint8(i), uint8(j), []byte{0})
			f.Add(uint8(i), uint8(j), []byte{2, 84})
		}
	}
	f.Fuzz(func(t *testing.T, wIdx, rIdx uint8, data []byte) {
		w := fuzzSchemas[int(wIdx)%len(fuzzSchemas)]
		r := fuzzSchemas[int(rIdx)%len(fuzzSchemas)]
		res, err := Resolve(w, r)
		if err != nil {
			return
		}
		var v any
		res.Decode(data, &v)
		// Vary target shape too.
		var v2 interface{ Foo() }
		res.Decode(data, &v2)
	})
}

// FuzzCustomTypeRoundTrip wires up a custom type and exercises
// encode/decode round-trip with arbitrary value bytes. The custom-type
// path goes through wrapDeserWithCustomDecoders / setCustomResult,
// which had a panic earlier; this fuzz keeps that path under coverage.
func FuzzCustomTypeRoundTrip(f *testing.F) {
	type Wrapped struct{ V int }
	ct := NewCustomType[Wrapped, int32](
		"",
		func(w Wrapped, _ *SchemaNode) (int32, error) { return int32(w.V), nil },
		func(v int32, _ *SchemaNode) (Wrapped, error) { return Wrapped{V: int(v)}, nil },
	)
	s, err := Parse(`"int"`, WithCustomType(ct))
	if err != nil {
		f.Fatal(err)
	}

	f.Add(int32(0))
	f.Add(int32(-1))
	f.Add(int32(1 << 30))
	f.Add(int32(-1 << 30))

	f.Fuzz(func(t *testing.T, val int32) {
		w := Wrapped{V: int(val)}
		encoded, err := s.AppendEncode(nil, w)
		if err != nil {
			return
		}
		var got Wrapped
		if _, err := s.Decode(encoded, &got); err != nil {
			t.Fatalf("decode after encode failed: %v\n  data: %x", err, encoded)
		}
		if got.V != w.V {
			t.Fatalf("custom type round-trip mismatch: got %v, want %v", got, w)
		}
		// And decode-into-interface variants.
		var anyV any
		if _, err := s.Decode(encoded, &anyV); err != nil {
			t.Fatalf("decode into *any failed: %v", err)
		}
	})
}

// FuzzConcurrentEncodeDecode hammers a shared *Schema from multiple
// goroutines with arbitrary inputs. The unsafe fast-path init uses
// atomic.Pointer; the per-type cache uses sync.Map. Concurrent
// fuzz exercise stresses these paths in a way single-threaded fuzz
// can't.
func FuzzConcurrentEncodeDecode(f *testing.F) {
	type Record struct {
		A int32  `avro:"a"`
		B string `avro:"b"`
	}
	s, err := Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`)
	if err != nil {
		f.Fatal(err)
	}

	f.Add(int32(1), "x", uint8(4))
	f.Add(int32(0), "", uint8(8))
	f.Add(int32(-1), "ababab", uint8(2))

	f.Fuzz(func(t *testing.T, a int32, b string, n uint8) {
		workers := 1 + int(n%8)
		// Collect panics from worker goroutines via channel rather than
		// calling t.Errorf directly: testing.T methods other than Log
		// aren't safe for concurrent use from non-test goroutines.
		panicCh := make(chan any, workers)
		done := make(chan struct{}, workers)
		for i := 0; i < workers; i++ {
			go func() {
				defer func() {
					if r := recover(); r != nil {
						panicCh <- r
					}
					done <- struct{}{}
				}()
				for j := 0; j < 20; j++ {
					rec := Record{A: a + int32(j), B: b}
					data, err := s.AppendEncode(nil, &rec)
					if err != nil {
						continue
					}
					var got Record
					s.Decode(data, &got)
					var anyV any
					s.Decode(data, &anyV)
				}
			}()
		}
		for i := 0; i < workers; i++ {
			<-done
		}
		close(panicCh)
		for p := range panicCh {
			t.Errorf("panic in concurrent worker: %v", p)
		}
	})
}

// FuzzTimeDateEdgeCases fuzzes time/date logical types around boundary
// values: pre-epoch, far future, leap seconds, NaN/Infinity for floats
// in time-millis representations.
func FuzzTimeDateEdgeCases(f *testing.F) {
	schemas := []string{
		`{"type":"long","logicalType":"timestamp-millis"}`,
		`{"type":"long","logicalType":"timestamp-micros"}`,
		`{"type":"long","logicalType":"timestamp-nanos"}`,
		`{"type":"int","logicalType":"date"}`,
		`{"type":"int","logicalType":"time-millis"}`,
		`{"type":"long","logicalType":"time-micros"}`,
	}
	parsed := make([]*Schema, len(schemas))
	for i, s := range schemas {
		p, err := Parse(s)
		if err != nil {
			f.Fatal(err)
		}
		parsed[i] = p
	}

	// Adversarial values.
	f.Add(uint8(0), int64(0))
	f.Add(uint8(0), int64(-62135596800000)) // 0001-01-01
	f.Add(uint8(0), int64(253402300800000)) // 9999 AD
	f.Add(uint8(0), int64(1<<62))           // overflow risk
	f.Add(uint8(0), int64(-1<<62))
	f.Add(uint8(2), int64(1))
	f.Add(uint8(3), int64(0))   // epoch
	f.Add(uint8(3), int64(-1))  // pre-epoch date
	f.Add(uint8(4), int64(0))   // midnight
	f.Add(uint8(4), int64(86400000))
	f.Add(uint8(5), int64(86400000000))

	// Track which schemas use "int" wire type vs "long" so the
	// fuzz body can pick the matching Go type without inspecting
	// the *Schema (no public accessor for the underlying kind).
	isInt := []bool{false, false, false, true, true, false}

	f.Fuzz(func(t *testing.T, schemaIdx uint8, val int64) {
		idx := int(schemaIdx) % len(parsed)
		s := parsed[idx]
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic on schema=%s val=%d: %v", s.String(), val, r)
			}
		}()
		var encoded []byte
		var err error
		if isInt[idx] {
			if val < -1<<31 || val > 1<<31-1 {
				return
			}
			encoded, err = s.AppendEncode(nil, int32(val))
		} else {
			encoded, err = s.AppendEncode(nil, val)
		}
		if err != nil {
			return
		}
		var v any
		s.Decode(encoded, &v)
		jsonEncoded, err := s.EncodeJSON(v)
		if err != nil {
			return
		}
		var v2 any
		s.DecodeJSON(jsonEncoded, &v2)
	})
}

// FuzzDepthBounds drives every encode/decode/skip/parse path with
// pathologically deep or cyclic inputs and asserts the library
// terminates with an error rather than panicking, hanging, or
// stack-overflowing. Specifically targets the depth-bound work in
// commits a302d51 and 21006ca: cyclic Go inputs, deeply nested wire
// data, deeply nested schemas, self-referential interfaces.
func FuzzDepthBounds(f *testing.F) {
	// nesting: how many record levels deep to recurse (binary input).
	// arrayCount: how many array blocks to chain (each with one item).
	// schemaDepth: nesting depth for the auto-generated array<array<...>> schema.
	// mode: which subtest to run (0..7).
	f.Add(uint16(2000), uint16(100), uint16(50), uint8(0))
	f.Add(uint16(5000), uint16(500), uint16(2000), uint8(1))
	f.Add(uint16(100), uint16(10), uint16(maxDepth+10), uint8(2))
	f.Add(uint16(50), uint16(50), uint16(10), uint8(3))
	f.Add(uint16(0), uint16(0), uint16(0), uint8(4))
	f.Add(uint16(maxDepth+50), uint16(0), uint16(0), uint8(5))
	f.Add(uint16(10), uint16(maxDepth+50), uint16(0), uint8(6))
	f.Add(uint16(0), uint16(0), uint16(0), uint8(7))

	recursiveSchema := `{"type":"record","name":"Node","fields":[
		{"name":"value","type":"int"},
		{"name":"next","type":["null","Node"]}
	]}`
	rs, err := Parse(recursiveSchema)
	if err != nil {
		f.Fatal(err)
	}
	type node struct {
		Value int32 `avro:"value"`
		Next  *node `avro:"next"`
	}

	f.Fuzz(func(t *testing.T, nesting, arrayCount, schemaDepth uint16, mode uint8) {
		// Hard caps to keep individual fuzz iterations bounded. The
		// schemaDepth cap is tight because encoding/json's recursive
		// parser is O(N²) on nested-array JSON — well past maxDepth
		// we just burn time in the stdlib without exercising more
		// of our code.
		if nesting > 20000 {
			nesting = 20000
		}
		if arrayCount > 5000 {
			arrayCount = 5000
		}
		if schemaDepth > maxDepth+10 {
			schemaDepth = maxDepth + 10
		}
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panicked (mode=%d, n=%d, a=%d, sd=%d): %v",
					mode, nesting, arrayCount, schemaDepth, r)
			}
		}()

		switch mode % 8 {
		case 0:
			// Deeply nested binary into recursive struct.
			var src []byte
			for range int(nesting) {
				src = append(src, 0, 0x02)
			}
			src = append(src, 0)
			var n node
			rs.Decode(src, &n)
		case 1:
			// Deeply nested binary into resolved decode (writer == reader).
			var src []byte
			for range int(nesting) {
				src = append(src, 0, 0x02)
			}
			src = append(src, 0)
			resolved, err := Resolve(rs, rs)
			if err != nil {
				return
			}
			var n node
			resolved.Decode(src, &n)
		case 2:
			// Deeply nested binary skipped via resolve (reader drops "next").
			rdrSchema, err := Parse(`{"type":"record","name":"Node","fields":[{"name":"value","type":"int"}]}`)
			if err != nil {
				return
			}
			resolved, err := Resolve(rs, rdrSchema)
			if err != nil {
				return
			}
			var src []byte
			for range int(nesting) {
				src = append(src, 0, 0x02)
			}
			src = append(src, 0)
			type rR struct{ Value int32 `avro:"value"` }
			var rv rR
			resolved.Decode(src, &rv)
		case 3:
			// Deeply nested JSON into recursive struct.
			var src []byte
			for range int(nesting) {
				src = append(src, []byte(`{"value":0,"next":{"Node":`)...)
			}
			src = append(src, []byte(`{"value":0,"next":null}`)...)
			for range int(nesting) {
				src = append(src, []byte(`}}`)...)
			}
			var n node
			rs.DecodeJSON(src, &n)
		case 4:
			// Cyclic struct encode (binary + JSON) through unsafe fast path.
			n := &node{Value: 1}
			n.Next = n
			rs.AppendEncode(nil, n)
			rs.AppendEncodeJSON(nil, n)
		case 5:
			// Schema-parse depth bound.
			var b strings.Builder
			d := int(schemaDepth)
			if d == 0 {
				d = maxDepth + 50
			}
			for range d {
				b.WriteString(`{"type":"array","items":`)
			}
			b.WriteString(`"int"`)
			for range d {
				b.WriteString(`}`)
			}
			Parse(b.String())
		case 6:
			// Long array-block chain (count > buffer is rejected; this
			// makes many small blocks each terminating with count=0).
			var src []byte
			for range int(arrayCount) {
				src = append(src, 0x02) // count=1
				src = append(src, 0)    // single item: int(0)
			}
			src = append(src, 0) // terminator
			arrSchema, err := Parse(`{"type":"array","items":"int"}`)
			if err != nil {
				return
			}
			var out []int32
			arrSchema.Decode(src, &out)
		case 7:
			// Self-referential `any` against various schemas.
			intS, err := Parse(`"int"`)
			if err != nil {
				return
			}
			nullableS, err := Parse(`["null","int"]`)
			if err != nil {
				return
			}
			var p any
			p = &p
			intS.AppendEncode(nil, p)
			intS.AppendEncodeJSON(nil, p)
			nullableS.AppendEncode(nil, p)
			nullableS.AppendEncodeJSON(nil, p)
		}
	})
}
