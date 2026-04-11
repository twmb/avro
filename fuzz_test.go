package avro

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
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
		var v1 any
		if err := s.DecodeJSON([]byte(input), &v1); err != nil {
			return
		}
		encoded, err := s.EncodeJSON(v1)
		if err != nil {
			return
		}
		var v2 any
		if err := s.DecodeJSON(encoded, &v2); err != nil {
			t.Fatalf("re-decode failed: %v\n  input: %s\n  encoded: %s", err, input, encoded)
		}
		if !fuzzEqual(v1, v2) {
			t.Fatalf("round-trip mismatch:\n  v1: %#v\n  v2: %#v\n  input: %s\n  encoded: %s", v1, v2, input, encoded)
		}
	})
}

// FuzzEncodeTaggedUnion verifies that Encode accepts tagged union maps
// from Decode(TaggedUnions) and produces identical binary.
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
		var tagged any
		rem, err := s.Decode(data, &tagged, TaggedUnions())
		if err != nil || len(rem) != 0 {
			return
		}
		reencoded, err := s.Encode(tagged)
		if err != nil {
			return
		}
		if !bytes.Equal(data, reencoded) {
			t.Fatalf("tagged round-trip mismatch:\n  original: %x\n  reencoded: %x", data, reencoded)
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
		`{"a":1}`,                      // b, c, d all missing
		`{"a":1,"b":5}`,                // c, d missing
		`{"a":1,"c":"only c"}`,         // b, d missing
		`{"a":1,"b":null,"c":null}`,    // explicit nulls
		`{"b":1,"c":"x"}`,              // missing required 'a'
		`{"a":"wrong type"}`,           // wrong type
		`{"a":1,"extra":"ignored"}`,    // extra key
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
