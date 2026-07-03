package avro

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// fuzzNamedString / fuzzNamedBytes / fuzzNamedFloat are named-type aliases
// used by FuzzSetValueTargets to exercise the set{Float,Bytes,String}Value
// helper arms that branch on Kind (not on concrete *Type), plus the
// TextUnmarshaler-via-Addr path.
type fuzzNamedString string

type fuzzNamedBytes []byte

type fuzzNamedFloat float64

// fuzzTextThing implements encoding.TextUnmarshaler / TextMarshaler so
// setStringValue's TextUnmarshaler-on-Addr branch fires.
type fuzzTextThing struct{ S string }

func (t *fuzzTextThing) UnmarshalText(b []byte) error { t.S = string(b); return nil }
func (t fuzzTextThing) MarshalText() ([]byte, error)  { return []byte(t.S), nil }

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
		// Decode tolerates non-canonical-but-valid input (any boolean
		// byte, bare unions, whole-number floats, etc.); invalid input
		// is skipped via the return below. Re-encoding produces canonical
		// output, so test canonical idempotence (encode → decode → encode
		// is stable) rather than bit-exact equality with the original.
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
		// Value-level fixpoint, NOT byte-level: Avro map encoding (binary and
		// JSON) iterates Go map keys in randomized, spec-legal order, so a
		// multi-key map re-encodes to a different byte ordering even though
		// the value is identical. fuzzEqual is order-robust (maps) and
		// NaN-robust, still catching real round-trip drift.
		if !fuzzEqual(v1, v2) {
			t.Fatalf("decode∘encode is not a value fixpoint:\n  v1: %#v\n  v2: %#v\n  input: %s\n  encoded1: %s\n  encoded2: %s", v1, v2, input, encoded1, encoded2)
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
		// Value-level fixpoint, NOT byte-level: Avro map encoding iterates Go
		// map keys in randomized, spec-legal order, so a multi-key map
		// re-encodes to a different byte ordering even though the value is
		// identical. fuzzEqual is order-robust (maps) and NaN-robust (a
		// decoded float NaN round-trips to an identical NaN that
		// reflect.DeepEqual would wrongly call unequal).
		if !fuzzEqual(tagged1, tagged2) {
			t.Fatalf("decode∘encode is not a value fixpoint:\n  tagged1: %#v\n  tagged2: %#v\n  encoded1: %x\n  encoded2: %x", tagged1, tagged2, encoded1, encoded2)
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
//
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
		for mode := range uint8(12) {
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
		for mode := range uint8(9) {
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
		for mode := range uint8(16) {
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
		// The concurrency surface is shared-schema state across goroutines,
		// not payload size: cap the fuzz-grown string so a corpus-mutated
		// multi-megabyte b doesn't turn one execution into seconds of
		// memcpy across workers×iterations (observed as the fuzzer's exec
		// counter freezing for whole intervals, risking the -fuzztime
		// shutdown deadline).
		if len(b) > 1024 {
			b = b[:1024]
		}
		workers := 1 + int(n%8)
		// Collect panics from worker goroutines via channel rather than
		// calling t.Errorf directly: testing.T methods other than Log
		// aren't safe for concurrent use from non-test goroutines.
		panicCh := make(chan any, workers)
		done := make(chan struct{}, workers)
		for range workers {
			go func() {
				defer func() {
					if r := recover(); r != nil {
						panicCh <- r
					}
					done <- struct{}{}
				}()
				for j := range 20 {
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
		for range workers {
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
	f.Add(uint8(3), int64(0))  // epoch
	f.Add(uint8(3), int64(-1)) // pre-epoch date
	f.Add(uint8(4), int64(0))  // midnight
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
	// Input-independent schemas/resolutions are built ONCE here: re-parsing
	// and re-resolving them per execution added constant fixture cost to
	// every iteration without exercising anything the first iteration
	// didn't — the per-exec-cost class that starves fuzz workers into
	// missing the coordinator's -fuzztime shutdown deadline.
	resolvedSame, err := Resolve(rs, rs)
	if err != nil {
		f.Fatal(err)
	}
	rdrSchema, err := Parse(`{"type":"record","name":"Node","fields":[{"name":"value","type":"int"}]}`)
	if err != nil {
		f.Fatal(err)
	}
	resolvedDrop, err := Resolve(rs, rdrSchema)
	if err != nil {
		f.Fatal(err)
	}
	arrSchema, err := Parse(`{"type":"array","items":"int"}`)
	if err != nil {
		f.Fatal(err)
	}
	intS, err := Parse(`"int"`)
	if err != nil {
		f.Fatal(err)
	}
	nullableS, err := Parse(`["null","int"]`)
	if err != nil {
		f.Fatal(err)
	}

	f.Fuzz(func(t *testing.T, nesting, arrayCount, schemaDepth uint16, mode uint8) {
		// Hard caps to keep individual fuzz iterations bounded. The depth
		// guard trips at maxDepth, so nesting past maxDepth+margin buys no
		// new coverage — it only linearly burns time building and walking
		// input (at a 20000 cap a single execution averaged tens of
		// milliseconds, sliding the exec rate low enough that a worker
		// could miss the -fuzztime shutdown deadline). The schemaDepth cap
		// is tight because encoding/json's recursive parser is O(N²) on
		// nested-array JSON — well past maxDepth we just burn time in the
		// stdlib without exercising more of our code.
		if nesting > maxDepth+200 {
			nesting = maxDepth + 200
		}
		if arrayCount > 2000 {
			arrayCount = 2000
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
			var n node
			resolvedSame.Decode(src, &n)
		case 2:
			// Deeply nested binary skipped via resolve (reader drops "next").
			var src []byte
			for range int(nesting) {
				src = append(src, 0, 0x02)
			}
			src = append(src, 0)
			type rR struct {
				Value int32 `avro:"value"`
			}
			var rv rR
			resolvedDrop.Decode(src, &rv)
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
			var out []int32
			arrSchema.Decode(src, &out)
		case 7:
			// Self-referential `any` against various schemas.
			var p any
			p = &p
			intS.AppendEncode(nil, p)
			intS.AppendEncodeJSON(nil, p)
			nullableS.AppendEncode(nil, p)
			nullableS.AppendEncodeJSON(nil, p)
		}
	})
}

// fuzzPromoteLogicalPairs enumerates the (writer wire kind, reader
// logical-typed schema) cells that promotionDeserForLogical wraps.
// The fuzz driver picks one cell by index, encodes arbitrary input
// against the writer, then resolves writer→reader and decodes into
// several Go target shapes. Locks the int→long+timestamp-*, int→
// long+time-micros, string→bytes+decimal, string→bytes+big-decimal,
// and bytes→string+uuid promotion-plus-logical paths under fuzz
// inputs — the regression tests pin specific values; this fuzz
// surfaces variants. Without the wrap, the decode produces the raw
// wire type (int64 / []byte / string) instead of the logical-typed
// result (time.Time / *big.Rat / [16]byte).
var fuzzPromoteLogicalPairs = []struct {
	writer    string
	reader    string
	encodeInt bool // writer is "int" (encode int32) vs string/bytes (encode []byte)
}{
	{`"int"`, `{"type":"long","logicalType":"timestamp-millis"}`, true},
	{`"int"`, `{"type":"long","logicalType":"timestamp-micros"}`, true},
	{`"int"`, `{"type":"long","logicalType":"timestamp-nanos"}`, true},
	{`"int"`, `{"type":"long","logicalType":"local-timestamp-millis"}`, true},
	{`"int"`, `{"type":"long","logicalType":"local-timestamp-micros"}`, true},
	{`"int"`, `{"type":"long","logicalType":"local-timestamp-nanos"}`, true},
	{`"int"`, `{"type":"long","logicalType":"time-micros"}`, true},
	{`"string"`, `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, false},
	{`"string"`, `{"type":"bytes","logicalType":"big-decimal"}`, false},
	{`"bytes"`, `{"type":"string","logicalType":"uuid"}`, false},
}

// fuzzPromoteLogicalNesting wraps a primitive (writer, reader) pair in
// each container the resolver dispatches through: top-level, record
// field, array items, map values, and reader-side union branch. The
// logical-conversion wrap must apply uniformly across every nesting,
// so the fuzz covers each container axis.
func fuzzPromoteLogicalNesting(writer, reader string, nesting uint8) (string, string) {
	switch nesting % 5 {
	case 0:
		return writer, reader
	case 1:
		return `{"type":"record","name":"R","fields":[{"name":"x","type":` + writer + `}]}`,
			`{"type":"record","name":"R","fields":[{"name":"x","type":` + reader + `}]}`
	case 2:
		return `{"type":"array","items":` + writer + `}`,
			`{"type":"array","items":` + reader + `}`
	case 3:
		return `{"type":"map","values":` + writer + `}`,
			`{"type":"map","values":` + reader + `}`
	case 4:
		return writer, `["null",` + reader + `]`
	}
	return writer, reader
}

func FuzzPromoteLogical(f *testing.F) {
	// Seeds: one canonical per pair × nesting combo, plus a couple of
	// adversarial wire payloads (varint overflow, length > buffer).
	for idx := uint8(0); idx < uint8(len(fuzzPromoteLogicalPairs)); idx++ {
		for n := range uint8(5) {
			pair := fuzzPromoteLogicalPairs[idx]
			w, _ := fuzzPromoteLogicalNesting(pair.writer, pair.reader, n)
			ws, err := Parse(w)
			if err != nil {
				continue
			}
			var v any
			switch {
			case strings.HasPrefix(w, `"int"`):
				v = int32(1742385600)
			case strings.HasPrefix(w, `"string"`):
				v = "12.34"
			case strings.HasPrefix(w, `"bytes"`):
				v = []byte("550e8400-e29b-41d4-a716-446655440000")
			case strings.Contains(w, `"type":"record"`):
				v = map[string]any{"x": canonicalInputFor(pair)}
			case strings.Contains(w, `"type":"array"`):
				v = []any{canonicalInputFor(pair)}
			case strings.Contains(w, `"type":"map"`):
				v = map[string]any{"k": canonicalInputFor(pair)}
			}
			if v == nil {
				continue
			}
			data, err := ws.AppendEncode(nil, v)
			if err != nil {
				continue
			}
			f.Add(idx, n, data)
		}
	}
	// Adversarial inputs: empty, single byte, varint overflow.
	f.Add(uint8(0), uint8(0), []byte{})
	f.Add(uint8(0), uint8(0), []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01})
	f.Add(uint8(7), uint8(0), []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01})

	f.Fuzz(func(t *testing.T, pairIdx, nestIdx uint8, data []byte) {
		pair := fuzzPromoteLogicalPairs[int(pairIdx)%len(fuzzPromoteLogicalPairs)]
		w, r := fuzzPromoteLogicalNesting(pair.writer, pair.reader, nestIdx)
		writer, err := Parse(w)
		if err != nil {
			return
		}
		reader, err := Parse(r)
		if err != nil {
			return
		}
		resolved, err := Resolve(writer, reader)
		if err != nil {
			return
		}
		// Decode against multiple target shapes — *any (the natural
		// promotion target), a typed container that should accept the
		// logical-typed value, and a deliberately-wrong type that should
		// error (not panic).
		var anyV any
		resolved.Decode(data, &anyV)
		// Typed container: a time.Time field for the timestamp cells,
		// *big.Rat for decimal cells, [16]byte for uuid. The fuzz only
		// cares that this never panics; mismatched promotions produce
		// errors but those are fine.
		switch nestIdx % 5 {
		case 1: // record
			switch pair.reader {
			case fuzzPromoteLogicalPairs[0].reader, fuzzPromoteLogicalPairs[1].reader, fuzzPromoteLogicalPairs[2].reader:
				var typed struct {
					X time.Time `avro:"x"`
				}
				resolved.Decode(data, &typed)
			case fuzzPromoteLogicalPairs[7].reader, fuzzPromoteLogicalPairs[8].reader:
				var typed struct {
					X *big.Rat `avro:"x"`
				}
				resolved.Decode(data, &typed)
			case fuzzPromoteLogicalPairs[9].reader:
				var typed struct {
					X [16]byte `avro:"x"`
				}
				resolved.Decode(data, &typed)
			}
		case 0: // top-level scalar
			var typedTime time.Time
			resolved.Decode(data, &typedTime)
			var typedRat *big.Rat
			resolved.Decode(data, &typedRat)
			var typedUUID [16]byte
			resolved.Decode(data, &typedUUID)
		}
	})
}

func canonicalInputFor(p struct {
	writer    string
	reader    string
	encodeInt bool
}) any {
	if p.encodeInt {
		return int32(1742385600)
	}
	if strings.HasPrefix(p.writer, `"bytes"`) {
		return []byte("550e8400-e29b-41d4-a716-446655440000")
	}
	return "12.34"
}

// FuzzBareSpecialFloat exercises the JSON decoder's bare-token path
// for NaN/Infinity/-Infinity (the unquoted form fastavro and
// python's json.dumps(..., allow_nan=True) emit). consumeBareSpecial-
// Float is reached only when the decoder hits a non-quote/-digit/-null
// token at a float/double position; the existing FuzzDecodeJSON seeds
// only have the quoted form. Coverage includes top-level + nested
// (record field, array element, map value, union branch) so the
// recursive descent's "peek a non-quote at a float position" arm is
// hit from every context.
func FuzzBareSpecialFloat(f *testing.F) {
	floatSchema := MustParse(`"float"`)
	doubleSchema := MustParse(`"double"`)
	recordSchema := MustParse(`{"type":"record","name":"R","fields":[{"name":"f","type":"float"},{"name":"d","type":"double"}]}`)
	arrayFloat := MustParse(`{"type":"array","items":"float"}`)
	mapDouble := MustParse(`{"type":"map","values":"double"}`)
	unionFloat := MustParse(`["null","float"]`)
	unionDouble := MustParse(`["null","double"]`)

	schemas := []*Schema{floatSchema, doubleSchema, recordSchema, arrayFloat, mapDouble, unionFloat, unionDouble}

	// Tokens: every casing + sign + word the lenient path must accept,
	// plus things that look almost-right but should error cleanly.
	tokens := []string{
		"NaN", "nan", "NAN", "Nan",
		"Infinity", "infinity", "INFINITY", "Inf", "inf",
		"-Infinity", "-infinity", "-Inf", "-inf",
		"+Infinity", "Inf inity", "InfX", "nul", "-",
		"NaNNaN",
	}

	// Seed each schema with bare tokens at the appropriate position.
	for tIdx := range tokens {
		f.Add(uint8(0), uint8(tIdx), uint8(0)) // top-level float
		f.Add(uint8(1), uint8(tIdx), uint8(0)) // top-level double
		f.Add(uint8(2), uint8(tIdx), uint8(0)) // record (both fields)
		f.Add(uint8(2), uint8(tIdx), uint8(1)) // record (just f)
		f.Add(uint8(3), uint8(tIdx), uint8(0)) // array of float
		f.Add(uint8(4), uint8(tIdx), uint8(0)) // map of double
		f.Add(uint8(5), uint8(tIdx), uint8(0)) // union float
		f.Add(uint8(6), uint8(tIdx), uint8(0)) // union double
	}

	f.Fuzz(func(t *testing.T, schemaIdx, tokenIdx, variant uint8) {
		s := schemas[int(schemaIdx)%len(schemas)]
		tok := tokens[int(tokenIdx)%len(tokens)]
		var input string
		switch schemaIdx % uint8(len(schemas)) {
		case 0, 1: // bare float / double
			input = tok
		case 2: // record
			if variant&1 == 0 {
				input = `{"f":` + tok + `,"d":` + tok + `}`
			} else {
				input = `{"f":` + tok + `,"d":1.0}`
			}
		case 3: // array of float
			input = `[` + tok + `,1.0,` + tok + `]`
		case 4: // map of double
			input = `{"a":` + tok + `,"b":2.0}`
		case 5, 6: // union (bare branch — the new path)
			input = tok
		}
		var v any
		s.DecodeJSON([]byte(input), &v)
		// Sanity: if decode succeeded against a top-level float/double,
		// EncodeJSON must round-trip without panicking. The encoder's
		// canonical form is the quoted variant, so re-decode of the
		// canonical form should land on the same value (NaN-aware).
		if schemaIdx <= 1 {
			if v == nil {
				return
			}
			out, err := s.EncodeJSON(v)
			if err != nil {
				return
			}
			var v2 any
			if err := s.DecodeJSON(out, &v2); err != nil {
				t.Fatalf("re-decode of canonical encoded failed: %v\n  in: %q\n  enc: %q", err, input, out)
			}
			if !fuzzEqual(v, v2) {
				t.Fatalf("round-trip mismatch:\n  v1: %#v\n  v2: %#v\n  input: %q\n  enc: %q", v, v2, input, out)
			}
		}
	})
}

// FuzzBytesFixedUTF8RoundTrip exercises the JSON encoder bytes/fixed
// arms that take Go strings as input. The JSON bytes/fixed arms route
// through avroStringValue so the wire form is codepoint-per-byte and
// round-trips; without this routing, Encode("é") against avro "bytes"
// would serialize the UTF-8 bytes c3 a9 on binary but emit the
// pre-mapping codepoint string "é" on JSON, producing JSON byte
// strings that re-decode to two-codepoint garbage. The fuzz seeds
// cover multibyte runes (2/3/4-byte UTF-8) inside arrays, maps,
// unions, records, and verifies binary↔JSON parity: encoding the
// same input through both paths and decoding back must produce the
// same Go value.
func FuzzBytesFixedUTF8RoundTrip(f *testing.F) {
	// Fixed sizes that fit common rune lengths.
	fixed2 := MustParse(`{"type":"fixed","name":"F2","size":2}`)
	fixed3 := MustParse(`{"type":"fixed","name":"F3","size":3}`)
	bytesSchema := MustParse(`"bytes"`)
	arrayBytes := MustParse(`{"type":"array","items":"bytes"}`)
	mapBytes := MustParse(`{"type":"map","values":"bytes"}`)
	unionBytes := MustParse(`["null","bytes"]`)
	recordBytesFixed := MustParse(`{"type":"record","name":"R","fields":[
		{"name":"b","type":"bytes"},
		{"name":"f","type":{"type":"fixed","name":"FF","size":3}}
	]}`)

	schemas := []*Schema{bytesSchema, fixed2, fixed3, arrayBytes, mapBytes, unionBytes, recordBytesFixed}

	// Multibyte fragments — every encoded byte must survive the JSON
	// pipeline as a code-point character. The encoded UTF-8 byte length
	// is exactly the size baked into the fixed schemas.
	frags := []string{
		"é",     // 'é' (2 bytes: c3 a9) — fits fixed2
		"€",     // '€' (3 bytes: e2 82 ac) — fits fixed3
		"ñ",     // 'ñ' (2 bytes) — fits fixed2
		"ÿ",     // 'ÿ' (2 bytes) — fits fixed2
		"À\xa9", // raw 0xc0 0xa9 — invalid UTF-8 but the bytes path
		// must still survive the round-trip (encoder canonicalizes via
		// replacement char if necessary)
		"abc", // ASCII (3 bytes) — fits fixed3
	}

	for sIdx := range schemas {
		for fIdx := range frags {
			f.Add(uint8(sIdx), uint8(fIdx))
		}
	}

	f.Fuzz(func(t *testing.T, schemaIdx, fragIdx uint8) {
		idx := int(schemaIdx) % len(schemas)
		s := schemas[idx]
		frag := frags[int(fragIdx)%len(frags)]
		// Build an input shaped for the chosen schema.
		var v any
		switch idx {
		case 0: // "bytes"
			v = frag
		case 1: // fixed(2)
			if len(frag) != 2 {
				return
			}
			v = frag
		case 2: // fixed(3)
			if len(frag) != 3 {
				return
			}
			v = frag
		case 3: // array of bytes
			v = []string{frag, frag}
		case 4: // map of bytes
			v = map[string]string{"k": frag}
		case 5: // union [null, bytes]
			v = frag
		case 6: // record with bytes + fixed(3)
			if len(frag) != 3 {
				return
			}
			v = map[string]any{"b": frag, "f": frag}
		}
		binWire, binErr := s.AppendEncode(nil, v)
		jsonWire, jsonErr := s.AppendEncodeJSON(nil, v)
		if binErr != nil || jsonErr != nil {
			return
		}
		var binDec, jsonDec any
		if _, err := s.Decode(binWire, &binDec); err != nil {
			t.Fatalf("Decode after AppendEncode failed: %v\n  v=%#v frag=%q", err, v, frag)
		}
		if err := s.DecodeJSON(jsonWire, &jsonDec); err != nil {
			t.Fatalf("DecodeJSON after AppendEncodeJSON failed: %v\n  v=%#v frag=%q jsonWire=%q", err, v, frag, jsonWire)
		}
		// Parity claim: binary-decoded and JSON-decoded results must
		// match for the bytes/fixed inputs. If JSON drops/munges the
		// multibyte sequence, fuzzEqual fails. NaN-aware comparator
		// suffices — no floats in this fuzz.
		if !fuzzEqual(binDec, jsonDec) {
			t.Fatalf("binary/JSON decode mismatch:\n  bin:  %#v\n  json: %#v\n  v:    %#v\n  frag: %q\n  binWire=%x\n  jsonWire=%q",
				binDec, jsonDec, v, frag, binWire, jsonWire)
		}
	})
}

// FuzzOCFBlockEnvelope is an ocf-package counterpart that lives here
// for proximity to FuzzOCFReader; the ocf-side variant is in
// ocf/fuzz_test.go. This fuzz target is in the avro package and
// exercises the avro.Schema decode path through OCF-style block
// framing: a (count, size, data, sync) envelope. It targets the
// readBlock's count=0 sync-validation path indirectly by encoding
// arbitrary count/size combinations the reader has to navigate;
// the ocf-side fuzz (FuzzOCFBlockEnvelope) wraps this in a full OCF.
// Kept here so devs can see all fuzz coverage in one place.
//
// (Implementation: see ocf/fuzz_test.go for the real fuzz target —
// this comment block documents the cross-package coverage map.)

// FuzzSetValueTargets fuzzes Decode/DecodeJSON across the new
// set{Float,Bytes,String}Value target arms with adversarial Go target
// types: named types, TextUnmarshaler-via-Addr, json.Number, big.Rat,
// and *big.Rat. The pre-existing FuzzDecodeVariedTargets only exercises
// shapes (*any / *map[string]any / interface{Foo()}), not named types.
// Bugs in the helper arms (e.g. forgetting to handle a named uint8-slice
// or a TextUnmarshaler value-receiver vs pointer-receiver) would not
// surface in the existing fuzzer.
func FuzzSetValueTargets(f *testing.F) {
	// Schemas where the new helpers fire.
	floatS := MustParse(`"float"`)
	doubleS := MustParse(`"double"`)
	bytesS := MustParse(`"bytes"`)
	fixedS := MustParse(`{"type":"fixed","name":"F","size":4}`)
	stringS := MustParse(`"string"`)

	// makeTarget: pick a Go target by mode. Every target is a
	// pointer-to-T so Decode/DecodeJSON can write through.
	makeTarget := func(mode uint8) any {
		switch mode % 14 {
		case 0:
			var v fuzzNamedFloat
			return &v
		case 1:
			var v fuzzNamedBytes
			return &v
		case 2:
			var v fuzzNamedString
			return &v
		case 3:
			var v fuzzTextThing
			return &v
		case 4:
			var v json.Number
			return &v
		case 5:
			var v big.Rat
			return &v
		case 6:
			var v *big.Rat
			return &v
		case 7:
			// Pointer to named alias of a float — exercises the
			// pointer-indirect arm of setFloatValue against named types.
			var v *fuzzNamedFloat
			return &v
		case 8:
			// [4]uint8 — exercises the fixed-array arm of setBytesValue.
			var v [4]byte
			return &v
		case 9:
			// [16]byte — UUID-style fixed target for bytes-as-string-uuid
			// promotions when reader is uuid.
			var v [16]byte
			return &v
		case 10:
			// Interface targets that should fall through to v.Set on the
			// kind=Interface arm.
			var v any
			return &v
		case 11:
			// Pointer-to-pointer — the indirectAlloc chain.
			var v **string
			return &v
		case 12:
			// uint64 — exercises setLongValue overflow / setFloatValue
			// CanUint arm.
			var v uint64
			return &v
		case 13:
			// Map-keyed-by-named-string — exercises mapKeyAs.
			var v map[fuzzNamedString]string
			return &v
		}
		var v any
		return &v
	}

	schemas := []*Schema{floatS, doubleS, bytesS, fixedS, stringS}

	// Seed every (schema, target) combo with valid + empty wire.
	for sIdx := range schemas {
		for m := range uint8(14) {
			f.Add(uint8(sIdx), m, []byte{})
			f.Add(uint8(sIdx), m, []byte{0})
			// A 4-byte fixed seed (legal for fixedS, harmless for the
			// rest — the fuzz body only cares about no-panic).
			f.Add(uint8(sIdx), m, []byte{1, 2, 3, 4})
		}
	}
	// One canonical encoded value per schema.
	f.Add(uint8(0), uint8(0), fuzzSeed(floatS, float32(1.5)))
	f.Add(uint8(1), uint8(0), fuzzSeed(doubleS, float64(2.5)))
	f.Add(uint8(2), uint8(1), fuzzSeed(bytesS, []byte{1, 2, 3, 4}))
	f.Add(uint8(3), uint8(8), fuzzSeed(fixedS, [4]byte{9, 8, 7, 6}))
	f.Add(uint8(4), uint8(3), fuzzSeed(stringS, "hello"))

	f.Fuzz(func(t *testing.T, schemaIdx, mode uint8, data []byte) {
		s := schemas[int(schemaIdx)%len(schemas)]
		tgt := makeTarget(mode)
		s.Decode(data, tgt)
		tgt2 := makeTarget(mode)
		s.DecodeJSON(data, tgt2)
	})
}

// FuzzFindUnionBranch fuzzes the (kind, logical) pair-match fallback
// in findUnionBranch via DecodeJSON inputs against unions that have
// ambiguous shapes: two same-kind branches that differ only by
// logical type. Pre-tightening, the fallback matched on kind alone
// and routed the tag to the first kind-match — silently dropping the
// logical conversion. Now (kind, logical) must match together. The
// fuzz seeds cover positive matches (the tag finds the right branch),
// negative matches (no branch should match, error not panic), and
// ambiguity (two branches differ only by namespace short-name).
func FuzzFindUnionBranch(f *testing.F) {
	// Schemas exercising every fallback class. Per spec a union may not
	// contain two schemas with the same primitive type even if their
	// logical types differ, so the same-kind disambiguation surface is
	// exercised by single-branch unions paired with adversarial tag
	// inputs (the wrong tag must miss). Fixed branches differ by named
	// type so the same-kind, different-logical-type case is reachable
	// for "fixed" kind only.
	// 0: single long+timestamp-millis (logical-tag match positive)
	// 1: plain long (logical-tag-on-plain miss case)
	// 2: two fixed branches differing by logical type (same kind, same-
	//    kind pair-match: only legal for fixed)
	// 3: two records with the same short name in different namespaces
	//    (short-name-leniency ambiguity guard)
	// 4: enum + record (short-name fallback)
	unions := []string{
		`[{"type":"long","logicalType":"timestamp-millis"}]`,
		`["long"]`,
		`[{"type":"fixed","name":"F","size":16,"logicalType":"uuid"},{"type":"fixed","name":"F2","size":12,"logicalType":"duration"}]`,
		`[{"type":"record","name":"a.R","fields":[{"name":"v","type":"int"}]},{"type":"record","name":"b.R","fields":[{"name":"v","type":"string"}]}]`,
		`[{"type":"enum","name":"E","symbols":["A","B"]},{"type":"record","name":"R","fields":[{"name":"v","type":"int"}]}]`,
	}
	parsed := make([]*Schema, len(unions))
	for i, u := range unions {
		parsed[i] = MustParse(u)
	}

	tags := []string{
		"long",
		"long.timestamp-millis",
		"long.timestamp-micros",
		"long.timestamp-nanos",
		"F.uuid",
		"F.duration",
		"F2.uuid",
		"F2.duration",
		"F",
		"F2",
		"R",
		"a.R",
		"b.R",
		"E",
		"null",
		"bogus",
		"long.",
		".timestamp-millis",
		"...",
	}
	values := []string{
		`1700000000000`,
		`"550e8400-e29b-41d4-a716-446655440000"`,
		`"AAAAAAAAAAAAAAAAAAAA"`, // 16 bytes codepoint-mapped
		`{"v":1}`,
		`{"v":"x"}`,
		`"A"`,
		`null`,
	}

	for u := uint8(0); u < uint8(len(parsed)); u++ {
		for tIdx := range tags {
			for vIdx := range values {
				f.Add(u, uint8(tIdx), uint8(vIdx))
			}
		}
	}

	f.Fuzz(func(t *testing.T, unionIdx, tagIdx, valIdx uint8) {
		s := parsed[int(unionIdx)%len(parsed)]
		tag := tags[int(tagIdx)%len(tags)]
		val := values[int(valIdx)%len(values)]
		// Tagged-union input: {"tag": val}
		// Use json.Marshal on the tag to ensure quoting/escaping is valid.
		tagBytes, err := json.Marshal(tag)
		if err != nil {
			return
		}
		input := []byte(`{` + string(tagBytes) + `:` + val + `}`)
		var v any
		s.DecodeJSON(input, &v)
		// Also the wrapped TaggedUnions form should never panic on the
		// re-decode of the same payload.
		s.DecodeJSON(input, &v, TaggedUnions())
	})
}

// FuzzUnionBranchErrorWrapping locks the decodeUnionObject / decode-
// UnionBare error wrapping. The fuzz only asserts no panics — the
// error-message check belongs to a regression test, not to fuzz.
// A target-type mismatch inside a matched tagged-union branch must
// preserve the underlying error via errors.Is/Unwrap rather than
// surface the generic "no union branch matched at offset N" message
// that hides the real cause. Fuzz here exercises every (union shape,
// tagged/bare input, target shape) combination to surface any panic
// path.
func FuzzUnionBranchErrorWrapping(f *testing.F) {
	unions := []*Schema{
		MustParse(`["null","int"]`),
		MustParse(`["null","int","string"]`),
		MustParse(`[{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]},"string"]`),
		MustParse(`[{"type":"long","logicalType":"timestamp-millis"},"string"]`),
	}
	inputs := []string{
		`null`, `42`, `"x"`, `true`, `[]`, `{}`,
		`{"int":1}`, `{"int":"x"}`, `{"null":null}`,
		`{"long.timestamp-millis":"not-a-number"}`,
		`{"R":{"x":1}}`, `{"R":{"x":"wrong"}}`,
		`{"unknown":1}`, `{"a.b.c":1}`,
	}
	type stringErr struct {
		V string `avro:"v"`
	}

	makeTarget := func(mode uint8) any {
		switch mode % 6 {
		case 0:
			var v any
			return &v
		case 1:
			var v int32
			return &v
		case 2:
			var v string
			return &v
		case 3:
			var v stringErr
			return &v
		case 4:
			var v map[string]any
			return &v
		case 5:
			var v *time.Time
			return &v
		}
		var v any
		return &v
	}

	for uIdx := range unions {
		for iIdx := range inputs {
			for m := range uint8(6) {
				f.Add(uint8(uIdx), uint8(iIdx), m)
			}
		}
	}

	f.Fuzz(func(t *testing.T, uIdx, iIdx, mode uint8) {
		s := unions[int(uIdx)%len(unions)]
		input := inputs[int(iIdx)%len(inputs)]
		tgt := makeTarget(mode)
		s.DecodeJSON([]byte(input), tgt)
		// And TaggedUnions mode.
		tgt2 := makeTarget(mode)
		s.DecodeJSON([]byte(input), tgt2, TaggedUnions())
	})
}

// FuzzResolveUnionUnionTags exercises resolveUnionUnion's reader-side
// branch-name path under TaggedUnions. Resolve(["null","int"] →
// ["null","long"]) decoded into *any with TaggedUnions must emit
// {"long":42} (reader-side branch name) — not {"int":42} (writer-
// side). The fuzz drives Resolve across writer×reader union pairs,
// encodes a value against the writer, resolves+decodes with
// TaggedUnions, and verifies the tagged map's key names a reader-side
// branch (or one of the documented short-name fallbacks). Bug
// surfaces would be: a tag key that doesn't match any reader branch,
// or a non-map return.
func FuzzResolveUnionUnionTags(f *testing.F) {
	type seed struct {
		writer, reader string
		val            any
	}
	seeds := []seed{
		{`["null","int"]`, `["null","long"]`, int32(42)},
		{`["null","int","string"]`, `["null","long","string"]`, int32(7)},
		{`["null","int","string"]`, `["null","long","string"]`, "hi"},
		{`["int","long"]`, `["long","float"]`, int32(1)},
		{`["int","string"]`, `["long","string"]`, "x"},
		{`["null",{"type":"int","logicalType":"date"}]`, `["null",{"type":"long","logicalType":"timestamp-millis"}]`, int32(19000)},
		{`["int"]`, `["long"]`, int32(99)},
	}
	for _, s := range seeds {
		ws, err := Parse(s.writer)
		if err != nil {
			continue
		}
		data, err := ws.AppendEncode(nil, s.val)
		if err != nil {
			continue
		}
		f.Add(s.writer, s.reader, data)
	}
	// Adversarial: empty + huge varint.
	f.Add(`["null","int"]`, `["null","long"]`, []byte{})
	f.Add(`["null","int"]`, `["null","long"]`, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x0F})

	f.Fuzz(func(t *testing.T, writerJSON, readerJSON string, data []byte) {
		w, err := Parse(writerJSON)
		if err != nil {
			return
		}
		r, err := Parse(readerJSON)
		if err != nil {
			return
		}
		resolved, err := Resolve(w, r)
		if err != nil {
			return
		}
		var got any
		if _, err := resolved.Decode(data, &got, TaggedUnions()); err != nil {
			return
		}
		// got is either nil (null branch) or map[string]any{<tag>: value}.
		// The tag MUST name a reader-side branch; if not, a regression
		// has been re-introduced.
		if got == nil {
			return
		}
		m, ok := got.(map[string]any)
		if !ok {
			t.Fatalf("TaggedUnions decode returned non-map: %T (%v)", got, got)
		}
		if len(m) != 1 {
			t.Fatalf("TaggedUnions map has %d keys, expected 1: %v", len(m), m)
		}
		var key string
		for k := range m {
			key = k
		}
		ok = slices.Contains(readerBranchTags(readerJSON), key)
		if !ok {
			t.Fatalf("TaggedUnions key %q not found in reader schema %s", key, readerJSON)
		}
	})
}

// readerBranchTags returns the legal tagged-union key forms for each
// branch in unionJSON. Used by FuzzResolveUnionUnionTags to validate
// the reader-side tag claim without re-implementing the encoder's
// naming rules. Returns nil if the schema isn't a union.
func readerBranchTags(unionJSON string) []string {
	s, err := Parse(unionJSON)
	if err != nil {
		return nil
	}
	root := s.Root()
	if len(root.Branches) == 0 {
		return []string{branchTagFor(root)}
	}
	tags := make([]string, 0, len(root.Branches))
	for i := range root.Branches {
		tags = append(tags, branchTagFor(root.Branches[i]))
	}
	return tags
}

// branchTagFor returns the standard binary-TaggedUnions key form. This
// matches unionBranchName in the codec: primitives use the kind alone
// (without logical-type qualifier — that qualifier only applies to the
// JSON-side TagLogicalTypes form). Named types use their short name.
func branchTagFor(n SchemaNode) string {
	switch n.Type {
	case "null":
		return "null"
	case "boolean", "int", "long", "float", "double", "bytes", "string":
		return n.Type
	case "record", "enum", "fixed":
		return n.Name
	default:
		return n.Type
	}
}

// FuzzDecodeUnionObjectDeep stresses the depth-tracked recursive
// descent through decodeUnionObject / decodeUnionBare with cyclic
// JSON inputs. The errTooDeep propagation must not be masked by the
// "try tagged then bare" fallback — errors.Is(err, errTooDeep)
// short-circuits before the bare retry, otherwise the tagged-side
// errTooDeep would be caught and the bare-side retry would burn
// more depth. Fuzz over deeply nested {"tag":{"tag":{...}}} sequences
// and assert the library terminates (no panic, no stack overflow).
func FuzzDecodeUnionObjectDeep(f *testing.F) {
	recursiveSchema := MustParse(`{"type":"record","name":"Node","fields":[
		{"name":"value","type":"int"},
		{"name":"next","type":["null","Node"]}
	]}`)
	// Seeds: short, medium, deeper-than-maxDepth.
	f.Add(uint16(10))
	f.Add(uint16(100))
	f.Add(uint16(maxDepth - 2))
	f.Add(uint16(maxDepth + 5))
	f.Add(uint16(maxDepth + 100))

	f.Fuzz(func(t *testing.T, depth uint16) {
		if depth > maxDepth+200 {
			depth = maxDepth + 200
		}
		// Build {"value":0,"next":{"Node":{"value":0,"next":{"Node":...}}}}
		var b strings.Builder
		for range int(depth) {
			b.WriteString(`{"value":0,"next":{"Node":`)
		}
		b.WriteString(`{"value":0,"next":null}`)
		for range int(depth) {
			b.WriteString(`}}`)
		}
		var n struct {
			Value int32 `avro:"value"`
			Next  any   `avro:"next"`
		}
		recursiveSchema.DecodeJSON([]byte(b.String()), &n)
	})
}

// FuzzNumberCarriers fuzzes the json.Number / *big.Rat / *big.Int /
// *big.Float carrier surface across primitive Avro types. These
// carriers are reachable via setFloatValue (jsonNumberType branch),
// setDecimalRat, and the big-decimal payload path. The fuzz seeds
// each carrier with adversarial numeric strings ("1e1000", "NaN",
// "0.0000000000000000000000000001", "9".Repeat(40)) and asserts no
// panic on encode or decode.
// safeForBigNum reports whether s is small enough to hand to the stdlib
// big.Rat.SetString / big.ParseFloat parsers without risking a multi-minute
// or multi-gigabyte materialization. Those parsers eagerly build the full
// mantissa and 10^exponent, so a 20-million-digit mantissa costs big.Rat
// ~8 minutes and a short "1e2000000000" allocates gigabytes. twmb's own
// numeric entry points are bounded (maxRatInputLen / decimalScaleLimit);
// this mirrors that bound for the fuzzer's DIRECT stdlib construction so the
// harness cannot DoS itself. (twmb's json.Number path above is exercised
// unbounded since it is internally capped.)
func safeForBigNum(s string) bool {
	if len(s) > 1024 {
		return false
	}
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		exp := strings.TrimLeft(s[i+1:], "+-")
		if len(exp) > 4 { // |exponent| could exceed 9999 → huge 10^exp
			return false
		}
	}
	return true
}

func FuzzNumberCarriers(f *testing.F) {
	floatS := MustParse(`"float"`)
	doubleS := MustParse(`"double"`)
	longS := MustParse(`"long"`)
	intS := MustParse(`"int"`)
	decimalS := MustParse(`{"type":"bytes","logicalType":"decimal","precision":20,"scale":4}`)
	bigDecimalS := MustParse(`{"type":"bytes","logicalType":"big-decimal"}`)

	schemas := []*Schema{floatS, doubleS, longS, intS, decimalS, bigDecimalS}

	for sIdx := range schemas {
		f.Add(uint8(sIdx), "1")
		f.Add(uint8(sIdx), "0")
		f.Add(uint8(sIdx), "-1")
		f.Add(uint8(sIdx), "1.5")
		f.Add(uint8(sIdx), "1e10")
		f.Add(uint8(sIdx), "1e1000")
		f.Add(uint8(sIdx), "-1e-1000")
		f.Add(uint8(sIdx), "NaN")
		f.Add(uint8(sIdx), "Infinity")
		f.Add(uint8(sIdx), strings.Repeat("9", 40))
		f.Add(uint8(sIdx), "0."+strings.Repeat("0", 100)+"1")
		f.Add(uint8(sIdx), "")
	}

	f.Fuzz(func(t *testing.T, schemaIdx uint8, numStr string) {
		s := schemas[int(schemaIdx)%len(schemas)]
		// As json.Number: twmb's own numeric entry points are length- and
		// magnitude-bounded (maxRatInputLen / maxParseFloatLen /
		// decimalScaleLimit), so any input is safe to hand to twmb here.
		s.AppendEncode(nil, json.Number(numStr))
		s.AppendEncodeJSON(nil, json.Number(numStr))
		// As *big.Rat / *big.Float the fuzzer builds the value DIRECTLY via
		// stdlib parsers, which — unlike twmb — eagerly materialize the full
		// mantissa and 10^exp: a 20-million-digit mantissa takes big.Rat
		// ~8 minutes, and "1e2000000000" allocates gigabytes, OOM-ing the
		// worker. Bound the input the way twmb itself does so the fuzzer
		// exercises twmb's big.Rat/big.Float handling without DoSing its own
		// harness (a 13-byte input could otherwise hang the whole run).
		if !safeForBigNum(numStr) {
			return
		}
		r := new(big.Rat)
		if _, ok := r.SetString(numStr); ok {
			s.AppendEncode(nil, r)
			s.AppendEncodeJSON(nil, r)
		}
		bf, _, err := big.ParseFloat(numStr, 10, 100, big.ToNearestEven)
		if err == nil {
			s.AppendEncode(nil, bf)
		}
	})
}

// emit binary into the silence-the-unused-import floor; only used by
// FuzzOCFBlockEnvelope's avro-side helpers, kept here so the import
// is referenced unconditionally.
var _ = errors.New
var _ = binary.AppendVarint
