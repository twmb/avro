package avro

import (
	"bytes"
	"math"
	"reflect"
	"testing"
)

// fuzzSchemas contains pre-compiled schemas covering all Avro types for use
// in fuzz targets that exercise decoding.
var fuzzSchemas []*Schema

func init() {
	schemas := []string{
		// 8 primitives
		`"null"`,
		`"boolean"`,
		`"int"`,
		`"long"`,
		`"float"`,
		`"double"`,
		`"bytes"`,
		`"string"`,
		// enum
		`{"type":"enum","name":"E","symbols":["A","B","C"]}`,
		// fixed
		`{"type":"fixed","name":"F","size":4}`,
		// array
		`{"type":"array","items":"int"}`,
		// map
		`{"type":"map","values":"string"}`,
		// null union
		`["null","string"]`,
		// general union
		`["null","int","string","boolean"]`,
		// multi-field record
		`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"},{"name":"c","type":"boolean"},{"name":"d","type":"double"}]}`,
		// nested record
		`{"type":"record","name":"Outer","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"},{"name":"y","type":"string"}]}},{"name":"z","type":"long"}]}`,
		// record with logical types
		`{"type":"record","name":"Logical","fields":[{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"d","type":{"type":"int","logicalType":"date"}},{"name":"id","type":{"type":"string","logicalType":"uuid"}}]}`,
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
		Parse(schema) //nolint:errcheck
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
		s.Decode(data, &v) //nolint:errcheck
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
		SingleObjectFingerprint(data) //nolint:errcheck
		for _, s := range fuzzSchemas {
			var v any
			s.DecodeSingleObject(data, &v) //nolint:errcheck
		}
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
		resolved.Decode(data, &v) //nolint:errcheck
	})
}
