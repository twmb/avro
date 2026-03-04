package conformance

import (
	"bytes"
	"math"
	"testing"
)

// -----------------------------------------------------------------------
// Binary Encoding Correctness
// Spec: "Data Serialization" — encoding of primitive and complex types.
//   - int/long:  variable-length zigzag encoding
//   - float:     4 bytes, little-endian IEEE 754
//   - double:    8 bytes, little-endian IEEE 754
//   - string:    long-encoded length + UTF-8 bytes
//   - bytes:     long-encoded length + raw bytes
//   - array/map: sequence of blocks; block = long count + items; 0 terminates
//   - enum:      int-encoded symbol index
//   - union:     int-encoded branch index + value
// https://avro.apache.org/docs/1.12.0/specification/#binary-encoding
// -----------------------------------------------------------------------

func TestSpecZigzagBoundaryValues(t *testing.T) {
	tests := []struct {
		name     string
		schema   string
		value    any
		expected []byte
	}{
		// int (varint, 32-bit zigzag)
		{"int 0", `"int"`, new(int32), []byte{0x00}},
		{"int -1", `"int"`, ptr(int32(-1)), []byte{0x01}},
		{"int 1", `"int"`, ptr(int32(1)), []byte{0x02}},
		{"int -2", `"int"`, ptr(int32(-2)), []byte{0x03}},
		{"int 2", `"int"`, ptr(int32(2)), []byte{0x04}},
		{"int MaxInt32", `"int"`, ptr(int32(math.MaxInt32)), []byte{0xFE, 0xFF, 0xFF, 0xFF, 0x0F}},
		{"int MinInt32", `"int"`, ptr(int32(math.MinInt32)), []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x0F}},

		// long (varlong, 64-bit zigzag)
		{"long 0", `"long"`, new(int64), []byte{0x00}},
		{"long -1", `"long"`, ptr(int64(-1)), []byte{0x01}},
		{"long 1", `"long"`, ptr(int64(1)), []byte{0x02}},
		{"long MaxInt64", `"long"`, ptr(int64(math.MaxInt64)),
			[]byte{0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}},
		{"long MinInt64", `"long"`, ptr(int64(math.MinInt64)),
			[]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := encode(t, tt.schema, tt.value)
			if !bytes.Equal(got, tt.expected) {
				t.Fatalf("encode %s: got %x, want %x", tt.name, got, tt.expected)
			}
		})
	}
}

func TestSpecUnionIndexEncoding(t *testing.T) {
	t.Run("two branch null union via record", func(t *testing.T) {
		type W struct {
			V *int32 `avro:"v"`
		}
		schema := `{"type":"record","name":"W","fields":[{"name":"v","type":["null","int"]}]}`

		dst := encode(t, schema, &W{V: nil})
		if !bytes.Equal(dst, []byte{0x00}) {
			t.Fatalf("null branch: got %x, want 00", dst)
		}

		v := int32(42)
		dst = encode(t, schema, &W{V: &v})
		if dst[0] != 0x02 {
			t.Fatalf("int branch index: got %x, want 02", dst[0])
		}
	})

	t.Run("three branch union via any", func(t *testing.T) {
		schema := `["null","int","string"]`

		var v any = int32(10)
		dst := encode(t, schema, &v)
		if dst[0] != 0x02 {
			t.Fatalf("int branch index: got %x, want 02", dst[0])
		}

		v = "hi"
		dst = encode(t, schema, &v)
		if dst[0] != 0x04 {
			t.Fatalf("string branch index: got %x, want 04", dst[0])
		}
	})

	t.Run("round trip three branch", func(t *testing.T) {
		schema := `["null","int","string"]`
		data := []byte{0x04, 0x04, 0x68, 0x69}
		var v any
		decode(t, schema, data, &v)
		if v != "hi" {
			t.Fatalf("decode string branch: got %v, want hi", v)
		}
	})
}

func TestSpecBytesStringLengthVarlong(t *testing.T) {
	t.Run("empty string", func(t *testing.T) {
		dst := encode(t, `"string"`, new(string))
		if !bytes.Equal(dst, []byte{0x00}) {
			t.Fatalf("empty string: got %x, want 00", dst)
		}
	})

	t.Run("empty bytes", func(t *testing.T) {
		b := []byte{}
		dst := encode(t, `"bytes"`, &b)
		if !bytes.Equal(dst, []byte{0x00}) {
			t.Fatalf("empty bytes: got %x, want 00", dst)
		}
	})

	t.Run("string length encoding", func(t *testing.T) {
		s := "hello"
		dst := encode(t, `"string"`, &s)
		if dst[0] != 0x0A {
			t.Fatalf("string length prefix: got %x, want 0a", dst[0])
		}
		if len(dst) != 6 {
			t.Fatalf("string total length: got %d, want 6", len(dst))
		}
	})

	t.Run("bytes round trip", func(t *testing.T) {
		b := make([]byte, 300)
		for i := range b {
			b[i] = byte(i)
		}
		got := roundTrip(t, `"bytes"`, b)
		if !bytes.Equal(got, b) {
			t.Fatal("300-byte round trip failed")
		}
	})
}

func TestSpecArrayMapBlockCountVarlong(t *testing.T) {
	t.Run("array positive block count", func(t *testing.T) {
		schema := `{"type":"array","items":"int"}`
		arr := []int32{1, 2, 3}
		dst := encode(t, schema, &arr)
		if dst[0] != 0x06 {
			t.Fatalf("array count: got %x, want 06", dst[0])
		}
		if dst[len(dst)-1] != 0x00 {
			t.Fatalf("array terminator: got %x, want 00", dst[len(dst)-1])
		}
	})

	t.Run("array empty", func(t *testing.T) {
		schema := `{"type":"array","items":"int"}`
		arr := []int32{}
		dst := encode(t, schema, &arr)
		if !bytes.Equal(dst, []byte{0x00}) {
			t.Fatalf("empty array: got %x, want 00", dst)
		}
	})

	t.Run("array negative block count decode", func(t *testing.T) {
		schema := `{"type":"array","items":"int"}`
		data := []byte{0x05, 0x06, 0x02, 0x04, 0x06, 0x00}
		var v []int32
		decode(t, schema, data, &v)
		if len(v) != 3 || v[0] != 1 || v[1] != 2 || v[2] != 3 {
			t.Fatalf("negative block count: got %v, want [1 2 3]", v)
		}
	})

	t.Run("map block count", func(t *testing.T) {
		schema := `{"type":"map","values":"int"}`
		m := map[string]int32{"a": 1}
		dst := encode(t, schema, &m)
		if dst[0] != 0x02 {
			t.Fatalf("map count: got %x, want 02", dst[0])
		}
	})

	t.Run("map negative block count decode", func(t *testing.T) {
		schema := `{"type":"map","values":"int"}`
		data := []byte{0x01, 0x08, 0x02, 0x61, 0x02, 0x00}
		var v map[string]int32
		decode(t, schema, data, &v)
		if len(v) != 1 || v["a"] != 1 {
			t.Fatalf("map negative block: got %v, want {a:1}", v)
		}
	})
}

func TestSpecEnumIndexVarint(t *testing.T) {
	t.Run("basic enum", func(t *testing.T) {
		schema := `{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]}`
		s := "RED"
		dst := encode(t, schema, &s)
		if !bytes.Equal(dst, []byte{0x00}) {
			t.Fatalf("RED: got %x, want 00", dst)
		}

		s = "GREEN"
		dst = encode(t, schema, &s)
		if !bytes.Equal(dst, []byte{0x02}) {
			t.Fatalf("GREEN: got %x, want 02", dst)
		}

		s = "BLUE"
		dst = encode(t, schema, &s)
		if !bytes.Equal(dst, []byte{0x04}) {
			t.Fatalf("BLUE: got %x, want 04", dst)
		}
	})

	t.Run("enum with many symbols multi-byte index", func(t *testing.T) {
		symbols := make([]string, 200)
		for i := range symbols {
			symbols[i] = "S" + string(rune('A'+i/26)) + string(rune('A'+i%26))
		}
		schema := `{"type":"enum","name":"Big","symbols":[`
		for i, s := range symbols {
			if i > 0 {
				schema += ","
			}
			schema += `"` + s + `"`
		}
		schema += `]}`

		s := symbols[199]
		dst := encode(t, schema, &s)
		if len(dst) != 2 {
			t.Fatalf("multi-byte enum index: got %d bytes, want 2", len(dst))
		}
		if !bytes.Equal(dst, []byte{0x8E, 0x03}) {
			t.Fatalf("index 199: got %x, want 8e03", dst)
		}

		var result string
		decode(t, schema, dst, &result)
		if result != s {
			t.Fatalf("round-trip: got %q, want %q", result, s)
		}
	})
}

func TestSpecFloatDoubleEncoding(t *testing.T) {
	t.Run("float NaN", func(t *testing.T) {
		v := float32(math.NaN())
		dst := encode(t, `"float"`, &v)
		if len(dst) != 4 {
			t.Fatalf("float NaN: got %d bytes, want 4", len(dst))
		}
		var out float32
		decode(t, `"float"`, dst, &out)
		if !math.IsNaN(float64(out)) {
			t.Fatalf("float NaN round-trip: got %v, want NaN", out)
		}
	})

	t.Run("float +Inf", func(t *testing.T) {
		v := float32(math.Inf(1))
		got := roundTrip(t, `"float"`, v)
		if !math.IsInf(float64(got), 1) {
			t.Fatalf("float +Inf round-trip: got %v", got)
		}
	})

	t.Run("float -Inf", func(t *testing.T) {
		v := float32(math.Inf(-1))
		got := roundTrip(t, `"float"`, v)
		if !math.IsInf(float64(got), -1) {
			t.Fatalf("float -Inf round-trip: got %v", got)
		}
	})

	t.Run("float -0", func(t *testing.T) {
		v := float32(math.Copysign(0, -1))
		dst := encode(t, `"float"`, &v)
		if !bytes.Equal(dst, []byte{0x00, 0x00, 0x00, 0x80}) {
			t.Fatalf("float -0: got %x, want 00000080", dst)
		}
		var out float32
		decode(t, `"float"`, dst, &out)
		if math.Float32bits(out) != math.Float32bits(v) {
			t.Fatalf("float -0 round-trip: bit pattern mismatch")
		}
	})

	t.Run("float subnormal", func(t *testing.T) {
		v := math.SmallestNonzeroFloat32
		got := roundTrip(t, `"float"`, v)
		if got != v {
			t.Fatalf("float subnormal round-trip: got %v, want %v", got, v)
		}
	})

	t.Run("double NaN", func(t *testing.T) {
		v := math.NaN()
		dst := encode(t, `"double"`, &v)
		if len(dst) != 8 {
			t.Fatalf("double NaN: got %d bytes, want 8", len(dst))
		}
		var out float64
		decode(t, `"double"`, dst, &out)
		if !math.IsNaN(out) {
			t.Fatalf("double NaN round-trip: got %v, want NaN", out)
		}
	})

	t.Run("double +Inf", func(t *testing.T) {
		got := roundTrip(t, `"double"`, math.Inf(1))
		if !math.IsInf(got, 1) {
			t.Fatalf("double +Inf round-trip: got %v", got)
		}
	})

	t.Run("double -Inf", func(t *testing.T) {
		got := roundTrip(t, `"double"`, math.Inf(-1))
		if !math.IsInf(got, -1) {
			t.Fatalf("double -Inf round-trip: got %v", got)
		}
	})

	t.Run("double -0", func(t *testing.T) {
		v := math.Copysign(0, -1)
		dst := encode(t, `"double"`, &v)
		if !bytes.Equal(dst, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80}) {
			t.Fatalf("double -0: got %x", dst)
		}
		var out float64
		decode(t, `"double"`, dst, &out)
		if math.Float64bits(out) != math.Float64bits(v) {
			t.Fatalf("double -0 round-trip: bit pattern mismatch")
		}
	})

	t.Run("double subnormal", func(t *testing.T) {
		v := math.SmallestNonzeroFloat64
		got := roundTrip(t, `"double"`, v)
		if got != v {
			t.Fatalf("double subnormal round-trip: got %v, want %v", got, v)
		}
	})
}

func TestSpecLongRejectsUnsignedOverflow(t *testing.T) {
	s := mustParse(t, `"long"`)
	v := ^uint64(0)
	if _, err := s.Encode(&v); err == nil {
		t.Fatal("expected overflow error encoding uint64 max as Avro long")
	}
}

// ptr returns a pointer to v. Used for building test values.
func ptr[T any](v T) *T { return &v }
