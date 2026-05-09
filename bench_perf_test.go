package avro

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
)

// Benchmarks targeting the deserString TextUnmarshaler/[]byte/UUID
// allocation paths.

type benchTextUnmarshaler struct{ s string }

func (b *benchTextUnmarshaler) UnmarshalText(text []byte) error {
	b.s = string(text)
	return nil
}

func BenchmarkDecodeStringTextUnmarshaler(b *testing.B) {
	type Encoded struct {
		V string `avro:"v"`
	}
	type Decoded struct {
		V benchTextUnmarshaler `avro:"v"`
	}
	schema := `{"type":"record","name":"r","fields":[{"name":"v","type":"string"}]}`
	s, err := Parse(schema)
	if err != nil {
		b.Fatal(err)
	}
	in := Encoded{V: "hello world this is a test"}
	enc, err := s.AppendEncode(nil, &in)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var out Decoded
		if _, err := s.Decode(enc, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeStringBytes(b *testing.B) {
	type R struct {
		V []byte `avro:"v"`
	}
	schema := `{"type":"record","name":"r","fields":[{"name":"v","type":"string"}]}`
	s, err := Parse(schema)
	if err != nil {
		b.Fatal(err)
	}
	in := R{V: bytes.Repeat([]byte("x"), 32)}
	enc, err := s.AppendEncode(nil, &in)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var out R
		if _, err := s.Decode(enc, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeUUIDIntoFixed(b *testing.B) {
	type R struct {
		V [16]byte `avro:"v"`
	}
	schema := `{"type":"record","name":"r","fields":[{"name":"v","type":{"type":"string","logicalType":"uuid"}}]}`
	s, err := Parse(schema)
	if err != nil {
		b.Fatal(err)
	}
	in := R{V: [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}}
	enc, err := s.AppendEncode(nil, &in)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var out R
		if _, err := s.Decode(enc, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseUUID(b *testing.B) {
	const s = "550e8400-e29b-41d4-a716-446655440000"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = parseUUID(s)
	}
}

// Map decode benchmarks targeting the size-hint optimization. We use a
// large map with mixed key/value sizes so any rehash cost shows up.

func BenchmarkDecodeMap_String_Small(b *testing.B) {
	benchDecodeMapStringValue(b, 8)
}

func BenchmarkDecodeMap_String_Medium(b *testing.B) {
	benchDecodeMapStringValue(b, 64)
}

func BenchmarkDecodeMap_String_Large(b *testing.B) {
	benchDecodeMapStringValue(b, 512)
}

func benchDecodeMapStringValue(b *testing.B, n int) {
	schema := `{"type":"map","values":"string"}`
	s, err := Parse(schema)
	if err != nil {
		b.Fatal(err)
	}
	in := make(map[string]string, n)
	for i := 0; i < n; i++ {
		in[fmt.Sprintf("key-%05d", i)] = fmt.Sprintf("value-%05d", i)
	}
	enc, err := s.AppendEncode(nil, &in)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var out map[string]string
		if _, err := s.Decode(enc, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeEnum_LargeAlphabet(b *testing.B) {
	// 32 symbols — exceeds the linear-scan threshold so the map index path
	// is exercised.
	syms := make([]string, 32)
	for i := range syms {
		syms[i] = fmt.Sprintf("SYM_%d", i)
	}
	enc, _ := json.Marshal(syms)
	schema := fmt.Sprintf(`{"type":"enum","name":"E","symbols":%s}`, enc)
	s, err := Parse(schema)
	if err != nil {
		b.Fatal(err)
	}
	val := "SYM_31" // worst case for linear scan
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, err := s.AppendEncode(nil, &val)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkResolveDecodeWithDefaults(b *testing.B) {
	writer := `{"type":"record","name":"r","fields":[
		{"name":"a","type":"string"},
		{"name":"b","type":"int"}
	]}`
	reader := `{"type":"record","name":"r","fields":[
		{"name":"a","type":"string"},
		{"name":"b","type":"int"},
		{"name":"c","type":"string","default":"default-c"},
		{"name":"d","type":"int","default":42},
		{"name":"e","type":["null","string"],"default":null}
	]}`
	w, err := Parse(writer)
	if err != nil {
		b.Fatal(err)
	}
	r, err := Parse(reader)
	if err != nil {
		b.Fatal(err)
	}
	resolved, err := Resolve(w, r)
	if err != nil {
		b.Fatal(err)
	}
	type WIn struct {
		A string `avro:"a"`
		B int32  `avro:"b"`
	}
	in := WIn{A: "hello", B: 7}
	enc, err := w.AppendEncode(nil, &in)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var out map[string]any
		if _, err := resolved.Decode(enc, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeMapInto_Any_Medium(b *testing.B) {
	schema := `{"type":"map","values":"string"}`
	s, err := Parse(schema)
	if err != nil {
		b.Fatal(err)
	}
	in := make(map[string]string, 64)
	for i := 0; i < 64; i++ {
		in[fmt.Sprintf("key-%05d", i)] = fmt.Sprintf("value-%05d", i)
	}
	enc, err := s.AppendEncode(nil, &in)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var out any
		if _, err := s.Decode(enc, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeArrayStringInto_Any_Medium(b *testing.B) {
	schema := `{"type":"array","items":"string"}`
	s, err := Parse(schema)
	if err != nil {
		b.Fatal(err)
	}
	in := make([]string, 64)
	for i := 0; i < 64; i++ {
		in[i] = fmt.Sprintf("value-%05d", i)
	}
	enc, err := s.AppendEncode(nil, &in)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var out any
		if _, err := s.Decode(enc, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeArrayIntInto_Any_Medium(b *testing.B) {
	schema := `{"type":"array","items":"int"}`
	s, err := Parse(schema)
	if err != nil {
		b.Fatal(err)
	}
	in := make([]int32, 64)
	for i := 0; i < 64; i++ {
		in[i] = int32(i * 1000)
	}
	enc, err := s.AppendEncode(nil, &in)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var out any
		if _, err := s.Decode(enc, &out); err != nil {
			b.Fatal(err)
		}
	}
}

// Multi-level pointer specialization benchmarks. Each measures
// serArray.serFoo or serMap.serFoo encoding a slice/map whose element
// type has zero, one, two, or three pointer-indirection levels. The
// goal is to quantify the cost of supporting deeper pointer chains in
// the per-primitive specializations — vs. the existing single-level
// unwrap, and vs. the fully direct path. Pre-multi-level-fix, ptr2/ptr3
// cases will b.Skip because the encoder rejects them.

func BenchmarkSpecArrayMultiLevelPointer(b *testing.B) {
	const N = 1024
	intS := MustParse(`{"type":"array","items":"int"}`)
	longS := MustParse(`{"type":"array","items":"long"}`)
	floatS := MustParse(`{"type":"array","items":"float"}`)
	doubleS := MustParse(`{"type":"array","items":"double"}`)
	stringS := MustParse(`{"type":"array","items":"string"}`)
	boolS := MustParse(`{"type":"array","items":"boolean"}`)

	int32Direct := make([]int32, N)
	int32Ptr1 := make([]*int32, N)
	int32Ptr2 := make([]**int32, N)
	int32Ptr3 := make([]***int32, N)
	int64Direct := make([]int64, N)
	int64Ptr1 := make([]*int64, N)
	int64Ptr2 := make([]**int64, N)
	int64Ptr3 := make([]***int64, N)
	float32Direct := make([]float32, N)
	float32Ptr1 := make([]*float32, N)
	float32Ptr2 := make([]**float32, N)
	float32Ptr3 := make([]***float32, N)
	float64Direct := make([]float64, N)
	float64Ptr1 := make([]*float64, N)
	float64Ptr2 := make([]**float64, N)
	float64Ptr3 := make([]***float64, N)
	stringDirect := make([]string, N)
	stringPtr1 := make([]*string, N)
	stringPtr2 := make([]**string, N)
	stringPtr3 := make([]***string, N)
	boolDirect := make([]bool, N)
	boolPtr1 := make([]*bool, N)
	boolPtr2 := make([]**bool, N)
	boolPtr3 := make([]***bool, N)
	for i := range N {
		i32 := int32(i)
		i32p1 := &i32
		i32p2 := &i32p1
		i64 := int64(i)
		i64p1 := &i64
		i64p2 := &i64p1
		f32 := float32(i)
		f32p1 := &f32
		f32p2 := &f32p1
		f64 := float64(i)
		f64p1 := &f64
		f64p2 := &f64p1
		s := fmt.Sprintf("v%d", i)
		sp1 := &s
		sp2 := &sp1
		bv := i&1 == 1
		bp1 := &bv
		bp2 := &bp1
		int32Direct[i] = i32
		int32Ptr1[i] = i32p1
		int32Ptr2[i] = i32p2
		int32Ptr3[i] = &i32p2
		int64Direct[i] = i64
		int64Ptr1[i] = i64p1
		int64Ptr2[i] = i64p2
		int64Ptr3[i] = &i64p2
		float32Direct[i] = f32
		float32Ptr1[i] = f32p1
		float32Ptr2[i] = f32p2
		float32Ptr3[i] = &f32p2
		float64Direct[i] = f64
		float64Ptr1[i] = f64p1
		float64Ptr2[i] = f64p2
		float64Ptr3[i] = &f64p2
		stringDirect[i] = s
		stringPtr1[i] = sp1
		stringPtr2[i] = sp2
		stringPtr3[i] = &sp2
		boolDirect[i] = bv
		boolPtr1[i] = bp1
		boolPtr2[i] = bp2
		boolPtr3[i] = &bp2
	}

	cases := []struct {
		name string
		s    *Schema
		v    any
	}{
		{"int32/direct", intS, int32Direct},
		{"int32/ptr1", intS, int32Ptr1},
		{"int32/ptr2", intS, int32Ptr2},
		{"int32/ptr3", intS, int32Ptr3},
		{"int64/direct", longS, int64Direct},
		{"int64/ptr1", longS, int64Ptr1},
		{"int64/ptr2", longS, int64Ptr2},
		{"int64/ptr3", longS, int64Ptr3},
		{"float32/direct", floatS, float32Direct},
		{"float32/ptr1", floatS, float32Ptr1},
		{"float32/ptr2", floatS, float32Ptr2},
		{"float32/ptr3", floatS, float32Ptr3},
		{"float64/direct", doubleS, float64Direct},
		{"float64/ptr1", doubleS, float64Ptr1},
		{"float64/ptr2", doubleS, float64Ptr2},
		{"float64/ptr3", doubleS, float64Ptr3},
		{"string/direct", stringS, stringDirect},
		{"string/ptr1", stringS, stringPtr1},
		{"string/ptr2", stringS, stringPtr2},
		{"string/ptr3", stringS, stringPtr3},
		{"bool/direct", boolS, boolDirect},
		{"bool/ptr1", boolS, boolPtr1},
		{"bool/ptr2", boolS, boolPtr2},
		{"bool/ptr3", boolS, boolPtr3},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			buf := make([]byte, 0, 16<<10)
			if _, err := tc.s.AppendEncode(buf, tc.v); err != nil {
				b.Skipf("unsupported pre-patch: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				buf, _ = tc.s.AppendEncode(buf[:0], tc.v)
			}
			_ = buf
		})
	}
}

func BenchmarkSpecMapMultiLevelPointer(b *testing.B) {
	const N = 1024
	intS := MustParse(`{"type":"map","values":"int"}`)
	stringS := MustParse(`{"type":"map","values":"string"}`)

	int32Direct := make(map[string]int32, N)
	int32Ptr1 := make(map[string]*int32, N)
	int32Ptr2 := make(map[string]**int32, N)
	int32Ptr3 := make(map[string]***int32, N)
	stringDirect := make(map[string]string, N)
	stringPtr1 := make(map[string]*string, N)
	stringPtr2 := make(map[string]**string, N)
	stringPtr3 := make(map[string]***string, N)
	for i := range N {
		key := fmt.Sprintf("k%04d", i)
		i32 := int32(i)
		i32p1 := &i32
		i32p2 := &i32p1
		s := fmt.Sprintf("v%d", i)
		sp1 := &s
		sp2 := &sp1
		int32Direct[key] = i32
		int32Ptr1[key] = i32p1
		int32Ptr2[key] = i32p2
		int32Ptr3[key] = &i32p2
		stringDirect[key] = s
		stringPtr1[key] = sp1
		stringPtr2[key] = sp2
		stringPtr3[key] = &sp2
	}

	cases := []struct {
		name string
		s    *Schema
		v    any
	}{
		{"int32/direct", intS, int32Direct},
		{"int32/ptr1", intS, int32Ptr1},
		{"int32/ptr2", intS, int32Ptr2},
		{"int32/ptr3", intS, int32Ptr3},
		{"string/direct", stringS, stringDirect},
		{"string/ptr1", stringS, stringPtr1},
		{"string/ptr2", stringS, stringPtr2},
		{"string/ptr3", stringS, stringPtr3},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			buf := make([]byte, 0, 32<<10)
			if _, err := tc.s.AppendEncode(buf, tc.v); err != nil {
				b.Skipf("unsupported pre-patch: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				buf, _ = tc.s.AppendEncode(buf[:0], tc.v)
			}
			_ = buf
		})
	}
}
