package conformance

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/twmb/avro"
)

// zigzagEncode32 encodes an int32 using Avro's zigzag varint encoding.
func zigzagEncode32(i int32) []byte {
	z := uint32((i << 1) ^ (i >> 31))
	var buf [5]byte
	n := 0
	for z >= 0x80 {
		buf[n] = byte(z) | 0x80
		z >>= 7
		n++
	}
	buf[n] = byte(z)
	return buf[:n+1]
}

// zigzagEncode64 encodes an int64 using Avro's zigzag varlong encoding.
func zigzagEncode64(i int64) []byte {
	z := uint64((i << 1) ^ (i >> 63))
	var buf [10]byte
	n := 0
	for z >= 0x80 {
		buf[n] = byte(z) | 0x80
		z >>= 7
		n++
	}
	buf[n] = byte(z)
	return buf[:n+1]
}

// encodeUint32LE encodes a uint32 in little-endian format (for float bits).
func encodeUint32LE(u uint32) []byte {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], u)
	return buf[:]
}

// encodeUint64LE encodes a uint64 in little-endian format (for double bits).
func encodeUint64LE(u uint64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], u)
	return buf[:]
}

// mustParse parses a schema string, failing the test on error.
func mustParse(t *testing.T, schema string) *avro.Schema {
	t.Helper()
	s, err := avro.Parse(schema)
	if err != nil {
		t.Fatalf("Parse(%q): %v", schema, err)
	}
	return s
}

// encode encodes v with the given schema string and returns the raw bytes.
func encode(t *testing.T, schema string, v any) []byte {
	t.Helper()
	s := mustParse(t, schema)
	dst, err := s.AppendEncode(nil, v)
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	return dst
}

// decode decodes src into v using the given schema string.
func decode(t *testing.T, schema string, src []byte, v any) {
	t.Helper()
	s := mustParse(t, schema)
	rem, err := s.Decode(src, v)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("Decode left %d unconsumed bytes", len(rem))
	}
}

// decodeErr expects Decode to return an error.
func decodeErr(t *testing.T, schema string, src []byte, v any) {
	t.Helper()
	s := mustParse(t, schema)
	_, err := s.Decode(src, v)
	if err == nil {
		t.Fatal("expected error from Decode, got nil")
	}
}

// roundTrip encodes then decodes a value, returning the result.
func roundTrip[T any](t *testing.T, schema string, input T) T {
	t.Helper()
	s := mustParse(t, schema)
	encoded, err := s.AppendEncode(nil, &input)
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	var output T
	rem, err := s.Decode(encoded, &output)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(rem) != 0 {
		t.Fatalf("Decode left %d unconsumed bytes", len(rem))
	}
	return output
}

// resolveEncodeDecode encodes input with writerSchema, resolves to readerSchema,
// and decodes into output.
func resolveEncodeDecode(t *testing.T, writerSchema, readerSchema string, input, output any) {
	t.Helper()
	writer := mustParse(t, writerSchema)
	reader := mustParse(t, readerSchema)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	encoded, err := writer.Encode(input)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	_, err = resolved.Decode(encoded, output)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
}

// buildReferenceBytes builds expected bytes for the interop reference test.
func buildReferenceBytes(boolVal bool, intVal int32, longVal int64, floatVal float32, doubleVal float64, strVal string, bytesVal []byte) []byte {
	var want []byte
	if boolVal {
		want = append(want, 0x01)
	} else {
		want = append(want, 0x00)
	}
	want = append(want, zigzagEncode32(intVal)...)
	want = append(want, zigzagEncode64(longVal)...)
	want = append(want, encodeUint32LE(math.Float32bits(floatVal))...)
	want = append(want, encodeUint64LE(math.Float64bits(doubleVal))...)
	want = append(want, zigzagEncode64(int64(len(strVal)))...)
	want = append(want, strVal...)
	want = append(want, zigzagEncode64(int64(len(bytesVal)))...)
	want = append(want, bytesVal...)
	return want
}
