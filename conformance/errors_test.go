package conformance

import (
	"testing"
)

// -----------------------------------------------------------------------
// Error Handling & Malformed Data
// Spec: "Binary Encoding" — decoders must reject truncated or invalid data.
//   - Truncated varints, floats, doubles, strings, fixed
//   - Out-of-range union/enum indices
//   - Go-specific: type mismatch, non-pointer decode target
// https://avro.apache.org/docs/1.12.0/specification/#binary-encoding
// -----------------------------------------------------------------------

func TestErrorTruncatedVarint(t *testing.T) {
	// Varint with continuation bit set but no following byte.
	decodeErr(t, `"int"`, []byte{0x80}, new(int32))
}

func TestErrorTruncatedFloat(t *testing.T) {
	// Less than 4 bytes for float.
	decodeErr(t, `"float"`, []byte{0x00, 0x00}, new(float32))
}

func TestErrorTruncatedDouble(t *testing.T) {
	// Less than 8 bytes for double.
	decodeErr(t, `"double"`, []byte{0x00, 0x00, 0x00, 0x00}, new(float64))
}

func TestErrorTruncatedString(t *testing.T) {
	// String length says 10 but only 2 bytes follow.
	// 10 in zigzag = 20 = 0x14
	decodeErr(t, `"string"`, []byte{0x14, 0x61, 0x62}, new(string))
}

func TestErrorTruncatedFixed(t *testing.T) {
	// Fixed(4) but only 2 bytes.
	decodeErr(t, `{"type":"fixed","name":"F","size":4}`, []byte{0x00, 0x00}, new([4]byte))
}

func TestErrorInvalidUnionIndex(t *testing.T) {
	// Union ["null","int"] has indices 0 and 1.
	// Index 5 (zigzag 10 = 0x0A) is out of range.
	decodeErr(t, `["null","int"]`, []byte{0x0A}, new(any))
}

func TestErrorInvalidEnumIndex(t *testing.T) {
	// Enum with 3 symbols. Index 10 (zigzag 20 = 0x14) is out of range.
	decodeErr(t, `{"type":"enum","name":"E","symbols":["A","B","C"]}`, []byte{0x14}, new(string))
}

func TestErrorTypeMismatch(t *testing.T) {
	// Try to decode int data into a string.
	s := mustParse(t, `"int"`)
	data := []byte{0x04} // int 2
	var out string
	_, err := s.Decode(data, &out)
	if err == nil {
		t.Fatal("expected error for type mismatch")
	}
}

func TestErrorNonPointerDecode(t *testing.T) {
	s := mustParse(t, `"int"`)
	data := []byte{0x04}
	var v int32
	_, err := s.Decode(data, v) // non-pointer
	if err == nil {
		t.Fatal("expected error for non-pointer decode")
	}
}
