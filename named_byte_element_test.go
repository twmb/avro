package avro_test

import (
	"bytes"
	"testing"

	"github.com/twmb/avro"
)

// A Go byte-container type whose ELEMENT type is a named byte (type B byte;
// [N]B, []B) has element Kind Uint8 but an element type that is not exactly
// uint8. The byte encoder accepts such types (serSize / doSerBytes /
// appendAvroJSONBytes iterate elements via Uint), so by the encode/decode
// target-type parity contract every decoder and the JSON encoder must accept
// them too. The decode and JSON paths used reflect.Copy /
// reflect.Value.Set(reflect.ValueOf([]byte)), which require the element type to
// be EXACTLY uint8 and PANIC on a named element ("reflect.Copy: B != uint8") —
// a panic reaching the caller of a public API on a valid Go value. These tests
// pin that every fixed/bytes/uuid path round-trips a named-byte-element type on
// both wires, scalar and as a struct field (the unsafe fast path), and through
// bytes->string+uuid promotion.

type nbeByte byte
type nbeFix3 [3]nbeByte
type nbeUUID [16]nbeByte
type nbeSlice []nbeByte

func TestRegression_NamedByteElementRoundTrip(t *testing.T) {
	uuidWire := nbeUUID{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe}

	t.Run("fixed/binary", func(t *testing.T) {
		s := avro.MustParse(`{"type":"fixed","name":"F","size":3}`)
		in := nbeFix3{1, 2, 3}
		b, err := s.AppendEncode(nil, in)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var out nbeFix3
		if _, err := s.Decode(b, &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if out != in {
			t.Fatalf("round-trip: got %v want %v", out, in)
		}
	})

	t.Run("fixed/json", func(t *testing.T) {
		s := avro.MustParse(`{"type":"fixed","name":"F","size":3}`)
		in := nbeFix3{1, 2, 3}
		j, err := s.AppendEncodeJSON(nil, in)
		if err != nil {
			t.Fatalf("encode json: %v", err)
		}
		var out nbeFix3
		if err := s.DecodeJSON(j, &out); err != nil {
			t.Fatalf("decode json: %v", err)
		}
		if out != in {
			t.Fatalf("round-trip json: got %v want %v", out, in)
		}
	})

	t.Run("bytes/array/binary", func(t *testing.T) {
		s := avro.MustParse(`"bytes"`)
		in := nbeFix3{4, 5, 6}
		b, err := s.AppendEncode(nil, in)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var out nbeFix3
		if _, err := s.Decode(b, &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if out != in {
			t.Fatalf("round-trip: got %v want %v", out, in)
		}
	})

	t.Run("bytes/slice/binary+json", func(t *testing.T) {
		s := avro.MustParse(`"bytes"`)
		in := nbeSlice{7, 8, 9}
		b, err := s.AppendEncode(nil, in)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var out nbeSlice
		if _, err := s.Decode(b, &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !bytes.Equal([]byte(toBytes(out)), []byte{7, 8, 9}) {
			t.Fatalf("round-trip: got %v", out)
		}
		j, err := s.AppendEncodeJSON(nil, in)
		if err != nil {
			t.Fatalf("encode json: %v", err)
		}
		var outJ nbeSlice
		if err := s.DecodeJSON(j, &outJ); err != nil {
			t.Fatalf("decode json: %v", err)
		}
	})

	t.Run("bytes/array->fixed-slice-target/binary", func(t *testing.T) {
		// deserFixed's slice arm (was Set(reflect.ValueOf), now SetBytes).
		s := avro.MustParse(`{"type":"fixed","name":"F","size":3}`)
		b, err := s.AppendEncode(nil, nbeFix3{1, 2, 3})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var out nbeSlice
		if _, err := s.Decode(b, &out); err != nil {
			t.Fatalf("decode into named-byte slice: %v", err)
		}
		if len(out) != 3 {
			t.Fatalf("got %v", out)
		}
	})

	t.Run("uuid-fixed/binary+json", func(t *testing.T) {
		s := avro.MustParse(`{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`)
		b, err := s.AppendEncode(nil, uuidWire)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var out nbeUUID
		if _, err := s.Decode(b, &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if out != uuidWire {
			t.Fatalf("round-trip: got %v want %v", out, uuidWire)
		}
		j, err := s.AppendEncodeJSON(nil, uuidWire)
		if err != nil {
			t.Fatalf("encode json: %v", err)
		}
		var outJ nbeUUID
		if err := s.DecodeJSON(j, &outJ); err != nil {
			t.Fatalf("decode json: %v", err)
		}
		if outJ != uuidWire {
			t.Fatalf("round-trip json: got %v want %v", outJ, uuidWire)
		}
	})

	t.Run("uuid-string->[16]named/binary+json", func(t *testing.T) {
		s := avro.MustParse(`{"type":"string","logicalType":"uuid"}`)
		str := "01234567-89ab-cdef-1032-547698badcfe"
		b, err := s.AppendEncode(nil, str)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var out nbeUUID
		if _, err := s.Decode(b, &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if out != uuidWire {
			t.Fatalf("round-trip: got %v want %v", out, uuidWire)
		}
		j, err := s.AppendEncodeJSON(nil, str)
		if err != nil {
			t.Fatalf("encode json: %v", err)
		}
		var outJ nbeUUID
		if err := s.DecodeJSON(j, &outJ); err != nil {
			t.Fatalf("decode json: %v", err)
		}
		if outJ != uuidWire {
			t.Fatalf("round-trip json: got %v want %v", outJ, uuidWire)
		}
	})

	t.Run("struct-field-fixed/unsafe-path/binary", func(t *testing.T) {
		type R struct {
			F nbeFix3
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"F","type":{"type":"fixed","name":"F3","size":3}}]}`)
		in := R{F: nbeFix3{1, 2, 3}}
		b, err := s.AppendEncode(nil, in)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var out R
		if _, err := s.Decode(b, &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if out != in {
			t.Fatalf("round-trip: got %v want %v", out, in)
		}
	})

	t.Run("struct-field-uuid/unsafe-path/binary", func(t *testing.T) {
		type R struct {
			U nbeUUID `avro:"u"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"u","type":{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}}]}`)
		in := R{U: uuidWire}
		b, err := s.AppendEncode(nil, in)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var out R
		if _, err := s.Decode(b, &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if out != in {
			t.Fatalf("round-trip: got %v want %v", out, in)
		}
	})

	t.Run("bytes->string+uuid promotion/[16]named", func(t *testing.T) {
		// promoteBytesToStringUUID: writer bytes, reader string+uuid, target
		// [16]named. Was reflect.Copy (panic), now copyBytesToArray.
		w := avro.MustParse(`"bytes"`)
		r := avro.MustParse(`{"type":"string","logicalType":"uuid"}`)
		res, err := avro.Resolve(w, r)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		// Writer encodes the 36-char canonical UUID string AS bytes.
		b, err := w.AppendEncode(nil, []byte("01234567-89ab-cdef-1032-547698badcfe"))
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var out nbeUUID
		if _, err := res.Decode(b, &out); err != nil {
			t.Fatalf("promoted decode: %v", err)
		}
		if out != uuidWire {
			t.Fatalf("promotion round-trip: got %v want %v", out, uuidWire)
		}
	})
}

// toBytes converts a named byte slice to []byte for comparison.
func toBytes(s nbeSlice) []byte {
	b := make([]byte, len(s))
	for i := range s {
		b[i] = byte(s[i])
	}
	return b
}

// TestRegression_ExactByteContainersStillRoundTrip is the boundary-1 control:
// the exact-uint8 element fast path (the common [N]byte / []byte case) must keep
// working unchanged after the named-byte-element fix relaxed the copy helpers.
func TestRegression_ExactByteContainersStillRoundTrip(t *testing.T) {
	t.Run("fixed/[3]byte", func(t *testing.T) {
		s := avro.MustParse(`{"type":"fixed","name":"F","size":3}`)
		in := [3]byte{1, 2, 3}
		b, _ := s.AppendEncode(nil, in)
		var out [3]byte
		if _, err := s.Decode(b, &out); err != nil || out != in {
			t.Fatalf("got %v err %v", out, err)
		}
		j, _ := s.AppendEncodeJSON(nil, in)
		var outJ [3]byte
		if err := s.DecodeJSON(j, &outJ); err != nil || outJ != in {
			t.Fatalf("json got %v err %v", outJ, err)
		}
	})
	t.Run("uuid-fixed/[16]byte", func(t *testing.T) {
		s := avro.MustParse(`{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`)
		in := [16]byte{1, 2, 3, 4}
		b, _ := s.AppendEncode(nil, in)
		var out [16]byte
		if _, err := s.Decode(b, &out); err != nil || out != in {
			t.Fatalf("got %v err %v", out, err)
		}
	})
	t.Run("bytes/[]byte", func(t *testing.T) {
		s := avro.MustParse(`"bytes"`)
		in := []byte{9, 8, 7}
		b, _ := s.AppendEncode(nil, in)
		var out []byte
		if _, err := s.Decode(b, &out); err != nil || !bytes.Equal(out, in) {
			t.Fatalf("got %v err %v", out, err)
		}
	})
}
