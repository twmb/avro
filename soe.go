package avro

import "fmt"

// AppendSingleObject encodes v using the Avro Single Object Encoding format
// and appends the result to dst. The format is a 2-byte magic header (0xC3,
// 0x01), an 8-byte little-endian CRC-64-AVRO fingerprint of the schema, and
// the Avro binary encoding of v.
func (s *Schema) AppendSingleObject(dst []byte, v any) ([]byte, error) {
	dst = append(dst, s.soe[:]...)
	return s.AppendEncode(dst, v)
}

// DecodeSingleObject decodes a Single Object Encoding message, verifying that
// the magic bytes and schema fingerprint match this schema. It returns the
// remaining bytes after the Avro binary payload.
func (s *Schema) DecodeSingleObject(data []byte, v any) ([]byte, error) {
	if len(data) < 10 {
		return nil, fmt.Errorf("avro: single-object encoding too short: need at least 10 bytes, have %d", len(data))
	}
	if data[0] != 0xC3 || data[1] != 0x01 {
		return nil, fmt.Errorf("avro: invalid single-object encoding magic: got [%#x, %#x], want [0xc3, 0x01]", data[0], data[1])
	}
	if [10]byte(data[:10]) != s.soe {
		return nil, fmt.Errorf("avro: single-object encoding fingerprint mismatch")
	}
	return s.Decode(data[10:], v)
}

// SingleObjectFingerprint extracts the 8-byte CRC-64-AVRO fingerprint from a
// Single Object Encoding message. It returns the fingerprint, the remaining
// bytes (the Avro binary payload), and any error. This is useful for looking
// up the schema by fingerprint before decoding.
func SingleObjectFingerprint(data []byte) (fp [8]byte, rest []byte, err error) {
	if len(data) < 10 {
		return fp, nil, fmt.Errorf("avro: single-object encoding too short: need at least 10 bytes, have %d", len(data))
	}
	if data[0] != 0xC3 || data[1] != 0x01 {
		return fp, nil, fmt.Errorf("avro: invalid single-object encoding magic: got [%#x, %#x], want [0xc3, 0x01]", data[0], data[1])
	}
	copy(fp[:], data[2:10])
	return fp, data[10:], nil
}
