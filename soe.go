package avro

import (
	"errors"
	"fmt"
)

// AppendSingleObject appends a Single Object Encoding of v to dst: 2-byte
// magic, 8-byte CRC-64-AVRO fingerprint, then the Avro binary payload.
func (s *Schema) AppendSingleObject(dst []byte, v any, opts ...Opt) ([]byte, error) {
	dst = append(dst, s.soe[:]...)
	return s.AppendEncode(dst, v, opts...)
}

// validateSOEHeader checks that data has the 10-byte Single Object Encoding
// header (length + magic bytes). Shared by DecodeSingleObject and
// SingleObjectFingerprint so the two paths agree on the error shapes.
func validateSOEHeader(data []byte) error {
	if len(data) < 10 {
		return fmt.Errorf("avro: single-object encoding too short: need at least 10 bytes, have %d", len(data))
	}
	if data[0] != 0xC3 || data[1] != 0x01 {
		return fmt.Errorf("avro: invalid single-object encoding magic: got [%#x, %#x], want [0xc3, 0x01]", data[0], data[1])
	}
	return nil
}

// DecodeSingleObject decodes a Single Object Encoding message into v after
// verifying the magic and fingerprint match this schema.
func (s *Schema) DecodeSingleObject(data []byte, v any, opts ...Opt) ([]byte, error) {
	if err := validateSOEHeader(data); err != nil {
		return nil, err
	}
	if [10]byte(data[:10]) != s.soe {
		return nil, errors.New("avro: single-object encoding fingerprint mismatch")
	}
	return s.Decode(data[10:], v, opts...)
}

// SingleObjectFingerprint extracts the 8-byte CRC-64-AVRO fingerprint and
// returns the remaining payload.
func SingleObjectFingerprint(data []byte) (fp [8]byte, rest []byte, err error) {
	if err := validateSOEHeader(data); err != nil {
		return fp, nil, err
	}
	copy(fp[:], data[2:10])
	return fp, data[10:], nil
}
