package conformance

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/twmb/avro"
)

// -----------------------------------------------------------------------
// Schema Fingerprints (CRC-64-AVRO / Rabin)
// Spec: "Schema Fingerprints" — a 64-bit Rabin fingerprint computed
// over a schema's Parsing Canonical Form.
//   - Polynomial: 0xc15d213aa4d7a795 (also the empty fingerprint)
//   - Input: the schema's canonical JSON representation
//   - Output: 8-byte fingerprint (big-endian via hash.Hash.Sum)
// https://avro.apache.org/docs/1.12.0/specification/#schema-fingerprints
// -----------------------------------------------------------------------

// TestFingerprintEmptyHash verifies that NewRabin with no data written
// returns the empty fingerprint constant 0xc15d213aa4d7a795.
func TestFingerprintEmptyHash(t *testing.T) {
	h := avro.NewRabin()
	got := h.Sum64()
	const want = uint64(0xc15d213aa4d7a795)
	if got != want {
		t.Fatalf("empty Rabin: got %#016x, want %#016x", got, want)
	}
}

// TestFingerprintPrimitiveSchemas verifies fingerprints of all 8
// primitive type canonical forms against known reference values.
// Reference values computed from the Avro spec's CRC-64-AVRO algorithm.
func TestFingerprintPrimitiveSchemas(t *testing.T) {
	vectors := []struct {
		schema string
		wantBE uint64 // expected fingerprint in big-endian uint64
	}{
		{`"null"`, 0x63dd24e7cc258f8a},
		{`"boolean"`, 0x9f42fc78a4d4f764},
		{`"int"`, 0x7275d51a3f395c8f},
		{`"long"`, 0xd054e14493f41db7},
		{`"float"`, 0x4d7c02cb3ea8d790},
		{`"double"`, 0x8e7535c032ab957e},
		{`"string"`, 0x8f014872634503c7},
		{`"bytes"`, 0x4fc016dac3201965},
	}

	for _, tc := range vectors {
		t.Run(tc.schema, func(t *testing.T) {
			s := mustParse(t, tc.schema)
			h := avro.NewRabin()
			fp := s.Fingerprint(h)
			got := binary.BigEndian.Uint64(fp)
			if got != tc.wantBE {
				t.Fatalf("fingerprint: got %#016x, want %#016x", got, tc.wantBE)
			}
		})
	}
}

// TestFingerprintDeterministic verifies that two schemas with the same
// canonical form produce the same fingerprint, regardless of JSON key order.
func TestFingerprintDeterministic(t *testing.T) {
	s1 := mustParse(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`)
	s2 := mustParse(t, `{"fields":[{"type":"int","name":"a"}],"name":"R","type":"record"}`)

	h := avro.NewRabin()
	fp1 := s1.Fingerprint(h)
	h.Reset()
	fp2 := s2.Fingerprint(h)

	if !bytes.Equal(fp1, fp2) {
		t.Fatalf("fingerprints differ: %x vs %x", fp1, fp2)
	}
}

// TestFingerprintDistinct verifies that different schemas produce
// different fingerprints.
func TestFingerprintDistinct(t *testing.T) {
	schemas := []string{
		`"int"`,
		`"long"`,
		`"string"`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}`,
		`{"type":"enum","name":"E","symbols":["A","B"]}`,
	}

	fps := make(map[uint64]string)
	for _, schema := range schemas {
		s := mustParse(t, schema)
		h := avro.NewRabin()
		fp := s.Fingerprint(h)
		val := binary.BigEndian.Uint64(fp)
		if prev, exists := fps[val]; exists {
			t.Fatalf("collision: %q and %q both produce %#016x", prev, schema, val)
		}
		fps[val] = schema
	}
}

// TestFingerprintReset verifies that resetting the hash produces
// consistent results.
func TestFingerprintReset(t *testing.T) {
	s := mustParse(t, `"int"`)
	h := avro.NewRabin()

	fp1 := s.Fingerprint(h)
	h.Reset()
	fp2 := s.Fingerprint(h)

	if !bytes.Equal(fp1, fp2) {
		t.Fatalf("after reset: %x vs %x", fp1, fp2)
	}
}

// TestFingerprintNamespaceExpansion verifies that the fingerprint is
// computed over the canonical form with expanded fullnames.
func TestFingerprintNamespaceExpansion(t *testing.T) {
	// These two produce the same canonical form (fullname "com.example.R").
	s1 := mustParse(t, `{"type":"record","name":"R","namespace":"com.example","fields":[{"name":"a","type":"int"}]}`)
	s2 := mustParse(t, `{"type":"record","name":"com.example.R","fields":[{"name":"a","type":"int"}]}`)

	h := avro.NewRabin()
	fp1 := s1.Fingerprint(h)
	h.Reset()
	fp2 := s2.Fingerprint(h)

	if !bytes.Equal(fp1, fp2) {
		t.Fatalf("namespace expansion: %x vs %x", fp1, fp2)
	}
}
