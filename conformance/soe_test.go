package conformance

import (
	"testing"

	"github.com/twmb/avro"
)

// -----------------------------------------------------------------------
// Single Object Encoding
// Spec: "Single Object Encoding" — a framing for single Avro values:
//   [0xC3, 0x01] + 8-byte little-endian CRC-64-AVRO fingerprint + payload
// https://avro.apache.org/docs/1.12.0/specification/#single-object-encoding
// -----------------------------------------------------------------------

func TestSOERoundTrip(t *testing.T) {
	schema := mustParse(t, `"int"`)
	v := int32(42)
	data, err := schema.AppendSingleObject(nil, &v)
	if err != nil {
		t.Fatal(err)
	}

	var out int32
	_, err = schema.DecodeSingleObject(data, &out)
	if err != nil {
		t.Fatal(err)
	}
	if out != 42 {
		t.Fatalf("got %d, want 42", out)
	}
}

func TestSOEMagicBytes(t *testing.T) {
	schema := mustParse(t, `"string"`)
	v := "hello"
	data, err := schema.AppendSingleObject(nil, &v)
	if err != nil {
		t.Fatal(err)
	}

	if len(data) < 2 {
		t.Fatal("data too short")
	}
	if data[0] != 0xC3 || data[1] != 0x01 {
		t.Fatalf("magic: got [%#x, %#x], want [0xc3, 0x01]", data[0], data[1])
	}
}

func TestSOEFingerprintMatch(t *testing.T) {
	schema := mustParse(t, `"long"`)
	v := int64(100)
	data, err := schema.AppendSingleObject(nil, &v)
	if err != nil {
		t.Fatal(err)
	}

	// Extract fingerprint from SOE (stored as little-endian uint64).
	fp, _, err := avro.SingleObjectFingerprint(data)
	if err != nil {
		t.Fatal(err)
	}

	// Schema.Fingerprint returns big-endian via h.Sum(nil).
	// SOE stores little-endian. Convert for comparison.
	h := avro.NewRabin()
	schemaFP := schema.Fingerprint(h)

	// Reverse schemaFP to get LE for comparison.
	var schemaFPLE [8]byte
	for i := range 8 {
		schemaFPLE[i] = schemaFP[7-i]
	}

	if fp != schemaFPLE {
		t.Fatalf("embedded fp %x != schema fp (LE) %x", fp, schemaFPLE)
	}
}

func TestSOEFingerprintMismatch(t *testing.T) {
	schema1 := mustParse(t, `"int"`)
	schema2 := mustParse(t, `"string"`)

	v := int32(42)
	data, err := schema1.AppendSingleObject(nil, &v)
	if err != nil {
		t.Fatal(err)
	}

	// Decoding with a different schema should fail.
	var out string
	_, err = schema2.DecodeSingleObject(data, &out)
	if err == nil {
		t.Fatal("expected error for fingerprint mismatch")
	}
}

func TestSOETruncatedData(t *testing.T) {
	// Less than 10 bytes should fail.
	short := []byte{0xC3, 0x01, 0x00, 0x00}
	schema := mustParse(t, `"int"`)
	var out int32
	_, err := schema.DecodeSingleObject(short, &out)
	if err == nil {
		t.Fatal("expected error for truncated SOE data")
	}

	// Empty input.
	_, err = schema.DecodeSingleObject(nil, &out)
	if err == nil {
		t.Fatal("expected error for nil SOE data")
	}
}

func TestSOEFingerprintExtraction(t *testing.T) {
	schema := mustParse(t, `"double"`)
	v := 3.14
	data, err := schema.AppendSingleObject(nil, &v)
	if err != nil {
		t.Fatal(err)
	}

	fp, rest, err := avro.SingleObjectFingerprint(data)
	if err != nil {
		t.Fatal(err)
	}

	// fp should be 8 bytes.
	if len(fp) != 8 {
		t.Fatalf("fp length: got %d, want 8", len(fp))
	}

	// rest should be the payload (double = 8 bytes).
	if len(rest) != 8 {
		t.Fatalf("rest length: got %d, want 8", len(rest))
	}

	// Decode rest directly.
	var out float64
	_, err = schema.Decode(rest, &out)
	if err != nil {
		t.Fatal(err)
	}
	if out != 3.14 {
		t.Fatalf("got %v, want 3.14", out)
	}
}
