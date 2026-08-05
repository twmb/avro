package avro

import (
	"encoding/binary"
	"testing"
)

func TestSingleObjectRoundTrip(t *testing.T) {
	t.Run("null", func(t *testing.T) {
		s, err := Parse(`"null"`)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := s.AppendSingleObject(nil, (*int)(nil))
		if err != nil {
			t.Fatalf("AppendSingleObject: %v", err)
		}
		if encoded[0] != 0xC3 || encoded[1] != 0x01 {
			t.Fatalf("bad magic: [%#x, %#x]", encoded[0], encoded[1])
		}
		var got *int
		rest, err := s.DecodeSingleObject(encoded, &got)
		if err != nil {
			t.Fatalf("DecodeSingleObject: %v", err)
		}
		if len(rest) != 0 {
			t.Fatalf("unexpected remaining bytes: %d", len(rest))
		}
	})

	tests := []struct {
		name   string
		schema string
		val    any
	}{
		{"boolean", `"boolean"`, new(bool)},
		{"int", `"int"`, new(int32)},
		{"long", `"long"`, new(int64)},
		{"string", `"string"`, new(string)},
		{
			"record", `{"type":"record","name":"r","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`,
			&map[string]any{"a": int32(7), "b": "world"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := Parse(tt.schema)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}

			encoded, err := s.AppendSingleObject(nil, tt.val)
			if err != nil {
				t.Fatalf("AppendSingleObject: %v", err)
			}

			if len(encoded) < 10 {
				t.Fatalf("encoded too short: %d", len(encoded))
			}
			if encoded[0] != 0xC3 || encoded[1] != 0x01 {
				t.Fatalf("bad magic: [%#x, %#x]", encoded[0], encoded[1])
			}

			var got any
			rest, err := s.DecodeSingleObject(encoded, &got)
			if err != nil {
				t.Fatalf("DecodeSingleObject: %v", err)
			}
			if len(rest) != 0 {
				t.Fatalf("unexpected remaining bytes: %d", len(rest))
			}
		})
	}
}

func TestSingleObjectFingerprint(t *testing.T) {
	s, err := Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := s.AppendSingleObject(nil, new(int32))
	if err != nil {
		t.Fatal(err)
	}

	fp, rest, err := SingleObjectFingerprint(encoded)
	if err != nil {
		t.Fatalf("SingleObjectFingerprint: %v", err)
	}

	// Verify fingerprint matches the schema's precomputed one.
	var want [8]byte
	copy(want[:], s.soe[2:10])
	if fp != want {
		t.Fatalf("fingerprint mismatch: got %x, want %x", fp, want)
	}

	// Verify rest is the payload (binary encoded int 0).
	if len(rest) == 0 {
		t.Fatal("expected non-empty rest")
	}
}

func TestSingleObjectFingerprintMismatch(t *testing.T) {
	a, err := Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Parse(`"string"`)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := a.AppendSingleObject(nil, new(int32))
	if err != nil {
		t.Fatal(err)
	}

	var got string
	_, err = b.DecodeSingleObject(encoded, &got)
	if err == nil {
		t.Fatal("expected fingerprint mismatch error")
	}
}

func TestSingleObjectBadMagic(t *testing.T) {
	s, err := Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := s.AppendSingleObject(nil, new(int32))
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt magic bytes.
	encoded[0] = 0x00
	encoded[1] = 0x00

	var got int32
	_, err = s.DecodeSingleObject(encoded, &got)
	if err == nil {
		t.Fatal("expected bad magic error")
	}

	// SingleObjectFingerprint should also fail.
	_, _, err = SingleObjectFingerprint(encoded)
	if err == nil {
		t.Fatal("expected bad magic error from SingleObjectFingerprint")
	}
}

func TestSingleObjectShortBuffer(t *testing.T) {
	s, err := Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	for _, n := range []int{0, 1, 5, 9} {
		data := make([]byte, n)
		var got int32
		_, err := s.DecodeSingleObject(data, &got)
		if err == nil {
			t.Fatalf("expected short buffer error for %d bytes", n)
		}

		_, _, err = SingleObjectFingerprint(data)
		if err == nil {
			t.Fatalf("expected short buffer error from SingleObjectFingerprint for %d bytes", n)
		}
	}
}

func TestSingleObjectFingerprintMatchesSpec(t *testing.T) {
	// Verify the fingerprint bytes are little-endian CRC-64-AVRO.
	s, err := Parse(`"int"`)
	if err != nil {
		t.Fatal(err)
	}

	h := NewRabin()
	h.Write(s.Canonical())
	sum := h.Sum64()

	var want [8]byte
	binary.LittleEndian.PutUint64(want[:], sum)

	var got [8]byte
	copy(got[:], s.soe[2:10])

	if got != want {
		t.Fatalf("SOE fingerprint does not match LE CRC-64-AVRO: got %x, want %x", got, want)
	}
}

// TestRegression_ResolvedDecodeSingleObjectAcceptsWriterFingerprint pins
// that a schema returned by Resolve(writer, reader) accepts SOE wire bytes
// bearing the WRITER schema's fingerprint. The SOE wire format puts the
// schema-that-produced-the-bytes' fingerprint on the wire (Avro spec),
// which is the writer; the resolved schema is the right thing to decode
// those bytes into a reader-shaped Go value. Java's BinaryMessageDecoder
// dispatches the wire fingerprint via a writer-fingerprint→codec registry;
// twmb stores the writer fingerprint on the resolved Schema so its
// DecodeSingleObject accepts both writer and reader fingerprints.
func TestRegression_ResolvedDecodeSingleObjectAcceptsWriterFingerprint(t *testing.T) {
	writer, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},
		{"name":"b","type":"string"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"}
	]}`)
	if err != nil {
		t.Fatal(err)
	}

	// Writer produces SOE wire bearing writer.soe.
	wire, err := writer.AppendSingleObject(nil, map[string]any{
		"a": int32(7),
		"b": "hello",
	})
	if err != nil {
		t.Fatalf("writer.AppendSingleObject: %v", err)
	}
	if [10]byte(wire[:10]) != writer.soe {
		t.Fatalf("wire header is not writer.soe")
	}

	resolved, err := Resolve(writer, reader)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	// Resolved schema must decode writer-fingerprinted wire (primary case).
	var got map[string]any
	rest, err := resolved.DecodeSingleObject(wire, &got)
	if err != nil {
		t.Fatalf("resolved.DecodeSingleObject(writer wire): %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("unexpected remaining bytes: %d", len(rest))
	}
	if got["a"] != int32(7) {
		t.Fatalf("a: got %v, want int32(7)", got["a"])
	}
	if _, present := got["b"]; present {
		t.Fatalf("b: expected projected out by reader, got %v", got["b"])
	}

	// A completely unrelated schema's fingerprint is still rejected.
	other := MustParse(`{"type":"record","name":"Other","fields":[{"name":"x","type":"int"}]}`)
	otherWire, err := other.AppendSingleObject(nil, map[string]any{"x": int32(1)})
	if err != nil {
		t.Fatalf("other.AppendSingleObject: %v", err)
	}
	if _, err := resolved.DecodeSingleObject(otherWire, &got); err == nil {
		t.Fatalf("resolved.DecodeSingleObject(unrelated wire) accepted; want fingerprint mismatch")
	}
}

// TestRegression_NonResolvedDecodeSingleObjectRejectsForeignFingerprint
// pins that a non-resolved schema continues to reject SOE wire whose
// fingerprint doesn't match its own — the zero-valued writerSoe must
// never silently accept arbitrary input.
func TestRegression_NonResolvedDecodeSingleObjectRejectsForeignFingerprint(t *testing.T) {
	a := MustParse(`{"type":"record","name":"A","fields":[{"name":"f","type":"int"}]}`)
	b := MustParse(`{"type":"record","name":"B","fields":[{"name":"f","type":"int"}]}`)
	wire, err := a.AppendSingleObject(nil, map[string]any{"f": int32(1)})
	if err != nil {
		t.Fatalf("a.AppendSingleObject: %v", err)
	}
	var got map[string]any
	if _, err := b.DecodeSingleObject(wire, &got); err == nil {
		t.Fatalf("b.DecodeSingleObject(a-wire) accepted; want fingerprint mismatch")
	}
}
