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
		{"record", `{"type":"record","name":"r","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`,
			&map[string]any{"a": int32(7), "b": "world"}},
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
