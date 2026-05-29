package avro

import (
	"errors"
	"strings"
	"testing"
)

// The text-interface (TextMarshaler / AppendText / TextUnmarshaler)
// precedence contract, pinned here for the string / enum / uuid sites on
// both the binary and JSON paths:
//
//   - For a bytes-shaped UUID wire (fixed+uuid), a [16]byte-shaped Go type
//     is authoritative by its raw bytes; the text interface is NOT
//     consulted. The 16 bytes ARE the UUID, so a MarshalText->parseUUID
//     round trip would be redundant and would let a non-canonical text
//     method diverge the binary and JSON wire.
//   - Everywhere the text interface IS consulted, it is tried BEFORE the
//     reflect.String fast path (and, for enum, before the int-ordinal
//     arm), matching encoding/json's preference for TextMarshaler and
//     Java's name-based enum matching.

// nonCanonicalArrUUID is a [16]byte that also implements the text
// interfaces, deliberately NON-canonically: MarshalText ignores the bytes
// and returns the all-zero UUID; UnmarshalText ignores its input and
// writes all-0xFF. For fixed+uuid the wire is the raw 16 bytes, so if
// either text method fired the encoded/decoded value would reflect
// zeros/0xFF and the binary and JSON paths would disagree.
type nonCanonicalArrUUID [16]byte

func (nonCanonicalArrUUID) MarshalText() ([]byte, error) {
	return []byte("00000000-0000-0000-0000-000000000000"), nil
}

func (u *nonCanonicalArrUUID) UnmarshalText([]byte) error {
	for i := range u {
		u[i] = 0xFF
	}
	return nil
}

func TestRegression_FixedUUIDByteArrayTrustsRawBytes(t *testing.T) {
	s := MustParse(`{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`)
	in := nonCanonicalArrUUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	bin, err := s.Encode(in)
	if err != nil {
		t.Fatalf("binary encode: %v", err)
	}
	// fixed has no length prefix; the wire is exactly the raw 16 bytes.
	// If MarshalText fired the wire would be the all-zero UUID's bytes.
	if len(bin) != 16 {
		t.Fatalf("binary wire len = %d, want 16", len(bin))
	}
	for i := range bin {
		if bin[i] != byte(i+1) {
			t.Fatalf("binary wire = % x, want raw 01..10 (MarshalText must not fire)", bin)
		}
	}

	jsonW, err := s.EncodeJSON(in)
	if err != nil {
		t.Fatalf("json encode: %v", err)
	}

	// Both wires decode back to the raw bytes; the [16]byte target trusts
	// them (UnmarshalText, which would write 0xFF, must not fire). Binary
	// and JSON must agree.
	var binBack, jsonBack nonCanonicalArrUUID
	if _, err := s.Decode(bin, &binBack); err != nil {
		t.Fatalf("binary decode: %v", err)
	}
	if err := s.DecodeJSON(jsonW, &jsonBack); err != nil {
		t.Fatalf("json decode: %v", err)
	}
	if binBack != in {
		t.Fatalf("binary round-trip = % x, want % x (UnmarshalText must not fire)", binBack[:], in[:])
	}
	if jsonBack != in {
		t.Fatalf("json round-trip = % x, want % x (UnmarshalText must not fire)", jsonBack[:], in[:])
	}
	if binBack != jsonBack {
		t.Fatalf("binary vs JSON decode diverge: % x vs % x", binBack[:], jsonBack[:])
	}
}

// upperString is a string-kind type whose text methods transform the
// value (MarshalText uppercases, UnmarshalText lowercases). A string-kind
// type implementing TextMarshaler must use the marshaled form rather than
// its raw underlying string, matching encoding/json and keeping binary and
// JSON in lockstep.
type upperString string

func (u upperString) MarshalText() ([]byte, error) {
	return []byte(strings.ToUpper(string(u))), nil
}

func (u *upperString) UnmarshalText(b []byte) error {
	*u = upperString(strings.ToLower(string(b)))
	return nil
}

func TestRegression_StringKindPrefersTextMarshaler(t *testing.T) {
	s := MustParse(`"string"`)
	in := upperString("hello")

	bin, err := s.Encode(in)
	if err != nil {
		t.Fatalf("binary encode: %v", err)
	}
	// 1-byte length prefix (zigzag(5)=0x0a), then the MARSHALED form
	// "HELLO" — not the raw underlying string "hello".
	if got := string(bin[1:]); got != "HELLO" {
		t.Fatalf("binary wire body = %q, want HELLO (TextMarshaler, not the raw string)", got)
	}
	jsonW, err := s.EncodeJSON(in)
	if err != nil {
		t.Fatalf("json encode: %v", err)
	}
	if string(jsonW) != `"HELLO"` {
		t.Fatalf("json wire = %s, want \"HELLO\"", jsonW)
	}

	// Decode applies UnmarshalText (lowercases): wire "HELLO" -> "hello"
	// on both paths.
	var binBack, jsonBack upperString
	if _, err := s.Decode(bin, &binBack); err != nil {
		t.Fatalf("binary decode: %v", err)
	}
	if err := s.DecodeJSON(jsonW, &jsonBack); err != nil {
		t.Fatalf("json decode: %v", err)
	}
	if binBack != "hello" {
		t.Fatalf("binary decode = %q, want hello (UnmarshalText lowercases)", binBack)
	}
	if jsonBack != "hello" {
		t.Fatalf("json decode = %q, want hello (UnmarshalText lowercases)", jsonBack)
	}
}

// TestRegression_StringKindTextMarshalerConsistentAcrossContexts pins that
// a string-kind type with text methods is handled identically whether it
// appears as a scalar, a struct field, or a container element. The unsafe
// struct fast paths (usString / udStringDeser) and the array/map fast loops
// (which capture reflect.Value.SetString) bypass appendAvroString /
// setStringValue, so they must be gated off for text-method types — else a
// struct field or container element would encode/decode its raw string
// while the same value as a scalar uses MarshalText/UnmarshalText.
func TestRegression_StringKindTextMarshalerConsistentAcrossContexts(t *testing.T) {
	// struct field — exercises usString / udStringDeser compile gates.
	t.Run("struct field", func(t *testing.T) {
		type rec struct {
			F upperString `avro:"f"`
		}
		s := MustParse(`{"type":"record","name":"R","fields":[{"name":"f","type":"string"}]}`)
		bin, err := s.Encode(rec{F: "hello"})
		if err != nil {
			t.Fatalf("binary encode: %v", err)
		}
		js, err := s.EncodeJSON(rec{F: "hello"})
		if err != nil {
			t.Fatalf("json encode: %v", err)
		}
		if !strings.Contains(string(bin), "HELLO") {
			t.Fatalf("struct binary = %q, want the MarshalText form HELLO", bin)
		}
		if !strings.Contains(string(js), "HELLO") {
			t.Fatalf("struct json = %s, want the MarshalText form HELLO", js)
		}
		var binBack, jsonBack rec
		if _, err := s.Decode(bin, &binBack); err != nil {
			t.Fatalf("binary decode: %v", err)
		}
		if err := s.DecodeJSON(js, &jsonBack); err != nil {
			t.Fatalf("json decode: %v", err)
		}
		if binBack.F != "hello" || jsonBack.F != "hello" {
			t.Fatalf("struct decode bin=%q json=%q, want hello (UnmarshalText)", binBack.F, jsonBack.F)
		}
	})

	// array element — exercises the deserArrayStringLoop fast-loop gate.
	t.Run("array element", func(t *testing.T) {
		s := MustParse(`{"type":"array","items":"string"}`)
		bin, err := s.Encode([]upperString{"hello", "world"})
		if err != nil {
			t.Fatalf("binary encode: %v", err)
		}
		js, err := s.EncodeJSON([]upperString{"hello", "world"})
		if err != nil {
			t.Fatalf("json encode: %v", err)
		}
		if string(js) != `["HELLO","WORLD"]` {
			t.Fatalf("array json = %s, want [\"HELLO\",\"WORLD\"]", js)
		}
		var binBack, jsonBack []upperString
		if _, err := s.Decode(bin, &binBack); err != nil {
			t.Fatalf("binary decode: %v", err)
		}
		if err := s.DecodeJSON(js, &jsonBack); err != nil {
			t.Fatalf("json decode: %v", err)
		}
		want := []upperString{"hello", "world"}
		for i := range want {
			if binBack[i] != want[i] || jsonBack[i] != want[i] {
				t.Fatalf("array decode bin=%v json=%v, want %v (UnmarshalText)", binBack, jsonBack, want)
			}
		}
	})

	// map value — exercises the deserMapStringBlock fast-loop gate.
	t.Run("map value", func(t *testing.T) {
		s := MustParse(`{"type":"map","values":"string"}`)
		bin, err := s.Encode(map[string]upperString{"k": "hello"})
		if err != nil {
			t.Fatalf("binary encode: %v", err)
		}
		js, err := s.EncodeJSON(map[string]upperString{"k": "hello"})
		if err != nil {
			t.Fatalf("json encode: %v", err)
		}
		if !strings.Contains(string(bin), "HELLO") || !strings.Contains(string(js), "HELLO") {
			t.Fatalf("map encode bin=%q json=%s, want MarshalText form HELLO", bin, js)
		}
		var binBack, jsonBack map[string]upperString
		if _, err := s.Decode(bin, &binBack); err != nil {
			t.Fatalf("binary decode: %v", err)
		}
		if err := s.DecodeJSON(js, &jsonBack); err != nil {
			t.Fatalf("json decode: %v", err)
		}
		if binBack["k"] != "hello" || jsonBack["k"] != "hello" {
			t.Fatalf("map decode bin=%q json=%q, want hello (UnmarshalText)", binBack["k"], jsonBack["k"])
		}
	})
}

var errUnknownOrdinalColor = errors.New("unknown ordinalColor symbol")

var enumColorNames = [...]string{"RED", "GREEN", "BLUE"}

// ordinalColor is an int-kind enum carrier with name-based text methods.
// Its Go integer values (ordRed=0, ordGreen=1, ordBlue=2) deliberately do
// NOT line up with the Avro symbol order used in the test
// (["BLUE","GREEN","RED"], where RED is ordinal 2). Name-based matching
// via the text interface must win over trusting the Go int as the ordinal.
type ordinalColor int

const (
	ordRed ordinalColor = iota
	ordGreen
	ordBlue
)

func (c ordinalColor) MarshalText() ([]byte, error) {
	if int(c) < 0 || int(c) >= len(enumColorNames) {
		return nil, errUnknownOrdinalColor
	}
	return []byte(enumColorNames[c]), nil
}

func (c *ordinalColor) UnmarshalText(b []byte) error {
	for i, n := range enumColorNames {
		if n == string(b) {
			*c = ordinalColor(i)
			return nil
		}
	}
	return errUnknownOrdinalColor
}

func TestRegression_EnumTextMarshalerNameMatchOverOrdinal(t *testing.T) {
	// Avro symbol order differs from the Go int order: "RED" is Avro
	// ordinal 2 here, while ordRed is Go int 0.
	s := MustParse(`{"type":"enum","name":"C","symbols":["BLUE","GREEN","RED"]}`)
	in := ordRed // Go 0; symbol "RED"; Avro ordinal 2

	bin, err := s.Encode(in)
	if err != nil {
		t.Fatalf("binary encode: %v", err)
	}
	// zigzag(2) = 0x04. If the encoder trusted the Go int (0) as the
	// ordinal, the wire would be zigzag(0) = 0x00 = "BLUE".
	if len(bin) != 1 || bin[0] != 0x04 {
		t.Fatalf("binary wire = % x, want 04 (RED = Avro ordinal 2, name-matched)", bin)
	}
	jsonW, err := s.EncodeJSON(in)
	if err != nil {
		t.Fatalf("json encode: %v", err)
	}
	if string(jsonW) != `"RED"` {
		t.Fatalf("json wire = %s, want \"RED\"", jsonW)
	}

	// Round-trip: wire ordinal 2 = "RED" -> UnmarshalText -> ordRed (Go 0).
	var binBack, jsonBack ordinalColor
	if _, err := s.Decode(bin, &binBack); err != nil {
		t.Fatalf("binary decode: %v", err)
	}
	if err := s.DecodeJSON(jsonW, &jsonBack); err != nil {
		t.Fatalf("json decode: %v", err)
	}
	if binBack != ordRed {
		t.Fatalf("binary decode = %d, want ordRed=0 (name-matched UnmarshalText)", binBack)
	}
	if jsonBack != ordRed {
		t.Fatalf("json decode = %d, want ordRed=0", jsonBack)
	}
}
