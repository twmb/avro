package avro

import (
	"bytes"
	"testing"
)

// Decoding empty Avro bytes into `any` must produce a NON-nil []byte.
// A nil []byte is nil-equivalent on re-encode (the documented nil-first
// union dispatch sends Go nil to the null branch), so a nil result flips
// {"bytes": ""} to null through any decode→re-encode pipeline:
//
//	["null","bytes"] wire 02 00 (bytes branch, length 0)
//	  → decode → []byte(nil) → re-encode → 00 (null branch)   ← corruption
//
// Java decodes empty bytes to an empty (non-null) ByteBuffer and fastavro
// to b'', both re-encoding onto the bytes branch; twmb's JSON decoder,
// deserFixed, and the unsafe udBytesDeser all already produce non-nil
// empties via make+copy. setBytesValue's interface arm was the one
// sibling manufacturing nil (append onto a nil base).
func TestRegression_EmptyBytesDecodeNonNil(t *testing.T) {
	s := MustParse(`"bytes"`)
	wire, err := s.AppendEncode(nil, []byte{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var a any
	if _, err := s.Decode(wire, &a); err != nil {
		t.Fatalf("decode: %v", err)
	}
	b, ok := a.([]byte)
	if !ok {
		t.Fatalf("decoded %T", a)
	}
	if b == nil {
		t.Fatal("empty bytes decoded into any as nil []byte; must be non-nil empty")
	}

	// The union round-trip consequence: the bytes branch survives a
	// decode→re-encode cycle.
	u := MustParse(`["null","bytes"]`)
	w1 := []byte{0x02, 0x00} // bytes branch, length 0
	var ua any
	if _, err := u.Decode(w1, &ua); err != nil {
		t.Fatalf("union decode: %v", err)
	}
	w2, err := u.AppendEncode(nil, ua)
	if err != nil {
		t.Fatalf("union re-encode: %v", err)
	}
	if !bytes.Equal(w2, w1) {
		t.Fatalf("union re-encode flipped branch: w1=%x w2=%x (empty bytes became null)", w1, w2)
	}

	// JSON decoder parity: also non-nil.
	var ja any
	if err := s.DecodeJSON([]byte(`""`), &ja); err != nil {
		t.Fatalf("decodeJSON: %v", err)
	}
	if jb, ok := ja.([]byte); !ok || jb == nil {
		t.Fatalf("JSON decoded %T nil=%v; want non-nil []byte", ja, ja == nil)
	}

	// The string→bytes promotion path shares setBytesValue, so a
	// promoted empty string also surfaces as non-nil bytes.
	ws := MustParse(`"string"`)
	rb := MustParse(`"bytes"`)
	res, err := Resolve(ws, rb)
	if err != nil {
		t.Fatalf("Resolve(string→bytes): %v", err)
	}
	emptyStr, _ := ws.AppendEncode(nil, "")
	var pa any
	if _, err := res.Decode(emptyStr, &pa); err != nil {
		t.Fatalf("promoted decode: %v", err)
	}
	if pb, ok := pa.([]byte); !ok || pb == nil {
		t.Fatalf("promoted empty string decoded as %T nil=%v; want non-nil []byte", pa, pa == nil)
	}

	// Encoding a Go-nil []byte still picks the null branch (the
	// documented nil-first dispatch is about ENCODE inputs, untouched).
	wNil, err := u.AppendEncode(nil, []byte(nil))
	if err != nil {
		t.Fatalf("encode nil []byte: %v", err)
	}
	if !bytes.Equal(wNil, []byte{0x00}) {
		t.Fatalf("nil []byte should encode to null branch: %x", wNil)
	}
}
