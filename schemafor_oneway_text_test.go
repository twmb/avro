package avro_test

import (
	"net"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// SchemaFor infers a "string" schema for a Go type that implements a text
// interface only when a string schema ROUND-TRIPS for that type: a string-kind
// or []byte-slice type round-trips via its kind regardless of which text
// methods it has, but any OTHER type round-trips only if it implements BOTH an
// encode-side method (TextMarshaler/AppendText) AND TextUnmarshaler. A
// non-string type implementing exactly one direction would yield a one-
// directional "string" schema whose unsupported direction fails at Encode or
// Decode far from the SchemaFor call; SchemaFor cannot guess which direction
// the caller wants, so it refuses at build time (the same strict-reject posture
// as logical-type tags on incompatible Go types).

// --- refused: non-string types with exactly one text direction ---

type sfTextDecodeOnly struct{ S string }

func (c *sfTextDecodeOnly) UnmarshalText(b []byte) error { c.S = string(b); return nil }

type sfTextEncodeOnly struct{ S string }

func (c sfTextEncodeOnly) MarshalText() ([]byte, error) { return []byte(c.S), nil }

// --- accepted: round-trippable text types ---

type sfTextBoth struct{ S string }

func (c sfTextBoth) MarshalText() ([]byte, error)  { return []byte(c.S), nil }
func (c *sfTextBoth) UnmarshalText(b []byte) error { c.S = string(b); return nil }

type sfStrEncodeOnly string // string KIND: round-trips via the reflect.String fallback

func (s sfStrEncodeOnly) MarshalText() ([]byte, error) { return []byte("v:" + string(s)), nil }

type sfBytesDecodeOnly []byte // []byte KIND: round-trips via the []byte fallback

func (b *sfBytesDecodeOnly) UnmarshalText(t []byte) error { *b = append((*b)[:0], t...); return nil }

func TestRegression_SchemaForOneWayTextRefused(t *testing.T) {
	t.Run("decode-only-struct-refused", func(t *testing.T) {
		type R struct{ V sfTextDecodeOnly }
		_, err := avro.SchemaFor[R]()
		if err == nil {
			t.Fatal("SchemaFor must refuse a non-string type implementing only TextUnmarshaler (it could decode from but not encode to a string schema)")
		}
		if !strings.Contains(err.Error(), "TextUnmarshaler") {
			t.Fatalf("error should name the missing/present text direction, got: %v", err)
		}
	})
	t.Run("encode-only-struct-refused", func(t *testing.T) {
		type R struct{ V sfTextEncodeOnly }
		_, err := avro.SchemaFor[R]()
		if err == nil {
			t.Fatal("SchemaFor must refuse a non-string type implementing only TextMarshaler (it could encode to but not decode from a string schema)")
		}
		if !strings.Contains(err.Error(), "TextMarshaler") {
			t.Fatalf("error should name the missing/present text direction, got: %v", err)
		}
	})
	t.Run("decode-only-struct-uuid-tag-refused", func(t *testing.T) {
		// The ,uuid arm has the same one-directional hazard.
		type R struct {
			V sfTextDecodeOnly `avro:"v,uuid"`
		}
		_, err := avro.SchemaFor[R]()
		if err == nil {
			t.Fatal("SchemaFor must refuse a ,uuid-tagged non-string type implementing only one text direction")
		}
	})
}

func TestRegression_SchemaForRoundTrippableTextStillBuilds(t *testing.T) {
	// Boundary-1: every type for which a string schema DOES round-trip must
	// still build and encode/decode. These must not regress when the
	// one-directional refusal above is added.
	t.Run("both-directions-struct", func(t *testing.T) {
		type R struct{ V sfTextBoth }
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatalf("a type implementing BOTH text directions must build: %v", err)
		}
		assertStringField(t, s)
		w, err := s.Encode(&R{V: sfTextBoth{S: "hi"}})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got R
		if _, err := s.Decode(w, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.V.S != "hi" {
			t.Fatalf("round-trip: got %q want %q", got.V.S, "hi")
		}
	})
	t.Run("string-kind-encode-only", func(t *testing.T) {
		type R struct{ V sfStrEncodeOnly }
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatalf("a string-KIND type round-trips via the kind fallback and must build: %v", err)
		}
		assertStringField(t, s)
		if _, err := s.Encode(&R{V: "x"}); err != nil {
			t.Fatalf("encode: %v", err)
		}
	})
	t.Run("byte-slice-decode-only", func(t *testing.T) {
		type R struct{ V sfBytesDecodeOnly }
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatalf("a []byte-slice type round-trips via the []byte fallback and must build: %v", err)
		}
		assertStringField(t, s)
		w, err := s.Encode(&R{V: sfBytesDecodeOnly("abc")})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got R
		if _, err := s.Decode(w, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
	})
	t.Run("net.IP-both-directions", func(t *testing.T) {
		type R struct{ IP net.IP }
		s, err := avro.SchemaFor[R]()
		if err != nil {
			t.Fatalf("net.IP (both text directions, []byte kind) must build: %v", err)
		}
		in := R{IP: net.ParseIP("192.168.1.7")}
		w, err := s.Encode(&in)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got R
		if _, err := s.Decode(w, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !in.IP.Equal(got.IP) {
			t.Fatalf("net.IP round-trip: got %v want %v", got.IP, in.IP)
		}
	})
}

func assertStringField(t *testing.T, s *avro.Schema) {
	t.Helper()
	if got := s.String(); !strings.Contains(got, `"string"`) {
		t.Fatalf("expected a string field schema, got: %s", got)
	}
}
