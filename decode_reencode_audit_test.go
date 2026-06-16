package avro

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

// TestDecodeReencodeSymmetry verifies that values the decoder produces
// can be re-encoded by the encoder. A pair where decode succeeds but
// encode of the decoded value fails would be a round-trip asymmetry.
//
// Exercises every Avro type with *any and one or more representative
// typed targets, then re-encodes each decoded value. Wire-byte
// canonicalization on the encoder side is fine; the Go-value
// round-trip is what we lock in.
//
// Documented intentional asymmetries (see SKIPPED_FOLLOWUPS.md) are
// excluded — see TestTextUnmarshalerOnlyAcceptedOnDecodeRejectedOnEncode
// for the TextUnmarshaler-only case.
func TestDecodeReencodeSymmetry(t *testing.T) {
	type tc struct {
		name   string
		schema string
		// build a fresh encoded payload for the test
		encoded func() []byte
		// list of decode target types to probe
		targets []func() any
	}

	// shorthand encoder
	enc := func(t *testing.T, schemaStr string, v any) []byte {
		s := MustParse(schemaStr)
		b, err := s.AppendEncode(nil, v)
		if err != nil {
			t.Fatalf("setup encode failed: %v", err)
		}
		return b
	}

	cases := []tc{
		{
			"boolean",
			`"boolean"`,
			func() []byte { return enc(t, `"boolean"`, true) },
			[]func() any{
				func() any { v := false; return &v },
				func() any { var v any; return &v },
			},
		},
		{
			"int",
			`"int"`,
			func() []byte { return enc(t, `"int"`, int32(42)) },
			[]func() any{
				func() any { var v int32; return &v },
				func() any { var v int64; return &v },
				func() any { var v float32; return &v },
				func() any { var v json.Number; return &v },
				func() any { var v any; return &v },
			},
		},
		{
			"long",
			`"long"`,
			func() []byte { return enc(t, `"long"`, int64(9007199254740993)) },
			[]func() any{
				func() any { var v int64; return &v },
				func() any { var v json.Number; return &v },
				func() any { var v any; return &v },
			},
		},
		{
			"double",
			`"double"`,
			func() []byte { return enc(t, `"double"`, 3.14) },
			[]func() any{
				func() any { var v float64; return &v },
				func() any { var v int64; return &v }, // decode rejects (3.14 is not whole-number); test verifies the skip-on-rejection path
				func() any { var v json.Number; return &v },
				func() any { var v any; return &v },
			},
		},
		{
			"string",
			`"string"`,
			func() []byte { return enc(t, `"string"`, "hello") },
			[]func() any{
				func() any { var v string; return &v },
				func() any { var v []byte; return &v },
				func() any { var v any; return &v },
			},
		},
		{
			"bytes",
			`"bytes"`,
			func() []byte { return enc(t, `"bytes"`, []byte{0xC3, 0xA9}) },
			[]func() any{
				func() any { var v []byte; return &v },
				func() any { var v string; return &v },
				func() any { var v any; return &v },
			},
		},
		{
			"date",
			`{"type":"int","logicalType":"date"}`,
			func() []byte {
				s := MustParse(`{"type":"int","logicalType":"date"}`)
				b, err := s.AppendEncode(nil, time.Date(2025, 5, 21, 0, 0, 0, 0, time.UTC))
				if err != nil {
					t.Fatalf("date encode: %v", err)
				}
				return b
			},
			[]func() any{
				func() any { var v time.Time; return &v },
				func() any { var v int32; return &v },
				func() any { var v string; return &v },
				func() any { var v any; return &v },
			},
		},
		{
			"timestamp-millis",
			`{"type":"long","logicalType":"timestamp-millis"}`,
			func() []byte {
				s := MustParse(`{"type":"long","logicalType":"timestamp-millis"}`)
				b, err := s.AppendEncode(nil, time.Date(2025, 5, 21, 12, 30, 0, 0, time.UTC))
				if err != nil {
					t.Fatalf("ts encode: %v", err)
				}
				return b
			},
			[]func() any{
				func() any { var v time.Time; return &v },
				func() any { var v int64; return &v },
				func() any { var v string; return &v },
				func() any { var v any; return &v },
			},
		},
		{
			"fixed(16)+uuid",
			`{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`,
			func() []byte {
				s := MustParse(`{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`)
				b, err := s.AppendEncode(nil, [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
				if err != nil {
					t.Fatalf("uuid encode: %v", err)
				}
				return b
			},
			[]func() any{
				func() any { var v [16]byte; return &v },
				func() any { var v string; return &v },
				func() any { var v any; return &v },
			},
		},
	}

	for _, c := range cases {
		for _, mk := range c.targets {
			target := mk()
			name := c.name + "/" + reflect.TypeOf(target).Elem().String()
			t.Run(name, func(t *testing.T) {
				wire := c.encoded()
				s := MustParse(c.schema)
				if _, err := s.Decode(wire, target); err != nil {
					// Decode rejection is fine; only
					// decode-succeeds-but-encode-fails is an
					// asymmetry.
					t.Logf("decode skipped (target rejects): %v", err)
					return
				}
				v := reflect.ValueOf(target).Elem().Interface()
				if _, err := s.AppendEncode(nil, v); err != nil {
					t.Fatalf("ASYMMETRY: decode produced %T(%v) but encode of that value rejects: %v", v, v, err)
				}
			})
		}
	}
}

// textUnmarshalerOnly implements UnmarshalText but not MarshalText or
// AppendText — the standard Go pattern for parse-only types (config
// values, enums, lookup keys, one-way ingest pipelines). Decode
// accepts; encode of the produced value rejects because the type
// provides no text-out method. This asymmetry matches Go's stdlib
// idiom — TextUnmarshaler is explicitly a one-way interface — and
// the library doesn't force users to write no-op MarshalText shims
// on types they only ever decode into.
type textUnmarshalerOnly struct{ Got string }

func (t *textUnmarshalerOnly) UnmarshalText(b []byte) error {
	t.Got = string(b)
	return nil
}

// TestTextUnmarshalerOnlyDecodeOnly pins the documented one-way
// pattern: Decode into a TextUnmarshaler-only target succeeds; the
// caller doesn't need a sibling MarshalText. Re-encoding the produced
// value would fail (no text-out method), but that's by user choice —
// the type is decode-only.
func TestTextUnmarshalerOnlyDecodeOnly(t *testing.T) {
	s := MustParse(`"string"`)
	wire, err := s.AppendEncode(nil, "hello")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var got textUnmarshalerOnly
	if _, err := s.Decode(wire, &got); err != nil {
		t.Fatalf("decode into TextUnmarshaler-only: %v", err)
	}
	if got.Got != "hello" {
		t.Fatalf("UnmarshalText not called: got %q", got.Got)
	}

	// Re-encoding the value rejects because the type has no text-out
	// method. Standard Go one-way pattern.
	if _, err := s.AppendEncode(nil, &got); err == nil {
		t.Fatalf("expected encode rejection of TextUnmarshaler-only value; got success")
	}
}

// ptrMarshalerSymmetry has MarshalText and UnmarshalText both on the
// pointer receiver. Used to verify symmetric encode/decode discovery
// of pointer-receiver text methods.
type ptrMarshalerSymmetry struct{ val string }

func (m *ptrMarshalerSymmetry) MarshalText() ([]byte, error) { return []byte(m.val), nil }
func (m *ptrMarshalerSymmetry) UnmarshalText(b []byte) error { m.val = string(b); return nil }

// colorEnum demonstrates a type with both MarshalText and UnmarshalText
// used as an Avro enum carrier (the text matches a symbol).
type colorEnum struct{ symbol string }

func (c colorEnum) MarshalText() ([]byte, error) { return []byte(c.symbol), nil }
func (c *colorEnum) UnmarshalText(b []byte) error {
	c.symbol = string(b)
	return nil
}

// uuidViaText demonstrates a type that carries a UUID via the Text*
// interfaces. The encode side parses the text as a UUID; the decode
// side receives the canonical hex-dash form.
type uuidViaText struct{ s string }

func (u uuidViaText) MarshalText() ([]byte, error) { return []byte(u.s), nil }
func (u *uuidViaText) UnmarshalText(b []byte) error {
	u.s = string(b)
	return nil
}

// TestTextInterfaceCoverageForEnumAndFixedUUID pins that the text-shaped
// Avro sites — enum and fixed+uuid — accept Text* on both binary and
// JSON paths. Parity with string and string+uuid which already accept.
func TestTextInterfaceCoverageForEnumAndFixedUUID(t *testing.T) {
	t.Run("enum binary round-trip via Text*", func(t *testing.T) {
		s := MustParse(`{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]}`)
		wire, err := s.AppendEncode(nil, colorEnum{symbol: "GREEN"})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got colorEnum
		if _, err := s.Decode(wire, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.symbol != "GREEN" {
			t.Fatalf("round-trip: got %q, want GREEN", got.symbol)
		}
	})
	t.Run("enum binary unknown symbol rejects", func(t *testing.T) {
		s := MustParse(`{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]}`)
		if _, err := s.AppendEncode(nil, colorEnum{symbol: "PURPLE"}); err == nil {
			t.Fatalf("expected encode rejection of unknown symbol")
		}
	})
	t.Run("enum JSON round-trip via Text*", func(t *testing.T) {
		s := MustParse(`{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]}`)
		out, err := s.EncodeJSON(colorEnum{symbol: "BLUE"})
		if err != nil {
			t.Fatalf("EncodeJSON: %v", err)
		}
		if string(out) != `"BLUE"` {
			t.Fatalf("EncodeJSON got %s, want \"BLUE\"", out)
		}
		var got colorEnum
		if err := s.DecodeJSON([]byte(`"RED"`), &got); err != nil {
			t.Fatalf("DecodeJSON: %v", err)
		}
		if got.symbol != "RED" {
			t.Fatalf("DecodeJSON got %q, want RED", got.symbol)
		}
	})

	const uuidSchema = `{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`
	const uuidStr = "12345678-1234-5678-1234-567812345678"

	t.Run("fixed+uuid binary round-trip via Text*", func(t *testing.T) {
		s := MustParse(uuidSchema)
		wire, err := s.AppendEncode(nil, uuidViaText{s: uuidStr})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		if len(wire) != 16 {
			t.Fatalf("expected 16-byte wire, got %d", len(wire))
		}
		var got uuidViaText
		if _, err := s.Decode(wire, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.s != uuidStr {
			t.Fatalf("round-trip: got %q, want %q", got.s, uuidStr)
		}
	})
	t.Run("fixed+uuid binary malformed text rejects", func(t *testing.T) {
		s := MustParse(uuidSchema)
		if _, err := s.AppendEncode(nil, uuidViaText{s: "not-a-uuid"}); err == nil {
			t.Fatalf("expected encode rejection of non-UUID text")
		}
	})
	t.Run("fixed+uuid JSON round-trip via Text*", func(t *testing.T) {
		s := MustParse(uuidSchema)
		out, err := s.EncodeJSON(uuidViaText{s: uuidStr})
		if err != nil {
			t.Fatalf("EncodeJSON: %v", err)
		}
		var got uuidViaText
		if err := s.DecodeJSON(out, &got); err != nil {
			t.Fatalf("DecodeJSON: %v", err)
		}
		if got.s != uuidStr {
			t.Fatalf("JSON round-trip: got %q, want %q", got.s, uuidStr)
		}
	})
}

// TestMapRecordEncodeIgnoresAliases pins that map-record encoding
// looks up only by canonical field name across every map encode path
// (binary map[string]any, binary typed-map with plain string key,
// binary typed-map with a named string-key subtype, plus the JSON
// equivalents). Aliases are a reader-side / decode concept (Avro 1.12
// spec; Apache Avro Java GenericDatumWriter; fastavro write_record
// — none of the three reference impls consult aliases on encode);
// our encode matches that. Input keyed by an alias hits the missing-
// field path just like any other unrecognized key. Canonical present
// + extra alias key is silently accepted (the alias key is simply not
// consulted — it's an unrecognized stray, not a collision).
func TestMapRecordEncodeIgnoresAliases(t *testing.T) {
	schema := `{"type":"record","name":"R","fields":[
		{"name":"new_name","type":"long","aliases":["old_name"]}
	]}`
	s := MustParse(schema)

	// --- binary encode ---

	t.Run("binary map[string]any via alias key: missing-field error", func(t *testing.T) {
		if _, err := s.AppendEncode(nil, map[string]any{"old_name": int64(42)}); err == nil {
			t.Fatalf("expected missing-key error; encode silently accepted the alias")
		}
	})

	t.Run("binary map[string]any via canonical key: succeeds", func(t *testing.T) {
		if _, err := s.AppendEncode(nil, map[string]any{"new_name": int64(42)}); err != nil {
			t.Fatalf("binary encode with canonical key: %v", err)
		}
	})

	t.Run("binary map[string]any canonical+stray alias: alias silently ignored", func(t *testing.T) {
		// Canonical present, alias also present; encoder iterates schema
		// fields by canonical name, so the alias key is a stray and is
		// simply not consulted. Same contract as any other unrecognized
		// key in the input map.
		out, err := s.AppendEncode(nil, map[string]any{"new_name": int64(42), "old_name": int64(99)})
		if err != nil {
			t.Fatalf("canonical+stray-alias should succeed: %v", err)
		}
		var got map[string]any
		if _, err := s.Decode(out, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got["new_name"] != int64(42) {
			t.Fatalf("expected new_name=42 (stray old_name ignored); got %v", got)
		}
	})

	t.Run("binary map[string]int64 via alias key: missing-field error", func(t *testing.T) {
		// Exercises the typed-map encode path (map[string]T with plain
		// string key — different from map[string]any).
		if _, err := s.AppendEncode(nil, map[string]int64{"old_name": 42}); err == nil {
			t.Fatalf("expected missing-key error on typed-map encode; encode accepted alias")
		}
	})

	t.Run("binary map[NK]int64 via alias key: missing-field error", func(t *testing.T) {
		// Exercises the typed-map encode path with a named-string-key
		// subtype (forces mapKeyAs.Convert).
		type NK string
		if _, err := s.AppendEncode(nil, map[NK]int64{"old_name": 42}); err == nil {
			t.Fatalf("expected missing-key error on named-key typed-map encode; encode accepted alias")
		}
	})

	t.Run("binary map[NK]int64 via canonical key: succeeds", func(t *testing.T) {
		type NK string
		if _, err := s.AppendEncode(nil, map[NK]int64{"new_name": 42}); err != nil {
			t.Fatalf("named-key typed-map encode with canonical key: %v", err)
		}
	})

	// --- JSON encode ---

	t.Run("JSON map[string]any via alias key: missing-field error", func(t *testing.T) {
		if _, err := s.EncodeJSON(map[string]any{"old_name": int64(42)}); err == nil {
			t.Fatalf("expected missing-field error; JSON encode silently accepted the alias")
		}
	})

	t.Run("JSON map[string]int64 via alias key: missing-field error", func(t *testing.T) {
		// JSON typed-map (non-map[string]any) generic path.
		if _, err := s.EncodeJSON(map[string]int64{"old_name": 42}); err == nil {
			t.Fatalf("expected missing-field error on JSON typed-map encode; alias accepted")
		}
	})
}

// TestPointerReceiverTextMarshalerSymmetry verifies that the encoder
// reaches a pointer-receiver MarshalText via v.Addr(), matching the
// decoder's TextUnmarshaler lookup via v.Addr(). Without the Addr()
// hop, only value-method-set MarshalText would resolve, silently
// missing a pointer-receiver MarshalText on an addressable struct
// field.
func TestPointerReceiverTextMarshalerSymmetry(t *testing.T) {
	t.Run("via pointer", func(t *testing.T) {
		s := MustParse(`"string"`)
		v := &ptrMarshalerSymmetry{val: "hello"}
		wire, err := s.AppendEncode(nil, v)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got ptrMarshalerSymmetry
		if _, err := s.Decode(wire, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.val != "hello" {
			t.Fatalf("round-trip got %q, want %q", got.val, "hello")
		}
	})
	t.Run("via struct field", func(t *testing.T) {
		type wrapper struct {
			Name ptrMarshalerSymmetry `avro:"name"`
		}
		s := MustParse(`{"type":"record","name":"R","fields":[{"name":"name","type":"string"}]}`)
		in := wrapper{Name: ptrMarshalerSymmetry{val: "world"}}
		wire, err := s.AppendEncode(nil, &in)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got wrapper
		if _, err := s.Decode(wire, &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.Name.val != "world" {
			t.Fatalf("round-trip got %q, want %q", got.Name.val, "world")
		}
	})
}
