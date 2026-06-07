package avro_test

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// Generative text-interface precedence net.
//
// Documented policy (BUG_AUDIT "Text interfaces take precedence over the
// reflect.String fast path"): a string-kind Go type implementing a text
// method encodes/decodes through that method, NOT its raw string value —
// uniformly across binary and JSON, scalar and container, addressable
// (unsafe) and not. The combinatorial matrix carries a text type with an
// IDENTITY MarshalText, so "text method used" produces the same bytes as
// "raw string" and the matrix cannot distinguish them: neutering the
// precedence (ser.go's textOutFor-before-reflect.String check) was caught
// ONLY by hand-written pins that use a TRANSFORMING method.
//
// This net uses TRANSFORMING text methods (a "T:" / "A:" marker prefix) so
// the wire is observably different from the raw string, and sweeps the
// precedence across {top, field, array, map-value, struct-field} positions
// and both wires. The discriminators: decoding the wire as a PLAIN string
// must show the MARSHALED form (the text method ran on encode); decoding
// into the text type must round-trip through UnmarshalText.
// ---------------------------------------------------------------------------

// markerMarshal is a string-kind type whose MarshalText TRANSFORMS (prefixes
// "T:"), so its Avro-string wire differs from its raw string value.
type markerMarshal string

func (m markerMarshal) MarshalText() ([]byte, error) { return []byte("T:" + string(m)), nil }
func (m *markerMarshal) UnmarshalText(b []byte) error {
	*m = markerMarshal(strings.TrimPrefix(string(b), "T:"))
	return nil
}

// markerAppend exercises the TextAppender path (textOutFor prefers AppendText
// over MarshalText for the alloc-free inline write — ser.go:986).
type markerAppend string

func (m markerAppend) AppendText(b []byte) ([]byte, error) {
	return append(append(b, "A:"...), m...), nil
}
func (m *markerAppend) UnmarshalText(b []byte) error {
	*m = markerAppend(bytes.TrimPrefix(b, []byte("A:")))
	return nil
}

// textPositions wrap a leaf value into a composed value at a position, with a
// matching "string"-schema composition and a way to (a) build a PLAIN-string
// decode target tree and (b) read the leaf string out of it.
var textPositions = []struct {
	label   string
	schema  string
	wrap    func(leaf any) any
	plainT  func() any // ptr to a tree with string leaves
	leafOf  func(decoded any) string
	typedT  func(t reflect.Type) reflect.Value
	leafTyp func(decoded reflect.Value) reflect.Value
}{
	{"top", `"string"`,
		func(leaf any) any { return leaf },
		func() any { return new(string) },
		func(d any) string { return *(d.(*string)) },
		func(t reflect.Type) reflect.Value { return reflect.New(t) },
		func(d reflect.Value) reflect.Value { return d.Elem() }},
	{"field", `{"type":"record","name":"TIR","fields":[{"name":"f","type":"string"}]}`,
		func(leaf any) any { return map[string]any{"f": leaf} },
		func() any { return &map[string]string{} },
		func(d any) string { return (*(d.(*map[string]string)))["f"] },
		func(t reflect.Type) reflect.Value {
			st := reflect.StructOf([]reflect.StructField{{Name: "F", Type: t, Tag: `avro:"f"`}})
			return reflect.New(st)
		},
		func(d reflect.Value) reflect.Value { return d.Elem().Field(0) }},
	{"array-item", `{"type":"array","items":"string"}`,
		func(leaf any) any { return []any{leaf} },
		func() any { return &[]string{} },
		func(d any) string { return (*(d.(*[]string)))[0] },
		func(t reflect.Type) reflect.Value { return reflect.New(reflect.SliceOf(t)) },
		func(d reflect.Value) reflect.Value { return d.Elem().Index(0) }},
	{"map-value", `{"type":"map","values":"string"}`,
		func(leaf any) any { return map[string]any{"k": leaf} },
		func() any { return &map[string]string{} },
		func(d any) string { return (*(d.(*map[string]string)))["k"] },
		func(t reflect.Type) reflect.Value {
			return reflect.New(reflect.MapOf(reflect.TypeFor[string](), t))
		},
		func(d reflect.Value) reflect.Value {
			return d.Elem().MapIndex(reflect.ValueOf("k"))
		}},
}

func TestMatrix_TextInterfacePrecedence(t *testing.T) {
	cases := []struct {
		label  string
		typ    reflect.Type
		value  func() any // a value of typ holding "hello"
		marked string     // the marshaled wire form of "hello"
		raw    string     // the raw string value
	}{
		{"MarshalText", reflect.TypeFor[markerMarshal](),
			func() any { return markerMarshal("hello") }, "T:hello", "hello"},
		{"AppendText", reflect.TypeFor[markerAppend](),
			func() any { return markerAppend("hello") }, "A:hello", "hello"},
	}

	for _, c := range cases {
		for _, pos := range textPositions {
			t.Run(c.label+"/"+pos.label, func(t *testing.T) {
				s := avro.MustParse(pos.schema)

				// Build the source value at this position. For struct-field,
				// pass a POINTER so the addressable unsafe encode path is hit.
				var src any
				if pos.label == "field" {
					st := reflect.StructOf([]reflect.StructField{{Name: "F", Type: c.typ, Tag: `avro:"f"`}})
					p := reflect.New(st)
					p.Elem().Field(0).Set(reflect.ValueOf(c.value()))
					src = p.Interface()
				} else {
					src = pos.wrap(c.value())
				}

				for _, enc := range []struct {
					name   string
					encode func(any) ([]byte, error)
					decode func([]byte, any) error
				}{
					{"binary",
						func(v any) ([]byte, error) { return s.AppendEncode(nil, v) },
						func(b []byte, tgt any) error { _, err := s.Decode(b, tgt); return err }},
					{"json",
						func(v any) ([]byte, error) { return s.AppendEncodeJSON(nil, v) },
						func(b []byte, tgt any) error { return s.DecodeJSON(b, tgt) }},
				} {
					wire, err := enc.encode(src)
					if err != nil {
						t.Fatalf("%s encode: %v", enc.name, err)
					}
					// Discriminator 1: decode the wire as PLAIN strings. The
					// leaf must be the MARSHALED form — proving the text
					// method ran on encode, not the raw string fast path.
					pt := pos.plainT()
					if err := enc.decode(wire, pt); err != nil {
						t.Fatalf("%s plain decode: %v", enc.name, err)
					}
					if got := pos.leafOf(pt); got != c.marked {
						t.Fatalf("%s: text method BYPASSED on encode — wire leaf %q, want marshaled %q (raw would be %q)",
							enc.name, got, c.marked, c.raw)
					}
					// Discriminator 2: decode into the text TYPE — must
					// round-trip through UnmarshalText back to the raw value.
					tt := pos.typedT(c.typ)
					if err := enc.decode(wire, tt.Interface()); err != nil {
						t.Fatalf("%s typed decode: %v", enc.name, err)
					}
					if got := pos.leafTyp(tt).String(); got != c.raw {
						t.Fatalf("%s: UnmarshalText not applied on decode — got %q, want %q", enc.name, got, c.raw)
					}
				}
			})
		}
	}
}

// The [16]byte uuid "trusts raw bytes" exception: a [16]byte-shaped Go type
// carrying a uuid logical type uses its RAW BYTES, NOT a text method, on both
// wires — the 16 bytes ARE the UUID, and consulting MarshalText would let a
// non-canonical text method diverge binary from JSON. A transforming text
// method on the array type must therefore be IGNORED.
type markerUUID [16]byte

func (m markerUUID) MarshalText() ([]byte, error) { return []byte("IGNORED"), nil }

func TestMatrix_UUIDByteArrayTrustsRawBytes(t *testing.T) {
	s := avro.MustParse(`{"type":"fixed","name":"TUU","size":16,"logicalType":"uuid"}`)
	var v markerUUID
	for i := range v {
		v[i] = byte(i + 1)
	}
	for _, enc := range []struct {
		name   string
		encode func(any) ([]byte, error)
	}{
		{"binary", func(x any) ([]byte, error) { return s.AppendEncode(nil, x) }},
		{"json", func(x any) ([]byte, error) { return s.AppendEncodeJSON(nil, x) }},
	} {
		wire, err := enc.encode(v)
		if err != nil {
			t.Fatalf("%s encode: %v", enc.name, err)
		}
		if bytes.Contains(wire, []byte("IGNORED")) {
			t.Fatalf("%s: [16]byte uuid used MarshalText instead of trusting raw bytes: %x", enc.name, wire)
		}
		// Round-trip: the raw bytes survive.
		var out markerUUID
		if _, err := s.Decode(mustBinUUID(t, s, v), &out); err == nil && out != v {
			t.Fatalf("%s: uuid bytes not preserved: %x vs %x", enc.name, out, v)
		}
	}
}

func mustBinUUID(t *testing.T, s *avro.Schema, v markerUUID) []byte {
	t.Helper()
	b, err := s.AppendEncode(nil, v)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return b
}
