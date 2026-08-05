package avro

import (
	"bytes"
	"strings"
	"testing"
)

// Degenerate-cardinality types: zero-size fixed, zero-symbol enums, and
// zero-branch unions. The Avro spec sets no minimum for any of the three
// ("size: an integer", "symbols: a JSON array", a union is "a JSON array");
// Java, fastavro, and avro-rs all parse all three (Java:
// SystemLimitException.checkMaxBytesLength rejects negative sizes — and
// caps above Integer.MAX_VALUE-8 — so 0 passes, EnumSchema's constructor
// does per-symbol checks only, UnionSchema's constructor loop no-ops on
// empty). A size-0 fixed is a usable type whose
// every value is the empty byte string; empty enums and unions are
// unusable-but-parseable (every encode/decode of the node itself errors),
// which matters for schema passthrough: a reader must be able to parse a
// foreign schema that carries a degenerate type in a position the data
// never exercises.

func TestRegression_FixedSizeZeroParses(t *testing.T) {
	for _, schema := range []string{
		`{"type":"fixed","name":"F","size":0}`,
		`{"type":"fixed","name":"F","size":"0"}`, // quoted-integer [INTEGERS] form
	} {
		if _, err := Parse(schema); err != nil {
			t.Errorf("Parse(%s) rejected size-0 fixed (Java/fastavro/avro-rs accept): %v", schema, err)
		}
	}
	// Negative sizes stay rejected on every form (Java parity).
	for _, schema := range []string{
		`{"type":"fixed","name":"F","size":-1}`,
		`{"type":"fixed","name":"F","size":"-1"}`,
	} {
		if _, err := Parse(schema); err == nil {
			t.Errorf("Parse(%s) accepted negative fixed size", schema)
		}
	}
}

func TestRegression_FixedSizeZeroWire(t *testing.T) {
	s, err := Parse(`{"type":"fixed","name":"F","size":0}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Every value is the empty byte string: 0 wire bytes.
	enc, err := s.AppendEncode(nil, []byte{})
	if err != nil {
		t.Fatalf("encode []byte{}: %v", err)
	}
	if len(enc) != 0 {
		t.Fatalf("size-0 fixed encoded to %d bytes, want 0", len(enc))
	}
	if _, err := s.AppendEncode(nil, [0]byte{}); err != nil {
		t.Errorf("encode [0]byte{}: %v", err)
	}
	if _, err := s.AppendEncode(nil, ""); err != nil {
		t.Errorf("encode \"\": %v", err)
	}
	// Wrong-length values reject, exactly like any other fixed.
	if _, err := s.AppendEncode(nil, []byte{1}); err == nil {
		t.Error("encode 1-byte value against size-0 fixed should error")
	}
	if _, err := s.AppendEncode(nil, "x"); err == nil {
		t.Error("encode 1-char string against size-0 fixed should error")
	}

	// Decode into every fixed-compatible target.
	var bs []byte
	if _, err := s.Decode(enc, &bs); err != nil {
		t.Errorf("decode []byte: %v", err)
	} else if len(bs) != 0 {
		t.Errorf("decode []byte: got %v, want empty", bs)
	}
	var arr [0]byte
	if _, err := s.Decode(enc, &arr); err != nil {
		t.Errorf("decode [0]byte: %v", err)
	}
	var str string
	if _, err := s.Decode(enc, &str); err != nil {
		t.Errorf("decode string: %v", err)
	} else if str != "" {
		t.Errorf("decode string: got %q, want empty", str)
	}
	var a any
	if _, err := s.Decode(enc, &a); err != nil {
		t.Errorf("decode any: %v", err)
	}

	// JSON wire form is the empty codepoint string.
	j, err := s.AppendEncodeJSON(nil, []byte{})
	if err != nil {
		t.Fatalf("encodeJSON: %v", err)
	}
	if string(j) != `""` {
		t.Errorf("encodeJSON: got %s, want \"\"", j)
	}
	var jb []byte
	if err := s.DecodeJSON([]byte(`""`), &jb); err != nil {
		t.Errorf("decodeJSON: %v", err)
	} else if len(jb) != 0 {
		t.Errorf("decodeJSON: got %v, want empty", jb)
	}

	// Canonical form keeps size 0; the schema fingerprints stably.
	if got := string(s.Canonical()); got != `{"name":"F","type":"fixed","size":0}` {
		t.Errorf("canonical: got %s", got)
	}

	// Metadata surfaces the zero size.
	if root := s.Root(); root.Size != 0 || root.Type != "fixed" {
		t.Errorf("Root(): Type=%q Size=%d", root.Type, root.Size)
	}

	// Metadata REBUILD: size is a required fixed attribute, so
	// Root().Schema() must re-emit "size":0 (not omit it as a zero
	// value) — at top level and nested in a union.
	for _, schema := range []string{
		`{"type":"fixed","name":"F","size":0}`,
		`["null",{"type":"fixed","name":"F","size":0}]`,
	} {
		ss := MustParse(schema)
		root := ss.Root()
		rebuilt, err := root.Schema()
		if err != nil {
			t.Errorf("Root().Schema() for %s: %v", schema, err)
			continue
		}
		if !bytes.Equal(ss.Fingerprint(NewRabin()), rebuilt.Fingerprint(NewRabin())) {
			t.Errorf("rebuild fingerprint mismatch for %s: rebuilt %s", schema, rebuilt.String())
		}
	}

	// A "" default on a size-0 fixed field validates (length 0 == size 0)
	// and fills on JSON decode.
	rs, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"f","type":{"type":"fixed","name":"F0","size":0},"default":""}]}`)
	if err != nil {
		t.Fatalf("parse record with size-0 fixed default: %v", err)
	}
	var out map[string]any
	if err := rs.DecodeJSON([]byte(`{}`), &out); err != nil {
		t.Fatalf("default fill: %v", err)
	}
}

func TestRegression_FixedSizeZeroArrayBounded(t *testing.T) {
	s, err := Parse(`{"type":"array","items":{"type":"fixed","name":"F","size":0}}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// A legitimate small array of zero-byte items round-trips.
	enc, err := s.AppendEncode(nil, [][]byte{{}, {}, {}})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got [][]byte
	if _, err := s.Decode(enc, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d items, want 3", len(got))
	}

	// A hostile block count of zero-byte items hits the absolute
	// maxZeroByteItems cap instead of looping count times.
	hostile := appendVarlong(nil, 1<<40) // block count
	hostile = append(hostile, 0x00)      // terminator (never reached)
	var sink any
	if _, err := s.Decode(hostile, &sink); err == nil {
		t.Fatal("hostile zero-byte-item count must be rejected")
	} else if !strings.Contains(err.Error(), "zero-byte items") {
		t.Fatalf("expected zero-byte cap error, got: %v", err)
	}
}

func TestRegression_SchemaForZeroLenByteArrayField(t *testing.T) {
	type R struct {
		A [0]byte `avro:"a"`
		B int32   `avro:"b"`
	}
	s, err := SchemaFor[R]()
	if err != nil {
		t.Fatalf("SchemaFor rejected a valid Go type with a [0]byte field: %v", err)
	}
	enc, err := s.AppendEncode(nil, R{B: 7})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out R
	if _, err := s.Decode(enc, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.B != 7 {
		t.Errorf("round-trip: got %+v", out)
	}
}

func TestRegression_EmptyEnumParses(t *testing.T) {
	s, err := Parse(`{"type":"enum","name":"E","symbols":[]}`)
	if err != nil {
		t.Fatalf("Parse rejected empty enum (Java/fastavro/avro-rs accept): %v", err)
	}
	if root := s.Root(); root.Type != "enum" || len(root.Symbols) != 0 {
		t.Errorf("Root(): Type=%q Symbols=%v", root.Type, root.Symbols)
	}
	if got := string(s.Canonical()); got != `{"name":"E","type":"enum","symbols":[]}` {
		t.Errorf("canonical: got %s", got)
	}

	// No valid values exist: every encode/decode errors, never panics.
	if _, err := s.AppendEncode(nil, "A"); err == nil {
		t.Error("encode symbol against empty enum should error")
	}
	if _, err := s.AppendEncode(nil, 0); err == nil {
		t.Error("encode ordinal against empty enum should error")
	}
	var str string
	if _, err := s.Decode([]byte{0x00}, &str); err == nil {
		t.Error("decode ordinal 0 against empty enum should error")
	}
	if _, err := s.AppendEncodeJSON(nil, "A"); err == nil {
		t.Error("encodeJSON against empty enum should error")
	}
	if err := s.DecodeJSON([]byte(`"A"`), &str); err == nil {
		t.Error("decodeJSON against empty enum should error")
	}

	// An enum-typed default on an empty enum rejects: no symbol is a
	// member (Java: EnumSchema constructor / isValidDefault containment).
	if _, err := Parse(`{"type":"enum","name":"E","symbols":[],"default":"A"}`); err == nil {
		t.Error("enum-level default on empty enum should reject")
	}
	if _, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"e","type":{"type":"enum","name":"E","symbols":[]},"default":"A"}]}`); err == nil {
		t.Error("field default on empty enum should reject")
	}

	// Union-default branch selection skips the empty-enum branch (Java's
	// isValidDefault anyMatch: no symbol matches, the string branch does).
	us, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"u","type":[{"type":"enum","name":"E","symbols":[]},"string"],"default":"A"}]}`)
	if err != nil {
		t.Fatalf("union default should pick the string branch: %v", err)
	}
	enc, err := us.AppendEncode(nil, map[string]any{})
	if err != nil {
		t.Fatalf("encode default: %v", err)
	}
	// Branch index 1 (string), zig-zag varint 0x02, then length-1 "A".
	if want := []byte{0x02, 0x02, 'A'}; !bytes.Equal(enc, want) {
		t.Errorf("default wire: got %x, want %x", enc, want)
	}

	// The metadata surface must pick the same branch as the wire. The
	// [empty-enum, bytes] pair discriminates: both branches' defaults are
	// JSON strings, but the bytes branch materializes []byte while a
	// vacuously-accepting empty-enum branch would surface string — so a
	// metadata-side branch-selection drift is visible in the Go type.
	bs, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"u","type":[{"type":"enum","name":"E","symbols":[]},"bytes"],"default":"Z"}]}`)
	if err != nil {
		t.Fatalf("union [empty-enum, bytes] default should pick bytes: %v", err)
	}
	benc, err := bs.AppendEncode(nil, map[string]any{})
	if err != nil {
		t.Fatalf("encode bytes default: %v", err)
	}
	// Branch index 1 (bytes), then length-1 0x5A.
	if want := []byte{0x02, 0x02, 'Z'}; !bytes.Equal(benc, want) {
		t.Errorf("bytes default wire: got %x, want %x", benc, want)
	}
	if d, ok := bs.Root().Fields[0].Default.([]byte); !ok || !bytes.Equal(d, []byte("Z")) {
		t.Errorf("metadata Default = %T %v, want []byte Z (same branch as wire)",
			bs.Root().Fields[0].Default, bs.Root().Fields[0].Default)
	}

	// In a null union the empty enum parses and nil round-trips.
	ns, err := Parse(`["null",{"type":"enum","name":"E","symbols":[]}]`)
	if err != nil {
		t.Fatalf("null union with empty enum: %v", err)
	}
	nenc, err := ns.AppendEncode(nil, nil)
	if err != nil {
		t.Fatalf("encode nil: %v", err)
	}
	var av any = "sentinel"
	if _, err := ns.Decode(nenc, &av); err != nil || av != nil {
		t.Errorf("nil round-trip: v=%v err=%v", av, err)
	}
}

func TestRegression_EmptyEnumResolve(t *testing.T) {
	full := MustParse(`{"type":"enum","name":"E","symbols":["A"]}`)
	empty := MustParse(`{"type":"enum","name":"E","symbols":[]}`)

	// Writer has symbols the empty reader can never map, and an empty
	// enum cannot declare a default: eager-fail at Resolve.
	if _, err := Resolve(full, empty); err == nil {
		t.Error("Resolve(full→empty) should fail: unmappable symbols, no default possible")
	}
	// Writer empty → reader full: no wire symbol can ever arrive;
	// vacuously compatible.
	if _, err := Resolve(empty, full); err != nil {
		t.Errorf("Resolve(empty→full) should be vacuously compatible: %v", err)
	}
	if _, err := Resolve(empty, empty); err != nil {
		t.Errorf("Resolve(empty→empty): %v", err)
	}
}

func TestRegression_EmptyUnionParses(t *testing.T) {
	s, err := Parse(`[]`)
	if err != nil {
		t.Fatalf("Parse rejected empty union (Java/fastavro/avro-rs accept): %v", err)
	}
	root := s.Root()
	if root.Type != "union" || len(root.Branches) != 0 {
		t.Errorf("Root(): Type=%q Branches=%d", root.Type, len(root.Branches))
	}
	if got := string(s.Canonical()); got != `[]` {
		t.Errorf("canonical: got %s, want []", got)
	}
	// SchemaNode.Schema() re-emits a parseable empty union.
	rt, err := root.Schema()
	if err != nil {
		t.Fatalf("Root().Schema(): %v", err)
	}
	if got := string(rt.Canonical()); got != `[]` {
		t.Errorf("round-trip canonical: got %s", got)
	}

	// No value can encode or decode; every path errors, never panics.
	for _, v := range []any{nil, int32(1), "x", []byte{1}, map[string]any{"int": 1}} {
		if _, err := s.AppendEncode(nil, v); err == nil {
			t.Errorf("encode %#v against empty union should error", v)
		}
		if _, err := s.AppendEncodeJSON(nil, v); err == nil {
			t.Errorf("encodeJSON %#v against empty union should error", v)
		}
	}
	var a any
	if _, err := s.Decode([]byte{0x00}, &a); err == nil {
		t.Error("decode index 0 against empty union should error")
	}
	for _, j := range []string{`null`, `1`, `"x"`, `{"int":1}`} {
		if err := s.DecodeJSON([]byte(j), &a); err == nil {
			t.Errorf("decodeJSON %s against empty union should error", j)
		}
	}

	// A union may not immediately contain another union — including an
	// empty one (Java: "Nested union"). Must error, not panic.
	if _, err := Parse(`[["int","null"]]`); err == nil {
		t.Error("union containing a union must reject")
	}
	if _, err := Parse(`[[]]`); err == nil {
		t.Error("union containing an empty union must reject")
	}

	// No default can match a zero-branch union; absent default is fine.
	if _, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"u","type":[],"default":null}]}`); err == nil {
		t.Error("default on empty-union field should reject (no branch accepts)")
	}
	rs, err := Parse(`{"type":"record","name":"R","fields":[
		{"name":"u","type":[]}]}`)
	if err != nil {
		t.Fatalf("record with empty-union field should parse: %v", err)
	}
	if _, err := rs.AppendEncode(nil, map[string]any{"u": 1}); err == nil {
		t.Error("encoding a record with an empty-union field should error")
	}
}

func TestRegression_EmptyUnionContainers(t *testing.T) {
	as, err := Parse(`{"type":"array","items":[]}`)
	if err != nil {
		t.Fatalf("array of empty union: %v", err)
	}
	// The empty array is the only inhabitable value.
	enc, err := as.AppendEncode(nil, []any{})
	if err != nil {
		t.Fatalf("encode empty array: %v", err)
	}
	var got []any
	if _, err := as.Decode(enc, &got); err != nil {
		t.Fatalf("decode empty array: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %v", got)
	}
	if _, err := as.AppendEncode(nil, []any{int32(1)}); err == nil {
		t.Error("non-empty array of empty union should error")
	}
	// Wire claiming items must error (first item has no valid branch),
	// without panicking or spinning.
	hostile := appendVarlong(nil, 3)
	hostile = append(hostile, 0x00, 0x00, 0x00, 0x00)
	var sink any
	if _, err := as.Decode(hostile, &sink); err == nil {
		t.Error("array wire with empty-union items must error")
	}

	ms, err := Parse(`{"type":"map","values":[]}`)
	if err != nil {
		t.Fatalf("map of empty union: %v", err)
	}
	menc, err := ms.AppendEncode(nil, map[string]any{})
	if err != nil {
		t.Fatalf("encode empty map: %v", err)
	}
	var mgot map[string]any
	if _, err := ms.Decode(menc, &mgot); err != nil {
		t.Fatalf("decode empty map: %v", err)
	}
	// A wire block claiming an entry must error cleanly.
	mhostile := appendVarlong(nil, 1)
	mhostile = append(mhostile, 0x02, 'k', 0x00, 0x00)
	if _, err := ms.Decode(mhostile, &sink); err == nil {
		t.Error("map wire with empty-union values must error")
	}
}

func TestRegression_EmptyUnionResolve(t *testing.T) {
	empty := MustParse(`[]`)
	intS := MustParse(`"int"`)

	// Writer empty union: no branch can ever appear on the wire, so any
	// reader is vacuously compatible (Java's WriterUnion builds per-branch
	// actions over zero branches and can never error at decode).
	if _, err := Resolve(empty, intS); err != nil {
		t.Errorf("Resolve(empty union → int) should be vacuously compatible: %v", err)
	}
	if err := CheckCompatibility(empty, intS); err != nil {
		t.Errorf("CheckCompatibility(empty union → int): %v", err)
	}
	// Reader empty union: no branch can accept the writer's values.
	if _, err := Resolve(intS, empty); err == nil {
		t.Error("Resolve(int → empty union) should fail: no reader branch matches")
	}
	if err := CheckCompatibility(intS, empty); err == nil {
		t.Error("CheckCompatibility(int → empty union) should fail")
	}
	// Empty ↔ empty: vacuous.
	if _, err := Resolve(empty, empty); err != nil {
		t.Errorf("Resolve(empty → empty): %v", err)
	}
}
