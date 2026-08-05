package avro_test

import (
	"crypto/sha256"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/twmb/avro"
)

// ─────────────────────────────────────────────────────────────────────────
// A nil *Schema is invalid and methods panic on it (programming error,
// idiomatic Go). This pins that the panic is CONSISTENT across the exported
// method set: every method that dereferences the nil receiver panics. The
// three methods that validate an argument BEFORE touching the receiver
// (Decode / DecodeJSON nil target, DecodeSingleObject malformed header) are
// exercised twice — once with a valid argument (must panic, reaching the
// receiver deref) and once with the bad argument (returns the arg-validation
// error, which is correct: the error is about the argument, not the receiver).
// See BUG_AUDIT.md §Known intentional divergences "nil *Schema panics".
// ─────────────────────────────────────────────────────────────────────────

// outcome runs fn and reports whether it panicked or returned an error.
func outcome(fn func() error) (panicked bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
		}
	}()
	err = fn()
	return
}

func mustPanic(t *testing.T, name string, fn func() error) {
	t.Helper()
	panicked, err := outcome(fn)
	if !panicked {
		t.Errorf("%s: expected panic on nil *Schema, got err=%v (no panic)", name, err)
	}
}

func mustErrorNotPanic(t *testing.T, name, wantSubstr string, fn func() error) {
	t.Helper()
	panicked, err := outcome(fn)
	if panicked {
		t.Errorf("%s: expected arg-validation error, got panic", name)
		return
	}
	if err == nil || !strings.Contains(err.Error(), wantSubstr) {
		t.Errorf("%s: expected error containing %q, got %v", name, wantSubstr, err)
	}
}

func TestRegression_NilSchemaPanicsConsistently(t *testing.T) {
	var s *avro.Schema // nil receiver
	validPtr := new(int)
	good := avro.MustParse(`"int"`)

	// Receiver-dereferencing methods: every one panics on a nil receiver
	// when given otherwise-valid arguments.
	mustPanic(t, "AppendEncode", func() error { _, e := s.AppendEncode(nil, 1); return e })
	mustPanic(t, "Encode", func() error { _, e := s.Encode(1); return e })
	mustPanic(t, "EncodeJSON", func() error { _, e := s.EncodeJSON(1); return e })
	mustPanic(t, "AppendEncodeJSON", func() error { _, e := s.AppendEncodeJSON(nil, 1); return e })
	mustPanic(t, "Canonical", func() error { _ = s.Canonical(); return nil })
	mustPanic(t, "Fingerprint", func() error { _ = s.Fingerprint(sha256.New()); return nil })
	mustPanic(t, "String", func() error { _ = s.String(); return nil })
	mustPanic(t, "Root", func() error { _ = s.Root(); return nil })
	mustPanic(t, "AppendSingleObject", func() error { _, e := s.AppendSingleObject(nil, 1); return e })

	// Decode / DecodeJSON / DecodeSingleObject validate the ARGUMENT first.
	// With a VALID argument they reach the receiver deref and panic; with the
	// BAD argument they surface the arg-validation error (correct — about the
	// argument, not the receiver).
	mustPanic(t, "Decode(valid target)", func() error { _, e := s.Decode([]byte{0}, validPtr); return e })
	mustErrorNotPanic(t, "Decode(nil target)", "non-nil pointer", func() error { _, e := s.Decode([]byte{0}, nil); return e })

	mustPanic(t, "DecodeJSON(valid target)", func() error { return s.DecodeJSON([]byte("1"), validPtr) })
	mustErrorNotPanic(t, "DecodeJSON(nil target)", "non-nil pointer", func() error { return s.DecodeJSON([]byte("1"), nil) })

	validHeader := append([]byte{0xC3, 0x01}, make([]byte, 8)...)
	mustPanic(t, "DecodeSingleObject(valid header)", func() error { _, e := s.DecodeSingleObject(validHeader, validPtr); return e })
	mustErrorNotPanic(t, "DecodeSingleObject(short header)", "too short", func() error { _, e := s.DecodeSingleObject([]byte{0x01}, validPtr); return e })

	// Resolve / CheckCompatibility panic when EITHER *Schema argument is nil
	// (each dereferences writer.node / reader.node before any guard runs).
	mustPanic(t, "Resolve(nil writer)", func() error { _, e := avro.Resolve(nil, good); return e })
	mustPanic(t, "Resolve(nil reader)", func() error { _, e := avro.Resolve(good, nil); return e })
	mustPanic(t, "CheckCompatibility(nil writer)", func() error { return avro.CheckCompatibility(nil, good) })
	mustPanic(t, "CheckCompatibility(nil reader)", func() error { return avro.CheckCompatibility(good, nil) })

	// *SchemaNode.Schema panics on a nil *SchemaNode receiver.
	var sn *avro.SchemaNode
	mustPanic(t, "SchemaNode.Schema", func() error { _, e := sn.Schema(); return e })

	// SchemaCache.Parse panics on a nil *SchemaCache receiver.
	var sc *avro.SchemaCache
	mustPanic(t, "SchemaCache.Parse", func() error { _, e := sc.Parse(`"int"`); return e })
}

// ─────────────────────────────────────────────────────────────────────────
// Quoted "size" / "precision" / "scale" at parse, mirroring Apache Avro
// (Java). Java REJECTS all three when quoted: size via Schema.java's
// `!sizeNode.isInt()` (a TextNode is not isInt), and precision/scale via
// LogicalTypes.Decimal.getInt's `obj instanceof Integer` (a quoted value
// deserializes to a Java String through JacksonUtils.toObject's isTextual
// arm, not an Integer). twmb mirrors Java on precision/scale (both reject
// quoted) and is intentionally MORE lenient than Java on size: it accepts a
// quoted size per the Avro spec's [INTEGERS] Parsing-Canonical-Form rule
// ("Eliminate quotes around ... JSON integer literals (which appear in the
// size attributes of fixed schemas)"), via laxInt. This pins the exact
// accept/reject at BOTH parse and Root() metadata-read. See BUG_AUDIT.md
// §Known intentional divergences "Quoted size/precision/scale at parse".
// ─────────────────────────────────────────────────────────────────────────

func TestRegression_QuotedSizePrecisionScaleMirrorsJava(t *testing.T) {
	accept := func(name, schema string) {
		t.Helper()
		if _, err := avro.Parse(schema); err != nil {
			t.Errorf("%s: expected ACCEPT, got reject: %v", name, err)
		}
	}
	reject := func(name, schema string) {
		t.Helper()
		if _, err := avro.Parse(schema); err == nil {
			t.Errorf("%s: expected REJECT, got accept", name)
		}
	}

	// size: numeric accepted; quoted accepted (twmb is more lenient than
	// Java here, per spec [INTEGERS]).
	accept("size numeric", `{"type":"fixed","name":"F","size":16}`)
	accept("size quoted", `{"type":"fixed","name":"F","size":"16"}`)
	accept("size quoted leading-zero", `{"type":"fixed","name":"F","size":"016"}`)

	// precision/scale: numeric accepted; quoted REJECTED (mirrors Java).
	accept("decimal numeric prec/scale", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)
	reject("decimal quoted precision", `{"type":"bytes","logicalType":"decimal","precision":"10","scale":2}`)
	reject("decimal quoted scale", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":"2"}`)
	reject("decimal both quoted", `{"type":"bytes","logicalType":"decimal","precision":"10","scale":"2"}`)
	reject("fixed-decimal quoted precision", `{"type":"fixed","name":"D","size":8,"logicalType":"decimal","precision":"10","scale":2}`)

	// Root() metadata-read agrees with parse for the accepted shapes: a
	// quoted size reads back as the numeric Size, and a numeric decimal
	// surfaces Precision/Scale. (Quoted precision/scale never reach Root()
	// because parse rejects them first — there is no parsed schema to read.)
	sQuotedSize := avro.MustParse(`{"type":"fixed","name":"F","size":"16"}`)
	if got := sQuotedSize.Root().Size; got != 16 {
		t.Errorf("Root().Size for quoted size: got %d, want 16", got)
	}
	sDecimal := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)
	if r := sDecimal.Root(); r.Precision != 10 || r.Scale != 2 {
		t.Errorf("Root() decimal: got precision=%d scale=%d, want 10/2", r.Precision, r.Scale)
	}
}

// ─────────────────────────────────────────────────────────────────────────
// Under TagLogicalTypes, a NAMED fixed carrying a logical type tags its
// tagged-union branch under the fixed's (fully-qualified) NAME — NOT
// "fixed.<logicalType>". This matches both linkedin/goavro (the envelope key
// is the branch codec's typeName.fullName; a named fixed's codec keeps the
// fixed's name) and Apache Avro's JsonEncoder (uses the branch's
// getFullName()). The "<kind>.<logicalType>" qualifier is retained ONLY for
// primitive-backed logicals (e.g. long.timestamp-millis), which is goavro's
// convention and the reason TagLogicalTypes exists. The encoding stays
// binary↔JSON uniform, round-trips, and the decoder still ACCEPTS the legacy
// "fixed.<logicalType>" form for backward compatibility. See BUG_AUDIT.md
// §Known intentional divergences "Named-fixed logical tagged-union name".
// ─────────────────────────────────────────────────────────────────────────

func TestRegression_NamedFixedLogicalTaggedUnionName(t *testing.T) {
	// keyOf returns the single key of a tagged JSON union envelope
	// {"key":...} produced by EncodeJSON.
	keyOf := func(t *testing.T, b []byte) string {
		t.Helper()
		s := string(b)
		if len(s) < 4 || s[0] != '{' || s[1] != '"' {
			t.Fatalf("not a tagged envelope: %s", s)
		}
		end := strings.IndexByte(s[2:], '"')
		if end < 0 {
			t.Fatalf("malformed envelope: %s", s)
		}
		return s[2 : 2+end]
	}

	uuidVal := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	decVal := big.NewRat(123, 100)
	durVal := avro.Duration{Months: 1, Days: 2, Milliseconds: 3}
	tsVal := time.Unix(0, 0).UTC()

	cases := []struct {
		name       string
		schema     string
		input      any
		tagLogical bool
		wantKey    string // exact tagged key under the given options
	}{
		// Named fixed + logical: name wins regardless of TagLogicalTypes.
		{"fixed-uuid TaggedUnions", `["null",{"type":"fixed","name":"F","size":16,"logicalType":"uuid"}]`, uuidVal, false, "F"},
		{"fixed-uuid +TagLogical", `["null",{"type":"fixed","name":"F","size":16,"logicalType":"uuid"}]`, uuidVal, true, "F"},
		{"fixed-decimal TaggedUnions", `["null",{"type":"fixed","name":"D","size":8,"logicalType":"decimal","precision":10,"scale":2}]`, decVal, false, "D"},
		{"fixed-decimal +TagLogical", `["null",{"type":"fixed","name":"D","size":8,"logicalType":"decimal","precision":10,"scale":2}]`, decVal, true, "D"},
		{"fixed-duration TaggedUnions", `["null",{"type":"fixed","name":"Dur","size":12,"logicalType":"duration"}]`, durVal, false, "Dur"},
		{"fixed-duration +TagLogical", `["null",{"type":"fixed","name":"Dur","size":12,"logicalType":"duration"}]`, durVal, true, "Dur"},
		// Unnamed primitive-backed logical: keeps the <kind>.<logical> form
		// only under TagLogicalTypes, else the bare kind.
		{"long-timestamp TaggedUnions", `["null",{"type":"long","logicalType":"timestamp-millis"}]`, tsVal, false, "long"},
		{"long-timestamp +TagLogical", `["null",{"type":"long","logicalType":"timestamp-millis"}]`, tsVal, true, "long.timestamp-millis"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := avro.MustParse(c.schema)
			opts := []avro.Opt{avro.TaggedUnions()}
			if c.tagLogical {
				opts = append(opts, avro.TagLogicalTypes())
			}

			// JSON encode emits the expected key.
			jb, err := s.EncodeJSON(c.input, opts...)
			if err != nil {
				t.Fatalf("EncodeJSON: %v", err)
			}
			if got := keyOf(t, jb); got != c.wantKey {
				t.Errorf("EncodeJSON tagged key: got %q, want %q (%s)", got, c.wantKey, jb)
			}

			// Binary decode into *any wraps under the SAME key — binary↔JSON
			// uniformity for the tagged-union name.
			wire, err := s.Encode(c.input)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			var decoded any
			if _, err := s.Decode(wire, &decoded, opts...); err != nil {
				t.Fatalf("Decode: %v", err)
			}
			m, ok := decoded.(map[string]any)
			if !ok {
				t.Fatalf("decoded not a tagged map: %#v", decoded)
			}
			if _, ok := m[c.wantKey]; !ok {
				t.Errorf("binary decode wrap key: got %v, want %q", mapKeys(m), c.wantKey)
			}

			// JSON round-trip through the emitted form.
			var jround any
			if err := s.DecodeJSON(jb, &jround, opts...); err != nil {
				t.Errorf("DecodeJSON round-trip of emitted form failed: %v", err)
			}
		})
	}

	// Backward compatibility: the decoder still ACCEPTS the legacy
	// "fixed.<logicalType>" tagged key even though it is no longer emitted.
	t.Run("legacy fixed.uuid still decodes", func(t *testing.T) {
		s := avro.MustParse(`["null",{"type":"fixed","name":"F","size":16,"logicalType":"uuid"}]`)
		// Obtain a valid 16-byte codepoint-string body from the encoder.
		jb, _ := s.EncodeJSON([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, avro.TaggedUnions())
		body := string(jb[strings.IndexByte(string(jb), ':')+1 : len(jb)-1])
		legacy := `{"fixed.uuid":` + body + `}`
		var out any
		if err := s.DecodeJSON([]byte(legacy), &out, avro.TaggedUnions()); err != nil {
			t.Errorf("legacy fixed.uuid tagged JSON must still decode: %v", err)
		}
	})
}

func mapKeys(m map[string]any) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}
