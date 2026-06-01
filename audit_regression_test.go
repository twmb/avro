package avro_test

import (
	"crypto/sha256"
	"math/big"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

// A decimal logical type on a non-bytes/fixed primitive is malformed; the
// parser soft-drops the logical (matching Java's lenient parser). When a
// decimal CustomType is registered, the logical is resurrected so the
// custom type can handle it — but the resurrected logical must not enter
// the built-in decimal code path, which assumes a bytes/fixed underlying
// with a validated precision and would dereference a nil precision pointer.
// A resurrected decimal on the wrong underlying type instead routes the
// raw value through the custom decoder.
func TestRegression_DecimalCustomTypeWrongUnderlyingNoPanic(t *testing.T) {
	ct := func() avro.CustomType {
		return avro.CustomType{
			LogicalType: "decimal",
			Decode:      func(v any, _ *avro.SchemaNode) (any, error) { return v, nil },
		}
	}

	// The exact malformed shape: decimal on int, with no precision. Must
	// parse without panicking (parser must never crash on valid JSON).
	for _, typ := range []string{"int", "long", "string"} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Parse panicked for %q+decimal: %v", typ, r)
				}
			}()
			if _, err := avro.Parse(`{"type":"`+typ+`","logicalType":"decimal"}`, avro.WithCustomType(ct())); err != nil {
				t.Fatalf("%q+decimal+CustomType should parse: %v", typ, err)
			}
		}()
	}

	// The resurrected logical routes the raw Avro-native value through the
	// custom decoder (the wire stays a plain int).
	s, err := avro.Parse(`{"type":"int","logicalType":"decimal"}`, avro.WithCustomType(ct()))
	if err != nil {
		t.Fatal(err)
	}
	wire, err := s.Encode(int32(42))
	if err != nil {
		t.Fatalf("encode int+decimal: %v", err)
	}
	var got any
	if _, err := s.Decode(wire, &got); err != nil {
		t.Fatalf("decode int+decimal: %v", err)
	}
	if got != int32(42) {
		t.Fatalf("int+decimal custom decode = %T %v, want int32(42)", got, got)
	}

	// A genuine bytes+decimal with a CustomType still parses and round-trips.
	sb, err := avro.Parse(`{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`, avro.WithCustomType(ct()))
	if err != nil {
		t.Fatalf("valid bytes+decimal+CustomType should parse: %v", err)
	}
	if _, err := sb.Encode(big.NewRat(314, 100)); err != nil {
		t.Fatalf("encode bytes+decimal: %v", err)
	}
}

type auditMoney int64

// A custom decoder result (or the ErrSkipCustomType all-skip fall-through,
// which yields the raw Avro-native value) assigned into a CONCRETE
// domain-typed target must return a SemanticError, not panic in
// reflect.Set — and DecodeJSON must agree with binary Decode. The JSON
// path previously assigned via a helper that only checked assignability
// for interface targets.
func TestRegression_DecodeJSONCustomDecoderConcreteTargetErrors(t *testing.T) {
	ct := avro.CustomType{
		LogicalType: "money",
		AvroType:    "long",
		Decode:      func(v any, _ *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType },
	}
	s, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"p","type":{"type":"long","logicalType":"money"}}]}`, avro.WithCustomType(ct))
	if err != nil {
		t.Fatal(err)
	}
	type R struct {
		P auditMoney `avro:"p"`
	}
	wire, err := s.Encode(R{P: 5})
	if err != nil {
		t.Fatal(err)
	}
	jsonBytes, err := s.EncodeJSON(R{P: 5})
	if err != nil {
		t.Fatal(err)
	}

	var rbin R
	_, binErr := s.Decode(wire, &rbin)
	if binErr == nil {
		t.Fatal("binary Decode should reject int64 into a non-assignable concrete target")
	}

	var rjson R
	jsonErr := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("DecodeJSON panicked where binary Decode returned %v: %v", binErr, r)
			}
		}()
		return s.DecodeJSON(jsonBytes, &rjson)
	}()
	if jsonErr == nil {
		t.Fatal("DecodeJSON should reject like binary Decode, not silently succeed")
	}
}

// A forward reference (a name used before its definition) to a type that
// inherits an enclosing namespace must resolve, the same way the
// byte-identical definition-first ordering does. The fix shares the
// namespace-qualified retry between build-time backward-ref resolution and
// finalize-time forward-ref resolution. Exercised at the three positions a
// forward ref can appear (record field, array items, union branch).
func TestRegression_ForwardRefNamespaceResolves(t *testing.T) {
	cases := map[string]string{
		"record field": `{"type":"record","name":"Outer","namespace":"com.x","fields":[
			{"name":"a","type":"Inner"},
			{"name":"b","type":{"type":"record","name":"Inner","fields":[{"name":"v","type":"int"}]}}]}`,
		"array items": `{"type":"record","name":"Outer","namespace":"com.x","fields":[
			{"name":"a","type":{"type":"array","items":"Inner"}},
			{"name":"b","type":{"type":"record","name":"Inner","fields":[{"name":"v","type":"int"}]}}]}`,
		"union branch": `{"type":"record","name":"Outer","namespace":"com.x","fields":[
			{"name":"a","type":["null","Inner"]},
			{"name":"b","type":{"type":"record","name":"Inner","fields":[{"name":"v","type":"int"}]}}]}`,
	}
	for name, schema := range cases {
		if _, err := avro.Parse(schema); err != nil {
			t.Errorf("%s forward-ref to namespaced Inner failed: %v", name, err)
		}
	}
	// Definition-first control (worked before too — pins the symmetry).
	back := `{"type":"record","name":"Outer","namespace":"com.x","fields":[
		{"name":"b","type":{"type":"record","name":"Inner","fields":[{"name":"v","type":"int"}]}},
		{"name":"a","type":"Inner"}]}`
	if _, err := avro.Parse(back); err != nil {
		t.Fatalf("definition-first namespaced ref should parse: %v", err)
	}
	// A genuinely unknown forward ref must still fail.
	if _, err := avro.Parse(`{"type":"record","name":"Outer","namespace":"com.x","fields":[{"name":"a","type":"Nope"}]}`); err == nil {
		t.Fatal("unknown forward ref should be rejected")
	}
}

// SchemaCache dedup is keyed on the schema string only. WithLaxNames
// changes what that string compiles to (a name strict parse rejects
// becomes accepted), so a lax parse must not populate the dedup cache —
// otherwise a later strict cache parse of the same string returns the
// cached lax schema and silently accepts an invalid name that bare Parse
// rejects.
func TestRegression_SchemaCacheLaxNamesNotDeduped(t *testing.T) {
	schema := `{"type":"record","name":"123Bad","fields":[{"name":"x","type":"int"}]}`
	if _, err := avro.Parse(schema); err == nil {
		t.Fatal("bare Parse should reject the invalid record name")
	}
	var c avro.SchemaCache
	if _, err := c.Parse(schema, avro.WithLaxNames(func(string) error { return nil })); err != nil {
		t.Fatalf("lax cache parse should succeed: %v", err)
	}
	if _, err := c.Parse(schema); err == nil {
		t.Fatal("strict SchemaCache.Parse must not return the cached lax schema for an invalid name")
	}

	// Trailing content past the first JSON value must be rejected, matching
	// bare Parse — the dedup normalizer must not silently truncate it.
	var c2 avro.SchemaCache
	if _, err := c2.Parse(`{"type":"int"} trailing`); err == nil {
		t.Fatal("SchemaCache.Parse must reject trailing content like bare Parse")
	}
}

// Parsing Canonical Form emits each named type's full body at its FIRST
// occurrence in the field walk (Apache Avro's SchemaNormalization rule),
// not at the textual definition site. For a forward reference these differ:
// the full body belongs at the reference, with a bare (full)name at the
// later definition. The fingerprint depends on this, so a mismatch breaks
// single-object-encoding / schema-registry interop with Java.
func TestRegression_CanonicalForwardRefFirstOccurrence(t *testing.T) {
	s, err := avro.Parse(`{"type":"record","name":"outer","fields":[
		{"name":"ref","type":{"type":"inner"}},
		{"name":"def","type":{"type":"record","name":"inner","fields":[{"name":"x","type":"int"}]}}]}`)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"name":"outer","type":"record","fields":[` +
		`{"name":"ref","type":{"name":"inner","type":"record","fields":[{"name":"x","type":"int"}]}},` +
		`{"name":"def","type":"inner"}]}`
	if got := string(s.Canonical()); got != want {
		t.Fatalf("Canonical mismatch:\n got %s\nwant %s", got, want)
	}

	// Definition-first ordering of the same field names: full body stays at
	// the definition (which is also the first occurrence) — byte-identical
	// to the un-transformed behavior, so existing schemas are unaffected.
	s2 := avro.MustParse(`{"type":"record","name":"outer","fields":[
		{"name":"def","type":{"type":"record","name":"inner","fields":[{"name":"x","type":"int"}]}},
		{"name":"ref","type":"inner"}]}`)
	const wantBack = `{"name":"outer","type":"record","fields":[` +
		`{"name":"def","type":{"name":"inner","type":"record","fields":[{"name":"x","type":"int"}]}},` +
		`{"name":"ref","type":"inner"}]}`
	if got := string(s2.Canonical()); got != wantBack {
		t.Fatalf("definition-first Canonical mismatch:\n got %s\nwant %s", got, wantBack)
	}

	// A namespaced forward ref normalizes the bare reference to the resolved
	// fullname and emits the full body at the first occurrence.
	s3 := avro.MustParse(`{"type":"record","name":"Outer","namespace":"com.x","fields":[
		{"name":"a","type":"Inner"},
		{"name":"b","type":{"type":"record","name":"Inner","fields":[{"name":"v","type":"int"}]}}]}`)
	canon := string(s3.Canonical())
	if !strings.Contains(canon, `{"name":"a","type":{"name":"com.x.Inner","type":"record"`) ||
		!strings.Contains(canon, `{"name":"b","type":"com.x.Inner"}`) {
		t.Fatalf("namespaced forward-ref canonical not first-occurrence/fullname: %s", canon)
	}

	// The fingerprint is a pure function of the canonical bytes.
	if h := s.Fingerprint(sha256.New()); len(h) != 32 {
		t.Fatalf("unexpected fingerprint length %d", len(h))
	}
}

// skipMap now bounds its block count against the remaining buffer like
// deserMap and skipArray (the unbounded int(count) loop truncated a count
// above 2^31 on a 32-bit build — the platform-dependent narrow-before-check
// class — mis-framing the skip). The 32-bit truncation isn't observable on
// a 64-bit host; this pins that the new bound does not break the valid
// map-skip path (a reader that drops a writer's map field skips it via
// skipMap during resolved decode).
func TestRegression_SkipMapBoundedValidSkip(t *testing.T) {
	writer := avro.MustParse(`{"type":"record","name":"W","fields":[
		{"name":"m","type":{"type":"map","values":"long"}},
		{"name":"keep","type":"int"}]}`)
	reader := avro.MustParse(`{"type":"record","name":"W","fields":[{"name":"keep","type":"int"}]}`)
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := writer.Encode(map[string]any{
		"m":    map[string]int64{"a": 1, "b": 2, "c": 3},
		"keep": int32(7),
	})
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if _, err := resolved.Decode(wire, &out); err != nil {
		t.Fatalf("resolved decode (skipping map field) failed: %v", err)
	}
	if out["keep"] != int32(7) {
		t.Fatalf("keep = %T %v after skipping map field, want int32(7)", out["keep"], out["keep"])
	}
	if _, present := out["m"]; present {
		t.Fatalf("dropped map field should not appear in reader output: %v", out)
	}
}
