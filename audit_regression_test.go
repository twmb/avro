package avro_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/avro"
	"github.com/twmb/avro/internal/avrotest"
)

// ---------- audit_regression_test.go ----------

// A decimal logical on a non-bytes/fixed primitive is malformed, so we
// soft-drop it. A registered decimal CustomType resurrects the logical so the
// custom can handle it. The resurrected logical must not enter the built-in
// decimal path. That path assumes a bytes/fixed underlying with a validated
// precision, and would dereference a nil precision pointer. We route the raw
// value through the custom decoder instead.
func TestRegression_DecimalCustomTypeWrongUnderlyingNoPanic(t *testing.T) {
	ct := func() avro.CustomType {
		return avro.CustomType{
			LogicalType: "decimal",
			Decode:      func(v any, _ *avro.SchemaNode) (any, error) { return v, nil },
		}
	}

	// The exact malformed shape: decimal on int, with no precision. The parse
	// must not panic; valid JSON must never crash the parser.
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

	// We route the resurrected logical's raw Avro-native value through the
	// custom decoder. The wire stays a plain int.
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

// A custom decoder returning ErrSkipCustomType falls through to built-in
// decode. The canonical Avro-native value lands in the target exactly as a
// no-custom decode would. A target the value fits succeeds and equals the
// no-custom decode on both wires. The fall-through re-decodes through the base
// deserializer rather than boxing the canonical int64 into `any` and gating it
// on AssignableTo. A target the value does *not* fit still errors with a
// SemanticError, never a panic in reflect.Set.
func TestRegression_DecodeJSONCustomDecoderConcreteTargetErrors(t *testing.T) {
	ct := avro.CustomType{
		LogicalType: "money",
		AvroType:    "long",
		Decode:      func(v any, _ *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType },
	}
	schema := `{"type":"record","name":"R","fields":[{"name":"p","type":{"type":"long","logicalType":"money"}}]}`
	plain := avro.MustParse(schema)
	s := avrotest.MustParse(t, schema, avro.WithCustomType(ct))

	// Compatible target (long into a named integer): skip-custom == no-custom
	// on both wires.
	type R struct {
		P auditMoney `avro:"p"`
	}
	wire := avrotest.MustEncode(t, plain, R{P: 5})
	jsonBytes := avrotest.MustEncodeJSON(t, plain, R{P: 5})
	var noCustom R
	if _, err := plain.Decode(wire, &noCustom); err != nil {
		t.Fatalf("no-custom decode (oracle): %v", err)
	}
	var rbin R
	if _, err := s.Decode(wire, &rbin); err != nil {
		t.Errorf("binary skip-custom into named integer should succeed (== no-custom): %v", err)
	}
	var rjson R
	if err := s.DecodeJSON(jsonBytes, &rjson); err != nil {
		t.Errorf("JSON skip-custom into named integer should succeed (== no-custom): %v", err)
	}
	if rbin != noCustom || rjson != noCustom {
		t.Errorf("skip-custom value diverges from no-custom: bin=%+v json=%+v want=%+v", rbin, rjson, noCustom)
	}

	// Incompatible target (long into a string field): both wires error
	// gracefully, no panic.
	type Bad struct {
		P string `avro:"p"`
	}
	binErr := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("binary Decode panicked on incompatible target: %v", r)
			}
		}()
		var b Bad
		_, err = s.Decode(wire, &b)
		return
	}()
	if binErr == nil {
		t.Fatal("binary Decode should reject a long into a string field")
	}
	jsonErr := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("DecodeJSON panicked on incompatible target where binary returned %v: %v", binErr, r)
			}
		}()
		var b Bad
		return s.DecodeJSON(jsonBytes, &b)
	}()
	if jsonErr == nil {
		t.Fatal("DecodeJSON should reject a long into a string field, like binary Decode")
	}
}

// A forward reference (a name used before its definition) to a type that
// inherits an enclosing namespace must resolve. The byte-identical
// definition-first ordering already does. We share the namespace-qualified
// retry between build-time backward-ref resolution and finalize-time
// forward-ref resolution. We exercise all three positions a forward ref can
// appear: record field, array items, union branch.
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
	// Definition-first ordering pins the symmetry.
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

// SchemaCache dedup is keyed on the schema string only. WithLaxNames changes
// what that string compiles to: a name a strict parse rejects becomes
// accepted. So a lax parse must not populate the dedup cache. Otherwise a
// later strict cache parse of the same string returns the cached lax schema,
// silently accepting an invalid name that bare Parse rejects.
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

	// Trailing content past the first JSON value is rejected, matching bare
	// Parse. The dedup normalizer must not silently truncate it.
	var c2 avro.SchemaCache
	if _, err := c2.Parse(`{"type":"int"} trailing`); err == nil {
		t.Fatal("SchemaCache.Parse must reject trailing content like bare Parse")
	}
}

// Parsing Canonical Form emits each named type's full body at its *first*
// occurrence in the field walk, not at the textual definition site, per
// Apache Avro's SchemaNormalization rule. For a forward reference the two
// differ: the full body belongs at the reference, with a bare (full)name at
// the later definition. The fingerprint depends on this, so a mismatch breaks
// single-object-encoding and schema-registry interop with Java.
func TestRegression_CanonicalForwardRefFirstOccurrence(t *testing.T) {
	s := avrotest.MustParse(t, `{"type":"record","name":"outer","fields":[
		{"name":"ref","type":{"type":"inner"}},
		{"name":"def","type":{"type":"record","name":"inner","fields":[{"name":"x","type":"int"}]}}]}`)
	const want = `{"name":"outer","type":"record","fields":[` +
		`{"name":"ref","type":{"name":"inner","type":"record","fields":[{"name":"x","type":"int"}]}},` +
		`{"name":"def","type":"inner"}]}`
	if got := string(s.Canonical()); got != want {
		t.Fatalf("Canonical mismatch:\n got %s\nwant %s", got, want)
	}

	// Definition-first ordering of the same field names: the full body stays
	// at the definition, which is also the first occurrence. That is
	// byte-identical to the un-transformed behavior, so existing schemas are
	// unaffected.
	s2 := avro.MustParse(`{"type":"record","name":"outer","fields":[
		{"name":"def","type":{"type":"record","name":"inner","fields":[{"name":"x","type":"int"}]}},
		{"name":"ref","type":"inner"}]}`)
	const wantBack = `{"name":"outer","type":"record","fields":[` +
		`{"name":"def","type":{"name":"inner","type":"record","fields":[{"name":"x","type":"int"}]}},` +
		`{"name":"ref","type":"inner"}]}`
	if got := string(s2.Canonical()); got != wantBack {
		t.Fatalf("definition-first Canonical mismatch:\n got %s\nwant %s", got, wantBack)
	}

	// A namespaced forward ref normalizes its bare reference to the resolved
	// fullname, and emits the full body at the first occurrence.
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

// A bare forward reference whose short name is shared across namespaces must
// still resolve to its in-scope fullname in the canonical form. Full body at
// the first walk occurrence, fullname after, matching Java
// SchemaNormalization. Upgrading a bare reference only when its short name is
// globally unique lets an ambiguous one leak through verbatim. That diverges
// the PCF, fingerprint and single-object header from Java. We cover all four
// forward-ref containers, and String() still preserves the short name as
// written.
func TestMatrix_CanonicalForwardRefAmbiguousShortName(t *testing.T) {
	// f1 forward-refs a.Inner (defined at f2) via each container; f3 defines
	// b.Inner, making the short name "Inner" ambiguous across namespaces.
	defs := `,
		{"name":"f2","type":{"type":"record","name":"Inner","namespace":"a","fields":[{"name":"x","type":"int"}]}},
		{"name":"f3","type":{"type":"record","name":"Inner","namespace":"b","fields":[{"name":"y","type":"int"}]}}]}`
	const aInnerBody = `{"name":"a.Inner","type":"record","fields":[{"name":"x","type":"int"}]}`
	const tail = `,{"name":"f2","type":"a.Inner"},` +
		`{"name":"f3","type":{"name":"b.Inner","type":"record","fields":[{"name":"y","type":"int"}]}}]}`
	cases := []struct{ name, in, want string }{
		{
			"field",
			`{"type":"record","name":"R","namespace":"a","fields":[{"name":"f1","type":"Inner"}` + defs,
			`{"name":"a.R","type":"record","fields":[{"name":"f1","type":` + aInnerBody + `}` + tail,
		},
		{
			"array",
			`{"type":"record","name":"R","namespace":"a","fields":[{"name":"f1","type":{"type":"array","items":"Inner"}}` + defs,
			`{"name":"a.R","type":"record","fields":[{"name":"f1","type":{"type":"array","items":` + aInnerBody + `}}` + tail,
		},
		{
			"map",
			`{"type":"record","name":"R","namespace":"a","fields":[{"name":"f1","type":{"type":"map","values":"Inner"}}` + defs,
			`{"name":"a.R","type":"record","fields":[{"name":"f1","type":{"type":"map","values":` + aInnerBody + `}}` + tail,
		},
		{
			"union",
			`{"type":"record","name":"R","namespace":"a","fields":[{"name":"f1","type":["null","Inner"]}` + defs,
			`{"name":"a.R","type":"record","fields":[{"name":"f1","type":["null",` + aInnerBody + `]}` + tail,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := avro.Parse(c.in)
			if err != nil {
				t.Fatal(err)
			}
			got := string(s.Canonical())
			if got != c.want {
				t.Fatalf("Canonical mismatch:\n got %s\nwant %s", got, c.want)
			}
			// Idempotency: re-parsing the canonical form and re-canonicalizing is
			// a fixed point (the emitted fullnames re-resolve identically).
			s2, err := avro.Parse(got)
			if err != nil {
				t.Fatalf("reparse canonical: %v", err)
			}
			if got2 := string(s2.Canonical()); got2 != got {
				t.Fatalf("Canonical not idempotent:\n round1 %s\nround2 %s", got, got2)
			}
		})
	}

	// Nested: a forward ref "T" inside b.S resolves to b.T (S's namespace),
	// *not* a.T, even though a.T also exists. The resolution namespace
	// switches when descending into a type with its own namespace.
	nested := avro.MustParse(`{"type":"record","name":"R","namespace":"a","fields":[
		{"name":"outer","type":{"type":"record","name":"S","namespace":"b","fields":[
			{"name":"f1","type":"T"},
			{"name":"f2","type":{"type":"record","name":"T","namespace":"b","fields":[{"name":"x","type":"int"}]}}
		]}},
		{"name":"other","type":{"type":"record","name":"T","namespace":"a","fields":[{"name":"y","type":"int"}]}}
	]}`)
	const wantNested = `{"name":"a.R","type":"record","fields":[` +
		`{"name":"outer","type":{"name":"b.S","type":"record","fields":[` +
		`{"name":"f1","type":{"name":"b.T","type":"record","fields":[{"name":"x","type":"int"}]}},` +
		`{"name":"f2","type":"b.T"}]}},` +
		`{"name":"other","type":{"name":"a.T","type":"record","fields":[{"name":"y","type":"int"}]}}]}`
	if got := string(nested.Canonical()); got != wantNested {
		t.Fatalf("nested Canonical mismatch:\n got %s\nwant %s", got, wantNested)
	}

	// We change canonical form only. String() preserves the short reference
	// as written, which is re-parseable in the enclosing namespace.
	field := avro.MustParse(`{"type":"record","name":"R","namespace":"a","fields":[{"name":"f1","type":"Inner"}` + defs)
	if str := field.String(); !strings.Contains(str, `{"name":"f1","type":"Inner"}`) {
		t.Fatalf("String() should preserve the bare short reference, got %s", str)
	}
}

// A union default must select the same branch on both wires. A default string
// with a codepoint above 255 cannot be a bytes or fixed default, since those
// map each codepoint 0-255 to one byte. Binary therefore falls through to the
// string branch. EncodeJSON's raw-UTF-8 appendAvroJSON accepted it as bytes,
// picking a different branch than binary, the default-fill and the metadata
// API.
func TestRegression_UnionBytesFixedDefaultJSONBinaryParity(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		want   any // the Go value both wire formats must decode the filled default into
	}{
		{
			"bytes-branch-codepoint-over-255-falls-through-to-string",
			`{"type":"record","name":"R","fields":[{"name":"f","type":["bytes","string"],"default":"Ā"}]}`,
			"Ā",
		},
		{
			"fixed-branch-codepoint-over-255-falls-through-to-string",
			`{"type":"record","name":"R","fields":[{"name":"f","type":[{"type":"fixed","name":"F","size":2},"string"],"default":"Ā"}]}`,
			"Ā",
		},
		{
			"bytes-branch-codepoint-in-range-stays-bytes",
			`{"type":"record","name":"R","fields":[{"name":"f","type":["bytes","string"],"default":"ÿ"}]}`,
			[]byte{0xff},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := avrotest.MustParse(t, c.schema)
			bw := avrotest.MustAppendEncode(t, s, nil, map[string]any{})
			var bm map[string]any
			avrotest.MustDecode(t, s, bw, &bm)
			jw := avrotest.MustAppendEncodeJSON(t, s, nil, map[string]any{})
			var jm map[string]any
			avrotest.MustDecodeJSON(t, s, jw, &jm)
			if !reflect.DeepEqual(bm["f"], c.want) {
				t.Errorf("binary default-fill f = %T(%v), want %T(%v)", bm["f"], bm["f"], c.want, c.want)
			}
			if !reflect.DeepEqual(jm["f"], c.want) {
				t.Errorf("JSON default-fill f = %T(%v), want %T(%v) — JSON picked a different union branch than binary (wire %s)", jm["f"], jm["f"], c.want, c.want, jw)
			}
		})
	}
}

// Resolve must agree with CheckCompatibility. PCF strips decimal
// precision/scale, so decimal(10,2) and decimal(10,3) are canonical-equal.
// Resolve's canonical-equal fast path therefore accepted a pair
// CheckCompatibility rejects, silently rescaling 3.14 to 0.314. Non-decimal
// logical mismatches are canonical-equal too, but CheckCompatibility allows
// them (reader-logical wins), so those must keep resolving.
func TestMatrix_ResolveHonorsDecimalCompatibility(t *testing.T) {
	dec := func(p, s int) *avro.Schema {
		return avro.MustParse(fmt.Sprintf(`{"type":"bytes","logicalType":"decimal","precision":%d,"scale":%d}`, p, s))
	}
	pairs := []struct {
		name           string
		writer, reader *avro.Schema
	}{
		{"scale-mismatch", dec(10, 2), dec(10, 3)},
		{"precision-mismatch", dec(10, 2), dec(10, 5)},
		{"identical-decimal", dec(10, 2), dec(10, 2)},
		{"long-to-timestamp", avro.MustParse(`"long"`), avro.MustParse(`{"type":"long","logicalType":"timestamp-millis"}`)},
	}
	for _, p := range pairs {
		t.Run(p.name, func(t *testing.T) {
			checkErr := avro.CheckCompatibility(p.writer, p.reader)
			_, resolveErr := avro.Resolve(p.writer, p.reader)
			if (checkErr == nil) != (resolveErr == nil) {
				t.Fatalf("Resolve and CheckCompatibility disagree: CheckCompatibility err=%v, Resolve err=%v", checkErr, resolveErr)
			}
		})
	}

	// The scale mismatch must be rejected, never silently rescaled.
	if _, err := avro.Resolve(dec(10, 2), dec(10, 3)); err == nil {
		t.Fatal("Resolve accepted a decimal scale mismatch that CheckCompatibility rejects (silent rescale)")
	}
}

// Resolve runs CheckCompatibility before its canonical-equal fast path. So a
// schema must be compatible with *itself* for every shape, or a Resolve that
// would have short-circuited through the fast path fails. We pin
// CheckCompatibility(s,s)==nil and Resolve(s,s) success across the schema zoo:
// recursion, mutual recursion, forward references, defaultless enums, and
// logical types. Those are the shapes most likely to trip a compatibility
// walker.
func TestMatrix_ResolveSelfCompatAllShapes(t *testing.T) {
	schemas := []string{
		`"null"`, `"boolean"`, `"int"`, `"long"`, `"float"`, `"double"`, `"bytes"`, `"string"`,
		`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`,
		`{"type":"string","logicalType":"uuid"}`,
		`{"type":"int","logicalType":"date"}`,
		`{"type":"long","logicalType":"timestamp-micros"}`,
		`{"type":"fixed","name":"Dur","size":12,"logicalType":"duration"}`,
		`{"type":"enum","name":"E","symbols":["A","B","C"]}`,               // no default
		`{"type":"enum","name":"E","symbols":["A","B","C"],"default":"A"}`, // with default
		`{"type":"fixed","name":"F","size":16}`,
		`{"type":"array","items":"int"}`,
		`{"type":"map","values":"long"}`,
		`["null","int"]`, `["int","null"]`, `["null","string","long","bytes"]`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`,                                                                                               // required field
		`{"type":"record","name":"R","fields":[{"name":"a","type":"int","default":7}]}`,                                                                                   // defaulted field
		`{"type":"record","name":"R","fields":[{"name":"a","type":"int","aliases":["b"]}]}`,                                                                               // field alias
		`{"type":"record","name":"R","fields":[{"name":"u","type":["null",{"type":"record","name":"S","fields":[{"name":"x","type":"int"}]}]}]}`,                          // record in union
		`{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}}`,                                                                      // array of records
		`{"type":"map","values":["null","string"]}`,                                                                                                                       // map of unions
		`{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]}]}`,                                                                               // recursive
		`{"type":"record","name":"A","fields":[{"name":"b","type":{"type":"record","name":"B","fields":[{"name":"a","type":["null","A"]}]}}]}`,                            // mutually recursive
		`{"type":"record","name":"R","fields":[{"name":"f1","type":"Inner"},{"name":"f2","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}}]}`, // forward ref
	}
	for _, sc := range schemas {
		t.Run(sc, func(t *testing.T) {
			s := avrotest.MustParse(t, sc)
			if err := avro.CheckCompatibility(s, s); err != nil {
				t.Errorf("CheckCompatibility(s,s) rejected a schema as incompatible with itself: %v", err)
			}
			if _, err := avro.Resolve(s, s); err != nil {
				t.Errorf("Resolve(s,s) failed: %v", err)
			}
		})
	}
}

// An empty Avro array decodes to a non-nil empty slice on both wire formats.
// That matches the JSON array decoder and the binary map decoder. A nil slice
// out of binary array decode sits beside a non-nil empty slice out of JSON,
// and a non-nil empty map out of binary map decode. One logical value would
// then have a different Go representation per wire format.
func TestRegression_EmptyArrayDecodesNonNilBothFormats(t *testing.T) {
	type Rec struct {
		Items []int `avro:"items"`
	}
	s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"items","type":{"type":"array","items":"int"}}]}`)

	bin := avrotest.MustAppendEncode(t, s, nil, Rec{Items: nil})
	js := avrotest.MustAppendEncodeJSON(t, s, nil, Rec{Items: nil})

	var bo, jo Rec
	avrotest.MustDecode(t, s, bin, &bo)
	avrotest.MustDecodeJSON(t, s, js, &jo)
	if bo.Items == nil {
		t.Error("binary decode of empty array left the slice nil; want non-nil empty (JSON/map parity)")
	}
	if jo.Items == nil {
		t.Error("JSON decode of empty array left the slice nil; want non-nil empty")
	}
	if len(bo.Items) != 0 || len(jo.Items) != 0 {
		t.Errorf("empty array decoded to non-empty: binary len=%d json len=%d", len(bo.Items), len(jo.Items))
	}

	// Top-level (non-field) empty array decodes non-nil too.
	st := avro.MustParse(`{"type":"array","items":"int"}`)
	tw := avrotest.MustAppendEncode(t, st, nil, []int{})
	var top []int
	avrotest.MustDecode(t, st, tw, &top)
	if top == nil {
		t.Error("binary decode of top-level empty array left the slice nil; want non-nil empty")
	}

	// Cover the other unsafe array element paths that funnel through
	// udArrayBlocks: a string slice (direct) and a pointer-record slice
	// (batch-allocated). All must come back non-nil empty too.
	type Sub struct {
		X int `avro:"x"`
	}
	type Multi struct {
		Strs []string `avro:"strs"`
		Recs []*Sub   `avro:"recs"`
	}
	ms := avro.MustParse(`{"type":"record","name":"M","fields":[
		{"name":"strs","type":{"type":"array","items":"string"}},
		{"name":"recs","type":{"type":"array","items":{"type":"record","name":"Sub","fields":[{"name":"x","type":"int"}]}}}]}`)
	mw := avrotest.MustAppendEncode(t, ms, nil, Multi{})
	var mo Multi
	avrotest.MustDecode(t, ms, mw, &mo)
	if mo.Strs == nil || mo.Recs == nil {
		t.Errorf("unsafe array fields left nil: strs nil=%v recs nil=%v", mo.Strs == nil, mo.Recs == nil)
	}
}

// skipMap bounds its block count against the remaining buffer, like deserMap
// and skipArray. An unbounded int(count) loop truncates a count above 2^31 on
// a 32-bit build (narrow before check) and mis-frames the skip. That
// truncation is not observable on a 64-bit host. So we pin instead that the
// bound does not break the valid map-skip path. A resolved decode takes that
// path when the reader drops a writer's map field.
func TestRegression_SkipMapBoundedValidSkip(t *testing.T) {
	writer := avro.MustParse(`{"type":"record","name":"W","fields":[
		{"name":"m","type":{"type":"map","values":"long"}},
		{"name":"keep","type":"int"}]}`)
	reader := avro.MustParse(`{"type":"record","name":"W","fields":[{"name":"keep","type":"int"}]}`)
	resolved := avrotest.MustResolve(t, writer, reader)
	wire := avrotest.MustEncode(t, writer, map[string]any{
		"m":    map[string]int64{"a": 1, "b": 2, "c": 3},
		"keep": int32(7),
	})
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

// CustomType.Decode must receive the raw Avro-native value its field
// documents: int32 for int, int64 for long, []byte for bytes/fixed. The binary
// path enforces that by suppressing the logical deserializer when a custom
// matches. The JSON path must produce the same raw value, not the
// logical-transformed Go type. Without parity, a custom decoder that works
// through Decode panics or misreads through DecodeJSON.
func TestMatrix_CustomDecodeReceivesRawValueBinaryJSONParity(t *testing.T) {
	cases := []struct {
		name     string
		logical  string
		avroType string
		schema   string
		encode   any    // Encode=nil: the built-in logical encoder handles this
		wantType string // raw Avro-native Go type the Decode callback must receive
	}{
		{"timestamp-millis", "timestamp-millis", "long", `{"type":"long","logicalType":"timestamp-millis"}`, time.UnixMilli(1700000000000).UTC(), "int64"},
		{"date", "date", "int", `{"type":"int","logicalType":"date"}`, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "int32"},
		{"time-micros", "time-micros", "long", `{"type":"long","logicalType":"time-micros"}`, 5 * time.Hour, "int64"},
		{"decimal-bytes", "decimal", "bytes", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, big.NewRat(33, 100), "[]uint8"},
		{"uuid-fixed", "uuid", "fixed", `{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`, [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, "[]uint8"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ct := avro.CustomType{
				LogicalType: c.logical,
				AvroType:    c.avroType,
				Decode:      func(v any, _ *avro.SchemaNode) (any, error) { return fmt.Sprintf("%T", v), nil },
			}
			s := avrotest.MustParse(t, c.schema, ct)
			bin := avrotest.MustEncode(t, s, c.encode)
			jsn := avrotest.MustEncodeJSON(t, s, c.encode)
			var binVal, jsonVal any
			avrotest.MustDecode(t, s, bin, &binVal)
			avrotest.MustDecodeJSON(t, s, jsn, &jsonVal)
			if binVal != c.wantType {
				t.Errorf("binary Decode callback received %v, want raw %s", binVal, c.wantType)
			}
			if jsonVal != c.wantType {
				t.Errorf("JSON Decode callback received %v, want raw %s (binary↔JSON parity)", jsonVal, c.wantType)
			}
		})
	}
}

// The type-safe NewCustomType constructor generates a v.(A) assertion in its
// Decode wrapper. Handing it the logical-transformed value (time.Time) where
// the raw int64 belongs panics on otherwise-valid input. We must round-trip
// through DecodeJSON without panicking.
func TestRegression_CustomDecodeNewCustomTypeJSONNoPanic(t *testing.T) {
	type eventTime time.Time
	ct := avro.NewCustomType[eventTime, int64]("timestamp-millis",
		func(e eventTime, _ *avro.SchemaNode) (int64, error) { return time.Time(e).UnixMilli(), nil },
		func(ms int64, _ *avro.SchemaNode) (eventTime, error) { return eventTime(time.UnixMilli(ms)), nil })
	s := avro.MustParse(`{"type":"long","logicalType":"timestamp-millis"}`, ct)
	ev := eventTime(time.UnixMilli(1700000000000))
	jsn := avrotest.MustEncodeJSON(t, s, ev)
	var out eventTime
	avrotest.MustDecodeJSON(t, s, jsn, &out)
	if !time.Time(out).Equal(time.Time(ev)) {
		t.Errorf("round-trip mismatch: got %v want %v", time.Time(out), time.Time(ev))
	}
}

// A CustomType whose GoType is a pointer (e.g. *url.URL, the documented
// pointer-GoType shape) must fire its Encode on both binary and JSON. The
// binary path checks GoType per indirection level while peeling. The JSON path
// must consult the custom hook before stripping the pointer. Otherwise the
// pointer GoType never matches and we silently skip the encoder.
func TestRegression_CustomEncodePointerGoTypeBinaryJSONParity(t *testing.T) {
	type w struct{ N int64 }
	mk := func(goType reflect.Type) avro.CustomType {
		return avro.CustomType{
			AvroType: "long", LogicalType: "wlt", GoType: goType,
			Encode: func(v any, _ *avro.SchemaNode) (any, error) {
				switch x := v.(type) {
				case *w:
					return x.N, nil
				case w:
					return x.N, nil
				}
				return nil, avro.ErrSkipCustomType
			},
		}
	}
	check := func(t *testing.T, s *avro.Schema, v any) {
		t.Helper()
		bin, errBin := s.Encode(v)
		if errBin != nil {
			t.Fatalf("binary Encode: %v", errBin)
		}
		jsn, errJSON := s.EncodeJSON(v)
		if errJSON != nil {
			t.Fatalf("JSON Encode (custom encoder skipped on JSON?): %v", errJSON)
		}
		var binVal, jsonVal any
		avrotest.MustDecode(t, s, bin, &binVal)
		avrotest.MustDecodeJSON(t, s, jsn, &jsonVal)
		if binVal != int64(5) || jsonVal != int64(5) {
			t.Errorf("binary=%v json=%v, want both int64(5)", binVal, jsonVal)
		}
	}
	t.Run("pointer-GoType", func(t *testing.T) {
		s := avro.MustParse(`{"type":"long","logicalType":"wlt"}`, mk(reflect.TypeOf((*w)(nil))))
		check(t, s, &w{N: 5})
	})
	t.Run("value-GoType-still-works", func(t *testing.T) {
		s := avro.MustParse(`{"type":"long","logicalType":"wlt"}`, mk(reflect.TypeOf(w{})))
		check(t, s, w{N: 5})
	})
	t.Run("value-GoType-user-pointer", func(t *testing.T) {
		// Value GoType with the user passing a pointer: customEncode peels then
		// matches at the value level. Must still work on both paths.
		s := avro.MustParse(`{"type":"long","logicalType":"wlt"}`, mk(reflect.TypeOf(w{})))
		check(t, s, &w{N: 5})
	})
}

// Parsing Canonical Form requires names, namespaces and enum symbols be
// rendered as raw UTF-8 (the STRINGS rule). Java's SchemaNormalization appends
// them verbatim. The characters < > & are reachable in names through the
// public WithLaxNames option (Java's parallel is NameValidator.NO_VALIDATION).
// They must *not* appear as \u00XX escapes, or the Rabin/SHA/MD5 fingerprint
// and the Single Object Encoding header diverge from every other Avro impl.
func TestMatrix_CanonicalRawUTF8ForHTMLChars(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		want   string
	}{
		{"enum symbol", `{"type":"enum","name":"E","symbols":["a<b"]}`, `{"name":"E","type":"enum","symbols":["a<b"]}`},
		{"record fullname", `{"type":"record","name":"R&S","fields":[{"name":"x","type":"int"}]}`, `{"name":"R&S","type":"record","fields":[{"name":"x","type":"int"}]}`},
		{"namespace", `{"type":"record","name":"R","namespace":"a<b.c","fields":[{"name":"x","type":"int"}]}`, `{"name":"a<b.c.R","type":"record","fields":[{"name":"x","type":"int"}]}`},
		{"field name", `{"type":"record","name":"R","fields":[{"name":"x>y","type":"int"}]}`, `{"name":"R","type":"record","fields":[{"name":"x>y","type":"int"}]}`},
		{"name reference", `{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"enum","name":"E&E","symbols":["X"]}},{"name":"b","type":"E&E"}]}`,
			`{"name":"R","type":"record","fields":[{"name":"a","type":{"name":"E&E","type":"enum","symbols":["X"]}},{"name":"b","type":"E&E"}]}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := avrotest.MustParse(t, c.schema, avro.WithLaxNames(nil))
			got := string(s.Canonical())
			if got != c.want {
				t.Errorf("canonical mismatch:\n got = %s\nwant = %s", got, c.want)
			}
			// The fingerprint must hash the raw-UTF-8 bytes. It equals the
			// Rabin of the want form, which is what Java/fastavro produce.
			// The got == want check above already proves the raw chars are
			// present. This guards the cross-impl fingerprint agreement.
			h := avro.NewRabin()
			h.Write([]byte(c.want))
			want64 := h.Sum64()
			h2 := avro.NewRabin()
			h2.Write(s.Canonical())
			if h2.Sum64() != want64 {
				t.Errorf("Rabin fingerprint %#016x != raw-UTF-8 fingerprint %#016x", h2.Sum64(), want64)
			}
		})
	}

	// Boundary: control characters (below 0x20) stay JSON-escaped, only < > &
	// are un-escaped. We emit valid JSON where Java would emit the raw control
	// byte and produce invalid JSON. The un-escape must not over-reach.
	s, err := avro.Parse("{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"a\\u001fb\"]}", avro.WithLaxNames(nil))
	if err != nil {
		t.Fatalf("parse control-char symbol: %v", err)
	}
	ctrlGot := string(s.Canonical())
	ctrlEsc := string([]byte{'\\', 'u', '0', '0', '1', 'f'}) // literal 6-char escape for U+001F
	if !strings.Contains(ctrlGot, ctrlEsc) {
		t.Errorf("control char must remain JSON-escaped in canonical form: %s", ctrlGot)
	}
	if strings.ContainsRune(ctrlGot, rune(0x1f)) {
		t.Errorf("raw control byte must not appear in canonical form: %q", ctrlGot)
	}

	// Boundary: an ordinary schema (no special chars) is byte-identical.
	plain := avro.MustParse(`{"type":"record","name":"P","fields":[{"name":"x","type":"long"}]}`)
	if got, want := string(plain.Canonical()), `{"name":"P","type":"record","fields":[{"name":"x","type":"long"}]}`; got != want {
		t.Errorf("plain canonical changed:\n got = %s\nwant = %s", got, want)
	}
}

// A custom type with a nil Decode callback suppresses the built-in logical
// decoder and produces the raw Avro-native value. CustomType.Decode and doc.go
// both document that. The binary path enforces it by suppressing the logical
// deserializer whenever any matching custom type exists. The JSON path must
// produce the same raw value, not the logical-transformed Go type, even with
// no Decode chain to wrap.
func TestMatrix_CustomDecodeNilRawValueBinaryJSONParity(t *testing.T) {
	type w struct{ N int64 }
	cases := []struct {
		name     string
		schema   string
		logical  string
		avroType string
		wantType string
		encVal   any // value the built-in logical encoder accepts
	}{
		// Every logical type: the drift-guard for jsonDecodeAppliesLogical,
		// which is derived by probing decodeLogical*. An Encode-only
		// non-wildcard custom suppresses the logical decoder, so a
		// decode-into-any must yield the raw Avro-native type on both paths.
		// If the probe wrongly reports a logical as non-transforming, JSON
		// leaks the enriched type here.
		{"date", `{"type":"int","logicalType":"date"}`, "date", "int", "int32", time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`, "time-millis", "int", "int32", 3 * time.Hour},
		{"time-micros", `{"type":"long","logicalType":"time-micros"}`, "time-micros", "long", "int64", 3 * time.Hour},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, "timestamp-millis", "long", "int64", time.UnixMilli(1700000000000).UTC()},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`, "timestamp-micros", "long", "int64", time.UnixMilli(1700000000000).UTC()},
		{"timestamp-nanos", `{"type":"long","logicalType":"timestamp-nanos"}`, "timestamp-nanos", "long", "int64", time.Unix(1700000000, 5).UTC()},
		{"local-timestamp-millis", `{"type":"long","logicalType":"local-timestamp-millis"}`, "local-timestamp-millis", "long", "int64", time.UnixMilli(1700000000000).UTC()},
		{"local-timestamp-micros", `{"type":"long","logicalType":"local-timestamp-micros"}`, "local-timestamp-micros", "long", "int64", time.UnixMilli(1700000000000).UTC()},
		{"local-timestamp-nanos", `{"type":"long","logicalType":"local-timestamp-nanos"}`, "local-timestamp-nanos", "long", "int64", time.Unix(1700000000, 5).UTC()},
		{"decimal-bytes", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, "decimal", "bytes", "[]uint8", big.NewRat(33, 100)},
		{"decimal-fixed", `{"type":"fixed","name":"DF","size":8,"logicalType":"decimal","precision":10,"scale":2}`, "decimal", "fixed", "[]uint8", big.NewRat(33, 100)},
		{"big-decimal", `{"type":"bytes","logicalType":"big-decimal"}`, "big-decimal", "bytes", "[]uint8", big.NewRat(33, 100)},
		{"uuid-string", `{"type":"string","logicalType":"uuid"}`, "uuid", "string", "string", "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		{"uuid-fixed", `{"type":"fixed","name":"UF","size":16,"logicalType":"uuid"}`, "uuid", "fixed", "[]uint8", [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},
		{"duration", `{"type":"fixed","name":"DUR","size":12,"logicalType":"duration"}`, "duration", "fixed", "[]uint8", [12]byte{1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Encode through a plain (no-custom) schema using the built-in
			// logical encoder, then decode through an Encode-only custom schema.
			plain := avro.MustParse(c.schema)
			bin := avrotest.MustEncode(t, plain, c.encVal)
			jsn := avrotest.MustEncodeJSON(t, plain, c.encVal)
			// Encode-only custom (Decode==nil) suppresses the logical decoder,
			// giving the raw Avro-native value on both decode paths. We never
			// invoke the Encode callback here (we only decode); its presence
			// is what makes the type Encode-only.
			custom := avro.MustParse(c.schema, avro.CustomType{
				LogicalType: c.logical, AvroType: c.avroType, GoType: reflect.TypeOf(w{}),
				Encode: func(v any, _ *avro.SchemaNode) (any, error) { return v, nil },
			})
			var bv, jv any
			avrotest.MustDecode(t, custom, bin, &bv)
			avrotest.MustDecodeJSON(t, custom, jsn, &jv)
			if got := fmt.Sprintf("%T", bv); got != c.wantType {
				t.Errorf("binary nil-Decode produced %s, want raw %s", got, c.wantType)
			}
			if got := fmt.Sprintf("%T", jv); got != c.wantType {
				t.Errorf("JSON nil-Decode produced %s, want raw %s (binary↔JSON parity)", got, c.wantType)
			}
		})
	}
}

// A custom encoder with a pointer GoType registered on a *union branch* must
// fire on both binary and JSON encode. The binary path passes the un-peeled
// value to the branch serializer. The JSON path must dispatch the union before
// the pointer-peel loop, so the branch's custom encoder matches the pointer.
func TestRegression_CustomEncodePointerGoTypeUnionBranchParity(t *testing.T) {
	type wu struct{ N int64 }
	s := avro.MustParse(`["null",{"type":"long","logicalType":"timestamp-millis"}]`, avro.CustomType{
		LogicalType: "timestamp-millis", AvroType: "long", GoType: reflect.TypeOf(&wu{}),
		Encode: func(v any, _ *avro.SchemaNode) (any, error) { return v.(*wu).N, nil },
		Decode: func(v any, _ *avro.SchemaNode) (any, error) { return wu{N: v.(int64)}, nil },
	})
	if _, err := s.Encode(&wu{N: 1000}); err != nil {
		t.Fatalf("binary Encode of union-branch pointer-GoType custom: %v", err)
	}
	if _, err := s.EncodeJSON(&wu{N: 1000}); err != nil {
		t.Errorf("JSON Encode of union-branch pointer-GoType custom failed but binary succeeded: %v", err)
	}
	// A pointer to a tagged-union map must still encode (regression guard for
	// dispatching union before the peel loop).
	su := avro.MustParse(`["null","int","string"]`)
	m := map[string]any{"int": int32(7)}
	if _, err := su.EncodeJSON(&m); err != nil {
		t.Errorf("EncodeJSON of *map tagged union regressed: %v", err)
	}
}

// A custom decimal/big-decimal encoder (Encode!=nil) suppresses the built-in
// decimal serializer to base bytes on the binary path. We write a value
// matching the custom GoType as its raw []byte, and reject a non-matching
// pass-through such as *big.Rat. JSON encode must agree on both directions.
func TestRegression_CustomDecimalEncodePassThroughParity(t *testing.T) {
	type bdec struct{ Raw []byte }
	for _, logical := range []string{"decimal", "big-decimal"} {
		t.Run(logical, func(t *testing.T) {
			// decimal carries precision/scale; big-decimal (AVRO-4124) is
			// scale-free (the scale rides in the payload).
			schema := `{"type":"bytes","logicalType":"big-decimal"}`
			if logical == "decimal" {
				schema = `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
			}
			s := avro.MustParse(schema, avro.CustomType{
				LogicalType: logical, AvroType: "bytes", GoType: reflect.TypeOf(bdec{}),
				Encode: func(v any, _ *avro.SchemaNode) (any, error) { return v.(bdec).Raw, nil },
				Decode: func(v any, _ *avro.SchemaNode) (any, error) { return bdec{Raw: v.([]byte)}, nil },
			})
			// Matching GoType: encodes raw bytes on both, round-trips.
			if _, err := s.Encode(bdec{Raw: []byte{0x21}}); err != nil {
				t.Fatalf("binary Encode of matching custom: %v", err)
			}
			if _, err := s.EncodeJSON(bdec{Raw: []byte{0x21}}); err != nil {
				t.Fatalf("JSON Encode of matching custom: %v", err)
			}
			// Pass-through *big.Rat: rejected on both paths (no decimal arm).
			_, eb := s.Encode(big.NewRat(33, 100))
			_, ej := s.EncodeJSON(big.NewRat(33, 100))
			if eb == nil {
				t.Errorf("binary must reject *big.Rat pass-through for custom %s", logical)
			}
			if ej == nil {
				t.Errorf("JSON must reject *big.Rat pass-through for custom %s (binary↔JSON parity)", logical)
			}
		})
	}

	// Non-custom decimal still coerces *big.Rat (the fix must not disable the
	// decimal arm globally).
	plain := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)
	if _, err := plain.EncodeJSON(big.NewRat(33, 100)); err != nil {
		t.Errorf("non-custom decimal JSON encode of *big.Rat regressed: %v", err)
	}
}

// The binary fixed build suppresses the serializer to base serSize for every
// fixed logical (decimal, duration, uuid) when a custom Encode exists. We then
// write a non-matching pass-through value as raw bytes, not through the strict
// logical encoder. JSON encode must agree. A 16-char non-UUID string against
// fixed+uuid+custom must encode (raw) on both, not reject on JSON via
// parseUUID.
func TestRegression_CustomEncodeFixedLogicalBaseBytesParity(t *testing.T) {
	type my16 struct{ B [16]byte }
	s := avro.MustParse(`{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`, avro.CustomType{
		LogicalType: "uuid", AvroType: "fixed", GoType: reflect.TypeOf(my16{}),
		Encode: func(v any, _ *avro.SchemaNode) (any, error) { b := v.(my16).B; return b[:], nil },
		Decode: func(v any, _ *avro.SchemaNode) (any, error) { return my16{B: [16]byte(v.([]byte))}, nil },
	})
	// Matching custom value round-trips.
	var x my16
	for i := range x.B {
		x.B[i] = byte(i)
	}
	if _, err := s.Encode(x); err != nil {
		t.Fatalf("binary Encode of matching custom: %v", err)
	}
	if _, err := s.EncodeJSON(x); err != nil {
		t.Fatalf("JSON Encode of matching custom: %v", err)
	}
	// Pass-through 16-char non-UUID string: base serSize accepts it on binary;
	// JSON must too (logical uuid arm suppressed by the custom encoder).
	_, eb := s.Encode("0123456789abcdef")
	_, ej := s.EncodeJSON("0123456789abcdef")
	if (eb == nil) != (ej == nil) {
		t.Errorf("fixed+uuid+custom pass-through string: binary err=%v json err=%v (must agree)", eb, ej)
	}
}

// A wildcard CustomType (empty LogicalType and AvroType, the property-based
// dispatch pattern) is excluded from the binary decoder-suppression gate.
// Binary therefore leaves the built-in logical decoder in place and feeds the
// callback the enriched value. The JSON decode path must *not* suppress the
// transform for wildcards either, or it feeds raw int64/[]byte instead.
// Non-wildcard customs still suppress to the raw value on both paths.
func TestRegression_WildcardCustomDecodeBinaryJSONParity(t *testing.T) {
	skip := func(v any, _ *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType }
	typeOf := func(v any) string {
		if v == nil {
			return "<nil>"
		}
		return reflect.TypeOf(v).String()
	}
	decBoth := func(t *testing.T, s *avro.Schema, enc any) (string, string) {
		t.Helper()
		bin := avrotest.MustEncode(t, s, enc)
		js := avrotest.MustEncodeJSON(t, s, enc)
		var bo, jo any
		avrotest.MustDecode(t, s, bin, &bo)
		avrotest.MustDecodeJSON(t, s, js, &jo)
		return typeOf(bo), typeOf(jo)
	}

	cases := []struct {
		name     string
		schema   string
		enc      any
		enriched string // the non-raw Go type the wildcard must see on both paths
		rawType  string // the raw Avro-native type a non-wildcard custom suppresses to
		logical  string
		avroType string
	}{
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, int64(1687221496000), "time.Time", "int64", "timestamp-millis", "long"},
		{"date", `{"type":"int","logicalType":"date"}`, int32(19000), "time.Time", "int32", "date", "int"},
		{"decimal", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, []byte{0x21}, "*big.Rat", "[]uint8", "decimal", "bytes"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Wildcard: enriched on both paths.
			ws := avro.MustParse(c.schema, avro.CustomType{Decode: skip})
			wb, wj := decBoth(t, ws, c.enc)
			if wb != wj {
				t.Errorf("wildcard: binary=%s json=%s (must agree)", wb, wj)
			}
			if wb != c.enriched {
				t.Errorf("wildcard binary should keep enriched %s, got %s", c.enriched, wb)
			}

			// Non-wildcard (LogicalType-only / AvroType-only / both): raw on both.
			for _, ct := range []avro.CustomType{
				{LogicalType: c.logical, Decode: skip},
				{AvroType: c.avroType, Decode: skip},
				{LogicalType: c.logical, AvroType: c.avroType, Decode: skip},
			} {
				ns := avro.MustParse(c.schema, ct)
				nb, nj := decBoth(t, ns, c.enc)
				if nb != nj {
					t.Errorf("non-wildcard: binary=%s json=%s (must agree)", nb, nj)
				}
				if nb != c.rawType {
					t.Errorf("non-wildcard should suppress to raw %s, got %s", c.rawType, nb)
				}
			}
		})
	}
}

// Encode-side mirror of the wildcard parity. The binary encoder-suppression
// gate also excludes wildcards, so a wildcard with an Encode keeps the
// built-in decimal/fixed serializer, which accepts *big.Rat. The JSON encode
// arms must gate on the same predicate, not on the custom[node].encode != nil
// proxy. Otherwise binary accepts *big.Rat while JSON rejects it. Non-wildcard
// Encode customs still suppress to base bytes on both.
func TestRegression_WildcardCustomEncodeBinaryJSONParity(t *testing.T) {
	skipEnc := func(v any, _ *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType }
	schemas := []string{
		`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`,
		`{"type":"fixed","name":"F","size":8,"logicalType":"decimal","precision":10,"scale":2}`,
		`{"type":"bytes","logicalType":"big-decimal"}`,
	}
	for _, sch := range schemas {
		// Wildcard with Encode: we accept the *big.Rat pass-through on both
		// paths. Binary keeps serBytesDecimal/serFixedDecimal, a wildcard being
		// excluded from the encoder-suppression gate.
		ws := avro.MustParse(sch, avro.CustomType{Encode: skipEnc})
		_, web := ws.Encode(big.NewRat(33, 100))
		_, wej := ws.EncodeJSON(big.NewRat(33, 100))
		if (web == nil) != (wej == nil) {
			t.Errorf("wildcard encode %s: binary err=%v json err=%v (must agree)", sch[:34], web, wej)
		}
		if web != nil {
			t.Errorf("wildcard encode %s should ACCEPT *big.Rat (binary), got %v", sch[:34], web)
		}

		// Non-wildcard with Encode: suppressed to base bytes, so the *big.Rat
		// pass-through is rejected on both paths.
		ns := avro.MustParse(sch, avro.CustomType{
			LogicalType: logicalOf(sch), AvroType: avroTypeOf(sch), Encode: skipEnc,
		})
		_, neb := ns.Encode(big.NewRat(33, 100))
		_, nej := ns.EncodeJSON(big.NewRat(33, 100))
		if (neb == nil) != (nej == nil) {
			t.Errorf("non-wildcard encode %s: binary err=%v json err=%v (must agree)", sch[:34], neb, nej)
		}
		if neb == nil {
			t.Errorf("non-wildcard encode %s should REJECT *big.Rat pass-through (suppressed)", sch[:34])
		}
	}
}

func logicalOf(sch string) string {
	if strings.Contains(sch, "big-decimal") {
		return "big-decimal"
	}
	return "decimal"
}
func avroTypeOf(sch string) string {
	if strings.Contains(sch, `"type":"fixed"`) {
		return "fixed"
	}
	return "bytes"
}

// A wildcard CustomType's Encode callback must fire the same number of times
// on both wires. The binary 2-branch ["null",T] fast path skips the null
// branch for a non-nil value, so the hook fires once. A JSON union try-each
// that trials null first fires it a spurious second time. That double-fires a
// side-effecting wildcard. N>=3 unions trial null on both paths and already
// agree, so the hazard is specific to 2-branch null-first unions.
func TestMatrix_WildcardEncodeCallbackCountUnionParity(t *testing.T) {
	count := func(schema string, v any) (bin, jsonN int) {
		var n int
		s := avro.MustParse(schema, avro.CustomType{
			Encode: func(any, *avro.SchemaNode) (any, error) { n++; return nil, avro.ErrSkipCustomType },
		})
		n = 0
		if _, err := s.Encode(v); err != nil {
			t.Fatalf("Encode %s: %v", schema, err)
		}
		bin = n
		n = 0
		if _, err := s.EncodeJSON(v); err != nil {
			t.Fatalf("EncodeJSON %s: %v", schema, err)
		}
		return bin, n
	}
	ts := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	cases := []struct {
		name, schema string
		v            any
	}{
		{"2-branch null-first", `["null",{"type":"long","logicalType":"timestamp-millis"}]`, ts},
		{"2-branch null-second", `[{"type":"long","logicalType":"timestamp-millis"},"null"]`, ts},
		{"3-branch null-first", `["null",{"type":"long","logicalType":"timestamp-millis"},"string"]`, ts},
		{"2-branch null-first int", `["null","int"]`, int32(7)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b, j := count(c.schema, c.v)
			if b != j {
				t.Errorf("wildcard Encode fired binary=%d json=%d times (must agree)", b, j)
			}
		})
	}
}

// customLogicalCase is one logical type plus a value the built-in logical
// encoder accepts. It carries the raw Avro-native Go type the suppressed
// decoder produces too. We share it between the no-callback-suppression and
// promotion-suppression regression tests so both cover every logical type
// uniformly.
type customLogicalCase struct {
	name     string
	schema   string
	logical  string
	avroType string
	encVal   any
	rawType  string // %T of the raw Avro-native value a suppressed decode yields
}

func customLogicalCases() []customLogicalCase {
	return []customLogicalCase{
		{"date", `{"type":"int","logicalType":"date"}`, "date", "int", time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "int32"},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`, "time-millis", "int", 3 * time.Hour, "int32"},
		{"time-micros", `{"type":"long","logicalType":"time-micros"}`, "time-micros", "long", 3 * time.Hour, "int64"},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, "timestamp-millis", "long", time.UnixMilli(1700000000000).UTC(), "int64"},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`, "timestamp-micros", "long", time.UnixMilli(1700000000000).UTC(), "int64"},
		{"timestamp-nanos", `{"type":"long","logicalType":"timestamp-nanos"}`, "timestamp-nanos", "long", time.Unix(1700000000, 5).UTC(), "int64"},
		{"local-timestamp-millis", `{"type":"long","logicalType":"local-timestamp-millis"}`, "local-timestamp-millis", "long", time.UnixMilli(1700000000000).UTC(), "int64"},
		{"local-timestamp-micros", `{"type":"long","logicalType":"local-timestamp-micros"}`, "local-timestamp-micros", "long", time.UnixMilli(1700000000000).UTC(), "int64"},
		{"local-timestamp-nanos", `{"type":"long","logicalType":"local-timestamp-nanos"}`, "local-timestamp-nanos", "long", time.Unix(1700000000, 5).UTC(), "int64"},
		{"decimal-bytes", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, "decimal", "bytes", big.NewRat(33, 100), "[]uint8"},
		{"decimal-fixed", `{"type":"fixed","name":"DF","size":8,"logicalType":"decimal","precision":10,"scale":2}`, "decimal", "fixed", big.NewRat(33, 100), "[]uint8"},
		{"big-decimal", `{"type":"bytes","logicalType":"big-decimal"}`, "big-decimal", "bytes", big.NewRat(33, 100), "[]uint8"},
		{"uuid-string", `{"type":"string","logicalType":"uuid"}`, "uuid", "string", "6ba7b810-9dad-11d1-80b4-00c04fd430c8", "string"},
		{"uuid-fixed", `{"type":"fixed","name":"UF","size":16,"logicalType":"uuid"}`, "uuid", "fixed", "6ba7b810-9dad-11d1-80b4-00c04fd430c8", "[]uint8"},
		{"duration", `{"type":"fixed","name":"DUR","size":12,"logicalType":"duration"}`, "duration", "fixed", avro.Duration{Months: 1, Days: 2, Milliseconds: 3}, "[]uint8"},
	}
}

// A CustomType that matches a logical node but provides neither callback still
// suppresses the built-in logical decoder on binary. hasMatchingCustomType
// counts callback-less matchers, excluding only wildcards. Per
// CustomType.Decode it yields the raw Avro-native type, and DecodeJSON must
// yield the same one. It will not, if the wiring for a callback-less matcher
// returns early before installing the JSON suppress-wrapper. We drive this
// across every logical and both matcher forms.
func TestMatrix_CustomNoCallbackSuppressionBinaryJSONParity(t *testing.T) {
	matchers := []struct {
		name string
		make func(c customLogicalCase) avro.CustomType
	}{
		{"logical-only", func(c customLogicalCase) avro.CustomType { return avro.CustomType{LogicalType: c.logical} }},
		{"avrotype-only", func(c customLogicalCase) avro.CustomType { return avro.CustomType{AvroType: c.avroType} }},
	}
	for _, c := range customLogicalCases() {
		for _, m := range matchers {
			t.Run(c.name+"/"+m.name, func(t *testing.T) {
				plain := avro.MustParse(c.schema)
				bin := avrotest.MustEncode(t, plain, c.encVal)
				jsn := avrotest.MustEncodeJSON(t, plain, c.encVal)
				cs := avro.MustParse(c.schema, m.make(c))
				var bv, jv any
				avrotest.MustDecode(t, cs, bin, &bv)
				avrotest.MustDecodeJSON(t, cs, jsn, &jv)
				if got := fmt.Sprintf("%T", bv); got != c.rawType {
					t.Errorf("binary callback-less Decode produced %s, want raw %s", got, c.rawType)
				}
				if got := fmt.Sprintf("%T", jv); got != c.rawType {
					t.Errorf("JSON callback-less DecodeJSON produced %s, want raw %s (binary<->JSON parity)", got, c.rawType)
				}
				if !reflect.DeepEqual(bv, jv) {
					t.Errorf("callback-less decode divergence: binary=%#v json=%#v", bv, jv)
				}
			})
		}
	}
}

// A matching CustomType suppresses the reader's built-in logical decoder. That
// suppression must hold whether we decode a value directly or reach it through
// a writer-to-reader promotion. A promotion deser that re-applies the reader's
// logical conversion unconditionally breaks that. One reader+custom then feeds
// the raw type from a direct long wire and the enriched type from a promoted
// int wire. That is an inconsistency inside the binary path itself. We drive
// this across every long-backed logical and all four callback configurations.
// The decode-bearing configs record which raw type they were handed, so a
// type-only check cannot mask a value divergence.
func TestMatrix_CustomPromotionHonorsLogicalSuppression(t *testing.T) {
	dummyGo := reflect.TypeOf(struct{ N int64 }{})
	mark := func(v any, _ *avro.SchemaNode) (any, error) { return "raw:" + fmt.Sprintf("%T", v), nil }
	enc := func(v any, _ *avro.SchemaNode) (any, error) { return v, nil }
	configs := []struct {
		name string
		make func(c customLogicalCase) avro.CustomType
	}{
		{"no-callbacks", func(c customLogicalCase) avro.CustomType { return avro.CustomType{LogicalType: c.logical} }},
		{"encode-only", func(c customLogicalCase) avro.CustomType {
			return avro.CustomType{LogicalType: c.logical, AvroType: c.avroType, GoType: dummyGo, Encode: enc}
		}},
		{"decode-only", func(c customLogicalCase) avro.CustomType {
			return avro.CustomType{LogicalType: c.logical, AvroType: c.avroType, Decode: mark}
		}},
		{"both", func(c customLogicalCase) avro.CustomType {
			return avro.CustomType{LogicalType: c.logical, AvroType: c.avroType, GoType: dummyGo, Encode: enc, Decode: mark}
		}},
	}
	for _, c := range customLogicalCases() {
		if c.avroType != "long" {
			continue // int->long promotion applies only to long-backed logicals
		}
		for _, cfg := range configs {
			t.Run(c.name+"/"+cfg.name, func(t *testing.T) {
				ct := cfg.make(c)
				r := avro.MustParse(c.schema, ct)

				longWire, err := avro.MustParse(c.schema).Encode(c.encVal)
				if err != nil {
					t.Fatalf("encode long wire: %v", err)
				}
				var direct any
				if _, err := r.Decode(longWire, &direct); err != nil {
					t.Fatalf("direct Decode: %v", err)
				}

				w := avro.MustParse(`"int"`)
				resolved, err := avro.Resolve(w, r)
				if err != nil {
					t.Fatalf("Resolve: %v", err)
				}
				intWire, _ := w.Encode(int32(1700000000))
				var promoted any
				if _, err := resolved.Decode(intWire, &promoted); err != nil {
					t.Fatalf("promoted Decode: %v", err)
				}

				// decode-only/both: the marker value records the raw type fed to
				// Decode and must match. no-callbacks/encode-only: the result Go
				// type itself (raw int64 vs enriched time.X) must match.
				dv, pv := fmt.Sprintf("%T=%v", direct, direct), fmt.Sprintf("%T=%v", promoted, promoted)
				dMark, _ := direct.(string)
				pMark, _ := promoted.(string)
				if dMark != "" || pMark != "" {
					if dMark != pMark {
						t.Errorf("custom Decode fed different raw types: direct=%q promoted=%q", dMark, pMark)
					}
					return
				}
				if fmt.Sprintf("%T", direct) != fmt.Sprintf("%T", promoted) {
					t.Errorf("promotion ignored custom suppression: direct=%s promoted=%s", dv, pv)
				}
			})
		}
	}
}

// On a Resolve-returned schema, DecodeJSON consumes writer-shaped JSON and
// applies full writer-to-reader resolution. That matches Java's
// ResolvingDecoder over a JsonDecoder built with the writer schema. We take
// the binary resolved decode as the oracle: resolved.DecodeJSON(writerJSON)
// must equal resolved.Decode(writerBinary). Decoding against the bare reader
// node instead errors on a writer-only enum symbol where binary produces the
// reader default.
func TestMatrix_ResolvedDecodeJSONMatchesBinary(t *testing.T) {
	cases := []struct {
		name           string
		writer, reader string
		readerOpts     []avro.SchemaOpt
		writerVal      any
	}{
		{
			"enum-writer-symbol-to-reader-default",
			`{"type":"enum","name":"E","symbols":["A","B","C"]}`,
			`{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}`,
			nil, "C", // only in writer; resolution -> reader default "A"
		},
		{
			"promotion-int-to-long",
			`"int"`, `"long"`, nil, int32(5),
		},
		{
			"promotion-int-to-timestamp-logical",
			`"int"`, `{"type":"long","logicalType":"timestamp-millis"}`, nil, int32(1700000000),
		},
		{
			"promotion-with-nocallback-custom-suppression",
			`"int"`, `{"type":"long","logicalType":"timestamp-millis"}`,
			[]avro.SchemaOpt{avro.CustomType{LogicalType: "timestamp-millis"}}, int32(1700000000),
		},
		{
			"record-add-default-drop-writer-field-promote",
			`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"x","type":"int"}]}`,
			`{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"c","type":"int","default":99}]}`,
			nil, map[string]any{"a": int32(1), "x": int32(2)},
		},
		{
			"record-field-rename-via-alias",
			`{"type":"record","name":"R","fields":[{"name":"old","type":"int"}]}`,
			`{"type":"record","name":"R","fields":[{"name":"new","type":"int","aliases":["old"]}]}`,
			nil, map[string]any{"old": int32(7)},
		},
		{
			"union-writer-branch-promote",
			`["null","int"]`, `["null","long"]`, nil, int32(9),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := avro.MustParse(c.writer)
			r := avro.MustParse(c.reader, c.readerOpts...)
			resolved, err := avro.Resolve(w, r)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			binWire, err := w.Encode(c.writerVal)
			if err != nil {
				t.Fatalf("writer Encode: %v", err)
			}
			jsonWire, err := w.EncodeJSON(c.writerVal)
			if err != nil {
				t.Fatalf("writer EncodeJSON: %v", err)
			}
			var binOut, jsonOut any
			if _, err := resolved.Decode(binWire, &binOut); err != nil {
				t.Fatalf("resolved.Decode (binary oracle): %v", err)
			}
			if err := resolved.DecodeJSON(jsonWire, &jsonOut); err != nil {
				t.Fatalf("resolved.DecodeJSON: %v", err)
			}
			if !reflect.DeepEqual(binOut, jsonOut) {
				t.Errorf("resolved JSON decode != binary decode:\n  binary=%#v\n  json  =%#v", binOut, jsonOut)
			}
		})
	}

	// The headline Java behavior, spelled out explicitly.
	t.Run("enum-default-value-explicit", func(t *testing.T) {
		w := avro.MustParse(`{"type":"enum","name":"E","symbols":["A","B","C"]}`)
		r := avro.MustParse(`{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}`)
		resolved := avrotest.MustResolve(t, w, r)
		var got any
		avrotest.MustDecodeJSON(t, resolved, []byte(`"C"`), &got)
		if got != "A" {
			t.Errorf("writer-only enum symbol via JSON resolution: got %v, want reader default A", got)
		}
	})
}

// A resolved schema's DecodeJSON must preserve tagged union branch identity
// through decode and re-encode. That includes the decoded value when
// resolution differs per branch. The {"branch": value} envelope is the only
// carrier of the writer's choice when two branches accept the same value.
// Re-deriving by first match rewrites it. Writer E2/"A" with reader E2
// dropping "A" for default "Y" yields "Y", where a flip to E1 yields "A".
// Oracle: Java's readIndex reads the label into the exact index, and binary
// Decode and fastavro's json_reader agree on "Y".
func TestRegression_ResolvedJSONTaggedUnionValueMatchesBinary(t *testing.T) {
	w := avro.MustParse(`[{"type":"enum","name":"E1","symbols":["A"]},{"type":"enum","name":"E2","symbols":["A","Y"]}]`)
	r := avro.MustParse(`[{"type":"enum","name":"E1","symbols":["A"]},{"type":"enum","name":"E2","symbols":["Y"],"default":"Y"}]`)
	resolved, err := avro.Resolve(w, r)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	// Binary oracle: the same tagged branch choice on the binary wire.
	wire, err := w.Encode(map[string]any{"E2": "A"})
	if err != nil {
		t.Fatalf("writer Encode tagged: %v", err)
	}
	var binOut any
	if _, err := resolved.Decode(wire, &binOut); err != nil {
		t.Fatalf("resolved.Decode: %v", err)
	}
	if binOut != "Y" {
		t.Fatalf("binary oracle: got %#v, want reader enum default \"Y\"", binOut)
	}

	var jsonOut any
	if err := resolved.DecodeJSON([]byte(`{"E2":"A"}`), &jsonOut); err != nil {
		t.Fatalf("resolved.DecodeJSON tagged: %v", err)
	}
	if !reflect.DeepEqual(jsonOut, binOut) {
		t.Errorf("resolved JSON decode diverged from binary on a tagged union:\n  binary=%#v\n  json  =%#v", binOut, jsonOut)
	}
}

// The tagged {"branch": value} envelope names the writer's union branch. A
// resolved DecodeJSON must dispatch on that name exactly like binary Decode
// dispatches on the wire index. Each shape declares a branch pair whose values
// are interchangeable, so only the envelope carries the choice. Naming the
// later branch must not silently rewrite it to the earlier one. We put the
// union in a record and add a defaulted reader field so writer != reader. The
// observable is the TaggedUnions envelope key, compared against binary Decode
// of the equivalent tagged wire.
func TestMatrix_ResolvedJSONTaggedUnionBranchIdentity(t *testing.T) {
	cases := []struct {
		name   string
		union  string // the colliding union (writer == reader)
		branch string // tagged branch the writer names (the later, collision-prone one)
		value  any    // the branch value for the binary-oracle encode
		json   string // the branch value as writer-shaped JSON
	}{
		{
			"enum-vs-string",
			`["string",{"type":"enum","name":"E","symbols":["A"]}]`,
			"E", "A", `"A"`,
		},
		{
			"two-records",
			`[{"type":"record","name":"R1","fields":[{"name":"f","type":"string"}]},{"type":"record","name":"R2","fields":[{"name":"f","type":"string"}]}]`,
			"R2", map[string]any{"f": "x"}, `{"f":"x"}`,
		},
		{
			"two-enums",
			`[{"type":"enum","name":"E1","symbols":["A","B"]},{"type":"enum","name":"E2","symbols":["A","C"]}]`,
			"E2", "A", `"A"`,
		},
		{
			"two-fixed",
			`[{"type":"fixed","name":"F1","size":2},{"type":"fixed","name":"F2","size":2}]`,
			"F2", []byte("ab"), `"ab"`,
		},
		{
			"map-vs-record",
			`[{"type":"map","values":"string"},{"type":"record","name":"R","fields":[{"name":"f","type":"string"}]}]`,
			"R", map[string]any{"f": "x"}, `{"f":"x"}`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := avro.MustParse(`{"type":"record","name":"Top","fields":[{"name":"u","type":` + c.union + `}]}`)
			r := avro.MustParse(`{"type":"record","name":"Top","fields":[{"name":"u","type":` + c.union + `},{"name":"pad","type":"int","default":0}]}`)
			resolved, err := avro.Resolve(w, r)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			wire, err := w.Encode(map[string]any{"u": map[string]any{c.branch: c.value}})
			if err != nil {
				t.Fatalf("writer Encode tagged: %v", err)
			}
			var binOut, jsonOut any
			if _, err := resolved.Decode(wire, &binOut, avro.TaggedUnions()); err != nil {
				t.Fatalf("resolved.Decode: %v", err)
			}
			if err := resolved.DecodeJSON([]byte(`{"u":{"`+c.branch+`":`+c.json+`}}`), &jsonOut, avro.TaggedUnions()); err != nil {
				t.Fatalf("resolved.DecodeJSON tagged: %v", err)
			}
			binKey := unionEnvelopeKey(t, binOut)
			jsonKey := unionEnvelopeKey(t, jsonOut)
			if binKey != c.branch {
				t.Fatalf("binary oracle picked branch %q, want %q (test construction)", binKey, c.branch)
			}
			if jsonKey != c.branch {
				t.Errorf("tagged JSON branch rewritten: envelope named %q, decoded as %q", c.branch, jsonKey)
			}
			if !reflect.DeepEqual(jsonOut, binOut) {
				t.Errorf("resolved JSON decode != binary decode:\n  binary=%#v\n  json  =%#v", binOut, jsonOut)
			}
		})
	}
}

// unionEnvelopeKey extracts the single {branch: value} envelope key of a
// decoded record's "u" union field.
func unionEnvelopeKey(t *testing.T, out any) string {
	t.Helper()
	m, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("decoded top not a map: %#v", out)
	}
	env, ok := m["u"].(map[string]any)
	if !ok {
		t.Fatalf("union field not enveloped: %#v", m["u"])
	}
	if len(env) != 1 {
		t.Fatalf("envelope not single-key: %#v", env)
	}
	for k := range env {
		return k
	}
	return ""
}

// A resolved schema's DecodeJSON must match its binary Decode, the oracle. That
// holds even when the writer carries a CustomType whose Decode its own Encode
// cannot reproduce. decodeJSONResolved transforms writer-JSON into
// writer-binary before the resolving decode. We run that transform against a
// custom-free view of the writer. Decoding writer-JSON through the writer's own
// custom Decode produces a Go-domain value the re-encode cannot invert. The
// invertible-custom cells take the identical path and round-trip either way, so
// they are the control rather than the probe.
func TestMatrix_ResolvedDecodeJSONWriterCustomDecodeRawRoundTrip(t *testing.T) {
	type domainTS struct{ ms int64 }
	type domainDec struct{ raw string }

	tsType := reflect.TypeFor[domainTS]()
	decType := reflect.TypeFor[domainDec]()

	// Decode-only customs (read-side domain mapping, no Encode) are the probe
	// cells. The writer encodes the Avro-native/enriched value through the
	// built-in encoder, since a Decode-only custom does not suppress encode.
	// That isolates the hazard to the resolved-JSON decode round-trip.
	logicals := []struct {
		name         string
		fieldType    string
		ct           avro.CustomType
		writerXVal   any
		assertDomain func(t *testing.T, x any)
	}{
		{
			"timestamp-millis",
			`{"type":"long","logicalType":"timestamp-millis"}`,
			avro.CustomType{
				LogicalType: "timestamp-millis", AvroType: "long", GoType: tsType,
				Decode: func(v any, _ *avro.SchemaNode) (any, error) { return domainTS{ms: v.(int64)}, nil },
			},
			time.UnixMilli(1700000000000).UTC(),
			func(t *testing.T, x any) {
				if _, ok := x.(domainTS); !ok {
					t.Fatalf("reader custom Decode did not fire (vacuous pass): x=%#v", x)
				}
			},
		},
		{
			"decimal-bytes",
			`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`,
			avro.CustomType{
				LogicalType: "decimal", AvroType: "bytes", GoType: decType,
				Decode: func(v any, _ *avro.SchemaNode) (any, error) { return domainDec{raw: string(v.([]byte))}, nil },
			},
			big.NewRat(12345, 100), // 123.45
			func(t *testing.T, x any) {
				if _, ok := x.(domainDec); !ok {
					t.Fatalf("reader custom Decode did not fire (vacuous pass): x=%#v", x)
				}
			},
		},
	}
	evolutions := []struct {
		name         string
		writerFields func(x string) string
		readerFields func(x string) string
		val          func(xv any) map[string]any
	}{
		{
			"reorder",
			func(x string) string { return `{"name":"x","type":` + x + `},{"name":"y","type":"int"}` },
			func(x string) string { return `{"name":"y","type":"int"},{"name":"x","type":` + x + `}` },
			func(xv any) map[string]any { return map[string]any{"x": xv, "y": int32(7)} },
		},
		{
			"drop-writer-field",
			func(x string) string { return `{"name":"x","type":` + x + `},{"name":"drop","type":"int"}` },
			func(x string) string { return `{"name":"x","type":` + x + `}` },
			func(xv any) map[string]any { return map[string]any{"x": xv, "drop": int32(3)} },
		},
		{
			"add-reader-default",
			func(x string) string { return `{"name":"x","type":` + x + `}` },
			func(x string) string {
				return `{"name":"x","type":` + x + `},{"name":"added","type":"int","default":42}`
			},
			func(xv any) map[string]any { return map[string]any{"x": xv} },
		},
	}

	roundTrip := func(t *testing.T, writer, reader *avro.Schema, val map[string]any) (binOut, jsonOut map[string]any) {
		binWire, err := writer.Encode(val)
		if err != nil {
			t.Fatalf("writer Encode: %v", err)
		}
		jsonWire, err := writer.EncodeJSON(val)
		if err != nil {
			t.Fatalf("writer EncodeJSON: %v", err)
		}
		resolved, err := avro.Resolve(writer, reader)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		if _, err := resolved.Decode(binWire, &binOut); err != nil {
			t.Fatalf("resolved.Decode (binary oracle): %v", err)
		}
		if err := resolved.DecodeJSON(jsonWire, &jsonOut); err != nil {
			t.Fatalf("resolved.DecodeJSON failed where binary Decode succeeded: %v\n  binary oracle: %#v", err, binOut)
		}
		if !reflect.DeepEqual(binOut, jsonOut) {
			t.Errorf("resolved JSON decode != binary decode:\n  binary=%#v\n  json  =%#v", binOut, jsonOut)
		}
		return binOut, jsonOut
	}

	for _, lg := range logicals {
		for _, ev := range evolutions {
			t.Run(lg.name+"/decode-only/"+ev.name, func(t *testing.T) {
				writer := avro.MustParse(`{"type":"record","name":"R","fields":[`+ev.writerFields(lg.fieldType)+`]}`, avro.WithCustomType(lg.ct))
				reader := avro.MustParse(`{"type":"record","name":"R","fields":[`+ev.readerFields(lg.fieldType)+`]}`, avro.WithCustomType(lg.ct))
				binOut, _ := roundTrip(t, writer, reader, ev.val(lg.writerXVal))
				lg.assertDomain(t, binOut["x"])
			})
		}
	}

	// Control: an invertible custom (Decode + Encode) takes the identical
	// resolved-JSON path and must round-trip. We route that round-trip through
	// a custom-free writer view regardless of the custom's invertibility. The
	// writer encodes the domain value here, so the custom Encode fires.
	t.Run("invertible-control/timestamp-millis/reorder", func(t *testing.T) {
		ct := avro.CustomType{
			LogicalType: "timestamp-millis", AvroType: "long", GoType: tsType,
			Decode: func(v any, _ *avro.SchemaNode) (any, error) { return domainTS{ms: v.(int64)}, nil },
			Encode: func(v any, _ *avro.SchemaNode) (any, error) { return v.(domainTS).ms, nil },
		}
		ft := `{"type":"long","logicalType":"timestamp-millis"}`
		writer := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"x","type":`+ft+`},{"name":"y","type":"int"}]}`, avro.WithCustomType(ct))
		reader := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"y","type":"int"},{"name":"x","type":`+ft+`}]}`, avro.WithCustomType(ct))
		binOut, _ := roundTrip(t, writer, reader, map[string]any{"x": domainTS{ms: 1700000000000}, "y": int32(7)})
		if _, ok := binOut["x"].(domainTS); !ok {
			t.Fatalf("invertible custom Decode did not fire (vacuous pass): x=%#v", binOut["x"])
		}
	})
}

// A self-/mutually-recursive or forward-referenced named type whose subtree
// contains a logical a registered CustomType matches must Parse, and both
// wires must then agree. The cached-named-ref guard is for types inherited
// from a SchemaCache across Parses. A self-reference resolves mid-build,
// before the record's fields finish wiring their CTs, so hadCustomType is
// still false there. Gating on cachedNames is what keeps the guard from
// rejecting valid recursive schemas.
func TestMatrix_RecursiveCustomTypeParsesAndParity(t *testing.T) {
	ct := avro.CustomType{LogicalType: "timestamp-millis"}
	schemas := []struct{ name, schema string }{
		{"self-nested", `{"type":"record","name":"Node","fields":[
			{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},
			{"name":"next","type":["null","Node"]}]}`},
		{"self-wrapped", `{"type":"record","name":"Node","fields":[
			{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},
			{"name":"next","type":["null",{"type":"Node"}]}]}`},
		{"mutual", `{"type":"record","name":"A","fields":[
			{"name":"b","type":["null",{"type":"record","name":"B","fields":[
				{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},
				{"name":"a","type":["null","A"]}]}]}]}`},
		{"shared-multiref", `{"type":"record","name":"R","fields":[
			{"name":"x","type":{"type":"record","name":"Pair","fields":[
				{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}]}},
			{"name":"y","type":"Pair"}]}`},
	}
	for _, sc := range schemas {
		t.Run(sc.name, func(t *testing.T) {
			s, err := avro.Parse(sc.schema, ct)
			if err != nil {
				t.Fatalf("Parse(recursive + CustomType) failed: %v", err)
			}
			// Decode a minimal leaf both ways; suppression -> raw int64 on both.
			plain := avro.MustParse(sc.schema)
			// Build a minimal value: just the ts (and nil recursion / required
			// subrecords).
			var val any
			switch sc.name {
			case "self-nested", "self-wrapped":
				val = map[string]any{"ts": time.UnixMilli(1700000000000).UTC(), "next": nil}
			case "mutual":
				val = map[string]any{"b": nil}
			case "shared-multiref":
				val = map[string]any{
					"x": map[string]any{"ts": time.UnixMilli(1700000000000).UTC()},
					"y": map[string]any{"ts": time.UnixMilli(1700000001000).UTC()},
				}
			}
			bin, err := plain.Encode(val)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			jsn, err := plain.EncodeJSON(val)
			if err != nil {
				t.Fatalf("encodeJSON: %v", err)
			}
			var bv, jv any
			avrotest.MustDecode(t, s, bin, &bv)
			avrotest.MustDecodeJSON(t, s, jsn, &jv)
			if !reflect.DeepEqual(bv, jv) {
				t.Errorf("recursive custom binary<->JSON divergence:\n  binary=%#v\n  json  =%#v", bv, jv)
			}
		})
	}
}

// A forward-referenced named type carrying a CustomType must encode and decode
// identically to an in-order reference, and to the JSON path. Finalize fixups
// that wire a forward reference through the unwrapped namedType functions skip
// the custom wrap. The field then encodes raw on binary while JSON applies the
// custom. A named reference is position-independent in Avro, so its encoding
// cannot depend on definition order. We route the in-order site and all three
// fixup sites through one shared wrap.
func TestMatrix_ForwardRefCustomTypeBinaryJSONParity(t *testing.T) {
	// E used in field "a" (forward ref) *before* its definition in field "b".
	enumPos := []struct{ name, schema string }{
		{"union-branch", `{"type":"record","name":"R","fields":[
			{"name":"a","type":["null","E"]},
			{"name":"b","type":{"type":"enum","name":"E","symbols":["RED","GREEN","BLUE"]}}]}`},
		{"array-item", `{"type":"record","name":"R","fields":[
			{"name":"a","type":{"type":"array","items":"E"}},
			{"name":"b","type":{"type":"enum","name":"E","symbols":["RED","GREEN","BLUE"]}}]}`},
	}
	for _, p := range enumPos {
		t.Run(p.name+"/encode", func(t *testing.T) {
			// Encode-side: a reorder Encode makes raw-ordinal vs custom-ordinal
			// observable. GoType drives the custom on the Color value.
			ct := avro.CustomType{AvroType: "enum", GoType: reflect.TypeOf(testColor(0)),
				Encode: func(v any, sn *avro.SchemaNode) (any, error) {
					return sn.Symbols[len(sn.Symbols)-1-int(v.(testColor))], nil
				}}
			s := avro.MustParse(p.schema, ct)
			var aVal any = testColor(0)
			if p.name == "array-item" {
				aVal = []testColor{0}
			}
			val := map[string]any{"a": aVal, "b": testColor(0)}
			bin, eb := s.Encode(val)
			jsn, ej := s.EncodeJSON(val)
			if eb != nil || ej != nil {
				t.Fatalf("encode: bin=%v json=%v", eb, ej)
			}
			var bv, jv any
			avrotest.MustDecode(t, s, bin, &bv)
			avrotest.MustDecodeJSON(t, s, jsn, &jv)
			if !reflect.DeepEqual(bv, jv) {
				t.Errorf("forward-ref custom encode divergence:\n  binary=%#v\n  json  =%#v", bv, jv)
			}
		})
		t.Run(p.name+"/decode", func(t *testing.T) {
			ct := avro.CustomType{AvroType: "enum",
				Decode: func(v any, _ *avro.SchemaNode) (any, error) { return "DEC:" + fmt.Sprintf("%v", v), nil }}
			plain := avro.MustParse(p.schema)
			var aVal any = "RED"
			if p.name == "array-item" {
				aVal = []any{"RED"}
			}
			val := map[string]any{"a": aVal, "b": "GREEN"}
			bin, _ := plain.Encode(val)
			jsn, _ := plain.EncodeJSON(val)
			s := avro.MustParse(p.schema, ct)
			var bv, jv any
			avrotest.MustDecode(t, s, bin, &bv)
			avrotest.MustDecodeJSON(t, s, jsn, &jv)
			if !reflect.DeepEqual(bv, jv) {
				t.Errorf("forward-ref custom decode divergence:\n  binary=%#v\n  json  =%#v", bv, jv)
			}
		})
	}
}

type testColor int32

// A no-Decode CustomType that suppresses a logical produces the raw
// Avro-native value, and the wires must agree on it. Take a fixed-size
// byte-array target: binary's raw deserFixed copies into [N]byte. A JSON path
// that boxes into any instead has setCustomResult reject the []byte to [N]byte
// assignment. Take uuid-on-string into [16]byte: binary's raw deserString has
// no array arm and errors, where JSON applying the uuid arm succeeds. We route
// suppression through the same raw decode arms binary uses.
func TestMatrix_CustomSuppressionByteArrayTargetParity(t *testing.T) {
	cases := []struct {
		name    string
		schema  string
		logical string
		wantErr bool // true: both wire formats must error ([N]byte can't hold a raw string)
	}{
		{"fixed-uuid-16", `{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`, "uuid", false},
		{"fixed-duration-12", `{"type":"fixed","name":"D","size":12,"logicalType":"duration"}`, "duration", false},
		{"fixed-decimal-8", `{"type":"fixed","name":"DF","size":8,"logicalType":"decimal","precision":10,"scale":2}`, "decimal", false},
		{"string-uuid-16", `{"type":"string","logicalType":"uuid"}`, "uuid", true},
	}
	u := "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plain := avro.MustParse(c.schema)
			var encVal any = u
			if c.name == "fixed-duration-12" {
				encVal = avro.Duration{Months: 1, Days: 2, Milliseconds: 3}
			} else if c.name == "fixed-decimal-8" {
				encVal = big.NewRat(33, 100)
			}
			bin, _ := plain.Encode(encVal)
			jsn, _ := plain.EncodeJSON(encVal)
			cs := avro.MustParse(c.schema, avro.CustomType{LogicalType: c.logical})

			szArr := func() reflect.Value {
				switch c.name {
				case "fixed-duration-12":
					return reflect.New(reflect.ArrayOf(12, reflect.TypeOf(byte(0))))
				case "fixed-decimal-8":
					return reflect.New(reflect.ArrayOf(8, reflect.TypeOf(byte(0))))
				default:
					return reflect.New(reflect.ArrayOf(16, reflect.TypeOf(byte(0))))
				}
			}
			bp, jp := szArr(), szArr()
			_, eb := cs.Decode(bin, bp.Interface())
			ej := cs.DecodeJSON(jsn, jp.Interface())
			if (eb == nil) != (ej == nil) {
				t.Fatalf("[N]byte target parity: binary err=%v ; json err=%v", eb, ej)
			}
			if c.wantErr && eb == nil {
				t.Errorf("expected both to error ([N]byte can't hold a raw string), got success")
			}
			if !c.wantErr {
				if eb != nil {
					t.Fatalf("expected success, got binary err=%v", eb)
				}
				if !reflect.DeepEqual(bp.Elem().Interface(), jp.Elem().Interface()) {
					t.Errorf("[N]byte value divergence: binary=%v json=%v", bp.Elem(), jp.Elem())
				}
			}
		})
	}
}

// SchemaCache: a cached named type and the Parse referencing it must agree on
// whether a matching CustomType is registered. The custom's effect is baked
// onto the shared cached node. A mismatch then silently changes what the
// referencing Schema decodes and encodes on both wires. We reject both
// directions with a clear error, and resolve a consistent registration. A
// current-Parse self- or forward reference is exempt, its CustomTypes being in
// scope for its single definition.
func TestMatrix_SchemaCacheCustomBoundaryGuard(t *testing.T) {
	tsCustom := avro.CustomType{LogicalType: "timestamp-millis"}
	rSchema := `{"type":"record","name":"R","fields":[{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}]}`
	outer := `{"type":"record","name":"Outer","fields":[{"name":"r","type":"R"}]}`

	t.Run("reverse-custom-cached-nocustom-ref-rejects", func(t *testing.T) {
		var cache avro.SchemaCache
		if _, err := cache.Parse(rSchema, tsCustom); err != nil {
			t.Fatalf("cache R with custom: %v", err)
		}
		_, err := cache.Parse(outer) // no custom -> must reject (would inherit suppression)
		if err == nil {
			t.Fatal("expected error referencing custom-built cached type without the CustomType")
		}
		if !strings.Contains(err.Error(), "R") || !strings.Contains(err.Error(), "CustomType") {
			t.Errorf("error should name the cached type and CustomType: %v", err)
		}
	})
	t.Run("forward-clean-cached-custom-ref-rejects", func(t *testing.T) {
		var cache avro.SchemaCache
		if _, err := cache.Parse(rSchema); err != nil { // no custom
			t.Fatalf("cache R clean: %v", err)
		}
		_, err := cache.Parse(outer, tsCustom) // custom -> must reject (would drop the custom)
		if err == nil {
			t.Fatal("expected error: custom would match a clean cached type's subtree")
		}
	})
	t.Run("consistent-custom-both-resolves", func(t *testing.T) {
		var cache avro.SchemaCache
		if _, err := cache.Parse(rSchema, tsCustom); err != nil {
			t.Fatalf("cache R with custom: %v", err)
		}
		if _, err := cache.Parse(outer, tsCustom); err != nil { // same custom -> OK
			t.Errorf("consistent custom reference should resolve, got: %v", err)
		}
	})
	t.Run("clean-both-resolves", func(t *testing.T) {
		var cache avro.SchemaCache
		if _, err := cache.Parse(rSchema); err != nil {
			t.Fatalf("cache R clean: %v", err)
		}
		if _, err := cache.Parse(outer); err != nil { // both clean -> OK
			t.Errorf("clean reference should resolve, got: %v", err)
		}
	})
}

// A self-referential schema cached without a CustomType and re-parsed (the
// same string) with a matching one must Parse and apply the custom. The
// re-parse re-defines the name, so its self-reference resolves to this parse's
// fresh node. rejectCachedRefIfCustomTypeWouldMatch must *not* fire. The guard
// keys "defined this parse" on definedSet, shared by reference across the
// nested builders. Keying on cachedNames instead false-rejects, and does so
// depending on field order. The genuine cross-parse hazard must still reject,
// asserted below.
func TestMatrix_SchemaCacheSelfRefReParseWithCustom(t *testing.T) {
	const ms = int64(1700000000000)
	// A value-transforming decoder: under suppression it receives the raw
	// int64 millis and returns a distinctive marker. "custom applied" is then
	// observable as a string in the decoded map. A no-op decoder would be
	// indistinguishable from the built-in time.Time decode, and a
	// callback-firing claim needs a value-transforming callback.
	customMS := func() avro.CustomType {
		return avro.CustomType{
			LogicalType: "timestamp-millis",
			Decode: func(v any, _ *avro.SchemaNode) (any, error) {
				return fmt.Sprintf("MS=%v", v), nil
			},
		}
	}
	wantMarker := fmt.Sprintf("MS=%v", ms)

	at := func(t *testing.T, m any, path ...string) any {
		t.Helper()
		cur := m
		for _, k := range path {
			mm, ok := cur.(map[string]any)
			if !ok {
				t.Fatalf("navigating %v: %T is not a map", path, cur)
			}
			cur = mm[k]
		}
		return cur
	}

	cases := []struct {
		name   string
		schema string
		value  any
		tPath  []string
	}{
		{
			// custom-matched field *before* the self-reference: the
			// partially-built node already shows the match, the shape a
			// name-keyed guard rejects.
			"flat-t-before-selfref",
			`{"type":"record","name":"Node","fields":[
				{"name":"t","type":{"type":"long","logicalType":"timestamp-millis"}},
				{"name":"next","type":["null","Node"]}]}`,
			map[string]any{"t": time.UnixMilli(ms).UTC(), "next": nil},
			[]string{"t"},
		},
		{
			// self-reference *before* the custom-matched field: the node's
			// fields slice is empty at resolve time. We pin that the accept is
			// order-independent.
			"flat-selfref-before-t",
			`{"type":"record","name":"Node","fields":[
				{"name":"next","type":["null","Node"]},
				{"name":"t","type":{"type":"long","logicalType":"timestamp-millis"}}]}`,
			map[string]any{"t": time.UnixMilli(ms).UTC(), "next": nil},
			[]string{"t"},
		},
		{
			// Node nested two record levels deep: the self-reference resolves
			// inside a nested builder. Only the reference-shared definedSet, not
			// the unnest-merged definedNamed, makes the this-parse definition
			// visible there.
			"depth-nested-selfref",
			`{"type":"record","name":"L1","fields":[
				{"name":"a","type":{"type":"record","name":"L2","fields":[
					{"name":"b","type":{"type":"record","name":"Node","fields":[
						{"name":"t","type":{"type":"long","logicalType":"timestamp-millis"}},
						{"name":"next","type":["null","Node"]}]}}]}}]}`,
			map[string]any{"a": map[string]any{"b": map[string]any{"t": time.UnixMilli(ms).UTC(), "next": nil}}},
			[]string{"a", "b", "t"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Control: bare Parse with the custom succeeds. The cache must not
			// change the accept/reject outcome of an identical (schema, opts).
			bare, err := avro.Parse(c.schema, avro.WithCustomType(customMS()))
			if err != nil {
				t.Fatalf("CONTROL: bare Parse(schema, custom) should succeed: %v", err)
			}

			var cache avro.SchemaCache
			if _, err := cache.Parse(c.schema); err != nil { // parse 1: no custom
				t.Fatalf("cache parse 1 (no custom): %v", err)
			}
			s, err := cache.Parse(c.schema, avro.WithCustomType(customMS())) // parse 2: same string, matching custom
			if err != nil {
				t.Fatalf("cache parse 2 falsely rejected a valid self-ref re-parse: %v", err)
			}

			// Wire produced by the plain (no-custom) schema: a raw long for the
			// timestamp-millis leaf.
			plain := avro.MustParse(c.schema)
			wire, err := plain.Encode(c.value)
			if err != nil {
				t.Fatalf("plain encode: %v", err)
			}

			// The cache-reparsed schema must decode with the custom applied
			// (marker string), identically to the bare-parsed schema. That
			// proves the custom is wired, not merely that Parse returned nil.
			var gotCache, gotBare any
			if _, err := s.Decode(wire, &gotCache); err != nil {
				t.Fatalf("cache schema decode: %v", err)
			}
			if _, err := bare.Decode(wire, &gotBare); err != nil {
				t.Fatalf("bare schema decode: %v", err)
			}
			if leaf := at(t, gotCache, c.tPath...); leaf != wantMarker {
				t.Errorf("custom NOT applied through cache re-parse: t leaf = %#v, want %q", leaf, wantMarker)
			}
			if cv, bv := at(t, gotCache, c.tPath...), at(t, gotBare, c.tPath...); cv != bv {
				t.Errorf("cache re-parse not equivalent to bare: cache=%#v bare=%#v", cv, bv)
			}
		})
	}

	// Reverse direction: cached with a custom, then re-parsed without one,
	// matches a bare no-custom Parse. It accepts, and the custom is *not*
	// applied: the leaf decodes to the built-in time.Time, not the marker.
	t.Run("reverse-custom-then-clean-matches-bare", func(t *testing.T) {
		schema := cases[0].schema
		var cache avro.SchemaCache
		if _, err := cache.Parse(schema, avro.WithCustomType(customMS())); err != nil {
			t.Fatalf("cache parse 1 (custom): %v", err)
		}
		s, err := cache.Parse(schema) // re-parse, no custom
		if err != nil {
			t.Fatalf("cache parse 2 (no custom) should succeed: %v", err)
		}
		plain := avro.MustParse(schema)
		wire, err := plain.Encode(map[string]any{"t": time.UnixMilli(ms).UTC(), "next": nil})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		var got any
		avrotest.MustDecode(t, s, wire, &got)
		// No custom -> built-in timestamp-millis -> time.Time, *not* the marker.
		if leaf := at(t, got, "t"); leaf == wantMarker {
			t.Errorf("custom wrongly applied on a no-custom re-parse: leaf = %#v", leaf)
		} else if _, ok := leaf.(time.Time); !ok {
			t.Errorf("no-custom leaf should be time.Time, got %T (%#v)", leaf, leaf)
		}
	})

	// Safety boundary (must keep rejecting): a genuine cross-parse reference
	// is the real stale-node hazard. That is a different schema referencing
	// the clean cached Node by name with a matching custom. The resolved node
	// is the cached clone, absent from definedSet. The guard must still fire;
	// TestMatrix_SchemaCacheCustomBoundaryGuard pins it too.
	t.Run("safety-boundary-cross-parse-reference-still-rejects", func(t *testing.T) {
		var cache avro.SchemaCache
		nodeDef := `{"type":"record","name":"Node","fields":[
			{"name":"t","type":{"type":"long","logicalType":"timestamp-millis"}}]}`
		if _, err := cache.Parse(nodeDef); err != nil { // cache Node clean
			t.Fatalf("cache Node clean: %v", err)
		}
		outer := `{"type":"record","name":"Outer","fields":[{"name":"n","type":"Node"}]}`
		if _, err := cache.Parse(outer, avro.WithCustomType(customMS())); err == nil {
			t.Fatal("expected rejection: a cross-parse REFERENCE to a clean cached type with a matching custom must still reject (stale-node hazard)")
		}
	})
}

// A CustomType whose Decode returns a pointer, decoded into a pointer target,
// must succeed identically on both wires. setCustomResult walks pointer
// indirections to find the assignable level. The JSON decoder chain must hand
// it the same un-indirected target binary does. Pre-dereferencing with
// indirectAlloc peels a *wrap result's outer pointer out of a **wrap target.
func TestRegression_CustomDecodePointerResultPointerTargetParity(t *testing.T) {
	type wrap struct{ V string }
	ct := avro.CustomType{
		LogicalType: "wrapped", AvroType: "string", GoType: reflect.TypeOf((*wrap)(nil)),
		Encode: func(v any, _ *avro.SchemaNode) (any, error) { return v.(*wrap).V, nil },
		Decode: func(v any, _ *avro.SchemaNode) (any, error) { return &wrap{V: v.(string)}, nil },
	}
	s := avro.MustParse(`{"type":"string","logicalType":"wrapped"}`, ct)
	bin := avrotest.MustEncode(t, s, &wrap{V: "hi"})
	jsn := avrotest.MustEncodeJSON(t, s, &wrap{V: "hi"})

	var tb, tj *wrap
	_, eb := s.Decode(bin, &tb)
	ej := s.DecodeJSON(jsn, &tj)
	if (eb == nil) != (ej == nil) {
		t.Fatalf("pointer-result/pointer-target parity broken: binary err=%v ; json err=%v", eb, ej)
	}
	if eb != nil {
		t.Fatalf("both should succeed (setCustomResult walks pointers), got binary err=%v json err=%v", eb, ej)
	}
	if tb == nil || tj == nil || *tb != *tj {
		t.Errorf("pointer-result divergence: binary=%v json=%v", tb, tj)
	}
	if tj.V != "hi" {
		t.Errorf("json pointer Decode result lost its value: %+v", tj)
	}
}

// A no-Decode CustomType that suppresses a logical must yield the raw
// Avro-native value on both wires for a scalar typed target, as binary's raw
// deser* already does. JSON per-kind decoders that honor suppression only in
// their decode-into-any branch apply the transform unconditionally for a typed
// target. A suppressed decimal into *string then reads "123.45" where binary
// hands back the raw payload. time-millis into time.Duration silently produces
// a different value. We thread the flag into assignBytes/decodeInt/decodeLong.
func TestMatrix_CustomSuppressionScalarTargetParity(t *testing.T) {
	strT := reflect.TypeOf("")
	ratT := reflect.TypeOf((*big.Rat)(nil))
	durT := reflect.TypeOf(avro.Duration{})
	timeT := reflect.TypeOf(time.Time{})
	godurT := reflect.TypeOf(time.Duration(0))
	cases := []struct {
		name    string
		schema  string
		logical string
		encVal  any
		target  reflect.Type // reflect.New(target) is the decode target
		wantRaw any          // when non-nil, both must succeed and DeepEqual this raw value
		wantErr bool         // both must reject (enriched target invalid once the arm is suppressed)
	}{
		// The headline case: 123.45 at scale 2 is unscaled 12345 = 0x3039, whose
		// two raw bytes are '0','9'. The suppressed string target must read "09",
		// *not* the logical-formatted "123.45".
		{"decimal-bytes/string", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, "decimal", big.NewRat(12345, 100), strT, "09", false},
		{"decimal-bytes/bigRat", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, "decimal", big.NewRat(12345, 100), ratT, nil, true},
		{"decimal-fixed/string", `{"type":"fixed","name":"DF","size":8,"logicalType":"decimal","precision":10,"scale":2}`, "decimal", big.NewRat(12345, 100), strT, nil, false},
		{"big-decimal/string", `{"type":"bytes","logicalType":"big-decimal"}`, "big-decimal", big.NewRat(12345, 100), strT, nil, false},
		{"big-decimal/bigRat", `{"type":"bytes","logicalType":"big-decimal"}`, "big-decimal", big.NewRat(12345, 100), ratT, nil, true},
		{"duration-fixed/string", `{"type":"fixed","name":"DUR","size":12,"logicalType":"duration"}`, "duration", avro.Duration{Months: 1, Days: 2, Milliseconds: 3}, strT, nil, false},
		{"duration-fixed/duration", `{"type":"fixed","name":"DUR2","size":12,"logicalType":"duration"}`, "duration", avro.Duration{Months: 1, Days: 2, Milliseconds: 3}, durT, nil, true},
		// Non-standard logical placements resurrected by a CustomType: the
		// logical sits on a kind it is not spec-valid for (uuid/duration are
		// fixed-only, big-decimal bytes-only), so validateLogical soft-drops
		// it. The CustomType restores it and suppresses the codec. The contract
		// is then the raw Avro-native bytes on both wires. Ungated, the JSON
		// typed-decode path applies the transform for the wrong kind while
		// binary returns raw.
		{"uuid-on-bytes/string", `{"type":"bytes","logicalType":"uuid"}`, "uuid", []byte("0123456789abcdef"), strT, "0123456789abcdef", false},
		{"duration-on-bytes/duration", `{"type":"bytes","logicalType":"duration"}`, "duration", []byte("aaaabbbbcccc"), durT, nil, true},
		{"big-decimal-on-fixed/bigRat", `{"type":"fixed","name":"FBD","size":4,"logicalType":"big-decimal"}`, "big-decimal", []byte{0x04, 0x30, 0x39, 0x04}, ratT, nil, true},
		// int/long logicals: the suppressed raw int must *not* be transformed.
		{"date/time", `{"type":"int","logicalType":"date"}`, "date", time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), timeT, nil, true},
		{"date/string", `{"type":"int","logicalType":"date"}`, "date", time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), strT, nil, true},
		// 3h as time-millis is 10_800_000 on the wire; raw into a time.Duration
		// (ns) is 10.8ms, *not* the logical 3h: the silent value-divergence case.
		{"time-millis/duration", `{"type":"int","logicalType":"time-millis"}`, "time-millis", 3 * time.Hour, godurT, time.Duration(10800000), false},
		{"timestamp-millis/time", `{"type":"long","logicalType":"timestamp-millis"}`, "timestamp-millis", time.UnixMilli(1700000000000).UTC(), timeT, nil, true},
		{"timestamp-millis/string", `{"type":"long","logicalType":"timestamp-millis"}`, "timestamp-millis", time.UnixMilli(1700000000000).UTC(), strT, nil, true},
		// 3h as time-micros is 10_800_000_000; raw into time.Duration is 10.8s.
		{"time-micros/duration", `{"type":"long","logicalType":"time-micros"}`, "time-micros", 3 * time.Hour, godurT, time.Duration(10800000000), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plain := avro.MustParse(c.schema)
			bin := avrotest.MustEncode(t, plain, c.encVal)
			jsn := avrotest.MustEncodeJSON(t, plain, c.encVal)
			cs := avro.MustParse(c.schema, avro.CustomType{LogicalType: c.logical})
			bp, jp := reflect.New(c.target), reflect.New(c.target)
			_, eb := cs.Decode(bin, bp.Interface())
			ej := cs.DecodeJSON(jsn, jp.Interface())
			if (eb == nil) != (ej == nil) {
				t.Fatalf("binary<->JSON parity broken: binary err=%v ; json err=%v", eb, ej)
			}
			if c.wantErr {
				if eb == nil {
					t.Errorf("expected both to reject the enriched target under suppression, got success (binary=%v)", bp.Elem())
				}
				return
			}
			if eb != nil {
				t.Fatalf("expected success, got binary err=%v json err=%v", eb, ej)
			}
			bv, jv := bp.Elem().Interface(), jp.Elem().Interface()
			if !reflect.DeepEqual(bv, jv) {
				t.Errorf("suppressed scalar-target divergence: binary=%#v json=%#v", bv, jv)
			}
			if c.wantRaw != nil && !reflect.DeepEqual(bv, c.wantRaw) {
				t.Errorf("expected RAW value %#v (logical arm suppressed), got %#v — JSON applied the logical transform", c.wantRaw, bv)
			}
		})
	}
}

// Encode-side complement of the scalar-target parity net. A CustomType
// registered for a built-in logical name resurrects that logical when
// validateLogical soft-dropped it for sitting on a kind it is not spec-valid
// for. The resurrection suppresses the codec, so the contract is the raw value
// on every path, the binary encoder included. logicalSer is keyed only on the
// logical name. Without a kind gate it writes the logical form, disagreeing
// with the raw value JSON encodes. Where the wire shapes differ, that produces
// a wire this schema's own decoder cannot read.
func TestMatrix_CustomSuppressionWrongKindLogicalEncodeParity(t *testing.T) {
	uuid16 := [16]byte{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}
	tm := time.Date(2023, 11, 14, 22, 13, 20, 0, time.UTC)
	// Every entry in the logical-serializer table, placed on a kind it is not
	// spec-valid for. uuid (string/fixed) on bytes; the int/long time logicals
	// on string. The encode value is a Go type the base (suppressed) serializer
	// accepts: a [16]byte for bytes, a time.Time (TextMarshaler) for string.
	cases := []struct {
		name, schema, logical string
		encVal                any
	}{
		{"uuid-on-bytes", `{"type":"bytes","logicalType":"uuid"}`, "uuid", uuid16},
		{"date-on-string", `{"type":"string","logicalType":"date"}`, "date", tm},
		{"time-millis-on-string", `{"type":"string","logicalType":"time-millis"}`, "time-millis", tm},
		{"time-micros-on-string", `{"type":"string","logicalType":"time-micros"}`, "time-micros", tm},
		{"timestamp-millis-on-string", `{"type":"string","logicalType":"timestamp-millis"}`, "timestamp-millis", tm},
		{"timestamp-micros-on-string", `{"type":"string","logicalType":"timestamp-micros"}`, "timestamp-micros", tm},
		{"timestamp-nanos-on-string", `{"type":"string","logicalType":"timestamp-nanos"}`, "timestamp-nanos", tm},
		{"local-timestamp-millis-on-string", `{"type":"string","logicalType":"local-timestamp-millis"}`, "local-timestamp-millis", tm},
		{"local-timestamp-micros-on-string", `{"type":"string","logicalType":"local-timestamp-micros"}`, "local-timestamp-micros", tm},
		{"local-timestamp-nanos-on-string", `{"type":"string","logicalType":"local-timestamp-nanos"}`, "local-timestamp-nanos", tm},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cs := avro.MustParse(c.schema, avro.CustomType{LogicalType: c.logical})
			bin, eb := cs.Encode(c.encVal)
			if eb != nil {
				t.Fatalf("binary Encode: %v", eb)
			}
			jsn, ej := cs.EncodeJSON(c.encVal)
			if ej != nil {
				t.Fatalf("EncodeJSON: %v", ej)
			}
			// The binary wire must be readable by this schema's own decoder. A
			// suppressed wrong-kind logical encodes the base kind, so a bytes/
			// string wire is length-prefixed, never a bare varint.
			var bv, jv any
			if _, err := cs.Decode(bin, &bv); err != nil {
				t.Fatalf("binary Encode produced a wire its own Decode rejects: %v", err)
			}
			avrotest.MustDecodeJSON(t, cs, jsn, &jv)
			// Both formats must encode the same raw Avro-native value.
			if !reflect.DeepEqual(bv, jv) {
				t.Errorf("binary vs JSON encode diverge under wrong-kind suppression: binary=%#v json=%#v", bv, jv)
			}
		})
	}
}

// The kind gate that suppresses a wrong-kind logical's binary serializer must
// *not* regress spec-valid placements. There the logical serializer is a
// genuine superset of the base serializer: it alone accepts time.Time or a UUID
// string. Encoding a time.Time against long+timestamp-millis (or a UUID string
// against string+uuid) succeeds only when the logical serializer stays applied.
// The base long/string serializer rejects a time.Time outright.
func TestMatrix_CustomSuppressionSpecValidLogicalStillApplied(t *testing.T) {
	tm := time.Date(2023, 11, 14, 22, 13, 20, 0, time.UTC)
	cases := []struct {
		name, schema, logical string
		encVal                any
	}{
		{"uuid-on-string", `{"type":"string","logicalType":"uuid"}`, "uuid", "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		{"date-on-int", `{"type":"int","logicalType":"date"}`, "date", tm},
		{"time-millis-on-int", `{"type":"int","logicalType":"time-millis"}`, "time-millis", 3 * time.Hour},
		{"timestamp-millis-on-long", `{"type":"long","logicalType":"timestamp-millis"}`, "timestamp-millis", tm},
		{"timestamp-micros-on-long", `{"type":"long","logicalType":"timestamp-micros"}`, "timestamp-micros", tm},
		// The spec-valid fixed sizes (uuid=16, duration=12) are the boundary-1
		// controls for the wrong-size fixed gate below. The logical serializer
		// must stay applied here: it alone accepts a UUID string or avro.Duration,
		// and the base serSize rejects them. Only the wrong-size placements drop
		// it.
		{"uuid-on-fixed16", `{"type":"fixed","name":"F16","size":16,"logicalType":"uuid"}`, "uuid", "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		{"duration-on-fixed12", `{"type":"fixed","name":"D12","size":12,"logicalType":"duration"}`, "duration", avro.Duration{Months: 1, Days: 2, Milliseconds: 3}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cs := avro.MustParse(c.schema, avro.CustomType{LogicalType: c.logical})
			bin, eb := cs.Encode(c.encVal)
			if eb != nil {
				t.Fatalf("spec-valid logical encode regressed (logical serializer suppressed): %v", eb)
			}
			jsn, ej := cs.EncodeJSON(c.encVal)
			if ej != nil {
				t.Fatalf("spec-valid logical EncodeJSON: %v", ej)
			}
			// The encode must succeed and round-trip through this schema's own
			// (suppressed-to-raw) decoders identically on both wires.
			var bv, jv any
			avrotest.MustDecode(t, cs, bin, &bv)
			avrotest.MustDecodeJSON(t, cs, jsn, &jv)
			if !reflect.DeepEqual(bv, jv) {
				t.Errorf("spec-valid logical binary vs JSON diverge: binary=%#v json=%#v", bv, jv)
			}
		})
	}
}

// Wrong-size complement of the wrong-kind encode parity net: uuid is
// fixed-valid only at size 16 and duration only at 12
// (logicalUnderlyingAccept, the same predicate validateLogical soft-drops
// with). A no-Encode CustomType resurrects the logical, so the contract is the
// raw value on every path. serFixedUUIDReflect always emits 16 bytes and
// serDuration 12, regardless of the declared size. Without the gate both write
// the logical form into a differently-sized fixed, a wire this schema's own
// decoders reject.
func TestRegression_CustomSuppressionWrongSizeFixedLogicalEncodeParity(t *testing.T) {
	uuid16 := [16]byte{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}
	dur := avro.Duration{Months: 1, Days: 2, Milliseconds: 3}
	cases := []struct {
		name, schema, logical string
		size                  int
		// logical is the Go value the size-blind logical serializer accepts
		// (a 16-byte UUID array or an avro.Duration). At the wrong size that
		// input produces a self-incompatible wire. The custom schema must
		// treat it identically to the plain fixed, both rejecting it since it
		// is not `size` raw bytes. raw[size] is the legitimate raw fixed value
		// both must accept identically.
		logical2 any
	}{
		{"uuid-on-fixed20", `{"type":"fixed","name":"F","size":20,"logicalType":"uuid"}`, "uuid", 20, uuid16},
		{"duration-on-fixed16", `{"type":"fixed","name":"D","size":16,"logicalType":"duration"}`, "duration", 16, dur},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			raw := make([]byte, c.size)
			for i := range raw {
				raw[i] = byte(i + 1)
			}
			cs := avro.MustParse(c.schema, avro.CustomType{LogicalType: c.logical})
			plain := avro.MustParse(c.schema) // no custom: logical soft-drops to plain fixed

			// A passive (no-Encode) custom registered for the logical name must
			// make the wrong-size fixed behave exactly like the plain fixed of
			// that size for every input. The raw[size] input pins the
			// legitimate round trip. The logical-shaped input pins that the
			// size-blind logical serializer is *not* applied: applying it
			// writes a wire the schema's own decoder rejects on both wires.
			for _, in := range []any{any(raw), c.logical2} {
				cbin, ceb := cs.Encode(in)
				pbin, peb := plain.Encode(in)
				if (ceb == nil) != (peb == nil) {
					t.Errorf("input %T: custom binary err=%v but plain binary err=%v — passive custom changed acceptance (wrong-size logical applied)", in, ceb, peb)
				}
				if ceb == nil && peb == nil {
					if !bytes.Equal(cbin, pbin) {
						t.Errorf("input %T: custom binary wire != plain wire — wrong-size logical serializer applied", in)
					}
					var bv any
					if _, err := cs.Decode(cbin, &bv); err != nil {
						t.Errorf("input %T: custom binary wire (len=%d) not readable by its own decoder: %v", in, len(cbin), err)
					}
				}

				cjsn, cej := cs.EncodeJSON(in)
				pjsn, pej := plain.EncodeJSON(in)
				if (cej == nil) != (pej == nil) {
					t.Errorf("input %T: custom JSON err=%v but plain JSON err=%v — passive custom changed acceptance (wrong-size logical applied)", in, cej, pej)
				}
				if cej == nil && pej == nil {
					if !bytes.Equal(cjsn, pjsn) {
						t.Errorf("input %T: custom JSON %q != plain JSON %q — wrong-size logical serializer applied", in, cjsn, pjsn)
					}
					var jv any
					if err := cs.DecodeJSON(cjsn, &jv); err != nil {
						t.Errorf("input %T: custom JSON not readable by its own decoder: %v", in, err)
					}
				}
			}
		})
	}
}

// We preserve an invalid-UTF-8 Avro string verbatim on the binary wire, and
// coerce each invalid byte to U+FFFD on the JSON wire. The divergence is
// documented and intentional. An RFC 8259 JSON string cannot carry a raw
// non-UTF-8 byte, so byte-faithful parity is impossible. Java produces
// byte-identical output on both wires (verified live by
// TestDifferentialJavaInvalidUTF8). Do not "fix" either side toward the other:
// making binary lossy corrupts the faithful wire, and making EncodeJSON reject
// diverges from Java's lenient coercion.
func TestRegression_InvalidUTF8StringBinaryVerbatimJSONCoercion(t *testing.T) {
	s := avro.MustParse(`"string"`)
	in := "A\xffB"

	bin, err := s.Encode(in)
	if err != nil {
		t.Fatalf("binary Encode must accept invalid UTF-8 (verbatim wire): %v", err)
	}
	// varint length 3, then the raw bytes 'A' 0xff 'B': byte-faithful.
	if want := "\x06A\xffB"; string(bin) != want {
		t.Errorf("binary wire = % x, want % x (verbatim bytes)", bin, want)
	}
	var binBack string
	avrotest.MustDecode(t, s, bin, &binBack)
	if binBack != in {
		t.Errorf("binary round-trip = %q, want verbatim %q", binBack, in)
	}

	jsn, err := s.EncodeJSON(in)
	if err != nil {
		t.Fatalf("EncodeJSON must accept invalid UTF-8 (U+FFFD coercion, Java parity): %v", err)
	}
	// `"A<efbfbd>B"` holds the invalid byte coerced to the replacement char,
	// byte-identical to Java's JsonEncoder output for the same datum.
	if want := "\"A�B\""; string(jsn) != want {
		t.Errorf("JSON wire = % x, want % x (U+FFFD coercion)", jsn, want)
	}
	var jsonBack string
	avrotest.MustDecodeJSON(t, s, jsn, &jsonBack)
	if jsonBack != "A�B" {
		t.Errorf("JSON round-trip = %q, want coerced %q", jsonBack, "A�B")
	}

	// Map keys take the same JSON path (appendJSONString); pin one level deep.
	ms := avro.MustParse(`{"type":"map","values":"int"}`)
	mj, err := ms.EncodeJSON(map[string]int32{"\xffA": 1})
	if err != nil {
		t.Fatalf("EncodeJSON map with invalid-UTF-8 key: %v", err)
	}
	if want := "{\"�A\":1}"; string(mj) != want {
		t.Errorf("map-key JSON = % x, want % x", mj, want)
	}
	mb, err := ms.Encode(map[string]int32{"\xffA": 1})
	if err != nil {
		t.Fatalf("binary Encode map with invalid-UTF-8 key: %v", err)
	}
	var mBack map[string]int32
	if _, err := ms.Decode(mb, &mBack); err != nil {
		t.Fatalf("binary Decode map: %v", err)
	}
	if _, ok := mBack["\xffA"]; !ok {
		t.Errorf("binary map round-trip lost the verbatim key: %v", mBack)
	}
}

type untaggedPinBig int64

// A bare (untagged) JSON union value cannot name its branch, so DecodeJSON
// commits to the first declaration-order branch of the matching token class.
// That is documented and intentional. The untagged wire for int32(7)-via-int
// and int64(7)-via-long is the identical byte `7`, so the writer's branch is
// information-theoretically unrecoverable. Do not "fix" it with branch
// guessing. We spell the consequences out below so the policy stays visible
// rather than reading as an unexplained asymmetry.
func TestRegression_UntaggedUnionBranchClassFirstMatch(t *testing.T) {
	t.Run("decode-into-any-first-class-branch", func(t *testing.T) {
		sc := avro.MustParse(`["long","int"]`)
		bw := avrotest.MustEncode(t, sc, int32(7))     // writer chose the "int" branch (index 1)
		jw := avrotest.MustEncodeJSON(t, sc, int32(7)) // bare wire: `7`, no branch on the wire
		var bo, jo any
		avrotest.MustDecode(t, sc, bw, &bo)
		avrotest.MustDecodeJSON(t, sc, jw, &jo)
		if _, ok := bo.(int32); !ok {
			t.Errorf("binary decode-into-any = %T, want int32 (wire index recovers the writer's branch)", bo)
		}
		if _, ok := jo.(int64); !ok {
			t.Errorf("untagged JSON decode-into-any = %T, want int64 (documented first-class-branch commit)", jo)
		}

		// TaggedUnions names the branch: decode recovers it (in the documented
		// {branch: value} envelope form for an any target).
		tw := avrotest.MustEncodeJSON(t, sc, int32(7), avro.TaggedUnions())
		var to any
		avrotest.MustDecodeJSON(t, sc, tw, &to, avro.TaggedUnions())
		env, ok := to.(map[string]any)
		if !ok || len(env) != 1 {
			t.Fatalf("tagged decode-into-any = %T %v, want one-key envelope", to, to)
		}
		if v, ok := env["int"]; !ok {
			t.Errorf("tagged envelope key = %v, want \"int\" (writer's branch recovered)", env)
		} else if _, ok := v.(int32); !ok {
			t.Errorf("tagged envelope value = %T, want int32", v)
		}
	})

	t.Run("custom-on-non-first-branch-skipped", func(t *testing.T) {
		ct := avro.NewCustomType[untaggedPinBig, int64]("upb",
			func(m untaggedPinBig, _ *avro.SchemaNode) (int64, error) { return int64(m), nil },
			func(v int64, _ *avro.SchemaNode) (untaggedPinBig, error) { return untaggedPinBig(v * 10), nil })
		sc := avro.MustParse(`["int",{"type":"long","logicalType":"upb"}]`, ct)
		bw := avrotest.MustEncode(t, sc, untaggedPinBig(7))     // long+custom branch on the wire
		jw := avrotest.MustEncodeJSON(t, sc, untaggedPinBig(7)) // bare `7`
		var bc, jc untaggedPinBig
		avrotest.MustDecode(t, sc, bw, &bc)
		avrotest.MustDecodeJSON(t, sc, jw, &jc)
		if bc != 70 {
			t.Errorf("binary concrete-target decode = %v, want 70 (custom Decode fired on the long branch)", bc)
		}
		if jc != 7 {
			t.Errorf("untagged JSON concrete-target decode = %v, want 7 (documented: first int branch + coercion, custom skipped)", jc)
		}

		// TaggedUnions recovers the custom branch for the concrete target.
		tw := avrotest.MustEncodeJSON(t, sc, untaggedPinBig(7), avro.TaggedUnions())
		var tc untaggedPinBig
		avrotest.MustDecodeJSON(t, sc, tw, &tc, avro.TaggedUnions())
		if tc != 70 {
			t.Errorf("tagged JSON concrete-target decode = %v, want 70 (branch named on the wire, custom fires)", tc)
		}
	})
}

// A schema nested past the limit must be rejected on the pre-scan, and one
// under the limit must still be accepted. aschema.UnmarshalJSON's object case
// scans each node's full JSON subtree to capture extra properties. A
// build-time maxDepth guard firing only after that unmarshal is too late. A
// linear pre-scan rejects past maxSchemaJSONDepth first. Its limit clears the
// provable ceiling of a build-acceptable schema by a full maxDepth. So this
// cell drives both sides of the line: 50000 and 4001 must reject, 900 must
// parse.
func TestRegression_DeepSchemaNestingRejectedInBoundedTime(t *testing.T) {
	arrayNest := func(d int) string {
		return strings.Repeat(`{"type":"array","items":`, d) + `"int"` + strings.Repeat(`}`, d)
	}

	// A schema nested far past the limit must be rejected, and rejected by the
	// pre-scan. This 1.25 MB input never reaches the recursive unmarshal.
	huge := arrayNest(50000)
	_, err := avro.Parse(huge)
	if err == nil {
		t.Fatal("a 50000-deep schema must be rejected, not parsed")
	}
	if !strings.Contains(err.Error(), "deep") {
		t.Errorf("expected a nesting-depth error, got: %v", err)
	}

	// Just past the limit is rejected; comfortably within it still parses.
	// maxSchemaJSONDepth is 4*maxDepth (4000) brackets.
	if _, err := avro.Parse(arrayNest(4001)); err == nil {
		t.Error("schema at 4001 array brackets (past the 4000 limit) must be rejected")
	}
	if _, err := avro.Parse(arrayNest(900)); err != nil {
		// 900 nested arrays = 900 brackets, well under the limit and under
		// maxDepth, so it is a valid schema.
		t.Errorf("a 900-deep array schema is valid and must parse: %v", err)
	}

	// No false rejection of a build-acceptable schema at its densest: a
	// maxDepth-1 chain of nested records reaches ~3*(maxDepth-1) brackets
	// (well under 4*maxDepth), so it must still parse.
	var b strings.Builder
	const recDepth = 999 // maxDepth-1: the deepest the builder accepts
	for i := range recDepth {
		fmt.Fprintf(&b, `{"type":"record","name":"R%d","fields":[{"name":"f","type":`, i)
	}
	b.WriteString(`"int"`)
	for range recDepth {
		b.WriteString(`}]}`)
	}
	if _, err := avro.Parse(b.String()); err != nil {
		t.Errorf("a %d-deep record schema is build-acceptable and must not be falsely rejected by the pre-scan: %v", recDepth, err)
	}

	// Ordinary schemas are unaffected.
	if _, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":["null","string"]}]}`); err != nil {
		t.Errorf("ordinary schema regressed: %v", err)
	}
}

// A CustomType on a recursive node must not skew the recursion-depth bound. The
// custom wrapper annotates an existing schema node rather than adding a nesting
// level. It charges 0 depth, matching the decode wrapper and the JSON path. A
// binary-encode wrapper that re-enters the base serializer at depth+1 charges
// an extra unit per recursive level. That trips errTooDeep on encode roughly
// 1.5x shallower than on decode: a value Decode can emit that Encode can never
// reproduce.
func TestRegression_CustomTypeRecursiveDepthUniform(t *testing.T) {
	ct := avro.CustomType{
		AvroType: "record",
		Encode:   func(v any, _ *avro.SchemaNode) (any, error) { return v, avro.ErrSkipCustomType },
		Decode:   func(v any, _ *avro.SchemaNode) (any, error) { return v, avro.ErrSkipCustomType },
	}
	const schema = `{"type":"record","name":"LL","fields":[{"name":"next","type":["null","LL"]},{"name":"v","type":"int"}]}`
	s, err := avro.Parse(schema, ct)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	plain := avro.MustParse(schema) // no custom: the reference depth bound

	mk := func(depth int) map[string]any {
		v := map[string]any{"v": int32(0), "next": nil}
		for i := 0; i < depth; i++ {
			v = map[string]any{"v": int32(0), "next": v}
		}
		return v
	}

	// A depth the plain (no-custom) schema encodes must also encode with the
	// custom applied: the custom must not lower the bound.
	plainMax := 0
	for d := 0; d <= 600; d++ {
		if _, e := plain.Encode(mk(d)); e != nil {
			break
		}
		plainMax = d
	}
	probe := plainMax - 50 // comfortably inside the plain bound, far above a depth+1 bound
	if probe < 350 {
		t.Fatalf("plain bound unexpectedly low (%d); test assumptions stale", plainMax)
	}
	b, err := s.Encode(mk(probe))
	if err != nil {
		t.Fatalf("custom-on-record Encode at depth %d must succeed (plain bound %d), got: %v", probe, plainMax, err)
	}
	var got any
	if _, err := s.Decode(b, &got); err != nil {
		t.Fatalf("custom-on-record Decode at depth %d: %v", probe, err)
	}

	// Cap still protects: a cyclic value must error on encode (not loop).
	type Node struct {
		Next *Node `avro:"next"`
		V    int32 `avro:"v"`
	}
	n := &Node{}
	n.Next = n
	if _, err := s.Encode(n); err == nil {
		t.Error("cyclic value must error (errTooDeep), not loop")
	}
}

// timestamp-nanos must encode the spec-correct "nanoseconds from epoch" for a
// negative-second instant. Java's TimestampNanosConversion.toLong has an
// off-by-1000 typo that corrupts pre-1970 sub-second instants. We are
// deliberately spec-correct. The other nanos tests use positive timestamps,
// which round-trip identically on both formulas. This is the pin that goes red
// if someone "fixes" us toward Java's bug.
func TestRegression_TimestampNanosNegativeSecondSpecValue(t *testing.T) {
	s := avro.MustParse(`{"type":"long","logicalType":"timestamp-nanos"}`)
	tm := time.Unix(-1, 500_000_000).UTC() // 0.5s before epoch, so -5e8 ns
	b := avrotest.MustEncode(t, s, tm)
	var got int64
	avrotest.MustDecode(t, s, b, &got)
	const specValue = int64(-500_000_000) // Java's off-by-1000 yields a different value
	if got != specValue {
		t.Errorf("timestamp-nanos(0.5s-before-epoch) = %d, want spec-correct %d (regression toward Java's off-by-1000?)", got, specValue)
	}
	var back time.Time
	if _, err := s.Decode(b, &back); err != nil {
		t.Fatalf("decode time: %v", err)
	}
	if !back.UTC().Equal(tm) {
		t.Errorf("round-trip = %v, want %v", back.UTC(), tm)
	}
}

// TestMatrix_JSONErrorsAreSemanticWithFieldPath pins doc.go's "# Errors"
// contract for the JSON wire. A type mismatch on EncodeJSON / DecodeJSON is an
// *avro.SemanticError carrying the same dotted record-field path the binary
// codecs produce. JSON encode arms returning bare fmt.Errorf values break that,
// as does a JSON decode path folding the field name into the message text only.
// A caller's errors.As + .Field handling then works for Encode/Decode and is
// silently broken for the JSON pair on the same value and schema.
func TestMatrix_JSONErrorsAreSemanticWithFieldPath(t *testing.T) {
	s := avro.MustParse(`{"type":"record","name":"O","fields":[
		{"name":"a","type":{"type":"record","name":"I","fields":[
			{"name":"b","type":"int"}]}}]}`)

	type inner struct {
		B string `avro:"b"` // string is not an int
	}
	type outer struct {
		A inner `avro:"a"`
	}

	assertFieldPath := func(t *testing.T, label string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s: expected error, got nil", label)
		}
		var se *avro.SemanticError
		if !errors.As(err, &se) {
			t.Fatalf("%s: error is not *SemanticError: %v", label, err)
		}
		if se.Field != "a.b" {
			t.Errorf("%s: SemanticError.Field = %q, want %q (err: %v)", label, se.Field, "a.b", err)
		}
	}

	if _, err := s.Encode(&outer{A: inner{B: "x"}}); true {
		assertFieldPath(t, "binary encode", err)
	}
	if _, err := s.EncodeJSON(&outer{A: inner{B: "x"}}); true {
		assertFieldPath(t, "json encode", err)
	}
	wire, err := s.Encode(map[string]any{"a": map[string]any{"b": int32(1)}})
	if err != nil {
		t.Fatalf("seed encode: %v", err)
	}
	var out1 outer
	if _, err := s.Decode(wire, &out1); true {
		assertFieldPath(t, "binary decode", err)
	}
	var out2 outer
	if err := s.DecodeJSON([]byte(`{"a":{"b":1}}`), &out2); true {
		assertFieldPath(t, "json decode", err)
	}

	// Top-level (non-record) JSON type mismatch must also be errors.As-able,
	// matching binary: the numeric coerce helpers tag their failures.
	for _, tc := range []struct {
		schema string
		encode any
	}{
		{`"int"`, "not an int"},
		{`"long"`, "not a long"},
		{`"double"`, "not a double"},
		{`"boolean"`, 5},
	} {
		js := avro.MustParse(tc.schema)
		_, err := js.EncodeJSON(tc.encode)
		var se *avro.SemanticError
		if err == nil || !errors.As(err, &se) {
			t.Errorf("EncodeJSON(%v) against %s: want *SemanticError, got %v", tc.encode, tc.schema, err)
		}
	}
}

// TestMatrix_JSONEncodeErrorSemanticParity is the standing net for the JSON
// error-surface class. For every Avro fragment, a wrong-Go-typed value wrapped
// as a record field "f" must be rejected by both encoders with an
// *avro.SemanticError carrying the field path "f". A JSON encoder returning
// bare fmt.Errorf values for any of these diverges silently from the binary
// encoder and from doc.go. The axis covers every fragment's JSON type-mismatch
// arm.
func TestMatrix_JSONEncodeErrorSemanticParity(t *testing.T) {
	frags := []struct {
		name, schema string
		wrong        any // a value of the wrong Go type for this fragment
	}{
		{"int", `"int"`, "x"},
		{"long", `"long"`, "x"},
		{"float", `"float"`, "x"},
		{"double", `"double"`, "x"},
		{"boolean", `"boolean"`, "x"},
		{"string", `"string"`, 5},
		{"bytes", `"bytes"`, 5},
		{"enum", `{"type":"enum","name":"E","symbols":["A"]}`, 5.5},
		// Right Go type, wrong content: a string naming no symbol is a
		// user-value failure. It must carry the same SemanticError identity
		// and field path as the type-mismatch rows on both wires.
		{"enum-unknown-symbol", `{"type":"enum","name":"E","symbols":["A"]}`, "NOPE"},
		{"fixed", `{"type":"fixed","name":"Fx","size":2}`, 5},
		{"array", `{"type":"array","items":"int"}`, 5},
		{"map", `{"type":"map","values":"int"}`, 5},
		{"record", `{"type":"record","name":"Sub","fields":[{"name":"n","type":"int"}]}`, 5},
	}
	for _, f := range frags {
		t.Run(f.name, func(t *testing.T) {
			s := avro.MustParse(fmt.Sprintf(`{"type":"record","name":"R","fields":[{"name":"f","type":%s}]}`, f.schema))
			val := map[string]any{"f": f.wrong}
			encoders := []struct {
				name string
				fn   func(any) ([]byte, error)
			}{
				{"binary", func(v any) ([]byte, error) { return s.AppendEncode(nil, v) }},
				{"json", func(v any) ([]byte, error) { return s.AppendEncodeJSON(nil, v) }},
			}
			for _, enc := range encoders {
				_, err := enc.fn(val)
				var se *avro.SemanticError
				if err == nil || !errors.As(err, &se) {
					t.Errorf("%s encode of wrong-typed %q field: want *SemanticError, got %v", enc.name, f.name, err)
					continue
				}
				if !strings.Contains(se.Field, "f") {
					t.Errorf("%s encode of %q: SemanticError.Field = %q, want it to contain \"f\"", enc.name, f.name, se.Field)
				}
			}
		})
	}
}

// TestRegression_CompatibilityErrorRenderingBounded pins that
// CompatibilityError.Error() bounds the user-controlled type and field names it
// renders. Names have no length cap at parse. A hostile schema with a
// megabyte-long name then drives a megabyte error string through logging, RPC
// and metric pipelines. That is the same 1:1 amplification class the OCF and
// wire-decode error sites guard. The public fields keep their full values, so
// callers that inspect the struct are unaffected.
func TestRegression_CompatibilityErrorRenderingBounded(t *testing.T) {
	huge := strings.Repeat("N", 1<<20)
	writer := avro.MustParse(fmt.Sprintf(`{"type":"record","name":"%s","fields":[{"name":"f","type":"int"}]}`, huge))
	reader := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"f","type":"int"}]}`)

	err := avro.CheckCompatibility(writer, reader)
	if err == nil {
		t.Fatal("expected a record-name-mismatch incompatibility")
	}
	if n := len(err.Error()); n > 4096 {
		t.Errorf("CompatibilityError.Error() is %d bytes (unbounded name echo); want bounded", n)
	}
	// The structured field is preserved in full (rendering-only truncation).
	var ce *avro.CompatibilityError
	if !errors.As(err, &ce) {
		t.Fatalf("not a *CompatibilityError: %v", err)
	}
	if ce.WriterType != huge {
		t.Errorf("CompatibilityError.WriterType was truncated in the struct (len %d); the field must keep its full value", len(ce.WriterType))
	}

	// Detail is *not* rendering-truncated, being a composed sentence. A
	// user-controlled value embedded in it must be bounded at construction.
	// Resolve of an enum whose writer carries a huge symbol absent from the
	// reader is rejected by Resolve's compatibility pre-check. That pre-check's
	// enum-symbol Detail embeds the symbol. The resolution-time twin resolveEnum
	// builds the same Detail and is likewise bounded for internal consistency.
	// The pre-check guards it from this input, so we exercise the pre-check path
	// here.
	wEnum := avro.MustParse(fmt.Sprintf(`{"type":"enum","name":"E","symbols":[%q,"B"]}`, huge))
	rEnum := avro.MustParse(`{"type":"enum","name":"E","symbols":["B"]}`)
	if _, rerr := avro.Resolve(wEnum, rEnum); rerr == nil {
		t.Fatal("expected an enum-symbol incompatibility from Resolve")
	} else if n := len(rerr.Error()); n > 4096 {
		t.Errorf("Resolve enum-symbol error is %d bytes (unbounded Detail echo); want bounded", n)
	}
}

// Parsing a fixed schema whose size is large must not allocate proportional to
// that size. A fixed size is schema-controlled and only validated non-negative,
// matching fastavro/avro-rs. When a CustomType matches a fixed logical node,
// parse consults jsonDecodeAppliesLogical. Its fixed arm doing
// make([]byte, node.size) turns a tiny untrusted schema into a multi-GB
// parse-time allocation, so we bound the probe to maxFixedLogicalLen+1. Here we
// pin the end-to-end parse path.
// TestMatrix_JSONDecodeAppliesLogicalMatchesDecode pins the probe-level answer.
func TestRegression_FixedLogicalProbeSizeBounded(t *testing.T) {
	ct := avro.CustomType{AvroType: "fixed", LogicalType: "duration"}
	const schema = `{"type":"fixed","size":9223372036854775807,"logicalType":"duration","name":"f"}`

	// The goroutine is the panic harness: a make([]byte, 2^63-1) is a fatal
	// runtime error the test must attribute, not an error return.
	done := make(chan struct{})
	var panicVal any
	go func() {
		defer func() { panicVal = recover(); close(done) }()
		_, _ = avro.Parse(schema, avro.WithCustomType(ct))
	}()
	<-done
	if panicVal != nil {
		t.Fatalf("Parse panicked on a large fixed size (parse-time make([]byte, size) DoS): %v", panicVal)
	}

	// Answer-preservation at the in-range sizes: a no-callback CustomType on a
	// uuid fixed (size 16) suppresses the logical. DecodeJSON into any then
	// yields the raw 16 bytes (matching binary's deserFixed), not an enriched
	// [16]byte.
	// If the probe cap broke the size-16/12 cases, suppression would not install
	// and JSON would leak the enriched type.
	suppressed := avro.MustParse(`{"type":"fixed","size":16,"logicalType":"uuid","name":"u"}`, avro.WithCustomType(avro.CustomType{AvroType: "fixed", LogicalType: "uuid"}))
	in := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	jb := avrotest.MustAppendEncodeJSON(t, suppressed, nil, in)
	var got any
	avrotest.MustDecodeJSON(t, suppressed, jb, &got)
	if _, ok := got.([]byte); !ok {
		t.Fatalf("uuid suppression broken by probe cap: DecodeJSON into any returned %T, want []byte (raw)", got)
	}
}

// A bare (untagged) JSON union value commits to the first token-class-matching
// container branch. It does *not* backtrack to re-decode the whole subtree as
// each later container branch. Backtracking is 2^depth for a recursive
// union-of-records: a ~120-byte bare nested object mismatching at the bottom
// takes seconds to reject. The spec encodes a non-null union as the tagged
// form, and Java, fastavro and goavro all read the branch from that tag. Scalar
// branches keep their bounded backtrack. They cannot recurse into the union, so
// they add no blowup and preserve the numeric-width fall-through.
func TestRegression_BareUnionJSONNoExponentialBacktrack(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		// keyed builds the nested object using the given field key. The
		// innermost value (a bare number) matches no branch, forcing failure at
		// the bottom: the worst case for container backtracking.
		key string
	}{
		{
			// Bare path: the field name ("v") is *not* a branch name, so every
			// level routes through decodeUnionBare. That must commit to the
			// first matching container branch instead of trying A then B.
			name: "bare-path-distinct-field-name",
			schema: `["null",
				{"type":"record","name":"A","fields":[{"name":"v","type":["null","A","B"]}]},
				{"type":"record","name":"B","fields":[{"name":"v","type":["null","A","B"]}]}]`,
			key: "v",
		},
		{
			// Tagged-fallback path: the field name ("A") collides with a branch
			// name, so decodeUnionObject's tagged decode matches a container
			// branch at every level. It must commit to the tagged interpretation
			// rather than also trying the bare fallback. The two together double
			// the recursion to 2^depth.
			name:   "tagged-fallback-field-name-collides-with-branch",
			schema: `["null",{"type":"record","name":"A","fields":[{"name":"A","type":["null","A"]}]}]`,
			key:    "A",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := avro.MustParse(c.schema)
			build := func(depth int) []byte {
				var b strings.Builder
				for range depth {
					b.WriteString(`{"`)
					b.WriteString(c.key)
					b.WriteString(`":`)
				}
				b.WriteString(`1`)
				for range depth {
					b.WriteString(`}`)
				}
				return []byte(b.String())
			}

			// Depth 20 costs ~2^20 full subtree re-decodes if the decoder
			// backtracks; committing to the first container branch rejects at
			// the bottom instead.
			var out any
			if err := s.DecodeJSON(build(20), &out); err == nil {
				t.Fatal("expected a decode error (innermost value matches no branch)")
			}

			// Depth 200 is the arm the backtracking cannot reach at all. 2^200
			// re-decodes do not return, so a regression here does not produce a
			// slow answer, it produces no answer. Depth 200 stays within
			// maxDepth, so the rejection is the bottom mismatch, exercising the
			// commit-to-first path itself.
			if err := s.DecodeJSON(build(200), &out); err == nil {
				t.Fatal("expected a decode error at depth 200")
			}
		})
	}
}

// A schema written in the flat (goavro-style) field format parses by design.
// The wire parser lifts the defining keys into a nested type definition named
// after the field. Root() must describe that same post-lift schema. That means
// the type node carrying the name and defining content, not an empty shell with
// the keys stranded in Field.Props. Root().Schema() must rebuild it canonically
// identically. The metadata walker shares the parser's lift predicate and key
// routing, so we pin here that the two sides describe one schema.
func TestMatrix_FlatFieldRootSchemaRoundTrip(t *testing.T) {
	for _, tt := range []struct {
		name, schema string
		check        func(t *testing.T, f avro.SchemaField)
	}{
		{
			"enum",
			`{"type":"record","name":"R","fields":[{"name":"E","type":"enum","symbols":["A","B"]}]}`,
			func(t *testing.T, f avro.SchemaField) {
				if f.Type.Name != "E" || len(f.Type.Symbols) != 2 {
					t.Errorf("lifted enum: Name=%q Symbols=%v, want E / [A B]", f.Type.Name, f.Type.Symbols)
				}
			},
		},
		{
			"fixed",
			`{"type":"record","name":"R","fields":[{"name":"F","type":"fixed","size":4}]}`,
			func(t *testing.T, f avro.SchemaField) {
				if f.Type.Name != "F" || f.Type.Size != 4 {
					t.Errorf("lifted fixed: Name=%q Size=%d, want F / 4", f.Type.Name, f.Type.Size)
				}
			},
		},
		{
			"array",
			`{"type":"record","name":"R","fields":[{"name":"A","type":"array","items":"int"}]}`,
			func(t *testing.T, f avro.SchemaField) {
				if f.Type.Items == nil || f.Type.Items.Type != "int" {
					t.Errorf("lifted array: Items=%v, want int items", f.Type.Items)
				}
			},
		},
		{
			"map",
			`{"type":"record","name":"R","fields":[{"name":"M","type":"map","values":"long"}]}`,
			func(t *testing.T, f avro.SchemaField) {
				if f.Type.Values == nil || f.Type.Values.Type != "long" {
					t.Errorf("lifted map: Values=%v, want long values", f.Type.Values)
				}
			},
		},
		{
			"record",
			`{"type":"record","name":"R","fields":[{"name":"Sub","type":"record","fields":[{"name":"x","type":"int"}]}]}`,
			func(t *testing.T, f avro.SchemaField) {
				if f.Type.Name != "Sub" || len(f.Type.Fields) != 1 {
					t.Errorf("lifted record: Name=%q Fields=%v, want Sub with 1 field", f.Type.Name, f.Type.Fields)
				}
			},
		},
		{
			"error",
			`{"type":"record","name":"R","fields":[{"name":"Sub","type":"error","fields":[{"name":"x","type":"int"}]}]}`,
			func(t *testing.T, f avro.SchemaField) {
				if f.Type.Type != "error" || f.Type.Name != "Sub" || len(f.Type.Fields) != 1 {
					t.Errorf("lifted error: Type=%q Name=%q Fields=%v, want error/Sub with 1 field", f.Type.Type, f.Type.Name, f.Type.Fields)
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := avrotest.MustParse(t, tt.schema)
			root := s.Root()
			if len(root.Fields) != 1 {
				t.Fatalf("Root fields: %d, want 1", len(root.Fields))
			}
			tt.check(t, root.Fields[0])
			rebuilt := avrotest.MustNodeSchema(t, root)
			if got, want := string(rebuilt.Canonical()), string(s.Canonical()); got != want {
				t.Fatalf("canonical mismatch:\n got %s\nwant %s", got, want)
			}
		})
	}
}

// A sibling field referencing a flat-defined fixed by name must surface its
// default as []byte, per the SchemaField.Default contract ("bytes and fixed
// schemas give []byte"). The lifted fixed carries the field's name, so it is
// registered in the metadata name table. The name-referencing sibling's default
// then coerces exactly as it would against a nested-form definition.
func TestRegression_FlatFixedNameRefDefaultCoerced(t *testing.T) {
	s := avrotest.MustParse(t, `{"type":"record","name":"R","fields":[
		{"name":"F","type":"fixed","size":4},
		{"name":"F2","type":"F","default":"abcd"}]}`)
	f2 := s.Root().Fields[1]
	b, ok := f2.Default.([]byte)
	if !ok {
		t.Fatalf("F2 default: got %T (%v), want []byte", f2.Default, f2.Default)
	}
	if string(b) != "abcd" {
		t.Fatalf("F2 default bytes: %q, want %q", b, "abcd")
	}
}

// ---------- custom_named_avro_type_test.go ----------

// A CustomType's Avro-native type A may be a named Go type over a canonical
// kind, such as type UnixMillis int64 as the long representation. inferAvroType
// classifies A by reflect.Kind, so it registers as an ordinary primitive
// custom. On decode the base deserializer produces the canonical Go value for
// that kind. The generated decode wrapper must convert it to A before invoking
// the user's decode. The canonical value's dynamic type is the base kind, not
// the named type, so a bare type assertion panics. The encode side already
// type-guards, so only decode is exposed. We pin every canonical kind on both
// wires.

type namedBool bool
type namedInt32 int32
type namedInt64 int64
type namedFloat32 float32
type namedFloat64 float64
type namedString string
type namedBytes []byte

func TestMatrix_CustomNamedAvroNativeTypeDecodes(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		ct     avro.CustomType
		in     any // a G value to encode
		want   any // the expected decoded G value
	}{
		{"boolean", `{"type":"boolean"}`,
			avro.NewCustomType[bool, namedBool]("",
				func(g bool, _ *avro.SchemaNode) (namedBool, error) { return namedBool(g), nil },
				func(a namedBool, _ *avro.SchemaNode) (bool, error) { return bool(a), nil }),
			true, true},
		{"int", `{"type":"int"}`,
			avro.NewCustomType[int32, namedInt32]("",
				func(g int32, _ *avro.SchemaNode) (namedInt32, error) { return namedInt32(g), nil },
				func(a namedInt32, _ *avro.SchemaNode) (int32, error) { return int32(a), nil }),
			int32(5), int32(5)},
		{"long", `{"type":"long"}`,
			avro.NewCustomType[int64, namedInt64]("",
				func(g int64, _ *avro.SchemaNode) (namedInt64, error) { return namedInt64(g), nil },
				func(a namedInt64, _ *avro.SchemaNode) (int64, error) { return int64(a), nil }),
			int64(1700000000000), int64(1700000000000)},
		{"float", `{"type":"float"}`,
			avro.NewCustomType[float32, namedFloat32]("",
				func(g float32, _ *avro.SchemaNode) (namedFloat32, error) { return namedFloat32(g), nil },
				func(a namedFloat32, _ *avro.SchemaNode) (float32, error) { return float32(a), nil }),
			float32(2.5), float32(2.5)},
		{"double", `{"type":"double"}`,
			avro.NewCustomType[float64, namedFloat64]("",
				func(g float64, _ *avro.SchemaNode) (namedFloat64, error) { return namedFloat64(g), nil },
				func(a namedFloat64, _ *avro.SchemaNode) (float64, error) { return float64(a), nil }),
			float64(2.5), float64(2.5)},
		{"string", `{"type":"string"}`,
			avro.NewCustomType[string, namedString]("",
				func(g string, _ *avro.SchemaNode) (namedString, error) { return namedString(g), nil },
				func(a namedString, _ *avro.SchemaNode) (string, error) { return string(a), nil }),
			"hello", "hello"},
		{"bytes", `{"type":"bytes"}`,
			avro.NewCustomType[[]byte, namedBytes]("",
				func(g []byte, _ *avro.SchemaNode) (namedBytes, error) { return namedBytes(g), nil },
				func(a namedBytes, _ *avro.SchemaNode) ([]byte, error) { return []byte(a), nil }),
			[]byte("hi"), []byte("hi")},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := avro.Parse(c.schema, avro.WithCustomType(c.ct))
			if err != nil {
				t.Fatalf("registration/parse: %v", err)
			}
			decodeNoPanic(t, "binary", c.want, func() (any, error) {
				wire, err := s.Encode(c.in)
				if err != nil {
					return nil, err
				}
				var got any
				_, err = s.Decode(wire, &got)
				return got, err
			})
			decodeNoPanic(t, "json", c.want, func() (any, error) {
				js, err := s.AppendEncodeJSON(nil, c.in)
				if err != nil {
					return nil, err
				}
				var got any
				err = s.DecodeJSON(js, &got)
				return got, err
			})
		})
	}
}

func decodeNoPanic(t *testing.T, label string, want any, run func() (any, error)) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("%s: decode PANIC on a registration-accepted custom: %v", label, r)
		}
	}()
	got, err := run()
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: round-trip = %#v, want %#v", label, got, want)
	}
}

// ---------- custom_skip_decode_test.go ----------

// Test types for the skip-custom parity matrix. Package-level so reflect.New
// sees named (not anonymous) types, matching real caller targets.
type csInner struct {
	X int32 `avro:"x"`
}
type csStruct struct {
	A int64  `avro:"a"`
	B string `avro:"b"`
}
type csNest struct {
	In csInner `avro:"in"`
	N  int64   `avro:"n"`
}
type csMoney int64
type csPtr struct {
	P *int32 `avro:"p"`
}
type csDec struct {
	D *big.Rat `avro:"d"`
}

// Named scalar types: the canonical Avro value (int32/string/bool/float32/
// float64/[]byte) is *not* assignable to a named type. A bare value placement
// cannot satisfy these targets. The all-skip fall-through must re-decode the
// wire into them through the base deserializer, exactly as a no-custom decode
// does. Mishandling a named target reds the matching row.
type (
	csI32   int32
	csStr   string
	csBool  bool
	csF32   float32
	csF64   float64
	csBytes []byte
)

// namedI32 is a named int32 so a *namedI32 union target is *not* trivially
// assignable from the canonical int32. The all-skip fall-through must re-decode
// the union through the base deserializer, which reads the exact wire branch. A
// plain *int32 is the easy case; the named type forces the full per-branch
// decode.
type namedI32 int32
type csUPtr struct {
	P *namedI32 `avro:"p"`
}

// TestMatrix_CustomSkipDecodeMatchesNoCustom pins that a custom decoder
// returning ErrSkipCustomType falls through to built-in decode. The value lands
// in the typed target exactly as a no-custom decode would, on binary, JSON and
// resolved paths. A wildcard custom wraps every node and skips, so the
// fall-through re-decodes every kind through the base deserializer. A wildcard
// does not suppress logicals, so each row decodes one wire with both schemas
// into one target. We prove non-vacuity by neutering the re-decode to place a
// probe value.
func TestMatrix_CustomSkipDecodeMatchesNoCustom(t *testing.T) {
	skip := avro.CustomType{Decode: func(any, *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType }}
	ptr := func(x int32) *int32 { return &x }
	pn := func(x namedI32) *namedI32 { return &x }

	rows := []struct {
		name   string
		schema string
		value  any // typed value; its type is also the decode target
	}{
		{"record-struct", `{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}`, csStruct{A: 1, B: "x"}},
		{"nested-struct", `{"type":"record","name":"R","fields":[{"name":"in","type":{"type":"record","name":"In","fields":[{"name":"x","type":"int"}]}},{"name":"n","type":"long"}]}`, csNest{In: csInner{X: 7}, N: 9}},
		{"record-map", `{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"long"}]}`, map[string]int64{"a": 1, "b": 2}},
		{"array-int", `{"type":"array","items":"long"}`, []int64{1, 2, 3}},
		{"array-struct", `{"type":"array","items":{"type":"record","name":"In","fields":[{"name":"x","type":"int"}]}}`, []csInner{{X: 1}, {X: 2}}},
		{"array-fixedlen", `{"type":"array","items":"int"}`, [3]int32{4, 5, 6}},
		{"map-int", `{"type":"map","values":"long"}`, map[string]int64{"k": 5}},
		{"map-struct", `{"type":"map","values":{"type":"record","name":"In","fields":[{"name":"x","type":"int"}]}}`, map[string]csInner{"k": {X: 3}}},
		{"named-long", `"long"`, csMoney(42)},
		{"named-int", `"int"`, csI32(7)},
		{"named-string", `"string"`, csStr("hi")},
		{"named-bool", `"boolean"`, csBool(true)},
		{"named-float", `"float"`, csF32(1.5)},
		{"named-double", `"double"`, csF64(2.5)},
		{"named-bytes", `"bytes"`, csBytes{1, 2, 3}},
		{"ptr-field-set", `{"type":"record","name":"R","fields":[{"name":"p","type":["null","int"]}]}`, csPtr{P: ptr(8)}},
		{"ptr-field-nil", `{"type":"record","name":"R","fields":[{"name":"p","type":["null","int"]}]}`, csPtr{P: nil}},
		// Union into a named-pointer target: int32 is not assignable to namedI32,
		// so setCustomResult's pointer-walk cannot resolve it. The union branches
		// arm then runs and recovers the lost branch index. A plain *int32
		// (ptr-field-set above) is resolved before the loop, leaving it un-netted.
		{"union-named-ptr-set", `{"type":"record","name":"R","fields":[{"name":"p","type":["null","int"]}]}`, csUPtr{P: pn(8)}},
		{"union-named-ptr-nil", `{"type":"record","name":"R","fields":[{"name":"p","type":["null","int"]}]}`, csUPtr{P: nil}},
		{"union-named-slice", `{"type":"array","items":["null","int"]}`, []*namedI32{pn(5), nil}},
		{"enum-string", `{"type":"enum","name":"E","symbols":["A","B","C"]}`, "B"},
		{"enum-named-int", `{"type":"enum","name":"E","symbols":["A","B","C"]}`, csMoney(2)},
		{"fixed-array", `{"type":"fixed","name":"F","size":4}`, [4]byte{1, 2, 3, 4}},
		{"bytes", `"bytes"`, []byte{9, 8, 7}},
		{"bool", `"boolean"`, true},
		{"float", `"float"`, float32(1.5)},
		{"double", `"double"`, float64(2.5)},
		{"string", `"string"`, "hi"},
		{"timestamp", `{"type":"long","logicalType":"timestamp-millis"}`, time.UnixMilli(1600000000000).UTC()},
		{"decimal-in-record", `{"type":"record","name":"R","fields":[{"name":"d","type":{"type":"bytes","logicalType":"decimal","precision":5,"scale":2}}]}`, csDec{D: big.NewRat(33, 100)}},
		{"uuid-string", `{"type":"string","logicalType":"uuid"}`, "12345678-1234-1234-1234-123456789abc"},
	}

	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			plain := avro.MustParse(r.schema)
			sskip := avro.MustParse(r.schema, skip)
			tt := reflect.TypeOf(r.value)

			wireBin, err := plain.Encode(r.value)
			if err != nil {
				t.Fatalf("encode binary: %v", err)
			}
			wireJSON, err := plain.EncodeJSON(r.value)
			if err != nil {
				t.Fatalf("encode json: %v", err)
			}

			dec := func(s *avro.Schema, wire []byte, jsonForm bool, opts ...avro.Opt) (any, error) {
				p := reflect.New(tt)
				if jsonForm {
					return p.Elem().Interface(), s.DecodeJSON(wire, p.Interface(), opts...)
				}
				_, err := s.Decode(wire, p.Interface(), opts...)
				return p.Elem().Interface(), err
			}

			resolved, rerr := avro.Resolve(plain, sskip)
			if rerr != nil {
				t.Fatalf("resolve: %v", rerr)
			}

			// Run the matrix untagged and with TaggedUnions. The all-skip
			// fall-through re-decodes the wire into the target, so a union field
			// lands identically to a no-custom decode under either option. The
			// re-decode reads the exact wire branch, so neither a typed target
			// nor a tagged envelope can be misplaced.
			check := func(opt string, opts ...avro.Opt) {
				// Oracle: plain no-custom decode (binary + JSON).
				binPlain, e := dec(plain, wireBin, false, opts...)
				if e != nil {
					t.Fatalf("[%s] plain binary decode: %v", opt, e)
				}
				jsonPlain, e := dec(plain, wireJSON, true, opts...)
				if e != nil {
					t.Fatalf("[%s] plain json decode: %v", opt, e)
				}
				// Skip-custom and resolved (skip-custom reader) must equal it.
				binSkip, e := dec(sskip, wireBin, false, opts...)
				if e != nil {
					t.Fatalf("[%s] skip-custom binary decode errored where no-custom succeeded: %v", opt, e)
				}
				if !matEqual(binPlain, binSkip) {
					t.Errorf("[%s] binary skip-custom != no-custom:\n no-custom=%#v\n skip     =%#v", opt, binPlain, binSkip)
				}
				jsonSkip, e := dec(sskip, wireJSON, true, opts...)
				if e != nil {
					t.Fatalf("[%s] skip-custom json decode errored where no-custom succeeded: %v", opt, e)
				}
				if !matEqual(jsonPlain, jsonSkip) {
					t.Errorf("[%s] json skip-custom != no-custom:\n no-custom=%#v\n skip     =%#v", opt, jsonPlain, jsonSkip)
				}
				binRes, e := dec(resolved, wireBin, false, opts...)
				if e != nil {
					t.Fatalf("[%s] resolved binary decode errored: %v", opt, e)
				}
				if !matEqual(binPlain, binRes) {
					t.Errorf("[%s] resolved binary skip-custom != no-custom:\n no-custom=%#v\n resolved =%#v", opt, binPlain, binRes)
				}
				jsonRes, e := dec(resolved, wireJSON, true, opts...)
				if e != nil {
					t.Fatalf("[%s] resolved json decode errored: %v", opt, e)
				}
				if !matEqual(jsonPlain, jsonRes) {
					t.Errorf("[%s] resolved json skip-custom != no-custom:\n no-custom=%#v\n resolved =%#v", opt, jsonPlain, jsonRes)
				}
			}
			check("untagged")
			check("tagged", avro.TaggedUnions())
		})
	}
}

type csTransformed struct{ Cents int64 }

// Targets for the nested-match axis. Each pairs a container holding a
// marked inner node with the sibling positions that must stay untouched.
type (
	csMatchField struct {
		Amt  csTransformed `avro:"amt"`
		Name string        `avro:"name"`
	}
	csMatchFieldRaw struct {
		Amt  int64  `avro:"amt"`
		Name string `avro:"name"`
	}
	csMatchInner struct {
		Amt csTransformed `avro:"amt"`
	}
	csMatchInnerRaw struct {
		Amt int64 `avro:"amt"`
	}
	csMatchOuter struct {
		In csMatchInner `avro:"in"`
		N  int64        `avro:"n"`
	}
	csMatchOuterRaw struct {
		In csMatchInnerRaw `avro:"in"`
		N  int64           `avro:"n"`
	}
)

// TestMatrix_CustomSkipNestedMatchRedecodes adds the selectivity axis. The skip
// matrix drives a wildcard that skips at every node, so its corpus only reaches
// the fall-through's bypass arm. The other value of the axis is a custom that
// skips at the outer node but matched somewhere in its subtree. The
// fall-through cannot bypass then, since that would discard the nested match.
// It re-decodes with customs active, and both wires carry their own copy of
// that decision.
//
// We cross that with container kind, since the fall-through sits in each
// container's decoder. The oracle is a no-custom decode into the raw-typed
// twin. The marked position carries the transform of exactly the value the raw
// decode saw, and every sibling is untouched.
func TestMatrix_CustomSkipNestedMatchRedecodes(t *testing.T) {
	t.Parallel()
	money := avro.CustomType{
		Decode: func(v any, sn *avro.SchemaNode) (any, error) {
			if sn.Props["domain"] == "money" {
				return csTransformed{Cents: v.(int64)}, nil
			}
			return nil, avro.ErrSkipCustomType
		},
	}
	const marked = `{"type":"long","domain":"money"}`

	rows := []struct {
		name   string
		schema string
		// raw is the value encoded through the plain schema, typed so that
		// the marked position is an ordinary int64.
		raw any
		// want is the same value as the custom-decoding target expects.
		want any
	}{
		{
			"record-field",
			`{"type":"record","name":"R","fields":[{"name":"amt","type":` + marked + `},{"name":"name","type":"string"}]}`,
			csMatchFieldRaw{Amt: 500, Name: "alice"},
			csMatchField{Amt: csTransformed{Cents: 500}, Name: "alice"},
		},
		{
			"record-nested",
			`{"type":"record","name":"R","fields":[{"name":"in","type":{"type":"record","name":"In","fields":[{"name":"amt","type":` + marked + `}]}},{"name":"n","type":"long"}]}`,
			csMatchOuterRaw{In: csMatchInnerRaw{Amt: 700}, N: 9},
			csMatchOuter{In: csMatchInner{Amt: csTransformed{Cents: 700}}, N: 9},
		},
		{
			"array-element",
			`{"type":"array","items":` + marked + `}`,
			[]int64{1, 2, 3},
			[]csTransformed{{Cents: 1}, {Cents: 2}, {Cents: 3}},
		},
		{
			"map-value",
			`{"type":"map","values":` + marked + `}`,
			map[string]int64{"k": 42},
			map[string]csTransformed{"k": {Cents: 42}},
		},
	}

	// Liveness floor, counted inside the cell. A row whose outer node
	// stopped skipping, or whose inner node stopped matching, would take
	// the bypass arm and pass every assertion below by accident.
	redecoded := 0

	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			plain := avro.MustParse(r.schema)
			s := avro.MustParse(r.schema, money)

			wireBin, err := plain.Encode(r.raw)
			if err != nil {
				t.Fatalf("encode binary: %v", err)
			}
			wireJSON, err := plain.EncodeJSON(r.raw)
			if err != nil {
				t.Fatalf("encode json: %v", err)
			}

			// The oracle: the raw-typed twin decoded with no custom. We read the
			// marked position's expected transform off it, so the expectation is
			// not a restatement of what the custom did.
			rawOut := reflect.New(reflect.TypeOf(r.raw))
			if _, err := plain.Decode(wireBin, rawOut.Interface()); err != nil {
				t.Fatalf("oracle decode: %v", err)
			}
			if !matEqual(rawOut.Elem().Interface(), r.raw) {
				t.Fatalf("oracle decode did not round-trip the raw value:\n got  %#v\n want %#v", rawOut.Elem().Interface(), r.raw)
			}

			for _, w := range []struct {
				name string
				run  func(any) error
			}{
				{"binary", func(p any) error { _, err := s.Decode(wireBin, p); return err }},
				{"json", func(p any) error { return s.DecodeJSON(wireJSON, p) }},
			} {
				t.Run(w.name, func(t *testing.T) {
					p := reflect.New(reflect.TypeOf(r.want))
					if err := w.run(p.Interface()); err != nil {
						t.Fatalf("decode: %v", err)
					}
					got := p.Elem().Interface()
					if !matEqual(got, r.want) {
						t.Fatalf("the nested match did not survive the outer skip:\n got  %#v\n want %#v", got, r.want)
					}
					redecoded++
				})
			}
		})
	}
	if want := len(rows) * 2; redecoded != want {
		t.Errorf("%d of %d cells reached the re-decode; a row stopped exercising the nested-match arm", redecoded, want)
	}
}

// TestRegression_CustomSkipDecodeMatchedTransformSurvives nets the deep-match
// re-decode path, which the main net's purely-skipping wildcard never carries.
// A wildcard transforms one node, a long tagged "domain":"money" into a domain
// Go type, and skips the rest. The record itself is skipped, but a nested
// custom matched in its subtree. The fall-through therefore re-decodes it with
// customs active, reproducing the money field's transform while the skipped
// sibling decodes normally. A bypass here, or a placement that drops the
// match, lands int64 where the transformed type is expected.
func TestRegression_CustomSkipDecodeMatchedTransformSurvives(t *testing.T) {
	ct := avro.CustomType{
		Decode: func(v any, sn *avro.SchemaNode) (any, error) {
			if sn.Props["domain"] == "money" {
				return csTransformed{Cents: v.(int64)}, nil
			}
			return nil, avro.ErrSkipCustomType
		},
	}
	schema := `{"type":"record","name":"R","fields":[{"name":"amt","type":{"type":"long","domain":"money"}},{"name":"name","type":"string"}]}`
	plain := avro.MustParse(schema)
	s := avro.MustParse(schema, ct)

	// Encode the raw wire through a plain (int64) shape.
	type Rw struct {
		Amt  int64  `avro:"amt"`
		Name string `avro:"name"`
	}
	wireBin := avrotest.MustEncode(t, plain, Rw{Amt: 500, Name: "alice"})
	wireJSON := avrotest.MustEncodeJSON(t, plain, Rw{Amt: 500, Name: "alice"})

	type R struct {
		Amt  csTransformed `avro:"amt"`
		Name string        `avro:"name"`
	}
	want := R{Amt: csTransformed{Cents: 500}, Name: "alice"}

	var gb R
	avrotest.MustDecode(t, s, wireBin, &gb)
	if gb != want {
		t.Errorf("binary: matched transform did not survive (or skipped sibling wrong):\n got=%+v\n want=%+v", gb, want)
	}
	var gj R
	avrotest.MustDecodeJSON(t, s, wireJSON, &gj)
	if gj != want {
		t.Errorf("json: matched transform did not survive (or skipped sibling wrong):\n got=%+v\n want=%+v", gj, want)
	}
}

// TestMatrix_CustomSkipDecodeReusesTarget pins that a wildcard all-skip custom
// decode reuses a pre-populated target identically to a no-custom decode. A
// non-nil typed map, and an interface already wrapping a map[string]any, retain
// keys absent from the wire. The fall-through re-decodes into the same target
// through the base deserializer, so reuse is inherited rather than
// re-implemented.
//
// Invisible to the main skip matrix, which decodes only into fresh targets. The
// map[string]any subtest is the cell an assignable-fast-path placement
// swallows. An Avro map decoded into `any` is the control, since deserMap's
// iface arm allocates fresh. Non-vacuity: neutering the typed-target re-decode
// reds the map[string]any and record-into-any cells.
func TestMatrix_CustomSkipDecodeReusesTarget(t *testing.T) {
	skip := avro.CustomType{Decode: func(any, *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType }}

	t.Run("typed-map", func(t *testing.T) {
		schema := `{"type":"map","values":"long"}`
		plain := avro.MustParse(schema)
		sskip := avro.MustParse(schema, skip)
		bin := avrotest.MustEncode(t, plain, map[string]int64{"k": 5})
		jsonw := avrotest.MustEncodeJSON(t, plain, map[string]int64{"k": 5})
		nb := map[string]int64{"stale": 99}
		avrotest.MustDecode(t, plain, bin, &nb)
		sb := map[string]int64{"stale": 99}
		avrotest.MustDecode(t, sskip, bin, &sb)
		if !reflect.DeepEqual(nb, sb) {
			t.Errorf("binary typed-map: skip-custom=%v != no-custom=%v (stale key must be retained)", sb, nb)
		}
		nj := map[string]int64{"stale": 99}
		avrotest.MustDecodeJSON(t, plain, jsonw, &nj)
		sj := map[string]int64{"stale": 99}
		avrotest.MustDecodeJSON(t, sskip, jsonw, &sj)
		if !reflect.DeepEqual(nj, sj) {
			t.Errorf("json typed-map: skip-custom=%v != no-custom=%v", sj, nj)
		}
	})

	t.Run("typed-map-any", func(t *testing.T) {
		// map[string]any (Kind Map, any-valued) is the value type an assignable
		// fast-path placement swallows. That replaces the whole map and drops
		// stale keys the base decoder retains. Re-decode reuses it like any
		// other non-nil typed map, on binary and JSON.
		schema := `{"type":"map","values":"long"}`
		plain := avro.MustParse(schema)
		sskip := avro.MustParse(schema, skip)
		bin := avrotest.MustEncode(t, plain, map[string]int64{"k": 5})
		jsonw := avrotest.MustEncodeJSON(t, plain, map[string]int64{"k": 5})
		nb := map[string]any{"stale": int64(99)}
		avrotest.MustDecode(t, plain, bin, &nb)
		sb := map[string]any{"stale": int64(99)}
		avrotest.MustDecode(t, sskip, bin, &sb)
		if !reflect.DeepEqual(nb, sb) {
			t.Errorf("binary map[string]any: skip-custom=%v != no-custom=%v (stale key must be retained)", sb, nb)
		}
		nj := map[string]any{"stale": int64(99)}
		avrotest.MustDecodeJSON(t, plain, jsonw, &nj)
		sj := map[string]any{"stale": int64(99)}
		avrotest.MustDecodeJSON(t, sskip, jsonw, &sj)
		if !reflect.DeepEqual(nj, sj) {
			t.Errorf("json map[string]any: skip-custom=%v != no-custom=%v", sj, nj)
		}
	})

	t.Run("record-into-any", func(t *testing.T) {
		schema := `{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}`
		plain := avro.MustParse(schema)
		sskip := avro.MustParse(schema, skip)
		type Rw struct {
			A int64 `avro:"a"`
		}
		bin := avrotest.MustEncode(t, plain, Rw{A: 5})
		var nb any = map[string]any{"stale": int64(99)}
		avrotest.MustDecode(t, plain, bin, &nb)
		var sb any = map[string]any{"stale": int64(99)}
		avrotest.MustDecode(t, sskip, bin, &sb)
		if !reflect.DeepEqual(nb, sb) {
			t.Errorf("binary record-into-any: skip-custom=%v != no-custom=%v (stale key must be retained)", sb, nb)
		}
	})

	t.Run("map-into-any-control", func(t *testing.T) {
		// Avro map into `any` must match no-custom, and no-custom does *not*
		// reuse: deserMap's iface arm allocates fresh, so the stale key is
		// dropped on both. This guards an over-eager reuse from retaining the
		// stale key where the base decoder would not.
		schema := `{"type":"map","values":"long"}`
		plain := avro.MustParse(schema)
		sskip := avro.MustParse(schema, skip)
		bin := avrotest.MustEncode(t, plain, map[string]int64{"k": 5})
		var nb any = map[string]any{"stale": int64(99)}
		avrotest.MustDecode(t, plain, bin, &nb)
		var sb any = map[string]any{"stale": int64(99)}
		avrotest.MustDecode(t, sskip, bin, &sb)
		if !reflect.DeepEqual(nb, sb) {
			t.Errorf("binary map-into-any: skip-custom=%v != no-custom=%v", sb, nb)
		}
	})
}

// TestMatrix_CustomSkipDecodeLogicalIntoBaseTypedTarget pins that a wildcard
// all-skip custom, which does not suppress logicals, decoding a logical node
// into a base typed target lands identically to a no-custom decode. The base
// deserializer fills the target natively and the fall-through re-decodes
// through it. A box-into-any placement cannot: its probe holds the enriched
// type no base-kind setter accepts. The main skip matrix holds the target equal
// to the encode value's own type, so we cross the foreclosed cell here.
func TestMatrix_CustomSkipDecodeLogicalIntoBaseTypedTarget(t *testing.T) {
	skip := avro.CustomType{Decode: func(any, *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType }}
	rows := []struct {
		name   string
		schema string
		enc    any
		mk     func() any
	}{
		{"date->int32", `{"type":"int","logicalType":"date"}`, int32(19000), func() any { return new(int32) }},
		{"timestamp-millis->int64", `{"type":"long","logicalType":"timestamp-millis"}`, int64(1600000000000), func() any { return new(int64) }},
		{"time-micros->int64", `{"type":"long","logicalType":"time-micros"}`, int64(3600000000), func() any { return new(int64) }},
		{"duration->array12", `{"type":"fixed","name":"D","size":12,"logicalType":"duration"}`, avro.Duration{Months: 1, Days: 2, Milliseconds: 3}, func() any { return new([12]byte) }},
		{"decimal->bytes", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`, big.NewRat(1234, 100), func() any { return new([]byte) }},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			plain := avro.MustParse(r.schema)
			sskip := avro.MustParse(r.schema, skip)
			bin := avrotest.MustEncode(t, plain, r.enc)
			jsonw := avrotest.MustEncodeJSON(t, plain, r.enc)
			no := r.mk()
			if _, err := plain.Decode(bin, no); err != nil {
				t.Fatalf("no-custom binary: %v", err)
			}
			sk := r.mk()
			if _, err := sskip.Decode(bin, sk); err != nil {
				t.Fatalf("skip-custom binary errored where no-custom succeeded: %v", err)
			}
			if !reflect.DeepEqual(no, sk) {
				t.Errorf("binary: skip-custom=%v != no-custom=%v", sk, no)
			}
			noj := r.mk()
			if err := plain.DecodeJSON(jsonw, noj); err != nil {
				t.Fatalf("no-custom json: %v", err)
			}
			skj := r.mk()
			if err := sskip.DecodeJSON(jsonw, skj); err != nil {
				t.Fatalf("skip-custom json errored where no-custom succeeded: %v", err)
			}
			if !reflect.DeepEqual(noj, skj) {
				t.Errorf("json: skip-custom=%v != no-custom=%v", skj, noj)
			}
		})
	}
}

// TestMatrix_CustomSkipDecodeTaggedUnionIntoAny pins that a wildcard all-skip
// custom decode into an interface target under TaggedUnions reproduces the
// {branch: value} envelope a no-custom decode emits. A fresh interface target
// goes straight through the base deserializer with TaggedUnions in force, so
// the envelope is produced natively. The main skip matrix decodes only into
// typed targets, which maybeWrap never tags, so this axis was unnetted.
func TestMatrix_CustomSkipDecodeTaggedUnionIntoAny(t *testing.T) {
	skip := avro.CustomType{Decode: func(any, *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType }}
	p := func(x int32) *int32 { return &x }

	rows := []struct {
		name   string
		schema string
		value  any
	}{
		{"null-first", `{"type":"record","name":"R","fields":[{"name":"u","type":["null","int"]}]}`, struct {
			U *int32 `avro:"u"`
		}{U: p(7)}},
		{"null-second", `{"type":"record","name":"R","fields":[{"name":"u","type":["int","null"]}]}`, struct {
			U *int32 `avro:"u"`
		}{U: p(7)}},
		{"multibranch-distinct", `{"type":"record","name":"R","fields":[{"name":"u","type":["int","string"]}]}`, struct {
			U string `avro:"u"`
		}{U: "hi"}},
		{"array-of-nullunion", `{"type":"array","items":["null","int"]}`, []*int32{p(7), nil}},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			plain := avro.MustParse(r.schema)
			sskip := avro.MustParse(r.schema, skip)
			bin := avrotest.MustEncode(t, plain, r.value)
			jsonw := avrotest.MustEncodeJSON(t, plain, r.value)
			var nb any
			avrotest.MustDecode(t, plain, bin, &nb, avro.TaggedUnions())
			var sb any
			avrotest.MustDecode(t, sskip, bin, &sb, avro.TaggedUnions())
			if !reflect.DeepEqual(nb, sb) {
				t.Errorf("binary: skip-custom=%#v != no-custom=%#v", sb, nb)
			}
			var nj any
			avrotest.MustDecodeJSON(t, plain, jsonw, &nj, avro.TaggedUnions())
			var sj any
			avrotest.MustDecodeJSON(t, sskip, jsonw, &sj, avro.TaggedUnions())
			if !reflect.DeepEqual(nj, sj) {
				t.Errorf("json: skip-custom=%#v != no-custom=%#v", sj, nj)
			}
		})
	}
}

// TestRegression_CustomSkipDecodeChainInputUntagged pins the custom decoder
// chain's input contract. The chain receives the probe value decoded with the
// caller's options in force. With no TaggedUnions that is the raw untagged
// value a no-custom decode into `any` produces. The custom records its input
// and we compare it to that oracle, so feeding the chain a tagged envelope or
// any transformed shape reds it.
func TestRegression_CustomSkipDecodeChainInputUntagged(t *testing.T) {
	var captured any
	rec := avro.CustomType{
		AvroType: "record",
		Decode: func(v any, sn *avro.SchemaNode) (any, error) {
			captured = v
			return nil, avro.ErrSkipCustomType
		},
	}
	schema := `{"type":"record","name":"R","fields":[` +
		`{"name":"u","type":["null","int"]},` +
		`{"name":"s","type":"string"},` +
		`{"name":"arr","type":{"type":"array","items":["null","long"]}}]}`
	plain := avro.MustParse(schema)
	s := avro.MustParse(schema, rec)

	type Rw struct {
		U   *int32   `avro:"u"`
		S   string   `avro:"s"`
		Arr []*int64 `avro:"arr"`
	}
	x32, x64 := int32(7), int64(9)
	in := Rw{U: &x32, S: "hi", Arr: []*int64{&x64, nil}}
	bin := avrotest.MustEncode(t, plain, in)
	jsonw := avrotest.MustEncodeJSON(t, plain, in)

	// Oracle: an untagged no-custom decode into any, what the chain sees.
	var oracle any
	avrotest.MustDecode(t, plain, bin, &oracle)

	captured = nil
	var sinkB any
	avrotest.MustDecode(t, s, bin, &sinkB)
	if !reflect.DeepEqual(captured, oracle) {
		t.Errorf("binary: custom chain saw\n %#v\n want untagged\n %#v", captured, oracle)
	}

	captured = nil
	var sinkJ any
	avrotest.MustDecodeJSON(t, s, jsonw, &sinkJ)
	if !reflect.DeepEqual(captured, oracle) {
		t.Errorf("json: custom chain saw\n %#v\n want untagged\n %#v", captured, oracle)
	}
}

// TestRegression_CustomSkipDecodeOverlappingUnion pins that the all-skip path
// recovers the exact wire branch of an overlapping same-symbol union by
// re-decoding the wire. The branch index comes from the wire itself, not a
// guess. An enum-union into an int-ordinal target gets the wire branch's
// ordinal, and into a tagged-any its name. A fall-through that places a probe
// value instead reds both: the ordinal arm cannot derive the right ordinal from
// an untagged probe, and the any arm mis-tags.
func TestRegression_CustomSkipDecodeOverlappingUnion(t *testing.T) {
	skip := avro.CustomType{Decode: func(any, *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType }}

	t.Run("ordinal-target", func(t *testing.T) {
		// EnumA "X"@0, EnumB "X"@1; the wire selects EnumB.
		schema := `{"type":"array","items":[` +
			`{"type":"enum","name":"EnumA","symbols":["X","Y"]},` +
			`{"type":"enum","name":"EnumB","symbols":["P","X"]}]}`
		plain := avro.MustParse(schema)
		sskip := avro.MustParse(schema, skip)

		// binary: array[1] = union branch 1 (EnumB) + enum idx 1 ("X"), end block.
		bin := []byte{0x02, 0x02, 0x02, 0x00}
		var nb, sb []int32
		avrotest.MustDecode(t, plain, bin, &nb)
		avrotest.MustDecode(t, sskip, bin, &sb)
		if !reflect.DeepEqual(nb, sb) || len(nb) != 1 || nb[0] != 1 {
			t.Errorf("binary ordinal: skip=%v no-custom=%v (want [1] = EnumB ordinal of X)", sb, nb)
		}

		// json: tagged-union spec form selecting EnumB.
		jsonw := []byte(`[{"EnumB":"X"}]`)
		var nj, sj []int32
		avrotest.MustDecodeJSON(t, plain, jsonw, &nj)
		avrotest.MustDecodeJSON(t, sskip, jsonw, &sj)
		if !reflect.DeepEqual(nj, sj) || len(nj) != 1 || nj[0] != 1 {
			t.Errorf("json ordinal: skip=%v no-custom=%v (want [1])", sj, nj)
		}
	})

	t.Run("tagged-any-target", func(t *testing.T) {
		schema := `{"type":"record","name":"R","fields":[{"name":"e","type":[` +
			`{"type":"enum","name":"EA","symbols":["X","Y"]},` +
			`{"type":"enum","name":"EB","symbols":["P","X"]}]}]}`
		plain := avro.MustParse(schema)
		sskip := avro.MustParse(schema, skip)

		// binary: record -> union branch 1 (EB) -> enum idx 1 ("X").
		bin := []byte{0x02, 0x02}
		var nb, sb any
		avrotest.MustDecode(t, plain, bin, &nb, avro.TaggedUnions())
		avrotest.MustDecode(t, sskip, bin, &sb, avro.TaggedUnions())
		if !reflect.DeepEqual(nb, sb) {
			t.Errorf("binary tagged-any: skip=%#v no-custom=%#v", sb, nb)
		}

		jsonw := []byte(`{"e":{"EB":"X"}}`)
		var nj, sj any
		avrotest.MustDecodeJSON(t, plain, jsonw, &nj, avro.TaggedUnions())
		avrotest.MustDecodeJSON(t, sskip, jsonw, &sj, avro.TaggedUnions())
		if !reflect.DeepEqual(nj, sj) {
			t.Errorf("json tagged-any: skip=%#v no-custom=%#v", sj, nj)
		}
	})
}

// TestRegression_CustomSkipDecodeLogicalSuppression crosses the
// logical-suppression axis through the all-skip path. A LogicalType-matching
// custom suppresses the built-in logical (hasMatchingCustomType), so a skip
// falls through to the raw Avro-native value. That is identical to a
// no-callback (Decode==nil) LogicalType custom, which suppresses the same way.
// A wildcard custom does *not* suppress, so a skip preserves the enriched
// value. The all-skip placement must honor each.
func TestRegression_CustomSkipDecodeLogicalSuppression(t *testing.T) {
	schema := `{"type":"long","logicalType":"timestamp-millis"}`
	matchSkip := avro.CustomType{LogicalType: "timestamp-millis", Decode: func(any, *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType }}
	matchRaw := avro.CustomType{LogicalType: "timestamp-millis"} // Decode==nil suppresses, giving raw
	wildSkip := avro.CustomType{Decode: func(any, *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType }}

	wire := avrotest.MustEncode(t, avro.MustParse(schema), time.UnixMilli(1600000000000).UTC())

	dec := func(opts ...avro.SchemaOpt) any {
		var got any
		avrotest.MustDecode(t, avro.MustParse(schema, opts...), wire, &got)
		return got
	}

	matchSkipVal := dec(matchSkip)
	matchRawVal := dec(matchRaw)
	wildSkipVal := dec(wildSkip)
	noCustomVal := func() any {
		var got any
		avro.MustParse(schema).Decode(wire, &got)
		return got
	}()

	// LogicalType-matching skip == no-callback (both suppress to raw int64).
	if !reflect.DeepEqual(matchSkipVal, matchRawVal) {
		t.Errorf("matching skip %T(%v) != no-callback %T(%v)", matchSkipVal, matchSkipVal, matchRawVal, matchRawVal)
	}
	if _, ok := matchSkipVal.(int64); !ok {
		t.Errorf("suppressed logical: want raw int64, got %T", matchSkipVal)
	}
	// Wildcard skip preserves the logical == no-custom (enriched time.Time).
	if !reflect.DeepEqual(wildSkipVal, noCustomVal) {
		t.Errorf("wildcard skip %T(%v) != no-custom %T(%v)", wildSkipVal, wildSkipVal, noCustomVal, noCustomVal)
	}
	if _, ok := wildSkipVal.(time.Time); !ok {
		t.Errorf("non-suppressing wildcard: want enriched time.Time, got %T", wildSkipVal)
	}
}

// ---------- lax_internal_reparse_test.go ----------

// Internal re-parse surfaces vs user lax names. Two sites re-parse
// library-produced schema text: Resolve's custom-free writer view and
// SchemaCache.Parse's rebuild from the spliced JSON. WithLaxNames(nil) does not
// subsume any user validator, being stricter for empty name components. That is
// the only class lax(nil) rejects that a user fn can accept. The original parse
// already validated those names, so both sites use internalReparseNames.

// ctLong is a decode-side domain type so the custom wiring in these tests
// is observable, not just registered.
type ctLong struct{ V int64 }

func ctLongDecodeOnly() avro.CustomType {
	return avro.CustomType{
		AvroType: "long",
		Decode: func(v any, _ *avro.SchemaNode) (any, error) {
			n, ok := v.(int64)
			if !ok {
				return nil, fmt.Errorf("ctLong decode: got %T", v)
			}
			return ctLong{n}, nil
		},
	}
}

// The lax-view finding, site 1 (resolve.go's custom-free writer view). A
// custom-typed writer parsed with a user WithLaxNames fn accepting empty name
// components is already-parsed, wire-valid text. Resolve's re-parse of
// writer.full must not re-litigate those names. A WithLaxNames(nil) re-parse
// rejects the empty component and hard-fails Resolve, blocking binary
// resolution too. The reader differs from the writer, so Resolve's canonical
// fast path cannot mask the construction.
func TestRegression_ResolveCustomTypedLaxWriterView(t *testing.T) {
	acceptAll := func(string) error { return nil }
	writerJSON := `{"type":"record","name":"R","namespace":"a..b","fields":[{"name":"f","type":"long"}]}`
	readerJSON := `{"type":"record","name":"R","namespace":"a..b","fields":[{"name":"f","type":"long"},{"name":"g","type":"string","default":"x"}]}`

	writer, err := avro.Parse(writerJSON, avro.WithLaxNames(acceptAll), ctLongDecodeOnly())
	if err != nil {
		t.Fatalf("writer parse: %v", err)
	}
	reader, err := avro.Parse(readerJSON, avro.WithLaxNames(acceptAll), ctLongDecodeOnly())
	if err != nil {
		t.Fatalf("reader parse: %v", err)
	}

	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatalf("Resolve of an already-parsed lax-named custom-typed writer must succeed: %v", err)
	}

	// The writer's names pass through verbatim, so we get parity with the
	// no-custom twin on wire bytes and fingerprint. The custom is decode-only,
	// and the canonical form ignores custom registrations either way.
	writerNC, err := avro.Parse(writerJSON, avro.WithLaxNames(acceptAll))
	if err != nil {
		t.Fatalf("no-custom writer parse: %v", err)
	}
	in := map[string]any{"f": int64(7)}
	wire, err := writer.Encode(in)
	if err != nil {
		t.Fatalf("writer encode: %v", err)
	}
	wireNC, err := writerNC.Encode(in)
	if err != nil {
		t.Fatalf("no-custom writer encode: %v", err)
	}
	if !bytes.Equal(wire, wireNC) {
		t.Errorf("wire bytes diverge from no-custom twin: %x vs %x", wire, wireNC)
	}
	if fp, fpNC := writer.Fingerprint(avro.NewRabin()), writerNC.Fingerprint(avro.NewRabin()); !bytes.Equal(fp, fpNC) {
		t.Errorf("rabin fingerprint diverges from no-custom twin: %x vs %x", fp, fpNC)
	}
	if canon := string(writer.Canonical()); !strings.Contains(canon, `"a..b.R"`) {
		t.Errorf("canonical does not carry the lax fullname verbatim: %s", canon)
	}

	// End-to-end through the resolved schema: binary decode and resolved
	// DecodeJSON (the path the custom-free view exists for) agree. The
	// reader's decode-only custom fires, and the added field defaults.
	want := map[string]any{"f": ctLong{7}, "g": "x"}
	var viaBinary map[string]any
	if _, err := resolved.Decode(wire, &viaBinary); err != nil {
		t.Fatalf("resolved binary decode: %v", err)
	}
	if !reflect.DeepEqual(viaBinary, want) {
		t.Errorf("resolved binary decode: got %#v, want %#v", viaBinary, want)
	}
	var viaJSON map[string]any
	if err := resolved.DecodeJSON([]byte(`{"f":7}`), &viaJSON); err != nil {
		t.Fatalf("resolved DecodeJSON: %v", err)
	}
	if !reflect.DeepEqual(viaJSON, want) {
		t.Errorf("resolved DecodeJSON: got %#v, want %#v", viaJSON, want)
	}
}

// The lax-view finding, site 2 (cache.go's splice-rebuild retry), in its
// transitive form. Parse-1 defines a..b.Inner under a user lax fn. Parses 2 and
// 3 pass no lax option, referencing the name only through the cache. Two
// rebuild attempts that reject the spliced form, first strict and then
// WithLaxNames(nil) on the empty component, degrade the metadata silently to a
// dangling reference unresolvable under any opts. The spliced forms must
// survive, re-parse under the user's validator, and match the directly-parsed
// twin byte-for-byte.
func TestRegression_CacheSpliceTransitiveLaxNames(t *testing.T) {
	acceptAll := func(string) error { return nil }
	var c avro.SchemaCache
	if _, err := c.Parse(`{"type":"record","name":"Inner","namespace":"a..b","fields":[{"name":"f","type":"long"}]}`, avro.WithLaxNames(acceptAll)); err != nil {
		t.Fatalf("parse-1 (lax define): %v", err)
	}
	s2, err := c.Parse(`{"type":"record","name":"Wrapper","namespace":"ok","fields":[{"name":"i","type":"a..b.Inner"}]}`)
	if err != nil {
		t.Fatalf("parse-2 (strict reference): %v", err)
	}
	s3, err := c.Parse(`{"type":"record","name":"Outer","fields":[{"name":"w","type":"ok.Wrapper"}]}`)
	if err != nil {
		t.Fatalf("parse-3 (strict transitive reference): %v", err)
	}

	// The wire path is a control here, not the finding.
	in := map[string]any{"w": map[string]any{"i": map[string]any{"f": int64(7)}}}
	wire, err := s3.Encode(in)
	if err != nil {
		t.Fatalf("cache-parsed encode: %v", err)
	}

	// String() must be self-contained: standalone re-parse under the
	// user's validator. A dangling reference reports unknown type
	// "ok.Wrapper" instead.
	re, err := avro.Parse(s3.String(), avro.WithLaxNames(acceptAll))
	if err != nil {
		t.Fatalf("parse-3 String() must re-parse self-contained: %v\nString(): %s", err, s3.String())
	}
	if !bytes.Equal(re.Canonical(), s3.Canonical()) {
		t.Errorf("String() re-parse canonical diverges:\n re: %s\n s3: %s", re.Canonical(), s3.Canonical())
	}
	// Canonical() re-parses too (it is valid schema JSON).
	if _, err := avro.Parse(string(s3.Canonical()), avro.WithLaxNames(acceptAll)); err != nil {
		t.Fatalf("parse-3 Canonical() must re-parse: %v\nCanonical(): %s", err, s3.Canonical())
	}
	// Parse-2's metadata forms are equally self-contained.
	if _, err := avro.Parse(s2.String(), avro.WithLaxNames(acceptAll)); err != nil {
		t.Fatalf("parse-2 String() must re-parse self-contained: %v\nString(): %s", err, s2.String())
	}

	// Parity with the directly-parsed twin: same schema, spliced by hand.
	twin, err := avro.Parse(`{"type":"record","name":"Outer","fields":[{"name":"w","type":{"type":"record","name":"Wrapper","namespace":"ok","fields":[{"name":"i","type":{"type":"record","name":"Inner","namespace":"a..b","fields":[{"name":"f","type":"long"}]}}]}}]}`, avro.WithLaxNames(acceptAll))
	if err != nil {
		t.Fatalf("twin parse: %v", err)
	}
	if !bytes.Equal(s3.Canonical(), twin.Canonical()) {
		t.Errorf("canonical diverges from directly-parsed twin:\n got: %s\nwant: %s", s3.Canonical(), twin.Canonical())
	}
	if fp, fpTwin := s3.Fingerprint(avro.NewRabin()), twin.Fingerprint(avro.NewRabin()); !bytes.Equal(fp, fpTwin) {
		t.Errorf("rabin fingerprint diverges from directly-parsed twin: %x vs %x", fp, fpTwin)
	}
	wireTwin, err := twin.Encode(in)
	if err != nil {
		t.Fatalf("twin encode: %v", err)
	}
	if !bytes.Equal(wire, wireTwin) {
		t.Errorf("wire bytes diverge from directly-parsed twin: %x vs %x", wire, wireTwin)
	}
	// Names pass through verbatim in the spliced text.
	if s := s3.String(); !strings.Contains(s, `"a..b"`) {
		t.Errorf("String() does not carry the lax namespace verbatim: %s", s)
	}
}

// Siblings of the canonical empty-name emission fix, in the metadata rebuild.
// toJSONWalk guards its name-key, namespace, cycle-reference and dedup arms
// with Name != "", and three more helpers share that idiom. All conflate
// "structurally unnamed node" with "named kind whose short name is empty".
// Reachable damage is threefold. The "ok." class rebuilds to the wrong schema
// silently. Recursive and diamond shapes hard-fail the rebuilt re-parse. A
// named child inside an empty-named parent loses its inherited scope. The named
// kind, or a non-empty fullname where a reference must exist, is the
// distinction.
func TestMatrix_SchemaNodeRebuildEmptyNames(t *testing.T) {
	acceptAll := func(string) error { return nil }
	for _, c := range []struct{ desc, js string }{
		{"bare", `{"type":"record","name":"","fields":[{"name":"f","type":"long"}]}`},
		{"ok", `{"type":"record","name":"","namespace":"ok","fields":[{"name":"f","type":"long"}]}`},
		{"ab", `{"type":"record","name":"R","namespace":"a..b","fields":[{"name":"f","type":"long"}]}`},
		{"recursive-ok", `{"type":"record","name":"","namespace":"ok","fields":[{"name":"f","type":"long"},{"name":"next","type":["null","ok."]}]}`},
		{"diamond-ok", `{"type":"record","name":"Top","fields":[{"name":"a","type":{"type":"record","name":"","namespace":"ok","fields":[{"name":"f","type":"long"}]}},{"name":"b","type":"ok."}]}`},
		{"nested-child-in-ok", `{"type":"record","name":"","namespace":"ok","fields":[{"name":"c","type":{"type":"record","name":"Child","fields":[{"name":"f","type":"long"}]}}]}`},
	} {
		t.Run(c.desc, func(t *testing.T) {
			s, err := avro.Parse(c.js, avro.WithLaxNames(acceptAll))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			root := s.Root()
			re, err := root.Schema(avro.WithLaxNames(acceptAll))
			if err != nil {
				t.Fatalf("Root().Schema() rebuild: %v", err)
			}
			if !bytes.Equal(re.Canonical(), s.Canonical()) {
				t.Errorf("rebuilt schema diverges:\n orig %s\n rebuilt %s", s.Canonical(), re.Canonical())
			}
		})
	}
}

// The reader-side twin of the customBaked writer-trigger fix: resolved decode
// dropping the reader's custom on SchemaCache-inherited subtrees. resolveNode
// re-applies reader customs to rebuilt nodes through resolveCtx.custom. A cache
// parse's overlay missing entries for inherited nodes then makes a resolution
// against a pre-evolution writer silently return raw values where the direct
// decode returns wrapped ones. tryAssignNamedRef completes the overlay.
func TestRegression_ResolvedDecodeCacheInheritedReaderCustom(t *testing.T) {
	var c avro.SchemaCache
	if _, err := c.Parse(`{"type":"record","name":"Inner","fields":[{"name":"f","type":"long"}]}`, ctLongDecodeOnly()); err != nil {
		t.Fatalf("cache define: %v", err)
	}
	reader, err := c.Parse(`{"type":"record","name":"Outer","fields":[{"name":"i","type":"Inner"},{"name":"added","type":"string","default":"x"}]}`, ctLongDecodeOnly())
	if err != nil {
		t.Fatalf("cache reader parse: %v", err)
	}
	writer, err := avro.Parse(`{"type":"record","name":"Outer","fields":[{"name":"i","type":{"type":"record","name":"Inner","fields":[{"name":"f","type":"long"}]}}]}`)
	if err != nil {
		t.Fatalf("writer parse: %v", err)
	}
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	want := map[string]any{"i": map[string]any{"f": ctLong{7}}, "added": "x"}

	// The parity at stake: resolved decode must equal the direct decode,
	// same value and same type.
	directWire, err := reader.Encode(map[string]any{"i": map[string]any{"f": int64(7)}, "added": "x"})
	if err != nil {
		t.Fatalf("direct encode: %v", err)
	}
	var direct map[string]any
	if _, err := reader.Decode(directWire, &direct); err != nil {
		t.Fatalf("direct decode: %v", err)
	}
	if !reflect.DeepEqual(direct, want) {
		t.Fatalf("direct decode (control): got %#v, want %#v", direct, want)
	}

	in := map[string]any{"i": map[string]any{"f": int64(7)}}
	wire, err := writer.Encode(in)
	if err != nil {
		t.Fatalf("writer encode: %v", err)
	}
	var viaBinary map[string]any
	if _, err := resolved.Decode(wire, &viaBinary); err != nil {
		t.Fatalf("resolved binary decode: %v", err)
	}
	if !reflect.DeepEqual(viaBinary, want) {
		t.Errorf("resolved binary decode dropped the reader custom: got %#v, want %#v", viaBinary, want)
	}
	wjson, err := writer.EncodeJSON(in)
	if err != nil {
		t.Fatalf("writer EncodeJSON: %v", err)
	}
	var viaJSON map[string]any
	if err := resolved.DecodeJSON(wjson, &viaJSON); err != nil {
		t.Fatalf("resolved DecodeJSON: %v", err)
	}
	if !reflect.DeepEqual(viaJSON, want) {
		t.Errorf("resolved DecodeJSON dropped the reader custom: got %#v, want %#v", viaJSON, want)
	}
}

// Control: a bare-reference-as-whole-schema cache parse keeps the defining
// parse's custom behavior. The composed ser/deser of the inherited named type
// carries the callback wraps. That holds on direct decode, as a custom-typed
// writer through Resolve (customBaked fires through the inherited hadCustomType
// stamp), and on resolved DecodeJSON.
func TestRegression_BareRefWriterCustomControl(t *testing.T) {
	var c avro.SchemaCache
	if _, err := c.Parse(`{"type":"record","name":"Inner","fields":[{"name":"f","type":"long"}]}`, ctLongDecodeOnly()); err != nil {
		t.Fatalf("cache define: %v", err)
	}
	bare, err := c.Parse(`"Inner"`, ctLongDecodeOnly())
	if err != nil {
		t.Fatalf("bare ref parse: %v", err)
	}
	wire, err := bare.Encode(map[string]any{"f": int64(7)})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got map[string]any
	avrotest.MustDecode(t, bare, wire, &got)
	if want := map[string]any{"f": ctLong{7}}; !reflect.DeepEqual(got, want) {
		t.Errorf("bare-ref direct decode: got %#v, want %#v", got, want)
	}
	reader, err := avro.Parse(`{"type":"record","name":"Inner","fields":[{"name":"f","type":"long"},{"name":"added","type":"string","default":"x"}]}`, ctLongDecodeOnly())
	if err != nil {
		t.Fatalf("reader parse: %v", err)
	}
	resolved, err := avro.Resolve(bare, reader)
	if err != nil {
		t.Fatalf("Resolve with bare-ref writer: %v", err)
	}
	var viaJSON map[string]any
	if err := resolved.DecodeJSON([]byte(`{"f":7}`), &viaJSON); err != nil {
		t.Fatalf("resolved DecodeJSON: %v", err)
	}
	if want := map[string]any{"f": ctLong{7}, "added": "x"}; !reflect.DeepEqual(viaJSON, want) {
		t.Errorf("resolved DecodeJSON via bare-ref writer: got %#v, want %#v", viaJSON, want)
	}
}

// Sibling of the lax-view finding, in the splice walkers rather than the
// re-parse validator. collectTreeDefs / inlineTreeDefs guard named-type
// definitions with `name != ""`. That conflates "no name key", an unnamed node,
// with "name key present and empty", a definition a user lax validator accepts.
// An empty short name with a namespace has fullname "ok.", dotted and hence
// referenceable across cache parses. Leaving its definition out of the def
// table keeps the splice from firing, degrading the metadata to the dangling
// reference even with the accept-all re-parse validator in place.
func TestRegression_CacheSpliceEmptyShortName(t *testing.T) {
	acceptAll := func(string) error { return nil }
	var c avro.SchemaCache
	if _, err := c.Parse(`{"type":"record","name":"","namespace":"ok","fields":[{"name":"f","type":"long"}]}`, avro.WithLaxNames(acceptAll)); err != nil {
		t.Fatalf("parse-1 (empty short name define): %v", err)
	}
	s2, err := c.Parse(`{"type":"record","name":"Outer","fields":[{"name":"i","type":"ok."}]}`)
	if err != nil {
		t.Fatalf("parse-2 (strict reference): %v", err)
	}
	re, err := avro.Parse(s2.String(), avro.WithLaxNames(acceptAll))
	if err != nil {
		t.Fatalf("String() must re-parse self-contained: %v\nString(): %s", err, s2.String())
	}
	if !bytes.Equal(re.Canonical(), s2.Canonical()) {
		t.Errorf("String() re-parse canonical diverges:\n re: %s\n s2: %s", re.Canonical(), s2.Canonical())
	}
	twin, err := avro.Parse(`{"type":"record","name":"Outer","fields":[{"name":"i","type":{"type":"record","name":"","namespace":"ok","fields":[{"name":"f","type":"long"}]}}]}`, avro.WithLaxNames(acceptAll))
	if err != nil {
		t.Fatalf("twin parse: %v", err)
	}
	if !bytes.Equal(s2.Canonical(), twin.Canonical()) {
		t.Errorf("canonical diverges from directly-parsed twin:\n got: %s\nwant: %s", s2.Canonical(), twin.Canonical())
	}
	if fp, fpTwin := s2.Fingerprint(avro.NewRabin()), twin.Fingerprint(avro.NewRabin()); !bytes.Equal(fp, fpTwin) {
		t.Errorf("rabin fingerprint diverges from directly-parsed twin: %x vs %x", fp, fpTwin)
	}
	in := map[string]any{"i": map[string]any{"f": int64(7)}}
	assertTwinWire(t, s2, twin, in)
}

// AUDIT_PATTERNS.md B7 second instance, the stale-splice arm. A keyless
// definition registers the parser fullname "x." and builds its children under
// x, so the nested Inner is x.Inner. Gating both the def visit and the child
// namespace scope on a string "name" key being present misfiles parse-1's
// nested definition under the enclosing-scoped "Inner". A later parse
// referencing the short name *before* locally defining a different Inner binds
// to the local one. The splice walker then finds the misfiled stale def and
// rewrites that definition to a reference, shipping metadata describing a field
// the wire rejects.
func TestRegression_CacheKeylessDefStaleSplice(t *testing.T) {
	acceptAll := func(string) error { return nil }
	var c avro.SchemaCache
	if _, err := c.Parse(`{"type":"record","namespace":"x","fields":[{"name":"i","type":{"type":"record","name":"Inner","fields":[{"name":"w","type":"long"}]}}]}`, avro.WithLaxNames(acceptAll)); err != nil {
		t.Fatalf("parse-1 (keyless define): %v", err)
	}
	src := `{"type":"record","name":"Outer2","fields":[{"name":"a","type":"Inner"},{"name":"b","type":{"type":"record","name":"Inner","fields":[{"name":"z","type":"string"}]}}]}`
	writer, err := c.Parse(src)
	if err != nil {
		t.Fatalf("parse-2 (reference-then-define): %v", err)
	}
	// The text is self-contained (the reference forward-binds the local
	// definition), so the cache-less parse of the same bytes is the twin.
	twin, err := avro.Parse(src)
	if err != nil {
		t.Fatalf("twin parse: %v", err)
	}
	if !bytes.Equal(writer.Canonical(), twin.Canonical()) {
		t.Errorf("canonical diverges from directly-parsed twin:\n got: %s\nwant: %s", writer.Canonical(), twin.Canonical())
	}
	if fp, fpTwin := writer.Fingerprint(avro.NewRabin()), twin.Fingerprint(avro.NewRabin()); !bytes.Equal(fp, fpTwin) {
		t.Errorf("rabin fingerprint diverges from directly-parsed twin: %x vs %x", fp, fpTwin)
	}
	re, err := avro.Parse(writer.String())
	if err != nil {
		t.Fatalf("String() must re-parse standalone: %v\nString(): %s", err, writer.String())
	}
	if !bytes.Equal(re.Canonical(), twin.Canonical()) {
		t.Errorf("String() re-parse describes a different schema than the wire codec:\n re: %s\nwant: %s", re.Canonical(), twin.Canonical())
	}
	// Root(): field a is the bare forward reference, field b the local
	// definition carrying the string field z (the schema the wire
	// implements). A stale splice inverts this, giving field a the
	// inherited Inner{w:long} definition and rewriting field b to a
	// reference.
	root := writer.Root()
	if got := root.Fields[0].Type.Type; got != "Inner" {
		t.Errorf("Root() field a: got type %q, want the bare reference %q", got, "Inner")
	}
	fb := root.Fields[1].Type
	if fb.Type != "record" || len(fb.Fields) != 1 || fb.Fields[0].Name != "z" || fb.Fields[0].Type.Type != "string" {
		t.Errorf("Root() field b: got %s %v, want the local record definition with the single string field z", fb.Type, fb.Fields)
	}
	// Wire controls: the codec implements the local Inner{z:string} at both
	// fields and rejects the stale inherited shape.
	in := map[string]any{"a": map[string]any{"z": "p"}, "b": map[string]any{"z": "q"}}
	wire := assertTwinWire(t, writer, twin, in)
	var out map[string]any
	avrotest.MustDecode(t, writer, wire, &out)
	if !reflect.DeepEqual(out, in) {
		t.Errorf("decode round-trip: got %#v, want %#v", out, in)
	}
	if _, err := writer.Encode(map[string]any{"a": map[string]any{"w": int64(7)}, "b": map[string]any{"w": int64(8)}}); err == nil {
		t.Error("wire unexpectedly accepts the stale inherited Inner{w:long} shape")
	}
}

// AUDIT_PATTERNS.md B7 second instance, the cross-parse dangle arm. Parse-1
// (lax) defines x.Inner nested inside a keyless record. Parse-2 passes no lax
// option, transitive reachability being the point. It references the
// parser-scoped fullname, which the wire resolves from the cache's named table.
// A definition misfiled under "Inner" leaves the dotted lookup finding nothing
// and the metadata keeping a dangling reference unresolvable under any opts.
func TestRegression_CacheKeylessDefCrossParseRef(t *testing.T) {
	acceptAll := func(string) error { return nil }
	var c avro.SchemaCache
	if _, err := c.Parse(`{"type":"record","namespace":"x","fields":[{"name":"i","type":{"type":"record","name":"Inner","fields":[{"name":"w","type":"long"}]}}]}`, avro.WithLaxNames(acceptAll)); err != nil {
		t.Fatalf("parse-1 (keyless define): %v", err)
	}
	writer, err := c.Parse(`{"type":"record","name":"Outer2","fields":[{"name":"a","type":"x.Inner"}]}`)
	if err != nil {
		t.Fatalf("parse-2 (cross-parse reference): %v", err)
	}
	re, err := avro.Parse(writer.String())
	if err != nil {
		t.Fatalf("String() must re-parse self-contained: %v\nString(): %s", err, writer.String())
	}
	twin, err := avro.Parse(`{"type":"record","name":"Outer2","fields":[{"name":"a","type":{"type":"record","name":"Inner","namespace":"x","fields":[{"name":"w","type":"long"}]}}]}`)
	if err != nil {
		t.Fatalf("twin parse: %v", err)
	}
	if !bytes.Equal(re.Canonical(), twin.Canonical()) {
		t.Errorf("String() re-parse canonical diverges from twin:\n re: %s\nwant: %s", re.Canonical(), twin.Canonical())
	}
	if !bytes.Equal(writer.Canonical(), twin.Canonical()) {
		t.Errorf("canonical diverges from directly-parsed twin:\n got: %s\nwant: %s", writer.Canonical(), twin.Canonical())
	}
	if fp, fpTwin := writer.Fingerprint(avro.NewRabin()), twin.Fingerprint(avro.NewRabin()); !bytes.Equal(fp, fpTwin) {
		t.Errorf("rabin fingerprint diverges from directly-parsed twin: %x vs %x", fp, fpTwin)
	}
	// Root() describes the spliced definition, not a dangling reference.
	fa := writer.Root().Fields[0].Type
	if fa.Type != "record" || fa.Name != "Inner" || fa.Namespace != "x" || len(fa.Fields) != 1 || fa.Fields[0].Name != "w" {
		t.Errorf("Root() field a: got type=%q name=%q namespace=%q fields=%v, want the spliced x.Inner definition", fa.Type, fa.Name, fa.Namespace, fa.Fields)
	}
	in := map[string]any{"a": map[string]any{"w": int64(7)}}
	wire := assertTwinWire(t, writer, twin, in)
	var out map[string]any
	avrotest.MustDecode(t, writer, wire, &out)
	if !reflect.DeepEqual(out, in) {
		t.Errorf("decode round-trip: got %#v, want %#v", out, in)
	}
}

// AUDIT_PATTERNS.md B7 third instance, arm one: parser self-consistency for
// leading-dot names. One leading dot and no other is the explicit
// null-namespace escape. qualifyAliases already applies that rule to aliases,
// and Java's Name constructor applies it to every name. So {"name":".x"} builds
// as "x" in the null namespace, lax-only. Registering the name verbatim,
// prefixing child registration with parentName[:dot+1] while reference
// resolution uses namespaceOf(".x") = "", makes the parser disagree with
// itself. A bare sibling reference inside ".x" then fails to parse.
func TestRegression_LeadingDotSiblingRefResolves(t *testing.T) {
	acceptAll := func(string) error { return nil }
	src := `{"type":"record","name":".x","fields":[{"name":"k","type":{"type":"record","name":"Inner","fields":[{"name":"f","type":"long"}]}},{"name":"r","type":"Inner"}]}`
	writer, err := avro.Parse(src, avro.WithLaxNames(acceptAll))
	if err != nil {
		t.Fatalf("bare sibling reference inside a leading-dot name must parse: %v", err)
	}
	// The ".x" spelling and the plain "x" spelling are the same type.
	twin, err := avro.Parse(`{"type":"record","name":"x","fields":[{"name":"k","type":{"type":"record","name":"Inner","fields":[{"name":"f","type":"long"}]}},{"name":"r","type":"Inner"}]}`)
	if err != nil {
		t.Fatalf("twin parse: %v", err)
	}
	if !bytes.Equal(writer.Canonical(), twin.Canonical()) {
		t.Errorf("canonical diverges from the plain-spelled twin:\n got: %s\nwant: %s", writer.Canonical(), twin.Canonical())
	}
	if fp, fpTwin := writer.Fingerprint(avro.NewRabin()), twin.Fingerprint(avro.NewRabin()); !bytes.Equal(fp, fpTwin) {
		t.Errorf("rabin fingerprint diverges from the plain-spelled twin: %x vs %x", fp, fpTwin)
	}
	in := map[string]any{"k": map[string]any{"f": int64(7)}, "r": map[string]any{"f": int64(8)}}
	wire, err := writer.Encode(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	wireTwin, err := twin.Encode(in)
	if err != nil {
		t.Fatalf("twin encode: %v", err)
	}
	if !bytes.Equal(wire, wireTwin) {
		t.Errorf("wire bytes diverge from the plain-spelled twin: %x vs %x", wire, wireTwin)
	}
}

// AUDIT_PATTERNS.md B7 third instance, arm two: a cross-parse ".x" reference
// splices self-contained. Parse-1 defines {"name":".x"} under lax. The def
// collectors store it under the collapsed fullname "x", nodeFullnameTree's
// split-rejoin implementing exactly the Name-ctor rule. A parser registering
// ".x" verbatim, with scopedRefKeys looking the reference up verbatim, makes
// the exact dotted lookup miss the def table and the metadata keep the dangling
// reference. Both must normalize to "x" so the splice fires and the spliced
// form is strict-parseable.
func TestRegression_LeadingDotCrossParseRefSplices(t *testing.T) {
	acceptAll := func(string) error { return nil }
	var c avro.SchemaCache
	if _, err := c.Parse(`{"type":"record","name":".x","fields":[{"name":"w","type":"long"}]}`, avro.WithLaxNames(acceptAll)); err != nil {
		t.Fatalf("parse-1 (leading-dot define): %v", err)
	}
	writer, err := c.Parse(`{"type":"record","name":"Outer2","fields":[{"name":"a","type":".x"}]}`)
	if err != nil {
		t.Fatalf("parse-2 (cross-parse reference): %v", err)
	}
	re, err := avro.Parse(writer.String())
	if err != nil {
		t.Fatalf("String() must re-parse self-contained: %v\nString(): %s", err, writer.String())
	}
	twin, err := avro.Parse(`{"type":"record","name":"Outer2","fields":[{"name":"a","type":{"type":"record","name":"x","fields":[{"name":"w","type":"long"}]}}]}`)
	if err != nil {
		t.Fatalf("twin parse: %v", err)
	}
	if !bytes.Equal(re.Canonical(), twin.Canonical()) {
		t.Errorf("String() re-parse canonical diverges from twin:\n re: %s\nwant: %s", re.Canonical(), twin.Canonical())
	}
	if !bytes.Equal(writer.Canonical(), twin.Canonical()) {
		t.Errorf("canonical diverges from directly-parsed twin:\n got: %s\nwant: %s", writer.Canonical(), twin.Canonical())
	}
	if fp, fpTwin := writer.Fingerprint(avro.NewRabin()), twin.Fingerprint(avro.NewRabin()); !bytes.Equal(fp, fpTwin) {
		t.Errorf("rabin fingerprint diverges from directly-parsed twin: %x vs %x", fp, fpTwin)
	}
	in := map[string]any{"a": map[string]any{"w": int64(7)}}
	assertTwinWire(t, writer, twin, in)
}

// AUDIT_PATTERNS.md B7 third instance, arm three: the executed stale-splice
// divergence. Registering {"name":".x"} as ".x" in the parser but "x" in the
// def table lets a later parse reference-then-locally-define the bare "x". The
// reference forward-binds to the local x{z:string}, and the stale misfiled def
// then splices in: canonical describes x{w:long} while the wire accepts
// {z:string}. Since ".x" *is* the fullname "x", the local re-definition is a
// duplicate and we reject the parse.
func TestRegression_LeadingDotStaleSpliceHealed(t *testing.T) {
	acceptAll := func(string) error { return nil }
	var c avro.SchemaCache
	if _, err := c.Parse(`{"type":"record","name":".x","fields":[{"name":"w","type":"long"}]}`, avro.WithLaxNames(acceptAll)); err != nil {
		t.Fatalf("parse-1 (leading-dot define): %v", err)
	}
	_, err := c.Parse(`{"type":"record","name":"Outer2","fields":[{"name":"a","type":"x"},{"name":"b","type":{"type":"record","name":"x","fields":[{"name":"z","type":"string"}]}}]}`)
	if err == nil {
		t.Fatal("local re-definition of the cache-inherited fullname x unexpectedly parsed")
	}
	if !strings.Contains(err.Error(), `duplicate named type "x"`) {
		t.Errorf("rejection shape changed: %v", err)
	}
}

// ---------- lax_internal_reparse_matrix_test.go ----------

// Class matrix for the internal re-parse surfaces against the full name class
// the original parse can accept:
//
//	{site: resolve-view, cache-splice}
//	  x {name class: strict, lax-nonempty, empty-component ns, empty-string name}
//	  x {custom: none, decode-only, encode+decode}
//	  x {reference: direct (recursive self-ref), transitive (diamond)}
//	plus a cache cell whose outer parse carries the user lax fn, and pinned
//	verdicts for the structurally-unreferenceable bare "" name.
//
// Every cell is parity with the original parse. Whatever the public parse
// accepted must survive Resolve, the String()/Canonical() re-parse, and
// resolved DecodeJSON, with wire bytes and fingerprints equal to the twin's.
// The reader always differs from the writer, so Resolve's canonical fast path
// cannot mask the writer-view construction.

func ctLongEncDec() avro.CustomType {
	return avro.CustomType{
		AvroType: "long",
		GoType:   reflect.TypeFor[ctLong](),
		Encode: func(v any, _ *avro.SchemaNode) (any, error) {
			c, ok := v.(ctLong)
			if !ok {
				return nil, fmt.Errorf("ctLong encode: got %T", v)
			}
			return c.V, nil
		},
		Decode: func(v any, _ *avro.SchemaNode) (any, error) {
			n, ok := v.(int64)
			if !ok {
				return nil, fmt.Errorf("ctLong decode: got %T", v)
			}
			return ctLong{n}, nil
		},
	}
}

type reparseNameClass struct {
	key  string
	ns   string // namespace of the lax-named type
	name string // short name of the lax-named type ("" = empty short name)
	full string // expected verbatim fullname in canonical/spliced forms
	opt  avro.SchemaOpt
}

type reparseCustomMode struct {
	key  string
	opts []avro.SchemaOpt
	wrap bool // decoded longs surface as ctLong
}

func TestMatrix_InternalReparseLaxNames(t *testing.T) {
	acceptAll := func(string) error { return nil }
	classes := []reparseNameClass{
		{"strict", "com.example", "N", "com.example.N", nil},
		{"laxnonempty", "a-b", "N", "a-b.N", avro.WithLaxNames(nil)},
		{"emptycomponent", "a..b", "N", "a..b.N", avro.WithLaxNames(acceptAll)},
		{"emptyname", "ok", "", "ok.", avro.WithLaxNames(acceptAll)},
	}
	customs := []reparseCustomMode{
		{"none", nil, false},
		{"decodeonly", []avro.SchemaOpt{ctLongDecodeOnly()}, true},
		{"encdec", []avro.SchemaOpt{ctLongEncDec()}, true},
	}
	withCustom := func(nc reparseNameClass, cm reparseCustomMode) []avro.SchemaOpt {
		return append(nameOnlyOpts(nc), cm.opts...)
	}
	L := func(wrap bool, v int64) any {
		if wrap {
			return ctLong{v}
		}
		return v
	}

	// Site: resolve-view. The lax-named type is the writer root with a
	// recursive self-reference (direct) or a nested definition whose name is
	// referenced a second time (transitive; diamond).
	for _, nc := range classes {
		for _, cm := range customs {
			var writerJSON, readerJSON string
			var in, inCt, want map[string]any
			innerDef := fmt.Sprintf(`{"type":"record","name":%q,"namespace":%q,"fields":[{"name":"f","type":"long"}]}`, nc.name, nc.ns)

			t.Run("resolve/"+nc.key+"/direct/"+cm.key, func(t *testing.T) {
				writerJSON = fmt.Sprintf(`{"type":"record","name":%q,"namespace":%q,"fields":[{"name":"f","type":"long"},{"name":"next","type":["null",%q]}]}`, nc.name, nc.ns, nc.full)
				readerJSON = fmt.Sprintf(`{"type":"record","name":%q,"namespace":%q,"fields":[{"name":"f","type":"long"},{"name":"next","type":["null",%q]},{"name":"added","type":"string","default":"x"}]}`, nc.name, nc.ns, nc.full)
				in = map[string]any{"f": int64(7), "next": map[string]any{"f": int64(8), "next": nil}}
				if cm.key == "encdec" {
					inCt = map[string]any{"f": ctLong{7}, "next": map[string]any{"f": ctLong{8}, "next": nil}}
				}
				want = map[string]any{"f": L(cm.wrap, 7), "next": map[string]any{"f": L(cm.wrap, 8), "next": nil, "added": "x"}, "added": "x"}

				writer, err := avro.Parse(writerJSON, withCustom(nc, cm)...)
				if err != nil {
					t.Fatalf("writer parse: %v", err)
				}
				twin, err := avro.Parse(writerJSON, nameOnlyOpts(nc)...)
				if err != nil {
					t.Fatalf("twin parse: %v", err)
				}
				reader, err := avro.Parse(readerJSON, withCustom(nc, cm)...)
				if err != nil {
					t.Fatalf("reader parse: %v", err)
				}
				battery(t, nc, writer, twin, reader, in, inCt, want)
			})

			t.Run("resolve/"+nc.key+"/transitive/"+cm.key, func(t *testing.T) {
				writerJSON = fmt.Sprintf(`{"type":"record","name":"Top","namespace":"root.ns","fields":[{"name":"a","type":%s},{"name":"b","type":%q}]}`, innerDef, nc.full)
				readerJSON = fmt.Sprintf(`{"type":"record","name":"Top","namespace":"root.ns","fields":[{"name":"a","type":%s},{"name":"b","type":%q},{"name":"added","type":"string","default":"x"}]}`, innerDef, nc.full)
				in = map[string]any{"a": map[string]any{"f": int64(7)}, "b": map[string]any{"f": int64(8)}}
				if cm.key == "encdec" {
					inCt = map[string]any{"a": map[string]any{"f": ctLong{7}}, "b": map[string]any{"f": ctLong{8}}}
				}
				want = map[string]any{"a": map[string]any{"f": L(cm.wrap, 7)}, "b": map[string]any{"f": L(cm.wrap, 8)}, "added": "x"}

				writer, err := avro.Parse(writerJSON, withCustom(nc, cm)...)
				if err != nil {
					t.Fatalf("writer parse: %v", err)
				}
				twin, err := avro.Parse(writerJSON, nameOnlyOpts(nc)...)
				if err != nil {
					t.Fatalf("twin parse: %v", err)
				}
				reader, err := avro.Parse(readerJSON, withCustom(nc, cm)...)
				if err != nil {
					t.Fatalf("reader parse: %v", err)
				}
				battery(t, nc, writer, twin, reader, in, inCt, want)
			})
		}
	}

	// Site: cache-splice. The lax-named type is defined in an earlier cache parse
	// and reaches the final parse's metadata only through the splice. The final
	// parse passes no name option, transitive reachability being the point.
	// Custom cells carry the custom on every parse in the chain, the cross-parse
	// custom-boundary guard requiring cache and referencing parse to agree. The
	// cache-parsed writer then feeds Resolve, composing both re-parse sites.
	for _, nc := range classes {
		for _, cm := range customs {
			innerDef := fmt.Sprintf(`{"type":"record","name":%q,"namespace":%q,"fields":[{"name":"f","type":"long"}]}`, nc.name, nc.ns)

			t.Run("cache/"+nc.key+"/direct/"+cm.key, func(t *testing.T) {
				var c avro.SchemaCache
				if _, err := c.Parse(innerDef, withCustom(nc, cm)...); err != nil {
					t.Fatalf("cache define: %v", err)
				}
				writer, err := c.Parse(fmt.Sprintf(`{"type":"record","name":"Outer","fields":[{"name":"i","type":%q}]}`, nc.full), cm.opts...)
				if err != nil {
					t.Fatalf("cache reference parse: %v", err)
				}
				twinJSON := fmt.Sprintf(`{"type":"record","name":"Outer","fields":[{"name":"i","type":%s}]}`, innerDef)
				twin, err := avro.Parse(twinJSON, nameOnlyOpts(nc)...)
				if err != nil {
					t.Fatalf("twin parse: %v", err)
				}
				readerJSON := fmt.Sprintf(`{"type":"record","name":"Outer","fields":[{"name":"i","type":%s},{"name":"added","type":"string","default":"x"}]}`, innerDef)
				reader, err := avro.Parse(readerJSON, withCustom(nc, cm)...)
				if err != nil {
					t.Fatalf("reader parse: %v", err)
				}
				in := map[string]any{"i": map[string]any{"f": int64(7)}}
				var inCt map[string]any
				if cm.key == "encdec" {
					inCt = map[string]any{"i": map[string]any{"f": ctLong{7}}}
				}
				want := map[string]any{"i": map[string]any{"f": L(cm.wrap, 7)}, "added": "x"}
				battery(t, nc, writer, twin, reader, in, inCt, want)
			})

			t.Run("cache/"+nc.key+"/transitive/"+cm.key, func(t *testing.T) {
				var c avro.SchemaCache
				if _, err := c.Parse(innerDef, withCustom(nc, cm)...); err != nil {
					t.Fatalf("cache define: %v", err)
				}
				// Two wrappers referencing the same inner type. The final
				// splice inlines both wrapper definitions, and the second
				// arrival of the inner definition must dedupe to a reference
				// (the diamond).
				for _, w := range []string{"WrapA", "WrapB"} {
					if _, err := c.Parse(fmt.Sprintf(`{"type":"record","name":%q,"namespace":"mid","fields":[{"name":"i","type":%q}]}`, w, nc.full), cm.opts...); err != nil {
						t.Fatalf("cache wrapper %s parse: %v", w, err)
					}
				}
				writer, err := c.Parse(`{"type":"record","name":"Outer","fields":[{"name":"x","type":"mid.WrapA"},{"name":"y","type":"mid.WrapB"}]}`, cm.opts...)
				if err != nil {
					t.Fatalf("cache transitive reference parse: %v", err)
				}
				twinJSON := fmt.Sprintf(`{"type":"record","name":"Outer","fields":[{"name":"x","type":{"type":"record","name":"WrapA","namespace":"mid","fields":[{"name":"i","type":%s}]}},{"name":"y","type":{"type":"record","name":"WrapB","namespace":"mid","fields":[{"name":"i","type":%q}]}}]}`, innerDef, nc.full)
				twin, err := avro.Parse(twinJSON, nameOnlyOpts(nc)...)
				if err != nil {
					t.Fatalf("twin parse: %v", err)
				}
				readerJSON := fmt.Sprintf(`{"type":"record","name":"Outer","fields":[{"name":"x","type":{"type":"record","name":"WrapA","namespace":"mid","fields":[{"name":"i","type":%s}]}},{"name":"y","type":{"type":"record","name":"WrapB","namespace":"mid","fields":[{"name":"i","type":%q}]}},{"name":"added","type":"string","default":"x"}]}`, innerDef, nc.full)
				reader, err := avro.Parse(readerJSON, withCustom(nc, cm)...)
				if err != nil {
					t.Fatalf("reader parse: %v", err)
				}
				in := map[string]any{"x": map[string]any{"i": map[string]any{"f": int64(7)}}, "y": map[string]any{"i": map[string]any{"f": int64(8)}}}
				var inCt map[string]any
				if cm.key == "encdec" {
					inCt = map[string]any{"x": map[string]any{"i": map[string]any{"f": ctLong{7}}}, "y": map[string]any{"i": map[string]any{"f": ctLong{8}}}}
				}
				want := map[string]any{"x": map[string]any{"i": map[string]any{"f": L(cm.wrap, 7)}}, "y": map[string]any{"i": map[string]any{"f": L(cm.wrap, 8)}}, "added": "x"}
				battery(t, nc, writer, twin, reader, in, inCt, want)
			})
		}
	}

	// Extra cell: the outer cache parse itself carries the user lax fn. The
	// splice rebuild's first attempt (this call's own opts) then succeeds and
	// the internal retry never fires. This guards the opts passthrough.
	t.Run("cache/emptycomponent/direct/none/outer-lax", func(t *testing.T) {
		nc := classes[2] // emptycomponent
		innerDef := fmt.Sprintf(`{"type":"record","name":%q,"namespace":%q,"fields":[{"name":"f","type":"long"}]}`, nc.name, nc.ns)
		var c avro.SchemaCache
		if _, err := c.Parse(innerDef, nc.opt); err != nil {
			t.Fatalf("cache define: %v", err)
		}
		writer, err := c.Parse(fmt.Sprintf(`{"type":"record","name":"Outer","fields":[{"name":"i","type":%q}]}`, nc.full), nc.opt)
		if err != nil {
			t.Fatalf("cache reference parse (outer lax): %v", err)
		}
		twin, err := avro.Parse(fmt.Sprintf(`{"type":"record","name":"Outer","fields":[{"name":"i","type":%s}]}`, innerDef), nc.opt)
		if err != nil {
			t.Fatalf("twin parse: %v", err)
		}
		reader, err := avro.Parse(fmt.Sprintf(`{"type":"record","name":"Outer","fields":[{"name":"i","type":%s},{"name":"added","type":"string","default":"x"}]}`, innerDef), nc.opt)
		if err != nil {
			t.Fatalf("reader parse: %v", err)
		}
		in := map[string]any{"i": map[string]any{"f": int64(7)}}
		want := map[string]any{"i": map[string]any{"f": int64(7)}, "added": "x"}
		battery(t, nc, writer, twin, reader, in, nil, want)
	})
}

// Reader-side cells for the overlay-completion fix. A cache-parsed reader whose
// custom matches only SchemaCache-inherited subtrees must apply the custom on
// resolved decode exactly as on direct decode. Every cell asserts resolved ==
// direct on value and type against an explicit want. It also asserts resolved
// DecodeJSON agreement, and that the no-custom twin's wire is unchanged by the
// registration. The evolution axis lives inside the inherited subtree and picks
// the three resolve-time re-application families: added-field, promotion,
// reorder.
func TestMatrix_CacheReaderInheritedCustomResolve(t *testing.T) {
	type evo struct {
		key         string
		writerInner string // writer Inner fields (pre-evolution)
		readerInner string // reader Inner fields (post-evolution)
		writerVal   map[string]any
		nativeVal   map[string]any // reader-shaped, native values
		ctVal       map[string]any // reader-shaped, domain-typed values
		wantInner   map[string]any
	}
	evos := []evo{
		{
			key:         "addedfield",
			writerInner: `[{"name":"f","type":"long"}]`,
			readerInner: `[{"name":"f","type":"long"},{"name":"g","type":"long","default":9}]`,
			writerVal:   map[string]any{"f": int64(7)},
			nativeVal:   map[string]any{"f": int64(7), "g": int64(9)},
			ctVal:       map[string]any{"f": ctLong{7}, "g": ctLong{9}},
			wantInner:   map[string]any{"f": ctLong{7}, "g": ctLong{9}},
		},
		{
			key:         "promotion",
			writerInner: `[{"name":"f","type":"int"}]`,
			readerInner: `[{"name":"f","type":"long"}]`,
			writerVal:   map[string]any{"f": int32(7)},
			nativeVal:   map[string]any{"f": int64(7)},
			ctVal:       map[string]any{"f": ctLong{7}},
			wantInner:   map[string]any{"f": ctLong{7}},
		},
		{
			key:         "reorder",
			writerInner: `[{"name":"f","type":"long"},{"name":"g","type":"string"}]`,
			readerInner: `[{"name":"g","type":"string"},{"name":"f","type":"long"}]`,
			writerVal:   map[string]any{"f": int64(7), "g": "z"},
			nativeVal:   map[string]any{"f": int64(7), "g": "z"},
			ctVal:       map[string]any{"f": ctLong{7}, "g": "z"},
			wantInner:   map[string]any{"f": ctLong{7}, "g": "z"},
		},
	}
	customs := []struct {
		key string
		ct  avro.CustomType
	}{
		{"decodeonly", ctLongDecodeOnly()},
		{"encdec", ctLongEncDec()},
	}

	for _, e := range evos {
		for _, cm := range customs {
			readerInnerDef := `{"type":"record","name":"Inner","fields":` + e.readerInner + `}`
			writerInnerDef := `{"type":"record","name":"Inner","fields":` + e.writerInner + `}`

			runCell := func(t *testing.T, reader, twin, writer *avro.Schema, wrap func(map[string]any) map[string]any) {
				t.Helper()
				want := wrap(e.wantInner)
				resolved, err := avro.Resolve(writer, reader)
				if err != nil {
					t.Fatalf("Resolve: %v", err)
				}
				// Direct decode, the parity target.
				directWire, err := reader.Encode(wrap(e.nativeVal))
				if err != nil {
					t.Fatalf("direct encode: %v", err)
				}
				var direct map[string]any
				if _, err := reader.Decode(directWire, &direct); err != nil {
					t.Fatalf("direct decode: %v", err)
				}
				if !reflect.DeepEqual(direct, want) {
					t.Fatalf("direct decode: got %#v, want %#v", direct, want)
				}
				// Resolved decode must match it, value and type.
				wire, err := writer.Encode(wrap(e.writerVal))
				if err != nil {
					t.Fatalf("writer encode: %v", err)
				}
				var viaBinary map[string]any
				if _, err := resolved.Decode(wire, &viaBinary); err != nil {
					t.Fatalf("resolved decode: %v", err)
				}
				if !reflect.DeepEqual(viaBinary, want) {
					t.Errorf("resolved binary decode: got %#v, want %#v", viaBinary, want)
				}
				wjson, err := writer.EncodeJSON(wrap(e.writerVal))
				if err != nil {
					t.Fatalf("writer EncodeJSON: %v", err)
				}
				var viaJSON map[string]any
				if err := resolved.DecodeJSON(wjson, &viaJSON); err != nil {
					t.Fatalf("resolved DecodeJSON: %v", err)
				}
				if !reflect.DeepEqual(viaJSON, want) {
					t.Errorf("resolved DecodeJSON: got %#v, want %#v", viaJSON, want)
				}
				// The custom never changes the wire: reader bytes equal the
				// no-custom twin's, from native input and (encode+decode
				// cells) from domain-typed input.
				twinWire, err := twin.Encode(wrap(e.nativeVal))
				if err != nil {
					t.Fatalf("twin encode: %v", err)
				}
				if !bytes.Equal(directWire, twinWire) {
					t.Errorf("reader wire bytes diverge from no-custom twin: %x vs %x", directWire, twinWire)
				}
				if cm.key == "encdec" {
					ctWire, err := reader.Encode(wrap(e.ctVal))
					if err != nil {
						t.Fatalf("domain-typed encode: %v", err)
					}
					if !bytes.Equal(ctWire, twinWire) {
						t.Errorf("domain-typed wire bytes diverge from no-custom twin: %x vs %x", ctWire, twinWire)
					}
				}
			}

			t.Run("reader/"+e.key+"/direct/"+cm.key, func(t *testing.T) {
				var c avro.SchemaCache
				if _, err := c.Parse(readerInnerDef, cm.ct); err != nil {
					t.Fatalf("cache define: %v", err)
				}
				reader, err := c.Parse(`{"type":"record","name":"Outer","fields":[{"name":"i","type":"Inner"}]}`, cm.ct)
				if err != nil {
					t.Fatalf("cache reader parse: %v", err)
				}
				twin, err := avro.Parse(`{"type":"record","name":"Outer","fields":[{"name":"i","type":` + readerInnerDef + `}]}`)
				if err != nil {
					t.Fatalf("twin parse: %v", err)
				}
				writer, err := avro.Parse(`{"type":"record","name":"Outer","fields":[{"name":"i","type":` + writerInnerDef + `}]}`)
				if err != nil {
					t.Fatalf("writer parse: %v", err)
				}
				runCell(t, reader, twin, writer, func(inner map[string]any) map[string]any {
					return map[string]any{"i": inner}
				})
			})

			t.Run("reader/"+e.key+"/transitive/"+cm.key, func(t *testing.T) {
				var c avro.SchemaCache
				if _, err := c.Parse(readerInnerDef, cm.ct); err != nil {
					t.Fatalf("cache define: %v", err)
				}
				if _, err := c.Parse(`{"type":"record","name":"Wrapper","namespace":"mid","fields":[{"name":"i","type":"Inner"}]}`, cm.ct); err != nil {
					t.Fatalf("cache wrapper parse: %v", err)
				}
				reader, err := c.Parse(`{"type":"record","name":"Outer","fields":[{"name":"w","type":"mid.Wrapper"}]}`, cm.ct)
				if err != nil {
					t.Fatalf("cache reader parse: %v", err)
				}
				twin, err := avro.Parse(`{"type":"record","name":"Outer","fields":[{"name":"w","type":{"type":"record","name":"Wrapper","namespace":"mid","fields":[{"name":"i","type":` + readerInnerDef + `}]}}]}`)
				if err != nil {
					t.Fatalf("twin parse: %v", err)
				}
				writer, err := avro.Parse(`{"type":"record","name":"Outer","fields":[{"name":"w","type":{"type":"record","name":"Wrapper","namespace":"mid","fields":[{"name":"i","type":` + writerInnerDef + `}]}}]}`)
				if err != nil {
					t.Fatalf("writer parse: %v", err)
				}
				runCell(t, reader, twin, writer, func(inner map[string]any) map[string]any {
					return map[string]any{"w": map[string]any{"i": inner}}
				})
			})
		}
	}
}

// The bare empty name ("" with no namespace) is definable under a user
// accept-all validator but not referenceable. Its only spelling as a reference
// is the empty string, which the parser rejects structurally upstream of any
// name validator. We pin these verdicts as the original parse's behavior, so
// the reference cells of the empty-name class run on the namespaced form
// (fullname "ok.") in TestMatrix_InternalReparseLaxNames.
func TestMatrix_InternalReparseBareEmptyName(t *testing.T) {
	acceptAll := func(string) error { return nil }

	// Definition accepted; reference rejected, single parse.
	if _, err := avro.Parse(`{"type":"record","name":"","fields":[{"name":"f","type":"long"}]}`, avro.WithLaxNames(acceptAll)); err != nil {
		t.Fatalf("bare empty-name definition must parse under accept-all: %v", err)
	}
	dia := `{"type":"record","name":"Top","fields":[{"name":"a","type":{"type":"record","name":"","fields":[{"name":"f","type":"long"}]}},{"name":"b","type":""}]}`
	if _, err := avro.Parse(dia, avro.WithLaxNames(acceptAll)); err == nil {
		t.Error(`reference "" must be structurally rejected (in-schema)`)
	} else if !strings.Contains(err.Error(), "not a primitive") {
		t.Errorf("in-schema rejection changed shape: %v", err)
	}
	// Reference rejected, cache cross-parse.
	var c avro.SchemaCache
	if _, err := c.Parse(`{"type":"record","name":"","fields":[{"name":"f","type":"long"}]}`, avro.WithLaxNames(acceptAll)); err != nil {
		t.Fatalf("cache bare empty-name definition: %v", err)
	}
	if _, err := c.Parse(`{"type":"record","name":"Outer","fields":[{"name":"i","type":""}]}`); err == nil {
		t.Error(`reference "" must be structurally rejected (cache cross-parse)`)
	} else if !strings.Contains(err.Error(), "not a primitive") {
		t.Errorf("cache rejection changed shape: %v", err)
	}

	// A bare empty-name root still survives the resolve-view re-parse: the
	// writer's own text carries "name":"" and parses under the internal
	// accept-all validator.
	writer, err := avro.Parse(`{"type":"record","name":"","fields":[{"name":"f","type":"long"}]}`, avro.WithLaxNames(acceptAll), ctLongDecodeOnly())
	if err != nil {
		t.Fatalf("bare empty-name custom writer parse: %v", err)
	}
	reader, err := avro.Parse(`{"type":"record","name":"","fields":[{"name":"f","type":"long"},{"name":"added","type":"string","default":"x"}]}`, avro.WithLaxNames(acceptAll), ctLongDecodeOnly())
	if err != nil {
		t.Fatalf("bare empty-name reader parse: %v", err)
	}
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatalf("Resolve of bare empty-name custom writer: %v", err)
	}
	wire, err := writer.Encode(map[string]any{"f": int64(7)})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	want := map[string]any{"f": ctLong{7}, "added": "x"}
	var viaBinary map[string]any
	if _, err := resolved.Decode(wire, &viaBinary); err != nil {
		t.Fatalf("resolved binary decode: %v", err)
	}
	if !reflect.DeepEqual(viaBinary, want) {
		t.Errorf("resolved binary decode: got %#v, want %#v", viaBinary, want)
	}
	var viaJSON map[string]any
	if err := resolved.DecodeJSON([]byte(`{"f":7}`), &viaJSON); err != nil {
		t.Fatalf("resolved DecodeJSON: %v", err)
	}
	if !reflect.DeepEqual(viaJSON, want) {
		t.Errorf("resolved DecodeJSON: got %#v, want %#v", viaJSON, want)
	}
	// String() re-parses (the as-written text keeps "name":"").
	if _, err := avro.Parse(writer.String(), avro.WithLaxNames(acceptAll)); err != nil {
		t.Errorf("String() re-parse: %v\nString(): %s", err, writer.String())
	}

	// NOT_BUGS #60: the bare empty-name root emits "name":"" in canonical
	// form, matching fastavro (1.12.2, executed), the only other
	// implementation known to parse the shape. Omitting the name emits a
	// spelling that fingerprints like nothing else. We pin canonical bytes and
	// the Rabin fingerprint against fastavro's executed values. The re-parse
	// holds either way, so the bytes are the discriminator.
	wantCanon := `{"name":"","type":"record","fields":[{"name":"f","type":"long"}]}`
	if canon := string(writer.Canonical()); canon != wantCanon {
		t.Errorf("bare empty-name canonical:\n got %s\nwant %s", canon, wantCanon)
	}
	if got, want := writer.Fingerprint(avro.NewRabin()), fastavroRabinBytes(t, "3d741707ff4bfa45"); !bytes.Equal(got, want) {
		t.Errorf("bare empty-name rabin: got %x, want %x (fastavro-executed)", got, want)
	}
	if _, err := avro.Parse(string(writer.Canonical()), avro.WithLaxNames(acceptAll)); err != nil {
		t.Errorf("bare empty-name canonical must re-parse under accept-all: %v", err)
	}
}

// Class matrix for keyless definitions in the SchemaCache def table: named-kind
// nodes with no "name" key at all, parseable only under a WithLaxNames fn
// accepting "". The parser registers a fullname for them regardless and scopes
// their children by it, so the def-collection and splice walkers must too.
//
//	{namespace attr: present "x", absent}
//	  x {parse-2 shape: cross-parse reference to the parser fullname,
//	     reference-then-local-define of the nested short name,
//	     local-define-then-reference}
//	plus keyless-def-visit, seen-parity, and same-string lax re-parse cells.
//
// Per cell the metadata forms describe the wire codec's schema, with
// twin-parity where a twin exists. The bare-namespace reference-then-define
// orders have no twin, the parser rejecting the parse, so they pin the
// rejection.
func TestMatrix_CacheKeylessDefCollection(t *testing.T) {
	acceptAll := func(string) error { return nil }
	lax := avro.WithLaxNames(acceptAll)

	const keylessNS = `{"type":"record","namespace":"x","fields":[{"name":"i","type":{"type":"record","name":"Inner","fields":[{"name":"w","type":"long"}]}}]}`
	const keylessBare = `{"type":"record","fields":[{"name":"i","type":{"type":"record","name":"Inner","fields":[{"name":"w","type":"long"}]}}]}`

	// Cross-parse reference to the nested definition's parser-scoped
	// fullname (define-then-reference across parses). With the namespace
	// attribute present the nested def is x.Inner; misfiled under "Inner"
	// the exact dotted lookup dangles.
	t.Run("ns/crossref-inner", func(t *testing.T) {
		var c avro.SchemaCache
		if _, err := c.Parse(keylessNS, lax); err != nil {
			t.Fatalf("keyless define: %v", err)
		}
		writer, err := c.Parse(`{"type":"record","name":"Outer2","fields":[{"name":"a","type":"x.Inner"}]}`)
		if err != nil {
			t.Fatalf("reference parse: %v", err)
		}
		nc := reparseNameClass{"keyless-ns", "x", "Inner", "x.Inner", nil}
		twinJSON := `{"type":"record","name":"Outer2","fields":[{"name":"a","type":{"type":"record","name":"Inner","namespace":"x","fields":[{"name":"w","type":"long"}]}}]}`
		in := map[string]any{"a": map[string]any{"w": int64(7)}}
		want := map[string]any{"a": map[string]any{"w": int64(7)}, "added": "x"}
		runReparseBattery(t, nc, writer, twinJSON, in, want)
	})

	// With the namespace attribute absent, the parser scopes the nested
	// def in the enclosing (null) namespace. This cell is the control for
	// the scope rule's other half. The spliced definition carries the
	// explicit-empty namespace escape and stays strict-parseable.
	t.Run("bare/crossref-inner", func(t *testing.T) {
		var c avro.SchemaCache
		if _, err := c.Parse(keylessBare, lax); err != nil {
			t.Fatalf("keyless define: %v", err)
		}
		writer, err := c.Parse(`{"type":"record","name":"Outer2","fields":[{"name":"a","type":"Inner"}]}`)
		if err != nil {
			t.Fatalf("reference parse: %v", err)
		}
		nc := reparseNameClass{"keyless-bare", "", "Inner", "Inner", nil}
		twinJSON := `{"type":"record","name":"Outer2","fields":[{"name":"a","type":{"type":"record","name":"Inner","namespace":"","fields":[{"name":"w","type":"long"}]}}]}`
		in := map[string]any{"a": map[string]any{"w": int64(7)}}
		want := map[string]any{"a": map[string]any{"w": int64(7)}, "added": "x"}
		runReparseBattery(t, nc, writer, twinJSON, in, want)
	})

	// Reference-then-define: parse-2 references the short name "Inner"
	// *before* locally defining a different Inner{z:string}. The parser
	// forward-binds the reference to the local definition (the cache's
	// named table holds only x-scoped keys). A splice walker that inlines
	// the misfiled stale Inner{w:long} instead, rewriting the local
	// definition to a reference, is the stale-splice divergence.
	t.Run("ns/refdefine", func(t *testing.T) {
		var c avro.SchemaCache
		if _, err := c.Parse(keylessNS, lax); err != nil {
			t.Fatalf("keyless define: %v", err)
		}
		src := `{"type":"record","name":"Outer2","fields":[{"name":"a","type":"Inner"},{"name":"b","type":{"type":"record","name":"Inner","fields":[{"name":"z","type":"string"}]}}]}`
		writer, err := c.Parse(src)
		if err != nil {
			t.Fatalf("reference-then-define parse: %v", err)
		}
		nc := reparseNameClass{"keyless-ns-refdefine", "", "Inner", "Inner", nil}
		in := map[string]any{"a": map[string]any{"z": "p"}, "b": map[string]any{"z": "q"}}
		want := map[string]any{"a": map[string]any{"z": "p"}, "b": map[string]any{"z": "q"}, "added": "x"}
		runReparseBattery(t, nc, writer, src, in, want)
		if _, err := writer.Encode(map[string]any{"a": map[string]any{"w": int64(7)}, "b": map[string]any{"w": int64(8)}}); err == nil {
			t.Error("wire unexpectedly accepts the stale inherited Inner{w:long} shape")
		}
	})

	// Define-then-reference: the local definition precedes the
	// reference, so the parser and the splice walker's positional seen
	// tracking both bind the local type. This is the order dual that
	// makes the matrix's position axis non-vacuous.
	t.Run("ns/definref", func(t *testing.T) {
		var c avro.SchemaCache
		if _, err := c.Parse(keylessNS, lax); err != nil {
			t.Fatalf("keyless define: %v", err)
		}
		src := `{"type":"record","name":"Outer2","fields":[{"name":"a","type":{"type":"record","name":"Inner","fields":[{"name":"z","type":"string"}]}},{"name":"b","type":"Inner"}]}`
		writer, err := c.Parse(src)
		if err != nil {
			t.Fatalf("define-then-reference parse: %v", err)
		}
		nc := reparseNameClass{"keyless-ns-definref", "", "Inner", "Inner", nil}
		in := map[string]any{"a": map[string]any{"z": "p"}, "b": map[string]any{"z": "q"}}
		want := map[string]any{"a": map[string]any{"z": "p"}, "b": map[string]any{"z": "q"}, "added": "x"}
		runReparseBattery(t, nc, writer, src, in, want)
	})

	// With the namespace attribute absent the nested def registers the
	// bare "Inner" in the parser's named table, so a parse that locally
	// re-defines it is a duplicate, rejected by the parser in either
	// order. No twin exists; the rejection is the pinned verdict.
	for _, order := range []struct{ key, src string }{
		{"bare/refdefine", `{"type":"record","name":"Outer2","fields":[{"name":"a","type":"Inner"},{"name":"b","type":{"type":"record","name":"Inner","fields":[{"name":"z","type":"string"}]}}]}`},
		{"bare/definref", `{"type":"record","name":"Outer2","fields":[{"name":"a","type":{"type":"record","name":"Inner","fields":[{"name":"z","type":"string"}]}},{"name":"b","type":"Inner"}]}`},
	} {
		t.Run(order.key, func(t *testing.T) {
			var c avro.SchemaCache
			if _, err := c.Parse(keylessBare, lax); err != nil {
				t.Fatalf("keyless define: %v", err)
			}
			_, err := c.Parse(order.src)
			if err == nil {
				t.Fatal("local re-definition of the cache-inherited bare Inner unexpectedly parsed")
			}
			if !strings.Contains(err.Error(), `duplicate named type "Inner"`) {
				t.Errorf("rejection shape changed: %v", err)
			}
		})
	}

	// The def visit itself: the keyless definition is collected under
	// the parser's fullname "x." and is referenceable across parses by
	// exact dotted lookup. The definition self-references (recursive),
	// so the spliced body's own "x." reference must stay bare.
	t.Run("ns/crossref-outer-recursive", func(t *testing.T) {
		var c avro.SchemaCache
		if _, err := c.Parse(`{"type":"record","namespace":"x","fields":[{"name":"f","type":"long"},{"name":"next","type":["null","x."]}]}`, lax); err != nil {
			t.Fatalf("keyless recursive define: %v", err)
		}
		writer, err := c.Parse(`{"type":"record","name":"Outer2","fields":[{"name":"a","type":"x."}]}`)
		if err != nil {
			t.Fatalf("reference parse: %v", err)
		}
		nc := reparseNameClass{"keyless-outer", "x", "", "x.", lax}
		twinJSON := `{"type":"record","name":"Outer2","fields":[{"name":"a","type":{"type":"record","name":"","namespace":"x","fields":[{"name":"f","type":"long"},{"name":"next","type":["null","x."]}]}}]}`
		in := map[string]any{"a": map[string]any{"f": int64(7), "next": map[string]any{"f": int64(8), "next": nil}}}
		want := map[string]any{"a": map[string]any{"f": int64(7), "next": map[string]any{"f": int64(8), "next": nil}}, "added": "x"}
		runReparseBattery(t, nc, writer, twinJSON, in, want)
	})

	// Diamond through the keyless def: parse-2 references both "x." and the
	// x.Inner nested inside it. The splice at the first reference carries the
	// Inner definition. Walking the spliced copy registers it, so the second
	// reference stays bare and resolves backward into the first splice. That is
	// one definition per name, the first-define-then-reference rule
	// inlineTreeDefs implements.
	t.Run("ns/diamond", func(t *testing.T) {
		var c avro.SchemaCache
		if _, err := c.Parse(keylessNS, lax); err != nil {
			t.Fatalf("keyless define: %v", err)
		}
		writer, err := c.Parse(`{"type":"record","name":"Outer2","fields":[{"name":"a","type":"x."},{"name":"b","type":"x.Inner"}]}`)
		if err != nil {
			t.Fatalf("diamond reference parse: %v", err)
		}
		nc := reparseNameClass{"keyless-diamond", "x", "", "x.", lax}
		twinJSON := `{"type":"record","name":"Outer2","fields":[{"name":"a","type":{"type":"record","name":"","namespace":"x","fields":[{"name":"i","type":{"type":"record","name":"Inner","fields":[{"name":"w","type":"long"}]}}]}},{"name":"b","type":"x.Inner"}]}`
		in := map[string]any{"a": map[string]any{"i": map[string]any{"w": int64(7)}}, "b": map[string]any{"w": int64(8)}}
		want := map[string]any{"a": map[string]any{"i": map[string]any{"w": int64(7)}}, "b": map[string]any{"w": int64(8)}, "added": "x"}
		runReparseBattery(t, nc, writer, twinJSON, in, want)
	})

	// Seen-parity, map arm: a keyless definition arriving inside a
	// spliced subtree (as-written, no name key) must register its
	// parser fullname "n." during the walk. Otherwise the later "n."
	// reference splices a second copy, and the duplicate-rejecting rebuild
	// degrades the metadata to the dangling original.
	t.Run("ns/nested-keyless-diamond", func(t *testing.T) {
		var c avro.SchemaCache
		if _, err := c.Parse(`{"type":"record","name":"X","namespace":"n","fields":[{"name":"k","type":{"type":"record","fields":[{"name":"f","type":"long"}]}}]}`, lax); err != nil {
			t.Fatalf("nested keyless define: %v", err)
		}
		writer, err := c.Parse(`{"type":"record","name":"Outer2","fields":[{"name":"a","type":"n.X"},{"name":"b","type":"n."}]}`)
		if err != nil {
			t.Fatalf("diamond reference parse: %v", err)
		}
		nc := reparseNameClass{"nested-keyless", "n", "", "n.", lax}
		twinJSON := `{"type":"record","name":"Outer2","fields":[{"name":"a","type":{"type":"record","name":"X","namespace":"n","fields":[{"name":"k","type":{"type":"record","fields":[{"name":"f","type":"long"}]}}]}},{"name":"b","type":"n."}]}`
		in := map[string]any{"a": map[string]any{"k": map[string]any{"f": int64(7)}}, "b": map[string]any{"f": int64(8)}}
		want := map[string]any{"a": map[string]any{"k": map[string]any{"f": int64(7)}}, "b": map[string]any{"f": int64(8)}, "added": "x"}
		runReparseBattery(t, nc, writer, twinJSON, in, want)
	})

	// Seen-parity, flat-field arm: the keyless definition is spelled as
	// a flat field (goavro-style, no field name either, so the lift
	// produces the keyless type). The parse-1 pins lock the flat
	// keyless lift end-to-end: canonical form, the metadata walker's
	// keyless handling, and the empty-string field name on the wire.
	t.Run("ns/flat-keyless", func(t *testing.T) {
		var c avro.SchemaCache
		s1, err := c.Parse(`{"type":"record","name":"X","namespace":"n","fields":[{"type":"record","fields":[{"name":"f","type":"long"}]}]}`, lax)
		if err != nil {
			t.Fatalf("flat keyless define: %v", err)
		}
		wantCanon := `{"name":"n.X","type":"record","fields":[{"name":"","type":{"name":"n.","type":"record","fields":[{"name":"f","type":"long"}]}}]}`
		if got := string(s1.Canonical()); got != wantCanon {
			t.Errorf("flat keyless canonical:\n got %s\nwant %s", got, wantCanon)
		}
		f0 := s1.Root().Fields[0]
		if f0.Name != "" || f0.Type.Type != "record" || f0.Type.Name != "" || f0.Type.Namespace != "n" || len(f0.Type.Fields) != 1 || f0.Type.Fields[0].Name != "f" {
			t.Errorf("Root() flat keyless field: got name=%q type=%q typeName=%q ns=%q fields=%v", f0.Name, f0.Type.Type, f0.Type.Name, f0.Type.Namespace, f0.Type.Fields)
		}
		wire1, err := s1.Encode(map[string]any{"": map[string]any{"f": int64(7)}})
		if err != nil {
			t.Fatalf("encode by empty field name: %v", err)
		}
		if !bytes.Equal(wire1, []byte{0x0e}) {
			t.Errorf("flat keyless wire: got %x, want 0e", wire1)
		}

		writer, err := c.Parse(`{"type":"record","name":"Outer2","fields":[{"name":"a","type":"n."}]}`)
		if err != nil {
			t.Fatalf("reference parse: %v", err)
		}
		nc := reparseNameClass{"flat-keyless", "n", "", "n.", lax}
		twinJSON := `{"type":"record","name":"Outer2","fields":[{"name":"a","type":{"type":"record","name":"","namespace":"n","fields":[{"name":"f","type":"long"}]}}]}`
		in := map[string]any{"a": map[string]any{"f": int64(7)}}
		want := map[string]any{"a": map[string]any{"f": int64(7)}, "added": "x"}
		runReparseBattery(t, nc, writer, twinJSON, in, want)
	})

	// Same-string lax re-parse: the second parse re-enters the builder
	// with the first parse's defs in the cache. Its splice walk then sees
	// the local keyless definition after splicing the (identical)
	// inherited one at the forward reference. dupDefRef must rewrite the
	// local keyless definition to its dotted reference, keeping the
	// canonical forms byte-stable across the two parses.
	t.Run("samestring-reparse", func(t *testing.T) {
		src := `{"type":"record","name":"Top","fields":[{"name":"a","type":["null","x."]},{"name":"b","type":{"type":"record","namespace":"x","fields":[{"name":"f","type":"long"}]}}]}`
		var c avro.SchemaCache
		s1, err := c.Parse(src, lax)
		if err != nil {
			t.Fatalf("parse-1: %v", err)
		}
		s2, err := c.Parse(src, lax)
		if err != nil {
			t.Fatalf("parse-2 (same string): %v", err)
		}
		if !bytes.Equal(s1.Canonical(), s2.Canonical()) {
			t.Errorf("canonical unstable across same-string re-parse:\n s1: %s\n s2: %s", s1.Canonical(), s2.Canonical())
		}
		twin, err := avro.Parse(src, lax)
		if err != nil {
			t.Fatalf("twin parse: %v", err)
		}
		if !bytes.Equal(s2.Canonical(), twin.Canonical()) {
			t.Errorf("canonical diverges from directly-parsed twin:\n got: %s\nwant: %s", s2.Canonical(), twin.Canonical())
		}
		if _, err := avro.Parse(s2.String(), lax); err != nil {
			t.Fatalf("String() must re-parse: %v\nString(): %s", err, s2.String())
		}
		// The splice route is pinned structurally, not just coherently. The
		// parser bound the forward reference at field a to the cached type, so
		// the faithful metadata materializes the definition there. Field b, the
		// local re-definition of the same fullname, becomes the dotted
		// reference. A fallback to the as-written text would be value-coherent
		// here but would invert the binding structure the wire used.
		if !strings.Contains(s2.String(), `{"name":"b","type":"x."}`) {
			t.Errorf("String() does not carry the dupDefRef-rewritten reference at field b:\n%s", s2.String())
		}
		in := map[string]any{"a": map[string]any{"f": int64(7)}, "b": map[string]any{"f": int64(8)}}
		w1, err := s1.Encode(in)
		if err != nil {
			t.Fatalf("s1 encode: %v", err)
		}
		w2, err := s2.Encode(in)
		if err != nil {
			t.Fatalf("s2 encode: %v", err)
		}
		if !bytes.Equal(w1, w2) {
			t.Errorf("wire bytes diverge across same-string re-parse: %x vs %x", w1, w2)
		}
	})
}

// Class matrix for leading-dot names. A single leading dot with no other dot is
// the explicit null-namespace escape. {"name":".x"} builds as "x" in the null
// namespace, and "." collapses to the bare empty name. That is the rule
// qualifyAliases already applies to aliases and Java's Name constructor applies
// to every name. Lax-only. fastavro 1.12.2 holds a third posture (executed
// 2026-07-14): it keeps ".x" verbatim in PCF and rejects the bare-"x"
// reference. We follow Java's normalized identity, which also keeps references
// self-consistent.
//
//	{".x" definition x reference spelling {"x", ".x"} x cross-parse
//	 x {pure reference, reference-then-define, define-then-reference}}
//	plus same-parse equivalence, the "." -> empty-name cell (NOT_BUGS #60),
//	a multi-dot verbatim control, and the Root()/parser agreement cell.
func TestMatrix_LeadingDotNameNormalization(t *testing.T) {
	acceptAll := func(string) error { return nil }
	lax := avro.WithLaxNames(acceptAll)
	const dotXDef = `{"type":"record","name":".x","fields":[{"name":"w","type":"long"}]}`

	// Cross-parse reference, both spellings: definition ".x" and
	// references "x" / ".x" all denote the null-namespace fullname "x",
	// so the splice fires and the spliced form (name "x") is
	// strict-parseable.
	for _, ref := range []string{"x", ".x"} {
		t.Run("crossref/"+strings.ReplaceAll(ref, ".", "dot"), func(t *testing.T) {
			var c avro.SchemaCache
			if _, err := c.Parse(dotXDef, lax); err != nil {
				t.Fatalf("leading-dot define: %v", err)
			}
			writer, err := c.Parse(fmt.Sprintf(`{"type":"record","name":"Outer2","fields":[{"name":"a","type":%q}]}`, ref))
			if err != nil {
				t.Fatalf("reference parse (%q): %v", ref, err)
			}
			nc := reparseNameClass{"leadingdot", "", "x", "x", nil}
			twinJSON := `{"type":"record","name":"Outer2","fields":[{"name":"a","type":{"type":"record","name":"x","fields":[{"name":"w","type":"long"}]}}]}`
			in := map[string]any{"a": map[string]any{"w": int64(7)}}
			want := map[string]any{"a": map[string]any{"w": int64(7)}, "added": "x"}
			runReparseBattery(t, nc, writer, twinJSON, in, want)
		})
	}

	// Reference-then-define and define-then-reference with a local "x"
	// definition. ".x" *is* the fullname "x", so the local re-definition
	// duplicates the cache-inherited name in either order and with either
	// reference spelling. That is the parser's standard conflict
	// rejection, same as every other same-fullname family.
	for _, order := range []struct{ key, src string }{
		{"refdefine/x", `{"type":"record","name":"Outer2","fields":[{"name":"a","type":"x"},{"name":"b","type":{"type":"record","name":"x","fields":[{"name":"z","type":"string"}]}}]}`},
		{"refdefine/dotx", `{"type":"record","name":"Outer2","fields":[{"name":"a","type":".x"},{"name":"b","type":{"type":"record","name":"x","fields":[{"name":"z","type":"string"}]}}]}`},
		{"definref", `{"type":"record","name":"Outer2","fields":[{"name":"a","type":{"type":"record","name":"x","fields":[{"name":"z","type":"string"}]}},{"name":"b","type":"x"}]}`},
	} {
		t.Run(order.key, func(t *testing.T) {
			var c avro.SchemaCache
			if _, err := c.Parse(dotXDef, lax); err != nil {
				t.Fatalf("leading-dot define: %v", err)
			}
			_, err := c.Parse(order.src)
			if err == nil {
				t.Fatal("local re-definition of the cache-inherited fullname x unexpectedly parsed")
			}
			if !strings.Contains(err.Error(), `duplicate named type "x"`) {
				t.Errorf("rejection shape changed: %v", err)
			}
		})
	}

	// Same-parse spelling equivalence: the ".x" spelling and the plain
	// "x" spelling are one type. That holds in both definition positions
	// and both reference directions (backward and forward). The twin is the same
	// schema spelled plainly; canonical, fingerprint, and wire bytes
	// must be identical.
	for _, cell := range []struct{ key, src, twin string }{
		{"sameparse/definref", `{"type":"record","name":"Top","fields":[{"name":"a","type":` + dotXDef + `},{"name":"b","type":"x"}]}`,
			`{"type":"record","name":"Top","fields":[{"name":"a","type":{"type":"record","name":"x","fields":[{"name":"w","type":"long"}]}},{"name":"b","type":"x"}]}`},
		{"sameparse/refdefine", `{"type":"record","name":"Top","fields":[{"name":"a","type":"x"},{"name":"b","type":` + dotXDef + `}]}`,
			`{"type":"record","name":"Top","fields":[{"name":"a","type":"x"},{"name":"b","type":{"type":"record","name":"x","fields":[{"name":"w","type":"long"}]}}]}`},
		{"sameparse/dotx-ref", `{"type":"record","name":"Top","fields":[{"name":"a","type":{"type":"record","name":"x","fields":[{"name":"w","type":"long"}]}},{"name":"b","type":".x"}]}`,
			`{"type":"record","name":"Top","fields":[{"name":"a","type":{"type":"record","name":"x","fields":[{"name":"w","type":"long"}]}},{"name":"b","type":"x"}]}`},
	} {
		t.Run(cell.key, func(t *testing.T) {
			writer := avrotest.MustParse(t, cell.src, lax)
			nc := reparseNameClass{"leadingdot-sameparse", "", "x", "x", lax}
			in := map[string]any{"a": map[string]any{"w": int64(7)}, "b": map[string]any{"w": int64(8)}}
			want := map[string]any{"a": map[string]any{"w": int64(7)}, "b": map[string]any{"w": int64(8)}, "added": "x"}
			runReparseBattery(t, nc, writer, cell.twin, in, want)
		})
	}

	// "." collapses into the empty-name family (NOT_BUGS #60). Its canonical
	// form and Rabin fingerprint are byte-identical to the bare {"name":""}
	// definition's, 3d741707ff4bfa45 being the fastavro-executed value. The
	// type stays unreferenceable in every spelling. fastavro 1.12.2 keeps
	// "." verbatim in PCF (executed: rabin b1eae635ed69c128), the same
	// verbatim-identity divergence as the ".x" root: documented, not adopted.
	t.Run("dot-family", func(t *testing.T) {
		s, err := avro.Parse(`{"type":"record","name":".","fields":[{"name":"f","type":"long"}]}`, lax)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		wantCanon := `{"name":"","type":"record","fields":[{"name":"f","type":"long"}]}`
		if got := string(s.Canonical()); got != wantCanon {
			t.Errorf("canonical:\n got %s\nwant %s", got, wantCanon)
		}
		if got, want := s.Fingerprint(avro.NewRabin()), fastavroRabinBytes(t, "3d741707ff4bfa45"); !bytes.Equal(got, want) {
			t.Errorf("rabin: got %x, want %x (the #60 family value)", got, want)
		}
		twin, err := avro.Parse(`{"type":"record","name":"","fields":[{"name":"f","type":"long"}]}`, lax)
		if err != nil {
			t.Fatalf("twin parse: %v", err)
		}
		if !bytes.Equal(s.Canonical(), twin.Canonical()) {
			t.Errorf("canonical diverges from the {\"name\":\"\"} twin:\n got: %s\nwant: %s", s.Canonical(), twin.Canonical())
		}
		in := map[string]any{"f": int64(7)}
		wire, err := s.Encode(in)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		wireTwin, err := twin.Encode(in)
		if err != nil {
			t.Fatalf("twin encode: %v", err)
		}
		if !bytes.Equal(wire, wireTwin) {
			t.Errorf("wire bytes diverge from the {\"name\":\"\"} twin: %x vs %x", wire, wireTwin)
		}
		// Unreferenceable in the "." spelling, same-parse and cross-parse.
		if _, err := avro.Parse(`{"type":"record","name":"Top","fields":[{"name":"a","type":{"type":"record","name":".","fields":[{"name":"f","type":"long"}]}},{"name":"b","type":"."}]}`, lax); err == nil {
			t.Error(`same-parse "." reference unexpectedly bound`)
		} else if !strings.Contains(err.Error(), `unknown type "."`) {
			t.Errorf("same-parse rejection shape changed: %v", err)
		}
		var c avro.SchemaCache
		if _, err := c.Parse(`{"type":"record","name":".","fields":[{"name":"f","type":"long"}]}`, lax); err != nil {
			t.Fatalf("cache define: %v", err)
		}
		if _, err := c.Parse(`{"type":"record","name":"Outer2","fields":[{"name":"a","type":"."}]}`); err == nil {
			t.Error(`cross-parse "." reference unexpectedly bound`)
		} else if !strings.Contains(err.Error(), `unknown type "."`) {
			t.Errorf("cross-parse rejection shape changed: %v", err)
		}
	})

	// Multi-dot control: the escape is only the single leading dot.
	// ".a.b" keeps its verbatim identity (namespace ".a"). Java's Name
	// ctor keeps any non-empty space, and fastavro's executed PCF agrees
	// byte-for-byte (rabin 013f503d468af517, 2026-07-14). Three-way
	// agreement pins the boundary of the rule.
	t.Run("multidot-verbatim", func(t *testing.T) {
		s, err := avro.Parse(`{"type":"record","name":".a.b","fields":[{"name":"f","type":"long"}]}`, lax)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		wantCanon := `{"name":".a.b","type":"record","fields":[{"name":"f","type":"long"}]}`
		if got := string(s.Canonical()); got != wantCanon {
			t.Errorf("canonical:\n got %s\nwant %s", got, wantCanon)
		}
		if got, want := s.Fingerprint(avro.NewRabin()), fastavroRabinBytes(t, "013f503d468af517"); !bytes.Equal(got, want) {
			t.Errorf("rabin: got %x, want %x (fastavro-executed)", got, want)
		}
		re, err := avro.Parse(string(s.Canonical()), lax)
		if err != nil {
			t.Fatalf("canonical re-parse under accept-all: %v", err)
		}
		if !bytes.Equal(re.Canonical(), s.Canonical()) {
			t.Errorf("canonical not idempotent:\n re %s\ngot %s", re.Canonical(), s.Canonical())
		}
	})

	// Agreement cell: the metadata walkers, the cache walkers, and the
	// parser agree on the ".x" identity. SchemaNode preserves the
	// as-written spellings (Name ".x" on the definition, Type ".x" on
	// the reference). Every computed identity resolves to the fullname
	// "x": canonical form, name-ref binding, and the Schema() rebuild's
	// dedup/cycle emission.
	t.Run("agreement", func(t *testing.T) {
		writer, err := avro.Parse(`{"type":"record","name":"Top","fields":[{"name":"k","type":`+dotXDef+`},{"name":"r","type":".x"}]}`, lax)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		twin, err := avro.Parse(`{"type":"record","name":"Top","fields":[{"name":"k","type":{"type":"record","name":"x","fields":[{"name":"w","type":"long"}]}},{"name":"r","type":"x"}]}`)
		if err != nil {
			t.Fatalf("twin parse: %v", err)
		}
		if !bytes.Equal(writer.Canonical(), twin.Canonical()) {
			t.Errorf("canonical diverges from the plain-spelled twin:\n got: %s\nwant: %s", writer.Canonical(), twin.Canonical())
		}
		root := writer.Root()
		if got := root.Fields[0].Type.Name; got != ".x" {
			t.Errorf("Root() definition Name: got %q, want the as-written %q", got, ".x")
		}
		if got := root.Fields[1].Type.Type; got != ".x" {
			t.Errorf("Root() reference Type: got %q, want the as-written %q", got, ".x")
		}
		rebuilt, err := root.Schema(lax)
		if err != nil {
			t.Fatalf("Root().Schema() rebuild: %v", err)
		}
		if !bytes.Equal(rebuilt.Canonical(), writer.Canonical()) {
			t.Errorf("Schema() rebuild canonical diverges:\n got: %s\nwant: %s", rebuilt.Canonical(), writer.Canonical())
		}
	})
}

// nameOnlyOpts returns the schema opts for a class's name validator
// alone (nil for strict), shared by the reparse batteries' twin/reader
// parses.
func nameOnlyOpts(nc reparseNameClass) []avro.SchemaOpt {
	if nc.opt != nil {
		return []avro.SchemaOpt{nc.opt}
	}
	return nil
}

// reparseAddedReader derives the reader schema for a battery cell by
// appending a defaulted top-level field to the twin's JSON.
func reparseAddedReader(twinJSON string) string {
	i := strings.LastIndex(twinJSON, "]")
	return twinJSON[:i] + `,{"name":"added","type":"string","default":"x"}` + twinJSON[i:]
}

// runReparseBattery parses the twin and reader from twinJSON under the
// class's name opts and runs the shared battery against writer.
func runReparseBattery(t *testing.T, nc reparseNameClass, writer *avro.Schema, twinJSON string, in, want map[string]any) {
	t.Helper()
	twin, err := avro.Parse(twinJSON, nameOnlyOpts(nc)...)
	if err != nil {
		t.Fatalf("twin parse: %v", err)
	}
	reader, err := avro.Parse(reparseAddedReader(twinJSON), nameOnlyOpts(nc)...)
	if err != nil {
		t.Fatalf("reader parse: %v", err)
	}
	battery(t, nc, writer, twin, reader, in, nil, want)
}

// battery runs the shared per-cell assertions for the reparse matrices.
// writer is the schema under test (plain-parsed or cache-parsed, possibly
// custom-typed). twin is the independent oracle: the same self-contained
// schema text parsed directly with the name opt only. reader adds a
// defaulted field.
func battery(t *testing.T, nc reparseNameClass, writer, twin, reader *avro.Schema, in, inCt, want map[string]any) {
	t.Helper()
	// Names pass through verbatim.
	if canon := string(writer.Canonical()); !strings.Contains(canon, `"`+nc.full+`"`) {
		t.Errorf("canonical does not carry fullname %q verbatim: %s", nc.full, canon)
	}
	// String()/Canonical() re-parse self-contained under the user's
	// validator, preserving canonical identity.
	re, err := avro.Parse(writer.String(), nameOnlyOpts(nc)...)
	if err != nil {
		t.Fatalf("String() re-parse: %v\nString(): %s", err, writer.String())
	}
	if !bytes.Equal(re.Canonical(), writer.Canonical()) {
		t.Errorf("String() re-parse canonical diverges:\n re: %s\ngot: %s", re.Canonical(), writer.Canonical())
	}
	reC, err := avro.Parse(string(writer.Canonical()), nameOnlyOpts(nc)...)
	if err != nil {
		t.Fatalf("Canonical() re-parse: %v\nCanonical(): %s", err, writer.Canonical())
	}
	if !bytes.Equal(reC.Canonical(), writer.Canonical()) {
		t.Errorf("Canonical() re-parse not idempotent:\n re: %s\ngot: %s", reC.Canonical(), writer.Canonical())
	}
	// Parity with the twin: canonical, fingerprint, wire bytes.
	if !bytes.Equal(writer.Canonical(), twin.Canonical()) {
		t.Errorf("canonical diverges from twin:\n got: %s\nwant: %s", writer.Canonical(), twin.Canonical())
	}
	if fp, fpTwin := writer.Fingerprint(avro.NewRabin()), twin.Fingerprint(avro.NewRabin()); !bytes.Equal(fp, fpTwin) {
		t.Errorf("rabin fingerprint diverges from twin: %x vs %x", fp, fpTwin)
	}
	wire, err := writer.Encode(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	wireTwin, err := twin.Encode(in)
	if err != nil {
		t.Fatalf("twin encode: %v", err)
	}
	if !bytes.Equal(wire, wireTwin) {
		t.Errorf("wire bytes diverge from twin: %x vs %x", wire, wireTwin)
	}
	if inCt != nil {
		wireCt, err := writer.Encode(inCt)
		if err != nil {
			t.Fatalf("custom-typed encode: %v", err)
		}
		if !bytes.Equal(wireCt, wire) {
			t.Errorf("custom-typed input wire bytes diverge: %x vs %x", wireCt, wire)
		}
	}
	// Resolve survives, then binary decode and resolved DecodeJSON agree
	// on the exact expected value (writer-shaped JSON from the twin).
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	var viaBinary map[string]any
	if _, err := resolved.Decode(wire, &viaBinary); err != nil {
		t.Fatalf("resolved binary decode: %v", err)
	}
	if !reflect.DeepEqual(viaBinary, want) {
		t.Errorf("resolved binary decode: got %#v, want %#v", viaBinary, want)
	}
	wjson, err := twin.EncodeJSON(in)
	if err != nil {
		t.Fatalf("twin EncodeJSON: %v", err)
	}
	var viaJSON map[string]any
	if err := resolved.DecodeJSON(wjson, &viaJSON); err != nil {
		t.Fatalf("resolved DecodeJSON(%s): %v", wjson, err)
	}
	if !reflect.DeepEqual(viaJSON, want) {
		t.Errorf("resolved DecodeJSON: got %#v, want %#v", viaJSON, want)
	}
}

// fastavroRabinBytes converts a fastavro-printed CRC-64-AVRO fingerprint
// (little-endian hex, fastavro printing the single-object wire order) to the
// big-endian byte order Schema.Fingerprint(NewRabin()) returns. Pins then
// compare bytes rather than presentation. The 64-bit value is the same one;
// only the order differs.
func fastavroRabinBytes(t *testing.T, leHex string) []byte {
	t.Helper()
	b, err := hex.DecodeString(leHex)
	if err != nil {
		t.Fatalf("bad hex %q: %v", leHex, err)
	}
	slices.Reverse(b)
	return b
}

// Canonical-form parity for the empty-name classes against executed fastavro
// 1.12.2. Every Canonical() must byte-match fastavro's PCF and Rabin
// fingerprint for {class} x {position}, and re-parse under the user's
// accept-all validator. The reference-position bare cell is a documented
// divergence: we structurally reject the "" reference spelling upstream of any
// validator while fastavro accepts it. fastavro rejects forward references
// entirely, so that cell pins the Java-rule first-occurrence form with no
// comparison.
func TestMatrix_CanonicalEmptyNameFastavroParity(t *testing.T) {
	acceptAll := func(string) error { return nil }
	const (
		bareDef = `{"type":"record","name":"","fields":[{"name":"f","type":"long"}]}`
		okDef   = `{"type":"record","name":"","namespace":"ok","fields":[{"name":"f","type":"long"}]}`
		abDef   = `{"type":"record","name":"R","namespace":"a..b","fields":[{"name":"f","type":"long"}]}`
	)
	nested := func(def string) string {
		return `{"type":"record","name":"Top","fields":[{"name":"a","type":` + def + `}]}`
	}
	diamond := func(def, ref string) string {
		return `{"type":"record","name":"Top","fields":[{"name":"a","type":` + def + `},{"name":"b","type":"` + ref + `"}]}`
	}
	cells := []struct {
		key       string
		schema    string
		wantPCF   string // fastavro's executed PCF, byte-for-byte
		rabinLEHx string // fastavro's executed fingerprint (little-endian hex)
	}{
		{"root/bare", bareDef,
			`{"name":"","type":"record","fields":[{"name":"f","type":"long"}]}`, "3d741707ff4bfa45"},
		{"root/ok", okDef,
			`{"name":"ok.","type":"record","fields":[{"name":"f","type":"long"}]}`, "6cfba61a610c50c2"},
		{"root/ab", abDef,
			`{"name":"a..b.R","type":"record","fields":[{"name":"f","type":"long"}]}`, "cad3b2bee0fed6fa"},
		{"nested/bare", nested(bareDef),
			`{"name":"Top","type":"record","fields":[{"name":"a","type":{"name":"","type":"record","fields":[{"name":"f","type":"long"}]}}]}`, "c5948d734d487874"},
		{"nested/ok", nested(okDef),
			`{"name":"Top","type":"record","fields":[{"name":"a","type":{"name":"ok.","type":"record","fields":[{"name":"f","type":"long"}]}}]}`, "0c2a9622507ffbc7"},
		{"nested/ab", nested(abDef),
			`{"name":"Top","type":"record","fields":[{"name":"a","type":{"name":"a..b.R","type":"record","fields":[{"name":"f","type":"long"}]}}]}`, "493fc67a41ba56e9"},
		{"reference/ok", diamond(okDef, "ok."),
			`{"name":"Top","type":"record","fields":[{"name":"a","type":{"name":"ok.","type":"record","fields":[{"name":"f","type":"long"}]}},{"name":"b","type":"ok."}]}`, "3801ed908d3951d8"},
		{"reference/ab", diamond(abDef, "a..b.R"),
			`{"name":"Top","type":"record","fields":[{"name":"a","type":{"name":"a..b.R","type":"record","fields":[{"name":"f","type":"long"}]}},{"name":"b","type":"a..b.R"}]}`, "b6e281b385d18d8c"},
		{"recursive/ok", `{"type":"record","name":"","namespace":"ok","fields":[{"name":"f","type":"long"},{"name":"next","type":["null","ok."]}]}`,
			`{"name":"ok.","type":"record","fields":[{"name":"f","type":"long"},{"name":"next","type":["null","ok."]}]}`, "fe8d701fc807f4ec"},
	}
	for _, c := range cells {
		t.Run(c.key, func(t *testing.T) {
			s, err := avro.Parse(c.schema, avro.WithLaxNames(acceptAll))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got := string(s.Canonical()); got != c.wantPCF {
				t.Errorf("canonical vs fastavro PCF:\n got %s\nwant %s", got, c.wantPCF)
			}
			if got, want := s.Fingerprint(avro.NewRabin()), fastavroRabinBytes(t, c.rabinLEHx); !bytes.Equal(got, want) {
				t.Errorf("rabin bytes vs fastavro: got %x, want %x", got, want)
			}
			re, err := avro.Parse(string(s.Canonical()), avro.WithLaxNames(acceptAll))
			if err != nil {
				t.Fatalf("canonical re-parse under accept-all: %v", err)
			}
			if !bytes.Equal(re.Canonical(), s.Canonical()) {
				t.Errorf("canonical not idempotent:\n re %s\ngot %s", re.Canonical(), s.Canonical())
			}
		})
	}

	// Documented divergence: the "" reference spelling. We reject it
	// structurally (a field type must be a primitive, complex, or union);
	// fastavro accepts and resolves it (executed: PCF keeps the bare ""
	// ref, rabin f9afa0dabf6cd566). Pinned as the rejection.
	t.Run("reference/bare-divergence", func(t *testing.T) {
		_, err := avro.Parse(diamond(bareDef, ""), avro.WithLaxNames(acceptAll))
		if err == nil {
			t.Fatal(`"" reference unexpectedly accepted (divergence pin flipped — recalibrate against fastavro)`)
		}
		if !strings.Contains(err.Error(), "not a primitive") {
			t.Errorf("rejection shape changed: %v", err)
		}
	})

	// Forward reference to the empty-named type, which only we accept
	// (fastavro rejects every forward reference; executed: UnknownType
	// "ok."). Java's first-occurrence rule: the full body is emitted at
	// the first walk occurrence (the referencing field), a bare fullname
	// afterward.
	t.Run("fwdref/ok", func(t *testing.T) {
		s := avrotest.MustParse(t, `{"type":"record","name":"Top","fields":[{"name":"b","type":"ok."},{"name":"a","type":`+okDef+`}]}`, avro.WithLaxNames(acceptAll))
		want := `{"name":"Top","type":"record","fields":[{"name":"b","type":{"name":"ok.","type":"record","fields":[{"name":"f","type":"long"}]}},{"name":"a","type":"ok."}]}`
		if got := string(s.Canonical()); got != want {
			t.Errorf("fwd-ref first-occurrence canonical:\n got %s\nwant %s", got, want)
		}
		if _, err := avro.Parse(string(s.Canonical()), avro.WithLaxNames(acceptAll)); err != nil {
			t.Fatalf("canonical re-parse: %v", err)
		}
	})
}

// Tagged-union JSON naming for an empty-named union branch (reachable only
// under a user WithLaxNames fn). The tag is the branch's fullname, as for any
// other named branch, matching fastavro's json_writer. fastavro cannot write
// the bare class but its reader accepts the "" key, so our `{"":"A"}` is
// fastavro-readable on both.
//
//	{class: bare "", namespaced "ok."}
//	  x {tagged encode emission, decode of own emission (plain and
//	     TaggedUnions), tagged-map encode routing, resolved DecodeJSON routing}
func TestMatrix_EmptyNameTaggedUnion(t *testing.T) {
	acceptAll := func(string) error { return nil }
	for _, tc := range []struct {
		class    string
		schema   string
		wantTag  string // exact tagged EncodeJSON output for symbol "A"
		mapKeys  []string
		rejected []string
	}{
		{
			class:   "ok",
			schema:  `["null",{"type":"enum","name":"","namespace":"ok","symbols":["A","B"]}]`,
			wantTag: `{"ok.":"A"}`,
			// The "" key routes through the unique-short-name fallback
			// (unqualified("ok.") is ""), the same input leniency every
			// namespaced branch's short name gets. The kind never tags a
			// named branch (goavro/Java: the envelope key is the fullname).
			mapKeys:  []string{"ok.", ""},
			rejected: []string{"enum"},
		},
		{
			class:    "bare",
			schema:   `["null",{"type":"enum","name":"","symbols":["A","B"]}]`,
			wantTag:  `{"":"A"}`,
			mapKeys:  []string{""},
			rejected: []string{"ok.", "enum"},
		},
	} {
		t.Run(tc.class, func(t *testing.T) {
			s, err := avro.Parse(tc.schema, avro.WithLaxNames(acceptAll))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}

			got, err := s.EncodeJSON("A", avro.TaggedUnions())
			if err != nil {
				t.Fatalf("tagged EncodeJSON: %v", err)
			}
			if string(got) != tc.wantTag {
				t.Errorf("tagged emission: got %s, want %s", got, tc.wantTag)
			}

			var plain any
			if err := s.DecodeJSON(got, &plain); err != nil {
				t.Errorf("plain decode of own tagged emission: %v", err)
			} else if plain != "A" {
				t.Errorf("plain decode: got %#v, want %q", plain, "A")
			}
			wantKey := "ok."
			if tc.class == "bare" {
				wantKey = ""
			}
			var tagged any
			if err := s.DecodeJSON(got, &tagged, avro.TaggedUnions()); err != nil {
				t.Errorf("tagged decode of own tagged emission: %v", err)
			} else if !reflect.DeepEqual(tagged, map[string]any{wantKey: "A"}) {
				t.Errorf("tagged decode: got %#v, want map[%q:A]", tagged, wantKey)
			}

			wire, err := s.Encode("A")
			if err != nil {
				t.Fatalf("binary encode: %v", err)
			}
			for _, key := range tc.mapKeys {
				in := map[string]any{key: "A"}
				bin, err := s.Encode(in)
				if err != nil {
					t.Errorf("binary Encode(map[%q]): %v", key, err)
				} else if !bytes.Equal(bin, wire) {
					t.Errorf("binary Encode(map[%q]): wire %x, want %x", key, bin, wire)
				}
				j, err := s.EncodeJSON(in, avro.TaggedUnions())
				if err != nil {
					t.Errorf("tagged EncodeJSON(map[%q]): %v", key, err)
				} else if string(j) != tc.wantTag {
					t.Errorf("tagged EncodeJSON(map[%q]): got %s, want %s", key, j, tc.wantTag)
				}
			}
			for _, key := range tc.rejected {
				if _, err := s.Encode(map[string]any{key: "A"}); err == nil {
					t.Errorf("binary Encode(map[%q]) unexpectedly accepted", key)
				}
			}
		})
	}

	// Resolved DecodeJSON keeps the empty-named branch's identity through
	// the tagged intermediate. The writer names the enum branch whose value
	// "B" would also satisfy the string branch, and the reader's enum drops
	// "B" for its declared default. A routing flip is then observable both as
	// branch identity and as the resolved value.
	t.Run("resolved-routing", func(t *testing.T) {
		w, err := avro.Parse(`["null",{"type":"enum","name":"","namespace":"ok","symbols":["A","B"]},"string"]`, avro.WithLaxNames(acceptAll))
		if err != nil {
			t.Fatalf("parse writer: %v", err)
		}
		r, err := avro.Parse(`["null","string",{"type":"enum","name":"","namespace":"ok","symbols":["A"],"default":"A"}]`, avro.WithLaxNames(acceptAll))
		if err != nil {
			t.Fatalf("parse reader: %v", err)
		}
		resolved, err := avro.Resolve(w, r)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		wire, err := w.Encode(map[string]any{"ok.": "B"})
		if err != nil {
			t.Fatalf("writer encode: %v", err)
		}
		var viaBinary, viaJSON any
		if _, err := resolved.Decode(wire, &viaBinary); err != nil {
			t.Fatalf("resolved binary decode: %v", err)
		}
		if err := resolved.DecodeJSON([]byte(`{"ok.": "B"}`), &viaJSON); err != nil {
			t.Fatalf("resolved DecodeJSON: %v", err)
		}
		if viaBinary != "A" || viaJSON != "A" {
			t.Errorf("resolved enum-default routing: binary %#v, JSON %#v, want %q on both (string-branch flip would keep %q)", viaBinary, viaJSON, "A", "B")
		}
	})
}

// ---------- resolve_custom_record_test.go ----------

// A record-level CustomType (AvroType:"record") Decode callback fires on a
// direct Decode, applyCustomTypes wiring record nodes at build time. It must
// also fire through a resolved decode. resolveRecord builds a fresh node.
// Unless it re-applies the reader's custom wiring as every other resolve arm
// does, any real evolution silently returns the raw map[string]any, a
// direct-vs-resolved divergence. The callback is value-transforming, so "fired"
// is distinguishable from "raw passthrough".
func TestRegression_RecordCustomTypeThroughResolve(t *testing.T) {
	const marker = "WRAPPED_BY_CUSTOM"
	newCT := func() avro.CustomType {
		return avro.CustomType{
			AvroType: "record",
			Decode: func(v any, _ *avro.SchemaNode) (any, error) {
				return map[string]any{marker: v}, nil
			},
		}
	}

	readerJSON := `{"type":"record","name":"R","fields":[
		{"name":"a","type":"int"},{"name":"b","type":"string"}]}`

	// Each writer schema is compatible with the reader but has a different
	// canonical form. Resolve therefore builds a real resolving decoder,
	// bypassing the canonical-equality fast path that returns the reader as-is.
	writers := map[string]string{
		"reorder": `{"type":"record","name":"R","fields":[
			{"name":"b","type":"string"},{"name":"a","type":"int"}]}`,
		"drop_extra_writer_field": `{"type":"record","name":"R","fields":[
			{"name":"a","type":"int"},{"name":"b","type":"string"},{"name":"c","type":"long"}]}`,
		"add_reader_default": `{"type":"record","name":"R","fields":[
			{"name":"a","type":"int"}]}`, // reader's "b" filled from... needs a default
	}

	// Control: a direct decode fires the custom (proves the harness + callback).
	reader := avro.MustParse(readerJSON, newCT())
	wireDirect, err := reader.AppendEncode(nil, map[string]any{"a": int32(1), "b": "x"})
	if err != nil {
		t.Fatalf("direct encode: %v", err)
	}
	var direct any
	if _, err := reader.Decode(wireDirect, &direct); err != nil {
		t.Fatalf("direct decode: %v", err)
	}
	if dm, ok := direct.(map[string]any); !ok || dm[marker] == nil {
		t.Fatalf("control: record custom did not fire on DIRECT decode: %#v", direct)
	}

	for name, writerJSON := range writers {
		t.Run(name, func(t *testing.T) {
			// The "add_reader_default" case needs the reader's dropped-from-writer
			// field to have a default; rebuild the reader accordingly.
			rJSON := readerJSON
			if name == "add_reader_default" {
				rJSON = `{"type":"record","name":"R","fields":[
					{"name":"a","type":"int"},{"name":"b","type":"string","default":"d"}]}`
			}
			r := avro.MustParse(rJSON, newCT())
			w := avro.MustParse(writerJSON)
			res, err := avro.Resolve(w, r)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}

			val := map[string]any{"a": int32(1), "b": "x"}
			if name == "drop_extra_writer_field" {
				val["c"] = int64(9)
			}
			if name == "add_reader_default" {
				delete(val, "b")
			}
			wire, err := w.AppendEncode(nil, val)
			if err != nil {
				t.Fatalf("writer encode: %v", err)
			}

			// Resolved binary decode must fire the custom.
			var gotBin any
			if _, err := res.Decode(wire, &gotBin); err != nil {
				t.Fatalf("resolved binary decode: %v", err)
			}
			if m, ok := gotBin.(map[string]any); !ok || m[marker] == nil {
				t.Fatalf("record custom DROPPED through resolved binary decode: %#v", gotBin)
			}

			// Resolved JSON decode (consumes writer-shaped JSON) must agree.
			wireJSON, err := w.AppendEncodeJSON(nil, val)
			if err != nil {
				t.Fatalf("writer encodeJSON: %v", err)
			}
			var gotJSON any
			if err := res.DecodeJSON(wireJSON, &gotJSON); err != nil {
				t.Fatalf("resolved JSON decode: %v", err)
			}
			if m, ok := gotJSON.(map[string]any); !ok || m[marker] == nil {
				t.Fatalf("record custom DROPPED through resolved JSON decode: %#v", gotJSON)
			}
		})
	}
}

// A record-level custom must also fire through resolution of a recursive
// (self-referential) record at every level. resolveNode's cycle placeholder
// copies the resolved node's contents, so the custom wrap applied before the
// copy must propagate to the inner recursive references.
func TestRegression_RecursiveRecordCustomThroughResolve(t *testing.T) {
	const marker = "WRAP"
	ct := avro.CustomType{
		AvroType: "record",
		Decode: func(v any, _ *avro.SchemaNode) (any, error) {
			return map[string]any{marker: v}, nil
		},
	}
	// Reader reorders fields vs writer so Resolve builds a real resolving decoder.
	reader := avro.MustParse(`{"type":"record","name":"LL","fields":[{"name":"v","type":"int"},{"name":"next","type":["null","LL"]}]}`, ct)
	writer := avro.MustParse(`{"type":"record","name":"LL","fields":[{"name":"next","type":["null","LL"]},{"name":"v","type":"int"}]}`)
	res := avrotest.MustResolve(t, writer, reader)
	val := map[string]any{"v": int32(1), "next": map[string]any{"v": int32(2), "next": nil}}
	wire := avrotest.MustAppendEncode(t, writer, nil, val)
	var got any
	if _, err := res.Decode(wire, &got); err != nil {
		t.Fatalf("resolved decode: %v", err)
	}
	m, ok := got.(map[string]any)
	if !ok || m[marker] == nil {
		t.Fatalf("record custom dropped at outer level: %#v", got)
	}
	inner, ok := m[marker].(map[string]any)["next"].(map[string]any)
	if !ok || inner[marker] == nil {
		t.Fatalf("record custom dropped at inner recursive level: %#v", m[marker])
	}
}

// Control: a non-custom resolved record decode must *not* grow a marker key.
// The re-applied wiring is a no-op when no CustomType is registered.
func TestResolvedRecordWithoutCustomIsUnwrapped(t *testing.T) {
	r := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`)
	w := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"b","type":"string"},{"name":"a","type":"int"}]}`)
	res := avrotest.MustResolve(t, w, r)
	wire := avrotest.MustAppendEncode(t, w, nil, map[string]any{"a": int32(1), "b": "x"})
	var got map[string]any
	avrotest.MustDecode(t, res, wire, &got)
	if fmt.Sprintf("%v", got["a"]) != "1" || got["b"] != "x" {
		t.Fatalf("plain resolved record decode wrong: %#v", got)
	}
}

// ---------- union_custom_pointer_reuse_test.go ----------

// A CustomType whose Decode returns a pointer, registered on a logical at a
// union branch, must decode identically on both wires even when the target is
// reused and already holds a non-nil pointer. That is the
// streaming-decode-into-a-reused-struct pattern. The binary union deser passes
// the un-indirected target to the custom wrapper. A JSON union decoder that
// pre-dereferences a reused *T held in an interface before dispatching to the
// branch rejects the fresh pointer result from the second datum onward. We pin
// per-branch indirection parity here.

type ucpEvent struct{ T time.Time }

func ucpCustom() avro.CustomType {
	return avro.CustomType{
		LogicalType: "timestamp-millis",
		AvroType:    "long",
		GoType:      reflect.TypeFor[*ucpEvent](),
		Decode: func(v any, _ *avro.SchemaNode) (any, error) {
			return &ucpEvent{T: time.UnixMilli(v.(int64)).UTC()}, nil
		},
		Encode: func(v any, _ *avro.SchemaNode) (any, error) {
			return v.(*ucpEvent).T.UnixMilli(), nil
		},
	}
}

func TestRegression_UnionCustomDecodePointerReusedTargetParity(t *testing.T) {
	const schema = `{"type":"record","name":"R","fields":[{"name":"when","type":["null",{"type":"long","logicalType":"timestamp-millis"}]}]}`
	s := avrotest.MustParse(t, schema, avro.WithCustomType(ucpCustom()))
	type Event struct {
		When any `avro:"when"`
	}
	want := time.UnixMilli(1700000000000).UTC()
	mk := func() Event { return Event{When: &ucpEvent{T: want}} }

	// Pre-encode three identical datums on each wire.
	var bin [][]byte
	var jsn [][]byte
	for i := 0; i < 3; i++ {
		b, err := s.AppendEncode(nil, mk())
		if err != nil {
			t.Fatalf("binary encode %d: %v", i, err)
		}
		bin = append(bin, b)
		j, err := s.AppendEncodeJSON(nil, mk())
		if err != nil {
			t.Fatalf("json encode %d: %v", i, err)
		}
		jsn = append(jsn, j)
	}

	check := func(name string, got Event, i int) {
		ev, ok := got.When.(*ucpEvent)
		if !ok {
			t.Fatalf("%s datum %d: got %T, want *ucpEvent", name, i, got.When)
		}
		if !ev.T.Equal(want) {
			t.Fatalf("%s datum %d: got %v want %v", name, i, ev.T, want)
		}
	}

	// Reused target (the streaming pattern): the same struct value decoded into
	// repeatedly, its any field carrying the prior *ucpEvent.
	var evB Event
	for i, b := range bin {
		if _, err := s.Decode(b, &evB); err != nil {
			t.Fatalf("binary decode (reused) %d: %v", i, err)
		}
		check("binary-reused", evB, i)
	}
	var evJ Event
	for i, j := range jsn {
		if err := s.DecodeJSON(j, &evJ); err != nil {
			t.Fatalf("json decode (reused) %d: %v", i, err)
		}
		check("json-reused", evJ, i)
	}

	// Fresh nil-interface target (first decode) must also produce the *ucpEvent
	// on both wires, the boundary the reuse case builds on.
	var freshB, freshJ Event
	if _, err := s.Decode(bin[0], &freshB); err != nil {
		t.Fatalf("binary decode (fresh): %v", err)
	}
	check("binary-fresh", freshB, 0)
	if err := s.DecodeJSON(jsn[0], &freshJ); err != nil {
		t.Fatalf("json decode (fresh): %v", err)
	}
	check("json-fresh", freshJ, 0)

	// TaggedUnions decode: the {branchName: value} envelope must wrap the custom
	// result identically on both wires, and still survive target reuse. The
	// envelope value is the (pointer) custom result; assert it key-agnostically.
	taggedVal := func(name string, v any) *ucpEvent {
		m, ok := v.(map[string]any)
		if !ok || len(m) != 1 {
			t.Fatalf("%s: got %T (%v), want single-entry map envelope", name, v, v)
		}
		for _, e := range m {
			ev, ok := e.(*ucpEvent)
			if !ok {
				t.Fatalf("%s: envelope value %T, want *ucpEvent", name, e)
			}
			return ev
		}
		return nil
	}
	var tagB, tagJ Event
	for i := range bin {
		if _, err := s.Decode(bin[i], &tagB, avro.TaggedUnions()); err != nil {
			t.Fatalf("binary tagged decode %d: %v", i, err)
		}
		if err := s.DecodeJSON(jsn[i], &tagJ, avro.TaggedUnions()); err != nil {
			t.Fatalf("json tagged decode %d: %v", i, err)
		}
		eb := taggedVal("binary-tagged", tagB.When)
		ej := taggedVal("json-tagged", tagJ.When)
		if !eb.T.Equal(want) || !ej.T.Equal(want) {
			t.Fatalf("tagged %d: binary=%v json=%v want %v", i, eb.T, ej.T, want)
		}
	}
}

// A CustomType.Decode returning a pointer must decode into a concrete *T field
// target through a union branch on every union shape and both wires. The
// general union deser passes the un-indirected target to the branch fn, so
// setCustomResult lands the *T. A 2-branch null-union fast path that
// pre-dereferences a concrete pointer first fails a *T target in a
// ["null",customLong] union while succeeding in a 3+-branch one. That is an
// arbitrary inconsistency, rejecting a target the general path decodes. The
// JSON unionTarget derefs concrete pointers the same way.
func TestRegression_UnionCustomDecodePointerFieldTarget(t *testing.T) {
	type EventPtr struct {
		When *ucpEvent `avro:"when"`
	}
	want := time.UnixMilli(1700000000000).UTC()

	cases := []struct {
		name   string
		schema string
	}{
		{"2-branch-null-union", `{"type":"record","name":"R","fields":[{"name":"when","type":["null",{"type":"long","logicalType":"timestamp-millis"}]}]}`},
		// A 3+-branch union routes through the general deserUnion.deser path.
		{"3-branch-union", `{"type":"record","name":"R","fields":[{"name":"when","type":["null",{"type":"long","logicalType":"timestamp-millis"},"string"]}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := avrotest.MustParse(t, tc.schema, avro.WithCustomType(ucpCustom()))
			in := EventPtr{When: &ucpEvent{T: want}}
			b := avrotest.MustAppendEncode(t, s, nil, in)
			var gotB EventPtr
			if _, err := s.Decode(b, &gotB); err != nil {
				t.Fatalf("binary decode into *T field: %v", err)
			}
			if gotB.When == nil || !gotB.When.T.Equal(want) {
				t.Fatalf("binary *T field: got %v", gotB.When)
			}
			j := avrotest.MustAppendEncodeJSON(t, s, nil, in)
			var gotJ EventPtr
			if err := s.DecodeJSON(j, &gotJ); err != nil {
				t.Fatalf("json decode into *T field: %v", err)
			}
			if gotJ.When == nil || !gotJ.When.T.Equal(want) {
				t.Fatalf("json *T field: got %v", gotJ.When)
			}
		})
	}
}

// Boundary-1 control: a non-custom union decode into a reused interface holding
// a manually pre-populated *T must keep doing in-place reuse (the result's
// dynamic type stays *T) identically on binary and JSON. The per-branch
// indirection must *not* regress this to a boxed value.
func TestRegression_UnionNonCustomReuseInPlaceUnchanged(t *testing.T) {
	s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"v","type":["null","long"]}]}`)
	type Rec struct {
		V any `avro:"v"`
	}
	bw, _ := s.AppendEncode(nil, Rec{V: int64(42)})
	jw, _ := s.AppendEncodeJSON(nil, Rec{V: int64(42)})

	// Manual *int64 pre-population reuses in place, holding *int64 on both.
	for _, tc := range []struct {
		name   string
		decode func(target *Rec) error
		wire   []byte
	}{
		{"binary", func(r *Rec) error { _, e := s.Decode(bw, r); return e }, bw},
		{"json", func(r *Rec) error { return s.DecodeJSON(jw, r) }, jw},
	} {
		p := int64(7)
		r := Rec{V: &p}
		if err := tc.decode(&r); err != nil {
			t.Fatalf("%s decode: %v", tc.name, err)
		}
		pp, ok := r.V.(*int64)
		if !ok {
			t.Fatalf("%s: in-place reuse lost: got %T, want *int64", tc.name, r.V)
		}
		if *pp != 42 {
			t.Fatalf("%s: got %d want 42", tc.name, *pp)
		}
	}

	// Fresh nil interface gives a boxed value (int64) on both.
	for _, tc := range []struct {
		name   string
		decode func(target *Rec) error
	}{
		{"binary", func(r *Rec) error { _, e := s.Decode(bw, r); return e }},
		{"json", func(r *Rec) error { return s.DecodeJSON(jw, r) }},
	} {
		var r Rec
		if err := tc.decode(&r); err != nil {
			t.Fatalf("%s fresh decode: %v", tc.name, err)
		}
		if got, ok := r.V.(int64); !ok || got != 42 {
			t.Fatalf("%s fresh: got %T %v, want int64 42", tc.name, r.V, r.V)
		}
	}
}

// ---------- cache_canonical_test.go ----------

// TestRegression_SchemaCacheCanonicalSelfContained pins that a schema built via
// SchemaCache that references a type registered in a prior Parse produces
// self-contained metadata forms, identical to the logically-equal
// inline-defined schema. The cache stores only the resolved node. A JSON form
// holding a dangling bare reference then gives a non-re-parseable canonical
// form and a cross-language-divergent fingerprint, breaking
// single-object-encoding interop.
func TestRegression_SchemaCacheCanonicalSelfContained(t *testing.T) {
	var c avro.SchemaCache
	if _, err := c.Parse(`{"type":"record","name":"ns.Inner","fields":[{"name":"x","type":"int"}]}`); err != nil {
		t.Fatalf("register Inner: %v", err)
	}
	viaCache, err := c.Parse(`{"type":"record","name":"Outer","fields":[` +
		`{"name":"i","type":"ns.Inner"},{"name":"j","type":"ns.Inner"}]}`)
	if err != nil {
		t.Fatalf("parse Outer via cache: %v", err)
	}
	inline := avro.MustParse(`{"type":"record","name":"Outer","fields":[` +
		`{"name":"i","type":{"type":"record","name":"ns.Inner","fields":[{"name":"x","type":"int"}]}},` +
		`{"name":"j","type":"ns.Inner"}]}`)

	// Control: identical wire (same logical schema).
	val := map[string]any{"i": map[string]any{"x": int32(1)}, "j": map[string]any{"x": int32(2)}}
	wc, err := viaCache.Encode(val)
	if err != nil {
		t.Fatalf("cache encode: %v", err)
	}
	wi, err := inline.Encode(val)
	if err != nil {
		t.Fatalf("inline encode: %v", err)
	}
	if string(wc) != string(wi) {
		t.Fatalf("control: wire differs (not the same logical schema)")
	}

	// Canonical form must be self-contained and equal to the inline schema's.
	if string(viaCache.Canonical()) != string(inline.Canonical()) {
		t.Errorf("Canonical() diverges:\n cache : %s\n inline: %s", viaCache.Canonical(), inline.Canonical())
	}
	if _, err := avro.Parse(string(viaCache.Canonical())); err != nil {
		t.Errorf("Parse(cache.Canonical()) FAILS — canonical form is not a valid schema: %v", err)
	}

	// Fingerprint must match (cross-language / SOE interop).
	if string(viaCache.Fingerprint(avro.NewRabin())) != string(inline.Fingerprint(avro.NewRabin())) {
		t.Errorf("Fingerprint() diverges for the same logical schema")
	}

	// Root() must rebuild a self-contained tree.
	root := viaCache.Root()
	if _, err := root.Schema(); err != nil {
		t.Errorf("Root().Schema() FAILS to rebuild a cache-built schema: %v", err)
	}
}

// TestRegression_SchemaCacheSOEInterop pins the user-visible consequence: a
// single-object-encoded message from a cache-built producer round-trips through
// a consumer holding the logically-identical inline schema (fingerprints
// match).
func TestRegression_SchemaCacheSOEInterop(t *testing.T) {
	var c avro.SchemaCache
	if _, err := c.Parse(`{"type":"record","name":"ns.Inner","fields":[{"name":"x","type":"int"}]}`); err != nil {
		t.Fatalf("register Inner: %v", err)
	}
	producer, err := c.Parse(`{"type":"record","name":"Outer","fields":[` +
		`{"name":"i","type":"ns.Inner"},{"name":"j","type":"ns.Inner"}]}`)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	consumer := avro.MustParse(`{"type":"record","name":"Outer","fields":[` +
		`{"name":"i","type":{"type":"record","name":"ns.Inner","fields":[{"name":"x","type":"int"}]}},` +
		`{"name":"j","type":"ns.Inner"}]}`)

	val := map[string]any{"i": map[string]any{"x": int32(1)}, "j": map[string]any{"x": int32(2)}}
	msg, err := producer.AppendSingleObject(nil, val)
	if err != nil {
		t.Fatalf("AppendSingleObject: %v", err)
	}
	var out map[string]any
	if _, err := consumer.DecodeSingleObject(msg, &out); err != nil {
		t.Errorf("DecodeSingleObject across cache/inline of the same schema FAILS: %v", err)
	}
}

// The cross-parse self-containment splice harvests inherited definitions by
// walking the prior schema's JSON tree, so it must mirror the parser exactly.
// That means case-insensitive object keys, and the flat goavro field form. In
// that form a field carries a named type's defining key alongside its own, and
// the parser lifts it into a registered type. A walker that only descends into
// a field's "type" value, reading keys case-sensitively, never collects those
// definitions. A later cross-parse reference then stays a dangling bare ref.
func TestRegression_SchemaCacheSelfContainedFlatFormDef(t *testing.T) {
	var c avro.SchemaCache
	// Prior parse defines enum E in the flat field form.
	if _, err := c.Parse(`{"type":"record","name":"H","fields":[{"name":"E","type":"enum","symbols":["A","B"]}]}`); err != nil {
		t.Fatalf("register flat-form E: %v", err)
	}
	viaCache, err := c.Parse(`{"type":"record","name":"R","fields":[{"name":"x","type":"E"}]}`)
	if err != nil {
		t.Fatalf("reference E via cache: %v", err)
	}
	inline := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"x","type":{"type":"enum","name":"E","symbols":["A","B"]}}]}`)

	assertSelfContained(t, viaCache, inline, map[string]any{"x": "B"})
}

// A flat ("linkedin/goavro") field can also carry an unnamed complex kind.
// {"name":"a","type":"array","items":...} puts the element type in the field's
// own "items" key, and the wire parser lifts it exactly like the named flat
// kinds (flatFieldNeedsLift covers all six). A cross-parse reference inside
// those items resolves against the cache and the wire codec works. The
// self-containment walkers must splice the same subtree, or the JSON-derived
// forms keep a dangling bare reference and the fingerprint diverges. The
// nested-spelling twin is the control.
func TestRegression_FlatArrayFieldCrossParseRefSplices(t *testing.T) {
	const itemDef = `{"type":"record","name":"ns.Item","fields":[{"name":"x","type":"int"}]}`
	inline := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"list","type":"array","items":` + itemDef + `}]}`)
	val := map[string]any{"list": []any{map[string]any{"x": int32(1)}}}

	t.Run("nested-twin-control", func(t *testing.T) {
		var c avro.SchemaCache
		if _, err := c.Parse(itemDef); err != nil {
			t.Fatalf("register Item: %v", err)
		}
		viaCache, err := c.Parse(`{"type":"record","name":"R","fields":[{"name":"list","type":{"type":"array","items":"ns.Item"}}]}`)
		if err != nil {
			t.Fatalf("nested-spelling parse via cache: %v", err)
		}
		assertSelfContained(t, viaCache, inline, val)
	})

	t.Run("flat", func(t *testing.T) {
		var c avro.SchemaCache
		if _, err := c.Parse(itemDef); err != nil {
			t.Fatalf("register Item: %v", err)
		}
		viaCache, err := c.Parse(`{"type":"record","name":"R","fields":[{"name":"list","type":"array","items":"ns.Item"}]}`)
		if err != nil {
			t.Fatalf("flat-spelling parse via cache: %v", err)
		}
		assertSelfContained(t, viaCache, inline, val)
	})
}

// The map twin of the flat-array cross-parse reference, with the reference
// spelled by short name. The lift drops name/namespace keys for unnamed kinds
// (flatLiftTypeMap), so a flat field's items/values sit directly in the
// record's namespace scope and a short reference resolves there. The splice
// walkers must bind the reference in that same scope.
func TestRegression_FlatMapFieldCrossParseRefSplices(t *testing.T) {
	const itemDef = `{"type":"record","name":"ns.Item","fields":[{"name":"x","type":"int"}]}`
	inline := avro.MustParse(`{"type":"record","name":"ns.R","fields":[{"name":"m","type":"map","values":` + itemDef + `}]}`)
	val := map[string]any{"m": map[string]any{"k": map[string]any{"x": int32(2)}}}

	t.Run("nested-twin-control", func(t *testing.T) {
		var c avro.SchemaCache
		if _, err := c.Parse(itemDef); err != nil {
			t.Fatalf("register Item: %v", err)
		}
		viaCache, err := c.Parse(`{"type":"record","name":"ns.R","fields":[{"name":"m","type":{"type":"map","values":"Item"}}]}`)
		if err != nil {
			t.Fatalf("nested-spelling parse via cache: %v", err)
		}
		assertSelfContained(t, viaCache, inline, val)
	})

	t.Run("flat", func(t *testing.T) {
		var c avro.SchemaCache
		if _, err := c.Parse(itemDef); err != nil {
			t.Fatalf("register Item: %v", err)
		}
		viaCache, err := c.Parse(`{"type":"record","name":"ns.R","fields":[{"name":"m","type":"map","values":"Item"}]}`)
		if err != nil {
			t.Fatalf("flat-spelling parse via cache: %v", err)
		}
		assertSelfContained(t, viaCache, inline, val)
	})
}

// The definition direction of the flat array/map subtree: a named type defined
// inside a flat array field's items is lifted and registered by the wire
// parser, so later parses can reference it. The collection walker must also
// capture its definition, or a later referencing parse resolves on the wire but
// never splices, leaving its JSON-derived forms dangling. The nested-spelling
// twin of the same definition is the control.
func TestRegression_FlatArrayFieldInlineDefCollected(t *testing.T) {
	const dDef = `{"type":"record","name":"ns.D","fields":[{"name":"x","type":"int"}]}`
	inline := avro.MustParse(`{"type":"record","name":"R2","fields":[{"name":"d","type":` + dDef + `}]}`)
	val := map[string]any{"d": map[string]any{"x": int32(3)}}

	t.Run("nested-twin-control", func(t *testing.T) {
		var c avro.SchemaCache
		if _, err := c.Parse(`{"type":"record","name":"H","fields":[{"name":"list","type":{"type":"array","items":` + dDef + `}}]}`); err != nil {
			t.Fatalf("register nested-spelling def: %v", err)
		}
		viaCache, err := c.Parse(`{"type":"record","name":"R2","fields":[{"name":"d","type":"ns.D"}]}`)
		if err != nil {
			t.Fatalf("reference ns.D via cache: %v", err)
		}
		assertSelfContained(t, viaCache, inline, val)
	})

	t.Run("flat", func(t *testing.T) {
		var c avro.SchemaCache
		if _, err := c.Parse(`{"type":"record","name":"H","fields":[{"name":"list","type":"array","items":` + dDef + `}]}`); err != nil {
			t.Fatalf("register flat-spelling def: %v", err)
		}
		viaCache, err := c.Parse(`{"type":"record","name":"R2","fields":[{"name":"d","type":"ns.D"}]}`)
		if err != nil {
			t.Fatalf("reference ns.D via cache: %v", err)
		}
		assertSelfContained(t, viaCache, inline, val)
	})
}

// A case-variant object key ("tYpe") is an ordinary custom property. An object
// spelling its type only as a variant therefore has no type attribute: the
// registering parse fails loud and nothing enters the cache. A definition
// carrying a variant key beside its exact structure registers normally. The
// cross-parse splice preserves the variant verbatim as a prop, without letting
// it scope, rename, or restructure the def.
func TestRegression_SchemaCacheCaseVariantKey(t *testing.T) {
	var c avro.SchemaCache
	if _, err := c.Parse(`{"type":"record","name":"Outer","fields":[{"name":"inner","type":{"tYpe":"record","name":"Inner","fields":[{"name":"a","type":"int"}]}}]}`); err == nil {
		t.Fatalf("variant-only tYpe object accepted; it has no type attribute and must reject")
	}
	if _, err := c.Parse(`{"type":"record","name":"R","fields":[{"name":"x","type":"Inner"}]}`); err == nil {
		t.Fatalf("Inner resolved from a rejected parse; a failed parse must register nothing")
	}

	if _, err := c.Parse(`{"type":"record","name":"Outer2","fields":[{"name":"inner","type":{"type":"record","name":"Inner2","nAmespace":"decoy","fields":[{"name":"a","type":"int"}]}}]}`); err != nil {
		t.Fatalf("register Inner2: %v", err)
	}
	viaCache, err := c.Parse(`{"type":"record","name":"R2","fields":[{"name":"x","type":"Inner2"}]}`)
	if err != nil {
		t.Fatalf("reference Inner2 via cache: %v", err)
	}
	inline := avro.MustParse(`{"type":"record","name":"R2","fields":[{"name":"x","type":{"type":"record","name":"Inner2","nAmespace":"decoy","fields":[{"name":"a","type":"int"}]}}]}`)
	assertSelfContained(t, viaCache, inline, map[string]any{"x": map[string]any{"a": int32(7)}})
	spliced := viaCache.Root().Fields[0].Type
	if got := spliced.Props["nAmespace"]; !reflect.DeepEqual(got, "decoy") {
		t.Errorf(`spliced Props["nAmespace"] = %#v; want the variant preserved verbatim`, got)
	}
	if spliced.Namespace != "" {
		t.Errorf("Namespace = %q; a variant key must not scope the def", spliced.Namespace)
	}
}

// The splice walker (inlineTreeDefs) is the parallel of collectTreeDefs and
// must mirror the parser the same way. Otherwise a transitive inherited
// reference reached through a flat-form definition dangles: the
// self-containment re-parse then fails and the whole splice is abandoned,
// leaving even the top-level reference bare. A case-variant structural key
// cannot smuggle a definition anywhere near the cache, the variant being an
// ordinary custom property. The record it rode on has no fields attribute, and
// its parse rejects before any registration.
func TestRegression_SchemaCacheSelfContainedTransitiveRefs(t *testing.T) {
	check := func(name string, defs []string, ref string) {
		t.Run(name, func(t *testing.T) {
			var c avro.SchemaCache
			for i, d := range defs {
				if _, err := c.Parse(d); err != nil {
					t.Fatalf("parse def %d: %v", i, err)
				}
			}
			s, err := c.Parse(ref)
			if err != nil {
				t.Fatalf("parse referencing schema: %v", err)
			}
			if _, err := avro.Parse(string(s.Canonical())); err != nil {
				t.Errorf("Parse(Canonical()) FAILS — not self-contained: %v\n canonical=%s", err, s.Canonical())
			}
			if _, err := avro.Parse(s.String()); err != nil {
				t.Errorf("Parse(String()) FAILS — not self-contained: %v", err)
			}
		})
	}

	// B defined in flat field form, transitively referencing A.
	check("flat_form_transitive",
		[]string{
			`{"type":"record","name":"A","fields":[{"name":"a","type":"int"}]}`,
			`{"type":"record","name":"H","fields":[{"name":"B","type":"record","fields":[{"name":"x","type":"A"}]}]}`,
		},
		`{"type":"record","name":"R","fields":[{"name":"y","type":"B"}]}`)

	// A record spelling "fields" only as a case-variant has no fields
	// attribute. It rejects at parse, as a would-be cached def and as the
	// referencing schema, so no variant-keyed definition can register.
	t.Run("case_variant_key_rejects", func(t *testing.T) {
		var c avro.SchemaCache
		if _, err := c.Parse(`{"type":"record","name":"B","fIelds":[{"name":"x","type":"int"}]}`); err == nil || !strings.Contains(err.Error(), "record is missing fields") {
			t.Errorf("variant-fIelds def: got %v; want the missing-fields reject", err)
		}
		if _, err := c.Parse(`{"type":"record","name":"A","fields":[{"name":"a","type":"int"}]}`); err != nil {
			t.Fatalf("parse def A: %v", err)
		}
		if _, err := c.Parse(`{"type":"record","name":"R","fIelds":[{"name":"y","type":"A"}]}`); err == nil || !strings.Contains(err.Error(), "record is missing fields") {
			t.Errorf("variant-fIelds referencing schema: got %v; want the missing-fields reject", err)
		}
	})
}

// assertSelfContained checks that a cache-built schema is byte-for-byte the
// same logical schema as its inline-defined twin. Identical wire for a value,
// identical canonical form and fingerprint, and re-parseable Canonical()/
// String()/Root().Schema().
func assertSelfContained(t *testing.T, viaCache, inline *avro.Schema, val map[string]any) {
	t.Helper()
	wc, err := viaCache.Encode(val)
	if err != nil {
		t.Fatalf("cache encode: %v", err)
	}
	wi, err := inline.Encode(val)
	if err != nil {
		t.Fatalf("inline encode: %v", err)
	}
	if string(wc) != string(wi) {
		t.Fatalf("control: wire differs (not the same logical schema)")
	}
	if string(viaCache.Canonical()) != string(inline.Canonical()) {
		t.Errorf("Canonical() diverges:\n cache : %s\n inline: %s", viaCache.Canonical(), inline.Canonical())
	}
	if _, err := avro.Parse(string(viaCache.Canonical())); err != nil {
		t.Errorf("Parse(cache.Canonical()) FAILS — not self-contained: %v", err)
	}
	if _, err := avro.Parse(viaCache.String()); err != nil {
		t.Errorf("Parse(cache.String()) FAILS — not self-contained: %v", err)
	}
	if string(viaCache.Fingerprint(avro.NewRabin())) != string(inline.Fingerprint(avro.NewRabin())) {
		t.Errorf("Fingerprint() diverges for the same logical schema (SOE/registry interop break)")
	}
	root := viaCache.Root()
	if _, err := root.Schema(); err != nil {
		t.Errorf("Root().Schema() FAILS to rebuild a cache-built schema: %v", err)
	}
}

// TestMatrix_SchemaCacheSelfContainedEdgeCases exercises the converter's
// delicate paths: a recursive cache type (cycle handling), a cache type with a
// field default (default round-trip), and enum/fixed cache refs. The bug is
// kind-agnostic. Each cache-built schema must have canonical form and
// fingerprint identical to the inline-defined equivalent, and re-parse.
func TestMatrix_SchemaCacheSelfContainedEdgeCases(t *testing.T) {
	cases := []struct {
		name   string
		defs   []string // types to register first
		ref    string   // schema referencing them (cache-built)
		inline string   // logically-identical inline schema
	}{
		{
			name: "recursive",
			defs: []string{`{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]},{"name":"v","type":"int"}]}`},
			ref:  `{"type":"record","name":"Wrap","fields":[{"name":"head","type":"Node"}]}`,
			inline: `{"type":"record","name":"Wrap","fields":[{"name":"head","type":` +
				`{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]},{"name":"v","type":"int"}]}}]}`,
		},
		{
			name: "field-default",
			defs: []string{`{"type":"record","name":"D","fields":[{"name":"x","type":"int","default":7}]}`},
			ref:  `{"type":"record","name":"DW","fields":[{"name":"d","type":"D"}]}`,
			inline: `{"type":"record","name":"DW","fields":[{"name":"d","type":` +
				`{"type":"record","name":"D","fields":[{"name":"x","type":"int","default":7}]}}]}`,
		},
		{
			name: "enum-and-fixed",
			defs: []string{`{"type":"enum","name":"E","symbols":["A","B"]}`, `{"type":"fixed","name":"F","size":4}`},
			ref:  `{"type":"record","name":"EF","fields":[{"name":"e","type":"E"},{"name":"f","type":"F"}]}`,
			inline: `{"type":"record","name":"EF","fields":[` +
				`{"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]}},` +
				`{"name":"f","type":{"type":"fixed","name":"F","size":4}}]}`,
		},
		{
			name: "namespaced",
			defs: []string{`{"type":"record","name":"Inner","namespace":"a.b","fields":[{"name":"x","type":"int"}]}`},
			ref:  `{"type":"record","name":"Outer","namespace":"a.b","fields":[{"name":"i","type":"Inner"}]}`,
			inline: `{"type":"record","name":"Outer","namespace":"a.b","fields":[{"name":"i","type":` +
				`{"type":"record","name":"Inner","namespace":"a.b","fields":[{"name":"x","type":"int"}]}}]}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var c avro.SchemaCache
			for _, d := range tc.defs {
				if _, err := c.Parse(d); err != nil {
					t.Fatalf("register %s: %v", d, err)
				}
			}
			viaCache, err := c.Parse(tc.ref)
			if err != nil {
				t.Fatalf("parse ref via cache: %v", err)
			}
			inline := avro.MustParse(tc.inline)
			if string(viaCache.Canonical()) != string(inline.Canonical()) {
				t.Errorf("Canonical diverges:\n cache : %s\n inline: %s", viaCache.Canonical(), inline.Canonical())
			}
			if string(viaCache.Fingerprint(avro.NewRabin())) != string(inline.Fingerprint(avro.NewRabin())) {
				t.Errorf("Fingerprint diverges")
			}
			if _, err := avro.Parse(string(viaCache.Canonical())); err != nil {
				t.Errorf("Parse(cache.Canonical()) FAILS: %v", err)
			}
			root := viaCache.Root()
			if _, err := root.Schema(); err != nil {
				t.Errorf("Root().Schema() FAILS: %v", err)
			}
		})
	}
}

// TestRegression_SchemaCacheRebuildPreservesMetadata pins that making a
// cache-referenced schema self-contained preserves every original attribute:
// node doc, field doc/order/props, at both the outer and the inlined inner
// level. It preserves them exactly as the logically-identical inline schema
// does. Rebuilding from the attribute-poor node tree drops them; the
// JSON-inline approach preserves them.
func TestRegression_SchemaCacheRebuildPreservesMetadata(t *testing.T) {
	innerDef := `{"type":"record","name":"ns.Inner","doc":"inner doc","fields":[` +
		`{"name":"x","type":"int","doc":"x field doc","order":"descending","ns.fprop":"xfp"}]}`
	outer := func(ref string) string {
		return `{"type":"record","name":"Outer","doc":"outer doc","fields":[` +
			`{"name":"i","type":` + ref + `,"doc":"i field doc","order":"ignore","ns.iprop":"ifp"}]}`
	}
	var c avro.SchemaCache
	if _, err := c.Parse(innerDef); err != nil {
		t.Fatalf("register Inner: %v", err)
	}
	viaCache, err := c.Parse(outer(`"ns.Inner"`))
	if err != nil {
		t.Fatalf("parse Outer via cache: %v", err)
	}
	inline := avro.MustParse(outer(innerDef))

	rc, ri := viaCache.Root(), inline.Root()
	if rc.Doc != ri.Doc {
		t.Errorf("Outer.Doc: cache=%q inline=%q", rc.Doc, ri.Doc)
	}
	if rc.Fields[0].Doc != ri.Fields[0].Doc {
		t.Errorf("Outer.i.Doc: cache=%q inline=%q", rc.Fields[0].Doc, ri.Fields[0].Doc)
	}
	if rc.Fields[0].Order != ri.Fields[0].Order {
		t.Errorf("Outer.i.Order: cache=%q inline=%q", rc.Fields[0].Order, ri.Fields[0].Order)
	}
	if fmt.Sprint(rc.Fields[0].Props) != fmt.Sprint(ri.Fields[0].Props) {
		t.Errorf("Outer.i.Props: cache=%v inline=%v", rc.Fields[0].Props, ri.Fields[0].Props)
	}
	// The inlined inner type's own metadata must survive too.
	ci, ii := rc.Fields[0].Type, ri.Fields[0].Type
	if ci.Doc != ii.Doc {
		t.Errorf("Inner.Doc: cache=%q inline=%q", ci.Doc, ii.Doc)
	}
	if ci.Fields[0].Doc != ii.Fields[0].Doc {
		t.Errorf("Inner.x.Doc: cache=%q inline=%q", ci.Fields[0].Doc, ii.Fields[0].Doc)
	}
	if ci.Fields[0].Order != ii.Fields[0].Order {
		t.Errorf("Inner.x.Order: cache=%q inline=%q", ci.Fields[0].Order, ii.Fields[0].Order)
	}
	if fmt.Sprint(ci.Fields[0].Props) != fmt.Sprint(ii.Fields[0].Props) {
		t.Errorf("Inner.x.Props: cache=%v inline=%v", ci.Fields[0].Props, ii.Fields[0].Props)
	}
}

// TestRegression_SchemaCacheTransitiveRefs pins transitive cross-parse
// references: C to B to A, each defined in its own Parse. C's self-contained
// form must inline B (which itself inlines A), matching the fully-inline
// schema.
func TestRegression_SchemaCacheTransitiveRefs(t *testing.T) {
	var c avro.SchemaCache
	aDef := `{"type":"record","name":"A","fields":[{"name":"x","type":"int"}]}`
	bDef := `{"type":"record","name":"B","fields":[{"name":"a","type":"A"}]}`
	if _, err := c.Parse(aDef); err != nil {
		t.Fatalf("A: %v", err)
	}
	if _, err := c.Parse(bDef); err != nil {
		t.Fatalf("B: %v", err)
	}
	viaCache, err := c.Parse(`{"type":"record","name":"C","fields":[{"name":"b","type":"B"}]}`)
	if err != nil {
		t.Fatalf("C: %v", err)
	}
	inline := avro.MustParse(`{"type":"record","name":"C","fields":[{"name":"b","type":` +
		`{"type":"record","name":"B","fields":[{"name":"a","type":` +
		`{"type":"record","name":"A","fields":[{"name":"x","type":"int"}]}}]}}]}`)
	if string(viaCache.Canonical()) != string(inline.Canonical()) {
		t.Errorf("transitive Canonical diverges:\n cache : %s\n inline: %s", viaCache.Canonical(), inline.Canonical())
	}
	if string(viaCache.Fingerprint(avro.NewRabin())) != string(inline.Fingerprint(avro.NewRabin())) {
		t.Errorf("transitive Fingerprint diverges")
	}
	if _, err := avro.Parse(string(viaCache.Canonical())); err != nil {
		t.Errorf("Parse(transitive cache.Canonical()) FAILS: %v", err)
	}
}

// TestMatrix_SchemaCacheCrossNamespaceSplice pins that splicing an inherited
// definition preserves its resolved namespace regardless of the enclosing
// namespace at the reference site. A definition that inherited its namespace is
// stored with no explicit "namespace". Splicing it verbatim into a different
// scope re-inherits that scope and resolves to the wrong fullname. That is a
// self-contained but *wrong* form, whose canonical and fingerprint silently
// diverge from the wire schema. Stored definitions therefore carry an explicit
// namespace.
func TestMatrix_SchemaCacheCrossNamespaceSplice(t *testing.T) {
	cases := []struct {
		name   string
		defs   []string
		ref    string
		inline string
		value  any
	}{
		{
			name:   "inherited-ns-referenced-from-other-ns",
			defs:   []string{`{"type":"record","name":"P","namespace":"com.a","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}}]}`},
			ref:    `{"type":"record","name":"Q","namespace":"com.b","fields":[{"name":"y","type":"com.a.Inner"}]}`,
			inline: `{"type":"record","name":"Q","namespace":"com.b","fields":[{"name":"y","type":{"type":"record","name":"Inner","namespace":"com.a","fields":[{"name":"x","type":"int"}]}}]}`,
			value:  map[string]any{"y": map[string]any{"x": int32(1)}},
		},
		{
			name:   "null-ns-referenced-from-namespaced",
			defs:   []string{`{"type":"record","name":"X","fields":[{"name":"v","type":"int"}]}`},
			ref:    `{"type":"record","name":"Q","namespace":"com.b","fields":[{"name":"x","type":"X"}]}`,
			inline: `{"type":"record","name":"Q","namespace":"com.b","fields":[{"name":"x","type":{"type":"record","name":"X","namespace":"","fields":[{"name":"v","type":"int"}]}}]}`,
			value:  map[string]any{"x": map[string]any{"v": int32(2)}},
		},
		{
			name:   "deep-inherited-chain-into-other-ns",
			defs:   []string{`{"type":"record","name":"Root","namespace":"x.y","fields":[{"name":"m","type":{"type":"record","name":"Mid","fields":[{"name":"l","type":{"type":"record","name":"Leaf","fields":[{"name":"z","type":"int"}]}}]}}]}`},
			ref:    `{"type":"record","name":"Q","namespace":"other","fields":[{"name":"mid","type":"x.y.Mid"}]}`,
			inline: `{"type":"record","name":"Q","namespace":"other","fields":[{"name":"mid","type":{"type":"record","name":"Mid","namespace":"x.y","fields":[{"name":"l","type":{"type":"record","name":"Leaf","namespace":"x.y","fields":[{"name":"z","type":"int"}]}}]}}]}`,
			value:  map[string]any{"mid": map[string]any{"l": map[string]any{"z": int32(4)}}},
		},
		{
			name:   "recursive-inherited-ns-into-other-ns",
			defs:   []string{`{"type":"record","name":"Holder","namespace":"r.s","fields":[{"name":"node","type":{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]},{"name":"v","type":"int"}]}}]}`},
			ref:    `{"type":"record","name":"W","namespace":"diff","fields":[{"name":"head","type":"r.s.Node"}]}`,
			inline: `{"type":"record","name":"W","namespace":"diff","fields":[{"name":"head","type":{"type":"record","name":"Node","namespace":"r.s","fields":[{"name":"next","type":["null","Node"]},{"name":"v","type":"int"}]}}]}`,
			value:  map[string]any{"head": map[string]any{"next": nil, "v": int32(8)}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var c avro.SchemaCache
			for _, d := range tc.defs {
				if _, err := c.Parse(d); err != nil {
					t.Fatalf("register %q: %v", d, err)
				}
			}
			viaCache, err := c.Parse(tc.ref)
			if err != nil {
				t.Fatalf("parse ref via cache: %v", err)
			}
			inline := avro.MustParse(tc.inline)

			// Control: identical wire confirms the node tree resolved the same
			// fullnames, so canonical/fingerprint must match too.
			wc, errc := viaCache.Encode(tc.value)
			wi, erri := inline.Encode(tc.value)
			if errc != nil || erri != nil {
				t.Fatalf("encode err: cache=%v inline=%v", errc, erri)
			}
			if fmt.Sprintf("%x", wc) != fmt.Sprintf("%x", wi) {
				t.Fatalf("control wire mismatch:\n cache=%x\n inline=%x", wc, wi)
			}
			if string(viaCache.Canonical()) != string(inline.Canonical()) {
				t.Errorf("Canonical diverges:\n cache : %s\n inline: %s", viaCache.Canonical(), inline.Canonical())
			}
			if string(viaCache.Fingerprint(avro.NewRabin())) != string(inline.Fingerprint(avro.NewRabin())) {
				t.Errorf("Fingerprint diverges (namespace lost on splice)")
			}
			if _, err := avro.Parse(string(viaCache.Canonical())); err != nil {
				t.Errorf("Parse(cache.Canonical()) FAILS: %v\n  %s", err, viaCache.Canonical())
			}
		})
	}
}

// A cross-parse reference spelled as a props-carrying wrapped object must
// splice to self-contained metadata like its bare-string and sole-key-wrapped
// twins. The inherited definition replaces the wrapper at that position, and
// the wrapper's props ride on the emitted definition. Props are
// canonical-stripped, so the schema's identity is unchanged. Java instead drops
// usage-site props at reference sites, so preserving them is the more faithful
// treatment of accepted input.
func TestRegression_CacheSpliceWrappedRefProps(t *testing.T) {
	t.Parallel()
	def := `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`
	for name, use := range map[string]string{
		"field_pos": `{"type":"R","foo":1}`,
		"union_pos": `["null",{"type":"R","foo":1}]`,
		"items_pos": `{"type":"array","items":{"type":"R","foo":1}}`,
	} {
		t.Run(name, func(t *testing.T) {
			var c avro.SchemaCache
			if _, err := c.Parse(def); err != nil {
				t.Fatalf("parse def: %v", err)
			}
			s, err := c.Parse(`{"type":"record","name":"Top","fields":[{"name":"a","type":` + use + `}]}`)
			if err != nil {
				t.Fatalf("parse use: %v", err)
			}
			if _, err := avro.Parse(s.String()); err != nil {
				t.Errorf("String() not self-contained: %v\n%s", err, s.String())
			}
			if _, err := avro.Parse(string(s.Canonical())); err != nil {
				t.Errorf("Canonical() not self-contained: %v\n%s", err, s.Canonical())
			}
			if !strings.Contains(s.String(), `"foo":1`) {
				t.Errorf("wrapper props dropped by the splice: %s", s.String())
			}
		})
	}
}

// The three reference spellings of the same schema, bare string, sole-key
// wrapper, and props-carrying wrapper, must produce identical canonical bytes.
// Canonical form strips props and resolves references, so spelling cannot be
// identity.
func TestRegression_WrappedRefSpellingsCanonicalInvariant(t *testing.T) {
	t.Parallel()
	def := `{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`
	use := func(ref string) string {
		return `{"type":"record","name":"Top","fields":[{"name":"a","type":` + ref + `}]}`
	}
	canonical := func(ref string) string {
		var c avro.SchemaCache
		if _, err := c.Parse(def); err != nil {
			t.Fatalf("parse def: %v", err)
		}
		s, err := c.Parse(use(ref))
		if err != nil {
			t.Fatalf("parse use %s: %v", ref, err)
		}
		return string(s.Canonical())
	}
	bare := canonical(`"R"`)
	sole := canonical(`{"type":"R"}`)
	props := canonical(`{"type":"R","foo":1}`)
	if bare != sole || bare != props {
		t.Errorf("canonical bytes differ across reference spellings:\n bare:  %s\n sole:  %s\n props: %s", bare, sole, props)
	}
}

// ---------- cache_overlap_test.go ----------

// TestMatrix_SchemaCacheOverlappingSpliceDefs pins self-containment when two
// cache-inherited references carry overlapping definitions. Each cached
// definition is stored self-contained. A schema referencing two types that
// share a transitive type (the diamond A over {B,C} over D) would then, with a
// naive splice, define the shared name twice. The splice must keep the first
// definition and rewrite later occurrences to name references, exactly as the
// parser's node tree shares one resolved type and as Java's toString emits via
// writeNameRef.
func TestMatrix_SchemaCacheOverlappingSpliceDefs(t *testing.T) {
	cases := []struct {
		name   string
		defs   []string // parsed into the cache first, in order
		ref    string   // the schema under test (cache-built)
		inline string   // logically-identical self-contained twin
		val    map[string]any
	}{
		{
			// Diamond with a shared record: D defined once (inside the first
			// reference's splice), referenced from the second.
			name: "diamond_record",
			defs: []string{
				`{"type":"record","name":"x.D","fields":[{"name":"n","type":"int"}]}`,
				`{"type":"record","name":"x.B","fields":[{"name":"d","type":"x.D"}]}`,
				`{"type":"record","name":"x.C","fields":[{"name":"d","type":"x.D"}]}`,
			},
			ref: `{"type":"record","name":"x.A","fields":[{"name":"b","type":"x.B"},{"name":"c","type":"x.C"}]}`,
			inline: `{"type":"record","name":"x.A","fields":[
				{"name":"b","type":{"type":"record","name":"x.B","fields":[
					{"name":"d","type":{"type":"record","name":"x.D","fields":[{"name":"n","type":"int"}]}}]}},
				{"name":"c","type":{"type":"record","name":"x.C","fields":[{"name":"d","type":"x.D"}]}}]}`,
			val: map[string]any{
				"b": map[string]any{"d": map[string]any{"n": 1}},
				"c": map[string]any{"d": map[string]any{"n": 2}},
			},
		},
		{
			// The mechanism is kind-agnostic: shared enum.
			name: "diamond_enum",
			defs: []string{
				`{"type":"enum","name":"x.D","symbols":["A","B"]}`,
				`{"type":"record","name":"x.B","fields":[{"name":"d","type":"x.D"}]}`,
				`{"type":"record","name":"x.C","fields":[{"name":"d","type":"x.D"}]}`,
			},
			ref: `{"type":"record","name":"x.A","fields":[{"name":"b","type":"x.B"},{"name":"c","type":"x.C"}]}`,
			inline: `{"type":"record","name":"x.A","fields":[
				{"name":"b","type":{"type":"record","name":"x.B","fields":[
					{"name":"d","type":{"type":"enum","name":"x.D","symbols":["A","B"]}}]}},
				{"name":"c","type":{"type":"record","name":"x.C","fields":[{"name":"d","type":"x.D"}]}}]}`,
			val: map[string]any{
				"b": map[string]any{"d": "A"},
				"c": map[string]any{"d": "B"},
			},
		},
		{
			// Shared fixed.
			name: "diamond_fixed",
			defs: []string{
				`{"type":"fixed","name":"x.D","size":2}`,
				`{"type":"record","name":"x.B","fields":[{"name":"d","type":"x.D"}]}`,
				`{"type":"record","name":"x.C","fields":[{"name":"d","type":"x.D"}]}`,
			},
			ref: `{"type":"record","name":"x.A","fields":[{"name":"b","type":"x.B"},{"name":"c","type":"x.C"}]}`,
			inline: `{"type":"record","name":"x.A","fields":[
				{"name":"b","type":{"type":"record","name":"x.B","fields":[
					{"name":"d","type":{"type":"fixed","name":"x.D","size":2}}]}},
				{"name":"c","type":{"type":"record","name":"x.C","fields":[{"name":"d","type":"x.D"}]}}]}`,
			val: map[string]any{
				"b": map[string]any{"d": []byte("xy")},
				"c": map[string]any{"d": []byte("zw")},
			},
		},
		{
			// Null-namespace diamond: the duplicate's rewrite must emit a bare
			// short-name reference (the only spelling a null-namespace type has).
			name: "diamond_null_namespace",
			defs: []string{
				`{"type":"record","name":"D","fields":[{"name":"n","type":"int"}]}`,
				`{"type":"record","name":"B","fields":[{"name":"d","type":"D"}]}`,
				`{"type":"record","name":"C","fields":[{"name":"d","type":"D"}]}`,
			},
			ref: `{"type":"record","name":"A","fields":[{"name":"b","type":"B"},{"name":"c","type":"C"}]}`,
			inline: `{"type":"record","name":"A","fields":[
				{"name":"b","type":{"type":"record","name":"B","fields":[
					{"name":"d","type":{"type":"record","name":"D","fields":[{"name":"n","type":"int"}]}}]}},
				{"name":"c","type":{"type":"record","name":"C","fields":[{"name":"d","type":"D"}]}}]}`,
			val: map[string]any{
				"b": map[string]any{"d": map[string]any{"n": 1}},
				"c": map[string]any{"d": map[string]any{"n": 2}},
			},
		},
		{
			// A nested type referenced *before* the container whose definition
			// carries it: the standalone splice lands first, so the copy inside
			// the container's splice is the duplicate.
			name: "nested_ref_before_container",
			defs: []string{
				`{"type":"record","name":"x.Outer","fields":[{"name":"i","type":{"type":"record","name":"Inner","fields":[{"name":"n","type":"int"}]}}]}`,
			},
			ref: `{"type":"record","name":"x.Wrap","fields":[{"name":"f1","type":"x.Inner"},{"name":"f2","type":"x.Outer"}]}`,
			inline: `{"type":"record","name":"x.Wrap","fields":[
				{"name":"f1","type":{"type":"record","name":"x.Inner","fields":[{"name":"n","type":"int"}]}},
				{"name":"f2","type":{"type":"record","name":"x.Outer","fields":[{"name":"i","type":"x.Inner"}]}}]}`,
			val: map[string]any{
				"f1": map[string]any{"n": 1},
				"f2": map[string]any{"i": map[string]any{"n": 2}},
			},
		},
		{
			// Control: container referenced first, so the nested definition
			// arrives with it and the later standalone reference stays
			// bare.
			name: "container_ref_before_nested",
			defs: []string{
				`{"type":"record","name":"x.Outer","fields":[{"name":"i","type":{"type":"record","name":"Inner","fields":[{"name":"n","type":"int"}]}}]}`,
			},
			ref: `{"type":"record","name":"x.Wrap","fields":[{"name":"f2","type":"x.Outer"},{"name":"f1","type":"x.Inner"}]}`,
			inline: `{"type":"record","name":"x.Wrap","fields":[
				{"name":"f2","type":{"type":"record","name":"x.Outer","fields":[
					{"name":"i","type":{"type":"record","name":"x.Inner","fields":[{"name":"n","type":"int"}]}}]}},
				{"name":"f1","type":"x.Inner"}]}`,
			val: map[string]any{
				"f1": map[string]any{"n": 1},
				"f2": map[string]any{"i": map[string]any{"n": 2}},
			},
		},
		{
			// A flat-form ("linkedin/goavro") field definition arriving as the
			// duplicate: the field must rewrite to normal form with a name
			// reference (a field object cannot be replaced by a bare string).
			name: "flat_form_duplicate",
			defs: []string{
				// H's flat field both defines x.B and registers it in the
				// cache (the def store captures the lifted form), so a later
				// schema can reference x.B standalone and through x.H.
				`{"type":"record","name":"x.H","fields":[{"name":"B","type":"record","fields":[{"name":"v","type":"int"}]}]}`,
			},
			ref: `{"type":"record","name":"x.Wrap","fields":[{"name":"f1","type":"x.B"},{"name":"f2","type":"x.H"}]}`,
			inline: `{"type":"record","name":"x.Wrap","fields":[
				{"name":"f1","type":{"type":"record","name":"x.B","fields":[{"name":"v","type":"int"}]}},
				{"name":"f2","type":{"type":"record","name":"x.H","fields":[{"name":"B","type":"x.B"}]}}]}`,
			val: map[string]any{
				"f1": map[string]any{"v": 1},
				"f2": map[string]any{"B": map[string]any{"v": 2}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var c avro.SchemaCache
			for i, d := range tc.defs {
				if _, err := c.Parse(d); err != nil {
					t.Fatalf("parse def %d: %v", i, err)
				}
			}
			s, err := c.Parse(tc.ref)
			if err != nil {
				t.Fatalf("parse referencing schema: %v", err)
			}
			inline, err := avro.Parse(tc.inline)
			if err != nil {
				t.Fatalf("parse inline twin: %v", err)
			}
			assertSelfContained(t, s, inline, tc.val)
		})
	}
}

// TestMatrix_SchemaCacheSpliceCascade pins that a self-contained schema
// built from overlapping splices is itself a usable cache definition. A later
// parse referencing it splices the coherent definition and stays
// self-contained. Without the duplicate-definition rewrite the diamond's failed
// rebuild records a dangling definition into the cache's def store, cascading
// the breakage into every downstream referencing schema.
func TestMatrix_SchemaCacheSpliceCascade(t *testing.T) {
	var c avro.SchemaCache
	for i, d := range []string{
		`{"type":"record","name":"x.D","fields":[{"name":"n","type":"int"}]}`,
		`{"type":"record","name":"x.B","fields":[{"name":"d","type":"x.D"}]}`,
		`{"type":"record","name":"x.C","fields":[{"name":"d","type":"x.D"}]}`,
		`{"type":"record","name":"x.A","fields":[{"name":"b","type":"x.B"},{"name":"c","type":"x.C"}]}`,
	} {
		if _, err := c.Parse(d); err != nil {
			t.Fatalf("parse def %d: %v", i, err)
		}
	}
	e, err := c.Parse(`{"type":"record","name":"x.E","fields":[{"name":"a","type":"x.A"}]}`)
	if err != nil {
		t.Fatalf("parse E: %v", err)
	}
	inline, err := avro.Parse(`{"type":"record","name":"x.E","fields":[{"name":"a","type":
		{"type":"record","name":"x.A","fields":[
			{"name":"b","type":{"type":"record","name":"x.B","fields":[
				{"name":"d","type":{"type":"record","name":"x.D","fields":[{"name":"n","type":"int"}]}}]}},
			{"name":"c","type":{"type":"record","name":"x.C","fields":[{"name":"d","type":"x.D"}]}}]}}]}`)
	if err != nil {
		t.Fatalf("parse inline twin: %v", err)
	}
	assertSelfContained(t, e, inline, map[string]any{
		"a": map[string]any{
			"b": map[string]any{"d": map[string]any{"n": 1}},
			"c": map[string]any{"d": map[string]any{"n": 2}},
		},
	})
}

// TestMatrix_SchemaCacheShortNameShadowNoMisbind pins that the
// duplicate-definition rewrite never emits a reference that would re-bind to a
// different type. A null-namespace type's only reference spelling is its bare
// short name, which the parser binds enclosing-namespace-first. So when a
// same-short-name namespaced type is defined earlier, rewriting the
// null-namespace duplicate to a bare reference silently re-binds it. The
// rewrite must decline in exactly that case. The forms may then stay
// non-self-contained, since the format has no absolute-reference spelling for
// null-namespace names and Java has the same limitation. They must never
// describe the wrong schema.
func TestMatrix_SchemaCacheShortNameShadowNoMisbind(t *testing.T) {
	var c avro.SchemaCache
	for i, d := range []string{
		// Null-namespace D, referenced from namespaced carriers F and G
		// (legal at their parse time: x.D does not exist yet, so the bare
		// "D" falls through to the null-namespace type).
		`{"type":"record","name":"D","fields":[{"name":"n","type":"int"}]}`,
		`{"type":"record","name":"x.F","fields":[{"name":"d","type":"D"}]}`,
		`{"type":"record","name":"x.G","fields":[{"name":"d","type":"D"}]}`,
		// A different type that shadows D's short name inside namespace x.
		`{"type":"record","name":"x.D","fields":[{"name":"z","type":"string"}]}`,
	} {
		if _, err := c.Parse(d); err != nil {
			t.Fatalf("parse def %d: %v", i, err)
		}
	}
	// p splices x.D first (registering its name), then f and g each carry the
	// null-namespace D definition: the duplicate inside g's splice cannot be
	// rewritten to "D" (it would re-bind to x.D at that position).
	a, err := c.Parse(`{"type":"record","name":"x.A","fields":[
		{"name":"p","type":"x.D"},
		{"name":"f","type":"x.F"},
		{"name":"g","type":"x.G"}]}`)
	if err != nil {
		t.Fatalf("parse A: %v", err)
	}

	val := map[string]any{
		"p": map[string]any{"z": "s"},
		"f": map[string]any{"d": map[string]any{"n": 1}},
		"g": map[string]any{"d": map[string]any{"n": 2}},
	}
	wire, err := a.Encode(val)
	if err != nil {
		t.Fatalf("wire path must be unaffected: %v", err)
	}
	var decoded any
	if _, err := a.Decode(wire, &decoded); err != nil {
		t.Fatalf("wire decode: %v", err)
	}

	// The correct schema here has no self-contained JSON spelling. The
	// null-namespace D inside g's subtree can only be written as a second
	// definition (duplicate, rejected) or as the bare reference "D", which
	// re-binds to x.D at that position. The metadata forms are therefore allowed
	// to stay non-self-contained, but if they do re-parse they must describe the
	// same schema the wire codec implements. A rewrite emitting the bare "D"
	// re-parses with g.d bound to x.D and fails this value-level check.
	if reparsed, err := avro.Parse(a.String()); err == nil {
		wire2, err := reparsed.Encode(val)
		if err != nil {
			t.Errorf("String() re-parses but rejects a value the wire codec accepts (mis-bound short-name reference): %v", err)
		} else if string(wire2) != string(wire) {
			t.Errorf("String() re-parses but produces different wire bytes for the same value (mis-bound short-name reference)")
		}
	}
}

// TestMatrix_SchemaCacheWrappedFormCrossParseRefSelfContains pins that a
// cross-parse reference spelled {"type":"X"} self-contains exactly like the
// bare "X". Both are documented-accepted spellings, including for forward refs.
// The splice replaces the whole wrapped object with the definition. Recursing
// into the "type" value instead produces the invalid {"type":{X-def}}, failing
// the rebuild Parse and falling the metadata back to a dangling reference. The
// oracle is the inline-defined twin, crossed over every nesting position.
func TestMatrix_SchemaCacheWrappedFormCrossParseRefSelfContains(t *testing.T) {
	const xDef = `{"type":"record","name":"X","fields":[{"name":"n","type":"int"}]}`
	cases := []struct {
		name    string
		wrapped string // Y referencing X via {"type":"X"}, cache-built after X
		inline  string // logically-identical twin with X defined inline
		val     map[string]any
	}{
		{
			name:    "field",
			wrapped: `{"type":"record","name":"Y","fields":[{"name":"f","type":{"type":"X"}}]}`,
			inline:  `{"type":"record","name":"Y","fields":[{"name":"f","type":` + xDef + `}]}`,
			val:     map[string]any{"f": map[string]any{"n": int32(5)}},
		},
		{
			name:    "union_branch",
			wrapped: `{"type":"record","name":"Y","fields":[{"name":"f","type":["null",{"type":"X"}]}]}`,
			inline:  `{"type":"record","name":"Y","fields":[{"name":"f","type":["null",` + xDef + `]}]}`,
			val:     map[string]any{"f": map[string]any{"n": int32(6)}},
		},
		{
			name:    "array_items",
			wrapped: `{"type":"record","name":"Y","fields":[{"name":"f","type":{"type":"array","items":{"type":"X"}}}]}`,
			inline:  `{"type":"record","name":"Y","fields":[{"name":"f","type":{"type":"array","items":` + xDef + `}}]}`,
			val:     map[string]any{"f": []any{map[string]any{"n": int32(7)}}},
		},
		{
			name:    "map_values",
			wrapped: `{"type":"record","name":"Y","fields":[{"name":"f","type":{"type":"map","values":{"type":"X"}}}]}`,
			inline:  `{"type":"record","name":"Y","fields":[{"name":"f","type":{"type":"map","values":` + xDef + `}}]}`,
			val:     map[string]any{"f": map[string]any{"k": map[string]any{"n": int32(8)}}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &avro.SchemaCache{}
			if _, err := c.Parse(xDef); err != nil {
				t.Fatalf("parse X into cache: %v", err)
			}
			y, err := c.Parse(tc.wrapped)
			if err != nil {
				t.Fatalf("parse wrapped Y: %v", err)
			}
			twin := avro.MustParse(tc.inline)

			// The wire codec is the logical-identity anchor; match it to the twin.
			wire, err := y.Encode(tc.val)
			if err != nil {
				t.Fatalf("Y.Encode: %v", err)
			}
			twinWire, err := twin.Encode(tc.val)
			if err != nil {
				t.Fatalf("twin.Encode: %v", err)
			}
			if string(wire) != string(twinWire) {
				t.Errorf("wrapped-form wire != inline-twin wire")
			}

			// Self-containment: String()/Canonical() must re-parse and match the
			// twin's canonical form + fingerprint (the surfaces the bug broke).
			if _, err := avro.Parse(y.String()); err != nil {
				t.Errorf("Parse(Y.String()) failed (dangling metadata): %v\n  %s", err, y.String())
			}
			if _, err := avro.Parse(string(y.Canonical())); err != nil {
				t.Errorf("Parse(Y.Canonical()) failed (dangling metadata): %v\n  %s", err, y.Canonical())
			}
			// Canonical-form equality is the fingerprint surface (the Rabin/SHA
			// fingerprint is a hash of these bytes), so matching the inline twin
			// here pins the SOE / schema-registry interop the bug broke.
			if string(y.Canonical()) != string(twin.Canonical()) {
				t.Errorf("wrapped-form Canonical != inline-twin Canonical:\n got:  %s\n want: %s", y.Canonical(), twin.Canonical())
			}
		})
	}

	// Boundary-1 control: the bare-string form already self-contains and must
	// stay correct.
	t.Run("bare_form_control", func(t *testing.T) {
		c := &avro.SchemaCache{}
		if _, err := c.Parse(xDef); err != nil {
			t.Fatalf("parse X: %v", err)
		}
		y, err := c.Parse(`{"type":"record","name":"Y2","fields":[{"name":"f","type":"X"}]}`)
		if err != nil {
			t.Fatalf("parse bare Y2: %v", err)
		}
		if _, err := avro.Parse(y.String()); err != nil {
			t.Errorf("control: Parse(bare Y2.String()) must succeed: %v", err)
		}
	})

	// A type referencing the same cached type twice via the wrapped form. The
	// first occurrence inlines X's definition. A later wrapped occurrence
	// resolves to an already-inlined type and so does not splice. Its wrapper
	// must collapse to the bare "X" the inline twin carries, or {"type":"X"}
	// survives in String() where the canonical bare reference belongs.
	// Single-reference cases can never reach this later-occurrence path.
	t.Run("repeated_ref_collapses_in_string", func(t *testing.T) {
		c := &avro.SchemaCache{}
		if _, err := c.Parse(xDef); err != nil {
			t.Fatalf("parse X: %v", err)
		}
		y, err := c.Parse(`{"type":"record","name":"Y3","fields":[{"name":"f1","type":{"type":"X"}},{"name":"f2","type":{"type":"X"}}]}`)
		if err != nil {
			t.Fatalf("parse wrapped Y3: %v", err)
		}
		twin := avro.MustParse(`{"type":"record","name":"Y3","fields":[{"name":"f1","type":` + xDef + `},{"name":"f2","type":"X"}]}`)

		// The surviving-wrapper signature is the value {"type":"X"} (a wrapped
		// name reference). f1 inlines X's full record definition, whose "type" is
		// "record" and whose only "X" is "name":"X". f2's bare reference is the
		// string "X", so {"type":"X"} appears nowhere unless a wrapper survived
		// the rebuild.
		if strings.Contains(y.String(), `{"type":"X"}`) {
			t.Errorf("String() kept a wrapped {\"type\":\"X\"} reference; the later occurrence must collapse to bare \"X\":\n  %s", y.String())
		}
		if !strings.Contains(y.String(), `"name":"X"`) {
			t.Errorf("String() lost X's inlined definition entirely:\n  %s", y.String())
		}
		// Wire (logical-identity anchor) and Canonical/fingerprint must match the
		// inline twin; String is the surface a surviving wrapper breaks.
		val := map[string]any{"f1": map[string]any{"n": int32(1)}, "f2": map[string]any{"n": int32(2)}}
		yw, err := y.Encode(val)
		if err != nil {
			t.Fatalf("Y3.Encode: %v", err)
		}
		tw, err := twin.Encode(val)
		if err != nil {
			t.Fatalf("twin.Encode: %v", err)
		}
		if string(yw) != string(tw) {
			t.Errorf("wrapped repeated-ref wire != inline-twin wire")
		}
		if string(y.Canonical()) != string(twin.Canonical()) {
			t.Errorf("Canonical diverges:\n got:  %s\n want: %s", y.Canonical(), twin.Canonical())
		}
		if _, err := avro.Parse(y.String()); err != nil {
			t.Errorf("Parse(Y3.String()) must succeed: %v\n  %s", err, y.String())
		}
	})
}

// TestMatrix_SpliceWrapperReservedKeyMerge drives the splice merge's
// reserved-key routing with wrapper props on cached definitions. A wrapper key
// the def's kind/logical consumes never survives the splice, matching Java's
// reference arms. An unconsumed key merges onto the definition as an ordinary
// custom property, definition-wins on collision. The decimal def omits "scale"
// on purpose: a consumed wrapper "scale" must be dropped by the routing, not
// masked by the def-wins presence check.
func TestMatrix_SpliceWrapperReservedKeyMerge(t *testing.T) {
	plainDef := `{"type":"fixed","name":"F","size":4}`
	decimalDef := `{"type":"fixed","name":"D","size":4,"logicalType":"decimal","precision":2}`

	cases := []struct {
		name    string
		def     string
		wrapper string
		check   func(t *testing.T, n avro.SchemaNode)
	}{
		{
			"nonstring-logicaltype-numeric-merges",
			plainDef,
			`{"type":"F","logicalType":123}`,
			func(t *testing.T, n avro.SchemaNode) {
				if got := n.Props["logicalType"]; got != int64(123) {
					t.Errorf("Props[logicalType] = %#v; want int64(123) merged as ordinary prop", got)
				}
				if n.LogicalType != "" {
					t.Errorf("non-string logicalType activated: %q", n.LogicalType)
				}
			},
		},
		{
			"nonstring-logicaltype-null-merges",
			plainDef,
			`{"type":"F","logicalType":null}`,
			func(t *testing.T, n avro.SchemaNode) {
				if got, ok := n.Props["logicalType"]; !ok || got != nil {
					t.Errorf("Props[logicalType] = %#v (present=%v); want JSON null merged as nil prop", got, ok)
				}
			},
		},
		{
			"string-logicaltype-consumed-drops",
			plainDef,
			`{"type":"F","logicalType":"decimal"}`,
			func(t *testing.T, n avro.SchemaNode) {
				if got, ok := n.Props["logicalType"]; ok {
					t.Errorf("consumed usage-site logicalType survived the splice as a prop: %#v", got)
				}
				if n.LogicalType != "" {
					t.Errorf("usage-site logicalType activated on the def: %q", n.LogicalType)
				}
			},
		},
		{
			"unconsumed-precision-valid-merges",
			plainDef,
			`{"type":"F","precision":3}`,
			func(t *testing.T, n avro.SchemaNode) {
				if got := n.Props["precision"]; got != int64(3) {
					t.Errorf("Props[precision] = %#v; want int64(3)", got)
				}
				if n.Precision != 0 {
					t.Errorf("unconsumed precision landed structurally: %d", n.Precision)
				}
			},
		},
		{
			"unconsumed-precision-malformed-merges-verbatim",
			plainDef,
			`{"type":"F","precision":"x"}`,
			func(t *testing.T, n avro.SchemaNode) {
				if got := n.Props["precision"]; got != "x" {
					t.Errorf("Props[precision] = %#v; want verbatim \"x\"", got)
				}
				if n.Precision != 0 {
					t.Errorf("malformed precision landed structurally: %d", n.Precision)
				}
			},
		},
		{
			"consumed-scale-malformed-drops",
			decimalDef,
			`{"type":"D","scale":"bogus"}`,
			func(t *testing.T, n avro.SchemaNode) {
				if got, ok := n.Props["scale"]; ok {
					t.Errorf("consumed usage-site scale survived the splice as a prop: %#v", got)
				}
				if n.Scale != 0 {
					t.Errorf("usage-site scale mutated the def: Scale = %d; want spec-default 0", n.Scale)
				}
			},
		},
		{
			"consumed-scale-valid-drops-def-wins",
			decimalDef,
			`{"type":"D","scale":1}`,
			func(t *testing.T, n avro.SchemaNode) {
				if got, ok := n.Props["scale"]; ok {
					t.Errorf("consumed usage-site scale survived the splice as a prop: %#v", got)
				}
				if n.Scale != 0 {
					t.Errorf("usage-site scale mutated the def: Scale = %d; want spec-default 0", n.Scale)
				}
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cache := &avro.SchemaCache{}
			if _, err := cache.Parse(c.def); err != nil {
				t.Fatalf("def Parse: %v", err)
			}
			s, err := cache.Parse(c.wrapper)
			if err != nil {
				t.Fatalf("wrapper Parse: %v", err)
			}
			n := s.Root()
			c.check(t, *n)

			// Wrapper props are metadata: the wire image is the def's own
			// ([]byte is the opaque carrier for both plain and decimal
			// fixed).
			def := avro.MustParse(c.def)
			got, err := s.Encode([]byte{1, 2, 3, 4})
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			want, err := def.Encode([]byte{1, 2, 3, 4})
			if err != nil {
				t.Fatalf("def Encode: %v", err)
			}
			if string(got) != string(want) {
				t.Errorf("wrapper props changed the wire: %x vs %x", got, want)
			}

			// The spliced, self-contained metadata tree must rebuild to a
			// schema that reparses and keeps the same routing.
			rb, err := n.Schema()
			if err != nil {
				t.Fatalf("Root().Schema() rebuild: %v", err)
			}
			c.check(t, *rb.Root())
		})
	}
}

// spliceStrayRoutedKeys is the shape-conditional reserved-key axis: keys with a
// SchemaNode structural field to land on. A stray placement then routes on
// whether its body parses as that key's schema shape, rather than on the key
// alone. It mirrors the parser's own strayRoutedKeys, and
// TestInvariant_StrayRoutedKeyAxisMirrorsTheSource reds if the source list
// changes. The three keys the older cells drive are *not* here: their binding
// turns on the value or the logical type, so each short-circuits first.
var spliceStrayRoutedKeys = []string{
	"items", "values", "fields", "symbols", "size", "name", "namespace", "aliases",
}

// spliceStrayOutcome is a cell's ruled verdict for a stray-routed key on a
// cached definition.
type spliceStrayOutcome string

const (
	// spliceReject: the wrapper carries a shape-OK structural attribute, so
	// it is no longer a bare reference object: the parse reads its "type"
	// as a kind name and fails before any splice runs. This is the parse
	// boundary that keeps a reference from being poisoned into a different
	// kind by a usage-site attribute.
	spliceReject spliceStrayOutcome = "reject"
	// spliceDrop: the definition's kind binds the key, so it is a consumed
	// usage-site attribute and dies at the splice, reaching neither Props
	// nor the definition's structural field.
	spliceDrop spliceStrayOutcome = "drop"
	// spliceMerge: the key is a stray on the definition's kind and its body
	// does not parse as the key's schema shape, so it is an ordinary custom
	// property and merges verbatim, asserted against the image the same
	// body takes under a non-reserved key.
	spliceMerge spliceStrayOutcome = "merge"
)

// spliceStrayBody is one body class per key. shapeOK parses as the key's
// schema shape; badElem is the right container with a wrong element; and
// badScalar is the wrong container entirely. The two bad classes are distinct
// arms of the shape decode, an array-valued key rejecting a non-array body
// before it ever inspects elements. Collapsing them would leave the container
// check unexercised.
type spliceStrayBody struct {
	class string
	json  string
}

var spliceStrayBodies = map[string][]spliceStrayBody{
	"items":     {{"shapeOK", `"int"`}, {"badElem", `[1]`}, {"badScalar", `123`}},
	"values":    {{"shapeOK", `"int"`}, {"badElem", `[1]`}, {"badScalar", `123`}},
	"fields":    {{"shapeOK", `[{"name":"f","type":"int"}]`}, {"badElem", `[1]`}, {"badScalar", `123`}},
	"symbols":   {{"shapeOK", `["A"]`}, {"badElem", `[1]`}, {"badScalar", `123`}},
	"size":      {{"shapeOK", `8`}, {"badScalar", `"x"`}},
	"name":      {{"shapeOK", `"nm"`}, {"badScalar", `12`}},
	"namespace": {{"shapeOK", `"ns"`}, {"badScalar", `12`}},
	"aliases":   {{"shapeOK", `["al"]`}, {"badElem", `[1]`}, {"badScalar", `12`}},
}

// spliceStrayDefs are the three named kinds a cached definition can be: the
// kind axis, which is what decides binding. Each kind binds a different subset
// of the stray-routed keys (size on fixed, symbols on enum, fields on record,
// and name/namespace/aliases on all three). A single kind would leave both
// sides of the binding split untested for most keys.
var spliceStrayDefs = []struct{ kind, schema string }{
	{"fixed", `{"type":"fixed","name":"F","size":4}`},
	{"enum", `{"type":"enum","name":"F","symbols":["A"]}`},
	{"record", `{"type":"record","name":"F","fields":[{"name":"x","type":"int"}]}`},
}

// spliceStrayRuling is the ruled outcome per (def kind, key, body class).
// Absent entries are spliceReject.
var spliceStrayRuling = map[string]spliceStrayOutcome{
	// fixed binds size / name / namespace / aliases.
	"fixed/items/badElem": spliceMerge, "fixed/items/badScalar": spliceMerge,
	"fixed/values/badElem": spliceMerge, "fixed/values/badScalar": spliceMerge,
	"fixed/fields/badElem": spliceMerge, "fixed/fields/badScalar": spliceMerge,
	"fixed/symbols/badElem": spliceMerge, "fixed/symbols/badScalar": spliceMerge,
	"fixed/size/badScalar":    spliceDrop,
	"fixed/name/badScalar":    spliceDrop,
	"fixed/namespace/shapeOK": spliceDrop, "fixed/namespace/badScalar": spliceDrop,
	"fixed/aliases/shapeOK": spliceDrop, "fixed/aliases/badElem": spliceDrop, "fixed/aliases/badScalar": spliceDrop,

	// enum binds symbols / name / namespace / aliases.
	"enum/items/badElem": spliceMerge, "enum/items/badScalar": spliceMerge,
	"enum/values/badElem": spliceMerge, "enum/values/badScalar": spliceMerge,
	"enum/fields/badElem": spliceMerge, "enum/fields/badScalar": spliceMerge,
	"enum/symbols/badElem": spliceDrop, "enum/symbols/badScalar": spliceDrop,
	"enum/size/badScalar":    spliceMerge,
	"enum/name/badScalar":    spliceDrop,
	"enum/namespace/shapeOK": spliceDrop, "enum/namespace/badScalar": spliceDrop,
	"enum/aliases/shapeOK": spliceDrop, "enum/aliases/badElem": spliceDrop, "enum/aliases/badScalar": spliceDrop,

	// record binds fields / name / namespace / aliases.
	"record/items/badElem": spliceMerge, "record/items/badScalar": spliceMerge,
	"record/values/badElem": spliceMerge, "record/values/badScalar": spliceMerge,
	"record/fields/badElem": spliceDrop, "record/fields/badScalar": spliceDrop,
	"record/symbols/badElem": spliceMerge, "record/symbols/badScalar": spliceMerge,
	"record/size/badScalar":    spliceMerge,
	"record/name/badScalar":    spliceDrop,
	"record/namespace/shapeOK": spliceDrop, "record/namespace/badScalar": spliceDrop,
	"record/aliases/shapeOK": spliceDrop, "record/aliases/badElem": spliceDrop, "record/aliases/badScalar": spliceDrop,
}

// TestMatrix_SpliceWrapperStrayRoutedKeyShape widens the wrapper-key axis onto
// the shape-conditional class. The older cells drive only keys settled before
// the routing reaches its shape question, leaving the splice's shape verdict
// unrun: the nil-verdict arm, the one call site with no recorded parse verdict
// to consult.
//
// The cross product is stray-routed key x definition kind x body class. Each
// cell is checked against an independent image rather than the routing's own
// answer. A merged body must equal what the same body becomes under a
// non-reserved key. Every cell must leave the definition's structural surface,
// wire image and canonical form untouched.
func TestMatrix_SpliceWrapperStrayRoutedKeyShape(t *testing.T) {
	t.Parallel()
	counts := map[spliceStrayOutcome]int{}
	shapeArms := map[string]int{}
	for _, d := range spliceStrayDefs {
		for _, key := range spliceStrayRoutedKeys {
			for _, body := range spliceStrayBodies[key] {
				cell := d.kind + "/" + key + "/" + body.class
				want, ok := spliceStrayRuling[cell]
				if !ok {
					want = spliceReject
				}
				t.Run(cell, func(t *testing.T) {
					wrapper := `{"type":"F","` + key + `":` + body.json + `}`
					cache := &avro.SchemaCache{}
					if _, err := cache.Parse(d.schema); err != nil {
						t.Fatalf("def Parse: %v", err)
					}
					s, err := cache.Parse(wrapper)
					if want == spliceReject {
						if err == nil {
							t.Fatalf("shape-OK stray %q on a %s reference accepted; want the parse to reject it as a kind name\n  surface: %+v", key, d.kind, *s.Root())
						}
						return
					}
					if err != nil {
						t.Fatalf("wrapper Parse: %v", err)
					}
					n := s.Root()

					// The definition is never redefined by a usage-site
					// attribute: structural surface, wire and canonical form
					// all stay the plain definition's.
					def := avro.MustParse(d.schema)
					ctl := def.Root()
					if n.Type != ctl.Type || n.Name != ctl.Name || n.Namespace != ctl.Namespace ||
						n.Size != ctl.Size || !reflect.DeepEqual(n.Symbols, ctl.Symbols) ||
						!reflect.DeepEqual(n.Aliases, ctl.Aliases) || len(n.Fields) != len(ctl.Fields) ||
						(n.Items == nil) != (ctl.Items == nil) || (n.Values == nil) != (ctl.Values == nil) {
						t.Fatalf("wrapper %q mutated the definition's structural surface:\n got:  %+v\n want: %+v", key, *n, *ctl)
					}
					if got, want := string(s.Canonical()), string(def.Canonical()); got != want {
						t.Errorf("wrapper %q changed the canonical form:\n got:  %s\n want: %s", key, got, want)
					}

					got, inProps := n.Props[key]
					switch want {
					case spliceDrop:
						if inProps {
							t.Errorf("consumed usage-site %q survived the splice as a prop: %#v", key, got)
						}
					case spliceMerge:
						// The independent image: the same body under a key
						// the routing has no opinion about.
						twinCache := &avro.SchemaCache{}
						if _, err := twinCache.Parse(d.schema); err != nil {
							t.Fatalf("twin def Parse: %v", err)
						}
						twin, err := twinCache.Parse(`{"type":"F","zzcustom":` + body.json + `}`)
						if err != nil {
							t.Fatalf("twin Parse: %v", err)
						}
						wantV := twin.Root().Props["zzcustom"]
						if !inProps {
							t.Fatalf("stray %q did not merge as a custom property; Props = %#v", key, n.Props)
						}
						if !reflect.DeepEqual(got, wantV) {
							t.Errorf("Props[%q] = %#v; want the custom-prop image %#v", key, got, wantV)
						}
					}

					// The spliced tree rebuilds and keeps the same routing.
					rb, err := n.Schema()
					if err != nil {
						t.Fatalf("Root().Schema() rebuild: %v", err)
					}
					if _, again := rb.Root().Props[key]; again != inProps {
						t.Errorf("rebuild changed the %q route: inProps %v -> %v", key, inProps, again)
					}
				})
				counts[want]++
				if want == spliceMerge {
					shapeArms[spliceStrayShapeArm(key)]++
				}
			}
		}
	}
	t.Logf("splice stray-routed cells: %d reject, %d drop, %d merge", counts[spliceReject], counts[spliceDrop], counts[spliceMerge])
}

// spliceStrayShapeArm groups the stray-routed keys by the arm of the shape
// decode they run: the decode switches on the key, and keys sharing an arm
// share the code the cell exercises.
func spliceStrayShapeArm(key string) string {
	switch key {
	case "items", "values":
		return "schema-position"
	case "fields":
		return "field-array"
	case "symbols", "aliases":
		return "string-array"
	case "size":
		return "lax-int"
	}
	return "string" // name, namespace
}

// TestMatrix_SpliceWrapperStrayRoutedKeyShapeIsNotVacuous is the liveness
// floor. Every cell above is generated, which says nothing about whether it is
// exercised. A ruling table that drifted to all-reject, or a body table whose
// "bad" bodies quietly became shape-OK, would still generate 66 cells and
// assert nothing about the splice's shape verdict. This fails when an arm of
// that verdict stops being reached.
func TestMatrix_SpliceWrapperStrayRoutedKeyShapeIsNotVacuous(t *testing.T) {
	t.Parallel()
	counts := map[spliceStrayOutcome]int{}
	arms := map[string]int{}
	keys := map[string]int{}
	kinds := map[string]int{}
	for _, d := range spliceStrayDefs {
		for _, key := range spliceStrayRoutedKeys {
			for _, body := range spliceStrayBodies[key] {
				want, ok := spliceStrayRuling[d.kind+"/"+key+"/"+body.class]
				if !ok {
					want = spliceReject
				}
				counts[want]++
				keys[key]++
				kinds[d.kind]++
				if want == spliceMerge {
					arms[spliceStrayShapeArm(key)]++
				}
			}
		}
	}
	if len(keys) != len(spliceStrayRoutedKeys) {
		t.Errorf("the matrix drives %d keys, the axis names %d", len(keys), len(spliceStrayRoutedKeys))
	}
	if len(kinds) != len(spliceStrayDefs) {
		t.Errorf("the matrix drives %d definition kinds, the axis names %d", len(kinds), len(spliceStrayDefs))
	}
	// All three outcomes must occur: a table that collapsed to one verdict
	// would pass while asking nothing.
	for _, o := range []spliceStrayOutcome{spliceReject, spliceDrop, spliceMerge} {
		if counts[o] == 0 {
			t.Errorf("no %q cells; the ruling table has collapsed to a single verdict", o)
		}
	}
	// The merge cells are the only ones that reach the splice's nil-verdict
	// shape decode: a bound key is settled before the shape is asked, and a
	// rejected one never reaches the splice. Each arm of that decode needs at
	// least one. "string" is deliberately absent: name, namespace and aliases
	// bind on every named kind, so no cached definition can carry one there.
	for _, arm := range []string{"schema-position", "field-array", "string-array", "lax-int"} {
		if arms[arm] == 0 {
			t.Errorf("shape-decode arm %q has no merge cell; the splice never runs it", arm)
		}
	}
	if counts[spliceMerge] < 12 {
		t.Fatalf("only %d merge cells reach the shape decode; the wrapper-key axis has narrowed", counts[spliceMerge])
	}
}

// A cached definition whose record field carries an unconsumed malformed
// precision splices through by-subtree. The field rides verbatim inside the
// inlined definition, the spliced tree rebuilds, and the pair stays on the
// field's Props. The splice merge touches only the wrapper's own keys, never
// field attributes inside the definition.
func TestRegression_SpliceDefFieldMalformedPrecisionRidesThrough(t *testing.T) {
	cache := &avro.SchemaCache{}
	if _, err := cache.Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":"int","precision":"x"}]}`); err != nil {
		t.Fatalf("def Parse: %v", err)
	}
	s, err := cache.Parse(`{"type":"R","myprop":1}`)
	if err != nil {
		t.Fatalf("wrapper Parse: %v", err)
	}
	n := s.Root()
	if got := n.Fields[0].Props["precision"]; got != "x" {
		t.Errorf("spliced def's field Props[precision] = %#v; want verbatim \"x\"", got)
	}
	if got := n.Props["myprop"]; got != int64(1) {
		t.Errorf("wrapper prop lost: %#v", got)
	}
	rb, err := n.Schema()
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if got := rb.Root().Fields[0].Props["precision"]; got != "x" {
		t.Errorf("rebuild field Props[precision] = %#v; want \"x\"", got)
	}
}

// ---------- wildcard_cache_test.go ----------

// TestRegression_SchemaCacheWildcardConsistentRegistration pins that a wildcard
// CustomType registered consistently across cache parses resolves, like a
// non-wildcard does. A cache-boundary reverse guard whose hadCustomType stamp
// counts any wiring, which a wildcard populates, disagrees with
// findCustomTypeMatchInSubtree, which skips wildcards. A cached
// wildcard-bearing type referenced with that same wildcard registered is then
// rejected, with an error demanding a registration that is already in place. A
// wildcard bakes nothing onto the shared node, so the stamp must exclude
// wildcard-only wiring.
func TestRegression_SchemaCacheWildcardConsistentRegistration(t *testing.T) {
	const innerJSON = `{"type":"record","name":"Inner","fields":[{"name":"v","type":"long","logicalType":"timestamp-millis"}]}`
	const outerRefJSON = `{"type":"record","name":"Outer","fields":[{"name":"in","type":"Inner"}]}`

	// Control: a non-wildcard CustomType registered consistently on both
	// parses resolves.
	t.Run("non-wildcard-consistent-resolves", func(t *testing.T) {
		mk := func() avro.CustomType {
			return avro.CustomType{
				AvroType:    "long",
				LogicalType: "timestamp-millis",
				Decode:      func(v any, _ *avro.SchemaNode) (any, error) { return v, avro.ErrSkipCustomType },
			}
		}
		c := &avro.SchemaCache{}
		if _, err := c.Parse(innerJSON, mk()); err != nil {
			t.Fatalf("inner parse: %v", err)
		}
		if _, err := c.Parse(outerRefJSON, mk()); err != nil {
			t.Fatalf("non-wildcard consistent registration should resolve: %v", err)
		}
	})

	// The same structure with a wildcard registered consistently on both
	// parses must also resolve.
	t.Run("wildcard-consistent-resolves", func(t *testing.T) {
		mk := func() avro.CustomType {
			return avro.CustomType{
				Decode: func(v any, _ *avro.SchemaNode) (any, error) { return v, avro.ErrSkipCustomType },
			}
		}
		c := &avro.SchemaCache{}
		if _, err := c.Parse(innerJSON, mk()); err != nil {
			t.Fatalf("inner parse with wildcard: %v", err)
		}
		if _, err := c.Parse(outerRefJSON, mk()); err != nil {
			t.Fatalf("wildcard registered consistently on BOTH parses must resolve, got: %v", err)
		}
	})
}

// ---------- slab_free_test.go ----------

// Issue #41: Decode of a slab-free schema (scalar leaf, no custom wiring)
// bypasses the internal slab pool entirely and runs on a nil *slab. These
// tests pin the classifier and prove, two-sidedly against the matrix corpus,
// that the classification exactly matches which compiled desers touch the
// slab. We reach internal state through the export_test.go bridges.

// TestScalarDecodeNoAllocAfterGC pins issue #41: Decode of a scalar schema must
// not allocate even when GC has drained the slab pool. That is the steady
// state of a low-allocation application. A Decode that unconditionally pulls a
// slab from the pool makes every post-GC decode pay a fresh allocation it never
// uses. The min-over-iterations guards against unrelated background mallocs. A
// genuinely slab-free Decode hits 0 on every quiet iteration, while a refilling
// one allocates on every one.
func TestScalarDecodeNoAllocAfterGC(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	s := avro.MustParse(`"long"`)
	wire := []byte{4}
	var v int64
	if _, err := s.Decode(wire, &v); err != nil { // warm one-time state
		t.Fatal(err)
	}
	minMallocs := ^uint64(0)
	var before, after runtime.MemStats
	for i := 0; i < 5; i++ {
		runtime.GC()
		runtime.GC()
		runtime.ReadMemStats(&before)
		avrotest.MustDecode(t, s, wire, &v)
		runtime.ReadMemStats(&after)
		minMallocs = min(minMallocs, after.Mallocs-before.Mallocs)
	}
	if v != 2 {
		t.Fatalf("decoded %d, want 2", v)
	}
	if minMallocs != 0 {
		t.Errorf("scalar Decode allocated on every post-GC iteration (min %d mallocs/op); slab pool should be bypassed for slab-free schemas", minMallocs)
	}
}

// TestSlabFreeClassifier pins slab-free membership across the axes that
// decide it: schema kind × logical type × custom wiring × cache-inherited
// custom × resolution × opts presence.
func TestSlabFreeClassifier(t *testing.T) {
	free := []string{
		`"null"`, `"boolean"`, `"int"`, `"long"`, `"float"`, `"double"`, `"bytes"`,
		`{"type":"fixed","name":"F","size":4}`,
		`{"type":"enum","name":"E","symbols":["A"]}`,
		`{"type":"int","logicalType":"date"}`,
		`{"type":"int","logicalType":"time-millis"}`,
		`{"type":"long","logicalType":"time-micros"}`,
		`{"type":"long","logicalType":"timestamp-millis"}`,
		`{"type":"long","logicalType":"timestamp-nanos"}`,
		`{"type":"long","logicalType":"local-timestamp-micros"}`,
		`{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
		`{"type":"fixed","name":"FD","size":4,"logicalType":"decimal","precision":4,"scale":2}`,
		`{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`,
		`{"type":"fixed","name":"D","size":12,"logicalType":"duration"}`,
	}
	needsSlab := []string{
		`"string"`,
		`{"type":"string","logicalType":"uuid"}`,
		`{"type":"record","name":"R","fields":[{"name":"f","type":"long"}]}`,
		`{"type":"array","items":"long"}`,
		`{"type":"map","values":"long"}`,
		`["null","long"]`,
		// Recursive and diamond named-type shapes: the second-occurrence
		// reference path can only appear under a composite kind, so it can
		// never smuggle a slab-needing node beneath a slab-free top.
		`{"type":"record","name":"L","fields":[{"name":"next","type":["null","L"]}]}`,
		`{"type":"record","name":"Dia","fields":[{"name":"a","type":{"type":"fixed","name":"Sh","size":2}},{"name":"b","type":"Sh"}]}`,
	}
	for _, sch := range free {
		if s := avro.MustParse(sch); !s.SlabFreeForTest() {
			t.Errorf("schema %s: slabFree=false, want true", sch)
		}
	}
	for _, sch := range needsSlab {
		if s := avro.MustParse(sch); s.SlabFreeForTest() {
			t.Errorf("schema %s: slabFree=true, want false", sch)
		}
	}

	// A custom decoder on a scalar forces the pool: the wrapper reads and
	// writes slab state (customMatches / bypassCustom).
	ct := avro.CustomType{
		AvroType: "fixed",
		Decode:   func(v any, _ *avro.SchemaNode) (any, error) { return v, nil },
	}
	sc, err := avro.Parse(`{"type":"fixed","name":"CF","size":2}`, ct)
	if err != nil {
		t.Fatalf("custom parse: %v", err)
	}
	if sc.SlabFreeForTest() {
		t.Error("custom-wired fixed: slabFree=true, want false")
	}
	var fxAny any
	if _, err := sc.Decode([]byte{1, 2}, &fxAny); err != nil {
		t.Errorf("custom-wired fixed decode: %v", err)
	}

	// Cache-inherited custom wraps: the defining parse wires the custom. A
	// later reference parse (re-registering the same custom, as the cache
	// requires) inherits the baked deser while its own overlay may stay
	// empty, applyCustomTypes visiting only newly built nodes. It must
	// therefore classify slab-needing through customBaked. The custom-free
	// twin inherits a plain deser and stays slab-free.
	ctEnum := avro.CustomType{
		AvroType: "enum",
		Decode:   func(v any, _ *avro.SchemaNode) (any, error) { return v, nil },
	}
	cc := &avro.SchemaCache{}
	if _, err := cc.Parse(`{"type":"enum","name":"CE","symbols":["A","B"]}`, ctEnum); err != nil {
		t.Fatalf("cache defining parse: %v", err)
	}
	ref, err := cc.Parse(`"CE"`, ctEnum)
	if err != nil {
		t.Fatalf("cache reference parse: %v", err)
	}
	if ref.SlabFreeForTest() {
		t.Error("cache-inherited custom enum reference: slabFree=true, want false")
	}
	cc2 := &avro.SchemaCache{}
	if _, err := cc2.Parse(`{"type":"enum","name":"CE","symbols":["A","B"]}`); err != nil {
		t.Fatalf("cache defining parse (no custom): %v", err)
	}
	ref2, err := cc2.Parse(`"CE"`)
	if err != nil {
		t.Fatalf("cache reference parse (no custom): %v", err)
	}
	if !ref2.SlabFreeForTest() {
		t.Error("cache-inherited plain enum reference: slabFree=false, want true")
	}
	var sym string
	if _, err := ref2.Decode([]byte{2}, &sym); err != nil || sym != "B" {
		t.Errorf("inherited enum nil-slab decode: %q, %v", sym, err)
	}

	// Non-identity resolution keeps the pool (zero value on the fresh
	// Schema). Promote/skip paths use the slab: bytes-to-string promotion
	// slab-copies, and resolved record skips bump the recursion depth.
	res, err := avro.Resolve(avro.MustParse(`"bytes"`), avro.MustParse(`"string"`))
	if err != nil {
		t.Fatalf("resolve bytes→string: %v", err)
	}
	if res.SlabFreeForTest() {
		t.Error("resolved bytes→string: slabFree=true, want false")
	}
	var str string
	if _, err := res.Decode([]byte{2, 'x'}, &str); err != nil || str != "x" {
		t.Errorf("resolved bytes→string decode: %q, %v", str, err)
	}
	// Identity resolution returns the reader itself; its own
	// classification applies because its own deser runs.
	ident, err := avro.Resolve(avro.MustParse(`"long"`), avro.MustParse(`"long"`))
	if err != nil {
		t.Fatalf("identity resolve: %v", err)
	}
	if !ident.SlabFreeForTest() {
		t.Error("identity-resolved long: slabFree=false, want true")
	}

	// Opts on a slab-free schema take the pooled path (opts only ever
	// alter slab state) and must stay correct.
	var lv int64
	if _, err := avro.MustParse(`"long"`).Decode([]byte{4}, &lv, avro.TaggedUnions()); err != nil || lv != 2 {
		t.Errorf("slab-free decode with opts: %d, %v", lv, err)
	}
}

// TestSlabFreeMatchesNilSlabOracle is the two-sided generative net. For every
// matrix fragment in every context, the slab-free classification must exactly
// equal the independent oracle "decoding every encoded value with a nil slab
// does not panic". Too eager is a user-visible crash in Decode; too
// conservative regresses issue #41. Non-vacuity: adding "string" to
// slabFreeKinds fails the string cells, and hardcoding slabFree=false fails
// every scalar cell the other way.
func TestSlabFreeMatchesNilSlabOracle(t *testing.T) {
	u := &uniq{}
	var freeCells, pooledCells int
	for _, fr := range matFrags() {
		for _, cx := range matCtxs() {
			if cx.skip != nil && cx.skip(fr.kind) {
				continue
			}
			schema := cx.schema(fr.schema(u), fr.kind, u)
			s, err := avro.Parse(schema)
			if err != nil {
				t.Fatalf("parse %s: %v", schema, err)
			}
			for _, val := range fr.values {
				wv := cx.wrap(val)
				wire, err := s.Encode(wv)
				if err != nil {
					t.Fatalf("encode %s %v: %v", schema, wv, err)
				}
				var nilGot any
				panicked := false
				var derr error
				var rest []byte
				func() {
					defer func() {
						if r := recover(); r != nil {
							panicked = true
						}
					}()
					rest, derr = s.DeserNilSlabForTest(wire, &nilGot)
				}()
				if panicked == s.SlabFreeForTest() {
					t.Fatalf("schema %s (frag %s, ctx %s): slabFree=%v but nil-slab decode panicked=%v",
						schema, fr.label, cx.label, s.SlabFreeForTest(), panicked)
				}
				if panicked {
					pooledCells++
					continue
				}
				freeCells++
				if derr != nil {
					t.Fatalf("schema %s: nil-slab decode error: %v", schema, derr)
				}
				if len(rest) != 0 {
					t.Fatalf("schema %s: nil-slab decode left %d bytes", schema, len(rest))
				}
				var poolGot any
				if _, err := s.Decode(wire, &poolGot); err != nil {
					t.Fatalf("schema %s: pooled decode error: %v", schema, err)
				}
				if !matEqual(nilGot, poolGot) {
					t.Fatalf("schema %s value %v: nil-slab decode %v != pooled decode %v", schema, wv, nilGot, poolGot)
				}
			}
		}
	}
	if freeCells == 0 || pooledCells == 0 {
		t.Fatalf("vacuous net: %d slab-free cells, %d pooled cells — both sides must be exercised", freeCells, pooledCells)
	}
	t.Logf("oracle cells: %d slab-free, %d pooled", freeCells, pooledCells)
}

// ---------- cyclic_field_type_test.go ----------

// A cyclic non-struct pointer type (type P *P, whose reflect graph has
// P.Elem() == P) used as a struct field must *not* crash the process. The
// unsafe struct-field encode compiler walks the field's pointer type graph.
// Without a bound it recurses forever at compile time and overflows the
// goroutine stack. The bound declines to the reflect slow path, which errors
// cleanly on the nil cyclic value, matching every other indirect walk in the
// package.
type cyclicPtrFieldType *cyclicPtrFieldType

type recordWithCyclicPtrField struct {
	F cyclicPtrFieldType `avro:"F"`
}

func TestRegression_EncodeStructCyclicPointerFieldTerminates(t *testing.T) {
	s := avro.MustParse(`{"type":"record","name":"S","fields":[{"name":"F","type":"int"}]}`)
	// Run in a goroutine with a generous deadline, so a lost bound surfaces as
	// a timeout rather than hanging the whole suite. A true stack overflow is
	// fatal and aborts the binary, which is itself an unmistakable failure
	// signal.
	done := make(chan error, 1)
	go func() {
		_, err := s.Encode(&recordWithCyclicPtrField{})
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("encode of a cyclic-pointer-typed field must error, got nil")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("encode of a cyclic-pointer-typed field did not terminate")
	}
}

// The unsafe struct-field fast encode path must accept exactly the
// pointer-chain depths the reflect encoder accepts, and every accepted chain
// must round-trip through Decode. Without the bound, the fast path accepts
// arbitrarily deep chains the reflect encoder and the package's own Decode
// reject. That encodes wire that cannot be read back. We drive a struct field
// of int-pointer depth 1..8, asserting fast == reflect on the verdict plus a
// decode-back on every accept.
func TestGenerative_StructFieldPointerChainMatchesReflect(t *testing.T) {
	rec := avro.MustParse(`{"type":"record","name":"S","fields":[{"name":"F","type":"int"}]}`)
	scalar := avro.MustParse(`"int"`)
	intType := reflect.TypeOf(int(0))

	anyAccepted := false
	for depth := 1; depth <= 8; depth++ {
		ptrType := intType
		for i := 0; i < depth; i++ {
			ptrType = reflect.PointerTo(ptrType)
		}
		structType := reflect.StructOf([]reflect.StructField{
			{Name: "F", Type: ptrType, Tag: `avro:"F"`},
		})

		// Build a fully-allocated chain ending in int(7).
		leaf := reflect.New(intType)
		leaf.Elem().SetInt(7)
		cur := leaf
		for i := 1; i < depth; i++ {
			p := reflect.New(cur.Type())
			p.Elem().Set(cur)
			cur = p
		}
		sv := reflect.New(structType)
		sv.Elem().Field(0).Set(cur)

		_, structErr := rec.Encode(sv.Interface())
		_, scalarErr := scalar.Encode(cur.Interface())

		if (structErr == nil) != (scalarErr == nil) {
			t.Fatalf("depth=%d: struct-field encode (err=%v) and reflect scalar encode (err=%v) disagree — fast path accepts a depth the reflect path rejects",
				depth, structErr, scalarErr)
		}
		if structErr == nil {
			anyAccepted = true
			// Accepted: the wire must round-trip back to the int.
			wire, err := rec.Encode(sv.Interface())
			if err != nil {
				t.Fatalf("depth=%d: re-encode failed: %v", depth, err)
			}
			out := reflect.New(structType)
			if _, err := rec.Decode(wire, out.Interface()); err != nil {
				t.Fatalf("depth=%d: struct-field encode accepted but Decode rejected the wire: %v", depth, err)
			}
		}
	}
	if !anyAccepted {
		t.Fatal("expected at least the shallow pointer depths to be accepted")
	}
}

// The same bound must cover the array-element route into the pointer arm: a
// cyclic pointer as a slice element must not crash the field-type compile, and
// a shallow pointer element must still round-trip.
type cyclicSliceElem *cyclicSliceElem

func TestRegression_SliceElementPointerChainBounded(t *testing.T) {
	t.Run("cyclic_element_terminates", func(t *testing.T) {
		type R struct {
			F []cyclicSliceElem `avro:"f"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"array","items":"int"}}]}`)
		done := make(chan struct{}, 1)
		go func() {
			s.Encode(&R{}) // nil slice; the compile of the cyclic element type must terminate
			done <- struct{}{}
		}()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Fatal("slice-of-cyclic-pointer field compile did not terminate")
		}
	})
	t.Run("shallow_element_roundtrips", func(t *testing.T) {
		type R struct {
			F []*int32 `avro:"f"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"array","items":"int"}}]}`)
		v := int32(7)
		wire := avrotest.MustEncode(t, s, &R{F: []*int32{&v}})
		var out R
		avrotest.MustDecode(t, s, wire, &out)
		if len(out.F) != 1 || out.F[0] == nil || *out.F[0] != 7 {
			t.Fatalf("roundtrip wrong: %#v", out)
		}
	})
}

// Encode and decode must agree on the maximum pointer-chain depth they accept.
// An encode-side indirect that accepts only maxIndirectDepth-1 levels, because
// it confirms a non-pointer base on a following loop iteration the cap has
// already spent, disagrees with indirectAlloc accepting maxIndirectDepth. A
// value exactly maxIndirectDepth pointers deep then decodes but fails to
// encode, a round-trip break for a hand-written schema. Both accept a chain
// bottoming at a non-pointer base within the cap and reject one deeper, on
// both wires.
func TestRegression_PointerChainEncodeDecodeDepthParity(t *testing.T) {
	s := avro.MustParse(`["null","int"]`)

	// At maxIndirectDepth (5) levels: a non-nil chain must round-trip on both
	// wires. Build *****int = &&&&&(7).
	n := int64(7)
	p1 := &n
	p2 := &p1
	p3 := &p2
	p4 := &p3
	p5 := &p4 // 5 pointers, non-nil all the way down

	bin, err := s.Encode(p5)
	if err != nil {
		t.Fatalf("binary Encode of a %d-deep non-nil pointer must succeed (encode↔decode parity): %v", maxDepthLevels, err)
	}
	var bg *****int64
	if _, err := s.Decode(bin, &bg); err != nil {
		t.Fatalf("binary Decode into a %d-deep pointer must succeed: %v", maxDepthLevels, err)
	}
	if bg == nil || *****bg != 7 {
		t.Fatalf("binary round-trip mismatch at depth %d", maxDepthLevels)
	}

	js, err := s.EncodeJSON(p5)
	if err != nil {
		t.Fatalf("JSON Encode of a %d-deep non-nil pointer must succeed: %v", maxDepthLevels, err)
	}
	var jg *****int64
	if err := s.DecodeJSON(js, &jg); err != nil {
		t.Fatalf("JSON Decode into a %d-deep pointer must succeed: %v", maxDepthLevels, err)
	}
	if jg == nil || *****jg != 7 {
		t.Fatalf("JSON round-trip mismatch at depth %d", maxDepthLevels)
	}

	// One level deeper (6) must be rejected symmetrically across both wires and
	// both directions. Encode (indirect) and decode (indirectAlloc) refuse a
	// chain past the cap, preserving the cyclic-interface DoS guard. JSON
	// matches binary on decode too: a union pointer target is indirected
	// exactly once, not twice (unionTarget returns a concrete target un-peeled,
	// so the branch decode's single indirectAlloc caps it).
	q6 := &p5 // 6 pointers
	if _, err := s.Encode(q6); err == nil {
		t.Fatalf("binary Encode of a %d-deep pointer must be rejected", maxDepthLevels+1)
	}
	var b6 ******int64
	if _, err := s.Decode(bin, &b6); err == nil {
		t.Fatalf("binary Decode into a %d-deep pointer must be rejected", maxDepthLevels+1)
	}
	if _, err := s.EncodeJSON(q6); err == nil {
		t.Fatalf("JSON Encode of a %d-deep pointer must be rejected", maxDepthLevels+1)
	}
	var j6 ******int64
	if err := s.DecodeJSON(js, &j6); err == nil {
		t.Fatalf("JSON Decode into a %d-deep pointer must be rejected (parity with binary)", maxDepthLevels+1)
	}
}

// Binary and JSON decode must accept the same set of pointer-target depths. A
// JSON union decode that indirects at two stages (unionTarget then the branch's
// decodeKind) peels up to 2*maxIndirectDepth levels and accepts targets binary
// rejects. Sweep every depth and assert the two wires agree.
func TestGenerative_UnionPointerTargetDepthBinaryJSONParity(t *testing.T) {
	s := avro.MustParse(`["null","int"]`)
	bin, _ := s.Encode(int64(7))
	js, _ := s.EncodeJSON(int64(7))
	intType := reflect.TypeOf(int64(0))
	for depth := 1; depth <= 9; depth++ {
		pt := intType
		for i := 0; i < depth; i++ {
			pt = reflect.PointerTo(pt)
		}
		_, binErr := s.Decode(bin, reflect.New(pt).Interface())
		jsonErr := s.DecodeJSON(js, reflect.New(pt).Interface())
		if (binErr != nil) != (jsonErr != nil) {
			t.Errorf("depth=%d: binary and JSON decode disagree on accept/reject (binErr=%v, jsonErr=%v)", depth, binErr, jsonErr)
		}
	}
}

// maxDepthLevels mirrors the package-internal maxIndirectDepth (5) for the
// external test above. If the internal cap changes, the depth-parity test's
// literal pointer types (*****T at the cap, ******T past it) must change with
// it.
const maxDepthLevels = 5

// An array<record> element's pointer chain must be capped at the same
// maxIndirectDepth every other context enforces. The unsafe struct-field array
// fast path peels one pointer level inline and hands the element to the record
// (de)serializer's own indirect budget. Without declining a multi-level element
// it accepts a chain one past the cap the reflect path, the JSON encoder, and
// the nullunion array arms enforce. A []******Record field then encodes a wire
// that the struct's own JSON encoder, a top-level encode, and a top-level
// decode all reject. The fast path stays single-pointer.
func TestRegression_ArrayRecordElementPointerChainDepthParity(t *testing.T) {
	const recJSON = `{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}`
	sArr := avro.MustParse(`{"type":"array","items":` + recJSON + `}`)
	sOuter := avro.MustParse(`{"type":"record","name":"Outer","fields":[{"name":"arr","type":{"type":"array","items":` + recJSON + `}}]}`)

	innerStruct := reflect.StructOf([]reflect.StructField{
		{Name: "X", Type: reflect.TypeOf(int32(0)), Tag: `avro:"x"`},
	})
	// chain returns the slice element type at the given pointer depth and a
	// non-nil value of that type bottoming at Inner{X:7}.
	chain := func(depth int) (reflect.Type, reflect.Value) {
		elemType := innerStruct
		for range depth {
			elemType = reflect.PointerTo(elemType)
		}
		v := reflect.New(innerStruct).Elem()
		v.Field(0).SetInt(7)
		for range depth {
			p := reflect.New(v.Type())
			p.Elem().Set(v)
			v = p
		}
		return elemType, v
	}

	for depth := 0; depth <= maxDepthLevels+1; depth++ {
		elemType, elemVal := chain(depth)
		sliceType := reflect.SliceOf(elemType)

		// Oracle: the same []*...*Inner value at top level goes through the
		// reflect serArray path, which peels with a single indirect budget,
		// capping at the cap. Its accept/reject is the boundary every other
		// context must match.
		topSlice := reflect.MakeSlice(sliceType, 1, 1)
		topSlice.Index(0).Set(elemVal)
		_, topErr := sArr.AppendEncode(nil, topSlice.Interface())

		// Struct field exercises the unsafe usArrayRecord fast path (the value is
		// addressable through reflect.New, so the unsafe compile is selected).
		outerType := reflect.StructOf([]reflect.StructField{
			{Name: "Arr", Type: sliceType, Tag: `avro:"arr"`},
		})
		outer := reflect.New(outerType)
		sl := reflect.MakeSlice(sliceType, 1, 1)
		sl.Index(0).Set(elemVal)
		outer.Elem().Field(0).Set(sl)

		binWire, binErr := sOuter.AppendEncode(nil, outer.Interface())
		_, jsonErr := sOuter.AppendEncodeJSON(nil, outer.Interface())

		if (binErr == nil) != (topErr == nil) {
			t.Errorf("depth %d: struct-field unsafe encode and top-level reflect encode disagree (structErr=%v topErr=%v)", depth, binErr, topErr)
		}
		if (binErr == nil) != (jsonErr == nil) {
			t.Errorf("depth %d: binary and JSON struct-field encode disagree (binErr=%v jsonErr=%v)", depth, binErr, jsonErr)
		}

		if depth <= maxDepthLevels {
			// Within the cap: must encode and round-trip back to the value.
			if binErr != nil {
				t.Errorf("depth %d (<= cap %d) must encode: %v", depth, maxDepthLevels, binErr)
				continue
			}
			got := reflect.New(outerType)
			if _, err := sOuter.Decode(binWire, got.Interface()); err != nil {
				t.Errorf("depth %d (<= cap) must decode: %v", depth, err)
				continue
			}
			gv := got.Elem().Field(0).Index(0)
			for gv.Kind() == reflect.Pointer {
				if gv.IsNil() {
					t.Errorf("depth %d round-trip produced a nil element", depth)
					break
				}
				gv = gv.Elem()
			}
			if gv.Kind() == reflect.Struct && gv.Field(0).Int() != 7 {
				t.Errorf("depth %d round-trip value mismatch: got %v", depth, gv.Field(0).Int())
			}
		} else if binErr == nil {
			// depth == cap+1: every encode context must reject.
			t.Errorf("depth %d (> cap %d) struct-field binary encode must reject; wire=%x", depth, maxDepthLevels, binWire)
		}
	}

	// Decode-side neuter-proof: a valid wire (one record element) decoded into a
	// struct whose array element is one level past the cap must reject. The fast
	// udArrayPtrRecord must not peel 1+maxIndirectDepth levels.
	depth0Slice := reflect.SliceOf(innerStruct)
	d0 := reflect.New(reflect.StructOf([]reflect.StructField{
		{Name: "Arr", Type: depth0Slice, Tag: `avro:"arr"`},
	}))
	one := reflect.New(innerStruct).Elem()
	one.Field(0).SetInt(7)
	s0 := reflect.MakeSlice(depth0Slice, 1, 1)
	s0.Index(0).Set(one)
	d0.Elem().Field(0).Set(s0)
	validWire, err := sOuter.AppendEncode(nil, d0.Interface())
	if err != nil {
		t.Fatalf("setup: encode of []Inner must succeed: %v", err)
	}
	deepElem := innerStruct
	for range maxDepthLevels + 1 {
		deepElem = reflect.PointerTo(deepElem)
	}
	deepOuter := reflect.New(reflect.StructOf([]reflect.StructField{
		{Name: "Arr", Type: reflect.SliceOf(deepElem), Tag: `avro:"arr"`},
	}))
	if _, err := sOuter.Decode(validWire, deepOuter.Interface()); err == nil {
		t.Errorf("decode into an array field whose element is %d pointers deep must reject (cap %d)", maxDepthLevels+1, maxDepthLevels)
	}
}

// A ["null", record] nullunion record field's pointer chain must be capped at
// maxIndirectDepth on decode, matching encode and every other context. The
// unsafe udNullUnionRecord consumes the nullunion's outer pointer and then
// indirectAllocs the remainder. Without declining a multi-level target it
// accepts a chain one past the cap the encode side, the reflect deser, and the
// JSON paths enforce. A valid wire then decodes into a ******record field where
// encode of the same type rejects. The fast path stays single-pointer.
func TestRegression_NullUnionRecordFieldPointerChainDepthParity(t *testing.T) {
	const recJSON = `{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}`
	s := avro.MustParse(`{"type":"record","name":"Outer","fields":[{"name":"f","type":["null",` + recJSON + `]}]}`)

	innerStruct := reflect.StructOf([]reflect.StructField{
		{Name: "X", Type: reflect.TypeOf(int32(0)), Tag: `avro:"x"`},
	})
	ptrType := func(depth int) reflect.Type {
		ft := innerStruct
		for range depth {
			ft = reflect.PointerTo(ft)
		}
		return ft
	}
	nonNilVal := func(depth int) reflect.Value {
		v := reflect.New(innerStruct).Elem()
		v.Field(0).SetInt(7)
		for range depth {
			p := reflect.New(v.Type())
			p.Elem().Set(v)
			v = p
		}
		return v
	}
	outerOf := func(fieldType reflect.Type) reflect.Type {
		return reflect.StructOf([]reflect.StructField{{Name: "F", Type: fieldType, Tag: `avro:"f"`}})
	}

	// A valid wire: a depth-1 (*record) non-nil value encodes the value branch.
	d1Type := outerOf(ptrType(1))
	d1 := reflect.New(d1Type)
	d1.Elem().Field(0).Set(nonNilVal(1))
	validWire, err := s.AppendEncode(nil, d1.Interface())
	if err != nil {
		t.Fatalf("setup: encode of a *record nullunion value must succeed: %v", err)
	}

	for depth := 1; depth <= maxDepthLevels+1; depth++ {
		outerType := outerOf(ptrType(depth))
		outer := reflect.New(outerType)
		outer.Elem().Field(0).Set(nonNilVal(depth))

		binWire, binErr := s.AppendEncode(nil, outer.Interface())
		_, jsonErr := s.AppendEncodeJSON(nil, outer.Interface())
		// Decode the valid depth-1 wire into this depth's target type: the
		// decode-side boundary, independent of whether encode produced a wire.
		_, decErr := s.Decode(validWire, reflect.New(outerType).Interface())

		if (binErr == nil) != (jsonErr == nil) {
			t.Errorf("depth %d: binary vs JSON encode disagree (bin=%v json=%v)", depth, binErr, jsonErr)
		}
		if (binErr == nil) != (decErr == nil) {
			t.Errorf("depth %d: encode and decode disagree on the cap boundary (encErr=%v decErr=%v)", depth, binErr, decErr)
		}

		if depth <= maxDepthLevels {
			if binErr != nil {
				t.Errorf("depth %d (<= cap %d) must encode: %v", depth, maxDepthLevels, binErr)
				continue
			}
			got := reflect.New(outerType)
			if _, err := s.Decode(binWire, got.Interface()); err != nil {
				t.Errorf("depth %d (<= cap) must decode: %v", depth, err)
				continue
			}
			gv := got.Elem().Field(0)
			for gv.Kind() == reflect.Pointer {
				if gv.IsNil() {
					t.Errorf("depth %d round-trip produced a nil value", depth)
					break
				}
				gv = gv.Elem()
			}
			if gv.Kind() == reflect.Struct && gv.Field(0).Int() != 7 {
				t.Errorf("depth %d round-trip value mismatch: got %v", depth, gv.Field(0).Int())
			}
		} else if decErr == nil {
			// depth == cap+1: decode into the too-deep target must reject (the
			// decode-side neuter-proof), matching encode.
			t.Errorf("depth %d (> cap %d) decode into a too-deep nullunion field must reject", depth, maxDepthLevels)
		}
	}
}

// ---------- named_byte_element_test.go ----------

// A Go byte-container type whose element type is a named byte (type B byte;
// [N]B, []B) has element Kind Uint8 but an element type that is not exactly
// uint8. The byte encoder accepts such types, so by the encode/decode
// target-type parity contract every decoder and the JSON encoder must too.
// reflect.Copy and Set(reflect.ValueOf([]byte)) require an exactly-uint8
// element and panic otherwise. That reaches the caller of a public API on a
// valid Go value. We pin every fixed/bytes/uuid path on both wires, scalar and
// as a struct field, and through bytes-to-string+uuid promotion.

type nbeByte byte
type nbeFix3 [3]nbeByte
type nbeUUID [16]nbeByte
type nbeSlice []nbeByte

func TestMatrix_NamedByteElementRoundTrip(t *testing.T) {
	uuidWire := nbeUUID{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe}

	t.Run("fixed/binary", func(t *testing.T) {
		s := avro.MustParse(`{"type":"fixed","name":"F","size":3}`)
		in := nbeFix3{1, 2, 3}
		b := avrotest.MustAppendEncode(t, s, nil, in)
		var out nbeFix3
		avrotest.MustDecode(t, s, b, &out)
		if out != in {
			t.Fatalf("round-trip: got %v want %v", out, in)
		}
	})

	t.Run("fixed/json", func(t *testing.T) {
		s := avro.MustParse(`{"type":"fixed","name":"F","size":3}`)
		in := nbeFix3{1, 2, 3}
		j := avrotest.MustAppendEncodeJSON(t, s, nil, in)
		var out nbeFix3
		avrotest.MustDecodeJSON(t, s, j, &out)
		if out != in {
			t.Fatalf("round-trip json: got %v want %v", out, in)
		}
	})

	t.Run("bytes/array/binary", func(t *testing.T) {
		s := avro.MustParse(`"bytes"`)
		in := nbeFix3{4, 5, 6}
		b := avrotest.MustAppendEncode(t, s, nil, in)
		var out nbeFix3
		avrotest.MustDecode(t, s, b, &out)
		if out != in {
			t.Fatalf("round-trip: got %v want %v", out, in)
		}
	})

	t.Run("bytes/slice/binary+json", func(t *testing.T) {
		s := avro.MustParse(`"bytes"`)
		in := nbeSlice{7, 8, 9}
		b := avrotest.MustAppendEncode(t, s, nil, in)
		var out nbeSlice
		avrotest.MustDecode(t, s, b, &out)
		if !bytes.Equal([]byte(toBytes(out)), []byte{7, 8, 9}) {
			t.Fatalf("round-trip: got %v", out)
		}
		j := avrotest.MustAppendEncodeJSON(t, s, nil, in)
		var outJ nbeSlice
		avrotest.MustDecodeJSON(t, s, j, &outJ)
	})

	t.Run("bytes/array->fixed-slice-target/binary", func(t *testing.T) {
		// deserFixed's slice arm must SetBytes, not Set(reflect.ValueOf).
		s := avro.MustParse(`{"type":"fixed","name":"F","size":3}`)
		b := avrotest.MustAppendEncode(t, s, nil, nbeFix3{1, 2, 3})
		var out nbeSlice
		if _, err := s.Decode(b, &out); err != nil {
			t.Fatalf("decode into named-byte slice: %v", err)
		}
		if len(out) != 3 {
			t.Fatalf("got %v", out)
		}
	})

	t.Run("uuid-fixed/binary+json", func(t *testing.T) {
		s := avro.MustParse(`{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`)
		b := avrotest.MustAppendEncode(t, s, nil, uuidWire)
		var out nbeUUID
		avrotest.MustDecode(t, s, b, &out)
		if out != uuidWire {
			t.Fatalf("round-trip: got %v want %v", out, uuidWire)
		}
		j := avrotest.MustAppendEncodeJSON(t, s, nil, uuidWire)
		var outJ nbeUUID
		avrotest.MustDecodeJSON(t, s, j, &outJ)
		if outJ != uuidWire {
			t.Fatalf("round-trip json: got %v want %v", outJ, uuidWire)
		}
	})

	t.Run("uuid-string->[16]named/binary+json", func(t *testing.T) {
		s := avro.MustParse(`{"type":"string","logicalType":"uuid"}`)
		str := "01234567-89ab-cdef-1032-547698badcfe"
		b := avrotest.MustAppendEncode(t, s, nil, str)
		var out nbeUUID
		avrotest.MustDecode(t, s, b, &out)
		if out != uuidWire {
			t.Fatalf("round-trip: got %v want %v", out, uuidWire)
		}
		j := avrotest.MustAppendEncodeJSON(t, s, nil, str)
		var outJ nbeUUID
		avrotest.MustDecodeJSON(t, s, j, &outJ)
		if outJ != uuidWire {
			t.Fatalf("round-trip json: got %v want %v", outJ, uuidWire)
		}
	})

	t.Run("struct-field-fixed/unsafe-path/binary", func(t *testing.T) {
		type R struct {
			F nbeFix3
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"F","type":{"type":"fixed","name":"F3","size":3}}]}`)
		in := R{F: nbeFix3{1, 2, 3}}
		b := avrotest.MustAppendEncode(t, s, nil, in)
		var out R
		avrotest.MustDecode(t, s, b, &out)
		if out != in {
			t.Fatalf("round-trip: got %v want %v", out, in)
		}
	})

	t.Run("struct-field-uuid/unsafe-path/binary", func(t *testing.T) {
		type R struct {
			U nbeUUID `avro:"u"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"u","type":{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}}]}`)
		in := R{U: uuidWire}
		b := avrotest.MustAppendEncode(t, s, nil, in)
		var out R
		avrotest.MustDecode(t, s, b, &out)
		if out != in {
			t.Fatalf("round-trip: got %v want %v", out, in)
		}
	})

	t.Run("bytes->string+uuid promotion/[16]named", func(t *testing.T) {
		// promoteBytesToStringUUID: writer bytes, reader string+uuid, target
		// [16]named. reflect.Copy panics here; copyBytesToArray does not.
		w := avro.MustParse(`"bytes"`)
		r := avro.MustParse(`{"type":"string","logicalType":"uuid"}`)
		res := avrotest.MustResolve(t, w, r)
		// Writer encodes the 36-char canonical UUID string as bytes.
		b := avrotest.MustAppendEncode(t, w, nil, []byte("01234567-89ab-cdef-1032-547698badcfe"))
		var out nbeUUID
		if _, err := res.Decode(b, &out); err != nil {
			t.Fatalf("promoted decode: %v", err)
		}
		if out != uuidWire {
			t.Fatalf("promotion round-trip: got %v want %v", out, uuidWire)
		}
	})
}

// toBytes converts a named byte slice to []byte for comparison.
func toBytes(s nbeSlice) []byte {
	b := make([]byte, len(s))
	for i := range s {
		b[i] = byte(s[i])
	}
	return b
}

// TestRegression_ExactByteContainersStillRoundTrip is the boundary-1 control.
// The exact-uint8 element fast path (the common [N]byte / []byte case) must
// keep working unchanged after the named-byte-element fix relaxed the copy
// helpers.
func TestRegression_ExactByteContainersStillRoundTrip(t *testing.T) {
	t.Run("fixed/[3]byte", func(t *testing.T) {
		s := avro.MustParse(`{"type":"fixed","name":"F","size":3}`)
		in := [3]byte{1, 2, 3}
		b, _ := s.AppendEncode(nil, in)
		var out [3]byte
		if _, err := s.Decode(b, &out); err != nil || out != in {
			t.Fatalf("got %v err %v", out, err)
		}
		j, _ := s.AppendEncodeJSON(nil, in)
		var outJ [3]byte
		if err := s.DecodeJSON(j, &outJ); err != nil || outJ != in {
			t.Fatalf("json got %v err %v", outJ, err)
		}
	})
	t.Run("uuid-fixed/[16]byte", func(t *testing.T) {
		s := avro.MustParse(`{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`)
		in := [16]byte{1, 2, 3, 4}
		b, _ := s.AppendEncode(nil, in)
		var out [16]byte
		if _, err := s.Decode(b, &out); err != nil || out != in {
			t.Fatalf("got %v err %v", out, err)
		}
	})
	t.Run("bytes/[]byte", func(t *testing.T) {
		s := avro.MustParse(`"bytes"`)
		in := []byte{9, 8, 7}
		b, _ := s.AppendEncode(nil, in)
		var out []byte
		if _, err := s.Decode(b, &out); err != nil || !bytes.Equal(out, in) {
			t.Fatalf("got %v err %v", out, err)
		}
	})
}

// ---------- unsafe_nullunion_nil_test.go ----------

// The 2-branch ["null",T] optimization treats a value as the null branch
// exactly when isNilValue reports it nil, peeling pointer/interface layers then
// nil-checking the bottom kind. A non-nil pointer to a nil slice/map is
// therefore null. The reflect-binary and JSON encoders both honor isNilValue,
// and the unsafe struct fast path must agree. One value must encode to one
// branch whether the struct is passed addressable or by value. A divergence is
// addressability-dependent wire corruption.

// nullUnionParity encodes v addressable (unsafe), by value (reflect), and as
// JSON, then asserts all three pick the same union branch. The two binary wires
// are byte-identical, the JSON wires are byte-identical, and the binary wire
// decodes to wantNull (true => the field comes back nil/absent).
func nullUnionParity(t *testing.T, schema string, v any, vptr any, wantNull bool) {
	t.Helper()
	s := avro.MustParse(schema)

	wPtr, err := s.AppendEncode(nil, vptr) // addressable struct -> unsafe fast path
	if err != nil {
		t.Fatalf("Encode(&v): %v", err)
	}
	wVal, err := s.AppendEncode(nil, v) // non-addressable -> reflect path
	if err != nil {
		t.Fatalf("Encode(v): %v", err)
	}
	if !bytes.Equal(wPtr, wVal) {
		t.Errorf("binary branch divergence (addressable vs value): Encode(&v)=% x  Encode(v)=% x", wPtr, wVal)
	}

	jPtr, err := s.AppendEncodeJSON(nil, vptr)
	if err != nil {
		t.Fatalf("EncodeJSON(&v): %v", err)
	}
	jVal, err := s.AppendEncodeJSON(nil, v)
	if err != nil {
		t.Fatalf("EncodeJSON(v): %v", err)
	}
	if !bytes.Equal(jPtr, jVal) {
		t.Errorf("JSON branch divergence (addressable vs value): EncodeJSON(&v)=%s  EncodeJSON(v)=%s", jPtr, jVal)
	}

	// The unsafe binary wire must agree with JSON on the branch too. Decode the
	// unsafe binary wire into a map and confirm the field is/isn't null.
	var got map[string]any
	if _, err := s.Decode(wPtr, &got); err != nil {
		t.Fatalf("Decode(Encode(&v)): %v", err)
	}
	isNull := got["f"] == nil
	if isNull != wantNull {
		t.Errorf("decode(Encode(&v)).f null=%v, want null=%v (value=%#v; wire=% x)", isNull, wantNull, got["f"], wPtr)
	}
}

func TestMatrix_NullUnionPtrToNilSliceEncodeParity(t *testing.T) {
	t.Run("ptr-to-nil-slice/array-null-first", func(t *testing.T) {
		var nilSlice []string
		nullUnionParity(t,
			`{"type":"record","name":"R","fields":[{"name":"f","type":["null",{"type":"array","items":"string"}]}]}`,
			Rec{F: &nilSlice}, &Rec{F: &nilSlice}, true)
	})

	t.Run("ptr-to-nil-slice/array-null-second", func(t *testing.T) {
		var nilSlice []string
		nullUnionParity(t,
			`{"type":"record","name":"R","fields":[{"name":"f","type":[{"type":"array","items":"string"},"null"]}]}`,
			Rec{F: &nilSlice}, &Rec{F: &nilSlice}, true)
	})

	t.Run("ptr-to-nil-bytes", func(t *testing.T) {
		type Rec struct {
			F *[]byte `avro:"f"`
		}
		var nilBytes []byte
		nullUnionParity(t,
			`{"type":"record","name":"R","fields":[{"name":"f","type":["null","bytes"]}]}`,
			Rec{F: &nilBytes}, &Rec{F: &nilBytes}, true)
	})

	t.Run("array-element-ptr-to-nil-slice", func(t *testing.T) {
		type Rec struct {
			A []*[]string `avro:"a"`
		}
		var nilSlice []string
		// One element: a non-nil pointer to a nil slice -> that element is the
		// null branch; reflect/JSON agree, unsafe must too.
		s := `{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"array","items":["null",{"type":"array","items":"string"}]}}]}`
		sc := avro.MustParse(s)
		wPtr, err := sc.AppendEncode(nil, &Rec{A: []*[]string{&nilSlice}})
		if err != nil {
			t.Fatalf("Encode(&v): %v", err)
		}
		wVal, err := sc.AppendEncode(nil, Rec{A: []*[]string{&nilSlice}})
		if err != nil {
			t.Fatalf("Encode(v): %v", err)
		}
		if !bytes.Equal(wPtr, wVal) {
			t.Errorf("array-element branch divergence: Encode(&v)=% x  Encode(v)=% x", wPtr, wVal)
		}
		jPtr, _ := sc.AppendEncodeJSON(nil, &Rec{A: []*[]string{&nilSlice}})
		jVal, _ := sc.AppendEncodeJSON(nil, Rec{A: []*[]string{&nilSlice}})
		if !bytes.Equal(jPtr, jVal) {
			t.Errorf("array-element JSON divergence: %s vs %s", jPtr, jVal)
		}
	})
}

func TestRegression_NullUnionPtrToNilMapEncodeParity(t *testing.T) {
	type Rec struct {
		F *map[string]string `avro:"f"`
	}
	var nilMap map[string]string
	nullUnionParity(t,
		`{"type":"record","name":"R","fields":[{"name":"f","type":["null",{"type":"map","values":"string"}]}]}`,
		Rec{F: &nilMap}, &Rec{F: &nilMap}, true)
}

func TestRegression_NullUnionPtrToNonNilSliceControl(t *testing.T) {
	// Control: a non-nil slice behind the pointer is the value branch on every
	// path.
	good := []string{"x"}
	nullUnionParity(t,
		`{"type":"record","name":"R","fields":[{"name":"f","type":["null",{"type":"array","items":"string"}]}]}`,
		Rec{F: &good}, &Rec{F: &good}, false)
}

// ---------- omitzero_bsoft_test.go ----------

// TestMatrix_OmitzeroFillsSchemaDefault pins the b-soft omitzero contract. On a
// zero/IsZero value, omitzero encodes the field's default if it has one, else
// null if the field is nullable, else nothing (encoding the zero, a forced
// no-op). It therefore matches map[string]any default-fill wherever a default
// exists. It deliberately diverges for a nullable field with no default, where
// omitzero encodes null while map-fill errors ("missing key").
func TestMatrix_OmitzeroFillsSchemaDefault(t *testing.T) {
	type R struct {
		Count int `avro:"Count,omitzero"`
	}
	cases := []struct {
		name, schema, wantHex string
		mapParity             bool // struct-omitzero wire must equal map{} default-fill wire
	}{
		// With a default, fill it. Matches map-fill.
		{"non-union int default", `{"type":"record","name":"R","fields":[{"name":"Count","type":"int","default":10}]}`, "14", true},
		{"null-second union default", `{"type":"record","name":"R","fields":[{"name":"Count","type":["int","null"],"default":5}]}`, "000a", true},
		// Nullable with no default gives null; map-fill errors instead.
		{"null-second union no default", `{"type":"record","name":"R","fields":[{"name":"Count","type":["int","null"]}]}`, "02", false},
		// Non-union with no default is a no-op: encode the zero.
		{"non-union int no default", `{"type":"record","name":"R","fields":[{"name":"Count","type":"int"}]}`, "00", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := avro.MustParse(tc.schema)
			structWire, err := s.AppendEncode(nil, R{Count: 0})
			if err != nil {
				t.Fatalf("encode struct: %v", err)
			}
			if got := fmt.Sprintf("%x", structWire); got != tc.wantHex {
				t.Errorf("omitzero wire = %s, want %s", got, tc.wantHex)
			}
			// The unsafe (addressable) path must agree with the reflect path.
			// It delegates actionable omitzero to the reflect slow path, so we
			// pin here that the delegation stays correct.
			ptrWire, err := s.AppendEncode(nil, &R{Count: 0})
			if err != nil {
				t.Fatalf("encode &struct (unsafe path): %v", err)
			}
			if !bytes.Equal(structWire, ptrWire) {
				t.Errorf("unsafe path diverges from reflect: value=%x ptr=%x", structWire, ptrWire)
			}
			if tc.mapParity {
				// Binary parity with map[string]any default-fill (the oracle).
				mapWire, err := s.AppendEncode(nil, map[string]any{})
				if err != nil {
					t.Fatalf("encode map{}: %v", err)
				}
				if !bytes.Equal(structWire, mapWire) {
					t.Errorf("omitzero != map default-fill (binary): struct=%x map=%x", structWire, mapWire)
				}
				// JSON path parity with the same oracle.
				sj, err := s.EncodeJSON(R{Count: 0})
				if err != nil {
					t.Fatalf("encode struct JSON: %v", err)
				}
				mj, err := s.EncodeJSON(map[string]any{})
				if err != nil {
					t.Fatalf("encode map{} JSON: %v", err)
				}
				if !bytes.Equal(sj, mj) {
					t.Errorf("omitzero != map default-fill (JSON): struct=%s map=%s", sj, mj)
				}
			}
		})
	}
}

// assertTwinWire requires s and its directly-parsed twin to encode in to the
// same bytes, the oracle-independent anchor for every cache/splice cell. Equal
// wire proves the two *are* the same logical schema, so a metadata divergence
// is provably a metadata bug. It returns s's wire for callers that go on to
// decode it.
func assertTwinWire(t *testing.T, s, twin *avro.Schema, in any) []byte {
	t.Helper()
	wire, wireTwin := avrotest.MustEncode(t, s, in), avrotest.MustEncode(t, twin, in)
	if !bytes.Equal(wire, wireTwin) {
		t.Errorf("wire bytes diverge from directly-parsed twin: %x vs %x", wire, wireTwin)
	}
	return wire
}
