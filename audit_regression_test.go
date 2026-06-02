package avro_test

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

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

// A bare forward reference whose short name is shared across namespaces must
// still resolve to its in-scope fullname in the canonical form — emitting the
// full body at the first walk occurrence and the fullname at later ones —
// matching Java SchemaNormalization. The previous canonical resolver only
// upgraded a bare reference to a fullname when the short name was GLOBALLY
// unique, so an ambiguous short name leaked through verbatim ("Inner"),
// diverging the PCF / fingerprint / single-object-encoding header from Java.
// The bug reached every forward-ref container (field, array, map, union); one
// lexical-resolution fix covers all four. The fix is canonical-only: the
// human-readable String() still preserves the short name as written.
func TestRegression_CanonicalForwardRefAmbiguousShortName(t *testing.T) {
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
			// Idempotency: re-parsing the canonical form and re-canonicalizing
			// is a fixed point (the emitted fullnames re-resolve identically).
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
	// NOT a.T — even though a.T also exists. Pins that the resolution
	// namespace switches when descending into a type with its own namespace.
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

	// The fix is canonical-only: String() preserves the short reference as
	// written (it is re-parseable in the enclosing namespace).
	field := avro.MustParse(`{"type":"record","name":"R","namespace":"a","fields":[{"name":"f1","type":"Inner"}` + defs)
	if str := field.String(); !strings.Contains(str, `{"name":"f1","type":"Inner"}`) {
		t.Fatalf("String() should preserve the bare short reference, got %s", str)
	}
}

// A union default must select the SAME branch on the JSON wire as on the binary
// wire. When the default string has a codepoint > 255 it cannot be a bytes or
// fixed default (a bytes/fixed JSON default maps each codepoint 0-255 to one
// byte), so binary correctly falls through to the string branch. EncodeJSON
// previously tested branch acceptance by encoding the string as raw UTF-8 via
// appendAvroJSON, which accepts it as bytes — picking a different branch than
// binary, Decode/DecodeJSON-fill, and the metadata API. The in-range case must
// still pick the bytes branch on both wires.
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
			s, err := avro.Parse(c.schema)
			if err != nil {
				t.Fatal(err)
			}
			bw, err := s.AppendEncode(nil, map[string]any{})
			if err != nil {
				t.Fatalf("binary encode: %v", err)
			}
			var bm map[string]any
			if _, err := s.Decode(bw, &bm); err != nil {
				t.Fatalf("binary decode: %v", err)
			}
			jw, err := s.AppendEncodeJSON(nil, map[string]any{})
			if err != nil {
				t.Fatalf("json encode: %v", err)
			}
			var jm map[string]any
			if err := s.DecodeJSON(jw, &jm); err != nil {
				t.Fatalf("json decode: %v", err)
			}
			if !reflect.DeepEqual(bm["f"], c.want) {
				t.Errorf("binary default-fill f = %T(%v), want %T(%v)", bm["f"], bm["f"], c.want, c.want)
			}
			if !reflect.DeepEqual(jm["f"], c.want) {
				t.Errorf("JSON default-fill f = %T(%v), want %T(%v) — JSON picked a different union branch than binary (wire %s)", jm["f"], jm["f"], c.want, c.want, jw)
			}
		})
	}
}

// Resolve must agree with CheckCompatibility. The parsing canonical form strips
// decimal precision/scale, so decimal(10,2) and decimal(10,3) are
// canonical-equal; Resolve's canonical-equal fast path therefore accepted a
// pair that CheckCompatibility rejects and silently rescaled the decoded value
// (3.14 -> 0.314). Non-decimal logical mismatches (e.g. long -> timestamp) are
// also canonical-equal but CheckCompatibility allows them (reader-logical
// wins), so those must keep resolving.
func TestRegression_ResolveHonorsDecimalCompatibility(t *testing.T) {
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

// Resolve now runs CheckCompatibility before its canonical-equal fast path, so a
// schema must be compatible with ITSELF for every shape — otherwise a Resolve
// that previously short-circuited through the fast path now fails. This pins
// CheckCompatibility(s,s)==nil and Resolve(s,s) success across the schema zoo,
// including recursion, mutual recursion, forward references, defaultless enums,
// and logical types (the shapes most likely to trip a compatibility walker).
func TestRegression_ResolveSelfCompatAllShapes(t *testing.T) {
	schemas := []string{
		`"null"`, `"boolean"`, `"int"`, `"long"`, `"float"`, `"double"`, `"bytes"`, `"string"`,
		`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`,
		`{"type":"string","logicalType":"uuid"}`,
		`{"type":"int","logicalType":"date"}`,
		`{"type":"long","logicalType":"timestamp-micros"}`,
		`{"type":"fixed","name":"Dur","size":12,"logicalType":"duration"}`,
		`{"type":"enum","name":"E","symbols":["A","B","C"]}`,                 // no default
		`{"type":"enum","name":"E","symbols":["A","B","C"],"default":"A"}`,   // with default
		`{"type":"fixed","name":"F","size":16}`,
		`{"type":"array","items":"int"}`,
		`{"type":"map","values":"long"}`,
		`["null","int"]`, `["int","null"]`, `["null","string","long","bytes"]`,
		`{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`,                                  // required field
		`{"type":"record","name":"R","fields":[{"name":"a","type":"int","default":7}]}`,                      // defaulted field
		`{"type":"record","name":"R","fields":[{"name":"a","type":"int","aliases":["b"]}]}`,                  // field alias
		`{"type":"record","name":"R","fields":[{"name":"u","type":["null",{"type":"record","name":"S","fields":[{"name":"x","type":"int"}]}]}]}`, // record in union
		`{"type":"array","items":{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}}`,         // array of records
		`{"type":"map","values":["null","string"]}`,                                                         // map of unions
		`{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]}]}`,                  // recursive
		`{"type":"record","name":"A","fields":[{"name":"b","type":{"type":"record","name":"B","fields":[{"name":"a","type":["null","A"]}]}}]}`, // mutually recursive
		`{"type":"record","name":"R","fields":[{"name":"f1","type":"Inner"},{"name":"f2","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"}]}}]}`, // forward ref
	}
	for _, sc := range schemas {
		t.Run(sc, func(t *testing.T) {
			s, err := avro.Parse(sc)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if err := avro.CheckCompatibility(s, s); err != nil {
				t.Errorf("CheckCompatibility(s,s) rejected a schema as incompatible with itself: %v", err)
			}
			if _, err := avro.Resolve(s, s); err != nil {
				t.Errorf("Resolve(s,s) failed: %v", err)
			}
		})
	}
}

// An empty Avro array decodes to a non-nil empty slice on BOTH wire formats —
// matching the JSON array decoder and the binary map decoder. Binary array
// decode previously left the target slice nil (while JSON allocated a non-nil
// empty slice and binary map decode produced a non-nil empty map), so the same
// logical value had a different Go representation depending on the wire format.
func TestRegression_EmptyArrayDecodesNonNilBothFormats(t *testing.T) {
	type Rec struct {
		Items []int `avro:"items"`
	}
	s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"items","type":{"type":"array","items":"int"}}]}`)

	bin, err := s.AppendEncode(nil, Rec{Items: nil})
	if err != nil {
		t.Fatal(err)
	}
	js, err := s.AppendEncodeJSON(nil, Rec{Items: nil})
	if err != nil {
		t.Fatal(err)
	}

	var bo, jo Rec
	if _, err := s.Decode(bin, &bo); err != nil {
		t.Fatal(err)
	}
	if err := s.DecodeJSON(js, &jo); err != nil {
		t.Fatal(err)
	}
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
	tw, err := st.AppendEncode(nil, []int{})
	if err != nil {
		t.Fatal(err)
	}
	var top []int
	if _, err := st.Decode(tw, &top); err != nil {
		t.Fatal(err)
	}
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
	mw, err := ms.AppendEncode(nil, Multi{})
	if err != nil {
		t.Fatal(err)
	}
	var mo Multi
	if _, err := ms.Decode(mw, &mo); err != nil {
		t.Fatal(err)
	}
	if mo.Strs == nil || mo.Recs == nil {
		t.Errorf("unsafe array fields left nil: strs nil=%v recs nil=%v", mo.Strs == nil, mo.Recs == nil)
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

// CustomType.Decode must receive the RAW Avro-native value (int32 for int,
// int64 for long, []byte for bytes/fixed) — the contract documented on the
// CustomType.Decode field. The binary path enforces this by suppressing the
// logical deserializer when a custom type matches, so the raw value reaches
// the callback; the JSON path must produce the same raw value rather than the
// logical-transformed Go type (time.Time / time.Duration / *big.Rat).
// Without parity, a custom decoder that works through Decode panics or
// misreads through DecodeJSON.
func TestRegression_CustomDecodeReceivesRawValueBinaryJSONParity(t *testing.T) {
	cases := []struct {
		name     string
		logical  string
		avroType string
		schema   string
		encode   any    // Encode=nil → built-in logical encoder handles this
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
			s, err := avro.Parse(c.schema, ct)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			bin, err := s.Encode(c.encode)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			jsn, err := s.EncodeJSON(c.encode)
			if err != nil {
				t.Fatalf("EncodeJSON: %v", err)
			}
			var binVal, jsonVal any
			if _, err := s.Decode(bin, &binVal); err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if err := s.DecodeJSON(jsn, &jsonVal); err != nil {
				t.Fatalf("DecodeJSON: %v", err)
			}
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
// Decode wrapper, so handing the logical-transformed value (time.Time) where
// the raw int64 is expected panics on otherwise-valid input. Round-trip
// through DecodeJSON must not panic.
func TestRegression_CustomDecodeNewCustomTypeJSONNoPanic(t *testing.T) {
	type eventTime time.Time
	ct := avro.NewCustomType[eventTime, int64]("timestamp-millis",
		func(e eventTime, _ *avro.SchemaNode) (int64, error) { return time.Time(e).UnixMilli(), nil },
		func(ms int64, _ *avro.SchemaNode) (eventTime, error) { return eventTime(time.UnixMilli(ms)), nil })
	s := avro.MustParse(`{"type":"long","logicalType":"timestamp-millis"}`, ct)
	ev := eventTime(time.UnixMilli(1700000000000))
	jsn, err := s.EncodeJSON(ev)
	if err != nil {
		t.Fatalf("EncodeJSON: %v", err)
	}
	var out eventTime
	if err := s.DecodeJSON(jsn, &out); err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	if !time.Time(out).Equal(time.Time(ev)) {
		t.Errorf("round-trip mismatch: got %v want %v", time.Time(out), time.Time(ev))
	}
}

// A non-custom decimal target still accepts the bare-number JSON convenience
// form (decode-only leniency). The raw-value suppression for custom decoders
// must not disable the bare-number arm for ordinary decimal decode.
func TestRegression_DecimalBareNumberStillAcceptedNonCustom(t *testing.T) {
	s := avro.MustParse(`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`)
	var r *big.Rat
	if err := s.DecodeJSON([]byte("0.33"), &r); err != nil {
		t.Fatalf("DecodeJSON bare number into non-custom decimal: %v", err)
	}
	if r == nil || r.Cmp(big.NewRat(33, 100)) != 0 {
		t.Errorf("got %v, want 33/100", r)
	}
}

// A CustomType whose GoType is a pointer (e.g. *url.URL, the documented
// pointer-GoType shape) must fire its Encode on BOTH binary and JSON. The
// binary path checks GoType per indirection level while peeling; the JSON
// path must consult the custom hook before stripping the pointer, or the
// pointer GoType never matches and the encoder is silently skipped.
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
		if _, err := s.Decode(bin, &binVal); err != nil {
			t.Fatal(err)
		}
		if err := s.DecodeJSON(jsn, &jsonVal); err != nil {
			t.Fatal(err)
		}
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

// Parsing Canonical Form requires names / namespaces / enum symbols be
// rendered as raw UTF-8 (the STRINGS rule): Java's SchemaNormalization
// appends them verbatim. The characters < > & are reachable in names via the
// public WithLaxNames option (Java's parallel is NameValidator.NO_VALIDATION);
// they must NOT appear as \u00XX escapes or the Rabin/SHA/MD5 fingerprint and
// the Single Object Encoding header diverge from every other Avro impl.
func TestRegression_CanonicalRawUTF8ForHTMLChars(t *testing.T) {
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
			s, err := avro.Parse(c.schema, avro.WithLaxNames(nil))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			got := string(s.Canonical())
			if got != c.want {
				t.Errorf("canonical mismatch:\n got = %s\nwant = %s", got, c.want)
			}
			// The fingerprint must hash the raw-UTF-8 bytes — i.e. equal the
			// Rabin of the want form, which is what Java/fastavro produce. The
			// exact got == want check above already proves the raw chars are
			// present; this guards the cross-impl fingerprint contract.
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

	// Boundary: control characters (< 0x20) stay JSON-escaped — only < > &
	// are un-escaped. twmb emits valid JSON (Java would emit the raw control
	// byte, producing invalid JSON); the un-escape must not over-reach.
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

// The unsafe struct-field path and the reflect path must produce the same
// SemanticError for the same overflow: the GoType field must name the Go field
// type (not be nil), so an overflow surfaced through an addressable struct
// reads identically to the same value encoded/decoded standalone.
func TestRegression_UnsafeOverflowErrorCarriesGoType(t *testing.T) {
	semGoType := func(t *testing.T, err error) reflect.Type {
		t.Helper()
		var se *avro.SemanticError
		if !errors.As(err, &se) {
			t.Fatalf("error is not *SemanticError: %T (%v)", err, err)
		}
		return se.GoType
	}

	t.Run("encode-int-overflow", func(t *testing.T) {
		type R struct {
			X int64 `avro:"x"`
		}
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`)
		r := R{X: math.MaxInt32 + 1}
		_, ue := s.Encode(&r) // addressable → unsafe fast path
		_, re := s.Encode(r)  // non-addressable → reflect path
		if ue == nil || re == nil {
			t.Fatalf("both paths must reject overflow: unsafe=%v reflect=%v", ue, re)
		}
		if got := semGoType(t, ue); got != reflect.TypeOf(int64(0)) {
			t.Errorf("unsafe encode error GoType = %v, want int64", got)
		}
		if !strings.Contains(ue.Error(), "int64") {
			t.Errorf("unsafe encode error omits Go type name: %q", ue.Error())
		}
	})

	t.Run("decode-int-overflow", func(t *testing.T) {
		type R struct {
			X int8 `avro:"x"`
		}
		recS := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"x","type":"int"}]}`)
		intS := avro.MustParse(`"int"`)
		wire, err := recS.Encode(map[string]any{"x": int32(300)})
		if err != nil {
			t.Fatal(err)
		}
		var r R
		_, ue := recS.Decode(wire, &r) // unsafe struct-field path
		iwire, _ := intS.Encode(int32(300))
		var i8 int8
		_, re := intS.Decode(iwire, &i8) // reflect path
		if ue == nil || re == nil {
			t.Fatalf("both paths must reject overflow: unsafe=%v reflect=%v", ue, re)
		}
		if got := semGoType(t, ue); got != reflect.TypeOf(int8(0)) {
			t.Errorf("unsafe decode error GoType = %v, want int8", got)
		}
	})

	t.Run("decode-double-to-float32-overflow", func(t *testing.T) {
		type R struct {
			X float32 `avro:"x"`
		}
		recS := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"x","type":"double"}]}`)
		wire, err := recS.Encode(map[string]any{"x": 1e300})
		if err != nil {
			t.Fatal(err)
		}
		var r R
		_, ue := recS.Decode(wire, &r)
		if ue == nil {
			t.Fatal("double 1e300 into float32 field must reject")
		}
		if got := semGoType(t, ue); got != reflect.TypeOf(float32(0)) {
			t.Errorf("unsafe decode error GoType = %v, want float32", got)
		}
	})
}

// A custom type with a nil Decode callback suppresses the built-in logical
// decoder and produces the RAW Avro-native value (documented on
// CustomType.Decode and doc.go). The binary path enforces this by suppressing
// the logical deserializer whenever any matching custom type exists; the JSON
// path must produce the same raw value rather than the logical-transformed Go
// type, even though there is no Decode chain to wrap.
func TestRegression_CustomDecodeNilRawValueBinaryJSONParity(t *testing.T) {
	type w struct{ N int64 }
	cases := []struct {
		name     string
		schema   string
		logical  string
		avroType string
		wantType string
		encVal   any // value the built-in logical encoder accepts
	}{
		// EVERY logical type — this is the drift-guard for jsonDecodeAppliesLogical
		// (now derived by probing decodeLogical*). An Encode-only non-wildcard
		// custom suppresses the logical decoder, so decode-into-any must yield
		// the RAW Avro-native type on BOTH paths; if the probe wrongly reports a
		// logical as non-transforming, JSON would leak the enriched type here.
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
			// Encode via a plain (no-custom) schema using the built-in logical
			// encoder, then decode via an Encode-only custom schema.
			plain := avro.MustParse(c.schema)
			bin, err := plain.Encode(c.encVal)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			jsn, err := plain.EncodeJSON(c.encVal)
			if err != nil {
				t.Fatalf("EncodeJSON: %v", err)
			}
			// Encode-only custom (Decode==nil) suppresses the logical decoder
			// → raw Avro-native value on BOTH decode paths. The Encode callback
			// is never invoked here (we only decode); its presence is what
			// makes the type Encode-only.
			custom := avro.MustParse(c.schema, avro.CustomType{
				LogicalType: c.logical, AvroType: c.avroType, GoType: reflect.TypeOf(w{}),
				Encode: func(v any, _ *avro.SchemaNode) (any, error) { return v, nil },
			})
			var bv, jv any
			if _, err := custom.Decode(bin, &bv); err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if err := custom.DecodeJSON(jsn, &jv); err != nil {
				t.Fatalf("DecodeJSON: %v", err)
			}
			if got := fmt.Sprintf("%T", bv); got != c.wantType {
				t.Errorf("binary nil-Decode produced %s, want raw %s", got, c.wantType)
			}
			if got := fmt.Sprintf("%T", jv); got != c.wantType {
				t.Errorf("JSON nil-Decode produced %s, want raw %s (binary↔JSON parity)", got, c.wantType)
			}
		})
	}
}

// A custom encoder with a pointer GoType registered on a UNION BRANCH must
// fire on both binary and JSON encode. The binary path passes the un-peeled
// value to the branch serializer; the JSON path must dispatch the union before
// the pointer-peel loop so the branch's custom encoder matches the pointer.
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
// decimal serializer to base bytes on the binary path: a value matching the
// custom GoType is written as its raw []byte and a non-matching pass-through
// (e.g. *big.Rat) is rejected. JSON encode must agree on both directions.
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
			// Pass-through *big.Rat: rejected on BOTH paths (no decimal arm).
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

// The binary fixed build suppresses the serializer to base serSize for ALL
// fixed logicals (decimal, duration, uuid) when a custom Encode exists, so a
// non-matching pass-through value is written as raw bytes rather than going
// through the strict logical encoder. JSON encode must agree — e.g. a 16-char
// non-UUID string against fixed+uuid+custom must encode (raw) on both, not
// reject on JSON via parseUUID.
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

// A WILDCARD CustomType (empty LogicalType AND AvroType — the property-based
// dispatch pattern) is excluded from the binary decoder-suppression gate
// (hasMatchingCustomType), so the binary path leaves the built-in logical
// decoder in place and feeds the callback (and the user) the ENRICHED value
// (time.Time / *big.Rat / [16]byte). The JSON decode path must NOT suppress
// the logical transform for wildcards, or it feeds raw int64/[]byte instead —
// a binary↔JSON divergence. Non-wildcard customs (LogicalType-only,
// AvroType-only, or both) still suppress to the raw value on both paths.
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
		bin, err := s.Encode(enc)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		js, err := s.EncodeJSON(enc)
		if err != nil {
			t.Fatalf("EncodeJSON: %v", err)
		}
		var bo, jo any
		if _, err := s.Decode(bin, &bo); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if err := s.DecodeJSON(js, &jo); err != nil {
			t.Fatalf("DecodeJSON: %v", err)
		}
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
			// Wildcard: enriched on BOTH paths.
			ws := avro.MustParse(c.schema, avro.CustomType{Decode: skip})
			wb, wj := decBoth(t, ws, c.enc)
			if wb != wj {
				t.Errorf("wildcard: binary=%s json=%s (must agree)", wb, wj)
			}
			if wb != c.enriched {
				t.Errorf("wildcard binary should keep enriched %s, got %s", c.enriched, wb)
			}

			// Non-wildcard (LogicalType-only / AvroType-only / both): RAW on both.
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

// ENCODE-side mirror of the wildcard parity: the binary ENCODER-suppression
// gate (hasMatchingCustomTypeWithEncode) also EXCLUDES wildcards, so a wildcard
// CustomType with an Encode keeps the built-in decimal/fixed serializer (which
// accepts *big.Rat) on the binary path. The JSON encode arms must gate on the
// same predicate (not the custom[node].encode != nil proxy) so a wildcard keeps
// the logical arm on JSON too — otherwise binary accepts *big.Rat while JSON
// rejects it. Non-wildcard Encode customs still suppress to base bytes on both.
func TestRegression_WildcardCustomEncodeBinaryJSONParity(t *testing.T) {
	skipEnc := func(v any, _ *avro.SchemaNode) (any, error) { return nil, avro.ErrSkipCustomType }
	schemas := []string{
		`{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`,
		`{"type":"fixed","name":"F","size":8,"logicalType":"decimal","precision":10,"scale":2}`,
		`{"type":"bytes","logicalType":"big-decimal"}`,
	}
	for _, sch := range schemas {
		// Wildcard with Encode: *big.Rat pass-through accepted on BOTH paths
		// (binary keeps serBytesDecimal/serFixedDecimal — wildcard excluded
		// from the encoder-suppression gate).
		ws := avro.MustParse(sch, avro.CustomType{Encode: skipEnc})
		_, web := ws.Encode(big.NewRat(33, 100))
		_, wej := ws.EncodeJSON(big.NewRat(33, 100))
		if (web == nil) != (wej == nil) {
			t.Errorf("wildcard encode %s: binary err=%v json err=%v (must agree)", sch[:34], web, wej)
		}
		if web != nil {
			t.Errorf("wildcard encode %s should ACCEPT *big.Rat (binary), got %v", sch[:34], web)
		}

		// Non-wildcard with Encode: suppressed to base bytes → reject *big.Rat
		// pass-through on BOTH paths.
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

// A wildcard CustomType's Encode callback must fire the SAME number of times on
// Encode and EncodeJSON. The binary 2-branch ["null",T] fast path
// (serNullUnionAt) skips the null branch for a non-nil value, so the wildcard
// hook fires once (on T); the JSON union try-each previously trialed the null
// branch first, firing the hook spuriously a second time. For a side-effecting
// wildcard (logging / property-based dispatch), that double-fires on JSON. N>=3
// unions trial null on BOTH paths (binary try-each), so they already agree —
// the regression is specific to 2-branch null-first unions.
func TestRegression_WildcardEncodeCallbackCountUnionParity(t *testing.T) {
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
