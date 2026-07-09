package avro_test

// Internal re-parse surfaces vs user lax names.
//
// Two sites re-parse LIBRARY-PRODUCED schema text: Resolve builds a
// custom-free writer view from writer.full for resolved DecodeJSON
// (resolve.go), and SchemaCache.Parse rebuilds the metadata forms from the
// self-contained spliced JSON (cache.go). Both re-parses once used
// WithLaxNames(nil), assuming it subsumes any user lax validator — false
// for empty name components (ns "a..b"), the only class lax(nil) rejects
// that a user fn can accept. The original parse already validated those
// names under the user's chosen validator; the internal validator has no
// safety role, so both sites now parse with an accept-everything name
// validator (internalReparseNames). These pins lock the two finding
// shapes; TestMatrix_InternalReparseLaxNames covers the class.

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

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

// The lax-view finding, site 1 (resolve.go's custom-free writer view).
// A custom-typed writer parsed with a user WithLaxNames fn that accepts
// empty name components (ns "a..b") is already-parsed, wire-valid text;
// Resolve re-parses writer.full to build the custom-free view and must
// not re-litigate the names. Pre-fix the WithLaxNames(nil) re-parse
// rejected the empty component and Resolve HARD-FAILED ("building
// custom-free writer view for resolved JSON decode: invalid record
// namespace \"a..b\": name must be non-empty"), blocking binary
// resolution too, while the no-custom control resolved. The reader
// differs from the writer so Resolve's canonical fast path (return
// reader as-is) cannot mask the view construction.
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

	// The writer's names pass through verbatim: parity with the no-custom
	// twin on wire bytes and fingerprint (the custom is decode-only; the
	// canonical form ignores custom registrations either way).
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
	// DecodeJSON (the path the custom-free view exists for) agree, the
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
// transitive form: parse-1 defines a..b.Inner under a user lax fn;
// parse-2 and parse-3 pass NO lax option and reference the name only
// through the cache. Pre-fix both rebuild attempts rejected the spliced
// form (strict, then WithLaxNames(nil) on the empty component), so the
// metadata forms silently degraded to a dangling reference:
// String()/Canonical() were unresolvable under ANY opts. Post-fix the
// spliced self-contained forms survive, re-parse under the user's
// validator, and match the directly-parsed twin byte-for-byte on
// canonical form, fingerprint, and wire bytes.
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

	// The wire path was correct all along — control, not the finding.
	in := map[string]any{"w": map[string]any{"i": map[string]any{"f": int64(7)}}}
	wire, err := s3.Encode(in)
	if err != nil {
		t.Fatalf("cache-parsed encode: %v", err)
	}

	// String() must be self-contained: standalone re-parse under the
	// user's validator. Pre-fix: unknown type "ok.Wrapper".
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

// The reader-side twin of the customBaked writer-trigger fix: resolved
// decode DROPPED the reader's custom on SchemaCache-inherited subtrees.
// resolveNode re-applies reader customs to REBUILT nodes through
// resolveCtx.custom (= reader.custom), and a cache parse's overlay had no
// entries for inherited nodes (applyCustomTypes visits only newly built
// nodes; the guard-satisfying form registers the custom on BOTH parses,
// but the referencing parse wires nothing new) — so a resolution against
// a pre-evolution writer silently returned raw values where the direct
// decode returned the custom-wrapped ones. tryAssignNamedRef now
// completes the overlay for cross-parse inherited subtrees
// (overlayInheritedCustom, the pure-wiring half of applyCustomTypes).
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

	// The parity the finding broke: resolved decode must equal the direct
	// decode — same value, same type.
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

// Control pinned as SAFE (probed during the 2026-07-08 round): a
// bare-reference-as-whole-schema cache parse keeps the defining parse's
// custom behavior — the composed ser/deser of the inherited named type
// carry the callback wraps — both on direct decode and as a
// custom-typed writer through Resolve (customBaked fires via the
// inherited hadCustomType stamp) and resolved DecodeJSON.
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
	if _, err := bare.Decode(wire, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
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
// re-parse validator: collectTreeDefs / inlineTreeDefs guarded named-type
// definitions with `name != ""`, conflating "no name key" (an unnamed
// node — array, map, field) with "name key present and empty" (a
// definition a user lax validator accepted). An empty short name with a
// namespace has fullname "ok." — dotted, hence referenceable across cache
// parses ({"type":"ok."} resolves by exact lookup) — but its definition
// was never collected into the cache's def table, so the splice never
// fired and the metadata forms silently degraded to the dangling
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
	wire, err := s2.Encode(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	wireTwin, err := twin.Encode(in)
	if err != nil {
		t.Fatalf("twin encode: %v", err)
	}
	if !bytes.Equal(wire, wireTwin) {
		t.Errorf("wire bytes diverge from directly-parsed twin: %x vs %x", wire, wireTwin)
	}
}
