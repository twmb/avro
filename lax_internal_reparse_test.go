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

// Siblings of the canonical empty-name emission fix, in the metadata
// rebuild: toJSONWalk (SchemaNode.Schema()) guarded its name-key,
// namespace, cycle-reference, and dedup arms with Name != "", and
// nsForChildren/collectNamedTypes/nodeFromJSONObject used the same idiom
// — all conflating "structurally unnamed node" (array/map) with "named
// kind whose short name is empty". Reachable damage through parsed
// schemas: the "ok." class rebuilt to the WRONG schema silently (name +
// namespace dropped, fullname "ok." became ""); recursive and diamond
// "ok." shapes hard-failed the rebuilt re-parse ("unknown type"); a named
// child inside an empty-named parent lost its inherited scope. The named
// KIND, or a non-empty fullname where a reference must exist, is the
// distinction — mirroring the canonical emitter fix.
func TestRegression_SchemaNodeRebuildEmptyNames(t *testing.T) {
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

// AUDIT_PATTERNS.md B7 second instance, the stale-splice arm. A KEYLESS
// definition — no "name" key at all, parseable only under a user
// WithLaxNames fn accepting "" — registers the parser fullname "x." for
// {"type":"record","namespace":"x",...} and builds its children under x,
// so the nested Inner definition is x.Inner. collectTreeDefs gated both
// the def visit and the child namespace scope on a string "name" KEY
// being present, so parse-1's nested definition was misfiled in
// SchemaCache.defs under the ENCLOSING-scoped fullname "Inner". A later
// same-cache parse that references the short name "Inner" BEFORE locally
// defining a DIFFERENT Inner is bound by the parser to the LOCAL later
// definition (the cache's named table holds only x-scoped keys, so the
// reference is a forward reference — eager, positional), and the wire
// codec implements Inner{z:string}; the splice walker instead found the
// misfiled stale def, inlined Inner{w:long} at the reference, and
// rewrote the local definition to a reference (dupDefRef), shipping
// String()/Root()/Canonical() that describe a field the wire rejects.
// Post-fix the defs table holds only parser-scoped fullnames, nothing
// splices, and every metadata form equals the directly-parsed twin's.
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
	// implements). Pre-fix the splice inverted this: field a carried the
	// stale inherited Inner{w:long} definition and field b was rewritten
	// to a reference.
	root := writer.Root()
	if got := root.Fields[0].Type.Type; got != "Inner" {
		t.Errorf("Root() field a: got type %q, want the bare reference %q", got, "Inner")
	}
	fb := root.Fields[1].Type
	if fb.Type != "record" || len(fb.Fields) != 1 || fb.Fields[0].Name != "z" || fb.Fields[0].Type.Type != "string" {
		t.Errorf("Root() field b: got %s %v, want the local record definition with the single string field z", fb.Type, fb.Fields)
	}
	// Wire controls (correct before and after the fix): the codec
	// implements the LOCAL Inner{z:string} at both fields and rejects the
	// stale inherited shape.
	in := map[string]any{"a": map[string]any{"z": "p"}, "b": map[string]any{"z": "q"}}
	wire, err := writer.Encode(in)
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
	var out map[string]any
	if _, err := writer.Decode(wire, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Errorf("decode round-trip: got %#v, want %#v", out, in)
	}
	if _, err := writer.Encode(map[string]any{"a": map[string]any{"w": int64(7)}, "b": map[string]any{"w": int64(8)}}); err == nil {
		t.Error("wire unexpectedly accepts the stale inherited Inner{w:long} shape")
	}
}

// AUDIT_PATTERNS.md B7 second instance, the cross-parse dangle arm
// (B9's coherent-degrade shape). Parse-1 (lax) defines x.Inner nested
// inside a keyless record; parse-2 — no lax option, transitive
// reachability is the point — references the parser-scoped fullname
// "x.Inner", which the wire resolves from the cache's named table.
// Pre-fix the definition sat misfiled in SchemaCache.defs under "Inner",
// so the exact dotted lookup found nothing to splice and
// String()/Canonical() kept the dangling reference (unresolvable under
// any opts). Post-fix the splice fires and the metadata forms are
// self-contained, strict-parseable (every spliced name here is
// strict-valid), and byte-equal to the directly-parsed twin's.
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
	wire, err := writer.Encode(in)
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
	var out map[string]any
	if _, err := writer.Decode(wire, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Errorf("decode round-trip: got %#v, want %#v", out, in)
	}
}

// AUDIT_PATTERNS.md B7 third instance, arm one: parser self-consistency
// for leading-dot names. One leading dot (and no other dot) is the
// explicit null-namespace escape — the rule qualifyAliases already
// applies to aliases and Java's Name constructor applies to every name
// (Schema.java ~1455: lastDot split; `if ("".equals(space)) space =
// null`) — so {"name":".x"} builds as name "x" in the null namespace
// (lax-only: the empty leading component never passes strict
// validation). Pre-fix the name registered VERBATIM as ".x" and child
// registration prefixed parentName[:dot+1] (nested Inner registered
// ".Inner") while reference resolution used namespaceOf(".x") = "" —
// the parser disagreed with itself, and a bare sibling reference
// inside ".x" failed to parse: unknown type "Inner". Post-fix children
// build in the null namespace and the bare reference binds.
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

// AUDIT_PATTERNS.md B7 third instance, arm two: a cross-parse ".x"
// reference splices self-contained. Parse-1 defines {"name":".x"}
// (lax); the def collectors already stored it under the collapsed
// fullname "x" (nodeFullnameTree's split-rejoin implements exactly the
// Name-ctor rule), but pre-fix the parser registered ".x" verbatim and
// scopedRefKeys looked the reference up verbatim, so the exact dotted
// lookup missed the def table and String()/Canonical() kept the
// dangling reference. Post-fix definition and reference both normalize
// to "x", the splice fires, and the spliced form (name "x", explicit
// null namespace) is strict-parseable.
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
	wire, err := writer.Encode(in)
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

// AUDIT_PATTERNS.md B7 third instance, arm three: the executed
// stale-splice divergence heals. Pre-fix, parse-1's {"name":".x"}
// registered ".x" in the parser but "x" in the def table, so a later
// parse that references-then-locally-defines the bare "x" parsed (no
// name conflict — ".x" != "x" in the parser's table), forward-bound
// the reference to the LOCAL x{z:string}, and then spliced the STALE
// misfiled def at the reference: canonical described x{w:long} while
// the wire accepted {z:string} and rejected {w:long}. Post-fix ".x"
// IS the fullname "x", so the local re-definition is a DUPLICATE of
// the cache-inherited name and the parse is rejected outright — the
// same verdict every other same-fullname redefinition gets.
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
