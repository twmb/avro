package avro_test

// Class matrix for the internal re-parse surfaces (Resolve's custom-free
// writer view, resolve.go; SchemaCache's splice rebuild, cache.go) against
// the full name class the ORIGINAL parse can accept:
//
//	{site: resolve-view, cache-splice}
//	  x {name class: strict, lax-nonempty, empty-component ns, empty-string name}
//	  x {custom: none, decode-only, encode+decode}
//	  x {reference: direct (recursive self-ref), transitive (diamond)}
//	plus a cache cell whose OUTER parse itself carries the user lax fn, and
//	pinned verdicts for the structurally-unreferenceable bare "" name.
//
// Every cell is framed as PARITY WITH THE ORIGINAL PARSE: whatever the
// public parse accepted must survive Resolve, String()/Canonical()
// re-parse, and resolved DecodeJSON, and the wire bytes and Rabin
// fingerprints must equal the no-custom / directly-parsed twin — names
// pass through verbatim (asserted, not assumed). The reader always
// differs from the writer (an added defaulted field) so Resolve's
// canonical fast path cannot mask the writer-view construction.

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/twmb/avro"
)

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
	nameOnly := func(nc reparseNameClass) []avro.SchemaOpt {
		if nc.opt != nil {
			return []avro.SchemaOpt{nc.opt}
		}
		return nil
	}
	withCustom := func(nc reparseNameClass, cm reparseCustomMode) []avro.SchemaOpt {
		return append(nameOnly(nc), cm.opts...)
	}
	L := func(wrap bool, v int64) any {
		if wrap {
			return ctLong{v}
		}
		return v
	}

	// battery runs the shared per-cell assertions. writer is the schema under
	// test (plain-parsed or cache-parsed, possibly custom-typed); twin is the
	// independent oracle: the same self-contained schema text parsed directly
	// with the name opt only. reader adds a defaulted field.
	battery := func(t *testing.T, nc reparseNameClass, writer, twin, reader *avro.Schema, in, inCt, want map[string]any) {
		t.Helper()
		// Names pass through verbatim.
		if canon := string(writer.Canonical()); !strings.Contains(canon, `"`+nc.full+`"`) {
			t.Errorf("canonical does not carry fullname %q verbatim: %s", nc.full, canon)
		}
		// String()/Canonical() re-parse self-contained under the user's
		// validator, preserving canonical identity.
		re, err := avro.Parse(writer.String(), nameOnly(nc)...)
		if err != nil {
			t.Fatalf("String() re-parse: %v\nString(): %s", err, writer.String())
		}
		if !bytes.Equal(re.Canonical(), writer.Canonical()) {
			t.Errorf("String() re-parse canonical diverges:\n re: %s\ngot: %s", re.Canonical(), writer.Canonical())
		}
		reC, err := avro.Parse(string(writer.Canonical()), nameOnly(nc)...)
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
				twin, err := avro.Parse(writerJSON, nameOnly(nc)...)
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
				twin, err := avro.Parse(writerJSON, nameOnly(nc)...)
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

	// Site: cache-splice. The lax-named type is defined in an earlier cache
	// parse and reaches the final parse's metadata only through the splice.
	// The final parse passes NO name option (transitive reachability is the
	// point); custom cells carry the custom on every parse in the chain (the
	// cross-parse custom-boundary guard requires cache and referencing parse
	// to agree). The cache-parsed writer then feeds Resolve, composing both
	// internal re-parse sites when customs are wired.
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
				twin, err := avro.Parse(twinJSON, nameOnly(nc)...)
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
				// Two wrappers referencing the same inner type: the final
				// splice inlines both wrapper definitions, and the SECOND
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
				twin, err := avro.Parse(twinJSON, nameOnly(nc)...)
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

	// Extra cell: the OUTER cache parse itself carries the user lax fn, so
	// the splice rebuild's FIRST attempt (this call's own opts) succeeds and
	// the internal retry never fires. Guards the opts passthrough.
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

// The bare empty name ("" with no namespace) is DEFINABLE under a user
// accept-all validator but not REFERENCEABLE: its only spelling as a
// reference is the empty string, which the parser rejects structurally
// ("schema is not a primitive, complex, nor union") upstream of any name
// validator. These verdicts are pinned as the original parse's behavior;
// the reference cells of the empty-name class therefore run on the
// namespaced form (fullname "ok.") in TestMatrix_InternalReparseLaxNames.
func TestMatrix_InternalReparseBareEmptyName(t *testing.T) {
	acceptAll := func(string) error { return nil }

	// Definition accepted; reference rejected — single parse.
	if _, err := avro.Parse(`{"type":"record","name":"","fields":[{"name":"f","type":"long"}]}`, avro.WithLaxNames(acceptAll)); err != nil {
		t.Fatalf("bare empty-name definition must parse under accept-all: %v", err)
	}
	dia := `{"type":"record","name":"Top","fields":[{"name":"a","type":{"type":"record","name":"","fields":[{"name":"f","type":"long"}]}},{"name":"b","type":""}]}`
	if _, err := avro.Parse(dia, avro.WithLaxNames(acceptAll)); err == nil {
		t.Error(`reference "" must be structurally rejected (in-schema)`)
	} else if !strings.Contains(err.Error(), "not a primitive") {
		t.Errorf("in-schema rejection changed shape: %v", err)
	}
	// Reference rejected — cache cross-parse.
	var c avro.SchemaCache
	if _, err := c.Parse(`{"type":"record","name":"","fields":[{"name":"f","type":"long"}]}`, avro.WithLaxNames(acceptAll)); err != nil {
		t.Fatalf("cache bare empty-name definition: %v", err)
	}
	if _, err := c.Parse(`{"type":"record","name":"Outer","fields":[{"name":"i","type":""}]}`); err == nil {
		t.Error(`reference "" must be structurally rejected (cache cross-parse)`)
	} else if !strings.Contains(err.Error(), "not a primitive") {
		t.Errorf("cache rejection changed shape: %v", err)
	}

	// A bare empty-name ROOT still survives the resolve-view re-parse: the
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

	// OBSERVED, pinned as-is (surfaced for adjudication, pre-existing and
	// untouched by the internal re-parse fix): the canonical form of a bare
	// empty-name ROOT omits the record's own "name" key entirely, so
	// Canonical() of THIS class does not re-parse. The shape only arises
	// under a user WithLaxNames fn — the default parse enforces the strict
	// name grammar (see the WithLaxNames doc) — so canonical-form interop
	// carries no expectation here; the namespaced empty name "ok." keeps
	// its name key (see the emptyname matrix cells), and the wire path,
	// String(), and Resolve are unaffected. This pin flips if canonical
	// emission for the class ever changes.
	if canon := string(writer.Canonical()); canon != `{"type":"record","fields":[{"name":"f","type":"long"}]}` {
		t.Errorf("bare empty-name canonical changed (pin flipped — re-adjudicate): %s", canon)
	}
}
