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
	"encoding/hex"
	"fmt"
	"reflect"
	"slices"
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

// Reader-side cells for the overlay-completion fix: a cache-parsed READER
// whose custom matches only SchemaCache-inherited subtrees must apply the
// custom on RESOLVED decode exactly as on direct decode. Every cell
// asserts resolved == direct on value AND type (both against an explicit
// custom-wrapped want), resolved DecodeJSON agreement, and that the
// no-custom twin's wire bytes are unchanged by the custom registration.
// The evolution axis lives INSIDE the inherited subtree and picks the
// three resolve-time custom re-application families: added-field (a
// custom-matched long default filled through defaultOp's wrap), promotion
// (int→long through the promoted-node wrap + suppression gate), and
// reorder (record rebuild + direct-reuse wrap).
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
				// Direct decode — the parity target.
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

	// RULED (re-adjudicated on executed evidence — NOT_BUGS #60): the bare
	// empty-name root EMITS "name":"" in canonical form, matching fastavro
	// (1.12.2, executed), the only other implementation known to parse the
	// shape. The previous omission emitted a missing-name spelling that
	// fingerprinted like nothing else; canonical bytes and the Rabin
	// fingerprint are pinned against fastavro's EXECUTED values, and the
	// canonical form re-parses under the user's accept-all validator
	// (missing name and empty name are the same fullname "" there, so the
	// re-parse held before and after — the BYTES are the discriminator).
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

// fastavroRabinBytes converts a fastavro-printed CRC-64-AVRO fingerprint
// (big-endian hex, as fastavro.schema.fingerprint prints) to the
// little-endian byte order Schema.Fingerprint(NewRabin()) returns, so
// pins compare BYTES rather than presentation.
func fastavroRabinBytes(t *testing.T, beHex string) []byte {
	t.Helper()
	b, err := hex.DecodeString(beHex)
	if err != nil {
		t.Fatalf("bad hex %q: %v", beHex, err)
	}
	slices.Reverse(b)
	return b
}

// Canonical-form parity for the empty-name classes against EXECUTED
// fastavro 1.12.2 (2026-07-09): every twmb Canonical() must byte-match
// fastavro's PCF and Rabin fingerprint for {class} × {position}, and
// re-parse under the user's accept-all validator. The reference-position
// bare cell is a DOCUMENTED DIVERGENCE: twmb structurally rejects the ""
// reference spelling upstream of any validator, while fastavro accepts it
// (its PCF keeps the bare "" ref; rabin f9afa0dabf6cd566) — pinned as
// twmb's rejection. Forward references are twmb-only territory (fastavro
// rejects ALL forward references — executed: UnknownType), so the fwd-ref
// cell pins twmb's Java-rule first-occurrence form with no fastavro
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
		rabinBEHx string // fastavro's executed fingerprint (big-endian hex)
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
			if got, want := s.Fingerprint(avro.NewRabin()), fastavroRabinBytes(t, c.rabinBEHx); !bytes.Equal(got, want) {
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

	// Documented divergence: the "" reference spelling. twmb rejects it
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

	// Forward reference to the empty-named type: twmb-only (fastavro
	// rejects every forward reference — executed: UnknownType "ok.").
	// Java's first-occurrence rule: the full body is emitted at the FIRST
	// walk occurrence (the referencing field), a bare fullname afterward.
	t.Run("fwdref/ok", func(t *testing.T) {
		s, err := avro.Parse(`{"type":"record","name":"Top","fields":[{"name":"b","type":"ok."},{"name":"a","type":`+okDef+`}]}`, avro.WithLaxNames(acceptAll))
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		want := `{"name":"Top","type":"record","fields":[{"name":"b","type":{"name":"ok.","type":"record","fields":[{"name":"f","type":"long"}]}},{"name":"a","type":"ok."}]}`
		if got := string(s.Canonical()); got != want {
			t.Errorf("fwd-ref first-occurrence canonical:\n got %s\nwant %s", got, want)
		}
		if _, err := avro.Parse(string(s.Canonical()), avro.WithLaxNames(acceptAll)); err != nil {
			t.Fatalf("canonical re-parse: %v", err)
		}
	})
}
