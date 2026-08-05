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

// Class matrix for KEYLESS definitions in the SchemaCache def table —
// named-kind nodes with NO "name" key at all, parseable only under a
// user WithLaxNames fn accepting "" (AUDIT_PATTERNS.md B7 second
// instance). The parser registers a fullname for them regardless
// ("ns." from the namespace attribute, or "" bare) and scopes their
// children by that namespace, so the def-collection and splice walkers
// must do the same: collectTreeDefs's visit fires with the parser's
// fullname and scopes children by nodeChildScope, and inlineTreeDefs's
// local-definition registration (map arm and flat-field arm) has no
// keyless carve-out.
//
//	{namespace attr: present "x", absent}
//	  x {parse-2 shape: cross-parse reference to the parser fullname
//	     (define-then-reference), reference-then-LOCAL-define of the
//	     nested short name, local-define-then-reference}
//	plus keyless-def-visit cells (reference to "x." itself: recursive
//	self-ref, diamond with the nested x.Inner), seen-parity cells
//	(nested and flat keyless defs arriving inside a spliced subtree),
//	and a same-string lax re-parse stability cell (dupDefRef on a
//	keyless local definition).
//
// Invariant per cell, as everywhere in this file: the metadata forms
// describe the wire codec's schema, twin-parity where a twin exists.
// The bare-namespace reference-then-define orders have NO twin: the
// parser itself rejects the parse (the cache's named table already
// holds "Inner", so the local re-definition is a duplicate) — pinned
// as the rejection.
func TestMatrix_CacheKeylessDefCollection(t *testing.T) {
	acceptAll := func(string) error { return nil }
	lax := avro.WithLaxNames(acceptAll)

	const keylessNS = `{"type":"record","namespace":"x","fields":[{"name":"i","type":{"type":"record","name":"Inner","fields":[{"name":"w","type":"long"}]}}]}`
	const keylessBare = `{"type":"record","fields":[{"name":"i","type":{"type":"record","name":"Inner","fields":[{"name":"w","type":"long"}]}}]}`

	// Cross-parse reference to the nested definition's parser-scoped
	// fullname (define-then-reference across parses). With the namespace
	// attribute present the nested def is x.Inner; pre-fix it was
	// misfiled under "Inner" and the exact dotted lookup dangled.
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
	// def in the ENCLOSING (null) namespace — the walkers agreed here
	// even pre-fix; the cell is the control for the scope rule's other
	// half. The spliced definition carries the explicit-empty namespace
	// escape and stays strict-parseable.
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
	// BEFORE locally defining a DIFFERENT Inner{z:string}. The parser
	// forward-binds the reference to the LOCAL definition (the cache's
	// named table holds only x-scoped keys); pre-fix the splice walker
	// inlined the misfiled stale Inner{w:long} instead and rewrote the
	// local definition to a reference — the stale-splice divergence.
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
	// tracking both bind the local type (the shape pre-fix behavior
	// already got right — the order dual that makes the matrix's
	// position axis non-vacuous).
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
	// re-defines it is a DUPLICATE — rejected by the parser in either
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

	// Diamond through the keyless def: parse-2 references BOTH "x." and
	// the x.Inner nested inside it. The splice at the first reference
	// carries the Inner definition; walking the spliced copy registers
	// it, so the second reference stays bare and resolves backward into
	// the first splice — one definition per name, the
	// first-define-then-reference rule the splice dedup implements (see
	// inlineTreeDefs).
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

	// Seen-parity, map arm: a KEYLESS definition arriving INSIDE a
	// spliced subtree (as-written, no name key) must register its
	// parser fullname "n." during the walk, or the later "n." reference
	// splices a SECOND copy and the duplicate-rejecting rebuild degrades
	// the metadata to the dangling original.
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
	// a FLAT field (goavro-style, no field name either — the lift
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
	// with the first parse's defs in the cache, so its splice walk sees
	// the LOCAL keyless definition after splicing the (identical)
	// inherited one at the forward reference — dupDefRef must rewrite
	// the local keyless definition to its dotted reference, keeping the
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
		// The splice route is pinned structurally, not just coherently:
		// the parser bound the forward reference at field a to the CACHED
		// type (eager), so the faithful metadata materializes the
		// definition there and field b — the local re-definition of the
		// same fullname — becomes the dotted reference (dupDefRef on a
		// keyless definition). A fallback to the as-written text would
		// also be value-coherent here (same string, identical defs) but
		// would invert the binding structure the wire used.
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

// Class matrix for LEADING-DOT names (AUDIT_PATTERNS.md B7 third
// instance). A single leading dot with no other dot is the explicit
// null-namespace escape: {"name":".x"} builds as name "x" in the null
// namespace and "." collapses to the bare empty name "" — the rule
// qualifyAliases already applies to aliases and Java's Name constructor
// applies to every name (Schema.java ~1455: lastDot split; `if
// ("".equals(space)) space = null`). Lax-only: strict parses still
// reject the empty leading component (twmb stays stricter than Java —
// documented, not widened). fastavro 1.12.2 holds a THIRD posture,
// executed 2026-07-14: it keeps ".x" VERBATIM in PCF (rabin
// c69859279c1a5fbe) and rejects the bare-"x" reference (UnknownType)
// while still scoping children in the null namespace; twmb follows
// Java's normalized identity instead, so post-fix PCF/fingerprints for
// the lax-only ".x" spelling match Java and diverge from fastavro
// (pre-fix twmb matched fastavro's bytes on definition-only shapes but
// was SELF-inconsistent on references: a bare sibling ref inside ".x"
// could not parse at all).
//
//	{".x" definition x reference spelling {"x", ".x"} x cross-parse
//	 x {pure reference, reference-then-define, define-then-reference}}
//	plus same-parse spelling-equivalence cells (both orders, both ref
//	spellings), the "." -> empty-name family cell (joins NOT_BUGS #60's
//	adjudicated behavior numerically), a multi-dot verbatim control
//	(".a.b" — all three implementations agree), and the
//	Root()/nodeFullnameTree/parser agreement cell.
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

	// Reference-then-define and define-then-reference with a LOCAL "x"
	// definition: ".x" now IS the fullname "x", so the local
	// re-definition duplicates the cache-inherited name in either order
	// and with either reference spelling — the parser's standard
	// conflict rejection, same as every other same-fullname family.
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
	// "x" spelling are one type, in both definition positions and both
	// reference directions (backward and forward). The twin is the same
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
			writer, err := avro.Parse(cell.src, lax)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			nc := reparseNameClass{"leadingdot-sameparse", "", "x", "x", lax}
			in := map[string]any{"a": map[string]any{"w": int64(7)}, "b": map[string]any{"w": int64(8)}}
			want := map[string]any{"a": map[string]any{"w": int64(7)}, "b": map[string]any{"w": int64(8)}, "added": "x"}
			runReparseBattery(t, nc, writer, cell.twin, in, want)
		})
	}

	// "." collapses into the adjudicated empty-name family (NOT_BUGS
	// #60): its canonical form and Rabin fingerprint are byte-identical
	// to the bare {"name":""} definition's — 3d741707ff4bfa45 is the
	// fastavro-EXECUTED value pinned for that family — and the type
	// stays unreferenceable in EVERY spelling: the "" reference is
	// structurally rejected (pinned in
	// TestMatrix_InternalReparseBareEmptyName) and the "." reference
	// finds nothing to bind, same-parse and cross-parse. fastavro
	// 1.12.2 keeps "." verbatim in PCF (executed 2026-07-14: rabin
	// b1eae635ed69c128) — the same verbatim-identity divergence as the
	// ".x" root, documented not adopted.
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

	// Multi-dot control: the escape is ONLY the single leading dot.
	// ".a.b" keeps its verbatim identity (namespace ".a") — Java's Name
	// ctor keeps any non-empty space, and fastavro's executed PCF
	// agrees byte-for-byte (rabin 013f503d468af517, 2026-07-14) — a
	// three-way agreement cell pinning the boundary of the rule.
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
	// parser now agree on the ".x" identity. SchemaNode preserves the
	// as-written spellings (Name ".x" on the definition, Type ".x" on
	// the reference) while every computed identity — canonical form,
	// name-ref binding, the Schema() rebuild's dedup/cycle emission —
	// resolves to the fullname "x".
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
// custom-typed); twin is the independent oracle: the same self-contained
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
// (LITTLE-endian hex — fastavro prints the single-object wire order) to the
// BIG-endian byte order Schema.Fingerprint(NewRabin()) returns, so pins
// compare BYTES rather than presentation. The 64-bit value is the same one;
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

// Tagged-union JSON naming for an EMPTY-NAMED union branch (short name ""
// with and without a namespace — reachable only under a user WithLaxNames
// fn). The tag is the branch's FULLNAME, exactly as for any other named
// branch: "ok." for the namespaced class and "" for the bare class —
// matching fastavro's json_writer (1.12.2, executed: `{"ok.": "A"}`; its
// reader resolves only that exact key, rejecting "" and the kind name).
// fastavro cannot WRITE the bare class (its writer errors "No key was set"
// on the falsy fullname) but its reader accepts the "" key, so twmb's
// `{"":"A"}` emission is fastavro-readable on both classes.
//
//	{class: bare "", namespaced "ok."}
//	  x {tagged encode emission (exact bytes),
//	     decode of own emission (plain and TaggedUnions),
//	     tagged-map encode routing (fullname key; "" short-name fallback),
//	     resolved DecodeJSON routing (per-branch-divergent resolution)}
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
			// The "" key routes via the unique-short-name fallback
			// (unqualified("ok.") is ""), the same input leniency every
			// namespaced branch's short name gets; the kind never tags a
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
	// the tagged intermediate: the writer names the enum branch whose value
	// "B" would ALSO satisfy the string branch, and the reader's enum drops
	// "B" for its declared default — so a routing flip is observable both
	// as branch identity and as the resolved value.
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
