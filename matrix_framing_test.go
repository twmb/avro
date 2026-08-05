package avro_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// Foreign-framing matrix: Avro permits several wire framings for the same
// array/map value — multiple blocks, negative-count blocks carrying a byte
// size, and non-canonical (overlong) varint counts. twmb's encoder emits
// only single-block canonical framing, so round-trip tests never exercise
// the alternatives; Java emits size-prefixed blocks when configured and
// foreign writers split large containers into many blocks. Every variant
// must decode to the same value and re-encode onto the canonical wire.
// ---------------------------------------------------------------------------

func putZigzag(dst []byte, n int64) []byte {
	u := uint64(n)<<1 ^ uint64(n>>63)
	for u >= 0x80 {
		dst = append(dst, byte(u)|0x80)
		u >>= 7
	}
	return append(dst, byte(u))
}

// putZigzagOverlong writes n as a deliberately non-canonical varint with one
// redundant continuation byte (e.g. 0x06 → 0x86 0x00).
func putZigzagOverlong(dst []byte, n int64) []byte {
	u := uint64(n)<<1 ^ uint64(n>>63)
	dst = append(dst, byte(u&0x7f)|0x80)
	u >>= 7
	for u >= 0x80 {
		dst = append(dst, byte(u)|0x80)
		u >>= 7
	}
	return append(dst, byte(u))
}

// frameVariants builds alternative wire framings for a container whose
// per-item encodings are given (for maps, each "item" is key+value).
func frameVariants(items [][]byte) map[string][]byte {
	n := int64(len(items))
	cat := func(bs [][]byte) []byte {
		var out []byte
		for _, b := range bs {
			out = append(out, b...)
		}
		return out
	}
	all := cat(items)

	variants := map[string][]byte{}

	// One block per item.
	var perItem []byte
	for _, it := range items {
		perItem = putZigzag(perItem, 1)
		perItem = append(perItem, it...)
	}
	perItem = append(perItem, 0x00)
	variants["block-per-item"] = perItem

	// Split: first item alone, remainder together.
	if n >= 2 {
		var split []byte
		split = putZigzag(split, 1)
		split = append(split, items[0]...)
		split = putZigzag(split, n-1)
		split = append(split, cat(items[1:])...)
		split = append(split, 0x00)
		variants["split-1-rest"] = split
	}

	// Negative count with byte size (the size-prefixed form).
	var sized []byte
	sized = putZigzag(sized, -n)
	sized = putZigzag(sized, int64(len(all)))
	sized = append(sized, all...)
	sized = append(sized, 0x00)
	variants["size-prefixed"] = sized

	// Size-prefixed, one block per item.
	var sizedPer []byte
	for _, it := range items {
		sizedPer = putZigzag(sizedPer, -1)
		sizedPer = putZigzag(sizedPer, int64(len(it)))
		sizedPer = append(sizedPer, it...)
	}
	sizedPer = append(sizedPer, 0x00)
	variants["size-prefixed-per-item"] = sizedPer

	// Canonical count written as an overlong varint.
	var over []byte
	over = putZigzagOverlong(over, n)
	over = append(over, all...)
	over = append(over, 0x00)
	variants["overlong-count"] = over

	return variants
}

func TestMatrix_ForeignContainerFraming(t *testing.T) {
	for _, fr := range matFrags() {
		t.Run(fr.label, func(t *testing.T) {
			u := &uniq{}
			itemSchemaJSON := fr.schema(u)
			itemSchema := avro.MustParse(itemSchemaJSON)
			v := fr.values[0]

			// Standalone per-item encodings.
			item, err := itemSchema.AppendEncode(nil, v)
			if err != nil {
				t.Fatalf("item encode: %v", err)
			}
			items := [][]byte{item, item, item}

			// ---- array ----
			u2 := &uniq{}
			arrSchema := avro.MustParse(fmt.Sprintf(`{"type":"array","items":%s}`, fr.schema(u2)))
			canonicalWire, err := arrSchema.AppendEncode(nil, []any{v, v, v})
			if err != nil {
				t.Fatalf("array encode: %v", err)
			}
			var want any
			if _, err := arrSchema.Decode(canonicalWire, &want); err != nil {
				t.Fatalf("canonical array decode: %v", err)
			}
			for name, wire := range frameVariants(items) {
				var got any
				rest, err := arrSchema.Decode(wire, &got)
				if err != nil || len(rest) != 0 {
					t.Fatalf("array %s decode: err=%v rest=%d\nwire=%x", name, err, len(rest), wire)
				}
				if !matEqual(got, want) {
					t.Fatalf("array %s value differs:\n got=%#v\nwant=%#v", name, got, want)
				}
				re, err := arrSchema.AppendEncode(nil, got)
				if err != nil || !bytes.Equal(re, canonicalWire) {
					t.Fatalf("array %s re-encode not canonical: err=%v\n re=%x\nwant=%x", name, err, re, canonicalWire)
				}
			}

			// ---- map (entries are key + value) ----
			u3 := &uniq{}
			mapSchema := avro.MustParse(fmt.Sprintf(`{"type":"map","values":%s}`, fr.schema(u3)))
			strSchema := avro.MustParse(`"string"`)
			var entries [][]byte
			keys := []string{"a", "b", "c"}
			for _, k := range keys {
				kb, _ := strSchema.AppendEncode(nil, k)
				entries = append(entries, append(kb, item...))
			}
			mv := map[string]any{"a": v, "b": v, "c": v}
			var mwant any
			mCanon, err := mapSchema.AppendEncode(nil, mv)
			if err != nil {
				t.Fatalf("map encode: %v", err)
			}
			if _, err := mapSchema.Decode(mCanon, &mwant); err != nil {
				t.Fatalf("canonical map decode: %v", err)
			}
			for name, wire := range frameVariants(entries) {
				var got any
				rest, err := mapSchema.Decode(wire, &got)
				if err != nil || len(rest) != 0 {
					t.Fatalf("map %s decode: err=%v rest=%d\nwire=%x", name, err, len(rest), wire)
				}
				if !matEqual(got, mwant) {
					t.Fatalf("map %s value differs:\n got=%#v\nwant=%#v", name, got, mwant)
				}
			}

			// Typed-slice targets see the same variants (the per-primitive
			// container fast loops have their own block-walking code, distinct
			// from the any-decode loop above). Assert the decoded ELEMENT VALUES
			// against the canonical any-decode (want), not just len — a fast
			// loop that mis-assembles values across split/negative-count/overlong
			// framings while still landing 3 slots must red here.
			wantArr := want.([]any)
			if fr.label == "int" {
				for name, wire := range frameVariants(items) {
					var typed []int32
					if _, err := arrSchema.Decode(wire, &typed); err != nil {
						t.Fatalf("typed array %s decode: %v", name, err)
					}
					if len(typed) != len(wantArr) {
						t.Fatalf("typed array %s: got len %d want %d (%v)", name, len(typed), len(wantArr), typed)
					}
					for i, e := range typed {
						if !matEqual(e, wantArr[i]) {
							t.Fatalf("typed array %s elem %d: got %v want %v", name, i, e, wantArr[i])
						}
					}
				}
			}
			if fr.label == "string" {
				for name, wire := range frameVariants(items) {
					var typed []string
					if _, err := arrSchema.Decode(wire, &typed); err != nil {
						t.Fatalf("typed string array %s decode: %v", name, err)
					}
					if len(typed) != len(wantArr) {
						t.Fatalf("typed string array %s: got len %d want %d (%v)", name, len(typed), len(wantArr), typed)
					}
					for i, e := range typed {
						if !matEqual(e, wantArr[i]) {
							t.Fatalf("typed string array %s elem %d: got %v want %v", name, i, e, wantArr[i])
						}
					}
				}
			}
		})
	}
}

// The same foreign framings inside a SKIPPED field: the skip path has its
// own block walker (including the byte-size fast-skip), which must consume
// every variant exactly.
func TestMatrix_ForeignFramingThroughSkip(t *testing.T) {
	wSchema := `{"type":"record","name":"R","fields":[
		{"name":"drop","type":{"type":"array","items":"string"}},
		{"name":"keep","type":"int"}]}`
	rSchema := `{"type":"record","name":"R","fields":[
		{"name":"keep","type":"int"}]}`
	w := avro.MustParse(wSchema)
	r := avro.MustParse(rSchema)
	res, err := avro.Resolve(w, r)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	strSchema := avro.MustParse(`"string"`)
	var items [][]byte
	for _, s := range []string{"x", "yy", "zzz"} {
		b, _ := strSchema.AppendEncode(nil, s)
		items = append(items, b)
	}
	keepWire, _ := avro.MustParse(`"int"`).AppendEncode(nil, int32(42))
	for name, arrWire := range frameVariants(items) {
		wire := append(append([]byte{}, arrWire...), keepWire...)
		var got map[string]any
		if _, err := res.Decode(wire, &got); err != nil {
			t.Fatalf("skip %s: %v", name, err)
		}
		if got["keep"] != int32(42) {
			t.Fatalf("skip %s corrupted following field: %#v", name, got)
		}
	}
}

// Multi-block array wires into FIXED-SIZE Go array targets: the remaining-
// capacity bound in deserFixedArray (count > arrLen-idx) is only
// distinguishable from a broken one once idx > 0 — i.e. on the SECOND
// block — because at idx=0 the subtraction is inert. A second block that
// over-claims must error ("got more"), never walk past the array end; an
// exact multi-block fill must succeed; an under-fill must report the
// shortfall. Single-block over-counts cannot pin this bound.
func TestMatrix_MultiBlockIntoFixedArray(t *testing.T) {
	s := avro.MustParse(`{"type":"array","items":"int"}`)
	item := func(v int64) []byte { return putZigzag(nil, v) }
	wire := func(blocks ...[]byte) []byte {
		var w []byte
		for _, b := range blocks {
			w = append(w, b...)
		}
		return append(w, 0x00)
	}
	block := func(items ...[]byte) []byte {
		b := putZigzag(nil, int64(len(items)))
		for _, it := range items {
			b = append(b, it...)
		}
		return b
	}

	t.Run("exact-fill-across-blocks", func(t *testing.T) {
		var out [2]int32
		w := wire(block(item(7)), block(item(8)))
		if _, err := s.Decode(w, &out); err != nil {
			t.Fatalf("two 1-item blocks into [2]int32: %v", err)
		}
		if out != [2]int32{7, 8} {
			t.Fatalf("got %v", out)
		}
	})
	t.Run("second-block-overclaims", func(t *testing.T) {
		var out [2]int32
		w := wire(block(item(7)), block(item(8), item(9)))
		if _, err := s.Decode(w, &out); err == nil {
			t.Fatal("3 items across blocks into [2]int32 must error, not panic")
		}
	})
	t.Run("underfill-reports-shortfall", func(t *testing.T) {
		var out [2]int32
		w := wire(block(item(7)))
		if _, err := s.Decode(w, &out); err == nil {
			t.Fatal("1 item into [2]int32 must error")
		}
	})
	t.Run("json-parity", func(t *testing.T) {
		var out [2]int32
		if err := s.DecodeJSON([]byte(`[7,8]`), &out); err != nil {
			t.Fatalf("JSON exact fill: %v", err)
		}
		if err := s.DecodeJSON([]byte(`[7,8,9]`), &out); err == nil {
			t.Fatal("JSON 3 items into [2]int32 must error")
		}
		if err := s.DecodeJSON([]byte(`[7]`), &out); err == nil {
			t.Fatal("JSON 1 item into [2]int32 must error")
		}
	})
}
