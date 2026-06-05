package avro_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/twmb/avro"
)

// ---------------------------------------------------------------------------
// Recursion matrix: recursive schemas through every carrier kind, at several
// depths, through every wire path (the six-step core + rebuild + identity
// resolve from matrix_core), plus TaggedUnions, typed-struct targets, and
// promotion-inside-recursion.
// ---------------------------------------------------------------------------

type recShape struct {
	label  string
	schema string
	// value builds a generic value chain of the given depth (depth 0 =
	// terminal node, no recursion taken).
	value func(depth int) any
	// tagged: whether the shape's unions are same-class ambiguous bare —
	// if so, runCore runs with TaggedUnions (branch fully recoverable).
	needTagged bool
}

func recShapes() []recShape {
	return []recShape{
		{label: "direct-nullunion",
			schema: `{"type":"record","name":"Node","fields":[
				{"name":"v","type":"int"},
				{"name":"next","type":["null","Node"],"default":null}]}`,
			value: func(d int) any {
				cur := map[string]any{"v": int32(d), "next": nil}
				for i := d - 1; i >= 0; i-- {
					cur = map[string]any{"v": int32(i), "next": cur}
				}
				return cur
			}},
		{label: "array-carrier",
			schema: `{"type":"record","name":"Node","fields":[
				{"name":"v","type":"int"},
				{"name":"kids","type":{"type":"array","items":"Node"}}]}`,
			value: func(d int) any {
				cur := map[string]any{"v": int32(d), "kids": []any{}}
				for i := d - 1; i >= 0; i-- {
					cur = map[string]any{"v": int32(i), "kids": []any{cur}}
				}
				return cur
			}},
		{label: "array-carrier-branch2",
			schema: `{"type":"record","name":"Node","fields":[
				{"name":"v","type":"int"},
				{"name":"kids","type":{"type":"array","items":"Node"}}]}`,
			value: func(d int) any {
				leaf := func(v int32) map[string]any { return map[string]any{"v": v, "kids": []any{}} }
				if d == 0 {
					return leaf(0)
				}
				cur := map[string]any{"v": int32(0), "kids": []any{leaf(1), leaf(2)}}
				for i := 1; i < d; i++ {
					cur = map[string]any{"v": int32(i), "kids": []any{cur, leaf(int32(100 + i))}}
				}
				return cur
			}},
		{label: "map-carrier",
			schema: `{"type":"record","name":"Node","fields":[
				{"name":"v","type":"int"},
				{"name":"kids","type":{"type":"map","values":"Node"}}]}`,
			value: func(d int) any {
				cur := map[string]any{"v": int32(d), "kids": map[string]any{}}
				for i := d - 1; i >= 0; i-- {
					cur = map[string]any{"v": int32(i), "kids": map[string]any{"c": cur}}
				}
				return cur
			}},
		{label: "nullable-array-carrier",
			schema: `{"type":"record","name":"Node","fields":[
				{"name":"v","type":"int"},
				{"name":"kids","type":["null",{"type":"array","items":"Node"}],"default":null}]}`,
			value: func(d int) any {
				cur := map[string]any{"v": int32(d), "kids": nil}
				for i := d - 1; i >= 0; i-- {
					cur = map[string]any{"v": int32(i), "kids": []any{cur}}
				}
				return cur
			}},
		{label: "multibranch-self",
			schema: `{"type":"record","name":"Node","fields":[
				{"name":"v","type":"int"},
				{"name":"next","type":["null","string","Node"],"default":null}]}`,
			value: func(d int) any {
				var cur any = "tail"
				for i := d - 1; i >= 0; i-- {
					cur = map[string]any{"v": int32(i), "next": cur}
				}
				if d == 0 {
					return map[string]any{"v": int32(0), "next": nil}
				}
				return cur
			}},
		{label: "mutual",
			schema: `{"type":"record","name":"A","fields":[
				{"name":"v","type":"int"},
				{"name":"b","type":["null",{"type":"record","name":"B","fields":[
					{"name":"w","type":"string"},
					{"name":"a","type":["null","A"],"default":null}]}],"default":null}]}`,
			value: func(d int) any {
				// Alternate A→B→A…, d levels of descent.
				var build func(level int) any
				build = func(level int) any {
					if level >= d {
						return map[string]any{"v": int32(level), "b": nil}
					}
					return map[string]any{"v": int32(level), "b": map[string]any{
						"w": fmt.Sprintf("w%d", level),
						"a": func() any {
							if level+1 >= d {
								return nil
							}
							return build(level + 1)
						}(),
					}}
				}
				return build(0)
			}},
		{label: "fwd-ref-union",
			schema: `{"type":"record","name":"Wrap","fields":[
				{"name":"early","type":["null","Node"],"default":null},
				{"name":"def","type":{"type":"record","name":"Node","fields":[
					{"name":"v","type":"int"},
					{"name":"next","type":["null","Node"],"default":null}]}}]}`,
			value: func(d int) any {
				chain := func() any {
					cur := map[string]any{"v": int32(d), "next": nil}
					for i := d - 1; i >= 0; i-- {
						cur = map[string]any{"v": int32(i), "next": cur}
					}
					return cur
				}
				return map[string]any{"early": chain(), "def": chain()}
			}},
		{label: "through-mid-record",
			schema: `{"type":"record","name":"Node","fields":[
				{"name":"v","type":"int"},
				{"name":"mid","type":{"type":"record","name":"Mid","fields":[
					{"name":"next","type":["null","Node"],"default":null}]}}]}`,
			value: func(d int) any {
				cur := map[string]any{"v": int32(d), "mid": map[string]any{"next": nil}}
				for i := d - 1; i >= 0; i-- {
					cur = map[string]any{"v": int32(i), "mid": map[string]any{"next": cur}}
				}
				return cur
			}},
	}
}

func TestMatrix_Recursion(t *testing.T) {
	depths := []int{0, 1, 3, 17}
	for _, sh := range recShapes() {
		for _, d := range depths {
			t.Run(fmt.Sprintf("%s/depth%d", sh.label, d), func(t *testing.T) {
				runCore(t, sh.schema, sh.value(d))
			})
			t.Run(fmt.Sprintf("%s/depth%d/tagged", sh.label, d), func(t *testing.T) {
				runCore(t, sh.schema, sh.value(d), avro.TaggedUnions())
			})
		}
	}
}

// Recursion composed INSIDE outer contexts: the recursive record as array
// item, map value, union branch, and nested record field.
func TestMatrix_RecursionInContext(t *testing.T) {
	node := `{"type":"record","name":"Node","fields":[
		{"name":"v","type":"int"},
		{"name":"next","type":["null","Node"],"default":null}]}`
	chain := func(d int) any {
		cur := map[string]any{"v": int32(d), "next": nil}
		for i := d - 1; i >= 0; i-- {
			cur = map[string]any{"v": int32(i), "next": cur}
		}
		return cur
	}
	cases := []struct {
		label  string
		schema string
		value  func(d int) any
	}{
		{"array-of-recursive", fmt.Sprintf(`{"type":"array","items":%s}`, node),
			func(d int) any { return []any{chain(d), chain(0)} }},
		{"map-of-recursive", fmt.Sprintf(`{"type":"map","values":%s}`, node),
			func(d int) any { return map[string]any{"k": chain(d)} }},
		{"nullunion-of-recursive", fmt.Sprintf(`["null",%s]`, node),
			func(d int) any { return chain(d) }},
		{"field-of-recursive", fmt.Sprintf(`{"type":"record","name":"Outer","fields":[{"name":"n","type":%s},{"name":"s","type":"string"}]}`, node),
			func(d int) any { return map[string]any{"n": chain(d), "s": "x"} }},
		{"array-of-nullunion-of-recursive", fmt.Sprintf(`{"type":"array","items":["null",%s]}`, node),
			func(d int) any { return []any{chain(d), nil, chain(1)} }},
	}
	for _, c := range cases {
		for _, d := range []int{0, 2, 9} {
			t.Run(fmt.Sprintf("%s/depth%d", c.label, d), func(t *testing.T) {
				runCore(t, c.schema, c.value(d))
			})
		}
	}
}

// Typed (struct) targets for recursive shapes: the unsafe struct path and
// the reflect path must agree with the generic path byte-for-byte, on both
// wire formats, for addressable and non-addressable encodes.
type recNode struct {
	V    int32    `avro:"v"`
	Next *recNode `avro:"next"`
}

type recArrNode struct {
	V    int32        `avro:"v"`
	Kids []recArrNode `avro:"kids"`
}

type recA struct {
	V int32 `avro:"v"`
	B *recB `avro:"b"`
}
type recB struct {
	W string `avro:"w"`
	A *recA  `avro:"a"`
}

func TestMatrix_RecursionTyped(t *testing.T) {
	mkChain := func(d int) *recNode {
		cur := &recNode{V: int32(d)}
		for i := d - 1; i >= 0; i-- {
			cur = &recNode{V: int32(i), Next: cur}
		}
		return cur
	}
	mkArr := func(d int) recArrNode {
		cur := recArrNode{V: int32(d), Kids: []recArrNode{}}
		for i := d - 1; i >= 0; i-- {
			cur = recArrNode{V: int32(i), Kids: []recArrNode{cur}}
		}
		return cur
	}
	mkAB := func(d int) *recA {
		var build func(level int) *recA
		build = func(level int) *recA {
			a := &recA{V: int32(level)}
			if level < d {
				b := &recB{W: fmt.Sprintf("w%d", level)}
				if level+1 < d {
					b.A = build(level + 1)
				}
				a.B = b
			}
			return a
		}
		return build(0)
	}

	directSchema := `{"type":"record","name":"Node","fields":[
		{"name":"v","type":"int"},
		{"name":"next","type":["null","Node"],"default":null}]}`
	arrSchema := `{"type":"record","name":"Node","fields":[
		{"name":"v","type":"int"},
		{"name":"kids","type":{"type":"array","items":"Node"}}]}`
	abSchema := `{"type":"record","name":"A","fields":[
		{"name":"v","type":"int"},
		{"name":"b","type":["null",{"type":"record","name":"B","fields":[
			{"name":"w","type":"string"},
			{"name":"a","type":["null","A"],"default":null}]}],"default":null}]}`

	for _, d := range []int{0, 1, 3, 17} {
		t.Run(fmt.Sprintf("direct/depth%d", d), func(t *testing.T) {
			typedCore(t, directSchema, *mkChain(d), func() any { return new(recNode) })
		})
		t.Run(fmt.Sprintf("array/depth%d", d), func(t *testing.T) {
			typedCore(t, arrSchema, mkArr(d), func() any { return new(recArrNode) })
		})
		t.Run(fmt.Sprintf("mutual/depth%d", d), func(t *testing.T) {
			typedCore(t, abSchema, *mkAB(d), func() any { return new(recA) })
		})
	}
}

// typedCore: encode the typed value (addressable AND non-addressable forms
// must agree), decode generic and typed, re-encode from the typed decode,
// and run the JSON twins — all byte-identical wires.
func typedCore(t *testing.T, schemaJSON string, typedVal any, newTarget func() any) {
	t.Helper()
	s, err := avro.Parse(schemaJSON)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	// Addressable (pointer) vs non-addressable (value) encode parity.
	wVal, err := s.AppendEncode(nil, typedVal)
	if err != nil {
		t.Fatalf("encode value-form: %v", err)
	}
	pv := newTarget()
	// Fill the pointer with the same value for the addressable encode:
	// decode the value-form wire into it (also exercises typed decode).
	if _, err := s.Decode(wVal, pv); err != nil {
		t.Fatalf("typed decode: %v", err)
	}
	wPtr, err := s.AppendEncode(nil, pv)
	if err != nil {
		t.Fatalf("encode pointer-form: %v", err)
	}
	if !bytes.Equal(wVal, wPtr) {
		t.Fatalf("addressable vs non-addressable wire differs:\n val=%x\n ptr=%x", wVal, wPtr)
	}
	// Generic decode agrees with typed round-trip.
	var generic any
	if _, err := s.Decode(wVal, &generic); err != nil {
		t.Fatalf("generic decode: %v", err)
	}
	wGen, err := s.AppendEncode(nil, generic)
	if err != nil || !bytes.Equal(wGen, wVal) {
		t.Fatalf("generic re-encode differs: err=%v\n val=%x\n gen=%x", err, wVal, wGen)
	}
	// JSON twins: typed encode, typed decode, generic agreement.
	jVal, err := s.AppendEncodeJSON(nil, typedVal)
	if err != nil {
		t.Fatalf("encodeJSON value-form: %v", err)
	}
	jPtr, err := s.AppendEncodeJSON(nil, pv)
	if err != nil || !bytes.Equal(jVal, jPtr) {
		t.Fatalf("JSON addressable/non-addressable differs: err=%v\n %s\n %s", err, jVal, jPtr)
	}
	pj := newTarget()
	if err := s.DecodeJSON(jVal, pj); err != nil {
		t.Fatalf("typed DecodeJSON: %v", err)
	}
	wFromJSON, err := s.AppendEncode(nil, pj)
	if err != nil || !bytes.Equal(wFromJSON, wVal) {
		t.Fatalf("typed JSON round-trip lands on different wire: err=%v\n w=%x\n j=%x", err, wVal, wFromJSON)
	}
}

// Promotion inside recursion across carriers: writer int chains resolve into
// reader long/double chains through every recursive carrier.
func TestMatrix_RecursionPromotion(t *testing.T) {
	pairs := []struct {
		label  string
		writer string
		reader string
	}{
		{"direct int→long",
			`{"type":"record","name":"N","fields":[{"name":"v","type":"int"},{"name":"next","type":["null","N"],"default":null}]}`,
			`{"type":"record","name":"N","fields":[{"name":"v","type":"long"},{"name":"next","type":["null","N"],"default":null}]}`},
		{"array int→double",
			`{"type":"record","name":"N","fields":[{"name":"v","type":"int"},{"name":"kids","type":{"type":"array","items":"N"}}]}`,
			`{"type":"record","name":"N","fields":[{"name":"v","type":"double"},{"name":"kids","type":{"type":"array","items":"N"}}]}`},
		{"map string→bytes",
			`{"type":"record","name":"N","fields":[{"name":"v","type":"string"},{"name":"kids","type":{"type":"map","values":"N"}}]}`,
			`{"type":"record","name":"N","fields":[{"name":"v","type":"bytes"},{"name":"kids","type":{"type":"map","values":"N"}}]}`},
		{"mutual int→long",
			`{"type":"record","name":"A","fields":[{"name":"v","type":"int"},{"name":"b","type":["null",{"type":"record","name":"B","fields":[{"name":"a","type":["null","A"],"default":null}]}],"default":null}]}`,
			`{"type":"record","name":"A","fields":[{"name":"v","type":"long"},{"name":"b","type":["null",{"type":"record","name":"B","fields":[{"name":"a","type":["null","A"],"default":null}]}],"default":null}]}`},
	}
	for _, p := range pairs {
		t.Run(p.label, func(t *testing.T) {
			w := avro.MustParse(p.writer)
			r := avro.MustParse(p.reader)
			res, err := avro.Resolve(w, r)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			// Build a depth-3 writer value generically against the writer.
			var v any
			switch p.label {
			case "direct int→long":
				v = map[string]any{"v": int32(1), "next": map[string]any{"v": int32(2), "next": map[string]any{"v": int32(3), "next": nil}}}
			case "array int→double":
				v = map[string]any{"v": int32(1), "kids": []any{map[string]any{"v": int32(2), "kids": []any{map[string]any{"v": int32(3), "kids": []any{}}}}}}
			case "map string→bytes":
				v = map[string]any{"v": "a", "kids": map[string]any{"k": map[string]any{"v": "b", "kids": map[string]any{}}}}
			case "mutual int→long":
				v = map[string]any{"v": int32(1), "b": map[string]any{"a": map[string]any{"v": int32(2), "b": nil}}}
			}
			wire, err := w.AppendEncode(nil, v)
			if err != nil {
				t.Fatalf("writer encode: %v", err)
			}
			var got any
			if _, err := res.Decode(wire, &got); err != nil {
				t.Fatalf("resolved decode: %v", err)
			}
			// Spot-check the promoted leaf types at the top level.
			top := got.(map[string]any)
			switch p.label {
			case "direct int→long", "mutual int→long":
				if _, ok := top["v"].(int64); !ok {
					t.Fatalf("v promoted to %T, want int64", top["v"])
				}
			case "array int→double":
				if _, ok := top["v"].(float64); !ok {
					t.Fatalf("v promoted to %T, want float64", top["v"])
				}
			case "map string→bytes":
				if _, ok := top["v"].([]byte); !ok {
					t.Fatalf("v promoted to %T, want []byte", top["v"])
				}
			}
			// And the resolved value re-encodes cleanly against the READER.
			rs := avro.MustParse(p.reader)
			if _, err := rs.AppendEncode(nil, got); err != nil {
				t.Fatalf("re-encode promoted value against reader: %v", err)
			}
		})
	}
}
