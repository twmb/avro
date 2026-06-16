package avro

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

// This file pins the structural invariant that the recursion bound
// (errTooDeep / maxDepth) is UNIFORM: for any recursive schema, the
// SAME nesting depth must trip errTooDeep on every code path —
// binary encode, binary typed-struct decode, binary decode-into-any,
// JSON encode, JSON decode — and the safe (reflect) and unsafe
// (compiled-field) variants must agree.
//
// The bound counts ONE increment per parent→child schema EDGE. A
// linked-list record `{next:["null",Self], v:int}` has TWO edges per
// link (record→union, union→record), so it costs 2 depth units per
// link on every path. A tree `{v:int, kids:array<Self>}` has TWO edges
// per level (record→array, array→record), so 2 per level. The absolute
// VALUE (maxDepth) is a deliberate DoS bound; this test does not pin
// the value, only that the trip depth is IDENTICAL across directions
// and container shapes.
//
// Why an oracle with HAND-ASSEMBLED wire rather than an encode→decode
// round-trip: a round-trip can only feed decode the depth encode
// produced, so it measures min(encode,decode) and reports a false
// "uniform" when decode actually accepts deeper than encode (or vice
// versa). The decode probes below build wire INDEPENDENTLY of the
// encoder so each direction's true trip depth is observed.

// isTooDeep reports whether err is the recursion-bound error (possibly
// wrapped by union try-each / record-field error context).
func isTooDeep(err error) bool { return errors.Is(err, errTooDeep) }

// largestOK returns the largest depth d in [0, hi] for which ok(d)
// returns (true, nil), requiring that every depth below the first
// errTooDeep also succeeds and that the failure is exactly errTooDeep
// (not some unrelated decode/encode error). It walks upward so a
// non-monotone or non-errTooDeep failure is reported precisely.
//
// ok(d) must return (true, nil) on success, (false, errTooDeep-wrapped)
// at/over the bound. Any other error fails the probe loudly.
func largestOK(t *testing.T, name string, hi int, ok func(d int) error) int {
	t.Helper()
	last := -1
	for d := 0; d <= hi; d++ {
		err := ok(d)
		if err == nil {
			if last != d-1 {
				t.Fatalf("%s: depth %d succeeded after an earlier failure at %d (non-monotone)", name, d, last+1)
			}
			last = d
			continue
		}
		if !isTooDeep(err) {
			t.Fatalf("%s: depth %d failed with non-errTooDeep error: %v", name, d, err)
		}
		// First errTooDeep: this is the boundary. Everything below
		// succeeded (last == d-1). Return the last accepted depth.
		if last != d-1 {
			t.Fatalf("%s: errTooDeep at %d but last success was %d (gap)", name, d, last)
		}
		return last
	}
	t.Fatalf("%s: never hit errTooDeep up to %d (bound not reached — budget gutted or probe too shallow)", name, hi)
	return -1
}

// hiProbe is the upper search bound: comfortably above maxDepth so even
// the deepest-counting path (2 units/level → trips near maxDepth/2 in
// LEVELS) is reached, with headroom.
const hiProbe = maxDepth + 50

//////////////////////////////////////////////////////////////////////
// Shape 1: linked list — record{next:["null",Self], v:int}
//////////////////////////////////////////////////////////////////////

type llNode struct {
	Next *llNode `avro:"next"`
	V    int32   `avro:"v"`
}

const llSchema = `{"type":"record","name":"LL","fields":[` +
	`{"name":"next","type":["null","LL"]},` +
	`{"name":"v","type":"int"}]}`

// llValue builds a chain of d links: d==0 is a single node (next=nil).
func llValue(d int) *llNode {
	n := &llNode{}
	for i := 0; i < d; i++ {
		n = &llNode{Next: n}
	}
	return n
}

// llWire hand-assembles the binary wire for a d-link chain.
// next-null = 0x00, next-val = 0x02 + inner, v=int32(0) = 0x00.
// depth-d = 0x02 + (depth-(d-1)) + 0x00, bottoming at depth0 = {0x00,0x00}.
func llWire(d int) []byte {
	// innermost: next=nil(0x00) v=0(0x00)
	w := []byte{0x00, 0x00}
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+2)
		nw = append(nw, 0x02) // next: value branch
		nw = append(nw, w...) // inner node
		nw = append(nw, 0x00) // v = 0
		w = nw
	}
	return w
}

// llJSON hand-assembles JSON for a d-link chain. ["null",T] union JSON
// encodes the value branch as {"LL": <inner>}; null as null.
func llJSON(d int) []byte {
	inner := `{"next":null,"v":0}`
	for i := 0; i < d; i++ {
		inner = `{"next":{"LL":` + inner + `},"v":0}`
	}
	return []byte(inner)
}

//////////////////////////////////////////////////////////////////////
// Shape 2: tree, value elements — record{v:int, kids:array<Self>}
//////////////////////////////////////////////////////////////////////

type treeV struct {
	V    int32   `avro:"v"`
	Kids []treeV `avro:"kids"`
}

const treeSchema = `{"type":"record","name":"T","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":{"type":"array","items":"T"}}]}`

func treeVValue(d int) *treeV {
	n := &treeV{}
	for i := 0; i < d; i++ {
		n = &treeV{Kids: []treeV{*n}}
	}
	return n
}

// treePtr is the pointer-element variant (exercises a different unsafe
// array path: []*T vs []T).
type treePtr struct {
	V    int32      `avro:"v"`
	Kids []*treePtr `avro:"kids"`
}

func treePtrValue(d int) *treePtr {
	n := &treePtr{}
	for i := 0; i < d; i++ {
		n = &treePtr{Kids: []*treePtr{n}}
	}
	return n
}

// treeWire: v=0(0x00) + array[count=1(0x02), inner, terminator(0x00)].
// depth-d = 0x00 + 0x02 + (depth-(d-1)) + 0x00, bottoming at depth0 =
// {0x00(v), 0x00(empty array)}.
func treeWire(d int) []byte {
	w := []byte{0x00, 0x00} // v=0, kids=empty array
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+3)
		nw = append(nw, 0x00) // v = 0
		nw = append(nw, 0x02) // kids: array block count = 1
		nw = append(nw, w...) // the single child
		nw = append(nw, 0x00) // array terminator
		w = nw
	}
	return w
}

func treeJSON(d int) []byte {
	inner := `{"v":0,"kids":[]}`
	for i := 0; i < d; i++ {
		inner = `{"v":0,"kids":[` + inner + `]}`
	}
	return []byte(inner)
}

//////////////////////////////////////////////////////////////////////
// Shape 3: map-recursive — record{v:int, kids:map<Self>}
//////////////////////////////////////////////////////////////////////

type mapNode struct {
	V    int32              `avro:"v"`
	Kids map[string]mapNode `avro:"kids"`
}

const mapSchema = `{"type":"record","name":"M","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":{"type":"map","values":"M"}}]}`

func mapValue(d int) *mapNode {
	n := &mapNode{}
	for i := 0; i < d; i++ {
		n = &mapNode{Kids: map[string]mapNode{"a": *n}}
	}
	return n
}

// mapWire: v=0(0x00) + map[count=1(0x02), keylen=1(0x02) "a"(0x61),
// value, terminator(0x00)]. depth0 = {0x00(v), 0x00(empty map)}.
func mapWire(d int) []byte {
	w := []byte{0x00, 0x00} // v=0, kids=empty map
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+5)
		nw = append(nw, 0x00)       // v = 0
		nw = append(nw, 0x02)       // kids: map block count = 1
		nw = append(nw, 0x02, 0x61) // key length 1, "a"
		nw = append(nw, w...)       // the single child value
		nw = append(nw, 0x00)       // map terminator
		w = nw
	}
	return w
}

func mapJSON(d int) []byte {
	inner := `{"v":0,"kids":{}}`
	for i := 0; i < d; i++ {
		inner = `{"v":0,"kids":{"a":` + inner + `}}`
	}
	return []byte(inner)
}

//////////////////////////////////////////////////////////////////////
// Shape 4: mutual recursion — A{b:["null",B]}, B{a:["null",A]}
//////////////////////////////////////////////////////////////////////

type mrA struct {
	B *mrB  `avro:"b"`
	V int32 `avro:"v"`
}
type mrB struct {
	A *mrA  `avro:"a"`
	V int32 `avro:"v"`
}

const mrSchema = `{"type":"record","name":"A","fields":[` +
	`{"name":"b","type":["null",{"type":"record","name":"B","fields":[` +
	`{"name":"a","type":["null","A"]},{"name":"v","type":"int"}]}]},` +
	`{"name":"v","type":"int"}]}`

// mrValue builds an A-topped A→B→A→… chain of exactly `hops` value-branch
// edges. hops==0 is a bare A with b=nil. The chain alternates record
// types, so the innermost record is an A when hops is even and a B when
// hops is odd; building innermost-out from the correctly-typed leaf keeps
// the value's depth in lockstep with mrWire(hops).
func mrValue(hops int) *mrA {
	if hops%2 == 0 {
		// innermost is an A (even number of edges back to the A root).
		a := &mrA{}
		for i := 0; i < hops/2; i++ {
			a = &mrA{B: &mrB{A: a}}
		}
		return a
	}
	// innermost is a B; wrap pairs up to the A root.
	b := &mrB{}
	a := &mrA{B: b}
	for i := 0; i < hops/2; i++ {
		a = &mrA{B: &mrB{A: a}}
	}
	return a
}

// mrWire hand-assembles the A-topped chain of `hops` value-branch edges.
// Each hop prepends 0x02 (the ["null",record] value branch) and appends
// 0x00 (the record's trailing v=0), wrapping the inner node:
//
//	A{}            = 00 00                 (b=null, v=0)
//	A{B{}}         = 02 (00 00) 00         (one hop)
//	A{B{A{}}}      = 02 (02 (00 00) 00) 00 (two hops)
//
// The byte shape is identical regardless of whether the inner record is
// an A or a B (both are {union, v:int}); the alternation lives only in
// the schema graph, not the wire.
func mrWire(hops int) []byte {
	w := []byte{0x00, 0x00} // innermost record: union=null, v=0
	for i := 0; i < hops; i++ {
		nw := make([]byte, 0, len(w)+2)
		nw = append(nw, 0x02) // union: value branch
		nw = append(nw, w...) // inner record
		nw = append(nw, 0x00) // this record's v = 0
		w = nw
	}
	return w
}

//////////////////////////////////////////////////////////////////////
// Container-of-union / array-element-union seam.
//
// These shapes interpose a union schema node between a container (array
// or map) and the recursive child, or wrap a container branch in a
// nullable field union. The union is its OWN schema node and must cost
// exactly one depth unit on EVERY path. The encode-side fast paths once
// either skipped that node entirely (the array fast path entered the
// record straight from the array depth, collapsing array→union→record to
// array→record) or charged the union edge without guarding the union node
// (a fence-post that tripped one level deeper than decode). Decode and
// JSON always charge the union node; these probes pin encode to agree.
//
// Each carrier is exercised in BOTH null-position orderings (["null",T]
// and [T,"null"]) where the Go-type plumbing allows, and through both the
// unsafe compiled-field path and the reflect path, because they charge
// depth via different mechanisms.
//////////////////////////////////////////////////////////////////////

// Shape: array of null-union of Self — record{v:int, kids:array<["null",Self]>}.
// Go []*N exercises the unsafe usArrayNullUnionRecord fast path.
type arrNUNode struct {
	V    int32        `avro:"v"`
	Kids []*arrNUNode `avro:"kids"`
}

const arrNUSchema = `{"type":"record","name":"AN","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":{"type":"array","items":["null","AN"]}}]}`

func arrNUValue(d int) any {
	n := &arrNUNode{}
	for i := 0; i < d; i++ {
		n = &arrNUNode{Kids: []*arrNUNode{n}}
	}
	return n
}

// wire: v=0(00) + array[count=1(02), union-val-branch(02), inner, term(00)].
// depth0 = v=0(00) + empty array(00).
func arrNUWire(d int) []byte {
	w := []byte{0x00, 0x00}
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+4)
		nw = append(nw, 0x00) // v
		nw = append(nw, 0x02) // array count=1
		nw = append(nw, 0x02) // union: value branch (index 1)
		nw = append(nw, w...) // inner record
		nw = append(nw, 0x00) // array terminator
		w = nw
	}
	return w
}

func arrNUJSON(d int) []byte {
	inner := `{"v":0,"kids":[]}`
	for i := 0; i < d; i++ {
		inner = `{"v":0,"kids":[{"AN":` + inner + `}]}`
	}
	return []byte(inner)
}

// Reader adds a defaulted field so Resolve builds the resolving deser,
// exercising the resolved decode path across the array-element union.
const arrNUReaderSchema = `{"type":"record","name":"AN","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":{"type":"array","items":["null","AN"]}},` +
	`{"name":"extra","type":["null","int"],"default":null}]}`

type arrNUReader struct {
	V     int32          `avro:"v"`
	Kids  []*arrNUReader `avro:"kids"`
	Extra *int32         `avro:"extra"`
}

// Shape: array of null-union of Self with []**N (multi-pointer element).
// The unsafe fast path declines multi-pointer elements, so this drives the
// REFLECT serArray.ser + serNullUnionAt serItem path on the same schema.
type arrNUPP struct {
	V    int32       `avro:"v"`
	Kids []**arrNUPP `avro:"kids"`
}

const arrNUPPSchema = `{"type":"record","name":"AP","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":{"type":"array","items":["null","AP"]}}]}`

func arrNUPPValue(d int) any {
	n := &arrNUPP{}
	for i := 0; i < d; i++ {
		inner := n
		n = &arrNUPP{Kids: []**arrNUPP{&inner}}
	}
	return n
}

func arrNUPPWire(d int) []byte {
	// identical wire to []*N (only the Go plumbing differs)
	w := []byte{0x00, 0x00}
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+4)
		nw = append(nw, 0x00, 0x02, 0x02)
		nw = append(nw, w...)
		nw = append(nw, 0x00)
		w = nw
	}
	return w
}

func arrNUPPJSON(d int) []byte {
	inner := `{"v":0,"kids":[]}`
	for i := 0; i < d; i++ {
		inner = `{"v":0,"kids":[{"AP":` + inner + `}]}`
	}
	return []byte(inner)
}

// Shape: array of NULL-SECOND union of Self — items [Self,"null"].
// value branch index 0 (0x00); null index 1 (0x02).
type arrNSNode struct {
	V    int32        `avro:"v"`
	Kids []*arrNSNode `avro:"kids"`
}

const arrNSSchema = `{"type":"record","name":"NS","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":{"type":"array","items":["NS","null"]}}]}`

func arrNSValue(d int) any {
	n := &arrNSNode{}
	for i := 0; i < d; i++ {
		n = &arrNSNode{Kids: []*arrNSNode{n}}
	}
	return n
}

func arrNSWire(d int) []byte {
	w := []byte{0x00, 0x00}
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+4)
		nw = append(nw, 0x00) // v
		nw = append(nw, 0x02) // array count=1
		nw = append(nw, 0x00) // union: value branch (index 0)
		nw = append(nw, w...)
		nw = append(nw, 0x00) // array terminator
		w = nw
	}
	return w
}

func arrNSJSON(d int) []byte {
	inner := `{"v":0,"kids":[]}`
	for i := 0; i < d; i++ {
		inner = `{"v":0,"kids":[{"NS":` + inner + `}]}`
	}
	return []byte(inner)
}

// Shape: map of null-union of Self — record{v:int, kids:map<["null",Self]>}.
// Go map[string]*N; maps have no unsafe path, so the serItem is the reflect
// serNullUnionAt (the same helper a nullunion field uses).
type mapNUNode struct {
	V    int32                 `avro:"v"`
	Kids map[string]*mapNUNode `avro:"kids"`
}

const mapNUSchema = `{"type":"record","name":"MN","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":{"type":"map","values":["null","MN"]}}]}`

func mapNUValue(d int) any {
	n := &mapNUNode{}
	for i := 0; i < d; i++ {
		n = &mapNUNode{Kids: map[string]*mapNUNode{"a": n}}
	}
	return n
}

func mapNUWire(d int) []byte {
	w := []byte{0x00, 0x00}
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+6)
		nw = append(nw, 0x00)       // v
		nw = append(nw, 0x02)       // map count=1
		nw = append(nw, 0x02, 0x61) // keylen=1, "a"
		nw = append(nw, 0x02)       // union value branch
		nw = append(nw, w...)       // inner record
		nw = append(nw, 0x00)       // map terminator
		w = nw
	}
	return w
}

func mapNUJSON(d int) []byte {
	inner := `{"v":0,"kids":{}}`
	for i := 0; i < d; i++ {
		inner = `{"v":0,"kids":{"a":{"MN":` + inner + `}}}`
	}
	return []byte(inner)
}

// Shape: field ["null", array<Self>] — a nullable container branch.
// Go *[]N exercises the unsafe usNullUnionPtr wrapping the array fn.
type nuArrNode struct {
	V    int32        `avro:"v"`
	Kids *[]nuArrNode `avro:"kids"`
}

const nuArrSchema = `{"type":"record","name":"NA","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":["null",{"type":"array","items":"NA"}]}]}`

func nuArrValue(d int) any {
	n := &nuArrNode{}
	for i := 0; i < d; i++ {
		kids := []nuArrNode{*n}
		n = &nuArrNode{Kids: &kids}
	}
	return n
}

func nuArrWire(d int) []byte {
	w := []byte{0x00, 0x00} // v=0, kids=null
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+4)
		nw = append(nw, 0x00) // v
		nw = append(nw, 0x02) // union value branch (array)
		nw = append(nw, 0x02) // array count=1
		nw = append(nw, w...) // inner record
		nw = append(nw, 0x00) // array terminator
		w = nw
	}
	return w
}

func nuArrJSON(d int) []byte {
	inner := `{"v":0,"kids":null}`
	for i := 0; i < d; i++ {
		inner = `{"v":0,"kids":{"array":[` + inner + `]}}`
	}
	return []byte(inner)
}

// Shape: field ["null", map<Self>] — nullable map branch. Go *map[string]N.
type nuMapNode struct {
	V    int32                 `avro:"v"`
	Kids *map[string]nuMapNode `avro:"kids"`
}

const nuMapSchema = `{"type":"record","name":"NMp","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":["null",{"type":"map","values":"NMp"}]}]}`

func nuMapValue(d int) any {
	n := &nuMapNode{}
	for i := 0; i < d; i++ {
		kids := map[string]nuMapNode{"a": *n}
		n = &nuMapNode{Kids: &kids}
	}
	return n
}

func nuMapWire(d int) []byte {
	w := []byte{0x00, 0x00} // v=0, kids=null
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+6)
		nw = append(nw, 0x00)       // v
		nw = append(nw, 0x02)       // union value branch (map)
		nw = append(nw, 0x02)       // map count=1
		nw = append(nw, 0x02, 0x61) // keylen=1, "a"
		nw = append(nw, w...)       // inner record value
		nw = append(nw, 0x00)       // map terminator
		w = nw
	}
	return w
}

func nuMapJSON(d int) []byte {
	inner := `{"v":0,"kids":null}`
	for i := 0; i < d; i++ {
		inner = `{"v":0,"kids":{"map":{"a":` + inner + `}}}`
	}
	return []byte(inner)
}

// Shape: array of multibranch (non-null-union) containing Self —
// array<["null","int",Self]>. Routes through the general serUnion.ser
// (3-branch), not the 2-branch null-union fast path. Go []any.
type arrMBNode struct {
	V    int32 `avro:"v"`
	Kids []any `avro:"kids"`
}

const arrMBSchema = `{"type":"record","name":"MB","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":{"type":"array","items":["null","int","MB"]}}]}`

func arrMBValue(d int) any {
	n := &arrMBNode{}
	for i := 0; i < d; i++ {
		n = &arrMBNode{Kids: []any{n}}
	}
	return n
}

func arrMBWire(d int) []byte {
	w := []byte{0x00, 0x00}
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+4)
		nw = append(nw, 0x00) // v
		nw = append(nw, 0x02) // array count=1
		nw = append(nw, 0x04) // union: branch index 2 (MB)
		nw = append(nw, w...)
		nw = append(nw, 0x00) // array terminator
		w = nw
	}
	return w
}

func arrMBJSON(d int) []byte {
	inner := `{"v":0,"kids":[]}`
	for i := 0; i < d; i++ {
		inner = `{"v":0,"kids":[{"MB":` + inner + `}]}`
	}
	return []byte(inner)
}

// Nested combo: array<map<["null",Self]>> — four schema nodes per level
// (array, map, union, record). Go []map[string]*N.
type arrMapNUNode struct {
	V    int32                      `avro:"v"`
	Kids []map[string]*arrMapNUNode `avro:"kids"`
}

const arrMapNUSchema = `{"type":"record","name":"AMN","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":{"type":"array","items":{"type":"map","values":["null","AMN"]}}}]}`

func arrMapNUValue(d int) any {
	n := &arrMapNUNode{}
	for i := 0; i < d; i++ {
		n = &arrMapNUNode{Kids: []map[string]*arrMapNUNode{{"a": n}}}
	}
	return n
}

func arrMapNUWire(d int) []byte {
	w := []byte{0x00, 0x00}
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+8)
		nw = append(nw, 0x00)       // v
		nw = append(nw, 0x02)       // array count=1
		nw = append(nw, 0x02)       // map count=1
		nw = append(nw, 0x02, 0x61) // key "a"
		nw = append(nw, 0x02)       // union value branch
		nw = append(nw, w...)       // inner record
		nw = append(nw, 0x00)       // map terminator
		nw = append(nw, 0x00)       // array terminator
		w = nw
	}
	return w
}

func arrMapNUJSON(d int) []byte {
	inner := `{"v":0,"kids":[]}`
	for i := 0; i < d; i++ {
		inner = `{"v":0,"kids":[{"a":{"AMN":` + inner + `}}]}`
	}
	return []byte(inner)
}

// Nested combo: map<array<["null",Self]>>. Go map[string][]*N.
type mapArrNUNode struct {
	V    int32                      `avro:"v"`
	Kids map[string][]*mapArrNUNode `avro:"kids"`
}

const mapArrNUSchema = `{"type":"record","name":"MAN","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":{"type":"map","values":{"type":"array","items":["null","MAN"]}}}]}`

func mapArrNUValue(d int) any {
	n := &mapArrNUNode{}
	for i := 0; i < d; i++ {
		n = &mapArrNUNode{Kids: map[string][]*mapArrNUNode{"a": {n}}}
	}
	return n
}

func mapArrNUWire(d int) []byte {
	w := []byte{0x00, 0x00}
	for i := 0; i < d; i++ {
		nw := make([]byte, 0, len(w)+8)
		nw = append(nw, 0x00)       // v
		nw = append(nw, 0x02)       // map count=1
		nw = append(nw, 0x02, 0x61) // key "a"
		nw = append(nw, 0x02)       // array count=1
		nw = append(nw, 0x02)       // union value branch
		nw = append(nw, w...)       // inner record
		nw = append(nw, 0x00)       // array terminator
		nw = append(nw, 0x00)       // map terminator
		w = nw
	}
	return w
}

func mapArrNUJSON(d int) []byte {
	inner := `{"v":0,"kids":{}}`
	for i := 0; i < d; i++ {
		inner = `{"v":0,"kids":{"a":[{"MAN":` + inner + `}]}}`
	}
	return []byte(inner)
}

//////////////////////////////////////////////////////////////////////
// The oracle.
//////////////////////////////////////////////////////////////////////

// shapeProbe describes one recursive schema and how to build depth-d
// encodings of it. encodeVal returns a fresh Go value of depth d; wire/
// json build depth-d binary/JSON encodings INDEPENDENT of the encoder
// (so decode's true trip depth is observed, not min(encode,decode)).
// newTyped is a fresh typed *struct decode destination.
//
// readerSchema (optional) is a writer→reader resolution target: a
// structurally compatible reader (the same recursive shape plus an
// extra defaulted field) that forces Resolve to build the resolving
// deser pipeline (Resolve returns the reader directly for identical
// schemas, which would NOT exercise the resolved path). newResolvedTyped
// is the reader-shaped typed destination. Empty readerSchema skips the
// resolved probe.
type shapeProbe struct {
	name             string
	schema           string
	encodeVal        func(d int) any
	wire             func(d int) []byte
	json             func(d int) []byte
	newTyped         func() any
	readerSchema     string
	newResolvedTyped func() any
}

// runShape returns each code path's trip depth (largest accepted depth)
// keyed by path name, so the caller can assert all-equal across however
// many paths the shape exercises.
func runShape(t *testing.T, p shapeProbe) map[string]int {
	t.Helper()
	s := MustParse(p.schema)
	out := map[string]int{}

	out["encode"] = largestOK(t, p.name+"/encode", hiProbe, func(d int) error {
		_, err := s.Encode(p.encodeVal(d))
		return err
	})
	out["typedDecode"] = largestOK(t, p.name+"/decode-typed", hiProbe, func(d int) error {
		_, err := s.Decode(p.wire(d), p.newTyped())
		return err
	})
	out["anyDecode"] = largestOK(t, p.name+"/decode-any", hiProbe, func(d int) error {
		var v any
		_, err := s.Decode(p.wire(d), &v)
		return err
	})
	out["jsonEncode"] = largestOK(t, p.name+"/json-encode", hiProbe, func(d int) error {
		_, err := s.EncodeJSON(p.encodeVal(d))
		return err
	})
	out["jsonDecode"] = largestOK(t, p.name+"/json-decode", hiProbe, func(d int) error {
		var v any
		err := s.DecodeJSON(p.json(d), &v)
		return err
	})
	if p.readerSchema != "" {
		writer := s
		reader := MustParse(p.readerSchema)
		rs, err := Resolve(writer, reader)
		if err != nil {
			t.Fatalf("%s: Resolve: %v", p.name, err)
		}
		out["resolvedTypedDecode"] = largestOK(t, p.name+"/resolved-typed", hiProbe, func(d int) error {
			_, err := rs.Decode(p.wire(d), p.newResolvedTyped())
			return err
		})
		out["resolvedAnyDecode"] = largestOK(t, p.name+"/resolved-any", hiProbe, func(d int) error {
			var v any
			_, err := rs.Decode(p.wire(d), &v)
			return err
		})
	}
	return out
}

// llReaderSchema / treeReaderSchema add one defaulted field so Resolve
// builds the resolving deser (rather than returning the reader directly
// for an identical schema), exercising the resolved decode path on the
// recursive union / array edge.
const llReaderSchema = `{"type":"record","name":"LL","fields":[` +
	`{"name":"next","type":["null","LL"]},` +
	`{"name":"v","type":"int"},` +
	`{"name":"extra","type":["null","int"],"default":null}]}`

const treeReaderSchema = `{"type":"record","name":"T","fields":[` +
	`{"name":"v","type":"int"},` +
	`{"name":"kids","type":{"type":"array","items":"T"}},` +
	`{"name":"extra","type":["null","int"],"default":null}]}`

type llReader struct {
	Next  *llReader `avro:"next"`
	V     int32     `avro:"v"`
	Extra *int32    `avro:"extra"`
}

type treeVReader struct {
	V     int32         `avro:"v"`
	Kids  []treeVReader `avro:"kids"`
	Extra *int32        `avro:"extra"`
}

func TestDepthUniformityOracle(t *testing.T) {
	shapes := []shapeProbe{
		{
			name:             "linked-list",
			schema:           llSchema,
			encodeVal:        func(d int) any { return llValue(d) },
			wire:             llWire,
			json:             llJSON,
			newTyped:         func() any { return new(llNode) },
			readerSchema:     llReaderSchema,
			newResolvedTyped: func() any { return new(llReader) },
		},
		{
			name:             "tree-value-elem",
			schema:           treeSchema,
			encodeVal:        func(d int) any { return treeVValue(d) },
			wire:             treeWire,
			json:             treeJSON,
			newTyped:         func() any { return new(treeV) },
			readerSchema:     treeReaderSchema,
			newResolvedTyped: func() any { return new(treeVReader) },
		},
		{
			name:      "tree-ptr-elem",
			schema:    treeSchema, // same schema "T"; struct uses []*T
			encodeVal: func(d int) any { return treePtrValue(d) },
			wire:      treeWire,
			json:      treeJSON,
			newTyped:  func() any { return new(treePtr) },
		},
		{
			name:      "map-recursive",
			schema:    mapSchema,
			encodeVal: func(d int) any { return mapValue(d) },
			wire:      mapWire,
			json:      mapJSON,
			newTyped:  func() any { return new(mapNode) },
		},
		// Container-of-union / array-element-union seam (see the block
		// above the oracle). Each interposes a union node between a
		// container and the recursive child, or wraps a container branch
		// in a nullable field union.
		{
			// Headline shape: the array fast path once entered the record
			// straight from the array depth, accepting ~1.5x the depth its
			// own decoder could read. Carries a resolved-decode probe.
			name:             "array-of-nullunion",
			schema:           arrNUSchema,
			encodeVal:        arrNUValue,
			wire:             arrNUWire,
			json:             arrNUJSON,
			newTyped:         func() any { return new(arrNUNode) },
			readerSchema:     arrNUReaderSchema,
			newResolvedTyped: func() any { return new(arrNUReader) },
		},
		{
			// Same schema, []**N: declines the unsafe array fast path,
			// driving the reflect serArray.ser + serNullUnionAt serItem.
			name:      "array-of-nullunion-reflect",
			schema:    arrNUPPSchema,
			encodeVal: arrNUPPValue,
			wire:      arrNUPPWire,
			json:      arrNUPPJSON,
			newTyped:  func() any { return new(arrNUPP) },
		},
		{
			name:      "array-of-nullsecond-union",
			schema:    arrNSSchema,
			encodeVal: arrNSValue,
			wire:      arrNSWire,
			json:      arrNSJSON,
			newTyped:  func() any { return new(arrNSNode) },
		},
		{
			name:      "map-of-nullunion",
			schema:    mapNUSchema,
			encodeVal: mapNUValue,
			wire:      mapNUWire,
			json:      mapNUJSON,
			newTyped:  func() any { return new(mapNUNode) },
		},
		{
			name:      "field-nullunion-of-array",
			schema:    nuArrSchema,
			encodeVal: nuArrValue,
			wire:      nuArrWire,
			json:      nuArrJSON,
			newTyped:  func() any { return new(nuArrNode) },
		},
		{
			name:      "field-nullunion-of-map",
			schema:    nuMapSchema,
			encodeVal: nuMapValue,
			wire:      nuMapWire,
			json:      nuMapJSON,
			newTyped:  func() any { return new(nuMapNode) },
		},
		{
			name:      "array-of-multibranch-union",
			schema:    arrMBSchema,
			encodeVal: arrMBValue,
			wire:      arrMBWire,
			json:      arrMBJSON,
			newTyped:  func() any { return new(arrMBNode) },
		},
		{
			name:      "array-of-map-of-nullunion",
			schema:    arrMapNUSchema,
			encodeVal: arrMapNUValue,
			wire:      arrMapNUWire,
			json:      arrMapNUJSON,
			newTyped:  func() any { return new(arrMapNUNode) },
		},
		{
			name:      "map-of-array-of-nullunion",
			schema:    mapArrNUSchema,
			encodeVal: mapArrNUValue,
			wire:      mapArrNUWire,
			json:      mapArrNUJSON,
			newTyped:  func() any { return new(mapArrNUNode) },
		},
	}

	for _, p := range shapes {
		p := p
		t.Run(p.name, func(t *testing.T) {
			depths := runShape(t, p)
			t.Logf("%s trip depths: %v", p.name, depths)
			// The core invariant: every path trips at the SAME depth.
			want := depths["encode"]
			for path, got := range depths {
				if got != want {
					t.Errorf("%s: non-uniform trip depth: %s=%d but encode=%d (all: %v)",
						p.name, path, got, want, depths)
				}
			}
			// Budget sanity: the bound must land near maxDepth (it was
			// normalized, not gutted). Shapes cost 2–4 edges/level, so the
			// trip lands between maxDepth/2 and maxDepth/4 levels (the
			// nested array<map<union>> combos are the deepest-counting, ~4
			// edges/level → ~maxDepth/4); allow generous slack but reject a
			// collapse to e.g. tens of levels.
			if want < maxDepth/5 {
				t.Errorf("%s: trip depth %d collapsed far below the budget (maxDepth=%d)", p.name, want, maxDepth)
			}
		})
	}
}

// TestDepthUniformityMutual is the mutual-recursion shape, separated
// because its decode-into-any value-shape assertion differs (the any
// tree alternates A/B map shapes), but the trip-depth uniformity is the
// same property.
func TestDepthUniformityMutual(t *testing.T) {
	s := MustParse(mrSchema)

	enc := largestOK(t, "mutual/encode", hiProbe, func(d int) error {
		_, err := s.Encode(mrValue(d))
		return err
	})
	typedDec := largestOK(t, "mutual/decode-typed", hiProbe, func(d int) error {
		_, err := s.Decode(mrWire(d), new(mrA))
		return err
	})
	anyDec := largestOK(t, "mutual/decode-any", hiProbe, func(d int) error {
		var v any
		_, err := s.Decode(mrWire(d), &v)
		return err
	})
	t.Logf("mutual trip depths: encode=%d typedDecode=%d anyDecode=%d", enc, typedDec, anyDec)
	if !(enc == typedDec && enc == anyDec) {
		t.Errorf("mutual: non-uniform trip depth: encode=%d typedDecode=%d anyDecode=%d", enc, typedDec, anyDec)
	}
}

// TestDepthUniformityNestedStructRecord pins the directly-nested struct-
// record edge: a record field whose Go type is a (non-pointer) struct
// mapped to a record, with NO intervening union/array/map node. The
// table-driven oracle above cannot express this shape — a recursive
// value-field struct has infinite size, so the deep nesting is built with
// reflect.StructOf over DISTINCT named record types that bottom at a leaf.
// Each level is exactly one record→record schema edge.
//
// This is the shape the container/union oracle structurally misses: the
// unsafe struct-fast encode path (serRecordVia via the compiled field fn)
// must charge that edge ONCE, exactly like the reflect encode path and
// every decode path. A double-count would make encode trip at ~half the
// depth decode accepts, so the pin probes a depth above that half-budget
// collapse point and requires encode AND decode to both accept it.
func TestDepthUniformityNestedStructRecord(t *testing.T) {
	// nestedRecordSchema builds a depth-d chain of distinct named records:
	// V0{v:int, inner:V1}, …, V(d-1){v:int, inner:Vd}, Vd{v:int}.
	nestedRecordSchema := func(d int) string {
		s := fmt.Sprintf(`{"type":"record","name":"V%d","fields":[{"name":"v","type":"int"}]}`, d)
		for i := d - 1; i >= 0; i-- {
			s = fmt.Sprintf(`{"type":"record","name":"V%d","fields":[{"name":"v","type":"int"},{"name":"inner","type":%s}]}`, i, s)
		}
		return s
	}
	// nestedRecordType is the matching Go type with VALUE (non-pointer)
	// Inner fields, so the struct-record unsafe fast path is exercised at
	// each level. Leaf is struct{V int32}.
	nestedRecordType := func(d int) reflect.Type {
		t := reflect.StructOf([]reflect.StructField{
			{Name: "V", Type: reflect.TypeOf(int32(0)), Tag: `avro:"v"`},
		})
		for i := 0; i < d; i++ {
			t = reflect.StructOf([]reflect.StructField{
				{Name: "V", Type: reflect.TypeOf(int32(0)), Tag: `avro:"v"`},
				{Name: "Inner", Type: t, Tag: `avro:"inner"`},
			})
		}
		return t
	}
	// nestedRecordWire: v=0 (0x00) at every level; the leaf is a lone v=0.
	nestedRecordWire := func(d int) []byte {
		w := []byte{0x00}
		for i := 0; i < d; i++ {
			w = append([]byte{0x00}, w...)
		}
		return w
	}

	// Probe at a single depth chosen ABOVE the half-budget collapse point
	// (maxDepth/2 levels, where a double-counted edge trips) and well BELOW
	// the schema-parse nesting ceiling (~maxDepth nodes), so Parse succeeds
	// and the value bound is the only thing that could reject. With one edge
	// per level, BOTH directions accept this depth; if the unsafe struct-
	// record encode edge were double-counted, encode would trip errTooDeep
	// here while decode still accepts — the exact asymmetry this pins. A
	// single deep probe (rather than walking every depth) keeps the test
	// from rebuilding O(d) schema/types at every d.
	const probeDepth = maxDepth*3/4 + 1 // 751: > maxDepth/2, < parse ceiling
	s, err := Parse(nestedRecordSchema(probeDepth))
	if err != nil {
		t.Fatalf("nested-struct-record: schema parse failed at depth %d: %v", probeDepth, err)
	}
	typ := nestedRecordType(probeDepth)
	if _, err := s.Encode(reflect.New(typ).Interface()); err != nil {
		t.Errorf("nested-struct-record: encode at depth %d failed (struct-record edge double-counted?): %v", probeDepth, err)
	}
	if _, err := s.Decode(nestedRecordWire(probeDepth), reflect.New(typ).Interface()); err != nil {
		t.Errorf("nested-struct-record: decode at depth %d failed: %v", probeDepth, err)
	}
}

// TestDepthBoundStillProtects confirms the bound VALUE is preserved: a
// genuinely cyclic Go value must error (not stack-overflow / infinite
// loop) on every encode path, and over-bound wire must be rejected on
// every decode path. This is the "didn't gut the budget" backstop that
// complements the uniformity oracle.
func TestDepthBoundStillProtects(t *testing.T) {
	// Cyclic *llNode pointing at itself.
	s := MustParse(llSchema)
	cyc := &llNode{V: 1}
	cyc.Next = cyc
	if _, err := s.Encode(cyc); !isTooDeep(err) {
		t.Errorf("cyclic encode: want errTooDeep, got %v", err)
	}
	if _, err := s.EncodeJSON(cyc); !isTooDeep(err) {
		t.Errorf("cyclic json encode: want errTooDeep, got %v", err)
	}
	// Over-bound wire on every decode path.
	deep := llWire(hiProbe)
	if _, err := s.Decode(deep, new(llNode)); !isTooDeep(err) {
		t.Errorf("over-bound typed decode: want errTooDeep, got %v", err)
	}
	var anyV any
	if _, err := s.Decode(deep, &anyV); !isTooDeep(err) {
		t.Errorf("over-bound any decode: want errTooDeep, got %v", err)
	}
	if err := s.DecodeJSON(llJSON(hiProbe), &anyV); !isTooDeep(err) {
		t.Errorf("over-bound json decode: want errTooDeep, got %v", err)
	}
}

// TestDepthBoundCyclicContainers confirms a cyclic Go value through EVERY
// container-of-union carrier (the seam fixed here) errors errTooDeep and
// never infinite-loops / OOMs, on binary AND JSON encode. Decode-side
// cyclic protection is covered by the over-bound wire probes in
// TestDepthBoundStillProtects (decode cannot build an unbounded value —
// the bound rejects the wire). A map[string]any self-reference (no Go
// pointer cycle, a value-level graph cycle) is included because it routes
// through the reflect map/union paths rather than the unsafe pointer path.
func TestDepthBoundCyclicContainers(t *testing.T) {
	cyc := func(name, schema string, v any) {
		t.Helper()
		s := MustParse(schema)
		if _, err := s.Encode(v); !isTooDeep(err) {
			t.Errorf("%s: binary encode: want errTooDeep, got %v", name, err)
		}
		if _, err := s.EncodeJSON(v); !isTooDeep(err) {
			t.Errorf("%s: json encode: want errTooDeep, got %v", name, err)
		}
	}

	an := &arrNUNode{V: 1}
	an.Kids = []*arrNUNode{an}
	cyc("array-of-nullunion", arrNUSchema, an)

	ns := &arrNSNode{V: 1}
	ns.Kids = []*arrNSNode{ns}
	cyc("array-of-nullsecond-union", arrNSSchema, ns)

	mn := &mapNUNode{V: 1}
	mn.Kids = map[string]*mapNUNode{"a": mn}
	cyc("map-of-nullunion", mapNUSchema, mn)

	{
		na := &nuArrNode{V: 1}
		kids := []nuArrNode{{V: 2}}
		na.Kids = &kids
		(*na.Kids)[0].Kids = na.Kids // slice element references the same slice
		cyc("field-nullunion-of-array", nuArrSchema, na)
	}
	{
		nm := &nuMapNode{V: 1}
		m := map[string]nuMapNode{}
		nm.Kids = &m
		m["self"] = nuMapNode{V: 2, Kids: nm.Kids}
		cyc("field-nullunion-of-map", nuMapSchema, nm)
	}

	amn := &arrMapNUNode{V: 1}
	amn.Kids = []map[string]*arrMapNUNode{{"a": amn}}
	cyc("array-of-map-of-nullunion", arrMapNUSchema, amn)

	man := &mapArrNUNode{V: 1}
	man.Kids = map[string][]*mapArrNUNode{"a": {man}}
	cyc("map-of-array-of-nullunion", mapArrNUSchema, man)

	mb := &arrMBNode{V: 1}
	mb.Kids = []any{mb}
	cyc("array-of-multibranch-union", arrMBSchema, mb)

	// map[string]any self-reference against the recursive linked-list
	// schema (tagged-union self-ref) — a value-graph cycle, not a Go
	// pointer cycle.
	{
		s := MustParse(llSchema)
		m := map[string]any{"v": int32(1)}
		m["next"] = map[string]any{"LL": m}
		if _, err := s.Encode(m); !isTooDeep(err) {
			t.Errorf("map[string]any self-ref binary encode: want errTooDeep, got %v", err)
		}
		if _, err := s.EncodeJSON(m); !isTooDeep(err) {
			t.Errorf("map[string]any self-ref json encode: want errTooDeep, got %v", err)
		}
	}
}

// Compile-time assert the hand-built wire matches the encoder at a
// shallow depth (guards against a wire-builder typo silently measuring
// the wrong thing).
func TestDepthOracleWireMatchesEncoder(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		val    any
		wire   []byte
	}{
		{"linked-list", llSchema, llValue(3), llWire(3)},
		{"tree-value", treeSchema, treeVValue(3), treeWire(3)},
		{"map", mapSchema, mapValue(2), mapWire(2)},
		{"mutual-even", mrSchema, mrValue(2), mrWire(2)},
		{"mutual-odd", mrSchema, mrValue(3), mrWire(3)},
		{"array-of-nullunion", arrNUSchema, arrNUValue(3), arrNUWire(3)},
		{"array-of-nullunion-reflect", arrNUPPSchema, arrNUPPValue(3), arrNUPPWire(3)},
		{"array-of-nullsecond-union", arrNSSchema, arrNSValue(3), arrNSWire(3)},
		{"map-of-nullunion", mapNUSchema, mapNUValue(3), mapNUWire(3)},
		{"field-nullunion-of-array", nuArrSchema, nuArrValue(3), nuArrWire(3)},
		{"field-nullunion-of-map", nuMapSchema, nuMapValue(3), nuMapWire(3)},
		{"array-of-multibranch-union", arrMBSchema, arrMBValue(3), arrMBWire(3)},
		{"array-of-map-of-nullunion", arrMapNUSchema, arrMapNUValue(2), arrMapNUWire(2)},
		{"map-of-array-of-nullunion", mapArrNUSchema, mapArrNUValue(2), mapArrNUWire(2)},
	}
	for _, c := range cases {
		s := MustParse(c.schema)
		got, err := s.Encode(c.val)
		if err != nil {
			t.Fatalf("%s: encode: %v", c.name, err)
		}
		if fmt.Sprintf("% x", got) != fmt.Sprintf("% x", c.wire) {
			t.Errorf("%s: hand-wire mismatch\n encoder: % x\n hand:    % x", c.name, got, c.wire)
		}
	}
}
