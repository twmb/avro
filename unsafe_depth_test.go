package avro

import "testing"

// TestRegression_UnsafeDecodeDepthBounded gives end-to-end coverage of the
// recursion-depth bound on the UNSAFE decode path: a self-referential record
// nested past maxDepth, decoded into an addressable struct, must error and not
// recurse unbounded. There was no such test before.
//
// Triage note: a scoped mutation run flagged the slab-depth bookkeeping
// (sl.depth++ / sl.depth-- at the recursive-dispatch sites) as surviving. This
// test does NOT kill those mutants — verified by neutering each: the decode
// still errors with the bookkeeping flipped, because the depth limit is
// enforced REDUNDANTLY (a primary guard fires regardless of those specific
// lines). They are therefore equivalent/redundant mutants, not an exploitable
// gap. The test stays for the genuine end-to-end coverage. The wire is hand-
// built because encode cannot produce an over-deep value (its own depth guard
// stops it first).
func TestRegression_UnsafeDecodeDepthBounded(t *testing.T) {
	// Node = record{ child: ["null", Node], v: int } — a self-referential
	// type whose decode recurses once per nesting level.
	s := MustParse(`{"type":"record","name":"Node","fields":[` +
		`{"name":"child","type":["null","Node"]},{"name":"v","type":"int"}]}`)

	// Build a wire nested deeper than maxDepth:
	//   level: child-union-index=1 (Node) ... then v=0
	//   leaf:  child-union-index=0 (null), v=0
	// zigzag(1)=0x02, zigzag(0)=0x00.
	const depth = maxDepth + 5
	var wire []byte
	for range depth {
		wire = append(wire, 0x02) // child = the Node branch
	}
	wire = append(wire, 0x00, 0x00) // innermost: child = null, v = 0
	for range depth {
		wire = append(wire, 0x00) // v = 0 unwinding each level
	}

	// Node has a *Node field, so decoding into an addressable &Node routes
	// through the unsafe null-union-record/record path that bumps sl.depth.
	type Node struct {
		Child *Node `avro:"child"`
		V     int32 `avro:"v"`
	}
	var n Node
	if _, err := s.Decode(wire, &n); err == nil {
		t.Fatal("decode of a structure nested past maxDepth through the unsafe path must error (recursion-depth DoS guard); got nil — the depth guard is defeated")
	}

	// A shallow value must still decode (the guard must not false-trigger on
	// the unwound path — catches the inverse sl.depth-- on enter / ++ on exit).
	shallow := []byte{0x02, 0x00, 0x00, 0x00} // one level: child=Node{child=null,v=0}, v=0
	var sn Node
	if _, err := s.Decode(shallow, &sn); err != nil {
		t.Fatalf("shallow nested decode falsely rejected (depth guard mis-restored): %v", err)
	}
}
