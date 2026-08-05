package avro_test

import (
	"testing"

	"github.com/twmb/avro"
)

// Per the Avro spec, a reader-union branch matches a writer fixed only when
// "both schemas are fixed whose sizes and (unqualified) names match", and
// resolution selects "the first schema in the reader's union that matches". So
// fixed SIZE is part of the match predicate: a wrong-size same-name branch does
// NOT match and selection must continue to a later size-matching branch.
// fastavro implements this (and the spec mandates it); branch selection that
// matched on name alone and only rejected on size afterward let a wrong-size
// branch MASK a correct-size one, erroring on a value that is fully decodable
// (and decodable just by reordering the reader union). This pins that size is
// folded into selection for both Resolve and CheckCompatibility, both wires.
func TestRegression_UnionFixedSizeFoldedIntoSelection(t *testing.T) {
	writer := avro.MustParse(`{"type":"fixed","name":"F","namespace":"ns0","size":4}`)
	// The wrong-size branch (size 8) is declared BEFORE the correct-size (size 4):
	// it must be skipped, not select-then-reject and mask the size-4 branch.
	reader := avro.MustParse(`["null",{"type":"fixed","name":"F","namespace":"ns1","size":8},{"type":"fixed","name":"F","namespace":"ns2","size":4}]`)

	if err := avro.CheckCompatibility(writer, reader); err != nil {
		t.Fatalf("CheckCompatibility rejected a writer that matches the size-4 branch: %v", err)
	}
	resolved, err := avro.Resolve(writer, reader)
	if err != nil {
		t.Fatalf("Resolve rejected a writer that matches the size-4 branch: %v", err)
	}

	wire, err := writer.AppendEncode(nil, [4]byte{1, 2, 3, 4})
	if err != nil {
		t.Fatalf("encode writer value: %v", err)
	}
	// Binary: the resolved union decodes the writer's 4-byte fixed via the
	// size-4 branch.
	var got any
	if _, err := resolved.Decode(wire, &got); err != nil {
		t.Fatalf("resolved binary decode: %v", err)
	}
	if !fixedBytesEqual(got, []byte{1, 2, 3, 4}) {
		t.Fatalf("resolved binary decode value: got %T %v, want fixed [1 2 3 4]", got, got)
	}

	// JSON: the resolved DecodeJSON consumes writer-shaped JSON and resolves the
	// same way (NOT_BUGS #2). The writer encodes its fixed as a codepoint string.
	jwire, err := writer.AppendEncodeJSON(nil, [4]byte{1, 2, 3, 4})
	if err != nil {
		t.Fatalf("encode writer JSON: %v", err)
	}
	var gotJ any
	if err := resolved.DecodeJSON(jwire, &gotJ); err != nil {
		t.Fatalf("resolved JSON decode: %v", err)
	}
	if !fixedBytesEqual(gotJ, []byte{1, 2, 3, 4}) {
		t.Fatalf("resolved JSON decode value: got %T %v, want fixed [1 2 3 4]", gotJ, gotJ)
	}

	// Order-independence control: the size-4 branch declared first also resolves.
	readerRev := avro.MustParse(`["null",{"type":"fixed","name":"F","namespace":"ns2","size":4},{"type":"fixed","name":"F","namespace":"ns1","size":8}]`)
	if _, err := avro.Resolve(writer, readerRev); err != nil {
		t.Fatalf("Resolve (size-4 first) unexpectedly rejected: %v", err)
	}

	// Boundary: NO size-matching branch present must still reject (no silent
	// wrong-branch selection).
	readerNoMatch := avro.MustParse(`["null",{"type":"fixed","name":"F","namespace":"ns1","size":8}]`)
	if _, err := avro.Resolve(writer, readerNoMatch); err == nil {
		t.Fatal("Resolve must reject when no reader branch matches the writer's fixed size")
	}
}

func fixedBytesEqual(v any, want []byte) bool {
	switch b := v.(type) {
	case []byte:
		return string(b) == string(want)
	case [4]byte:
		return string(b[:]) == string(want)
	}
	return false
}
