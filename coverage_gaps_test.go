package avro

import (
	"fmt"
	"testing"
)

// TestCoverage_JSONNumericIntSizeForms exercises jsonNumericInt via the public
// parse paths that actually reach it: a bare numeric size (int64 arm) and a
// quoted-string size "16" (string arm, the Avro [INTEGERS] rule). Both flow
// through getCIInt during Root() metadata-tree construction. NOTE the honest
// limit: the float64 and json.Number arms stay uncovered — they are DEFENSIVE
// breadth (the function accepts every numeric representation) that no current
// public path produces, so covering them would require constructing the
// metadata value by hand. Left uncovered on purpose rather than with a theater
// test; this is the kind of "incomplete" coverage that is defensive, not a bug.
func TestCoverage_JSONNumericIntSizeForms(t *testing.T) {
	for _, sz := range []string{`16`, `"16"`} { // bare (json.Number) and quoted (string)
		s := MustParse(fmt.Sprintf(`{"type":"fixed","name":"F","size":%s}`, sz))
		root := s.Root()
		if root.Size != 16 {
			t.Fatalf("fixed size %s: Root().Size = %d, want 16", sz, root.Size)
		}
		// The quoted and bare forms must produce the same wire behavior too.
		wire, err := s.AppendEncode(nil, make([]byte, 16))
		if err != nil || len(wire) != 16 {
			t.Fatalf("fixed size %s: encode 16 bytes: err=%v len=%d", sz, err, len(wire))
		}
	}
}

// TestCoverage_RatFromBytesHostileScale exercises bytesToRat's public-API
// safety guard: RatFromBytes is exported, so a caller can pass an
// attacker-controlled scale beyond decimalScaleLimit (internal callers pass
// schema-validated bounded scale). The guard must return a zero Rat rather
// than materialize a 10^scale big.Int. This branch had no coverage.
func TestCoverage_RatFromBytesHostileScale(t *testing.T) {
	for _, scale := range []int{decimalScaleLimit + 1, -decimalScaleLimit - 1} {
		r := RatFromBytes([]byte{0x01}, scale)
		if r == nil || r.Sign() != 0 {
			t.Fatalf("RatFromBytes with hostile scale %d: got %v, want zero Rat", scale, r)
		}
	}
	// A within-bounds scale still works (control).
	if r := RatFromBytes([]byte{0x01}, 2); r.Cmp(scaledRat(bytesToBigInt([]byte{0x01}), 2)) != 0 {
		t.Fatalf("RatFromBytes within bounds diverged: %v", r)
	}
}
