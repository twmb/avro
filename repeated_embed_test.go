package avro

import (
	"testing"
)

// TestRegression_RepeatedEmbedShallowestWins pins doc.go's documented field
// precedence ("among fields with the same tagged status, the shallowest
// wins") for the case where the SAME embedded type is
// reachable through two different embed paths at different depths. The
// field-mapper's cycle-breaking visited map was marked-forever, so the
// depth-first walk collected only the FIRST (deeper) occurrence of the
// repeated type and the shallower occurrence never reached the
// shallowest-wins dedup — encode and decode both silently selected the deep
// field, disagreeing with Go's own promotion (r.X), reflect.FieldByName, and
// encoding/json.
func TestRegression_RepeatedEmbedShallowestWins(t *testing.T) {
	type C struct {
		X int32 `avro:"X"`
	}
	type D struct{ C }
	type R struct {
		D
		C // shallower X — Go's r.X and encoding/json both select this one
	}

	s := MustParse(`{"type":"record","name":"R","fields":[{"name":"X","type":"int"}]}`)

	// Encode must read the SHALLOW field (r.C.X), matching Go promotion.
	var r R
	r.D.C.X = 1 // deeper
	r.C.X = 2   // shallower — the one Go's r.X selects
	if got := r.X; got != 2 {
		t.Fatalf("Go promotion sanity: r.X = %d, want 2", got)
	}
	data, err := s.AppendEncode(nil, &r)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out map[string]any
	if _, err := s.Decode(data, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out["X"] != int32(2) {
		t.Fatalf("encode selected the DEEPER field: X=%v, want 2 (shallowest-wins / Go promotion)", out["X"])
	}

	// Decode must WRITE the shallow field (r.C.X), leaving the deep one zero.
	wire, err := s.AppendEncode(nil, map[string]any{"X": int32(9)})
	if err != nil {
		t.Fatalf("encode map: %v", err)
	}
	var r2 R
	if _, err := s.Decode(wire, &r2); err != nil {
		t.Fatalf("decode into struct: %v", err)
	}
	if r2.C.X != 9 || r2.D.C.X != 0 {
		t.Fatalf("decode wrote the DEEPER field: shallow=%d deep=%d, want shallow=9 deep=0", r2.C.X, r2.D.C.X)
	}
}

// TestRegression_DoubleInlineRejectsDuplicate pins a corollary of the
// per-path fix: a type inlined twice (struct{ A P ",inline"; B P ",inline" })
// is a genuine duplicate-field collision. The old mark-forever prune made
// B's fields vanish before SchemaFor's duplicate-name check, silently
// accepting the declaration and dropping B's data on encode. With per-path
// collection both copies surface, so SchemaFor rejects it as the dup it is.
func TestRegression_DoubleInlineRejectsDuplicate(t *testing.T) {
	type P struct {
		X int32 `avro:"x"`
		Y int32 `avro:"y"`
	}
	type Inl struct {
		A P `avro:",inline"`
		B P `avro:",inline"`
	}
	if _, err := SchemaFor[Inl](); err == nil {
		t.Fatal("SchemaFor accepted a type inlined twice (silent data drop); must reject as a duplicate-field collision")
	}
}

// TestRegression_EmbedCycleStillTerminates confirms the per-path fix did not
// reintroduce infinite recursion: a self-referential embed (a pointer to the
// same type) must still map cleanly — the cycle revisits a type while it is
// ON the current path, which the per-path visited set still prunes.
func TestRegression_EmbedCycleStillTerminates(t *testing.T) {
	type Node struct {
		*Node       // embedded self-pointer (cycle)
		V     int32 `avro:"v"`
	}
	s := MustParse(`{"type":"record","name":"N","fields":[{"name":"v","type":"int"}]}`)
	data, err := s.AppendEncode(nil, &Node{V: 7})
	if err != nil {
		t.Fatalf("encode cyclic-embed type: %v", err)
	}
	var n Node
	if _, err := s.Decode(data, &n); err != nil {
		t.Fatalf("decode cyclic-embed type: %v", err)
	}
	if n.V != 7 {
		t.Fatalf("cyclic-embed round-trip: V=%d, want 7", n.V)
	}
}
