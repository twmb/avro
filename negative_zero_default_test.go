package avro_test

import (
	"math"
	"testing"

	"github.com/twmb/avro"
)

// doubleDefaultWireSignbit encodes the auto-filled single-double-field default
// and reports whether the wire double has its IEEE sign bit set.
func doubleDefaultWireSignbit(t *testing.T, s *avro.Schema) bool {
	t.Helper()
	wire, err := s.Encode(map[string]any{})
	if err != nil {
		t.Fatalf("encode default-fill: %v", err)
	}
	if len(wire) != 8 {
		t.Fatalf("expected 8 wire bytes for one double field, got %d (%x)", len(wire), wire)
	}
	return wire[7]&0x80 != 0 // little-endian double: sign bit in the top byte
}

// A negative zero written in FLOAT syntax ("-0.0", "-0e0") must keep its IEEE
// sign coherently across the wire, the metadata API (Root().Fields[].Default),
// and a Root().Schema() rebuild. Before the fix the metadata pipeline collapsed
// "-0.0" to int64(0) (a big.Rat has no signed zero), so Default reported +0.0
// while the wire wrote -0.0, and the rebuild re-emitted +0.0 (Go's json.Marshal
// renders -0.0 as the integer token "-0"). The fix preserves the sign in
// normalizeJSONNumber and re-emits float syntax ("-0.0") on rebuild.
func TestRegression_NegativeZeroFloatSyntaxDefaultCoherence(t *testing.T) {
	for _, lit := range []string{"-0.0", "-0e0"} {
		t.Run(lit, func(t *testing.T) {
			s, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"d","type":"double","default":` + lit + `}]}`)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if !doubleDefaultWireSignbit(t, s) {
				t.Errorf("%s: wire lost the negative sign", lit)
			}
			def, ok := s.Root().Fields[0].Default.(float64)
			if !ok {
				t.Fatalf("%s: Default is %T, want float64", lit, s.Root().Fields[0].Default)
			}
			if !math.Signbit(def) {
				t.Errorf("%s: metadata Default lost the negative sign: %v", lit, def)
			}
			// Rebuild must not flip the default wire.
			root := s.Root()
			s2, err := root.Schema()
			if err != nil {
				t.Fatalf("%s: rebuild: %v", lit, err)
			}
			w1, _ := s.Encode(map[string]any{})
			w2, _ := s2.Encode(map[string]any{})
			if string(w1) != string(w2) {
				t.Errorf("%s: Root().Schema() rebuild changed the default wire: %x -> %x", lit, w1, w2)
			}
		})
	}
}

// Positive zero (either syntax) is unsigned everywhere — the control that the
// fix's negative-zero detection does not over-trigger.
func TestNegativeZeroPositiveControls(t *testing.T) {
	for _, lit := range []string{"0.0", "0"} {
		s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"d","type":"double","default":` + lit + `}]}`)
		if doubleDefaultWireSignbit(t, s) {
			t.Errorf("%s: positive zero wrote a sign bit", lit)
		}
		def := s.Root().Fields[0].Default.(float64)
		if math.Signbit(def) {
			t.Errorf("%s: positive zero metadata has a sign bit: %v", lit, def)
		}
	}
}

// Documented residual: a negative zero written in INTEGER syntax ("-0") is the
// integer 0. twmb's wire pipeline parses it via strconv.ParseFloat (which keeps
// the sign → -0.0 on the wire, identically on binary and JSON), while the
// metadata pipeline collapses it to int64(0) → +0.0; the rebuild then re-emits
// the metadata +0.0. The references (Java Jackson IntNode, fastavro int) treat
// "-0" as sign-less 0 on every surface. Reconciling twmb's wire to +0.0 would
// require changing the shared json.Number→float parser, which also drives
// runtime json.Number encode/decode and JSON float formatting (Go renders -0.0
// as "-0"), rippling into binary↔JSON round-trip stability for genuine -0.0.
// twmb keeps the wire internally consistent (binary == JSON) and accepts the
// metadata-vs-wire divergence on this degenerate literal. This pins the exact
// current behavior so a future change is noticed.
func TestNegativeZeroIntegerLiteralResidual(t *testing.T) {
	s := avro.MustParse(`{"type":"record","name":"R","fields":[{"name":"d","type":"double","default":-0}]}`)
	if !doubleDefaultWireSignbit(t, s) {
		t.Error("integer -0: wire is expected to carry the ParseFloat sign (-0.0) today")
	}
	def := s.Root().Fields[0].Default.(float64)
	if math.Signbit(def) {
		t.Error("integer -0: metadata is expected to be +0.0 (int64 collapse) today")
	}
	// Binary and JSON wire must agree (the invariant twmb protects).
	binWire, _ := s.Encode(map[string]any{})
	jsonWire, _ := s.AppendEncodeJSON(nil, map[string]any{})
	var got map[string]any
	if err := s.DecodeJSON(jsonWire, &got); err != nil {
		t.Fatalf("decodeJSON: %v", err)
	}
	reBin, _ := s.Encode(got)
	if string(binWire) != string(reBin) {
		t.Errorf("integer -0: binary (%x) and JSON-roundtrip (%x) wire diverge", binWire, reBin)
	}
}

// The float32 field arm and the Props metadata arm carry the same float-syntax
// rule. A "-0.0" default on a float field surfaces as float32(-0.0), and a
// "-0.0" property surfaces as float64(-0.0) (Java's Jackson DoubleNode), each
// stable through a Root().Schema() rebuild.
func TestRegression_NegativeZeroFloat32AndProps(t *testing.T) {
	t.Run("float32_field", func(t *testing.T) {
		s, err := avro.Parse(`{"type":"record","name":"R","fields":[{"name":"f","type":"float","default":-0.0}]}`)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		def, ok := s.Root().Fields[0].Default.(float32)
		if !ok {
			t.Fatalf("Default is %T, want float32", s.Root().Fields[0].Default)
		}
		if !math.Signbit(float64(def)) {
			t.Errorf("float32 default lost its sign: %v", def)
		}
		wire, _ := s.Encode(map[string]any{})
		if len(wire) != 4 || wire[3]&0x80 == 0 {
			t.Errorf("float32 default wire not negative zero: %x", wire)
		}
	})

	t.Run("props_neg_zero", func(t *testing.T) {
		s, err := avro.Parse(`{"type":"record","name":"R","namespace":"ns","x":-0.0,"fields":[]}`)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		f, ok := s.Root().Props["x"].(float64)
		if !ok {
			t.Fatalf("Props[x] is %T, want float64", s.Root().Props["x"])
		}
		if !math.Signbit(f) {
			t.Errorf("Props negative zero lost its sign: %v", f)
		}
		root := s.Root()
		s2, err := root.Schema()
		if err != nil {
			t.Fatalf("rebuild: %v", err)
		}
		f2, ok := s2.Root().Props["x"].(float64)
		if !ok || !math.Signbit(f2) {
			t.Errorf("rebuilt Props[x] = %v (%T), sign lost", s2.Root().Props["x"], s2.Root().Props["x"])
		}
	})
}
